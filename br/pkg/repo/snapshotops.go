// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package repo

import (
	"cmp"
	"context"
	"path"
	"slices"
	"strconv"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"golang.org/x/sync/errgroup"
)

type PendingBackupState string

const (
	// PendingBackupStateStale means the pending backup is not resumable.
	// Completed snapshot metadata/data, if present, are kept.
	PendingBackupStateStale PendingBackupState = "stale"
	// PendingBackupStateUnfinished means the backup has checkpoint metadata but no completed backup metadata yet.
	PendingBackupStateUnfinished PendingBackupState = "unfinished"
)

type PendingBackup struct {
	BackupID    BackupID
	MarkerPaths []string
	State       PendingBackupState
}

type SnapshotDeleteResult struct {
	BackupID        BackupID
	MetadataDeleted int
	DataDeleted     int
	PendingDeleted  int
}

type SnapshotOps struct {
	storeapi.Storage
}

// SnapshotMutationOption customizes repo snapshot mutation progress observation.
type SnapshotMutationOption func(*snapshotMutationOptions)

type snapshotMutationOptions struct {
	onDiscover func(int)
	onDelete   func(int)
}

// WithMutationDiscoveredProgress reports discovered object-count deltas during mutations.
// For example, pass `func(n int) { total += n }` to grow a dynamic progress bar.
func WithMutationDiscoveredProgress(onDiscover func(int)) SnapshotMutationOption {
	return func(opts *snapshotMutationOptions) {
		opts.onDiscover = onDiscover
	}
}

// WithMutationDeletedProgress reports deleted object-count deltas during mutations.
// For example, pass `func(n int) { done += n }` to advance a dynamic progress bar.
func WithMutationDeletedProgress(onDelete func(int)) SnapshotMutationOption {
	return func(opts *snapshotMutationOptions) {
		opts.onDelete = onDelete
	}
}

func collectSnapshotMutationOptions(options []SnapshotMutationOption) snapshotMutationOptions {
	var opts snapshotMutationOptions
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}
	return opts
}

func (opts snapshotMutationOptions) discovered(count int) {
	if count <= 0 || opts.onDiscover == nil {
		return
	}
	opts.onDiscover(count)
}

func (opts snapshotMutationOptions) deleted(count int) {
	if count <= 0 || opts.onDelete == nil {
		return
	}
	opts.onDelete(count)
}

// SnapshotOpsExtension wraps repo-root storage with snapshot management helpers.
// For example, `SnapshotOpsExtension(root).DeleteSnapshot(ctx, id)` deletes one
// snapshot.
func SnapshotOpsExtension(storage storeapi.Storage) SnapshotOps {
	return SnapshotOps{Storage: storage}
}

// ListPendingBackups returns all pending backups with stale or unfinished state.
// For example, a marker whose backup has `backupmeta` is returned as stale.
func (ops SnapshotOps) ListPendingBackups(ctx context.Context) ([]PendingBackup, error) {
	return listPendingBackups(ctx, ops.Storage, WalkPendingMarkers(ctx, ops.Storage))
}

// ListPendingBackupsForConfigHash returns pending backups under one config hash.
// For example, backup startup uses it with the current config hash to find
// resumable attempts.
func (ops SnapshotOps) ListPendingBackupsForConfigHash(
	ctx context.Context,
	configHash []byte,
) ([]PendingBackup, error) {
	return listPendingBackups(
		ctx,
		ops.Storage,
		walkPendingMarkers(ctx, ops.Storage, &storeapi.WalkOption{SubDir: PendingDir(configHash)}),
	)
}

func listPendingBackups(
	ctx context.Context,
	storage storeapi.Storage,
	markers TrySeq[PendingMarker],
) ([]PendingBackup, error) {
	grouped := make(map[BackupID]*PendingBackup)
	for err, marker := range markers {
		if err != nil {
			return nil, errors.Annotate(err, "walk pending snapshot markers")
		}
		entry, ok := grouped[marker.BackupID]
		if !ok {
			entry = &PendingBackup{BackupID: marker.BackupID}
			grouped[marker.BackupID] = entry
		}
		entry.MarkerPaths = append(entry.MarkerPaths, marker.Path)
	}

	backups := make([]PendingBackup, 0, len(grouped))
	ops := SnapshotOps{Storage: storage}
	for _, entry := range grouped {
		slices.Sort(entry.MarkerPaths)
		state, err := ops.inspectPendingBackupState(ctx, entry.BackupID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		entry.State = state
		backups = append(backups, *entry)
	}

	slices.SortFunc(backups, func(a, b PendingBackup) int {
		return cmp.Compare(a.BackupID, b.BackupID)
	})
	return backups, nil
}

func (ops SnapshotOps) inspectPendingBackupState(
	ctx context.Context,
	backupID BackupID,
) (PendingBackupState, error) {
	metadataStorage := NewPrefixedStorage(ops.Storage, SnapshotMetadataDir(backupID))
	hasBackupMeta, err := metadataStorage.FileExists(ctx, metautil.MetaFile)
	if err != nil {
		return "", errors.Annotatef(err, "check %s for pending backup %s", metautil.MetaFile, backupID)
	}
	if hasBackupMeta {
		return PendingBackupStateStale, nil
	}
	hasCheckpoint, err := metadataStorage.FileExists(ctx, checkpoint.CheckpointMetaPathForBackup)
	if err != nil {
		return "", errors.Annotatef(err, "check %s for pending backup %s", checkpoint.CheckpointMetaPathForBackup, backupID)
	}
	if hasCheckpoint {
		return PendingBackupStateUnfinished, nil
	}
	return PendingBackupStateStale, nil
}

func filePathStream(paths []string) TrySeq[string] {
	return func(yield func(error, string) bool) {
		for _, filePath := range paths {
			if !yield(nil, filePath) {
				return
			}
		}
	}
}

func filterFilePathStream(stream TrySeq[string], include func(string) bool) TrySeq[string] {
	return func(yield func(error, string) bool) {
		for err, filePath := range stream {
			if err != nil {
				yield(err, "")
				return
			}
			if include != nil && !include(filePath) {
				continue
			}
			if !yield(nil, filePath) {
				return
			}
		}
	}
}

func (ops SnapshotOps) deleteFilesFromStream(
	ctx context.Context,
	stream TrySeq[string],
	opts snapshotMutationOptions,
) (int, error) {
	const maxConcurrentWork = 128

	deleteCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, groupCtx := errgroup.WithContext(deleteCtx)
	group.SetLimit(maxConcurrentWork)
	var deleted atomic.Int64
	var streamErr error
	for err, filePath := range stream {
		if err != nil {
			streamErr = errors.Trace(err)
			cancel()
			break
		}
		if groupCtx.Err() != nil {
			break
		}
		opts.discovered(1)
		group.Go(func() error {
			if err := ops.Storage.DeleteFile(groupCtx, filePath); err != nil {
				return errors.Annotatef(err, "delete file %s", filePath)
			}
			deleted.Add(1)
			opts.deleted(1)
			return nil
		})
	}

	groupErr := group.Wait()
	deletedCount := int(deleted.Load())
	if streamErr != nil {
		if groupErr != nil && !errors.ErrorEqual(groupErr, context.Canceled) {
			return deletedCount, errors.Annotatef(streamErr, "wait in-flight deletions: %v", groupErr)
		}
		return deletedCount, streamErr
	}
	if groupErr != nil {
		return deletedCount, errors.Trace(groupErr)
	}
	return deletedCount, nil
}

// DeleteSnapshot removes one snapshot backup or unfinished backup attempt's
// metadata, data files, and pending markers. For a completed snapshot, it writes
// a DELETING marker before removing the rest of the metadata so interrupted
// deletions are visible to list operations. For example, BackupID(0xF00D)
// deletes `_meta/snapshot/000000000000F00D` and matching data prefixes.
func (ops SnapshotOps) DeleteSnapshot(
	ctx context.Context,
	backupID BackupID,
	options ...SnapshotMutationOption,
) (SnapshotDeleteResult, error) {
	opts := collectSnapshotMutationOptions(options)
	result := SnapshotDeleteResult{BackupID: backupID}
	if err := ops.requireRepoStartAfter(); err != nil {
		return result, errors.Annotatef(err, "delete snapshot %s", backupID)
	}

	removeDeletingMarker, err := ops.prepareSnapshotDeletingMarker(ctx, backupID)
	if err != nil {
		return result, errors.Annotatef(err, "prepare snapshot deleting marker for %s", backupID)
	}
	result.MetadataDeleted, err = ops.deleteSnapshotMetadataFiles(ctx, backupID, opts)
	if err != nil {
		return result, errors.Annotatef(err, "delete snapshot metadata for %s", backupID)
	}
	result.DataDeleted, err = ops.deleteSnapshotDataFilesForBackup(ctx, backupID, opts)
	if err != nil {
		return result, errors.Annotatef(err, "delete snapshot data files for %s", backupID)
	}
	result.PendingDeleted, err = ops.deletePendingMarkersForBackup(ctx, backupID, opts)
	if err != nil {
		return result, errors.Annotatef(err, "delete pending markers for snapshot %s", backupID)
	}
	if removeDeletingMarker {
		if err := ops.deleteSnapshotDeletingMarker(ctx, backupID); err != nil {
			return result, errors.Trace(err)
		}
	}
	return result, nil
}

// ValidateRepoStartAfterSupport checks whether storage can use WalkDir StartAfter.
// It inspects the Features bitset returned by FeatureOf.
func ValidateRepoStartAfterSupport(storage storeapi.Storage) error {
	if storeapi.FeatureOf(storage)&storeapi.FeatureSupportsStartAfter != 0 {
		return nil
	}
	return errors.Annotatef(
		berrors.ErrUnsupportedOperation,
		"storage %s does not support WalkDir StartAfter required by repo snapshot operations",
		storage.URI(),
	)
}

func (ops SnapshotOps) requireRepoStartAfter() error {
	return ValidateRepoStartAfterSupport(ops.Storage)
}

func (ops SnapshotOps) deletePendingMarkersForBackup(
	ctx context.Context,
	backupID BackupID,
	opts snapshotMutationOptions,
) (int, error) {
	backups, err := ops.ListPendingBackups(ctx)
	if err != nil {
		return 0, errors.Annotatef(err, "list pending backups before deleting markers for %s", backupID)
	}
	for _, pending := range backups {
		if pending.BackupID != backupID {
			continue
		}
		return ops.deleteFilesFromStream(ctx, filePathStream(pending.MarkerPaths), opts)
	}
	return 0, nil
}

func (ops SnapshotOps) prepareSnapshotDeletingMarker(ctx context.Context, backupID BackupID) (bool, error) {
	metadataStorage := NewPrefixedStorage(ops.Storage, SnapshotMetadataDir(backupID))
	hasBackupMeta, err := metadataStorage.FileExists(ctx, metautil.MetaFile)
	if err != nil {
		return false, errors.Annotatef(err, "check snapshot metadata for %s", backupID)
	}
	if hasBackupMeta {
		if err := metadataStorage.WriteFile(ctx, snapshotDeletingMarkerFile, []byte("deleting\n")); err != nil {
			return false, errors.Annotatef(err, "write snapshot deleting marker for %s", backupID)
		}
		return true, nil
	}
	hasDeletingMarker, err := metadataStorage.FileExists(ctx, snapshotDeletingMarkerFile)
	if err != nil {
		return false, errors.Annotatef(err, "check snapshot deleting marker for %s", backupID)
	}
	return hasDeletingMarker, nil
}

func (ops SnapshotOps) deleteSnapshotMetadataFiles(
	ctx context.Context,
	backupID BackupID,
	opts snapshotMutationOptions,
) (int, error) {
	deletingMarkerPath := SnapshotDeletingMarkerFile(backupID)
	metadataFiles := filterFilePathStream(
		ops.walkFilesWithPrefix(ctx, SnapshotMetadataDir(backupID)),
		func(filePath string) bool {
			return filePath != deletingMarkerPath
		},
	)
	return ops.deleteFilesFromStream(ctx, metadataFiles, opts)
}

func (ops SnapshotOps) deleteSnapshotDeletingMarker(ctx context.Context, backupID BackupID) error {
	if err := ops.Storage.DeleteFile(ctx, SnapshotDeletingMarkerFile(backupID)); err != nil {
		return errors.Annotatef(err, "delete snapshot deleting marker for %s", backupID)
	}
	return nil
}

func (ops SnapshotOps) deleteSnapshotDataFilesForBackup(
	ctx context.Context,
	backupID BackupID,
	opts snapshotMutationOptions,
) (int, error) {
	deleted := 0
	for err, head := range ops.walkSnapshotStoreHeads(ctx) {
		if err != nil {
			return deleted, errors.Annotatef(err, "walk snapshot store heads while deleting backup %s", backupID)
		}
		storeDeleted, err := ops.deleteFilesFromStream(
			ctx,
			ops.walkSnapshotDataPathsForStoreBackup(ctx, head.StoreID, backupID),
			opts,
		)
		if err != nil {
			return deleted, errors.Annotatef(err, "delete snapshot data files for store %d backup %s", head.StoreID, backupID)
		}
		deleted += storeDeleted
	}
	return deleted, nil
}

func (ops SnapshotOps) walkFilesWithPrefix(ctx context.Context, prefix string) TrySeq[string] {
	return func(yield func(error, string) bool) {
		err := ops.Storage.WalkDir(ctx, &storeapi.WalkOption{SubDir: prefix}, func(filePath string, _ int64) error {
			if yield(nil, filePath) {
				return nil
			}
			return errStopWalkSeq
		})
		if err != nil && !errors.ErrorEqual(err, errStopWalkSeq) {
			yield(errors.Annotatef(err, "walk files with prefix %s", prefix), "")
		}
	}
}

func (ops SnapshotOps) walkSnapshotDataPathsForStoreBackup(
	ctx context.Context,
	storeID uint64,
	backupID BackupID,
) TrySeq[string] {
	return func(yield func(error, string) bool) {
		for err, dataFile := range ops.walkSnapshotDataFilesForStoreBackup(ctx, storeID, backupID) {
			if err != nil {
				yield(errors.Annotatef(err, "walk snapshot data paths for store %d backup %s", storeID, backupID), "")
				return
			}
			if !yield(nil, dataFile.Path) {
				return
			}
		}
	}
}

func (ops SnapshotOps) walkSnapshotStoreHeads(ctx context.Context) TrySeq[SnapshotDataFile] {
	return func(yield func(error, SnapshotDataFile) bool) {
		cursor := ""
		for {
			found := false
			for err, dataFile := range ops.walkSnapshotDataFilesAfter(ctx, cursor) {
				if err != nil {
					yield(errors.Annotatef(err, "walk snapshot data files after %q", cursor), SnapshotDataFile{})
					return
				}
				found = true
				cursor = snapshotStoreEndAnchor(dataFile.StoreID)
				if !yield(nil, dataFile) {
					return
				}
				break
			}
			if !found {
				return
			}
		}
	}
}

func (ops SnapshotOps) walkSnapshotDataFilesForStoreBackup(
	ctx context.Context,
	storeID uint64,
	backupID BackupID,
) TrySeq[SnapshotDataFile] {
	return func(yield func(error, SnapshotDataFile) bool) {
		for err, dataFile := range ops.walkSnapshotDataFilesAfter(ctx, snapshotStoreBackupAnchor(storeID, backupID)) {
			if err != nil {
				yield(
					errors.Annotatef(err, "walk snapshot data files for store %d backup %s", storeID, backupID),
					SnapshotDataFile{},
				)
				return
			}
			if dataFile.StoreID != storeID || dataFile.BackupID != backupID {
				return
			}
			if !yield(nil, dataFile) {
				return
			}
		}
	}
}

func (ops SnapshotOps) walkSnapshotDataFilesAfter(ctx context.Context, startAfter string) TrySeq[SnapshotDataFile] {
	return walkParsedSeq(
		ctx,
		ops.Storage,
		&storeapi.WalkOption{SubDir: snapshotDataRootDir, StartAfter: startAfter},
		ParseSnapshotDataFilePath,
	)
}

func snapshotStoreBackupAnchor(storeID uint64, backupID BackupID) string {
	return SnapshotStoreDataPrefix(storeID, backupID)
}

func snapshotStoreEndAnchor(storeID uint64) string {
	// '/' sorts before digits, so appending '0' skips the entire `storeID/` subtree.
	return snapshotStoreDir(storeID) + "0"
}

func snapshotStoreDir(storeID uint64) string {
	return path.Join(snapshotDataRootDir, strconv.FormatUint(storeID, 10))
}
