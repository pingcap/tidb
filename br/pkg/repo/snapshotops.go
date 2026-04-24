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
	"strings"
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

type PendingDiscardResult struct {
	BackupID        BackupID
	MetadataDeleted int
	DataDeleted     int
	PendingDeleted  int
	StalePending    bool
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

// WithMutationDiscoveredProgress reports discovered object counts while a repo
// snapshot mutation runs. The callback receives the delta for the current event.
func WithMutationDiscoveredProgress(onDiscover func(int)) SnapshotMutationOption {
	return func(opts *snapshotMutationOptions) {
		opts.onDiscover = onDiscover
	}
}

// WithMutationDeletedProgress reports deleted object counts while a repo
// snapshot mutation runs. The callback receives the delta for the current event.
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

func SnapshotOpsExtension(storage storeapi.Storage) SnapshotOps {
	return SnapshotOps{Storage: storage}
}

func (ops SnapshotOps) ListPendingBackups(ctx context.Context) ([]PendingBackup, error) {
	return listPendingBackups(ctx, ops.Storage, WalkPendingMarkers(ctx, ops.Storage))
}

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

func (ops SnapshotOps) ListSnapshotOrphans(ctx context.Context) ([]string, error) {
	result := make([]string, 0)
	for err, orphanPath := range ops.WalkSnapshotOrphans(ctx) {
		if err != nil {
			return nil, errors.Trace(err)
		}
		result = append(result, orphanPath)
	}
	return result, nil
}

func (ops SnapshotOps) WalkSnapshotOrphans(ctx context.Context) TrySeq[string] {
	completed, err := ListCompletedSnapshotIDs(ctx, ops.Storage)
	if err != nil {
		return func(yield func(error, string) bool) {
			yield(errors.Annotatef(err, "list completed snapshot IDs"), "")
		}
	}

	completedSet := make(map[BackupID]struct{}, len(completed))
	for _, id := range completed {
		completedSet[id] = struct{}{}
	}

	if err := ops.requireRepoStartAfter(); err != nil {
		return func(yield func(error, string) bool) {
			yield(errors.Trace(err), "")
		}
	}

	return func(yield func(error, string) bool) {
		for err, head := range ops.walkSnapshotStoreBackupHeads(ctx) {
			if err != nil {
				yield(errors.Annotatef(err, "walk snapshot store backup heads"), "")
				return
			}
			if _, ok := completedSet[head.BackupID]; ok {
				continue
			}
			for err, dataFile := range ops.walkSnapshotDataFilesForStoreBackup(ctx, head.StoreID, head.BackupID) {
				if err != nil {
					yield(errors.Annotatef(
						err,
						"walk snapshot data files for store %d backup %s",
						head.StoreID,
						head.BackupID,
					), "")
					return
				}
				if !yield(nil, dataFile.Path) {
					return
				}
			}
		}
	}
}

func (ops SnapshotOps) DeleteSnapshotOrphans(ctx context.Context, options ...SnapshotMutationOption) (int, error) {
	return ops.deleteFilesFromStream(ctx, ops.WalkSnapshotOrphans(ctx), collectSnapshotMutationOptions(options))
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

	var err error
	result.MetadataDeleted, err = ops.deletePrefixFiles(ctx, SnapshotMetadataDir(backupID), opts)
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
	return result, nil
}

var repoStartAfterSchemes = [...]string{
	"s3://",
	"ks3://",
	"gcs://",
	"file://",
}

func supportsRepoStartAfter(storage storeapi.Storage) bool {
	uri := strings.ToLower(storage.URI())
	for _, scheme := range repoStartAfterSchemes {
		if strings.HasPrefix(uri, scheme) {
			return true
		}
	}
	return false
}

// ValidateRepoV1StartAfterSupport checks whether the resolved storage exposes
// WalkDir StartAfter, which repo-v1 snapshot operations use to scan one backup's
// data files without rewalking unrelated snapshot objects.
func ValidateRepoV1StartAfterSupport(storage storeapi.Storage) error {
	if supportsRepoStartAfter(storage) {
		return nil
	}
	return errors.Annotatef(
		berrors.ErrUnsupportedOperation,
		"storage %s does not support WalkDir StartAfter required by repo-v1 snapshot operations",
		storage.URI(),
	)
}

func (ops SnapshotOps) requireRepoStartAfter() error {
	return ValidateRepoV1StartAfterSupport(ops.Storage)
}

func (ops SnapshotOps) DiscardPendingSnapshot(
	ctx context.Context,
	target PendingBackup,
	options ...SnapshotMutationOption,
) (PendingDiscardResult, error) {
	opts := collectSnapshotMutationOptions(options)
	var err error
	result := PendingDiscardResult{BackupID: target.BackupID}
	switch target.State {
	case PendingBackupStateStale:
		result.StalePending = true
		metadataStorage := NewPrefixedStorage(ops.Storage, SnapshotMetadataDir(target.BackupID))
		if err := checkpoint.RemoveCheckpointDataForBackup(ctx, metadataStorage); err != nil {
			return result, errors.Annotatef(err, "delete checkpoint files for stale backup %s", target.BackupID)
		}
		result.PendingDeleted, err = ops.deleteFilesFromStream(ctx, filePathStream(target.MarkerPaths), opts)
		if err != nil {
			return result, errors.Annotatef(err, "delete pending markers for stale backup %s", target.BackupID)
		}
	case PendingBackupStateUnfinished:
		if err := ops.requireRepoStartAfter(); err != nil {
			return result, errors.Annotatef(err, "discard unfinished pending snapshot %s", target.BackupID)
		}
		result.MetadataDeleted, err = ops.deletePrefixFiles(ctx, SnapshotMetadataDir(target.BackupID), opts)
		if err != nil {
			return result, errors.Annotatef(err, "delete snapshot metadata for unfinished backup %s", target.BackupID)
		}
		result.DataDeleted, err = ops.deleteSnapshotDataFilesForBackup(ctx, target.BackupID, opts)
		if err != nil {
			return result, errors.Annotatef(err, "delete snapshot data files for unfinished backup %s", target.BackupID)
		}
		result.PendingDeleted, err = ops.deleteFilesFromStream(ctx, filePathStream(target.MarkerPaths), opts)
		if err != nil {
			return result, errors.Annotatef(err, "delete pending markers for unfinished backup %s", target.BackupID)
		}
	default:
		return result, errors.Annotatef(berrors.ErrInvalidArgument, "unknown pending backup state %q", target.State)
	}
	return result, nil
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

func (ops SnapshotOps) deletePrefixFiles(
	ctx context.Context,
	prefix string,
	opts snapshotMutationOptions,
) (int, error) {
	return ops.deleteFilesFromStream(ctx, ops.walkFilesWithPrefix(ctx, prefix), opts)
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

func (ops SnapshotOps) walkSnapshotStoreBackupHeads(ctx context.Context) TrySeq[SnapshotDataFile] {
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
				cursor = snapshotStoreBackupEndAnchor(dataFile.StoreID, dataFile.BackupID)
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

func snapshotStoreBackupEndAnchor(storeID uint64, backupID BackupID) string {
	// '/' sorts before digits, so appending '0' skips the entire `backupID/` subtree.
	return snapshotStoreBackupAnchor(storeID, backupID) + "0"
}

func snapshotStoreEndAnchor(storeID uint64) string {
	// '/' sorts before digits, so appending '0' skips the entire `storeID/` subtree.
	return snapshotStoreDir(storeID) + "0"
}

func snapshotStoreDir(storeID uint64) string {
	return path.Join(snapshotDataRootDir, strconv.FormatUint(storeID, 10))
}
