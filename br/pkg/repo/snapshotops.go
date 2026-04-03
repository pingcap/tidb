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
	PendingBackupStateStale        PendingBackupState = "stale"
	PendingBackupStateUnfinished   PendingBackupState = "unfinished"
	PendingBackupStateInconsistent PendingBackupState = "inconsistent"
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

func SnapshotOpsExtension(storage storeapi.Storage) SnapshotOps {
	return SnapshotOps{Storage: storage}
}

func (ops SnapshotOps) ListPendingBackups(ctx context.Context) ([]PendingBackup, error) {
	grouped := make(map[BackupID]*PendingBackup)
	for err, marker := range WalkPendingMarkers(ctx, ops.Storage) {
		if err != nil {
			return nil, errors.Trace(err)
		}
		entry, ok := grouped[marker.BackupID]
		if !ok {
			entry = &PendingBackup{BackupID: marker.BackupID}
			grouped[marker.BackupID] = entry
		}
		entry.MarkerPaths = append(entry.MarkerPaths, marker.Path)
	}

	backups := make([]PendingBackup, 0, len(grouped))
	for _, entry := range grouped {
		slices.Sort(entry.MarkerPaths)
		metadataStorage := NewPrefixedStorage(ops.Storage, snapshotMetadataDir(entry.BackupID))
		hasBackupMeta, err := metadataStorage.FileExists(ctx, metautil.MetaFile)
		if err != nil {
			return nil, errors.Trace(err)
		}
		hasCheckpoint, err := metadataStorage.FileExists(ctx, checkpoint.CheckpointMetaPathForBackup)
		if err != nil {
			return nil, errors.Trace(err)
		}
		switch {
		case hasBackupMeta:
			entry.State = PendingBackupStateStale
		case hasCheckpoint:
			entry.State = PendingBackupStateUnfinished
		default:
			entry.State = PendingBackupStateInconsistent
		}
		backups = append(backups, *entry)
	}

	slices.SortFunc(backups, func(a, b PendingBackup) int {
		return cmp.Compare(a.BackupID, b.BackupID)
	})
	return backups, nil
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
			yield(errors.Trace(err), "")
		}
	}

	completedSet := make(map[BackupID]struct{}, len(completed))
	for _, id := range completed {
		completedSet[id] = struct{}{}
	}

	if !ops.supportsRepoStartAfter() {
		return ops.walkSnapshotOrphansByFullScan(ctx, completedSet)
	}

	return func(yield func(error, string) bool) {
		for err, head := range ops.walkSnapshotStoreBackupHeads(ctx) {
			if err != nil {
				yield(errors.Trace(err), "")
				return
			}
			if _, ok := completedSet[head.BackupID]; ok {
				continue
			}
			for err, dataFile := range ops.walkSnapshotDataFilesForStoreBackup(ctx, head.StoreID, head.BackupID) {
				if err != nil {
					yield(errors.Trace(err), "")
					return
				}
				if !yield(nil, dataFile.Path) {
					return
				}
			}
		}
	}
}

func (ops SnapshotOps) walkSnapshotOrphansByFullScan(ctx context.Context, completedSet map[BackupID]struct{}) TrySeq[string] {
	return func(yield func(error, string) bool) {
		for err, dataFile := range WalkSnapshotDataFiles(ctx, ops.Storage) {
			if err != nil {
				yield(errors.Trace(err), "")
				return
			}
			if _, ok := completedSet[dataFile.BackupID]; ok {
				continue
			}
			if !yield(nil, dataFile.Path) {
				return
			}
		}
	}
}

func (ops SnapshotOps) DeleteSnapshotOrphans(ctx context.Context) (int, error) {
	return ops.deleteFilesFromStream(ctx, ops.WalkSnapshotOrphans(ctx))
}

func (ops SnapshotOps) deleteFilesFromStream(ctx context.Context, stream TrySeq[string]) (int, error) {
	const maxConcurrentWork = 128

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(maxConcurrentWork)
	var deleted atomic.Int64
	for err, filePath := range stream {
		if err != nil {
			return 0, errors.Trace(err)
		}
		group.Go(func() error {
			if err := ops.Storage.DeleteFile(groupCtx, filePath); err != nil {
				return errors.Trace(err)
			}
			deleted.Add(1)
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return 0, errors.Trace(err)
	}
	return int(deleted.Load()), nil
}

func (ops SnapshotOps) DeleteSnapshot(ctx context.Context, backupID BackupID) (*SnapshotDeleteResult, error) {
	result := &SnapshotDeleteResult{BackupID: backupID}
	if err := ops.requireRepoStartAfter(); err != nil {
		return nil, errors.Trace(err)
	}

	var err error
	result.MetadataDeleted, err = ops.deletePrefixFiles(ctx, snapshotMetadataDir(backupID))
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.DataDeleted, err = ops.deleteSnapshotDataFilesForBackup(ctx, backupID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.PendingDeleted, err = ops.deletePendingMarkersForBackup(ctx, backupID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

func (ops SnapshotOps) supportsRepoStartAfter() bool {
	uri := strings.ToLower(ops.Storage.URI())
	return strings.HasPrefix(uri, "s3://") || strings.HasPrefix(uri, "ks3://") || strings.HasPrefix(uri, "file://")
}

func (ops SnapshotOps) requireRepoStartAfter() error {
	if ops.supportsRepoStartAfter() {
		return nil
	}
	return errors.Errorf(
		"storage %s does not support WalkDir StartAfter required by repo-v1 snapshot deletion",
		ops.Storage.URI(),
	)
}

func (ops SnapshotOps) DiscardPendingSnapshot(
	ctx context.Context,
	target PendingBackup,
) (*PendingDiscardResult, error) {
	var err error
	result := &PendingDiscardResult{BackupID: target.BackupID}
	switch target.State {
	case PendingBackupStateStale:
		if err := ops.Storage.DeleteFiles(ctx, target.MarkerPaths); err != nil {
			return nil, errors.Trace(err)
		}
		result.PendingDeleted = len(target.MarkerPaths)
		result.StalePending = true
	case PendingBackupStateUnfinished:
		if err := ops.requireRepoStartAfter(); err != nil {
			return nil, errors.Trace(err)
		}
		result.MetadataDeleted, err = ops.deletePrefixFiles(ctx, snapshotMetadataDir(target.BackupID))
		if err != nil {
			return nil, errors.Trace(err)
		}
		result.DataDeleted, err = ops.deleteSnapshotDataFilesForBackup(ctx, target.BackupID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if err := ops.Storage.DeleteFiles(ctx, target.MarkerPaths); err != nil {
			return nil, errors.Trace(err)
		}
		result.PendingDeleted = len(target.MarkerPaths)
	default:
		return nil, errors.Annotatef(
			berrors.ErrInvalidArgument,
			"found inconsistent repo-v1 pending backup %s: pending marker exists but neither %s nor %s was found",
			target.BackupID,
			metautil.MetaFile,
			checkpoint.CheckpointMetaPathForBackup,
		)
	}
	return result, nil
}

func (ops SnapshotOps) deletePendingMarkersForBackup(ctx context.Context, backupID BackupID) (int, error) {
	backups, err := ops.ListPendingBackups(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	for _, pending := range backups {
		if pending.BackupID != backupID {
			continue
		}
		if err := ops.Storage.DeleteFiles(ctx, pending.MarkerPaths); err != nil {
			return 0, errors.Trace(err)
		}
		return len(pending.MarkerPaths), nil
	}
	return 0, nil
}

func (ops SnapshotOps) deletePrefixFiles(ctx context.Context, prefix string) (int, error) {
	return ops.deleteFilesFromStream(ctx, ops.walkFilesWithPrefix(ctx, prefix))
}

func (ops SnapshotOps) deleteSnapshotDataFilesForBackup(ctx context.Context, backupID BackupID) (int, error) {
	deleted := 0
	for err, head := range ops.walkSnapshotStoreHeads(ctx) {
		if err != nil {
			return deleted, errors.Trace(err)
		}
		storeDeleted, err := ops.deleteFilesFromStream(ctx, ops.walkSnapshotDataPathsForStoreBackup(ctx, head.StoreID, backupID))
		if err != nil {
			return deleted, errors.Trace(err)
		}
		deleted += storeDeleted
	}
	return deleted, nil
}

func (ops SnapshotOps) walkFilesWithPrefix(ctx context.Context, prefix string) TrySeq[string] {
	return func(yield func(error, string) bool) {
		if err := ops.Storage.WalkDir(ctx, &storeapi.WalkOption{SubDir: prefix}, func(filePath string, _ int64) error {
			yield(nil, filePath)
			return nil
		}); err != nil {
			yield(errors.Trace(err), "")
		}
	}
}

func (ops SnapshotOps) walkSnapshotDataPathsForStoreBackup(ctx context.Context, storeID uint64, backupID BackupID) TrySeq[string] {
	return func(yield func(error, string) bool) {
		for err, dataFile := range ops.walkSnapshotDataFilesForStoreBackup(ctx, storeID, backupID) {
			if err != nil {
				yield(errors.Trace(err), "")
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
					yield(errors.Trace(err), SnapshotDataFile{})
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
					yield(errors.Trace(err), SnapshotDataFile{})
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

func (ops SnapshotOps) walkSnapshotDataFilesForStoreBackup(ctx context.Context, storeID uint64, backupID BackupID) TrySeq[SnapshotDataFile] {
	return func(yield func(error, SnapshotDataFile) bool) {
		for err, dataFile := range ops.walkSnapshotDataFilesAfter(ctx, snapshotStoreBackupAnchor(storeID, backupID)) {
			if err != nil {
				yield(errors.Trace(err), SnapshotDataFile{})
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
		&storeapi.WalkOption{SubDir: snapshotDataDir(), StartAfter: startAfter},
		ParseSnapshotDataFilePath,
	)
}

func snapshotMetadataDir(backupID BackupID) string {
	return path.Join("_meta", "snapshot", backupID.StorageName())
}

func snapshotDataDir() string {
	return path.Join("_data", "snapshot")
}

func snapshotStoreBackupAnchor(storeID uint64, backupID BackupID) string {
	return path.Join(snapshotStoreDir(storeID), backupID.StorageName())
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
	return path.Join(snapshotDataDir(), strconv.FormatUint(storeID, 10))
}
