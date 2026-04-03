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
	"context"
	"iter"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
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

func ListPendingBackups(ctx context.Context, storage storeapi.Storage) ([]PendingBackup, error) {
	grouped := make(map[BackupID]*PendingBackup)
	for marker, err := range WalkPendingMarkers(ctx, storage) {
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
		metadataStorage := NewPrefixedStorage(storage, snapshotMetadataDir(entry.BackupID))
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
		switch {
		case a.BackupID < b.BackupID:
			return -1
		case a.BackupID > b.BackupID:
			return 1
		default:
			return 0
		}
	})
	return backups, nil
}

func ListSnapshotOrphans(ctx context.Context, storage storeapi.Storage) ([]string, error) {
	result := make([]string, 0)
	for orphanPath, err := range WalkSnapshotOrphans(ctx, storage) {
		if err != nil {
			return nil, errors.Trace(err)
		}
		result = append(result, orphanPath)
	}
	return result, nil
}

func WalkSnapshotOrphans(ctx context.Context, storage storeapi.Storage) iter.Seq2[string, error] {
	completed, err := ListCompletedSnapshotIDs(ctx, storage)
	if err != nil {
		return func(yield func(string, error) bool) {
			yield("", errors.Trace(err))
		}
	}

	completedSet := make(map[BackupID]struct{}, len(completed))
	for _, id := range completed {
		completedSet[id] = struct{}{}
	}

	if !supportsRepoStartAfter(storage) {
		return walkSnapshotOrphansByFullScan(ctx, storage, completedSet)
	}

	return func(yield func(string, error) bool) {
		for head, err := range walkSnapshotStoreBackupHeads(ctx, storage) {
			if err != nil {
				yield("", errors.Trace(err))
				return
			}
			if _, ok := completedSet[head.BackupID]; ok {
				continue
			}
			for dataFile, err := range walkSnapshotDataFilesForStoreBackup(ctx, storage, head.StoreID, head.BackupID) {
				if err != nil {
					yield("", errors.Trace(err))
					return
				}
				if !yield(dataFile.Path, nil) {
					return
				}
			}
		}
	}
}

func walkSnapshotOrphansByFullScan(
	ctx context.Context,
	storage storeapi.Storage,
	completedSet map[BackupID]struct{},
) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for dataFile, err := range WalkSnapshotDataFiles(ctx, storage) {
			if err != nil {
				yield("", errors.Trace(err))
				return
			}
			if _, ok := completedSet[dataFile.BackupID]; ok {
				continue
			}
			if !yield(dataFile.Path, nil) {
				return
			}
		}
	}
}

func DeleteSnapshotOrphans(ctx context.Context, storage storeapi.Storage) (int, error) {
	paths := make([]string, 0)
	for orphanPath, err := range WalkSnapshotOrphans(ctx, storage) {
		if err != nil {
			return 0, errors.Trace(err)
		}
		paths = append(paths, orphanPath)
	}
	return deleteSpecificFiles(ctx, storage, paths)
}

func DeleteSnapshot(ctx context.Context, storage storeapi.Storage, backupID BackupID) (*SnapshotDeleteResult, error) {
	result := &SnapshotDeleteResult{BackupID: backupID}
	if err := requireRepoStartAfter(storage); err != nil {
		return nil, errors.Trace(err)
	}

	var err error
	result.MetadataDeleted, err = deletePrefixFiles(ctx, storage, snapshotMetadataDir(backupID))
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.DataDeleted, err = deleteSnapshotDataFilesForBackup(ctx, storage, backupID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.PendingDeleted, err = deletePendingMarkersForBackup(ctx, storage, backupID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

func supportsRepoStartAfter(storage storeapi.Storage) bool {
	uri := strings.ToLower(storage.URI())
	return strings.HasPrefix(uri, "s3://") || strings.HasPrefix(uri, "ks3://") || strings.HasPrefix(uri, "file://")
}

func requireRepoStartAfter(storage storeapi.Storage) error {
	if supportsRepoStartAfter(storage) {
		return nil
	}
	return errors.Errorf(
		"storage %s does not support WalkDir StartAfter required by repo-v1 snapshot deletion",
		storage.URI(),
	)
}

func DiscardPendingSnapshot(
	ctx context.Context,
	storage storeapi.Storage,
	target PendingBackup,
) (*PendingDiscardResult, error) {
	var err error
	result := &PendingDiscardResult{BackupID: target.BackupID}
	switch target.State {
	case PendingBackupStateStale:
		result.PendingDeleted, err = deleteSpecificFiles(ctx, storage, target.MarkerPaths)
		if err != nil {
			return nil, errors.Trace(err)
		}
		result.StalePending = true
	case PendingBackupStateUnfinished:
		if err := requireRepoStartAfter(storage); err != nil {
			return nil, errors.Trace(err)
		}
		result.MetadataDeleted, err = deletePrefixFiles(ctx, storage, snapshotMetadataDir(target.BackupID))
		if err != nil {
			return nil, errors.Trace(err)
		}
		result.DataDeleted, err = deleteSnapshotDataFilesForBackup(ctx, storage, target.BackupID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		result.PendingDeleted, err = deleteSpecificFiles(ctx, storage, target.MarkerPaths)
		if err != nil {
			return nil, errors.Trace(err)
		}
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

func deletePendingMarkersForBackup(ctx context.Context, storage storeapi.Storage, backupID BackupID) (int, error) {
	backups, err := ListPendingBackups(ctx, storage)
	if err != nil {
		return 0, errors.Trace(err)
	}
	for _, pending := range backups {
		if pending.BackupID != backupID {
			continue
		}
		return deleteSpecificFiles(ctx, storage, pending.MarkerPaths)
	}
	return 0, nil
}

func deletePrefixFiles(ctx context.Context, storage storeapi.Storage, prefix string) (int, error) {
	paths := make([]string, 0)
	err := storage.WalkDir(ctx, &storeapi.WalkOption{SubDir: prefix}, func(filePath string, _ int64) error {
		paths = append(paths, filePath)
		return nil
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	return deleteSpecificFiles(ctx, storage, paths)
}

func deleteSpecificFiles(ctx context.Context, storage storeapi.Storage, paths []string) (int, error) {
	if len(paths) == 0 {
		return 0, nil
	}
	if err := storage.DeleteFiles(ctx, paths); err != nil {
		return 0, errors.Trace(err)
	}
	return len(paths), nil
}

func deleteSnapshotDataFilesForBackup(ctx context.Context, storage storeapi.Storage, backupID BackupID) (int, error) {
	deleted := 0
	for head, err := range walkSnapshotStoreHeads(ctx, storage) {
		if err != nil {
			return deleted, errors.Trace(err)
		}
		storeDeleted, err := deleteSnapshotDataFilesForStoreBackup(ctx, storage, head.StoreID, backupID)
		if err != nil {
			return deleted, errors.Trace(err)
		}
		deleted += storeDeleted
	}
	return deleted, nil
}

func deleteSnapshotDataFilesForStoreBackup(
	ctx context.Context,
	storage storeapi.Storage,
	storeID uint64,
	backupID BackupID,
) (int, error) {
	paths := make([]string, 0)
	for dataFile, err := range walkSnapshotDataFilesForStoreBackup(ctx, storage, storeID, backupID) {
		if err != nil {
			return 0, errors.Trace(err)
		}
		paths = append(paths, dataFile.Path)
	}
	return deleteSpecificFiles(ctx, storage, paths)
}

func walkSnapshotStoreHeads(ctx context.Context, storage storeapi.Storage) iter.Seq2[SnapshotDataFile, error] {
	return func(yield func(SnapshotDataFile, error) bool) {
		cursor := ""
		for {
			found := false
			for dataFile, err := range walkSnapshotDataFilesAfter(ctx, storage, cursor) {
				if err != nil {
					yield(SnapshotDataFile{}, errors.Trace(err))
					return
				}
				found = true
				cursor = snapshotStoreEndAnchor(dataFile.StoreID)
				if !yield(dataFile, nil) {
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

func walkSnapshotStoreBackupHeads(ctx context.Context, storage storeapi.Storage) iter.Seq2[SnapshotDataFile, error] {
	return func(yield func(SnapshotDataFile, error) bool) {
		cursor := ""
		for {
			found := false
			for dataFile, err := range walkSnapshotDataFilesAfter(ctx, storage, cursor) {
				if err != nil {
					yield(SnapshotDataFile{}, errors.Trace(err))
					return
				}
				found = true
				cursor = snapshotStoreBackupEndAnchor(dataFile.StoreID, dataFile.BackupID)
				if !yield(dataFile, nil) {
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

func walkSnapshotDataFilesForStoreBackup(
	ctx context.Context,
	storage storeapi.Storage,
	storeID uint64,
	backupID BackupID,
) iter.Seq2[SnapshotDataFile, error] {
	return func(yield func(SnapshotDataFile, error) bool) {
		for dataFile, err := range walkSnapshotDataFilesAfter(ctx, storage, snapshotStoreBackupAnchor(storeID, backupID)) {
			if err != nil {
				yield(SnapshotDataFile{}, errors.Trace(err))
				return
			}
			if dataFile.StoreID != storeID || dataFile.BackupID != backupID {
				return
			}
			if !yield(dataFile, nil) {
				return
			}
		}
	}
}

func walkSnapshotDataFilesAfter(
	ctx context.Context,
	storage storeapi.Storage,
	startAfter string,
) iter.Seq2[SnapshotDataFile, error] {
	return walkParsedSeq(
		ctx,
		storage,
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
