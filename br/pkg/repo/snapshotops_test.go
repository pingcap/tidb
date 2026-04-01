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

package repo_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/repo/snapshotpaths"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestListPendingBackupsClassifiesStates(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()

	staleID := repo.BackupID(0x1111)
	unfinishedID := repo.BackupID(0x2222)
	inconsistentID := repo.BackupID(0x3333)

	createPendingMarker(t, ctx, storage, staleID)
	createBackupMeta(t, ctx, storage, staleID)

	createPendingCheckpoint(t, ctx, storage, unfinishedID)
	createPendingMarker(t, ctx, storage, inconsistentID)

	backups, err := repo.ListPendingBackups(ctx, storage)
	require.NoError(t, err)
	require.Len(t, backups, 3)
	require.Equal(t, repo.PendingBackupStateStale, backups[0].State)
	require.Equal(t, staleID, backups[0].BackupID)
	require.Equal(t, repo.PendingBackupStateUnfinished, backups[1].State)
	require.Equal(t, unfinishedID, backups[1].BackupID)
	require.Equal(t, repo.PendingBackupStateInconsistent, backups[2].State)
	require.Equal(t, inconsistentID, backups[2].BackupID)
}

func TestDiscardPendingSnapshotStalePendingRemovesOnlyMarker(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()

	backupID := repo.BackupID(0x1234)
	createPendingMarker(t, ctx, storage, backupID)
	createBackupMeta(t, ctx, storage, backupID)
	createDataFile(t, ctx, storage, 1, backupID, "stale.sst")

	result, err := repo.DiscardPendingSnapshot(ctx, storage, mustFindPendingBackup(t, ctx, storage, backupID))
	require.NoError(t, err)
	require.True(t, result.StalePending)
	require.Equal(t, 1, result.PendingDeleted)
	require.Zero(t, result.MetadataDeleted)
	require.Zero(t, result.DataDeleted)

	requireFileMissing(t, ctx, storage, snapshotpaths.PendingFile([]byte("hash"), backupID))
	requireFileExists(t, ctx, storage, snapshotpaths.MetadataFile(backupID))
	requireFileExists(t, ctx, storage, snapshotpaths.StoreDataPrefix(1, backupID)+"/stale.sst")
}

func TestDiscardPendingSnapshotUnfinishedRemovesMetadataAndData(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()

	backupID := repo.BackupID(0x2345)
	createPendingCheckpoint(t, ctx, storage, backupID)
	createDataFile(t, ctx, storage, 1, backupID, "a.sst")
	createDataFile(t, ctx, storage, 2, backupID, "b.sst")

	result, err := repo.DiscardPendingSnapshot(ctx, storage, mustFindPendingBackup(t, ctx, storage, backupID))
	require.NoError(t, err)
	require.False(t, result.StalePending)
	require.Equal(t, 1, result.MetadataDeleted)
	require.Equal(t, 2, result.DataDeleted)
	require.Equal(t, 1, result.PendingDeleted)

	requireFileMissing(t, ctx, storage, snapshotpaths.PendingFile([]byte("hash"), backupID))
	requireFileMissing(t, ctx, storage, pathJoin(snapshotpaths.MetadataDir(backupID), checkpoint.CheckpointMetaPathForBackup))
	requireFileMissing(t, ctx, storage, snapshotpaths.StoreDataPrefix(1, backupID)+"/a.sst")
	requireFileMissing(t, ctx, storage, snapshotpaths.StoreDataPrefix(2, backupID)+"/b.sst")
}

func TestDiscardPendingSnapshotRejectsInconsistent(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()

	backupID := repo.BackupID(0x3456)
	createPendingMarker(t, ctx, storage, backupID)

	_, err := repo.DiscardPendingSnapshot(ctx, storage, mustFindPendingBackup(t, ctx, storage, backupID))
	require.Error(t, err)
	require.Contains(t, err.Error(), "inconsistent repo-v1 pending backup")
}

func TestDeleteSnapshotWithoutMetadataBatched(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()

	backupID := repo.BackupID(0x4567)
	for i := 0; i < 1025; i++ {
		createDataFile(t, ctx, storage, uint64(i%3+1), backupID, fmt.Sprintf("file-%04d.sst", i))
	}
	createDataFile(t, ctx, storage, 9, repo.BackupID(0x9999), "keep.sst")
	createPendingMarker(t, ctx, storage, backupID)

	result, err := repo.DeleteSnapshot(ctx, storage, backupID)
	require.NoError(t, err)
	require.Zero(t, result.MetadataDeleted)
	require.Equal(t, 1025, result.DataDeleted)
	require.Equal(t, 1, result.PendingDeleted)

	requireFileExists(t, ctx, storage, snapshotpaths.StoreDataPrefix(9, repo.BackupID(0x9999))+"/keep.sst")
	requireFileMissing(t, ctx, storage, snapshotpaths.PendingFile([]byte("hash"), backupID))
}

func TestListAndDeleteSnapshotOrphans(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()

	completedID := repo.BackupID(0x5678)
	orphanID := repo.BackupID(0x6789)
	createBackupMeta(t, ctx, storage, completedID)
	createDataFile(t, ctx, storage, 1, completedID, "keep.sst")
	createDataFile(t, ctx, storage, 1, orphanID, "orphan-a.sst")
	createDataFile(t, ctx, storage, 2, orphanID, "orphan-b.sst")

	backupIDs, err := repo.ListCompletedSnapshotIDs(ctx, storage)
	require.NoError(t, err)
	require.Equal(t, []repo.BackupID{completedID}, backupIDs)

	orphans, err := repo.ListSnapshotOrphans(ctx, storage)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		snapshotpaths.StoreDataPrefix(1, orphanID) + "/orphan-a.sst",
		snapshotpaths.StoreDataPrefix(2, orphanID) + "/orphan-b.sst",
	}, orphans)

	deleted, err := repo.DeleteSnapshotOrphans(ctx, storage)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
	requireFileExists(t, ctx, storage, snapshotpaths.StoreDataPrefix(1, completedID)+"/keep.sst")
	requireFileMissing(t, ctx, storage, snapshotpaths.StoreDataPrefix(1, orphanID)+"/orphan-a.sst")
	requireFileMissing(t, ctx, storage, snapshotpaths.StoreDataPrefix(2, orphanID)+"/orphan-b.sst")
}

func TestWalkSnapshotOrphansStreamsPaths(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()

	completedID := repo.BackupID(0x7777)
	orphanID := repo.BackupID(0x8888)
	createBackupMeta(t, ctx, storage, completedID)
	createDataFile(t, ctx, storage, 1, completedID, "keep.sst")
	createDataFile(t, ctx, storage, 1, orphanID, "orphan.sst")

	var orphanPaths []string
	for orphanPath, err := range repo.WalkSnapshotOrphans(ctx, storage) {
		require.NoError(t, err)
		orphanPaths = append(orphanPaths, orphanPath)
	}
	require.Equal(t, []string{snapshotpaths.StoreDataPrefix(1, orphanID) + "/orphan.sst"}, orphanPaths)
}

func createPendingCheckpoint(t *testing.T, ctx context.Context, storage storeapi.Storage, backupID repo.BackupID) {
	t.Helper()
	createPendingMarker(t, ctx, storage, backupID)
	metadataStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(backupID))
	require.NoError(t, metadataStorage.WriteFile(ctx, checkpoint.CheckpointMetaPathForBackup, []byte("checkpoint")))
}

func createPendingMarker(t *testing.T, ctx context.Context, storage storeapi.Storage, backupID repo.BackupID) {
	t.Helper()
	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile([]byte("hash"), backupID), []byte("{}")))
}

func createBackupMeta(
	t *testing.T,
	ctx context.Context,
	storage storeapi.Storage,
	backupID repo.BackupID,
	options ...func(*backuppb.BackupMeta),
) {
	t.Helper()
	cipherInfo := backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	metaWriter := metautil.NewMetaWriter(
		repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(backupID)),
		metautil.MetaFileSize,
		false,
		"",
		&cipherInfo,
	)
	metaWriter.Update(func(meta *backuppb.BackupMeta) {
		meta.ClusterId = uint64(backupID)
		for _, option := range options {
			option(meta)
		}
	})
	require.NoError(t, metaWriter.FlushBackupMeta(ctx))
}

func createDataFile(t *testing.T, ctx context.Context, storage storeapi.Storage, storeID uint64, backupID repo.BackupID, name string) {
	t.Helper()
	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.StoreDataPrefix(storeID, backupID)+"/"+name, []byte(name)))
}

func mustFindPendingBackup(
	t *testing.T,
	ctx context.Context,
	storage storeapi.Storage,
	backupID repo.BackupID,
) repo.PendingBackup {
	t.Helper()
	backups, err := repo.ListPendingBackups(ctx, storage)
	require.NoError(t, err)
	for _, backup := range backups {
		if backup.BackupID == backupID {
			return backup
		}
	}
	t.Fatalf("pending backup %s not found", backupID)
	return repo.PendingBackup{}
}

func requireFileExists(t *testing.T, ctx context.Context, storage storeapi.Storage, name string) {
	t.Helper()
	exists, err := storage.FileExists(ctx, name)
	require.NoError(t, err)
	require.True(t, exists, name)
}

func requireFileMissing(t *testing.T, ctx context.Context, storage storeapi.Storage, name string) {
	t.Helper()
	exists, err := storage.FileExists(ctx, name)
	require.NoError(t, err)
	require.False(t, exists, name)
}

func pathJoin(parts ...string) string {
	return filepath.ToSlash(filepath.Join(parts...))
}
