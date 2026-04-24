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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestListPendingBackupsClassifiesStates(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	snapshotOps := repo.SnapshotOpsExtension(storage)

	staleID := repo.BackupID(0x1111)
	unfinishedID := repo.BackupID(0x2222)
	markerOnlyID := repo.BackupID(0x3333)

	createPendingMarker(ctx, t, storage, staleID)
	createBackupMeta(ctx, t, storage, staleID)

	createPendingCheckpoint(ctx, t, storage, unfinishedID)
	createPendingMarker(ctx, t, storage, markerOnlyID)

	backups, err := snapshotOps.ListPendingBackups(ctx)
	require.NoError(t, err)
	require.Len(t, backups, 3)
	require.Equal(t, repo.PendingBackupStateStale, backups[0].State)
	require.Equal(t, staleID, backups[0].BackupID)
	require.Equal(t, repo.PendingBackupStateUnfinished, backups[1].State)
	require.Equal(t, unfinishedID, backups[1].BackupID)
	require.Equal(t, repo.PendingBackupStateStale, backups[2].State)
	require.Equal(t, markerOnlyID, backups[2].BackupID)
}

func TestDiscardPendingSnapshotStalePendingRemovesOnlyMarker(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	snapshotOps := repo.SnapshotOpsExtension(storage)

	backupID := repo.BackupID(0x1234)
	createPendingMarker(ctx, t, storage, backupID)
	createBackupMeta(ctx, t, storage, backupID)
	createDataFile(ctx, t, storage, 1, backupID, "stale.sst")

	result, err := snapshotOps.DiscardPendingSnapshot(ctx, mustFindPendingBackup(ctx, t, storage, backupID))
	require.NoError(t, err)
	require.True(t, result.StalePending)
	require.Equal(t, 1, result.PendingDeleted)
	require.Zero(t, result.MetadataDeleted)
	require.Zero(t, result.DataDeleted)

	requireFileMissing(ctx, t, storage, repo.PendingFile([]byte("hash"), backupID))
	requireFileExists(ctx, t, storage, repo.SnapshotMetadataFile(backupID))
	requireFileExists(ctx, t, storage, repo.SnapshotStoreDataPrefix(1, backupID)+"/stale.sst")
}

func TestDiscardPendingSnapshotUnfinishedRemovesMetadataAndData(t *testing.T) {
	ctx := context.Background()
	storage := newWalkAssertingLocalStorage(t, true)
	snapshotOps := repo.SnapshotOpsExtension(storage)

	backupID := repo.BackupID(0x2345)
	createPendingCheckpoint(ctx, t, storage, backupID)
	createDataFile(ctx, t, storage, 1, backupID, "a.sst")
	createDataFile(ctx, t, storage, 2, backupID, "b.sst")
	createDataFile(ctx, t, storage, 9, repo.BackupID(0x9999), "keep.sst")

	result, err := snapshotOps.DiscardPendingSnapshot(ctx, mustFindPendingBackup(ctx, t, storage, backupID))
	require.NoError(t, err)
	require.False(t, result.StalePending)
	require.Equal(t, 1, result.MetadataDeleted)
	require.Equal(t, 2, result.DataDeleted)
	require.Equal(t, 1, result.PendingDeleted)

	requireFileMissing(ctx, t, storage, repo.PendingFile([]byte("hash"), backupID))
	requireFileMissing(ctx, t, storage, pathJoin(repo.SnapshotMetadataDir(backupID), checkpoint.CheckpointMetaPathForBackup))
	requireFileMissing(ctx, t, storage, repo.SnapshotStoreDataPrefix(1, backupID)+"/a.sst")
	requireFileMissing(ctx, t, storage, repo.SnapshotStoreDataPrefix(2, backupID)+"/b.sst")
	requireFileExists(ctx, t, storage, repo.SnapshotStoreDataPrefix(9, repo.BackupID(0x9999))+"/keep.sst")
}

func TestDiscardPendingSnapshotUnfinishedRejectsUnsupportedStorage(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	snapshotOps := repo.SnapshotOpsExtension(storage)

	backupID := repo.BackupID(0x2456)
	createPendingCheckpoint(ctx, t, storage, backupID)
	createDataFile(ctx, t, storage, 1, backupID, "a.sst")

	result, err := snapshotOps.DiscardPendingSnapshot(ctx, mustFindPendingBackup(ctx, t, storage, backupID))
	require.Error(t, err)
	require.ErrorContains(t, err, "repo-v1 snapshot operations")
	require.True(t, errors.ErrorEqual(err, berrors.ErrUnsupportedOperation))
	require.Equal(t, backupID, result.BackupID)
	require.Zero(t, result.MetadataDeleted)
	require.Zero(t, result.DataDeleted)
	require.Zero(t, result.PendingDeleted)
	requireFileExists(ctx, t, storage, repo.PendingFile([]byte("hash"), backupID))
	requireFileExists(ctx, t, storage, pathJoin(repo.SnapshotMetadataDir(backupID), checkpoint.CheckpointMetaPathForBackup))
	requireFileExists(ctx, t, storage, repo.SnapshotStoreDataPrefix(1, backupID)+"/a.sst")
}

func TestDiscardPendingSnapshotRejectsTransient(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	snapshotOps := repo.SnapshotOpsExtension(storage)

	backupID := repo.BackupID(0x3456)
	createPendingMarker(ctx, t, storage, backupID)
	metadataStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID))
	require.NoError(t, metadataStorage.WriteFile(ctx, checkpoint.CheckpointLockPathForBackup, []byte("lock")))
	require.NoError(t, metadataStorage.WriteFile(ctx, checkpoint.CheckpointDataDirForBackup+"/partial.cpt", []byte("data")))

	result, err := snapshotOps.DiscardPendingSnapshot(ctx, mustFindPendingBackup(ctx, t, storage, backupID))
	require.NoError(t, err)
	require.True(t, result.StalePending)
	require.Equal(t, 1, result.PendingDeleted)
	require.Zero(t, result.MetadataDeleted)
	require.Zero(t, result.DataDeleted)
	requireFileMissing(ctx, t, storage, repo.PendingFile([]byte("hash"), backupID))
	requireFileMissing(ctx, t, storage, pathJoin(repo.SnapshotMetadataDir(backupID), checkpoint.CheckpointLockPathForBackup))
	requireFileMissing(ctx, t, storage, pathJoin(repo.SnapshotMetadataDir(backupID), checkpoint.CheckpointDataDirForBackup+"/partial.cpt"))
}

func TestDeleteSnapshotWithoutMetadataUsesStartAfter(t *testing.T) {
	ctx := context.Background()
	storage := newWalkAssertingLocalStorage(t, true)
	snapshotOps := repo.SnapshotOpsExtension(storage)

	backupID := repo.BackupID(0x4567)
	for i := 0; i < 1025; i++ {
		createDataFile(ctx, t, storage, uint64(i%3+1), backupID, fmt.Sprintf("file-%04d.sst", i))
	}
	createDataFile(ctx, t, storage, 9, repo.BackupID(0x9999), "keep.sst")
	createDataFile(ctx, t, storage, 1, repo.BackupID(0xFFFF), "keep-other.sst")
	createPendingMarker(ctx, t, storage, backupID)

	result, err := snapshotOps.DeleteSnapshot(ctx, backupID)
	require.NoError(t, err)
	require.Zero(t, result.MetadataDeleted)
	require.Equal(t, 1025, result.DataDeleted)
	require.Equal(t, 1, result.PendingDeleted)

	requireFileExists(ctx, t, storage, repo.SnapshotStoreDataPrefix(9, repo.BackupID(0x9999))+"/keep.sst")
	requireFileExists(ctx, t, storage, repo.SnapshotStoreDataPrefix(1, repo.BackupID(0xFFFF))+"/keep-other.sst")
	requireFileMissing(ctx, t, storage, repo.PendingFile([]byte("hash"), backupID))
}

func TestDeleteSnapshotRejectsUnsupportedStorage(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	snapshotOps := repo.SnapshotOpsExtension(storage)

	backupID := repo.BackupID(0x6789)
	createBackupMeta(ctx, t, storage, backupID)
	createDataFile(ctx, t, storage, 1, backupID, "a.sst")
	createPendingMarker(ctx, t, storage, backupID)

	result, err := snapshotOps.DeleteSnapshot(ctx, backupID)
	require.Error(t, err)
	require.ErrorContains(t, err, "repo-v1 snapshot operations")
	require.True(t, errors.ErrorEqual(err, berrors.ErrUnsupportedOperation))
	require.Equal(t, backupID, result.BackupID)
	require.Zero(t, result.MetadataDeleted)
	require.Zero(t, result.DataDeleted)
	require.Zero(t, result.PendingDeleted)
	requireFileExists(ctx, t, storage, repo.SnapshotMetadataFile(backupID))
	requireFileExists(ctx, t, storage, repo.SnapshotStoreDataPrefix(1, backupID)+"/a.sst")
	requireFileExists(ctx, t, storage, repo.PendingFile([]byte("hash"), backupID))
}

func TestListAndDeleteSnapshotOrphansUsesStartAfterWhenAvailable(t *testing.T) {
	ctx := context.Background()
	storage := newWalkAssertingLocalStorage(t, true)
	snapshotOps := repo.SnapshotOpsExtension(storage)

	completedID := repo.BackupID(0x5678)
	orphanID := repo.BackupID(0x6789)
	createBackupMeta(ctx, t, storage, completedID)
	createDataFile(ctx, t, storage, 1, completedID, "keep-a.sst")
	createDataFile(ctx, t, storage, 1, completedID, "keep-b.sst")
	createDataFile(ctx, t, storage, 1, orphanID, "orphan-a.sst")
	createDataFile(ctx, t, storage, 2, orphanID, "orphan-b.sst")

	orphans, err := snapshotOps.ListSnapshotOrphans(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		repo.SnapshotStoreDataPrefix(1, orphanID) + "/orphan-a.sst",
		repo.SnapshotStoreDataPrefix(2, orphanID) + "/orphan-b.sst",
	}, orphans)

	deleted, err := snapshotOps.DeleteSnapshotOrphans(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
	requireFileExists(ctx, t, storage, repo.SnapshotStoreDataPrefix(1, completedID)+"/keep-a.sst")
	requireFileExists(ctx, t, storage, repo.SnapshotStoreDataPrefix(1, completedID)+"/keep-b.sst")
	requireFileMissing(ctx, t, storage, repo.SnapshotStoreDataPrefix(1, orphanID)+"/orphan-a.sst")
	requireFileMissing(ctx, t, storage, repo.SnapshotStoreDataPrefix(2, orphanID)+"/orphan-b.sst")
}

func TestDeleteSnapshotWaitsForStartedDeletesAfterWalkError(t *testing.T) {
	ctx := context.Background()
	storage := newWalkErrorDeleteBlockingStorage(t)
	snapshotOps := repo.SnapshotOpsExtension(storage)

	backupID := repo.BackupID(0x7ABC)
	createBackupMeta(ctx, t, storage, backupID)
	storage.failSubDir = pathJoin("_meta", "snapshot", backupID.StorageName())
	storage.failPath = repo.SnapshotMetadataFile(backupID)

	var (
		result repo.SnapshotDeleteResult
		err    error
	)
	deleteDone := make(chan struct{})
	go func() {
		defer close(deleteDone)
		result, err = snapshotOps.DeleteSnapshot(ctx, backupID)
	}()

	select {
	case <-storage.deleteStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("delete worker did not start")
	}

	select {
	case <-deleteDone:
		t.Fatalf("DeleteSnapshot returned before in-flight delete finished: result=%v err=%v", result, err)
	case <-time.After(100 * time.Millisecond):
	}

	close(storage.releaseDelete)
	<-deleteDone

	require.Error(t, err)
	require.ErrorContains(t, err, "injected walk error")
	require.Equal(t, 1, result.MetadataDeleted)
}

func TestDeleteSnapshotStopsWalkingAfterDeleteError(t *testing.T) {
	ctx := context.Background()
	backupID := repo.BackupID(0x7ABD)
	prefix := repo.SnapshotMetadataDir(backupID)
	paths := []string{
		pathJoin(prefix, "meta-1"),
		pathJoin(prefix, "meta-2"),
		pathJoin(prefix, "meta-3"),
	}
	storage := newStopAfterDeleteErrorStorage(t, prefix, paths)
	snapshotOps := repo.SnapshotOpsExtension(storage)

	result, err := snapshotOps.DeleteSnapshot(ctx, backupID)
	require.Error(t, err)
	require.ErrorContains(t, err, paths[0])
	require.Equal(t, backupID, result.BackupID)
	require.Zero(t, result.MetadataDeleted)
	require.Equal(t, int32(2), storage.walked.Load())
}

func TestDiscardPendingSnapshotReportsPartialMarkerDeletion(t *testing.T) {
	ctx := context.Background()
	storage := newMarkerDeletePartialFailureStorage()
	snapshotOps := repo.SnapshotOpsExtension(storage)

	backupID := repo.BackupID(0x7ABD)
	firstMarker := repo.PendingFile([]byte("hash-a"), backupID)
	secondMarker := repo.PendingFile([]byte("hash-b"), backupID)
	require.NoError(t, storage.WriteFile(ctx, firstMarker, []byte("{}")))
	require.NoError(t, storage.WriteFile(ctx, secondMarker, []byte("{}")))
	storage.firstPath = firstMarker
	storage.secondPath = secondMarker

	var deletedCallbacks atomic.Int64
	result, err := snapshotOps.DiscardPendingSnapshot(
		ctx,
		repo.PendingBackup{
			BackupID:    backupID,
			MarkerPaths: []string{firstMarker, secondMarker},
			State:       repo.PendingBackupStateStale,
		},
		repo.WithMutationDeletedProgress(func(count int) {
			deletedCallbacks.Add(int64(count))
		}),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, secondMarker)
	require.Equal(t, 1, result.PendingDeleted)
	require.Equal(t, int64(1), deletedCallbacks.Load())
	require.True(t, result.StalePending)
	requireFileMissing(ctx, t, storage, firstMarker)
	requireFileExists(ctx, t, storage, secondMarker)
}

func createPendingCheckpoint(ctx context.Context, t *testing.T, storage storeapi.Storage, backupID repo.BackupID) {
	t.Helper()
	createPendingMarker(ctx, t, storage, backupID)
	metadataStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID))
	require.NoError(t, metadataStorage.WriteFile(ctx, checkpoint.CheckpointMetaPathForBackup, []byte("checkpoint")))
}

func createPendingMarker(ctx context.Context, t *testing.T, storage storeapi.Storage, backupID repo.BackupID) {
	t.Helper()
	require.NoError(t, storage.WriteFile(ctx, repo.PendingFile([]byte("hash"), backupID), []byte("{}")))
}

func createBackupMeta(
	ctx context.Context,
	t *testing.T,
	storage storeapi.Storage,
	backupID repo.BackupID,
	options ...func(*backuppb.BackupMeta),
) {
	t.Helper()
	cipherInfo := backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	metaWriter := metautil.NewMetaWriter(
		repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID)),
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

func createDataFile(ctx context.Context, t *testing.T, storage storeapi.Storage, storeID uint64, backupID repo.BackupID, name string) {
	t.Helper()
	require.NoError(t, storage.WriteFile(ctx, repo.SnapshotStoreDataPrefix(storeID, backupID)+"/"+name, []byte(name)))
}

func mustFindPendingBackup(
	ctx context.Context,
	t *testing.T,
	storage storeapi.Storage,
	backupID repo.BackupID,
) repo.PendingBackup {
	t.Helper()
	backups, err := repo.SnapshotOpsExtension(storage).ListPendingBackups(ctx)
	require.NoError(t, err)
	for _, backup := range backups {
		if backup.BackupID == backupID {
			return backup
		}
	}
	t.Fatalf("pending backup %s not found", backupID)
	return repo.PendingBackup{}
}

func requireFileExists(ctx context.Context, t *testing.T, storage storeapi.Storage, name string) {
	t.Helper()
	exists, err := storage.FileExists(ctx, name)
	require.NoError(t, err)
	require.True(t, exists, name)
}

func requireFileMissing(ctx context.Context, t *testing.T, storage storeapi.Storage, name string) {
	t.Helper()
	exists, err := storage.FileExists(ctx, name)
	require.NoError(t, err)
	require.False(t, exists, name)
}

func pathJoin(parts ...string) string {
	return filepath.ToSlash(filepath.Join(parts...))
}

type walkAssertingLocalStorage struct {
	storeapi.Storage
	rejectEmptySnapshotWalkAfterFirst bool
}

func newWalkAssertingLocalStorage(t *testing.T, rejectEmptySnapshotWalkAfterFirst bool) *walkAssertingLocalStorage {
	t.Helper()
	storage, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	return &walkAssertingLocalStorage{
		Storage:                           storage,
		rejectEmptySnapshotWalkAfterFirst: rejectEmptySnapshotWalkAfterFirst,
	}
}

func (s *walkAssertingLocalStorage) WalkDir(
	ctx context.Context,
	opt *storeapi.WalkOption,
	fn func(string, int64) error,
) error {
	emitted := 0
	return s.Storage.WalkDir(ctx, opt, func(path string, size int64) error {
		emitted++
		if s.rejectEmptySnapshotWalkAfterFirst &&
			opt != nil &&
			opt.SubDir == snapshotDataRoot() &&
			opt.StartAfter == "" &&
			emitted > 1 {
			return fmt.Errorf("unexpected full scan of %s", opt.SubDir)
		}
		return fn(path, size)
	})
}

type walkErrorDeleteBlockingStorage struct {
	*walkAssertingLocalStorage
	failSubDir    string
	failPath      string
	deleteStarted chan struct{}
	releaseDelete chan struct{}
}

type stopAfterDeleteErrorStorage struct {
	*walkAssertingLocalStorage
	prefix              string
	paths               []string
	firstDeleteReturned chan struct{}
	walked              atomic.Int32
}

func newWalkErrorDeleteBlockingStorage(t *testing.T) *walkErrorDeleteBlockingStorage {
	t.Helper()
	return &walkErrorDeleteBlockingStorage{
		walkAssertingLocalStorage: newWalkAssertingLocalStorage(t, false),
		deleteStarted:             make(chan struct{}, 1),
		releaseDelete:             make(chan struct{}),
	}
}

func newStopAfterDeleteErrorStorage(t *testing.T, prefix string, paths []string) *stopAfterDeleteErrorStorage {
	t.Helper()
	return &stopAfterDeleteErrorStorage{
		walkAssertingLocalStorage: newWalkAssertingLocalStorage(t, false),
		prefix:                    prefix,
		paths:                     append([]string(nil), paths...),
		firstDeleteReturned:       make(chan struct{}),
	}
}

func (s *walkErrorDeleteBlockingStorage) WalkDir(
	ctx context.Context,
	opt *storeapi.WalkOption,
	fn func(string, int64) error,
) error {
	if opt != nil && opt.SubDir == s.failSubDir {
		if err := fn(s.failPath, 1); err != nil {
			return err
		}
		return fmt.Errorf("injected walk error for %s", opt.SubDir)
	}
	return s.walkAssertingLocalStorage.WalkDir(ctx, opt, fn)
}

func (s *walkErrorDeleteBlockingStorage) DeleteFile(ctx context.Context, name string) error {
	if name == s.failPath {
		select {
		case s.deleteStarted <- struct{}{}:
		default:
		}
		<-s.releaseDelete
	}
	return s.Storage.DeleteFile(ctx, name)
}

func (s *stopAfterDeleteErrorStorage) WalkDir(
	ctx context.Context,
	opt *storeapi.WalkOption,
	fn func(string, int64) error,
) error {
	if opt != nil && opt.SubDir == s.prefix {
		for i, path := range s.paths {
			if i == 1 {
				select {
				case <-s.firstDeleteReturned:
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
					return fmt.Errorf("timed out waiting for failed delete of %s", s.paths[0])
				}
				time.Sleep(10 * time.Millisecond)
			}
			s.walked.Add(1)
			if err := fn(path, 1); err != nil {
				return err
			}
		}
		return nil
	}
	return s.walkAssertingLocalStorage.WalkDir(ctx, opt, fn)
}

func (s *stopAfterDeleteErrorStorage) DeleteFile(_ context.Context, name string) error {
	if len(s.paths) > 0 && name == s.paths[0] {
		close(s.firstDeleteReturned)
		return fmt.Errorf("failed to delete file %s", name)
	}
	return nil
}

type markerDeletePartialFailureStorage struct {
	storeapi.Storage
	firstPath   string
	secondPath  string
	firstDelete chan struct{}
}

func newMarkerDeletePartialFailureStorage() *markerDeletePartialFailureStorage {
	return &markerDeletePartialFailureStorage{
		Storage:     objstore.NewMemStorage(),
		firstDelete: make(chan struct{}, 1),
	}
}

func (s *markerDeletePartialFailureStorage) DeleteFile(ctx context.Context, name string) error {
	switch name {
	case s.firstPath:
		err := s.Storage.DeleteFile(ctx, name)
		if err == nil {
			select {
			case s.firstDelete <- struct{}{}:
			default:
			}
		}
		return err
	case s.secondPath:
		<-s.firstDelete
		return fmt.Errorf("failed to delete file %s", name)
	default:
		return s.Storage.DeleteFile(ctx, name)
	}
}

func snapshotDataRoot() string {
	return pathJoin("_data", "snapshot")
}
