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

package taskrepo

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRewriteDataBackendForStore(t *testing.T) {
	local := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	err := rewriteDataBackendForStore(local, 123, repo.BackupID(0xf00d))
	require.NoError(t, err)
	require.Equal(t, "/tmp/repo/_data/snapshot/123/000000000000F00D", local.GetLocal().Path)

	s3 := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_S3{S3: &backuppb.S3{Bucket: "bucket", Prefix: "root"}},
	}
	err = rewriteDataBackendForStore(s3, 7, repo.BackupID(0xbeef))
	require.NoError(t, err)
	require.Equal(t, "root/_data/snapshot/7/000000000000BEEF", s3.GetS3().Prefix)

	s3.GetS3().Prefix = "root/../base"
	err = rewriteDataBackendForStore(s3, 7, repo.BackupID(0xbeef))
	require.NoError(t, err)
	require.Equal(t, "root/../base/_data/snapshot/7/000000000000BEEF", s3.GetS3().Prefix)
}

func TestPreparedRepoSnapshotBackupRewritesStoreRequestAndResponse(t *testing.T) {
	baseDir := t.TempDir()
	prepared := &PreparedRepoSnapshotBackup{
		SnapshotStorageRef: SnapshotStorageRef{BackupID: repo.BackupID(0x1234)},
	}
	request := backuppb.BackupRequest{
		StorageBackend: &backuppb.StorageBackend{
			Backend: &backuppb.StorageBackend_Local{
				Local: &backuppb.Local{Path: baseDir},
			},
		},
	}

	require.NoError(t, prepared.RewriteStoreRequest(7, &request))
	expectedDataDir := filepath.Join(baseDir, "_data", "snapshot", "7", "0000000000001234")
	require.Equal(t, expectedDataDir, request.GetStorageBackend().GetLocal().Path)
	info, err := os.Stat(expectedDataDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	files, err := prepared.RewriteStoreResponseFiles(7, []*backuppb.File{{Name: "file.sst"}})
	require.NoError(t, err)
	require.Equal(t, "_data/snapshot/7/0000000000001234/file.sst", files[0].Name)
}

func TestCollectResumablePendingBackups(t *testing.T) {
	t.Run("stale_markers_removed_and_unfinished_retained", func(t *testing.T) {
		ctx := context.Background()
		storage := newSnapshotRepoTestStorage(t)
		cfgHash := []byte("hash")
		staleID := repo.BackupID(1)
		unfinishedID := repo.BackupID(2)

		require.NoError(t, storage.WriteFile(ctx, repo.PendingFile(cfgHash, staleID), []byte("{}")))
		require.NoError(t, storage.WriteFile(ctx, repo.PendingFile(cfgHash, unfinishedID), []byte("{}")))

		staleMetaStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(staleID))
		require.NoError(t, staleMetaStorage.WriteFile(ctx, metautil.MetaFile, []byte("done")))

		unfinishedMetaStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(unfinishedID))
		require.NoError(t, unfinishedMetaStorage.WriteFile(ctx, checkpoint.CheckpointMetaPathForBackup, []byte("cp")))

		unfinished, err := collectResumablePendingBackups(ctx, storage, cfgHash)
		require.NoError(t, err)
		require.Equal(t, []repo.BackupID{unfinishedID}, unfinished)

		exists, err := storage.FileExists(ctx, repo.PendingFile(cfgHash, staleID))
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("non_resumable_pending_cleans_checkpoint_debris", func(t *testing.T) {
		ctx := context.Background()
		storage := newSnapshotRepoTestStorage(t)
		cfgHash := []byte("hash")
		backupID := repo.BackupID(3)
		metadataStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID))

		require.NoError(t, storage.WriteFile(ctx, repo.PendingFile(cfgHash, backupID), []byte("{}")))
		require.NoError(t, metadataStorage.WriteFile(ctx, checkpoint.CheckpointLockPathForBackup, []byte("lock")))
		require.NoError(t, metadataStorage.WriteFile(ctx, checkpoint.CheckpointDataDirForBackup+"/partial.cpt", []byte("data")))

		unfinished, err := collectResumablePendingBackups(ctx, storage, cfgHash)
		require.NoError(t, err)
		require.Empty(t, unfinished)

		exists, err := storage.FileExists(ctx, repo.PendingFile(cfgHash, backupID))
		require.NoError(t, err)
		require.False(t, exists)
		exists, err = metadataStorage.FileExists(ctx, checkpoint.CheckpointLockPathForBackup)
		require.NoError(t, err)
		require.False(t, exists)
		exists, err = metadataStorage.FileExists(ctx, checkpoint.CheckpointDataDirForBackup+"/partial.cpt")
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestLoadSnapshotBackupMetaReadsRepoMetadataStorage(t *testing.T) {
	t.Run("load_backup_meta_from_repo_snapshot_storage", func(t *testing.T) {
		ctx := context.Background()
		baseDir := t.TempDir()
		storage, err := storage.NewLocalStorage(baseDir)
		require.NoError(t, err)
		backupID := repo.BackupID(0x1234)
		cipherInfo := backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
		rootBackend := &backuppb.StorageBackend{
			Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: baseDir}},
		}

		_, err = repo.EnsureRepo(ctx, storage, "test")
		require.NoError(t, err)

		metaWriter := metautil.NewMetaWriter(
			repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID)),
			metautil.MetaFileSize,
			false,
			"",
			&cipherInfo,
		)
		metaWriter.Update(func(m *backuppb.BackupMeta) {
			m.ClusterId = 42
		})
		require.NoError(t, metaWriter.FlushBackupMeta(ctx))

		resolved := &SnapshotStorageRef{
			BackupID:    backupID,
			RootBackend: rootBackend,
			RootStorage: storage,
		}
		require.NoError(t, resolved.Validate(ctx))
		backupMeta, err := resolved.LoadBackupMeta(ctx, &cipherInfo)
		require.NoError(t, err)
		require.Equal(t, backupID, resolved.BackupID)
		require.Equal(t, uint64(42), backupMeta.ClusterId)

		exists, err := storage.FileExists(ctx, metautil.MetaFile)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("validate_rejects_repo_without_startafter_support", func(t *testing.T) {
		ctx := context.Background()
		storage := &uriOverrideStorage{
			Storage: newSnapshotRepoTestStorage(t),
			uri:     "azure://bucket/prefix",
		}
		rootBackend := &backuppb.StorageBackend{
			Backend: &backuppb.StorageBackend_AzureBlobStorage{
				AzureBlobStorage: &backuppb.AzureBlobStorage{Bucket: "bucket", Prefix: "prefix"},
			},
		}

		_, err := repo.EnsureRepo(ctx, storage, "test")
		require.NoError(t, err)

		resolved := &SnapshotStorageRef{
			BackupID:    repo.BackupID(0x1234),
			RootBackend: rootBackend,
			RootStorage: storage,
		}
		err = resolved.Validate(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "does not support WalkDir StartAfter")
		require.True(t, errors.ErrorEqual(err, berrors.ErrUnsupportedOperation))
	})
}

func TestPrepareRepoSnapshotBackupStartsNewWithoutPending(t *testing.T) {
	ctx := context.Background()
	storage := newSnapshotRepoTestStorage(t)
	_, err := repo.EnsureRepo(ctx, storage, "test")
	require.NoError(t, err)

	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	cfgHash := []byte("hash")

	resolved, err := PrepareRepoSnapshotBackup(ctx, glue.ConsoleOperations{ConsoleGlue: glue.NoOPConsoleGlue{}}, rootBackend, storage, SnapshotBackupStorageParams{
		UseCheckpoint: true,
		ConfigHash:    cfgHash,
		CreatedBy:     "test",
		AllocateBackupID: func(context.Context) (repo.BackupID, error) {
			return repo.BackupID(0x1111), nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, repo.BackupID(0x1111), resolved.BackupID)
	require.Contains(t, resolved.MetadataStorage().URI(), repo.SnapshotMetadataDir(repo.BackupID(0x1111)))
}

func TestPrepareRepoSnapshotBackupResumePendingBackup(t *testing.T) {
	ctx := context.Background()
	storage := newSnapshotRepoTestStorage(t)
	_, err := repo.EnsureRepo(ctx, storage, "test")
	require.NoError(t, err)

	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	cfgHash := []byte("hash")
	backupID := repo.BackupID(0x1234)
	metaStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID))
	require.NoError(t, storage.WriteFile(ctx, repo.PendingFile(cfgHash, backupID), []byte("{}")))
	require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, metaStorage, &checkpoint.CheckpointMetadataForBackup{
		GCServiceId: "checkpoint-gc",
		ConfigHash:  cfgHash,
		BackupTS:    0x2222,
	}))

	console := newRepoSnapshotTestConsole("\n")
	allocateCalled := false
	resolved, err := PrepareRepoSnapshotBackup(ctx, console.Operations(), rootBackend, storage, SnapshotBackupStorageParams{
		UseCheckpoint: true,
		ConfigHash:    cfgHash,
		CreatedBy:     "test",
		AllocateBackupID: func(context.Context) (repo.BackupID, error) {
			allocateCalled = true
			return repo.BackupID(0x1111), nil
		},
	})
	require.NoError(t, err)
	require.False(t, allocateCalled)
	require.Equal(t, backupID, resolved.BackupID)
	require.Equal(t, repo.PendingFile(cfgHash, backupID), resolved.PendingMarkerPath)
	require.True(t, resolved.ResumeFromCheckpoint)
	require.Contains(t, resolved.MetadataStorage().URI(), repo.SnapshotMetadataDir(backupID))

	output := console.output.String()
	require.Contains(t, output, "Found an unfinished repo snapshot backup")
	require.Contains(t, output, backupID.String())
	require.Contains(t, output, repo.PendingConfigHashStorageName(cfgHash))
	require.Contains(t, output, repo.PendingFile(cfgHash, backupID))
	require.Contains(t, output, "Resume this backup? (Y/n)")

	declineConsole := newRepoSnapshotTestConsole("n\n")
	allocateCalled = false
	resolved, err = PrepareRepoSnapshotBackup(ctx, declineConsole.Operations(), rootBackend, storage, SnapshotBackupStorageParams{
		UseCheckpoint: true,
		ConfigHash:    cfgHash,
		CreatedBy:     "test",
		AllocateBackupID: func(context.Context) (repo.BackupID, error) {
			allocateCalled = true
			return repo.BackupID(0x1111), nil
		},
	})
	require.Nil(t, resolved)
	require.Error(t, err)
	require.True(t, berrors.Is(err, berrors.ErrOperationAborted))
	require.False(t, allocateCalled)
	require.Contains(t, err.Error(), "no new backup was created")
	require.Contains(t, err.Error(), "--use-checkpoint=false")
	require.Contains(t, err.Error(), "br repo snapshot delete --storage <repo-storage> --backup-id "+backupID.String())
}

func TestActivateSnapshotBackupResumeRejectsMismatchedCheckpointBackupID(t *testing.T) {
	for _, tc := range []struct {
		name       string
		metadata   *checkpoint.CheckpointMetadataForBackup
		requireErr string
	}{
		{
			name: "mismatched_backup_id",
			metadata: &checkpoint.CheckpointMetadataForBackup{
				ConfigHash: []byte("hash"),
				BackupTS:   0x2222,
				BackupID:   0x5678,
			},
			requireErr: "checkpoint metadata backup id",
		},
		{
			name: "legacy_metadata_without_backup_id",
			metadata: &checkpoint.CheckpointMetadataForBackup{
				ConfigHash: []byte("hash"),
				BackupTS:   0x2222,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			storage := newSnapshotRepoTestStorage(t)
			prepared := &PreparedRepoSnapshotBackup{
				SnapshotStorageRef: SnapshotStorageRef{
					BackupID:    repo.BackupID(0x1234),
					RootStorage: storage,
				},
				ResumeFromCheckpoint: true,
			}
			cfgHash := []byte("hash")
			require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, prepared.MetadataStorage(), tc.metadata))

			err := ActivateSnapshotBackupResume(ctx, &backup.Client{}, prepared, cfgHash)
			if tc.requireErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.requireErr)
		})
	}
}

func TestPrepareRepoSnapshotBackupRejectsAmbiguousResume(t *testing.T) {
	ctx := context.Background()
	storage := newSnapshotRepoTestStorage(t)
	_, err := repo.EnsureRepo(ctx, storage, "test")
	require.NoError(t, err)

	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	cfgHash := []byte("hash")
	for _, id := range []repo.BackupID{0x1234, 0x2345} {
		require.NoError(t, storage.WriteFile(ctx, repo.PendingFile(cfgHash, id), []byte("{}")))
		metaStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(id))
		require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, metaStorage, &checkpoint.CheckpointMetadataForBackup{
			GCServiceId: "checkpoint-gc",
			ConfigHash:  cfgHash,
			BackupTS:    uint64(id),
		}))
	}

	_, prepErr := PrepareRepoSnapshotBackup(ctx, glue.ConsoleOperations{ConsoleGlue: glue.NoOPConsoleGlue{}}, rootBackend, storage, SnapshotBackupStorageParams{
		UseCheckpoint: true,
		ConfigHash:    cfgHash,
		CreatedBy:     "test",
		AllocateBackupID: func(context.Context) (repo.BackupID, error) {
			return repo.BackupID(0x1111), nil
		},
	})
	require.Error(t, prepErr)
	require.Contains(t, prepErr.Error(), "cannot resume an ambiguous backup")
}

func TestPrepareRepoSnapshotBackupWithoutCheckpointStartsFreshDespitePending(t *testing.T) {
	ctx := context.Background()
	storage := newSnapshotRepoTestStorage(t)
	_, err := repo.EnsureRepo(ctx, storage, "test")
	require.NoError(t, err)

	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	cfgHash := []byte("hash")
	pendingID := repo.BackupID(0x1234)
	metaStorage := repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(pendingID))
	require.NoError(t, storage.WriteFile(ctx, repo.PendingFile(cfgHash, pendingID), []byte("{}")))
	require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, metaStorage, &checkpoint.CheckpointMetadataForBackup{
		GCServiceId: "checkpoint-gc",
		ConfigHash:  cfgHash,
		BackupTS:    0x2222,
	}))

	resolved, err := PrepareRepoSnapshotBackup(ctx, glue.ConsoleOperations{ConsoleGlue: glue.NoOPConsoleGlue{}}, rootBackend, storage, SnapshotBackupStorageParams{
		UseCheckpoint: false,
		ConfigHash:    cfgHash,
		CreatedBy:     "test",
		AllocateBackupID: func(context.Context) (repo.BackupID, error) {
			return repo.BackupID(0x1111), nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, repo.BackupID(0x1111), resolved.BackupID)
	require.Equal(t, repo.PendingFile(cfgHash, repo.BackupID(0x1111)), resolved.PendingMarkerPath)
}

func newSnapshotRepoTestStorage(t *testing.T) storage.Storage {
	t.Helper()
	storage, err := storage.NewLocalStorage(t.TempDir())
	require.NoError(t, err)
	return storage
}

type uriOverrideStorage struct {
	storage.Storage
	uri string
}

func (s *uriOverrideStorage) URI() string {
	return s.uri
}
