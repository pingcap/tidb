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

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/backup"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/pkg/objstore"
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

func TestValidateSnapshotBackupRepoConfigRejectsNoCheckpoint(t *testing.T) {
	err := ValidateSnapshotBackupRepoConfig(repo.LayoutRepoV1, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--use-checkpoint")
}

func TestPreparedRepoV1SnapshotBackupRewritesStoreRequestAndResponse(t *testing.T) {
	baseDir := t.TempDir()
	prepared := &PreparedRepoV1SnapshotBackup{
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
		storage := objstore.NewMemStorage()
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

	t.Run("transient_state_cleaned_without_checkpoint_meta", func(t *testing.T) {
		ctx := context.Background()
		storage := objstore.NewMemStorage()
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
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	backupID := repo.BackupID(0x1234)
	cipherInfo := backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}

	_, err := repo.EnsureRepo(ctx, storage, "test")
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

	resolved, backupMeta, err := LoadSnapshotBackupMeta(ctx, repo.LayoutRepoV1, backupID, rootBackend, storage, &cipherInfo)
	require.NoError(t, err)
	require.Equal(t, backupID, resolved.BackupID)
	require.Equal(t, uint64(42), backupMeta.ClusterId)

	exists, err := storage.FileExists(ctx, metautil.MetaFile)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestPrepareRepoV1SnapshotBackupOnPendingNoneStartsNew(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	_, err := repo.EnsureRepo(ctx, storage, "test")
	require.NoError(t, err)

	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	cfgHash := []byte("hash")

	resolved, err := PrepareRepoV1SnapshotBackup(ctx, rootBackend, storage, SnapshotBackupStorageParams{
		OnPending:  OnPendingError,
		ConfigHash: cfgHash,
		CreatedBy:  "test",
		AllocateBackupID: func(context.Context) (repo.BackupID, error) {
			return repo.BackupID(0x1111), nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, repo.BackupID(0x1111), resolved.BackupID)
	require.Contains(t, resolved.MetadataStorage.URI(), repo.SnapshotMetadataDir(repo.BackupID(0x1111)))
}

func TestPrepareRepoV1SnapshotBackupResumePendingBackup(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
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

	allocateCalled := false
	resolved, err := PrepareRepoV1SnapshotBackup(ctx, rootBackend, storage, SnapshotBackupStorageParams{
		OnPending:  OnPendingResume,
		ConfigHash: cfgHash,
		CreatedBy:  "test",
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
	require.Contains(t, resolved.MetadataStorage.URI(), repo.SnapshotMetadataDir(backupID))
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
			storage := objstore.NewMemStorage()
			prepared := &PreparedRepoV1SnapshotBackup{
				SnapshotStorageRef: SnapshotStorageRef{
					Layout:          repo.LayoutRepoV1,
					BackupID:        repo.BackupID(0x1234),
					MetadataStorage: storage,
				},
				ResumeFromCheckpoint: true,
			}
			cfgHash := []byte("hash")
			require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, storage, tc.metadata))

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

func TestPrepareRepoV1SnapshotBackupRejectsPendingWhenErrorMode(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
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

	_, prepErr := PrepareRepoV1SnapshotBackup(ctx, rootBackend, storage, SnapshotBackupStorageParams{
		OnPending:  OnPendingError,
		ConfigHash: cfgHash,
		CreatedBy:  "test",
		AllocateBackupID: func(context.Context) (repo.BackupID, error) {
			return repo.BackupID(0x1111), nil
		},
	})
	require.Error(t, prepErr)
	require.Contains(t, prepErr.Error(), backupID.String())
	require.Contains(t, prepErr.Error(), "--on-pending=resume")
}

func TestPrepareRepoV1SnapshotBackupRejectsAmbiguousResume(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
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

	_, prepErr := PrepareRepoV1SnapshotBackup(ctx, rootBackend, storage, SnapshotBackupStorageParams{
		OnPending:  OnPendingResume,
		ConfigHash: cfgHash,
		CreatedBy:  "test",
		AllocateBackupID: func(context.Context) (repo.BackupID, error) {
			return repo.BackupID(0x1111), nil
		},
	})
	require.Error(t, prepErr)
	require.Contains(t, prepErr.Error(), "cannot resume an ambiguous backup")
}

func TestPrepareRepoV1SnapshotBackupNewStartsFreshDespitePending(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
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

	resolved, err := PrepareRepoV1SnapshotBackup(ctx, rootBackend, storage, SnapshotBackupStorageParams{
		OnPending:  OnPendingNew,
		ConfigHash: cfgHash,
		CreatedBy:  "test",
		AllocateBackupID: func(context.Context) (repo.BackupID, error) {
			return repo.BackupID(0x1111), nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, repo.BackupID(0x1111), resolved.BackupID)
	require.Equal(t, repo.PendingFile(cfgHash, repo.BackupID(0x1111)), resolved.PendingMarkerPath)
}
