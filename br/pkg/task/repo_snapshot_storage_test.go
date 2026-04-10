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

package task

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
	"github.com/pingcap/tidb/br/pkg/repo/snapshotpaths"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestSnapshotRepoFlags(t *testing.T) {
	flags := pflag.NewFlagSet("snapshot", pflag.ContinueOnError)
	DefineSnapshotRepoFlags(flags, true)
	require.NoError(t, flags.Parse([]string{"--storage-layout=repo-v1", "--backup-id=61453"}))

	layout, err := parseSnapshotStorageLayoutFlag(flags)
	require.NoError(t, err)
	require.Equal(t, repo.LayoutRepoV1, layout)

	backupID, err := parseSnapshotBackupIDFlag(flags)
	require.NoError(t, err)
	require.Equal(t, repo.BackupID(0xf00d), backupID)
}

func TestSnapshotRepoBackupFlagsOnPending(t *testing.T) {
	flags := pflag.NewFlagSet("snapshot", pflag.ContinueOnError)
	DefineSnapshotRepoFlags(flags, false)
	require.NoError(t, flags.Parse([]string{"--storage-layout=repo-v1", "--on-pending=resume"}))

	action, err := parseSnapshotOnPendingFlag(flags)
	require.NoError(t, err)
	require.Equal(t, snapshotRepoOnPendingResume, action)
}

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

func TestRewriteDataFilesForStore(t *testing.T) {
	files, err := rewriteDataFilesForStore([]*backuppb.File{
		{Name: "123/file.sst"},
		{Name: "file2.sst"},
	}, 7, repo.BackupID(0xf00d))
	require.NoError(t, err)
	require.Equal(t, "_data/snapshot/7/000000000000F00D/123/file.sst", files[0].Name)
	require.Equal(t, "_data/snapshot/7/000000000000F00D/file2.sst", files[1].Name)
	require.Equal(t, "123/file.sst", files[0].Name[len("_data/snapshot/7/000000000000F00D/"):])

	files, err = rewriteDataFilesForStore([]*backuppb.File{{Name: "foo/../bar.sst"}}, 7, repo.BackupID(0xf00d))
	require.NoError(t, err)
	require.Equal(t, "_data/snapshot/7/000000000000F00D/foo/../bar.sst", files[0].Name)
}

func TestSnapshotRepoBackupLifecycleRewriteResponseFiles(t *testing.T) {
	lifecycle := &snapshotRepoBackupLifecycle{
		resolvedStorage: &resolvedSnapshotStorage{Layout: repo.LayoutRepoV1, BackupID: repo.BackupID(0xbeef)},
	}
	rewriter := lifecycle.RewriteResponseFiles()
	require.NotNil(t, rewriter)

	files, err := rewriter(9, []*backuppb.File{{Name: "7/file.sst"}})
	require.NoError(t, err)
	require.Equal(t, "_data/snapshot/9/000000000000BEEF/7/file.sst", files[0].Name)

	var nilLifecycle *snapshotRepoBackupLifecycle
	require.Nil(t, nilLifecycle.RewriteResponseFiles())
}

func TestValidateSnapshotBackupRepoConfigRejectsNoCheckpoint(t *testing.T) {
	cfg := &BackupConfig{
		UseCheckpoint: false,
		SnapshotRepoBackupOptions: SnapshotRepoBackupOptions{
			Layout: repo.LayoutRepoV1,
		},
	}
	err := validateSnapshotBackupRepoConfig(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--use-checkpoint")
}

func TestSnapshotRegistrationBackupID(t *testing.T) {
	require.Empty(t, snapshotRegistrationBackupID(repo.LayoutLegacy, repo.BackupID(0x1234)))
	require.Empty(t, snapshotRegistrationBackupID(repo.LayoutRepoV1, 0))
	require.Equal(t, "4660", snapshotRegistrationBackupID(repo.LayoutRepoV1, repo.BackupID(0x1234)))
}

func TestSnapshotRepoBackupLifecyclePendingFileInvariant(t *testing.T) {
	cfgHash := []byte("hash")
	lifecycle := &snapshotRepoBackupLifecycle{
		resolvedStorage: &resolvedSnapshotStorage{
			Layout:   repo.LayoutRepoV1,
			BackupID: repo.BackupID(0x1234),
		},
	}

	_, err := lifecycle.pendingFileForCheckpoint()
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing pending marker path")

	lifecycle.pendingFile = snapshotpaths.PendingFile(cfgHash, lifecycle.resolvedStorage.BackupID)
	pendingFile, err := lifecycle.pendingFileForCheckpoint()
	require.NoError(t, err)
	require.Equal(t, snapshotpaths.PendingFile(cfgHash, lifecycle.resolvedStorage.BackupID), pendingFile)

	var nilLifecycle *snapshotRepoBackupLifecycle
	pendingFile, err = nilLifecycle.pendingFileForCheckpoint()
	require.NoError(t, err)
	require.Empty(t, pendingFile)
}

func TestPendingConfigHashStorageName(t *testing.T) {
	require.Equal(t, "DEADBEEF", snapshotpaths.PendingConfigHashStorageName([]byte{0xde, 0xad, 0xbe, 0xef}))
}

func TestSnapshotRepoBackupLifecycleBeforeFirstRequestToStorePreparesLocalDir(t *testing.T) {
	baseDir := t.TempDir()
	targetDir := filepath.Join(baseDir, "_data", "snapshot", "1", "0000000000001234")
	lifecycle := &snapshotRepoBackupLifecycle{
		resolvedStorage: &resolvedSnapshotStorage{Layout: repo.LayoutRepoV1, BackupID: repo.BackupID(0x1234)},
	}

	prepare := lifecycle.BeforeFirstRequestToStore()
	require.NotNil(t, prepare)
	require.NoError(t, prepare(1, backuppb.BackupRequest{
		StorageBackend: &backuppb.StorageBackend{
			Backend: &backuppb.StorageBackend_Local{
				Local: &backuppb.Local{Path: targetDir},
			},
		},
	}))

	info, err := os.Stat(targetDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestResolveUnfinishedPendingBackups(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	cfgHash := []byte("hash")
	staleID := repo.BackupID(1)
	unfinishedID := repo.BackupID(2)

	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile(cfgHash, staleID), []byte("{}")))
	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile(cfgHash, unfinishedID), []byte("{}")))

	staleMetaStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(staleID))
	require.NoError(t, staleMetaStorage.WriteFile(ctx, metautil.MetaFile, []byte("done")))

	unfinishedMetaStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(unfinishedID))
	require.NoError(t, unfinishedMetaStorage.WriteFile(ctx, checkpoint.CheckpointMetaPathForBackup, []byte("cp")))

	unfinished, err := resolveUnfinishedPendingBackups(ctx, storage, cfgHash)
	require.NoError(t, err)
	require.Equal(t, []repo.BackupID{unfinishedID}, unfinished)

	exists, err := storage.FileExists(ctx, snapshotpaths.PendingFile(cfgHash, staleID))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestResolveUnfinishedPendingBackupsCleansTransientStateWithoutCheckpointMeta(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	cfgHash := []byte("hash")
	backupID := repo.BackupID(3)
	metadataStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(backupID))

	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile(cfgHash, backupID), []byte("{}")))
	require.NoError(t, metadataStorage.WriteFile(ctx, checkpoint.CheckpointLockPathForBackup, []byte("lock")))
	require.NoError(t, metadataStorage.WriteFile(ctx, checkpoint.CheckpointDataDirForBackup+"/partial.cpt", []byte("data")))

	unfinished, err := resolveUnfinishedPendingBackups(ctx, storage, cfgHash)
	require.NoError(t, err)
	require.Empty(t, unfinished)

	exists, err := storage.FileExists(ctx, snapshotpaths.PendingFile(cfgHash, backupID))
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = metadataStorage.FileExists(ctx, checkpoint.CheckpointLockPathForBackup)
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = metadataStorage.FileExists(ctx, checkpoint.CheckpointDataDirForBackup+"/partial.cpt")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestResolveSnapshotBackupMetaReadsRepoMetadataStorage(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	backupID := repo.BackupID(0x1234)
	cfg := &RestoreConfig{
		Config: Config{
			CipherInfo: backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT},
		},
		Layout:   repo.LayoutRepoV1,
		BackupID: backupID,
	}
	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}

	_, err := repo.EnsureRepo(ctx, storage, snapshotpaths.RepoMetaPath, snapshotpaths.RootLockPath, "test")
	require.NoError(t, err)

	metaWriter := metautil.NewMetaWriter(
		repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(backupID)),
		metautil.MetaFileSize,
		false,
		"",
		&cfg.CipherInfo,
	)
	metaWriter.Update(func(m *backuppb.BackupMeta) {
		m.ClusterId = 42
	})
	require.NoError(t, metaWriter.FlushBackupMeta(ctx))

	resolved, backupMeta, err := resolveSnapshotBackupMeta(ctx, cfg, rootBackend, storage)
	require.NoError(t, err)
	require.Equal(t, backupID, resolved.BackupID)
	require.Equal(t, uint64(42), backupMeta.ClusterId)

	exists, err := storage.FileExists(ctx, metautil.MetaFile)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestPrepareSnapshotBackupStorageOnPendingNoneStartsNew(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	_, err := repo.EnsureRepo(ctx, storage, snapshotpaths.RepoMetaPath, snapshotpaths.RootLockPath, "test")
	require.NoError(t, err)

	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	cfgHash := []byte("hash")

	for _, onPending := range []snapshotRepoOnPendingAction{
		snapshotRepoOnPendingError,
		snapshotRepoOnPendingResume,
		snapshotRepoOnPendingNew,
	} {
		resolved, err := prepareSnapshotBackupStorage(ctx, rootBackend, storage, snapshotBackupStorageParams{
			onPending: onPending,
			cfgHash:   cfgHash,
			createdBy: "test",
			allocateBackupID: func(context.Context) (repo.BackupID, error) {
				return repo.BackupID(0x1111), nil
			},
		})
		require.NoError(t, err)
		require.Equal(t, repo.BackupID(0x1111), resolved.BackupID)
		require.Contains(t, resolved.MetadataStorage.URI(), snapshotpaths.MetadataDir(repo.BackupID(0x1111)))
	}
}

func TestPrepareSnapshotBackupStorageResumePendingBackup(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	_, err := repo.EnsureRepo(ctx, storage, snapshotpaths.RepoMetaPath, snapshotpaths.RootLockPath, "test")
	require.NoError(t, err)

	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	cfgHash := []byte("hash")
	backupID := repo.BackupID(0x1234)
	metaStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(backupID))
	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile(cfgHash, backupID), []byte("{}")))
	require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, metaStorage, &checkpoint.CheckpointMetadataForBackup{
		GCServiceId: "checkpoint-gc",
		ConfigHash:  cfgHash,
		BackupTS:    0x2222,
	}))

	allocateCalled := false
	resolved, err := prepareSnapshotBackupStorage(ctx, rootBackend, storage, snapshotBackupStorageParams{
		onPending: snapshotRepoOnPendingResume,
		cfgHash:   cfgHash,
		createdBy: "test",
		allocateBackupID: func(context.Context) (repo.BackupID, error) {
			allocateCalled = true
			return repo.BackupID(0x1111), nil
		},
	})
	require.NoError(t, err)
	require.False(t, allocateCalled)
	require.Equal(t, backupID, resolved.BackupID)
	require.Equal(t, snapshotpaths.PendingFile(cfgHash, backupID), resolved.pendingFile)
	require.True(t, resolved.resumeCheckpoint)
	require.Contains(t, resolved.MetadataStorage.URI(), snapshotpaths.MetadataDir(backupID))
}

func TestActivateSnapshotBackupResumeRejectsMismatchedCheckpointBackupID(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	prepared := &preparedSnapshotBackupStorage{
		resolvedSnapshotStorage: resolvedSnapshotStorage{
			Layout:          repo.LayoutRepoV1,
			BackupID:        repo.BackupID(0x1234),
			MetadataStorage: storage,
		},
		resumeCheckpoint: true,
	}
	cfgHash := []byte("hash")
	require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, storage, &checkpoint.CheckpointMetadataForBackup{
		ConfigHash: cfgHash,
		BackupTS:   0x2222,
		BackupID:   0x5678,
	}))

	err := activateSnapshotBackupResume(ctx, &backup.Client{}, prepared, cfgHash)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checkpoint metadata backup id")
}

func TestActivateSnapshotBackupResumeAllowsLegacyCheckpointMetadataWithoutBackupID(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	prepared := &preparedSnapshotBackupStorage{
		resolvedSnapshotStorage: resolvedSnapshotStorage{
			Layout:          repo.LayoutRepoV1,
			BackupID:        repo.BackupID(0x1234),
			MetadataStorage: storage,
		},
		resumeCheckpoint: true,
	}
	cfgHash := []byte("hash")
	require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, storage, &checkpoint.CheckpointMetadataForBackup{
		ConfigHash: cfgHash,
		BackupTS:   0x2222,
	}))

	err := activateSnapshotBackupResume(ctx, &backup.Client{}, prepared, cfgHash)
	require.NoError(t, err)
}

func TestPrepareSnapshotBackupStorageRejectsPendingWhenErrorMode(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	_, err := repo.EnsureRepo(ctx, storage, snapshotpaths.RepoMetaPath, snapshotpaths.RootLockPath, "test")
	require.NoError(t, err)

	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	cfgHash := []byte("hash")
	backupID := repo.BackupID(0x1234)
	metaStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(backupID))
	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile(cfgHash, backupID), []byte("{}")))
	require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, metaStorage, &checkpoint.CheckpointMetadataForBackup{
		GCServiceId: "checkpoint-gc",
		ConfigHash:  cfgHash,
		BackupTS:    0x2222,
	}))

	_, prepErr := prepareSnapshotBackupStorage(ctx, rootBackend, storage, snapshotBackupStorageParams{
		onPending: snapshotRepoOnPendingError,
		cfgHash:   cfgHash,
		createdBy: "test",
		allocateBackupID: func(context.Context) (repo.BackupID, error) {
			return repo.BackupID(0x1111), nil
		},
	})
	require.Error(t, prepErr)
	require.Contains(t, prepErr.Error(), backupID.String())
	require.Contains(t, prepErr.Error(), "--on-pending=resume")
}

func TestPrepareSnapshotBackupStorageRejectsAmbiguousResume(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	_, err := repo.EnsureRepo(ctx, storage, snapshotpaths.RepoMetaPath, snapshotpaths.RootLockPath, "test")
	require.NoError(t, err)

	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	cfgHash := []byte("hash")
	for _, id := range []repo.BackupID{0x1234, 0x2345} {
		require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile(cfgHash, id), []byte("{}")))
		metaStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(id))
		require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, metaStorage, &checkpoint.CheckpointMetadataForBackup{
			GCServiceId: "checkpoint-gc",
			ConfigHash:  cfgHash,
			BackupTS:    uint64(id),
		}))
	}

	_, prepErr := prepareSnapshotBackupStorage(ctx, rootBackend, storage, snapshotBackupStorageParams{
		onPending: snapshotRepoOnPendingResume,
		cfgHash:   cfgHash,
		createdBy: "test",
		allocateBackupID: func(context.Context) (repo.BackupID, error) {
			return repo.BackupID(0x1111), nil
		},
	})
	require.Error(t, prepErr)
	require.Contains(t, prepErr.Error(), "cannot resume an ambiguous backup")
}

func TestPrepareSnapshotBackupStorageNewStartsFreshDespitePending(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	_, err := repo.EnsureRepo(ctx, storage, snapshotpaths.RepoMetaPath, snapshotpaths.RootLockPath, "test")
	require.NoError(t, err)

	rootBackend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	cfgHash := []byte("hash")
	pendingID := repo.BackupID(0x1234)
	metaStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(pendingID))
	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile(cfgHash, pendingID), []byte("{}")))
	require.NoError(t, checkpoint.SaveCheckpointMetadata(ctx, metaStorage, &checkpoint.CheckpointMetadataForBackup{
		GCServiceId: "checkpoint-gc",
		ConfigHash:  cfgHash,
		BackupTS:    0x2222,
	}))

	resolved, err := prepareSnapshotBackupStorage(ctx, rootBackend, storage, snapshotBackupStorageParams{
		onPending: snapshotRepoOnPendingNew,
		cfgHash:   cfgHash,
		createdBy: "test",
		allocateBackupID: func(context.Context) (repo.BackupID, error) {
			return repo.BackupID(0x1111), nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, repo.BackupID(0x1111), resolved.BackupID)
	require.Equal(t, snapshotpaths.PendingFile(cfgHash, repo.BackupID(0x1111)), resolved.pendingFile)
}
