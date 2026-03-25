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
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
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

func TestRewriteDataBackendForStore(t *testing.T) {
	local := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: "/tmp/repo"}},
	}
	rewritten, err := rewriteDataBackendForStore(local, 123, repo.BackupID(0xf00d))
	require.NoError(t, err)
	require.Equal(t, "/tmp/repo/_data/snapshot/123/000000000000F00D", rewritten.GetLocal().Path)

	s3 := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_S3{S3: &backuppb.S3{Bucket: "bucket", Prefix: "root"}},
	}
	rewritten, err = rewriteDataBackendForStore(s3, 7, repo.BackupID(0xbeef))
	require.NoError(t, err)
	require.Equal(t, "root/_data/snapshot/7/000000000000BEEF", rewritten.GetS3().Prefix)
}

func TestValidateSnapshotBackupRepoConfigRejectsNoCheckpoint(t *testing.T) {
	cfg := &BackupConfig{
		UseCheckpoint: false,
		Layout:        repo.LayoutRepoV1,
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

func TestResolveUnfinishedPendingBackups(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	cfgHashHex := "hash"
	staleID := repo.BackupID(1)
	unfinishedID := repo.BackupID(2)

	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile(cfgHashHex, staleID), []byte("{}")))
	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile(cfgHashHex, unfinishedID), []byte("{}")))

	staleMetaStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(staleID))
	require.NoError(t, staleMetaStorage.WriteFile(ctx, metautil.MetaFile, []byte("done")))

	unfinishedMetaStorage := repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(unfinishedID))
	require.NoError(t, unfinishedMetaStorage.WriteFile(ctx, checkpoint.CheckpointMetaPathForBackup, []byte("cp")))

	unfinished, err := resolveUnfinishedPendingBackups(ctx, storage, cfgHashHex)
	require.NoError(t, err)
	require.Equal(t, []repo.BackupID{unfinishedID}, unfinished)

	exists, err := storage.FileExists(ctx, snapshotpaths.PendingFile(cfgHashHex, staleID))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestResolveUnfinishedPendingBackupsRejectsInconsistentState(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	cfgHashHex := "hash"
	backupID := repo.BackupID(3)

	require.NoError(t, storage.WriteFile(ctx, snapshotpaths.PendingFile(cfgHashHex, backupID), []byte("{}")))

	_, err := resolveUnfinishedPendingBackups(ctx, storage, cfgHashHex)
	require.Error(t, err)
	require.Contains(t, err.Error(), "inconsistent")
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
