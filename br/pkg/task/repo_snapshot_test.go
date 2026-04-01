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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/repo/snapshotpaths"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestRunRepoSnapshotGetBasicViewDefault(t *testing.T) {
	ctx, cfg, storage := newRepoSnapshotTestEnv(t)
	defer storage.Close()

	backupID := repo.BackupID(0x1237)
	createBackupMeta(t, ctx, storage, backupID, func(meta *backuppb.BackupMeta) {
		meta.ClusterVersion = "v8.5.0"
		meta.BrVersion = "br-test"
		meta.StartVersion = 100
		meta.EndVersion = 200
		meta.IsTxnKv = true
		meta.BackupResult = "succeeded"
		meta.BackupSize = 4096
		meta.Mode = backuppb.BackupMode_FILE
	})

	result, err := RunRepoSnapshotGet(ctx, RepoSnapshotGetConfig{
		Config:   cfg,
		BackupID: backupID,
	})
	require.NoError(t, err)

	var basic repoSnapshotBasicView
	require.NoError(t, json.Unmarshal(result, &basic))
	require.Equal(t, repoSnapshotBasicView{
		ClusterID:      4663,
		ClusterVersion: "v8.5.0",
		BRVersion:      "br-test",
		StartVersion:   100,
		EndVersion:     200,
		IsTxnKV:        true,
		BackupResult:   "succeeded",
		Mode:           int32(backuppb.BackupMode_FILE),
	}, repoSnapshotBasicView{
		ClusterID:      basic.ClusterID,
		ClusterVersion: basic.ClusterVersion,
		BRVersion:      basic.BRVersion,
		StartVersion:   basic.StartVersion,
		EndVersion:     basic.EndVersion,
		IsTxnKV:        basic.IsTxnKV,
		BackupResult:   basic.BackupResult,
		Mode:           basic.Mode,
	})
	require.Greater(t, basic.BackupSize, uint64(0))
}

func TestRunRepoSnapshotGetTablesViewSorted(t *testing.T) {
	ctx, cfg, storage := newRepoSnapshotTestEnv(t)
	defer storage.Close()

	backupID := repo.BackupID(0x1238)
	createBackupMeta(t, ctx, storage, backupID, func(meta *backuppb.BackupMeta) {
		meta.Schemas = []*backuppb.Schema{
			newSchemaForView(t, 2, 22, "zeta", "t2", 22, 220, 2),
			newSchemaForView(t, 1, 11, "alpha", "t1", 11, 110, 1),
			newSchemaForView(t, 1, 10, "alpha", "t0", 10, 100, 0),
		}
	})

	result, err := RunRepoSnapshotGet(ctx, RepoSnapshotGetConfig{
		Config:   cfg,
		BackupID: backupID,
		View:     "tables",
	})
	require.NoError(t, err)

	var tables []repoSnapshotTableView
	require.NoError(t, json.Unmarshal(result, &tables))
	require.Equal(t, []repoSnapshotTableView{
		{DBName: "alpha", TableName: "t0", KVCount: 10, KVSize: 100, TiFlashReplica: 0},
		{DBName: "alpha", TableName: "t1", KVCount: 11, KVSize: 110, TiFlashReplica: 1},
		{DBName: "zeta", TableName: "t2", KVCount: 22, KVSize: 220, TiFlashReplica: 2},
	}, tables)
}

func TestRunRepoSnapshotGetFilesViewSorted(t *testing.T) {
	files := []*backuppb.File{
		newFileForView(t, "backup/data/z.sst", "6f", "7f", "write", 128, 12, 1200, "bb"),
		newFileForView(t, "backup/data/a.sst", "0a", "1a", "write", 64, 6, 600, "aa"),
		newFileForView(t, "backup/data/a.sst", "00", "09", "default", 32, 3, 300, "ab"),
	}
	expected := []repoSnapshotFileView{
		convertRepoSnapshotFileView(files[2]),
		convertRepoSnapshotFileView(files[1]),
		convertRepoSnapshotFileView(files[0]),
	}

	for _, useV2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("v%d", map[bool]int{false: 1, true: 2}[useV2]), func(t *testing.T) {
			ctx, cfg, storage := newRepoSnapshotTestEnv(t)
			defer storage.Close()

			backupID := repo.BackupID(0x1235)
			createBackupMetaWithFiles(t, ctx, storage, backupID, useV2, files)

			result, err := RunRepoSnapshotGet(ctx, RepoSnapshotGetConfig{
				Config:   cfg,
				BackupID: backupID,
				View:     "files",
			})
			require.NoError(t, err)

			var got []repoSnapshotFileView
			require.NoError(t, json.Unmarshal(result, &got))
			require.Equal(t, expected, got)
		})
	}
}

func TestRunRepoSnapshotGetInvalidView(t *testing.T) {
	ctx, cfg, storage := newRepoSnapshotTestEnv(t)
	defer storage.Close()

	backupID := repo.BackupID(0x1239)
	createBackupMeta(t, ctx, storage, backupID)

	_, err := RunRepoSnapshotGet(ctx, RepoSnapshotGetConfig{
		Config:   cfg,
		BackupID: backupID,
		View:     "unknown",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported snapshot metadata view")
}

func TestRunRepoSnapshotGetRejectsInvalidMetaWindow(t *testing.T) {
	ctx, cfg, storage := newRepoSnapshotTestEnv(t)
	defer storage.Close()

	backupID := repo.BackupID(0x1241)
	createBackupMeta(t, ctx, storage, backupID, func(meta *backuppb.BackupMeta) {
		meta.StartVersion = 200
		meta.EndVersion = 100
	})

	_, err := RunRepoSnapshotGet(ctx, RepoSnapshotGetConfig{
		Config:   cfg,
		BackupID: backupID,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "start version")
	require.Contains(t, err.Error(), "end version")
}

func TestRunRepoSnapshotPendingDiscardRejectsAmbiguousWithoutBackupID(t *testing.T) {
	ctx, cfg, storage := newRepoSnapshotTestEnv(t)
	defer storage.Close()

	createPendingCheckpoint(t, ctx, storage, repo.BackupID(0x101))
	createPendingCheckpoint(t, ctx, storage, repo.BackupID(0x102))

	_, err := RunRepoSnapshotPendingDiscard(ctx, RepoSnapshotPendingDiscardConfig{Config: cfg})
	require.Error(t, err)
	require.Contains(t, err.Error(), "backup id is required")
}

func newRepoSnapshotTestEnv(t *testing.T) (context.Context, Config, storeapi.Storage) {
	t.Helper()

	ctx := context.Background()
	cfg := Config{
		Storage: fmt.Sprintf("local://%s", filepath.ToSlash(t.TempDir())),
		CipherInfo: backuppb.CipherInfo{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
		},
	}
	_, storage, err := GetStorage(ctx, cfg.Storage, &cfg)
	require.NoError(t, err)
	_, err = repo.EnsureRepo(ctx, storage, snapshotpaths.RepoMetaPath, snapshotpaths.RootLockPath, "test")
	require.NoError(t, err)
	return ctx, cfg, storage
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

func createBackupMetaWithFiles(
	t *testing.T,
	ctx context.Context,
	storage storeapi.Storage,
	backupID repo.BackupID,
	useV2 bool,
	files []*backuppb.File,
	options ...func(*backuppb.BackupMeta),
) {
	t.Helper()

	cipherInfo := backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	metaWriter := metautil.NewMetaWriter(
		repo.NewPrefixedStorage(storage, snapshotpaths.MetadataDir(backupID)),
		metautil.MetaFileSize,
		useV2,
		"",
		&cipherInfo,
	)
	metaWriter.Update(func(meta *backuppb.BackupMeta) {
		meta.ClusterId = uint64(backupID)
		if !useV2 {
			meta.Files = files
		}
		for _, option := range options {
			option(meta)
		}
	})
	if !useV2 {
		require.NoError(t, metaWriter.FlushBackupMeta(ctx))
		return
	}
	metaWriter.StartWriteMetasAsync(ctx, metautil.AppendDataFile)
	require.NoError(t, metaWriter.Send(files, metautil.AppendDataFile))
	require.NoError(t, metaWriter.FinishWriteMetas(ctx, metautil.AppendDataFile))
	require.NoError(t, metaWriter.FlushBackupMeta(ctx))
}

func newSchemaForView(
	t *testing.T,
	dbID int64,
	tableID int64,
	dbName string,
	tableName string,
	kvCount uint64,
	kvSize uint64,
	tiflashReplica uint64,
) *backuppb.Schema {
	t.Helper()

	dbInfoBytes, err := json.Marshal(&model.DBInfo{
		ID:   dbID,
		Name: ast.NewCIStr(dbName),
	})
	require.NoError(t, err)
	tableInfoBytes, err := json.Marshal(&model.TableInfo{
		ID:   tableID,
		Name: ast.NewCIStr(tableName),
	})
	require.NoError(t, err)
	return &backuppb.Schema{
		Db:              dbInfoBytes,
		Table:           tableInfoBytes,
		TotalKvs:        kvCount,
		TotalBytes:      kvSize,
		TiflashReplicas: uint32(tiflashReplica),
	}
}

func newFileForView(
	t *testing.T,
	name string,
	startKeyHex string,
	endKeyHex string,
	cf string,
	size uint64,
	totalKVs uint64,
	totalBytes uint64,
	sha256Hex string,
) *backuppb.File {
	t.Helper()

	startKey, err := hex.DecodeString(startKeyHex)
	require.NoError(t, err)
	endKey, err := hex.DecodeString(endKeyHex)
	require.NoError(t, err)
	sha256Bytes, err := hex.DecodeString(sha256Hex)
	require.NoError(t, err)
	return &backuppb.File{
		Name:       name,
		StartKey:   startKey,
		EndKey:     endKey,
		Cf:         cf,
		Size_:      size,
		TotalKvs:   totalKVs,
		TotalBytes: totalBytes,
		Sha256:     sha256Bytes,
	}
}
