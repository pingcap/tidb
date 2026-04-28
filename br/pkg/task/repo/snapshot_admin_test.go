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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/repo"
	taskcommon "github.com/pingcap/tidb/br/pkg/task/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func decodeRepoSnapshotJSONStream[T any](t *testing.T, payload []byte) []T {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(payload))
	items := make([]T, 0)
	for {
		var item T
		err := decoder.Decode(&item)
		if err == io.EOF {
			return items
		}
		require.NoError(t, err)
		items = append(items, item)
	}
}

func TestRunRepoSnapshotGetBasicViewDefault(t *testing.T) {
	ctx, cfg, storage := newRepoSnapshotTestEnv(t)
	defer storage.Close()
	console := glue.ConsoleOperations{ConsoleGlue: glue.NoOPConsoleGlue{}}

	backupID := repo.BackupID(0x1237)
	createBackupMeta(ctx, t, storage, backupID, func(meta *backuppb.BackupMeta) {
		meta.ClusterVersion = "v8.5.0"
		meta.BrVersion = "br-test"
		meta.StartVersion = 100
		meta.EndVersion = 200
		meta.IsTxnKv = true
		meta.BackupResult = "succeeded"
		meta.BackupSize = 4096
		meta.Mode = backuppb.BackupMode_FILE
		meta.Schemas = []*backuppb.Schema{
			newSchemaForView(t, 2, 22, "zeta", "t2", 22, 220, 2),
			newSchemaForView(t, 1, 11, "alpha", "t1", 11, 110, 1),
			newSchemaForView(t, 1, 10, "alpha", "t0", 10, 100, 0),
		}
	})

	t.Run("basic", func(t *testing.T) {
		result, err := RunRepoSnapshotGet(ctx, console, RepoSnapshotGetConfig{
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
	})

	t.Run("tables", func(t *testing.T) {
		result, err := RunRepoSnapshotGet(ctx, console, RepoSnapshotGetConfig{
			Config:   cfg,
			BackupID: backupID,
			View:     "tables",
		})
		require.NoError(t, err)

		tables := decodeRepoSnapshotJSONStream[repoSnapshotTableView](t, result)
		require.ElementsMatch(t, []repoSnapshotTableView{
			{DBName: "alpha", TableName: "t0", KVCount: 10, KVSize: 100, TiFlashReplica: 0},
			{DBName: "alpha", TableName: "t1", KVCount: 11, KVSize: 110, TiFlashReplica: 1},
			{DBName: "zeta", TableName: "t2", KVCount: 22, KVSize: 220, TiFlashReplica: 2},
		}, tables)
	})

	t.Run("files", func(t *testing.T) {
		files := []*backuppb.File{
			newFileForView(t, "backup/data/z.sst", "6f", "7f", "write", 128, 12, 1200, "bb"),
			newFileForView(t, "backup/data/a.sst", "0a", "1a", "write", 64, 6, 600, "aa"),
			newFileForView(t, "backup/data/a.sst", "00", "09", "default", 32, 3, 300, "ab"),
		}
		expectedFiles := []repoSnapshotFileView{
			convertRepoSnapshotFileView(files[0]),
			convertRepoSnapshotFileView(files[1]),
			convertRepoSnapshotFileView(files[2]),
		}
		for i, useV2 := range []bool{false, true} {
			filesBackupID := backupID + repo.BackupID(i+1)
			createBackupMetaWithFiles(ctx, t, storage, filesBackupID, useV2, files)
			result, err := RunRepoSnapshotGet(ctx, console, RepoSnapshotGetConfig{
				Config:   cfg,
				BackupID: filesBackupID,
				View:     "files",
			})
			require.NoError(t, err)

			got := decodeRepoSnapshotJSONStream[repoSnapshotFileView](t, result)
			require.ElementsMatch(t, expectedFiles, got)
		}
	})

	t.Run("invalid_view", func(t *testing.T) {
		_, err := RunRepoSnapshotGet(ctx, console, RepoSnapshotGetConfig{
			Config:   cfg,
			BackupID: backupID,
			View:     "unknown",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported snapshot metadata view")
	})

	t.Run("invalid_meta_window", func(t *testing.T) {
		invalidBackupID := repo.BackupID(0x1241)
		createBackupMeta(ctx, t, storage, invalidBackupID, func(meta *backuppb.BackupMeta) {
			meta.StartVersion = 200
			meta.EndVersion = 100
		})

		_, err := RunRepoSnapshotGet(ctx, console, RepoSnapshotGetConfig{
			Config:   cfg,
			BackupID: invalidBackupID,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "start version")
		require.Contains(t, err.Error(), "end version")
	})
}

func TestRunRepoSnapshotDeleteConfirmationAbortKeepsFiles(t *testing.T) {
	ctx, cfg, storage := newRepoSnapshotTestEnv(t)
	defer storage.Close()

	backupID := repo.BackupID(0x4101)
	createBackupMeta(ctx, t, storage, backupID, func(meta *backuppb.BackupMeta) {
		meta.BackupSize = 4096
		meta.IsTxnKv = true
	})
	pendingPath := repo.PendingFile([]byte("hash"), backupID)
	dataPath := repo.SnapshotStoreDataPrefix(1, backupID) + "/a.sst"
	createPendingMarker(ctx, t, storage, backupID)
	require.NoError(t, storage.WriteFile(ctx, dataPath, []byte("a")))

	console := newRepoSnapshotTestConsole("n\n")
	result, err := RunRepoSnapshotDelete(ctx, console.Operations(), RepoSnapshotDeleteConfig{
		Config:   cfg,
		BackupID: backupID,
	})
	require.Equal(t, RepoSnapshotDeleteResult{}, result)
	require.Error(t, err)
	require.True(t, berrors.Is(err, berrors.ErrOperationAborted))
	require.Empty(t, console.progressBars)
	require.Contains(t, console.output.String(), "Continue? (y/N) ")
	require.Contains(t, console.output.String(), backupID.String())
	require.Contains(t, console.output.String(), "backup-size")
	require.Contains(t, console.output.String(), "pending-markers")
	requireRepoSnapshotFileExists(ctx, t, repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID)), metautil.MetaFile)
	requireRepoSnapshotFileExists(ctx, t, storage, pendingPath)
	requireRepoSnapshotFileExists(ctx, t, storage, dataPath)
}

func TestRunRepoSnapshotPendingDiscardConfirmationAbortKeepsFiles(t *testing.T) {
	t.Run("ambiguous_requires_backup_id", func(t *testing.T) {
		ctx, cfg, storage := newRepoSnapshotTestEnv(t)
		defer storage.Close()

		createPendingCheckpoint(ctx, t, storage, repo.BackupID(0x101))
		createPendingCheckpoint(ctx, t, storage, repo.BackupID(0x102))

		console := glue.ConsoleOperations{ConsoleGlue: glue.NoOPConsoleGlue{}}
		_, err := RunRepoSnapshotPendingDiscard(ctx, console, RepoSnapshotPendingDiscardConfig{Config: cfg})
		require.Error(t, err)
		require.Contains(t, err.Error(), "backup id is required")
	})

	t.Run("confirmation_abort_keeps_files", func(t *testing.T) {
		ctx, cfg, storage := newRepoSnapshotTestEnv(t)
		defer storage.Close()

		backupID := repo.BackupID(0x4102)
		dataPath := repo.SnapshotStoreDataPrefix(1, backupID) + "/a.sst"
		pendingPath := repo.PendingFile([]byte("hash"), backupID)
		createPendingCheckpoint(ctx, t, storage, backupID)
		require.NoError(t, storage.WriteFile(ctx, dataPath, []byte("a")))

		console := newRepoSnapshotTestConsole("n\n")
		result, err := RunRepoSnapshotPendingDiscard(ctx, console.Operations(), RepoSnapshotPendingDiscardConfig{
			Config:   cfg,
			BackupID: backupID,
		})
		require.Equal(t, RepoSnapshotPendingDiscardResult{}, result)
		require.Error(t, err)
		require.True(t, berrors.Is(err, berrors.ErrOperationAborted))
		require.Empty(t, console.progressBars)
		require.Contains(t, console.output.String(), "Continue? (y/N) ")
		require.Contains(t, console.output.String(), backupID.String())
		require.Contains(t, console.output.String(), "state: unfinished")
		require.Contains(t, console.output.String(), "pending-markers: 1")
		requireRepoSnapshotFileExists(ctx, t, repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID)), checkpoint.CheckpointMetaPathForBackup)
		requireRepoSnapshotFileExists(ctx, t, storage, pendingPath)
		requireRepoSnapshotFileExists(ctx, t, storage, dataPath)
	})
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
	_, storage, err := taskcommon.GetStorage(ctx, cfg.Storage, cfg.BackendOptions, cfg.NoCreds, cfg.SendCreds)
	require.NoError(t, err)
	_, err = repo.EnsureRepo(ctx, storage, "test")
	require.NoError(t, err)
	return ctx, cfg, storage
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

func createBackupMetaWithFiles(
	ctx context.Context,
	t *testing.T,
	storage storeapi.Storage,
	backupID repo.BackupID,
	useV2 bool,
	files []*backuppb.File,
	options ...func(*backuppb.BackupMeta),
) {
	t.Helper()

	cipherInfo := backuppb.CipherInfo{CipherType: encryptionpb.EncryptionMethod_PLAINTEXT}
	metaWriter := metautil.NewMetaWriter(
		repo.NewPrefixedStorage(storage, repo.SnapshotMetadataDir(backupID)),
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

type repoSnapshotTestConsole struct {
	progressBars []*repoSnapshotTestProgress
	waitErr      error
	interactive  bool
	input        bytes.Buffer
	output       bytes.Buffer
}

func newRepoSnapshotTestConsole(input string) *repoSnapshotTestConsole {
	console := &repoSnapshotTestConsole{interactive: true}
	_, _ = console.input.WriteString(input)
	return console
}

func (c *repoSnapshotTestConsole) Operations() glue.ConsoleOperations {
	return glue.ConsoleOperations{
		ConsoleGlue: c,
		Overrides: glue.ConsoleOperationsOverrides{
			StartProgressBar:        c.startProgressBar,
			StartDynamicProgressBar: c.startDynamicProgressBar,
			IsInteractive:           func() bool { return c.interactive },
		},
	}
}

func (c *repoSnapshotTestConsole) In() io.Reader {
	return &c.input
}

func (c *repoSnapshotTestConsole) Out() io.Writer {
	return &c.output
}

func (c *repoSnapshotTestConsole) startProgressBar(
	title string,
	total int,
	extraFields ...glue.ExtraField,
) glue.ProgressWaiter {
	progress := &repoSnapshotTestProgress{title: title, waitErr: c.waitErr, extraFields: extraFields}
	if total == glue.OnlyOneTask {
		total = 1
	}
	progress.SetTotal(int64(total))
	c.progressBars = append(c.progressBars, progress)
	return progress
}

func (c *repoSnapshotTestConsole) startDynamicProgressBar(
	title string,
	extraFields ...glue.ExtraField,
) glue.DynamicProgressWaiter {
	progress := &repoSnapshotTestProgress{title: title, waitErr: c.waitErr, extraFields: extraFields}
	c.progressBars = append(c.progressBars, progress)
	return progress
}

type repoSnapshotTestProgress struct {
	title       string
	waitErr     error
	extraFields []glue.ExtraField
	total       atomic.Int64
	closed      atomic.Bool
	waited      atomic.Bool
	completed   atomic.Bool
	count       atomic.Int64
}

func (p *repoSnapshotTestProgress) Inc() {
	p.count.Add(1)
}

func (p *repoSnapshotTestProgress) IncBy(cnt int64) {
	p.count.Add(cnt)
}

func (p *repoSnapshotTestProgress) GetCurrent() int64 {
	return p.count.Load()
}

func (p *repoSnapshotTestProgress) Close() {
	p.closed.Store(true)
}

func (p *repoSnapshotTestProgress) Wait(context.Context) error {
	p.waited.Store(true)
	if p.completed.Load() {
		return p.waitErr
	}
	if total := p.total.Load(); total > 0 && p.count.Load() >= total {
		p.completed.Store(true)
	}
	return p.waitErr
}

func (p *repoSnapshotTestProgress) SetTotal(total int64) {
	p.total.Store(total)
}

func (p *repoSnapshotTestProgress) AddTotal(delta int64) {
	if delta <= 0 {
		return
	}
	p.total.Add(delta)
}

func (p *repoSnapshotTestProgress) Complete() {
	p.completed.Store(true)
}

func requireRepoSnapshotProgress(
	t *testing.T,
	console *repoSnapshotTestConsole,
	title string,
	total int,
	current int64,
) {
	t.Helper()
	require.Len(t, console.progressBars, 1)
	progress := console.progressBars[0]
	require.Equal(t, title, progress.title)
	require.Equal(t, int64(total), progress.total.Load())
	require.Equal(t, current, progress.GetCurrent())
	require.True(t, progress.completed.Load())
	require.True(t, progress.waited.Load())
	require.True(t, progress.closed.Load())
}

func requireRepoSnapshotFileExists(ctx context.Context, t *testing.T, storage storeapi.Storage, path string) {
	t.Helper()
	exists, err := storage.FileExists(ctx, path)
	require.NoError(t, err)
	require.True(t, exists, path)
}
