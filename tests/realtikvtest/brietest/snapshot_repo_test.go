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

package brietest

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/repo/snapshotpaths"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/testkit"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

type snapshotRepoGlue struct {
	TestKitGlue
	records map[string]uint64
}

func (g *snapshotRepoGlue) Record(name string, value uint64) {
	if g.records == nil {
		g.records = make(map[string]uint64)
	}
	g.records[name] = value
}

type snapshotRepoSuite struct {
	t       *testing.T
	tk      *testkit.TestKit
	repoDir string
	repoURI string
	dbName  string
}

type repoSnapshotTableView struct {
	DBName    string `json:"db-name"`
	TableName string `json:"table-name"`
}

func newSnapshotRepoSuite(t *testing.T, dbName string) *snapshotRepoSuite {
	t.Helper()
	baseDir := getTestTempDir(t)
	tk := initTestKit(t)
	cleanupRestoreRegistry(tk)
	t.Cleanup(func() {
		cleanupRestoreRegistry(tk)
	})
	return &snapshotRepoSuite{
		t:       t,
		tk:      tk,
		repoDir: filepath.Join(baseDir, "repo"),
		repoURI: "local://" + filepath.Join(baseDir, "repo"),
		dbName:  dbName,
	}
}

func cleanupRestoreRegistry(tk *testkit.TestKit) {
	tk.MustExec("DELETE FROM " + registry.RestoreRegistryDBName + "." + registry.RestoreRegistryTableName)
}

func (s *snapshotRepoSuite) backupConfig() task.BackupConfig {
	s.t.Helper()
	cfg := task.DefaultBackupConfig(task.DefaultConfig())
	cfg.Storage = s.repoURI
	cfg.Layout = repo.LayoutRepoV1
	cfg.UseCheckpoint = true
	cfg.CheckRequirements = false
	cfg.Checksum = false
	cfg.FilterStr = []string{s.dbName + ".*"}
	var err error
	cfg.TableFilter, err = filter.Parse(cfg.FilterStr)
	require.NoError(s.t, err)
	return cfg
}

func (s *snapshotRepoSuite) restoreConfig(backupID repo.BackupID) task.RestoreConfig {
	s.t.Helper()
	cfg := task.DefaultRestoreConfig(task.DefaultConfig())
	cfg.Storage = s.repoURI
	cfg.Layout = repo.LayoutRepoV1
	cfg.BackupID = backupID
	cfg.UseCheckpoint = false
	cfg.CheckRequirements = false
	cfg.Checksum = false
	cfg.WithSysTable = false
	cfg.FilterStr = []string{s.dbName + ".*"}
	var err error
	cfg.TableFilter, err = filter.Parse(cfg.FilterStr)
	require.NoError(s.t, err)
	return cfg
}

func (s *snapshotRepoSuite) taskConfig() task.Config {
	s.t.Helper()
	cfg := task.DefaultConfig()
	cfg.Storage = s.repoURI
	return cfg
}

func (s *snapshotRepoSuite) runBackup(cfg task.BackupConfig) (repo.BackupID, error) {
	s.t.Helper()
	glue := &snapshotRepoGlue{
		TestKitGlue: TestKitGlue{tk: s.tk},
		records:     make(map[string]uint64),
	}
	err := task.RunBackup(context.Background(), glue, task.FullBackupCmd, &cfg)
	if err != nil {
		return 0, err
	}
	backupID, ok := glue.records["backup id"]
	require.True(s.t, ok)
	require.NotZero(s.t, backupID)
	return repo.BackupID(backupID), nil
}

func (s *snapshotRepoSuite) backup() repo.BackupID {
	s.t.Helper()
	backupID, err := s.runBackup(s.backupConfig())
	require.NoError(s.t, err)
	return backupID
}

func (s *snapshotRepoSuite) backupDataFiles(backupID repo.BackupID) []*backuppb.File {
	s.t.Helper()
	cfg := s.restoreConfig(backupID)
	_, rootStorage, err := task.GetStorage(context.Background(), cfg.Storage, &cfg.Config)
	require.NoError(s.t, err)
	metaStorage := repo.NewPrefixedStorage(rootStorage, snapshotpaths.MetadataDir(backupID))
	meta, err := task.ReadBackupMetaFromStorage(context.Background(), metautil.MetaFile, metaStorage, &cfg.Config)
	require.NoError(s.t, err)
	reader := metautil.NewMetaReader(meta, metaStorage, &cfg.CipherInfo)
	dbs, err := metautil.LoadBackupTables(context.Background(), reader, false)
	require.NoError(s.t, err)
	files := make([]*backuppb.File, 0)
	for _, db := range dbs {
		for _, table := range db.Tables {
			for _, physicalFiles := range table.FilesOfPhysicals {
				files = append(files, physicalFiles...)
			}
		}
	}
	return files
}

func (s *snapshotRepoSuite) restore(backupID repo.BackupID) {
	s.t.Helper()
	cfg := s.restoreConfig(backupID)
	require.NoError(s.t, task.RunRestore(context.Background(), &TestKitGlue{tk: s.tk}, task.FullRestoreCmd, &cfg))
}

func (s *snapshotRepoSuite) repoPath(rel string) string {
	s.t.Helper()
	return filepath.Join(s.repoDir, filepath.FromSlash(rel))
}

func (s *snapshotRepoSuite) writeRepoFile(rel string, content []byte) {
	s.t.Helper()
	path := s.repoPath(rel)
	require.NoError(s.t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(s.t, os.WriteFile(path, content, 0o644))
}

func (s *snapshotRepoSuite) pendingBackupIDs() []repo.BackupID {
	s.t.Helper()
	files := s.pendingFiles()
	ids := make([]repo.BackupID, 0, len(files))
	for _, file := range files {
		backupID, err := repo.ParseBackupIDStorageName(strings.TrimSuffix(filepath.Base(file), ".json"))
		require.NoError(s.t, err)
		ids = append(ids, backupID)
	}
	return ids
}

func (s *snapshotRepoSuite) checkpointMetadata(backupID repo.BackupID) checkpoint.CheckpointMetadataForBackup {
	s.t.Helper()
	payload, err := os.ReadFile(s.repoPath(filepath.ToSlash(filepath.Join(snapshotpaths.MetadataDir(backupID), checkpoint.CheckpointMetaPathForBackup))))
	require.NoError(s.t, err)
	var meta checkpoint.CheckpointMetadataForBackup
	require.NoError(s.t, json.Unmarshal(payload, &meta))
	return meta
}

func (s *snapshotRepoSuite) pendingFiles() []string {
	s.t.Helper()
	matches, err := filepath.Glob(filepath.Join(s.repoDir, "_meta", "pending", "*", "*.json"))
	require.NoError(s.t, err)
	return matches
}

func (s *snapshotRepoSuite) dataDirs(backupID repo.BackupID) []string {
	s.t.Helper()
	matches, err := filepath.Glob(filepath.Join(s.repoDir, "_data", "snapshot", "*", backupID.StorageName()))
	require.NoError(s.t, err)
	return matches
}

func (s *snapshotRepoSuite) sstFiles(backupID repo.BackupID) []string {
	s.t.Helper()
	root := filepath.Join(s.repoDir, "_data", "snapshot", "*", backupID.StorageName())
	matches := make([]string, 0)
	roots, err := filepath.Glob(root)
	require.NoError(s.t, err)
	for _, dir := range roots {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			if strings.HasSuffix(path, ".sst") {
				matches = append(matches, path)
			}
			return nil
		})
		require.NoError(s.t, err)
	}
	return matches
}

func (s *snapshotRepoSuite) requirePathExists(rel string) {
	s.t.Helper()
	_, err := os.Stat(s.repoPath(rel))
	require.NoError(s.t, err)
}

func (s *snapshotRepoSuite) requirePathMissing(rel string) {
	s.t.Helper()
	_, err := os.Stat(s.repoPath(rel))
	require.ErrorIs(s.t, err, os.ErrNotExist)
}

func (s *snapshotRepoSuite) createSimpleTable() {
	s.t.Helper()
	s.tk.MustExec("drop database if exists " + s.dbName)
	s.t.Cleanup(func() {
		s.tk.MustExec("drop database if exists " + s.dbName)
	})
	s.tk.MustExec("create database " + s.dbName)
	s.tk.MustExec("create table " + s.dbName + ".t(id int primary key, v int)")
}

func TestSnapshotRepoSuiteBackupRestoreLayout(t *testing.T) {
	suite := newSnapshotRepoSuite(t, "br_repo_v1_layout")
	suite.createSimpleTable()
	suite.tk.MustExec("insert into " + suite.dbName + ".t values (1, 10), (2, 20), (3, 30)")

	backupID1 := suite.backup()
	suite.tk.MustExec("insert into " + suite.dbName + ".t values (4, 40), (5, 50)")
	backupID2 := suite.backup()

	require.Greater(t, uint64(backupID1), uint64(0))
	require.Greater(t, uint64(backupID2), uint64(backupID1))
	require.Len(t, backupID1.StorageName(), 16)
	require.Len(t, backupID2.StorageName(), 16)
	require.Equal(t, strings.ToUpper(backupID1.StorageName()), backupID1.StorageName())
	require.Equal(t, strings.ToUpper(backupID2.StorageName()), backupID2.StorageName())

	suite.requirePathExists("backup.lock")
	suite.requirePathExists(snapshotpaths.RepoMetaPath)
	suite.requirePathExists(snapshotpaths.MetadataFile(backupID1))
	suite.requirePathExists(snapshotpaths.MetadataFile(backupID2))
	require.Len(t, suite.pendingFiles(), 0)

	dataDirs1 := suite.dataDirs(backupID1)
	dataDirs2 := suite.dataDirs(backupID2)
	require.NotEmpty(t, dataDirs1)
	require.NotEmpty(t, dataDirs2)
	require.NotEmpty(t, suite.sstFiles(backupID1))
	require.NotEmpty(t, suite.sstFiles(backupID2))
	for _, backupID := range []repo.BackupID{backupID1, backupID2} {
		files := suite.backupDataFiles(backupID)
		require.NotEmpty(t, files)
		for _, file := range files {
			parts := strings.Split(file.Name, "/")
			require.GreaterOrEqual(t, len(parts), 5)
			require.Equal(t, "_data", parts[0])
			require.Equal(t, "snapshot", parts[1])
			require.Equal(t, backupID.StorageName(), parts[3])
		}
	}
	for _, dir := range append(dataDirs1, dataDirs2...) {
		storeID := filepath.Base(filepath.Dir(dir))
		_, err := strconv.ParseUint(storeID, 10, 64)
		require.NoError(t, err)
	}
	suite.requirePathMissing(filepath.ToSlash(filepath.Join("_data", "snapshot", backupID1.StorageName())))
	suite.requirePathMissing(filepath.ToSlash(filepath.Join("_data", "snapshot", backupID2.StorageName())))

	suite.tk.MustExec("drop database " + suite.dbName)
	suite.restore(backupID1)
	suite.tk.MustQuery("select count(*) from " + suite.dbName + ".t").Check(testkit.Rows("3"))

	suite.tk.MustExec("drop database " + suite.dbName)
	suite.restore(backupID2)
	suite.tk.MustQuery("select count(*) from " + suite.dbName + ".t").Check(testkit.Rows("5"))
}

func TestSnapshotRepoSuitePendingIndexBehavior(t *testing.T) {
	suite := newSnapshotRepoSuite(t, "br_repo_v1_pending")
	suite.createSimpleTable()
	suite.tk.MustExec("insert into " + suite.dbName + ".t values (1, 10)")

	backupCfg := suite.backupConfig()
	cfgHash, err := backupCfg.Hash()
	require.NoError(t, err)

	backupID1 := suite.backup()
	stalePendingFile := snapshotpaths.PendingFile(cfgHash, backupID1)
	suite.writeRepoFile(stalePendingFile, []byte("{}"))

	foreignPendingFile := filepath.ToSlash(filepath.Join(
		"_meta", "pending", strings.Repeat("A", 64), repo.BackupID(0xF0F0).StorageName()+".json",
	))
	suite.writeRepoFile(foreignPendingFile, []byte("{}"))

	suite.tk.MustExec("insert into " + suite.dbName + ".t values (2, 20)")
	backupID2 := suite.backup()
	require.NotEqual(t, backupID1, backupID2)
	suite.requirePathMissing(stalePendingFile)
	suite.requirePathExists(foreignPendingFile)

	ambiguous1 := repo.BackupID(0x1001)
	ambiguous2 := repo.BackupID(0x1002)
	suite.writeRepoFile(snapshotpaths.PendingFile(cfgHash, ambiguous1), []byte("{}"))
	suite.writeRepoFile(snapshotpaths.PendingFile(cfgHash, ambiguous2), []byte("{}"))
	suite.writeRepoFile(
		filepath.ToSlash(filepath.Join(snapshotpaths.MetadataDir(ambiguous1), checkpoint.CheckpointMetaPathForBackup)),
		[]byte("{}"),
	)
	suite.writeRepoFile(
		filepath.ToSlash(filepath.Join(snapshotpaths.MetadataDir(ambiguous2), checkpoint.CheckpointMetaPathForBackup)),
		[]byte("{}"),
	)

	ambiguousCfg := suite.backupConfig()
	err = task.RunBackup(context.Background(), &TestKitGlue{tk: suite.tk}, task.FullBackupCmd, &ambiguousCfg)
	require.Error(t, err)
	require.ErrorContains(t, err, "multiple unfinished repo-v1 backups")
}

func TestSnapshotRepoSuiteTaskCompletedSnapshotAdmin(t *testing.T) {
	suite := newSnapshotRepoSuite(t, "br_repo_v1_task_ops")
	suite.createSimpleTable()
	suite.tk.MustExec("insert into " + suite.dbName + ".t values (1, 10), (2, 20), (3, 30)")

	backupID1 := suite.backup()
	suite.tk.MustExec("insert into " + suite.dbName + ".t values (4, 40)")
	backupID2 := suite.backup()

	cfg := suite.taskConfig()
	ctx := context.Background()

	backupIDs, err := task.RunRepoSnapshotList(ctx, nil, task.RepoSnapshotListConfig{Config: cfg})
	require.NoError(t, err)
	require.Equal(t, []repo.BackupID{backupID1, backupID2}, backupIDs)

	tablesPayload, err := task.RunRepoSnapshotGet(ctx, nil, task.RepoSnapshotGetConfig{
		Config:   cfg,
		BackupID: backupID2,
		View:     "tables",
	})
	require.NoError(t, err)
	var tables []repoSnapshotTableView
	require.NoError(t, json.Unmarshal(tablesPayload, &tables))
	require.Len(t, tables, 1)
	require.Equal(t, repoSnapshotTableView{
		DBName:    suite.dbName,
		TableName: "t",
	}, tables[0])

	result, err := task.RunRepoSnapshotDelete(ctx, nil, task.RepoSnapshotDeleteConfig{
		Config:   cfg,
		BackupID: backupID1,
	})
	require.NoError(t, err)
	require.Equal(t, backupID1, result.BackupID)
	require.Greater(t, result.MetadataDeleted, 0)
	require.Greater(t, result.DataDeleted, 0)
	require.Zero(t, result.PendingDeleted)
	suite.requirePathMissing(snapshotpaths.MetadataFile(backupID1))
	require.Empty(t, suite.sstFiles(backupID1))
	suite.requirePathExists(snapshotpaths.MetadataFile(backupID2))
	require.NotEmpty(t, suite.sstFiles(backupID2))

	backupIDs, err = task.RunRepoSnapshotList(ctx, nil, task.RepoSnapshotListConfig{Config: cfg})
	require.NoError(t, err)
	require.Equal(t, []repo.BackupID{backupID2}, backupIDs)

	suite.tk.MustExec("drop database " + suite.dbName)
	suite.restore(backupID2)
	suite.tk.MustQuery("select count(*) from " + suite.dbName + ".t").Check(testkit.Rows("4"))
}

func TestSnapshotRepoSuiteTaskCleanupArtifacts(t *testing.T) {
	suite := newSnapshotRepoSuite(t, "br_repo_v1_task_pending")
	suite.createSimpleTable()
	suite.tk.MustExec("insert into " + suite.dbName + ".t values (1, 10), (2, 20)")

	backupID := suite.backup()
	cfg := suite.taskConfig()
	ctx := context.Background()

	stalePendingFile := snapshotpaths.PendingFile([]byte("stale"), backupID)
	suite.writeRepoFile(stalePendingFile, []byte("{}"))

	discardResult, err := task.RunRepoSnapshotPendingDiscard(ctx, nil, task.RepoSnapshotPendingDiscardConfig{
		Config:   cfg,
		BackupID: backupID,
	})
	require.NoError(t, err)
	require.Equal(t, backupID, discardResult.BackupID)
	require.True(t, discardResult.StalePending)
	require.Equal(t, 1, discardResult.PendingDeleted)
	require.Zero(t, discardResult.MetadataDeleted)
	require.Zero(t, discardResult.DataDeleted)
	suite.requirePathMissing(stalePendingFile)
	suite.requirePathExists(snapshotpaths.MetadataFile(backupID))
	require.NotEmpty(t, suite.sstFiles(backupID))

	orphanID := repo.BackupID(0xDEAD)
	orphanPath := filepath.ToSlash(filepath.Join(snapshotpaths.StoreDataPrefix(9, orphanID), "orphan.sst"))
	suite.writeRepoFile(orphanPath, []byte("orphan"))

	orphans, err := task.RunRepoSnapshotOrphansList(ctx, nil, task.RepoSnapshotOrphansConfig{Config: cfg})
	require.NoError(t, err)
	require.Equal(t, []string{orphanPath}, orphans)

	deleted, err := task.RunRepoSnapshotOrphansDelete(ctx, nil, task.RepoSnapshotOrphansConfig{Config: cfg})
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	suite.requirePathMissing(orphanPath)
	suite.requirePathExists(snapshotpaths.MetadataFile(backupID))
	require.NotEmpty(t, suite.sstFiles(backupID))

	backupIDs, err := task.RunRepoSnapshotList(ctx, nil, task.RepoSnapshotListConfig{Config: cfg})
	require.NoError(t, err)
	require.Equal(t, []repo.BackupID{backupID}, backupIDs)
}

func TestSnapshotRepoSuiteResumeKeepsBackupIDAndReusesCheckpointData(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("only run snapshot repo resume test with real tikv")
	}
	for _, fp := range []string{
		"github.com/pingcap/tidb/br/pkg/backup/backup-response-error-after-checkpoint",
	} {
		fp := fp
		t.Cleanup(func() {
			_ = failpoint.Disable(fp)
		})
	}

	suite := newSnapshotRepoSuite(t, "br_repo_v1_resume")
	suite.tk.MustExec("drop database if exists " + suite.dbName)
	t.Cleanup(func() {
		suite.tk.MustExec("drop database if exists " + suite.dbName)
	})
	suite.tk.MustExec("create database " + suite.dbName)
	suite.tk.MustExec("create table " + suite.dbName + ".t(id int primary key, v longtext)")
	_ = suite.tk.MustQuery(fmt.Sprintf("split table %s.t between (0) and (4096000) regions 32", suite.dbName))

	payload := strings.Repeat("x", 3072)
	for start := 1; start <= 4096; start += 128 {
		end := min(start+128, 4097)
		var builder strings.Builder
		builder.WriteString("insert into ")
		builder.WriteString(suite.dbName)
		builder.WriteString(".t values ")
		for id := start; id < end; id++ {
			if id > start {
				builder.WriteByte(',')
			}
			builder.WriteString(fmt.Sprintf("(%d,'%s')", id*1000, payload))
		}
		suite.tk.MustExec(builder.String())
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/br/pkg/backup/backup-response-error-after-checkpoint", "1*return(\"failpoint: backup response error after checkpoint\")"))
	failedCfg := suite.backupConfig()
	failedCfg.RateLimit = 1 << 20
	_, err := suite.runBackup(failedCfg)
	require.ErrorContains(t, err, "backup response error after checkpoint")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/br/pkg/backup/backup-response-error-after-checkpoint"))

	pendingIDs := suite.pendingBackupIDs()
	require.Len(t, pendingIDs, 1)
	failedBackupID := pendingIDs[0]
	checkpointMeta := suite.checkpointMetadata(failedBackupID)
	require.Equal(t, uint64(failedBackupID), checkpointMeta.BackupID)
	require.NotZero(t, checkpointMeta.BackupTS)
	checkpointLockPath := filepath.ToSlash(filepath.Join(snapshotpaths.MetadataDir(failedBackupID), checkpoint.CheckpointLockPathForBackup))
	require.NoError(t, os.Remove(suite.repoPath(checkpointLockPath)))
	partialSSTCount := len(suite.sstFiles(failedBackupID))
	require.Greater(t, partialSSTCount, 0)

	resumeCfg := suite.backupConfig()
	resumeCfg.RateLimit = 1 << 20
	require.NoError(t, json.Unmarshal([]byte(`{"on-pending":"resume"}`), &resumeCfg.SnapshotRepoBackupOptions))
	resumedBackupID, err := suite.runBackup(resumeCfg)
	require.NoError(t, err)

	finalSSTCount := len(suite.sstFiles(resumedBackupID))
	require.Equal(t, failedBackupID, resumedBackupID)
	require.GreaterOrEqual(t, finalSSTCount, partialSSTCount)
	require.Empty(t, suite.pendingFiles())

	suite.tk.MustExec("drop database " + suite.dbName)
	suite.restore(resumedBackupID)
	suite.tk.MustQuery(fmt.Sprintf("select count(*) from %s.t", suite.dbName)).Check(testkit.Rows("4096"))
}
