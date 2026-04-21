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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/registry"
	"github.com/pingcap/tidb/br/pkg/repo"
	"github.com/pingcap/tidb/br/pkg/task"
	taskrepo "github.com/pingcap/tidb/br/pkg/task/repo"
	"github.com/pingcap/tidb/pkg/ddl"
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
	task.ApplyTiDBRuntimeConfig(&cfg.Config)
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
	task.ApplyTiDBRuntimeConfig(&cfg.Config)
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

func (s *snapshotRepoSuite) taskConfig() taskrepo.Config {
	s.t.Helper()
	cfg := task.DefaultConfig()
	return taskrepo.Config{
		BackendOptions: cfg.BackendOptions,
		Storage:        s.repoURI,
		CipherInfo:     cfg.CipherInfo,
		NoCreds:        cfg.NoCreds,
		SendCreds:      cfg.SendCreds,
	}
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

func (s *snapshotRepoSuite) runRestore(cfg task.RestoreConfig) error {
	s.t.Helper()
	return task.RunRestore(context.Background(), &TestKitGlue{tk: s.tk}, task.FullRestoreCmd, &cfg)
}

func (s *snapshotRepoSuite) restore(backupID repo.BackupID) {
	s.t.Helper()
	cfg := s.restoreConfig(backupID)
	require.NoError(s.t, s.runRestore(cfg))
}

func (s *snapshotRepoSuite) repoPath(rel string) string {
	s.t.Helper()
	return filepath.Join(s.repoDir, filepath.FromSlash(rel))
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
	payload, err := os.ReadFile(s.repoPath(filepath.ToSlash(filepath.Join(repo.SnapshotMetadataDir(backupID), checkpoint.CheckpointMetaPathForBackup))))
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

func (s *snapshotRepoSuite) allDataBackupIDs() []repo.BackupID {
	s.t.Helper()
	matches, err := filepath.Glob(filepath.Join(s.repoDir, "_data", "snapshot", "*", "*"))
	require.NoError(s.t, err)
	uniq := make(map[repo.BackupID]struct{})
	for _, dir := range matches {
		backupID, err := repo.ParseBackupIDStorageName(filepath.Base(dir))
		require.NoError(s.t, err)
		uniq[backupID] = struct{}{}
	}
	ids := make([]repo.BackupID, 0, len(uniq))
	for backupID := range uniq {
		ids = append(ids, backupID)
	}
	return ids
}

func (s *snapshotRepoSuite) allMetadataBackupIDs() []repo.BackupID {
	s.t.Helper()
	matches, err := filepath.Glob(filepath.Join(s.repoDir, "_meta", "snapshot", "*"))
	require.NoError(s.t, err)
	ids := make([]repo.BackupID, 0, len(matches))
	for _, dir := range matches {
		backupID, err := repo.ParseBackupIDStorageName(filepath.Base(dir))
		require.NoError(s.t, err)
		ids = append(ids, backupID)
	}
	return ids
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

func TestSnapshotRepoSuiteTaskCompletedSnapshotAdmin(t *testing.T) {
	suite := newSnapshotRepoSuite(t, "br_repo_v1_task_ops")
	suite.createSimpleTable()
	suite.tk.MustExec("insert into " + suite.dbName + ".t values (1, 10), (2, 20), (3, 30)")

	backupID1 := suite.backup()
	suite.tk.MustExec("insert into " + suite.dbName + ".t values (4, 40)")
	backupID2 := suite.backup()

	cfg := suite.taskConfig()
	ctx := context.Background()

	backupIDs, err := taskrepo.RunRepoSnapshotList(ctx, nil, taskrepo.RepoSnapshotListConfig{Config: cfg})
	require.NoError(t, err)
	require.Equal(t, []repo.BackupID{backupID1, backupID2}, backupIDs)

	tablesPayload, err := taskrepo.RunRepoSnapshotGet(ctx, nil, taskrepo.RepoSnapshotGetConfig{
		Config:   cfg,
		BackupID: backupID2,
		View:     "tables",
	})
	require.NoError(t, err)
	decoder := json.NewDecoder(bytes.NewReader(tablesPayload))
	tables := make([]repoSnapshotTableView, 0, 1)
	for {
		var table repoSnapshotTableView
		err := decoder.Decode(&table)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		tables = append(tables, table)
	}
	require.Len(t, tables, 1)
	require.Equal(t, repoSnapshotTableView{
		DBName:    suite.dbName,
		TableName: "t",
	}, tables[0])

	result, err := taskrepo.RunRepoSnapshotDelete(ctx, nil, taskrepo.RepoSnapshotDeleteConfig{
		Config:   cfg,
		BackupID: backupID1,
	})
	require.NoError(t, err)
	require.Equal(t, backupID1, result.BackupID)
	require.Greater(t, result.MetadataDeleted, 0)
	require.Greater(t, result.DataDeleted, 0)
	require.Zero(t, result.PendingDeleted)
	suite.requirePathMissing(repo.SnapshotMetadataFile(backupID1))
	require.Empty(t, suite.sstFiles(backupID1))
	suite.requirePathExists(repo.SnapshotMetadataFile(backupID2))
	require.NotEmpty(t, suite.sstFiles(backupID2))

	backupIDs, err = taskrepo.RunRepoSnapshotList(ctx, nil, taskrepo.RepoSnapshotListConfig{Config: cfg})
	require.NoError(t, err)
	require.Equal(t, []repo.BackupID{backupID2}, backupIDs)

	suite.tk.MustExec("drop database " + suite.dbName)
	suite.restore(backupID2)
	suite.tk.MustQuery("select count(*) from " + suite.dbName + ".t").Check(testkit.Rows("4"))
}

func TestSnapshotRepoSuiteResumeKeepsBackupIDAndReusesCheckpointData(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("only run snapshot repo resume test with real tikv")
	}
	for _, fp := range []string{
		"github.com/pingcap/tidb/br/pkg/backup/backup-response-error-after-checkpoint",
		"github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files",
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
	suite.tk.MustExec("create table " + suite.dbName + ".t_tail(id int primary key, v varchar(64))")
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
	suite.tk.MustExec("insert into " + suite.dbName + ".t_tail values (1, 'tail-1'), (2, 'tail-2'), (3, 'tail-3')")
	checkpointFP := "github.com/pingcap/tidb/br/pkg/backup/backup-response-error-after-checkpoint"
	killBackupAfterCheckpoint := func(cfg task.BackupConfig) {
		require.NoError(t, failpoint.Enable(checkpointFP, "1*return(\"failpoint: backup response error after checkpoint\")"))
		_, err := suite.runBackup(cfg)
		require.ErrorContains(t, err, "backup response error after checkpoint")
		require.NoError(t, failpoint.Disable(checkpointFP))
	}
	assertSingleBackupPath := func(expected repo.BackupID) {
		require.Equal(t, []repo.BackupID{expected}, suite.pendingBackupIDs())
		checkpointMeta := suite.checkpointMetadata(expected)
		require.Equal(t, uint64(expected), checkpointMeta.BackupID)
		require.NotZero(t, checkpointMeta.BackupTS)
		require.ElementsMatch(t, []repo.BackupID{expected}, suite.allMetadataBackupIDs())
		require.ElementsMatch(t, []repo.BackupID{expected}, suite.allDataBackupIDs())
		require.NotEmpty(t, suite.dataDirs(expected))
	}

	failedCfg := suite.backupConfig()
	failedCfg.RateLimit = 1 << 20
	killBackupAfterCheckpoint(failedCfg)

	pendingIDs := suite.pendingBackupIDs()
	require.Len(t, pendingIDs, 1)
	failedBackupID := pendingIDs[0]
	checkpointLockPath := filepath.ToSlash(filepath.Join(repo.SnapshotMetadataDir(failedBackupID), checkpoint.CheckpointLockPathForBackup))
	partialSSTCount := len(suite.sstFiles(failedBackupID))
	require.Greater(t, partialSSTCount, 0)
	assertSingleBackupPath(failedBackupID)

	resumeCfg := suite.backupConfig()
	resumeCfg.RateLimit = 1 << 20
	resumeCfg.SnapshotBackupOptions.OnPending = taskrepo.OnPendingResume
	_, err := suite.runBackup(resumeCfg)
	require.ErrorContains(t, err, "another BR")
	assertSingleBackupPath(failedBackupID)

	for range 3 {
		require.NoError(t, os.Remove(suite.repoPath(checkpointLockPath)))
		killBackupAfterCheckpoint(resumeCfg)
		assertSingleBackupPath(failedBackupID)
	}

	require.NoError(t, os.Remove(suite.repoPath(checkpointLockPath)))
	resumedBackupID, err := suite.runBackup(resumeCfg)
	require.NoError(t, err)

	finalSSTCount := len(suite.sstFiles(resumedBackupID))
	require.Equal(t, failedBackupID, resumedBackupID)
	require.GreaterOrEqual(t, finalSSTCount, partialSSTCount)
	require.Empty(t, suite.pendingFiles())
	require.ElementsMatch(t, []repo.BackupID{failedBackupID}, suite.allMetadataBackupIDs())
	require.ElementsMatch(t, []repo.BackupID{failedBackupID}, suite.allDataBackupIDs())

	suite.tk.MustExec("drop database " + suite.dbName)

	originSplitTableRegion := atomic.LoadUint32(&ddl.EnableSplitTableRegion)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	defer atomic.StoreUint32(&ddl.EnableSplitTableRegion, originSplitTableRegion)

	restoreCheckpointFP := "github.com/pingcap/tidb/br/pkg/restore/snap_client/corrupt-files"
	restoreCfg := suite.restoreConfig(resumedBackupID)
	restoreCfg.UseCheckpoint = true
	require.NoError(t, failpoint.Enable(restoreCheckpointFP, `return("corrupt-last-table-files")`))
	err = suite.runRestore(restoreCfg)
	require.ErrorContains(t, err, "skip the last table files")
	require.NoError(t, failpoint.Disable(restoreCheckpointFP))

	registryRows := suite.tk.MustQuery(fmt.Sprintf(
		"select id, status from %s.%s order by id",
		registry.RestoreRegistryDBName,
		registry.RestoreRegistryTableName,
	)).Rows()
	require.Len(t, registryRows, 1)
	require.Equal(t, "paused", registryRows[0][1])
	restoreID, err := strconv.ParseUint(fmt.Sprint(registryRows[0][0]), 10, 64)
	require.NoError(t, err)
	checkpointDB := fmt.Sprintf("%s_%d", checkpoint.SnapshotRestoreCheckpointDatabaseName, restoreID)
	t.Cleanup(func() {
		suite.tk.MustExec("drop database if exists `" + checkpointDB + "`")
	})
	suite.tk.MustQuery(
		fmt.Sprintf("select schema_name from information_schema.schemata where schema_name = '%s'", checkpointDB),
	).Check(testkit.Rows(checkpointDB))
	checkpointRows := suite.tk.MustQuery(fmt.Sprintf("select count(*) from `%s`.cpt_data", checkpointDB)).Rows()
	checkpointRangeCount, err := strconv.Atoi(fmt.Sprint(checkpointRows[0][0]))
	require.NoError(t, err)
	require.Greater(t, checkpointRangeCount, 0)

	resumeRestoreCfg := suite.restoreConfig(resumedBackupID)
	resumeRestoreCfg.UseCheckpoint = true
	require.NoError(t, failpoint.Enable(restoreCheckpointFP, `return("only-last-table-files")`))
	require.NoError(t, suite.runRestore(resumeRestoreCfg))
	require.NoError(t, failpoint.Disable(restoreCheckpointFP))

	suite.tk.MustQuery(fmt.Sprintf(
		"select count(*) from %s.%s",
		registry.RestoreRegistryDBName,
		registry.RestoreRegistryTableName,
	)).Check(testkit.Rows("0"))
	suite.tk.MustQuery(
		fmt.Sprintf("select schema_name from information_schema.schemata where schema_name = '%s'", checkpointDB),
	).Check(testkit.Rows())
	suite.tk.MustQuery(fmt.Sprintf("select count(*) from %s.t", suite.dbName)).Check(testkit.Rows("4096"))
	suite.tk.MustQuery(fmt.Sprintf("select count(*) from %s.t_tail", suite.dbName)).Check(testkit.Rows("3"))
}
