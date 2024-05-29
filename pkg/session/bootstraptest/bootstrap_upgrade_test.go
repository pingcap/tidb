// Copyright 2022 PingCAP, Inc.
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

package bootstraptest_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	tidb_util "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	atomicutil "go.uber.org/atomic"
)

func TestUpgradeVersion83AndVersion84(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	ver, err := session.GetBootstrapVersion(tk.Session())
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	statsHistoryTblFields := []struct {
		field string
		tp    string
	}{
		{"table_id", "bigint(64)"},
		{"stats_data", "longblob"},
		{"seq_no", "bigint(64)"},
		{"version", "bigint(64)"},
		{"create_time", "datetime(6)"},
	}
	rStatsHistoryTbl, err := tk.Exec(`desc mysql.stats_history`)
	require.NoError(t, err)
	req := rStatsHistoryTbl.NewChunk(nil)
	require.NoError(t, rStatsHistoryTbl.Next(ctx, req))
	require.Equal(t, 5, req.NumRows())
	for i := 0; i < 5; i++ {
		row := req.GetRow(i)
		require.Equal(t, statsHistoryTblFields[i].field, strings.ToLower(row.GetString(0)))
		require.Equal(t, statsHistoryTblFields[i].tp, strings.ToLower(row.GetString(1)))
	}

	statsMetaHistoryTblFields := []struct {
		field string
		tp    string
	}{
		{"table_id", "bigint(64)"},
		{"modify_count", "bigint(64)"},
		{"count", "bigint(64)"},
		{"version", "bigint(64)"},
		{"source", "varchar(40)"},
		{"create_time", "datetime(6)"},
	}
	rStatsMetaHistoryTbl, err := tk.Exec(`desc mysql.stats_meta_history`)
	require.NoError(t, err)
	req = rStatsMetaHistoryTbl.NewChunk(nil)
	require.NoError(t, rStatsMetaHistoryTbl.Next(ctx, req))
	require.Equal(t, 6, req.NumRows())
	for i := 0; i < 6; i++ {
		row := req.GetRow(i)
		require.Equal(t, statsMetaHistoryTblFields[i].field, strings.ToLower(row.GetString(0)))
		require.Equal(t, statsMetaHistoryTblFields[i].tp, strings.ToLower(row.GetString(1)))
	}
}

func revertVersionAndVariables(t *testing.T, se sessiontypes.Session, ver int) {
	session.MustExec(t, se, fmt.Sprintf("update mysql.tidb set variable_value='%d' where variable_name='tidb_server_version'", ver))
	if ver <= 195 {
		// for version <= version195, tidb_enable_dist_task should be disabled before upgrade
		session.MustExec(t, se, "update mysql.global_variables set variable_value='off' where variable_name='tidb_enable_dist_task'")
	}
}

func TestUpgradeVersion66(t *testing.T) {
	ctx := context.Background()
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	seV65 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(65))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	revertVersionAndVariables(t, seV65, 65)
	session.MustExec(t, seV65, "set @@global.tidb_track_aggregate_memory_usage = 0")
	session.MustExec(t, seV65, "commit")
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV65)
	require.NoError(t, err)
	require.Equal(t, int64(65), ver)
	dom.Close()
	domV66, err := session.BootstrapSession(store)
	require.NoError(t, err)

	seV66 := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seV66)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)
	r := session.MustExecToRecodeSet(t, seV66, `select @@global.tidb_track_aggregate_memory_usage, @@session.tidb_track_aggregate_memory_usage`)
	req := r.NewChunk(nil)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, 1, req.NumRows())
	row := req.GetRow(0)
	require.Equal(t, int64(1), row.GetInt64(0))
	require.Equal(t, int64(1), row.GetInt64(1))
	domV66.Close()
}

func TestUpgradeVersion74(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		oldValue int
		newValue int
	}{
		{200, 3000},
		{3000, 3000},
		{3001, 3001},
	}

	for _, ca := range cases {
		func() {
			store, dom := session.CreateStoreAndBootstrap(t)
			defer func() { require.NoError(t, store.Close()) }()

			seV73 := session.CreateSessionAndSetID(t, store)
			txn, err := store.Begin()
			require.NoError(t, err)
			m := meta.NewMeta(txn)
			err = m.FinishBootstrap(int64(73))
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)
			revertVersionAndVariables(t, seV73, 72)
			session.MustExec(t, seV73, "set @@global.tidb_stmt_summary_max_stmt_count = "+strconv.Itoa(ca.oldValue))
			session.MustExec(t, seV73, "commit")
			session.UnsetStoreBootstrapped(store.UUID())
			ver, err := session.GetBootstrapVersion(seV73)
			require.NoError(t, err)
			require.Equal(t, int64(72), ver)
			dom.Close()
			domV74, err := session.BootstrapSession(store)
			require.NoError(t, err)
			defer domV74.Close()
			seV74 := session.CreateSessionAndSetID(t, store)
			ver, err = session.GetBootstrapVersion(seV74)
			require.NoError(t, err)
			require.Equal(t, session.CurrentBootstrapVersion, ver)
			r := session.MustExecToRecodeSet(t, seV74, `SELECT @@global.tidb_stmt_summary_max_stmt_count`)
			req := r.NewChunk(nil)
			require.NoError(t, r.Next(ctx, req))
			require.Equal(t, 1, req.NumRows())
			row := req.GetRow(0)
			require.Equal(t, strconv.Itoa(ca.newValue), row.GetString(0))
		}()
	}
}

func TestUpgradeVersion75(t *testing.T) {
	ctx := context.Background()

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV74 := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(int64(74))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	revertVersionAndVariables(t, seV74, 74)
	session.MustExec(t, seV74, "commit")
	session.MustExec(t, seV74, "ALTER TABLE mysql.user DROP PRIMARY KEY")
	session.MustExec(t, seV74, "ALTER TABLE mysql.user MODIFY COLUMN Host CHAR(64)")
	session.MustExec(t, seV74, "ALTER TABLE mysql.user ADD PRIMARY KEY(Host, User)")
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV74)
	require.NoError(t, err)
	require.Equal(t, int64(74), ver)
	r := session.MustExecToRecodeSet(t, seV74, `desc mysql.user`)
	req := r.NewChunk(nil)
	row := req.GetRow(0)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, "host", strings.ToLower(row.GetString(0)))
	require.Equal(t, "char(64)", strings.ToLower(row.GetString(1)))
	dom.Close()
	domV75, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domV75.Close()
	seV75 := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seV75)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)
	r = session.MustExecToRecodeSet(t, seV75, `desc mysql.user`)
	req = r.NewChunk(nil)
	row = req.GetRow(0)
	require.NoError(t, r.Next(ctx, req))
	require.Equal(t, "host", strings.ToLower(row.GetString(0)))
	require.Equal(t, "char(255)", strings.ToLower(row.GetString(1)))
}

func TestUpgradeVersionMockLatest(t *testing.T) {
	mock := true
	session.WithMockUpgrade = &mock

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(session.CurrentBootstrapVersion - 1)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	revertVersionAndVariables(t, seV, int(session.CurrentBootstrapVersion-1))
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion-1, ver)
	dom.Close()
	startUpgrade(store)
	domLatestV, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domLatestV.Close()

	finishUpgrade(store)

	seLatestV := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seLatestV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion+1, ver)

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("show create table mysql.mock_sys_t").Check(testkit.Rows(
		"mock_sys_t CREATE TABLE `mock_sys_t` (\n" +
			"  `c1` int(11) DEFAULT NULL,\n" +
			"  `c2` int(11) NOT NULL,\n" +
			"  `c11` char(10) DEFAULT NULL,\n" +
			"  `c4` bigint(20) DEFAULT NULL,\n" +
			"  `mayNullCol` bigint(20) NOT NULL DEFAULT '1',\n" +
			"  KEY `fk_c1` (`c1`),\n" +
			"  UNIQUE KEY `idx_uc2` (`c2`),\n" +
			"  KEY `idx_c2` (`c2`),\n" +
			"  KEY `idx_v` (`c1`) /*!80000 INVISIBLE */,\n" +
			"  KEY `rename_idx2` (`c1`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table mysql.mock_sys_partition").Check(testkit.Rows(
		"mock_sys_partition CREATE TABLE `mock_sys_partition` (\n" +
			"  `c1` int(11) NOT NULL,\n" +
			"  `c2` int(11) DEFAULT NULL,\n" +
			"  `c3` int(11) DEFAULT NULL,\n" +
			"  UNIQUE KEY `c3_index` (`c1`),\n" +
			"  PRIMARY KEY (`c1`) /*T![clustered_index] NONCLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
			"PARTITION BY RANGE (`c1`)\n" +
			"(PARTITION `p0` VALUES LESS THAN (1024),\n" +
			" PARTITION `p1` VALUES LESS THAN (2048),\n" +
			" PARTITION `p2` VALUES LESS THAN (3072),\n" +
			" PARTITION `p3` VALUES LESS THAN (4096),\n" +
			" PARTITION `p4` VALUES LESS THAN (7096))"))
}

// TestUpgradeVersionWithUpgradeHTTPOp tests SupportUpgradeHTTPOpVer upgrade SupportUpgradeHTTPOpVer++ with HTTP op.
func TestUpgradeVersionWithUpgradeHTTPOp(t *testing.T) {
	mock := true
	session.WithMockUpgrade = &mock
	session.MockUpgradeToVerLatestKind = session.MockSimpleUpgradeToVerLatest

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(session.SupportUpgradeHTTPOpVer)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	revertVersionAndVariables(t, seV, int(session.SupportUpgradeHTTPOpVer))
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV)
	require.NoError(t, err)
	require.Equal(t, session.SupportUpgradeHTTPOpVer, ver)
	dom.Close()

	// Start the upgrade test.
	// Current cluster state is normal.
	isUpgrading, err := session.IsUpgradingClusterState(seV)
	require.NoError(t, err)
	require.Equal(t, false, isUpgrading)
	upgradeHandler := handler.NewClusterUpgradeHandler(store)
	upgradeHandler.StartUpgrade()
	domLatestV, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domLatestV.Close()
	seLatestV := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seLatestV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion+1, ver)
	// Current cluster state is upgrading.
	isUpgrading, err = session.IsUpgradingClusterState(seLatestV)
	require.NoError(t, err)
	require.Equal(t, true, isUpgrading)
	upgradeHandler.FinishUpgrade()
	// Upgrading is finished and current cluster state is normal.
	isUpgrading, err = session.IsUpgradingClusterState(seV)
	require.NoError(t, err)
	require.Equal(t, false, isUpgrading)
}

// TestUpgradeVersionWithoutUpgradeHTTPOp tests SupportUpgradeHTTPOpVer upgrade SupportUpgradeHTTPOpVer++ without HTTP op.
func TestUpgradeVersionWithoutUpgradeHTTPOp(t *testing.T) {
	mock := true
	session.WithMockUpgrade = &mock
	session.MockUpgradeToVerLatestKind = session.MockSimpleUpgradeToVerLatest

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(session.SupportUpgradeHTTPOpVer)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	revertVersionAndVariables(t, seV, int(session.SupportUpgradeHTTPOpVer))
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV)
	require.NoError(t, err)
	require.Equal(t, session.SupportUpgradeHTTPOpVer, ver)
	dom.Close()

	// Start the upgrade test.
	// Current cluster state is normal.
	isUpgrading, err := session.IsUpgradingClusterState(seV)
	require.NoError(t, err)
	require.Equal(t, false, isUpgrading)
	domLatestV, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domLatestV.Close()
	seLatestV := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seLatestV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion+1, ver)
	// Current cluster state is upgrading.
	isUpgrading, err = session.IsUpgradingClusterState(seLatestV)
	require.NoError(t, err)
	require.Equal(t, false, isUpgrading)
	upgradeHandler := handler.NewClusterUpgradeHandler(store)
	upgradeHandler.FinishUpgrade()
	// Upgrading is finished and current cluster state is normal.
	isUpgrading, err = session.IsUpgradingClusterState(seV)
	require.NoError(t, err)
	require.Equal(t, false, isUpgrading)
}

func TestUpgradeVersionForPausedJob(t *testing.T) {
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(session.CurrentBootstrapVersion - 1)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	revertVersionAndVariables(t, seV, int(session.CurrentBootstrapVersion-1))
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion-1, ver)

	// Add a paused DDL job before upgrade.
	session.MustExec(t, seV, "create table test.upgrade_tbl(a int)")
	ch := make(chan struct{})
	var jobID int64
	hook := &callback.TestDDLCallback{}
	hook.OnJobRunAfterExported = func(job *model.Job) {
		if job.SchemaState == model.StateWriteOnly {
			se := session.CreateSessionAndSetID(t, store)
			session.MustExec(t, se, fmt.Sprintf("admin pause ddl jobs %d", job.ID))
			ch <- struct{}{}
			jobID = job.ID
		}
	}
	dom.DDL().SetHook(hook)
	go func() {
		_, err = execute(context.Background(), seV, "alter table test.upgrade_tbl add index idx(a)")
	}()

	<-ch
	dom.Close()
	// Make sure upgrade is successful.
	startUpgrade(store)
	domLatestV, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domLatestV.Close()
	seLatestV := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seLatestV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	finishUpgrade(store)

	// Resume the DDL job, then add index operation can be executed successfully.
	session.MustExec(t, seLatestV, fmt.Sprintf("admin resume ddl jobs %d", jobID))
	checkDDLJobExecSucc(t, seLatestV, jobID)
}

// checkDDLJobExecSucc is used to make sure the DDL operation is successful.
func checkDDLJobExecSucc(t *testing.T, se sessiontypes.Session, jobID int64) {
	sql := fmt.Sprintf(" admin show ddl jobs where job_id=%d", jobID)
	suc := false
	for i := 0; i < 20; i++ {
		rows, err := execute(context.Background(), se, sql)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, rows[0].GetString(2), "upgrade_tbl")

		state := rows[0].GetString(11)
		if state == "synced" {
			suc = true
			break
		}
		time.Sleep(time.Millisecond * 200)
	}
	require.True(t, suc)
}

// TestUpgradeVersionForSystemPausedJob tests mock the first upgrade failed, and it has a mock system DDL in queue.
// Then we do re-upgrade(This operation will pause all DDL jobs by the system).
func TestUpgradeVersionForSystemPausedJob(t *testing.T) {
	// Mock a general and a reorg job in bootstrap.
	mock := true
	session.WithMockUpgrade = &mock
	session.MockUpgradeToVerLatestKind = session.MockSimpleUpgradeToVerLatest

	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(session.CurrentBootstrapVersion - 1)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	revertVersionAndVariables(t, seV, int(session.CurrentBootstrapVersion-1))
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion-1, ver)

	// Add a paused DDL job before upgrade.
	session.MustExec(t, seV, "create table mysql.upgrade_tbl(a int)")
	ch := make(chan struct{})
	var jobID int64
	hook := &callback.TestDDLCallback{}
	hook.OnJobRunAfterExported = func(job *model.Job) {
		if job.SchemaState == model.StateDeleteOnly {
			se := session.CreateSessionAndSetID(t, store)
			session.MustExec(t, se, fmt.Sprintf("admin pause ddl jobs %d", job.ID))
		}
		if job.State == model.JobStatePaused && jobID == 0 {
			// Mock pause the ddl job by system.
			job.AdminOperator = model.AdminCommandBySystem
			ch <- struct{}{}
			jobID = job.ID
		}
	}
	dom.DDL().SetHook(hook)
	go func() {
		_, err = execute(context.Background(), seV, "alter table mysql.upgrade_tbl add column b int")
	}()

	<-ch
	dom.Close()
	// Make sure upgrade is successful.
	startUpgrade(store)
	domLatestV, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domLatestV.Close()
	seLatestV := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seLatestV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion+1, ver)

	finishUpgrade(store)

	checkDDLJobExecSucc(t, seLatestV, jobID)
}

func TestUpgradeVersionForResumeJob(t *testing.T) {
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	seV := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(session.CurrentBootstrapVersion - 1)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	revertVersionAndVariables(t, seV, int(session.CurrentBootstrapVersion-1))
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion-1, ver)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockResumeAllJobsFailed", `return(true)`))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockResumeAllJobsFailed")

	// Add a paused DDL job before upgrade.
	session.MustExec(t, seV, "create table test.upgrade_tbl(a int, b int)")
	session.MustExec(t, seV, "create table test.upgrade_tbl1(a int, b int)")
	ch := make(chan struct{})
	hook := &callback.TestDDLCallback{}
	var jobID int64
	wg := sync.WaitGroup{}
	wg.Add(1)
	times := 0
	hook.OnGetJobAfterExported = func(tp string, job *model.Job) {
		if job.SchemaState == model.StateWriteOnly && times == 0 {
			ch <- struct{}{}
			jobID = job.ID
			times = 1
		}
		if job.ID == jobID && job.State == model.JobStateDone && job.SchemaState == model.StatePublic {
			wg.Done()
		}
	}

	dom.DDL().SetHook(hook)
	go func() {
		// This "add index" job will be paused when upgrading.
		_, _ = execute(context.Background(), seV, "alter table test.upgrade_tbl add index idx(a)")
	}()

	<-ch
	dom.Close()
	// Make sure upgrade is successful.
	startUpgrade(store)
	domLatestV, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domLatestV.Close()
	domLatestV.DDL().SetHook(hook)
	finishUpgrade(store)
	seLatestV := session.CreateSessionAndSetID(t, store)
	// Add a new DDL (an "add index" job uses a different table than the previous DDL job) to the DDL table.
	session.MustExec(t, seLatestV, "alter table test.upgrade_tbl1 add index idx2(a)")
	ver, err = session.GetBootstrapVersion(seLatestV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion, ver)

	wg.Wait()
	// Make sure the second add index operation is successful.
	sql := fmt.Sprintf("select job_meta from mysql.tidb_ddl_history where job_id >=%d order by job_id", jobID)
	rows, err := execute(context.Background(), seLatestV, sql)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rows), 2)
	var idxFinishTS uint64
	for i, row := range rows {
		jobBinary := row.GetBytes(0)
		runJob := model.Job{}
		err := runJob.Decode(jobBinary)
		require.NoError(t, err)
		require.Equal(t, model.JobStateSynced.String(), runJob.State.String())
		if i == 0 {
			// The first add index op.
			idxFinishTS = runJob.BinlogInfo.FinishedTS
		} else {
			// The second add index op.
			if strings.Contains(runJob.TableName, "upgrade_tbl") {
				require.Greater(t, runJob.BinlogInfo.FinishedTS, idxFinishTS)
			} else {
				// The upgrade DDL ops. These jobs' finishedTS must less than add index ops.
				require.Less(t, runJob.BinlogInfo.FinishedTS, idxFinishTS)
			}
		}
	}
}

func execute(ctx context.Context, s sessionctx.Context, query string) ([]chunk.Row, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnDDL)
	rs, err := s.GetSQLExecutor().ExecuteInternal(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func startUpgrade(store kv.Storage) {
	upgradeHandler := handler.NewClusterUpgradeHandler(store)
	upgradeHandler.StartUpgrade()
}

func finishUpgrade(store kv.Storage) {
	upgradeHandler := handler.NewClusterUpgradeHandler(store)
	upgradeHandler.FinishUpgrade()
}

// TestUpgradeWithPauseDDL adds a user and a system DB's DDL operations, before every test bootstrap(DDL operation). It tests:
//
//	1.Before and after each test bootstrap, the DDL of the user DB is paused, but the DDL of the system DB is not paused.
//	2.Check user DDLs are handled after system DDLs.
func TestUpgradeWithPauseDDL(t *testing.T) {
	session.SupportUpgradeHTTPOpVer--
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	store, dom := session.CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()

	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	_, err = execute(context.Background(), se, "create table test.pause_user_ddl_t(a int, b int)")
	require.NoError(t, err)
	_, err = execute(context.Background(), se, "create table mysql.pause_user_ddl_t(a int, b int)")
	require.NoError(t, err)
	_, err = execute(context.Background(), se, "create table test.pause_user_ddl_t1(a int, b int)")
	require.NoError(t, err)
	_, err = execute(context.Background(), se, "create table mysql.pause_user_ddl_t1(a int, b int)")
	require.NoError(t, err)

	tc := session.TestCallback{Cnt: atomicutil.NewInt32(0)}
	sql := "select job_meta, processing from mysql.tidb_ddl_job where job_id in (select min(job_id) from mysql.tidb_ddl_job group by schema_ids, table_ids, processing) order by processing desc, job_id"
	tc.OnBootstrapBeforeExported = func(s sessiontypes.Session) {
		rows, err := execute(context.Background(), s, sql)
		require.NoError(t, err)
		require.Len(t, rows, 0)
	}

	wg := sync.WaitGroup{}
	asyncExecDDL := func(query string) {
		ch := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch <- struct{}{}
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			_, err := tk.ExecWithContext(context.Background(), query)
			if err != nil {
				// Maybe the bootstrap domain is closed.
				require.Equal(t, err.Error(), context.Canceled.Error())
			}
		}()
		<-ch
	}
	checkDDLJobState := func(s sessiontypes.Session) {
		rows, err := execute(context.Background(), s, sql)
		require.NoError(t, err)
		for _, row := range rows {
			jobBinary := row.GetBytes(0)
			runJob := model.Job{}
			err := runJob.Decode(jobBinary)
			require.NoError(t, err)
			cmt := fmt.Sprintf("job: %s", runJob.String())
			isPause := runJob.IsPausedBySystem() || runJob.IsPausing()
			if tidb_util.IsSysDB(runJob.SchemaName) {
				require.False(t, isPause, cmt)
			} else {
				require.True(t, isPause, cmt)
			}
		}
	}
	// Before every test bootstrap(DDL operation), we add a user and a system DB's DDL operations.
	tc.OnBootstrapExported = func(s sessiontypes.Session) {
		var query1, query2 string
		switch tc.Cnt.Load() % 2 {
		case 0:
			query1 = fmt.Sprintf("alter table mysql.pause_user_ddl_t add index idx_%d(a)", tc.Cnt.Load())
			query2 = fmt.Sprintf("alter table test.pause_user_ddl_t add column c_%d int", tc.Cnt.Load())
		case 1:
			// Make sure case0 and case1 use different table ID. Then case1's table won't be filtered because they use the same table ID.
			query1 = fmt.Sprintf("alter table test.pause_user_ddl_t1 add index idx_%d(a)", tc.Cnt.Load())
			query2 = fmt.Sprintf("alter table mysql.pause_user_ddl_t1 add column c_%d int", tc.Cnt.Load())
		}
		tc.Cnt.Add(1)
		asyncExecDDL(query1)
		asyncExecDDL(query2)

		checkDDLJobState(s)
	}

	tc.OnBootstrapAfterExported = func(s sessiontypes.Session) {
		checkDDLJobState(s)
	}
	session.TestHook = tc

	mock := true
	session.WithMockUpgrade = &mock
	seV := session.CreateSessionAndSetID(t, store)
	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	err = m.FinishBootstrap(session.CurrentBootstrapVersion - 1)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	revertVersionAndVariables(t, seV, int(session.CurrentBootstrapVersion-1))
	session.UnsetStoreBootstrapped(store.UUID())
	ver, err := session.GetBootstrapVersion(seV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion-1, ver)
	dom.Close()
	startUpgrade(store)
	domLatestV, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domLatestV.Close()

	seLatestV := session.CreateSessionAndSetID(t, store)
	ver, err = session.GetBootstrapVersion(seLatestV)
	require.NoError(t, err)
	require.Equal(t, session.CurrentBootstrapVersion+1, ver)

	wg.Wait()
	finishUpgrade(store)

	tk := testkit.NewTestKit(t, store)
	var rows []chunk.Row

	// Make sure all DDLs are done.
	for i := 0; i < 50; i++ {
		sql = "select count(1) from mysql.tidb_ddl_job"
		rows, err = execute(context.Background(), tk.Session(), sql)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		if rows[0].GetInt64(0) == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	require.Equal(t, int64(0), rows[0].GetInt64(0))

	// Check user DDLs are handled after system DDLs.
	sql = fmt.Sprintf("select job_meta from mysql.tidb_ddl_history order by job_id desc limit %d", tc.Cnt.Load())
	rows, err = execute(context.Background(), tk.Session(), sql)
	require.NoError(t, err)
	require.Len(t, rows, int(tc.Cnt.Load()))
	type info struct {
		ts  uint64
		sql string
	}
	mysqlOpInfos := make([]info, 0, len(rows))
	testOpInfos := make([]info, 0, len(rows))
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		runJob := model.Job{}
		err := runJob.Decode(jobBinary)
		require.NoError(t, err)
		if strings.EqualFold(runJob.SchemaName, "mysql") {
			mysqlOpInfos = append(mysqlOpInfos, info{runJob.BinlogInfo.FinishedTS, runJob.Query})
		} else {
			testOpInfos = append(testOpInfos, info{runJob.BinlogInfo.FinishedTS, runJob.Query})
		}
	}
	for _, mysqlInfo := range mysqlOpInfos {
		for _, testInfo := range testOpInfos {
			cmt := fmt.Sprintf("test sql:%s, ts:%v, mysql sql:%s, ts:%v",
				testInfo.sql, testInfo.ts, mysqlInfo.sql, mysqlInfo.ts)
			require.Greater(t, testInfo.ts, mysqlInfo.ts, cmt)
		}
	}
	// Check system schema in mock bootstrap.
	tk.MustQuery("show create table mysql.mock_sys_t").Check(testkit.Rows(
		"mock_sys_t CREATE TABLE `mock_sys_t` (\n" +
			"  `c1` int(11) DEFAULT NULL,\n" +
			"  `c2` int(11) NOT NULL,\n" +
			"  `c11` char(10) DEFAULT NULL,\n" +
			"  `c4` bigint(20) DEFAULT NULL,\n" +
			"  `mayNullCol` bigint(20) NOT NULL DEFAULT '1',\n" +
			"  KEY `fk_c1` (`c1`),\n" +
			"  UNIQUE KEY `idx_uc2` (`c2`),\n" +
			"  KEY `idx_c2` (`c2`),\n" +
			"  KEY `idx_v` (`c1`) /*!80000 INVISIBLE */,\n" +
			"  KEY `rename_idx2` (`c1`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show create table mysql.mock_sys_partition").Check(testkit.Rows(
		"mock_sys_partition CREATE TABLE `mock_sys_partition` (\n" +
			"  `c1` int(11) NOT NULL,\n" +
			"  `c2` int(11) DEFAULT NULL,\n" +
			"  `c3` int(11) DEFAULT NULL,\n" +
			"  UNIQUE KEY `c3_index` (`c1`),\n" +
			"  PRIMARY KEY (`c1`) /*T![clustered_index] NONCLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n" +
			"PARTITION BY RANGE (`c1`)\n" +
			"(PARTITION `p0` VALUES LESS THAN (1024),\n" +
			" PARTITION `p1` VALUES LESS THAN (2048),\n" +
			" PARTITION `p2` VALUES LESS THAN (3072),\n" +
			" PARTITION `p3` VALUES LESS THAN (4096),\n" +
			" PARTITION `p4` VALUES LESS THAN (7096))"))
}
