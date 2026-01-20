// Copyright 2025 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

func TestRefreshTableStats(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustExec("create table t2 (a int, b int, index idx(a))")
	tk.MustExec("insert into t2 values (1,1), (2,2), (3,3)")
	tk.MustExec("analyze table t1, t2 all columns with 1 topn, 2 buckets")

	is := dom.InfoSchema()
	handle := dom.StatsHandle()
	ctx := context.Background()
	tbl1, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl2, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tbl1Meta := tbl1.Meta()
	tbl1Stats := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	tbl2Meta := tbl2.Meta()
	tbl2Stats := handle.GetPhysicalTableStats(tbl2Meta.ID, tbl2Meta)
	tk.MustExec("refresh stats t1, test.t1")
	tbl1StatsUpdated := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	tbl2StatsUpdated := handle.GetPhysicalTableStats(tbl2Meta.ID, tbl2Meta)
	require.NotSame(t, tbl1Stats, tbl1StatsUpdated)
	require.Nil(t, tbl1StatsUpdated.GetIdx(1), "index stats shouldn't be loaded in lite mode")
	require.Same(t, tbl2Stats, tbl2StatsUpdated)
	tk.MustExec("REFRESH STATS *.* FULL")
	tbl1StatsUpdated = handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.NotNil(t, tbl1StatsUpdated.GetIdx(1), "index stats should be loaded in full mode")
	tbl2StatsUpdated = handle.GetPhysicalTableStats(tbl2Meta.ID, tbl2Meta)
	require.NotSame(t, tbl2Stats, tbl2StatsUpdated)
	require.NotNil(t, tbl2StatsUpdated.GetIdx(1), "index stats should be loaded in full mode")
}

func TestRefreshStatsWarningsForMissingObjects(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("analyze table t all columns")

	vars := tk.Session().GetSessionVars()

	vars.StmtCtx.SetWarnings(nil)
	tk.MustExec("refresh stats missing_db.*")
	warnings := vars.StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.Equal(t, infoschema.ErrDatabaseNotExists.FastGenByArgs("missing_db").Error(), warnings[0].Err.Error())

	vars.StmtCtx.SetWarnings(nil)
	tk.MustExec("refresh stats test.t_missing, test.t")
	warnings = vars.StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.Equal(t, infoschema.ErrTableNotExists.FastGenByArgs("test", "t_missing").Error(), warnings[0].Err.Error())

	vars.StmtCtx.SetWarnings(nil)
	tk.MustExec("refresh stats t, t1")
	warnings = vars.StmtCtx.GetWarnings()
	require.Len(t, warnings, 1)
	require.Equal(t, infoschema.ErrTableNotExists.FastGenByArgs("test", "t1").Error(), warnings[0].Err.Error())

	vars.StmtCtx.SetWarnings(nil)
	tk.MustExec("refresh stats t")
	require.Len(t, vars.StmtCtx.GetWarnings(), 0)
}

func TestRefreshAllNonExistentTables(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustExec("analyze table t1 all columns with 1 topn, 2 buckets")

	is := dom.InfoSchema()
	handle := dom.StatsHandle()
	ctx := context.Background()
	tbl1, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl1Meta := tbl1.Meta()
	tbl1Stats := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	tk.MustExec("refresh stats missing_db.*, t2")
	tbl1StatsUpdated := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.Same(t, tbl1Stats, tbl1StatsUpdated)
}

func TestRefreshStatsNoTables(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("refresh stats *.*")
}

func TestRefreshStatsRequiresDefaultDB(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustGetDBError("refresh stats t1", plannererrors.ErrNoDB)
}

func TestRefreshStatsWhenDatabaseIsEmpty(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	vars := tk.Session().GetSessionVars()
	vars.StmtCtx.SetWarnings(nil)
	tk.MustExec("refresh stats test.*")
	require.Len(t, vars.StmtCtx.GetWarnings(), 0)
}

func TestRefreshStatsPrivilegeChecks(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_refresh_priv")
	tk.MustExec("create table t_refresh_priv (a int)")

	t.Run("table scope requires select", func(t *testing.T) {
		tk.MustExec("drop user if exists 'refresh_reader'@'%'")
		tk.MustExec("create user 'refresh_reader'@'%'")

		tkUser := testkit.NewTestKit(t, store)
		require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "refresh_reader", Hostname: "%"}, nil, nil, nil))
		tkUser.MustGetErrCode("refresh stats test.t_refresh_priv", errno.ErrTableaccessDenied)

		tk.MustExec("grant select on test.t_refresh_priv to 'refresh_reader'@'%'")
		tkUser.MustExec("refresh stats test.t_refresh_priv")
	})

	t.Run("database scope requires select", func(t *testing.T) {
		tk.MustExec("drop user if exists 'refresh_db_reader'@'%'")
		tk.MustExec("create user 'refresh_db_reader'@'%'")

		tkUser := testkit.NewTestKit(t, store)
		require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "refresh_db_reader", Hostname: "%"}, nil, nil, nil))
		tkUser.MustGetErrCode("refresh stats test.*", errno.ErrDBaccessDenied)

		tk.MustExec("grant select on test.* to 'refresh_db_reader'@'%'")
		tkUser.MustExec("refresh stats test.*")
	})

	t.Run("global scope requires global select", func(t *testing.T) {
		tk.MustExec("drop user if exists 'refresh_global_reader'@'%'")
		tk.MustExec("create user 'refresh_global_reader'@'%'")

		tkUser := testkit.NewTestKit(t, store)
		require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "refresh_global_reader", Hostname: "%"}, nil, nil, nil))
		tkUser.MustGetErrCode("refresh stats *.*", errno.ErrPrivilegeCheckFail)

		tk.MustExec("grant select on *.* to 'refresh_global_reader'@'%'")
		tkUser.MustExec("refresh stats *.*")
	})
}

func TestRefreshStatsWithRestoreAdmin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	const user = "restore_admin_tester"
	defer tk.MustExec("drop user if exists '" + user + "'@'%'")

	tk.MustExec("drop user if exists '" + user + "'@'%'")
	tk.MustExec("create user '" + user + "'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: user, Hostname: "%"}, nil, nil, nil))
	tkUser.MustGetErrCode("refresh stats *.*", errno.ErrPrivilegeCheckFail)

	tk.MustExec("grant restore_admin on *.* to '" + user + "'@'%'")
	tkUser.MustExec("refresh stats *.*")
}

// TestRefreshStatsWithFullMode verifies that running "refresh stats ... full" loads and updates
// index statistics even when lite-init-stats is enabled, ensuring a full refresh keeps index
// statistics resident in memory as users expect after explicitly requesting full mode.
func TestRefreshStatsWithFullMode(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustExec("analyze table t1 all columns with 1 topn, 2 buckets")

	is := dom.InfoSchema()
	handle := dom.StatsHandle()
	ctx := context.Background()
	tbl1, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl1Meta := tbl1.Meta()
	statsBeforeRefresh := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	tk.MustExec("refresh stats t1")
	statsAfterDefaultRefresh := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.NotSame(t, statsBeforeRefresh, statsAfterDefaultRefresh)
	require.Nil(t, statsAfterDefaultRefresh.GetIdx(1), "index stats should not be loaded in lite mode")

	tk.MustExec("select * from t1 where a = 1")
	statsAfterSelect := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.NotSame(t, statsBeforeRefresh, statsAfterSelect, "stats versuon should not be changed after select")
	require.NotNil(t, statsAfterSelect.GetIdx(1), "index stats will be loaded after select")

	tk.MustExec("refresh stats t1")
	statsAfterDefaultRefresh = handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.NotSame(t, statsBeforeRefresh, statsAfterDefaultRefresh)
	require.Nil(t, statsAfterDefaultRefresh.GetIdx(1), "index stats should be removed in lite mode")

	// Issue a full refresh to ensure the index stats are loaded.
	tk.MustExec("refresh stats t1 full")
	statsAfterFullRefresh := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.NotSame(t, statsAfterDefaultRefresh, statsAfterFullRefresh)
	require.NotNil(t, statsAfterFullRefresh.GetIdx(1), "index stats should be loaded in full mode")
	require.Len(t, statsAfterFullRefresh.GetIdx(1).Buckets, 1, "buckets should be loaded in full mode")
	indexVersionAfterFullRefresh := statsAfterFullRefresh.GetIdx(1).LastUpdateVersion

	// Insert additional data and run ANALYZE again.
	tk.MustExec("insert into t1 values (4,4), (5,5)")
	// Analyze loads statistics based on the current state of the in-memory stats (all statistics have been loaded) while running in lite mode.
	tk.MustExec("analyze table t1 all columns with 1 topn, 2 buckets")
	statsAfterAnalyze := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.NoError(t, err)
	require.NotNil(t, statsAfterAnalyze, "analyze loads statistics from the current in-memory data when running in lite mode")
	require.NotSame(t, statsAfterFullRefresh, statsAfterAnalyze)
	indexVersionAfterAnalyze := statsAfterAnalyze.GetIdx(1).LastUpdateVersion
	require.Len(t, statsAfterAnalyze.GetIdx(1).Buckets, 2, "buckets should be loaded in full mode")
	require.Greater(t, indexVersionAfterAnalyze, indexVersionAfterFullRefresh, "index stats should be updated")

	// Manually load it again to check it works well.
	statsAfterLoad, err := handle.TableStatsFromStorage(tbl1Meta, tbl1Meta.ID, false, 0)
	require.NoError(t, err)
	require.NotNil(t, statsAfterLoad)
	require.NotSame(t, statsAfterAnalyze, statsAfterLoad)
	indexVersionAfterLoad := statsAfterLoad.GetIdx(1).LastUpdateVersion
	require.Len(t, statsAfterLoad.GetIdx(1).Buckets, 2, "nothing should be changed")
	require.Equal(t, indexVersionAfterLoad, indexVersionAfterAnalyze, "nothing should be changed")
}

// TestRefreshStatsWithLiteMode verifies that running "refresh stats ...  lite" omits index stats,
// while a subsequent loading operation repopulates them. Typically, users wouldn’t expect to run a lite refresh
// with lite-init-stats=false, so we shouldn’t persist this behavior after the lite refresh stats.
func TestRefreshStatsWithLiteMode(t *testing.T) {
	oriVal := config.GetGlobalConfig().Performance.LiteInitStats
	config.GetGlobalConfig().Performance.LiteInitStats = false
	defer func() {
		config.GetGlobalConfig().Performance.LiteInitStats = oriVal
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustExec("analyze table t1 all columns with 1 topn, 2 buckets")

	is := dom.InfoSchema()
	handle := dom.StatsHandle()
	ctx := context.Background()
	tbl1, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tbl1Meta := tbl1.Meta()
	statsBeforeRefresh := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	tk.MustExec("refresh stats t1")
	statsAfterFullRefresh := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.NotSame(t, statsBeforeRefresh, statsAfterFullRefresh)
	require.NotNil(t, statsAfterFullRefresh.GetIdx(1), "index stats should be loaded in full mode")

	// Run a lite refresh and verify the index stats remain unloaded.
	tk.MustExec("refresh stats t1 lite")
	statsAfterLiteRefresh := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.NotSame(t, statsAfterFullRefresh, statsAfterLiteRefresh)
	require.Nil(t, statsAfterLiteRefresh.GetIdx(1), "index stats should not be loaded in lite mode")

	// Insert additional data and run ANALYZE again.
	tk.MustExec("insert into t1 values (4,4), (5,5)")
	// Analyze loads all statistics when running in full mode.
	tk.MustExec("analyze table t1 all columns with 1 topn, 2 buckets")
	statsAfterAnalyze := handle.GetPhysicalTableStats(tbl1Meta.ID, tbl1Meta)
	require.NoError(t, err)
	require.NotNil(t, statsAfterAnalyze, "analyze loads all statistics when running in full mode")
	require.NotSame(t, statsAfterFullRefresh, statsAfterAnalyze)
	indexVersionAfterAnalyze := statsAfterAnalyze.GetIdx(1).LastUpdateVersion
	require.Len(t, statsAfterAnalyze.GetIdx(1).Buckets, 2, "buckets should be loaded in full mode")
	require.Greater(t, indexVersionAfterAnalyze, uint64(0), "index stats should be updated")

	// Manually load it again to check it works well.
	statsAfterLoad, err := handle.TableStatsFromStorage(tbl1Meta, tbl1Meta.ID, false, 0)
	require.NoError(t, err)
	require.NotNil(t, statsAfterLoad)
	require.NotSame(t, statsAfterAnalyze, statsAfterLoad)
	indexVersionAfterLoad := statsAfterLoad.GetIdx(1).LastUpdateVersion
	require.Len(t, statsAfterLoad.GetIdx(1).Buckets, 2, "nothing should be changed")
	require.Equal(t, indexVersionAfterLoad, indexVersionAfterAnalyze, "nothing should be changed")
}

func TestRefreshStatsConcurrently(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t_partition")
	tk.MustExec("create table t1 (a int, b int, index idx_a(a))")
	tk.MustExec("create table t2 (a int, b int, index idx_a(a))")
	tk.MustExec(`create table t_partition (
		id int,
		a int,
		b int,
		index idx_a(a)
	) partition by hash(id) partitions 4`)
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3),(4,4)")
	tk.MustExec("insert into t2 values (5,5),(6,6),(7,7),(8,8)")
	tk.MustExec("insert into t_partition values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6)")
	tk.MustExec("analyze table t1, t2, t_partition all columns with 1 topn, 2 buckets")

	handle := dom.StatsHandle()

	sqls := []string{
		"REFRESH STATS test.t1",
		"REFRESH STATS test.t2 FULL",
		"REFRESH STATS test.t_partition",
		"REFRESH STATS test.*",
		"REFRESH STATS test.t1 FULL",
		"REFRESH STATS test.t_partition FULL",
		"REFRESH STATS test.t2",
		"REFRESH STATS *.* FULL",
	}

	const rounds = 2
	const workerCount = 4

	workers := make([]*testkit.TestKit, workerCount)
	for i := range workers {
		worker := testkit.NewTestKit(t, store)
		worker.MustExec("use test")
		workers[i] = worker
	}

	var wg sync.WaitGroup
	for _, tkWorker := range workers {
		wg.Add(1)
		go func(tkWorker *testkit.TestKit) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				for _, sql := range sqls {
					tkWorker.MustExec(sql)
				}
			}
		}(tkWorker)
	}
	wg.Wait()

	ctx := context.Background()
	is := dom.InfoSchema()
	checkFullIndex := func(tblName string) {
		tbl, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr(tblName))
		require.NoError(t, err)
		stats := handle.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
		require.NotNil(t, stats)
		idx := stats.GetIdx(1)
		require.NotNilf(t, idx, "index stats for %s should be present", tblName)
		require.Truef(t, idx.IsFullLoad(), "index stats for %s should be fully loaded", tblName)
	}
	checkFullIndex("t1")
	checkFullIndex("t2")
	checkFullIndex("t_partition")
}

func TestFlushStatsDelta(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")

	// Get table ID for later query
	ctx := context.Background()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID

	// Insert some rows to generate stats delta
	tk.MustExec("insert into t values (1,1), (2,2), (3,3), (4,4), (5,5)")
	tk.MustExec("flush stats_delta")
	rows := tk.MustQuery("select modify_count from mysql.stats_meta where table_id = ?", tableID).Rows()
	require.Len(t, rows, 1, "stats_meta should have entry for the table")
	modifyCnt, err := strconv.ParseInt(rows[0][0].(string), 10, 64)
	require.NoError(t, err)
	require.Equal(t, modifyCnt, int64(5), "modify_count should increase by 5 after inserting 5 rows and flushing")

	tk.MustExec("insert into t values (1,1), (2,2)")
	tk.MustExec("flush stats_delta")
	rows = tk.MustQuery("select modify_count from mysql.stats_meta where table_id = ?", tableID).Rows()
	require.Len(t, rows, 1, "stats_meta should have entry for the table")
	modifyCnt, err = strconv.ParseInt(rows[0][0].(string), 10, 64)
	require.NoError(t, err)
	require.Equal(t, modifyCnt, int64(7), "modify_count should increase by 5 after inserting 5 rows and flushing")
}
