// Copyright 2021 PingCAP, Inc.
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

package planstats_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestPlanStatsLoad(t *testing.T) {
	p := parser.New()
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	ctx := tk.Session().(sessionctx.Context)
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("set @@session.tidb_stats_load_sync_wait = 60000")
	tk.MustExec("create table t(a int, b int, c int, d int, primary key(a), key idx(b))")
	tk.MustExec("insert into t values (1,1,1,1),(2,2,2,2),(3,3,3,3)")
	tk.MustExec("create table pt(a int, b int, c int) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than maxvalue)")
	tk.MustExec("insert into pt values (1,1,1),(2,2,2),(13,13,13),(14,14,14),(25,25,25),(36,36,36)")

	oriLease := dom.StatsHandle().Lease()
	dom.StatsHandle().SetLease(1)
	defer func() {
		dom.StatsHandle().SetLease(oriLease)
	}()
	tk.MustExec("analyze table t")
	tk.MustExec("analyze table pt")

	testCases := []struct {
		sql   string
		skip  bool
		check func(p base.Plan, tableInfo *model.TableInfo)
	}{
		{ // DataSource
			sql: "select * from t where c>1",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				switch pp := p.(type) {
				case *plannercore.PhysicalTableReader:
					stats := pp.StatsInfo().HistColl
					require.Equal(t, 0, countFullStats(stats, tableInfo.Columns[1].ID))
					require.Greater(t, countFullStats(stats, tableInfo.Columns[2].ID), 0)
				default:
					t.Error("unexpected plan:", pp)
				}
			},
		},
		{ // PartitionTable
			sql: "select * from pt where a < 15 and c > 1",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				pua, ok := p.(*plannercore.PhysicalUnionAll)
				require.True(t, ok)
				for _, child := range pua.Children() {
					require.Greater(t, countFullStats(child.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
				}
			},
		},
		{ // Join
			sql: "select * from t t1 inner join t t2 on t1.b=t2.b where t1.d=3",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				pp, ok := p.(base.PhysicalPlan)
				require.True(t, ok)
				require.Greater(t, countFullStats(pp.Children()[0].StatsInfo().HistColl, tableInfo.Columns[3].ID), 0)
				require.Greater(t, countFullStats(pp.Children()[1].StatsInfo().HistColl, tableInfo.Columns[3].ID), 0)
			},
		},
		{ // Apply
			sql: "select * from t t1 where t1.b > (select count(*) from t t2 where t2.c > t1.a and t2.d>1) and t1.c>2",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				pp, ok := p.(*plannercore.PhysicalProjection)
				require.True(t, ok)
				pa, ok := pp.Children()[0].(*plannercore.PhysicalApply)
				require.True(t, ok)
				left := pa.PhysicalHashJoin.Children()[0]
				right := pa.PhysicalHashJoin.Children()[0]
				require.Greater(t, countFullStats(left.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
				require.Greater(t, countFullStats(right.StatsInfo().HistColl, tableInfo.Columns[3].ID), 0)
			},
		},
		{ // > Any
			sql: "select * from t where t.b > any(select d from t where t.c > 2)",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				ph, ok := p.(*plannercore.PhysicalHashJoin)
				require.True(t, ok)
				ptr, ok := ph.Children()[0].(*plannercore.PhysicalTableReader)
				require.True(t, ok)
				require.Greater(t, countFullStats(ptr.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
			},
		},
		{ // in
			sql: "select * from t where t.b in (select d from t where t.c > 2)",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				ph, ok := p.(*plannercore.PhysicalHashJoin)
				require.True(t, ok)
				ptr, ok := ph.Children()[1].(*plannercore.PhysicalTableReader)
				require.True(t, ok)
				require.Greater(t, countFullStats(ptr.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
			},
		},
		{ // not in
			sql: "select * from t where t.b not in (select d from t where t.c > 2)",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				ph, ok := p.(*plannercore.PhysicalHashJoin)
				require.True(t, ok)
				ptr, ok := ph.Children()[1].(*plannercore.PhysicalTableReader)
				require.True(t, ok)
				require.Greater(t, countFullStats(ptr.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
			},
		},
		{ // exists
			sql: "select * from t t1 where exists (select * from t t2 where t1.b > t2.d and t2.c>1)",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				ph, ok := p.(*plannercore.PhysicalHashJoin)
				require.True(t, ok)
				ptr, ok := ph.Children()[1].(*plannercore.PhysicalTableReader)
				require.True(t, ok)
				require.Greater(t, countFullStats(ptr.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
			},
		},
		{ // not exists
			sql: "select * from t t1 where not exists (select * from t t2 where t1.b > t2.d and t2.c>1)",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				ph, ok := p.(*plannercore.PhysicalHashJoin)
				require.True(t, ok)
				ptr, ok := ph.Children()[1].(*plannercore.PhysicalTableReader)
				require.True(t, ok)
				require.Greater(t, countFullStats(ptr.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
			},
		},
		{ // CTE
			sql: "with cte(x, y) as (select d + 1, b from t where c > 1) select * from cte where x < 3",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				ps, ok := p.(*plannercore.PhysicalProjection)
				require.True(t, ok)
				pc, ok := ps.Children()[0].(*plannercore.PhysicalTableReader)
				require.True(t, ok)
				pp, ok := pc.GetTablePlan().(*plannercore.PhysicalSelection)
				require.True(t, ok)
				reader, ok := pp.Children()[0].(*plannercore.PhysicalTableScan)
				require.True(t, ok)
				require.Greater(t, countFullStats(reader.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
			},
		},
		{ // recursive CTE
			sql: "with recursive cte(x, y) as (select a, b from t where c > 1 union select x + 1, y from cte where x < 5) select * from cte",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				pc, ok := p.(*plannercore.PhysicalCTE)
				require.True(t, ok)
				pp, ok := pc.SeedPlan.(*plannercore.PhysicalProjection)
				require.True(t, ok)
				reader, ok := pp.Children()[0].(*plannercore.PhysicalTableReader)
				require.True(t, ok)
				require.Greater(t, countFullStats(reader.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
			},
		},
		{ // check idx(b)
			sql: "select * from t USE INDEX(idx) where b >= 10",
			check: func(p base.Plan, tableInfo *model.TableInfo) {
				pr, ok := p.(*plannercore.PhysicalIndexLookUpReader)
				require.True(t, ok)
				pis, ok := pr.IndexPlans[0].(*plannercore.PhysicalIndexScan)
				require.True(t, ok)
				require.True(t, pis.StatsInfo().HistColl.Indices[1].IsEssentialStatsLoaded())
			},
		},
	}
	for _, testCase := range testCases {
		if testCase.skip {
			continue
		}
		is := dom.InfoSchema()
		dom.StatsHandle().Clear() // clear statsCache
		require.NoError(t, dom.StatsHandle().Update(is))
		stmt, err := p.ParseOneStmt(testCase.sql, "", "")
		require.NoError(t, err)
		err = executor.ResetContextOfStmt(ctx, stmt)
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		require.NoError(t, err)
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		tableInfo := tbl.Meta()
		testCase.check(p, tableInfo)
	}
}

func countFullStats(stats *statistics.HistColl, colID int64) int {
	for _, col := range stats.Columns {
		if col.Info.ID == colID {
			return col.Histogram.Len() + col.TopN.Num()
		}
	}
	return -1
}

func TestPlanStatsLoadTimeout(t *testing.T) {
	p := parser.New()
	originConfig := config.GetGlobalConfig()
	newConfig := config.NewConfig()
	newConfig.Performance.StatsLoadConcurrency = -1 // no worker to consume channel
	newConfig.Performance.StatsLoadQueueSize = 1
	config.StoreGlobalConfig(newConfig)
	defer config.StoreGlobalConfig(originConfig)
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	originalVal1 := tk.MustQuery("select @@tidb_stats_load_pseudo_timeout").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_stats_load_pseudo_timeout = %v", originalVal1))
	}()

	ctx := tk.Session().(sessionctx.Context)
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	// since queue full, make sync-wait return as timeout as soon as possible
	tk.MustExec("set @@session.tidb_stats_load_sync_wait = 1")
	tk.MustExec("create table t(a int, b int, c int, primary key(a))")
	tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

	oriLease := dom.StatsHandle().Lease()
	dom.StatsHandle().SetLease(1)
	defer func() {
		dom.StatsHandle().SetLease(oriLease)
	}()
	tk.MustExec("analyze table t")
	is := dom.InfoSchema()
	require.NoError(t, dom.StatsHandle().Update(is))
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	neededColumn := model.StatsLoadItem{TableItemID: model.TableItemID{TableID: tableInfo.ID, ID: tableInfo.Columns[0].ID, IsIndex: false}, FullLoad: true}
	resultCh := make(chan stmtctx.StatsLoadResult, 1)
	timeout := time.Duration(1<<63 - 1)
	task := &types.NeededItemTask{
		Item:      neededColumn,
		ResultCh:  resultCh,
		ToTimeout: time.Now().Local().Add(timeout),
	}
	dom.StatsHandle().AppendNeededItem(task, timeout) // make channel queue full
	sql := "select /*+ MAX_EXECUTION_TIME(1000) */ * from t where c>1"
	stmt, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	tk.MustExec("set global tidb_stats_load_pseudo_timeout=false")
	_, _, err = planner.Optimize(context.TODO(), ctx, stmt, is)
	require.Error(t, err) // fail sql for timeout when pseudo=false

	tk.MustExec("set global tidb_stats_load_pseudo_timeout=true")
	require.NoError(t, failpoint.Enable("github.com/pingcap/executor/assertSyncStatsFailed", `return(true)`))
	tk.MustExec(sql) // not fail sql for timeout when pseudo=true
	failpoint.Disable("github.com/pingcap/executor/assertSyncStatsFailed")

	// Test Issue #50872.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/assertSyncWaitFailed", `return(true)`))
	tk.MustExec(sql)
	failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/assertSyncWaitFailed")

	plan, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
	require.NoError(t, err) // not fail sql for timeout when pseudo=true
	switch pp := plan.(type) {
	case *plannercore.PhysicalTableReader:
		stats := pp.StatsInfo().HistColl
		require.Equal(t, 0, countFullStats(stats, tableInfo.Columns[0].ID))
		require.Equal(t, 0, countFullStats(stats, tableInfo.Columns[2].ID)) // pseudo stats
	default:
		t.Error("unexpected plan:", pp)
	}
}

func TestPlanStatsStatusRecord(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=0`) // affect this ut
	tk.MustExec(`create table t (b int,key b(b))`)
	tk.MustExec("insert into t (b) values (1)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select * from t where b >= 1")
	require.Equal(t, tk.Session().GetSessionVars().StmtCtx.RecordedStatsLoadStatusCnt(), 0)
	// drop stats in order to change status
	domain.GetDomain(tk.Session()).StatsHandle().SetStatsCacheCapacity(1)
	tk.MustQuery("select * from t where b >= 1")
	for _, usedStatsForTbl := range tk.Session().GetSessionVars().StmtCtx.GetUsedStatsInfo(false).Values() {
		if usedStatsForTbl == nil {
			continue
		}
		for _, status := range usedStatsForTbl.IndexStatsLoadStatus {
			require.Equal(t, status, "allEvicted")
		}
		for _, status := range usedStatsForTbl.ColumnStatsLoadStatus {
			require.Equal(t, status, "allEvicted")
		}
	}
}

func TestCollectDependingVirtualCols(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c json," +
		"index ic_char((cast(c->'$' as char(32) array)))," +
		"index ic_unsigned((cast(c->'$.unsigned' as unsigned array)))," +
		"index ic_signed((cast(c->'$.signed' as unsigned array)))" +
		")")
	tk.MustExec("create table t1(a int, b int, c int," +
		"vab int as (a + b) virtual," +
		"vc int as (c - 5) virtual," +
		"vvc int as (b - vc) virtual," +
		"vvabvvc int as (vab * vvc) virtual," +
		"index ib((b + 1))," +
		"index icvab((c + vab))," +
		"index ivvcvab((vvc / vab))" +
		")")

	is := dom.InfoSchema()
	tableNames := []string{"t", "t1"}
	tblName2TblID := make(map[string]int64)
	tblID2Tbl := make(map[int64]table.Table)
	for _, tblName := range tableNames {
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr(tblName))
		require.NoError(t, err)
		tblName2TblID[tblName] = tbl.Meta().ID
		tblID2Tbl[tbl.Meta().ID] = tbl
	}

	var input []struct {
		TableName     string
		InputColNames []string
	}
	var output []struct {
		TableName      string
		InputColNames  []string
		OutputColNames []string
	}
	testData := GetPlanStatsData()
	testData.LoadTestCases(t, &input, &output)

	for i, testCase := range input {
		// prepare the input
		tbl := tblID2Tbl[tblName2TblID[testCase.TableName]]
		require.NotNil(t, tbl)
		neededItems := make([]model.StatsLoadItem, 0, len(testCase.InputColNames))
		for _, colName := range testCase.InputColNames {
			col := tbl.Meta().FindPublicColumnByName(colName)
			require.NotNil(t, col)
			neededItems = append(neededItems, model.StatsLoadItem{TableItemID: model.TableItemID{TableID: tbl.Meta().ID, ID: col.ID}, FullLoad: true})
		}

		// call the function
		res := plannercore.CollectDependingVirtualCols(tblID2Tbl, neededItems)

		// record and check the output
		cols := make([]string, 0, len(res))
		for _, tblColID := range res {
			colName := tbl.Meta().FindColumnNameByID(tblColID.ID)
			require.NotEmpty(t, colName)
			cols = append(cols, colName)
		}
		slices.Sort(cols)
		testdata.OnRecord(func() {
			output[i].TableName = testCase.TableName
			output[i].InputColNames = testCase.InputColNames
			output[i].OutputColNames = cols
		})
		require.Equal(t, output[i].OutputColNames, cols)
	}
}

func TestPartialStatsInExplain(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")
	tk.MustExec("create table t2(a int, primary key(a))")
	tk.MustExec("insert into t2 values (1),(2),(3)")
	tk.MustExec(
		"create table tp(a int, b int, c int, index ic(c)) partition by range(a)" +
			"(partition p0 values less than (10)," +
			"partition p1 values less than (20)," +
			"partition p2 values less than maxvalue)",
	)
	tk.MustExec("insert into tp values (1,1,1),(2,2,2),(13,13,13),(14,14,14),(25,25,25),(36,36,36)")

	oriLease := dom.StatsHandle().Lease()
	dom.StatsHandle().SetLease(1)
	defer func() {
		dom.StatsHandle().SetLease(oriLease)
	}()
	tk.MustExec("analyze table t")
	tk.MustExec("analyze table t2")
	tk.MustExec("analyze table tp")
	tk.RequireNoError(dom.StatsHandle().Update(dom.InfoSchema()))
	tk.MustQuery("explain select * from tp where a = 1")
	tk.MustExec("set @@tidb_stats_load_sync_wait = 0")
	var (
		input  []string
		output []struct {
			Query  string
			Result []string
		}
	)
	testData := GetPlanStatsData()
	testData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		testdata.OnRecord(func() {
			output[i].Query = input[i]
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		})
		tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
	}
}
