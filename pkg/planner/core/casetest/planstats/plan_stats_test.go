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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/core/rule"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/stretchr/testify/require"
)

func TestPlanStatsLoad(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		p := parser.New()
		testKit.MustExec("use test")
		ctx := testKit.Session().(sessionctx.Context)
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("set @@session.tidb_analyze_version=2")
		testKit.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
		testKit.MustExec("set @@session.tidb_stats_load_sync_wait = 60000")
		testKit.MustExec("set tidb_opt_projection_push_down = 0")
		testKit.MustExec("create table t(a int, b int, c int, d int, primary key(a), key idx(b))")
		testKit.MustExec("insert into t values (1,1,1,1),(2,2,2,2),(3,3,3,3)")
		testKit.MustExec("create table pt(a int, b int, c int) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than maxvalue)")
		testKit.MustExec("insert into pt values (1,1,1),(2,2,2),(13,13,13),(14,14,14),(25,25,25),(36,36,36)")

		oriLease := dom.StatsHandle().Lease()
		dom.StatsHandle().SetLease(1)
		defer func() {
			dom.StatsHandle().SetLease(oriLease)
		}()
		testKit.MustExec("analyze table t all columns")
		testKit.MustExec("analyze table pt all columns")

		testCases := []struct {
			sql   string
			skip  bool
			check func(p base.Plan, tableInfo *model.TableInfo)
		}{
			{ // DataSource
				sql: "select * from t where c>1",
				check: func(p base.Plan, tableInfo *model.TableInfo) {
					switch pp := p.(type) {
					case *physicalop.PhysicalTableReader:
						stats := pp.StatsInfo().HistColl
						require.Equal(t, -1, countFullStats(stats, tableInfo.Columns[1].ID))
						require.Greater(t, countFullStats(stats, tableInfo.Columns[2].ID), 0)
					default:
						t.Error("unexpected plan:", pp)
					}
				},
			},
			{ // PartitionTable
				sql: "select * from pt where a < 15 and c > 1",
				check: func(p base.Plan, tableInfo *model.TableInfo) {
					pua, ok := p.(*physicalop.PhysicalUnionAll)
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
					pp, ok := p.(*physicalop.PhysicalProjection)
					require.True(t, ok)
					pa, ok := pp.Children()[0].(*physicalop.PhysicalApply)
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
					ph, ok := p.(*physicalop.PhysicalHashJoin)
					require.True(t, ok)
					ptr, ok := ph.Children()[0].(*physicalop.PhysicalTableReader)
					require.True(t, ok)
					require.Greater(t, countFullStats(ptr.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
				},
			},
			{ // in
				sql: "select * from t where t.b in (select d from t where t.c > 2)",
				check: func(p base.Plan, tableInfo *model.TableInfo) {
					ph, ok := p.(*physicalop.PhysicalHashJoin)
					require.True(t, ok)
					ptr, ok := ph.Children()[1].(*physicalop.PhysicalTableReader)
					require.True(t, ok)
					require.Greater(t, countFullStats(ptr.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
				},
			},
			{ // not in
				sql: "select * from t where t.b not in (select d from t where t.c > 2)",
				check: func(p base.Plan, tableInfo *model.TableInfo) {
					ph, ok := p.(*physicalop.PhysicalHashJoin)
					require.True(t, ok)
					ptr, ok := ph.Children()[1].(*physicalop.PhysicalTableReader)
					require.True(t, ok)
					require.Greater(t, countFullStats(ptr.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
				},
			},
			{ // exists
				sql: "select * from t t1 where exists (select * from t t2 where t1.b > t2.d and t2.c>1)",
				check: func(p base.Plan, tableInfo *model.TableInfo) {
					ph, ok := p.(*physicalop.PhysicalHashJoin)
					require.True(t, ok)
					ptr, ok := ph.Children()[1].(*physicalop.PhysicalTableReader)
					require.True(t, ok)
					require.Greater(t, countFullStats(ptr.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
				},
			},
			{ // not exists
				sql: "select * from t t1 where not exists (select * from t t2 where t1.b > t2.d and t2.c>1)",
				check: func(p base.Plan, tableInfo *model.TableInfo) {
					ph, ok := p.(*physicalop.PhysicalHashJoin)
					require.True(t, ok)
					ptr, ok := ph.Children()[1].(*physicalop.PhysicalTableReader)
					require.True(t, ok)
					require.Greater(t, countFullStats(ptr.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
				},
			},
			{ // recursive CTE
				sql: "with recursive cte(x, y) as (select a, b from t where c > 1 union select x + 1, y from cte where x < 5) select * from cte",
				check: func(p base.Plan, tableInfo *model.TableInfo) {
					pc, ok := p.(*physicalop.PhysicalCTE)
					require.True(t, ok)
					pp, ok := pc.SeedPlan.(*physicalop.PhysicalProjection)
					require.True(t, ok)
					reader, ok := pp.Children()[0].(*physicalop.PhysicalTableReader)
					require.True(t, ok)
					require.Greater(t, countFullStats(reader.StatsInfo().HistColl, tableInfo.Columns[2].ID), 0)
				},
			},
			{ // check idx(b)
				sql: "select * from t USE INDEX(idx) where b >= 10",
				check: func(p base.Plan, tableInfo *model.TableInfo) {
					pr, ok := p.(*physicalop.PhysicalIndexLookUpReader)
					require.True(t, ok)
					pis, ok := pr.IndexPlans[0].(*physicalop.PhysicalIndexScan)
					require.True(t, ok)
					require.True(t, pis.StatsInfo().HistColl.GetIdx(1).IsEssentialStatsLoaded())
				},
			},
		}
		for _, testCase := range testCases {
			if testCase.skip {
				continue
			}
			is := dom.InfoSchema()
			dom.StatsHandle().Clear() // clear statsCache
			require.NoError(t, dom.StatsHandle().Update(context.Background(), is))
			stmt, err := p.ParseOneStmt(testCase.sql, "", "")
			require.NoError(t, err)
			err = executor.ResetContextOfStmt(ctx, stmt)
			require.NoError(t, err)
			nodeW := resolve.NewNodeW(stmt)
			p, _, err := planner.Optimize(context.TODO(), ctx, nodeW, is)
			require.NoError(t, err)
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tableInfo := tbl.Meta()
			testCase.check(p, tableInfo)
		}
	})
}

func TestPlanStatsLoadForCTE(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("set @@session.tidb_analyze_version=2")
		testKit.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
		testKit.MustExec("set @@session.tidb_stats_load_sync_wait = 60000")
		testKit.MustExec("set tidb_opt_projection_push_down = 0")
		testKit.MustExec("create table t(a int, b int, c int, d int, primary key(a), key idx(b))")
		testKit.MustExec("insert into t values (1,1,1,1),(2,2,2,2),(3,3,3,3)")
		testKit.MustExec("create table pt(a int, b int, c int) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than maxvalue)")
		testKit.MustExec("insert into pt values (1,1,1),(2,2,2),(13,13,13),(14,14,14),(25,25,25),(36,36,36)")

		oriLease := dom.StatsHandle().Lease()
		dom.StatsHandle().SetLease(1)
		defer func() {
			dom.StatsHandle().SetLease(oriLease)
		}()
		testKit.MustExec("analyze table t all columns")
		testKit.MustExec("analyze table pt all columns")

		var (
			input  []string
			output []struct {
				Query  string
				Result []string
			}
		)
		testData := GetPlanStatsData()
		testData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].Query = input[i]
				output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(sql).Rows())
			})
			testKit.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
	})
}

func countFullStats(stats *statistics.HistColl, colID int64) int {
	cnt := -1
	stats.ForEachColumnImmutable(func(_ int64, col *statistics.Column) bool {
		if col.Info.ID == colID {
			cnt = col.Histogram.Len() + col.TopN.Num()
			return true
		}
		return false
	})
	return cnt
}

func TestPlanStatsLoadTimeout(t *testing.T) {
	p := parser.New()
	originConfig := config.GetGlobalConfig()
	newConfig := config.NewConfig()
	newConfig.Performance.StatsLoadConcurrency = -1 // no worker to consume channel
	newConfig.Performance.StatsLoadQueueSize = 1
	config.StoreGlobalConfig(newConfig)
	defer config.StoreGlobalConfig(originConfig)

	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		originalVal1 := testKit.MustQuery("select @@tidb_stats_load_pseudo_timeout").Rows()[0][0].(string)
		defer func() {
			testKit.MustExec(fmt.Sprintf("set global tidb_stats_load_pseudo_timeout = %v", originalVal1))
		}()

		ctx := testKit.Session().(sessionctx.Context)
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("set @@session.tidb_analyze_version=2")
		// since queue full, make sync-wait return as timeout as soon as possible
		testKit.MustExec("set @@session.tidb_stats_load_sync_wait = 1")
		testKit.MustExec("create table t(a int, b int, c int, primary key(a))")
		testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")

		oriLease := dom.StatsHandle().Lease()
		dom.StatsHandle().SetLease(1)
		defer func() {
			dom.StatsHandle().SetLease(oriLease)
		}()
		testKit.MustExec("analyze table t all columns")
		is := dom.InfoSchema()
		require.NoError(t, dom.StatsHandle().Update(context.Background(), is))
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
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
		testKit.MustExec("set global tidb_stats_load_pseudo_timeout=false")
		nodeW := resolve.NewNodeW(stmt)
		_, _, err = planner.Optimize(context.TODO(), ctx, nodeW, is)
		require.Error(t, err) // fail sql for timeout when pseudo=false

		testKit.MustExec("set global tidb_stats_load_pseudo_timeout=true")
		require.NoError(t, failpoint.Enable("github.com/pingcap/executor/assertSyncStatsFailed", `return(true)`))
		testKit.MustExec(sql) // not fail sql for timeout when pseudo=true
		failpoint.Disable("github.com/pingcap/executor/assertSyncStatsFailed")

		// Test Issue #50872.
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/assertSyncWaitFailed", `return(true)`))
		testKit.MustExec(sql)
		failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/assertSyncWaitFailed")

		plan, _, err := planner.Optimize(context.TODO(), ctx, nodeW, is)
		require.NoError(t, err) // not fail sql for timeout when pseudo=true
		switch pp := plan.(type) {
		case *physicalop.PhysicalTableReader:
			stats := pp.StatsInfo().HistColl
			require.Equal(t, 0, countFullStats(stats, tableInfo.Columns[0].ID))
			require.Equal(t, 0, countFullStats(stats, tableInfo.Columns[2].ID)) // pseudo stats
		default:
			t.Error("unexpected plan:", pp)
		}
	})
}

func TestPlanStatsStatusRecord(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec(`set @@tidb_enable_non_prepared_plan_cache=0`) // affect this ut
		testKit.MustExec(`create table t (b int,key b(b))`)
		testKit.MustExec("insert into t (b) values (1)")
		testKit.MustExec("analyze table t")
		testKit.MustQuery("select * from t where b >= 1")
		require.Equal(t, testKit.Session().GetSessionVars().StmtCtx.RecordedStatsLoadStatusCnt(), 0)
		// drop stats in order to change status
		domain.GetDomain(testKit.Session()).StatsHandle().SetStatsCacheCapacity(1)
		testKit.MustQuery("select * from t where b >= 1")
		for _, usedStatsForTbl := range testKit.Session().GetSessionVars().StmtCtx.GetUsedStatsInfo(false).Values() {
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
	})
}

func TestCollectDependingVirtualCols(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("create table t(a int, b int, c json," +
			"index ic_char((cast(c->'$' as char(32) array)))," +
			"index ic_unsigned((cast(c->'$.unsigned' as unsigned array)))," +
			"index ic_signed((cast(c->'$.signed' as unsigned array)))" +
			")")
		testKit.MustExec("create table t1(a int, b int, c int," +
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
		tblID2Tbl := make(map[int64]*model.TableInfo)
		for _, tblName := range tableNames {
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tblName))
			require.NoError(t, err)
			tblName2TblID[tblName] = tbl.Meta().ID
			tblID2Tbl[tbl.Meta().ID] = tbl.Meta()
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
		testData.LoadTestCases(t, &input, &output, cascades, caller)

		for i, testCase := range input {
			// prepare the input
			tbl := tblID2Tbl[tblName2TblID[testCase.TableName]]
			require.NotNil(t, tbl)
			neededItems := make([]model.StatsLoadItem, 0, len(testCase.InputColNames))
			for _, colName := range testCase.InputColNames {
				col := tbl.FindPublicColumnByName(colName)
				require.NotNil(t, col)
				neededItems = append(neededItems, model.StatsLoadItem{TableItemID: model.TableItemID{TableID: tbl.ID, ID: col.ID}, FullLoad: true})
			}

			// call the function
			res := rule.CollectDependingVirtualCols(tblID2Tbl, neededItems)

			// record and check the output
			cols := make([]string, 0, len(res))
			for _, tblColID := range res {
				colName := tbl.FindColumnNameByID(tblColID.ID)
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
	})
}

func TestStatsAnalyzedInDDL(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("set session tidb_stats_update_during_ddl = 1")
		// test normal table
		testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")

		// insert data
		for i := range 50 {
			testKit.MustExec("insert into t values (?,?,?)", i, i, i)
		}
		var (
			input  []string
			output []struct {
				Query  string
				Result []string
			}
		)
		testData := GetPlanStatsData()
		testData.LoadTestCases(t, &input, &output, cascades, caller)
		var (
			lastIsSelect     bool
			lastStatsVersion string
			curStatsVersion  string
		)
		getHistID := func(name string, isIndex bool) (int64, int64) {
			require.NoError(t, dom.Reload())
			is := dom.InfoSchema()
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tableID := tbl.Meta().ID
			histID := int64(0)
			if isIndex {
				histID = tbl.Meta().FindIndexByName(name).ID
			} else {
				histID = dbutil.FindColumnByName(tbl.Meta().Columns, name).ID
			}
			return tableID, histID
		}
		for i, sql := range input {
			isSelect := strings.HasPrefix(sql, "select")
			testdata.OnRecord(func() {
				output[i].Query = input[i]
				if isSelect {
					// explain the query
					output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery("explain format='brief' " + sql).Rows())
				} else {
					output[i].Result = nil
				}
			})
			if isSelect {
				// explain the query
				testKit.MustQuery("explain format='brief' " + sql).Check(testkit.Rows(output[i].Result...))
				// assert the version
				indexName := ""
				if strings.Contains(sql, "idx_c") {
					indexName = "idx_c"
				} else {
					indexName = "idx_bc"
				}
				tableID, histID := getHistID(indexName, true)
				res := testKit.MustQuery("select version from mysql.stats_histograms where table_id = ? and hist_id = ? and is_index=?;", tableID, histID, true)
				if len(res.Rows()) > 0 {
					curStatsVersion = res.Rows()[0][0].(string)
					// since the index is re-analyzed, so each usage of them use a new version.
					if lastIsSelect {
						require.Equal(t, lastStatsVersion, curStatsVersion)
					} else {
						// last is ddl
						require.NotEqual(t, lastStatsVersion, curStatsVersion)
					}
					lastStatsVersion = curStatsVersion
				}
				lastIsSelect = true
			} else {
				// run the ddl anyway
				testKit.MustExec(sql)
				lastIsSelect = false
			}
		}
	})
}

func TestPartialStatsInExplain(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
		testKit.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3)")
		testKit.MustExec("create table t2(a int, primary key(a))")
		testKit.MustExec("insert into t2 values (1),(2),(3)")
		testKit.MustExec(
			"create table tp(a int, b int, c int, index ic(c)) partition by range(a)" +
				"(partition p0 values less than (10)," +
				"partition p1 values less than (20)," +
				"partition p2 values less than maxvalue)",
		)
		testKit.MustExec("insert into tp values (1,1,1),(2,2,2),(13,13,13),(14,14,14),(25,25,25),(36,36,36)")

		oriLease := dom.StatsHandle().Lease()
		dom.StatsHandle().SetLease(1)
		defer func() {
			dom.StatsHandle().SetLease(oriLease)
		}()
		testKit.MustExec("analyze table t all columns")
		testKit.MustExec("analyze table t2")
		testKit.MustExec("analyze table tp all columns")
		testKit.RequireNoError(dom.StatsHandle().Update(context.Background(), dom.InfoSchema()))
		testKit.MustQuery("explain select * from tp where a = 1")
		testKit.MustExec("set @@tidb_stats_load_sync_wait = 0")
		var (
			input  []string
			output []struct {
				Query  string
				Result []string
			}
		)
		testData := GetPlanStatsData()
		testData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].Query = input[i]
				output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(sql).Rows())
			})
			testKit.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
			require.NoError(t, dom.StatsHandle().LoadNeededHistograms(dom.InfoSchema()))
		}
	})
}
