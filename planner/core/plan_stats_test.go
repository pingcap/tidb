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

package core_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testPlanStatsSuite{})

type testPlanStatsSuite struct {
	*parser.Parser
}

func (s *testPlanStatsSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
}

func (s *testPlanStatsSuite) TearDownSuite(c *C) {
}

func (s *testPlanStatsSuite) TestPlanStatsLoad(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Check(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	ctx := tk.Se.(sessionctx.Context)
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
		check func(p plannercore.Plan, tableInfo *model.TableInfo)
	}{
		{ // DataSource
			sql: "select * from t where c>1",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				switch pp := p.(type) {
				case *plannercore.PhysicalTableReader:
					stats := pp.Stats().HistColl
					c.Assert(countFullStats(stats, tableInfo.Columns[1].ID), Equals, 0)
					c.Assert(countFullStats(stats, tableInfo.Columns[2].ID), Greater, 0)
				default:
					c.Error("unexpected plan:", pp)
				}
			},
		},
		{ // PartitionTable
			sql: "select * from pt where a < 15 and c > 1",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				pua, ok := p.(*plannercore.PhysicalUnionAll)
				c.Check(ok, IsTrue)
				for _, child := range pua.Children() {
					c.Assert(countFullStats(child.Stats().HistColl, tableInfo.Columns[2].ID), Greater, 0)
				}
			},
		},
		{ // Join
			sql: "select * from t t1 inner join t t2 on t1.b=t2.b where t1.d=3",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				pp, ok := p.(plannercore.PhysicalPlan)
				c.Check(ok, IsTrue)
				c.Assert(countFullStats(pp.Children()[0].Stats().HistColl, tableInfo.Columns[3].ID), Greater, 0)
				c.Assert(countFullStats(pp.Children()[1].Stats().HistColl, tableInfo.Columns[3].ID), Greater, 0)
			},
		},
		{ // Apply
			sql: "select * from t t1 where t1.b > (select count(*) from t t2 where t2.c > t1.a and t2.d>1) and t1.c>2",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				pp, ok := p.(*plannercore.PhysicalProjection)
				c.Check(ok, IsTrue)
				pa, ok := pp.Children()[0].(*plannercore.PhysicalApply)
				c.Check(ok, IsTrue)
				left := pa.PhysicalHashJoin.Children()[0]
				right := pa.PhysicalHashJoin.Children()[0]
				c.Assert(countFullStats(left.Stats().HistColl, tableInfo.Columns[2].ID), Greater, 0)
				c.Assert(countFullStats(right.Stats().HistColl, tableInfo.Columns[3].ID), Greater, 0)
			},
		},
		{ // > Any
			sql: "select * from t where t.b > any(select d from t where t.c > 2)",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				ph, ok := p.(*plannercore.PhysicalHashJoin)
				c.Check(ok, IsTrue)
				ptr, ok := ph.Children()[0].(*plannercore.PhysicalTableReader)
				c.Check(ok, IsTrue)
				c.Assert(countFullStats(ptr.Stats().HistColl, tableInfo.Columns[2].ID), Greater, 0)
			},
		},
		{ // in
			sql: "select * from t where t.b in (select d from t where t.c > 2)",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				ph, ok := p.(*plannercore.PhysicalHashJoin)
				c.Check(ok, IsTrue)
				ptr, ok := ph.Children()[1].(*plannercore.PhysicalTableReader)
				c.Check(ok, IsTrue)
				c.Assert(countFullStats(ptr.Stats().HistColl, tableInfo.Columns[2].ID), Greater, 0)
			},
		},
		{ // not in
			sql: "select * from t where t.b not in (select d from t where t.c > 2)",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				ph, ok := p.(*plannercore.PhysicalHashJoin)
				c.Check(ok, IsTrue)
				ptr, ok := ph.Children()[1].(*plannercore.PhysicalTableReader)
				c.Check(ok, IsTrue)
				c.Assert(countFullStats(ptr.Stats().HistColl, tableInfo.Columns[2].ID), Greater, 0)
			},
		},
		{ // exists
			sql: "select * from t t1 where exists (select * from t t2 where t1.b > t2.d and t2.c>1)",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				ph, ok := p.(*plannercore.PhysicalHashJoin)
				c.Check(ok, IsTrue)
				ptr, ok := ph.Children()[1].(*plannercore.PhysicalTableReader)
				c.Check(ok, IsTrue)
				c.Assert(countFullStats(ptr.Stats().HistColl, tableInfo.Columns[2].ID), Greater, 0)
			},
		},
		{ // not exists
			sql: "select * from t t1 where not exists (select * from t t2 where t1.b > t2.d and t2.c>1)",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				ph, ok := p.(*plannercore.PhysicalHashJoin)
				c.Check(ok, IsTrue)
				ptr, ok := ph.Children()[1].(*plannercore.PhysicalTableReader)
				c.Check(ok, IsTrue)
				c.Assert(countFullStats(ptr.Stats().HistColl, tableInfo.Columns[2].ID), Greater, 0)
			},
		},
		{ // CTE
			sql: "with cte(x, y) as (select d + 1, b from t where c > 1) select * from cte where x < 3",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				ps, ok := p.(*plannercore.PhysicalSelection)
				c.Check(ok, IsTrue)
				pc, ok := ps.Children()[0].(*plannercore.PhysicalCTE)
				c.Check(ok, IsTrue)
				pp, ok := pc.SeedPlan.(*plannercore.PhysicalProjection)
				c.Check(ok, IsTrue)
				reader, ok := pp.Children()[0].(*plannercore.PhysicalTableReader)
				c.Check(ok, IsTrue)
				c.Assert(countFullStats(reader.Stats().HistColl, tableInfo.Columns[2].ID), Greater, 0)
			},
		},
		{ // recursive CTE
			sql: "with recursive cte(x, y) as (select a, b from t where c > 1 union select x + 1, y from cte where x < 5) select * from cte",
			check: func(p plannercore.Plan, tableInfo *model.TableInfo) {
				pc, ok := p.(*plannercore.PhysicalCTE)
				c.Check(ok, IsTrue)
				pp, ok := pc.SeedPlan.(*plannercore.PhysicalProjection)
				c.Check(ok, IsTrue)
				reader, ok := pp.Children()[0].(*plannercore.PhysicalTableReader)
				c.Check(ok, IsTrue)
				c.Assert(countFullStats(reader.Stats().HistColl, tableInfo.Columns[2].ID), Greater, 0)
			},
		},
	}
	for _, testCase := range testCases {
		if testCase.skip {
			continue
		}
		is := dom.InfoSchema()
		dom.StatsHandle().Clear() // clear statsCache
		c.Assert(dom.StatsHandle().Update(is), IsNil)
		stmt, err := s.ParseOneStmt(testCase.sql, "", "")
		c.Check(err, IsNil)
		err = executor.ResetContextOfStmt(ctx, stmt)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Check(err, IsNil)
		tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		c.Assert(err, IsNil)
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

func (s *testPlanStatsSuite) TestPlanStatsLoadTimeout(c *C) {
	originConfig := config.GetGlobalConfig()
	newConfig := config.NewConfig()
	newConfig.Performance.StatsLoadConcurrency = 0 // no worker to consume channel
	newConfig.Performance.StatsLoadQueueSize = 1
	config.StoreGlobalConfig(newConfig)
	defer config.StoreGlobalConfig(originConfig)
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Check(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	originalVal1 := tk.MustQuery("select @@tidb_stats_load_pseudo_timeout").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_stats_load_pseudo_timeout = %v", originalVal1))
	}()

	ctx := tk.Se.(sessionctx.Context)
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
	c.Assert(dom.StatsHandle().Update(is), IsNil)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tableInfo := tbl.Meta()
	neededColumn := model.TableColumnID{TableID: tableInfo.ID, ColumnID: tableInfo.Columns[0].ID}
	resultCh := make(chan model.TableColumnID, 1)
	timeout := time.Duration(1<<63 - 1)
	dom.StatsHandle().AppendNeededColumn(neededColumn, resultCh, timeout) // make channel queue full
	stmt, err := s.ParseOneStmt("select * from t where c>1", "", "")
	c.Check(err, IsNil)
	tk.MustExec("set global tidb_stats_load_pseudo_timeout=false")
	_, _, err = planner.Optimize(context.TODO(), ctx, stmt, is)
	c.Check(err, NotNil) // fail sql for timeout when pseudo=false
	tk.MustExec("set global tidb_stats_load_pseudo_timeout=true")
	plan, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
	c.Check(err, IsNil) // not fail sql for timeout when pseudo=true
	switch pp := plan.(type) {
	case *plannercore.PhysicalTableReader:
		stats := pp.Stats().HistColl
		c.Assert(countFullStats(stats, tableInfo.Columns[0].ID), Greater, 0)
		c.Assert(countFullStats(stats, tableInfo.Columns[2].ID), Equals, 0) // pseudo stats
	default:
		c.Error("unexpected plan:", pp)
	}
}
