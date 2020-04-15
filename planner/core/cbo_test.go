// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testAnalyzeSuite{})

type testAnalyzeSuite struct {
	testData testutil.TestData
}

func (s *testAnalyzeSuite) SetUpSuite(c *C) {
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "analyze_suite")
	c.Assert(err, IsNil)
}

func (s *testAnalyzeSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testAnalyzeSuite) loadTableStats(fileName string, dom *domain.Domain) error {
	statsPath := filepath.Join("testdata", fileName)
	bytes, err := ioutil.ReadFile(statsPath)
	if err != nil {
		return err
	}
	statsTbl := &handle.JSONTable{}
	err = json.Unmarshal(bytes, statsTbl)
	if err != nil {
		return err
	}
	statsHandle := dom.StatsHandle()
	err = statsHandle.LoadStatsFromJSON(dom.InfoSchema(), statsTbl)
	if err != nil {
		return err
	}
	return nil
}

func (s *testAnalyzeSuite) TestExplainAnalyze(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	tk.MustExec("create table t1(a int, b int, c int, key idx(a, b))")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5)")
	tk.MustExec("insert into t2 values (2, 22), (3, 33), (5, 55), (233, 2), (333, 3), (3434, 5)")
	tk.MustExec("analyze table t1, t2")
	rs := tk.MustQuery("explain analyze select t1.a, t1.b, sum(t1.c) from t1 join t2 on t1.a = t2.b where t1.a > 1")
	c.Assert(len(rs.Rows()), Equals, 10)
	for _, row := range rs.Rows() {
		c.Assert(len(row), Equals, 9)
		execInfo := row[5].(string)
		c.Assert(strings.Contains(execInfo, "time"), Equals, true)
		c.Assert(strings.Contains(execInfo, "loops"), Equals, true)
	}
}

// TestCBOWithoutAnalyze tests the plan with stats that only have count info.
func (s *testAnalyzeSuite) TestCBOWithoutAnalyze(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("create table t1 (a int)")
	testKit.MustExec("create table t2 (a int)")
	h := dom.StatsHandle()
	c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)
	c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)
	testKit.MustExec("insert into t1 values (1), (2), (3), (4), (5), (6)")
	testKit.MustExec("insert into t2 values (1), (2), (3), (4), (5), (6)")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(dom.InfoSchema()), IsNil)
	testKit.MustExec("analyze table t1, t2")
	testKit.MustQuery("explain select * from t1, t2 where t1.a = t2.a").Check(testkit.Rows(
		"HashJoin_8 6.00 root  inner join, equal:[eq(test.t1.a, test.t2.a)]",
		"├─TableReader_15(Build) 6.00 root  data:Selection_14",
		"│ └─Selection_14 6.00 cop[tikv]  not(isnull(test.t2.a))",
		"│   └─TableFullScan_13 6.00 cop[tikv] table:t2 keep order:false",
		"└─TableReader_12(Probe) 6.00 root  data:Selection_11",
		"  └─Selection_11 6.00 cop[tikv]  not(isnull(test.t1.a))",
		"    └─TableFullScan_10 6.00 cop[tikv] table:t1 keep order:false",
	))
	testKit.MustQuery("explain format = 'hint' select * from t1, t2 where t1.a = t2.a").Check(testkit.Rows(
		"use_index(@`sel_1` `test`.`t1` ), use_index(@`sel_1` `test`.`t2` ), hash_join(@`sel_1` `test`.`t1`)"))
}

func (s *testAnalyzeSuite) TestStraightJoin(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	testKit.MustExec("use test")
	h := dom.StatsHandle()
	for _, tblName := range []string{"t1", "t2", "t3", "t4"} {
		testKit.MustExec(fmt.Sprintf("create table %s (a int)", tblName))
		c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)
	}
	var input []string
	var output [][]string
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func (s *testAnalyzeSuite) TestTableDual(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()

	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec(`use test`)
	h := dom.StatsHandle()
	testKit.MustExec(`create table t(a int)`)
	testKit.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
	c.Assert(h.HandleDDLEvent(<-h.DDLEventCh()), IsNil)

	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(dom.InfoSchema()), IsNil)

	testKit.MustQuery(`explain select * from t where 1 = 0`).Check(testkit.Rows(
		`TableDual_6 0.00 root  rows:0`,
	))

	testKit.MustQuery(`explain select * from t where 1 = 1 limit 0`).Check(testkit.Rows(
		`TableDual_5 0.00 root  rows:0`,
	))
}

func (s *testAnalyzeSuite) TestEstimation(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
		statistics.RatioOfPseudoEstimate.Store(0.7)
	}()
	statistics.RatioOfPseudoEstimate.Store(10.0)
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int)")
	testKit.MustExec("insert into t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	h := dom.StatsHandle()
	h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t")
	for i := 1; i <= 8; i++ {
		testKit.MustExec("delete from t where a = ?", i)
	}
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(dom.InfoSchema()), IsNil)
	testKit.MustQuery("explain select count(*) from t group by a").Check(testkit.Rows(
		"HashAgg_9 2.00 root  group by:test.t.a, funcs:count(Column#4)->Column#3",
		"└─TableReader_10 2.00 root  data:HashAgg_5",
		"  └─HashAgg_5 2.00 cop[tikv]  group by:test.t.a, funcs:count(1)->Column#4",
		"    └─TableFullScan_8 8.00 cop[tikv] table:t keep order:false",
	))
}

func constructInsertSQL(i, n int) string {
	sql := "insert into t (a,b,c,e)values "
	for j := 0; j < n; j++ {
		sql += fmt.Sprintf("(%d, %d, '%d', %d)", i*n+j, i, i+j, i*n+j)
		if j != n-1 {
			sql += ", "
		}
	}
	return sql
}

func (s *testAnalyzeSuite) TestIndexRead(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t, t1")
	testKit.MustExec("create table t (a int primary key, b int, c varchar(200), d datetime DEFAULT CURRENT_TIMESTAMP, e int, ts timestamp DEFAULT CURRENT_TIMESTAMP)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("create index d on t (d)")
	testKit.MustExec("create index e on t (e)")
	testKit.MustExec("create index b_c on t (b,c)")
	testKit.MustExec("create index ts on t (ts)")
	testKit.MustExec("create table t1 (a int, b int, index idx(a), index idxx(b))")

	// This stats is generated by following format:
	// fill (a, b, c, e) as (i*100+j, i, i+j, i*100+j), i and j is dependent and range of this two are [0, 99].
	err = s.loadTableStats("analyzesSuiteTestIndexReadT.json", dom)
	c.Assert(err, IsNil)
	for i := 1; i < 16; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t1 values(%v, %v)", i, i))
	}
	testKit.MustExec("analyze table t1")
	ctx := testKit.Se.(sessionctx.Context)
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	for i, tt := range input {
		stmts, err := session.Parse(ctx, tt)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := domain.GetDomain(ctx).InfoSchema()
		err = core.Preprocess(ctx, stmt, is)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)
		planString := core.ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], Commentf("for %s", tt))
	}
}

func (s *testAnalyzeSuite) TestEmptyTable(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t, t1")
	testKit.MustExec("create table t (c1 int)")
	testKit.MustExec("create table t1 (c1 int)")
	testKit.MustExec("analyze table t, t1")
	var input, output []string
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		ctx := testKit.Se.(sessionctx.Context)
		stmts, err := session.Parse(ctx, tt)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := domain.GetDomain(ctx).InfoSchema()
		err = core.Preprocess(ctx, stmt, is)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)
		planString := core.ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], Commentf("for %s", tt))
	}
}

func (s *testAnalyzeSuite) TestAnalyze(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t, t1, t2, t3")
	testKit.MustExec("create table t (a int, b int)")
	testKit.MustExec("create index a on t (a)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("insert into t (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze table t")

	testKit.MustExec("create table t1 (a int, b int)")
	testKit.MustExec("create index a on t1 (a)")
	testKit.MustExec("create index b on t1 (b)")
	testKit.MustExec("insert into t1 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")

	testKit.MustExec("create table t2 (a int, b int)")
	testKit.MustExec("create index a on t2 (a)")
	testKit.MustExec("create index b on t2 (b)")
	testKit.MustExec("insert into t2 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze table t2 index a")

	testKit.MustExec("create table t3 (a int, b int)")
	testKit.MustExec("create index a on t3 (a)")

	testKit.MustExec("create table t4 (a int, b int) partition by range (a) (partition p1 values less than (2), partition p2 values less than (3))")
	testKit.MustExec("create index a on t4 (a)")
	testKit.MustExec("create index b on t4 (b)")
	testKit.MustExec("insert into t4 (a,b) values (1,1),(1,2),(1,3),(1,4),(2,5),(2,6),(2,7),(2,8)")
	testKit.MustExec("analyze table t4")

	testKit.MustExec("create view v as select * from t")
	_, err = testKit.Exec("analyze table v")
	c.Assert(err.Error(), Equals, "analyze v is not supported now.")
	testKit.MustExec("drop view v")

	var input, output []string
	s.testData.GetTestCases(c, &input, &output)

	for i, tt := range input {
		ctx := testKit.Se.(sessionctx.Context)
		stmts, err := session.Parse(ctx, tt)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := domain.GetDomain(ctx).InfoSchema()
		err = core.Preprocess(ctx, stmt, is)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)
		planString := core.ToString(p)
		s.testData.OnRecord(func() {
			output[i] = planString
		})
		c.Assert(planString, Equals, output[i], Commentf("for %s", tt))
	}
}

func (s *testAnalyzeSuite) TestOutdatedAnalyze(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("create table t (a int, b int, index idx(a))")
	for i := 0; i < 10; i++ {
		testKit.MustExec(fmt.Sprintf("insert into t values (%d,%d)", i, i))
	}
	h := dom.StatsHandle()
	h.HandleDDLEvent(<-h.DDLEventCh())
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	testKit.MustExec("analyze table t")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	testKit.MustExec("insert into t select * from t")
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(dom.InfoSchema()), IsNil)
	statistics.RatioOfPseudoEstimate.Store(10.0)
	testKit.MustQuery("explain select * from t where a <= 5 and b <= 5").Check(testkit.Rows(
		"TableReader_7 35.91 root  data:Selection_6",
		"└─Selection_6 35.91 cop[tikv]  le(test.t.a, 5), le(test.t.b, 5)",
		"  └─TableFullScan_5 80.00 cop[tikv] table:t keep order:false",
	))
	statistics.RatioOfPseudoEstimate.Store(0.7)
	testKit.MustQuery("explain select * from t where a <= 5 and b <= 5").Check(testkit.Rows(
		"TableReader_7 0.00 root  data:Selection_6",
		"└─Selection_6 0.00 cop[tikv]  le(test.t.a, 5), le(test.t.b, 5)",
		"  └─TableFullScan_5 0.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
}

func (s *testAnalyzeSuite) TestPreparedNullParam(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()

	cfg := config.GetGlobalConfig()
	orgEnable := cfg.PreparedPlanCache.Enabled
	orgCapacity := cfg.PreparedPlanCache.Capacity
	flags := []bool{false, true}
	for _, flag := range flags {
		cfg.PreparedPlanCache.Enabled = flag
		cfg.PreparedPlanCache.Capacity = 100
		testKit := testkit.NewTestKit(c, store)
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (id int, KEY id (id))")
		testKit.MustExec("insert into t values (1), (2), (3)")

		sql := "select * from t where id = ?"
		best := "Dual"

		ctx := testKit.Se.(sessionctx.Context)
		stmts, err := session.Parse(ctx, sql)
		c.Assert(err, IsNil)
		stmt := stmts[0]

		is := domain.GetDomain(ctx).InfoSchema()
		err = core.Preprocess(ctx, stmt, is, core.InPrepare)
		c.Assert(err, IsNil)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)

		c.Assert(core.ToString(p), Equals, best, Commentf("for %s", sql))
	}
	cfg.PreparedPlanCache.Enabled = orgEnable
	cfg.PreparedPlanCache.Capacity = orgCapacity
}

func (s *testAnalyzeSuite) TestNullCount(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	testKit := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a int, b int, index idx(a))")
	testKit.MustExec("insert into t values (null, null), (null, null)")
	testKit.MustExec("analyze table t")
	var input []string
	var output [][]string
	s.testData.GetTestCases(c, &input, &output)
	for i := 0; i < 2; i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
	h := dom.StatsHandle()
	h.Clear()
	c.Assert(h.Update(dom.InfoSchema()), IsNil)
	for i := 2; i < 4; i++ {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(testKit.MustQuery(input[i]).Rows())
		})
		testKit.MustQuery(input[i]).Check(testkit.Rows(output[i]...))
	}
}

func (s *testAnalyzeSuite) TestCorrelatedEstimation(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	tk.MustExec("create table t(a int, b int, c int, index idx(c,b,a))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9),(10,10,10)")
	tk.MustExec("analyze table t")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		rs := tk.MustQuery(tt)
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(rs.Rows())
		})
		rs.Check(testkit.Rows(output[i]...))
	}
}

func (s *testAnalyzeSuite) TestInconsistentEstimation(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index ab(a,b), index ac(a,c))")
	tk.MustExec("insert into t values (1,1,1), (1000,1000,1000)")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values (5,5,5), (10,10,10)")
	}
	tk.MustExec("analyze table t with 2 buckets")
	// Force using the histogram to estimate.
	tk.MustExec("update mysql.stats_histograms set stats_ver = 0")
	dom.StatsHandle().Clear()
	dom.StatsHandle().Update(dom.InfoSchema())
	// Using the histogram (a, b) to estimate `a = 5` will get 1.22, while using the CM Sketch to estimate
	// the `a = 5 and c = 5` will get 10, it is not consistent.
	tk.MustQuery("explain select * from t use index(ab) where a = 5 and c = 5").
		Check(testkit.Rows(
			"IndexLookUp_8 10.00 root  ",
			"├─IndexRangeScan_5(Build) 12.50 cop[tikv] table:t, index:ab(a, b) range:[5,5], keep order:false",
			"└─Selection_7(Probe) 10.00 cop[tikv]  eq(test.t.c, 5)",
			"  └─TableRowIDScan_6 12.50 cop[tikv] table:t keep order:false",
		))
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	session.SetSchemaLease(0)
	session.DisableStats4Test()

	dom, err := session.BootstrapSession(store)
	if err != nil {
		return nil, nil, err
	}

	dom.SetStatsUpdating(true)
	return store, dom, errors.Trace(err)
}

func BenchmarkOptimize(b *testing.B) {
	c := &C{}
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()

	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t")
	testKit.MustExec("create table t (a int primary key, b int, c varchar(200), d datetime DEFAULT CURRENT_TIMESTAMP, e int, ts timestamp DEFAULT CURRENT_TIMESTAMP)")
	testKit.MustExec("create index b on t (b)")
	testKit.MustExec("create index d on t (d)")
	testKit.MustExec("create index e on t (e)")
	testKit.MustExec("create index b_c on t (b,c)")
	testKit.MustExec("create index ts on t (ts)")
	for i := 0; i < 100; i++ {
		testKit.MustExec(constructInsertSQL(i, 100))
	}
	testKit.MustExec("analyze table t")
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select count(*) from t group by e",
			best: "IndexReader(Index(t.e)[[NULL,+inf]])->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e <= 10 group by e",
			best: "IndexReader(Index(t.e)[[-inf,10]])->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e <= 50",
			best: "IndexReader(Index(t.e)[[-inf,50]]->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(*) from t where c > '1' group by b",
			best: "IndexReader(Index(t.b_c)[[NULL,+inf]]->Sel([gt(test.t.c, 1)]))->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e = 1 group by b",
			best: "IndexLookUp(Index(t.e)[[1,1]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(*) from t where e > 1 group by b",
			best: "TableReader(Table(t)->Sel([gt(test.t.e, 1)])->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 20",
			best: "IndexLookUp(Index(t.b)[[-inf,20]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 30",
			best: "IndexLookUp(Index(t.b)[[-inf,30]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 40",
			best: "IndexLookUp(Index(t.b)[[-inf,40]], Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)])->HashAgg)->HashAgg",
		},
		{
			sql:  "select * from t where t.b <= 40",
			best: "IndexLookUp(Index(t.b)[[-inf,40]], Table(t))",
		},
		{
			sql:  "select * from t where t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)]))",
		},
		// test panic
		{
			sql:  "select * from t where 1 and t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)]))",
		},
		{
			sql:  "select * from t where t.b <= 100 order by t.a limit 1",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 100)])->Limit)->Limit",
		},
		{
			sql:  "select * from t where t.b <= 1 order by t.a limit 10",
			best: "IndexLookUp(Index(t.b)[[-inf,1]]->TopN([test.t.a],0,10), Table(t))->TopN([test.t.a],0,10)",
		},
		{
			sql:  "select * from t use index(b) where b = 1 order by a",
			best: "IndexLookUp(Index(t.b)[[1,1]], Table(t))->Sort",
		},
		// test datetime
		{
			sql:  "select * from t where d < cast('1991-09-05' as datetime)",
			best: "IndexLookUp(Index(t.d)[[-inf,1991-09-05 00:00:00)], Table(t))",
		},
		// test timestamp
		{
			sql:  "select * from t where ts < '1991-09-05'",
			best: "IndexLookUp(Index(t.ts)[[-inf,1991-09-05 00:00:00)], Table(t))",
		},
	}
	for _, tt := range tests {
		ctx := testKit.Se.(sessionctx.Context)
		stmts, err := session.Parse(ctx, tt.sql)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := domain.GetDomain(ctx).InfoSchema()
		err = core.Preprocess(ctx, stmt, is)
		c.Assert(err, IsNil)

		b.Run(tt.sql, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := planner.Optimize(context.TODO(), ctx, stmt, is)
				c.Assert(err, IsNil)
			}
			b.ReportAllocs()
		})
	}
}

func (s *testAnalyzeSuite) TestIssue9562(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test")
	var input [][]string
	var output []struct {
		SQL  []string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				output[i].SQL = ts
				if j == len(ts)-1 {
					output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			}
		}
	}
}

func (s *testAnalyzeSuite) TestIssue9805(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`
		create table t1 (
			id bigint primary key,
			a bigint not null,
			b varchar(100) not null,
			c varchar(10) not null,
			d bigint as (a % 30) not null,
			key (d, b, c)
		)
	`)
	tk.MustExec(`
		create table t2 (
			id varchar(50) primary key,
			a varchar(100) unique,
			b datetime,
			c varchar(45),
			d int not null unique auto_increment
		)
	`)
	// Test when both tables are empty, EXPLAIN ANALYZE for IndexLookUp would not panic.
	tk.MustExec("explain analyze select /*+ TIDB_INLJ(t2) */ t1.id, t2.a from t1 join t2 on t1.a = t2.d where t1.b = 't2' and t1.d = 4")
}

func (s *testAnalyzeSuite) TestLimitCrossEstimation(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int not null, c int not null default 0, index idx_bc(b, c))")
	var input [][]string
	var output []struct {
		SQL  []string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				output[i].SQL = ts
				if j == len(ts)-1 {
					output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			}
		}
	}
}

func (s *testAnalyzeSuite) TestUpdateProjEliminate(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("explain update t t1, (select distinct b from t) t2 set t1.b = t2.b")
}

func (s *testAnalyzeSuite) TestTiFlashCostModel(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int, primary key(a))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3)")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	c.Assert(err, IsNil)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	var input, output [][]string
	s.testData.GetTestCases(c, &input, &output)
	for i, ts := range input {
		for j, tt := range ts {
			if j != len(ts)-1 {
				tk.MustExec(tt)
			}
			s.testData.OnRecord(func() {
				if j == len(ts)-1 {
					output[i] = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				}
			})
			if j == len(ts)-1 {
				tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
			}
		}
	}
}
