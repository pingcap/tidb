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
		c.Assert(len(row), Equals, 6)
		execInfo := row[4].(string)
		c.Assert(strings.Contains(execInfo, "time"), Equals, true)
		c.Assert(strings.Contains(execInfo, "loops"), Equals, true)
		c.Assert(strings.Contains(execInfo, "rows"), Equals, true)
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
	testKit.MustQuery("explain select * from t1, t2 where t1.a = t2.a").Check(testkit.Rows(
		"HashLeftJoin_8 7.49 root inner join, inner:TableReader_15, equal:[eq(test.t1.a, test.t2.a)]",
		"├─TableReader_12 5.99 root data:Selection_11",
		"│ └─Selection_11 5.99 cop[tikv] not(isnull(test.t1.a))",
		"│   └─TableScan_10 6.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_15 5.99 root data:Selection_14",
		"  └─Selection_14 5.99 cop[tikv] not(isnull(test.t2.a))",
		"    └─TableScan_13 6.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
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

	testKit.MustQuery("explain select straight_join * from t1, t2, t3, t4").Check(testkit.Rows(
		"HashLeftJoin_10 10000000000000000.00 root CARTESIAN inner join, inner:TableReader_23",
		"├─HashLeftJoin_12 1000000000000.00 root CARTESIAN inner join, inner:TableReader_21",
		"│ ├─HashLeftJoin_14 100000000.00 root CARTESIAN inner join, inner:TableReader_19",
		"│ │ ├─TableReader_17 10000.00 root data:TableScan_16",
		"│ │ │ └─TableScan_16 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ │ └─TableReader_19 10000.00 root data:TableScan_18",
		"│ │   └─TableScan_18 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ └─TableReader_21 10000.00 root data:TableScan_20",
		"│   └─TableScan_20 10000.00 cop[tikv] table:t3, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_23 10000.00 root data:TableScan_22",
		"  └─TableScan_22 10000.00 cop[tikv] table:t4, range:[-inf,+inf], keep order:false, stats:pseudo",
	))

	testKit.MustQuery("explain select * from t1 straight_join t2 straight_join t3 straight_join t4").Check(testkit.Rows(
		"HashLeftJoin_10 10000000000000000.00 root CARTESIAN inner join, inner:TableReader_23",
		"├─HashLeftJoin_12 1000000000000.00 root CARTESIAN inner join, inner:TableReader_21",
		"│ ├─HashLeftJoin_14 100000000.00 root CARTESIAN inner join, inner:TableReader_19",
		"│ │ ├─TableReader_17 10000.00 root data:TableScan_16",
		"│ │ │ └─TableScan_16 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ │ └─TableReader_19 10000.00 root data:TableScan_18",
		"│ │   └─TableScan_18 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ └─TableReader_21 10000.00 root data:TableScan_20",
		"│   └─TableScan_20 10000.00 cop[tikv] table:t3, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_23 10000.00 root data:TableScan_22",
		"  └─TableScan_22 10000.00 cop[tikv] table:t4, range:[-inf,+inf], keep order:false, stats:pseudo",
	))

	testKit.MustQuery("explain select straight_join * from t1, t2, t3, t4 where t1.a=t4.a;").Check(testkit.Rows(
		"HashLeftJoin_11 1248750000000.00 root inner join, inner:TableReader_26, equal:[eq(test.t1.a, test.t4.a)]",
		"├─HashLeftJoin_13 999000000000.00 root CARTESIAN inner join, inner:TableReader_23",
		"│ ├─HashRightJoin_16 99900000.00 root CARTESIAN inner join, inner:TableReader_19",
		"│ │ ├─TableReader_19 9990.00 root data:Selection_18",
		"│ │ │ └─Selection_18 9990.00 cop[tikv] not(isnull(test.t1.a))",
		"│ │ │   └─TableScan_17 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ │ └─TableReader_21 10000.00 root data:TableScan_20",
		"│ │   └─TableScan_20 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ └─TableReader_23 10000.00 root data:TableScan_22",
		"│   └─TableScan_22 10000.00 cop[tikv] table:t3, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_26 9990.00 root data:Selection_25",
		"  └─Selection_25 9990.00 cop[tikv] not(isnull(test.t4.a))",
		"    └─TableScan_24 10000.00 cop[tikv] table:t4, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
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
		`TableDual_6 0.00 root rows:0`,
	))

	testKit.MustQuery(`explain select * from t where 1 = 1 limit 0`).Check(testkit.Rows(
		`TableDual_5 0.00 root rows:0`,
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
		"HashAgg_9 2.00 root group by:col_1, funcs:count(col_0)",
		"└─TableReader_10 2.00 root data:HashAgg_5",
		"  └─HashAgg_5 2.00 cop[tikv] group by:test.t.a, funcs:count(1)",
		"    └─TableScan_8 8.00 cop[tikv] table:t, range:[-inf,+inf], keep order:false",
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
	sessionVars := ctx.GetSessionVars()
	sessionVars.HashAggFinalConcurrency = 1
	sessionVars.HashAggPartialConcurrency = 1
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select count(*) from t group by e",
			best: "IndexReader(Index(t.e)[[NULL,+inf]]->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e <= 10 group by e",
			best: "IndexReader(Index(t.e)[[-inf,10]]->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e <= 50",
			best: "IndexReader(Index(t.e)[[-inf,50]]->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select count(*) from t where c > '1' group by b",
			best: "IndexReader(Index(t.b_c)[[NULL,+inf]]->Sel([gt(test.t.c, 1)])->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select count(*) from t where e = 1 group by b",
			best: "IndexLookUp(Index(t.e)[[1,1]], Table(t))->HashAgg",
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
			best: "TableReader(Table(t)->Sel([le(test.t.b, 30)])->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 40",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 40)])->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)])->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select count(e) from t where t.b <= 100000000000",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 100000000000)])->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select * from t where t.b <= 40",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 40)]))",
		},
		{
			sql:  "select * from t where t.b <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 50)]))",
		},
		{
			sql:  "select * from t where t.b <= 10000000000",
			best: "TableReader(Table(t)->Sel([le(test.t.b, 10000000000)]))",
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
		{
			sql:  "select sum(a) from t1 use index(idx) where a = 3 and b = 100000 group by a limit 1",
			best: "IndexLookUp(Index(t1.idx)[[3,3]], Table(t1)->Sel([eq(test.t1.b, 100000)]))->Projection->Projection->StreamAgg->Limit",
		},
	}
	for _, tt := range tests {
		stmts, err := session.Parse(ctx, tt.sql)
		c.Assert(err, IsNil)
		c.Assert(stmts, HasLen, 1)
		stmt := stmts[0]
		is := domain.GetDomain(ctx).InfoSchema()
		err = core.Preprocess(ctx, stmt, is)
		c.Assert(err, IsNil)
		p, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
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
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t where t.c1 <= 50",
			best: "TableReader(Table(t)->Sel([le(test.t.c1, 50)]))",
		},
		{
			sql:  "select * from t where c1 in (select c1 from t1)",
			best: "LeftHashJoin{TableReader(Table(t)->Sel([not(isnull(test.t.c1))]))->TableReader(Table(t1)->Sel([not(isnull(test.t1.c1))]))->HashAgg}(test.t.c1,test.t1.c1)->Projection",
		},
		{
			sql:  "select * from t, t1 where t.c1 = t1.c1",
			best: "LeftHashJoin{TableReader(Table(t)->Sel([not(isnull(test.t.c1))]))->TableReader(Table(t1)->Sel([not(isnull(test.t1.c1))]))}(test.t.c1,test.t1.c1)",
		},
		{
			sql:  "select * from t limit 0",
			best: "Dual",
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
		p, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
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

	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "analyze table t3",
			best: "Analyze{Index(a),Table(a, b)}",
		},
		// Test analyze full table.
		{
			sql:  "select * from t where t.a <= 2",
			best: "TableReader(Table(t)->Sel([le(test.t.a, 2)]))",
		},
		{
			sql:  "select * from t where t.b < 2",
			best: "TableReader(Table(t)->Sel([lt(test.t.b, 2)]))",
		},
		{
			sql:  "select * from t where t.a = 1 and t.b <= 2",
			best: "TableReader(Table(t)->Sel([eq(test.t.a, 1) le(test.t.b, 2)]))",
		},
		// Test not analyzed table.
		{
			sql:  "select * from t1 where t1.a <= 2",
			best: "TableReader(Table(t1)->Sel([le(test.t1.a, 2)]))",
		},
		{
			sql:  "select * from t1 where t1.a = 1 and t1.b <= 2",
			best: "IndexLookUp(Index(t1.a)[[1,1]], Table(t1)->Sel([le(test.t1.b, 2)]))",
		},
		// Test analyze single index.
		{
			sql:  "select * from t2 where t2.a <= 2",
			best: "TableReader(Table(t2)->Sel([le(test.t2.a, 2)]))",
		},
		// Test analyze all index.
		{
			sql:  "analyze table t2 index",
			best: "Analyze{Index(a),Index(b)}",
		},
		// Test partitioned table.
		{
			sql:  "select * from t4 where t4.a <= 2",
			best: "UnionAll{TableReader(Table(t4)->Sel([le(test.t4.a, 2)]))->TableReader(Table(t4)->Sel([le(test.t4.a, 2)]))}",
		},
		{
			sql:  "select * from t4 where t4.b < 2",
			best: "UnionAll{TableReader(Table(t4)->Sel([lt(test.t4.b, 2)]))->TableReader(Table(t4)->Sel([lt(test.t4.b, 2)]))}",
		},
		{
			sql:  "select * from t4 where t4.a = 1 and t4.b <= 2",
			best: "TableReader(Table(t4)->Sel([eq(test.t4.a, 1) le(test.t4.b, 2)]))",
		},
		// TODO: Refine these tests in the future.
		//{
		//	sql:  "select * from t2 where t2.a = 1 and t2.b <= 2",
		//	best: "IndexLookUp(Index(t2.b)[[-inf,2]], Table(t2)->Sel([eq(test.t2.a, 1)]))",
		//},
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
		p, err := planner.Optimize(context.TODO(), ctx, stmt, is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
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
		"TableReader_7 35.91 root data:Selection_6",
		"└─Selection_6 35.91 cop[tikv] le(test.t.a, 5), le(test.t.b, 5)",
		"  └─TableScan_5 80.00 cop[tikv] table:t, range:[-inf,+inf], keep order:false",
	))
	statistics.RatioOfPseudoEstimate.Store(0.7)
	testKit.MustQuery("explain select * from t where a <= 5 and b <= 5").Check(testkit.Rows(
		"TableReader_7 8.84 root data:Selection_6",
		"└─Selection_6 8.84 cop[tikv] le(test.t.a, 5), le(test.t.b, 5)",
		"  └─TableScan_5 80.00 cop[tikv] table:t, range:[-inf,+inf], keep order:false, stats:pseudo",
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
		p, err := planner.Optimize(context.TODO(), ctx, stmt, is)
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
	testKit.MustQuery("explain select * from t where a is null").Check(testkit.Rows(
		"TableReader_7 2.00 root data:Selection_6",
		"└─Selection_6 2.00 cop[tikv] isnull(test.t.a)",
		"  └─TableScan_5 2.00 cop[tikv] table:t, range:[-inf,+inf], keep order:false",
	))
	testKit.MustQuery("explain select * from t use index(idx) where a is null").Check(testkit.Rows(
		"IndexLookUp_7 2.00 root ",
		"├─IndexScan_5 2.00 cop[tikv] table:t, index:a, range:[NULL,NULL], keep order:false",
		"└─TableScan_6 2.00 cop[tikv] table:t, keep order:false",
	))
	h := dom.StatsHandle()
	h.Clear()
	c.Assert(h.Update(dom.InfoSchema()), IsNil)
	testKit.MustQuery("explain select * from t where b = 1").Check(testkit.Rows(
		"TableReader_7 0.00 root data:Selection_6",
		"└─Selection_6 0.00 cop[tikv] eq(test.t.b, 1)",
		"  └─TableScan_5 2.00 cop[tikv] table:t, range:[-inf,+inf], keep order:false",
	))
	testKit.MustQuery("explain select * from t where b < 1").Check(testkit.Rows(
		"TableReader_7 0.00 root data:Selection_6",
		"└─Selection_6 0.00 cop[tikv] lt(test.t.b, 1)",
		"  └─TableScan_5 2.00 cop[tikv] table:t, range:[-inf,+inf], keep order:false",
	))
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
	tk.MustExec("create table t(a int, b int, c int, index idx(c))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9),(10,10,10)")
	tk.MustExec("analyze table t")
	ctx := tk.Se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()
	sessionVars.HashAggFinalConcurrency = 1
	sessionVars.HashAggPartialConcurrency = 1
	tk.MustQuery("explain select t.c in (select count(*) from t s , t t1 where s.a = t.a and s.a = t1.a) from t;").
		Check(testkit.Rows(
			"Projection_11 10.00 root 9_aux_0",
			"└─Apply_13 10.00 root CARTESIAN left outer semi join, inner:StreamAgg_20, other cond:eq(test.t.c, 7_col_0)",
			"  ├─TableReader_15 10.00 root data:TableScan_14",
			"  │ └─TableScan_14 10.00 cop[tikv] table:t, range:[-inf,+inf], keep order:false",
			"  └─StreamAgg_20 1.00 root funcs:count(1)",
			"    └─HashLeftJoin_21 1.00 root inner join, inner:TableReader_28, equal:[eq(test.s.a, test.t1.a)]",
			"      ├─TableReader_25 1.00 root data:Selection_24",
			"      │ └─Selection_24 1.00 cop[tikv] eq(test.s.a, test.t.a), not(isnull(test.s.a))",
			"      │   └─TableScan_23 10.00 cop[tikv] table:s, range:[-inf,+inf], keep order:false",
			"      └─TableReader_28 1.00 root data:Selection_27",
			"        └─Selection_27 1.00 cop[tikv] eq(test.t1.a, test.t.a), not(isnull(test.t1.a))",
			"          └─TableScan_26 10.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false",
		))
	tk.MustQuery("explain select (select concat(t1.a, \",\", t1.b) from t t1 where t1.a=t.a and t1.c=t.c) from t").
		Check(testkit.Rows(
			"Projection_8 10.00 root concat(t1.a, \",\", t1.b)",
			"└─Apply_10 10.00 root CARTESIAN left outer join, inner:MaxOneRow_13",
			"  ├─TableReader_12 10.00 root data:TableScan_11",
			"  │ └─TableScan_11 10.00 cop[tikv] table:t, range:[-inf,+inf], keep order:false",
			"  └─MaxOneRow_13 1.00 root ",
			"    └─Projection_14 0.10 root concat(cast(test.t1.a), \",\", cast(test.t1.b))",
			"      └─IndexLookUp_21 0.10 root ",
			"        ├─IndexScan_18 1.00 cop[tikv] table:t1, index:c, range: decided by [eq(test.t1.c, test.t.c)], keep order:false",
			"        └─Selection_20 0.10 cop[tikv] eq(test.t1.a, test.t.a)",
			"          └─TableScan_19 1.00 cop[tikv] table:t1, keep order:false",
		))
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
			"IndexLookUp_8 10.00 root ",
			"├─IndexScan_5 12.50 cop[tikv] table:t, index:a, b, range:[5,5], keep order:false",
			"└─Selection_7 10.00 cop[tikv] eq(test.t.c, 5)",
			"  └─TableScan_6 12.50 cop[tikv] table:t, keep order:false",
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
				_, err := planner.Optimize(context.TODO(), ctx, stmt, is)
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
	tk.MustExec("create table t1(a bigint, b bigint, c bigint)")
	tk.MustExec("create table t2(a bigint, b bigint, c bigint, index idx(a, b, c))")

	tk.MustQuery("explain select /*+ TIDB_INLJ(t2) */ * from t1 join t2 on t2.a=t1.a and t2.b>t1.b-1 and t2.b<t1.b+1 and t2.c=t1.c;").Check(testkit.Rows(
		"IndexJoin_9 12475.01 root inner join, inner:IndexReader_8, outer key:test.t1.a, inner key:test.t2.a, other cond:eq(test.t1.c, test.t2.c), gt(test.t2.b, minus(test.t1.b, 1)), lt(test.t2.b, plus(test.t1.b, 1))",
		"├─TableReader_12 9980.01 root data:Selection_11",
		"│ └─Selection_11 9980.01 cop[tikv] not(isnull(test.t1.a)), not(isnull(test.t1.c))",
		"│   └─TableScan_10 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─IndexReader_8 9.98 root index:Selection_7",
		"  └─Selection_7 9.98 cop[tikv] not(isnull(test.t2.a)), not(isnull(test.t2.c))",
		"    └─IndexScan_6 10.00 cop[tikv] table:t2, index:a, b, c, range: decided by [eq(test.t2.a, test.t1.a) gt(test.t2.b, minus(test.t1.b, 1)) lt(test.t2.b, plus(test.t1.b, 1))], keep order:false, stats:pseudo",
	))

	tk.MustExec("create table t(a int, b int, index idx_ab(a, b))")
	tk.MustQuery("explain select * from t t1 join t t2 where t1.b = t2.b and t2.b is null").Check(testkit.Rows(
		"Projection_7 0.00 root test.t1.a, test.t1.b, test.t2.a, test.t2.b",
		"└─HashRightJoin_9 0.00 root inner join, inner:IndexReader_12, equal:[eq(test.t2.b, test.t1.b)]",
		"  ├─IndexReader_12 0.00 root index:Selection_11",
		"  │ └─Selection_11 0.00 cop[tikv] isnull(test.t2.b), not(isnull(test.t2.b))",
		"  │   └─IndexScan_10 10000.00 cop[tikv] table:t2, index:a, b, range:[NULL,+inf], keep order:false, stats:pseudo",
		"  └─IndexReader_15 9990.00 root index:Selection_14",
		"    └─Selection_14 9990.00 cop[tikv] not(isnull(test.t1.b))",
		"      └─IndexScan_13 10000.00 cop[tikv] table:t1, index:a, b, range:[NULL,+inf], keep order:false, stats:pseudo",
	))
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
	rs := tk.MustQuery("explain analyze select t1.id, t2.a from t1 join t2 on t1.a = t2.d where t1.b = 't2' and t1.d = 4")

	// Expected output is like:
	//
	// +--------------------------------+----------+------+----------------------------------------------------------------------------------+----------------------------------+
	// | id                             | count    | task | operator info                                                                    | execution info                   |
	// +--------------------------------+----------+------+----------------------------------------------------------------------------------+----------------------------------+
	// | Projection_9                   | 10.00    | root | test.t1.id, test.t2.a                                                            | time:203.355µs, loops:1, rows:0  |
	// | └─IndexJoin_13                 | 10.00    | root | inner join, inner:IndexLookUp_12, outer key:test.t1.a, inner key:test.t2.d       | time:199.633µs, loops:1, rows:0  |
	// |   ├─Projection_16              | 8.00     | root | test.t1.id, test.t1.a, test.t1.b, cast(mod(test.t1.a, 30))                       | time:164.587µs, loops:1, rows:0  |
	// |   │ └─Selection_17             | 8.00     | root | eq(cast(mod(test.t1.a, 30)), 4)                                                  | time:157.768µs, loops:1, rows:0  |
	// |   │   └─TableReader_20         | 10.00    | root | data:Selection_19                                                                | time:154.61µs, loops:1, rows:0   |
	// |   │     └─Selection_19         | 10.00    | cop[tikv]  | eq(test.t1.b, "t2")                                                              | time:28.824µs, loops:1, rows:0   |
	// |   │       └─TableScan_18       | 10000.00 | cop[tikv]  | table:t1, range:[-inf,+inf], keep order:false, stats:pseudo                      | time:27.654µs, loops:1, rows:0   |
	// |   └─IndexLookUp_12             | 10.00    | root |                                                                                  | time:0ns, loops:0, rows:0        |
	// |     ├─IndexScan_10             | 10.00    | cop[tikv]  | table:t2, index:d, range: decided by [test.t1.a], keep order:false, stats:pseudo | time:0ns, loops:0, rows:0        |
	// |     └─TableScan_11             | 10.00    | cop[tikv]  | table:t2, keep order:false, stats:pseudo                                         | time:0ns, loops:0, rows:0        |
	// +--------------------------------+----------+------+----------------------------------------------------------------------------------+----------------------------------+
	// 10 rows in set (0.00 sec)
	//
	c.Assert(rs.Rows(), HasLen, 10)
	hasIndexLookUp12 := false
	for _, row := range rs.Rows() {
		c.Assert(row, HasLen, 6)
		if strings.HasSuffix(row[0].(string), "IndexLookUp_12") {
			hasIndexLookUp12 = true
			c.Assert(row[4], Equals, "time:0ns, loops:0, rows:0")
		}
	}
	c.Assert(hasIndexLookUp12, IsTrue)
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

	tk.MustQuery("desc select * from t").Check(testkit.Rows(
		"TableReader_7 10000.00 root data:TableScan_6",
		"└─TableScan_6 10000.00 cop[tiflash] table:t, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("desc select /*+ read_from_storage(tikv[t]) */ * from t").Check(testkit.Rows(
		"TableReader_5 10000.00 root data:TableScan_4",
		"└─TableScan_4 10000.00 cop[tikv] table:t, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("desc select * from t where t.a = 1 or t.a = 2").Check(testkit.Rows(
		"TableReader_6 2.00 root data:TableScan_5",
		"└─TableScan_5 2.00 cop[tikv] table:t, range:[1,1], [2,2], keep order:false, stats:pseudo",
	))
	tk.MustExec("set @@session.tidb_isolation_read_engines='tiflash'")
	tk.MustQuery("desc select * from t where t.a = 1 or t.a = 2").Check(testkit.Rows(
		"TableReader_6 2.00 root data:TableScan_5",
		"└─TableScan_5 2.00 cop[tiflash] table:t, range:[1,1], [2,2], keep order:false, stats:pseudo",
	))
}

func (s *testAnalyzeSuite) TestIndexEqualUnknown(c *C) {
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
	testKit.MustExec("CREATE TABLE t(a bigint(20) NOT NULL, b bigint(20) NOT NULL, c bigint(20) NOT NULL, PRIMARY KEY (a,c,b), KEY (b))")
	err = s.loadTableStats("analyzeSuiteTestIndexEqualUnknownT.json", dom)
	c.Assert(err, IsNil)
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
		})
		testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}
