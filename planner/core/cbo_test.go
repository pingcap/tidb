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
)

var _ = Suite(&testAnalyzeSuite{})

type testAnalyzeSuite struct {
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
		"│ └─Selection_11 5.99 cop not(isnull(test.t1.a))",
		"│   └─TableScan_10 6.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_15 5.99 root data:Selection_14",
		"  └─Selection_14 5.99 cop not(isnull(test.t2.a))",
		"    └─TableScan_13 6.00 cop table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
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
		"│ │ │ └─TableScan_16 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ │ └─TableReader_19 10000.00 root data:TableScan_18",
		"│ │   └─TableScan_18 10000.00 cop table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ └─TableReader_21 10000.00 root data:TableScan_20",
		"│   └─TableScan_20 10000.00 cop table:t3, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_23 10000.00 root data:TableScan_22",
		"  └─TableScan_22 10000.00 cop table:t4, range:[-inf,+inf], keep order:false, stats:pseudo",
	))

	testKit.MustQuery("explain select * from t1 straight_join t2 straight_join t3 straight_join t4").Check(testkit.Rows(
		"HashLeftJoin_10 10000000000000000.00 root CARTESIAN inner join, inner:TableReader_23",
		"├─HashLeftJoin_12 1000000000000.00 root CARTESIAN inner join, inner:TableReader_21",
		"│ ├─HashLeftJoin_14 100000000.00 root CARTESIAN inner join, inner:TableReader_19",
		"│ │ ├─TableReader_17 10000.00 root data:TableScan_16",
		"│ │ │ └─TableScan_16 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ │ └─TableReader_19 10000.00 root data:TableScan_18",
		"│ │   └─TableScan_18 10000.00 cop table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ └─TableReader_21 10000.00 root data:TableScan_20",
		"│   └─TableScan_20 10000.00 cop table:t3, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_23 10000.00 root data:TableScan_22",
		"  └─TableScan_22 10000.00 cop table:t4, range:[-inf,+inf], keep order:false, stats:pseudo",
	))

	testKit.MustQuery("explain select straight_join * from t1, t2, t3, t4 where t1.a=t4.a;").Check(testkit.Rows(
		"HashLeftJoin_11 1248750000000.00 root inner join, inner:TableReader_26, equal:[eq(test.t1.a, test.t4.a)]",
		"├─HashLeftJoin_13 999000000000.00 root CARTESIAN inner join, inner:TableReader_23",
		"│ ├─HashRightJoin_16 99900000.00 root CARTESIAN inner join, inner:TableReader_19",
		"│ │ ├─TableReader_19 9990.00 root data:Selection_18",
		"│ │ │ └─Selection_18 9990.00 cop not(isnull(test.t1.a))",
		"│ │ │   └─TableScan_17 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ │ └─TableReader_21 10000.00 root data:TableScan_20",
		"│ │   └─TableScan_20 10000.00 cop table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
		"│ └─TableReader_23 10000.00 root data:TableScan_22",
		"│   └─TableScan_22 10000.00 cop table:t3, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─TableReader_26 9990.00 root data:Selection_25",
		"  └─Selection_25 9990.00 cop not(isnull(test.t4.a))",
		"    └─TableScan_24 10000.00 cop table:t4, range:[-inf,+inf], keep order:false, stats:pseudo",
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
		"  └─HashAgg_5 2.00 cop group by:test.t.a, funcs:count(1)",
		"    └─TableScan_8 8.00 cop table:t, range:[-inf,+inf], keep order:false",
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
			best: "IndexLookUp(Index(t.b)[[1,1]], Table(t))",
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
			best: "LeftHashJoin{TableReader(Table(t)->Sel([not(isnull(test.t.c1))]))->TableReader(Table(t1)->Sel([not(isnull(test.t1.c1))])->HashAgg)->HashAgg}(test.t.c1,test.t1.c1)->Projection",
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
			sql:  "select b from t where t.b < 2",
			best: "IndexReader(Index(t.b)[[-inf,2)])",
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
			sql:  "select b from t4 where t4.b < 2",
			best: "UnionAll{IndexReader(Index(t4.b)[[-inf,2)])->IndexReader(Index(t4.b)[[-inf,2)])}",
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
		"└─Selection_6 35.91 cop le(test.t.a, 5), le(test.t.b, 5)",
		"  └─TableScan_5 80.00 cop table:t, range:[-inf,+inf], keep order:false",
	))
	statistics.RatioOfPseudoEstimate.Store(0.7)
	testKit.MustQuery("explain select * from t where a <= 5 and b <= 5").Check(testkit.Rows(
		"TableReader_7 8.84 root data:Selection_6",
		"└─Selection_6 8.84 cop le(test.t.a, 5), le(test.t.b, 5)",
		"  └─TableScan_5 80.00 cop table:t, range:[-inf,+inf], keep order:false, stats:pseudo",
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
		"└─Selection_6 2.00 cop isnull(test.t.a)",
		"  └─TableScan_5 2.00 cop table:t, range:[-inf,+inf], keep order:false",
	))
	testKit.MustQuery("explain select * from t use index(idx) where a is null").Check(testkit.Rows(
		"IndexLookUp_7 2.00 root ",
		"├─IndexScan_5 2.00 cop table:t, index:a, range:[NULL,NULL], keep order:false",
		"└─TableScan_6 2.00 cop table:t, keep order:false",
	))
	h := dom.StatsHandle()
	h.Clear()
	c.Assert(h.Update(dom.InfoSchema()), IsNil)
	testKit.MustQuery("explain select * from t where b = 1").Check(testkit.Rows(
		"TableReader_7 0.00 root data:Selection_6",
		"└─Selection_6 0.00 cop eq(test.t.b, 1)",
		"  └─TableScan_5 2.00 cop table:t, range:[-inf,+inf], keep order:false",
	))
	testKit.MustQuery("explain select * from t where b < 1").Check(testkit.Rows(
		"TableReader_7 0.00 root data:Selection_6",
		"└─Selection_6 0.00 cop lt(test.t.b, 1)",
		"  └─TableScan_5 2.00 cop table:t, range:[-inf,+inf], keep order:false",
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
	tk.MustExec("create table t(a int, b int, c int, index idx(c,b,a))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5), (6,6,6), (7,7,7), (8,8,8), (9,9,9),(10,10,10)")
	tk.MustExec("analyze table t")
	tk.MustQuery("explain select t.c in (select count(*) from t s , t t1 where s.a = t.a and s.a = t1.a) from t;").
		Check(testkit.Rows(
			"Projection_11 10.00 root 9_aux_0",
			"└─Apply_13 10.00 root CARTESIAN left outer semi join, inner:StreamAgg_20, other cond:eq(test.t.c, 7_col_0)",
			"  ├─TableReader_15 10.00 root data:TableScan_14",
			"  │ └─TableScan_14 10.00 cop table:t, range:[-inf,+inf], keep order:false",
			"  └─StreamAgg_20 1.00 root funcs:count(1)",
			"    └─HashLeftJoin_21 1.00 root inner join, inner:TableReader_28, equal:[eq(test.s.a, test.t1.a)]",
			"      ├─TableReader_25 1.00 root data:Selection_24",
			"      │ └─Selection_24 1.00 cop eq(test.s.a, test.t.a), not(isnull(test.s.a))",
			"      │   └─TableScan_23 10.00 cop table:s, range:[-inf,+inf], keep order:false",
			"      └─TableReader_28 1.00 root data:Selection_27",
			"        └─Selection_27 1.00 cop eq(test.t1.a, test.t.a), not(isnull(test.t1.a))",
			"          └─TableScan_26 10.00 cop table:t1, range:[-inf,+inf], keep order:false",
		))
	tk.MustQuery("explain select (select concat(t1.a, \",\", t1.b) from t t1 where t1.a=t.a and t1.c=t.c) from t").
		Check(testkit.Rows(
			"Projection_8 10.00 root concat(t1.a, \",\", t1.b)",
			"└─Apply_10 10.00 root CARTESIAN left outer join, inner:MaxOneRow_13",
			"  ├─TableReader_12 10.00 root data:TableScan_11",
			"  │ └─TableScan_11 10.00 cop table:t, range:[-inf,+inf], keep order:false",
			"  └─MaxOneRow_13 1.00 root ",
			"    └─Projection_14 0.10 root concat(cast(test.t1.a), \",\", cast(test.t1.b))",
			"      └─IndexReader_17 0.10 root index:Selection_16",
			"        └─Selection_16 0.10 cop eq(test.t1.a, test.t.a)",
			"          └─IndexScan_15 1.00 cop table:t1, index:c, b, a, range: decided by [eq(test.t1.c, test.t.c)], keep order:false",
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
			"├─IndexScan_5 12.50 cop table:t, index:a, b, range:[5,5], keep order:false",
			"└─Selection_7 10.00 cop eq(test.t.c, 5)",
			"  └─TableScan_6 12.50 cop table:t, keep order:false",
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
		"IndexMergeJoin_14 12475.01 root inner join, inner:IndexReader_12, outer key:test.t1.a, inner key:test.t2.a, other cond:eq(test.t1.c, test.t2.c), gt(test.t2.b, minus(test.t1.b, 1)), lt(test.t2.b, plus(test.t1.b, 1))",
		"├─TableReader_17 9980.01 root data:Selection_16",
		"│ └─Selection_16 9980.01 cop not(isnull(test.t1.a)), not(isnull(test.t1.c))",
		"│   └─TableScan_15 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"└─IndexReader_12 9.98 root index:Selection_11",
		"  └─Selection_11 9.98 cop not(isnull(test.t2.a)), not(isnull(test.t2.c))",
		"    └─IndexScan_10 10.00 cop table:t2, index:a, b, c, range: decided by [eq(test.t2.a, test.t1.a) gt(test.t2.b, minus(test.t1.b, 1)) lt(test.t2.b, plus(test.t1.b, 1))], keep order:true, stats:pseudo",
	))

	tk.MustExec("create table t(a int, b int, index idx_ab(a, b))")
	tk.MustQuery("explain select * from t t1 join t t2 where t1.b = t2.b and t2.b is null").Check(testkit.Rows(
		"Projection_7 0.00 root test.t1.a, test.t1.b, test.t2.a, test.t2.b",
		"└─HashRightJoin_9 0.00 root inner join, inner:TableReader_12, equal:[eq(test.t2.b, test.t1.b)]",
		"  ├─TableReader_12 0.00 root data:Selection_11",
		"  │ └─Selection_11 0.00 cop isnull(test.t2.b), not(isnull(test.t2.b))",
		"  │   └─TableScan_10 10000.00 cop table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
		"  └─TableReader_15 9990.00 root data:Selection_14",
		"    └─Selection_14 9990.00 cop not(isnull(test.t1.b))",
		"      └─TableScan_13 10000.00 cop table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
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
	// +--------------------------------+----------+------+-------------------------------------------------------------------------------------------------+----------------------------------+---------------+
	// | id                             | count    | task | operator info                                                                                   | execution info                   | memory        |
	// +--------------------------------+----------+------+-------------------------------------------------------------------------------------------------+----------------------------------+---------------+
	// | Projection_9                   | 10.00    | root | test.t1.id, test.t2.a                                                                           | time:0s, loops:0, rows:0         | N/A           |
	// | └─IndexJoin_13                 | 10.00    | root | inner join, inner:IndexLookUp_12, outer key:test.t1.a, inner key:test.t2.d                      | time:275.903µs, loops:1, rows:0  | 1.57421875 KB |
	// |   ├─Projection_21              | 8.00     | root | test.t1.id, test.t1.a, test.t1.b, cast(mod(test.t1.a, 30))                                      | time:23.811µs, loops:1, rows:0   | N/A           |
	// |   │ └─Selection_22             | 8.00     | root | eq(cast(mod(test.t1.a, 30)), 4)                                                                 | time:18.658µs, loops:1, rows:0   | N/A           |
	// |   │   └─TableReader_25         | 10.00    | root | data:Selection_24                                                                               | time:15.488µs, loops:1, rows:0   | 117 Bytes     |
	// |   │     └─Selection_24         | 10.00    | cop  | eq(test.t1.b, "t2")                                                                             | time:21.872µs, loops:1, rows:0   | N/A           |
	// |   │       └─TableScan_23       | 10000.00 | cop  | table:t1, range:[-inf,+inf], keep order:false, stats:pseudo                                     | time:20.79µs, loops:1, rows:0    | N/A           |
	// |   └─IndexLookUp_12             | 10.00    | root |                                                                                                 | time:0ns, loops:0, rows:0        | N/A           |
	// |     ├─IndexScan_10             | 10.00    | cop  | table:t2, index:d, range: decided by [eq(test.t2.d, test.t1.a)], keep order:false, stats:pseudo | time:0ns, loops:0, rows:0        | N/A           |
	// |     └─TableScan_11             | 10.00    | cop  | table:t2, keep order:false, stats:pseudo                                                        | time:0ns, loops:0, rows:0        | N/A           |
	// +--------------------------------+----------+------+-------------------------------------------------------------------------------------------------+----------------------------------+---------------+
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
	tk.MustExec("set session tidb_opt_correlation_exp_factor = 0")
	// Pseudo stats.
	tk.MustQuery("EXPLAIN SELECT * FROM t WHERE b = 2 ORDER BY a limit 1;").Check(testkit.Rows(
		"TopN_8 1.00 root test.t.a:asc, offset:0, count:1",
		"└─IndexReader_16 1.00 root index:TopN_15",
		"  └─TopN_15 1.00 cop test.t.a:asc, offset:0, count:1",
		"    └─IndexScan_14 10.00 cop table:t, index:b, c, range:[2,2], keep order:false, stats:pseudo",
	))
	// Positive correlation.
	tk.MustExec("insert into t (a, b) values (1, 1),(2, 1),(3, 1),(4, 1),(5, 1),(6, 1),(7, 1),(8, 1),(9, 1),(10, 1),(11, 1),(12, 1),(13, 1),(14, 1),(15, 1),(16, 1),(17, 1),(18, 1),(19, 1),(20, 2),(21, 2),(22, 2),(23, 2),(24, 2),(25, 2)")
	tk.MustExec("analyze table t")
	tk.MustQuery("EXPLAIN SELECT * FROM t WHERE b = 2 ORDER BY a limit 1;").Check(testkit.Rows(
		"TopN_8 1.00 root test.t.a:asc, offset:0, count:1",
		"└─IndexReader_16 1.00 root index:TopN_15",
		"  └─TopN_15 1.00 cop test.t.a:asc, offset:0, count:1",
		"    └─IndexScan_14 6.00 cop table:t, index:b, c, range:[2,2], keep order:false",
	))
	// Negative correlation.
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t (a, b) values (1, 25),(2, 24),(3, 23),(4, 23),(5, 21),(6, 20),(7, 19),(8, 18),(9, 17),(10, 16),(11, 15),(12, 14),(13, 13),(14, 12),(15, 11),(16, 10),(17, 9),(18, 8),(19, 7),(20, 6),(21, 5),(22, 4),(23, 3),(24, 2),(25, 1)")
	tk.MustExec("analyze table t")
	tk.MustQuery("EXPLAIN SELECT * FROM t WHERE b <= 6 ORDER BY a limit 1").Check(testkit.Rows(
		"TopN_8 1.00 root test.t.a:asc, offset:0, count:1",
		"└─IndexReader_16 1.00 root index:TopN_15",
		"  └─TopN_15 1.00 cop test.t.a:asc, offset:0, count:1",
		"    └─IndexScan_14 6.00 cop table:t, index:b, c, range:[-inf,6], keep order:false",
	))
	// Outer plan of index join (to test that correct column ID is used).
	tk.MustQuery("EXPLAIN SELECT *, t1.a IN (SELECT t2.b FROM t t2) FROM t t1 WHERE t1.b <= 6 ORDER BY t1.a limit 1").Check(testkit.Rows(
		"Limit_17 1.00 root offset:0, count:1",
		"└─IndexMergeJoin_66 1.00 root left outer semi join, inner:IndexReader_64, outer key:test.t1.a, inner key:test.t2.b",
		"  ├─TopN_27 1.00 root test.t1.a:asc, offset:0, count:1",
		"  │ └─IndexReader_35 1.00 root index:TopN_34",
		"  │   └─TopN_34 1.00 cop test.t1.a:asc, offset:0, count:1",
		"  │     └─IndexScan_33 6.00 cop table:t1, index:b, c, range:[-inf,6], keep order:false",
		"  └─IndexReader_64 1.04 root index:IndexScan_63",
		"    └─IndexScan_63 1.04 cop table:t2, index:b, c, range: decided by [eq(test.t2.b, test.t1.a)], keep order:true",
	))
	// Desc TableScan.
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t (a, b) values (1, 1),(2, 1),(3, 1),(4, 1),(5, 1),(6, 1),(7, 2),(8, 2),(9, 2),(10, 2),(11, 2),(12, 2),(13, 2),(14, 2),(15, 2),(16, 2),(17, 2),(18, 2),(19, 2),(20, 2),(21, 2),(22, 2),(23, 2),(24, 2),(25, 2)")
	tk.MustExec("analyze table t")
	tk.MustQuery("EXPLAIN SELECT * FROM t WHERE b = 1 ORDER BY a desc limit 1").Check(testkit.Rows(
		"TopN_8 1.00 root test.t.a:desc, offset:0, count:1",
		"└─IndexReader_16 1.00 root index:TopN_15",
		"  └─TopN_15 1.00 cop test.t.a:desc, offset:0, count:1",
		"    └─IndexScan_14 6.00 cop table:t, index:b, c, range:[1,1], keep order:false",
	))
	// Correlation threshold not met.
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t (a, b) values (1, 1),(2, 1),(3, 1),(4, 1),(5, 1),(6, 1),(7, 1),(8, 1),(9, 2),(10, 1),(11, 1),(12, 1),(13, 1),(14, 2),(15, 2),(16, 1),(17, 2),(18, 1),(19, 2),(20, 1),(21, 2),(22, 1),(23, 1),(24, 1),(25, 1)")
	tk.MustExec("analyze table t")
	tk.MustQuery("EXPLAIN SELECT * FROM t WHERE b = 2 ORDER BY a limit 1").Check(testkit.Rows(
		"Limit_11 1.00 root offset:0, count:1",
		"└─TableReader_22 1.00 root data:Limit_21",
		"  └─Limit_21 1.00 cop offset:0, count:1",
		"    └─Selection_20 1.00 cop eq(test.t.b, 2)",
		"      └─TableScan_19 4.17 cop table:t, range:[-inf,+inf], keep order:true",
	))
	tk.MustExec("set session tidb_opt_correlation_exp_factor = 1")
	tk.MustQuery("EXPLAIN SELECT * FROM t WHERE b = 2 ORDER BY a limit 1").Check(testkit.Rows(
		"TopN_8 1.00 root test.t.a:asc, offset:0, count:1",
		"└─IndexReader_16 1.00 root index:TopN_15",
		"  └─TopN_15 1.00 cop test.t.a:asc, offset:0, count:1",
		"    └─IndexScan_14 6.00 cop table:t, index:b, c, range:[2,2], keep order:false",
	))
	tk.MustExec("set session tidb_opt_correlation_exp_factor = 0")
	// TableScan has access conditions, but correlation is 1.
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t (a, b) values (1, 1),(2, 1),(3, 1),(4, 1),(5, 1),(6, 1),(7, 1),(8, 1),(9, 1),(10, 1),(11, 1),(12, 1),(13, 1),(14, 1),(15, 1),(16, 1),(17, 1),(18, 1),(19, 1),(20, 2),(21, 2),(22, 2),(23, 2),(24, 2),(25, 2)")
	tk.MustExec("analyze table t")
	tk.MustQuery("EXPLAIN SELECT * FROM t WHERE b = 2 and a > 0 ORDER BY a limit 1").Check(testkit.Rows(
		"TopN_8 1.00 root test.t.a:asc, offset:0, count:1",
		"└─IndexReader_19 1.00 root index:TopN_18",
		"  └─TopN_18 1.00 cop test.t.a:asc, offset:0, count:1",
		"    └─Selection_17 6.00 cop gt(test.t.a, 0)",
		"      └─IndexScan_16 6.00 cop table:t, index:b, c, range:[2,2], keep order:false",
	))
	// Multi-column filter.
	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int primary key, b int, c int, d bigint default 2147483648, e bigint default 2147483648, f bigint default 2147483648, index idx(b,d,a,c))")
	tk.MustExec("insert into t(a, b, c) values (1, 1, 1),(2, 1, 2),(3, 1, 1),(4, 1, 2),(5, 1, 1),(6, 1, 2),(7, 1, 1),(8, 1, 2),(9, 1, 1),(10, 1, 2),(11, 1, 1),(12, 1, 2),(13, 1, 1),(14, 1, 2),(15, 1, 1),(16, 1, 2),(17, 1, 1),(18, 1, 2),(19, 1, 1),(20, 2, 2),(21, 2, 1),(22, 2, 2),(23, 2, 1),(24, 2, 2),(25, 2, 1)")
	tk.MustExec("analyze table t")
	tk.MustQuery("EXPLAIN SELECT a FROM t WHERE b = 2 and c > 0 ORDER BY a limit 1").Check(testkit.Rows(
		"Projection_7 1.00 root test.t.a",
		"└─TopN_8 1.00 root test.t.a:asc, offset:0, count:1",
		"  └─IndexReader_17 1.00 root index:TopN_16",
		"    └─TopN_16 1.00 cop test.t.a:asc, offset:0, count:1",
		"      └─Selection_15 6.00 cop gt(test.t.c, 0)",
		"        └─IndexScan_14 6.00 cop table:t, index:b, d, a, c, range:[2,2], keep order:false",
	))
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
