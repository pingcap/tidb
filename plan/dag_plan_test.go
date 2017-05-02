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

package plan_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testPlanSuite{})

type testPlanSuite struct {
	*parser.Parser
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
}

func (s *testPlanSuite) TestDAGPlanBuilderSimpleCase(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)

	plan.UseDAGPlanBuilder = true
	defer func() {
		plan.UseDAGPlanBuilder = false
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		// Test unready index hint.
		{
			sql:  "select * from t t1 use index(e)",
			best: "TableReader(Table(t))",
		},
		// Test index hint.
		{
			sql:  "select * from t t1 use index(c_d_e)",
			best: "IndexLookUp(Index(t.c_d_e)[[<nil>,+inf]], Table(t))",
		},
		// Test ts + Sort vs. DoubleRead + filter.
		{
			sql:  "select a from t where a between 1 and 2 order by c",
			best: "TableReader(Table(t))->Sort->Projection",
		},
		// Test DNF condition + Double Read.
		{
			sql:  "select * from t where (t.c > 0 and t.c < 1) or (t.c > 2 and t.c < 3) or (t.c > 4 and t.c < 5) or (t.c > 6 and t.c < 7) or (t.c > 9 and t.c < 10)",
			best: "IndexLookUp(Index(t.c_d_e)[(0 +inf,1 <nil>) (2 +inf,3 <nil>) (4 +inf,5 <nil>) (6 +inf,7 <nil>) (9 +inf,10 <nil>)], Table(t))",
		},
		// Test TopN to table branch in double read.
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 order by t.b limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t)->Sort + Limit(1) + Offset(0))->Sort + Limit(1) + Offset(0)",
		},
		// Test TopN to index branch in double read.
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 order by t.e limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)])->Sort + Limit(1) + Offset(0), Table(t))->Sort + Limit(1) + Offset(0)",
		},
		// Test TopN to Limit in double read.
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 order by t.d limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)])->Limit, Table(t))->Limit",
		},
		// Test TopN to Limit in index single read.
		{
			sql:  "select c from t where t.c = 1 and t.e = 1 order by t.d limit 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)])->Limit)->Limit->Projection->Projection",
		},
		// Test TopN to Limit in table single read.
		{
			sql:  "select c from t order by t.a limit 1",
			best: "TableReader(Table(t)->Limit)->Limit->Projection->Projection",
		},
		// Test TopN push down in table single read.
		{
			sql:  "select c from t order by t.a + t.b limit 1",
			best: "TableReader(Table(t)->Sort + Limit(1) + Offset(0))->Sort + Limit(1) + Offset(0)->Projection->Projection",
		},
		// Test Limit push down in table single read.
		{
			sql:  "select c from t  limit 1",
			best: "TableReader(Table(t)->Limit)->Limit",
		},
		// Test Limit push down in index single read.
		{
			sql:  "select c from t where c = 1 limit 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->Limit)->Limit",
		},
		// Test index single read and Selection.
		{
			sql:  "select c from t where c = 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])",
		},
		// Test index single read and Sort.
		{
			sql:  "select c from t order by c",
			best: "IndexReader(Index(t.c_d_e)[[<nil>,+inf]])",
		},
		// Test index single read and Sort.
		{
			sql:  "select c from t where c = 1 order by e",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->Sort->Projection",
		},
		// Test Limit push down in double single read.
		{
			sql:  "select c, b from t where c = 1 limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Limit, Table(t))->Limit->Projection",
		},
		// Test Selection + Limit push down in double single read.
		{
			sql:  "select c, b from t where c = 1 and e = 1 and b = 1 limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t)->Sel([eq(test.t.b, 1)])->Limit)->Limit->Projection",
		},
		// Test Order by multi columns.
		{
			sql:  "select c from t where c = 1 order by d, c",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->Sort->Projection",
		},
		// Test for index with length.
		{
			sql:  "select c_str from t where e_str = '1' order by d_str, c_str",
			best: "IndexLookUp(Index(t.e_d_c_str_prefix)[[1,1]], Table(t))->Projection->Sort->Projection",
		},
		// Test PK in index single read.
		{
			sql:  "select c from t where t.c = 1 and t.a = 1 order by t.d limit 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.a, 1)])->Limit)->Limit->Projection->Projection",
		},
		// Test PK in index double read.
		{
			sql:  "select * from t where t.c = 1 and t.a = 1 order by t.d limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.a, 1)])->Limit, Table(t))->Limit",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn()
		c.Assert(err, IsNil)

		is, err := plan.MockResolve(stmt)
		c.Assert(err, IsNil)
		p, err := plan.Optimize(se, stmt, is)
		c.Assert(err, IsNil)
		c.Assert(plan.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderJoin(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)

	plan.UseDAGPlanBuilder = true
	defer func() {
		plan.UseDAGPlanBuilder = false
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a join t t3 on t1.a = t3.a",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t1.a,t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a order by t1.a",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->Sort",
		},
		{
			sql:  "select * from t t1 left outer join t t2 on t1.a = t2.a right outer join t t3 on t1.a = t3.a",
			best: "RightHashJoin{LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t1.a,t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a join t t3 on t1.a = t3.a and t1.b = 1 and t3.c = 1",
			best: "LeftHashJoin{RightHashJoin{TableReader(Table(t)->Sel([eq(t1.b, 1)]))->TableReader(Table(t))}(t1.a,t2.a)->IndexLookUp(Index(t.c_d_e)[[1,1]], Table(t))}(t1.a,t3.a)",
		},
		{
			sql:  "select * from t where t.c in (select b from t s where s.a = t.a)",
			best: "SemiJoin{TableReader(Table(t))->TableReader(Table(t))}",
		},
		{
			sql:  "select t.c in (select b from t s where s.a = t.a) from t",
			best: "SemiJoinWithAux{TableReader(Table(t))->TableReader(Table(t))}->Projection",
		},
		// Test Single Merge Join.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.b",
			best: "MergeJoin{TableReader(Table(t))->TableReader(Table(t))->Sort}(t1.a,t2.b)",
		},
		// Test Single Merge Join + Sort.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.a order by t2.a",
			best: "MergeJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)",
		},
		// Test Multi Merge Join.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1, t t2, t t3 where t1.a = t2.a and t2.a = t3.a",
			best: "MergeJoin{MergeJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t2.a,t3.a)",
		},
		// Test Multi Merge Join + Outer Join.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1 left outer join t t2 on t1.a = t2.a left outer join t t3 on t2.a = t3.a",
			best: "MergeJoin{MergeJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->Sort->TableReader(Table(t))}(t2.a,t3.a)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1 left outer join t t2 on t1.a = t2.a left outer join t t3 on t1.a = t3.a",
			best: "MergeJoin{MergeJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t1.a,t3.a)",
		},
		// Test Apply.
		{
			sql:  "select t.c in (select count(*) from t s , t t1 where s.a = t.a and s.a = t1.a) from t",
			best: "Apply{TableReader(Table(t))->RightHashJoin{TableReader(Table(t))->Sel([eq(s.a, test.t.a)])->TableReader(Table(t))}(s.a,t1.a)->HashAgg}->Projection",
		},
		{
			sql:  "select (select count(*) from t s , t t1 where s.a = t.a and s.a = t1.a) from t",
			best: "Apply{TableReader(Table(t))->RightHashJoin{TableReader(Table(t))->Sel([eq(s.a, test.t.a)])->TableReader(Table(t))}(s.a,t1.a)->HashAgg}->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := plan.MockResolve(stmt)
		c.Assert(err, IsNil)
		p, err := plan.Optimize(se, stmt, is)
		c.Assert(err, IsNil)
		c.Assert(plan.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderBasePhysicalPlan(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)

	plan.UseDAGPlanBuilder = true
	defer func() {
		plan.UseDAGPlanBuilder = false
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		// Test for update.
		{
			sql: "select * from t order by b limit 1 for update",
			// TODO: This is not reasonable. Mysql do like this because the limit of InnoDB, should TiDB keep consistency with MySQL?
			best: "TableReader(Table(t))->Lock->Sort + Limit(1) + Offset(0)",
		},
		// Test complex update.
		{
			sql:  "update t set a = 5 where b < 1 order by d limit 1",
			best: "TableReader(Table(t)->Sel([lt(test.t.b, 1)])->Sort + Limit(1) + Offset(0))->Sort + Limit(1) + Offset(0)->*plan.Update",
		},
		// Test simple update.
		{
			sql:  "update t set a = 5",
			best: "TableReader(Table(t))->*plan.Update",
		},
		// TODO: Test delete/update with join.
		// Test complex delete.
		{
			sql:  "delete from t where b < 1 order by d limit 1",
			best: "TableReader(Table(t)->Sel([lt(test.t.b, 1)])->Sort + Limit(1) + Offset(0))->Sort + Limit(1) + Offset(0)->*plan.Delete",
		},
		// Test simple delete.
		{
			sql:  "delete from t",
			best: "TableReader(Table(t))->*plan.Delete",
		},
		// Test complex insert.
		{
			sql:  "insert into t select * from t where b < 1 order by d limit 1",
			best: "TableReader(Table(t)->Sel([lt(test.t.b, 1)])->Sort + Limit(1) + Offset(0))->Sort + Limit(1) + Offset(0)->*plan.Insert",
		},
		// Test simple insert.
		{
			sql:  "insert into t values(0,0,0,0,0,0,0)",
			best: "*plan.Insert",
		},
		// Test dual.
		{
			sql:  "select 1",
			best: "Dual->Projection",
		},
		{
			sql:  "select * from t where false",
			best: "Dual",
		},
		// Test show.
		{
			sql:  "show tables",
			best: "*plan.Show",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := plan.MockResolve(stmt)
		c.Assert(err, IsNil)
		p, err := plan.Optimize(se, stmt, is)
		c.Assert(err, IsNil)
		c.Assert(plan.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderUnion(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)

	plan.UseDAGPlanBuilder = true
	defer func() {
		plan.UseDAGPlanBuilder = false
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		// Test simple union.
		{
			sql:  "select * from t union all select * from t",
			best: "UnionAll{TableReader(Table(t))->TableReader(Table(t))}",
		},
		// Test Order by + Union.
		{
			sql:  "select * from t union all (select * from t) order by a ",
			best: "UnionAll{TableReader(Table(t))->TableReader(Table(t))}->Sort",
		},
		// Test Limit + Union.
		{
			sql:  "select * from t union all (select * from t) limit 1",
			best: "UnionAll{TableReader(Table(t)->Limit)->Limit->TableReader(Table(t)->Limit)->Limit}->Limit",
		},
		// Test TopN + Union.
		{
			sql:  "select a from t union all (select c from t) order by a limit 1",
			best: "UnionAll{TableReader(Table(t)->Limit)->Limit->IndexReader(Index(t.c_d_e)[[<nil>,+inf]]->Limit)->Limit}->Sort + Limit(1) + Offset(0)",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := plan.MockResolve(stmt)
		c.Assert(err, IsNil)
		p, err := plan.Optimize(se, stmt, is)
		c.Assert(err, IsNil)
		c.Assert(plan.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderUnionScan(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)

	plan.UseDAGPlanBuilder = true
	defer func() {
		plan.UseDAGPlanBuilder = false
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		// Read table.
		{
			sql:  "select * from t",
			best: "TableReader(Table(t))->UnionScan([])",
		},
		{
			sql:  "select * from t where b = 1",
			best: "TableReader(Table(t)->Sel([eq(test.t.b, 1)]))->UnionScan([eq(test.t.b, 1)])",
		},
		{
			sql:  "select * from t where a = 1",
			best: "TableReader(Table(t))->UnionScan([eq(test.t.a, 1)])",
		},
		{
			sql:  "select * from t where a = 1 order by a",
			best: "TableReader(Table(t))->UnionScan([eq(test.t.a, 1)])",
		},
		{
			sql:  "select * from t where a = 1 order by b",
			best: "TableReader(Table(t))->UnionScan([eq(test.t.a, 1)])->Sort",
		},
		{
			sql:  "select * from t where a = 1 limit 1",
			best: "TableReader(Table(t))->UnionScan([eq(test.t.a, 1)])->Limit",
		},
		{
			sql:  "select * from t where c = 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]], Table(t))->UnionScan([eq(test.t.c, 1)])",
		},
		{
			sql:  "select c from t where c = 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->UnionScan([eq(test.t.c, 1)])",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := plan.MockResolve(stmt)
		c.Assert(err, IsNil)

		err = se.NewTxn()
		c.Assert(err, IsNil)
		// Make txn not read only.
		se.Txn().Set(nil, nil)
		p, err := plan.Optimize(se, stmt, is)
		c.Assert(err, IsNil)
		c.Assert(plan.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderAgg(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)

	plan.UseDAGPlanBuilder = true
	defer func() {
		plan.UseDAGPlanBuilder = false
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		// Test agg + table.
		{
			sql:  "select sum(a), avg(b + c) from t group by d",
			best: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
		// Test agg + index single.
		{
			sql:  "select sum(e), avg(e + c) from t where c = 1 group by d",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->HashAgg)->HashAgg",
		},
		// Test agg + index double.
		{
			sql:  "select sum(e), avg(b + c) from t where c = 1 and e = 1 group by d",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t)->HashAgg)->HashAgg",
		},
		// Test agg + order.
		{
			sql:  "select sum(e) as k, avg(b + c) from t where c = 1 and b = 1 and e = 1 group by d order by k",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t)->Sel([eq(test.t.b, 1)])->HashAgg)->HashAgg->Sort",
		},
		// Test agg can't push down.
		{
			sql:  "select sum(to_base64(e)) from t where c = 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->HashAgg",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := plan.MockResolve(stmt)
		c.Assert(err, IsNil)
		p, err := plan.Optimize(se, stmt, is)
		c.Assert(err, IsNil)
		c.Assert(plan.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}
