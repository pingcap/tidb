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

	defer func() {
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
		// FIXME: Some bugs still exist in selectivity.
		//{
		//	sql:  "select * from t where (t.c > 0 and t.c < 1) or (t.c > 2 and t.c < 3) or (t.c > 4 and t.c < 5) or (t.c > 6 and t.c < 7) or (t.c > 9 and t.c < 10)",
		//	best: "IndexLookUp(Index(t.c_d_e)[(0 +inf,1 <nil>) (2 +inf,3 <nil>) (4 +inf,5 <nil>) (6 +inf,7 <nil>) (9 +inf,10 <nil>)], Table(t))",
		//},
		// Test TopN to table branch in double read.
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 order by t.b limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t)->TopN([test.t.b],0,1))->TopN([test.t.b],0,1)",
		},
		// Test TopN to index branch in double read.
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 order by t.e limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)])->TopN([test.t.e],0,1), Table(t))->TopN([test.t.e],0,1)",
		},
		// Test TopN to Limit in double read.
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 order by t.d limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)])->Limit, Table(t))->Limit",
		},
		// Test TopN to Limit in index single read.
		{
			sql:  "select c from t where t.c = 1 and t.e = 1 order by t.d limit 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)])->Limit)->Limit->Projection",
		},
		// Test TopN to Limit in table single read.
		{
			sql:  "select c from t order by t.a limit 1",
			best: "TableReader(Table(t)->Limit)->Limit->Projection",
		},
		// Test TopN push down in table single read.
		{
			sql:  "select c from t order by t.a + t.b limit 1",
			best: "TableReader(Table(t)->TopN([plus(test.t.a, test.t.b)],0,1))->TopN([plus(test.t.a, test.t.b)],0,1)->Projection",
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
			best: "IndexLookUp(Index(t.e_d_c_str_prefix)[[1,1]], Table(t))->Sort->Projection",
		},
		// Test PK in index single read.
		{
			sql:  "select c from t where t.c = 1 and t.a = 1 order by t.d limit 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.a, 1)])->Limit)->Limit->Projection",
		},
		// Test composed index.
		// FIXME: The TopN didn't be pushed.
		{
			sql:  "select c from t where t.c = 1 and t.d = 1 order by t.a limit 1",
			best: "IndexReader(Index(t.c_d_e)[[1 1,1 1]])->TopN([test.t.a],0,1)->Projection",
		},
		// Test PK in index double read.
		{
			sql:  "select * from t where t.c = 1 and t.a = 1 order by t.d limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.a, 1)])->Limit, Table(t))->Limit",
		},
		// Test index filter condition push down.
		{
			sql:  "select * from t use index(e_d_c_str_prefix) where t.c_str = 'abcdefghijk' and t.d_str = 'd' and t.e_str = 'e'",
			best: "IndexLookUp(Index(t.e_d_c_str_prefix)[[e d [97 98 99 100 101 102 103 104 105 106],e d [97 98 99 100 101 102 103 104 105 106]]], Table(t)->Sel([eq(test.t.c_str, abcdefghijk)]))",
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

	defer func() {
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.c_str",
			best: "LeftHashJoin{TableReader(Table(t))->Projection->TableReader(Table(t))->Projection}(cast(t1.a),cast(t2.c_str))->Projection",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.a",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.b,t2.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a join t t3 on t1.a = t3.a",
			best: "MergeJoin{MergeJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t1.a,t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a join t t3 on t1.b = t3.a",
			best: "LeftHashJoin{MergeJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t1.b,t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.a order by t1.a",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.b,t2.a)->Sort",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.a order by t1.a limit 1",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.b,t2.a)->Limit",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.a and t1.c = 1 and t1.d = 1 and t1.e = 1 order by t1.a limit 1",
			best: "IndexJoin{IndexLookUp(Index(t.c_d_e)[[1 1 1,1 1 1]], Table(t))->TableReader(Table(t))}(t1.b,t2.a)->TopN([t1.a],0,1)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.b join t t3 on t1.b = t3.b",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.b,t2.b)->TableReader(Table(t))}(t1.b,t3.b)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a order by t1.a",
			best: "MergeJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)",
		},
		{
			sql:  "select * from t t1 left outer join t t2 on t1.a = t2.a right outer join t t3 on t1.a = t3.a",
			best: "MergeJoin{MergeJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t1.a,t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a join t t3 on t1.a = t3.a and t1.b = 1 and t3.c = 1",
			best: "LeftHashJoin{IndexJoin{TableReader(Table(t)->Sel([eq(t1.b, 1)]))->TableReader(Table(t))}(t1.a,t2.a)->IndexLookUp(Index(t.c_d_e)[[1,1]], Table(t))}(t1.a,t3.a)",
		},
		{
			sql:  "select * from t where t.c in (select b from t s where s.a = t.a)",
			best: "SemiJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,s.a)(test.t.c,s.b)",
		},
		{
			sql:  "select t.c in (select b from t s where s.a = t.a) from t",
			best: "SemiJoinWithAux{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,s.a)(test.t.c,s.b)->Projection",
		},
		// Test Single Merge Join.
		// Merge Join will no longer enforce a sort. If a hint doesn't take effect, we will choose other types of join.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.b",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.b)",
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
		// Test Multi Merge Join with multi keys.
		// TODO: More tests should be added.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1, t t2, t t3 where t1.c = t2.c and t1.d = t2.d and t3.c = t1.c and t3.d = t1.d",
			best: "MergeJoin{MergeJoin{IndexLookUp(Index(t.c_d_e)[[<nil>,+inf]], Table(t))->IndexLookUp(Index(t.c_d_e)[[<nil>,+inf]], Table(t))}(t1.c,t2.c)(t1.d,t2.d)->IndexLookUp(Index(t.c_d_e)[[<nil>,+inf]], Table(t))}(t1.c,t3.c)(t1.d,t3.d)",
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
		// Test Index Join + TableScan.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1, t t2 where t1.a = t2.a",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)",
		},
		// Test Index Join + DoubleRead.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1, t t2 where t1.a = t2.c",
			best: "IndexJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[<nil>,+inf]], Table(t))}(t1.a,t2.c)",
		},
		// Test Index Join + SingleRead.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ t1.a , t2.a from t t1, t t2 where t1.a = t2.c",
			best: "IndexJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[<nil>,+inf]])}(t1.a,t2.c)->Projection",
		},
		// Test Index Join + Order by.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ t1.a, t2.a from t t1, t t2 where t1.a = t2.a order by t1.c",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->Sort->Projection",
		},
		// Test Index Join + Order by.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ t1.a, t2.a from t t1, t t2 where t1.a = t2.a order by t2.c",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->Sort->Projection",
		},
		// Test Index Join + TableScan + Rotate.
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ t1.a , t2.a from t t1, t t2 where t1.a = t2.c",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t2.c,t1.a)->Projection",
		},
		// Test Index Join + OuterJoin + TableScan.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 left outer join t t2 on t1.a = t2.a and t2.b < 1",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t)->Sel([lt(t2.b, 1)]))}(t1.a,t2.a)",
		},
		// Test Index Join failed.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 left outer join t t2 on t1.a = t2.b",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.b)",
		},
		// Test Index Join failed.
		{
			sql:  "select /*+ TIDB_INLJ(t1) */ * from t t1 right outer join t t2 on t1.a = t2.b",
			best: "RightHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.b)",
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

func (s *testPlanSuite) TestDAGPlanBuilderSubquery(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)

	defer func() {
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		// Test join key with cast.
		{
			sql:  "select * from t where exists (select s.a from t s having sum(s.a) = t.a )",
			best: "SemiJoin{TableReader(Table(t))->Projection->TableReader(Table(t)->HashAgg)->HashAgg}(cast(test.t.a),sel_agg_1)->Projection",
		},
		{
			sql:  "select * from t where exists (select s.a from t s having sum(s.a) = t.a ) order by t.a",
			best: "SemiJoin{TableReader(Table(t))->Projection->TableReader(Table(t)->HashAgg)->HashAgg}(cast(test.t.a),sel_agg_1)->Projection",
		},
		// FIXME: Report error by resolver.
		//{
		//	sql:  "select * from t where exists (select s.a from t s having s.a = t.a ) order by t.a",
		//	best: "SemiJoin{TableReader(Table(t))->Projection->TableReader(Table(t)->HashAgg)->HashAgg}(cast(test.t.a),sel_agg_1)->Projection->Sort",
		//},
		{
			sql:  "select * from t where a in (select s.a from t s) order by t.a",
			best: "SemiJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,s.a)",
		},
		{
			// Test Nested sub query.
			sql:  "select * from t where exists (select s.a from t s where s.c in (select c from t as k where k.d = s.d) having sum(s.a) = t.a )",
			best: "SemiJoin{TableReader(Table(t))->Projection->SemiJoin{TableReader(Table(t))->TableReader(Table(t))}(s.d,k.d)(s.c,k.c)->HashAgg}(cast(test.t.a),sel_agg_1)->Projection",
		},
		// Test Apply.
		{
			sql:  "select t.c in (select count(*) from t s , t t1 where s.a = t.a and s.a = t1.a) from t",
			best: "Apply{TableReader(Table(t))->MergeJoin{TableReader(Table(t))->Sel([eq(s.a, test.t.a)])->TableReader(Table(t))}(s.a,t1.a)->HashAgg}->Projection",
		},
		{
			sql:  "select (select count(*) from t s , t t1 where s.a = t.a and s.a = t1.a) from t",
			best: "Apply{TableReader(Table(t))->MergeJoin{TableReader(Table(t))->Sel([eq(s.a, test.t.a)])->TableReader(Table(t))}(s.a,t1.a)->HashAgg}->Projection",
		},
		{
			sql:  "select (select count(*) from t s , t t1 where s.a = t.a and s.a = t1.a) from t order by t.a",
			best: "Apply{TableReader(Table(t))->MergeJoin{TableReader(Table(t))->Sel([eq(s.a, test.t.a)])->TableReader(Table(t))}(s.a,t1.a)->HashAgg}->Projection",
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

func (s *testPlanSuite) TestDAGPlanTopN(c *C) {
	store, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer store.Close()
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)

	defer func() {
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t t1 left join t t2 on t1.b = t2.b left join t t3 on t2.b = t3.b order by t1.a limit 1",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t)->Limit)->TableReader(Table(t))}(t1.b,t2.b)->TableReader(Table(t))}(t2.b,t3.b)->TopN([t1.a],0,1)",
		},
		{
			sql:  "select * from t t1 left join t t2 on t1.b = t2.b left join t t3 on t2.b = t3.b order by t1.b limit 1",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t)->TopN([t1.b],0,1))->TableReader(Table(t))}(t1.b,t2.b)->TableReader(Table(t))}(t2.b,t3.b)->TopN([t1.b],0,1)",
		},
		{
			sql:  "select * from t t1 left join t t2 on t1.b = t2.b left join t t3 on t2.b = t3.b limit 1",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t)->Limit)->TableReader(Table(t))}(t1.b,t2.b)->TableReader(Table(t))}(t2.b,t3.b)->Limit",
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

	defer func() {
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
			best: "TableReader(Table(t))->Lock->TopN([test.t.b],0,1)",
		},
		// Test complex update.
		{
			sql:  "update t set a = 5 where b < 1 order by d limit 1",
			best: "TableReader(Table(t)->Sel([lt(test.t.b, 1)])->TopN([test.t.d],0,1))->TopN([test.t.d],0,1)->*plan.Update",
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
			best: "TableReader(Table(t)->Sel([lt(test.t.b, 1)])->TopN([test.t.d],0,1))->TopN([test.t.d],0,1)->*plan.Delete",
		},
		// Test simple delete.
		{
			sql:  "delete from t",
			best: "TableReader(Table(t))->*plan.Delete",
		},
		// Test complex insert.
		{
			sql:  "insert into t select * from t where b < 1 order by d limit 1",
			best: "TableReader(Table(t)->Sel([lt(test.t.b, 1)])->TopN([test.t.d],0,1))->TopN([test.t.d],0,1)->*plan.Insert",
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

	defer func() {
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
			best: "UnionAll{TableReader(Table(t)->Limit)->TableReader(Table(t)->Limit)}->Limit",
		},
		// Test TopN + Union.
		{
			sql:  "select a from t union all (select c from t) order by a limit 1",
			best: "UnionAll{TableReader(Table(t)->Limit)->IndexReader(Index(t.c_d_e)[[<nil>,+inf]]->Limit)}->TopN([a],0,1)",
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

	defer func() {
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

	defer func() {
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
		{
			sql:  "select sum(distinct a), avg(b + c) from t group by d",
			best: "TableReader(Table(t))->HashAgg",
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
