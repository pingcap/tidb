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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testPlanSuite{})

type testPlanSuite struct {
	*parser.Parser

	is infoschema.InfoSchema
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{core.MockTable()})
	s.Parser = parser.New()
}

func (s *testPlanSuite) TestDAGPlanBuilderSimpleCase(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	tests := []struct {
		sql  string
		best string
	}{
		// Test index hint.
		{
			sql:  "select * from t t1 use index(c_d_e)",
			best: "IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))",
		},
		// Test ts + Sort vs. DoubleRead + filter.
		{
			sql:  "select a from t where a between 1 and 2 order by c",
			best: "TableReader(Table(t))->Sort->Projection",
		},
		// Test DNF condition + Double Read.
		{
			sql:  "select * from t where (t.c > 0 and t.c < 2) or (t.c > 4 and t.c < 6) or (t.c > 8 and t.c < 10) or (t.c > 12 and t.c < 14) or (t.c > 16 and t.c < 18)",
			best: "IndexLookUp(Index(t.c_d_e)[(0,2) (4,6) (8,10) (12,14) (16,18)], Table(t))",
		},
		{
			sql:  "select * from t where (t.c > 0 and t.c < 1) or (t.c > 2 and t.c < 3) or (t.c > 4 and t.c < 5) or (t.c > 6 and t.c < 7) or (t.c > 9 and t.c < 10)",
			best: "Dual",
		},
		// Test TopN to table branch in double read.
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 order by t.b limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t))->TopN([test.t.b],0,1)",
		},
		// Test Null Range
		{
			sql:  "select * from t where t.e_str is null",
			best: "IndexLookUp(Index(t.e_d_c_str_prefix)[[NULL,NULL]], Table(t))",
		},
		// Test Null Range but the column has not null flag.
		{
			sql:  "select * from t where t.c is null",
			best: "Dual",
		},
		// Test TopN to index branch in double read.
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 order by t.e limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t))->TopN([test.t.e],0,1)",
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
			best: "IndexReader(Index(t.c_d_e)[[NULL,+inf]])",
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
			best: `IndexLookUp(Index(t.e_d_c_str_prefix)[["1","1"]], Table(t))->Sort->Projection`,
		},
		// Test PK in index single read.
		{
			sql:  "select c from t where t.c = 1 and t.a > 1 order by t.d limit 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->Sel([gt(test.t.a, 1)])->Limit)->Limit->Projection",
		},
		// Test composed index.
		// FIXME: The TopN didn't be pushed.
		{
			sql:  "select c from t where t.c = 1 and t.d = 1 order by t.a limit 1",
			best: "IndexReader(Index(t.c_d_e)[[1 1,1 1]])->TopN([test.t.a],0,1)->Projection",
		},
		// Test PK in index double read.
		{
			sql:  "select * from t where t.c = 1 and t.a > 1 order by t.d limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([gt(test.t.a, 1)])->Limit, Table(t))->Limit",
		},
		// Test index filter condition push down.
		{
			sql:  "select * from t use index(e_d_c_str_prefix) where t.c_str = 'abcdefghijk' and t.d_str = 'd' and t.e_str = 'e'",
			best: "IndexLookUp(Index(t.e_d_c_str_prefix)[[\"e\" \"d\" \"abcdefghij\",\"e\" \"d\" \"abcdefghij\"]], Table(t)->Sel([eq(test.t.c_str, abcdefghijk)]))",
		},
		{
			sql:  "select * from t use index(e_d_c_str_prefix) where t.e_str = b'1110000'",
			best: "IndexLookUp(Index(t.e_d_c_str_prefix)[[\"p\",\"p\"]], Table(t))",
		},
		{
			sql:  "select * from (select * from t use index() order by b) t left join t t1 on t.a=t1.a limit 10",
			best: "IndexJoin{TableReader(Table(t)->TopN([test.t.b],0,10))->TopN([test.t.b],0,10)->TableReader(Table(t))}(test.t.a,t1.a)->Limit",
		},
		// Test embedded ORDER BY which imposes on different number of columns than outer query.
		{
			sql:  "select * from ((SELECT 1 a,3 b) UNION (SELECT 2,1) ORDER BY (SELECT 2)) t order by a,b",
			best: "UnionAll{Dual->Projection->Dual->Projection}->HashAgg->Sort",
		},
		{
			sql:  "select * from ((SELECT 1 a,6 b) UNION (SELECT 2,5) UNION (SELECT 2, 4) ORDER BY 1) t order by 1, 2",
			best: "UnionAll{Dual->Projection->Dual->Projection->Dual->Projection}->HashAgg->Sort->Sort",
		},
	}
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderJoin(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

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
			best: "MergeInnerJoin{MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t1.a,t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a join t t3 on t1.b = t3.a",
			best: "LeftHashJoin{MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t1.b,t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.a order by t1.a",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.b,t2.a)->Sort",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.a order by t1.a limit 1",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.b,t2.a)->Limit",
		},
		// Test hash join's hint.
		{
			sql:  "select /*+ TIDB_HJ(t1, t2) */ * from t t1 join t t2 on t1.b = t2.a order by t1.a limit 1",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.b,t2.a)->TopN([t1.a],0,1)",
		},
		{
			sql:  "select * from t t1 left join t t2 on t1.b = t2.a where 1 = 1 limit 1",
			best: "IndexJoin{TableReader(Table(t)->Limit)->Limit->TableReader(Table(t))}(t1.b,t2.a)->Limit",
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
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)",
		},
		{
			sql:  "select * from t t1 left outer join t t2 on t1.a = t2.a right outer join t t3 on t1.a = t3.a",
			best: "MergeRightOuterJoin{MergeLeftOuterJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t1.a,t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a join t t3 on t1.a = t3.a and t1.b = 1 and t3.c = 1",
			best: "IndexJoin{IndexJoin{TableReader(Table(t)->Sel([eq(t1.b, 1)]))->IndexLookUp(Index(t.c_d_e)[[1,1]], Table(t))}(t3.a,t1.a)->TableReader(Table(t))}(t1.a,t2.a)->Projection",
		},
		{
			sql:  "select * from t where t.c in (select b from t s where s.a = t.a)",
			best: "MergeSemiJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,s.a)",
		},
		{
			sql:  "select t.c in (select b from t s where s.a = t.a) from t",
			best: "MergeLeftOuterSemiJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,s.a)->Projection",
		},
		// Test Single Merge Join.
		// Merge Join now enforce a sort.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.b",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))->Sort}(t1.a,t2.b)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.a",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)",
		},
		// Test Single Merge Join + Sort.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.a order by t2.a",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.b = t2.b order by t2.a",
			best: "MergeInnerJoin{TableReader(Table(t))->Sort->TableReader(Table(t))->Sort}(t1.b,t2.b)->Sort",
		},
		// Test Single Merge Join + Sort + desc.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.a order by t2.a desc",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.b = t2.b order by t2.b desc",
			best: "MergeInnerJoin{TableReader(Table(t))->Sort->TableReader(Table(t))->Sort}(t1.b,t2.b)",
		},
		// Test Multi Merge Join.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1, t t2, t t3 where t1.a = t2.a and t2.a = t3.a",
			best: "MergeInnerJoin{MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t2.a,t3.a)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1, t t2, t t3 where t1.a = t2.b and t2.a = t3.b",
			best: "MergeInnerJoin{MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))->Sort}(t1.a,t2.b)->Sort->TableReader(Table(t))->Sort}(t2.a,t3.b)",
		},
		// Test Multi Merge Join with multi keys.
		// TODO: More tests should be added.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1, t t2, t t3 where t1.c = t2.c and t1.d = t2.d and t3.c = t1.c and t3.d = t1.d",
			best: "MergeInnerJoin{MergeInnerJoin{IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(t1.c,t2.c)(t1.d,t2.d)->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(t1.c,t3.c)(t1.d,t3.d)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1, t t2, t t3 where t1.c = t2.c and t1.d = t2.d and t3.c = t1.c and t3.d = t1.d order by t1.c",
			best: "MergeInnerJoin{MergeInnerJoin{IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(t1.c,t2.c)(t1.d,t2.d)->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(t1.c,t3.c)(t1.d,t3.d)",
		},
		// Test Multi Merge Join + Outer Join.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1 left outer join t t2 on t1.a = t2.a left outer join t t3 on t2.a = t3.a",
			best: "MergeLeftOuterJoin{MergeLeftOuterJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t2.a,t3.a)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1 left outer join t t2 on t1.a = t2.a left outer join t t3 on t1.a = t3.a",
			best: "MergeLeftOuterJoin{MergeLeftOuterJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->TableReader(Table(t))}(t1.a,t3.a)",
		},
		// Test Index Join + TableScan.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1, t t2 where t1.a = t2.a",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)",
		},
		// Test Index Join + DoubleRead.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1, t t2 where t1.a = t2.c",
			best: "IndexJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(t1.a,t2.c)",
		},
		// Test Index Join + SingleRead.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ t1.a , t2.a from t t1, t t2 where t1.a = t2.c",
			best: "IndexJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(t1.a,t2.c)->Projection",
		},
		// Test Index Join + Order by.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ t1.a, t2.a from t t1, t t2 where t1.a = t2.a order by t1.c",
			best: "IndexJoin{IndexReader(Index(t.c_d_e)[[NULL,+inf]])->TableReader(Table(t))}(t1.a,t2.a)->Projection",
		},
		// Test Index Join + Order by.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ t1.a, t2.a from t t1, t t2 where t1.a = t2.a order by t2.c",
			best: "IndexJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(t2.a,t1.a)->Projection",
		},
		// Test Index Join + TableScan + Rotate.
		{
			sql:  "select /*+ TIDB_INLJ(t1) */ t1.a , t2.a from t t1, t t2 where t1.a = t2.c",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t2.c,t1.a)->Projection",
		},
		// Test Index Join + OuterJoin + TableScan.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 left outer join t t2 on t1.a = t2.a and t2.b < 1",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t)->Sel([lt(t2.b, 1)]))}(t1.a,t2.a)",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 join t t2 on t1.d=t2.d and t2.c = 1",
			best: "IndexJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(t1.d,t2.d)",
		},
		// Test Index Join failed.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 left outer join t t2 on t1.a = t2.b",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.b)",
		},
		// Test Index Join failed.
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 right outer join t t2 on t1.a = t2.b",
			best: "RightHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.b)",
		},
		// Test Semi Join hint success.
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 where t1.a in (select a from t t2)",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->Projection",
		},
		// Test Semi Join hint fail.
		{
			sql:  "select /*+ TIDB_INLJ(t1) */ * from t t1 where t1.a in (select a from t t2)",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t2.a,t1.a)->Projection",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.c=t2.c and t1.f=t2.f",
			best: "IndexJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(t1.c,t2.c)",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.a = t2.a and t1.f=t2.f",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.f=t2.f and t1.a=t2.a",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.a=t2.a and t2.a in (1, 2)",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t)->Sel([in(t2.a, 1, 2)]))}(t1.a,t2.a)",
		},
	}
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderSubquery(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	tests := []struct {
		sql  string
		best string
	}{
		// Test join key with cast.
		{
			sql:  "select * from t where exists (select s.a from t s having sum(s.a) = t.a )",
			best: "LeftHashJoin{TableReader(Table(t))->Projection->TableReader(Table(t)->StreamAgg)->StreamAgg}(cast(test.t.a),sel_agg_1)->Projection",
		},
		{
			sql:  "select * from t where exists (select s.a from t s having sum(s.a) = t.a ) order by t.a",
			best: "LeftHashJoin{TableReader(Table(t))->Projection->TableReader(Table(t)->StreamAgg)->StreamAgg}(cast(test.t.a),sel_agg_1)->Projection->Sort",
		},
		// FIXME: Report error by resolver.
		//{
		//	sql:  "select * from t where exists (select s.a from t s having s.a = t.a ) order by t.a",
		//	best: "SemiJoin{TableReader(Table(t))->Projection->TableReader(Table(t)->HashAgg)->HashAgg}(cast(test.t.a),sel_agg_1)->Projection->Sort",
		//},
		{
			sql:  "select * from t where a in (select s.a from t s) order by t.a",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,s.a)->Projection",
		},
		// Test Nested sub query.
		{
			sql:  "select * from t where exists (select s.a from t s where s.c in (select c from t as k where k.d = s.d) having sum(s.a) = t.a )",
			best: "LeftHashJoin{TableReader(Table(t))->Projection->MergeSemiJoin{IndexReader(Index(t.c_d_e)[[NULL,+inf]])->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(s.c,k.c)(s.d,k.d)->StreamAgg}(cast(test.t.a),sel_agg_1)->Projection",
		},
		// Test Semi Join + Order by.
		{
			sql:  "select * from t where a in (select a from t) order by b",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,test.t.a)->Projection->Sort",
		},
		// Test Apply.
		{
			sql:  "select t.c in (select count(*) from t s , t t1 where s.a = t.a and s.a = t1.a) from t",
			best: "Apply{TableReader(Table(t))->IndexJoin{TableReader(Table(t))->TableReader(Table(t)->Sel([eq(t1.a, test.t.a)]))}(s.a,t1.a)->StreamAgg}->Projection",
		},
		{
			sql:  "select (select count(*) from t s , t t1 where s.a = t.a and s.a = t1.a) from t",
			best: "LeftHashJoin{TableReader(Table(t))->MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(s.a,t1.a)->StreamAgg}(test.t.a,s.a)->Projection->Projection",
		},
		{
			sql:  "select (select count(*) from t s , t t1 where s.a = t.a and s.a = t1.a) from t order by t.a",
			best: "LeftHashJoin{TableReader(Table(t))->MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(s.a,t1.a)->StreamAgg}(test.t.a,s.a)->Projection->Sort->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestDAGPlanTopN(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t t1 left join t t2 on t1.b = t2.b left join t t3 on t2.b = t3.b order by t1.a limit 1",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t)->Limit)->Limit->TableReader(Table(t))}(t1.b,t2.b)->TopN([t1.a],0,1)->TableReader(Table(t))}(t2.b,t3.b)->TopN([t1.a],0,1)",
		},
		{
			sql:  "select * from t t1 left join t t2 on t1.b = t2.b left join t t3 on t2.b = t3.b order by t1.b limit 1",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t)->TopN([t1.b],0,1))->TopN([t1.b],0,1)->TableReader(Table(t))}(t1.b,t2.b)->TopN([t1.b],0,1)->TableReader(Table(t))}(t2.b,t3.b)->TopN([t1.b],0,1)",
		},
		{
			sql:  "select * from t t1 left join t t2 on t1.b = t2.b left join t t3 on t2.b = t3.b limit 1",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t)->Limit)->Limit->TableReader(Table(t))}(t1.b,t2.b)->Limit->TableReader(Table(t))}(t2.b,t3.b)->Limit",
		},
		{
			sql:  "select * from t where b = 1 and c = 1 order by c limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]], Table(t)->Sel([eq(test.t.b, 1)]))->Limit",
		},
		{
			sql:  "select * from t where c = 1 order by c limit 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Limit, Table(t))->Limit",
		},
		{
			sql:  "select * from t order by a limit 1",
			best: "TableReader(Table(t)->Limit)->Limit",
		},
		{
			sql:  "select c from t order by c limit 1",
			best: "IndexReader(Index(t.c_d_e)[[NULL,+inf]]->Limit)->Limit",
		},
	}
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderBasePhysicalPlan(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)

	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

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
			best: "TableReader(Table(t)->Sel([lt(test.t.b, 1)])->TopN([test.t.d],0,1))->TopN([test.t.d],0,1)->Update",
		},
		// Test simple update.
		{
			sql:  "update t set a = 5",
			best: "TableReader(Table(t))->Update",
		},
		// TODO: Test delete/update with join.
		// Test join hint for delete and update
		{
			sql:  "delete /*+ TIDB_INLJ(t1, t2) */ t1 from t t1, t t2 where t1.c=t2.c",
			best: "IndexJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(t1.c,t2.c)->Delete",
		},
		{
			sql:  "delete /*+ TIDB_SMJ(t1, t2) */ from t1 using t t1, t t2 where t1.c=t2.c",
			best: "MergeInnerJoin{IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(t1.c,t2.c)->Delete",
		},
		{
			sql:  "update /*+ TIDB_SMJ(t1, t2) */ t t1, t t2 set t1.a=1, t2.a=1 where t1.a=t2.a",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->Update",
		},
		{
			sql:  "update /*+ TIDB_HJ(t1, t2) */ t t1, t t2 set t1.a=1, t2.a=1 where t1.a=t2.a",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->Update",
		},
		// Test complex delete.
		{
			sql:  "delete from t where b < 1 order by d limit 1",
			best: "TableReader(Table(t)->Sel([lt(test.t.b, 1)])->TopN([test.t.d],0,1))->TopN([test.t.d],0,1)->Delete",
		},
		// Test simple delete.
		{
			sql:  "delete from t",
			best: "TableReader(Table(t))->Delete",
		},
		// Test "USE INDEX" hint in delete statement from single table
		{
			sql:  "delete from t use index(c_d_e) where b = 1",
			best: "IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t)->Sel([eq(test.t.b, 1)]))->Delete",
		},
		// Test complex insert.
		{
			sql:  "insert into t select * from t where b < 1 order by d limit 1",
			best: "TableReader(Table(t)->Sel([lt(test.t.b, 1)])->TopN([test.t.d],0,1))->TopN([test.t.d],0,1)->Insert",
		},
		// Test simple insert.
		{
			sql:  "insert into t (a, b, c, e, f, g) values(0,0,0,0,0,0)",
			best: "Insert",
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
			best: "Show",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		core.Preprocess(se, stmt, s.is, false)
		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderUnion(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

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
			best: "UnionAll{TableReader(Table(t)->Limit)->Limit->IndexReader(Index(t.c_d_e)[[NULL,+inf]]->Limit)->Limit}->TopN([a],0,1)",
		},
	}
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderUnionScan(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

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
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->UnionScan([eq(test.t.c, 1)])->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		// Make txn not read only.
		txn, err := se.Txn(true)
		c.Assert(err, IsNil)
		txn.Set(kv.Key("AAA"), []byte("BBB"))
		c.Assert(se.StmtCommit(), IsNil)
		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderAgg(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	se.Execute(context.Background(), "use test")
	se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	c.Assert(err, IsNil)

	tests := []struct {
		sql  string
		best string
	}{
		// Test distinct.
		{
			sql:  "select distinct b from t",
			best: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select count(*) from (select * from t order by b) t group by b",
			best: "TableReader(Table(t))->Sort->StreamAgg",
		},
		{
			sql:  "select count(*), x from (select b as bbb, a + 1 as x from (select * from t order by b) t) t group by bbb",
			best: "TableReader(Table(t))->Sort->Projection->StreamAgg",
		},
		// Test agg + table.
		{
			sql:  "select sum(a), avg(b + c) from t group by d",
			best: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
		{
			sql:  "select sum(distinct a), avg(b + c) from t group by d",
			best: "TableReader(Table(t))->HashAgg",
		},
		//  Test group by (c + d)
		{
			sql:  "select sum(e), avg(e + c) from t where c = 1 group by (c + d)",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->HashAgg)->HashAgg",
		},
		// Test stream agg + index single.
		{
			sql:  "select sum(e), avg(e + c) from t where c = 1 group by c",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->StreamAgg)->StreamAgg",
		},
		// Test hash agg + index single.
		{
			sql:  "select sum(e), avg(e + c) from t where c = 1 group by d",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->HashAgg)->HashAgg",
		},
		// Test hash agg + index double.
		{
			sql:  "select sum(e), avg(b + c) from t where c = 1 and e = 1 group by d",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t))->HashAgg",
		},
		// Test stream agg + index double.
		{
			sql:  "select sum(e), avg(b + c) from t where c = 1 and b = 1 group by c",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]], Table(t)->Sel([eq(test.t.b, 1)]))->StreamAgg",
		},
		// Test hash agg + order.
		{
			sql:  "select sum(e) as k, avg(b + c) from t where c = 1 and b = 1 and e = 1 group by d order by k",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t)->Sel([eq(test.t.b, 1)]))->HashAgg->Sort",
		},
		// Test stream agg + order.
		{
			sql:  "select sum(e) as k, avg(b + c) from t where c = 1 and b = 1 and e = 1 group by c order by k",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t)->Sel([eq(test.t.b, 1)]))->StreamAgg->Sort",
		},
		// Test agg can't push down.
		{
			sql:  "select sum(to_base64(e)) from t where c = 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->StreamAgg",
		},
		{
			sql:  "select (select count(1) k from t s where s.a = t.a having k != 0) from t",
			best: "MergeLeftOuterJoin{TableReader(Table(t))->TableReader(Table(t))->Projection}(test.t.a,s.a)->Projection->Projection",
		},
		// Test stream agg with multi group by columns.
		{
			sql:  "select sum(to_base64(e)) from t group by e,d,c order by c",
			best: "IndexReader(Index(t.c_d_e)[[NULL,+inf]])->StreamAgg->Projection",
		},
		{
			sql:  "select sum(e+1) from t group by e,d,c order by c",
			best: "IndexReader(Index(t.c_d_e)[[NULL,+inf]]->StreamAgg)->StreamAgg->Projection",
		},
		{
			sql:  "select sum(to_base64(e)) from t group by e,d,c order by c,e",
			best: "IndexReader(Index(t.c_d_e)[[NULL,+inf]])->StreamAgg->Sort->Projection",
		},
		{
			sql:  "select sum(e+1) from t group by e,d,c order by c,e",
			best: "IndexReader(Index(t.c_d_e)[[NULL,+inf]]->StreamAgg)->StreamAgg->Sort->Projection",
		},
		// Test stream agg + limit or sort
		{
			sql:  "select count(*) from t group by g order by g limit 10",
			best: "IndexReader(Index(t.g)[[NULL,+inf]]->StreamAgg)->StreamAgg->Limit->Projection",
		},
		{
			sql:  "select count(*) from t group by g limit 10",
			best: "IndexReader(Index(t.g)[[NULL,+inf]]->StreamAgg)->StreamAgg->Limit",
		},
		{
			sql:  "select count(*) from t group by g order by g",
			best: "IndexReader(Index(t.g)[[NULL,+inf]]->StreamAgg)->StreamAgg->Projection",
		},
		{
			sql:  "select count(*) from t group by g order by g desc limit 1",
			best: "IndexReader(Index(t.g)[[NULL,+inf]]->StreamAgg)->StreamAgg->Limit->Projection",
		},
		// Test hash agg + limit or sort
		{
			sql:  "select count(*) from t group by b order by b limit 10",
			best: "TableReader(Table(t)->HashAgg)->HashAgg->TopN([test.t.b],0,10)->Projection",
		},
		{
			sql:  "select count(*) from t group by b order by b",
			best: "TableReader(Table(t)->HashAgg)->HashAgg->Sort->Projection",
		},
		{
			sql:  "select count(*) from t group by b limit 10",
			best: "TableReader(Table(t)->HashAgg)->HashAgg->Limit",
		},
		// Test merge join + stream agg
		{
			sql:  "select sum(a.g), sum(b.g) from t a join t b on a.g = b.g group by a.g",
			best: "MergeInnerJoin{IndexReader(Index(t.g)[[NULL,+inf]])->IndexReader(Index(t.g)[[NULL,+inf]])}(a.g,b.g)->StreamAgg",
		},
		// Test index join + stream agg
		{
			sql:  "select /*+ tidb_inlj(a,b) */ sum(a.g), sum(b.g) from t a join t b on a.g = b.g and a.g > 60 group by a.g order by a.g limit 1",
			best: "IndexJoin{IndexReader(Index(t.g)[(60,+inf]])->IndexReader(Index(t.g)[[NULL,+inf]]->Sel([gt(b.g, 60)]))}(a.g,b.g)->StreamAgg->Limit->Projection",
		},
		{
			sql:  "select sum(a.g), sum(b.g) from t a join t b on a.g = b.g and a.a>5 group by a.g order by a.g limit 1",
			best: "IndexJoin{IndexReader(Index(t.g)[[NULL,+inf]]->Sel([gt(a.a, 5)]))->IndexReader(Index(t.g)[[NULL,+inf]])}(a.g,b.g)->StreamAgg->Limit->Projection",
		},
		{
			sql:  "select sum(d) from t",
			best: "TableReader(Table(t)->StreamAgg)->StreamAgg",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestRefine(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select a from t where c is not null",
			best: "IndexReader(Index(t.c_d_e)[[-inf,+inf]])->Projection",
		},
		{
			sql:  "select a from t where c >= 4",
			best: "IndexReader(Index(t.c_d_e)[[4,+inf]])->Projection",
		},
		{
			sql:  "select a from t where c <= 4",
			best: "IndexReader(Index(t.c_d_e)[[-inf,4]])->Projection",
		},
		{
			sql:  "select a from t where c = 4 and d = 5 and e = 6",
			best: "IndexReader(Index(t.c_d_e)[[4 5 6,4 5 6]])->Projection",
		},
		{
			sql:  "select a from t where d = 4 and c = 5",
			best: "IndexReader(Index(t.c_d_e)[[5 4,5 4]])->Projection",
		},
		{
			sql:  "select a from t where c = 4 and e < 5",
			best: "IndexReader(Index(t.c_d_e)[[4,4]]->Sel([lt(test.t.e, 5)]))->Projection",
		},
		{
			sql:  "select a from t where c = 4 and d <= 5 and d > 3",
			best: "IndexReader(Index(t.c_d_e)[(4 3,4 5]])->Projection",
		},
		{
			sql:  "select a from t where d <= 5 and d > 3",
			best: "TableReader(Table(t)->Sel([le(test.t.d, 5) gt(test.t.d, 3)]))->Projection",
		},
		{
			sql:  "select a from t where c between 1 and 2",
			best: "IndexReader(Index(t.c_d_e)[[1,2]])->Projection",
		},
		{
			sql:  "select a from t where c not between 1 and 2",
			best: "IndexReader(Index(t.c_d_e)[[-inf,1) (2,+inf]])->Projection",
		},
		{
			sql:  "select a from t where c <= 5 and c >= 3 and d = 1",
			best: "IndexReader(Index(t.c_d_e)[[3,5]]->Sel([eq(test.t.d, 1)]))->Projection",
		},
		{
			sql:  "select a from t where c = 1 or c = 2 or c = 3",
			best: "IndexReader(Index(t.c_d_e)[[1,3]])->Projection",
		},
		{
			sql:  "select b from t where c = 1 or c = 2 or c = 3 or c = 4 or c = 5",
			best: "IndexLookUp(Index(t.c_d_e)[[1,5]], Table(t))->Projection",
		},
		{
			sql:  "select a from t where c = 5",
			best: "IndexReader(Index(t.c_d_e)[[5,5]])->Projection",
		},
		{
			sql:  "select a from t where c = 5 and b = 1",
			best: "IndexLookUp(Index(t.c_d_e)[[5,5]], Table(t)->Sel([eq(test.t.b, 1)]))->Projection",
		},
		{
			sql:  "select a from t where not a",
			best: "TableReader(Table(t)->Sel([not(test.t.a)]))",
		},
		{
			sql:  "select a from t where c in (1)",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->Projection",
		},
		{
			sql:  "select a from t where c in ('1')",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->Projection",
		},
		{
			sql:  "select a from t where c = 1.0",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->Projection",
		},
		{
			sql:  "select a from t where c in (1) and d > 3",
			best: "IndexReader(Index(t.c_d_e)[(1 3,1 +inf]])->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3) and (d > 3 and d < 4 or d > 5 and d < 6)",
			best: "Dual->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3) and (d > 2 and d < 4 or d > 5 and d < 7)",
			best: "IndexReader(Index(t.c_d_e)[(1 2,1 4) (1 5,1 7) (2 2,2 4) (2 5,2 7) (3 2,3 4) (3 5,3 7)])->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3)",
			best: "IndexReader(Index(t.c_d_e)[[1,1] [2,2] [3,3]])->Projection",
		},
		{
			sql:  "select a from t where c in (1, 2, 3) and d in (1,2) and e = 1",
			best: "IndexReader(Index(t.c_d_e)[[1 1 1,1 1 1] [1 2 1,1 2 1] [2 1 1,2 1 1] [2 2 1,2 2 1] [3 1 1,3 1 1] [3 2 1,3 2 1]])->Projection",
		},
		{
			sql:  "select a from t where d in (1, 2, 3)",
			best: "TableReader(Table(t)->Sel([in(test.t.d, 1, 2, 3)]))->Projection",
		},
		{
			sql:  "select a from t where c not in (1)",
			best: "IndexReader(Index(t.c_d_e)[(NULL,1) (1,+inf]])->Projection",
		},
		// test like
		{
			sql:  "select a from t use index(c_d_e) where c != 1",
			best: "IndexReader(Index(t.c_d_e)[[-inf,1) (1,+inf]])->Projection",
		},
		{
			sql:  "select a from t where c_str like ''",
			best: `IndexReader(Index(t.c_d_e_str)[["",""]])->Projection`,
		},
		{
			sql:  "select a from t where c_str like 'abc'",
			best: `IndexReader(Index(t.c_d_e_str)[["abc","abc"]])->Projection`,
		},
		{
			sql:  "select a from t where c_str not like 'abc'",
			best: "TableReader(Table(t)->Sel([not(like(test.t.c_str, abc, 92))]))->Projection",
		},
		{
			sql:  "select a from t where not (c_str like 'abc' or c_str like 'abd')",
			best: `TableReader(Table(t)->Sel([and(not(like(test.t.c_str, abc, 92)), not(like(test.t.c_str, abd, 92)))]))->Projection`,
		},
		{
			sql:  "select a from t where c_str like '_abc'",
			best: "TableReader(Table(t)->Sel([like(test.t.c_str, _abc, 92)]))->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc%'`,
			best: `IndexReader(Index(t.c_d_e_str)[["abc","abd")])->Projection`,
		},
		{
			sql:  "select a from t where c_str like 'abc_'",
			best: `IndexReader(Index(t.c_d_e_str)[("abc","abd")]->Sel([like(test.t.c_str, abc_, 92)]))->Projection`,
		},
		{
			sql:  "select a from t where c_str like 'abc%af'",
			best: `IndexReader(Index(t.c_d_e_str)[["abc","abd")]->Sel([like(test.t.c_str, abc%af, 92)]))->Projection`,
		},
		{
			sql:  `select a from t where c_str like 'abc\\_' escape ''`,
			best: `IndexReader(Index(t.c_d_e_str)[["abc_","abc_"]])->Projection`,
		},
		{
			sql:  `select a from t where c_str like 'abc\\_'`,
			best: `IndexReader(Index(t.c_d_e_str)[["abc_","abc_"]])->Projection`,
		},
		{
			sql:  `select a from t where c_str like 'abc\\\\_'`,
			best: "IndexReader(Index(t.c_d_e_str)[(\"abc\\\",\"abc]\")]->Sel([like(test.t.c_str, abc\\\\_, 92)]))->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\_%'`,
			best: "IndexReader(Index(t.c_d_e_str)[[\"abc_\",\"abc`\")])->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc=_%' escape '='`,
			best: "IndexReader(Index(t.c_d_e_str)[[\"abc_\",\"abc`\")])->Projection",
		},
		{
			sql:  `select a from t where c_str like 'abc\\__'`,
			best: "IndexReader(Index(t.c_d_e_str)[(\"abc_\",\"abc`\")]->Sel([like(test.t.c_str, abc\\__, 92)]))->Projection",
		},
		{
			// Check that 123 is converted to string '123'. index can be used.
			sql:  `select a from t where c_str like 123`,
			best: "IndexReader(Index(t.c_d_e_str)[[\"123\",\"123\"]])->Projection",
		},
		// c is type int which will be added cast to specified type when building function signature, no index can be used.
		{
			sql:  `select a from t where c like '1'`,
			best: "TableReader(Table(t))->Sel([like(cast(test.t.c), 1, 92)])->Projection",
		},
		{
			sql:  `select a from t where c = 1.9 and d > 3`,
			best: "Dual",
		},
		{
			sql:  `select a from t where c < 1.1`,
			best: "IndexReader(Index(t.c_d_e)[[-inf,2)])->Projection",
		},
		{
			sql:  `select a from t where c <= 1.9`,
			best: "IndexReader(Index(t.c_d_e)[[-inf,1]])->Projection",
		},
		{
			sql:  `select a from t where c >= 1.1`,
			best: "IndexReader(Index(t.c_d_e)[[2,+inf]])->Projection",
		},
		{
			sql:  `select a from t where c > 1.9`,
			best: "IndexReader(Index(t.c_d_e)[(1,+inf]])->Projection",
		},
		{
			sql:  `select a from t where c = 123456789098765432101234`,
			best: "Dual",
		},
		{
			sql:  `select a from t where c = 'hanfei'`,
			best: "TableReader(Table(t))->Sel([eq(cast(test.t.c), cast(hanfei))])->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		sc := se.(sessionctx.Context).GetSessionVars().StmtCtx
		sc.IgnoreTruncate = false
		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestAggEliminater(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	se.Execute(context.Background(), "set sql_mode='STRICT_TRANS_TABLES'") // disable only full group by
	tests := []struct {
		sql  string
		best string
	}{
		// Max to Limit + Sort-Desc.
		{
			sql:  "select max(a) from t;",
			best: "TableReader(Table(t)->Limit)->Limit->StreamAgg",
		},
		// Min to Limit + Sort.
		{
			sql:  "select min(a) from t;",
			best: "TableReader(Table(t)->Limit)->Limit->StreamAgg",
		},
		// Min to Limit + Sort, and isnull() should be added.
		{
			sql:  "select min(c_str) from t;",
			best: "IndexReader(Index(t.c_d_e_str)[[-inf,+inf]]->Limit)->Limit->StreamAgg",
		},
		// Do nothing to max + firstrow.
		{
			sql:  "select max(a), b from t;",
			best: "TableReader(Table(t)->StreamAgg)->StreamAgg",
		},
		// If max/min contains scalar function, we can still do transformation.
		{
			sql:  "select max(a+1) from t;",
			best: "TableReader(Table(t)->Sel([not(isnull(plus(test.t.a, 1)))])->TopN([plus(test.t.a, 1) true],0,1))->TopN([plus(test.t.a, 1) true],0,1)->StreamAgg",
		},
		// Do nothing to max+min.
		{
			sql:  "select max(a), min(a) from t;",
			best: "TableReader(Table(t)->StreamAgg)->StreamAgg",
		},
		// Do nothing to max with groupby.
		{
			sql:  "select max(a) from t group by b;",
			best: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
		// If inner is not a data source, we can still do transformation.
		{
			sql:  "select max(a) from (select t1.a from t t1 join t t2 on t1.a=t2.a) t",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(t1.a,t2.a)->Limit->StreamAgg",
		},
	}

	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		sc := se.(sessionctx.Context).GetSessionVars().StmtCtx
		sc.IgnoreTruncate = false
		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

type overrideStore struct{ kv.Storage }

func (store overrideStore) GetClient() kv.Client {
	cli := store.Storage.GetClient()
	return overrideClient{cli}
}

type overrideClient struct{ kv.Client }

func (cli overrideClient) IsRequestTypeSupported(reqType, subType int64) bool {
	return false
}

func (s *testPlanSuite) TestRequestTypeSupportedOff(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(overrideStore{store})
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)

	sql := "select * from t where a in (1, 10, 20)"
	expect := "TableReader(Table(t))->Sel([in(test.t.a, 1, 10, 20)])"

	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	p, err := planner.Optimize(se, stmt, s.is)
	c.Assert(err, IsNil)
	c.Assert(core.ToString(p), Equals, expect, Commentf("for %s", sql))
}

func (s *testPlanSuite) TestIndexJoinUnionScan(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	tests := []struct {
		sql  string
		best string
	}{
		// Test Index Join + UnionScan + TableScan.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1, t t2 where t1.a = t2.a",
			best: "IndexJoin{TableReader(Table(t))->UnionScan([])->TableReader(Table(t))->UnionScan([])}(t1.a,t2.a)",
		},
		// Test Index Join + UnionScan + DoubleRead.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1, t t2 where t1.a = t2.c",
			best: "IndexJoin{TableReader(Table(t))->UnionScan([])->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))->UnionScan([])}(t1.a,t2.c)",
		},
		// Test Index Join + UnionScan + IndexScan.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ t1.a , t2.c from t t1, t t2 where t1.a = t2.c",
			best: "IndexJoin{TableReader(Table(t))->UnionScan([])->IndexReader(Index(t.c_d_e)[[NULL,+inf]])->UnionScan([])}(t1.a,t2.c)->Projection",
		},
	}
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		// Make txn not read only.
		txn, err := se.Txn(true)
		c.Assert(err, IsNil)
		txn.Set(kv.Key("AAA"), []byte("BBB"))
		c.Assert(se.StmtCommit(), IsNil)
		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestDoSubquery(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()
	se, err := session.CreateSession4Test(store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test")
	c.Assert(err, IsNil)
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "do 1 in (select a from t)",
			best: "LeftHashJoin{Dual->TableReader(Table(t))}->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := planner.Optimize(se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, comment)
	}
}
