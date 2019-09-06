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
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testPlanSuite{})

type testPlanSuite struct {
	*parser.Parser

	is infoschema.InfoSchema
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{core.MockSignedTable(), core.MockUnsignedTable()})
	s.Parser = parser.New()
	s.Parser.EnableWindowFunc(true)
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
			best: "TableReader(Table(t)->TopN([plus(test.t.a, test.t.b)],0,1))->Projection->TopN([col_3],0,1)->Projection->Projection",
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
			best: "IndexMergeJoin{TableReader(Table(t)->TopN([test.t.b],0,10))->TopN([test.t.b],0,10)->TableReader(Table(t))}(test.t.a,test.t1.a)->Limit",
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
		{
			sql:  "select * from (select *, NULL as xxx from t) t order by xxx",
			best: "TableReader(Table(t))->Projection",
		},
		{
			sql:  "select lead(a, 1) over (partition by null) as c from t",
			best: "TableReader(Table(t))->Window(lead(test.t.a, 1) over())->Projection",
		},
		{
			sql:  "select * from t use index(f) where f = 1 and a = 1",
			best: "IndexLookUp(Index(t.f)[[1,1]]->Sel([eq(test.t.a, 1)]), Table(t))",
		},
		{
			sql:  "select * from t2 use index(b) where b = 1 and a = 1",
			best: "IndexLookUp(Index(t2.b)[[1,1]]->Sel([eq(test.t2.a, 1)]), Table(t2))",
		},
	}
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		err = se.NewTxn(context.Background())
		c.Assert(err, IsNil)
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
			best: "LeftHashJoin{TableReader(Table(t))->Projection->TableReader(Table(t))->Projection}(cast(test.t1.a),cast(test.t2.c_str))->Projection",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.a",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.b,test.t2.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a join t t3 on t1.a = t3.a",
			best: "LeftHashJoin{MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)->TableReader(Table(t))}(test.t1.a,test.t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a join t t3 on t1.b = t3.a",
			best: "LeftHashJoin{MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)->TableReader(Table(t))}(test.t1.b,test.t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.a order by t1.a",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.b,test.t2.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.a order by t1.a limit 1",
			best: "IndexJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.b,test.t2.a)->Limit",
		},
		// Test hash join's hint.
		{
			sql:  "select /*+ TIDB_HJ(t1, t2) */ * from t t1 join t t2 on t1.b = t2.a order by t1.a limit 1",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.b,test.t2.a)->TopN([test.t1.a],0,1)",
		},
		{
			sql:  "select * from t t1 left join t t2 on t1.b = t2.a where 1 = 1 limit 1",
			best: "IndexMergeJoin{TableReader(Table(t)->Limit)->Limit->TableReader(Table(t))}(test.t1.b,test.t2.a)->Limit",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.a and t1.c = 1 and t1.d = 1 and t1.e = 1 order by t1.a limit 1",
			best: "IndexMergeJoin{IndexLookUp(Index(t.c_d_e)[[1 1 1,1 1 1]], Table(t))->TableReader(Table(t))}(test.t1.b,test.t2.a)->TopN([test.t1.a],0,1)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.b = t2.b join t t3 on t1.b = t3.b",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.b,test.t2.b)->TableReader(Table(t))}(test.t1.b,test.t3.b)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a order by t1.a",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)",
		},
		{
			sql:  "select * from t t1 left outer join t t2 on t1.a = t2.a right outer join t t3 on t1.a = t3.a",
			best: "MergeRightOuterJoin{MergeLeftOuterJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)->TableReader(Table(t))}(test.t1.a,test.t3.a)",
		},
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a join t t3 on t1.a = t3.a and t1.b = 1 and t3.c = 1",
			best: "IndexMergeJoin{IndexMergeJoin{TableReader(Table(t)->Sel([eq(test.t1.b, 1)]))->IndexLookUp(Index(t.c_d_e)[[1,1]], Table(t))}(test.t3.a,test.t1.a)->TableReader(Table(t))}(test.t1.a,test.t2.a)->Projection",
		},
		{
			sql:  "select * from t where t.c in (select b from t s where s.a = t.a)",
			best: "MergeSemiJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,test.s.a)",
		},
		{
			sql:  "select t.c in (select b from t s where s.a = t.a) from t",
			best: "MergeLeftOuterSemiJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,test.s.a)->Projection",
		},
		// Test Single Merge Join.
		// Merge Join now enforce a sort.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.b",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))->Sort}(test.t1.a,test.t2.b)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.a",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)",
		},
		// Test Single Merge Join + Sort.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.a order by t2.a",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.b = t2.b order by t2.a",
			best: "MergeInnerJoin{TableReader(Table(t))->Sort->TableReader(Table(t))->Sort}(test.t1.b,test.t2.b)->Sort",
		},
		// Test Single Merge Join + Sort + desc.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.a = t2.a order by t2.a desc",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2)*/ * from t t1, t t2 where t1.b = t2.b order by t2.b desc",
			best: "MergeInnerJoin{TableReader(Table(t))->Sort->TableReader(Table(t))->Sort}(test.t1.b,test.t2.b)",
		},
		// Test Multi Merge Join.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1, t t2, t t3 where t1.a = t2.a and t2.a = t3.a",
			best: "MergeInnerJoin{MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)->TableReader(Table(t))}(test.t2.a,test.t3.a)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1, t t2, t t3 where t1.a = t2.b and t2.a = t3.b",
			best: "MergeInnerJoin{MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))->Sort}(test.t1.a,test.t2.b)->Sort->TableReader(Table(t))->Sort}(test.t2.a,test.t3.b)",
		},
		// Test Multi Merge Join with multi keys.
		// TODO: More tests should be added.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1, t t2, t t3 where t1.c = t2.c and t1.d = t2.d and t3.c = t1.c and t3.d = t1.d",
			best: "MergeInnerJoin{MergeInnerJoin{IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(test.t1.c,test.t2.c)(test.t1.d,test.t2.d)->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(test.t1.c,test.t3.c)(test.t1.d,test.t3.d)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1, t t2, t t3 where t1.c = t2.c and t1.d = t2.d and t3.c = t1.c and t3.d = t1.d order by t1.c",
			best: "MergeInnerJoin{MergeInnerJoin{IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(test.t1.c,test.t2.c)(test.t1.d,test.t2.d)->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(test.t1.c,test.t3.c)(test.t1.d,test.t3.d)",
		},
		// Test Multi Merge Join + Outer Join.
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1 left outer join t t2 on t1.a = t2.a left outer join t t3 on t2.a = t3.a",
			best: "MergeLeftOuterJoin{MergeLeftOuterJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)->TableReader(Table(t))}(test.t2.a,test.t3.a)",
		},
		{
			sql:  "select /*+ TIDB_SMJ(t1,t2,t3)*/ * from t t1 left outer join t t2 on t1.a = t2.a left outer join t t3 on t1.a = t3.a",
			best: "MergeLeftOuterJoin{MergeLeftOuterJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)->TableReader(Table(t))}(test.t1.a,test.t3.a)",
		},
		// Test Index Join + TableScan.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1, t t2 where t1.a = t2.a",
			best: "IndexMergeJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)",
		},
		// Test Index Join + DoubleRead.
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1, t t2 where t1.a = t2.c",
			best: "IndexJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(test.t1.a,test.t2.c)",
		},
		// Test Index Join + SingleRead.
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ t1.a , t2.a from t t1, t t2 where t1.a = t2.c",
			best: "IndexMergeJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t1.a,test.t2.c)->Projection",
		},
		// Test Index Join + Order by.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ t1.a, t2.a from t t1, t t2 where t1.a = t2.a order by t1.c",
			best: "IndexJoin{IndexReader(Index(t.c_d_e)[[NULL,+inf]])->TableReader(Table(t))}(test.t1.a,test.t2.a)->Projection",
		},
		// Test Index Join + Order by.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ t1.a, t2.a from t t1, t t2 where t1.a = t2.a order by t2.c",
			best: "IndexJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t1.a)->Projection",
		},
		// Test Index Join + TableScan + Rotate.
		{
			sql:  "select /*+ TIDB_INLJ(t1) */ t1.a , t2.a from t t1, t t2 where t1.a = t2.c",
			best: "IndexMergeJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t2.c,test.t1.a)->Projection",
		},
		// Test Index Join + OuterJoin + TableScan.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 left outer join t t2 on t1.a = t2.a and t2.b < 1",
			best: "IndexMergeJoin{TableReader(Table(t))->TableReader(Table(t)->Sel([lt(test.t2.b, 1)]))}(test.t1.a,test.t2.a)",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 join t t2 on t1.d=t2.d and t2.c = 1",
			best: "IndexJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(test.t1.d,test.t2.d)",
		},
		// Test Index Join failed.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 left outer join t t2 on t1.a = t2.b",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.b)",
		},
		// Test Index Join failed.
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 right outer join t t2 on t1.a = t2.b",
			best: "RightHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.b)",
		},
		// Test Semi Join hint success.
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 where t1.a in (select a from t t2)",
			best: "IndexMergeJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)->Projection",
		},
		// Test Semi Join hint fail.
		{
			sql:  "select /*+ TIDB_INLJ(t1) */ * from t t1 where t1.a in (select a from t t2)",
			best: "IndexMergeJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t2.a,test.t1.a)->Projection",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.c=t2.c and t1.f=t2.f",
			best: "IndexJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(test.t1.c,test.t2.c)",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.a = t2.a and t1.f=t2.f",
			best: "IndexMergeJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.f=t2.f and t1.a=t2.a",
			best: "IndexMergeJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.a=t2.a and t2.a in (1, 2)",
			best: "IndexMergeJoin{TableReader(Table(t))->TableReader(Table(t)->Sel([in(test.t2.a, 1, 2)]))}(test.t1.a,test.t2.a)",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.b=t2.c and t1.b=1 and t2.d > t1.d-10 and t2.d < t1.d+10",
			best: "IndexJoin{TableReader(Table(t)->Sel([eq(test.t1.b, 1)]))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.b=t2.b and t1.c=1 and t2.c=1 and t2.d > t1.d-10 and t2.d < t1.d+10",
			best: "LeftHashJoin{IndexLookUp(Index(t.c_d_e)[[1,1]], Table(t))->IndexLookUp(Index(t.c_d_e)[[1,1]], Table(t))}(test.t1.b,test.t2.b)",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t2.c > t1.d-10 and t2.c < t1.d+10",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t1.b = t2.c and t2.c=1 and t2.d=2 and t2.e=4",
			best: "LeftHashJoin{TableReader(Table(t)->Sel([eq(test.t1.b, 1)]))->IndexLookUp(Index(t.c_d_e)[[1 2 4,1 2 4]], Table(t))}",
		},
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1 join t t2 where t2.c=1 and t2.d=1 and t2.e > 10 and t2.e < 20",
			best: "LeftHashJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[(1 1 10,1 1 20)], Table(t))}",
		},
	}
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
	ctx := se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()
	sessionVars.HashAggFinalConcurrency = 1
	sessionVars.HashAggPartialConcurrency = 1
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
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,test.s.a)->Projection",
		},
		// Test Nested sub query.
		{
			sql:  "select * from t where exists (select s.a from t s where s.c in (select c from t as k where k.d = s.d) having sum(s.a) = t.a )",
			best: "LeftHashJoin{TableReader(Table(t))->Projection->MergeSemiJoin{IndexReader(Index(t.c_d_e)[[NULL,+inf]])->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.s.c,test.k.c)(test.s.d,test.k.d)->Projection->StreamAgg}(cast(test.t.a),sel_agg_1)->Projection",
		},
		// Test Semi Join + Order by.
		{
			sql:  "select * from t where a in (select a from t) order by b",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t.a,test.t.a)->Projection->Sort",
		},
		// Test Apply.
		{
			sql:  "select t.c in (select count(*) from t s, t t1 where s.a = t.a and s.a = t1.a) from t",
			best: "Apply{TableReader(Table(t))->MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.s.a,test.t1.a)->StreamAgg}->Projection",
		},
		{
			sql:  "select (select count(*) from t s, t t1 where s.a = t.a and s.a = t1.a) from t",
			best: "LeftHashJoin{TableReader(Table(t))->MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.s.a,test.t1.a)->StreamAgg}(test.t.a,test.s.a)->Projection",
		},
		{
			sql:  "select (select count(*) from t s, t t1 where s.a = t.a and s.a = t1.a) from t order by t.a",
			best: "LeftHashJoin{TableReader(Table(t))->MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.s.a,test.t1.a)->StreamAgg}(test.t.a,test.s.a)->Projection->Sort->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t)->Limit)->Limit->TableReader(Table(t))}(test.t1.b,test.t2.b)->TopN([test.t1.a],0,1)->TableReader(Table(t))}(test.t2.b,test.t3.b)->TopN([test.t1.a],0,1)",
		},
		{
			sql:  "select * from t t1 left join t t2 on t1.b = t2.b left join t t3 on t2.b = t3.b order by t1.b limit 1",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t)->TopN([test.t1.b],0,1))->TopN([test.t1.b],0,1)->TableReader(Table(t))}(test.t1.b,test.t2.b)->TopN([test.t1.b],0,1)->TableReader(Table(t))}(test.t2.b,test.t3.b)->TopN([test.t1.b],0,1)",
		},
		{
			sql:  "select * from t t1 left join t t2 on t1.b = t2.b left join t t3 on t2.b = t3.b limit 1",
			best: "LeftHashJoin{LeftHashJoin{TableReader(Table(t)->Limit)->Limit->TableReader(Table(t))}(test.t1.b,test.t2.b)->Limit->TableReader(Table(t))}(test.t2.b,test.t3.b)->Limit",
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

		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
			best: "IndexJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(test.t1.c,test.t2.c)->Delete",
		},
		{
			sql:  "delete /*+ TIDB_SMJ(t1, t2) */ from t1 using t t1, t t2 where t1.c=t2.c",
			best: "MergeInnerJoin{IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))->IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))}(test.t1.c,test.t2.c)->Delete",
		},
		{
			sql:  "update /*+ TIDB_SMJ(t1, t2) */ t t1, t t2 set t1.a=1, t2.a=1 where t1.a=t2.a",
			best: "MergeInnerJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)->Update",
		},
		{
			sql:  "update /*+ TIDB_HJ(t1, t2) */ t t1, t t2 set t1.a=1, t2.a=1 where t1.a=t2.a",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)->Update",
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

		core.Preprocess(se, stmt, s.is)
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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

		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
	ctx := se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()
	sessionVars.HashAggFinalConcurrency = 1
	sessionVars.HashAggPartialConcurrency = 1

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
			best: "TableReader(Table(t))->Projection->HashAgg",
		},
		{
			sql:  "select sum(distinct a), avg(b + c) from t group by d",
			best: "TableReader(Table(t))->Projection->HashAgg",
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
			sql:  "select sum(e), avg(e + c) from t where c = 1 group by e",
			best: "IndexReader(Index(t.c_d_e)[[1,1]]->HashAgg)->HashAgg",
		},
		// Test hash agg + index double.
		{
			sql:  "select sum(e), avg(b + c) from t where c = 1 and e = 1 group by d",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t))->Projection->HashAgg",
		},
		// Test stream agg + index double.
		{
			sql:  "select sum(e), avg(b + c) from t where c = 1 and b = 1",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]], Table(t)->Sel([eq(test.t.b, 1)]))->Projection->StreamAgg",
		},
		// Test hash agg + order.
		{
			sql:  "select sum(e) as k, avg(b + c) from t where c = 1 and b = 1 and e = 1 group by d order by k",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t)->Sel([eq(test.t.b, 1)]))->Projection->Projection->StreamAgg->Sort",
		},
		// Test stream agg + order.
		{
			sql:  "select sum(e) as k, avg(b + c) from t where c = 1 and b = 1 and e = 1 group by c order by k",
			best: "IndexLookUp(Index(t.c_d_e)[[1,1]]->Sel([eq(test.t.e, 1)]), Table(t)->Sel([eq(test.t.b, 1)]))->Projection->Projection->StreamAgg->Sort",
		},
		// Test agg can't push down.
		{
			sql:  "select sum(to_base64(e)) from t where c = 1",
			best: "IndexReader(Index(t.c_d_e)[[1,1]])->Projection->StreamAgg",
		},
		{
			sql:  "select (select count(1) k from t s where s.a = t.a having k != 0) from t",
			best: "MergeLeftOuterJoin{TableReader(Table(t))->TableReader(Table(t))->Projection}(test.t.a,test.s.a)->Projection",
		},
		// Test stream agg with multi group by columns.
		{
			sql:  "select sum(to_base64(e)) from t group by e,d,c order by c",
			best: "IndexReader(Index(t.c_d_e)[[NULL,+inf]])->Projection->StreamAgg->Projection",
		},
		{
			sql:  "select sum(e+1) from t group by e,d,c order by c",
			best: "IndexReader(Index(t.c_d_e)[[NULL,+inf]]->StreamAgg)->StreamAgg->Projection",
		},
		{
			sql:  "select sum(to_base64(e)) from t group by e,d,c order by c,e",
			best: "IndexReader(Index(t.c_d_e)[[NULL,+inf]])->Projection->StreamAgg->Sort->Projection",
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
			best: "MergeInnerJoin{IndexReader(Index(t.g)[[NULL,+inf]])->IndexReader(Index(t.g)[[NULL,+inf]])}(test.a.g,test.b.g)->Projection->StreamAgg",
		},
		// Test index join + stream agg
		{
			sql:  "select /*+ tidb_inlj(a,b) */ sum(a.g), sum(b.g) from t a join t b on a.g = b.g and a.g > 60 group by a.g order by a.g limit 1",
			best: "IndexMergeJoin{IndexReader(Index(t.g)[(60,+inf]])->IndexReader(Index(t.g)[[NULL,+inf]]->Sel([gt(test.b.g, 60)]))}(test.a.g,test.b.g)->Projection->StreamAgg->Limit->Projection",
		},
		{
			sql:  "select sum(a.g), sum(b.g) from t a join t b on a.g = b.g and a.a>5 group by a.g order by a.g limit 1",
			best: "MergeInnerJoin{IndexReader(Index(t.g)[[NULL,+inf]]->Sel([gt(test.a.a, 5)]))->IndexReader(Index(t.g)[[NULL,+inf]])}(test.a.g,test.b.g)->Projection->StreamAgg->Limit->Projection",
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

		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
			best: "IndexReader(Index(t.c_d_e)[[-inf,1) (1,+inf]])->Projection",
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
			best: `IndexReader(Index(t.c_d_e_str)[[-inf,"abc") ("abc",+inf]])->Projection`,
		},
		{
			sql:  "select a from t where not (c_str like 'abc' or c_str like 'abd')",
			best: `IndexReader(Index(t.c_d_e_str)[[-inf,"abc") ("abc","abd") ("abd",+inf]])->Projection`,
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
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil, comment)
		c.Assert(core.ToString(p), Equals, tt.best, comment)
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
			best: "TableReader(Table(t)->Sel([not(isnull(plus(test.t.a, 1)))])->TopN([plus(test.t.a, 1) true],0,1))->Projection->TopN([col_1 true],0,1)->Projection->Projection->StreamAgg",
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
			best: "IndexMergeJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.a)->Limit->StreamAgg",
		},
	}

	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		sc := se.(sessionctx.Context).GetSessionVars().StmtCtx
		sc.IgnoreTruncate = false
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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
	p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
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

	definitions := []model.PartitionDefinition{
		{
			ID:       41,
			Name:     model.NewCIStr("p1"),
			LessThan: []string{"16"},
		},
		{
			ID:       42,
			Name:     model.NewCIStr("p2"),
			LessThan: []string{"32"},
		},
	}
	pis := core.MockPartitionInfoSchema(definitions)

	tests := []struct {
		sql  string
		best string
		is   infoschema.InfoSchema
	}{
		// Test Index Join + UnionScan + TableScan.
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1, t t2 where t1.a = t2.a",
			best: "IndexMergeJoin{TableReader(Table(t))->UnionScan([])->TableReader(Table(t))->UnionScan([])}(test.t1.a,test.t2.a)",
			is:   s.is,
		},
		// Test Index Join + UnionScan + DoubleRead.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ * from t t1, t t2 where t1.a = t2.c",
			best: "IndexMergeJoin{TableReader(Table(t))->UnionScan([])->TableReader(Table(t))->UnionScan([])}(test.t2.c,test.t1.a)",
			is:   s.is,
		},
		// Test Index Join + UnionScan + IndexScan.
		{
			sql:  "select /*+ TIDB_INLJ(t1, t2) */ t1.a , t2.c from t t1, t t2 where t1.a = t2.c",
			best: "IndexMergeJoin{TableReader(Table(t))->UnionScan([])->TableReader(Table(t))->UnionScan([])}(test.t2.c,test.t1.a)->Projection",
			is:   s.is,
		},
		// Index Join + Union Scan + Union All is not supported now.
		{
			sql:  "select /*+ TIDB_INLJ(t2) */ * from t t1, t t2 where t1.a = t2.a",
			best: "LeftHashJoin{UnionAll{TableReader(Table(t))->UnionScan([])->TableReader(Table(t))->UnionScan([])}->UnionAll{TableReader(Table(t))->UnionScan([])->TableReader(Table(t))->UnionScan([])}}(test.t1.a,test.t2.a)",
			is:   pis,
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
		p, err := planner.Optimize(context.TODO(), se, stmt, tt.is)
		c.Assert(err, IsNil, comment)
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
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestIndexLookupCartesianJoin(c *C) {
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
	sql := "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 join t t2"
	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
	c.Assert(err, IsNil)
	c.Assert(core.ToString(p), Equals, "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}")
	warnings := se.GetSessionVars().StmtCtx.GetWarnings()
	lastWarn := warnings[len(warnings)-1]
	err = core.ErrInternal.GenWithStack("TIDB_INLJ hint is inapplicable without column equal ON condition")
	c.Assert(terror.ErrorEqual(err, lastWarn.Err), IsTrue)
}

func (s *testPlanSuite) TestSemiJoinToInner(c *C) {
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
	sql := "select t1.a, (select count(t2.a) from t t2 where t2.g in (select t3.d from t t3 where t3.c = t1.a)) as agg_col from t t1;"
	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
	c.Assert(err, IsNil)
	c.Assert(core.ToString(p), Equals, "Apply{TableReader(Table(t))->IndexMergeJoin{IndexReader(Index(t.c_d_e)[[NULL,+inf]]->HashAgg)->HashAgg->IndexReader(Index(t.g)[[NULL,+inf]])}(test.t3.d,test.t2.g)}->HashAgg")
}

func (s *testPlanSuite) TestUnmatchedTableInHint(c *C) {
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
		sql     string
		warning string
	}{
		{
			sql:     "SELECT /*+ TIDB_SMJ(t3, t4) */ * from t t1, t t2 where t1.a = t2.a",
			warning: "[planner:1815]There are no matching table names for (t3, t4) in optimizer hint /*+ SM_JOIN(t3, t4) */ or /*+ TIDB_SMJ(t3, t4) */. Maybe you can use the table alias name",
		},
		{
			sql:     "SELECT /*+ TIDB_HJ(t3, t4) */ * from t t1, t t2 where t1.a = t2.a",
			warning: "[planner:1815]There are no matching table names for (t3, t4) in optimizer hint /*+ HASH_JOIN(t3, t4) */ or /*+ TIDB_HJ(t3, t4) */. Maybe you can use the table alias name",
		},
		{
			sql:     "SELECT /*+ TIDB_INLJ(t3, t4) */ * from t t1, t t2 where t1.a = t2.a",
			warning: "[planner:1815]There are no matching table names for (t3, t4) in optimizer hint /*+ INL_JOIN(t3, t4) */ or /*+ TIDB_INLJ(t3, t4) */. Maybe you can use the table alias name",
		},
		{
			sql:     "SELECT /*+ TIDB_SMJ(t1, t2) */ * from t t1, t t2 where t1.a = t2.a",
			warning: "",
		},
		{
			sql:     "SELECT /*+ TIDB_SMJ(t3, t4) */ * from t t1, t t2, t t3 where t1.a = t2.a and t2.a = t3.a",
			warning: "[planner:1815]There are no matching table names for (t4) in optimizer hint /*+ SM_JOIN(t3, t4) */ or /*+ TIDB_SMJ(t3, t4) */. Maybe you can use the table alias name",
		},
	}
	for _, test := range tests {
		se.GetSessionVars().StmtCtx.SetWarnings(nil)
		stmt, err := s.ParseOneStmt(test.sql, "", "")
		c.Assert(err, IsNil)
		_, err = planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil)
		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		if test.warning == "" {
			c.Assert(len(warnings), Equals, 0)
		} else {
			c.Assert(len(warnings), Equals, 1)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warnings[0].Err.Error(), Equals, test.warning)
		}
	}
}

func (s *testPlanSuite) TestJoinHints(c *C) {
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
		sql     string
		best    string
		warning string
	}{
		{
			sql:     "select /*+ TIDB_INLJ(t1) */ t1.a, t2.a, t3.a from t t1, t t2, t t3 where t1.a = t2.a and t2.a = t3.a;",
			best:    "MergeInnerJoin{TableReader(Table(t))->IndexMergeJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t2.a,test.t1.a)}(test.t3.a,test.t2.a)->Projection",
			warning: "",
		},
		{
			sql:     "select /*+ TIDB_INLJ(t1) */ t1.b, t2.a from t t1, t t2 where t1.b = t2.a;",
			best:    "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.b,test.t2.a)",
			warning: "[planner:1815]Optimizer Hint /*+ INL_JOIN(t1) */ or /*+ TIDB_INLJ(t1) */ is inapplicable",
		},
		{
			sql:     "select /*+ TIDB_INLJ(t2) */ t1.b, t2.a from t2 t1, t2 t2 where t1.b=t2.b and t2.c=-1;",
			best:    "IndexJoin{IndexReader(Index(t2.b)[[NULL,+inf]])->TableReader(Table(t2)->Sel([eq(test.t2.c, -1)]))}(test.t2.b,test.t1.b)->Projection",
			warning: "[planner:1815]Optimizer Hint /*+ INL_JOIN(t2) */ or /*+ TIDB_INLJ(t2) */ is inapplicable",
		},
	}
	ctx := context.Background()
	for i, test := range tests {
		comment := Commentf("case:%v sql:%s", i, test)
		stmt, err := s.ParseOneStmt(test.sql, "", "")
		c.Assert(err, IsNil, comment)

		se.GetSessionVars().StmtCtx.SetWarnings(nil)
		p, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, test.best)

		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		if test.warning == "" {
			c.Assert(len(warnings), Equals, 0)
		} else {
			c.Assert(len(warnings), Equals, 1, Commentf("%v", warnings))
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning)
			c.Assert(warnings[0].Err.Error(), Equals, test.warning)
		}
	}
}

func (s *testPlanSuite) TestAggregationHints(c *C) {
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

	sessionVars := se.(sessionctx.Context).GetSessionVars()
	sessionVars.HashAggFinalConcurrency = 1
	sessionVars.HashAggPartialConcurrency = 1

	tests := []struct {
		sql         string
		best        string
		warning     string
		aggPushDown bool
	}{
		// without Aggregation hints
		{
			sql:  "select count(*) from t t1, t t2 where t1.a = t2.b",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.b)->StreamAgg",
		},
		{
			sql:  "select count(t1.a) from t t1, t t2 where t1.a = t2.a*2 group by t1.a",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))->Projection}(test.t1.a,mul(test.t2.a, 2))->HashAgg",
		},
		// with Aggregation hints
		{
			sql:  "select /*+ HASH_AGG() */ count(*) from t t1, t t2 where t1.a = t2.b",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.b)->HashAgg",
		},
		{
			sql:  "select /*+ STREAM_AGG() */ count(t1.a) from t t1, t t2 where t1.a = t2.a*2 group by t1.a",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))->Projection}(test.t1.a,mul(test.t2.a, 2))->Sort->StreamAgg",
		},
		// test conflict warning
		{
			sql:     "select /*+ HASH_AGG() STREAM_AGG() */ count(*) from t t1, t t2 where t1.a = t2.b",
			best:    "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t1.a,test.t2.b)->StreamAgg",
			warning: "[planner:1815]Optimizer aggregation hints are conflicted",
		},
		// additional test
		{
			sql:  "select /*+ STREAM_AGG() */ distinct a from t",
			best: "TableReader(Table(t)->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select /*+ HASH_AGG() */ t1.a from t t1 where t1.a < any(select t2.b from t t2)",
			best: "LeftHashJoin{TableReader(Table(t)->Sel([if(isnull(test.t1.a), <nil>, 1)]))->TableReader(Table(t)->HashAgg)->HashAgg->Sel([ne(agg_col_cnt, 0)])}->Projection->Projection",
		},
		{
			sql:  "select /*+ hash_agg() */ t1.a from t t1 where t1.a != any(select t2.b from t t2)",
			best: "LeftHashJoin{TableReader(Table(t)->Sel([if(isnull(test.t1.a), <nil>, 1)]))->TableReader(Table(t))->Projection->HashAgg->Sel([ne(agg_col_cnt, 0)])}->Projection->Projection",
		},
		{
			sql:  "select /*+ hash_agg() */ t1.a from t t1 where t1.a = all(select t2.b from t t2)",
			best: "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))->Projection->HashAgg}->Projection->Projection",
		},
		{
			sql:         "select /*+ STREAM_AGG() */ sum(t1.a) from t t1 join t t2 on t1.b = t2.b group by t1.b",
			best:        "LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))->Sort->Projection->StreamAgg}(test.t2.b,test.t1.b)->HashAgg",
			warning:     "[planner:1815]Optimizer Hint STREAM_AGG is inapplicable",
			aggPushDown: true,
		},
	}
	ctx := context.Background()
	for i, test := range tests {
		comment := Commentf("case:%v sql:%s", i, test)
		se.GetSessionVars().StmtCtx.SetWarnings(nil)
		se.GetSessionVars().AllowAggPushDown = test.aggPushDown

		stmt, err := s.ParseOneStmt(test.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, test.best, comment)

		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		if test.warning == "" {
			c.Assert(len(warnings), Equals, 0, comment)
		} else {
			c.Assert(len(warnings), Equals, 1, comment)
			c.Assert(warnings[0].Level, Equals, stmtctx.WarnLevelWarning, comment)
			c.Assert(warnings[0].Err.Error(), Equals, test.warning, comment)
		}
	}
}

func (s *testPlanSuite) TestHintAlias(c *C) {
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
		sql1 string
		sql2 string
	}{
		{
			sql1: "select /*+ TIDB_SMJ(t1) */ t1.a, t1.b from t t1, (select /*+ TIDB_INLJ(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ SM_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ INL_JOIN(t3) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ TIDB_HJ(t1) */ t1.a, t1.b from t t1, (select /*+ TIDB_SMJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ HASH_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ SM_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
		{
			sql1: "select /*+ TIDB_INLJ(t1) */ t1.a, t1.b from t t1, (select /*+ TIDB_HJ(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			sql2: "select /*+ INL_JOIN(t1) */ t1.a, t1.b from t t1, (select /*+ HASH_JOIN(t2) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql1:%s sql2:%s", i, tt.sql1, tt.sql2)
		stmt1, err := s.ParseOneStmt(tt.sql1, "", "")
		c.Assert(err, IsNil, comment)
		stmt2, err := s.ParseOneStmt(tt.sql2, "", "")
		c.Assert(err, IsNil, comment)

		p1, err := planner.Optimize(ctx, se, stmt1, s.is)
		c.Assert(err, IsNil)
		p2, err := planner.Optimize(ctx, se, stmt2, s.is)
		c.Assert(err, IsNil)

		c.Assert(core.ToString(p1), Equals, core.ToString(p2))
	}
}

func (s *testPlanSuite) TestIndexHint(c *C) {
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
		sql     string
		best    string
		hasWarn bool
	}{
		// simple case
		{
			sql:     "select /*+ INDEX(t, c_d_e) */ * from t",
			best:    "IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))",
			hasWarn: false,
		},
		{
			sql:     "select /*+ INDEX(t, c_d_e) */ * from t t1",
			best:    "TableReader(Table(t))",
			hasWarn: false,
		},
		{
			sql:     "select /*+ INDEX(t1, c_d_e) */ * from t t1",
			best:    "IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))",
			hasWarn: false,
		},
		{
			sql:     "select /*+ INDEX(t1, c_d_e), INDEX(t2, f) */ * from t t1, t t2 where t1.a = t2.b",
			best:    "LeftHashJoin{IndexLookUp(Index(t.c_d_e)[[NULL,+inf]], Table(t))->IndexLookUp(Index(t.f)[[NULL,+inf]], Table(t))}(test.t1.a,test.t2.b)",
			hasWarn: false,
		},
		// test multiple indexes
		{
			sql:     "select /*+ INDEX(t, c_d_e, f, g) */ * from t order by f",
			best:    "IndexLookUp(Index(t.f)[[NULL,+inf]], Table(t))",
			hasWarn: false,
		},
		// there will be a warning instead of error when index not exist
		{
			sql:     "select /*+ INDEX(t, no_such_index) */ * from t",
			best:    "TableReader(Table(t))",
			hasWarn: true,
		},
	}
	ctx := context.Background()
	for i, test := range tests {
		comment := Commentf("case:%v sql:%s", i, test)
		stmt, err := s.ParseOneStmt(test.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil)
		c.Assert(core.ToString(p), Equals, test.best, comment)

		warnings := se.GetSessionVars().StmtCtx.GetWarnings()
		if test.hasWarn {
			c.Assert(warnings, HasLen, 1, comment)
		} else {
			c.Assert(warnings, HasLen, 0, comment)
		}
	}
}

func (s *testPlanSuite) TestQueryBlockHint(c *C) {
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
		plan string
	}{
		{
			sql:  "select /*+ SM_JOIN(@sel_1 t1), INL_JOIN(@sel_2 t3) */ t1.a, t1.b from t t1, (select t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "MergeInnerJoin{TableReader(Table(t))->IndexMergeJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t3.c)}(test.t1.a,test.t2.a)->Projection",
		},
		{
			sql:  "select /*+ SM_JOIN(@sel_1 t1), INL_JOIN(@qb t3) */ t1.a, t1.b from t t1, (select /*+ QB_NAME(qb) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "MergeInnerJoin{TableReader(Table(t))->IndexMergeJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t3.c)}(test.t1.a,test.t2.a)->Projection",
		},
		{
			sql:  "select /*+ HASH_JOIN(@sel_1 t1), SM_JOIN(@sel_2 t2) */ t1.a, t1.b from t t1, (select t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "RightHashJoin{TableReader(Table(t))->MergeInnerJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t3.c)}(test.t1.a,test.t2.a)->Projection",
		},
		{
			sql:  "select /*+ HASH_JOIN(@sel_1 t1), SM_JOIN(@qb t2) */ t1.a, t1.b from t t1, (select /*+ QB_NAME(qb) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "RightHashJoin{TableReader(Table(t))->MergeInnerJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])}(test.t2.a,test.t3.c)}(test.t1.a,test.t2.a)->Projection",
		},
		{
			sql:  "select /*+ INL_JOIN(@sel_1 t1), HASH_JOIN(@sel_2 t2) */ t1.a, t1.b from t t1, (select t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "IndexMergeJoin{TableReader(Table(t))->LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t2.a,test.t3.c)}(test.t2.a,test.t1.a)->Projection",
		},
		{
			sql:  "select /*+ INL_JOIN(@sel_1 t1), HASH_JOIN(@qb t2) */ t1.a, t1.b from t t1, (select /*+ QB_NAME(qb) */ t2.a from t t2, t t3 where t2.a = t3.c) s where t1.a=s.a",
			plan: "IndexMergeJoin{TableReader(Table(t))->LeftHashJoin{TableReader(Table(t))->TableReader(Table(t))}(test.t2.a,test.t3.c)}(test.t2.a,test.t1.a)->Projection",
		},
		{
			sql:  "select /*+ HASH_AGG(@sel_1), STREAM_AGG(@sel_2) */ count(*) from t t1 where t1.a < (select count(*) from t t2 where t1.a > t2.a)",
			plan: "Apply{TableReader(Table(t))->TableReader(Table(t)->Sel([gt(test.t1.a, test.t2.a)])->StreamAgg)->StreamAgg->Sel([not(isnull(count(*)))])}->HashAgg",
		},
		{
			sql:  "select /*+ STREAM_AGG(@sel_1), HASH_AGG(@qb) */ count(*) from t t1 where t1.a < (select /*+ QB_NAME(qb) */ count(*) from t t2 where t1.a > t2.a)",
			plan: "Apply{TableReader(Table(t))->TableReader(Table(t)->Sel([gt(test.t1.a, test.t2.a)])->HashAgg)->HashAgg->Sel([not(isnull(count(*)))])}->StreamAgg",
		},
		{
			sql:  "select /*+ HASH_AGG(@sel_2) */ a, (select count(*) from t t1 where t1.b > t.a) from t where b > (select b from t t2 where t2.b = t.a limit 1)",
			plan: "Apply{Apply{TableReader(Table(t))->TableReader(Table(t)->Sel([eq(test.t2.b, test.t.a)])->Limit)->Limit}->TableReader(Table(t)->Sel([gt(test.t1.b, test.t.a)])->HashAgg)->HashAgg}->Projection",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql: %s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := planner.Optimize(ctx, se, stmt, s.is)
		c.Assert(err, IsNil, comment)

		c.Assert(core.ToString(p), Equals, tt.plan, comment)
	}
}

func (s *testPlanSuite) TestDAGPlanBuilderSplitAvg(c *C) {
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
		plan string
	}{
		{
			sql:  "select avg(a),avg(b),avg(c) from t",
			plan: "TableReader(Table(t)->StreamAgg)->StreamAgg",
		},
		{
			sql:  "select /*+ HASH_AGG() */ avg(a),avg(b),avg(c) from t",
			plan: "TableReader(Table(t)->HashAgg)->HashAgg",
		},
	}

	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		core.Preprocess(se, stmt, s.is)
		p, err := planner.Optimize(context.TODO(), se, stmt, s.is)
		c.Assert(err, IsNil, comment)

		c.Assert(core.ToString(p), Equals, tt.plan, comment)
		root, ok := p.(core.PhysicalPlan)
		if !ok {
			continue
		}
		testDAGPlanBuilderSplitAvg(c, root)
	}
}

func testDAGPlanBuilderSplitAvg(c *C, root core.PhysicalPlan) {
	if p, ok := root.(*core.PhysicalTableReader); ok {
		if p.TablePlans != nil {
			baseAgg := p.TablePlans[len(p.TablePlans)-1]
			if agg, ok := baseAgg.(*core.PhysicalHashAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					c.Assert(agg.Schema().Columns[i].RetType, Equals, aggfunc.RetTp)
				}
			}
			if agg, ok := baseAgg.(*core.PhysicalStreamAgg); ok {
				for i, aggfunc := range agg.AggFuncs {
					c.Assert(agg.Schema().Columns[i].RetType, Equals, aggfunc.RetTp)
				}
			}
		}
	}

	childs := root.Children()
	if childs == nil {
		return
	}
	for _, son := range childs {
		testDAGPlanBuilderSplitAvg(c, son)
	}
}
