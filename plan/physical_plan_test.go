// Copyright 2016 PingCAP, Inc.
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

package plan

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testPlanSuite) TestPushDownAggregation(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql       string
		best      string
		aggFuns   string
		aggFields string
		gbyItems  string
	}{
		{
			sql:       "select count(*) from t",
			best:      "Table(t)->HashAgg->Projection",
			aggFuns:   "[count(1)]",
			aggFields: "[blob bigint(21)]",
			gbyItems:  "[]",
		},
		{
			sql:       "select distinct a,b from t",
			best:      "Table(t)->HashAgg",
			aggFuns:   "[firstrow(test.t.a) firstrow(test.t.b)]",
			aggFields: "[blob int int]",
			gbyItems:  "[test.t.a test.t.b]",
		},
		{
			sql:       "select sum(b) from t group by c",
			best:      "Table(t)->HashAgg->Projection",
			aggFuns:   "[sum(test.t.b)]",
			aggFields: "[blob decimal(23) BINARY]",
			gbyItems:  "[test.t.c]",
		},
		{
			sql:       "select max(b + c), min(case when b then 1 else 2 end) from t group by d + e, a",
			best:      "Table(t)->HashAgg->Projection",
			aggFuns:   "[max(plus(test.t.b, test.t.c)) min(case(test.t.b, 1, 2))]",
			aggFields: "[blob bigint BINARY bigint(1,0) BINARY]",
			gbyItems:  "[plus(test.t.d, test.t.e) test.t.a]",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)
		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		lp.PruneColumns(lp.Schema().Columns)
		solver := &aggregationOptimizer{builder.allocator, builder.ctx}
		solver.aggPushDown(lp)
		lp.ResolveIndices()
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		c.Assert(err, IsNil)
		c.Assert(ToString(info.p), Equals, tt.best, Commentf("for %s", tt.sql))
		p = info.p
		for {
			var ts *physicalTableSource
			switch x := p.(type) {
			case *PhysicalTableScan:
				ts = &x.physicalTableSource
			case *PhysicalIndexScan:
				ts = &x.physicalTableSource
			}
			if ts != nil {
				c.Assert(fmt.Sprintf("%s", ts.aggFuncs), Equals, tt.aggFuns, Commentf("for %s", tt.sql))
				c.Assert(fmt.Sprintf("%s", ts.gbyItems), Equals, tt.gbyItems, Commentf("for %s", tt.sql))
				fields := make([]*types.FieldType, ts.schema.Len())
				for i, col := range ts.schema.Columns {
					fields[i] = col.RetType
				}
				c.Assert(fmt.Sprintf("%s", fields), Equals, tt.aggFields, Commentf("for %s", tt.sql))
				break
			}
			p = p.Children()[0]
		}
	}
}

func (s *testPlanSuite) TestPushDownOrderByAndLimit(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql          string
		best         string
		orderByItmes string
		limit        string
	}{
		{
			sql:          "select * from t order by a limit 5",
			best:         "Table(t)->Limit",
			orderByItmes: "[]",
			limit:        "5",
		},
		{
			sql:          "select * from t where a < 1 limit 1, 1",
			best:         "Table(t)->Limit",
			orderByItmes: "[]",
			limit:        "2",
		},
		{
			sql:          "select * from t limit 5",
			best:         "Table(t)->Limit",
			orderByItmes: "[]",
			limit:        "5",
		},
		{
			sql:          "select * from t order by 1 limit 5",
			best:         "Table(t)->Limit",
			orderByItmes: "[]",
			limit:        "5",
		},
		{
			sql:          "select c from t order by c limit 5",
			best:         "Index(t.c_d_e)[[<nil>,+inf]]->Limit",
			orderByItmes: "[]",
			limit:        "5",
		},
		{
			sql:          "select * from t order by d limit 1",
			best:         "Table(t)->Sort + Limit(1) + Offset(0)",
			orderByItmes: "[test.t.d]",
			limit:        "1",
		},
		{
			sql:          "select * from t where c > 0 order by d limit 1",
			best:         "Index(t.c_d_e)[(0 +inf,+inf +inf]]->Sort + Limit(1) + Offset(0)",
			orderByItmes: "[test.t.d]",
			limit:        "1",
		},
		{
			sql:          "select * from t a where a.c < 10000 and a.d in (1000, a.e) order by a.b limit 2",
			best:         "Index(t.c_d_e)[[-inf <nil>,10000 <nil>)]->Selection->Sort + Limit(2) + Offset(0)",
			orderByItmes: "[]",
			limit:        "nil",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)
		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)
		p, err = doOptimize(builder.optFlag, lp, builder.ctx, builder.allocator)
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
		for {
			var ts *physicalTableSource
			switch x := p.(type) {
			case *PhysicalTableScan:
				ts = &x.physicalTableSource
			case *PhysicalIndexScan:
				ts = &x.physicalTableSource
			}
			if ts != nil {
				c.Assert(fmt.Sprintf("%s", ts.sortItems), Equals, tt.orderByItmes, Commentf("for %s", tt.sql))
				var limitStr string
				if ts.LimitCount == nil {
					limitStr = fmt.Sprint("nil")
				} else {
					limitStr = fmt.Sprintf("%d", *ts.LimitCount)
				}
				c.Assert(limitStr, Equals, tt.limit, Commentf("for %s", tt.sql))
				break
			}
			p = p.Children()[0]
		}
	}
}

// TestPushDownExpression tests whether expressions have been pushed down successfully.
func (s *testPlanSuite) TestPushDownExpression(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		cond string // readable expressions.
	}{
		{
			sql:  "a and b",
			cond: "test.t.b",
		},
		{
			sql:  "a or (b and c)",
			cond: "or(test.t.a, and(test.t.b, test.t.c))",
		},
		{
			sql:  "c=1 and d =1 and e =1 and b=1",
			cond: "eq(test.t.b, 1)",
		},
		{
			sql:  "a or b",
			cond: "or(test.t.a, test.t.b)",
		},
		{
			sql:  "a and (b or c)",
			cond: "or(test.t.b, test.t.c)",
		},
		{
			sql:  "not a",
			cond: "not(test.t.a)",
		},
		{
			sql:  "a xor b",
			cond: "xor(test.t.a, test.t.b)",
		},
		{
			sql:  "a & b",
			cond: "bitand(test.t.a, test.t.b)",
		},
		{
			sql:  "a | b",
			cond: "bitor(test.t.a, test.t.b)",
		},
		{
			sql:  "a ^ b",
			cond: "bitxor(test.t.a, test.t.b)",
		},
		{
			sql:  "~a",
			cond: "bitneg(test.t.a)",
		},
		{
			sql:  "a = case a when b then 1 when a then 0 end",
			cond: "eq(test.t.a, case(eq(test.t.a, test.t.b), 1, eq(test.t.a, test.t.a), 0))",
		},
		// if
		{
			sql:  "a = if(a, 1, 0)",
			cond: "eq(test.t.a, if(test.t.a, 1, 0))",
		},
		// nullif
		{
			sql:  "a = nullif(a, 1)",
			cond: "eq(test.t.a, nullif(test.t.a, 1))",
		},
		// ifnull
		// TODO: ifnull(null, a) will be wrapped with cast which can not be pushed down.
		//{
		//	sql:  "a = ifnull(null, a)",
		//	cond: "eq(test.t.a, ifnull(<nil>, test.t.a))",
		//},
		// coalesce
		{
			sql:  "a = coalesce(null, null, a, b)",
			cond: "eq(test.t.a, coalesce(<nil>, <nil>, test.t.a, test.t.b))",
		},
		// isnull
		{
			sql:  "b is null",
			cond: "isnull(test.t.b)",
		},
	}
	for _, tt := range tests {
		sql := "select * from t where " + tt.sql
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)
		err = expression.InferType(mockContext().GetSessionVars().StmtCtx, stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		lp.PruneColumns(lp.Schema().Columns)
		lp.ResolveIndices()
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		c.Assert(err, IsNil)
		p = info.p
		for {
			var ts *physicalTableSource
			switch x := p.(type) {
			case *PhysicalTableScan:
				ts = &x.physicalTableSource
			case *PhysicalIndexScan:
				ts = &x.physicalTableSource
			}
			if ts != nil {
				conditions := append(ts.indexFilterConditions, ts.tableFilterConditions...)
				c.Assert(fmt.Sprintf("%s", expression.ComposeCNFCondition(mock.NewContext(), conditions...).String()), Equals, tt.cond, Commentf("for %s", sql))
				break
			}
			p = p.Children()[0]
		}
	}
}

func (s *testPlanSuite) TestCBO(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t t1 use index(e)",
			best: "Table(t)",
		},
		{
			sql:  "select a from t where a between 1 and 2 order by c",
			best: "Table(t)->Sort->Projection",
		},
		{
			sql:  "select * from t t1 use index(c_d_e)",
			best: "Index(t.c_d_e)[[<nil>,+inf]]",
		},
		{
			sql:  "select * from t where (t.c > 0 and t.c < 1) or (t.c > 2 and t.c < 3) or (t.c > 4 and t.c < 5) or (t.c > 6 and t.c < 7) or (t.c > 9 and t.c < 10)",
			best: "Index(t.c_d_e)[(0 +inf,1 <nil>) (2 +inf,3 <nil>) (4 +inf,5 <nil>) (6 +inf,7 <nil>) (9 +inf,10 <nil>)]",
		},
		{
			sql:  "select sum(t.a) from t where t.c in (1,2) and t.d in (1,3) group by t.d order by t.d",
			best: "Index(t.c_d_e)[[1 1,1 1] [1 3,1 3] [2 1,2 1] [2 3,2 3]]->HashAgg->Sort->Projection",
		},
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 order by t.a limit 1",
			best: "Index(t.c_d_e)[[1,1]]->Sort + Limit(1) + Offset(0)",
		},
		{
			sql:  "select * from t where t.c = 1 order by t.f limit 1",
			best: "Index(t.c_d_e)[[1,1]]->Sort + Limit(1) + Offset(0)",
		},
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 order by t.f limit 1",
			best: "Index(t.c_d_e)[[1,1]]->Sort + Limit(1) + Offset(0)",
		},
		{
			sql:  "select * from t where t.c = 1 and t.e = 1 and t.f = 1 order by t.f limit 1",
			best: "Index(t.c_d_e)[[1,1]]->Sort + Limit(1) + Offset(0)",
		},
		{
			sql:  "select * from t t1 ignore index(e) where c < 0",
			best: "Index(t.c_d_e)[[-inf <nil>,0 <nil>)]",
		},
		{
			sql:  "select * from t t1 ignore index(c_d_e) where c < 0",
			best: "Table(t)",
		},
		{
			sql:  "select * from t where f in (1,2) and g in(1,2,3,4,5)",
			best: "Index(t.f_g)[[1 1,1 1] [1 2,1 2] [1 3,1 3] [1 4,1 4] [1 5,1 5] [2 1,2 1] [2 2,2 2] [2 3,2 3] [2 4,2 4] [2 5,2 5]]",
		},
		{
			sql:  "select * from t t1 where 1 = 0",
			best: "Dual",
		},
		{
			sql:  "select * from t t1 where c in (1,2,3,4,5,6,7,8,9,0)",
			best: "Index(t.c_d_e)[[0,0] [1,1] [2,2] [3,3] [4,4] [5,5] [6,6] [7,7] [8,8] [9,9]]",
		},
		{
			sql:  "select * from t t1 where a in (1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9)",
			best: "Table(t)",
		},
		{
			sql:  "select count(*) from t t1 having 1 = 0",
			best: "Dual->HashAgg->Selection",
		},
		{
			sql:  "select sum(a.b), sum(b.b) from t a join t b on a.c = b.c group by a.d order by a.d",
			best: "LeftHashJoin{Table(t)->Table(t)}(a.c,b.c)->HashAgg->Sort->Projection",
		},
		{
			sql:  "select * from t t1 left outer join t t2 on true where least(1,2,3,t1.a,t2.b) > 0 order by t2.a limit 10",
			best: "LeftHashJoin{Table(t)->Table(t)}->Selection->Sort + Limit(10) + Offset(0)",
		},
		{
			sql:  "select * from t t1 left outer join t t2 on true where least(1,2,3,t1.a,t2.b) > 0 limit 10",
			best: "LeftHashJoin{Table(t)->Table(t)}->Selection->Limit",
		},
		{
			sql:  "select count(*) from t where concat(a,b) = 'abc' group by c",
			best: "Index(t.c_d_e)[[<nil>,+inf]]->Selection->StreamAgg",
		},
		{
			sql:  "select sum(b.a) from t a, t b where a.c = b.c and cast(b.d as char) group by b.d",
			best: "RightHashJoin{Index(t.c_d_e)[[<nil>,+inf]]->Selection->StreamAgg->Table(t)}(b.c,a.c)->HashAgg",
		},
		{
			sql:  "select count(*) from t group by e order by d limit 1",
			best: "Table(t)->HashAgg->Sort + Limit(1) + Offset(0)->Projection",
		},
		{
			sql:  "select count(*) from t where concat(a,b) = 'abc' group by c",
			best: "Index(t.c_d_e)[[<nil>,+inf]]->Selection->StreamAgg",
		},
		{
			sql:  "select count(*) from t where concat(a,b) = 'abc' group by a order by a",
			best: "Table(t)->Selection->Projection->Projection",
		},
		{
			sql:  "select count(distinct e) from t where c = 1 and concat(c,d) = 'abc' group by d",
			best: "Index(t.c_d_e)[[1,1]]->Selection->StreamAgg",
		},
		{
			sql:  "select count(distinct e) from t group by d",
			best: "Table(t)->HashAgg",
		},
		{
			// Multi distinct column can't apply stream agg.
			sql:  "select count(distinct e), sum(distinct c) from t where c = 1 group by d",
			best: "Index(t.c_d_e)[[1,1]]->StreamAgg",
		},
		{
			sql:  "select * from t a where a.c = 1 order by a.d limit 2",
			best: "Index(t.c_d_e)[[1,1]]",
		},
		{
			sql:  "select * from t a order by a.c desc limit 2",
			best: "Index(t.c_d_e)[[<nil>,+inf]]->Limit",
		},
		{
			sql:  "select * from t t1, t t2 right join t t3 on t2.a = t3.b order by t1.a, t1.b, t2.a, t2.b, t3.a, t3.b",
			best: "RightHashJoin{Table(t)->RightHashJoin{Table(t)->Table(t)}(t2.a,t3.b)}->Sort",
		},
		{
			sql:  "select * from t a where 1 = a.c and a.d > 1 order by a.d desc limit 2",
			best: "Index(t.c_d_e)[(1 1 +inf,1 +inf +inf]]",
		},
		{
			sql:  "select * from t a where a.c < 10000 order by a.a limit 2",
			best: "Table(t)",
		},
		{
			sql:  "select * from t a where a.c < 10000 and a.d in (1000, a.e) order by a.a limit 2",
			best: "Index(t.c_d_e)[[-inf <nil>,10000 <nil>)]->Selection->Sort + Limit(2) + Offset(0)",
		},
		{
			sql:  "select * from (select * from t) a left outer join (select * from t) b on 1 order by a.c",
			best: "LeftHashJoin{Index(t.c_d_e)[[<nil>,+inf]]->Table(t)}",
		},
		{
			sql:  "select * from (select * from t) a left outer join (select * from t) b on 1 order by b.c",
			best: "LeftHashJoin{Table(t)->Table(t)}->Sort",
		},
		{
			sql:  "select * from (select * from t) a right outer join (select * from t) b on 1 order by a.c",
			best: "RightHashJoin{Table(t)->Table(t)}->Sort",
		},
		{
			sql:  "select * from (select * from t) a right outer join (select * from t) b on 1 order by b.c",
			best: "RightHashJoin{Table(t)->Index(t.c_d_e)[[<nil>,+inf]]}",
		},
		{
			sql:  "select * from t a where exists(select * from t b where a.a = b.a) and a.c = 1 order by a.d limit 3",
			best: "SemiJoin{Index(t.c_d_e)[[1,1]]->Table(t)}->Limit",
		},
		{
			sql:  "select exists(select * from t b where a.a = b.a and b.c = 1) from t a order by a.c limit 3",
			best: "SemiJoinWithAux{Index(t.c_d_e)[[<nil>,+inf]]->Limit->Index(t.c_d_e)[[1,1]]}->Projection->Projection",
		},
		{
			sql:  "select * from (select t.a from t union select t.d from t where t.c = 1 union select t.c from t) k order by a limit 1",
			best: "UnionAll{Table(t)->Projection->Index(t.c_d_e)[[1,1]]->HashAgg->Table(t)->HashAgg}->HashAgg->Sort + Limit(1) + Offset(0)",
		},
		{
			sql:  "select * from (select t.a from t union all select t.d from t where t.c = 1 union all select t.c from t) k order by a limit 1",
			best: "UnionAll{Table(t)->Limit->Index(t.c_d_e)[[1,1]]->Projection->Index(t.c_d_e)[[<nil>,+inf]]->Limit}->Sort + Limit(1) + Offset(0)",
		},
		{
			sql:  "select * from (select t.a from t union select t.d from t union select t.c from t) k order by a limit 1",
			best: "UnionAll{Table(t)->Projection->Table(t)->HashAgg->Table(t)->HashAgg}->HashAgg->Sort + Limit(1) + Offset(0)",
		},
		{
			sql:  "select t.c from t where 0 = (select count(b) from t t1 where t.a = t1.b)",
			best: "LeftHashJoin{Table(t)->Table(t)->HashAgg}(test.t.a,t1.b)->Projection->Selection->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)
		lp, err = logicalOptimize(builder.optFlag, lp, builder.ctx, builder.allocator)
		lp.ResolveIndices()
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		c.Assert(err, IsNil)
		c.Assert(ToString(EliminateProjection(info.p)), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestProjectionElimination(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		ans string
	}{
		// projection can be eliminated in following cases.
		{
			sql: "select a from t",
			ans: "Table(t)",
		},
		{
			sql: "select a from t where a > 1",
			ans: "Table(t)",
		},
		{
			sql: "select a from t where a is null",
			ans: "Table(t)",
		},
		{
			sql: "select a, b from t where b > 0",
			ans: "Table(t)",
		},
		{
			sql: "select a as c1, b as c2 from t where a = 3",
			ans: "Table(t)",
		},
		{
			sql: "select a as c1, b as c2 from t as t1 where t1.a = 0",
			ans: "Table(t)",
		},
		{
			sql: "select a from t where exists(select 1 from t as x where x.a < t.a)",
			ans: "SemiJoin{Table(t)->Table(t)}",
		},
		{
			sql: "select a from (select d as a from t where d = 0) k where k.a = 5",
			ans: "Dual",
		},
		{
			sql: "select t1.a from t t1 where t1.a in (select t2.a from t t2 where t2.a > 1)",
			ans: "SemiJoin{Table(t)->Table(t)}",
		},
		{
			sql: "select t1.a, t2.b from t t1, t t2 where t1.a > 0 and t2.b < 0",
			ans: "RightHashJoin{Table(t)->Table(t)}",
		},
		{
			sql: "select t1.a, t1.b, t2.a, t2.b from t t1, t t2 where t1.a > 0 and t2.b < 0",
			ans: "RightHashJoin{Table(t)->Table(t)}",
		},
		{
			sql: "select * from (t t1 join t t2) join (t t3 join t t4)",
			ans: "LeftHashJoin{LeftHashJoin{Table(t)->Table(t)}->LeftHashJoin{Table(t)->Table(t)}}",
		},
		// projection can not be eliminated in following cases.
		{
			sql: "select t1.b, t1.a, t2.b, t2.a from t t1, t t2 where t1.a > 0 and t2.b < 0",
			ans: "RightHashJoin{Table(t)->Table(t)}->Projection",
		},
		{
			sql: "select d, c, b, a from t where a = b and b = 1",
			ans: "Table(t)->Projection",
		},
		{
			sql: "select d as a, b as c from t as t1 where d > 0 and b < 0",
			ans: "Table(t)->Projection",
		},
		{
			sql: "select c as a, c as b from t",
			ans: "Table(t)->Projection",
		},
		{
			sql: "select c as a, c as b from t where d > 0",
			ans: "Table(t)->Projection",
		},
		{
			sql: "select t1.a, t2.b, t2.a, t1.b from t t1, t t2 where t1.a > 0 and t2.b < 0",
			ans: "RightHashJoin{Table(t)->Table(t)}->Projection",
		},
		{
			sql: "select t1.a from t t1 where t1.a in (select t2.a from t t2 where t1.a > 1)",
			ans: "SemiJoin{Table(t)->Table(t)}",
		},
		{
			sql: "select t1.a from t t1, (select @a:=0, @b:=0) t2",
			ans: "LeftHashJoin{Table(t)->Dual->Projection}->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp, err := logicalOptimize(flagPredicatePushDown|flagPrunColumns|flagDecorrelate, p.(LogicalPlan), builder.ctx, builder.allocator)
		lp.ResolveIndices()
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		p = EliminateProjection(info.p)
		c.Assert(ToString(p), Equals, tt.ans, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestCoveringIndex(c *C) {
	tests := []struct {
		columnNames []string
		indexNames  []string
		indexLens   []int
		isCovering  bool
	}{
		{[]string{"a"}, []string{"a"}, []int{-1}, true},
		{[]string{"a"}, []string{"a", "b"}, []int{-1, -1}, true},
		{[]string{"a", "b"}, []string{"b", "a"}, []int{-1, -1}, true},
		{[]string{"a", "b"}, []string{"b", "c"}, []int{-1, -1}, false},
		{[]string{"a", "b"}, []string{"a", "b"}, []int{50, -1}, false},
		{[]string{"a", "b"}, []string{"a", "c"}, []int{-1, -1}, false},
		{[]string{"id", "a"}, []string{"a", "b"}, []int{-1, -1}, true},
	}
	for _, tt := range tests {
		var columns []*model.ColumnInfo
		var pkIsHandle bool
		for _, cn := range tt.columnNames {
			col := &model.ColumnInfo{Name: model.NewCIStr(cn)}
			if cn == "id" {
				pkIsHandle = true
				col.Flag = mysql.PriKeyFlag
			}
			columns = append(columns, col)
		}
		var indexCols []*model.IndexColumn
		for i := range tt.indexNames {
			icn := tt.indexNames[i]
			icl := tt.indexLens[i]
			indexCols = append(indexCols, &model.IndexColumn{Name: model.NewCIStr(icn), Length: icl})
		}
		covering := isCoveringIndex(columns, indexCols, pkIsHandle)
		c.Assert(covering, Equals, tt.isCovering)
	}
}

func (s *testPlanSuite) TestFilterConditionPushDown(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql         string
		access      string
		indexFilter string
		tableFilter string
	}{
		{
			sql:         "select * from t",
			access:      "[]",
			indexFilter: "[]",
			tableFilter: "[]",
		},
		{
			sql:         "select * from t where t.c < 10000 and t.d = 1 and t.g > 1",
			access:      "[lt(test.t.c, 10000)]",
			indexFilter: "[eq(test.t.d, 1)]",
			tableFilter: "[gt(test.t.g, 1)]",
		},
		{
			sql:         "select * from t where t.a < 1 and t.c < t.d",
			access:      "[lt(test.t.a, 1)]",
			indexFilter: "[]",
			tableFilter: "[lt(test.t.c, test.t.d)]",
		},
		{
			sql:         "select * from t use index(c_d_e) where t.a < 1 and t.c =1 and t.d < t.e and t.b > (t.a - t.d)",
			access:      "[eq(test.t.c, 1)]",
			indexFilter: "[lt(test.t.a, 1) lt(test.t.d, test.t.e)]",
			tableFilter: "[gt(test.t.b, minus(test.t.a, test.t.d))]",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)
		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		lp.PruneColumns(lp.Schema().Columns)
		lp.ResolveIndices()
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		c.Assert(err, IsNil)
		p = info.p
		for {
			var ts *physicalTableSource
			switch x := p.(type) {
			case *PhysicalTableScan:
				ts = &x.physicalTableSource
			case *PhysicalIndexScan:
				ts = &x.physicalTableSource
			}
			if ts != nil {
				c.Assert(fmt.Sprintf("%s", ts.AccessCondition), Equals, tt.access, Commentf("for %s", tt.sql))
				c.Assert(fmt.Sprintf("%s", ts.indexFilterConditions), Equals, tt.indexFilter, Commentf("for %s", tt.sql))
				c.Assert(fmt.Sprintf("%s", ts.tableFilterConditions), Equals, tt.tableFilter, Commentf("for %s", tt.sql))
				break
			}
			p = p.Children()[0]
		}
	}
}

func (s *testPlanSuite) TestAddCache(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		ans string
	}{
		{
			sql: "select * from t t1 where t1.a=(select min(t2.a) from t t2, t t3 where t2.a=t3.a and t2.b > t1.b + t3.b)",
			ans: "Apply{Table(t)->LeftHashJoin{Table(t)->Cache->Table(t)->Cache}(t2.a,t3.a)->StreamAgg->MaxOneRow}->Selection->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)
		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		lp.PruneColumns(lp.Schema().Columns)
		lp.ResolveIndices()
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		pp := info.p
		pp = EliminateProjection(pp)
		addCachePlan(pp, builder.allocator)
		c.Assert(ToString(pp), Equals, tt.ans, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestScanController(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		ans string
	}{
		{
			sql: "select (select count(1) k from t s where s.a = t.a having k != 0) from t",
			ans: "Apply{Table(t)->Table(t)->Selection->StreamAgg}->Projection",
		},
		{
			sql: "select (select count(1) k from t s where s.b = t.b having k != 0) from t",
			ans: "Apply{Table(t)->Table(t)->Cache->Selection->StreamAgg}->Projection",
		},
		{
			sql: "select (select count(1) k from t s where s.f = t.f having k != 0) from t",
			ans: "Apply{Table(t)->Index(t.f)[]->Selection->StreamAgg}->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)
		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		lp.PruneColumns(lp.Schema().Columns)
		dSolver := &decorrelateSolver{}
		lp, err = dSolver.optimize(lp, mockContext(), new(idAllocator))
		c.Assert(err, IsNil)
		lp.ResolveIndices()
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		pp := info.p
		pp = EliminateProjection(pp)
		addCachePlan(pp, builder.allocator)
		c.Assert(ToString(pp), Equals, tt.ans, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestJoinAlgorithm(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		ans string
	}{
		{
			sql: "select * from t t1 join t t2 on t1.a = t2.a",
			ans: "LeftHashJoin{Table(t)->Table(t)}(t1.a,t2.a)",
		},
		{
			sql: "select /*+ tidb_smj(t1, t2) */ * from t t1 join t t2 on t1.a = t2.a",
			ans: "MergeJoin{Table(t)->Table(t)}(t1.a,t2.a)",
		},
		{
			sql: "select /*+ tidb_smj(t1) */ * from t t1 join t t2 on t1.a = t2.a",
			ans: "MergeJoin{Table(t)->Table(t)}(t1.a,t2.a)",
		},
		{
			sql: "select /*+ tidb_inlj(t2) */ * from t t1 join t t2 on t1.a = t2.a",
			ans: "Apply{Table(t)->Selection->Table(t)}",
		},
		{
			sql: "select /*+ TIDB_SMJ(t1, t2) */ * from t t1 join t t2 on t1.a > t2.a",
			ans: "LeftHashJoin{Table(t)->Table(t)}",
		},
		{
			sql: "select /*+ TIDB_INLJ(t1, t2) */ * from t t1 join t t2 on t1.a > t2.a",
			ans: "LeftHashJoin{Table(t)->Table(t)}",
		},
		{
			sql: "select /*+ tidb_inlj(t1, t2) */ * from t t1 right outer join t t2 on t1.a = t2.c",
			ans: "Apply{Table(t)->Selection->Table(t)}",
		},
		{
			sql: "select /*+ tidb_inlj(t1, t2) */ * from t t1 right outer join t t2 on t1.a > t2.c",
			ans: "RightHashJoin{Table(t)->Table(t)}",
		},
		{
			sql: "select /*+ tidb_inlj(t1, t2) */ * from t t1 left outer join t t2 on t1.a = t2.e",
			ans: "LeftHashJoin{Table(t)->Table(t)}(t1.a,t2.e)",
		},
		{
			sql: "select /*+ tidb_inlj(t1, t2) */ * from t t1 left outer join t t2 on t1.a = t2.c",
			ans: "Apply{Table(t)->Index(t.c_d_e)[]->Selection}",
		},
		{
			sql: "select /*+ tidb_inlj(t, tt) */ * from t tt join t on tt.a=t.f and tt.f>1",
			ans: "Apply{Index(t.f)[(1,+inf]]->Index(t.f)[]->Selection}",
		},
		{
			sql: "select /*+ tidb_inlj(t, tt) */ * from t tt join t on tt.a>t.f",
			ans: "LeftHashJoin{Table(t)->Table(t)}",
		},
		{
			sql: "select /*+ tidb_inlj(t2) */ * from t t1 join t t2 on t1.c=t2.c and t1.d=t2.d and t1.e > t2.e",
			ans: "Apply{Index(t.c_d_e)[]->Selection->Table(t)}",
		},
		{
			sql: "select /*+ TIDB_INLJ(t1) */ * from t join t t1 where t.a=t1.a and t.b > 100 and t1.b>10",
			ans: "Apply{Table(t)->Selection->Table(t)}",
		},
		{
			sql: "select /*+ TIDB_INLJ(tt, t1) */ * from (select * from t where t.b > 100) tt left join t t1 on tt.a=t1.a and t1.b>10",
			ans: "Apply{Table(t)->Table(t)->Selection}",
		},
		{
			sql: "select /*+ TIDB_INLJ(t, t1) */ * from t left join (select * from t where t.b > 10) t1 on t.a=t1.a and t.b > 100",
			ans: "LeftHashJoin{Table(t)->Table(t)}(test.t.a,t1.a)",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		pp, err := doOptimize(builder.optFlag, p.(LogicalPlan), builder.ctx, builder.allocator)
		c.Assert(err, IsNil)
		c.Assert(ToString(pp), Equals, tt.ans, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestAutoJoinChosen(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql         string
		ans         string
		genStatsTbl bool
	}{
		{
			sql: "select * from (select * from t limit 0, 128) t1 join t t2 on t1.a = t2.a",
			ans: "Apply{Table(t)->Limit->Table(t)->Selection}",
		},
		{
			sql: "select * from (select * from t limit 0, 129) t1 join t t2 on t1.a = t2.a",
			ans: "RightHashJoin{Table(t)->Limit->Table(t)}(t1.a,t2.a)",
		},
		{
			sql: "select * from (select * from t limit 0, 10 union select * from t limit 10, 100) t1 join t t2 on t1.a = t2.a",
			ans: "Apply{UnionAll{Table(t)->Limit->Projection->Table(t)->Limit->Projection}->HashAgg->Table(t)->Selection}",
		},
		{
			sql: "select * from (select * from t limit 0, 29 union all select * from t limit 0, 100) t1 join t t2 on t1.a = t2.a",
			ans: "RightHashJoin{UnionAll{Table(t)->Limit->Table(t)->Limit}->Table(t)}(t1.a,t2.a)",
		},
		{
			sql:         "select * from t t1 join t t2 on t1.f = t2.f and t1.a < 5",
			ans:         "Apply{Table(t)->Index(t.f)[]->Selection}",
			genStatsTbl: true,
		},
		{
			sql:         "select * from t t1 join t t2 on t1.f = t2.f and t1.a < 19",
			ans:         "RightHashJoin{Table(t)->Table(t)}(t1.f,t2.f)",
			genStatsTbl: true,
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		is, err := MockResolve(stmt)
		c.Assert(err, IsNil)

		ctx := mockContext()

		if ca.genStatsTbl {
			handle := sessionctx.GetDomain(ctx).StatsHandle()
			tb, _ := is.TableByID(0)
			tbl := tb.Meta()
			// generate 40 distinct values for pk.
			pkValues := make([]types.Datum, 40)
			for i := 0; i < 40; i++ {
				pkValues[i] = types.NewIntDatum(int64(i))
			}
			// make the statistic col info for pk, every distinct value occurs 10 times.
			pkStatsCol := &statistics.Column{Histogram: *mockStatsHistogram(1, pkValues, 10)}
			// mock the statistic table and set the value of pk column.
			statsTbl := mockStatsTable(tbl, 400)
			statsTbl.Columns[1] = pkStatsCol
			handle.UpdateTableStats([]*statistics.Table{statsTbl}, nil)
		}

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       ctx,
			colMapper: make(map[*ast.ColumnNameExpr]int),
			is:        is,
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		pp, err := doOptimize(builder.optFlag, p.(LogicalPlan), builder.ctx, builder.allocator)
		c.Assert(err, IsNil)
		c.Assert(ToString(pp), Equals, ca.ans, Commentf("for %s", ca.sql))

		if ca.genStatsTbl {
			sessionctx.GetDomain(ctx).StatsHandle().Clear()
		}
	}
}

func (s *testPlanSuite) TestIssue3337(c *C) {
	defer testleak.AfterTest(c)()
	is := infoschema.MockInfoSchema([]*model.TableInfo{MockTable()})
	tb, _ := is.TableByID(0)
	tbl := tb.Meta()
	statsTbl := mockStatsTable(tbl, 0)
	ran := ranger.FullIndexRange()
	rowCount, err := statsTbl.GetRowCountByIndexRanges(new(variable.StatementContext), 1, ran)
	c.Assert(err, IsNil)
	c.Assert(rowCount, Equals, float64(0))
}
