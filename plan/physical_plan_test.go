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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testPlanSuite) TestPushDownOrderbyAndLimit(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql          string
		best         string
		orderByItmes string
		limit        string
	}{
		{
			sql:          "select * from t order by a limit 5",
			best:         "Table(t)->Limit->Projection",
			orderByItmes: "[]",
			limit:        "5",
		},
		{
			sql:          "select * from t limit 5",
			best:         "Table(t)->Limit->Projection",
			orderByItmes: "[]",
			limit:        "5",
		},
		{
			sql:          "select c from t order by c limit 5",
			best:         "Index(t.c_d_e)[[<nil>,+inf]]->Limit->Projection",
			orderByItmes: "[]",
			limit:        "5",
		},
		{
			sql:          "select * from t order by d limit 1",
			best:         "Table(t)->Sort + Limit(1) + Offset(0)->Projection",
			orderByItmes: "[(test.t.d, false)]",
			limit:        "1",
		},
		{
			sql:          "select * from t where c > 0 order by d limit 1",
			best:         "Index(t.c_d_e)[(0,+inf]]->Sort + Limit(1) + Offset(0)->Projection",
			orderByItmes: "[(test.t.d, false)]",
			limit:        "1",
		},
		{
			sql:          "select * from t a where a.c < 10000 and a.d in (1000, a.e) order by a.b limit 2",
			best:         "Index(t.c_d_e)[[-inf,10000)]->Selection->Sort + Limit(2) + Offset(0)->Projection",
			orderByItmes: "[]",
			limit:        "nil",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)
		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = lp.PruneColumnsAndResolveIndices(lp.GetSchema())
		c.Assert(err, IsNil)
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		c.Assert(err, IsNil)
		c.Assert(ToString(info.p), Equals, ca.best, Commentf("for %s", ca.sql))
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
				c.Assert(fmt.Sprintf("%s", ts.sortItems), Equals, ca.orderByItmes, Commentf("for %s", ca.sql))
				var limitStr string
				if ts.LimitCount == nil {
					limitStr = fmt.Sprint("nil")
				} else {
					limitStr = fmt.Sprintf("%d", *ts.LimitCount)
				}
				c.Assert(limitStr, Equals, ca.limit, Commentf("for %s", ca.sql))
				break
			}
			p = p.GetChildByIndex(0)
		}
	}
}

// TestPushDownExpression tests whether expressions have been pushed down successfully.
func (s *testPlanSuite) TestPushDownExpression(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
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
		// coalesce
		{
			sql:  "a = coalesce(null, null, a, b)",
			cond: "eq(test.t.a, coalesce(<nil>, <nil>, test.t.a, test.t.b))",
		},
	}
	for _, ca := range cases {
		sql := "select * from t where " + ca.sql
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)
		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mockContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = lp.PruneColumnsAndResolveIndices(lp.GetSchema())
		c.Assert(err, IsNil)
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
				c.Assert(fmt.Sprintf("%s", expression.ComposeCNFCondition(ts.conditions).String()), Equals, ca.cond, Commentf("for %s", sql))
				break
			}
			p = p.GetChildByIndex(0)
		}
	}
}

func (s *testPlanSuite) TestCBO(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t t1 use index(e)",
			best: "Table(t)",
		},
		{
			sql:  "select * from t t1 use index(c_d_e)",
			best: "Index(t.c_d_e)[[<nil>,+inf]]",
		},
		{
			sql:  "select * from t where (t.c > 0 and t.c < 1) or (t.c > 2 and t.c < 3) or (t.c > 4 and t.c < 5) or (t.c > 6 and t.c < 7) or (t.c > 9 and t.c < 10)",
			best: "Index(t.c_d_e)[(0,1) (2,3) (4,5) (6,7) (9,10)]",
		},
		{
			sql:  "select sum(t.a) from t where t.c in (1,2) and t.d in (1,3) group by t.d order by t.d",
			best: "Index(t.c_d_e)[[1 1,1 1] [1 3,1 3] [2 1,2 1] [2 3,2 3]]->HashAgg->Sort->Trim",
		},
		{
			sql:  "select * from t t1 ignore index(e) where c < 0",
			best: "Index(t.c_d_e)[[-inf,0)]",
		},
		{
			sql:  "select * from t t1 ignore index(c_d_e) where c < 0",
			best: "Table(t)->Selection",
		},
		{
			sql:  "select * from t where f in (1,2) and g in(1,2,3,4,5)",
			best: "Index(t.f_g)[[1 1,1 1] [1 2,1 2] [1 3,1 3] [1 4,1 4] [1 5,1 5] [2 1,2 1] [2 2,2 2] [2 3,2 3] [2 4,2 4] [2 5,2 5]]",
		},
		{
			sql:  "select * from t t1 where 1 = 0",
			best: "Dummy",
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
			best: "Dummy->HashAgg->Selection",
		},
		{
			sql:  "select sum(a.b), sum(b.b) from t a join t b on a.c = b.c group by a.d order by a.d",
			best: "LeftHashJoin{Table(t)->Table(t)}(a.c,b.c)->HashAgg->Sort->Trim",
		},
		{
			sql:  "select count(*) from t group by c",
			best: "Index(t.c_d_e)[[<nil>,+inf]]->StreamAgg",
		},
		{
			sql:  "select count(*) from t group by e order by d limit 1",
			best: "Table(t)->HashAgg->Sort + Limit(1) + Offset(0)->Trim",
		},
		{
			sql:  "select count(*) from t group by a",
			best: "Table(t)->StreamAgg",
		},
		{
			sql:  "select count(*) from t group by a order by a",
			best: "Table(t)->StreamAgg->Trim",
		},
		{
			sql:  "select count(distinct e) from t where c = 1 group by d",
			best: "Index(t.c_d_e)[[1,1]]->StreamAgg",
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
			best: "Index(t.c_d_e)[(1 1,1 +inf]]",
		},
		{
			sql:  "select * from t a where a.c < 10000 order by a.a limit 2",
			best: "Index(t.c_d_e)[[-inf,10000)]->Sort + Limit(2) + Offset(0)",
		},
		{
			sql:  "select * from t a where a.c < 10000 and a.d in (1000, a.e) order by a.a limit 2",
			best: "Index(t.c_d_e)[[-inf,10000)]->Selection->Sort + Limit(2) + Offset(0)",
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
			best: "SemiJoinWithAux{Index(t.c_d_e)[[<nil>,+inf]]->Limit->Index(t.c_d_e)[[1,1]]}->Projection->Trim",
		},
		{
			sql:  "select * from (select t.a from t union select t.d from t where t.c = 1 union select t.c from t) k order by a limit 1",
			best: "UnionAll{Table(t)->Index(t.c_d_e)[[1,1]]->Projection->Index(t.c_d_e)[[<nil>,+inf]]}->Distinct->Limit",
		},
		{
			sql:  "select * from (select t.a from t union select t.d from t union select t.c from t) k order by a limit 1",
			best: "UnionAll{Table(t)->Table(t)->Table(t)}->Distinct->Sort + Limit(1) + Offset(0)",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mock.NewContext(),
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)

		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = lp.PruneColumnsAndResolveIndices(lp.GetSchema())
		c.Assert(err, IsNil)
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		c.Assert(err, IsNil)
		c.Assert(ToString(EliminateProjection(info.p)), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestProjectionElimination(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
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
			ans: "Table(t)->Selection",
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
			ans: "Dummy",
		},
		{
			sql: "select t1.a from t t1 where t1.a in (select t2.a from t t2 where t2.a > 1)",
			ans: "SemiJoin{Table(t)->Table(t)}",
		},
		{
			sql: "select t1.a, t2.b from t t1, t t2 where t1.a > 0 and t2.b < 0",
			ans: "RightHashJoin{Table(t)->Table(t)->Selection}",
		},
		{
			sql: "select t1.a, t1.b, t2.a, t2.b from t t1, t t2 where t1.a > 0 and t2.b < 0",
			ans: "RightHashJoin{Table(t)->Table(t)->Selection}",
		},
		{
			sql: "select * from (t t1 join t t2) join (t t3 join t t4)",
			ans: "LeftHashJoin{LeftHashJoin{Table(t)->Table(t)}->LeftHashJoin{Table(t)->Table(t)}}",
		},
		// projection can not be eliminated in following cases.
		{
			sql: "select t1.b, t1.a, t2.b, t2.a from t t1, t t2 where t1.a > 0 and t2.b < 0",
			ans: "RightHashJoin{Table(t)->Table(t)->Selection}->Projection",
		},
		{
			sql: "select d, c, b, a from t where a = b and b = 1",
			ans: "Table(t)->Selection->Projection",
		},
		{
			sql: "select d as a, b as c from t as t1 where d > 0 and b < 0",
			ans: "Table(t)->Selection->Projection",
		},
		{
			sql: "select c as a, c as b from t",
			ans: "Table(t)->Projection",
		},
		{
			sql: "select c as a, c as b from t where d > 0",
			ans: "Table(t)->Selection->Projection",
		},
		{
			sql: "select t1.a, t2.b, t2.a, t1.b from t t1, t t2 where t1.a > 0 and t2.b < 0",
			ans: "RightHashJoin{Table(t)->Table(t)->Selection}->Projection",
		},
		{
			sql: "select t1.a from t t1 where t1.a in (select t2.a from t t2 where t1.a > 1)",
			ans: "Table(t)->Apply(Table(t)->Selection->Projection)->Selection->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = mockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
			ctx:       mock.NewContext(),
		}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		lp := p.(LogicalPlan)
		_, lp, err = lp.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = lp.PruneColumnsAndResolveIndices(lp.GetSchema())
		c.Assert(err, IsNil)
		info, err := lp.convert2PhysicalPlan(&requiredProperty{})
		p = EliminateProjection(info.p)
		c.Assert(ToString(p), Equals, ca.ans, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestCoveringIndex(c *C) {
	cases := []struct {
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
	for _, ca := range cases {
		var columns []*model.ColumnInfo
		var pkIsHandle bool
		for _, cn := range ca.columnNames {
			col := &model.ColumnInfo{Name: model.NewCIStr(cn)}
			if cn == "id" {
				pkIsHandle = true
				col.Flag = mysql.PriKeyFlag
			}
			columns = append(columns, col)
		}
		var indexCols []*model.IndexColumn
		for i := range ca.indexNames {
			icn := ca.indexNames[i]
			icl := ca.indexLens[i]
			indexCols = append(indexCols, &model.IndexColumn{Name: model.NewCIStr(icn), Length: icl})
		}
		covering := isCoveringIndex(columns, indexCols, pkIsHandle)
		c.Assert(covering, Equals, ca.isCovering)
	}
}
