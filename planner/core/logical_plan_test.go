// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"fmt"
	"sort"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testPlanSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testPlanSuite struct {
	*parser.Parser

	is  infoschema.InfoSchema
	ctx sessionctx.Context
}

func (s *testPlanSuite) SetUpSuite(c *C) {
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{MockTable(), MockView()})
	s.ctx = MockContext()
	s.Parser = parser.New()
}

func (s *testPlanSuite) TestPredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select count(*) from t a, t b where a.a = b.a",
			best: "Join{DataScan(a)->DataScan(b)}(a.a,b.a)->Aggr(count(1))->Projection",
		},
		{
			sql:  "select a from (select a from t where d = 0) k where k.a = 5",
			best: "DataScan(t)->Projection->Projection",
		},
		{
			sql:  "select a from (select a+1 as a from t) k where k.a = 5",
			best: "DataScan(t)->Projection->Projection",
		},
		{
			sql:  "select a from (select 1+2 as a from t where d = 0) k where k.a = 5",
			best: "DataScan(t)->Projection->Projection",
		},
		{
			sql:  "select a from (select d as a from t where d = 0) k where k.a = 5",
			best: "DataScan(t)->Projection->Projection",
		},
		{
			sql:  "select * from t ta, t tb where (ta.d, ta.a) = (tb.b, tb.c)",
			best: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.b)(ta.a,tb.c)->Projection",
		},
		{
			sql:  "select * from t t1, t t2 where t1.a = t2.b and t2.b > 0 and t1.a = t1.c and t1.d like 'abc' and t2.d = t1.d",
			best: "Join{DataScan(t1)->Sel([like(cast(t1.d), abc, 92)])->DataScan(t2)->Sel([like(cast(t2.d), abc, 92)])}(t1.a,t2.b)(t1.d,t2.d)->Projection",
		},
		{
			sql:  "select * from t ta join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			best: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta join t tb on ta.d = tb.d where ta.d > 1 and tb.a = 0",
			best: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			best: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta right outer join t tb on ta.d = tb.d and ta.a > 1 where tb.a = 0",
			best: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where ta.d = 0",
			best: "Join{DataScan(ta)->DataScan(tb)}->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where tb.d = 0",
			best: "Join{DataScan(ta)->DataScan(tb)}->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where tb.c is not null and tb.c = 0 and ifnull(tb.d, 1)",
			best: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.a = tb.a left outer join t tc on tb.b = tc.b where tc.c > 0",
			best: "Join{Join{DataScan(ta)->DataScan(tb)}(ta.a,tb.a)->DataScan(tc)}(tb.b,tc.b)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.a = tb.a left outer join t tc on tc.b = ta.b where tb.c > 0",
			best: "Join{Join{DataScan(ta)->DataScan(tb)}(ta.a,tb.a)->DataScan(tc)}(ta.b,tc.b)->Projection",
		},
		{
			sql:  "select * from t as ta left outer join (t as tb left join t as tc on tc.b = tb.b) on tb.a = ta.a where tc.c > 0",
			best: "Join{DataScan(ta)->Join{DataScan(tb)->DataScan(tc)}(tb.b,tc.b)}(ta.a,tb.a)->Projection",
		},
		{
			sql:  "select * from ( t as ta left outer join t as tb on ta.a = tb.a) join ( t as tc left join t as td on tc.b = td.b) on ta.c = td.c where tb.c = 2 and td.a = 1",
			best: "Join{Join{DataScan(ta)->DataScan(tb)}(ta.a,tb.a)->Join{DataScan(tc)->DataScan(td)}(tc.b,td.b)}(ta.c,td.c)->Projection",
		},
		{
			sql:  "select * from t ta left outer join (t tb left outer join t tc on tc.b = tb.b) on tb.a = ta.a and tc.c = ta.c where tc.d > 0 or ta.d > 0",
			best: "Join{DataScan(ta)->Join{DataScan(tb)->DataScan(tc)}(tb.b,tc.b)}(ta.a,tb.a)(ta.c,tc.c)->Sel([or(gt(tc.d, 0), gt(ta.d, 0))])->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where ifnull(tb.d, 1) or tb.d is null",
			best: "Join{DataScan(ta)->DataScan(tb)}(ta.d,tb.d)->Sel([or(ifnull(tb.d, 1), isnull(tb.d))])->Projection",
		},
		{
			sql:  "select a, d from (select * from t union all select * from t union all select * from t) z where a < 10",
			best: "UnionAll{DataScan(t)->Projection->Projection->DataScan(t)->Projection->Projection->DataScan(t)->Projection->Projection}->Projection",
		},
		{
			sql:  "select (select count(*) from t where t.a = k.a) from t k",
			best: "Apply{DataScan(k)->DataScan(t)->Aggr(count(1))->Projection->MaxOneRow}->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a < t.a)",
			best: "Join{DataScan(t)->DataScan(x)}->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a = t.a and t.a < 1 and x.a < 1)",
			best: "Join{DataScan(t)->DataScan(x)}(test.t.a,x.a)->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a = t.a and x.a < 1) and a < 1",
			best: "Join{DataScan(t)->DataScan(x)}(test.t.a,x.a)->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a = t.a) and exists(select 1 from t as x where x.a = t.a)",
			best: "Join{Join{DataScan(t)->DataScan(x)}(test.t.a,x.a)->DataScan(x)}(test.t.a,x.a)->Projection",
		},
		{
			sql:  "select * from (select a, b, sum(c) as s from t group by a, b) k where k.a > k.b * 2 + 1",
			best: "DataScan(t)->Aggr(sum(test.t.c),firstrow(test.t.a),firstrow(test.t.b))->Projection->Projection",
		},
		{
			sql:  "select * from (select a, b, sum(c) as s from t group by a, b) k where k.a > 1 and k.b > 2",
			best: "DataScan(t)->Aggr(sum(test.t.c),firstrow(test.t.a),firstrow(test.t.b))->Projection->Projection",
		},
		{
			sql:  "select * from (select k.a, sum(k.s) as ss from (select a, sum(b) as s from t group by a) k group by k.a) l where l.a > 2",
			best: "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Projection->Aggr(sum(k.s),firstrow(k.a))->Projection->Projection",
		},
		{
			sql:  "select * from (select a, sum(b) as s from t group by a) k where a > s",
			best: "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Sel([gt(cast(test.t.a), 2_col_0)])->Projection->Projection",
		},
		{
			sql:  "select * from (select a, sum(b) as s from t group by a + 1) k where a > 1",
			best: "DataScan(t)->Aggr(sum(test.t.b),firstrow(test.t.a))->Sel([gt(test.t.a, 1)])->Projection->Projection",
		},
		{
			sql:  "select * from (select a, sum(b) as s from t group by a having 1 = 0) k where a > 1",
			best: "Dual->Sel([gt(k.a, 1)])->Projection",
		},
		{
			sql:  "select a, count(a) cnt from t group by a having cnt < 1",
			best: "DataScan(t)->Aggr(count(test.t.a),firstrow(test.t.a))->Sel([lt(2_col_0, 1)])->Projection",
		},
		// issue #3873
		{
			sql:  "select t1.a, t2.a from t as t1 left join t as t2 on t1.a = t2.a where t1.a < 1.0",
			best: "Join{DataScan(t1)->DataScan(t2)}(t1.a,t2.a)->Projection",
		},
		// issue #7728
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a where t2.a = null",
			best: "Dual->Projection",
		},
	}
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(flagPredicatePushDown|flagDecorrelate|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestJoinPredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql   string
		left  string
		right string
	}{
		// issue #7628, inner join
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b where t1.a > t2.a",
			left:  "[]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b where t1.a=1 or t2.a=1",
			left:  "[]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b where (t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2)",
			left:  "[or(eq(t1.a, 1), eq(t1.a, 2))]",
			right: "[or(eq(t2.a, 1), eq(t2.a, 2))]",
		},
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b where (t1.c=1 and (t1.a=3 or t2.a=3)) or (t1.a=2 and t2.a=2)",
			left:  "[or(eq(t1.c, 1), eq(t1.a, 2))]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b where (t1.c=1 and ((t1.a=3 and t2.a=3) or (t1.a=4 and t2.a=4)))",
			left:  "[eq(t1.c, 1) or(eq(t1.a, 3), eq(t1.a, 4))]",
			right: "[or(eq(t2.a, 3), eq(t2.a, 4))]",
		},
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b where (t1.a>1 and t1.a < 3 and t2.a=1) or (t1.a=2 and t2.a=2)",
			left:  "[or(and(gt(t1.a, 1), lt(t1.a, 3)), eq(t1.a, 2))]",
			right: "[or(eq(t2.a, 1), eq(t2.a, 2))]",
		},
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b and ((t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2))",
			left:  "[or(eq(t1.a, 1), eq(t1.a, 2))]",
			right: "[or(eq(t2.a, 1), eq(t2.a, 2))]",
		},
		// issue #7628, left join
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b and ((t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2))",
			left:  "[]",
			right: "[or(eq(t2.a, 1), eq(t2.a, 2))]",
		},
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b and t1.a > t2.a",
			left:  "[]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b and (t1.a=1 or t2.a=1)",
			left:  "[]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b and ((t1.c=1 and (t1.a=3 or t2.a=3)) or (t1.a=2 and t2.a=2))",
			left:  "[]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b and ((t2.c=1 and (t1.a=3 or t2.a=3)) or (t1.a=2 and t2.a=2))",
			left:  "[]",
			right: "[or(eq(t2.c, 1), eq(t2.a, 2))]",
		},
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b and ((t1.c=1 and ((t1.a=3 and t2.a=3) or (t1.a=4 and t2.a=4))) or (t1.a=2 and t2.a=2))",
			left:  "[]",
			right: "[or(or(eq(t2.a, 3), eq(t2.a, 4)), eq(t2.a, 2))]",
		},
	}
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(flagPredicatePushDown|flagDecorrelate|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		proj, ok := p.(*LogicalProjection)
		c.Assert(ok, IsTrue, comment)
		join, ok := proj.children[0].(*LogicalJoin)
		c.Assert(ok, IsTrue, comment)
		leftPlan, ok := join.children[0].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		rightPlan, ok := join.children[1].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		leftCond := fmt.Sprintf("%s", leftPlan.pushedDownConds)
		rightCond := fmt.Sprintf("%s", rightPlan.pushedDownConds)
		c.Assert(leftCond, Equals, ca.left, comment)
		c.Assert(rightCond, Equals, ca.right, comment)
	}
}

func (s *testPlanSuite) TestOuterWherePredicatePushDown(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql   string
		sel   string
		left  string
		right string
	}{
		// issue #7628, left join with where condition
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b where (t1.a=1 and t2.a is null) or (t1.a=2 and t2.a=2)",
			sel:   "[or(and(eq(t1.a, 1), isnull(t2.a)), and(eq(t1.a, 2), eq(t2.a, 2)))]",
			left:  "[or(eq(t1.a, 1), eq(t1.a, 2))]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b where (t1.c=1 and (t1.a=3 or t2.a=3)) or (t1.a=2 and t2.a=2)",
			sel:   "[or(and(eq(t1.c, 1), or(eq(t1.a, 3), eq(t2.a, 3))), and(eq(t1.a, 2), eq(t2.a, 2)))]",
			left:  "[or(eq(t1.c, 1), eq(t1.a, 2))]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b where (t1.c=1 and ((t1.a=3 and t2.a=3) or (t1.a=4 and t2.a=4))) or (t1.a=2 and t2.a is null)",
			sel:   "[or(and(eq(t1.c, 1), or(and(eq(t1.a, 3), eq(t2.a, 3)), and(eq(t1.a, 4), eq(t2.a, 4)))), and(eq(t1.a, 2), isnull(t2.a)))]",
			left:  "[or(and(eq(t1.c, 1), or(eq(t1.a, 3), eq(t1.a, 4))), eq(t1.a, 2))]",
			right: "[]",
		},
	}
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(flagPredicatePushDown|flagDecorrelate|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		proj, ok := p.(*LogicalProjection)
		c.Assert(ok, IsTrue, comment)
		selection, ok := proj.children[0].(*LogicalSelection)
		c.Assert(ok, IsTrue, comment)
		selCond := fmt.Sprintf("%s", selection.Conditions)
		c.Assert(selCond, Equals, ca.sel, comment)
		join, ok := selection.children[0].(*LogicalJoin)
		c.Assert(ok, IsTrue, comment)
		leftPlan, ok := join.children[0].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		rightPlan, ok := join.children[1].(*DataSource)
		c.Assert(ok, IsTrue, comment)
		leftCond := fmt.Sprintf("%s", leftPlan.pushedDownConds)
		rightCond := fmt.Sprintf("%s", rightPlan.pushedDownConds)
		c.Assert(leftCond, Equals, ca.left, comment)
		c.Assert(rightCond, Equals, ca.right, comment)
	}
}

func (s *testPlanSuite) TestSimplifyOuterJoin(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql      string
		best     string
		joinType string
	}{
		{
			sql:      "select * from t t1 left join t t2 on t1.b = t2.b where t1.c > 1 or t2.c > 1;",
			best:     "Join{DataScan(t1)->DataScan(t2)}(t1.b,t2.b)->Sel([or(gt(t1.c, 1), gt(t2.c, 1))])->Projection",
			joinType: "left outer join",
		},
		{
			sql:      "select * from t t1 left join t t2 on t1.b = t2.b where t1.c > 1 and t2.c > 1;",
			best:     "Join{DataScan(t1)->DataScan(t2)}(t1.b,t2.b)->Projection",
			joinType: "inner join",
		},
		{
			sql:      "select * from t t1 left join t t2 on t1.b = t2.b where not (t1.c > 1 or t2.c > 1);",
			best:     "Join{DataScan(t1)->DataScan(t2)}(t1.b,t2.b)->Projection",
			joinType: "inner join",
		},
		{
			sql:      "select * from t t1 left join t t2 on t1.b = t2.b where not (t1.c > 1 and t2.c > 1);",
			best:     "Join{DataScan(t1)->DataScan(t2)}(t1.b,t2.b)->Sel([not(and(le(t1.c, 1), le(t2.c, 1)))])->Projection",
			joinType: "left outer join",
		},
		{
			sql:      "select * from t t1 left join t t2 on t1.b > 1 where t1.c = t2.c;",
			best:     "Join{DataScan(t1)->DataScan(t2)}(t1.c,t2.c)->Projection",
			joinType: "inner join",
		},
		{
			sql:      "select * from t t1 left join t t2 on true where t1.b <=> t2.b;",
			best:     "Join{DataScan(t1)->DataScan(t2)}->Sel([nulleq(t1.b, t2.b)])->Projection",
			joinType: "left outer join",
		},
	}
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		c.Assert(ToString(p), Equals, ca.best, comment)
		join, ok := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		if !ok {
			join, ok = p.(LogicalPlan).Children()[0].Children()[0].(*LogicalJoin)
			c.Assert(ok, IsTrue, comment)
		}
		joinType := fmt.Sprintf("%s", join.JoinType.String())
		c.Assert(joinType, Equals, ca.joinType, comment)
	}
}

func (s *testPlanSuite) TestAntiSemiJoinConstFalse(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql      string
		best     string
		joinType string
	}{
		{
			sql:      "select a from t t1 where not exists (select a from t t2 where t1.a = t2.a and t2.b = 1 and t2.b = 2)",
			best:     "Join{DataScan(t1)->DataScan(t2)}->Projection",
			joinType: "anti semi join",
		},
	}
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(flagDecorrelate|flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		c.Assert(ToString(p), Equals, ca.best, comment)
		join, _ := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		joinType := fmt.Sprintf("%s", join.JoinType.String())
		c.Assert(joinType, Equals, ca.joinType, comment)
	}
}

func (s *testPlanSuite) TestTablePartition(c *C) {
	defer testleak.AfterTest(c)()
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
		{
			ID:       43,
			Name:     model.NewCIStr("p3"),
			LessThan: []string{"64"},
		},
		{
			ID:       44,
			Name:     model.NewCIStr("p4"),
			LessThan: []string{"128"},
		},
		{
			ID:       45,
			Name:     model.NewCIStr("p5"),
			LessThan: []string{"maxvalue"},
		},
	}
	is := MockPartitionInfoSchema(definitions)
	// is1 equals to is without maxvalue partition.
	definitions1 := make([]model.PartitionDefinition, len(definitions)-1)
	copy(definitions1, definitions)
	is1 := MockPartitionInfoSchema(definitions1)

	tests := []struct {
		sql   string
		first string
		best  string
		is    infoschema.InfoSchema
	}{
		{
			sql:  "select * from t",
			best: "UnionAll{Partition(41)->Partition(42)->Partition(43)->Partition(44)->Partition(45)}->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.h < 31",
			best: "UnionAll{Partition(41)->Partition(42)}->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.h < 61",
			best: "UnionAll{Partition(41)->Partition(42)->Partition(43)}->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.h > 17 and t.h < 61",
			best: "UnionAll{Partition(42)->Partition(43)}->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.h < 8",
			best: "Partition(41)->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.h > 128",
			best: "Partition(45)->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.h > 128",
			best: "Dual->Projection",
			is:   is1,
		},
		{
			// NULL will be located in the first partition.
			sql:  "select * from t where t.h is null",
			best: "Partition(41)->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.h is null or t.h > 70",
			best: "UnionAll{Partition(41)->Partition(44)}->Projection",
			is:   is1,
		},
	}
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(s.ctx, stmt, ca.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(flagDecorrelate|flagPrunColumns|flagPredicatePushDown|flagPartitionProcessor, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestSubquery(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			// This will be resolved as in sub query.
			sql:  "select * from t where 10 in (select b from t s where s.a = t.a)",
			best: "Join{DataScan(t)->DataScan(s)}(test.t.a,s.a)->Projection",
		},
		{
			sql:  "select count(c) ,(select b from t s where s.a = t.a) from t",
			best: "Join{DataScan(t)->Aggr(count(test.t.c),firstrow(test.t.a))->DataScan(s)}(test.t.a,s.a)->Projection->Projection",
		},
		{
			sql:  "select count(c) ,(select count(s.b) from t s where s.a = t.a) from t",
			best: "Join{DataScan(t)->Aggr(count(test.t.c),firstrow(test.t.a))->DataScan(s)}(test.t.a,s.a)->Aggr(firstrow(2_col_0),firstrow(test.t.a),count(s.b))->Projection->Projection",
		},
		{
			// Semi-join with agg cannot decorrelate.
			sql:  "select t.c in (select count(s.b) from t s where s.a = t.a) from t",
			best: "Apply{DataScan(t)->DataScan(s)->Sel([eq(s.a, test.t.a)])->Aggr(count(s.b))}->Projection",
		},
		{
			sql:  "select (select count(s.b) k from t s where s.a = t.a having k != 0) from t",
			best: "Join{DataScan(t)->DataScan(s)->Aggr(count(s.b),firstrow(s.a))}(test.t.a,s.a)->Projection->Projection->Projection",
		},
		{
			sql:  "select (select count(s.b) k from t s where s.a = t1.a) from t t1, t t2",
			best: "Join{Join{DataScan(t1)->DataScan(t2)}->DataScan(s)->Aggr(count(s.b),firstrow(s.a))}(t1.a,s.a)->Projection->Projection->Projection",
		},
		{
			sql:  "select (select count(1) k from t s where s.a = t.a having k != 0) from t",
			best: "Join{DataScan(t)->DataScan(s)->Aggr(count(1),firstrow(s.a))}(test.t.a,s.a)->Projection->Projection->Projection",
		},
		{
			sql:  "select a from t where a in (select a from t s group by t.b)",
			best: "Join{DataScan(t)->DataScan(s)->Aggr(firstrow(s.a))->Projection}(test.t.a,s.a)->Projection",
		},
		{
			// This will be resolved as in sub query.
			sql:  "select * from t where 10 in (((select b from t s where s.a = t.a)))",
			best: "Join{DataScan(t)->DataScan(s)}(test.t.a,s.a)->Projection",
		},
		{
			// This will be resolved as in function.
			sql:  "select * from t where 10 in (((select b from t s where s.a = t.a)), 10)",
			best: "Join{DataScan(t)->DataScan(s)}(test.t.a,s.a)->Projection->Sel([in(10, s.b, 10)])->Projection",
		},
		{
			sql:  "select * from t where exists (select s.a from t s having sum(s.a) = t.a )",
			best: "Join{DataScan(t)->DataScan(s)->Aggr(sum(s.a))->Projection}->Projection",
		},
		{
			// Test MaxOneRow for limit.
			sql:  "select (select * from (select b from t limit 1) x where x.b = t1.b) from t t1",
			best: "Join{DataScan(t1)->DataScan(t)->Projection->Limit}(t1.b,x.b)->Projection->Projection",
		},
		{
			// Test Nested sub query.
			sql:  "select * from t where exists (select s.a from t s where s.c in (select c from t as k where k.d = s.d) having sum(s.a) = t.a )",
			best: "Join{DataScan(t)->Join{DataScan(s)->DataScan(k)}(s.d,k.d)(s.c,k.c)->Aggr(sum(s.a))->Projection}->Projection",
		},
		{
			sql:  "select t1.b from t t1 where t1.b = (select max(t2.a) from t t2 where t1.b=t2.b)",
			best: "Join{DataScan(t1)->DataScan(t2)->Aggr(max(t2.a),firstrow(t2.b))}(t1.b,t2.b)->Projection->Sel([eq(t1.b, max(t2.a))])->Projection",
		},
		{
			sql:  "select t1.b from t t1 where t1.b = (select avg(t2.a) from t t2 where t1.g=t2.g and (t1.b = 4 or t2.b = 2))",
			best: "Apply{DataScan(t1)->DataScan(t2)->Sel([eq(t1.g, t2.g) or(eq(t1.b, 4), eq(t2.b, 2))])->Aggr(avg(t2.a))}->Projection->Sel([eq(cast(t1.b), avg(t2.a))])->Projection",
		},
	}

	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		Preprocess(s.ctx, stmt, s.is, false)
		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		if lp, ok := p.(LogicalPlan); ok {
			p, err = logicalOptimize(flagBuildKeyInfo|flagDecorrelate|flagPrunColumns, lp)
			c.Assert(err, IsNil)
		}
		c.Assert(ToString(p), Equals, ca.best, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestPlanBuilder(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		plan string
	}{
		{
			sql:  "select * from t for update",
			plan: "DataScan(t)->Lock->Projection",
		},
		{
			sql:  "update t set t.a = t.a * 1.5 where t.a >= 1000 order by t.a desc limit 10",
			plan: "TableReader(Table(t)->Limit)->Limit->Update",
		},
		{
			sql:  "delete from t where t.a >= 1000 order by t.a desc limit 10",
			plan: "TableReader(Table(t)->Limit)->Limit->Delete",
		},
		{
			sql:  "explain select * from t union all select * from t limit 1, 1",
			plan: "*core.Explain",
		},
		// The correctness of explain result is checked at integration test. There is to improve coverage.
		{
			sql:  "explain select /*+ TIDB_INLJ(t1, t2) */ * from t t1 left join t t2 on t1.a=t2.a where t1.b=1 and t2.b=1 and (t1.c=1 or t2.c=1)",
			plan: "*core.Explain",
		},
		{
			sql:  "explain select /*+ TIDB_HJ(t1, t2) */ * from t t1 left join t t2 on t1.a=t2.a where t1.b=1 and t2.b=1 and (t1.c=1 or t2.c=1)",
			plan: "*core.Explain",
		},
		{
			sql:  "explain select /*+ TIDB_SMJ(t1, t2) */ * from t t1 right join t t2 on t1.a=t2.a where t1.b=1 and t2.b=1 and (t1.c=1 or t2.c=1)",
			plan: "*core.Explain",
		},
		{
			sql:  `explain format="dot" select /*+ TIDB_SMJ(t1, t2) */ * from t t1, t t2 where t1.a=t2.a`,
			plan: "*core.Explain",
		},
		{
			sql:  "explain select * from t order by b",
			plan: "*core.Explain",
		},
		{
			sql:  "explain select * from t order by b limit 1",
			plan: "*core.Explain",
		},
		{
			sql:  `explain format="dot" select * from t order by a`,
			plan: "*core.Explain",
		},
		{
			sql:  "insert into t select * from t",
			plan: "TableReader(Table(t))->Insert",
		},
		{
			sql:  "show columns from t where `Key` = 'pri' like 't*'",
			plan: "Show([eq(cast(key), 0)])",
		},
		{
			sql:  "do sleep(5)",
			plan: "Dual->Projection",
		},
		{
			sql:  "select substr(\"abc\", 1)",
			plan: "Dual->Projection",
		},
		{
			sql:  "select * from t t1, t t2 where 1 = 0",
			plan: "Dual->Projection",
		},
		{
			sql:  "select * from t t1 join t t2 using(a)",
			plan: "Join{DataScan(t1)->DataScan(t2)}->Projection",
		},
		{
			sql:  "select * from t t1 natural join t t2",
			plan: "Join{DataScan(t1)->DataScan(t2)}->Projection",
		},
		{
			sql: "delete from t where a in (select b from t where c = 666) or b in (select a from t where c = 42)",
			// Note the Projection before Delete: the final schema should be the schema of
			// table t rather than Join.
			// If this schema is not set correctly, table.RemoveRecord would fail when adding
			// binlog columns, because the schema and data are not consistent.
			plan: "LeftHashJoin{LeftHashJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[666,666]], Table(t))}(test.t.a,test.t.b)->IndexReader(Index(t.c_d_e)[[42,42]])}(test.t.b,test.t.a)->Sel([or(6_aux_0, 10_aux_0)])->Projection->Delete",
		},
	}
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		s.ctx.GetSessionVars().HashJoinConcurrency = 1
		Preprocess(s.ctx, stmt, s.is, false)
		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		if lp, ok := p.(LogicalPlan); ok {
			p, err = logicalOptimize(flagPrunColumns, lp)
			c.Assert(err, IsNil)
		}
		c.Assert(ToString(p), Equals, ca.plan, Commentf("for %s", ca.sql))
	}
}

func (s *testPlanSuite) TestJoinReOrder(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5, t t6 where t1.a = t2.b and t2.a = t3.b and t3.c = t4.a and t4.d = t2.c and t5.d = t6.d",
			best: "Join{Join{Join{Join{DataScan(t1)->DataScan(t2)}(t1.a,t2.b)->DataScan(t3)}(t2.a,t3.b)->DataScan(t4)}(t3.c,t4.a)(t2.c,t4.d)->Join{DataScan(t5)->DataScan(t6)}(t5.d,t6.d)}->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5, t t6, t t7, t t8 where t1.a = t8.a",
			best: "Join{Join{Join{Join{DataScan(t1)->DataScan(t8)}(t1.a,t8.a)->DataScan(t2)}->Join{DataScan(t3)->DataScan(t4)}}->Join{Join{DataScan(t5)->DataScan(t6)}->DataScan(t7)}}->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5 where t1.a = t5.a and t5.a = t4.a and t4.a = t3.a and t3.a = t2.a and t2.a = t1.a and t1.a = t3.a and t2.a = t4.a and t5.b < 8",
			best: "Join{Join{Join{Join{DataScan(t5)->DataScan(t1)}(t5.a,t1.a)->DataScan(t2)}(t1.a,t2.a)->DataScan(t3)}(t2.a,t3.a)(t1.a,t3.a)->DataScan(t4)}(t5.a,t4.a)(t3.a,t4.a)(t2.a,t4.a)->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5 where t1.a = t5.a and t5.a = t4.a and t4.a = t3.a and t3.a = t2.a and t2.a = t1.a and t1.a = t3.a and t2.a = t4.a and t3.b = 1 and t4.a = 1",
			best: "Join{Join{Join{DataScan(t3)->DataScan(t1)}->Join{DataScan(t2)->DataScan(t4)}}->DataScan(t5)}->Projection",
		},
		{
			sql:  "select * from t o where o.b in (select t3.c from t t1, t t2, t t3 where t1.a = t3.a and t2.a = t3.a and t2.a = o.a)",
			best: "Apply{DataScan(o)->Join{Join{DataScan(t1)->DataScan(t3)}(t1.a,t3.a)->DataScan(t2)}(t3.a,t2.a)->Projection}->Projection",
		},
		{
			sql:  "select * from t o where o.b in (select t3.c from t t1, t t2, t t3 where t1.a = t3.a and t2.a = t3.a and t2.a = o.a and t1.a = 1)",
			best: "Apply{DataScan(o)->Join{Join{DataScan(t3)->DataScan(t1)}->DataScan(t2)}->Projection}->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(flagPredicatePushDown|flagJoinReOrderGreedy, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
}

func (s *testPlanSuite) TestEagerAggregation(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select sum(t.a), sum(t.a+1), sum(t.a), count(t.a), sum(t.a) + count(t.a) from t",
			best: "DataScan(t)->Aggr(sum(test.t.a),sum(plus(test.t.a, 1)),count(test.t.a))->Projection",
		},
		{
			sql:  "select sum(t.a + t.b), sum(t.a + t.c), sum(t.a + t.b), count(t.a) from t having sum(t.a + t.b) > 0 order by sum(t.a + t.c)",
			best: "DataScan(t)->Aggr(sum(plus(test.t.a, test.t.b)),sum(plus(test.t.a, test.t.c)),count(test.t.a))->Sel([gt(2_col_0, 0)])->Projection->Sort->Projection",
		},
		{
			sql:  "select sum(a.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(b.a),firstrow(b.c))}(a.c,b.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(b.a), a.a from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(b.a),firstrow(b.c))}(a.c,b.c)->Aggr(sum(join_agg_0),firstrow(a.a))->Projection",
		},
		{
			sql:  "select sum(a.a), b.a from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0),firstrow(b.a))->Projection",
		},
		{
			sql:  "select sum(a.a), sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)}(a.c,b.c)->Aggr(sum(a.a),sum(b.a))->Projection",
		},
		{
			sql:  "select sum(a.a), max(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0),max(b.a))->Projection",
		},
		{
			sql:  "select max(a.a), sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(b.a),firstrow(b.c))}(a.c,b.c)->Aggr(max(a.a),sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a, t b, t c where a.c = b.c and b.c = c.c",
			best: "Join{Join{DataScan(a)->DataScan(b)}(a.c,b.c)->DataScan(c)}(b.c,c.c)->Aggr(sum(a.a))->Projection",
		},
		{
			sql:  "select sum(b.a) from t a left join t b on a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(b.a),firstrow(b.c))}(a.c,b.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a left join t b on a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a right join t b on a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(a.a),firstrow(a.c))->DataScan(b)}(a.c,b.c)->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select sum(a) from (select * from t) x",
			best: "DataScan(t)->Aggr(sum(test.t.a))->Projection",
		},
		{
			sql:  "select sum(c1) from (select c c1, d c2 from t a union all select a c1, b c2 from t b union all select b c1, e c2 from t c) x group by c2",
			best: "UnionAll{DataScan(a)->Projection->Aggr(sum(a.c1),firstrow(a.c2))->DataScan(b)->Projection->Aggr(sum(b.c1),firstrow(b.c2))->DataScan(c)->Projection->Aggr(sum(c.c1),firstrow(c.c2))}->Aggr(sum(join_agg_0))->Projection",
		},
		{
			sql:  "select max(a.b), max(b.b) from t a join t b on a.c = b.c group by a.a",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(max(b.b),firstrow(b.c))}(a.c,b.c)->Projection->Projection",
		},
		{
			sql:  "select max(a.b), max(b.b) from t a join t b on a.a = b.a group by a.c",
			best: "Join{DataScan(a)->DataScan(b)}(a.a,b.a)->Aggr(max(a.b),max(b.b))->Projection",
		},
		{
			sql:  "select max(c.b) from (select * from t a union all select * from t b) c group by c.a",
			best: "UnionAll{DataScan(a)->Projection->Projection->Projection->DataScan(b)->Projection->Projection->Projection}->Aggr(max(join_agg_0))->Projection",
		},
		{
			sql:  "select max(a.c) from t a join t b on a.a=b.a and a.b=b.b group by a.b",
			best: "Join{DataScan(a)->DataScan(b)}(a.a,b.a)(a.b,b.b)->Aggr(max(a.c))->Projection",
		},
		{
			sql:  "select t1.a, count(t2.b) from t t1, t t2 where t1.a = t2.a group by t1.a",
			best: "Join{DataScan(t1)->DataScan(t2)}(t1.a,t2.a)->Projection->Projection",
		},
	}
	s.ctx.GetSessionVars().AllowAggPushDown = true
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(flagBuildKeyInfo|flagPredicatePushDown|flagPrunColumns|flagPushDownAgg, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, Commentf("for %s", tt.sql))
	}
	s.ctx.GetSessionVars().AllowAggPushDown = false
}

func (s *testPlanSuite) TestColumnPruning(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		ans map[int][]string
	}{
		{
			sql: "select count(*) from t group by a",
			ans: map[int][]string{
				1: {"a"},
			},
		},
		{
			sql: "select count(*) from t",
			ans: map[int][]string{
				1: {},
			},
		},
		{
			sql: "select count(*) from t a join t b where a.a < 1",
			ans: map[int][]string{
				1: {"a"},
				2: {},
			},
		},
		{
			sql: "select count(*) from t a join t b on a.a = b.d",
			ans: map[int][]string{
				1: {"a"},
				2: {"d"},
			},
		},
		{
			sql: "select count(*) from t a join t b on a.a = b.d order by sum(a.d)",
			ans: map[int][]string{
				1: {"a", "d"},
				2: {"d"},
			},
		},
		{
			sql: "select count(b.a) from t a join t b on a.a = b.d group by b.b order by sum(a.d)",
			ans: map[int][]string{
				1: {"a", "d"},
				2: {"a", "b", "d"},
			},
		},
		{
			sql: "select * from (select count(b.a) from t a join t b on a.a = b.d group by b.b having sum(a.d) < 0) tt",
			ans: map[int][]string{
				1: {"a", "d"},
				2: {"a", "b", "d"},
			},
		},
		{
			sql: "select (select count(a) from t where b = k.a) from t k",
			ans: map[int][]string{
				1: {"a"},
				3: {"a", "b"},
			},
		},
		{
			sql: "select exists (select count(*) from t where b = k.a) from t k",
			ans: map[int][]string{
				1: {},
			},
		},
		{
			sql: "select b = (select count(*) from t where b = k.a) from t k",
			ans: map[int][]string{
				1: {"a", "b"},
				3: {"b"},
			},
		},
		{
			sql: "select exists (select count(a) from t where b = k.a group by b) from t k",
			ans: map[int][]string{
				1: {"a"},
				3: {"b"},
			},
		},
		{
			sql: "select a as c1, b as c2 from t order by 1, c1 + c2 + c",
			ans: map[int][]string{
				1: {"a", "b", "c"},
			},
		},
		{
			sql: "select a from t where b < any (select c from t)",
			ans: map[int][]string{
				1: {"a", "b"},
				3: {"c"},
			},
		},
		{
			sql: "select a from t where (b,a) != all (select c,d from t)",
			ans: map[int][]string{
				1: {"a", "b"},
				3: {"c", "d"},
			},
		},
		{
			sql: "select a from t where (b,a) in (select c,d from t)",
			ans: map[int][]string{
				1: {"a", "b"},
				3: {"c", "d"},
			},
		},
		{
			sql: "select a from t where a in (select a from t s group by t.b)",
			ans: map[int][]string{
				1: {"a"},
				3: {"a"},
			},
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		lp, err := logicalOptimize(flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil)
		checkDataSourceCols(lp, c, tt.ans, comment)
	}
}

func (s *testPlanSuite) TestAllocID(c *C) {
	ctx := MockContext()
	pA := DataSource{}.Init(ctx)
	pB := DataSource{}.Init(ctx)
	c.Assert(pA.id+1, Equals, pB.id)
}

func checkDataSourceCols(p LogicalPlan, c *C, ans map[int][]string, comment CommentInterface) {
	switch p.(type) {
	case *DataSource:
		colList, ok := ans[p.ID()]
		c.Assert(ok, IsTrue, comment)
		for i, colName := range colList {
			c.Assert(colName, Equals, p.Schema().Columns[i].ColName.L, comment)
		}
	}
	for _, child := range p.Children() {
		checkDataSourceCols(child, c, ans, comment)
	}
}

func (s *testPlanSuite) TestValidate(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		err *terror.Error
	}{
		{
			sql: "select date_format((1,2), '%H');",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select cast((1,2) as date)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) between (3,4) and (5,6)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) rlike '1'",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) like '1'",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select case(1,2) when(1,2) then true end",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) in ((3,4),(5,6))",
			err: nil,
		},
		{
			sql: "select row(1,(2,3)) in (select a,b from t)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select row(1,2) in (select a,b from t)",
			err: nil,
		},
		{
			sql: "select (1,2) in ((3,4),5)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) is true",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) is null",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (+(1,2))=(1,2)",
			err: nil,
		},
		{
			sql: "select (-(1,2))=(1,2)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2)||(1,2)",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select (1,2) < (3,4)",
			err: nil,
		},
		{
			sql: "select (1,2) < 3",
			err: expression.ErrOperandColumns,
		},
		{
			sql: "select 1, * from t",
			err: ErrInvalidWildCard,
		},
		{
			sql: "select *, 1 from t",
			err: nil,
		},
		{
			sql: "select 1, t.* from t",
			err: nil,
		},
		{
			sql: "select 1 from t t1, t t2 where t1.a > all((select a) union (select a))",
			err: ErrAmbiguous,
		},
		{
			sql: "insert into t set a = 1, b = a + 1",
			err: nil,
		},
		{
			sql: "insert into t set a = 1, b = values(a) + 1",
			err: nil,
		},
		{
			sql: "select a, b, c from t order by 0",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a, b, c from t order by 4",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a as c1, b as c1 from t order by c1",
			err: ErrAmbiguous,
		},
		{
			sql: "(select a as b, b from t) union (select a, b from t) order by b",
			err: ErrAmbiguous,
		},
		{
			sql: "(select a as b, b from t) union (select a, b from t) order by a",
			err: ErrUnknownColumn,
		},
		{
			sql: "select * from t t1 use index(e)",
			err: ErrKeyDoesNotExist,
		},
		{
			sql: "select a from t having c2",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a from t group by c2 + 1 having c2",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a as b, b from t having b",
			err: ErrAmbiguous,
		},
		{
			sql: "select a + 1 from t having a",
			err: ErrUnknownColumn,
		},
		{
			sql: "select a from t having sum(avg(a))",
			err: ErrInvalidGroupFuncUse,
		},
		{
			sql: "select concat(c_str, d_str) from t group by `concat(c_str, d_str)`",
			err: nil,
		},
		{
			sql: "select concat(c_str, d_str) from t group by `concat(c_str,d_str)`",
			err: ErrUnknownColumn,
		},
	}
	for _, tt := range tests {
		sql := tt.sql
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is, false)
		_, err = BuildLogicalPlan(s.ctx, stmt, s.is)
		if tt.err == nil {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(tt.err.Equal(err), IsTrue, comment)
		}
	}
}

func checkUniqueKeys(p LogicalPlan, c *C, ans map[int][][]string, sql string) {
	keyList, ok := ans[p.ID()]
	c.Assert(ok, IsTrue, Commentf("for %s, %v not found", sql, p.ID()))
	c.Assert(len(p.Schema().Keys), Equals, len(keyList), Commentf("for %s, %v, the number of key doesn't match, the schema is %s", sql, p.ID(), p.Schema()))
	for i, key := range keyList {
		c.Assert(len(key), Equals, len(p.Schema().Keys[i]), Commentf("for %s, %v %v, the number of column doesn't match", sql, p.ID(), key))
		for j, colName := range key {
			c.Assert(colName, Equals, p.Schema().Keys[i][j].String(), Commentf("for %s, %v %v, column dosen't match", sql, p.ID(), key))
		}
	}
	for _, child := range p.Children() {
		checkUniqueKeys(child, c, ans, sql)
	}
}

func (s *testPlanSuite) TestUniqueKeyInfo(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		ans map[int][][]string
	}{
		{
			sql: "select a, sum(e) from t group by b",
			ans: map[int][][]string{
				1: {{"test.t.a"}},
				2: {{"test.t.a"}},
				3: {{"test.t.a"}},
			},
		},
		{
			sql: "select a, b, sum(f) from t group by b",
			ans: map[int][][]string{
				1: {{"test.t.f"}, {"test.t.a"}},
				2: {{"test.t.a"}, {"test.t.b"}},
				3: {{"test.t.a"}, {"test.t.b"}},
			},
		},
		{
			sql: "select c, d, e, sum(a) from t group by c, d, e",
			ans: map[int][][]string{
				1: {{"test.t.a"}},
				2: {{"test.t.c", "test.t.d", "test.t.e"}},
				3: {{"test.t.c", "test.t.d", "test.t.e"}},
			},
		},
		{
			sql: "select f, g, sum(a) from t",
			ans: map[int][][]string{
				1: {{"test.t.f"}, {"test.t.f", "test.t.g"}, {"test.t.a"}},
				2: {{"test.t.f"}, {"test.t.f", "test.t.g"}},
				3: {{"test.t.f"}, {"test.t.f", "test.t.g"}},
			},
		},
		{
			sql: "select * from t t1 join t t2 on t1.a = t2.e",
			ans: map[int][][]string{
				1: {{"t1.f"}, {"t1.f", "t1.g"}, {"t1.a"}},
				2: {{"t2.f"}, {"t2.f", "t2.g"}, {"t2.a"}},
				3: {{"t2.f"}, {"t2.f", "t2.g"}, {"t2.a"}},
				4: {{"t2.f"}, {"t2.f", "t2.g"}, {"t2.a"}},
			},
		},
		{
			sql: "select f from t having sum(a) > 0",
			ans: map[int][][]string{
				1: {{"test.t.f"}, {"test.t.a"}},
				2: {{"test.t.f"}},
				6: {{"test.t.f"}},
				3: {{"test.t.f"}},
				5: {{"test.t.f"}},
			},
		},
		{
			sql: "select * from t t1 left join t t2 on t1.a = t2.a",
			ans: map[int][][]string{
				1: {{"t1.f"}, {"t1.f", "t1.g"}, {"t1.a"}},
				2: {{"t2.f"}, {"t2.f", "t2.g"}, {"t2.a"}},
				3: {{"t1.f"}, {"t1.f", "t1.g"}, {"t1.a"}},
				4: {{"t1.f"}, {"t1.f", "t1.g"}, {"t1.a"}},
			},
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		lp, err := logicalOptimize(flagPredicatePushDown|flagPrunColumns|flagBuildKeyInfo, p.(LogicalPlan))
		c.Assert(err, IsNil)
		checkUniqueKeys(lp, c, tt.ans, tt.sql)
	}
}

func (s *testPlanSuite) TestAggPrune(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select a, count(b) from t group by a",
			best: "DataScan(t)->Projection->Projection",
		},
		{
			sql:  "select sum(b) from t group by c, d, e",
			best: "DataScan(t)->Aggr(sum(test.t.b))->Projection",
		},
		{
			sql:  "select tt.a, sum(tt.b) from (select a, b from t) tt group by tt.a",
			best: "DataScan(t)->Projection->Projection",
		},
		{
			sql:  "select count(1) from (select count(1), a as b from t group by a) tt group by b",
			best: "DataScan(t)->Projection->Projection",
		},
		{
			sql:  "select a, count(b) from t group by a",
			best: "DataScan(t)->Projection->Projection",
		},
		{
			sql:  "select a, count(distinct a, b) from t group by a",
			best: "DataScan(t)->Projection->Projection",
		},
	}
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(s.ctx, stmt, s.is)
		c.Assert(err, IsNil)

		p, err = logicalOptimize(flagPredicatePushDown|flagPrunColumns|flagBuildKeyInfo|flagEliminateAgg|flagEliminateProjection, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestVisitInfo(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		ans []visitInfo
	}{
		{
			sql: "insert into t (a) values (1)",
			ans: []visitInfo{
				{mysql.InsertPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "delete from t where a = 1",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", "", nil},
				{mysql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "delete from a1 using t as a1 inner join t as a2 where a1.a = a2.a",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", "", nil},
				{mysql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "update t set a = 7 where a = 1",
			ans: []visitInfo{
				{mysql.UpdatePriv, "test", "t", "", nil},
				{mysql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "update t, (select * from t) a1 set t.a = a1.a;",
			ans: []visitInfo{
				{mysql.UpdatePriv, "test", "t", "", nil},
				{mysql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "update t a1 set a1.a = a1.a + 1",
			ans: []visitInfo{
				{mysql.UpdatePriv, "test", "t", "", nil},
				{mysql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "select a, sum(e) from t group by a",
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "truncate table t",
			ans: []visitInfo{
				{mysql.DeletePriv, "test", "t", "", nil},
			},
		},
		{
			sql: "drop table t",
			ans: []visitInfo{
				{mysql.DropPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "create table t (a int)",
			ans: []visitInfo{
				{mysql.CreatePriv, "test", "t", "", nil},
			},
		},
		{
			sql: "create table t1 like t",
			ans: []visitInfo{
				{mysql.CreatePriv, "test", "t1", "", nil},
				{mysql.SelectPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "create database test",
			ans: []visitInfo{
				{mysql.CreatePriv, "test", "", "", nil},
			},
		},
		{
			sql: "drop database test",
			ans: []visitInfo{
				{mysql.DropPriv, "test", "", "", nil},
			},
		},
		{
			sql: "create index t_1 on t (a)",
			ans: []visitInfo{
				{mysql.IndexPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "drop index e on t",
			ans: []visitInfo{
				{mysql.IndexPriv, "test", "t", "", nil},
			},
		},
		{
			sql: `create user 'test'@'%' identified by '123456'`,
			ans: []visitInfo{
				{mysql.CreateUserPriv, "", "", "", nil},
			},
		},
		{
			sql: `drop user 'test'@'%'`,
			ans: []visitInfo{
				{mysql.CreateUserPriv, "", "", "", nil},
			},
		},
		{
			sql: `grant all privileges on test.* to 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "", "", nil},
				{mysql.InsertPriv, "test", "", "", nil},
				{mysql.UpdatePriv, "test", "", "", nil},
				{mysql.DeletePriv, "test", "", "", nil},
				{mysql.CreatePriv, "test", "", "", nil},
				{mysql.DropPriv, "test", "", "", nil},
				{mysql.GrantPriv, "test", "", "", nil},
				{mysql.AlterPriv, "test", "", "", nil},
				{mysql.ExecutePriv, "test", "", "", nil},
				{mysql.IndexPriv, "test", "", "", nil},
			},
		},
		{
			sql: `grant select on test.ttt to 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SelectPriv, "test", "ttt", "", nil},
				{mysql.GrantPriv, "test", "ttt", "", nil},
			},
		},
		{
			sql: `revoke all privileges on *.* from 'test'@'%'`,
			ans: []visitInfo{
				{mysql.SuperPriv, "", "", "", nil},
			},
		},
		{
			sql: `set password for 'root'@'%' = 'xxxxx'`,
			ans: []visitInfo{},
		},
		{
			sql: `show create table test.ttt`,
			ans: []visitInfo{
				{mysql.AllPrivMask, "test", "ttt", "", nil},
			},
		},
	}

	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is, false)
		builder := &PlanBuilder{
			colMapper: make(map[*ast.ColumnNameExpr]int),
			ctx:       MockContext(),
			is:        s.is,
		}
		builder.ctx.GetSessionVars().HashJoinConcurrency = 1
		_, err = builder.Build(stmt)
		c.Assert(err, IsNil, comment)

		checkVisitInfo(c, builder.visitInfo, tt.ans, comment)
	}
}

type visitInfoArray []visitInfo

func (v visitInfoArray) Len() int {
	return len(v)
}

func (v visitInfoArray) Less(i, j int) bool {
	if v[i].privilege < v[j].privilege {
		return true
	}
	if v[i].db < v[j].db {
		return true
	}
	if v[i].table < v[j].table {
		return true
	}
	if v[i].column < v[j].column {
		return true
	}

	return false
}

func (v visitInfoArray) Swap(i, j int) {
	v[i], v[j] = v[j], v[i]
}

func unique(v []visitInfo) []visitInfo {
	repeat := 0
	for i := 1; i < len(v); i++ {
		if v[i] == v[i-1] {
			repeat++
		} else {
			v[i-repeat] = v[i]
		}
	}
	return v[:len(v)-repeat]
}

func checkVisitInfo(c *C, v1, v2 []visitInfo, comment CommentInterface) {
	sort.Sort(visitInfoArray(v1))
	sort.Sort(visitInfoArray(v2))
	v1 = unique(v1)
	v2 = unique(v2)

	c.Assert(len(v1), Equals, len(v2), comment)
	for i := 0; i < len(v1); i++ {
		c.Assert(v1[i], Equals, v2[i], comment)
	}
}

func (s *testPlanSuite) TestUnion(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
		err  bool
	}{
		{
			sql:  "select a from t union select a from t",
			best: "UnionAll{DataScan(t)->Projection->DataScan(t)->Projection}->Aggr(firstrow(a))",
			err:  false,
		},
		{
			sql:  "select a from t union all select a from t",
			best: "UnionAll{DataScan(t)->Projection->DataScan(t)->Projection}",
			err:  false,
		},
		{
			sql:  "select a from t union select a from t union all select a from t",
			best: "UnionAll{UnionAll{DataScan(t)->Projection->DataScan(t)->Projection}->Aggr(firstrow(a))->Projection->DataScan(t)->Projection}",
			err:  false,
		},
		{
			sql:  "select a from t union select a from t union all select a from t union select a from t union select a from t",
			best: "UnionAll{DataScan(t)->Projection->DataScan(t)->Projection->DataScan(t)->Projection->DataScan(t)->Projection->DataScan(t)->Projection}->Aggr(firstrow(a))",
			err:  false,
		},
		{
			sql:  "select a from t union select a, b from t",
			best: "",
			err:  true,
		},
		{
			sql:  "select * from (select 1 as a  union select 1 union all select 2) t order by a",
			best: "UnionAll{UnionAll{Dual->Projection->Projection->Dual->Projection->Projection}->Aggr(firstrow(a))->Projection->Dual->Projection->Projection}->Projection->Sort",
			err:  false,
		},
		{
			sql:  "select * from (select 1 as a  union select 1 union all select 2) t order by (select a)",
			best: "Apply{UnionAll{UnionAll{Dual->Projection->Projection->Dual->Projection->Projection}->Aggr(firstrow(a))->Projection->Dual->Projection->Projection}->Dual->Projection->MaxOneRow}->Sort->Projection",
			err:  false,
		},
	}
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is, false)
		builder := &PlanBuilder{
			ctx:       MockContext(),
			is:        s.is,
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		plan, err := builder.Build(stmt)
		if tt.err {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		p := plan.(LogicalPlan)
		p, err = logicalOptimize(builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestTopNPushDown(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		// Test TopN + Selection.
		{
			sql:  "select * from t where a < 1 order by b limit 5",
			best: "DataScan(t)->TopN([test.t.b],0,5)->Projection",
		},
		// Test Limit + Selection.
		{
			sql:  "select * from t where a < 1 limit 5",
			best: "DataScan(t)->Limit->Projection",
		},
		// Test Limit + Agg + Proj .
		{
			sql:  "select a, count(b) from t group by b limit 5",
			best: "DataScan(t)->Aggr(count(test.t.b),firstrow(test.t.a))->Limit->Projection",
		},
		// Test TopN + Agg + Proj .
		{
			sql:  "select a, count(b) from t group by b order by c limit 5",
			best: "DataScan(t)->Aggr(count(test.t.b),firstrow(test.t.a),firstrow(test.t.c))->TopN([test.t.c],0,5)->Projection",
		},
		// Test TopN + Join + Proj.
		{
			sql:  "select * from t, t s order by t.a limit 5",
			best: "Join{DataScan(t)->DataScan(s)}->TopN([test.t.a],0,5)->Projection",
		},
		// Test Limit + Join + Proj.
		{
			sql:  "select * from t, t s limit 5",
			best: "Join{DataScan(t)->DataScan(s)}->Limit->Projection",
		},
		// Test TopN + Left Join + Proj.
		{
			sql:  "select * from t left outer join t s on t.a = s.a order by t.a limit 5",
			best: "Join{DataScan(t)->TopN([test.t.a],0,5)->DataScan(s)}(test.t.a,s.a)->TopN([test.t.a],0,5)->Projection",
		},
		// Test TopN + Left Join + Proj.
		{
			sql:  "select * from t left outer join t s on t.a = s.a order by t.a limit 5, 5",
			best: "Join{DataScan(t)->TopN([test.t.a],0,10)->DataScan(s)}(test.t.a,s.a)->TopN([test.t.a],5,5)->Projection",
		},
		// Test Limit + Left Join + Proj.
		{
			sql:  "select * from t left outer join t s on t.a = s.a limit 5",
			best: "Join{DataScan(t)->Limit->DataScan(s)}(test.t.a,s.a)->Limit->Projection",
		},
		// Test Limit + Left Join Apply + Proj.
		{
			sql:  "select (select s.a from t s where t.a = s.a) from t limit 5",
			best: "Join{DataScan(t)->Limit->DataScan(s)}(test.t.a,s.a)->Limit->Projection",
		},
		// Test TopN + Left Join Apply + Proj.
		{
			sql:  "select (select s.a from t s where t.a = s.a) from t order by t.a limit 5",
			best: "Join{DataScan(t)->TopN([test.t.a],0,5)->DataScan(s)}(test.t.a,s.a)->TopN([test.t.a],0,5)->Projection",
		},
		// Test TopN + Left Semi Join Apply + Proj.
		{
			sql:  "select exists (select s.a from t s where t.a = s.a) from t order by t.a limit 5",
			best: "Join{DataScan(t)->TopN([test.t.a],0,5)->DataScan(s)}(test.t.a,s.a)->TopN([test.t.a],0,5)->Projection",
		},
		// Test TopN + Semi Join Apply + Proj.
		{
			sql:  "select * from t where exists (select s.a from t s where t.a = s.a) order by t.a limit 5",
			best: "Join{DataScan(t)->DataScan(s)}(test.t.a,s.a)->TopN([test.t.a],0,5)->Projection",
		},
		// Test TopN + Right Join + Proj.
		{
			sql:  "select * from t right outer join t s on t.a = s.a order by s.a limit 5",
			best: "Join{DataScan(t)->DataScan(s)->TopN([s.a],0,5)}(test.t.a,s.a)->TopN([s.a],0,5)->Projection",
		},
		// Test Limit + Right Join + Proj.
		{
			sql:  "select * from t right outer join t s on t.a = s.a order by s.a,t.b limit 5",
			best: "Join{DataScan(t)->DataScan(s)}(test.t.a,s.a)->TopN([s.a test.t.b],0,5)->Projection",
		},
		// Test TopN + UA + Proj.
		{
			sql:  "select * from t union all (select * from t s) order by a,b limit 5",
			best: "UnionAll{DataScan(t)->TopN([test.t.a test.t.b],0,5)->Projection->DataScan(s)->TopN([s.a s.b],0,5)->Projection}->TopN([a b],0,5)",
		},
		// Test TopN + UA + Proj.
		{
			sql:  "select * from t union all (select * from t s) order by a,b limit 5, 5",
			best: "UnionAll{DataScan(t)->TopN([test.t.a test.t.b],0,10)->Projection->DataScan(s)->TopN([s.a s.b],0,10)->Projection}->TopN([a b],5,5)",
		},
		// Test Limit + UA + Proj + Sort.
		{
			sql:  "select * from t union all (select * from t s order by a) limit 5",
			best: "UnionAll{DataScan(t)->Limit->Projection->DataScan(s)->TopN([s.a],0,5)->Projection}->Limit",
		},
		// Test `ByItem` containing column from both sides.
		{
			sql:  "select ifnull(t1.b, t2.a) from t t1 left join t t2 on t1.e=t2.e order by ifnull(t1.b, t2.a) limit 5",
			best: "Join{DataScan(t1)->TopN([t1.b],0,5)->DataScan(t2)}(t1.e,t2.e)->TopN([t1.b],0,5)->Projection",
		},
		// Test ifnull cannot be eliminated
		{
			sql:  "select ifnull(t1.h, t2.b) from t t1 left join t t2 on t1.e=t2.e order by ifnull(t1.h, t2.b) limit 5",
			best: "Join{DataScan(t1)->DataScan(t2)}(t1.e,t2.e)->TopN([ifnull(t1.h, t2.b)],0,5)->Projection->Projection",
		},
	}
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is, false)
		builder := &PlanBuilder{
			ctx:       MockContext(),
			is:        s.is,
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p, err := builder.Build(stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestNameResolver(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql string
		err string
	}{
		{"select a from t", ""},
		{"select c3 from t", "[planner:1054]Unknown column 'c3' in 'field list'"},
		{"select c1 from t4", "[schema:1146]Table 'test.t4' doesn't exist"},
		{"select * from t", ""},
		{"select t.* from t", ""},
		{"select t2.* from t", "[planner:1051]Unknown table 't2'"},
		{"select b as a, c as a from t group by a", "[planner:1052]Column 'c' in field list is ambiguous"},
		{"select 1 as a, b as a, c as a from t group by a", ""},
		{"select a, b as a from t group by a+1", ""},
		{"select c, a as c from t order by c+1", ""},
		{"select * from t as t1, t as t2 join t as t3 on t2.a = t3.a", ""},
		{"select * from t as t1, t as t2 join t as t3 on t1.c1 = t2.a", "[planner:1054]Unknown column 't1.c1' in 'on clause'"},
		{"select a from t group by a having a = 3", ""},
		{"select a from t group by a having c2 = 3", "[planner:1054]Unknown column 'c2' in 'having clause'"},
		{"select a from t where exists (select b)", ""},
		{"select cnt from (select count(a) as cnt from t group by b) as t2 group by cnt", ""},
		{"select a from t where t11.a < t.a", "[planner:1054]Unknown column 't11.a' in 'where clause'"},
		{"select a from t having t11.c1 < t.a", "[planner:1054]Unknown column 't11.c1' in 'having clause'"},
		{"select a from t where t.a < t.a order by t11.c1", "[planner:1054]Unknown column 't11.c1' in 'order clause'"},
		{"select a from t group by t11.c1", "[planner:1054]Unknown column 't11.c1' in 'group statement'"},
		{"delete a from (select * from t ) as a, t", "[planner:1288]The target table a of the DELETE is not updatable"},
		{"delete b from (select * from t ) as a, t", "[planner:1109]Unknown table 'b' in MULTI DELETE"},
		{"select '' as fakeCol from t group by values(fakeCol)", "[planner:1054]Unknown column '' in 'VALUES() function'"},
		{"update t, (select * from t) as b set b.a = t.a", "[planner:1288]The target table b of the UPDATE is not updatable"},
	}

	for _, t := range tests {
		comment := Commentf("for %s", t.sql)
		stmt, err := s.ParseOneStmt(t.sql, "", "")
		c.Assert(err, IsNil, comment)
		s.ctx.GetSessionVars().HashJoinConcurrency = 1

		_, err = BuildLogicalPlan(s.ctx, stmt, s.is)
		if t.err == "" {
			c.Check(err, IsNil)
		} else {
			c.Assert(err.Error(), Equals, t.err)
		}
	}
}

func (s *testPlanSuite) TestOuterJoinEliminator(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		// Test left outer join + distinct
		{
			sql:  "select distinct t1.a, t1.b from t t1 left outer join t t2 on t1.b = t2.b",
			best: "DataScan(t1)->Aggr(firstrow(t1.a),firstrow(t1.b))",
		},
		// Test right outer join + distinct
		{
			sql:  "select distinct t2.a, t2.b from t t1 right outer join t t2 on t1.b = t2.b",
			best: "DataScan(t2)->Aggr(firstrow(t2.a),firstrow(t2.b))",
		},
		// Test duplicate agnostic agg functions on join
		{
			sql:  "select max(t1.a), min(t1.b) from t t1 left join t t2 on t1.b = t2.b",
			best: "DataScan(t1)->Aggr(max(t1.a),min(t1.b))->Projection",
		},
		{
			sql:  "select sum(distinct t1.a) from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b",
			best: "DataScan(t1)->Aggr(sum(t1.a))->Projection",
		},
		{
			sql:  "select count(distinct t1.a, t1.b) from t t1 left join t t2 on t1.b = t2.b",
			best: "DataScan(t1)->Aggr(count(t1.a, t1.b))->Projection",
		},
		// Test left outer join
		{
			sql:  "select t1.b from t t1 left outer join t t2 on t1.a = t2.a",
			best: "DataScan(t1)->Projection",
		},
		// Test right outer join
		{
			sql:  "select t2.b from t t1 right outer join t t2 on t1.a = t2.a",
			best: "DataScan(t2)->Projection",
		},
		// For complex join query
		{
			sql:  "select max(t3.b) from (t t1 left join t t2 on t1.a = t2.a) right join t t3 on t1.b = t3.b",
			best: "DataScan(t3)->TopN([t3.b true],0,1)->Aggr(max(t3.b))->Projection",
		},
	}

	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is, false)
		builder := &PlanBuilder{
			ctx:       MockContext(),
			is:        s.is,
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p, err := builder.Build(stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestSelectView(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select * from v",
			best: "DataScan(t)->Projection",
		},
	}
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is, false)
		builder := &PlanBuilder{
			ctx:       MockContext(),
			is:        s.is,
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p, err := builder.Build(stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}

func (s *testPlanSuite) TestWindowFunction(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql    string
		result string
	}{
		{
			sql:    "select a, avg(a) over(partition by a) from t",
			result: "TableReader(Table(t))->Window(avg(cast(test.t.a)))->Projection",
		},
		{
			sql:    "select a, avg(a) over(partition by b) from t",
			result: "TableReader(Table(t))->Sort->Window(avg(cast(test.t.a)))->Projection",
		},
		{
			sql:    "select a, avg(a+1) over(partition by (a+1)) from t",
			result: "TableReader(Table(t))->Projection->Sort->Window(avg(cast(2_proj_window_3)))->Projection",
		},
		{
			sql:    "select a, avg(a) over(order by a asc, b desc) from t order by a asc, b desc",
			result: "TableReader(Table(t))->Sort->Window(avg(cast(test.t.a)))->Projection",
		},
		{
			sql:    "select a, b as a, avg(a) over(partition by a) from t",
			result: "TableReader(Table(t))->Window(avg(cast(test.t.a)))->Projection",
		},
		{
			sql:    "select a, b as z, sum(z) over() from t",
			result: "[planner:1054]Unknown column 'z' in 'field list'",
		},
		{
			sql:    "select a, b as z from t order by (sum(z) over())",
			result: "TableReader(Table(t))->Window(sum(cast(test.t.z)))->Sort->Projection",
		},
		{
			sql:    "select sum(avg(a)) over() from t",
			result: "TableReader(Table(t)->StreamAgg)->StreamAgg->Window(sum(sel_agg_2))->Projection",
		},
		{
			sql:    "select b from t order by(sum(a) over())",
			result: "TableReader(Table(t))->Window(sum(cast(test.t.a)))->Sort->Projection",
		},
		{
			sql:    "select b from t order by(sum(a) over(partition by a))",
			result: "TableReader(Table(t))->Window(sum(cast(test.t.a)))->Sort->Projection",
		},
		{
			sql:    "select b from t order by(sum(avg(a)) over())",
			result: "TableReader(Table(t)->StreamAgg)->StreamAgg->Window(sum(sel_agg_2))->Sort->Projection",
		},
		{
			sql:    "select a from t having (select sum(a) over() as w from t tt where a > t.a)",
			result: "Apply{TableReader(Table(t))->TableReader(Table(t)->Sel([gt(tt.a, test.t.a)]))->Window(sum(cast(tt.a)))->MaxOneRow->Sel([w])}->Projection",
		},
		{
			sql:    "select avg(a) over() as w from t having w > 1",
			result: "[planner:3594]You cannot use the alias 'w' of an expression containing a window function in this context.'",
		},
		{
			sql:    "select sum(a) over() as sum_a from t group by sum_a",
			result: "[planner:1247]Reference 'sum_a' not supported (reference to window function)",
		},
		{
			sql:    "select sum(a) over() from t window w1 as (w2)",
			result: "[planner:3579]Window name 'w2' is not defined.",
		},
		{
			sql:    "select sum(a) over(w) from t",
			result: "[planner:3579]Window name 'w' is not defined.",
		},
		{
			sql:    "select sum(a) over() from t window w1 as (w2), w2 as (w1)",
			result: "[planner:3580]There is a circularity in the window dependency graph.",
		},
		{
			sql:    "select sum(a) over(w partition by a) from t window w as ()",
			result: "[planner:3581]A window which depends on another cannot define partitioning.",
		},
		{
			sql:    "select sum(a) over(w) from t window w as (rows between 1 preceding AND 1 following)",
			result: "[planner:3582]Window 'w' has a frame definition, so cannot be referenced by another window.",
		},
		{
			sql:    "select sum(a) over(w order by b) from t window w as (order by a)",
			result: "[planner:3583]Window '<unnamed window>' cannot inherit 'w' since both contain an ORDER BY clause.",
		},
		{
			sql:    "select sum(a) over() from t window w1 as (), w1 as ()",
			result: "[planner:3591]Window 'w1' is defined twice.",
		},
		{
			sql:    "select sum(a) over(w1), avg(a) over(w2) from t window w1 as (partition by a), w2 as (w1)",
			result: "TableReader(Table(t))->Window(sum(cast(test.t.a)))->Window(avg(cast(test.t.a)))->Projection",
		},
		{
			sql:    "select a from t window w1 as (partition by a) order by (sum(a) over(w1))",
			result: "TableReader(Table(t))->Window(sum(cast(test.t.a)))->Sort->Projection",
		},
	}

	s.Parser.EnableWindowFunc(true)
	defer func() {
		s.Parser.EnableWindowFunc(false)
	}()
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is, false)
		builder := &PlanBuilder{
			ctx:       MockContext(),
			is:        s.is,
			colMapper: make(map[*ast.ColumnNameExpr]int),
		}
		p, err := builder.Build(stmt)
		if err != nil {
			c.Assert(err.Error(), Equals, tt.result, comment)
			continue
		}
		c.Assert(err, IsNil)
		p, err = logicalOptimize(builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		lp, ok := p.(LogicalPlan)
		c.Assert(ok, IsTrue)
		p, err = physicalOptimize(lp)
		c.Assert(ToString(p), Equals, tt.result, comment)
	}
}
