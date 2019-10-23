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
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/property"
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
	s.is = infoschema.MockInfoSchema([]*model.TableInfo{MockSignedTable(), MockUnsignedTable(), MockView()})
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
			best: "Join{DataScan(a)->DataScan(b)}(Column#1,Column#13)->Aggr(count(1))->Projection",
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
			best: "Join{DataScan(ta)->DataScan(tb)}(Column#4,Column#14)(Column#1,Column#15)->Projection",
		},
		{
			sql:  "select * from t t1, t t2 where t1.a = t2.b and t2.b > 0 and t1.a = t1.c and t1.d like 'abc' and t2.d = t1.d",
			best: "Join{DataScan(t1)->Sel([like(cast(Column#4), abc, 92)])->DataScan(t2)->Sel([like(cast(Column#16), abc, 92)])}(Column#1,Column#14)(Column#4,Column#16)->Projection",
		},
		{
			sql:  "select * from t ta join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			best: "Join{DataScan(ta)->DataScan(tb)}(Column#4,Column#16)->Projection",
		},
		{
			sql:  "select * from t ta join t tb on ta.d = tb.d where ta.d > 1 and tb.a = 0",
			best: "Join{DataScan(ta)->DataScan(tb)}(Column#4,Column#16)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			best: "Join{DataScan(ta)->DataScan(tb)}(Column#4,Column#16)->Projection",
		},
		{
			sql:  "select * from t ta right outer join t tb on ta.d = tb.d and ta.a > 1 where tb.a = 0",
			best: "Join{DataScan(ta)->DataScan(tb)}(Column#4,Column#16)->Projection",
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
			best: "Join{DataScan(ta)->DataScan(tb)}(Column#4,Column#16)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.a = tb.a left outer join t tc on tb.b = tc.b where tc.c > 0",
			best: "Join{Join{DataScan(ta)->DataScan(tb)}(Column#1,Column#13)->DataScan(tc)}(Column#14,Column#26)->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.a = tb.a left outer join t tc on tc.b = ta.b where tb.c > 0",
			best: "Join{Join{DataScan(ta)->DataScan(tb)}(Column#1,Column#13)->DataScan(tc)}(Column#2,Column#26)->Projection",
		},
		{
			sql:  "select * from t as ta left outer join (t as tb left join t as tc on tc.b = tb.b) on tb.a = ta.a where tc.c > 0",
			best: "Join{DataScan(ta)->Join{DataScan(tb)->DataScan(tc)}(Column#14,Column#26)}(Column#1,Column#13)->Projection",
		},
		{
			sql:  "select * from ( t as ta left outer join t as tb on ta.a = tb.a) join ( t as tc left join t as td on tc.b = td.b) on ta.c = td.c where tb.c = 2 and td.a = 1",
			best: "Join{Join{DataScan(ta)->DataScan(tb)}(Column#1,Column#13)->Join{DataScan(tc)->DataScan(td)}(Column#26,Column#38)}(Column#3,Column#39)->Projection",
		},
		{
			sql:  "select * from t ta left outer join (t tb left outer join t tc on tc.b = tb.b) on tb.a = ta.a and tc.c = ta.c where tc.d > 0 or ta.d > 0",
			best: "Join{DataScan(ta)->Join{DataScan(tb)->DataScan(tc)}(Column#14,Column#26)}(Column#1,Column#13)(Column#3,Column#27)->Sel([or(gt(Column#28, 0), gt(Column#4, 0))])->Projection",
		},
		{
			sql:  "select * from t ta left outer join t tb on ta.d = tb.d and ta.a > 1 where ifnull(tb.d, 1) or tb.d is null",
			best: "Join{DataScan(ta)->DataScan(tb)}(Column#4,Column#16)->Sel([or(ifnull(Column#16, 1), isnull(Column#16))])->Projection",
		},
		{
			sql:  "select a, d from (select * from t union all select * from t union all select * from t) z where a < 10",
			best: "UnionAll{DataScan(t)->Projection->Projection->DataScan(t)->Projection->Projection->DataScan(t)->Projection->Projection}->Projection",
		},
		{
			sql:  "select (select count(*) from t where t.a = k.a) from t k",
			best: "Apply{DataScan(k)->DataScan(t)->Aggr(count(1))->Projection->MaxOneRow}->Projection",
		}, {
			sql:  "select a from t where exists(select 1 from t as x where x.a < t.a)",
			best: "Join{DataScan(t)->DataScan(x)}->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a = t.a and t.a < 1 and x.a < 1)",
			best: "Join{DataScan(t)->DataScan(x)}(Column#1,Column#13)->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a = t.a and x.a < 1) and a < 1",
			best: "Join{DataScan(t)->DataScan(x)}(Column#1,Column#13)->Projection",
		},
		{
			sql:  "select a from t where exists(select 1 from t as x where x.a = t.a) and exists(select 1 from t as x where x.a = t.a)",
			best: "Join{Join{DataScan(t)->DataScan(x)}(Column#1,Column#13)->DataScan(x)}(Column#1,Column#26)->Projection",
		},
		{
			sql:  "select * from (select a, b, sum(c) as s from t group by a, b) k where k.a > k.b * 2 + 1",
			best: "DataScan(t)->Aggr(sum(Column#3),firstrow(Column#1),firstrow(Column#2))->Projection->Projection",
		},
		{
			sql:  "select * from (select a, b, sum(c) as s from t group by a, b) k where k.a > 1 and k.b > 2",
			best: "DataScan(t)->Aggr(sum(Column#3),firstrow(Column#1),firstrow(Column#2))->Projection->Projection",
		},
		{
			sql:  "select * from (select k.a, sum(k.s) as ss from (select a, sum(b) as s from t group by a) k group by k.a) l where l.a > 2",
			best: "DataScan(t)->Aggr(sum(Column#2),firstrow(Column#1))->Projection->Aggr(sum(Column#15),firstrow(Column#14))->Projection->Projection",
		},
		{
			sql:  "select * from (select a, sum(b) as s from t group by a) k where a > s",
			best: "DataScan(t)->Aggr(sum(Column#2),firstrow(Column#1))->Sel([gt(cast(Column#1), Column#13)])->Projection->Projection",
		},
		{
			sql:  "select * from (select a, sum(b) as s from t group by a + 1) k where a > 1",
			best: "DataScan(t)->Aggr(sum(Column#2),firstrow(Column#1))->Sel([gt(Column#1, 1)])->Projection->Projection",
		},
		{
			sql:  "select * from (select a, sum(b) as s from t group by a having 1 = 0) k where a > 1",
			best: "Dual->Sel([gt(Column#14, 1)])->Projection",
		},
		{
			sql:  "select a, count(a) cnt from t group by a having cnt < 1",
			best: "DataScan(t)->Aggr(count(Column#1),firstrow(Column#1))->Sel([lt(Column#13, 1)])->Projection",
		},
		// issue #3873
		{
			sql:  "select t1.a, t2.a from t as t1 left join t as t2 on t1.a = t2.a where t1.a < 1.0",
			best: "Join{DataScan(t1)->DataScan(t2)}(Column#1,Column#13)->Projection",
		},
		// issue #7728
		{
			sql:  "select * from t t1 join t t2 on t1.a = t2.a where t2.a = null",
			best: "Dual->Projection",
		},
		{
			sql:  "select a, b from (select a, b, min(a) over(partition by b) as min_a from t)as tt where a < 10 and b > 10 and b = min_a",
			best: "DataScan(t)->Projection->Projection->Window(min(Column#13))->Sel([lt(Column#13, 10) eq(Column#14, Column#16)])->Projection->Projection",
		},
		{
			sql:  "select a, b from (select a, b, c, d, sum(a) over(partition by b, c) as sum_a from t)as tt where b + c > 10 and b in (1, 2) and sum_a > b",
			best: "DataScan(t)->Projection->Projection->Window(sum(cast(Column#13)))->Sel([gt(Column#18, cast(Column#14))])->Projection->Projection",
		},
	}
	s.Parser.EnableWindowFunc(true)
	defer func() {
		s.Parser.EnableWindowFunc(false)
	}()
	ctx := context.Background()
	for ith, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagDecorrelate|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, ca.best, Commentf("for %s %d", ca.sql, ith))
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
			left:  "[or(eq(Column#1, 1), eq(Column#1, 2))]",
			right: "[or(eq(Column#13, 1), eq(Column#13, 2))]",
		},
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b where (t1.c=1 and (t1.a=3 or t2.a=3)) or (t1.a=2 and t2.a=2)",
			left:  "[or(eq(Column#3, 1), eq(Column#1, 2))]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b where (t1.c=1 and ((t1.a=3 and t2.a=3) or (t1.a=4 and t2.a=4)))",
			left:  "[eq(Column#3, 1) or(eq(Column#1, 3), eq(Column#1, 4))]",
			right: "[or(eq(Column#13, 3), eq(Column#13, 4))]",
		},
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b where (t1.a>1 and t1.a < 3 and t2.a=1) or (t1.a=2 and t2.a=2)",
			left:  "[or(and(gt(Column#1, 1), lt(Column#1, 3)), eq(Column#1, 2))]",
			right: "[or(eq(Column#13, 1), eq(Column#13, 2))]",
		},
		{
			sql:   "select * from t as t1 join t as t2 on t1.b = t2.b and ((t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2))",
			left:  "[or(eq(Column#1, 1), eq(Column#1, 2))]",
			right: "[or(eq(Column#13, 1), eq(Column#13, 2))]",
		},
		// issue #7628, left join
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b and ((t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2))",
			left:  "[]",
			right: "[or(eq(Column#13, 1), eq(Column#13, 2))]",
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
			right: "[or(eq(Column#15, 1), eq(Column#13, 2))]",
		},
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b and ((t1.c=1 and ((t1.a=3 and t2.a=3) or (t1.a=4 and t2.a=4))) or (t1.a=2 and t2.a=2))",
			left:  "[]",
			right: "[or(or(eq(Column#13, 3), eq(Column#13, 4)), eq(Column#13, 2))]",
		},
		// Duplicate condition would be removed.
		{
			sql:   "select * from t t1 join t t2 on t1.a > 1 and t1.a > 1",
			left:  "[gt(Column#1, 1)]",
			right: "[]",
		},
	}

	ctx := context.Background()
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagDecorrelate|flagPrunColumns, p.(LogicalPlan))
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
			sel:   "[or(and(eq(Column#1, 1), isnull(Column#13)), and(eq(Column#1, 2), eq(Column#13, 2)))]",
			left:  "[or(eq(Column#1, 1), eq(Column#1, 2))]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b where (t1.c=1 and (t1.a=3 or t2.a=3)) or (t1.a=2 and t2.a=2)",
			sel:   "[or(and(eq(Column#3, 1), or(eq(Column#1, 3), eq(Column#13, 3))), and(eq(Column#1, 2), eq(Column#13, 2)))]",
			left:  "[or(eq(Column#3, 1), eq(Column#1, 2))]",
			right: "[]",
		},
		{
			sql:   "select * from t as t1 left join t as t2 on t1.b = t2.b where (t1.c=1 and ((t1.a=3 and t2.a=3) or (t1.a=4 and t2.a=4))) or (t1.a=2 and t2.a is null)",
			sel:   "[or(and(eq(Column#3, 1), or(and(eq(Column#1, 3), eq(Column#13, 3)), and(eq(Column#1, 4), eq(Column#13, 4)))), and(eq(Column#1, 2), isnull(Column#13)))]",
			left:  "[or(and(eq(Column#3, 1), or(eq(Column#1, 3), eq(Column#1, 4))), eq(Column#1, 2))]",
			right: "[]",
		},
	}

	ctx := context.Background()
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagDecorrelate|flagPrunColumns, p.(LogicalPlan))
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
			best:     "Join{DataScan(t1)->DataScan(t2)}(Column#2,Column#14)->Sel([or(gt(Column#3, 1), gt(Column#15, 1))])->Projection",
			joinType: "left outer join",
		},
		{
			sql:      "select * from t t1 left join t t2 on t1.b = t2.b where t1.c > 1 and t2.c > 1;",
			best:     "Join{DataScan(t1)->DataScan(t2)}(Column#2,Column#14)->Projection",
			joinType: "inner join",
		},
		{
			sql:      "select * from t t1 left join t t2 on t1.b = t2.b where not (t1.c > 1 or t2.c > 1);",
			best:     "Join{DataScan(t1)->DataScan(t2)}(Column#2,Column#14)->Projection",
			joinType: "inner join",
		},
		{
			sql:      "select * from t t1 left join t t2 on t1.b = t2.b where not (t1.c > 1 and t2.c > 1);",
			best:     "Join{DataScan(t1)->DataScan(t2)}(Column#2,Column#14)->Sel([not(and(gt(Column#3, 1), gt(Column#15, 1)))])->Projection",
			joinType: "left outer join",
		},
		{
			sql:      "select * from t t1 left join t t2 on t1.b > 1 where t1.c = t2.c;",
			best:     "Join{DataScan(t1)->DataScan(t2)}(Column#3,Column#15)->Projection",
			joinType: "inner join",
		},
		{
			sql:      "select * from t t1 left join t t2 on true where t1.b <=> t2.b;",
			best:     "Join{DataScan(t1)->DataScan(t2)}->Sel([nulleq(Column#2, Column#14)])->Projection",
			joinType: "left outer join",
		},
	}

	ctx := context.Background()
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		c.Assert(ToString(p), Equals, ca.best, comment)
		join, ok := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		if !ok {
			join, ok = p.(LogicalPlan).Children()[0].Children()[0].(*LogicalJoin)
			c.Assert(ok, IsTrue, comment)
		}
		c.Assert(join.JoinType.String(), Equals, ca.joinType, comment)
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
			best:     "Join{DataScan(t1)->DataScan(t2)}(Column#1,Column#13)->Projection",
			joinType: "anti semi join",
		},
	}

	ctx := context.Background()
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagDecorrelate|flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		c.Assert(ToString(p), Equals, ca.best, comment)
		join, _ := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		c.Assert(join.JoinType.String(), Equals, ca.joinType, comment)
	}
}

func (s *testPlanSuite) TestDeriveNotNullConds(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql   string
		plan  string
		left  string
		right string
	}{
		{
			sql:   "select * from t t1 inner join t t2 on t1.e = t2.e",
			plan:  "Join{DataScan(t1)->DataScan(t2)}(Column#5,Column#17)->Projection",
			left:  "[not(isnull(Column#5))]",
			right: "[not(isnull(Column#17))]",
		},
		{
			sql:   "select * from t t1 inner join t t2 on t1.e > t2.e",
			plan:  "Join{DataScan(t1)->DataScan(t2)}->Projection",
			left:  "[not(isnull(Column#5))]",
			right: "[not(isnull(Column#17))]",
		},
		{
			sql:   "select * from t t1 inner join t t2 on t1.e = t2.e and t1.e is not null",
			plan:  "Join{DataScan(t1)->DataScan(t2)}(Column#5,Column#17)->Projection",
			left:  "[not(isnull(Column#5))]",
			right: "[not(isnull(Column#17))]",
		},
		{
			sql:   "select * from t t1 left join t t2 on t1.e = t2.e",
			plan:  "Join{DataScan(t1)->DataScan(t2)}(Column#5,Column#17)->Projection",
			left:  "[]",
			right: "[not(isnull(Column#17))]",
		},
		{
			sql:   "select * from t t1 left join t t2 on t1.e > t2.e",
			plan:  "Join{DataScan(t1)->DataScan(t2)}->Projection",
			left:  "[]",
			right: "[not(isnull(Column#17))]",
		},
		{
			sql:   "select * from t t1 left join t t2 on t1.e = t2.e and t2.e is not null",
			plan:  "Join{DataScan(t1)->DataScan(t2)}(Column#5,Column#17)->Projection",
			left:  "[]",
			right: "[not(isnull(Column#17))]",
		},
		{
			sql:   "select * from t t1 right join t t2 on t1.e = t2.e and t1.e is not null",
			plan:  "Join{DataScan(t1)->DataScan(t2)}(Column#5,Column#17)->Projection",
			left:  "[not(isnull(Column#5))]",
			right: "[]",
		},
		{
			sql:   "select * from t t1 inner join t t2 on t1.e <=> t2.e",
			plan:  "Join{DataScan(t1)->DataScan(t2)}->Projection",
			left:  "[]",
			right: "[]",
		},
		{
			sql:   "select * from t t1 left join t t2 on t1.e <=> t2.e",
			plan:  "Join{DataScan(t1)->DataScan(t2)}->Projection",
			left:  "[]",
			right: "[]",
		},
		// Not deriving if column has NotNull flag already.
		{
			sql:   "select * from t t1 inner join t t2 on t1.b = t2.b",
			plan:  "Join{DataScan(t1)->DataScan(t2)}(Column#2,Column#14)->Projection",
			left:  "[]",
			right: "[]",
		},
		{
			sql:   "select * from t t1 left join t t2 on t1.b = t2.b",
			plan:  "Join{DataScan(t1)->DataScan(t2)}(Column#2,Column#14)->Projection",
			left:  "[]",
			right: "[]",
		},
		{
			sql:   "select * from t t1 left join t t2 on t1.b > t2.b",
			plan:  "Join{DataScan(t1)->DataScan(t2)}->Projection",
			left:  "[]",
			right: "[]",
		},
		// Not deriving for AntiSemiJoin
		{
			sql:   "select * from t t1 where not exists (select * from t t2 where t2.e = t1.e)",
			plan:  "Join{DataScan(t1)->DataScan(t2)}(Column#5,Column#17)->Projection",
			left:  "[]",
			right: "[]",
		},
	}

	ctx := context.Background()
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns|flagDecorrelate, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		c.Assert(ToString(p), Equals, ca.plan, comment)
		join := p.(LogicalPlan).Children()[0].(*LogicalJoin)
		left := join.Children()[0].(*DataSource)
		right := join.Children()[1].(*DataSource)
		leftConds := fmt.Sprintf("%s", left.pushedDownConds)
		rightConds := fmt.Sprintf("%s", right.pushedDownConds)
		c.Assert(leftConds, Equals, ca.left, comment)
		c.Assert(rightConds, Equals, ca.right, comment)
	}
}

func buildLogicPlan4GroupBy(s *testPlanSuite, c *C, sql string) (Plan, error) {
	sqlMode := s.ctx.GetSessionVars().SQLMode
	mockedTableInfo := MockSignedTable()
	// mock the table info here for later use
	// enable only full group by
	s.ctx.GetSessionVars().SQLMode = sqlMode | mysql.ModeOnlyFullGroupBy
	defer func() { s.ctx.GetSessionVars().SQLMode = sqlMode }() // restore it
	comment := Commentf("for %s", sql)
	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil, comment)

	stmt.(*ast.SelectStmt).From.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).TableInfo = mockedTableInfo

	p, err := BuildLogicalPlan(context.Background(), s.ctx, stmt, s.is)
	return p, err
}

func (s *testPlanSuite) TestGroupByWhenNotExistCols(c *C) {
	sqlTests := []struct {
		sql              string
		expectedErrMatch string
	}{
		{
			sql:              "select a from t group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has an as column alias
			sql:              "select a as tempField from t group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has as table alias
			sql:              "select tempTable.a from t as tempTable group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.tempTable\\.a'.*",
		},
		{
			// has a func call
			sql:              "select length(a) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two cols
			sql:              "select length(b + a) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two cols
			sql:              "select length(a + b) from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
		{
			// has a func call with two cols
			sql:              "select length(a + b) as tempField from t  group by b",
			expectedErrMatch: ".*contains nonaggregated column 'test\\.t\\.a'.*",
		},
	}
	for _, test := range sqlTests {
		sql := test.sql
		p, err := buildLogicPlan4GroupBy(s, c, sql)
		c.Assert(err, NotNil)
		c.Assert(p, IsNil)
		c.Assert(err, ErrorMatches, test.expectedErrMatch)
	}
}

func (s *testPlanSuite) TestDupRandJoinCondsPushDown(c *C) {
	sql := "select * from t as t1 join t t2 on t1.a > rand() and t1.a > rand()"
	comment := Commentf("for %s", sql)
	stmt, err := s.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil, comment)
	p, err := BuildLogicalPlan(context.Background(), s.ctx, stmt, s.is)
	c.Assert(err, IsNil, comment)
	p, err = logicalOptimize(context.TODO(), flagPredicatePushDown, p.(LogicalPlan))
	c.Assert(err, IsNil, comment)
	proj, ok := p.(*LogicalProjection)
	c.Assert(ok, IsTrue, comment)
	join, ok := proj.children[0].(*LogicalJoin)
	c.Assert(ok, IsTrue, comment)
	leftPlan, ok := join.children[0].(*LogicalSelection)
	c.Assert(ok, IsTrue, comment)
	leftCond := fmt.Sprintf("%s", leftPlan.Conditions)
	// Condition with mutable function cannot be de-duplicated when push down join conds.
	c.Assert(leftCond, Equals, "[gt(cast(Column#1), rand()) gt(cast(Column#1), rand())]", comment)
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
			sql:  "select * from t where t.ptn < 31",
			best: "UnionAll{Partition(41)->Partition(42)}->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.ptn < 61",
			best: "UnionAll{Partition(41)->Partition(42)->Partition(43)}->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.ptn > 17 and t.ptn < 61",
			best: "UnionAll{Partition(42)->Partition(43)}->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.ptn < 8",
			best: "Partition(41)->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.ptn > 128",
			best: "Partition(45)->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.ptn > 128",
			best: "Dual->Projection",
			is:   is1,
		},
		{
			// NULL will be located in the first partition.
			sql:  "select * from t where t.ptn is null",
			best: "Partition(41)->Projection",
			is:   is,
		},
		{
			sql:  "select * from t where t.ptn is null or t.ptn > 70",
			best: "UnionAll{Partition(41)->Partition(44)}->Projection",
			is:   is1,
		},
	}

	ctx := context.Background()
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, ca.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagDecorrelate|flagPrunColumns|flagPredicatePushDown|flagPartitionProcessor, p.(LogicalPlan))
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
			best: "Join{DataScan(t)->DataScan(s)}(Column#1,Column#13)->Projection",
		},
		{
			sql:  "select count(c) ,(select b from t s where s.a = t.a) from t",
			best: "Join{DataScan(t)->Aggr(count(Column#3),firstrow(Column#1))->DataScan(s)}(Column#1,Column#15)->Projection->Projection",
		},
		{
			sql:  "select count(c) ,(select count(s.b) from t s where s.a = t.a) from t",
			best: "Join{DataScan(t)->Aggr(count(Column#3),firstrow(Column#1))->DataScan(s)}(Column#1,Column#15)->Aggr(firstrow(Column#13),firstrow(Column#1),count(Column#16))->Projection->Projection",
		},
		{
			// Semi-join with agg cannot decorrelate.
			sql:  "select t.c in (select count(s.b) from t s where s.a = t.a) from t",
			best: "Apply{DataScan(t)->DataScan(s)->Sel([eq(Column#13, Column#1)])->Aggr(count(Column#14))}->Projection",
		},
		{
			sql:  "select (select count(s.b) k from t s where s.a = t.a having k != 0) from t",
			best: "Join{DataScan(t)->DataScan(s)->Aggr(count(Column#14),firstrow(Column#13))}(Column#1,Column#13)->Projection->Projection->Projection",
		},
		{
			sql:  "select (select count(s.b) k from t s where s.a = t1.a) from t t1, t t2",
			best: "Join{Join{DataScan(t1)->DataScan(t2)}->DataScan(s)->Aggr(count(Column#26),firstrow(Column#25))}(Column#1,Column#25)->Projection->Projection->Projection",
		},
		{
			sql:  "select (select count(1) k from t s where s.a = t.a having k != 0) from t",
			best: "Join{DataScan(t)->DataScan(s)->Aggr(count(1),firstrow(Column#13))}(Column#1,Column#13)->Projection->Projection->Projection",
		},
		{
			sql:  "select a from t where a in (select a from t s group by t.b)",
			best: "Join{DataScan(t)->DataScan(s)->Aggr(firstrow(Column#13))->Projection}(Column#1,Column#25)->Projection",
		},
		{
			// This will be resolved as in sub query.
			sql:  "select * from t where 10 in (((select b from t s where s.a = t.a)))",
			best: "Join{DataScan(t)->DataScan(s)}(Column#1,Column#13)->Projection",
		},
		{
			// This will be resolved as in function.
			sql:  "select * from t where 10 in (((select b from t s where s.a = t.a)), 10)",
			best: "Join{DataScan(t)->DataScan(s)}(Column#1,Column#13)->Projection->Sel([in(10, Column#25, 10)])->Projection",
		},
		{
			sql:  "select * from t where exists (select s.a from t s having sum(s.a) = t.a )",
			best: "Join{DataScan(t)->DataScan(s)->Aggr(sum(Column#13))->Projection}->Projection",
		},
		{
			// Test MaxOneRow for limit.
			sql:  "select (select * from (select b from t limit 1) x where x.b = t1.b) from t t1",
			best: "Join{DataScan(t1)->DataScan(t)->Projection->Limit}(Column#2,Column#25)->Projection->Projection",
		},
		{
			// Test Nested sub query.
			sql:  "select * from t where exists (select s.a from t s where s.c in (select c from t as k where k.d = s.d) having sum(s.a) = t.a )",
			best: "Join{DataScan(t)->Join{DataScan(s)->DataScan(k)}(Column#16,Column#28)(Column#15,Column#27)->Aggr(sum(Column#13))->Projection}->Projection",
		},
		{
			sql:  "select t1.b from t t1 where t1.b = (select max(t2.a) from t t2 where t1.b=t2.b)",
			best: "Join{DataScan(t1)->DataScan(t2)->Aggr(max(Column#13),firstrow(Column#14))}(Column#2,Column#14)->Projection->Sel([eq(Column#2, Column#26)])->Projection",
		},
		{
			sql:  "select t1.b from t t1 where t1.b = (select avg(t2.a) from t t2 where t1.g=t2.g and (t1.b = 4 or t2.b = 2))",
			best: "Apply{DataScan(t1)->DataScan(t2)->Sel([eq(Column#10, Column#22) or(eq(Column#2, 4), eq(Column#14, 2))])->Aggr(avg(Column#13))}->Projection->Sel([eq(cast(Column#2), Column#26)])->Projection",
		},
		{
			sql:  "select t1.b from t t1 where t1.b = (select max(t2.a) from t t2 where t1.b=t2.b order by t1.a)",
			best: "Join{DataScan(t1)->DataScan(t2)->Aggr(max(Column#13),firstrow(Column#14))}(Column#2,Column#14)->Projection->Sel([eq(Column#2, Column#26)])->Projection",
		},
		{
			sql:  "select t1.b from t t1 where t1.b in (select t2.b from t t2 where t2.a = t1.a order by t2.a)",
			best: "Join{DataScan(t1)->DataScan(t2)}(Column#1,Column#13)(Column#2,Column#14)->Projection",
		},
		{
			sql:  "select t1.b from t t1 where exists(select t2.b from t t2 where t2.a = t1.a order by t2.a)",
			best: "Join{DataScan(t1)->DataScan(t2)}(Column#1,Column#13)->Projection",
		},
		{
			// `Sort` will not be eliminated, if it is not the top level operator.
			sql:  "select t1.b from t t1 where t1.b = (select t2.b from t t2 where t2.a = t1.a order by t2.a limit 1)",
			best: "Apply{DataScan(t1)->DataScan(t2)->Sel([eq(Column#13, Column#1)])->Projection->Sort->Limit}->Projection->Sel([eq(Column#2, Column#27)])->Projection",
		},
	}

	ctx := context.Background()
	for ith, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		Preprocess(s.ctx, stmt, s.is)
		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		if lp, ok := p.(LogicalPlan); ok {
			p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagDecorrelate|flagPrunColumns, lp)
			c.Assert(err, IsNil)
		}
		c.Assert(ToString(p), Equals, ca.best, Commentf("for %s %d", ca.sql, ith))
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
			plan: "Show->Sel([eq(cast(Column#4), 0)])->Projection",
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
			plan: "LeftHashJoin{LeftHashJoin{TableReader(Table(t))->IndexLookUp(Index(t.c_d_e)[[666,666]], Table(t))}(Column#1,Column#14)->IndexReader(Index(t.c_d_e)[[42,42]])}(Column#2,Column#27)->Sel([or(Column#26, Column#40)])->Projection->Delete",
		},
		{
			sql:  "update t set a = 2 where b in (select c from t)",
			plan: "LeftHashJoin{TableReader(Table(t))->IndexReader(Index(t.c_d_e)[[NULL,+inf]])->HashAgg}(Column#2,Column#25)->Projection->Update",
		},
	}

	ctx := context.Background()
	for _, ca := range tests {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)

		s.ctx.GetSessionVars().HashJoinConcurrency = 1
		Preprocess(s.ctx, stmt, s.is)
		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		if lp, ok := p.(LogicalPlan); ok {
			p, err = logicalOptimize(context.TODO(), flagPrunColumns, lp)
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
			best: "Join{Join{Join{Join{DataScan(t1)->DataScan(t2)}(Column#1,Column#14)->DataScan(t3)}(Column#13,Column#26)->DataScan(t4)}(Column#27,Column#37)(Column#15,Column#40)->Join{DataScan(t5)->DataScan(t6)}(Column#52,Column#64)}->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5, t t6, t t7, t t8 where t1.a = t8.a",
			best: "Join{Join{Join{Join{DataScan(t1)->DataScan(t8)}(Column#1,Column#85)->DataScan(t2)}->Join{DataScan(t3)->DataScan(t4)}}->Join{Join{DataScan(t5)->DataScan(t6)}->DataScan(t7)}}->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5 where t1.a = t5.a and t5.a = t4.a and t4.a = t3.a and t3.a = t2.a and t2.a = t1.a and t1.a = t3.a and t2.a = t4.a and t5.b < 8",
			best: "Join{Join{Join{Join{DataScan(t5)->DataScan(t1)}(Column#49,Column#1)->DataScan(t2)}(Column#1,Column#13)->DataScan(t3)}(Column#13,Column#25)(Column#1,Column#25)->DataScan(t4)}(Column#49,Column#37)(Column#25,Column#37)(Column#13,Column#37)->Projection",
		},
		{
			sql:  "select * from t t1, t t2, t t3, t t4, t t5 where t1.a = t5.a and t5.a = t4.a and t4.a = t3.a and t3.a = t2.a and t2.a = t1.a and t1.a = t3.a and t2.a = t4.a and t3.b = 1 and t4.a = 1",
			best: "Join{Join{Join{DataScan(t3)->DataScan(t1)}->Join{DataScan(t2)->DataScan(t4)}}->DataScan(t5)}->Projection",
		},
		{
			sql:  "select * from t o where o.b in (select t3.c from t t1, t t2, t t3 where t1.a = t3.a and t2.a = t3.a and t2.a = o.a)",
			best: "Apply{DataScan(o)->Join{Join{DataScan(t1)->DataScan(t3)}(Column#13,Column#37)->DataScan(t2)}(Column#37,Column#25)->Projection}->Projection",
		},
		{
			sql:  "select * from t o where o.b in (select t3.c from t t1, t t2, t t3 where t1.a = t3.a and t2.a = t3.a and t2.a = o.a and t1.a = 1)",
			best: "Apply{DataScan(o)->Join{Join{DataScan(t1)->DataScan(t2)}->DataScan(t3)}->Projection}->Projection",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagJoinReOrder, p.(LogicalPlan))
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
			best: "DataScan(t)->Aggr(sum(Column#1),sum(plus(Column#1, 1)),count(Column#1))->Projection",
		},
		{
			sql:  "select sum(t.a + t.b), sum(t.a + t.c), sum(t.a + t.b), count(t.a) from t having sum(t.a + t.b) > 0 order by sum(t.a + t.c)",
			best: "DataScan(t)->Aggr(sum(plus(Column#1, Column#2)),sum(plus(Column#1, Column#3)),count(Column#1))->Sel([gt(Column#13, 0)])->Projection->Sort->Projection",
		},
		{
			sql:  "select sum(a.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(Column#1),firstrow(Column#3))->DataScan(b)}(Column#3,Column#15)->Aggr(sum(Column#27))->Projection",
		},
		{
			sql:  "select sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(Column#13),firstrow(Column#15))}(Column#3,Column#15)->Aggr(sum(Column#27))->Projection",
		},
		{
			sql:  "select sum(b.a), a.a from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(Column#13),firstrow(Column#15))}(Column#3,Column#15)->Aggr(sum(Column#28),firstrow(Column#1))->Projection",
		},
		{
			sql:  "select sum(a.a), b.a from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(Column#1),firstrow(Column#3))->DataScan(b)}(Column#3,Column#15)->Aggr(sum(Column#28),firstrow(Column#13))->Projection",
		},
		{
			sql:  "select sum(a.a), sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)}(Column#3,Column#15)->Aggr(sum(Column#1),sum(Column#13))->Projection",
		},
		{
			sql:  "select sum(a.a), max(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(Column#1),firstrow(Column#3))->DataScan(b)}(Column#3,Column#15)->Aggr(sum(Column#29),max(Column#13))->Projection",
		},
		{
			sql:  "select max(a.a), sum(b.a) from t a, t b where a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(Column#13),firstrow(Column#15))}(Column#3,Column#15)->Aggr(max(Column#1),sum(Column#29))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a, t b, t c where a.c = b.c and b.c = c.c",
			best: "Join{Join{DataScan(a)->DataScan(b)}(Column#3,Column#15)->DataScan(c)}(Column#15,Column#27)->Aggr(sum(Column#1))->Projection",
		},
		{
			sql:  "select sum(b.a) from t a left join t b on a.c = b.c",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(sum(Column#13),firstrow(Column#15))}(Column#3,Column#15)->Aggr(sum(Column#27))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a left join t b on a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(Column#1),firstrow(Column#3))->DataScan(b)}(Column#3,Column#15)->Aggr(sum(Column#27))->Projection",
		},
		{
			sql:  "select sum(a.a) from t a right join t b on a.c = b.c",
			best: "Join{DataScan(a)->Aggr(sum(Column#1),firstrow(Column#3))->DataScan(b)}(Column#3,Column#15)->Aggr(sum(Column#27))->Projection",
		},
		{
			sql:  "select sum(a) from (select * from t) x",
			best: "DataScan(t)->Aggr(sum(Column#1))->Projection",
		},
		{
			sql:  "select sum(c1) from (select c c1, d c2 from t a union all select a c1, b c2 from t b union all select b c1, e c2 from t c) x group by c2",
			best: "UnionAll{DataScan(a)->Projection->Aggr(sum(Column#41),firstrow(Column#42))->DataScan(b)->Projection->Aggr(sum(Column#27),firstrow(Column#28))->DataScan(c)->Projection->Aggr(sum(Column#13),firstrow(Column#14))}->Aggr(sum(Column#47))->Projection",
		},
		{
			sql:  "select max(a.b), max(b.b) from t a join t b on a.c = b.c group by a.a",
			best: "Join{DataScan(a)->DataScan(b)->Aggr(max(Column#14),firstrow(Column#15))}(Column#3,Column#15)->Projection->Projection",
		},
		{
			sql:  "select max(a.b), max(b.b) from t a join t b on a.a = b.a group by a.c",
			best: "Join{DataScan(a)->DataScan(b)}(Column#1,Column#13)->Aggr(max(Column#2),max(Column#14))->Projection",
		},
		{
			sql:  "select max(c.b) from (select * from t a union all select * from t b) c group by c.a",
			best: "UnionAll{DataScan(a)->Projection->Projection->Projection->DataScan(b)->Projection->Projection->Projection}->Aggr(max(Column#63))->Projection",
		},
		{
			sql:  "select max(a.c) from t a join t b on a.a=b.a and a.b=b.b group by a.b",
			best: "Join{DataScan(a)->DataScan(b)}(Column#1,Column#13)(Column#2,Column#14)->Aggr(max(Column#3))->Projection",
		},
		{
			sql:  "select t1.a, count(t2.b) from t t1, t t2 where t1.a = t2.a group by t1.a",
			best: "Join{DataScan(t1)->DataScan(t2)}(Column#1,Column#13)->Projection->Projection",
		},
	}

	ctx := context.Background()
	s.ctx.GetSessionVars().AllowAggPushDown = true
	for ith, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagPredicatePushDown|flagPrunColumns|flagPushDownAgg, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, Commentf("for %s %d", tt.sql, ith))
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
				1: {"Column#1"},
			},
		},
		{
			sql: "select count(*) from t",
			ans: map[int][]string{
				1: {"Column#1"},
			},
		},
		{
			sql: "select count(*) from t a join t b where a.a < 1",
			ans: map[int][]string{
				1: {"Column#1"},
				2: {"Column#13"},
			},
		},
		{
			sql: "select count(*) from t a join t b on a.a = b.d",
			ans: map[int][]string{
				1: {"Column#1"},
				2: {"Column#16"},
			},
		},
		{
			sql: "select count(*) from t a join t b on a.a = b.d order by sum(a.d)",
			ans: map[int][]string{
				1: {"Column#1", "Column#4"},
				2: {"Column#16"},
			},
		},
		{
			sql: "select count(b.a) from t a join t b on a.a = b.d group by b.b order by sum(a.d)",
			ans: map[int][]string{
				1: {"Column#1", "Column#4"},
				2: {"Column#13", "Column#14", "Column#16"},
			},
		},
		{
			sql: "select * from (select count(b.a) from t a join t b on a.a = b.d group by b.b having sum(a.d) < 0) tt",
			ans: map[int][]string{
				1: {"Column#1", "Column#4"},
				2: {"Column#13", "Column#14", "Column#16"},
			},
		},
		{
			sql: "select (select count(a) from t where b = k.a) from t k",
			ans: map[int][]string{
				1: {"Column#1"},
				3: {"Column#13", "Column#14"},
			},
		},
		{
			sql: "select exists (select count(*) from t where b = k.a) from t k",
			ans: map[int][]string{
				1: {"Column#1"},
			},
		},
		{
			sql: "select b = (select count(*) from t where b = k.a) from t k",
			ans: map[int][]string{
				1: {"Column#1", "Column#2"},
				3: {"Column#14"},
			},
		},
		{
			sql: "select exists (select count(a) from t where b = k.a group by b) from t k",
			ans: map[int][]string{
				1: {"Column#1"},
				3: {"Column#14"},
			},
		},
		{
			sql: "select a as c1, b as c2 from t order by 1, c1 + c2 + c",
			ans: map[int][]string{
				1: {"Column#1", "Column#2", "Column#3"},
			},
		},
		{
			sql: "select a from t where b < any (select c from t)",
			ans: map[int][]string{
				1: {"Column#1", "Column#2"},
				3: {"Column#15"},
			},
		},
		{
			sql: "select a from t where (b,a) != all (select c,d from t)",
			ans: map[int][]string{
				1: {"Column#1", "Column#2"},
				3: {"Column#15", "Column#16"},
			},
		},
		{
			sql: "select a from t where (b,a) in (select c,d from t)",
			ans: map[int][]string{
				1: {"Column#1", "Column#2"},
				3: {"Column#15", "Column#16"},
			},
		},
		{
			sql: "select a from t where a in (select a from t s group by t.b)",
			ans: map[int][]string{
				1: {"Column#1"},
				3: {"Column#13"},
			},
		},
		{
			sql: "select t01.a from (select a from t t21 union all select a from t t22) t2 join t t01 on 1 left outer join t t3 on 1 join t t4 on 1",
			ans: map[int][]string{
				1:  {"Column#1"},
				3:  {"Column#14"},
				5:  {"Column#27"},
				8:  {"Column#28"},
				10: {"Column#40"},
				12: {"Column#52"},
			},
		},
		{
			sql: "select 1 from (select count(b) as cnt from t) t1;",
			ans: map[int][]string{
				1: {"Column#1"},
			},
		},
		{
			sql: "select count(1) from (select count(b) as cnt from t) t1;",
			ans: map[int][]string{
				1: {"Column#1"},
			},
		},
		{
			sql: "select count(1) from (select count(b) as cnt from t group by c) t1;",
			ans: map[int][]string{
				1: {"Column#3"},
			},
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		lp, err := logicalOptimize(ctx, flagPredicatePushDown|flagPrunColumns, p.(LogicalPlan))
		c.Assert(err, IsNil)
		checkDataSourceCols(lp, c, tt.ans, comment)
	}
}

func (s *testPlanSuite) TestProjectionEliminator(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select 1+num from (select 1+a as num from t) t1;",
			best: "DataScan(t)->Projection",
		},
	}

	ctx := context.Background()
	for ith, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(context.TODO(), flagBuildKeyInfo|flagPrunColumns|flagEliminateProjection, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, Commentf("for %s %d", tt.sql, ith))
	}
}

func (s *testPlanSuite) TestAllocID(c *C) {
	ctx := MockContext()
	pA := DataSource{}.Init(ctx, 0)
	pB := DataSource{}.Init(ctx, 0)
	c.Assert(pA.id+1, Equals, pB.id)
}

func checkDataSourceCols(p LogicalPlan, c *C, ans map[int][]string, comment CommentInterface) {
	switch p.(type) {
	case *DataSource:
		colList, ok := ans[p.ID()]
		c.Assert(ok, IsTrue, Commentf("For %v DataSource ID %d Not found", comment, p.ID()))
		c.Assert(len(p.Schema().Columns), Equals, len(colList), comment)
		for i, colName := range colList {
			c.Assert(p.Schema().Columns[i].String(), Equals, colName, comment)
		}
	case *LogicalUnionAll:
		colList, ok := ans[p.ID()]
		c.Assert(ok, IsTrue, Commentf("For %v UnionAll ID %d Not found", comment, p.ID()))
		c.Assert(len(p.Schema().Columns), Equals, len(colList), comment)
		for i, colName := range colList {
			c.Assert(p.Schema().Columns[i].String(), Equals, colName, comment)
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

	ctx := context.Background()
	for _, tt := range tests {
		sql := tt.sql
		comment := Commentf("for %s", sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		_, err = BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
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
		c.Assert(len(p.Schema().Keys[i]), Equals, len(key), Commentf("for %s, %v %v, the number of column doesn't match", sql, p.ID(), key))
		for j, colName := range key {
			c.Assert(p.Schema().Keys[i][j].String(), Equals, colName, Commentf("for %s, %v %v, column dosen't match", sql, p.ID(), key))
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
				1: {{"Column#1"}},
				2: {{"Column#1"}},
				3: {{"Column#14"}},
			},
		},
		{
			sql: "select a, b, sum(f) from t group by b",
			ans: map[int][][]string{
				1: {{"Column#9"}, {"Column#1"}},
				2: {{"Column#1"}, {"Column#2"}},
				3: {{"Column#14"}, {"Column#15"}},
			},
		},
		{
			sql: "select c, d, e, sum(a) from t group by c, d, e",
			ans: map[int][][]string{
				1: {{"Column#1"}},
				2: {{"Column#3", "Column#4", "Column#5"}},
				3: {{"Column#14", "Column#15", "Column#16"}},
			},
		},
		{
			sql: "select f, g, sum(a) from t",
			ans: map[int][][]string{
				1: {{"Column#9"}, {"Column#9", "Column#10"}, {"Column#1"}},
				2: {{"Column#9"}, {"Column#9", "Column#10"}},
				3: {{"Column#14"}, {"Column#14", "Column#15"}},
			},
		},
		{
			sql: "select * from t t1 join t t2 on t1.a = t2.e",
			ans: map[int][][]string{
				1: {{"Column#9"}, {"Column#9", "Column#10"}, {"Column#1"}},
				2: {{"Column#21"}, {"Column#21", "Column#22"}, {"Column#13"}},
				3: {{"Column#21"}, {"Column#21", "Column#22"}, {"Column#13"}},
				4: {{"Column#45"}, {"Column#45", "Column#46"}, {"Column#37"}},
			},
		},
		{
			sql: "select f from t having sum(a) > 0",
			ans: map[int][][]string{
				1: {{"Column#9"}, {"Column#1"}},
				2: {{"Column#9"}},
				6: {{"Column#9"}},
				3: {{"Column#14"}},
				5: {{"Column#17"}},
			},
		},
		{
			sql: "select * from t t1 left join t t2 on t1.a = t2.a",
			ans: map[int][][]string{
				1: {{"Column#9"}, {"Column#9", "Column#10"}, {"Column#1"}},
				2: {{"Column#21"}, {"Column#21", "Column#22"}, {"Column#13"}},
				3: {{"Column#9"}, {"Column#9", "Column#10"}, {"Column#1"}},
				4: {{"Column#33"}, {"Column#33", "Column#34"}, {"Column#25"}},
			},
		},
	}

	ctx := context.Background()
	for ith, tt := range tests {
		comment := Commentf("for %s %d", tt.sql, ith)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)
		lp, err := logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns|flagBuildKeyInfo, p.(LogicalPlan))
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
			best: "DataScan(t)->Projection",
		},
		{
			sql:  "select sum(b) from t group by c, d, e",
			best: "DataScan(t)->Aggr(sum(Column#2))->Projection",
		},
		{
			sql:  "select tt.a, sum(tt.b) from (select a, b from t) tt group by tt.a",
			best: "DataScan(t)->Projection",
		},
		{
			sql:  "select count(1) from (select count(1), a as b from t group by a) tt group by b",
			best: "DataScan(t)->Projection",
		},
		{
			sql:  "select a, count(b) from t group by a",
			best: "DataScan(t)->Projection",
		},
		{
			sql:  "select a, count(distinct a, b) from t group by a",
			best: "DataScan(t)->Projection",
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)

		p, err := BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
		c.Assert(err, IsNil)

		p, err = logicalOptimize(context.TODO(), flagPredicatePushDown|flagPrunColumns|flagBuildKeyInfo|flagEliminateAgg|flagEliminateProjection, p.(LogicalPlan))
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
				{mysql.DropPriv, "test", "t", "", nil},
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
				{mysql.CreateViewPriv, "test", "", "", nil},
				{mysql.ShowViewPriv, "test", "", "", nil},
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
			sql: `grant select on ttt to 'test'@'%'`,
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
		{
			sql: "alter table t add column a int(4)",
			ans: []visitInfo{
				{mysql.AlterPriv, "test", "t", "", nil},
			},
		},
		{
			sql: "rename table t_old to t_new",
			ans: []visitInfo{
				{mysql.AlterPriv, "test", "t_old", "", nil},
				{mysql.DropPriv, "test", "t_old", "", nil},
				{mysql.CreatePriv, "test", "t_new", "", nil},
				{mysql.InsertPriv, "test", "t_new", "", nil},
			},
		},
		{
			sql: "alter table t_old rename to t_new",
			ans: []visitInfo{
				{mysql.AlterPriv, "test", "t_old", "", nil},
				{mysql.DropPriv, "test", "t_old", "", nil},
				{mysql.CreatePriv, "test", "t_new", "", nil},
				{mysql.InsertPriv, "test", "t_new", "", nil},
			},
		},
		{
			sql: "alter table t drop partition p0;",
			ans: []visitInfo{
				{mysql.AlterPriv, "test", "t", "", nil},
				{mysql.DropPriv, "test", "t", "", nil},
			},
		},
	}

	for _, tt := range tests {
		comment := Commentf("for %s", tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &BlockHintProcessor{})
		builder.ctx.GetSessionVars().HashJoinConcurrency = 1
		_, err = builder.Build(context.TODO(), stmt)
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
		// loose compare errors for code match
		c.Assert(terror.ErrorEqual(v1[i].err, v2[i].err), IsTrue, Commentf("err1 %v, err2 %v for %s", v1[i].err, v2[i].err, comment))
		// compare remainder
		v1[i].err = v2[i].err
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
			best: "UnionAll{DataScan(t)->Projection->DataScan(t)->Projection}->Aggr(firstrow(Column#27))",
			err:  false,
		},
		{
			sql:  "select a from t union all select a from t",
			best: "UnionAll{DataScan(t)->Projection->DataScan(t)->Projection}",
			err:  false,
		},
		{
			sql:  "select a from t union select a from t union all select a from t",
			best: "UnionAll{UnionAll{DataScan(t)->Projection->DataScan(t)->Projection}->Aggr(firstrow(Column#40))->Projection->DataScan(t)->Projection}",
			err:  false,
		},
		{
			sql:  "select a from t union select a from t union all select a from t union select a from t union select a from t",
			best: "UnionAll{DataScan(t)->Projection->DataScan(t)->Projection->DataScan(t)->Projection->DataScan(t)->Projection->DataScan(t)->Projection}->Aggr(firstrow(Column#66))",
			err:  false,
		},
		{
			sql:  "select a from t union select a, b from t",
			best: "",
			err:  true,
		},
		{
			sql:  "select * from (select 1 as a  union select 1 union all select 2) t order by a",
			best: "UnionAll{UnionAll{Dual->Projection->Dual->Projection}->Aggr(firstrow(Column#4))->Projection->Dual->Projection}->Projection->Sort",
			err:  false,
		},
		{
			sql:  "select * from (select 1 as a  union select 1 union all select 2) t order by (select a)",
			best: "Apply{UnionAll{UnionAll{Dual->Projection->Dual->Projection}->Aggr(firstrow(Column#4))->Projection->Dual->Projection}->Dual->Projection->MaxOneRow}->Sort->Projection",
			err:  false,
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &BlockHintProcessor{})
		plan, err := builder.Build(ctx, stmt)
		if tt.err {
			c.Assert(err, NotNil)
			continue
		}
		c.Assert(err, IsNil)
		p := plan.(LogicalPlan)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
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
			best: "DataScan(t)->TopN([Column#2],0,5)->Projection",
		},
		// Test Limit + Selection.
		{
			sql:  "select * from t where a < 1 limit 5",
			best: "DataScan(t)->Limit->Projection",
		},
		// Test Limit + Agg + Proj .
		{
			sql:  "select a, count(b) from t group by b limit 5",
			best: "DataScan(t)->Aggr(count(Column#2),firstrow(Column#1))->Limit->Projection",
		},
		// Test TopN + Agg + Proj .
		{
			sql:  "select a, count(b) from t group by b order by c limit 5",
			best: "DataScan(t)->Aggr(count(Column#2),firstrow(Column#1),firstrow(Column#3))->TopN([Column#3],0,5)->Projection",
		},
		// Test TopN + Join + Proj.
		{
			sql:  "select * from t, t s order by t.a limit 5",
			best: "Join{DataScan(t)->DataScan(s)}->TopN([Column#1],0,5)->Projection",
		},
		// Test Limit + Join + Proj.
		{
			sql:  "select * from t, t s limit 5",
			best: "Join{DataScan(t)->DataScan(s)}->Limit->Projection",
		},
		// Test TopN + Left Join + Proj.
		{
			sql:  "select * from t left outer join t s on t.a = s.a order by t.a limit 5",
			best: "Join{DataScan(t)->TopN([Column#1],0,5)->DataScan(s)}(Column#1,Column#13)->TopN([Column#1],0,5)->Projection",
		},
		// Test TopN + Left Join + Proj.
		{
			sql:  "select * from t left outer join t s on t.a = s.a order by t.a limit 5, 5",
			best: "Join{DataScan(t)->TopN([Column#1],0,10)->DataScan(s)}(Column#1,Column#13)->TopN([Column#1],5,5)->Projection",
		},
		// Test Limit + Left Join + Proj.
		{
			sql:  "select * from t left outer join t s on t.a = s.a limit 5",
			best: "Join{DataScan(t)->Limit->DataScan(s)}(Column#1,Column#13)->Limit->Projection",
		},
		// Test Limit + Left Join Apply + Proj.
		{
			sql:  "select (select s.a from t s where t.a = s.a) from t limit 5",
			best: "Join{DataScan(t)->Limit->DataScan(s)}(Column#1,Column#13)->Limit->Projection",
		},
		// Test TopN + Left Join Apply + Proj.
		{
			sql:  "select (select s.a from t s where t.a = s.a) from t order by t.a limit 5",
			best: "Join{DataScan(t)->TopN([Column#1],0,5)->DataScan(s)}(Column#1,Column#13)->TopN([Column#1],0,5)->Projection",
		},
		// Test TopN + Left Semi Join Apply + Proj.
		{
			sql:  "select exists (select s.a from t s where t.a = s.a) from t order by t.a limit 5",
			best: "Join{DataScan(t)->TopN([Column#1],0,5)->DataScan(s)}(Column#1,Column#13)->TopN([Column#1],0,5)->Projection",
		},
		// Test TopN + Semi Join Apply + Proj.
		{
			sql:  "select * from t where exists (select s.a from t s where t.a = s.a) order by t.a limit 5",
			best: "Join{DataScan(t)->DataScan(s)}(Column#1,Column#13)->TopN([Column#1],0,5)->Projection",
		},
		// Test TopN + Right Join + Proj.
		{
			sql:  "select * from t right outer join t s on t.a = s.a order by s.a limit 5",
			best: "Join{DataScan(t)->DataScan(s)->TopN([Column#13],0,5)}(Column#1,Column#13)->TopN([Column#13],0,5)->Projection",
		},
		// Test Limit + Right Join + Proj.
		{
			sql:  "select * from t right outer join t s on t.a = s.a order by s.a,t.b limit 5",
			best: "Join{DataScan(t)->DataScan(s)}(Column#1,Column#13)->TopN([Column#13 Column#2],0,5)->Projection",
		},
		// Test TopN + UA + Proj.
		{
			sql:  "select * from t union all (select * from t s) order by a,b limit 5",
			best: "UnionAll{DataScan(t)->TopN([Column#25 Column#26],0,5)->Projection->DataScan(s)->TopN([Column#1 Column#2],0,5)->Projection}->TopN([Column#49 Column#50],0,5)",
		},
		// Test TopN + UA + Proj.
		{
			sql:  "select * from t union all (select * from t s) order by a,b limit 5, 5",
			best: "UnionAll{DataScan(t)->TopN([Column#25 Column#26],0,10)->Projection->DataScan(s)->TopN([Column#1 Column#2],0,10)->Projection}->TopN([Column#49 Column#50],5,5)",
		},
		// Test Limit + UA + Proj + Sort.
		{
			sql:  "select * from t union all (select * from t s order by a) limit 5",
			best: "UnionAll{DataScan(t)->Limit->Projection->DataScan(s)->TopN([Column#1],0,5)->Projection}->Limit",
		},
		// Test `ByItem` containing column from both sides.
		{
			sql:  "select ifnull(t1.b, t2.a) from t t1 left join t t2 on t1.e=t2.e order by ifnull(t1.b, t2.a) limit 5",
			best: "Join{DataScan(t1)->TopN([Column#2],0,5)->DataScan(t2)}(Column#5,Column#17)->TopN([Column#2],0,5)->Projection",
		},
		// Test ifnull cannot be eliminated
		{
			sql:  "select ifnull(t1.h, t2.b) from t t1 left join t t2 on t1.e=t2.e order by ifnull(t1.h, t2.b) limit 5",
			best: "Join{DataScan(t1)->DataScan(t2)}(Column#5,Column#17)->TopN([ifnull(Column#11, Column#14)],0,5)->Projection->Projection",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
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

	ctx := context.Background()
	for _, t := range tests {
		comment := Commentf("for %s", t.sql)
		stmt, err := s.ParseOneStmt(t.sql, "", "")
		c.Assert(err, IsNil, comment)
		s.ctx.GetSessionVars().HashJoinConcurrency = 1

		_, err = BuildLogicalPlan(ctx, s.ctx, stmt, s.is)
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
			best: "DataScan(t1)->Aggr(firstrow(Column#1),firstrow(Column#2))",
		},
		// Test right outer join + distinct
		{
			sql:  "select distinct t2.a, t2.b from t t1 right outer join t t2 on t1.b = t2.b",
			best: "DataScan(t2)->Aggr(firstrow(Column#13),firstrow(Column#14))",
		},
		// Test duplicate agnostic agg functions on join
		{
			sql:  "select max(t1.a), min(test.t1.b) from t t1 left join t t2 on t1.b = t2.b",
			best: "DataScan(t1)->Aggr(max(Column#1),min(Column#2))->Projection",
		},
		{
			sql:  "select sum(distinct t1.a) from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b",
			best: "DataScan(t1)->Aggr(sum(Column#1))->Projection",
		},
		{
			sql:  "select count(distinct t1.a, t1.b) from t t1 left join t t2 on t1.b = t2.b",
			best: "DataScan(t1)->Aggr(count(Column#1, Column#2))->Projection",
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
			best: "Join{Join{DataScan(t1)->DataScan(t2)}(Column#1,Column#13)->DataScan(t3)->TopN([Column#26 true],0,1)}(Column#2,Column#26)->TopN([Column#26 true],0,1)->Aggr(max(Column#26))->Projection",
		},
		{
			sql:  "select t1.a ta, t1.b tb from t t1 left join t t2 on t1.a = t2.a",
			best: "DataScan(t1)->Projection",
		},
		{
			// Because the `order by` uses t2.a, the `join` can't be eliminated.
			sql:  "select t1.a, t1.b from t t1 left join t t2 on t1.a = t2.a order by t2.a",
			best: "Join{DataScan(t1)->DataScan(t2)}(Column#1,Column#13)->Sort->Projection",
		},
		// For issue 11167
		{
			sql:  "select a.a from t a natural left join t b natural left join t c",
			best: "DataScan(a)->Projection",
		},
	}

	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
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
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
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
			result: "TableReader(Table(t))->Window(avg(cast(Column#1)) over(partition by Column#1))->Projection",
		},
		{
			sql:    "select a, avg(a) over(partition by b) from t",
			result: "TableReader(Table(t))->Sort->Window(avg(cast(Column#1)) over(partition by Column#2))->Projection",
		},
		{
			sql:    "select a, avg(a+1) over(partition by (a+1)) from t",
			result: "IndexReader(Index(t.f)[[NULL,+inf]])->Projection->Sort->Window(avg(cast(Column#17)) over(partition by Column#16))->Projection",
		},
		{
			sql:    "select a, avg(a) over(order by a asc, b desc) from t order by a asc, b desc",
			result: "TableReader(Table(t))->Sort->Window(avg(cast(Column#1)) over(order by Column#1 asc, Column#2 desc range between unbounded preceding and current row))->Projection",
		},
		{
			sql:    "select a, b as a, avg(a) over(partition by a) from t",
			result: "TableReader(Table(t))->Window(avg(cast(Column#1)) over(partition by Column#1))->Projection",
		},
		{
			sql:    "select a, b as z, sum(z) over() from t",
			result: "[planner:1054]Unknown column 'z' in 'field list'",
		},
		{
			sql:    "select a, b as z from t order by (sum(z) over())",
			result: "TableReader(Table(t))->Window(sum(cast(Column#2)) over())->Sort->Projection",
		},
		{
			sql:    "select sum(avg(a)) over() from t",
			result: "IndexReader(Index(t.f)[[NULL,+inf]]->StreamAgg)->StreamAgg->Window(sum(Column#13) over())->Projection",
		},
		{
			sql:    "select b from t order by(sum(a) over())",
			result: "TableReader(Table(t))->Window(sum(cast(Column#1)) over())->Sort->Projection",
		},
		{
			sql:    "select b from t order by(sum(a) over(partition by a))",
			result: "TableReader(Table(t))->Window(sum(cast(Column#1)) over(partition by Column#1))->Sort->Projection",
		},
		{
			sql:    "select b from t order by(sum(avg(a)) over())",
			result: "TableReader(Table(t)->StreamAgg)->StreamAgg->Window(sum(Column#13) over())->Sort->Projection",
		},
		{
			sql:    "select a from t having (select sum(a) over() as w from t tt where a > t.a)",
			result: "Apply{IndexReader(Index(t.f)[[NULL,+inf]])->IndexReader(Index(t.f)[[NULL,+inf]]->Sel([gt(Column#14, Column#1)]))->Window(sum(cast(Column#14)) over())->MaxOneRow->Sel([Column#28])}->Projection",
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
			sql:    "SELECT FIRST_VALUE(a) RESPECT NULLS OVER (w1 PARTITION BY b ORDER BY b ASC, a DESC ROWS 2 PRECEDING) AS 'first_value', a, b FROM ( SELECT a, b FROM `t` ) as t WINDOW w1 AS (PARTITION BY b ORDER BY b ASC, a ASC );",
			result: "[planner:3581]A window which depends on another cannot define partitioning.",
		},
		{
			sql:    "select sum(a) over(w) from t window w as (rows between 1 preceding AND 1 following)",
			result: "[planner:3582]Window 'w' has a frame definition, so cannot be referenced by another window.",
		},
		{
			sql:    "select sum(a) over w from t window w as (rows between 1 preceding AND 1 following)",
			result: "IndexReader(Index(t.f)[[NULL,+inf]])->Window(sum(cast(Column#1)) over(rows between 1 preceding and 1 following))->Projection",
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
			sql:    "select avg(a) over(w2) from t window w1 as (partition by a), w2 as (w1)",
			result: "TableReader(Table(t))->Window(avg(cast(Column#1)) over(partition by Column#1))->Projection",
		},
		{
			sql:    "select a from t window w1 as (partition by a) order by (sum(a) over(w1))",
			result: "TableReader(Table(t))->Window(sum(cast(Column#1)) over(partition by Column#1))->Sort->Projection",
		},
		{
			sql:    "select sum(a) over(groups 1 preceding) from t",
			result: "[planner:1235]This version of TiDB doesn't yet support 'GROUPS'",
		},
		{
			sql:    "select sum(a) over(rows between unbounded following and 1 preceding) from t",
			result: "[planner:3584]Window '<unnamed window>': frame start cannot be UNBOUNDED FOLLOWING.",
		},
		{
			sql:    "select sum(a) over(rows between current row and unbounded preceding) from t",
			result: "[planner:3585]Window '<unnamed window>': frame end cannot be UNBOUNDED PRECEDING.",
		},
		{
			sql:    "select sum(a) over(rows interval 1 MINUTE_SECOND preceding) from t",
			result: "[planner:3596]Window '<unnamed window>': INTERVAL can only be used with RANGE frames.",
		},
		{
			sql:    "select sum(a) over(rows between 1.0 preceding and 1 following) from t",
			result: "[planner:3586]Window '<unnamed window>': frame start or end is negative, NULL or of non-integral type",
		},
		{
			sql:    "select sum(a) over(range between 1 preceding and 1 following) from t",
			result: "[planner:3587]Window '<unnamed window>' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type",
		},
		{
			sql:    "select sum(a) over(order by c_str range between 1 preceding and 1 following) from t",
			result: "[planner:3587]Window '<unnamed window>' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type",
		},
		{
			sql:    "select sum(a) over(order by a range interval 1 MINUTE_SECOND preceding) from t",
			result: "[planner:3589]Window '<unnamed window>' with RANGE frame has ORDER BY expression of numeric type, INTERVAL bound value not allowed.",
		},
		{
			sql:    "select sum(a) over(order by i_date range interval a MINUTE_SECOND preceding) from t",
			result: "[planner:3590]Window '<unnamed window>' has a non-constant frame bound.",
		},
		{
			sql:    "select sum(a) over(order by i_date range interval -1 MINUTE_SECOND preceding) from t",
			result: "[planner:3586]Window '<unnamed window>': frame start or end is negative, NULL or of non-integral type",
		},
		{
			sql:    "select sum(a) over(order by i_date range 1 preceding) from t",
			result: "[planner:3588]Window '<unnamed window>' with RANGE frame has ORDER BY expression of datetime type. Only INTERVAL bound value allowed.",
		},
		{
			sql:    "select sum(a) over(order by a range between 1.0 preceding and 1 following) from t",
			result: "TableReader(Table(t))->Window(sum(cast(Column#1)) over(order by Column#1 asc range between 1.0 preceding and 1 following))->Projection",
		},
		{
			sql:    "select row_number() over(rows between 1 preceding and 1 following) from t",
			result: "IndexReader(Index(t.f)[[NULL,+inf]])->Window(row_number() over())->Projection",
		},
		{
			sql:    "select avg(b), max(avg(b)) over(rows between 1 preceding and 1 following) max from t group by c",
			result: "TableReader(Table(t))->HashAgg->Window(max(Column#13) over(rows between 1 preceding and 1 following))->Projection",
		},
		{
			sql:    "select nth_value(a, 1.0) over() from t",
			result: "[planner:1210]Incorrect arguments to nth_value",
		},
		{
			sql:    "SELECT NTH_VALUE(a, 1.0) OVER() FROM t",
			result: "[planner:1210]Incorrect arguments to nth_value",
		},
		{
			sql:    "select nth_value(a, 0) over() from t",
			result: "[planner:1210]Incorrect arguments to nth_value",
		},
		{
			sql:    "select ntile(0) over() from t",
			result: "[planner:1210]Incorrect arguments to ntile",
		},
		{
			sql:    "select ntile(null) over() from t",
			result: "IndexReader(Index(t.f)[[NULL,+inf]])->Window(ntile(<nil>) over())->Projection",
		},
		{
			sql:    "select avg(a) over w from t window w as(partition by b)",
			result: "TableReader(Table(t))->Sort->Window(avg(cast(Column#1)) over(partition by Column#2))->Projection",
		},
		{
			sql:    "select nth_value(i_date, 1) over() from t",
			result: "TableReader(Table(t))->Window(nth_value(Column#12, 1) over())->Projection",
		},
		{
			sql:    "select sum(b) over w, sum(c) over w from t window w as (order by a)",
			result: "TableReader(Table(t))->Window(sum(cast(Column#2)), sum(cast(Column#3)) over(order by Column#1 asc range between unbounded preceding and current row))->Projection",
		},
		{
			sql:    "delete from t order by (sum(a) over())",
			result: "[planner:3593]You cannot use the window function 'sum' in this context.'",
		},
		{
			sql:    "delete from t order by (SUM(a) over())",
			result: "[planner:3593]You cannot use the window function 'sum' in this context.'",
		},
		{
			sql:    "SELECT * from t having ROW_NUMBER() over()",
			result: "[planner:3593]You cannot use the window function 'row_number' in this context.'",
		},
		{
			// The best execution order should be (a,c), (a, b, c), (a, b), (), it requires only 2 sort operations.
			sql:    "select sum(a) over (partition by a order by b), sum(b) over (order by a, b, c), sum(c) over(partition by a order by c), sum(d) over() from t",
			result: "TableReader(Table(t))->Sort->Window(sum(cast(Column#3)) over(partition by Column#1 order by Column#3 asc range between unbounded preceding and current row))->Sort->Window(sum(cast(Column#2)) over(order by Column#1 asc, Column#2 asc, Column#3 asc range between unbounded preceding and current row))->Window(sum(cast(Column#1)) over(partition by Column#1 order by Column#2 asc range between unbounded preceding and current row))->Window(sum(cast(Column#4)) over())->Projection",
		},
		// Test issue 11010.
		{
			sql:    "select dense_rank() over w1, a, b from t window w1 as (partition by t.b order by t.a desc, t.b desc range between current row and 1 following)",
			result: "[planner:3587]Window 'w1' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type",
		},
		{
			sql:    "select dense_rank() over w1, a, b from t window w1 as (partition by t.b order by t.a desc, t.b desc range between current row and unbounded following)",
			result: "TableReader(Table(t))->Sort->Window(dense_rank() over(partition by Column#2 order by Column#1 desc, Column#2 desc))->Projection",
		},
		{
			sql:    "select dense_rank() over w1, a, b from t window w1 as (partition by t.b order by t.a desc, t.b desc range between 1 preceding and 1 following)",
			result: "[planner:3587]Window 'w1' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type",
		},
		// Test issue 11001.
		{
			sql:    "SELECT PERCENT_RANK() OVER w1 AS 'percent_rank', fieldA, fieldB FROM ( SELECT a AS fieldA, b AS fieldB FROM t ) t1 WINDOW w1 AS ( ROWS BETWEEN 0 FOLLOWING AND UNBOUNDED PRECEDING)",
			result: "[planner:3585]Window 'w1': frame end cannot be UNBOUNDED PRECEDING.",
		},
		// Test issue 11002.
		{
			sql:    "SELECT PERCENT_RANK() OVER w1 AS 'percent_rank', fieldA, fieldB FROM ( SELECT a AS fieldA, b AS fieldB FROM t ) as t1 WINDOW w1 AS ( ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING)",
			result: "[planner:3584]Window 'w1': frame start cannot be UNBOUNDED FOLLOWING.",
		},
		// Test issue 11011.
		{
			sql:    "select dense_rank() over w1, a, b from t window w1 as (partition by t.b order by t.a asc range between 1250951168 following AND 1250951168 preceding)",
			result: "[planner:3586]Window 'w1': frame start or end is negative, NULL or of non-integral type",
		},
		// Test issue 10556.
		{
			sql:    "SELECT FIRST_VALUE(a) IGNORE NULLS OVER () FROM t",
			result: "[planner:1235]This version of TiDB doesn't yet support 'IGNORE NULLS'",
		},
		{
			sql:    "SELECT SUM(DISTINCT a) OVER () FROM t",
			result: "[planner:1235]This version of TiDB doesn't yet support '<window function>(DISTINCT ..)'",
		},
		{
			sql:    "SELECT NTH_VALUE(a, 1) FROM LAST over (partition by b order by b), a FROM t",
			result: "[planner:1235]This version of TiDB doesn't yet support 'FROM LAST'",
		},
		{
			sql:    "SELECT NTH_VALUE(a, 1) FROM LAST IGNORE NULLS over (partition by b order by b), a FROM t",
			result: "[planner:1235]This version of TiDB doesn't yet support 'IGNORE NULLS'",
		},
		{
			sql:    "SELECT NTH_VALUE(fieldA, ATAN(-1)) OVER (w1) AS 'ntile', fieldA, fieldB FROM ( SELECT a AS fieldA, b AS fieldB FROM t ) as te WINDOW w1 AS ( ORDER BY fieldB ASC, fieldA DESC )",
			result: "[planner:1210]Incorrect arguments to nth_value",
		},
		{
			sql:    "SELECT NTH_VALUE(fieldA, -1) OVER (w1 PARTITION BY fieldB ORDER BY fieldB , fieldA ) AS 'ntile', fieldA, fieldB FROM ( SELECT a AS fieldA, b AS fieldB FROM t ) as temp WINDOW w1 AS ( ORDER BY fieldB ASC, fieldA DESC )",
			result: "[planner:1210]Incorrect arguments to nth_value",
		},
		{
			sql:    "SELECT SUM(a) OVER w AS 'sum' FROM t WINDOW w AS (ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW )",
			result: "[planner:3586]Window 'w': frame start or end is negative, NULL or of non-integral type",
		},
		{
			sql:    "SELECT SUM(a) OVER w AS 'sum' FROM t WINDOW w AS (ROWS BETWEEN CURRENT ROW AND 1 PRECEDING )",
			result: "[planner:3586]Window 'w': frame start or end is negative, NULL or of non-integral type",
		},
		{
			sql:    "SELECT SUM(a) OVER w AS 'sum' FROM t WINDOW w AS (ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING )",
			result: "[planner:3586]Window 'w': frame start or end is negative, NULL or of non-integral type",
		},
		// Test issue 11943
		{
			sql:    "SELECT ROW_NUMBER() OVER (partition by b) + a FROM t",
			result: "TableReader(Table(t))->Sort->Window(row_number() over(partition by Column#2))->Projection",
		},
	}

	s.Parser.EnableWindowFunc(true)
	defer func() {
		s.Parser.EnableWindowFunc(false)
	}()
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		p, stmt, err := s.optimize(ctx, tt.sql)
		if err != nil {
			c.Assert(err.Error(), Equals, tt.result, comment)
			continue
		}
		c.Assert(ToString(p), Equals, tt.result, comment)

		var sb strings.Builder
		// After restore, the result should be the same.
		err = stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
		c.Assert(err, IsNil)
		p, _, err = s.optimize(ctx, sb.String())
		if err != nil {
			c.Assert(err.Error(), Equals, tt.result, comment)
			continue
		}
		c.Assert(ToString(p), Equals, tt.result, comment)
	}
}

func (s *testPlanSuite) optimize(ctx context.Context, sql string) (PhysicalPlan, ast.Node, error) {
	stmt, err := s.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, nil, err
	}
	err = Preprocess(s.ctx, stmt, s.is)
	if err != nil {
		return nil, nil, err
	}
	builder := NewPlanBuilder(MockContext(), s.is, &BlockHintProcessor{})
	p, err := builder.Build(ctx, stmt)
	if err != nil {
		return nil, nil, err
	}
	p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
	if err != nil {
		return nil, nil, err
	}
	p, err = physicalOptimize(p.(LogicalPlan))
	return p.(PhysicalPlan), stmt, err
}

func byItemsToProperty(byItems []*ByItems) *property.PhysicalProperty {
	pp := &property.PhysicalProperty{}
	for _, item := range byItems {
		pp.Items = append(pp.Items, property.Item{Col: item.Expr.(*expression.Column), Desc: item.Desc})
	}
	return pp
}

func pathsName(paths []*candidatePath) string {
	var names []string
	for _, path := range paths {
		if path.path.isTablePath {
			names = append(names, "PRIMARY_KEY")
		} else {
			names = append(names, path.path.index.Name.O)
		}
	}
	return strings.Join(names, ",")
}

func (s *testPlanSuite) TestSkylinePruning(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql    string
		result string
	}{
		{
			sql:    "select * from t",
			result: "PRIMARY_KEY",
		},
		{
			sql:    "select * from t order by f",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select * from t where a > 1",
			result: "PRIMARY_KEY",
		},
		{
			sql:    "select * from t where a > 1 order by f",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select * from t where f > 1",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select f from t where f > 1",
			result: "f,f_g",
		},
		{
			sql:    "select f from t where f > 1 order by a",
			result: "PRIMARY_KEY,f,f_g",
		},
		{
			sql:    "select * from t where f > 1 and g > 1",
			result: "PRIMARY_KEY,f,g,f_g",
		},
		{
			sql:    "select count(1) from t",
			result: "PRIMARY_KEY,c_d_e,f,g,f_g,c_d_e_str,e_d_c_str_prefix",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		if err != nil {
			c.Assert(err.Error(), Equals, tt.result, comment)
			continue
		}
		c.Assert(err, IsNil, comment)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil, comment)
		lp := p.(LogicalPlan)
		_, err = lp.recursiveDeriveStats()
		c.Assert(err, IsNil, comment)
		var ds *DataSource
		var byItems []*ByItems
		for ds == nil {
			switch v := lp.(type) {
			case *DataSource:
				ds = v
			case *LogicalSort:
				byItems = v.ByItems
				lp = lp.Children()[0]
			case *LogicalProjection:
				newItems := make([]*ByItems, 0, len(byItems))
				for _, col := range byItems {
					idx := v.schema.ColumnIndex(col.Expr.(*expression.Column))
					switch expr := v.Exprs[idx].(type) {
					case *expression.Column:
						newItems = append(newItems, &ByItems{Expr: expr, Desc: col.Desc})
					}
				}
				byItems = newItems
				lp = lp.Children()[0]
			default:
				lp = lp.Children()[0]
			}
		}
		paths := ds.skylinePruning(byItemsToProperty(byItems))
		c.Assert(pathsName(paths), Equals, tt.result, comment)
	}
}

func (s *testPlanSuite) TestFastPlanContextTables(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql      string
		fastPlan bool
	}{
		{
			"select * from t where a=1",
			true,
		},
		{

			"update t set f=0 where a=43215",
			true,
		},
		{
			"delete from t where a =43215",
			true,
		},
		{
			"select * from t where a>1",
			false,
		},
	}
	for _, tt := range tests {
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil)
		Preprocess(s.ctx, stmt, s.is)
		s.ctx.GetSessionVars().StmtCtx.Tables = nil
		p := TryFastPlan(s.ctx, stmt)
		if tt.fastPlan {
			c.Assert(p, NotNil)
			c.Assert(len(s.ctx.GetSessionVars().StmtCtx.Tables), Equals, 1)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.Tables[0].Table, Equals, "t")
			c.Assert(s.ctx.GetSessionVars().StmtCtx.Tables[0].DB, Equals, "test")
		} else {
			c.Assert(p, IsNil)
			c.Assert(len(s.ctx.GetSessionVars().StmtCtx.Tables), Equals, 0)
		}
	}
}

func (s *testPlanSuite) TestUpdateEQCond(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		sql  string
		best string
	}{
		{
			sql:  "select t1.a from t t1, t t2 where t1.a = t2.a+1",
			best: "Join{DataScan(t1)->DataScan(t2)->Projection}(Column#1,Column#26)->Projection",
		},
	}
	ctx := context.TODO()
	for i, tt := range tests {
		comment := Commentf("case:%v sql:%s", i, tt.sql)
		stmt, err := s.ParseOneStmt(tt.sql, "", "")
		c.Assert(err, IsNil, comment)
		Preprocess(s.ctx, stmt, s.is)
		builder := NewPlanBuilder(MockContext(), s.is, &BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		c.Assert(err, IsNil)
		p, err = logicalOptimize(ctx, builder.optFlag, p.(LogicalPlan))
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, tt.best, comment)
	}
}
