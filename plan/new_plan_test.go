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
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testPlanSuite{})

func newMockResolve(node ast.Node) error {
	indices := []*model.IndexInfo{
		{
			Name: model.NewCIStr("b"),
			Columns: []*model.IndexColumn{
				{
					Name: model.NewCIStr("b"),
				},
			},
		},
		{
			Name: model.NewCIStr("c_d_e"),
			Columns: []*model.IndexColumn{
				{
					Name: model.NewCIStr("c"),
				},
				{
					Name: model.NewCIStr("d"),
				},
				{
					Name: model.NewCIStr("e"),
				},
			},
		},
	}
	pkColumn := &model.ColumnInfo{
		State: model.StatePublic,
		Name:  model.NewCIStr("a"),
	}
	col0 := &model.ColumnInfo{
		State: model.StatePublic,
		Name:  model.NewCIStr("b"),
	}
	col1 := &model.ColumnInfo{
		State: model.StatePublic,
		Name:  model.NewCIStr("c"),
	}
	col2 := &model.ColumnInfo{
		State: model.StatePublic,
		Name:  model.NewCIStr("d"),
	}
	pkColumn.Flag = mysql.PriKeyFlag
	table := &model.TableInfo{
		Columns:    []*model.ColumnInfo{pkColumn, col0, col1, col2},
		Indices:    indices,
		Name:       model.NewCIStr("t"),
		PKIsHandle: true,
	}
	is := infoschema.MockInfoSchema([]*model.TableInfo{table})
	return MockResolveName(node, is, "test")
}

func (s *testPlanSuite) TestPredicatePushDown(c *C) {
	UseNewPlanner = true
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql   string
		first string
		best  string
	}{
		{
			sql:   "select a from (select a from t where d = 0) k where k.a = 5",
			first: "DataScan(t)->Selection->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Projection->Projection",
		},
		{
			sql:   "select a from (select 1+2 as a from t where d = 0) k where k.a = 5",
			first: "DataScan(t)->Selection->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Projection->Selection->Projection",
		},
		{
			sql:   "select a from (select d as a from t where d = 0) k where k.a = 5",
			first: "DataScan(t)->Selection->Projection->Selection->Projection",
			best:  "DataScan(t)->Selection->Projection->Projection",
		},
		{
			sql:   "select * from t ta join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select * from t ta join t tb on ta.d = tb.d where ta.d > 1 and tb.a = 0",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select * from t ta left outer join t tb on ta.d = tb.d and ta.d > 1 where tb.a = 0",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
		},
		{
			sql:   "select * from t ta right outer join t tb on ta.d = tb.d and ta.a > 1 where tb.a = 0",
			first: "Join{DataScan(t)->DataScan(t)}->Selection->Projection",
			best:  "Join{DataScan(t)->Selection->DataScan(t)->Selection}->Projection",
		},
		{
			sql:   "select a, d from (select * from t union all select * from t union all select * from t) z where a < 10",
			first: "UnionAll{DataScan(t)->Projection->DataScan(t)->Projection->DataScan(t)->Projection}->Selection->Projection",
			best:  "UnionAll{DataScan(t)->Selection->Projection->DataScan(t)->Selection->Projection->DataScan(t)->Selection->Projection}->Projection",
		},
		{
			sql:   "select (select count(*) from t where t.a = k.a) from t k",
			first: "DataScan(t)->Apply(DataScan(t)->Selection->Aggr->Projection->MaxOneRow)->Projection",
			best:  "DataScan(t)->Apply(DataScan(t)->Selection->Aggr->Projection->MaxOneRow)->Projection",
		},
		{
			sql:   "select a from t where exists(select 1 from t as x where x.a < t.a)",
			first: "DataScan(t)->Apply(DataScan(t)->Selection->Projection->Exists)->Selection->Projection",
			best:  "DataScan(t)->Apply(DataScan(t)->Selection->Projection->Exists)->Selection->Projection",
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := parser.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = newMockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)
		c.Assert(ToString(p), Equals, ca.first, Commentf("for %s", ca.sql))

		_, err = builder.predicatePushDown(p, []expression.Expression{})
		c.Assert(err, IsNil)
		_, err = pruneColumnsAndResolveIndices(p, p.GetSchema())
		c.Assert(err, IsNil)
		c.Assert(ToString(p), Equals, ca.best, Commentf("for %s", ca.sql))
	}
	UseNewPlanner = false
}

func (s *testPlanSuite) TestColumnPruning(c *C) {
	UseNewPlanner = true
	defer testleak.AfterTest(c)()
	cases := []struct {
		sql string
		ans map[string][]string
	}{
		{
			sql: "select count(*) from t group by a",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a"},
			},
		},
		{
			sql: "select count(*) from t",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {},
			},
		},
		{
			sql: "select count(*) from t a join t b where a.a < 1",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a"},
				"*plan.NewTableScan_2": {},
			},
		},
		{
			sql: "select count(*) from t a join t b on a.a = b.d",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a"},
				"*plan.NewTableScan_2": {"d"},
			},
		},
		{
			sql: "select count(*) from t a join t b on a.a = b.d order by sum(a.d)",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a", "d"},
				"*plan.NewTableScan_2": {"d"},
			},
		},
		{
			sql: "select count(b.a) from t a join t b on a.a = b.d group by b.b order by sum(a.d)",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a", "d"},
				"*plan.NewTableScan_2": {"a", "b", "d"},
			},
		},
		{
			sql: "select * from (select count(b.a) from t a join t b on a.a = b.d group by b.b having sum(a.d) < 0) tt",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a", "d"},
				"*plan.NewTableScan_2": {"a", "b", "d"},
			},
		},
		{
			sql: "select (select count(a) from t where b = k.a) from t k",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a"},
				"*plan.NewTableScan_2": {"a", "b"},
			},
		},
		{
			sql: "select exists (select count(*) from t where b = k.a) from t k",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a"},
				"*plan.NewTableScan_2": {"b"},
			},
		},
		{
			sql: "select b = (select count(*) from t where b = k.a) from t k",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a", "b"},
				"*plan.NewTableScan_2": {"b"},
			},
		},
		{
			sql: "select exists (select count(a) from t where b = k.a) from t k",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a"},
				"*plan.NewTableScan_2": {"b"},
			},
		},
		{
			sql: "select a as c1, b as c2 from t order by 1, c1 + c2 + c",
			ans: map[string][]string{
				"*plan.NewTableScan_1": {"a", "b", "c"},
			},
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := parser.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = newMockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{}
		p := builder.build(stmt)
		c.Assert(builder.err, IsNil)

		_, err = builder.predicatePushDown(p, []expression.Expression{})
		c.Assert(err, IsNil)
		_, err = pruneColumnsAndResolveIndices(p, p.GetSchema())
		c.Assert(err, IsNil)
		check(p, c, ca.ans, comment)
	}
	UseNewPlanner = false
}

func (s *testPlanSuite) TestAllocID(c *C) {
	bA := &planBuilder{}
	pA := &NewTableScan{}

	bB := &planBuilder{}
	pB := &NewTableScan{}

	pA.id = bA.allocID(pA)
	pB.id = bB.allocID(pB)
	c.Assert(pA.id, Equals, pB.id)
}

func (s *testPlanSuite) TestNewRangeBuilder(c *C) {
	UseNewPlanner = true
	defer testleak.AfterTest(c)()
	rb := &rangeBuilder{}

	cases := []struct {
		exprStr   string
		resultStr string
	}{
		{
			exprStr:   "a = 1",
			resultStr: "[[1 1]]",
		},
		{
			exprStr:   "1 = a",
			resultStr: "[[1 1]]",
		},
		{
			exprStr:   "a != 1",
			resultStr: "[[-inf 1) (1 +inf]]",
		},
		{
			exprStr:   "1 != a",
			resultStr: "[[-inf 1) (1 +inf]]",
		},
		{
			exprStr:   "a > 1",
			resultStr: "[(1 +inf]]",
		},
		{
			exprStr:   "1 < a",
			resultStr: "[(1 +inf]]",
		},
		{
			exprStr:   "a >= 1",
			resultStr: "[[1 +inf]]",
		},
		{
			exprStr:   "1 <= a",
			resultStr: "[[1 +inf]]",
		},
		{
			exprStr:   "a < 1",
			resultStr: "[[-inf 1)]",
		},
		{
			exprStr:   "1 > a",
			resultStr: "[[-inf 1)]",
		},
		{
			exprStr:   "a <= 1",
			resultStr: "[[-inf 1]]",
		},
		{
			exprStr:   "1 >= a",
			resultStr: "[[-inf 1]]",
		},
		{
			exprStr:   "(a)",
			resultStr: "[[-inf 0) (0 +inf]]",
		},
		{
			exprStr:   `0.4`,
			resultStr: `[]`,
		},
		{
			exprStr:   `a > NULL`,
			resultStr: `[]`,
		},
	}

	for _, ca := range cases {
		sql := "select 1 from t where " + ca.exprStr
		stmts, err := parser.Parse(sql, "", "")
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, ca.exprStr))
		stmt := stmts[0].(*ast.SelectStmt)

		err = newMockResolve(stmt)
		c.Assert(err, IsNil)

		p, err := BuildPlan(stmt, nil)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, ca.exprStr))
		var selection *Selection
		for _, child := range p.GetChildren() {
			plan, ok := child.(*Selection)
			if ok {
				selection = plan
				break
			}
		}
		c.Assert(selection, NotNil, Commentf("expr:%v", ca.exprStr))
		c.Assert(selection.Conditions, HasLen, 1, Commentf("conditions:%v, expr:%v",
			selection.Conditions, ca.exprStr))
		result := rb.newBuild(selection.Conditions[0])
		c.Assert(rb.err, IsNil)
		got := fmt.Sprintf("%v", result)
		c.Assert(got, Equals, ca.resultStr, Commentf("differen for expr %s", ca.exprStr))
	}
	UseNewPlanner = false
}

func check(p Plan, c *C, ans map[string][]string, comment CommentInterface) {
	switch p.(type) {
	case *NewTableScan:
		colList, ok := ans[p.GetID()]
		c.Assert(ok, IsTrue, comment)
		for i, colName := range colList {
			c.Assert(colName, Equals, p.GetSchema()[i].ColName.L, comment)
		}
	}
	for _, child := range p.GetChildren() {
		check(child, c, ans, comment)
	}
}
