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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

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
	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	return MockResolveName(node, is, "test", ctx)
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
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = newMockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			allocator: new(idAllocator),
		}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil)
		c.Assert(ToString(p), Equals, ca.first, Commentf("for %s", ca.sql))

		_, p, err = p.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = p.PruneColumnsAndResolveIndices(p.GetSchema())
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
				"TableScan_1": {"a"},
			},
		},
		{
			sql: "select count(*) from t",
			ans: map[string][]string{
				"TableScan_1": {},
			},
		},
		{
			sql: "select count(*) from t a join t b where a.a < 1",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_2": {},
			},
		},
		{
			sql: "select count(*) from t a join t b on a.a = b.d",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_2": {"d"},
			},
		},
		{
			sql: "select count(*) from t a join t b on a.a = b.d order by sum(a.d)",
			ans: map[string][]string{
				"TableScan_1": {"a", "d"},
				"TableScan_2": {"d"},
			},
		},
		{
			sql: "select count(b.a) from t a join t b on a.a = b.d group by b.b order by sum(a.d)",
			ans: map[string][]string{
				"TableScan_1": {"a", "d"},
				"TableScan_2": {"a", "b", "d"},
			},
		},
		{
			sql: "select * from (select count(b.a) from t a join t b on a.a = b.d group by b.b having sum(a.d) < 0) tt",
			ans: map[string][]string{
				"TableScan_1": {"a", "d"},
				"TableScan_2": {"a", "b", "d"},
			},
		},
		{
			sql: "select (select count(a) from t where b = k.a) from t k",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_2": {"a", "b"},
			},
		},
		{
			sql: "select exists (select count(*) from t where b = k.a) from t k",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_2": {"b"},
			},
		},
		{
			sql: "select b = (select count(*) from t where b = k.a) from t k",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_2": {"b"},
			},
		},
		{
			sql: "select exists (select count(a) from t where b = k.a) from t k",
			ans: map[string][]string{
				"TableScan_1": {"a"},
				"TableScan_2": {"b"},
			},
		},
		{
			sql: "select a as c1, b as c2 from t order by 1, c1 + c2 + c",
			ans: map[string][]string{
				"TableScan_1": {"a", "b", "c"},
			},
		},
		{
			sql: "select a from t where b < any (select c from t)",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_2": {"c"},
			},
		},
		{
			sql: "select a from t where (b,a) != all (select c,d from t)",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_2": {"c", "d"},
			},
		},
		{
			sql: "select a from t where (b,a) in (select c,d from t)",
			ans: map[string][]string{
				"TableScan_1": {"a", "b"},
				"TableScan_2": {"c", "d"},
			},
		},
	}
	for _, ca := range cases {
		comment := Commentf("for %s", ca.sql)
		stmt, err := s.ParseOneStmt(ca.sql, "", "")
		c.Assert(err, IsNil, comment)
		ast.SetFlag(stmt)

		err = newMockResolve(stmt)
		c.Assert(err, IsNil)

		builder := &planBuilder{
			colMapper: make(map[*ast.ColumnNameExpr]int),
			allocator: new(idAllocator)}
		p := builder.build(stmt).(LogicalPlan)
		c.Assert(builder.err, IsNil, comment)

		_, p, err = p.PredicatePushDown(nil)
		c.Assert(err, IsNil)
		_, err = p.PruneColumnsAndResolveIndices(p.GetSchema())
		c.Assert(err, IsNil)
		check(p, c, ca.ans, comment)
	}
	UseNewPlanner = false
}

func (s *testPlanSuite) TestAllocID(c *C) {
	pA := &NewTableScan{baseLogicalPlan: newBaseLogicalPlan(Ts, new(idAllocator))}

	pB := &NewTableScan{baseLogicalPlan: newBaseLogicalPlan(Ts, new(idAllocator))}

	pA.initID()
	pB.initID()
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
			exprStr:   "a in (1, 3, NULL, 2)",
			resultStr: "[[<nil> <nil>] [1 1] [2 2] [3 3]]",
		},
		{
			exprStr:   `a IN (8,8,81,45)`,
			resultStr: `[[8 8] [45 45] [81 81]]`,
		},
		{
			exprStr:   "a between 1 and 2",
			resultStr: "[[1 2]]",
		},
		{
			exprStr:   "a not between 1 and 2",
			resultStr: "[[-inf 1) (2 +inf]]",
		},
		{
			exprStr:   "a not between null and 0",
			resultStr: "[(0 +inf]]",
		},
		{
			exprStr:   "a between 2 and 1",
			resultStr: "[]",
		},
		{
			exprStr:   "a not between 2 and 1",
			resultStr: "[[-inf +inf]]",
		},
		{
			exprStr:   "a IS NULL",
			resultStr: "[[<nil> <nil>]]",
		},
		{
			exprStr:   "a IS NOT NULL",
			resultStr: "[[-inf +inf]]",
		},
		{
			exprStr:   "a IS TRUE",
			resultStr: "[[-inf 0) (0 +inf]]",
		},
		{
			exprStr:   "a IS NOT TRUE",
			resultStr: "[[<nil> <nil>] [0 0]]",
		},
		{
			exprStr:   "a IS FALSE",
			resultStr: "[[0 0]]",
		},
		{
			exprStr:   "a IS NOT FALSE",
			resultStr: "[[<nil> 0) (0 +inf]]",
		},
		{
			exprStr:   "a LIKE 'abc%'",
			resultStr: "[[abc abd)]",
		},
		{
			exprStr:   "a LIKE 'abc_'",
			resultStr: "[(abc abd)]",
		},
		{
			exprStr:   "a LIKE 'abc'",
			resultStr: "[[abc abc]]",
		},
		{
			exprStr:   `a LIKE "ab\_c"`,
			resultStr: "[[ab_c ab_c]]",
		},
		{
			exprStr:   "a LIKE '%'",
			resultStr: "[[-inf +inf]]",
		},
		{
			exprStr:   `a LIKE '\%a'`,
			resultStr: `[[%a %a]]`,
		},
		{
			exprStr:   `a LIKE "\\"`,
			resultStr: `[[\ \]]`,
		},
		{
			exprStr:   `a LIKE "\\\\a%"`,
			resultStr: `[[\a \b)]`,
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
		stmts, err := s.Parse(sql, "", "")
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
