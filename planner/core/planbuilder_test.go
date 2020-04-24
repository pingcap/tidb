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

package core

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hint"
)

var _ = Suite(&testPlanBuilderSuite{})

func (s *testPlanBuilderSuite) SetUpSuite(c *C) {
}

type testPlanBuilderSuite struct {
}

func (s *testPlanBuilderSuite) TestShow(c *C) {
	node := &ast.ShowStmt{}
	tps := []ast.ShowStmtType{
		ast.ShowEngines,
		ast.ShowDatabases,
		ast.ShowTables,
		ast.ShowTableStatus,
		ast.ShowColumns,
		ast.ShowWarnings,
		ast.ShowCharset,
		ast.ShowVariables,
		ast.ShowStatus,
		ast.ShowCollation,
		ast.ShowCreateTable,
		ast.ShowCreateUser,
		ast.ShowGrants,
		ast.ShowTriggers,
		ast.ShowProcedureStatus,
		ast.ShowIndex,
		ast.ShowProcessList,
		ast.ShowCreateDatabase,
		ast.ShowEvents,
		ast.ShowMasterStatus,
	}
	for _, tp := range tps {
		node.Tp = tp
		schema, _ := buildShowSchema(node, false, false)
		for _, col := range schema.Columns {
			c.Assert(col.RetType.Flen, Greater, 0)
		}
	}
}

func (s *testPlanBuilderSuite) TestGetPathByIndexName(c *C) {
	tblInfo := &model.TableInfo{
		Indices:    make([]*model.IndexInfo, 0),
		PKIsHandle: true,
	}

	accessPath := []*util.AccessPath{
		{IsTablePath: true},
		{Index: &model.IndexInfo{Name: model.NewCIStr("idx")}},
	}

	path := getPathByIndexName(accessPath, model.NewCIStr("idx"), tblInfo)
	c.Assert(path, NotNil)
	c.Assert(path, Equals, accessPath[1])

	path = getPathByIndexName(accessPath, model.NewCIStr("primary"), tblInfo)
	c.Assert(path, NotNil)
	c.Assert(path, Equals, accessPath[0])

	path = getPathByIndexName(accessPath, model.NewCIStr("not exists"), tblInfo)
	c.Assert(path, IsNil)

	tblInfo = &model.TableInfo{
		Indices:    make([]*model.IndexInfo, 0),
		PKIsHandle: false,
	}

	path = getPathByIndexName(accessPath, model.NewCIStr("primary"), tblInfo)
	c.Assert(path, IsNil)
}

func (s *testPlanBuilderSuite) TestRewriterPool(c *C) {
	builder := NewPlanBuilder(MockContext(), nil, &hint.BlockHintProcessor{})

	// Make sure PlanBuilder.getExpressionRewriter() provides clean rewriter from pool.
	// First, pick one rewriter from the pool and make it dirty.
	builder.rewriterCounter++
	dirtyRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	dirtyRewriter.asScalar = true
	dirtyRewriter.aggrMap = make(map[*ast.AggregateFuncExpr]int)
	dirtyRewriter.preprocess = func(ast.Node) ast.Node { return nil }
	dirtyRewriter.insertPlan = &Insert{}
	dirtyRewriter.disableFoldCounter = 1
	dirtyRewriter.ctxStack = make([]expression.Expression, 2)
	dirtyRewriter.ctxNameStk = make([]*types.FieldName, 2)
	builder.rewriterCounter--
	// Then, pick again and check if it's cleaned up.
	builder.rewriterCounter++
	cleanRewriter := builder.getExpressionRewriter(context.TODO(), nil)
	c.Assert(cleanRewriter, Equals, dirtyRewriter) // Rewriter should be reused.
	c.Assert(cleanRewriter.asScalar, Equals, false)
	c.Assert(cleanRewriter.aggrMap, IsNil)
	c.Assert(cleanRewriter.preprocess, IsNil)
	c.Assert(cleanRewriter.insertPlan, IsNil)
	c.Assert(cleanRewriter.disableFoldCounter, Equals, 0)
	c.Assert(len(cleanRewriter.ctxStack), Equals, 0)
	builder.rewriterCounter--
}

func (s *testPlanBuilderSuite) TestDisableFold(c *C) {
	// Functions like BENCHMARK() shall not be folded into result 0,
	// but normal outer function with constant args should be folded.
	// Types of expression and first layer of args will be validated.
	cases := []struct {
		SQL      string
		Expected expression.Expression
		Args     []expression.Expression
	}{
		{`select sin(length("abc"))`, &expression.Constant{}, nil},
		{`select benchmark(3, sin(123))`, &expression.ScalarFunction{}, []expression.Expression{
			&expression.Constant{},
			&expression.ScalarFunction{},
		}},
		{`select pow(length("abc"), benchmark(3, sin(123)))`, &expression.ScalarFunction{}, []expression.Expression{
			&expression.Constant{},
			&expression.ScalarFunction{},
		}},
	}

	ctx := MockContext()
	for _, t := range cases {
		st, err := parser.New().ParseOneStmt(t.SQL, "", "")
		c.Assert(err, IsNil)
		stmt := st.(*ast.SelectStmt)
		expr := stmt.Fields.Fields[0].Expr

		builder := NewPlanBuilder(ctx, nil, &hint.BlockHintProcessor{})
		builder.rewriterCounter++
		rewriter := builder.getExpressionRewriter(context.TODO(), nil)
		c.Assert(rewriter, NotNil)
		c.Assert(rewriter.disableFoldCounter, Equals, 0)
		rewritenExpression, _, err := builder.rewriteExprNode(rewriter, expr, true)
		c.Assert(err, IsNil)
		c.Assert(rewriter.disableFoldCounter, Equals, 0) // Make sure the counter is reduced to 0 in the end.
		builder.rewriterCounter--

		c.Assert(rewritenExpression, FitsTypeOf, t.Expected)
		for i, expectedArg := range t.Args {
			rewritenArg := expression.GetFuncArg(rewritenExpression, i)
			c.Assert(rewritenArg, FitsTypeOf, expectedArg)
		}
	}
}
