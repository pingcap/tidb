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

package ast_test

import (
	. "github.com/pingcap/check"
	. "github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

var _ = Suite(&testExpressionsSuite{})

type testExpressionsSuite struct {
}

type checkVisitor struct{}

func (v checkVisitor) Enter(in Node) (Node, bool) {
	if e, ok := in.(*checkExpr); ok {
		e.enterCnt++
		return in, true
	}
	return in, false
}

func (v checkVisitor) Leave(in Node) (Node, bool) {
	if e, ok := in.(*checkExpr); ok {
		e.leaveCnt++
	}
	return in, true
}

type checkExpr struct {
	ValueExpr

	enterCnt int
	leaveCnt int
}

func (n *checkExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*checkExpr)
	return v.Leave(n)
}

func (n *checkExpr) reset() {
	n.enterCnt = 0
	n.leaveCnt = 0
}

func (tc *testExpressionsSuite) TestExpresionsVisitorCover(c *C) {
	ce := &checkExpr{}
	stmts :=
		[]struct {
			node             Node
			expectedEnterCnt int
			expectedLeaveCnt int
		}{
			{&BetweenExpr{Expr: ce, Left: ce, Right: ce}, 3, 3},
			{&BinaryOperationExpr{L: ce, R: ce}, 2, 2},
			{&CaseExpr{Value: ce, WhenClauses: []*WhenClause{{Expr: ce, Result: ce},
				{Expr: ce, Result: ce}}, ElseClause: ce}, 6, 6},
			{&ColumnNameExpr{Name: &ColumnName{}}, 0, 0},
			{&CompareSubqueryExpr{L: ce, R: ce}, 2, 2},
			{&DefaultExpr{Name: &ColumnName{}}, 0, 0},
			{&ExistsSubqueryExpr{Sel: ce}, 1, 1},
			{&IsNullExpr{Expr: ce}, 1, 1},
			{&IsTruthExpr{Expr: ce}, 1, 1},
			{NewParamMarkerExpr(0), 0, 0},
			{&ParenthesesExpr{Expr: ce}, 1, 1},
			{&PatternInExpr{Expr: ce, List: []ExprNode{ce, ce, ce}, Sel: ce}, 5, 5},
			{&PatternLikeExpr{Expr: ce, Pattern: ce}, 2, 2},
			{&PatternRegexpExpr{Expr: ce, Pattern: ce}, 2, 2},
			{&PositionExpr{}, 0, 0},
			{&RowExpr{Values: []ExprNode{ce, ce}}, 2, 2},
			{&UnaryOperationExpr{V: ce}, 1, 1},
			{NewValueExpr(0), 0, 0},
			{&ValuesExpr{Column: &ColumnNameExpr{Name: &ColumnName{}}}, 0, 0},
			{&VariableExpr{Value: ce}, 1, 1},
		}

	for _, v := range stmts {
		ce.reset()
		v.node.Accept(checkVisitor{})
		c.Check(ce.enterCnt, Equals, v.expectedEnterCnt)
		c.Check(ce.leaveCnt, Equals, v.expectedLeaveCnt)
		v.node.Accept(visitor1{})
	}
}

func (tc *testExpressionsSuite) TestUnaryOperationExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"++1", "++1"},
		{"--1", "--1"},
		{"-+1", "-+1"},
		{"-1", "-1"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestColumnNameExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"abc", "`abc`"},
		{"`abc`", "`abc`"},
		{"`ab``c`", "`ab``c`"},
		{"sabc.tABC", "`sabc`.`tABC`"},
		{"dabc.sabc.tabc", "`dabc`.`sabc`.`tabc`"},
		{"dabc.`sabc`.tabc", "`dabc`.`sabc`.`tabc`"},
		{"`dABC`.`sabc`.tabc", "`dABC`.`sabc`.`tabc`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestIsNullExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"a is null", "`a` IS NULL"},
		{"a is not null", "`a` IS NOT NULL"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestBetweenExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"b between 1 and 2", "`b` BETWEEN 1 AND 2"},
		{"b not between 1 and 2", "`b` NOT BETWEEN 1 AND 2"},
		{"b between a and b", "`b` BETWEEN `a` AND `b`"},
		{"b between '' and 'b'", "`b` BETWEEN '' AND 'b'"},
		{"b between '2018-11-01' and '2018-11-02'", "`b` BETWEEN '2018-11-01' AND '2018-11-02'"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}
