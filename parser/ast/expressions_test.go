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
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
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
			{NewValueExpr(0, mysql.DefaultCharset, mysql.DefaultCollationName), 0, 0},
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
		{"not true", "!TRUE"},
		{"~3", "~3"},
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

func (tc *testExpressionsSuite) TestIsTruthRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"a is true", "`a` IS TRUE"},
		{"a is not true", "`a` IS NOT TRUE"},
		{"a is FALSE", "`a` IS FALSE"},
		{"a is not false", "`a` IS NOT FALSE"},
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

func (tc *testExpressionsSuite) TestCaseExpr(c *C) {
	testCases := []NodeRestoreTestCase{
		{"case when 1 then 2 end", "CASE WHEN 1 THEN 2 END"},
		{"case when 1 then 'a' when 2 then 'b' end", "CASE WHEN 1 THEN 'a' WHEN 2 THEN 'b' END"},
		{"case when 1 then 'a' when 2 then 'b' else 'c' end", "CASE WHEN 1 THEN 'a' WHEN 2 THEN 'b' ELSE 'c' END"},
		{"case when 'a'!=1 then true else false end", "CASE WHEN 'a'!=1 THEN TRUE ELSE FALSE END"},
		{"case a when 'a' then true else false end", "CASE `a` WHEN 'a' THEN TRUE ELSE FALSE END"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestBinaryOperationExpr(c *C) {
	testCases := []NodeRestoreTestCase{
		{"'a'!=1", "'a'!=1"},
		{"a!=1", "`a`!=1"},
		{"3<5", "3<5"},
		{"10>5", "10>5"},
		{"3+5", "3+5"},
		{"3-5", "3-5"},
		{"a<>5", "`a`!=5"},
		{"a=1", "`a`=1"},
		{"a mod 2", "`a`%2"},
		{"a div 2", "`a` DIV 2"},
		{"true and true", "TRUE AND TRUE"},
		{"false or false", "FALSE OR FALSE"},
		{"true xor false", "TRUE XOR FALSE"},
		{"3 & 4", "3&4"},
		{"5 | 6", "5|6"},
		{"7 ^ 8", "7^8"},
		{"9 << 10", "9<<10"},
		{"11 >> 12", "11>>12"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestBinaryOperationExprWithFlags(c *C) {
	testCases := []NodeRestoreTestCase{
		{"'a'!=1", "'a' != 1"},
		{"a!=1", "`a` != 1"},
		{"3<5", "3 < 5"},
		{"10>5", "10 > 5"},
		{"3+5", "3 + 5"},
		{"3-5", "3 - 5"},
		{"a<>5", "`a` != 5"},
		{"a=1", "`a` = 1"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	flags := format.DefaultRestoreFlags | format.RestoreSpacesAroundBinaryOperation
	RunNodeRestoreTestWithFlags(c, testCases, "select %s", extractNodeFunc, flags)
}

func (tc *testExpressionsSuite) TestParenthesesExpr(c *C) {
	testCases := []NodeRestoreTestCase{
		{"(1+2)*3", "(1+2)*3"},
		{"1+2*3", "1+2*3"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestWhenClause(c *C) {
	testCases := []NodeRestoreTestCase{
		{"when 1 then 2", "WHEN 1 THEN 2"},
		{"when 1 then 'a'", "WHEN 1 THEN 'a'"},
		{"when 'a'!=1 then true", "WHEN 'a'!=1 THEN TRUE"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr.(*CaseExpr).WhenClauses[0]
	}
	RunNodeRestoreTest(c, testCases, "select case %s end", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestDefaultExpr(c *C) {
	testCases := []NodeRestoreTestCase{
		{"default", "DEFAULT"},
		{"default(i)", "DEFAULT(`i`)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*InsertStmt).Lists[0][0]
	}
	RunNodeRestoreTest(c, testCases, "insert into t values(%s)", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestPatternInExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"'a' in ('b')", "'a' IN ('b')"},
		{"2 in (0,3,7)", "2 IN (0,3,7)"},
		{"2 not in (0,3,7)", "2 NOT IN (0,3,7)"},
		{"2 in (select 2)", "2 IN (SELECT 2)"},
		{"2 not in (select 2)", "2 NOT IN (SELECT 2)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestPatternLikeExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"a like 't1'", "`a` LIKE 't1'"},
		{"a like 't1%'", "`a` LIKE 't1%'"},
		{"a like '%t1%'", "`a` LIKE '%t1%'"},
		{"a like '%t1_|'", "`a` LIKE '%t1_|'"},
		{"a not like 't1'", "`a` NOT LIKE 't1'"},
		{"a not like 't1%'", "`a` NOT LIKE 't1%'"},
		{"a not like '%D%v%'", "`a` NOT LIKE '%D%v%'"},
		{"a not like '%t1_|'", "`a` NOT LIKE '%t1_|'"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestValuesExpr(c *C) {
	testCases := []NodeRestoreTestCase{
		{"values(a)", "VALUES(`a`)"},
		{"values(a)+values(b)", "VALUES(`a`)+VALUES(`b`)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*InsertStmt).OnDuplicate[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "insert into t values (1,2,3) on duplicate key update c=%s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestPatternRegexpExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"a regexp 't1'", "`a` REGEXP 't1'"},
		{"a regexp '^[abc][0-9]{11}|ok$'", "`a` REGEXP '^[abc][0-9]{11}|ok$'"},
		{"a rlike 't1'", "`a` REGEXP 't1'"},
		{"a rlike '^[abc][0-9]{11}|ok$'", "`a` REGEXP '^[abc][0-9]{11}|ok$'"},
		{"a not regexp 't1'", "`a` NOT REGEXP 't1'"},
		{"a not regexp '^[abc][0-9]{11}|ok$'", "`a` NOT REGEXP '^[abc][0-9]{11}|ok$'"},
		{"a not rlike 't1'", "`a` NOT REGEXP 't1'"},
		{"a not rlike '^[abc][0-9]{11}|ok$'", "`a` NOT REGEXP '^[abc][0-9]{11}|ok$'"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestRowExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"(1,2)", "ROW(1,2)"},
		{"(col1,col2)", "ROW(`col1`,`col2`)"},
		{"row(1,2)", "ROW(1,2)"},
		{"row(col1,col2)", "ROW(`col1`,`col2`)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Where.(*BinaryOperationExpr).L
	}
	RunNodeRestoreTest(c, testCases, "select 1 from t1 where %s = row(1,2)", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestMaxValueExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"maxvalue", "MAXVALUE"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*AlterTableStmt).Specs[0].PartDefinitions[0].Clause.(*PartitionDefinitionClauseLessThan).Exprs[0]
	}
	RunNodeRestoreTest(c, testCases, "alter table posts add partition ( partition p1 values less than %s)", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestPositionExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"1", "1"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).OrderBy.Items[0]
	}
	RunNodeRestoreTest(c, testCases, "select * from t order by %s", extractNodeFunc)

}

func (tc *testExpressionsSuite) TestExistsSubqueryExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"EXISTS (SELECT 2)", "EXISTS (SELECT 2)"},
		{"NOT EXISTS (SELECT 2)", "NOT EXISTS (SELECT 2)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Where
	}
	RunNodeRestoreTest(c, testCases, "select 1 from t1 where %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestVariableExpr(c *C) {
	testCases := []NodeRestoreTestCase{
		{"@a>1", "@`a`>1"},
		{"@`aB`+1", "@`aB`+1"},
		{"@'a':=1", "@`a`:=1"},
		{"@`a``b`=4", "@`a``b`=4"},
		{`@"aBC">1`, "@`aBC`>1"},
		{"@`a`+1", "@`a`+1"},
		{"@``", "@``"},
		{"@", "@``"},
		{"@@``", "@@``"},
		{"@@", "@@``"},
		{"@@var", "@@`var`"},
		{"@@global.b='foo'", "@@GLOBAL.`b`='foo'"},
		{"@@session.'C'", "@@SESSION.`c`"},
		{`@@local."aBc"`, "@@SESSION.`abc`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Fields.Fields[0].Expr
	}
	RunNodeRestoreTest(c, testCases, "select %s", extractNodeFunc)
}

func (tc *testExpressionsSuite) TestMatchAgainstExpr(c *C) {
	testCases := []NodeRestoreTestCase{
		{`MATCH(content, title) AGAINST ('search for')`, "MATCH (`content`,`title`) AGAINST ('search for')"},
		{`MATCH(content) AGAINST ('search for' IN BOOLEAN MODE)`, "MATCH (`content`) AGAINST ('search for' IN BOOLEAN MODE)"},
		{`MATCH(content, title) AGAINST ('search for' WITH QUERY EXPANSION)`, "MATCH (`content`,`title`) AGAINST ('search for' WITH QUERY EXPANSION)"},
		{`MATCH(content) AGAINST ('search for' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)`, "MATCH (`content`) AGAINST ('search for' WITH QUERY EXPANSION)"},
		{`MATCH(content) AGAINST ('search') AND id = 1`, "MATCH (`content`) AGAINST ('search') AND `id`=1"},
		{`MATCH(content) AGAINST ('search') OR id = 1`, "MATCH (`content`) AGAINST ('search') OR `id`=1"},
		{`MATCH(content) AGAINST (X'40404040' | X'01020304') OR id = 1`, "MATCH (`content`) AGAINST (x'40404040'|x'01020304') OR `id`=1"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Where
	}
	RunNodeRestoreTest(c, testCases, "SELECT * FROM t WHERE %s", extractNodeFunc)
}
