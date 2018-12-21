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
)

var _ = Suite(&testDMLSuite{})

type testDMLSuite struct {
}

func (ts *testDMLSuite) TestDMLVisitorCover(c *C) {
	ce := &checkExpr{}

	tableRefsClause := &TableRefsClause{TableRefs: &Join{Left: &TableSource{Source: &TableName{}}, On: &OnCondition{Expr: ce}}}

	stmts := []struct {
		node             Node
		expectedEnterCnt int
		expectedLeaveCnt int
	}{
		{&DeleteStmt{TableRefs: tableRefsClause, Tables: &DeleteTableList{}, Where: ce,
			Order: &OrderByClause{}, Limit: &Limit{Count: ce, Offset: ce}}, 4, 4},
		{&ShowStmt{Table: &TableName{}, Column: &ColumnName{}, Pattern: &PatternLikeExpr{Expr: ce, Pattern: ce}, Where: ce}, 3, 3},
		{&LoadDataStmt{Table: &TableName{}, Columns: []*ColumnName{{}}, FieldsInfo: &FieldsClause{}, LinesInfo: &LinesClause{}}, 0, 0},
		{&Assignment{Column: &ColumnName{}, Expr: ce}, 1, 1},
		{&ByItem{Expr: ce}, 1, 1},
		{&GroupByClause{Items: []*ByItem{{Expr: ce}, {Expr: ce}}}, 2, 2},
		{&HavingClause{Expr: ce}, 1, 1},
		{&Join{Left: &TableSource{Source: &TableName{}}}, 0, 0},
		{&Limit{Count: ce, Offset: ce}, 2, 2},
		{&OnCondition{Expr: ce}, 1, 1},
		{&OrderByClause{Items: []*ByItem{{Expr: ce}, {Expr: ce}}}, 2, 2},
		{&SelectField{Expr: ce, WildCard: &WildCardField{}}, 1, 1},
		{&TableName{}, 0, 0},
		{tableRefsClause, 1, 1},
		{&TableSource{Source: &TableName{}}, 0, 0},
		{&WildCardField{}, 0, 0},

		// TODO: cover childrens
		{&InsertStmt{Table: tableRefsClause}, 1, 1},
		{&UnionStmt{}, 0, 0},
		{&UpdateStmt{TableRefs: tableRefsClause}, 1, 1},
		{&SelectStmt{}, 0, 0},
		{&FieldList{}, 0, 0},
		{&UnionSelectList{}, 0, 0},
		{&WindowSpec{}, 0, 0},
		{&PartitionByClause{}, 0, 0},
		{&FrameClause{}, 0, 0},
		{&FrameBound{}, 0, 0},
	}

	for _, v := range stmts {
		ce.reset()
		v.node.Accept(checkVisitor{})
		c.Check(ce.enterCnt, Equals, v.expectedEnterCnt)
		c.Check(ce.leaveCnt, Equals, v.expectedLeaveCnt)
		v.node.Accept(visitor1{})
	}
}

func (tc *testDMLSuite) TestTableNameRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"dbb.`tbb1`", "`dbb`.`tbb1`"},
		{"`tbb2`", "`tbb2`"},
		{"tbb3", "`tbb3`"},
		{"dbb.`hello-world`", "`dbb`.`hello-world`"},
		{"`dbb`.`hello-world`", "`dbb`.`hello-world`"},
		{"`dbb.HelloWorld`", "`dbb.HelloWorld`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateTableStmt).Table
	}
	RunNodeRestoreTest(c, testCases, "CREATE TABLE %s (id VARCHAR(128) NOT NULL);", extractNodeFunc)
}

func (tc *testDMLSuite) TestTableNameIndexHintsRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"t use index (hello)", "`t` USE INDEX (`hello`)"},
		{"t use index (hello, world)", "`t` USE INDEX (`hello`, `world`)"},
		{"t use index ()", "`t` USE INDEX ()"},
		{"t use key ()", "`t` USE INDEX ()"},
		{"t ignore key ()", "`t` IGNORE INDEX ()"},
		{"t force key ()", "`t` FORCE INDEX ()"},
		{"t use index for order by (idx1)", "`t` USE INDEX FOR ORDER BY (`idx1`)"},

		{"t use index (hello, world, yes) force key (good)", "`t` USE INDEX (`hello`, `world`, `yes`) FORCE INDEX (`good`)"},
		{"t use index (hello, world, yes) use index for order by (good)", "`t` USE INDEX (`hello`, `world`, `yes`) USE INDEX FOR ORDER BY (`good`)"},
		{"t ignore key (hello, world, yes) force key (good)", "`t` IGNORE INDEX (`hello`, `world`, `yes`) FORCE INDEX (`good`)"},

		{"t use index for group by (idx1) use index for order by (idx2)", "`t` USE INDEX FOR GROUP BY (`idx1`) USE INDEX FOR ORDER BY (`idx2`)"},
		{"t use index for group by (idx1) ignore key for order by (idx2)", "`t` USE INDEX FOR GROUP BY (`idx1`) IGNORE INDEX FOR ORDER BY (`idx2`)"},
		{"t use index for group by (idx1) ignore key for group by (idx2)", "`t` USE INDEX FOR GROUP BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`)"},
		{"t use index for order by (idx1) ignore key for group by (idx2)", "`t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`)"},

		{"t use index for order by (idx1) ignore key for group by (idx2) use index (idx3)", "`t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`) USE INDEX (`idx3`)"},
		{"t use index for order by (idx1) ignore key for group by (idx2) use index (idx3)", "`t` USE INDEX FOR ORDER BY (`idx1`) IGNORE INDEX FOR GROUP BY (`idx2`) USE INDEX (`idx3`)"},

		{"t use index (`foo``bar`) force index (`baz``1`, `xyz`)", "`t` USE INDEX (`foo``bar`) FORCE INDEX (`baz``1`, `xyz`)"},
		{"t force index (`foo``bar`) ignore index (`baz``1`, xyz)", "`t` FORCE INDEX (`foo``bar`) IGNORE INDEX (`baz``1`, `xyz`)"},
		{"t ignore index (`foo``bar`) force key (`baz``1`, xyz)", "`t` IGNORE INDEX (`foo``bar`) FORCE INDEX (`baz``1`, `xyz`)"},
		{"t ignore index (`foo``bar`) ignore key for group by (`baz``1`, xyz)", "`t` IGNORE INDEX (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"},
		{"t ignore index (`foo``bar`) ignore key for order by (`baz``1`, xyz)", "`t` IGNORE INDEX (`foo``bar`) IGNORE INDEX FOR ORDER BY (`baz``1`, `xyz`)"},

		{"t use index for group by (`foo``bar`) use index for order by (`baz``1`, `xyz`)", "`t` USE INDEX FOR GROUP BY (`foo``bar`) USE INDEX FOR ORDER BY (`baz``1`, `xyz`)"},
		{"t use index for group by (`foo``bar`) ignore key for order by (`baz``1`, `xyz`)", "`t` USE INDEX FOR GROUP BY (`foo``bar`) IGNORE INDEX FOR ORDER BY (`baz``1`, `xyz`)"},
		{"t use index for group by (`foo``bar`) ignore key for group by (`baz``1`, `xyz`)", "`t` USE INDEX FOR GROUP BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"},
		{"t use index for order by (`foo``bar`) ignore key for group by (`baz``1`, `xyz`)", "`t` USE INDEX FOR ORDER BY (`foo``bar`) IGNORE INDEX FOR GROUP BY (`baz``1`, `xyz`)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).From.TableRefs.Left.(*TableSource).Source.(*TableName)
	}
	RunNodeRestoreTest(c, testCases, "SELECT * FROM %s", extractNodeFunc)
}

func (tc *testDMLSuite) TestLimitRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"limit 10", "LIMIT 10"},
		{"limit 10,20", "LIMIT 10,20"},
		{"limit 20 offset 10", "LIMIT 10,20"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*SelectStmt).Limit
	}
	RunNodeRestoreTest(c, testCases, "SELECT 1 %s", extractNodeFunc)
}

