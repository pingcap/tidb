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

var _ = Suite(&testDDLSuite{})

type testDDLSuite struct {
}

func (ts *testDDLSuite) TestDDLVisitorCover(c *C) {
	ce := &checkExpr{}
	constraint := &Constraint{Keys: []*IndexColName{{Column: &ColumnName{}}, {Column: &ColumnName{}}}, Refer: &ReferenceDef{}, Option: &IndexOption{}}

	alterTableSpec := &AlterTableSpec{Constraint: constraint, Options: []*TableOption{{}}, NewTable: &TableName{}, NewColumns: []*ColumnDef{{Name: &ColumnName{}}}, OldColumnName: &ColumnName{}, Position: &ColumnPosition{RelativeColumn: &ColumnName{}}}

	stmts := []struct {
		node             Node
		expectedEnterCnt int
		expectedLeaveCnt int
	}{
		{&CreateDatabaseStmt{}, 0, 0},
		{&DropDatabaseStmt{}, 0, 0},
		{&DropIndexStmt{Table: &TableName{}}, 0, 0},
		{&DropTableStmt{Tables: []*TableName{{}, {}}}, 0, 0},
		{&RenameTableStmt{OldTable: &TableName{}, NewTable: &TableName{}}, 0, 0},
		{&TruncateTableStmt{Table: &TableName{}}, 0, 0},

		// TODO: cover children
		{&AlterTableStmt{Table: &TableName{}, Specs: []*AlterTableSpec{alterTableSpec}}, 0, 0},
		{&CreateIndexStmt{Table: &TableName{}}, 0, 0},
		{&CreateTableStmt{Table: &TableName{}, ReferTable: &TableName{}}, 0, 0},
		{&AlterTableSpec{}, 0, 0},
		{&ColumnDef{Name: &ColumnName{}, Options: []*ColumnOption{{Expr: ce}}}, 1, 1},
		{&ColumnOption{Expr: ce}, 1, 1},
		{&ColumnPosition{RelativeColumn: &ColumnName{}}, 0, 0},
		{&Constraint{Keys: []*IndexColName{{Column: &ColumnName{}}, {Column: &ColumnName{}}}, Refer: &ReferenceDef{}, Option: &IndexOption{}}, 0, 0},
		{&IndexColName{Column: &ColumnName{}}, 0, 0},
		{&ReferenceDef{Table: &TableName{}, IndexColNames: []*IndexColName{{Column: &ColumnName{}}, {Column: &ColumnName{}}}, OnDelete: &OnDeleteOpt{}, OnUpdate: &OnUpdateOpt{}}, 0, 0},
	}

	for _, v := range stmts {
		ce.reset()
		v.node.Accept(checkVisitor{})
		c.Check(ce.enterCnt, Equals, v.expectedEnterCnt)
		c.Check(ce.leaveCnt, Equals, v.expectedLeaveCnt)
		v.node.Accept(visitor1{})
	}
}

func (ts *testDDLSuite) TestDDLIndexColNameRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"world", "`world`"},
		{"world(2)", "`world`(2)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateIndexStmt).IndexColNames[0]
	}
	RunNodeRestoreTest(c, testCases, "CREATE INDEX idx ON t (%s) USING HASH", extractNodeFunc)
}

func (ts *testDDLSuite) TestDDLOnDeleteRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"on delete restrict", "ON DELETE RESTRICT"},
		{"on delete CASCADE", "ON DELETE CASCADE"},
		{"on delete SET NULL", "ON DELETE SET NULL"},
		{"on delete no action", "ON DELETE NO ACTION"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateTableStmt).Constraints[1].Refer.OnDelete
	}
	RunNodeRestoreTest(c, testCases, "CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) %s)", extractNodeFunc)
}

func (ts *testDDLSuite) TestDDLOnUpdateRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"ON UPDATE RESTRICT", "ON UPDATE RESTRICT"},
		{"on update CASCADE", "ON UPDATE CASCADE"},
		{"on update SET NULL", "ON UPDATE SET NULL"},
		{"on update no action", "ON UPDATE NO ACTION"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateTableStmt).Constraints[1].Refer.OnUpdate
	}
	RunNodeRestoreTest(c, testCases, "CREATE TABLE child ( id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE %s )", extractNodeFunc)
}

func (ts *testDDLSuite) TestDDLIndexOption(c *C) {
	testCases := []NodeRestoreTestCase{
		{"key_block_size=16", "KEY_BLOCK_SIZE=16"},
		{"USING HASH", "USING HASH"},
		{"comment 'hello'", "COMMENT 'hello'"},
		{"key_block_size=16 USING HASH", "KEY_BLOCK_SIZE=16 USING HASH"},
		{"USING HASH KEY_BLOCK_SIZE=16", "KEY_BLOCK_SIZE=16 USING HASH"},
		{"USING HASH COMMENT 'foo'", "USING HASH COMMENT 'foo'"},
		{"COMMENT 'foo'", "COMMENT 'foo'"},
		{"key_block_size = 32 using hash comment 'hello'", "KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'"},
		{"key_block_size=32 using btree comment 'hello'", "KEY_BLOCK_SIZE=32 USING BTREE COMMENT 'hello'"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateIndexStmt).IndexOption
	}
	RunNodeRestoreTest(c, testCases, "CREATE INDEX idx ON t (a) %s", extractNodeFunc)
}

func (ts *testDDLSuite) TestTableToTableRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"t1 to t2", "`t1` TO `t2`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*RenameTableStmt).TableToTables[0]
	}
	RunNodeRestoreTest(c, testCases, "rename table %s", extractNodeFunc)
}

func (ts *testDDLSuite) TestDDLReferenceDefRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"},
		{"REFERENCES parent(id) ON DELETE CASCADE", "REFERENCES `parent`(`id`) ON DELETE CASCADE"},
		{"REFERENCES parent(id,hello) ON DELETE CASCADE", "REFERENCES `parent`(`id`, `hello`) ON DELETE CASCADE"},
		{"REFERENCES parent(id,hello(12)) ON DELETE CASCADE", "REFERENCES `parent`(`id`, `hello`(12)) ON DELETE CASCADE"},
		{"REFERENCES parent(id(8),hello(12)) ON DELETE CASCADE", "REFERENCES `parent`(`id`(8), `hello`(12)) ON DELETE CASCADE"},
		{"REFERENCES parent(id)", "REFERENCES `parent`(`id`)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateTableStmt).Constraints[1].Refer
	}
	RunNodeRestoreTest(c, testCases, "CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) %s)", extractNodeFunc)
}

func (ts *testDDLSuite) TestDDLTruncateTableStmtRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"truncate t1", "TRUNCATE TABLE `t1`"},
		{"truncate table t1", "TRUNCATE TABLE `t1`"},
		{"truncate a.t1", "TRUNCATE TABLE `a`.`t1`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*TruncateTableStmt)
	}
	RunNodeRestoreTest(c, testCases, "%s", extractNodeFunc)
}
