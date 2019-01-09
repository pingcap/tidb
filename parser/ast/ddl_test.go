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

func (ts *testDDLSuite) TestDDLConstraintRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"INDEX par_ind (parent_id)", "INDEX `par_ind`(`parent_id`)"},
		{"INDEX par_ind (parent_id(6))", "INDEX `par_ind`(`parent_id`(6))"},
		{"key par_ind (parent_id)", "INDEX `par_ind`(`parent_id`)"},
		{"unique par_ind (parent_id)", "UNIQUE `par_ind`(`parent_id`)"},
		{"unique key par_ind (parent_id)", "UNIQUE `par_ind`(`parent_id`)"},
		{"unique index par_ind (parent_id)", "UNIQUE `par_ind`(`parent_id`)"},
		{"fulltext key full_id (parent_id)", "FULLTEXT `full_id`(`parent_id`)"},
		{"fulltext INDEX full_id (parent_id)", "FULLTEXT `full_id`(`parent_id`)"},
		{"PRIMARY KEY (id)", "PRIMARY KEY(`id`)"},
		{"PRIMARY KEY (id) key_block_size = 32 using hash comment 'hello'", "PRIMARY KEY(`id`) KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'"},
		{"FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "FOREIGN KEY(`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"},
		{"FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "FOREIGN KEY(`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateTableStmt).Constraints[0]
	}
	RunNodeRestoreTest(c, testCases, "CREATE TABLE child (id INT, parent_id INT, %s)", extractNodeFunc)
}

func (ts *testDDLSuite) TestDDLColumnOptionRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"primary key", "PRIMARY KEY"},
		{"not null", "NOT NULL"},
		{"null", "NULL"},
		{"auto_increment", "AUTO_INCREMENT"},
		{"DEFAULT 10", "DEFAULT 10"},
		{"DEFAULT '10'", "DEFAULT '10'"},
		{"DEFAULT 'hello'", "DEFAULT 'hello'"},
		{"DEFAULT 1.1", "DEFAULT 1.1"},
		{"DEFAULT NULL", "DEFAULT NULL"},
		{"DEFAULT ''", "DEFAULT ''"},
		{"DEFAULT TRUE", "DEFAULT TRUE"},
		{"DEFAULT FALSE", "DEFAULT FALSE"},
		{"UNIQUE KEY", "UNIQUE KEY"},
		{"on update CURRENT_TIMESTAMP", "ON UPDATE CURRENT_TIMESTAMP()"},
		{"comment 'hello'", "COMMENT 'hello'"},
		{"generated always as(id + 1)", "GENERATED ALWAYS AS(`id`+1)"},
		{"REFERENCES parent(id)", "REFERENCES `parent`(`id`)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateTableStmt).Cols[0].Options[0]
	}
	RunNodeRestoreTest(c, testCases, "CREATE TABLE child (id INT %s)", extractNodeFunc)
}

func (ts *testDDLSuite) TestDDLColumnDefRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		// for type
		{"id json", "`id` JSON"},
		{"id time(5)", "`id` TIME(5)"},
		{"id int(5) unsigned", "`id` INT(5) UNSIGNED"},
		{"id int(5) UNSIGNED ZEROFILL", "`id` INT(5) UNSIGNED ZEROFILL"},
		{"id float(12,3)", "`id` FLOAT(12,3)"},
		{"id float", "`id` FLOAT"},
		{"id double(22,3)", "`id` DOUBLE(22,3)"},
		{"id double", "`id` DOUBLE"},
		{"id tinyint(4)", "`id` TINYINT(4)"},
		{"id smallint(6)", "`id` SMALLINT(6)"},
		{"id mediumint(9)", "`id` MEDIUMINT(9)"},
		{"id integer(11)", "`id` INT(11)"},
		{"id bigint(20)", "`id` BIGINT(20)"},
		{"id DATE", "`id` DATE"},
		{"id DATETIME", "`id` DATETIME"},
		{"id DECIMAL(4,2)", "`id` DECIMAL(4,2)"},
		{"id char(1)", "`id` CHAR(1)"},
		{"id varchar(10) BINARY", "`id` VARCHAR(10) BINARY"},
		{"id binary(1)", "`id` BINARY(1)"},
		{"id timestamp(2)", "`id` TIMESTAMP(2)"},
		{"id timestamp", "`id` TIMESTAMP"},
		{"id datetime(2)", "`id` DATETIME(2)"},
		{"id date", "`id` DATE"},
		{"id year", "`id` YEAR"},
		{"id INT", "`id` INT"},
		{"id INT NULL", "`id` INT NULL"},
		{"id enum('a','b')", "`id` ENUM('a','b')"},
		{"id enum('''a''','''b''')", "`id` ENUM('''a''','''b''')"},
		{"id enum('a\\nb','a\\tb','a\\rb')", "`id` ENUM('a\nb','a\tb','a\rb')"},
		{"id set('a','b')", "`id` SET('a','b')"},
		{"id set('''a''','''b''')", "`id` SET('''a''','''b''')"},
		{"id set('a\\nb','a''	\\r\\nb','a\\rb')", "`id` SET('a\nb','a''	\r\nb','a\rb')"},
		{`id set("a'\nb","a'b\tc")`, "`id` SET('a''\nb','a''b\tc')"},
		{"id TEXT CHARACTER SET UTF8 COLLATE UTF8_UNICODE_G", "`id` TEXT CHARACTER SET UTF8 COLLATE UTF8_UNICODE_G"},
		{"id text character set UTF8", "`id` TEXT CHARACTER SET UTF8"},
		{"id text charset UTF8", "`id` TEXT CHARACTER SET UTF8"},
		{"id varchar(50) collate UTF8MB4_CZECH_CI", "`id` VARCHAR(50) COLLATE UTF8MB4_CZECH_CI"},
		{"id varchar(50) collate utf8", "`id` VARCHAR(50) COLLATE utf8"},
		{"c1 char(10) character set LATIN1 collate latin1_german1_ci", "`c1` CHAR(10) CHARACTER SET LATIN1 COLLATE latin1_german1_ci"},

		{"id int(11) PRIMARY KEY", "`id` INT(11) PRIMARY KEY"},
		{"id int(11) NOT NULL", "`id` INT(11) NOT NULL"},
		{"id INT(11) NULL", "`id` INT(11) NULL"},
		{"id INT(11) auto_increment", "`id` INT(11) AUTO_INCREMENT"},
		{"id INT(11) DEFAULT 10", "`id` INT(11) DEFAULT 10"},
		{"id INT(11) DEFAULT '10'", "`id` INT(11) DEFAULT '10'"},
		{"id INT(11) DEFAULT 1.1", "`id` INT(11) DEFAULT 1.1"},
		{"id INT(11) UNIQUE KEY", "`id` INT(11) UNIQUE KEY"},
		{"id INT(11) on update CURRENT_TIMESTAMP", "`id` INT(11) ON UPDATE CURRENT_TIMESTAMP()"},
		{"id INT(11) comment 'hello'", "`id` INT(11) COMMENT 'hello'"},
		{"id INT(11) generated always as(id + 1)", "`id` INT(11) GENERATED ALWAYS AS(`id`+1)"},
		{"id INT(11) REFERENCES parent(id)", "`id` INT(11) REFERENCES `parent`(`id`)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateTableStmt).Cols[0]
	}
	RunNodeRestoreTest(c, testCases, "CREATE TABLE t (%s)", extractNodeFunc)
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

func (ts *testDDLSuite) TestColumnPositionRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"", ""},
		{"first", "FIRST"},
		{"after b", "AFTER `b`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*AlterTableStmt).Specs[0].Position
	}
	RunNodeRestoreTest(c, testCases, "alter table t add column a varchar(255) %s", extractNodeFunc)
}
