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
	constraint := &Constraint{Keys: []*IndexPartSpecification{{Column: &ColumnName{}}, {Column: &ColumnName{}}}, Refer: &ReferenceDef{}, Option: &IndexOption{}}

	alterTableSpec := &AlterTableSpec{Constraint: constraint, Options: []*TableOption{{}}, NewTable: &TableName{}, NewColumns: []*ColumnDef{{Name: &ColumnName{}}}, OldColumnName: &ColumnName{}, Position: &ColumnPosition{RelativeColumn: &ColumnName{}}}

	stmts := []struct {
		node             Node
		expectedEnterCnt int
		expectedLeaveCnt int
	}{
		{&CreateDatabaseStmt{}, 0, 0},
		{&AlterDatabaseStmt{}, 0, 0},
		{&DropDatabaseStmt{}, 0, 0},
		{&DropIndexStmt{Table: &TableName{}}, 0, 0},
		{&DropTableStmt{Tables: []*TableName{{}, {}}}, 0, 0},
		{&RenameTableStmt{OldTable: &TableName{}, NewTable: &TableName{}}, 0, 0},
		{&TruncateTableStmt{Table: &TableName{}}, 0, 0},

		// TODO: cover children
		{&AlterTableStmt{Table: &TableName{}, Specs: []*AlterTableSpec{alterTableSpec}}, 0, 0},
		{&CreateIndexStmt{Table: &TableName{}}, 0, 0},
		{&CreateTableStmt{Table: &TableName{}, ReferTable: &TableName{}}, 0, 0},
		{&CreateViewStmt{ViewName: &TableName{}, Select: &SelectStmt{}}, 0, 0},
		{&AlterTableSpec{}, 0, 0},
		{&ColumnDef{Name: &ColumnName{}, Options: []*ColumnOption{{Expr: ce}}}, 1, 1},
		{&ColumnOption{Expr: ce}, 1, 1},
		{&ColumnPosition{RelativeColumn: &ColumnName{}}, 0, 0},
		{&Constraint{Keys: []*IndexPartSpecification{{Column: &ColumnName{}}, {Column: &ColumnName{}}}, Refer: &ReferenceDef{}, Option: &IndexOption{}}, 0, 0},
		{&IndexPartSpecification{Column: &ColumnName{}}, 0, 0},
		{&ReferenceDef{Table: &TableName{}, IndexPartSpecifications: []*IndexPartSpecification{{Column: &ColumnName{}}, {Column: &ColumnName{}}}, OnDelete: &OnDeleteOpt{}, OnUpdate: &OnUpdateOpt{}}, 0, 0},
		{&AlterTableSpec{NewConstraints: []*Constraint{constraint, constraint}}, 0, 0},
		{&AlterTableSpec{NewConstraints: []*Constraint{constraint}, NewColumns: []*ColumnDef{{Name: &ColumnName{}}}}, 0, 0},
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
		{"(a + 1)", "(`a`+1)"},
		{"(1 * 1 + (1 + 1))", "(1*1+(1+1))"},
		{"((1 * 1 + (1 + 1)))", "((1*1+(1+1)))"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateIndexStmt).IndexPartSpecifications[0]
	}
	RunNodeRestoreTest(c, testCases, "CREATE INDEX idx ON t (%s) USING HASH", extractNodeFunc)
}

func (ts *testDDLSuite) TestDDLIndexExprRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"world", "`world`"},
		{"world(2)", "`world`(2)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*CreateIndexStmt).IndexPartSpecifications[0]
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
	RunNodeRestoreTest(c, testCases, "CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) on update CASCADE %s)", extractNodeFunc)
	RunNodeRestoreTest(c, testCases, "CREATE TABLE child (id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) %s on update CASCADE)", extractNodeFunc)
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
	RunNodeRestoreTest(c, testCases, "CREATE TABLE child ( id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id) %s ON DELETE CASCADE)", extractNodeFunc)
	RunNodeRestoreTest(c, testCases, "CREATE TABLE child ( id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id)  %s )", extractNodeFunc)
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
		{"REFERENCES parent((id+1))", "REFERENCES `parent`((`id`+1))"},
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
		{"INDEX expr_ind ((id + parent_id))", "INDEX `expr_ind`((`id`+`parent_id`))"},
		{"INDEX expr_ind ((lower(id)))", "INDEX `expr_ind`((LOWER(`id`)))"},
		{"key par_ind (parent_id)", "INDEX `par_ind`(`parent_id`)"},
		{"key expr_ind ((lower(id)))", "INDEX `expr_ind`((LOWER(`id`)))"},
		{"unique par_ind (parent_id)", "UNIQUE `par_ind`(`parent_id`)"},
		{"unique key par_ind (parent_id)", "UNIQUE `par_ind`(`parent_id`)"},
		{"unique index par_ind (parent_id)", "UNIQUE `par_ind`(`parent_id`)"},
		{"unique expr_ind ((id + parent_id))", "UNIQUE `expr_ind`((`id`+`parent_id`))"},
		{"unique expr_ind ((lower(id)))", "UNIQUE `expr_ind`((LOWER(`id`)))"},
		{"unique key expr_ind ((id + parent_id))", "UNIQUE `expr_ind`((`id`+`parent_id`))"},
		{"unique key expr_ind ((lower(id)))", "UNIQUE `expr_ind`((LOWER(`id`)))"},
		{"unique index expr_ind ((id + parent_id))", "UNIQUE `expr_ind`((`id`+`parent_id`))"},
		{"unique index expr_ind ((lower(id)))", "UNIQUE `expr_ind`((LOWER(`id`)))"},
		{"fulltext key full_id (parent_id)", "FULLTEXT `full_id`(`parent_id`)"},
		{"fulltext INDEX full_id (parent_id)", "FULLTEXT `full_id`(`parent_id`)"},
		{"fulltext INDEX full_id ((parent_id+1))", "FULLTEXT `full_id`((`parent_id`+1))"},
		{"PRIMARY KEY (id)", "PRIMARY KEY(`id`)"},
		{"PRIMARY KEY (id) key_block_size = 32 using hash comment 'hello'", "PRIMARY KEY(`id`) KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'"},
		{"PRIMARY KEY ((id+1))", "PRIMARY KEY((`id`+1))"},
		{"CONSTRAINT FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"},
		{"CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"},
		{"CONSTRAINT FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent((id+1)) ON DELETE CASCADE", "CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`((`id`+1)) ON DELETE CASCADE"},
		{"CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent((id+1)) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`((`id`+1)) ON DELETE CASCADE ON UPDATE RESTRICT"},
		{"CONSTRAINT fk_123 FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"},
		{"CONSTRAINT fk_123 FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"},
		{"CONSTRAINT fk_123 FOREIGN KEY ((parent_id+1),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "CONSTRAINT `fk_123` FOREIGN KEY ((`parent_id`+1), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"},
		{"CONSTRAINT fk_123 FOREIGN KEY ((parent_id+1)) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT `fk_123` FOREIGN KEY ((`parent_id`+1)) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"},
		{"FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"},
		{"FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"},
		{"FOREIGN KEY ((parent_id+1),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "CONSTRAINT FOREIGN KEY ((`parent_id`+1), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"},
		{"FOREIGN KEY ((parent_id+1)) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "CONSTRAINT FOREIGN KEY ((`parent_id`+1)) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"},
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
		{"generated always as(id + 1)", "GENERATED ALWAYS AS(`id`+1) VIRTUAL"},
		{"generated always as(id + 1) virtual", "GENERATED ALWAYS AS(`id`+1) VIRTUAL"},
		{"generated always as(id + 1) stored", "GENERATED ALWAYS AS(`id`+1) STORED"},
		{"REFERENCES parent(id)", "REFERENCES `parent`(`id`)"},
		{"COLLATE utf8_bin", "COLLATE utf8_bin"},
		{"STORAGE DEFAULT", "STORAGE DEFAULT"},
		{"STORAGE DISK", "STORAGE DISK"},
		{"STORAGE MEMORY", "STORAGE MEMORY"},
		{"AUTO_RANDOM (3)", "AUTO_RANDOM(3)"},
		{"AUTO_RANDOM", "AUTO_RANDOM"},
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
		{"id TEXT CHARACTER SET UTF8 COLLATE UTF8_UNICODE_CI", "`id` TEXT CHARACTER SET UTF8 COLLATE utf8_unicode_ci"},
		{"id text character set UTF8", "`id` TEXT CHARACTER SET UTF8"},
		{"id text charset UTF8", "`id` TEXT CHARACTER SET UTF8"},
		{"id varchar(50) collate UTF8MB4_CZECH_CI", "`id` VARCHAR(50) COLLATE utf8mb4_czech_ci"},
		{"id varchar(50) collate utf8_bin", "`id` VARCHAR(50) COLLATE utf8_bin"},
		{"id varchar(50) collate utf8_unicode_ci collate utf8mb4_bin", "`id` VARCHAR(50) COLLATE utf8_unicode_ci COLLATE utf8mb4_bin"},
		{"c1 char(10) character set LATIN1 collate latin1_german1_ci", "`c1` CHAR(10) CHARACTER SET LATIN1 COLLATE latin1_german1_ci"},

		{"id int(11) PRIMARY KEY", "`id` INT(11) PRIMARY KEY"},
		{"id int(11) NOT NULL", "`id` INT(11) NOT NULL"},
		{"id INT(11) NULL", "`id` INT(11) NULL"},
		{"id INT(11) auto_increment", "`id` INT(11) AUTO_INCREMENT"},
		{"id INT(11) DEFAULT 10", "`id` INT(11) DEFAULT 10"},
		{"id INT(11) DEFAULT '10'", "`id` INT(11) DEFAULT '10'"},
		{"id INT(11) DEFAULT 1.1", "`id` INT(11) DEFAULT 1.1"},
		{"id INT(11) UNIQUE KEY", "`id` INT(11) UNIQUE KEY"},
		{"id INT(11) COLLATE ascii_bin", "`id` INT(11) COLLATE ascii_bin"},
		{"id INT(11) collate ascii_bin collate utf8_bin", "`id` INT(11) COLLATE ascii_bin COLLATE utf8_bin"},
		{"id INT(11) on update CURRENT_TIMESTAMP", "`id` INT(11) ON UPDATE CURRENT_TIMESTAMP()"},
		{"id INT(11) comment 'hello'", "`id` INT(11) COMMENT 'hello'"},
		{"id INT(11) generated always as(id + 1)", "`id` INT(11) GENERATED ALWAYS AS(`id`+1) VIRTUAL"},
		{"id INT(11) REFERENCES parent(id)", "`id` INT(11) REFERENCES `parent`(`id`)"},

		{"id bit", "`id` BIT(1)"},
		{"id bit(1)", "`id` BIT(1)"},
		{"id bit(64)", "`id` BIT(64)"},
		{"id tinyint", "`id` TINYINT"},
		{"id tinyint(255)", "`id` TINYINT(255)"},
		{"id bool", "`id` TINYINT(1)"},
		{"id boolean", "`id` TINYINT(1)"},
		{"id smallint", "`id` SMALLINT"},
		{"id smallint(255)", "`id` SMALLINT(255)"},
		{"id mediumint", "`id` MEDIUMINT"},
		{"id mediumint(255)", "`id` MEDIUMINT(255)"},
		{"id int", "`id` INT"},
		{"id int(255)", "`id` INT(255)"},
		{"id integer", "`id` INT"},
		{"id integer(255)", "`id` INT(255)"},
		{"id bigint", "`id` BIGINT"},
		{"id bigint(255)", "`id` BIGINT(255)"},
		{"id decimal", "`id` DECIMAL"},
		{"id decimal(10)", "`id` DECIMAL(10)"},
		{"id decimal(10,0)", "`id` DECIMAL(10,0)"},
		{"id decimal(65)", "`id` DECIMAL(65)"},
		{"id decimal(65,30)", "`id` DECIMAL(65,30)"},
		{"id dec(10,0)", "`id` DECIMAL(10,0)"},
		{"id numeric(10,0)", "`id` DECIMAL(10,0)"},
		{"id float(0)", "`id` FLOAT"},
		{"id float(24)", "`id` FLOAT"},
		{"id float(25)", "`id` DOUBLE"},
		{"id float(53)", "`id` DOUBLE"},
		{"id float(7,0)", "`id` FLOAT(7,0)"},
		{"id float(25,0)", "`id` FLOAT(25,0)"},
		{"id double(15,0)", "`id` DOUBLE(15,0)"},
		{"id double precision(15,0)", "`id` DOUBLE(15,0)"},
		{"id real(15,0)", "`id` DOUBLE(15,0)"},
		{"id year(4)", "`id` YEAR(4)"},
		{"id time", "`id` TIME"},
		{"id char", "`id` CHAR"},
		{"id char(0)", "`id` CHAR(0)"},
		{"id char(255)", "`id` CHAR(255)"},
		{"id national char(0)", "`id` CHAR(0)"},
		{"id binary", "`id` BINARY"},
		{"id varbinary(0)", "`id` VARBINARY(0)"},
		{"id varbinary(65535)", "`id` VARBINARY(65535)"},
		{"id tinyblob", "`id` TINYBLOB"},
		{"id tinytext", "`id` TINYTEXT"},
		{"id blob", "`id` BLOB"},
		{"id blob(0)", "`id` BLOB(0)"},
		{"id blob(65535)", "`id` BLOB(65535)"},
		{"id text(0)", "`id` TEXT(0)"},
		{"id text(65535)", "`id` TEXT(65535)"},
		{"id mediumblob", "`id` MEDIUMBLOB"},
		{"id mediumtext", "`id` MEDIUMTEXT"},
		{"id longblob", "`id` LONGBLOB"},
		{"id longtext", "`id` LONGTEXT"},
		{"id json", "`id` JSON"},
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

func (ts *testDDLSuite) TestDDLDropTableStmtRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"drop table t1", "DROP TABLE `t1`"},
		{"drop table if exists t1", "DROP TABLE IF EXISTS `t1`"},
		{"drop temporary table t1", "DROP TEMPORARY TABLE `t1`"},
		{"drop temporary table if exists t1", "DROP TEMPORARY TABLE IF EXISTS `t1`"},
		{"DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `test`", "DROP TEMPORARY TABLE IF EXISTS `test`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*DropTableStmt)
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

func (ts *testDDLSuite) TestAlterTableSpecRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"ENGINE innodb", "ENGINE = innodb"},
		{"ENGINE = innodb", "ENGINE = innodb"},
		{"ENGINE = 'innodb'", "ENGINE = innodb"},
		{"ENGINE tokudb", "ENGINE = tokudb"},
		{"ENGINE = tokudb", "ENGINE = tokudb"},
		{"ENGINE = 'tokudb'", "ENGINE = tokudb"},
		{"DEFAULT CHARACTER SET utf8", "DEFAULT CHARACTER SET = UTF8"},
		{"DEFAULT CHARACTER SET = utf8", "DEFAULT CHARACTER SET = UTF8"},
		{"DEFAULT CHARSET utf8", "DEFAULT CHARACTER SET = UTF8"},
		{"DEFAULT CHARSET = utf8", "DEFAULT CHARACTER SET = UTF8"},
		{"DEFAULT COLLATE utf8_bin", "DEFAULT COLLATE = UTF8_BIN"},
		{"DEFAULT COLLATE = utf8_bin", "DEFAULT COLLATE = UTF8_BIN"},
		{"AUTO_INCREMENT 3", "AUTO_INCREMENT = 3"},
		{"AUTO_INCREMENT = 6", "AUTO_INCREMENT = 6"},
		{"COMMENT ''", "COMMENT = ''"},
		{"COMMENT 'system role'", "COMMENT = 'system role'"},
		{"COMMENT = 'system role'", "COMMENT = 'system role'"},
		{"AVG_ROW_LENGTH 12", "AVG_ROW_LENGTH = 12"},
		{"AVG_ROW_LENGTH = 6", "AVG_ROW_LENGTH = 6"},
		{"connection 'abc'", "CONNECTION = 'abc'"},
		{"CONNECTION = 'abc'", "CONNECTION = 'abc'"},
		{"checksum 1", "CHECKSUM = 1"},
		{"checksum = 0", "CHECKSUM = 0"},
		{"PASSWORD '123456'", "PASSWORD = '123456'"},
		{"PASSWORD = ''", "PASSWORD = ''"},
		{"compression 'NONE'", "COMPRESSION = 'NONE'"},
		{"compression = 'lz4'", "COMPRESSION = 'lz4'"},
		{"key_block_size 1024", "KEY_BLOCK_SIZE = 1024"},
		{"KEY_BLOCK_SIZE = 1024", "KEY_BLOCK_SIZE = 1024"},
		{"max_rows 1000", "MAX_ROWS = 1000"},
		{"max_rows = 1000", "MAX_ROWS = 1000"},
		{"min_rows 1000", "MIN_ROWS = 1000"},
		{"MIN_ROWS = 1000", "MIN_ROWS = 1000"},
		{"DELAY_KEY_WRITE 1", "DELAY_KEY_WRITE = 1"},
		{"DELAY_KEY_WRITE = 1000", "DELAY_KEY_WRITE = 1000"},
		{"ROW_FORMAT default", "ROW_FORMAT = DEFAULT"},
		{"ROW_FORMAT = default", "ROW_FORMAT = DEFAULT"},
		{"ROW_FORMAT = fixed", "ROW_FORMAT = FIXED"},
		{"ROW_FORMAT = compressed", "ROW_FORMAT = COMPRESSED"},
		{"ROW_FORMAT = compact", "ROW_FORMAT = COMPACT"},
		{"ROW_FORMAT = redundant", "ROW_FORMAT = REDUNDANT"},
		{"ROW_FORMAT = dynamic", "ROW_FORMAT = DYNAMIC"},
		{"ROW_FORMAT tokudb_default", "ROW_FORMAT = TOKUDB_DEFAULT"},
		{"ROW_FORMAT = tokudb_default", "ROW_FORMAT = TOKUDB_DEFAULT"},
		{"ROW_FORMAT = tokudb_fast", "ROW_FORMAT = TOKUDB_FAST"},
		{"ROW_FORMAT = tokudb_small", "ROW_FORMAT = TOKUDB_SMALL"},
		{"ROW_FORMAT = tokudb_zlib", "ROW_FORMAT = TOKUDB_ZLIB"},
		{"ROW_FORMAT = tokudb_quicklz", "ROW_FORMAT = TOKUDB_QUICKLZ"},
		{"ROW_FORMAT = tokudb_lzma", "ROW_FORMAT = TOKUDB_LZMA"},
		{"ROW_FORMAT = tokudb_snappy", "ROW_FORMAT = TOKUDB_SNAPPY"},
		{"ROW_FORMAT = tokudb_uncompressed", "ROW_FORMAT = TOKUDB_UNCOMPRESSED"},
		{"shard_row_id_bits 1", "SHARD_ROW_ID_BITS = 1"},
		{"shard_row_id_bits = 1", "SHARD_ROW_ID_BITS = 1"},
		{"CONVERT TO CHARACTER SET utf8", "CONVERT TO CHARACTER SET UTF8"},
		{"CONVERT TO CHARSET utf8", "CONVERT TO CHARACTER SET UTF8"},
		{"CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin", "CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN"},
		{"CONVERT TO CHARSET utf8 COLLATE utf8_bin", "CONVERT TO CHARACTER SET UTF8 COLLATE UTF8_BIN"},
		{"ADD COLUMN (a SMALLINT UNSIGNED)", "ADD COLUMN (`a` SMALLINT UNSIGNED)"},
		{"ADD COLUMN (a SMALLINT UNSIGNED, b varchar(255))", "ADD COLUMN (`a` SMALLINT UNSIGNED, `b` VARCHAR(255))"},
		{"ADD COLUMN a SMALLINT UNSIGNED", "ADD COLUMN `a` SMALLINT UNSIGNED"},
		{"ADD COLUMN a SMALLINT UNSIGNED FIRST", "ADD COLUMN `a` SMALLINT UNSIGNED FIRST"},
		{"ADD COLUMN a SMALLINT UNSIGNED AFTER b", "ADD COLUMN `a` SMALLINT UNSIGNED AFTER `b`"},
		{"ADD COLUMN name mediumtext CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL", "ADD COLUMN `name` MEDIUMTEXT CHARACTER SET UTF8MB4 COLLATE utf8mb4_unicode_ci NOT NULL"},
		{"ADD CONSTRAINT INDEX par_ind (parent_id)", "ADD INDEX `par_ind`(`parent_id`)"},
		{"ADD CONSTRAINT INDEX par_ind (parent_id(6))", "ADD INDEX `par_ind`(`parent_id`(6))"},
		{"ADD CONSTRAINT key par_ind (parent_id)", "ADD INDEX `par_ind`(`parent_id`)"},
		{"ADD CONSTRAINT unique par_ind (parent_id)", "ADD UNIQUE `par_ind`(`parent_id`)"},
		{"ADD CONSTRAINT unique key par_ind (parent_id)", "ADD UNIQUE `par_ind`(`parent_id`)"},
		{"ADD CONSTRAINT unique index par_ind (parent_id)", "ADD UNIQUE `par_ind`(`parent_id`)"},
		{"ADD CONSTRAINT fulltext key full_id (parent_id)", "ADD FULLTEXT `full_id`(`parent_id`)"},
		{"ADD CONSTRAINT fulltext INDEX full_id (parent_id)", "ADD FULLTEXT `full_id`(`parent_id`)"},
		{"ADD CONSTRAINT PRIMARY KEY (id)", "ADD PRIMARY KEY(`id`)"},
		{"ADD CONSTRAINT PRIMARY KEY (id) key_block_size = 32 using hash comment 'hello'", "ADD PRIMARY KEY(`id`) KEY_BLOCK_SIZE=32 USING HASH COMMENT 'hello'"},
		{"ADD CONSTRAINT FOREIGN KEY (parent_id(2),hello(4)) REFERENCES parent(id) ON DELETE CASCADE", "ADD CONSTRAINT FOREIGN KEY (`parent_id`(2), `hello`(4)) REFERENCES `parent`(`id`) ON DELETE CASCADE"},
		{"ADD CONSTRAINT FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "ADD CONSTRAINT FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"},
		{"ADD CONSTRAINT fk_123 FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE ON UPDATE RESTRICT", "ADD CONSTRAINT `fk_123` FOREIGN KEY (`parent_id`) REFERENCES `parent`(`id`) ON DELETE CASCADE ON UPDATE RESTRICT"},
		{"DROP COLUMN a", "DROP COLUMN `a`"},
		{"DROP COLUMN a RESTRICT", "DROP COLUMN `a`"},
		{"DROP COLUMN a CASCADE", "DROP COLUMN `a`"},
		{"DROP PRIMARY KEY", "DROP PRIMARY KEY"},
		{"drop index a", "DROP INDEX `a`"},
		{"drop key a", "DROP INDEX `a`"},
		{"drop FOREIGN key a", "DROP FOREIGN KEY `a`"},
		{"MODIFY column a varchar(255)", "MODIFY COLUMN `a` VARCHAR(255)"},
		{"modify COLUMN a varchar(255) FIRST", "MODIFY COLUMN `a` VARCHAR(255) FIRST"},
		{"modify COLUMN a varchar(255) AFTER b", "MODIFY COLUMN `a` VARCHAR(255) AFTER `b`"},
		{"change column a b VARCHAR(255)", "CHANGE COLUMN `a` `b` VARCHAR(255)"},
		{"change COLUMN a b varchar(255) CHARACTER SET UTF8 BINARY", "CHANGE COLUMN `a` `b` VARCHAR(255) BINARY CHARACTER SET UTF8"},
		{"CHANGE column a b varchar(255) FIRST", "CHANGE COLUMN `a` `b` VARCHAR(255) FIRST"},
		{"change COLUMN a b varchar(255) AFTER c", "CHANGE COLUMN `a` `b` VARCHAR(255) AFTER `c`"},
		{"RENAME db1.t1", "RENAME AS `db1`.`t1`"},
		{"RENAME to db1.t1", "RENAME AS `db1`.`t1`"},
		{"RENAME as t1", "RENAME AS `t1`"},
		{"ALTER a SET DEFAULT 1", "ALTER COLUMN `a` SET DEFAULT 1"},
		{"ALTER a DROP DEFAULT", "ALTER COLUMN `a` DROP DEFAULT"},
		{"ALTER COLUMN a SET DEFAULT 1", "ALTER COLUMN `a` SET DEFAULT 1"},
		{"ALTER COLUMN a DROP DEFAULT", "ALTER COLUMN `a` DROP DEFAULT"},
		{"LOCK=NONE", "LOCK = NONE"},
		{"LOCK=DEFAULT", "LOCK = DEFAULT"},
		{"LOCK=SHARED", "LOCK = SHARED"},
		{"LOCK=EXCLUSIVE", "LOCK = EXCLUSIVE"},
		{"RENAME KEY a TO b", "RENAME INDEX `a` TO `b`"},
		{"RENAME INDEX a TO b", "RENAME INDEX `a` TO `b`"},
		{"ADD PARTITION", "ADD PARTITION"},
		{"ADD PARTITION ( PARTITION P1 VALUES LESS THAN (2010))", "ADD PARTITION (PARTITION `P1` VALUES LESS THAN (2010))"},
		{"ADD PARTITION ( PARTITION P2 VALUES LESS THAN MAXVALUE)", "ADD PARTITION (PARTITION `P2` VALUES LESS THAN (MAXVALUE))"},
		{"ADD PARTITION (\nPARTITION P1 VALUES LESS THAN (2010),\nPARTITION P2 VALUES LESS THAN (2015),\nPARTITION P3 VALUES LESS THAN MAXVALUE)", "ADD PARTITION (PARTITION `P1` VALUES LESS THAN (2010), PARTITION `P2` VALUES LESS THAN (2015), PARTITION `P3` VALUES LESS THAN (MAXVALUE))"},
		{"ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT 'AP_START \\' AP_END')", "ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'AP_START '' AP_END')"},
		{"ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')", "ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')"},
		{"coalesce partition 3", "COALESCE PARTITION 3"},
		{"drop partition p1", "DROP PARTITION `p1`"},
		{"TRUNCATE PARTITION p0", "TRUNCATE PARTITION `p0`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node.(*AlterTableStmt).Specs[0]
	}
	RunNodeRestoreTest(c, testCases, "ALTER TABLE t %s", extractNodeFunc)
}

func (ts *testDDLSuite) TestAdminRepairTableRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"ADMIN REPAIR TABLE t CREATE TABLE t (a int)", "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` INT)"},
		{"ADMIN REPAIR TABLE t CREATE TABLE t (a char(1), b int)", "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` CHAR(1),`b` INT)"},
		{"ADMIN REPAIR TABLE t CREATE TABLE t (a TINYINT UNSIGNED)", "ADMIN REPAIR TABLE `t` CREATE TABLE `t` (`a` TINYINT UNSIGNED)"},
	}
	extractNodeFunc := func(node Node) Node {
		return node
	}
	RunNodeRestoreTest(c, testCases, "%s", extractNodeFunc)
}

func (ts *testDDLSuite) TestSequenceRestore(c *C) {
	testCases := []NodeRestoreTestCase{
		{"create sequence seq", "CREATE SEQUENCE `seq`"},
		{"create sequence if not exists seq", "CREATE SEQUENCE IF NOT EXISTS `seq`"},
		{"create sequence if not exists seq", "CREATE SEQUENCE IF NOT EXISTS `seq`"},
		{"create sequence if not exists seq increment 1", "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1"},
		{"create sequence if not exists seq increment = 1", "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1"},
		{"create sequence if not exists seq minvalue 1", "CREATE SEQUENCE IF NOT EXISTS `seq` MINVALUE 1"},
		{"create sequence if not exists seq minvalue = 1", "CREATE SEQUENCE IF NOT EXISTS `seq` MINVALUE 1"},
		{"create sequence if not exists seq nominvalue", "CREATE SEQUENCE IF NOT EXISTS `seq` NO MINVALUE"},
		{"create sequence if not exists seq no minvalue", "CREATE SEQUENCE IF NOT EXISTS `seq` NO MINVALUE"},
		{"create sequence if not exists seq maxvalue 1", "CREATE SEQUENCE IF NOT EXISTS `seq` MAXVALUE 1"},
		{"create sequence if not exists seq maxvalue = 1", "CREATE SEQUENCE IF NOT EXISTS `seq` MAXVALUE 1"},
		{"create sequence if not exists seq nomaxvalue", "CREATE SEQUENCE IF NOT EXISTS `seq` NO MAXVALUE"},
		{"create sequence if not exists seq no maxvalue", "CREATE SEQUENCE IF NOT EXISTS `seq` NO MAXVALUE"},
		{"create sequence if not exists seq start 1", "CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1"},
		{"create sequence if not exists seq start with 1", "CREATE SEQUENCE IF NOT EXISTS `seq` START WITH 1"},
		{"create sequence if not exists seq cache 1", "CREATE SEQUENCE IF NOT EXISTS `seq` CACHE 1"},
		{"create sequence if not exists seq nocache", "CREATE SEQUENCE IF NOT EXISTS `seq` NOCACHE"},
		{"create sequence if not exists seq no cache", "CREATE SEQUENCE IF NOT EXISTS `seq` NOCACHE"},
		{"create sequence if not exists seq cycle", "CREATE SEQUENCE IF NOT EXISTS `seq` CYCLE"},
		{"create sequence if not exists seq nocycle", "CREATE SEQUENCE IF NOT EXISTS `seq` NOCYCLE"},
		{"create sequence if not exists seq no cycle", "CREATE SEQUENCE IF NOT EXISTS `seq` NOCYCLE"},
		{"create sequence seq increment 1 minvalue 0 maxvalue 1000", "CREATE SEQUENCE `seq` INCREMENT BY 1 MINVALUE 0 MAXVALUE 1000"},
		{"create sequence seq minvalue 0 maxvalue 1000 increment 1", "CREATE SEQUENCE `seq` MINVALUE 0 MAXVALUE 1000 INCREMENT BY 1"},
		{"create sequence seq cache = 1 minvalue 0 maxvalue -1000", "CREATE SEQUENCE `seq` CACHE 1 MINVALUE 0 MAXVALUE -1000"},
		{"create sequence seq increment -1 minvalue 0 maxvalue -1000", "CREATE SEQUENCE `seq` INCREMENT BY -1 MINVALUE 0 MAXVALUE -1000"},
		{"create sequence seq nocycle nocache maxvalue 1000 cache 1", "CREATE SEQUENCE `seq` NOCYCLE NOCACHE MAXVALUE 1000 CACHE 1"},
		{"create sequence seq increment -1 no minvalue no maxvalue cache = 1", "CREATE SEQUENCE `seq` INCREMENT BY -1 NO MINVALUE NO MAXVALUE CACHE 1"},
		{"create sequence if not exists seq increment 1 minvalue 0 nomaxvalue cache 100 nocycle", "CREATE SEQUENCE IF NOT EXISTS `seq` INCREMENT BY 1 MINVALUE 0 NO MAXVALUE CACHE 100 NOCYCLE"},

		// test drop sequence
		{"drop sequence seq", "DROP SEQUENCE `seq`"},
		{"drop sequence seq, seq2", "DROP SEQUENCE `seq`, `seq2`"},
		{"drop sequence if exists seq, seq2", "DROP SEQUENCE IF EXISTS `seq`, `seq2`"},
		{"drop sequence if exists seq", "DROP SEQUENCE IF EXISTS `seq`"},
		{"drop sequence sequence", "DROP SEQUENCE `sequence`"},
	}
	extractNodeFunc := func(node Node) Node {
		return node
	}
	RunNodeRestoreTest(c, testCases, "%s", extractNodeFunc)
}
