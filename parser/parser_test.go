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

package parser

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/stmt/stmts"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testParserSuite{})

type testParserSuite struct {
}

// TODO: table 43 and 50 parse failed
func (s *testParserSuite) TestParser0(c *C) {
	table := []struct {
		src string
		ok  bool
	}{
		{"", true},
		{";", true},
		{"CREATE", false},
		{"CREATE TABLE", false},
		{"CREATE TABLE foo (", false},
		// 5
		{"CREATE TABLE foo ()", false},
		{"CREATE TABLE foo ();", false},
		{"CREATE TABLE foo (a TINYINT UNSIGNED);", true},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true},
		// 10
		{"CREATE TABLE foo (a bigint unsigned, b bool);", true},
		{"CREATE TABLE foo (a TINYINT, b SMALLINT) CREATE TABLE bar (x INT, y int64)", false},
		{"CREATE TABLE foo (a int, b float); CREATE TABLE bar (x double, y float)", true},
		{"INSERT INTO foo VALUES (1234)", true},
		{"INSERT INTO foo VALUES (1234, 5678)", true},
		// 15
		{"INSERT INTO foo VALUES (1 || 2)", true},
		{"INSERT INTO foo VALUES (1 | 2)", true},
		{"INSERT INTO foo VALUES (false || true)", true},
		{"INSERT INTO foo VALUES (bar(5678))", false},
		// 20
		{"INSERT INTO foo VALUES ()", true},
		{"CREATE TABLE foo (a.b, b);", false},
		{"CREATE TABLE foo (a, b.c);", false},
		{"SELECT * FROM t", true},
		{"SELECT * FROM t AS u", true},
		// 25
		{"SELECT * FROM t, v", true},
		{"SELECT * FROM t AS u, v", true},
		{"SELECT * FROM t, v AS w", true},
		{"SELECT * FROM t AS u, v AS w", true},
		{"SELECT * FROM foo, bar, foo", true},
		// 30
		{"CREATE TABLE foo (a bytes)", false},
		{"SELECT DISTINCTS * FROM t", false},
		{"SELECT DISTINCT * FROM t", true},
		{"INSERT INTO foo (a) VALUES (42)", true},
		{"INSERT INTO foo (a,) VALUES (42,)", false},
		// 35
		{"INSERT INTO foo (a,b) VALUES (42,314)", true},
		{"INSERT INTO foo (a,b,) VALUES (42,314)", false},
		{"INSERT INTO foo (a,b,) VALUES (42,314,)", false},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) -- foo", true},
		// 40
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) // foo", true},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true},
		{"CREATE TABLE foo /* foo */ (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true},
		/*{`-- Examples
		ALTER TABLE Stock ADD Qty int;

		ALTER TABLE Income DROP COLUMN Taxes;

		CREATE TABLE department
		(
			DepartmentID   int,
			DepartmentName string,	// optional comma
		);

		CREATE TABLE employee
		(
			LastName	string,
			DepartmentID	int	// optional comma
		);

		DROP TABLE Inventory;

		INSERT INTO department (DepartmentID) VALUES (42);

		INSERT INTO department (
			DepartmentName,
			DepartmentID,
		)
		VALUES (
			"R&D",
			42,
		);

		INSERT INTO department VALUES (
			42,
			"R&D",
		);

		SELECT * FROM Stock;

		SELECT DepartmentID
		FROM department
		WHERE DepartmentID == 42
		ORDER BY DepartmentName;

		SELECT employee.LastName
		FROM department, employee
		WHERE department.DepartmentID == employee.DepartmentID
		ORDER BY DepartmentID;

		SELECT a.b, c.d
		FROM
			x AS a,
			(
				SELECT * FROM y; // optional semicolon
			) AS c
		WHERE a.e > c.e;

		SELECT a.b, c.d
		FROM
			x AS a,
			(
				SELECT * FROM y // no semicolon
			) AS c
		WHERE a.e > c.e;

		TRUNCATE TABLE department;

		SELECT DepartmentID
		FROM department
		WHERE DepartmentID == ?1
		ORDER BY DepartmentName;

		SELECT employee.LastName
		FROM department, employee
		WHERE department.DepartmentID == $1 && employee.LastName > $2
		ORDER BY DepartmentID;

		`, true},
		*/
		{"BEGIN", true},
		{"START TRANSACTION", true},
		// 45
		{"COMMIT", true},
		{"ROLLBACK", true},
		{`
		BEGIN;
			INSERT INTO foo VALUES (42, 3.14);
			INSERT INTO foo VALUES (-1, 2.78);
		COMMIT;`, true},
		{` // A
		BEGIN;
			INSERT INTO tmp SELECT * from bar;
		SELECT * from tmp;

		// B
		ROLLBACK;`, true},
		// 50
		/*
			{`-- 6
				ALTER TABLE none DROP COLUMN c1;
			`, true},
		*/

		// set
		// user defined
		{"SET @a = 1", true},
		// session system variables
		{"SET SESSION autocommit = 1", true},
		{"SET @@session.autocommit = 1", true},
		{"SET LOCAL autocommit = 1", true},
		{"SET @@local.autocommit = 1", true},
		{"SET @@autocommit = 1", true},
		{"SET autocommit = 1", true},
		// global system variables
		{"SET GLOBAL autocommit = 1", true},
		{"SET @@global.autocommit = 1", true},
		// SET CHARACTER SET
		{"SET CHARACTER SET utf8mb4;", true},
		{"SET CHARACTER SET 'utf8mb4';", true},

		// qualified select
		{"SELECT a.b.c FROM t", true},
		{"SELECT a.b.*.c FROM t", false},
		{"SELECT a.b.* FROM t", true},
		{"SELECT a FROM t", true},
		{"SELECT a.b.c.d FROM t", false},

		// Do statement
		{"DO 1", true},
		{"DO 1 from t", false},

		// Sign expression
		{"SELECT ++1", true},
		{"SELECT -*1", false},
		{"SELECT -+1", true},
		{"SELECT -1", true},
		{"SELECT --1", true},

		// Select for update
		{"SELECT * from t for update", true},
		{"SELECT * from t lock in share mode", true},

		// For alter table
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED", true},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED FIRST", true},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED AFTER b", true},

		// from join
		{"SELECT * from t1, t2, t3", true},
		{"select * from t1 join t2 left join t3 on t2.id = t3.id", true},
		{"select * from t1 right join t2 on t1.id = t2.id left join t3 on t3.id = t2.id", true},
		{"select * from t1 right join t2 on t1.id = t2.id left join t3", false},

		// For default value
		{"CREATE TABLE sbtest (id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, k integer UNSIGNED DEFAULT '0' NOT NULL, c char(120) DEFAULT '' NOT NULL, pad char(60) DEFAULT '' NOT NULL, PRIMARY KEY  (id) )", true},

		// For show full columns
		{"show columns in t;", true},
		{"show full columns in t;", true},

		// For set names
		{"set names utf8", true},
		{"set names utf8 collate utf8_unicode_ci", true},

		// For show character set
		{"show character set;", true},
		// For on duplicate key update
		{"INSERT INTO t (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b);", true},
		{"INSERT IGNORE INTO t (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b);", true},

		// For buildin functions
		{"SELECT DAYOFMONTH('2007-02-03');", true},

		{"SELECT SUBSTRING('Quadratically',5);", true},
		{"SELECT SUBSTRING('Quadratically',5, 3);", true},
		{"SELECT SUBSTRING('Quadratically' FROM 5);", true},
		{"SELECT SUBSTRING('Quadratically' FROM 5 FOR 3);", true},

		{"SELECT CONVERT('111', SIGNED);", true},

		// For delete statement
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;", true},
		{"DELETE FROM t1, t2 USING t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;", true},
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id limit 10;", false},

		// For time fsp
		{"CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true},

		// For row
		{"select row(1)", false},
		{"select row(1, 1,)", false},
		{"select (1, 1,)", false},
		{"select row(1, 1) > row(1, 1), row(1, 1, 1) > row(1, 1, 1)", true},
		{"Select (1, 1) > (1, 1)", true},

		// For SHOW statement
		{"SHOW VARIABLES LIKE 'character_set_results'", true},
		{"SHOW GLOBAL VARIABLES LIKE 'character_set_results'", true},
		{"SHOW SESSION VARIABLES LIKE 'character_set_results'", true},
		{"SHOW VARIABLES", true},
		{"SHOW GLOBAL VARIABLES", true},

		// For compare subquery
		{"SELECT 1 > (select 1)", true},
		{"SELECT 1 > ANY (select 1)", true},
		{"SELECT 1 > ALL (select 1)", true},
		{"SELECT 1 > SOME (select 1)", true},

		// For exists subquery
		{"SELECT EXISTS select 1", false},
		{"SELECT EXISTS (select 1)", true},
	}

	for _, t := range table {
		fmt.Printf("%s\n", t.src)
		l := NewLexer(t.src)
		ok := yyParse(l) == 0
		c.Assert(ok, Equals, t.ok, Commentf("source %v", t.src))

		switch ok {
		case true:
			c.Assert(len(l.errs), Equals, 0)
		case false:
			c.Assert(len(l.errs), Not(Equals), 0)
		}
	}

	// Testcase for unreserved keywords
	unreservedKws := []string{
		"auto_increment", "after", "begin", "bit", "bool", "boolean", "charset", "columns", "commit",
		"date", "datetime", "deallocate", "do", "end", "engine", "engines", "execute", "first", "full",
		"local", "names", "offset", "password", "prepare", "quick", "rollback", "session", "signed",
		"start", "global", "tables", "text", "time", "timestamp", "transaction", "truncate", "unknown",
		"value", "warnings", "year", "now", "substring", "mode", "any", "some",
	}
	for _, kw := range unreservedKws {
		src := fmt.Sprintf("SELECT %s FROM tbl;", kw)
		l := NewLexer(src)
		c.Assert(yyParse(l), Equals, 0)
		c.Assert(l.errs, HasLen, 0, Commentf("source %s", src))
	}

	// Testcase for prepared statement
	src := "SELECT id+?, id+? from t;"
	l := NewLexer(src)
	l.SetPrepare()
	c.Assert(yyParse(l), Equals, 0)
	c.Assert(len(l.ParamList), Equals, 2)
	c.Assert(len(l.Stmts()), Equals, 1)

	// Testcase for -- Comment and unary -- operator
	src = "CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED); -- foo\nSelect --1 from foo;"
	l = NewLexer(src)
	l.SetPrepare()
	c.Assert(yyParse(l), Equals, 0)
	c.Assert(len(l.Stmts()), Equals, 2)

	// Testcase for CONVERT(expr,type)
	src = "SELECT CONVERT('111', SIGNED);"
	l = NewLexer(src)
	c.Assert(yyParse(l), Equals, 0)
	st := l.Stmts()[0]
	ss, ok := st.(*stmts.SelectStmt)
	c.Assert(ok, IsTrue)
	cv, ok := ss.Fields[0].Expr.(*expressions.FunctionCast)
	c.Assert(ok, IsTrue)
	c.Assert(cv.IsConvert, IsTrue)
}
