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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/subquery"
	"github.com/pingcap/tidb/stmt/stmts"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testParserSuite{})

type testParserSuite struct {
}

func (s *testParserSuite) TestOriginText(c *C) {
	src := `SELECT stuff.id 
		FROM stuff 
		WHERE stuff.value >= ALL (SELECT stuff.value 
		FROM stuff)`

	l := NewLexer(src)
	c.Assert(yyParse(l), Equals, 0)
	node := l.Stmts()[0].(*stmts.SelectStmt)
	sq := node.Where.Expr.(*expression.CompareSubQuery).R
	c.Assert(sq, NotNil)
	subsel := sq.(*subquery.SubQuery)
	c.Assert(subsel.Stmt.OriginText(), Equals,
		`SELECT stuff.value 
		FROM stuff`)
}

func (s *testParserSuite) TestSimple(c *C) {
	// Testcase for unreserved keywords
	unreservedKws := []string{
		"auto_increment", "after", "begin", "bit", "bool", "boolean", "charset", "columns", "commit",
		"date", "datetime", "deallocate", "do", "end", "engine", "engines", "execute", "first", "full",
		"local", "names", "offset", "password", "prepare", "quick", "rollback", "session", "signed",
		"start", "global", "tables", "text", "time", "timestamp", "transaction", "truncate", "unknown",
		"value", "warnings", "year", "now", "substring", "mode", "any", "some", "user", "identified",
		"collation", "comment", "avg_row_length", "checksum", "compression", "connection", "key_block_size",
		"max_rows", "min_rows", "national", "row", "quarter", "escape",
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
	cv, ok := ss.Fields[0].Expr.(*expression.FunctionCast)
	c.Assert(ok, IsTrue)
	c.Assert(cv.FunctionType, Equals, expression.ConvertFunction)

	// For query start with comment
	srcs := []string{
		"/* some comments */ SELECT CONVERT('111', SIGNED) ;",
		"/* some comments */ /*comment*/ SELECT CONVERT('111', SIGNED) ;",
		"SELECT /*comment*/ CONVERT('111', SIGNED) ;",
		"SELECT CONVERT('111', /*comment*/ SIGNED) ;",
		"SELECT CONVERT('111', SIGNED) /*comment*/;",
	}
	for _, src := range srcs {
		l = NewLexer(src)
		c.Assert(yyParse(l), Equals, 0)
		st = l.Stmts()[0]
		ss, ok = st.(*stmts.SelectStmt)
		c.Assert(ok, IsTrue)
	}
}

type testCase struct {
	src string
	ok  bool
}

func (s *testParserSuite) RunTest(c *C, table []testCase) {
	for _, t := range table {
		l := NewLexer(t.src)
		ok := yyParse(l) == 0
		c.Assert(ok, Equals, t.ok, Commentf("source %v %v", t.src, l.errs))
		switch ok {
		case true:
			c.Assert(l.errs, HasLen, 0, Commentf("src: %s", t.src))
		case false:
			c.Assert(len(l.errs), Not(Equals), 0, Commentf("src: %s", t.src))
		}
	}
}
func (s *testParserSuite) TestDMLStmt(c *C) {
	table := []testCase{
		{"", true},
		{";", true},
		{"INSERT INTO foo VALUES (1234)", true},
		{"INSERT INTO foo VALUES (1234, 5678)", true},
		// 15
		{"INSERT INTO foo VALUES (1 || 2)", true},
		{"INSERT INTO foo VALUES (1 | 2)", true},
		{"INSERT INTO foo VALUES (false || true)", true},
		{"INSERT INTO foo VALUES (bar(5678))", false},
		// 20
		{"INSERT INTO foo VALUES ()", true},
		{"SELECT * FROM t", true},
		{"SELECT * FROM t AS u", true},
		// 25
		{"SELECT * FROM t, v", true},
		{"SELECT * FROM t AS u, v", true},
		{"SELECT * FROM t, v AS w", true},
		{"SELECT * FROM t AS u, v AS w", true},
		{"SELECT * FROM foo, bar, foo", true},
		// 30
		{"SELECT DISTINCTS * FROM t", false},
		{"SELECT DISTINCT * FROM t", true},
		{"INSERT INTO foo (a) VALUES (42)", true},
		{"INSERT INTO foo (a,) VALUES (42,)", false},
		// 35
		{"INSERT INTO foo (a,b) VALUES (42,314)", true},
		{"INSERT INTO foo (a,b,) VALUES (42,314)", false},
		{"INSERT INTO foo (a,b,) VALUES (42,314,)", false},
		{"INSERT INTO foo () VALUES ()", true},
		{"INSERT INTO foo VALUE ()", true},
		// 40
		{`SELECT stuff.id 
		FROM stuff 
		WHERE stuff.value >= ALL (SELECT stuff.value 
		FROM stuff)`, true},
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
		// Set password
		{"SET PASSWORD = 'password';", true},
		{"SET PASSWORD FOR 'root'@'localhost' = 'password';", true},

		// qualified select
		{"SELECT a.b.c FROM t", true},
		{"SELECT a.b.*.c FROM t", false},
		{"SELECT a.b.* FROM t", true},
		{"SELECT a FROM t", true},
		{"SELECT a.b.c.d FROM t", false},

		// Do statement
		{"DO 1", true},
		{"DO 1 from t", false},

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

		// For SHOW statement
		{"SHOW VARIABLES LIKE 'character_set_results'", true},
		{"SHOW GLOBAL VARIABLES LIKE 'character_set_results'", true},
		{"SHOW SESSION VARIABLES LIKE 'character_set_results'", true},
		{"SHOW VARIABLES", true},
		{"SHOW GLOBAL VARIABLES", true},
		{"SHOW GLOBAL VARIABLES WHERE Variable_name = 'autocommit'", true},
		{`SHOW FULL TABLES FROM icar_qa LIKE play_evolutions`, true},
		{`SHOW FULL TABLES WHERE Table_Type != 'VIEW'`, true},

		// For default value
		{"CREATE TABLE sbtest (id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, k integer UNSIGNED DEFAULT '0' NOT NULL, c char(120) DEFAULT '' NOT NULL, pad char(60) DEFAULT '' NOT NULL, PRIMARY KEY  (id) )", true},

		// For delete statement
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;", true},
		{"DELETE FROM t1, t2 USING t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;", true},
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id limit 10;", false},

		// For update statement
		{"UPDATE t SET id = id + 1 ORDER BY id DESC;", true},
		{"UPDATE items,month SET items.price=month.price WHERE items.id=month.id;", true},
		{"UPDATE items,month SET items.price=month.price WHERE items.id=month.id LIMIT 10;", false},
		{"UPDATE user T0 LEFT OUTER JOIN user_profile T1 ON T1.id = T0.profile_id SET T0.profile_id = 1 WHERE T0.profile_id IN (1);", true},

		// For select with where clause
		{"SELECT * FROM t WHERE 1 = 1", true},

		// For show collation
		{"show collation", true},
		{"show collation like 'utf8%'", true},
		{"show collation where Charset = 'utf8' and Collation = 'utf8_bin'", true},

		// For dual
		{"select 1 from dual", true},
		{"select 1 from dual limit 1", true},
		{"select 1 where exists (select 2)", false},
		{"select 1 from dual where not exists (select 2)", true},

		// For show create table
		{"show create table test.t", true},
		{"show create table t", true},

		// For https://github.com/pingcap/tidb/issues/320
		{`(select 1);`, true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestExpression(c *C) {
	table := []testCase{
		// Sign expression
		{"SELECT ++1", true},
		{"SELECT -*1", false},
		{"SELECT -+1", true},
		{"SELECT -1", true},
		{"SELECT --1", true},

		// For string literal
		{`select '''a''', """a"""`, true},
		{`select ''a''`, false},
		{`select ""a""`, false},
		{`select '''a''';`, true},
		{`select '\'a\'';`, true},
		{`select "\"a\"";`, true},
		{`select """a""";`, true},
		// For comparison
		{"select 1 <=> 0, 1 <=> null, 1 = null", true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestBuiltin(c *C) {
	table := []testCase{
		// For buildin functions
		{"SELECT DAYOFMONTH('2007-02-03');", true},
		{"SELECT RAND();", true},
		{"SELECT RAND(1);", true},

		{"SELECT SUBSTRING('Quadratically',5);", true},
		{"SELECT SUBSTRING('Quadratically',5, 3);", true},
		{"SELECT SUBSTRING('Quadratically' FROM 5);", true},
		{"SELECT SUBSTRING('Quadratically' FROM 5 FOR 3);", true},

		{"SELECT CONVERT('111', SIGNED);", true},

		{"SELECT DATABASE();", true},
		{"SELECT USER();", true},
		{"SELECT CURRENT_USER();", true},
		{"SELECT CURRENT_USER;", true},

		{"SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);", true},
		{"SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2);", true},

		{`SELECT LOWER("A"), UPPER("a")`, true},

		{`SELECT LOCATE('bar', 'foobarbar');`, true},
		{`SELECT LOCATE('bar', 'foobarbar', 5);`, true},

		{"select current_date, current_date(), curdate()", true},

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
		{"create table t (row int)", true},

		// For cast with charset
		{"SELECT *, CAST(data AS CHAR CHARACTER SET utf8) FROM t;", true},

		// For binary operator
		{"SELECT binary 'a';", true},

		// Select time
		{"select current_timestamp", true},
		{"select current_timestamp()", true},
		{"select current_timestamp(6)", true},
		{"select now()", true},
		{"select now(6)", true},
		{"select sysdate(), sysdate(6)", true},

		// For time extract
		{`select extract(microsecond from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(second from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(minute from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(hour from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(day from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(week from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(month from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(quarter from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(year from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(second_microsecond from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(minute_microsecond from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(minute_second from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(hour_microsecond from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(hour_second from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(hour_minute from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(day_microsecond from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(day_second from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(day_minute from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(day_hour from "2011-11-11 10:10:10.123456")`, true},
		{`select extract(year_month from "2011-11-11 10:10:10.123456")`, true},

		// For issue 224
		{`SELECT CAST('test collated returns' AS CHAR CHARACTER SET utf8) COLLATE utf8_bin;`, true},

		// For trim
		{`SELECT TRIM('  bar   ');`, true},
		{`SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');`, true},
		{`SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx');`, true},
		{`SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');`, true},

		// For date_add
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 second)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 minute)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 hour)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 day)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 week)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 month)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 quarter)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 year)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestIdentifier(c *C) {
	table := []testCase{
		// For quote identifier
		{"select `a`, `a.b`, `a b` from t", true},
		// For unquoted identifier
		{"create table MergeContextTest$Simple (value integer not null, primary key (value))", true},
		// For as
		{"select 1 as a, 1 as `a`, 1 as \"a\", 1 as 'a'", true},
		{`select 1 as a, 1 as "a", 1 as 'a'`, true},
		{`select 1 a, 1 "a", 1 'a'`, true},
		{`select * from t as "a"`, false},
		{`select * from t a`, true},
		{`select * from t as a`, true},
		{"select 1 full, 1 row, 1 abs", true},
		{"select * from t full, t1 row, t2 abs", true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestDDL(c *C) {
	table := []testCase{
		{"CREATE", false},
		{"CREATE TABLE", false},
		{"CREATE TABLE foo (", false},
		{"CREATE TABLE foo ()", false},
		{"CREATE TABLE foo ();", false},
		{"CREATE TABLE foo (a TINYINT UNSIGNED);", true},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true},
		{"CREATE TABLE foo (a bigint unsigned, b bool);", true},
		{"CREATE TABLE foo (a TINYINT, b SMALLINT) CREATE TABLE bar (x INT, y int64)", false},
		{"CREATE TABLE foo (a int, b float); CREATE TABLE bar (x double, y float)", true},
		{"CREATE TABLE foo (a bytes)", false},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) -- foo", true},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) // foo", true},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true},
		{"CREATE TABLE foo /* foo */ (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true},
		{"CREATE TABLE foo (name CHAR(50) BINARY)", true},
		{"CREATE TABLE foo (name CHAR(50) COLLATE utf8_bin)", true},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET utf8)", true},
		{"CREATE TABLE foo (name CHAR(50) BINARY CHARACTER SET utf8 COLLATE utf8_bin)", true},

		{"CREATE TABLE foo (a.b, b);", false},
		{"CREATE TABLE foo (a, b.c);", false},
		// For table option
		{"create table t (c int) avg_row_length = 3", true},
		{"create table t (c int) avg_row_length 3", true},
		{"create table t (c int) checksum = 0", true},
		{"create table t (c int) checksum 1", true},
		{"create table t (c int) compression = none", true},
		{"create table t (c int) compression lz4", true},
		{"create table t (c int) connection = 'abc'", true},
		{"create table t (c int) connection 'abc'", true},
		{"create table t (c int) key_block_size = 1024", true},
		{"create table t (c int) key_block_size 1024", true},
		{"create table t (c int) max_rows = 1000", true},
		{"create table t (c int) max_rows 1000", true},
		{"create table t (c int) min_rows = 1000", true},
		{"create table t (c int) min_rows 1000", true},
		{"create table t (c int) password = 'abc'", true},
		{"create table t (c int) password 'abc'", true},
		// For check clause
		{"create table t (c1 bool, c2 bool, check (c1 in (0, 1)), check (c2 in (0, 1)))", true},
		{"CREATE TABLE Customer (SD integer CHECK (SD > 0), First_Name varchar(30));", true},

		{"create database xxx", true},
		{"create database if exists xxx", false},
		{"create database if not exists xxx", true},
		{"create schema xxx", true},
		{"create schema if exists xxx", false},
		{"create schema if not exists xxx", true},
		// For drop datbase/schema
		{"drop database xxx", true},
		{"drop database if exists xxx", true},
		{"drop database if not exists xxx", false},
		{"drop schema xxx", true},
		{"drop schema if exists xxx", true},
		{"drop schema if not exists xxx", false},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestType(c *C) {
	table := []testCase{
		// For time fsp
		{"CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true},

		// For hexadecimal
		{"SELECT x'0a', X'11', 0x11", true},
		{"select x'0xaa'", false},
		{"select 0X11", false},

		// For bit
		{"select 0b01, 0b0, b'11', B'11'", true},
		{"select 0B01", false},
		{"select 0b21", false},

		// For enum and set type
		{"create table t (c1 enum('a', 'b'), c2 set('a', 'b'))", true},
		{"create table t (c1 enum)", false},
		{"create table t (c1 set)", false},

		// For blob and text field length
		{"create table t (c1 blob(1024), c2 text(1024))", true},

		// For year
		{"create table t (y year(4), y1 year)", true},

		// For national
		{"create table t (c1 national char(2), c2 national varchar(2))", true},

		// For https://github.com/pingcap/tidb/issues/312
		{`create table t (c float(53));`, true},
		{`create table t (c float(54));`, false},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestPrivilege(c *C) {
	table := []testCase{
		// For create user
		{`CREATE USER IF NOT EXISTS 'root'@'localhost' IDENTIFIED BY 'new-password'`, true},
		{`CREATE USER 'root'@'localhost' IDENTIFIED BY 'new-password'`, true},
		{`CREATE USER 'root'@'localhost' IDENTIFIED BY PASSWORD 'hashstring'`, true},
		{`CREATE USER 'root'@'localhost' IDENTIFIED BY 'new-password', 'root'@'127.0.0.1' IDENTIFIED BY PASSWORD 'hashstring'`, true},

		// For grant statement
		{"GRANT ALL ON db1.* TO 'jeffrey'@'localhost';", true},
		{"GRANT SELECT ON db2.invoice TO 'jeffrey'@'localhost';", true},
		{"GRANT ALL ON *.* TO 'someuser'@'somehost';", true},
		{"GRANT SELECT, INSERT ON *.* TO 'someuser'@'somehost';", true},
		{"GRANT ALL ON mydb.* TO 'someuser'@'somehost';", true},
		{"GRANT SELECT, INSERT ON mydb.* TO 'someuser'@'somehost';", true},
		{"GRANT ALL ON mydb.mytbl TO 'someuser'@'somehost';", true},
		{"GRANT SELECT, INSERT ON mydb.mytbl TO 'someuser'@'somehost';", true},
		{"GRANT SELECT (col1), INSERT (col1,col2) ON mydb.mytbl TO 'someuser'@'somehost';", true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestComment(c *C) {
	table := []testCase{
		{"create table t (c int comment 'comment')", true},
		{"create table t (c int) comment = 'comment'", true},
		{"create table t (c int) comment 'comment'", true},
		{"create table t (c int) comment comment", false},
		{"create table t (comment text)", true},
		// For comment in query
		{"/*comment*/ /*comment*/ select c /* this is a comment */ from t;", true},
	}
	s.RunTest(c, table)
}
func (s *testParserSuite) TestSubquery(c *C) {
	table := []testCase{
		// For compare subquery
		{"SELECT 1 > (select 1)", true},
		{"SELECT 1 > ANY (select 1)", true},
		{"SELECT 1 > ALL (select 1)", true},
		{"SELECT 1 > SOME (select 1)", true},

		// For exists subquery
		{"SELECT EXISTS select 1", false},
		{"SELECT EXISTS (select 1)", true},
		{"SELECT + EXISTS (select 1)", true},
		{"SELECT - EXISTS (select 1)", true},
		{"SELECT NOT EXISTS (select 1)", true},
		{"SELECT + NOT EXISTS (select 1)", false},
		{"SELECT - NOT EXISTS (select 1)", false},
	}
	s.RunTest(c, table)
}
func (s *testParserSuite) TestUnion(c *C) {
	table := []testCase{
		{"select c1 from t1 union select c2 from t2", true},
		{"select c1 from t1 union (select c2 from t2)", true},
		{"select c1 from t1 union (select c2 from t2) order by c1", true},
		{"select c1 from t1 union select c2 from t2 order by c2", true},
		{"select c1 from t1 union (select c2 from t2) limit 1", true},
		{"select c1 from t1 union (select c2 from t2) limit 1, 1", true},
		{"select c1 from t1 union (select c2 from t2) order by c1 limit 1", true},
		{"(select c1 from t1) union distinct select c2 from t2", true},
		{"(select c1 from t1) union all select c2 from t2", true},
		{"(select c1 from t1) union (select c2 from t2) order by c1 union select c3 from t3", false},
		{"(select c1 from t1) union (select c2 from t2) limit 1 union select c3 from t3", false},
		{"(select c1 from t1) union select c2 from t2 union (select c3 from t3) order by c1 limit 1", true},
		{"select (select 1 union select 1) as a", true},
		{"select * from (select 1 union select 2) as a", true},
		{"insert into t select c1 from t1 union select c2 from t2", true},
		{"insert into t (c) select c1 from t1 union select c2 from t2", true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestLikeEscape(c *C) {
	table := []testCase{
		// For like escape
		{`select "abc_" like "abc\\_" escape ''`, true},
		{`select "abc_" like "abc\\_" escape '\\'`, true},
		{`select "abc_" like "abc\\_" escape '||'`, false},
		{`select "abc" like "escape" escape '+'`, true},
	}

	s.RunTest(c, table)
}
