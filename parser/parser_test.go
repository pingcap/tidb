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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testParserSuite{})

type testParserSuite struct {
}

func (s *testParserSuite) TestSimple(c *C) {
	defer testleak.AfterTest(c)()
	// Testcase for unreserved keywords
	unreservedKws := []string{
		"auto_increment", "after", "begin", "bit", "bool", "boolean", "charset", "columns", "commit",
		"date", "datetime", "deallocate", "do", "end", "engine", "engines", "execute", "first", "full",
		"local", "names", "offset", "password", "prepare", "quick", "rollback", "session", "signed",
		"start", "global", "tables", "text", "time", "timestamp", "transaction", "truncate", "unknown",
		"value", "warnings", "year", "now", "substr", "substring", "mode", "any", "some", "user", "identified",
		"collation", "comment", "avg_row_length", "checksum", "compression", "connection", "key_block_size",
		"max_rows", "min_rows", "national", "row", "quarter", "escape", "grants", "status", "fields", "triggers",
		"delay_key_write", "isolation", "repeatable", "committed", "uncommitted", "only", "serializable", "level",
		"curtime", "variables", "dayname", "version", "btree", "hash", "row_format", "dynamic", "fixed", "compressed",
		"compact", "redundant", "sql_no_cache sql_no_cache", "sql_cache sql_cache", "action", "round",
		"enable", "disable",
	}
	for _, kw := range unreservedKws {
		src := fmt.Sprintf("SELECT %s FROM tbl;", kw)
		_, err := ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))
	}

	// Testcase for prepared statement
	src := "SELECT id+?, id+? from t;"
	_, err := ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// Testcase for -- Comment and unary -- operator
	src = "CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED); -- foo\nSelect --1 from foo;"
	stmts, err := Parse(src, "", "")
	c.Assert(err, IsNil)
	c.Assert(stmts, HasLen, 2)

	// Testcase for /*! xx */
	// See: http://dev.mysql.com/doc/refman/5.7/en/comments.html
	// Fix: https://github.com/pingcap/tidb/issues/971
	src = "/*!40101 SET character_set_client = utf8 */;"
	stmts, err = Parse(src, "", "")
	c.Assert(err, IsNil)
	c.Assert(stmts, HasLen, 1)
	stmt := stmts[0]
	_, ok := stmt.(*ast.SetStmt)
	c.Assert(ok, IsTrue)

	// Testcase for CONVERT(expr,type)
	src = "SELECT CONVERT('111', SIGNED);"
	st, err := ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	ss, ok := st.(*ast.SelectStmt)
	c.Assert(ok, IsTrue)
	c.Assert(len(ss.Fields.Fields), Equals, 1)
	cv, ok := ss.Fields.Fields[0].Expr.(*ast.FuncCastExpr)
	c.Assert(ok, IsTrue)
	c.Assert(cv.FunctionType, Equals, ast.CastConvertFunction)

	// For query start with comment
	srcs := []string{
		"/* some comments */ SELECT CONVERT('111', SIGNED) ;",
		"/* some comments */ /*comment*/ SELECT CONVERT('111', SIGNED) ;",
		"SELECT /*comment*/ CONVERT('111', SIGNED) ;",
		"SELECT CONVERT('111', /*comment*/ SIGNED) ;",
		"SELECT CONVERT('111', SIGNED) /*comment*/;",
	}
	for _, src := range srcs {
		st, err = ParseOneStmt(src, "", "")
		c.Assert(err, IsNil)
		ss, ok = st.(*ast.SelectStmt)
		c.Assert(ok, IsTrue)
	}

	// For issue #961
	src = "create table t (c int key);"
	st, err = ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	cs, ok := st.(*ast.CreateTableStmt)
	c.Assert(ok, IsTrue)
	c.Assert(cs.Cols, HasLen, 1)
	c.Assert(cs.Cols[0].Options, HasLen, 1)
	c.Assert(cs.Cols[0].Options[0].Tp, Equals, ast.ColumnOptionPrimaryKey)
}

type testCase struct {
	src string
	ok  bool
}

func (s *testParserSuite) RunTest(c *C, table []testCase) {
	for _, t := range table {
		fmt.Printf("********************* src:%v \n", t.src)
		_, err := Parse(t.src, "", "")
		comment := Commentf("source %v", t.src)
		if t.ok {
			c.Assert(err, IsNil, comment)
		} else {
			c.Assert(err, NotNil, comment)
		}
	}
}
func (s *testParserSuite) TestDMLStmt(c *C) {
	defer testleak.AfterTest(c)()
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

		{"REPLACE INTO foo VALUES (1 || 2)", true},
		{"REPLACE INTO foo VALUES (1 | 2)", true},
		{"REPLACE INTO foo VALUES (false || true)", true},
		{"REPLACE INTO foo VALUES (bar(5678))", false},
		{"REPLACE INTO foo VALUES ()", true},
		{"REPLACE INTO foo (a,b) VALUES (42,314)", true},
		{"REPLACE INTO foo (a,b,) VALUES (42,314)", false},
		{"REPLACE INTO foo (a,b,) VALUES (42,314,)", false},
		{"REPLACE INTO foo () VALUES ()", true},
		{"REPLACE INTO foo VALUE ()", true},
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
		{"SET @b := 1", true},
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
		// SET TRANSACTION Syntax
		{"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", true},
		{"SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ", true},
		{"SET SESSION TRANSACTION READ WRITE", true},
		{"SET SESSION TRANSACTION READ ONLY", true},
		{"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", true},
		{"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", true},
		{"SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE", true},

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
		{"ALTER TABLE t DISABLE KEYS", true},
		{"ALTER TABLE t ENABLE KEYS", true},

		// from join
		{"SELECT * from t1, t2, t3", true},
		{"select * from t1 join t2 left join t3 on t2.id = t3.id", true},
		{"select * from t1 right join t2 on t1.id = t2.id left join t3 on t3.id = t2.id", true},
		{"select * from t1 right join t2 on t1.id = t2.id left join t3", false},

		// For show full columns
		{"show columns in t;", true},
		{"show full columns in t;", true},

		// For admin
		{"admin show ddl;", true},
		{"admin check table t1, t2;", true},

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
		{"SHOW STATUS", true},
		{"SHOW GLOBAL STATUS", true},
		{"SHOW SESSION STATUS", true},
		{"SHOW STATUS LIKE 'Up%'", true},
		{"SHOW STATUS WHERE Variable_name LIKE 'Up%'", true},
		{`SHOW FULL TABLES FROM icar_qa LIKE play_evolutions`, true},
		{`SHOW FULL TABLES WHERE Table_Type != 'VIEW'`, true},
		{`SHOW GRANTS`, true},
		{`SHOW GRANTS FOR 'test'@'localhost'`, true},
		{`SHOW COLUMNS FROM City;`, true},
		{`SHOW FIELDS FROM City;`, true},
		{`SHOW TRIGGERS LIKE 't'`, true},
		{`SHOW DATABASES LIKE 'test2'`, true},
		{`SHOW PROCEDURE STATUS WHERE Db='test'`, true},
		{`SHOW INDEX FROM t;`, true},

		// For default value
		{"CREATE TABLE sbtest (id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, k integer UNSIGNED DEFAULT '0' NOT NULL, c char(120) DEFAULT '' NOT NULL, pad char(60) DEFAULT '' NOT NULL, PRIMARY KEY  (id) )", true},
		{"create table test (create_date TIMESTAMP NOT NULL COMMENT '创建日期 create date' DEFAULT now());", true},

		// For truncate statement
		{"TRUNCATE TABLE t1", true},
		{"TRUNCATE t1", true},

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

		// For https://github.com/pingcap/tidb/issues/1050
		{`SELECT /*!40001 SQL_NO_CACHE */ * FROM test WHERE 1 limit 0, 2000;`, true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestExpression(c *C) {
	defer testleak.AfterTest(c)()
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
		{`select _utf8"string";`, true},
		// For comparison
		{"select 1 <=> 0, 1 <=> null, 1 = null", true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestBuiltin(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		// For buildin functions
		{"SELECT POW(1, 2)", true},
		{"SELECT POW(1, 0.5)", true},
		{"SELECT POW(1, -1)", true},
		{"SELECT POW(-1, 1)", true},
		{"SELECT RAND();", true},
		{"SELECT RAND(1);", true},
		{"SELECT MOD(10, 2);", true},
		{"SELECT ROUND(-1.23);", true},
		{"SELECT ROUND(1.23, 1);", true},

		{"SELECT SUBSTR('Quadratically',5);", true},
		{"SELECT SUBSTR('Quadratically',5, 3);", true},
		{"SELECT SUBSTR('Quadratically' FROM 5);", true},
		{"SELECT SUBSTR('Quadratically' FROM 5 FOR 3);", true},

		{"SELECT SUBSTRING('Quadratically',5);", true},
		{"SELECT SUBSTRING('Quadratically',5, 3);", true},
		{"SELECT SUBSTRING('Quadratically' FROM 5);", true},
		{"SELECT SUBSTRING('Quadratically' FROM 5 FOR 3);", true},

		{"SELECT CONVERT('111', SIGNED);", true},

		// Information Functions
		{"SELECT DATABASE();", true},
		{"SELECT USER();", true},
		{"SELECT CURRENT_USER();", true},
		{"SELECT CURRENT_USER;", true},
		{"SELECT CONNECTION_ID();", true},
		{"SELECT VERSION();", true},

		{"SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);", true},
		{"SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2);", true},

		{`SELECT ASCII(""), ASCII("A"), ASCII(1);`, true},

		{`SELECT LOWER("A"), UPPER("a")`, true},
		{`SELECT LCASE("A"), UCASE("a")`, true},

		{`SELECT REPLACE('www.mysql.com', 'w', 'Ww')`, true},

		{`SELECT LOCATE('bar', 'foobarbar');`, true},
		{`SELECT LOCATE('bar', 'foobarbar', 5);`, true},

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

		// Select current_time
		{"select current_time", true},
		{"select current_time()", true},
		{"select current_time(6)", true},
		{"select curtime()", true},
		{"select curtime(6)", true},

		// for microsecond, second, minute, hour
		{"SELECT MICROSECOND('2009-12-31 23:59:59.000010');", true},
		{"SELECT SECOND('10:05:03');", true},
		{"SELECT MINUTE('2008-02-03 10:05:03');", true},
		{"SELECT HOUR('10:05:03');", true},

		// for date, day, weekday
		{"SELECT DATE('2003-12-31 01:02:03');", true},
		{"SELECT CURRENT_DATE, CURRENT_DATE(), CURDATE()", true},
		{"SELECT DAY('2007-02-03');", true},
		{"SELECT DAYOFMONTH('2007-02-03');", true},
		{"SELECT DAYOFWEEK('2007-02-03');", true},
		{"SELECT DAYOFYEAR('2007-02-03');", true},
		{"SELECT DAYNAME('2007-02-03');", true},
		{"SELECT WEEKDAY('2007-02-03');", true},

		// For utc_date
		{"SELECT UTC_DATE, UTC_DATE();", true},

		// for week, month, year
		{"SELECT WEEK('2007-02-03');", true},
		{"SELECT WEEK('2007-02-03', 0);", true},
		{"SELECT WEEKOFYEAR('2007-02-03');", true},
		{"SELECT MONTH('2007-02-03');", true},
		{"SELECT YEAR('2007-02-03');", true},
		{"SELECT YEARWEEK('2007-02-03');", true},
		{"SELECT YEARWEEK('2007-02-03', 0);", true},

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

		// For string functions
		// Trim
		{`SELECT TRIM('  bar   ');`, true},
		{`SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');`, true},
		{`SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx');`, true},
		{`SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');`, true},
		{`SELECT LTRIM(' foo ');`, true},
		{`SELECT RTRIM(' bar ');`, true},

		// Repeat
		{`SELECT REPEAT("a", 10);`, true},

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

		// For adddate
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 second)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 minute)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 hour)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 day)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 week)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 month)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 quarter)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 year)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", 10)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", 0.10)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", "11,11")`, true},

		// For date_sub
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 second)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 minute)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 hour)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 day)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 week)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 month)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 quarter)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 year)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true},

		// For subdate
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 second)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 minute)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 hour)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 day)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 week)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 month)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 quarter)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 year)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", 10)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", 0.10)`, true},
		{`select adddate("2011-11-11 10:10:10.123456", "11,11")`, true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestIdentifier(c *C) {
	defer testleak.AfterTest(c)()
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
	defer testleak.AfterTest(c)()
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
		{"create table t (c int) DELAY_KEY_WRITE=1", true},
		{"create table t (c int) DELAY_KEY_WRITE 1", true},
		{"create table t (c int) ROW_FORMAT = default", true},
		{"create table t (c int) ROW_FORMAT default", true},
		{"create table t (c int) ROW_FORMAT = fixed", true},
		{"create table t (c int) ROW_FORMAT = compressed", true},
		{"create table t (c int) ROW_FORMAT = compact", true},
		{"create table t (c int) ROW_FORMAT = redundant", true},
		{"create table t (c int) ROW_FORMAT = dynamic", true},
		// For check clause
		{"create table t (c1 bool, c2 bool, check (c1 in (0, 1)), check (c2 in (0, 1)))", true},
		{"CREATE TABLE Customer (SD integer CHECK (SD > 0), First_Name varchar(30));", true},

		{"create database xxx", true},
		{"create database if exists xxx", false},
		{"create database if not exists xxx", true},
		{"create schema xxx", true},
		{"create schema if exists xxx", false},
		{"create schema if not exists xxx", true},
		// For drop datbase/schema/table
		{"drop database xxx", true},
		{"drop database if exists xxx", true},
		{"drop database if not exists xxx", false},
		{"drop schema xxx", true},
		{"drop schema if exists xxx", true},
		{"drop schema if not exists xxx", false},
		{"drop table xxx", true},
		{"drop table xxx, yyy", true},
		{"drop tables xxx", true},
		{"drop tables xxx, yyy", true},
		{"drop table if exists xxx", true},
		{"drop table if not exists xxx", false},
		// For issue 974
		{`CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(128) NOT NULL,
		address_detail varchar(128) NOT NULL,
		cellphone varchar(16) NOT NULL,
		latitude double NOT NULL,
		longitude double NOT NULL,
		name varchar(16) NOT NULL,
		sex tinyint(1) NOT NULL,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true},
		// For issue 975
		{`CREATE TABLE test_data (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(255) NOT NULL,
		amount decimal(19,2) DEFAULT NULL,
		charge_id varchar(32) DEFAULT NULL,
		paid_amount decimal(19,2) DEFAULT NULL,
		transaction_no varchar(64) DEFAULT NULL,
		wx_mp_app_id varchar(32) DEFAULT NULL,
		contacts varchar(50) DEFAULT NULL,
		deliver_fee decimal(19,2) DEFAULT NULL,
		deliver_info varchar(255) DEFAULT NULL,
		deliver_time varchar(255) DEFAULT NULL,
		description varchar(255) DEFAULT NULL,
		invoice varchar(255) DEFAULT NULL,
		order_from int(11) DEFAULT NULL,
		order_state int(11) NOT NULL,
		packing_fee decimal(19,2) DEFAULT NULL,
		payment_time datetime DEFAULT NULL,
		payment_type int(11) DEFAULT NULL,
		phone varchar(50) NOT NULL,
		store_employee_id bigint(20) DEFAULT NULL,
		store_id bigint(20) NOT NULL,
		user_id bigint(20) NOT NULL,
		payment_mode int(11) NOT NULL,
		current_latitude double NOT NULL,
		current_longitude double NOT NULL,
		address_latitude double NOT NULL,
		address_longitude double NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT food_order_ibfk_1 FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		CONSTRAINT food_order_ibfk_2 FOREIGN KEY (store_id) REFERENCES waimaiqa.store (id),
		CONSTRAINT food_order_ibfk_3 FOREIGN KEY (store_employee_id) REFERENCES waimaiqa.store_employee (id),
		UNIQUE FK_UNIQUE_charge_id USING BTREE (charge_id) comment '',
		INDEX FK_eqst2x1xisn3o0wbrlahnnqq8 USING BTREE (store_employee_id) comment '',
		INDEX FK_8jcmec4kb03f4dod0uqwm54o9 USING BTREE (store_id) comment '',
		INDEX FK_a3t0m9apja9jmrn60uab30pqd USING BTREE (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=95 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true},
		{`create table t (c int KEY);`, true},
		{`CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		create_at datetime NOT NULL,
		deleted tinyint(1) NOT NULL,
		update_at datetime NOT NULL,
		version bigint(20) DEFAULT NULL,
		address varchar(128) NOT NULL,
		address_detail varchar(128) NOT NULL,
		cellphone varchar(16) NOT NULL,
		latitude double NOT NULL,
		longitude double NOT NULL,
		name varchar(16) NOT NULL,
		sex tinyint(1) NOT NULL,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id) ON DELETE CASCADE ON UPDATE NO ACTION,
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestType(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		// For time fsp
		{"CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true},

		// For hexadecimal
		{"SELECT x'0a', X'11', 0x11", true},
		{"select x'0xaa'", false},
		{"select 0X11", false},
		{"select 0x4920616D2061206C6F6E672068657820737472696E67", true},

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
	defer testleak.AfterTest(c)()
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
	defer testleak.AfterTest(c)()
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
	defer testleak.AfterTest(c)()
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
	defer testleak.AfterTest(c)()
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
	defer testleak.AfterTest(c)()
	table := []testCase{
		// For like escape
		{`select "abc_" like "abc\\_" escape ''`, true},
		{`select "abc_" like "abc\\_" escape '\\'`, true},
		{`select "abc_" like "abc\\_" escape '||'`, false},
		{`select "abc" like "escape" escape '+'`, true},
	}

	s.RunTest(c, table)
}

func (s *testParserSuite) TestMysqlDump(c *C) {
	defer testleak.AfterTest(c)()
	// Statements used by mysqldump.
	table := []testCase{
		{`UNLOCK TABLES;`, true},
		{`LOCK TABLES t1 READ;`, true},
		{`show table status like 't'`, true},
		{`LOCK TABLES t2 WRITE`, true},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestIndexHint(c *C) {
	defer testleak.AfterTest(c)()
	table := []testCase{
		{`select * from t use index ();`, true},
		{`select * from t use index (idx);`, true},
		{`select * from t use index (idx1, idx2);`, true},
		{`select * from t ignore key (idx1)`, true},
		{`select * from t force index for join (idx1)`, true},
		{`select * from t use index for order by (idx1)`, true},
		{`select * from t force index for group by (idx1)`, true},
		{`select * from t use index for group by (idx1) use index for order by (idx2), t2`, true},
	}
	s.RunTest(c, table)
}
