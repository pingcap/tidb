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
	"runtime"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testParserSuite{})

type testParserSuite struct {
	enableWindowFunc bool
}

func (s *testParserSuite) TestSimple(c *C) {
	parser := New()

	reservedKws := []string{
		"add", "all", "alter", "analyze", "and", "as", "asc", "between", "bigint",
		"binary", "blob", "both", "by", "cascade", "case", "change", "character", "check", "collate",
		"column", "constraint", "convert", "create", "cross", "current_date", "current_time",
		"current_timestamp", "current_user", "database", "databases", "day_hour", "day_microsecond",
		"day_minute", "day_second", "decimal", "default", "delete", "desc", "describe",
		"distinct", "distinctRow", "div", "double", "drop", "dual", "else", "enclosed", "escaped",
		"exists", "explain", "false", "float", "for", "force", "foreign", "from",
		"fulltext", "grant", "group", "having", "hour_microsecond", "hour_minute",
		"hour_second", "if", "ignore", "in", "index", "infile", "inner", "insert", "int", "into", "integer",
		"interval", "is", "join", "key", "keys", "kill", "leading", "left", "like", "limit", "lines", "load",
		"localtime", "localtimestamp", "lock", "longblob", "longtext", "mediumblob", "maxvalue", "mediumint", "mediumtext",
		"minute_microsecond", "minute_second", "mod", "not", "no_write_to_binlog", "null", "numeric",
		"on", "option", "or", "order", "outer", "partition", "precision", "primary", "procedure", "range", "read", "real",
		"references", "regexp", "rename", "repeat", "replace", "revoke", "restrict", "right", "rlike",
		"schema", "schemas", "second_microsecond", "select", "set", "show", "smallint",
		"starting", "table", "terminated", "then", "tinyblob", "tinyint", "tinytext", "to",
		"trailing", "true", "union", "unique", "unlock", "unsigned",
		"update", "use", "using", "utc_date", "values", "varbinary", "varchar",
		"when", "where", "write", "xor", "year_month", "zerofill",
		"generated", "virtual", "stored", "usage",
		"delayed", "high_priority", "low_priority",
		"cumeDist", "denseRank", "firstValue", "lag", "lastValue", "lead", "nthValue", "ntile",
		"over", "percentRank", "rank", "row", "rows", "rowNumber", "window",
		// TODO: support the following keywords
		// "with",
	}
	for _, kw := range reservedKws {
		src := fmt.Sprintf("SELECT * FROM db.%s;", kw)
		_, err := parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))

		src = fmt.Sprintf("SELECT * FROM %s.desc", kw)
		_, err = parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))

		src = fmt.Sprintf("SELECT t.%s FROM t", kw)
		_, err = parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))
	}

	// Testcase for unreserved keywords
	unreservedKws := []string{
		"auto_increment", "after", "begin", "bit", "bool", "boolean", "charset", "columns", "commit",
		"date", "datediff", "datetime", "deallocate", "do", "from_days", "end", "engine", "engines", "execute", "first", "full",
		"local", "names", "offset", "password", "prepare", "quick", "rollback", "session", "signed",
		"start", "global", "tables", "tablespace", "text", "time", "timestamp", "tidb", "transaction", "truncate", "unknown",
		"value", "warnings", "year", "now", "substr", "subpartition", "subpartitions", "substring", "mode", "any", "some", "user", "identified",
		"collation", "comment", "avg_row_length", "checksum", "compression", "connection", "key_block_size",
		"max_rows", "min_rows", "national", "quarter", "escape", "grants", "status", "fields", "triggers",
		"delay_key_write", "isolation", "partitions", "repeatable", "committed", "uncommitted", "only", "serializable", "level",
		"curtime", "variables", "dayname", "version", "btree", "hash", "row_format", "dynamic", "fixed", "compressed",
		"compact", "redundant", "sql_no_cache sql_no_cache", "sql_cache sql_cache", "action", "round",
		"enable", "disable", "reverse", "space", "privileges", "get_lock", "release_lock", "sleep", "no", "greatest", "least",
		"binlog", "hex", "unhex", "function", "indexes", "from_unixtime", "processlist", "events", "less", "than", "timediff",
		"ln", "log", "log2", "log10", "timestampdiff", "pi", "quote", "none", "super", "shared", "exclusive",
		"always", "stats", "stats_meta", "stats_histogram", "stats_buckets", "stats_healthy", "tidb_version", "replication", "slave", "client",
		"max_connections_per_hour", "max_queries_per_hour", "max_updates_per_hour", "max_user_connections", "event", "reload", "routine", "temporary",
		"following", "preceding", "unbounded", "respect", "nulls", "current", "last",
	}
	for _, kw := range unreservedKws {
		src := fmt.Sprintf("SELECT %s FROM tbl;", kw)
		_, err := parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil, Commentf("source %s", src))
	}

	// Testcase for prepared statement
	src := "SELECT id+?, id+? from t;"
	_, err := parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// Testcase for -- Comment and unary -- operator
	src = "CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED); -- foo\nSelect --1 from foo;"
	stmts, err := parser.Parse(src, "", "")
	c.Assert(err, IsNil)
	c.Assert(stmts, HasLen, 2)

	// Testcase for /*! xx */
	// See http://dev.mysql.com/doc/refman/5.7/en/comments.html
	// Fix: https://github.com/pingcap/tidb/issues/971
	src = "/*!40101 SET character_set_client = utf8 */;"
	stmts, err = parser.Parse(src, "", "")
	c.Assert(err, IsNil)
	c.Assert(stmts, HasLen, 1)
	stmt := stmts[0]
	_, ok := stmt.(*ast.SetStmt)
	c.Assert(ok, IsTrue)

	// for issue #2017
	src = "insert into blobtable (a) values ('/*! truncated */');"
	stmt, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	is, ok := stmt.(*ast.InsertStmt)
	c.Assert(ok, IsTrue)
	c.Assert(is.Lists, HasLen, 1)
	c.Assert(is.Lists[0], HasLen, 1)
	c.Assert(is.Lists[0][0].(ast.ValueExpr).GetDatumString(), Equals, "/*! truncated */")

	// Testcase for CONVERT(expr,type)
	src = "SELECT CONVERT('111', SIGNED);"
	st, err := parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	ss, ok := st.(*ast.SelectStmt)
	c.Assert(ok, IsTrue)
	c.Assert(len(ss.Fields.Fields), Equals, 1)
	cv, ok := ss.Fields.Fields[0].Expr.(*ast.FuncCastExpr)
	c.Assert(ok, IsTrue)
	c.Assert(cv.FunctionType, Equals, ast.CastConvertFunction)

	// for query start with comment
	srcs := []string{
		"/* some comments */ SELECT CONVERT('111', SIGNED) ;",
		"/* some comments */ /*comment*/ SELECT CONVERT('111', SIGNED) ;",
		"SELECT /*comment*/ CONVERT('111', SIGNED) ;",
		"SELECT CONVERT('111', /*comment*/ SIGNED) ;",
		"SELECT CONVERT('111', SIGNED) /*comment*/;",
	}
	for _, src := range srcs {
		st, err = parser.ParseOneStmt(src, "", "")
		c.Assert(err, IsNil)
		ss, ok = st.(*ast.SelectStmt)
		c.Assert(ok, IsTrue)
	}

	// for issue #961
	src = "create table t (c int key);"
	st, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	cs, ok := st.(*ast.CreateTableStmt)
	c.Assert(ok, IsTrue)
	c.Assert(cs.Cols, HasLen, 1)
	c.Assert(cs.Cols[0].Options, HasLen, 1)
	c.Assert(cs.Cols[0].Options[0].Tp, Equals, ast.ColumnOptionPrimaryKey)

	// for issue #4497
	src = "create table t1(a NVARCHAR(100));"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// for issue 2803
	src = "use quote;"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// issue #4354
	src = "select b'';"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	src = "select B'';"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// src = "select 0b'';"
	// _, err = parser.ParseOneStmt(src, "", "")
	// c.Assert(err, NotNil)

	// for #4909, support numericType `signed` filedOpt.
	src = "CREATE TABLE t(_sms smallint signed, _smu smallint unsigned);"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	// for #7371, support NATIONAL CHARACTER
	// reference link: https://dev.mysql.com/doc/refman/5.7/en/charset-national.html
	src = "CREATE TABLE t(c1 NATIONAL CHARACTER(10));"
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)

	src = `CREATE TABLE t(a tinyint signed,
		b smallint signed,
		c mediumint signed,
		d int signed,
		e int1 signed,
		f int2 signed,
		g int3 signed,
		h int4 signed,
		i int8 signed,
		j integer signed,
		k bigint signed,
		l bool signed,
		m boolean signed
		);`

	st, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	ct, ok := st.(*ast.CreateTableStmt)
	c.Assert(ok, IsTrue)
	for _, col := range ct.Cols {
		c.Assert(col.Tp.Flag&mysql.UnsignedFlag, Equals, uint(0))
	}

	// for issue #4006
	src = `insert into tb(v) (select v from tb);`
	_, err = parser.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
}

type testCase struct {
	src     string
	ok      bool
	restore string
}

type testErrMsgCase struct {
	src string
	ok  bool
	err error
}

func (s *testParserSuite) RunTest(c *C, table []testCase) {
	parser := New()
	parser.EnableWindowFunc(s.enableWindowFunc)
	for _, t := range table {
		_, err := parser.Parse(t.src, "", "")
		comment := Commentf("source %v", t.src)
		if !t.ok {
			c.Assert(err, NotNil, comment)
			continue
		}
		c.Assert(err, IsNil, comment)
		// restore correctness test
		if t.ok && t.restore != "" {
			s.RunRestoreTest(c, t.src, t.restore)
		}
	}
}

func (s *testParserSuite) RunRestoreTest(c *C, sourceSQLs, expectSQLs string) {
	var sb strings.Builder
	parser := New()
	comment := Commentf("source %v", sourceSQLs)
	stmts, err := parser.Parse(sourceSQLs, "", "")
	c.Assert(err, IsNil, comment)
	restoreSQLs := ""
	for _, stmt := range stmts {
		err = stmt.Restore(&sb)
		c.Assert(err, IsNil, comment)
		restoreSQL := sb.String()
		comment = Commentf("source %v; restore %v", sourceSQLs, restoreSQL)
		restoreStmt, err := parser.ParseOneStmt(restoreSQL, "", "")
		c.Assert(err, IsNil, comment)
		CleanNodeText(stmt)
		CleanNodeText(restoreStmt)
		c.Assert(restoreStmt, DeepEquals, stmt, comment)
		if restoreSQLs != "" {
			restoreSQLs += "; "
		}
		restoreSQLs += restoreSQL
	}
	comment = Commentf("restore %v; expect %v", restoreSQLs, expectSQLs)
	c.Assert(restoreSQLs, Equals, expectSQLs, comment)
}

func (s *testParserSuite) RunErrMsgTest(c *C, table []testErrMsgCase) {
	parser := New()
	for _, t := range table {
		_, err := parser.Parse(t.src, "", "")
		comment := Commentf("source %v", t.src)
		if t.err != nil {
			c.Assert(terror.ErrorEqual(err, t.err), IsTrue, comment)
		} else {
			c.Assert(err, IsNil, comment)
		}
	}
}

func (s *testParserSuite) TestDMLStmt(c *C) {
	table := []testCase{
		{"", true, ""},
		{";", true, ""},
		{"INSERT INTO foo VALUES (1234)", true, ""},
		{"INSERT INTO foo VALUES (1234, 5678)", true, ""},
		{"INSERT INTO t1 (SELECT * FROM t2)", true, ""},
		// 15
		{"INSERT INTO foo VALUES (1 || 2)", true, ""},
		{"INSERT INTO foo VALUES (1 | 2)", true, ""},
		{"INSERT INTO foo VALUES (false || true)", true, ""},
		{"INSERT INTO foo VALUES (bar(5678))", true, ""},
		// 20
		{"INSERT INTO foo VALUES ()", true, ""},
		{"SELECT * FROM t", true, ""},
		{"SELECT * FROM t AS u", true, ""},
		// 25
		{"SELECT * FROM t, v", true, ""},
		{"SELECT * FROM t AS u, v", true, ""},
		{"SELECT * FROM t, v AS w", true, ""},
		{"SELECT * FROM t AS u, v AS w", true, ""},
		{"SELECT * FROM foo, bar, foo", true, ""},
		// 30
		{"SELECT DISTINCTS * FROM t", false, ""},
		{"SELECT DISTINCT * FROM t", true, ""},
		{"SELECT DISTINCTROW * FROM t", true, ""},
		{"SELECT ALL * FROM t", true, ""},
		{"SELECT DISTINCT ALL * FROM t", false, ""},
		{"SELECT DISTINCTROW ALL * FROM t", false, ""},
		{"INSERT INTO foo (a) VALUES (42)", true, ""},
		{"INSERT INTO foo (a,) VALUES (42,)", false, ""},
		// 35
		{"INSERT INTO foo (a,b) VALUES (42,314)", true, ""},
		{"INSERT INTO foo (a,b,) VALUES (42,314)", false, ""},
		{"INSERT INTO foo (a,b,) VALUES (42,314,)", false, ""},
		{"INSERT INTO foo () VALUES ()", true, ""},
		{"INSERT INTO foo VALUE ()", true, ""},

		// for issue 2402
		{"INSERT INTO tt VALUES (01000001783);", true, ""},
		{"INSERT INTO tt VALUES (default);", true, ""},

		{"REPLACE INTO foo VALUES (1 || 2)", true, ""},
		{"REPLACE INTO foo VALUES (1 | 2)", true, ""},
		{"REPLACE INTO foo VALUES (false || true)", true, ""},
		{"REPLACE INTO foo VALUES (bar(5678))", true, ""},
		{"REPLACE INTO foo VALUES ()", true, ""},
		{"REPLACE INTO foo (a,b) VALUES (42,314)", true, ""},
		{"REPLACE INTO foo (a,b,) VALUES (42,314)", false, ""},
		{"REPLACE INTO foo (a,b,) VALUES (42,314,)", false, ""},
		{"REPLACE INTO foo () VALUES ()", true, ""},
		{"REPLACE INTO foo VALUE ()", true, ""},
		// 40
		{`SELECT stuff.id
			FROM stuff
			WHERE stuff.value >= ALL (SELECT stuff.value
			FROM stuff)`, true, ""},
		{"BEGIN", true, ""},
		{"START TRANSACTION", true, ""},
		// 45
		{"COMMIT", true, ""},
		{"ROLLBACK", true, ""},
		{`BEGIN;
			INSERT INTO foo VALUES (42, 3.14);
			INSERT INTO foo VALUES (-1, 2.78);
		COMMIT;`, true, ""},
		{`BEGIN;
			INSERT INTO tmp SELECT * from bar;
			SELECT * from tmp;
		ROLLBACK;`, true, ""},

		// qualified select
		{"SELECT a.b.c FROM t", true, ""},
		{"SELECT a.b.*.c FROM t", false, ""},
		{"SELECT a.b.* FROM t", true, ""},
		{"SELECT a FROM t", true, ""},
		{"SELECT a.b.c.d FROM t", false, ""},

		// do statement
		{"DO 1", true, ""},
		{"DO 1 from t", false, ""},

		// load data
		{"load data infile '/tmp/t.csv' into table t", true, ""},
		{"load data infile '/tmp/t.csv' into table t character set utf8", true, ""},
		{"load data infile '/tmp/t.csv' into table t fields terminated by 'ab'", true, ""},
		{"load data infile '/tmp/t.csv' into table t columns terminated by 'ab'", true, ""},
		{"load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b'", true, ""},
		{"load data infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, ""},
		{"load data infile '/tmp/t.csv' into table t lines starting by 'ab'", true, ""},
		{"load data infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy'", true, ""},
		{"load data infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy'", true, ""},
		{"load data infile '/tmp/t.csv' into table t terminated by 'xy' fields terminated by 'ab'", false, ""},
		{"load data local infile '/tmp/t.csv' into table t", true, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab'", true, ""},
		{"load data local infile '/tmp/t.csv' into table t columns terminated by 'ab'", true, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b'", true, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, ""},
		{"load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' enclosed by 'b' escaped by '*'", true, ""},
		{"load data local infile '/tmp/t.csv' into table t lines starting by 'ab'", true, ""},
		{"load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy'", true, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy'", true, ""},
		{"load data local infile '/tmp/t.csv' into table t terminated by 'xy' fields terminated by 'ab'", false, ""},
		{"load data infile '/tmp/t.csv' into table t (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t columns terminated by 'ab' (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*' (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' enclosed by 'b' escaped by '*' (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t lines starting by 'ab' (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy' (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t character set utf8 fields terminated by 'ab' lines terminated by 'xy' (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' lines terminated by 'xy' (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t (a,b) fields terminated by 'ab'", false, ""},
		{"load data local infile '/tmp/t.csv' into table t ignore 1 lines", true, ""},
		{"load data local infile '/tmp/t.csv' into table t ignore -1 lines", false, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' (a,b) ignore 1 lines", false, ""},
		{"load data local infile '/tmp/t.csv' into table t lines starting by 'ab' terminated by 'xy' ignore 1 lines", true, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by '*' ignore 1 lines (a,b)", true, ""},
		{"load data local infile '/tmp/t.csv' into table t fields terminated by 'ab' enclosed by 'b' escaped by ''", true, ""},

		// select for update
		{"SELECT * from t for update", true, ""},
		{"SELECT * from t lock in share mode", true, ""},

		// from join
		{"SELECT * from t1, t2, t3", true, ""},
		{"select * from t1 join t2 left join t3 on t2.id = t3.id", true, ""},
		{"select * from t1 right join t2 on t1.id = t2.id left join t3 on t3.id = t2.id", true, ""},
		{"select * from t1 right join t2 on t1.id = t2.id left join t3", false, ""},
		{"select * from t1 join t2 left join t3 using (id)", true, ""},
		{"select * from t1 right join t2 using (id) left join t3 using (id)", true, ""},
		{"select * from t1 right join t2 using (id) left join t3", false, ""},
		{"select * from t1 natural join t2", true, ""},
		{"select * from t1 natural right join t2", true, ""},
		{"select * from t1 natural left outer join t2", true, ""},
		{"select * from t1 natural inner join t2", false, ""},
		{"select * from t1 natural cross join t2", false, ""},

		// for straight_join
		{"select * from t1 straight_join t2 on t1.id = t2.id", true, ""},
		{"select straight_join * from t1 join t2 on t1.id = t2.id", true, ""},
		{"select straight_join * from t1 left join t2 on t1.id = t2.id", true, ""},
		{"select straight_join * from t1 right join t2 on t1.id = t2.id", true, ""},
		{"select straight_join * from t1 straight_join t2 on t1.id = t2.id", true, ""},

		// for "USE INDEX" in delete statement
		{"DELETE FROM t1 USE INDEX(idx_a) WHERE t1.id=1;", true, ""},
		{"DELETE t1, t2 FROM t1 USE INDEX(idx_a) JOIN t2 WHERE t1.id=t2.id;", true, ""},
		{"DELETE t1, t2 FROM t1 USE INDEX(idx_a) JOIN t2 USE INDEX(idx_a) WHERE t1.id=t2.id;", true, ""},

		// for admin
		{"admin show ddl;", true, ""},
		{"admin show ddl jobs;", true, ""},
		{"admin show ddl jobs 20;", true, ""},
		{"admin show ddl jobs -1;", false, ""},
		{"admin show ddl job queries 1", true, ""},
		{"admin show ddl job queries 1, 2, 3, 4", true, ""},
		{"admin show t1 next_row_id", true, ""},
		{"admin check table t1, t2;", true, ""},
		{"admin check index tableName idxName;", true, ""},
		{"admin check index tableName idxName (1, 2), (4, 5);", true, ""},
		{"admin checksum table t1, t2;", true, ""},
		{"admin cancel ddl jobs 1", true, ""},
		{"admin cancel ddl jobs 1, 2", true, ""},
		{"admin recover index t1 idx_a", true, ""},
		{"admin cleanup index t1 idx_a", true, ""},
		{"admin show slow top 3", true, ""},
		{"admin show slow top internal 7", true, ""},
		{"admin show slow top all 9", true, ""},
		{"admin show slow recent 11", true, ""},

		// for on duplicate key update
		{"INSERT INTO t (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b);", true, ""},
		{"INSERT IGNORE INTO t (a,b,c) VALUES (1,2,3),(4,5,6) ON DUPLICATE KEY UPDATE c=VALUES(a)+VALUES(b);", true, ""},

		// for delete statement
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;", true, ""},
		{"DELETE FROM t1, t2 USING t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id;", true, ""},
		{"DELETE t1, t2 FROM t1 INNER JOIN t2 INNER JOIN t3 WHERE t1.id=t2.id AND t2.id=t3.id limit 10;", false, ""},
		{"DELETE /*+ TiDB_INLJ(t1, t2) */ t1, t2 from t1, t2 where t1.id=t2.id;", true, ""},
		{"DELETE /*+ TiDB_HJ(t1, t2) */ t1, t2 from t1, t2 where t1.id=t2.id", true, ""},

		// for update statement
		{"UPDATE t SET id = id + 1 ORDER BY id DESC;", true, ""},
		{"UPDATE items,month SET items.price=month.price WHERE items.id=month.id;", true, ""},
		{"UPDATE items,month SET items.price=month.price WHERE items.id=month.id LIMIT 10;", false, ""},
		{"UPDATE user T0 LEFT OUTER JOIN user_profile T1 ON T1.id = T0.profile_id SET T0.profile_id = 1 WHERE T0.profile_id IN (1);", true, ""},
		{"UPDATE /*+ TiDB_INLJ(t1, t2) */ t1, t2 set t1.profile_id = 1, t2.profile_id = 1 where ta.a=t.ba", true, ""},
		{"UPDATE /*+ TiDB_SMJ(t1, t2) */ t1, t2 set t1.profile_id = 1, t2.profile_id = 1 where ta.a=t.ba", true, ""},

		// for select with where clause
		{"SELECT * FROM t WHERE 1 = 1", true, ""},

		// for dual
		{"select 1 from dual", true, ""},
		{"select 1 from dual limit 1", true, ""},
		{"select 1 where exists (select 2)", false, ""},
		{"select 1 from dual where not exists (select 2)", true, ""},
		{"select 1 as a from dual order by a", true, ""},
		{"select 1 as a from dual where 1 < any (select 2) order by a", true, ""},
		{"select 1 order by 1", true, ""},

		// for https://github.com/pingcap/tidb/issues/320
		{`(select 1);`, true, ""},

		// for https://github.com/pingcap/tidb/issues/1050
		{`SELECT /*!40001 SQL_NO_CACHE */ * FROM test WHERE 1 limit 0, 2000;`, true, ""},

		{`ANALYZE TABLE t`, true, ""},

		// for comments
		{`/** 20180417 **/ show databases;`, true, ""},
		{`/* 20180417 **/ show databases;`, true, ""},
		{`/** 20180417 */ show databases;`, true, ""},
		{`/** 20180417 ******/ show databases;`, true, ""},

		// for Binlog stmt
		{`BINLOG '
BxSFVw8JAAAA8QAAAPUAAAAAAAQANS41LjQ0LU1hcmlhREItbG9nAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAEzgNAAgAEgAEBAQEEgAA2QAEGggAAAAICAgCAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
AAAAAAAAAAAA5gm5Mg==
'/*!*/;`, true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestDBAStmt(c *C) {
	table := []testCase{
		// for SHOW statement
		{"SHOW VARIABLES LIKE 'character_set_results'", true, ""},
		{"SHOW GLOBAL VARIABLES LIKE 'character_set_results'", true, ""},
		{"SHOW SESSION VARIABLES LIKE 'character_set_results'", true, ""},
		{"SHOW VARIABLES", true, ""},
		{"SHOW GLOBAL VARIABLES", true, ""},
		{"SHOW GLOBAL VARIABLES WHERE Variable_name = 'autocommit'", true, ""},
		{"SHOW STATUS", true, ""},
		{"SHOW GLOBAL STATUS", true, ""},
		{"SHOW SESSION STATUS", true, ""},
		{`SHOW STATUS LIKE 'Up%'`, true, ""},
		{`SHOW STATUS WHERE Variable_name LIKE 'Up%'`, true, ""},
		{`SHOW FULL TABLES FROM icar_qa LIKE play_evolutions`, true, ""},
		{`SHOW FULL TABLES WHERE Table_Type != 'VIEW'`, true, ""},
		{`SHOW GRANTS`, true, ""},
		{`SHOW GRANTS FOR 'test'@'localhost'`, true, ""},
		{`SHOW GRANTS FOR current_user()`, true, ""},
		{`SHOW GRANTS FOR current_user`, true, ""},
		{`SHOW COLUMNS FROM City;`, true, ""},
		{`SHOW COLUMNS FROM tv189.1_t_1_x;`, true, ""},
		{`SHOW FIELDS FROM City;`, true, ""},
		{`SHOW TRIGGERS LIKE 't'`, true, ""},
		{`SHOW DATABASES LIKE 'test2'`, true, ""},
		{`SHOW PROCEDURE STATUS WHERE Db='test'`, true, ""},
		{`SHOW FUNCTION STATUS WHERE Db='test'`, true, ""},
		{`SHOW INDEX FROM t;`, true, ""},
		{`SHOW KEYS FROM t;`, true, ""},
		{`SHOW INDEX IN t;`, true, ""},
		{`SHOW KEYS IN t;`, true, ""},
		{`SHOW INDEXES IN t where true;`, true, ""},
		{`SHOW KEYS FROM t FROM test where true;`, true, ""},
		{`SHOW EVENTS FROM test_db WHERE definer = 'current_user'`, true, ""},
		{`SHOW PLUGINS`, true, ""},
		{`SHOW PROFILES`, true, ""},
		{`SHOW MASTER STATUS`, true, ""},
		{`SHOW PRIVILEGES`, true, ""},
		// for show character set
		{"show character set;", true, ""},
		{"show charset", true, ""},
		// for show collation
		{"show collation", true, ""},
		{`show collation like 'utf8%'`, true, ""},
		{"show collation where Charset = 'utf8' and Collation = 'utf8_bin'", true, ""},
		// for show full columns
		{"show columns in t;", true, ""},
		{"show full columns in t;", true, ""},
		// for show create table
		{"show create table test.t", true, ""},
		{"show create table t", true, ""},
		// for show stats_meta.
		{"show stats_meta", true, ""},
		{"show stats_meta where table_name = 't'", true, ""},
		// for show stats_histograms
		{"show stats_histograms", true, ""},
		{"show stats_histograms where col_name = 'a'", true, ""},
		// for show stats_buckets
		{"show stats_buckets", true, ""},
		{"show stats_buckets where col_name = 'a'", true, ""},
		// for show stats_healthy.
		{"show stats_healthy", true, ""},
		{"show stats_healthy where table_name = 't'", true, ""},

		// for load stats
		{"load stats '/tmp/stats.json'", true, ""},
		// set
		// user defined
		{"SET @ = 1", true, ""},
		{"SET @' ' = 1", true, ""},
		{"SET @! = 1", false, ""},
		{"SET @1 = 1", true, ""},
		{"SET @a = 1", true, ""},
		{"SET @b := 1", true, ""},
		{"SET @.c = 1", true, ""},
		{"SET @_d = 1", true, ""},
		{"SET @_e._$. = 1", true, ""},
		{"SET @~f = 1", false, ""},
		{"SET @`g,` = 1", true, ""},
		// session system variables
		{"SET SESSION autocommit = 1", true, ""},
		{"SET @@session.autocommit = 1", true, ""},
		{"SET @@SESSION.autocommit = 1", true, ""},
		{"SET @@GLOBAL.GTID_PURGED = '123'", true, ""},
		{"SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN", true, ""},
		{"SET LOCAL autocommit = 1", true, ""},
		{"SET @@local.autocommit = 1", true, ""},
		{"SET @@autocommit = 1", true, ""},
		{"SET autocommit = 1", true, ""},
		// global system variables
		{"SET GLOBAL autocommit = 1", true, ""},
		{"SET @@global.autocommit = 1", true, ""},
		// set default value
		{"SET @@global.autocommit = default", true, ""},
		{"SET @@session.autocommit = default", true, ""},
		// SET CHARACTER SET
		{"SET CHARACTER SET utf8mb4;", true, ""},
		{"SET CHARACTER SET 'utf8mb4';", true, ""},
		// set password
		{"SET PASSWORD = 'password';", true, ""},
		{"SET PASSWORD FOR 'root'@'localhost' = 'password';", true, ""},
		// SET TRANSACTION Syntax
		{"SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", true, ""},
		{"SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ", true, ""},
		{"SET SESSION TRANSACTION READ WRITE", true, ""},
		{"SET SESSION TRANSACTION READ ONLY", true, ""},
		{"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", true, ""},
		{"SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", true, ""},
		{"SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE", true, ""},
		{"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", true, ""},
		{"SET TRANSACTION READ WRITE", true, ""},
		{"SET TRANSACTION READ ONLY", true, ""},
		{"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", true, ""},
		{"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", true, ""},
		{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", true, ""},
		// for set names
		{"set names utf8", true, ""},
		{"set names utf8 collate utf8_unicode_ci", true, ""},
		{"set names binary", true, ""},
		// for set names and set vars
		{"set names utf8, @@session.sql_mode=1;", true, ""},
		{"set @@session.sql_mode=1, names utf8, charset utf8;", true, ""},

		// for FLUSH statement
		{"flush no_write_to_binlog tables tbl1 with read lock", true, ""},
		{"flush table", true, ""},
		{"flush tables", true, ""},
		{"flush tables tbl1", true, ""},
		{"flush no_write_to_binlog tables tbl1", true, ""},
		{"flush local tables tbl1", true, ""},
		{"flush table with read lock", true, ""},
		{"flush tables tbl1, tbl2, tbl3", true, ""},
		{"flush tables tbl1, tbl2, tbl3 with read lock", true, ""},
		{"flush privileges", true, ""},
		{"flush status", true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestFlushTable(c *C) {
	parser := New()
	stmt, err := parser.Parse("flush local tables tbl1,tbl2 with read lock", "", "")
	c.Assert(err, IsNil)
	flushTable := stmt[0].(*ast.FlushStmt)
	c.Assert(flushTable.Tp, Equals, ast.FlushTables)
	c.Assert(flushTable.Tables[0].Name.L, Equals, "tbl1")
	c.Assert(flushTable.Tables[1].Name.L, Equals, "tbl2")
	c.Assert(flushTable.NoWriteToBinLog, IsTrue)
	c.Assert(flushTable.ReadLock, IsTrue)
}

func (s *testParserSuite) TestFlushPrivileges(c *C) {
	parser := New()
	stmt, err := parser.Parse("flush privileges", "", "")
	c.Assert(err, IsNil)
	flushPrivilege := stmt[0].(*ast.FlushStmt)
	c.Assert(flushPrivilege.Tp, Equals, ast.FlushPrivileges)
}

func (s *testParserSuite) TestExpression(c *C) {
	table := []testCase{
		// sign expression
		{"SELECT ++1", true, ""},
		{"SELECT -*1", false, ""},
		{"SELECT -+1", true, ""},
		{"SELECT -1", true, ""},
		{"SELECT --1", true, ""},

		// for string literal
		{`select '''a''', """a"""`, true, ""},
		{`select ''a''`, false, ""},
		{`select ""a""`, false, ""},
		{`select '''a''';`, true, ""},
		{`select '\'a\'';`, true, ""},
		{`select "\"a\"";`, true, ""},
		{`select """a""";`, true, ""},
		{`select _utf8"string";`, true, ""},
		{`select _binary"string";`, true, ""},
		{"select N'string'", true, ""},
		{"select n'string'", true, ""},
		// for comparison
		{"select 1 <=> 0, 1 <=> null, 1 = null", true, ""},
		// for date literal
		{"select date'1989-09-10'", true, ""},
		{"select date 19890910", false, ""},
		// for time literal
		{"select time '00:00:00.111'", true, ""},
		{"select time 19890910", false, ""},
		// for timestamp literal
		{"select timestamp '1989-09-10 11:11:11'", true, ""},
		{"select timestamp 19890910", false, ""},

		// The ODBC syntax for time/date/timestamp literal.
		// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
		{"select {ts '1989-09-10 11:11:11'}", true, ""},
		{"select {d '1989-09-10'}", true, ""},
		{"select {t '00:00:00.111'}", true, ""},
		// If the identifier is not in (t, d, ts), we just ignore it and consider the following expression as the value.
		// See: https://dev.mysql.com/doc/refman/5.7/en/expressions.html
		{"select {ts123 '1989-09-10 11:11:11'}", true, ""},
		{"select {ts123 123}", true, ""},
		{"select {ts123 1 xor 1}", true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestBuiltin(c *C) {
	table := []testCase{
		// for builtin functions
		{"SELECT POW(1, 2)", true, ""},
		{"SELECT POW(1, 2, 1)", true, ""}, // illegal number of arguments shall pass too
		{"SELECT POW(1, 0.5)", true, ""},
		{"SELECT POW(1, -1)", true, ""},
		{"SELECT POW(-1, 1)", true, ""},
		{"SELECT RAND();", true, ""},
		{"SELECT RAND(1);", true, ""},
		{"SELECT MOD(10, 2);", true, ""},
		{"SELECT ROUND(-1.23);", true, ""},
		{"SELECT ROUND(1.23, 1);", true, ""},
		{"SELECT ROUND(1.23, 1, 1);", true, ""},
		{"SELECT CEIL(-1.23);", true, ""},
		{"SELECT CEILING(1.23);", true, ""},
		{"SELECT FLOOR(-1.23);", true, ""},
		{"SELECT LN(1);", true, ""},
		{"SELECT LN(1, 2);", true, ""},
		{"SELECT LOG(-2);", true, ""},
		{"SELECT LOG(2, 65536);", true, ""},
		{"SELECT LOG(2, 65536, 1);", true, ""},
		{"SELECT LOG2(2);", true, ""},
		{"SELECT LOG2(2, 2);", true, ""},
		{"SELECT LOG10(10);", true, ""},
		{"SELECT LOG10(10, 1);", true, ""},
		{"SELECT ABS(10, 1);", true, ""},
		{"SELECT ABS(10);", true, ""},
		{"SELECT ABS();", true, ""},
		{"SELECT CONV(10+'10'+'10'+X'0a',10,10);", true, ""},
		{"SELECT CONV();", true, ""},
		{"SELECT CRC32('MySQL');", true, ""},
		{"SELECT CRC32();", true, ""},
		{"SELECT SIGN();", true, ""},
		{"SELECT SIGN(0);", true, ""},
		{"SELECT SQRT(0);", true, ""},
		{"SELECT SQRT();", true, ""},
		{"SELECT ACOS();", true, ""},
		{"SELECT ACOS(1);", true, ""},
		{"SELECT ACOS(1, 2);", true, ""},
		{"SELECT ASIN();", true, ""},
		{"SELECT ASIN(1);", true, ""},
		{"SELECT ASIN(1, 2);", true, ""},
		{"SELECT ATAN(0), ATAN(1), ATAN(1, 2);", true, ""},
		{"SELECT ATAN2(), ATAN2(1,2);", true, ""},
		{"SELECT COS(0);", true, ""},
		{"SELECT COS(1);", true, ""},
		{"SELECT COS(1, 2);", true, ""},
		{"SELECT COT();", true, ""},
		{"SELECT COT(1);", true, ""},
		{"SELECT COT(1, 2);", true, ""},
		{"SELECT DEGREES();", true, ""},
		{"SELECT DEGREES(0);", true, ""},
		{"SELECT EXP();", true, ""},
		{"SELECT EXP(1);", true, ""},
		{"SELECT PI();", true, ""},
		{"SELECT PI(1);", true, ""},
		{"SELECT RADIANS();", true, ""},
		{"SELECT RADIANS(1);", true, ""},
		{"SELECT SIN();", true, ""},
		{"SELECT SIN(1);", true, ""},
		{"SELECT TAN(1);", true, ""},
		{"SELECT TAN();", true, ""},
		{"SELECT TRUNCATE(1.223,1);", true, ""},
		{"SELECT TRUNCATE();", true, ""},

		{"SELECT SUBSTR('Quadratically',5);", true, ""},
		{"SELECT SUBSTR('Quadratically',5, 3);", true, ""},
		{"SELECT SUBSTR('Quadratically' FROM 5);", true, ""},
		{"SELECT SUBSTR('Quadratically' FROM 5 FOR 3);", true, ""},

		{"SELECT SUBSTRING('Quadratically',5);", true, ""},
		{"SELECT SUBSTRING('Quadratically',5, 3);", true, ""},
		{"SELECT SUBSTRING('Quadratically' FROM 5);", true, ""},
		{"SELECT SUBSTRING('Quadratically' FROM 5 FOR 3);", true, ""},

		{"SELECT CONVERT('111', SIGNED);", true, ""},

		{"SELECT LEAST(), LEAST(1, 2, 3);", true, ""},

		{"SELECT INTERVAL(1, 0, 1, 2)", true, ""},
		{"SELECT DATE_ADD('2008-01-02', INTERVAL INTERVAL(1, 0, 1) DAY);", true, ""},

		// information functions
		{"SELECT DATABASE();", true, ""},
		{"SELECT SCHEMA();", true, ""},
		{"SELECT USER();", true, ""},
		{"SELECT USER(1);", true, ""},
		{"SELECT CURRENT_USER();", true, ""},
		{"SELECT CURRENT_USER;", true, ""},
		{"SELECT CONNECTION_ID();", true, ""},
		{"SELECT VERSION();", true, ""},
		{"SELECT BENCHMARK(1000000, AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3')));", true, ""},
		{"SELECT BENCHMARK(AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3')));", true, ""},
		{"SELECT CHARSET('abc');", true, ""},
		{"SELECT COERCIBILITY('abc');", true, ""},
		{"SELECT COERCIBILITY('abc', 'a');", true, ""},
		{"SELECT COLLATION('abc');", true, ""},
		{"SELECT ROW_COUNT();", true, ""},
		{"SELECT SESSION_USER();", true, ""},
		{"SELECT SYSTEM_USER();", true, ""},

		{"SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);", true, ""},
		{"SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2);", true, ""},

		{`SELECT ASCII(), ASCII(""), ASCII("A"), ASCII(1);`, true, ""},

		{`SELECT LOWER("A"), UPPER("a")`, true, ""},
		{`SELECT LCASE("A"), UCASE("a")`, true, ""},

		{`SELECT REPLACE('www.mysql.com', 'w', 'Ww')`, true, ""},

		{`SELECT LOCATE('bar', 'foobarbar');`, true, ""},
		{`SELECT LOCATE('bar', 'foobarbar', 5);`, true, ""},

		{`SELECT tidb_version();`, true, ""},
		{`SELECT tidb_is_ddl_owner();`, true, ""},

		// for time fsp
		{"CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true, ""},

		// for row
		{"select row(1)", false, ""},
		{"select row(1, 1,)", false, ""},
		{"select (1, 1,)", false, ""},
		{"select row(1, 1) > row(1, 1), row(1, 1, 1) > row(1, 1, 1)", true, ""},
		{"Select (1, 1) > (1, 1)", true, ""},
		{"create table t (`row` int)", true, ""},
		{"create table t (row int)", false, ""},

		// for cast with charset
		{"SELECT *, CAST(data AS CHAR CHARACTER SET utf8) FROM t;", true, ""},

		// for cast as JSON
		{"SELECT *, CAST(data AS JSON) FROM t;", true, ""},

		// for cast as signed int, fix issue #3691.
		{"select cast(1 as signed int);", true, ""},

		// for last_insert_id
		{"SELECT last_insert_id();", true, ""},
		{"SELECT last_insert_id(1);", true, ""},

		// for binary operator
		{"SELECT binary 'a';", true, ""},

		// for bit_count
		{`SELECT BIT_COUNT(1);`, true, ""},

		// select time
		{"select current_timestamp", true, ""},
		{"select current_timestamp()", true, ""},
		{"select current_timestamp(6)", true, ""},
		{"select current_timestamp(null)", false, ""},
		{"select current_timestamp(-1)", false, ""},
		{"select current_timestamp(1.0)", false, ""},
		{"select current_timestamp('2')", false, ""},
		{"select now()", true, ""},
		{"select now(6)", true, ""},
		{"select sysdate(), sysdate(6)", true, ""},
		{"SELECT time('01:02:03');", true, ""},
		{"SELECT time('01:02:03.1')", true, ""},
		{"SELECT time('20.1')", true, ""},
		{"SELECT TIMEDIFF('2000:01:01 00:00:00', '2000:01:01 00:00:00.000001');", true, ""},
		{"SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01');", true, ""},
		{"SELECT TIMESTAMPDIFF(YEAR,'2002-05-01','2001-01-01');", true, ""},
		{"SELECT TIMESTAMPDIFF(MINUTE,'2003-02-01','2003-05-01 12:05:55');", true, ""},

		// select current_time
		{"select current_time", true, ""},
		{"select current_time()", true, ""},
		{"select current_time(6)", true, ""},
		{"select current_time(-1)", false, ""},
		{"select current_time(1.0)", false, ""},
		{"select current_time('1')", false, ""},
		{"select current_time(null)", false, ""},
		{"select curtime()", true, ""},
		{"select curtime(6)", true, ""},
		{"select curtime(-1)", false, ""},
		{"select curtime(1.0)", false, ""},
		{"select curtime('1')", false, ""},
		{"select curtime(null)", false, ""},

		// select utc_timestamp
		{"select utc_timestamp", true, ""},
		{"select utc_timestamp()", true, ""},
		{"select utc_timestamp(6)", true, ""},
		{"select utc_timestamp(-1)", false, ""},
		{"select utc_timestamp(1.0)", false, ""},
		{"select utc_timestamp('1')", false, ""},
		{"select utc_timestamp(null)", false, ""},

		// select utc_time
		{"select utc_time", true, ""},
		{"select utc_time()", true, ""},
		{"select utc_time(6)", true, ""},
		{"select utc_time(-1)", false, ""},
		{"select utc_time(1.0)", false, ""},
		{"select utc_time('1')", false, ""},
		{"select utc_time(null)", false, ""},

		// for microsecond, second, minute, hour
		{"SELECT MICROSECOND('2009-12-31 23:59:59.000010');", true, ""},
		{"SELECT SECOND('10:05:03');", true, ""},
		{"SELECT MINUTE('2008-02-03 10:05:03');", true, ""},
		{"SELECT HOUR(), HOUR('10:05:03');", true, ""},

		// for date, day, weekday
		{"SELECT CURRENT_DATE, CURRENT_DATE(), CURDATE()", true, ""},
		{"SELECT CURRENT_DATE, CURRENT_DATE(), CURDATE(1)", false, ""},
		{"SELECT DATEDIFF('2003-12-31', '2003-12-30');", true, ""},
		{"SELECT DATE('2003-12-31 01:02:03');", true, ""},
		{"SELECT DATE();", true, ""},
		{"SELECT DATE('2003-12-31 01:02:03', '');", true, ""},
		{`SELECT DATE_FORMAT('2003-12-31 01:02:03', '%W %M %Y');`, true, ""},
		{"SELECT DAY('2007-02-03');", true, ""},
		{"SELECT DAYOFMONTH('2007-02-03');", true, ""},
		{"SELECT DAYOFWEEK('2007-02-03');", true, ""},
		{"SELECT DAYOFYEAR('2007-02-03');", true, ""},
		{"SELECT DAYNAME('2007-02-03');", true, ""},
		{"SELECT FROM_DAYS(1423);", true, ""},
		{"SELECT WEEKDAY('2007-02-03');", true, ""},

		// for utc_date
		{"SELECT UTC_DATE, UTC_DATE();", true, ""},
		{"SELECT UTC_DATE(), UTC_DATE()+0", true, ""},

		// for week, month, year
		{"SELECT WEEK();", true, ""},
		{"SELECT WEEK('2007-02-03');", true, ""},
		{"SELECT WEEK('2007-02-03', 0);", true, ""},
		{"SELECT WEEKOFYEAR('2007-02-03');", true, ""},
		{"SELECT MONTH('2007-02-03');", true, ""},
		{"SELECT MONTHNAME('2007-02-03');", true, ""},
		{"SELECT YEAR('2007-02-03');", true, ""},
		{"SELECT YEARWEEK('2007-02-03');", true, ""},
		{"SELECT YEARWEEK('2007-02-03', 0);", true, ""},

		// for ADDTIME, SUBTIME
		{"SELECT ADDTIME('01:00:00.999999', '02:00:00.999998');", true, ""},
		{"SELECT ADDTIME('02:00:00.999998');", true, ""},
		{"SELECT ADDTIME();", true, ""},
		{"SELECT SUBTIME('01:00:00.999999', '02:00:00.999998');", true, ""},

		// for CONVERT_TZ
		{"SELECT CONVERT_TZ();", true, ""},
		{"SELECT CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00');", true, ""},
		{"SELECT CONVERT_TZ('2004-01-01 12:00:00','+00:00','+10:00', '+10:00');", true, ""},

		// for GET_FORMAT
		{"SELECT GET_FORMAT(DATE, 'USA');", true, ""},
		{"SELECT GET_FORMAT(DATETIME, 'USA');", true, ""},
		{"SELECT GET_FORMAT(TIME, 'USA');", true, ""},
		{"SELECT GET_FORMAT(TIMESTAMP, 'USA');", true, ""},

		// for LOCALTIME, LOCALTIMESTAMP
		{"SELECT LOCALTIME(), LOCALTIME(1)", true, ""},
		{"SELECT LOCALTIMESTAMP(), LOCALTIMESTAMP(2)", true, ""},

		// for MAKEDATE, MAKETIME
		{"SELECT MAKEDATE(2011,31);", true, ""},
		{"SELECT MAKETIME(12,15,30);", true, ""},
		{"SELECT MAKEDATE();", true, ""},
		{"SELECT MAKETIME();", true, ""},

		// for PERIOD_ADD, PERIOD_DIFF
		{"SELECT PERIOD_ADD(200801,2)", true, ""},
		{"SELECT PERIOD_DIFF(200802,200703)", true, ""},

		// for QUARTER
		{"SELECT QUARTER('2008-04-01');", true, ""},

		// for SEC_TO_TIME
		{"SELECT SEC_TO_TIME(2378)", true, ""},

		// for TIME_FORMAT
		{`SELECT TIME_FORMAT('100:00:00', '%H %k %h %I %l')`, true, ""},

		// for TIME_TO_SEC
		{"SELECT TIME_TO_SEC('22:23:00')", true, ""},

		// for TIMESTAMPADD
		{"SELECT TIMESTAMPADD(WEEK,1,'2003-01-02');", true, ""},

		// for TO_DAYS, TO_SECONDS
		{"SELECT TO_DAYS('2007-10-07')", true, ""},
		{"SELECT TO_SECONDS('2009-11-29')", true, ""},

		// for LAST_DAY
		{"SELECT LAST_DAY('2003-02-05');", true, ""},

		// for UTC_TIME
		{"SELECT UTC_TIME(), UTC_TIME(1)", true, ""},

		// for time extract
		{`select extract(microsecond from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(second from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(minute from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(hour from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(day from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(week from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(month from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(quarter from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(year from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(second_microsecond from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(minute_microsecond from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(minute_second from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(hour_microsecond from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(hour_second from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(hour_minute from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(day_microsecond from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(day_second from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(day_minute from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(day_hour from "2011-11-11 10:10:10.123456")`, true, ""},
		{`select extract(year_month from "2011-11-11 10:10:10.123456")`, true, ""},

		// for from_unixtime
		{`select from_unixtime(1447430881)`, true, ""},
		{`select from_unixtime(1447430881.123456)`, true, ""},
		{`select from_unixtime(1447430881.1234567)`, true, ""},
		{`select from_unixtime(1447430881.9999999)`, true, ""},
		{`select from_unixtime(1447430881, "%Y %D %M %h:%i:%s %x")`, true, ""},
		{`select from_unixtime(1447430881.123456, "%Y %D %M %h:%i:%s %x")`, true, ""},
		{`select from_unixtime(1447430881.1234567, "%Y %D %M %h:%i:%s %x")`, true, ""},

		// for issue 224
		{`SELECT CAST('test collated returns' AS CHAR CHARACTER SET utf8) COLLATE utf8_bin;`, true, ""},

		// for string functions
		// trim
		{`SELECT TRIM('  bar   ');`, true, ""},
		{`SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');`, true, ""},
		{`SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx');`, true, ""},
		{`SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');`, true, ""},
		{`SELECT LTRIM(' foo ');`, true, ""},
		{`SELECT RTRIM(' bar ');`, true, ""},

		{`SELECT RPAD('hi', 6, 'c');`, true, ""},
		{`SELECT BIT_LENGTH('hi');`, true, ""},
		{`SELECT CHAR(65);`, true, ""},
		{`SELECT CHAR_LENGTH('abc');`, true, ""},
		{`SELECT CHARACTER_LENGTH('abc');`, true, ""},
		{`SELECT FIELD('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo');`, true, ""},
		{`SELECT FIND_IN_SET('foo', 'foo,bar')`, true, ""},
		{`SELECT FIND_IN_SET('foo')`, true, ""}, // illegal number of argument still pass
		{`SELECT MAKE_SET(1,'a'), MAKE_SET(1,'a','b','c')`, true, ""},
		{`SELECT MID('Sakila', -5, 3)`, true, ""},
		{`SELECT OCT(12)`, true, ""},
		{`SELECT OCTET_LENGTH('text')`, true, ""},
		{`SELECT ORD('2')`, true, ""},
		{`SELECT POSITION('bar' IN 'foobarbar')`, true, ""},
		{`SELECT QUOTE('Don\'t!')`, true, ""},
		{`SELECT BIN(12)`, true, ""},
		{`SELECT ELT(1, 'ej', 'Heja', 'hej', 'foo')`, true, ""},
		{`SELECT EXPORT_SET(5,'Y','N'), EXPORT_SET(5,'Y','N',','), EXPORT_SET(5,'Y','N',',',4)`, true, ""},
		{`SELECT FORMAT(), FORMAT(12332.2,2,'de_DE'), FORMAT(12332.123456, 4)`, true, ""},
		{`SELECT FROM_BASE64('abc')`, true, ""},
		{`SELECT TO_BASE64('abc')`, true, ""},
		{`SELECT INSERT(), INSERT('Quadratic', 3, 4, 'What'), INSTR('foobarbar', 'bar')`, true, ""},
		{`SELECT LOAD_FILE('/tmp/picture')`, true, ""},
		{`SELECT LPAD('hi',4,'??')`, true, ""},
		{`SELECT LEFT("foobar", 3)`, true, ""},
		{`SELECT RIGHT("foobar", 3)`, true, ""},

		// repeat
		{`SELECT REPEAT("a", 10);`, true, ""},

		// for miscellaneous functions
		{`SELECT SLEEP(10);`, true, ""},
		{`SELECT ANY_VALUE(@arg);`, true, ""},
		{`SELECT INET_ATON('10.0.5.9');`, true, ""},
		{`SELECT INET_NTOA(167773449);`, true, ""},
		{`SELECT INET6_ATON('fdfe::5a55:caff:fefa:9089');`, true, ""},
		{`SELECT INET6_NTOA(INET_NTOA(167773449));`, true, ""},
		{`SELECT IS_FREE_LOCK(@str);`, true, ""},
		{`SELECT IS_IPV4('10.0.5.9');`, true, ""},
		{`SELECT IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'));`, true, ""},
		{`SELECT IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'));`, true, ""},
		{`SELECT IS_IPV6('10.0.5.9');`, true, ""},
		{`SELECT IS_USED_LOCK(@str);`, true, ""},
		{`SELECT MASTER_POS_WAIT(@log_name, @log_pos), MASTER_POS_WAIT(@log_name, @log_pos, @timeout), MASTER_POS_WAIT(@log_name, @log_pos, @timeout, @channel_name);`, true, ""},
		{`SELECT NAME_CONST('myname', 14);`, true, ""},
		{`SELECT RELEASE_ALL_LOCKS();`, true, ""},
		{`SELECT UUID();`, true, ""},
		{`SELECT UUID_SHORT()`, true, ""},
		// test illegal arguments
		{`SELECT SLEEP();`, true, ""},
		{`SELECT ANY_VALUE();`, true, ""},
		{`SELECT INET_ATON();`, true, ""},
		{`SELECT INET_NTOA();`, true, ""},
		{`SELECT INET6_ATON();`, true, ""},
		{`SELECT INET6_NTOA(INET_NTOA());`, true, ""},
		{`SELECT IS_FREE_LOCK();`, true, ""},
		{`SELECT IS_IPV4();`, true, ""},
		{`SELECT IS_IPV4_COMPAT(INET6_ATON());`, true, ""},
		{`SELECT IS_IPV4_MAPPED(INET6_ATON());`, true, ""},
		{`SELECT IS_IPV6()`, true, ""},
		{`SELECT IS_USED_LOCK();`, true, ""},
		{`SELECT MASTER_POS_WAIT();`, true, ""},
		{`SELECT NAME_CONST();`, true, ""},
		{`SELECT RELEASE_ALL_LOCKS(1);`, true, ""},
		{`SELECT UUID(1);`, true, ""},
		{`SELECT UUID_SHORT(1)`, true, ""},
		// interval
		{`select "2011-11-11 10:10:10.123456" + interval 10 second`, true, ""},
		{`select "2011-11-11 10:10:10.123456" - interval 10 second`, true, ""},
		// for date_add
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 second)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 minute)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 hour)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10 day)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 week)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 month)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval 1 year)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, ""},
		{`select date_add("2011-11-11 10:10:10.123456", 10)`, false, ""},
		{`select date_add("2011-11-11 10:10:10.123456", 0.10)`, false, ""},
		{`select date_add("2011-11-11 10:10:10.123456", "11,11")`, false, ""},

		// for strcmp
		{`select strcmp('abc', 'def')`, true, ""},

		// for adddate
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 second)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 minute)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 hour)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10 day)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 week)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 month)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval 1 year)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", 10)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", 0.10)`, true, ""},
		{`select adddate("2011-11-11 10:10:10.123456", "11,11")`, true, ""},

		// for date_sub
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 second)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 minute)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 hour)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10 day)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 week)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 month)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 1 year)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", 10)`, false, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", 0.10)`, false, ""},
		{`select date_sub("2011-11-11 10:10:10.123456", "11,11")`, false, ""},

		// for subdate
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 microsecond)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 second)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 minute)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 hour)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10 day)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 week)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 month)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 quarter)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval 1 year)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10.10" second_microsecond)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10.10" minute_microsecond)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10" minute_second)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10:10.10" hour_microsecond)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10:10" hour_second)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "10:10" hour_minute)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval 10.10 hour_minute)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10:10.10" day_microsecond)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10:10" day_second)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10:10" day_minute)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11 10" day_hour)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", interval "11-11" year_month)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", 10)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", 0.10)`, true, ""},
		{`select subdate("2011-11-11 10:10:10.123456", "11,11")`, true, ""},

		// for unix_timestamp
		{`select unix_timestamp()`, true, ""},
		{`select unix_timestamp('2015-11-13 10:20:19.012')`, true, ""},

		// for misc functions
		{`SELECT GET_LOCK('lock1',10);`, true, ""},
		{`SELECT RELEASE_LOCK('lock1');`, true, ""},

		// for aggregate functions
		{`select avg(), avg(c1,c2) from t;`, false, ""},
		{`select avg(distinct c1) from t;`, true, ""},
		{`select avg(distinctrow c1) from t;`, true, ""},
		{`select avg(distinct all c1) from t;`, true, ""},
		{`select avg(distinctrow all c1) from t;`, true, ""},
		{`select avg(c2) from t;`, true, ""},
		{`select bit_and(c1) from t;`, true, ""},
		{`select bit_and(all c1) from t;`, true, ""},
		{`select bit_and(distinct c1) from t;`, false, ""},
		{`select bit_and(distinctrow c1) from t;`, false, ""},
		{`select bit_and(distinctrow all c1) from t;`, false, ""},
		{`select bit_and(distinct all c1) from t;`, false, ""},
		{`select bit_and(), bit_and(distinct c1) from t;`, false, ""},
		{`select bit_and(), bit_and(distinctrow c1) from t;`, false, ""},
		{`select bit_and(), bit_and(all c1) from t;`, false, ""},
		{`select bit_or(c1) from t;`, true, ""},
		{`select bit_or(all c1) from t;`, true, ""},
		{`select bit_or(distinct c1) from t;`, false, ""},
		{`select bit_or(distinctrow c1) from t;`, false, ""},
		{`select bit_or(distinctrow all c1) from t;`, false, ""},
		{`select bit_or(distinct all c1) from t;`, false, ""},
		{`select bit_or(), bit_or(distinct c1) from t;`, false, ""},
		{`select bit_or(), bit_or(distinctrow c1) from t;`, false, ""},
		{`select bit_or(), bit_or(all c1) from t;`, false, ""},
		{`select bit_xor(c1) from t;`, true, ""},
		{`select bit_xor(all c1) from t;`, true, ""},
		{`select bit_xor(distinct c1) from t;`, false, ""},
		{`select bit_xor(distinctrow c1) from t;`, false, ""},
		{`select bit_xor(distinctrow all c1) from t;`, false, ""},
		{`select bit_xor(), bit_xor(distinct c1) from t;`, false, ""},
		{`select bit_xor(), bit_xor(distinctrow c1) from t;`, false, ""},
		{`select bit_xor(), bit_xor(all c1) from t;`, false, ""},
		{`select max(c1,c2) from t;`, false, ""},
		{`select max(distinct c1) from t;`, true, ""},
		{`select max(distinctrow c1) from t;`, true, ""},
		{`select max(distinct all c1) from t;`, true, ""},
		{`select max(distinctrow all c1) from t;`, true, ""},
		{`select max(c2) from t;`, true, ""},
		{`select min(c1,c2) from t;`, false, ""},
		{`select min(distinct c1) from t;`, true, ""},
		{`select min(distinctrow c1) from t;`, true, ""},
		{`select min(distinct all c1) from t;`, true, ""},
		{`select min(distinctrow all c1) from t;`, true, ""},
		{`select min(c2) from t;`, true, ""},
		{`select sum(c1,c2) from t;`, false, ""},
		{`select sum(distinct c1) from t;`, true, ""},
		{`select sum(distinctrow c1) from t;`, true, ""},
		{`select sum(distinct all c1) from t;`, true, ""},
		{`select sum(distinctrow all c1) from t;`, true, ""},
		{`select sum(c2) from t;`, true, ""},
		{`select count(c1) from t;`, true, ""},
		{`select count(distinct *) from t;`, false, ""},
		{`select count(distinctrow *) from t;`, false, ""},
		{`select count(*) from t;`, true, ""},
		{`select count(distinct c1, c2) from t;`, true, ""},
		{`select count(distinctrow c1, c2) from t;`, true, ""},
		{`select count(c1, c2) from t;`, false, ""},
		{`select count(all c1) from t;`, true, ""},
		{`select count(distinct all c1) from t;`, false, ""},
		{`select count(distinctrow all c1) from t;`, false, ""},
		{`select group_concat(c2,c1) from t group by c1;`, true, ""},
		{`select group_concat(c2,c1 SEPARATOR ';') from t group by c1;`, true, ""},
		{`select group_concat(distinct c2,c1) from t group by c1;`, true, ""},
		{`select group_concat(distinctrow c2,c1) from t group by c1;`, true, ""},
		{`SELECT student_name, GROUP_CONCAT(DISTINCT test_score ORDER BY test_score DESC SEPARATOR ' ') FROM student GROUP BY student_name;`, true, ""},
		{`select std(c1), std(all c1), std(distinct c1) from t`, true, ""},
		{`select std(c1, c2) from t`, false, ""},
		{`select stddev(c1), stddev(all c1), stddev(distinct c1) from t`, true, ""},
		{`select stddev(c1, c2) from t`, false, ""},
		{`select stddev_pop(c1), stddev_pop(all c1), stddev_pop(distinct c1) from t`, true, ""},
		{`select stddev_pop(c1, c2) from t`, false, ""},
		{`select stddev_samp(c1), stddev_samp(all c1), stddev_samp(distinct c1) from t`, true, ""},
		{`select stddev_samp(c1, c2) from t`, false, ""},
		{`select variance(c1), variance(all c1), variance(distinct c1) from t`, true, ""},
		{`select variance(c1, c2) from t`, false, ""},
		{`select var_pop(c1), var_pop(all c1), var_pop(distinct c1) from t`, true, ""},
		{`select var_pop(c1, c2) from t`, false, ""},
		{`select var_samp(c1), var_samp(all c1), var_samp(distinct c1) from t`, true, ""},
		{`select var_samp(c1, c2) from t`, false, ""},

		// for encryption and compression functions
		{`select AES_ENCRYPT('text',UNHEX('F3229A0B371ED2D9441B830D21A390C3'))`, true, ""},
		{`select AES_DECRYPT(@crypt_str,@key_str)`, true, ""},
		{`select AES_DECRYPT(@crypt_str,@key_str,@init_vector);`, true, ""},
		{`SELECT COMPRESS('');`, true, ""},
		{`SELECT DECODE(@crypt_str, @pass_str);`, true, ""},
		{`SELECT DES_DECRYPT(@crypt_str), DES_DECRYPT(@crypt_str, @key_str);`, true, ""},
		{`SELECT DES_ENCRYPT(@str), DES_ENCRYPT(@key_num);`, true, ""},
		{`SELECT ENCODE('cleartext', CONCAT('my_random_salt','my_secret_password'));`, true, ""},
		{`SELECT ENCRYPT('hello'), ENCRYPT('hello', @salt);`, true, ""},
		{`SELECT MD5('testing');`, true, ""},
		{`SELECT OLD_PASSWORD(@str);`, true, ""},
		{`SELECT PASSWORD(@str);`, true, ""},
		{`SELECT RANDOM_BYTES(@len);`, true, ""},
		{`SELECT SHA1('abc');`, true, ""},
		{`SELECT SHA('abc');`, true, ""},
		{`SELECT SHA2('abc', 224);`, true, ""},
		{`SELECT UNCOMPRESS('any string');`, true, ""},
		{`SELECT UNCOMPRESSED_LENGTH(@compressed_string);`, true, ""},
		{`SELECT VALIDATE_PASSWORD_STRENGTH(@str);`, true, ""},

		// For JSON functions.
		{`SELECT JSON_EXTRACT();`, true, ""},
		{`SELECT JSON_UNQUOTE();`, true, ""},
		{`SELECT JSON_TYPE('[123]');`, true, ""},
		{`SELECT JSON_TYPE();`, true, ""},

		// For two json grammar sugar.
		{`SELECT a->'$.a' FROM t`, true, ""},
		{`SELECT a->>'$.a' FROM t`, true, ""},
		{`SELECT '{}'->'$.a' FROM t`, false, ""},
		{`SELECT '{}'->>'$.a' FROM t`, false, ""},
		{`SELECT a->3 FROM t`, false, ""},
		{`SELECT a->>3 FROM t`, false, ""},

		// Test that quoted identifier can be a function name.
		{"SELECT `uuid`()", true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestIdentifier(c *C) {
	table := []testCase{
		// for quote identifier
		{"select `a`, `a.b`, `a b` from t", true, ""},
		// for unquoted identifier
		{"create table MergeContextTest$Simple (value integer not null, primary key (value))", true, ""},
		// for as
		{"select 1 as a, 1 as `a`, 1 as \"a\", 1 as 'a'", true, ""},
		{`select 1 as a, 1 as "a", 1 as 'a'`, true, ""},
		{`select 1 a, 1 "a", 1 'a'`, true, ""},
		{`select * from t as "a"`, false, ""},
		{`select * from t a`, true, ""},
		// reserved keyword can't be used as identifier directly, but A.B pattern is an exception
		{`select COUNT from DESC`, false, ""},
		{`select COUNT from SELECT.DESC`, true, ""},
		{"use `select`", true, "USE `select`"},
		{"use `sel``ect`", true, "USE `sel``ect`"},
		{"use select", false, "USE `select`"},
		{`select * from t as a`, true, ""},
		{"select 1 full, 1 row, 1 abs", false, ""},
		{"select 1 full, 1 `row`, 1 abs", true, ""},
		{"select * from t full, t1 row, t2 abs", false, ""},
		{"select * from t full, t1 `row`, t2 abs", true, ""},
		// for issue 1878, identifiers may begin with digit.
		{"create database 123test", true, "CREATE DATABASE `123test`"},
		{"create database 123", false, "CREATE DATABASE `123`"},
		{"create database `123`", true, "CREATE DATABASE `123`"},
		{"create database `12``3`", true, "CREATE DATABASE `12``3`"},
		{"create table `123` (123a1 int)", true, ""},
		{"create table 123 (123a1 int)", false, ""},
		{fmt.Sprintf("select * from t%cble", 0), false, ""},
		// for issue 3954, should NOT be recognized as identifiers.
		{`select .78+123`, true, ""},
		{`select .78+.21`, true, ""},
		{`select .78-123`, true, ""},
		{`select .78-.21`, true, ""},
		{`select .78--123`, true, ""},
		{`select .78*123`, true, ""},
		{`select .78*.21`, true, ""},
		{`select .78/123`, true, ""},
		{`select .78/.21`, true, ""},
		{`select .78,123`, true, ""},
		{`select .78,.21`, true, ""},
		{`select .78 , 123`, true, ""},
		{`select .78.123`, false, ""},
		{`select .78#123`, true, ""}, // select .78
		{`insert float_test values(.67, 'string');`, true, ""},
		{`select .78'123'`, true, ""}, // select .78 as '123'
		{"select .78`123`", true, ""}, // select .78 as `123`
		{`select .78"123"`, true, ""}, // select .78 as "123"
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestDDL(c *C) {
	table := []testCase{
		{"CREATE", false, ""},
		{"CREATE TABLE", false, ""},
		{"CREATE TABLE foo (", false, ""},
		{"CREATE TABLE foo ()", false, ""},
		{"CREATE TABLE foo ();", false, ""},
		{"CREATE TABLE foo (a TINYINT UNSIGNED);", true, ""},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true, ""},
		{"CREATE TABLE foo (a bigint unsigned, b bool);", true, ""},
		{"CREATE TABLE foo (a TINYINT, b SMALLINT) CREATE TABLE bar (x INT, y int64)", false, ""},
		{"CREATE TABLE foo (a int, b float); CREATE TABLE bar (x double, y float)", true, ""},
		{"CREATE TABLE foo (a bytes)", false, ""},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED)", true, ""},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) -- foo", true, ""},
		// {"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) // foo", true,"},
		{"CREATE TABLE foo (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true, ""},
		{"CREATE TABLE foo /* foo */ (a SMALLINT UNSIGNED, b INT UNSIGNED) /* foo */", true, ""},
		{"CREATE TABLE foo (name CHAR(50) BINARY)", true, ""},
		{"CREATE TABLE foo (name CHAR(50) COLLATE utf8_bin)", true, ""},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET utf8)", true, ""},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET utf8 BINARY)", true, ""},
		{"CREATE TABLE foo (name CHAR(50) CHARACTER SET utf8 BINARY CHARACTER set utf8)", false, ""},
		{"CREATE TABLE foo (name CHAR(50) BINARY CHARACTER SET utf8 COLLATE utf8_bin)", true, ""},
		{"CREATE TABLE foo (a.b, b);", false, ""},
		{"CREATE TABLE foo (a, b.c);", false, ""},
		{"CREATE TABLE (name CHAR(50) BINARY)", false, ""},
		// for table option
		{"create table t (c int) avg_row_length = 3", true, ""},
		{"create table t (c int) avg_row_length 3", true, ""},
		{"create table t (c int) checksum = 0", true, ""},
		{"create table t (c int) checksum 1", true, ""},
		{"create table t (c int) compression = 'NONE'", true, ""},
		{"create table t (c int) compression 'lz4'", true, ""},
		{"create table t (c int) connection = 'abc'", true, ""},
		{"create table t (c int) connection 'abc'", true, ""},
		{"create table t (c int) key_block_size = 1024", true, ""},
		{"create table t (c int) key_block_size 1024", true, ""},
		{"create table t (c int) max_rows = 1000", true, ""},
		{"create table t (c int) max_rows 1000", true, ""},
		{"create table t (c int) min_rows = 1000", true, ""},
		{"create table t (c int) min_rows 1000", true, ""},
		{"create table t (c int) password = 'abc'", true, ""},
		{"create table t (c int) password 'abc'", true, ""},
		{"create table t (c int) DELAY_KEY_WRITE=1", true, ""},
		{"create table t (c int) DELAY_KEY_WRITE 1", true, ""},
		{"create table t (c int) ROW_FORMAT = default", true, ""},
		{"create table t (c int) ROW_FORMAT default", true, ""},
		{"create table t (c int) ROW_FORMAT = fixed", true, ""},
		{"create table t (c int) ROW_FORMAT = compressed", true, ""},
		{"create table t (c int) ROW_FORMAT = compact", true, ""},
		{"create table t (c int) ROW_FORMAT = redundant", true, ""},
		{"create table t (c int) ROW_FORMAT = dynamic", true, ""},
		{"create table t (c int) STATS_PERSISTENT = default", true, ""},
		{"create table t (c int) STATS_PERSISTENT = 0", true, ""},
		{"create table t (c int) STATS_PERSISTENT = 1", true, ""},
		{"create table t (c int) PACK_KEYS = 1", true, ""},
		{"create table t (c int) PACK_KEYS = 0", true, ""},
		{"create table t (c int) PACK_KEYS = DEFAULT", true, ""},
		{`create table testTableCompression (c VARCHAR(15000)) compression="ZLIB";`, true, ""},
		{`create table t1 (c1 int) compression="zlib";`, true, ""},

		// partition option
		{"create table t (c int) PARTITION BY HASH (c) PARTITIONS 32;", true, ""},
		{"create table t (c int) PARTITION BY HASH (Year(VDate)) (PARTITION p1980 VALUES LESS THAN (1980) ENGINE = MyISAM, PARTITION p1990 VALUES LESS THAN (1990) ENGINE = MyISAM, PARTITION pothers VALUES LESS THAN MAXVALUE ENGINE = MyISAM)", false, ""},
		{"create table t (c int) PARTITION BY RANGE (Year(VDate)) (PARTITION p1980 VALUES LESS THAN (1980) ENGINE = MyISAM, PARTITION p1990 VALUES LESS THAN (1990) ENGINE = MyISAM, PARTITION pothers VALUES LESS THAN MAXVALUE ENGINE = MyISAM)", true, ""},
		{"create table t (c int, `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '') PARTITION BY RANGE (UNIX_TIMESTAMP(create_time)) (PARTITION p201610 VALUES LESS THAN(1477929600), PARTITION p201611 VALUES LESS THAN(1480521600),PARTITION p201612 VALUES LESS THAN(1483200000),PARTITION p201701 VALUES LESS THAN(1485878400),PARTITION p201702 VALUES LESS THAN(1488297600),PARTITION p201703 VALUES LESS THAN(1490976000))", true, ""},
		{"CREATE TABLE `md_product_shop` (`shopCode` varchar(4) DEFAULT NULL COMMENT '地点') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!50100 PARTITION BY KEY (shopCode) PARTITIONS 19 */;", true, ""},
		{"CREATE TABLE `payinfo1` (`id` bigint(20) NOT NULL AUTO_INCREMENT, `oderTime` datetime NOT NULL) ENGINE=InnoDB AUTO_INCREMENT=641533032 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8 /*!50500 PARTITION BY RANGE COLUMNS(oderTime) (PARTITION P2011 VALUES LESS THAN ('2012-01-01 00:00:00') ENGINE = InnoDB, PARTITION P1201 VALUES LESS THAN ('2012-02-01 00:00:00') ENGINE = InnoDB, PARTITION PMAX VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB)*/;", true, ""},
		{`CREATE TABLE app_channel_daily_report (id bigint(20) NOT NULL AUTO_INCREMENT, app_version varchar(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT 'default', gmt_create datetime NOT NULL COMMENT '创建时间', PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=33703438 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
/*!50100 PARTITION BY RANGE (month(gmt_create)-1)
(PARTITION part0 VALUES LESS THAN (1) COMMENT = '1月份' ENGINE = InnoDB,
 PARTITION part1 VALUES LESS THAN (2) COMMENT = '2月份' ENGINE = InnoDB,
 PARTITION part2 VALUES LESS THAN (3) COMMENT = '3月份' ENGINE = InnoDB,
 PARTITION part3 VALUES LESS THAN (4) COMMENT = '4月份' ENGINE = InnoDB,
 PARTITION part4 VALUES LESS THAN (5) COMMENT = '5月份' ENGINE = InnoDB,
 PARTITION part5 VALUES LESS THAN (6) COMMENT = '6月份' ENGINE = InnoDB,
 PARTITION part6 VALUES LESS THAN (7) COMMENT = '7月份' ENGINE = InnoDB,
 PARTITION part7 VALUES LESS THAN (8) COMMENT = '8月份' ENGINE = InnoDB,
 PARTITION part8 VALUES LESS THAN (9) COMMENT = '9月份' ENGINE = InnoDB,
 PARTITION part9 VALUES LESS THAN (10) COMMENT = '10月份' ENGINE = InnoDB,
 PARTITION part10 VALUES LESS THAN (11) COMMENT = '11月份' ENGINE = InnoDB,
 PARTITION part11 VALUES LESS THAN (12) COMMENT = '12月份' ENGINE = InnoDB) */ ;`, true, ""},

		// for check clause
		{"create table t (c1 bool, c2 bool, check (c1 in (0, 1)), check (c2 in (0, 1)))", true, ""},
		{"CREATE TABLE Customer (SD integer CHECK (SD > 0), First_Name varchar(30));", true, ""},

		{"create database xxx", true, "CREATE DATABASE `xxx`"},
		{"create database if exists xxx", false, ""},
		{"create database if not exists xxx", true, "CREATE DATABASE IF NOT EXISTS `xxx`"},
		{"create schema xxx", true, ""},
		{"create schema if exists xxx", false, ""},
		{"create schema if not exists xxx", true, ""},
		// for drop database/schema/table/view/stats
		{"drop database xxx", true, "DROP DATABASE `xxx`"},
		{"drop database if exists xxx", true, "DROP DATABASE IF EXISTS `xxx`"},
		{"drop database if not exists xxx", false, ""},
		{"drop schema xxx", true, ""},
		{"drop schema if exists xxx", true, ""},
		{"drop schema if not exists xxx", false, ""},
		{"drop table", false, ""},
		{"drop table xxx", true, ""},
		{"drop table xxx, yyy", true, ""},
		{"drop tables xxx", true, ""},
		{"drop tables xxx, yyy", true, ""},
		{"drop table if exists xxx", true, ""},
		{"drop table if not exists xxx", false, ""},
		{"drop table xxx restrict", true, ""},
		{"drop table xxx, yyy cascade", true, ""},
		{"drop table if exists xxx restrict", true, ""},
		{"drop view", false, ""},
		{"drop view xxx", true, ""},
		{"drop view xxx, yyy", true, ""},
		{"drop view if exists xxx", true, ""},
		{"drop view if exists xxx, yyy", true, ""},
		{"drop stats t", true, ""},
		// for issue 974
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
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, ""},
		// for issue 975
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
		) ENGINE=InnoDB AUTO_INCREMENT=95 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, ""},
		{`create table t (c int KEY);`, true, ""},
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
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`, true, ""},
		{"CREATE TABLE address (\r\nid bigint(20) NOT NULL AUTO_INCREMENT,\r\ncreate_at datetime NOT NULL,\r\ndeleted tinyint(1) NOT NULL,\r\nupdate_at datetime NOT NULL,\r\nversion bigint(20) DEFAULT NULL,\r\naddress varchar(128) NOT NULL,\r\naddress_detail varchar(128) NOT NULL,\r\ncellphone varchar(16) NOT NULL,\r\nlatitude double NOT NULL,\r\nlongitude double NOT NULL,\r\nname varchar(16) NOT NULL,\r\nsex tinyint(1) NOT NULL,\r\nuser_id bigint(20) NOT NULL,\r\nPRIMARY KEY (id),\r\nCONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id) ON DELETE CASCADE ON UPDATE NO ACTION,\r\nINDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''\r\n) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;", true, ""},
		// for issue 1802
		{`CREATE TABLE t1 (
		accout_id int(11) DEFAULT '0',
		summoner_id int(11) DEFAULT '0',
		union_name varbinary(52) NOT NULL,
		union_id int(11) DEFAULT '0',
		PRIMARY KEY (union_name)) ENGINE=MyISAM DEFAULT CHARSET=binary;`, true, ""},
		// Create table with multiple index options.
		{`create table t (c int, index ci (c) USING BTREE COMMENT "123");`, true, ""},
		// for default value
		{"CREATE TABLE sbtest (id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, k integer UNSIGNED DEFAULT '0' NOT NULL, c char(120) DEFAULT '' NOT NULL, pad char(60) DEFAULT '' NOT NULL, PRIMARY KEY  (id) )", true, ""},
		{"create table test (create_date TIMESTAMP NOT NULL COMMENT '创建日期 create date' DEFAULT now());", true, ""},
		{"create table ts (t int, v timestamp(3) default CURRENT_TIMESTAMP(3));", true, ""},
		// Create table with primary key name.
		{"create table if not exists `t` (`id` int not null auto_increment comment '消息ID', primary key `pk_id` (`id`) );", true, ""},
		// Create table with like.
		{"create table a like b", true, ""},
		{"create table a (like b)", true, ""},
		{"create table if not exists a like b", true, ""},
		{"create table if not exists a (like b)", true, ""},
		{"create table if not exists a like (b)", false, ""},
		{"create table a (t int) like b", false, ""},
		{"create table a (t int) like (b)", false, ""},
		// Create table with select statement
		{"create table a select * from b", true, ""},
		{"create table a as select * from b", true, ""},
		{"create table a (m int, n datetime) as select * from b", true, ""},
		{"create table a (unique(n)) as select n from b", true, ""},
		{"create table a ignore as select n from b", true, ""},
		{"create table a replace as select n from b", true, ""},
		{"create table a (m int) replace as (select n as m from b union select n+1 as m from c group by 1 limit 2)", true, ""},

		// Create table with no option is valid for parser
		{"create table a", true, ""},

		{"create table t (a timestamp default now)", false, ""},
		{"create table t (a timestamp default now())", true, ""},
		{"create table t (a timestamp default now() on update now)", false, ""},
		{"create table t (a timestamp default now() on update now())", true, ""},
		{"CREATE TABLE t (c TEXT) default CHARACTER SET utf8, default COLLATE utf8_general_ci;", true, ""},
		{"CREATE TABLE t (c TEXT) shard_row_id_bits = 1;", true, ""},
		// Create table with ON UPDATE CURRENT_TIMESTAMP(6), specify fraction part.
		{"CREATE TABLE IF NOT EXISTS `general_log` (`event_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),`user_host` mediumtext NOT NULL,`thread_id` bigint(20) unsigned NOT NULL,`server_id` int(10) unsigned NOT NULL,`command_type` varchar(64) NOT NULL,`argument` mediumblob NOT NULL) ENGINE=CSV DEFAULT CHARSET=utf8 COMMENT='General log'", true, ""},
		// For reference_definition in column_definition.
		{"CREATE TABLE followers ( f1 int NOT NULL REFERENCES user_profiles (uid) );", true, ""},

		// for alter table
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED)", true, ""},
		{"ALTER TABLE ADD COLUMN (a SMALLINT UNSIGNED)", false, ""},
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED, b varchar(255))", true, ""},
		{"ALTER TABLE t ADD COLUMN (a SMALLINT UNSIGNED FIRST)", false, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED FIRST", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED AFTER b", true, ""},
		{"ALTER TABLE employees ADD PARTITION", true, ""},
		{"ALTER TABLE employees ADD PARTITION ( PARTITION P1 VALUES LESS THAN (2010))", true, ""},
		{"ALTER TABLE employees ADD PARTITION ( PARTITION P2 VALUES LESS THAN MAXVALUE)", true, ""},
		{`ALTER TABLE employees ADD PARTITION (
				PARTITION P1 VALUES LESS THAN (2010),
				PARTITION P2 VALUES LESS THAN (2015),
				PARTITION P3 VALUES LESS THAN MAXVALUE)`, true, ""},
		// For drop table partition statement.
		{"alter table t drop partition p1;", true, ""},
		{"alter table t drop partition p2;", true, ""},
		{"alter table employees add partition partitions 1;", true, ""},
		{"alter table employees add partition partitions 2;", true, ""},
		{"alter table clients coalesce partition 3;", true, ""},
		{"alter table clients coalesce partition 4;", true, ""},
		{"ALTER TABLE t DISABLE KEYS", true, ""},
		{"ALTER TABLE t ENABLE KEYS", true, ""},
		{"ALTER TABLE t MODIFY COLUMN a varchar(255)", true, ""},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255)", true, ""},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255) CHARACTER SET utf8 BINARY", true, ""},
		{"ALTER TABLE t CHANGE COLUMN a b varchar(255) FIRST", true, ""},
		{"ALTER TABLE db.t RENAME to db1.t1", true, ""},
		{"ALTER TABLE db.t RENAME db1.t1", true, ""},
		{"ALTER TABLE t RENAME as t1", true, ""},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT 1", true, ""},
		{"ALTER TABLE t ALTER a SET DEFAULT 1", true, ""},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT CURRENT_TIMESTAMP", false, ""},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT NOW()", false, ""},
		{"ALTER TABLE t ALTER COLUMN a SET DEFAULT 1+1", false, ""},
		{"ALTER TABLE t ALTER COLUMN a DROP DEFAULT", true, ""},
		{"ALTER TABLE t ALTER a DROP DEFAULT", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=none", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=default", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=shared", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, lock=exclusive", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=NONE", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=DEFAULT", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=SHARED", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, LOCK=EXCLUSIVE", true, ""},
		{"ALTER TABLE t ADD FULLTEXT KEY `FullText` (`name` ASC)", true, ""},
		{"ALTER TABLE t ADD FULLTEXT INDEX `FullText` (`name` ASC)", true, ""},
		{"ALTER TABLE t ADD INDEX (a) USING BTREE COMMENT 'a'", true, ""},
		{"ALTER TABLE t ADD KEY (a) USING HASH COMMENT 'a'", true, ""},
		{"ALTER TABLE t ADD PRIMARY KEY (a) COMMENT 'a'", true, ""},
		{"ALTER TABLE t ADD UNIQUE (a) COMMENT 'a'", true, ""},
		{"ALTER TABLE t ADD UNIQUE KEY (a) COMMENT 'a'", true, ""},
		{"ALTER TABLE t ADD UNIQUE INDEX (a) COMMENT 'a'", true, ""},
		{"ALTER TABLE t ENGINE ''", true, ""},
		{"ALTER TABLE t ENGINE = ''", true, ""},
		{"ALTER TABLE t ENGINE = 'innodb'", true, ""},
		{"ALTER TABLE t ENGINE = innodb", true, ""},
		{"ALTER TABLE `db`.`t` ENGINE = ``", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT UNSIGNED, ADD COLUMN a SMALLINT", true, ""},
		{"ALTER TABLE t ADD COLUMN a SMALLINT, ENGINE = '', default COLLATE = utf8_general_ci", true, ""},
		{"ALTER TABLE t ENGINE = '', COMMENT='', default COLLATE = utf8_general_ci", true, ""},
		{"ALTER TABLE t ENGINE = '', ADD COLUMN a SMALLINT", true, ""},
		{"ALTER TABLE t default COLLATE = utf8_general_ci, ENGINE = '', ADD COLUMN a SMALLINT", true, ""},
		{"ALTER TABLE t shard_row_id_bits = 1", true, ""},
		{"ALTER TABLE t AUTO_INCREMENT 3", true, ""},
		{"ALTER TABLE t AUTO_INCREMENT = 3", true, ""},
		{"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL , ALGORITHM = DEFAULT;", true, ""},
		{"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL , ALGORITHM = INPLACE;", true, ""},
		{"ALTER TABLE `hello-world@dev`.`User` ADD COLUMN `name` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL , ALGORITHM = COPY;", true, ""},
		{"ALTER TABLE t CONVERT TO CHARACTER SET utf8;", true, ""},
		{"ALTER TABLE t CONVERT TO CHARSET utf8;", true, ""},
		{"ALTER TABLE t CONVERT TO CHARACTER SET utf8 COLLATE utf8_bin;", true, ""},
		{"ALTER TABLE t CONVERT TO CHARSET utf8 COLLATE utf8_bin;", true, ""},
		{"ALTER TABLE t FORCE", true, ""},
		{"ALTER TABLE t DROP INDEX;", false, ""},
		{"ALTER TABLE t DROP COLUMN a CASCADE", true, ""},
		{`ALTER TABLE testTableCompression COMPRESSION="LZ4";`, true, ""},
		{`ALTER TABLE t1 COMPRESSION="zlib";`, true, ""},

		// For #6405
		{"ALTER TABLE t RENAME KEY a TO b;", true, ""},
		{"ALTER TABLE t RENAME INDEX a TO b;", true, ""},

		{"alter table t analyze partition a", true, ""},
		{"alter table t analyze partition a with 4 buckets", true, ""},
		{"alter table t analyze partition a index b", true, ""},
		{"alter table t analyze partition a index b with 4 buckets", true, ""},

		// For create index statement
		{"CREATE INDEX idx ON t (a)", true, ""},
		{"CREATE INDEX idx ON t (a) USING HASH", true, ""},
		{"CREATE INDEX idx ON t (a) COMMENT 'foo'", true, ""},
		{"CREATE INDEX idx ON t (a) USING HASH COMMENT 'foo'", true, ""},
		{"CREATE INDEX idx ON t (a) LOCK=NONE", true, ""},
		{"CREATE INDEX idx USING BTREE ON t (a) USING HASH COMMENT 'foo'", true, ""},
		{"CREATE INDEX idx USING BTREE ON t (a)", true, ""},

		// for rename table statement
		{"RENAME TABLE t TO t1", true, ""},
		{"RENAME TABLE t t1", false, ""},
		{"RENAME TABLE d.t TO d1.t1", true, ""},
		{"RENAME TABLE t1 TO t2, t3 TO t4", true, ""},

		// for truncate statement
		{"TRUNCATE TABLE t1", true, ""},
		{"TRUNCATE t1", true, ""},

		// for empty alert table index
		{"ALTER TABLE t ADD INDEX () ", false, ""},
		{"ALTER TABLE t ADD UNIQUE ()", false, ""},
		{"ALTER TABLE t ADD UNIQUE INDEX ()", false, ""},
		{"ALTER TABLE t ADD UNIQUE KEY ()", false, ""},

		// for issue 4538
		{"create table a (process double)", true, ""},

		// for issue 4740
		{"create table t (a int1, b int2, c int3, d int4, e int8)", true, ""},

		// for issue 5918
		{"create table t (lv long varchar null)", true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestOptimizerHints(c *C) {
	parser := New()
	stmt, err := parser.Parse("select /*+ tidb_SMJ(T1,t2) tidb_smj(T3,t4) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	c.Assert(err, IsNil)
	selectStmt := stmt[0].(*ast.SelectStmt)

	hints := selectStmt.TableHints
	c.Assert(len(hints), Equals, 2)
	c.Assert(hints[0].HintName.L, Equals, "tidb_smj")
	c.Assert(len(hints[0].Tables), Equals, 2)
	c.Assert(hints[0].Tables[0].L, Equals, "t1")
	c.Assert(hints[0].Tables[1].L, Equals, "t2")

	c.Assert(hints[1].HintName.L, Equals, "tidb_smj")
	c.Assert(hints[1].Tables[0].L, Equals, "t3")
	c.Assert(hints[1].Tables[1].L, Equals, "t4")

	c.Assert(len(selectStmt.TableHints), Equals, 2)

	stmt, err = parser.Parse("select /*+ TIDB_INLJ(t1, T2) tidb_inlj(t3, t4) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	c.Assert(err, IsNil)
	selectStmt = stmt[0].(*ast.SelectStmt)

	hints = selectStmt.TableHints
	c.Assert(len(hints), Equals, 2)
	c.Assert(hints[0].HintName.L, Equals, "tidb_inlj")
	c.Assert(len(hints[0].Tables), Equals, 2)
	c.Assert(hints[0].Tables[0].L, Equals, "t1")
	c.Assert(hints[0].Tables[1].L, Equals, "t2")

	c.Assert(hints[1].HintName.L, Equals, "tidb_inlj")
	c.Assert(hints[1].Tables[0].L, Equals, "t3")
	c.Assert(hints[1].Tables[1].L, Equals, "t4")

	stmt, err = parser.Parse("select /*+ TIDB_HJ(t1, T2) tidb_hj(t3, t4) */ c1, c2 from t1, t2 where t1.c1 = t2.c1", "", "")
	c.Assert(err, IsNil)
	selectStmt = stmt[0].(*ast.SelectStmt)

	hints = selectStmt.TableHints
	c.Assert(len(hints), Equals, 2)
	c.Assert(hints[0].HintName.L, Equals, "tidb_hj")
	c.Assert(len(hints[0].Tables), Equals, 2)
	c.Assert(hints[0].Tables[0].L, Equals, "t1")
	c.Assert(hints[0].Tables[1].L, Equals, "t2")

	c.Assert(hints[1].HintName.L, Equals, "tidb_hj")
	c.Assert(hints[1].Tables[0].L, Equals, "t3")
	c.Assert(hints[1].Tables[1].L, Equals, "t4")

	stmt, err = parser.Parse("SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t1 INNER JOIN t2 where t1.c1 = t2.c1", "", "")
	c.Assert(err, IsNil)
	selectStmt = stmt[0].(*ast.SelectStmt)
	hints = selectStmt.TableHints
	c.Assert(len(hints), Equals, 1)
	c.Assert(hints[0].HintName.L, Equals, "max_execution_time")
	c.Assert(hints[0].MaxExecutionTime, Equals, uint64(1000))
}

func (s *testParserSuite) TestType(c *C) {
	table := []testCase{
		// for time fsp
		{"CREATE TABLE t( c1 TIME(2), c2 DATETIME(2), c3 TIMESTAMP(2) );", true, ""},

		// for hexadecimal
		{"select x'0a', X'11', 0x11", true, ""},
		{"select x'13181C76734725455A'", true, ""},
		{"select x'0xaa'", false, ""},
		{"select 0X11", false, ""},
		{"select 0x4920616D2061206C6F6E672068657820737472696E67", true, ""},

		// for bit
		{"select 0b01, 0b0, b'11', B'11'", true, ""},
		{"select 0B01", false, ""},
		{"select 0b21", false, ""},

		// for enum and set type
		{"create table t (c1 enum('a', 'b'), c2 set('a', 'b'))", true, ""},
		{"create table t (c1 enum)", false, ""},
		{"create table t (c1 set)", false, ""},

		// for blob and text field length
		{"create table t (c1 blob(1024), c2 text(1024))", true, ""},

		// for year
		{"create table t (y year(4), y1 year)", true, ""},
		{"create table t (y year(4) unsigned zerofill zerofill, y1 year signed unsigned zerofill)", true, ""},

		// for national
		{"create table t (c1 national char(2), c2 national varchar(2))", true, ""},

		// for json type
		{`create table t (a JSON);`, true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestPrivilege(c *C) {
	table := []testCase{
		// for create user
		{`CREATE USER 'test'`, true, ""},
		{`CREATE USER test`, true, ""},
		{"CREATE USER `test`", true, ""},
		{"CREATE USER test-user", false, ""},
		{"CREATE USER test.user", false, ""},
		{"CREATE USER 'test-user'", true, ""},
		{"CREATE USER `test-user`", true, ""},
		{"CREATE USER test.user", false, ""},
		{"CREATE USER 'test.user'", true, ""},
		{"CREATE USER `test.user`", true, ""},
		{"CREATE USER uesr1@localhost", true, ""},
		{"CREATE USER `uesr1`@localhost", true, ""},
		{"CREATE USER uesr1@`localhost`", true, ""},
		{"CREATE USER `uesr1`@`localhost`", true, ""},
		{"CREATE USER 'uesr1'@localhost", true, ""},
		{"CREATE USER uesr1@'localhost'", true, ""},
		{"CREATE USER 'uesr1'@'localhost'", true, ""},
		{"CREATE USER 'uesr1'@`localhost`", true, ""},
		{"CREATE USER `uesr1`@'localhost'", true, ""},
		{"create user 'bug19354014user'@'%' identified WITH mysql_native_password", true, ""},
		{"create user 'bug19354014user'@'%' identified WITH mysql_native_password by 'new-password'", true, ""},
		{"create user 'bug19354014user'@'%' identified WITH mysql_native_password as 'hashstring'", true, ""},
		{`CREATE USER IF NOT EXISTS 'root'@'localhost' IDENTIFIED BY 'new-password'`, true, ""},
		{`CREATE USER 'root'@'localhost' IDENTIFIED BY 'new-password'`, true, ""},
		{`CREATE USER 'root'@'localhost' IDENTIFIED BY PASSWORD 'hashstring'`, true, ""},
		{`CREATE USER 'root'@'localhost' IDENTIFIED BY 'new-password', 'root'@'127.0.0.1' IDENTIFIED BY PASSWORD 'hashstring'`, true, ""},
		{`ALTER USER IF EXISTS 'root'@'localhost' IDENTIFIED BY 'new-password'`, true, ""},
		{`ALTER USER 'root'@'localhost' IDENTIFIED BY 'new-password'`, true, ""},
		{`ALTER USER 'root'@'localhost' IDENTIFIED BY PASSWORD 'hashstring'`, true, ""},
		{`ALTER USER 'root'@'localhost' IDENTIFIED BY 'new-password', 'root'@'127.0.0.1' IDENTIFIED BY PASSWORD 'hashstring'`, true, ""},
		{`ALTER USER USER() IDENTIFIED BY 'new-password'`, true, ""},
		{`ALTER USER IF EXISTS USER() IDENTIFIED BY 'new-password'`, true, ""},
		{`DROP USER 'root'@'localhost', 'root1'@'localhost'`, true, ""},
		{`DROP USER IF EXISTS 'root'@'localhost'`, true, ""},

		// for grant statement
		{"GRANT ALL ON db1.* TO 'jeffrey'@'localhost';", true, ""},
		{"GRANT ALL ON db1.* TO 'jeffrey'@'localhost' WITH GRANT OPTION;", true, ""},
		{"GRANT SELECT ON db2.invoice TO 'jeffrey'@'localhost';", true, ""},
		{"GRANT ALL ON *.* TO 'someuser'@'somehost';", true, ""},
		{"GRANT SELECT, INSERT ON *.* TO 'someuser'@'somehost';", true, ""},
		{"GRANT ALL ON mydb.* TO 'someuser'@'somehost';", true, ""},
		{"GRANT SELECT, INSERT ON mydb.* TO 'someuser'@'somehost';", true, ""},
		{"GRANT ALL ON mydb.mytbl TO 'someuser'@'somehost';", true, ""},
		{"GRANT SELECT, INSERT ON mydb.mytbl TO 'someuser'@'somehost';", true, ""},
		{"GRANT SELECT (col1), INSERT (col1,col2) ON mydb.mytbl TO 'someuser'@'somehost';", true, ""},
		{"grant all privileges on zabbix.* to 'zabbix'@'localhost' identified by 'password';", true, ""},
		{"GRANT SELECT ON test.* to 'test'", true, ""},                                                                                                            // For issue 2654.
		{"grant PROCESS,usage, REPLICATION SLAVE, REPLICATION CLIENT on *.* to 'xxxxxxxxxx'@'%' identified by password 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx'", true, ""}, // For issue 4865
		{"/* rds internal mark */ GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, RELOAD, PROCESS, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES,      EXECUTE, REPLICATION SLAVE, REPLICATION CLIENT, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, CREATE USER, EVENT,      TRIGGER on *.* to 'root2'@'%' identified by password '*sdsadsdsadssadsadsadsadsada' with grant option", true, ""},

		// for revoke statement
		{"REVOKE ALL ON db1.* FROM 'jeffrey'@'localhost';", true, ""},
		{"REVOKE SELECT ON db2.invoice FROM 'jeffrey'@'localhost';", true, ""},
		{"REVOKE ALL ON *.* FROM 'someuser'@'somehost';", true, ""},
		{"REVOKE SELECT, INSERT ON *.* FROM 'someuser'@'somehost';", true, ""},
		{"REVOKE ALL ON mydb.* FROM 'someuser'@'somehost';", true, ""},
		{"REVOKE SELECT, INSERT ON mydb.* FROM 'someuser'@'somehost';", true, ""},
		{"REVOKE ALL ON mydb.mytbl FROM 'someuser'@'somehost';", true, ""},
		{"REVOKE SELECT, INSERT ON mydb.mytbl FROM 'someuser'@'somehost';", true, ""},
		{"REVOKE SELECT (col1), INSERT (col1,col2) ON mydb.mytbl FROM 'someuser'@'somehost';", true, ""},
		{"REVOKE all privileges on zabbix.* FROM 'zabbix'@'localhost' identified by 'password';", true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestComment(c *C) {
	table := []testCase{
		{"create table t (c int comment 'comment')", true, ""},
		{"create table t (c int) comment = 'comment'", true, ""},
		{"create table t (c int) comment 'comment'", true, ""},
		{"create table t (c int) comment comment", false, ""},
		{"create table t (comment text)", true, ""},
		{"START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */", true, ""},
		// for comment in query
		{"/*comment*/ /*comment*/ select c /* this is a comment */ from t;", true, ""},
		// for unclosed comment
		{"delete from t where a = 7 or 1=1/*' and b = 'p'", false, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestCommentErrMsg(c *C) {
	table := []testErrMsgCase{
		{"delete from t where a = 7 or 1=1/*' and b = 'p'", false, errors.New("[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '/*' and b = 'p'' at line 1")},
		{"delete from t where a = 7 or\n 1=1/*' and b = 'p'", false, errors.New("[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '/*' and b = 'p'' at line 2")},
		{"select 1/*", false, errors.New("[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '/*' at line 1")},
		{"select 1/* comment */", false, nil},
	}
	s.RunErrMsgTest(c, table)
}

type subqueryChecker struct {
	text string
	c    *C
}

// Enter implements ast.Visitor interface.
func (sc *subqueryChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	if expr, ok := inNode.(*ast.SubqueryExpr); ok {
		sc.c.Assert(expr.Query.Text(), Equals, sc.text)
		return inNode, true
	}
	return inNode, false
}

// Leave implements ast.Visitor interface.
func (sc *subqueryChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	return inNode, true
}

func (s *testParserSuite) TestSubquery(c *C) {
	table := []testCase{
		// for compare subquery
		{"SELECT 1 > (select 1)", true, ""},
		{"SELECT 1 > ANY (select 1)", true, ""},
		{"SELECT 1 > ALL (select 1)", true, ""},
		{"SELECT 1 > SOME (select 1)", true, ""},

		// for exists subquery
		{"SELECT EXISTS select 1", false, ""},
		{"SELECT EXISTS (select 1)", true, ""},
		{"SELECT + EXISTS (select 1)", true, ""},
		{"SELECT - EXISTS (select 1)", true, ""},
		{"SELECT NOT EXISTS (select 1)", true, ""},
		{"SELECT + NOT EXISTS (select 1)", false, ""},
		{"SELECT - NOT EXISTS (select 1)", false, ""},
	}
	s.RunTest(c, table)

	tests := []struct {
		input string
		text  string
	}{
		{"SELECT 1 > (select 1)", "select 1"},
		{"SELECT 1 > (select 1 union select 2)", "select 1 union select 2"},
	}
	parser := New()
	for _, t := range tests {
		stmt, err := parser.ParseOneStmt(t.input, "", "")
		c.Assert(err, IsNil)
		stmt.Accept(&subqueryChecker{
			text: t.text,
			c:    c,
		})
	}
}
func (s *testParserSuite) TestUnion(c *C) {
	table := []testCase{
		{"select c1 from t1 union select c2 from t2", true, ""},
		{"select c1 from t1 union (select c2 from t2)", true, ""},
		{"select c1 from t1 union (select c2 from t2) order by c1", true, ""},
		{"select c1 from t1 union select c2 from t2 order by c2", true, ""},
		{"select c1 from t1 union (select c2 from t2) limit 1", true, ""},
		{"select c1 from t1 union (select c2 from t2) limit 1, 1", true, ""},
		{"select c1 from t1 union (select c2 from t2) order by c1 limit 1", true, ""},
		{"(select c1 from t1) union distinct select c2 from t2", true, ""},
		{"(select c1 from t1) union distinctrow select c2 from t2", true, ""},
		{"(select c1 from t1) union all select c2 from t2", true, ""},
		{"(select c1 from t1) union distinct all select c2 from t2", false, ""},
		{"(select c1 from t1) union distinctrow all select c2 from t2", false, ""},
		{"(select c1 from t1) union (select c2 from t2) order by c1 union select c3 from t3", false, ""},
		{"(select c1 from t1) union (select c2 from t2) limit 1 union select c3 from t3", false, ""},
		{"(select c1 from t1) union select c2 from t2 union (select c3 from t3) order by c1 limit 1", true, ""},
		{"select (select 1 union select 1) as a", true, ""},
		{"select * from (select 1 union select 2) as a", true, ""},
		{"insert into t select c1 from t1 union select c2 from t2", true, ""},
		{"insert into t (c) select c1 from t1 union select c2 from t2", true, ""},
		{"select 2 as a from dual union select 1 as b from dual order by a", true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestUnionOrderBy(c *C) {
	parser := New()
	parser.EnableWindowFunc(s.enableWindowFunc)

	tests := []struct {
		src        string
		hasOrderBy []bool
	}{
		{"select 2 as a from dual union select 1 as b from dual order by a", []bool{false, false, true}},
		{"select 2 as a from dual union (select 1 as b from dual order by a)", []bool{false, true, false}},
		{"(select 2 as a from dual order by a) union select 1 as b from dual order by a", []bool{true, false, true}},
		{"select 1 a, 2 b from dual order by a", []bool{true}},
		{"select 1 a, 2 b from dual", []bool{false}},
	}

	for _, t := range tests {
		stmt, err := parser.Parse(t.src, "", "")
		c.Assert(err, IsNil)
		us, ok := stmt[0].(*ast.UnionStmt)
		if ok {
			var i int
			for _, s := range us.SelectList.Selects {
				c.Assert(s.OrderBy != nil, Equals, t.hasOrderBy[i])
				i++
			}
			c.Assert(us.OrderBy != nil, Equals, t.hasOrderBy[i])
		}
		ss, ok := stmt[0].(*ast.SelectStmt)
		if ok {
			c.Assert(ss.OrderBy != nil, Equals, t.hasOrderBy[0])
		}
	}
}

func (s *testParserSuite) TestLikeEscape(c *C) {
	table := []testCase{
		// for like escape
		{`select "abc_" like "abc\\_" escape ''`, true, ""},
		{`select "abc_" like "abc\\_" escape '\\'`, true, ""},
		{`select "abc_" like "abc\\_" escape '||'`, false, ""},
		{`select "abc" like "escape" escape '+'`, true, ""},
	}

	s.RunTest(c, table)
}

func (s *testParserSuite) TestMysqlDump(c *C) {
	// Statements used by mysqldump.
	table := []testCase{
		{`UNLOCK TABLES;`, true, ""},
		{`LOCK TABLES t1 READ;`, true, ""},
		{`show table status like 't'`, true, ""},
		{`LOCK TABLES t2 WRITE`, true, ""},

		// for unlock table and lock table
		{`UNLOCK TABLE;`, true, ""},
		{`LOCK TABLE t1 READ;`, true, ""},
		{`show table status like 't'`, true, ""},
		{`LOCK TABLE t2 WRITE`, true, ""},
		{`LOCK TABLE t1 WRITE, t3 READ`, true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestIndexHint(c *C) {
	table := []testCase{
		{`select * from t use index (primary)`, true, ""},
		{"select * from t use index (`primary`)", true, ""},
		{`select * from t use index ();`, true, ""},
		{`select * from t use index (idx);`, true, ""},
		{`select * from t use index (idx1, idx2);`, true, ""},
		{`select * from t ignore key (idx1)`, true, ""},
		{`select * from t force index for join (idx1)`, true, ""},
		{`select * from t use index for order by (idx1)`, true, ""},
		{`select * from t force index for group by (idx1)`, true, ""},
		{`select * from t use index for group by (idx1) use index for order by (idx2), t2`, true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestPriority(c *C) {
	table := []testCase{
		{`select high_priority * from t`, true, ""},
		{`select low_priority * from t`, true, ""},
		{`select delayed * from t`, true, ""},
		{`insert high_priority into t values (1)`, true, ""},
		{`insert LOW_PRIORITY into t values (1)`, true, ""},
		{`insert delayed into t values (1)`, true, ""},
		{`update low_priority t set a = 2`, true, ""},
		{`update high_priority t set a = 2`, true, ""},
		{`update delayed t set a = 2`, true, ""},
		{`delete low_priority from t where a = 2`, true, ""},
		{`delete high_priority from t where a = 2`, true, ""},
		{`delete delayed from t where a = 2`, true, ""},
		{`replace high_priority into t values (1)`, true, ""},
		{`replace LOW_PRIORITY into t values (1)`, true, ""},
		{`replace delayed into t values (1)`, true, ""},
	}
	s.RunTest(c, table)

	parser := New()
	stmt, err := parser.Parse("select HIGH_PRIORITY * from t", "", "")
	c.Assert(err, IsNil)
	sel := stmt[0].(*ast.SelectStmt)
	c.Assert(sel.SelectStmtOpts.Priority, Equals, mysql.HighPriority)
}

func (s *testParserSuite) TestSQLNoCache(c *C) {
	table := []testCase{
		{`select SQL_NO_CACHE * from t`, false, ""},
		{`select SQL_CACHE * from t`, true, ""},
		{`select * from t`, true, ""},
	}

	parser := New()
	for _, tt := range table {
		stmt, err := parser.Parse(tt.src, "", "")
		c.Assert(err, IsNil)

		sel := stmt[0].(*ast.SelectStmt)
		c.Assert(sel.SelectStmtOpts.SQLCache, Equals, tt.ok)
	}
}

func (s *testParserSuite) TestEscape(c *C) {
	table := []testCase{
		{`select """;`, false, ""},
		{`select """";`, true, ""},
		{`select "汉字";`, true, ""},
		{`select 'abc"def';`, true, ""},
		{`select 'a\r\n';`, true, ""},
		{`select "\a\r\n"`, true, ""},
		{`select "\xFF"`, true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestInsertStatementMemoryAllocation(c *C) {
	sql := "insert t values (1)" + strings.Repeat(",(1)", 1000)
	var oldStats, newStats runtime.MemStats
	runtime.ReadMemStats(&oldStats)
	_, err := New().ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	runtime.ReadMemStats(&newStats)
	c.Assert(int(newStats.TotalAlloc-oldStats.TotalAlloc), Less, 1024*500)
}

func (s *testParserSuite) TestExplain(c *C) {
	table := []testCase{
		{"explain select c1 from t1", true, ""},
		{"explain delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;", true, ""},
		{"explain insert into t values (1), (2), (3)", true, ""},
		{"explain replace into foo values (1 || 2)", true, ""},
		{"explain update t set id = id + 1 order by id desc;", true, ""},
		{"explain select c1 from t1 union (select c2 from t2) limit 1, 1", true, ""},
		{`explain format = "row" select c1 from t1 union (select c2 from t2) limit 1, 1`, true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestTrace(c *C) {
	table := []testCase{
		{"trace select c1 from t1", true, ""},
		{"trace delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;", true, ""},
		{"trace insert into t values (1), (2), (3)", true, ""},
		{"trace replace into foo values (1 || 2)", true, ""},
		{"trace update t set id = id + 1 order by id desc;", true, ""},
		{"trace select c1 from t1 union (select c2 from t2) limit 1, 1", true, ""},
		{"trace format = 'row' select c1 from t1 union (select c2 from t2) limit 1, 1", true, ""},
		{"trace format = 'json' update t set id = id + 1 order by id desc;", true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestView(c *C) {
	table := []testCase{
		{"create view v as select * from t", true, ""},
		{"create or replace view v as select * from t", true, ""},
		{"create or replace algorithm = undefined view v as select * from t", true, ""},
		{"create or replace algorithm = merge view v as select * from t", true, ""},
		{"create or replace algorithm = temptable view v as select * from t", true, ""},
		{"create or replace algorithm = merge definer = 'root' view v as select * from t", true, ""},
		{"create or replace algorithm = merge definer = 'root' sql security definer view v as select * from t", true, ""},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v as select * from t", true, ""},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as select * from t", true, ""},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as select * from t with local check option", true, ""},
		{"create or replace algorithm = merge definer = 'root' sql security invoker view v(a,b) as select * from t with cascaded check option", true, ""},
		{"create or replace algorithm = merge definer = current_user view v as select * from t", true, ""},
	}
	s.RunTest(c, table)

	// Test case for the text of the select statement in create view statement.
	p := New()
	sms, err := p.Parse("create view v as select * from t", "", "")
	c.Assert(err, IsNil)
	v, ok := sms[0].(*ast.CreateViewStmt)
	c.Assert(ok, IsTrue)
	c.Assert(v.Algorithm, Equals, model.AlgorithmUndefined)
	c.Assert(v.Select.Text(), Equals, "select * from t")
	c.Assert(v.Security, Equals, model.SecurityDefiner)
	c.Assert(v.CheckOption, Equals, model.CheckOptionCascaded)

	src := `CREATE OR REPLACE ALGORITHM = UNDEFINED DEFINER = root@localhost
                  SQL SECURITY DEFINER
			      VIEW V(a,b,c) AS select c,d,e from t 
                  WITH CASCADED CHECK OPTION;`

	var st ast.StmtNode
	st, err = p.ParseOneStmt(src, "", "")
	c.Assert(err, IsNil)
	v, ok = st.(*ast.CreateViewStmt)
	c.Assert(ok, IsTrue)
	c.Assert(v.OrReplace, IsTrue)
	c.Assert(v.Algorithm, Equals, model.AlgorithmUndefined)
	c.Assert(v.Definer.Username, Equals, "root")
	c.Assert(v.Definer.Hostname, Equals, "localhost")
	c.Assert(v.Cols[0], Equals, model.CIStr{"a", "a"})
	c.Assert(v.Cols[1], Equals, model.CIStr{"b", "b"})
	c.Assert(v.Cols[2], Equals, model.CIStr{"c", "c"})
	c.Assert(v.Select.Text(), Equals, "select c,d,e from t")
	c.Assert(v.Security, Equals, model.SecurityDefiner)
	c.Assert(v.CheckOption, Equals, model.CheckOptionCascaded)
}

func (s *testParserSuite) TestTimestampDiffUnit(c *C) {
	// Test case for timestampdiff unit.
	// TimeUnit should be unified to upper case.
	parser := New()
	stmt, err := parser.Parse("SELECT TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01'), TIMESTAMPDIFF(month,'2003-02-01','2003-05-01');", "", "")
	c.Assert(err, IsNil)
	ss := stmt[0].(*ast.SelectStmt)
	fields := ss.Fields.Fields
	c.Assert(len(fields), Equals, 2)
	expr := fields[0].Expr
	f, ok := expr.(*ast.FuncCallExpr)
	c.Assert(ok, IsTrue)
	c.Assert(f.Args[0].(ast.ValueExpr).GetDatumString(), Equals, "MONTH")

	expr = fields[1].Expr
	f, ok = expr.(*ast.FuncCallExpr)
	c.Assert(ok, IsTrue)
	c.Assert(f.Args[0].(ast.ValueExpr).GetDatumString(), Equals, "MONTH")

	// Test Illegal TimeUnit for TimestampDiff
	table := []testCase{
		{"SELECT TIMESTAMPDIFF(SECOND_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(MINUTE_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(MINUTE_SECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(HOUR_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(HOUR_SECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(HOUR_MINUTE,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_MICROSECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_SECOND,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_MINUTE,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(DAY_HOUR,'2003-02-01','2003-05-01')", false, ""},
		{"SELECT TIMESTAMPDIFF(YEAR_MONTH,'2003-02-01','2003-05-01')", false, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestSessionManage(c *C) {
	table := []testCase{
		// Kill statement.
		// See https://dev.mysql.com/doc/refman/5.7/en/kill.html
		{"kill 23123", true, ""},
		{"kill connection 23123", true, ""},
		{"kill query 23123", true, ""},
		{"kill tidb 23123", true, ""},
		{"kill tidb connection 23123", true, ""},
		{"kill tidb query 23123", true, ""},
		{"show processlist", true, ""},
		{"show full processlist", true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestSQLModeANSIQuotes(c *C) {
	parser := New()
	parser.SetSQLMode(mysql.ModeANSIQuotes)
	tests := []string{
		`CREATE TABLE "table" ("id" int)`,
		`select * from t "tt"`,
	}
	for _, test := range tests {
		_, err := parser.Parse(test, "", "")
		c.Assert(err, IsNil)
	}
}

func (s *testParserSuite) TestDDLStatements(c *C) {
	parser := New()
	// Tests that whatever the charset it is define, we always assign utf8 charset and utf8_bin collate.
	createTableStr := `CREATE TABLE t (
		a varchar(64) binary,
		b char(10) charset utf8 collate utf8_general_ci,
		c text charset latin1) ENGINE=innoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin`
	stmts, err := parser.Parse(createTableStr, "", "")
	c.Assert(err, IsNil)
	stmt := stmts[0].(*ast.CreateTableStmt)
	c.Assert(mysql.HasBinaryFlag(stmt.Cols[0].Tp.Flag), IsTrue)
	for _, colDef := range stmt.Cols[1:] {
		c.Assert(mysql.HasBinaryFlag(colDef.Tp.Flag), IsFalse)
	}
	for _, tblOpt := range stmt.Options {
		switch tblOpt.Tp {
		case ast.TableOptionCharset:
			c.Assert(tblOpt.StrValue, Equals, "utf8")
		case ast.TableOptionCollate:
			c.Assert(tblOpt.StrValue, Equals, "utf8_bin")
		}
	}
	createTableStr = `CREATE TABLE t (
		a varbinary(64),
		b binary(10),
		c blob)`
	stmts, err = parser.Parse(createTableStr, "", "")
	c.Assert(err, IsNil)
	stmt = stmts[0].(*ast.CreateTableStmt)
	for _, colDef := range stmt.Cols {
		c.Assert(colDef.Tp.Charset, Equals, charset.CharsetBin)
		c.Assert(colDef.Tp.Collate, Equals, charset.CollationBin)
		c.Assert(mysql.HasBinaryFlag(colDef.Tp.Flag), IsTrue)
	}
}

func (s *testParserSuite) TestAnalyze(c *C) {
	table := []testCase{
		{"analyze table t1", true, ""},
		{"analyze table t,t1", true, ""},
		{"analyze table t1 index", true, ""},
		{"analyze table t1 index a", true, ""},
		{"analyze table t1 index a,b", true, ""},
		{"analyze table t with 4 buckets", true, ""},
		{"analyze table t index a with 4 buckets", true, ""},
		{"analyze table t partition a", true, ""},
		{"analyze table t partition a with 4 buckets", true, ""},
		{"analyze table t partition a index b", true, ""},
		{"analyze table t partition a index b with 4 buckets", true, ""},
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestGeneratedColumn(c *C) {
	tests := []struct {
		input string
		ok    bool
		expr  string
	}{
		{"create table t (c int, d int generated always as (c + 1) virtual)", true, "c + 1"},
		{"create table t (c int, d int as (   c + 1   ) virtual)", true, "c + 1"},
		{"create table t (c int, d int as (1 + 1) stored)", true, "1 + 1"},
	}
	parser := New()
	for _, tt := range tests {
		stmtNodes, err := parser.Parse(tt.input, "", "")
		if tt.ok {
			c.Assert(err, IsNil)
			stmtNode := stmtNodes[0]
			for _, col := range stmtNode.(*ast.CreateTableStmt).Cols {
				for _, opt := range col.Options {
					if opt.Tp == ast.ColumnOptionGenerated {
						c.Assert(opt.Expr.Text(), Equals, tt.expr)
					}
				}
			}
		} else {
			c.Assert(err, NotNil)
		}
	}

}

func (s *testParserSuite) TestSetTransaction(c *C) {
	// Set transaction is equivalent to setting the global or session value of tx_isolation.
	// For example:
	// SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED
	// SET SESSION tx_isolation='READ-COMMITTED'
	tests := []struct {
		input    string
		isGlobal bool
		value    string
	}{
		{
			"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
			false, "READ-COMMITTED",
		},
		{
			"SET GLOBAL TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			true, "REPEATABLE-READ",
		},
	}
	parser := New()
	for _, t := range tests {
		stmt1, err := parser.ParseOneStmt(t.input, "", "")
		c.Assert(err, IsNil)
		setStmt := stmt1.(*ast.SetStmt)
		vars := setStmt.Variables[0]
		c.Assert(vars.Name, Equals, "tx_isolation")
		c.Assert(vars.IsGlobal, Equals, t.isGlobal)
		c.Assert(vars.IsSystem, Equals, true)
		c.Assert(vars.Value.(ast.ValueExpr).GetValue(), Equals, t.value)
	}
}

func (s *testParserSuite) TestSideEffect(c *C) {
	// This test cover a bug that parse an error SQL doesn't leave the parser in a
	// clean state, cause the following SQL parse fail.
	parser := New()
	_, err := parser.ParseOneStmt("create table t /*!50100 'abc', 'abc' */;", "", "")
	c.Assert(err, NotNil)

	_, err = parser.ParseOneStmt("show tables;", "", "")
	c.Assert(err, IsNil)
}

func (s *testParserSuite) TestTablePartition(c *C) {
	table := []testCase{
		{"ALTER TABLE t1 TRUNCATE PARTITION p0", true, ""},
		{"ALTER TABLE t1 ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT 'APSTART \\' APEND')", true, ""},
		{"ALTER TABLE t1 ADD PARTITION (PARTITION `p5` VALUES LESS THAN (2010) COMMENT = 'xxx')", true, ""},
		{`CREATE TABLE t1 (a int not null,b int not null,c int not null,primary key(a,b))
		partition by range (a)
		partitions 3
		(partition x1 values less than (5),
		 partition x2 values less than (10),
		 partition x3 values less than maxvalue);`, true, ""},
		{"CREATE TABLE t1 (a int not null) partition by range (a) (partition x1 values less than (5) tablespace ts1)", true, ""},
		{`create table t (a int) partition by range (a)
		  (PARTITION p0 VALUES LESS THAN (63340531200) ENGINE = MyISAM,
		   PARTITION p1 VALUES LESS THAN (63342604800) ENGINE MyISAM)`, true, ""},
		{`create table t (a int) partition by range (a)
		  (PARTITION p0 VALUES LESS THAN (63340531200) ENGINE = MyISAM COMMENT 'xxx',
		   PARTITION p1 VALUES LESS THAN (63342604800) ENGINE = MyISAM)`, true, ""},
		{`create table t1 (a int) partition by range (a)
		  (PARTITION p0 VALUES LESS THAN (63340531200) COMMENT 'xxx' ENGINE = MyISAM ,
		   PARTITION p1 VALUES LESS THAN (63342604800) ENGINE = MyISAM)`, true, ""},
		{`create table t (id int)
		    partition by range (id)
		    subpartition by key (id) subpartitions 2
		    (partition p0 values less than (42))`, true, ""},
		{`create table t (id int)
		    partition by range (id)
		    subpartition by hash (id)
		    (partition p0 values less than (42))`, true, ""},
	}
	s.RunTest(c, table)

	// Check comment content.
	parser := New()
	stmt, err := parser.ParseOneStmt("create table t (id int) partition by range (id) (partition p0 values less than (10) comment 'check')", "", "")
	c.Assert(err, IsNil)
	createTable := stmt.(*ast.CreateTableStmt)
	c.Assert(createTable.Partition.Definitions[0].Comment, Equals, "check")
}

func (s *testParserSuite) TestNotExistsSubquery(c *C) {
	table := []testCase{
		{`select * from t1 where not exists (select * from t2 where t1.a = t2.a)`, true, ""},
	}

	parser := New()
	for _, tt := range table {
		stmt, err := parser.Parse(tt.src, "", "")
		c.Assert(err, IsNil)

		sel := stmt[0].(*ast.SelectStmt)
		exists, ok := sel.Where.(*ast.ExistsSubqueryExpr)
		c.Assert(ok, IsTrue)
		c.Assert(exists.Not, Equals, tt.ok)
	}
}

func (s *testParserSuite) TestWindowFunctionIdentifier(c *C) {
	var table []testCase
	s.enableWindowFunc = true
	for key := range windowFuncTokenMap {
		table = append(table, testCase{fmt.Sprintf("select 1 %s", key), false, ""})
	}
	s.RunTest(c, table)

	s.enableWindowFunc = false
	for i := range table {
		table[i].ok = true
	}
	s.RunTest(c, table)
}

func (s *testParserSuite) TestWindowFunctions(c *C) {
	table := []testCase{
		// For window function descriptions.
		// See https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html
		{`SELECT CUME_DIST() OVER w FROM t;`, true, ""},
		{`SELECT DENSE_RANK() OVER w FROM t;`, true, ""},
		{`SELECT FIRST_VALUE(val) OVER w FROM t;`, true, ""},
		{`SELECT FIRST_VALUE(val) RESPECT NULLS OVER w FROM t;`, true, ""},
		{`SELECT FIRST_VALUE(val) IGNORE NULLS OVER w FROM t;`, true, ""},
		{`SELECT LAG(val) OVER w FROM t;`, true, ""},
		{`SELECT LAG(val, 1) OVER w FROM t;`, true, ""},
		{`SELECT LAG(val, 1, def) OVER w FROM t;`, true, ""},
		{`SELECT LAST_VALUE(val) OVER w FROM t;`, true, ""},
		{`SELECT LEAD(val) OVER w FROM t;`, true, ""},
		{`SELECT LEAD(val, 1) OVER w FROM t;`, true, ""},
		{`SELECT LEAD(val, 1, def) OVER w FROM t;`, true, ""},
		{`SELECT NTH_VALUE(val, 233) OVER w FROM t;`, true, ""},
		{`SELECT NTH_VALUE(val, 233) FROM FIRST OVER w FROM t;`, true, ""},
		{`SELECT NTH_VALUE(val, 233) FROM LAST OVER w FROM t;`, true, ""},
		{`SELECT NTH_VALUE(val, 233) FROM LAST IGNORE NULLS OVER w FROM t;`, true, ""},
		{`SELECT NTH_VALUE(val) OVER w FROM t;`, false, ""},
		{`SELECT NTILE(233) OVER w FROM t;`, true, ""},
		{`SELECT PERCENT_RANK() OVER w FROM t;`, true, ""},
		{`SELECT RANK() OVER w FROM t;`, true, ""},
		{`SELECT ROW_NUMBER() OVER w FROM t;`, true, ""},
		{`SELECT n, LAG(n, 1, 0) OVER w, LEAD(n, 1, 0) OVER w, n + LAG(n, 1, 0) OVER w FROM fib;`, true, ""},

		// For window function concepts and syntax.
		// See https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html
		{`SELECT SUM(profit) OVER(PARTITION BY country) AS country_profit FROM sales;`, true, ""},
		{`SELECT SUM(profit) OVER() AS country_profit FROM sales;`, true, ""},
		{`SELECT AVG(profit) OVER() AS country_profit FROM sales;`, true, ""},
		{`SELECT BIT_XOR(profit) OVER() AS country_profit FROM sales;`, true, ""},
		{`SELECT COUNT(profit) OVER() AS country_profit FROM sales;`, true, ""},
		{`SELECT COUNT(ALL profit) OVER() AS country_profit FROM sales;`, true, ""},
		{`SELECT COUNT(*) OVER() AS country_profit FROM sales;`, true, ""},
		{`SELECT MAX(profit) OVER() AS country_profit FROM sales;`, true, ""},
		{`SELECT MIN(profit) OVER() AS country_profit FROM sales;`, true, ""},
		{`SELECT SUM(profit) OVER() AS country_profit FROM sales;`, true, ""},
		{`SELECT ROW_NUMBER() OVER(PARTITION BY country) AS row_num1 FROM sales;`, true, ""},
		{`SELECT ROW_NUMBER() OVER(PARTITION BY country, d ORDER BY year, product) AS row_num2 FROM sales;`, true, ""},

		// For window function frame specification.
		// See https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
		{`SELECT SUM(val) OVER (PARTITION BY subject ORDER BY time ROWS UNBOUNDED PRECEDING) FROM t;`, true, ""},
		{`SELECT AVG(val) OVER (PARTITION BY subject ORDER BY time ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;`, true, ""},
		{`SELECT AVG(val) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t;`, true, ""},
		{`SELECT AVG(val) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM t;`, true, ""},
		{`SELECT AVG(val) OVER (RANGE BETWEEN INTERVAL 5 DAY PRECEDING AND INTERVAL '2:30' MINUTE_SECOND FOLLOWING) FROM t;`, true, ""},
		{`SELECT AVG(val) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM t;`, true, ""},
		{`SELECT AVG(val) OVER (RANGE CURRENT ROW) FROM t;`, true, ""},

		// For named windows.
		// See https://dev.mysql.com/doc/refman/8.0/en/window-functions-named-windows.html
		{`SELECT RANK() OVER w FROM t WINDOW w AS (ORDER BY val);`, true, ""},
		{`SELECT RANK() OVER w FROM t WINDOW w AS ();`, true, ""},
		{`SELECT FIRST_VALUE(year) OVER (w ORDER BY year ASC) AS first FROM sales WINDOW w AS (PARTITION BY country);`, true, ""},
		{`SELECT RANK() OVER w1 FROM t WINDOW w1 AS (w2), w2 AS (), w3 AS (w1);`, true, ""},
		{`SELECT RANK() OVER w1 FROM t WINDOW w1 AS (w2), w2 AS (w3), w3 AS (w1);`, true, ""},

		// For tidb_parse_tso
		{`select tidb_parse_tso(1)`, true, ""},
		{`select from_unixtime(404411537129996288)`, true, ""},
		{`select from_unixtime(404411537129996288.22)`, true, ""},
	}
	s.enableWindowFunc = true
	s.RunTest(c, table)
}

type windowFrameBoundChecker struct {
	fb         *ast.FrameBound
	exprRc     int
	timeUnitRc int
}

// Enter implements ast.Visitor interface.
func (wfc *windowFrameBoundChecker) Enter(inNode ast.Node) (outNode ast.Node, skipChildren bool) {
	if _, ok := inNode.(*ast.FrameBound); ok {
		wfc.fb = inNode.(*ast.FrameBound)
	}
	return inNode, false
}

// Leave implements ast.Visitor interface.
func (wfc *windowFrameBoundChecker) Leave(inNode ast.Node) (node ast.Node, ok bool) {
	if _, ok := inNode.(*ast.FrameBound); ok {
		wfc.fb = nil
	}
	if wfc.fb != nil {
		if inNode == wfc.fb.Expr {
			wfc.exprRc += 1
		} else if inNode == wfc.fb.Unit {
			wfc.timeUnitRc += 1
		}
	}
	return inNode, true
}

// For issue #51
// See https://github.com/pingcap/parser/pull/51 for details
func (s *testParserSuite) TestVisitFrameBound(c *C) {
	parser := New()
	parser.EnableWindowFunc(true)
	table := []struct {
		s          string
		exprRc     int
		timeUnitRc int
	}{
		{`SELECT AVG(val) OVER (RANGE INTERVAL '2:30' MINUTE_SECOND PRECEDING) FROM t;`, 1, 1},
		{`SELECT AVG(val) OVER (RANGE 5 PRECEDING) FROM t;`, 1, 0},
		{`SELECT AVG(val) OVER () FROM t;`, 0, 0},
	}
	for _, t := range table {
		stmt, err := parser.ParseOneStmt(t.s, "", "")
		c.Assert(err, IsNil)
		checker := windowFrameBoundChecker{}
		stmt.Accept(&checker)
		c.Assert(checker.exprRc, Equals, t.exprRc)
		c.Assert(checker.timeUnitRc, Equals, t.timeUnitRc)
	}

}

func (s *testParserSuite) TestFieldText(c *C) {
	parser := New()
	stmts, err := parser.Parse("select a from t", "", "")
	c.Assert(err, IsNil)
	tmp := stmts[0].(*ast.SelectStmt)
	c.Assert(tmp.Fields.Fields[0].Text(), Equals, "a")

	sqls := []string{
		"trace select a from t",
		"trace format = 'row' select a from t",
		"trace format = 'json' select a from t",
	}
	for _, sql := range sqls {
		stmts, err = parser.Parse(sql, "", "")
		c.Assert(err, IsNil)
		traceStmt := stmts[0].(*ast.TraceStmt)
		c.Assert(traceStmt.Text(), Equals, sql)
		c.Assert(traceStmt.Stmt.Text(), Equals, "select a from t")
	}
}

// CleanNodeText set the text of node and all child node empty.
// For test only.
func CleanNodeText(node ast.Node) {
	var cleaner nodeTextCleaner
	node.Accept(&cleaner)
}

// nodeTextCleaner clean the text of a node and it's child node.
// For test only.
type nodeTextCleaner struct {
}

// Enter implements Visitor interface.
func (checker *nodeTextCleaner) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	in.SetText("")
	return in, false
}

// Leave implements Visitor interface.
func (checker *nodeTextCleaner) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
