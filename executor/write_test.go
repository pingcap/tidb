// Copyright 2016 PingCAP, Inc.
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

package executor_test

import (
	"errors"
	"fmt"
	"sync/atomic"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists insert_test;create table insert_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	testSQL = `insert insert_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)

	errInsertSelectSQL := `insert insert_test (c1) values ();`
	tk.MustExec("begin")
	_, err := tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test (xxx) values (3);`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test_xxx (c1) values ();`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	insertSetSQL := `insert insert_test set c1 = 3;`
	tk.MustExec(insertSetSQL)

	errInsertSelectSQL = `insert insert_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test set xxx = 6;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	insertSelectSQL := `create table insert_test_1 (id int, c1 int);`
	tk.MustExec(insertSelectSQL)
	insertSelectSQL = `insert insert_test_1 select id, c1 from insert_test;`
	tk.MustExec(insertSelectSQL)

	insertSelectSQL = `create table insert_test_2 (id int, c1 int);`
	tk.MustExec(insertSelectSQL)
	insertSelectSQL = `insert insert_test_1 select id, c1 from insert_test union select id * 10, c1 * 10 from insert_test;`
	tk.MustExec(insertSelectSQL)

	errInsertSelectSQL = `insert insert_test_1 select c1 from insert_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	// Updating column is PK handle.
	// Make sure the record is "1, 1, nil, 1".
	r := tk.MustQuery("select * from insert_test where id = 1;")
	rowStr := fmt.Sprintf("%v %v %v %v", "1", "1", nil, "1")
	r.Check(testkit.Rows(rowStr))
	insertSQL := `insert into insert_test (id, c3) values (1, 2) on duplicate key update id=values(id), c2=10;`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "1")
	r.Check(testkit.Rows(rowStr))

	insertSQL = `insert into insert_test (id, c2) values (1, 1) on duplicate key update insert_test.c2=10;`
	tk.MustExec(insertSQL)

	_, err = tk.Exec(`insert into insert_test (id, c2) values(1, 1) on duplicate key update t.c2 = 10`)
	c.Assert(err, NotNil)

	// for on duplicate key
	insertSQL = `INSERT INTO insert_test (id, c3) VALUES (1, 2) ON DUPLICATE KEY UPDATE c3=values(c3)+c3+3;`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "6")
	r.Check(testkit.Rows(rowStr))

	tk.MustExec("create table insert_err (id int, c1 varchar(8))")
	_, err = tk.Exec("insert insert_err values (1, 'abcdabcdabcd')")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	_, err = tk.Exec("insert insert_err values (1, '你好，世界')")
	c.Assert(err, IsNil)

	tk.MustExec("create table TEST1 (ID INT NOT NULL, VALUE INT DEFAULT NULL, PRIMARY KEY (ID))")
	_, err = tk.Exec("INSERT INTO TEST1(id,value) VALUE(3,3) on DUPLICATE KEY UPDATE VALUE=4")
	c.Assert(err, IsNil)

	tk.MustExec("create table t (id int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("update t t1 set id = (select count(*) + 1 from t t2 where t1.id = t2.id)")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("2"))

	// issue 3235
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c decimal(5, 5))")
	_, err = tk.Exec("insert into t value(0)")
	c.Assert(err, IsNil)
	_, err = tk.Exec("insert into t value(1)")
	c.Assert(types.ErrOverflow.Equal(err), IsTrue)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c binary(255))")
	_, err = tk.Exec("insert into t value(1)")
	c.Assert(err, IsNil)
	r = tk.MustQuery("select length(c) from t;")
	r.Check(testkit.Rows("255"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c varbinary(255))")
	_, err = tk.Exec("insert into t value(1)")
	c.Assert(err, IsNil)
	r = tk.MustQuery("select length(c) from t;")
	r.Check(testkit.Rows("1"))

	// issue 3509
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c int)")
	tk.MustExec("set @origin_time_zone = @@time_zone")
	tk.MustExec("set @@time_zone = '+08:00'")
	_, err = tk.Exec("insert into t value(Unix_timestamp('2002-10-27 01:00'))")
	c.Assert(err, IsNil)
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("1035651600"))
	tk.MustExec("set @@time_zone = @origin_time_zone")

	// issue 3832
	tk.MustExec("create table t1 (b char(0));")
	_, err = tk.Exec(`insert into t1 values ("");`)
	c.Assert(err, IsNil)

	// issue 3895
	tk = testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4,2));")
	tk.MustExec("INSERT INTO t VALUES (1.000001);")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1265 Data Truncated"))
	tk.MustExec("INSERT INTO t VALUES (1.000000);")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows())

	// issue 4653
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a datetime);")
	_, err = tk.Exec("INSERT INTO t VALUES('2017-00-00')")
	c.Assert(err, NotNil)
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("INSERT INTO t VALUES('2017-00-00')")
	r = tk.MustQuery("SELECT * FROM t;")
	r.Check(testkit.Rows("2017-00-00 00:00:00"))
	tk.MustExec("set sql_mode = 'strict_all_tables';")
	r = tk.MustQuery("SELECT * FROM t;")
	r.Check(testkit.Rows("2017-00-00 00:00:00"))

	// test auto_increment with unsigned.
	tk.MustExec("drop table if exists test")
	tk.MustExec("CREATE TABLE test(id int(10) UNSIGNED NOT NULL AUTO_INCREMENT, p int(10) UNSIGNED NOT NULL, PRIMARY KEY(p), KEY(id))")
	tk.MustExec("insert into test(p) value(1)")
	tk.MustQuery("select * from test").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from test use index (id) where id = 1").Check(testkit.Rows("1 1"))
	tk.MustExec("insert into test values(NULL, 2)")
	tk.MustQuery("select * from test use index (id) where id = 2").Check(testkit.Rows("2 2"))
	tk.MustExec("insert into test values(2, 3)")
	tk.MustQuery("select * from test use index (id) where id = 2").Check(testkit.Rows("2 2", "2 3"))
}

func (s *testSuite) TestInsertAutoInc(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	createSQL := `drop table if exists insert_autoinc_test; create table insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createSQL)

	insertSQL := `insert into insert_autoinc_test(c1) values (1), (2)`
	tk.MustExec(insertSQL)
	tk.MustExec("begin")
	r := tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 := fmt.Sprintf("%v %v", "1", "1")
	rowStr2 := fmt.Sprintf("%v %v", "2", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertSQL = `insert into insert_autoinc_test(id, c1) values (5,5)`
	tk.MustExec(insertSQL)
	insertSQL = `insert into insert_autoinc_test(c1) values (6)`
	tk.MustExec(insertSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 := fmt.Sprintf("%v %v", "5", "5")
	rowStr4 := fmt.Sprintf("%v %v", "6", "6")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3, rowStr4))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertSQL = `insert into insert_autoinc_test(id, c1) values (3,3)`
	tk.MustExec(insertSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 := fmt.Sprintf("%v %v", "3", "3")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr5, rowStr3, rowStr4))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertSQL = `insert into insert_autoinc_test(c1) values (7)`
	tk.MustExec(insertSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 := fmt.Sprintf("%v %v", "7", "7")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr5, rowStr3, rowStr4, rowStr6))
	tk.MustExec("commit")

	// issue-962
	createSQL = `drop table if exists insert_autoinc_test; create table insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createSQL)
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0.3, 1)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 = fmt.Sprintf("%v %v", "1", "1")
	r.Check(testkit.Rows(rowStr1))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (-0.3, 2)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr2 = fmt.Sprintf("%v %v", "2", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (-3.3, 3)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 = fmt.Sprintf("%v %v", "-3", "3")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (4.3, 4)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr4 = fmt.Sprintf("%v %v", "4", "4")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4))
	insertSQL = `insert into insert_autoinc_test(c1) values (5)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 = fmt.Sprintf("%v %v", "5", "5")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4, rowStr5))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (null, 6)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 = fmt.Sprintf("%v %v", "6", "6")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4, rowStr5, rowStr6))

	// SQL_MODE=NO_AUTO_VALUE_ON_ZERO
	createSQL = `drop table if exists insert_autoinc_test; create table insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createSQL)
	insertSQL = `insert into insert_autoinc_test(id, c1) values (5, 1)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 = fmt.Sprintf("%v %v", "5", "1")
	r.Check(testkit.Rows(rowStr1))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0, 2)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr2 = fmt.Sprintf("%v %v", "6", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0, 3)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 = fmt.Sprintf("%v %v", "7", "3")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3))
	tk.MustExec("set SQL_MODE=NO_AUTO_VALUE_ON_ZERO")
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0, 4)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr4 = fmt.Sprintf("%v %v", "0", "4")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0, 5)`
	_, err := tk.Exec(insertSQL)
	// ERROR 1062 (23000): Duplicate entry '0' for key 'PRIMARY'
	c.Assert(err, NotNil)
	insertSQL = `insert into insert_autoinc_test(c1) values (6)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 = fmt.Sprintf("%v %v", "8", "6")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (null, 7)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 = fmt.Sprintf("%v %v", "9", "7")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5, rowStr6))
	tk.MustExec("set SQL_MODE='';")
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0, 8)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr7 := fmt.Sprintf("%v %v", "10", "8")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5, rowStr6, rowStr7))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (null, 9)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr8 := fmt.Sprintf("%v %v", "11", "9")
	r.Check(testkit.Rows(rowStr4, rowStr1, rowStr2, rowStr3, rowStr5, rowStr6, rowStr7, rowStr8))
}

func (s *testSuite) TestInsertIgnore(c *C) {
	var cfg kv.InjectionConfig
	tk := testkit.NewTestKit(c, kv.NewInjectedStore(s.store, &cfg))
	tk.MustExec("use test")
	testSQL := `drop table if exists t;
    create table t (id int PRIMARY KEY AUTO_INCREMENT, c1 int);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1, 2);`
	tk.MustExec(testSQL)

	r := tk.MustQuery("select * from t;")
	rowStr := fmt.Sprintf("%v %v", "1", "2")
	r.Check(testkit.Rows(rowStr))

	tk.MustExec("insert ignore into t values (1, 3), (2, 3)")

	r = tk.MustQuery("select * from t;")
	rowStr = fmt.Sprintf("%v %v", "1", "2")
	rowStr1 := fmt.Sprintf("%v %v", "2", "3")
	r.Check(testkit.Rows(rowStr, rowStr1))

	cfg.SetGetError(errors.New("foo"))
	_, err := tk.Exec("insert ignore into t values (1, 3)")
	c.Assert(err, NotNil)
	cfg.SetGetError(nil)

	// for issue 4268
	testSQL = `drop table if exists t;
	create table t (a bigint);`
	tk.MustExec(testSQL)
	testSQL = "insert ignore into t select '1a';"
	_, err = tk.Exec(testSQL)
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1265 Data Truncated"))
	testSQL = "insert ignore into t values ('1a')"
	_, err = tk.Exec(testSQL)
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1265 Data Truncated"))

	// for duplicates with warning
	testSQL = `drop table if exists t;
	create table t(a int primary key, b int);`
	tk.MustExec(testSQL)
	testSQL = "insert ignore into t values (1,1);"
	tk.MustExec(testSQL)
	_, err = tk.Exec(testSQL)
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
}

func (s *testSuite) TestReplace(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists replace_test;
    create table replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	testSQL = `replace replace_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)

	errReplaceSQL := `replace replace_test (c1) values ();`
	tk.MustExec("begin")
	_, err := tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test (xxx) values (3);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test_xxx (c1) values ();`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceSetSQL := `replace replace_test set c1 = 3;`
	tk.MustExec(replaceSetSQL)

	errReplaceSetSQL := `replace replace_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSetSQL = `replace replace_test set xxx = 6;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceSelectSQL := `create table replace_test_1 (id int, c1 int);`
	tk.MustExec(replaceSelectSQL)
	replaceSelectSQL = `replace replace_test_1 select id, c1 from replace_test;`
	tk.MustExec(replaceSelectSQL)

	replaceSelectSQL = `create table replace_test_2 (id int, c1 int);`
	tk.MustExec(replaceSelectSQL)
	replaceSelectSQL = `replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`
	tk.MustExec(replaceSelectSQL)

	errReplaceSelectSQL := `replace replace_test_1 select c1 from replace_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceUniqueIndexSQL := `create table replace_test_3 (c1 int, c2 int, UNIQUE INDEX (c2));`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	replaceUniqueIndexSQL = `replace into replace_test_3 set c1=1, c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))

	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replaceUniqueIndexSQL = `create table replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2));`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replacePrimaryKeySQL := `create table replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2));`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	// For Issue989
	issue989SQL := `CREATE TABLE tIssue989 (a int, b int, PRIMARY KEY(a), UNIQUE KEY(b));`
	tk.MustExec(issue989SQL)
	issue989SQL = `insert into tIssue989 (a, b) values (1, 2);`
	tk.MustExec(issue989SQL)
	issue989SQL = `replace into tIssue989(a, b) values (111, 2);`
	tk.MustExec(issue989SQL)
	r := tk.MustQuery("select * from tIssue989;")
	r.Check(testkit.Rows("111 2"))

	// For Issue1012
	issue1012SQL := `CREATE TABLE tIssue1012 (a int, b int, PRIMARY KEY(a), UNIQUE KEY(b));`
	tk.MustExec(issue1012SQL)
	issue1012SQL = `insert into tIssue1012 (a, b) values (1, 2);`
	tk.MustExec(issue1012SQL)
	issue1012SQL = `insert into tIssue1012 (a, b) values (2, 1);`
	tk.MustExec(issue1012SQL)
	issue1012SQL = `replace into tIssue1012(a, b) values (1, 1);`
	tk.MustExec(issue1012SQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(3))
	r = tk.MustQuery("select * from tIssue1012;")
	r.Check(testkit.Rows("1 1"))
}

func (s *testSuite) TestUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "update_test")

	updateStr := `UPDATE update_test SET name = "abc" where id > 0;`
	tk.MustExec(updateStr)
	tk.CheckExecResult(2, 0)

	// select data
	tk.MustExec("begin")
	r := tk.MustQuery(`SELECT * from update_test limit 2;`)
	r.Check(testkit.Rows("1 abc", "2 abc"))
	tk.MustExec("commit")

	tk.MustExec(`UPDATE update_test SET name = "foo"`)
	tk.CheckExecResult(2, 0)

	// table option is auto-increment
	tk.MustExec("begin")
	tk.MustExec("drop table if exists update_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create table update_test(id int not null auto_increment, name varchar(255), primary key(id))")
	tk.MustExec("insert into update_test(name) values ('aa')")
	tk.MustExec("update update_test set id = 8 where name = 'aa'")
	tk.MustExec("insert into update_test(name) values ('bb')")
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from update_test;")
	r.Check(testkit.Rows("8 aa", "9 bb"))
	tk.MustExec("commit")

	tk.MustExec("begin")
	tk.MustExec("drop table if exists update_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create table update_test(id int not null auto_increment, name varchar(255), index(id))")
	tk.MustExec("insert into update_test(name) values ('aa')")
	_, err := tk.Exec("update update_test set id = null where name = 'aa'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), DeepEquals, "Column 'id' cannot be null")

	tk.MustExec("drop table update_test")
	tk.MustExec("create table update_test(id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into update_test(id) values (1)")
	tk.MustExec("update update_test set id = 2 where id = 1 limit 1")
	r = tk.MustQuery("select * from update_test;")
	r.Check(testkit.Rows("2"))
	tk.MustExec("commit")

	// Test that in a transaction, when a constraint failed in an update statement, the record is not inserted.
	tk.MustExec("create table update_unique (id int primary key, name int unique)")
	tk.MustExec("insert update_unique values (1, 1), (2, 2);")
	tk.MustExec("begin")
	_, err = tk.Exec("update update_unique set name = 1 where id = 2")
	c.Assert(err, NotNil)
	tk.MustExec("commit")
	tk.MustQuery("select * from update_unique").Check(testkit.Rows("1 1", "2 2"))

	// test update ignore for pimary key
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint, primary key (a));")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	_, err = tk.Exec("update ignore t set a = 1 where a = 2;")
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))

	// test update ignore for truncate as warning
	_, err = tk.Exec("update ignore t set a = 1 where a = (select '2a')")
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1265 Data Truncated", "Warning 1265 Data Truncated", "Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))

	// test update ignore for unique key
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint, unique key I_uniq (a));")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	_, err = tk.Exec("update ignore t set a = 1 where a = 2;")
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 key already exist"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id integer auto_increment, t1 datetime, t2 datetime, primary key (id))")
	tk.MustExec("insert into t(t1, t2) values('2000-10-01 01:01:01', '2017-01-01 10:10:10')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2000-10-01 01:01:01 2017-01-01 10:10:10"))
	tk.MustExec("update t set t1 = '2017-10-01 10:10:11', t2 = date_add(t1, INTERVAL 10 MINUTE) where id = 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2017-10-01 10:10:11 2017-10-01 10:20:11"))

	// for issue #5132
	tk.MustExec("CREATE TABLE `tt1` (" +
		"`a` int(11) NOT NULL," +
		"`b` varchar(32) DEFAULT NULL," +
		"`c` varchar(32) DEFAULT NULL," +
		"PRIMARY KEY (`a`)," +
		"UNIQUE KEY `b_idx` (`b`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")
	tk.MustExec("insert into tt1 values(1, 'a', 'a');")
	tk.MustExec("insert into tt1 values(2, 'd', 'b');")
	r = tk.MustQuery("select * from tt1;")
	r.Check(testkit.Rows("1 a a", "2 d b"))
	tk.MustExec("update tt1 set a=5 where c='b';")
	r = tk.MustQuery("select * from tt1;")
	r.Check(testkit.Rows("1 a a", "5 d b"))

	// Automatic Updating for TIMESTAMP
	tk.MustExec("CREATE TABLE `tsup` (" +
		"`a` int," +
		"`ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
		"KEY `idx` (`ts`)" +
		");")
	tk.MustExec("insert into tsup values(1, '0000-00-00 00:00:00');")
	tk.MustExec("update tsup set a=5;")
	r1 := tk.MustQuery("select ts from tsup use index (idx);")
	r2 := tk.MustQuery("select ts from tsup;")
	r1.Check(r2.Rows())
}

// TestUpdateCastOnlyModifiedValues for issue #4514.
func (s *testSuite) TestUpdateCastOnlyModifiedValues(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table update_modified (col_1 int, col_2 enum('a', 'b'))")
	tk.MustExec("set SQL_MODE=''")
	tk.MustExec("insert into update_modified values (0, 3)")
	r := tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("0 "))
	tk.MustExec("set SQL_MODE=STRICT_ALL_TABLES")
	tk.MustExec("update update_modified set col_1 = 1")
	r = tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("1 "))
	_, err := tk.Exec("update update_modified set col_1 = 2, col_2 = 'c'")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("1 "))
	tk.MustExec("update update_modified set col_1 = 3, col_2 = 'a'")
	r = tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("3 a"))

	// Test update a field with different column type.
	tk.MustExec(`CREATE TABLE update_with_diff_type (a int, b JSON)`)
	tk.MustExec(`INSERT INTO update_with_diff_type VALUES(3, '{"a": "测试"}')`)
	tk.MustExec(`UPDATE update_with_diff_type SET a = '300'`)
	r = tk.MustQuery("SELECT a FROM update_with_diff_type")
	r.Check(testkit.Rows("300"))
	tk.MustExec(`UPDATE update_with_diff_type SET b = '{"a":   "\\u6d4b\\u8bd5"}'`)
	r = tk.MustQuery("SELECT b FROM update_with_diff_type")
	r.Check(testkit.Rows(`{"a":"测试"}`))
}

func (s *testSuite) fillMultiTableForUpdate(tk *testkit.TestKit) {
	// Create and fill table items
	tk.MustExec("CREATE TABLE items (id int, price TEXT);")
	tk.MustExec(`insert into items values (11, "items_price_11"), (12, "items_price_12"), (13, "items_price_13");`)
	tk.CheckExecResult(3, 0)
	// Create and fill table month
	tk.MustExec("CREATE TABLE month (mid int, mprice TEXT);")
	tk.MustExec(`insert into month values (11, "month_price_11"), (22, "month_price_22"), (13, "month_price_13");`)
	tk.CheckExecResult(3, 0)
}

func (s *testSuite) TestMultipleTableUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillMultiTableForUpdate(tk)

	tk.MustExec(`UPDATE items, month  SET items.price=month.mprice WHERE items.id=month.mid;`)
	tk.MustExec("begin")
	r := tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 month_price_11", "12 items_price_12", "13 month_price_13"))
	tk.MustExec("commit")

	// Single-table syntax but with multiple tables
	tk.MustExec(`UPDATE items join month on items.id=month.mid SET items.price=month.mid;`)
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 11", "12 items_price_12", "13 13"))
	tk.MustExec("commit")

	// JoinTable with alias table name.
	tk.MustExec(`UPDATE items T0 join month T1 on T0.id=T1.mid SET T0.price=T1.mprice;`)
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 month_price_11", "12 items_price_12", "13 month_price_13"))
	tk.MustExec("commit")

	// fix https://github.com/pingcap/tidb/issues/369
	testSQL := `
		DROP TABLE IF EXISTS t1, t2;
		create table t1 (c int);
		create table t2 (c varchar(256));
		insert into t1 values (1), (2);
		insert into t2 values ("a"), ("b");
		update t1, t2 set t1.c = 10, t2.c = "abc";`
	tk.MustExec(testSQL)

	// fix https://github.com/pingcap/tidb/issues/376
	testSQL = `DROP TABLE IF EXISTS t1, t2;
		create table t1 (c1 int);
		create table t2 (c2 int);
		insert into t1 values (1), (2);
		insert into t2 values (1), (2);
		update t1, t2 set t1.c1 = 10, t2.c2 = 2 where t2.c2 = 1;`
	tk.MustExec(testSQL)

	r = tk.MustQuery("select * from t1")
	r.Check(testkit.Rows("10", "10"))

	// test https://github.com/pingcap/tidb/issues/3604
	tk.MustExec("drop table if exists t, t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3)")
	tk.MustExec("update t m, t n set m.a = m.a + 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1", "3 2", "4 3"))
	tk.MustExec("update t m, t n set n.a = n.a - 1, n.b = n.b + 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "2 3", "3 4"))
}

func (s *testSuite) TestDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.fillData(tk, "delete_test")

	tk.MustExec(`update delete_test set name = "abc" where id = 2;`)
	tk.CheckExecResult(1, 0)

	tk.MustExec(`delete from delete_test where id = 2 limit 1;`)
	tk.CheckExecResult(1, 0)

	// Test delete with false condition
	tk.MustExec(`delete from delete_test where 0;`)
	tk.CheckExecResult(0, 0)

	tk.MustExec("insert into delete_test values (2, 'abc')")
	tk.MustExec(`delete from delete_test where delete_test.id = 2 limit 1`)
	tk.CheckExecResult(1, 0)

	// Select data
	tk.MustExec("begin")
	rows := tk.MustQuery(`SELECT * from delete_test limit 2;`)
	rows.Check(testkit.Rows("1 hello"))
	tk.MustExec("commit")

	// Test delete ignore
	tk.MustExec("insert into delete_test values (2, 'abc')")
	_, err := tk.Exec("delete from delete_test where id = (select '2a')")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete ignore from delete_test where id = (select '2a')")
	c.Assert(err, IsNil)
	tk.CheckExecResult(1, 0)
	r := tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1265 Data Truncated", "Warning 1265 Data Truncated"))

	tk.MustExec(`delete from delete_test ;`)
	tk.CheckExecResult(1, 0)
}

func (s *testSuite) fillDataMultiTable(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	// Create and fill table t1
	tk.MustExec("create table t1 (id int, data int);")
	tk.MustExec("insert into t1 values (11, 121), (12, 122), (13, 123);")
	tk.CheckExecResult(3, 0)
	// Create and fill table t2
	tk.MustExec("create table t2 (id int, data int);")
	tk.MustExec("insert into t2 values (11, 221), (22, 222), (23, 223);")
	tk.CheckExecResult(3, 0)
	// Create and fill table t3
	tk.MustExec("create table t3 (id int, data int);")
	tk.MustExec("insert into t3 values (11, 321), (22, 322), (23, 323);")
	tk.CheckExecResult(3, 0)
}

func (s *testSuite) TestMultiTableDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.fillDataMultiTable(tk)

	tk.MustExec(`delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;`)
	tk.CheckExecResult(2, 0)

	// Select data
	r := tk.MustQuery("select * from t3")
	c.Assert(r.Rows(), HasLen, 3)
}

func (s *testSuite) TestQualifiedDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (c1 int, c2 int, index (c1))")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("insert into t1 values (1, 1), (2, 2)")

	// delete with index
	tk.MustExec("delete from t1 where t1.c1 = 1")
	tk.CheckExecResult(1, 0)

	// delete with no index
	tk.MustExec("delete from t1 where t1.c2 = 2")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("select * from t1")
	c.Assert(r.Rows(), HasLen, 0)

	_, err := tk.Exec("delete from t1 as a where a.c1 = 1")
	c.Assert(err, NotNil)

	tk.MustExec("insert into t1 values (1, 1), (2, 2)")
	tk.MustExec("insert into t2 values (2, 1), (3,1)")
	tk.MustExec("delete t1, t2 from t1 join t2 where t1.c1 = t2.c2")
	tk.CheckExecResult(3, 0)

	tk.MustExec("insert into t2 values (2, 1), (3,1)")
	tk.MustExec("delete a, b from t1 as a join t2 as b where a.c2 = b.c1")
	tk.CheckExecResult(2, 0)

	_, err = tk.Exec("delete t1, t2 from t1 as a join t2 as b where a.c2 = b.c1")
	c.Assert(err, NotNil)
}

func (s *testSuite) TestLoadData(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	createSQL := `drop table if exists load_data_test;
		create table load_data_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 varchar(255) default "def", c3 int);`
	_, err := tk.Exec("load data local infile '/tmp/nonexistence.csv' into table load_data_test")
	c.Assert(err, NotNil)
	tk.MustExec(createSQL)
	_, err = tk.Exec("load data infile '/tmp/nonexistence.csv' into table load_data_test")
	c.Assert(err, NotNil)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table load_data_test")
	ctx := tk.Se.(context.Context)
	ld := makeLoadDataInfo(4, nil, ctx, c)

	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	// data1 = nil, data2 = nil, fields and lines is default
	_, reachLimit, err := ld.InsertData(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(reachLimit, IsFalse)
	r := tk.MustQuery(selectSQL)
	r.Check(nil)

	sc := ctx.GetSessionVars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
	}()
	sc.IgnoreTruncate = false
	// fields and lines are default, InsertData returns data is nil
	tests := []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("\n"), []string{"1|0||0"}, nil},
		{nil, []byte("\t\n"), []string{"2|0||0"}, nil},
		{nil, []byte("3\t2\t3\t4\n"), []string{"3|2|3|4"}, nil},
		{nil, []byte("3*1\t2\t3\t4\n"), []string{"3|2|3|4"}, nil},
		{nil, []byte("4\t2\t\t3\t4\n"), []string{"4|2||3"}, nil},
		{nil, []byte("\t1\t2\t3\t4\n"), []string{"5|1|2|3"}, nil},
		{nil, []byte("6\t2\t3\n"), []string{"6|2|3|0"}, nil},
		{nil, []byte("\t2\t3\t4\n\t22\t33\t44\n"), []string{"7|2|3|4", "8|22|33|44"}, nil},
		{nil, []byte("7\t2\t3\t4\n7\t22\t33\t44\n"), []string{"7|2|3|4"}, nil},

		// data1 != nil, data2 = nil
		{[]byte("\t2\t3\t4"), nil, []string{"9|2|3|4"}, nil},

		// data1 != nil, data2 != nil
		{[]byte("\t2\t3"), []byte("\t4\t5\n"), []string{"10|2|3|4"}, nil},
		{[]byte("\t2\t3"), []byte("4\t5\n"), []string{"11|2|34|5"}, nil},

		// data1 != nil, data2 != nil, InsertData returns data isn't nil
		{[]byte("\t2\t3"), []byte("\t4\t5"), nil, []byte("\t2\t3\t4\t5")},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
	c.Assert(sc.WarningCount(), Equals, uint16(3))

	// lines starting symbol is "" and terminated symbol length is 2, InsertData returns data is nil
	ld.LinesInfo.Terminated = "||"
	tests = []testCase{
		// data1 != nil, data2 != nil
		{[]byte("0\t2\t3"), []byte("\t4\t5||"), []string{"12|2|3|4"}, nil},
		{[]byte("1\t2\t3\t4\t5|"), []byte("|"), []string{"1|2|3|4"}, nil},
		{[]byte("2\t2\t3\t4\t5|"), []byte("|3\t22\t33\t44\t55||"),
			[]string{"2|2|3|4", "3|22|33|44"}, nil},
		{[]byte("3\t2\t3\t4\t5|"), []byte("|4\t22\t33||"), []string{
			"3|2|3|4", "4|22|33|0"}, nil},
		{[]byte("4\t2\t3\t4\t5|"), []byte("|5\t22\t33||6\t222||"),
			[]string{"4|2|3|4", "5|22|33|0", "6|222||0"}, nil},
		{[]byte("6\t2\t3"), []byte("4\t5||"), []string{"6|2|34|5"}, nil},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

	// fields and lines aren't default, InsertData returns data is nil
	ld.FieldsInfo.Terminated = "\\"
	ld.LinesInfo.Starting = "xxx"
	ld.LinesInfo.Terminated = "|!#^"
	tests = []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("xxx|!#^"), []string{"13|0||0"}, nil},
		{nil, []byte("xxx\\|!#^"), []string{"14|0||0"}, nil},
		{nil, []byte("xxx3\\2\\3\\4|!#^"), []string{"3|2|3|4"}, nil},
		{nil, []byte("xxx4\\2\\\\3\\4|!#^"), []string{"4|2||3"}, nil},
		{nil, []byte("xxx\\1\\2\\3\\4|!#^"), []string{"15|1|2|3"}, nil},
		{nil, []byte("xxx6\\2\\3|!#^"), []string{"6|2|3|0"}, nil},
		{nil, []byte("xxx\\2\\3\\4|!#^xxx\\22\\33\\44|!#^"), []string{
			"16|2|3|4",
			"17|22|33|44"}, nil},
		{nil, []byte("\\2\\3\\4|!#^\\22\\33\\44|!#^xxx\\222\\333\\444|!#^"), []string{
			"18|222|333|444"}, nil},

		// data1 != nil, data2 = nil
		{[]byte("xxx\\2\\3\\4"), nil, []string{"19|2|3|4"}, nil},
		{[]byte("\\2\\3\\4|!#^"), nil, []string{}, nil},
		{[]byte("\\2\\3\\4|!#^xxx18\\22\\33\\44|!#^"), nil,
			[]string{"18|22|33|44"}, nil},

		// data1 != nil, data2 != nil
		{[]byte("xxx10\\2\\3"), []byte("\\4|!#^"),
			[]string{"10|2|3|4"}, nil},
		{[]byte("10\\2\\3xx"), []byte("x11\\4\\5|!#^"),
			[]string{"11|4|5|0"}, nil},
		{[]byte("xxx21\\2\\3\\4\\5|!"), []byte("#^"),
			[]string{"21|2|3|4"}, nil},
		{[]byte("xxx22\\2\\3\\4\\5|!"), []byte("#^xxx23\\22\\33\\44\\55|!#^"),
			[]string{"22|2|3|4", "23|22|33|44"}, nil},
		{[]byte("xxx23\\2\\3\\4\\5|!"), []byte("#^xxx24\\22\\33|!#^"),
			[]string{"23|2|3|4", "24|22|33|0"}, nil},
		{[]byte("xxx24\\2\\3\\4\\5|!"), []byte("#^xxx25\\22\\33|!#^xxx26\\222|!#^"),
			[]string{"24|2|3|4", "25|22|33|0", "26|222||0"}, nil},
		{[]byte("xxx25\\2\\3\\4\\5|!"), []byte("#^26\\22\\33|!#^xxx27\\222|!#^"),
			[]string{"25|2|3|4", "27|222||0"}, nil},
		{[]byte("xxx\\2\\3"), []byte("4\\5|!#^"), []string{"28|2|34|5"}, nil},

		// InsertData returns data isn't nil
		{nil, []byte("\\2\\3\\4|!#^"), nil, []byte("#^")},
		{nil, []byte("\\4\\5"), nil, []byte("\\5")},
		{[]byte("\\2\\3"), []byte("\\4\\5"), nil, []byte("\\5")},
		{[]byte("xxx1\\2\\3|"), []byte("!#^\\4\\5|!#"),
			[]string{"1|2|3|0"}, []byte("!#")},
		{[]byte("xxx1\\2\\3\\4\\5|!"), []byte("#^xxx2\\22\\33|!#^3\\222|!#^"),
			[]string{"1|2|3|4", "2|22|33|0"}, []byte("#^")},
		{[]byte("xx1\\2\\3"), []byte("\\4\\5|!#^"), nil, []byte("#^")},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

	// lines starting symbol is the same as terminated symbol, InsertData returns data is nil
	ld.LinesInfo.Terminated = "xxx"
	tests = []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("xxxxxx"), []string{"29|0||0"}, nil},
		{nil, []byte("xxx3\\2\\3\\4xxx"), []string{"3|2|3|4"}, nil},
		{nil, []byte("xxx\\2\\3\\4xxxxxx\\22\\33\\44xxx"),
			[]string{"30|2|3|4", "31|22|33|44"}, nil},

		// data1 != nil, data2 = nil
		{[]byte("xxx\\2\\3\\4"), nil, []string{"32|2|3|4"}, nil},

		// data1 != nil, data2 != nil
		{[]byte("xxx10\\2\\3"), []byte("\\4\\5xxx"), []string{"10|2|3|4"}, nil},
		{[]byte("xxxxx10\\2\\3"), []byte("\\4\\5xxx"), []string{"33|2|3|4"}, nil},
		{[]byte("xxx21\\2\\3\\4\\5xx"), []byte("x"), []string{"21|2|3|4"}, nil},
		{[]byte("xxx32\\2\\3\\4\\5x"), []byte("xxxxx33\\22\\33\\44\\55xxx"),
			[]string{"32|2|3|4", "33|22|33|44"}, nil},
		{[]byte("xxx33\\2\\3\\4\\5xxx"), []byte("xxx34\\22\\33xxx"),
			[]string{"33|2|3|4", "34|22|33|0"}, nil},
		{[]byte("xxx34\\2\\3\\4\\5xx"), []byte("xxxx35\\22\\33xxxxxx36\\222xxx"),
			[]string{"34|2|3|4", "35|22|33|0", "36|222||0"}, nil},

		// InsertData returns data isn't nil
		{nil, []byte("\\2\\3\\4xxxx"), nil, []byte("xxxx")},
		{[]byte("\\2\\3\\4xxx"), nil, []string{"37|0||0"}, nil},
		{[]byte("\\2\\3\\4xxxxxx11\\22\\33\\44xxx"), nil,
			[]string{"38|0||0", "39|0||0"}, nil},
		{[]byte("xx10\\2\\3"), []byte("\\4\\5xxx"), nil, []byte("xxx")},
		{[]byte("xxx10\\2\\3"), []byte("\\4xxxx"), []string{"10|2|3|4"}, []byte("x")},
		{[]byte("xxx10\\2\\3\\4\\5x"), []byte("xx11\\22\\33xxxxxx12\\222xxx"),
			[]string{"10|2|3|4", "40|0||0"}, []byte("xxx")},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

func (s *testSuite) TestLoadDataEscape(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table load_data_test")
	ctx := tk.Se.(context.Context)
	ld := makeLoadDataInfo(2, nil, ctx, c)
	// test escape
	tests := []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("1\ta string\n"), []string{"1|a string"}, nil},
		{nil, []byte("2\tstr \\t\n"), []string{"2|str \t"}, nil},
		{nil, []byte("3\tstr \\n\n"), []string{"3|str \n"}, nil},
		{nil, []byte("4\tboth \\t\\n\n"), []string{"4|both \t\n"}, nil},
		{nil, []byte("5\tstr \\\\\n"), []string{"5|str \\"}, nil},
		{nil, []byte("6\t\\r\\t\\n\\0\\Z\\b\n"), []string{"6|" + string([]byte{'\r', '\t', '\n', 0, 26, '\b'})}, nil},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

// TestLoadDataSpecifiedCoumns reuse TestLoadDataEscape's test case :-)
func (s *testSuite) TestLoadDataSpecifiedCoumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec(`create table load_data_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 varchar(255) default "def", c3 int default 0);`)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table load_data_test (c1, c2)")
	ctx := tk.Se.(context.Context)
	ld := makeLoadDataInfo(2, []string{"c1", "c2"}, ctx, c)
	// test
	tests := []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("7\ta string\n"), []string{"1|7|a string|0"}, nil},
		{nil, []byte("8\tstr \\t\n"), []string{"2|8|str \t|0"}, nil},
		{nil, []byte("9\tstr \\n\n"), []string{"3|9|str \n|0"}, nil},
		{nil, []byte("10\tboth \\t\\n\n"), []string{"4|10|both \t\n|0"}, nil},
		{nil, []byte("11\tstr \\\\\n"), []string{"5|11|str \\|0"}, nil},
		{nil, []byte("12\t\\r\\t\\n\\0\\Z\\b\n"), []string{"6|12|" + string([]byte{'\r', '\t', '\n', 0, 26, '\b'}) + "|0"}, nil},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

func makeLoadDataInfo(column int, specifiedColumns []string, ctx context.Context, c *C) (ld *executor.LoadDataInfo) {
	dom := domain.GetDomain(ctx)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("load_data_test"))
	c.Assert(err, IsNil)
	columns := tbl.Cols()
	// filter specified columns
	if len(specifiedColumns) > 0 {
		columns, err = table.FindCols(columns, specifiedColumns)
		c.Assert(err, IsNil)
	}
	fields := &ast.FieldsClause{Terminated: "\t"}
	lines := &ast.LinesClause{Starting: "", Terminated: "\n"}
	ld = executor.NewLoadDataInfo(make([]types.Datum, column), ctx, tbl, columns)
	ld.SetBatchCount(0)
	ld.FieldsInfo = fields
	ld.LinesInfo = lines
	return
}

func (s *testSuite) TestBatchInsertDelete(c *C) {
	originLimit := atomic.LoadUint64(&kv.TxnEntryCountLimit)
	defer func() {
		atomic.StoreUint64(&kv.TxnEntryCountLimit, originLimit)
	}()
	// Set the limitation to a small value, make it easier to reach the limitation.
	atomic.StoreUint64(&kv.TxnEntryCountLimit, 100)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists batch_insert")
	tk.MustExec("create table batch_insert (c int)")
	tk.MustExec("drop table if exists batch_insert_on_duplicate")
	tk.MustExec("create table batch_insert_on_duplicate (id int primary key, c int)")
	// Insert 10 rows.
	tk.MustExec("insert into batch_insert values (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)")
	r := tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("10"))
	// Insert 10 rows.
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("20"))
	// Insert 20 rows.
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("40"))
	// Insert 40 rows.
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("80"))
	// Insert 80 rows.
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("160"))
	// for on duplicate key
	for i := 0; i < 160; i++ {
		tk.MustExec(fmt.Sprintf("insert into batch_insert_on_duplicate values(%d, %d);", i, i))
	}
	r = tk.MustQuery("select count(*) from batch_insert_on_duplicate;")
	r.Check(testkit.Rows("160"))

	// This will meet txn too large error.
	_, err := tk.Exec("insert into batch_insert (c) select * from batch_insert;")
	c.Assert(err, NotNil)
	c.Assert(kv.ErrTxnTooLarge.Equal(err), IsTrue)
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("160"))

	// for on duplicate key
	_, err = tk.Exec(`insert into batch_insert_on_duplicate select * from batch_insert_on_duplicate as tt
		on duplicate key update batch_insert_on_duplicate.id=batch_insert_on_duplicate.id+1000;`)
	c.Assert(err, NotNil)
	c.Assert(kv.ErrTxnTooLarge.Equal(err), IsTrue, Commentf("%v", err))
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("160"))

	// Change to batch inset mode and batch size to 50.
	tk.MustExec("set @@session.tidb_batch_insert=1;")
	tk.MustExec("set @@session.tidb_dml_batch_size=50;")
	tk.MustExec("insert into batch_insert (c) select * from batch_insert;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("320"))

	// Enlarge the batch size to 150 which is larger than the txn limitation (100).
	// So the insert will meet error.
	tk.MustExec("set @@session.tidb_dml_batch_size=150;")
	_, err = tk.Exec("insert into batch_insert (c) select * from batch_insert;")
	c.Assert(err, NotNil)
	c.Assert(kv.ErrTxnTooLarge.Equal(err), IsTrue)
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("320"))
	// Set it back to 50.
	tk.MustExec("set @@session.tidb_dml_batch_size=50;")

	// for on duplicate key
	_, err = tk.Exec(`insert into batch_insert_on_duplicate select * from batch_insert_on_duplicate as tt
		on duplicate key update batch_insert_on_duplicate.id=batch_insert_on_duplicate.id+1000;`)
	c.Assert(err, IsNil)
	r = tk.MustQuery("select count(*) from batch_insert_on_duplicate;")
	r.Check(testkit.Rows("160"))

	// Disable BachInsert mode in transition.
	tk.MustExec("begin;")
	_, err = tk.Exec("insert into batch_insert (c) select * from batch_insert;")
	c.Assert(err, NotNil)
	c.Assert(kv.ErrTxnTooLarge.Equal(err), IsTrue)
	tk.MustExec("rollback;")
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("320"))

	// Test case for batch delete.
	// This will meet txn too large error.
	_, err = tk.Exec("delete from batch_insert;")
	c.Assert(err, NotNil)
	c.Assert(kv.ErrTxnTooLarge.Equal(err), IsTrue)
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("320"))
	// Enable batch delete and set batch size to 50.
	tk.MustExec("set @@session.tidb_batch_delete=on;")
	tk.MustExec("set @@session.tidb_dml_batch_size=50;")
	tk.MustExec("delete from batch_insert;")
	// Make sure that all rows are gone.
	r = tk.MustQuery("select count(*) from batch_insert;")
	r.Check(testkit.Rows("0"))
}

func (s *testSuite) TestNullDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists test_null_default;")
	tk.MustExec("set timestamp = 1234")
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustExec("create table test_null_default (ts timestamp null default current_timestamp)")
	tk.MustExec("insert into test_null_default values (null)")
	tk.MustQuery("select * from test_null_default").Check(testkit.Rows("<nil>"))
	tk.MustExec("insert into test_null_default values ()")
	tk.MustQuery("select * from test_null_default").Check(testkit.Rows("<nil>", "1970-01-01 08:20:34"))
}

func (s *testSuite) TestGetFieldsFromLine(c *C) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			`"1","a string","100.20"`,
			[]string{"1", "a string", "100.20"},
		},
		{
			`"2","a string containing a , comma","102.20"`,
			[]string{"2", "a string containing a , comma", "102.20"},
		},
		{
			`"3","a string containing a \" quote","102.20"`,
			[]string{"3", "a string containing a \" quote", "102.20"},
		},
		{
			`"4","a string containing a \", quote and comma","102.20"`,
			[]string{"4", "a string containing a \", quote and comma", "102.20"},
		},
		// Test some escape char.
		{
			`"\0\b\n\r\t\Z\\\  \c\'\""`,
			[]string{string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'})},
		},
	}
	fieldsInfo := &ast.FieldsClause{
		Enclosed:   '"',
		Terminated: ",",
	}

	for _, test := range tests {
		got, err := executor.GetFieldsFromLine([]byte(test.input), fieldsInfo)
		c.Assert(err, IsNil, Commentf("failed: %s", test.input))
		assertEqualStrings(c, got, test.expected)
	}

	_, err := executor.GetFieldsFromLine([]byte(`1,a string,100.20`), fieldsInfo)
	c.Assert(err, NotNil)
}

func assertEqualStrings(c *C, got []string, expect []string) {
	c.Assert(len(got), Equals, len(expect))
	for i := 0; i < len(got); i++ {
		c.Assert(got[i], Equals, expect[i])
	}
}

// TestIssue4067 Test issue https://github.com/pingcap/tidb/issues/4067
func (s *testSuite) TestIssue4067(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")
	tk.MustExec("insert into t1 values(123)")
	tk.MustExec("insert into t2 values(123)")
	tk.MustExec("delete from t1 where id not in (select id from t2)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("123"))
	tk.MustExec("delete from t1 where id in (select id from t2)")
	tk.MustQuery("select * from t1").Check(nil)
}

func (s *testSuite) TestInsertCalculatedValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t set a=1, b=a+1")
	tk.MustQuery("select a, b from t").Check(testkit.Rows("1 2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default 100, b int)")
	tk.MustExec("insert into t set b=a+1, a=1")
	tk.MustQuery("select a, b from t").Check(testkit.Rows("1 101"))
	tk.MustExec("insert into t (b) value (a)")
	tk.MustQuery("select * from t where b = 100").Check(testkit.Rows("100 100"))
	tk.MustExec("insert into t set a=2, b=a+1")
	tk.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustExec("insert into test.t set test.t.c = '1'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default 1)")
	tk.MustExec("insert into t values (a)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, d int)")
	tk.MustExec("insert into t value (1, 2, a+1, b+1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2 2 3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int not null)")
	tk.MustExec("insert into t values (a+2)")
	tk.MustExec("insert into t values (a)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("0", "2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a bigint not null, b bigint not null)")
	tk.MustExec("insert into t value(b + 1, a)")
	tk.MustExec("insert into t set a = b + a, b = a + 1")
	tk.MustExec("insert into t value(1000, a)")
	tk.MustExec("insert t set b = sqrt(a + 4), a = 10")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("0 1", "1 1", "10 2", "1000 1000"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(a)")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil>"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a enum('a', 'b'))")
	tk.MustExec("insert into t values(a)")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil>"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a enum('a', 'b') default 'a')")
	tk.MustExec("insert into t values(a)")
	tk.MustExec("insert into t values(a+1)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("a", "b"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a blob)")
	tk.MustExec("insert into t values(a)")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil>"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(20) default 'a')")
	tk.MustExec("insert into t values(a)")
	tk.MustExec("insert into t values(upper(a))")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("A", "a"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(20) not null, b varchar(20))")
	tk.MustExec("insert into t value (a, b)")
	tk.MustQuery("select * from t").Check(testkit.Rows(" <nil>"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(a*b, b*b)")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil> <nil>"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a json not null, b int)")
	tk.MustExec("insert into t value (a,a->'$')")
	tk.MustQuery("select * from t").Check(testkit.Rows("null 0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a json, b int, c int as (a->'$.a'))")
	tk.MustExec("insert into t (a, b) value (a, a->'$.a'+1)")
	tk.MustExec("insert into t (b) value (a->'$.a'+1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil> <nil> <nil>", "<nil> <nil> <nil>"))
	tk.MustExec(`insert into t (a, b) value ('{"a": 1}', a->'$.a'+1)`)
	tk.MustQuery("select * from t where c = 1").Check(testkit.Rows(`{"a":1} 2 1`))
	tk.MustExec("truncate table t")
	tk.MustExec("insert t set b = c + 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustExec("truncate table t")
	tk.MustExec(`insert t set a = '{"a": 1}', b = c`)
	tk.MustQuery("select * from t").Check(testkit.Rows(`{"a":1} <nil> 1`))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int auto_increment key, b int)")
	tk.MustExec("insert into t (b) value (a)")
	tk.MustExec("insert into t value (a, a+1)")
	tk.MustExec("set SQL_MODE=NO_AUTO_VALUE_ON_ZERO")
	tk.MustExec("insert into t (b) value (a+1)")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("1 0", "2 1", "3 1"))

	tk.MustExec("set SQL_MODE=STRICT_ALL_TABLES")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int, c int as (sqrt(a)))")
	tk.MustExec("insert t set b = a, a = 4")
	tk.MustQuery("select * from t").Check(testkit.Rows("4 0 2"))
}
