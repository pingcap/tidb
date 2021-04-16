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
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

type testBypassSuite struct{}

func (s *testBypassSuite) SetUpSuite(c *C) {
}

func (s *testSuite) TestInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists insert_test;create table insert_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	testSQL = `insert insert_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("Records: 3  Duplicates: 0  Warnings: 0")

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
	tk.CheckLastMessage("")

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
	tk.CheckLastMessage("Records: 4  Duplicates: 0  Warnings: 0")

	insertSelectSQL = `create table insert_test_2 (id int, c1 int);`
	tk.MustExec(insertSelectSQL)
	insertSelectSQL = `insert insert_test_1 select id, c1 from insert_test union select id * 10, c1 * 10 from insert_test;`
	tk.MustExec(insertSelectSQL)
	tk.CheckLastMessage("Records: 8  Duplicates: 0  Warnings: 0")

	errInsertSelectSQL = `insert insert_test_1 select c1 from insert_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test_1 values(default, default, default, default, default)`
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
	tk.CheckLastMessage("")
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "1")
	r.Check(testkit.Rows(rowStr))

	insertSQL = `insert into insert_test (id, c2) values (1, 1) on duplicate key update insert_test.c2=10;`
	tk.MustExec(insertSQL)
	tk.CheckLastMessage("")

	_, err = tk.Exec(`insert into insert_test (id, c2) values(1, 1) on duplicate key update t.c2 = 10`)
	c.Assert(err, NotNil)

	// for on duplicate key
	insertSQL = `INSERT INTO insert_test (id, c3) VALUES (1, 2) ON DUPLICATE KEY UPDATE c3=values(c3)+c3+3;`
	tk.MustExec(insertSQL)
	tk.CheckLastMessage("")
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "6")
	r.Check(testkit.Rows(rowStr))

	// for on duplicate key with ignore
	insertSQL = `INSERT IGNORE INTO insert_test (id, c3) VALUES (1, 2) ON DUPLICATE KEY UPDATE c3=values(c3)+c3+3;`
	tk.MustExec(insertSQL)
	tk.CheckLastMessage("")
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "11")
	r.Check(testkit.Rows(rowStr))

	tk.MustExec("create table insert_err (id int, c1 varchar(8))")
	_, err = tk.Exec("insert insert_err values (1, 'abcdabcdabcd')")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	_, err = tk.Exec("insert insert_err values (1, '你好，世界')")
	c.Assert(err, IsNil)

	tk.MustExec("create table TEST1 (ID INT NOT NULL, VALUE INT DEFAULT NULL, PRIMARY KEY (ID))")
	_, err = tk.Exec("INSERT INTO TEST1(id,value) VALUE(3,3) on DUPLICATE KEY UPDATE VALUE=4")
	c.Assert(err, IsNil)
	tk.CheckLastMessage("")

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
	c.Assert(types.ErrWarnDataOutOfRange.Equal(err), IsTrue)

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
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect DECIMAL value: '1.000001'"))
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

	// issue 6360
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint unsigned);")
	tk.MustExec(" set @orig_sql_mode = @@sql_mode; set @@sql_mode = 'strict_all_tables';")
	_, err = tk.Exec("insert into t value (-1);")
	c.Assert(types.ErrWarnDataOutOfRange.Equal(err), IsTrue)
	tk.MustExec("set @@sql_mode = '';")
	tk.MustExec("insert into t value (-1);")
	// TODO: the following warning messages are not consistent with MySQL, fix them in the future PRs
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 constant -1 overflows bigint"))
	tk.MustExec("insert into t select -1;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 constant -1 overflows bigint"))
	tk.MustExec("insert into t select cast(-1 as unsigned);")
	tk.MustExec("insert into t value (-1.111);")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 constant -1.111 overflows bigint"))
	tk.MustExec("insert into t value ('-1.111');")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 BIGINT UNSIGNED value is out of range in '-1'"))
	tk.MustExec("update t set a = -1 limit 1;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 constant -1 overflows bigint"))
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("0", "0", "18446744073709551615", "0", "0"))
	tk.MustExec("set @@sql_mode = @orig_sql_mode;")

	// issue 6424 & issue 20207
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a time(6))")
	tk.MustExec("insert into t value('20070219173709.055870'), ('20070219173709.055'), ('20070219173709.055870123')")
	tk.MustQuery("select * from t").Check(testkit.Rows("17:37:09.055870", "17:37:09.055000", "17:37:09.055870"))
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t value(20070219173709.055870), (20070219173709.055), (20070219173709.055870123)")
	tk.MustQuery("select * from t").Check(testkit.Rows("17:37:09.055870", "17:37:09.055000", "17:37:09.055870"))
	_, err = tk.Exec("insert into t value(-20070219173709.055870)")
	c.Assert(err.Error(), Equals, "[table:1292]Incorrect time value: '-20070219173709.055870' for column 'a' at row 1")

	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("create table t(a float unsigned, b double unsigned)")
	tk.MustExec("insert into t value(-1.1, -1.1), (-2.1, -2.1), (0, 0), (1.1, 1.1)")
	tk.MustQuery("show warnings").
		Check(testkit.Rows("Warning 1690 constant -1.1 overflows float", "Warning 1690 constant -1.1 overflows double",
			"Warning 1690 constant -2.1 overflows float", "Warning 1690 constant -2.1 overflows double"))
	tk.MustQuery("select * from t").Check(testkit.Rows("0 0", "0 0", "0 0", "1.1 1.1"))

	// issue 7061
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default 1, b int default 2)")
	tk.MustExec("insert into t values(default, default)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(default(b), default(a))")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1"))
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t (b) values(default)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t (b) values(default(a))")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))

	tk.MustExec("create view v as select * from t")
	_, err = tk.Exec("insert into v values(1,2)")
	c.Assert(err.Error(), Equals, "insert into view v is not supported now.")
	_, err = tk.Exec("replace into v values(1,2)")
	c.Assert(err.Error(), Equals, "replace into view v is not supported now.")
	tk.MustExec("drop view v")

	tk.MustExec("create sequence seq")
	_, err = tk.Exec("insert into seq values()")
	c.Assert(err.Error(), Equals, "insert into sequence seq is not supported now.")
	_, err = tk.Exec("replace into seq values()")
	c.Assert(err.Error(), Equals, "replace into sequence seq is not supported now.")
	tk.MustExec("drop sequence seq")

	// issue 22851
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(name varchar(255), b int, c int, primary key(name(2)))")
	tk.MustExec("insert into t(name, b) values(\"cha\", 3)")
	_, err = tk.Exec("insert into t(name, b) values(\"chb\", 3)")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry 'ch' for key 'PRIMARY'")
	tk.MustExec("insert into t(name, b) values(\"测试\", 3)")
	_, err = tk.Exec("insert into t(name, b) values(\"测试\", 3)")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '测试' for key 'PRIMARY'")
}

func (s *testSuiteP2) TestMultiBatch(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t,t0")
	tk.MustExec("create table t0 (i int)")
	tk.MustExec("insert into t0 values (1), (1)")
	tk.MustExec("create table t (i int unique key)")
	tk.MustExec("set @@tidb_dml_batch_size = 1")
	tk.MustExec("insert ignore into t select * from t0")
	tk.MustExec("admin check table t")
}

func (s *testSuite4) TestInsertAutoInc(c *C) {
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

func (s *testSuite4) TestInsertIgnore(c *C) {
	var cfg kv.InjectionConfig
	tk := testkit.NewTestKit(c, kv.NewInjectedStore(s.store, &cfg))
	tk.MustExec("use test")
	testSQL := `drop table if exists t;
    create table t (id int PRIMARY KEY AUTO_INCREMENT, c1 int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1, 2);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")

	r := tk.MustQuery("select * from t;")
	rowStr := fmt.Sprintf("%v %v", "1", "2")
	r.Check(testkit.Rows(rowStr))

	tk.MustExec("insert ignore into t values (1, 3), (2, 3)")
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 1")
	r = tk.MustQuery("select * from t;")
	rowStr1 := fmt.Sprintf("%v %v", "2", "3")
	r.Check(testkit.Rows(rowStr, rowStr1))

	tk.MustExec("insert ignore into t values (3, 4), (3, 4)")
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 1")
	r = tk.MustQuery("select * from t;")
	rowStr2 := fmt.Sprintf("%v %v", "3", "4")
	r.Check(testkit.Rows(rowStr, rowStr1, rowStr2))

	tk.MustExec("begin")
	tk.MustExec("insert ignore into t values (4, 4), (4, 5), (4, 6)")
	tk.CheckLastMessage("Records: 3  Duplicates: 2  Warnings: 2")
	r = tk.MustQuery("select * from t;")
	rowStr3 := fmt.Sprintf("%v %v", "4", "5")
	r.Check(testkit.Rows(rowStr, rowStr1, rowStr2, rowStr3))
	tk.MustExec("commit")

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
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '1a'"))
	testSQL = "insert ignore into t values ('1a')"
	_, err = tk.Exec(testSQL)
	c.Assert(err, IsNil)
	tk.CheckLastMessage("")
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '1a'"))

	// for duplicates with warning
	testSQL = `drop table if exists t;
	create table t(a int primary key, b int);`
	tk.MustExec(testSQL)
	testSQL = "insert ignore into t values (1,1);"
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	_, err = tk.Exec(testSQL)
	tk.CheckLastMessage("")
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))

	testSQL = `drop table if exists test;
create table test (i int primary key, j int unique);
begin;
insert into test values (1,1);
insert ignore into test values (2,1);
commit;`
	tk.MustExec(testSQL)
	testSQL = `select * from test;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 1"))

	testSQL = `delete from test;
insert into test values (1, 1);
begin;
delete from test where i = 1;
insert ignore into test values (2, 1);
commit;`
	tk.MustExec(testSQL)
	testSQL = `select * from test;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("2 1"))

	testSQL = `delete from test;
insert into test values (1, 1);
begin;
update test set i = 2, j = 2 where i = 1;
insert ignore into test values (1, 3);
insert ignore into test values (2, 4);
commit;`
	tk.MustExec(testSQL)
	testSQL = `select * from test order by i;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 3", "2 2"))

	testSQL = `create table badnull (i int not null)`
	tk.MustExec(testSQL)
	testSQL = `insert ignore into badnull values (null)`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'i' cannot be null"))
	testSQL = `select * from badnull`
	tk.MustQuery(testSQL).Check(testkit.Rows("0"))

	tk.MustExec("create table tp (id int) partition by range (id) (partition p0 values less than (1), partition p1 values less than(2))")
	tk.MustExec("insert ignore into tp values (1), (3)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1526 Table has no partition for value 3"))
}

func (s *testSuite8) TestInsertOnDup(c *C) {
	var cfg kv.InjectionConfig
	tk := testkit.NewTestKit(c, kv.NewInjectedStore(s.store, &cfg))
	tk.MustExec("use test")
	testSQL := `drop table if exists t;
    create table t (i int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1),(2);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")

	r := tk.MustQuery("select * from t;")
	rowStr1 := fmt.Sprintf("%v", "1")
	rowStr2 := fmt.Sprintf("%v", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))

	tk.MustExec("insert into t values (1), (2) on duplicate key update i = values(i)")
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows(rowStr1, rowStr2))

	tk.MustExec("insert into t values (2), (3) on duplicate key update i = 3")
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 0")
	r = tk.MustQuery("select * from t;")
	rowStr3 := fmt.Sprintf("%v", "3")
	r.Check(testkit.Rows(rowStr1, rowStr3))

	testSQL = `drop table if exists t;
    create table t (i int primary key, j int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (-1, 1);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")

	r = tk.MustQuery("select * from t;")
	rowStr1 = fmt.Sprintf("%v %v", "-1", "1")
	r.Check(testkit.Rows(rowStr1))

	tk.MustExec("insert into t values (1, 1) on duplicate key update j = values(j)")
	tk.CheckLastMessage("")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows(rowStr1))

	testSQL = `drop table if exists test;
create table test (i int primary key, j int unique);
begin;
insert into test values (1,1);
insert into test values (2,1) on duplicate key update i = -i, j = -j;
commit;`
	tk.MustExec(testSQL)
	testSQL = `select * from test;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("-1 -1"))

	testSQL = `delete from test;
insert into test values (1, 1);
begin;
delete from test where i = 1;
insert into test values (2, 1) on duplicate key update i = -i, j = -j;
commit;`
	tk.MustExec(testSQL)
	testSQL = `select * from test;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("2 1"))

	testSQL = `delete from test;
insert into test values (1, 1);
begin;
update test set i = 2, j = 2 where i = 1;
insert into test values (1, 3) on duplicate key update i = -i, j = -j;
insert into test values (2, 4) on duplicate key update i = -i, j = -j;
commit;`
	tk.MustExec(testSQL)
	testSQL = `select * from test order by i;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("-2 -2", "1 3"))

	testSQL = `delete from test;
begin;
insert into test values (1, 3), (1, 3) on duplicate key update i = values(i), j = values(j);
commit;`
	tk.MustExec(testSQL)
	testSQL = `select * from test order by i;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 3"))

	testSQL = `create table tmp (id int auto_increment, code int, primary key(id, code));
	create table m (id int primary key auto_increment, code int unique);
	insert tmp (code) values (1);
	insert tmp (code) values (1);
	set tidb_init_chunk_size=1;
	insert m (code) select code from tmp on duplicate key update code = values(code);`
	tk.MustExec(testSQL)
	testSQL = `select * from m;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 1"))

	// The following two cases are used for guaranteeing the last_insert_id
	// to be set as the value of on-duplicate-update assigned.
	testSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT AUTO_INCREMENT PRIMARY KEY,
	f2 VARCHAR(5) NOT NULL UNIQUE);
	INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))
	testSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))

	testSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT AUTO_INCREMENT UNIQUE,
	f2 VARCHAR(5) NOT NULL UNIQUE);
	INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))
	testSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))
	testSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = 2;`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))

	testSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT);
	INSERT t1 VALUES (1) ON DUPLICATE KEY UPDATE f1 = 1;`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	tk.MustQuery(`SELECT * FROM t1;`).Check(testkit.Rows("1"))

	testSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT PRIMARY KEY, f2 INT NOT NULL UNIQUE);
	INSERT t1 VALUES (1, 1);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	tk.MustExec(`INSERT t1 VALUES (1, 1), (1, 1) ON DUPLICATE KEY UPDATE f1 = 2, f2 = 2;`)
	tk.CheckLastMessage("Records: 2  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`SELECT * FROM t1 order by f1;`).Check(testkit.Rows("1 1", "2 2"))
	_, err := tk.Exec(`INSERT t1 VALUES (1, 1) ON DUPLICATE KEY UPDATE f2 = null;`)
	c.Assert(err, NotNil)
	tk.MustExec(`INSERT IGNORE t1 VALUES (1, 1) ON DUPLICATE KEY UPDATE f2 = null;`)
	tk.CheckLastMessage("")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'f2' cannot be null"))
	tk.MustQuery(`SELECT * FROM t1 order by f1;`).Check(testkit.Rows("1 0", "2 2"))

	tk.MustExec(`SET sql_mode='';`)
	tk.MustExec(`INSERT t1 VALUES (1, 1) ON DUPLICATE KEY UPDATE f2 = null;`)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'f2' cannot be null"))
	tk.MustQuery(`SELECT * FROM t1 order by f1;`).Check(testkit.Rows("1 0", "2 2"))
}

func (s *testSuite4) TestInsertIgnoreOnDup(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists t;
    create table t (i int not null primary key, j int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1, 1), (2, 2);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")
	testSQL = `insert ignore into t values(1, 1) on duplicate key update i = 2;`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	testSQL = `select * from t;`
	r := tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 1", "2 2"))
	testSQL = `insert ignore into t values(1, 1) on duplicate key update j = 2;`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("")
	testSQL = `select * from t;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 1", "2 2"))

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(`col_25` set('Alice','Bob','Charlie','David') NOT NULL,`col_26` date NOT NULL DEFAULT '2016-04-15', PRIMARY KEY (`col_26`) clustered, UNIQUE KEY `idx_9` (`col_25`,`col_26`),UNIQUE KEY `idx_10` (`col_25`))")
	tk.MustExec("insert into t2(col_25, col_26) values('Bob', '1989-03-23'),('Alice', '2023-11-24'), ('Charlie', '2023-12-05')")
	tk.MustExec("insert ignore into t2 (col_25,col_26) values ( 'Bob','1977-11-23' ) on duplicate key update col_25 = 'Alice', col_26 = '2036-12-13'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry 'Alice' for key 'idx_10'"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("Bob 1989-03-23", "Alice 2023-11-24", "Charlie 2023-12-05"))

	tk.MustExec("drop table if exists t4")
	tk.MustExec("create table t4(id int primary key clustered, k int, v int, unique key uk1(k))")
	tk.MustExec("insert into t4 values (1, 10, 100), (3, 30, 300)")
	tk.MustExec("insert ignore into t4 (id, k, v) values(1, 0, 0) on duplicate key update id = 2, k = 30")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '30' for key 'uk1'"))
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 10 100", "3 30 300"))

	tk.MustExec("drop table if exists t5")
	tk.MustExec("create table t5(k1 varchar(100), k2 varchar(100), uk1 int, v int, primary key(k1, k2) clustered, unique key ukk1(uk1), unique key ukk2(v))")
	tk.MustExec("insert into t5(k1, k2, uk1, v) values('1', '1', 1, '100'), ('1', '3', 2, '200')")
	tk.MustExec("update ignore t5 set k2 = '2', uk1 = 2 where k1 = '1' and k2 = '1'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '2' for key 'ukk1'"))
	tk.MustQuery("select * from t5").Check(testkit.Rows("1 1 1 100", "1 3 2 200"))

	tk.MustExec("drop table if exists t6")
	tk.MustExec("create table t6 (a int, b int, c int, primary key(a, b) clustered, unique key idx_14(b), unique key idx_15(b), unique key idx_16(a, b))")
	tk.MustExec("insert into t6 select 10, 10, 20")
	tk.MustExec("insert ignore into t6 set a = 20, b = 10 on duplicate key update a = 100")
	tk.MustQuery("select * from t6").Check(testkit.Rows("100 10 20"))
	tk.MustExec("insert ignore into t6 set a = 200, b= 10 on duplicate key update c = 1000")
	tk.MustQuery("select * from t6").Check(testkit.Rows("100 10 1000"))
}

func (s *testSuite4) TestInsertSetWithDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// Assign `DEFAULT` in `INSERT ... SET ...` statement
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int default 10, b int default 20);")
	tk.MustExec("insert into t1 set a=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set b=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set b=default, a=1;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set a=default(a);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("10 20"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set a=default(b), b=default(a)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("20 10"))
	tk.MustExec("delete from t1;")
	tk.MustExec("insert into t1 set a=default(b)+default(a);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("30 20"))
	// With generated columns
	tk.MustExec("create table t2 (a int default 10, b int generated always as (-a) virtual, c int generated always as (-a) stored);")
	tk.MustExec("insert into t2 set a=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("10 -10 -10"))
	tk.MustExec("delete from t2;")
	tk.MustExec("insert into t2 set a=2, b=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("2 -2 -2"))
	tk.MustExec("delete from t2;")
	tk.MustExec("insert into t2 set c=default, a=3;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("3 -3 -3"))
	tk.MustExec("delete from t2;")
	tk.MustExec("insert into t2 set a=default, b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("10 -10 -10"))
	tk.MustExec("delete from t2;")
	tk.MustExec("insert into t2 set a=default(a), b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("10 -10 -10"))
	tk.MustExec("delete from t2;")
	tk.MustGetErrCode("insert into t2 set b=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("insert into t2 set a=default(b), b=default(b);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("insert into t2 set a=default(a), c=default(c);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("insert into t2 set a=default(a), c=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustExec("drop table t1, t2")
}

func (s *testSuite4) TestInsertOnDupUpdateDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// Assign `DEFAULT` in `INSERT ... ON DUPLICATE KEY UPDATE ...` statement
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int unique, b int default 20, c int default 30);")
	tk.MustExec("insert into t1 values (1,default,default);")
	tk.MustExec("insert into t1 values (1,default,default) on duplicate key update b=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30"))
	tk.MustExec("insert into t1 values (1,default,default) on duplicate key update c=default, b=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30"))
	tk.MustExec("insert into t1 values (1,default,default) on duplicate key update c=default, a=2")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("2 20 30"))
	tk.MustExec("insert into t1 values (2,default,default) on duplicate key update c=default(b)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("2 20 20"))
	tk.MustExec("insert into t1 values (2,default,default) on duplicate key update a=default(b)+default(c)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("50 20 20"))
	// With generated columns
	tk.MustExec("create table t2 (a int unique, b int generated always as (-a) virtual, c int generated always as (-a) stored);")
	tk.MustExec("insert into t2 values (1,default,default);")
	tk.MustExec("insert into t2 values (1,default,default) on duplicate key update a=2, b=default;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 -2 -2"))
	tk.MustExec("insert into t2 values (2,default,default) on duplicate key update a=3, c=default;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("3 -3 -3"))
	tk.MustExec("insert into t2 values (3,default,default) on duplicate key update c=default, b=default, a=4;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("4 -4 -4"))
	tk.MustExec("insert into t2 values (10,default,default) on duplicate key update b=default, a=20, c=default;")
	tk.MustQuery("select * from t2").Check(testkit.Rows("4 -4 -4", "10 -10 -10"))
	tk.MustGetErrCode("insert into t2 values (4,default,default) on duplicate key update b=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("insert into t2 values (4,default,default) on duplicate key update a=default(b), b=default(b);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("insert into t2 values (4,default,default) on duplicate key update a=default(a), c=default(c);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("insert into t2 values (4,default,default) on duplicate key update a=default(a), c=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustExec("drop table t1, t2")

	tk.MustExec("set @@tidb_txn_mode = 'pessimistic'")
	tk.MustExec("create table t ( c_int int, c_string varchar(40) collate utf8mb4_bin , primary key (c_string), unique key (c_int));")
	tk.MustExec("insert into t values (22, 'gold witch'), (24, 'gray singer'), (21, 'silver sight');")
	tk.MustExec("begin;")
	err := tk.ExecToErr("insert into t values (21,'black warlock'), (22, 'dark sloth'), (21,  'cyan song') on duplicate key update c_int = c_int + 1, c_string = concat(c_int, ':', c_string);")
	c.Assert(kv.ErrKeyExists.Equal(err), IsTrue)
	tk.MustExec("commit;")
	tk.MustQuery("select * from t order by c_int;").Check(testutil.RowsWithSep("|", "21|silver sight", "22|gold witch", "24|gray singer"))
	tk.MustExec("drop table t;")
}

func (s *testSuite4) TestReplace(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists replace_test;
    create table replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	testSQL = `replace replace_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("Records: 3  Duplicates: 0  Warnings: 0")

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
	tk.CheckLastMessage("")

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
	tk.CheckLastMessage("Records: 4  Duplicates: 0  Warnings: 0")

	replaceSelectSQL = `create table replace_test_2 (id int, c1 int);`
	tk.MustExec(replaceSelectSQL)
	replaceSelectSQL = `replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`
	tk.MustExec(replaceSelectSQL)
	tk.CheckLastMessage("Records: 8  Duplicates: 0  Warnings: 0")

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
	tk.CheckLastMessage("")

	replaceUniqueIndexSQL = `replace into replace_test_3 set c1=1, c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))
	tk.CheckLastMessage("")

	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")

	replaceUniqueIndexSQL = `create table replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2));`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")

	replacePrimaryKeySQL := `create table replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2));`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")

	// For Issue989
	issue989SQL := `CREATE TABLE tIssue989 (a int, b int, PRIMARY KEY(a), UNIQUE KEY(b));`
	tk.MustExec(issue989SQL)
	issue989SQL = `insert into tIssue989 (a, b) values (1, 2);`
	tk.MustExec(issue989SQL)
	tk.CheckLastMessage("")
	issue989SQL = `replace into tIssue989(a, b) values (111, 2);`
	tk.MustExec(issue989SQL)
	tk.CheckLastMessage("")
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
	tk.CheckLastMessage("")
	r = tk.MustQuery("select * from tIssue1012;")
	r.Check(testkit.Rows("1 1"))

	// Test Replace with info message
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(a int primary key, b int);`)
	tk.MustExec(`insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);`)
	tk.MustExec(`replace into t1 values(1,1);`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`replace into t1 values(1,1),(2,2);`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))
	tk.CheckLastMessage("Records: 2  Duplicates: 0  Warnings: 0")
	tk.MustExec(`replace into t1 values(4,14),(5,15),(6,16),(7,17),(8,18)`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(7))
	tk.CheckLastMessage("Records: 5  Duplicates: 2  Warnings: 0")
	tk.MustExec(`replace into t1 select * from (select 1, 2) as tmp;`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")

	// Assign `DEFAULT` in `REPLACE` statement
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int primary key, b int default 20, c int default 30);")
	tk.MustExec("insert into t1 value (1, 2, 3);")
	tk.MustExec("replace t1 set a=1, b=default;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30"))
	tk.MustExec("replace t1 set a=2, b=default, c=default")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30", "2 20 30"))
	tk.MustExec("replace t1 set a=2, b=default(c), c=default(b);")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30", "2 30 20"))
	tk.MustExec("replace t1 set a=default(b)+default(c)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 20 30", "2 30 20", "50 20 30"))
	// With generated columns
	tk.MustExec("create table t2 (pk int primary key, a int default 1, b int generated always as (-a) virtual, c int generated always as (-a) stored);")
	tk.MustExec("replace t2 set pk=1, b=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1 -1 -1"))
	tk.MustExec("replace t2 set pk=2, a=10, b=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1 -1 -1", "2 10 -10 -10"))
	tk.MustExec("replace t2 set pk=2, c=default, a=20;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1 -1 -1", "2 20 -20 -20"))
	tk.MustExec("replace t2 set pk=2, a=default, b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1 -1 -1", "2 1 -1 -1"))
	tk.MustExec("replace t2 set pk=3, a=default(a), b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 1 -1 -1", "2 1 -1 -1", "3 1 -1 -1"))
	tk.MustGetErrCode("replace t2 set b=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("replace t2 set a=default(b), b=default(b);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("replace t2 set a=default(a), c=default(c);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("replace t2 set a=default(a), c=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustExec("drop table t1, t2")
}

func (s *testSuite2) TestGeneratedColumnForInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// test cases for default behavior
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(id int, id_gen int as(id + 42), b int, unique key id_gen(id_gen));`)
	tk.MustExec(`insert into t1 (id, b) values(1,1),(2,2),(3,3),(4,4),(5,5);`)
	tk.MustExec(`replace into t1 (id, b) values(1,1);`)
	tk.MustExec(`replace into t1 (id, b) values(1,1),(2,2);`)
	tk.MustExec(`replace into t1 (id, b) values(6,16),(7,17),(8,18);`)
	tk.MustQuery("select * from t1;").Check(testkit.Rows(
		"1 43 1", "2 44 2", "3 45 3", "4 46 4", "5 47 5", "6 48 16", "7 49 17", "8 50 18"))
	tk.MustExec(`insert into t1 (id, b) values (6,18) on duplicate key update id = -id;`)
	tk.MustExec(`insert into t1 (id, b) values (7,28) on duplicate key update b = -values(b);`)
	tk.MustQuery("select * from t1;").Check(testkit.Rows(
		"1 43 1", "2 44 2", "3 45 3", "4 46 4", "5 47 5", "-6 36 16", "7 49 -28", "8 50 18"))

	// test cases for virtual and stored columns in the same table
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t
	(i int as(k+1) stored, j int as(k+2) virtual, k int, unique key idx_i(i), unique key idx_j(j))`)
	tk.MustExec(`insert into t (k) values (1), (2)`)
	tk.MustExec(`replace into t (k) values (1), (2)`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2 3 1", "3 4 2"))

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t
	(i int as(k+1) stored, j int as(k+2) virtual, k int, unique key idx_j(j))`)
	tk.MustExec(`insert into t (k) values (1), (2)`)
	tk.MustExec(`replace into t (k) values (1), (2)`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2 3 1", "3 4 2"))

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t
	(i int as(k+1) stored, j int as(k+2) virtual, k int, unique key idx_i(i))`)
	tk.MustExec(`insert into t (k) values (1), (2)`)
	tk.MustExec(`replace into t (k) values (1), (2)`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows("2 3 1", "3 4 2"))

	// For issue 14340
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(f1 json, f2 real as (cast(f1 as decimal(2,1))))`)
	tk.MustGetErrMsg(`INSERT INTO t1 (f1) VALUES (CAST(1000 AS JSON))`, "[types:1690]DECIMAL value is out of range in '(2, 1)'")
	tk.MustExec(`set @@sql_mode = ''`)
	tk.MustExec(`INSERT INTO t1 (f1) VALUES (CAST(1000 AS JSON))`)
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1000 9.9"))
}

func (s *testSuite4) TestPartitionedTableReplace(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists replace_test;
		    create table replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)
			partition by range (id) (
			PARTITION p0 VALUES LESS THAN (3),
			PARTITION p1 VALUES LESS THAN (5),
			PARTITION p2 VALUES LESS THAN (7),
			PARTITION p3 VALUES LESS THAN (9));`
	tk.MustExec(testSQL)
	testSQL = `replace replace_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("Records: 3  Duplicates: 0  Warnings: 0")

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
	tk.CheckLastMessage("")

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

	tk.MustExec(`drop table if exists replace_test_1`)
	tk.MustExec(`create table replace_test_1 (id int, c1 int) partition by range (id) (
			PARTITION p0 VALUES LESS THAN (4),
			PARTITION p1 VALUES LESS THAN (6),
			PARTITION p2 VALUES LESS THAN (8),
			PARTITION p3 VALUES LESS THAN (10),
			PARTITION p4 VALUES LESS THAN (100))`)
	tk.MustExec(`replace replace_test_1 select id, c1 from replace_test;`)
	tk.CheckLastMessage("Records: 4  Duplicates: 0  Warnings: 0")

	tk.MustExec(`drop table if exists replace_test_2`)
	tk.MustExec(`create table replace_test_2 (id int, c1 int) partition by range (id) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN (50),
			PARTITION p2 VALUES LESS THAN (100),
			PARTITION p3 VALUES LESS THAN (300))`)
	tk.MustExec(`replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`)
	tk.CheckLastMessage("Records: 8  Duplicates: 0  Warnings: 0")

	errReplaceSelectSQL := `replace replace_test_1 select c1 from replace_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	tk.MustExec(`drop table if exists replace_test_3`)
	replaceUniqueIndexSQL := `create table replace_test_3 (c1 int, c2 int, UNIQUE INDEX (c2)) partition by range (c2) (
				    PARTITION p0 VALUES LESS THAN (4),
				    PARTITION p1 VALUES LESS THAN (7),
				    PARTITION p2 VALUES LESS THAN (11))`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=8;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=8;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")
	replaceUniqueIndexSQL = `replace into replace_test_3 set c1=8, c2=8;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))
	tk.CheckLastMessage("")

	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.CheckLastMessage("")

	replaceUniqueIndexSQL = `create table replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2)) partition by range (c1) (
				    PARTITION p0 VALUES LESS THAN (4),
				    PARTITION p1 VALUES LESS THAN (7),
				    PARTITION p2 VALUES LESS THAN (11));`
	tk.MustExec(`drop table if exists replace_test_4`)
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replacePrimaryKeySQL := `create table replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2)) partition by range (c2) (
				    PARTITION p0 VALUES LESS THAN (4),
				    PARTITION p1 VALUES LESS THAN (7),
				    PARTITION p2 VALUES LESS THAN (11));`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	issue989SQL := `CREATE TABLE tIssue989 (a int, b int, KEY(a), UNIQUE KEY(b)) partition by range (b) (
			    PARTITION p1 VALUES LESS THAN (100),
			    PARTITION p2 VALUES LESS THAN (200))`
	tk.MustExec(issue989SQL)
	issue989SQL = `insert into tIssue989 (a, b) values (1, 2);`
	tk.MustExec(issue989SQL)
	issue989SQL = `replace into tIssue989(a, b) values (111, 2);`
	tk.MustExec(issue989SQL)
	r := tk.MustQuery("select * from tIssue989;")
	r.Check(testkit.Rows("111 2"))
}

func (s *testSuite4) TestHashPartitionedTableReplace(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec("drop table if exists replace_test;")
	testSQL := `create table replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)
			partition by hash(id) partitions 4;`
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

	tk.MustExec(`replace replace_test set c1 = 3;`)
	tk.MustExec(`replace replace_test set c1 = 4;`)
	tk.MustExec(`replace replace_test set c1 = 5;`)
	tk.MustExec(`replace replace_test set c1 = 6;`)
	tk.MustExec(`replace replace_test set c1 = 7;`)

	tk.MustExec(`drop table if exists replace_test_1`)
	tk.MustExec(`create table replace_test_1 (id int, c1 int) partition by hash(id) partitions 5;`)
	tk.MustExec(`replace replace_test_1 select id, c1 from replace_test;`)

	tk.MustExec(`drop table if exists replace_test_2`)
	tk.MustExec(`create table replace_test_2 (id int, c1 int) partition by hash(id) partitions 6;`)

	tk.MustExec(`replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`)

	errReplaceSelectSQL := `replace replace_test_1 select c1 from replace_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	tk.MustExec(`drop table if exists replace_test_3`)
	replaceUniqueIndexSQL := `create table replace_test_3 (c1 int, c2 int, UNIQUE INDEX (c2)) partition by hash(c2) partitions 7;`
	tk.MustExec(replaceUniqueIndexSQL)

	tk.MustExec(`replace into replace_test_3 set c2=8;`)
	tk.MustExec(`replace into replace_test_3 set c2=8;`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	tk.MustExec(`replace into replace_test_3 set c1=8, c2=8;`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))

	tk.MustExec(`replace into replace_test_3 set c2=NULL;`)
	tk.MustExec(`replace into replace_test_3 set c2=NULL;`)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	for i := 0; i < 100; i++ {
		sql := fmt.Sprintf("replace into replace_test_3 set c2=%d;", i)
		tk.MustExec(sql)
	}
	result := tk.MustQuery("select count(*) from replace_test_3")
	result.Check(testkit.Rows("102"))

	replaceUniqueIndexSQL = `create table replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2)) partition by hash(c1) partitions 8;`
	tk.MustExec(`drop table if exists replace_test_4`)
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replacePrimaryKeySQL := `create table replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2)) partition by hash (c2) partitions 9;`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	issue989SQL := `CREATE TABLE tIssue989 (a int, b int, KEY(a), UNIQUE KEY(b)) partition by hash (b) partitions 10;`
	tk.MustExec(issue989SQL)
	issue989SQL = `insert into tIssue989 (a, b) values (1, 2);`
	tk.MustExec(issue989SQL)
	issue989SQL = `replace into tIssue989(a, b) values (111, 2);`
	tk.MustExec(issue989SQL)
	r := tk.MustQuery("select * from tIssue989;")
	r.Check(testkit.Rows("111 2"))
}

func (s *testSuite8) TestUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "update_test")

	updateStr := `UPDATE update_test SET name = "abc" where id > 0;`
	tk.MustExec(updateStr)
	tk.CheckExecResult(2, 0)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")

	// select data
	tk.MustExec("begin")
	r := tk.MustQuery(`SELECT * from update_test limit 2;`)
	r.Check(testkit.Rows("1 abc", "2 abc"))
	tk.MustExec("commit")

	tk.MustExec(`UPDATE update_test SET name = "foo"`)
	tk.CheckExecResult(2, 0)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")

	// table option is auto-increment
	tk.MustExec("begin")
	tk.MustExec("drop table if exists update_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create table update_test(id int not null auto_increment, name varchar(255), primary key(id))")
	tk.MustExec("insert into update_test(name) values ('aa')")
	tk.MustExec("update update_test set id = 8 where name = 'aa'")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
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
	c.Assert(err.Error(), DeepEquals, "[table:1048]Column 'id' cannot be null")

	tk.MustExec("drop table update_test")
	tk.MustExec("create table update_test(id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into update_test(id) values (1)")
	tk.MustExec("update update_test set id = 2 where id = 1 limit 1")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
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
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))

	// test update ignore for truncate as warning
	_, err = tk.Exec("update ignore t set a = 1 where a = (select '2a')")
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '2a'", "Warning 1292 Truncated incorrect FLOAT value: '2a'", "Warning 1062 Duplicate entry '1' for key 'PRIMARY'"))

	tk.MustExec("update ignore t set a = 42 where a = 2;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "42"))

	// test update ignore for unique key
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint, unique key I_uniq (a));")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	_, err = tk.Exec("update ignore t set a = 1 where a = 2;")
	c.Assert(err, IsNil)
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'I_uniq'"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))

	// test issue21965
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("create table t (a int) partition by list (a) (partition p0 values in (0,1));")
	tk.MustExec("insert ignore into t values (1);")
	tk.MustExec("update ignore t set a=2 where a=1;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 0")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int key) partition by list (a) (partition p0 values in (0,1));")
	tk.MustExec("insert ignore into t values (1);")
	tk.MustExec("update ignore t set a=2 where a=1;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 0")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id integer auto_increment, t1 datetime, t2 datetime, primary key (id))")
	tk.MustExec("insert into t(t1, t2) values('2000-10-01 01:01:01', '2017-01-01 10:10:10')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2000-10-01 01:01:01 2017-01-01 10:10:10"))
	tk.MustExec("update t set t1 = '2017-10-01 10:10:11', t2 = date_add(t1, INTERVAL 10 MINUTE) where id = 1")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2017-10-01 10:10:11 2000-10-01 01:11:01"))

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
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("select * from tt1;")
	r.Check(testkit.Rows("1 a a", "5 d b"))

	// Automatic Updating for TIMESTAMP
	tk.MustExec("CREATE TABLE `tsup` (" +
		"`a` int," +
		"`ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
		"KEY `idx` (`ts`)" +
		");")
	tk.MustExec("set @orig_sql_mode=@@sql_mode; set @@sql_mode='';")
	tk.MustExec("insert into tsup values(1, '0000-00-00 00:00:00');")
	tk.MustExec("update tsup set a=5;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r1 := tk.MustQuery("select ts from tsup use index (idx);")
	r2 := tk.MustQuery("select ts from tsup;")
	r1.Check(r2.Rows())
	tk.MustExec("update tsup set ts='2019-01-01';")
	tk.MustQuery("select ts from tsup;").Check(testkit.Rows("2019-01-01 00:00:00"))
	tk.MustExec("set @@sql_mode=@orig_sql_mode;")

	// issue 5532
	tk.MustExec("create table decimals (a decimal(20, 0) not null)")
	tk.MustExec("insert into decimals values (201)")
	// A warning rather than data truncated error.
	tk.MustExec("update decimals set a = a + 1.23;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect DECIMAL value: '202.23'"))
	r = tk.MustQuery("select * from decimals")
	r.Check(testkit.Rows("202"))

	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE `t` (	`c1` year DEFAULT NULL, `c2` year DEFAULT NULL, `c3` date DEFAULT NULL, `c4` datetime DEFAULT NULL,	KEY `idx` (`c1`,`c2`))")
	_, err = tk.Exec("UPDATE t SET c2=16777215 WHERE c1>= -8388608 AND c1 < -9 ORDER BY c1 LIMIT 2")
	c.Assert(err, IsNil)

	tk.MustExec("update (select * from t) t set c1 = 1111111")

	// test update ignore for bad null error
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (i int not null default 10)`)
	tk.MustExec("insert into t values (1)")
	tk.MustExec("update ignore t set i = null;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1048 Column 'i' cannot be null"))
	tk.MustQuery("select * from t").Check(testkit.Rows("0"))

	// issue 7237, update subquery table should be forbidden
	tk.MustExec("drop table t")
	tk.MustExec("create table t (k int, v int)")
	_, err = tk.Exec("update t, (select * from t) as b set b.k = t.k")
	c.Assert(err.Error(), Equals, "[planner:1288]The target table b of the UPDATE is not updatable")
	tk.MustExec("update t, (select * from t) as b set t.k = b.k")

	// issue 8045
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`CREATE TABLE t1 (c1 float)`)
	tk.MustExec("INSERT INTO t1 SET c1 = 1")
	tk.MustExec("UPDATE t1 SET c1 = 1.2 WHERE c1=1;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")

	// issue 8119
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c1 float(1,1));")
	tk.MustExec("insert into t values (0.0);")
	_, err = tk.Exec("update t set c1 = 2.0;")
	c.Assert(types.ErrWarnDataOutOfRange.Equal(err), IsTrue)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime not null, b datetime)")
	tk.MustExec("insert into t value('1999-12-12', '1999-12-13')")
	tk.MustExec("set @orig_sql_mode=@@sql_mode; set @@sql_mode='';")
	tk.MustQuery("select * from t").Check(testkit.Rows("1999-12-12 00:00:00 1999-12-13 00:00:00"))
	tk.MustExec("update t set a = ''")
	tk.MustQuery("select * from t").Check(testkit.Rows("0000-00-00 00:00:00 1999-12-13 00:00:00"))
	tk.MustExec("update t set b = ''")
	tk.MustQuery("select * from t").Check(testkit.Rows("0000-00-00 00:00:00 0000-00-00 00:00:00"))
	tk.MustExec("set @@sql_mode=@orig_sql_mode;")

	tk.MustExec("create view v as select * from t")
	_, err = tk.Exec("update v set a = '2000-11-11'")
	c.Assert(err.Error(), Equals, core.ErrViewInvalid.GenWithStackByArgs("test", "v").Error())
	tk.MustExec("drop view v")

	tk.MustExec("create sequence seq")
	_, err = tk.Exec("update seq set minvalue=1")
	c.Assert(err.Error(), Equals, "update sequence seq is not supported now.")
	tk.MustExec("drop sequence seq")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, d int, e int, index idx(a))")
	tk.MustExec("create table t2(a int, b int, c int)")
	tk.MustExec("update t1 join t2 on t1.a=t2.a set t1.a=1 where t2.b=1 and t2.c=2")

	// Assign `DEFAULT` in `UPDATE` statement
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int default 1, b int default 2);")
	tk.MustExec("insert into t1 values (10, 10), (20, 20);")
	tk.MustExec("update t1 set a=default where b=10;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "20 20"))
	tk.MustExec("update t1 set a=30, b=default where a=20;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "30 2"))
	tk.MustExec("update t1 set a=default, b=default where a=30;")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 10", "1 2"))
	tk.MustExec("insert into t1 values (40, 40)")
	tk.MustExec("update t1 set a=default, b=default")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("1 2", "1 2", "1 2"))
	tk.MustExec("update t1 set a=default(b), b=default(a)")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("2 1", "2 1", "2 1"))
	// With generated columns
	tk.MustExec("create table t2 (a int default 1, b int generated always as (-a) virtual, c int generated always as (-a) stored);")
	tk.MustExec("insert into t2 values (10, default, default), (20, default, default)")
	tk.MustExec("update t2 set b=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("10 -10 -10", "20 -20 -20"))
	tk.MustExec("update t2 set a=30, b=default where a=10;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("30 -30 -30", "20 -20 -20"))
	tk.MustExec("update t2 set c=default, a=40 where c=-20;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("30 -30 -30", "40 -40 -40"))
	tk.MustExec("update t2 set a=default, b=default, c=default where b=-30;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 -1 -1", "40 -40 -40"))
	tk.MustExec("update t2 set a=default(a), b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 -1 -1", "1 -1 -1"))
	tk.MustGetErrCode("update t2 set b=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("update t2 set a=default(b), b=default(b);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("update t2 set a=default(a), c=default(c);", mysql.ErrBadGeneratedColumn)
	tk.MustGetErrCode("update t2 set a=default(a), c=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustExec("drop table t1, t2")
}

func (s *testSuite4) TestPartitionedTableUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (id int not null default 1, name varchar(255))
			PARTITION BY RANGE ( id ) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21))`)

	tk.MustExec(`insert INTO t VALUES (1, "hello");`)
	tk.CheckExecResult(1, 0)
	tk.MustExec(`insert INTO t VALUES (7, "hello");`)
	tk.CheckExecResult(1, 0)

	// update non partition column
	tk.MustExec(`UPDATE t SET name = "abc" where id > 0;`)
	tk.CheckExecResult(2, 0)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
	r := tk.MustQuery(`SELECT * from t order by id limit 2;`)
	r.Check(testkit.Rows("1 abc", "7 abc"))

	// update partition column
	tk.MustExec(`update t set id = id + 1`)
	tk.CheckExecResult(2, 0)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
	r = tk.MustQuery(`SELECT * from t order by id limit 2;`)
	r.Check(testkit.Rows("2 abc", "8 abc"))

	// update partition column, old and new record locates on different partitions
	tk.MustExec(`update t set id = 20 where id = 8`)
	tk.CheckExecResult(1, 0)
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery(`SELECT * from t order by id limit 2;`)
	r.Check(testkit.Rows("2 abc", "20 abc"))

	// table option is auto-increment
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (id int not null auto_increment, name varchar(255), primary key(id))
			PARTITION BY RANGE ( id ) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21))`)

	tk.MustExec("insert into t(name) values ('aa')")
	tk.MustExec("update t set id = 8 where name = 'aa'")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("insert into t(name) values ('bb')")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("8 aa", "9 bb"))

	_, err := tk.Exec("update t set id = null where name = 'aa'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), DeepEquals, "[table:1048]Column 'id' cannot be null")

	// Test that in a transaction, when a constraint failed in an update statement, the record is not inserted.
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (id int, name int unique)
			PARTITION BY RANGE ( name ) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (16),
			PARTITION p3 VALUES LESS THAN (21))`)
	tk.MustExec("insert t values (1, 1), (2, 2);")
	_, err = tk.Exec("update t set name = 1 where id = 2")
	c.Assert(err, NotNil)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1", "2 2"))

	// test update ignore for pimary key
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t(a bigint, primary key (a))
			PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11))`)
	tk.MustExec("insert into t values (5)")
	tk.MustExec("insert into t values (7)")
	_, err = tk.Exec("update ignore t set a = 5 where a = 7;")
	c.Assert(err, IsNil)
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '5' for key 'PRIMARY'"))
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("5", "7"))

	// test update ignore for truncate as warning
	_, err = tk.Exec("update ignore t set a = 1 where a = (select '2a')")
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '2a'", "Warning 1292 Truncated incorrect FLOAT value: '2a'"))

	// test update ignore for unique key
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t(a bigint, unique key I_uniq (a))
			PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (6),
			PARTITION p1 VALUES LESS THAN (11))`)
	tk.MustExec("insert into t values (5)")
	tk.MustExec("insert into t values (7)")
	_, err = tk.Exec("update ignore t set a = 5 where a = 7;")
	c.Assert(err, IsNil)
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '5' for key 'I_uniq'"))
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("5", "7"))
}

// TestUpdateCastOnlyModifiedValues for issue #4514.
func (s *testSuite4) TestUpdateCastOnlyModifiedValues(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table update_modified (col_1 int, col_2 enum('a', 'b'))")
	tk.MustExec("set SQL_MODE=''")
	tk.MustExec("insert into update_modified values (0, 3)")
	r := tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("0 "))
	tk.MustExec("set SQL_MODE=STRICT_ALL_TABLES")
	tk.MustExec("update update_modified set col_1 = 1")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("1 "))
	_, err := tk.Exec("update update_modified set col_1 = 2, col_2 = 'c'")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("1 "))
	tk.MustExec("update update_modified set col_1 = 3, col_2 = 'a'")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("3 a"))

	// Test update a field with different column type.
	tk.MustExec(`CREATE TABLE update_with_diff_type (a int, b JSON)`)
	tk.MustExec(`INSERT INTO update_with_diff_type VALUES(3, '{"a": "测试"}')`)
	tk.MustExec(`UPDATE update_with_diff_type SET a = '300'`)
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("SELECT a FROM update_with_diff_type")
	r.Check(testkit.Rows("300"))
	tk.MustExec(`UPDATE update_with_diff_type SET b = '{"a":   "\\u6d4b\\u8bd5"}'`)
	tk.CheckLastMessage("Rows matched: 1  Changed: 0  Warnings: 0")
	r = tk.MustQuery("SELECT b FROM update_with_diff_type")
	r.Check(testkit.Rows(`{"a": "测试"}`))
}

func (s *testSuite4) fillMultiTableForUpdate(tk *testkit.TestKit) {
	// Create and fill table items
	tk.MustExec("CREATE TABLE items (id int, price TEXT);")
	tk.MustExec(`insert into items values (11, "items_price_11"), (12, "items_price_12"), (13, "items_price_13");`)
	tk.CheckExecResult(3, 0)
	// Create and fill table month
	tk.MustExec("CREATE TABLE month (mid int, mprice TEXT);")
	tk.MustExec(`insert into month values (11, "month_price_11"), (22, "month_price_22"), (13, "month_price_13");`)
	tk.CheckExecResult(3, 0)
}

func (s *testSuite4) TestMultipleTableUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillMultiTableForUpdate(tk)

	tk.MustExec(`UPDATE items, month  SET items.price=month.mprice WHERE items.id=month.mid;`)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
	tk.MustExec("begin")
	r := tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 month_price_11", "12 items_price_12", "13 month_price_13"))
	tk.MustExec("commit")

	// Single-table syntax but with multiple tables
	tk.MustExec(`UPDATE items join month on items.id=month.mid SET items.price=month.mid;`)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 11", "12 items_price_12", "13 13"))
	tk.MustExec("commit")

	// JoinTable with alias table name.
	tk.MustExec(`UPDATE items T0 join month T1 on T0.id=T1.mid SET T0.price=T1.mprice;`)
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
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
	tk.CheckLastMessage("Rows matched: 4  Changed: 4  Warnings: 0")

	// fix https://github.com/pingcap/tidb/issues/376
	testSQL = `DROP TABLE IF EXISTS t1, t2;
		create table t1 (c1 int);
		create table t2 (c2 int);
		insert into t1 values (1), (2);
		insert into t2 values (1), (2);
		update t1, t2 set t1.c1 = 10, t2.c2 = 2 where t2.c2 = 1;`
	tk.MustExec(testSQL)
	tk.CheckLastMessage("Rows matched: 3  Changed: 3  Warnings: 0")

	r = tk.MustQuery("select * from t1")
	r.Check(testkit.Rows("10", "10"))

	// test https://github.com/pingcap/tidb/issues/3604
	tk.MustExec("drop table if exists t, t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3)")
	tk.CheckLastMessage("Records: 3  Duplicates: 0  Warnings: 0")
	tk.MustExec("update t m, t n set m.a = m.a + 1")
	tk.CheckLastMessage("Rows matched: 3  Changed: 3  Warnings: 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1", "3 2", "4 3"))
	tk.MustExec("update t m, t n set n.a = n.a - 1, n.b = n.b + 1")
	tk.CheckLastMessage("Rows matched: 3  Changed: 3  Warnings: 0")
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
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '2a'", "Warning 1292 Truncated incorrect FLOAT value: '2a'"))

	tk.MustExec(`delete from delete_test ;`)
	tk.CheckExecResult(1, 0)

	tk.MustExec("create view v as select * from delete_test")
	_, err = tk.Exec("delete from v where name = 'aaa'")
	c.Assert(err.Error(), Equals, core.ErrViewInvalid.GenWithStackByArgs("test", "v").Error())
	tk.MustExec("drop view v")

	tk.MustExec("create sequence seq")
	_, err = tk.Exec("delete from seq")
	c.Assert(err.Error(), Equals, "delete sequence seq is not supported now.")
	tk.MustExec("drop sequence seq")
}

func (s *testSuite4) TestPartitionedTableDelete(c *C) {
	createTable := `CREATE TABLE test.t (id int not null default 1, name varchar(255), index(id))
			  PARTITION BY RANGE ( id ) (
			  PARTITION p0 VALUES LESS THAN (6),
			  PARTITION p1 VALUES LESS THAN (11),
			  PARTITION p2 VALUES LESS THAN (16),
			  PARTITION p3 VALUES LESS THAN (21))`

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(createTable)
	for i := 1; i < 21; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, "hello")`, i))
	}

	tk.MustExec(`delete from t where id = 2 limit 1;`)
	tk.CheckExecResult(1, 0)

	// Test delete with false condition
	tk.MustExec(`delete from t where 0;`)
	tk.CheckExecResult(0, 0)

	tk.MustExec("insert into t values (2, 'abc')")
	tk.MustExec(`delete from t where t.id = 2 limit 1`)
	tk.CheckExecResult(1, 0)

	// Test delete ignore
	tk.MustExec("insert into t values (2, 'abc')")
	_, err := tk.Exec("delete from t where id = (select '2a')")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete ignore from t where id = (select '2a')")
	c.Assert(err, IsNil)
	tk.CheckExecResult(1, 0)
	r := tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect FLOAT value: '2a'", "Warning 1292 Truncated incorrect FLOAT value: '2a'"))

	// Test delete without using index, involve multiple partitions.
	tk.MustExec("delete from t ignore index(id) where id >= 13 and id <= 17")
	tk.CheckExecResult(5, 0)

	tk.MustExec("admin check table t")
	tk.MustExec(`delete from t;`)
	tk.CheckExecResult(14, 0)

	// Fix that partitioned table should not use PointGetPlan.
	tk.MustExec(`create table t1 (c1 bigint, c2 bigint, c3 bigint, primary key(c1)) partition by range (c1) (partition p0 values less than (3440))`)
	tk.MustExec("insert into t1 values (379, 379, 379)")
	tk.MustExec("delete from t1 where c1 = 379")
	tk.CheckExecResult(1, 0)
	tk.MustExec(`drop table t1;`)
}

func (s *testSuite4) fillDataMultiTable(tk *testkit.TestKit) {
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

func (s *testSuite4) TestMultiTableDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.fillDataMultiTable(tk)

	tk.MustExec(`delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;`)
	tk.CheckExecResult(2, 0)

	// Select data
	r := tk.MustQuery("select * from t3")
	c.Assert(r.Rows(), HasLen, 3)
}

func (s *testSuite4) TestQualifiedDelete(c *C) {
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

	tk.MustExec("insert into t1 values (1, 3)")
	tk.MustExec("delete from t1 as a where a.c1 = 1")
	tk.CheckExecResult(1, 0)

	tk.MustExec("insert into t1 values (1, 1), (2, 2)")
	tk.MustExec("insert into t2 values (2, 1), (3,1)")
	tk.MustExec("delete t1, t2 from t1 join t2 where t1.c1 = t2.c2")
	tk.CheckExecResult(3, 0)

	tk.MustExec("insert into t2 values (2, 1), (3,1)")
	tk.MustExec("delete a, b from t1 as a join t2 as b where a.c2 = b.c1")
	tk.CheckExecResult(2, 0)

	_, err := tk.Exec("delete t1, t2 from t1 as a join t2 as b where a.c2 = b.c1")
	c.Assert(err, NotNil)
}

func (s *testSuite8) TestLoadDataMissingColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	createSQL := `create table load_data_missing (id int, t timestamp not null)`
	tk.MustExec(createSQL)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' ignore into table load_data_missing")
	ctx := tk.Se.(sessionctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)

	deleteSQL := "delete from load_data_missing"
	selectSQL := "select id, hour(t), minute(t) from load_data_missing;"
	_, reachLimit, err := ld.InsertData(context.Background(), nil, nil)
	c.Assert(err, IsNil)
	c.Assert(reachLimit, IsFalse)
	r := tk.MustQuery(selectSQL)
	r.Check(nil)

	curTime := types.CurrentTime(mysql.TypeTimestamp)
	timeHour := curTime.Hour()
	timeMinute := curTime.Minute()
	tests := []testCase{
		{nil, []byte("12\n"), []string{fmt.Sprintf("12|%v|%v", timeHour, timeMinute)}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

	tk.MustExec("alter table load_data_missing add column t2 timestamp null")
	curTime = types.CurrentTime(mysql.TypeTimestamp)
	timeHour = curTime.Hour()
	timeMinute = curTime.Minute()
	selectSQL = "select id, hour(t), minute(t), t2 from load_data_missing;"
	tests = []testCase{
		{nil, []byte("12\n"), []string{fmt.Sprintf("12|%v|%v|<nil>", timeHour, timeMinute)}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

}

func (s *testSuite4) TestIssue18681(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	createSQL := `drop table if exists load_data_test;
		create table load_data_test (a bit(1),b bit(1),c bit(1),d bit(1));`
	tk.MustExec(createSQL)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' ignore into table load_data_test")
	ctx := tk.Se.(sessionctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)

	deleteSQL := "delete from load_data_test"
	selectSQL := "select bin(a), bin(b), bin(c), bin(d) from load_data_test;"
	ctx.GetSessionVars().StmtCtx.DupKeyAsWarning = true
	ctx.GetSessionVars().StmtCtx.BadNullAsWarning = true
	ld.SetMaxRowsInBatch(20000)

	sc := ctx.GetSessionVars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
	}()
	sc.IgnoreTruncate = false
	tests := []testCase{
		{nil, []byte("true\tfalse\t0\t1\n"), []string{"1|0|0|1"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
	c.Assert(sc.WarningCount(), Equals, uint16(0))
}

func (s *testSuite4) TestLoadData(c *C) {
	trivialMsg := "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	createSQL := `drop table if exists load_data_test;
		create table load_data_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 varchar(255) default "def", c3 int);`
	_, err := tk.Exec("load data local infile '/tmp/nonexistence.csv' into table load_data_test")
	c.Assert(err, NotNil)
	tk.MustExec(createSQL)
	_, err = tk.Exec("load data infile '/tmp/nonexistence.csv' into table load_data_test")
	c.Assert(err, NotNil)
	_, err = tk.Exec("load data local infile '/tmp/nonexistence.csv' replace into table load_data_test")
	c.Assert(err, NotNil)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' ignore into table load_data_test")
	ctx := tk.Se.(sessionctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)

	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	// data1 = nil, data2 = nil, fields and lines is default
	ctx.GetSessionVars().StmtCtx.DupKeyAsWarning = true
	ctx.GetSessionVars().StmtCtx.BadNullAsWarning = true
	_, reachLimit, err := ld.InsertData(context.Background(), nil, nil)
	c.Assert(err, IsNil)
	c.Assert(reachLimit, IsFalse)
	err = ld.CheckAndInsertOneBatch(context.Background(), ld.GetRows(), ld.GetCurBatchCnt())
	c.Assert(err, IsNil)
	ld.SetMaxRowsInBatch(20000)
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
		{nil, []byte("\n"), []string{"1|<nil>|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("\t\n"), []string{"2|0|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 2"},
		{nil, []byte("3\t2\t3\t4\n"), []string{"3|2|3|4"}, nil, trivialMsg},
		{nil, []byte("3*1\t2\t3\t4\n"), []string{"3|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("4\t2\t\t3\t4\n"), []string{"4|2||3"}, nil, trivialMsg},
		{nil, []byte("\t1\t2\t3\t4\n"), []string{"5|1|2|3"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("6\t2\t3\n"), []string{"6|2|3|<nil>"}, nil, trivialMsg},
		{nil, []byte("\t2\t3\t4\n\t22\t33\t44\n"), []string{"7|2|3|4", "8|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{nil, []byte("7\t2\t3\t4\n7\t22\t33\t44\n"), []string{"7|2|3|4"}, nil, "Records: 2  Deleted: 0  Skipped: 1  Warnings: 1"},

		// data1 != nil, data2 = nil
		{[]byte("\t2\t3\t4"), nil, []string{"9|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		// data1 != nil, data2 != nil
		{[]byte("\t2\t3"), []byte("\t4\t5\n"), []string{"10|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("\t2\t3"), []byte("4\t5\n"), []string{"11|2|34|5"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		// data1 != nil, data2 != nil, InsertData returns data isn't nil
		{[]byte("\t2\t3"), []byte("\t4\t5"), nil, []byte("\t2\t3\t4\t5"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
	c.Assert(sc.WarningCount(), Equals, uint16(1))

	// lines starting symbol is "" and terminated symbol length is 2, InsertData returns data is nil
	ld.LinesInfo.Terminated = "||"
	tests = []testCase{
		// data1 != nil, data2 != nil
		{[]byte("0\t2\t3"), []byte("\t4\t5||"), []string{"12|2|3|4"}, nil, trivialMsg},
		{[]byte("1\t2\t3\t4\t5|"), []byte("|"), []string{"1|2|3|4"}, nil, trivialMsg},
		{[]byte("2\t2\t3\t4\t5|"), []byte("|3\t22\t33\t44\t55||"),
			[]string{"2|2|3|4", "3|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("3\t2\t3\t4\t5|"), []byte("|4\t22\t33||"), []string{
			"3|2|3|4", "4|22|33|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("4\t2\t3\t4\t5|"), []byte("|5\t22\t33||6\t222||"),
			[]string{"4|2|3|4", "5|22|33|<nil>", "6|222|<nil>|<nil>"}, nil, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("6\t2\t3"), []byte("4\t5||"), []string{"6|2|34|5"}, nil, trivialMsg},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

	// fields and lines aren't default, InsertData returns data is nil
	ld.FieldsInfo.Terminated = "\\"
	ld.LinesInfo.Starting = "xxx"
	ld.LinesInfo.Terminated = "|!#^"
	tests = []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("xxx|!#^"), []string{"13|<nil>|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("xxx\\|!#^"), []string{"14|0|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 2"},
		{nil, []byte("xxx3\\2\\3\\4|!#^"), []string{"3|2|3|4"}, nil, trivialMsg},
		{nil, []byte("xxx4\\2\\\\3\\4|!#^"), []string{"4|2||3"}, nil, trivialMsg},
		{nil, []byte("xxx\\1\\2\\3\\4|!#^"), []string{"15|1|2|3"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("xxx6\\2\\3|!#^"), []string{"6|2|3|<nil>"}, nil, trivialMsg},
		{nil, []byte("xxx\\2\\3\\4|!#^xxx\\22\\33\\44|!#^"), []string{
			"16|2|3|4",
			"17|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{nil, []byte("\\2\\3\\4|!#^\\22\\33\\44|!#^xxx\\222\\333\\444|!#^"), []string{
			"18|222|333|444"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		// data1 != nil, data2 = nil
		{[]byte("xxx\\2\\3\\4"), nil, []string{"19|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("\\2\\3\\4|!#^"), nil, []string{}, nil, "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("\\2\\3\\4|!#^xxx18\\22\\33\\44|!#^"), nil,
			[]string{"18|22|33|44"}, nil, trivialMsg},

		// data1 != nil, data2 != nil
		{[]byte("xxx10\\2\\3"), []byte("\\4|!#^"),
			[]string{"10|2|3|4"}, nil, trivialMsg},
		{[]byte("10\\2\\3xx"), []byte("x11\\4\\5|!#^"),
			[]string{"11|4|5|<nil>"}, nil, trivialMsg},
		{[]byte("xxx21\\2\\3\\4\\5|!"), []byte("#^"),
			[]string{"21|2|3|4"}, nil, trivialMsg},
		{[]byte("xxx22\\2\\3\\4\\5|!"), []byte("#^xxx23\\22\\33\\44\\55|!#^"),
			[]string{"22|2|3|4", "23|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx23\\2\\3\\4\\5|!"), []byte("#^xxx24\\22\\33|!#^"),
			[]string{"23|2|3|4", "24|22|33|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx24\\2\\3\\4\\5|!"), []byte("#^xxx25\\22\\33|!#^xxx26\\222|!#^"),
			[]string{"24|2|3|4", "25|22|33|<nil>", "26|222|<nil>|<nil>"}, nil, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx25\\2\\3\\4\\5|!"), []byte("#^26\\22\\33|!#^xxx27\\222|!#^"),
			[]string{"25|2|3|4", "27|222|<nil>|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx\\2\\3"), []byte("4\\5|!#^"), []string{"28|2|34|5"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		// InsertData returns data isn't nil
		{nil, []byte("\\2\\3\\4|!#^"), nil, []byte("#^"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{nil, []byte("\\4\\5"), nil, []byte("\\5"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("\\2\\3"), []byte("\\4\\5"), nil, []byte("\\5"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx1\\2\\3|"), []byte("!#^\\4\\5|!#"),
			[]string{"1|2|3|<nil>"}, []byte("!#"), trivialMsg},
		{[]byte("xxx1\\2\\3\\4\\5|!"), []byte("#^xxx2\\22\\33|!#^3\\222|!#^"),
			[]string{"1|2|3|4", "2|22|33|<nil>"}, []byte("#^"), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xx1\\2\\3"), []byte("\\4\\5|!#^"), nil, []byte("#^"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

	// lines starting symbol is the same as terminated symbol, InsertData returns data is nil
	ld.LinesInfo.Terminated = "xxx"
	tests = []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("xxxxxx"), []string{"29|<nil>|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{nil, []byte("xxx3\\2\\3\\4xxx"), []string{"3|2|3|4"}, nil, trivialMsg},
		{nil, []byte("xxx\\2\\3\\4xxxxxx\\22\\33\\44xxx"),
			[]string{"30|2|3|4", "31|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},

		// data1 != nil, data2 = nil
		{[]byte("xxx\\2\\3\\4"), nil, []string{"32|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},

		// data1 != nil, data2 != nil
		{[]byte("xxx10\\2\\3"), []byte("\\4\\5xxx"), []string{"10|2|3|4"}, nil, trivialMsg},
		{[]byte("xxxxx10\\2\\3"), []byte("\\4\\5xxx"), []string{"33|2|3|4"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("xxx21\\2\\3\\4\\5xx"), []byte("x"), []string{"21|2|3|4"}, nil, trivialMsg},
		{[]byte("xxx32\\2\\3\\4\\5x"), []byte("xxxxx33\\22\\33\\44\\55xxx"),
			[]string{"32|2|3|4", "33|22|33|44"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx33\\2\\3\\4\\5xxx"), []byte("xxx34\\22\\33xxx"),
			[]string{"33|2|3|4", "34|22|33|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx34\\2\\3\\4\\5xx"), []byte("xxxx35\\22\\33xxxxxx36\\222xxx"),
			[]string{"34|2|3|4", "35|22|33|<nil>", "36|222|<nil>|<nil>"}, nil, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 0"},

		// InsertData returns data isn't nil
		{nil, []byte("\\2\\3\\4xxxx"), nil, []byte("xxxx"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("\\2\\3\\4xxx"), nil, []string{"37|<nil>|<nil>|<nil>"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
		{[]byte("\\2\\3\\4xxxxxx11\\22\\33\\44xxx"), nil,
			[]string{"38|<nil>|<nil>|<nil>", "39|<nil>|<nil>|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
		{[]byte("xx10\\2\\3"), []byte("\\4\\5xxx"), nil, []byte("xxx"), "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx10\\2\\3"), []byte("\\4xxxx"), []string{"10|2|3|4"}, []byte("x"), trivialMsg},
		{[]byte("xxx10\\2\\3\\4\\5x"), []byte("xx11\\22\\33xxxxxx12\\222xxx"),
			[]string{"10|2|3|4", "40|<nil>|<nil>|<nil>"}, []byte("xxx"), "Records: 2  Deleted: 0  Skipped: 0  Warnings: 1"},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

	// test line terminator in field quoter
	ld.LinesInfo.Terminated = "\n"
	ld.FieldsInfo.Enclosed = '"'
	tests = []testCase{
		{[]byte("xxx1\\1\\\"2\n\"\\3\nxxx4\\4\\\"5\n5\"\\6"), nil, []string{"1|1|2\n|3", "4|4|5\n5|6"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

	ld.LinesInfo.Terminated = "#\n"
	ld.FieldsInfo.Terminated = "#"
	tests = []testCase{
		{[]byte("xxx1#\nxxx2#\n"), nil, []string{"1|<nil>|<nil>|<nil>", "2|<nil>|<nil>|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx1#2#3#4#\nnxxx2#3#4#5#\n"), nil, []string{"1|2|3|4", "2|3|4|5"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("xxx1#2#\"3#\"#\"4\n\"#\nxxx2#3#\"#4#\n\"#5#\n"), nil, []string{"1|2|3#|4", "2|3|#4#\n|5"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

	ld.LinesInfo.Terminated = "#"
	ld.FieldsInfo.Terminated = "##"
	ld.LinesInfo.Starting = ""
	tests = []testCase{
		{[]byte("1#2#"), nil, []string{"1|<nil>|<nil>|<nil>", "2|<nil>|<nil>|<nil>"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("1##2##3##4#2##3##4##5#"), nil, []string{"1|2|3|4", "2|3|4|5"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("1##2##\"3##\"##\"4\n\"#2##3##\"##4#\"##5#"), nil, []string{"1|2|3##|4", "2|3|##4#|5"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

func (s *testSuite4) TestLoadDataEscape(c *C) {
	trivialMsg := "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table load_data_test")
	ctx := tk.Se.(sessionctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)
	// test escape
	tests := []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("1\ta string\n"), []string{"1|a string"}, nil, trivialMsg},
		{nil, []byte("2\tstr \\t\n"), []string{"2|str \t"}, nil, trivialMsg},
		{nil, []byte("3\tstr \\n\n"), []string{"3|str \n"}, nil, trivialMsg},
		{nil, []byte("4\tboth \\t\\n\n"), []string{"4|both \t\n"}, nil, trivialMsg},
		{nil, []byte("5\tstr \\\\\n"), []string{"5|str \\"}, nil, trivialMsg},
		{nil, []byte("6\t\\r\\t\\n\\0\\Z\\b\n"), []string{"6|" + string([]byte{'\r', '\t', '\n', 0, 26, '\b'})}, nil, trivialMsg},
		{nil, []byte("7\trtn0ZbN\n"), []string{"7|" + string([]byte{'r', 't', 'n', '0', 'Z', 'b', 'N'})}, nil, trivialMsg},
		{nil, []byte("8\trtn0Zb\\N\n"), []string{"8|" + string([]byte{'r', 't', 'n', '0', 'Z', 'b', 'N'})}, nil, trivialMsg},
		{nil, []byte("9\ttab\\	tab\n"), []string{"9|tab	tab"}, nil, trivialMsg},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

// TestLoadDataSpecifiedColumns reuse TestLoadDataEscape's test case :-)
func (s *testSuite4) TestLoadDataSpecifiedColumns(c *C) {
	trivialMsg := "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec(`create table load_data_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 varchar(255) default "def", c3 int default 0);`)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table load_data_test (c1, c2)")
	ctx := tk.Se.(sessionctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)
	// test
	tests := []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("7\ta string\n"), []string{"1|7|a string|0"}, nil, trivialMsg},
		{nil, []byte("8\tstr \\t\n"), []string{"2|8|str \t|0"}, nil, trivialMsg},
		{nil, []byte("9\tstr \\n\n"), []string{"3|9|str \n|0"}, nil, trivialMsg},
		{nil, []byte("10\tboth \\t\\n\n"), []string{"4|10|both \t\n|0"}, nil, trivialMsg},
		{nil, []byte("11\tstr \\\\\n"), []string{"5|11|str \\|0"}, nil, trivialMsg},
		{nil, []byte("12\t\\r\\t\\n\\0\\Z\\b\n"), []string{"6|12|" + string([]byte{'\r', '\t', '\n', 0, 26, '\b'}) + "|0"}, nil, trivialMsg},
		{nil, []byte("\\N\ta string\n"), []string{"7|<nil>|a string|0"}, nil, trivialMsg},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

func (s *testSuite4) TestLoadDataIgnoreLines(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table load_data_test ignore 1 lines")
	ctx := tk.Se.(sessionctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)
	tests := []testCase{
		{nil, []byte("1\tline1\n2\tline2\n"), []string{"2|line2"}, nil, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 0"},
		{nil, []byte("1\tline1\n2\tline2\n3\tline3\n"), []string{"2|line2", "3|line3"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 0"},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

// TestLoadDataOverflowBigintUnsigned related to issue 6360
func (s *testSuite4) TestLoadDataOverflowBigintUnsigned(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (a bigint unsigned);")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table load_data_test")
	ctx := tk.Se.(sessionctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ok, IsTrue)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	c.Assert(ld, NotNil)
	tests := []testCase{
		{nil, []byte("-1\n-18446744073709551615\n-18446744073709551616\n"), []string{"0", "0", "0"}, nil, "Records: 3  Deleted: 0  Skipped: 0  Warnings: 3"},
		{nil, []byte("-9223372036854775809\n18446744073709551616\n"), []string{"0", "18446744073709551615"}, nil, "Records: 2  Deleted: 0  Skipped: 0  Warnings: 2"},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

func (s *testSuite4) TestLoadDataIntoPartitionedTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table range_t (a int, b int) partition by range (a) ( " +
		"partition p0 values less than (4)," +
		"partition p1 values less than (7)," +
		"partition p2 values less than (11))")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table range_t fields terminated by ','")
	ctx := tk.Se.(sessionctx.Context)
	ld := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	c.Assert(ctx.NewTxn(context.Background()), IsNil)

	_, _, err := ld.InsertData(context.Background(), nil, []byte("1,2\n3,4\n5,6\n7,8\n9,10\n"))
	c.Assert(err, IsNil)
	err = ld.CheckAndInsertOneBatch(context.Background(), ld.GetRows(), ld.GetCurBatchCnt())
	c.Assert(err, IsNil)
	ld.SetMaxRowsInBatch(20000)
	ld.SetMessage()
	ctx.StmtCommit()
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testSuite4) TestNullDefault(c *C) {
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

func (s *testSuite4) TestNotNullDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists t1,t2;")
	defer tk.MustExec("drop table t1,t2")
	tk.MustExec("create table t1 (a int not null default null default 1);")
	tk.MustExec("create table t2 (a int);")
	tk.MustExec("alter table  t2 change column a a int not null default null default 1;")
}

func (s *testBypassSuite) TestLatch(c *C) {
	store, err := mockstore.NewMockStore(
		// Small latch slot size to make conflicts.
		mockstore.WithTxnLocalLatches(64),
	)
	c.Assert(err, IsNil)
	defer func() {
		err := store.Close()
		c.Assert(err, IsNil)
	}()

	dom, err1 := session.BootstrapSession(store)
	c.Assert(err1, IsNil)
	defer dom.Close()

	tk1 := testkit.NewTestKit(c, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t")
	tk1.MustExec("create table t (id int)")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = true")

	tk2 := testkit.NewTestKit(c, store)
	tk2.MustExec("use test")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = true")

	fn := func() {
		tk1.MustExec("begin")
		for i := 0; i < 100; i++ {
			tk1.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		}
		tk2.MustExec("begin")
		for i := 100; i < 200; i++ {
			tk1.MustExec(fmt.Sprintf("insert into t values (%d)", i))
		}
		tk2.MustExec("commit")
	}

	// txn1 and txn2 data range do not overlap, using latches should not
	// result in txn conflict.
	fn()
	tk1.MustExec("commit")

	tk1.MustExec("truncate table t")
	fn()
	tk1.MustExec("commit")

	// Test the error type of latch and it could be retry if TiDB enable the retry.
	tk1.MustExec("begin")
	tk1.MustExec("update t set id = id + 1")
	tk2.MustExec("update t set id = id + 1")
	_, err = tk1.Exec("commit")
	c.Assert(kv.ErrWriteConflictInTiDB.Equal(err), IsTrue)

	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("update t set id = id + 1")
	tk2.MustExec("update t set id = id + 1")
	tk1.MustExec("commit")
}

// TestIssue4067 Test issue https://github.com/pingcap/tidb/issues/4067
func (s *testSuite7) TestIssue4067(c *C) {
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

func (s *testSuite7) TestInsertCalculatedValue(c *C) {
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
	tk.MustQuery("select * from t where c = 1").Check(testkit.Rows(`{"a": 1} 2 1`))
	tk.MustExec("truncate table t")
	tk.MustExec("insert t set b = c + 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustExec("truncate table t")
	tk.MustExec(`insert t set a = '{"a": 1}', b = c`)
	tk.MustQuery("select * from t").Check(testkit.Rows(`{"a": 1} <nil> 1`))

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

func (s *testSuite7) TestDataTooLongErrMsg(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(2));")
	_, err := tk.Exec("insert into t values('123');")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(err.Error(), Equals, "[types:1406]Data too long for column 'a' at row 1")
	tk.MustExec("insert into t values('12')")
	_, err = tk.Exec("update t set a = '123' where a = '12';")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(err.Error(), Equals, "[types:1406]Data too long for column 'a' at row 1")
}

func (s *testSuite7) TestUpdateSelect(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table msg (id varchar(8), b int, status int, primary key (id, b))")
	tk.MustExec("insert msg values ('abc', 1, 1)")
	tk.MustExec("create table detail (id varchar(8), start varchar(8), status int, index idx_start(start))")
	tk.MustExec("insert detail values ('abc', '123', 2)")
	tk.MustExec("UPDATE msg SET msg.status = (SELECT detail.status FROM detail WHERE msg.id = detail.id)")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("admin check table msg")
}

func (s *testSuite7) TestUpdateDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE ttt (id bigint(20) NOT NULL, host varchar(30) NOT NULL, PRIMARY KEY (id), UNIQUE KEY i_host (host));")
	tk.MustExec("insert into ttt values (8,8),(9,9);")

	tk.MustExec("begin")
	tk.MustExec("update ttt set id = 0, host='9' where id = 9 limit 1;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("delete from ttt where id = 0 limit 1;")
	tk.MustQuery("select * from ttt use index (i_host) order by host;").Check(testkit.Rows("8 8"))
	tk.MustExec("update ttt set id = 0, host='8' where id = 8 limit 1;")
	tk.CheckLastMessage("Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("delete from ttt where id = 0 limit 1;")
	tk.MustQuery("select * from ttt use index (i_host) order by host;").Check(testkit.Rows())
	tk.MustExec("commit")
	tk.MustExec("admin check table ttt;")
	tk.MustExec("drop table ttt")
}

func (s *testSuite7) TestUpdateAffectRowCnt(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table a(id int auto_increment, a int default null, primary key(id))")
	tk.MustExec("insert into a values (1, 1001), (2, 1001), (10001, 1), (3, 1)")
	tk.MustExec("update a set id = id*10 where a = 1001")
	ctx := tk.Se.(sessionctx.Context)
	c.Assert(ctx.GetSessionVars().StmtCtx.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")

	tk.MustExec("drop table a")
	tk.MustExec("create table a ( a bigint, b bigint)")
	tk.MustExec("insert into a values (1, 1001), (2, 1001), (10001, 1), (3, 1)")
	tk.MustExec("update a set a = a*10 where b = 1001")
	ctx = tk.Se.(sessionctx.Context)
	c.Assert(ctx.GetSessionVars().StmtCtx.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Rows matched: 2  Changed: 2  Warnings: 0")
}

func (s *testSuite7) TestReplaceLog(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table testLog (a int not null primary key, b int unique key);`)

	// Make some dangling index.
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	is := s.domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("testLog")
	tbl, err := is.TableByName(dbName, tblName)
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("b")
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(1), kv.IntHandle(1), nil)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	_, err = tk.Exec(`replace into testLog values (0, 0), (1, 1);`)
	c.Assert(err, NotNil)
	expErr := errors.New(`can not be duplicated row, due to old row not found. handle 1 not found`)
	c.Assert(expErr.Error() == err.Error(), IsTrue, Commentf("obtained error: (%s)\nexpected error: (%s)", err.Error(), expErr.Error()))

	tk.MustQuery(`admin cleanup index testLog b;`).Check(testkit.Rows("1"))
}

// TestRebaseIfNeeded is for issue 7422.
// There is no need to do the rebase when updating a record if the auto-increment ID not changed.
// This could make the auto ID increasing speed slower.
func (s *testSuite7) TestRebaseIfNeeded(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int not null primary key auto_increment, b int unique key);`)
	tk.MustExec(`insert into t (b) values (1);`)

	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	tbl, err := s.domain.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(s.ctx.NewTxn(context.Background()), IsNil)
	// AddRecord directly here will skip to rebase the auto ID in the insert statement,
	// which could simulate another TiDB adds a large auto ID.
	_, err = tbl.AddRecord(s.ctx, types.MakeDatums(30001, 2))
	c.Assert(err, IsNil)
	txn, err := s.ctx.Txn(true)
	c.Assert(err, IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)

	tk.MustExec(`update t set b = 3 where a = 30001;`)
	tk.MustExec(`insert into t (b) values (4);`)
	tk.MustQuery(`select a from t where b = 4;`).Check(testkit.Rows("2"))

	tk.MustExec(`insert into t set b = 3 on duplicate key update a = a;`)
	tk.MustExec(`insert into t (b) values (5);`)
	tk.MustQuery(`select a from t where b = 5;`).Check(testkit.Rows("4"))

	tk.MustExec(`insert into t set b = 3 on duplicate key update a = a + 1;`)
	tk.MustExec(`insert into t (b) values (6);`)
	tk.MustQuery(`select a from t where b = 6;`).Check(testkit.Rows("30003"))
}

func (s *testSuite7) TestDeferConstraintCheckForDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set tidb_constraint_check_in_place = 0")
	tk.MustExec("set @@tidb_txn_mode = 'optimistic'")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2, t3, t4, t5")
	tk.MustExec("create table t1(i int primary key, j int)")
	tk.MustExec("insert into t1 values(1, 2)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(1, 3)")
	tk.MustExec("delete from t1 where j = 3")
	_, err := tk.Exec("commit")
	c.Assert(err.Error(), Equals, "previous statement: delete from t1 where j = 3: [kv:1062]Duplicate entry '1' for key 'PRIMARY'")
	tk.MustExec("rollback")

	tk.MustExec("create table t2(i int, j int, unique index idx(i))")
	tk.MustExec("insert into t2 values(1, 2)")
	tk.MustExec("begin")
	tk.MustExec("insert into t2 values(1, 3)")
	tk.MustExec("delete from t2 where j = 3")
	_, err = tk.Exec("commit")
	c.Assert(err.Error(), Equals, "previous statement: delete from t2 where j = 3: [kv:1062]Duplicate entry '1' for key 'idx'")
	tk.MustExec("admin check table t2")

	tk.MustExec("create table t3(i int, j int, primary key(i))")
	tk.MustExec("begin")
	tk.MustExec("insert into t3 values(1, 3)")
	tk.MustExec("delete from t3 where j = 3")
	tk.MustExec("commit")

	tk.MustExec("create table t4(i int, j int, primary key(i))")
	tk.MustExec("begin")
	tk.MustExec("insert into t4 values(1, 3)")
	tk.MustExec("delete from t4 where j = 3")
	tk.MustExec("insert into t4 values(2, 3)")
	tk.MustExec("commit")
	tk.MustExec("admin check table t4")
	tk.MustQuery("select * from t4").Check(testkit.Rows("2 3"))

	tk.MustExec("create table t5(i int, j int, primary key(i))")
	tk.MustExec("begin")
	tk.MustExec("insert into t5 values(1, 3)")
	tk.MustExec("delete from t5 where j = 3")
	tk.MustExec("insert into t5 values(1, 4)")
	tk.MustExec("commit")
	tk.MustExec("admin check table t5")
	tk.MustQuery("select * from t5").Check(testkit.Rows("1 4"))
}

func (s *testSuite7) TestDeferConstraintCheckForInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)

	tk.MustExec(`drop table if exists t;create table t (a int primary key, b int);`)
	tk.MustExec(`insert into t values (1,2),(2,2)`)
	_, err := tk.Exec("update t set a=a+1 where b=2")
	c.Assert(err, NotNil)

	tk.MustExec(`drop table if exists t;create table t (i int key);`)
	tk.MustExec(`insert t values (1);`)
	tk.MustExec(`set tidb_constraint_check_in_place = 1;`)
	tk.MustExec(`begin;`)
	_, err = tk.Exec(`insert t values (1);`)
	c.Assert(err, NotNil)
	tk.MustExec(`update t set i = 2 where i = 1;`)
	tk.MustExec(`commit;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows("2"))

	tk.MustExec(`set tidb_constraint_check_in_place = 0;`)
	tk.MustExec("replace into t values (1),(2)")
	tk.MustExec("begin")
	_, err = tk.Exec("update t set i = 2 where i = 1")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values (1) on duplicate key update i = i + 1")
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	tk.MustExec(`drop table t; create table t (id int primary key, v int unique);`)
	tk.MustExec(`insert into t values (1, 1)`)
	tk.MustExec(`set tidb_constraint_check_in_place = 1;`)
	tk.MustExec(`set @@autocommit = 0;`)

	_, err = tk.Exec("insert into t values (3, 1)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values (1, 3)")
	c.Assert(err, NotNil)
	tk.MustExec("commit")

	tk.MustExec(`set tidb_constraint_check_in_place = 0;`)
	tk.MustExec("insert into t values (3, 1)")
	tk.MustExec("insert into t values (1, 3)")
	_, err = tk.Exec("commit")
	c.Assert(err, NotNil)
}

func (s *testSuite7) TestPessimisticDeleteYourWrites(c *C) {
	session1 := testkit.NewTestKitWithInit(c, s.store)
	session2 := testkit.NewTestKitWithInit(c, s.store)

	session1.MustExec("drop table if exists x;")
	session1.MustExec("create table x (id int primary key, c int);")

	session1.MustExec("set tidb_txn_mode = 'pessimistic'")
	session2.MustExec("set tidb_txn_mode = 'pessimistic'")

	session1.MustExec("begin;")
	session1.MustExec("insert into x select 1, 1")
	session1.MustExec("delete from x where id = 1")
	session2.MustExec("begin;")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		session2.MustExec("insert into x select 1, 2")
		wg.Done()
	}()
	session1.MustExec("commit;")
	wg.Wait()
	session2.MustExec("commit;")
	session2.MustQuery("select * from x").Check(testkit.Rows("1 2"))
}

func (s *testSuite7) TestDefEnumInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table test (id int, prescription_type enum('a','b','c','d','e','f') NOT NULL, primary key(id));")
	tk.MustExec("insert into test (id)  values (1)")
	tk.MustQuery("select prescription_type from test").Check(testkit.Rows("a"))
}

func (s *testSuite7) TestIssue11059(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t (pk int primary key, uk int unique, v int)")
	tk.MustExec("insert into t values (2, 11, 215)")
	tk.MustExec("insert into t values (3, 7, 2111)")
	_, err := tk.Exec("update t set pk = 2 where uk = 7")
	c.Assert(err, NotNil)
}

func (s *testSuite7) TestSetWithRefGenCol(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (i int, j int as (i+1) not null);`)
	tk.MustExec(`insert into t set i = j + 1;`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec(`insert into t set i = j + 100;`)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "100 101"))

	tk.MustExec(`create table te (i int)`)
	tk.MustExec(`insert into te set i = i + 10;`)
	tk.MustQuery("select * from te").Check(testkit.Rows("<nil>"))
	tk.MustExec(`insert into te set i = i;`)
	tk.MustQuery("select * from te").Check(testkit.Rows("<nil>", "<nil>"))

	tk.MustExec(`create table tn (i int not null)`)
	tk.MustExec(`insert into tn set i = i;`)
	tk.MustQuery("select * from tn").Check(testkit.Rows("0"))
	tk.MustExec(`insert into tn set i = i + 10;`)
	tk.MustQuery("select * from tn").Check(testkit.Rows("0", "10"))

	//
	tk.MustExec(`create table t1 (j int(11) GENERATED ALWAYS AS (i + 1) stored, i int(11) DEFAULT '10');`)
	tk.MustExec(`insert into t1 values()`)
	tk.MustQuery("select * from t1").Check(testkit.Rows("11 10"))
	tk.MustExec(`insert into t1 values()`)
	tk.MustQuery("select * from t1").Check(testkit.Rows("11 10", "11 10"))

	tk.MustExec(`create table t2 (j int(11) GENERATED ALWAYS AS (i + 1) stored not null, i int(11) DEFAULT '5');`)
	tk.MustExec(`insert into t2 set i = j + 9`)
	tk.MustQuery("select * from t2").Check(testkit.Rows("10 9"))
	_, err := tk.Exec(`insert into t2 set j = i + 1`)
	c.Assert(err, NotNil)
	tk.MustExec(`insert into t2 set i = j + 100`)
	tk.MustQuery("select * from t2").Check(testkit.Rows("10 9", "101 100"))

	tk.MustExec(`create table t3(j int(11) GENERATED ALWAYS AS (i + 1) stored, i int(11) DEFAULT '5');`)
	tk.MustExec(`insert into t3 set i = j + 100`)
	tk.MustQuery("select * from t3").Check(testkit.Rows("<nil> <nil>"))
	_, err = tk.Exec(`insert into t3 set j = i + 1`)
	c.Assert(err, NotNil)
}

func (s *testSuite7) TestSetWithCurrentTimestampAndNow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists tbl;`)
	tk.MustExec(`create table t1(c1 timestamp default current_timestamp, c2 int, c3 timestamp default current_timestamp);`)
	// c1 insert using now() function result, c3 using default value calculation, should be same
	tk.MustExec(`insert into t1 set c1 = current_timestamp, c2 = sleep(2);`)
	tk.MustQuery("select c1 = c3 from t1").Check(testkit.Rows("1"))
	tk.MustExec(`insert into t1 set c1 = current_timestamp, c2 = sleep(1);`)
	tk.MustQuery("select c1 = c3 from t1").Check(testkit.Rows("1", "1"))
}

func (s *testSuite7) TestApplyWithPointAndBatchPointGet(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t ( c_int int, c_str varchar(40),c_datetime datetime, c_timestamp timestamp,
c_double double, c_decimal decimal(12, 6) , primary key(c_int, c_str) , unique key(c_int) , unique key(c_str) ,
unique key(c_decimal) , unique key(c_datetime) , key(c_timestamp) );`)
	tk.MustExec(`insert into t values (1, 'zen ardinghelli', '2020-02-03 18:15:17', '2020-03-11 05:47:11', 36.226534, 3.763),
(2, 'suspicious joliot', '2020-01-01 22:56:37', '2020-04-07 06:19:07', 62.756537, 5.567),
(3, 'keen zhukovsky', '2020-01-21 04:09:20', '2020-06-06 08:32:14', 33.115065, 1.381),
(4, 'crazy newton', '2020-02-14 21:37:56', '2020-04-28 08:33:48', 44.146318, 4.249),
(5, 'happy black', '2020-03-12 16:04:14', '2020-01-18 09:17:37', 41.962653, 5.959);`)
	tk.MustExec(`insert into t values (6, 'vigilant swartz', '2020-06-01 07:37:44', '2020-05-25 01:26:43', 56.352233, 2.202),
(7, 'suspicious germain', '2020-04-16 23:25:23', '2020-03-17 05:06:57', 55.897698, 3.460),
(8, 'festive chandrasekhar', '2020-02-11 23:40:29', '2020-04-08 10:13:04', 77.565691, 0.540),
(9, 'vigorous meninsky', '2020-02-17 10:03:17', '2020-01-02 15:02:02', 6.484815, 6.292),
(10, 'heuristic moser', '2020-04-20 12:18:49', '2020-06-20 20:20:18', 28.023822, 2.765);`)
	tk.MustExec(`insert into t values (11, 'sharp carver', '2020-03-01 11:23:41', '2020-03-23 17:59:05', 40.842442, 6.345),
(12, 'trusting noether', '2020-03-28 06:42:34', '2020-01-27 15:33:40', 49.544658, 4.811),
(13, 'objective ishizaka', '2020-01-28 17:30:55', '2020-04-02 17:45:39', 59.523930, 5.015),
(14, 'sad rhodes', '2020-03-30 21:43:37', '2020-06-09 06:53:53', 87.295753, 2.413),
(15, 'wonderful shockley', '2020-04-29 09:17:11', '2020-03-14 04:36:51', 6.778588, 8.497);`)
	tk.MustExec("begin pessimistic")
	tk.MustExec(`insert into t values (13, 'vibrant yalow', '2020-05-15 06:59:05', '2020-05-03 05:58:45', 43.721929, 8.066),
(14, 'xenodochial spence', '2020-02-13 17:28:07', '2020-04-01 12:18:30', 19.981331, 5.774),
(22, 'eloquent neumann', '2020-02-10 16:00:20', '2020-03-28 00:24:42', 10.702532, 7.618)
on duplicate key update c_int=values(c_int), c_str=values(c_str), c_double=values(c_double), c_timestamp=values(c_timestamp);`)
	// Test pointGet.
	tk.MustQuery(`select sum((select t1.c_str from t t1 where t1.c_int = 11 and t1.c_str > t.c_str order by t1.c_decimal limit 1) is null) nulls
from t order by c_str;`).Check(testkit.Rows("10"))
	// Test batchPointGet
	tk.MustQuery(`select sum((select t1.c_str from t t1 where t1.c_int in (11, 10086) and t1.c_str > t.c_str order by t1.c_decimal limit 1) is null) nulls
from t order by c_str;`).Check(testkit.Rows("10"))
	tk.MustExec("commit")
	tk.MustQuery(`select sum((select t1.c_str from t t1 where t1.c_int = 11 and t1.c_str > t.c_str order by t1.c_decimal limit 1) is null) nulls
from t order by c_str;`).Check(testkit.Rows("10"))
	// Test batchPointGet
	tk.MustQuery(`select sum((select t1.c_str from t t1 where t1.c_int in (11, 10086) and t1.c_str > t.c_str order by t1.c_decimal limit 1) is null) nulls
from t order by c_str;`).Check(testkit.Rows("10"))
}

func (s *testSuite4) TestWriteListPartitionTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (id int, name varchar(10), unique index idx (id)) partition by list  (id) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
	);`)

	// Test insert,update,delete
	tk.MustExec("insert into t values  (1, 'a')")
	tk.MustExec("update t set name='b' where id=2;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 a"))
	tk.MustExec("update t set name='b' where id=1;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 b"))
	tk.MustExec("replace into t values  (1, 'c')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 c"))
	tk.MustExec("insert into t values (1, 'd') on duplicate key update name='e'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 e"))
	tk.MustExec("delete from t where id=1")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values  (2, 'f')")
	tk.MustExec("delete from t where name='f'")
	tk.MustQuery("select * from t").Check(testkit.Rows())

	// Test insert error
	tk.MustExec("insert into t values  (1, 'a')")
	_, err := tk.Exec("insert into t values (1, 'd')")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1' for key 'idx'")
	_, err = tk.Exec("insert into t values (100, 'd')")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 100")
	tk.MustExec("admin check table t;")

	// Test select partition
	tk.MustExec("insert into t values  (2,'b'),(3,'c'),(4,'d'),(7,'f'), (null,null)")
	tk.MustQuery("select * from t partition (p0) order by id").Check(testkit.Rows("3 c"))
	tk.MustQuery("select * from t partition (p1,p3) order by id").Check(testkit.Rows("<nil> <nil>", "1 a", "2 b", "7 f"))
	tk.MustQuery("select * from t partition (p1,p3,p0,p2) order by id").Check(testkit.Rows("<nil> <nil>", "1 a", "2 b", "3 c", "4 d", "7 f"))
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("<nil> <nil>", "1 a", "2 b", "3 c", "4 d", "7 f"))
	tk.MustExec("delete from t partition (p0)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("<nil> <nil>", "1 a", "2 b", "4 d", "7 f"))
	tk.MustExec("delete from t partition (p3,p2)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 a", "2 b"))
}

func (s *testSuite4) TestWriteListColumnsPartitionTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (id int, name varchar(10), unique index idx (id)) partition by list columns (id) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
	);`)

	// Test insert,update,delete
	tk.MustExec("insert into t values  (1, 'a')")
	tk.MustExec("update t set name='b' where id=2;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 a"))
	tk.MustExec("update t set name='b' where id=1;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 b"))
	tk.MustExec("replace into t values  (1, 'c')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 c"))
	tk.MustExec("insert into t values (1, 'd') on duplicate key update name='e'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 e"))
	tk.MustExec("delete from t where id=1")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values  (2, 'f')")
	tk.MustExec("delete from t where name='f'")
	tk.MustQuery("select * from t").Check(testkit.Rows())

	// Test insert error
	tk.MustExec("insert into t values  (1, 'a')")
	_, err := tk.Exec("insert into t values (1, 'd')")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1' for key 'idx'")
	_, err = tk.Exec("insert into t values (100, 'd')")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")
	tk.MustExec("admin check table t;")

	// Test select partition
	tk.MustExec("insert into t values  (2,'b'),(3,'c'),(4,'d'),(7,'f'), (null,null)")
	tk.MustQuery("select * from t partition (p0) order by id").Check(testkit.Rows("3 c"))
	tk.MustQuery("select * from t partition (p1,p3) order by id").Check(testkit.Rows("<nil> <nil>", "1 a", "2 b", "7 f"))
	tk.MustQuery("select * from t partition (p1,p3,p0,p2) order by id").Check(testkit.Rows("<nil> <nil>", "1 a", "2 b", "3 c", "4 d", "7 f"))
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("<nil> <nil>", "1 a", "2 b", "3 c", "4 d", "7 f"))
	tk.MustExec("delete from t partition (p0)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("<nil> <nil>", "1 a", "2 b", "4 d", "7 f"))
	tk.MustExec("delete from t partition (p3,p2)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 a", "2 b"))
}

// TestWriteListPartitionTable1 test for write list partition when the partition expression is simple.
func (s *testSuite4) TestWriteListPartitionTable1(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (id int, name varchar(10)) partition by list  (id) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
	);`)

	// Test add unique index failed.
	tk.MustExec("insert into t values  (1, 'a'),(1,'b')")
	_, err := tk.Exec("alter table t add unique index idx (id)")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1' for key 'idx'")
	// Test add unique index success.
	tk.MustExec("delete from t where name='b'")
	tk.MustExec("alter table t add unique index idx (id)")

	// --------------------------Test insert---------------------------
	// Test insert 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values  (1, 'a'),(2,'b'),(10,'c')")
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 a", "2 b", "10 c"))
	// Test insert multi-partitions.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values  (1, 'a'),(3,'c'),(4,'e')")
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows("3 c"))
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 a"))
	tk.MustQuery("select * from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows())
	// Test insert on duplicate.
	tk.MustExec("insert into t values (1, 'd'), (3,'f'),(5,'g') on duplicate key update name='x'")
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows("3 x", "5 g"))
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 x"))
	tk.MustQuery("select * from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows())
	// Test insert on duplicate error
	_, err = tk.Exec("insert into t values (3, 'a'), (11,'x') on duplicate key update id=id+1")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '4' for key 'idx'")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 x", "3 x", "4 e", "5 g"))
	// Test insert ignore with duplicate
	tk.MustExec("insert ignore into t values  (1, 'b'), (5,'a'),(null,'y')")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'idx'", "Warning 1062 Duplicate entry '5' for key 'idx'"))
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows("3 x", "5 g"))
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 x"))
	tk.MustQuery("select * from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows("<nil> y"))
	// Test insert ignore without duplicate
	tk.MustExec("insert ignore into t values  (15, 'a'),(17,'a')")
	tk.MustQuery("select * from t partition(p0,p1,p2) order by id").Check(testkit.Rows("1 x", "3 x", "4 e", "5 g", "17 a"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows("<nil> y", "15 a"))
	// Test insert meet no partition error.
	_, err = tk.Exec("insert into t values (100, 'd')")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 100")

	// --------------------------Test update---------------------------
	// Test update 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values  (1, 'a'),(2,'b'),(3,'c')")
	tk.MustExec("update t set name='b' where id=2;")
	tk.MustQuery("select * from t partition(p1)").Check(testkit.Rows("1 a", "2 b"))
	tk.MustExec("update t set name='x' where id in (1,2)")
	tk.MustQuery("select * from t partition(p1)").Check(testkit.Rows("1 x", "2 x"))
	tk.MustExec("update t set name='y' where id < 3")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 y", "2 y", "3 c"))
	// Test update meet duplicate error.
	_, err = tk.Exec("update t set id=2 where id = 1")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '2' for key 'idx'")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 y", "2 y", "3 c"))

	// Test update multi-partitions
	tk.MustExec("update t set name='z' where id in (1,2,3);")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 z", "2 z", "3 z"))
	tk.MustExec("update t set name='a' limit 3")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 a", "2 a", "3 a"))
	tk.MustExec("update t set id=id*10 where id in (1,2)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))
	// Test update meet duplicate error.
	_, err = tk.Exec("update t set id=id+17 where id in (3,10)")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '20' for key 'idx'")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))
	// Test update meet no partition error.
	_, err = tk.Exec("update t set id=id*2 where id in (3,20)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 40")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))

	// --------------------------Test replace---------------------------
	// Test replace 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("replace into t values  (1, 'a'),(2,'b')")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 a", "2 b"))
	// Test replace multi-partitions.
	tk.MustExec("replace into t values  (3, 'c'),(4,'d'),(7,'f')")
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows("3 c"))
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 a", "2 b"))
	tk.MustQuery("select * from t partition(p2) order by id").Check(testkit.Rows("4 d"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows("7 f"))
	// Test replace on duplicate.
	tk.MustExec("replace into t values  (1, 'x'),(7,'x')")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 x", "2 b", "3 c", "4 d", "7 x"))
	// Test replace meet no partition error.
	_, err = tk.Exec("replace into t values  (10,'x'),(50,'x')")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 50")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 x", "2 b", "3 c", "4 d", "7 x"))

	// --------------------------Test delete---------------------------
	// Test delete 1 partition.
	tk.MustExec("delete from t where id = 3")
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows())
	tk.MustExec("delete from t where id in (1,2)")
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows())
	// Test delete multi-partitions.
	tk.MustExec("delete from t where id in (4,7,10,11)")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values  (3, 'c'),(4,'d'),(7,'f')")
	tk.MustExec("delete from t where id < 10")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values  (3, 'c'),(4,'d'),(7,'f')")
	tk.MustExec("delete from t limit 3")
	tk.MustQuery("select * from t").Check(testkit.Rows())
}

// TestWriteListPartitionTable2 test for write list partition when the partition expression is complicated and contain generated column.
func (s *testSuite4) TestWriteListPartitionTable2(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (id int, name varchar(10),b int generated always as (length(name)+1) virtual)
      partition by list  (id*2 + b*b + b*b - b*b*2 - abs(id)) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
	);`)

	// Test add unique index failed.
	tk.MustExec("insert into t (id,name) values  (1, 'a'),(1,'b')")
	_, err := tk.Exec("alter table t add unique index idx (id,b)")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1-2' for key 'idx'")
	// Test add unique index success.
	tk.MustExec("delete from t where name='b'")
	tk.MustExec("alter table t add unique index idx (id,b)")

	// --------------------------Test insert---------------------------
	// Test insert 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t (id,name) values  (1, 'a'),(2,'b'),(10,'c')")
	tk.MustQuery("select id,name from t partition(p1) order by id").Check(testkit.Rows("1 a", "2 b", "10 c"))
	// Test insert multi-partitions.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t (id,name) values  (1, 'a'),(3,'c'),(4,'e')")
	tk.MustQuery("select id,name from t partition(p0) order by id").Check(testkit.Rows("3 c"))
	tk.MustQuery("select id,name from t partition(p1) order by id").Check(testkit.Rows("1 a"))
	tk.MustQuery("select id,name from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select id,name from t partition(p3) order by id").Check(testkit.Rows())
	// Test insert on duplicate.
	tk.MustExec("insert into t (id,name) values (1, 'd'), (3,'f'),(5,'g') on duplicate key update name='x'")
	tk.MustQuery("select id,name from t partition(p0) order by id").Check(testkit.Rows("3 x", "5 g"))
	tk.MustQuery("select id,name from t partition(p1) order by id").Check(testkit.Rows("1 x"))
	tk.MustQuery("select id,name from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select id,name from t partition(p3) order by id").Check(testkit.Rows())
	// Test insert on duplicate error
	_, err = tk.Exec("insert into t (id,name) values (3, 'a'), (11,'x') on duplicate key update id=id+1")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '4-2' for key 'idx'")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 x", "3 x", "4 e", "5 g"))
	// Test insert ignore with duplicate
	tk.MustExec("insert ignore into t (id,name) values  (1, 'b'), (5,'a'),(null,'y')")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1-2' for key 'idx'", "Warning 1062 Duplicate entry '5-2' for key 'idx'"))
	tk.MustQuery("select id,name from t partition(p0) order by id").Check(testkit.Rows("3 x", "5 g"))
	tk.MustQuery("select id,name from t partition(p1) order by id").Check(testkit.Rows("1 x"))
	tk.MustQuery("select id,name from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select id,name from t partition(p3) order by id").Check(testkit.Rows("<nil> y"))
	// Test insert ignore without duplicate
	tk.MustExec("insert ignore into t (id,name) values  (15, 'a'),(17,'a')")
	tk.MustQuery("select id,name from t partition(p0,p1,p2) order by id").Check(testkit.Rows("1 x", "3 x", "4 e", "5 g", "17 a"))
	tk.MustQuery("select id,name from t partition(p3) order by id").Check(testkit.Rows("<nil> y", "15 a"))
	// Test insert meet no partition error.
	_, err = tk.Exec("insert into t (id,name) values (100, 'd')")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 100")

	// --------------------------Test update---------------------------
	// Test update 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t (id,name) values  (1, 'a'),(2,'b'),(3,'c')")
	tk.MustExec("update t set name='b' where id=2;")
	tk.MustQuery("select id,name from t partition(p1)").Check(testkit.Rows("1 a", "2 b"))
	tk.MustExec("update t set name='x' where id in (1,2)")
	tk.MustQuery("select id,name from t partition(p1)").Check(testkit.Rows("1 x", "2 x"))
	tk.MustExec("update t set name='y' where id < 3")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 y", "2 y", "3 c"))
	// Test update meet duplicate error.
	_, err = tk.Exec("update t set id=2 where id = 1")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '2-2' for key 'idx'")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 y", "2 y", "3 c"))

	// Test update multi-partitions
	tk.MustExec("update t set name='z' where id in (1,2,3);")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 z", "2 z", "3 z"))
	tk.MustExec("update t set name='a' limit 3")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 a", "2 a", "3 a"))
	tk.MustExec("update t set id=id*10 where id in (1,2)")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))
	// Test update meet duplicate error.
	_, err = tk.Exec("update t set id=id+17 where id in (3,10)")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '20-2' for key 'idx'")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))
	// Test update meet no partition error.
	_, err = tk.Exec("update t set id=id*2 where id in (3,20)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 40")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))

	// --------------------------Test replace---------------------------
	// Test replace 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("replace into t (id,name) values  (1, 'a'),(2,'b')")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 a", "2 b"))
	// Test replace multi-partitions.
	tk.MustExec("replace into t (id,name) values  (3, 'c'),(4,'d'),(7,'f')")
	tk.MustQuery("select id,name from t partition(p0) order by id").Check(testkit.Rows("3 c"))
	tk.MustQuery("select id,name from t partition(p1) order by id").Check(testkit.Rows("1 a", "2 b"))
	tk.MustQuery("select id,name from t partition(p2) order by id").Check(testkit.Rows("4 d"))
	tk.MustQuery("select id,name from t partition(p3) order by id").Check(testkit.Rows("7 f"))
	// Test replace on duplicate.
	tk.MustExec("replace into t (id,name) values  (1, 'x'),(7,'x')")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 x", "2 b", "3 c", "4 d", "7 x"))
	// Test replace meet no partition error.
	_, err = tk.Exec("replace into t (id,name) values  (10,'x'),(50,'x')")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 50")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 x", "2 b", "3 c", "4 d", "7 x"))

	// --------------------------Test delete---------------------------
	// Test delete 1 partition.
	tk.MustExec("delete from t where id = 3")
	tk.MustQuery("select id,name from t partition(p0) order by id").Check(testkit.Rows())
	tk.MustExec("delete from t where id in (1,2)")
	tk.MustQuery("select id,name from t partition(p1) order by id").Check(testkit.Rows())
	// Test delete multi-partitions.
	tk.MustExec("delete from t where id in (4,7,10,11)")
	tk.MustQuery("select id,name from t").Check(testkit.Rows())
	tk.MustExec("insert into t (id,name) values  (3, 'c'),(4,'d'),(7,'f')")
	tk.MustExec("delete from t where id < 10")
	tk.MustQuery("select id,name from t").Check(testkit.Rows())
	tk.MustExec("insert into t (id,name) values  (3, 'c'),(4,'d'),(7,'f')")
	tk.MustExec("delete from t limit 3")
	tk.MustQuery("select id,name from t").Check(testkit.Rows())
}

func (s *testSuite4) TestWriteListColumnsPartitionTable1(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")

	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (id int, name varchar(10)) partition by list columns (id) (
    	partition p0 values in (3,5,6,9,17),
    	partition p1 values in (1,2,10,11,19,20),
    	partition p2 values in (4,12,13,14,18),
    	partition p3 values in (7,8,15,16,null)
	);`)

	// Test add unique index failed.
	tk.MustExec("insert into t values  (1, 'a'),(1,'b')")
	_, err := tk.Exec("alter table t add unique index idx (id)")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1' for key 'idx'")
	// Test add unique index success.
	tk.MustExec("delete from t where name='b'")
	tk.MustExec("alter table t add unique index idx (id)")

	// --------------------------Test insert---------------------------
	// Test insert 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values  (1, 'a'),(2,'b'),(10,'c')")
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 a", "2 b", "10 c"))
	// Test insert multi-partitions.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values  (1, 'a'),(3,'c'),(4,'e')")
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows("3 c"))
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 a"))
	tk.MustQuery("select * from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows())
	// Test insert on duplicate.
	tk.MustExec("insert into t values (1, 'd'), (3,'f'),(5,'g') on duplicate key update name='x'")
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows("3 x", "5 g"))
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 x"))
	tk.MustQuery("select * from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows())
	// Test insert on duplicate error
	_, err = tk.Exec("insert into t values (3, 'a'), (11,'x') on duplicate key update id=id+1")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '4' for key 'idx'")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 x", "3 x", "4 e", "5 g"))
	// Test insert ignore with duplicate
	tk.MustExec("insert ignore into t values  (1, 'b'), (5,'a'),(null,'y')")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 'idx'", "Warning 1062 Duplicate entry '5' for key 'idx'"))
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows("3 x", "5 g"))
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 x"))
	tk.MustQuery("select * from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows("<nil> y"))
	// Test insert ignore without duplicate
	tk.MustExec("insert ignore into t values  (15, 'a'),(17,'a')")
	tk.MustQuery("select * from t partition(p0,p1,p2) order by id").Check(testkit.Rows("1 x", "3 x", "4 e", "5 g", "17 a"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows("<nil> y", "15 a"))
	// Test insert meet no partition error.
	_, err = tk.Exec("insert into t values (100, 'd')")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")

	// --------------------------Test update---------------------------
	// Test update 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values  (1, 'a'),(2,'b'),(3,'c')")
	tk.MustExec("update t set name='b' where id=2;")
	tk.MustQuery("select * from t partition(p1)").Check(testkit.Rows("1 a", "2 b"))
	tk.MustExec("update t set name='x' where id in (1,2)")
	tk.MustQuery("select * from t partition(p1)").Check(testkit.Rows("1 x", "2 x"))
	tk.MustExec("update t set name='y' where id < 3")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 y", "2 y", "3 c"))
	// Test update meet duplicate error.
	_, err = tk.Exec("update t set id=2 where id = 1")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '2' for key 'idx'")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 y", "2 y", "3 c"))

	// Test update multi-partitions
	tk.MustExec("update t set name='z' where id in (1,2,3);")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 z", "2 z", "3 z"))
	tk.MustExec("update t set name='a' limit 3")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 a", "2 a", "3 a"))
	tk.MustExec("update t set id=id*10 where id in (1,2)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))
	// Test update meet duplicate error.
	_, err = tk.Exec("update t set id=id+17 where id in (3,10)")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '20' for key 'idx'")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))
	// Test update meet no partition error.
	_, err = tk.Exec("update t set id=id*2 where id in (3,20)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))

	// --------------------------Test replace---------------------------
	// Test replace 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("replace into t values  (1, 'a'),(2,'b')")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 a", "2 b"))
	// Test replace multi-partitions.
	tk.MustExec("replace into t values  (3, 'c'),(4,'d'),(7,'f')")
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows("3 c"))
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 a", "2 b"))
	tk.MustQuery("select * from t partition(p2) order by id").Check(testkit.Rows("4 d"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows("7 f"))
	// Test replace on duplicate.
	tk.MustExec("replace into t values  (1, 'x'),(7,'x')")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 x", "2 b", "3 c", "4 d", "7 x"))
	// Test replace meet no partition error.
	_, err = tk.Exec("replace into t values  (10,'x'),(100,'x')")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 x", "2 b", "3 c", "4 d", "7 x"))

	// --------------------------Test delete---------------------------
	// Test delete 1 partition.
	tk.MustExec("delete from t where id = 3")
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows())
	tk.MustExec("delete from t where id in (1,2)")
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows())
	// Test delete multi-partitions.
	tk.MustExec("delete from t where id in (4,7,10,11)")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values  (3, 'c'),(4,'d'),(7,'f')")
	tk.MustExec("delete from t where id < 10")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert into t values  (3, 'c'),(4,'d'),(7,'f')")
	tk.MustExec("delete from t limit 3")
	tk.MustQuery("select * from t").Check(testkit.Rows())
}

// TestWriteListColumnsPartitionTable2 test for write list partition when the partition by multi-columns.
func (s *testSuite4) TestWriteListColumnsPartitionTable2(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (location varchar(10), id int, a int) partition by list columns (location,id) (
    	partition p_west  values in (('w', 1),('w', 2),('w', 3),('w', 4)),
    	partition p_east  values in (('e', 5),('e', 6),('e', 7),('e', 8)),
    	partition p_north values in (('n', 9),('n',10),('n',11),('n',12)),
    	partition p_south values in (('s',13),('s',14),('s',15),('s',16))
	);`)

	// Test add unique index failed.
	tk.MustExec("insert into t values  ('w', 1, 1),('w', 1, 2)")
	_, err := tk.Exec("alter table t add unique index idx (location,id)")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry 'w-1' for key 'idx'")
	// Test add unique index success.
	tk.MustExec("delete from t where a=2")
	tk.MustExec("alter table t add unique index idx (location,id)")

	// --------------------------Test insert---------------------------
	// Test insert 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values  ('w', 1, 1),('w', 2, 2),('w', 3, 3)")
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 1", "w 2 2", "w 3 3"))
	// Test insert multi-partitions.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values  ('w', 1, 1),('e', 5, 5),('n', 9, 9)")
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 1"))
	tk.MustQuery("select * from t partition(p_east) order by id").Check(testkit.Rows("e 5 5"))
	tk.MustQuery("select * from t partition(p_north) order by id").Check(testkit.Rows("n 9 9"))
	tk.MustQuery("select * from t partition(p_south) order by id").Check(testkit.Rows())
	// Test insert on duplicate.
	tk.MustExec("insert into t values  ('w', 1, 1) on duplicate key update a=a+1")
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 2"))
	// Test insert on duplicate and move from partition 1 to partition 2
	tk.MustExec("insert into t values  ('w', 1, 1) on duplicate key update location='s', id=13")
	tk.MustQuery("select * from t partition(p_south) order by id").Check(testkit.Rows("s 13 2"))
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows())
	// Test insert on duplicate error
	tk.MustExec("insert into t values  ('w', 2, 2), ('w', 1, 1)")
	_, err = tk.Exec("insert into t values  ('w', 2, 3) on duplicate key update id=1")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry 'w-1' for key 'idx'")
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 1", "w 2 2"))
	// Test insert ignore with duplicate
	tk.MustExec("insert ignore into t values  ('w', 2, 2), ('w', 3, 3), ('n', 10, 10)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry 'w-2' for key 'idx'"))
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 1", "w 2 2", "w 3 3"))
	tk.MustQuery("select * from t partition(p_north) order by id").Check(testkit.Rows("n 9 9", "n 10 10"))
	// Test insert ignore without duplicate
	tk.MustExec("insert ignore into t values  ('w', 4, 4), ('s', 14, 14)")
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 1", "w 2 2", "w 3 3", "w 4 4"))
	tk.MustQuery("select * from t partition(p_south) order by id").Check(testkit.Rows("s 13 2", "s 14 14"))
	// Test insert meet no partition error.
	_, err = tk.Exec("insert into t values  ('w', 5, 5)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")
	_, err = tk.Exec("insert into t values  ('s', 5, 5)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")
	_, err = tk.Exec("insert into t values  ('s', 100, 5)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")
	_, err = tk.Exec("insert into t values  ('x', 1, 5)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")

	// --------------------------Test update---------------------------
	// Test update 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values  ('w', 1, 1),('w', 2, 2),('w', 3, 3)")
	tk.MustExec("update t set a=2 where a=1")
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 2", "w 2 2", "w 3 3"))
	tk.MustExec("update t set a=3 where location='w'")
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 3", "w 2 3", "w 3 3"))
	tk.MustExec("update t set a=4 where location='w' and id=1")
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 4", "w 2 3", "w 3 3"))
	tk.MustExec("update t set a=5 where id=1")
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 5", "w 2 3", "w 3 3"))
	tk.MustExec("update t set a=a+id where id>1")
	tk.MustQuery("select * from t partition(p_west) order by id,a").Check(testkit.Rows("w 1 5", "w 2 5", "w 3 6"))
	// Test update meet duplicate error.
	_, err = tk.Exec("update t set id=id+1 where location='w' and id<2")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry 'w-2' for key 'idx'")
	tk.MustQuery("select * from t partition(p_west) order by id,a").Check(testkit.Rows("w 1 5", "w 2 5", "w 3 6"))

	// Test update multi-partitions
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values  ('w', 1, 1), ('w', 2, 2), ('e', 8, 8),('n', 11, 11)")
	tk.MustExec("update t set a=a+1 where id < 20")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 2", "w 2 3", "e 8 9", "n 11 12"))
	tk.MustExec("update t set a=a+1 where location in ('w','s','n')")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 3", "w 2 4", "e 8 9", "n 11 13"))
	tk.MustExec("update t set a=a+1 where location in ('w','s','n') and id in (1,3,5,7,8,9,11)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 4", "w 2 4", "e 8 9", "n 11 14"))
	tk.MustExec("update t set a=a+1 where location='n' and id=12")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 4", "w 2 4", "e 8 9", "n 11 14"))
	tk.MustExec("update t set a=a+1 where location='n' and id=11")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 4", "w 2 4", "e 8 9", "n 11 15"))
	// Test update meet duplicate error.
	_, err = tk.Exec("update t set id=id+1 where location='w' and id in (1,2)")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry 'w-2' for key 'idx'")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 4", "w 2 4", "e 8 9", "n 11 15"))
	// Test update meet no partition error.
	_, err = tk.Exec("update t set id=id+3 where location='w' and id in (1,2)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 4", "w 2 4", "e 8 9", "n 11 15"))
	// Test update that move from partition 1 to partition 2.
	// TODO: fix this
	tk.MustExec("update t set location='s', id=14 where location='e' and id=8")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 4", "w 2 4", "n 11 15", "s 14 9"))

	// --------------------------Test replace---------------------------
	// Test replace 1 partition.
	tk.MustExec("delete from t")
	tk.MustExec("replace into t values  ('w', 1, 1),('w', 2, 2),('w', 3, 3)")
	tk.MustQuery("select * from t partition(p_west) order by id").Check(testkit.Rows("w 1 1", "w 2 2", "w 3 3"))
	// Test replace multi-partitions.
	tk.MustExec("delete from t")
	tk.MustExec("replace into t values  ('w', 1, 1),('e', 5, 5),('n', 9, 9)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 1", "e 5 5", "n 9 9"))
	// Test replace on duplicate.
	tk.MustExec("replace into t values  ('w', 1, 2),('n', 10, 10)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 2", "e 5 5", "n 9 9", "n 10 10"))
	// Test replace meet no partition error.
	_, err = tk.Exec("replace into t values  ('w', 5, 5)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")
	_, err = tk.Exec("replace into t values  ('s', 5, 5)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")
	_, err = tk.Exec("replace into t values  ('s', 100, 5)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")
	_, err = tk.Exec("replace into t values  ('x', 1, 5)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value from column_list")

	// --------------------------Test delete---------------------------
	// Test delete 1 partition.
	tk.MustExec("delete from t where location='w' and id=2")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("w 1 2", "e 5 5", "n 9 9", "n 10 10"))
	tk.MustExec("delete from t where location='w' and id=1")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("e 5 5", "n 9 9", "n 10 10"))
	// Test delete multi-partitions.
	tk.MustExec("delete from t where location in ('w','e','n') and id in (1,2,3,4,5,8,9)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("n 10 10"))
	tk.MustExec("delete from t where a=10")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows())
	tk.MustExec("replace into t values  ('w', 1, 1),('e', 5, 5),('n', 11, 11)")
	tk.MustExec("delete from t where id < 10")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("n 11 11"))
	tk.MustExec("delete from t limit 1")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows())
}

// TestWriteListColumnsPartitionTable2 test for write list partition when the partition by multi-columns.
func (s *testSuite4) TestWriteListPartitionTableIssue21437(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (a int) partition by list (a%10) (partition p0 values in (0,1));`)
	_, err := tk.Exec("replace into t values  (null)")
	c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value NULL")
}

func (s *testSuite4) TestListPartitionWithAutoRandom(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (a bigint key auto_random (3), b int) partition by list (a%5) (partition p0 values in (0,1,2), partition p1 values in (3,4));`)
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")
	tk.MustExec("replace into t values  (1,1)")
	result := []string{"1"}
	for i := 2; i < 100; i++ {
		sql := fmt.Sprintf("insert into t (b) values (%v)", i)
		tk.MustExec(sql)
		result = append(result, strconv.Itoa(i))
	}
	tk.MustQuery("select b from t order by b").Check(testkit.Rows(result...))
	tk.MustExec("update t set b=b+1 where a=1")
	tk.MustQuery("select b from t where a=1").Check(testkit.Rows("2"))
	tk.MustExec("update t set b=b+1 where a<2")
	tk.MustQuery("select b from t where a<2").Check(testkit.Rows("3"))
	tk.MustExec("insert into t values (1, 1) on duplicate key update b=b+1")
	tk.MustQuery("select b from t where a=1").Check(testkit.Rows("4"))
}

func (s *testSuite4) TestListPartitionWithAutoIncrement(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (a bigint key auto_increment, b int) partition by list (a%5) (partition p0 values in (0,1,2), partition p1 values in (3,4));`)
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")
	tk.MustExec("replace into t values  (1,1)")
	result := []string{"1"}
	for i := 2; i < 100; i++ {
		sql := fmt.Sprintf("insert into t (b) values (%v)", i)
		tk.MustExec(sql)
		result = append(result, strconv.Itoa(i))
	}
	tk.MustQuery("select b from t order by b").Check(testkit.Rows(result...))
	tk.MustExec("update t set b=b+1 where a=1")
	tk.MustQuery("select b from t where a=1").Check(testkit.Rows("2"))
	tk.MustExec("update t set b=b+1 where a<2")
	tk.MustQuery("select b from t where a<2").Check(testkit.Rows("3"))
	tk.MustExec("insert into t values (1, 1) on duplicate key update b=b+1")
	tk.MustQuery("select b from t where a=1").Check(testkit.Rows("4"))
}

func (s *testSuite4) TestListPartitionWithGeneratedColumn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	// Test for generated column with bigint type.
	tableDefs := []string{
		// Test for virtual generated column for list partition
		`create table t (a bigint, b bigint GENERATED ALWAYS AS (3*a - 2*a) VIRTUAL, index idx(a)) partition by list (5*b - 4*b) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10));`,
		// Test for stored generated column for list partition
		`create table t (a bigint, b bigint GENERATED ALWAYS AS (3*a - 2*a) STORED, index idx(a)) partition by list (5*b - 4*b) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10));`,
		// Test for virtual generated column for list columns partition
		`create table t (a bigint, b bigint GENERATED ALWAYS AS (3*a - 2*a) VIRTUAL, index idx(a)) partition by list columns(b) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10));`,
		// Test for stored generated column for list columns partition
		`create table t (a bigint, b bigint GENERATED ALWAYS AS (3*a - 2*a) STORED, index idx(a)) partition by list columns(b) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10));`,
	}
	for _, tbl := range tableDefs {
		tk.MustExec("drop table if exists t")
		tk.MustExec(tbl)
		// Test for insert
		tk.MustExec("insert into t (a) values (1),(3),(5),(7),(9)")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("1", "3", "5"))
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("7", "9"))
		tk.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 1"))
		// Test for update
		tk.MustExec("update t set a=a+1 where a = 1")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("2", "3", "5"))
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("7", "9"))
		tk.MustQuery("select * from t where a = 1").Check(testkit.Rows())
		tk.MustQuery("select * from t where a = 2").Check(testkit.Rows("2 2"))
		// Test for delete
		tk.MustExec("delete from t where a>10")
		tk.MustQuery("select count(1) from t").Check(testkit.Rows("5"))
		tk.MustExec("delete from t where a=9")
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("7"))
		tk.MustQuery("select count(1) from t").Check(testkit.Rows("4"))

		// Test for insert meet no partition error
		_, err := tk.Exec("insert into t (a) values (11)")
		c.Assert(table.ErrNoPartitionForGivenValue.Equal(err), IsTrue)
		// Test for update meet no partition error
		_, err = tk.Exec("update t set a=a+10 where a = 2")
		c.Assert(table.ErrNoPartitionForGivenValue.Equal(err), IsTrue)
	}
}

func (s *testSuite4) TestListPartitionWithGeneratedColumn1(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	// Test for generated column with year type.
	tableDefs := []string{
		// Test for virtual generated column for list partition
		`create table t (a year, b year GENERATED ALWAYS AS (3*a - 2*a) VIRTUAL, index idx(a)) partition by list (1 + b - 1) (partition p0 values in (2001,2002,2003,2004,2005), partition p1 values in (2006,2007,2008,2009));`,
		// Test for stored generated column for list partition
		`create table t (a year, b year GENERATED ALWAYS AS (3*a - 2*a) STORED, index idx(a)) partition by list (1 + b - 1) (partition p0 values in (2001,2002,2003,2004,2005), partition p1 values in (2006,2007,2008,2009));`,
	}
	for _, tbl := range tableDefs {
		tk.MustExec("drop table if exists t")
		tk.MustExec(tbl)
		// Test for insert
		tk.MustExec("insert into t (a) values (1),(3),(5),(7),(9)")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("2001", "2003", "2005"))
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("2007", "2009"))
		tk.MustQuery("select * from t where a = 1").Check(testkit.Rows("2001 2001"))
		// Test for update
		tk.MustExec("update t set a=a+1 where a = 1")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("2002", "2003", "2005"))
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("2007", "2009"))
		tk.MustQuery("select * from t where a = 1").Check(testkit.Rows())
		tk.MustQuery("select * from t where a = 2").Check(testkit.Rows("2002 2002"))
		// Test for delete
		tk.MustExec("delete from t where a>10")
		tk.MustQuery("select count(1) from t").Check(testkit.Rows("5"))
		tk.MustExec("delete from t where a=9")
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("2007"))
		tk.MustQuery("select count(1) from t").Check(testkit.Rows("4"))

		// Test for insert meet no partition error
		_, err := tk.Exec("insert into t (a) values (11)")
		c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 2011")
		// Test for update meet no partition error
		_, err = tk.Exec("update t set a=a+10 where a = 2")
		c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 2012")

		tk.MustExec("delete from t")

		// Test for insert
		tk.MustExec("insert into t (a) values (2001),(2003),(2005),(2007),(2009)")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("2001", "2003", "2005"))
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("2007", "2009"))
		tk.MustQuery("select * from t where a = 2001").Check(testkit.Rows("2001 2001"))
		// Test for update
		tk.MustExec("update t set a=a+1 where a = 2001")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("2002", "2003", "2005"))
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("2007", "2009"))
		tk.MustQuery("select * from t where a = 2001").Check(testkit.Rows())
		tk.MustQuery("select * from t where a = 2002").Check(testkit.Rows("2002 2002"))
		// Test for delete
		tk.MustExec("delete from t where a>2010")
		tk.MustQuery("select count(1) from t").Check(testkit.Rows("5"))
		tk.MustExec("delete from t where a=2009")
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("2007"))
		tk.MustQuery("select count(1) from t").Check(testkit.Rows("4"))

		// Test for insert meet no partition error
		_, err = tk.Exec("insert into t (a) values (2011)")
		c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 2011")
		// Test for update meet no partition error
		_, err = tk.Exec("update t set a=a+10 where a = 2002")
		c.Assert(err.Error(), Equals, "[table:1526]Table has no partition for value 2012")
	}
}

func (s *testSuite4) TestListPartitionWithGeneratedColumn2(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tableDefs := []string{
		// Test for virtual generated column for datetime type in list partition.
		`create table t (a datetime, b bigint GENERATED ALWAYS AS (to_seconds(a)) VIRTUAL, index idx(a)) partition by list (1 + b - 1) (
				partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')),
				partition p1 values in (to_seconds('2020-09-28 17:03:40'),to_seconds('2020-09-28 17:03:41')));`,
		// Test for stored generated column for datetime type in list partition.
		`create table t (a datetime, b bigint GENERATED ALWAYS AS (to_seconds(a)) STORED, index idx(a)) partition by list (1 + b - 1) (
				partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')),
				partition p1 values in (to_seconds('2020-09-28 17:03:40'),to_seconds('2020-09-28 17:03:41')));`,
		// Test for virtual generated column for timestamp type in list partition.
		`create table t (a timestamp, b bigint GENERATED ALWAYS AS (to_seconds(a)) VIRTUAL, index idx(a)) partition by list (1 + b - 1) (
				partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')),
				partition p1 values in (to_seconds('2020-09-28 17:03:40'),to_seconds('2020-09-28 17:03:41')));`,
		// Test for stored generated column for timestamp type in list partition.
		`create table t (a timestamp, b bigint GENERATED ALWAYS AS (to_seconds(a)) STORED, index idx(a)) partition by list (1 + b - 1) (
				partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')),
				partition p1 values in (to_seconds('2020-09-28 17:03:40'),to_seconds('2020-09-28 17:03:41')));`,
		// Test for virtual generated column for timestamp type in list columns partition.
		`create table t (a timestamp, b bigint GENERATED ALWAYS AS (to_seconds(a)) VIRTUAL, index idx(a)) partition by list columns(b) (
				partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')),
				partition p1 values in (to_seconds('2020-09-28 17:03:40'),to_seconds('2020-09-28 17:03:41')));`,
		// Test for stored generated column for timestamp type in list columns partition.
		`create table t (a timestamp, b bigint GENERATED ALWAYS AS (to_seconds(a)) STORED, index idx(a)) partition by list columns(b) (
				partition p0 values in (to_seconds('2020-09-28 17:03:38'),to_seconds('2020-09-28 17:03:39')),
				partition p1 values in (to_seconds('2020-09-28 17:03:40'),to_seconds('2020-09-28 17:03:41')));`,
	}
	for _, tbl := range tableDefs {
		tk.MustExec("drop table if exists t")
		tk.MustExec(tbl)
		tk.MustExec("insert into t (a) values  ('2020-09-28 17:03:38'),('2020-09-28 17:03:40')")
		tk.MustQuery("select a from t partition (p0)").Check(testkit.Rows("2020-09-28 17:03:38"))
		tk.MustQuery("select a from t where a = '2020-09-28 17:03:40'").Check(testkit.Rows("2020-09-28 17:03:40"))
		tk.MustExec("update t set a='2020-09-28 17:03:41' where a = '2020-09-28 17:03:38'")
		tk.MustQuery("select a from t partition (p0)").Check(testkit.Rows())
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("2020-09-28 17:03:40", "2020-09-28 17:03:41"))
	}
}

func (s *testSuite4) TestListColumnsPartitionWithGeneratedColumn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	// Test for generated column with substr expression.
	tableDefs := []string{
		// Test for virtual generated column
		`create table t (a varchar(10), b varchar(1) GENERATED ALWAYS AS (substr(a,1,1)) VIRTUAL, index idx(a)) partition by list columns(b) (partition p0 values in ('a','c'), partition p1 values in ('b','d'));`,
		// Test for stored generated column
		`create table t (a varchar(10), b varchar(1) GENERATED ALWAYS AS (substr(a,1,1)) STORED, index idx(a)) partition by list columns(b) (partition p0 values in ('a','c'), partition p1 values in ('b','d'));`,
	}
	for _, tbl := range tableDefs {
		tk.MustExec("drop table if exists t")
		tk.MustExec(tbl)
		tk.MustExec("insert into t (a) values  ('aaa'),('abc'),('acd')")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("aaa", "abc", "acd"))
		tk.MustQuery("select * from t where a = 'abc' order by a").Check(testkit.Rows("abc a"))
		tk.MustExec("update t set a='bbb' where a = 'aaa'")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("abc", "acd"))
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("bbb"))
		tk.MustQuery("select * from t where a = 'bbb' order by a").Check(testkit.Rows("bbb b"))
	}
}

func (s *testSerialSuite2) TestListColumnsPartitionWithGlobalIndex(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	// Test generated column with global index
	restoreConfig := config.RestoreFunc()
	defer restoreConfig()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = true
	})
	tableDefs := []string{
		// Test for virtual generated column with global index
		`create table t (a varchar(10), b varchar(1) GENERATED ALWAYS AS (substr(a,1,1)) VIRTUAL) partition by list columns(b) (partition p0 values in ('a','c'), partition p1 values in ('b','d'));`,
		// Test for stored generated column with global index
		`create table t (a varchar(10), b varchar(1) GENERATED ALWAYS AS (substr(a,1,1)) STORED) partition by list columns(b) (partition p0 values in ('a','c'), partition p1 values in ('b','d'));`,
	}
	for _, tbl := range tableDefs {
		tk.MustExec("drop table if exists t")
		tk.MustExec(tbl)
		tk.MustExec("alter table t add unique index (a)")
		tk.MustExec("insert into t (a) values  ('aaa'),('abc'),('acd')")
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("aaa", "abc", "acd"))
		tk.MustQuery("select * from t where a = 'abc' order by a").Check(testkit.Rows("abc a"))
		tk.MustExec("update t set a='bbb' where a = 'aaa'")
		tk.MustExec("admin check table t")
		tk.MustQuery("select a from t order by a").Check(testkit.Rows("abc", "acd", "bbb"))
		// TODO: fix below test.
		//tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("abc", "acd"))
		//tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("bbb"))
		tk.MustQuery("select * from t where a = 'bbb' order by a").Check(testkit.Rows("bbb b"))
		// Test insert meet duplicate error.
		_, err := tk.Exec("insert into t (a) values  ('abc')")
		c.Assert(err, NotNil)
		// Test insert on duplicate update
		tk.MustExec("insert into t (a) values ('abc') on duplicate key update a='bbc'")
		tk.MustQuery("select a from t order by a").Check(testkit.Rows("acd", "bbb", "bbc"))
		tk.MustQuery("select * from t where a = 'bbc'").Check(testkit.Rows("bbc b"))
		// TODO: fix below test.
		//tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("acd"))
		//tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("bbb", "bbc"))
	}
}

func (s *testSerialSuite) TestIssue20724(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a varchar(10) collate utf8mb4_general_ci)")
	tk.MustExec("insert into t1 values ('a')")
	tk.MustExec("update t1 set a = 'A'")
	tk.MustQuery("select * from t1").Check(testkit.Rows("A"))
	tk.MustExec("drop table t1")
}

func (s *testSerialSuite) TestIssue20840(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t1 (i varchar(20) unique key) collate=utf8mb4_general_ci")
	tk.MustExec("insert into t1 values ('a')")
	tk.MustExec("replace into t1 values ('A')")
	tk.MustQuery("select * from t1").Check(testkit.Rows("A"))
	tk.MustExec("drop table t1")
}

func (s *testSerialSuite) TestIssue22496(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t12")
	tk.MustExec("create table t12(d decimal(15,2));")
	_, err := tk.Exec("insert into t12 values('1,9999.00')")
	c.Assert(err, NotNil)
	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert into t12 values('1,999.00');")
	tk.MustQuery("SELECT * FROM t12;").Check(testkit.Rows("1.00"))
	tk.MustExec("drop table t12")
}

func (s *testSuite) TestEqualDatumsAsBinary(c *C) {
	tests := []struct {
		a    []interface{}
		b    []interface{}
		same bool
	}{
		// Positive cases
		{[]interface{}{1}, []interface{}{1}, true},
		{[]interface{}{1, "aa"}, []interface{}{1, "aa"}, true},
		{[]interface{}{1, "aa", 1}, []interface{}{1, "aa", 1}, true},

		// negative cases
		{[]interface{}{1}, []interface{}{2}, false},
		{[]interface{}{1, "a"}, []interface{}{1, "aaaaaa"}, false},
		{[]interface{}{1, "aa", 3}, []interface{}{1, "aa", 2}, false},

		// Corner cases
		{[]interface{}{}, []interface{}{}, true},
		{[]interface{}{nil}, []interface{}{nil}, true},
		{[]interface{}{}, []interface{}{1}, false},
		{[]interface{}{1}, []interface{}{1, 1}, false},
		{[]interface{}{nil}, []interface{}{1}, false},
	}
	for _, tt := range tests {
		testEqualDatumsAsBinary(c, tt.a, tt.b, tt.same)
	}
}

func (s *testSuite) TestIssue21232(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a varchar(1), index idx(a))")
	tk.MustExec("create table t1(a varchar(5), index idx(a))")
	tk.MustExec("insert into t values('a'), ('b')")
	tk.MustExec("insert into t1 values('a'), ('bbbbb')")
	tk.MustExec("update /*+ INL_JOIN(t) */ t, t1 set t.a='a' where t.a=t1.a")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from t").Check(testkit.Rows("a", "b"))
	tk.MustExec("update /*+ INL_HASH_JOIN(t) */ t, t1 set t.a='a' where t.a=t1.a")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from t").Check(testkit.Rows("a", "b"))
	tk.MustExec("update /*+ INL_MERGE_JOIN(t) */ t, t1 set t.a='a' where t.a=t1.a")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select * from t").Check(testkit.Rows("a", "b"))
}

func testEqualDatumsAsBinary(c *C, a []interface{}, b []interface{}, same bool) {
	sc := new(stmtctx.StatementContext)
	re := new(executor.ReplaceExec)
	sc.IgnoreTruncate = true
	res, err := re.EqualDatumsAsBinary(sc, types.MakeDatums(a...), types.MakeDatums(b...))
	c.Assert(err, IsNil)
	c.Assert(res, Equals, same, Commentf("a: %v, b: %v", a, b))
}
