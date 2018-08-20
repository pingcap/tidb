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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
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

	// for on duplicate key with ignore
	insertSQL = `INSERT IGNORE INTO insert_test (id, c3) VALUES (1, 2) ON DUPLICATE KEY UPDATE c3=values(c3)+c3+3;`
	tk.MustExec(insertSQL)
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

	// issue 6424
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a time(6))")
	tk.MustExec("insert into t value('20070219173709.055870'), ('20070219173709.055'), ('-20070219173709.055870'), ('20070219173709.055870123')")
	tk.MustQuery("select * from t").Check(testkit.Rows("17:37:09.055870", "17:37:09.055000", "17:37:09.055870", "17:37:09.055870"))
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t value(20070219173709.055870), (20070219173709.055), (20070219173709.055870123)")
	tk.MustQuery("select * from t").Check(testkit.Rows("17:37:09.055870", "17:37:09.055000", "17:37:09.055870"))
	_, err = tk.Exec("insert into t value(-20070219173709.055870)")
	c.Assert(err.Error(), Equals, "[types:1292]Incorrect time value: '-20070219173709.055870'")

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
    create table t (id int PRIMARY KEY AUTO_INCREMENT, c1 int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1, 2);`
	tk.MustExec(testSQL)

	r := tk.MustQuery("select * from t;")
	rowStr := fmt.Sprintf("%v %v", "1", "2")
	r.Check(testkit.Rows(rowStr))

	tk.MustExec("insert ignore into t values (1, 3), (2, 3)")
	r = tk.MustQuery("select * from t;")
	rowStr1 := fmt.Sprintf("%v %v", "2", "3")
	r.Check(testkit.Rows(rowStr, rowStr1))

	tk.MustExec("insert ignore into t values (3, 4), (3, 4)")
	r = tk.MustQuery("select * from t;")
	rowStr2 := fmt.Sprintf("%v %v", "3", "4")
	r.Check(testkit.Rows(rowStr, rowStr1, rowStr2))

	tk.MustExec("begin")
	tk.MustExec("insert ignore into t values (4, 4), (4, 5), (4, 6)")
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
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'i' cannot be null"))
	testSQL = `select * from badnull`
	tk.MustQuery(testSQL).Check(testkit.Rows("0"))
}

func (s *testSuite) TestInsertOnDup(c *C) {
	var cfg kv.InjectionConfig
	tk := testkit.NewTestKit(c, kv.NewInjectedStore(s.store, &cfg))
	tk.MustExec("use test")
	testSQL := `drop table if exists t;
    create table t (i int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1),(2);`
	tk.MustExec(testSQL)

	r := tk.MustQuery("select * from t;")
	rowStr1 := fmt.Sprintf("%v", "1")
	rowStr2 := fmt.Sprintf("%v", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))

	tk.MustExec("insert into t values (1), (2) on duplicate key update i = values(i)")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows(rowStr1, rowStr2))

	tk.MustExec("insert into t values (2), (3) on duplicate key update i = 3")
	r = tk.MustQuery("select * from t;")
	rowStr3 := fmt.Sprintf("%v", "3")
	r.Check(testkit.Rows(rowStr1, rowStr3))

	testSQL = `drop table if exists t;
    create table t (i int primary key, j int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (-1, 1);`
	tk.MustExec(testSQL)

	r = tk.MustQuery("select * from t;")
	rowStr1 = fmt.Sprintf("%v %v", "-1", "1")
	r.Check(testkit.Rows(rowStr1))

	tk.MustExec("insert into t values (1, 1) on duplicate key update j = values(j)")
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
	set tidb_max_chunk_size=1;
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
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))
	testSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testSQL)
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))

	testSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT AUTO_INCREMENT UNIQUE,
	f2 VARCHAR(5) NOT NULL UNIQUE);
	INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testSQL)
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))
	testSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testSQL)
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))

	testSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT);
	INSERT t1 VALUES (1) ON DUPLICATE KEY UPDATE f1 = 1;`
	tk.MustExec(testSQL)
	tk.MustQuery(`SELECT * FROM t1;`).Check(testkit.Rows("1"))

	testSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT PRIMARY KEY, f2 INT UNIQUE);
	INSERT t1 VALUES (1, 1);`
	tk.MustExec(testSQL)
	tk.MustExec(`INSERT t1 VALUES (1, 1), (1, 1) ON DUPLICATE KEY UPDATE f1 = 2, f2 = 2;`)
	tk.MustQuery(`SELECT * FROM t1 order by f1;`).Check(testkit.Rows("1 1", "2 2"))
}

func (s *testSuite) TestInsertIgnoreOnDup(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists t;
    create table t (i int not null primary key, j int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1, 1), (2, 2);`
	tk.MustExec(testSQL)
	testSQL = `insert ignore into t values(1, 1) on duplicate key update i = 2;`
	tk.MustExec(testSQL)
	testSQL = `select * from t;`
	r := tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 1", "2 2"))
	testSQL = `insert ignore into t values(1, 1) on duplicate key update j = 2;`
	tk.MustExec(testSQL)
	testSQL = `select * from t;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 1", "2 2"))
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

func (s *testSuite) TestPartitionedTableReplace(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_table_partition=1")
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

	tk.MustExec(`drop table if exists replace_test_1`)
	tk.MustExec(`create table replace_test_1 (id int, c1 int) partition by range (id) (
			PARTITION p0 VALUES LESS THAN (4),
			PARTITION p1 VALUES LESS THAN (6),
			PARTITION p2 VALUES LESS THAN (8),
			PARTITION p3 VALUES LESS THAN (10),
			PARTITION p4 VALUES LESS THAN (100))`)
	tk.MustExec(`replace replace_test_1 select id, c1 from replace_test;`)

	tk.MustExec(`drop table if exists replace_test_2`)
	tk.MustExec(`create table replace_test_2 (id int, c1 int) partition by range (id) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN (50),
			PARTITION p2 VALUES LESS THAN (100),
			PARTITION p3 VALUES LESS THAN (300))`)
	tk.MustExec(`replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`)

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
	replaceUniqueIndexSQL = `replace into replace_test_3 set c1=8, c2=8;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))

	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

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
	c.Assert(err.Error(), DeepEquals, "[table:1048]Column 'id' cannot be null")

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

	// issue 5532
	tk.MustExec("create table decimals (a decimal(20, 0) not null)")
	tk.MustExec("insert into decimals values (201)")
	// A warning rather than data truncated error.
	tk.MustExec("update decimals set a = a + 1.23;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1265 Data Truncated"))
	r = tk.MustQuery("select * from decimals")
	r.Check(testkit.Rows("202"))

	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE `t` (	`c1` year DEFAULT NULL, `c2` year DEFAULT NULL, `c3` date DEFAULT NULL, `c4` datetime DEFAULT NULL,	KEY `idx` (`c1`,`c2`))")
	_, err = tk.Exec("UPDATE t SET c2=16777215 WHERE c1>= -8388608 AND c1 < -9 ORDER BY c1 LIMIT 2")
	c.Assert(err.Error(), Equals, "cannot convert datum from bigint to type year.")

	tk.MustExec("update (select * from t) t set c1 = 1111111")
}

func (s *testSuite) TestPartitionedTableUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@session.tidb_enable_table_partition=1")
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
	r := tk.MustQuery(`SELECT * from t order by id limit 2;`)
	r.Check(testkit.Rows("1 abc", "7 abc"))

	// update partition column
	tk.MustExec(`update t set id = id + 1`)
	tk.CheckExecResult(2, 0)
	r = tk.MustQuery(`SELECT * from t order by id limit 2;`)
	r.Check(testkit.Rows("2 abc", "8 abc"))

	// update partition column, old and new record locates on different partitions
	tk.MustExec(`update t set id = 20 where id = 8`)
	tk.CheckExecResult(2, 0)
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
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '5' for key 'PRIMARY'"))
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("5", "7"))

	// test update ignore for truncate as warning
	_, err = tk.Exec("update ignore t set a = 1 where a = (select '2a')")
	c.Assert(err, IsNil)
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1265 Data Truncated", "Warning 1265 Data Truncated"))

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
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '5' for key 'I_uniq'"))
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("5", "7"))
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
	r.Check(testkit.Rows(`{"a": "测试"}`))
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

func (s *testSuite) TestPartitionedTableDelete(c *C) {
	createTable := `CREATE TABLE test.t (id int not null default 1, name varchar(255), index(id))
			  PARTITION BY RANGE ( id ) (
			  PARTITION p0 VALUES LESS THAN (6),
			  PARTITION p1 VALUES LESS THAN (11),
			  PARTITION p2 VALUES LESS THAN (16),
			  PARTITION p3 VALUES LESS THAN (21))`

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@session.tidb_enable_table_partition=1")
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
	r.Check(testkit.Rows("Warning 1265 Data Truncated", "Warning 1265 Data Truncated"))

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
	ctx := tk.Se.(sessionctx.Context)
	ld := makeLoadDataInfo(4, nil, ctx, c)

	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	// data1 = nil, data2 = nil, fields and lines is default
	ctx.GetSessionVars().StmtCtx.DupKeyAsWarning = true
	ctx.GetSessionVars().StmtCtx.BadNullAsWarning = true
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
		{nil, []byte("\n"), []string{"1|<nil>|<nil>|<nil>"}, nil},
		{nil, []byte("\t\n"), []string{"2|0|<nil>|<nil>"}, nil},
		{nil, []byte("3\t2\t3\t4\n"), []string{"3|2|3|4"}, nil},
		{nil, []byte("3*1\t2\t3\t4\n"), []string{"3|2|3|4"}, nil},
		{nil, []byte("4\t2\t\t3\t4\n"), []string{"4|2||3"}, nil},
		{nil, []byte("\t1\t2\t3\t4\n"), []string{"5|1|2|3"}, nil},
		{nil, []byte("6\t2\t3\n"), []string{"6|2|3|<nil>"}, nil},
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
	c.Assert(sc.WarningCount(), Equals, uint16(1))

	// lines starting symbol is "" and terminated symbol length is 2, InsertData returns data is nil
	ld.LinesInfo.Terminated = "||"
	tests = []testCase{
		// data1 != nil, data2 != nil
		{[]byte("0\t2\t3"), []byte("\t4\t5||"), []string{"12|2|3|4"}, nil},
		{[]byte("1\t2\t3\t4\t5|"), []byte("|"), []string{"1|2|3|4"}, nil},
		{[]byte("2\t2\t3\t4\t5|"), []byte("|3\t22\t33\t44\t55||"),
			[]string{"2|2|3|4", "3|22|33|44"}, nil},
		{[]byte("3\t2\t3\t4\t5|"), []byte("|4\t22\t33||"), []string{
			"3|2|3|4", "4|22|33|<nil>"}, nil},
		{[]byte("4\t2\t3\t4\t5|"), []byte("|5\t22\t33||6\t222||"),
			[]string{"4|2|3|4", "5|22|33|<nil>", "6|222|<nil>|<nil>"}, nil},
		{[]byte("6\t2\t3"), []byte("4\t5||"), []string{"6|2|34|5"}, nil},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

	// fields and lines aren't default, InsertData returns data is nil
	ld.FieldsInfo.Terminated = "\\"
	ld.LinesInfo.Starting = "xxx"
	ld.LinesInfo.Terminated = "|!#^"
	tests = []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("xxx|!#^"), []string{"13|<nil>|<nil>|<nil>"}, nil},
		{nil, []byte("xxx\\|!#^"), []string{"14|0|<nil>|<nil>"}, nil},
		{nil, []byte("xxx3\\2\\3\\4|!#^"), []string{"3|2|3|4"}, nil},
		{nil, []byte("xxx4\\2\\\\3\\4|!#^"), []string{"4|2||3"}, nil},
		{nil, []byte("xxx\\1\\2\\3\\4|!#^"), []string{"15|1|2|3"}, nil},
		{nil, []byte("xxx6\\2\\3|!#^"), []string{"6|2|3|<nil>"}, nil},
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
			[]string{"11|4|5|<nil>"}, nil},
		{[]byte("xxx21\\2\\3\\4\\5|!"), []byte("#^"),
			[]string{"21|2|3|4"}, nil},
		{[]byte("xxx22\\2\\3\\4\\5|!"), []byte("#^xxx23\\22\\33\\44\\55|!#^"),
			[]string{"22|2|3|4", "23|22|33|44"}, nil},
		{[]byte("xxx23\\2\\3\\4\\5|!"), []byte("#^xxx24\\22\\33|!#^"),
			[]string{"23|2|3|4", "24|22|33|<nil>"}, nil},
		{[]byte("xxx24\\2\\3\\4\\5|!"), []byte("#^xxx25\\22\\33|!#^xxx26\\222|!#^"),
			[]string{"24|2|3|4", "25|22|33|<nil>", "26|222|<nil>|<nil>"}, nil},
		{[]byte("xxx25\\2\\3\\4\\5|!"), []byte("#^26\\22\\33|!#^xxx27\\222|!#^"),
			[]string{"25|2|3|4", "27|222|<nil>|<nil>"}, nil},
		{[]byte("xxx\\2\\3"), []byte("4\\5|!#^"), []string{"28|2|34|5"}, nil},

		// InsertData returns data isn't nil
		{nil, []byte("\\2\\3\\4|!#^"), nil, []byte("#^")},
		{nil, []byte("\\4\\5"), nil, []byte("\\5")},
		{[]byte("\\2\\3"), []byte("\\4\\5"), nil, []byte("\\5")},
		{[]byte("xxx1\\2\\3|"), []byte("!#^\\4\\5|!#"),
			[]string{"1|2|3|<nil>"}, []byte("!#")},
		{[]byte("xxx1\\2\\3\\4\\5|!"), []byte("#^xxx2\\22\\33|!#^3\\222|!#^"),
			[]string{"1|2|3|4", "2|22|33|<nil>"}, []byte("#^")},
		{[]byte("xx1\\2\\3"), []byte("\\4\\5|!#^"), nil, []byte("#^")},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)

	// lines starting symbol is the same as terminated symbol, InsertData returns data is nil
	ld.LinesInfo.Terminated = "xxx"
	tests = []testCase{
		// data1 = nil, data2 != nil
		{nil, []byte("xxxxxx"), []string{"29|<nil>|<nil>|<nil>"}, nil},
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
			[]string{"33|2|3|4", "34|22|33|<nil>"}, nil},
		{[]byte("xxx34\\2\\3\\4\\5xx"), []byte("xxxx35\\22\\33xxxxxx36\\222xxx"),
			[]string{"34|2|3|4", "35|22|33|<nil>", "36|222|<nil>|<nil>"}, nil},

		// InsertData returns data isn't nil
		{nil, []byte("\\2\\3\\4xxxx"), nil, []byte("xxxx")},
		{[]byte("\\2\\3\\4xxx"), nil, []string{"37|<nil>|<nil>|<nil>"}, nil},
		{[]byte("\\2\\3\\4xxxxxx11\\22\\33\\44xxx"), nil,
			[]string{"38|<nil>|<nil>|<nil>", "39|<nil>|<nil>|<nil>"}, nil},
		{[]byte("xx10\\2\\3"), []byte("\\4\\5xxx"), nil, []byte("xxx")},
		{[]byte("xxx10\\2\\3"), []byte("\\4xxxx"), []string{"10|2|3|4"}, []byte("x")},
		{[]byte("xxx10\\2\\3\\4\\5x"), []byte("xx11\\22\\33xxxxxx12\\222xxx"),
			[]string{"10|2|3|4", "40|<nil>|<nil>|<nil>"}, []byte("xxx")},
	}
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

func (s *testSuite) TestLoadDataEscape(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec("CREATE TABLE load_data_test (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table load_data_test")
	ctx := tk.Se.(sessionctx.Context)
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
		{nil, []byte("7\trtn0ZbN\n"), []string{"7|" + string([]byte{'r', 't', 'n', '0', 'Z', 'b', 'N'})}, nil},
		{nil, []byte("8\trtn0Zb\\N\n"), []string{"8|" + string([]byte{'r', 't', 'n', '0', 'Z', 'b', 'N'})}, nil},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

// TestLoadDataSpecifiedColumns reuse TestLoadDataEscape's test case :-)
func (s *testSuite) TestLoadDataSpecifiedColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test; drop table if exists load_data_test;")
	tk.MustExec(`create table load_data_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 varchar(255) default "def", c3 int default 0);`)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table load_data_test (c1, c2)")
	ctx := tk.Se.(sessionctx.Context)
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
		{nil, []byte("\\N\ta string\n"), []string{"7|<nil>|a string|0"}, nil},
	}
	deleteSQL := "delete from load_data_test"
	selectSQL := "select * from load_data_test;"
	checkCases(tests, ld, c, tk, ctx, selectSQL, deleteSQL)
}

func makeLoadDataInfo(column int, specifiedColumns []string, ctx sessionctx.Context, c *C) (ld *executor.LoadDataInfo) {
	dom := domain.GetDomain(ctx)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("load_data_test"))
	c.Assert(err, IsNil)
	columns := tbl.Cols()
	// filter specified columns
	if len(specifiedColumns) > 0 {
		columns, err = table.FindCols(columns, specifiedColumns, true)
		c.Assert(err, IsNil)
	}
	fields := &ast.FieldsClause{Terminated: "\t"}
	lines := &ast.LinesClause{Starting: "", Terminated: "\n"}
	ld = executor.NewLoadDataInfo(ctx, make([]types.Datum, column), tbl, columns)
	ld.SetMaxRowsInBatch(0)
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

func (s *testBypassSuite) TestBypassLatch(c *C) {
	store, err := mockstore.NewMockTikvStore(
		// Small latch slot size to make conflicts.
		mockstore.WithTxnLocalLatches(64),
	)
	c.Assert(err, IsNil)
	defer store.Close()

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

	// txn1 and txn2 data range do not overlap, but using latches result in txn conflict.
	fn()
	_, err = tk1.Exec("commit")
	c.Assert(err, NotNil)

	tk1.MustExec("truncate table t")
	fn()
	txn := tk1.Se.Txn()
	txn.SetOption(kv.BypassLatch, true)
	// Bypass latch, there will be no conflicts.
	tk1.MustExec("commit")
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

func (s *testSuite) TestDataTooLongErrMsg(c *C) {
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

func (s *testSuite) TestUpdateSelect(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table msg (id varchar(8), b int, status int, primary key (id, b))")
	tk.MustExec("insert msg values ('abc', 1, 1)")
	tk.MustExec("create table detail (id varchar(8), start varchar(8), status int, index idx_start(start))")
	tk.MustExec("insert detail values ('abc', '123', 2)")
	tk.MustExec("UPDATE msg SET msg.status = (SELECT detail.status FROM detail WHERE msg.id = detail.id)")
	tk.MustExec("admin check table msg")
}

func (s *testSuite) TestUpdateDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE ttt (id bigint(20) NOT NULL, host varchar(30) NOT NULL, PRIMARY KEY (id), UNIQUE KEY i_host (host));")
	tk.MustExec("insert into ttt values (8,8),(9,9);")

	tk.MustExec("begin")
	tk.MustExec("update ttt set id = 0, host='9' where id = 9 limit 1;")
	tk.MustExec("delete from ttt where id = 0 limit 1;")
	tk.MustQuery("select * from ttt use index (i_host) order by host;").Check(testkit.Rows("8 8"))
	tk.MustExec("update ttt set id = 0, host='8' where id = 8 limit 1;")
	tk.MustExec("delete from ttt where id = 0 limit 1;")
	tk.MustQuery("select * from ttt use index (i_host) order by host;").Check(testkit.Rows())
	tk.MustExec("commit")
	tk.MustExec("admin check table ttt;")
	tk.MustExec("drop table ttt")
}

func (s *testSuite) TestUpdateAffectRowCnt(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table a(id int auto_increment, a int default null, primary key(id))")
	tk.MustExec("insert into a values (1, 1001), (2, 1001), (10001, 1), (3, 1)")
	tk.MustExec("update a set id = id*10 where a = 1001")
	ctx := tk.Se.(sessionctx.Context)
	c.Assert(ctx.GetSessionVars().StmtCtx.AffectedRows(), Equals, uint64(2))

	tk.MustExec("drop table a")
	tk.MustExec("create table a ( a bigint, b bigint)")
	tk.MustExec("insert into a values (1, 1001), (2, 1001), (10001, 1), (3, 1)")
	tk.MustExec("update a set a = a*10 where b = 1001")
	ctx = tk.Se.(sessionctx.Context)
	c.Assert(ctx.GetSessionVars().StmtCtx.AffectedRows(), Equals, uint64(2))
}
