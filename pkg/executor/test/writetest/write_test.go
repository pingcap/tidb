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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package writetest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/internal"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testSQL := `drop table if exists insert_test;create table insert_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	testSQL = `insert insert_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)
	require.Equal(t, tk.Session().LastMessage(), "Records: 3  Duplicates: 0  Warnings: 0")

	errInsertSelectSQL := `insert insert_test (c1) values ();`
	tk.MustExec("begin")
	err := tk.ExecToErr(errInsertSelectSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	err = tk.ExecToErr(errInsertSelectSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test (xxx) values (3);`
	tk.MustExec("begin")
	err = tk.ExecToErr(errInsertSelectSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test_xxx (c1) values ();`
	tk.MustExec("begin")
	err = tk.ExecToErr(errInsertSelectSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	insertSetSQL := `insert insert_test set c1 = 3;`
	tk.MustExec(insertSetSQL)
	require.Empty(t, tk.Session().LastMessage())

	errInsertSelectSQL = `insert insert_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	err = tk.ExecToErr(errInsertSelectSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test set xxx = 6;`
	tk.MustExec("begin")
	err = tk.ExecToErr(errInsertSelectSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	insertSelectSQL := `create table insert_test_1 (id int, c1 int);`
	tk.MustExec(insertSelectSQL)
	insertSelectSQL = `insert insert_test_1 select id, c1 from insert_test;`
	tk.MustExec(insertSelectSQL)
	require.Equal(t, tk.Session().LastMessage(), "Records: 4  Duplicates: 0  Warnings: 0")

	insertSelectSQL = `create table insert_test_2 (id int, c1 int);`
	tk.MustExec(insertSelectSQL)
	insertSelectSQL = `insert insert_test_1 select id, c1 from insert_test union select id * 10, c1 * 10 from insert_test;`
	tk.MustExec(insertSelectSQL)
	require.Equal(t, tk.Session().LastMessage(), "Records: 8  Duplicates: 0  Warnings: 0")

	errInsertSelectSQL = `insert insert_test_1 select c1 from insert_test;`
	tk.MustExec("begin")
	err = tk.ExecToErr(errInsertSelectSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test_1 values(default, default, default, default, default)`
	tk.MustExec("begin")
	err = tk.ExecToErr(errInsertSelectSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	// Updating column is PK handle.
	// Make sure the record is "1, 1, nil, 1".
	r := tk.MustQuery("select * from insert_test where id = 1;")
	rowStr := fmt.Sprintf("%v %v %v %v", "1", "1", nil, "1")
	r.Check(testkit.Rows(rowStr))
	insertSQL := `insert into insert_test (id, c3) values (1, 2) on duplicate key update id=values(id), c2=10;`
	tk.MustExec(insertSQL)
	require.Empty(t, tk.Session().LastMessage())
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "1")
	r.Check(testkit.Rows(rowStr))

	insertSQL = `insert into insert_test (id, c2) values (1, 1) on duplicate key update insert_test.c2=10;`
	tk.MustExec(insertSQL)
	require.Empty(t, tk.Session().LastMessage())

	err = tk.ExecToErr(`insert into insert_test (id, c2) values(1, 1) on duplicate key update t.c2 = 10`)
	require.Error(t, err)

	// for on duplicate key
	insertSQL = `INSERT INTO insert_test (id, c3) VALUES (1, 2) ON DUPLICATE KEY UPDATE c3=values(c3)+c3+3;`
	tk.MustExec(insertSQL)
	require.Empty(t, tk.Session().LastMessage())
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "6")
	r.Check(testkit.Rows(rowStr))

	// for on duplicate key with ignore
	insertSQL = `INSERT IGNORE INTO insert_test (id, c3) VALUES (1, 2) ON DUPLICATE KEY UPDATE c3=values(c3)+c3+3;`
	tk.MustExec(insertSQL)
	require.Empty(t, tk.Session().LastMessage())
	r = tk.MustQuery("select * from insert_test where id = 1;")
	rowStr = fmt.Sprintf("%v %v %v %v", "1", "1", "10", "11")
	r.Check(testkit.Rows(rowStr))

	tk.MustExec("create table insert_err (id int, c1 varchar(8))")
	err = tk.ExecToErr("insert insert_err values (1, 'abcdabcdabcd')")
	require.True(t, types.ErrDataTooLong.Equal(err))
	err = tk.ExecToErr("insert insert_err values (1, '你好，世界')")
	require.NoError(t, err)

	tk.MustExec("create table TEST1 (ID INT NOT NULL, VALUE INT DEFAULT NULL, PRIMARY KEY (ID))")
	err = tk.ExecToErr("INSERT INTO TEST1(id,value) VALUE(3,3) on DUPLICATE KEY UPDATE VALUE=4")
	require.NoError(t, err)
	require.Empty(t, tk.Session().LastMessage())

	tk.MustExec("create table t (id int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("update t t1 set id = (select count(*) + 1 from t t2 where t1.id = t2.id)")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("2"))

	// issue 3235
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c decimal(5, 5))")
	err = tk.ExecToErr("insert into t value(0)")
	require.NoError(t, err)
	err = tk.ExecToErr("insert into t value(1)")
	require.True(t, types.ErrWarnDataOutOfRange.Equal(err))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c binary(255))")
	err = tk.ExecToErr("insert into t value(1)")
	require.NoError(t, err)
	r = tk.MustQuery("select length(c) from t;")
	r.Check(testkit.Rows("255"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c varbinary(255))")
	err = tk.ExecToErr("insert into t value(1)")
	require.NoError(t, err)
	r = tk.MustQuery("select length(c) from t;")
	r.Check(testkit.Rows("1"))

	// issue 3509
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c int)")
	tk.MustExec("set @origin_time_zone = @@time_zone")
	tk.MustExec("set @@time_zone = '+08:00'")
	err = tk.ExecToErr("insert into t value(Unix_timestamp('2002-10-27 01:00'))")
	require.NoError(t, err)
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows("1035651600"))
	tk.MustExec("set @@time_zone = @origin_time_zone")

	// issue 3832
	tk.MustExec("create table t1 (b char(0));")
	err = tk.ExecToErr(`insert into t1 values ("");`)
	require.NoError(t, err)

	// issue 3895
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4,2));")
	tk.MustExec("INSERT INTO t VALUES (1.000001);")
	r = tk.MustQuery("SHOW WARNINGS;")
	// TODO: MySQL8.0 reports Note 1265 Data truncated for column 'a' at row 1
	r.Check(testkit.Rows("Warning 1366 Incorrect decimal value: '1.000001' for column 'a' at row 1"))
	tk.MustExec("INSERT INTO t VALUES (1.000000);")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows())

	// issue 4653
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a datetime);")
	err = tk.ExecToErr("INSERT INTO t VALUES('2017-00-00')")
	require.Error(t, err)
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
	err = tk.ExecToErr("insert into t value (-1);")
	require.True(t, types.ErrWarnDataOutOfRange.Equal(err))
	tk.MustExec("set @@sql_mode = '';")
	tk.MustExec("insert into t value (-1);")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1264 Out of range value for column 'a' at row 1"))
	tk.MustExec("insert into t select -1;")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1690 constant -1 overflows bigint"))
	tk.MustExec("insert into t select cast(-1 as unsigned);")
	tk.MustExec("insert into t value (-1.111);")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1264 Out of range value for column 'a' at row 1"))
	tk.MustExec("insert into t value ('-1.111');")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1264 Out of range value for column 'a' at row 1"))
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
	err = tk.ExecToErr("insert into t value(-20070219173709.055870)")
	require.EqualError(t, err, "[table:1292]Incorrect time value: '-20070219173709.055870' for column 'a' at row 1")

	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@sql_mode=''")
	tk.MustExec("create table t(a float unsigned, b double unsigned)")
	tk.MustExec("insert into t value(-1.1, -1.1), (-2.1, -2.1), (0, 0), (1.1, 1.1)")
	tk.MustQuery("show warnings").
		Check(testkit.Rows("Warning 1264 Out of range value for column 'a' at row 1", "Warning 1264 Out of range value for column 'b' at row 1",
			"Warning 1264 Out of range value for column 'a' at row 2", "Warning 1264 Out of range value for column 'b' at row 2"))
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
	err = tk.ExecToErr("insert into v values(1,2)")
	require.EqualError(t, err, "insert into view v is not supported now")
	err = tk.ExecToErr("replace into v values(1,2)")
	require.EqualError(t, err, "replace into view v is not supported now")
	tk.MustExec("drop view v")

	tk.MustExec("create sequence seq")
	err = tk.ExecToErr("insert into seq values()")
	require.EqualError(t, err, "insert into sequence seq is not supported now")
	err = tk.ExecToErr("replace into seq values()")
	require.EqualError(t, err, "replace into sequence seq is not supported now")
	tk.MustExec("drop sequence seq")

	// issue 22851
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(name varchar(255), b int, c int, primary key(name(2)))")
	tk.MustExec("insert into t(name, b) values(\"cha\", 3)")
	err = tk.ExecToErr("insert into t(name, b) values(\"chb\", 3)")
	require.EqualError(t, err, "[kv:1062]Duplicate entry 'ch' for key 't.PRIMARY'")
	tk.MustExec("insert into t(name, b) values(\"测试\", 3)")
	err = tk.ExecToErr("insert into t(name, b) values(\"测试\", 3)")
	require.EqualError(t, err, "[kv:1062]Duplicate entry '\xe6\xb5' for key 't.PRIMARY'")
}

func TestInsertAutoInc(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
	err := tk.ExecToErr(insertSQL)
	// ERROR 1062 (23000): Duplicate entry '0' for key 'PRIMARY'
	require.Error(t, err)
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

func TestInsertIgnore(t *testing.T) {
	store := testkit.CreateMockStore(t)
	var cfg kv.InjectionConfig
	tk := testkit.NewTestKit(t, kv.NewInjectedStore(store, &cfg))
	tk.MustExec("use test")
	testSQL := `drop table if exists t;
    create table t (id int PRIMARY KEY AUTO_INCREMENT, c1 int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1, 2);`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())

	r := tk.MustQuery("select * from t;")
	rowStr := fmt.Sprintf("%v %v", "1", "2")
	r.Check(testkit.Rows(rowStr))

	tk.MustExec("insert ignore into t values (1, 3), (2, 3)")
	require.Equal(t, tk.Session().LastMessage(), "Records: 2  Duplicates: 1  Warnings: 1")
	r = tk.MustQuery("select * from t;")
	rowStr1 := fmt.Sprintf("%v %v", "2", "3")
	r.Check(testkit.Rows(rowStr, rowStr1))

	tk.MustExec("insert ignore into t values (3, 4), (3, 4)")
	require.Equal(t, tk.Session().LastMessage(), "Records: 2  Duplicates: 1  Warnings: 1")
	r = tk.MustQuery("select * from t;")
	rowStr2 := fmt.Sprintf("%v %v", "3", "4")
	r.Check(testkit.Rows(rowStr, rowStr1, rowStr2))

	tk.MustExec("begin")
	tk.MustExec("insert ignore into t values (4, 4), (4, 5), (4, 6)")
	require.Equal(t, tk.Session().LastMessage(), "Records: 3  Duplicates: 2  Warnings: 2")
	r = tk.MustQuery("select * from t;")
	rowStr3 := fmt.Sprintf("%v %v", "4", "5")
	r.Check(testkit.Rows(rowStr, rowStr1, rowStr2, rowStr3))
	tk.MustExec("commit")

	cfg.SetGetError(errors.New("foo"))
	err := tk.ExecToErr("insert ignore into t values (1, 3)")
	require.Error(t, err)
	cfg.SetGetError(nil)

	// for issue 4268
	testSQL = `drop table if exists t;
	create table t (a bigint);`
	tk.MustExec(testSQL)
	testSQL = "insert ignore into t select '1a';"
	err = tk.ExecToErr(testSQL)
	require.NoError(t, err)
	require.Equal(t, tk.Session().LastMessage(), "Records: 1  Duplicates: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect DOUBLE value: '1a'"))
	testSQL = "insert ignore into t values ('1a')"
	err = tk.ExecToErr(testSQL)
	require.NoError(t, err)
	require.Empty(t, tk.Session().LastMessage())
	r = tk.MustQuery("SHOW WARNINGS")
	// TODO: MySQL8.0 reports Warning 1265 Data truncated for column 'a' at row 1
	r.Check(testkit.Rows("Warning 1366 Incorrect bigint value: '1a' for column 'a' at row 1"))

	// for duplicates with warning
	testSQL = `drop table if exists t;
	create table t(a int primary key, b int);`
	tk.MustExec(testSQL)
	testSQL = "insert ignore into t values (1,1);"
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	err = tk.ExecToErr(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	require.NoError(t, err)
	r = tk.MustQuery("SHOW WARNINGS")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 't.PRIMARY'"))

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
	require.Empty(t, tk.Session().LastMessage())
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'i' cannot be null"))
	testSQL = `select * from badnull`
	tk.MustQuery(testSQL).Check(testkit.Rows("0"))

	tk.MustExec("create table tp (id int) partition by range (id) (partition p0 values less than (1), partition p1 values less than(2))")
	tk.MustExec("insert ignore into tp values (1), (3)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1526 Table has no partition for value 3"))
}

func TestIssue38950(t *testing.T) {
	store := testkit.CreateMockStore(t)
	var cfg kv.InjectionConfig
	tk := testkit.NewTestKit(t, kv.NewInjectedStore(store, &cfg))
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t; create table t (id smallint auto_increment primary key);")
	tk.MustExec("alter table t add column c1 int default 1;")
	tk.MustExec("insert ignore into t(id) values (194626268);")
	require.Empty(t, tk.Session().LastMessage())

	tk.MustQuery("select * from t").Check(testkit.Rows("32767 1"))

	tk.MustExec("insert ignore into t(id) values ('*') on duplicate key update c1 = 2;")
	require.Equal(t, int64(2), int64(tk.Session().AffectedRows()))
	require.Empty(t, tk.Session().LastMessage())

	tk.MustQuery("select * from t").Check(testkit.Rows("32767 2"))
}

func TestInsertOnDup(t *testing.T) {
	store := testkit.CreateMockStore(t)
	var cfg kv.InjectionConfig
	tk := testkit.NewTestKit(t, kv.NewInjectedStore(store, &cfg))
	tk.MustExec("use test")
	testSQL := `drop table if exists t;
    create table t (i int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1),(2);`
	tk.MustExec(testSQL)
	require.Equal(t, tk.Session().LastMessage(), "Records: 2  Duplicates: 0  Warnings: 0")

	r := tk.MustQuery("select * from t;")
	rowStr1 := fmt.Sprintf("%v", "1")
	rowStr2 := fmt.Sprintf("%v", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))

	tk.MustExec("insert into t values (1), (2) on duplicate key update i = values(i)")
	require.Equal(t, tk.Session().LastMessage(), "Records: 2  Duplicates: 0  Warnings: 0")
	r = tk.MustQuery("select * from t;")
	r.Check(testkit.Rows(rowStr1, rowStr2))

	tk.MustExec("insert into t values (2), (3) on duplicate key update i = 3")
	require.Equal(t, tk.Session().LastMessage(), "Records: 2  Duplicates: 1  Warnings: 0")
	r = tk.MustQuery("select * from t;")
	rowStr3 := fmt.Sprintf("%v", "3")
	r.Check(testkit.Rows(rowStr1, rowStr3))

	testSQL = `drop table if exists t;
    create table t (i int primary key, j int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (-1, 1);`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())

	r = tk.MustQuery("select * from t;")
	rowStr1 = fmt.Sprintf("%v %v", "-1", "1")
	r.Check(testkit.Rows(rowStr1))

	tk.MustExec("insert into t values (1, 1) on duplicate key update j = values(j)")
	require.Empty(t, tk.Session().LastMessage())
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
	require.Empty(t, tk.Session().LastMessage())
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))
	testSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))

	testSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT AUTO_INCREMENT UNIQUE,
	f2 VARCHAR(5) NOT NULL UNIQUE);
	INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))
	testSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = LAST_INSERT_ID(f1);`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))
	testSQL = `INSERT t1 (f2) VALUES ('test') ON DUPLICATE KEY UPDATE f1 = 2;`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	testSQL = `SELECT LAST_INSERT_ID();`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1"))

	testSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT);
	INSERT t1 VALUES (1) ON DUPLICATE KEY UPDATE f1 = 1;`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	tk.MustQuery(`SELECT * FROM t1;`).Check(testkit.Rows("1"))

	testSQL = `DROP TABLE IF EXISTS t1;
	CREATE TABLE t1 (f1 INT PRIMARY KEY, f2 INT NOT NULL UNIQUE);
	INSERT t1 VALUES (1, 1);`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	tk.MustExec(`INSERT t1 VALUES (1, 1), (1, 1) ON DUPLICATE KEY UPDATE f1 = 2, f2 = 2;`)
	require.Equal(t, tk.Session().LastMessage(), "Records: 2  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`SELECT * FROM t1 order by f1;`).Check(testkit.Rows("1 1", "2 2"))
	err := tk.ExecToErr(`INSERT t1 VALUES (1, 1) ON DUPLICATE KEY UPDATE f2 = null;`)
	require.Error(t, err)
	tk.MustExec(`INSERT IGNORE t1 VALUES (1, 1) ON DUPLICATE KEY UPDATE f2 = null;`)
	require.Empty(t, tk.Session().LastMessage())
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'f2' cannot be null"))
	tk.MustQuery(`SELECT * FROM t1 order by f1;`).Check(testkit.Rows("1 0", "2 2"))

	tk.MustExec(`SET sql_mode='';`)
	tk.MustExec(`INSERT t1 VALUES (1, 1) ON DUPLICATE KEY UPDATE f2 = null;`)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1048 Column 'f2' cannot be null"))
	tk.MustQuery(`SELECT * FROM t1 order by f1;`).Check(testkit.Rows("1 0", "2 2"))
}

func TestInsertIgnoreOnDup(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testSQL := `drop table if exists t;
    create table t (i int not null primary key, j int unique key);`
	tk.MustExec(testSQL)
	testSQL = `insert into t values (1, 1), (2, 2);`
	tk.MustExec(testSQL)
	require.Equal(t, tk.Session().LastMessage(), "Records: 2  Duplicates: 0  Warnings: 0")
	testSQL = `insert ignore into t values(1, 1) on duplicate key update i = 2;`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	testSQL = `select * from t;`
	r := tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 1", "2 2"))
	testSQL = `insert ignore into t values(1, 1) on duplicate key update j = 2;`
	tk.MustExec(testSQL)
	require.Empty(t, tk.Session().LastMessage())
	testSQL = `select * from t;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1 1", "2 2"))

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(`col_25` set('Alice','Bob','Charlie','David') NOT NULL,`col_26` date NOT NULL DEFAULT '2016-04-15', PRIMARY KEY (`col_26`) clustered, UNIQUE KEY `idx_9` (`col_25`,`col_26`),UNIQUE KEY `idx_10` (`col_25`))")
	tk.MustExec("insert into t2(col_25, col_26) values('Bob', '1989-03-23'),('Alice', '2023-11-24'), ('Charlie', '2023-12-05')")
	tk.MustExec("insert ignore into t2 (col_25,col_26) values ( 'Bob','1977-11-23' ) on duplicate key update col_25 = 'Alice', col_26 = '2036-12-13'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry 'Alice' for key 't2.idx_10'"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("Bob 1989-03-23", "Alice 2023-11-24", "Charlie 2023-12-05"))

	tk.MustExec("drop table if exists t4")
	tk.MustExec("create table t4(id int primary key clustered, k int, v int, unique key uk1(k))")
	tk.MustExec("insert into t4 values (1, 10, 100), (3, 30, 300)")
	tk.MustExec("insert ignore into t4 (id, k, v) values(1, 0, 0) on duplicate key update id = 2, k = 30")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '30' for key 't4.uk1'"))
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 10 100", "3 30 300"))

	tk.MustExec("drop table if exists t5")
	tk.MustExec("create table t5(k1 varchar(100), k2 varchar(100), uk1 int, v int, primary key(k1, k2) clustered, unique key ukk1(uk1), unique key ukk2(v))")
	tk.MustExec("insert into t5(k1, k2, uk1, v) values('1', '1', 1, '100'), ('1', '3', 2, '200')")
	tk.MustExec("update ignore t5 set k2 = '2', uk1 = 2 where k1 = '1' and k2 = '1'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '2' for key 't5.ukk1'"))
	tk.MustQuery("select * from t5").Check(testkit.Rows("1 1 1 100", "1 3 2 200"))

	tk.MustExec("drop table if exists t6")
	tk.MustExec("create table t6 (a int, b int, c int, primary key(a, b) clustered, unique key idx_14(b), unique key idx_15(b), unique key idx_16(a, b))")
	tk.MustExec("insert into t6 select 10, 10, 20")
	tk.MustExec("insert ignore into t6 set a = 20, b = 10 on duplicate key update a = 100")
	tk.MustQuery("select * from t6").Check(testkit.Rows("100 10 20"))
	tk.MustExec("insert ignore into t6 set a = 200, b= 10 on duplicate key update c = 1000")
	tk.MustQuery("select * from t6").Check(testkit.Rows("100 10 1000"))
}

func TestReplace(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testSQL := `drop table if exists replace_test;
    create table replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	testSQL = `replace replace_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)
	require.Equal(t, tk.Session().LastMessage(), "Records: 3  Duplicates: 0  Warnings: 0")

	errReplaceSQL := `replace replace_test (c1) values ();`
	tk.MustExec("begin")
	err := tk.ExecToErr(errReplaceSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	err = tk.ExecToErr(errReplaceSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test (xxx) values (3);`
	tk.MustExec("begin")
	err = tk.ExecToErr(errReplaceSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test_xxx (c1) values ();`
	tk.MustExec("begin")
	err = tk.ExecToErr(errReplaceSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	replaceSetSQL := `replace replace_test set c1 = 3;`
	tk.MustExec(replaceSetSQL)
	require.Empty(t, tk.Session().LastMessage())

	errReplaceSetSQL := `replace replace_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	err = tk.ExecToErr(errReplaceSetSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	errReplaceSetSQL = `replace replace_test set xxx = 6;`
	tk.MustExec("begin")
	err = tk.ExecToErr(errReplaceSetSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	replaceSelectSQL := `create table replace_test_1 (id int, c1 int);`
	tk.MustExec(replaceSelectSQL)
	replaceSelectSQL = `replace replace_test_1 select id, c1 from replace_test;`
	tk.MustExec(replaceSelectSQL)
	require.Equal(t, tk.Session().LastMessage(), "Records: 4  Duplicates: 0  Warnings: 0")

	replaceSelectSQL = `create table replace_test_2 (id int, c1 int);`
	tk.MustExec(replaceSelectSQL)
	replaceSelectSQL = `replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`
	tk.MustExec(replaceSelectSQL)
	require.Equal(t, tk.Session().LastMessage(), "Records: 8  Duplicates: 0  Warnings: 0")

	errReplaceSelectSQL := `replace replace_test_1 select c1 from replace_test;`
	tk.MustExec("begin")
	err = tk.ExecToErr(errReplaceSelectSQL)
	require.Error(t, err)
	tk.MustExec("rollback")

	replaceUniqueIndexSQL := `create table replace_test_3 (c1 int, c2 int, UNIQUE INDEX (c2));`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	require.Equal(t, int64(1), int64(tk.Session().AffectedRows()))
	require.Empty(t, tk.Session().LastMessage())

	replaceUniqueIndexSQL = `replace into replace_test_3 set c1=1, c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	require.Equal(t, int64(2), int64(tk.Session().AffectedRows()))
	require.Empty(t, tk.Session().LastMessage())

	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	require.Equal(t, int64(1), int64(tk.Session().AffectedRows()))
	require.Empty(t, tk.Session().LastMessage())

	replaceUniqueIndexSQL = `create table replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2));`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	require.Equal(t, int64(1), int64(tk.Session().AffectedRows()))
	require.Empty(t, tk.Session().LastMessage())

	replacePrimaryKeySQL := `create table replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2));`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	require.Equal(t, int64(1), int64(tk.Session().AffectedRows()))
	require.Empty(t, tk.Session().LastMessage())

	// For Issue989
	issue989SQL := `CREATE TABLE tIssue989 (a int, b int, PRIMARY KEY(a), UNIQUE KEY(b));`
	tk.MustExec(issue989SQL)
	issue989SQL = `insert into tIssue989 (a, b) values (1, 2);`
	tk.MustExec(issue989SQL)
	require.Empty(t, tk.Session().LastMessage())
	issue989SQL = `replace into tIssue989(a, b) values (111, 2);`
	tk.MustExec(issue989SQL)
	require.Empty(t, tk.Session().LastMessage())
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
	require.Equal(t, int64(3), int64(tk.Session().AffectedRows()))
	require.Empty(t, tk.Session().LastMessage())
	r = tk.MustQuery("select * from tIssue1012;")
	r.Check(testkit.Rows("1 1"))

	// Test Replace with info message
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(a int primary key, b int);`)
	tk.MustExec(`insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);`)
	tk.MustExec(`replace into t1 values(1,1);`)
	require.Equal(t, int64(1), int64(tk.Session().AffectedRows()))
	require.Empty(t, tk.Session().LastMessage())
	tk.MustExec(`replace into t1 values(1,1),(2,2);`)
	require.Equal(t, int64(2), int64(tk.Session().AffectedRows()))
	require.Equal(t, tk.Session().LastMessage(), "Records: 2  Duplicates: 0  Warnings: 0")
	tk.MustExec(`replace into t1 values(4,14),(5,15),(6,16),(7,17),(8,18)`)
	require.Equal(t, int64(7), int64(tk.Session().AffectedRows()))
	require.Equal(t, tk.Session().LastMessage(), "Records: 5  Duplicates: 2  Warnings: 0")
	tk.MustExec(`replace into t1 select * from (select 1, 2) as tmp;`)
	require.Equal(t, int64(2), int64(tk.Session().AffectedRows()))
	require.Equal(t, tk.Session().LastMessage(), "Records: 1  Duplicates: 1  Warnings: 0")

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
	tk.MustGetErrCode("replace t2 set a=default(a), c=default(c);", mysql.ErrNoDefaultForField)
	tk.MustGetErrCode("replace t2 set c=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustExec("drop table t1, t2")
}

// TestUpdateCastOnlyModifiedValues for issue #4514.
func TestUpdateCastOnlyModifiedValues(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table update_modified (col_1 int, col_2 enum('a', 'b'))")
	tk.MustExec("set SQL_MODE=''")
	tk.MustExec("insert into update_modified values (0, 3)")
	r := tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("0 "))
	tk.MustExec("set SQL_MODE=STRICT_ALL_TABLES")
	tk.MustExec("update update_modified set col_1 = 1")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("1 "))
	err := tk.ExecToErr("update update_modified set col_1 = 2, col_2 = 'c'")
	require.Error(t, err)
	r = tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("1 "))
	tk.MustExec("update update_modified set col_1 = 3, col_2 = 'a'")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("SELECT * FROM update_modified")
	r.Check(testkit.Rows("3 a"))

	// Test update a field with different column type.
	tk.MustExec(`CREATE TABLE update_with_diff_type (a int, b JSON)`)
	tk.MustExec(`INSERT INTO update_with_diff_type VALUES(3, '{"a": "测试"}')`)
	tk.MustExec(`UPDATE update_with_diff_type SET a = '300'`)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("SELECT a FROM update_with_diff_type")
	r.Check(testkit.Rows("300"))
	tk.MustExec(`UPDATE update_with_diff_type SET b = '{"a":   "\\u6d4b\\u8bd5"}'`)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 0  Warnings: 0")
	r = tk.MustQuery("SELECT b FROM update_with_diff_type")
	r.Check(testkit.Rows(`{"a": "测试"}`))
}

func fillMultiTableForUpdate(tk *testkit.TestKit) {
	// Create and fill table items
	tk.MustExec("CREATE TABLE items (id int, price TEXT);")
	tk.MustExec(`insert into items values (11, "items_price_11"), (12, "items_price_12"), (13, "items_price_13");`)
	tk.CheckExecResult(3, 0)
	// Create and fill table month
	tk.MustExec("CREATE TABLE month (mid int, mprice TEXT);")
	tk.MustExec(`insert into month values (11, "month_price_11"), (22, "month_price_22"), (13, "month_price_13");`)
	tk.CheckExecResult(3, 0)
}

func TestMultipleTableUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	fillMultiTableForUpdate(tk)

	tk.MustExec(`UPDATE items, month  SET items.price=month.mprice WHERE items.id=month.mid;`)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 2  Changed: 2  Warnings: 0")
	tk.MustExec("begin")
	r := tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 month_price_11", "12 items_price_12", "13 month_price_13"))
	tk.MustExec("commit")

	// Single-table syntax but with multiple tables
	tk.MustExec(`UPDATE items join month on items.id=month.mid SET items.price=month.mid;`)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 2  Changed: 2  Warnings: 0")
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM items")
	r.Check(testkit.Rows("11 11", "12 items_price_12", "13 13"))
	tk.MustExec("commit")

	// JoinTable with alias table name.
	tk.MustExec(`UPDATE items T0 join month T1 on T0.id=T1.mid SET T0.price=T1.mprice;`)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 2  Changed: 2  Warnings: 0")
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
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 4  Changed: 4  Warnings: 0")

	// fix https://github.com/pingcap/tidb/issues/376
	testSQL = `DROP TABLE IF EXISTS t1, t2;
		create table t1 (c1 int);
		create table t2 (c2 int);
		insert into t1 values (1), (2);
		insert into t2 values (1), (2);
		update t1, t2 set t1.c1 = 10, t2.c2 = 2 where t2.c2 = 1;`
	tk.MustExec(testSQL)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 3  Changed: 3  Warnings: 0")

	r = tk.MustQuery("select * from t1")
	r.Check(testkit.Rows("10", "10"))

	// test https://github.com/pingcap/tidb/issues/3604
	tk.MustExec("drop table if exists t, t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3)")
	require.Equal(t, tk.Session().LastMessage(), "Records: 3  Duplicates: 0  Warnings: 0")
	tk.MustExec("update t m, t n set m.a = m.a + 1")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 3  Changed: 3  Warnings: 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1", "3 2", "4 3"))
	tk.MustExec("update t m, t n set n.a = n.a - 1, n.b = n.b + 1")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 3  Changed: 3  Warnings: 0")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "2 3", "3 4"))
}

func TestDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	internal.FillData(tk, "delete_test")

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
	err := tk.ExecToErr("delete from delete_test where id = (select '2a')")
	require.Error(t, err)
	err = tk.ExecToErr("delete ignore from delete_test where id = (select '2a')")
	require.NoError(t, err)
	tk.CheckExecResult(1, 0)
	r := tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect DOUBLE value: '2a'", "Warning 1292 Truncated incorrect DOUBLE value: '2a'"))

	tk.MustExec(`delete from delete_test ;`)
	tk.CheckExecResult(1, 0)

	tk.MustExec("create view v as select * from delete_test")
	err = tk.ExecToErr("delete from v where name = 'aaa'")
	require.EqualError(t, err, "delete view v is not supported now")
	tk.MustExec("drop view v")

	tk.MustExec("create sequence seq")
	err = tk.ExecToErr("delete from seq")
	require.EqualError(t, err, "delete sequence seq is not supported now")
	tk.MustExec("drop sequence seq")
}

func fillDataMultiTable(tk *testkit.TestKit) {
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

func TestMultiTableDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	fillDataMultiTable(tk)

	tk.MustExec(`delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;`)
	tk.CheckExecResult(2, 0)

	// Select data
	r := tk.MustQuery("select * from t3")
	require.Len(t, r.Rows(), 3)
}

func TestQualifiedDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
	require.Len(t, r.Rows(), 0)
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

	err := tk.ExecToErr("delete t1, t2 from t1 as a join t2 as b where a.c2 = b.c1")
	require.Error(t, err)
}

type testCase struct {
	data        []byte
	expected    []string
	expectedMsg string
}

func checkCases(
	tests []testCase,
	ld *executor.LoadDataWorker,
	t *testing.T,
	tk *testkit.TestKit,
	ctx sessionctx.Context,
	selectSQL, deleteSQL string,
) {
	for _, tt := range tests {
		parser, err := mydump.NewCSVParser(
			context.Background(),
			ld.GetController().GenerateCSVConfig(),
			mydump.NewStringReader(string(tt.data)),
			1,
			nil,
			false,
			nil)
		require.NoError(t, err)

		err = ld.TestLoadLocal(parser)
		require.NoError(t, err)
		require.Equal(t, tt.expectedMsg, tk.Session().LastMessage(), tt.expected)
		tk.MustQuery(selectSQL).Check(testkit.RowsWithSep("|", tt.expected...))
		tk.MustExec(deleteSQL)
	}
}

func TestLoadDataMissingColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createSQL := `create table load_data_missing (id int, t timestamp not null)`
	tk.MustExec(createSQL)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' ignore into table load_data_missing")
	ctx := tk.Session().(sessionctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataWorker)
	require.True(t, ok)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	require.NotNil(t, ld)

	deleteSQL := "delete from load_data_missing"
	selectSQL := "select id, hour(t), minute(t) from load_data_missing;"

	curTime := types.CurrentTime(mysql.TypeTimestamp)
	timeHour := curTime.Hour()
	timeMinute := curTime.Minute()
	tests := []testCase{
		{[]byte(""), nil, "Records: 0  Deleted: 0  Skipped: 0  Warnings: 0"},
		{[]byte("12\n"), []string{fmt.Sprintf("12|%v|%v", timeHour, timeMinute)}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	}
	checkCases(tests, ld, t, tk, ctx, selectSQL, deleteSQL)

	tk.MustExec("alter table load_data_missing add column t2 timestamp null")
	curTime = types.CurrentTime(mysql.TypeTimestamp)
	timeHour = curTime.Hour()
	timeMinute = curTime.Minute()
	selectSQL = "select id, hour(t), minute(t), t2 from load_data_missing;"
	tests = []testCase{
		{[]byte("12\n"), []string{fmt.Sprintf("12|%v|%v|<nil>", timeHour, timeMinute)}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	}
	checkCases(tests, ld, t, tk, ctx, selectSQL, deleteSQL)
}

func TestIssue18681(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createSQL := `drop table if exists load_data_test;
		create table load_data_test (a bit(1),b bit(1),c bit(1),d bit(1),e bit(32),f bit(1));`
	tk.MustExec(createSQL)
	tk.MustExec("load data local infile '/tmp/nonexistence.csv' ignore into table load_data_test")
	ctx := tk.Session().(sessionctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataWorker)
	require.True(t, ok)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	require.NotNil(t, ld)

	deleteSQL := "delete from load_data_test"
	selectSQL := "select bin(a), bin(b), bin(c), bin(d), bin(e), bin(f) from load_data_test;"
	ctx.GetSessionVars().StmtCtx.DupKeyAsWarning = true
	ctx.GetSessionVars().StmtCtx.BadNullAsWarning = true

	sc := ctx.GetSessionVars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate.Load()
	defer func() {
		sc.IgnoreTruncate.Store(originIgnoreTruncate)
	}()
	sc.IgnoreTruncate.Store(false)
	tests := []testCase{
		{[]byte("true\tfalse\t0\t1\tb'1'\tb'1'\n"), []string{"1|1|1|1|1100010001001110011000100100111|1"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 5"},
	}
	checkCases(tests, ld, t, tk, ctx, selectSQL, deleteSQL)
	require.Equal(t, uint16(0), sc.WarningCount())
}

func TestIssue34358(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := tk.Session().(sessionctx.Context)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists load_data_test")
	tk.MustExec("create table load_data_test (a varchar(10), b varchar(10))")

	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table load_data_test ( @v1, @v2 ) set a = @v1, b = @v2")
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataWorker)
	require.True(t, ok)
	require.NotNil(t, ld)
	checkCases([]testCase{
		{[]byte("\\N\n"), []string{"<nil>|<nil>"}, "Records: 1  Deleted: 0  Skipped: 0  Warnings: 1"},
	}, ld, t, tk, ctx, "select * from load_data_test", "delete from load_data_test")
}

func TestLatch(t *testing.T) {
	store, err := mockstore.NewMockStore(
		// Small latch slot size to make conflicts.
		mockstore.WithTxnLocalLatches(64),
	)
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	dom, err1 := session.BootstrapSession(store)
	require.Nil(t, err1)
	defer dom.Close()

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t")
	tk1.MustExec("create table t (id int)")
	tk1.MustExec("set @@tidb_disable_txn_auto_retry = true")

	tk2 := testkit.NewTestKit(t, store)
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
	tk1.MustGetDBError("commit", kv.ErrWriteConflictInTiDB)

	tk1.MustExec("set @@tidb_disable_txn_auto_retry = 0")
	tk1.MustExec("update t set id = id + 1")
	tk2.MustExec("update t set id = id + 1")
	tk1.MustExec("commit")
}

func TestUpdateSelect(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table msg (id varchar(8), b int, status int, primary key (id, b))")
	tk.MustExec("insert msg values ('abc', 1, 1)")
	tk.MustExec("create table detail (id varchar(8), start varchar(8), status int, index idx_start(start))")
	tk.MustExec("insert detail values ('abc', '123', 2)")
	tk.MustExec("UPDATE msg SET msg.status = (SELECT detail.status FROM detail WHERE msg.id = detail.id)")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("admin check table msg")
}

func TestUpdateDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE ttt (id bigint(20) NOT NULL, host varchar(30) NOT NULL, PRIMARY KEY (id), UNIQUE KEY i_host (host));")
	tk.MustExec("insert into ttt values (8,8),(9,9);")

	tk.MustExec("begin")
	tk.MustExec("update ttt set id = 0, host='9' where id = 9 limit 1;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("delete from ttt where id = 0 limit 1;")
	tk.MustQuery("select * from ttt use index (i_host) order by host;").Check(testkit.Rows("8 8"))
	tk.MustExec("update ttt set id = 0, host='8' where id = 8 limit 1;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	tk.MustExec("delete from ttt where id = 0 limit 1;")
	tk.MustQuery("select * from ttt use index (i_host) order by host;").Check(testkit.Rows())
	tk.MustExec("commit")
	tk.MustExec("admin check table ttt;")
	tk.MustExec("drop table ttt")
}

func TestUpdateAffectRowCnt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table a(id int auto_increment, a int default null, primary key(id))")
	tk.MustExec("insert into a values (1, 1001), (2, 1001), (10001, 1), (3, 1)")
	tk.MustExec("update a set id = id*10 where a = 1001")
	ctx := tk.Session().(sessionctx.Context)
	require.Equal(t, uint64(2), ctx.GetSessionVars().StmtCtx.AffectedRows())
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 2  Changed: 2  Warnings: 0")

	tk.MustExec("drop table a")
	tk.MustExec("create table a ( a bigint, b bigint)")
	tk.MustExec("insert into a values (1, 1001), (2, 1001), (10001, 1), (3, 1)")
	tk.MustExec("update a set a = a*10 where b = 1001")
	ctx = tk.Session().(sessionctx.Context)
	require.Equal(t, uint64(2), ctx.GetSessionVars().StmtCtx.AffectedRows())
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 2  Changed: 2  Warnings: 0")
}

func TestReplaceLog(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table testLog (a int not null primary key, b int unique key);`)

	// Make some dangling index.
	ctx := mock.NewContext()
	ctx.Store = store
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("testLog")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("b")
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := store.Begin()
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(1), kv.IntHandle(1), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	err = tk.ExecToErr(`replace into testLog values (0, 0), (1, 1);`)
	require.Error(t, err)
	require.EqualError(t, err, `can not be duplicated row, due to old row not found. handle 1 not found`)
	tk.MustQuery(`admin cleanup index testLog b;`).Check(testkit.Rows("1"))
}

// TestRebaseIfNeeded is for issue 7422.
// There is no need to do the rebase when updating a record if the auto-increment ID not changed.
// This could make the auto ID increasing speed slower.
func TestRebaseIfNeeded(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int not null primary key auto_increment, b int unique key);`)
	tk.MustExec(`insert into t (b) values (1);`)

	ctx := mock.NewContext()
	ctx.Store = store
	tbl, err := domain.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Nil(t, sessiontxn.NewTxn(context.Background(), ctx))
	// AddRecord directly here will skip to rebase the auto ID in the insert statement,
	// which could simulate another TiDB adds a large auto ID.
	_, err = tbl.AddRecord(ctx, types.MakeDatums(30001, 2))
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(context.Background()))

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

func TestDeferConstraintCheckForInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	tk.MustExec(`drop table if exists t;create table t (a int primary key, b int);`)
	tk.MustExec(`insert into t values (1,2),(2,2)`)
	err := tk.ExecToErr("update t set a=a+1 where b=2")
	require.Error(t, err)

	tk.MustExec(`drop table if exists t;create table t (i int key);`)
	tk.MustExec(`insert t values (1);`)
	tk.MustExec(`set tidb_constraint_check_in_place = 1;`)
	tk.MustExec(`begin;`)
	err = tk.ExecToErr(`insert t values (1);`)
	require.Error(t, err)
	tk.MustExec(`update t set i = 2 where i = 1;`)
	tk.MustExec(`commit;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows("2"))

	tk.MustExec(`set tidb_constraint_check_in_place = 0;`)
	tk.MustExec("replace into t values (1),(2)")
	tk.MustExec("begin")
	err = tk.ExecToErr("update t set i = 2 where i = 1")
	require.Error(t, err)
	err = tk.ExecToErr("insert into t values (1) on duplicate key update i = i + 1")
	require.Error(t, err)
	tk.MustExec("rollback")

	tk.MustExec(`drop table t; create table t (id int primary key, v int unique);`)
	tk.MustExec(`insert into t values (1, 1)`)
	tk.MustExec(`set tidb_constraint_check_in_place = 1;`)
	tk.MustExec(`set @@autocommit = 0;`)

	err = tk.ExecToErr("insert into t values (3, 1)")
	require.Error(t, err)
	err = tk.ExecToErr("insert into t values (1, 3)")
	require.Error(t, err)
	tk.MustExec("commit")

	tk.MustExec(`set tidb_constraint_check_in_place = 0;`)
	tk.MustExec("insert into t values (3, 1)")
	tk.MustExec("insert into t values (1, 3)")
	err = tk.ExecToErr("commit")
	require.Error(t, err)

	// Cover the temporary table.
	for val := range []int{0, 1} {
		tk.MustExec("set tidb_constraint_check_in_place = ?", val)

		tk.MustExec("drop table t")
		tk.MustExec("create global temporary table t (a int primary key, b int) on commit delete rows")
		tk.MustExec("begin")
		tk.MustExec("insert into t values (1, 1)")
		err = tk.ExecToErr(`insert into t values (1, 3)`)
		require.Error(t, err)
		tk.MustExec("insert into t values (2, 2)")
		err = tk.ExecToErr("update t set a = a + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into t values (1, 3) on duplicated key update a = a + 1")
		require.Error(t, err)
		tk.MustExec("commit")

		tk.MustExec("drop table t")
		tk.MustExec("create global temporary table t (a int, b int unique) on commit delete rows")
		tk.MustExec("begin")
		tk.MustExec("insert into t values (1, 1)")
		err = tk.ExecToErr(`insert into t values (3, 1)`)
		require.Error(t, err)
		tk.MustExec("insert into t values (2, 2)")
		err = tk.ExecToErr("update t set b = b + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into t values (3, 1) on duplicated key update b = b + 1")
		require.Error(t, err)
		tk.MustExec("commit")

		// cases for temporary table
		tk.MustExec("drop table if exists tl")
		tk.MustExec("create temporary table tl (a int primary key, b int)")
		tk.MustExec("begin")
		tk.MustExec("insert into tl values (1, 1)")
		err = tk.ExecToErr(`insert into tl values (1, 3)`)
		require.Error(t, err)
		tk.MustExec("insert into tl values (2, 2)")
		err = tk.ExecToErr("update tl set a = a + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into tl values (1, 3) on duplicated key update a = a + 1")
		require.Error(t, err)
		tk.MustExec("commit")

		tk.MustExec("begin")
		tk.MustQuery("select * from tl").Check(testkit.Rows("1 1", "2 2"))
		err = tk.ExecToErr(`insert into tl values (1, 3)`)
		require.Error(t, err)
		err = tk.ExecToErr("update tl set a = a + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into tl values (1, 3) on duplicated key update a = a + 1")
		require.Error(t, err)
		tk.MustExec("rollback")

		tk.MustExec("drop table tl")
		tk.MustExec("create temporary table tl (a int, b int unique)")
		tk.MustExec("begin")
		tk.MustExec("insert into tl values (1, 1)")
		err = tk.ExecToErr(`insert into tl values (3, 1)`)
		require.Error(t, err)
		tk.MustExec("insert into tl values (2, 2)")
		err = tk.ExecToErr("update tl set b = b + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into tl values (3, 1) on duplicated key update b = b + 1")
		require.Error(t, err)
		tk.MustExec("commit")

		tk.MustExec("begin")
		tk.MustQuery("select * from tl").Check(testkit.Rows("1 1", "2 2"))
		err = tk.ExecToErr(`insert into tl values (3, 1)`)
		require.Error(t, err)
		err = tk.ExecToErr("update tl set b = b + 1 where a = 1")
		require.Error(t, err)
		err = tk.ExecToErr("insert into tl values (3, 1) on duplicated key update b = b + 1")
		require.Error(t, err)
		tk.MustExec("rollback")
	}
}

func TestPessimisticDeleteYourWrites(t *testing.T) {
	store := testkit.CreateMockStore(t)

	session1 := testkit.NewTestKit(t, store)
	session1.MustExec("use test")
	session2 := testkit.NewTestKit(t, store)
	session2.MustExec("use test")

	session1.MustExec("drop table if exists x;")
	session1.MustExec("create table x (id int primary key, c int);")

	session1.MustExec("set tidb_txn_mode = 'pessimistic'")
	session2.MustExec("set tidb_txn_mode = 'pessimistic'")

	session1.MustExec("begin;")
	session1.MustExec("insert into x select 1, 1")
	session1.MustExec("delete from x where id = 1")
	session2.MustExec("begin;")
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		session2.MustExec("insert into x select 1, 2")
	})
	session1.MustExec("commit;")
	wg.Wait()
	session2.MustExec("commit;")
	session2.MustQuery("select * from x").Check(testkit.Rows("1 2"))
}

// TestWriteListPartitionTable2 test for write list partition when the partition expression is complicated and contain generated column.
func TestWriteListPartitionTable2(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
	err := tk.ExecToErr("alter table t add unique index idx (id,b)")
	require.EqualError(t, err, "[kv:1062]Duplicate entry '1-2' for key 't.idx'")
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
	err = tk.ExecToErr("insert into t (id,name) values (3, 'a'), (11,'x') on duplicate key update id=id+1")
	require.EqualError(t, err, "[kv:1062]Duplicate entry '4-2' for key 't.idx'")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 x", "3 x", "4 e", "5 g"))
	// Test insert ignore with duplicate
	tk.MustExec("insert ignore into t (id,name) values  (1, 'b'), (5,'a'),(null,'y')")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1-2' for key 't.idx'", "Warning 1062 Duplicate entry '5-2' for key 't.idx'"))
	tk.MustQuery("select id,name from t partition(p0) order by id").Check(testkit.Rows("3 x", "5 g"))
	tk.MustQuery("select id,name from t partition(p1) order by id").Check(testkit.Rows("1 x"))
	tk.MustQuery("select id,name from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select id,name from t partition(p3) order by id").Check(testkit.Rows("<nil> y"))
	// Test insert ignore without duplicate
	tk.MustExec("insert ignore into t (id,name) values  (15, 'a'),(17,'a')")
	tk.MustQuery("select id,name from t partition(p0,p1,p2) order by id").Check(testkit.Rows("1 x", "3 x", "4 e", "5 g", "17 a"))
	tk.MustQuery("select id,name from t partition(p3) order by id").Check(testkit.Rows("<nil> y", "15 a"))
	// Test insert meet no partition error.
	err = tk.ExecToErr("insert into t (id,name) values (100, 'd')")
	require.EqualError(t, err, "[table:1526]Table has no partition for value 100")

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
	tk.MustGetErrMsg("update t set id=2 where id = 1", "[kv:1062]Duplicate entry '2-2' for key 't.idx'")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 y", "2 y", "3 c"))

	// Test update multi-partitions
	tk.MustExec("update t set name='z' where id in (1,2,3);")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 z", "2 z", "3 z"))
	tk.MustExec("update t set name='a' limit 3")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("1 a", "2 a", "3 a"))
	tk.MustExec("update t set id=id*10 where id in (1,2)")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))
	// Test update meet duplicate error.
	tk.MustGetErrMsg("update t set id=id+17 where id in (3,10)", "[kv:1062]Duplicate entry '20-2' for key 't.idx'")
	tk.MustQuery("select id,name from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))
	// Test update meet no partition error.
	tk.MustGetErrMsg("update t set id=id*2 where id in (3,20)", "[table:1526]Table has no partition for value 40")
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
	tk.MustGetErrMsg("replace into t (id,name) values  (10,'x'),(50,'x')", "[table:1526]Table has no partition for value 50")
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

func TestWriteListColumnsPartitionTable1(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
	tk.MustGetErrMsg("alter table t add unique index idx (id)", "[kv:1062]Duplicate entry '1' for key 't.idx'")
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
	tk.MustGetErrMsg("insert into t values (3, 'a'), (11,'x') on duplicate key update id=id+1", "[kv:1062]Duplicate entry '4' for key 't.idx'")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 x", "3 x", "4 e", "5 g"))
	// Test insert ignore with duplicate
	tk.MustExec("insert ignore into t values  (1, 'b'), (5,'a'),(null,'y')")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 't.idx'", "Warning 1062 Duplicate entry '5' for key 't.idx'"))
	tk.MustQuery("select * from t partition(p0) order by id").Check(testkit.Rows("3 x", "5 g"))
	tk.MustQuery("select * from t partition(p1) order by id").Check(testkit.Rows("1 x"))
	tk.MustQuery("select * from t partition(p2) order by id").Check(testkit.Rows("4 e"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows("<nil> y"))
	// Test insert ignore without duplicate
	tk.MustExec("insert ignore into t values  (15, 'a'),(17,'a')")
	tk.MustQuery("select * from t partition(p0,p1,p2) order by id").Check(testkit.Rows("1 x", "3 x", "4 e", "5 g", "17 a"))
	tk.MustQuery("select * from t partition(p3) order by id").Check(testkit.Rows("<nil> y", "15 a"))
	// Test insert meet no partition error.
	tk.MustGetErrMsg("insert into t values (100, 'd')", "[table:1526]Table has no partition for value from column_list")

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
	tk.MustGetErrMsg("update t set id=2 where id = 1", "[kv:1062]Duplicate entry '2' for key 't.idx'")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 y", "2 y", "3 c"))

	// Test update multi-partitions
	tk.MustExec("update t set name='z' where id in (1,2,3);")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 z", "2 z", "3 z"))
	tk.MustExec("update t set name='a' limit 3")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1 a", "2 a", "3 a"))
	tk.MustExec("update t set id=id*10 where id in (1,2)")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))
	// Test update meet duplicate error.
	tk.MustGetErrMsg("update t set id=id+17 where id in (3,10)", "[kv:1062]Duplicate entry '20' for key 't.idx'")
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("3 a", "10 a", "20 a"))
	// Test update meet no partition error.
	tk.MustGetErrMsg("update t set id=id*2 where id in (3,20)", "[table:1526]Table has no partition for value from column_list")
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
	tk.MustGetErrMsg("replace into t values  (10,'x'),(100,'x')", "[table:1526]Table has no partition for value from column_list")
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

func TestListPartitionWithAutoRandom(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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

func TestListPartitionWithAutoIncrement(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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

func TestUpdate(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	internal.FillData(tk, "update_test")

	updateStr := `UPDATE update_test SET name = "abc" where id > 0;`
	tk.MustExec(updateStr)
	tk.CheckExecResult(2, 0)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 2  Changed: 2  Warnings: 0")

	// select data
	tk.MustExec("begin")
	r := tk.MustQuery(`SELECT * from update_test limit 2;`)
	r.Check(testkit.Rows("1 abc", "2 abc"))
	tk.MustExec("commit")

	tk.MustExec(`UPDATE update_test SET name = "foo"`)
	tk.CheckExecResult(2, 0)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 2  Changed: 2  Warnings: 0")

	// table option is auto-increment
	tk.MustExec("begin")
	tk.MustExec("drop table if exists update_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create table update_test(id int not null auto_increment, name varchar(255), primary key(id))")
	tk.MustExec("insert into update_test(name) values ('aa')")
	tk.MustExec("update update_test set id = 8 where name = 'aa'")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
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
	err := tk.ExecToErr("update update_test set id = null where name = 'aa'")
	require.EqualError(t, err, "[table:1048]Column 'id' cannot be null")

	tk.MustExec("drop table update_test")
	tk.MustExec("create table update_test(id int)")
	tk.MustExec("begin")
	tk.MustExec("insert into update_test(id) values (1)")
	tk.MustExec("update update_test set id = 2 where id = 1 limit 1")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
	r = tk.MustQuery("select * from update_test;")
	r.Check(testkit.Rows("2"))
	tk.MustExec("commit")

	// Test that in a transaction, when a constraint failed in an update statement, the record is not inserted.
	tk.MustExec("create table update_unique (id int primary key, name int unique)")
	tk.MustExec("insert update_unique values (1, 1), (2, 2);")
	tk.MustExec("begin")
	err = tk.ExecToErr("update update_unique set name = 1 where id = 2")
	require.Error(t, err)
	tk.MustExec("commit")
	tk.MustQuery("select * from update_unique").Check(testkit.Rows("1 1", "2 2"))

	// test update ignore for pimary key
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint, primary key (a));")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	err = tk.ExecToErr("update ignore t set a = 1 where a = 2;")
	require.NoError(t, err)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 't.PRIMARY'"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))

	// test update ignore for truncate as warning
	err = tk.ExecToErr("update ignore t set a = 1 where a = (select '2a')")
	require.NoError(t, err)
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1292 Truncated incorrect DOUBLE value: '2a'", "Warning 1292 Truncated incorrect DOUBLE value: '2a'", "Warning 1062 Duplicate entry '1' for key 't.PRIMARY'"))

	tk.MustExec("update ignore t set a = 42 where a = 2;")
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "42"))

	// test update ignore for unique key
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint, unique key I_uniq (a));")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (2)")
	err = tk.ExecToErr("update ignore t set a = 1 where a = 2;")
	require.NoError(t, err)
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 0  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1062 Duplicate entry '1' for key 't.I_uniq'"))
	tk.MustQuery("select * from t").Check(testkit.Rows("1", "2"))

	// test issue21965
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("create table t (a int) partition by list (a) (partition p0 values in (0,1));")
	tk.MustExec("insert ignore into t values (1);")
	tk.MustExec("update ignore t set a=2 where a=1;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 0  Warnings: 0")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int key) partition by list (a) (partition p0 values in (0,1));")
	tk.MustExec("insert ignore into t values (1);")
	tk.MustExec("update ignore t set a=2 where a=1;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 0  Warnings: 0")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id integer auto_increment, t1 datetime, t2 datetime, primary key (id))")
	tk.MustExec("insert into t(t1, t2) values('2000-10-01 01:01:01', '2017-01-01 10:10:10')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2000-10-01 01:01:01 2017-01-01 10:10:10"))
	tk.MustExec("update t set t1 = '2017-10-01 10:10:11', t2 = date_add(t1, INTERVAL 10 MINUTE) where id = 1")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
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
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
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
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")
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
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect DECIMAL value: '202.23'"))
	r = tk.MustQuery("select * from decimals")
	r.Check(testkit.Rows("202"))

	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE `t` (	`c1` year DEFAULT NULL, `c2` year DEFAULT NULL, `c3` date DEFAULT NULL, `c4` datetime DEFAULT NULL,	KEY `idx` (`c1`,`c2`))")
	err = tk.ExecToErr("UPDATE t SET c2=16777215 WHERE c1>= -8388608 AND c1 < -9 ORDER BY c1 LIMIT 2")
	require.NoError(t, err)

	tk.MustGetErrCode("update (select * from t) t set c1 = 1111111", mysql.ErrNonUpdatableTable)

	// test update ignore for bad null error
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (i int not null default 10)`)
	tk.MustExec("insert into t values (1)")
	tk.MustExec("update ignore t set i = null;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 1")
	r = tk.MustQuery("SHOW WARNINGS;")
	r.Check(testkit.Rows("Warning 1048 Column 'i' cannot be null"))
	tk.MustQuery("select * from t").Check(testkit.Rows("0"))

	// issue 7237, update subquery table should be forbidden
	tk.MustExec("drop table t")
	tk.MustExec("create table t (k int, v int)")
	err = tk.ExecToErr("update t, (select * from t) as b set b.k = t.k")
	require.EqualError(t, err, "[planner:1288]The target table b of the UPDATE is not updatable")
	tk.MustExec("update t, (select * from t) as b set t.k = b.k")

	// issue 8045
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`CREATE TABLE t1 (c1 float)`)
	tk.MustExec("INSERT INTO t1 SET c1 = 1")
	tk.MustExec("UPDATE t1 SET c1 = 1.2 WHERE c1=1;")
	require.Equal(t, tk.Session().LastMessage(), "Rows matched: 1  Changed: 1  Warnings: 0")

	// issue 8119
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c1 float(1,1));")
	tk.MustExec("insert into t values (0.0);")
	err = tk.ExecToErr("update t set c1 = 2.0;")
	require.True(t, types.ErrWarnDataOutOfRange.Equal(err))

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
	err = tk.ExecToErr("update v set a = '2000-11-11'")
	require.EqualError(t, err, "[planner:1288]The target table v of the UPDATE is not updatable")
	tk.MustExec("drop view v")

	tk.MustExec("create sequence seq")
	tk.MustGetErrCode("update seq set minvalue=1", mysql.ErrBadField)
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
	// Same as in MySQL 8.0.27, but still weird behavior: a=default(b) => NULL
	tk.MustExec("update t2 set a=default(b), b=default, c=default;")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("<nil> <nil> <nil>", "<nil> <nil> <nil>"))
	tk.MustGetErrCode("update t2 set b=default(a);", mysql.ErrBadGeneratedColumn)
	tk.MustExec("update t2 set a=default(a), c=default(c)")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 -1 -1", "1 -1 -1"))
	// Same as in MySQL 8.0.27, but still weird behavior: a=default(b) => NULL
	tk.MustExec("update t2 set a=default(b), b=default(b)")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("<nil> <nil> <nil>", "<nil> <nil> <nil>"))
	tk.MustExec("update t2 set a=default(a), c=default(c)")
	tk.MustQuery("select * from t2;").Check(testkit.Rows("1 -1 -1", "1 -1 -1"))
	// Allowed in MySQL, but should probably not be allowed.
	tk.MustGetErrCode("update t2 set a=default(a), c=default(a)", mysql.ErrBadGeneratedColumn)
	tk.MustExec("drop table t1, t2")
}

func TestListColumnsPartitionWithGlobalIndex(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("abc", "acd"))
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("bbb"))
		tk.MustQuery("select * from t where a = 'bbb' order by a").Check(testkit.Rows("bbb b"))
		// Test insert meet duplicate error.
		err := tk.ExecToErr("insert into t (a) values  ('abc')")
		require.Error(t, err)
		// Test insert on duplicate update
		tk.MustExec("insert into t (a) values ('abc') on duplicate key update a='bbc'")
		tk.MustQuery("select a from t order by a").Check(testkit.Rows("acd", "bbb", "bbc"))
		tk.MustQuery("select * from t where a = 'bbc'").Check(testkit.Rows("bbc b"))
		tk.MustQuery("select a from t partition (p0) order by a").Check(testkit.Rows("acd"))
		tk.MustQuery("select a from t partition (p1) order by a").Check(testkit.Rows("bbb", "bbc"))
	}
}

func TestMutipleReplaceAndInsertInOneSession(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_securities(id bigint not null auto_increment primary key, security_id varchar(8), market_id smallint, security_type int, unique key uu(security_id, market_id))")
	tk.MustExec(`insert into t_securities (security_id, market_id, security_type) values ("1", 2, 7), ("7", 1, 7) ON DUPLICATE KEY UPDATE security_type = VALUES(security_type)`)
	tk.MustExec(`replace into t_securities (security_id, market_id, security_type) select security_id+1, 1, security_type from t_securities  where security_id="7";`)
	tk.MustExec(`INSERT INTO t_securities (security_id, market_id, security_type) values ("1", 2, 7), ("7", 1, 7) ON DUPLICATE KEY UPDATE security_type = VALUES(security_type)`)

	tk.MustQuery("select * from t_securities").Sort().Check(testkit.Rows("1 1 2 7", "2 7 1 7", "3 8 1 7"))

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	tk2.MustExec(`insert into t_securities (security_id, market_id, security_type) values ("1", 2, 7), ("7", 1, 7) ON DUPLICATE KEY UPDATE security_type = VALUES(security_type)`)
	tk2.MustExec(`insert into t_securities (security_id, market_id, security_type) select security_id+2, 1, security_type from t_securities  where security_id="7";`)
	tk2.MustExec(`INSERT INTO t_securities (security_id, market_id, security_type) values ("1", 2, 7), ("7", 1, 7) ON DUPLICATE KEY UPDATE security_type = VALUES(security_type)`)

	tk2.MustQuery("select * from t_securities").Sort().Check(testkit.Rows("1 1 2 7", "2 7 1 7", "3 8 1 7", "8 9 1 7"))
}
