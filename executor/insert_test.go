// Copyright 2018 PingCAP, Inc.
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

package executor_test

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/stretchr/testify/require"
)

func TestInsertOnDuplicateKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	tk.MustExec(`insert into t1 values(1, 100);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t2 values(1, 200);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = a2;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 1"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = b2;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 200"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update a1 = a2;`)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 200"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = 300;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 300"))

	tk.MustExec(`insert into t1 values(1, 1) on duplicate key update b1 = 400;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 400"))

	tk.MustExec(`insert into t1 select 1, 500 from t2 on duplicate key update b1 = 400;`)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 400"))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	_, err := tk.Exec(`insert into t1 select * from t2 on duplicate key update c = t2.b;`)
	require.Equal(t, `[planner:1054]Unknown column 'c' in 'field list'`, err.Error())

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	_, err = tk.Exec(`insert into t1 select * from t2 on duplicate key update a = b;`)
	require.Equal(t, `[planner:1052]Column 'b' in field list is ambiguous`, err.Error())

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	_, err = tk.Exec(`insert into t1 select * from t2 on duplicate key update c = b;`)
	require.Equal(t, `[planner:1054]Unknown column 'c' in 'field list'`, err.Error())

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	_, err = tk.Exec(`insert into t1 select * from t2 on duplicate key update a1 = values(b2);`)
	require.Equal(t, `[planner:1054]Unknown column 'b2' in 'field list'`, err.Error())

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	tk.MustExec(`insert into t1 values(1, 100);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t2 values(1, 200);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 400"))
	tk.MustExec(`insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;`)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 400"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(k1 bigint, k2 bigint, val bigint, primary key(k1, k2));`)
	tk.MustExec(`insert into t (val, k1, k2) values (3, 1, 2);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))
	tk.MustExec(`insert into t (val, k1, k2) select c, a, b from (select 1 as a, 2 as b, 4 as c) tmp on duplicate key update val = tmp.c;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 4`))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(k1 double, k2 double, v double, primary key(k1, k2));`)
	tk.MustExec(`insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))
	tk.MustExec(`insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;`)
	require.Equal(t, uint64(0), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (2, 2, 1);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (3, 3, 1);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`create table t2(a int primary key, b int, unique(b));`)
	tk.MustExec(`insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;`)
	require.Equal(t, uint64(5), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 3  Duplicates: 2  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`3 1`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (2, 1, 2);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (3, 3, 1);`)
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.CheckLastMessage("")
	tk.MustExec(`create table t2(a int primary key, b int, unique(b));`)
	tk.MustExec(`insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;`)
	require.Equal(t, uint64(4), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 3  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`1 2`, `3 1`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int, c int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1, 1);`)
	tk.MustExec(`insert into t1 values (2, 2, 1, 2);`)
	tk.MustExec(`insert into t1 values (3, 3, 2, 2);`)
	tk.MustExec(`insert into t1 values (4, 4, 2, 2);`)
	tk.MustExec(`create table t2(a int primary key, b int, c int, unique(b), unique(c));`)
	tk.MustExec(`insert into t2 select a, b, c from t1 order by id on duplicate key update b=t2.b, c=t2.c;`)
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 4  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`1 1 1`, `3 2 2`))

	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(a int primary key, b int);`)
	tk.MustExec(`insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);`)
	require.Equal(t, uint64(5), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 5  Duplicates: 0  Warnings: 0")
	tk.MustExec(`insert into t1 values(4,14),(5,15),(6,16),(7,17),(8,18) on duplicate key update b=b+10`)
	require.Equal(t, uint64(7), tk.Session().AffectedRows())
	tk.CheckLastMessage("Records: 5  Duplicates: 2  Warnings: 0")

	tk.MustExec("drop table if exists a, b")
	tk.MustExec("create table a(x int primary key)")
	tk.MustExec("create table b(x int, y int)")
	tk.MustExec("insert into a values(1)")
	tk.MustExec("insert into b values(1, 2)")
	tk.MustExec("insert into a select x from b ON DUPLICATE KEY UPDATE a.x=b.y")
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.MustQuery("select * from a").Check(testkit.Rows("2"))

	// Test issue 28078.
	// Use different types of columns so that there's likely to be error if the types mismatches.
	tk.MustExec("drop table if exists a, b")
	tk.MustExec("create table a(id int, a1 timestamp, a2 varchar(10), a3 float, unique(id))")
	tk.MustExec("create table b(id int, b1 time, b2 varchar(10), b3 int)")
	tk.MustExec("insert into a values (1, '2022-01-04 07:02:04', 'a', 1.1), (2, '2022-01-04 07:02:05', 'b', 2.2)")
	tk.MustExec("insert into b values (2, '12:34:56', 'c', 10), (3, '01:23:45', 'd', 20)")
	tk.MustExec("insert into a (id) select id from b on duplicate key update a.a2 = b.b2, a.a3 = 3.3")
	require.Equal(t, uint64(3), tk.Session().AffectedRows())
	tk.MustQuery("select * from a").Check(testkit.RowsWithSep("/",
		"1/2022-01-04 07:02:04/a/1.1",
		"2/2022-01-04 07:02:05/c/3.3",
		"3/<nil>/<nil>/<nil>"))
	tk.MustExec("insert into a (id) select 4 from b where b3 = 20 on duplicate key update a.a3 = b.b3")
	require.Equal(t, uint64(1), tk.Session().AffectedRows())
	tk.MustQuery("select * from a").Check(testkit.RowsWithSep("/",
		"1/2022-01-04 07:02:04/a/1.1",
		"2/2022-01-04 07:02:05/c/3.3",
		"3/<nil>/<nil>/<nil>",
		"4/<nil>/<nil>/<nil>"))
	tk.MustExec("insert into a (a2, a3) select 'x', 1.2 from b on duplicate key update a.a2 = b.b3")
	require.Equal(t, uint64(2), tk.Session().AffectedRows())
	tk.MustQuery("select * from a").Check(testkit.RowsWithSep("/",
		"1/2022-01-04 07:02:04/a/1.1",
		"2/2022-01-04 07:02:05/c/3.3",
		"3/<nil>/<nil>/<nil>",
		"4/<nil>/<nil>/<nil>",
		"<nil>/<nil>/x/1.2",
		"<nil>/<nil>/x/1.2"))

	// reproduce insert on duplicate key update bug under new row format.
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(c1 decimal(6,4), primary key(c1))`)
	tk.MustExec(`insert into t1 set c1 = 0.1`)
	tk.MustExec(`insert into t1 set c1 = 0.1 on duplicate key update c1 = 1`)
	tk.MustQuery(`select * from t1 use index(primary)`).Check(testkit.Rows(`1.0000`))
}

func TestClusterIndexInsertOnDuplicateKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists cluster_index_duplicate_entry_error;")
	tk.MustExec("create database cluster_index_duplicate_entry_error;")
	tk.MustExec("use cluster_index_duplicate_entry_error;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("create table t(a char(20), b int, primary key(a));")
	tk.MustExec("insert into t values('aa', 1), ('bb', 1);")
	_, err := tk.Exec("insert into t values('aa', 2);")
	require.Regexp(t, ".*Duplicate entry 'aa' for.*", err.Error())

	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a char(20), b varchar(30), c varchar(10), primary key(a, b, c));")
	tk.MustExec("insert into t values ('a', 'b', 'c'), ('b', 'a', 'c');")
	_, err = tk.Exec("insert into t values ('a', 'b', 'c');")
	require.Regexp(t, ".*Duplicate entry 'a-b-c' for.*", err.Error())
}

func TestPaddingCommonHandle(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t1;")
	tk.MustExec(`create table t1(c1 decimal(6,4), primary key(c1))`)
	tk.MustExec(`insert into t1 set c1 = 0.1`)
	tk.MustExec(`insert into t1 set c1 = 0.1 on duplicate key update c1 = 1`)
	tk.MustQuery(`select * from t1`).Check(testkit.Rows(`1.0000`))
}

func TestInsertReorgDelete(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	inputs := []struct {
		typ string
		dat string
	}{
		{"year", "'2004'"},
		{"year", "2004"},
		{"bit", "1"},
		{"smallint unsigned", "1"},
		{"int unsigned", "1"},
		{"smallint", "-1"},
		{"int", "-1"},
		{"decimal(6,4)", "'1.1'"},
		{"decimal", "1.1"},
		{"numeric", "-1"},
		{"float", "1.2"},
		{"double", "1.2"},
		{"double", "1.3"},
		{"real", "1.4"},
		{"date", "'2020-01-01'"},
		{"time", "'20:00:00'"},
		{"datetime", "'2020-01-01 22:22:22'"},
		{"timestamp", "'2020-01-01 22:22:22'"},
		{"year", "'2020'"},
		{"char(15)", "'test'"},
		{"varchar(15)", "'test'"},
		{"binary(3)", "'a'"},
		{"varbinary(3)", "'b'"},
		{"blob", "'test'"},
		{"text", "'test'"},
		{"enum('a', 'b')", "'a'"},
		{"set('a', 'b')", "'a,b'"},
	}

	for _, i := range inputs {
		tk.MustExec(`drop table if exists t1`)
		tk.MustExec(fmt.Sprintf(`create table t1(c1 %s)`, i.typ))
		tk.MustExec(fmt.Sprintf(`insert into t1 set c1 = %s`, i.dat))
		switch i.typ {
		case "blob", "text":
			tk.MustExec(`alter table t1 add index idx(c1(3))`)
		default:
			tk.MustExec(`alter table t1 add index idx(c1)`)
		}
		tk.MustExec(`delete from t1`)
		tk.MustExec(`admin check table t1`)
	}
}

func TestUpdateDuplicateKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table c(i int,j int,k int,primary key(i,j,k));`)
	tk.MustExec(`insert into c values(1,2,3);`)
	tk.MustExec(`insert into c values(1,2,4);`)
	_, err := tk.Exec(`update c set i=1,j=2,k=4 where i=1 and j=2 and k=3;`)
	require.EqualError(t, err, "[kv:1062]Duplicate entry '1-2-4' for key 'PRIMARY'")
}

func TestInsertWrongValueForField(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a bigint);`)
	_, err := tk.Exec(`insert into t1 values("asfasdfsajhlkhlksdaf");`)
	require.True(t, terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField))

	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a varchar(10)) charset ascii;`)
	_, err = tk.Exec(`insert into t1 values('我');`)
	require.True(t, terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField))

	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a char(10) charset utf8);`)
	tk.MustExec(`insert into t1 values('我');`)
	tk.MustExec(`alter table t1 add column b char(10) charset ascii as ((a));`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("我 ?"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a year);`)
	tk.MustGetErrMsg(`insert into t values(2156);`,
		"[types:1264]Out of range value for column 'a' at row 1")

	tk.MustExec(`DROP TABLE IF EXISTS ts`)
	tk.MustExec(`CREATE TABLE ts (id int DEFAULT NULL, time1 TIMESTAMP NULL DEFAULT NULL)`)
	tk.MustExec(`SET @@sql_mode=''`)
	tk.MustExec(`INSERT INTO ts (id, time1) VALUES (1, TIMESTAMP '1018-12-23 00:00:00')`)
	tk.MustQuery(`SHOW WARNINGS`).Check(testkit.Rows(`Warning 1292 Incorrect timestamp value: '1018-12-23 00:00:00'`))
	tk.MustQuery(`SELECT * FROM ts ORDER BY id`).Check(testkit.Rows(`1 0000-00-00 00:00:00`))

	tk.MustExec(`SET @@sql_mode='STRICT_TRANS_TABLES'`)
	_, err = tk.Exec(`INSERT INTO ts (id, time1) VALUES (2, TIMESTAMP '1018-12-24 00:00:00')`)
	require.EqualError(t, err, `[table:1292]Incorrect timestamp value: '1018-12-24 00:00:00' for column 'time1' at row 1`)
	tk.MustExec(`DROP TABLE ts`)
}

func TestInsertValueForCastDecimalField(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a decimal(15,2));`)
	tk.MustExec(`insert into t1 values (1111111111111.01);`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows(`1111111111111.01`))
	tk.MustQuery(`select cast(a as decimal) from t1;`).Check(testkit.Rows(`9999999999`))
}

func TestInsertDateTimeWithTimeZone(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test;`)
	tk.MustExec(`set time_zone="+09:00";`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (id int, c1 datetime not null default CURRENT_TIMESTAMP);`)
	tk.MustExec(`set TIMESTAMP = 1234;`)
	tk.MustExec(`insert t (id) values (1);`)

	tk.MustQuery(`select * from t;`).Check(testkit.Rows(
		`1 1970-01-01 09:20:34`,
	))

	// test for ambiguous cases
	cases := []struct {
		lit    string
		expect string
	}{
		{"2020-10-22", "2020-10-22 00:00:00"},
		{"2020-10-22-16", "2020-10-22 16:00:00"},
		{"2020-10-22 16-31", "2020-10-22 16:31:00"},
		{"2020-10-22 16:31-15", "2020-10-22 16:31:15"},
		{"2020-10-22T16:31:15-10", "2020-10-23 10:31:15"},

		{"2020.10-22", "2020-10-22 00:00:00"},
		{"2020-10.22-16", "2020-10-22 16:00:00"},
		{"2020-10-22.16-31", "2020-10-22 16:31:00"},
		{"2020-10-22 16.31-15", "2020-10-22 16:31:15"},
		{"2020-10-22T16.31.15+14", "2020-10-22 10:31:15"},

		{"2020-10:22", "2020-10-22 00:00:00"},
		{"2020-10-22:16", "2020-10-22 16:00:00"},
		{"2020-10-22-16:31", "2020-10-22 16:31:00"},
		{"2020-10-22 16-31:15", "2020-10-22 16:31:15"},
		{"2020-10-22T16.31.15+09:30", "2020-10-22 15:01:15"},

		{"2020.10-22:16", "2020-10-22 16:00:00"},
		{"2020-10.22-16:31", "2020-10-22 16:31:00"},
		{"2020-10-22.16-31:15", "2020-10-22 16:31:15"},
		{"2020-10-22T16:31.15+09:30", "2020-10-22 15:01:15"},
	}
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (dt datetime)`)
	tk.MustExec(`set @@time_zone='+08:00'`)
	for _, ca := range cases {
		tk.MustExec(`delete from t`)
		tk.MustExec(fmt.Sprintf("insert into t values ('%s')", ca.lit))
		tk.MustQuery(`select * from t`).Check(testkit.Rows(ca.expect))
	}

	// test for time zone change
	tzcCases := []struct {
		tz1  string
		lit  string
		tz2  string
		exp1 string
		exp2 string
	}{
		{"+08:00", "2020-10-22T16:53:40Z", "+00:00", "2020-10-23 00:53:40", "2020-10-22 16:53:40"},
		{"-08:00", "2020-10-22T16:53:40Z", "+08:00", "2020-10-22 08:53:40", "2020-10-23 00:53:40"},
		{"-03:00", "2020-10-22T16:53:40+03:00", "+08:00", "2020-10-22 10:53:40", "2020-10-22 21:53:40"},
		{"+08:00", "2020-10-22T16:53:40+08:00", "+08:00", "2020-10-22 16:53:40", "2020-10-22 16:53:40"},
	}
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (dt datetime, ts timestamp)")
	for _, ca := range tzcCases {
		tk.MustExec("delete from t")
		tk.MustExec(fmt.Sprintf("set @@time_zone='%s'", ca.tz1))
		tk.MustExec(fmt.Sprintf("insert into t values ('%s', '%s')", ca.lit, ca.lit))
		tk.MustExec(fmt.Sprintf("set @@time_zone='%s'", ca.tz2))
		tk.MustQuery("select * from t").Check(testkit.Rows(ca.exp1 + " " + ca.exp2))
	}

	// test for datetime in compare
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (ts timestamp)")
	tk.MustExec("insert into t values ('2020-10-22T12:00:00Z'), ('2020-10-22T13:00:00Z'), ('2020-10-22T14:00:00Z')")
	tk.MustQuery("select count(*) from t where ts > '2020-10-22T12:00:00Z'").Check(testkit.Rows("2"))

	// test for datetime with fsp
	fspCases := []struct {
		fsp  uint
		lit  string
		exp1 string
		exp2 string
	}{
		{2, "2020-10-27T14:39:10.10+00:00", "2020-10-27 22:39:10.10", "2020-10-27 22:39:10.10"},
		{1, "2020-10-27T14:39:10.3+0200", "2020-10-27 20:39:10.3", "2020-10-27 20:39:10.3"},
		{6, "2020-10-27T14:39:10.3-02", "2020-10-28 00:39:10.300000", "2020-10-28 00:39:10.300000"},
		{2, "2020-10-27T14:39:10.10Z", "2020-10-27 22:39:10.10", "2020-10-27 22:39:10.10"},
	}

	tk.MustExec("set @@time_zone='+08:00'")
	for _, ca := range fspCases {
		tk.MustExec("drop table if exists t")
		tk.MustExec(fmt.Sprintf("create table t (dt datetime(%d), ts timestamp(%d))", ca.fsp, ca.fsp))
		tk.MustExec(fmt.Sprintf("insert into t values ('%s', '%s')", ca.lit, ca.lit))
		tk.MustQuery("select * from t").Check(testkit.Rows(ca.exp1 + " " + ca.exp2))
	}
}

func TestInsertZeroYear(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a year(4));`)
	tk.MustExec(`insert into t1 values(0000),(00),("0000"),("000"), ("00"), ("0"), (79), ("79");`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows(
		`0`,
		`0`,
		`0`,
		`2000`,
		`2000`,
		`2000`,
		`1979`,
		`1979`,
	))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(f_year year NOT NULL DEFAULT '0000')ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`insert into t values();`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(
		`0`,
	))
	tk.MustExec(`insert into t values('0000');`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(
		`0`,
		`0`,
	))
}

func TestAllowInvalidDates(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1, t2, t3, t4;`)
	tk.MustExec(`create table t1(d date);`)
	tk.MustExec(`create table t2(d datetime);`)
	tk.MustExec(`create table t3(d date);`)
	tk.MustExec(`create table t4(d datetime);`)

	runWithMode := func(mode string) {
		inputs := []string{"0000-00-00", "2019-00-00", "2019-01-00", "2019-00-01", "2019-02-31"}
		results := testkit.Rows(`0 0 0`, `2019 0 0`, `2019 1 0`, `2019 0 1`, `2019 2 31`)
		oldMode := tk.MustQuery(`select @@sql_mode`).Rows()[0][0]
		defer func() {
			tk.MustExec(fmt.Sprintf(`set sql_mode='%s'`, oldMode))
		}()

		tk.MustExec(`truncate t1;truncate t2;truncate t3;truncate t4;`)
		tk.MustExec(fmt.Sprintf(`set sql_mode='%s';`, mode))
		for _, input := range inputs {
			tk.MustExec(fmt.Sprintf(`insert into t1 values ('%s')`, input))
			tk.MustExec(fmt.Sprintf(`insert into t2 values ('%s')`, input))
		}
		tk.MustQuery(`select year(d), month(d), day(d) from t1;`).Check(results)
		tk.MustQuery(`select year(d), month(d), day(d) from t2;`).Check(results)
		tk.MustExec(`insert t3 select d from t1;`)
		tk.MustQuery(`select year(d), month(d), day(d) from t3;`).Check(results)
		tk.MustExec(`insert t4 select d from t2;`)
		tk.MustQuery(`select year(d), month(d), day(d) from t4;`).Check(results)
	}

	runWithMode("STRICT_TRANS_TABLES,ALLOW_INVALID_DATES")
	runWithMode("ALLOW_INVALID_DATES")
}

func TestInsertWithAutoidSchema(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1(id int primary key auto_increment, n int);`)
	tk.MustExec(`create table t2(id int unsigned primary key auto_increment, n int);`)
	tk.MustExec(`create table t3(id tinyint primary key auto_increment, n int);`)
	tk.MustExec(`create table t4(id int primary key, n float auto_increment, key I_n(n));`)
	tk.MustExec(`create table t5(id int primary key, n float unsigned auto_increment, key I_n(n));`)
	tk.MustExec(`create table t6(id int primary key, n double auto_increment, key I_n(n));`)
	tk.MustExec(`create table t7(id int primary key, n double unsigned auto_increment, key I_n(n));`)
	// test for inserting multiple values
	tk.MustExec(`create table t8(id int primary key auto_increment, n int);`)

	tests := []struct {
		insert string
		query  string
		result [][]interface{}
	}{
		{
			`insert into t1(id, n) values(1, 1)`,
			`select * from t1 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t1(n) values(2)`,
			`select * from t1 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t1(n) values(3)`,
			`select * from t1 where id = 3`,
			testkit.Rows(`3 3`),
		},
		{
			`insert into t1(id, n) values(-1, 4)`,
			`select * from t1 where id = -1`,
			testkit.Rows(`-1 4`),
		},
		{
			`insert into t1(n) values(5)`,
			`select * from t1 where id = 4`,
			testkit.Rows(`4 5`),
		},
		{
			`insert into t1(id, n) values('5', 6)`,
			`select * from t1 where id = 5`,
			testkit.Rows(`5 6`),
		},
		{
			`insert into t1(n) values(7)`,
			`select * from t1 where id = 6`,
			testkit.Rows(`6 7`),
		},
		{
			`insert into t1(id, n) values(7.4, 8)`,
			`select * from t1 where id = 7`,
			testkit.Rows(`7 8`),
		},
		{
			`insert into t1(id, n) values(7.5, 9)`,
			`select * from t1 where id = 8`,
			testkit.Rows(`8 9`),
		},
		{
			`insert into t1(n) values(9)`,
			`select * from t1 where id = 9`,
			testkit.Rows(`9 9`),
		},
		// test last insert id
		{
			`insert into t1 values(3000, -1), (null, -2)`,
			`select * from t1 where id = 3000`,
			testkit.Rows(`3000 -1`),
		},
		{
			`;`,
			`select * from t1 where id = 3001`,
			testkit.Rows(`3001 -2`),
		},
		{
			`;`,
			`select last_insert_id()`,
			testkit.Rows(`3001`),
		},
		{
			`insert into t2(id, n) values(1, 1)`,
			`select * from t2 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t2(n) values(2)`,
			`select * from t2 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t2(n) values(3)`,
			`select * from t2 where id = 3`,
			testkit.Rows(`3 3`),
		},
		{
			`insert into t3(id, n) values(1, 1)`,
			`select * from t3 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t3(n) values(2)`,
			`select * from t3 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t3(n) values(3)`,
			`select * from t3 where id = 3`,
			testkit.Rows(`3 3`),
		},
		{
			`insert into t3(id, n) values(-1, 4)`,
			`select * from t3 where id = -1`,
			testkit.Rows(`-1 4`),
		},
		{
			`insert into t3(n) values(5)`,
			`select * from t3 where id = 4`,
			testkit.Rows(`4 5`),
		},
		{
			`insert into t4(id, n) values(1, 1)`,
			`select * from t4 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t4(id) values(2)`,
			`select * from t4 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t4(id, n) values(3, -1)`,
			`select * from t4 where id = 3`,
			testkit.Rows(`3 -1`),
		},
		{
			`insert into t4(id) values(4)`,
			`select * from t4 where id = 4`,
			testkit.Rows(`4 3`),
		},
		{
			`insert into t4(id, n) values(5, 5.5)`,
			`select * from t4 where id = 5`,
			testkit.Rows(`5 5.5`),
		},
		{
			`insert into t4(id) values(6)`,
			`select * from t4 where id = 6`,
			testkit.Rows(`6 7`),
		},
		{
			`insert into t4(id, n) values(7, '7.7')`,
			`select * from t4 where id = 7`,
			testkit.Rows(`7 7.7`),
		},
		{
			`insert into t4(id) values(8)`,
			`select * from t4 where id = 8`,
			testkit.Rows(`8 9`),
		},
		{
			`insert into t4(id, n) values(9, 10.4)`,
			`select * from t4 where id = 9`,
			testkit.Rows(`9 10.4`),
		},
		{
			`insert into t4(id) values(10)`,
			`select * from t4 where id = 10`,
			testkit.Rows(`10 11`),
		},
		{
			`insert into t5(id, n) values(1, 1)`,
			`select * from t5 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t5(id) values(2)`,
			`select * from t5 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t5(id) values(3)`,
			`select * from t5 where id = 3`,
			testkit.Rows(`3 3`),
		},
		{
			`insert into t6(id, n) values(1, 1)`,
			`select * from t6 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t6(id) values(2)`,
			`select * from t6 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t6(id, n) values(3, -1)`,
			`select * from t6 where id = 3`,
			testkit.Rows(`3 -1`),
		},
		{
			`insert into t6(id) values(4)`,
			`select * from t6 where id = 4`,
			testkit.Rows(`4 3`),
		},
		{
			`insert into t6(id, n) values(5, 5.5)`,
			`select * from t6 where id = 5`,
			testkit.Rows(`5 5.5`),
		},
		{
			`insert into t6(id) values(6)`,
			`select * from t6 where id = 6`,
			testkit.Rows(`6 7`),
		},
		{
			`insert into t6(id, n) values(7, '7.7')`,
			`select * from t4 where id = 7`,
			testkit.Rows(`7 7.7`),
		},
		{
			`insert into t6(id) values(8)`,
			`select * from t4 where id = 8`,
			testkit.Rows(`8 9`),
		},
		{
			`insert into t6(id, n) values(9, 10.4)`,
			`select * from t6 where id = 9`,
			testkit.Rows(`9 10.4`),
		},
		{
			`insert into t6(id) values(10)`,
			`select * from t6 where id = 10`,
			testkit.Rows(`10 11`),
		},
		{
			`insert into t7(id, n) values(1, 1)`,
			`select * from t7 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`insert into t7(id) values(2)`,
			`select * from t7 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`insert into t7(id) values(3)`,
			`select * from t7 where id = 3`,
			testkit.Rows(`3 3`),
		},

		// the following is test for insert multiple values.
		{
			`insert into t8(n) values(1),(2)`,
			`select * from t8 where id = 1`,
			testkit.Rows(`1 1`),
		},
		{
			`;`,
			`select * from t8 where id = 2`,
			testkit.Rows(`2 2`),
		},
		{
			`;`,
			`select last_insert_id();`,
			testkit.Rows(`1`),
		},
		// test user rebase and auto alloc mixture.
		{
			`insert into t8 values(null, 3),(-1, -1),(null,4),(null, 5)`,
			`select * from t8 where id = 3`,
			testkit.Rows(`3 3`),
		},
		// -1 won't rebase allocator here cause -1 < base.
		{
			`;`,
			`select * from t8 where id = -1`,
			testkit.Rows(`-1 -1`),
		},
		{
			`;`,
			`select * from t8 where id = 4`,
			testkit.Rows(`4 4`),
		},
		{
			`;`,
			`select * from t8 where id = 5`,
			testkit.Rows(`5 5`),
		},
		{
			`;`,
			`select last_insert_id();`,
			testkit.Rows(`3`),
		},
		{
			`insert into t8 values(null, 6),(10, 7),(null, 8)`,
			`select * from t8 where id = 6`,
			testkit.Rows(`6 6`),
		},
		// 10 will rebase allocator here.
		{
			`;`,
			`select * from t8 where id = 10`,
			testkit.Rows(`10 7`),
		},
		{
			`;`,
			`select * from t8 where id = 11`,
			testkit.Rows(`11 8`),
		},
		{
			`;`,
			`select last_insert_id()`,
			testkit.Rows(`6`),
		},
		// fix bug for last_insert_id should be first allocated id in insert rows (skip the rebase id).
		{
			`insert into t8 values(100, 9),(null,10),(null,11)`,
			`select * from t8 where id = 100`,
			testkit.Rows(`100 9`),
		},
		{
			`;`,
			`select * from t8 where id = 101`,
			testkit.Rows(`101 10`),
		},
		{
			`;`,
			`select * from t8 where id = 102`,
			testkit.Rows(`102 11`),
		},
		{
			`;`,
			`select last_insert_id()`,
			testkit.Rows(`101`),
		},
		// test with sql_mode: NO_AUTO_VALUE_ON_ZERO.
		{
			`;`,
			`select @@sql_mode`,
			testkit.Rows(`ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION`),
		},
		{
			`;`,
			"set session sql_mode = `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,NO_AUTO_VALUE_ON_ZERO`",
			nil,
		},
		{
			`insert into t8 values (0, 12), (null, 13)`,
			`select * from t8 where id = 0`,
			testkit.Rows(`0 12`),
		},
		{
			`;`,
			`select * from t8 where id = 103`,
			testkit.Rows(`103 13`),
		},
		{
			`;`,
			`select last_insert_id()`,
			testkit.Rows(`103`),
		},
		// test without sql_mode: NO_AUTO_VALUE_ON_ZERO.
		{
			`;`,
			"set session sql_mode = `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION`",
			nil,
		},
		// value 0 will be substitute by autoid.
		{
			`insert into t8 values (0, 14), (null, 15)`,
			`select * from t8 where id = 104`,
			testkit.Rows(`104 14`),
		},
		{
			`;`,
			`select * from t8 where id = 105`,
			testkit.Rows(`105 15`),
		},
		{
			`;`,
			`select last_insert_id()`,
			testkit.Rows(`104`),
		},
		// last test : auto increment allocation can find in retryInfo.
		{
			`retry : insert into t8 values (null, 16), (null, 17)`,
			`select * from t8 where id = 1000`,
			testkit.Rows(`1000 16`),
		},
		{
			`;`,
			`select * from t8 where id = 1001`,
			testkit.Rows(`1001 17`),
		},
		{
			`;`,
			`select last_insert_id()`,
			// this insert doesn't has the last_insert_id, should be same as the last insert case.
			testkit.Rows(`104`),
		},
	}

	for _, tt := range tests {
		if strings.HasPrefix(tt.insert, "retry : ") {
			// it's the last retry insert case, change the sessionVars.
			retryInfo := &variable.RetryInfo{Retrying: true}
			retryInfo.AddAutoIncrementID(1000)
			retryInfo.AddAutoIncrementID(1001)
			tk.Session().GetSessionVars().RetryInfo = retryInfo
			tk.MustExec(tt.insert[8:])
			tk.Session().GetSessionVars().RetryInfo = &variable.RetryInfo{}
		} else {
			tk.MustExec(tt.insert)
		}
		if tt.query == "set session sql_mode = `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,NO_AUTO_VALUE_ON_ZERO`" ||
			tt.query == "set session sql_mode = `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION`" {
			tk.MustExec(tt.query)
		} else {
			tk.MustQuery(tt.query).Check(tt.result)
		}
	}

}

func TestPartitionInsertOnDuplicate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int,b int,primary key(a,b)) partition by range(a) (partition p0 values less than (100),partition p1 values less than (1000))`)
	tk.MustExec(`insert into t1 set a=1, b=1`)
	tk.MustExec(`insert into t1 set a=1,b=1 on duplicate key update a=1,b=1`)
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 1"))

	tk.MustExec(`create table t2 (a int,b int,primary key(a,b)) partition by hash(a) partitions 4`)
	tk.MustExec(`insert into t2 set a=1,b=1;`)
	tk.MustExec(`insert into t2 set a=1,b=1 on duplicate key update a=1,b=1`)
	tk.MustQuery(`select * from t2`).Check(testkit.Rows("1 1"))

	tk.MustExec(`CREATE TABLE t3 (a int, b int, c int, d int, e int,
  PRIMARY KEY (a,b),
  UNIQUE KEY (b,c,d)
) PARTITION BY RANGE ( b ) (
  PARTITION p0 VALUES LESS THAN (4),
  PARTITION p1 VALUES LESS THAN (7),
  PARTITION p2 VALUES LESS THAN (11)
)`)
	tk.MustExec("insert into t3 values (1,2,3,4,5)")
	tk.MustExec("insert into t3 values (1,2,3,4,5),(6,2,3,4,6) on duplicate key update e = e + values(e)")
	tk.MustQuery("select * from t3").Check(testkit.Rows("1 2 3 4 16"))
}

func TestBit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a bit(3))`)
	_, err := tk.Exec("insert into t1 values(-1)")
	require.True(t, types.ErrDataTooLong.Equal(err))
	require.Regexp(t, ".*Data too long for column 'a' at.*", err.Error())
	_, err = tk.Exec("insert into t1 values(9)")
	require.Regexp(t, ".*Data too long for column 'a' at.*", err.Error())

	tk.MustExec(`create table t64 (a bit(64))`)
	tk.MustExec("insert into t64 values(-1)")
	tk.MustExec("insert into t64 values(18446744073709551615)")      // 2^64 - 1
	_, err = tk.Exec("insert into t64 values(18446744073709551616)") // z^64
	require.Regexp(t, ".*Out of range value for column 'a' at.*", err.Error())

}

func TestAllocateContinuousRowID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int,b int, key I_a(a));`)
	var wg util.WaitGroupWrapper
	for i := 0; i < 5; i++ {
		idx := i
		wg.Run(func() {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			for j := 0; j < 10; j++ {
				k := strconv.Itoa(idx*100 + j)
				sql := "insert into t1(a,b) values (" + k + ", 2)"
				for t := 0; t < 20; t++ {
					sql += ",(" + k + ",2)"
				}
				tk.MustExec(sql)
				q := "select _tidb_rowid from t1 where a=" + k
				rows := tk.MustQuery(q).Rows()
				require.Equal(t, 21, len(rows))
				last := 0
				for _, r := range rows {
					require.Equal(t, 1, len(r))
					v, err := strconv.Atoi(r[0].(string))
					require.Equal(t, nil, err)
					if last > 0 {
						require.Equal(t, v, last+1)
					}
					last = v
				}
			}
		})
	}
	wg.Wait()
}

func TestJiraIssue5366(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table bug (a varchar(100))`)
	tk.MustExec(` insert into bug select  ifnull(JSON_UNQUOTE(JSON_EXTRACT('[{"amount":2000,"feeAmount":0,"merchantNo":"20190430140319679394","shareBizCode":"20160311162_SECOND"}]', '$[0].merchantNo')),'') merchant_no union SELECT '20180531557' merchant_no;`)
	tk.MustQuery(`select * from bug`).Sort().Check(testkit.Rows("20180531557", "20190430140319679394"))
}

func TestDMLCast(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b double)`)
	tk.MustExec(`insert into t values (ifnull('',0)+0, 0)`)
	tk.MustExec(`insert into t values (0, ifnull('',0)+0)`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows("0 0", "0 0"))
	_, err := tk.Exec(`insert into t values ('', 0)`)
	require.Error(t, err)
	_, err = tk.Exec(`insert into t values (0, '')`)
	require.Error(t, err)
	_, err = tk.Exec(`update t set a = ''`)
	require.Error(t, err)
	_, err = tk.Exec(`update t set b = ''`)
	require.Error(t, err)
	tk.MustExec("update t set a = ifnull('',0)+0")
	tk.MustExec("update t set b = ifnull('',0)+0")
	tk.MustExec("delete from t where a = ''")
	tk.MustQuery(`select * from t`).Check(testkit.Rows())
}

func TestInsertFloatOverflow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t,t1;`)
	tk.MustExec("create table t(col1 FLOAT, col2 FLOAT(10,2), col3 DOUBLE, col4 DOUBLE(10,2), col5 DECIMAL, col6 DECIMAL(10,2));")
	_, err := tk.Exec("insert into t values (-3.402823466E+68, -34028234.6611, -1.7976931348623157E+308, -17976921.34, -9999999999, -99999999.99);")
	require.EqualError(t, err, "[types:1264]Out of range value for column 'col1' at row 1")
	_, err = tk.Exec("insert into t values (-34028234.6611, -3.402823466E+68, -1.7976931348623157E+308, -17976921.34, -9999999999, -99999999.99);")
	require.EqualError(t, err, "[types:1264]Out of range value for column 'col2' at row 1")
	_, err = tk.Exec("create table t1(id1 float,id2 float)")
	require.NoError(t, err)
	_, err = tk.Exec("insert ignore into t1 values(999999999999999999999999999999999999999,-999999999999999999999999999999999999999)")
	require.NoError(t, err)
	tk.MustQuery("select @@warning_count").Check(testkit.RowsWithSep("|", "2"))
	tk.MustQuery("select convert(id1,decimal(65)),convert(id2,decimal(65)) from t1").Check(testkit.Rows("340282346638528860000000000000000000000 -340282346638528860000000000000000000000"))
	tk.MustExec("drop table if exists t,t1")
}

// TestAutoIDIncrementAndOffset There is a potential issue in MySQL: when the value of auto_increment_offset is greater
// than that of auto_increment_increment, the value of auto_increment_offset is ignored
// (https://dev.mysql.com/doc/refman/8.0/en/replication-options-master.html#sysvar_auto_increment_increment),
// This issue is a flaw of the implementation of MySQL and it doesn't exist in TiDB.
func TestAutoIDIncrementAndOffset(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	// Test for offset is larger than increment.
	tk.Session().GetSessionVars().AutoIncrementIncrement = 5
	tk.Session().GetSessionVars().AutoIncrementOffset = 10
	tk.MustExec(`create table io (a int key auto_increment)`)
	tk.MustExec(`insert into io values (null),(null),(null)`)
	tk.MustQuery(`select * from io`).Check(testkit.Rows("10", "15", "20"))
	tk.MustExec(`drop table io`)

	// Test handle is PK.
	tk.MustExec(`create table io (a int key auto_increment)`)
	tk.Session().GetSessionVars().AutoIncrementOffset = 10
	tk.Session().GetSessionVars().AutoIncrementIncrement = 2
	tk.MustExec(`insert into io values (),(),()`)
	tk.MustQuery(`select * from io`).Check(testkit.Rows("10", "12", "14"))
	tk.MustExec(`delete from io`)

	// Test reset the increment.
	tk.Session().GetSessionVars().AutoIncrementIncrement = 5
	tk.MustExec(`insert into io values (),(),()`)
	tk.MustQuery(`select * from io`).Check(testkit.Rows("15", "20", "25"))
	tk.MustExec(`delete from io`)

	tk.Session().GetSessionVars().AutoIncrementIncrement = 10
	tk.MustExec(`insert into io values (),(),()`)
	tk.MustQuery(`select * from io`).Check(testkit.Rows("30", "40", "50"))
	tk.MustExec(`delete from io`)

	tk.Session().GetSessionVars().AutoIncrementIncrement = 5
	tk.MustExec(`insert into io values (),(),()`)
	tk.MustQuery(`select * from io`).Check(testkit.Rows("55", "60", "65"))
	tk.MustExec(`drop table io`)

	// Test handle is not PK.
	tk.Session().GetSessionVars().AutoIncrementIncrement = 2
	tk.Session().GetSessionVars().AutoIncrementOffset = 10
	tk.MustExec(`create table io (a int, b int auto_increment, key(b))`)
	tk.MustExec(`insert into io(b) values (null),(null),(null)`)
	// AutoID allocation will take increment and offset into consideration.
	tk.MustQuery(`select b from io`).Check(testkit.Rows("10", "12", "14"))
	// HandleID allocation will ignore the increment and offset.
	tk.MustQuery(`select _tidb_rowid from io`).Check(testkit.Rows("15", "16", "17"))
	tk.MustExec(`delete from io`)

	tk.Session().GetSessionVars().AutoIncrementIncrement = 10
	tk.MustExec(`insert into io(b) values (null),(null),(null)`)
	tk.MustQuery(`select b from io`).Check(testkit.Rows("20", "30", "40"))
	tk.MustQuery(`select _tidb_rowid from io`).Check(testkit.Rows("41", "42", "43"))

	// Test invalid value.
	tk.Session().GetSessionVars().AutoIncrementIncrement = -1
	tk.Session().GetSessionVars().AutoIncrementOffset = -2
	_, err := tk.Exec(`insert into io(b) values (null),(null),(null)`)
	require.Error(t, err)
	require.EqualError(t, err, "[autoid:8060]Invalid auto_increment settings: auto_increment_increment: -1, auto_increment_offset: -2, both of them must be in range [1..65535]")
	tk.MustExec(`delete from io`)

	tk.Session().GetSessionVars().AutoIncrementIncrement = 65536
	tk.Session().GetSessionVars().AutoIncrementOffset = 65536
	_, err = tk.Exec(`insert into io(b) values (null),(null),(null)`)
	require.Error(t, err)
	require.EqualError(t, err, "[autoid:8060]Invalid auto_increment settings: auto_increment_increment: 65536, auto_increment_offset: 65536, both of them must be in range [1..65535]")
}

// Fix https://github.com/pingcap/tidb/issues/32601.
func TestTextTooLongError(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Set strict sql_mode
	tk.MustExec("set sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_ALL_TABLES,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';")

	// For max_allowed_packet default value is big enough to ensure tinytext, text can test correctly.
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec("CREATE TABLE t1(c1 TINYTEXT CHARACTER SET utf8mb4);")
	_, err := tk.Exec("INSERT INTO t1 (c1) VALUES(REPEAT(X'C385', 128));")
	require.EqualError(t, err, "[types:1406]Data too long for column 'c1' at row 1")

	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec("CREATE TABLE t1(c1 Text CHARACTER SET utf8mb4);")
	_, err = tk.Exec("INSERT INTO t1 (c1) VALUES(REPEAT(X'C385', 32768));")
	require.EqualError(t, err, "[types:1406]Data too long for column 'c1' at row 1")

	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec("CREATE TABLE t1(c1 mediumtext);")
	_, err = tk.Exec("INSERT INTO t1 (c1) VALUES(REPEAT(X'C385', 8777215));")
	require.EqualError(t, err, "[types:1406]Data too long for column 'c1' at row 1")

	// For long text, max_allowed_packet default value can not allow 4GB package, skip the test case.

	// Set non strict sql_mode, we are not supposed to raise an error but to truncate the value.
	tk.MustExec("set sql_mode = 'ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';")

	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec("CREATE TABLE t1(c1 TINYTEXT CHARACTER SET utf8mb4);")
	_, err = tk.Exec("INSERT INTO t1 (c1) VALUES(REPEAT(X'C385', 128));")
	require.NoError(t, err)
	tk.MustQuery(`select length(c1) from t1;`).Check(testkit.Rows("254"))

	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec("CREATE TABLE t1(c1 Text CHARACTER SET utf8mb4);")
	_, err = tk.Exec("INSERT INTO t1 (c1) VALUES(REPEAT(X'C385', 32768));")
	require.NoError(t, err)
	tk.MustQuery(`select length(c1) from t1;`).Check(testkit.Rows("65534"))
	// For mediumtext or bigger size, for tikv limit, we will get:ERROR 8025 (HY000): entry too large, the max entry size is 6291456, the size of data is 16777247, no need to test.
}

func TestAutoRandomID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key clustered auto_random, name char(10))`)

	tk.MustExec(`insert into ar(id) values (null)`)
	rs := tk.MustQuery(`select id from ar`)
	require.Equal(t, 1, len(rs.Rows()))
	firstValue, err := strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(id) values (0)`)
	rs = tk.MustQuery(`select id from ar`)
	require.Equal(t, 1, len(rs.Rows()))
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(name) values ('a')`)
	rs = tk.MustQuery(`select id from ar`)
	require.Equal(t, 1, len(rs.Rows()))
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))

	tk.MustExec(`drop table ar`)
	tk.MustExec(`create table ar (id bigint key clustered auto_random(15), name char(10))`)
	overflowVal := 1 << (64 - 5)
	errMsg := fmt.Sprintf(autoid.AutoRandomRebaseOverflow, overflowVal, 1<<(64-16)-1)
	_, err = tk.Exec(fmt.Sprintf("alter table ar auto_random_base = %d", overflowVal))
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), errMsg))
}

func TestMultiAutoRandomID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key clustered auto_random, name char(10))`)

	tk.MustExec(`insert into ar(id) values (null),(null),(null)`)
	rs := tk.MustQuery(`select id from ar order by id`)
	require.Equal(t, 3, len(rs.Rows()))
	firstValue, err := strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	require.Equal(t, fmt.Sprintf("%d", firstValue+1), rs.Rows()[1][0].(string))
	require.Equal(t, fmt.Sprintf("%d", firstValue+2), rs.Rows()[2][0].(string))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(id) values (0),(0),(0)`)
	rs = tk.MustQuery(`select id from ar order by id`)
	require.Equal(t, 3, len(rs.Rows()))
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	require.Equal(t, fmt.Sprintf("%d", firstValue+1), rs.Rows()[1][0].(string))
	require.Equal(t, fmt.Sprintf("%d", firstValue+2), rs.Rows()[2][0].(string))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(name) values ('a'),('a'),('a')`)
	rs = tk.MustQuery(`select id from ar order by id`)
	require.Equal(t, 3, len(rs.Rows()))
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	require.Equal(t, fmt.Sprintf("%d", firstValue+1), rs.Rows()[1][0].(string))
	require.Equal(t, fmt.Sprintf("%d", firstValue+2), rs.Rows()[2][0].(string))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))

	tk.MustExec(`drop table ar`)
}

func TestAutoRandomIDAllowZero(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key clustered auto_random, name char(10))`)

	rs := tk.MustQuery(`select @@session.sql_mode`)
	sqlMode := rs.Rows()[0][0].(string)
	tk.MustExec(fmt.Sprintf(`set session sql_mode="%s,%s"`, sqlMode, "NO_AUTO_VALUE_ON_ZERO"))

	tk.MustExec(`insert into ar(id) values (0)`)
	rs = tk.MustQuery(`select id from ar`)
	require.Equal(t, 1, len(rs.Rows()))
	firstValue, err := strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Equal(t, 0, firstValue)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(id) values (null)`)
	rs = tk.MustQuery(`select id from ar`)
	require.Equal(t, 1, len(rs.Rows()))
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	require.NoError(t, err)
	require.Greater(t, firstValue, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))

	tk.MustExec(`drop table ar`)
}

func TestAutoRandomIDExplicit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key clustered auto_random, name char(10))`)

	tk.MustExec(`insert into ar(id) values (1)`)
	tk.MustQuery(`select id from ar`).Check(testkit.Rows("1"))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows("0"))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(id) values (1), (2)`)
	tk.MustQuery(`select id from ar`).Check(testkit.Rows("1", "2"))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows("0"))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`drop table ar`)
}

func TestInsertErrorMsg(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (a int primary key, b datetime, d date)`)
	_, err := tk.Exec(`insert into t values (1, '2019-02-11 30:00:00', '2019-01-31')`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Incorrect datetime value: '2019-02-11 30:00:00' for column 'b' at row 1")
}

func TestIssue16366(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(c numeric primary key);`)
	tk.MustExec("insert ignore into t values(null);")
	_, err := tk.Exec(`insert into t values(0);`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Duplicate entry '0' for key 'PRIMARY'")
}

func TestClusterPrimaryTablePlainInsert(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec(`drop table if exists t1pk`)
	tk.MustExec(`create table t1pk(id varchar(200) primary key, v int)`)
	tk.MustExec(`insert into t1pk(id, v) values('abc', 1)`)
	tk.MustQuery(`select * from t1pk`).Check(testkit.Rows("abc 1"))
	tk.MustExec(`set @@tidb_constraint_check_in_place=true`)
	tk.MustGetErrCode(`insert into t1pk(id, v) values('abc', 2)`, errno.ErrDupEntry)
	tk.MustExec(`set @@tidb_constraint_check_in_place=false`)
	tk.MustGetErrCode(`insert into t1pk(id, v) values('abc', 3)`, errno.ErrDupEntry)
	tk.MustQuery(`select v, id from t1pk`).Check(testkit.Rows("1 abc"))
	tk.MustQuery(`select id from t1pk where id = 'abc'`).Check(testkit.Rows("abc"))
	tk.MustQuery(`select v, id from t1pk where id = 'abc'`).Check(testkit.Rows("1 abc"))

	tk.MustExec(`drop table if exists t3pk`)
	tk.MustExec(`create table t3pk(id1 varchar(200), id2 varchar(200), v int, id3 int, primary key(id1, id2, id3))`)
	tk.MustExec(`insert into t3pk(id1, id2, id3, v) values('abc', 'xyz', 100, 1)`)
	tk.MustQuery(`select * from t3pk`).Check(testkit.Rows("abc xyz 1 100"))
	tk.MustExec(`set @@tidb_constraint_check_in_place=true`)
	tk.MustGetErrCode(`insert into t3pk(id1, id2, id3, v) values('abc', 'xyz', 100, 2)`, errno.ErrDupEntry)
	tk.MustExec(`set @@tidb_constraint_check_in_place=false`)
	tk.MustGetErrCode(`insert into t3pk(id1, id2, id3, v) values('abc', 'xyz', 100, 3)`, errno.ErrDupEntry)
	tk.MustQuery(`select v, id3, id2, id1 from t3pk`).Check(testkit.Rows("1 100 xyz abc"))
	tk.MustQuery(`select id3, id2, id1 from t3pk where id3 = 100 and id2 = 'xyz' and id1 = 'abc'`).Check(testkit.Rows("100 xyz abc"))
	tk.MustQuery(`select id3, id2, id1, v from t3pk where id3 = 100 and id2 = 'xyz' and id1 = 'abc'`).Check(testkit.Rows("100 xyz abc 1"))
	tk.MustExec(`insert into t3pk(id1, id2, id3, v) values('abc', 'xyz', 101, 1)`)
	tk.MustExec(`insert into t3pk(id1, id2, id3, v) values('abc', 'zzz', 101, 1)`)

	tk.MustExec(`drop table if exists t1pku`)
	tk.MustExec(`create table t1pku(id varchar(200) primary key, uk int, v int, unique key ukk(uk))`)
	tk.MustExec(`insert into t1pku(id, uk, v) values('abc', 1, 2)`)
	tk.MustQuery(`select * from t1pku where id = 'abc'`).Check(testkit.Rows("abc 1 2"))
	tk.MustGetErrCode(`insert into t1pku(id, uk, v) values('aaa', 1, 3)`, errno.ErrDupEntry)
	tk.MustQuery(`select * from t1pku`).Check(testkit.Rows("abc 1 2"))

	tk.MustQuery(`select * from t3pk where (id1, id2, id3) in (('abc', 'xyz', 100), ('abc', 'xyz', 101), ('abc', 'zzz', 101))`).
		Check(testkit.Rows("abc xyz 1 100", "abc xyz 1 101", "abc zzz 1 101"))
}

func TestClusterPrimaryTableInsertIgnore(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec(`drop table if exists it1pk`)
	tk.MustExec(`create table it1pk(id varchar(200) primary key, v int)`)
	tk.MustExec(`insert into it1pk(id, v) values('abc', 1)`)
	tk.MustExec(`insert ignore into it1pk(id, v) values('abc', 2)`)
	tk.MustQuery(`select * from it1pk where id = 'abc'`).Check(testkit.Rows("abc 1"))

	tk.MustExec(`drop table if exists it2pk`)
	tk.MustExec(`create table it2pk(id1 varchar(200), id2 varchar(200), v int, primary key(id1, id2))`)
	tk.MustExec(`insert into it2pk(id1, id2, v) values('abc', 'cba', 1)`)
	tk.MustQuery(`select * from it2pk where id1 = 'abc' and id2 = 'cba'`).Check(testkit.Rows("abc cba 1"))
	tk.MustExec(`insert ignore into it2pk(id1, id2, v) values('abc', 'cba', 2)`)
	tk.MustQuery(`select * from it2pk where id1 = 'abc' and id2 = 'cba'`).Check(testkit.Rows("abc cba 1"))

	tk.MustExec(`drop table if exists it1pku`)
	tk.MustExec(`create table it1pku(id varchar(200) primary key, uk int, v int, unique key ukk(uk))`)
	tk.MustExec(`insert into it1pku(id, uk, v) values('abc', 1, 2)`)
	tk.MustQuery(`select * from it1pku where id = 'abc'`).Check(testkit.Rows("abc 1 2"))
	tk.MustExec(`insert ignore into it1pku(id, uk, v) values('aaa', 1, 3), ('bbb', 2, 1)`)
	tk.MustQuery(`select * from it1pku`).Check(testkit.Rows("abc 1 2", "bbb 2 1"))
}

func TestClusterPrimaryTableInsertDuplicate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec(`drop table if exists dt1pi`)
	tk.MustExec(`create table dt1pi(id varchar(200) primary key, v int)`)
	tk.MustExec(`insert into dt1pi(id, v) values('abb', 1),('acc', 2)`)
	tk.MustExec(`insert into dt1pi(id, v) values('abb', 2) on duplicate key update v = v + 1`)
	tk.MustQuery(`select * from dt1pi`).Check(testkit.Rows("abb 2", "acc 2"))
	tk.MustExec(`insert into dt1pi(id, v) values('abb', 2) on duplicate key update v = v + 1, id = 'xxx'`)
	tk.MustQuery(`select * from dt1pi`).Check(testkit.Rows("acc 2", "xxx 3"))

	tk.MustExec(`drop table if exists dt1piu`)
	tk.MustExec(`create table dt1piu(id varchar(200) primary key, uk int, v int, unique key uuk(uk))`)
	tk.MustExec(`insert into dt1piu(id, uk, v) values('abb', 1, 10),('acc', 2, 20)`)
	tk.MustExec(`insert into dt1piu(id, uk, v) values('xyz', 1, 100) on duplicate key update v = v + 1`)
	tk.MustQuery(`select * from dt1piu`).Check(testkit.Rows("abb 1 11", "acc 2 20"))
	tk.MustExec(`insert into dt1piu(id, uk, v) values('abb', 1, 2) on duplicate key update v = v + 1, id = 'xxx'`)
	tk.MustQuery(`select * from dt1piu`).Check(testkit.Rows("acc 2 20", "xxx 1 12"))

	tk.MustExec(`drop table if exists ts1pk`)
	tk.MustExec(`create table ts1pk(id1 timestamp, id2 timestamp, v int, primary key(id1, id2))`)
	ts := "2018-01-01 11:11:11"
	tk.MustExec(`insert into ts1pk (id1, id2, v) values(?, ?, ?)`, ts, ts, 1)
	tk.MustQuery(`select id1, id2, v from ts1pk`).Check(testkit.Rows("2018-01-01 11:11:11 2018-01-01 11:11:11 1"))
	tk.MustExec(`insert into ts1pk (id1, id2, v) values(?, ?, ?) on duplicate key update v = values(v)`, ts, ts, 2)
	tk.MustQuery(`select id1, id2, v from ts1pk`).Check(testkit.Rows("2018-01-01 11:11:11 2018-01-01 11:11:11 2"))
	tk.MustExec(`insert into ts1pk (id1, id2, v) values(?, ?, ?) on duplicate key update v = values(v), id1 = ?`, ts, ts, 2, "2018-01-01 11:11:12")
	tk.MustQuery(`select id1, id2, v from ts1pk`).Check(testkit.Rows("2018-01-01 11:11:12 2018-01-01 11:11:11 2"))
}

func TestClusterPrimaryKeyForIndexScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("drop table if exists pkt1;")
	tk.MustExec("CREATE TABLE pkt1 (a varchar(255), b int, index idx(b), primary key(a,b));")
	tk.MustExec("insert into pkt1 values ('aaa',1);")
	tk.MustQuery(`select b from pkt1 where b = 1;`).Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists pkt2;")
	tk.MustExec("CREATE TABLE pkt2 (a varchar(255), b int, unique index idx(b), primary key(a,b));")
	tk.MustExec("insert into pkt2 values ('aaa',1);")
	tk.MustQuery(`select b from pkt2 where b = 1;`).Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists issue_18232;")
	tk.MustExec("create table issue_18232 (a int, b int, c int, d int, primary key (a, b), index idx(c));")

	iter, cnt := combination([]string{"a", "b", "c", "d"}), 0
	for {
		comb := iter()
		if comb == nil {
			break
		}
		selField := strings.Join(comb, ",")
		sql := fmt.Sprintf("select %s from issue_18232 use index (idx);", selField)
		tk.MustExec(sql)
		cnt++
	}
	require.Equal(t, 15, cnt)
}

func TestInsertRuntimeStat(t *testing.T) {
	stats := &executor.InsertRuntimeStat{
		BasicRuntimeStats:    &execdetails.BasicRuntimeStats{},
		SnapshotRuntimeStats: nil,
		CheckInsertTime:      2 * time.Second,
		Prefetch:             1 * time.Second,
	}
	stats.BasicRuntimeStats.Record(5*time.Second, 1)
	require.Equal(t, "prepare: 3s, check_insert: {total_time: 2s, mem_insert_time: 1s, prefetch: 1s}", stats.String())
	require.Equal(t, stats.Clone().String(), stats.String())
	stats.Merge(stats.Clone())
	require.Equal(t, "prepare: 6s, check_insert: {total_time: 4s, mem_insert_time: 2s, prefetch: 2s}", stats.String())
}

func TestDuplicateEntryMessage(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	for _, enable := range []variable.ClusteredIndexDefMode{variable.ClusteredIndexDefModeOn, variable.ClusteredIndexDefModeOff, variable.ClusteredIndexDefModeIntOnly} {
		tk.Session().GetSessionVars().EnableClusteredIndex = enable
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t(a int, b char(10), unique key(b)) collate utf8mb4_general_ci;")
		tk.MustExec("insert into t value (34, '12Ak');")
		tk.MustGetErrMsg("insert into t value (34, '12Ak');", "[kv:1062]Duplicate entry '12Ak' for key 'b'")

		tk.MustExec("begin optimistic;")
		tk.MustExec("insert into t value (34, '12ak');")
		tk.MustExec("delete from t where b = '12ak';")
		tk.MustGetErrMsg("commit;", "previous statement: delete from t where b = '12ak';: [kv:1062]Duplicate entry '12ak' for key 'b'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a datetime primary key);")
		tk.MustExec("insert into t values ('2020-01-01');")
		tk.MustGetErrMsg("insert into t values ('2020-01-01');", "[kv:1062]Duplicate entry '2020-01-01 00:00:00' for key 'PRIMARY'")

		tk.MustExec("begin optimistic;")
		tk.MustExec("insert into t values ('2020-01-01');")
		tk.MustExec("delete from t where a = '2020-01-01';")
		tk.MustGetErrMsg("commit;", "previous statement: delete from t where a = '2020-01-01';: [kv:1062]Duplicate entry '2020-01-01 00:00:00' for key 'PRIMARY'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a int primary key );")
		tk.MustExec("insert into t value (1);")
		tk.MustGetErrMsg("insert into t value (1);", "[kv:1062]Duplicate entry '1' for key 'PRIMARY'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a datetime unique);")
		tk.MustExec("insert into t values ('2020-01-01');")
		tk.MustGetErrMsg("insert into t values ('2020-01-01');", "[kv:1062]Duplicate entry '2020-01-01 00:00:00' for key 'a'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a datetime, b int, c varchar(10), primary key (a, b, c)) collate utf8mb4_general_ci;")
		tk.MustExec("insert into t values ('2020-01-01', 1, 'aSDd');")
		tk.MustGetErrMsg("insert into t values ('2020-01-01', 1, 'ASDD');", "[kv:1062]Duplicate entry '2020-01-01 00:00:00-1-ASDD' for key 'PRIMARY'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a datetime, b int, c varchar(10), unique key (a, b, c)) collate utf8mb4_general_ci;")
		tk.MustExec("insert into t values ('2020-01-01', 1, 'aSDd');")
		tk.MustGetErrMsg("insert into t values ('2020-01-01', 1, 'ASDD');", "[kv:1062]Duplicate entry '2020-01-01 00:00:00-1-ASDD' for key 'a'")

		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t (a char(10) collate utf8mb4_unicode_ci, b char(20) collate utf8mb4_general_ci, c int(11), primary key (a, b, c), unique key (a));")
		tk.MustExec("insert ignore into t values ('$', 'C', 10);")
		tk.MustExec("insert ignore into t values ('$', 'C', 10);")
		tk.MustQuery("show warnings;").Check(testkit.RowsWithSep("|", "Warning|1062|Duplicate entry '$-C-10' for key 'PRIMARY'"))

		tk.MustExec("begin pessimistic;")
		tk.MustExec("insert into t values ('a7', 'a', 10);")
		tk.MustGetErrMsg("insert into t values ('a7', 'a', 10);", "[kv:1062]Duplicate entry 'a7-a-10' for key 'PRIMARY'")
		tk.MustExec("rollback;")

		// Test for large unsigned integer handle.
		// See https://github.com/pingcap/tidb/issues/12420.
		tk.MustExec("drop table if exists t;")
		tk.MustExec("create table t(a bigint unsigned primary key);")
		tk.MustExec("insert into t values(18446744073709551615);")
		tk.MustGetErrMsg("insert into t values(18446744073709551615);", "[kv:1062]Duplicate entry '18446744073709551615' for key 'PRIMARY'")
	}
}

func TestIssue20768(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a year, primary key(a))")
	tk.MustExec("insert ignore into t1 values(null)")
	tk.MustExec("create table t2(a int, key(a))")
	tk.MustExec("insert into t2 values(0)")
	tk.MustQuery("select /*+ hash_join(t1) */ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows("0 0"))
	tk.MustQuery("select /*+ inl_join(t1) */ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows("0 0"))
	tk.MustQuery("select /*+ inl_join(t2) */ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows("0 0"))
	tk.MustQuery("select /*+ inl_hash_join(t1) */ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows("0 0"))
	tk.MustQuery("select /*+ inl_merge_join(t1) */ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows("0 0"))
	tk.MustQuery("select /*+ merge_join(t1) */ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows("0 0"))
}

func TestIssue10402(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table vctt (v varchar(4), c char(4))")
	tk.MustExec("insert into vctt values ('ab  ', 'ab   ')")
	tk.MustQuery("select * from vctt").Check(testkit.Rows("ab   ab"))
	tk.MustExec("delete from vctt")
	tk.Session().GetSessionVars().StmtCtx.SetWarnings(nil)
	tk.MustExec("insert into vctt values ('ab\\n\\n\\n', 'ab\\n\\n\\n'), ('ab\\t\\t\\t', 'ab\\t\\t\\t'), ('ab    ', 'ab    '), ('ab\\r\\r\\r', 'ab\\r\\r\\r')")
	require.Equal(t, uint16(4), tk.Session().GetSessionVars().StmtCtx.WarningCount())
	warns := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	require.Equal(t, "[{Warning [types:1265]Data truncated, field len 4, data len 5} {Warning [types:1265]Data truncated, field len 4, data len 5} {Warning [types:1265]Data truncated, field len 4, data len 6} {Warning [types:1265]Data truncated, field len 4, data len 5}]",
		fmt.Sprintf("%v", warns))
	tk.MustQuery("select * from vctt").Check(testkit.Rows("ab\n\n ab\n\n", "ab\t\t ab\t\t", "ab   ab", "ab\r\r ab\r\r"))
	tk.MustQuery("select length(v), length(c) from vctt").Check(testkit.Rows("4 4", "4 4", "4 2", "4 4"))
}

func combination(items []string) func() []string {
	current := 1
	buf := make([]string, len(items))
	return func() []string {
		if current >= int(math.Pow(2, float64(len(items)))) {
			return nil
		}
		buf = buf[:0]
		for i, e := range items {
			if (1<<i)&current != 0 {
				buf = append(buf, e)
			}
		}
		current++
		return buf
	}
}

// TestDuplicatedEntryErr See https://github.com/pingcap/tidb/issues/24582
func TestDuplicatedEntryErr(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int, b varchar(20), primary key(a,b(3)) clustered);")
	tk.MustExec("insert into t1 values(1,'aaaaa');")
	err := tk.ExecToErr("insert into t1 values(1,'aaaaa');")
	require.EqualError(t, err, "[kv:1062]Duplicate entry '1-aaa' for key 'PRIMARY'")
	err = tk.ExecToErr("insert into t1 select 1, 'aaa'")
	require.EqualError(t, err, "[kv:1062]Duplicate entry '1-aaa' for key 'PRIMARY'")
	tk.MustExec("insert into t1 select 1, 'bb'")
	err = tk.ExecToErr("insert into t1 select 1, 'bb'")
	require.EqualError(t, err, "[kv:1062]Duplicate entry '1-bb' for key 'PRIMARY'")
}

func TestBinaryLiteralInsertToEnum(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("drop table if exists bintest")

	tk.MustExec("create table bintest (h enum(0x61, '1', 'b')) character set utf8mb4")
	tk.MustExec("insert into bintest(h) values(0x61)")
	tk.MustQuery("select * from bintest").Check(testkit.Rows("a"))
}

func TestBinaryLiteralInsertToSet(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("drop table if exists bintest")

	tk.MustExec("create table bintest (h set(0x61, '1', 'b')) character set utf8mb4")
	tk.MustExec("insert into bintest(h) values(0x61)")
	tk.MustQuery("select * from bintest").Check(testkit.Rows("a"))
}

func TestGlobalTempTableAutoInc(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("drop table if exists temp_test")
	tk.MustExec("create global temporary table temp_test(id int primary key auto_increment) on commit delete rows")
	defer tk.MustExec("drop table if exists temp_test")

	// Data is cleared after transaction auto commits.
	tk.MustExec("insert into temp_test(id) values(0)")
	tk.MustQuery("select * from temp_test").Check(testkit.Rows())

	// Data is not cleared inside a transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into temp_test(id) values(0)")
	tk.MustQuery("select * from temp_test").Check(testkit.Rows("1"))
	tk.MustExec("commit")

	// AutoID allocator is cleared.
	tk.MustExec("begin")
	tk.MustExec("insert into temp_test(id) values(0)")
	tk.MustQuery("select * from temp_test").Check(testkit.Rows("1"))
	// Test whether auto-inc is incremental
	tk.MustExec("insert into temp_test(id) values(0)")
	tk.MustQuery("select id from temp_test order by id").Check(testkit.Rows("1", "2"))
	tk.MustExec("commit")

	// multi-value insert
	tk.MustExec("begin")
	tk.MustExec("insert into temp_test(id) values(0), (0)")
	tk.MustQuery("select id from temp_test order by id").Check(testkit.Rows("1", "2"))
	tk.MustExec("insert into temp_test(id) values(0), (0)")
	tk.MustQuery("select id from temp_test order by id").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustExec("commit")

	// rebase
	tk.MustExec("begin")
	tk.MustExec("insert into temp_test(id) values(10)")
	tk.MustExec("insert into temp_test(id) values(0)")
	tk.MustQuery("select id from temp_test order by id").Check(testkit.Rows("10", "11"))
	tk.MustExec("insert into temp_test(id) values(20), (30)")
	tk.MustExec("insert into temp_test(id) values(0), (0)")
	tk.MustQuery("select id from temp_test order by id").Check(testkit.Rows("10", "11", "20", "30", "31", "32"))
	tk.MustExec("commit")
}

func TestGlobalTempTableRowID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("drop table if exists temp_test")
	tk.MustExec("create global temporary table temp_test(id int) on commit delete rows")
	defer tk.MustExec("drop table if exists temp_test")

	// Data is cleared after transaction auto commits.
	tk.MustExec("insert into temp_test(id) values(0)")
	tk.MustQuery("select _tidb_rowid from temp_test").Check(testkit.Rows())

	// Data is not cleared inside a transaction.
	tk.MustExec("begin")
	tk.MustExec("insert into temp_test(id) values(0)")
	tk.MustQuery("select _tidb_rowid from temp_test").Check(testkit.Rows("1"))
	tk.MustExec("commit")

	// AutoID allocator is cleared.
	tk.MustExec("begin")
	tk.MustExec("insert into temp_test(id) values(0)")
	tk.MustQuery("select _tidb_rowid from temp_test").Check(testkit.Rows("1"))
	// Test whether row id is incremental
	tk.MustExec("insert into temp_test(id) values(0)")
	tk.MustQuery("select _tidb_rowid from temp_test order by _tidb_rowid").Check(testkit.Rows("1", "2"))
	tk.MustExec("commit")

	// multi-value insert
	tk.MustExec("begin")
	tk.MustExec("insert into temp_test(id) values(0), (0)")
	tk.MustQuery("select _tidb_rowid from temp_test order by _tidb_rowid").Check(testkit.Rows("1", "2"))
	tk.MustExec("insert into temp_test(id) values(0), (0)")
	tk.MustQuery("select _tidb_rowid from temp_test order by _tidb_rowid").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustExec("commit")
}

func TestGlobalTempTableParallel(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("drop table if exists temp_test")
	tk.MustExec("create global temporary table temp_test(id int primary key auto_increment) on commit delete rows")
	defer tk.MustExec("drop table if exists temp_test")

	threads := 8
	loops := 1
	var wg util.WaitGroupWrapper

	insertFunc := func() {
		newTk := testkit.NewTestKit(t, store)
		newTk.MustExec("use test")
		newTk.MustExec("begin")
		for i := 0; i < loops; i++ {
			newTk.MustExec("insert temp_test value(0)")
			newTk.MustExec("insert temp_test value(0), (0)")
		}
		maxID := strconv.Itoa(loops * 3)
		newTk.MustQuery("select max(id) from temp_test").Check(testkit.Rows(maxID))
		newTk.MustExec("commit")
	}

	for i := 0; i < threads; i++ {
		wg.Run(insertFunc)
	}
	wg.Wait()
}

func TestIssue26762(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 date);")
	_, err := tk.Exec("insert into t1 values('2020-02-31');")
	require.EqualError(t, err, `[table:1292]Incorrect date value: '2020-02-31' for column 'c1' at row 1`)

	tk.MustExec("set @@sql_mode='ALLOW_INVALID_DATES';")
	tk.MustExec("insert into t1 values('2020-02-31');")
	tk.MustQuery("select * from t1").Check(testkit.Rows("2020-02-31"))

	tk.MustExec("set @@sql_mode='STRICT_TRANS_TABLES';")
	_, err = tk.Exec("insert into t1 values('2020-02-31');")
	require.EqualError(t, err, `[table:1292]Incorrect date value: '2020-02-31' for column 'c1' at row 1`)
}

func TestStringtoDecimal(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id decimal(10))")
	tk.MustGetErrCode("insert into t values('1sdf')", errno.ErrTruncatedWrongValueForField)
	tk.MustGetErrCode("insert into t values('1edf')", errno.ErrTruncatedWrongValueForField)
	tk.MustGetErrCode("insert into t values('12Ea')", errno.ErrTruncatedWrongValueForField)
	tk.MustGetErrCode("insert into t values('1E')", errno.ErrTruncatedWrongValueForField)
	tk.MustGetErrCode("insert into t values('1e')", errno.ErrTruncatedWrongValueForField)
	tk.MustGetErrCode("insert into t values('1.2A')", errno.ErrTruncatedWrongValueForField)
	tk.MustGetErrCode("insert into t values('1.2.3.4.5')", errno.ErrTruncatedWrongValueForField)
	tk.MustGetErrCode("insert into t values('1.2.')", errno.ErrTruncatedWrongValueForField)
	tk.MustGetErrCode("insert into t values('1,999.00')", errno.ErrTruncatedWrongValueForField)
	tk.MustExec("insert into t values('12e-3')")
	tk.MustQuery("show warnings;").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect DECIMAL value: '0.012'"))
	tk.MustQuery("select id from t").Check(testkit.Rows("0"))
	tk.MustExec("drop table if exists t")
}

func TestIssue17745(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("drop table if exists tt1")
	tk.MustExec("create table tt1 (c1 decimal(64))")
	tk.MustGetErrCode("insert into tt1 values(89000000000000000000000000000000000000000000000000000000000000000000000000000000000000000)", errno.ErrWarnDataOutOfRange)
	tk.MustGetErrCode("insert into tt1 values(89123456789012345678901234567890123456789012345678901234567890123456789012345678900000000)", errno.ErrWarnDataOutOfRange)
	tk.MustExec("insert ignore into tt1 values(89123456789012345678901234567890123456789012345678901234567890123456789012345678900000000)")
	tk.MustQuery("show warnings;").Check(testkit.Rows(`Warning 1690 DECIMAL value is out of range in '(64, 0)'`, `Warning 1292 Truncated incorrect DECIMAL value: '789012345678901234567890123456789012345678901234567890123456789012345678900000000'`))
	tk.MustQuery("select c1 from tt1").Check(testkit.Rows("9999999999999999999999999999999999999999999999999999999999999999"))
	tk.MustGetErrCode("update tt1 set c1 = 89123456789012345678901234567890123456789012345678901234567890123456789012345678900000000", errno.ErrWarnDataOutOfRange)
	tk.MustExec("drop table if exists tt1")
	tk.MustGetErrCode("insert into tt1 values(4556414e723532)", errno.ErrIllegalValueForType)
	tk.MustQuery("select 888888888888888888888888888888888888888888888888888888888888888888888888888888888888").Check(testkit.Rows("99999999999999999999999999999999999999999999999999999999999999999"))
	tk.MustQuery("show warnings;").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect DECIMAL value: '888888888888888888888888888888888888888888888888888888888888888888888888888888888'"))
}

// TestInsertIssue29892 test the double type with auto_increment problem, just leverage the serial test suite.
func TestInsertIssue29892(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	tk.MustExec("set global tidb_txn_mode='optimistic';")
	tk.MustExec("set global tidb_disable_txn_auto_retry=false;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a double auto_increment key, b int)")
	tk.MustExec("insert into t values (146576794, 1)")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec(`use test`)
	tk1.MustExec("begin")
	tk1.MustExec("insert into t(b) select 1")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`use test`)
	tk2.MustExec("begin")
	tk2.MustExec("insert into t values (146576795, 1)")
	tk2.MustExec("insert into t values (146576796, 1)")
	tk2.MustExec("commit")

	// since the origin auto-id (146576795) is cached in retryInfo, it will be fetched again to do the retry again,
	// which will duplicate with what has been inserted in tk1.
	_, err := tk1.Exec("commit")
	require.Error(t, err)
	require.Equal(t, true, strings.Contains(err.Error(), "Duplicate entry"))
}

// https://github.com/pingcap/tidb/issues/29483.
func TestReplaceAllocatingAutoID(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists replace_auto_id;")
	tk.MustExec("create database replace_auto_id;")
	tk.MustExec(`use replace_auto_id;`)

	tk.MustExec("SET sql_mode='NO_ENGINE_SUBSTITUTION';")
	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1 (a tinyint not null auto_increment primary key, b char(20));")
	tk.MustExec("INSERT INTO t1 VALUES (127,'maxvalue');")
	// Note that this error is different from MySQL's duplicated primary key error.
	tk.MustGetErrCode("REPLACE INTO t1 VALUES (0,'newmaxvalue');", errno.ErrAutoincReadFailed)
}

func TestInsertIntoSelectError(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1(a INT) ENGINE = InnoDB;")
	tk.MustExec("INSERT IGNORE into t1(SELECT SLEEP(NULL));")
	tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows("Warning 1210 Incorrect arguments to sleep"))
	tk.MustExec("INSERT IGNORE into t1(SELECT SLEEP(-1));")
	tk.MustQuery("SHOW WARNINGS;").Check(testkit.Rows("Warning 1210 Incorrect arguments to sleep"))
	tk.MustExec("INSERT IGNORE into t1(SELECT SLEEP(1));")
	tk.MustQuery("SELECT * FROM t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustExec("DROP TABLE t1;")
}

// https://github.com/pingcap/tidb/issues/32213.
func TestIssue32213(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	tk.MustExec("create table test.t1(c1 float)")
	tk.MustExec("insert into test.t1 values(999.99)")
	tk.MustQuery("select cast(test.t1.c1 as decimal(4, 1)) from test.t1").Check(testkit.Rows("999.9"))
	tk.MustQuery("select cast(test.t1.c1 as decimal(5, 1)) from test.t1").Check(testkit.Rows("1000.0"))

	tk.MustExec("drop table if exists test.t1")
	tk.MustExec("create table test.t1(c1 decimal(6, 4))")
	tk.MustExec("insert into test.t1 values(99.9999)")
	tk.MustQuery("select cast(test.t1.c1 as decimal(5, 3)) from test.t1").Check(testkit.Rows("99.999"))
	tk.MustQuery("select cast(test.t1.c1 as decimal(6, 3)) from test.t1").Check(testkit.Rows("100.000"))
}
