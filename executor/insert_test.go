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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testSuite8) TestInsertOnDuplicateKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	tk.MustExec(`insert into t1 values(1, 100);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t2 values(1, 200);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = a2;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 1"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = b2;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 200"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update a1 = a2;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(0))
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 200"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = 300;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 300"))

	tk.MustExec(`insert into t1 values(1, 1) on duplicate key update b1 = 400;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 400"))

	tk.MustExec(`insert into t1 select 1, 500 from t2 on duplicate key update b1 = 400;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(0))
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 400"))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	_, err := tk.Exec(`insert into t1 select * from t2 on duplicate key update c = t2.b;`)
	c.Assert(err.Error(), Equals, `[planner:1054]Unknown column 'c' in 'field list'`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	_, err = tk.Exec(`insert into t1 select * from t2 on duplicate key update a = b;`)
	c.Assert(err.Error(), Equals, `[planner:1052]Column 'b' in field list is ambiguous`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint primary key, b bigint);`)
	tk.MustExec(`create table t2(a bigint primary key, b bigint);`)
	_, err = tk.Exec(`insert into t1 select * from t2 on duplicate key update c = b;`)
	c.Assert(err.Error(), Equals, `[planner:1054]Unknown column 'c' in 'field list'`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	_, err = tk.Exec(`insert into t1 select * from t2 on duplicate key update a1 = values(b2);`)
	c.Assert(err.Error(), Equals, `[planner:1054]Unknown column 'b2' in 'field list'`)

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	tk.MustExec(`insert into t1 values(1, 100);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t2 values(1, 200);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 400"))
	tk.MustExec(`insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(0))
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 400"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(k1 bigint, k2 bigint, val bigint, primary key(k1, k2));`)
	tk.MustExec(`insert into t (val, k1, k2) values (3, 1, 2);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))
	tk.MustExec(`insert into t (val, k1, k2) select c, a, b from (select 1 as a, 2 as b, 4 as c) tmp on duplicate key update val = tmp.c;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 1  Duplicates: 1  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 4`))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(k1 double, k2 double, v double, primary key(k1, k2));`)
	tk.MustExec(`insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))
	tk.MustExec(`insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(0))
	tk.CheckLastMessage("Records: 1  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (2, 2, 1);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (3, 3, 1);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`create table t2(a int primary key, b int, unique(b));`)
	tk.MustExec(`insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(5))
	tk.CheckLastMessage("Records: 3  Duplicates: 2  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`3 1`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (2, 1, 2);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`insert into t1 values (3, 3, 1);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(1))
	tk.CheckLastMessage("")
	tk.MustExec(`create table t2(a int primary key, b int, unique(b));`)
	tk.MustExec(`insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(4))
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
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(2))
	tk.CheckLastMessage("Records: 4  Duplicates: 0  Warnings: 0")
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`1 1 1`, `3 2 2`))

	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(a int primary key, b int);`)
	tk.MustExec(`insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(5))
	tk.CheckLastMessage("Records: 5  Duplicates: 0  Warnings: 0")
	tk.MustExec(`insert into t1 values(4,14),(5,15),(6,16),(7,17),(8,18) on duplicate key update b=b+10`)
	c.Assert(tk.Se.AffectedRows(), Equals, uint64(7))
	tk.CheckLastMessage("Records: 5  Duplicates: 2  Warnings: 0")

	// reproduce insert on duplicate key update bug under new row format.
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1(c1 decimal(6,4), primary key(c1))`)
	tk.MustExec(`insert into t1 set c1 = 0.1`)
	tk.MustExec(`insert into t1 set c1 = 0.1 on duplicate key update c1 = 1`)
	tk.MustQuery(`select * from t1 use index(primary)`).Check(testkit.Rows(`1.0000`))
}

func (s *testSuite8) TestClusterIndexInsertOnDuplicateKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists cluster_index_duplicate_entry_error;")
	tk.MustExec("create database cluster_index_duplicate_entry_error;")
	tk.MustExec("use cluster_index_duplicate_entry_error;")
	tk.MustExec("set @@tidb_enable_clustered_index = 1")

	tk.MustExec("create table t(a char(20), b int, primary key(a));")
	tk.MustExec("insert into t values('aa', 1), ('bb', 1);")
	_, err := tk.Exec("insert into t values('aa', 2);")
	c.Assert(err, ErrorMatches, ".*Duplicate entry 'aa' for.*")

	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a char(20), b varchar(30), c varchar(10), primary key(a, b, c));")
	tk.MustExec("insert into t values ('a', 'b', 'c'), ('b', 'a', 'c');")
	_, err = tk.Exec("insert into t values ('a', 'b', 'c');")
	c.Assert(err, ErrorMatches, ".*Duplicate entry 'a-b-c' for.*")
}

func (s *testSuite10) TestPaddingCommonHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_clustered_index = 1")
	tk.MustExec(`create table t1(c1 decimal(6,4), primary key(c1))`)
	tk.MustExec(`insert into t1 set c1 = 0.1`)
	tk.MustExec(`insert into t1 set c1 = 0.1 on duplicate key update c1 = 1`)
	tk.MustQuery(`select * from t1`).Check(testkit.Rows(`1.0000`))
}

func (s *testSuite2) TestInsertReorgDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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

func (s *testSuite3) TestUpdateDuplicateKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table c(i int,j int,k int,primary key(i,j,k));`)
	tk.MustExec(`insert into c values(1,2,3);`)
	tk.MustExec(`insert into c values(1,2,4);`)
	_, err := tk.Exec(`update c set i=1,j=2,k=4 where i=1 and j=2 and k=3;`)
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1-2-4' for key 'PRIMARY'")
}

func (s *testSuite3) TestInsertWrongValueForField(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a bigint);`)
	_, err := tk.Exec(`insert into t1 values("asfasdfsajhlkhlksdaf");`)
	c.Assert(terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), IsTrue)

	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a varchar(10)) charset ascii;`)
	_, err = tk.Exec(`insert into t1 values('我');`)
	c.Assert(terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), IsTrue)

	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a char(10) charset utf8);`)
	tk.MustExec(`insert into t1 values('我');`)
	tk.MustExec(`alter table t1 add column b char(10) charset ascii as ((a));`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows(`我 `))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a year);`)
	_, err = tk.Exec(`insert into t values(2156);`)
	c.Assert(err.Error(), Equals, `[types:8033]invalid year`)
}

func (s *testSuite3) TestInsertDateTimeWithTimeZone(c *C) {
	tk := testkit.NewTestKit(c, s.store)

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
	tk.MustQuery(fmt.Sprintf("select count(*) from t where ts > '2020-10-22T12:00:00Z'")).Check(testkit.Rows("2"))

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

func (s *testSuite3) TestInsertZeroYear(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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

func (s *testSuiteP1) TestAllowInvalidDates(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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

func (s *testSuite3) TestInsertWithAutoidSchema(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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
			tk.Se.GetSessionVars().RetryInfo = retryInfo
			tk.MustExec(tt.insert[8:])
			tk.Se.GetSessionVars().RetryInfo = &variable.RetryInfo{}
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

func (s *testSuite3) TestPartitionInsertOnDuplicate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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

func (s *testSuite3) TestBit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a bit(3))`)
	_, err := tk.Exec("insert into t1 values(-1)")
	c.Assert(types.ErrDataTooLong.Equal(err), IsTrue)
	c.Assert(err.Error(), Matches, ".*Data too long for column 'a' at.*")
	_, err = tk.Exec("insert into t1 values(9)")
	c.Assert(err.Error(), Matches, ".*Data too long for column 'a' at.*")

	tk.MustExec(`create table t64 (a bit(64))`)
	tk.MustExec("insert into t64 values(-1)")
	tk.MustExec("insert into t64 values(18446744073709551615)")      // 2^64 - 1
	_, err = tk.Exec("insert into t64 values(18446744073709551616)") // z^64
	c.Assert(err.Error(), Matches, ".*Out of range value for column 'a' at.*")

}

func (s *testSuiteP1) TestAllocateContinuousRowID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int,b int, key I_a(a));`)
	wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tk := testkit.NewTestKitWithInit(c, s.store)
			for j := 0; j < 10; j++ {
				k := strconv.Itoa(idx*100 + j)
				sql := "insert into t1(a,b) values (" + k + ", 2)"
				for t := 0; t < 20; t++ {
					sql += ",(" + k + ",2)"
				}
				tk.MustExec(sql)
				q := "select _tidb_rowid from t1 where a=" + k
				rows := tk.MustQuery(q).Rows()
				c.Assert(len(rows), Equals, 21)
				last := 0
				for _, r := range rows {
					c.Assert(len(r), Equals, 1)
					v, err := strconv.Atoi(r[0].(string))
					c.Assert(err, Equals, nil)
					if last > 0 {
						c.Assert(last+1, Equals, v)
					}
					last = v
				}
			}
		}(i)
	}
	wg.Wait()
}

func (s *testSuite3) TestJiraIssue5366(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table bug (a varchar(100))`)
	tk.MustExec(` insert into bug select  ifnull(JSON_UNQUOTE(JSON_EXTRACT('[{"amount":2000,"feeAmount":0,"merchantNo":"20190430140319679394","shareBizCode":"20160311162_SECOND"}]', '$[0].merchantNo')),'') merchant_no union SELECT '20180531557' merchant_no;`)
	tk.MustQuery(`select * from bug`).Sort().Check(testkit.Rows("20180531557", "20190430140319679394"))
}

func (s *testSuite3) TestDMLCast(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b double)`)
	tk.MustExec(`insert into t values (ifnull('',0)+0, 0)`)
	tk.MustExec(`insert into t values (0, ifnull('',0)+0)`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows("0 0", "0 0"))
	_, err := tk.Exec(`insert into t values ('', 0)`)
	c.Assert(err, NotNil)
	_, err = tk.Exec(`insert into t values (0, '')`)
	c.Assert(err, NotNil)
	_, err = tk.Exec(`update t set a = ''`)
	c.Assert(err, NotNil)
	_, err = tk.Exec(`update t set b = ''`)
	c.Assert(err, NotNil)
	tk.MustExec("update t set a = ifnull('',0)+0")
	tk.MustExec("update t set b = ifnull('',0)+0")
	tk.MustExec("delete from t where a = ''")
	tk.MustQuery(`select * from t`).Check(testkit.Rows())
}

func (s *testSuite3) TestInsertFloatOverflow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec("create table t(col1 FLOAT, col2 FLOAT(10,2), col3 DOUBLE, col4 DOUBLE(10,2), col5 DECIMAL, col6 DECIMAL(10,2));")
	_, err := tk.Exec("insert into t values (-3.402823466E+68, -34028234.6611, -1.7976931348623157E+308, -17976921.34, -9999999999, -99999999.99);")
	c.Assert(err.Error(), Equals, "[types:1264]Out of range value for column 'col1' at row 1")
	_, err = tk.Exec("insert into t values (-34028234.6611, -3.402823466E+68, -1.7976931348623157E+308, -17976921.34, -9999999999, -99999999.99);")
	c.Assert(err.Error(), Equals, "[types:1264]Out of range value for column 'col2' at row 1")
}

// There is a potential issue in MySQL: when the value of auto_increment_offset is greater
// than that of auto_increment_increment, the value of auto_increment_offset is ignored
// (https://dev.mysql.com/doc/refman/8.0/en/replication-options-master.html#sysvar_auto_increment_increment),
// This issue is a flaw of the implementation of MySQL and it doesn't exist in TiDB.
func (s *testSuite3) TestAutoIDIncrementAndOffset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	// Test for offset is larger than increment.
	tk.Se.GetSessionVars().AutoIncrementIncrement = 5
	tk.Se.GetSessionVars().AutoIncrementOffset = 10
	tk.MustExec(`create table io (a int key auto_increment)`)
	tk.MustExec(`insert into io values (null),(null),(null)`)
	tk.MustQuery(`select * from io`).Check(testkit.Rows("10", "15", "20"))
	tk.MustExec(`drop table io`)

	// Test handle is PK.
	tk.MustExec(`create table io (a int key auto_increment)`)
	tk.Se.GetSessionVars().AutoIncrementOffset = 10
	tk.Se.GetSessionVars().AutoIncrementIncrement = 2
	tk.MustExec(`insert into io values (),(),()`)
	tk.MustQuery(`select * from io`).Check(testkit.Rows("10", "12", "14"))
	tk.MustExec(`delete from io`)

	// Test reset the increment.
	tk.Se.GetSessionVars().AutoIncrementIncrement = 5
	tk.MustExec(`insert into io values (),(),()`)
	tk.MustQuery(`select * from io`).Check(testkit.Rows("15", "20", "25"))
	tk.MustExec(`delete from io`)

	tk.Se.GetSessionVars().AutoIncrementIncrement = 10
	tk.MustExec(`insert into io values (),(),()`)
	tk.MustQuery(`select * from io`).Check(testkit.Rows("30", "40", "50"))
	tk.MustExec(`delete from io`)

	tk.Se.GetSessionVars().AutoIncrementIncrement = 5
	tk.MustExec(`insert into io values (),(),()`)
	tk.MustQuery(`select * from io`).Check(testkit.Rows("55", "60", "65"))
	tk.MustExec(`drop table io`)

	// Test handle is not PK.
	tk.Se.GetSessionVars().AutoIncrementIncrement = 2
	tk.Se.GetSessionVars().AutoIncrementOffset = 10
	tk.MustExec(`create table io (a int, b int auto_increment, key(b))`)
	tk.MustExec(`insert into io(b) values (null),(null),(null)`)
	// AutoID allocation will take increment and offset into consideration.
	tk.MustQuery(`select b from io`).Check(testkit.Rows("10", "12", "14"))
	// HandleID allocation will ignore the increment and offset.
	tk.MustQuery(`select _tidb_rowid from io`).Check(testkit.Rows("15", "16", "17"))
	tk.MustExec(`delete from io`)

	tk.Se.GetSessionVars().AutoIncrementIncrement = 10
	tk.MustExec(`insert into io(b) values (null),(null),(null)`)
	tk.MustQuery(`select b from io`).Check(testkit.Rows("20", "30", "40"))
	tk.MustQuery(`select _tidb_rowid from io`).Check(testkit.Rows("41", "42", "43"))

	// Test invalid value.
	tk.Se.GetSessionVars().AutoIncrementIncrement = -1
	tk.Se.GetSessionVars().AutoIncrementOffset = -2
	_, err := tk.Exec(`insert into io(b) values (null),(null),(null)`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[autoid:8060]Invalid auto_increment settings: auto_increment_increment: -1, auto_increment_offset: -2, both of them must be in range [1..65535]")
	tk.MustExec(`delete from io`)

	tk.Se.GetSessionVars().AutoIncrementIncrement = 65536
	tk.Se.GetSessionVars().AutoIncrementOffset = 65536
	_, err = tk.Exec(`insert into io(b) values (null),(null),(null)`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[autoid:8060]Invalid auto_increment settings: auto_increment_increment: 65536, auto_increment_offset: 65536, both of them must be in range [1..65535]")
}

var _ = SerialSuites(&testSuite9{&baseTestSuite{}})

type testSuite9 struct {
	*baseTestSuite
}

func (s *testSuite9) TestAutoRandomID(c *C) {
	testutil.ConfigTestUtils.SetupAutoRandomTestConfig()
	defer testutil.ConfigTestUtils.RestoreAutoRandomTestConfig()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key auto_random, name char(10))`)

	tk.MustExec(`insert into ar(id) values (null)`)
	rs := tk.MustQuery(`select id from ar`)
	c.Assert(len(rs.Rows()), Equals, 1)
	firstValue, err := strconv.Atoi(rs.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(firstValue, Greater, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(id) values (0)`)
	rs = tk.MustQuery(`select id from ar`)
	c.Assert(len(rs.Rows()), Equals, 1)
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(firstValue, Greater, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(name) values ('a')`)
	rs = tk.MustQuery(`select id from ar`)
	c.Assert(len(rs.Rows()), Equals, 1)
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(firstValue, Greater, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))

	tk.MustExec(`drop table ar`)
	tk.MustExec(`create table ar (id bigint key auto_random(15), name char(10))`)
	overflowVal := 1 << (64 - 5)
	errMsg := fmt.Sprintf(autoid.AutoRandomRebaseOverflow, overflowVal, 1<<(64-16)-1)
	_, err = tk.Exec(fmt.Sprintf("alter table ar auto_random_base = %d", overflowVal))
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), errMsg), IsTrue)
}

func (s *testSuite9) TestMultiAutoRandomID(c *C) {
	testutil.ConfigTestUtils.SetupAutoRandomTestConfig()
	defer testutil.ConfigTestUtils.RestoreAutoRandomTestConfig()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key auto_random, name char(10))`)

	tk.MustExec(`insert into ar(id) values (null),(null),(null)`)
	rs := tk.MustQuery(`select id from ar order by id`)
	c.Assert(len(rs.Rows()), Equals, 3)
	firstValue, err := strconv.Atoi(rs.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(firstValue, Greater, 0)
	c.Assert(rs.Rows()[1][0].(string), Equals, fmt.Sprintf("%d", firstValue+1))
	c.Assert(rs.Rows()[2][0].(string), Equals, fmt.Sprintf("%d", firstValue+2))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(id) values (0),(0),(0)`)
	rs = tk.MustQuery(`select id from ar order by id`)
	c.Assert(len(rs.Rows()), Equals, 3)
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(firstValue, Greater, 0)
	c.Assert(rs.Rows()[1][0].(string), Equals, fmt.Sprintf("%d", firstValue+1))
	c.Assert(rs.Rows()[2][0].(string), Equals, fmt.Sprintf("%d", firstValue+2))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(name) values ('a'),('a'),('a')`)
	rs = tk.MustQuery(`select id from ar order by id`)
	c.Assert(len(rs.Rows()), Equals, 3)
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(firstValue, Greater, 0)
	c.Assert(rs.Rows()[1][0].(string), Equals, fmt.Sprintf("%d", firstValue+1))
	c.Assert(rs.Rows()[2][0].(string), Equals, fmt.Sprintf("%d", firstValue+2))
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))

	tk.MustExec(`drop table ar`)
}

func (s *testSuite9) TestAutoRandomIDAllowZero(c *C) {
	testutil.ConfigTestUtils.SetupAutoRandomTestConfig()
	defer testutil.ConfigTestUtils.RestoreAutoRandomTestConfig()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key auto_random, name char(10))`)

	rs := tk.MustQuery(`select @@session.sql_mode`)
	sqlMode := rs.Rows()[0][0].(string)
	tk.MustExec(fmt.Sprintf(`set session sql_mode="%s,%s"`, sqlMode, "NO_AUTO_VALUE_ON_ZERO"))

	tk.MustExec(`insert into ar(id) values (0)`)
	rs = tk.MustQuery(`select id from ar`)
	c.Assert(len(rs.Rows()), Equals, 1)
	firstValue, err := strconv.Atoi(rs.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(firstValue, Equals, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))
	tk.MustExec(`delete from ar`)

	tk.MustExec(`insert into ar(id) values (null)`)
	rs = tk.MustQuery(`select id from ar`)
	c.Assert(len(rs.Rows()), Equals, 1)
	firstValue, err = strconv.Atoi(rs.Rows()[0][0].(string))
	c.Assert(err, IsNil)
	c.Assert(firstValue, Greater, 0)
	tk.MustQuery(`select last_insert_id()`).Check(testkit.Rows(fmt.Sprintf("%d", firstValue)))

	tk.MustExec(`drop table ar`)
}

func (s *testSuite9) TestAutoRandomIDExplicit(c *C) {
	testutil.ConfigTestUtils.SetupAutoRandomTestConfig()
	defer testutil.ConfigTestUtils.RestoreAutoRandomTestConfig()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@allow_auto_random_explicit_insert = true")

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists ar`)
	tk.MustExec(`create table ar (id bigint key auto_random, name char(10))`)

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

func (s *testSuite9) TestInsertErrorMsg(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (a int primary key, b datetime, d date)`)
	_, err := tk.Exec(`insert into t values (1, '2019-02-11 30:00:00', '2019-01-31')`)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "Incorrect datetime value: '2019-02-11 30:00:00' for column 'b' at row 1"), IsTrue, Commentf("%v", err))
}

func (s *testSuite9) TestIssue16366(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(c numeric primary key);`)
	tk.MustExec("insert ignore into t values(null);")
	_, err := tk.Exec(`insert into t values(0);`)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "Duplicate entry '0' for key 'PRIMARY'"), IsTrue, Commentf("%v", err))
}

var _ = SerialSuites(&testSuite10{&baseTestSuite{}})

type testSuite10 struct {
	*baseTestSuite
}

func (s *testSuite10) TestClusterPrimaryTablePlainInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`set @@tidb_enable_clustered_index=true`)

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

func (s *testSuite10) TestClusterPrimaryTableInsertIgnore(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`set @@tidb_enable_clustered_index=true`)

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

func (s *testSuite10) TestClusterPrimaryTableInsertDuplicate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`set @@tidb_enable_clustered_index=true`)

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

func (s *testSuite10) TestClusterPrimaryKeyForIndexScan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`set @@tidb_enable_clustered_index=true`)

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
	c.Assert(cnt, Equals, 15)
}

func (s *testSuite10) TestInsertRuntimeStat(c *C) {
	stats := &executor.InsertRuntimeStat{
		BasicRuntimeStats:    &execdetails.BasicRuntimeStats{},
		SnapshotRuntimeStats: nil,
		CheckInsertTime:      2 * time.Second,
		Prefetch:             1 * time.Second,
	}
	stats.BasicRuntimeStats.Record(5*time.Second, 1)
	c.Assert(stats.String(), Equals, "prepare:3s, check_insert:{total_time:2s, mem_insert_time:1s, prefetch:1s}")
	c.Assert(stats.String(), Equals, stats.Clone().String())
	stats.Merge(stats.Clone())
	c.Assert(stats.String(), Equals, "prepare:6s, check_insert:{total_time:4s, mem_insert_time:2s, prefetch:2s}")
}

func (s *testSerialSuite) TestDuplicateEntryMessage(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	for _, enable := range []int{0, 1} {
		tk.MustExec(fmt.Sprintf("set session tidb_enable_clustered_index=%d;", enable))
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
	}
}

func (s *testSerialSuite) TestIssue20768(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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
