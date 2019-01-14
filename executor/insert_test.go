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

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestInsertOnDuplicateKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a1 bigint primary key, b1 bigint);`)
	tk.MustExec(`create table t2(a2 bigint primary key, b2 bigint);`)
	tk.MustExec(`insert into t1 values(1, 100);`)
	tk.MustExec(`insert into t2 values(1, 200);`)

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = a2;`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 1"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = b2;`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 200"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update a1 = a2;`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 200"))

	tk.MustExec(`insert into t1 select a2, b2 from t2 on duplicate key update b1 = 300;`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 300"))

	tk.MustExec(`insert into t1 values(1, 1) on duplicate key update b1 = 400;`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows("1 400"))

	tk.MustExec(`insert into t1 select 1, 500 from t2 on duplicate key update b1 = 400;`)
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
	tk.MustExec(`insert into t2 values(1, 200);`)
	tk.MustExec(`insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;`)
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 400"))
	tk.MustExec(`insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;`)
	tk.MustQuery(`select * from t1`).Check(testkit.Rows("1 400"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(k1 bigint, k2 bigint, val bigint, primary key(k1, k2));`)
	tk.MustExec(`insert into t (val, k1, k2) values (3, 1, 2);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))
	tk.MustExec(`insert into t (val, k1, k2) select c, a, b from (select 1 as a, 2 as b, 4 as c) tmp on duplicate key update val = tmp.c;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 4`))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(k1 double, k2 double, v double, primary key(k1, k2));`)
	tk.MustExec(`insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))
	tk.MustExec(`insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 2 3`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1);`)
	tk.MustExec(`insert into t1 values (2, 2, 1);`)
	tk.MustExec(`insert into t1 values (3, 3, 1);`)
	tk.MustExec(`create table t2(a int primary key, b int, unique(b));`)
	tk.MustExec(`insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;`)
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`3 1`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1);`)
	tk.MustExec(`insert into t1 values (2, 1, 2);`)
	tk.MustExec(`insert into t1 values (3, 3, 1);`)
	tk.MustExec(`create table t2(a int primary key, b int, unique(b));`)
	tk.MustExec(`insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;`)
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`1 2`, `3 1`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(id int, a int, b int, c int);`)
	tk.MustExec(`insert into t1 values (1, 1, 1, 1);`)
	tk.MustExec(`insert into t1 values (2, 2, 1, 2);`)
	tk.MustExec(`insert into t1 values (3, 3, 2, 2);`)
	tk.MustExec(`insert into t1 values (4, 4, 2, 2);`)
	tk.MustExec(`create table t2(a int primary key, b int, c int, unique(b), unique(c));`)
	tk.MustExec(`insert into t2 select a, b, c from t1 order by id on duplicate key update b=t2.b, c=t2.c;`)
	tk.MustQuery(`select * from t2 order by a;`).Check(testkit.Rows(`1 1 1`, `3 2 2`))
}

func (s *testSuite) TestUpdateDuplicateKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table c(i int,j int,k int,primary key(i,j,k));`)
	tk.MustExec(`insert into c values(1,2,3);`)
	tk.MustExec(`insert into c values(1,2,4);`)
	_, err := tk.Exec(`update c set i=1,j=2,k=4 where i=1 and j=2 and k=3;`)
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1-2-4' for key 'PRIMARY'")
}

func (s *testSuite) TestInsertWrongValueForField(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a bigint);`)
	_, err := tk.Exec(`insert into t1 values("asfasdfsajhlkhlksdaf");`)
	c.Assert(terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), IsTrue)
}

func (s *testSuite) TestInsertDateTimeWithTimeZone(c *C) {
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
}

func (s *testSuite) TestInsertZeroYear(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t1(a year(4));`)
	tk.MustExec(`insert into t1 values(0000),(00),("0000"),("00");`)
	tk.MustQuery(`select * from t1;`).Check(testkit.Rows(
		`0`,
		`0`,
		`2000`,
		`2000`,
	))
}

func (s *testSuite) TestAllowInvalidDates(c *C) {
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
