// Copyright 2020 PingCAP, Inc.
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
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func checkApplyPlan(t *testing.T, tk *testkit.TestKit, sql string, parallel int) {
	results := tk.MustQuery("explain analyze " + sql)
	containApply := false
	for _, row := range results.Rows() {
		line := fmt.Sprintf("%v", row)
		if strings.Contains(line, "Apply") {
			if parallel > 0 { // check the concurrency if parallel is larger than 0
				str := "Concurrency:"
				if parallel > 1 {
					str += fmt.Sprintf("%v", parallel)
				}
				require.Contains(t, line, str)
			}
			containApply = true
			break
		}
	}
	require.True(t, containApply)
}

func TestParallelApplyPlan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (null, null)")

	q1 := "select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a)"
	checkApplyPlan(t, tk, q1, 0)
	tk.MustQuery(q1).Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.MustExec("set tidb_enable_parallel_apply=true")
	checkApplyPlan(t, tk, q1, 1)
	tk.MustQuery(q1).Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))

	q2 := "select * from t t0 where t0.b <= (select max(t1.b) from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a and t0.a > t2.a));"
	checkApplyPlan(t, tk, q2, 1) // only the outside apply can be parallel
	tk.MustQuery(q2).Sort().Check(testkit.Rows("1 1", "2 2", "3 3", "4 4", "5 5", "6 6", "7 7", "8 8", "9 9"))
	q3 := "select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a) order by t1.a"
	checkApplyPlan(t, tk, q3, 0)
	tk.MustExec("alter table t add index idx(a)")
	checkApplyPlan(t, tk, q3, 1)
	tk.MustQuery(q3).Sort().Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Parallel Apply rejects the possible order properties of its outer child currently"))
}

func TestApplyColumnType(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")

	// int
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("insert into t values(1,1), (2,2), (5,5), (2, 4), (5, 2), (9, 4)")
	tk.MustExec("insert into t1 values(2, 3), (4, 9), (10, 4), (1, 10)")
	sql := "select * from t where t.b > (select min(t1.b) from t1 where t1.a > t.a)"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("5 5"))

	// varchar
	tk.MustExec("drop table t, t1")
	tk.MustExec("create table t1(a varchar(255), b varchar(255))")
	tk.MustExec("create table t2(a varchar(255))")
	tk.MustExec(`insert into t1 values("aa", "bb"), ("aa", "tikv"), ("bb", "cc"), ("bb", "ee")`)
	tk.MustExec(`insert into t2 values("kk"), ("aa"), ("dd"), ("bb")`)
	sql = "select (select min(t2.a) from t2 where t2.a > t1.a) from t1"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("bb", "bb", "dd", "dd"))

	// bit
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a bit(10), b int)")
	tk.MustExec("create table t2(a bit(10), b int)")
	tk.MustExec(`insert into t1 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)`)
	tk.MustExec(`insert into t2 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)`)
	sql = "select b from t1 where t1.b > (select min(t2.b) from t2 where t2.a < t1.a)"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("2", "2", "3", "3", "4", "4"))

	// char
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a char(25), b int)")
	tk.MustExec("create table t2(a char(10), b int)")
	tk.MustExec(`insert into t1 values("abc", 1), ("abc", "5"), ("fff", 4), ("fff", 9), ("tidb", 6), ("tidb", 5)`)
	tk.MustExec(`insert into t2 values()`)
	sql = "select t1.b from t1 where t1.b > (select max(t2.b) from t2 where t2.a > t1.a)"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows())

	// double
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a int, b double)")
	tk.MustExec("create table t2(a int, b double)")
	tk.MustExec("insert into t1 values(1, 2.12), (1, 1.11), (2, 3), (2, 4.56), (5, 55), (5, -4)")
	tk.MustExec("insert into t2 values(1, 3.22), (3, 4.5), (5, 2.3), (4, 5.55)")
	sql = "select * from t1 where t1.a < (select avg(t2.a) from t2 where t2.b > t1.b)"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 1.11", "1 2.12", "2 3", "2 4.56"))

	// date
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a date, b int, c int)")
	tk.MustExec("create table t2(a date, b int)")
	tk.MustExec(`insert into t1 values("2020-01-01", 3, 4), ("2020-01-01", 4, 5), ("2020-01-01", 4, 3), ("2020-02-01", 7, 7), ("2020-02-01", 6, 6)`)
	tk.MustExec(`insert into t2 values("2020-01-02", 4), ("2020-02-02", 8), ("2020-02-02", 7)`)
	sql = "select * from t1 where t1.b >= (select min(t2.b) from t2 where t2.a > t1.a) and t1.c >= (select min(t2.b) from t2 where t2.a > t1.a)"
	tk.MustQuery(sql).Sort().Check(testkit.Rows("2020-01-01 4 5", "2020-02-01 7 7"))

	// datetime
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a datetime, b int)")
	tk.MustExec("create table t2(a datetime, b int)")
	tk.MustExec(`insert into t1 values("2020-01-01 00:00:00", 1), ("2020-01-01 00:00:00", 2), ("2020-06-06 00:00:00", 3), ("2020-06-06 00:00:00", 4), ("2020-09-08 00:00:00", 4)`)
	tk.MustExec(`insert into t2 values("2020-01-01 00:00:00", 1), ("2020-01-01 00:00:01", 2), ("2020-08-20 00:00:00", 4)`)
	sql = "select b from t1 where t1.b >= (select max(t2.b) from t2 where t2.a > t1.a)"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("4"))

	// timestamp
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a timestamp, b int)")
	tk.MustExec("create table t2(a timestamp, b int)")
	tk.MustExec(`insert into t1 values("2020-01-01 00:00:00", 1), ("2020-01-01 00:00:00", 2), ("2020-06-06 00:00:00", 3), ("2020-06-06 00:00:00", 4), ("2020-09-08 00:00:00", 4)`)
	tk.MustExec(`insert into t2 values("2020-01-01 00:00:00", 1), ("2020-01-01 00:00:01", 2), ("2020-08-20 00:00:00", 4)`)
	sql = "select b from t1 where t1.b >= (select max(t2.b) from t2 where t2.a > t1.a)"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("4"))
}

func TestApplyMultiColumnType(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")

	// int & int
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values (1, 1), (1, 1), (2, 2), (2, 3), (2, 3), (1, 1), (1, 1), (2, 2), (2, 3), (2, 3)")
	tk.MustExec("insert into t2 values (2, 2), (3,3), (-1, 1), (5, 4), (2, 2), (3,3), (-1, 1), (5, 4)")
	sql := "select (select count(*) from t2 where t2.a > t1.a and t2.b > t1.a) from t1"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("4", "4", "4", "4", "4", "4", "6", "6", "6", "6"))

	// int & char
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a int, b char(20))")
	tk.MustExec("create table t2(a int, b char(20))")
	tk.MustExec(`insert into t1 values (1, "a"), (2, "b"), (3, "c"), (1, "a"), (2, "b"), (3, "c")`)
	tk.MustExec(`insert into t2 values (1, "a"), (2, "b"), (3, "c"), (1, "a"), (2, "b"), (3, "c")`)
	sql = "select (select sum(t2.a) from t2 where t2.a > t1.a or t2.b < t1.b) from t1"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("10", "10", "6", "6", "8", "8"))

	// int & bit
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a int, b bit(10), c int)")
	tk.MustExec("create table t2(a int, b int, c int)")
	tk.MustExec(`insert into t1 values (1, '1', 1), (2, '2', 4), (3, '3', 6), (4, '4', 8), (1, '1', 1), (2, '2', 4), (3, '3', 6), (4, '4', 8)`)
	tk.MustExec(`insert into t2 values (1, 1111, 11), (2, 2222, 22), (1, 1111, 11), (2, 2222, 22)`)
	sql = "select a, c from t1 where (select max(t2.c) from t2 where t2.a > t1.a and t2.b > t1.b) > 4"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 1", "1 1"))

	// char & char
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a char(20), b varchar(255))")
	tk.MustExec("create table t2(a char(20), b varchar(255))")
	tk.MustExec(`insert into t1 values ('7', '7'), ('8', '8'), ('9', '9'), ('7', '7'), ('8', '8'), ('9', '9')`)
	tk.MustExec(`insert into t2 values ('7', '7'), ('8', '8'), ('9', '9'), ('7', '7'), ('8', '8'), ('9', '9')`)
	sql = "select count(*) from t1 where (select sum(t2.a) from t2 where t2.a >= t1.a and t2.b >= t1.b) > 4"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("6"))

	// enum & char
	tk.MustExec("drop table t1, t2")
	tk.MustExec(`create table t1(a varchar(20), b enum("a", "b", "c", "d", "e","f"))`)
	tk.MustExec("create table t2(a varchar(20), b int)")
	tk.MustExec(`insert into t1 values ('1', 'a'), ('2', 'b'), ('3', 'c'), ('1', 'a'), ('2', 'b'), ('3', 'c')`)
	tk.MustExec(`insert into t2 values ('1', 100), ('2', 200), ('3', 300), ('4', 400), ('1', 100), ('2', 200), ('3', 300), ('4', 400)`)
	sql = "select * from t1 where (select sum(t2.b) from t2 where t2.a > t1.a and t2.b * 2 > t1.b) > 0"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 a", "1 a", "2 b", "2 b", "3 c", "3 c"))

	// char & bit
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a varchar(20), b bit(10))")
	tk.MustExec("create table t2(a varchar(20), b int)")
	tk.MustExec("insert into t1 values ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4'), ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4')")
	tk.MustExec("insert into t2 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)")
	sql = "select a from t1 where (select sum(t2.b) from t2 where t2.a > t1.a and t2.b < t1.b) > 4"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1", "1", "2", "2", "3", "3"))

	// int & double
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1 (a int, b double)")
	tk.MustExec("create table t2 (a int, b double)")
	tk.MustExec("insert into t1 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)")
	tk.MustExec("insert into t2 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)")
	sql = "select * from t1 where (select min(t2.a) from t2 where t2.a < t1.a and t2.a > 1 and t2.b < t1.b) > 0"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("3 3.3", "3 3.3", "4 4.4", "4 4.4"))

	// int & datetime
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a int, b datetime)")
	tk.MustExec("create table t2(a int, b datetime)")
	tk.MustExec(`insert into t1 values (1, "2020-01-01"), (2, "2020-02-02"), (3, "2020-03-03"), (1, "2020-01-01"), (2, "2020-02-02"), (3, "2020-03-03")`)
	tk.MustExec(`insert into t2 values (1, "2020-01-01"), (2, "2020-02-02"), (3, "2020-03-03"), (1, "2020-01-01"), (2, "2020-02-02"), (3, "2020-03-03")`)
	sql = `select * from t1 where (select count(*) from t2 where t2.a >= t1.a and t2.b between t1.b and "2020-09-07 00:00:00") > 1`
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 2020-01-01 00:00:00", "1 2020-01-01 00:00:00", "2 2020-02-02 00:00:00", "2 2020-02-02 00:00:00", "3 2020-03-03 00:00:00", "3 2020-03-03 00:00:00"))

	// int & int & char
	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a int, b int, c varchar(20))")
	tk.MustExec("create table t2(a int, b int, c varchar(20))")
	tk.MustExec("insert into t1 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3')")
	tk.MustExec("insert into t2 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3')")
	sql = "select (select min(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b and t2.c > t1.c) from t1"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("2", "2", "3", "3", "<nil>", "<nil>"))
}

func TestSetTiDBEnableParallelApply(t *testing.T) {
	// validate the tidb_enable_parallel_apply's value
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=0")
	tk.MustQuery("select @@tidb_enable_parallel_apply").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_enable_parallel_apply=1")
	tk.MustQuery("select @@tidb_enable_parallel_apply").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_enable_parallel_apply=on")
	tk.MustQuery("select @@tidb_enable_parallel_apply").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_enable_parallel_apply=off")
	tk.MustQuery("select @@tidb_enable_parallel_apply").Check(testkit.Rows("0"))
	require.Error(t, tk.ExecToErr("set tidb_enable_parallel_apply=-1"))
	require.Error(t, tk.ExecToErr("set tidb_enable_parallel_apply=2"))
	require.Error(t, tk.ExecToErr("set tidb_enable_parallel_apply=1000"))
	require.Error(t, tk.ExecToErr("set tidb_enable_parallel_apply='onnn'"))
}

func TestMultipleApply(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")

	// compare apply with constant values
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`create table t1(a varchar(20), b enum("a", "b", "c", "d", "e","f"))`)
	tk.MustExec("create table t2(a varchar(20), b int)")
	tk.MustExec(`insert into t1 values ("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("1", "a"), ("2", "b"), ("3", "c"), ("4", "d")`)
	tk.MustExec(`insert into t2 values ("1", 1), ("2", 2), ("3", 3), ("4", 4), ("1", 1), ("2", 2), ("3", 3), ("4", 4)`)
	sql := "select * from t1 where (select sum(t2.b) from t2 where t2.a > t1.a) >= (select sum(t2.b) from t2 where t2.b > t1.b)"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 a", "1 a", "2 b", "2 b", "3 c", "3 c"))

	// 2 apply operators in where conditions
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b double)")
	tk.MustExec("create table t2(a int, b double)")
	tk.MustExec("insert into t1 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)")
	tk.MustExec("insert into t2 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)")
	sql = "select * from t1 where (select min(t2.a) from t2 where t2.a < t1.a and t2.a > 1) * (select min(t2.a) from t2 where t2.b < t1.b) > 1"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("3 3.3", "3 3.3", "4 4.4", "4 4.4"))

	// 2 apply operators and compare it with constant values
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a varchar(20), b bit(10))")
	tk.MustExec("create table t2(a varchar(20), b int)")
	tk.MustExec("insert into t1 values ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4'), ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4')")
	tk.MustExec("insert into t2 values ('1', 1111), ('2', 2222), ('3', 3333), ('4', 4444), ('1', 1111), ('2', 2222), ('3', 3333), ('4', 4444)")
	sql = "select a from t1 where (select sum(t2.b) from t2 where t2.a > t1.a) > 4 and (select sum(t2.b) from t2 where t2.b > t1.b) > 4"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1", "1", "2", "2", "3", "3"))

	// multiple fields and where conditions
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c varchar(20))")
	tk.MustExec("create table t2(a int, b int, c varchar(20))")
	tk.MustExec("insert into t1 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')")
	tk.MustExec("insert into t2 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')")
	sql = "select (select min(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b), (select max(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b) from t1 where (select count(*) from t2 where t2.c > t1.c) > 3"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("2 4", "2 4", "3 4", "3 4"))
}

func TestApplyWithOtherOperators(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")

	// hash join
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)")
	sql := "select /*+ hash_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"))

	// merge join
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a double, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)")
	sql = "select /*+ merge_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"))

	// index merge join
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("create table t2(a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)")
	sql = "select /*+ inl_merge_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("0", "0", "2", "2", "4", "4"))
	sql = "select /*+ inl_merge_join(t2) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("0", "0", "2", "2", "4", "4"))

	// index hash join
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, index idx(a, b))")
	tk.MustExec("create table t2(a int, b int, index idx(a))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)")
	sql = "select /*+ inl_hash_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"))
	sql = "select /*+ inl_hash_join(t2) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"))

	// index join
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int , b int, unique index idx(a))")
	tk.MustExec("create table t2(a int, b int, unique index idx(a))")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3)")
	sql = "select /*+ inl_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("0", "1", "2"))
	sql = "select /*+ inl_join(t2) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("0", "1", "2"))

	// index merge
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idxa(a), unique index idxb(b))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (1, 5, 1), (2, 6, 2), (3, 7, 3), (4, 8, 4)")
	sql = "select /*+ use_index_merge(t) */ * from t where (a > 0 or b < 0) and (select count(*) from t t1 where t1.c > t.a) > 0"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 1 1", "1 5 1", "2 2 2", "2 6 2", "3 3 3", "3 7 3"))

	// aggregation
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4), (1, 1), (2, 2), (3, 3), (4, 4)")
	sql = "select /*+ stream_agg() */ a from t where (select count(*) from t1 where t1.b > t.a) > 1 group by a"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1"))
	sql = "select /*+ hash_agg() */ a from t where (select count(*) from t1 where t1.b > t.a) > 1 group by a"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1"))
}

func TestApplyWithOtherFeatures(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")

	// collation 1
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, b int)")
	tk.MustExec("create table t1(a varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, b int)")
	tk.MustExec("insert into t values ('a', 1), ('A', 2), ('a', 3), ('A', 4)")
	tk.MustExec("insert into t1 values ('a', 1), ('A', 2), ('a', 3), ('A', 4)")
	sql := "select (select min(t1.b) from t1 where t1.a >= t.a), (select sum(t1.b) from t1 where t1.a >= t.a) from t"
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 10", "1 10", "1 10", "1 10"))

	// collation 2
	sql = "select (select min(t1.b) from t1 where t1.a >= t.a and t1.b >= t.b), (select sum(t1.b) from t1 where t1.a >= t.a and t1.b >= t.b) from t"
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 10", "2 9", "3 7", "4 4"))
	collate.SetNewCollationEnabledForTest(false)
	defer collate.SetNewCollationEnabledForTest(true)

	// plan cache
	orgEnable := core.PreparedPlanCacheEnabled()
	core.SetPreparedPlanCache(true)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values (1, 1), (1, 5), (2, 3), (2, 4), (3, 3)")
	tk.MustExec("insert into t2 values (0, 1), (2, -1), (3, 2)")
	tk.MustExec(`prepare stmt from "select * from t1 where t1.b >= (select sum(t2.b) from t2 where t2.a > t1.a and t2.a > ?)"`)
	tk.MustExec("set @a=1")
	tk.MustQuery("execute stmt using @a").Sort().Check(testkit.Rows("1 1", "1 5", "2 3", "2 4"))
	tk.MustExec("set @a=2")
	tk.MustQuery("execute stmt using @a").Sort().Check(testkit.Rows("1 5", "2 3", "2 4"))
	tk.MustQuery(" select @@last_plan_from_cache").Check(testkit.Rows("0")) // sub-queries are not cacheable
	core.SetPreparedPlanCache(orgEnable)

	// cluster index
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t, t2")
	tk.MustExec("create table t(a int, b int, c int, primary key(a, b))")
	tk.MustExec("create table t2(a int, b int, c int, primary key(a, c))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)")
	tk.MustExec("insert into t2 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)")
	sql = "select * from t where (select min(t2.b) from t2 where t2.a > t.a) > 0"
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 1 1", "2 2 2", "3 3 3"))
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly

	// partitioning table
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int) partition by range(a) (partition p0 values less than(10), partition p1 values less than(20), partition p2 values less than(30), partition p3 values less than(40))")
	tk.MustExec("create table t2(a int, b int) partition by hash(a) partitions 4")
	tk.MustExec("insert into t1 values (5, 5), (15, 15), (25, 25), (35, 35)")
	tk.MustExec("insert into t2 values (5, 5), (15, 15), (25, 25), (35, 35)")
	sql = "select (select count(*) from t2 where t2.a > t1.b and t2.a=20), (select max(t2.b) from t2 where t2.a between t1.a and 20) from t1 where t1.a > 10"
	tk.MustQuery(sql).Sort().Check(testkit.Rows("0 15", "0 <nil>", "0 <nil>"))
}

func TestApplyInDML(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")

	// delete
	tk.MustExec("drop table if exists t, t2")
	tk.MustExec("create table t(a bigint, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4), (1, 1), (2, 2), (3, 3), (4, 4)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3), (4, 4), (1, 1), (2, 2), (3, 3), (4, 4)")
	tk.MustExec("delete from t where (select min(t2.a) * 2 from t2 where t2.a < t.a) > 1")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 1", "1 1"))

	// insert
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustExec("insert into t (select * from t where (select count(*) from t t1 where t1.b > t.a) > 2)")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 1 1", "1 1 1", "1 1 1", "1 1 1", "2 2 2", "2 2 2", "3 3 3", "3 3 3"))

	// update
	tk.MustExec("drop table if exists t, t2")
	tk.MustExec("create table t(a smallint, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)")
	tk.MustExec("update t set a = a + 1 where (select count(*) from t2 where t2.a <= t.a) in (1, 2)")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("2 1", "2 1", "2 2", "2 2", "3 3", "3 3"))

	// replace
	tk.MustExec("drop table if exists t, t2")
	tk.MustExec("create table t(a tinyint, b int, unique index idx(a))")
	tk.MustExec("create table t2(a tinyint, b int)")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)")
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)")
	tk.MustExec("replace into t (select pow(t2.a, 2), t2.b from t2 where (select min(t.a) from t where t.a > t2.a) between 1 and 5)")
	tk.MustQuery("select * from t").Sort().Check(testkit.Rows("1 1", "2 2", "3 3", "4 2", "9 3"))

	// Transaction
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values (1, 2), (1, 3)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (1, 4), (2, 3), (2, 5)")
	tk.MustExec("insert into t2 values (2, 3), (3, 4)")
	sql := "select * from t1 where t1.b > any (select t2.b from t2 where t2.b < t1.b)"
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1 4", "2 5"))
	tk.MustExec("delete from t1 where a = 1")
	tk.MustQuery(sql).Sort().Check(testkit.Rows("2 5"))
	tk.MustExec("commit")
	tk.MustQuery(sql).Sort().Check(testkit.Rows("2 5"))
}

func TestApplyConcurrency(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")

	// tidb_executor_concurrency
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	sql := "select * from t1 where t1.b > (select sum(t2.b) from t2 where t2.a > t1.a)"
	tk.MustExec("set tidb_executor_concurrency = 3")
	checkApplyPlan(t, tk, sql, 3)
	tk.MustExec("set tidb_executor_concurrency = 5")
	checkApplyPlan(t, tk, sql, 5)

	// concurrency
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	vals := ""
	n := 100
	for i := 1; i <= n; i++ {
		if i > 1 {
			vals += ","
		}
		vals = vals + fmt.Sprintf("(%v)", i)
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", vals))
	sql = "select sum(a) from t where t.a >= (select max(a) from t t1 where t1.a <= t.a)"
	for cc := 1; cc <= 10; cc += 3 {
		tk.MustExec(fmt.Sprintf("set tidb_executor_concurrency = %v", cc))
		tk.MustQuery(sql).Check(testkit.Rows(fmt.Sprintf("%v", (n*(n+1))/2)))
	}
}

func TestApplyCacheRatio(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	sql := "select * from t1 where (select min(t2.b) from t2 where t2.a> t1.a) > 10"
	tk.MustExec("insert into t2 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")

	checkRatio := func(ratio string) bool {
		rows := tk.MustQuery("explain analyze " + sql).Rows()
		for _, r := range rows {
			line := fmt.Sprintf("%v", r)
			if strings.Contains(line, "cacheHitRatio:"+ratio) {
				return true
			}
		}
		return false
	}
	// 10%
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (1, 1)")
	require.True(t, checkRatio("10.000%"))
	tk.MustExec("set tidb_mem_quota_apply_cache = 0")
	require.False(t, checkRatio(""))
	tk.MustExec("set tidb_mem_quota_apply_cache = 33554432")

	// 20%
	tk.MustExec("truncate t1")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (2, 2), (1, 1)")
	require.True(t, checkRatio("20.000%"))
	tk.MustExec("set tidb_mem_quota_apply_cache = 0")
	require.False(t, checkRatio(""))
	tk.MustExec("set tidb_mem_quota_apply_cache = 33554432")
	// 50%
	tk.MustExec("truncate t1")
	tk.MustExec("insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)")
	require.True(t, checkRatio("50.000%"))
	tk.MustExec("set tidb_mem_quota_apply_cache = 0")
	require.False(t, checkRatio(""))
}

func TestApplyGoroutinePanic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values (1, 1), (1, 1), (2, 2), (2, 3), (2, 3), (1, 1), (1, 1), (2, 2), (2, 3), (2, 3)")
	tk.MustExec("insert into t2 values (2, 2), (3,3), (-1, 1), (5, 4), (2, 2), (3,3), (-1, 1), (5, 4)")

	// no panic
	sql := "select (select count(*) from t2 where t2.a > t1.a and t2.b > t1.a) from t1"
	checkApplyPlan(t, tk, sql, 1)
	tk.MustQuery(sql).Sort().Check(testkit.Rows("4", "4", "4", "4", "4", "4", "6", "6", "6", "6"))

	// panic in a inner worker
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/parallelApplyInnerWorkerPanic", "panic"))
	err := tk.QueryToErr(sql)
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/parallelApplyInnerWorkerPanic"))

	for _, panicName := range []string{"parallelApplyInnerWorkerPanic", "parallelApplyOuterWorkerPanic", "parallelApplyGetCachePanic", "parallelApplySetCachePanic"} {
		panicPath := fmt.Sprintf("github.com/pingcap/tidb/executor/%v", panicName)
		require.NoError(t, failpoint.Enable(panicPath, "panic"))
		require.Error(t, tk.QueryToErr(sql))
		require.NoError(t, failpoint.Disable(panicPath))
	}
}

func TestIssue24930(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")
	tk.MustQuery(`select case when t1.a is null
    then (select t2.a from t2 where t2.a = t1.a limit 1) else t1.a end a
	from t1 where t1.a=1 order by a limit 1`).Check(testkit.Rows()) // can return an empty result instead of hanging forever
}
