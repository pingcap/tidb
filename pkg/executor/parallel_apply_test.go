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
	"github.com/pingcap/tidb/pkg/testkit"
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
	store := testkit.CreateMockStore(t)
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
	// With ordered parallel apply, ordering is now preserved via a reorder
	// buffer so we no longer reject the plan.  The Apply should show the
	// configured concurrency.
	checkApplyPlan(t, tk, q3, 1)
	tk.MustQuery(q3).Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func TestApplyColumnType(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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

func TestMultipleApply(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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

func TestApplyConcurrency(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/parallelApplyInnerWorkerPanic", "panic"))
	err := tk.QueryToErr(sql)
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/parallelApplyInnerWorkerPanic"))

	for _, panicName := range []string{"parallelApplyInnerWorkerPanic", "parallelApplyOuterWorkerPanic", "parallelApplyGetCachePanic", "parallelApplySetCachePanic"} {
		panicPath := fmt.Sprintf("github.com/pingcap/tidb/pkg/executor/%v", panicName)
		require.NoError(t, failpoint.Enable(panicPath, "panic"))
		require.Error(t, tk.QueryToErr(sql))
		require.NoError(t, failpoint.Disable(panicPath))
	}
}

func TestParallelApplyCorrectness(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (c1 bigint, c2 int, c3 int, c4 int, primary key(c1, c2), index (c3));")
	tk.MustExec("insert into t1 values(1, 1, 1, 1), (1, 2, 3, 3), (2, 1, 4, 4), (2, 2, 2, 2);")

	tk.MustExec("set tidb_enable_parallel_apply=true")
	sql := "select (select /*+ NO_DECORRELATE() */ sum(c4) from t1 where t1.c3 = alias.c3) from t1 alias where alias.c1 = 1;"
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1", "3"))

	tk.MustExec("set tidb_enable_parallel_apply=false")
	tk.MustQuery(sql).Sort().Check(testkit.Rows("1", "3"))
}

func TestOrderedParallelApply(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int, index idx_a(a))")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100)")
	tk.MustExec("insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)")

	// ----------------------------------------------------------------
	// 1. ORDER BY with scalar correlated subquery – verify row order
	//    is preserved (not just sorted after the fact).
	// ----------------------------------------------------------------
	q1 := "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a"
	// Serial: get baseline
	tk.MustExec("set tidb_enable_parallel_apply=false")
	serialResult := tk.MustQuery(q1)
	serialRows := serialResult.Rows()

	// Parallel ordered: should produce identical order.
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("set tidb_executor_concurrency=5")
	checkApplyPlan(t, tk, q1, 5)
	tk.MustQuery(q1).Check(testkit.RowsWithSep(" ", flattenRows(serialRows)...))

	// ----------------------------------------------------------------
	// 2. ORDER BY + LIMIT – should use Limit (not TopN) because the
	//    ordered parallel apply preserves outer order.
	// ----------------------------------------------------------------
	q2 := "select t1.a, t1.b from t1 where t1.b > (select min(t2.b) from t2 where t2.a >= t1.a) order by t1.a limit 5"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	serialResult2 := tk.MustQuery(q2)
	serialRows2 := serialResult2.Rows()

	tk.MustExec("set tidb_enable_parallel_apply=true")
	checkApplyPlan(t, tk, q2, 5)
	// The planner should use a streamable Limit (not TopN) because the
	// ordered parallel apply preserves the outer scan order.
	// Verify the outer plan uses a streamable Limit (not TopN).
	// TopN may legitimately appear inside the inner (Probe) side
	// (e.g. for min/max optimization), so only check operators at
	// or above the Apply level.
	planRows := tk.MustQuery("explain " + q2).Rows()
	hasLimit := false
	hasOuterTopN := false
	seenApply := false
	for _, row := range planRows {
		line := fmt.Sprintf("%v", row)
		if strings.Contains(line, "Apply") {
			seenApply = true
		}
		if !seenApply {
			// Operators above Apply — check for Limit vs TopN.
			if strings.Contains(line, "Limit") {
				hasLimit = true
			}
			if strings.Contains(line, "TopN") {
				hasOuterTopN = true
			}
		}
	}
	require.True(t, hasLimit, "plan should contain Limit above Apply for ORDER BY + LIMIT with ordered apply")
	require.False(t, hasOuterTopN, "plan should not contain TopN above Apply when ordered apply preserves order")
	tk.MustQuery(q2).Check(testkit.RowsWithSep(" ", flattenRows(serialRows2)...))

	// ----------------------------------------------------------------
	// 3. EXISTS semi-join with ORDER BY – common pattern from the
	//    correlate branch.
	// ----------------------------------------------------------------
	q3 := "select t1.a from t1 where exists (select 1 from t2 where t2.a = t1.a and t2.b > 3) order by t1.a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	serialResult3 := tk.MustQuery(q3)
	serialRows3 := serialResult3.Rows()

	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q3).Check(testkit.RowsWithSep(" ", flattenRows(serialRows3)...))

	// ----------------------------------------------------------------
	// 4. Varying concurrency levels – results must be identical.
	// ----------------------------------------------------------------
	q4 := "select t1.a, (select count(*) from t2 where t2.a > t1.a) from t1 order by t1.a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected := tk.MustQuery(q4).Rows()

	tk.MustExec("set tidb_enable_parallel_apply=true")
	for _, cc := range []int{2, 3, 5, 7, 10} {
		tk.MustExec(fmt.Sprintf("set tidb_executor_concurrency=%d", cc))
		tk.MustQuery(q4).Check(testkit.RowsWithSep(" ", flattenRows(expected)...))
	}

	// ----------------------------------------------------------------
	// 5. Ordered parallel apply with OFFSET.
	// ----------------------------------------------------------------
	q5 := "select t1.a, t1.b from t1 where t1.b > (select min(t2.b) from t2 where t2.a >= t1.a) order by t1.a limit 3 offset 2"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	serialResult5 := tk.MustQuery(q5)
	serialRows5 := serialResult5.Rows()

	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("set tidb_executor_concurrency=5")
	tk.MustQuery(q5).Check(testkit.RowsWithSep(" ", flattenRows(serialRows5)...))
}

func TestOrderedParallelApplyEdgeCases(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("set tidb_executor_concurrency=4")

	// ----------------------------------------------------------------
	// 1. Outer filter with unselected rows — exercises the
	//    processOneOuterRow !or.selected path.
	// ----------------------------------------------------------------
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int, index idx_a(a))")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("insert into t2 values (1,10),(2,20),(3,30)")
	// The WHERE t1.a > 2 acts as an outer filter so rows with a<=2 hit
	// the unselected path in processOneOuterRow.
	q1 := "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a > 2 order by t1.a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected1 := tk.MustQuery(q1).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	checkApplyPlan(t, tk, q1, 4)
	tk.MustQuery(q1).Check(testkit.RowsWithSep(" ", flattenRows(expected1)...))

	// ----------------------------------------------------------------
	// 2. NOT EXISTS (anti-semi-join) with ORDER BY — exercises
	//    OnMissMatch in the ordered path.
	// ----------------------------------------------------------------
	q2 := "select t1.a from t1 where not exists (select 1 from t2 where t2.a = t1.a) order by t1.a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected2 := tk.MustQuery(q2).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q2).Check(testkit.RowsWithSep(" ", flattenRows(expected2)...))

	// ----------------------------------------------------------------
	// 3. ORDER BY + LIMIT 1 — the primary use-case for eager flush.
	//    Verifies the reorder worker flushes partial output early.
	// ----------------------------------------------------------------
	q3 := "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a limit 1"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected3 := tk.MustQuery(q3).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q3).Check(testkit.RowsWithSep(" ", flattenRows(expected3)...))

	// ----------------------------------------------------------------
	// 4. Empty outer side — orderedResultCh closes immediately.
	// ----------------------------------------------------------------
	q4 := "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a > 999 order by t1.a"
	tk.MustQuery(q4).Check(testkit.Rows())

	// ----------------------------------------------------------------
	// 5. Single row outer — minimal ordered path exercise.
	// ----------------------------------------------------------------
	q5 := "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a = 3 order by t1.a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected5 := tk.MustQuery(q5).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q5).Check(testkit.RowsWithSep(" ", flattenRows(expected5)...))

	// ----------------------------------------------------------------
	// 6. Left outer semi-join with ORDER BY — exercises the e.outer
	//    path where OnMissMatch must emit the outer row.
	// ----------------------------------------------------------------
	tk.MustExec("drop table if exists t3, t4")
	tk.MustExec("create table t3 (a int, index idx_a(a))")
	tk.MustExec("create table t4 (a int)")
	tk.MustExec("insert into t3 values (1),(2),(3),(4),(5)")
	tk.MustExec("insert into t4 values (2),(4)")
	// The IN subquery uses left-outer-semi-join producing a boolean column.
	q6 := "select a, a in (select /*+ NO_DECORRELATE() */ a from t4 where t4.a = t3.a) from t3 order by a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected6 := tk.MustQuery(q6).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q6).Check(testkit.RowsWithSep(" ", flattenRows(expected6)...))

	// ----------------------------------------------------------------
	// 7. Large result set — exercises the appendRow→IsFull→flushOutput
	//    path where the output chunk fills to capacity and must be
	//    flushed mid-drain.
	// ----------------------------------------------------------------
	tk.MustExec("drop table if exists t5, t6")
	tk.MustExec("create table t5 (a int, index idx_a(a))")
	tk.MustExec("create table t6 (a int)")
	vals := ""
	for i := 1; i <= 2000; i++ {
		if i > 1 {
			vals += ","
		}
		vals += fmt.Sprintf("(%d)", i)
	}
	tk.MustExec("insert into t5 values " + vals)
	tk.MustExec("insert into t6 values (1),(2),(3)")
	q7 := "select t5.a, (select count(*) from t6 where t6.a <= t5.a) from t5 order by t5.a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected7 := tk.MustQuery(q7).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q7).Check(testkit.RowsWithSep(" ", flattenRows(expected7)...))
}

func TestOrderedParallelApplyLargeInner(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("set tidb_executor_concurrency=4")

	// ----------------------------------------------------------------
	// Each outer row joins with many inner rows, forcing the chunk to
	// fill to capacity in processOneOuterRow (chk.IsFull path) and
	// the reorder worker's appendRow→flushOutput path.
	// ----------------------------------------------------------------
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, index idx_a(a))")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("insert into t1 values (1),(2),(3)")
	// Insert enough rows to exceed chunk capacity for each outer row.
	vals := ""
	for i := 1; i <= 2000; i++ {
		if i > 1 {
			vals += ","
		}
		vals += fmt.Sprintf("(%d, %d)", i%3+1, i)
	}
	tk.MustExec("insert into t2 values " + vals)

	q := "select t1.a, (select /*+ NO_DECORRELATE() */ count(*) from t2 where t2.a = t1.a) from t1 order by t1.a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected := tk.MustQuery(q).Rows()

	tk.MustExec("set tidb_enable_parallel_apply=true")
	checkApplyPlan(t, tk, q, 4)
	tk.MustQuery(q).Check(testkit.RowsWithSep(" ", flattenRows(expected)...))

	// ----------------------------------------------------------------
	// Cartesian-style: every outer row pairs with every inner row to
	// produce a large result set (exercises multi-chunk flush in the
	// reorder worker).
	// ----------------------------------------------------------------
	tk.MustExec("drop table if exists t3, t4")
	tk.MustExec("create table t3 (a int, index idx_a(a))")
	tk.MustExec("create table t4 (b int)")
	tk.MustExec("insert into t3 values (1),(2),(3)")
	vals = ""
	for i := 1; i <= 500; i++ {
		if i > 1 {
			vals += ","
		}
		vals += fmt.Sprintf("(%d)", i)
	}
	tk.MustExec("insert into t4 values " + vals)

	q2 := "select t3.a, (select /*+ NO_DECORRELATE() */ sum(t4.b) from t4 where t4.b <= t3.a * 100) from t3 order by t3.a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected2 := tk.MustQuery(q2).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q2).Check(testkit.RowsWithSep(" ", flattenRows(expected2)...))
}

func TestOrderedParallelApplyLeftOuterSemiJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("set tidb_executor_concurrency=4")

	// ----------------------------------------------------------------
	// Left outer semi-join with outer filter: some outer rows are
	// not selected, exercising the e.outer && !or.selected path in
	// processOneOuterRow. Also exercises OnMissMatch in ordered mode.
	// ----------------------------------------------------------------
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int, index idx_a(a))")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50)")
	tk.MustExec("insert into t2 values (2,2),(4,4)")

	// IN subquery → left outer semi join; the NOT IN variant produces
	// anti-semi join with OnMissMatch for non-matching rows.
	q1 := "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 order by a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected1 := tk.MustQuery(q1).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q1).Check(testkit.RowsWithSep(" ", flattenRows(expected1)...))

	// NOT IN subquery — anti-semi-join path
	q2 := "select a from t1 where a not in (select /*+ NO_DECORRELATE() */ a from t2 where t2.b < t1.b) order by a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected2 := tk.MustQuery(q2).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q2).Check(testkit.RowsWithSep(" ", flattenRows(expected2)...))

	// Left outer join with ORDER BY — the subquery returns a value for
	// every outer row, exercising both matched and unmatched paths.
	q3 := "select t1.a, (select t2.b from t2 where t2.a = t1.a) from t1 order by t1.a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected3 := tk.MustQuery(q3).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q3).Check(testkit.RowsWithSep(" ", flattenRows(expected3)...))

	// ----------------------------------------------------------------
	// Left outer semi-join with outer-side WHERE conditions.
	// Note: the planner pushes outer-side filters below the Apply
	// into the outer child (e.g. IndexRangeScan), so the outerFilter
	// field on the Apply is empty and selected=true for all rows
	// reaching processOneOuterRow.  These tests verify correctness
	// of the left outer semi join with reduced outer row sets and
	// nullable columns.
	// ----------------------------------------------------------------
	tk.MustExec("insert into t1 values (null, null), (6, null)")
	q4 := "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 where t1.b is not null order by a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected4 := tk.MustQuery(q4).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q4).Check(testkit.RowsWithSep(" ", flattenRows(expected4)...))

	q5 := "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 where t1.a > 2 order by a"
	tk.MustExec("set tidb_enable_parallel_apply=false")
	expected5 := tk.MustQuery(q5).Rows()
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustQuery(q5).Check(testkit.RowsWithSep(" ", flattenRows(expected5)...))
}

func TestOrderedParallelApplyGoroutinePanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int, index idx_a(a))")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("insert into t2 values (1,10),(2,20),(3,30)")
	tk.MustExec("set tidb_enable_parallel_apply=true")
	tk.MustExec("set tidb_executor_concurrency=3")

	sql := "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a"
	// Verify baseline works.
	checkApplyPlan(t, tk, sql, 3)
	tk.MustQuery(sql).Check(testkit.Rows("1 10", "2 20", "3 30", "4 30", "5 30"))

	// Panic in ordered inner worker — error should propagate through
	// reorder worker to the consumer.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/parallelApplyInnerWorkerOrderedPanic", "panic"))
	require.Error(t, tk.QueryToErr(sql))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/parallelApplyInnerWorkerOrderedPanic"))

	// Panic in outer worker (shared with unordered, but verify it
	// works with the reorder worker present).
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/parallelApplyOuterWorkerPanic", "panic"))
	require.Error(t, tk.QueryToErr(sql))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/parallelApplyOuterWorkerPanic"))
}

// flattenRows converts [][]interface{} from MustQuery().Rows() into
// []string suitable for testkit.RowsWithSep(" ", ...).
func flattenRows(rows [][]any) []string {
	result := make([]string, 0, len(rows))
	for _, row := range rows {
		s := ""
		for i, col := range row {
			if i > 0 {
				s += " "
			}
			s += fmt.Sprintf("%v", col)
		}
		result = append(result, s)
	}
	return result
}
