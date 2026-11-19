// Copyright 2026 PingCAP, Inc.
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

//! Go `pkg/executor/parallel_apply_test.go` (`TestParallelApplyPlan`, `TestApplyColumnType`, `TestApplyMultiColumnType`, `TestMultipleApply`, `TestApplyWithOtherOperators`, `TestApplyConcurrency`, `TestApplyCacheRatio`, `TestApplyGoroutinePanic`, `TestParallelApplyCorrectness`, `TestParallelApplyCancelInflight`, `TestOrderedParallelApply`, `TestOrderedParallelApplyEdgeCases`, `TestOrderedParallelApplyLargeInner`, `TestOrderedParallelApplyLeftOuterSemiJoin`, `TestOrderedParallelApplyGoroutinePanic`, `TestOrderedParallelApplyKillSignal`, `TestOrderedParallelApplyNested`), on the mock store through the session, as Go's testkit
//! runs them.

#![allow(unused_mut, unused_variables, unused_imports)]

use crate::support::testkit::*;

/// Go `checkApplyPlan`: the plan has an Apply, running with `parallel`
/// workers when `parallel` is positive.
fn check_apply_plan(tk: &mut TestKit, sql: &str, parallel: i64) {
    let results = tk.must_query(&format!("explain analyze {sql}"));
    let mut contain_apply = false;
    for row in results.rows() {
        let line = go_v(row);
        if line.contains("Apply") {
            if parallel > 0 {
                let mut str = "Concurrency:".to_owned();
                if parallel > 1 {
                    str += &parallel.to_string();
                }
                assert!(line.contains(&str), "{line}");
            }
            contain_apply = true;
            break;
        }
    }
    assert!(contain_apply);
}

/// Go `TestParallelApplyPlan`.
#[test]
fn test_parallel_apply_plan() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (a int, b int)");
    tk.must_exec("insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (null, null)");
    let mut q1 =
        "select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a)".to_owned();
    check_apply_plan(&mut tk, &q1, 0);
    tk.must_query(&q1)
        .sort()
        .check(["1", "2", "3", "4", "5", "6", "7", "8", "9"]);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    check_apply_plan(&mut tk, &q1, 1);
    tk.must_query(&q1)
        .sort()
        .check(["1", "2", "3", "4", "5", "6", "7", "8", "9"]);
    let mut q2 = "select * from t t0 where t0.b <= (select max(t1.b) from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a and t0.a > t2.a));".to_owned();
    check_apply_plan(&mut tk, &q2, 1);
    tk.must_query(&q2).sort().check([
        "1 1", "2 2", "3 3", "4 4", "5 5", "6 6", "7 7", "8 8", "9 9",
    ]);
    let mut q3 = "select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a) order by t1.a".to_owned();
    check_apply_plan(&mut tk, &q3, 0);
    tk.must_exec("alter table t add index idx(a)");
    // With ordered parallel apply, ordering is now preserved via a reorder
    // buffer so we no longer reject the plan.  The Apply should show the
    // configured concurrency.
    check_apply_plan(&mut tk, &q3, 1);
    tk.must_query(&q3)
        .check(["1", "2", "3", "4", "5", "6", "7", "8", "9"]);
    tk.must_query("show warnings").check_empty();
}

/// Go `TestApplyColumnType`.
#[test]
fn test_apply_column_type() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    // int
    tk.must_exec("create table t(a int, b int)");
    tk.must_exec("create table t1(a int, b int)");
    tk.must_exec("insert into t values(1,1), (2,2), (5,5), (2, 4), (5, 2), (9, 4)");
    tk.must_exec("insert into t1 values(2, 3), (4, 9), (10, 4), (1, 10)");
    let mut sql =
        "select * from t where t.b > (select min(t1.b) from t1 where t1.a > t.a)".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check(["5 5"]);
    // varchar
    tk.must_exec("drop table t, t1");
    tk.must_exec("create table t1(a varchar(255), b varchar(255))");
    tk.must_exec("create table t2(a varchar(255))");
    tk.must_exec(
        r#"insert into t1 values("aa", "bb"), ("aa", "tikv"), ("bb", "cc"), ("bb", "ee")"#,
    );
    tk.must_exec(r#"insert into t2 values("kk"), ("aa"), ("dd"), ("bb")"#);
    sql = "select (select min(t2.a) from t2 where t2.a > t1.a) from t1".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check(["bb", "bb", "dd", "dd"]);
    // bit
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a bit(10), b int)");
    tk.must_exec("create table t2(a bit(10), b int)");
    tk.must_exec(r#"insert into t1 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)"#);
    tk.must_exec(r#"insert into t2 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)"#);
    sql = "select b from t1 where t1.b > (select min(t2.b) from t2 where t2.a < t1.a)".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["2", "2", "3", "3", "4", "4"]);
    // char
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a char(25), b int)");
    tk.must_exec("create table t2(a char(10), b int)");
    tk.must_exec(r#"insert into t1 values("abc", 1), ("abc", "5"), ("fff", 4), ("fff", 9), ("tidb", 6), ("tidb", 5)"#);
    tk.must_exec(r#"insert into t2 values()"#);
    sql =
        "select t1.b from t1 where t1.b > (select max(t2.b) from t2 where t2.a > t1.a)".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check_empty();
    // double
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a int, b double)");
    tk.must_exec("create table t2(a int, b double)");
    tk.must_exec("insert into t1 values(1, 2.12), (1, 1.11), (2, 3), (2, 4.56), (5, 55), (5, -4)");
    tk.must_exec("insert into t2 values(1, 3.22), (3, 4.5), (5, 2.3), (4, 5.55)");
    sql = "select * from t1 where t1.a < (select avg(t2.a) from t2 where t2.b > t1.b)".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["1 1.11", "1 2.12", "2 3", "2 4.56"]);
    // date
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a date, b int, c int)");
    tk.must_exec("create table t2(a date, b int)");
    tk.must_exec(r#"insert into t1 values("2020-01-01", 3, 4), ("2020-01-01", 4, 5), ("2020-01-01", 4, 3), ("2020-02-01", 7, 7), ("2020-02-01", 6, 6)"#);
    tk.must_exec(r#"insert into t2 values("2020-01-02", 4), ("2020-02-02", 8), ("2020-02-02", 7)"#);
    sql = "select * from t1 where t1.b >= (select min(t2.b) from t2 where t2.a > t1.a) and t1.c >= (select min(t2.b) from t2 where t2.a > t1.a)".to_owned();
    tk.must_query(&sql)
        .sort()
        .check(["2020-01-01 4 5", "2020-02-01 7 7"]);
    // datetime
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a datetime, b int)");
    tk.must_exec("create table t2(a datetime, b int)");
    tk.must_exec(r#"insert into t1 values("2020-01-01 00:00:00", 1), ("2020-01-01 00:00:00", 2), ("2020-06-06 00:00:00", 3), ("2020-06-06 00:00:00", 4), ("2020-09-08 00:00:00", 4)"#);
    tk.must_exec(r#"insert into t2 values("2020-01-01 00:00:00", 1), ("2020-01-01 00:00:01", 2), ("2020-08-20 00:00:00", 4)"#);
    sql = "select b from t1 where t1.b >= (select max(t2.b) from t2 where t2.a > t1.a)".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check(["4"]);
    // timestamp
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a timestamp, b int)");
    tk.must_exec("create table t2(a timestamp, b int)");
    tk.must_exec(r#"insert into t1 values("2020-01-01 00:00:00", 1), ("2020-01-01 00:00:00", 2), ("2020-06-06 00:00:00", 3), ("2020-06-06 00:00:00", 4), ("2020-09-08 00:00:00", 4)"#);
    tk.must_exec(r#"insert into t2 values("2020-01-01 00:00:00", 1), ("2020-01-01 00:00:01", 2), ("2020-08-20 00:00:00", 4)"#);
    sql = "select b from t1 where t1.b >= (select max(t2.b) from t2 where t2.a > t1.a)".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check(["4"]);
}

/// Go `TestApplyMultiColumnType`.
#[test]
fn test_apply_multi_column_type() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    // int & int
    tk.must_exec("create table t1(a int, b int)");
    tk.must_exec("create table t2(a int, b int)");
    tk.must_exec("insert into t1 values (1, 1), (1, 1), (2, 2), (2, 3), (2, 3), (1, 1), (1, 1), (2, 2), (2, 3), (2, 3)");
    tk.must_exec(
        "insert into t2 values (2, 2), (3,3), (-1, 1), (5, 4), (2, 2), (3,3), (-1, 1), (5, 4)",
    );
    let mut sql =
        "select (select count(*) from t2 where t2.a > t1.a and t2.b > t1.a) from t1".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["4", "4", "4", "4", "4", "4", "6", "6", "6", "6"]);
    // int & char
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a int, b char(20))");
    tk.must_exec("create table t2(a int, b char(20))");
    tk.must_exec(
        r#"insert into t1 values (1, "a"), (2, "b"), (3, "c"), (1, "a"), (2, "b"), (3, "c")"#,
    );
    tk.must_exec(
        r#"insert into t2 values (1, "a"), (2, "b"), (3, "c"), (1, "a"), (2, "b"), (3, "c")"#,
    );
    sql = "select (select sum(t2.a) from t2 where t2.a > t1.a or t2.b < t1.b) from t1".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["10", "10", "6", "6", "8", "8"]);
    // int & bit
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a int, b bit(10), c int)");
    tk.must_exec("create table t2(a int, b int, c int)");
    tk.must_exec(r#"insert into t1 values (1, '1', 1), (2, '2', 4), (3, '3', 6), (4, '4', 8), (1, '1', 1), (2, '2', 4), (3, '3', 6), (4, '4', 8)"#);
    tk.must_exec(
        r#"insert into t2 values (1, 1111, 11), (2, 2222, 22), (1, 1111, 11), (2, 2222, 22)"#,
    );
    sql = "select a, c from t1 where (select max(t2.c) from t2 where t2.a > t1.a and t2.b > t1.b) > 4".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check(["1 1", "1 1"]);
    // char & char
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a char(20), b varchar(255))");
    tk.must_exec("create table t2(a char(20), b varchar(255))");
    tk.must_exec(r#"insert into t1 values ('7', '7'), ('8', '8'), ('9', '9'), ('7', '7'), ('8', '8'), ('9', '9')"#);
    tk.must_exec(r#"insert into t2 values ('7', '7'), ('8', '8'), ('9', '9'), ('7', '7'), ('8', '8'), ('9', '9')"#);
    sql = "select count(*) from t1 where (select sum(t2.a) from t2 where t2.a >= t1.a and t2.b >= t1.b) > 4".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check(["6"]);
    // enum & char
    tk.must_exec("drop table t1, t2");
    tk.must_exec(r#"create table t1(a varchar(20), b enum("a", "b", "c", "d", "e","f"))"#);
    tk.must_exec("create table t2(a varchar(20), b int)");
    tk.must_exec(r#"insert into t1 values ('1', 'a'), ('2', 'b'), ('3', 'c'), ('1', 'a'), ('2', 'b'), ('3', 'c')"#);
    tk.must_exec(r#"insert into t2 values ('1', 100), ('2', 200), ('3', 300), ('4', 400), ('1', 100), ('2', 200), ('3', 300), ('4', 400)"#);
    sql = "select * from t1 where (select sum(t2.b) from t2 where t2.a > t1.a and t2.b * 2 > t1.b) > 0".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["1 a", "1 a", "2 b", "2 b", "3 c", "3 c"]);
    // char & bit
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a varchar(20), b bit(10))");
    tk.must_exec("create table t2(a varchar(20), b int)");
    tk.must_exec("insert into t1 values ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4'), ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4')");
    tk.must_exec("insert into t2 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)");
    sql = "select a from t1 where (select sum(t2.b) from t2 where t2.a > t1.a and t2.b < t1.b) > 4"
        .to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["1", "1", "2", "2", "3", "3"]);
    // int & double
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1 (a int, b double)");
    tk.must_exec("create table t2 (a int, b double)");
    tk.must_exec("insert into t1 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)");
    tk.must_exec("insert into t2 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)");
    sql = "select * from t1 where (select min(t2.a) from t2 where t2.a < t1.a and t2.a > 1 and t2.b < t1.b) > 0".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["3 3.3", "3 3.3", "4 4.4", "4 4.4"]);
    // int & datetime
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a int, b datetime)");
    tk.must_exec("create table t2(a int, b datetime)");
    tk.must_exec(r#"insert into t1 values (1, "2020-01-01"), (2, "2020-02-02"), (3, "2020-03-03"), (1, "2020-01-01"), (2, "2020-02-02"), (3, "2020-03-03")"#);
    tk.must_exec(r#"insert into t2 values (1, "2020-01-01"), (2, "2020-02-02"), (3, "2020-03-03"), (1, "2020-01-01"), (2, "2020-02-02"), (3, "2020-03-03")"#);
    sql = r#"select * from t1 where (select count(*) from t2 where t2.a >= t1.a and t2.b between t1.b and "2020-09-07 00:00:00") > 1"#.to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check([
        "1 2020-01-01 00:00:00",
        "1 2020-01-01 00:00:00",
        "2 2020-02-02 00:00:00",
        "2 2020-02-02 00:00:00",
        "3 2020-03-03 00:00:00",
        "3 2020-03-03 00:00:00",
    ]);
    // int & int & char
    tk.must_exec("drop table t1, t2");
    tk.must_exec("create table t1(a int, b int, c varchar(20))");
    tk.must_exec("create table t2(a int, b int, c varchar(20))");
    tk.must_exec("insert into t1 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3')");
    tk.must_exec("insert into t2 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3')");
    sql = "select (select min(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b and t2.c > t1.c) from t1".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["2", "2", "3", "3", "<nil>", "<nil>"]);
}

/// Go `TestMultipleApply`.
#[test]
fn test_multiple_apply() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    // compare apply with constant values
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec(r#"create table t1(a varchar(20), b enum("a", "b", "c", "d", "e","f"))"#);
    tk.must_exec("create table t2(a varchar(20), b int)");
    tk.must_exec(r#"insert into t1 values ("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("1", "a"), ("2", "b"), ("3", "c"), ("4", "d")"#);
    tk.must_exec(r#"insert into t2 values ("1", 1), ("2", 2), ("3", 3), ("4", 4), ("1", 1), ("2", 2), ("3", 3), ("4", 4)"#);
    let mut sql = "select * from t1 where (select sum(t2.b) from t2 where t2.a > t1.a) >= (select sum(t2.b) from t2 where t2.b > t1.b)".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["1 a", "1 a", "2 b", "2 b", "3 c", "3 c"]);
    // 2 apply operators in where conditions
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int, b double)");
    tk.must_exec("create table t2(a int, b double)");
    tk.must_exec("insert into t1 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)");
    tk.must_exec("insert into t2 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)");
    sql = "select * from t1 where (select min(t2.a) from t2 where t2.a < t1.a and t2.a > 1) * (select min(t2.a) from t2 where t2.b < t1.b) > 1".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["3 3.3", "3 3.3", "4 4.4", "4 4.4"]);
    // 2 apply operators and compare it with constant values
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a varchar(20), b bit(10))");
    tk.must_exec("create table t2(a varchar(20), b int)");
    tk.must_exec("insert into t1 values ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4'), ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4')");
    tk.must_exec("insert into t2 values ('1', 1111), ('2', 2222), ('3', 3333), ('4', 4444), ('1', 1111), ('2', 2222), ('3', 3333), ('4', 4444)");
    sql = "select a from t1 where (select sum(t2.b) from t2 where t2.a > t1.a) > 4 and (select sum(t2.b) from t2 where t2.b > t1.b) > 4".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["1", "1", "2", "2", "3", "3"]);
    // multiple fields and where conditions
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int, b int, c varchar(20))");
    tk.must_exec("create table t2(a int, b int, c varchar(20))");
    tk.must_exec("insert into t1 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')");
    tk.must_exec("insert into t2 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')");
    sql = "select (select min(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b), (select max(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b) from t1 where (select count(*) from t2 where t2.c > t1.c) > 3".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["2 4", "2 4", "3 4", "3 4"]);
}

/// Go `TestApplyWithOtherOperators`.
#[test]
fn test_apply_with_other_operators() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    // hash join
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int, b int)");
    tk.must_exec("create table t2(a int, b int)");
    tk.must_exec("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    tk.must_exec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    let mut sql = "select /*+ hash_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"]);
    // merge join
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a double, b int)");
    tk.must_exec("create table t2(a int, b int)");
    tk.must_exec("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    tk.must_exec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    sql = "select /*+ merge_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"]);
    // index merge join
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int primary key, b int)");
    tk.must_exec("create table t2(a int, b int, index idx(a))");
    tk.must_exec("insert into t1 values (1, 1), (2, 2), (3, 3)");
    tk.must_exec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    sql = "select /*+ inl_merge_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["0", "0", "2", "2", "4", "4"]);
    sql = "select /*+ inl_merge_join(t2) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["0", "0", "2", "2", "4", "4"]);
    // index hash join
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int, b int, index idx(a, b))");
    tk.must_exec("create table t2(a int, b int, index idx(a))");
    tk.must_exec("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    tk.must_exec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    sql = "select /*+ inl_hash_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"]);
    sql = "select /*+ inl_hash_join(t2) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"]);
    // index join
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int , b int, unique index idx(a))");
    tk.must_exec("create table t2(a int, b int, unique index idx(a))");
    tk.must_exec("insert into t1 values (1, 1), (2, 2), (3, 3)");
    tk.must_exec("insert into t2 values (1, 1), (2, 2), (3, 3)");
    sql = "select /*+ inl_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check(["0", "1", "2"]);
    sql = "select /*+ inl_join(t2) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check(["0", "1", "2"]);
    // index merge
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a int, b int, c int, index idxa(a), unique index idxb(b))");
    tk.must_exec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (1, 5, 1), (2, 6, 2), (3, 7, 3), (4, 8, 4)");
    sql = "select /*+ use_index_merge(t) */ * from t where (a > 0 or b < 0) and (select count(*) from t t1 where t1.c > t.a) > 0".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["1 1 1", "1 5 1", "2 2 2", "2 6 2", "3 3 3", "3 7 3"]);
    // aggregation
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a int, b int)");
    tk.must_exec(
        "insert into t values (1, 1), (2, 2), (3, 3), (4, 4), (1, 1), (2, 2), (3, 3), (4, 4)",
    );
    sql = "select /*+ stream_agg() */ a from t where (select count(*) from t1 where t1.b > t.a) > 1 group by a".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check(["1"]);
    sql = "select /*+ hash_agg() */ a from t where (select count(*) from t1 where t1.b > t.a) > 1 group by a".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql).sort().check(["1"]);
}

/// Go `TestApplyConcurrency`.
#[test]
fn test_apply_concurrency() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    // tidb_executor_concurrency
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int, b int)");
    tk.must_exec("create table t2(a int, b int)");
    let mut sql =
        "select * from t1 where t1.b > (select sum(t2.b) from t2 where t2.a > t1.a)".to_owned();
    tk.must_exec("set tidb_executor_concurrency = 3");
    check_apply_plan(&mut tk, &sql, 3);
    tk.must_exec("set tidb_executor_concurrency = 5");
    check_apply_plan(&mut tk, &sql, 5);
    // concurrency
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (a int)");
    let mut vals = "".to_owned();
    let mut n = 100;
    for i in 1..=n {
        if i > 1 {
            vals += ",";
        }
        vals += &format!("({})", go_v(&i));
    }
    tk.must_exec(&format!("insert into t values {}", go_v(&vals)));
    sql =
        "select sum(a) from t where t.a >= (select max(a) from t t1 where t1.a <= t.a)".to_owned();
    for cc in (1..=10).step_by(3 as usize) {
        tk.must_exec(&format!("set tidb_executor_concurrency = {}", go_v(&cc)));
        tk.must_query(&sql)
            .check([format!("{}", (n * (n + 1)) / 2).as_str()]);
    }
}

/// Go `TestApplyCacheRatio`.
#[test]
fn test_apply_cache_ratio() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int, b int)");
    tk.must_exec("create table t2(a int, b int)");
    let mut sql =
        "select * from t1 where (select min(t2.b) from t2 where t2.a> t1.a) > 10".to_owned();
    tk.must_exec("insert into t2 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)");
    let check_ratio = |tk: &mut TestKit, ratio: &str| -> bool {
        let mut rows = tk.must_query(&format!("{}{}", "explain analyze ", sql));
        for r in rows.clone() {
            let mut line = format!("{}", go_v(&r));
            if line.contains(&format!("{}{}", "cacheHitRatio:", ratio)) {
                return true;
            }
        }
        return false;
    };
    // 10%
    tk.must_exec("insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (1, 1)");
    assert!(check_ratio(&mut tk, "10.000%"));
    tk.must_exec("set tidb_mem_quota_apply_cache = 0");
    assert!(!check_ratio(&mut tk, ""));
    tk.must_exec("set tidb_mem_quota_apply_cache = 33554432");
    // 20%
    tk.must_exec("truncate t1");
    tk.must_exec("insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (2, 2), (1, 1)");
    assert!(check_ratio(&mut tk, "20.000%"));
    tk.must_exec("set tidb_mem_quota_apply_cache = 0");
    assert!(!check_ratio(&mut tk, ""));
    tk.must_exec("set tidb_mem_quota_apply_cache = 33554432");
    // 50%
    tk.must_exec("truncate t1");
    tk.must_exec("insert into t1 values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)");
    assert!(check_ratio(&mut tk, "50.000%"));
    tk.must_exec("set tidb_mem_quota_apply_cache = 0");
    assert!(!check_ratio(&mut tk, ""));
}

/// Go `TestApplyGoroutinePanic`.
#[test]
fn test_apply_goroutine_panic() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int, b int)");
    tk.must_exec("create table t2(a int, b int)");
    tk.must_exec("insert into t1 values (1, 1), (1, 1), (2, 2), (2, 3), (2, 3), (1, 1), (1, 1), (2, 2), (2, 3), (2, 3)");
    tk.must_exec(
        "insert into t2 values (2, 2), (3,3), (-1, 1), (5, 4), (2, 2), (3,3), (-1, 1), (5, 4)",
    );
    // no panic
    let mut sql =
        "select (select count(*) from t2 where t2.a > t1.a and t2.b > t1.a) from t1".to_owned();
    check_apply_plan(&mut tk, &sql, 1);
    tk.must_query(&sql)
        .sort()
        .check(["4", "4", "4", "4", "4", "4", "6", "6", "6", "6"]);
    // panic in a inner worker
    let fp1 = enable_failpoint(
        "github.com/pingcap/tidb/pkg/executor/parallelApplyInnerWorkerPanic",
        "panic",
    );
    let mut err = tk.exec_to_err(&sql);
    assert!(err.is_some());
    drop(fp1);
    for panic_name in [
        "parallelApplyInnerWorkerPanic",
        "parallelApplyOuterWorkerPanic",
        "parallelApplyGetCachePanic",
        "parallelApplySetCachePanic",
    ] {
        let mut panic_path = format!("github.com/pingcap/tidb/pkg/executor/{}", go_v(&panic_name));
        let fp2 = enable_failpoint(&panic_path, "panic");
        assert!(tk.exec_to_err(&sql).is_some());
        drop(fp2);
    }
}

/// Go `TestParallelApplyCorrectness`.
#[test]
fn test_parallel_apply_correctness() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1;");
    tk.must_exec(
        "create table t1 (c1 bigint, c2 int, c3 int, c4 int, primary key(c1, c2), index (c3));",
    );
    tk.must_exec("insert into t1 values(1, 1, 1, 1), (1, 2, 3, 3), (2, 1, 4, 4), (2, 2, 2, 2);");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    let mut sql = "select (select /*+ NO_DECORRELATE() */ sum(c4) from t1 where t1.c3 = alias.c3) from t1 alias where alias.c1 = 1;".to_owned();
    tk.must_query(&sql).sort().check(["1", "3"]);
    tk.must_exec("set tidb_enable_parallel_apply=false");
    tk.must_query(&sql).sort().check(["1", "3"]);
}

/// Go `TestParallelApplyCancelInflight`.
#[test]
fn test_parallel_apply_cancel_inflight() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_exec("set tidb_executor_concurrency = 3");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int)");
    tk.must_exec("create table t2(a int)");
    for i in 1..=20 {
        tk.must_exec(&format!("insert into t1 values ({})", go_v(&i)));
    }
    tk.must_exec("insert into t2 values (1), (2), (3)");
    // Verify the optimizer chooses a parallel Apply plan for this query.
    let mut sql = "select * from (select t1.a from t1 where exists (select /*+ NO_DECORRELATE() */ 1 from t2 where t2.a < t1.a)) sub limit 1".to_owned();
    check_apply_plan(&mut tk, &sql, 3);
    // The failpoint makes each inner execution sleep 300ms but respects
    // context cancellation, so cancelled workers return immediately.
    let fp1 = enable_failpoint(
        "github.com/pingcap/tidb/pkg/executor/parallelApplySlowInner",
        r#"return(300)"#,
    );
    // LIMIT 1: once one row is produced, Close() fires and should cancel
    // in-flight inner workers via context cancellation.
    let start = std::time::Instant::now();
    let mut rows = tk.must_query(&sql);
    let elapsed = start.elapsed();
    // We got exactly 1 row.
    assert_eq!(rows.len(), 1 as usize);
    // Without cancel-in-flight, all 20 outer rows would be processed
    // (~2s per run). With cancellation, Close() fires after the first
    // result and aborts workers sleeping in the failpoint via context
    // cancellation. Note: fillInnerChunk may process multiple outer
    // rows per call, so the effective savings depend on how many rows
    // are in-flight when cancel fires. The 3s threshold is generous
    // enough to avoid CI flakiness while catching regressions where
    // cancellation is completely broken.
    assert!(elapsed < std::time::Duration::from_secs(3), "{:?}", elapsed);
}

/// Go `TestOrderedParallelApply`.
#[test]
fn test_ordered_parallel_apply() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a int, b int, index idx_a(a))");
    tk.must_exec("create table t2 (a int, b int)");
    tk.must_exec("insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100)");
    tk.must_exec(
        "insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)",
    );
    // ----------------------------------------------------------------
    // 1. ORDER BY with scalar correlated subquery – verify row order
    //    is preserved (not just sorted after the fact).
    // ----------------------------------------------------------------
    let mut q1 = "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a"
        .to_owned();
    // Serial: get baseline
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut serial_result = tk.must_query(&q1);
    let mut serial_rows = serial_result;
    // Parallel ordered: should produce identical order.
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_exec("set tidb_executor_concurrency=5");
    check_apply_plan(&mut tk, &q1, 5);
    tk.must_query(&q1).check_result(&serial_rows);
    // ----------------------------------------------------------------
    // 2. ORDER BY + LIMIT – should use Limit (not TopN) because the
    //    ordered parallel apply preserves outer order.
    // ----------------------------------------------------------------
    let mut q2 = "select t1.a, t1.b from t1 where t1.b > (select min(t2.b) from t2 where t2.a >= t1.a) order by t1.a limit 5".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut serial_result2 = tk.must_query(&q2);
    let mut serial_rows2 = serial_result2;
    tk.must_exec("set tidb_enable_parallel_apply=true");
    check_apply_plan(&mut tk, &q2, 5);
    // The planner should use a streamable Limit (not TopN) because the
    // ordered parallel apply preserves the outer scan order.
    // Verify the outer plan uses a streamable Limit (not TopN).
    // TopN may legitimately appear inside the inner (Probe) side
    // (e.g. for min/max optimization), so only check operators at
    // or above the Apply level.
    let mut plan_rows = tk.must_query(&format!("{}{}", "explain ", q2));
    let mut has_limit = false;
    let mut has_outer_top_n = false;
    let mut seen_apply = false;
    for row in plan_rows.clone() {
        let mut line = format!("{}", go_v(&row));
        if line.contains("Apply") {
            seen_apply = true;
        }
        if !seen_apply {
            // Operators above Apply — check for Limit vs TopN.
            if line.contains("Limit") {
                has_limit = true;
            }
            if line.contains("TopN") {
                has_outer_top_n = true;
            }
        }
    }
    assert!(has_limit);
    assert!(!has_outer_top_n);
    tk.must_query(&q2).check_result(&serial_rows2);
    // ----------------------------------------------------------------
    // 3. EXISTS semi-join with ORDER BY – common pattern from the
    //    correlate branch.
    // ----------------------------------------------------------------
    let mut q3 = "select t1.a from t1 where exists (select 1 from t2 where t2.a = t1.a and t2.b > 3) order by t1.a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut serial_result3 = tk.must_query(&q3);
    let mut serial_rows3 = serial_result3;
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q3).check_result(&serial_rows3);
    // ----------------------------------------------------------------
    // 4. Varying concurrency levels – results must be identical.
    // ----------------------------------------------------------------
    let mut q4 =
        "select t1.a, (select count(*) from t2 where t2.a > t1.a) from t1 order by t1.a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected = tk.must_query(&q4);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    for cc in [2, 3, 5, 7, 10] {
        tk.must_exec(&format!("set tidb_executor_concurrency={}", go_v(&cc)));
        tk.must_query(&q4).check_result(&expected);
    }
    // ----------------------------------------------------------------
    // 5. Ordered parallel apply with OFFSET.
    // ----------------------------------------------------------------
    let mut q5 = "select t1.a, t1.b from t1 where t1.b > (select min(t2.b) from t2 where t2.a >= t1.a) order by t1.a limit 3 offset 2".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut serial_result5 = tk.must_query(&q5);
    let mut serial_rows5 = serial_result5;
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_exec("set tidb_executor_concurrency=5");
    tk.must_query(&q5).check_result(&serial_rows5);
}

/// Go `TestOrderedParallelApplyEdgeCases`.
#[test]
fn test_ordered_parallel_apply_edge_cases() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_exec("set tidb_executor_concurrency=4");
    // ----------------------------------------------------------------
    // 1. Outer filter with unselected rows — exercises the
    //    processOneOuterRow !or.selected path.
    // ----------------------------------------------------------------
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a int, b int, index idx_a(a))");
    tk.must_exec("create table t2 (a int, b int)");
    tk.must_exec("insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5)");
    tk.must_exec("insert into t2 values (1,10),(2,20),(3,30)");
    // The WHERE t1.a > 2 acts as an outer filter so rows with a<=2 hit
    // the unselected path in processOneOuterRow.
    let mut q1 = "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a > 2 order by t1.a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected1 = tk.must_query(&q1);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    check_apply_plan(&mut tk, &q1, 4);
    tk.must_query(&q1).check_result(&expected1);
    // ----------------------------------------------------------------
    // 2. NOT EXISTS (anti-semi-join) with ORDER BY — exercises
    //    OnMissMatch in the ordered path.
    // ----------------------------------------------------------------
    let mut q2 =
        "select t1.a from t1 where not exists (select 1 from t2 where t2.a = t1.a) order by t1.a"
            .to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected2 = tk.must_query(&q2);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q2).check_result(&expected2);
    // ----------------------------------------------------------------
    // 3. ORDER BY + LIMIT 1 — the primary use-case for eager flush.
    //    Verifies the reorder worker flushes partial output early.
    // ----------------------------------------------------------------
    let mut q3 =
        "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a limit 1"
            .to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected3 = tk.must_query(&q3);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q3).check_result(&expected3);
    // ----------------------------------------------------------------
    // 4. Empty outer side — orderedResultCh closes immediately.
    // ----------------------------------------------------------------
    let mut q4 = "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a > 999 order by t1.a".to_owned();
    tk.must_query(&q4).check_empty();
    // ----------------------------------------------------------------
    // 5. Single row outer — minimal ordered path exercise.
    // ----------------------------------------------------------------
    let mut q5 = "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a = 3 order by t1.a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected5 = tk.must_query(&q5);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q5).check_result(&expected5);
    // ----------------------------------------------------------------
    // 6. Left outer semi-join with ORDER BY — exercises the e.outer
    //    path where OnMissMatch must emit the outer row.
    // ----------------------------------------------------------------
    tk.must_exec("drop table if exists t3, t4");
    tk.must_exec("create table t3 (a int, index idx_a(a))");
    tk.must_exec("create table t4 (a int)");
    tk.must_exec("insert into t3 values (1),(2),(3),(4),(5)");
    tk.must_exec("insert into t4 values (2),(4)");
    // The IN subquery uses left-outer-semi-join producing a boolean column.
    let mut q6 = "select a, a in (select /*+ NO_DECORRELATE() */ a from t4 where t4.a = t3.a) from t3 order by a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected6 = tk.must_query(&q6);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q6).check_result(&expected6);
    // ----------------------------------------------------------------
    // 7. Large result set — exercises the appendRow→IsFull→flushOutput
    //    path where the output chunk fills to capacity and must be
    //    flushed mid-drain.
    // ----------------------------------------------------------------
    tk.must_exec("drop table if exists t5, t6");
    tk.must_exec("create table t5 (a int, index idx_a(a))");
    tk.must_exec("create table t6 (a int)");
    let mut vals = "".to_owned();
    for i in 1..=2000 {
        if i > 1 {
            vals += ",";
        }
        vals += &format!("({})", go_v(&i));
    }
    tk.must_exec(&format!("{}{}", "insert into t5 values ", vals));
    tk.must_exec("insert into t6 values (1),(2),(3)");
    let mut q7 = "select t5.a, (select count(*) from t6 where t6.a <= t5.a) from t5 order by t5.a"
        .to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected7 = tk.must_query(&q7);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q7).check_result(&expected7);
}

/// Go `TestOrderedParallelApplyLargeInner`.
#[test]
fn test_ordered_parallel_apply_large_inner() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_exec("set tidb_executor_concurrency=4");
    // ----------------------------------------------------------------
    // Each outer row joins with many inner rows, forcing the chunk to
    // fill to capacity in processOneOuterRow (chk.IsFull path) and
    // the reorder worker's appendRow→flushOutput path.
    // ----------------------------------------------------------------
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a int, index idx_a(a))");
    tk.must_exec("create table t2 (a int, b int)");
    tk.must_exec("insert into t1 values (1),(2),(3)");
    // Insert enough rows to exceed chunk capacity for each outer row.
    let mut vals = "".to_owned();
    for i in 1..=2000 {
        if i > 1 {
            vals += ",";
        }
        vals += &format!("({}, {})", i % 3 + 1, go_v(&i));
    }
    tk.must_exec(&format!("{}{}", "insert into t2 values ", vals));
    let mut q = "select t1.a, (select /*+ NO_DECORRELATE() */ count(*) from t2 where t2.a = t1.a) from t1 order by t1.a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected = tk.must_query(&q);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    check_apply_plan(&mut tk, &q, 4);
    tk.must_query(&q).check_result(&expected);
    // ----------------------------------------------------------------
    // Cartesian-style: every outer row pairs with every inner row to
    // produce a large result set (exercises multi-chunk flush in the
    // reorder worker).
    // ----------------------------------------------------------------
    tk.must_exec("drop table if exists t3, t4");
    tk.must_exec("create table t3 (a int, index idx_a(a))");
    tk.must_exec("create table t4 (b int)");
    tk.must_exec("insert into t3 values (1),(2),(3)");
    vals = "".to_owned();
    for i in 1..=500 {
        if i > 1 {
            vals += ",";
        }
        vals += &format!("({})", go_v(&i));
    }
    tk.must_exec(&format!("{}{}", "insert into t4 values ", vals));
    let mut q2 = "select t3.a, (select /*+ NO_DECORRELATE() */ sum(t4.b) from t4 where t4.b <= t3.a * 100) from t3 order by t3.a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected2 = tk.must_query(&q2);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q2).check_result(&expected2);
}

/// Go `TestOrderedParallelApplyLeftOuterSemiJoin`.
#[test]
fn test_ordered_parallel_apply_left_outer_semi_join() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_exec("set tidb_executor_concurrency=4");
    // ----------------------------------------------------------------
    // Left outer semi-join with outer filter: some outer rows are
    // not selected, exercising the e.outer && !or.selected path in
    // processOneOuterRow. Also exercises OnMissMatch in ordered mode.
    // ----------------------------------------------------------------
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a int, b int, index idx_a(a))");
    tk.must_exec("create table t2 (a int, b int)");
    tk.must_exec("insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50)");
    tk.must_exec("insert into t2 values (2,2),(4,4)");
    // IN subquery → left outer semi join; the NOT IN variant produces
    // anti-semi join with OnMissMatch for non-matching rows.
    let mut q1 = "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 order by a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected1 = tk.must_query(&q1);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q1).check_result(&expected1);
    // NOT IN subquery — anti-semi-join path
    let mut q2 = "select a from t1 where a not in (select /*+ NO_DECORRELATE() */ a from t2 where t2.b < t1.b) order by a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected2 = tk.must_query(&q2);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q2).check_result(&expected2);
    // Left outer join with ORDER BY — the subquery returns a value for
    // every outer row, exercising both matched and unmatched paths.
    let mut q3 =
        "select t1.a, (select t2.b from t2 where t2.a = t1.a) from t1 order by t1.a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected3 = tk.must_query(&q3);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q3).check_result(&expected3);
    // ----------------------------------------------------------------
    // Left outer semi-join with outer-side WHERE conditions.
    // Note: the planner pushes outer-side filters below the Apply
    // into the outer child (e.g. IndexRangeScan), so the outerFilter
    // field on the Apply is empty and selected=true for all rows
    // reaching processOneOuterRow.  These tests verify correctness
    // of the left outer semi join with reduced outer row sets and
    // nullable columns.
    // ----------------------------------------------------------------
    tk.must_exec("insert into t1 values (null, null), (6, null)");
    let mut q4 = "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 where t1.b is not null order by a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected4 = tk.must_query(&q4);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q4).check_result(&expected4);
    let mut q5 = "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 where t1.a > 2 order by a".to_owned();
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut expected5 = tk.must_query(&q5);
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_query(&q5).check_result(&expected5);
}

/// Go `TestOrderedParallelApplyGoroutinePanic`.
#[test]
fn test_ordered_parallel_apply_goroutine_panic() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a int, b int, index idx_a(a))");
    tk.must_exec("create table t2 (a int, b int)");
    tk.must_exec("insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5)");
    tk.must_exec("insert into t2 values (1,10),(2,20),(3,30)");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_exec("set tidb_executor_concurrency=3");
    let mut sql =
        "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a"
            .to_owned();
    // Verify baseline works.
    check_apply_plan(&mut tk, &sql, 3);
    tk.must_query(&sql)
        .check(["1 10", "2 20", "3 30", "4 30", "5 30"]);
    // Panic in ordered inner worker — error should propagate through
    // reorder worker to the consumer.
    let fp1 = enable_failpoint(
        "github.com/pingcap/tidb/pkg/executor/parallelApplyInnerWorkerOrderedPanic",
        "panic",
    );
    assert!(tk.exec_to_err(&sql).is_some());
    drop(fp1);
    // Panic in outer worker (shared with unordered, but verify it
    // works with the reorder worker present).
    let fp2 = enable_failpoint(
        "github.com/pingcap/tidb/pkg/executor/parallelApplyOuterWorkerPanic",
        "panic",
    );
    assert!(tk.exec_to_err(&sql).is_some());
    drop(fp2);
}

/// Go `TestOrderedParallelApplyKillSignal`.
#[test]
fn test_ordered_parallel_apply_kill_signal() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a int, b int, index idx_a(a))");
    tk.must_exec("create table t2 (a int, b int)");
    tk.must_exec("insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5)");
    tk.must_exec("insert into t2 values (1,10),(2,20),(3,30)");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_exec("set tidb_executor_concurrency=3");
    let mut sql =
        "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a"
            .to_owned();
    // Verify baseline works before testing kill.
    check_apply_plan(&mut tk, &sql, 3);
    tk.must_query(&sql)
        .check(["1 10", "2 20", "3 30", "4 30", "5 30"]);
    // Enable a failpoint that makes inner workers sleep, giving us time
    // to send a kill signal while the ordered pipeline is active.
    let mut fp_path = "github.com/pingcap/tidb/pkg/executor/parallelApplyOrderedSleep".to_owned();
    let fp1 = enable_failpoint(&fp_path, "return(500)");
    // Kill signal during inner worker processing.
    // The kill signal is sent after 200ms while inner workers are sleeping
    // for 500ms each. The exec.Next() kill check after the sleep should
    // detect the signal and return ErrQueryInterrupted, which propagates
    // through orderedResultCh → reorder worker → resultChkCh → consumer.
    let killer = tk.session().sql_killer();
    killer.reset();
    let wg = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(200));
        killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
    });
    let start = std::time::Instant::now();
    let mut err = tk.exec_to_err(&sql);
    let elapsed = start.elapsed();
    assert!(err.is_some());
    // The query should be interrupted well before all 5 rows × 500ms
    // sleep would complete (~2.5s). Allow generous headroom but verify
    // it didn't run to completion.
    assert!(elapsed < std::time::Duration::from_secs(2), "{:?}", elapsed);
    wg.join().unwrap();
}

/// Go `TestOrderedParallelApplyNested`.
#[test]
fn test_ordered_parallel_apply_nested() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2, t3");
    tk.must_exec("create table t1 (a int, b int, index idx_a(a))");
    tk.must_exec("create table t2 (a int, b int)");
    tk.must_exec("create table t3 (a int, b int)");
    tk.must_exec("insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50)");
    tk.must_exec("insert into t2 values (1,100),(2,200),(3,300)");
    tk.must_exec("insert into t3 values (1,1000),(2,2000),(3,3000),(4,4000)");
    tk.must_exec("set tidb_executor_concurrency=3");
    // ----------------------------------------------------------------
    // Case 1: Two correlated subqueries in SELECT with ORDER BY.
    // This produces two stacked Apply operators:
    //   Apply_outer(outer=Apply_inner(outer=IndexScan, inner=t2_subq), inner=t3_subq)
    // Both Apply operators are on the outer-child path, so both are
    // parallelized with KeepOrder=true.
    // ----------------------------------------------------------------
    let mut q1 = r#"select t1.a, (select /*+ NO_DECORRELATE() */ max(t2.b) from t2 where t2.a = t1.a), (select /*+ NO_DECORRELATE() */ max(t3.b) from t3 where t3.a = t1.a) from t1 order by t1.a"#.to_owned();
    // Verify both Apply operators are parallel with Concurrency:3.
    tk.must_exec("set tidb_enable_parallel_apply=true");
    let mut rows = tk.must_query(&format!("{}{}", "explain analyze ", q1));
    let mut apply_count = 0;
    for row in rows.clone() {
        let mut id = format!("{}", row[0]);
        if id.contains("Apply") {
            apply_count += 1;
            let mut exec_info = format!("{}", row[5]);
            assert!(exec_info.contains("Concurrency:3"), "{:?}", exec_info);
        }
    }
    assert_eq!(2, apply_count);
    // Verify ordering: compare parallel vs serial results.
    let mut expected1 = tk.must_query(&q1);
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut serial1 = tk.must_query(&q1);
    assert_eq!(serial1, expected1);
    // ----------------------------------------------------------------
    // Case 2: Subquery inside subquery (Apply inside inner child).
    // This produces: Apply_outer(outer=IndexScan, inner=Apply_inner(...))
    // Per limitation 2, Apply_inner stays serial while Apply_outer is
    // parallel with KeepOrder=true.
    // ----------------------------------------------------------------
    tk.must_exec("set tidb_enable_parallel_apply=true");
    let mut q2 = r#"select t1.a, (select /*+ NO_DECORRELATE() */ max(t2.b) + (select /*+ NO_DECORRELATE() */ max(t3.b) from t3 where t3.a = t2.a) from t2 where t2.a = t1.a) from t1 order by t1.a"#.to_owned();
    let mut rows2 = tk.must_query(&format!("{}{}", "explain analyze ", q2));
    let mut parallel_apply_count = 0;
    let mut serial_apply_count = 0;
    for row in rows2.clone() {
        let mut id = format!("{}", row[0]);
        let mut exec_info = format!("{}", row[5]);
        if !id.contains("Apply") {
            continue;
        }
        if exec_info.contains("concurrency:OFF") {
            serial_apply_count += 1;
        } else if exec_info.contains("Concurrency:") {
            parallel_apply_count += 1;
        }
    }
    // Outer Apply is parallel; inner-side Apply stays serial (limitation 2).
    assert_eq!(1, parallel_apply_count);
    assert_eq!(1, serial_apply_count);
    let mut expected2 = tk.must_query(&q2);
    tk.must_exec("set tidb_enable_parallel_apply=false");
    let mut serial2 = tk.must_query(&q2);
    assert_eq!(serial2, expected2);
    // ----------------------------------------------------------------
    // Case 3: Two stacked Apply with LIMIT — verify ordering preserved
    // through both layers.
    // ----------------------------------------------------------------
    tk.must_exec("set tidb_enable_parallel_apply=true");
    let mut q3 = r#"select t1.a, (select /*+ NO_DECORRELATE() */ max(t2.b) from t2 where t2.a = t1.a), (select /*+ NO_DECORRELATE() */ max(t3.b) from t3 where t3.a = t1.a) from t1 order by t1.a limit 3"#.to_owned();
    tk.must_query(&q3)
        .check(["1 100 1000", "2 200 2000", "3 300 3000"]);
    // ----------------------------------------------------------------
    // Case 4: Varying concurrency with nested apply.
    // ----------------------------------------------------------------
    for conc in [1, 2, 5] {
        tk.must_exec(&format!("set tidb_executor_concurrency={}", go_v(&conc)));
        let mut result = tk.must_query(&q1);
        assert_eq!(serial1, result);
    }
}
