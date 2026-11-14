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

//! Go `pkg/executor/parallel_apply_test.go`: original SQL, expected rows,
//! plan/concurrency assertions, cache ratios, cancellation and failpoints.

use std::time::{Duration, Instant};
use tidb_session::{Session, StmtResult};

struct TestKit {
    session: Session,
}
impl TestKit {
    fn new() -> Self {
        Self {
            session: Session::new(),
        }
    }
    fn exec(&mut self, sql: &str) {
        self.session
            .run(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    fn rows(&mut self, sql: &str) -> Vec<Vec<String>> {
        let StmtResult::Rows(rows) = self
            .session
            .run(sql)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        else {
            panic!("expected rows: {sql}");
        };
        rows.iter()
            .map(|row| {
                row.iter()
                    .map(|value| {
                        if value.is_null() {
                            "<nil>".to_owned()
                        } else {
                            String::from_utf8(value.to_bytes().unwrap()).unwrap()
                        }
                    })
                    .collect()
            })
            .collect()
    }
    fn check(&mut self, sql: &str, sorted: bool, expected: &[&str]) {
        let mut actual: Vec<_> = self.rows(sql).iter().map(|row| row.join(" ")).collect();
        if sorted {
            actual.sort();
        }
        assert_eq!(actual, expected, "{sql}");
    }
    fn compare_serial(&mut self, sql: &str) {
        self.exec("set tidb_enable_parallel_apply=false");
        let expected = self.rows(sql);
        self.exec("set tidb_enable_parallel_apply=true");
        assert_eq!(self.rows(sql), expected, "{sql}");
    }
    /// Go `checkApplyPlan`.
    fn plan(&mut self, sql: &str, concurrency: usize) {
        let rows = self.rows(&format!("explain analyze {sql}"));
        let apply = rows
            .iter()
            .find(|row| row[0].contains("Apply"))
            .unwrap_or_else(|| panic!("no Apply: {sql}: {rows:?}"));
        if concurrency > 0 {
            let text = if concurrency == 1 {
                "Concurrency:".to_owned()
            } else {
                format!("Concurrency:{concurrency}")
            };
            assert!(apply.join(" ").contains(&text), "{sql}: {rows:?}");
        }
    }
}

/// Go `TestParallelApplyPlan`.
#[test]
fn test_parallel_apply_plan() {
    let mut tk = TestKit::new();
    tk.exec("use test");
    tk.exec("drop table if exists t");
    tk.exec("create table t (a int, b int)");
    tk.exec("insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (null, null)");
    let mut q1 = "select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a)";
    tk.plan(q1, 0);
    tk.check(q1, true, &["1", "2", "3", "4", "5", "6", "7", "8", "9"]);
    tk.exec("set tidb_enable_parallel_apply=true");
    tk.plan(q1, 1);
    tk.check(q1, true, &["1", "2", "3", "4", "5", "6", "7", "8", "9"]);
    let mut q2 = "select * from t t0 where t0.b <= (select max(t1.b) from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a and t0.a > t2.a));";
    tk.plan(q2, 1);
    tk.check(
        q2,
        true,
        &[
            "1 1", "2 2", "3 3", "4 4", "5 5", "6 6", "7 7", "8 8", "9 9",
        ],
    );
    let mut q3 = "select t1.b from t t1 where t1.b > (select max(b) from t t2 where t1.a > t2.a) order by t1.a";
    tk.plan(q3, 0);
    tk.exec("alter table t add index idx(a)");
    tk.plan(q3, 1);
    tk.check(q3, false, &["1", "2", "3", "4", "5", "6", "7", "8", "9"]);
    tk.check("show warnings", false, &[]);
}

/// Go `TestApplyColumnType`.
#[test]
fn test_apply_column_type() {
    let mut tk = TestKit::new();
    tk.exec("use test");
    tk.exec("set tidb_enable_parallel_apply=true");
    tk.exec("create table t(a int, b int)");
    tk.exec("create table t1(a int, b int)");
    tk.exec("insert into t values(1,1), (2,2), (5,5), (2, 4), (5, 2), (9, 4)");
    tk.exec("insert into t1 values(2, 3), (4, 9), (10, 4), (1, 10)");
    let mut sql = "select * from t where t.b > (select min(t1.b) from t1 where t1.a > t.a)";
    tk.plan(sql, 1);
    tk.check(sql, true, &["5 5"]);
    tk.exec("drop table t, t1");
    tk.exec("create table t1(a varchar(255), b varchar(255))");
    tk.exec("create table t2(a varchar(255))");
    tk.exec("insert into t1 values(\"aa\", \"bb\"), (\"aa\", \"tikv\"), (\"bb\", \"cc\"), (\"bb\", \"ee\")");
    tk.exec("insert into t2 values(\"kk\"), (\"aa\"), (\"dd\"), (\"bb\")");
    sql = "select (select min(t2.a) from t2 where t2.a > t1.a) from t1";
    tk.plan(sql, 1);
    tk.check(sql, true, &["bb", "bb", "dd", "dd"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a bit(10), b int)");
    tk.exec("create table t2(a bit(10), b int)");
    tk.exec("insert into t1 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)");
    tk.exec("insert into t2 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)");
    sql = "select b from t1 where t1.b > (select min(t2.b) from t2 where t2.a < t1.a)";
    tk.plan(sql, 1);
    tk.check(sql, true, &["2", "2", "3", "3", "4", "4"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a char(25), b int)");
    tk.exec("create table t2(a char(10), b int)");
    tk.exec("insert into t1 values(\"abc\", 1), (\"abc\", \"5\"), (\"fff\", 4), (\"fff\", 9), (\"tidb\", 6), (\"tidb\", 5)");
    tk.exec("insert into t2 values()");
    sql = "select t1.b from t1 where t1.b > (select max(t2.b) from t2 where t2.a > t1.a)";
    tk.plan(sql, 1);
    tk.check(sql, true, &[]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a int, b double)");
    tk.exec("create table t2(a int, b double)");
    tk.exec("insert into t1 values(1, 2.12), (1, 1.11), (2, 3), (2, 4.56), (5, 55), (5, -4)");
    tk.exec("insert into t2 values(1, 3.22), (3, 4.5), (5, 2.3), (4, 5.55)");
    sql = "select * from t1 where t1.a < (select avg(t2.a) from t2 where t2.b > t1.b)";
    tk.plan(sql, 1);
    tk.check(sql, true, &["1 1.11", "1 2.12", "2 3", "2 4.56"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a date, b int, c int)");
    tk.exec("create table t2(a date, b int)");
    tk.exec("insert into t1 values(\"2020-01-01\", 3, 4), (\"2020-01-01\", 4, 5), (\"2020-01-01\", 4, 3), (\"2020-02-01\", 7, 7), (\"2020-02-01\", 6, 6)");
    tk.exec("insert into t2 values(\"2020-01-02\", 4), (\"2020-02-02\", 8), (\"2020-02-02\", 7)");
    sql = "select * from t1 where t1.b >= (select min(t2.b) from t2 where t2.a > t1.a) and t1.c >= (select min(t2.b) from t2 where t2.a > t1.a)";
    tk.check(sql, true, &["2020-01-01 4 5", "2020-02-01 7 7"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a datetime, b int)");
    tk.exec("create table t2(a datetime, b int)");
    tk.exec("insert into t1 values(\"2020-01-01 00:00:00\", 1), (\"2020-01-01 00:00:00\", 2), (\"2020-06-06 00:00:00\", 3), (\"2020-06-06 00:00:00\", 4), (\"2020-09-08 00:00:00\", 4)");
    tk.exec("insert into t2 values(\"2020-01-01 00:00:00\", 1), (\"2020-01-01 00:00:01\", 2), (\"2020-08-20 00:00:00\", 4)");
    sql = "select b from t1 where t1.b >= (select max(t2.b) from t2 where t2.a > t1.a)";
    tk.plan(sql, 1);
    tk.check(sql, true, &["4"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a timestamp, b int)");
    tk.exec("create table t2(a timestamp, b int)");
    tk.exec("insert into t1 values(\"2020-01-01 00:00:00\", 1), (\"2020-01-01 00:00:00\", 2), (\"2020-06-06 00:00:00\", 3), (\"2020-06-06 00:00:00\", 4), (\"2020-09-08 00:00:00\", 4)");
    tk.exec("insert into t2 values(\"2020-01-01 00:00:00\", 1), (\"2020-01-01 00:00:01\", 2), (\"2020-08-20 00:00:00\", 4)");
    sql = "select b from t1 where t1.b >= (select max(t2.b) from t2 where t2.a > t1.a)";
    tk.plan(sql, 1);
    tk.check(sql, true, &["4"]);
}

/// Go `TestApplyMultiColumnType`.
#[test]
fn test_apply_multi_column_type() {
    let mut tk = TestKit::new();
    tk.exec("use test");
    tk.exec("set tidb_enable_parallel_apply=true");
    tk.exec("create table t1(a int, b int)");
    tk.exec("create table t2(a int, b int)");
    tk.exec("insert into t1 values (1, 1), (1, 1), (2, 2), (2, 3), (2, 3), (1, 1), (1, 1), (2, 2), (2, 3), (2, 3)");
    tk.exec("insert into t2 values (2, 2), (3,3), (-1, 1), (5, 4), (2, 2), (3,3), (-1, 1), (5, 4)");
    let mut sql = "select (select count(*) from t2 where t2.a > t1.a and t2.b > t1.a) from t1";
    tk.plan(sql, 1);
    tk.check(
        sql,
        true,
        &["4", "4", "4", "4", "4", "4", "6", "6", "6", "6"],
    );
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a int, b char(20))");
    tk.exec("create table t2(a int, b char(20))");
    tk.exec("insert into t1 values (1, \"a\"), (2, \"b\"), (3, \"c\"), (1, \"a\"), (2, \"b\"), (3, \"c\")");
    tk.exec("insert into t2 values (1, \"a\"), (2, \"b\"), (3, \"c\"), (1, \"a\"), (2, \"b\"), (3, \"c\")");
    sql = "select (select sum(t2.a) from t2 where t2.a > t1.a or t2.b < t1.b) from t1";
    tk.plan(sql, 1);
    tk.check(sql, true, &["10", "10", "6", "6", "8", "8"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a int, b bit(10), c int)");
    tk.exec("create table t2(a int, b int, c int)");
    tk.exec("insert into t1 values (1, '1', 1), (2, '2', 4), (3, '3', 6), (4, '4', 8), (1, '1', 1), (2, '2', 4), (3, '3', 6), (4, '4', 8)");
    tk.exec("insert into t2 values (1, 1111, 11), (2, 2222, 22), (1, 1111, 11), (2, 2222, 22)");
    sql = "select a, c from t1 where (select max(t2.c) from t2 where t2.a > t1.a and t2.b > t1.b) > 4";
    tk.plan(sql, 1);
    tk.check(sql, true, &["1 1", "1 1"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a char(20), b varchar(255))");
    tk.exec("create table t2(a char(20), b varchar(255))");
    tk.exec("insert into t1 values ('7', '7'), ('8', '8'), ('9', '9'), ('7', '7'), ('8', '8'), ('9', '9')");
    tk.exec("insert into t2 values ('7', '7'), ('8', '8'), ('9', '9'), ('7', '7'), ('8', '8'), ('9', '9')");
    sql = "select count(*) from t1 where (select sum(t2.a) from t2 where t2.a >= t1.a and t2.b >= t1.b) > 4";
    tk.plan(sql, 1);
    tk.check(sql, true, &["6"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a varchar(20), b enum(\"a\", \"b\", \"c\", \"d\", \"e\",\"f\"))");
    tk.exec("create table t2(a varchar(20), b int)");
    tk.exec("insert into t1 values ('1', 'a'), ('2', 'b'), ('3', 'c'), ('1', 'a'), ('2', 'b'), ('3', 'c')");
    tk.exec("insert into t2 values ('1', 100), ('2', 200), ('3', 300), ('4', 400), ('1', 100), ('2', 200), ('3', 300), ('4', 400)");
    sql = "select * from t1 where (select sum(t2.b) from t2 where t2.a > t1.a and t2.b * 2 > t1.b) > 0";
    tk.plan(sql, 1);
    tk.check(sql, true, &["1 a", "1 a", "2 b", "2 b", "3 c", "3 c"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a varchar(20), b bit(10))");
    tk.exec("create table t2(a varchar(20), b int)");
    tk.exec("insert into t1 values ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4'), ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4')");
    tk.exec("insert into t2 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)");
    sql = "select a from t1 where (select sum(t2.b) from t2 where t2.a > t1.a and t2.b < t1.b) > 4";
    tk.plan(sql, 1);
    tk.check(sql, true, &["1", "1", "2", "2", "3", "3"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1 (a int, b double)");
    tk.exec("create table t2 (a int, b double)");
    tk.exec("insert into t1 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)");
    tk.exec("insert into t2 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)");
    sql = "select * from t1 where (select min(t2.a) from t2 where t2.a < t1.a and t2.a > 1 and t2.b < t1.b) > 0";
    tk.plan(sql, 1);
    tk.check(sql, true, &["3 3.3", "3 3.3", "4 4.4", "4 4.4"]);
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a int, b datetime)");
    tk.exec("create table t2(a int, b datetime)");
    tk.exec("insert into t1 values (1, \"2020-01-01\"), (2, \"2020-02-02\"), (3, \"2020-03-03\"), (1, \"2020-01-01\"), (2, \"2020-02-02\"), (3, \"2020-03-03\")");
    tk.exec("insert into t2 values (1, \"2020-01-01\"), (2, \"2020-02-02\"), (3, \"2020-03-03\"), (1, \"2020-01-01\"), (2, \"2020-02-02\"), (3, \"2020-03-03\")");
    sql = "select * from t1 where (select count(*) from t2 where t2.a >= t1.a and t2.b between t1.b and \"2020-09-07 00:00:00\") > 1";
    tk.plan(sql, 1);
    tk.check(
        sql,
        true,
        &[
            "1 2020-01-01 00:00:00",
            "1 2020-01-01 00:00:00",
            "2 2020-02-02 00:00:00",
            "2 2020-02-02 00:00:00",
            "3 2020-03-03 00:00:00",
            "3 2020-03-03 00:00:00",
        ],
    );
    tk.exec("drop table t1, t2");
    tk.exec("create table t1(a int, b int, c varchar(20))");
    tk.exec("create table t2(a int, b int, c varchar(20))");
    tk.exec("insert into t1 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3')");
    tk.exec("insert into t2 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3')");
    sql = "select (select min(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b and t2.c > t1.c) from t1";
    tk.plan(sql, 1);
    tk.check(sql, true, &["2", "2", "3", "3", "<nil>", "<nil>"]);
}

/// Go `TestMultipleApply`.
#[test]
fn test_multiple_apply() {
    let mut tk = TestKit::new();
    tk.exec("use test");
    tk.exec("set tidb_enable_parallel_apply=true");
    tk.exec("drop table if exists t1, t2");
    tk.exec("create table t1(a varchar(20), b enum(\"a\", \"b\", \"c\", \"d\", \"e\",\"f\"))");
    tk.exec("create table t2(a varchar(20), b int)");
    tk.exec("insert into t1 values (\"1\", \"a\"), (\"2\", \"b\"), (\"3\", \"c\"), (\"4\", \"d\"), (\"1\", \"a\"), (\"2\", \"b\"), (\"3\", \"c\"), (\"4\", \"d\")");
    tk.exec("insert into t2 values (\"1\", 1), (\"2\", 2), (\"3\", 3), (\"4\", 4), (\"1\", 1), (\"2\", 2), (\"3\", 3), (\"4\", 4)");
    let mut sql = "select * from t1 where (select sum(t2.b) from t2 where t2.a > t1.a) >= (select sum(t2.b) from t2 where t2.b > t1.b)";
    tk.plan(sql, 1);
    tk.check(sql, true, &["1 a", "1 a", "2 b", "2 b", "3 c", "3 c"]);
    tk.exec("drop table if exists t1, t2");
    tk.exec("create table t1(a int, b double)");
    tk.exec("create table t2(a int, b double)");
    tk.exec("insert into t1 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)");
    tk.exec("insert into t2 values (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4), (1, 1.1), (2, 2.2), (3, 3.3), (4, 4.4)");
    sql = "select * from t1 where (select min(t2.a) from t2 where t2.a < t1.a and t2.a > 1) * (select min(t2.a) from t2 where t2.b < t1.b) > 1";
    tk.plan(sql, 1);
    tk.check(sql, true, &["3 3.3", "3 3.3", "4 4.4", "4 4.4"]);
    tk.exec("drop table if exists t1, t2");
    tk.exec("create table t1(a varchar(20), b bit(10))");
    tk.exec("create table t2(a varchar(20), b int)");
    tk.exec("insert into t1 values ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4'), ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4')");
    tk.exec("insert into t2 values ('1', 1111), ('2', 2222), ('3', 3333), ('4', 4444), ('1', 1111), ('2', 2222), ('3', 3333), ('4', 4444)");
    sql = "select a from t1 where (select sum(t2.b) from t2 where t2.a > t1.a) > 4 and (select sum(t2.b) from t2 where t2.b > t1.b) > 4";
    tk.plan(sql, 1);
    tk.check(sql, true, &["1", "1", "2", "2", "3", "3"]);
    tk.exec("drop table if exists t1, t2");
    tk.exec("create table t1(a int, b int, c varchar(20))");
    tk.exec("create table t2(a int, b int, c varchar(20))");
    tk.exec("insert into t1 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')");
    tk.exec("insert into t2 values (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4'), (1, 1, '1'), (2, 2, '2'), (3, 3, '3'), (4, 4, '4')");
    sql = "select (select min(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b), (select max(t2.a) from t2 where t2.a > t1.a and t2.b > t1.b) from t1 where (select count(*) from t2 where t2.c > t1.c) > 3";
    tk.plan(sql, 1);
    tk.check(sql, true, &["2 4", "2 4", "3 4", "3 4"]);
}

/// Go `TestApplyWithOtherOperators`.
#[test]
fn test_apply_with_other_operators() {
    let mut tk = TestKit::new();
    tk.exec("use test");
    tk.exec("set tidb_enable_parallel_apply=true");
    tk.exec("drop table if exists t1, t2");
    tk.exec("create table t1(a int, b int)");
    tk.exec("create table t2(a int, b int)");
    tk.exec("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    tk.exec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    let mut sql = "select /*+ hash_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a";
    tk.plan(sql, 1);
    tk.check(
        sql,
        true,
        &["0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"],
    );
    tk.exec("drop table if exists t1, t2");
    tk.exec("create table t1(a double, b int)");
    tk.exec("create table t2(a int, b int)");
    tk.exec("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    tk.exec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    sql = "select /*+ merge_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a";
    tk.plan(sql, 1);
    tk.check(
        sql,
        true,
        &["0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"],
    );
    tk.exec("drop table if exists t1, t2");
    tk.exec("create table t1(a int primary key, b int)");
    tk.exec("create table t2(a int, b int, index idx(a))");
    tk.exec("insert into t1 values (1, 1), (2, 2), (3, 3)");
    tk.exec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    sql = "select /*+ inl_merge_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a";
    tk.plan(sql, 1);
    tk.check(sql, true, &["0", "0", "2", "2", "4", "4"]);
    sql = "select /*+ inl_merge_join(t2) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a";
    tk.plan(sql, 1);
    tk.check(sql, true, &["0", "0", "2", "2", "4", "4"]);
    tk.exec("drop table if exists t1, t2");
    tk.exec("create table t1(a int, b int, index idx(a, b))");
    tk.exec("create table t2(a int, b int, index idx(a))");
    tk.exec("insert into t1 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    tk.exec("insert into t2 values (1, 1), (2, 2), (3, 3), (1, 1), (2, 2), (3, 3)");
    sql = "select /*+ inl_hash_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a";
    tk.plan(sql, 1);
    tk.check(
        sql,
        true,
        &["0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"],
    );
    sql = "select /*+ inl_hash_join(t2) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a";
    tk.plan(sql, 1);
    tk.check(
        sql,
        true,
        &["0", "0", "0", "0", "2", "2", "2", "2", "4", "4", "4", "4"],
    );
    tk.exec("drop table if exists t1, t2");
    tk.exec("create table t1(a int , b int, unique index idx(a))");
    tk.exec("create table t2(a int, b int, unique index idx(a))");
    tk.exec("insert into t1 values (1, 1), (2, 2), (3, 3)");
    tk.exec("insert into t2 values (1, 1), (2, 2), (3, 3)");
    sql = "select /*+ inl_join(t1) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a";
    tk.plan(sql, 1);
    tk.check(sql, true, &["0", "1", "2"]);
    sql = "select /*+ inl_join(t2) */ (select count(t2.b) from t2 where t1.a > t2.a) from t1, t2 where t1.a = t2.a";
    tk.plan(sql, 1);
    tk.check(sql, true, &["0", "1", "2"]);
    tk.exec("drop table if exists t");
    tk.exec("create table t(a int, b int, c int, index idxa(a), unique index idxb(b))");
    tk.exec("insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (1, 5, 1), (2, 6, 2), (3, 7, 3), (4, 8, 4)");
    sql = "select /*+ use_index_merge(t) */ * from t where (a > 0 or b < 0) and (select count(*) from t t1 where t1.c > t.a) > 0";
    tk.plan(sql, 1);
    tk.check(
        sql,
        true,
        &["1 1 1", "1 5 1", "2 2 2", "2 6 2", "3 3 3", "3 7 3"],
    );
    tk.exec("drop table if exists t");
    tk.exec("create table t(a int, b int)");
    tk.exec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4), (1, 1), (2, 2), (3, 3), (4, 4)");
    sql = "select /*+ stream_agg() */ a from t where (select count(*) from t1 where t1.b > t.a) > 1 group by a";
    tk.plan(sql, 1);
    tk.check(sql, true, &["1"]);
    sql = "select /*+ hash_agg() */ a from t where (select count(*) from t1 where t1.b > t.a) > 1 group by a";
    tk.plan(sql, 1);
    tk.check(sql, true, &["1"]);
}

/// Go `TestParallelApplyCorrectness`.
#[test]
fn test_parallel_apply_correctness() {
    let mut tk = TestKit::new();
    tk.exec("use test");
    tk.exec("drop table if exists t1;");
    tk.exec(
        "create table t1 (c1 bigint, c2 int, c3 int, c4 int, primary key(c1, c2), index (c3));",
    );
    tk.exec("insert into t1 values(1, 1, 1, 1), (1, 2, 3, 3), (2, 1, 4, 4), (2, 2, 2, 2);");
    tk.exec("set tidb_enable_parallel_apply=true");
    let mut sql = "select (select /*+ NO_DECORRELATE() */ sum(c4) from t1 where t1.c3 = alias.c3) from t1 alias where alias.c1 = 1;";
    tk.check(sql, true, &["1", "3"]);
    tk.exec("set tidb_enable_parallel_apply=false");
    tk.check(sql, true, &["1", "3"]);
}

/// Go `TestApplyConcurrency`.
#[test]
fn test_apply_concurrency() {
    let mut tk = TestKit::new();
    tk.exec("set tidb_enable_parallel_apply=true");
    tk.exec("create table t1(a int,b int)");
    tk.exec("create table t2(a int,b int)");
    let sql = "select * from t1 where t1.b > (select sum(t2.b) from t2 where t2.a > t1.a)";
    for n in [3, 5] {
        tk.exec(&format!("set tidb_executor_concurrency={n}"));
        tk.plan(sql, n);
    }
    tk.exec("create table t(a int)");
    let values = (1..=100)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    tk.exec(&format!("insert into t values {values}"));
    for n in [1, 4, 7, 10] {
        tk.exec(&format!("set tidb_executor_concurrency={n}"));
        tk.check(
            "select sum(a) from t where t.a >= (select max(a) from t t1 where t1.a <= t.a)",
            false,
            &["5050"],
        );
    }
}

/// Go `TestApplyCacheRatio` (Go leaves parallel apply disabled in this test).
#[test]
fn test_apply_cache_ratio() {
    let mut tk = TestKit::new();
    tk.exec("create table t1(a int,b int)");
    tk.exec("create table t2(a int,b int)");
    tk.exec("insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5)");
    let sql =
        "explain analyze select * from t1 where (select min(t2.b) from t2 where t2.a> t1.a) > 10";
    for (values, ratio) in [
        (
            "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(1,1)",
            "10.000%",
        ),
        (
            "(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(2,2),(1,1)",
            "20.000%",
        ),
        (
            "(1,1),(2,2),(3,3),(4,4),(5,5),(1,1),(2,2),(3,3),(4,4),(5,5)",
            "50.000%",
        ),
    ] {
        tk.exec("truncate t1");
        tk.exec(&format!("insert into t1 values {values}"));
        let plan = tk.rows(sql);
        assert!(
            plan.iter()
                .any(|row| row[5].contains(&format!("cacheHitRatio:{ratio}"))),
            "{plan:?}"
        );
        tk.exec("set tidb_mem_quota_apply_cache=0");
        assert!(!tk
            .rows(sql)
            .iter()
            .any(|row| row[5].contains("cacheHitRatio:")));
        tk.exec("set tidb_mem_quota_apply_cache=33554432");
    }
}

/// Go `TestApplyGoroutinePanic`.
#[test]
fn test_apply_goroutine_panic() {
    let mut tk = TestKit::new();
    for sql in [
        "set tidb_enable_parallel_apply=true",
        "create table t1(a int,b int)",
        "create table t2(a int,b int)",
        "insert into t1 values (1,1),(1,1),(2,2),(2,3),(2,3),(1,1),(1,1),(2,2),(2,3),(2,3)",
        "insert into t2 values (2,2),(3,3),(-1,1),(5,4),(2,2),(3,3),(-1,1),(5,4)",
    ] {
        tk.exec(sql);
    }
    let sql = "select (select count(*) from t2 where t2.a > t1.a and t2.b > t1.a) from t1";
    tk.plan(sql, 1);
    tk.check(
        sql,
        true,
        &["4", "4", "4", "4", "4", "4", "6", "6", "6", "6"],
    );
    for name in [
        "parallelApplyInnerWorkerPanic",
        "parallelApplyOuterWorkerPanic",
        "parallelApplyGetCachePanic",
        "parallelApplySetCachePanic",
    ] {
        let _guard = tidb_failpoint::enable_for_test(
            &format!("github.com/pingcap/tidb/pkg/executor/{name}"),
            "panic",
        );
        assert!(tk.session.run(sql).is_err(), "{name}");
    }
}

/// Go `TestParallelApplyCancelInflight`.
#[test]
fn test_parallel_apply_cancel_inflight() {
    let mut tk = TestKit::new();
    for sql in [
        "set tidb_enable_parallel_apply=true",
        "set tidb_executor_concurrency=3",
        "create table t1(a int)",
        "create table t2(a int)",
        "insert into t2 values (1),(2),(3)",
    ] {
        tk.exec(sql);
    }
    for i in 1..=20 {
        tk.exec(&format!("insert into t1 values ({i})"));
    }
    let sql = "select * from (select t1.a from t1 where exists (select /*+ NO_DECORRELATE() */ 1 from t2 where t2.a < t1.a)) sub limit 1";
    tk.plan(sql, 3);
    let _guard = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/parallelApplySlowInner",
        "return(300)",
    );
    let start = Instant::now();
    assert_eq!(tk.rows(sql).len(), 1);
    assert!(start.elapsed() < Duration::from_secs(3));
}

/// Go `TestOrderedParallelApply`.
#[test]
fn test_ordered_parallel_apply() {
    let mut tk = TestKit::new();
    for sql in ["create table t1(a int,b int,index idx_a(a))", "create table t2(a int,b int)",
        "insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50),(6,60),(7,70),(8,80),(9,90),(10,100)",
        "insert into t2 values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)",
        "set tidb_executor_concurrency=5", "set tidb_enable_parallel_apply=true"] { tk.exec(sql); }
    let q1 = "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a";
    tk.plan(q1, 5);
    tk.compare_serial(q1);
    let q2 = "select t1.a, t1.b from t1 where t1.b > (select min(t2.b) from t2 where t2.a >= t1.a) order by t1.a limit 5";
    tk.plan(q2, 5);
    tk.compare_serial(q2);
    let plan = tk.rows(&format!("explain {q2}"));
    let above: Vec<_> = plan
        .iter()
        .take_while(|row| !row[0].contains("Apply"))
        .collect();
    assert!(above.iter().any(|row| row[0].contains("Limit")), "{plan:?}");
    assert!(!above.iter().any(|row| row[0].contains("TopN")), "{plan:?}");
    tk.compare_serial("select t1.a from t1 where exists (select 1 from t2 where t2.a = t1.a and t2.b > 3) order by t1.a");
    for n in [2, 3, 5, 7, 10] {
        tk.exec(&format!("set tidb_executor_concurrency={n}"));
        tk.compare_serial(
            "select t1.a, (select count(*) from t2 where t2.a > t1.a) from t1 order by t1.a",
        );
    }
    tk.exec("set tidb_executor_concurrency=5");
    tk.compare_serial("select t1.a, t1.b from t1 where t1.b > (select min(t2.b) from t2 where t2.a >= t1.a) order by t1.a limit 3 offset 2");
}

/// Go ordered tests' t1/t2 fixture.
fn ordered_fixture() -> TestKit {
    let mut tk = TestKit::new();
    for sql in [
        "set tidb_enable_parallel_apply=true",
        "set tidb_executor_concurrency=4",
        "create table t1(a int,b int,index idx_a(a))",
        "create table t2(a int,b int)",
        "insert into t1 values (1,1),(2,2),(3,3),(4,4),(5,5)",
        "insert into t2 values (1,10),(2,20),(3,30)",
    ] {
        tk.exec(sql);
    }
    tk
}

/// Go `TestOrderedParallelApplyEdgeCases`.
#[test]
fn test_ordered_parallel_apply_edge_cases() {
    let mut tk = ordered_fixture();
    let q1 = "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a > 2 order by t1.a";
    tk.plan(q1, 4);
    tk.compare_serial(q1);
    for sql in [
        "select t1.a from t1 where not exists (select 1 from t2 where t2.a = t1.a) order by t1.a",
        "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a limit 1",
        "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a = 3 order by t1.a",
    ] { tk.compare_serial(sql); }
    tk.check("select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 where t1.a > 999 order by t1.a", false, &[]);
    for sql in [
        "create table t3(a int,index idx_a(a))",
        "create table t4(a int)",
        "insert into t3 values (1),(2),(3),(4),(5)",
        "insert into t4 values (2),(4)",
    ] {
        tk.exec(sql);
    }
    tk.compare_serial("select a, a in (select /*+ NO_DECORRELATE() */ a from t4 where t4.a = t3.a) from t3 order by a");
    tk.exec("create table t5(a int,index idx_a(a))");
    tk.exec("create table t6(a int)");
    let values = (1..=2000)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    tk.exec(&format!("insert into t5 values {values}"));
    tk.exec("insert into t6 values (1),(2),(3)");
    tk.compare_serial(
        "select t5.a, (select count(*) from t6 where t6.a <= t5.a) from t5 order by t5.a",
    );
}

/// Go `TestOrderedParallelApplyLargeInner`.
#[test]
fn test_ordered_parallel_apply_large_inner() {
    let mut tk = TestKit::new();
    for sql in [
        "set tidb_enable_parallel_apply=true",
        "set tidb_executor_concurrency=4",
        "create table t1(a int,index idx_a(a))",
        "create table t2(a int,b int)",
        "insert into t1 values (1),(2),(3)",
    ] {
        tk.exec(sql);
    }
    let values = (1..=2000)
        .map(|i| format!("({},{i})", i % 3 + 1))
        .collect::<Vec<_>>()
        .join(",");
    tk.exec(&format!("insert into t2 values {values}"));
    let q = "select t1.a, (select /*+ NO_DECORRELATE() */ count(*) from t2 where t2.a = t1.a) from t1 order by t1.a";
    tk.plan(q, 4);
    tk.compare_serial(q);
    for sql in [
        "create table t3(a int,index idx_a(a))",
        "create table t4(b int)",
        "insert into t3 values (1),(2),(3)",
    ] {
        tk.exec(sql);
    }
    let values = (1..=500)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(",");
    tk.exec(&format!("insert into t4 values {values}"));
    tk.compare_serial("select t3.a, (select /*+ NO_DECORRELATE() */ sum(t4.b) from t4 where t4.b <= t3.a * 100) from t3 order by t3.a");
}

/// Go `TestOrderedParallelApplyLeftOuterSemiJoin`.
#[test]
fn test_ordered_parallel_apply_left_outer_semi_join() {
    let mut tk = TestKit::new();
    for sql in [
        "set tidb_enable_parallel_apply=true",
        "set tidb_executor_concurrency=4",
        "create table t1(a int,b int,index idx_a(a))",
        "create table t2(a int,b int)",
        "insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50)",
        "insert into t2 values (2,2),(4,4)",
    ] {
        tk.exec(sql);
    }
    for sql in [
        "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 order by a",
        "select a from t1 where a not in (select /*+ NO_DECORRELATE() */ a from t2 where t2.b < t1.b) order by a",
        "select t1.a, (select t2.b from t2 where t2.a = t1.a) from t1 order by t1.a",
    ] { tk.compare_serial(sql); }
    tk.exec("insert into t1 values (null,null),(6,null)");
    for sql in [
        "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 where t1.b is not null order by a",
        "select a, a in (select /*+ NO_DECORRELATE() */ a from t2 where t2.a = t1.a) from t1 where t1.a > 2 order by a",
    ] { tk.compare_serial(sql); }
}

/// Go `TestOrderedParallelApplyGoroutinePanic`.
#[test]
fn test_ordered_parallel_apply_goroutine_panic() {
    let mut tk = ordered_fixture();
    tk.exec("set tidb_executor_concurrency=3");
    let sql = "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a";
    tk.plan(sql, 3);
    tk.check(sql, false, &["1 10", "2 20", "3 30", "4 30", "5 30"]);
    for name in [
        "parallelApplyInnerWorkerOrderedPanic",
        "parallelApplyOuterWorkerPanic",
    ] {
        let _guard = tidb_failpoint::enable_for_test(
            &format!("github.com/pingcap/tidb/pkg/executor/{name}"),
            "panic",
        );
        assert!(tk.session.run(sql).is_err(), "{name}");
    }
}

/// Go `TestOrderedParallelApplyKillSignal`.
#[test]
fn test_ordered_parallel_apply_kill_signal() {
    let mut tk = ordered_fixture();
    tk.exec("set tidb_executor_concurrency=3");
    let sql = "select t1.a, (select max(t2.b) from t2 where t2.a <= t1.a) from t1 order by t1.a";
    tk.plan(sql, 3);
    tk.check(sql, false, &["1 10", "2 20", "3 30", "4 30", "5 30"]);
    let _guard = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/parallelApplyOrderedSleep",
        "return(500)",
    );
    let killer = tk.session.sql_killer();
    let handle = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        killer.send_kill_signal(tidb_sqlkiller::KillSignal::QueryInterrupted);
    });
    let start = Instant::now();
    let error = tk.session.run(sql).unwrap_err();
    assert_eq!(error.to_mysql_error().code, 1317);
    assert!(start.elapsed() < Duration::from_secs(2));
    handle.join().unwrap();
}

/// Go `TestOrderedParallelApplyNested`.
#[test]
fn test_ordered_parallel_apply_nested() {
    let mut tk = TestKit::new();
    for sql in [
        "create table t1(a int,b int,index idx_a(a))",
        "create table t2(a int,b int)",
        "create table t3(a int,b int)",
        "insert into t1 values (1,10),(2,20),(3,30),(4,40),(5,50)",
        "insert into t2 values (1,100),(2,200),(3,300)",
        "insert into t3 values (1,1000),(2,2000),(3,3000),(4,4000)",
        "set tidb_executor_concurrency=3",
        "set tidb_enable_parallel_apply=true",
    ] {
        tk.exec(sql);
    }
    let q1 = "select t1.a, (select /*+ NO_DECORRELATE() */ max(t2.b) from t2 where t2.a = t1.a), (select /*+ NO_DECORRELATE() */ max(t3.b) from t3 where t3.a = t1.a) from t1 order by t1.a";
    let plan = tk.rows(&format!("explain analyze {q1}"));
    let applies: Vec<_> = plan.iter().filter(|row| row[0].contains("Apply")).collect();
    assert_eq!(applies.len(), 2, "{plan:?}");
    assert!(
        applies.iter().all(|row| row[5].contains("Concurrency:3")),
        "{plan:?}"
    );
    tk.compare_serial(q1);
    let q2 = "select t1.a, (select /*+ NO_DECORRELATE() */ max(t2.b) + (select /*+ NO_DECORRELATE() */ max(t3.b) from t3 where t3.a = t2.a) from t2 where t2.a = t1.a) from t1 order by t1.a";
    let plan = tk.rows(&format!("explain analyze {q2}"));
    let applies: Vec<_> = plan.iter().filter(|row| row[0].contains("Apply")).collect();
    assert_eq!(
        applies
            .iter()
            .filter(|row| row[5].contains("Concurrency:"))
            .count(),
        1,
        "{plan:?}"
    );
    assert_eq!(
        applies
            .iter()
            .filter(|row| row[5].contains("concurrency:OFF"))
            .count(),
        1,
        "{plan:?}"
    );
    tk.compare_serial(q2);
    tk.check(
        &format!("{q1} limit 3"),
        false,
        &["1 100 1000", "2 200 2000", "3 300 3000"],
    );
    for n in [1, 2, 5] {
        tk.exec(&format!("set tidb_executor_concurrency={n}"));
        tk.compare_serial(q1);
    }
}

/// Go `zzprobe/c02.TestC02`: a synchronized session allocator, parallel
/// Apply at concurrency 3, three distinct cache keys (27 bytes), and one
/// retained outer chunk (380 bytes). Result/free/temporary chunks do not
/// charge the Apply tracker. Both ordered and unordered use these lists.
#[test]
fn a_parallel_apply_charges_gos_outer_list_and_shared_cache() {
    for order in ["", " order by a"] {
        let mut tk = TestKit::new();
        for sql in [
            "create table t(a int,b int)",
            "create table s(x int,y int)",
            "insert into t values(1,1),(2,2),(3,3)",
            "insert into s values(1,10),(2,20),(3,30)",
            "set tidb_enable_parallel_apply=1",
            "set tidb_executor_concurrency=3",
        ] {
            tk.exec(sql);
        }
        let sql = format!("explain analyze select a,(select y from s where s.x=t.a) from t{order}");
        let plan = tk.rows(&sql);
        let row = plan.iter().find(|row| row[0].contains("Apply")).unwrap();
        assert!(row[5].contains("Concurrency:3"), "{plan:?}");
        assert_eq!(row[7], "407 Bytes", "{plan:?}");
    }
}
