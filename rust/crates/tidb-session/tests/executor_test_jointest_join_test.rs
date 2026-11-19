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

//! Go `pkg/executor/test/jointest/join_test.go`, on the mock store through the session, as Go's testkit
//! runs it.

#![allow(unused_mut, unused_variables)]

use crate::support::testkit::TestKit;

/// Go `TestIssue11895`.
#[test]
fn test_issue11895() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("drop table if exists t1;");
    tk.must_exec("create table t(c1 bigint unsigned);");
    tk.must_exec("create table t1(c1 bit(64));");
    tk.must_exec("insert into t value(18446744073709551615);");
    tk.must_exec("insert into t1 value(-1);");
    tk.must_query("select t.c1, hex(t1.c1) from t, t1 where t.c1 = t1.c1;")
        .check(["18446744073709551615 FFFFFFFFFFFFFFFF"]);
}

/// Go `TestIssue11896`.
#[test]
fn test_issue11896() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("drop table if exists t1;");
    tk.must_exec("create table t(c1 bigint);");
    tk.must_exec("create table t1(c1 bit(64));");
    tk.must_exec("insert into t value(1);");
    tk.must_exec("insert into t1 value(1);");
    tk.must_query("select t.c1, hex(t1.c1) from t, t1 where t.c1 = t1.c1;").check(["1 1"]);

    tk.must_exec("drop table if exists t;");
    tk.must_exec("drop table if exists t1;");
    tk.must_exec("create table t(c1 bigint);");
    tk.must_exec("create table t1(c1 bit(64));");
    tk.must_exec("insert into t value(-1);");
    tk.must_exec("insert into t1 value(18446744073709551615);");
    tk.must_query("select * from t, t1 where t.c1 = t1.c1;").check_empty();
}

/// Go `TestSingleTaskIncrementalIndexHashJoin`.
#[test]
fn test_single_task_incremental_index_hash_join() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1(a int primary key)");
    tk.must_exec("create table t2(b int not null, c varchar(100), index idx_b(b))");
    // 9 rows in t1, a is 2-10.
    let t1 = (2..=10).map(|i| format!("({i})")).collect::<Vec<_>>().join(",");
    tk.must_exec(&format!("insert into t1 values {t1}"));
    // 9000 rows in t2, b is 0-9.
    let t2 = (1..=9000).map(|i| format!("({}, 'abc')", i / 1000)).collect::<Vec<_>>().join(",");
    tk.must_exec(&format!("insert into t2 values {t2}"));

    tk.must_query("select /*+ inl_hash_join(t1,t2) */ * from t1 inner join t2 on t1.a = t2.b");
    tk.must_query("select /*+ inl_hash_join(t1,t2) */ count(*) from t1 inner join t2 on t1.a = t2.b")
        .check(["7001"]);
    tk.must_query("select /*+ inl_hash_join(t1,t2) */ * from t1 left join t2 on t1.a = t2.b");
    tk.must_query("select /*+ inl_hash_join(t1,t2) */ count(*) from t1 left join t2 on t1.a = t2.b")
        .check(["7002"]);
    tk.must_query("select /*+ inl_hash_join(t2,t1) */ * from t2 right join t1 on t1.a = t2.b");
    tk.must_query("select /*+ inl_hash_join(t2,t1) */ count(*) from t2 right join t1 on t1.a = t2.b")
        .check(["7002"]);
    tk.must_query(
        "select /*+ inl_hash_join(t2,t1) */ * from t1 where t1.a not in (select t2.b from t2 where t2.b = t1.a)",
    );
    tk.must_query(
        "select /*+ inl_hash_join(t2,t1) */ count(*) from t1 where t1.a not in (select t2.b from t2 where t2.b = t1.a)",
    )
    .check(["1"]);
}

/// Go `TestJoin2`.
#[test]
fn test_join2() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@tidb_index_lookup_join_concurrency = 200");
    tk.must_query("select @@tidb_index_lookup_join_concurrency").check(["200"]);
    tk.must_exec("set @@tidb_index_lookup_join_concurrency = 4");
    tk.must_query("select @@tidb_index_lookup_join_concurrency").check(["4"]);
    tk.must_exec("set @@tidb_index_lookup_size = 2");
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (c int)");
    tk.must_exec("insert t values (1)");
    for (sql, result) in [
        ("select 1 from t as a left join t as b on 0", ["1"]),
        ("select 1 from t as a join t as b on 1", ["1"]),
    ] {
        tk.must_query(sql).check(result);
    }
    tk.must_exec("drop table if exists t");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t(c1 int, c2 int)");
    tk.must_exec("create table t1(c1 int, c2 int)");
    tk.must_exec("insert into t values(1,1),(2,2)");
    tk.must_exec("insert into t1 values(2,3),(4,4)");
    let mut result = tk.must_query("select * from t left outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20");
    result.check(["1 1 <nil> <nil>"]);
    result = tk.must_query("select * from t1 right outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20");
    result.check(["<nil> <nil> 1 1"]);
    result = tk.must_query("select * from t right outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20");
    result.check_empty();
    result = tk.must_query("select * from t left outer join t1 on t.c1 = t1.c1 where t1.c1 = 3 or false");
    result.check_empty();
    result = tk.must_query("select * from t left outer join t1 on t.c1 = t1.c1 and t.c1 != 1 order by t1.c1");
    result.check(["1 1 <nil> <nil>", "2 2 2 3"]);
    result = tk.must_query("select t.c1, t1.c1 from t left outer join t1 on t.c1 = t1.c1 and t.c2 + t1.c2 <= 5 order by t.c1");
    result.check(["1 <nil>", "2 2"]);
    tk.must_exec("drop table if exists t1");
    tk.must_exec("drop table if exists t2");
    tk.must_exec("drop table if exists t3");
    tk.must_exec("create table t1 (c1 int, c2 int)");
    tk.must_exec("create table t2 (c1 int, c2 int)");
    tk.must_exec("create table t3 (c1 int, c2 int)");
    tk.must_exec("insert into t1 values (1,1), (2,2), (3,3)");
    tk.must_exec("insert into t2 values (1,1), (3,3), (5,5)");
    tk.must_exec("insert into t3 values (1,1), (5,5), (9,9)");
    result = tk.must_query("select * from t1 left join t2 on t1.c1 = t2.c1 right join t3 on t2.c1 = t3.c1 order by t1.c1, t1.c2, t2.c1, t2.c2, t3.c1, t3.c2;");
    result.check(["<nil> <nil> <nil> <nil> 5 5", "<nil> <nil> <nil> <nil> 9 9", "1 1 1 1 1 1"]);
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1 (c1 int)");
    tk.must_exec("insert into t1 values (1), (1), (1)");
    result = tk.must_query("select * from t1 a join t1 b on a.c1 = b.c1;");
    result.check(["1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1"]);
    tk.must_exec("drop table if exists t");
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t(c1 int, index k(c1))");
    tk.must_exec("create table t1(c1 int)");
    tk.must_exec("insert into t values (1),(2),(3),(4),(5),(6),(7)");
    tk.must_exec("insert into t1 values (1),(2),(3),(4),(5),(6),(7)");
    result = tk.must_query("select a.c1 from t a , t1 b where a.c1 = b.c1 order by a.c1;");
    result.check(["1", "2", "3", "4", "5", "6", "7"]);
    // Test race.
    result = tk.must_query("select a.c1 from t a , t1 b where a.c1 = b.c1 and a.c1 + b.c1 > 5 order by b.c1");
    result.check(["3", "4", "5", "6", "7"]);
    result = tk.must_query("select a.c1 from t a , (select * from t1 limit 3) b where a.c1 = b.c1 order by b.c1;");
    result.check(["1", "2", "3"]);
    tk.must_exec("drop table if exists t,t2,t1");
    tk.must_exec("create table t(c1 int)");
    tk.must_exec("create table t1(c1 int, c2 int)");
    tk.must_exec("create table t2(c1 int, c2 int)");
    tk.must_exec("insert into t1 values(1,2),(2,3),(3,4)");
    tk.must_exec("insert into t2 values(1,0),(2,0),(3,0)");
    tk.must_exec("insert into t values(1),(2),(3)");
    result = tk.must_query("select * from t1 , t2 where t2.c1 = t1.c1 and t2.c2 = 0 and t1.c2 in (select * from t)");
    result.sort().check(["1 2 1 0", "2 3 2 0"]);
    result = tk.must_query("select * from t1 , t2 where t2.c1 = t1.c1 and t2.c2 = 0 and t1.c1 = 1 order by t1.c2 limit 1");
    result.sort().check(["1 2 1 0"]);
    tk.must_exec("drop table if exists t, t1");
    tk.must_exec("create table t(a int primary key, b int)");
    tk.must_exec("create table t1(a int, b int, key s(b))");
    tk.must_exec("insert into t values(1, 1), (2, 2), (3, 3)");
    tk.must_exec("insert into t1 values(1, 2), (1, 3), (1, 4), (3, 4), (4, 5)");
    // The physical plans of the two sql are tested at physical_plan_test.go
    tk.must_query("select /*+ INL_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").check(["1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"]);
    tk.must_query("select /*+ INL_HASH_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").sort().check(["1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"]);
    tk.must_query("select /*+ INL_MERGE_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").check(["1 1 1 4", "1 1 1 3", "1 1 1 2", "3 3 3 4"]);
    tk.must_query("select /*+ INL_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").check(["1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"]);
    tk.must_query("select /*+ INL_HASH_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").sort().check(["1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"]);
    tk.must_query("select /*+ INL_MERGE_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").check(["1 4 1 1", "1 3 1 1", "1 2 1 1", "3 4 3 3"]);
    // Test single index reader.
    tk.must_query("select /*+ INL_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").check(["2", "3"]);
    tk.must_query("select /*+ INL_HASH_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").sort().check(["2", "3"]);
    tk.must_query("select /*+ INL_MERGE_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").check(["2", "3"]);
    tk.must_query("select /*+ INL_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").sort().check(["1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"]);
    tk.must_query("select /*+ INL_HASH_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").sort().check(["1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"]);
    tk.must_query("select /*+ INL_MERGE_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").sort().check(["1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"]);
    tk.must_query("select /*+ INL_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").check(["1.5000"]);
    tk.must_query("select /*+ INL_HASH_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").check(["1.5000"]);
    tk.must_query("select /*+ INL_MERGE_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").check(["1.5000"]);
    // Test that two conflict hints will return warning.
    tk.must_exec("select /*+ TIDB_INLJ(t) TIDB_SMJ(t) */ * from t join t1 on t.a=t1.a");
    assert_eq!(tk.session().warnings().len(), 1);
    tk.must_exec("select /*+ TIDB_INLJ(t) TIDB_HJ(t) */ * from t join t1 on t.a=t1.a");
    assert_eq!(tk.session().warnings().len(), 1);
    tk.must_exec("select /*+ TIDB_SMJ(t) TIDB_HJ(t) */ * from t join t1 on t.a=t1.a");
    assert_eq!(tk.session().warnings().len(), 1);
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a int)");
    tk.must_exec("insert into t values(1),(2), (3)");
    tk.must_query("select @a := @a + 1 from t, (select @a := 0) b;").check(["1", "2", "3"]);
    tk.must_exec("drop table if exists t, t1");
    tk.must_exec("create table t(a int primary key, b int, key s(b))");
    tk.must_exec("create table t1(a int, b int)");
    tk.must_exec("insert into t values(1, 3), (2, 2), (3, 1)");
    tk.must_exec("insert into t1 values(0, 0), (1, 2), (1, 3), (3, 4)");
    tk.must_query("select /*+ INL_JOIN(t1) */ * from t join t1 on t.a=t1.a order by t.b").sort().check(["1 3 1 2", "1 3 1 3", "3 1 3 4"]);
    tk.must_query("select /*+ INL_HASH_JOIN(t1) */ * from t join t1 on t.a=t1.a order by t.b").sort().check(["1 3 1 2", "1 3 1 3", "3 1 3 4"]);
    tk.must_query("select /*+ INL_MERGE_JOIN(t1) */ * from t join t1 on t.a=t1.a order by t.b").sort().check(["1 3 1 2", "1 3 1 3", "3 1 3 4"]);
    tk.must_query("select /*+ INL_JOIN(t) */ t.a, t.b from t join t1 on t.a=t1.a where t1.b = 4 limit 1").check(["3 1"]);
    tk.must_query("select /*+ INL_HASH_JOIN(t) */ t.a, t.b from t join t1 on t.a=t1.a where t1.b = 4 limit 1").check(["3 1"]);
    tk.must_query("select /*+ INL_MERGE_JOIN(t) */ t.a, t.b from t join t1 on t.a=t1.a where t1.b = 4 limit 1").check(["3 1"]);
    tk.must_query("select /*+ INL_JOIN(t, t1) */ * from t right join t1 on t.a=t1.a order by t.b").sort().check(["1 3 1 2", "1 3 1 3", "3 1 3 4", "<nil> <nil> 0 0"]);
    tk.must_query("select /*+ INL_HASH_JOIN(t, t1) */ * from t right join t1 on t.a=t1.a order by t.b").sort().check(["1 3 1 2", "1 3 1 3", "3 1 3 4", "<nil> <nil> 0 0"]);
    tk.must_query("select /*+ INL_MERGE_JOIN(t, t1) */ * from t right join t1 on t.a=t1.a order by t.b").sort().check(["1 3 1 2", "1 3 1 3", "3 1 3 4", "<nil> <nil> 0 0"]);
    // join reorder will disorganize the resulting schema
    tk.must_exec("drop table if exists t, t1");
    tk.must_exec("create table t(a int, b int)");
    tk.must_exec("create table t1(a int, b int)");
    tk.must_exec("insert into t values(1,2)");
    tk.must_exec("insert into t1 values(3,4)");
    tk.must_query("select (select t1.a from t1 , t where t.a = s.a limit 2) from t as s").check(["3"]);
    // test index join bug
    tk.must_exec("drop table if exists t, t1");
    tk.must_exec("create table t(a int, b int, key s1(a,b), key s2(b))");
    tk.must_exec("create table t1(a int)");
    tk.must_exec("insert into t values(1,2), (5,3), (6,4)");
    tk.must_exec("insert into t1 values(1), (2), (3)");
    tk.must_query("select /*+ INL_JOIN(t) */ t1.a from t1, t where t.a = 5 and t.b = t1.a").check(["3"]);
    tk.must_query("select /*+ INL_HASH_JOIN(t) */ t1.a from t1, t where t.a = 5 and t.b = t1.a").check(["3"]);
    tk.must_query("select /*+ INL_MERGE_JOIN(t) */ t1.a from t1, t where t.a = 5 and t.b = t1.a").check(["3"]);
    // test issue#4997
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec(r#" CREATE TABLE t1 ( pk int(11) NOT NULL AUTO_INCREMENT primary key, a int(11) DEFAULT NULL, b date DEFAULT NULL, c varchar(1) DEFAULT NULL, KEY a (a), KEY b (b), KEY c (c,a) )"#);
    tk.must_exec(r#" CREATE TABLE t2 ( pk int(11) NOT NULL AUTO_INCREMENT primary key, a int(11) DEFAULT NULL, b date DEFAULT NULL, c varchar(1) DEFAULT NULL, KEY a (a), KEY b (b), KEY c (c,a) )"#);
    tk.must_exec(r#"insert into t1 value(1,1,"2000-11-11", null);"#);
    result = tk.must_query(r#" SELECT table2.b AS field2 FROM ( t1 AS table1  LEFT OUTER JOIN (SELECT tmp_t2.* FROM ( t2 AS tmp_t1 RIGHT JOIN t1 AS tmp_t2 ON (tmp_t2.a = tmp_t1.a))) AS table2 ON (table2.c = table1.c) ) "#);
    result.check(["<nil>"]);
    // test virtual rows are included (issue#5771)
    result = tk.must_query(r#"SELECT 1 FROM (SELECT 1) t1, (SELECT 1) t2"#);
    result.check(["1"]);
    result = tk.must_query(r#" SELECT @NUM := @NUM + 1 as NUM FROM ( SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 ) a INNER JOIN ( SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 ) b, (SELECT @NUM := 0) d; "#);
    result.check(["1", "2", "3", "4", "5", "6", "7", "8", "9"]);
    // This case is for testing:
    // when the main thread calls Executor.Close() while the out data fetch worker and join workers are still working,
    // we need to stop the goroutines as soon as possible to avoid unexpected error.
    tk.must_exec("set @@tidb_hash_join_concurrency=5");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t(a int)");
    for _ in 0..100 {
    tk.must_exec("insert into t value(1)");
    }
    result = tk.must_query("select /*+ TIDB_HJ(s, r) */ * from t as s join t as r on s.a = r.a limit 1;");
    result.check(["1 1"]);
    tk.must_exec("drop table if exists user, aa, bb");
    tk.must_exec("create table aa(id int)");
    tk.must_exec("insert into aa values(1)");
    tk.must_exec("create table bb(id int)");
    tk.must_exec("insert into bb values(1)");
    tk.must_exec("create table user(id int, name varchar(20))");
    tk.must_exec("insert into user values(1, 'a'), (2, 'b')");
    tk.must_query("select user.id,user.name from user left join aa on aa.id = user.id left join bb on aa.id = bb.id where bb.id < 10;").check(["1 a"]);
    tk.must_exec(r#"drop table if exists t;"#);
    tk.must_exec(r#"create table t (a bigint);"#);
    tk.must_exec(r#"insert into t values (1);"#);
    tk.must_query(r#"select t2.a, t1.a from t t1 inner join (select "1" as a) t2 on t2.a = t1.a;"#).check(["1 1"]);
    tk.must_query(r#"select t2.a, t1.a from t t1 inner join (select "2" as b, "1" as a) t2 on t2.a = t1.a;"#).check(["1 1"]);
    tk.must_exec("drop table if exists t1, t2, t3, t4");
    tk.must_exec("create table t1(a int, b int)");
    tk.must_exec("create table t2(a int, b int)");
    tk.must_exec("create table t3(a int, b int)");
    tk.must_exec("create table t4(a int, b int)");
    tk.must_exec("insert into t1 values(1, 1)");
    tk.must_exec("insert into t2 values(1, 1)");
    tk.must_exec("insert into t3 values(1, 1)");
    tk.must_exec("insert into t4 values(1, 1)");
    tk.must_query("select min(t2.b) from t1 right join t2 on t2.a=t1.a right join t3 on t2.a=t3.a left join t4 on t3.a=t4.a").check(["1"]);
}
