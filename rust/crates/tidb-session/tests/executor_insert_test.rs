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

//! Go `pkg/executor/insert_test.go`, on the mock store through the session, as Go's testkit
//! runs it.

use crate::support::testkit::*;

/// Go `TestInsertOnDuplicateKeyWithBinlog`.
#[test]
fn test_insert_on_duplicate_key_with_binlog() {
    let mut tk = TestKit::new();
    test_insert_on_duplicate_key(&mut tk);
}

/// Go `testInsertOnDuplicateKey`.
fn test_insert_on_duplicate_key(tk: &mut TestKit) {
    tk.must_exec("use test");
    tk.must_exec(r#"drop table if exists t1, t2;"#);
    tk.must_exec(r#"create table t1(a1 bigint primary key, b1 bigint);"#);
    tk.must_exec(r#"create table t2(a2 bigint primary key, b2 bigint);"#);
    tk.must_exec(r#"insert into t1 values(1, 100);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_exec(r#"insert into t2 values(1, 200);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_exec(r#"insert into t1 select a2, b2 from t2 on duplicate key update b1 = a2;"#);
    assert_eq!(tk.affected_rows(), 2);
    tk.check_last_message("Records: 1  Duplicates: 1  Warnings: 0");
    tk.must_query(r#"select * from t1;"#).check(["1 1"]);
    tk.must_exec(r#"insert into t1 select a2, b2 from t2 on duplicate key update b1 = b2;"#);
    assert_eq!(tk.affected_rows(), 2);
    tk.check_last_message("Records: 1  Duplicates: 1  Warnings: 0");
    tk.must_query(r#"select * from t1;"#).check(["1 200"]);
    tk.must_exec(r#"insert into t1 select a2, b2 from t2 on duplicate key update a1 = a2;"#);
    assert_eq!(tk.affected_rows(), 0);
    tk.check_last_message("Records: 1  Duplicates: 0  Warnings: 0");
    tk.must_query(r#"select * from t1;"#).check(["1 200"]);
    tk.must_exec(r#"insert into t1 select a2, b2 from t2 on duplicate key update b1 = 300;"#);
    assert_eq!(tk.affected_rows(), 2);
    tk.check_last_message("Records: 1  Duplicates: 1  Warnings: 0");
    tk.must_query(r#"select * from t1;"#).check(["1 300"]);
    tk.must_exec(r#"insert into t1 values(1, 1) on duplicate key update b1 = 400;"#);
    assert_eq!(tk.affected_rows(), 2);
    tk.check_last_message("");
    tk.must_query(r#"select * from t1;"#).check(["1 400"]);
    tk.must_exec(r#"insert into t1 select 1, 500 from t2 on duplicate key update b1 = 400;"#);
    assert_eq!(tk.affected_rows(), 0);
    tk.check_last_message("Records: 1  Duplicates: 0  Warnings: 0");
    tk.must_query(r#"select * from t1;"#).check(["1 400"]);
    tk.must_exec(r#"drop table if exists t1, t2;"#);
    tk.must_exec(r#"create table t1(a bigint primary key, b bigint);"#);
    tk.must_exec(r#"create table t2(a bigint primary key, b bigint);"#);
    tk.must_get_err_msg(
        r#"insert into t1 select * from t2 on duplicate key update c = t2.b;"#,
        r#"[planner:1054]Unknown column 'c' in 'field list'"#,
    );
    tk.must_exec(r#"drop table if exists t1, t2;"#);
    tk.must_exec(r#"create table t1(a bigint primary key, b bigint);"#);
    tk.must_exec(r#"create table t2(a bigint primary key, b bigint);"#);
    tk.must_get_err_msg(
        r#"insert into t1 select * from t2 on duplicate key update a = b;"#,
        r#"[planner:1052]Column 'b' in field list is ambiguous"#,
    );
    tk.must_exec(r#"drop table if exists t1, t2;"#);
    tk.must_exec(r#"create table t1(a bigint primary key, b bigint);"#);
    tk.must_exec(r#"create table t2(a bigint primary key, b bigint);"#);
    tk.must_get_err_msg(
        r#"insert into t1 select * from t2 on duplicate key update c = b;"#,
        r#"[planner:1054]Unknown column 'c' in 'field list'"#,
    );
    tk.must_exec(r#"drop table if exists t1, t2;"#);
    tk.must_exec(r#"create table t1(a1 bigint primary key, b1 bigint);"#);
    tk.must_exec(r#"create table t2(a2 bigint primary key, b2 bigint);"#);
    tk.must_get_err_msg(
        r#"insert into t1 select * from t2 on duplicate key update a1 = values(b2);"#,
        r#"[planner:1054]Unknown column 'b2' in 'field list'"#,
    );
    tk.must_exec(r#"drop table if exists t1, t2;"#);
    tk.must_exec(r#"create table t1(a1 bigint primary key, b1 bigint);"#);
    tk.must_exec(r#"create table t2(a2 bigint primary key, b2 bigint);"#);
    tk.must_exec(r#"insert into t1 values(1, 100);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_exec(r#"insert into t2 values(1, 200);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_exec(
        r#"insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;"#,
    );
    assert_eq!(tk.affected_rows(), 2);
    tk.check_last_message("Records: 1  Duplicates: 1  Warnings: 0");
    tk.must_query(r#"select * from t1"#).check(["1 400"]);
    tk.must_exec(
        r#"insert into t1 select * from t2 on duplicate key update b1 = values(b1) + b2;"#,
    );
    assert_eq!(tk.affected_rows(), 0);
    tk.check_last_message("Records: 1  Duplicates: 0  Warnings: 0");
    tk.must_query(r#"select * from t1"#).check(["1 400"]);
    tk.must_exec(r#"drop table if exists t;"#);
    tk.must_exec(r#"create table t(k1 bigint, k2 bigint, val bigint, primary key(k1, k2));"#);
    tk.must_exec(r#"insert into t (val, k1, k2) values (3, 1, 2);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_query(r#"select * from t;"#).check([r#"1 2 3"#]);
    tk.must_exec(r#"insert into t (val, k1, k2) select c, a, b from (select 1 as a, 2 as b, 4 as c) tmp on duplicate key update val = tmp.c;"#);
    assert_eq!(tk.affected_rows(), 2);
    tk.check_last_message("Records: 1  Duplicates: 1  Warnings: 0");
    tk.must_query(r#"select * from t;"#).check([r#"1 2 4"#]);
    tk.must_exec(r#"drop table if exists t;"#);
    tk.must_exec(r#"create table t(k1 double, k2 double, v double, primary key(k1, k2));"#);
    tk.must_exec(r#"insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("Records: 1  Duplicates: 0  Warnings: 0");
    tk.must_query(r#"select * from t;"#).check([r#"1 2 3"#]);
    tk.must_exec(r#"insert into t (v, k1, k2) select c, a, b from (select "3" c, "1" a, "2" b) tmp on duplicate key update v=c;"#);
    assert_eq!(tk.affected_rows(), 0);
    tk.check_last_message("Records: 1  Duplicates: 0  Warnings: 0");
    tk.must_query(r#"select * from t;"#).check([r#"1 2 3"#]);
    tk.must_exec(r#"drop table if exists t1, t2;"#);
    tk.must_exec(r#"create table t1(id int, a int, b int);"#);
    tk.must_exec(r#"insert into t1 values (1, 1, 1);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_exec(r#"insert into t1 values (2, 2, 1);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_exec(r#"insert into t1 values (3, 3, 1);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_exec(r#"create table t2(a int primary key, b int, unique(b));"#);
    tk.must_exec(
        r#"insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;"#,
    );
    assert_eq!(tk.affected_rows(), 5);
    tk.check_last_message("Records: 3  Duplicates: 2  Warnings: 0");
    tk.must_query(r#"select * from t2 order by a;"#)
        .check([r#"3 1"#]);
    tk.must_exec(r#"drop table if exists t1, t2;"#);
    tk.must_exec(r#"create table t1(id int, a int, b int);"#);
    tk.must_exec(r#"insert into t1 values (1, 1, 1);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_exec(r#"insert into t1 values (2, 1, 2);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_exec(r#"insert into t1 values (3, 3, 1);"#);
    assert_eq!(tk.affected_rows(), 1);
    tk.check_last_message("");
    tk.must_exec(r#"create table t2(a int primary key, b int, unique(b));"#);
    tk.must_exec(
        r#"insert into t2 select a, b from t1 order by id on duplicate key update a=t1.a, b=t1.b;"#,
    );
    assert_eq!(tk.affected_rows(), 4);
    tk.check_last_message("Records: 3  Duplicates: 1  Warnings: 0");
    tk.must_query(r#"select * from t2 order by a;"#)
        .check([r#"1 2"#, r#"3 1"#]);
    tk.must_exec(r#"drop table if exists t1, t2;"#);
    tk.must_exec(r#"create table t1(id int, a int, b int, c int);"#);
    tk.must_exec(r#"insert into t1 values (1, 1, 1, 1);"#);
    tk.must_exec(r#"insert into t1 values (2, 2, 1, 2);"#);
    tk.must_exec(r#"insert into t1 values (3, 3, 2, 2);"#);
    tk.must_exec(r#"insert into t1 values (4, 4, 2, 2);"#);
    tk.must_exec(r#"create table t2(a int primary key, b int, c int, unique(b), unique(c));"#);
    tk.must_exec(r#"insert into t2 select a, b, c from t1 order by id on duplicate key update b=t2.b, c=t2.c;"#);
    assert_eq!(tk.affected_rows(), 2);
    tk.check_last_message("Records: 4  Duplicates: 0  Warnings: 0");
    tk.must_query(r#"select * from t2 order by a;"#)
        .check([r#"1 1 1"#, r#"3 2 2"#]);
    tk.must_exec(r#"drop table if exists t1"#);
    tk.must_exec(r#"create table t1(a int primary key, b int);"#);
    tk.must_exec(r#"insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);"#);
    assert_eq!(tk.affected_rows(), 5);
    tk.check_last_message("Records: 5  Duplicates: 0  Warnings: 0");
    tk.must_exec(
        r#"insert into t1 values(4,14),(5,15),(6,16),(7,17),(8,18) on duplicate key update b=b+10"#,
    );
    assert_eq!(tk.affected_rows(), 7);
    tk.check_last_message("Records: 5  Duplicates: 2  Warnings: 0");
    tk.must_exec("drop table if exists a, b");
    tk.must_exec("create table a(x int primary key)");
    tk.must_exec("create table b(x int, y int)");
    tk.must_exec("insert into a values(1)");
    tk.must_exec("insert into b values(1, 2)");
    tk.must_exec("insert into a select x from b ON DUPLICATE KEY UPDATE a.x=b.y");
    assert_eq!(tk.affected_rows(), 2);
    tk.must_query("select * from a").check(["2"]);
    // Test issue 28078.
    // Use different types of columns so that there's likely to be error if the types mismatches.
    tk.must_exec("drop table if exists a, b");
    tk.must_exec("create table a(id int, a1 timestamp, a2 varchar(10), a3 float, unique(id))");
    tk.must_exec("create table b(id int, b1 time, b2 varchar(10), b3 int)");
    tk.must_exec("insert into a values (1, '2022-01-04 07:02:04', 'a', 1.1), (2, '2022-01-04 07:02:05', 'b', 2.2)");
    tk.must_exec("insert into b values (2, '12:34:56', 'c', 10), (3, '01:23:45', 'd', 20)");
    tk.must_exec(
        "insert into a (id) select id from b on duplicate key update a.a2 = b.b2, a.a3 = 3.3",
    );
    assert_eq!(tk.affected_rows(), 3);
    tk.must_query("select * from a").check_with_sep(
        "/",
        [
            "1/2022-01-04 07:02:04/a/1.1",
            "2/2022-01-04 07:02:05/c/3.3",
            "3/<nil>/<nil>/<nil>",
        ],
    );
    tk.must_exec(
        "insert into a (id) select 4 from b where b3 = 20 on duplicate key update a.a3 = b.b3",
    );
    assert_eq!(tk.affected_rows(), 1);
    tk.must_query("select * from a").check_with_sep(
        "/",
        [
            "1/2022-01-04 07:02:04/a/1.1",
            "2/2022-01-04 07:02:05/c/3.3",
            "3/<nil>/<nil>/<nil>",
            "4/<nil>/<nil>/<nil>",
        ],
    );
    tk.must_exec(
        "insert into a (a2, a3) select 'x', 1.2 from b on duplicate key update a.a2 = b.b3",
    );
    assert_eq!(tk.affected_rows(), 2);
    tk.must_query("select * from a").check_with_sep(
        "/",
        [
            "1/2022-01-04 07:02:04/a/1.1",
            "2/2022-01-04 07:02:05/c/3.3",
            "3/<nil>/<nil>/<nil>",
            "4/<nil>/<nil>/<nil>",
            "<nil>/<nil>/x/1.2",
            "<nil>/<nil>/x/1.2",
        ],
    );
    // Test issue 56829
    tk.must_exec(r#" CREATE TABLE cache ( cache_key varchar(512) NOT NULL, updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, expired_at datetime GENERATED ALWAYS AS (if(expires > 0, date_add(updated_at, interval expires second), date_add(updated_at, interval 99 year))) VIRTUAL, expires int(11), PRIMARY KEY (cache_key) /*T![clustered_index] CLUSTERED */, KEY idx_c_on_expired_at (expired_at) )"#);
    tk.must_exec("INSERT INTO cache(cache_key, expires) VALUES ('2001-01-01 11:11:11', 60) ON DUPLICATE KEY UPDATE expires = expires + 1");
    tk.must_exec("select sleep(1)");
    tk.must_exec("INSERT INTO cache(cache_key, expires) VALUES ('2001-01-01 11:11:11', 60) ON DUPLICATE KEY UPDATE expires = expires + 1");
    tk.must_exec("admin check table cache");
    let mut rs1 =
        tk.must_query("select cache_key, expired_at from cache use index() order by cache_key");
    let mut rs2 = tk.must_query(
        "select cache_key, expired_at from cache use index(idx_c_on_expired_at) order by cache_key",
    );
    assert_eq!(rs1.rows(), rs2.rows());
    // reproduce insert on duplicate key update bug under new row format.
    tk.must_exec(r#"drop table if exists t1"#);
    tk.must_exec(r#"create table t1(c1 decimal(6,4), primary key(c1))"#);
    tk.must_exec(r#"insert into t1 set c1 = 0.1"#);
    tk.must_exec(r#"insert into t1 set c1 = 0.1 on duplicate key update c1 = 1"#);
    tk.must_query(r#"select * from t1 use index(primary)"#)
        .check([r#"1.0000"#]);
}

/// Go `TestDuplicateEntryMessage`.
#[test]
fn test_duplicate_entry_message() {
    let mut tk = TestKit::new();
    tk.must_exec("use test;");
    for enable in ["ON", "OFF", "INT_ONLY"] {
        tk.must_exec(&format!("set @@tidb_enable_clustered_index = '{enable}'"));
        tk.must_exec("drop table if exists t;");
        tk.must_exec(
            "create table t(a int, b char(10), unique key(b)) collate utf8mb4_general_ci;",
        );
        tk.must_exec("insert into t value (34, '12Ak');");
        tk.must_get_err_msg(
            "insert into t value (34, '12Ak');",
            "[kv:1062]Duplicate entry '12Ak' for key 't.b'",
        );
        tk.must_exec("begin optimistic;");
        tk.must_exec("insert into t value (34, '12ak');");
        tk.must_exec("delete from t where b = '12ak';");
        tk.must_get_err_msg("commit;", "previous statement: delete from t where b = '12ak';: [kv:1062]Duplicate entry '12ak' for key 't.b'");
        tk.must_exec("drop table if exists t;");
        tk.must_exec("create table t (a datetime primary key);");
        tk.must_exec("insert into t values ('2020-01-01');");
        tk.must_get_err_msg(
            "insert into t values ('2020-01-01');",
            "[kv:1062]Duplicate entry '2020-01-01 00:00:00' for key 't.PRIMARY'",
        );
        tk.must_exec("begin optimistic;");
        tk.must_exec("insert into t values ('2020-01-01');");
        tk.must_exec("delete from t where a = '2020-01-01';");
        tk.must_get_err_msg("commit;", "previous statement: delete from t where a = '2020-01-01';: [kv:1062]Duplicate entry '2020-01-01 00:00:00' for key 't.PRIMARY'");
        tk.must_exec("drop table if exists t;");
        tk.must_exec("create table t (a int primary key );");
        tk.must_exec("insert into t value (1);");
        tk.must_get_err_msg(
            "insert into t value (1);",
            "[kv:1062]Duplicate entry '1' for key 't.PRIMARY'",
        );
        tk.must_exec("drop table if exists t;");
        tk.must_exec("create table t (a datetime unique);");
        tk.must_exec("insert into t values ('2020-01-01');");
        tk.must_get_err_msg(
            "insert into t values ('2020-01-01');",
            "[kv:1062]Duplicate entry '2020-01-01 00:00:00' for key 't.a'",
        );
        tk.must_exec("drop table if exists t;");
        tk.must_exec("create table t (a datetime, b int, c varchar(10), primary key (a, b, c)) collate utf8mb4_general_ci;");
        tk.must_exec("insert into t values ('2020-01-01', 1, 'aSDd');");
        tk.must_get_err_msg(
            "insert into t values ('2020-01-01', 1, 'ASDD');",
            "[kv:1062]Duplicate entry '2020-01-01 00:00:00-1-ASDD' for key 't.PRIMARY'",
        );
        tk.must_exec("drop table if exists t;");
        tk.must_exec("create table t (a datetime, b int, c varchar(10), unique key (a, b, c)) collate utf8mb4_general_ci;");
        tk.must_exec("insert into t values ('2020-01-01', 1, 'aSDd');");
        tk.must_get_err_msg(
            "insert into t values ('2020-01-01', 1, 'ASDD');",
            "[kv:1062]Duplicate entry '2020-01-01 00:00:00-1-ASDD' for key 't.a'",
        );
        tk.must_exec("drop table if exists t;");
        tk.must_exec("create table t (a char(10) collate utf8mb4_unicode_ci, b char(20) collate utf8mb4_general_ci, c int(11), primary key (a, b, c), unique key (a));");
        tk.must_exec("insert ignore into t values ('$', 'C', 10);");
        tk.must_exec("insert ignore into t values ('$', 'C', 10);");
        tk.must_query("show warnings;").check_with_sep(
            "|",
            ["Warning|1062|Duplicate entry '$-C-10' for key 't.PRIMARY'"],
        );
        tk.must_exec("begin pessimistic;");
        tk.must_exec("insert into t values ('a7', 'a', 10);");
        tk.must_get_err_msg(
            "insert into t values ('a7', 'a', 10);",
            "[kv:1062]Duplicate entry 'a7-a-10' for key 't.PRIMARY'",
        );
        tk.must_exec("rollback;");
        // Test for large unsigned integer handle.
        // See https://github.com/pingcap/tidb/issues/12420.
        tk.must_exec("drop table if exists t;");
        tk.must_exec("create table t(a bigint unsigned primary key);");
        tk.must_exec("insert into t values(18446744073709551615);");
        tk.must_get_err_msg(
            "insert into t values(18446744073709551615);",
            "[kv:1062]Duplicate entry '18446744073709551615' for key 't.PRIMARY'",
        );
    }
}

/// Go `TestInsertNullInNonStrictMode`.
#[test]
fn test_insert_null_in_non_strict_mode() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table t1 (id int primary key, col1 varchar(10) not null default '')");
    tk.must_exec("create table t2 (id int primary key, col1 varchar(10))");
    tk.must_exec("insert into t2 values (1, null)");
    tk.must_exec("insert ignore into t1 values(5, null)");
    tk.must_exec("set session sql_mode = ''");
    assert_eq!(
        tk.must_exec_to_err("insert into t1 values(1, null)")
            .error_string(),
        "[table:1048]Column 'col1' cannot be null"
    );
    assert_eq!(
        tk.must_exec_to_err("insert into t1 set id = 1, col1 = null")
            .error_string(),
        "[table:1048]Column 'col1' cannot be null"
    );
    assert_eq!(
        tk.must_exec_to_err("insert t1 VALUES (5, 5) ON DUPLICATE KEY UPDATE col1 = null")
            .error_string(),
        "[table:1048]Column 'col1' cannot be null"
    );
    tk.must_exec("insert into t1 select * from t2");
    tk.must_exec("insert into t1 values(2, null), (3, 3), (4, 4)");
    tk.must_exec("update t1 set col1 = null where id = 3");
    tk.must_exec("insert ignore t1 VALUES (4, 4) ON DUPLICATE KEY UPDATE col1 = null");
    tk.must_query("select * from t1")
        .check_with_sep("|", ["1|", "2|", "3|", "4|", "5|"]);
}

/// Go `TestAllocateContinuousRowID`.
#[test]
fn test_allocate_continuous_row_id() {
    let mut tk = TestKit::new();
    tk.must_exec(r#"use test"#);
    tk.must_exec(r#"create table t1 (a int,b int, key I_a(a));"#);
    let sessions: Vec<TestKit> = (0..5).map(|_| tk.new_tk()).collect();
    std::thread::scope(|scope| {
        for (idx, mut tk) in sessions.into_iter().enumerate() {
            scope.spawn(move || {
                tk.must_exec("use test");
                for j in 0..10 {
                    let k = (idx * 100 + j).to_string();
                    let mut sql = format!("insert into t1(a,b) values ({k}, 2)");
                    for _ in 0..20 {
                        sql += &format!(",({k},2)");
                    }
                    tk.must_exec(&sql);
                    let q = format!("select _tidb_rowid from t1 where a={k}");
                    let rows = tk.must_query(&q);
                    assert_eq!(21, rows.len());
                    let mut last = 0;
                    for r in rows {
                        assert_eq!(1, r.len());
                        let (v, err) = go_atoi(&r[0]);
                        assert_eq!(None, err);
                        if last > 0 {
                            assert_eq!(v, last + 1);
                        }
                        last = v;
                    }
                }
            });
        }
    });
}

/// Go `TestAutoRandomID`.
#[test]
fn test_auto_random_id() {
    let mut tk = TestKit::new();
    tk.must_exec(r#"use test"#);
    tk.must_exec(r#"drop table if exists ar"#);
    tk.must_exec(r#"create table ar (id bigint key clustered auto_random, name char(10))"#);
    tk.must_exec(r#"insert into ar(id) values (null)"#);
    let mut rs = tk.must_query(r#"select id from ar"#);
    assert_eq!(1, (rs.rows().len() as i64));
    let (mut first_value, mut err) = go_atoi(&rs.rows()[0 as usize][0 as usize].clone());
    assert!(err.is_none(), "{err:?}");
    assert!(first_value > 0);
    tk.must_query(r#"select last_insert_id()"#)
        .check([format!("{}", go_v(&first_value)).as_str()]);
    tk.must_exec(r#"delete from ar"#);
    tk.must_exec(r#"insert into ar(id) values (0)"#);
    rs = tk.must_query(r#"select id from ar"#);
    assert_eq!(1, (rs.rows().len() as i64));
    (first_value, err) = go_atoi(&rs.rows()[0 as usize][0 as usize].clone());
    assert!(err.is_none(), "{err:?}");
    assert!(first_value > 0);
    tk.must_query(r#"select last_insert_id()"#)
        .check([format!("{}", go_v(&first_value)).as_str()]);
    tk.must_exec(r#"delete from ar"#);
    tk.must_exec(r#"insert into ar(name) values ('a')"#);
    rs = tk.must_query(r#"select id from ar"#);
    assert_eq!(1, (rs.rows().len() as i64));
    (first_value, err) = go_atoi(&rs.rows()[0 as usize][0 as usize].clone());
    assert!(err.is_none(), "{err:?}");
    assert!(first_value > 0);
    tk.must_query(r#"select last_insert_id()"#)
        .check([format!("{}", go_v(&first_value)).as_str()]);
    tk.must_exec(r#"drop table ar"#);
    tk.must_exec(r#"create table ar (id bigint key clustered auto_random(15), name char(10))"#);
    let mut overflow_val: i64 = 1 << (64 - 5);
    let mut err_msg = go_sprintf(
        "alter auto_random_base to %d overflows the incremental bits, max allowed base is %d",
        &[&overflow_val, &((1i64 << (64 - 16)) - 1)],
    );
    tk.must_contain_err_msg(
        &format!("alter table ar auto_random_base = {}", go_v(&overflow_val)),
        &err_msg,
    );
}

/// Go `TestAutoRandomIDAllowZero`.
#[test]
fn test_auto_random_id_allow_zero() {
    let mut tk = TestKit::new();
    tk.must_exec(r#"use test"#);
    tk.must_exec(r#"drop table if exists ar"#);
    tk.must_exec(r#"create table ar (id bigint key clustered auto_random, name char(10))"#);
    let mut rs = tk.must_query(r#"select @@session.sql_mode"#);
    let mut sql_mode = rs.rows()[0 as usize][0 as usize].clone();
    tk.must_exec(&format!(
        r#"set session sql_mode="{},{}""#,
        go_v(&sql_mode),
        "NO_AUTO_VALUE_ON_ZERO"
    ));
    tk.must_exec(r#"insert into ar(id) values (0)"#);
    rs = tk.must_query(r#"select id from ar"#);
    assert_eq!(1, (rs.rows().len() as i64));
    let (mut first_value, mut err) = go_atoi(&rs.rows()[0 as usize][0 as usize].clone());
    assert!(err.is_none(), "{err:?}");
    assert_eq!(0, first_value);
    tk.must_query(r#"select last_insert_id()"#)
        .check([format!("{}", go_v(&first_value)).as_str()]);
    tk.must_exec(r#"delete from ar"#);
    tk.must_exec(r#"insert into ar(id) values (null)"#);
    rs = tk.must_query(r#"select id from ar"#);
    assert_eq!(1, (rs.rows().len() as i64));
    (first_value, err) = go_atoi(&rs.rows()[0 as usize][0 as usize].clone());
    assert!(err.is_none(), "{err:?}");
    assert!(first_value > 0);
    tk.must_query(r#"select last_insert_id()"#)
        .check([format!("{}", go_v(&first_value)).as_str()]);
    tk.must_exec(r#"drop table ar"#);
}

/// Go `TestMultiAutoRandomID`.
#[test]
fn test_multi_auto_random_id() {
    let mut tk = TestKit::new();
    tk.must_exec(r#"use test"#);
    tk.must_exec(r#"drop table if exists ar"#);
    tk.must_exec(r#"create table ar (id bigint key clustered auto_random, name char(10))"#);
    tk.must_exec(r#"insert into ar(id) values (null),(null),(null)"#);
    let mut rs = tk.must_query(r#"select id from ar order by id"#);
    assert_eq!(3, (rs.rows().len() as i64));
    let (mut first_value, mut err) = go_atoi(&rs.rows()[0 as usize][0 as usize].clone());
    assert!(err.is_none(), "{err:?}");
    assert!(first_value > 0);
    assert_eq!(
        format!("{}", first_value + 1),
        rs.rows()[1 as usize][0 as usize].clone()
    );
    assert_eq!(
        format!("{}", first_value + 2),
        rs.rows()[2 as usize][0 as usize].clone()
    );
    tk.must_query(r#"select last_insert_id()"#)
        .check([format!("{}", go_v(&first_value)).as_str()]);
    tk.must_exec(r#"delete from ar"#);
    tk.must_exec(r#"insert into ar(id) values (0),(0),(0)"#);
    rs = tk.must_query(r#"select id from ar order by id"#);
    assert_eq!(3, (rs.rows().len() as i64));
    (first_value, err) = go_atoi(&rs.rows()[0 as usize][0 as usize].clone());
    assert!(err.is_none(), "{err:?}");
    assert!(first_value > 0);
    assert_eq!(
        format!("{}", first_value + 1),
        rs.rows()[1 as usize][0 as usize].clone()
    );
    assert_eq!(
        format!("{}", first_value + 2),
        rs.rows()[2 as usize][0 as usize].clone()
    );
    tk.must_query(r#"select last_insert_id()"#)
        .check([format!("{}", go_v(&first_value)).as_str()]);
    tk.must_exec(r#"delete from ar"#);
    tk.must_exec(r#"insert into ar(name) values ('a'),('a'),('a')"#);
    rs = tk.must_query(r#"select id from ar order by id"#);
    assert_eq!(3, (rs.rows().len() as i64));
    (first_value, err) = go_atoi(&rs.rows()[0 as usize][0 as usize].clone());
    assert!(err.is_none(), "{err:?}");
    assert!(first_value > 0);
    assert_eq!(
        format!("{}", first_value + 1),
        rs.rows()[1 as usize][0 as usize].clone()
    );
    assert_eq!(
        format!("{}", first_value + 2),
        rs.rows()[2 as usize][0 as usize].clone()
    );
    tk.must_query(r#"select last_insert_id()"#)
        .check([format!("{}", go_v(&first_value)).as_str()]);
    tk.must_exec(r#"drop table ar"#);
}
