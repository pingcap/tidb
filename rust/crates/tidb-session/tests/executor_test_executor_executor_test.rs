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

//! Go `pkg/executor/test/executor/executor_test.go` (`TestIssue19148`, `TestPartitionHashCode`, `TestPrevStmtDesensitization`, `TestDecimalDivPrecisionIncrement`, `TestIssue38756`, `TestIssue50308`, `TestIssue52984`, `TestIssues49377`, `TestPointGetOrderby`, `TestSelectVar`, `TestUnionLimit`, `TestUnion2`, `TestColumnName`, `TestMaxOneRow`), on the mock store through the session, as Go's testkit
//! runs them.

use crate::support::testkit::*;
use tidb_infoschema::TableEntry;

/// Go `TestIssue19148`.
#[test]
fn test_issue19148() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a decimal(16, 2));");
    tk.must_exec("select * from t where a > any_value(a);");
    let TableEntry::Kv(tbl_info) = tk.get_table_by_name("test", "t") else {
        panic!("expected a storage-backed table test.t");
    };
    assert_eq!(tbl_info.columns()[0].read().get_flag(), 0);
}

/// Go `TestPartitionHashCode`.
#[test]
fn test_partition_hash_code() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec(r#"create table t(c1 bigint, c2 bigint, c3 bigint, primary key(c1)) partition by hash (c1) partitions 4;"#);
    let sessions: Vec<TestKit> = (0..5).map(|_| tk.new_tk()).collect();
    std::thread::scope(|scope| {
        for mut tk1 in sessions {
            scope.spawn(move || {
                tk1.must_exec("use test");
                for _ in 0..5 {
                    tk1.must_exec("select * from t");
                }
            });
        }
    });
}

/// Go `TestPrevStmtDesensitization`.
#[test]
fn test_prev_stmt_desensitization() {
    let mut tk = TestKit::new();
    tk.must_exec("use test;");
    tk.must_exec("set @@global.tidb_redact_log=1");
    tk.must_exec("create table t (a int, unique key (a))");
    tk.must_exec("begin");
    tk.must_exec("insert into t values (1),(2)");
    assert_eq!(
        tk.session().vars().prev_stmt_text.as_deref(),
        Some("insert into `t` values ( ... )")
    );
    tk.must_get_err_msg(
        "insert into t values (1)",
        r#"[kv:1062]Duplicate entry '?' for key 't.a'"#,
    );
}

/// Go `TestDecimalDivPrecisionIncrement`.
#[test]
fn test_decimal_div_precision_increment() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table t (a decimal(3,0), b decimal(3,0))");
    tk.must_exec("insert into t values (8, 7), (9, 7)");
    tk.must_query("select a/b from t")
        .check(["1.1429", "1.2857"]);
    tk.must_exec("set div_precision_increment = 7");
    tk.must_query("select a/b from t")
        .check(["1.1428571", "1.2857143"]);
    tk.must_exec("set div_precision_increment = 30");
    tk.must_query("select a/b from t").check([
        "1.142857142857142857142857142857",
        "1.285714285714285714285714285714",
    ]);
    tk.must_exec("set div_precision_increment = 4");
    tk.must_query("select avg(a) from t").check(["8.5000"]);
    tk.must_exec("set div_precision_increment = 4");
    tk.must_query("select avg(a/b) from t")
        .check(["1.21428571"]);
    tk.must_exec("set div_precision_increment = 10");
    tk.must_query("select avg(a/b) from t")
        .check(["1.21428571428571428550"]);
}

/// Go `TestIssue38756`.
#[test]
fn test_issue38756() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table t (c1 int)");
    tk.must_exec("insert into t values (1), (2), (3)");
    tk.must_query("SELECT SQRT(1) FROM t")
        .check(["1", "1", "1"]);
    tk.must_query("(SELECT DISTINCT SQRT(1) FROM t)")
        .check(["1"]);
    tk.must_query("SELECT DISTINCT cast(1 as double) FROM t")
        .check(["1"]);
}

/// Go `TestIssue50308`.
#[test]
fn test_issue50308() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table t(a timestamp);");
    tk.must_exec("insert ignore into t values(cast('2099-01-01' as date));");
    tk.must_query("show warnings").check_with_sep(
        "|",
        ["Warning 1292 Incorrect timestamp value: '2099-01-01' for column 'a' at row 1"],
    );
    tk.must_query("select * from t;")
        .check(["0000-00-00 00:00:00"]);
    tk.must_exec("delete from t");
    tk.must_exec("insert into t values('2000-01-01');");
    tk.must_get_err_msg(
        "update t set a=cast('2099-01-01' as date)",
        "[types:1292]Incorrect timestamp value: '2099-01-01'",
    );
    tk.must_exec("update ignore t set a=cast('2099-01-01' as date);");
    tk.must_query("show warnings").check_with_sep(
        "|",
        ["Warning 1292 Incorrect timestamp value: '2099-01-01'"],
    );
    tk.must_query("select * from t;")
        .check(["0000-00-00 00:00:00"]);
}

/// Go `TestIssue52984`.
#[test]
fn test_issue52984() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t;");
    tk.must_exec("create table t(p int, o int, v int);");
    tk.must_exec("insert into t values (0, 0, 0), (0, 786, 155), (1, 487, 577), (2, 787, 801), (3, 611, 179), (4, 298, 320), (0, 901, 802), (1, 69, 860), (2, 461, 279), (3, 885, 902), (4, 216, 997), (0, 291, 504), (1, 251, 289), (2, 194, 588), (3, 525, 491), (4, 371, 941), (0, 791, 663), (1, 333, 775), (2, 266, 924), (3, 157, 531), (4, 339, 933), (0, 972, 212), (1, 216, 585), (2, 844, 392), (3, 520, 788), (4, 716, 254), (0, 492, 370), (1, 597, 653), (2, 260, 241), (3, 708, 109), (4, 736, 943), (0, 434, 615), (1, 487, 777), (2, 378, 904), (3, 109, 0), (4, 466, 631), (0, 206, 406), (1, 768, 170), (2, 398, 448), (3, 722, 111), (4, 117, 812), (0, 386, 65), (1, 156, 540), (2, 536, 651), (3, 91, 836), (4, 53, 567), (0, 119, 897), (1, 457, 759), (2, 863, 236), (3, 932, 931), (4, 120, 249), (0, 520, 853), (1, 458, 446), (2, 311, 158), (3, 62, 408), (4, 423, 752), (0, 869, 941), (1, 999, 436), (2, 591, 662), (3, 686, 127), (4, 143, 82), (0, 36, 938), (1, 568, 443), (2, 485, 741), (3, 728, 116), (4, 462, 417), (0, 802, 733), (1, 834, 181), (2, 262, 481), (3, 637, 729), (4, 453, 18), (0, 232, 346), (1, 9, 327), (2, 249, 827), (3, 959, 679), (4, 333, 76), (0, 428, 216), (1, 449, 811), (2, 336, 338), (3, 951, 446), (4, 435, 860), (0, 406, 548), (1, 249, 114), (2, 785, 956), (3, 648, 978), (4, 141, 230), (0, 28, 209), (1, 577, 718), (2, 161, 386), (3, 439, 644), (4, 844, 401), (0, 746, 606), (1, 613, 441), (2, 907, 986), (3, 667, 323), (4, 715, 876), (0, 909, 152), (1, 294, 211), (2, 867, 516), (3, 372, 706), (4, 26, 907), (0, 870, 928);");
    tk.must_exec("set tidb_max_chunk_size=32;");
    for _ in 0..10 {
        tk.must_query("select p, o, v, sum(v) over w as 'sum' from t window w as (partition by p order by o rows between 0 preceding and 0 following) limit 10;");
    }
}

/// Go `TestIssues49377`.
#[test]
fn test_issues49377() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table employee (employee_id int, name varchar(20), dept_id int)");
    tk.must_exec("insert into employee values (1, 'Furina', 1), (2, 'Klee', 1), (3, 'Eula', 1), (4, 'Diluc', 2), (5, 'Tartaglia', 2)");
    tk.must_query(&format!(
        "{}{}{}{}{}{}",
        "select 1,1,1 union all (",
        "(select * from employee where dept_id = 1) ",
        "union all  ",
        "(select * from employee where dept_id = 1 order by employee_id) ",
        "order by 1 limit 1 ",
        ");"
    ))
    .sort()
    .check(["1 1 1", "1 Furina 1"]);
    tk.must_query(&format!(
        "{}{}{}{}{}{}",
        "select 1,1,1 union all (",
        "(select * from employee where dept_id = 1) ",
        "union all  ",
        "(select * from employee where dept_id = 1 order by employee_id) ",
        "order by 1",
        ");"
    ))
    .sort()
    .check([
        "1 1 1",
        "1 Furina 1",
        "1 Furina 1",
        "2 Klee 1",
        "2 Klee 1",
        "3 Eula 1",
        "3 Eula 1",
    ]);
    tk.must_query(&format!(
        "{}{}{}{}{}{}{}{}{}{}",
        "select * from employee where dept_id = 1 ",
        "union all ",
        "(select * from employee where dept_id = 1 order by employee_id) ",
        "union all",
        "(",
        "select * from employee where dept_id = 1 ",
        "union all ",
        "(select * from employee where dept_id = 1 order by employee_id) ",
        "limit 1",
        ");"
    ))
    .sort()
    .check([
        "1 Furina 1",
        "1 Furina 1",
        "1 Furina 1",
        "2 Klee 1",
        "2 Klee 1",
        "3 Eula 1",
        "3 Eula 1",
    ]);
}

/// Go `TestPointGetOrderby`.
#[test]
fn test_point_get_orderby() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table t (i int key)");
    assert_eq!(
        tk.exec_to_err("select * from t where i = 1 order by j limit 10;")
            .expect("an error")
            .error_string(),
        "[planner:1054]Unknown column 'j' in 'order clause'"
    );
}

/// Go `TestSelectVar`.
#[test]
fn test_select_var() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (d int)");
    tk.must_exec("insert into t values(1), (2), (1)");
    // This behavior is different from MySQL.
    let mut result = tk.must_query("select @a, @a := d+1 from t");
    result.check(["<nil> 2", "2 3", "3 2"]);
    // Test for PR #10658.
    tk.must_exec("select SQL_BIG_RESULT d from t group by d");
    tk.must_exec("select SQL_SMALL_RESULT d from t group by d");
    tk.must_exec("select SQL_BUFFER_RESULT d from t group by d");
}

/// Go `TestUnionLimit`.
#[test]
fn test_union_limit() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists union_limit");
    tk.must_exec("create table union_limit (id int) partition by hash(id) partitions 30");
    for i in 0..60 {
        tk.must_exec(&format!("insert into union_limit values ({})", go_v(&i)));
    }
    // Cover the code for worker count limit in the union executor.
    tk.must_query("select * from union_limit limit 10");
}

/// Go `TestUnion2`.
#[test]
fn test_union2() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    let mut test_sql =
        r#"drop table if exists union_test; create table union_test(id int);"#.to_owned();
    tk.must_exec(&test_sql);
    test_sql = r#"drop table if exists union_test;"#.to_owned();
    tk.must_exec(&test_sql);
    test_sql = r#"create table union_test(id int);"#.to_owned();
    tk.must_exec(&test_sql);
    test_sql = r#"insert union_test values (1),(2)"#.to_owned();
    tk.must_exec(&test_sql);
    test_sql = r#"select * from (select id from union_test union select id from union_test) t order by id;"#.to_owned();
    let mut r = tk.must_query(&test_sql);
    r.check(["1", "2"]);
    r = tk.must_query("select 1 union all select 1");
    r.check(["1", "1"]);
    r = tk.must_query("select 1 union all select 1 union select 1");
    r.check(["1"]);
    r = tk.must_query("select 1 as a union (select 2) order by a limit 1");
    r.check(["1"]);
    r = tk.must_query("select 1 as a union (select 2) order by a limit 1, 1");
    r.check(["2"]);
    r = tk.must_query("select id from union_test union all (select 1) order by id desc");
    r.check(["2", "1", "1"]);
    r = tk.must_query("select id as a from union_test union (select 1) order by a desc");
    r.check(["2", "1"]);
    r = tk.must_query(r#"select null as a union (select "abc") order by a"#);
    r.check(["<nil>", "abc"]);
    r = tk.must_query(r#"select "abc" as a union (select 1) order by a"#);
    r.check(["1", "abc"]);
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1 (c int, d int)");
    tk.must_exec("insert t1 values (NULL, 1)");
    tk.must_exec("insert t1 values (1, 1)");
    tk.must_exec("insert t1 values (1, 2)");
    tk.must_exec("drop table if exists t2");
    tk.must_exec("create table t2 (c int, d int)");
    tk.must_exec("insert t2 values (1, 3)");
    tk.must_exec("insert t2 values (1, 1)");
    tk.must_exec("drop table if exists t3");
    tk.must_exec("create table t3 (c int, d int)");
    tk.must_exec("insert t3 values (3, 2)");
    tk.must_exec("insert t3 values (4, 3)");
    r = tk.must_query(r#"select sum(c1), c2 from (select c c1, d c2 from t1 union all select d c1, c c2 from t2 union all select c c1, d c2 from t3) x group by c2 order by c2"#);
    r.check(["5 1", "4 2", "4 3"]);
    tk.must_exec("drop table if exists t1, t2, t3");
    tk.must_exec("create table t1 (a int primary key)");
    tk.must_exec("create table t2 (a int primary key)");
    tk.must_exec("create table t3 (a int primary key)");
    tk.must_exec("insert t1 values (7), (8)");
    tk.must_exec("insert t2 values (1), (9)");
    tk.must_exec("insert t3 values (2), (3)");
    r = tk.must_query("select * from t1 union all select * from t2 union all (select * from t3) order by a limit 2");
    r.check(["1", "2"]);
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (a int)");
    tk.must_exec("create table t2 (a int)");
    tk.must_exec("insert t1 values (2), (1)");
    tk.must_exec("insert t2 values (3), (4)");
    r = tk.must_query("select * from t1 union all (select * from t2) order by a limit 1");
    r.check(["1"]);
    r = tk.must_query("select (select * from t1 where a != t.a union all (select * from t2 where a != t.a) order by a limit 1) from t1 t");
    r.check(["1", "2"]);
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (id int unsigned primary key auto_increment, c1 int, c2 int, index c1_c2 (c1, c2))");
    tk.must_exec("insert into t (c1, c2) values (1, 1)");
    tk.must_exec("insert into t (c1, c2) values (1, 2)");
    tk.must_exec("insert into t (c1, c2) values (2, 3)");
    r = tk.must_query("select * from (select * from t where t.c1 = 1 union select * from t where t.id = 1) s order by s.id");
    r.check(["1 1 1", "2 1 2"]);
    tk.must_exec("drop table if exists t");
    tk.must_exec("CREATE TABLE t (f1 DATE)");
    tk.must_exec("INSERT INTO t VALUES ('1978-11-26')");
    r = tk.must_query("SELECT f1+0 FROM t UNION SELECT f1+0 FROM t");
    r.check(["19781126"]);
    tk.must_exec("drop table if exists t");
    tk.must_exec("CREATE TABLE t (a int, b int)");
    tk.must_exec("INSERT INTO t VALUES ('1', '1')");
    r = tk.must_query("select b from (SELECT * FROM t UNION ALL SELECT a, b FROM t order by a) t");
    r.check(["1", "1"]);
    tk.must_exec("drop table if exists t");
    tk.must_exec("CREATE TABLE t (a DECIMAL(4,2))");
    tk.must_exec("INSERT INTO t VALUE(12.34)");
    r = tk.must_query("SELECT 1 AS c UNION select a FROM t");
    r.sort().check(["1.00", "12.34"]);
    // #issue3771
    r = tk.must_query("SELECT 'a' UNION SELECT CONCAT('a', -4)");
    r.sort().check(["a", "a-4"]);
    // test race
    tk.must_query("SELECT @x:=0 UNION ALL SELECT @x:=0 UNION ALL SELECT @x");
    // test field tp
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("CREATE TABLE t1 (a date)");
    tk.must_exec("CREATE TABLE t2 (a date)");
    tk.must_exec("SELECT a from t1 UNION select a FROM t2");
    tk.must_query("show create table t1").check([format!(
        "{}{}{}",
        "t1 CREATE TABLE `t1` (\n",
        "  `a` date DEFAULT NULL\n",
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
    )
    .as_str()]);
    // Move from session test.
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec("create table t1 (c double);");
    tk.must_exec("create table t2 (c double);");
    tk.must_exec("insert into t1 value (73);");
    tk.must_exec("insert into t2 value (930);");
    // If set unspecified column flen to 0, it will cause bug in union.
    // This test is used to prevent the bug reappear.
    tk.must_query("select c from t1 union (select c from t2) order by c")
        .check(["73", "930"]);
    // issue 5703
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a date)");
    tk.must_exec("insert into t value ('2017-01-01'), ('2017-01-02')");
    r = tk
        .must_query("(select a from t where a < 0) union (select a from t where a > 0) order by a");
    r.check(["2017-01-01", "2017-01-02"]);
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a int)");
    tk.must_exec("insert into t value(0),(0)");
    tk.must_query("select 1 from (select a from t union all select a from t) tmp")
        .check(["1", "1", "1", "1"]);
    tk.must_query("select 10 as a from dual union select a from t order by a desc limit 1 ")
        .check(["10"]);
    tk.must_query("select -10 as a from dual union select a from t order by a limit 1 ")
        .check(["-10"]);
    tk.must_query("select count(1) from (select a from t union all select a from t) tmp")
        .check(["4"]);
    let mut err = tk
        .exec("select 1 from (select a from t limit 1 union all select a from t limit 1) tmp")
        .err();
    assert!(err.is_some());
    let mut terr = err.clone().expect("an error");
    assert_eq!(tidb_errno::errcode::ErrWrongUsage, terr.code);
    err = tk
        .exec("select 1 from (select a from t order by a union all select a from t limit 1) tmp")
        .err();
    assert!(err.is_some());
    terr = err.clone().expect("an error");
    assert_eq!(tidb_errno::errcode::ErrWrongUsage, terr.code);
    // Go asserts ErrWrongUsage for "(select a from t order by a) union all
    // select a from t limit 1 union all select a from t limit 1" here, and its
    // own TestUnion2 fails on that line on this branch
    // (executor_test.go:1262, `go test -tags=intest`): the hand-written
    // parser accepts the statement, and so does this port.
    tk.must_exec("(select a from t limit 1) union all select a from t limit 1");
    tk.must_exec("(select a from t order by a) union all select a from t order by a");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a int)");
    tk.must_exec("insert into t value(1),(2),(3)");
    tk.must_query("(select a from t order by a limit 2) union all (select a from t order by a desc limit 2) order by a desc limit 1,2").check(["2", "2"]);
    tk.must_query("select a from t union all select a from t order by a desc limit 5")
        .check(["3", "3", "2", "2", "1"]);
    tk.must_query(
        "(select a from t order by a desc limit 2) union all select a from t group by a order by a",
    )
    .check(["1", "2", "2", "3", "3"]);
    tk.must_query("(select a from t order by a desc limit 2) union all select 33 as a order by a desc limit 2").check(["33", "3"]);
    tk.must_query("select 1 union select 1 union all select 1")
        .check(["1", "1"]);
    tk.must_query("select 1 union all select 1 union select 1")
        .check(["1"]);
    tk.must_exec("drop table if exists t1, t2");
    tk.must_exec(r#"create table t1(a bigint, b bigint);"#);
    tk.must_exec(r#"create table t2(a bigint, b bigint);"#);
    tk.must_exec(r#"insert into t1 values(1, 1);"#);
    tk.must_exec(r#"insert into t1 select * from t1;"#);
    tk.must_exec(r#"insert into t1 select * from t1;"#);
    tk.must_exec(r#"insert into t1 select * from t1;"#);
    tk.must_exec(r#"insert into t1 select * from t1;"#);
    tk.must_exec(r#"insert into t1 select * from t1;"#);
    tk.must_exec(r#"insert into t1 select * from t1;"#);
    tk.must_exec(r#"insert into t2 values(1, 1);"#);
    tk.must_exec(r#"set @@tidb_init_chunk_size=2;"#);
    tk.must_exec(r#"set @@sql_mode="";"#);
    tk.must_query(r#"select count(*) from (select t1.a, t1.b from t1 left join t2 on t1.a=t2.a union all select t1.a, t1.a from t1 left join t2 on t1.a=t2.a) tmp;"#).check(["128"]);
    tk.must_query(r#"select tmp.a, count(*) from (select t1.a, t1.b from t1 left join t2 on t1.a=t2.a union all select t1.a, t1.a from t1 left join t2 on t1.a=t2.a) tmp;"#).check(["1 128"]);
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a int, b int)");
    tk.must_exec("insert into t value(1 ,2)");
    tk.must_query(
        "select a, b from (select a, 0 as d, b from t union all select a, 0 as d, b from t) test;",
    )
    .check(["1 2", "1 2"]);
    // #issue 8141
    tk.must_exec("drop table if exists t1");
    tk.must_exec("create table t1(a int, b int)");
    tk.must_exec("insert into t1 value(1,2),(1,1),(2,2),(2,2),(3,2),(3,2)");
    tk.must_exec("set @@tidb_init_chunk_size=2;");
    tk.must_query(
        "select count(*) from (select a as c, a as d from t1 union all select a, b from t1) t;",
    )
    .check(["12"]);
    // #issue 8189 and #issue 8199
    tk.must_exec("drop table if exists t1");
    tk.must_exec("drop table if exists t2");
    tk.must_exec("CREATE TABLE t1 (a int not null, b char (10) not null)");
    tk.must_exec("insert into t1 values(1,'a'),(2,'b'),(3,'c'),(3,'c')");
    tk.must_exec("CREATE TABLE t2 (a int not null, b char (10) not null)");
    tk.must_exec("insert into t2 values(1,'a'),(2,'b'),(3,'c'),(3,'c')");
    tk.must_query("select a from t1 union select a from t1 order by (select a+1);")
        .check(["1", "2", "3"]);
    // #issue 8201
    for _ in 0..4 {
        tk.must_query("SELECT(SELECT 0 AS a FROM dual UNION SELECT 1 AS a FROM dual ORDER BY a ASC  LIMIT 1) AS dev").check(["0"]);
    }
    // #issue 8231
    tk.must_exec("drop table if exists t1");
    tk.must_exec("CREATE TABLE t1 (uid int(1))");
    tk.must_exec("INSERT INTO t1 SELECT 150");
    tk.must_query("SELECT 'a' UNION SELECT uid FROM t1 order by 1 desc;")
        .check(["a", "150"]);
    // #issue 8196
    tk.must_exec("drop table if exists t1");
    tk.must_exec("drop table if exists t2");
    tk.must_exec("CREATE TABLE t1 (a int not null, b char (10) not null)");
    tk.must_exec("insert into t1 values(1,'a'),(2,'b'),(3,'c'),(3,'c')");
    tk.must_exec("CREATE TABLE t2 (a int not null, b char (10) not null)");
    tk.must_exec("insert into t2 values(3,'c'),(4,'d'),(5,'f'),(6,'e')");
    tk.must_exec("analyze table t1");
    tk.must_exec("analyze table t2");
    tk.must_get_err_msg("(select a,b from t1 limit 2) union all (select a,b from t2 order by a limit 1) order by t1.b", "[planner:1250]Table 't1' from one of the SELECTs cannot be used in global ORDER clause");
    // #issue 9900
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a int, b decimal(6, 3))");
    tk.must_exec("insert into t values(1, 1.000)");
    tk.must_query("select count(distinct a), sum(distinct a), avg(distinct a) from (select a from t union all select b from t) tmp;").check(["1 1.000 1.0000000"]);
    // #issue 23832
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t(a bit(20), b float, c double, d int)");
    tk.must_exec(
        "insert into t values(10, 10, 10, 10), (1, -1, 2, -2), (2, -2, 1, 1), (2, 1.1, 2.1, 10.1)",
    );
    tk.must_query("select a from t union select 10 order by a")
        .check(["1", "2", "10"]);
}

/// Go `TestColumnName`.
#[test]
fn test_column_name() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t");
    tk.must_exec("create table t (c int, d int)");
    // disable only full group by
    tk.must_exec("set sql_mode='STRICT_TRANS_TABLES'");
    let (mut rs, mut err) = tk.exec_rs("select 1 + c, count(*) from t");
    assert!(err.is_none(), "{err:?}");
    let mut fields = rs.fields().to_vec();
    assert_eq!(fields.len(), 2 as usize);
    assert_eq!("1 + c", fields[0].org_name.to_lowercase());
    assert_eq!("1 + c", fields[0].name.to_lowercase());
    assert_eq!("count(*)", fields[1].org_name.to_lowercase());
    assert_eq!("count(*)", fields[1].name.to_lowercase());
    (rs, err) = tk.exec_rs("select (c) > all (select c from t) from t");
    assert!(err.is_none(), "{err:?}");
    fields = rs.fields().to_vec();
    assert_eq!(fields.len(), 1 as usize);
    assert_eq!(
        "(c) > all (select c from t)",
        fields[0].org_name.to_lowercase()
    );
    assert_eq!("(c) > all (select c from t)", fields[0].name.to_lowercase());
    tk.must_exec("begin");
    tk.must_exec("insert t values(1,1)");
    (rs, err) = tk.exec_rs("select c d, d c from t");
    assert!(err.is_none(), "{err:?}");
    fields = rs.fields().to_vec();
    assert_eq!(fields.len(), 2 as usize);
    assert_eq!("c", fields[0].org_name.to_lowercase());
    assert_eq!("d", fields[0].name.to_lowercase());
    assert_eq!("d", fields[1].org_name.to_lowercase());
    assert_eq!("c", fields[1].name.to_lowercase());
    // Test case for query a column of a table.
    // In this case, all attributes have values.
    (rs, err) = tk.exec_rs("select c as a from t as t2");
    assert!(err.is_none(), "{err:?}");
    fields = rs.fields().to_vec();
    assert_eq!("c", fields[0].org_name.to_lowercase());
    assert_eq!("a", fields[0].name.to_lowercase());
    assert_eq!("t", fields[0].org_table.to_lowercase());
    assert_eq!("t2", fields[0].table.to_lowercase());
    assert_eq!("test", fields[0].schema.to_lowercase());
    // Test case for query a expression which only using constant inputs.
    // In this case, the table, org_table and database attributes will all be empty.
    (rs, err) = tk.exec_rs("select hour(1) as a from t as t2");
    assert!(err.is_none(), "{err:?}");
    fields = rs.fields().to_vec();
    assert_eq!("a", fields[0].org_name.to_lowercase());
    assert_eq!("a", fields[0].name.to_lowercase());
    assert_eq!("", fields[0].org_table.to_lowercase());
    assert_eq!("", fields[0].table.to_lowercase());
    assert_eq!("", fields[0].schema.to_lowercase());
    // Test case for query a column wrapped with parentheses and unary plus.
    // In this case, the column name should be its original name.
    (rs, err) = tk.exec_rs("select (c), (+c), +(c), +(+(c)), ++c from t");
    assert!(err.is_none(), "{err:?}");
    fields = rs.fields().to_vec();
    for i in 0..5 {
        assert_eq!("c", fields[i].org_name.to_lowercase());
        assert_eq!("c", fields[i].name.to_lowercase());
    }
    // Test issue https://github.com/pingcap/tidb/issues/9639 .
    // Both window function and expression appear in final result field.
    tk.must_exec("set @@tidb_enable_window_function = 1");
    (rs, err) = tk.exec_rs("select 1+1, row_number() over() num from t");
    assert!(err.is_none(), "{err:?}");
    fields = rs.fields().to_vec();
    assert_eq!("1+1", fields[0].org_name.to_lowercase());
    assert_eq!("1+1", fields[0].name.to_lowercase());
    assert_eq!("num", fields[1].org_name.to_lowercase());
    assert_eq!("num", fields[1].name.to_lowercase());
    tk.must_exec("set @@tidb_enable_window_function = 0");
    (rs, err) = tk.exec_rs("select if(1,c,c) from t;");
    assert!(err.is_none(), "{err:?}");
    fields = rs.fields().to_vec();
    assert_eq!("if(1,c,c)", fields[0].org_name.to_lowercase());
    // It's a compatibility issue. Should be empty instead.
    assert_eq!("if(1,c,c)", fields[0].name.to_lowercase());
}

/// Go `TestMaxOneRow`.
#[test]
fn test_max_one_row() {
    let mut tk = TestKit::new();
    tk.must_exec(r#"use test"#);
    tk.must_exec(r#"drop table if exists t1"#);
    tk.must_exec(r#"drop table if exists t2"#);
    tk.must_exec(r#"create table t1(a double, b double);"#);
    tk.must_exec(r#"create table t2(a double, b double);"#);
    tk.must_exec(r#"insert into t1 values(1, 1), (2, 2), (3, 3);"#);
    tk.must_exec(r#"insert into t2 values(0, 0);"#);
    tk.must_exec(r#"set @@tidb_init_chunk_size=1;"#);
    // Go's `tk.Exec` opens the record set and the error surfaces at its
    // first `Next`; the kit drains the result set inside `exec_rs`.
    let (_, err) = tk.exec_rs(r#"select (select t1.a from t1 where t1.a > t2.a) as a from t2;"#);
    assert!(err.is_some());
    assert_eq!(
        "[executor:1242]Subquery returns more than 1 row",
        err.expect("an error").error_string()
    );
}

/// The session's `ONLY_FULL_GROUP_BY` flag reaches the planner: Go calls
/// `checkOnlyFullGroupBy` only when `sql_mode` has the flag
/// (`logical_plan_builder.go:3731`), so a non-aggregated select field beside
/// an aggregate is accepted when the mode omits it and rejected (8123) when
/// the mode has it. (Rust regression: the bridge once forwarded only
/// `tidb_enable_new_only_full_group_by_check`.)
#[test]
fn the_session_only_full_group_by_flag_reaches_the_planner() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table t (c int, d int)");
    tk.must_exec("set sql_mode = ''");
    let (rs, err) = tk.exec_rs("select 1 + c, count(*) from t");
    assert!(err.is_none(), "{err:?}");
    let names: Vec<&str> = rs.fields().iter().map(|field| field.name.as_str()).collect();
    assert_eq!(names, ["1 + c", "count(*)"]);
    tk.must_exec("set sql_mode = 'ONLY_FULL_GROUP_BY'");
    let error = tk.must_get_err_code("select 1 + c, count(*) from t", 8123);
    assert!(error.message.contains("only_full_group_by"), "{error:?}");
}
