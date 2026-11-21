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

//! Go `pkg/planner/core/tests/pointget/point_get_plan_test.go` over the
//! cop-backed TestKit stack.

use super::*;

fn session(stack: &UnistoreClusterStack, connection: u64) -> Session {
    let mut session = stack.factory.open_session(session_context(connection)).expect("session opens");
    rows(&mut session, "use test");
    session
}

fn plan_tree(session: &mut Session, sql: &str) -> Vec<String> {
    displayed(rows(session, &format!("explain format = 'plan_tree' {sql}")))
        .into_iter()
        .map(|row| row.join(" "))
        .collect()
}

/// Go `TestIssue52592` (classic kernel): fix control 52592 turns a handle
/// point read into a table range scan; a negative unsigned handle is still a
/// `TableDual`.
#[test]
fn test_issue_52592() {
    if !tidb_config_kerneltype::is_classic() {
        return;
    }
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 451);
    rows(&mut tk, r#"set @@tidb_opt_fix_control = "52592:OFF""#);
    rows(&mut tk, "drop table if exists t");
    rows(&mut tk, "create table t(a bigint unsigned primary key, b int, c int, key idx_bc(b,c))");
    rows(&mut tk, "insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3)");
    assert_eq!(plan_tree(&mut tk, "select * from t where a = 1"), ["Point_Get root table:t handle:1"]);
    assert_eq!(plan_tree(&mut tk, "select * from t where 1 = a"), ["Point_Get root table:t handle:1"]);
    assert_eq!(
        plan_tree(&mut tk, "update t set b=b+1, c=c+1 where a = 1"),
        ["Update root  N/A", "└─Point_Get root table:t handle:1"]
    );
    assert_eq!(
        plan_tree(&mut tk, "delete from t where a = 1"),
        ["Delete root  N/A", "└─Point_Get root table:t handle:1"]
    );
    assert_eq!(plan_tree(&mut tk, "select a from t where a = -1"), ["TableDual root  rows:0"]);
    rows(&mut tk, r#"set @@tidb_opt_fix_control = "52592:ON""#);
    let range = [
        "TableReader root  data:TableRangeScan",
        "└─TableRangeScan cop[tikv] table:t range:[1,1], keep order:false, stats:pseudo",
    ];
    assert_eq!(plan_tree(&mut tk, "select * from t where a = 1"), range);
    assert_eq!(plan_tree(&mut tk, "select * from t where 1 = a"), range);
    assert_eq!(
        plan_tree(&mut tk, "update t set b=b+1, c=c+1 where a = 1"),
        [
            "Update root  N/A",
            "└─TableReader root  data:TableRangeScan",
            "  └─TableRangeScan cop[tikv] table:t range:[1,1], keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(
        plan_tree(&mut tk, "delete from t where a = 1"),
        [
            "Delete root  N/A",
            "└─TableReader root  data:TableRangeScan",
            "  └─TableRangeScan cop[tikv] table:t range:[1,1], keep order:false, stats:pseudo",
        ]
    );
    assert_eq!(plan_tree(&mut tk, "select a from t where a = -1"), ["TableDual root  rows:0"]);
}

/// Go `TestIssue18042`: a point get keeps the statement's MAX_EXECUTION_TIME
/// and MEMORY_QUOTA hints.
#[test]
fn test_issue_18042() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 452);
    rows(&mut tk, "drop table if exists t");
    rows(&mut tk, "create table t(a int, b int, c int, primary key(a), index ab(a, b));");
    rows(&mut tk, "insert into t values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)");
    rows(&mut tk, "SELECT /*+ MAX_EXECUTION_TIME(100), MEMORY_QUOTA(1 MB) */ * FROM t where a = 1;");
    assert_eq!(tk.stmt_hints.mem_quota_query, 1 << 20);
    assert_eq!(tk.stmt_hints.max_execution_time, 100);
    rows(&mut tk, "drop table t");
}

/// Go `TestIssue56832`: an UPDATE of an ENUM through a point get writes the
/// member the number names.
#[test]
fn test_issue_56832() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 453);
    rows(&mut tk, "drop table if exists t");
    rows(&mut tk, "create table t (id int primary key, c enum('0', '1', '2'));");
    rows(&mut tk, "insert into t values (0,'0'), (1,'1'), (2,'2');");
    rows(&mut tk, "update t set c = 2 where id = 0;");
    assert_eq!(displayed(rows(&mut tk, "select c from t where id = 0")), [["1"]]);
}

/// Go `TestIssue20692`: a pessimistic UPDATE through the unique key waits for
/// the INSERT that reused the key a committed DELETE freed.
#[test]
fn test_issue_20692() {
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 454);
    rows(&mut tk, "drop table if exists t");
    rows(&mut tk, "create table t (id int primary key, v int, vv int, vvv int, unique key u0(id, v, vv));");
    rows(&mut tk, "insert into t values(1, 1, 1, 1);");
    let mut tk1 = session(&stack, 455);
    let mut tk2 = session(&stack, 456);
    let mut tk3 = session(&stack, 457);
    rows(&mut tk1, "begin pessimistic;");
    rows(&mut tk2, "begin pessimistic;");
    rows(&mut tk3, "begin pessimistic;");
    rows(&mut tk1, "delete from t where id = 1 and v = 1 and vv = 1;");
    // Go's goroutine runs tk2's INSERT, then tk3's UPDATE; each runs on a
    // thread of its own here so the main thread can commit the session the
    // other waits on.
    let insert = std::thread::spawn(move || {
        rows(&mut tk2, "insert into t values(1, 2, 3, 4);");
        tk2
    });
    rows(&mut tk1, "commit;");
    let mut tk2 = insert.join().expect("tk2's insert");
    let (stop2_tx, stop2) = std::sync::mpsc::channel();
    let update = std::thread::spawn(move || {
        rows(&mut tk3, "update t set id = 10, v = 20, vv = 30, vvv = 40 where id = 1 and v = 2 and vv = 3;");
        stop2_tx.send(()).unwrap();
        tk3
    });
    // wait 50ms to ensure tk3 is blocked by tk2
    assert!(
        stop2.recv_timeout(std::time::Duration::from_millis(50)).is_err(),
        "tk3 was not blocked by tk2"
    );
    rows(&mut tk2, "commit;");
    stop2.recv().unwrap();
    let mut tk3 = update.join().expect("tk3's update");
    rows(&mut tk3, "commit;");
    assert_eq!(displayed(rows(&mut tk3, "select * from t;")), [["10", "20", "30", "40"]]);
}

/// Go `TestPointGetPlanCache` (classic kernel): point plans of prepared
/// SELECT, UPDATE and DELETE statements hit the plan cache on their second
/// execution, by handle or by unique index; an out-of-range handle answers
/// nothing without being cached.
#[test]
fn test_point_get_plan_cache() {
    if !tidb_config_kerneltype::is_classic() {
        return;
    }
    let (stack, _users) = cop_backed_stack();
    let mut tk = session(&stack, 458);
    rows(&mut tk, "set @@tidb_enable_non_prepared_plan_cache=0"); // affect hit counter in this ut
    rows(&mut tk, "set tidb_enable_prepared_plan_cache=1");
    rows(&mut tk, "drop table if exists t");
    rows(&mut tk, "create table t(a bigint unsigned primary key, b int, c int, key idx_bc(b,c))");
    rows(&mut tk, "insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3)");
    assert_eq!(plan_tree(&mut tk, "select * from t where a = 1"), ["Point_Get root table:t handle:1"]);
    assert_eq!(plan_tree(&mut tk, "select * from t where 1 = a"), ["Point_Get root table:t handle:1"]);
    assert_eq!(
        plan_tree(&mut tk, "update t set b=b+1, c=c+1 where a = 1"),
        ["Update root  N/A", "└─Point_Get root table:t handle:1"]
    );
    assert_eq!(
        plan_tree(&mut tk, "delete from t where a = 1"),
        ["Delete root  N/A", "└─Point_Get root table:t handle:1"]
    );
    assert_eq!(plan_tree(&mut tk, "select a from t where a = -1"), ["TableDual root  rows:0"]);
    rows(&mut tk, r#"prepare stmt0 from "select a from t where a = ?""#);
    rows(&mut tk, "set @p0 = -1");
    assert!(rows(&mut tk, "execute stmt0 using @p0").is_empty());
    tidb_metrics::server::RESETTABLE_PLAN_CACHE_COUNTER_FORT_TEST.store(true, std::sync::atomic::Ordering::Relaxed);
    tidb_metrics::server::PLAN_CACHE_COUNTER.reset();
    let counter = tidb_metrics::server::PLAN_CACHE_COUNTER.with_label_values(&["prepare"]);
    let select = |tk: &mut Session, sql: &str| displayed(rows(tk, sql));
    // PointGetPlan for Select.
    rows(&mut tk, r#"prepare stmt1 from "select * from t where a = ?""#);
    rows(&mut tk, r#"prepare stmt2 from "select * from t where b = ? and c = ?""#);
    rows(&mut tk, "set @param=1");
    assert_eq!(select(&mut tk, "execute stmt1 using @param"), [["1", "1", "1"]]);
    assert_eq!(counter.get(), 0.0);
    rows(&mut tk, "set @param=2");
    assert_eq!(select(&mut tk, "execute stmt1 using @param"), [["2", "2", "2"]]);
    assert_eq!(counter.get(), 1.0);
    assert_eq!(select(&mut tk, "execute stmt2 using @param, @param"), [["2", "2", "2"]]);
    assert_eq!(counter.get(), 1.0);
    rows(&mut tk, "set @param=1");
    assert_eq!(select(&mut tk, "execute stmt2 using @param, @param"), [["1", "1", "1"]]);
    assert_eq!(counter.get(), 2.0);
    // PointGetPlan for Update.
    rows(&mut tk, r#"prepare stmt3 from "update t set b=b+1, c=c+1 where a = ?""#);
    rows(&mut tk, r#"prepare stmt4 from "update t set a=a+1 where b = ? and c = ?""#);
    rows(&mut tk, "set @param=3");
    rows(&mut tk, "execute stmt3 using @param");
    assert_eq!(select(&mut tk, "select * from t"), [["1", "1", "1"], ["2", "2", "2"], ["3", "4", "4"]]);
    assert_eq!(counter.get(), 2.0);
    rows(&mut tk, "set @param=4");
    rows(&mut tk, "execute stmt4 using @param, @param");
    assert_eq!(select(&mut tk, "select * from t"), [["1", "1", "1"], ["2", "2", "2"], ["4", "4", "4"]]);
    assert_eq!(counter.get(), 2.0);
    // PointGetPlan for Delete.
    rows(&mut tk, r#"prepare stmt5 from "delete from t where a = ?""#);
    rows(&mut tk, r#"prepare stmt6 from "delete from t where b = ? and c = ?""#);
    rows(&mut tk, "execute stmt5 using @param");
    assert_eq!(select(&mut tk, "select * from t"), [["1", "1", "1"], ["2", "2", "2"]]);
    assert_eq!(counter.get(), 2.0);
    rows(&mut tk, "set @param=2");
    rows(&mut tk, "execute stmt6 using @param, @param");
    assert_eq!(select(&mut tk, "select * from t"), [["1", "1", "1"]]);
    assert_eq!(counter.get(), 2.0);
    rows(&mut tk, "insert into t (a, b, c) values (18446744073709551615, 4, 4)");
    rows(&mut tk, "set @p1=-1");
    rows(&mut tk, "set @p2=1");
    rows(&mut tk, r#"prepare stmt7 from "select a from t where a = ?""#);
    assert!(rows(&mut tk, "execute stmt7 using @p1").is_empty());
    assert_eq!(select(&mut tk, "execute stmt7 using @p2"), [["1"]]);
    assert_eq!(counter.get(), 2.0);
}
