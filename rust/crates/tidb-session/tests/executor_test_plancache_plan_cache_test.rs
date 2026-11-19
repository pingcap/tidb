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

//! Go `pkg/executor/test/plancache/plan_cache_test.go` (`TestPointGetPreparedPlan`, `TestPointUpdatePreparedPlan`, `TestPreparedPlanCachePlanSelectionRegressions`, `TestPreparedPlanCacheOperators`), on the mock store through the session, as Go's testkit
//! runs them.

#![allow(unused_mut, unused_variables, unused_imports)]

use crate::support::testkit::*;

/// Go `TestPointGetPreparedPlan`.
#[test]
fn test_point_get_prepared_plan() {
    let mut tk = TestKit::new();
    tk.must_exec("drop database if exists ps_text");
    tk.must_exec("create database ps_text");
    tk.must_exec("use ps_text");
    tk.must_exec(r#"create table t (a int, b int, c int, primary key k_a(a), unique key k_b(b))"#);
    tk.must_exec("insert into t values (1, 1, 1)");
    tk.must_exec("insert into t values (2, 2, 2)");
    tk.must_exec("insert into t values (3, 3, 3)");
    let (mut pspk1_id, _, _, mut err) = tk.prepare_stmt("select * from t where a = ?");
    assert!(err.is_none(), "{err:?}");
    tk.set_prepared_stmt_cacheable(pspk1_id, false);
    let (mut pspk2_id, _, _, mut err) = tk.prepare_stmt("select * from t where ? = a ");
    assert!(err.is_none(), "{err:?}");
    tk.set_prepared_stmt_cacheable(pspk2_id, false);
    // first time plan generated
    let (mut rs, mut err) = tk.execute_prepared_stmt(pspk1_id, &[go_arg(0)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check_empty();
    // using the generated plan but with different params
    (rs, err) = tk.execute_prepared_stmt(pspk1_id, &[go_arg(1)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["1 1 1"]);
    (rs, err) = tk.execute_prepared_stmt(pspk1_id, &[go_arg(2)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["2 2 2"]);
    (rs, err) = tk.execute_prepared_stmt(pspk2_id, &[go_arg(3)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["3 3 3"]);
    (rs, err) = tk.execute_prepared_stmt(pspk2_id, &[go_arg(0)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check_empty();
    (rs, err) = tk.execute_prepared_stmt(pspk2_id, &[go_arg(1)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["1 1 1"]);
    (rs, err) = tk.execute_prepared_stmt(pspk2_id, &[go_arg(2)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["2 2 2"]);
    (rs, err) = tk.execute_prepared_stmt(pspk2_id, &[go_arg(3)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["3 3 3"]);
    // unique index
    let (mut psuk1_id, _, _, mut err) = tk.prepare_stmt("select * from t where b = ? ");
    assert!(err.is_none(), "{err:?}");
    tk.set_prepared_stmt_cacheable(psuk1_id, false);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(1)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["1 1 1"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(2)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["2 2 2"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(3)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["3 3 3"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(0)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check_empty();
    // test schema changed, cached plan should be invalidated
    tk.must_exec("alter table t add column col4 int default 10 after c");
    (rs, err) = tk.execute_prepared_stmt(pspk1_id, &[go_arg(0)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check_empty();
    (rs, err) = tk.execute_prepared_stmt(pspk1_id, &[go_arg(1)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["1 1 1 10"]);
    (rs, err) = tk.execute_prepared_stmt(pspk1_id, &[go_arg(2)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["2 2 2 10"]);
    (rs, err) = tk.execute_prepared_stmt(pspk2_id, &[go_arg(3)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["3 3 3 10"]);
    tk.must_exec("alter table t drop index k_b");
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(1)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["1 1 1 10"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(2)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["2 2 2 10"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(3)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["3 3 3 10"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(0)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check_empty();
    tk.must_exec(r#"insert into t values(4, 3, 3, 11)"#);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(1)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["1 1 1 10"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(2)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["2 2 2 10"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(3)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["3 3 3 10", "4 3 3 11"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(0)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check_empty();
    tk.must_exec("delete from t where a = 4");
    tk.must_exec("alter table t add index k_b(b)");
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(1)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["1 1 1 10"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(2)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["2 2 2 10"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(3)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["3 3 3 10"]);
    (rs, err) = tk.execute_prepared_stmt(psuk1_id, &[go_arg(0)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check_empty();
    // use pk again
    (rs, err) = tk.execute_prepared_stmt(pspk2_id, &[go_arg(3)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["3 3 3 10"]);
    (rs, err) = tk.execute_prepared_stmt(pspk1_id, &[go_arg(3)]);
    assert!(err.is_none(), "{err:?}");
    rs.clone().check(["3 3 3 10"]);
}

/// Go `TestPointUpdatePreparedPlan`.
#[test]
fn test_point_update_prepared_plan() {
    let mut tk = TestKit::new();
    tk.must_exec("drop database if exists pu_test");
    tk.must_exec("create database pu_test");
    tk.must_exec("use pu_test");
    tk.must_exec(r#"create table t (a int, b int, c int, primary key k_a(a), unique key k_b(b))"#);
    tk.must_exec("insert into t values (1, 1, 1)");
    tk.must_exec("insert into t values (2, 2, 2)");
    tk.must_exec("insert into t values (3, 3, 3)");
    let (mut update_id1, mut pc, _, mut err) =
        tk.prepare_stmt(r#"update t set c = c + 1 where a = ?"#);
    assert!(err.is_none(), "{err:?}");
    tk.set_prepared_stmt_cacheable(update_id1, false);
    assert_eq!(1, pc);
    let (mut update_id2, mut pc, _, mut err) =
        tk.prepare_stmt(r#"update t set c = c + 2 where ? = a"#);
    assert!(err.is_none(), "{err:?}");
    tk.set_prepared_stmt_cacheable(update_id2, false);
    assert_eq!(1, pc);
    // first time plan generated
    let (mut rs, mut err) = tk.execute_prepared_stmt(update_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 4"]);
    // using the generated plan but with different params
    (rs, err) = tk.execute_prepared_stmt(update_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 5"]);
    (rs, err) = tk.execute_prepared_stmt(update_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 6"]);
    // updateID2
    (rs, err) = tk.execute_prepared_stmt(update_id2, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 8"]);
    (rs, err) = tk.execute_prepared_stmt(update_id2, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 10"]);
    // unique index
    let (mut upd_uk_id1, _, _, mut err) = tk.prepare_stmt(r#"update t set c = c + 10 where b = ?"#);
    assert!(err.is_none(), "{err:?}");
    tk.set_prepared_stmt_cacheable(upd_uk_id1, false);
    (rs, err) = tk.execute_prepared_stmt(upd_uk_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 20"]);
    (rs, err) = tk.execute_prepared_stmt(upd_uk_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 30"]);
    // test schema changed, cached plan should be invalidated
    tk.must_exec("alter table t add column col4 int default 10 after c");
    (rs, err) = tk.execute_prepared_stmt(update_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 31 10"]);
    (rs, err) = tk.execute_prepared_stmt(update_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 32 10"]);
    tk.must_exec("alter table t drop index k_b");
    (rs, err) = tk.execute_prepared_stmt(upd_uk_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 42 10"]);
    (rs, err) = tk.execute_prepared_stmt(upd_uk_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 52 10"]);
    tk.must_exec("alter table t add unique index k_b(b)");
    (rs, err) = tk.execute_prepared_stmt(upd_uk_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 62 10"]);
    (rs, err) = tk.execute_prepared_stmt(upd_uk_id1, &[go_arg(3)]);
    assert!(rs.is_nil());
    assert!(err.is_none(), "{err:?}");
    tk.must_query("select * from t where a = 3")
        .check(["3 3 72 10"]);
    tk.must_query("select * from t where a = 1")
        .check(["1 1 1 10"]);
    tk.must_query("select * from t where a = 2")
        .check(["2 2 2 10"]);
}

/// Go `TestPreparedPlanCachePlanSelectionRegressions`.
#[test]
fn test_prepared_plan_cache_plan_selection_regressions() {
    let mut tk = TestKit::new();
    test_prepared_null_param(&mut tk);
    test_issue29850(&mut tk);
    test_issue28064(&mut tk);
    test_issue29101(&mut tk);
    test_issue57528(&mut tk);
}

/// Go `testIssue28064`.
fn test_issue28064(tk: &mut TestKit) {
    cleanup_prepared_plan_selection_state(tk);
    tk.must_exec(r#"set tidb_enable_prepared_plan_cache=1"#);
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t28064");
    tk.must_exec(&format!(
        "{}{}{}{}{}{}",
        "CREATE TABLE `t28064` (",
        "`a` decimal(10,0) DEFAULT NULL,",
        "`b` decimal(10,0) DEFAULT NULL,",
        "`c` decimal(10,0) DEFAULT NULL,",
        "`d` decimal(10,0) DEFAULT NULL,",
        "KEY `iabc` (`a`,`b`,`c`));"
    ));
    tk.must_exec("set @a='123', @b='234', @c='345';");
    tk.must_exec("set @@tidb_enable_collect_execution_info=0;");
    tk.must_exec("prepare stmt1 from 'select * from t28064 use index (iabc) where a = ? and b = ? and c = ?';");
    tk.must_exec("execute stmt1 using @a, @b, @c;");
    let mut rows = tk.must_query(&format!("explain for connection {}", tk.connection_id()));
    rows.check(["IndexLookUp_7 1.25 root  ", "├─IndexRangeScan_5(Build) 1.25 cop[tikv] table:t28064, index:iabc(a, b, c) range:[123 234 345,123 234 345], keep order:false, stats:pseudo", "└─TableRowIDScan_6(Probe) 1.25 cop[tikv] table:t28064 keep order:false, stats:pseudo"]);
    tk.must_exec("execute stmt1 using @a, @b, @c;");
    tk.must_query("select @@last_plan_from_cache").check(["1"]);
}

/// Go `testIssue29101`.
fn test_issue29101(tk: &mut TestKit) {
    cleanup_prepared_plan_selection_state(tk);
    tk.must_exec("set @old_tidb_opt_advanced_join_hint := @@tidb_opt_advanced_join_hint");
    tk.must_exec(r#"set tidb_enable_prepared_plan_cache=1"#);
    tk.must_exec(r#"set @@tidb_opt_advanced_join_hint=0"#);
    tk.must_exec(r#"use test"#);
    tk.must_exec("set @@tidb_enable_collect_execution_info=0;");
    tk.must_exec(r#"CREATE TABLE customer (c_id int(11) NOT NULL, c_d_id int(11) NOT NULL, c_w_id int(11) NOT NULL, c_first varchar(16) DEFAULT NULL, c_last varchar(16) DEFAULT NULL, c_credit char(2) DEFAULT NULL, c_discount decimal(4,4) DEFAULT NULL, PRIMARY KEY (c_w_id,c_d_id,c_id) NONCLUSTERED, KEY idx_customer (c_w_id,c_d_id,c_last,c_first) )"#);
    tk.must_exec(r#"CREATE TABLE warehouse (w_id int(11) NOT NULL, w_tax decimal(4,4) DEFAULT NULL, PRIMARY KEY (w_id) )"#);
    tk.must_exec(r#"prepare s1 from 'SELECT /*+ TIDB_INLJ(customer,warehouse) */ c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?'"#);
    tk.must_exec(r#"set @a=936,@b=7,@c=158"#);
    tk.must_query(r#"execute s1 using @a,@b,@c"#).check_empty();
    tk.must_query(&format!("explain for connection {}", tk.connection_id())).check(["Projection_6 1.00 root  test.customer.c_discount, test.customer.c_last, test.customer.c_credit, test.warehouse.w_tax", "└─IndexJoin_18 1.00 root  inner join, inner:IndexLookUp_32, outer key:test.warehouse.w_id, inner key:test.customer.c_w_id, equal cond:eq(test.warehouse.w_id, test.customer.c_w_id)", "  ├─Point_Get_33(Build) 1.00 root table:warehouse handle:936", "  └─IndexLookUp_32(Probe) 1.00 root  ", "    ├─Selection_31(Build) 1.00 cop[tikv]  eq(test.customer.c_w_id, 936)", "    │ └─IndexRangeScan_29 1.00 cop[tikv] table:customer, index:PRIMARY(c_w_id, c_d_id, c_id) range: decided by [eq(test.customer.c_w_id, test.warehouse.w_id) eq(test.customer.c_d_id, 7) eq(test.customer.c_id, 158)], keep order:false, stats:pseudo", "    └─TableRowIDScan_30(Probe) 1.00 cop[tikv] table:customer keep order:false, stats:pseudo"]);
    tk.must_query(r#"execute s1 using @a,@b,@c"#).check_empty();
    tk.must_query(r#"select @@last_plan_from_cache"#)
        .check(["1"]);
}

/// Go `testIssue29850`.
fn test_issue29850(tk: &mut TestKit) {
    cleanup_prepared_plan_selection_state(tk);
    tk.must_exec(r#"set tidb_enable_prepared_plan_cache=1"#);
    tk.must_exec(r#"set tidb_enable_clustered_index=on"#);
    tk.must_exec("set @@tidb_enable_collect_execution_info=0");
    tk.must_exec(r#"use test"#);
    tk.must_exec(r#"CREATE TABLE customer (c_id int(11) NOT NULL, c_d_id int(11) NOT NULL, c_first varchar(16) DEFAULT NULL, c_w_id int(11) NOT NULL, c_last varchar(16) DEFAULT NULL, c_credit char(2) DEFAULT NULL, c_discount decimal(4,4) DEFAULT NULL, PRIMARY KEY (c_w_id,c_d_id,c_id), KEY idx_customer (c_w_id,c_d_id,c_last,c_first))"#);
    tk.must_exec(r#"CREATE TABLE warehouse (w_id int(11) NOT NULL, w_tax decimal(4,4) DEFAULT NULL, PRIMARY KEY (w_id))"#);
    tk.must_exec(r#"prepare stmt from 'SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?'"#);
    tk.must_exec(r#"set @w_id=1262"#);
    tk.must_exec(r#"set @c_d_id=7"#);
    tk.must_exec(r#"set @c_id=1549"#);
    tk.must_query(r#"execute stmt using @w_id, @c_d_id, @c_id"#)
        .check_empty();
    tk.must_query(&format!("explain format='brief' for connection {}", tk.connection_id())).check([r#"Projection 1.00 root  test.customer.c_discount, test.customer.c_last, test.customer.c_credit, test.warehouse.w_tax"#, r#"└─MergeJoin 1.00 root  inner join, left key:test.customer.c_w_id, right key:test.warehouse.w_id"#, r#"  ├─Point_Get(Build) 1.00 root table:warehouse handle:1262"#, r#"  └─Point_Get(Probe) 1.00 root table:customer, clustered index:PRIMARY(c_w_id, c_d_id, c_id) "#]);
    tk.must_query(r#"execute stmt using @w_id, @c_d_id, @c_id"#)
        .check_empty();
    tk.must_query(r#"select @@last_plan_from_cache"#)
        .check(["1"]);
    tk.must_exec(r#"create table t (a int primary key)"#);
    tk.must_exec(r#"insert into t values (1), (2)"#);
    tk.must_exec(r#"prepare stmt from 'select * from t where a>=? and a<=?'"#);
    tk.must_exec(r#"set @a1=1, @a2=2"#);
    tk.must_query(r#"execute stmt using @a1, @a1"#).check(["1"]);
    tk.must_query(&format!("explain for connection {}", tk.connection_id()))
        .check([r#"Point_Get_5 1.00 root table:t handle:1"#]);
    tk.must_query(r#"execute stmt using @a1, @a2"#)
        .check(["1", "2"]);
    tk.must_query(r#"select @@last_plan_from_cache"#)
        .check(["0"]);
    tk.must_exec(r#"prepare stmt from 'select * from t where a=? or a=?'"#);
    tk.must_query(r#"execute stmt using @a1, @a1"#).check(["1"]);
    tk.must_query(&format!("explain for connection {}", tk.connection_id()))
        .check([r#"Point_Get_5 1.00 root table:t handle:1"#]);
    tk.must_query(r#"execute stmt using @a1, @a2"#)
        .check(["1", "2"]);
}

/// Go `testIssue57528`.
fn test_issue57528(tk: &mut TestKit) {
    cleanup_prepared_plan_selection_state(tk);
    tk.must_exec(r#"set tidb_enable_prepared_plan_cache=1"#);
    tk.must_exec("set @@tidb_enable_collect_execution_info=1");
    tk.must_exec(r#"use test"#);
    tk.must_exec(r#"CREATE TABLE customer (c_id int(11) NOT NULL, c_discount int(11) DEFAULT NULL, PRIMARY KEY (c_id))"#);
    tk.must_exec(r#"insert into customer values (1, 2)"#);
    tk.must_exec(r#"prepare stmt from 'SELECT c_discount FROM customer WHERE c_id = ?'"#);
    tk.must_exec(r#"set @c_id=1"#);
    tk.must_query(r#"execute stmt using @c_id"#).check(["2"]);
    tk.must_query(r#"execute stmt using @c_id"#).check(["2"]);
    tk.must_query(r#"select @@last_plan_from_cache"#)
        .check(["1"]);
    tk.must_exec("set @@tidb_enable_collect_execution_info=0");
    tk.must_query(r#"execute stmt using @c_id"#).check(["2"]);
    tk.must_query(r#"select @@last_plan_from_cache"#)
        .check(["1"]);
}

/// Go `testPreparedNullParam`.
fn test_prepared_null_param(tk: &mut TestKit) {
    cleanup_prepared_plan_selection_state(tk);
    for flag in [false, true] {
        tk.must_exec(&format!(
            r#"set tidb_enable_prepared_plan_cache={}"#,
            go_v(&flag)
        ));
        tk.must_exec("use test");
        tk.must_exec("set @@tidb_enable_collect_execution_info=0");
        tk.must_exec("drop table if exists t");
        tk.must_exec("create table t (id int not null, KEY id (id))");
        tk.must_exec("insert into t values (1), (2), (3)");
        tk.must_exec("prepare stmt from 'select * from t where id = ?'");
        tk.must_exec("set @a= null");
        tk.must_query("execute stmt using @a").check_empty();
        tk.must_query(&format!("explain for connection {}", tk.connection_id()))
            .check(["TableDual_6 0.00 root  rows:0"]);
    }
}

/// Go `cleanupPreparedPlanSelectionState`.
fn cleanup_prepared_plan_selection_state(tk: &mut TestKit) {
    tk.must_exec("use test");
    tk.must_exec("drop table if exists t, customer, warehouse, t28064");
}

/// Go `TestPreparedPlanCacheOperators`.
#[test]
fn test_prepared_plan_cache_operators() {
    let mut tk = TestKit::new();
    tk.must_exec(r#"set tidb_enable_prepared_plan_cache=1"#);
    /// Go `prepCase`: a statement and the parameter lists it executes with.
    struct PrepCase {
        stmt: &'static str,
        cases: &'static [&'static [&'static str]],
    }
    let cases = [
        PrepCase {
            stmt: "use test",
            cases: &[],
        },
        PrepCase {
            stmt: "create table t (a int, b int, key(a))",
            cases: &[],
        },
        PrepCase {
            stmt: "insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,null)",
            cases: &[],
        },
        PrepCase {
            stmt: "select * from t where a=?",
            cases: &[&["1"], &["2"], &["3"]],
        },
        PrepCase {
            stmt: "select * from t where a in (?,?,?)",
            cases: &[&["1", "1", "1"], &["2", "3", "4"]],
        },
        PrepCase {
            stmt: "select /*+ HASH_JOIN(t1, t2) */ * from t t1, t t2 where t1.a=t2.a and t1.b>?",
            cases: &[&["1"], &["3"]],
        },
        PrepCase {
            stmt: "select * from t t1 where t1.b>? and t1.a > (select min(t2.a) from t t2 where t2.b < t1.b)",
            cases: &[&["1"], &["3"]],
        },
        PrepCase {
            stmt: "drop table t",
            cases: &[],
        },
        PrepCase {
            stmt: "create table t (name varchar(50), y int, sale decimal(14,2))",
            cases: &[],
        },
        PrepCase {
            stmt: "insert into t values ('Bob',2016,2.4), ('Bob',2017,3.2), ('Alice',2016,1.4), ('Alice',2017,2), ('John',2016,4), ('John',2017,2.1)",
            cases: &[],
        },
        PrepCase {
            stmt: "select *, sum(sale) over (partition by y order by sale+? rows 2 preceding) total from t order by y",
            cases: &[&["0.1"], &["0.5"]],
        },
        PrepCase {
            stmt: "select *, first_value(sale) over (partition by y order by sale rows ? preceding) total from t order by y",
            cases: &[&["1"], &["2"]],
        },
        PrepCase {
            stmt: "drop table t",
            cases: &[],
        },
        PrepCase {
            stmt: "create table t (a int)",
            cases: &[],
        },
        PrepCase {
            stmt: "insert into t values (1), (1), (2), (2), (3), (4), (5), (6)",
            cases: &[],
        },
        PrepCase {
            stmt: "select * from t limit ?",
            cases: &[&["20"], &["30"]],
        },
        PrepCase {
            stmt: "drop table t",
            cases: &[],
        },
        PrepCase {
            stmt: "create table t (a int, b int)",
            cases: &[],
        },
        PrepCase {
            stmt: "insert into t values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)",
            cases: &[],
        },
        PrepCase {
            stmt: "select * from t order by b+?",
            cases: &[&["1"], &["2"]],
        },
        PrepCase {
            stmt: "select * from t order by b limit ?",
            cases: &[&["1"], &["2"]],
        },
    ];
    for prep in &cases {
        if !prep.stmt.contains("select") {
            tk.must_exec(prep.stmt);
            continue;
        }
        tk.must_exec(&format!("prepare stmt from '{}'", prep.stmt));
        for parameters in prep.cases {
            let mut using_stmt = String::new();
            if !parameters.is_empty() {
                let mut set_stmt = "set ".to_owned();
                using_stmt = "using ".to_owned();
                for (i, parameter) in parameters.iter().enumerate() {
                    if i > 0 {
                        set_stmt += ", ";
                        using_stmt += ", ";
                    }
                    set_stmt += &format!("@x{i}={parameter}");
                    using_stmt += &format!("@x{i}");
                }
                tk.must_exec(&set_stmt);
            }
            let result = tk.must_query(&format!("execute stmt {using_stmt}")).sort();
            let parts: Vec<&str> = prep.stmt.split('?').collect();
            assert_eq!(parameters.len() + 1, parts.len());
            let mut query = String::new();
            for (i, part) in parts.iter().enumerate() {
                query += part;
                if i < parameters.len() {
                    query += parameters[i];
                }
            }
            tk.must_query(&query).sort().check_result(&result);
        }
    }
}
