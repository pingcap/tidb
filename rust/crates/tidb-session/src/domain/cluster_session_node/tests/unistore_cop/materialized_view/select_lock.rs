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

//! Go master `pkg/planner/core/casetest/mview/mv_refresh_test.go`'s
//! `TestMViewSelectLock`: locking reads refuse a materialized view and its
//! log as lock targets (`checkLockClauseTables` / `CheckMViewUpdatable`),
//! except in MV maintenance.

use super::*;

/// `ErrNonUpdatableTable`, `ErrUnknownTable`, `ErrLockAcquireFailAndNoWaitSet`.
const ERR_NON_UPDATABLE_TABLE: u16 = 1288;
const ERR_UNKNOWN_TABLE: u16 = 1109;
const ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET: u16 = 3572;

fn next_conn_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(4600);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Go `tk.MustGetErrCode`.
fn must_get_err_code(tk: &mut Session, sql: &str, code: u16) {
    let answered = answer(tk, sql);
    assert!(answered.starts_with(&format!("ERR {code} ")), "{sql}: {answered}");
}

/// Go `tk.MustQuery(sql)`.
fn must_query(tk: &mut Session, sql: &str) -> Vec<String> {
    let answered = answer(tk, sql);
    assert!(!answered.starts_with("ERR "), "{sql}: {answered}");
    super::refresh::go_check(tk, sql)
}

/// Go `tk.MustQuery(sql).Sort().Check(...)`.
fn sorted(tk: &mut Session, sql: &str) -> Vec<String> {
    let mut rows = super::refresh::go_check(tk, sql);
    rows.sort();
    rows
}

/// `@@last_plan_from_cache` (Go `FoundInPlanCache`).
fn found_in_plan_cache(tk: &mut Session) -> bool {
    super::refresh::go_check(tk, "select @@last_plan_from_cache") == ["1"]
}

/// Go `vars.InRestrictedSQL, vars.InMViewMaintenance = true, true`.
fn set_maintenance(tk: &mut Session, on: bool) {
    (tk.vars.in_mview_maintenance, tk.vars.restricted_sql) = (on, on);
}

#[test]
fn test_m_view_select_lock() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(
        &mut tk,
        &[
            "use test",
            "set tidb_mview_enable = on",
            "set tidb_enable_noop_functions = on",
            "set tidb_enable_shared_lock_promotion = off",
            "create table t_lock (a int primary key, b int not null)",
            "create materialized view log on t_lock (a, b) purge next date_add(now(), interval 1 hour)",
            "insert into t_lock values (1, 10), (2, 20)",
            "create materialized view mv_lock (a, s, cnt) refresh fast as select a, sum(b), count(1) from t_lock group by a",
        ],
    );

    for (name, table, key) in [("mv", "mv_lock", "a"), ("mlog", "`$mlog$t_lock`", "_tidb_rowid")] {
        let point_sql = format!("select * from {table} where {key} = 1");
        let batch_sql = format!("select * from {table} where {key} in (1, 2)");
        let explain = |tk: &mut Session, sql: &str| super::refresh::go_check(tk, &format!("explain format = 'brief' {sql}")).join("\n");
        assert!(explain(&mut tk, &point_sql).contains("Point_Get"), "{name}");
        assert!(explain(&mut tk, &batch_sql).contains("Batch_Point_Get"), "{name}");
        must_query(&mut tk, &point_sql);
        must_query(&mut tk, &batch_sql);

        for lock in ["for update", "for update nowait", "for update wait 1", "for share", "for share nowait", "lock in share mode"] {
            for access in [
                format!("select * from {table}"),
                point_sql.clone(),
                batch_sql.clone(),
                format!("select m.* from {table} m where m.{key} = 1"),
            ] {
                must_get_err_code(&mut tk, &format!("{access} {lock}"), ERR_NON_UPDATABLE_TABLE);
            }
        }

        // "join_lock_targets"
        must_exec_all(&mut tk, &["begin pessimistic"]);
        let join_sql = format!("select n.a from t_lock n join {table} m on n.a = m.a");
        must_get_err_code(&mut tk, &format!("{join_sql} for update"), ERR_NON_UPDATABLE_TABLE);
        must_get_err_code(&mut tk, &format!("{join_sql} for update of m"), ERR_NON_UPDATABLE_TABLE);
        must_get_err_code(&mut tk, &format!("{join_sql} for update of n, m"), ERR_NON_UPDATABLE_TABLE);
        must_get_err_code(&mut tk, &format!("{join_sql} for update of m, missing"), ERR_UNKNOWN_TABLE);
        assert_eq!(vec!["1", "2"], sorted(&mut tk, &format!("{join_sql} for update of n")), "{name}");
        must_exec_all(&mut tk, &["rollback"]);

        // "query_block_lock_targets"
        must_exec_all(&mut tk, &["begin pessimistic"]);
        for (case, sql, want_err) in [
            ("derived", "select n.a from t_lock n join (select a from %s) d on n.a = d.a for update", false),
            ("derived_of_n", "select n.a from t_lock n join (select a from %s) d on n.a = d.a for update of n", false),
            ("cte", "with d as (select a from %s) select n.a from t_lock n join d on n.a = d.a for update", false),
            ("cte_of", "with d as (select a from %s) select n.a from t_lock n join d on n.a = d.a for update of d", false),
            ("cte_only_of", "with d as (select a from %s) select d.a from d for update of d", false),
            ("cte_of_direct_mv", "with d as (select a from t_lock) select m.a from %s m join d on m.a = d.a for update of d", true),
            ("cte_alias_of_direct_mv", "with d as (select a from t_lock) select m.a from %s m join d c on m.a = c.a for update of c", true),
            ("cte_and_base_of", "with d as (select a from t_lock) select n.a from t_lock n join %s m on n.a = m.a join d on n.a = d.a for update of d, n", false),
            ("derived_only", "select d.a from (select a from %s) d for update", false),
            ("derived_inner_lock", "select n.a from t_lock n join (select a from %s for update) d on n.a = d.a", true),
            ("derived_inner_lock_outer_of_n", "select n.a from t_lock n join (select a from %s for update) d on n.a = d.a for update of n", true),
            ("cte_inner_lock", "with d as (select a from %s for update) select n.a from t_lock n join d on n.a = d.a for update of n", true),
        ] {
            let sql = sql.replace("%s", table);
            if want_err {
                must_get_err_code(&mut tk, &sql, ERR_NON_UPDATABLE_TABLE);
            } else {
                assert_eq!(vec!["1", "2"], sorted(&mut tk, &sql), "{name}/{case}");
            }
        }
        must_exec_all(&mut tk, &["rollback"]);

        // "shared_lock_promotion"
        must_exec_all(&mut tk, &["set tidb_enable_shared_lock_promotion = on"]);
        for lock in ["for share", "for share nowait", "lock in share mode"] {
            must_get_err_code(&mut tk, &format!("{point_sql} {lock}"), ERR_NON_UPDATABLE_TABLE);
        }
        must_exec_all(&mut tk, &["set tidb_enable_shared_lock_promotion = off"]);

        // "pessimistic_transaction"
        must_exec_all(&mut tk, &["begin pessimistic"]);
        must_get_err_code(&mut tk, &format!("{point_sql} for update"), ERR_NON_UPDATABLE_TABLE);
        must_get_err_code(&mut tk, &format!("{batch_sql} for update"), ERR_NON_UPDATABLE_TABLE);
        assert_eq!(vec!["1 10"], super::refresh::go_check(&mut tk, "select * from t_lock where a = 1 for update"));
        must_exec_all(&mut tk, &["rollback"]);

        // "prepared_statement"
        must_get_err_code(
            &mut tk,
            &format!("prepare lock_stmt from 'select * from {table} where {key} = ? for update'"),
            ERR_NON_UPDATABLE_TABLE,
        );

        for instance_cache in [false, true] {
            // "prepared_lock_scope/instance=<instance_cache>": Go forces the
            // instance plan cache through the context; here the GLOBAL switch.
            must_exec_all(
                &mut tk,
                &[if instance_cache {
                    "set global tidb_enable_instance_plan_cache = on"
                } else {
                    "set global tidb_enable_instance_plan_cache = off"
                }],
            );
            let new_reader = || {
                let mut reader = super::create::new_mview_test_kit(&stack, next_conn_id());
                must_exec_all(
                    &mut reader,
                    &[
                        "use test",
                        "set tidb_mview_enable = on",
                        "set tidb_enable_prepared_plan_cache = on",
                        &format!("prepare lock_scope_stmt from 'select /*+ USE_PLAN_CACHE() */ t_lock.a, m.a from t_lock join {table} m on t_lock.a = m.a where t_lock.a > ? for update of t_lock'"),
                        "set @p = 0",
                        "begin pessimistic",
                    ],
                );
                reader
            };
            let mut probe = super::create::new_mview_test_kit(&stack, next_conn_id());
            must_exec_all(&mut probe, &["use test", "set tidb_mview_enable = on"]);
            set_maintenance(&mut probe, true);
            let mut check_lock_scope = |reader: &mut Session, cache_hit: bool| {
                assert_eq!(vec!["1 1", "2 2"], sorted(reader, "execute lock_scope_stmt using @p"), "{name}");
                assert_eq!(cache_hit, found_in_plan_cache(reader), "{name} instance={instance_cache}");
                // The user transaction must lock the ordinary target and leave MV/MLog rows available.
                must_exec_all(&mut probe, &["begin pessimistic"]);
                assert_eq!(vec!["1", "2"], sorted(&mut probe, &format!("select a from {table} for update nowait")), "{name}");
                must_exec_all(&mut probe, &["rollback", "begin pessimistic"]);
                must_get_err_code(&mut probe, "select * from t_lock for update nowait", ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET);
                must_exec_all(&mut probe, &["rollback"]);
            };
            let mut reader = new_reader();
            for execution in 0..3 {
                check_lock_scope(&mut reader, execution > 0);
            }
            must_exec_all(&mut reader, &["rollback"]);
            if instance_cache {
                // A different session must preserve the same scope when sharing this cached plan.
                let mut other = new_reader();
                check_lock_scope(&mut other, true);
                must_exec_all(&mut other, &["rollback"]);
            }
            set_maintenance(&mut probe, false);
        }
        must_exec_all(&mut tk, &["set global tidb_enable_instance_plan_cache = off"]);

        // "internal_maintenance"
        set_maintenance(&mut tk, true);
        must_exec_all(&mut tk, &["begin pessimistic"]);
        must_query(&mut tk, &format!("{point_sql} for update"));
        must_query(&mut tk, &format!("{batch_sql} for update"));
        must_query(&mut tk, &format!("select * from {table} for update"));
        must_exec_all(&mut tk, &["rollback"]);
        set_maintenance(&mut tk, false);
    }

    // "ordinary_tables"
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(
        &mut tk,
        &[
            "use test",
            "create table t_lock_left (a int primary key)",
            "create table t_lock_right (a int primary key)",
            "insert into t_lock_left values (1), (2)",
            "insert into t_lock_right values (1), (2)",
        ],
    );
    for instance_cache in [false, true] {
        must_exec_all(
            &mut tk,
            &[if instance_cache {
                "set global tidb_enable_instance_plan_cache = on"
            } else {
                "set global tidb_enable_instance_plan_cache = off"
            }],
        );
        let mut reader = super::create::new_mview_test_kit(&stack, next_conn_id());
        must_exec_all(&mut reader, &["use test", "set tidb_enable_prepared_plan_cache = on", "set @p = 0"]);
        let mut probe = super::create::new_mview_test_kit(&stack, next_conn_id());
        must_exec_all(&mut probe, &["use test"]);
        for (case, clause, locks) in [
            ("left", "for update of t_lock_left", [true, false]),
            ("right", "for update of t_lock_right", [false, true]),
            ("all", "for update", [true, true]),
        ] {
            must_exec_all(
                &mut reader,
                &[&format!("prepare lock_targets from 'select /*+ USE_PLAN_CACHE() */ t_lock_left.a, t_lock_right.a from t_lock_left join t_lock_right on t_lock_left.a = t_lock_right.a where t_lock_left.a > ? {clause}'")],
            );
            for execution in 0..3 {
                must_exec_all(&mut reader, &["begin pessimistic"]);
                assert_eq!(vec!["1 1", "2 2"], sorted(&mut reader, "execute lock_targets using @p"), "{case}");
                assert_eq!(execution > 0, found_in_plan_cache(&mut reader), "{case} instance={instance_cache} execution={execution}");
                for (table_index, table) in ["t_lock_left", "t_lock_right"].into_iter().enumerate() {
                    must_exec_all(&mut probe, &["begin pessimistic"]);
                    let sql = format!("select * from {table} for update nowait");
                    if locks[table_index] {
                        must_get_err_code(&mut probe, &sql, ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET);
                    } else {
                        assert_eq!(vec!["1", "2"], sorted(&mut probe, &sql), "{case}");
                    }
                    must_exec_all(&mut probe, &["rollback"]);
                }
                must_exec_all(&mut reader, &["rollback"]);
            }
        }
    }
}
