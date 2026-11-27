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

//! Go master `pkg/executor/test/materializedviewlog/materialized_view_purge_test.go`
//! (its cancel-job, cancel-watcher and heartbeat tests live with the task
//! monitor's in `materialized_view.rs`).

use super::*;

fn mview_test_kit() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>, Session) {
    let (stack, users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test"]);
    (stack, users, tk)
}

/// Go `mustExecMViewPurgeInternal`: the statement as materialized view
/// maintenance's restricted SQL.
fn must_exec_mview_purge_internal(tk: &mut Session, sql: &str) {
    assert_eq!(exec_refresh_internal(tk, sql), "OK", "{sql}");
}

fn user_session(stack: &UnistoreClusterStack, users: &ConfiguredUserStore, connection_id: u64, user: &str) -> Session {
    let mut session = stack
        .factory
        .open_session(super::super::super::node_fixture::session_context_with_identity(connection_id, log_in(users, user)))
        .unwrap();
    must_exec_all(&mut session, &["set tidb_mview_enable = on", "use test"]);
    session
}

#[test]
fn test_purge_materialized_view_log_no_db() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    // Go `errno.ErrNoDB`.
    let error = answer(&mut tk, "purge materialized view log on t");
    assert!(error.starts_with("ERR 1046 "), "{error}");
}

#[test]
fn test_purge_materialized_view_log_missing_mlog() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_purge_no_mlog (a int)"]);
    exec_err_contains(&mut tk, "purge materialized view log on t_purge_no_mlog", "materialized view log does not exist");
}

#[test]
fn test_purge_materialized_view_log_uses_current_session_mv_maintain_mem_quota() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_quota (id int primary key, v int)",
        "create materialized view log on t_purge_quota (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_quota values (1, 10), (2, 20), (3, 30)",
        "set @@session.tidb_mem_quota_query = 1073741824",
        "set @@global.tidb_mview_maintain_mem_quota = 536870912",
        "set @@session.tidb_mview_maintain_mem_quota = 268435456",
    ]);
    let applied = Arc::new(std::sync::Mutex::new(None::<(i64, i64)>));
    let _fp = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeSession", {
        let applied = applied.clone();
        move |arg: &mut dyn std::any::Any| *applied.lock().unwrap() = arg.downcast_ref::<(i64, i64)>().copied()
    });
    must_exec_all(&mut tk, &["purge materialized view log on t_purge_quota"]);
    assert_eq!(applied.lock().unwrap().take(), Some((268435456, 268435456)));
    must_exec_all(&mut tk, &["insert into t_purge_quota values (4, 40)"]);
    must_exec_mview_purge_internal(&mut tk, "purge materialized view log on t_purge_quota");
    assert_eq!(applied.lock().unwrap().take(), Some((268435456, 268435456)));
    must_exec_all(&mut tk, &[&format!("set @@global.tidb_mview_maintain_mem_quota = {}", 2i64 * 1024 * 1024 * 1024)]);
}

#[test]
fn test_purge_materialized_view_log_manual_sql_fails_when_apply_maintenance_mem_quota_fails() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_apply_quota_manual (id int primary key, v int)",
        "create materialized view log on t_purge_apply_quota_manual (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_apply_quota_manual values (1, 10), (2, 20)",
    ]);
    let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVMaintenanceMemQuotaApplyError", "return(true)");
    let error = exec_to_err(&mut tk, "purge materialized view log on t_purge_apply_quota_manual");
    assert!(error.contains("mock mv maintenance mem quota apply error"), "{error}");
}

#[test]
fn test_purge_materialized_view_log_internal_sql_falls_back_when_apply_maintenance_mem_quota_fails() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_apply_quota_internal (id int primary key, v int)",
        "create materialized view log on t_purge_apply_quota_internal (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_apply_quota_internal values (1, 10), (2, 20)",
        "set @@session.tidb_mem_quota_query = 1073741824",
        "set @@session.tidb_mview_maintain_mem_quota = 268435456",
    ]);
    let _error = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVMaintenanceMemQuotaApplyError", "return(true)");
    let applied = Arc::new(std::sync::Mutex::new(None::<(i64, i64)>));
    let _applied = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeSession", {
        let applied = applied.clone();
        move |arg: &mut dyn std::any::Any| *applied.lock().unwrap() = arg.downcast_ref::<(i64, i64)>().copied()
    });
    must_exec_mview_purge_internal(&mut tk, "purge materialized view log on t_purge_apply_quota_internal");
    assert_eq!(applied.lock().unwrap().take(), Some((1073741824, 268435456)));
}

/// Captures the last `(current, target)` read engines the purge session
/// reported.
fn capture_purge_engines() -> (Arc<std::sync::Mutex<Option<(String, String)>>>, tidb_failpoint::Guard) {
    let got = Arc::new(std::sync::Mutex::new(None));
    let guard = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvMaintainIsolationReadEnginesAppliedOnPurgeSession", {
        let got = got.clone();
        move |arg: &mut dyn std::any::Any| *got.lock().unwrap() = arg.downcast_ref::<(String, String)>().cloned()
    });
    (got, guard)
}

#[test]
fn test_purge_materialized_view_log_uses_current_session_mv_maintain_isolation_read_engines() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_isolation (id int primary key, v int)",
        "create materialized view log on t_purge_isolation (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_isolation values (1, 10), (2, 20), (3, 30)",
        "set @@session.tidb_isolation_read_engines = 'tikv,tiflash,tidb'",
        "set @@session.tidb_mview_maintain_isolation_read_engines = 'tikv'",
    ]);
    let (got, _fp) = capture_purge_engines();
    must_exec_all(&mut tk, &["purge materialized view log on t_purge_isolation"]);
    assert_eq!(got.lock().unwrap().take(), Some(("tikv".to_owned(), "tikv".to_owned())));
    assert_eq!(single_value(&mut tk, "select @@session.tidb_isolation_read_engines"), "tikv,tiflash,tidb");
    must_exec_all(&mut tk, &["insert into t_purge_isolation values (4, 40)"]);
    must_exec_mview_purge_internal(&mut tk, "purge materialized view log on t_purge_isolation");
    assert_eq!(got.lock().unwrap().take(), Some(("tikv".to_owned(), "tikv".to_owned())));
    assert_eq!(single_value(&mut tk, "select @@session.tidb_isolation_read_engines"), "tikv,tiflash,tidb");
}

#[test]
fn test_purge_materialized_view_log_default_mv_maintain_isolation_read_engines_does_not_inherit_current_session() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_isolation_default (id int primary key, v int)",
        "create materialized view log on t_purge_isolation_default (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_isolation_default values (1, 10), (2, 20), (3, 30)",
    ]);
    let default_engines = single_value(&mut tk, "select @@global.tidb_mview_maintain_isolation_read_engines");
    // Go `differentIsolationReadEnginesForTest`.
    let current = ["tikv", "tiflash", "tidb", "tikv,tidb", "tikv,tiflash", "tikv,tiflash,tidb"]
        .into_iter()
        .find(|candidate| *candidate != default_engines)
        .unwrap_or("tikv");
    must_exec_all(&mut tk, &[&format!("set @@session.tidb_isolation_read_engines = '{current}'")]);
    let (got, _fp) = capture_purge_engines();
    must_exec_all(&mut tk, &["purge materialized view log on t_purge_isolation_default"]);
    assert_eq!(got.lock().unwrap().take(), Some((default_engines.clone(), default_engines)));
    assert_eq!(single_value(&mut tk, "select @@session.tidb_isolation_read_engines"), current);
}

#[test]
fn test_purge_materialized_view_log_privilege() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_priv (a int)",
        "create materialized view log on t_purge_priv (a) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_purge_priv (a, cnt) as select a, count(1) from t_purge_priv group by a",
        // Go creates the users without a password; a Rust session logs in
        // with the fixture's.
        "create user 'u1'@'%' identified by 'abc'",
        "create user 'u2'@'%' identified by 'abc'",
        "create user 'u3'@'%' identified by 'abc'",
        "grant select on test.t_purge_priv to 'u1'@'%'",
        "grant operate view on test.`$mlog$t_purge_priv` to 'u2'@'%'",
        "grant operate view on *.* to 'u3'@'%'",
    ]);
    let mut u1 = user_session(&stack, &users, 3901, "u1");
    exec_err_contains(&mut u1, "purge materialized view log on t_purge_priv", "OPERATE VIEW command denied");
    must_exec_all(&mut user_session(&stack, &users, 3902, "u2"), &["purge materialized view log on t_purge_priv"]);
    must_exec_all(&mut tk, &["drop materialized view mv_purge_priv"]);
    must_exec_all(&mut user_session(&stack, &users, 3903, "u3"), &["purge materialized view log on t_purge_priv"]);
    must_exec_all(&mut tk, &["drop user 'u1'@'%'", "drop user 'u2'@'%'", "drop user 'u3'@'%'"]);
}

#[test]
fn test_purge_materialized_view_log_disallow_explicit_transaction() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_mlog_purge_txn (a int not null, b int not null)",
        "create materialized view log on t_mlog_purge_txn (a, b) purge next date_add(now(), interval 1 hour)",
        "begin",
    ]);
    let refused = "cannot run PURGE MATERIALIZED VIEW LOG in explicit transaction";
    assert_eq!(exec_to_err(&mut tk, "purge materialized view log on t_mlog_purge_txn"), refused);
    must_exec_all(&mut tk, &["rollback", r#"prepare stmt from "purge materialized view log on t_mlog_purge_txn""#, "begin"]);
    assert_eq!(exec_to_err(&mut tk, "execute stmt"), refused);
    must_exec_all(&mut tk, &["rollback", "deallocate prepare stmt"]);
}

/// Go `sessionctx.LastExecuteDDL` after a statement: set by a DDL node
/// (`session.go`), which the server's query metrics skip; this node's server
/// keys that skip on `Stmt::Ddl` (`mysql_connection.rs`).
fn last_execute_ddl(tk: &Session, sql: &str) -> bool {
    matches!(tk.parse(sql).unwrap(), tidb_ast::Stmt::Ddl(_))
}

#[test]
fn test_purge_materialized_view_log_does_not_mark_ddl_execution() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_mlog_purge_ddl_flag (id int primary key, v int)"]);
    for (sql, ddl) in [
        ("create materialized view log on t_mlog_purge_ddl_flag (id, v) purge next date_add(now(), interval 1 hour)", true),
        ("insert into t_mlog_purge_ddl_flag values (1, 10), (2, 20)", false),
        ("purge materialized view log on t_mlog_purge_ddl_flag", false),
    ] {
        must_exec_all(&mut tk, &[sql]);
        assert_eq!(last_execute_ddl(&tk, sql), ddl, "{sql}");
    }
}

#[test]
fn test_truncate_materialized_view_related_tables_rejected() {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_truncate_mv (a int not null, b int)",
        "create materialized view log on t_truncate_mv (a, b)",
    ]);
    exec_err_contains(&mut tk, "truncate table t_truncate_mv", "TRUNCATE TABLE on base table with materialized view log");
    exec_err_contains(&mut tk, "truncate table `$mlog$t_truncate_mv`", "TRUNCATE TABLE on materialized view log table");
    must_exec_all(&mut tk, &[
        "create materialized view mv_truncate_mv (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_truncate_mv group by a",
    ]);
    exec_err_contains(&mut tk, "truncate table mv_truncate_mv", "TRUNCATE TABLE on materialized view table");
    exec_err_contains(&mut tk, "truncate table `$mlog$t_truncate_mv`", "TRUNCATE TABLE on materialized view log table");
    exec_err_contains(&mut tk, "truncate table t_truncate_mv", "TRUNCATE TABLE on base table with materialized view dependencies");
    must_exec_all(&mut tk, &[
        "create table t_normal_truncate (a int)",
        "insert into t_normal_truncate values (1), (2)",
        "truncate table t_normal_truncate",
    ]);
    assert_eq!(single_value(&mut tk, "select count(*) from t_normal_truncate"), "0");
}

/// Go `mustQueryMViewPurgeInternal`: the first cell of the query's first
/// row, run as materialized view maintenance's restricted SQL.
fn must_query_mview_purge_internal_first(tk: &mut Session, sql: &str) -> String {
    let original = (tk.vars.in_mview_maintenance, tk.vars.restricted_sql);
    (tk.vars.in_mview_maintenance, tk.vars.restricted_sql) = (true, true);
    let value = single_value(tk, sql);
    (tk.vars.in_mview_maintenance, tk.vars.restricted_sql) = original;
    value
}

fn read_affected_rows_metric_value(label: &str) -> f64 {
    tidb_metrics::executor::AFFECTED_ROWS.with_label_values(&[label]).get()
}

/// Waits until the DDL notifier has consumed every published event. The
/// affected-rows counters are process-wide, and the notifier removes each
/// delivered event with a one-row `DELETE` on its next one-second poll (Go
/// `tableStore.DeleteAndCommit`), which lands in the "Delete" counter up to a
/// second after the DDL statement returned; a test that reads the counter
/// around a statement of its own starts from a notifier with nothing left to
/// delete.
pub(super) fn wait_ddl_notifier_drained(tk: &mut Session) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while single_value(tk, "select count(*) from mysql.tidb_ddl_notifier") != "0" {
        assert!(std::time::Instant::now() < deadline, "the DDL notifier kept undelivered events");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn last_purge_hist_row(tk: &mut Session, mlog_id: i64, columns: &str) -> Vec<String> {
    super::refresh::go_check(
        tk,
        &format!("select {columns} from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} order by PURGE_JOB_ID desc limit 1"),
    )
}

fn mlog_table_id(stack: &UnistoreClusterStack, name: &str) -> i64 {
    latest_table(stack, name).id
}

#[test]
fn test_purge_materialized_view_log_lock_row_missing() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_lock_row_missing (a int)",
        "create materialized view log on t_purge_lock_row_missing (a) purge next date_add(now(), interval 1 hour)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_lock_row_missing");
    must_exec_all(&mut tk, &[&format!("delete from mysql.tidb_mlog_purge_info where mlog_id = {mlog_id}")]);
    exec_err_contains(&mut tk, "purge materialized view log on t_purge_lock_row_missing", "mlog purge lock row does not exist");
}

#[test]
fn test_purge_materialized_view_log_nowait_conflict() {
    let (stack, _users, mut tk1) = mview_test_kit();
    let mut tk2 = super::create::new_mview_test_kit(&stack, 3911);
    must_exec_all(&mut tk1, &[
        "create table t_purge_nowait_conflict (a int)",
        "create materialized view log on t_purge_nowait_conflict (a) purge next date_add(now(), interval 1 hour)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_nowait_conflict");
    let lock_name = format!("mlog_purge_{mlog_id}");
    assert_eq!(single_value(&mut tk1, &format!("select get_lock('{lock_name}', 1)")), "1");
    // Go `errno.ErrMVAdvisoryLockConflict`.
    let refused = answer(&mut tk2, "purge materialized view log on t_purge_nowait_conflict");
    assert!(refused.starts_with("ERR 8182 "), "{refused}");
    assert_eq!(single_value(&mut tk1, &format!("select release_lock('{lock_name}')")), "1");
    must_exec_all(&mut tk1, &["begin pessimistic"]);
    assert_eq!(single_value(&mut tk1, &format!("select 1 from mysql.tidb_mlog_purge_info where mlog_id = {mlog_id} for update")), "1");
    // Go `ErrorContains` reads `Error()`: the annotation over the wire's cause.
    let error = exec_to_err(&mut tk2, "purge materialized view log on t_purge_nowait_conflict");
    assert!(error.contains("another purge is running"), "{error}");
    must_exec_all(&mut tk1, &["rollback"]);
    must_exec_all(&mut tk2, &["purge materialized view log on t_purge_nowait_conflict"]);
}

#[test]
fn test_purge_materialized_view_log_batch_delete() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_batch_delete (id int primary key, v int)",
        "create materialized view log on t_purge_batch_delete (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_batch_delete values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_batch_delete");
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_batch_delete`"), "5");
    let max_commit_ts: u64 = must_query_mview_purge_internal_first(&mut tk, "select max(_tidb_commit_ts) from `$mlog$t_purge_batch_delete`").parse().unwrap();
    must_exec_all(&mut tk, &["set @@session.tidb_mlog_purge_batch_size = 2"]);
    wait_ddl_notifier_drained(&mut tk);
    let (before_delete, before_purge) = (read_affected_rows_metric_value("Delete"), read_affected_rows_metric_value("PurgeMLog"));
    must_exec_all(&mut tk, &["purge materialized view log on t_purge_batch_delete"]);
    assert_eq!(tk.affected_rows(), 5);
    assert_eq!(tk.statement_message(), "Rows inserted: 0  Updated: 0  Deleted: 5");
    assert_eq!(read_affected_rows_metric_value("Delete") - before_delete, 0.0);
    assert_eq!(read_affected_rows_metric_value("PurgeMLog") - before_purge, 5.0);
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_batch_delete`"), "0");
    assert_eq!(
        last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_ROWS, PURGE_DURATION_SEC = cast(timestampdiff(microsecond, PURGE_START_TIME, PURGE_END_TIME) as decimal(18,6)) / 1000000, PURGE_DURATION_SEC >= 0"),
        vec!["success 5 1 1"]
    );
    assert_eq!(last_purge_hist_row(&mut tk, mlog_id, &format!("PURGE_CUTOFF_TSO is not null, PURGE_CUTOFF_TSO >= {max_commit_ts}")), vec!["1 1"]);
    assert_eq!(last_purge_hist_row(&mut tk, mlog_id, "BASE_TABLE_SCHEMA, BASE_TABLE_NAME"), vec!["test t_purge_batch_delete"]);
    assert_eq!(
        single_value(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_hist where BASE_TABLE_SCHEMA = 'TEST' and BASE_TABLE_NAME = 'T_PURGE_BATCH_DELETE' and MLOG_ID = {mlog_id}")),
        "1"
    );
    assert_eq!(
        super::refresh::go_check(&mut tk, &format!("select LAST_PURGED_TSO is not null, LAST_PURGED_TSO >= {max_commit_ts} from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")),
        vec!["1 1"]
    );
}

#[test]
fn test_purge_materialized_view_log_last_purged_tso_short_circuit() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_short_circuit (a int not null, b int)",
        "create materialized view log on t_purge_short_circuit (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_purge_short_circuit (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_purge_short_circuit group by a",
        "insert into t_purge_short_circuit values (1, 10), (1, 20), (2, 30)",
    ]);
    let mv_id = latest_table(&stack, "mv_purge_short_circuit").id;
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_short_circuit");
    let max_commit_ts = must_query_mview_purge_internal_first(&mut tk, "select max(_tidb_commit_ts) from `$mlog$t_purge_short_circuit`");
    must_exec_all(&mut tk, &[
        &format!("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = {max_commit_ts} where MVIEW_ID = {mv_id}"),
        "set @@session.tidb_mlog_purge_batch_size = 2",
        "purge materialized view log on t_purge_short_circuit",
    ]);
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_short_circuit`"), "0");
    let last_purged = format!("select LAST_PURGED_TSO from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}");
    assert_eq!(single_value(&mut tk, &last_purged), max_commit_ts);
    let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogDeleteErr", "return(true)");
    // "`LAST_PURGED_TSO >= safe_purge_tso` should short-circuit before
    // delete SQL execution."
    must_exec_all(&mut tk, &["purge materialized view log on t_purge_short_circuit"]);
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_short_circuit`"), "0");
    assert_eq!(last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_ROWS"), vec!["success 0"]);
    assert_eq!(single_value(&mut tk, &last_purged), max_commit_ts);
}

#[test]
fn test_purge_materialized_view_log_skips_when_cutoff_fence_would_go_backward() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_cutoff_fence (a int not null, b int)",
        "create materialized view log on t_purge_cutoff_fence (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_purge_cutoff_fence (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_purge_cutoff_fence group by a",
        "insert into t_purge_cutoff_fence values (1, 10), (1, 20), (2, 30)",
    ]);
    let mv_id = latest_table(&stack, "mv_purge_cutoff_fence").id;
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_cutoff_fence");
    let max_commit_ts: u64 = must_query_mview_purge_internal_first(&mut tk, "select max(_tidb_commit_ts) from `$mlog$t_purge_cutoff_fence`").parse().unwrap();
    let cutoff_fence_tso = max_commit_ts + 1000;
    must_exec_all(&mut tk, &[
        &format!("update mysql.tidb_mview_refresh_info set LAST_SUCCESS_READ_TSO = {max_commit_ts} where MVIEW_ID = {mv_id}"),
        &format!(
            "insert into mysql.tidb_mlog_purge_hist (PURGE_JOB_ID, MLOG_ID, BASE_TABLE_SCHEMA, BASE_TABLE_NAME, PURGE_METHOD, PURGE_START_TIME, PURGE_ROWS, PURGE_STATUS, PURGE_CUTOFF_TSO, LAST_HEARTBEAT_TIME) values ({cutoff_fence_tso}, {mlog_id}, 'test', 't_purge_cutoff_fence', 'manual', now(6), 0, 'success', {cutoff_fence_tso}, now(6))"
        ),
        "purge materialized view log on t_purge_cutoff_fence",
    ]);
    assert_eq!(tk.affected_rows(), 0);
    assert_eq!(tk.statement_message(), "Rows inserted: 0  Updated: 0  Deleted: 0");
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_cutoff_fence`"), "3");
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id}")), "1");
    assert_eq!(last_purge_hist_row(&mut tk, mlog_id, "PURGE_CUTOFF_TSO"), vec![cutoff_fence_tso.to_string()]);
    assert_eq!(single_value(&mut tk, &format!("select LAST_PURGED_TSO is null from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")), "1");
}

#[test]
fn test_purge_materialized_view_log_delete_error_no_dirty_write() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_delete_err (id int primary key, v int)",
        "create materialized view log on t_purge_delete_err (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_delete_err values (1, 10), (2, 20), (3, 30)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_delete_err");
    let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogDeleteErr", "return(true)");
    let error = exec_to_err(&mut tk, "purge materialized view log on t_purge_delete_err");
    assert!(error.contains("mock purge mlog delete error"), "{error}");
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_delete_err`"), "3");
    assert_eq!(
        last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_ROWS, PURGE_END_TIME is not null, PURGE_FAILED_REASON like '%mock purge mlog delete error%'"),
        vec!["failed 0 1 1"]
    );
}

#[test]
fn test_purge_materialized_view_log_early_failure_writes_hist() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_early_fail (id int primary key, v int)",
        "create materialized view log on t_purge_early_fail (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_early_fail values (1, 10), (2, 20), (3, 30)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_early_fail");
    // t.Run("panic preserves error")
    {
        let _panic = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvMaintainMemQuotaAppliedOnPurgeSession", |_| {
            std::panic::panic_any(tidb_terror::MysqlError::from_terror(&tidb_exeerrors::ERR_QUERY_INTERRUPTED.gen_with_stack_by_args(&[])))
        });
        // Go `exeerrors.ErrQueryInterrupted.Equal(err)`: the panic's error is
        // the statement's.
        let error = answer(&mut tk, "purge materialized view log on t_purge_early_fail");
        assert!(error.starts_with("ERR 1317 "), "{error}");
        assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_early_fail`"), "3");
        assert_eq!(last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_ROWS, PURGE_END_TIME is not null"), vec!["failed 0 1"]);
    }
    let before: i64 = single_value(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id}")).parse().unwrap();
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogErrorBeforeInsertHist",
        r#"return("mock early purge failure")"#,
    );
    let error = exec_to_err(&mut tk, "purge materialized view log on t_purge_early_fail");
    assert!(error.contains("mock early purge failure"), "{error}");
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id}")), (before + 1).to_string());
    assert_eq!(
        last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_METHOD = 'manual', PURGE_ROWS, PURGE_START_TIME is not null, PURGE_END_TIME is not null, PURGE_DURATION_SEC = cast(timestampdiff(microsecond, PURGE_START_TIME, PURGE_END_TIME) as decimal(18,6)) / 1000000, PURGE_FAILED_REASON is not null"),
        vec!["failed 1 0 1 1 1 1"]
    );
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} and PURGE_STATUS = 'running'")), "0");
    let reason = last_purge_hist_row(&mut tk, mlog_id, "PURGE_FAILED_REASON");
    assert!(reason[0].contains("mock early purge failure"), "{reason:?}");
}

#[test]
fn test_purge_materialized_view_log_finalize_failure_after_commit_is_warning() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_finalize_warn (id int primary key, v int)",
        "create materialized view log on t_purge_finalize_warn (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_finalize_warn values (1, 10), (2, 20), (3, 30)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_finalize_warn");
    let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogFinalizeSuccessErr", "return(true)");
    must_exec_all(&mut tk, &["purge materialized view log on t_purge_finalize_warn"]);
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_finalize_warn`"), "0");
    assert_eq!(last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_END_TIME is null"), vec!["running 1"]);
}

#[test]
fn test_purge_materialized_view_log_finalize_retry_succeeds() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_finalize_retry (id int primary key, v int)",
        "create materialized view log on t_purge_finalize_retry (id, v) purge next date_add(now(), interval 1 hour)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_finalize_retry");
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockUpdateMaterializedViewLogPurgeStateErr",
        "1*return(true)->return(false)",
    );
    must_exec_all(&mut tk, &["purge materialized view log on t_purge_finalize_retry"]);
    assert_eq!(last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_END_TIME is not null, PURGE_ROWS"), vec!["success 1 0"]);
}

/// Go `TestPurgeMaterializedViewLogFinalizeFailureUsesWithoutCancel`: a
/// purge whose statement context is canceled after its running history row
/// fails with the context's error, and its history row is still finalized
/// (`finalizeCtx`).
#[test]
fn test_purge_materialized_view_log_finalize_failure_uses_without_cancel() {
    let (stack, _users, mut tk) = mview_test_kit();
    let mut observer = super::create::new_mview_test_kit(&stack, 3923);
    must_exec_all(&mut tk, &[
        "create table t_purge_cancel_finalize (id int primary key, v int)",
        "create materialized view log on t_purge_cancel_finalize (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_cancel_finalize values (1, 10)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_cancel_finalize");
    let pause = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/pausePurgeMaterializedViewLogAfterInsertPurgeHistRunning",
        "pause",
    );
    let (ctx, cancel) = tidb_gocontext::Context::with_cancel(&tidb_gocontext::Context::background());
    let purge = std::thread::spawn(move || {
        tk.exec_with_context(&ctx, "purge materialized view log on t_purge_cancel_finalize").map(|_| ()).map_err(|e| format!("{e:?}"))
    });
    eventually_single_value(
        &mut observer,
        &format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} and PURGE_STATUS = 'running'"),
        "1",
    );
    cancel.cancel();
    drop(pause);
    let error = purge.join().unwrap().unwrap_err();
    assert!(error.contains("context canceled"), "{error}");
    assert_eq!(
        last_purge_hist_row(&mut observer, mlog_id, "PURGE_STATUS, PURGE_END_TIME is not null, PURGE_FAILED_REASON like '%context canceled%'"),
        vec!["failed 1 1"]
    );
}

fn next_purge_unix_seconds(tk: &mut Session, mlog_id: i64) -> String {
    super::refresh::go_check(tk, &format!("select NEXT_PURGE_UNIX_SECONDS from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")).remove(0)
}

#[test]
fn test_purge_materialized_view_log_delete_error_after_partial_success() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_partial_delete_err (id int primary key, v int)",
        "create materialized view log on t_purge_partial_delete_err (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_partial_delete_err values (1, 10), (2, 20), (3, 30)",
        "set @@session.tidb_mlog_purge_batch_size = 1",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_partial_delete_err");
    let before_next = next_purge_unix_seconds(&mut tk, mlog_id);
    assert_eq!(single_value(&mut tk, &format!("select LAST_PURGED_TSO is null from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")), "1");
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogDeleteErr",
        "1*return(false)->return(true)",
    );
    let error = exec_to_err(&mut tk, "purge materialized view log on t_purge_partial_delete_err");
    assert!(error.contains("mock purge mlog delete error"), "{error}");
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_partial_delete_err`"), "2");
    assert_eq!(
        last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_ROWS, PURGE_FAILED_REASON like '%mock purge mlog delete error%'"),
        vec!["failed 1 1"]
    );
    assert_eq!(
        super::refresh::go_check(&mut tk, &format!("select LAST_PURGED_TSO is null, NEXT_PURGE_UNIX_SECONDS from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")),
        vec![format!("1 {before_next}")]
    );
}

/// Go's `require.Eventually` over a single-value query.
fn eventually_single_value(tk: &mut Session, sql: &str, want: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while single_value(tk, sql) != want {
        assert!(std::time::Instant::now() < deadline, "{sql} never answered {want}");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

#[test]
fn test_purge_materialized_view_log_manual_cancel_after_partial_success() {
    let (stack, _users, mut tk) = mview_test_kit();
    let mut observer = super::create::new_mview_test_kit(&stack, 3921);
    must_exec_all(&mut tk, &[
        "create table t_purge_partial_delete_cancel (id int primary key, v int)",
        "create materialized view log on t_purge_partial_delete_cancel (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_partial_delete_cancel values (1, 10), (2, 20), (3, 30)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_partial_delete_cancel");
    let before_next = next_purge_unix_seconds(&mut tk, mlog_id);
    must_exec_all(&mut tk, &["set @@session.tidb_mlog_purge_batch_size = 1"]);
    let _poll = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMVTaskMonitorPollInterval", "return(50)");
    let pause = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/pausePurgeMaterializedViewLogAfterDeleteBatch", "pause");
    let mut tk_purge = super::create::new_mview_test_kit(&stack, 3922);
    must_exec_all(&mut tk_purge, &["set @@session.tidb_mlog_purge_batch_size = 1"]);
    let purge = std::thread::spawn(move || exec_to_err(&mut tk_purge, "purge materialized view log on t_purge_partial_delete_cancel"));
    eventually_single_value(&mut observer, "select count(*) from `$mlog$t_purge_partial_delete_cancel`", "2");
    eventually_single_value(
        &mut observer,
        &format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} and PURGE_STATUS = 'running'"),
        "1",
    );
    let purge_job_id = single_value(
        &mut observer,
        &format!("select PURGE_JOB_ID from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id} and PURGE_STATUS = 'running' limit 1"),
    );
    let monitor_name: &'static str = Box::leak(format!("mlog-purge-{purge_job_id}").into_boxed_str());
    let (cancel_observed, _watch) = wait_cancel_watcher_requested(monitor_name);
    let requester = "'partial_cancel_req'@'stage-d'";
    must_exec_all(&mut observer, &[&format!(
        "UPDATE mysql.tidb_mlog_purge_hist SET CANCEL_REQUEST_TIME = NOW(6), CANCEL_REQUESTED_BY = '{}' WHERE MLOG_ID = {mlog_id} AND PURGE_STATUS = 'running' AND CANCEL_REQUEST_TIME IS NULL",
        requester.replace('\'', "''")
    )]);
    cancel_observed
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("timed out waiting for purge task monitor to observe manual cancel request");
    drop(pause);
    let error = purge.join().unwrap();
    assert!(error.contains("materialized view task canceled manually"), "{error}");
    assert_eq!(single_value(&mut observer, "select count(*) from `$mlog$t_purge_partial_delete_cancel`"), "2");
    assert_eq!(
        last_purge_hist_row(&mut observer, mlog_id, "PURGE_STATUS, PURGE_ROWS, PURGE_FAILED_REASON"),
        vec![format!("failed 1 cancelled manually by {requester}")]
    );
    assert_eq!(
        super::refresh::go_check(&mut observer, &format!("select LAST_PURGED_TSO is null, NEXT_PURGE_UNIX_SECONDS from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")),
        vec![format!("1 {before_next}")]
    );
}

#[test]
fn test_purge_materialized_view_log_begin_failure() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_begin_fail (id int primary key, v int)",
        "create materialized view log on t_purge_begin_fail (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_begin_fail values (1, 10), (2, 20), (3, 30)",
        "set @@session.tidb_mlog_purge_batch_size = 1",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_begin_fail");
    let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogBeginErr", "return(true)");
    let error = exec_to_err(&mut tk, "purge materialized view log on t_purge_begin_fail");
    assert!(error.contains("mock purge begin error"), "{error}");
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_begin_fail`"), "3");
    assert_eq!(
        last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_ROWS, PURGE_END_TIME is not null, PURGE_FAILED_REASON like '%mock purge begin error%'"),
        vec!["failed 0 1 1"]
    );
}

#[test]
fn test_purge_materialized_view_log_zero_start_ts() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_zero_start_ts (id int primary key, v int)",
        "create materialized view log on t_purge_zero_start_ts (id, v) purge next date_add(now(), interval 1 hour)",
        "insert into t_purge_zero_start_ts values (1, 10), (2, 20)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_zero_start_ts");
    let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockPurgeMaterializedViewLogZeroStartTS", "return(true)");
    let error = exec_to_err(&mut tk, "purge materialized view log on t_purge_zero_start_ts");
    assert!(error.contains("purge materialized view log: invalid transaction start tso"), "{error}");
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_zero_start_ts`"), "2");
    assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id}")), "1");
}

/// Go's three adaptive-throttle fallback tests: the throttle's own failure
/// falls back to unthrottled deletes.
fn adaptive_throttle_fallback(table: &str, purge_clause: &str, failpoint: &str, batch_size: Option<u32>) {
    let (_stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        &format!("create table {table} (id int primary key, v int)"),
        &format!("create materialized view log on {table} (id, v) {purge_clause}"),
        &format!("insert into {table} values (1, 10), (2, 20), (3, 30)"),
        "set @@session.tidb_mlog_purge_min_rate = 1",
        "set @@session.tidb_mlog_purge_rate_budget_ratio = 0.5",
    ]);
    if let Some(batch_size) = batch_size {
        must_exec_all(&mut tk, &[&format!("set @@session.tidb_mlog_purge_batch_size = {batch_size}")]);
    }
    let _fp = tidb_failpoint::enable_for_test(&format!("github.com/pingcap/tidb/pkg/executor/{failpoint}"), "return(true)");
    must_exec_all(&mut tk, &[&format!("purge materialized view log on {table}")]);
    assert_eq!(tk.affected_rows(), 3);
    assert_eq!(tk.statement_message(), "Rows inserted: 0  Updated: 0  Deleted: 3");
    assert_eq!(single_value(&mut tk, &format!("select count(*) from `$mlog${table}`")), "0");
}

#[test]
fn test_purge_materialized_view_log_adaptive_throttle_fallback_on_count_failure() {
    adaptive_throttle_fallback(
        "t_purge_adaptive_count_fail",
        "purge next date_add(now(), interval 1 hour)",
        "mockMLogPurgeAdaptiveCountErr",
        None,
    );
}

#[test]
fn test_purge_materialized_view_log_adaptive_throttle_fallback_on_deadline_failure() {
    adaptive_throttle_fallback(
        "t_purge_adaptive_deadline_fail",
        "purge start with now() next date_add(now(), interval 1 hour)",
        "mockMLogPurgeAdaptiveDeadlineErr",
        None,
    );
}

#[test]
fn test_purge_materialized_view_log_adaptive_throttle_fallback_on_sleep_failure() {
    adaptive_throttle_fallback(
        "t_purge_adaptive_sleep_fail",
        "purge next date_add(now(), interval 1 hour)",
        "mockMLogPurgeAdaptiveSleepErr",
        Some(1),
    );
}

#[test]
fn test_purge_materialized_view_log_missing_public_mview_refresh_row() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_missing_public_refresh (a int)",
        "create materialized view log on t_purge_missing_public_refresh (a) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_purge_missing_public_refresh (a, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, count(1) from t_purge_missing_public_refresh group by a",
    ]);
    let mv_id = latest_table(&stack, "mv_purge_missing_public_refresh").id;
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_missing_public_refresh");
    must_exec_all(&mut tk, &[&format!("delete from mysql.tidb_mview_refresh_info where mview_id = {mv_id}")]);
    exec_err_contains(&mut tk, "purge materialized view log on t_purge_missing_public_refresh", "materialized view refresh info is missing");
    // "Purge failure should still be finalized in history."
    assert_eq!(last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_ROWS, PURGE_END_TIME is not null"), vec!["failed 0 1"]);
}

#[test]
fn test_purge_materialized_view_log_writes_state() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_state (id int primary key, v int)",
        "create materialized view log on t_purge_state (id, v) purge next date_add(now(), interval 1 hour)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_state");
    for count in ["1", "2"] {
        must_exec_all(&mut tk, &["purge materialized view log on t_purge_state"]);
        assert_eq!(last_purge_hist_row(&mut tk, mlog_id, "PURGE_STATUS, PURGE_METHOD, PURGE_ROWS, PURGE_END_TIME is not null"), vec!["success manual 0 1"]);
        assert_eq!(single_value(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_hist where MLOG_ID = {mlog_id}")), count);
    }
}

#[test]
fn test_purge_materialized_view_log_updates_stats_meta_row_count() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_stats_meta (id int primary key, v int)",
        "create materialized view log on t_purge_stats_meta (id, v) purge next date_add(now(), interval 1 hour)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_stats_meta");
    must_exec_all(&mut tk, &["insert into t_purge_stats_meta values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)"]);
    let stats_meta = format!("select modify_count, count from mysql.stats_meta where table_id = {mlog_id}");
    stack.factory.dump_stats_delta_to_kv(true, &[]).unwrap();
    assert_eq!(super::refresh::go_check(&mut tk, &stats_meta), vec!["5 5"]);
    must_exec_all(&mut tk, &["purge materialized view log on t_purge_stats_meta"]);
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_stats_meta`"), "0");
    stack.factory.dump_stats_delta_to_kv(true, &[]).unwrap();
    assert_eq!(super::refresh::go_check(&mut tk, &stats_meta), vec!["10 0"]);
}

/// The log's metadata as the node's schema holds it: Go's tests write
/// `mlogTable.Meta().MaterializedViewLog` in place, which this shared
/// pointer is.
fn mlog_info(stack: &UnistoreClusterStack, name: &str) -> tidb_model::GoShared<tidb_model::table::MaterializedViewLogInfo> {
    latest_table(stack, name).materialized_view_log.expect("a materialized view log")
}

fn last_purge_method(tk: &mut Session, mlog_id: i64) -> String {
    last_purge_hist_row(tk, mlog_id, "PURGE_METHOD").remove(0)
}

#[test]
fn test_purge_materialized_view_log_next_unix_seconds_only_updates_for_internal_sql() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_internal_next (a int not null, b int not null)",
        "create materialized view log on t_purge_internal_next (a, b) purge start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute)",
    ]);
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_internal_next");
    let reset = format!("update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = null where MLOG_ID = {mlog_id}");
    let next = format!("select NEXT_PURGE_UNIX_SECONDS from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}");
    let purge = "purge materialized view log on t_purge_internal_next";
    must_exec_all(&mut tk, &[&reset]);
    // "User SQL purge should not update NEXT_PURGE_UNIX_SECONDS."
    must_exec_all(&mut tk, &[purge]);
    assert_eq!(single_value(&mut tk, &format!("select NEXT_PURGE_UNIX_SECONDS is null from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")), "1");
    assert_eq!(last_purge_method(&mut tk, mlog_id), "manual");
    // "Internal SQL purge should update NEXT_PURGE_UNIX_SECONDS by
    // evaluating PurgeNext."
    must_exec_mview_purge_internal(&mut tk, purge);
    assert_eq!(
        super::refresh::go_check(&mut tk, &format!("select NEXT_PURGE_UNIX_SECONDS is not null, NEXT_PURGE_UNIX_SECONDS > TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 20 minute), NEXT_PURGE_UNIX_SECONDS < TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', UTC_TIMESTAMP() + interval 2 hour) from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")),
        vec!["1 1 1"]
    );
    assert_eq!(last_purge_method(&mut tk, mlog_id), "auto");
    let info = mlog_info(&stack, "$mlog$t_purge_internal_next");
    let unix = |year, month, day| chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp().to_string();
    // "DATE schedule expressions are evaluated at midnight UTC."
    info.write().purge_next = "CAST('2030-01-02' AS DATE)".to_owned();
    must_exec_all(&mut tk, &[&reset]);
    must_exec_mview_purge_internal(&mut tk, purge);
    assert_eq!(single_value(&mut tk, &next), unix(2030, 1, 2));
    // "Runtime schedule parsing must use the SQL mode persisted with the
    // MLog."
    {
        let mut info = info.write();
        info.purge_next = "CAST(DATE_ADD('2030-01-01', INTERVAL (1 || 2) DAY) AS DATETIME)".to_owned();
        info.purge_schedule_sql_mode = i64::try_from(tidb_mysql::consts::ModePipesAsConcat.0).unwrap();
    }
    must_exec_all(&mut tk, &[&reset]);
    must_exec_mview_purge_internal(&mut tk, purge);
    assert_eq!(single_value(&mut tk, &next), unix(2030, 1, 13));
    {
        let mut info = info.write();
        info.purge_next = r"CAST(DATE_ADD('2030-01-01', INTERVAL CAST('1\2' AS UNSIGNED) DAY) AS DATETIME)".to_owned();
        info.purge_schedule_sql_mode = i64::try_from(tidb_mysql::consts::ModeNoBackslashEscapes.0).unwrap();
    }
    must_exec_all(&mut tk, &[&reset]);
    must_exec_mview_purge_internal(&mut tk, purge);
    assert_eq!(single_value(&mut tk, &next), unix(2030, 1, 2));
    info.write().purge_next = "CAST('2021-03-14 02:30:00' AS DATETIME)".to_owned();
    must_exec_all(&mut tk, &["set time_zone = 'America/Los_Angeles'"]);
    must_exec_mview_purge_internal(&mut tk, purge);
    assert_eq!(single_value(&mut tk, &next), "1615689000");
}

#[test]
fn test_purge_materialized_view_log_internal_sql_start_with_no_next_sets_next_unix_seconds_null() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_internal_start_only (a int not null, b int not null)",
        "create materialized view log on t_purge_internal_start_only (a, b) purge start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute)",
    ]);
    {
        // "Simulate scheduler metadata state: START WITH is set but NEXT is
        // empty."
        let info = mlog_info(&stack, "$mlog$t_purge_internal_start_only");
        let mut info = info.write();
        info.purge_start_with = "DATE_ADD(NOW(), INTERVAL 2 HOUR)".to_owned();
        info.purge_next = String::new();
    }
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_internal_start_only");
    must_exec_all(&mut tk, &[&format!("update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = UNIX_TIMESTAMP() + 3 * 60 * 60 where MLOG_ID = {mlog_id}")]);
    // "User SQL purge should keep NEXT_PURGE_UNIX_SECONDS unchanged."
    must_exec_all(&mut tk, &["purge materialized view log on t_purge_internal_start_only"]);
    assert_eq!(single_value(&mut tk, &format!("select NEXT_PURGE_UNIX_SECONDS is not null from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")), "1");
    assert_eq!(last_purge_method(&mut tk, mlog_id), "manual");
    // "Internal SQL purge should explicitly set NEXT_PURGE_UNIX_SECONDS =
    // NULL when START WITH exists and NEXT is empty."
    must_exec_mview_purge_internal(&mut tk, "purge materialized view log on t_purge_internal_start_only");
    assert_eq!(single_value(&mut tk, &format!("select NEXT_PURGE_UNIX_SECONDS is null from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")), "1");
    assert_eq!(last_purge_method(&mut tk, mlog_id), "auto");
}

#[test]
fn test_purge_materialized_view_log_internal_sql_no_schedule_sets_next_unix_seconds_null() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_purge_internal_no_schedule (a int not null, b int not null)",
        "create materialized view log on t_purge_internal_no_schedule (a, b) purge start with date_add(now(), interval 2 hour) next date_add(now(), interval 40 minute)",
    ]);
    {
        // "Simulate scheduler metadata state: schedule is fully removed."
        let info = mlog_info(&stack, "$mlog$t_purge_internal_no_schedule");
        let mut info = info.write();
        info.purge_start_with = String::new();
        info.purge_next = String::new();
    }
    let mlog_id = mlog_table_id(&stack, "$mlog$t_purge_internal_no_schedule");
    must_exec_all(&mut tk, &[&format!("update mysql.tidb_mlog_purge_info set NEXT_PURGE_UNIX_SECONDS = UNIX_TIMESTAMP() + 3 * 60 * 60 where MLOG_ID = {mlog_id}")]);
    must_exec_mview_purge_internal(&mut tk, "purge materialized view log on t_purge_internal_no_schedule");
    assert_eq!(single_value(&mut tk, &format!("select NEXT_PURGE_UNIX_SECONDS is null from mysql.tidb_mlog_purge_info where MLOG_ID = {mlog_id}")), "1");
    assert_eq!(last_purge_method(&mut tk, mlog_id), "auto");
}

/// Go master `TestMViewCommitTSAccessIndependentOfMViewEnable`
/// (4c3c87c5b44): `_tidb_commit_ts` reads do not depend on
/// `tidb_mview_enable`, and a purge-session delete by it removes exactly
/// what it matches.
#[test]
fn test_m_view_commit_ts_access_independent_of_m_view_enable() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "use test",
        "set tidb_mview_enable = on",
        "create table t_mlog_commit_ts_gate (id int primary key, v int)",
        "create materialized view log on t_mlog_commit_ts_gate (id, v)",
        "insert into t_mlog_commit_ts_gate values (1, 10)",
    ]);
    assert_eq!(single_value(&mut tk, "select _tidb_commit_ts > 0 from `$mlog$t_mlog_commit_ts_gate`"), "1");
    must_exec_all(&mut tk, &["set tidb_mview_enable = off"]);
    assert_eq!(single_value(&mut tk, "select _tidb_commit_ts > 0 from `$mlog$t_mlog_commit_ts_gate`"), "1");
    must_exec_all(&mut tk, &["delete from t_mlog_commit_ts_gate where _tidb_commit_ts = 0"]);
    assert_eq!(single_value(&mut tk, "select count(*) from t_mlog_commit_ts_gate"), "1");
    // "Pooled purge sessions start from the default global value (OFF). A
    // commit_ts predicate that matches nothing must leave the log row in
    // place."
    must_exec_all(&mut tk, &["set tidb_mview_enable = on"]);
    must_exec_mview_purge_internal(&mut tk, "delete from `$mlog$t_mlog_commit_ts_gate` where _tidb_commit_ts = 0");
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_mlog_commit_ts_gate`"), "1");
    must_exec_mview_purge_internal(&mut tk, "delete from `$mlog$t_mlog_commit_ts_gate` where _tidb_commit_ts > 0 limit 1");
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_mlog_commit_ts_gate`"), "0");
    must_exec_all(&mut tk, &[
        "insert into t_mlog_commit_ts_gate values (2, 20)",
        "purge materialized view log on t_mlog_commit_ts_gate",
    ]);
    assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_mlog_commit_ts_gate`"), "0");
}

#[test]
fn test_purge_materialized_view_log_next_null_log() {
    let message = "purge materialized view log: automatic purge schedule disabled because NEXT expression evaluated to NULL, updating NEXT_PURGE_UNIX_SECONDS to NULL";
    for (name, expr, want_null, want_log) in [
        ("no NEXT", "", "1", false),
        ("valid NEXT", "DATE_ADD(NOW(), INTERVAL 1 HOUR)", "0", false),
        ("NULL NEXT", "CAST(NULLIF('purge-next-secret', 'purge-next-secret') AS DATETIME)", "1", true),
    ] {
        let (stack, _users, mut tk) = mview_test_kit();
        must_exec_all(&mut tk, &[
            "create table src_null_next (g int not null, v int not null)",
            "create materialized view log on src_null_next (g,v) purge next date_add(now(), interval 1 hour)",
        ]);
        mlog_info(&stack, "$mlog$src_null_next").write().purge_next = expr.to_owned();
        let (observer, recorded) = tidb_logutil::observer::new(tidb_log::Level::Error);
        let restore = tidb_log::replace_globals(observer);
        must_exec_all(&mut tk, &["purge materialized view log on src_null_next"]);
        let manual_entries = recorded.filter_message(message).len();
        let manual_next = single_value(&mut tk, "select next_purge_unix_seconds is not null from mysql.tidb_mlog_purge_info");
        must_exec_mview_purge_internal(&mut tk, "purge materialized view log on src_null_next");
        let internal_null = single_value(&mut tk, "select next_purge_unix_seconds is null from mysql.tidb_mlog_purge_info");
        let entries = recorded.filter_message(message);
        restore.restore();
        assert_eq!(manual_entries, 0, "{name}");
        assert_eq!(manual_next, "1", "{name}");
        assert_eq!(internal_null, want_null, "{name}");
        if want_log {
            assert!(!entries.is_empty(), "{name}");
            for entry in entries {
                let context = entry.context_map();
                assert_eq!(context.get("schemaName").map(String::as_str), Some("test"), "{name}");
                assert_eq!(context.get("tableName").map(String::as_str), Some("$mlog$src_null_next"), "{name}");
                assert!(!format!("{context:?}").contains("purge-next-secret"), "{name}: {context:?}");
            }
        } else {
            assert!(entries.is_empty(), "{name}: {entries:?}");
        }
    }
}

/// Go `testkit.CreateMockStoreAndDomainWithSchemaLease(..., mockstore.WithMockTiFlash(2))`
/// and `newMViewTestKit`.
fn mock_tiflash_mview_test_kit(conn_id: u64) -> (UnistoreClusterStack, Session) {
    let stack = super::super::tiflash_partition::tiflash_stack(2);
    // Go master's bootstrap materialized view tables.
    drop(go_master_mview_tables(&stack));
    let mut tk = stack.factory.open_session(super::super::super::node_fixture::session_context(conn_id)).unwrap();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test"]);
    (stack, tk)
}

/// Go `testkit.SetTiFlashReplica`: the table's TiFlash replica, available.
fn set_tiflash_replica(stack: &UnistoreClusterStack, tk: &mut Session, table: &str) {
    must_exec_all(tk, &[&format!("alter table `{table}` set tiflash replica 1")]);
    let table_id = latest_table(stack, table).id;
    stack.factory.ddl.update_table_replica_info(table_id, true, &tidb_sessionctx::StmtContext::for_query()).unwrap();
}

#[test]
fn test_purge_materialized_view_log_tiflash_threads() {
    for (name, internal, count_failure, empty) in [
        ("manual", false, false, false),
        ("internal", true, false, false),
        ("count failure", false, true, false),
        ("empty", false, false, true),
    ] {
        let (stack, mut tk) = mock_tiflash_mview_test_kit(3931);
        must_exec_all(&mut tk, &[
            "create table t_purge_tiflash_threads (id int primary key, v int)",
            "create materialized view log on t_purge_tiflash_threads (id, v) purge next date_add(now(), interval 1 hour)",
        ]);
        set_tiflash_replica(&stack, &mut tk, "$mlog$t_purge_tiflash_threads");
        if !empty {
            must_exec_all(&mut tk, &["insert into t_purge_tiflash_threads values (1, 10), (2, 20), (3, 30)"]);
        }
        must_exec_all(&mut tk, &[
            "set @@session.tidb_max_tiflash_threads = 9",
            "set @@session.tidb_mlog_purge_delete_tiflash_threads = 2",
            "set @@session.tidb_mlog_purge_batch_size = 1",
        ]);
        let applied = Arc::new(std::sync::Mutex::new(Vec::<(i64, i64)>::new()));
        let counted = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));
        let _apply = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvMLogPurgeDeleteTiFlashThreadsAppliedOnPurgeDeleteSession", {
            let applied = applied.clone();
            move |arg: &mut dyn std::any::Any| applied.lock().unwrap().extend(arg.downcast_ref::<(i64, i64)>().copied())
        });
        let _count = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvMLogPurgeCountTiFlashThreadsUsed", {
            let counted = counted.clone();
            move |arg: &mut dyn std::any::Any| counted.lock().unwrap().extend(arg.downcast_ref::<i64>().copied())
        });
        let _count_failure = count_failure
            .then(|| tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockMLogPurgeAdaptiveCountErr", "return(true)"));
        if internal {
            must_exec_mview_purge_internal(&mut tk, "purge materialized view log on t_purge_tiflash_threads");
        } else {
            must_exec_all(&mut tk, &["purge materialized view log on t_purge_tiflash_threads"]);
        }
        let counted = counted.lock().unwrap().clone();
        if count_failure {
            assert!(counted.is_empty(), "{name}: {counted:?}");
        } else {
            assert_eq!(counted, vec![tidb_vardef::defaults::DEF_TIFLASH_MAX_THREADS], "{name}");
        }
        let applied = applied.lock().unwrap().clone();
        if empty {
            assert!(applied.is_empty(), "{name}: {applied:?}");
            assert_eq!(tk.affected_rows(), 0, "{name}");
        } else {
            assert_eq!(applied, vec![(2, 2)], "{name}");
            assert_eq!(tk.affected_rows(), 3, "{name}");
        }
        assert_eq!(single_value(&mut tk, "select count(*) from `$mlog$t_purge_tiflash_threads`"), "0", "{name}");
        assert_eq!(single_value(&mut tk, "select @@session.tidb_max_tiflash_threads"), "9", "{name}");
    }
}

#[test]
fn test_purge_materialized_view_log_uses_row_id_range_delete_sql() {
    let (stack, mut tk) = mock_tiflash_mview_test_kit(3932);
    must_exec_all(&mut tk, &[
        "create table t_purge_rowid_range (id int primary key, v int)",
        "create materialized view log on t_purge_rowid_range (id, v) purge next date_add(now(), interval 1 hour)",
    ]);
    set_tiflash_replica(&stack, &mut tk, "$mlog$t_purge_rowid_range");
    must_exec_all(&mut tk, &[
        "insert into t_purge_rowid_range values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
        "set @@session.tidb_mlog_purge_batch_size = 2",
    ]);
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let _sql = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/purgeMaterializedViewLogDeleteSQL", {
        let seen = seen.clone();
        move |arg: &mut dyn std::any::Any| seen.lock().unwrap().extend(arg.downcast_ref::<String>().cloned())
    });
    must_exec_all(&mut tk, &["purge materialized view log on t_purge_rowid_range"]);
    let seen = seen.lock().unwrap().clone();
    assert!(!seen.is_empty());
    for part in ["_tidb_rowid >=", "_tidb_rowid <=", "_tidb_commit_ts <= ", "LIMIT 2"] {
        assert!(seen[0].contains(part), "{part}: {}", seen[0]);
    }
}
