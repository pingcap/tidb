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

//! Go master `pkg/ddl/tests/materializedviewlog/materialized_view_drop_test.go`.

use super::create::new_mview_test_kit;
use super::*;

fn mview_test_kit() -> (UnistoreClusterStack, Arc<ConfiguredUserStore>, Session) {
    let (stack, users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test"]);
    (stack, users, tk)
}

/// A `beforeRunOneJobStep` callback pausing the first retry of the job
/// `matches` picks.
fn pause_retry(matches: impl Fn(&tidb_model::Job) -> bool + Send + Sync + 'static) -> (Latch, Latch, tidb_failpoint::Guard) {
    let (started, allow) = (Latch::default(), Latch::default());
    let guard = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", {
        let (started, allow) = (started.clone(), allow.clone());
        move |job| {
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            if matches(job) {
                started.open();
                allow.wait();
            }
        }
    });
    (started, allow, guard)
}

#[test]
fn test_drop_materialized_view_log_recheck_with_concurrent_create_materialized_view() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_drop_recheck (a int not null, b int not null)",
        "insert into t_drop_recheck values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t_drop_recheck (a, b) purge next date_add(now(), interval 1 hour)",
    ]);
    let check_done = Latch::default();
    let _after_check = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterCheckDropMaterializedViewLog", {
        let done = check_done.clone();
        move |_| done.open()
    });
    let pause = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/pauseDropMaterializedViewLogAfterCheck", "pause");
    let mut tk_drop = new_mview_test_kit(&stack, 3101);
    let drop_log = std::thread::spawn(move || answer(&mut tk_drop, "drop materialized view log on t_drop_recheck"));
    check_done.wait_for("DROP MATERIALIZED VIEW LOG precheck");
    must_exec_all(&mut tk, &[
        "create materialized view mv_drop_dep (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_drop_recheck group by a",
    ]);
    drop(pause);
    let error = drop_log.join().unwrap();
    assert!(error.contains("dependent materialized views exist"), "{error}");
    assert_eq!(answer(&mut tk, "show tables like '$mlog$t_drop_recheck'"), r#"RS:[["$mlog$t_drop_recheck"]]"#);
    must_exec_all(&mut tk, &["drop materialized view mv_drop_dep"]);
    assert!(dependent_mview_ids(&latest_table(&stack, "$mlog$t_drop_recheck")).is_empty());
    must_exec_all(&mut tk, &["drop materialized view log on t_drop_recheck"]);
    let base = latest_table(&stack, "t_drop_recheck");
    assert!(base.materialized_view_base.is_none() || (mlog_id(&base) == 0 && mview_ids(&base).is_empty()));
}

#[test]
fn test_drop_materialized_view_log_purge_info_failure_rolls_back_metadata() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_drop_mlog_atomic (a int)", "create materialized view log on t_drop_mlog_atomic (a)"]);
    let mlog_id = latest_table(&stack, "$mlog$t_drop_mlog_atomic").id;
    let cleanup = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDeleteMaterializedViewLogPurgeInfoErr",
        r#"1*return("mock purge info delete error")"#,
    );
    let (retry_started, allow_retry, _pause) = pause_retry(move |job| {
        job.type_ == tidb_model::ActionType::ACTION_DROP_MATERIALIZED_VIEW_LOG
            && job.table_id == mlog_id
            && job.schema_state == tidb_model::SchemaState::DELETE_ONLY
            && job.error_count > 0
    });
    let mut tk_inspect = new_mview_test_kit(&stack, 3102);
    let drop_log = std::thread::spawn(move || answer(&mut tk, "drop materialized view log on t_drop_mlog_atomic"));
    retry_started.wait_for("DROP MATERIALIZED VIEW LOG retry");
    assert_eq!(answer(&mut tk_inspect, "show tables like '$mlog$t_drop_mlog_atomic'"), r#"RS:[["$mlog$t_drop_mlog_atomic"]]"#);
    assert_eq!(single_value(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = {mlog_id}")), "1");
    drop(cleanup);
    allow_retry.open();
    assert_eq!(drop_log.join().unwrap(), "OK");
}

#[test]
fn test_drop_materialized_view_log_notify_failure_rolls_back_metadata() {
    let (stack, _users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &["create table t_drop_mlog_notify_atomic (a int)", "create materialized view log on t_drop_mlog_notify_atomic (a)"]);
    let mlog_id = latest_table(&stack, "$mlog$t_drop_mlog_notify_atomic").id;
    let _notify = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/asyncNotifyEventError", "1*return()");
    let (retry_started, allow_retry, _pause) = pause_retry(move |job| {
        job.type_ == tidb_model::ActionType::ACTION_DROP_MATERIALIZED_VIEW_LOG && job.table_id == mlog_id && job.error_count > 0
    });
    let mut tk_inspect = new_mview_test_kit(&stack, 3103);
    let drop_log = std::thread::spawn(move || answer(&mut tk, "drop materialized view log on t_drop_mlog_notify_atomic"));
    retry_started.wait_for("DROP MATERIALIZED VIEW LOG retry");
    assert_eq!(
        answer(&mut tk_inspect, "show tables like '$mlog$t_drop_mlog_notify_atomic'"),
        r#"RS:[["$mlog$t_drop_mlog_notify_atomic"]]"#
    );
    assert_eq!(single_value(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = {mlog_id}")), "1");
    allow_retry.open();
    assert_eq!(drop_log.join().unwrap(), "OK");
}

#[test]
fn test_drop_materialized_view_log_privilege() {
    let (stack, users, mut tk) = mview_test_kit();
    must_exec_all(&mut tk, &[
        "create table t_drop_mlog_priv (a int)",
        "create materialized view log on t_drop_mlog_priv (a)",
        // The node logs in by password; Go's testkit `Auth` takes none.
        "create user 'u_drop_mlog_select'@'%' identified by 'abc'",
        "create user 'u_drop_mlog_ok'@'%' identified by 'abc'",
        "grant select on test.t_drop_mlog_priv to 'u_drop_mlog_select'@'%'",
        "grant drop on test.`$mlog$t_drop_mlog_priv` to 'u_drop_mlog_ok'@'%'",
    ]);
    let login = |connection_id: u64, user: &str| {
        let mut session = stack
            .factory
            .open_session(super::super::super::node_fixture::session_context_with_identity(connection_id, log_in(&users, user)))
            .unwrap();
        must_exec_all(&mut session, &["set tidb_mview_enable = on"]);
        session
    };
    let error = answer(&mut login(3104, "u_drop_mlog_select"), "drop materialized view log on test.t_drop_mlog_priv");
    assert!(error.contains("DROP MATERIALIZED VIEW LOG command denied"), "{error}");
    assert!(error.contains("for table 't_drop_mlog_priv'"), "{error}");
    must_exec_all(&mut login(3105, "u_drop_mlog_ok"), &["drop materialized view log on test.t_drop_mlog_priv"]);
    must_exec_all(&mut tk, &["drop user 'u_drop_mlog_select'@'%'", "drop user 'u_drop_mlog_ok'@'%'"]);
}
