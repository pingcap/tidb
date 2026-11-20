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

//! Go master `pkg/ddl/tests/materializedview/materialized_view_drop_test.go`.

use super::create::{new_mview_test_kit, persisted_table};
use super::*;

fn count(session: &mut Session, sql: &str) -> String {
    single_value(session, sql)
}

/// Go `meta.NewReader(txn).GetDatabase(dbID)` in a new transaction.
fn persisted_database_exists(session: &mut Session, db_id: i64) -> bool {
    let domain = session.domain_services().unwrap().domain.upgrade().unwrap();
    let ts = domain.transactions.fresh_timestamp().unwrap();
    let mut meta = domain.get_snapshot_meta(ts).unwrap();
    tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| reader.database(db_id)).unwrap().is_some()
}

/// A `beforeRunOneJobStep` callback pausing the first retry (error count
/// above zero) of the job `matches` picks.
fn pause_first_retry(
    matches: impl Fn(&tidb_model::Job) -> bool + Send + Sync + 'static,
) -> (Latch, Latch, tidb_failpoint::Guard) {
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
fn test_drop_or_truncate_table_recheck_materialized_view_constraints() {
    for (ddl, op, after_check) in [
        (
            "drop table t_drop_or_truncate_mlog_recheck",
            "DROP TABLE",
            "github.com/pingcap/tidb/pkg/ddl/afterCheckDropTableMaterializedViewConstraints",
        ),
        (
            "truncate table t_drop_or_truncate_mlog_recheck",
            "TRUNCATE TABLE",
            "github.com/pingcap/tidb/pkg/ddl/afterCheckTruncateTableMaterializedViewConstraints",
        ),
    ] {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test", "create table t_drop_or_truncate_mlog_recheck (a int)"]);
        let base_id = latest_table(&stack, "t_drop_or_truncate_mlog_recheck").id;

        let (create_started, allow_create) = (Latch::default(), Latch::default());
        let _create_pause = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", {
            let (started, allow) = (create_started.clone(), allow_create.clone());
            move |job| {
                let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                if job.type_ != tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG
                    || job.table_name.to_string() != "$mlog$t_drop_or_truncate_mlog_recheck"
                {
                    return;
                }
                started.open();
                allow.wait();
            }
        });
        // Go's `t.Cleanup(allowCreate)`.
        struct Allow(Latch);
        impl Drop for Allow {
            fn drop(&mut self) {
                self.0.open();
            }
        }
        let allow_create = Allow(allow_create);
        let mut tk_create = new_mview_test_kit(&stack, 2701);
        let create = std::thread::spawn(move || answer(&mut tk_create, "create materialized view log on t_drop_or_truncate_mlog_recheck (a)"));
        create_started.wait_for("CREATE MATERIALIZED VIEW LOG worker");

        let entry_check_done = Latch::default();
        let _after_check = tidb_failpoint::enable_call_for_test(after_check, {
            let done = entry_check_done.clone();
            move |table_id| {
                if *table_id.downcast_mut::<i64>().unwrap() == base_id {
                    done.open();
                }
            }
        });
        let mut tk_ddl = new_mview_test_kit(&stack, 2702);
        let ddl = std::thread::spawn(move || answer(&mut tk_ddl, ddl));
        entry_check_done.wait_for("DDL materialized view constraint precheck");

        drop(allow_create);
        assert_eq!(create.join().unwrap(), "OK");
        let error = ddl.join().unwrap();
        assert!(error.contains(&format!("{op} on base table with materialized view log")), "{op}: {error}");

        let base = latest_table(&stack, "t_drop_or_truncate_mlog_recheck");
        assert_eq!(base.id, base_id);
        assert!(base.materialized_view_base.is_some());
        let mlog = stack
            .factory
            .catalog
            .get_latest()
            .table_info_by_id(mlog_id(&base))
            .expect("the log")
            .read()
            .clone();
        assert_eq!(mlog.materialized_view_log.as_ref().expect("a log").read().base_table_id, base_id);
    }
}

#[test]
fn test_drop_materialized_view_refresh_info_failure_rolls_back_metadata() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_drop_mv_atomic (a int)",
        "create materialized view log on t_drop_mv_atomic (a)",
        "create materialized view mv_drop_atomic (a, cnt) as select a, count(1) from t_drop_mv_atomic group by a",
    ]);
    let mv_id = latest_table(&stack, "mv_drop_atomic").id;
    let cleanup = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoErr",
        r#"1*return("mock refresh info delete error")"#,
    );
    let (retry_started, allow_retry, _pause) = pause_first_retry(move |job| {
        job.type_ == tidb_model::ActionType::ACTION_DROP_MATERIALIZED_VIEW
            && job.table_id == mv_id
            && job.schema_state == tidb_model::SchemaState::DELETE_ONLY
            && job.error_count > 0
    });
    let mut tk_inspect = new_mview_test_kit(&stack, 2703);
    let drop_mv = std::thread::spawn(move || answer(&mut tk, "drop materialized view mv_drop_atomic"));
    retry_started.wait_for("DROP MATERIALIZED VIEW retry");
    assert_eq!(answer(&mut tk_inspect, "show tables like 'mv_drop_atomic'"), r#"RS:[["mv_drop_atomic"]]"#);
    assert_eq!(count(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {mv_id}")), "1");
    assert_eq!(
        count(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mv_id}")),
        "1"
    );
    drop(cleanup);
    allow_retry.open();
    assert_eq!(drop_mv.join().unwrap(), "OK");
    assert_eq!(
        count(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mv_id}")),
        "0"
    );
}

#[test]
fn test_drop_materialized_view_notify_failure_rolls_back_metadata() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_drop_mv_notify_atomic (a int)",
        "create materialized view log on t_drop_mv_notify_atomic (a)",
        "create materialized view mv_drop_notify_atomic (a, cnt) as select a, count(1) from t_drop_mv_notify_atomic group by a",
    ]);
    let mv_id = latest_table(&stack, "mv_drop_notify_atomic").id;
    let _notify = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/asyncNotifyEventError", "1*return()");
    let (retry_started, allow_retry, _pause) = pause_first_retry(move |job| {
        job.type_ == tidb_model::ActionType::ACTION_DROP_MATERIALIZED_VIEW && job.table_id == mv_id && job.error_count > 0
    });
    let mut tk_inspect = new_mview_test_kit(&stack, 2704);
    let drop_mv = std::thread::spawn(move || answer(&mut tk, "drop materialized view mv_drop_notify_atomic"));
    retry_started.wait_for("DROP MATERIALIZED VIEW retry");
    assert_eq!(answer(&mut tk_inspect, "show tables like 'mv_drop_notify_atomic'"), r#"RS:[["mv_drop_notify_atomic"]]"#);
    assert_eq!(count(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {mv_id}")), "1");
    assert_eq!(
        count(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mv_id}")),
        "1"
    );
    allow_retry.open();
    assert_eq!(drop_mv.join().unwrap(), "OK");
    assert_eq!(
        count(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mv_id}")),
        "0"
    );
}

/// The `drop database` fixture: `t`, its log and `mv` in `db_name`; the
/// database, view and log IDs.
fn drop_database_fixture(stack: &UnistoreClusterStack, tk: &mut Session, db_name: &str) -> (i64, i64, i64) {
    must_exec_all(tk, &[
        "set tidb_mview_enable = on",
        &format!("create database {db_name}"),
        &format!("use {db_name}"),
        "create table t (a int)",
        "create materialized view log on t (a)",
        "create materialized view mv (a, cnt) as select a, count(1) from t group by a",
    ]);
    let latest = stack.factory.catalog.get_latest();
    let db_id = latest.schema_by_name(db_name).unwrap().read().id;
    let mv_id = latest.table_info_by_name(db_name, "mv").unwrap().1.read().id;
    let mlog_id = latest.table_info_by_name(db_name, "$mlog$t").unwrap().1.read().id;
    (db_id, mv_id, mlog_id)
}

fn assert_mview_rows(session: &mut Session, mv_id: i64, mlog_id: i64, want: &str) {
    assert_eq!(count(session, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {mv_id}")), want);
    assert_eq!(count(session, &format!("select count(*) from mysql.tidb_mview_fast_refresh_source_info where mview_id = {mv_id}")), want);
    assert_eq!(count(session, &format!("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = {mlog_id}")), want);
}

#[test]
fn test_drop_database_mview_info_failure_rolls_back_metadata() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    let (db_id, mv_id, mlog_id) = drop_database_fixture(&stack, &mut tk, "mv_drop_db_atomic");
    let cleanup = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoErr",
        r#"1*return("mock refresh info delete error")"#,
    );
    let (retry_started, allow_retry, _pause) = pause_first_retry(|job| {
        job.type_ == tidb_model::ActionType::ACTION_DROP_SCHEMA
            && job.schema_state == tidb_model::SchemaState::DELETE_ONLY
            && job.error_count > 0
    });
    let mut tk_inspect = new_mview_test_kit(&stack, 2705);
    let drop_db = std::thread::spawn(move || answer(&mut tk, "drop database mv_drop_db_atomic"));
    retry_started.wait_for("DROP DATABASE retry");
    assert_mview_rows(&mut tk_inspect, mv_id, mlog_id, "1");
    assert!(persisted_database_exists(&mut tk_inspect, db_id));
    drop(cleanup);
    allow_retry.open();
    assert_eq!(drop_db.join().unwrap(), "OK");
    assert_mview_rows(&mut tk_inspect, mv_id, mlog_id, "0");
    assert!(!persisted_database_exists(&mut tk_inspect, db_id));
}

#[test]
fn test_drop_database_notify_failure_rolls_back_mview_metadata() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    let (db_id, mv_id, mlog_id) = drop_database_fixture(&stack, &mut tk, "mv_drop_db_notify_atomic");
    let _notify = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/asyncNotifyEventError", "1*return()");
    let (retry_started, allow_retry, _pause) = pause_first_retry(move |job| {
        job.type_ == tidb_model::ActionType::ACTION_DROP_SCHEMA && job.schema_id == db_id && job.error_count > 0
    });
    let mut tk_inspect = new_mview_test_kit(&stack, 2706);
    let drop_db = std::thread::spawn(move || answer(&mut tk, "drop database mv_drop_db_notify_atomic"));
    retry_started.wait_for("DROP DATABASE retry");
    assert_mview_rows(&mut tk_inspect, mv_id, mlog_id, "1");
    assert!(persisted_database_exists(&mut tk_inspect, db_id));
    allow_retry.open();
    assert_eq!(drop_db.join().unwrap(), "OK");
}

fn insert_refresh_alert(session: &mut Session, mv_id: i64, schema: &str, name: &str, level: &str) {
    must_exec_all(session, &[&format!(
        "insert into mysql.tidb_mview_refresh_alert (MVIEW_ID, MVIEW_SCHEMA, MVIEW_NAME, ALERT_LEVEL, LAST_SUCCESS_SNAPSHOT_TIME, UPDATE_TIME) values ({mv_id}, '{schema}', '{name}', '{level}', UTC_TIMESTAMP(), UTC_TIMESTAMP())"
    )]);
}

#[test]
fn test_drop_materialized_view_and_database_clean_mview_state() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    const DB: &str = "mv_drop_cleanup";
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "drop database if exists mv_drop_cleanup",
        "create database mv_drop_cleanup",
        "use mv_drop_cleanup",
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (2, 20)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    let ids = |stack: &UnistoreClusterStack| {
        let latest = stack.factory.catalog.get_latest();
        (latest.table_info_by_name(DB, "mv").unwrap().1.read().id, latest.table_info_by_name(DB, "$mlog$t").unwrap().1.read().id)
    };
    let (mv_id, mlog_id) = ids(&stack);
    insert_refresh_alert(&mut tk, mv_id, DB, "mv", "warning");
    assert_mview_rows(&mut tk, mv_id, mlog_id, "1");
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = {mv_id}")), "1");

    must_exec_all(&mut tk, &["drop materialized view mv"]);
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {mv_id}")), "0");
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_fast_refresh_source_info where mview_id = {mv_id}")), "0");
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = {mv_id}")), "0");
    must_exec_all(&mut tk, &["drop materialized view log on t"]);
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mlog_purge_info where mlog_id = {mlog_id}")), "0");

    must_exec_all(&mut tk, &[
        "create materialized view log on t (a, b)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    let (mv_id, mlog_id) = ids(&stack);
    insert_refresh_alert(&mut tk, mv_id, DB, "mv", "warning");
    assert_mview_rows(&mut tk, mv_id, mlog_id, "1");
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = {mv_id}")), "1");
    must_exec_all(&mut tk, &["drop database mv_drop_cleanup"]);
    assert_mview_rows(&mut tk, mv_id, mlog_id, "0");
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = {mv_id}")), "0");
    assert!(stack.factory.catalog.get_latest().schema_by_name(DB).is_none());
}

#[test]
fn test_drop_materialized_view_cleans_refresh_alert() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_drop_alert_cleanup (a int not null, b int not null)",
        "insert into t_drop_alert_cleanup values (1, 10), (1, 5), (2, 7)",
        "create materialized view log on t_drop_alert_cleanup (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_drop_alert_cleanup (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_drop_alert_cleanup group by a",
    ]);
    let mv_id = latest_table(&stack, "mv_drop_alert_cleanup").id;
    insert_refresh_alert(&mut tk, mv_id, "test", "mv_drop_alert_cleanup", "warning");
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = {mv_id}")), "1");
    must_exec_all(&mut tk, &["drop materialized view mv_drop_alert_cleanup"]);
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {mv_id}")), "0");
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = {mv_id}")), "0");
    must_exec_all(&mut tk, &["drop materialized view log on t_drop_alert_cleanup"]);
}

#[test]
fn test_drop_database_ignores_refresh_alert_delete_failure() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    const DB: &str = "mv_drop_db_alert_delete_fail";
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "drop database if exists mv_drop_db_alert_delete_fail",
        "create database mv_drop_db_alert_delete_fail",
        "use mv_drop_db_alert_delete_fail",
        "create table t (a int not null, b int not null)",
        "insert into t values (1, 10), (2, 20)",
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a",
    ]);
    let mv_id = stack.factory.catalog.get_latest().table_info_by_name(DB, "mv").unwrap().1.read().id;
    insert_refresh_alert(&mut tk, mv_id, DB, "mv", "overdue");
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = {mv_id}")), "1");
    let _alert = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshAlertErr",
        r#"return("mock drop schema alert delete error")"#,
    );
    must_exec_all(&mut tk, &["drop database mv_drop_db_alert_delete_fail"]);
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {mv_id}")), "0");
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = {mv_id}")), "1");
}

#[test]
fn test_drop_materialized_view_ignores_refresh_alert_delete_failure() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_drop_alert (a int not null, b int not null)",
        "create materialized view log on t_drop_alert (a, b) purge next date_add(now(), interval 1 hour)",
        "create materialized view mv_drop_alert (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_drop_alert group by a",
    ]);
    let mv_id = latest_table(&stack, "mv_drop_alert").id;
    insert_refresh_alert(&mut tk, mv_id, "test", "mv_drop_alert", "warning");
    let _alert = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshAlertErr",
        r#"return("mock drop table alert delete error")"#,
    );
    must_exec_all(&mut tk, &["drop materialized view mv_drop_alert"]);
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {mv_id}")), "0");
    assert_eq!(count(&mut tk, &format!("select count(*) from mysql.tidb_mview_refresh_alert where mview_id = {mv_id}")), "1");
    must_exec_all(&mut tk, &["drop materialized view log on t_drop_alert"]);
}

#[test]
fn test_drop_materialized_view_privilege() {
    let (stack, users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_drop_mv_priv (a int)",
        "create materialized view log on t_drop_mv_priv (a)",
        "create materialized view mv_drop_priv (a, cnt) as select a, count(1) from t_drop_mv_priv group by a",
        // The node logs in by password; Go's testkit `Auth` takes none.
        "create user 'u_drop_mv_select'@'%' identified by 'abc'",
        "create user 'u_drop_mv_ok'@'%' identified by 'abc'",
        "grant select on test.mv_drop_priv to 'u_drop_mv_select'@'%'",
        "grant drop on test.mv_drop_priv to 'u_drop_mv_ok'@'%'",
    ]);
    let mut tk_select = stack
        .factory
        .open_session(super::super::super::node_fixture::session_context_with_identity(2707, log_in(&users, "u_drop_mv_select")))
        .unwrap();
    must_exec_all(&mut tk_select, &["set tidb_mview_enable = on"]);
    exec_err_contains(&mut tk_select, "drop materialized view test.mv_drop_priv", "DROP command denied");
    let mut tk_drop = stack
        .factory
        .open_session(super::super::super::node_fixture::session_context_with_identity(2708, log_in(&users, "u_drop_mv_ok")))
        .unwrap();
    must_exec_all(&mut tk_drop, &["set tidb_mview_enable = on", "drop materialized view test.mv_drop_priv"]);
    must_exec_all(&mut tk, &[
        "drop materialized view log on test.t_drop_mv_priv",
        "drop table test.t_drop_mv_priv",
        "drop user 'u_drop_mv_select'@'%'",
        "drop user 'u_drop_mv_ok'@'%'",
    ]);
}
