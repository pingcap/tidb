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

//! Go master `pkg/ddl/tests/materializedview/materialized_view_create_test.go`.

use super::*;

/// Go `newMViewTestKit` on a fresh store, `use test`.
fn new_mview_test_kit(stack: &UnistoreClusterStack, connection_id: u64) -> Session {
    let mut tk = stack.factory.open_session(session_context(connection_id)).unwrap();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test"]);
    tk
}

/// The common fixture: `t (a, b)` with three rows and a log on both columns.
fn create_fixture(table_ddl: &str, rows: &str) -> (UnistoreClusterStack, Session) {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        table_ddl,
        rows,
        "create materialized view log on t (a, b) purge next date_add(now(), interval 1 hour)",
    ]);
    (stack, tk)
}

/// Go: the base `t` keeps its log and no view depends on it.
fn assert_base_without_mviews(stack: &UnistoreClusterStack, base: &str) {
    let base = latest_table(stack, base);
    assert!(base.materialized_view_base.is_some());
    assert_ne!(mlog_id(&base), 0);
    assert!(mview_ids(&base).is_empty());
}

/// Go `kv.RunInNewTxn` + `meta.NewMutator(txn).GetTable(dbID, tableID)`.
fn persisted_table(session: &mut Session, db_id: i64, table_id: i64) -> Option<tidb_model::TableInfo> {
    let domain = session.domain_services().unwrap().domain.upgrade().unwrap();
    let ts = domain.transactions.fresh_timestamp().unwrap();
    let mut meta = domain.get_snapshot_meta(ts).unwrap();
    tidb_infoschema::cluster_catalog::with_meta_reader(&mut meta, |reader| reader.table(db_id, table_id)).unwrap()
}

fn test_schema_id(stack: &UnistoreClusterStack) -> i64 {
    stack.factory.catalog.get_latest().schema_by_name("test").unwrap().read().id
}

/// `admin show ddl jobs where ...` as rows.
fn ddl_job_rows(session: &mut Session, sql: &str) -> Vec<Vec<Option<String>>> {
    let rows = answer(session, sql);
    serde_json::from_str(rows.strip_prefix("RS:").unwrap_or_else(|| panic!("{sql}: {rows}"))).unwrap()
}

fn is_create_mview(job: &tidb_model::Job) -> bool {
    job.type_ == tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW
}

/// Go's `beforeRunOneJobStep` callback pausing the second attempt of a
/// rolling-back CREATE MATERIALIZED VIEW: returns (retry started, allow
/// retry, the view ID, the guard).
fn pause_second_rollback_attempt() -> (Latch, Latch, Arc<std::sync::atomic::AtomicI64>, tidb_failpoint::Guard) {
    let (started, allow) = (Latch::default(), Latch::default());
    let mview_id = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let attempts = std::sync::atomic::AtomicI64::new(0);
    let guard = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", {
        let (started, allow, mview_id) = (started.clone(), allow.clone(), Arc::clone(&mview_id));
        move |job| {
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            if !is_create_mview(job) || !job.is_rollingback() {
                return;
            }
            if attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 != 2 {
                return;
            }
            mview_id.store(job.table_id, std::sync::atomic::Ordering::SeqCst);
            started.open();
            allow.wait();
        }
    });
    (started, allow, mview_id, guard)
}

/// Go `beforeUpdateSchemaVersion` failing once for a rolled-back CREATE
/// MATERIALIZED VIEW.
fn fail_rollback_schema_version_once() -> tidb_failpoint::Guard {
    let once = std::sync::Once::new();
    tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeUpdateSchemaVersion", move |call| {
        let (job, error) = call.downcast_mut::<(tidb_model::Job, Option<String>)>().unwrap();
        if is_create_mview(job) && job.is_rollback_done() {
            once.call_once(|| *error = Some("mock materialized view rollback schema version error".to_owned()));
        }
    })
}

const CREATE_MV_FAST: &str = "refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t group by a";

#[test]
fn test_create_materialized_view_build_failure_rollback() {
    let (stack, mut tk) = create_fixture("create table t (a int not null, b int not null)", "insert into t values (1, 10), (1, 5), (2, 7)");
    let _build = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return");
    exec_err_contains(&mut tk, &format!("create materialized view mv_fail (a, s, cnt) {CREATE_MV_FAST}"), "");
    assert_eq!(
        answer(&mut tk, "select count(*) from information_schema.tables where table_schema = 'test' and table_name = 'mv_fail'"),
        r#"RS:[["0"]]"#
    );
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_refresh_info"), r#"RS:[["0"]]"#);
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_fast_refresh_source_info"), r#"RS:[["0"]]"#);
    assert_base_without_mviews(&stack, "t");
}

#[test]
fn test_create_materialized_view_build_context_canceled_rollback() {
    let (stack, mut tk) = create_fixture("create table t (a int not null, b int not null)", "insert into t values (1, 10), (1, 5), (2, 7)");
    let _build = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr",
        r#"return("context-canceled")"#,
    );
    exec_err_contains(&mut tk, &format!("create materialized view mv_ctx_cancel (a, s, cnt) {CREATE_MV_FAST}"), "");
    assert_eq!(answer(&mut tk, "show tables like 'mv_ctx_cancel'"), "RS:[]");
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_refresh_info"), r#"RS:[["0"]]"#);
    assert_base_without_mviews(&stack, "t");
}

#[test]
fn test_create_materialized_view_rollback_ignore_missing_refresh_info_table() {
    let (stack, mut tk) = create_fixture("create table t (a int not null, b int not null)", "insert into t values (1, 10), (1, 5), (2, 7)");
    let _build = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return");
    let _missing = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoTableNotExists",
        "return(true)",
    );
    exec_err_contains(&mut tk, &format!("create materialized view mv_missing_refresh_meta (a, s, cnt) {CREATE_MV_FAST}"), "");
    assert_eq!(answer(&mut tk, "show tables like 'mv_missing_refresh_meta'"), "RS:[]");
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_refresh_info"), r#"RS:[["0"]]"#);
    assert_base_without_mviews(&stack, "t");
}

#[test]
fn test_create_materialized_view_rollback_refresh_info_failure_rolls_back_metadata() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_create_mv_rollback_atomic (a int not null, b int not null)",
        "create materialized view log on t_create_mv_rollback_atomic (a, b) purge next date_add(now(), interval 1 hour)",
    ]);
    let db_id = test_schema_id(&stack);
    let base_id = latest_table(&stack, "t_create_mv_rollback_atomic").id;
    let _build = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return");
    let cleanup = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshInfoErr",
        r#"1*return("mock refresh info delete error")"#,
    );
    let (retry_started, allow_retry, mview_id, _pause) = pause_second_rollback_attempt();
    let mut tk_inspect = new_mview_test_kit(&stack, 2601);
    let ddl = std::thread::spawn(move || {
        let answer = answer(
            &mut tk,
            "create materialized view mv_create_rollback_atomic (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_create_mv_rollback_atomic group by a",
        );
        (tk, answer)
    });
    retry_started.wait_for("CREATE MATERIALIZED VIEW rollback retry");
    let mview_id = mview_id.load(std::sync::atomic::Ordering::SeqCst);
    assert_ne!(mview_id, 0);
    assert_eq!(
        answer(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {mview_id}")),
        r#"RS:[["1"]]"#
    );
    assert_eq!(
        answer(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mview_id}")),
        r#"RS:[["0"]]"#
    );
    assert!(persisted_table(&mut tk_inspect, db_id, mview_id).is_some());
    let base = persisted_table(&mut tk_inspect, db_id, base_id).unwrap();
    assert!(base.materialized_view_base.is_some());
    assert!(mview_ids(&base).contains(&mview_id));

    drop(cleanup);
    allow_retry.open();
    let (_tk, ddl_answer) = ddl.join().unwrap();
    assert!(ddl_answer.starts_with("ERR"), "{ddl_answer}");
    assert_eq!(
        answer(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mview_id}")),
        r#"RS:[["0"]]"#
    );
}

#[test]
fn test_create_materialized_view_rollback_update_schema_version_failure_retries() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_create_mv_rollback_retry (a int not null, b int not null)",
        "create materialized view log on t_create_mv_rollback_retry (a, b) purge next date_add(now(), interval 1 hour)",
    ]);
    let db_id = test_schema_id(&stack);
    let base_id = latest_table(&stack, "t_create_mv_rollback_retry").id;
    let mlog_table_id = latest_table(&stack, "$mlog$t_create_mv_rollback_retry").id;
    let _build = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return");
    let _version = fail_rollback_schema_version_once();
    let (retry_started, allow_retry, mview_id, _pause) = pause_second_rollback_attempt();
    let mut tk_inspect = new_mview_test_kit(&stack, 2602);
    let ddl = std::thread::spawn(move || {
        let answer = answer(
            &mut tk,
            "create materialized view mv_create_rollback_retry (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_create_mv_rollback_retry group by a",
        );
        (tk, answer)
    });
    retry_started.wait_for("CREATE MATERIALIZED VIEW rollback retry");
    let mview_id = mview_id.load(std::sync::atomic::Ordering::SeqCst);
    assert_ne!(mview_id, 0);
    assert_eq!(
        answer(&mut tk_inspect, &format!("select count(*) from mysql.tidb_mview_refresh_info where mview_id = {mview_id}")),
        r#"RS:[["1"]]"#
    );
    assert!(persisted_table(&mut tk_inspect, db_id, mview_id).is_some());
    let base = persisted_table(&mut tk_inspect, db_id, base_id).unwrap();
    assert!(mview_ids(&base).contains(&mview_id));
    let mlog = persisted_table(&mut tk_inspect, db_id, mlog_table_id).unwrap();
    assert!(mlog.materialized_view_log.is_some());
    assert!(dependent_mview_ids(&mlog).contains(&mview_id));

    allow_retry.open();
    let (mut tk, ddl_answer) = ddl.join().unwrap();
    assert!(ddl_answer.starts_with("ERR"), "{ddl_answer}");
    assert_eq!(answer(&mut tk, "show tables like 'mv_create_rollback_retry'"), "RS:[]");
    must_exec_all(&mut tk, &["drop materialized view log on t_create_mv_rollback_retry"]);
}

#[test]
fn test_create_materialized_view_rollback_rechecks_job_meta_after_reopen_txn() {
    let (stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &[
        "set tidb_mview_enable = on",
        "use test",
        "create table t_create_mv_recheck_job_meta (a int not null, b int not null)",
        "create materialized view log on t_create_mv_recheck_job_meta (a, b) purge next date_add(now(), interval 1 hour)",
    ]);
    let _build = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewBuildErr", "return");
    let _version = fail_rollback_schema_version_once();

    let tk_concurrent = Arc::new(std::sync::Mutex::new(new_mview_test_kit(&stack, 2603)));
    let expected_error_count = Arc::new(std::sync::atomic::AtomicI64::new(-1));
    let concurrent_update_done = Latch::default();
    let update_once = std::sync::Once::new();
    let _after_rollback = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/afterRollbackTxnForMustRollbackTxnOnError",
        {
            let (expected, done) = (Arc::clone(&expected_error_count), concurrent_update_done.clone());
            move |job| {
                let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                if !is_create_mview(job) || !job.is_rollingback() {
                    return;
                }
                update_once.call_once(|| {
                    let bytes = job.clone().encode(false).unwrap();
                    let mut concurrent = tidb_model::Job::default();
                    concurrent.decode(&bytes).unwrap();
                    concurrent.error_count += 1;
                    expected.store(concurrent.error_count, std::sync::atomic::Ordering::SeqCst);
                    let updated = concurrent.encode(false).unwrap();
                    let hex: String = updated.iter().map(|byte| format!("{byte:02X}")).collect();
                    let mut tk = tk_concurrent.lock().unwrap();
                    assert_eq!(
                        answer(&mut tk, &format!("update mysql.tidb_ddl_job set job_meta = 0x{hex} where job_id = {}", job.id)),
                        "OK"
                    );
                    done.open();
                });
            }
        },
    );

    let (retry_started, allow_retry) = (Latch::default(), Latch::default());
    let retry_once = std::sync::Once::new();
    let _before_run = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", {
        let (expected, done, started, allow) =
            (Arc::clone(&expected_error_count), concurrent_update_done.clone(), retry_started.clone(), allow_retry.clone());
        move |job| {
            let job = job.downcast_mut::<tidb_model::Job>().unwrap();
            if !is_create_mview(job) || !job.is_rollingback() {
                return;
            }
            if !*done.0 .0.lock().unwrap() {
                return;
            }
            if job.error_count != expected.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            retry_once.call_once(|| {
                started.open();
                allow.wait();
            });
        }
    });

    let ddl = std::thread::spawn(move || {
        answer(
            &mut tk,
            "create materialized view mv_create_recheck_job_meta (a, s, cnt) refresh fast next date_add(now(), interval 1 hour) as select a, sum(b), count(1) from t_create_mv_recheck_job_meta group by a",
        )
    });
    retry_started.wait_for("CREATE MATERIALIZED VIEW retry with concurrent job metadata");
    allow_retry.open();
    let ddl_answer = ddl.join().unwrap();
    assert!(ddl_answer.starts_with("ERR"), "{ddl_answer}");
}

#[test]
fn test_create_materialized_view_refresh_info_upsert_failure_rollback() {
    let (stack, mut tk) = create_fixture("create table t (a int not null, b int not null)", "insert into t values (1, 10), (1, 5), (2, 7)");
    let _upsert = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists",
        "1*return(true)",
    );
    let error = answer(&mut tk, &format!("create materialized view mv_upsert_fail (a, s, cnt) {CREATE_MV_FAST}"));
    assert!(error.starts_with("ERR") && error.contains("tidb_mview_refresh_info"), "{error}");
    assert!(!error.contains("Information schema is changed"), "{error}");
    assert!(!error.contains("Duplicate entry"), "{error}");
    assert_eq!(answer(&mut tk, "show tables like 'mv_upsert_fail'"), "RS:[]");
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_refresh_info"), r#"RS:[["0"]]"#);
    let rows = ddl_job_rows(&mut tk, "admin show ddl jobs where JOB_TYPE='create materialized view'");
    let job_id = rows[0][0].clone().unwrap();
    assert_eq!(
        answer(
            &mut tk,
            &format!(
                "select ((select count(*) from mysql.gc_delete_range where job_id={job_id}) + (select count(*) from mysql.gc_delete_range_done where job_id={job_id})) > 0"
            )
        ),
        r#"RS:[["1"]]"#
    );
    assert_base_without_mviews(&stack, "t");
}

#[test]
fn test_create_materialized_view_retry_with_residual_build_rows_rollback() {
    let (stack, mut tk) =
        create_fixture("create table t (a int, b int not null)", "insert into t values (1, 10), (1, 5), (null, 7), (null, 3)");
    let _retry = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildRetryableErr",
        "1*return(true)",
    );
    let error = answer(&mut tk, &format!("create materialized view mv_retry_residual (a, s, cnt) {CREATE_MV_FAST}"));
    assert!(error.starts_with("ERR") && error.contains("detected residual build rows on retry"), "{error}");
    assert!(!error.contains("Duplicate entry"), "{error}");
    assert_eq!(answer(&mut tk, "show tables like 'mv_retry_residual'"), "RS:[]");
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_refresh_info"), r#"RS:[["0"]]"#);
    assert_base_without_mviews(&stack, "t");
}

#[test]
fn test_create_materialized_view_retry_after_upsert_failure() {
    let (_stack, mut tk) = create_fixture("create table t (a int not null, b int not null)", "insert into t values (1, 10), (1, 5), (2, 7)");
    let _upsert = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockUpsertCreateMaterializedViewRefreshInfoTableNotExists",
        "1*return(true)",
    );
    let _alert = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockDeleteCreateMaterializedViewRefreshAlertErr",
        r#"return("mock rollback alert delete error")"#,
    );
    let error = answer(&mut tk, &format!("create materialized view mv_retry (a, s, cnt) {CREATE_MV_FAST}"));
    assert!(error.starts_with("ERR") && error.contains("tidb_mview_refresh_info"), "{error}");
    assert!(!error.contains("Information schema is changed"), "{error}");
    assert!(!error.contains("mock rollback alert delete error"), "{error}");
    assert_eq!(answer(&mut tk, "show tables like 'mv_retry'"), "RS:[]");
    must_exec_all(&mut tk, &[&format!("create materialized view mv_retry (a, s, cnt) {CREATE_MV_FAST}")]);
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv_retry order by a"), r#"RS:[["1","15","2"],["2","7","1"]]"#);
}

#[test]
fn test_create_materialized_view_history_job_schema_version() {
    let (stack, mut tk) = create_fixture("create table t (a int not null, b int not null)", "insert into t values (1, 10), (1, 5), (2, 7)");
    must_exec_all(&mut tk, &[&format!("create materialized view mv_hist_schema_ver (a, s, cnt) {CREATE_MV_FAST}")]);
    let rows = ddl_job_rows(&mut tk, "admin show ddl jobs where JOB_TYPE='create materialized view'");
    let job_id = rows[0][0].clone().unwrap();
    let meta = single_value(&mut tk, &format!("select hex(job_meta) from mysql.tidb_ddl_history where job_id = {job_id}"));
    let bytes: Vec<u8> = (0..meta.len()).step_by(2).map(|i| u8::from_str_radix(&meta[i..i + 2], 16).unwrap()).collect();
    let mut history = tidb_model::Job::default();
    history.decode(&bytes).unwrap();
    let binlog = history.binlog_info.as_ref().unwrap().read().clone();
    assert!(binlog.schema_version > 0);
    let mv = latest_table(&stack, "mv_hist_schema_ver");
    let table = binlog.table_info.as_ref().expect("the history job's table").read().clone();
    assert_eq!(table.id, mv.id);
    assert_eq!(table.name, mv.name);
}

/// Waits for the CREATE MATERIALIZED VIEW job to reach write reorganization;
/// its ID.
fn wait_create_mview_write_reorganization(session: &mut Session) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let rows = ddl_job_rows(session, "admin show ddl jobs where JOB_TYPE='create materialized view'");
        if let Some(row) = rows.first() {
            if row.len() >= 5 && row[4].as_deref().is_some_and(|state| state.to_lowercase() == "write reorganization") {
                return row[0].clone().unwrap();
            }
        }
        assert!(std::time::Instant::now() < deadline, "the job never reached write reorganization");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// The `admin show ddl jobs where JOB_ID=...` row's job state.
fn ddl_job_state(session: &mut Session, job_id: &str) -> String {
    let rows = ddl_job_rows(session, &format!("admin show ddl jobs where JOB_ID={job_id}"));
    let row = &rows[0];
    row[row.len() - 2].clone().unwrap_or_default()
}

#[test]
fn test_create_materialized_view_cancel_rollback() {
    let (stack, mut tk) = create_fixture("create table t (a int not null, b int not null)", "insert into t values (1, 10), (1, 5), (2, 7)");
    let mut tk_cancel = new_mview_test_kit(&stack, 2604);
    let pause = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild", "pause");
    let mut tk_ddl = new_mview_test_kit(&stack, 2605);
    let ddl = std::thread::spawn(move || answer(&mut tk_ddl, &format!("create materialized view mv_cancel (a, s, cnt) {CREATE_MV_FAST}")));
    let job_id = wait_create_mview_write_reorganization(&mut tk_cancel);
    assert_eq!(answer(&mut tk_cancel, &format!("admin cancel ddl jobs {job_id}")), format!(r#"RS:[["{job_id}","successful"]]"#));
    drop(pause);
    let error = ddl.join().unwrap();
    assert!(error.contains("Cancelled DDL job"), "{error}");
    assert_eq!(ddl_job_state(&mut tk_cancel, &job_id), "rollback done");
    assert_eq!(answer(&mut tk, "show tables like 'mv_cancel'"), "RS:[]");
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_refresh_info"), r#"RS:[["0"]]"#);
    assert_base_without_mviews(&stack, "t");
}

#[test]
fn test_create_materialized_view_build_read_tsquery_type_alignment() {
    let (_stack, _users, mut tk) = go_master_mview_stack();
    must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test", "create table t (a int)", "insert into t values (1)"]);
    assert_eq!(answer(&mut tk, "select * from t"), r#"RS:[["1"]]"#);
    let expected = tk.vars().last_query_info.start_ts;
    assert_ne!(expected, 0);
    assert_eq!(
        answer(
            &mut tk,
            "SELECT COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(@@tidb_last_query_info, '$.start_ts')) AS UNSIGNED), CAST(0 AS UNSIGNED))"
        ),
        format!(r#"RS:[["{expected}"]]"#)
    );
}

#[test]
fn test_create_materialized_view_success_refresh_info_visibility_before_commit() {
    let (stack, mut tk) = create_fixture("create table t (a int not null, b int not null)", "insert into t values (1, 10), (1, 5), (2, 7)");
    let (paused, resume) = (Latch::default(), Latch::default());
    let _after_upsert = tidb_failpoint::enable_call_for_test(
        "github.com/pingcap/tidb/pkg/ddl/afterCreateMaterializedViewSuccessRefreshInfoUpsert",
        {
            let (paused, resume) = (paused.clone(), resume.clone());
            move |_| {
                paused.open();
                resume.wait();
            }
        },
    );
    let _retry = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/ddl/mockCreateMaterializedViewPostBuildAfterRefreshInfoUpsertRetryableErr",
        "1*return(true)",
    );
    // Go's deferred release.
    struct Release(Latch);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.open();
        }
    }
    let release = Release(resume);
    let mut tk_ddl = new_mview_test_kit(&stack, 2606);
    let ddl = std::thread::spawn(move || {
        answer(&mut tk_ddl, &format!("create materialized view mv_upsert_visibility (a, s, cnt) {CREATE_MV_FAST}"))
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let prewrite_ts = loop {
        let rows = ddl_job_rows(&mut tk, "select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info");
        if let [row] = rows.as_slice() {
            if let Some(ts) = row[0].as_deref().and_then(|ts| ts.parse::<u64>().ok()).filter(|&ts| ts != 0) {
                break ts;
            }
        }
        assert!(std::time::Instant::now() < deadline, "the prewrite never stored its refresh info");
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    paused.wait_for("the post-upsert failpoint");
    assert_eq!(single_value(&mut tk, "select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info"), prewrite_ts.to_string());
    // "The input log baseline becomes visible with the successful build
    // watermark."
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_fast_refresh_source_info"), r#"RS:[["0"]]"#);
    drop(release);
    let error = ddl.join().unwrap();
    assert!(error.starts_with("ERR") && error.contains("detected residual build rows on retry"), "{error}");
    assert!(!error.contains("Duplicate entry"), "{error}");
    assert_eq!(answer(&mut tk, "show tables like 'mv_upsert_visibility'"), "RS:[]");
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_refresh_info"), r#"RS:[["0"]]"#);
    assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_fast_refresh_source_info"), r#"RS:[["0"]]"#);
    assert_base_without_mviews(&stack, "t");
}

#[test]
fn test_create_materialized_view_pause_and_resume() {
    let (stack, mut tk) = create_fixture("create table t (a int not null, b int not null)", "insert into t values (1, 10), (1, 5), (2, 7)");
    let pause = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/pauseCreateMaterializedViewBuild", "pause");
    let mut tk_ddl = new_mview_test_kit(&stack, 2607);
    let ddl = std::thread::spawn(move || answer(&mut tk_ddl, &format!("create materialized view mv_pause (a, s, cnt) {CREATE_MV_FAST}")));
    let mut tk_ctl = new_mview_test_kit(&stack, 2608);
    let job_id = wait_create_mview_write_reorganization(&mut tk_ctl);
    assert_eq!(answer(&mut tk_ctl, &format!("admin pause ddl jobs {job_id}")), format!(r#"RS:[["{job_id}","successful"]]"#));
    drop(pause);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while ddl_job_state(&mut tk_ctl, &job_id).to_lowercase() != "paused" {
        assert!(std::time::Instant::now() < deadline, "the job never paused");
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert_eq!(answer(&mut tk, "show tables like 'mv_pause'"), r#"RS:[["mv_pause"]]"#);
    for sql in ["select * from mv_pause", "select * from mv_pause where a = 1", "select * from mv_pause where a in (1, 2)"] {
        exec_err_contains(&mut tk, sql, "initial build is in progress");
    }
    for sql in [
        "insert into mv_pause values (9, 1, 1)",
        "replace into mv_pause values (9, 1, 1)",
        "load data local infile '/tmp/nonexistent.csv' into table mv_pause",
        "import into mv_pause from '/tmp/nonexistent.csv'",
    ] {
        exec_err_contains(&mut tk, sql, "not updatable");
    }
    assert_eq!(answer(&mut tk_ctl, &format!("admin resume ddl jobs {job_id}")), format!(r#"RS:[["{job_id}","successful"]]"#));
    let result = ddl.join().unwrap();
    if result != "OK" {
        assert!(result.contains("detected residual build rows on retry"), "{result}");
        assert_eq!(answer(&mut tk, "show tables like 'mv_pause'"), "RS:[]");
        return;
    }
    assert_eq!(answer(&mut tk, "select a, s, cnt from mv_pause order by a"), r#"RS:[["1","15","2"],["2","7","1"]]"#);
}

#[test]
fn test_create_materialized_view_if_not_exists() {
    // t.Run("existing")
    {
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &[
            "set tidb_mview_enable = on",
            "use test",
            "create table t (a int not null, b int not null)",
            "insert into t values (1, 10)",
            "create materialized view log if not exists on t (a, b)",
            "create materialized view if not exists mv (a, s, cnt) as select a, sum(b), count(*) from t group by a",
        ]);
        assert_eq!(answer(&mut tk, "select * from mv"), r#"RS:[["1","10","1"]]"#);
        let mv_info = serde_json::to_string(&latest_table(&stack, "mv")).unwrap();
        let log_info = serde_json::to_string(&latest_table(&stack, "$mlog$t")).unwrap();
        let refresh_info = answer(&mut tk, "select * from mysql.tidb_mview_refresh_info");
        let source_info = answer(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info");
        let purge_info = answer(&mut tk, "select * from mysql.tidb_mlog_purge_info");
        must_exec_all(&mut tk, &["insert into t values (2, 20)"]);
        let log_rows = answer(&mut tk, "select * from `$mlog$t`");

        must_exec_all(&mut tk, &["create materialized view log if not exists on t (a)"]);
        assert_eq!(answer(&mut tk, "show warnings"), r#"RS:[["Note","1050","Table 'test.$mlog$t' already exists"]]"#);
        must_exec_all(&mut tk, &["create materialized view if not exists mv (a, cnt) as select a, count(*) from t group by a"]);
        assert_eq!(answer(&mut tk, "show warnings"), r#"RS:[["Note","1050","Table 'test.mv' already exists"]]"#);
        assert_eq!(answer(&mut tk, "select * from mv"), r#"RS:[["1","10","1"]]"#);
        assert_eq!(answer(&mut tk, "select * from `$mlog$t`"), log_rows);
        assert_eq!(answer(&mut tk, "select * from mysql.tidb_mview_refresh_info"), refresh_info);
        assert_eq!(answer(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info"), source_info);
        assert_eq!(answer(&mut tk, "select * from mysql.tidb_mlog_purge_info"), purge_info);
        assert_eq!(serde_json::to_string(&latest_table(&stack, "mv")).unwrap(), mv_info);
        assert_eq!(serde_json::to_string(&latest_table(&stack, "$mlog$t")).unwrap(), log_info);

        for (sql, code) in [
            ("create materialized view log on t (a)", "ERR 1050 "),
            ("create materialized view mv (a, cnt) as select a, count(*) from t group by a", "ERR 1050 "),
            ("create materialized view log if not exists on missing (a)", "ERR 1146 "),
            ("create materialized view if not exists missing_mv (a, cnt) as select a, count(*) from missing group by a", "ERR 1146 "),
        ] {
            let answer = answer(&mut tk, sql);
            assert!(answer.starts_with(code), "{sql}: {answer}");
        }
        must_exec_all(&mut tk, &[
            "create table occupied (a int)",
            "create materialized view if not exists occupied (a, cnt) as select a, count(*) from t group by a",
        ]);
        assert_eq!(answer(&mut tk, "show warnings"), r#"RS:[["Note","1050","Table 'test.occupied' already exists"]]"#);
    }

    for is_log in [false, true] {
        // t.Run(fmt.Sprintf("concurrent/mlog=%t", isLog))
        let (stack, _users, mut tk) = go_master_mview_stack();
        must_exec_all(&mut tk, &["set tidb_mview_enable = on", "use test", "create table t (a int not null)", "insert into t values (1)"]);
        let (sql, action, target) = if is_log {
            ("create materialized view log if not exists on t (a)", tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG, "$mlog$t")
        } else {
            must_exec_all(&mut tk, &["create materialized view log on t (a)"]);
            (
                "create materialized view if not exists mv (a, cnt) as select a, count(*) from t group by a",
                tidb_model::ActionType::ACTION_CREATE_MATERIALIZED_VIEW,
                "mv",
            )
        };
        let (entered, release) = (Latch::default(), Latch::default());
        let paused = std::sync::atomic::AtomicBool::new(false);
        let _pause = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", {
            let (entered, release) = (entered.clone(), release.clone());
            move |job| {
                let job = job.downcast_mut::<tidb_model::Job>().unwrap();
                if job.type_ == action
                    && job.table_name.to_string() == target
                    && paused.compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_ok()
                {
                    entered.open();
                    release.wait();
                }
            }
        });
        let create = |connection_id: u64| {
            let mut creator = new_mview_test_kit(&stack, connection_id);
            std::thread::spawn(move || {
                let result = answer(&mut creator, sql);
                let notes = ddl_job_rows(&mut creator, "show warnings").len();
                (result, notes)
            })
        };
        let first = create(2609);
        entered.wait_for("the first create job");
        let second = create(2610);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        // "both creators must pass the existence precheck before either job
        // creates the object"
        while single_value(&mut tk, "select count(*) from mysql.tidb_ddl_job") != "2" {
            assert!(std::time::Instant::now() < deadline, "both creators must pass the existence precheck");
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        release.open();
        let mut notes = 0;
        for creator in [first, second] {
            let (result, creator_notes) = creator.join().unwrap();
            assert_eq!(result, "OK");
            notes += creator_notes;
        }
        assert_eq!(notes, 1);
        assert_eq!(
            answer(&mut tk, &format!("select count(*) from information_schema.tables where table_schema='test' and table_name='{target}'")),
            r#"RS:[["1"]]"#
        );
        assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mlog_purge_info"), r#"RS:[["1"]]"#);
        if !is_log {
            assert_eq!(answer(&mut tk, "select * from mv"), r#"RS:[["1","1"]]"#);
            assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_refresh_info"), r#"RS:[["1"]]"#);
            assert_eq!(answer(&mut tk, "select count(*) from mysql.tidb_mview_fast_refresh_source_info"), r#"RS:[["1"]]"#);
        }
    }
}
