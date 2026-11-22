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

//! Go master `pkg/executor/test/mviewrefreshsource/mview_refresh_source_test.go`.

use super::*;

/// Go `newMViewRefreshSourceFixture`: the view, and its base table's log.
fn new_fixture() -> (UnistoreClusterStack, Session, i64, i64) {
    let (stack, tk, mview_id) = new_mview_refresh_source_fixture();
    let mlog_id = mlog_id(&latest_table(&stack, "t_source_init"));
    (stack, tk, mview_id, mlog_id)
}

fn rows_text(tk: &mut Session, sql: &str) -> String {
    answer(tk, sql)
}

fn current_mview_id(tk: &mut Session) -> i64 {
    single_value(tk, "select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_init'").parse().unwrap()
}

#[test]
fn test_m_view_refresh_source_info_rejects_missing_records() {
    let (_stack, mut tk, mview_id, _mlog_id) = new_fixture();
    let source_sql = format!("select * from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mview_id}");
    must_exec_all(&mut tk, &[&format!("delete from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mview_id}")]);
    let before_refresh = rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info order by MVIEW_ID");
    let before_data = rows_text(&mut tk, "select * from mv_source_init order by a");
    for method in ["fast", "complete delta apply", "complete in place", "complete out of place"] {
        let error = exec_to_err(&mut tk, &format!("refresh materialized view mv_source_init {method}"));
        assert!(error.contains("input log records are missing"), "{method}: {error}");
        assert_eq!(answer(&mut tk, &source_sql), "RS:[]", "{method}");
        assert_eq!(rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info order by MVIEW_ID"), before_refresh, "{method}");
        assert_eq!(rows_text(&mut tk, "select * from mv_source_init order by a"), before_data, "{method}");
    }
}

#[test]
fn test_m_view_refresh_source_info_cutover_cancel_rolls_back() {
    let (stack, mut tk, mview_id, mlog_id) = new_fixture();
    let before = rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info");
    let tk_inject = Arc::new(std::sync::Mutex::new(super::create::new_mview_test_kit(&stack, 3941)));
    let injected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _step = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", {
        let (tk_inject, injected) = (tk_inject.clone(), injected.clone());
        move |job: &mut dyn std::any::Any| {
            let Some(job) = job.downcast_ref::<tidb_model::Job>() else { return };
            if job.type_ == tidb_model::ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER
                && job.table_id == mview_id
                && !injected.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                // "Force a validation error after cutover has staged
                // refresh-info migration."
                must_exec_all(&mut tk_inject.lock().unwrap(), &[&format!(
                    "delete from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mview_id}"
                )]);
            }
        }
    });
    must_exec_all(&mut tk, &["insert into t_source_init values (1, 5)"]);
    let error = exec_to_err(&mut tk, "refresh materialized view mv_source_init complete out of place");
    assert!(error.contains("input log records are missing"), "{error}");
    assert!(injected.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(current_mview_id(&mut tk), mview_id);
    assert_eq!(rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info"), before);
    assert_eq!(single_value(&mut tk, "select count(*) from mysql.tidb_mview_fast_refresh_source_info"), "0");
    must_exec_all(&mut tk, &[
        &format!("insert into mysql.tidb_mview_fast_refresh_source_info (MVIEW_ID, MLOG_ID) values ({mview_id}, {mlog_id})"),
        "refresh materialized view mv_source_init complete out of place",
    ]);
    assert_eq!(answer(&mut tk, "select * from mv_source_init order by a"), super::refresh::rows(&["1 15 2", "2 20 1"]));
    assert_eq!(single_value(&mut tk, "select count(*) from mysql.tidb_mview_fast_refresh_source_info"), "1");
}

#[test]
fn test_m_view_refresh_source_info_cutover_cancel_after_sql() {
    for changed in [false, true] {
        let (_stack, mut tk, mview_id, _mlog_id) = new_fixture();
        if changed {
            must_exec_all(&mut tk, &[&format!("update mysql.tidb_mview_fast_refresh_source_info set MLOG_ID = -1 where MVIEW_ID = {mview_id}")]);
        }
        let before_refresh = rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info");
        let before_sources = rows_text(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info");
        let _fail = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/mockMViewRefreshOutOfPlaceCutoverAfterMigrateRefreshInfoError", "1*return()");
        let _cancel = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", move |job: &mut dyn std::any::Any| {
            let Some(job) = job.downcast_mut::<tidb_model::Job>() else { return };
            if job.type_ == tidb_model::ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER && job.table_id == mview_id && job.error.is_some() {
                // "Exercise terminal cancellation after SQL has released the
                // runtime writes from statement staging into the DDL
                // transaction buffer."
                job.state = tidb_model::JobState::CANCELLED;
            }
        });
        must_exec_all(&mut tk, &["insert into t_source_init values (1, 5)"]);
        let error = exec_to_err(&mut tk, "refresh materialized view mv_source_init complete out of place");
        assert!(error.contains("error after migrating refresh info"), "changed={changed}: {error}");
        assert_eq!(rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info"), before_refresh, "changed={changed}");
        assert_eq!(rows_text(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info"), before_sources, "changed={changed}");
        assert_eq!(current_mview_id(&mut tk), mview_id, "changed={changed}");
    }
}

#[test]
fn test_m_view_refresh_source_info_baseline_recovery() {
    let truth = "select a, sum(b), count(*) from t_source_init group by a order by a";
    let data = "select * from mv_source_init order by a";
    for method in ["complete delta apply", "complete in place", "complete out of place"] {
        let (_stack, mut tk, mut mview_id, mlog_id) = new_fixture();
        for logs in [vec![-1], vec![0], vec![mlog_id, -1], vec![0, mlog_id]] {
            must_exec_all(&mut tk, &[&format!("delete from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mview_id}")]);
            for log_id in &logs {
                must_exec_all(&mut tk, &[&format!("insert into mysql.tidb_mview_fast_refresh_source_info values ({mview_id}, {log_id})")]);
            }
            let before = rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info");
            let before_sources = rows_text(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID");
            let before_data = rows_text(&mut tk, data);
            must_exec_all(&mut tk, &["insert into t_source_init values (1, 5)"]);
            // Go `exeerrors.ErrMVRefreshLogBaselineChanged` (8183).
            let refused = answer(&mut tk, "refresh materialized view mv_source_init fast");
            assert!(refused.starts_with("ERR 8183 "), "{method} {logs:?}: {refused}");
            let error = exec_to_err(&mut tk, "refresh materialized view mv_source_init fast");
            assert!(error.contains("refresh materialized view test.mv_source_init fast:"), "{error}");
            assert_eq!(rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info"), before);
            assert_eq!(rows_text(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID"), before_sources);
            assert_eq!(rows_text(&mut tk, data), before_data);
            must_exec_all(&mut tk, &[&format!("refresh materialized view mv_source_init {method}")]);
            assert_eq!(rows_text(&mut tk, data), rows_text(&mut tk, truth), "{method} {logs:?}");
            mview_id = current_mview_id(&mut tk);
            assert_eq!(answer(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info"), super::refresh::rows(&[&format!("{mview_id} {mlog_id}")]));
            must_exec_all(&mut tk, &["insert into t_source_init values (2, 3)", "refresh materialized view mv_source_init fast"]);
            assert_eq!(rows_text(&mut tk, data), rows_text(&mut tk, truth), "{method} {logs:?}");
        }
    }
    // t.Run("unchanged input")
    for method in ["complete delta apply", "complete in place"] {
        let (stack, mut tk, mview_id, mlog_id) = new_fixture();
        let mut holder = super::create::new_mview_test_kit(&stack, 3942);
        must_exec_all(&mut holder, &["begin pessimistic"]);
        assert_eq!(
            single_value(&mut holder, &format!("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = {mview_id} and MLOG_ID = {mlog_id} for update")),
            mlog_id.to_string()
        );
        // "Holding the registration lock must not block a COMPLETE whose
        // input baseline is unchanged. A redundant rewrite would time out."
        let _timeout = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvRefreshInputCaptured", |session: &mut dyn std::any::Any| {
            if let Some(session) = session.downcast_mut::<Session>() {
                must_exec_all(session, &["set @@session.innodb_lock_wait_timeout = 1"]);
            }
        });
        let before = last_success_read_tso(&mut tk, mview_id);
        must_exec_all(&mut tk, &["insert into t_source_init values (1, 5)", &format!("refresh materialized view mv_source_init {method}")]);
        assert_eq!(answer(&mut tk, data), super::refresh::rows(&["1 15 2", "2 20 1"]), "{method}");
        assert_eq!(answer(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info"), super::refresh::rows(&[&format!("{mview_id} {mlog_id}")]));
        assert!(last_success_read_tso(&mut tk, mview_id) > before, "{method}");
        must_exec_all(&mut holder, &["rollback"]);
    }
    // t.Run("unchanged out-of-place input")
    {
        let (_stack, mut tk, old_mview_id, mlog_id) = new_fixture();
        // "A migration must preserve additional metadata when the log set is
        // unchanged."
        must_exec_all(&mut tk, &[
            "alter table mysql.tidb_mview_fast_refresh_source_info add column EXTRA int default 7",
            &format!("update mysql.tidb_mview_fast_refresh_source_info set EXTRA = 42 where MVIEW_ID = {old_mview_id}"),
            "insert into t_source_init values (1, 5)",
            "refresh materialized view mv_source_init complete out of place",
        ]);
        let new_mview_id = current_mview_id(&mut tk);
        assert_ne!(old_mview_id, new_mview_id);
        assert_eq!(
            answer(&mut tk, "select MVIEW_ID, MLOG_ID, EXTRA from mysql.tidb_mview_fast_refresh_source_info"),
            super::refresh::rows(&[&format!("{new_mview_id} {mlog_id} 42")])
        );
        assert_eq!(answer(&mut tk, data), super::refresh::rows(&["1 15 2", "2 20 1"]));
        must_exec_all(&mut tk, &["insert into t_source_init values (2, 3)", "refresh materialized view mv_source_init fast"]);
        assert_eq!(answer(&mut tk, data), super::refresh::rows(&["1 15 2", "2 23 2"]));
    }
    // t.Run("empty input")
    {
        let (_stack, mut tk, _mview_id, mlog_id) = new_fixture();
        must_exec_all(&mut tk, &[
            "drop materialized view mv_source_init",
            "create materialized view mv_source_init (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init where false group by a",
        ]);
        for method in ["complete delta apply", "complete in place", "complete out of place"] {
            must_exec_all(&mut tk, &["update mysql.tidb_mview_fast_refresh_source_info set MLOG_ID = 0", &format!("refresh materialized view mv_source_init {method}")]);
            assert_eq!(answer(&mut tk, "select * from mv_source_init"), "RS:[]", "{method}");
            assert_eq!(single_value(&mut tk, "select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info"), mlog_id.to_string(), "{method}");
            must_exec_all(&mut tk, &["refresh materialized view mv_source_init fast"]);
        }
    }
}

fn last_success_read_tso(tk: &mut Session, mview_id: i64) -> u64 {
    single_value(tk, &format!("select LAST_SUCCESS_READ_TSO from mysql.tidb_mview_refresh_info where MVIEW_ID = {mview_id}")).parse().unwrap()
}

#[test]
fn test_m_view_refresh_source_info_recovery_atomicity() {
    let (_stack, mut tk, mview_id, _mlog_id) = new_fixture();
    must_exec_all(&mut tk, &[
        "create materialized view mv_source_peer (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init group by a",
        &format!("update mysql.tidb_mview_fast_refresh_source_info set MLOG_ID = -1 where MVIEW_ID = {mview_id}"),
    ]);
    let before = rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info order by MVIEW_ID");
    let before_sources = rows_text(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID");
    must_exec_all(&mut tk, &["insert into t_source_init values (1, 5), (3, 7)"]);
    let unchanged = |tk: &mut Session| {
        assert_eq!(rows_text(tk, "select * from mysql.tidb_mview_refresh_info order by MVIEW_ID"), before);
        assert_eq!(rows_text(tk, "select * from mysql.tidb_mview_fast_refresh_source_info order by MVIEW_ID, MLOG_ID"), before_sources);
        assert_eq!(answer(tk, "select * from mv_source_init order by a"), super::refresh::rows(&["1 10 1", "2 20 1"]));
    };
    {
        let _fp = tidb_failpoint::enable_for_test(
            "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges",
            r#"return("source recovery rollback")"#,
        );
        let error = exec_to_err(&mut tk, "refresh materialized view mv_source_init complete delta apply");
        assert!(error.contains("source recovery rollback"), "{error}");
    }
    unchanged(&mut tk);
    {
        let _fp = tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewAfterReplaceSourceInfoError", "return()");
        let error = exec_to_err(&mut tk, "refresh materialized view mv_source_init complete delta apply");
        assert!(error.contains("after replacing source info"), "{error}");
    }
    unchanged(&mut tk);
    // "A successful COMPLETE leaves the peer's input baseline and watermark
    // unchanged."
    let peer = "select * from mysql.tidb_mview_refresh_info where MVIEW_ID = (select TIDB_TABLE_ID from information_schema.tables where table_schema='test' and table_name='mv_source_peer')";
    let peer_before = rows_text(&mut tk, peer);
    let computation_ts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let _replaced = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterReplaceSourceInfo", {
        let computation_ts = computation_ts.clone();
        move |ts: &mut dyn std::any::Any| {
            if let Some(ts) = ts.downcast_ref::<u64>() {
                computation_ts.store(*ts, std::sync::atomic::Ordering::SeqCst);
            }
        }
    });
    must_exec_all(&mut tk, &["refresh materialized view mv_source_init complete delta apply"]);
    let computation_ts = computation_ts.load(std::sync::atomic::Ordering::SeqCst);
    assert_ne!(computation_ts, 0);
    assert_eq!(last_success_read_tso(&mut tk, mview_id), computation_ts);
    assert_eq!(rows_text(&mut tk, peer), peer_before);
    assert_eq!(answer(&mut tk, "select * from mv_source_init order by a"), super::refresh::rows(&["1 15 2", "2 20 1", "3 7 1"]));
}

/// Go `mviewRefreshSourceSnapshot`: the snapshot with some tables replaced
/// (`None` hides one), as `TableByID` reads them.
struct ReplacedSnapshot {
    inner: Box<dyn crate::mview_refresh::RefreshSourceSchema>,
    replaced: std::collections::HashMap<i64, Option<tidb_model::GoShared<tidb_model::TableInfo>>>,
}

impl crate::mview_refresh::RefreshSourceSchema for ReplacedSnapshot {
    fn table_info_by_id(&self, id: i64) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
        match self.replaced.get(&id) {
            Some(table) => table.clone(),
            None => self.inner.table_info_by_id(id),
        }
    }
    fn schema_name_by_table_id(&self, id: i64) -> Option<String> {
        self.inner.schema_name_by_table_id(id)
    }
}

/// Go `replaceSnapshot` in `TestMViewRefreshSourceInfoSnapshotIdentity`:
/// changes the computation snapshot (and for FAST the planned log) as
/// `change` names. Each changed object is a copy, as a table definition read
/// from storage is.
fn replace_snapshot(
    change: &str,
    mview_id: i64,
    mlog_id: i64,
    plan_mlog_table_id: Option<&mut i64>,
    snapshot: &mut Box<dyn crate::mview_refresh::RefreshSourceSchema>,
) {
    let mv = snapshot.table_info_by_id(mview_id).expect("the view").read().clone_like_go();
    let source_id = mv.materialized_view.as_ref().unwrap().read().base_table_ids.iter().copied().next().unwrap();
    let source = snapshot.table_info_by_id(source_id).expect("the source").read().clone_like_go();
    let log = snapshot.table_info_by_id(mlog_id).expect("the log").read().clone_like_go();
    let (mut new_mv, mut new_source, mut new_log) = (mv, source, log);
    let copy_mv = |info: &mut tidb_model::TableInfo| {
        let inner = info.materialized_view.as_ref().unwrap().read().clone();
        info.materialized_view = Some(tidb_model::GoShared::new(inner));
    };
    let copy_base = |info: &mut tidb_model::TableInfo| {
        let inner = info.materialized_view_base.as_ref().unwrap().read().clone();
        info.materialized_view_base = Some(tidb_model::GoShared::new(inner));
    };
    let copy_log = |info: &mut tidb_model::TableInfo| {
        let inner = info.materialized_view_log.as_ref().unwrap().read().clone();
        info.materialized_view_log = Some(tidb_model::GoShared::new(inner));
    };
    copy_mv(&mut new_mv);
    copy_base(&mut new_source);
    copy_log(&mut new_log);
    let mut replaced = std::collections::HashMap::new();
    match change {
        "source replaced" | "eliminated source replaced" => {
            replaced.insert(new_source.id, None);
            new_source.id += 10000;
            new_log.materialized_view_log.as_ref().unwrap().write().base_table_id = new_source.id;
            new_mv.materialized_view.as_ref().unwrap().write().base_table_ids = vec![new_source.id].into();
        }
        "log replaced" | "snapshot and plan log replaced" => {
            new_log.id += 10000;
            new_source.materialized_view_base.as_ref().unwrap().write().mlog_id = new_log.id;
            replaced.insert(mlog_id, None);
            if change == "snapshot and plan log replaced" {
                *plan_mlog_table_id.expect("a FAST refresh's plan") = new_log.id;
            }
        }
        "missing log" => new_source.materialized_view_base.as_ref().unwrap().write().mlog_id = 0,
        "planned input replaced" => {
            *plan_mlog_table_id.expect("a FAST refresh's plan") += 10000;
            return;
        }
        other => unreachable!("{other}"),
    }
    for info in [new_mv, new_source, new_log] {
        replaced.insert(info.id, Some(tidb_model::GoShared::new(info)));
    }
    let inner = std::mem::replace(snapshot, Box::new(EmptySnapshot));
    *snapshot = Box::new(ReplacedSnapshot { inner, replaced });
}

/// A placeholder snapshot while [`replace_snapshot`] swaps the real one.
struct EmptySnapshot;

impl crate::mview_refresh::RefreshSourceSchema for EmptySnapshot {
    fn table_info_by_id(&self, _: i64) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
        None
    }
    fn schema_name_by_table_id(&self, _: i64) -> Option<String> {
        None
    }
}

/// Go `TestMViewRefreshSourceInfoSnapshotIdentity`.
#[test]
fn test_m_view_refresh_source_info_snapshot_identity() {
    for method in ["fast", "complete delta apply", "complete in place", "complete out of place"] {
        for change in ["source replaced", "eliminated source replaced", "log replaced", "missing log", "planned input replaced", "snapshot and plan log replaced"] {
            if method != "fast" && matches!(change, "source replaced" | "planned input replaced" | "snapshot and plan log replaced") {
                continue;
            }
            let (_stack, mut tk, mut mview_id, mlog_id) = new_fixture();
            if change == "eliminated source replaced" {
                must_exec_all(&mut tk, &[
                    "drop materialized view mv_source_init",
                    "create materialized view mv_source_init (a, s, cnt) refresh fast as select a, sum(b), count(*) from t_source_init where false group by a",
                ]);
                mview_id = current_mview_id(&mut tk);
            }
            let before = rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info");
            let before_sources = rows_text(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info");
            let before_data = rows_text(&mut tk, "select * from mv_source_init order by a");
            must_exec_all(&mut tk, &["insert into t_source_init values (1, 5)"]);
            let checked = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let change_owned = change.to_owned();
            let _hook = if method == "fast" {
                let checked = Arc::clone(&checked);
                tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvRefreshValidateInputSnapshot", move |input| {
                    let input = input.downcast_mut::<crate::mview_refresh::MvRefreshValidateInputSnapshot>().unwrap();
                    checked.store(true, std::sync::atomic::Ordering::SeqCst);
                    replace_snapshot(&change_owned, mview_id, mlog_id, Some(&mut input.plan_mlog_table_id), &mut input.snapshot);
                })
            } else {
                let checked = Arc::clone(&checked);
                tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvRefreshResolveInputSnapshot", move |input| {
                    let input = input.downcast_mut::<crate::mview_refresh::MvRefreshResolveInputSnapshot>().unwrap();
                    checked.store(true, std::sync::atomic::Ordering::SeqCst);
                    replace_snapshot(&change_owned, mview_id, mlog_id, None, &mut input.snapshot);
                })
            };
            let result = tk.run(&format!("refresh materialized view mv_source_init {method}"));
            drop(_hook);
            let case = format!("{method}/{change}");
            if matches!(change, "eliminated source replaced" | "log replaced" | "missing log") && method != "fast" {
                result.unwrap_or_else(|e| panic!("{case}: {e:?}"));
                if change == "eliminated source replaced" {
                    assert_eq!(rows_text(&mut tk, "select * from mv_source_init"), "RS:[]", "{case}");
                } else {
                    assert_eq!(rows_text(&mut tk, "select * from mv_source_init order by a"), super::refresh::rows(&["1 15 2", "2 20 1"]), "{case}");
                }
                let expected_log_id = match change {
                    "log replaced" => mlog_id + 10000,
                    "missing log" => 0,
                    _ => mlog_id,
                };
                assert_eq!(
                    rows_text(&mut tk, "select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info"),
                    super::refresh::rows(&[&expected_log_id.to_string()]),
                    "{case}"
                );
            } else {
                let err = result.expect_err(&case);
                let error = format!("{err:?}");
                if method == "fast" && matches!(change, "log replaced" | "missing log" | "snapshot and plan log replaced") {
                    let baseline = tidb_exeerrors::ERR_MV_REFRESH_LOG_BASELINE_CHANGED.identity();
                    assert!(matches!(&err, tidb_driver_error::DriverError::Mysql(e) if e.identity == Some(baseline)), "{case}: {error}");
                    assert!(error.contains("refresh materialized view test.mv_source_init fast:"), "{case}: {error}");
                    if change == "missing log" {
                        assert!(!error.contains("COMPLETE") && !error.contains("restore"), "{case}: {error}");
                        assert!(error.contains("source has no materialized view log; cannot perform FAST refresh"), "{case}: {error}");
                    }
                }
                assert_eq!(rows_text(&mut tk, "select * from mysql.tidb_mview_refresh_info"), before, "{case}");
                assert_eq!(rows_text(&mut tk, "select * from mysql.tidb_mview_fast_refresh_source_info"), before_sources, "{case}");
                assert_eq!(rows_text(&mut tk, "select * from mv_source_init order by a"), before_data, "{case}");
            }
            assert!(checked.load(std::sync::atomic::Ordering::SeqCst), "{case}");
        }
    }
}

/// Go `TestMViewRefreshSourceInfoReadTSAfterRetry`: a refresh whose first
/// attempt hits a write conflict retries, validates its input again at the
/// retry's read TSO, and persists that TSO.
#[test]
fn test_m_view_refresh_source_info_read_ts_after_retry() {
    use std::sync::Mutex;
    for method in ["fast", "complete delta apply"] {
        let (_stack, mut tk, mview_id, _mlog_id) = new_fixture();
        must_exec_all(&mut tk, &["insert into t_source_init values (1, 5)"]);
        let validated_ts = Arc::new(Mutex::new(Vec::<u64>::new()));
        let computation_ts = Arc::new(Mutex::new(0u64));
        let metadata_ts = Arc::new(Mutex::new(0u64));
        let captured_validation_count = Arc::new(Mutex::new(0usize));
        let _captured = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvRefreshInputCaptured", {
            let (validated_ts, computation_ts, captured_validation_count) =
                (Arc::clone(&validated_ts), Arc::clone(&computation_ts), Arc::clone(&captured_validation_count));
            move |session: &mut dyn std::any::Any| {
                let session = session.downcast_mut::<Session>().expect("the refresh session");
                // Go `actualRefreshReadTSO`: the refresh session's for-update TS.
                let ts = u64::try_from(session.current_for_update_ts().value()).unwrap_or_default();
                *computation_ts.lock().unwrap() = ts;
                let validated = validated_ts.lock().unwrap();
                assert!(!validated.is_empty());
                *captured_validation_count.lock().unwrap() = validated.len();
                assert_eq!(ts, *validated.last().unwrap());
            }
        });
        let conflict = Arc::new(Mutex::new(None));
        let _hist_running = tidb_failpoint::enable_call_for_test(
            "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterInsertRefreshHistRunning",
            {
                let conflict = Arc::clone(&conflict);
                move |_: &mut dyn std::any::Any| {
                    *conflict.lock().unwrap() = Some(tidb_failpoint::enable_for_test(
                        "github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/pessimisticLockReturnWriteConflict",
                        "1*return(true)",
                    ));
                }
            },
        );
        let _data_changes = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges", {
            let metadata_ts = Arc::clone(&metadata_ts);
            move |session: &mut dyn std::any::Any| {
                let session = session.downcast_mut::<Session>().expect("the refresh session");
                crate::mview_refresh::sys_sql(
                    session,
                    "UPDATE mysql.tidb_mview_fast_refresh_source_info SET MLOG_ID = MLOG_ID WHERE MVIEW_ID = %?",
                    &[tidb_sqlescape::SqlArg::Signed(mview_id)],
                )
                .unwrap();
                *metadata_ts.lock().unwrap() = u64::try_from(session.current_for_update_ts().value()).unwrap_or_default();
            }
        });
        let _validate = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvRefreshValidateInputSnapshot", {
            let validated_ts = Arc::clone(&validated_ts);
            move |input: &mut dyn std::any::Any| {
                let input = input.downcast_mut::<crate::mview_refresh::MvRefreshValidateInputSnapshot>().unwrap();
                validated_ts.lock().unwrap().push(input.read_tso);
            }
        });
        let _resolve = tidb_failpoint::enable_call_for_test("github.com/pingcap/tidb/pkg/executor/mvRefreshResolveInputSnapshot", {
            let validated_ts = Arc::clone(&validated_ts);
            move |input: &mut dyn std::any::Any| {
                let input = input.downcast_mut::<crate::mview_refresh::MvRefreshResolveInputSnapshot>().unwrap();
                validated_ts.lock().unwrap().push(input.read_tso);
            }
        });
        must_exec_all(&mut tk, &[&format!("refresh materialized view mv_source_init {method}")]);
        let validated = validated_ts.lock().unwrap().clone();
        let count = *captured_validation_count.lock().unwrap();
        let computation = *computation_ts.lock().unwrap();
        assert!(count > 1, "{method}: {validated:?}");
        assert_eq!(validated.len(), count, "{method}");
        assert!(computation > validated[0], "{method}");
        assert!(*metadata_ts.lock().unwrap() > computation, "{method}");
        assert_eq!(last_success_read_tso(&mut tk, mview_id), computation, "{method}");
        assert_eq!(rows_text(&mut tk, "select * from mv_source_init order by a"), super::refresh::rows(&["1 15 2", "2 20 1"]), "{method}");
    }
}
