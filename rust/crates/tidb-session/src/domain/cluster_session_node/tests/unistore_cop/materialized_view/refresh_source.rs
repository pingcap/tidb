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
