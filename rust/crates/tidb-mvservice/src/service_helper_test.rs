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

//! Go master `pkg/mvservice/service_helper_test.go`, its session-free cases
//! (the session-backed ones run against a node in `tidb-session`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tidb_datatype::Datum;
use tidb_gocontext::Context;
use tidb_model::TableInfo;
use tidb_sqlescape::SqlArg;
use tidb_timer::go_time::{GoTime, MINUTE};

use crate::server_maintainer::ServerInfo;
use crate::service_helper::{
    latest_server_infos_by_instance, need_analyze_mlog, MLogStats, MVServiceSessionContext, MVServiceSessionPool,
    ServiceHelper, SystemSessionPool,
};
use crate::task_handler::{MVTaskHandler, MvsError};
use crate::time_proxy::{mvs_unix, mvs_unix_milli, unix_milli};

/// Go test_helpers_test.go's SQL texts.
const TEST_SQL_FETCH_MV_LOG_PURGE: &str =
    "SELECT NEXT_PURGE_UNIX_SECONDS, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_PURGE_UNIX_SECONDS IS NOT NULL";
const TEST_SQL_FETCH_MV_REFRESH: &str = "SELECT NEXT_REFRESH_UNIX_SECONDS, MVIEW_ID, LAST_SUCCESS_READ_TSO FROM mysql.tidb_mview_refresh_info WHERE NEXT_REFRESH_UNIX_SECONDS IS NOT NULL";
const TEST_SQL_REFRESH_MV: &str = "REFRESH MATERIALIZED VIEW %n.%n FAST";
const TEST_SQL_FIND_MV_NEXT_TIME: &str =
    "SELECT NEXT_REFRESH_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_REFRESH_UNIX_SECONDS IS NOT NULL";
const TEST_SQL_PURGE_MV_LOG: &str = "PURGE MATERIALIZED VIEW LOG ON %n.%n";
const TEST_SQL_FIND_PURGE_NEXT_TIME: &str =
    "SELECT NEXT_PURGE_UNIX_SECONDS FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? AND NEXT_PURGE_UNIX_SECONDS IS NOT NULL";

/// Go `recordingSessionContext`: restricted SQL answered from canned rows by
/// its text, every call recorded, the schema from Go's `withMockInfoSchema`.
#[derive(Default)]
pub(crate) struct RecordingSession {
    pub(crate) executed_sql: Vec<String>,
    pub(crate) exec_errs: HashMap<String, MvsError>,
    pub(crate) executed_restricted_sql: Vec<String>,
    pub(crate) executed_restricted_args: Vec<Vec<String>>,
    pub(crate) restricted_rows: HashMap<String, Vec<Vec<Datum>>>,
    pub(crate) restricted_errs: HashMap<String, MvsError>,
    pub(crate) restricted_affected_rows: HashMap<String, Vec<u64>>,
    restricted_affected_pos: HashMap<String, usize>,
    affected_rows: u64,
    /// Go `withMockInfoSchema`'s tables, in schema `test`.
    pub(crate) tables: HashMap<i64, TableInfo>,
    pub(crate) global_vars: HashMap<String, String>,
    pub(crate) global_var_errs: HashMap<String, MvsError>,
    /// Go `faultyGlobalAccessor.failAfter`: a variable's reads fail after
    /// this many succeed.
    pub(crate) global_var_fail_after: HashMap<String, usize>,
    global_var_get_calls: HashMap<String, usize>,
    /// The values a session variable refuses (Go `SetSystemVar`'s validation
    /// of `not-a-float`, `bad-quota`, ...).
    pub(crate) invalid_system_var_values: Vec<(String, String)>,
    pub(crate) system_vars: HashMap<String, String>,
    /// The listed session variables' values at each restricted statement
    /// (Go's `restricted*` slices).
    pub(crate) watched_vars: Vec<String>,
    pub(crate) restricted_var_values: Vec<HashMap<String, Option<String>>>,
    pub(crate) store_version: u64,
}

pub(crate) struct RecordingSessionContext(pub(crate) Mutex<RecordingSession>);

fn arg_text(arg: &SqlArg<'_>) -> String {
    match arg {
        SqlArg::String(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        SqlArg::Signed(v) => v.to_string(),
        SqlArg::Unsigned(v) => v.to_string(),
        other => format!("{other:?}"),
    }
}

impl MVServiceSessionContext for RecordingSessionContext {
    fn exec_restricted_sql(&self, sql: &str, args: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, MvsError> {
        let mut s = self.0.lock().unwrap();
        s.executed_restricted_sql.push(sql.to_owned());
        s.executed_restricted_args.push(args.iter().map(arg_text).collect());
        let values = s.watched_vars.iter().map(|name| (name.clone(), s.system_vars.get(name).cloned())).collect();
        s.restricted_var_values.push(values);
        let affected = match s.restricted_affected_rows.get(sql).cloned() {
            Some(seq) => {
                let pos = s.restricted_affected_pos.entry(sql.to_owned()).or_default();
                let value = seq.get(*pos).copied().unwrap_or(0);
                *pos += 1;
                value
            }
            None => 0,
        };
        s.affected_rows = affected;
        if let Some(err) = s.restricted_errs.get(sql) {
            return Err(err.clone());
        }
        Ok(s.restricted_rows.get(sql).cloned().unwrap_or_default())
    }

    fn execute_internal(&self, sql: &str) -> Result<(), MvsError> {
        let mut s = self.0.lock().unwrap();
        s.executed_sql.push(sql.to_owned());
        s.exec_errs.get(sql).cloned().map_or(Ok(()), Err)
    }

    fn affected_rows(&self) -> u64 {
        self.0.lock().unwrap().affected_rows
    }

    fn table_info_by_id(&self, id: i64) -> Option<TableInfo> {
        self.0.lock().unwrap().tables.get(&id).map(TableInfo::clone_like_go)
    }

    fn schema_name_by_id(&self, _id: i64) -> Option<String> {
        Some("test".to_owned())
    }

    fn global_system_var(&self, name: &str) -> Result<String, MvsError> {
        let mut s = self.0.lock().unwrap();
        let calls = {
            let calls = s.global_var_get_calls.entry(name.to_owned()).or_default();
            *calls += 1;
            *calls
        };
        if let Some(&ok_after) = s.global_var_fail_after.get(name) {
            if calls > ok_after {
                return Err(s
                    .global_var_errs
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| MvsError::Other("mock global read failure".to_owned())));
            }
        } else if let Some(err) = s.global_var_errs.get(name) {
            return Err(err.clone());
        }
        Ok(s.global_vars.get(name).cloned().unwrap_or_default())
    }

    fn system_var(&self, name: &str) -> Option<String> {
        self.0.lock().unwrap().system_vars.get(name).cloned()
    }

    fn set_system_var(&self, name: &str, value: &str) -> Result<(), MvsError> {
        let mut s = self.0.lock().unwrap();
        if s.invalid_system_var_values.iter().any(|(n, v)| n == name && v == value) {
            return Err(MvsError::Other(format!("invalid value {value} for {name}")));
        }
        s.system_vars.insert(name.to_owned(), value.to_owned());
        Ok(())
    }

    fn store_current_version(&self) -> Result<u64, MvsError> {
        Ok(self.0.lock().unwrap().store_version)
    }

    fn statement_now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

/// Go `recordingSessionPool`.
pub(crate) struct RecordingSessionPool(pub(crate) Arc<RecordingSessionContext>);

impl MVServiceSessionPool for RecordingSessionPool {
    fn with_session(&self, f: &mut dyn FnMut(&dyn MVServiceSessionContext) -> Result<(), MvsError>) -> Result<(), MvsError> {
        f(self.0.as_ref())
    }
}

/// `newRecordingSessionContext()` and its pool, as `SessionPool`.
fn new_recording_session() -> (Arc<RecordingSessionContext>, SystemSessionPool) {
    let se = Arc::new(RecordingSessionContext(Mutex::new(RecordingSession::default())));
    (Arc::clone(&se), SystemSessionPool(Arc::new(RecordingSessionPool(se))))
}

/// Go `&serviceHelper{}`.
fn service_helper() -> ServiceHelper {
    ServiceHelper::new_for_test()
}

fn ints(values: &[Option<i64>]) -> Vec<Datum> {
    values.iter().map(|v| v.map_or(Datum::Null, Datum::Int)).collect()
}

/// Go `withMockInfoSchema(t, tables...)`.
fn with_mock_info_schema(se: &RecordingSessionContext, tables: Vec<TableInfo>) {
    let mut s = se.0.lock().unwrap();
    for table in tables {
        s.tables.insert(table.id, table);
    }
}

fn table(id: i64, name: &str) -> TableInfo {
    TableInfo {
        id,
        name: tidb_ast::CiString::new(name),
        state: tidb_model::SchemaState::PUBLIC,
        db_id: 1,
        ..Default::default()
    }
}

/// Go `buildMockMVBaseAndMVLogTables`.
fn build_mock_mv_base_and_mv_log_tables(base_table_id: i64, mlog_id: i64, mview_ids: &[i64]) -> (TableInfo, TableInfo) {
    let mut base = table(base_table_id, "mv_base");
    base.materialized_view_base = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewBaseInfo {
        mlog_id,
        mview_ids: mview_ids.to_vec().into(),
        ..Default::default()
    }));
    let mut mlog = table(mlog_id, "mlog1");
    mlog.materialized_view_log = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewLogInfo {
        base_table_id,
        purge_method: "TIME_WINDOW".to_owned(),
        purge_start_with: "'2026-01-02 03:04:05'".to_owned(),
        purge_next: "60".to_owned(),
        ..Default::default()
    }));
    (base, mlog)
}

#[test]
fn test_server_helper_load_all_tidb_m_log_purge() {
    let (se, pool) = new_recording_session();
    se.0.lock().unwrap().restricted_rows.insert(
        TEST_SQL_FETCH_MV_LOG_PURGE.to_owned(),
        vec![
            // Valid rows.
            ints(&[Some(600), Some(201)]),
            ints(&[Some(720), Some(202)]),
            // "Invalid row with non-positive ID should be ignored."
            ints(&[Some(720), Some(0)]),
            // "Invalid row with NULL NEXT_PURGE_UNIX_SECONDS should be ignored."
            ints(&[None, Some(203)]),
        ],
    );
    let got = service_helper().load_all_tidb_mv_log_purge(&Context::background(), &pool).unwrap();
    assert_eq!(vec![TEST_SQL_FETCH_MV_LOG_PURGE.to_owned()], se.0.lock().unwrap().executed_restricted_sql);
    assert_eq!(got.len(), 2);
    for (id, sec) in [(201, 600), (202, 720)] {
        let l = &got[&id];
        let expect = mvs_unix(sec, 0);
        assert_eq!(expect, *l.next_purge.lock().unwrap());
        assert_eq!(unix_milli(&expect), l.order_ts.load(std::sync::atomic::Ordering::SeqCst));
    }
}

#[test]
fn test_server_helper_load_all_tidb_mv_refresh() {
    let (se, pool) = new_recording_session();
    se.0.lock().unwrap().restricted_rows.insert(
        TEST_SQL_FETCH_MV_REFRESH.to_owned(),
        vec![
            ints(&[Some(900), Some(101), Some(123_456_789)]),
            ints(&[Some(1200), Some(102), Some(223_456_789)]),
            ints(&[Some(1200), Some(0), Some(323_456_789)]),
            ints(&[None, Some(103), Some(423_456_789)]),
        ],
    );
    let got = service_helper().load_all_tidb_mv_refresh(&Context::background(), &pool).unwrap();
    assert_eq!(vec![TEST_SQL_FETCH_MV_REFRESH.to_owned()], se.0.lock().unwrap().executed_restricted_sql);
    assert_eq!(got.len(), 2);
    for (id, sec, tso) in [(101, 900, 123_456_789u64), (102, 1200, 223_456_789)] {
        let m = &got[&id];
        let expect = mvs_unix(sec, 0);
        let state = m.state.lock().unwrap();
        assert_eq!(expect, state.next_refresh);
        assert_eq!(unix_milli(&expect), m.order_ts.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(tso, state.last_success_read_tso);
        // Go `mvsUnixMilli(oracle.ExtractPhysical(tso))`.
        assert_eq!(mvs_unix_milli((tso >> 18) as i64), state.last_success_time);
    }
}

#[test]
fn test_server_helper_refresh_mv_deleted_when_meta_not_found() {
    let (se, pool) = new_recording_session();
    let next_refresh = service_helper().refresh_mv(&Context::background(), &pool, 100).unwrap();
    assert!(next_refresh.is_zero());
    let s = se.0.lock().unwrap();
    assert!(s.executed_sql.is_empty());
    assert!(s.executed_restricted_sql.is_empty());
}

#[test]
fn test_server_helper_refresh_mv_success() {
    let (se, pool) = new_recording_session();
    let expected_next_refresh = mvs_unix(1_700_000_000, 0).add(MINUTE);
    se.0.lock().unwrap().restricted_rows.insert(
        TEST_SQL_FIND_MV_NEXT_TIME.to_owned(),
        vec![ints(&[Some(expected_next_refresh.unix())])],
    );
    with_mock_info_schema(&se, vec![table(101, "mv1")]);
    let next_refresh = service_helper().refresh_mv(&Context::background(), &pool, 101).unwrap();
    assert_eq!(expected_next_refresh.unix(), next_refresh.unix());
    let s = se.0.lock().unwrap();
    assert!(s.executed_sql.is_empty());
    assert_eq!(vec![TEST_SQL_REFRESH_MV.to_owned(), TEST_SQL_FIND_MV_NEXT_TIME.to_owned()], s.executed_restricted_sql);
}


/// Go `TestLatestServerInfosByInstanceKeepsNewestDDLID`.
#[test]
fn test_latest_server_infos_by_instance_keeps_newest_ddlid() {
    let server = |id: &str, ip: &str, start: i64| {
        (
            ServerInfo { id: id.to_owned(), ip: ip.to_owned(), port: 4000, start_timestamp: start },
            format!("{ip}:4000"),
        )
    };
    let got = latest_server_infos_by_instance(vec![
        server("old-node", "127.0.0.1", 100),
        server("new-node", "127.0.0.1", 200),
        server("peer-node", "127.0.0.2", 150),
    ]);
    assert_eq!(got.len(), 2);
    assert!(!got.contains_key("old-node"));
    assert_eq!(got["new-node"], server("new-node", "127.0.0.1", 200).0);
    assert_eq!(got["peer-node"], server("peer-node", "127.0.0.2", 150).0);
    // The same instances listed newest first keep the same answer.
    let got = latest_server_infos_by_instance(vec![
        server("new-node", "127.0.0.1", 200),
        server("old-node", "127.0.0.1", 100),
        server("peer-node", "127.0.0.2", 150),
    ]);
    assert_eq!(got.len(), 2);
    assert!(got.contains_key("new-node"));
}

/// Go `TestNeedAnalyzeMLog` (with Go's `AutoAnalyzeMinCnt` 1000).
#[test]
fn test_need_analyze_m_log() {
    const MIN: i64 = 1000;
    let analyzed = |realtime_count, modify_count| MLogStats {
        realtime_count,
        modify_count,
        analyzed: true,
        analyze_row_count: realtime_count as f64,
        ..MLogStats::default()
    };
    let unanalyzed = |realtime_count| MLogStats { realtime_count, ..MLogStats::default() };
    let cases: [(&str, Option<MLogStats>, f64, bool, &str); 7] = [
        ("nil stats", None, 0.5, false, ""),
        ("pseudo stats", Some(MLogStats { pseudo: true, ..unanalyzed(MIN + 1) }), 0.5, false, ""),
        ("below min count", Some(unanalyzed(MIN - 1)), 0.0, false, ""),
        ("unanalyzed reaches min count even when ratio disabled", Some(unanalyzed(MIN)), 0.0, true, "mlog unanalyzed"),
        ("analyzed modify trigger disabled by zero ratio", Some(analyzed(1000, 2000)), 0.0, false, ""),
        ("analyzed below ratio", Some(analyzed(1000, 200)), 0.5, false, ""),
        ("analyzed above ratio", Some(analyzed(1000, 600)), 0.5, true, "too many mlog modifications"),
    ];
    for (name, stats, ratio, need, reason) in cases {
        let (got_need, got_reason) = need_analyze_mlog(stats.as_ref(), ratio, MIN);
        assert_eq!(got_need, need, "{name}");
        assert!(got_reason.contains(reason), "{name}: {got_reason}");
        if !need {
            assert!(got_reason.is_empty(), "{name}");
        }
    }
}

/// Go `TestBuildResolvedMVRefreshAlertSQL`.
#[test]
fn test_build_resolved_mv_refresh_alert_sql() {
    use crate::service_helper::{
        build_clear_disabled_mv_refresh_alert_level_sql, build_clear_resolved_mv_refresh_alert_level_sql,
        build_delete_resolved_mv_refresh_alert_sql,
    };
    let now = crate::time_proxy::mvs_unix(1_700_000_000, 0);
    let ids = [103, 104];
    assert_eq!(
        build_delete_resolved_mv_refresh_alert_sql(&ids),
        "DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID IN (103,104) AND REFRESH_FAILED IS NULL"
    );
    assert_eq!(
        build_clear_resolved_mv_refresh_alert_level_sql(&now, &ids),
        format!(
            "UPDATE mysql.tidb_mview_refresh_alert SET ALERT_LEVEL = NULL, UPDATE_TIME = '{}' WHERE MVIEW_ID IN (103,104) AND REFRESH_FAILED IS NOT NULL AND ALERT_LEVEL IS NOT NULL",
            now.naive_local().format("%Y-%m-%d %H:%M:%S")
        )
    );
    assert_eq!(
        build_clear_disabled_mv_refresh_alert_level_sql(),
        "UPDATE mysql.tidb_mview_refresh_alert AS a
JOIN mysql.tidb_mview_refresh_info AS i ON a.MVIEW_ID = i.MVIEW_ID
SET a.ALERT_LEVEL = NULL, a.UPDATE_TIME = NOW(6)
WHERE i.NEXT_REFRESH_UNIX_SECONDS IS NULL AND a.ALERT_LEVEL IS NOT NULL"
    );
}

/// Go `setupPurgeMVLogMetaForTest`.
fn setup_purge_mv_log_meta_for_test(se: &RecordingSessionContext, next_time_rows: Vec<Vec<Datum>>) {
    se.0.lock().unwrap().restricted_rows.insert(TEST_SQL_FIND_PURGE_NEXT_TIME.to_owned(), next_time_rows);
    let (mview_table, mvlog_table) = build_mock_mv_base_and_mv_log_tables(101, 201, &[101, 102]);
    with_mock_info_schema(se, vec![mview_table, mvlog_table]);
}

fn canceled_manually() -> MvsError {
    MvsError::Other("materialized view task canceled manually".to_owned())
}

#[test]
fn test_server_helper_refresh_mv_manual_cancel_normalized() {
    let (se, pool) = new_recording_session();
    se.0.lock().unwrap().restricted_errs.insert(TEST_SQL_REFRESH_MV.to_owned(), canceled_manually());
    with_mock_info_schema(&se, vec![table(101, "mv1")]);
    let err = service_helper().refresh_mv(&Context::background(), &pool, 101).unwrap_err();
    assert_eq!(crate::task_handler::err_mv_task_canceled_manually(), err);
    assert_eq!(vec![TEST_SQL_REFRESH_MV.to_owned()], se.0.lock().unwrap().executed_restricted_sql);
}

#[test]
fn test_server_helper_refresh_mv_deleted_when_next_time_not_found() {
    let (se, pool) = new_recording_session();
    with_mock_info_schema(&se, vec![table(101, "mv1")]);
    se.0.lock().unwrap().restricted_rows.insert(TEST_SQL_FIND_MV_NEXT_TIME.to_owned(), Vec::new());
    let next_refresh = service_helper().refresh_mv(&Context::background(), &pool, 101).unwrap();
    assert!(next_refresh.is_zero());
    let s = se.0.lock().unwrap();
    assert!(s.executed_sql.is_empty());
    assert_eq!(vec![TEST_SQL_REFRESH_MV.to_owned(), TEST_SQL_FIND_MV_NEXT_TIME.to_owned()], s.executed_restricted_sql);
}

#[test]
fn test_server_helper_purge_mv_log_deleted_when_meta_not_found() {
    let (se, pool) = new_recording_session();
    let next_purge = service_helper().purge_mv_log(&Context::background(), &pool, 200).unwrap();
    assert!(next_purge.is_zero());
    let s = se.0.lock().unwrap();
    assert!(s.executed_sql.is_empty());
    assert!(s.executed_restricted_sql.is_empty());
}

fn require_expected_purge_mv_log_sql(se: &RecordingSessionContext) {
    let s = se.0.lock().unwrap();
    assert!(s.executed_sql.is_empty());
    // Go `testExpectedPurgeMVLogSQL`.
    assert_eq!(vec![TEST_SQL_PURGE_MV_LOG.to_owned(), TEST_SQL_FIND_PURGE_NEXT_TIME.to_owned()], s.executed_restricted_sql);
    assert_eq!(s.executed_restricted_args.len(), 2);
    assert_eq!(s.executed_restricted_args[0].len(), 2);
    assert_eq!("mv_base", s.executed_restricted_args[0][1]);
}

#[test]
fn test_server_helper_purge_mv_log_success() {
    let (se, pool) = new_recording_session();
    setup_purge_mv_log_meta_for_test(&se, vec![ints(&[Some(mvs_unix(1_700_000_000, 0).add(MINUTE).unix())])]);
    let next_purge = service_helper().purge_mv_log(&Context::background(), &pool, 201).unwrap();
    assert!(!next_purge.is_zero());
    assert!(next_purge.after(&mvs_unix(1_700_000_000, 0).add(-2 * tidb_timer::go_time::SECOND)));
    require_expected_purge_mv_log_sql(&se);
}

#[test]
fn test_server_helper_purge_mv_log_manual_cancel_normalized() {
    let (se, pool) = new_recording_session();
    setup_purge_mv_log_meta_for_test(&se, Vec::new());
    se.0.lock().unwrap().restricted_errs.insert(TEST_SQL_PURGE_MV_LOG.to_owned(), canceled_manually());
    let err = service_helper().purge_mv_log(&Context::background(), &pool, 201).unwrap_err();
    assert_eq!(crate::task_handler::err_mv_task_canceled_manually(), err);
    assert_eq!(vec![TEST_SQL_PURGE_MV_LOG.to_owned()], se.0.lock().unwrap().executed_restricted_sql);
}

#[test]
fn test_purge_mv_log_skip_when_auto_purge() {
    let now = mvs_unix(1_700_000_000, 0);
    let cases: [(&str, Vec<Vec<Datum>>, GoTime); 2] = [
        ("next_time_null", vec![ints(&[None])], GoTime::zero()),
        ("next_time_future", vec![ints(&[Some(now.add(MINUTE).unix())])], mvs_unix(now.add(MINUTE).unix(), 0)),
    ];
    for (name, next_rows, expected) in cases {
        let (se, pool) = new_recording_session();
        setup_purge_mv_log_meta_for_test(&se, next_rows);
        let next_purge = service_helper().purge_mv_log(&Context::background(), &pool, 201).unwrap();
        assert_eq!(expected, next_purge, "{name}");
        require_expected_purge_mv_log_sql(&se);
    }
}

const TEST_SQL_LOCK_MV_NEXT_TIME: &str =
    "SELECT NEXT_REFRESH_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? FOR UPDATE NOWAIT";
const TEST_SQL_UPDATE_MV_NEXT_TIME: &str = "UPDATE mysql.tidb_mview_refresh_info SET NEXT_REFRESH_UNIX_SECONDS = %? WHERE MVIEW_ID = %?";
const TEST_SQL_FETCH_MV_LOG_ACCUMULATION: &str = "SELECT MLOG_ID FROM mysql.tidb_mlog_purge_info";
const TEST_SQL_COUNT_MV_LOG_ROWS: &str = "SELECT /*+ read_from_storage(tiflash[%n.%n]) */ COUNT(*) FROM %n.%n";

#[test]
fn test_server_helper_get_current_tso() {
    let (se, pool) = new_recording_session();
    se.0.lock().unwrap().store_version = 123_456;
    assert_eq!(123_456, service_helper().get_current_tso(&Context::background(), &pool).unwrap());
}

#[test]
fn test_server_helper_get_current_tso_invalid_version() {
    let (se, pool) = new_recording_session();
    se.0.lock().unwrap().store_version = 0;
    let err = service_helper().get_current_tso(&Context::background(), &pool).unwrap_err();
    assert!(err.to_string().contains("invalid version"), "{err}");
}

/// Go `tryBackoffMVTaskManualCancel` over the refresh SQL with `resolve`.
fn backoff_with(
    se: &Arc<RecordingSessionContext>,
    pool: &SystemSessionPool,
    next_time: &GoTime,
    resolve: fn(&dyn MVServiceSessionContext, i64) -> Result<(Option<GoTime>, bool), MvsError>,
) -> (bool, GoTime, Option<MvsError>) {
    let _ = se;
    crate::service_helper::try_backoff_mv_task_manual_cancel(
        pool,
        TEST_SQL_LOCK_MV_NEXT_TIME,
        TEST_SQL_UPDATE_MV_NEXT_TIME,
        101,
        next_time,
        resolve,
    )
}

fn base_time() -> GoTime {
    mvs_unix(1_700_000_000, 0)
}

fn lock_row(se: &RecordingSessionContext, value: Option<i64>) {
    se.0.lock().unwrap().restricted_rows.insert(TEST_SQL_LOCK_MV_NEXT_TIME.to_owned(), vec![ints(&[value])]);
}

fn require_backoff_sql(se: &RecordingSessionContext, executed_sql: &[&str], restricted: &[&str]) {
    let s = se.0.lock().unwrap();
    assert_eq!(executed_sql.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>(), s.executed_sql);
    assert_eq!(restricted.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>(), s.executed_restricted_sql);
    assert_eq!(vec!["101".to_owned()], s.executed_restricted_args[0]);
}

fn update_args(se: &RecordingSessionContext) -> Vec<String> {
    se.0.lock().unwrap().executed_restricted_args[1].clone()
}

static RESOLVER_CALLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Go `TestServerHelperTryBackoffRefreshManualCancel` (its subtests in order).
#[test]
fn test_server_helper_try_backoff_refresh_manual_cancel() {
    use crate::service::MANUAL_CANCEL_BACKOFF_DELAY;
    // "applied"
    let (se, pool) = new_recording_session();
    lock_row(&se, Some(1));
    let expected_next = base_time().add(MANUAL_CANCEL_BACKOFF_DELAY);
    let (applied, applied_next, err) =
        backoff_with(&se, &pool, &expected_next, |_, _| Ok((Some(mvs_unix(1_700_000_000, 0).add(MINUTE)), false)));
    assert!(err.is_none());
    assert!(applied);
    assert_eq!(expected_next, applied_next);
    require_backoff_sql(&se, &["BEGIN PESSIMISTIC", "COMMIT"], &[TEST_SQL_LOCK_MV_NEXT_TIME, TEST_SQL_UPDATE_MV_NEXT_TIME]);
    assert_eq!(vec![expected_next.unix().to_string(), "101".to_owned()], update_args(&se));

    // "keep_later_next_time"
    let (se, pool) = new_recording_session();
    lock_row(&se, Some(1));
    let cooldown_next = base_time().add(MANUAL_CANCEL_BACKOFF_DELAY);
    let current_next = base_time().add(MANUAL_CANCEL_BACKOFF_DELAY + MINUTE);
    let (applied, applied_next, err) = backoff_with(&se, &pool, &cooldown_next, |_, _| {
        Ok((Some(mvs_unix(1_700_000_000, 0).add(crate::service::MANUAL_CANCEL_BACKOFF_DELAY + MINUTE)), true))
    });
    assert!(err.is_none());
    assert!(applied);
    assert_eq!(current_next, applied_next);
    require_backoff_sql(&se, &["BEGIN PESSIMISTIC", "COMMIT"], &[TEST_SQL_LOCK_MV_NEXT_TIME, TEST_SQL_UPDATE_MV_NEXT_TIME]);
    assert_eq!(vec![current_next.unix().to_string(), "101".to_owned()], update_args(&se));

    // "no_next_schedule_clears_next_time"
    let (se, pool) = new_recording_session();
    lock_row(&se, Some(1));
    let (applied, applied_next, err) = backoff_with(&se, &pool, &cooldown_next, |_, _| Ok((None, true)));
    assert!(err.is_none());
    assert!(applied);
    assert!(applied_next.is_zero());
    require_backoff_sql(&se, &["BEGIN PESSIMISTIC", "COMMIT"], &[TEST_SQL_LOCK_MV_NEXT_TIME, TEST_SQL_UPDATE_MV_NEXT_TIME]);
    assert_eq!(vec!["Null".to_owned(), "101".to_owned()], update_args(&se));

    // "locked_next_time_null_keeps_unscheduled"
    let (se, pool) = new_recording_session();
    lock_row(&se, None);
    RESOLVER_CALLED.store(false, std::sync::atomic::Ordering::SeqCst);
    let (applied, applied_next, err) = backoff_with(&se, &pool, &cooldown_next, |_, _| {
        RESOLVER_CALLED.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok((Some(mvs_unix(1_700_000_000, 0).add(tidb_timer::go_time::HOUR)), true))
    });
    assert!(err.is_none());
    assert!(applied);
    assert!(applied_next.is_zero());
    assert!(!RESOLVER_CALLED.load(std::sync::atomic::Ordering::SeqCst));
    require_backoff_sql(&se, &["BEGIN PESSIMISTIC", "COMMIT"], &[TEST_SQL_LOCK_MV_NEXT_TIME]);

    // "resolver_error_falls_back_to_cooldown"
    let (se, pool) = new_recording_session();
    lock_row(&se, Some(1));
    let (applied, applied_next, err) =
        backoff_with(&se, &pool, &cooldown_next, |_, _| Err(MvsError::Other("mock resolver error".to_owned())));
    assert!(err.is_none());
    assert!(applied);
    assert_eq!(cooldown_next, applied_next);
    require_backoff_sql(&se, &["BEGIN PESSIMISTIC", "COMMIT"], &[TEST_SQL_LOCK_MV_NEXT_TIME, TEST_SQL_UPDATE_MV_NEXT_TIME]);
    assert_eq!(vec![cooldown_next.unix().to_string(), "101".to_owned()], update_args(&se));

    // "lock_conflict"
    let (se, pool) = new_recording_session();
    se.0.lock().unwrap().restricted_errs.insert(
        TEST_SQL_LOCK_MV_NEXT_TIME.to_owned(),
        MvsError::Sql { code: 3572, message: "lock acquire failed and no wait is set".to_owned() },
    );
    let (applied, applied_next, err) = service_helper().try_backoff_refresh_manual_cancel(
        &Context::background(),
        &pool,
        101,
        &base_time().add(MANUAL_CANCEL_BACKOFF_DELAY),
    );
    assert!(err.is_none());
    assert!(!applied);
    assert!(applied_next.is_zero());
    let s = se.0.lock().unwrap();
    assert_eq!(vec!["BEGIN PESSIMISTIC".to_owned(), "ROLLBACK".to_owned()], s.executed_sql);
    assert_eq!(vec![TEST_SQL_LOCK_MV_NEXT_TIME.to_owned()], s.executed_restricted_sql);
}

#[test]
fn test_server_helper_load_all_tidb_mv_log_accumulation_tasks_skips_disabled_by_default() {
    let (se, pool) = new_recording_session();
    se.0.lock().unwrap().restricted_rows.insert(
        TEST_SQL_FETCH_MV_LOG_ACCUMULATION.to_owned(),
        vec![ints(&[Some(201)]), ints(&[Some(202)])],
    );
    let (enabled_base_table, enabled_table) = build_mock_mv_base_and_mv_log_tables(101, 201, &[101]);
    enabled_table.materialized_view_log.as_ref().unwrap().write().log_accumulation_alert_rows = Some(2048);
    let (disabled_base_table, disabled_table) = build_mock_mv_base_and_mv_log_tables(102, 202, &[102]);
    with_mock_info_schema(&se, vec![enabled_base_table, enabled_table, disabled_base_table, disabled_table]);

    let got = service_helper().load_all_tidb_mv_log_accumulation_tasks(&Context::background(), &pool).unwrap();
    let s = se.0.lock().unwrap();
    assert_eq!(vec![TEST_SQL_FETCH_MV_LOG_ACCUMULATION.to_owned()], s.executed_restricted_sql);
    assert_eq!(s.executed_restricted_args.len(), 1);
    assert!(s.executed_restricted_args[0].is_empty());
    assert_eq!(got.len(), 1);
    assert_eq!(
        crate::task_handler::MlogAccumulationTask { schema_name: "test".to_owned(), mlog_name: "mlog1".to_owned(), alert_rows: 2048 },
        got[&201]
    );
}

#[test]
fn test_server_helper_load_tidb_mv_log_accumulation_row_counts() {
    let concurrency = tidb_vardef::tidb_vars::TIDB_DIST_SQL_SCAN_CONCURRENCY;
    let (se, pool) = new_recording_session();
    {
        let mut s = se.0.lock().unwrap();
        s.system_vars.insert(concurrency.to_owned(), "7".to_owned());
        s.watched_vars = vec![concurrency.to_owned()];
        s.restricted_rows.insert(TEST_SQL_COUNT_MV_LOG_ROWS.to_owned(), vec![ints(&[Some(50)])]);
    }
    let tasks = HashMap::from([(
        201,
        crate::task_handler::MlogAccumulationTask { schema_name: "test".to_owned(), mlog_name: "mlog1".to_owned(), alert_rows: 50 },
    )]);
    let got = service_helper().load_tidb_mv_log_accumulation_row_counts(&Context::background(), &pool, &tasks).unwrap();
    let s = se.0.lock().unwrap();
    assert_eq!(vec![TEST_SQL_COUNT_MV_LOG_ROWS.to_owned()], s.executed_restricted_sql);
    assert_eq!(vec!["test", "mlog1", "test", "mlog1"], s.executed_restricted_args[0]);
    assert_eq!(Some("1".to_owned()), s.restricted_var_values[0][concurrency]);
    assert_eq!(Some("7"), s.system_vars.get(concurrency).map(String::as_str));
    assert_eq!(HashMap::from([(201, 50)]), got);
}

const TEST_SQL_LOCK_PURGE_NEXT_TIME: &str =
    "SELECT NEXT_PURGE_UNIX_SECONDS FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE NOWAIT";
const TEST_SQL_UPDATE_PURGE_NEXT_TIME: &str = "UPDATE mysql.tidb_mlog_purge_info SET NEXT_PURGE_UNIX_SECONDS = %? WHERE MLOG_ID = %?";

/// Go `TestServerHelperTryBackoffPurgeManualCancel` (its subtests in order).
#[test]
fn test_server_helper_try_backoff_purge_manual_cancel() {
    use crate::service::MANUAL_CANCEL_BACKOFF_DELAY;
    let backoff = |pool: &SystemSessionPool,
                   next: &GoTime,
                   resolve: fn(&dyn MVServiceSessionContext, i64) -> Result<(Option<GoTime>, bool), MvsError>| {
        crate::service_helper::try_backoff_mv_task_manual_cancel(
            pool,
            TEST_SQL_LOCK_PURGE_NEXT_TIME,
            TEST_SQL_UPDATE_PURGE_NEXT_TIME,
            201,
            next,
            resolve,
        )
    };
    let lock = |se: &RecordingSessionContext, value: Option<i64>| {
        se.0.lock().unwrap().restricted_rows.insert(TEST_SQL_LOCK_PURGE_NEXT_TIME.to_owned(), vec![ints(&[value])]);
    };
    let check = |se: &RecordingSessionContext, restricted: &[&str], update: Option<Vec<String>>| {
        let s = se.0.lock().unwrap();
        assert_eq!(vec!["BEGIN PESSIMISTIC".to_owned(), "COMMIT".to_owned()], s.executed_sql);
        assert_eq!(restricted.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>(), s.executed_restricted_sql);
        assert_eq!(vec!["201".to_owned()], s.executed_restricted_args[0]);
        if let Some(update) = update {
            assert_eq!(update, s.executed_restricted_args[1]);
        }
    };
    let both = [TEST_SQL_LOCK_PURGE_NEXT_TIME, TEST_SQL_UPDATE_PURGE_NEXT_TIME];

    // "applied"
    let (se, pool) = new_recording_session();
    lock(&se, Some(1));
    let expected_next = base_time().add(MANUAL_CANCEL_BACKOFF_DELAY);
    let (applied, applied_next, err) = backoff(&pool, &expected_next, |_, _| Ok((Some(mvs_unix(1_700_000_000, 0).add(MINUTE)), false)));
    assert!(err.is_none() && applied);
    assert_eq!(expected_next, applied_next);
    check(&se, &both, Some(vec![expected_next.unix().to_string(), "201".to_owned()]));

    // "keep_later_next_time"
    let (se, pool) = new_recording_session();
    lock(&se, Some(1));
    let cooldown_next = base_time().add(MANUAL_CANCEL_BACKOFF_DELAY);
    let current_next = base_time().add(MANUAL_CANCEL_BACKOFF_DELAY + MINUTE);
    let (applied, applied_next, err) = backoff(&pool, &cooldown_next, |_, _| {
        Ok((Some(mvs_unix(1_700_000_000, 0).add(crate::service::MANUAL_CANCEL_BACKOFF_DELAY + MINUTE)), true))
    });
    assert!(err.is_none() && applied);
    assert_eq!(current_next, applied_next);
    check(&se, &both, Some(vec![current_next.unix().to_string(), "201".to_owned()]));

    // "no_next_schedule_clears_next_time"
    let (se, pool) = new_recording_session();
    lock(&se, Some(1));
    let (applied, applied_next, err) = backoff(&pool, &cooldown_next, |_, _| Ok((None, true)));
    assert!(err.is_none() && applied);
    assert!(applied_next.is_zero());
    check(&se, &both, Some(vec!["Null".to_owned(), "201".to_owned()]));

    // "locked_next_time_null_keeps_unscheduled"
    let (se, pool) = new_recording_session();
    lock(&se, None);
    RESOLVER_CALLED.store(false, std::sync::atomic::Ordering::SeqCst);
    let (applied, applied_next, err) = backoff(&pool, &cooldown_next, |_, _| {
        RESOLVER_CALLED.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok((Some(mvs_unix(1_700_000_000, 0).add(tidb_timer::go_time::HOUR)), true))
    });
    assert!(err.is_none() && applied);
    assert!(applied_next.is_zero());
    assert!(!RESOLVER_CALLED.load(std::sync::atomic::Ordering::SeqCst));
    check(&se, &[TEST_SQL_LOCK_PURGE_NEXT_TIME], None);
}

/// The refresh's maintenance variables Go's `recordingSessionContext`
/// samples at each restricted statement.
fn refresh_session_var_names() -> Vec<&'static str> {
    use tidb_vardef::tidb_vars as v;
    vec![
        v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA,
        v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES,
        v::TIDB_MAX_TIFLASH_THREADS,
        v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN,
        v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY,
        v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT,
        v::TIFLASH_MEM_QUOTA_QUERY_PER_NODE,
        v::TIFLASH_QUERY_SPILL_RATIO,
        v::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT,
        v::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE,
        v::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS,
        v::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA,
    ]
}

/// Go `vars.SetSystemVar` for each `(name, value)`.
fn set_session_vars(se: &RecordingSessionContext, vars: &[(&str, &str)]) {
    let mut s = se.0.lock().unwrap();
    for (name, value) in vars {
        s.system_vars.insert((*name).to_owned(), (*value).to_owned());
    }
}

/// The values each restricted statement saw for `name` (Go's
/// `restricted*` slices).
fn sampled(se: &RecordingSessionContext, name: &str) -> Vec<String> {
    se.0.lock().unwrap().restricted_var_values.iter().map(|values| values[name].clone().unwrap_or_default()).collect()
}

fn refresh_mv_ready_session() -> (Arc<RecordingSessionContext>, SystemSessionPool, GoTime) {
    let (se, pool) = new_recording_session();
    let expected_next_refresh = mvs_unix(1_700_000_000, 0).add(MINUTE);
    se.0.lock().unwrap().restricted_rows.insert(
        TEST_SQL_FIND_MV_NEXT_TIME.to_owned(),
        vec![ints(&[Some(expected_next_refresh.unix())])],
    );
    with_mock_info_schema(&se, vec![table(101, "mv1")]);
    se.0.lock().unwrap().watched_vars = refresh_session_var_names().iter().map(|n| (*n).to_owned()).collect();
    (se, pool, expected_next_refresh)
}

#[test]
fn test_server_helper_refresh_mv_uses_global_refresh_session_vars() {
    use tidb_vardef::tidb_vars as v;
    let (se, pool, expected_next_refresh) = refresh_mv_ready_session();
    {
        let mut s = se.0.lock().unwrap();
        for (name, value) in [
            (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "536870912"),
            (v::TIDB_MAX_TIFLASH_THREADS, "8"),
            (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN, "111"),
            (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY, "222"),
            (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT, "333"),
            (v::TIFLASH_MEM_QUOTA_QUERY_PER_NODE, "444"),
            (v::TIFLASH_QUERY_SPILL_RATIO, "0.25"),
            (v::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT, "16"),
            (v::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE, "4096"),
            (v::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS, "12"),
            (v::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA, "64gib"),
            (v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES, "tikv"),
        ] {
            s.global_vars.insert(name.to_owned(), value.to_owned());
        }
    }
    let session = [
        (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "268435456"),
        (v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES, "tidb"),
        (v::TIDB_ISOLATION_READ_ENGINES, "tidb"),
        (v::TIDB_MAX_TIFLASH_THREADS, "2"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN, "101"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY, "202"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT, "303"),
        (v::TIFLASH_MEM_QUOTA_QUERY_PER_NODE, "404"),
        (v::TIFLASH_QUERY_SPILL_RATIO, "0.75"),
        (v::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT, "4"),
        (v::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE, "1024"),
        (v::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS, "3"),
        (v::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA, "8gib"),
    ];
    set_session_vars(&se, &session);

    let next_refresh = service_helper().refresh_mv(&Context::background(), &pool, 101).unwrap();
    assert_eq!(expected_next_refresh.unix(), next_refresh.unix());
    assert_eq!(
        vec![TEST_SQL_REFRESH_MV.to_owned(), TEST_SQL_FIND_MV_NEXT_TIME.to_owned()],
        se.0.lock().unwrap().executed_restricted_sql
    );
    for (name, value) in [
        (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "536870912"),
        (v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES, "tikv"),
        (v::TIDB_MAX_TIFLASH_THREADS, "8"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN, "111"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY, "222"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT, "333"),
        (v::TIFLASH_MEM_QUOTA_QUERY_PER_NODE, "444"),
        (v::TIFLASH_QUERY_SPILL_RATIO, "0.25"),
        (v::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT, "16"),
        (v::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE, "4096"),
        (v::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS, "12"),
        (v::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA, "64gib"),
    ] {
        assert_eq!(vec![value.to_owned(), value.to_owned()], sampled(&se, name), "{name}");
    }

    se.0.lock().unwrap().restricted_errs.insert(TEST_SQL_REFRESH_MV.to_owned(), MvsError::Other("mock refresh failure".to_owned()));
    let err = service_helper().refresh_mv(&Context::background(), &pool, 101).unwrap_err();
    assert_eq!(MvsError::Other("mock refresh failure".to_owned()), err);
    // The session's own values come back.
    let s = se.0.lock().unwrap();
    for (name, value) in session {
        assert_eq!(Some(value), s.system_vars.get(name).map(String::as_str), "{name}");
    }
}

#[test]
fn test_server_helper_refresh_mv_best_effort_when_global_session_vars_unavailable() {
    use tidb_vardef::tidb_vars as v;
    let (se, pool, expected_next_refresh) = refresh_mv_ready_session();
    {
        let mut s = se.0.lock().unwrap();
        s.global_var_errs.insert(v::TIDB_MAX_TIFLASH_THREADS.to_owned(), MvsError::Other("mock global read failure".to_owned()));
        s.global_var_errs.insert(
            v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES.to_owned(),
            MvsError::Other("mock global read failure".to_owned()),
        );
        s.global_var_fail_after.insert(v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES.to_owned(), 1);
        for (name, value) in [
            (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "536870912"),
            (v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES, "tidb"),
            (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN, "111"),
            (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY, "222"),
            (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT, "333"),
            (v::TIFLASH_MEM_QUOTA_QUERY_PER_NODE, "444"),
            (v::TIFLASH_QUERY_SPILL_RATIO, "not-a-float"),
            (v::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT, "16"),
            (v::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE, "4096"),
            (v::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS, "12"),
            (v::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA, "bad-quota"),
        ] {
            s.global_vars.insert(name.to_owned(), value.to_owned());
        }
        // Go's `SetSystemVar` refuses these values.
        s.invalid_system_var_values = vec![
            (v::TIFLASH_QUERY_SPILL_RATIO.to_owned(), "not-a-float".to_owned()),
            (v::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA.to_owned(), "bad-quota".to_owned()),
        ];
    }
    let session = [
        (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "268435456"),
        (v::TIDB_ISOLATION_READ_ENGINES, "tikv"),
        (v::TIDB_MAX_TIFLASH_THREADS, "2"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN, "101"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY, "202"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT, "303"),
        (v::TIFLASH_MEM_QUOTA_QUERY_PER_NODE, "404"),
        (v::TIFLASH_QUERY_SPILL_RATIO, "0.75"),
        (v::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT, "4"),
        (v::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE, "1024"),
        (v::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS, "3"),
        (v::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA, "8gib"),
    ];
    set_session_vars(&se, &session);

    let next_refresh = service_helper().refresh_mv(&Context::background(), &pool, 101).unwrap();
    assert_eq!(expected_next_refresh.unix(), next_refresh.unix());
    assert_eq!(
        vec![TEST_SQL_REFRESH_MV.to_owned(), TEST_SQL_FIND_MV_NEXT_TIME.to_owned()],
        se.0.lock().unwrap().executed_restricted_sql
    );
    for (name, value) in [
        (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "536870912"),
        (v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES, "tidb"),
        (v::TIDB_MAX_TIFLASH_THREADS, "2"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_JOIN, "111"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_GROUP_BY, "222"),
        (v::TIDB_MAX_BYTES_BEFORE_TIFLASH_EXTERNAL_SORT, "333"),
        (v::TIFLASH_MEM_QUOTA_QUERY_PER_NODE, "444"),
        (v::TIFLASH_QUERY_SPILL_RATIO, "0.75"),
        (v::TIFLASH_FINE_GRAINED_SHUFFLE_STREAM_COUNT, "16"),
        (v::TIFLASH_FINE_GRAINED_SHUFFLE_BATCH_SIZE, "4096"),
        (v::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS, "12"),
        (v::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA, "8gib"),
    ] {
        assert_eq!(vec![value.to_owned(), value.to_owned()], sampled(&se, name), "{name}");
    }
    let s = se.0.lock().unwrap();
    for (name, value) in session {
        assert_eq!(Some(value), s.system_vars.get(name).map(String::as_str), "{name}");
    }
}
