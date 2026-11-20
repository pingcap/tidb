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
        let s = self.0.lock().unwrap();
        if let Some(err) = s.global_var_errs.get(name) {
            return Err(err.clone());
        }
        Ok(s.global_vars.get(name).cloned().unwrap_or_default())
    }

    fn system_var(&self, name: &str) -> Option<String> {
        self.0.lock().unwrap().system_vars.get(name).cloned()
    }

    fn set_system_var(&self, name: &str, value: &str) -> Result<(), MvsError> {
        self.0.lock().unwrap().system_vars.insert(name.to_owned(), value.to_owned());
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
