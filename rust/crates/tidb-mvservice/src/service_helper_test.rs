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
    /// Go `StmtCtx.TimeZone()`.
    statement_time_zone: String,
}

/// Go `recordingSessionContext`: one session that is also its own
/// restricted SQL executor.
pub(crate) struct RecordingSessionContext(pub(crate) Arc<Mutex<RecordingSession>>);

/// Go `mock.NewContext()`'s variables: a variable the test did not set
/// holds its default, GLOBAL or session.
fn sys_var_default(name: &str) -> Option<String> {
    tidb_variable::sysvar::get_sys_var(name).map(|definition| definition.value.to_owned())
}

impl RecordingSession {
    /// Go `ExecRestrictedSQL`: records the statement, its arguments and the
    /// watched variables, then answers from the canned rows.
    fn exec_restricted(&mut self, sql: &str, args: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, MvsError> {
        self.executed_restricted_sql.push(sql.to_owned());
        self.executed_restricted_args.push(args.iter().map(arg_text).collect());
        let values = self.watched_vars.iter().map(|name| (name.clone(), self.system_vars.get(name).cloned())).collect();
        self.restricted_var_values.push(values);
        let affected = match self.restricted_affected_rows.get(sql).cloned() {
            Some(seq) => {
                let pos = self.restricted_affected_pos.entry(sql.to_owned()).or_default();
                let value = seq.get(*pos).copied().unwrap_or(0);
                *pos += 1;
                value
            }
            None => 0,
        };
        self.affected_rows = affected;
        if let Some(err) = self.restricted_errs.get(sql) {
            return Err(err.clone());
        }
        Ok(self.restricted_rows.get(sql).cloned().unwrap_or_default())
    }

    /// Go `faultyGlobalAccessor.GetGlobalSysVar` over
    /// `variable.NewMockGlobalAccessor4Tests()`.
    fn global_system_var(&mut self, name: &str) -> Result<String, MvsError> {
        let calls = {
            let calls = self.global_var_get_calls.entry(name.to_owned()).or_default();
            *calls += 1;
            *calls
        };
        if let Some(&ok_after) = self.global_var_fail_after.get(name) {
            if calls > ok_after {
                return Err(self
                    .global_var_errs
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| MvsError::Other("mock global read failure".to_owned())));
            }
        } else if let Some(err) = self.global_var_errs.get(name) {
            return Err(err.clone());
        }
        Ok(self.global_vars.get(name).cloned().or_else(|| sys_var_default(name)).unwrap_or_default())
    }

    fn system_var(&self, name: &str) -> Option<String> {
        self.system_vars.get(name).cloned().or_else(|| sys_var_default(name))
    }

    fn set_system_var(&mut self, name: &str, value: &str) -> Result<(), MvsError> {
        if self.invalid_system_var_values.iter().any(|(n, v)| n == name && v == value) {
            return Err(MvsError::Other(format!("invalid value {value} for {name}")));
        }
        self.system_vars.insert(name.to_owned(), value.to_owned());
        Ok(())
    }
}

fn arg_text(arg: &SqlArg<'_>) -> String {
    match arg {
        SqlArg::String(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        SqlArg::Signed(v) => v.to_string(),
        SqlArg::Unsigned(v) => v.to_string(),
        other => format!("{other:?}"),
    }
}

impl MVServiceSessionContext for RecordingSessionContext {
    fn stats_session(&self) -> &dyn crate::service_helper::MVServiceStatsSession {
        self
    }

    fn exec_restricted_sql(&self, sql: &str, args: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, MvsError> {
        self.0.lock().unwrap().exec_restricted(sql, args)
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
        self.0.lock().unwrap().global_system_var(name)
    }

    fn system_var(&self, name: &str) -> Option<String> {
        self.0.lock().unwrap().system_var(name)
    }

    fn set_system_var(&self, name: &str, value: &str) -> Result<(), MvsError> {
        self.0.lock().unwrap().set_system_var(name, value)
    }

    fn store_current_version(&self) -> Result<u64, MvsError> {
        Ok(self.0.lock().unwrap().store_version)
    }

    fn statement_now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

/// The recording session as its own restricted SQL executor (Go
/// `GetRestrictedSQLExecutor` returns the session).
struct RecordingExecutor(Arc<Mutex<RecordingSession>>);

fn exec_error(err: MvsError) -> tidb_sqlexec::SqlExecError {
    err.to_string().into()
}

impl tidb_sqlexec::RestrictedSqlExecutor for RecordingExecutor {
    fn parse_with_params(
        &self,
        _: &dyn tidb_sqlexec::ExecutionContext,
        _: &str,
        _: &[SqlArg<'_>],
    ) -> tidb_sqlexec::Result<tidb_ast::Stmt> {
        Err("parse is not recorded".into())
    }

    fn exec_restricted_stmt(
        &self,
        _: &dyn tidb_sqlexec::ExecutionContext,
        _: &tidb_ast::Stmt,
        _: &[tidb_sqlexec::OptionFuncAlias],
    ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
        Err("statements are not recorded".into())
    }

    fn exec_restricted_sql(
        &self,
        _: &dyn tidb_sqlexec::ExecutionContext,
        _: &[tidb_sqlexec::OptionFuncAlias],
        sql: &str,
        args: &[SqlArg<'_>],
    ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
        self.0.lock().unwrap().exec_restricted(sql, args).map(|rows| (rows, Vec::new())).map_err(exec_error)
    }
}

impl tidb_sqlexec::SqlExecutor for RecordingExecutor {
    fn execute(
        &self,
        _: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
    ) -> tidb_sqlexec::Result<Vec<Box<dyn tidb_sqlexec::RecordSet>>> {
        self.0.lock().unwrap().executed_sql.push(sql.to_owned());
        Ok(Vec::new())
    }

    fn execute_internal(
        &self,
        _: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        _: &[SqlArg<'_>],
    ) -> tidb_sqlexec::Result<Option<Box<dyn tidb_sqlexec::RecordSet>>> {
        let mut s = self.0.lock().unwrap();
        s.executed_sql.push(sql.to_owned());
        s.exec_errs.get(sql).cloned().map_or(Ok(None), |err| Err(exec_error(err)))
    }

    fn execute_stmt(
        &self,
        _: &dyn tidb_sqlexec::ExecutionContext,
        _: &tidb_ast::Stmt,
    ) -> tidb_sqlexec::Result<Option<Box<dyn tidb_sqlexec::RecordSet>>> {
        Err("statements are not recorded".into())
    }
}

impl tidb_syssession::SessionContext for RecordingSessionContext {
    fn close(&self) {}

    fn rollback_txn(&self, _: &dyn tidb_sqlexec::ExecutionContext) {}

    fn has_prepared_txn_future(&self) -> bool {
        false
    }

    fn txn_valid(&self) -> Result<bool, tidb_sqlexec::SqlExecError> {
        Ok(false)
    }

    fn sql_executor(&self) -> Arc<dyn tidb_sqlexec::SqlExecutor> {
        Arc::new(RecordingExecutor(Arc::clone(&self.0)))
    }

    fn restricted_sql_executor(&self) -> Arc<dyn tidb_sqlexec::RestrictedSqlExecutor> {
        Arc::new(RecordingExecutor(Arc::clone(&self.0)))
    }

    fn register_internal_session(&self) {}

    fn unregister_internal_session(&self) {}

    fn contains_internal_session(&self) -> bool {
        false
    }

    fn store_internal_session(&self) -> bool {
        true
    }
}

const ON_OFF: [&str; 2] = ["OFF", "ON"];

impl RecordingSessionContext {
    fn stats_var(&self, name: &str) -> String {
        self.0.lock().unwrap().system_var(name).unwrap_or_default()
    }

    fn set_stats_var(&self, name: &str, value: impl Into<String>) {
        self.0.lock().unwrap().system_vars.insert(name.to_owned(), value.into());
    }
}

/// Go `GetSessionVars()` as the statistics packages read and write it, over
/// the recorded session variables.
impl tidb_stats_handle_util::StatsSessionContext for RecordingSessionContext {
    fn global_system_var(&self, name: &str) -> Result<String, tidb_sqlexec::SqlExecError> {
        self.0.lock().unwrap().global_system_var(name).map_err(exec_error)
    }

    fn set_analyze_partition_concurrency(&self, concurrency: i64) {
        self.set_stats_var(tidb_vardef::tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY, concurrency.to_string());
    }

    fn set_analyze_version(&self, version: i64) {
        self.set_stats_var(tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION, version.to_string());
    }

    fn set_analyze_store_batch_size(&self, value: &str) -> Result<(), tidb_sqlexec::SqlExecError> {
        self.set_stats_var(tidb_vardef::tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE, value);
        Ok(())
    }

    fn set_partition_prune_mode(&self, mode: &str) {
        self.set_stats_var(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE, mode);
    }

    fn partition_prune_mode(&self) -> String {
        self.stats_var(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
    }

    fn set_enable_analyze_snapshot(&self, enabled: bool) {
        self.set_stats_var(tidb_vardef::tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT, ON_OFF[usize::from(enabled)]);
    }

    fn set_analyze_skip_column_types(&self, value: std::collections::BTreeSet<String>) {
        self.set_stats_var(
            tidb_vardef::tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES,
            value.into_iter().collect::<Vec<_>>().join(","),
        );
    }

    fn set_skip_missing_partition_stats(&self, enabled: bool) {
        self.set_stats_var(tidb_vardef::tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS, ON_OFF[usize::from(enabled)]);
    }

    fn set_lock_wait_timeout(&self, milliseconds: i64) {
        self.set_stats_var("innodb_lock_wait_timeout", (milliseconds / 1_000).to_string());
    }

    fn set_time_zone(&self, value: &str) -> Result<(), tidb_sqlexec::SqlExecError> {
        self.set_stats_var("time_zone", value);
        Ok(())
    }

    fn location(&self) -> String {
        self.stats_var("time_zone")
    }

    fn set_statement_time_zone(&self, value: &str) {
        self.0.lock().unwrap().statement_time_zone = value.to_owned();
    }

    fn statement_time_zone(&self) -> String {
        self.0.lock().unwrap().statement_time_zone.clone()
    }

    fn system_var(&self, name: &str) -> Option<String> {
        self.0.lock().unwrap().system_var(name)
    }

    fn analyze_skip_column_types(&self) -> std::collections::BTreeSet<String> {
        tidb_stats_handle_util::parse_analyze_skip_column_types(
            &self.stats_var(tidb_vardef::tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES),
        )
    }

    fn enable_analyze_snapshot(&self) -> bool {
        tidb_vardef::option_values::tidb_opt_on(&self.stats_var(tidb_vardef::tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT))
    }

    fn skip_missing_partition_stats(&self) -> bool {
        tidb_vardef::option_values::tidb_opt_on(&self.stats_var(tidb_vardef::tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS))
    }

    fn analyze_partition_concurrency(&self) -> i64 {
        self.stats_var(tidb_vardef::tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY).parse().unwrap_or_default()
    }

    fn analyze_version(&self) -> i64 {
        self.stats_var(tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION).parse().unwrap_or_default()
    }

    fn analyze_store_batch_size(&self) -> i64 {
        self.stats_var(tidb_vardef::tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE).parse().unwrap_or_default()
    }

    fn lock_wait_timeout(&self) -> i64 {
        self.stats_var("innodb_lock_wait_timeout").parse::<i64>().unwrap_or_default() * 1_000
    }

    fn transaction_start_ts(&self, _active: bool) -> Result<u64, tidb_sqlexec::SqlExecError> {
        Ok(0)
    }

    fn current_ts(&self) -> Result<u64, tidb_sqlexec::SqlExecError> {
        Ok(self.0.lock().unwrap().store_version)
    }

    fn affected_rows(&self) -> u64 {
        self.0.lock().unwrap().affected_rows
    }

    fn latest_table_meta_by_id(&self, _table_id: i64) -> Option<tidb_model::GoShared<TableInfo>> {
        None
    }
}

impl tidb_stats_handle_autoanalyze_exec::AutoAnalyzeSessionContext for RecordingSessionContext {
    fn partition_prune_mode(&self) -> String {
        tidb_stats_handle_util::StatsSessionContext::partition_prune_mode(self)
    }

    fn enable_analyze_snapshot(&self) -> bool {
        tidb_stats_handle_util::StatsSessionContext::enable_analyze_snapshot(self)
    }
}

/// Go `recordingSessionPool`: the one session, `onGet` called first.
pub(crate) struct RecordingSessionPool(pub(crate) Arc<RecordingSessionContext>, pub(crate) Option<Box<dyn Fn() + Send + Sync>>);

impl MVServiceSessionPool for RecordingSessionPool {
    fn with_session(&self, f: &mut dyn FnMut(&dyn MVServiceSessionContext) -> Result<(), MvsError>) -> Result<(), MvsError> {
        if let Some(on_get) = &self.1 {
            on_get();
        }
        f(self.0.as_ref())
    }
}

/// `newRecordingSessionContext()` and its pool, as `SessionPool`.
fn new_recording_session() -> (Arc<RecordingSessionContext>, SystemSessionPool) {
    let se = Arc::new(RecordingSessionContext(Arc::new(Mutex::new(RecordingSession {
        // Go `mock.NewContext()`'s statement context zone.
        statement_time_zone: "UTC".to_owned(),
        ..RecordingSession::default()
    }))));
    (Arc::clone(&se), SystemSessionPool(Arc::new(RecordingSessionPool(Arc::clone(&se), None))))
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

/// Go `config.GetGlobalConfig().IsolationRead.Engines` joined: the default
/// of `tidb_mview_maintain_isolation_read_engines` a fresh Go session holds.
const DEFAULT_ISOLATION_READ_ENGINES: &str = "tikv,tiflash,tidb";

/// The purge's maintenance variables Go's `recordingSessionContext`
/// samples at each restricted statement.
fn purge_session_var_names() -> Vec<&'static str> {
    use tidb_vardef::tidb_vars as v;
    vec![
        v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA,
        v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES,
        v::TIDB_MLOG_PURGE_BATCH_SIZE,
        v::TIDB_MLOG_PURGE_MIN_RATE,
        v::TIDB_MLOG_PURGE_RATE_BUDGET_RATIO,
        v::TIDB_MLOG_PURGE_DELETE_TIFLASH_THREADS,
    ]
}

fn purge_mv_log_ready_session() -> (Arc<RecordingSessionContext>, SystemSessionPool) {
    let (se, pool) = new_recording_session();
    setup_purge_mv_log_meta_for_test(&se, vec![ints(&[Some(mvs_unix(1_700_000_000, 0).add(MINUTE).unix())])]);
    {
        let mut s = se.0.lock().unwrap();
        s.watched_vars = purge_session_var_names().iter().map(|n| (*n).to_owned()).collect();
        s.system_vars.insert(
            tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES.to_owned(),
            DEFAULT_ISOLATION_READ_ENGINES.to_owned(),
        );
    }
    (se, pool)
}

#[test]
fn test_server_helper_purge_mv_log_uses_global_maintain_mem_quota() {
    use tidb_vardef::tidb_vars as v;
    let (se, pool) = purge_mv_log_ready_session();
    {
        let mut s = se.0.lock().unwrap();
        for (name, value) in [
            (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "536870912"),
            (v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES, "tikv"),
            (v::TIDB_MLOG_PURGE_BATCH_SIZE, "4321"),
            (v::TIDB_MLOG_PURGE_MIN_RATE, "8765"),
            (v::TIDB_MLOG_PURGE_RATE_BUDGET_RATIO, "0.75"),
            (v::TIDB_MLOG_PURGE_DELETE_TIFLASH_THREADS, "3"),
        ] {
            s.global_vars.insert(name.to_owned(), value.to_owned());
        }
    }
    let session = [
        (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "268435456"),
        (v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES, "tidb"),
        (v::TIDB_ISOLATION_READ_ENGINES, "tidb"),
        (v::TIDB_MLOG_PURGE_BATCH_SIZE, "1234"),
        (v::TIDB_MLOG_PURGE_MIN_RATE, "2345"),
        (v::TIDB_MLOG_PURGE_RATE_BUDGET_RATIO, "0.25"),
        (v::TIDB_MLOG_PURGE_DELETE_TIFLASH_THREADS, "5"),
    ];
    set_session_vars(&se, &session);

    let next_purge = service_helper().purge_mv_log(&Context::background(), &pool, 201).unwrap();
    assert!(!next_purge.is_zero());
    require_expected_purge_mv_log_sql(&se);
    for (name, value) in [
        (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "536870912"),
        (v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES, "tikv"),
        (v::TIDB_MLOG_PURGE_BATCH_SIZE, "4321"),
        (v::TIDB_MLOG_PURGE_MIN_RATE, "8765"),
        (v::TIDB_MLOG_PURGE_RATE_BUDGET_RATIO, "0.75"),
        (v::TIDB_MLOG_PURGE_DELETE_TIFLASH_THREADS, "3"),
    ] {
        assert_eq!(vec![value.to_owned(), value.to_owned()], sampled(&se, name), "{name}");
    }

    se.0.lock().unwrap().restricted_errs.insert(TEST_SQL_PURGE_MV_LOG.to_owned(), MvsError::Other("mock purge failure".to_owned()));
    let err = service_helper().purge_mv_log(&Context::background(), &pool, 201).unwrap_err();
    assert_eq!(MvsError::Other("mock purge failure".to_owned()), err);
    let s = se.0.lock().unwrap();
    for (name, value) in session {
        assert_eq!(Some(value), s.system_vars.get(name).map(String::as_str), "{name}");
    }
}

#[test]
fn test_server_helper_purge_mv_log_best_effort_when_global_maintain_mem_quota_unavailable() {
    use tidb_vardef::tidb_vars as v;
    let (se, pool) = purge_mv_log_ready_session();
    {
        let mut s = se.0.lock().unwrap();
        for name in purge_session_var_names() {
            s.global_var_errs.insert(name.to_owned(), MvsError::Other("mock global read failure".to_owned()));
        }
        s.global_var_fail_after.insert(v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES.to_owned(), 1);
        s.global_vars.insert(v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES.to_owned(), "tidb".to_owned());
    }
    let session = [
        (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "268435456"),
        (v::TIDB_ISOLATION_READ_ENGINES, "tikv"),
        (v::TIDB_MLOG_PURGE_BATCH_SIZE, "3456"),
        (v::TIDB_MLOG_PURGE_MIN_RATE, "4567"),
        (v::TIDB_MLOG_PURGE_RATE_BUDGET_RATIO, "0.35"),
        (v::TIDB_MLOG_PURGE_DELETE_TIFLASH_THREADS, "7"),
    ];
    set_session_vars(&se, &session);

    let next_purge = service_helper().purge_mv_log(&Context::background(), &pool, 201).unwrap();
    assert!(!next_purge.is_zero());
    require_expected_purge_mv_log_sql(&se);
    for (name, value) in [
        (v::TIDB_MVIEW_MAINTAIN_MEM_QUOTA, "268435456"),
        (v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES, "tidb"),
        (v::TIDB_MLOG_PURGE_BATCH_SIZE, "3456"),
        (v::TIDB_MLOG_PURGE_MIN_RATE, "4567"),
        (v::TIDB_MLOG_PURGE_RATE_BUDGET_RATIO, "0.35"),
        (v::TIDB_MLOG_PURGE_DELETE_TIFLASH_THREADS, "7"),
    ] {
        assert_eq!(vec![value.to_owned(), value.to_owned()], sampled(&se, name), "{name}");
    }
    let s = se.0.lock().unwrap();
    for (name, value) in session {
        assert_eq!(Some(value), s.system_vars.get(name).map(String::as_str), "{name}");
    }
    assert_eq!(
        Some(DEFAULT_ISOLATION_READ_ENGINES),
        s.system_vars.get(v::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES).map(String::as_str)
    );
}

use crate::service::DEFAULT_MV_HISTORY_GC_MAX_RECORDS;
use crate::service_helper::{
    build_delete_log_purge_history_by_count_limit_sql, build_delete_log_purge_history_by_cutoff_tso_sql,
    build_delete_refresh_history_by_count_limit_sql, build_delete_refresh_history_by_cutoff_tso_sql,
    build_mark_log_purge_history_running_rows_orphaned_sql, build_mark_refresh_history_running_rows_orphaned_sql,
    COUNT_LOG_PURGE_HISTORY_SQL, COUNT_REFRESH_HISTORY_SQL, HISTORY_GC_DELETE_BATCH_SIZE,
};

/// Go's `COUNT(*), MIN(job id)` row of the history count query.
fn count_cap_row(count: u64, min_job_id: u64) -> Vec<Datum> {
    vec![Datum::Int(count as i64), Datum::UInt(min_job_id)]
}

#[derive(Default)]
struct PurgeMVHistoryCase {
    name: &'static str,
    current_tso: u64,
    mview_retention: i64,
    mlog_retention: i64,
    refresh_time_affected_rows: Vec<u64>,
    purge_time_affected_rows: Vec<u64>,
    refresh_count_rows: Option<Vec<Vec<Datum>>>,
    purge_count_rows: Option<Vec<Vec<Datum>>>,
    refresh_time_err: Option<MvsError>,
    expect_refresh_exec: usize,
    expect_purge_exec: usize,
    expect_refresh_count_cap: Vec<u64>,
    expect_purge_count_cap: Vec<u64>,
    refresh_count_affected_rows: Vec<u64>,
    purge_count_affected_rows: Vec<u64>,
    assert_args_are_equal_tso: bool,
    assert_args_not_equal: bool,
    expect_err_contains: Vec<&'static str>,
}

#[test]
fn test_server_helper_purge_mv_history_before_tso() {
    const HOUR: i64 = tidb_timer::go_time::HOUR;
    let build_expected_sql = |refresh_exec: usize, purge_exec: usize, refresh_caps: &[u64], purge_caps: &[u64]| {
        let mut sqls = vec![build_mark_refresh_history_running_rows_orphaned_sql()];
        sqls.extend(std::iter::repeat_n(build_delete_refresh_history_by_cutoff_tso_sql(), refresh_exec));
        sqls.push(COUNT_REFRESH_HISTORY_SQL.to_owned());
        sqls.extend(refresh_caps.iter().map(|&limit| build_delete_refresh_history_by_count_limit_sql(limit)));
        sqls.push(build_mark_log_purge_history_running_rows_orphaned_sql());
        sqls.extend(std::iter::repeat_n(build_delete_log_purge_history_by_cutoff_tso_sql(), purge_exec));
        sqls.push(COUNT_LOG_PURGE_HISTORY_SQL.to_owned());
        sqls.extend(purge_caps.iter().map(|&limit| build_delete_log_purge_history_by_count_limit_sql(limit)));
        sqls
    };
    let cases = vec![
        PurgeMVHistoryCase {
            name: "zero_retention",
            current_tso: 987_654_321,
            expect_refresh_exec: 1,
            expect_purge_exec: 1,
            assert_args_are_equal_tso: true,
            ..Default::default()
        },
        PurgeMVHistoryCase {
            name: "separate_retention",
            current_tso: ((10 * HOUR / 1_000_000) as u64) << 18,
            mview_retention: 2 * HOUR,
            mlog_retention: 4 * HOUR,
            expect_refresh_exec: 1,
            expect_purge_exec: 1,
            assert_args_not_equal: true,
            ..Default::default()
        },
        PurgeMVHistoryCase {
            name: "batch_delete",
            current_tso: 987_654_321,
            refresh_time_affected_rows: vec![HISTORY_GC_DELETE_BATCH_SIZE, HISTORY_GC_DELETE_BATCH_SIZE, 1],
            purge_time_affected_rows: vec![0],
            expect_refresh_exec: 3,
            expect_purge_exec: 1,
            ..Default::default()
        },
        PurgeMVHistoryCase {
            name: "batch_delete_no_max_batches",
            current_tso: 987_654_321,
            expect_refresh_exec: 26,
            expect_purge_exec: 1,
            ..Default::default()
        },
        PurgeMVHistoryCase {
            name: "count_cap_delete_budget",
            current_tso: 987_654_321,
            refresh_count_rows: Some(vec![count_cap_row(
                DEFAULT_MV_HISTORY_GC_MAX_RECORDS + HISTORY_GC_DELETE_BATCH_SIZE + 5,
                500,
            )]),
            purge_count_rows: Some(vec![count_cap_row(DEFAULT_MV_HISTORY_GC_MAX_RECORDS + 8, 900)]),
            refresh_time_affected_rows: vec![0],
            purge_time_affected_rows: vec![0],
            expect_refresh_exec: 1,
            expect_purge_exec: 1,
            expect_refresh_count_cap: vec![HISTORY_GC_DELETE_BATCH_SIZE, 5],
            expect_purge_count_cap: vec![8],
            refresh_count_affected_rows: vec![HISTORY_GC_DELETE_BATCH_SIZE, 5],
            purge_count_affected_rows: vec![8],
            ..Default::default()
        },
        PurgeMVHistoryCase {
            name: "count_cap_under_limit",
            current_tso: 987_654_321,
            refresh_time_affected_rows: vec![0],
            purge_time_affected_rows: vec![0],
            refresh_count_rows: Some(vec![count_cap_row(DEFAULT_MV_HISTORY_GC_MAX_RECORDS, 500)]),
            purge_count_rows: Some(vec![count_cap_row(DEFAULT_MV_HISTORY_GC_MAX_RECORDS - 1, 900)]),
            expect_refresh_exec: 1,
            expect_purge_exec: 1,
            ..Default::default()
        },
        PurgeMVHistoryCase {
            name: "continue_after_refresh_retention_error",
            current_tso: 987_654_321,
            refresh_time_err: Some(MvsError::Other("refresh retention gc failed".to_owned())),
            purge_time_affected_rows: vec![0],
            expect_refresh_exec: 1,
            expect_purge_exec: 1,
            expect_err_contains: vec!["purge mview refresh history by retention failed", "refresh retention gc failed"],
            ..Default::default()
        },
    ];

    for tc in cases {
        let (se, pool) = new_recording_session();
        {
            let mut s = se.0.lock().unwrap();
            s.restricted_affected_rows.insert(build_mark_refresh_history_running_rows_orphaned_sql(), vec![0]);
            s.restricted_affected_rows.insert(build_mark_log_purge_history_running_rows_orphaned_sql(), vec![0]);
            if tc.name == "batch_delete_no_max_batches" {
                const EXTRA_BATCHES: usize = 25;
                let mut refresh_affected = vec![HISTORY_GC_DELETE_BATCH_SIZE; EXTRA_BATCHES];
                refresh_affected.push(1);
                s.restricted_affected_rows.insert(build_delete_refresh_history_by_cutoff_tso_sql(), refresh_affected);
                s.restricted_affected_rows.insert(build_delete_log_purge_history_by_cutoff_tso_sql(), vec![0]);
            } else {
                if !tc.refresh_time_affected_rows.is_empty() {
                    s.restricted_affected_rows
                        .insert(build_delete_refresh_history_by_cutoff_tso_sql(), tc.refresh_time_affected_rows.clone());
                }
                if !tc.purge_time_affected_rows.is_empty() {
                    s.restricted_affected_rows
                        .insert(build_delete_log_purge_history_by_cutoff_tso_sql(), tc.purge_time_affected_rows.clone());
                }
            }
            for (i, &limit) in tc.expect_refresh_count_cap.iter().enumerate() {
                s.restricted_affected_rows
                    .insert(build_delete_refresh_history_by_count_limit_sql(limit), vec![tc.refresh_count_affected_rows[i]]);
            }
            for (i, &limit) in tc.expect_purge_count_cap.iter().enumerate() {
                s.restricted_affected_rows
                    .insert(build_delete_log_purge_history_by_count_limit_sql(limit), vec![tc.purge_count_affected_rows[i]]);
            }
            if let Some(rows) = &tc.refresh_count_rows {
                s.restricted_rows.insert(COUNT_REFRESH_HISTORY_SQL.to_owned(), rows.clone());
            }
            if let Some(rows) = &tc.purge_count_rows {
                s.restricted_rows.insert(COUNT_LOG_PURGE_HISTORY_SQL.to_owned(), rows.clone());
            }
            if let Some(err) = &tc.refresh_time_err {
                s.restricted_errs.insert(build_delete_refresh_history_by_cutoff_tso_sql(), err.clone());
            }
        }

        let result = service_helper().purge_mv_history_before_tso(
            &Context::background(),
            &pool,
            tc.current_tso,
            tc.mview_retention,
            tc.mlog_retention,
        );
        if tc.expect_err_contains.is_empty() {
            assert!(result.is_ok(), "{}: {result:?}", tc.name);
        } else {
            let err = result.unwrap_err().to_string();
            for msg in &tc.expect_err_contains {
                assert!(err.contains(msg), "{}: {err}", tc.name);
            }
        }
        let s = se.0.lock().unwrap();
        assert_eq!(
            build_expected_sql(
                tc.expect_refresh_exec,
                tc.expect_purge_exec,
                &tc.expect_refresh_count_cap,
                &tc.expect_purge_count_cap
            ),
            s.executed_restricted_sql,
            "{}",
            tc.name
        );
        let args = &s.executed_restricted_args;
        assert_eq!(args.len(), s.executed_restricted_sql.len());

        let refresh_retention_arg_pos = 1;
        let purge_orphan_arg_pos = tc.expect_refresh_exec + tc.expect_refresh_count_cap.len() + 2;
        let purge_time_arg_pos = purge_orphan_arg_pos + 1;
        if tc.assert_args_are_equal_tso {
            assert_eq!(1, args[0].len());
            assert_eq!(1, args[purge_orphan_arg_pos].len());
            assert_eq!(args[0][0], args[purge_orphan_arg_pos][0]);
            assert_eq!(vec![tc.current_tso.to_string()], args[refresh_retention_arg_pos]);
            assert_eq!(vec![tc.current_tso.to_string()], args[purge_time_arg_pos]);
        }
        if tc.assert_args_not_equal {
            assert_ne!(args[refresh_retention_arg_pos][0], args[purge_time_arg_pos][0]);
        }
        assert!(args[tc.expect_refresh_exec + 1].is_empty());
        let arg_base = tc.expect_refresh_exec + 2;
        if let Some(rows) = &tc.refresh_count_rows {
            let Datum::UInt(min_job_id) = rows[0][1] else { unreachable!() };
            for i in 0..tc.expect_refresh_count_cap.len() {
                assert_eq!(vec![min_job_id.to_string()], args[arg_base + i]);
            }
        }
        let count_pos = purge_time_arg_pos + tc.expect_purge_exec;
        assert!(args[count_pos].is_empty());
        if let Some(rows) = &tc.purge_count_rows {
            let Datum::UInt(min_job_id) = rows[0][1] else { unreachable!() };
            for i in 0..tc.expect_purge_count_cap.len() {
                assert_eq!(vec![min_job_id.to_string()], args[count_pos + 1 + i]);
            }
        }
    }
}

#[test]
fn test_server_helper_purge_mv_history_before_tso_treats_null_status_as_deletable() {
    let (se, pool) = new_recording_session();
    {
        let mut s = se.0.lock().unwrap();
        s.restricted_affected_rows.insert(build_delete_refresh_history_by_cutoff_tso_sql(), vec![0]);
        s.restricted_affected_rows.insert(build_delete_log_purge_history_by_cutoff_tso_sql(), vec![0]);
        s.restricted_rows.insert(COUNT_REFRESH_HISTORY_SQL.to_owned(), vec![count_cap_row(DEFAULT_MV_HISTORY_GC_MAX_RECORDS, 500)]);
        s.restricted_rows.insert(COUNT_LOG_PURGE_HISTORY_SQL.to_owned(), vec![count_cap_row(DEFAULT_MV_HISTORY_GC_MAX_RECORDS, 900)]);
    }
    service_helper().purge_mv_history_before_tso(&Context::background(), &pool, 987_654_321, 0, 0).unwrap();
    let s = se.0.lock().unwrap();
    assert!(s.executed_restricted_sql.contains(&build_delete_refresh_history_by_cutoff_tso_sql()));
    assert!(s.executed_restricted_sql.contains(&build_delete_log_purge_history_by_cutoff_tso_sql()));
    assert!(build_delete_refresh_history_by_cutoff_tso_sql().contains("REFRESH_STATUS IS NULL OR REFRESH_STATUS <> 'running'"));
    assert!(build_delete_log_purge_history_by_cutoff_tso_sql().contains("PURGE_STATUS IS NULL OR PURGE_STATUS <> 'running'"));
    assert!(build_delete_refresh_history_by_count_limit_sql(1).contains("REFRESH_STATUS IS NULL OR REFRESH_STATUS <> 'running'"));
    assert!(build_delete_log_purge_history_by_count_limit_sql(1).contains("PURGE_STATUS IS NULL OR PURGE_STATUS <> 'running'"));
}

#[test]
fn test_server_helper_purge_mv_history_before_tso_reconciles_stale_running_hist_rows() {
    const HOUR: i64 = tidb_timer::go_time::HOUR;
    let current_tso = ((36 * HOUR / 1_000_000) as u64) << 18;
    // Go `oracle.GetTimeFromTS(oracle.ComposeTS(12h in ms, 0))`.
    let expected_cutoff_time = arg_text(&crate::service_helper::time_arg(&mvs_unix_milli(12 * HOUR / 1_000_000)));

    let (se, pool) = new_recording_session();
    {
        let mut s = se.0.lock().unwrap();
        s.restricted_affected_rows.insert(build_mark_refresh_history_running_rows_orphaned_sql(), vec![2]);
        s.restricted_affected_rows.insert(build_mark_log_purge_history_running_rows_orphaned_sql(), vec![1]);
        s.restricted_affected_rows.insert(build_delete_refresh_history_by_cutoff_tso_sql(), vec![0]);
        s.restricted_affected_rows.insert(build_delete_log_purge_history_by_cutoff_tso_sql(), vec![0]);
        s.restricted_rows.insert(COUNT_REFRESH_HISTORY_SQL.to_owned(), vec![count_cap_row(DEFAULT_MV_HISTORY_GC_MAX_RECORDS, 500)]);
        s.restricted_rows.insert(COUNT_LOG_PURGE_HISTORY_SQL.to_owned(), vec![count_cap_row(DEFAULT_MV_HISTORY_GC_MAX_RECORDS, 900)]);
    }
    service_helper().purge_mv_history_before_tso(&Context::background(), &pool, current_tso, 0, 0).unwrap();
    let s = se.0.lock().unwrap();
    assert_eq!(
        vec![
            build_mark_refresh_history_running_rows_orphaned_sql(),
            build_delete_refresh_history_by_cutoff_tso_sql(),
            COUNT_REFRESH_HISTORY_SQL.to_owned(),
            build_mark_log_purge_history_running_rows_orphaned_sql(),
            build_delete_log_purge_history_by_cutoff_tso_sql(),
            COUNT_LOG_PURGE_HISTORY_SQL.to_owned(),
        ],
        s.executed_restricted_sql
    );
    assert_eq!(vec![expected_cutoff_time.clone()], s.executed_restricted_args[0]);
    assert_eq!(vec![current_tso.to_string()], s.executed_restricted_args[1]);
    assert_eq!(vec![expected_cutoff_time], s.executed_restricted_args[3]);
    assert_eq!(vec![current_tso.to_string()], s.executed_restricted_args[4]);
}

#[test]
fn test_server_helper_purge_mv_history_before_tso_uses_status_index_hints() {
    assert!(build_mark_refresh_history_running_rows_orphaned_sql().contains("FORCE INDEX (idx_refresh_status_start_time)"));
    assert!(build_mark_log_purge_history_running_rows_orphaned_sql().contains("FORCE INDEX (idx_purge_status_start_time)"));
}

#[test]
fn test_server_helper_sync_mv_refresh_alert_states() {
    use crate::service::{RefreshAlertTask, MV_REFRESH_ALERT_LEVEL_OVERDUE, MV_REFRESH_ALERT_LEVEL_WARNING};
    use crate::service_helper::{
        build_clear_resolved_mv_refresh_alert_level_sql, build_delete_resolved_mv_refresh_alert_sql,
        build_upsert_mv_refresh_alert_sql,
    };
    let now = mvs_unix(1_700_000_000, 0);
    let (se, pool) = new_recording_session();
    let states = vec![
        RefreshAlertTask {
            mview_id: 101,
            schema_name: "test".to_owned(),
            mview_name: "mv_warn".to_owned(),
            last_success_time: now.add(-MINUTE),
            alert_level: MV_REFRESH_ALERT_LEVEL_WARNING.to_owned(),
            ..Default::default()
        },
        RefreshAlertTask {
            mview_id: 102,
            schema_name: "test".to_owned(),
            mview_name: "mv_overdue".to_owned(),
            last_success_time: now.add(-2 * MINUTE),
            alert_level: MV_REFRESH_ALERT_LEVEL_OVERDUE.to_owned(),
            ..Default::default()
        },
        RefreshAlertTask { mview_id: 103, ..Default::default() },
        RefreshAlertTask { mview_id: 104, metadata_unresolved: true, ..Default::default() },
        RefreshAlertTask { mview_id: 0, ..Default::default() },
        RefreshAlertTask {
            mview_id: 105,
            schema_name: "test".to_owned(),
            mview_name: "mv_no_success".to_owned(),
            alert_level: MV_REFRESH_ALERT_LEVEL_WARNING.to_owned(),
            ..Default::default()
        },
    ];

    service_helper().sync_mv_refresh_alert_states(&Context::background(), &pool, &now, &states).unwrap();
    let upsert_sql =
        build_upsert_mv_refresh_alert_sql(&now, &[states[0].clone(), states[1].clone(), states[5].clone()]);
    let s = se.0.lock().unwrap();
    assert_eq!(
        vec![
            build_delete_resolved_mv_refresh_alert_sql(&[103]),
            build_clear_resolved_mv_refresh_alert_level_sql(&now, &[103]),
            upsert_sql.clone(),
        ],
        s.executed_restricted_sql
    );
    assert_eq!(3, s.executed_restricted_args.len());
    assert!(upsert_sql.contains("'mv_no_success', 'warning', NULL,"), "{upsert_sql}");
    assert!(s.executed_restricted_args.iter().all(Vec::is_empty));
}

#[test]
fn test_server_helper_cleanup_stale_mv_refresh_alerts() {
    use crate::service_helper::{build_cleanup_stale_mv_refresh_alert_sql, build_clear_disabled_mv_refresh_alert_level_sql};
    let (se, pool) = new_recording_session();
    service_helper().cleanup_stale_mv_refresh_alerts(&Context::background(), &pool).unwrap();
    let s = se.0.lock().unwrap();
    assert_eq!(
        vec![
            build_clear_disabled_mv_refresh_alert_level_sql().to_owned(),
            build_cleanup_stale_mv_refresh_alert_sql().to_owned()
        ],
        s.executed_restricted_sql
    );
    assert_eq!(2, s.executed_restricted_args.len());
    assert!(s.executed_restricted_args.iter().all(Vec::is_empty));
}

/// Go `recordingStatsHandle`: canned statistics, process ID 1.
#[derive(Default)]
pub(crate) struct RecordingStatsHandle(pub(crate) Mutex<HashMap<i64, MLogStats>>);

impl tidb_stats_handle_util::AutoAnalyzeProcIdGenerator for RecordingStatsHandle {
    fn auto_analyze_proc_id(&self) -> u64 {
        1
    }

    fn release_auto_analyze_proc_id(&self, _: u64) {}
}

impl crate::service_helper::MVServiceStatsHandle for RecordingStatsHandle {
    fn get_non_pseudo_physical_table_stats(&self, physical_table_id: i64) -> Option<MLogStats> {
        self.0.lock().unwrap().get(&physical_table_id).copied()
    }
}

/// Go `&recordingSysProcTracker{}`.
fn recording_sys_proc_tracker() -> crate::service_helper::SysProcTracker {
    crate::service_helper::SysProcTracker { track: Arc::new(|_, _| Ok(())), untrack: Arc::new(|_| {}) }
}

/// Go `newServiceHelper(statsHandleGetter, &recordingSysProcTracker{})`.
pub(crate) fn service_helper_with_stats(handle: &Arc<RecordingStatsHandle>) -> ServiceHelper {
    let handle: Arc<dyn crate::service_helper::MVServiceStatsHandle> = Arc::clone(handle) as _;
    let mut helper = service_helper();
    helper.stats_handle_getter = Some(Arc::new(move || Some(Arc::clone(&handle))));
    helper.sys_proc_tracker = Some(recording_sys_proc_tracker());
    helper
}

/// Go `statistics.AutoAnalyzeMinCnt`.
fn auto_analyze_min_cnt() -> i64 {
    tidb_stats::AUTO_ANALYZE_MIN_CNT.load(std::sync::atomic::Ordering::SeqCst)
}

/// Go `newMLogAnalyzeHelperForTest`.
pub(crate) fn new_m_log_analyze_helper_for_test() -> (ServiceHelper, Arc<RecordingStatsHandle>, Arc<RecordingSessionContext>, SystemSessionPool) {
    let (se, pool) = new_recording_session();
    let (_, mlog_table) = build_mock_mv_base_and_mv_log_tables(101, 201, &[101]);
    with_mock_info_schema(&se, vec![mlog_table]);
    se.0.lock().unwrap().restricted_rows.insert(TEST_SQL_FETCH_MV_LOG_ACCUMULATION.to_owned(), vec![ints(&[Some(201)])]);
    let handle = Arc::new(RecordingStatsHandle::default());
    handle.0.lock().unwrap().insert(201, MLogStats { realtime_count: auto_analyze_min_cnt() + 1, ..MLogStats::default() });
    (service_helper_with_stats(&handle), handle, se, pool)
}

/// Go `testSQLAutoAnalyzeParameters`.
pub(crate) const TEST_SQL_AUTO_ANALYZE_PARAMETERS: &str =
    "select variable_name, variable_value from mysql.global_variables where variable_name in (%?, %?, %?)";
const TEST_SQL_STATS_TABLE_LOCKED: &str = "SELECT table_id FROM mysql.stats_table_locked";

/// Go `containsAnalyzeSQL`.
pub(crate) fn contains_analyze_sql(se: &RecordingSessionContext) -> bool {
    se.0.lock().unwrap().executed_restricted_sql.iter().any(|sql| sql == "ANALYZE TABLE %n.%n")
}

/// Go `vardef.RunAutoAnalyze.Store(enabled)`, restored on drop (Go `t.Cleanup`).
pub(crate) struct RunAutoAnalyzeGuard(bool);

impl RunAutoAnalyzeGuard {
    pub(crate) fn set(enabled: bool) -> Self {
        Self(tidb_vardef::RUN_AUTO_ANALYZE.swap(enabled, std::sync::atomic::Ordering::SeqCst))
    }
}

impl Drop for RunAutoAnalyzeGuard {
    fn drop(&mut self) {
        tidb_vardef::RUN_AUTO_ANALYZE.store(self.0, std::sync::atomic::Ordering::SeqCst);
    }
}

#[test]
fn test_server_helper_analyze_mv_log_temporarily_updates_stats_session_vars() {
    use tidb_vardef::tidb_vars as v;
    let _serial = crate::test_helpers_test::serial();
    let (se, pool) = new_recording_session();
    let (_, mlog_table) = build_mock_mv_base_and_mv_log_tables(101, 201, &[101]);
    with_mock_info_schema(&se, vec![mlog_table]);
    {
        let mut s = se.0.lock().unwrap();
        s.watched_vars = vec![v::TIDB_ANALYZE_PARTITION_CONCURRENCY.to_owned()];
        for (name, session) in [
            (v::TIDB_ANALYZE_PARTITION_CONCURRENCY, "0"),
            (v::TIDB_ANALYZE_VERSION, "1"),
            (v::TIDB_ENABLE_ANALYZE_SNAPSHOT, "OFF"),
            (v::TIDB_PARTITION_PRUNE_MODE, "static"),
            (v::TIDB_ANALYZE_SKIP_COLUMN_TYPES, "json"),
        ] {
            s.system_vars.insert(name.to_owned(), session.to_owned());
        }
        for (name, global) in [
            (v::TIDB_ANALYZE_PARTITION_CONCURRENCY, "3"),
            (v::TIDB_ANALYZE_VERSION, "2"),
            (v::TIDB_ENABLE_ANALYZE_SNAPSHOT, "on"),
            (v::TIDB_PARTITION_PRUNE_MODE, "dynamic"),
            (v::TIDB_ANALYZE_SKIP_COLUMN_TYPES, "blob"),
        ] {
            s.global_vars.insert(name.to_owned(), global.to_owned());
        }
    }
    let handle = Arc::new(RecordingStatsHandle::default());
    handle
        .0
        .lock()
        .unwrap()
        .insert(201, MLogStats { stats_ver: 2, realtime_count: auto_analyze_min_cnt() + 1, ..MLogStats::default() });
    let helper = service_helper_with_stats(&handle);

    helper.analyze_mv_log(&Context::background(), &pool, 201).unwrap();
    let s = se.0.lock().unwrap();
    assert!(!s.executed_restricted_sql.is_empty());
    let analyze_idx = s.executed_restricted_sql.len() - 1;
    assert_eq!("ANALYZE TABLE %n.%n", s.executed_restricted_sql[analyze_idx]);
    assert_eq!(vec!["test".to_owned(), "mlog1".to_owned()], s.executed_restricted_args[analyze_idx]);
    assert_eq!(Some("3"), s.restricted_var_values[analyze_idx][v::TIDB_ANALYZE_PARTITION_CONCURRENCY].as_deref());
    drop(s);
    assert_eq!(0, tidb_stats_handle_util::StatsSessionContext::analyze_partition_concurrency(se.as_ref()));
    assert_eq!(1, tidb_stats_handle_util::StatsSessionContext::analyze_version(se.as_ref()));
    assert!(!tidb_stats_handle_util::StatsSessionContext::enable_analyze_snapshot(se.as_ref()));
    assert_eq!("static", tidb_stats_handle_util::StatsSessionContext::partition_prune_mode(se.as_ref()));
    assert_eq!(
        std::collections::BTreeSet::from(["json".to_owned()]),
        tidb_stats_handle_util::StatsSessionContext::analyze_skip_column_types(se.as_ref())
    );
}

#[test]
fn test_server_helper_m_log_analyze_rechecks_eligibility() {
    let _serial = crate::test_helpers_test::serial();
    let _run = RunAutoAnalyzeGuard::set(true);
    let row_count = auto_analyze_min_cnt() + 1;
    let analyzed_stats =
        |modify_count: i64| MLogStats { realtime_count: row_count, modify_count, analyzed: true, ..MLogStats::default() };
    let ratio_var = tidb_vardef::tidb_vars::TIDB_MLOG_AUTO_ANALYZE_RATIO;
    for (name, ratio, stats, want_analyze) in [
        ("ratio raised", "2", analyzed_stats(row_count), false),
        ("statistics refreshed", "0.5", analyzed_stats(0), false),
        ("still eligible", "0.5", analyzed_stats(row_count), true),
        (
            "initial analysis with raised ratio",
            "2",
            MLogStats { realtime_count: row_count, ..MLogStats::default() },
            true,
        ),
    ] {
        let (helper, handle, se, pool) = new_m_log_analyze_helper_for_test();
        if stats.analyzed {
            handle.0.lock().unwrap().insert(201, analyzed_stats(row_count));
        }
        se.0.lock().unwrap().global_vars.insert(ratio_var.to_owned(), "0.5".to_owned());
        let tasks = helper.load_all_tidb_mv_log_analyze_tasks(&Context::background(), &pool).unwrap();
        assert!(tasks.contains_key(&201), "{name}");

        // Change the configuration or cached statistics after the task was selected.
        se.0.lock().unwrap().global_vars.insert(ratio_var.to_owned(), ratio.to_owned());
        handle.0.lock().unwrap().insert(201, stats);
        helper.analyze_mv_log(&Context::background(), &pool, 201).unwrap();
        assert_eq!(want_analyze, contains_analyze_sql(&se), "{name}");
    }
}

#[test]
fn test_server_helper_m_log_analyze_locked_stats() {
    let _serial = crate::test_helpers_test::serial();
    let _run = RunAutoAnalyzeGuard::set(true);
    for phase in ["scan", "execute"] {
        // "lock query error"
        let (helper, _, se, pool) = new_m_log_analyze_helper_for_test();
        se.0.lock()
            .unwrap()
            .restricted_errs
            .insert(TEST_SQL_STATS_TABLE_LOCKED.to_owned(), MvsError::Other("lock query failed".to_owned()));
        let err = if phase == "scan" {
            helper.load_all_tidb_mv_log_analyze_tasks(&Context::background(), &pool).unwrap_err()
        } else {
            helper.analyze_mv_log(&Context::background(), &pool, 201).unwrap_err()
        };
        assert!(err.to_string().contains("lock query failed"), "{phase}: {err}");
        assert!(!contains_analyze_sql(&se), "{phase}");
        for locked in [false, true] {
            let (helper, _, se, pool) = new_m_log_analyze_helper_for_test();
            if locked {
                se.0.lock().unwrap().restricted_rows.insert(TEST_SQL_STATS_TABLE_LOCKED.to_owned(), vec![ints(&[Some(201)])]);
            }
            if phase == "scan" {
                let tasks = helper.load_all_tidb_mv_log_analyze_tasks(&Context::background(), &pool).unwrap();
                assert_eq!(!locked, tasks.len() == 1, "{phase} locked={locked}");
            } else {
                helper.analyze_mv_log(&Context::background(), &pool, 201).unwrap();
                assert_eq!(!locked, contains_analyze_sql(&se), "{phase} locked={locked}");
            }
        }
    }
}

#[test]
fn test_server_helper_m_log_analyze_stops_queued_tasks() {
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    let _serial = crate::test_helpers_test::serial();
    let _run = RunAutoAnalyzeGuard::set(true);
    let (mut helper, _, se, pool) = new_m_log_analyze_helper_for_test();
    let stopped = Arc::new(AtomicBool::new(false));
    let stopped_load = Arc::clone(&stopped);
    helper.is_auto_analyze_stopped = Some(Arc::new(move || stopped_load.load(SeqCst)));
    let helper = Arc::new(helper);
    let pool = Arc::new(pool);
    let executor = crate::executor::TaskExecutor::new(1, 0);
    let (entered_tx, entered) = crossbeam_channel::bounded::<()>(1);
    let (release_tx, release) = crossbeam_channel::bounded::<()>(1);
    executor.run();
    assert!(executor.submit(
        "running",
        Box::new(move || {
            let _ = entered_tx.send(());
            let _ = release.recv();
            Ok(())
        })
    ));
    entered.recv().unwrap();
    let (queued_helper, queued_pool) = (Arc::clone(&helper), Arc::clone(&pool));
    assert!(executor.submit(
        "queued analyze",
        Box::new(move || {
            queued_helper.analyze_mv_log(&Context::background(), &*queued_pool, 201).map_err(|err| err.to_string())
        })
    ));
    stopped.store(true, SeqCst);
    release_tx.send(()).unwrap();
    crate::test_helpers_test::eventually(
        || executor.metrics.counters.finished_count.load(SeqCst) == 2,
        crate::test_helpers_test::TEST_EVENTUALLY_WAIT,
        crate::test_helpers_test::TEST_EVENTUALLY_TICK,
    );
    executor.close();
    assert!(!contains_analyze_sql(&se));

    stopped.store(false, SeqCst);
    se.0.lock().unwrap().executed_restricted_sql.clear();
    let on_get_stopped = Arc::clone(&stopped);
    let pool = SystemSessionPool(Arc::new(RecordingSessionPool(
        Arc::clone(&se),
        Some(Box::new(move || on_get_stopped.store(true, SeqCst))),
    )));
    helper.analyze_mv_log(&Context::background(), &pool, 201).unwrap();
    assert!(!contains_analyze_sql(&se));
}

#[test]
fn test_mv_service_m_log_analyze_logs_owned_candidates() {
    let _serial = crate::test_helpers_test::serial();
    let _run = RunAutoAnalyzeGuard::set(true);
    let (observer, recorded) = tidb_logutil::observer::new(tidb_log::Level::Info);
    let restore = tidb_log::replace_globals(observer);
    let (helper, handle, se, pool) = new_m_log_analyze_helper_for_test();
    let (_, owned) = build_mock_mv_base_and_mv_log_tables(101, 201, &[]);
    let (_, mut unowned) = build_mock_mv_base_and_mv_log_tables(102, 301, &[]);
    unowned.name = tidb_ast::CiString::new("mlog2");
    with_mock_info_schema(&se, vec![owned, unowned]);
    {
        let mut stats = handle.0.lock().unwrap();
        let owned_stats = stats[&201];
        stats.insert(301, owned_stats);
    }
    se.0.lock()
        .unwrap()
        .restricted_rows
        .insert(TEST_SQL_FETCH_MV_LOG_ACCUMULATION.to_owned(), vec![ints(&[Some(201)]), ints(&[Some(301)])]);
    let svc = crate::service_settings::new_mv_service(
        Context::background(),
        Arc::new(pool),
        Arc::new(helper),
        crate::service_settings::default_mv_service_config(),
    );
    crate::mvservice_test::set_task_owners_for_test(&svc, &[(201, 5), (301, 25)]);
    let ids = svc.fetch_mlog_analyze_tasks().unwrap();
    let entries = recorded.filter_message("mv service: mlog analyze needed");
    restore.restore();
    assert_eq!(vec![201], ids);
    assert_eq!(1, entries.len());
    let context: HashMap<_, _> = entries[0].context.iter().cloned().collect();
    assert_eq!(Some("201"), context.get("mvlog_id").map(String::as_str));
    assert_eq!(Some("mlog unanalyzed"), context.get("reason").map(String::as_str));
}

#[test]
fn test_register_mv_service_bootstrap_and_ddl_handler() {
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering::SeqCst};
    use tidb_model::ActionType;
    let called = Arc::new(AtomicI32::new(0));
    let stopped = Arc::new(AtomicBool::new(false));
    let mut got = None;
    let (stopped_load, called_add) = (Arc::clone(&stopped), Arc::clone(&called));
    let svc = crate::service_helper::register_mv_service(
        Context::background(),
        |id, handler| got = Some((id, handler)),
        Arc::new(crate::mvservice_test::MockSessionPool),
        crate::service_helper::ServerViews {
            local_server: Box::new(|| Ok(ServerInfo::default())),
            all_servers: Box::new(|| Ok(Vec::new())),
        },
        None,
        None,
        Arc::new(move || stopped_load.load(SeqCst)),
        Arc::new(move || {
            called_add.fetch_add(1, SeqCst);
        }),
    );
    stopped.store(true, SeqCst);
    // Go `svc.mh.(*serviceHelper).isAutoAnalyzeStopped()`: a stopped helper
    // answers the analyze scan before it needs a statistics handle.
    {
        let _serial = crate::test_helpers_test::serial();
        let _run = RunAutoAnalyzeGuard::set(true);
        let tasks = svc.mh.load_all_tidb_mv_log_analyze_tasks(&Context::background(), &crate::mvservice_test::MockSessionPool);
        assert!(tasks.unwrap().is_empty());
    }
    assert!(svc.ddl_dirty.load(SeqCst));
    assert!(svc.notifier.is_awake());
    let (handler_id, handler) = got.expect("the DDL handler is registered");
    assert_eq!(tidb_ddl_notifier::MV_SERVICE_HANDLER_ID, handler_id);
    assert_eq!(
        crate::service::TaskBackpressureConfig {
            cpu_threshold: crate::backpressure::DEFAULT_BACKPRESSURE_CPU_THRESHOLD,
            mem_threshold: crate::backpressure::DEFAULT_BACKPRESSURE_MEM_THRESHOLD,
            delay: crate::backpressure::DEFAULT_TASK_BACKPRESSURE_DELAY,
        },
        svc.get_task_backpressure_config()
    );
    assert!(svc.refresh_executor.backpressure.read().unwrap().is_some());
    assert!(svc.purge_executor.backpressure.read().unwrap().is_some());
    assert!(svc.mlog_analyze_executor.backpressure.read().unwrap().is_some());

    let handle = |event: tidb_ddl_notifier::SchemaChangeEvent| {
        let mut session = NoNotifierSession;
        handler(&mut session, &event).unwrap();
        called.load(SeqCst)
    };
    let typed = |action: ActionType| {
        serde_json::from_str::<tidb_ddl_notifier::SchemaChangeEvent>(&format!(r#"{{"type":{}}}"#, action.0)).unwrap()
    };
    assert_eq!(0, handle(tidb_ddl_notifier::SchemaChangeEvent::create_table(table(1, "t"))));
    let mut mlog = table(2, "mlog");
    mlog.materialized_view_log = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewLogInfo {
        base_table_id: 1,
        ..Default::default()
    }));
    assert_eq!(1, handle(tidb_ddl_notifier::SchemaChangeEvent::create_table(mlog)));
    let mut mv = table(3, "mv");
    mv.materialized_view = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewInfo {
        base_table_ids: vec![1].into(),
        ..Default::default()
    }));
    assert_eq!(2, handle(tidb_ddl_notifier::SchemaChangeEvent::create_table(mv.clone_like_go())));
    assert_eq!(2, handle(typed(ActionType::ACTION_CREATE_MATERIALIZED_VIEW_LOG)));
    assert_eq!(2, handle(typed(ActionType::ACTION_CREATE_MATERIALIZED_VIEW)));
    assert_eq!(3, handle(typed(ActionType::ACTION_ALTER_MATERIALIZED_VIEW_REFRESH)));
    assert_eq!(4, handle(typed(ActionType::ACTION_ALTER_MATERIALIZED_VIEW_LOG_PURGE)));
    assert_eq!(5, handle(typed(ActionType::ACTION_MVIEW_REFRESH_OUT_OF_PLACE_CUTOVER)));
    assert_eq!(5, handle(tidb_ddl_notifier::SchemaChangeEvent::drop_table(table(4, "t4"))));
    let mut drop_mv = table(5, "mv5");
    drop_mv.materialized_view = mv.materialized_view.clone();
    assert_eq!(6, handle(tidb_ddl_notifier::SchemaChangeEvent::drop_table(drop_mv)));
}

/// Go's `nil` session the MV handler ignores.
struct NoNotifierSession;

impl tidb_ddl_notifier::NotifierSession for NoNotifierSession {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn begin_pessimistic(&mut self) -> Result<(), tidb_ddl_notifier::NotifierError> {
        Ok(())
    }

    fn commit(&mut self) -> Result<(), tidb_ddl_notifier::NotifierError> {
        Ok(())
    }

    fn rollback(&mut self) {}
}
