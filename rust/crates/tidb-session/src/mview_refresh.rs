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

//! Go master `executor.RefreshMaterializedViewExec`
//! (`pkg/executor/mview_refresh.go`): REFRESH MATERIALIZED VIEW on a system
//! session of the domain, its refresh-info row locked in a pessimistic
//! transaction, its history row in `mysql.tidb_mview_refresh_hist` and its
//! advisory lock held throughout.
//!
//! COMPLETE IN PLACE runs here. FAST, COMPLETE OUT OF PLACE, COMPLETE DELTA
//! APPLY and the DRY RUN / WITH PROFILE observers are refused until they are
//! ported (phase 7 of the E29 ExecPlan).

use tidb_ast::{RefreshMaterializedViewMode, RefreshMaterializedViewObserveType, RefreshMaterializedViewStmt};
use tidb_datatype::Datum;
use tidb_driver_error::DriverError;
use tidb_executor::sessionctx::StmtOutput;
use tidb_sqlescape::SqlArg;
use tidb_terror::MysqlError;

use crate::Session;

/// Go master `mvRefreshAdvisoryLockTimeoutSec`.
const MV_REFRESH_ADVISORY_LOCK_TIMEOUT_SEC: i64 = 1;
/// Go master `refreshHistStatusRunning` / `Success` / `Failed`.
const REFRESH_HIST_STATUS_RUNNING: &str = "running";
const REFRESH_HIST_STATUS_SUCCESS: &str = "success";
const REFRESH_HIST_STATUS_FAILED: &str = "failed";

/// Go master `mvRefreshShadowTablePrefix`.
const MV_REFRESH_SHADOW_TABLE_PREFIX: &str = "__mv_shadow_";

pub(crate) fn plain(message: impl Into<String>) -> DriverError {
    DriverError::Mysql(MysqlError::unknown(message.into()))
}

/// Go `errors.Annotate(cause, message)`: `Error()` reads `message: cause`,
/// while the client and `SHOW WARNINGS` see the cause (`errors.Cause`).
pub(crate) fn annotate(error: &DriverError, message: &str) -> DriverError {
    DriverError::Mysql(error.clone().to_mysql_error().annotate(message))
}

/// Go `err.Error()`.
pub(crate) fn driver_message(error: &DriverError) -> String {
    match error {
        DriverError::Mysql(error) => error.error_string(),
        other => other.to_string(),
    }
}

/// Go `StmtCtx.AppendWarning(err)` as `SHOW WARNINGS` reads it: the cause's
/// code and message (`errors.Cause`).
pub(crate) fn append_cause_warning(session: &mut Session, error: &DriverError) {
    let cause = error.clone().to_mysql_error();
    session.append_warning(tidb_context::WarningLevel::Warning, cause.code, cause.message);
}

pub(crate) fn is_table_not_exists(error: &DriverError) -> bool {
    matches!(error, DriverError::Mysql(error) if error.equal(&tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS))
}

pub(crate) fn datum_u64(datum: &Datum) -> Option<u64> {
    match datum {
        Datum::UInt(value) => Some(*value),
        Datum::Int(value) => u64::try_from(*value).ok(),
        _ => None,
    }
}

pub(crate) fn datum_i64(datum: &Datum) -> Option<i64> {
    match datum {
        Datum::Int(value) => Some(*value),
        Datum::UInt(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

pub(crate) fn datum_text(datum: &Datum) -> String {
    match datum {
        Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        _ => String::new(),
    }
}

/// One statement on a system session (Go `sqlExec.ExecuteInternal`).
pub(crate) fn sys_sql(session: &mut Session, sql: &str, arguments: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, DriverError> {
    let sql = crate::domain::cluster_session_node::sql_with_params(sql, arguments)
        .map_err(|error| plain(error.to_string()))?;
    let context = tidb_sqlexec::BackgroundContext;
    crate::domain::cluster_session_node::execute_internal_under(session, &context, &sql)
        .map(|(rows, _)| rows)
        .map_err(|error| match error.downcast::<tidb_store_driver_error::sql_query_error::SqlQueryError>() {
            Ok(error) => DriverError::Mysql((*error).into()),
            Err(error) => plain(error.to_string()),
        })
}

/// Go master `refreshInfoSnapshot`.
#[derive(Clone, Copy, Default)]
struct RefreshInfoSnapshot {
    last_success_read_tso: u64,
    last_success_read_tso_null: bool,
}

/// Go master `decodeRefreshInfoSnapshot` (the read TSO column).
fn decode_refresh_info_snapshot(row: &[Datum], read_tso_idx: usize) -> RefreshInfoSnapshot {
    match row.get(read_tso_idx).and_then(datum_u64) {
        Some(read_tso) => RefreshInfoSnapshot { last_success_read_tso: read_tso, last_success_read_tso_null: false },
        None => RefreshInfoSnapshot { last_success_read_tso: 0, last_success_read_tso_null: true },
    }
}

fn refresh_info_missing_table() -> DriverError {
    plain("refresh materialized view: required system table mysql.tidb_mview_refresh_info does not exist")
}

/// Go master `lockRefreshInfoRow`.
fn lock_refresh_info_row(session: &mut Session, mview_id: i64) -> Result<RefreshInfoSnapshot, DriverError> {
    let rows = sys_sql(
        session,
        "SELECT MVIEW_ID, LAST_SUCCESS_READ_TSO, LAST_SUCCESS_REFRESH_END_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? FOR UPDATE NOWAIT",
        &[SqlArg::Signed(mview_id)],
    )
    .map_err(|error| if is_table_not_exists(&error) { refresh_info_missing_table() } else { error })?;
    let Some(row) = rows.first() else {
        return Err(plain("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh_info"));
    };
    Ok(decode_refresh_info_snapshot(row, 1))
}

/// Go master `readRefreshInfoSnapshot`.
fn read_refresh_info_snapshot(session: &mut Session, mview_id: i64) -> Result<RefreshInfoSnapshot, DriverError> {
    let rows = sys_sql(
        session,
        "SELECT LAST_SUCCESS_READ_TSO, LAST_SUCCESS_REFRESH_END_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %?",
        &[SqlArg::Signed(mview_id)],
    )
    .map_err(|error| if is_table_not_exists(&error) { refresh_info_missing_table() } else { error })?;
    let Some(row) = rows.first() else {
        return Err(plain("refresh materialized view: refresh info row missing in mysql.tidb_mview_refresh_info"));
    };
    Ok(decode_refresh_info_snapshot(row, 0))
}

/// Go master `persistRefreshSuccess` (a manual refresh leaves the schedule).
fn persist_refresh_success(
    session: &mut Session,
    mview_id: i64,
    locked: RefreshInfoSnapshot,
    refresh_read_tso: u64,
    last_success_refresh_end_unix_seconds: i64,
) -> Result<(), DriverError> {
    let locked_read_tso = if locked.last_success_read_tso_null {
        SqlArg::Null
    } else {
        SqlArg::Unsigned(locked.last_success_read_tso)
    };
    sys_sql(
        session,
        "UPDATE mysql.tidb_mview_refresh_info\nSET\n\tLAST_SUCCESS_READ_TSO = %?,\n\tLAST_SUCCESS_REFRESH_END_UNIX_SECONDS = %?\nWHERE MVIEW_ID = %? AND LAST_SUCCESS_READ_TSO <=> %?",
        &[
            SqlArg::Unsigned(refresh_read_tso),
            SqlArg::Signed(last_success_refresh_end_unix_seconds),
            SqlArg::Signed(mview_id),
            locked_read_tso,
        ],
    )
    .map_err(|error| if is_table_not_exists(&error) { refresh_info_missing_table() } else { error })?;
    let persisted = read_refresh_info_snapshot(session, mview_id)?;
    if persisted.last_success_read_tso_null || persisted.last_success_read_tso != refresh_read_tso {
        return Err(plain("refresh materialized view: inconsistent LAST_SUCCESS_READ_TSO after success update"));
    }
    Ok(())
}

fn refresh_hist_missing_table() -> DriverError {
    plain("refresh materialized view: required system table mysql.tidb_mview_refresh_hist does not exist")
}

/// Go master `histTime`: a time in the history session's zone, to the
/// microsecond.
pub(crate) fn hist_time(at: chrono::DateTime<chrono::Local>) -> String {
    at.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

/// Go master `formatDurationSecondsBetween`.
pub(crate) fn format_duration_seconds_between(start: chrono::DateTime<chrono::Local>, end: chrono::DateTime<chrono::Local>) -> String {
    let micros = (end - start).num_microseconds().unwrap_or_default().max(0);
    format!("{}.{:06}", micros / 1_000_000, micros % 1_000_000)
}

/// Go master `insertRefreshHistRunning`.
#[allow(clippy::too_many_arguments)]
fn insert_refresh_hist_running(
    session: &mut Session,
    refresh_job_id: u64,
    mview_id: i64,
    schema: &str,
    name: &str,
    method: &str,
    start: &str,
) -> Result<(), DriverError> {
    sys_sql(
        session,
        "INSERT INTO mysql.tidb_mview_refresh_hist (\n\tREFRESH_JOB_ID,\n\tMVIEW_ID,\n\tMVIEW_SCHEMA,\n\tMVIEW_NAME,\n\tREFRESH_METHOD,\n\tREFRESH_START_TIME,\n\tREFRESH_STATUS,\n\tLAST_HEARTBEAT_TIME\n) VALUES (\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?\n)",
        &[
            SqlArg::Unsigned(refresh_job_id),
            SqlArg::Signed(mview_id),
            SqlArg::String(schema.as_bytes()),
            SqlArg::String(name.as_bytes()),
            SqlArg::String(method.as_bytes()),
            SqlArg::String(start.as_bytes()),
            SqlArg::String(REFRESH_HIST_STATUS_RUNNING.as_bytes()),
            SqlArg::String(start.as_bytes()),
        ],
    )
    .map(|_| ())
    .map_err(|error| if is_table_not_exists(&error) { refresh_hist_missing_table() } else { error })
}

/// Go master `insertRefreshHistFailed`.
#[allow(clippy::too_many_arguments)]
fn insert_refresh_hist_failed(
    session: &mut Session,
    refresh_job_id: u64,
    mview_id: i64,
    schema: &str,
    name: &str,
    method: &str,
    start: chrono::DateTime<chrono::Local>,
    end: chrono::DateTime<chrono::Local>,
    reason: &str,
) -> Result<(), DriverError> {
    let (start_text, end_text) = (hist_time(start), hist_time(end));
    let duration = format_duration_seconds_between(start, end);
    sys_sql(
        session,
        "INSERT INTO mysql.tidb_mview_refresh_hist (\n\tREFRESH_JOB_ID,\n\tMVIEW_ID,\n\tMVIEW_SCHEMA,\n\tMVIEW_NAME,\n\tREFRESH_METHOD,\n\tREFRESH_START_TIME,\n\tREFRESH_END_TIME,\n\tREFRESH_DURATION_SEC,\n\tREFRESH_STATUS,\n\tREFRESH_READ_TSO,\n\tREFRESH_FAILED_REASON,\n\tLAST_HEARTBEAT_TIME\n) VALUES (\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?,\n\t%?\n)",
        &[
            SqlArg::Unsigned(refresh_job_id),
            SqlArg::Signed(mview_id),
            SqlArg::String(schema.as_bytes()),
            SqlArg::String(name.as_bytes()),
            SqlArg::String(method.as_bytes()),
            SqlArg::String(start_text.as_bytes()),
            SqlArg::String(end_text.as_bytes()),
            SqlArg::String(duration.as_bytes()),
            SqlArg::String(REFRESH_HIST_STATUS_FAILED.as_bytes()),
            SqlArg::Null,
            SqlArg::String(reason.as_bytes()),
            SqlArg::String(end_text.as_bytes()),
        ],
    )
    .map(|_| ())
    .map_err(|error| if is_table_not_exists(&error) { refresh_hist_missing_table() } else { error })
}

/// Go master `finalizeRefreshHist`.
#[allow(clippy::too_many_arguments)]
fn finalize_refresh_hist(
    session: &mut Session,
    refresh_job_id: u64,
    mview_id: i64,
    status: &str,
    read_tso: Option<u64>,
    commit_tso: Option<u64>,
    start: chrono::DateTime<chrono::Local>,
    end: chrono::DateTime<chrono::Local>,
    failed_reason: Option<&str>,
) -> Result<(), DriverError> {
    let end_text = hist_time(end);
    let duration = format_duration_seconds_between(start, end);
    sys_sql(
        session,
        "UPDATE mysql.tidb_mview_refresh_hist\nSET\n\tREFRESH_END_TIME = %?,\n\tREFRESH_STATUS = %?,\n\tREFRESH_ROWS = %?,\n\tREFRESH_DURATION_SEC = %?,\n\tREFRESH_SCHEDULE_DURATION_SEC = %?,\n\tREFRESH_READ_TSO = %?,\n\tREFRESH_COMMIT_TSO = %?,\n\tREFRESH_FAILED_REASON = %?\nWHERE REFRESH_JOB_ID = %?\n  AND MVIEW_ID = %?",
        &[
            SqlArg::String(end_text.as_bytes()),
            SqlArg::String(status.as_bytes()),
            SqlArg::Null,
            SqlArg::String(duration.as_bytes()),
            SqlArg::Null,
            read_tso.map_or(SqlArg::Null, SqlArg::Unsigned),
            commit_tso.map_or(SqlArg::Null, SqlArg::Unsigned),
            failed_reason.map_or(SqlArg::Null, |reason| SqlArg::String(reason.as_bytes())),
            SqlArg::Unsigned(refresh_job_id),
            SqlArg::Signed(mview_id),
        ],
    )
    .map(|_| ())
    .map_err(|error| if is_table_not_exists(&error) { refresh_hist_missing_table() } else { error })
}

/// Go master `finalizeRefreshHistWithRetry`.
#[allow(clippy::too_many_arguments)]
fn finalize_refresh_hist_with_retry(
    session: &mut Session,
    refresh_job_id: u64,
    mview_id: i64,
    status: &str,
    read_tso: Option<u64>,
    commit_tso: Option<u64>,
    start: chrono::DateTime<chrono::Local>,
    end: chrono::DateTime<chrono::Local>,
    failed_reason: Option<&str>,
) -> Result<(), DriverError> {
    let first = match finalize_refresh_hist(session, refresh_job_id, mview_id, status, read_tso, commit_tso, start, end, failed_reason) {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    finalize_refresh_hist(session, refresh_job_id, mview_id, status, read_tso, commit_tso, start, end, failed_reason)
        .map_err(|retry| annotate(&retry, &format!("first finalize attempt failed: {}", driver_message(&first))))
}

/// Go master `markRefreshFailedAlertState`, through `reportMVRefreshFailed`
/// (which only logs its own failure).
fn report_mv_refresh_failed(session: &mut Session, report: bool, mview_id: i64, schema: &str, name: &str) {
    if !report {
        return;
    }
    let _ = sys_sql(
        session,
        "INSERT INTO mysql.tidb_mview_refresh_alert (\n\tMVIEW_ID,\n\tMVIEW_SCHEMA,\n\tMVIEW_NAME,\n\tREFRESH_FAILED,\n\tUPDATE_TIME\n) VALUES (\n\t%?,\n\t%?,\n\t%?,\n\t'YES',\n\tNOW(6)\n) ON DUPLICATE KEY UPDATE\nMVIEW_SCHEMA = VALUES(MVIEW_SCHEMA),\nMVIEW_NAME = VALUES(MVIEW_NAME),\nREFRESH_FAILED = VALUES(REFRESH_FAILED),\nUPDATE_TIME = VALUES(UPDATE_TIME)",
        &[SqlArg::Signed(mview_id), SqlArg::String(schema.as_bytes()), SqlArg::String(name.as_bytes())],
    );
}

/// Go master `deleteRefreshAlertState`.
fn delete_refresh_alert_state(session: &mut Session, mview_id: i64) -> Result<(), DriverError> {
    sys_sql(session, "DELETE FROM mysql.tidb_mview_refresh_alert\nWHERE MVIEW_ID = %?", &[SqlArg::Signed(mview_id)])
        .map(|_| ())
        .map_err(|error| {
            if is_table_not_exists(&error) {
                plain("refresh materialized view: required system table mysql.tidb_mview_refresh_alert does not exist")
            } else {
                error
            }
        })
}

/// Go master `mviewutil.ReadRefreshSourceIDs`.
fn read_refresh_source_ids(session: &mut Session, mview_id: i64) -> Result<Vec<i64>, DriverError> {
    let rows = sys_sql(
        session,
        &format!(
            "SELECT MLOG_ID FROM mysql.{} WHERE MVIEW_ID = %? ORDER BY MLOG_ID",
            tidb_metadef::system_tables_def::TIDB_MVIEW_FAST_REFRESH_SOURCE_INFO_TABLE_NAME
        ),
        &[SqlArg::Signed(mview_id)],
    )?;
    Ok(rows.iter().filter_map(|row| row.first().and_then(datum_i64)).collect())
}

/// Go master `mviewutil.ReplaceRefreshSourceIDs`.
fn replace_refresh_source_ids(session: &mut Session, mview_id: i64, mlog_ids: &[i64]) -> Result<(), DriverError> {
    let table = tidb_metadef::system_tables_def::TIDB_MVIEW_FAST_REFRESH_SOURCE_INFO_TABLE_NAME;
    sys_sql(session, &format!("DELETE FROM mysql.{table} WHERE MVIEW_ID IN (%?)"), &[SqlArg::Signed(mview_id)])?;
    if mlog_ids.is_empty() {
        return Ok(());
    }
    let values = mlog_ids.iter().map(|_| "(%?, %?)").collect::<Vec<_>>().join(",");
    let arguments: Vec<SqlArg<'_>> =
        mlog_ids.iter().flat_map(|&mlog_id| [SqlArg::Signed(mview_id), SqlArg::Signed(mlog_id)]).collect();
    sys_sql(session, &format!("INSERT INTO mysql.{table} (MVIEW_ID, MLOG_ID) VALUES {values}"), &arguments).map(|_| ())
}

/// The last statement's answer of `@@tidb_last_query_info` / `@@tidb_last_txn_info`.
fn session_json_u64(session: &mut Session, variable: &str, key: &str) -> Option<u64> {
    let rows = sys_sql(session, &format!("SELECT @@{variable}"), &[]).ok()?;
    let text = rows.first().and_then(|row| row.first()).map(datum_text)?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    value.get(key).and_then(serde_json::Value::as_u64).filter(|&ts| ts != 0)
}

/// What the refresh session runs under (Go master
/// `applyRefreshExecutionSessionVars` then `ddl.InitMViewSession`): every
/// variable, and the session's own value to restore.
fn refresh_session_vars(
    user: &Session,
    info: &tidb_model::table::MaterializedViewInfo,
) -> Vec<(String, String)> {
    let execution = tidb_variable::mview_execution::MViewExecutionSessionVars::capture(|name| {
        user.vars.get_system(name).ok()
    });
    let mut vars: Vec<(String, String)> = Vec::new();
    for (name, value) in execution.job_system_vars() {
        let name = match name {
            tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_MEM_QUOTA => "tidb_mem_quota_query",
            tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_ISOLATION_READ_ENGINES => "tidb_isolation_read_engines",
            other => other,
        };
        vars.push((name.to_owned(), value));
    }
    vars.push((tidb_vardef::tidb_vars::TIDB_MVIEW_ENABLE.to_owned(), "ON".to_owned()));
    vars.push(("sql_mode".to_owned(), tidb_ddl::cluster_ddl::materialized_view::format_sql_mode(info.definition_sql_mode)));
    let zone = &info.definition_time_zone;
    let zone_name = if zone.offset == 0 {
        zone.name.to_string()
    } else {
        let sign = if zone.offset < 0 { '-' } else { '+' };
        format!("{sign}{:02}:{:02}", zone.offset.abs() / 3600, zone.offset.abs() % 3600 / 60)
    };
    vars.push(("time_zone".to_owned(), zone_name));
    vars.push(("div_precision_increment".to_owned(), info.definition_div_precision_increment.to_string()));
    vars
}

fn set_session_vars(session: &mut Session, vars: &[(String, String)]) -> Result<(), DriverError> {
    for (name, value) in vars {
        sys_sql(session, &format!("SET @@SESSION.{name} = %?"), &[SqlArg::String(value.as_bytes())])?;
    }
    Ok(())
}

fn read_session_vars(session: &mut Session, names: &[String]) -> Result<Vec<String>, DriverError> {
    let select = format!("SELECT {}", names.iter().map(|name| format!("@@SESSION.{name}")).collect::<Vec<_>>().join(", "));
    let rows = sys_sql(session, &select, &[])?;
    Ok(rows.first().map(|row| row.iter().map(datum_text).collect()).unwrap_or_default())
}

/// Go master `validateRefreshMaterializedViewStmt`: the mode and the history
/// row's `REFRESH_METHOD`.
fn validate_refresh_materialized_view_stmt(s: &RefreshMaterializedViewStmt) -> Result<String, DriverError> {
    let method = match s.mode {
        RefreshMaterializedViewMode::Fast if s.as_of.is_some() => "bounded fast",
        RefreshMaterializedViewMode::Fast => "fast",
        RefreshMaterializedViewMode::CompleteDeltaApply => "complete delta apply",
        RefreshMaterializedViewMode::CompleteInPlace => "complete in place",
        RefreshMaterializedViewMode::CompleteOutOfPlace => "complete out of place",
    };
    if s.with_async_mode {
        return Err(plain("refresh materialized view: WITH ASYNC MODE is not supported yet"));
    }
    if s.as_of.is_some() && s.mode != RefreshMaterializedViewMode::Fast {
        return Err(plain("refresh materialized view: AS OF TIMESTAMP is only supported for FAST refresh"));
    }
    // A statement this node runs is never Go's internal (scheduled) SQL.
    Ok(format!("{method} manual"))
}

impl Session {
    /// Go master `RefreshMaterializedViewExec.Next` for COMPLETE IN PLACE and
    /// COMPLETE OUT OF PLACE; `sql` is the statement's text (the cutover
    /// job's query).
    pub(crate) fn refresh_materialized_view(
        &mut self,
        s: &RefreshMaterializedViewStmt,
        sql: &str,
    ) -> Result<StmtOutput, DriverError> {
        let method = validate_refresh_materialized_view_stmt(s)?;
        if s.observe_type != RefreshMaterializedViewObserveType::None {
            return Err(DriverError::unsupported("REFRESH MATERIALIZED VIEW DRY RUN / WITH PROFILE on this node"));
        }
        if !matches!(s.mode, RefreshMaterializedViewMode::CompleteInPlace | RefreshMaterializedViewMode::CompleteOutOfPlace) {
            return Err(DriverError::unsupported(format!("REFRESH MATERIALIZED VIEW {method} on this node")));
        }
        let refresh_start = chrono::Local::now();
        let (schema, info) = self.resolve_refresh_materialized_view_target(s)?;
        let mview = info.materialized_view.as_ref().expect("resolved as a view").read().clone();
        self.check_refresh_materialized_view_base_table_select(&mview)?;
        if s.mode == RefreshMaterializedViewMode::CompleteInPlace
            && info.materialized_view_base.as_ref().is_some_and(|base| base.read().mlog_id != 0)
        {
            return Err(plain(format!(
                "refresh materialized view complete IN PLACE is not supported for materialized view {schema}.{} with a materialized view log",
                info.name.original()
            )));
        }
        let name = info.name.original().to_owned();
        let mut refresh_job_id = 0u64;
        let mut hist_inserted = false;
        let result = self.refresh_complete(
            s.mode,
            sql,
            &schema,
            &info,
            &mview,
            &method,
            refresh_start,
            &mut refresh_job_id,
            &mut hist_inserted,
        );
        let Err(error) = result else {
            return result;
        };
        if hist_inserted {
            return Err(error);
        }
        // Go master `insertRefreshHistFailedFallback`.
        let mut hist = self.open_refresh_system_session()?;
        if refresh_job_id == 0 {
            refresh_job_id = session_current_tso(&mut hist)?;
        }
        let reason = driver_message(&error);
        report_mv_refresh_failed(&mut hist, mview.alert_refresh_failed, info.id, &schema, &name);
        insert_refresh_hist_failed(
            &mut hist,
            refresh_job_id,
            info.id,
            &schema,
            &name,
            &method,
            refresh_start,
            chrono::Local::now(),
            &reason,
        )
        .map_err(|hist_error| {
            annotate(
                &hist_error,
                &format!("refresh materialized view: failed to insert failed refresh history after error {reason}"),
            )
        })?;
        Err(error)
    }

    /// Go `GetLatestInfoSchema` of the refreshing session.
    pub(crate) fn refresh_latest_info_schema(&self) -> std::sync::Arc<tidb_infoschema::SessionExtendedInfoSchema> {
        match &self.node {
            Some(node) => node.latest_info_schema(),
            None => std::sync::Arc::new(
                self.shared_catalog().lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone(),
            ),
        }
    }

    pub(crate) fn open_refresh_system_session(&self) -> Result<Session, DriverError> {
        use crate::domain::cluster_session_node::DomainSessions as _;
        let domain = self
            .domain_services()
            .and_then(|services| services.domain.upgrade())
            .ok_or_else(|| DriverError::unsupported("the session has no domain to take a system session from"))?;
        domain.open_system_session().map_err(crate::domain::cluster_session_node::sql_query_error_to_driver)
    }

    /// Go master `resolveRefreshMaterializedViewTarget`.
    fn resolve_refresh_materialized_view_target(
        &self,
        s: &RefreshMaterializedViewStmt,
    ) -> Result<(String, tidb_model::TableInfo), DriverError> {
        let catalog = self.refresh_latest_info_schema();
        let (schema, view) = match s.view_name.as_slice() {
            [schema, view] => (schema.clone(), view.clone()),
            [view] => {
                if self.current_database().is_empty() {
                    return Err(DriverError::Mysql(MysqlError::from_terror(
                        &tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                    )));
                }
                (self.current_database().to_owned(), view.clone())
            }
            _ => return Err(plain("refresh materialized view: missing view name")),
        };
        if catalog.schema_by_name(&schema).is_none() {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS.gen_with_stack_by_args(&[schema.into()]),
            )));
        }
        let Some((_, table)) = catalog.table_info_by_name(&schema, &view) else {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[schema.into(), view.into()]),
            )));
        };
        let table = table.read().clone_like_go();
        let Some(info) = table.materialized_view.as_ref().map(|info| info.read().clone()) else {
            return Err(DriverError::Mysql(MysqlError::from_terror(&tidb_dbterror::ERR_WRONG_OBJECT.gen_with_stack_by_args(
                &[schema.clone().into(), view.clone().into(), "MATERIALIZED VIEW".into()],
            ))));
        };
        if info.sql_content.is_empty() {
            return Err(plain("refresh materialized view: invalid select sql"));
        }
        // Go master `checkRefreshMaterializedViewReady`.
        let state = tidb_model::table::MaterializedViewInfo::get_init_build_state(Some(&info));
        if !state.is_ready() {
            return Err(plain(state.access_error_message(&format!("{schema}.{}", table.name.original()))));
        }
        Ok((schema, table))
    }

    /// Go master `checkRefreshMaterializedViewBaseTableSelect` for a user
    /// statement: SELECT on every base table.
    fn check_refresh_materialized_view_base_table_select(
        &self,
        info: &tidb_model::table::MaterializedViewInfo,
    ) -> Result<(), DriverError> {
        let catalog = self.refresh_latest_info_schema();
        for &id in info.base_table_ids.iter() {
            let Some(base) = catalog.table_info_by_id(id) else {
                continue;
            };
            let Some(schema) = catalog.schema_name_by_table_id(id) else {
                continue;
            };
            let base_name = base.read().name.lowercase().to_owned();
            if !self.has_table_privilege(&tidb_hack::go_to_lower(&schema), &base_name, tidb_mysql::SelectPriv) {
                let (user, host) = self
                    .current_identity()
                    .map(|(user, host)| (user.to_owned(), host.to_owned()))
                    .unwrap_or_default();
                return Err(DriverError::Mysql(MysqlError::from_terror(
                    &tidb_plannererrors::ERR_TABLEACCESS_DENIED.gen_with_stack_by_args(&[
                        "SELECT".into(),
                        user.into(),
                        host.into(),
                        base_name.into(),
                    ]),
                )));
            }
        }
        Ok(())
    }

    /// Go master `executeRefreshMaterializedView` for the COMPLETE modes:
    /// the refresh system session, the advisory lock and its checks, then
    /// the mode's path to the history row.
    #[allow(clippy::too_many_arguments)]
    fn refresh_complete(
        &mut self,
        mode: RefreshMaterializedViewMode,
        sql: &str,
        schema: &str,
        info: &tidb_model::TableInfo,
        mview: &tidb_model::table::MaterializedViewInfo,
        method: &str,
        refresh_start: chrono::DateTime<chrono::Local>,
        refresh_job_id: &mut u64,
        hist_inserted: &mut bool,
    ) -> Result<StmtOutput, DriverError> {
        let name = info.name.original().to_owned();
        let mview_id = info.id;
        let mut refresh = self.open_refresh_system_session()?;
        let vars = refresh_session_vars(self, mview);
        let names: Vec<String> = vars.iter().map(|(name, _)| name.clone()).collect();
        let original = read_session_vars(&mut refresh, &names)?;
        let original_db = refresh.current_database().to_owned();
        let restore: Vec<(String, String)> = names.into_iter().zip(original).collect();
        let outcome = (|| {
            set_session_vars(&mut refresh, &vars)?;
            sys_sql(&mut refresh, "USE %n", &[SqlArg::String(schema.as_bytes())])?;
            // Go master `acquireMVRefreshAdvisoryLock`.
            let lock_name = tidb_mviewutil::refresh_advisory_lock_name(info.db_id, mview_id);
            let got = sys_sql(
                &mut refresh,
                "SELECT GET_LOCK(%?, %?)",
                &[SqlArg::String(lock_name.as_bytes()), SqlArg::Signed(MV_REFRESH_ADVISORY_LOCK_TIMEOUT_SEC)],
            )?;
            if got.first().and_then(|row| row.first()).and_then(datum_i64) != Some(1) {
                return Err(annotate(
                    &plain("materialized view refresh advisory lock conflict"),
                    &format!(
                        "another refresh or materialized view log creation is running for materialized view {schema}.{name}, please retry later"
                    ),
                ));
            }
            let locked = self.recheck_refresh_target_under_lock(schema, &name, mview_id, mode).and_then(|()| {
                // Go: CREATE publishes the input logs; a missing
                // registration is metadata corruption.
                if read_refresh_source_ids(&mut refresh, mview_id)?.is_empty() {
                    return Err(plain(format!(
                        "refresh materialized view: input log records are missing for {schema}.{name}"
                    )));
                }
                if mode == RefreshMaterializedViewMode::CompleteOutOfPlace {
                    return self.refresh_complete_out_of_place_locked(
                        &mut refresh,
                        sql,
                        schema,
                        info,
                        mview,
                        method,
                        refresh_start,
                        refresh_job_id,
                        hist_inserted,
                    );
                }
                self.refresh_complete_in_place_locked(
                    &mut refresh,
                    schema,
                    &name,
                    mview_id,
                    mview,
                    method,
                    refresh_start,
                    refresh_job_id,
                    hist_inserted,
                )
            });
            // Go master `releaseMVRefreshAdvisoryLockFully`: the lock is held
            // exactly once.
            let mut released = 0;
            while sys_sql(&mut refresh, "SELECT RELEASE_LOCK(%?)", &[SqlArg::String(lock_name.as_bytes())])
                .ok()
                .and_then(|rows| rows.first().and_then(|row| row.first()).and_then(datum_i64))
                == Some(1)
            {
                released += 1;
            }
            if released != 1 {
                let invariant = format!(
                    "refresh materialized view: advisory lock cleanup invariant violated (lock={lock_name} released={released})"
                );
                return match locked {
                    Ok(_) => Err(plain(invariant)),
                    Err(error) => Err(annotate(&error, &invariant)),
                };
            }
            locked
        })();
        let _ = set_session_vars(&mut refresh, &restore);
        if !original_db.is_empty() {
            let _ = sys_sql(&mut refresh, "USE %n", &[SqlArg::String(original_db.as_bytes())]);
        }
        outcome
    }

    /// Go master's checks under the advisory lock: the view read again from
    /// the latest schema (`TableByID`) and IN PLACE refused for a view that
    /// gained a log. Go first waits for this node's schema to reach the
    /// store's version (`ensureMVRefreshSchemaVersion`), which this port does
    /// not do.
    fn recheck_refresh_target_under_lock(
        &self,
        schema: &str,
        name: &str,
        mview_id: i64,
        mode: RefreshMaterializedViewMode,
    ) -> Result<(), DriverError> {
        let catalog = self.refresh_latest_info_schema();
        let Some(current) = catalog.table_info_by_id(mview_id) else {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[schema.to_owned().into(), name.to_owned().into()]),
            )));
        };
        if mode == RefreshMaterializedViewMode::CompleteInPlace
            && current.read().materialized_view_base.as_ref().is_some_and(|base| base.read().mlog_id != 0)
        {
            return Err(plain(format!(
                "refresh materialized view complete IN PLACE is not supported for materialized view {schema}.{name} with a materialized view log"
            )));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn refresh_complete_in_place_locked(
        &mut self,
        refresh: &mut Session,
        schema: &str,
        name: &str,
        mview_id: i64,
        mview: &tidb_model::table::MaterializedViewInfo,
        method: &str,
        refresh_start: chrono::DateTime<chrono::Local>,
        refresh_job_id: &mut u64,
        hist_inserted: &mut bool,
    ) -> Result<StmtOutput, DriverError> {
        sys_sql(refresh, "BEGIN PESSIMISTIC", &[])?;
        let mut txn_finished = false;
        let result = (|| {
            let locked = lock_refresh_info_row(refresh, mview_id)?;
            let mut hist = self.open_refresh_system_session()?;
            if read_refresh_source_ids(refresh, mview_id)?.is_empty() {
                return Err(plain("refresh materialized view: input log records are missing"));
            }
            // Go `refreshSctx.Txn(true).StartTS()`: the lock above activated
            // the transaction.
            let start_ts = refresh
                .node_conn()
                .explicit_start_ts()
                .filter(|&ts| ts != 0)
                .ok_or_else(|| plain("refresh materialized view: invalid transaction start tso"))?;
            *refresh_job_id = start_ts;
            insert_refresh_hist_running(&mut hist, start_ts, mview_id, schema, name, method, &hist_time(refresh_start))?;
            *hist_inserted = true;
            let finalize_failure = |hist: &mut Session, refresh: &mut Session, txn_finished: &mut bool, error: DriverError| {
                let mut reason = driver_message(&error);
                let mut rollback_error = None;
                if !*txn_finished {
                    if let Err(rollback) = sys_sql(refresh, "ROLLBACK", &[]) {
                        reason = format!("{reason}; rollback error: {}", driver_message(&rollback));
                        rollback_error = Some(rollback);
                    }
                    *txn_finished = true;
                }
                report_mv_refresh_failed(hist, mview.alert_refresh_failed, mview_id, schema, name);
                let finalized = finalize_refresh_hist_with_retry(
                    hist,
                    start_ts,
                    mview_id,
                    REFRESH_HIST_STATUS_FAILED,
                    None,
                    None,
                    refresh_start,
                    chrono::Local::now(),
                    Some(&reason),
                );
                if let Err(hist_error) = finalized {
                    let context = match &rollback_error {
                        Some(rollback) => format!(
                            "refresh materialized view: rollback failed ({}) and failed to finalize refresh history after error {}",
                            driver_message(rollback),
                            driver_message(&error)
                        ),
                        None => format!(
                            "refresh materialized view: failed to finalize refresh history after error {}",
                            driver_message(&error)
                        ),
                    };
                    return annotate(&hist_error, &context);
                }
                if let Some(rollback) = rollback_error {
                    return annotate(
                        &rollback,
                        &format!("refresh materialized view: rollback failed after error {}", driver_message(&error)),
                    );
                }
                error
            };
            // Go master `executeRefreshMaterializedViewCompleteInPlace`.
            let data = (|| {
                let view_args = [SqlArg::String(schema.as_bytes()), SqlArg::String(name.as_bytes())];
                sys_sql(refresh, "DELETE FROM %n.%n", &view_args)?;
                // Go `StmtCtx.AffectedRows()` of each statement.
                let delete_rows = i64::try_from(refresh.affected_rows()).unwrap_or(i64::MAX);
                let prefix = tidb_sqlescape::escape_sql_text("INSERT INTO %n.%n ", &view_args)
                    .map_err(|error| plain(error.to_string()))?;
                sys_sql(refresh, &format!("{prefix}{}", mview.sql_content), &[])?;
                let insert_rows = i64::try_from(refresh.affected_rows()).unwrap_or(i64::MAX);
                let read_tso = session_json_u64(refresh, "tidb_last_query_info", "for_update_ts")
                    .ok_or_else(|| plain("refresh materialized view: invalid refresh read tso"))?;
                Ok::<_, DriverError>((delete_rows, insert_rows, read_tso))
            })();
            let (delete_rows, insert_rows, read_tso) = match data {
                Ok(data) => data,
                Err(error) => return Err(finalize_failure(&mut hist, refresh, &mut txn_finished, error)),
            };
            if let Some(value) =
                tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges")
            {
                let message = value
                    .as_string()
                    .map_or_else(|| "mock refresh materialized view error after data changes".to_owned(), str::to_owned);
                return Err(finalize_failure(&mut hist, refresh, &mut txn_finished, plain(message)));
            }
            // Go master `resolveMVCompleteRefreshSources` and the source
            // baseline replaced when the logs changed.
            let persisted = (|| {
                let confirmed = refresh_source_log_ids(&self.resolve_mv_complete_refresh_sources(mview_id, read_tso)?);
                let registered = read_refresh_source_ids(refresh, mview_id)?;
                if registered != confirmed {
                    replace_refresh_source_ids(refresh, mview_id, &confirmed)?;
                }
                persist_refresh_success(refresh, mview_id, locked, read_tso, chrono::Utc::now().timestamp())?;
                sys_sql(refresh, "COMMIT", &[]).map(|_| ())
            })();
            if let Err(error) = persisted {
                return Err(finalize_failure(&mut hist, refresh, &mut txn_finished, error));
            }
            txn_finished = true;
            let commit_tso = session_json_u64(refresh, "tidb_last_txn_info", "commit_ts");
            if commit_tso.is_none() {
                append_cause_warning(
                    self,
                    &annotate(
                        &plain("refresh materialized view: last transaction info is empty"),
                        "refresh materialized view: refresh committed but failed to capture refresh commit tso",
                    ),
                );
            }
            // Go `finalizeSuccess`.
            if let Err(error) = finalize_refresh_hist_with_retry(
                &mut hist,
                start_ts,
                mview_id,
                REFRESH_HIST_STATUS_SUCCESS,
                Some(read_tso),
                commit_tso,
                refresh_start,
                chrono::Local::now(),
                None,
            ) {
                append_cause_warning(
                    self,
                    &annotate(&error, "refresh materialized view: refresh committed but failed to finalize refresh history"),
                );
            }
            if let Err(error) = delete_refresh_alert_state(&mut hist, mview_id) {
                append_cause_warning(
                    self,
                    &annotate(&error, "refresh materialized view: refresh committed but failed to delete refresh alert"),
                );
            }
            // Go `newMVRefreshStmtResultFromWriteCounts`.
            self.statement_message = format!("Rows inserted: {insert_rows}  Updated: 0  Deleted: {delete_rows}");
            Ok(StmtOutput::Affected(u64::try_from(insert_rows + delete_rows).unwrap_or_default()))
        })();
        if !txn_finished {
            let _ = sys_sql(refresh, "ROLLBACK", &[]);
        }
        result
    }

    /// Go master `executeRefreshMaterializedView`'s COMPLETE OUT OF PLACE
    /// branch: the job ID from the store (`allocJobID`), the running history
    /// row, the shadow build and cutover, then the finalized history row.
    #[allow(clippy::too_many_arguments)]
    fn refresh_complete_out_of_place_locked(
        &mut self,
        refresh: &mut Session,
        sql: &str,
        schema: &str,
        info: &tidb_model::TableInfo,
        mview: &tidb_model::table::MaterializedViewInfo,
        method: &str,
        refresh_start: chrono::DateTime<chrono::Local>,
        refresh_job_id: &mut u64,
        hist_inserted: &mut bool,
    ) -> Result<StmtOutput, DriverError> {
        let (mview_id, name) = (info.id, info.name.original().to_owned());
        let expected = read_refresh_info_snapshot(refresh, mview_id)?;
        *refresh_job_id = session_current_tso(refresh)
            .map_err(|error| annotate(&error, "refresh materialized view: failed to allocate refresh job id"))?;
        let job_id = *refresh_job_id;
        let mut hist = self.open_refresh_system_session()?;
        insert_refresh_hist_running(&mut hist, job_id, mview_id, schema, &name, method, &hist_time(refresh_start))?;
        *hist_inserted = true;
        let build = self.execute_refresh_materialized_view_complete_out_of_place(refresh, sql, schema, info, mview, expected);
        let (build_read_tso, inserted) = match build {
            Ok(build) => build,
            Err(error) => {
                // Go `finalizeFailure`.
                let reason = driver_message(&error);
                report_mv_refresh_failed(&mut hist, mview.alert_refresh_failed, mview_id, schema, &name);
                if let Err(hist_error) = finalize_refresh_hist_with_retry(
                    &mut hist,
                    job_id,
                    mview_id,
                    REFRESH_HIST_STATUS_FAILED,
                    None,
                    None,
                    refresh_start,
                    chrono::Local::now(),
                    Some(&reason),
                ) {
                    return Err(annotate(
                        &hist_error,
                        &format!("refresh materialized view: failed to finalize refresh history after error {reason}"),
                    ));
                }
                return Err(error);
            }
        };
        // Go `finalizeSuccess`.
        if let Err(error) = finalize_refresh_hist_with_retry(
            &mut hist,
            job_id,
            mview_id,
            REFRESH_HIST_STATUS_SUCCESS,
            Some(build_read_tso),
            None,
            refresh_start,
            chrono::Local::now(),
            None,
        ) {
            append_cause_warning(
                self,
                &annotate(&error, "refresh materialized view: refresh committed but failed to finalize refresh history"),
            );
        }
        if let Err(error) = delete_refresh_alert_state(&mut hist, mview_id) {
            append_cause_warning(
                self,
                &annotate(&error, "refresh materialized view: refresh committed but failed to delete refresh alert"),
            );
        }
        // Go `newMVRefreshStmtResultFromWriteCounts(shadow load rows, 0, 0)`.
        self.statement_message = format!("Rows inserted: {inserted}  Updated: 0  Deleted: 0");
        Ok(StmtOutput::Affected(u64::try_from(inserted).unwrap_or_default()))
    }

    /// Go master `executeRefreshMaterializedViewCompleteOutOfPlace`: the
    /// shadow table (`CreateMaterializedViewShadowTable`), its build on its own
    /// system session (Go's TiKV store builds by IMPORT INTO; this port runs
    /// the INSERT path everywhere, as the initial build does), the build read
    /// TSO, the sources at that TSO and the cutover job; a failure after the
    /// shadow table exists drops it (`DropMaterializedViewShadowTable`).
    /// Answers the build read TSO and the rows loaded.
    fn execute_refresh_materialized_view_complete_out_of_place(
        &mut self,
        refresh: &mut Session,
        sql: &str,
        schema: &str,
        info: &tidb_model::TableInfo,
        mview: &tidb_model::table::MaterializedViewInfo,
        expected: RefreshInfoSnapshot,
    ) -> Result<(u64, i64), DriverError> {
        use tidb_executor::stmt_ctx_build::ResetContextOfStmt as _;
        let name = info.name.original().to_owned();
        // Go `domain.GetDomain(e.Ctx()).InfoSchema()`: the domain's latest.
        let mut build = self.open_refresh_system_session()?;
        let current = build.refresh_latest_info_schema().table_info_by_id(info.id).map(|table| table.read().clone());
        let Some(current) = current else {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[schema.to_owned().into(), name.clone().into()]),
            )));
        };
        if current.materialized_view_base.as_ref().is_some_and(|base| {
            let base = base.read();
            base.mlog_id != 0 || !base.mview_ids.is_empty()
        }) {
            return Err(plain(format!(
                "refresh materialized view complete OUT OF PLACE is not supported for materialized view {schema}.{name} with a materialized view log or dependent materialized view"
            )));
        }
        let ddl = self
            .domain_services()
            .map(|services| std::sync::Arc::clone(&services.ddl))
            .ok_or_else(|| DriverError::unsupported("the session has no DDL to refresh out of place with"))?;
        let vars = refresh_session_vars(self, mview);
        let names: Vec<String> = vars.iter().map(|(name, _)| name.clone()).collect();
        let original = read_session_vars(&mut build, &names)?;
        let restore: Vec<(String, String)> = names.into_iter().zip(original).collect();
        let shadow_name = format!(
            "{MV_REFRESH_SHADOW_TABLE_PREFIX}{}_{}",
            info.id,
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let mut shadow_needs_cleanup = false;
        let outcome = (|| {
            set_session_vars(&mut build, &vars)?;
            sys_sql(&mut build, "USE %n", &[SqlArg::String(schema.as_bytes())])?;
            // Go `buildMVRefreshOutOfPlaceShadowTableInfo`.
            let mut shadow = tidb_ddl_build::table_info_build::build_table_info_with_like(
                schema,
                &shadow_name,
                info,
                tidb_ast::CreateTableTemporary::None,
                false,
            )
            .map_err(|error| DriverError::Mysql(error.to_mysql_error()))?;
            shadow.materialized_view_shadow = Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewShadowInfo {
                source_mview_id: info.id,
                ..Default::default()
            }));
            ddl.create_materialized_view_shadow_table(info.db_id, schema, shadow, &refresh.ddl_statement_context())?;
            shadow_needs_cleanup = true;
            // Go `buildMVRefreshOutOfPlaceBuildSQL` (the INSERT path).
            if mview.sql_content.is_empty() {
                return Err(plain("refresh materialized view: invalid select sql"));
            }
            let prefix = tidb_sqlescape::escape_sql_text(
                "INSERT INTO %n.%n ",
                &[SqlArg::String(schema.as_bytes()), SqlArg::String(shadow_name.as_bytes())],
            )
            .map_err(|error| plain(error.to_string()))?;
            sys_sql(&mut build, &format!("{prefix}{}", mview.sql_content), &[])?;
            // Go `buildSessVars.StmtCtx.AffectedRows()` of the load.
            let inserted = i64::try_from(build.affected_rows()).unwrap_or(i64::MAX);
            // Go `getMVRefreshOutOfPlaceBuildReadTSO`.
            let build_read_tso = sys_sql(
                &mut build,
                "SELECT COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(@@tidb_last_query_info, '$.start_ts')) AS UNSIGNED), CAST(0 AS UNSIGNED))",
                &[],
            )?
            .first()
            .and_then(|row| row.first())
            .and_then(datum_u64)
            .ok_or_else(|| plain("refresh materialized view complete OUT OF PLACE: cannot fetch build read tso"))?;
            if build_read_tso == 0 {
                return Err(plain("refresh materialized view complete OUT OF PLACE: invalid build read tso"));
            }
            if let Some(value) = tidb_failpoint::eval(
                "github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewOutOfPlaceBuildErrorAfterLoadShadow",
            ) {
                return Err(plain(value.as_string().map_or_else(
                    || "mock refresh materialized view out-of-place build error after load shadow".to_owned(),
                    str::to_owned,
                )));
            }
            let sources = self.resolve_mv_complete_refresh_sources(info.id, build_read_tso)?;
            // Go `getMVRefreshOutOfPlaceShadowTableID`.
            // Go `buildSctx.GetLatestInfoSchema()`.
            let shadow_table_id = build
                .refresh_latest_info_schema()
                .table_info_by_name(schema, &shadow_name)
                .map(|(_, table)| table.read().id)
                .ok_or_else(|| plain("refresh materialized view complete OUT OF PLACE: cannot resolve shadow table id"))?;
            if shadow_table_id == 0 {
                return Err(plain("refresh materialized view complete OUT OF PLACE: invalid shadow table id"));
            }
            // A manual refresh keeps the view's schedule
            // (`deriveRuntimeMaterializedScheduleNextUnixSeconds` is for an
            // internal one).
            let args = tidb_model::RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs {
                old_mview_id: info.id,
                shadow_table_id,
                build_read_tso,
                build_sources: Some(sources),
                expected_old_mview_revision: Some(info.revision),
                expected_last_success_read_tso: expected.last_success_read_tso,
                expected_last_success_read_tso_null: expected.last_success_read_tso_null,
                next_refresh_unix_seconds: None,
                should_update_next_refresh_unix_seconds: false,
            };
            let context = self.ddl_statement_context().with_ddl_query(sql);
            ddl.refresh_materialized_view_complete_out_of_place_cutover(
                info.db_id,
                schema,
                &tidb_hack::go_to_lower(&name),
                args,
                &context,
            )?;
            shadow_needs_cleanup = false;
            Ok((build_read_tso, inserted))
        })();
        let outcome = match outcome {
            Err(error) if shadow_needs_cleanup => {
                let query = tidb_sqlescape::escape_sql_text(
                    "DROP TABLE IF EXISTS %n.%n",
                    &[SqlArg::String(schema.as_bytes()), SqlArg::String(shadow_name.as_bytes())],
                )
                .unwrap_or_default();
                let context = build.ddl_statement_context().with_ddl_query(query);
                match ddl.drop_materialized_view_shadow_table(schema, &shadow_name, &context) {
                    Ok(_) => Err(error),
                    Err(drop_error) => {
                        tidb_logutil::bg_logger().warn(
                            "failed to cleanup shadow table after out-of-place complete refresh error",
                            &[
                                tidb_log::Field::new("schema", tidb_log::Value::Str(schema.to_owned())),
                                tidb_log::Field::new("mview", tidb_log::Value::Str(name.clone())),
                                tidb_log::Field::new("shadowTable", tidb_log::Value::Str(shadow_name.clone())),
                                tidb_log::Field::new("error", tidb_log::Value::Str(driver_message(&drop_error))),
                            ],
                        );
                        Err(annotate(
                            &error,
                            &format!("cleanup shadow table {schema}.{shadow_name} failed: {}", driver_message(&drop_error)),
                        ))
                    }
                }
            }
            other => other,
        };
        let _ = set_session_vars(&mut build, &restore);
        outcome
    }

    /// Go master `resolveMVCompleteRefreshSources`: the view's sources and
    /// their logs in the schema as of `read_tso` (`GetSnapshotInfoSchema`,
    /// `mviewutil.ResolveRefreshSources`), sorted by table ID.
    fn resolve_mv_complete_refresh_sources(
        &self,
        mview_id: i64,
        read_tso: u64,
    ) -> Result<Vec<tidb_model::MViewRefreshSource>, DriverError> {
        let domain = self
            .domain_services()
            .and_then(|services| services.domain.upgrade())
            .ok_or_else(|| DriverError::unsupported("the session has no domain to read a snapshot schema from"))?;
        let catalog = crate::domain::cluster_session_node::get_snapshot_info_schema(&domain, read_tso)
            .map_err(|error| annotate(&error, "refresh materialized view: cannot load computation snapshot schema"))?;
        let mview = catalog
            .table_info_by_id(mview_id)
            .and_then(|table| table.read().materialized_view.clone())
            .ok_or_else(|| plain(format!("refresh materialized view: target {mview_id} is missing from the computation schema")))?;
        let mut sources = Vec::new();
        for &id in mview.read().base_table_ids.iter() {
            let source = catalog
                .table_info_by_id(id)
                .ok_or_else(|| plain(format!("refresh materialized view: source {id} is missing from the computation schema")))?;
            let source = source.read();
            sources.push(tidb_model::MViewRefreshSource {
                table_id: source.id,
                mlog_id: source.materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id),
            });
        }
        sources.sort_by_key(|source| source.table_id);
        if sources.is_empty() {
            return Err(plain("refresh materialized view: source set is empty"));
        }
        Ok(sources)
    }
}

/// Go master `mviewutil.RefreshSourceLogIDs`: the sorted, distinct log set.
fn refresh_source_log_ids(sources: &[tidb_model::MViewRefreshSource]) -> Vec<i64> {
    let mut ids: Vec<i64> = sources.iter().map(|source| source.mlog_id).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Go master `allocJobID`: the store's current version, read as a fresh
/// transaction's start TS.
pub(crate) fn session_current_tso(session: &mut Session) -> Result<u64, DriverError> {
    let version = session.node_conn().store_current_version().map_err(plain)?;
    if version == 0 {
        return Err(plain("invalid job id"));
    }
    Ok(version)
}
