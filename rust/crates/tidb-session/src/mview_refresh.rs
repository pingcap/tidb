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

use crate::mview_task::{read_hist_cancel_request, start_mv_task_monitor, MvTaskCancelController, MvTaskMonitor, MvTaskWatch};
use crate::Session;

const FP: &str = "github.com/pingcap/tidb/pkg/executor/";

/// Go master `mvRefreshAdvisoryLockTimeoutSec`.
const MV_REFRESH_ADVISORY_LOCK_TIMEOUT_SEC: i64 = 1;
/// Go master `refreshHistStatusRunning` / `Success` / `Failed`.
const REFRESH_HIST_STATUS_RUNNING: &str = "running";
const REFRESH_HIST_STATUS_SUCCESS: &str = "success";
const REFRESH_HIST_STATUS_FAILED: &str = "failed";

/// Go master `mvRefreshShadowTablePrefix`.
const MV_REFRESH_SHADOW_TABLE_PREFIX: &str = tidb_mviewutil::SHADOW_TABLE_PREFIX;
/// Go master `mvRefreshImportIntoStoreName`.
const MV_REFRESH_IMPORT_INTO_STORE_NAME: &str = "TiKV";

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
pub(crate) struct RefreshInfoSnapshot {
    pub(crate) last_success_read_tso: u64,
    pub(crate) last_success_read_tso_null: bool,
    /// Go `lastSuccessRefreshEndUnixSeconds` (`None` for its NULL).
    pub(crate) last_success_refresh_end_unix_seconds: Option<i64>,
}

impl RefreshInfoSnapshot {
    /// Go `refreshInfoSnapshot.previousSuccessTime`.
    fn previous_success_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        if let Some(seconds) = self.last_success_refresh_end_unix_seconds {
            return chrono::DateTime::from_timestamp(seconds, 0);
        }
        if !self.last_success_read_tso_null && self.last_success_read_tso > 0 {
            return chrono::DateTime::from_timestamp_millis((self.last_success_read_tso >> 18) as i64);
        }
        None
    }
}

/// Go master `buildRefreshScheduleDuration`: for an internal (scheduled)
/// refresh, its start less the previous success.
fn build_refresh_schedule_duration(
    is_internal_sql: bool,
    refresh_start: chrono::DateTime<chrono::Local>,
    info: &RefreshInfoSnapshot,
) -> Option<std::time::Duration> {
    if !is_internal_sql {
        return None;
    }
    let previous = info.previous_success_time()?;
    // "Defined as current success time - previous success time - current
    // refresh duration, which is equivalent to current refresh start time -
    // previous success time."
    (refresh_start.with_timezone(&chrono::Utc) - previous).to_std().ok()
}

/// Go master `observeMVRefreshScheduleDuration`.
fn observe_mv_refresh_schedule_duration(duration: Option<std::time::Duration>) {
    if let Some(duration) = duration {
        tidb_metrics::materialized_view::MV_SERVICE_REFRESH_SCHEDULE_DURATION_HISTOGRAM.observe(duration.as_secs_f64());
    }
}

/// Go master `deriveRuntimeMaterializedScheduleNextUnixSeconds`: an internal
/// refresh re-derives NEXT_REFRESH_UNIX_SECONDS from the view's NEXT
/// expression; a manual one keeps the schedule.
fn derive_runtime_materialized_schedule_next_unix_seconds(
    mview: &tidb_model::table::MaterializedViewInfo,
    is_internal_sql: bool,
) -> Result<(Option<i64>, bool), DriverError> {
    if !is_internal_sql {
        return Ok((None, false));
    }
    tidb_ddl_build::materialized_view::derive_materialized_schedule_next_unix_seconds(
        "",
        &mview.refresh_next,
        mview.refresh_schedule_sql_mode,
        chrono::Utc::now(),
    )
    .map_err(DriverError::Mysql)
}

/// Go master `refreshHistReadTSOOnFailure`: a failed AS OF refresh records
/// its target.
fn refresh_hist_read_tso_on_failure(s: &RefreshMaterializedViewStmt, target_refresh_read_tso: u64) -> Option<u64> {
    (s.as_of.is_some() && target_refresh_read_tso != 0).then_some(target_refresh_read_tso)
}

/// Go master `decodeRefreshInfoSnapshot` (the read TSO column).
fn decode_refresh_info_snapshot(row: &[Datum], read_tso_idx: usize) -> RefreshInfoSnapshot {
    let last_success_refresh_end_unix_seconds = row.get(read_tso_idx + 1).and_then(datum_i64);
    match row.get(read_tso_idx).and_then(datum_u64) {
        Some(read_tso) => RefreshInfoSnapshot {
            last_success_read_tso: read_tso,
            last_success_read_tso_null: false,
            last_success_refresh_end_unix_seconds,
        },
        None => RefreshInfoSnapshot { last_success_read_tso: 0, last_success_read_tso_null: true, last_success_refresh_end_unix_seconds },
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
pub(crate) fn read_refresh_info_snapshot(session: &mut Session, mview_id: i64) -> Result<RefreshInfoSnapshot, DriverError> {
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

/// Go master `persistRefreshSuccess`: an internal refresh also moves the
/// schedule.
fn persist_refresh_success(
    session: &mut Session,
    mview_id: i64,
    locked: RefreshInfoSnapshot,
    refresh_read_tso: u64,
    last_success_refresh_end_unix_seconds: i64,
    (next_refresh_unix_seconds, should_update_next_refresh_unix_seconds): (Option<i64>, bool),
) -> Result<(), DriverError> {
    let locked_read_tso = if locked.last_success_read_tso_null {
        SqlArg::Null
    } else {
        SqlArg::Unsigned(locked.last_success_read_tso)
    };
    let mut set_clauses = vec!["LAST_SUCCESS_READ_TSO = %?", "LAST_SUCCESS_REFRESH_END_UNIX_SECONDS = %?"];
    let mut args = vec![SqlArg::Unsigned(refresh_read_tso), SqlArg::Signed(last_success_refresh_end_unix_seconds)];
    if should_update_next_refresh_unix_seconds {
        set_clauses.push("NEXT_REFRESH_UNIX_SECONDS = %?");
        args.push(next_refresh_unix_seconds.map_or(SqlArg::Null, SqlArg::Signed));
    }
    args.push(SqlArg::Signed(mview_id));
    args.push(locked_read_tso);
    sys_sql(
        session,
        &format!(
            "UPDATE mysql.tidb_mview_refresh_info\nSET\n\t{}\nWHERE MVIEW_ID = %? AND LAST_SUCCESS_READ_TSO <=> %?",
            set_clauses.join(",\n\t")
        ),
        &args,
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
    read_tso: Option<u64>,
    reason: &str,
) -> Result<(), DriverError> {
    if tidb_failpoint::eval(&format!("{FP}mockInsertRefreshHistFailedError")).is_some_and(|value| value.as_bool() == Some(true)) {
        return Err(plain("mock insert failed refresh history error"));
    }
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
            read_tso.map_or(SqlArg::Null, SqlArg::Unsigned),
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
    refresh_rows: Option<i64>,
    start: chrono::DateTime<chrono::Local>,
    end: chrono::DateTime<chrono::Local>,
    schedule_duration: Option<std::time::Duration>,
    failed_reason: Option<&str>,
) -> Result<(), DriverError> {
    if tidb_failpoint::eval(&format!("{FP}mockFinalizeRefreshHistError")).is_some_and(|value| value.as_bool() == Some(true)) {
        return Err(plain("mock finalize refresh history error"));
    }
    let end_text = hist_time(end);
    // Go `formatDurationSeconds`.
    let schedule_duration = schedule_duration.map(|duration| {
        let micros = duration.as_micros();
        format!("{}.{:06}", micros / 1_000_000, micros % 1_000_000)
    });
    let duration = format_duration_seconds_between(start, end);
    sys_sql(
        session,
        "UPDATE mysql.tidb_mview_refresh_hist\nSET\n\tREFRESH_END_TIME = %?,\n\tREFRESH_STATUS = %?,\n\tREFRESH_ROWS = %?,\n\tREFRESH_DURATION_SEC = %?,\n\tREFRESH_SCHEDULE_DURATION_SEC = %?,\n\tREFRESH_READ_TSO = %?,\n\tREFRESH_COMMIT_TSO = %?,\n\tREFRESH_FAILED_REASON = %?\nWHERE REFRESH_JOB_ID = %?\n  AND MVIEW_ID = %?",
        &[
            SqlArg::String(end_text.as_bytes()),
            SqlArg::String(status.as_bytes()),
            refresh_rows.map_or(SqlArg::Null, SqlArg::Signed),
            SqlArg::String(duration.as_bytes()),
            schedule_duration.as_ref().map_or(SqlArg::Null, |duration| SqlArg::String(duration.as_bytes())),
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
    refresh_rows: Option<i64>,
    start: chrono::DateTime<chrono::Local>,
    end: chrono::DateTime<chrono::Local>,
    schedule_duration: Option<std::time::Duration>,
    failed_reason: Option<&str>,
) -> Result<(), DriverError> {
    let first = match finalize_refresh_hist(
        session,
        refresh_job_id,
        mview_id,
        status,
        read_tso,
        commit_tso,
        refresh_rows,
        start,
        end,
        schedule_duration,
        failed_reason,
    ) {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    finalize_refresh_hist(session, refresh_job_id, mview_id, status, read_tso, commit_tso, refresh_rows, start, end, schedule_duration, failed_reason)
        .map_err(|retry| annotate(&retry, &format!("first finalize attempt failed: {}", driver_message(&first))))
}

/// Go master's refresh `startMVTaskMonitor`: the cancel request read from
/// and the heartbeat written to the job's `tidb_mview_refresh_hist` row.
fn start_refresh_task_monitor(
    watch: Session,
    controller: &std::sync::Arc<MvTaskCancelController>,
    refresh_job_id: u64,
    mview_id: i64,
) -> Result<MvTaskMonitor, DriverError> {
    start_mv_task_monitor(
        watch,
        std::sync::Arc::clone(controller),
        format!("refresh-{refresh_job_id}"),
        // Go master `readRefreshHistCancelRequest`.
        Box::new(move |session: &mut Session| {
            read_hist_cancel_request(
                session,
                "SELECT CANCEL_REQUEST_TIME, CANCEL_REQUESTED_BY\nFROM mysql.tidb_mview_refresh_hist\nWHERE REFRESH_JOB_ID = %?\n  AND MVIEW_ID = %?",
                refresh_job_id,
                mview_id,
                "refresh materialized view: required system table mysql.tidb_mview_refresh_hist does not exist",
            )
        }),
        // Go master `updateRefreshHistHeartbeat`.
        Some(Box::new(move |session: &mut Session| {
            sys_sql(
                session,
                "UPDATE mysql.tidb_mview_refresh_hist\nSET LAST_HEARTBEAT_TIME = NOW(6)\nWHERE REFRESH_JOB_ID = %?\n  AND MVIEW_ID = %?\n  AND REFRESH_STATUS = 'running'",
                &[SqlArg::Unsigned(refresh_job_id), SqlArg::Signed(mview_id)],
            )
            .map(|_| ())
            .map_err(|error| if is_table_not_exists(&error) { refresh_hist_missing_table() } else { error })
        })),
    )
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

/// Go master `releaseMVRefreshAdvisoryLockFully`: how many times the lock
/// was released.
fn release_mv_refresh_advisory_lock_fully(refresh: &mut Session, lock_name: &str) -> i64 {
    if let Some(count) = tidb_failpoint::eval(&format!("{FP}mockReleaseMVRefreshAdvisoryLockFullyCount")).and_then(|value| value.as_int()) {
        return count;
    }
    let mut released = 0;
    while sys_sql(refresh, "SELECT RELEASE_LOCK(%?)", &[SqlArg::String(lock_name.as_bytes())])
        .ok()
        .and_then(|rows| rows.first().and_then(|row| row.first()).and_then(datum_i64))
        == Some(1)
    {
        released += 1;
    }
    released
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

/// `session` as Go's `sctx.GetSQLExecutor()` for `mviewutil`.
pub(crate) fn session_sql_exec(
    session: &mut Session,
) -> impl FnMut(&str, &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, DriverError> + '_ {
    move |sql, args| sys_sql(session, sql, args)
}

/// Go master `mviewutil.ReadRefreshSourceIDs`.
fn read_refresh_source_ids(session: &mut Session, mview_id: i64) -> Result<Vec<i64>, DriverError> {
    tidb_mviewutil::refresh_source::read_refresh_source_ids(&mut session_sql_exec(session), mview_id)
}

/// Go master `mviewutil.ReplaceRefreshSourceIDs`.
fn replace_refresh_source_ids(session: &mut Session, mview_id: i64, mlog_ids: &[i64]) -> Result<(), DriverError> {
    tidb_mviewutil::refresh_source::replace_refresh_source_ids(&mut session_sql_exec(session), mview_id, mlog_ids)
}

/// Go master `getRefreshReadTSOForSuccess`: MV refresh executes in a
/// pessimistic transaction and reads data at `for_update_ts`, which the
/// refresh metadata persists (`TxnCtx.GetForUpdateTS()` as the data-change
/// statement left it).
fn get_refresh_read_tso_for_success(refresh: &Session) -> Result<u64, DriverError> {
    match u64::try_from(refresh.current_for_update_ts().value()).unwrap_or(0) {
        0 => Err(plain("refresh materialized view: invalid refresh read tso")),
        read_tso => Ok(read_tso),
    }
}

/// The last statement's answer of `@@tidb_last_query_info` / `@@tidb_last_txn_info`.
fn session_json_u64(session: &mut Session, variable: &str, key: &str) -> Option<u64> {
    let rows = sys_sql(session, &format!("SELECT @@{variable}"), &[]).ok()?;
    let text = rows.first().and_then(|row| row.first()).map(datum_text)?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    value.get(key).and_then(serde_json::Value::as_u64).filter(|&ts| ts != 0)
}

use tidb_variable::mview_execution::MViewExecutionSessionVars;

/// Go `recover()` with `util.GetRecoverError`: a panic in `f` becomes its
/// error, the panic's value as text.
fn recover_refresh_panic<T>(f: impl FnOnce() -> Result<T, DriverError>) -> Result<T, DriverError> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(panic) => Err(plain(
            panic
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic".to_owned()),
        )),
    }
}

/// Go master `captureRefreshExecutionSessionVars`: the caller's
/// materialized view execution settings.
pub(crate) fn capture_refresh_execution_session_vars(user: &Session) -> MViewExecutionSessionVars {
    MViewExecutionSessionVars::capture(|name| user.vars.get_system(name).ok())
}

/// The settings `session` currently runs under (Go master
/// `CaptureAppliedMViewExecutionSessionVars`).
fn applied_execution_session_vars(session: &Session) -> MViewExecutionSessionVars {
    MViewExecutionSessionVars::capture_applied(|name| session.vars.get_system(name).ok())
}

/// Go master `ddl.applyMViewExecutionSessionVars`
/// (`variable.ApplyMViewExecutionSessionVarsWithConfig`): every setting of
/// `target` set on `session`, the maintenance memory quota and read engines
/// as its `tidb_mem_quota_query` / `tidb_isolation_read_engines`. A failure
/// restores the session and fails, or -- best effort -- keeps the session's
/// value. Answers the values to restore.
fn apply_mview_execution_session_vars(
    session: &mut Session,
    target: &MViewExecutionSessionVars,
    best_effort: bool,
) -> Result<Vec<(String, String)>, DriverError> {
    use tidb_vardef::tidb_vars;
    let assignments = target.assignments(tidb_vars::TIDB_MEM_QUOTA_QUERY, tidb_vars::TIDB_ISOLATION_READ_ENGINES);
    let names: Vec<String> = assignments.iter().map(|assignment| assignment.name.to_owned()).collect();
    let origin = read_session_vars(session, &names)?;
    let restore: Vec<(String, String)> = names.into_iter().zip(origin).collect();
    for assignment in &assignments {
        let error = match tidb_ddl::cluster_ddl::materialized_view::maybe_mock_mview_execution_session_var_apply_error(assignment.name) {
            Some(message) => Some(plain(message)),
            None => set_session_vars(session, &[(assignment.name.to_owned(), assignment.value.clone())]).err(),
        };
        let Some(error) = error else { continue };
        if !best_effort {
            restore_mview_execution_session_vars(session, &restore);
            return Err(annotate(&error, assignment.failure_message));
        }
        tidb_logutil::bg_logger().warn(
            "mv execution: failed to apply session var, fallback to current session value",
            &[
                tidb_log::Field::new("var", tidb_log::Value::Str(assignment.name.to_owned())),
                tidb_log::Field::new("value", tidb_log::Value::Str(assignment.value.clone())),
                tidb_log::Field::new("error", tidb_log::Value::Str(driver_message(&error))),
            ],
        );
    }
    Ok(restore)
}

/// Go master `restoreMViewExecutionSessionVars`: every setting back to its
/// origin, a failure logged.
fn restore_mview_execution_session_vars(session: &mut Session, origin: &[(String, String)]) {
    for (name, value) in origin {
        if let Err(error) = set_session_vars(session, &[(name.clone(), value.clone())]) {
            tidb_logutil::bg_logger().warn(
                "mv execution: failed to restore session var",
                &[
                    tidb_log::Field::new("var", tidb_log::Value::Str(name.clone())),
                    tidb_log::Field::new("origin", tidb_log::Value::Str(value.clone())),
                    tidb_log::Field::new("error", tidb_log::Value::Str(driver_message(&error))),
                ],
            );
        }
    }
}

/// Go master `applyRefreshExecutionSessionVars`: `target` applied to the
/// maintenance session -- best effort for an internal refresh, whose failure
/// falls back to the session's defaults -- then the applied TiFlash settings
/// reported. Answers the values to restore.
pub(crate) fn apply_refresh_execution_session_vars(
    session: &mut Session,
    target: &MViewExecutionSessionVars,
    best_effort: bool,
) -> Result<Vec<(String, String)>, DriverError> {
    let applied = if tidb_failpoint::eval(&format!("{FP}mockRefreshExecutionSessionVarsApplyError")).is_some() {
        Err(plain("mock refresh execution session vars apply error"))
    } else {
        apply_mview_execution_session_vars(session, target, best_effort)
    };
    let restore = match applied {
        Ok(restore) => restore,
        Err(error) if best_effort => {
            tidb_logutil::bg_logger().warn(
                "refresh materialized view: failed to apply execution session vars, fallback to internal session defaults",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(driver_message(&error)))],
            );
            return Ok(Vec::new());
        }
        Err(error) => return Err(error),
    };
    let applied = applied_execution_session_vars(session);
    inject_tiflash_session_vars_applied(
        "refreshMaterializedViewTiFlashSessionVarsApplied",
        "refreshMaterializedViewTiFlashSpillSessionVarsApplied",
        &applied,
    );
    Ok(restore)
}

/// Go's paired `...TiFlashSessionVarsApplied` / `...TiFlashSpillSessionVarsApplied`
/// InjectCalls with the session's applied settings.
pub(crate) fn inject_tiflash_session_vars_applied(vars_failpoint: &str, spill_failpoint: &str, applied: &MViewExecutionSessionVars) {
    tidb_failpoint::inject_call(
        &format!("{FP}{vars_failpoint}"),
        &mut (applied.tiflash_max_threads, applied.fine_grained_stream_count, applied.fine_grained_batch_size),
    );
    tidb_failpoint::inject_call(
        &format!("{FP}{spill_failpoint}"),
        &mut (
            applied.tiflash_max_bytes_before_ext_join,
            applied.tiflash_max_bytes_before_ext_agg,
            applied.tiflash_max_bytes_before_ext_sort,
            applied.tiflash_mem_quota_query_per_node,
            applied.tiflash_query_spill_ratio,
        ),
    );
}

/// Go master's refresh and out-of-place build session setup:
/// `applyRefreshExecutionSessionVars` of the caller's settings, the
/// applied-settings InjectCalls, then `ddl.InitMViewSession`: the
/// definition's variables, the view's schema and the caller's statement
/// resource group. Every variable's previous value lands in `restore` as it
/// is set; the session itself is discarded afterwards.
#[allow(clippy::too_many_arguments)]
fn init_refresh_session(
    session: &mut Session,
    target: &MViewExecutionSessionVars,
    is_internal_sql: bool,
    mview: &tidb_model::table::MaterializedViewInfo,
    schema: &str,
    resource_group: &str,
    out_of_place_build: bool,
    restore: &mut Vec<(String, String)>,
) -> Result<(), DriverError> {
    restore.extend(apply_refresh_execution_session_vars(session, target, is_internal_sql)?);
    let applied = applied_execution_session_vars(session);
    let (mem_quota, engines) = if out_of_place_build {
        ("mvMaintainMemQuotaAppliedOnRefreshOutOfPlaceBuildSession", "refreshMaterializedViewOutOfPlaceBuildIsolationReadEnginesApplied")
    } else {
        ("mvMaintainMemQuotaAppliedOnRefreshSession", "refreshMaterializedViewIsolationReadEnginesApplied")
    };
    tidb_failpoint::inject_call(&format!("{FP}{mem_quota}"), &mut (applied.maintain_mem_quota, target.maintain_mem_quota));
    tidb_failpoint::inject_call(
        &format!("{FP}{engines}"),
        &mut (applied.isolation_read_engines.clone(), target.isolation_read_engines.clone()),
    );
    if out_of_place_build {
        inject_tiflash_session_vars_applied(
            "refreshMaterializedViewOutOfPlaceBuildTiFlashSessionVarsApplied",
            "refreshMaterializedViewOutOfPlaceBuildTiFlashSpillSessionVarsApplied",
            &applied,
        );
        tidb_failpoint::inject_call(
            &format!("{FP}refreshMaterializedViewOutOfPlaceBuildImportSessionVarsApplied"),
            &mut (applied.import_threads, applied.import_disk_quota.clone()),
        );
    }
    // Go `ddl.InitMViewSession`.
    let init = init_mview_session_vars(mview);
    let names: Vec<String> = init.iter().map(|(name, _)| name.clone()).collect();
    let original = read_session_vars(session, &names)?;
    restore.extend(names.into_iter().zip(original));
    set_session_vars(session, &init)?;
    sys_sql(session, "USE %n", &[SqlArg::String(schema.as_bytes())])?;
    if !resource_group.is_empty() {
        session.vars.resource_group = resource_group.to_owned();
    }
    session.active_resource_group.clone_from(&session.vars.resource_group);
    tidb_failpoint::inject_call(&format!("{FP}refreshMaterializedViewAfterInitSession"), session);
    Ok(())
}

/// Go master `ddl.InitMViewSession`'s variables: materialized views on,
/// and the definition's SQL mode, time zone and division precision.
pub(crate) fn init_mview_session_vars(info: &tidb_model::table::MaterializedViewInfo) -> Vec<(String, String)> {
    let mut vars = vec![(tidb_vardef::tidb_vars::TIDB_MVIEW_ENABLE.to_owned(), "ON".to_owned())];
    vars.push(("sql_mode".to_owned(), tidb_ddl::cluster_ddl::materialized_view::format_sql_mode(info.definition_sql_mode)));
    vars.push((
        "time_zone".to_owned(),
        tidb_ddl::cluster_ddl::materialized_view::definition_time_zone_value(&info.definition_time_zone),
    ));
    vars.push(("div_precision_increment".to_owned(), info.definition_div_precision_increment.to_string()));
    vars
}

pub(crate) fn set_session_vars(session: &mut Session, vars: &[(String, String)]) -> Result<(), DriverError> {
    for (name, value) in vars {
        sys_sql(session, &format!("SET @@SESSION.{name} = %?"), &[SqlArg::String(value.as_bytes())])?;
    }
    Ok(())
}

pub(crate) fn read_session_vars(session: &mut Session, names: &[String]) -> Result<Vec<String>, DriverError> {
    let select = format!("SELECT {}", names.iter().map(|name| format!("@@SESSION.{name}")).collect::<Vec<_>>().join(", "));
    let rows = sys_sql(session, &select, &[])?;
    Ok(rows.first().map(|row| row.iter().map(datum_text).collect()).unwrap_or_default())
}

/// Go master `validateRefreshMaterializedViewStmt`: the mode and the history
/// row's `REFRESH_METHOD`.
pub(crate) fn validate_refresh_materialized_view_stmt(
    s: &RefreshMaterializedViewStmt,
    is_internal_sql: bool,
) -> Result<String, DriverError> {
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
    let origin = if is_internal_sql { "auto" } else { "manual" };
    Ok(format!("{method} {origin}"))
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
        // Go master `isInternalSQL`: the MV service runs its refreshes as
        // restricted SQL.
        let method = validate_refresh_materialized_view_stmt(s, self.vars.restricted_sql)?;
        if s.observe_type == RefreshMaterializedViewObserveType::DryRun {
            return self.refresh_materialized_view_dry_run(s);
        }
        if s.observe_type == RefreshMaterializedViewObserveType::Profile {
            return self.refresh_materialized_view_profile(s, sql);
        }
        // Go master `evaluateRefreshMaterializedViewTargetTSO`: FAST ... AS OF
        // TIMESTAMP bounds the window.
        let target_tso = match &s.as_of {
            Some(as_of) => {
                let target = self.calculate_as_of_ts_expr(as_of)?;
                self.validate_snapshot_read_ts(target)?;
                target
            }
            None => 0,
        };
        let refresh_start = chrono::Local::now();
        let (schema, info) = self.resolve_refresh_materialized_view_target(s)?;
        let mview = info.materialized_view.as_ref().expect("resolved as a view").read().clone();
        self.check_refresh_materialized_view_base_table_select(&self.refresh_latest_info_schema(), &mview)?;
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
        let controller = std::sync::Arc::new(MvTaskCancelController::default());
        // Go's deferred `recover()`: a panic fails the refresh with its value.
        let result = recover_refresh_panic(|| {
            self.refresh_complete(
                &controller,
                s.mode,
                target_tso,
                sql,
                &schema,
                &info,
                &mview,
                &method,
                refresh_start,
                &mut refresh_job_id,
                &mut hist_inserted,
            )
        });
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
        let (failed_reason, error) = controller.normalize_task_failure(error);
        let reason = failed_reason.unwrap_or_else(|| driver_message(&error));
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
            refresh_hist_read_tso_on_failure(s, target_tso),
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

    /// Go `domain.GetDomain(sctx).InfoSchema()`.
    pub(crate) fn refresh_latest_info_schema(&self) -> std::sync::Arc<tidb_infoschema::SessionExtendedInfoSchema> {
        if let Some(domain) = self.domain_services().and_then(|services| services.domain.upgrade()) {
            return domain.catalog.get_latest();
        }
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
    pub(crate) fn resolve_refresh_materialized_view_target(
        &self,
        s: &RefreshMaterializedViewStmt,
    ) -> Result<(String, tidb_model::TableInfo), DriverError> {
        self.resolve_mview_statement_target(&self.refresh_latest_info_schema(), &s.view_name, "refresh materialized view")
    }

    /// Go master `resolveRefreshMaterializedViewTarget` and
    /// `resolveCompareMaterializedViewTarget` over `catalog`: the view's
    /// schema and table, `op` prefixing the invalid-SQL error.
    pub(crate) fn resolve_mview_statement_target(
        &self,
        catalog: &tidb_infoschema::SessionExtendedInfoSchema,
        view_name: &[String],
        op: &str,
    ) -> Result<(String, tidb_model::TableInfo), DriverError> {
        let (schema, view) = match view_name {
            [schema, view] => (schema.clone(), view.clone()),
            [view] => {
                if self.current_database().is_empty() {
                    return Err(DriverError::Mysql(MysqlError::from_terror(
                        &tidb_plannererrors::ERR_NO_DB.gen_with_stack_by_args(&[]),
                    )));
                }
                (self.current_database().to_owned(), view.clone())
            }
            _ => return Err(plain(format!("{op}: missing view name"))),
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
            return Err(plain(format!("{op}: invalid select sql")));
        }
        // Go master `checkRefreshMaterializedViewReady`.
        let state = tidb_model::table::MaterializedViewInfo::get_init_build_state(Some(&info));
        if !state.is_ready() {
            return Err(plain(state.access_error_message(&format!("{schema}.{}", table.name.original()))));
        }
        Ok((schema, table))
    }

    /// Go master `checkRefreshMaterializedViewBaseTableSelect` over
    /// `catalog`: a user statement needs SELECT on every base table.
    pub(crate) fn check_refresh_materialized_view_base_table_select(
        &self,
        catalog: &tidb_infoschema::SessionExtendedInfoSchema,
        info: &tidb_model::table::MaterializedViewInfo,
    ) -> Result<(), DriverError> {
        if self.vars.in_mview_maintenance {
            if !self.vars.restricted_sql {
                return Err(DriverError::Mysql(MysqlError::from_terror(
                    &tidb_plannererrors::ERR_INTERNAL
                        .gen_with_stack("materialized view maintenance should only run in restricted SQL mode"),
                )));
            }
            return Ok(());
        }
        if self.vars.restricted_sql {
            return Ok(());
        }
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
        controller: &std::sync::Arc<MvTaskCancelController>,
        mode: RefreshMaterializedViewMode,
        target_tso: u64,
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
        if self.mv_refresh_observer.is_some() {
            // Go master `setMVRefreshObserveRedaction`: plans follow the
            // caller's redaction.
            refresh.vars.set_enable_redact_log(self.vars.enable_redact_log().to_owned());
        }
        // Go: the refresh session runs under the task's cancelable context.
        let refresh_watch = controller.watch_guard(&refresh);
        let target = capture_refresh_execution_session_vars(self);
        let is_internal_sql = self.vars.restricted_sql;
        let resource_group = self.active_resource_group().to_owned();
        let original_db = refresh.current_database().to_owned();
        let mut restore: Vec<(String, String)> = Vec::new();
        let outcome = (|| {
            init_refresh_session(&mut refresh, &target, is_internal_sql, mview, schema, &resource_group, false, &mut restore)?;
            // Go master `acquireMVRefreshAdvisoryLock`.
            let lock_name = tidb_mviewutil::refresh_advisory_lock_name(info.db_id, mview_id);
            let got = sys_sql(
                &mut refresh,
                "SELECT GET_LOCK(%?, %?)",
                &[SqlArg::String(lock_name.as_bytes()), SqlArg::Signed(MV_REFRESH_ADVISORY_LOCK_TIMEOUT_SEC)],
            )?;
            if got.first().and_then(|row| row.first()).and_then(datum_i64) != Some(1) {
                return Err(DriverError::Mysql(MysqlError::from_terror(
                    &tidb_exeerrors::ERR_MV_ADVISORY_LOCK_CONFLICT.fast_gen_by_args(&[format!(
                        "another refresh or materialized view log creation is running for materialized view {schema}.{name}, please retry later"
                    )
                    .into()]),
                )));
            }
            let locked = self.recheck_refresh_target_under_lock(schema, &name, mview_id, mode).and_then(|()| {
                // Go: CREATE publishes the input logs; a missing
                // registration is metadata corruption.
                if read_refresh_source_ids(&mut refresh, mview_id)?.is_empty() {
                    return Err(plain(format!(
                        "refresh materialized view: input log records are missing for {schema}.{name}"
                    )));
                }
                tidb_failpoint::inject_call(&format!("{FP}refreshMaterializedViewAfterAcquireAdvisoryLock"), &mut ());
                if let Some(value) = tidb_failpoint::eval(&format!("{FP}mockRefreshMaterializedViewErrorBeforeInsertHist")) {
                    return Err(plain(value.string()));
                }
                if mode == RefreshMaterializedViewMode::CompleteOutOfPlace {
                    return self.refresh_complete_out_of_place_locked(
                        controller,
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
                    mode,
                    target_tso,
                    controller,
                    &refresh_watch,
                    &mut refresh,
                    schema,
                    info,
                    mview,
                    method,
                    refresh_start,
                    refresh_job_id,
                    hist_inserted,
                )
            });
            // Go releases the lock through the session, outside the task's
            // canceled context.
            refresh_watch.reset();
            // Go master `releaseMVRefreshAdvisoryLockFully`: the lock is held
            // exactly once.
            let released = release_mv_refresh_advisory_lock_fully(&mut refresh, &lock_name);
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

    /// Go master's checks under the advisory lock: this node's schema brought
    /// up to every committed DDL the refresh depends on
    /// (`ensureMVRefreshSchemaVersion`), the view read again from the latest
    /// schema (`TableByID`) and IN PLACE refused for a view that gained a log.
    fn recheck_refresh_target_under_lock(
        &self,
        schema: &str,
        name: &str,
        mview_id: i64,
        mode: RefreshMaterializedViewMode,
    ) -> Result<(), DriverError> {
        // "Check schema freshness under the shared lock so refresh sees MLogs
        // committed by a previous DDL owner, even before normal schema
        // synchronization."
        if let Some(domain) = self.domain_services().and_then(|services| services.domain.upgrade()) {
            ensure_mv_refresh_schema_version(&domain, &|| self.refresh_latest_info_schema(), mview_id)?;
        }
        let catalog = self.refresh_latest_info_schema();
        let Some(current) = catalog.table_info_by_id(mview_id) else {
            return Err(DriverError::Mysql(MysqlError::from_terror(
                &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .gen_with_stack_by_args(&[schema.to_owned().into(), name.to_owned().into()]),
            )));
        };
        if mode == RefreshMaterializedViewMode::CompleteOutOfPlace {
            // "A resumed MLog DDL can hold the DDL scheduling claim while
            // waiting for this lock. Reject its intermediate schema before
            // submitting any shadow/cutover DDL, which would otherwise form a
            // circular wait."
            check_mv_refresh_out_of_place_mlog_schema(&catalog, schema, &current.read())?;
        }
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
        mode: RefreshMaterializedViewMode,
        target_tso: u64,
        controller: &std::sync::Arc<MvTaskCancelController>,
        refresh_watch: &MvTaskWatch,
        refresh: &mut Session,
        schema: &str,
        info: &tidb_model::TableInfo,
        mview: &tidb_model::table::MaterializedViewInfo,
        method: &str,
        refresh_start: chrono::DateTime<chrono::Local>,
        refresh_job_id: &mut u64,
        hist_inserted: &mut bool,
    ) -> Result<StmtOutput, DriverError> {
        self.observe_mv_refresh_step("TXN_BEGIN", || sys_sql(refresh, "BEGIN PESSIMISTIC", &[]))?;
        let mut txn_finished = false;
        tidb_failpoint::inject_call(&format!("{FP}refreshMaterializedViewAfterBegin"), &mut ());
        tidb_failpoint::inject(&format!("{FP}pauseRefreshMaterializedViewAfterBegin"), |_| {});
        let (name, mview_id) = (info.name.original(), info.id);
        let fast = mode == RefreshMaterializedViewMode::Fast;
        let result = (|| {
            let locked = self.observe_mv_refresh_step("LOCK_REFRESH_INFO", || lock_refresh_info_row(refresh, mview_id))?;
            let schedule_duration = build_refresh_schedule_duration(self.vars.restricted_sql, refresh_start, &locked);
            if fast && locked.last_success_read_tso_null {
                return Err(plain("refresh materialized view fast: LAST_SUCCESS_READ_TSO is NULL"));
            }
            if fast && target_tso > 0 && target_tso == locked.last_success_read_tso {
                // Go `newMVRefreshStmtResultFromWriteCounts(0, 0, 0)`: the
                // window is empty.
                self.statement_message = "Rows inserted: 0  Updated: 0  Deleted: 0".to_owned();
                return Ok(StmtOutput::Affected(0));
            }
            let mut hist = self.open_refresh_system_session()?;
            // Re-read the registration under the refresh-info row lock.
            let registered_logs = read_refresh_source_ids(refresh, mview_id)?;
            if registered_logs.is_empty() {
                return Err(plain("refresh materialized view: input log records are missing"));
            }
            let latest = self.refresh_latest_info_schema();
            if fast {
                check_mv_refresh_log_baseline(&latest, mview_id, &resolve_refresh_sources(&latest, mview_id)?, &registered_logs)?;
            }
            let bounded_fast = fast && target_tso > 0 && target_tso > locked.last_success_read_tso;
            // Go `checkFastRefreshMLogIntegrity`, on the history session so it
            // sees the latest committed purge state.
            let mlog_retained_lower_tso = if fast {
                check_fast_refresh_mlog_integrity(&mut hist, &latest, info, locked.last_success_read_tso)?
            } else {
                0
            };
            // Go `refreshSctx.Txn(true).StartTS()`: the lock above activated
            // the transaction.
            let start_ts = refresh
                .node_conn()
                .explicit_start_ts()
                .filter(|&ts| ts != 0)
                .ok_or_else(|| plain("refresh materialized view: invalid transaction start tso"))?;
            *refresh_job_id = start_ts;
            self.observe_mv_refresh_step("INSERT_HIST_RUNNING", || {
                insert_refresh_hist_running(&mut hist, start_ts, mview_id, schema, name, method, &hist_time(refresh_start))
            })?;
            *hist_inserted = true;
            let finalize_failure = |hist: &mut Session, refresh: &mut Session, txn_finished: &mut bool, error: DriverError| {
                let (failed_reason, error) = controller.normalize_task_failure(error);
                let mut reason = failed_reason.unwrap_or_else(|| driver_message(&error));
                let mut rollback_error = None;
                if !*txn_finished {
                    // Go `finalizeCtx`: the rollback runs uncanceled.
                    refresh_watch.reset();
                    if let Err(rollback) = sys_sql(refresh, "ROLLBACK", &[]) {
                        reason = format!("{reason}; rollback error: {}", driver_message(&rollback));
                        rollback_error = Some(rollback);
                    }
                    *txn_finished = true;
                }
                report_mv_refresh_failed(hist, mview.alert_refresh_failed, mview_id, schema, name);
                let finalized = self.observe_mv_refresh_step("FINALIZE_HIST", || {
                    finalize_refresh_hist_with_retry(
                        hist,
                        start_ts,
                        mview_id,
                        REFRESH_HIST_STATUS_FAILED,
                        // Go `refreshHistReadTSOOnFailure`: only an AS OF
                        // refresh has a target.
                        (target_tso != 0).then_some(target_tso),
                        None,
                        None,
                        refresh_start,
                        chrono::Local::now(),
                        None,
                        Some(&reason),
                    )
                });
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
            let monitor =
                self.open_refresh_system_session().and_then(|watch| start_refresh_task_monitor(watch, controller, start_ts, mview_id));
            // Go `defer stopTaskMonitor()`: dropped when the refresh returns.
            let _monitor = match monitor {
                Ok(monitor) => monitor,
                Err(error) => return Err(finalize_failure(&mut hist, refresh, &mut txn_finished, error)),
            };
            tidb_failpoint::inject_call(&format!("{FP}refreshMaterializedViewAfterInsertRefreshHistRunning"), &mut ());
            tidb_failpoint::inject(&format!("{FP}pauseRefreshMaterializedViewAfterInsertRefreshHistRunning"), |_| {});
            // Go master `executeRefreshMaterializedViewDataChanges`: the data
            // changes run in materialized view maintenance.
            let original_maintenance = std::mem::replace(&mut refresh.vars.in_mview_maintenance, true);
            // Go master `executeRefreshMaterializedViewCompleteDeltaApply`:
            // the internal `RefreshMaterializedViewImplementStmt` on the refresh
            // session; its OK packet is the statement's.
            if fast && target_tso > 0 && target_tso < locked.last_success_read_tso {
                let error = plain(format!(
                    "refresh materialized view fast as of timestamp: target tso {target_tso} is older than LAST_SUCCESS_READ_TSO {}",
                    locked.last_success_read_tso
                ));
                refresh.vars.in_mview_maintenance = original_maintenance;
                return Err(finalize_failure(&mut hist, refresh, &mut txn_finished, error));
            }
            let implemented = matches!(mode, RefreshMaterializedViewMode::Fast | RefreshMaterializedViewMode::CompleteDeltaApply);
            let delta_apply = implemented.then(|| {
                let (step, window) = if fast {
                    ("DATA_CHANGE_FAST_MERGE", (locked.last_success_read_tso, target_tso, mlog_retained_lower_tso, registered_logs.clone()))
                } else {
                    ("DATA_CHANGE_COMPLETE_DELTA_APPLY", (0, 0, 0, Vec::new()))
                };
                let implement = tidb_ast::Stmt::Admin(tidb_ast::NodeBox::new(tidb_ast::AdminStmt::RefreshMaterializedViewImplement(
                    Box::new(tidb_ast::RefreshMaterializedViewImplementStmt {
                        refresh_stmt: tidb_ast::RefreshMaterializedViewStmt {
                            view_name: vec![schema.to_owned(), name.to_owned()],
                            with_async_mode: false,
                            mode,
                            observe_type: RefreshMaterializedViewObserveType::None,
                            as_of: None,
                        },
                        last_successful_refresh_read_tso: window.0,
                        target_refresh_read_tso: window.1,
                        mlog_retained_lower_tso: window.2,
                        registered_mlog_ids: window.3,
                    }),
                )));
                let sql = implement.restore();
                let (affected_rows, _) = self.observe_mv_refresh_step(step, || {
                    refresh
                        .node_conn()
                        .run_ordinary_write(&sql, &implement)
                        .map_err(crate::domain::cluster_session_node::sql_query_error_to_driver)
                })?;
                self.emit_mv_refresh_step_plan_rows(step, refresh);
                // Go `collectFastRefreshMLogScanRows`, from the statement's plan.
                let refresh_rows = if fast { refresh.fast_refresh_mlog_scan_rows() } else { None };
                // Go `captureMVRefreshStmtResult`, before the next statement
                // resets the statement context.
                let message = refresh.statement_message().to_owned();
                let read_tso = get_refresh_read_tso_for_success(refresh)?;
                Ok::<_, DriverError>((affected_rows, message, read_tso, refresh_rows))
            });
            // Go master `executeRefreshMaterializedViewCompleteInPlace`.
            let mut in_place = || {
                let view_args = [SqlArg::String(schema.as_bytes()), SqlArg::String(name.as_bytes())];
                self.observe_mv_refresh_step("DATA_CHANGE_COMPLETE_DELETE", || sys_sql(refresh, "DELETE FROM %n.%n", &view_args))?;
                // Go `StmtCtx.AffectedRows()` of each statement.
                let delete_rows = i64::try_from(refresh.affected_rows()).unwrap_or(i64::MAX);
                self.emit_mv_refresh_step_plan_rows("DATA_CHANGE_COMPLETE_DELETE", refresh);
                let prefix = tidb_sqlescape::escape_sql_text("INSERT INTO %n.%n ", &view_args)
                    .map_err(|error| plain(error.to_string()))?;
                self.observe_mv_refresh_step("DATA_CHANGE_COMPLETE_INSERT", || {
                    sys_sql(refresh, &format!("{prefix}{}", mview.sql_content), &[])
                })?;
                let insert_rows = i64::try_from(refresh.affected_rows()).unwrap_or(i64::MAX);
                self.emit_mv_refresh_step_plan_rows("DATA_CHANGE_COMPLETE_INSERT", refresh);
                let read_tso = get_refresh_read_tso_for_success(refresh)?;
                // Go `newMVRefreshStmtResultFromWriteCounts`.
                Ok::<_, DriverError>((
                    u64::try_from(insert_rows + delete_rows).unwrap_or_default(),
                    format!("Rows inserted: {insert_rows}  Updated: 0  Deleted: {delete_rows}"),
                    read_tso,
                    None,
                ))
            };
            let data = match delta_apply {
                Some(applied) => applied,
                None => in_place(),
            };
            refresh.vars.in_mview_maintenance = original_maintenance;
            let (affected_rows, statement_message, actual_read_tso, refresh_rows) = match data {
                Ok(data) => data,
                Err(error) => return Err(finalize_failure(&mut hist, refresh, &mut txn_finished, error)),
            };
            let mut read_tso = actual_read_tso;
            if bounded_fast {
                if target_tso > actual_read_tso {
                    let error = plain(format!(
                        "refresh materialized view fast as of timestamp: target tso {target_tso} is newer than actual refresh read tso {actual_read_tso}"
                    ));
                    return Err(finalize_failure(&mut hist, refresh, &mut txn_finished, error));
                }
                read_tso = target_tso;
            }
            tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterDataChanges", &mut ());
            if let Some(value) =
                tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewErrorAfterDataChanges")
            {
                let message = value
                    .as_string()
                    .map_or_else(|| "mock refresh materialized view error after data changes".to_owned(), str::to_owned);
                return Err(finalize_failure(&mut hist, refresh, &mut txn_finished, plain(message)));
            }
            // Go master `deriveRuntimeMaterializedScheduleNextUnixSeconds`.
            let next_refresh = match derive_runtime_materialized_schedule_next_unix_seconds(mview, self.vars.restricted_sql) {
                Ok(next) => next,
                Err(error) => return Err(finalize_failure(&mut hist, refresh, &mut txn_finished, error)),
            };
            // Go master `resolveMVCompleteRefreshSources` and the source
            // baseline replaced when the logs changed.
            let persisted = (|| {
                self.observe_mv_refresh_step("PERSIST_REFRESH_INFO", || {
                    // FAST keeps its registered logs; a COMPLETE refresh
                    // re-registers the ones its read confirmed.
                    if !fast {
                        let confirmed = tidb_mviewutil::refresh_source::refresh_source_log_ids(&self.resolve_mv_complete_refresh_sources(mview_id, read_tso)?);
                        if registered_logs != confirmed {
                            replace_refresh_source_ids(refresh, mview_id, &confirmed)?;
                        }
                    }
                    tidb_failpoint::inject_call(
                        "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewAfterReplaceSourceInfo",
                        &mut read_tso.clone(),
                    );
                    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/executor/mockRefreshMaterializedViewAfterReplaceSourceInfoError").is_some() {
                        return Err(plain("mock refresh error after replacing source info"));
                    }
                    persist_refresh_success(refresh, mview_id, locked, read_tso, chrono::Utc::now().timestamp(), next_refresh)
                })?;
                self.observe_mv_refresh_step("TXN_COMMIT", || sys_sql(refresh, "COMMIT", &[]).map(|_| ()))
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
            if let Err(error) = self.observe_mv_refresh_step("FINALIZE_HIST", || {
                finalize_refresh_hist_with_retry(
                    &mut hist,
                    start_ts,
                    mview_id,
                    REFRESH_HIST_STATUS_SUCCESS,
                    Some(read_tso),
                    commit_tso,
                    refresh_rows,
                    refresh_start,
                    chrono::Local::now(),
                    schedule_duration,
                    None,
                )
            }) {
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
            // Go `captureMVRefreshStmtResult`.
            self.statement_message = statement_message;
            // Go `observeMVRefreshScheduleDuration`.
            observe_mv_refresh_schedule_duration(schedule_duration);
            Ok(StmtOutput::Affected(affected_rows))
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
        controller: &std::sync::Arc<MvTaskCancelController>,
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
        let schedule_duration = build_refresh_schedule_duration(self.vars.restricted_sql, refresh_start, &expected);
        *refresh_job_id = session_current_tso(refresh)
            .map_err(|error| annotate(&error, "refresh materialized view: failed to allocate refresh job id"))?;
        let job_id = *refresh_job_id;
        let mut hist = self.open_refresh_system_session()?;
        self.observe_mv_refresh_step("INSERT_HIST_RUNNING", || {
            insert_refresh_hist_running(&mut hist, job_id, mview_id, schema, &name, method, &hist_time(refresh_start))
        })?;
        *hist_inserted = true;
        let monitor = self
            .open_refresh_system_session()
            .and_then(|watch| start_refresh_task_monitor(watch, controller, job_id, mview_id));
        let build = monitor.and_then(|monitor| {
            tidb_failpoint::inject_call(&format!("{FP}refreshMaterializedViewAfterInsertRefreshHistRunning"), &mut ());
            tidb_failpoint::inject(&format!("{FP}pauseRefreshMaterializedViewAfterInsertRefreshHistRunning"), |_| {});
            let build =
                self.execute_refresh_materialized_view_complete_out_of_place(controller, refresh, sql, schema, info, mview, expected);
            // Go `defer stopTaskMonitor()`.
            drop(monitor);
            build
        });
        let (build_read_tso, inserted) = match build {
            Ok(build) => build,
            Err(error) => {
                // Go `finalizeFailure`.
                let (failed_reason, error) = controller.normalize_task_failure(error);
                let reason = failed_reason.unwrap_or_else(|| driver_message(&error));
                report_mv_refresh_failed(&mut hist, mview.alert_refresh_failed, mview_id, schema, &name);
                if let Err(hist_error) = self.observe_mv_refresh_step("FINALIZE_HIST", || {
                    finalize_refresh_hist_with_retry(
                        &mut hist,
                        job_id,
                        mview_id,
                        REFRESH_HIST_STATUS_FAILED,
                        None,
                        None,
                        None,
                        refresh_start,
                        chrono::Local::now(),
                        None,
                        Some(&reason),
                    )
                }) {
                    return Err(annotate(
                        &hist_error,
                        &format!("refresh materialized view: failed to finalize refresh history after error {reason}"),
                    ));
                }
                return Err(error);
            }
        };
        // Go `finalizeSuccess`.
        if let Err(error) = self.observe_mv_refresh_step("FINALIZE_HIST", || {
            finalize_refresh_hist_with_retry(
                &mut hist,
                job_id,
                mview_id,
                REFRESH_HIST_STATUS_SUCCESS,
                Some(build_read_tso),
                None,
                None,
                refresh_start,
                chrono::Local::now(),
                schedule_duration,
                None,
            )
        }) {
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
        // Go `observeMVRefreshScheduleDuration`.
        observe_mv_refresh_schedule_duration(schedule_duration);
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
        controller: &std::sync::Arc<MvTaskCancelController>,
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
        if self.mv_refresh_observer.is_some() {
            build.vars.set_enable_redact_log(self.vars.enable_redact_log().to_owned());
        }
        // Go: the build session runs under the task's cancelable context.
        let _build_watch = controller.watch_guard(&build);
        let ddl = self
            .domain_services()
            .map(|services| std::sync::Arc::clone(&services.ddl))
            .ok_or_else(|| DriverError::unsupported("the session has no DDL to refresh out of place with"))?;
        let target = capture_refresh_execution_session_vars(self);
        let is_internal_sql = self.vars.restricted_sql;
        let resource_group = self.active_resource_group().to_owned();
        let mut restore: Vec<(String, String)> = Vec::new();
        let shadow_name = format!(
            "{MV_REFRESH_SHADOW_TABLE_PREFIX}{}_{}",
            info.id,
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        );
        let mut shadow_needs_cleanup = false;
        // Go master: the build session runs in materialized view maintenance.
        let original_maintenance = std::mem::replace(&mut build.vars.in_mview_maintenance, true);
        // Go: a panic unwinding the build still drops the shadow (the deferred
        // cleanup), and the refresh reports it (`util.GetRecoverError`).
        let outcome = recover_refresh_panic(|| {
            init_refresh_session(&mut build, &target, is_internal_sql, mview, schema, &resource_group, true, &mut restore)?;
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
            self.observe_mv_refresh_step("OUT_OF_PLACE_CREATE_SHADOW", || {
                ddl.create_materialized_view_shadow_table(info.db_id, schema, shadow, &refresh.ddl_statement_context())
            })?;
            shadow_needs_cleanup = true;
            tidb_failpoint::inject_call(&format!("{FP}refreshMaterializedViewOutOfPlaceAfterCreateShadow"), &mut ());
            tidb_failpoint::inject(&format!("{FP}pauseRefreshMaterializedViewOutOfPlaceAfterCreateShadow"), |_| {});
            // Go `buildMVRefreshOutOfPlaceBuildSQL`: a TiKV store loads the
            // shadow by IMPORT INTO with the refresh's import options, any
            // other by INSERT INTO.
            if mview.sql_content.is_empty() {
                return Err(plain("refresh materialized view: invalid select sql"));
            }
            let store_name = ddl.store_name();
            let import_into = store_name == MV_REFRESH_IMPORT_INTO_STORE_NAME;
            tidb_logutil::bg_logger().info(
                "refresh materialized view complete out-of-place: choose shadow build method",
                &[
                    tidb_log::Field::new("schema", tidb_log::Value::Str(schema.to_owned())),
                    tidb_log::Field::new("mview", tidb_log::Value::Str(name.clone())),
                    tidb_log::Field::new("storeName", tidb_log::Value::Str(store_name.to_owned())),
                    tidb_log::Field::new(
                        "expectedStoreName",
                        tidb_log::Value::Str(MV_REFRESH_IMPORT_INTO_STORE_NAME.to_owned()),
                    ),
                    tidb_log::Field::new("caseSensitiveEqual", tidb_log::Value::Bool(import_into)),
                    tidb_log::Field::new(
                        "caseInsensitiveEqual",
                        tidb_log::Value::Bool(store_name.eq_ignore_ascii_case(MV_REFRESH_IMPORT_INTO_STORE_NAME)),
                    ),
                    tidb_log::Field::new(
                        "method",
                        tidb_log::Value::Str(if import_into { "import-into" } else { "insert-into" }.to_owned()),
                    ),
                ],
            );
            let (threads, disk_quota) = if import_into {
                let threads = self
                    .vars
                    .get_system(tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_THREADS)
                    .ok()
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(tidb_vardef::defaults::DEF_TIDB_MVIEW_MAINTAIN_IMPORT_THREADS);
                let disk_quota =
                    self.vars.get_system(tidb_vardef::tidb_vars::TIDB_MVIEW_MAINTAIN_IMPORT_DISK_QUOTA).unwrap_or_default();
                (threads, disk_quota)
            } else {
                (0, String::new())
            };
            let build_sql = build_mv_refresh_out_of_place_build_sql(
                schema,
                &shadow_name,
                &mview.sql_content,
                store_name,
                threads,
                &disk_quota,
            )?;
            self.observe_mv_refresh_step("OUT_OF_PLACE_LOAD_SHADOW", || sys_sql(&mut build, &build_sql, &[]))?;
            // Go `buildSessVars.StmtCtx.AffectedRows()` of the load.
            let inserted = i64::try_from(build.affected_rows()).unwrap_or(i64::MAX);
            self.emit_mv_refresh_step_plan_rows("OUT_OF_PLACE_LOAD_SHADOW", &mut build);
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
            tidb_failpoint::inject_call(
                "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewOutOfPlaceAfterBuildDataLoad",
                &mut build_read_tso.clone(),
            );
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
            // Go: an internal refresh re-derives the schedule
            // (`deriveRuntimeMaterializedScheduleNextUnixSeconds`); a manual
            // one keeps it.
            let (next_refresh_unix_seconds, should_update_next_refresh_unix_seconds) =
                derive_runtime_materialized_schedule_next_unix_seconds(mview, self.vars.restricted_sql)?;
            // The DDL executor fills the output log's IDs (Go
            // `RefreshMaterializedViewCompleteOutOfPlaceCutover`, submit).
            let args = tidb_model::RefreshMaterializedViewCompleteOutOfPlaceCutoverArgs {
                old_mlog_id: 0,
                new_mlog_id: 0,
                old_mview_id: info.id,
                shadow_table_id,
                build_read_tso,
                build_sources: Some(sources),
                expected_old_mview_revision: Some(info.revision),
                expected_last_success_read_tso: expected.last_success_read_tso,
                expected_last_success_read_tso_null: expected.last_success_read_tso_null,
                next_refresh_unix_seconds,
                should_update_next_refresh_unix_seconds,
            };
            let context = self.ddl_statement_context().with_ddl_query(sql);
            self.observe_mv_refresh_step("OUT_OF_PLACE_CUTOVER", || {
                ddl.refresh_materialized_view_complete_out_of_place_cutover(
                    info.db_id,
                    schema,
                    &tidb_hack::go_to_lower(&name),
                    args,
                    &context,
                )
            })?;
            shadow_needs_cleanup = false;
            Ok((build_read_tso, inserted))
        });
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
        build.vars.in_mview_maintenance = original_maintenance;
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
        resolve_refresh_sources(&self.computation_snapshot_info_schema(read_tso)?, mview_id)
    }

    /// Go master `buildRefreshMaterializedViewImplement`'s
    /// `FullUpdateSnapshot`: an AS OF refresh's MIN / MAX lookup reads the base
    /// table at the target -- the GC safe point checked
    /// (`gcutil.ValidateSnapshotWithGCSafePoint`), over the target's schema
    /// (`staleread.GetSessionSnapshotInfoSchema`) and its committed data alone.
    pub(crate) fn mview_full_update_snapshot_source(
        &self,
    ) -> impl Fn(&tidb_sessionctx::StmtContext, u64) -> Result<tidb_executor::driver::mview_delta_merge::FullUpdateSnapshot, DriverError>
           + use<> {
        let restricted = self.restricted_sql_executor();
        let domain = self.domain_services().and_then(|services| services.domain.upgrade());
        move |ctx, to_ts| {
            let restricted = restricted
                .as_ref()
                .ok_or_else(|| DriverError::unsupported("the session has no restricted SQL executor to read the GC safe point"))?;
            crate::gcutil::validate_snapshot(restricted.as_ref(), to_ts)?;
            let domain =
                domain.as_ref().ok_or_else(|| DriverError::unsupported("the session has no domain to read a snapshot from"))?;
            let catalog = crate::domain::cluster_session_node::get_snapshot_info_schema(domain, to_ts)
                .map_err(|error| annotate(&error, "refresh materialized view: cannot load computation snapshot schema"))?;
            let snapshot = domain
                .transactions
                .open_snapshot_at(to_ts, tidb_model::DEFAULT_RESOURCE_GROUP_NAME)
                .map_err(DriverError::unsupported)?;
            let store = ctx
                .txn_store()
                .and_then(|store| store.snapshot_reader_at(snapshot))
                .ok_or_else(|| DriverError::unsupported("the statement's store cannot read at a snapshot"))?;
            Ok(tidb_executor::driver::mview_delta_merge::FullUpdateSnapshot { catalog, ctx: ctx.clone().with_txn_store(store) })
        }
    }

    /// The schema at a refresh computation's read TSO (Go `snapshotIS`).
    fn computation_snapshot_info_schema(
        &self,
        read_tso: u64,
    ) -> Result<tidb_infoschema::SessionExtendedInfoSchema, DriverError> {
        let domain = self
            .domain_services()
            .and_then(|services| services.domain.upgrade())
            .ok_or_else(|| DriverError::unsupported("the session has no domain to read a snapshot schema from"))?;
        crate::domain::cluster_session_node::get_snapshot_info_schema(&domain, read_tso)
            .map_err(|error| annotate(&error, "refresh materialized view: cannot load computation snapshot schema"))
    }
}

/// Go master `mviewutil.ResolveRefreshSources`: the view's sources and their
/// logs in `catalog`, sorted by table ID.
pub(crate) fn resolve_refresh_sources(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    mview_id: i64,
) -> Result<Vec<tidb_model::MViewRefreshSource>, DriverError> {
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

/// Go master `allocJobID`: the store's current version, read as a fresh
/// transaction's start TS.
pub(crate) fn session_current_tso(session: &mut Session) -> Result<u64, DriverError> {
    let version = session.node_conn().store_current_version().map_err(plain)?;
    if version == 0 {
        return Err(plain("invalid job id"));
    }
    Ok(version)
}

impl Session {
    /// The statement context of the refresh session's internal
    /// `RefreshMaterializedViewImplementStmt`: Go's `ResetContextOfStmt`
    /// gives it the `default` branch's type flags (`WithIgnoreTruncateErr(true)`),
    /// not an INSERT / UPDATE / DELETE's, so a truncation in the view's
    /// predicates (`varchar_col != 0`) neither errors nor warns.
    fn refresh_implement_statement_context(&self) -> tidb_sessionctx::StmtContext {
        use tidb_executor::stmt_ctx_build::ResetContextOfStmt as _;
        self.statement_context_ignoring(true, false).with_truncate_level(tidb_exprctx::ErrorLevel::Ignore)
    }

    /// Go master `MViewCompleteDeltaApplyExec` for the refresh session's
    /// internal `RefreshMaterializedViewImplementStmt`: the view's diff rows
    /// applied, the statement's affected rows and message set.
    pub(crate) fn mview_complete_delta_apply(
        &mut self,
        apply: &tidb_planner::common_plans::MViewCompleteDeltaApply,
    ) -> Result<StmtOutput, DriverError> {
        use tidb_executor::stmt_ctx_build::ResetContextOfStmt as _;
        let current_db = self.current_database().to_owned();
        let ctx = self.refresh_implement_statement_context();
        let stats = self.with_staged_catalog(|catalog| {
            tidb_executor::driver::mview_delta_apply::run_mview_complete_delta_apply(apply, catalog, &current_db, &ctx)
        })?;
        self.statement_message = stats.stmt_message();
        Ok(StmtOutput::Affected(stats.affected_rows()))
    }

    /// Go master `MViewDeltaMergeAggExec` (with `buildMViewDeltaMerge`'s
    /// `validateMVFastRefreshInput`) for FAST refresh's internal
    /// `RefreshMaterializedViewImplementStmt`: the log window merged into the
    /// view, the statement's affected rows and message set.
    pub(crate) fn mview_delta_merge(
        &mut self,
        merge: &tidb_planner::common_plans::MViewDeltaMerge,
    ) -> Result<StmtOutput, DriverError> {
        use tidb_executor::stmt_ctx_build::ResetContextOfStmt as _;
        let current_db = self.current_database().to_owned();
        let ctx = self.refresh_implement_statement_context();
        let build = self.with_staged_catalog(|catalog| {
            tidb_executor::driver::mview_delta_merge::build_merge_source(merge, catalog, &ctx)
        })?;
        // Go `validateMVFastRefreshInput` at the statement's for-update TS.
        let read_tso = u64::try_from(self.current_for_update_ts().value()).unwrap_or_default();
        let snapshot_is = self.computation_snapshot_info_schema(read_tso)?;
        let sources = resolve_refresh_sources(&snapshot_is, build.mv_table_id)?;
        check_mv_refresh_log_baseline(&snapshot_is, build.mv_table_id, &sources, &merge.registered_mlog_ids)?;
        if !sources.iter().any(|source| source.table_id == build.base_table_id && source.mlog_id == build.mlog_table_id) {
            return Err(plain(
                "refresh materialized view: planned source/log binding differs from the computation snapshot; retry refresh",
            ));
        }
        // Go: the retained mlog window ends at the statement's for-update TS.
        let source = self.mview_full_update_snapshot_source();
        let full_update_snapshot = |to_ts: u64| source(&ctx, to_ts);
        let stats = self.with_staged_catalog(|catalog| {
            tidb_executor::driver::mview_delta_merge::run_mview_delta_merge(
                merge,
                &build,
                catalog,
                &current_db,
                &ctx,
                read_tso,
                &full_update_snapshot,
            )
        })?;
        self.statement_message = stats.stmt_message();
        Ok(StmtOutput::Affected(stats.affected_rows()))
    }

    /// Go `sessionctx.ValidateSnapshotReadTS` for a stale read: a read
    /// timestamp after the store's current one is refused.
    pub(crate) fn validate_snapshot_read_ts(&self, read_ts: u64) -> Result<(), DriverError> {
        let mut sys = self.open_refresh_system_session()?;
        let current = sys.node_conn().store_current_version().map_err(plain)?;
        if read_ts > current {
            return Err(plain(format!(
                "cannot set read timestamp to a future time, readTS: {read_ts}, currentTS: {current}"
            )));
        }
        Ok(())
    }
}

/// Go master `buildMVRefreshOutOfPlaceBuildSQL`: IMPORT INTO the shadow on a
/// TiKV store (`shouldUseImportIntoForMVRefreshOutOfPlace`), INSERT INTO
/// otherwise.
fn build_mv_refresh_out_of_place_build_sql(
    schema_name: &str,
    shadow_table_name: &str,
    select_sql: &str,
    store_name: &str,
    import_threads: i64,
    import_disk_quota: &str,
) -> Result<String, DriverError> {
    if select_sql.is_empty() {
        return Err(plain("refresh materialized view: invalid select sql"));
    }
    let target = [SqlArg::String(schema_name.as_bytes()), SqlArg::String(shadow_table_name.as_bytes())];
    if store_name == MV_REFRESH_IMPORT_INTO_STORE_NAME {
        let prefix =
            tidb_sqlescape::escape_sql_text("IMPORT INTO %n.%n FROM ", &target).map_err(|error| plain(error.to_string()))?;
        let options = tidb_ddl_build::materialized_view::build_mview_import_into_options(import_threads, import_disk_quota)
            .map_err(DriverError::Mysql)?;
        return Ok(format!("{prefix}({select_sql}) WITH {}", options.join(", ")));
    }
    let prefix = tidb_sqlescape::escape_sql_text("INSERT INTO %n.%n ", &target).map_err(|error| plain(error.to_string()))?;
    Ok(format!("{prefix}{select_sql}"))
}

#[cfg(test)]
mod out_of_place_build_sql_tests {
    /// Go master `materialized_view_refresh_internal_test.go`'s
    /// `buildMVRefreshOutOfPlaceBuildSQL` cases.
    #[test]
    fn test_build_mv_refresh_out_of_place_build_sql() {
        let select = "select a, count(1) from t group by a";
        let sql = super::build_mv_refresh_out_of_place_build_sql("test", "__mv_shadow_1", select, "TiKV", 12, "64gib").unwrap();
        assert!(sql.contains("IMPORT INTO `test`.`__mv_shadow_1` FROM ("), "{sql}");
        let options = tidb_ddl_build::materialized_view::build_mview_import_into_options(12, "64gib").unwrap();
        assert!(sql.contains(&format!("WITH {}", options.join(", "))), "{sql}");
        let sql = super::build_mv_refresh_out_of_place_build_sql("test", "__mv_shadow_1", select, "TiDB", 12, "64gib").unwrap();
        assert_eq!(sql, "INSERT INTO `test`.`__mv_shadow_1` select a, count(1) from t group by a");
    }
}

/// Go master `checkMVRefreshOutOfPlaceMLogSchema`: the view's own log must
/// have no column mid-change.
fn check_mv_refresh_out_of_place_mlog_schema(
    is: &tidb_infoschema::SessionExtendedInfoSchema,
    schema: &str,
    mview: &tidb_model::TableInfo,
) -> Result<(), DriverError> {
    let mlog_id = mview.materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id);
    if mlog_id == 0 {
        return Ok(());
    }
    let Some(mlog) = is.table_info_by_id(mlog_id) else {
        return Err(DriverError::Mysql(MysqlError::from_terror(
            &tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.gen_with_stack_by_args(&[
                schema.to_owned().into(),
                tidb_model::table::materialized_view_log_table_name(&mview.name).original().to_owned().into(),
            ]),
        )));
    };
    if mlog.read().columns.iter_deref().any(|column| column.read().state != tidb_model::SchemaState::PUBLIC) {
        return Err(DriverError::Mysql(MysqlError::from_terror(
            &tidb_exeerrors::ERR_MV_ADVISORY_LOCK_CONFLICT.fast_gen_by_args(&[format!(
                "cannot refresh materialized view {schema}.{} complete OUT OF PLACE: materialized view log schema change is in progress, please retry later",
                mview.name.original()
            )
            .into()]),
        )));
    }
    Ok(())
}

/// Go master `checkMVRefreshLogBaseline`. "Keep diagnostic name lookups on
/// error paths: every FAST refresh runs this check."
fn check_mv_refresh_log_baseline(
    is: &tidb_infoschema::SessionExtendedInfoSchema,
    mview_id: i64,
    sources: &[tidb_model::MViewRefreshSource],
    registered: &[i64],
) -> Result<(), DriverError> {
    // Go `is.TableItemByID(mviewID)`, whose names are empty when missing.
    let names = || -> [tidb_mysql::errors::FormatArg; 2] {
        let db = is.schema_name_by_table_id(mview_id).unwrap_or_default();
        let table = is.table_info_by_id(mview_id).map(|table| table.read().name.original().to_owned()).unwrap_or_default();
        [db.into(), table.into()]
    };
    if sources.iter().any(|source| source.mlog_id == 0) {
        return Err(DriverError::Mysql(MysqlError::from_terror(&tidb_exeerrors::ERR_MV_REFRESH_LOG_BASELINE_CHANGED.fast_gen(
            "refresh materialized view %s.%s fast: source has no materialized view log; cannot perform FAST refresh",
            &names(),
        ))));
    }
    if tidb_mviewutil::refresh_source::refresh_source_log_ids(sources) != registered {
        return Err(DriverError::Mysql(MysqlError::from_terror(
            &tidb_exeerrors::ERR_MV_REFRESH_LOG_BASELINE_CHANGED.fast_gen_by_args(&names()),
        )));
    }
    Ok(())
}

/// Go master `readMLogPurgeInfoLastPurgedTSO`.
fn read_mlog_purge_info_last_purged_tso(session: &mut Session, mlog_id: i64) -> Result<Option<u64>, DriverError> {
    let rows = sys_sql(session, "SELECT LAST_PURGED_TSO FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %?", &[SqlArg::Signed(mlog_id)])
        .map_err(|error| {
            if is_table_not_exists(&error) {
                plain("refresh materialized view: required system table mysql.tidb_mlog_purge_info does not exist")
            } else {
                error
            }
        })?;
    let Some(row) = rows.first() else {
        return Err(plain(format!("refresh materialized view: mlog purge info row missing for mlog id {mlog_id}")));
    };
    Ok(row.first().and_then(datum_u64))
}

/// Go master `readLatestHazardousMLogPurgeCutoffTSO`.
fn read_latest_hazardous_mlog_purge_cutoff_tso(session: &mut Session, mlog_id: i64) -> Result<Option<u64>, DriverError> {
    let rows = sys_sql(
        session,
        "SELECT PURGE_CUTOFF_TSO\nFROM mysql.tidb_mlog_purge_hist\nWHERE MLOG_ID = %?\n  AND (\n    PURGE_STATUS = %?\n    OR PURGE_STATUS = %?\n    OR PURGE_ROWS > 0\n  )\nORDER BY PURGE_JOB_ID DESC\nLIMIT 1",
        &[SqlArg::Signed(mlog_id), SqlArg::String(b"running"), SqlArg::String(b"orphaned")],
    )
    .map_err(|error| {
        if is_table_not_exists(&error) {
            plain("refresh materialized view: required system table mysql.tidb_mlog_purge_hist does not exist")
        } else {
            error
        }
    })?;
    Ok(rows.first().and_then(|row| row.first()).and_then(datum_u64))
}

/// Go master `resolveRefreshMaterializedViewLogInfo`: the source's log, from
/// the planner's FAST source resolution.
fn resolve_refresh_materialized_view_log_info(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    info: &tidb_model::TableInfo,
) -> Result<i64, DriverError> {
    let table_by_id = |id: i64| catalog.table_info_by_id(id).map(|table| table.read().clone());
    tidb_planner::mview::resolve_fast_refresh_source(info, &table_by_id)
        .map(|source| source.mlog_table_id)
        .map_err(|error| plain(format!("refresh materialized view fast: {error}")))
}

/// Go master `checkFastRefreshMLogIntegrity` (with
/// `resolveRefreshMaterializedViewLogInfo`): the log may not have been purged
/// past the last successful read TSO; answers the retained lower TSO.
pub(crate) fn check_fast_refresh_mlog_integrity(
    session: &mut Session,
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    info: &tidb_model::TableInfo,
    last_success_read_tso: u64,
) -> Result<u64, DriverError> {
    let mlog_id = resolve_refresh_materialized_view_log_info(catalog, info)?;
    let last_purged = read_mlog_purge_info_last_purged_tso(session, mlog_id)?;
    let hazard_cutoff = read_latest_hazardous_mlog_purge_cutoff_tso(session, mlog_id)?;
    let hazard_fence = last_purged.unwrap_or(0).max(hazard_cutoff.unwrap_or(0));
    if hazard_fence > last_success_read_tso {
        return Err(plain(format!(
            "refresh materialized view fast: materialized view log may have been purged beyond LAST_SUCCESS_READ_TSO (hazard tso {hazard_fence}, LAST_SUCCESS_READ_TSO {last_success_read_tso})"
        )));
    }
    Ok(hazard_fence)
}

impl Session {
    /// Go master `collectFastRefreshMLogScanRows`: the rows the FAST merge's
    /// one log scan read, from this session's last statement plan and its
    /// runtime stats.
    pub(crate) fn fast_refresh_mlog_scan_rows(&self) -> Option<i64> {
        let (plan, runtime) = {
            let cell = self.process_plan_info.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (cell.plan.clone()?, cell.runtime_stats_coll.clone()?)
        };
        let physical = plan.get::<tidb_planner_core_operator_physicalop::PhysicalPlan>()?;
        let tidb_planner_core_operator_physicalop::PhysicalPlan::Dml(root) = &*physical else {
            return None;
        };
        if root.go_operator != "MViewDeltaMerge" {
            return None;
        }
        let mut scan_ids = Vec::new();
        collect_mlog_scan_plan_ids(root.select_plan.as_deref()?, &mut scan_ids);
        if scan_ids.len() != 1 {
            return None;
        }
        let source = runtime.get::<std::sync::Arc<dyn tidb_execdetails::runtime_stats::ExecutorRuntimeStatsSource>>()?;
        source.cop_runtime_info(scan_ids[0]).map(|info| info.act_rows)
    }
}

/// Go master `collectMLogScanPlanIDs`: the plan ids of the scans of a
/// materialized view log, pushed-down plans included.
fn collect_mlog_scan_plan_ids(plan: &tidb_planner_core_operator_physicalop::PhysicalPlan, out: &mut Vec<i64>) {
    use tidb_planner_core_operator_physicalop::PhysicalPlan;
    match plan {
        PhysicalPlan::TableScan(scan) => {
            if scan.table.as_ref().is_some_and(|table| table.read().materialized_view_log.is_some()) {
                out.push(i64::from(plan.id()));
            }
        }
        PhysicalPlan::TableReader(reader) => {
            if let Some(pushed) = reader.table_plan.as_deref() {
                collect_mlog_scan_plan_ids(pushed, out);
            }
        }
        _ => {
            for child in plan.children() {
                collect_mlog_scan_plan_ids(child, out);
            }
        }
    }
}

/// Go `mvRefreshSchemaReloadMu`.
static MV_REFRESH_SCHEMA_RELOAD_MU: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Go `ensureMVRefreshSchemaVersion`: under the advisory lock, a refresh
/// reloads this node's schema when the store holds a committed DDL that
/// touches the view, its base tables or their logs (a log a previous DDL
/// owner committed before this node synchronized it).
fn ensure_mv_refresh_schema_version(
    domain: &tidb_domain_core::domain::Domain,
    latest_info_schema: &dyn Fn() -> std::sync::Arc<tidb_infoschema::SessionExtendedInfoSchema>,
    mview_id: i64,
) -> Result<(), DriverError> {
    let required_version = mv_refresh_required_schema_version(domain, &latest_info_schema(), mview_id)?;
    if latest_info_schema().schema_meta_version() >= required_version {
        return Ok(());
    }
    tidb_failpoint::inject_call(
        "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeSchemaReloadLock",
        &mut required_version.clone(),
    );
    let _guard = MV_REFRESH_SCHEMA_RELOAD_MU
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // "Another refresh or the background schema loader may have caught up
    // while this refresh was waiting."
    if latest_info_schema().schema_meta_version() >= required_version {
        return Ok(());
    }
    tidb_failpoint::inject_call(
        "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeSchemaReload",
        &mut required_version.clone(),
    );
    domain
        .ddl
        .reload_schema()
        .map_err(|error| plain(format!("refresh materialized view: failed to reload schema: {error}")))?;
    let current_version = latest_info_schema().schema_meta_version();
    if current_version < required_version {
        return Err(plain(format!(
            "refresh materialized view: schema version {current_version} is behind required version {required_version}"
        )));
    }
    Ok(())
}

/// Go `mvRefreshRequiredSchemaVersion`: the newest committed schema version
/// this refresh must see, or 0 when the node's schema already suffices.
fn mv_refresh_required_schema_version(
    domain: &tidb_domain_core::domain::Domain,
    current_is: &tidb_infoschema::SessionExtendedInfoSchema,
    mview_id: i64,
) -> Result<i64, DriverError> {
    let ver = domain
        .transactions
        .fresh_timestamp()
        .map_err(|error| plain(format!("refresh materialized view: failed to get schema-check timestamp: {error}")))?;
    let mut m = domain
        .get_snapshot_meta(ver)
        .map_err(|error| plain(format!("refresh materialized view: failed to get schema version: {error}")))?;
    let latest_version = tidb_infoschema::cluster_catalog::with_meta_reader(&mut m, |reader| reader.schema_version())
        .map_err(|error| plain(format!("refresh materialized view: failed to get schema version: {error}")))?;
    let current_version = current_is.schema_meta_version();
    if current_version >= latest_version {
        return Ok(0);
    }
    // "Use the same InfoSchema for the dependency IDs and the lower version
    // bound."
    tidb_failpoint::inject_call(
        "github.com/pingcap/tidb/pkg/executor/refreshMaterializedViewBeforeCollectRelatedTables",
        &mut (),
    );
    let table_ids = mv_refresh_related_table_ids(current_is, mview_id)?;
    let mut latest_committed_version = 0;
    for version in (current_version + 1..=latest_version).rev() {
        let diff = tidb_infoschema::cluster_catalog::with_meta_reader(&mut m, |reader| reader.schema_diff(version))
            .map_err(|error| {
                plain(format!(
                    "refresh materialized view: failed to get schema diff for version {version}: {error}"
                ))
            })?;
        // "DDL can reserve versions without committing schema diffs."
        let Some(diff) = diff else {
            continue;
        };
        latest_committed_version = latest_committed_version.max(version);
        // "Related DDL can introduce new dependencies absent from tableIDs,
        // so require the newest committed version, including subsequent
        // changes."
        if latest_version - current_version
            >= tidb_infoschema_issyncer::catalog_reload::LOAD_SCHEMA_DIFF_VERSION_GAP_THRESHOLD
            || mv_refresh_schema_diff_affects_tables(&diff, &table_ids)
        {
            return Ok(latest_committed_version);
        }
    }
    Ok(0)
}

/// Go `mvRefreshRelatedTableIDs`: the view, its log, its base tables and
/// their logs.
fn mv_refresh_related_table_ids(
    is: &tidb_infoschema::SessionExtendedInfoSchema,
    mview_id: i64,
) -> Result<std::collections::HashSet<i64>, DriverError> {
    let get_table_info = |id: i64| -> Result<tidb_model::GoShared<tidb_model::TableInfo>, DriverError> {
        let not_found = || plain(format!("refresh materialized view: related table ID {id} not found in infoschema"));
        let Some(v2) = is.v2() else {
            return is.table_info_by_id(id).ok_or_else(not_found);
        };
        let item = v2.table_item_by_id(id).ok_or_else(not_found)?;
        // "TableByName loads and caches missing table metadata in V2, while
        // preserving storage errors that TableByID hides behind a false
        // result."
        match is.try_get_in(item.db_name.original(), item.table_name.original()) {
            Ok(Some(tidb_infoschema::TableEntry::Kv(table))) => Ok(table.meta().clone()),
            Ok(_) => Err(not_found()),
            Err(error) => Err(plain(format!("refresh materialized view: failed to load related table: {error}"))),
        }
    };
    let mview = get_table_info(mview_id)?;
    let mview = mview.read();
    let base_table_ids: Vec<i64> = match &mview.materialized_view {
        Some(info) if !info.read().base_table_ids.is_empty() => info.read().base_table_ids.iter().copied().collect(),
        _ => {
            return Err(plain(format!(
                "refresh materialized view: invalid metadata for materialized view ID {mview_id}"
            )))
        }
    };
    let mut table_ids = std::collections::HashSet::from([mview_id]);
    if let Some(base) = &mview.materialized_view_base {
        let mlog_id = base.read().mlog_id;
        if mlog_id != 0 {
            table_ids.insert(mlog_id);
        }
    }
    for base_id in base_table_ids {
        table_ids.insert(base_id);
        let base = get_table_info(base_id)?;
        let mlog_id = base.read().materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id);
        if mlog_id == 0 {
            return Err(plain(format!(
                "refresh materialized view: source table ID {base_id} has no materialized view log"
            )));
        }
        table_ids.insert(mlog_id);
    }
    Ok(table_ids)
}

/// Go `mvRefreshSchemaDiffAffectsTables`.
fn mv_refresh_schema_diff_affects_tables(
    diff: &tidb_model::SchemaDiff,
    table_ids: &std::collections::HashSet<i64>,
) -> bool {
    // "Schema/global changes and indirect metadata changes cannot be excluded
    // by table ID. Batch CREATE TABLE is fully described by its affected
    // options."
    if diff.regenerate_schema_map
        || diff.read_table_from_meta
        || (diff.table_id <= 0 && diff.action_type != tidb_model::ActionType::ACTION_CREATE_TABLES)
    {
        return true;
    }
    if table_ids.contains(&diff.table_id) || table_ids.contains(&diff.old_table_id) {
        return true;
    }
    diff.affected_options.iter_handles().flatten().any(|opt| {
        let opt = opt.read();
        table_ids.contains(&opt.table_id) || table_ids.contains(&opt.old_table_id)
    })
}
