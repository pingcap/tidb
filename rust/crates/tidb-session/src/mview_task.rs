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

//! Go master `pkg/executor/mview_task.go`: what REFRESH MATERIALIZED VIEW
//! and PURGE MATERIALIZED VIEW LOG share -- the task's cancel controller,
//! the monitor that polls the history row for a cancel request and writes
//! its heartbeat, and CANCEL MATERIALIZED VIEW ... JOB, which requests one.
//!
//! Go cancels the task's context; here the controller interrupts the task
//! sessions' statements through their `KILL QUERY` handles, which stop the
//! running statement and every later one on that session until the task
//! drops them -- the same reach a canceled context has.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_ast::{CancelMaterializedViewJobStmt, CancelMaterializedViewJobType};
use tidb_driver_error::DriverError;
use tidb_executor::sessionctx::StmtOutput;
use tidb_sqlescape::SqlArg;
use tidb_terror::MysqlError;

use crate::mview_refresh::{datum_i64, datum_text, is_table_not_exists, plain, sys_sql};
use crate::Session;

/// Go master `mvTaskMonitorPollInterval`.
const MV_TASK_MONITOR_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Go master `mvTaskHistHeartbeatInterval`.
const MV_TASK_HIST_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// Go master `errMVTaskCanceledManually`.
pub(crate) const ERR_MV_TASK_CANCELED_MANUALLY: &str = "materialized view task canceled manually";

const FP: &str = "github.com/pingcap/tidb/pkg/executor/";

/// Go master `getMVTaskMonitorPollInterval` / `getMVTaskHistHeartbeatInterval`:
/// the interval, or the failpoint's milliseconds.
fn interval(failpoint: &str, default: Duration) -> Duration {
    tidb_failpoint::eval(&format!("{FP}{failpoint}"))
        .and_then(|value| value.as_int())
        .and_then(|millis| u64::try_from(millis).ok())
        .map_or(default, Duration::from_millis)
}

/// Go master `mvTaskCancelController`: whether a manual cancel was
/// requested and by whom, and the task sessions' statement handles it
/// interrupts.
pub(crate) struct MvTaskCancelController {
    /// Go `ctx`: the task's context, canceled with its parent (the
    /// statement's) or by a manual cancel.
    ctx: tidb_gocontext::Context,
    cancel: tidb_gocontext::CancelFunc,
    state: Mutex<CancelState>,
}

impl Drop for MvTaskCancelController {
    /// Go's `defer taskCancelController.cancel()`.
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[derive(Default)]
struct CancelState {
    manual: bool,
    requester: String,
    handles: Vec<Arc<tidb_sqlkiller::StatementCancellation>>,
}

impl MvTaskCancelController {
    /// Go master `newMVTaskCancelController(parent)`.
    pub(crate) fn new(parent: &tidb_gocontext::Context) -> Self {
        let (ctx, cancel) = tidb_gocontext::Context::with_cancel(parent);
        Self { ctx, cancel, state: Mutex::default() }
    }

    /// Go master `context()`: the task's context (`kctx`).
    pub(crate) fn context(&self) -> &tidb_gocontext::Context {
        &self.ctx
    }

    /// A task session whose statements a cancel interrupts (Go: the session
    /// runs under the controller's context).
    pub(crate) fn watch(&self, session: &Session) -> Arc<tidb_sqlkiller::StatementCancellation> {
        let handle = Arc::new(session.begin_query_cancellation());
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.manual {
            handle.cancel();
        }
        state.handles.push(Arc::clone(&handle));
        handle
    }

    /// Go `kctx.Err()` on the task's cancelable context.
    pub(crate) fn err(&self) -> Result<(), DriverError> {
        match self.ctx.err() {
            Some(error) => Err(plain(error.to_string())),
            None => Ok(()),
        }
    }

    /// [`Self::watch`] for the guard's lifetime: dropping it resets the
    /// session's statements to uncanceled (Go's cleanup runs on
    /// `finalizeCtx`, outside the canceled context).
    pub(crate) fn watch_guard(&self, session: &Session) -> MvTaskWatch {
        MvTaskWatch(self.watch(session))
    }

    /// Go master `requestManualCancelByRequester`.
    fn request_manual_cancel_by_requester(&self, requester: &str) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.manual = true;
        if state.requester.is_empty() && !requester.is_empty() {
            state.requester = requester.to_owned();
        }
        for handle in &state.handles {
            handle.cancel();
        }
        drop(state);
        self.cancel.cancel();
    }

    /// Go master `normalizeTaskFailure`: a manually canceled task fails with
    /// `errMVTaskCanceledManually` and records who canceled it.
    pub(crate) fn normalize_task_failure(&self, error: DriverError) -> (Option<String>, DriverError) {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.manual {
            return (None, error);
        }
        (Some(format_mv_manual_cancel_failure_reason(&state.requester)), plain(ERR_MV_TASK_CANCELED_MANUALLY))
    }

    /// Go master `isManualCancelRequested`.
    pub(crate) fn is_manual_cancel_requested(&self) -> bool {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).manual
    }
}

/// A watched task session; dropping it resets the session's cancellation.
pub(crate) struct MvTaskWatch(Arc<tidb_sqlkiller::StatementCancellation>);

impl MvTaskWatch {
    /// Resets the session's statements to uncanceled now.
    pub(crate) fn reset(&self) {
        self.0.reset();
    }
}

impl Drop for MvTaskWatch {
    fn drop(&mut self) {
        self.0.reset();
    }
}

/// Go master `formatMVManualCancelFailureReason`.
fn format_mv_manual_cancel_failure_reason(requester: &str) -> String {
    if requester.is_empty() {
        "cancelled manually".to_owned()
    } else {
        format!("cancelled manually by {requester}")
    }
}

/// Go master `formatMVManualCancelRequester`.
fn format_mv_manual_cancel_requester(user: Option<(&str, &str)>) -> String {
    let Some((username, hostname)) = user else {
        return String::new();
    };
    if username.is_empty() && hostname.is_empty() {
        return String::new();
    }
    format!("'{}'@'{}'", username.replace('\'', "''"), hostname.replace('\'', "''"))
}

/// Go master `mvTaskCancelPoller`.
pub(crate) type MvTaskCancelPoller = Box<dyn Fn(&mut Session) -> Result<(bool, String), DriverError> + Send>;
/// Go master `mvTaskHeartbeatWriter`.
pub(crate) type MvTaskHeartbeatWriter = Box<dyn Fn(&mut Session) -> Result<(), DriverError> + Send>;

/// Go master `startMVTaskMonitor`'s stop function: stops the monitor and
/// waits for it.
pub(crate) struct MvTaskMonitor {
    stop: Option<std::sync::mpsc::Sender<()>>,
    done: Option<std::thread::JoinHandle<()>>,
}

impl MvTaskMonitor {
    fn stop_and_wait(&mut self) {
        drop(self.stop.take());
        if let Some(done) = self.done.take() {
            let _ = done.join();
        }
    }
}

impl Drop for MvTaskMonitor {
    fn drop(&mut self) {
        self.stop_and_wait();
    }
}

/// Go master `startMVTaskMonitor`: a system session of its own polls the
/// history row for a cancel request every poll interval and writes the
/// heartbeat every heartbeat interval, until the task stops it or a cancel
/// is requested.
pub(crate) fn start_mv_task_monitor(
    monitor_session: Session,
    controller: Arc<MvTaskCancelController>,
    monitor_name: String,
    poller: MvTaskCancelPoller,
    heartbeat_writer: Option<MvTaskHeartbeatWriter>,
) -> Result<MvTaskMonitor, DriverError> {
    let (stop, stopped) = std::sync::mpsc::channel::<()>();
    let mut session = monitor_session;
    let done = std::thread::Builder::new()
        .name(monitor_name.clone())
        .spawn(move || {
            let poll_interval = interval("mockMVTaskMonitorPollInterval", MV_TASK_MONITOR_POLL_INTERVAL);
            let heartbeat_interval = interval("mockMVTaskHistHeartbeatInterval", MV_TASK_HIST_HEARTBEAT_INTERVAL);
            let mut next_heartbeat_at = Instant::now() + heartbeat_interval;
            loop {
                if let Some(writer) = &heartbeat_writer {
                    if Instant::now() >= next_heartbeat_at {
                        let written = writer(&mut session);
                        next_heartbeat_at = Instant::now() + heartbeat_interval;
                        if let Err(error) = written {
                            if !matches!(stopped.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)) {
                                return;
                            }
                            warn("materialized view task heartbeat failed", &monitor_name, &error);
                        }
                    }
                }
                let polled = poller(&mut session);
                tidb_failpoint::inject_call(&format!("{FP}mvTaskMonitorPolled"), &mut monitor_name.clone());
                match polled {
                    Err(error) => {
                        if !matches!(stopped.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)) {
                            return;
                        }
                        warn("materialized view task monitor cancel poll failed", &monitor_name, &error);
                    }
                    Ok((true, requester)) => {
                        controller.request_manual_cancel_by_requester(&requester);
                        tidb_failpoint::inject_call(&format!("{FP}mvTaskCancelWatcherRequested"), &mut monitor_name.clone());
                        return;
                    }
                    Ok((false, _)) => {}
                }
                match stopped.recv_timeout(poll_interval) {
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    _ => return,
                }
            }
        })
        .map_err(|error| plain(error.to_string()))?;
    Ok(MvTaskMonitor { stop: Some(stop), done: Some(done) })
}

fn warn(msg: &str, monitor: &str, error: &DriverError) {
    tidb_logutil::bg_logger().warn(
        msg,
        &[
            tidb_log::Field::new("monitor", tidb_log::Value::Str(monitor.to_owned())),
            tidb_logutil::zap_error(crate::mview_refresh::driver_message(error)),
        ],
    );
}

/// Go master `readRefreshHistCancelRequest` / `readPurgeHistCancelRequest`.
pub(crate) fn read_hist_cancel_request(
    session: &mut Session,
    sql: &str,
    job_id: u64,
    object_id: i64,
    missing_table: &str,
) -> Result<(bool, String), DriverError> {
    let rows = sys_sql(session, sql, &[SqlArg::Unsigned(job_id), SqlArg::Signed(object_id)])
        .map_err(|error| if is_table_not_exists(&error) { plain(missing_table) } else { error })?;
    let Some(row) = rows.first() else {
        return Ok((false, String::new()));
    };
    if row.first().is_none_or(tidb_datatype::Datum::is_null) {
        return Ok((false, String::new()));
    }
    Ok((true, row.get(1).filter(|datum| !datum.is_null()).map(datum_text).unwrap_or_default()))
}

impl Session {
    /// Go master `CancelMaterializedViewJobExec.Next`.
    pub(crate) fn cancel_materialized_view_job(
        &mut self,
        stmt: &CancelMaterializedViewJobStmt,
    ) -> Result<StmtOutput, DriverError> {
        let requester = format_mv_manual_cancel_requester(self.current_identity());
        let mut sys = self.open_refresh_system_session()?;
        self.check_cancel_materialized_view_job_privilege(&mut sys, stmt)?;
        let job_id = u64::try_from(stmt.job_id).unwrap_or_default();
        let requester_arg = if requester.is_empty() { SqlArg::Null } else { SqlArg::String(requester.as_bytes()) };
        let sql = match stmt.tp {
            // Go master `requestRefreshHistCancel`.
            CancelMaterializedViewJobType::Refresh => "UPDATE mysql.tidb_mview_refresh_hist\nSET CANCEL_REQUEST_TIME = NOW(6),\n\tCANCEL_REQUESTED_BY = %?\nWHERE REFRESH_JOB_ID = %?\n  AND REFRESH_STATUS = 'running'\n  AND CANCEL_REQUEST_TIME IS NULL",
            // Go master `requestPurgeHistCancel`.
            CancelMaterializedViewJobType::LogPurge => "UPDATE mysql.tidb_mlog_purge_hist\nSET CANCEL_REQUEST_TIME = NOW(6), CANCEL_REQUESTED_BY = %?\nWHERE PURGE_JOB_ID = %? AND PURGE_STATUS = 'running' AND CANCEL_REQUEST_TIME IS NULL",
        };
        sys_sql(&mut sys, sql, &[requester_arg, SqlArg::Unsigned(job_id)])?;
        if sys.affected_rows() == 0 {
            return Err(cancel_materialized_view_job_user_error(stmt));
        }
        Ok(StmtOutput::Done(true))
    }

    /// Go master `checkCancelMaterializedViewJobPrivilege`: OPERATE VIEW on
    /// the running job's view (refresh) or log (purge).
    fn check_cancel_materialized_view_job_privilege(
        &self,
        sys: &mut Session,
        stmt: &CancelMaterializedViewJobStmt,
    ) -> Result<(), DriverError> {
        let Some((user, host)) = self.current_identity().map(|(user, host)| (user.to_owned(), host.to_owned())) else {
            return Ok(());
        };
        let catalog = self.refresh_latest_info_schema();
        let job_id = u64::try_from(stmt.job_id).unwrap_or_default();
        let (sql, missing) = match stmt.tp {
            CancelMaterializedViewJobType::Refresh => (
                "SELECT MVIEW_ID\nFROM mysql.tidb_mview_refresh_hist\nWHERE REFRESH_JOB_ID = %?\n  AND REFRESH_STATUS = 'running'",
                "refresh materialized view: required system table mysql.tidb_mview_refresh_hist does not exist",
            ),
            CancelMaterializedViewJobType::LogPurge => (
                "SELECT MLOG_ID\nFROM mysql.tidb_mlog_purge_hist\nWHERE PURGE_JOB_ID = %? AND PURGE_STATUS = 'running'",
                "required system table mysql.tidb_mlog_purge_hist does not exist",
            ),
        };
        let rows = sys_sql(sys, sql, &[SqlArg::Unsigned(job_id)])
            .map_err(|error| if is_table_not_exists(&error) { plain(missing) } else { error })?;
        let Some(id) = rows.first().and_then(|row| row.first()).and_then(datum_i64) else {
            return Err(cancel_materialized_view_job_user_error(stmt));
        };
        // Go master `resolveCancelRefreshJobPrivilegeTarget` /
        // `resolveCancelPurgeJobPrivilegeTarget`.
        let table = catalog.table_info_by_id(id);
        let (schema, name) = match stmt.tp {
            CancelMaterializedViewJobType::Refresh => {
                let Some(table) = table else {
                    return Err(plain(format!(
                        "refresh materialized view: cannot resolve target materialized view {id} for cancel job {job_id}"
                    )));
                };
                let Some(schema) = catalog.schema_name_by_table_id(id) else {
                    return Err(plain(format!("refresh materialized view: cannot resolve schema for materialized view {id}")));
                };
                let name = table.read().name.lowercase().to_owned();
                (schema, name)
            }
            CancelMaterializedViewJobType::LogPurge => {
                let Some(table) = table else {
                    return Err(plain(format!("cannot resolve materialized view log {id} for cancel job {job_id}")));
                };
                if table.read().materialized_view_log.is_none() {
                    return Err(plain(format!("table {id} is not a materialized view log")));
                }
                let Some(schema) = catalog.schema_name_by_table_id(id) else {
                    return Err(plain(format!("cannot resolve schema for materialized view log {id}")));
                };
                let name = table.read().name.lowercase().to_owned();
                (schema, name)
            }
        };
        if self.has_table_privilege(&tidb_hack::go_to_lower(&schema), &name, tidb_mysql::OperateViewPriv) {
            return Ok(());
        }
        Err(DriverError::Mysql(MysqlError::from_terror(&tidb_plannererrors::ERR_TABLEACCESS_DENIED.gen_with_stack_by_args(&[
            "OPERATE VIEW".into(),
            user.into(),
            host.into(),
            name.into(),
        ]))))
    }
}

/// Go master `cancelMaterializedViewJobUserError`.
fn cancel_materialized_view_job_user_error(stmt: &CancelMaterializedViewJobStmt) -> DriverError {
    match stmt.tp {
        CancelMaterializedViewJobType::Refresh => plain(format!("cannot cancel materialized view refresh job {}", stmt.job_id)),
        CancelMaterializedViewJobType::LogPurge => {
            plain(format!("cannot cancel materialized view log purge job {}", stmt.job_id))
        }
    }
}
