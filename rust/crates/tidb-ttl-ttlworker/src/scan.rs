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

//! Go `pkg/ttl/ttlworker/scan.go`.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::Sender;
use tidb_datatype::Datum;
use tidb_logutil::bg_logger;
use tidb_timer::go_time::{GoTime, MINUTE};
use tidb_ttl::cache::table::{PhysicalTable, TimeUnitType};
use tidb_ttl::cache::task::TTLTask;
use tidb_ttl::sql_builder::ScanQueryGenerator;
use tidb_ttl_client::context::Context;
use tidb_ttl_metrics::{
    new_scan_worker_phase_tracer, PhaseTracer, PhaseTracerExt, DELETE_ERROR_EXPIRED_ROWS,
    DELETE_SUCCESS_EXPIRED_ROWS, PHASE_DISPATCH, PHASE_IDLE, PHASE_OTHER, PHASE_WAIT_RETRY,
    SCANNED_EXPIRED_ROWS, SELECT_ERROR_DURATION, SELECT_SUCCESS_DURATION,
};

use crate::del::TtlDeleteTask;
use crate::session::{eval_expire_time, new_scan_session, with_session, SessionPool};
use crate::worker::{BaseWorker, Worker, WorkerStatus};
use crate::{field, int_field, session_now, Result, Row, Session};

/// Go `scanTaskExecuteSQLMaxRetry`.
pub static SCAN_TASK_EXECUTE_SQL_MAX_RETRY: AtomicUsize = AtomicUsize::new(5);
/// Go `scanTaskExecuteSQLRetryInterval`, in nanoseconds.
pub static SCAN_TASK_EXECUTE_SQL_RETRY_INTERVAL: AtomicU64 = AtomicU64::new(2_000_000_000);
/// Go `taskStartCheckErrorRateCnt`.
pub static TASK_START_CHECK_ERROR_RATE_CNT: AtomicU64 = AtomicU64::new(10000);
/// Go `taskMaxErrorRate`.
pub const TASK_MAX_ERROR_RATE: f64 = 0.4;

/// Go `ttlStatistics`.
#[derive(Debug, Default)]
pub struct TtlStatistics {
    /// Go `TotalRows`.
    pub total_rows: AtomicU64,
    /// Go `SuccessRows`.
    pub success_rows: AtomicU64,
    /// Go `ErrorRows`.
    pub error_rows: AtomicU64,
}

impl TtlStatistics {
    /// Go `IncTotalRows`.
    pub fn inc_total_rows(&self, cnt: usize) {
        SCANNED_EXPIRED_ROWS.inc_by(cnt as f64);
        self.total_rows.fetch_add(cnt as u64, Ordering::SeqCst);
    }

    /// Go `IncSuccessRows`.
    pub fn inc_success_rows(&self, cnt: usize) {
        DELETE_SUCCESS_EXPIRED_ROWS.inc_by(cnt as f64);
        self.success_rows.fetch_add(cnt as u64, Ordering::SeqCst);
    }

    /// Go `IncErrorRows`.
    pub fn inc_error_rows(&self, cnt: usize) {
        DELETE_ERROR_EXPIRED_ROWS.inc_by(cnt as f64);
        self.error_rows.fetch_add(cnt as u64, Ordering::SeqCst);
    }

    /// Go `Reset`.
    pub fn reset(&self) {
        self.success_rows.store(0, Ordering::SeqCst);
        self.error_rows.store(0, Ordering::SeqCst);
        self.total_rows.store(0, Ordering::SeqCst);
    }
}

impl std::fmt::Display for TtlStatistics {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "Total Rows: {}, Success Rows: {}, Error Rows: {}",
            self.total_rows.load(Ordering::SeqCst),
            self.success_rows.load(Ordering::SeqCst),
            self.error_rows.load(Ordering::SeqCst)
        )
    }
}

/// Go `cache.TTLTask` with its datetimes resolved in the session location,
/// as Go's `RowToTTLTask(loc, row)` returns them.
#[derive(Debug, Clone, Default)]
pub struct Task {
    /// The task row.
    pub task: TTLTask,
    /// Go `ExpireTime`.
    pub expire_time: GoTime,
    /// Go `OwnerHBTime`.
    pub owner_hb_time: GoTime,
}

impl Task {
    /// Go `cache.RowToTTLTask(loc, row)`.
    pub fn from_row(location: &tidb_timeutil::TimeZone, row: &Row) -> Result<Self> {
        let task = tidb_ttl::cache::task::row_to_ttl_task(row).map_err(|err| err.to_string())?;
        let resolve = |time: Option<tidb_datatype::Time>| {
            time.map_or_else(GoTime::zero, |time| crate::mysql_time_to_go(time, location))
        };
        Ok(Self {
            expire_time: resolve(task.expire_time),
            owner_hb_time: resolve(task.owner_hb_time),
            task,
        })
    }

    /// Go `JobID`.
    pub fn job_id(&self) -> &str {
        &self.task.job_id
    }

    /// Go `ScanID`.
    pub fn scan_id(&self) -> i64 {
        self.task.scan_id
    }

    /// Go `TableID`.
    pub fn table_id(&self) -> i64 {
        self.task.table_id
    }
}

/// Go `ttlScanTask`.
pub struct TtlScanTask {
    /// Go `ctx`.
    pub ctx: Context,
    /// Go's embedded `*cache.TTLTask`.
    pub task: Task,
    /// Go `tbl`.
    pub tbl: PhysicalTable,
    /// Go `statistics`.
    pub statistics: Arc<TtlStatistics>,
}

/// Go `TaskTerminateReason`: why the task is terminated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskTerminateReason {
    /// Go `ReasonTaskFinished`.
    TaskFinished,
    /// Go `ReasonError`.
    Error,
    /// Go `ReasonWorkerStop`: the scan worker stopped; the task should be
    /// rescheduled.
    WorkerStop,
}

impl TaskTerminateReason {
    /// The Go string value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TaskFinished => "finished",
            Self::Error => "error",
            Self::WorkerStop => "workerStop",
        }
    }
}

/// Go `ttlScanTaskExecResult`.
#[derive(Clone)]
pub struct TtlScanTaskExecResult {
    /// Go `time`.
    pub time: GoTime,
    /// Go `task`.
    pub task: Arc<TtlScanTask>,
    /// Go `err`.
    pub err: Option<String>,
    /// Go `reason`.
    pub reason: TaskTerminateReason,
}

/// Go `Location().String()`.
pub(crate) fn location_string(location: &tidb_timeutil::TimeZone) -> String {
    match location {
        tidb_timeutil::TimeZone::Local => "Local".to_owned(),
        tidb_timeutil::TimeZone::Named(zone) => zone.name().to_owned(),
        tidb_timeutil::TimeZone::Fixed { name, .. } => name.clone(),
    }
}

impl TtlScanTask {
    /// Go `result`.
    pub fn result(self: &Arc<Self>, err: Option<String>) -> TtlScanTaskExecResult {
        let reason = if err.is_some() {
            TaskTerminateReason::Error
        } else {
            TaskTerminateReason::TaskFinished
        };
        TtlScanTaskExecResult {
            time: GoTime::now(),
            task: Arc::clone(self),
            err,
            reason,
        }
    }

    /// Go `getDatumRows`: each row's key columns as datums.
    fn get_datum_rows(&self, rows: Vec<Row>) -> Vec<Vec<Datum>> {
        let width = self.tbl.key_column_types.len();
        rows.into_iter()
            .map(|row| {
                let mut datums = row.0;
                datums.truncate(width);
                datums
            })
            .collect()
    }

    /// Go `taskLogger`'s fields.
    pub fn task_log_fields(&self) -> Vec<tidb_log::Field> {
        vec![
            field("jobID", self.task.job_id()),
            int_field("scanID", self.task.scan_id()),
            int_field("tableID", self.task.table_id()),
            field("table", self.tbl.full_name()),
        ]
    }

    /// Go `doScan`.
    pub fn do_scan(
        self: &Arc<Self>,
        ctx: &Context,
        tracer: Option<&PhaseTracer>,
        del_ch: &Sender<TtlDeleteTask>,
        sess_pool: &dyn SessionPool,
    ) -> TtlScanTaskExecResult {
        let err = with_session(sess_pool, &mut |se| self.do_scan_with_session(ctx, tracer, del_ch, se)).err();
        self.result(err)
    }

    /// Go `doScanWithSession`.
    fn do_scan_with_session(
        &self,
        ctx: &Context,
        tracer: Option<&PhaseTracer>,
        del_ch: &Sender<TtlDeleteTask>,
        raw_sess: &Session,
    ) -> Result<()> {
        let task_ctx = &self.task_ctx();
        let restore_phase = tracer.phase();
        tracer.enter_phase(PHASE_OTHER);

        // Keep the SQL execution context canceled when either the worker or the TTL task stops.
        let (scan_ctx, cancel_scan_ctx) = Context::with_cancel(ctx);
        let (do_scan_finished, set_do_scan_finished) = Context::with_cancel(&Context::background());
        let result = std::thread::scope(|scope| {
            scope.spawn(|| {
                crossbeam_channel::select! {
                    recv(task_ctx.done()) -> _ => {}
                    recv(ctx.done()) -> _ => {}
                    recv(do_scan_finished.done()) -> _ => return,
                }
                cancel_scan_ctx.cancel();
                let fields = self.task_log_fields();
                bg_logger().info(
                    "kill the running statement in scan task because the task or worker cancelled",
                    &fields,
                );
                raw_sess.kill_stmt();
                let ticker = crossbeam_channel::tick(Duration::from_secs(60));
                loop {
                    // Have a small probability that the kill signal will be lost when the session is idle.
                    // So wait for a while and send the kill signal again if the scan is still running.
                    crossbeam_channel::select! {
                        recv(do_scan_finished.done()) -> _ => return,
                        recv(ticker) -> _ => {
                            bg_logger().warn(
                                "scan task is still running after the kill signal sent, kill it again",
                                &fields,
                            );
                            raw_sess.kill_stmt();
                        }
                    }
                }
            });
            let result = self.scan_rows(task_ctx, ctx, &scan_ctx, tracer, del_ch, raw_sess);
            set_do_scan_finished.cancel();
            result
        });
        cancel_scan_ctx.cancel();
        tracer.enter_phase(&restore_phase);
        result
    }

    fn task_ctx(&self) -> Context {
        self.ctx.clone()
    }

    /// The body of Go's `doScanWithSession` after the kill watcher starts.
    fn scan_rows(
        &self,
        task_ctx: &Context,
        ctx: &Context,
        scan_ctx: &Context,
        tracer: Option<&PhaseTracer>,
        del_ch: &Sender<TtlDeleteTask>,
        raw_sess: &Session,
    ) -> Result<()> {
        let now = session_now(raw_sess);
        let safe_expire = eval_expire_time(&self.tbl, raw_sess, &now)?.add(MINUTE);

        // Check the expired time to avoid to delete some unexpected rows.
        // It can happen that the table metas used by job and task is different. For example:
        // 	1. Job computes TTL expire time with expired interval 2 days
        //  2. User updates the expired interval to 1 day after job submitted
        //  3. A task deserialized and begins to execute. The expired time it uses is computed by job
        //     but table meta is the latest one that got from the information schema.
        // If we do not make this check, the scan task will continue to run without any error
        // because `ExecuteSQLWithCheck` only do checks when the table meta used by task is different with the latest one.
        // In this case, some rows will be deleted unexpectedly.
        if self.task.expire_time.after(&safe_expire) {
            let (interval_expr, interval_unit) = self
                .tbl
                .table_info
                .read()
                .ttl_info
                .as_ref()
                .map(|ttl| {
                    let ttl = ttl.read();
                    (ttl.interval_expr_str.clone(), ttl.interval_time_unit)
                })
                .unwrap_or_default();
            return Err(format!(
                "current expire time is after safe expire time. ({} > {}, expire expr: {} {}, now: {}, nowTZ: {})",
                self.task.expire_time.unix(),
                safe_expire.unix(),
                interval_expr,
                TimeUnitType::from_i64(interval_unit).as_str(),
                now.unix(),
                location_string(now.location()),
            ));
        }

        let (sess, restore_session) = new_scan_session(raw_sess, &self.tbl, &self.task.expire_time)?;
        let result = self.scan_with_generator(task_ctx, ctx, scan_ctx, tracer, del_ch, &sess);
        if let Err(err) = restore_session(raw_sess) {
            bg_logger().warn("encountered error", &[field("error", err)]);
        }
        result
    }

    fn scan_with_generator(
        &self,
        task_ctx: &Context,
        ctx: &Context,
        scan_ctx: &Context,
        tracer: Option<&PhaseTracer>,
        del_ch: &Sender<TtlDeleteTask>,
        sess: &crate::session::TableSession<'_>,
    ) -> Result<()> {
        let empty = Vec::new();
        let mut generator = ScanQueryGenerator::new(
            &self.tbl,
            self.task.expire_time.unix(),
            self.task.task.scan_range_start.as_ref().unwrap_or(&empty),
            self.task.task.scan_range_end.as_ref().unwrap_or(&empty),
        )
        .map_err(|err| err.to_string())?;

        let mut retry_sql = String::new();
        let mut retry_times = 0;
        let mut last_result: Vec<Vec<Datum>> = Vec::new();
        loop {
            if let Some(err) = task_ctx.err() {
                return Err(err.to_string());
            }
            if let Some(err) = ctx.err() {
                return Err(err.to_string());
            }

            let total = self.statistics.total_rows.load(Ordering::SeqCst);
            if total > TASK_START_CHECK_ERROR_RATE_CNT.load(Ordering::SeqCst)
                && self.statistics.error_rows.load(Ordering::SeqCst) as f64 > total as f64 * TASK_MAX_ERROR_RATE
            {
                return Err("error exceeds the limit".to_owned());
            }

            let mut sql = retry_sql.clone();
            if sql.is_empty() {
                let limit = tidb_vardef::TTL_SCAN_BATCH_SIZE.load(Ordering::SeqCst);
                sql = generator.next_sql(&last_result, limit).map_err(|err| err.to_string())?;
            }

            if sql.is_empty() {
                return Ok(());
            }

            let sql_start = std::time::Instant::now();
            let (rows, retryable, sql_err) = sess.execute_sql_with_check(tracer, &sql);
            let select_interval = sql_start.elapsed();
            if let Some(sql_err) = sql_err {
                SELECT_ERROR_DURATION.observe(select_interval.as_secs_f64());
                let need_retry = retryable
                    && retry_times < SCAN_TASK_EXECUTE_SQL_MAX_RETRY.load(Ordering::SeqCst)
                    && scan_ctx.err().is_none();
                bg_logger().warn(
                    "execute query for ttl scan task failed",
                    &[
                        field("SQL", sql.clone()),
                        int_field("retryTimes", retry_times as i64),
                        tidb_log::Field::new("needRetry", tidb_log::Value::Bool(need_retry)),
                        field("error", sql_err.clone()),
                    ],
                );

                if !need_retry {
                    return Err(sql_err);
                }
                retry_sql = sql;
                retry_times += 1;

                tracer.enter_phase(PHASE_WAIT_RETRY);
                let interval = Duration::from_nanos(SCAN_TASK_EXECUTE_SQL_RETRY_INTERVAL.load(Ordering::SeqCst));
                if scan_ctx.sleep(interval) {
                    return Err(scan_ctx.err().map(|err| err.to_string()).unwrap_or_default());
                }
                tracer.enter_phase(PHASE_OTHER);
                continue;
            }

            SELECT_SUCCESS_DURATION.observe(select_interval.as_secs_f64());
            retry_sql.clear();
            retry_times = 0;
            let row_count = rows.len();
            last_result = self.get_datum_rows(rows);
            if row_count == 0 {
                continue;
            }

            let del_task = TtlDeleteTask {
                job_id: self.task.job_id().to_owned(),
                scan_id: self.task.scan_id(),
                tbl: self.tbl.clone(),
                expire: self.task.expire_time.clone(),
                rows: last_result.clone(),
                statistics: Arc::clone(&self.statistics),
            };
            tracer.enter_phase(PHASE_DISPATCH);
            crossbeam_channel::select! {
                recv(scan_ctx.done()) -> _ => {
                    return Err(scan_ctx.err().map(|err| err.to_string()).unwrap_or_default());
                }
                send(del_ch, del_task) -> sent => {
                    if sent.is_err() {
                        // Go's send on a channel nobody reads blocks until the
                        // scan context ends.
                        let _ = scan_ctx.done().recv();
                        return Err(scan_ctx.err().map(|err| err.to_string()).unwrap_or_default());
                    }
                    self.statistics.inc_total_rows(last_result.len());
                }
            }
            tracer.enter_phase(PHASE_OTHER);
        }
    }
}

/// Go `scanTaskExecEndMsg`.
pub struct ScanTaskExecEndMsg {
    /// Go `result`.
    pub result: TtlScanTaskExecResult,
}

/// The fields Go's `ttlScanWorker` guards with the worker mutex.
#[derive(Default)]
pub struct ScanWorkerState {
    cur_task: Option<Arc<TtlScanTask>>,
    cur_task_result: Option<TtlScanTaskExecResult>,
}

/// Go `scanWorker`.
pub trait ScanWorker: Worker {
    /// Go `CouldSchedule`.
    fn could_schedule(&self) -> bool;
    /// Go `Schedule`.
    fn schedule(&self, task: Arc<TtlScanTask>) -> Result<()>;
    /// Go `PollTaskResult`.
    fn poll_task_result(&self) -> Option<TtlScanTaskExecResult>;
    /// Go `CurrentTask`.
    fn current_task(&self) -> Option<Arc<TtlScanTask>>;
}

/// Go `ttlScanWorker`.
pub struct TtlScanWorker {
    base: Arc<BaseWorker<Arc<TtlScanTask>, ScanWorkerState>>,
}

/// Go `newScanWorker`.
pub fn new_scan_worker(
    del_ch: Sender<TtlDeleteTask>,
    notify_state_ch: Option<Sender<ScanTaskExecEndMsg>>,
    sess_pool: Arc<dyn SessionPool>,
) -> Arc<TtlScanWorker> {
    let base = BaseWorker::new(ScanWorkerState::default());
    let loop_base = Arc::downgrade(&base);
    base.set_loop(Box::new(move || {
        let Some(base) = loop_base.upgrade() else {
            return Ok(());
        };
        scan_worker_loop(&base, &del_ch, notify_state_ch.as_ref(), sess_pool.as_ref())
    }));
    Arc::new(TtlScanWorker { base })
}

/// Go `(*ttlScanWorker).loop`.
fn scan_worker_loop(
    base: &Arc<BaseWorker<Arc<TtlScanTask>, ScanWorkerState>>,
    del_ch: &Sender<TtlDeleteTask>,
    notify_state_ch: Option<&Sender<ScanTaskExecEndMsg>>,
    sess_pool: &dyn SessionPool,
) -> Result<()> {
    let ctx = base.ctx.clone();
    let tracer = new_scan_worker_phase_tracer();
    let tracer = Some(&tracer);
    let ticker = crossbeam_channel::tick(Duration::from_secs(5));
    while base.status() == WorkerStatus::Running {
        tracer.enter_phase(PHASE_IDLE);
        crossbeam_channel::select! {
            recv(ctx.done()) -> _ => break,
            // ticker is used to update metrics on time
            recv(ticker) -> _ => {}
            recv(base.ch.1) -> msg => {
                tracer.enter_phase(PHASE_OTHER);
                let Ok(task) = msg else {
                    break;
                };
                handle_scan_task(base, tracer, &task, del_ch, notify_state_ch, sess_pool);
            }
        }
    }
    tracer.end_phase();
    bg_logger().info("ttlScanWorker loop exited.", &[]);
    Ok(())
}

/// Go `handleScanTask`.
fn handle_scan_task(
    base: &BaseWorker<Arc<TtlScanTask>, ScanWorkerState>,
    tracer: Option<&PhaseTracer>,
    task: &Arc<TtlScanTask>,
    del_ch: &Sender<TtlDeleteTask>,
    notify_state_ch: Option<&Sender<ScanTaskExecEndMsg>>,
    sess_pool: &dyn SessionPool,
) {
    let mut result = task.do_scan(&base.ctx, tracer, del_ch, sess_pool);
    if result.reason == TaskTerminateReason::Error && base.ctx.err().is_some() {
        result.reason = TaskTerminateReason::WorkerStop;
    }

    base.lock().extra.cur_task_result = Some(result.clone());

    if let Some(notify_state_ch) = notify_state_ch {
        let _ = notify_state_ch.try_send(ScanTaskExecEndMsg { result });
    }
}

impl Worker for TtlScanWorker {
    fn start(&self) {
        self.base.start();
    }

    fn stop(&self) {
        self.base.stop();
    }

    fn status(&self) -> WorkerStatus {
        self.base.status()
    }

    fn error(&self) -> Option<String> {
        self.base.error()
    }

    fn wait_stopped(&self, ctx: &Context, timeout: Duration) -> std::result::Result<(), tidb_ttl_client::CtxError> {
        self.base.wait_stopped(ctx, timeout)
    }
}

impl ScanWorker for TtlScanWorker {
    fn could_schedule(&self) -> bool {
        let state = self.base.lock();
        // see `Schedule`. If a `worker.CouldSchedule()` is true, `worker.Schedule` must success
        state.status_is_running() && state.extra.cur_task.is_none() && state.extra.cur_task_result.is_none()
    }

    fn schedule(&self, task: Arc<TtlScanTask>) -> Result<()> {
        {
            let mut state = self.base.lock();
            if !state.status_is_running() {
                return Err("worker is not running".to_owned());
            }
            if state.extra.cur_task_result.is_some() {
                return Err("the result of previous task has not been polled".to_owned());
            }
            if state.extra.cur_task.is_some() {
                return Err("a task is running".to_owned());
            }
            state.extra.cur_task = Some(Arc::clone(&task));
            state.extra.cur_task_result = None;
        }
        self.base.ch.0.send(task).map_err(|_| "worker is not running".to_owned())
    }

    fn current_task(&self) -> Option<Arc<TtlScanTask>> {
        self.base.lock().extra.cur_task.clone()
    }

    fn poll_task_result(&self) -> Option<TtlScanTaskExecResult> {
        let mut state = self.base.lock();
        let result = state.extra.cur_task_result.take()?;
        state.extra.cur_task = None;
        Some(result)
    }
}
