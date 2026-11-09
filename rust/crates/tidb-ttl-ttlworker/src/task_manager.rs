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

//! Go `pkg/ttl/ttlworker/task_manager.go`.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};
use tidb_datatype::Datum;
use tidb_logutil::Logger;
use tidb_timer::go_time::GoTime;
use tidb_ttl::cache::infoschema::InfoSchemaCache;
use tidb_ttl::cache::task::{self as cache_task, TTLTaskState, TaskStatus};
use tidb_ttl::session::{SessionError, TxnMode};
use tidb_ttl_client::context::{CancelFunc, Context};
use tidb_ttl_metrics::{DELETING_TASK_CNT, SCANNING_TASK_CNT};

use crate::config::{get_task_manager_heart_beat_interval, TTL_INTERNAL_SQL_TIMEOUT};
use crate::del::{new_delete_worker, TtlDeleteTask};
use crate::scan::{
    new_scan_worker, ScanTaskExecEndMsg, ScanWorker, Task, TaskTerminateReason, TtlScanTask,
    TtlScanTaskExecResult, TtlStatistics,
};
use crate::session::SessionPool;
use crate::worker::Worker;
use crate::{field, int_field, str_arg, uint_field, wrap_execute_sql, Result, Session, Store};

/// Go `setTTLTaskOwnerTemplate`.
pub const SET_TTL_TASK_OWNER_TEMPLATE: &str = "UPDATE mysql.tidb_ttl_task
	SET owner_id = %?,
		owner_hb_time = %?,
		status = 'running',
		status_update_time = %?
	WHERE job_id = %? AND scan_id = %?";

/// Go `setTTLTaskFinishedTemplate`.
pub const SET_TTL_TASK_FINISHED_TEMPLATE: &str = "UPDATE mysql.tidb_ttl_task
	SET status = 'finished',
		status_update_time = %?,
		state = %?
	WHERE job_id = %? AND scan_id = %? AND status = 'running' AND owner_id = %?";

/// Go `updateTTLTaskHeartBeatTemplate`.
pub const UPDATE_TTL_TASK_HEART_BEAT_TEMPLATE: &str = "UPDATE mysql.tidb_ttl_task
    SET state = %?,
		owner_hb_time = %?
    WHERE job_id = %? AND scan_id = %? AND owner_id = %?";

/// Go `resignOwnerSQLTemplate`.
pub const RESIGN_OWNER_SQL_TEMPLATE: &str = "UPDATE mysql.tidb_ttl_task
    SET state = %?,
        status = 'waiting',
        owner_id = NULL,
        status_update_time = %?,
		owner_hb_time = NULL
    WHERE job_id = %? AND scan_id = %? AND owner_id = %?";

/// Go `countRunningTasks`.
pub const COUNT_RUNNING_TASKS: &str = "SELECT count(1) FROM mysql.tidb_ttl_task WHERE status = 'running'";

/// Go `waitTaskProcessRowsTimeout`: how long a finished scan task waits for
/// its rows to be processed before it is marked finished anyway.
pub const WAIT_TASK_PROCESS_ROWS_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Go `errAlreadyScheduled`.
pub const ERR_ALREADY_SCHEDULED: &str = "task is already scheduled";
/// Go `errTooManyRunningTasks`.
pub const ERR_TOO_MANY_RUNNING_TASKS: &str = "there are too many running tasks";

/// Go `waitWorkerStopTimeout`.
pub const WAIT_WORKER_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Go `json.Marshal(state)` of a `cache.TTLTaskState`.
pub fn marshal_task_state(state: &TTLTaskState) -> String {
    let prev_owner = if state.previous_owner.is_empty() {
        String::new()
    } else {
        format!(",\"prev_owner\":{}", serde_json::to_string(&state.previous_owner).expect("a string marshals"))
    };
    format!(
        "{{\"total_rows\":{},\"success_rows\":{},\"error_rows\":{},\"scan_task_err\":{}{prev_owner}}}",
        state.total_rows,
        state.success_rows,
        state.error_rows,
        serde_json::to_string(&state.scan_task_err).expect("a string marshals"),
    )
}

/// Go `setTTLTaskOwnerSQL`.
pub fn set_ttl_task_owner_sql(job_id: &str, scan_id: i64, id: &str, now: &GoTime) -> (&'static str, Vec<Datum>) {
    (
        SET_TTL_TASK_OWNER_TEMPLATE,
        vec![
            str_arg(id),
            str_arg(&now.format_date_time()),
            str_arg(&now.format_date_time()),
            str_arg(job_id),
            Datum::new_int(scan_id),
        ],
    )
}

/// Go `setTTLTaskFinishedSQL`.
pub fn set_ttl_task_finished_sql(
    job_id: &str,
    scan_id: i64,
    state: &TTLTaskState,
    now: &GoTime,
    owner_id: &str,
) -> (&'static str, Vec<Datum>) {
    (
        SET_TTL_TASK_FINISHED_TEMPLATE,
        vec![
            str_arg(&now.format_date_time()),
            str_arg(&marshal_task_state(state)),
            str_arg(job_id),
            Datum::new_int(scan_id),
            str_arg(owner_id),
        ],
    )
}

/// Go `updateTTLTaskHeartBeatSQL`.
pub fn update_ttl_task_heart_beat_sql(
    job_id: &str,
    scan_id: i64,
    now: &GoTime,
    state: &TTLTaskState,
    owner_id: &str,
) -> (&'static str, Vec<Datum>) {
    (
        UPDATE_TTL_TASK_HEART_BEAT_TEMPLATE,
        vec![
            str_arg(&marshal_task_state(state)),
            str_arg(&now.format_date_time()),
            str_arg(job_id),
            Datum::new_int(scan_id),
            str_arg(owner_id),
        ],
    )
}

/// Go `resignOwnerSQL`.
pub fn resign_owner_sql(
    job_id: &str,
    scan_id: i64,
    now: &GoTime,
    state: &TTLTaskState,
    owner_id: &str,
) -> (&'static str, Vec<Datum>) {
    (
        RESIGN_OWNER_SQL_TEMPLATE,
        vec![
            str_arg(&marshal_task_state(state)),
            str_arg(&now.format_date_time()),
            str_arg(job_id),
            Datum::new_int(scan_id),
            str_arg(owner_id),
        ],
    )
}

/// `pkg/ttl/cache`'s statement arguments as `%?` datums.
pub(crate) fn cache_args(args: Vec<cache_task::SqlArg>) -> Vec<Datum> {
    args.into_iter()
        .map(|arg| match arg {
            cache_task::SqlArg::Str(value) => str_arg(&value),
            cache_task::SqlArg::Int(value) => Datum::new_int(value),
            cache_task::SqlArg::Bytes(value) => Datum::new_bytes_datum(value),
            cache_task::SqlArg::Time(value) => Datum::new_time(value),
        })
        .collect()
}

/// Whether `err` is Go's `storeerr.ErrLockWaitTimeout`.
///
/// `// boundary:` Go compares `errors.Cause(err)` with the error value; the
/// Rust session reports a statement error as its message, so the error is
/// recognised by that message.
pub(crate) fn is_lock_wait_timeout(err: &str) -> bool {
    err.contains("Lock wait timeout exceeded; try restarting transaction")
}

/// Go `runningScanTask`.
pub struct RunningScanTask {
    /// Go's embedded `*ttlScanTask`.
    pub scan_task: Arc<TtlScanTask>,
    /// Go `cancel`.
    pub cancel: CancelFunc,
    /// Go `result`.
    pub result: Option<TtlScanTaskExecResult>,
}

impl RunningScanTask {
    /// Go `JobID`.
    pub fn job_id(&self) -> &str {
        self.scan_task.task.job_id()
    }

    /// Go `ScanID`.
    pub fn scan_id(&self) -> i64 {
        self.scan_task.task.scan_id()
    }

    /// Go `Context`: the task's context (used by tests).
    pub fn context(&self) -> &Context {
        &self.scan_task.ctx
    }

    /// Go `dumpNewTaskState`: the state to store for the task.
    pub fn dump_new_task_state(&self) -> TTLTaskState {
        let statistics: &TtlStatistics = &self.scan_task.statistics;
        let mut state = TTLTaskState {
            total_rows: statistics.total_rows.load(Ordering::SeqCst),
            success_rows: statistics.success_rows.load(Ordering::SeqCst),
            error_rows: statistics.error_rows.load(Ordering::SeqCst),
            ..TTLTaskState::default()
        };

        if let Some(prev_state) = &self.scan_task.task.task.state {
            // If a task was timeout and taken over by the current instance,
            // adding the previous state to the current state to make the statistics more accurate.
            state.total_rows += prev_state.success_rows + prev_state.error_rows;
            state.success_rows += prev_state.success_rows;
            state.error_rows += prev_state.error_rows;
        }

        if let Some(result) = &self.result {
            if let Some(err) = &result.err {
                state.scan_task_err = err.clone();
            }
        }

        state
    }

    /// Go `finished`: whether the scan ended and its rows were processed
    /// (or have had long enough).
    pub fn finished(&self, logger: &Logger) -> bool {
        let Some(result) = &self.result else {
            // Scan task isn't finished
            return false;
        };

        let statistics = &self.scan_task.statistics;
        let total_rows = statistics.total_rows.load(Ordering::SeqCst);
        let err_rows = statistics.error_rows.load(Ordering::SeqCst);
        let success_rows = statistics.success_rows.load(Ordering::SeqCst);
        let processed_rows = success_rows + err_rows;
        let mut fields = self.scan_task.task_log_fields();
        fields.push(uint_field("totalRows", total_rows));
        fields.push(uint_field("successRows", success_rows));
        fields.push(uint_field("errorRows", err_rows));
        if processed_rows == total_rows {
            // All rows are processed.
            logger.info("will mark TTL task finished because all scanned rows are processed", &fields);
            return true;
        }

        if processed_rows > total_rows {
            // All rows are processed but processed rows are more than total rows.
            // We still think it is finished.
            logger.warn("will mark TTL task finished but processed rows are more than total rows", &fields);
            return true;
        }

        if GoTime::now().sub(&result.time) > WAIT_TASK_PROCESS_ROWS_TIMEOUT.as_nanos() as i64 {
            // If the scan task is finished and not all rows are processed, we should wait a certain time to report the task.
            // After a certain time, if the rows are still not processed, we need to mark the task finished anyway to make
            // sure the TTL job does not hang.
            logger.info(
                "will mark TTL task finished because timeout for waiting all scanned rows processed after scan task done",
                &fields,
            );
            return true;
        }

        false
    }
}

/// Go `findTaskWithID`.
pub fn find_task_with_id<'a>(
    tasks: &'a mut [RunningScanTask],
    job_id: &str,
    scan_id: i64,
) -> Option<&'a mut RunningScanTask> {
    tasks
        .iter_mut()
        .find(|task| task.scan_id() == scan_id && task.job_id() == job_id)
}

/// Go `getMaxRunningTasksLimit`.
pub fn get_max_running_tasks_limit(store: &Store) -> i64 {
    let ttl_running_task = tidb_vardef::TTL_RUNNING_TASKS.load(Ordering::SeqCst);
    if ttl_running_task != -1 {
        return i64::from(ttl_running_task);
    }
    let max = tidb_vardef::tidb_vars::MAX_CONFIGURABLE_CONCURRENCY as i64;
    match store.as_ref().and_then(|store| store.tikv_store_count()) {
        Some(tikv_cnt) => (tikv_cnt as i64).min(max),
        None => max,
    }
}

/// Go `taskManager`: schedules and manages the ttl tasks on this instance.
pub struct TaskManager {
    ctx: Context,
    pub(crate) logger: Logger,
    sess_pool: Arc<dyn SessionPool>,
    id: String,
    store: Store,
    /// Go `scanWorkers`.
    pub scan_workers: Vec<Arc<dyn ScanWorker>>,
    /// Go `delWorkers`.
    pub del_workers: Vec<Arc<dyn Worker>>,
    info_schema_cache: Arc<Mutex<InfoSchemaCache>>,
    /// Go `runningTasks`.
    pub running_tasks: Vec<RunningScanTask>,
    del_ch: (Sender<TtlDeleteTask>, Receiver<TtlDeleteTask>),
    /// Go `notifyStateCh`.
    pub notify_state_ch: (Sender<ScanTaskExecEndMsg>, Receiver<ScanTaskExecEndMsg>),
}

/// Go `newTaskManager`.
pub fn new_task_manager(
    ctx: &Context,
    logger: &Logger,
    sess_pool: Arc<dyn SessionPool>,
    info_schema_cache: Arc<Mutex<InfoSchemaCache>>,
    id: &str,
    store: Store,
) -> TaskManager {
    TaskManager {
        ctx: ctx.clone(),
        logger: logger.with_fields(&[field("ttl-worker", "task-manager")]),
        sess_pool,
        id: id.to_owned(),
        store,
        scan_workers: Vec::new(),
        del_workers: Vec::new(),
        info_schema_cache,
        running_tasks: Vec::new(),
        del_ch: crossbeam_channel::bounded(0),
        notify_state_ch: crossbeam_channel::bounded(1),
    }
}

impl TaskManager {
    fn exec(&self, se: &Session, sql: &str, args: &[Datum]) -> Result<Vec<crate::Row>> {
        se.execute_sql(&self.ctx, sql, args).map_err(|err| wrap_execute_sql(err, sql))
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, InfoSchemaCache> {
        self.info_schema_cache.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `resizeWorkersWithSysVar`.
    pub fn resize_workers_with_sys_var(&mut self) {
        if let Err(err) = self.resize_scan_workers(tidb_vardef::TTL_SCAN_WORKER_COUNT.load(Ordering::SeqCst) as usize) {
            self.logger.warn("fail to resize scan workers", &[field("error", err)]);
        }
        if let Err(err) = self.resize_del_workers(tidb_vardef::TTL_DELETE_WORKER_COUNT.load(Ordering::SeqCst) as usize) {
            self.logger.warn("fail to resize delete workers", &[field("error", err)]);
        }
    }

    /// Go `resizeScanWorkers`.
    pub fn resize_scan_workers(&mut self, count: usize) -> Result<()> {
        let del_tx = self.del_ch.0.clone();
        let notify_tx = self.notify_state_ch.0.clone();
        let pool = Arc::clone(&self.sess_pool);
        let workers = std::mem::take(&mut self.scan_workers);
        let (workers, canceled_workers, err) = self.resize_workers(workers, count, &|| {
            new_scan_worker(del_tx.clone(), Some(notify_tx.clone()), Arc::clone(&pool)) as Arc<dyn ScanWorker>
        });
        self.scan_workers = workers;

        for s in canceled_workers {
            let result = match s.poll_task_result() {
                Some(result) => result,
                None => {
                    // if the scan worker failed to poll the task, it's possible that the `WaitStopped` has timeout
                    // we still consider the scan task as finished
                    let Some(cur_task) = s.current_task() else {
                        continue;
                    };
                    let mut result = cur_task.result(Some("timeout to cancel scan task".to_owned()));
                    result.reason = TaskTerminateReason::WorkerStop;
                    result
                }
            };
            let job_id = result.task.task.job_id().to_owned();
            let scan_id = result.task.task.scan_id();
            let scan_err = result.err.clone().unwrap_or_default();
            let logger = self.logger.clone();
            let Some(task) = find_task_with_id(&mut self.running_tasks, &job_id, scan_id) else {
                logger.warn(
                    "task state changed but job not found",
                    &[field("jobID", job_id), int_field("scanID", scan_id)],
                );
                continue;
            };
            logger.debug(
                "scan task finished",
                &[field("jobID", job_id), int_field("taskID", scan_id), field("error", scan_err)],
            );
            task.result = Some(result);
        }
        err
    }

    /// Go `resizeDelWorkers`.
    pub fn resize_del_workers(&mut self, count: usize) -> Result<()> {
        let del_rx = self.del_ch.1.clone();
        let pool = Arc::clone(&self.sess_pool);
        let workers = std::mem::take(&mut self.del_workers);
        let (workers, _, err) = self.resize_workers(workers, count, &|| {
            new_delete_worker(del_rx.clone(), Arc::clone(&pool)) as Arc<dyn Worker>
        });
        self.del_workers = workers;
        err
    }

    /// Go `resizeWorkers`: the full set of workers, the stopped ones, and
    /// the stop errors.
    pub fn resize_workers<W: Worker + ?Sized>(
        &self,
        mut workers: Vec<Arc<W>>,
        count: usize,
        factory: &dyn Fn() -> Arc<W>,
    ) -> (Vec<Arc<W>>, Vec<Arc<W>>, Result<()>) {
        if count < workers.len() {
            self.logger.info(
                "shrink ttl worker",
                &[int_field("originalCount", workers.len() as i64), int_field("newCount", count as i64)],
            );

            for w in &workers[count..] {
                w.stop();
            }

            let mut errs: Vec<String> = Vec::new();
            // don't use `m.ctx` here, because when shutdown the server, `m.ctx` has already been cancelled
            let (ctx, cancel) = Context::with_timeout(&Context::background(), WAIT_WORKER_STOP_TIMEOUT);
            for w in &workers[count..] {
                if let Err(err) = w.wait_stopped(&ctx, WAIT_WORKER_STOP_TIMEOUT) {
                    self.logger.warn("fail to stop ttl worker", &[field("error", err.to_string())]);
                    errs.push(err.to_string());
                }
            }
            cancel.cancel();

            // remove the existing workers, and keep the left workers
            let stopped = workers.split_off(count);
            let err = if errs.is_empty() { Ok(()) } else { Err(errs.join("; ")) };
            return (workers, stopped, err);
        }

        if count > workers.len() {
            self.logger.info(
                "scale ttl worker",
                &[int_field("originalCount", workers.len() as i64), int_field("newCount", count as i64)],
            );
            for _ in workers.len()..count {
                let w = factory();
                w.start();
                workers.push(w);
            }
        }
        (workers, Vec::new(), Ok(()))
    }

    /// Go `handleScanFinishedTask`: polls the scan workers' results, and
    /// whether there were any.
    pub fn handle_scan_finished_task(&mut self) -> bool {
        let results = self.poll_scan_worker_results();
        let polled = !results.is_empty();
        for result in results {
            let mut fields = result.task.task_log_fields();
            if let Some(err) = &result.err {
                fields.push(field("error", err.clone()));
            }
            let logger = self.logger.clone();
            let job_id = result.task.task.job_id().to_owned();
            let scan_id = result.task.task.scan_id();
            let Some(task) = find_task_with_id(&mut self.running_tasks, &job_id, scan_id) else {
                logger.warn("task state changed but task not found", &fields);
                continue;
            };
            logger.info("task scans finished", &fields);
            task.result = Some(result);
        }
        polled
    }

    /// Go `pollScanWorkerResults`.
    fn poll_scan_worker_results(&self) -> Vec<TtlScanTaskExecResult> {
        self.scan_workers
            .iter()
            .filter_map(|worker| worker.poll_task_result())
            .collect()
    }

    /// Go `idleScanWorkers`.
    fn idle_scan_workers(&self) -> Vec<Arc<dyn ScanWorker>> {
        self.scan_workers
            .iter()
            .filter(|worker| worker.could_schedule())
            .cloned()
            .collect()
    }

    /// Go `rescheduleTasks`.
    pub fn reschedule_tasks(&mut self, se: &Session, now: &GoTime) {
        let mut idle_scan_workers = self.idle_scan_workers();
        if idle_scan_workers.is_empty() {
            return;
        }

        let tasks = match self.peek_waiting_scan_tasks(se, now) {
            Ok(tasks) => tasks,
            Err(err) => {
                self.logger.warn("fail to peek scan task", &[field("error", err)]);
                return;
            }
        };

        if tasks.is_empty() {
            return;
        }

        if let Err(err) = self.cache().update(se) {
            self.logger.warn("fail to update infoSchemaCache", &[field("error", err.to_string())]);
            return;
        }

        for t in tasks {
            let log_fields = vec![
                field("jobID", t.job_id()),
                int_field("scanID", t.scan_id()),
                int_field("tableID", t.table_id()),
            ];
            let task = match self.lock_scan_task(se, &t, now) {
                Ok(task) => task,
                Err(err) => {
                    if err == ERR_ALREADY_SCHEDULED {
                        continue;
                    }
                    if err == ERR_TOO_MANY_RUNNING_TASKS || is_lock_wait_timeout(&err) {
                        // don't step into the next step to avoid exceeding the limit
                        break;
                    }
                    let mut fields = log_fields;
                    fields.push(field("error", err));
                    self.logger.warn("fail to lock scan task", &fields);
                    continue;
                }
            };

            let idle_worker = idle_scan_workers.remove(0);
            if let Err(err) = idle_worker.schedule(Arc::clone(&task.scan_task)) {
                let mut fields = log_fields;
                fields.push(field("error", err));
                self.logger.warn("fail to schedule task", &fields);
                task.cancel.cancel();
                continue;
            }

            let (prev_total_rows, prev_success_rows, prev_error_rows) = task
                .scan_task
                .task
                .task
                .state
                .as_ref()
                .map_or((0, 0, 0), |state| (state.total_rows, state.success_rows, state.error_rows));
            let mut fields = log_fields;
            fields.push(uint_field("prevTotalRows", prev_total_rows));
            fields.push(uint_field("prevSuccessRows", prev_success_rows));
            fields.push(uint_field("prevErrorRows", prev_error_rows));
            self.logger.info("scheduled ttl task", &fields);
            self.running_tasks.push(task);

            if idle_scan_workers.is_empty() {
                return;
            }
        }
    }

    /// Go `peekWaitingScanTasks`.
    fn peek_waiting_scan_tasks(&self, se: &Session, now: &GoTime) -> Result<Vec<Task>> {
        debug_assert_eq!(se.get_session_vars().location(), *now.location());
        let hb_expire = now.add(-2 * get_task_manager_heart_beat_interval().as_nanos() as i64);
        let (sql, args) = cache_task::peek_waiting_ttl_task(&hb_expire.format_date_time());
        let rows = self.exec(se, &sql, &cache_args(args))?;
        let location = se.get_session_vars().location();
        rows.iter().map(|row| Task::from_row(&location, row)).collect()
    }

    /// Go `lockScanTask`.
    fn lock_scan_task(&self, se: &Session, task: &Task, now: &GoTime) -> Result<RunningScanTask> {
        let Some(table) = self.cache().tables.get(&task.table_id()).cloned() else {
            return Err(format!("didn't find table with id: {}", task.table_id()));
        };

        let hb_interval = get_task_manager_heart_beat_interval().as_nanos() as i64;
        se.run_in_txn(&self.ctx, 
            &mut || {
                // The `for update` here ensures two things:
                // 1. The instance which will update this row has committed, and we can get the newest information.
                // 2. There is only one successful transaction concurrently.
                //    If it's a `for update nowait`, we cannot avoid the situation that multiple transactions fetched the tasks
                //    at the same time, and the task limitation doesn't work.
                let task = self.sync_task_from_table(se, task.job_id(), task.scan_id(), true).map_err(SessionError)?;
                if !task.task.owner_id.is_empty() && !task.owner_hb_time.add(2 * hb_interval).before(now) {
                    return Err(SessionError(ERR_ALREADY_SCHEDULED.to_owned()));
                }

                // check the count of running task is smaller or equal than the limit
                let rows = self.exec(se, COUNT_RUNNING_TASKS, &[]).map_err(SessionError)?;
                let count = rows.first().map_or(0, |row| tidb_ttl::session::ResultRow::get_int64(row, 0));
                if !self.meet_ttl_running_task(count, &task.task.status) {
                    return Err(SessionError(ERR_TOO_MANY_RUNNING_TASKS.to_owned()));
                }

                if !task.task.owner_id.is_empty() {
                    self.logger.info(
                        "try to lock a heartbeat timeout task",
                        &[
                            field("jobID", task.job_id()),
                            int_field("scanID", task.scan_id()),
                            field("prevOwner", task.task.owner_id.clone()),
                            field("lastHeartbeat", task.owner_hb_time.format_date_time()),
                        ],
                    );
                } else if let Some(state) = task.task.state.as_ref().filter(|state| !state.previous_owner.is_empty()) {
                    self.logger.info(
                        "try to lock a task resigned from another instance",
                        &[
                            field("jobID", task.job_id()),
                            int_field("scanID", task.scan_id()),
                            field("prevOwner", state.previous_owner.clone()),
                        ],
                    );
                }

                debug_assert_eq!(se.get_session_vars().location(), *now.location());
                let (sql, args) = set_ttl_task_owner_sql(task.job_id(), task.scan_id(), &self.id, now);
                self.exec(se, sql, &args).map(drop).map_err(SessionError)
            },
            TxnMode::PESSIMISTIC,
            None,
        )
        .map_err(|err| err.to_string())?;

        // update the task after setting status and owner
        let task = self.sync_task_from_table(se, task.job_id(), task.scan_id(), false)?;

        let (ctx, cancel) = Context::with_cancel(&self.ctx);
        let scan_task = Arc::new(TtlScanTask {
            ctx,
            task,
            tbl: table,
            statistics: Arc::new(TtlStatistics::default()),
        });

        Ok(RunningScanTask {
            scan_task,
            cancel,
            result: None,
        })
    }

    /// Go `syncTaskFromTable`.
    fn sync_task_from_table(&self, se: &Session, job_id: &str, scan_id: i64, wait_lock: bool) -> Result<Task> {
        let (mut sql, args) = cache_task::select_from_ttl_task_with_id(job_id, scan_id);
        if wait_lock {
            sql.push_str(" FOR UPDATE");
        }
        let rows = self.exec(se, &sql, &cache_args(args))?;
        let Some(row) = rows.first() else {
            return Err(format!("didn't find task with jobID: {job_id}, scanID: {scan_id}"));
        };
        Task::from_row(&se.get_session_vars().location(), row)
    }

    /// Go `updateHeartBeat`: heartbeats every task this instance owns.
    pub fn update_heart_beat(&self, ctx: &Context, se: &Session, now: &GoTime) {
        for task in &self.running_tasks {
            if let Err(err) = self.task_heartbeat_or_resign_owner(ctx, se, now, task, false) {
                let mut fields = task.scan_task.task_log_fields();
                fields.push(field("error", err));
                self.logger.warn("fail to update task heart beat", &fields);
            }
        }
    }

    /// Go `taskHeartbeatOrResignOwner`.
    fn task_heartbeat_or_resign_owner(
        &self,
        ctx: &Context,
        se: &Session,
        now: &GoTime,
        task: &RunningScanTask,
        is_resign_owner: bool,
    ) -> Result<()> {
        let mut state = task.dump_new_task_state();
        debug_assert_eq!(se.get_session_vars().location(), *now.location());
        let (sql, args) = if is_resign_owner {
            state.previous_owner = self.id.clone();
            resign_owner_sql(task.job_id(), task.scan_id(), now, &state, &self.id)
        } else {
            update_ttl_task_heart_beat_sql(task.job_id(), task.scan_id(), now, &state, &self.id)
        };
        se.execute_sql(ctx, sql, &args).map_err(|err| wrap_execute_sql(err, sql))?;
        let affected_rows = se.get_session_vars().affected_rows();
        if affected_rows != 1 {
            return Err(format!(
                "fail to update task status, maybe the owner is not myself ({}), affected rows: {affected_rows}",
                self.id
            ));
        }
        Ok(())
    }

    /// Go `tryResignTaskOwner`: whether the owner was resigned.
    fn try_resign_task_owner(&self, se: &Session, task: &RunningScanTask, reason: &str, now: &GoTime) -> bool {
        let statistics = &task.scan_task.statistics;
        let total_rows = statistics.total_rows.load(Ordering::SeqCst);
        let success_rows = statistics.success_rows.load(Ordering::SeqCst);
        let err_rows = statistics.error_rows.load(Ordering::SeqCst);
        let processed_rows = success_rows + err_rows;

        let task_end_time = task.result.as_ref().map_or_else(GoTime::zero, |result| result.time.clone());

        let mut fields = task.scan_task.task_log_fields();
        fields.push(field("taskEndTime", task_end_time.format_date_time()));
        fields.push(field("reason", reason));
        fields.push(uint_field("totalRows", total_rows));
        fields.push(uint_field("processedRows", processed_rows));
        fields.push(uint_field("successRows", success_rows));
        fields.push(uint_field("errRows", err_rows));

        let since_task_end = now.sub(&task_end_time);
        if since_task_end < 0 {
            self.logger.warn("task end time is after current time, something may goes wrong", &fields);
        }

        let ten_seconds = 10 * tidb_timer::go_time::SECOND;
        if total_rows > processed_rows && since_task_end < ten_seconds && since_task_end > -ten_seconds {
            self.logger.info("wait all rows processed before resign the owner of a TTL task", &fields);
            return false;
        }

        if total_rows > processed_rows {
            self.logger.info("wait all rows processed timeout, force to resign the owner of a TTL task", &fields);
        } else {
            self.logger.info("resign the owner of a TTL task", &fields);
        }

        task.cancel.cancel();
        // Update the task state with heartbeatTask for the last time.
        // The task will be taken over by another instance after timeout.
        if let Err(err) = self.task_heartbeat_or_resign_owner(&self.ctx, se, now, task, true) {
            fields.push(field("error", err));
            self.logger.warn("fail to update the state before resign the task owner", &fields);
        }

        true
    }

    /// Go `checkFinishedTask`.
    pub fn check_finished_task(&mut self, se: &Session, now: &GoTime) {
        if self.running_tasks.is_empty() {
            return;
        }
        let running_tasks = std::mem::take(&mut self.running_tasks);
        let mut still_running_tasks = Vec::with_capacity(running_tasks.len());
        for task in running_tasks {
            if let Some(reason) = should_running_task_resign_owner(&task) {
                if !self.try_resign_task_owner(se, &task, reason, now) {
                    still_running_tasks.push(task);
                }
                continue;
            }

            if !task.finished(&self.logger) {
                still_running_tasks.push(task);
                continue;
            }
            // we should cancel task to release inner context and avoid memory leak
            task.cancel.cancel();
            if let Err(err) = self.report_task_finished(se, now, &task) {
                let mut fields = task.scan_task.task_log_fields();
                fields.push(field("error", err));
                self.logger.error("fail to report finished task", &fields);
                still_running_tasks.push(task);
                continue;
            }
        }

        self.running_tasks = still_running_tasks;
    }

    /// Go `reportTaskFinished`.
    ///
    /// Go also sets the in-memory `task.Status = cache.TaskStatusFinished`;
    /// nothing reads the field of a task leaving `runningTasks`, so that
    /// write has no counterpart.
    fn report_task_finished(&self, se: &Session, now: &GoTime, task: &RunningScanTask) -> Result<()> {
        let state = task.dump_new_task_state();
        debug_assert_eq!(se.get_session_vars().location(), *now.location());
        let (sql, args) = set_ttl_task_finished_sql(task.job_id(), task.scan_id(), &state, now, &self.id);
        let (timeout_ctx, cancel) = Context::with_timeout(&self.ctx, TTL_INTERNAL_SQL_TIMEOUT);
        let result = se.execute_sql(&timeout_ctx, sql, &args);
        cancel.cancel();
        result.map_err(|err| err.to_string())?;
        let affected_rows = se.get_session_vars().affected_rows();
        if affected_rows != 1 {
            return Err(format!(
                "fail to update task status, maybe the owner is not myself ({}) or task is not running, affected rows: {affected_rows}",
                self.id
            ));
        }

        let mut fields = task.scan_task.task_log_fields();
        fields.push(uint_field("finalTotalRows", state.total_rows));
        fields.push(uint_field("finalSuccessRows", state.success_rows));
        fields.push(uint_field("finalErrorRows", state.error_rows));
        self.logger.info("TTL task finished", &fields);
        Ok(())
    }

    /// Go `checkInvalidTask`: drops the tasks whose owner is not this
    /// instance or which disappeared.
    pub fn check_invalid_task(&mut self, se: &Session) {
        if self.running_tasks.is_empty() {
            return;
        }
        // TODO: optimize this function through cache or something else
        let running_tasks = std::mem::take(&mut self.running_tasks);
        let mut own_running_task = Vec::with_capacity(running_tasks.len());
        for task in running_tasks {
            let (sql, args) = cache_task::select_from_ttl_task_with_id(task.job_id(), task.scan_id());
            let mut fields = task.scan_task.task_log_fields();
            let (timeout_ctx, cancel) = Context::with_timeout(&self.ctx, TTL_INTERNAL_SQL_TIMEOUT);
            let rows = se.execute_sql(&timeout_ctx, &sql, &cache_args(args));
            cancel.cancel();
            let rows = match rows {
                Ok(rows) => rows,
                Err(err) => {
                    fields.push(field("sql", sql));
                    fields.push(field("error", err.to_string()));
                    self.logger.warn("fail to execute sql", &fields);
                    task.cancel.cancel();
                    continue;
                }
            };
            let Some(row) = rows.first() else {
                self.logger.warn("didn't find task", &fields);
                task.cancel.cancel();
                continue;
            };
            let t = match Task::from_row(&se.get_session_vars().location(), row) {
                Ok(t) => t,
                Err(err) => {
                    fields.push(field("error", err));
                    self.logger.warn("fail to get task", &fields);
                    task.cancel.cancel();
                    continue;
                }
            };

            if t.task.owner_id != self.id {
                fields.push(field("myOwnerID", self.id.clone()));
                fields.push(field("taskOwnerID", t.task.owner_id.clone()));
                self.logger.warn("task owner changed", &fields);
                task.cancel.cancel();
                continue;
            }

            own_running_task.push(task);
        }
        self.running_tasks = own_running_task;
    }

    /// Go `reportMetrics`.
    pub fn report_metrics(&self) {
        let mut scanning_task_cnt = 0;
        let mut deleting_task_cnt = 0;
        for task in &self.running_tasks {
            if task.result.is_some() {
                scanning_task_cnt += 1;
            } else {
                deleting_task_cnt += 1;
            }
        }
        SCANNING_TASK_CNT.set(f64::from(scanning_task_cnt));
        DELETING_TASK_CNT.set(f64::from(deleting_task_cnt));
    }

    /// Go `meetTTLRunningTask`.
    fn meet_ttl_running_task(&self, count: i64, task_status: &TaskStatus) -> bool {
        if task_status.0 == TaskStatus::RUNNING {
            // always return true for already running task because it is already included in count
            return true;
        }
        get_max_running_tasks_limit(&self.store) > count
    }
}

/// Go `shouldRunningTaskResignOwner`.
pub fn should_running_task_resign_owner(task: &RunningScanTask) -> Option<&'static str> {
    match task.result.as_ref().map(|result| result.reason) {
        Some(TaskTerminateReason::WorkerStop) => Some(TaskTerminateReason::WorkerStop.as_str()),
        _ => None,
    }
}
