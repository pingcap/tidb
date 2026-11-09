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

//! Go `task_executor.go`.

use std::error::Error as StdError;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use tidb_dxf::{
    step_2_str, NodeResource, Subtask, SubtaskState, Task, TaskBase, SUBTASK_STATE_CANCELED, SUBTASK_STATE_FAILED,
    SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING, TASK_STATE_MODIFYING, TASK_STATE_RUNNING,
};
use tidb_dxf_execute::StepExecutor;
use tidb_dxf_storage::StorageError;
use tidb_gocontext::{CancelCauseFunc, CancelFunc, Context};
use tidb_log::{Field, Level, Value};

use crate::interface::is_err;
use crate::manager::{max_subtask_check_interval, subtask_check_interval, MAX_CHECKS_WHEN_NO_SUBTASK};
use crate::{Error, Extension, SlotManager, TaskExecutor, TaskExecutorError, TaskTable};

/// Go `checkBalanceSubtaskInterval`: how often the executor checks whether
/// subtasks were balanced to or away from the node.
pub(crate) static CHECK_BALANCE_SUBTASK_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_secs(2));

/// Go `DetectParamModifyInterval`: how often task param modification is
/// detected. Exported for testing.
pub static DETECT_PARAM_MODIFY_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_secs(5));

pub(crate) fn read(duration: &RwLock<Duration>) -> Duration {
    *duration.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Go `time.Duration` in `pkg/util/backoff`'s nanoseconds.
pub(crate) fn nanos(duration: Duration) -> tidb_backoff::Duration {
    duration.as_nanos() as tidb_backoff::Duration
}

pub(crate) fn str_field(name: &str, value: impl Into<String>) -> Field {
    Field::new(name, Value::Str(value.into()))
}

pub(crate) fn i64_field(name: &str, value: i64) -> Field {
    Field::new(name, Value::I64(value))
}

pub(crate) fn error_field(err: &dyn std::fmt::Display) -> Field {
    str_field("error", err.to_string())
}

/// A message-only error (Go `errors.New` / `errors.Errorf`).
#[derive(Debug)]
pub(crate) struct PlainError(pub(crate) String);

impl std::fmt::Display for PlainError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl StdError for PlainError {}

pub(crate) fn plain(message: impl Into<String>) -> Error {
    Arc::new(PlainError(message.into()))
}

/// Go `errors.Annotate(err, message)`.
#[derive(Debug)]
struct Annotated {
    message: &'static str,
    cause: Error,
}

impl std::fmt::Display for Annotated {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.message, self.cause)
    }
}

impl StdError for Annotated {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.cause.as_ref())
    }
}

/// `handle.RunWithRetry`'s error as Go's `error`: the function's own, or
/// the context's.
pub(crate) fn retry_err(err: tidb_dxf_handle::RetryError<StorageError>) -> Error {
    match err {
        tidb_dxf_handle::RetryError::Func(err) => Arc::new(err),
        tidb_dxf_handle::RetryError::Context(err) => Arc::new(err),
    }
}

pub(crate) fn as_dyn<'a>(err: &'a (dyn StdError + Send + Sync + 'static)) -> &'a (dyn StdError + 'static) {
    err
}

/// Go `recover()`'s value as `%v` prints it.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default()
}

/// Go `util.WaitGroupWrapper.RunWithLog`'s body: a panic is logged, not
/// propagated.
pub(crate) fn run_with_log(f: impl FnOnce()) {
    if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(f)) {
        tidb_logutil::bg_logger().error(
            "panic in the recoverable goroutine",
            &[str_field("r", panic_message(panic.as_ref()))],
        );
    }
}

/// Go's `defer cancel()`.
struct CancelOnDrop<'a>(&'a CancelFunc);

impl Drop for CancelOnDrop<'_> {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Go `Param`: what a task executor is created with.
#[derive(Clone)]
pub struct Param {
    pub(crate) task_table: Arc<dyn TaskTable>,
    pub(crate) slot_mgr: Arc<SlotManager>,
    pub(crate) node_rc: NodeResource,
    /// The node's ID, the server ID now, i.e. host:port.
    pub(crate) exec_id: String,
    /// Go `TaskRuntime`: the task keyspace's runtime view, not owned; the
    /// managers release it.
    pub task_runtime: Option<tidb_dxf_dxfutil::TaskRuntime>,
}

impl Param {
    /// Go `NewParamForTest`.
    #[must_use]
    pub fn new_for_test(task_table: Arc<dyn TaskTable>, slot_mgr: Arc<SlotManager>, node_rc: NodeResource, exec_id: &str) -> Self {
        Self { task_table, slot_mgr, node_rc, exec_id: exec_id.to_owned(), task_runtime: None }
    }
}

/// The step executor of the step being run, with its context and timed log.
struct StepState {
    exec: Arc<dyn StepExecutor>,
    ctx: Context,
    logger: tidb_lightning_log::Task,
}

/// Go `BaseTaskExecutor`: what every task type's executor embeds; the task
/// type implements [`Extension`].
pub struct BaseTaskExecutor {
    /// Go's embedded `Param`.
    pub param: Param,
    /// The task, aligned periodically with the task table; a param
    /// modification applied successfully also updates it in memory (see
    /// `detect_and_handle_param_modify_loop`). Replaced whole, never changed
    /// in place.
    task: RwLock<Arc<Task>>,
    logger: tidb_logutil::Logger,
    pub(crate) sample_logger: RwLock<tidb_logutil::SampledLogger>,
    ctx: Context,
    cancel: CancelFunc,
    /// Go's embedded `Extension`, which the factory MUST set.
    extension: RwLock<Option<Weak<dyn Extension>>>,
    pub(crate) curr_subtask_id: AtomicI64,
    /// Go `mu.runtimeCancel`: cancels the running step when an error
    /// occurs.
    runtime_cancel: Mutex<Option<CancelCauseFunc>>,
    /// Go `stepExec`, `stepCtx` and `stepLogger`.
    step: Mutex<Option<StepState>>,
}

impl BaseTaskExecutor {
    /// Go `NewBaseTaskExecutor`. Task types create their executor from the
    /// whole task, as add-index's `Init` still needs it.
    #[must_use]
    pub fn new(ctx: &Context, task: Task, param: Param) -> Self {
        let mut fields = vec![i64_field("task-id", task.base.id), str_field("task-key", task.base.key.clone())];
        if tidb_intest::IN_TEST {
            fields.push(str_field("server-id", param.exec_id.clone()));
        }
        let logger = tidb_logutil::err_verbose_logger().with_fields(&fields);
        let sample_logger = tidb_dxf_handle::new_sample_err_verbose_logger(&fields);
        let (ctx, cancel) = Context::with_cancel(ctx);
        Self {
            param,
            task: RwLock::new(Arc::new(task)),
            logger,
            sample_logger: RwLock::new(sample_logger),
            ctx,
            cancel,
            extension: RwLock::new(None),
            curr_subtask_id: AtomicI64::new(0),
            runtime_cancel: Mutex::new(None),
            step: Mutex::new(None),
        }
    }

    /// Sets Go's embedded `Extension`, the task type's executor.
    pub fn set_extension(&self, extension: Weak<dyn Extension>) {
        *self.extension.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(extension);
    }

    fn ext(&self) -> Arc<dyn Extension> {
        self.extension
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
            .expect("the task executor factory sets the extension")
    }

    fn sample_logger(&self) -> tidb_logutil::SampledLogger {
        self.sample_logger.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    pub(crate) fn get_task(&self) -> Arc<Task> {
        Arc::clone(&self.task.read().unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    fn store_task(&self, task: Task) {
        *self.task.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(task);
    }

    /// Go `stepExec`.
    pub(crate) fn step_exec(&self) -> Option<Arc<dyn StepExecutor>> {
        self.step.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref().map(|step| Arc::clone(&step.exec))
    }

    /// Go `stepExec` and `stepCtx`.
    fn step_exec_and_ctx(&self) -> Option<(Arc<dyn StepExecutor>, Context)> {
        self.step
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|step| (Arc::clone(&step.exec), step.ctx.clone()))
    }

    /// Sets Go's `stepExec` field (tests set it directly).
    #[cfg(test)]
    pub(crate) fn set_step_exec(&self, exec: Arc<dyn StepExecutor>) {
        let logger = tidb_lightning_log::begin_logutil_task(self.logger.clone(), "execute task step");
        *self.step.lock().unwrap() = Some(StepState { exec, ctx: self.ctx.clone(), logger });
    }

    /// Go `checkBalanceSubtask`: whether subtasks were balanced to or away
    /// from the node.
    ///   - another subtask in `running` state scheduled to the node goes back
    ///     to `pending`, so subtasks can be balanced when the node scales out.
    ///   - when the running subtask was scheduled away, i.e. the node is taken
    ///     as down, its run is cancelled.
    pub(crate) fn check_balance_subtask(&self, ctx: &Context, subtask_ctx_cancel: Option<&CancelFunc>) {
        let mut start = Instant::now();
        let ticker = crossbeam_channel::tick(read(&CHECK_BALANCE_SUBTASK_INTERVAL));
        let done = ctx.done();
        loop {
            crossbeam_channel::select! {
                recv(done) -> _ => return,
                recv(ticker) -> _ => {}
            }

            let task = self.get_task();
            if start.elapsed() > Duration::from_secs(3600) {
                start = Instant::now();
                self.logger.info(
                    "subtask running for too long",
                    &[i64_field("subtaskID", self.curr_subtask_id.load(Ordering::SeqCst))],
                );
                tidb_dxf_dxfmetric::EXECUTE_EVENT_COUNTER
                    .with_label_values(&[&task.base.id.to_string(), tidb_dxf_dxfmetric::EVENT_SUBTASK_SLOW])
                    .inc();
            }

            let subtasks = match self.param.task_table.get_subtasks_by_exec_id_and_step_and_states(
                &self.param.exec_id,
                task.base.id,
                task.base.step,
                &[SUBTASK_STATE_RUNNING],
            ) {
                Ok(subtasks) => subtasks,
                Err(err) => {
                    self.logger.warn("get subtasks failed", &[error_field(&err)]);
                    continue;
                }
            };
            if subtasks.is_empty() {
                self.logger.info(
                    "subtask is scheduled away, cancel running",
                    &[i64_field("subtaskID", self.curr_subtask_id.load(Ordering::SeqCst))],
                );
                // cancels runStep, but leave the subtask state unchanged.
                tidb_dxf_dxfmetric::EXECUTE_EVENT_COUNTER
                    .with_label_values(&[&task.base.id.to_string(), tidb_dxf_dxfmetric::EVENT_SUBTASK_SCHEDULED_AWAY])
                    .inc();
                if let Some(cancel) = subtask_ctx_cancel {
                    cancel.cancel();
                }
                return;
            }

            let mut extra_running_subtasks = Vec::with_capacity(subtasks.len());
            for st in &subtasks {
                if st.base.id == self.curr_subtask_id.load(Ordering::SeqCst) {
                    continue;
                }
                if !self.ext().is_idempotent(st) {
                    if let Err(err) = self.update_subtask_state_and_error_impl(
                        ctx,
                        &st.base.exec_id,
                        st.base.id,
                        &SUBTASK_STATE_FAILED,
                        Some(&TaskExecutorError::NonIdempotentSubtask),
                    ) {
                        self.logger.error("failed to update subtask to 'failed' state", &[error_field(&err)]);
                        continue;
                    }
                    // if a subtask fail, scheduler will notice and start revert the
                    // task, so we can directly return.
                    return;
                }
                extra_running_subtasks.push(st.base.clone());
            }
            if !extra_running_subtasks.is_empty() {
                match self.param.task_table.running_subtasks_back2_pending(&extra_running_subtasks) {
                    Err(err) => self.logger.error("update running subtasks back to pending failed", &[error_field(&err)]),
                    Ok(()) => self.logger.info(
                        "update extra running subtasks back to pending",
                        &[str_field("subtasks", format!("{extra_running_subtasks:?}"))],
                    ),
                }
            }
        }
    }

    /// Go `updateSubtaskSummaryLoop`.
    fn update_subtask_summary_loop(&self, check_ctx: &Context, run_step_ctx: &Context, step_exec: &dyn StepExecutor) {
        let task_mgr = self.param.task_table.as_task_manager().expect("hasRealtimeSummary checked the task table");
        let ticker = crossbeam_channel::tick(tidb_dxf_execute::UPDATE_SUBTASK_SUMMARY_INTERVAL);
        let done = check_ctx.done();
        let cur_subtask_id = self.curr_subtask_id.load(Ordering::SeqCst);
        // A step executor with a summary hands it out every time
        // (hasRealtimeSummary).
        let update_summary = || match step_exec.realtime_summary() {
            Some(summary) => task_mgr.update_subtask_summary(cur_subtask_id, &summary),
            None => Ok(()),
        };
        loop {
            crossbeam_channel::select! {
                recv(done) -> _ => {
                    // Try the best effort to update the summary before exiting.
                    // Total retry time is 0.2s + 0.4s + 0.8s + 1.6s + 3.2s + 5s * 5 = 31.2s
                    let mut backoffer =
                        tidb_backoff::new_exponential(nanos(Duration::from_millis(200)), 2.0, nanos(Duration::from_secs(5)));
                    if let Err(err) =
                        tidb_dxf_handle::run_with_retry(run_step_ctx, 10, &mut backoffer, &self.logger, |_| (true, update_summary()))
                    {
                        self.logger.info("update subtask row count failed", &[error_field(&err)]);
                    }
                    return;
                }
                recv(ticker) -> _ => {}
            }
            if let Err(err) = update_summary() {
                self.logger.info("update subtask row count failed", &[error_field(&err)]);
            }
        }
    }

    /// Go `Ctx`: the executor's context.
    #[must_use]
    pub fn ctx(&self) -> &Context {
        &self.ctx
    }

    /// Go `Run`.
    pub fn run(&self) {
        if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(|| self.run_loop())) {
            let recovered = panic_message(panic.as_ref());
            self.logger.error("run task panicked, fail the task", &[str_field("recover", recovered.clone())]);
            let task_id = self.get_task().base.id;
            self.fail_one_subtask(&self.ctx, task_id, &plain(recovered));
        }
        if self.step_exec().is_some() {
            self.clean_step_executor();
        }
        tidb_dxf_metering::unregister_recorder(self.get_task_base().id);
    }

    fn run_loop(&self) {
        // task executor occupies resources, if there's no subtask to run for 10s,
        // we release the resources so that other tasks can use them.
        // 300ms + 600ms + 1.2s + 2s * 4 = 10.1s
        let mut backoffer =
            tidb_backoff::new_exponential(nanos(subtask_check_interval()), 2.0, nanos(max_subtask_check_interval()));
        let (mut check_interval, mut no_subtask_check_cnt) = (subtask_check_interval(), 0);
        let mut skip_backoff = false;
        loop {
            if self.ctx.err().is_some() {
                return;
            }
            if !skip_backoff && self.ctx.sleep(check_interval) {
                return;
            }
            skip_backoff = false;
            let old_task = self.get_task();
            let new_task = match self.param.task_table.get_task_by_id(old_task.base.id) {
                Ok(task) => task,
                Err(err) if err.is(&StorageError::TaskNotFound) => return,
                Err(err) => {
                    self.sample_logger().warn("refresh task failed", &[error_field(&err)]);
                    continue;
                }
            };

            if old_task.meta != new_task.meta {
                self.logger.info(
                    "task meta modification applied",
                    &[
                        str_field("oldStep", step_2_str(&old_task.base.tp, old_task.base.step)),
                        str_field("newStep", step_2_str(&new_task.base.tp, new_task.base.step)),
                    ],
                );
                // when task switch to next step, task meta might change too, but in
                // this case step executor will be recreated with new required slots and
                // meta, so we only notify it when it's still running the same step.
                if let Some((step_exec, step_ctx)) = self.step_exec_and_ctx() {
                    if step_exec.get_step() == new_task.base.step {
                        self.logger.info("notify step executor to update task meta", &[]);
                        if let Err(err2) = step_exec.task_meta_modified(&step_ctx, &new_task.meta) {
                            self.logger.info("notify step executor failed, will try recreate it later", &[error_field(&err2)]);
                            self.clean_step_executor();
                            continue;
                        }
                    }
                }
            }
            if new_task.base.required_slots != old_task.base.required_slots {
                if !self.param.slot_mgr.exchange(&new_task.base) {
                    self.logger.info(
                        "task required slots modified, but not enough slots, executor exit",
                        &[
                            i64_field("old", old_task.base.required_slots as i64),
                            i64_field("new", new_task.base.required_slots as i64),
                        ],
                    );
                    return;
                }
                self.logger.info(
                    "task required slots modification applied",
                    &[
                        i64_field("old", old_task.base.required_slots as i64),
                        i64_field("new", new_task.base.required_slots as i64),
                        i64_field("availableSlots", self.param.slot_mgr.available_slots() as i64),
                    ],
                );
                let new_resource = self.param.node_rc.get_step_resource(&new_task.base);

                if let Some(step_exec) = self.step_exec() {
                    tidb_dxf_execute::StepExecFrameworkInfo::set_resource(step_exec.as_ref(), Arc::new(new_resource));
                }
            }

            self.store_task(new_task);
            let task = self.get_task();
            if task.base.state != TASK_STATE_RUNNING && task.base.state != TASK_STATE_MODIFYING {
                return;
            }

            let subtask = match self.param.task_table.get_first_subtask_in_states(
                &self.param.exec_id,
                task.base.id,
                task.base.step,
                &[SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING],
            ) {
                Err(err) => {
                    self.sample_logger().warn("get first subtask meets error", &[error_field(&err)]);
                    continue;
                }
                Ok(None) => {
                    #[cfg(any(test, feature = "failpoints"))]
                    if fail::eval("avoidTaskExecutorExitWhenNoSubtask", |_| ()).is_some() {
                        no_subtask_check_cnt = 0;
                    }
                    if no_subtask_check_cnt >= MAX_CHECKS_WHEN_NO_SUBTASK {
                        self.logger.info("no subtask to run for a while, exit", &[]);
                        break;
                    }
                    check_interval = Duration::from_nanos(backoffer.backoff(no_subtask_check_cnt) as u64);
                    no_subtask_check_cnt += 1;
                    continue;
                }
                Ok(Some(subtask)) => subtask,
            };
            // reset it when we get a subtask
            (check_interval, no_subtask_check_cnt) = (subtask_check_interval(), 0);

            if let Some(step_exec) = self.step_exec() {
                if step_exec.get_step() != subtask.base.step {
                    self.clean_step_executor();
                }
            }
            if self.step_exec().is_none() {
                if let Err(err2) = self.create_step_executor() {
                    self.sample_logger().error(
                        "create step executor failed",
                        &[str_field("step", step_2_str(&task.base.tp, task.base.step)), error_field(&err2)],
                    );
                    continue;
                }
            }
            let (_, step_ctx) = self.step_exec_and_ctx().expect("the step executor was created");
            if let Some(err) = step_ctx.err() {
                self.logger.error(
                    "step executor context is done, the task should have been reverted",
                    &[str_field("step", step_2_str(&task.base.tp, task.base.step)), error_field(&err)],
                );
                continue;
            }
            match self.run_subtask(subtask) {
                Err(err) => {
                    // task executor keeps running its subtasks even though some subtask
                    // might have failed, we rely on scheduler to detect the error, and
                    // notify task executor or manager to cancel.
                    if is_err(as_dyn(err.as_ref()), TaskExecutorError::CancelSubtask)
                        || tidb_lightning_log::is_context_canceled_error(Some(as_dyn(err.as_ref())))
                    {
                        // Cancellation is expected when scheduler/manager cancels executor or
                        // the task enters reverting, so log as info instead of a subtask failure.
                        let short = tidb_lightning_log::short_error(Some(as_dyn(err.as_ref())));
                        self.sample_logger().info("subtask run canceled", short.as_slice());
                    } else {
                        self.sample_logger().error("run subtask failed", &[error_field(&err)]);
                    }
                }
                // if we run a subtask successfully, we will try to run next subtask
                // immediately for once.
                Ok(()) => skip_backoff = true,
            }
        }
    }

    /// Go `createStepExecutor`.
    fn create_step_executor(&self) -> std::result::Result<(), Error> {
        let task = self.get_task();

        let step_executor = match self.ext().get_step_executor(&task) {
            Ok(step_executor) => step_executor,
            Err(err) => {
                self.logger.info("failed to get step executor", &[error_field(&err)]);
                self.fail_one_subtask(&self.ctx, task.base.id, &err);
                return Err(err);
            }
        };
        let resource = Arc::new(self.param.node_rc.get_step_resource(&self.get_task_base()));
        let (update_table, get_table) = (Arc::clone(&self.param.task_table), Arc::clone(&self.param.task_table));
        tidb_dxf_execute::set_framework_info(
            step_executor.as_ref(),
            &task,
            Arc::clone(&resource),
            Some(Arc::new(move |_: &Context, subtask_id, checkpoint: &serde_json::Value| {
                update_table.update_subtask_checkpoint(subtask_id, checkpoint).map_err(|err| Box::new(err) as tidb_dxf_execute::Error)
            })),
            Some(Arc::new(move |_: &Context, subtask_id| {
                get_table.get_subtask_checkpoint(subtask_id).map_err(|err| Box::new(err) as tidb_dxf_execute::Error)
            })),
        );

        if let Err(err) = step_executor.init(&self.ctx) {
            let err: Error = Arc::from(err);
            if self.is_retryable_error(as_dyn(err.as_ref())) {
                tidb_dxf_dxfmetric::EXECUTE_EVENT_COUNTER
                    .with_label_values(&[&task.base.id.to_string(), tidb_dxf_dxfmetric::EVENT_RETRY])
                    .inc();
                self.logger.info("meet retryable err when init step executor", &[error_field(&err)]);
            } else {
                self.logger.info("failed to init step executor", &[error_field(&err)]);
                self.fail_one_subtask(&self.ctx, task.base.id, &err);
            }
            return Err(err);
        }

        // Go also logs `mem-limit-percent` (gctuner) and `server-mem-limit`
        // (memory.ServerMemoryLimitOriginText), which are not ported.
        let step_logger = tidb_lightning_log::begin_logutil_task(
            self.logger.with_fields(&[
                str_field("step", step_2_str(&task.base.tp, task.base.step)),
                str_field("resource", resource.to_string()),
            ]),
            "execute task step",
        );

        let (run_step_ctx, run_step_cancel) = Context::with_cancel_cause(&self.ctx);
        *self.step.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(StepState { exec: step_executor, ctx: run_step_ctx, logger: step_logger });
        *self.runtime_cancel.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(run_step_cancel);
        Ok(())
    }

    /// Go `cleanStepExecutor`.
    fn clean_step_executor(&self) {
        let step = self
            .step
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("the step executor runs");
        if let Err(err2) = step.exec.cleanup(&self.ctx) {
            self.logger.error("cleanup subtask exec env failed", &[error_field(&err2)]);
            // Cleanup is not a critical path of running subtask, so no need to
            // affect state of subtasks. there might be no subtask to change even
            // we want to if all subtasks are finished.
        }
        step.logger.end(Level::Info, None, &[]);

        if let Some(cancel) = self.runtime_cancel.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take() {
            cancel.cancel(None);
        }
    }

    /// Go `runSubtask`.
    fn run_subtask(&self, mut subtask: Subtask) -> std::result::Result<(), Error> {
        let (step_exec, step_ctx) = self.step_exec_and_ctx().expect("the step executor was created");
        if subtask.base.state == SUBTASK_STATE_RUNNING {
            if !self.ext().is_idempotent(&subtask) {
                self.logger.info(
                    "subtask in running state and is not idempotent, fail it",
                    &[i64_field("subtask-id", subtask.base.id)],
                );
                self.update_subtask_state_and_error_impl(
                    &step_ctx,
                    &subtask.base.exec_id,
                    subtask.base.id,
                    &SUBTASK_STATE_FAILED,
                    Some(&TaskExecutorError::NonIdempotentSubtask),
                )?;
                return Err(Arc::new(TaskExecutorError::NonIdempotentSubtask));
            }
            tidb_dxf_dxfmetric::EXECUTE_EVENT_COUNTER
                .with_label_values(&[&subtask.base.task_id.to_string(), tidb_dxf_dxfmetric::EVENT_SUBTASK_RERUN])
                .inc();
            self.logger.info("subtask in running state and is idempotent", &[i64_field("subtask-id", subtask.base.id)]);
        } else {
            // subtask.State == proto.SubtaskStatePending
            if let Err(err) = self.start_subtask(&step_ctx, subtask.base.id) {
                // should ignore ErrSubtaskNotFound
                // since it only means that the subtask not owned by current task executor.
                if !is_subtask_not_found(&err) {
                    self.logger.warn("start subtask meets error", &[error_field(&err)]);
                }
                return Err(err);
            }
        }

        let logger = self.logger.with_fields(&[
            i64_field("subtaskID", subtask.base.id),
            str_field("step", step_2_str(&subtask.base.tp, subtask.base.step)),
        ]);
        let log_task = tidb_lightning_log::begin_logutil_task(logger.clone(), "run subtask");
        let (subtask_ctx, subtask_cancel) = Context::with_cancel(&step_ctx);
        let _subtask_cancel = CancelOnDrop(&subtask_cancel);
        self.curr_subtask_id.store(subtask.base.id, Ordering::SeqCst);
        let (check_ctx, check_cancel) = Context::with_cancel(&subtask_ctx);
        let subtask_err: Option<Error> = std::thread::scope(|scope| {
            // Go's `defer checkCancel(); wg.Wait()`: the helpers end with the run,
            // also when it panics.
            let _check_cancel = CancelOnDrop(&check_cancel);
            scope.spawn(|| run_with_log(|| self.check_balance_subtask(&check_ctx, Some(&subtask_cancel))));
            if self.has_realtime_summary(step_exec.as_ref()) {
                step_exec.reset_summary();
                scope.spawn(|| run_with_log(|| self.update_subtask_summary_loop(&check_ctx, &subtask_ctx, step_exec.as_ref())));
            }
            scope.spawn(|| run_with_log(|| self.detect_and_handle_param_modify_loop(&check_ctx)));
            step_exec.run_subtask(&subtask_ctx, &mut subtask).err().map(Error::from)
        });
        log_task.end2(Level::Info, subtask_err.as_deref().map(as_dyn), &[]);

        if let Some(subtask_err) = subtask_err {
            if let Err(err) = self.mark_sub_task_canceled_or_failed(&subtask_ctx, &subtask, &subtask_err) {
                logger.error("failed to handle subtask error", &[error_field(&err)]);
            }
            return Err(subtask_err);
        }

        self.finish_subtask(&step_ctx, &subtask)
    }

    /// Go `hasRealtimeSummary`.
    fn has_realtime_summary(&self, step_executor: &dyn StepExecutor) -> bool {
        self.param.task_table.as_task_manager().is_some() && step_executor.realtime_summary().is_some()
    }

    /// Go `detectAndHandleParamModifyLoop`. Two places detect task param
    /// modification:
    ///   - the Run loop, to apply the modifications to all later subtasks
    ///   - this loop, to try to apply them to the running subtask
    ///
    /// A step executor gets a modification applied successfully once; a
    /// failed one is retried in this loop. To do that the task in memory
    /// reflects the modifications applied successfully; those that failed
    /// here are retried in the Run loop.
    pub(crate) fn detect_and_handle_param_modify_loop(&self, ctx: &Context) {
        let ticker = crossbeam_channel::tick(read(&DETECT_PARAM_MODIFY_INTERVAL));
        let done = ctx.done();
        loop {
            crossbeam_channel::select! {
                recv(done) -> _ => return,
                recv(ticker) -> _ => {}
            }

            if let Err(err) = self.detect_and_handle_param_modify(ctx) {
                if ctx.err().is_some() {
                    return;
                }
                self.logger.warn(
                    "failed to detect and handle param modification",
                    &[i64_field("currSubtaskID", self.curr_subtask_id.load(Ordering::SeqCst)), error_field(&err)],
                );
            }
        }
    }

    /// Go `detectAndHandleParamModify`.
    pub(crate) fn detect_and_handle_param_modify(&self, ctx: &Context) -> std::result::Result<(), Error> {
        let old_task = self.get_task();
        let latest_task = self.param.task_table.get_task_by_id(old_task.base.id).map_err(|err| Arc::new(err) as Error)?;

        let meta_modified = latest_task.meta != old_task.meta;
        if latest_task.base.required_slots == old_task.base.required_slots && !meta_modified {
            return Ok(());
        }

        self.logger.info(
            "task param modification detected",
            &[
                i64_field("currSubtaskID", self.curr_subtask_id.load(Ordering::SeqCst)),
                Field::new("metaModified", Value::Bool(meta_modified)),
                i64_field("oldRequiredSlots", old_task.base.required_slots as i64),
                i64_field("newRequiredSlots", latest_task.base.required_slots as i64),
            ],
        );

        // we don't report error here, as we might fail to modify task required slots
        // due to not enough slots, we still need try to apply meta modification.
        self.try_modify_task_required_slots(ctx, &old_task, &latest_task);
        if meta_modified {
            let step_exec = self.step_exec().expect("the step executor runs a subtask");
            if let Err(err) = step_exec.task_meta_modified(ctx, &latest_task.meta) {
                return Err(Arc::new(Annotated { message: "failed to apply task param modification", cause: Arc::from(err) }));
            }
            self.meta_modify_applied(latest_task.meta.clone());
        }
        Ok(())
    }

    /// Go `tryModifyTaskRequiredSlots`.
    fn try_modify_task_required_slots(&self, ctx: &Context, old_task: &Task, latest_task: &Task) {
        let logger = self.logger.with_fields(&[
            i64_field("currSubtaskID", self.curr_subtask_id.load(Ordering::SeqCst)),
            i64_field("old", old_task.base.required_slots as i64),
            i64_field("new", latest_task.base.required_slots as i64),
        ]);
        if latest_task.base.required_slots < old_task.base.required_slots {
            // we need try to release the resource first, then free slots, to avoid
            // OOM when manager starts other task executor and start to allocate memory
            // immediately.
            let new_resource = self.param.node_rc.get_step_resource(&latest_task.base);
            let step_exec = self.step_exec().expect("the step executor runs a subtask");
            if let Err(err) = step_exec.resource_modified(ctx, &new_resource) {
                logger.warn("failed to reduce resource usage", &[error_field(&err)]);
                return;
            }
            if !self.param.slot_mgr.exchange(&latest_task.base) {
                // we are returning resource back, should not happen
                logger.warn("failed to free slots", &[]);
                tidb_intest::assert(false);
                return;
            }

            // Go triggers a GC here, as garbage may not be recycled in time
            // after the memory usage is reduced; Rust frees eagerly.
            self.required_slots_modify_applied(latest_task.base.required_slots);
        } else if latest_task.base.required_slots > old_task.base.required_slots {
            if !self.param.slot_mgr.exchange(&latest_task.base) {
                logger.info(
                    "failed to exchange slots",
                    &[i64_field("availableSlots", self.param.slot_mgr.available_slots() as i64)],
                );
                return;
            }
            let new_resource = self.param.node_rc.get_step_resource(&latest_task.base);
            let step_exec = self.step_exec().expect("the step executor runs a subtask");
            if let Err(err) = step_exec.resource_modified(ctx, &new_resource) {
                let exchanged = self.param.slot_mgr.exchange(&old_task.base);
                tidb_intest::assert(exchanged);
                logger.warn(
                    "failed to increase resource usage, return slots back",
                    &[
                        error_field(&err),
                        i64_field("availableSlots", self.param.slot_mgr.available_slots() as i64),
                        Field::new("exchanged", Value::Bool(exchanged)),
                    ],
                );
                return;
            }

            self.required_slots_modify_applied(latest_task.base.required_slots);
        }
    }

    /// Go `requiredSlotsModifyApplied`.
    fn required_slots_modify_applied(&self, new_slots: isize) {
        let mut clone = self.get_task().as_ref().clone();
        self.logger.info(
            "task required slots modification applied",
            &[
                i64_field("currSubtaskID", self.curr_subtask_id.load(Ordering::SeqCst)),
                i64_field("old", clone.base.required_slots as i64),
                i64_field("new", new_slots as i64),
                i64_field("availableSlots", self.param.slot_mgr.available_slots() as i64),
            ],
        );
        clone.base.required_slots = new_slots;
        self.store_task(clone);
    }

    /// Go `metaModifyApplied`.
    fn meta_modify_applied(&self, new_meta: Vec<u8>) {
        self.logger.info(
            "task meta modification applied",
            &[i64_field("currSubtaskID", self.curr_subtask_id.load(Ordering::SeqCst))],
        );
        let mut clone = self.get_task().as_ref().clone();
        clone.meta = new_meta;
        self.store_task(clone);
    }

    /// Go `GetTaskBase`.
    #[must_use]
    pub fn get_task_base(&self) -> TaskBase {
        self.get_task().base.clone()
    }

    /// Go `GetExecID`: the node's ID, host:port.
    #[must_use]
    pub fn get_exec_id(&self) -> &str {
        &self.param.exec_id
    }

    /// Go `cancelRunStepWith`.
    fn cancel_run_step_with(&self, cause: Error) {
        if let Some(cancel) = self.runtime_cancel.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref() {
            cancel.cancel(Some(cause));
        }
    }

    /// Go `updateSubtaskStateAndErrorImpl`. Go first injects its DXF random
    /// error failpoint (`injectfailpoint.DXFRandomErrorWithOnePercent`),
    /// which the Rust storage does not port either.
    pub(crate) fn update_subtask_state_and_error_impl(
        &self,
        ctx: &Context,
        exec_id: &str,
        subtask_id: i64,
        state: &SubtaskState,
        sub_task_err: Option<&(dyn StdError + 'static)>,
    ) -> std::result::Result<(), Error> {
        let start = Instant::now();
        // retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
        let mut backoffer = tidb_dxf_scheduler::new_retry_backoffer();
        tidb_dxf_handle::run_with_retry(ctx, tidb_dxf_scheduler::retry_sql_times(), &mut backoffer, &self.logger, |_| {
            (true, self.param.task_table.update_subtask_state_and_error(exec_id, subtask_id, state, sub_task_err))
        })
        .map_err(|err| {
            self.logger.error(
                "failed to update subtask state",
                &[
                    i64_field("subtaskID", subtask_id),
                    str_field("targetState", state.to_string()),
                    str_field("subtaskErr", sub_task_err.map(ToString::to_string).unwrap_or_default()),
                    Field::new("takes", Value::Duration(start.elapsed().as_nanos() as i64)),
                    error_field(&err),
                ],
            );
            retry_err(err)
        })
    }

    /// Go `startSubtask`: the subtask runs on the node only if the node
    /// owns it.
    fn start_subtask(&self, ctx: &Context, subtask_id: i64) -> std::result::Result<(), Error> {
        let start = Instant::now();
        // retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
        let mut backoffer = tidb_dxf_scheduler::new_retry_backoffer();
        tidb_dxf_handle::run_with_retry(ctx, tidb_dxf_scheduler::retry_sql_times(), &mut backoffer, &self.logger, |_| {
            match self.param.task_table.start_subtask(subtask_id, &self.param.exec_id) {
                // No need to retry.
                Err(err) if err.is(&StorageError::SubtaskNotFound) => (false, Err(err)),
                result => (true, result),
            }
        })
        .map_err(|err| {
            let err = retry_err(err);
            if !is_subtask_not_found(&err) {
                self.logger.error(
                    "failed to start subtask",
                    &[
                        i64_field("subtaskID", subtask_id),
                        Field::new("takes", Value::Duration(start.elapsed().as_nanos() as i64)),
                        error_field(&err),
                    ],
                );
            }
            err
        })
    }

    /// Go `finishSubtask`.
    fn finish_subtask(&self, ctx: &Context, subtask: &Subtask) -> std::result::Result<(), Error> {
        let start = Instant::now();
        // retry for 3+6+12+24+(30-4)*30 ~= 825s ~= 14 minutes
        let mut backoffer = tidb_dxf_scheduler::new_retry_backoffer();
        tidb_dxf_handle::run_with_retry(ctx, tidb_dxf_scheduler::retry_sql_times(), &mut backoffer, &self.logger, |_| {
            (true, self.param.task_table.finish_subtask(&subtask.base.exec_id, subtask.base.id, &subtask.meta))
        })
        .map_err(|err| {
            self.logger.error(
                "failed to finish subtask",
                &[
                    i64_field("subtaskID", subtask.base.id),
                    Field::new("takes", Value::Duration(start.elapsed().as_nanos() as i64)),
                    error_field(&err),
                ],
            );
            retry_err(err)
        })
    }

    /// Go `markSubTaskCanceledOrFailed`: the subtask's state after its error.
    ///   1. Only cancel subtasks when meet ErrCancelSubtask.
    ///   2. Only fail subtasks when meet non retryable error.
    ///   3. When meet other errors, don't change subtasks' state.
    fn mark_sub_task_canceled_or_failed(&self, ctx: &Context, subtask: &Subtask, st_err: &Error) -> std::result::Result<(), Error> {
        if ctx.err().is_some() {
            let cause = ctx.cause();
            if cause.as_deref().and_then(|cause| cause.downcast_ref::<TaskExecutorError>()) == Some(&TaskExecutorError::CancelSubtask) {
                self.logger.warn("subtask canceled", &[]);
                return self.update_subtask_state_and_error_impl(
                    &self.ctx,
                    &subtask.base.exec_id,
                    subtask.base.id,
                    &SUBTASK_STATE_CANCELED,
                    None,
                );
            }

            self.logger.info("meet context canceled for gracefully shutdown", &[]);
        } else if self.is_retryable_error(as_dyn(st_err.as_ref())) {
            tidb_dxf_dxfmetric::EXECUTE_EVENT_COUNTER
                .with_label_values(&[&subtask.base.task_id.to_string(), tidb_dxf_dxfmetric::EVENT_RETRY])
                .inc();
            self.logger.warn("meet retryable error", &[error_field(st_err)]);
        } else {
            self.logger.warn("subtask failed", &[error_field(st_err)]);
            return self.update_subtask_state_and_error_impl(
                &self.ctx,
                &subtask.base.exec_id,
                subtask.base.id,
                &SUBTASK_STATE_FAILED,
                Some(as_dyn(st_err.as_ref())),
            );
        }
        Ok(())
    }

    /// Go `failOneSubtask`: on a fatal error, fails a subtask so the
    /// scheduler reverts the task. Its own error is only logged: nothing
    /// more can be done about a failure to handle a fatal error.
    fn fail_one_subtask(&self, ctx: &Context, task_id: i64, subtask_err: &Error) {
        let start = Instant::now();
        let mut backoffer = tidb_dxf_scheduler::new_retry_backoffer();
        if let Err(err1) =
            tidb_dxf_handle::run_with_retry(ctx, tidb_dxf_scheduler::retry_sql_times(), &mut backoffer, &self.logger, |_| {
                (true, self.param.task_table.fail_subtask(&self.param.exec_id, task_id, Some(as_dyn(subtask_err.as_ref()))))
            })
        {
            self.logger.error(
                "fail one subtask failed",
                &[
                    str_field("subtaskErr", subtask_err.to_string()),
                    Field::new("takes", Value::Duration(start.elapsed().as_nanos() as i64)),
                    error_field(&err1),
                ],
            );
        }
    }

    /// Go `GetTaskTable`.
    #[must_use]
    pub fn get_task_table(&self) -> &Arc<dyn TaskTable> {
        &self.param.task_table
    }
}

/// Go `errors.Is(err, storage.ErrSubtaskNotFound)`.
fn is_subtask_not_found(err: &Error) -> bool {
    err.downcast_ref::<StorageError>().is_some_and(|err| err.is(&StorageError::SubtaskNotFound))
}

impl TaskExecutor for BaseTaskExecutor {
    /// Go `Init`: checks the task's runtime.
    fn init(&self, _: &Context) -> std::result::Result<(), Error> {
        let runtime = self.param.task_runtime.as_ref().ok_or_else(|| plain("the task executor has no task runtime"))?;
        tidb_dxf_dxfutil::check_task_runtime(runtime.as_ref(), &self.get_task_base().keyspace).map_err(|err| Arc::new(err) as Error)
    }
    fn run(&self) {
        Self::run(self);
    }
    fn get_task_base(&self) -> TaskBase {
        Self::get_task_base(self)
    }
    /// Go `CancelRunningSubtask`.
    fn cancel_running_subtask(&self) {
        self.cancel_run_step_with(Arc::new(TaskExecutorError::CancelSubtask));
    }
    /// Go `Cancel`.
    fn cancel(&self) {
        self.cancel.cancel();
    }
    /// Go `Close`: closes the executor when all the subtasks are complete.
    fn close(&self) {
        TaskExecutor::cancel(self);

        tidb_metrics::delete_partial_match(
            &tidb_dxf_dxfmetric::EXECUTE_EVENT_COUNTER,
            &[(tidb_dxf_dxfmetric::LBL_TASK_ID, &self.get_task_base().id.to_string())],
        );
    }
    fn is_retryable_error(&self, err: &(dyn StdError + 'static)) -> bool {
        self.ext().is_retryable_error(err)
    }
}
