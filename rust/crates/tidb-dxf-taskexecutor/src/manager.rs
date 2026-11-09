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

//! Go `manager.go`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_dxf::{NodeResource, TaskBase, TASK_STATE_PAUSING, TASK_STATE_REVERTING, TASK_STATE_RUNNING};
use tidb_dxf_storage::{StorageError, TaskExecInfo};
use tidb_gocontext::{CancelFunc, Context};

use crate::task_executor::{as_dyn, error_field, i64_field, plain, read, retry_err, run_with_log, str_field};
use crate::{get_task_executor_factory, Error, Param, SlotManager, TaskExecutor, TaskTable};

/// Go `TaskCheckInterval`: how often the manager looks for tasks to run.
pub static TASK_CHECK_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_millis(300));
/// Go `SubtaskCheckInterval`: how often an executor looks for subtasks to
/// run. Exported for testing.
pub static SUBTASK_CHECK_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_millis(300));
/// Go `MaxSubtaskCheckInterval`: the most an executor waits between two
/// looks for subtasks. Exported for testing.
pub static MAX_SUBTASK_CHECK_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_secs(2));
/// Go `maxChecksWhenNoSubtask`.
pub(crate) const MAX_CHECKS_WHEN_NO_SUBTASK: isize = 7;
/// Go `recoverMetaInterval`.
const RECOVER_META_INTERVAL: Duration = Duration::from_secs(90);

pub(crate) fn subtask_check_interval() -> Duration {
    read(&SUBTASK_CHECK_INTERVAL)
}

pub(crate) fn max_subtask_check_interval() -> Duration {
    read(&MAX_SUBTASK_CHECK_INTERVAL)
}

/// Go's `beforeCallStartTaskExecutor` call hook.
#[cfg(any(test, feature = "failpoints"))]
pub static BEFORE_CALL_START_TASK_EXECUTOR: RwLock<Option<Arc<dyn Fn(&TaskBase) + Send + Sync>>> = RwLock::new(None);

/// Go `Manager`: watches the task table and manages the node's task
/// executors.
pub struct Manager {
    task_table: Arc<dyn TaskTable>,
    /// Task ID -> TaskExecutor.
    task_executors: RwLock<HashMap<i64, Arc<dyn TaskExecutor>>>,
    /// The node's ID, the server ID now, i.e. host:port.
    id: String,
    wg: Mutex<Vec<JoinHandle<()>>>,
    executor_wg: Mutex<Vec<JoinHandle<()>>>,
    ctx: Context,
    cancel: CancelFunc,
    logger: tidb_logutil::Logger,
    sample_logger: tidb_logutil::SampledLogger,
    slot_manager: Arc<SlotManager>,
    node_resource: NodeResource,
}

impl Manager {
    /// Go `NewManager`.
    #[must_use]
    pub fn new(ctx: &Context, id: &str, task_table: Arc<dyn TaskTable>, resource: NodeResource) -> Arc<Self> {
        let mut logger = tidb_logutil::err_verbose_logger();
        let mut fields = Vec::new();
        if tidb_intest::IN_TEST {
            fields.push(str_field("server-id", id));
            logger = logger.with_fields(&fields);
        }
        let sample_logger = tidb_dxf_handle::new_sample_err_verbose_logger(&fields);
        let (ctx, cancel) = Context::with_cancel(ctx);
        Arc::new(Self {
            task_table,
            task_executors: RwLock::default(),
            id: id.to_owned(),
            wg: Mutex::default(),
            executor_wg: Mutex::default(),
            ctx,
            cancel,
            logger,
            sample_logger,
            slot_manager: Arc::new(SlotManager::new(resource.total_cpu)),
            node_resource: resource,
        })
    }

    /// Go `InitMeta`: not a must-succeed step before the manager starts, as
    /// the manager recovers the meta periodically.
    ///
    /// # Errors
    /// The meta was not inserted after the retries, or the manager stopped.
    pub fn init_meta(&self) -> Result<(), Error> {
        self.run_with_retry(
            || {
                self.task_table
                    .init_meta(&self.id, &tidb_config::config_tree::config::get_global_config().instance.tidb_service_scope)
            },
            "init meta failed",
        )
    }

    /// Go `recoverMeta`.
    fn recover_meta(&self) -> Result<(), Error> {
        self.run_with_retry(
            || {
                self.task_table
                    .recover_meta(&self.id, &tidb_config::config_tree::config::get_global_config().instance.tidb_service_scope)
            },
            "recover meta failed",
        )
    }

    /// Go `tidbutil.WaitGroupWrapper.Run` with `defer tidbutil.Recover(...,
    /// loop, false)`: the loop runs again after a panic.
    fn spawn_loop(self: &Arc<Self>, name: &'static str, f: fn(&Arc<Self>)) {
        let manager = Arc::clone(self);
        let handle = std::thread::Builder::new()
            .name(format!("dxf-executor-{name}"))
            .spawn(move || loop {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&manager))) {
                    Ok(()) => return,
                    Err(panic) => tidb_logutil::log_recovered_panic(name, panic.as_ref()),
                }
            })
            .expect("the task executor manager thread spawns");
        self.wg.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(handle);
    }

    /// Go `Start`.
    ///
    /// # Errors
    /// None: Go's signature has one.
    pub fn start(self: &Arc<Self>) -> Result<(), Error> {
        self.logger.info("task executor manager start", &[]);
        self.spawn_loop("handleTasksLoop", Self::handle_tasks_loop);
        self.spawn_loop("recoverMetaLoop", Self::recover_meta_loop);
        Ok(())
    }

    /// Go `Cancel`: used in test to simulate a node shutdown.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Go `Stop`.
    pub fn stop(&self) {
        self.cancel.cancel();
        Self::wait(&self.executor_wg);
        Self::wait(&self.wg);
    }

    /// Go `WaitGroupWrapper.Wait`.
    fn wait(wg: &Mutex<Vec<JoinHandle<()>>>) {
        let handles = std::mem::take(&mut *wg.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        for handle in handles {
            let _ = handle.join();
        }
    }

    /// Go `handleTasksLoop`: handles the tasks of interested states:
    ///   - pending/running: starts the task executor.
    ///   - reverting: cancels the task executor, and marks the running
    ///     subtasks as canceled.
    ///   - pausing: cancels the task executor, and marks all the node's
    ///     pending/running subtasks as paused.
    ///
    /// Every executor handles pausing, so that no executor runs a subtask
    /// before the task is marked paused.
    fn handle_tasks_loop(self: &Arc<Self>) {
        let ticker = crossbeam_channel::tick(read(&TASK_CHECK_INTERVAL));
        let done = self.ctx.done();
        loop {
            crossbeam_channel::select! {
                recv(done) -> _ => {
                    self.logger.info("handle tasks loop done", &[]);
                    return;
                }
                recv(ticker) -> _ => {}
            }

            self.handle_tasks();

            // service scope might change, so we call WithLabelValues every time.
            tidb_dxf_dxfmetric::USED_SLOTS_GAUGE
                .with_label_values(&[&tidb_vardef::service_scope()])
                .set(self.slot_manager.used_slots() as f64);
            // Go also sets `GlobalSortUploadWorkerCount` from
            // `objstore.GetActiveUploadWorkerCount()`; `pkg/objstore` is not
            // ported.
        }
    }

    /// Go `handleTasks`: tasks run by their rank (`Task.Compare`). The
    /// manager runs the tasks of high ranking before those of low ranking;
    /// when there are not enough slots it may preempt tasks of low ranking,
    /// i.e. cancel their executors directly, so some of their subtasks are
    /// half done and run again when the task is scheduled again. When there
    /// are not enough slots for a task even after preemption, tasks of low
    /// ranking can run.
    pub(crate) fn handle_tasks(self: &Arc<Self>) {
        // we don't query task in 'modifying' state, if it's prev-state is 'pending'
        // or 'paused', then they are not executable, if it's 'running', it should be
        // queried out soon as 'modifying' is a fast process.
        // it's possible that after we create task executor for a 'running' task, it
        // enters 'modifying', as slots are allocated already, that's ok.
        let tasks = match self.task_table.get_task_exec_info_by_exec_id(&self.id) {
            Ok(tasks) => tasks,
            Err(err) => {
                self.sample_logger.error("failed to get executable task", &[error_field(&err)]);
                return;
            }
        };

        let mut executable_tasks = Vec::with_capacity(tasks.len());
        for task in tasks {
            let state = &task.task_base.state;
            if *state == TASK_STATE_RUNNING {
                if !self.is_executor_started(task.task_base.id) {
                    executable_tasks.push(task);
                }
            } else if *state == TASK_STATE_PAUSING {
                if let Err(err) = self.handle_pausing_task(task.task_base.id) {
                    self.logger.error("failed to handle task in pausing state", &[error_field(&err)]);
                }
            } else if *state == TASK_STATE_REVERTING {
                if let Err(err) = self.handle_reverting_task(task.task_base.id) {
                    self.logger.error("failed to handle task in reverting state", &[error_field(&err)]);
                }
            }
        }

        if !executable_tasks.is_empty() {
            self.handle_executable_tasks(&executable_tasks);
        }
    }

    /// Go `handleExecutableTasks`.
    pub(crate) fn handle_executable_tasks(self: &Arc<Self>, task_infos: &[TaskExecInfo]) {
        for task in task_infos {
            let (can_alloc, tasks_need_free) = self.slot_manager.can_alloc(&task.task_base);
            if !tasks_need_free.is_empty() {
                self.cancel_task_executors(&tasks_need_free);
                self.logger.info(
                    "need to preempt tasks of low ranking",
                    &[
                        str_field("task", format!("{:?}", task.task_base)),
                        str_field("preemptedTasks", format!("{tasks_need_free:?}")),
                    ],
                );
                // do not handle the tasks with low ranking if current task is waiting
                // other tasks to free slots to make sure the order of running.
                break;
            }

            if !can_alloc {
                // try to run tasks of low ranking
                self.logger.debug(
                    "no enough slots to run task",
                    &[i64_field("task-id", task.task_base.id), str_field("task-key", task.task_base.key.clone())],
                );
                continue;
            }
            #[cfg(any(test, feature = "failpoints"))]
            if let Some(hook) = BEFORE_CALL_START_TASK_EXECUTOR.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone() {
                hook(&task.task_base);
            }
            if !self.start_task_executor(&task.task_base) {
                // we break to make sure the order of running.
                // it's possible some other low ranking tasks alloc more slots at
                // runtime, in this case we should try preempt them in next iteration.
                break;
            }
        }
    }

    /// Go `cancelRunningSubtaskOf`: the task's running subtask switches to
    /// `canceled`.
    pub(crate) fn cancel_running_subtask_of(&self, task_id: i64) {
        let executors = self.task_executors.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(executor) = executors.get(&task_id) {
            self.logger.info("onCanceledTasks", &[i64_field("task-id", task_id)]);
            executor.cancel_running_subtask();
        }
    }

    /// Go `handlePausingTask`: pauses/cancels the pending/running subtasks.
    pub(crate) fn handle_pausing_task(&self, task_id: i64) -> Result<(), StorageError> {
        let executors = self.task_executors.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        self.logger.info("handle pausing task", &[i64_field("task-id", task_id)]);
        if let Some(executor) = executors.get(&task_id) {
            executor.cancel();
        }
        // we pause subtasks belongs to this exec node even when there's no executor running.
        // as balancer might move subtasks to this node when the executor hasn't started.
        self.task_table.pause_subtasks(&self.id, task_id)
    }

    /// Go `handleRevertingTask`.
    pub(crate) fn handle_reverting_task(&self, task_id: i64) -> Result<(), StorageError> {
        self.cancel_running_subtask_of(task_id);
        self.task_table.cancel_subtask(&self.id, task_id)
    }

    /// Go `recoverMetaLoop`: inserts `dist_framework_meta` back for the
    /// node. The owner deletes it after a long network partition of the
    /// node; once the partition heals, the meta must be inserted again.
    fn recover_meta_loop(self: &Arc<Self>) {
        let ticker = crossbeam_channel::tick(RECOVER_META_INTERVAL);
        let done = self.ctx.done();
        loop {
            crossbeam_channel::select! {
                recv(done) -> _ => {
                    self.logger.info("recoverMetaLoop done", &[]);
                    return;
                }
                recv(ticker) -> _ => {
                    if let Err(err) = self.recover_meta() {
                        self.logger.error("failed to recover node meta", &[error_field(&err)]);
                    }
                }
            }
        }
    }

    /// Go `cancelTaskExecutors`: unlike `cancelRunningSubtaskOf`, the
    /// subtasks' states stay.
    pub(crate) fn cancel_task_executors(&self, tasks: &[TaskBase]) {
        let executors = self.task_executors.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        for task in tasks {
            self.logger.info("cancel task executor", &[i64_field("task-id", task.id)]);
            if let Some(executor) = executors.get(&task.id) {
                executor.cancel();
            }
        }
    }

    /// Go `startTaskExecutor`: starts the runnable task's executor.
    pub(crate) fn start_task_executor(self: &Arc<Self>, task_base: &TaskBase) -> bool {
        // TODO: remove it when we can create task executor with task base.
        let task = match self.task_table.get_task_by_id(task_base.id) {
            Ok(task) => task,
            Err(err) => {
                self.logger.error(
                    "get task failed",
                    &[i64_field("task-id", task_base.id), str_field("task-key", task_base.key.clone()), error_field(&err)],
                );
                return false;
            }
        };

        if !self.slot_manager.alloc(&task.base) {
            self.logger.info(
                "alloc slots failed, maybe other task executor alloc more slots at runtime",
                &[
                    i64_field("task-id", task_base.id),
                    str_field("task-key", task_base.key.clone()),
                    i64_field("required-slots", task_base.required_slots as i64),
                    i64_field("remaining-slots", self.slot_manager.available_slots() as i64),
                ],
            );
            return false;
        }
        let task_id = task.base.id;
        let executor_started = self.start_allocated_task_executor(task_base, task);
        // free the slot if executor not started.
        if !executor_started {
            self.slot_manager.free(task_id);
        }
        executor_started
    }

    /// The rest of Go `startTaskExecutor`, once the slots are allocated.
    fn start_allocated_task_executor(self: &Arc<Self>, task_base: &TaskBase, task: tidb_dxf::Task) -> bool {
        let holder_id = tidb_dxf_dxfutil::gen_holder_id("executor", task.base.id);
        let session_provider = TaskTableSessions(Arc::clone(&self.task_table));
        let (task_runtime, release_fn) =
            match tidb_dxf_dxfutil::acquire_task_runtime(&session_provider, &task.base.keyspace, &holder_id) {
                Ok(acquired) => acquired,
                Err(err) => {
                    self.logger.warn(
                        "acquire task runtime failed",
                        &[i64_field("task-id", task_base.id), str_field("task-key", task_base.key.clone()), error_field(&err)],
                    );
                    return false;
                }
            };

        let Some(factory) = get_task_executor_factory(&task.base.tp) else {
            let err = plain(format!("task type {} not found", task.base.tp));
            self.fail_subtask(&err, task.base.id, None);
            release_fn();
            return false;
        };
        let (task_id, task_key, task_type, required_slots, runtime_slots) = (
            task.base.id,
            task.base.key.clone(),
            task.base.tp.clone(),
            task.base.required_slots,
            task.base.get_runtime_slots(),
        );
        let executor = factory(
            &self.ctx,
            task,
            Param {
                task_table: Arc::clone(&self.task_table),
                slot_mgr: Arc::clone(&self.slot_manager),
                node_rc: self.get_node_resource(),
                exec_id: self.id.clone(),
                task_runtime: Some(task_runtime),
            },
        );
        if let Err(err) = executor.init(&self.ctx) {
            self.fail_subtask(&err, task_id, Some(executor.as_ref()));
            release_fn();
            return false;
        }
        self.add_task_executor(Arc::clone(&executor));
        self.logger.info(
            "task executor started",
            &[
                i64_field("task-id", task_id),
                str_field("task-key", task_key.clone()),
                str_field("type", task_type.to_string()),
                i64_field("required-slots", required_slots as i64),
                i64_field("runtime-slots", runtime_slots as i64),
                i64_field("node-remaining-slots", self.slot_manager.available_slots() as i64),
            ],
        );
        let manager = Arc::clone(self);
        let handle = std::thread::Builder::new()
            .name(format!("dxf-task-executor-{task_id}"))
            .spawn(move || {
                run_with_log(|| executor.run());
                executor.close();
                release_fn();
                manager.del_task_executor(executor.as_ref());
                manager.slot_manager.free(task_id);
                manager.logger.info(
                    "task executor exit",
                    &[i64_field("task-id", task_id), str_field("task-key", task_key), str_field("type", task_type.to_string())],
                );
            })
            .expect("the task executor thread spawns");
        self.executor_wg.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(handle);
        true
    }

    /// Go `getNodeResource`: a copy.
    fn get_node_resource(&self) -> NodeResource {
        self.node_resource.clone()
    }

    /// Go `addTaskExecutor`.
    pub(crate) fn add_task_executor(&self, executor: Arc<dyn TaskExecutor>) {
        let id = executor.get_task_base().id;
        self.task_executors.write().unwrap_or_else(std::sync::PoisonError::into_inner).insert(id, executor);
    }

    /// Go `delTaskExecutor`.
    pub(crate) fn del_task_executor(&self, executor: &dyn TaskExecutor) {
        let id = executor.get_task_base().id;
        self.task_executors.write().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&id);
    }

    /// Go `isExecutorStarted`.
    pub(crate) fn is_executor_started(&self, task_id: i64) -> bool {
        self.task_executors.read().unwrap_or_else(std::sync::PoisonError::into_inner).contains_key(&task_id)
    }

    /// Go `failSubtask`. `Init`'s error ought to be fatal, but add-index's
    /// `Init` has code that needs retry: a retryable error is skipped until
    /// that is decoupled.
    fn fail_subtask(&self, err: &Error, task_id: i64, task_executor: Option<&dyn TaskExecutor>) {
        self.logger.error("update one subtask as failed", &[error_field(err)]);
        if task_executor.is_some_and(|executor| executor.is_retryable_error(as_dyn(err.as_ref()))) {
            self.logger.warn("met retryable err", &[error_field(err)]);
            return;
        }
        let err1 = self.run_with_retry(
            || self.task_table.fail_subtask(&self.id, task_id, Some(as_dyn(err.as_ref()))),
            "update to subtask failed",
        );
        if err1.is_ok() {
            self.logger.info("update error to subtask success", &[i64_field("task-id", task_id), error_field(err)]);
        }
    }

    /// Go `runWithRetry`.
    fn run_with_retry(&self, f: impl Fn() -> Result<(), StorageError>, msg: &str) -> Result<(), Error> {
        let mut backoffer = tidb_dxf_scheduler::new_retry_backoffer();
        tidb_dxf_handle::run_with_retry(&self.ctx, tidb_dxf_scheduler::retry_sql_times(), &mut backoffer, &self.logger, |_| {
            (true, f())
        })
        .map_err(|err1| {
            self.logger.warn(msg, &[error_field(&err1)]);
            retry_err(err1)
        })
    }

    /// Go's `m.slotManager`.
    #[cfg(test)]
    pub(crate) fn slot_manager(&self) -> &Arc<SlotManager> {
        &self.slot_manager
    }

    /// Go `len(m.mu.taskExecutors)`.
    #[cfg(test)]
    pub(crate) fn task_executor_count(&self) -> usize {
        self.task_executors.read().unwrap().len()
    }

    /// Go `m.executorWG.Wait()`, failing on an executor thread's panic.
    #[cfg(test)]
    pub(crate) fn wait_executors(&self) {
        let handles = std::mem::take(&mut *self.executor_wg.lock().unwrap());
        for handle in handles {
            if let Err(panic) = handle.join() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

/// Go's `TaskTable` as `dxfutil`'s session provider.
struct TaskTableSessions(Arc<dyn TaskTable>);

impl tidb_dxf_dxfutil::SessionProvider for TaskTableSessions {
    fn with_new_session(
        &self,
        f: &mut dyn FnMut(&dyn tidb_dxf_storage::SessionContext) -> tidb_dxf_storage::Result<()>,
    ) -> tidb_dxf_storage::Result<()> {
        self.0.with_new_session(f)
    }
}
