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

//! Go `scheduler.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::RwLock;
use std::time::Duration;

use tidb_domain_serverinfo::serverinfo::ServerInfo;

/// Go `CheckTaskFinishedInterval`: the scheduler's tick. Exported for
/// testing.
pub static CHECK_TASK_FINISHED_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_millis(500));
/// Go `RetrySQLTimes`: the max retries of a SQL statement. Tests reduce it.
pub static RETRY_SQL_TIMES: AtomicUsize = AtomicUsize::new(30);
/// Go `RetrySQLInterval`: the initial interval between two SQL retries.
/// Tests reduce it.
pub static RETRY_SQL_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_secs(3));
/// Go `RetrySQLMaxInterval`: the max interval between two SQL retries.
pub static RETRY_SQL_MAX_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_secs(30));

/// Go `RetrySQLTimes`'s current value.
#[must_use]
pub fn retry_sql_times() -> usize {
    RETRY_SQL_TIMES.load(AtomicOrdering::SeqCst)
}

/// Go `CheckTaskFinishedInterval`'s current value.
pub(crate) fn check_task_finished_interval() -> Duration {
    *CHECK_TASK_FINISHED_INTERVAL.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Go's `infosync.GetAllServerInfo`: the cluster's server infos, which the
/// domain's server-info syncer answers (Go reads its global syncer).
pub trait ServerInfoSource: Send + Sync {
    /// Go `GetAllServerInfo`.
    ///
    /// # Errors
    /// The infos cannot be read.
    fn get_all_server_info(&self) -> Result<HashMap<String, ServerInfo>, String>;
}

impl ServerInfoSource for tidb_domain_serverinfo::serverinfo_syncer::Syncer {
    fn get_all_server_info(&self) -> Result<HashMap<String, ServerInfo>, String> {
        tidb_domain_serverinfo::serverinfo_syncer::Syncer::get_all_server_info(self)
    }
}

/// Go `GetLiveExecIDs`: the IDs of all live executor nodes.
///
/// # Errors
/// The server infos cannot be read, or there are none.
pub fn get_live_exec_ids(server_info: &dyn ServerInfoSource) -> Result<Vec<String>, String> {
    #[cfg(any(test, feature = "failpoints"))]
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/dxf/framework/scheduler/mockTaskExecutorNodes").is_some() {
        return Ok(MOCK_SERVER_INFO.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone());
    }
    let server_infos = generate_task_executor_nodes(server_info)?;
    Ok(server_infos.iter().map(tidb_util_disttask::generate_exec_id).collect())
}

/// Go `MockServerInfo`: the live exec IDs under the `mockTaskExecutorNodes`
/// failpoint. Exported for tests.
#[cfg(any(test, feature = "failpoints"))]
pub static MOCK_SERVER_INFO: RwLock<Vec<String>> = RwLock::new(Vec::new());

/// Go `MockServerInfo.Store`.
#[cfg(any(test, feature = "failpoints"))]
pub fn set_mock_server_info(exec_ids: Vec<String>) {
    *MOCK_SERVER_INFO.write().unwrap_or_else(std::sync::PoisonError::into_inner) = exec_ids;
}

/// Go `generateTaskExecutorNodes`.
fn generate_task_executor_nodes(server_info: &dyn ServerInfoSource) -> Result<Vec<ServerInfo>, String> {
    let server_infos = server_info.get_all_server_info()?;
    if server_infos.is_empty() {
        return Err("not found instance".to_owned());
    }
    Ok(server_infos.into_values().collect())
}

use std::sync::{Arc, Weak};

use tidb_dxf::{
    step_2_str, Modification, Step, Subtask, SubtaskState, Task, TaskState, MODIFY_MAX_NODE_COUNT,
    MODIFY_REQUIRED_SLOTS, STEP_DONE, STEP_INIT, STEP_PREPARED, SUBTASK_STATE_CANCELED, SUBTASK_STATE_FAILED,
    SUBTASK_STATE_PAUSED, SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING, SUBTASK_STATE_SUCCEED,
    TASK_STATE_AWAITING_RESOLUTION, TASK_STATE_CANCELLING, TASK_STATE_FAILED, TASK_STATE_MODIFYING,
    TASK_STATE_PAUSED, TASK_STATE_PAUSING, TASK_STATE_PENDING, TASK_STATE_RESUMING, TASK_STATE_REVERTED,
    TASK_STATE_REVERTING, TASK_STATE_RUNNING, TASK_STATE_SUCCEED,
};
use tidb_dxf_execute::SubtaskSummary;
use tidb_dxf_storage::{SessionContext, SessionExecutor, StorageError, TaskHandle};
use tidb_gocontext::Context;
use tidb_log::{Field, Value};

use crate::interface::{Error, Extension, Param};
use crate::nodes::filter_by_scope;
use crate::TaskManager;

/// Go `metricStateAll`.
const METRIC_STATE_ALL: &str = "all";

/// A plain error (Go `errors.New` / `errors.Errorf`).
pub(crate) fn plain(message: impl Into<String>) -> Error {
    Arc::new(StorageError::Other(message.into()))
}

fn storage(error: StorageError) -> Error {
    Arc::new(error)
}

fn str_field(name: &str, value: impl Into<String>) -> Field {
    Field::new(name, Value::Str(value.into()))
}

/// Go `Scheduler`: manages the lifetime of a task, submitting its subtasks
/// and updating its status.
pub trait Scheduler: Extension {
    /// Go `Init`: called before `ScheduleTask`; an error is fatal, the
    /// manager fails the task.
    ///
    /// # Errors
    /// The scheduler cannot run the task.
    fn init(&self) -> std::result::Result<(), Error>;
    /// Go `ScheduleTask`: schedules the task step by step.
    fn schedule_task(&self);
    /// Go `Close`: called once `Init` succeeded.
    fn close(&self);
    /// Go `GetTask`: the task, read only; other threads may read it too.
    fn get_task(&self) -> Arc<Task>;
}

/// Go `BaseScheduler`: what every task type's scheduler embeds; the task
/// type implements [`Extension`].
pub struct BaseScheduler {
    ctx: Context,
    /// Go's embedded `Param`.
    pub param: Param,
    /// The task, which other threads read: replaced whole, never changed in
    /// place.
    task: std::sync::RwLock<Arc<Task>>,
    logger: tidb_logutil::Logger,
    sample_logger: tidb_logutil::SampledLogger,
    /// Go's embedded `Extension`, which the factory MUST set.
    extension: std::sync::RwLock<Option<Weak<dyn Extension>>>,
}

impl BaseScheduler {
    /// Go `NewBaseScheduler`.
    #[must_use]
    pub fn new(ctx: &Context, task: Task, param: Param) -> Self {
        let mut fields = vec![Field::new("task-id", Value::I64(task.base.id)), str_field("task-key", task.base.key.clone())];
        if tidb_intest::IN_TEST {
            fields.push(str_field("server-id", param.server_id.clone()));
        }
        let logger = tidb_logutil::err_verbose_logger().with_fields(&fields);
        let sample_logger = tidb_dxf_handle::new_sample_err_verbose_logger(&fields);
        logger.info(
            "create base scheduler",
            &[str_field("task-type", task.base.tp.to_string()), Field::new("allocated-slots", Value::Bool(param.allocated_slots))],
        );
        Self {
            ctx: ctx.clone(),
            param,
            task: std::sync::RwLock::new(Arc::new(task)),
            logger,
            sample_logger,
            extension: std::sync::RwLock::new(None),
        }
    }

    /// Sets Go's embedded `Extension`, the task type's scheduler.
    pub fn set_extension(&self, extension: Weak<dyn Extension>) {
        *self.extension.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(extension);
    }

    fn ext(&self) -> Arc<dyn Extension> {
        self.extension
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(Weak::upgrade)
            .expect("the scheduler factory sets the extension")
    }

    fn task_mgr(&self) -> &dyn TaskManager {
        self.param.task_mgr.as_ref()
    }

    fn store_task(&self, task: Task) {
        *self.task.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(task);
    }

    /// Go `Init`: checks the task's runtime.
    ///
    /// # Errors
    /// The runtime is not the task keyspace's.
    pub fn init(&self) -> std::result::Result<(), Error> {
        let runtime = self.param.task_runtime.as_ref().ok_or_else(|| plain("the scheduler has no task runtime"))?;
        tidb_dxf_dxfutil::check_task_runtime(runtime.as_ref(), &self.get_task().base.keyspace).map_err(storage)
    }

    /// Go `ScheduleTask`.
    pub fn schedule_task(&self) {
        let task = self.get_task();
        self.logger.info(
            "schedule task",
            &[
                str_field("state", task.base.state.to_string()),
                str_field("step", step_2_str(&task.base.tp, task.base.step)),
                Field::new("requiredSlots", Value::I64(task.base.required_slots as i64)),
                str_field("prepare-mode", task.base.extra_params.prepare_mode.to_string()),
            ],
        );
        self.schedule_task_loop();
    }

    /// Go `Close`.
    pub fn close(&self) {
        tidb_metrics::delete_partial_match(
            &tidb_dxf_dxfmetric::SCHEDULE_EVENT_COUNTER,
            &[(tidb_dxf_dxfmetric::LBL_TASK_ID, &self.get_task().base.id.to_string())],
        );
    }

    /// Go `GetTask`.
    #[must_use]
    pub fn get_task(&self) -> Arc<Task> {
        Arc::clone(&self.task.read().unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    /// Go `getTaskClone`.
    fn get_task_clone(&self) -> Task {
        self.get_task().as_ref().clone()
    }

    /// Go `refreshTaskIfNeeded`: the task from the task table, whole when
    /// its state or step changed (by the user, or by another scheduler in a
    /// network partition).
    fn refresh_task_if_needed(&self) -> tidb_dxf_storage::Result<()> {
        let task = self.get_task();
        // we only query the base fields of task to reduce memory usage, other fields
        // are refreshed when needed.
        let new_task_base = self.task_mgr().get_task_base_by_id(&self.ctx, task.base.id)?;
        if new_task_base.state != task.base.state || new_task_base.step != task.base.step {
            self.logger.info(
                "task state/step changed by user or other scheduler",
                &[
                    str_field("old-state", task.base.state.to_string()),
                    str_field("new-state", new_task_base.state.to_string()),
                    str_field("old-step", step_2_str(&task.base.tp, task.base.step)),
                    str_field("new-step", step_2_str(&task.base.tp, new_task_base.step)),
                ],
            );
            let new_task = self.task_mgr().get_task_by_id(&self.ctx, task.base.id)?;
            self.store_task(new_task);
        }
        Ok(())
    }

    /// Go `scheduleTask`: schedules the task step by step.
    fn schedule_task_loop(&self) {
        let ticker = crossbeam_channel::tick(check_task_finished_interval());
        let done = self.ctx.done();
        loop {
            crossbeam_channel::select! {
                recv(done) -> _ => {
                    self.logger.info("schedule task exits", &[]);
                    return;
                }
                recv(ticker) -> _ => {}
            }

            #[cfg(any(test, feature = "failpoints"))]
            if let Some(hook) = BEFORE_REFRESH_TASK.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone() {
                hook(&self.get_task());
            }
            if let Err(err) = self.refresh_task_if_needed() {
                if err.is(&StorageError::TaskNotFound) {
                    // this can happen when task is reverted/succeed, but before
                    // We reach here when task cleanup moves it to history.
                    self.logger.debug("task not found, might be reverted/succeed/failed", &[]);
                    return;
                }
                self.sample_logger.warn("refresh task failed", &[str_field("error", err.to_string())]);
                continue;
            }
            #[cfg(any(test, feature = "failpoints"))]
            call_hook(&AFTER_REFRESH_TASK, |hook| hook(&self.get_task()));
            let task = self.get_task_clone();
            #[cfg(any(test, feature = "failpoints"))]
            if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/dxf/framework/scheduler/exitScheduler").is_some() {
                return;
            }

            let state = &task.base.state;
            let result = if *state == TASK_STATE_CANCELLING {
                self.on_cancelling()
            } else if *state == TASK_STATE_PAUSING {
                self.on_pausing()
            } else if *state == TASK_STATE_PAUSED {
                let result = self.on_paused();
                // close the scheduler.
                if result.is_ok() {
                    return;
                }
                result
            } else if *state == TASK_STATE_RESUMING {
                // need to check allocatedSlots for the following case:
                // 1. task in pausing state.
                // 2. node1 and node2 start schedulers with task in pausing state without allocatedSlots.
                // 3. node1's scheduler transfer the node from pausing to paused state.
                // 4. resume the task.
                // 5. node2 scheduler call refreshTask and get task with resuming state.
                if !self.param.allocated_slots {
                    self.logger.info("scheduler exit since not allocated slots", &[str_field("state", state.to_string())]);
                    return;
                }
                self.on_resuming()
            } else if *state == TASK_STATE_REVERTING {
                self.on_reverting()
            } else if *state == TASK_STATE_PENDING {
                // need to check allocatedSlots for the following case:
                // 1. task in modifying state, node A and B start schedulers with
                //    task in modifying state without allocatedSlots.
                // 2. node A's scheduler finished modifying, and transfer the node
                //    from modifying to pending state.
                // 3. node B's scheduler call refreshTask and get task with pending
                //    state, but this scheduler has not allocated slots.
                if !self.param.allocated_slots {
                    self.logger.info("scheduler exit since not allocated slots", &[str_field("state", state.to_string())]);
                    return;
                }
                self.on_pending()
            } else if *state == TASK_STATE_RUNNING {
                // need to check allocatedSlots for the following case:
                // 1. task in pausing state.
                // 2. node1 and node2 start schedulers with task in pausing state without allocatedSlots.
                // 3. node1's scheduler transfer the node from pausing to paused state.
                // 4. resume the task.
                // 5. node1 start another scheduler and transfer the node from resuming to running state.
                // 6. node2 scheduler call refreshTask and get task with running state.
                if !self.param.allocated_slots {
                    self.logger.info("scheduler exit since not allocated slots", &[str_field("state", state.to_string())]);
                    return;
                }
                self.on_running()
            } else if *state == TASK_STATE_MODIFYING {
                match self.on_modifying() {
                    Ok(true) => return,
                    Ok(false) => Ok(()),
                    Err(err) => Err(err),
                }
            } else if *state == TASK_STATE_SUCCEED || *state == TASK_STATE_REVERTED || *state == TASK_STATE_FAILED {
                self.on_finished();
                return;
            } else {
                Ok(())
            };
            if let Err(err) = result {
                self.sample_logger.info("schedule task meet err, reschedule it", &[str_field("error", err.to_string())]);
            }

            #[cfg(any(test, feature = "failpoints"))]
            call_hook(&MOCK_OWNER_CHANGE, |hook| hook());
        }
    }

    /// Go `onCancelling`: reverts the task.
    fn on_cancelling(&self) -> std::result::Result<(), Error> {
        let task = self.get_task();
        self.logger.info(
            "on cancelling state",
            &[str_field("state", task.base.state.to_string()), str_field("step", step_2_str(&task.base.tp, task.base.step))],
        );
        self.revert_task(plain(tidb_dxf_storage::TASK_CANCEL_MESSAGE))
    }

    /// Go `onPausing`: the task is paused once its running subtasks are.
    fn on_pausing(&self) -> std::result::Result<(), Error> {
        let mut task = self.get_task_clone();
        self.sample_logger.info(
            "on pausing state",
            &[str_field("state", task.base.state.to_string()), str_field("step", step_2_str(&task.base.tp, task.base.step))],
        );
        let cnt_by_states = self.task_mgr().get_subtask_cnt_group_by_states(&self.ctx, task.base.id, task.base.step).map_err(|err| {
            self.logger.warn("check task failed", &[str_field("error", err.to_string())]);
            storage(err)
        })?;
        let running_pending_cnt = cnt(&cnt_by_states, &SUBTASK_STATE_RUNNING) + cnt(&cnt_by_states, &SUBTASK_STATE_PENDING);
        if running_pending_cnt > 0 {
            self.logger.debug("on pausing state, this task keeps current state", &[str_field("state", task.base.state.to_string())]);
            return Ok(());
        }
        let sub_task_errs = self.get_subtask_errors_if_any_failed_or_canceled(&task, &cnt_by_states)?;
        let paused_failed = self.pause_failed_subtasks_on_kv_disk_full(&mut task, &cnt_by_states, &sub_task_errs)?;
        if paused_failed {
            self.logger.info(
                "converted failed disk-full subtasks to paused, keep task pausing",
                &[str_field("state", task.base.state.to_string())],
            );
            return Ok(());
        }

        self.logger.info("all running subtasks paused, update the task to paused state", &[]);
        self.task_mgr().paused_task(&self.ctx, task.base.id).map_err(storage)?;
        task.base.state = TASK_STATE_PAUSED;
        self.store_task(task);
        Ok(())
    }

    /// Go `pauseFailedSubtasksOnKVDiskFull`.
    fn pause_failed_subtasks_on_kv_disk_full(
        &self,
        task: &mut Task,
        cnt_by_states: &HashMap<SubtaskState, i64>,
        sub_task_errs: &[Option<Error>],
    ) -> std::result::Result<bool, Error> {
        if cnt(cnt_by_states, &SUBTASK_STATE_FAILED) == 0 {
            return Ok(false);
        }
        if !should_pause_on_kv_disk_full(task, cnt_by_states, sub_task_errs) {
            return Ok(false);
        }
        let first = sub_task_errs[0].clone();
        self.task_mgr()
            .pause_task_on_error(&self.ctx, task.base.id, &task.base.state, task.base.step, first.as_deref().map(as_dyn))
            .map_err(storage)?;
        task.error = first;
        self.store_task(task.clone());
        Ok(true)
    }

    /// Go `onPaused`.
    fn on_paused(&self) -> std::result::Result<(), Error> {
        let task = self.get_task();
        self.logger.info(
            "on paused state",
            &[str_field("state", task.base.state.to_string()), str_field("step", step_2_str(&task.base.tp, task.base.step))],
        );
        Ok(())
    }

    /// Go `onResuming`: the task runs again once its paused subtasks are
    /// pending.
    fn on_resuming(&self) -> std::result::Result<(), Error> {
        let mut task = self.get_task_clone();
        self.sample_logger.info(
            "on resuming state",
            &[str_field("state", task.base.state.to_string()), str_field("step", step_2_str(&task.base.tp, task.base.step))],
        );
        let cnt_by_states = self.task_mgr().get_subtask_cnt_group_by_states(&self.ctx, task.base.id, task.base.step).map_err(|err| {
            self.logger.warn("check task failed", &[str_field("error", err.to_string())]);
            storage(err)
        })?;
        if cnt(&cnt_by_states, &SUBTASK_STATE_PAUSED) == 0 {
            // Finish the resuming process.
            self.logger.info("all paused tasks converted to pending state, update the task to running state", &[]);
            self.task_mgr().resumed_task(&self.ctx, task.base.id).map_err(storage)?;
            task.base.state = TASK_STATE_RUNNING;
            self.store_task(task);
            return Ok(());
        }

        self.task_mgr().resume_subtasks(&self.ctx, task.base.id).map_err(storage)
    }

    /// Go `onReverting`: the task is reverted once its revert subtasks
    /// finished.
    fn on_reverting(&self) -> std::result::Result<(), Error> {
        let mut task = self.get_task_clone();
        self.logger.debug(
            "on reverting state",
            &[str_field("state", task.base.state.to_string()), str_field("step", step_2_str(&task.base.tp, task.base.step))],
        );
        let cnt_by_states = self.task_mgr().get_subtask_cnt_group_by_states(&self.ctx, task.base.id, task.base.step).map_err(|err| {
            self.logger.warn("check task failed", &[str_field("error", err.to_string())]);
            storage(err)
        })?;
        let runnable_subtask_cnt = cnt(&cnt_by_states, &SUBTASK_STATE_PENDING) + cnt(&cnt_by_states, &SUBTASK_STATE_RUNNING);
        if runnable_subtask_cnt == 0 {
            self.ext().on_done(&self.ctx, self, &mut task)?;
            self.task_mgr().reverted_task(&self.ctx, task.base.id).map_err(storage)?;
            task.base.state = TASK_STATE_REVERTED;
            let (state, error) = (task.base.state.clone(), task.error.clone());
            self.store_task(task);
            on_task_finished(&state, error.as_deref().map(as_dyn));
            return Ok(());
        }
        // Wait all subtasks in this step finishes.
        self.ext().on_tick(&self.ctx, &task);
        self.logger.debug("on reverting state, this task keeps current state", &[str_field("state", task.base.state.to_string())]);
        Ok(())
    }

    /// Go `onPending`: prepares the task when it needs it, then schedules its
    /// first step's subtasks.
    fn on_pending(&self) -> std::result::Result<(), Error> {
        let mut task = self.get_task_clone();
        self.logger.debug(
            "on pending state",
            &[str_field("state", task.base.state.to_string()), str_field("step", step_2_str(&task.base.tp, task.base.step))],
        );
        if task.base.step == STEP_INIT && task.base.extra_params.prepare_mode == tidb_dxf::PREPARE_MODE_REQUIRED {
            if let Err(err) = self.ext().on_prepare(&self.ctx, self, &mut task) {
                return self.handle_prepare_or_plan_err(err);
            }
            let switched = self.task_mgr().switch_task_step_after_prepare(&self.ctx, &task).map_err(storage)?;
            if !switched {
                return Ok(());
            }
            task.base.step = STEP_PREPARED;
            self.store_task(task);
            // fall through to switch to next step to avoid wait another tick to
            // schedule subtasks after prepare.
        }
        self.switch2_next_step()
    }

    /// Go `onRunning`: once the current step's subtasks finished, the next
    /// step.
    fn on_running(&self) -> std::result::Result<(), Error> {
        let task = self.get_task();
        self.logger.debug(
            "on running state",
            &[str_field("state", task.base.state.to_string()), str_field("step", step_2_str(&task.base.tp, task.base.step))],
        );
        // check current step finishes.
        let cnt_by_states = self.task_mgr().get_subtask_cnt_group_by_states(&self.ctx, task.base.id, task.base.step).map_err(|err| {
            self.logger.warn("check task failed", &[str_field("error", err.to_string())]);
            storage(err)
        })?;
        if cnt(&cnt_by_states, &SUBTASK_STATE_FAILED) > 0 || cnt(&cnt_by_states, &SUBTASK_STATE_CANCELED) > 0 {
            let sub_task_errs = self.get_subtask_errors_if_any_failed_or_canceled(&task, &cnt_by_states)?;
            if sub_task_errs.is_empty() {
                let task_err = plain(format!(
                    "subtasks in failed/canceled state without error, taskID {}, step {}, failed {}, canceled {}",
                    task.base.id,
                    task.base.step.0,
                    cnt(&cnt_by_states, &SUBTASK_STATE_FAILED),
                    cnt(&cnt_by_states, &SUBTASK_STATE_CANCELED)
                ));
                self.logger.warn("subtasks encounter failed/canceled states without error", &[str_field("error", task_err.to_string())]);
                return self.revert_task_or_manual_recover(task_err);
            }
            self.logger.warn(
                "subtasks encounter errors",
                &[str_field(
                    "subtask-errs",
                    format!("{:?}", sub_task_errs.iter().map(|err| err.as_ref().map_or_else(|| "<nil>".to_owned(), ToString::to_string)).collect::<Vec<_>>()),
                )],
            );
            if should_pause_on_kv_disk_full(&task, &cnt_by_states, &sub_task_errs) {
                self.task_mgr()
                    .pause_task_on_error(&self.ctx, task.base.id, &task.base.state, task.base.step, sub_task_errs[0].as_deref().map(as_dyn))
                    .map_err(storage)?;
                let mut task_clone = task.as_ref().clone();
                task_clone.base.state = TASK_STATE_PAUSING;
                task_clone.error = sub_task_errs[0].clone();
                self.store_task(task_clone);
                return Ok(());
            }
            // we only store the first error as task error.
            let first = sub_task_errs[0].clone().unwrap_or_else(|| plain("<nil>"));
            return self.revert_task_or_manual_recover(first);
        } else if is_step_succeed(&cnt_by_states) {
            return self.switch2_next_step();
        }

        // Wait all subtasks in this step finishes.
        self.ext().on_tick(&self.ctx, &task);
        self.logger.debug("on running state, this task keeps current state", &[str_field("state", task.base.state.to_string())]);
        Ok(())
    }

    /// Go `getSubtaskErrorsIfAnyFailedOrCanceled`.
    fn get_subtask_errors_if_any_failed_or_canceled(
        &self,
        task: &Task,
        cnt_by_states: &HashMap<SubtaskState, i64>,
    ) -> std::result::Result<Vec<Option<Error>>, Error> {
        if cnt(cnt_by_states, &SUBTASK_STATE_FAILED) == 0 && cnt(cnt_by_states, &SUBTASK_STATE_CANCELED) == 0 {
            return Ok(Vec::new());
        }
        self.task_mgr().get_subtask_errors(&self.ctx, task.base.id).map_err(|err| {
            self.logger.warn("collect subtask error failed", &[str_field("error", err.to_string())]);
            storage(err)
        })
    }

    /// Go `onModifying`: applies the task's modifications; whether the
    /// scheduler should be recreated.
    fn on_modifying(&self) -> std::result::Result<bool, Error> {
        let mut task = self.get_task_clone();
        self.sample_logger.info("on modifying state", &[str_field("param", format!("{:?}", task.modify_param))]);
        let mut recreate_scheduler = false;
        let mut meta_modifies: Vec<Modification> = Vec::with_capacity(task.modify_param.modifications.len());
        for m in &task.modify_param.modifications {
            if m.tp == MODIFY_REQUIRED_SLOTS {
                if task.base.required_slots == m.to as isize {
                    // shouldn't happen normally.
                    self.logger.info(
                        "task required slots not changed, skip",
                        &[Field::new("requiredSlots", Value::I64(task.base.required_slots as i64))],
                    );
                    continue;
                }
                self.logger.info(
                    "modify task required slots",
                    &[Field::new("from", Value::I64(task.base.required_slots as i64)), Field::new("to", Value::I64(m.to))],
                );
                recreate_scheduler = true;
                task.base.required_slots = m.to as isize;
            } else if m.tp == MODIFY_MAX_NODE_COUNT {
                if m.to <= 0 {
                    self.logger.warn("task max-node-count should be greater than 0, skip", &[]);
                    continue;
                }
                self.logger.info(
                    "modify task max-node-count",
                    &[Field::new("from", Value::I64(task.base.max_node_count as i64)), Field::new("to", Value::I64(m.to))],
                );
                task.base.max_node_count = m.to as isize;
            } else {
                meta_modifies.push(m.clone());
            }
        }
        if !meta_modifies.is_empty() {
            self.logger.info("modify task meta", &[str_field("modifies", format!("{meta_modifies:?}"))]);
            task.meta = self.ext().modify_meta(&task.meta, &meta_modifies)?;
        }
        self.task_mgr().modified_task(&self.ctx, &task).map_err(storage)?;
        task.base.state = task.modify_param.prev_state.clone();
        task.modify_param = tidb_dxf::ModifyParam::default();
        self.store_task(task);
        Ok(recreate_scheduler)
    }

    /// Go `onFinished`.
    fn on_finished(&self) {
        let task = self.get_task();
        self.logger.debug("schedule task, task is finished", &[str_field("state", task.base.state.to_string())]);
    }

    /// Go `switch2NextStep`: the next step's subtasks, or the task's success.
    fn switch2_next_step(&self) -> std::result::Result<(), Error> {
        let mut task = self.get_task_clone();
        let next_step = self.ext().get_next_step(&task.base);
        self.logger.info(
            "switch to next step",
            &[
                str_field("current-step", step_2_str(&task.base.tp, task.base.step)),
                str_field("next-step", step_2_str(&task.base.tp, next_step)),
                Field::new("required-slots", Value::I64(task.base.required_slots as i64)),
                Field::new("runtime-slots", Value::I64(task.base.get_runtime_slots() as i64)),
            ],
        );

        if next_step == STEP_DONE {
            self.ext().on_done(&self.ctx, self, &mut task)?;
            self.task_mgr().succeed_task(&self.ctx, task.base.id).map_err(storage)?;
            task.base.step = next_step;
            task.base.state = TASK_STATE_SUCCEED;
            let (state, error) = (task.base.state.clone(), task.error.clone());
            self.store_task(task);
            on_task_finished(&state, error.as_deref().map(as_dyn));
            return Ok(());
        }

        let nodes = self.param.node_mgr.get_nodes();
        let node_ids = filter_by_scope(&nodes, &task.base.target_scope);
        let mut eligible_nodes = get_eligible_nodes(&self.ctx, self.ext().as_ref(), &task, node_ids)?;
        if task.base.max_node_count > 0 && eligible_nodes.len() > task.base.max_node_count as usize {
            // OnNextSubtasksBatch may use len(eligibleNodes) as a hint to
            // calculate the number of subtasks, so we need to do this before
            // filtering nodes by available slots in scheduleSubtask.
            eligible_nodes.truncate(task.base.max_node_count as usize);
        }

        self.logger.info("eligible instances", &[Field::new("num", Value::I64(eligible_nodes.len() as i64))]);
        if eligible_nodes.is_empty() {
            return Err(plain("no available TiDB node to dispatch subtasks"));
        }

        let metas = match self.ext().on_next_subtasks_batch(&self.ctx, self, &mut task, &eligible_nodes, next_step) {
            Ok(metas) => metas,
            Err(err) => {
                self.logger.warn("generate part of subtasks failed", &[str_field("error", err.to_string())]);
                return self.handle_prepare_or_plan_err(err);
            }
        };

        self.schedule_sub_task(&task, next_step, metas, &eligible_nodes)?;
        task.base.step = next_step;
        task.base.state = TASK_STATE_RUNNING;
        // and OnNextSubtasksBatch might change meta of task.
        self.store_task(task);
        Ok(())
    }

    /// Go `scheduleSubTask`: the subtasks of the step, placed round-robin on
    /// the nodes with enough slots, switched to in one statement or, when
    /// large, in batches.
    fn schedule_sub_task(
        &self,
        task: &Task,
        subtask_step: Step,
        metas: Vec<Vec<u8>>,
        eligible_nodes: &[String],
    ) -> std::result::Result<(), Error> {
        self.logger.info(
            "schedule subtasks",
            &[
                str_field("state", task.base.state.to_string()),
                str_field("step", step_2_str(&task.base.tp, subtask_step)),
                Field::new("requiredSlots", Value::I64(task.base.required_slots as i64)),
                Field::new("subtasks", Value::I64(metas.len() as i64)),
            ],
        );

        // the scheduled node of the subtask might not be optimal, as we run all
        // scheduler in parallel, and update might be called too many times when
        // multiple tasks are switching to next step.
        // balancer will assign the subtasks to the right instance according to
        // the system load of all nodes.
        self.param.slot_mgr.update(&self.ctx, &self.param.node_mgr, self.task_mgr()).map_err(storage)?;
        let adjusted_eligible_nodes = self.param.slot_mgr.adjust_eligible_nodes(eligible_nodes, task.base.required_slots);
        let mut size: u64 = 0;
        let mut sub_tasks = Vec::with_capacity(metas.len());
        for (i, meta) in metas.into_iter().enumerate() {
            // our schedule target is to maximize the resource usage of all nodes while
            // fulfilling the target of schedule tasks in the task order, we will try
            // to pack the subtasks of different tasks onto as minimal number of nodes
            // as possible, to allow later tasks of higher required slots can be scheduled
            // and run, so we order nodes, see TaskManager.GetAllNodes and assign the
            // subtask to the instance in a round-robin way.
            let pos = i % adjusted_eligible_nodes.len();
            let instance_id = adjusted_eligible_nodes[pos].clone();
            self.logger.debug("create subtasks", &[str_field("instanceID", instance_id.clone())]);
            size += meta.len() as u64;
            sub_tasks.push(Subtask::new(
                subtask_step,
                task.base.id,
                task.base.tp.clone(),
                instance_id,
                task.base.required_slots,
                meta,
                i as isize + 1,
            ));
        }
        #[cfg(any(test, feature = "failpoints"))]
        call_hook(&CANCEL_BEFORE_UPDATE_TASK, |hook| hook(task.base.id));

        // as other fields and generated key and index KV takes space too, we limit
        // the size of subtasks to 80% of the transaction limit.
        let limit = ((tidb_txnkv::txn_total_size_limit() as f64 * 0.8) as u64).max(1);
        let in_batch = size >= limit;
        if in_batch {
            // On default, transaction size limit is controlled by tidb_mem_quota_query
            // which is 1G on default, so it's unlikely to reach this limit, but in
            // case user set txn-total-size-limit explicitly, we insert in batch.
            self.logger.info(
                "subtasks size exceed limit, will insert in batch",
                &[Field::new("size", Value::U64(size)), Field::new("limit", Value::U64(limit))],
            );
        }

        let mut backoffer = new_retry_backoffer();
        tidb_dxf_handle::run_with_retry(&self.ctx, retry_sql_times(), &mut backoffer, &self.logger, |_| {
            let result = if in_batch {
                self.task_mgr().switch_task_step_in_batch(&self.ctx, task, &TASK_STATE_RUNNING, subtask_step, &sub_tasks)
            } else {
                self.task_mgr().switch_task_step(&self.ctx, task, &TASK_STATE_RUNNING, subtask_step, &sub_tasks)
            };
            match result {
                Err(err) if err.is(&StorageError::UnstableSubtasks) => (false, Err(err)),
                result => (true, result),
            }
        })
        .map_err(retry_error)
    }

    /// Go `handlePrepareOrPlanErr`: retries a retryable error, else reverts.
    fn handle_prepare_or_plan_err(&self, err: Error) -> std::result::Result<(), Error> {
        let task = self.get_task();
        self.logger.warn(
            "prepare or generate plan failed",
            &[str_field("error", err.to_string()), str_field("state", task.base.state.to_string())],
        );
        if self.ext().is_retryable_err(err.as_ref()) {
            tidb_dxf_dxfmetric::SCHEDULE_EVENT_COUNTER
                .with_label_values(&[&task.base.id.to_string(), tidb_dxf_dxfmetric::EVENT_RETRY])
                .inc();
            return Err(err);
        }
        self.revert_task(err)
    }

    /// Go `revertTask`.
    fn revert_task(&self, task_err: Error) -> std::result::Result<(), Error> {
        let mut task = self.get_task_clone();
        self.task_mgr().revert_task(&self.ctx, task.base.id, &task.base.state, Some(as_dyn(task_err.as_ref()))).map_err(storage)?;
        task.base.state = TASK_STATE_REVERTING;
        task.error = Some(task_err);
        self.store_task(task);
        Ok(())
    }

    /// Go `revertTaskOrManualRecover`: a task with manual recovery awaits
    /// resolution instead of reverting.
    fn revert_task_or_manual_recover(&self, task_err: Error) -> std::result::Result<(), Error> {
        let mut task = self.get_task_clone();
        if task.base.extra_params.manual_recovery {
            self.task_mgr()
                .awaiting_resolve_task(&self.ctx, task.base.id, &task.base.state, Some(as_dyn(task_err.as_ref())))
                .map_err(storage)?;
            task.base.state = TASK_STATE_AWAITING_RESOLUTION;
            task.error = Some(task_err);
            self.store_task(task);
            return Ok(());
        }
        self.revert_task(task_err)
    }

    /// Go `GetTaskMgr`.
    #[must_use]
    pub fn get_task_mgr(&self) -> Arc<dyn TaskManager> {
        Arc::clone(&self.param.task_mgr)
    }

    /// Go `GetLogger`.
    #[must_use]
    pub fn get_logger(&self) -> &tidb_logutil::Logger {
        &self.logger
    }
}

/// A Go call-failpoint (`failpoint.InjectCall`): the test's function, if set.
#[cfg(any(test, feature = "failpoints"))]
pub type CallHook<F> = RwLock<Option<Arc<F>>>;

/// Runs a call-failpoint's function, if a test set one.
#[cfg(any(test, feature = "failpoints"))]
pub(crate) fn call_hook<F: ?Sized>(hook: &CallHook<F>, call: impl FnOnce(&F)) {
    let hook = hook.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
    if let Some(hook) = hook {
        call(&hook);
    }
}

/// Go's `mockOwnerChange` call-failpoint, after each round of scheduling.
#[cfg(any(test, feature = "failpoints"))]
pub static MOCK_OWNER_CHANGE: CallHook<dyn Fn() + Send + Sync> = RwLock::new(None);

/// Go's `afterRefreshTask` call-failpoint, which receives the task.
#[cfg(any(test, feature = "failpoints"))]
pub static AFTER_REFRESH_TASK: CallHook<dyn Fn(&Task) + Send + Sync> = RwLock::new(None);

/// Go's `cancelBeforeUpdateTask` call-failpoint, which receives the task ID.
#[cfg(any(test, feature = "failpoints"))]
pub static CANCEL_BEFORE_UPDATE_TASK: CallHook<dyn Fn(i64) + Send + Sync> = RwLock::new(None);

/// Go's `beforeRefreshTask` call-failpoint, which receives the task.
#[cfg(any(test, feature = "failpoints"))]
pub static BEFORE_REFRESH_TASK: RwLock<Option<Arc<dyn Fn(&Task) + Send + Sync>>> = RwLock::new(None);

/// Go's embedded `Extension`: the base scheduler answers for the task type.
impl Extension for BaseScheduler {
    fn on_tick(&self, ctx: &Context, task: &Task) {
        self.ext().on_tick(ctx, task);
    }
    fn on_next_subtasks_batch(
        &self,
        ctx: &Context,
        h: &dyn TaskHandle,
        task: &mut Task,
        exec_ids: &[String],
        next_step: Step,
    ) -> std::result::Result<Vec<Vec<u8>>, Error> {
        self.ext().on_next_subtasks_batch(ctx, h, task, exec_ids, next_step)
    }
    fn on_done(&self, ctx: &Context, h: &dyn TaskHandle, task: &mut Task) -> std::result::Result<(), Error> {
        self.ext().on_done(ctx, h, task)
    }
    fn get_eligible_instances(&self, ctx: &Context, task: &Task) -> std::result::Result<Vec<String>, Error> {
        self.ext().get_eligible_instances(ctx, task)
    }
    fn is_retryable_err(&self, err: &(dyn std::error::Error + 'static)) -> bool {
        self.ext().is_retryable_err(err)
    }
    fn get_next_step(&self, task: &tidb_dxf::TaskBase) -> Step {
        self.ext().get_next_step(task)
    }
    fn on_prepare(&self, ctx: &Context, h: &dyn TaskHandle, task: &mut Task) -> std::result::Result<(), Error> {
        self.ext().on_prepare(ctx, h, task)
    }
    fn modify_meta(&self, old_meta: &[u8], modifies: &[Modification]) -> std::result::Result<Vec<u8>, Error> {
        self.ext().modify_meta(old_meta, modifies)
    }
}

/// Go's `BaseScheduler` is a `Scheduler`.
impl Scheduler for BaseScheduler {
    fn init(&self) -> std::result::Result<(), Error> {
        BaseScheduler::init(self)
    }
    fn schedule_task(&self) {
        BaseScheduler::schedule_task(self);
    }
    fn close(&self) {
        BaseScheduler::close(self);
    }
    fn get_task(&self) -> Arc<Task> {
        BaseScheduler::get_task(self)
    }
}

impl SessionExecutor for BaseScheduler {
    /// Go `WithNewSession`.
    fn with_new_session(&self, f: &mut dyn FnMut(&dyn SessionContext) -> tidb_dxf_storage::Result<()>) -> tidb_dxf_storage::Result<()> {
        self.task_mgr().with_new_session(f)
    }

    /// Go `WithNewTxn`.
    fn with_new_txn(
        &self,
        ctx: &tidb_gocontext::Context,
        f: &mut dyn FnMut(&dyn SessionContext) -> tidb_dxf_storage::Result<()>,
    ) -> tidb_dxf_storage::Result<()> {
        self.task_mgr().with_new_txn(ctx, f)
    }
}

impl TaskHandle for BaseScheduler {
    /// Go `GetPreviousSubtaskMetas`: the metas of the step's succeeded
    /// subtasks.
    fn get_previous_subtask_metas(&self, task_id: i64, step: Step) -> tidb_dxf_storage::Result<Vec<Vec<u8>>> {
        let previous_subtasks =
            self.task_mgr().get_all_subtasks_by_step_and_state(&self.ctx, task_id, step, &SUBTASK_STATE_SUCCEED).map_err(|err| {
                self.logger.warn(
                    "get previous succeed subtask failed",
                    &[str_field("step", step_2_str(&self.get_task().base.tp, step))],
                );
                err
            })?;
        Ok(previous_subtasks.into_iter().map(|subtask| subtask.meta).collect())
    }

    /// Go `GetPreviousSubtaskSummary`.
    fn get_previous_subtask_summary(&self, task_id: i64, step: Step) -> tidb_dxf_storage::Result<Vec<SubtaskSummary>> {
        self.task_mgr().get_all_subtask_summary_by_step(&self.ctx, task_id, step)
    }
}

fn cnt(cnt_by_states: &HashMap<SubtaskState, i64>, state: &SubtaskState) -> i64 {
    cnt_by_states.get(state).copied().unwrap_or(0)
}

fn as_dyn<'a>(err: &'a (dyn std::error::Error + Send + Sync + 'static)) -> &'a (dyn std::error::Error + 'static) {
    err
}

fn retry_error(err: tidb_dxf_handle::RetryError<StorageError>) -> Error {
    match err {
        tidb_dxf_handle::RetryError::Func(err) => storage(err),
        tidb_dxf_handle::RetryError::Context(err) => Arc::new(err),
    }
}

/// Go `backoff.NewExponential(RetrySQLInterval, 2, RetrySQLMaxInterval)`.
#[must_use]
pub fn new_retry_backoffer() -> tidb_backoff::Exponential {
    tidb_backoff::new_exponential(
        RETRY_SQL_INTERVAL.read().unwrap_or_else(std::sync::PoisonError::into_inner).as_nanos() as i64,
        2.0,
        RETRY_SQL_MAX_INTERVAL.read().unwrap_or_else(std::sync::PoisonError::into_inner).as_nanos() as i64,
    )
}

/// Go `shouldPauseOnKVDiskFull`: every failed subtask hit TiKV's disk full,
/// and the task pauses on it.
fn should_pause_on_kv_disk_full(task: &Task, cnt_by_states: &HashMap<SubtaskState, i64>, sub_task_errs: &[Option<Error>]) -> bool {
    if !task.base.extra_params.pause_on_kv_disk_full || cnt(cnt_by_states, &SUBTASK_STATE_CANCELED) > 0 || sub_task_errs.is_empty() {
        return false;
    }
    if cnt(cnt_by_states, &SUBTASK_STATE_FAILED) != sub_task_errs.len() as i64 {
        return false;
    }
    sub_task_errs.iter().all(|err| err.as_deref().is_some_and(|err| tidb_ingestor_errdef::is_kv_disk_full_error(as_dyn(err))))
}

/// Go `isStepSucceed`.
fn is_step_succeed(cnt_by_states: &HashMap<SubtaskState, i64>) -> bool {
    cnt_by_states.is_empty() || (cnt_by_states.len() == 1 && cnt_by_states.contains_key(&SUBTASK_STATE_SUCCEED))
}

/// Go `getEligibleNodes`: the task type's eligible instances, whose
/// liveliness is not checked; the managed nodes when it names none.
pub(crate) fn get_eligible_nodes(
    ctx: &Context,
    ext: &dyn Extension,
    task: &Task,
    managed_nodes: Vec<String>,
) -> std::result::Result<Vec<String>, Error> {
    let server_nodes = ext.get_eligible_instances(ctx, task)?;
    tidb_logutil::bg_logger().debug("eligible instances", &[Field::new("num", Value::I64(server_nodes.len() as i64))]);
    if server_nodes.is_empty() {
        return Ok(managed_nodes);
    }
    Ok(server_nodes)
}

/// Go `onTaskFinished`: counts finished tasks as succeed, failed, cancelled
/// or data-error.
pub(crate) fn on_task_finished(state: &TaskState, task_err: Option<&(dyn std::error::Error + 'static)>) {
    let metric_state = get_metric_state(state, task_err);
    if !metric_state.is_empty() {
        tidb_dxf_dxfmetric::FINISHED_TASK_COUNTER.with_label_values(&[METRIC_STATE_ALL]).inc();
        tidb_dxf_dxfmetric::FINISHED_TASK_COUNTER.with_label_values(&[&metric_state]).inc();
    }
}

/// Go `getMetricState`.
fn get_metric_state(state: &TaskState, task_err: Option<&(dyn std::error::Error + 'static)>) -> String {
    if *state == TASK_STATE_SUCCEED || *state == TASK_STATE_FAILED {
        return state.to_string();
    }
    if *state == TASK_STATE_REVERTED {
        let classification = tidb_dxf_storage::classify_task_error(state, task_err);
        if !classification.is_empty() {
            return classification;
        }
        return TASK_STATE_FAILED.to_string();
    }
    String::new()
}

/// Go `IsCancelledErr`, retained for callers not yet on the storage-owned
/// cancellation classifier.
#[must_use]
pub fn is_cancelled_err(err: Option<&(dyn std::error::Error + 'static)>) -> bool {
    tidb_dxf_storage::is_cancelled_err(err)
}

#[cfg(test)]
#[path = "scheduler_nokit_tests.rs"]
mod nokit_tests;
