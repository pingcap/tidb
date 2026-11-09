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

//! Go `interface.go`, and the package's sentinel errors.

use std::error::Error as StdError;
use std::sync::Arc;

use tidb_dxf::{Step, Subtask, SubtaskBase, SubtaskState, Task, TaskBase};
use tidb_dxf_execute::{FrameworkInfoSlot, StepExecFrameworkInfo, StepExecutor, SubtaskSummary};
use tidb_dxf_storage::{Result, SessionExecutor, TaskExecInfo};
use tidb_gocontext::Context;

/// Go's `error` across the package's interfaces.
pub type Error = Arc<dyn StdError + Send + Sync>;

/// Go's sentinel errors `ErrCancelSubtask` and `ErrNonIdempotentSubtask`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExecutorError {
    /// Go `ErrCancelSubtask`: the cancel cause when cancelling subtasks.
    /// Step executors may also return it from `RunSubtask`, for example when
    /// propagating the context's cause; the framework treats it as an
    /// expected cancellation instead of a subtask failure.
    CancelSubtask,
    /// Go `ErrNonIdempotentSubtask`: the subtask was left in running state
    /// and is not idempotent, so cannot be run again.
    NonIdempotentSubtask,
}

impl std::fmt::Display for TaskExecutorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::CancelSubtask => "cancel subtasks",
            Self::NonIdempotentSubtask => "subtask in running state and is not idempotent",
        })
    }
}

impl StdError for TaskExecutorError {}

/// Go `errors.Is(err, target)` for the package's sentinels.
pub(crate) fn is_err(err: &(dyn StdError + 'static), target: TaskExecutorError) -> bool {
    let mut current = Some(err);
    while let Some(err) = current {
        if err.downcast_ref::<TaskExecutorError>() == Some(&target) {
            return true;
        }
        current = err.source();
    }
    false
}

/// Go `TaskTable`: the task table as a node's executors reach it.
pub trait TaskTable: SessionExecutor {
    /// Go `GetTaskExecInfoByExecID`: the infos of the tasks with
    /// executable subtasks on the node; a task without any is left out.
    ///
    /// # Errors
    /// The query failed.
    fn get_task_exec_info_by_exec_id(&self, exec_id: &str) -> Result<Vec<TaskExecInfo>>;
    /// Go `GetTaskByID`.
    ///
    /// # Errors
    /// No such task, or the query failed.
    fn get_task_by_id(&self, task_id: i64) -> Result<Task>;
    /// Go `GetTaskBaseByID`.
    ///
    /// # Errors
    /// No such task, or the query failed.
    fn get_task_base_by_id(&self, task_id: i64) -> Result<TaskBase>;
    /// Go `GetSubtasksByExecIDAndStepAndStates`.
    ///
    /// # Errors
    /// The query failed.
    fn get_subtasks_by_exec_id_and_step_and_states(
        &self,
        exec_id: &str,
        task_id: i64,
        step: Step,
        states: &[SubtaskState],
    ) -> Result<Vec<Subtask>>;
    /// Go `GetFirstSubtaskInStates`.
    ///
    /// # Errors
    /// The query failed.
    fn get_first_subtask_in_states(
        &self,
        instance_id: &str,
        task_id: i64,
        step: Step,
        states: &[SubtaskState],
    ) -> Result<Option<Subtask>>;
    /// Go `InitMeta`: inserts the node into `dist_framework_meta`, when the
    /// executor starts or the service scope is set.
    ///
    /// # Errors
    /// The statement failed.
    fn init_meta(&self, exec_id: &str, role: &str) -> Result<()>;
    /// Go `RecoverMeta`: inserts the node's meta back, periodically.
    ///
    /// # Errors
    /// The statement failed.
    fn recover_meta(&self, exec_id: &str, role: &str) -> Result<()>;
    /// Go `StartSubtask`: the subtask runs if the node owns it.
    ///
    /// # Errors
    /// The node does not own the subtask (`ErrSubtaskNotFound`), or the
    /// statement failed.
    fn start_subtask(&self, subtask_id: i64, exec_id: &str) -> Result<()>;
    /// Go `UpdateSubtaskStateAndError`.
    ///
    /// # Errors
    /// The statement failed.
    fn update_subtask_state_and_error(
        &self,
        exec_id: &str,
        subtask_id: i64,
        state: &SubtaskState,
        err: Option<&(dyn StdError + 'static)>,
    ) -> Result<()>;
    /// Go `FailSubtask`: fails the task's subtask on the node with `err`.
    ///
    /// # Errors
    /// The statement failed.
    fn fail_subtask(&self, exec_id: &str, task_id: i64, err: Option<&(dyn StdError + 'static)>) -> Result<()>;
    /// Go `CancelSubtask`.
    ///
    /// # Errors
    /// The statement failed.
    fn cancel_subtask(&self, exec_id: &str, task_id: i64) -> Result<()>;
    /// Go `FinishSubtask`: stores the subtask meta and marks it succeeded.
    ///
    /// # Errors
    /// The statement failed.
    fn finish_subtask(&self, exec_id: &str, subtask_id: i64, meta: &[u8]) -> Result<()>;
    /// Go `PauseSubtasks`.
    ///
    /// # Errors
    /// The statement failed.
    fn pause_subtasks(&self, exec_id: &str, task_id: i64) -> Result<()>;
    /// Go `RunningSubtasksBack2Pending`: the node's running subtasks back to
    /// pending.
    ///
    /// # Errors
    /// The statement failed.
    fn running_subtasks_back2_pending(&self, subtasks: &[SubtaskBase]) -> Result<()>;
    /// Go `UpdateSubtaskCheckpoint`.
    ///
    /// # Errors
    /// The statement failed.
    fn update_subtask_checkpoint(&self, subtask_id: i64, checkpoint: &serde_json::Value) -> Result<()>;
    /// Go `GetSubtaskCheckpoint`.
    ///
    /// # Errors
    /// The query failed.
    fn get_subtask_checkpoint(&self, subtask_id: i64) -> Result<String>;
    /// Go's `taskTable.(*storage.TaskManager)`.
    fn as_task_manager(&self) -> Option<&tidb_dxf_storage::TaskManager> {
        None
    }
}

impl TaskTable for tidb_dxf_storage::TaskManager {
    fn get_task_exec_info_by_exec_id(&self, exec_id: &str) -> Result<Vec<TaskExecInfo>> {
        Self::get_task_exec_info_by_exec_id(self, exec_id)
    }
    fn get_task_by_id(&self, task_id: i64) -> Result<Task> {
        Self::get_task_by_id(self, task_id)
    }
    fn get_task_base_by_id(&self, task_id: i64) -> Result<TaskBase> {
        Self::get_task_base_by_id(self, task_id)
    }
    fn get_subtasks_by_exec_id_and_step_and_states(
        &self,
        exec_id: &str,
        task_id: i64,
        step: Step,
        states: &[SubtaskState],
    ) -> Result<Vec<Subtask>> {
        Self::get_subtasks_by_exec_id_and_step_and_states(self, exec_id, task_id, step, states)
    }
    fn get_first_subtask_in_states(
        &self,
        instance_id: &str,
        task_id: i64,
        step: Step,
        states: &[SubtaskState],
    ) -> Result<Option<Subtask>> {
        Self::get_first_subtask_in_states(self, instance_id, task_id, step, states)
    }
    fn init_meta(&self, exec_id: &str, role: &str) -> Result<()> {
        Self::init_meta(self, exec_id, role)
    }
    fn recover_meta(&self, exec_id: &str, role: &str) -> Result<()> {
        Self::recover_meta(self, exec_id, role)
    }
    fn start_subtask(&self, subtask_id: i64, exec_id: &str) -> Result<()> {
        Self::start_subtask(self, subtask_id, exec_id)
    }
    fn update_subtask_state_and_error(
        &self,
        exec_id: &str,
        subtask_id: i64,
        state: &SubtaskState,
        err: Option<&(dyn StdError + 'static)>,
    ) -> Result<()> {
        Self::update_subtask_state_and_error(self, exec_id, subtask_id, state, err)
    }
    fn fail_subtask(&self, exec_id: &str, task_id: i64, err: Option<&(dyn StdError + 'static)>) -> Result<()> {
        Self::fail_subtask(self, exec_id, task_id, err)
    }
    fn cancel_subtask(&self, exec_id: &str, task_id: i64) -> Result<()> {
        Self::cancel_subtask(self, exec_id, task_id)
    }
    fn finish_subtask(&self, exec_id: &str, subtask_id: i64, meta: &[u8]) -> Result<()> {
        Self::finish_subtask(self, exec_id, subtask_id, meta)
    }
    fn pause_subtasks(&self, exec_id: &str, task_id: i64) -> Result<()> {
        Self::pause_subtasks(self, exec_id, task_id)
    }
    fn running_subtasks_back2_pending(&self, subtasks: &[SubtaskBase]) -> Result<()> {
        Self::running_subtasks_back2_pending(self, subtasks)
    }
    fn update_subtask_checkpoint(&self, subtask_id: i64, checkpoint: &serde_json::Value) -> Result<()> {
        Self::update_subtask_checkpoint(self, subtask_id, checkpoint)
    }
    fn get_subtask_checkpoint(&self, subtask_id: i64) -> Result<String> {
        Self::get_subtask_checkpoint(self, subtask_id)
    }
    fn as_task_manager(&self) -> Option<&tidb_dxf_storage::TaskManager> {
        Some(self)
    }
}

/// Go `TaskExecutor`: the executor of a task on the node; every task type
/// has one.
///
/// Go's context tree of task execution:
///
/// ```text
/// Manager.ctx
/// └── TaskExecutor.ctx: Cancel cancels this one
///    └── RunStep.ctx: CancelRunningSubtask cancels this one
/// ```
pub trait TaskExecutor: Send + Sync {
    /// Go `Init`: its error is fatal and fails the task directly. `ctx` is
    /// the manager's, which the task's end does not cancel: no long-running
    /// routine may use it. The task meta must not be relied on here, as the
    /// executor may be given the task base only in the future; do that in
    /// `GetStepExecutor`.
    ///
    /// # Errors
    /// The executor cannot run the task.
    fn init(&self, ctx: &Context) -> std::result::Result<(), Error>;
    /// Go `Run`: runs the task step by step; with no subtask to run for a
    /// while (10s now) it exits, so the manager can reuse the resource. All
    /// steps are assumed to use the same resource for now.
    fn run(&self);
    /// Go `GetTaskBase`: the task, read only.
    fn get_task_base(&self) -> TaskBase;
    /// Go `CancelRunningSubtask`: cancels the running subtask and changes
    /// its state to `cancelled`; the executor keeps running, to update the
    /// subtask's state or handle the revert.
    fn cancel_running_subtask(&self);
    /// Go `Cancel`: cancels the executor and leaves the running subtask's
    /// state; unlike `Close` it does not wait for the resources.
    fn cancel(&self);
    /// Go `Close`.
    fn close(&self);
    /// Go `IsRetryableError`.
    fn is_retryable_error(&self, err: &(dyn StdError + 'static)) -> bool;
}

/// Go `Extension`: what every task type adds to its executor.
pub trait Extension: Send + Sync {
    /// Go `IsIdempotent`: whether the subtask may run again; a restart can
    /// leave a subtask in running state, and a subtask that is not
    /// idempotent is then failed.
    fn is_idempotent(&self, subtask: &Subtask) -> bool;
    /// Go `GetStepExecutor`: its error is fatal and fails the task.
    ///
    /// # Errors
    /// The task's step has no executor.
    fn get_step_executor(&self, task: &Task) -> std::result::Result<Arc<dyn StepExecutor>, Error>;
    /// Go `IsRetryableError`: whether the error is transient, so the
    /// subtasks are not failed and run again.
    fn is_retryable_error(&self, err: &(dyn StdError + 'static)) -> bool;
}

/// Go `BaseStepExecutor`: the step executor that does nothing, which task
/// types embed.
#[derive(Default)]
pub struct BaseStepExecutor {
    framework_info: FrameworkInfoSlot,
}

impl StepExecFrameworkInfo for BaseStepExecutor {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.framework_info
    }
}

impl StepExecutor for BaseStepExecutor {
    fn init(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        Ok(())
    }
    fn run_subtask(&self, _: &Context, _: &mut Subtask) -> tidb_dxf_execute::Result<()> {
        Ok(())
    }
    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        None
    }
    fn reset_summary(&self) {}
    fn cleanup(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        Ok(())
    }
    fn task_meta_modified(&self, _: &Context, _: &[u8]) -> tidb_dxf_execute::Result<()> {
        Err("not implemented".into())
    }
    fn resource_modified(&self, _: &Context, _: &tidb_dxf::StepResource) -> tidb_dxf_execute::Result<()> {
        Err("not implemented".into())
    }
}
