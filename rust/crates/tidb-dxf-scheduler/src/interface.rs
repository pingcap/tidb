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

//! Go `interface.go`.

use std::collections::HashMap;
use std::error::Error;

use tidb_dxf::{ManagedNode, Step, Subtask, SubtaskBase, SubtaskState, Task, TaskBase, TaskState};
use tidb_dxf_execute::SubtaskSummary;
use tidb_dxf_storage::{Result, SessionExecutor, SubtaskError};

/// Go `TaskManager`: the task table as the scheduler reaches it.
pub trait TaskManager: SessionExecutor {
    /// Go `GetTopUnfinishedTasks`: unfinished tasks in task order, limited
    /// to make sure tasks of low ranking can be scheduled when resources
    /// allow.
    ///
    /// # Errors
    /// The query failed.
    fn get_top_unfinished_tasks(&self) -> Result<Vec<TaskBase>>;
    /// Go `GetTopNoNeedResourceTasks`: tasks that need no resources to run
    /// (reverting, pausing, cancelling, modifying), answered fast as only a
    /// limited number of tasks is scheduled at a time.
    ///
    /// # Errors
    /// The query failed.
    fn get_top_no_need_resource_tasks(&self) -> Result<Vec<TaskBase>>;
    /// Go `GetAllTasks`.
    ///
    /// # Errors
    /// The query failed.
    fn get_all_tasks(&self) -> Result<Vec<TaskBase>>;
    /// Go `GetAllSubtasks`.
    ///
    /// # Errors
    /// The query failed.
    fn get_all_subtasks(&self) -> Result<Vec<SubtaskBase>>;
    /// Go `GetCleanupTasks`: finished tasks, limited by the cleanup batch
    /// size.
    ///
    /// # Errors
    /// The query failed.
    fn get_cleanup_tasks(&self) -> Result<Vec<Task>>;
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
    /// Go `GCSubtasks`.
    ///
    /// # Errors
    /// The statement failed.
    fn gc_subtasks(&self) -> Result<()>;
    /// Go `GetAllNodes`.
    ///
    /// # Errors
    /// The query failed.
    fn get_all_nodes(&self) -> Result<Vec<ManagedNode>>;
    /// Go `DeleteDeadNodes`.
    ///
    /// # Errors
    /// The statement failed.
    fn delete_dead_nodes(&self, nodes: &[String]) -> Result<()>;
    /// Go `TransferTasks2History`: moves the tasks and their subtasks to the
    /// history tables.
    ///
    /// # Errors
    /// A statement failed.
    fn transfer_tasks2_history(&self, tasks: &[Task]) -> Result<()>;
    /// Go `CancelTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn cancel_task(&self, task_id: i64) -> Result<()>;
    /// Go `FailTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn fail_task(&self, task_id: i64, current_state: &TaskState, task_err: Option<&(dyn Error + 'static)>) -> Result<()>;
    /// Go `RevertTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn revert_task(&self, task_id: i64, task_state: &TaskState, task_err: Option<&(dyn Error + 'static)>) -> Result<()>;
    /// Go `AwaitingResolveTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn awaiting_resolve_task(&self, task_id: i64, task_state: &TaskState, task_err: Option<&(dyn Error + 'static)>) -> Result<()>;
    /// Go `RevertedTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn reverted_task(&self, task_id: i64) -> Result<()>;
    /// Go `PauseTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn pause_task(&self, task_key: &str) -> Result<bool>;
    /// Go `PauseTaskOnError`.
    ///
    /// # Errors
    /// The task changed, or a statement failed.
    fn pause_task_on_error(
        &self,
        task_id: i64,
        task_state: &TaskState,
        step: Step,
        task_err: Option<&(dyn Error + 'static)>,
    ) -> Result<()>;
    /// Go `PausedTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn paused_task(&self, task_id: i64) -> Result<()>;
    /// Go `ResumedTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn resumed_task(&self, task_id: i64) -> Result<()>;
    /// Go `ModifiedTask`.
    ///
    /// # Errors
    /// A statement failed.
    fn modified_task(&self, task: &Task) -> Result<()>;
    /// Go `SucceedTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn succeed_task(&self, task_id: i64) -> Result<()>;
    /// Go `SwitchTaskStep`.
    ///
    /// # Errors
    /// A statement failed.
    fn switch_task_step(&self, task: &Task, next_state: &TaskState, next_step: Step, subtasks: &[Subtask]) -> Result<()>;
    /// Go `SwitchTaskStepInBatch`.
    ///
    /// # Errors
    /// The subtasks are unstable, or a statement failed.
    fn switch_task_step_in_batch(
        &self,
        task: &Task,
        next_state: &TaskState,
        next_step: Step,
        subtasks: &[Subtask],
    ) -> Result<()>;
    /// Go `SwitchTaskStepAfterPrepare`.
    ///
    /// # Errors
    /// The statement failed.
    fn switch_task_step_after_prepare(&self, task: &Task) -> Result<bool>;
    /// Go `GetUsedSlotsOnNodes`.
    ///
    /// # Errors
    /// The query failed.
    fn get_used_slots_on_nodes(&self) -> Result<HashMap<String, isize>>;
    /// Go `GetActiveSubtasks`.
    ///
    /// # Errors
    /// The query failed.
    fn get_active_subtasks(&self, task_id: i64) -> Result<Vec<SubtaskBase>>;
    /// Go `GetSubtaskCntGroupByStates`.
    ///
    /// # Errors
    /// The query failed.
    fn get_subtask_cnt_group_by_states(&self, task_id: i64, step: Step) -> Result<HashMap<SubtaskState, i64>>;
    /// Go `GetSubtaskStateCntAndErrorsByStep`.
    ///
    /// # Errors
    /// The query failed.
    fn get_subtask_state_cnt_and_errors_by_step(
        &self,
        task_id: i64,
        step: Step,
    ) -> Result<(HashMap<SubtaskState, i64>, Vec<SubtaskError>)>;
    /// Go `ResumeSubtasks`.
    ///
    /// # Errors
    /// The statement failed.
    fn resume_subtasks(&self, task_id: i64) -> Result<()>;
    /// Go `GetSubtaskErrors`.
    ///
    /// # Errors
    /// The query failed.
    fn get_subtask_errors(&self, task_id: i64) -> Result<Vec<Option<SubtaskError>>>;
    /// Go `UpdateSubtasksExecIDs`.
    ///
    /// # Errors
    /// The transaction failed.
    fn update_subtasks_exec_ids(&self, subtasks: &[SubtaskBase]) -> Result<()>;
    /// Go `GetAllSubtasksByStepAndState`.
    ///
    /// # Errors
    /// The query failed.
    fn get_all_subtasks_by_step_and_state(&self, task_id: i64, step: Step, state: &SubtaskState) -> Result<Vec<Subtask>>;
    /// Go `GetAllSubtaskSummaryByStep`.
    ///
    /// # Errors
    /// The query failed.
    fn get_all_subtask_summary_by_step(&self, task_id: i64, step: Step) -> Result<Vec<SubtaskSummary>>;
}

impl TaskManager for tidb_dxf_storage::TaskManager {
    fn get_top_unfinished_tasks(&self) -> Result<Vec<TaskBase>> {
        Self::get_top_unfinished_tasks(self)
    }
    fn get_top_no_need_resource_tasks(&self) -> Result<Vec<TaskBase>> {
        Self::get_top_no_need_resource_tasks(self)
    }
    fn get_all_tasks(&self) -> Result<Vec<TaskBase>> {
        Self::get_all_tasks(self)
    }
    fn get_all_subtasks(&self) -> Result<Vec<SubtaskBase>> {
        Self::get_all_subtasks(self)
    }
    fn get_cleanup_tasks(&self) -> Result<Vec<Task>> {
        Self::get_cleanup_tasks(self)
    }
    fn get_task_by_id(&self, task_id: i64) -> Result<Task> {
        Self::get_task_by_id(self, task_id)
    }
    fn get_task_base_by_id(&self, task_id: i64) -> Result<TaskBase> {
        Self::get_task_base_by_id(self, task_id)
    }
    fn gc_subtasks(&self) -> Result<()> {
        Self::gc_subtasks(self)
    }
    fn get_all_nodes(&self) -> Result<Vec<ManagedNode>> {
        Self::get_all_nodes(self)
    }
    fn delete_dead_nodes(&self, nodes: &[String]) -> Result<()> {
        Self::delete_dead_nodes(self, nodes)
    }
    fn transfer_tasks2_history(&self, tasks: &[Task]) -> Result<()> {
        Self::transfer_tasks2_history(self, tasks)
    }
    fn cancel_task(&self, task_id: i64) -> Result<()> {
        Self::cancel_task(self, task_id)
    }
    fn fail_task(&self, task_id: i64, current_state: &TaskState, task_err: Option<&(dyn Error + 'static)>) -> Result<()> {
        Self::fail_task(self, task_id, current_state, task_err)
    }
    fn revert_task(&self, task_id: i64, task_state: &TaskState, task_err: Option<&(dyn Error + 'static)>) -> Result<()> {
        Self::revert_task(self, task_id, task_state, task_err)
    }
    fn awaiting_resolve_task(&self, task_id: i64, task_state: &TaskState, task_err: Option<&(dyn Error + 'static)>) -> Result<()> {
        Self::awaiting_resolve_task(self, task_id, task_state, task_err)
    }
    fn reverted_task(&self, task_id: i64) -> Result<()> {
        Self::reverted_task(self, task_id)
    }
    fn pause_task(&self, task_key: &str) -> Result<bool> {
        Self::pause_task(self, task_key)
    }
    fn pause_task_on_error(
        &self,
        task_id: i64,
        task_state: &TaskState,
        step: Step,
        task_err: Option<&(dyn Error + 'static)>,
    ) -> Result<()> {
        Self::pause_task_on_error(self, task_id, task_state, step, task_err)
    }
    fn paused_task(&self, task_id: i64) -> Result<()> {
        Self::paused_task(self, task_id)
    }
    fn resumed_task(&self, task_id: i64) -> Result<()> {
        Self::resumed_task(self, task_id)
    }
    fn modified_task(&self, task: &Task) -> Result<()> {
        Self::modified_task(self, task)
    }
    fn succeed_task(&self, task_id: i64) -> Result<()> {
        Self::succeed_task(self, task_id)
    }
    fn switch_task_step(&self, task: &Task, next_state: &TaskState, next_step: Step, subtasks: &[Subtask]) -> Result<()> {
        Self::switch_task_step(self, task, next_state, next_step, subtasks)
    }
    fn switch_task_step_in_batch(
        &self,
        task: &Task,
        next_state: &TaskState,
        next_step: Step,
        subtasks: &[Subtask],
    ) -> Result<()> {
        Self::switch_task_step_in_batch(self, task, next_state, next_step, subtasks)
    }
    fn switch_task_step_after_prepare(&self, task: &Task) -> Result<bool> {
        Self::switch_task_step_after_prepare(self, task)
    }
    fn get_used_slots_on_nodes(&self) -> Result<HashMap<String, isize>> {
        Self::get_used_slots_on_nodes(self)
    }
    fn get_active_subtasks(&self, task_id: i64) -> Result<Vec<SubtaskBase>> {
        Self::get_active_subtasks(self, task_id)
    }
    fn get_subtask_cnt_group_by_states(&self, task_id: i64, step: Step) -> Result<HashMap<SubtaskState, i64>> {
        Self::get_subtask_cnt_group_by_states(self, task_id, step)
    }
    fn get_subtask_state_cnt_and_errors_by_step(
        &self,
        task_id: i64,
        step: Step,
    ) -> Result<(HashMap<SubtaskState, i64>, Vec<SubtaskError>)> {
        Self::get_subtask_state_cnt_and_errors_by_step(self, task_id, step)
    }
    fn resume_subtasks(&self, task_id: i64) -> Result<()> {
        Self::resume_subtasks(self, task_id)
    }
    fn get_subtask_errors(&self, task_id: i64) -> Result<Vec<Option<SubtaskError>>> {
        Self::get_subtask_errors(self, task_id)
    }
    fn update_subtasks_exec_ids(&self, subtasks: &[SubtaskBase]) -> Result<()> {
        Self::update_subtasks_exec_ids(self, subtasks)
    }
    fn get_all_subtasks_by_step_and_state(&self, task_id: i64, step: Step, state: &SubtaskState) -> Result<Vec<Subtask>> {
        Self::get_all_subtasks_by_step_and_state(self, task_id, step, state)
    }
    fn get_all_subtask_summary_by_step(&self, task_id: i64, step: Step) -> Result<Vec<SubtaskSummary>> {
        Self::get_all_subtask_summary_by_step(self, task_id, step)
    }
}
