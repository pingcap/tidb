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
use std::error::Error as StdError;

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
    fn fail_task(&self, task_id: i64, current_state: &TaskState, task_err: Option<&(dyn StdError + 'static)>) -> Result<()>;
    /// Go `RevertTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn revert_task(&self, task_id: i64, task_state: &TaskState, task_err: Option<&(dyn StdError + 'static)>) -> Result<()>;
    /// Go `AwaitingResolveTask`.
    ///
    /// # Errors
    /// The statement failed.
    fn awaiting_resolve_task(&self, task_id: i64, task_state: &TaskState, task_err: Option<&(dyn StdError + 'static)>) -> Result<()>;
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
        task_err: Option<&(dyn StdError + 'static)>,
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
    fn fail_task(&self, task_id: i64, current_state: &TaskState, task_err: Option<&(dyn StdError + 'static)>) -> Result<()> {
        Self::fail_task(self, task_id, current_state, task_err)
    }
    fn revert_task(&self, task_id: i64, task_state: &TaskState, task_err: Option<&(dyn StdError + 'static)>) -> Result<()> {
        Self::revert_task(self, task_id, task_state, task_err)
    }
    fn awaiting_resolve_task(&self, task_id: i64, task_state: &TaskState, task_err: Option<&(dyn StdError + 'static)>) -> Result<()> {
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
        task_err: Option<&(dyn StdError + 'static)>,
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

/// The error an extension or a scheduler step returns (Go `error`); it can
/// become the task's error.
pub type Error = std::sync::Arc<dyn std::error::Error + Send + Sync>;

/// Go `Extension`: what each task type adds to the base scheduler. Go
/// embeds it in `BaseScheduler` to simulate abstract methods.
pub trait Extension: Send + Sync {
    /// Go `OnTick`: periodic work of the task type, every
    /// `CheckTaskRunningInterval`, only while the task is unfinished and
    /// error free. Keep it light: it blocks the scheduler.
    fn on_tick(&self, ctx: &tidb_gocontext::Context, task: &Task);

    /// Go `OnNextSubtasksBatch`: the subtask metas of the next step, when the
    /// task enters its first step from pending, or all scheduled subtasks
    /// finished without error. It returns nothing for `StepDone`, and must
    /// not change the task's state. `exec_ids` are the eligible execution
    /// nodes, within the task's limits such as its max node count.
    ///
    /// # Errors
    /// The subtasks cannot be generated.
    fn on_next_subtasks_batch(
        &self,
        ctx: &tidb_gocontext::Context,
        h: &dyn tidb_dxf_storage::TaskHandle,
        task: &mut Task,
        exec_ids: &[String],
        next_step: Step,
    ) -> std::result::Result<Vec<Vec<u8>>, Error>;

    /// Go `OnDone`: the task is done, succeeded or failed. Not called for a
    /// task that fails to initialize its scheduler, or of an unknown type.
    ///
    /// # Errors
    /// The task type's finishing work failed.
    fn on_done(
        &self,
        ctx: &tidb_gocontext::Context,
        h: &dyn tidb_dxf_storage::TaskHandle,
        task: &mut Task,
    ) -> std::result::Result<(), Error>;

    /// Go `GetEligibleInstances`: the instances that can run the task, such
    /// as those with more disk; none means all are eligible.
    ///
    /// # Errors
    /// The instances cannot be decided.
    fn get_eligible_instances(&self, ctx: &tidb_gocontext::Context, task: &Task) -> std::result::Result<Vec<String>, Error>;

    /// Go `IsRetryableErr`: whether an error met in the scheduler is
    /// retryable.
    fn is_retryable_err(&self, err: &(dyn std::error::Error + 'static)) -> bool;

    /// Go `GetNextStep`: the step after the task's current one. A successful
    /// task goes from `StepInit` through its business steps to `StepDone`;
    /// in prepare mode the scheduler first persists `StepPrepared`. Do not
    /// decide it by the task meta; initialize what is needed in `Init`.
    fn get_next_step(&self, task: &TaskBase) -> Step;

    /// Go `OnPrepare`: the task is pending at init and needs prepare mode. It
    /// may update the task's meta, required slots and max node count, and
    /// nothing else.
    ///
    /// # Errors
    /// The preparation failed.
    fn on_prepare(
        &self,
        ctx: &tidb_gocontext::Context,
        h: &dyn tidb_dxf_storage::TaskHandle,
        task: &mut Task,
    ) -> std::result::Result<(), Error>;

    /// Go `ModifyMeta`: the new meta after applying the modifications to the
    /// old one, while the task is modifying. The task executor notifies the
    /// change later.
    ///
    /// # Errors
    /// The modification does not apply.
    fn modify_meta(&self, old_meta: &[u8], modifies: &[tidb_dxf::Modification]) -> std::result::Result<Vec<u8>, Error>;
}

/// Go `Param`: what a scheduler is created with.
#[derive(Clone)]
pub struct Param {
    pub(crate) task_mgr: std::sync::Arc<dyn TaskManager>,
    pub(crate) node_mgr: std::sync::Arc<crate::NodeManager>,
    pub(crate) slot_mgr: std::sync::Arc<crate::SlotManager>,
    pub(crate) server_id: String,
    pub(crate) allocated_slots: bool,
    pub(crate) node_res: Option<tidb_dxf::NodeResource>,
    /// Go `TaskRuntime`: the task keyspace's runtime view, not owned; the
    /// managers release it.
    pub task_runtime: Option<tidb_dxf_dxfutil::TaskRuntime>,
}

impl Param {
    /// Go `NewParamForTest`.
    #[must_use]
    pub fn new_for_test(
        task_mgr: std::sync::Arc<dyn TaskManager>,
        runtime: Option<tidb_dxf_dxfutil::TaskRuntime>,
    ) -> Self {
        Self {
            task_mgr,
            node_mgr: std::sync::Arc::new(crate::NodeManager::new("")),
            slot_mgr: std::sync::Arc::new(crate::SlotManager::new()),
            server_id: String::new(),
            allocated_slots: false,
            node_res: None,
            task_runtime: runtime,
        }
    }

    /// Go `GetNodeResource`.
    #[must_use]
    pub fn get_node_resource(&self) -> Option<&tidb_dxf::NodeResource> {
        self.node_res.as_ref()
    }
}

/// Go `schedulerFactoryFn`: creates a task type's scheduler.
pub type SchedulerFactoryFn =
    std::sync::Arc<dyn Fn(&tidb_gocontext::Context, Task, Param) -> std::sync::Arc<dyn crate::Scheduler> + Send + Sync>;

/// Go `schedulerFactoryMap`.
static SCHEDULER_FACTORY_MAP: std::sync::LazyLock<std::sync::RwLock<HashMap<tidb_dxf::TaskType, SchedulerFactoryFn>>> =
    std::sync::LazyLock::new(Default::default);

/// Go `RegisterSchedulerFactory`: registers the scheduler factory of a task
/// type, normally before the server starts.
pub fn register_scheduler_factory(task_type: tidb_dxf::TaskType, ctor: SchedulerFactoryFn) {
    SCHEDULER_FACTORY_MAP.write().unwrap_or_else(std::sync::PoisonError::into_inner).insert(task_type, ctor);
}

/// Go `getSchedulerFactory`.
pub(crate) fn get_scheduler_factory(task_type: &tidb_dxf::TaskType) -> Option<SchedulerFactoryFn> {
    SCHEDULER_FACTORY_MAP.read().unwrap_or_else(std::sync::PoisonError::into_inner).get(task_type).cloned()
}

/// Go `ClearSchedulerFactory`: only used in test.
pub fn clear_scheduler_factory() {
    SCHEDULER_FACTORY_MAP.write().unwrap_or_else(std::sync::PoisonError::into_inner).clear();
}

/// Go `Cleaner`: the cleanup of a finished task.
pub trait Cleaner: Send + Sync {
    /// Go `Clean`: cleans up one task, which may update its meta, such as to
    /// redact sensitive info.
    ///
    /// # Errors
    /// The cleanup failed.
    fn clean(&self, ctx: &tidb_gocontext::Context, task: &mut Task) -> std::result::Result<(), Error>;

    /// Go's `cleaner.(BatchCleaner)` assertion.
    fn as_batch_cleaner(&self) -> Option<&dyn BatchCleaner> {
        None
    }
}

/// Go `BatchCleaner`: a cleaner the scheduler calls once per pass with a
/// non-empty group of tasks of the same type. When it succeeds, the whole
/// group is transferred to history together; when it fails, none is. There
/// is no atomicity across the cleanup and the transfer, so implementations
/// must be idempotent and safe to retry.
pub trait BatchCleaner: Cleaner {
    /// Go `BatchClean`.
    ///
    /// # Errors
    /// The cleanup failed.
    fn batch_clean(&self, ctx: &tidb_gocontext::Context, tasks: &mut [Task]) -> std::result::Result<(), Error>;
}

/// Go `cleanerFactoryFn`.
pub type CleanerFactoryFn = std::sync::Arc<dyn Fn() -> std::sync::Arc<dyn Cleaner> + Send + Sync>;

/// Go `cleanerFactoryMap`.
static CLEANER_FACTORY_MAP: std::sync::LazyLock<std::sync::RwLock<HashMap<tidb_dxf::TaskType, CleanerFactoryFn>>> =
    std::sync::LazyLock::new(Default::default);

/// Go `CleanUpRoutine`: the pre-`Cleaner` cleanup contract, kept for task
/// types not yet migrated; new code implements [`Cleaner`].
pub trait CleanUpRoutine: Send + Sync {
    /// Go `CleanUp`.
    ///
    /// # Errors
    /// The cleanup failed.
    fn clean_up(&self, ctx: &tidb_gocontext::Context, task: &mut Task) -> std::result::Result<(), Error>;

    /// Go's `legacy.(BatchCleanUpRoutine)` assertion.
    fn as_batch_clean_up_routine(&self) -> Option<&dyn BatchCleanUpRoutine> {
        None
    }
}

/// Go `BatchCleanUpRoutine`: the pre-`Cleaner` batched cleanup contract.
pub trait BatchCleanUpRoutine: CleanUpRoutine {
    /// Go `CleanUpBatch`.
    ///
    /// # Errors
    /// The cleanup failed.
    fn clean_up_batch(&self, ctx: &tidb_gocontext::Context, tasks: &mut [Task]) -> std::result::Result<(), Error>;
}

/// Go `legacyCleanerAdapter` and `legacyBatchCleanerAdapter`.
struct LegacyCleanerAdapter(std::sync::Arc<dyn CleanUpRoutine>);

impl Cleaner for LegacyCleanerAdapter {
    fn clean(&self, ctx: &tidb_gocontext::Context, task: &mut Task) -> std::result::Result<(), Error> {
        self.0.clean_up(ctx, task)
    }

    fn as_batch_cleaner(&self) -> Option<&dyn BatchCleaner> {
        self.0.as_batch_clean_up_routine().map(|_| self as &dyn BatchCleaner)
    }
}

impl BatchCleaner for LegacyCleanerAdapter {
    fn batch_clean(&self, ctx: &tidb_gocontext::Context, tasks: &mut [Task]) -> std::result::Result<(), Error> {
        self.0.as_batch_clean_up_routine().expect("adapted as a batch cleaner").clean_up_batch(ctx, tasks)
    }
}

/// Go `RegisterSchedulerCleanUpFactory`: adapts the legacy cleanup API, kept
/// so task types can migrate independently.
pub fn register_scheduler_clean_up_factory(
    task_type: tidb_dxf::TaskType,
    ctor: std::sync::Arc<dyn Fn() -> std::sync::Arc<dyn CleanUpRoutine> + Send + Sync>,
) {
    register_cleaner_factory(
        task_type,
        std::sync::Arc::new(move || std::sync::Arc::new(LegacyCleanerAdapter(ctor())) as std::sync::Arc<dyn Cleaner>),
    );
}

/// Go `ClearSchedulerCleanUpFactory`: clears the legacy cleanup registry.
pub fn clear_scheduler_clean_up_factory() {
    clear_cleaner_factory();
}

/// Go `RegisterCleanerFactory`: registers the cleaner factory of a task
/// type.
pub fn register_cleaner_factory(task_type: tidb_dxf::TaskType, ctor: CleanerFactoryFn) {
    CLEANER_FACTORY_MAP.write().unwrap_or_else(std::sync::PoisonError::into_inner).insert(task_type, ctor);
}

/// Go `getCleanerFactory`.
pub(crate) fn get_cleaner_factory(task_type: &tidb_dxf::TaskType) -> Option<CleanerFactoryFn> {
    CLEANER_FACTORY_MAP.read().unwrap_or_else(std::sync::PoisonError::into_inner).get(task_type).cloned()
}

/// Go `ClearCleanerFactory`: only used in test.
pub fn clear_cleaner_factory() {
    CLEANER_FACTORY_MAP.write().unwrap_or_else(std::sync::PoisonError::into_inner).clear();
}
