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

//! Go `pkg/dxf/framework/mock`'s `MockTaskManager` (gomock) and the
//! scheduler test's `MockServerInfo`: queued expectations, answered in
//! order; an unexpected call panics.

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::Mutex;

use tidb_domain_serverinfo::serverinfo::ServerInfo;
use tidb_dxf::{ManagedNode, Step, Subtask, SubtaskBase, SubtaskState, Task, TaskBase, TaskState};
use tidb_dxf_execute::SubtaskSummary;
use tidb_dxf_storage::{Result, SessionContext, SessionExecutor, SubtaskError};

use crate::scheduler::ServerInfoSource;
use crate::TaskManager;

/// Go `mock.MockTaskManager`.
#[derive(Default)]
pub(crate) struct MockTaskManager {
    expected: Mutex<HashMap<&'static str, VecDeque<Box<dyn Any + Send>>>>,
}

impl MockTaskManager {
    pub(crate) fn expect<T: Send + 'static>(&self, method: &'static str, answer: T) {
        self.expected.lock().unwrap().entry(method).or_default().push_back(Box::new(answer));
    }

    fn answer<T: 'static>(&self, method: &'static str) -> T {
        let answer = self
            .expected
            .lock()
            .unwrap()
            .get_mut(method)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| panic!("unexpected call of {method}"));
        *answer.downcast::<T>().unwrap_or_else(|_| panic!("{method} answered with the wrong type"))
    }

    /// Go `ctrl.Satisfied()`.
    pub(crate) fn assert_satisfied(&self) {
        for (method, answers) in self.expected.lock().unwrap().iter() {
            assert!(answers.is_empty(), "{} expected call(s) of {method} missing", answers.len());
        }
    }

    pub(crate) fn expect_get_used_slots_on_nodes(&self, answer: Result<HashMap<String, isize>>) {
        self.expect("GetUsedSlotsOnNodes", answer);
    }

    pub(crate) fn expect_get_all_nodes(&self, answer: Result<Vec<ManagedNode>>) {
        self.expect("GetAllNodes", answer);
    }

    pub(crate) fn expect_delete_dead_nodes(&self, answer: Result<()>) {
        self.expect("DeleteDeadNodes", answer);
    }
}

impl SessionExecutor for MockTaskManager {
    fn with_new_session(&self, _: &mut dyn FnMut(&dyn SessionContext) -> Result<()>) -> Result<()> {
        self.answer("WithNewSession")
    }
    fn with_new_txn(&self, _: &mut dyn FnMut(&dyn SessionContext) -> Result<()>) -> Result<()> {
        self.answer("WithNewTxn")
    }
}

impl TaskManager for MockTaskManager {
    fn get_top_unfinished_tasks(&self) -> Result<Vec<TaskBase>> {
        self.answer("GetTopUnfinishedTasks")
    }
    fn get_top_no_need_resource_tasks(&self) -> Result<Vec<TaskBase>> {
        self.answer("GetTopNoNeedResourceTasks")
    }
    fn get_all_tasks(&self) -> Result<Vec<TaskBase>> {
        self.answer("GetAllTasks")
    }
    fn get_all_subtasks(&self) -> Result<Vec<SubtaskBase>> {
        self.answer("GetAllSubtasks")
    }
    fn get_cleanup_tasks(&self) -> Result<Vec<Task>> {
        self.answer("GetCleanupTasks")
    }
    fn get_task_by_id(&self, _: i64) -> Result<Task> {
        self.answer("GetTaskByID")
    }
    fn get_task_base_by_id(&self, _: i64) -> Result<TaskBase> {
        self.answer("GetTaskBaseByID")
    }
    fn gc_subtasks(&self) -> Result<()> {
        self.answer("GCSubtasks")
    }
    fn get_all_nodes(&self) -> Result<Vec<ManagedNode>> {
        self.answer("GetAllNodes")
    }
    fn delete_dead_nodes(&self, _: &[String]) -> Result<()> {
        self.answer("DeleteDeadNodes")
    }
    fn transfer_tasks2_history(&self, _: &[Task]) -> Result<()> {
        self.answer("TransferTasks2History")
    }
    fn cancel_task(&self, _: i64) -> Result<()> {
        self.answer("CancelTask")
    }
    fn fail_task(&self, _: i64, _: &TaskState, _: Option<&(dyn Error + 'static)>) -> Result<()> {
        self.answer("FailTask")
    }
    fn revert_task(&self, _: i64, _: &TaskState, _: Option<&(dyn Error + 'static)>) -> Result<()> {
        self.answer("RevertTask")
    }
    fn awaiting_resolve_task(&self, _: i64, _: &TaskState, _: Option<&(dyn Error + 'static)>) -> Result<()> {
        self.answer("AwaitingResolveTask")
    }
    fn reverted_task(&self, _: i64) -> Result<()> {
        self.answer("RevertedTask")
    }
    fn pause_task(&self, _: &str) -> Result<bool> {
        self.answer("PauseTask")
    }
    fn pause_task_on_error(&self, _: i64, _: &TaskState, _: Step, _: Option<&(dyn Error + 'static)>) -> Result<()> {
        self.answer("PauseTaskOnError")
    }
    fn paused_task(&self, _: i64) -> Result<()> {
        self.answer("PausedTask")
    }
    fn resumed_task(&self, _: i64) -> Result<()> {
        self.answer("ResumedTask")
    }
    fn modified_task(&self, _: &Task) -> Result<()> {
        self.answer("ModifiedTask")
    }
    fn succeed_task(&self, _: i64) -> Result<()> {
        self.answer("SucceedTask")
    }
    fn switch_task_step(&self, _: &Task, _: &TaskState, _: Step, _: &[Subtask]) -> Result<()> {
        self.answer("SwitchTaskStep")
    }
    fn switch_task_step_in_batch(&self, _: &Task, _: &TaskState, _: Step, _: &[Subtask]) -> Result<()> {
        self.answer("SwitchTaskStepInBatch")
    }
    fn switch_task_step_after_prepare(&self, _: &Task) -> Result<bool> {
        self.answer("SwitchTaskStepAfterPrepare")
    }
    fn get_used_slots_on_nodes(&self) -> Result<HashMap<String, isize>> {
        self.answer("GetUsedSlotsOnNodes")
    }
    fn get_active_subtasks(&self, _: i64) -> Result<Vec<SubtaskBase>> {
        self.answer("GetActiveSubtasks")
    }
    fn get_subtask_cnt_group_by_states(&self, _: i64, _: Step) -> Result<HashMap<SubtaskState, i64>> {
        self.answer("GetSubtaskCntGroupByStates")
    }
    fn get_subtask_state_cnt_and_errors_by_step(
        &self,
        _: i64,
        _: Step,
    ) -> Result<(HashMap<SubtaskState, i64>, Vec<SubtaskError>)> {
        self.answer("GetSubtaskStateCntAndErrorsByStep")
    }
    fn resume_subtasks(&self, _: i64) -> Result<()> {
        self.answer("ResumeSubtasks")
    }
    fn get_subtask_errors(&self, _: i64) -> Result<Vec<Option<SubtaskError>>> {
        self.answer("GetSubtaskErrors")
    }
    fn update_subtasks_exec_ids(&self, _: &[SubtaskBase]) -> Result<()> {
        self.answer("UpdateSubtasksExecIDs")
    }
    fn get_all_subtasks_by_step_and_state(&self, _: i64, _: Step, _: &SubtaskState) -> Result<Vec<Subtask>> {
        self.answer("GetAllSubtasksByStepAndState")
    }
    fn get_all_subtask_summary_by_step(&self, _: i64, _: Step) -> Result<Vec<SubtaskSummary>> {
        self.answer("GetAllSubtaskSummaryByStep")
    }
}

/// Go `MockServerInfo` with the `mockTaskExecutorNodes` failpoint on: the
/// live exec IDs a test stores.
pub(crate) struct MockServerInfo(Mutex<Vec<String>>);

impl MockServerInfo {
    pub(crate) fn new(exec_ids: &[&str]) -> Self {
        let source = Self(Mutex::new(Vec::new()));
        source.store(exec_ids);
        source
    }

    pub(crate) fn store(&self, exec_ids: &[&str]) {
        *self.0.lock().unwrap() = exec_ids.iter().map(|id| (*id).to_owned()).collect();
    }
}

impl ServerInfoSource for MockServerInfo {
    fn get_all_server_info(&self) -> std::result::Result<HashMap<String, ServerInfo>, String> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|exec_id| {
                let (ip, port) = exec_id.rsplit_once(':').expect("an exec ID is host:port");
                let mut info = ServerInfo::default();
                info.static_info.id = exec_id.clone();
                info.static_info.ip = ip.to_owned();
                info.static_info.port = port.parse().expect("a port");
                (exec_id.clone(), info)
            })
            .collect())
    }
}
