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
    /// The calls that carry an error or a task, as "Method(args)", for
    /// gomock's argument matchers.
    calls: Mutex<Vec<String>>,
    /// The subtasks `UpdateSubtasksExecIDs` was given: Go's tests read the
    /// balanced exec IDs back through the shared subtask pointers.
    updated_exec_ids: Mutex<Vec<SubtaskBase>>,
}

/// A queued answer computed from the call's task (gomock `DoAndReturn`).
pub(crate) type TaskAnswer<T> = Box<dyn FnOnce(&Task) -> T + Send>;

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

    fn record(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    /// Applies the exec IDs `UpdateSubtasksExecIDs` was given to `subtasks`,
    /// as Go's in-place update of the shared subtasks shows them.
    pub(crate) fn apply_updated_exec_ids(&self, subtasks: &mut [SubtaskBase]) {
        for updated in self.updated_exec_ids.lock().unwrap().iter().cloned() {
            if let Some(subtask) = subtasks.iter_mut().find(|subtask| subtask.id == updated.id) {
                subtask.exec_id = updated.exec_id;
            }
        }
    }

    /// The recorded calls, drained.
    pub(crate) fn take_calls(&self) -> Vec<String> {
        std::mem::take(&mut *self.calls.lock().unwrap())
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

impl MockTaskManager {
    /// An answer queued as a value, or as a closure of the call's task.
    fn answer_with_task<T: 'static>(&self, method: &'static str, task: &Task) -> T {
        let answer = self
            .expected
            .lock()
            .unwrap()
            .get_mut(method)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| panic!("unexpected call of {method}"));
        match answer.downcast::<TaskAnswer<T>>() {
            Ok(answer) => answer(task),
            Err(answer) => *answer.downcast::<T>().unwrap_or_else(|_| panic!("{method} answered with the wrong type")),
        }
    }
}

fn err_text(err: Option<&(dyn Error + 'static)>) -> String {
    err.map_or_else(|| "<nil>".to_owned(), ToString::to_string)
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
    fn fail_task(&self, task_id: i64, state: &TaskState, err: Option<&(dyn Error + 'static)>) -> Result<()> {
        self.record(format!("FailTask({task_id}, {state}, {})", err_text(err)));
        self.answer("FailTask")
    }
    fn revert_task(&self, task_id: i64, state: &TaskState, err: Option<&(dyn Error + 'static)>) -> Result<()> {
        self.record(format!("RevertTask({task_id}, {state}, {})", err_text(err)));
        self.answer("RevertTask")
    }
    fn awaiting_resolve_task(&self, task_id: i64, state: &TaskState, err: Option<&(dyn Error + 'static)>) -> Result<()> {
        self.record(format!("AwaitingResolveTask({task_id}, {state}, {})", err_text(err)));
        self.answer("AwaitingResolveTask")
    }
    fn reverted_task(&self, _: i64) -> Result<()> {
        self.answer("RevertedTask")
    }
    fn pause_task(&self, _: &str) -> Result<bool> {
        self.answer("PauseTask")
    }
    fn pause_task_on_error(&self, task_id: i64, state: &TaskState, step: Step, err: Option<&(dyn Error + 'static)>) -> Result<()> {
        self.record(format!("PauseTaskOnError({task_id}, {state}, {}, {})", step.0, err_text(err)));
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
    fn switch_task_step(&self, _: &Task, state: &TaskState, step: Step, subtasks: &[Subtask]) -> Result<()> {
        self.record(format!("SwitchTaskStep({state}, {}, {})", step.0, subtasks.len()));
        self.answer("SwitchTaskStep")
    }
    fn switch_task_step_in_batch(&self, _: &Task, _: &TaskState, _: Step, _: &[Subtask]) -> Result<()> {
        self.answer("SwitchTaskStepInBatch")
    }
    fn switch_task_step_after_prepare(&self, task: &Task) -> Result<bool> {
        self.answer_with_task("SwitchTaskStepAfterPrepare", task)
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
    fn update_subtasks_exec_ids(&self, subtasks: &[SubtaskBase]) -> Result<()> {
        self.updated_exec_ids.lock().unwrap().extend_from_slice(subtasks);
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

/// Go `schmock.MockExtension`.
#[derive(Default)]
pub(crate) struct MockExtension {
    expected: Mutex<HashMap<&'static str, VecDeque<Box<dyn Any + Send>>>>,
    /// gomock's `AnyTimes` answers of `GetNextStep`.
    next_step: Mutex<Option<Box<dyn Fn(&TaskBase) -> Step + Send>>>,
    /// gomock's `AnyTimes` answer of `GetEligibleInstances`.
    eligible_instances: Mutex<Option<Vec<String>>>,
    /// The errors `IsRetryableErr` was asked about.
    retryable_asked: Mutex<Vec<String>>,
}

/// A queued answer computed from the call's mutable task (gomock
/// `DoAndReturn` of `OnPrepare`).
pub(crate) type TaskMutAnswer<T> = Box<dyn FnOnce(&mut Task) -> T + Send>;

impl MockExtension {
    pub(crate) fn expect<T: Send + 'static>(&self, method: &'static str, answer: T) {
        self.expected.lock().unwrap().entry(method).or_default().push_back(Box::new(answer));
    }

    fn take(&self, method: &'static str) -> Box<dyn Any + Send> {
        self.expected
            .lock()
            .unwrap()
            .get_mut(method)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| panic!("unexpected call of {method}"))
    }

    fn answer<T: 'static>(&self, method: &'static str) -> T {
        *self.take(method).downcast::<T>().unwrap_or_else(|_| panic!("{method} answered with the wrong type"))
    }

    fn answer_with_task<T: 'static>(&self, method: &'static str, task: &mut Task) -> T {
        match self.take(method).downcast::<TaskMutAnswer<T>>() {
            Ok(answer) => answer(task),
            Err(answer) => *answer.downcast::<T>().unwrap_or_else(|_| panic!("{method} answered with the wrong type")),
        }
    }

    pub(crate) fn set_next_step(&self, f: impl Fn(&TaskBase) -> Step + Send + 'static) {
        *self.next_step.lock().unwrap() = Some(Box::new(f));
    }

    pub(crate) fn set_eligible_instances(&self, instances: Vec<String>) {
        *self.eligible_instances.lock().unwrap() = Some(instances);
    }

    pub(crate) fn take_retryable_asked(&self) -> Vec<String> {
        std::mem::take(&mut *self.retryable_asked.lock().unwrap())
    }

    /// Go `ctrl.Satisfied()`.
    pub(crate) fn assert_satisfied(&self) {
        for (method, answers) in self.expected.lock().unwrap().iter() {
            assert!(answers.is_empty(), "{} expected call(s) of {method} missing", answers.len());
        }
    }
}

impl crate::Extension for MockExtension {
    fn on_tick(&self, _: &tidb_gocontext::Context, _: &Task) {
        self.answer::<()>("OnTick");
    }
    fn on_next_subtasks_batch(
        &self,
        _: &tidb_gocontext::Context,
        _: &dyn tidb_dxf_storage::TaskHandle,
        _: &mut Task,
        _: &[String],
        _: Step,
    ) -> std::result::Result<Vec<Vec<u8>>, crate::Error> {
        self.answer("OnNextSubtasksBatch")
    }
    fn on_done(&self, _: &tidb_gocontext::Context, _: &dyn tidb_dxf_storage::TaskHandle, _: &mut Task) -> std::result::Result<(), crate::Error> {
        self.answer("OnDone")
    }
    fn get_eligible_instances(&self, _: &tidb_gocontext::Context, _: &Task) -> std::result::Result<Vec<String>, crate::Error> {
        if let Some(instances) = self.eligible_instances.lock().unwrap().clone() {
            return Ok(instances);
        }
        self.answer("GetEligibleInstances")
    }
    fn is_retryable_err(&self, err: &(dyn Error + 'static)) -> bool {
        self.retryable_asked.lock().unwrap().push(err.to_string());
        self.answer("IsRetryableErr")
    }
    fn get_next_step(&self, task: &TaskBase) -> Step {
        if let Some(f) = self.next_step.lock().unwrap().as_ref() {
            return f(task);
        }
        self.answer("GetNextStep")
    }
    fn on_prepare(&self, _: &tidb_gocontext::Context, _: &dyn tidb_dxf_storage::TaskHandle, task: &mut Task) -> std::result::Result<(), crate::Error> {
        self.answer_with_task("OnPrepare", task)
    }
    fn modify_meta(&self, _: &[u8], _: &[tidb_dxf::Modification]) -> std::result::Result<Vec<u8>, crate::Error> {
        self.answer("ModifyMeta")
    }
}

/// Go `utilmock.NewContext()` with a store of the keyspace.
pub(crate) struct MockSession {
    keyspace: String,
}

impl tidb_syssession::SessionContext for MockSession {
    fn close(&self) {}
    fn rollback_txn(&self, _: &dyn tidb_sqlexec::ExecutionContext) {}
    fn has_prepared_txn_future(&self) -> bool {
        false
    }
    fn txn_valid(&self) -> std::result::Result<bool, tidb_sqlexec::SqlExecError> {
        Ok(false)
    }
    fn sql_executor(&self) -> std::sync::Arc<dyn tidb_sqlexec::SqlExecutor> {
        unreachable!("the tests run no SQL")
    }
    fn restricted_sql_executor(&self) -> std::sync::Arc<dyn tidb_sqlexec::RestrictedSqlExecutor> {
        unreachable!("the tests run no SQL")
    }
    fn register_internal_session(&self) {}
    fn unregister_internal_session(&self) {}
    fn contains_internal_session(&self) -> bool {
        false
    }
    fn store_internal_session(&self) -> bool {
        true
    }
}

impl SessionContext for MockSession {
    fn commit_txn(&self, _: &dyn tidb_sqlexec::ExecutionContext) -> Result<()> {
        Ok(())
    }
    fn txn_entry_size_limit(&self) -> u64 {
        0
    }
    fn set_txn_entry_size_limit(&self, _: u64) {}
    fn global_txn_entry_size_limit(&self) -> u64 {
        0
    }
    fn affected_rows(&self) -> u64 {
        0
    }
    fn mem_quota_query(&self) -> i64 {
        0
    }
    fn set_system_var(&self, _: &str, _: &str) -> Result<()> {
        Ok(())
    }
    fn store_keyspace(&self) -> String {
        self.keyspace.clone()
    }
    fn sql_server(&self) -> std::sync::Arc<dyn tidb_domain_sqlsvrapi::Server<dyn SessionContext>> {
        unreachable!("the tests reach no server")
    }
}

/// Go `newSessionPoolForStore`: one session of the store's keyspace.
pub(crate) fn new_session_pool_for_store(keyspace: &str) -> std::sync::Arc<dyn tidb_syssession::Pool<dyn SessionContext>> {
    let keyspace = keyspace.to_owned();
    std::sync::Arc::new(tidb_syssession::AdvancedSessionPool::<dyn SessionContext>::new(1, move || {
        Ok(std::sync::Arc::new(MockSession { keyspace: keyspace.clone() }) as std::sync::Arc<dyn SessionContext>)
    }))
}

/// Go `sqlsvrapimock.MockRuntime` over a store of the keyspace.
pub(crate) struct MockRuntime {
    pub(crate) keyspace: String,
    pub(crate) se_pool: Option<std::sync::Arc<dyn tidb_syssession::Pool<dyn SessionContext>>>,
}

impl tidb_domain_sqlsvrapi::Runtime<dyn SessionContext> for MockRuntime {
    fn store_keyspace(&self) -> String {
        self.keyspace.clone()
    }
    fn sys_session_pool(&self) -> std::sync::Arc<dyn tidb_syssession::Pool<dyn SessionContext>> {
        std::sync::Arc::clone(self.se_pool.as_ref().expect("the test sets a session pool"))
    }
}

/// Go `mock.MockScheduler`: the task it returns, and the extension's
/// answers.
pub(crate) struct MockScheduler {
    pub(crate) task: Mutex<Task>,
    pub(crate) ext: MockExtension,
}

impl MockScheduler {
    pub(crate) fn new(task: Task) -> Self {
        Self { task: Mutex::new(task), ext: MockExtension::default() }
    }
}

impl crate::Extension for MockScheduler {
    fn on_tick(&self, ctx: &tidb_gocontext::Context, task: &Task) {
        self.ext.on_tick(ctx, task);
    }
    fn on_next_subtasks_batch(
        &self,
        ctx: &tidb_gocontext::Context,
        h: &dyn tidb_dxf_storage::TaskHandle,
        task: &mut Task,
        exec_ids: &[String],
        next_step: Step,
    ) -> std::result::Result<Vec<Vec<u8>>, crate::Error> {
        self.ext.on_next_subtasks_batch(ctx, h, task, exec_ids, next_step)
    }
    fn on_done(&self, ctx: &tidb_gocontext::Context, h: &dyn tidb_dxf_storage::TaskHandle, task: &mut Task) -> std::result::Result<(), crate::Error> {
        self.ext.on_done(ctx, h, task)
    }
    fn get_eligible_instances(&self, ctx: &tidb_gocontext::Context, task: &Task) -> std::result::Result<Vec<String>, crate::Error> {
        self.ext.get_eligible_instances(ctx, task)
    }
    fn is_retryable_err(&self, err: &(dyn Error + 'static)) -> bool {
        self.ext.is_retryable_err(err)
    }
    fn get_next_step(&self, task: &TaskBase) -> Step {
        self.ext.get_next_step(task)
    }
    fn on_prepare(&self, ctx: &tidb_gocontext::Context, h: &dyn tidb_dxf_storage::TaskHandle, task: &mut Task) -> std::result::Result<(), crate::Error> {
        self.ext.on_prepare(ctx, h, task)
    }
    fn modify_meta(&self, old_meta: &[u8], modifies: &[tidb_dxf::Modification]) -> std::result::Result<Vec<u8>, crate::Error> {
        self.ext.modify_meta(old_meta, modifies)
    }
}

impl crate::Scheduler for MockScheduler {
    fn init(&self) -> std::result::Result<(), crate::Error> {
        self.ext.answer("Init")
    }
    fn schedule_task(&self) {
        self.ext.answer::<()>("ScheduleTask");
    }
    fn close(&self) {
        self.ext.answer::<()>("Close");
    }
    fn get_task(&self) -> std::sync::Arc<Task> {
        std::sync::Arc::new(self.task.lock().unwrap().clone())
    }
}
