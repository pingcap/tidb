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

//! gomock-style mocks of the package's interfaces (Go `mock.MockTaskTable`,
//! `mock.MockTaskExecutor`, `mock.MockExtension`,
//! `mockexecute.MockStepExecutor`) and of `sqlsvrapi`.
//!
//! They answer through gomock's [`Controller`] (`tidb-gomock`). Go's
//! `pkg/dxf/framework/mock` is `tidb-dxf-mock`; these copies exist because a
//! dev-dependency on it would build this crate twice.

use std::error::Error as StdError;
use std::sync::{Arc, Mutex};

use tidb_dxf::{Step, StepResource, Subtask, SubtaskBase, SubtaskState, Task, TaskBase};
use tidb_dxf_execute::{FrameworkInfoSlot, StepExecFrameworkInfo, StepExecutor, SubtaskSummary};
use tidb_dxf_storage::{Result, SessionContext, SessionExecutor, StorageError, TaskExecInfo};
use tidb_gocontext::Context;

use crate::{Error, Extension, TaskExecutor, TaskTable};

pub(crate) use tidb_gomock::{Controller, Times, ONCE};

fn states(states: &[SubtaskState]) -> String {
    states.iter().map(ToString::to_string).collect::<Vec<_>>().join(",")
}

fn err_text(err: Option<&(dyn StdError + 'static)>) -> String {
    err.map_or_else(|| "nil".to_owned(), ToString::to_string)
}

/// Keys of `MockTaskTable`'s calls.
pub(crate) mod key {
    use super::{states, SubtaskState};
    use tidb_dxf::Step;

    pub(crate) fn get_task_by_id(task_id: i64) -> String {
        format!("GetTaskByID|{task_id}")
    }
    pub(crate) fn get_first_subtask_in_states(exec_id: &str, task_id: i64, step: Step, subtask_states: &[SubtaskState]) -> String {
        format!("GetFirstSubtaskInStates|{exec_id}|{task_id}|{}|{}", step.0, states(subtask_states))
    }
    pub(crate) fn get_subtasks_by_exec_id_and_step_and_states(
        exec_id: &str,
        task_id: i64,
        step: Step,
        subtask_states: &[SubtaskState],
    ) -> String {
        format!("GetSubtasksByExecIDAndStepAndStates|{exec_id}|{task_id}|{}|{}", step.0, states(subtask_states))
    }
    pub(crate) fn start_subtask(subtask_id: i64, exec_id: &str) -> String {
        format!("StartSubtask|{subtask_id}|{exec_id}")
    }
    pub(crate) fn finish_subtask(exec_id: &str, subtask_id: i64) -> String {
        format!("FinishSubtask|{exec_id}|{subtask_id}|*")
    }
    pub(crate) fn update_subtask_state_and_error(exec_id: &str, subtask_id: i64, state: &SubtaskState, err: &str) -> String {
        format!("UpdateSubtaskStateAndError|{exec_id}|{subtask_id}|{state}|{err}")
    }
    pub(crate) fn fail_subtask(exec_id: &str, task_id: i64, err: &str) -> String {
        format!("FailSubtask|{exec_id}|{task_id}|{err}")
    }
}

/// Go `mock.MockTaskTable`.
pub(crate) struct MockTaskTable {
    ctrl: Arc<Controller>,
    /// Go's `WithNewSession(...).DoAndReturn(fn(session))`, any times.
    session: Mutex<Option<Arc<dyn SessionContext>>>,
    /// The error texts `FailSubtask` was given.
    pub(crate) failed_subtask_errs: Mutex<Vec<String>>,
}

impl MockTaskTable {
    pub(crate) fn new(ctrl: &Arc<Controller>) -> Self {
        Self { ctrl: Arc::clone(ctrl), session: Mutex::new(None), failed_subtask_errs: Mutex::default() }
    }

    pub(crate) fn set_session(&self, session: Arc<dyn SessionContext>) {
        *self.session.lock().unwrap() = Some(session);
    }

    fn call<T: 'static>(&self, key: &str) -> T {
        self.ctrl.call(key, &())
    }
}

impl SessionExecutor for MockTaskTable {
    fn with_new_session(&self, f: &mut dyn FnMut(&dyn SessionContext) -> Result<()>) -> Result<()> {
        let session = self.session.lock().unwrap().clone();
        match session {
            Some(session) => f(session.as_ref()),
            None => self.call("WithNewSession"),
        }
    }
    fn with_new_txn(&self, _: &mut dyn FnMut(&dyn SessionContext) -> Result<()>) -> Result<()> {
        self.call("WithNewTxn")
    }
}

impl TaskTable for MockTaskTable {
    fn get_task_exec_info_by_exec_id(&self, exec_id: &str) -> Result<Vec<TaskExecInfo>> {
        self.call(&format!("GetTaskExecInfoByExecID|{exec_id}"))
    }
    fn get_task_by_id(&self, task_id: i64) -> Result<Task> {
        self.call(&key::get_task_by_id(task_id))
    }
    fn get_task_base_by_id(&self, task_id: i64) -> Result<TaskBase> {
        self.call(&format!("GetTaskBaseByID|{task_id}"))
    }
    fn get_subtasks_by_exec_id_and_step_and_states(
        &self,
        exec_id: &str,
        task_id: i64,
        step: Step,
        subtask_states: &[SubtaskState],
    ) -> Result<Vec<Subtask>> {
        self.call(&key::get_subtasks_by_exec_id_and_step_and_states(exec_id, task_id, step, subtask_states))
    }
    fn get_first_subtask_in_states(
        &self,
        instance_id: &str,
        task_id: i64,
        step: Step,
        subtask_states: &[SubtaskState],
    ) -> Result<Option<Subtask>> {
        self.call(&key::get_first_subtask_in_states(instance_id, task_id, step, subtask_states))
    }
    fn init_meta(&self, exec_id: &str, role: &str) -> Result<()> {
        self.call(&format!("InitMeta|{exec_id}|{role}"))
    }
    fn recover_meta(&self, exec_id: &str, role: &str) -> Result<()> {
        self.call(&format!("RecoverMeta|{exec_id}|{role}"))
    }
    fn start_subtask(&self, subtask_id: i64, exec_id: &str) -> Result<()> {
        self.call(&key::start_subtask(subtask_id, exec_id))
    }
    fn update_subtask_state_and_error(
        &self,
        exec_id: &str,
        subtask_id: i64,
        state: &SubtaskState,
        err: Option<&(dyn StdError + 'static)>,
    ) -> Result<()> {
        self.call(&key::update_subtask_state_and_error(exec_id, subtask_id, state, &err_text(err)))
    }
    fn fail_subtask(&self, exec_id: &str, task_id: i64, err: Option<&(dyn StdError + 'static)>) -> Result<()> {
        self.failed_subtask_errs.lock().unwrap().push(err_text(err));
        self.call(&key::fail_subtask(exec_id, task_id, &err_text(err)))
    }
    fn cancel_subtask(&self, exec_id: &str, task_id: i64) -> Result<()> {
        self.call(&format!("CancelSubtask|{exec_id}|{task_id}"))
    }
    fn finish_subtask(&self, exec_id: &str, subtask_id: i64, meta: &[u8]) -> Result<()> {
        self.call(&format!("FinishSubtask|{exec_id}|{subtask_id}|{}", String::from_utf8_lossy(meta)))
    }
    fn pause_subtasks(&self, exec_id: &str, task_id: i64) -> Result<()> {
        self.call(&format!("PauseSubtasks|{exec_id}|{task_id}"))
    }
    fn running_subtasks_back2_pending(&self, subtasks: &[SubtaskBase]) -> Result<()> {
        let subtasks: Vec<String> = subtasks.iter().map(|subtask| format!("{}@{}", subtask.id, subtask.exec_id)).collect();
        self.call(&format!("RunningSubtasksBack2Pending|{}", subtasks.join(",")))
    }
    fn update_subtask_checkpoint(&self, subtask_id: i64, _: &serde_json::Value) -> Result<()> {
        self.call(&format!("UpdateSubtaskCheckpoint|{subtask_id}"))
    }
    fn get_subtask_checkpoint(&self, subtask_id: i64) -> Result<String> {
        self.call(&format!("GetSubtaskCheckpoint|{subtask_id}"))
    }
}

/// Go `mock.MockTaskExecutor`; its calls are keyed `<name>.Method`.
pub(crate) struct MockTaskExecutor {
    ctrl: Arc<Controller>,
    name: String,
}

impl MockTaskExecutor {
    pub(crate) fn new(ctrl: &Arc<Controller>, name: &str) -> Arc<Self> {
        Arc::new(Self { ctrl: Arc::clone(ctrl), name: name.to_owned() })
    }

    /// The key of the executor's `method` call.
    pub(crate) fn key(&self, method: &str) -> String {
        format!("{}.{method}", self.name)
    }
}

impl TaskExecutor for MockTaskExecutor {
    fn init(&self, _: &Context) -> std::result::Result<(), Error> {
        self.ctrl.call(&self.key("Init"), &())
    }
    fn run(&self) {
        self.ctrl.call::<()>(&self.key("Run"), &());
    }
    fn get_task_base(&self) -> TaskBase {
        self.ctrl.call(&self.key("GetTaskBase"), &())
    }
    fn cancel_running_subtask(&self) {
        self.ctrl.call::<()>(&self.key("CancelRunningSubtask"), &());
    }
    fn cancel(&self) {
        self.ctrl.call::<()>(&self.key("Cancel"), &());
    }
    fn close(&self) {
        self.ctrl.call::<()>(&self.key("Close"), &());
    }
    fn is_retryable_error(&self, err: &(dyn StdError + 'static)) -> bool {
        self.ctrl.call(&format!("{}|{err}", self.key("IsRetryableError")), &())
    }
}

/// Go `mock.MockExtension`; its calls are keyed `ext.Method`.
pub(crate) struct MockExtension {
    ctrl: Arc<Controller>,
}

impl MockExtension {
    pub(crate) fn new(ctrl: &Arc<Controller>) -> Arc<Self> {
        Arc::new(Self { ctrl: Arc::clone(ctrl) })
    }
}

impl Extension for MockExtension {
    fn is_idempotent(&self, subtask: &Subtask) -> bool {
        self.ctrl.call(&format!("ext.IsIdempotent|{}", subtask.base.id), &())
    }
    fn get_step_executor(&self, _: &Task) -> std::result::Result<Arc<dyn StepExecutor>, Error> {
        self.ctrl.call("ext.GetStepExecutor", &())
    }
    fn is_retryable_error(&self, err: &(dyn StdError + 'static)) -> bool {
        self.ctrl.call(&format!("ext.IsRetryableError|{err}"), &())
    }
}

/// Go `stepExecWrap`: `mockexecute.MockStepExecutor` with the framework's
/// info, whose resource the framework sets; its calls are keyed
/// `step.Method`, and `RunSubtask`'s and `Cleanup`'s answers get the call's
/// context.
pub(crate) struct MockStepExecutor {
    ctrl: Arc<Controller>,
    framework_info: FrameworkInfoSlot,
}

impl MockStepExecutor {
    pub(crate) fn new(ctrl: &Arc<Controller>) -> Arc<Self> {
        Arc::new(Self { ctrl: Arc::clone(ctrl), framework_info: FrameworkInfoSlot::default() })
    }
}

impl StepExecFrameworkInfo for MockStepExecutor {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.framework_info
    }
    fn get_step(&self) -> Step {
        self.ctrl.call("step.GetStep", &())
    }
}

impl StepExecutor for MockStepExecutor {
    fn init(&self, _: &Context) -> tidb_dxf_execute::Result<()> {
        self.ctrl.call::<std::result::Result<(), String>>("step.Init", &()).map_err(Into::into)
    }
    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> tidb_dxf_execute::Result<()> {
        self.ctrl
            .call::<std::result::Result<(), Error>>(&format!("step.RunSubtask|{}", subtask.base.id), ctx)
            .map_err(|err| Box::new(Wrapped(err)) as tidb_dxf_execute::Error)
    }
    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        None
    }
    fn reset_summary(&self) {}
    fn cleanup(&self, ctx: &Context) -> tidb_dxf_execute::Result<()> {
        self.ctrl.call::<std::result::Result<(), String>>("step.Cleanup", ctx).map_err(Into::into)
    }
    fn task_meta_modified(&self, _: &Context, new_meta: &[u8]) -> tidb_dxf_execute::Result<()> {
        self.ctrl
            .call::<std::result::Result<(), String>>(&format!("step.TaskMetaModified|{}", String::from_utf8_lossy(new_meta)), &())
            .map_err(Into::into)
    }
    fn resource_modified(&self, _: &Context, _: &StepResource) -> tidb_dxf_execute::Result<()> {
        self.ctrl.call::<std::result::Result<(), String>>("step.ResourceModified", &()).map_err(Into::into)
    }
}

/// An `Error` returned through the step executor's boxed error, keeping
/// its identity as the error's source.
#[derive(Debug)]
struct Wrapped(Error);

impl std::fmt::Display for Wrapped {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl StdError for Wrapped {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Go `utilmock.NewContext()` with a store of the keyspace, and Go's
/// `sessionWithSQLServer` when it has a server.
pub(crate) struct MockSession {
    pub(crate) keyspace: String,
    pub(crate) server: Option<Arc<dyn tidb_domain_sqlsvrapi::Server<dyn SessionContext>>>,
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
    fn sql_executor(&self) -> Arc<dyn tidb_sqlexec::SqlExecutor> {
        unreachable!("the tests run no SQL")
    }
    fn restricted_sql_executor(&self) -> Arc<dyn tidb_sqlexec::RestrictedSqlExecutor> {
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
    fn sql_server(&self) -> Arc<dyn tidb_domain_sqlsvrapi::Server<dyn SessionContext>> {
        Arc::clone(self.server.as_ref().expect("the session has a server"))
    }
}

/// Go `newSessionPoolForStore`: one session of the store's keyspace.
pub(crate) fn new_session_pool_for_store(keyspace: &str) -> Arc<dyn tidb_syssession::Pool<dyn SessionContext>> {
    let keyspace = keyspace.to_owned();
    Arc::new(tidb_syssession::AdvancedSessionPool::<dyn SessionContext>::new(1, move || {
        Ok(Arc::new(MockSession { keyspace: keyspace.clone(), server: None }) as Arc<dyn SessionContext>)
    }))
}

/// Go `sqlsvrapimock.MockRuntime` over a store of the keyspace.
pub(crate) struct MockRuntime {
    pub(crate) keyspace: String,
    pub(crate) se_pool: Option<Arc<dyn tidb_syssession::Pool<dyn SessionContext>>>,
}

impl tidb_domain_sqlsvrapi::Runtime<dyn SessionContext> for MockRuntime {
    fn store_keyspace(&self) -> String {
        self.keyspace.clone()
    }
    fn sys_session_pool(&self) -> Arc<dyn tidb_syssession::Pool<dyn SessionContext>> {
        Arc::clone(self.se_pool.as_ref().expect("the test sets a session pool"))
    }
}

/// Go `sqlsvrapimock.MockKSRuntimeHandle` over a store of the keyspace.
pub(crate) struct MockRuntimeHandle {
    pub(crate) keyspace: String,
    pub(crate) released: std::sync::atomic::AtomicUsize,
}

impl tidb_domain_sqlsvrapi::Runtime<dyn SessionContext> for MockRuntimeHandle {
    fn store_keyspace(&self) -> String {
        self.keyspace.clone()
    }
    fn sys_session_pool(&self) -> Arc<dyn tidb_syssession::Pool<dyn SessionContext>> {
        unreachable!("Go's handle answers a nil pool")
    }
}

impl tidb_domain_sqlsvrapi::KsRuntimeHandle<dyn SessionContext> for MockRuntimeHandle {
    fn release(&self) {
        self.released.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn as_runtime(self: Arc<Self>) -> Arc<dyn tidb_domain_sqlsvrapi::Runtime<dyn SessionContext>> {
        self
    }
}

/// Go `sqlsvrapimock.MockServer`.
#[derive(Default)]
pub(crate) struct MockServer {
    pub(crate) runtime: Option<Arc<dyn tidb_domain_sqlsvrapi::Runtime<dyn SessionContext>>>,
    pub(crate) acquire: Mutex<std::collections::VecDeque<std::result::Result<Arc<MockRuntimeHandle>, String>>>,
    pub(crate) acquired: Mutex<Vec<(String, String)>>,
}

impl tidb_domain_sqlsvrapi::Server<dyn SessionContext> for MockServer {
    fn get_runtime(&self) -> Arc<dyn tidb_domain_sqlsvrapi::Runtime<dyn SessionContext>> {
        Arc::clone(self.runtime.as_ref().expect("the test expects GetRuntime"))
    }
    fn acquire_ks_runtime(
        &self,
        target_ks: &str,
        holder_id: &str,
    ) -> std::result::Result<Arc<dyn tidb_domain_sqlsvrapi::KsRuntimeHandle<dyn SessionContext>>, String> {
        self.acquired.lock().unwrap().push((target_ks.to_owned(), holder_id.to_owned()));
        self.acquire
            .lock()
            .unwrap()
            .pop_front()
            .expect("the test expects AcquireKSRuntime")
            .map(|handle| handle as Arc<dyn tidb_domain_sqlsvrapi::KsRuntimeHandle<dyn SessionContext>>)
    }
}

/// Go `errors.New(text)` as a storage error.
pub(crate) fn storage_err(text: &str) -> StorageError {
    StorageError::Other(text.to_owned())
}
