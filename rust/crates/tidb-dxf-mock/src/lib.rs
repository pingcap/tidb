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

//! Go's generated DXF mocks: `pkg/dxf/framework/mock` (the task executor's
//! `MockExtension`, `MockCleaner`, `MockScheduler`), `mock/execute`
//! (`MockStepExecutor`) and `scheduler/mock` (the scheduler's
//! `MockExtension`), over gomock's [`Controller`](tidb_gomock::Controller).
//! A call's key is `Method|arg|...`; its answer gets the call's arguments as
//! the tuple each method documents. The other generated mocks come with
//! their tests.

use std::error::Error as StdError;
use std::sync::{Arc, Mutex};

use tidb_dxf::{Modification, Step, StepResource, Subtask, Task, TaskBase};
use tidb_dxf_execute::{FrameworkInfoSlot, StepExecFrameworkInfo, StepExecutor, SubtaskSummary};
use tidb_dxf_storage::TaskHandle;
use tidb_gocontext::Context;
use tidb_gomock::{Controller, Receiver};

/// Go `mock.MockExtension`, the task executor's extension.
pub struct MockTaskExecutorExtension {
    ctrl: Receiver,
}

impl MockTaskExecutorExtension {
    /// Go `mock.NewMockExtension`.
    #[must_use]
    pub fn new(ctrl: &Arc<Controller>) -> Arc<Self> {
        Arc::new(Self { ctrl: Receiver::new(ctrl) })
    }
}

impl tidb_dxf_taskexecutor::Extension for MockTaskExecutorExtension {
    /// `IsIdempotent|{subtask id}`, answered with the subtask.
    fn is_idempotent(&self, subtask: &Subtask) -> bool {
        self.ctrl.call(&format!("IsIdempotent|{}", subtask.base.id), &subtask.clone())
    }
    /// `GetStepExecutor`, answered with the task.
    fn get_step_executor(&self, task: &Task) -> Result<Arc<dyn StepExecutor>, tidb_dxf_taskexecutor::Error> {
        self.ctrl.call("GetStepExecutor", &task.clone())
    }
    /// `IsRetryableError|{err}`, answered with the error's text.
    fn is_retryable_error(&self, err: &(dyn StdError + 'static)) -> bool {
        self.ctrl.call(&format!("IsRetryableError|{err}"), &err.to_string())
    }
}

/// Go `mockexecute.MockStepExecutor`. Go's generated mock has no framework
/// info field (`SetFrameworkInfo` skips it); the Rust trait reads one, so
/// `GetResource`/`SetResource` read and write it.
pub struct MockStepExecutor {
    ctrl: Receiver,
    framework_info: FrameworkInfoSlot,
}

impl MockStepExecutor {
    /// Go `mockexecute.NewMockStepExecutor`.
    #[must_use]
    pub fn new(ctrl: &Arc<Controller>) -> Arc<Self> {
        Arc::new(Self { ctrl: Receiver::new(ctrl), framework_info: FrameworkInfoSlot::default() })
    }
}

impl StepExecFrameworkInfo for MockStepExecutor {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        &self.framework_info
    }
    /// `GetStep`.
    fn get_step(&self) -> Step {
        self.ctrl.call("GetStep", &())
    }
}

/// A step executor's error, as Go's `error`.
pub type StepResult = Result<(), Arc<dyn StdError + Send + Sync>>;

/// Keeps an `Arc` error's identity through the step executor's boxed error.
#[derive(Debug)]
struct Wrapped(Arc<dyn StdError + Send + Sync>);

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

fn boxed(result: StepResult) -> tidb_dxf_execute::Result<()> {
    result.map_err(|err| Box::new(Wrapped(err)) as tidb_dxf_execute::Error)
}

impl StepExecutor for MockStepExecutor {
    /// `Init`, answered with the context; returns a [`StepResult`].
    fn init(&self, ctx: &Context) -> tidb_dxf_execute::Result<()> {
        boxed(self.ctrl.call("Init", &ctx.clone()))
    }
    /// `RunSubtask|{subtask id}`, answered with `(ctx, subtask)`; returns a
    /// [`StepResult`].
    fn run_subtask(&self, ctx: &Context, subtask: &mut Subtask) -> tidb_dxf_execute::Result<()> {
        boxed(self.ctrl.call(&format!("RunSubtask|{}", subtask.base.id), &(ctx.clone(), subtask.clone())))
    }
    /// `RealtimeSummary`.
    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        self.ctrl.call("RealtimeSummary", &())
    }
    /// `ResetSummary`.
    fn reset_summary(&self) {
        self.ctrl.call::<()>("ResetSummary", &());
    }
    /// `Cleanup`, answered with the context; returns a [`StepResult`].
    fn cleanup(&self, ctx: &Context) -> tidb_dxf_execute::Result<()> {
        boxed(self.ctrl.call("Cleanup", &ctx.clone()))
    }
    /// `TaskMetaModified`, answered with the new meta; returns a
    /// [`StepResult`].
    fn task_meta_modified(&self, _: &Context, new_meta: &[u8]) -> tidb_dxf_execute::Result<()> {
        boxed(self.ctrl.call("TaskMetaModified", &new_meta.to_vec()))
    }
    /// `ResourceModified`, answered with the new resource's CPU and memory;
    /// returns a [`StepResult`].
    fn resource_modified(&self, _: &Context, new_resource: &StepResource) -> tidb_dxf_execute::Result<()> {
        boxed(self.ctrl.call("ResourceModified", &(new_resource.cpu.capacity(), new_resource.mem.capacity())))
    }
}

/// Go `mock.MockCleaner`.
pub struct MockCleaner {
    ctrl: Receiver,
}

impl MockCleaner {
    /// Go `mock.NewMockCleaner`.
    #[must_use]
    pub fn new(ctrl: &Arc<Controller>) -> Arc<Self> {
        Arc::new(Self { ctrl: Receiver::new(ctrl) })
    }
}

impl tidb_dxf_scheduler::Cleaner for MockCleaner {
    /// `Clean`, answered with the task.
    fn clean(&self, _: &Context, task: &mut Task) -> Result<(), tidb_dxf_scheduler::Error> {
        self.ctrl.call("Clean", &task.clone())
    }
}

/// A call whose answer gets `Mutex<Task>` and may change the task, as Go's
/// `*proto.Task` lets it.
fn mutable_task_call<T: 'static>(ctrl: &Receiver, key: &str, task: &mut Task) -> T {
    let cell = Mutex::new(task.clone());
    let result = ctrl.call(key, &cell);
    *task = cell.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
    result
}

/// Go `scheduler/mock.MockExtension`, the scheduler's extension.
pub struct MockSchedulerExtension {
    ctrl: Receiver,
}

impl MockSchedulerExtension {
    /// Go `mockScheduler.NewMockExtension`.
    #[must_use]
    pub fn new(ctrl: &Arc<Controller>) -> Arc<Self> {
        Arc::new(Self { ctrl: Receiver::new(ctrl) })
    }
}

impl tidb_dxf_scheduler::Extension for MockSchedulerExtension {
    /// `OnTick`, answered with the task.
    fn on_tick(&self, _: &Context, task: &Task) {
        self.ctrl.call::<()>("OnTick", &task.clone());
    }
    /// `OnNextSubtasksBatch`, answered with `(task, exec_ids, next_step)`;
    /// the answer may change the task, as Go's pointer lets it.
    fn on_next_subtasks_batch(
        &self,
        _: &Context,
        _: &dyn TaskHandle,
        task: &mut Task,
        exec_ids: &[String],
        next_step: Step,
    ) -> Result<Vec<Vec<u8>>, tidb_dxf_scheduler::Error> {
        let input = (Mutex::new(task.clone()), exec_ids.to_vec(), next_step);
        let result = self.ctrl.call("OnNextSubtasksBatch", &input);
        *task = input.0.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
        result
    }
    /// `OnDone`, answered with the task, which the answer may change.
    fn on_done(&self, _: &Context, _: &dyn TaskHandle, task: &mut Task) -> Result<(), tidb_dxf_scheduler::Error> {
        mutable_task_call(&self.ctrl, "OnDone", task)
    }
    /// `GetEligibleInstances`, answered with the task.
    fn get_eligible_instances(&self, _: &Context, task: &Task) -> Result<Vec<String>, tidb_dxf_scheduler::Error> {
        self.ctrl.call("GetEligibleInstances", &task.clone())
    }
    /// `IsRetryableErr`, answered with the error's text.
    fn is_retryable_err(&self, err: &(dyn StdError + 'static)) -> bool {
        self.ctrl.call("IsRetryableErr", &err.to_string())
    }
    /// `GetNextStep`, answered with the task base.
    fn get_next_step(&self, task: &TaskBase) -> Step {
        self.ctrl.call("GetNextStep", &task.clone())
    }
    /// `OnPrepare`, answered with the task, which the answer may change.
    fn on_prepare(&self, _: &Context, _: &dyn TaskHandle, task: &mut Task) -> Result<(), tidb_dxf_scheduler::Error> {
        mutable_task_call(&self.ctrl, "OnPrepare", task)
    }
    /// `ModifyMeta`, answered with `(old_meta, modifies)`.
    fn modify_meta(&self, old_meta: &[u8], modifies: &[Modification]) -> Result<Vec<u8>, tidb_dxf_scheduler::Error> {
        self.ctrl.call("ModifyMeta", &(old_meta.to_vec(), modifies.to_vec()))
    }
}

/// Go `mock.MockScheduler`: its `Extension` methods are the scheduler
/// extension mock's, on the same receiver.
pub struct MockScheduler {
    ext: MockSchedulerExtension,
}

impl MockScheduler {
    /// Go `mock.NewMockScheduler`.
    #[must_use]
    pub fn new(ctrl: &Arc<Controller>) -> Arc<Self> {
        Arc::new(Self { ext: MockSchedulerExtension { ctrl: Receiver::new(ctrl) } })
    }
}

impl tidb_dxf_scheduler::Extension for MockScheduler {
    fn on_tick(&self, ctx: &Context, task: &Task) {
        self.ext.on_tick(ctx, task);
    }
    fn on_next_subtasks_batch(
        &self,
        ctx: &Context,
        task_handle: &dyn TaskHandle,
        task: &mut Task,
        exec_ids: &[String],
        next_step: Step,
    ) -> Result<Vec<Vec<u8>>, tidb_dxf_scheduler::Error> {
        self.ext.on_next_subtasks_batch(ctx, task_handle, task, exec_ids, next_step)
    }
    fn on_done(&self, ctx: &Context, task_handle: &dyn TaskHandle, task: &mut Task) -> Result<(), tidb_dxf_scheduler::Error> {
        self.ext.on_done(ctx, task_handle, task)
    }
    fn get_eligible_instances(&self, ctx: &Context, task: &Task) -> Result<Vec<String>, tidb_dxf_scheduler::Error> {
        self.ext.get_eligible_instances(ctx, task)
    }
    fn is_retryable_err(&self, err: &(dyn StdError + 'static)) -> bool {
        self.ext.is_retryable_err(err)
    }
    fn get_next_step(&self, task: &TaskBase) -> Step {
        self.ext.get_next_step(task)
    }
    fn on_prepare(&self, ctx: &Context, task_handle: &dyn TaskHandle, task: &mut Task) -> Result<(), tidb_dxf_scheduler::Error> {
        self.ext.on_prepare(ctx, task_handle, task)
    }
    fn modify_meta(&self, old_meta: &[u8], modifies: &[Modification]) -> Result<Vec<u8>, tidb_dxf_scheduler::Error> {
        self.ext.modify_meta(old_meta, modifies)
    }
}

impl tidb_dxf_scheduler::Scheduler for MockScheduler {
    /// `Init`.
    fn init(&self) -> Result<(), tidb_dxf_scheduler::Error> {
        self.ext.ctrl.call("Init", &())
    }
    /// `ScheduleTask`.
    fn schedule_task(&self) {
        self.ext.ctrl.call::<()>("ScheduleTask", &());
    }
    /// `Close`.
    fn close(&self) {
        self.ext.ctrl.call::<()>("Close", &());
    }
    /// `GetTask`.
    fn get_task(&self) -> Arc<Task> {
        self.ext.ctrl.call("GetTask", &())
    }
}

/// The mock's `EXPECT()`.
impl std::ops::Deref for MockScheduler {
    type Target = Receiver;
    fn deref(&self) -> &Receiver {
        &self.ext.ctrl
    }
}

macro_rules! receiver {
    ($($mock:ident),*) => {$(
        /// The mock's `EXPECT()`.
        impl std::ops::Deref for $mock {
            type Target = Receiver;
            fn deref(&self) -> &Receiver {
                &self.ctrl
            }
        }
    )*};
}

receiver!(MockTaskExecutorExtension, MockStepExecutor, MockCleaner, MockSchedulerExtension);
