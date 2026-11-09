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
use std::sync::Arc;

use tidb_dxf::{StepResource, Subtask, Task, TaskBase};
use tidb_dxf_execute::{FrameworkInfoSlot, StepExecFrameworkInfo, StepExecutor, SubtaskSummary};
use tidb_dxf_taskexecutor::{BaseStepExecutor, BaseTaskExecutor, Error, Extension, Param, TaskExecutor};
use tidb_gocontext::Context;
use tidb_log::{Field, Value};

use crate::proto::SubtaskMeta;

/// Go `taskExecutor`.
pub struct TaskExecutorImpl {
    base: BaseTaskExecutor,
}

/// Go `newTaskExecutor`.
#[must_use]
pub fn new_task_executor(ctx: &Context, task: Task, param: Param) -> Arc<TaskExecutorImpl> {
    Arc::new_cyclic(|this| {
        let e = TaskExecutorImpl { base: BaseTaskExecutor::new(ctx, task, param) };
        e.base.set_extension(this.clone());
        e
    })
}

impl Extension for TaskExecutorImpl {
    fn is_idempotent(&self, _: &Subtask) -> bool {
        true
    }
    fn get_step_executor(&self, _: &Task) -> Result<Arc<dyn StepExecutor>, Error> {
        Ok(Arc::new(StepExecutorImpl::default()))
    }
    fn is_retryable_error(&self, _: &(dyn StdError + 'static)) -> bool {
        true
    }
}

/// Go's embedded `*taskexecutor.BaseTaskExecutor`.
impl TaskExecutor for TaskExecutorImpl {
    fn init(&self, ctx: &Context) -> Result<(), Error> {
        TaskExecutor::init(&self.base, ctx)
    }
    fn run(&self) {
        self.base.run();
    }
    fn get_task_base(&self) -> TaskBase {
        self.base.get_task_base()
    }
    fn cancel_running_subtask(&self) {
        TaskExecutor::cancel_running_subtask(&self.base);
    }
    fn cancel(&self) {
        TaskExecutor::cancel(&self.base);
    }
    fn close(&self) {
        TaskExecutor::close(&self.base);
    }
    fn is_retryable_error(&self, err: &(dyn StdError + 'static)) -> bool {
        Extension::is_retryable_error(self, err)
    }
}

/// Go `stepExecutor`. Different steps could have different step executors.
#[derive(Default)]
struct StepExecutorImpl {
    base: BaseStepExecutor,
}

impl StepExecFrameworkInfo for StepExecutorImpl {
    fn framework_info_slot(&self) -> &FrameworkInfoSlot {
        self.base.framework_info_slot()
    }
}

impl StepExecutor for StepExecutorImpl {
    fn init(&self, ctx: &Context) -> tidb_dxf_execute::Result<()> {
        self.base.init(ctx)
    }
    fn run_subtask(&self, _: &Context, subtask: &mut Subtask) -> tidb_dxf_execute::Result<()> {
        let st_meta: SubtaskMeta = serde_json::from_slice(&subtask.meta)?;
        tidb_logutil::bg_logger().info(
            "RunSubtask",
            &[Field::new("subtaskID", Value::I64(subtask.base.id)), Field::new("message", Value::Str(st_meta.message))],
        );
        Ok(())
    }
    fn realtime_summary(&self) -> Option<Arc<SubtaskSummary>> {
        self.base.realtime_summary()
    }
    fn reset_summary(&self) {
        self.base.reset_summary();
    }
    fn cleanup(&self, ctx: &Context) -> tidb_dxf_execute::Result<()> {
        self.base.cleanup(ctx)
    }
    fn task_meta_modified(&self, ctx: &Context, new_meta: &[u8]) -> tidb_dxf_execute::Result<()> {
        self.base.task_meta_modified(ctx, new_meta)
    }
    fn resource_modified(&self, ctx: &Context, new_resource: &StepResource) -> tidb_dxf_execute::Result<()> {
        self.base.resource_modified(ctx, new_resource)
    }
}
