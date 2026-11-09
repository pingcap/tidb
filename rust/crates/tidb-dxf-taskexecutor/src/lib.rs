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

//! Go `pkg/dxf/framework/taskexecutor`: on every node, the manager starts a
//! task executor for each task that has subtasks on the node, within the
//! node's slots; the executor runs the subtasks step by step.

mod interface;
mod manager;
#[cfg(test)]
mod manager_tests;
#[cfg(test)]
mod mock;
mod register;
mod slot;
mod task_executor;
#[cfg(test)]
mod task_executor_tests;

pub use interface::{BaseStepExecutor, Error, Extension, TaskExecutor, TaskExecutorError, TaskTable};
pub use manager::{Manager, MAX_SUBTASK_CHECK_INTERVAL, SUBTASK_CHECK_INTERVAL, TASK_CHECK_INTERVAL};
pub use register::{clear_task_executors, get_task_executor_factory, register_task_type, FactoryFn};
pub use slot::SlotManager;
pub use task_executor::{BaseTaskExecutor, Param, DETECT_PARAM_MODIFY_INTERVAL};
#[cfg(any(test, feature = "failpoints"))]
pub use task_executor::{
    CallHook, AFTER_RUN_SUBTASK, BEFORE_GET_TASK_BY_ID_IN_RUN, BEFORE_SET_FRAMEWORK_INFO, MOCK_TIDB_SHUTDOWN,
};
