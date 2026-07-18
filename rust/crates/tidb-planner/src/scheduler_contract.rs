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

//! Cascades scheduler contract from
//! `pkg/planner/cascades/base/task_scheduler_base.go`.
//!
//! The source interface accepts an opaque task, executes pending tasks, and
//! releases its resources. Rust keeps that dynamic task boundary as
//! `Box<dyn Task>` and leaves concrete scheduling policy to the existing
//! `SimpleTaskScheduler` owner.

use crate::task_scheduler::{Task, TaskError};

/// Interface implemented by serial or concurrent cascades schedulers.
pub trait Scheduler {
    /// Executes pending tasks until completion or the first task error.
    fn execute_tasks(&mut self) -> Result<(), TaskError>;
    /// Releases pending scheduler resources.
    fn destroy(&mut self);
    /// Enqueues an opaque task for execution.
    fn push_task(&mut self, task: Box<dyn Task>);
}
