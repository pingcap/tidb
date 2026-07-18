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

//! Cascades task-stack contract from
//! `pkg/planner/cascades/base/task_stack_base.go`.
//!
//! The source `Task` interface combines execution and description while the
//! existing Rust scheduler leaf intentionally owns only execution. This
//! module keeps the stack contract's richer task surface local, so a future
//! cascades adapter can bridge both owners without weakening either one.

/// Opaque task accepted by the source stack interface.
pub trait StackTask {
    /// Executes this task and returns its source-shaped error, if any.
    fn execute(&mut self) -> Result<(), String>;
    /// Returns the task's source diagnostic description.
    fn desc(&self) -> String;
}

/// Abstract task-container contract used by cascades schedulers.
pub trait Stack {
    /// Pushes a task onto the stack.
    fn push(&mut self, task: Box<dyn StackTask>);
    /// Pops the most recently pushed task, or `None` when empty.
    fn pop(&mut self) -> Option<Box<dyn StackTask>>;
    /// Returns whether no task is pending.
    fn is_empty(&self) -> bool;
    /// Clears all pending tasks.
    fn destroy(&mut self);
}
