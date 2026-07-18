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

//! Serial cascades task scheduling from
//! `pkg/planner/cascades/task/task_scheduler.go`.
//!
//! The source scheduler is a thin LIFO driver: tasks are pushed onto a stack,
//! popped in reverse insertion order, and the first task error stops execution.
//! This leaf preserves that control flow over an opaque Rust task trait while
//! leaving the source stack pool, task descriptions, and cascades context
//! external.

/// Error returned by a scheduled task.
pub type TaskError = String;

/// A unit of serial optimizer work.
pub trait Task {
    /// Executes the task and returns its source-shaped error string, if any.
    fn execute(&mut self) -> Result<(), TaskError>;
}

/// Serial LIFO scheduler for optimizer tasks.
pub struct SimpleTaskScheduler {
    stack: Vec<Box<dyn Task>>,
}

impl Default for SimpleTaskScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleTaskScheduler {
    /// Creates an empty scheduler.
    #[must_use]
    pub const fn new() -> Self {
        Self { stack: Vec::new() }
    }

    /// Pushes a task onto the scheduler's LIFO stack.
    pub fn push_task<T>(&mut self, task: T)
    where
        T: Task + 'static,
    {
        self.stack.push(Box::new(task));
    }

    /// Executes tasks until the stack is empty or one task returns an error.
    pub fn execute_tasks(&mut self) -> Result<(), TaskError> {
        while let Some(mut task) = self.stack.pop() {
            task.execute()?;
        }
        Ok(())
    }

    /// Releases all pending tasks.
    pub fn destroy(&mut self) {
        self.stack.clear();
    }

    /// Returns the number of tasks still pending after execution or failure.
    #[must_use]
    pub const fn pending_len(&self) -> usize {
        self.stack.len()
    }
}

impl crate::scheduler_contract::Scheduler for SimpleTaskScheduler {
    fn execute_tasks(&mut self) -> Result<(), TaskError> {
        Self::execute_tasks(self)
    }

    fn destroy(&mut self) {
        Self::destroy(self);
    }

    fn push_task(&mut self, task: Box<dyn Task>) {
        self.stack.push(task);
    }
}
