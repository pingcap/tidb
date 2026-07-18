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

//! Source-shaped LIFO task stack from
//! `pkg/planner/cascades/task/task.go`.
//!
//! The Go stack owns a reusable slice of opaque tasks.  This leaf preserves
//! the observable stack contract (the source default capacity, LIFO pop,
//! empty-pop, description order, and destroy retaining allocation capacity)
//! without coupling the planner crate to the unfinished cascades context.

/// The opaque task surface needed by a stack description.
pub trait StackTask {
    /// Returns the source task description.
    fn desc(&self) -> String;
}

/// Reusable LIFO stack for cascades tasks.
pub struct TaskStack {
    tasks: Vec<Box<dyn StackTask>>,
}

impl Default for TaskStack {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskStack {
    /// The source stack starts with room for four task interfaces.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(4)
    }

    /// Creates a stack with caller-selected initial capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            tasks: Vec::with_capacity(capacity),
        }
    }

    /// Appends a task to the top of the stack.
    pub fn push<T>(&mut self, task: T)
    where
        T: StackTask + 'static,
    {
        self.tasks.push(Box::new(task));
    }

    /// Removes and returns the most recently pushed task, if any.
    pub fn pop(&mut self) -> Option<Box<dyn StackTask>> {
        self.tasks.pop()
    }

    /// Returns whether no task is pending.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Returns the number of pending tasks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns the current allocation capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.tasks.capacity()
    }

    /// Writes each pending task description in stack order and clears tasks.
    pub fn describe(&self) -> String {
        let mut output = String::new();
        for task in &self.tasks {
            output.push_str(&task.desc());
            output.push('\n');
        }
        output
    }

    /// Clears pending tasks while retaining the backing allocation.
    pub fn destroy(&mut self) {
        self.tasks.clear();
    }
}
