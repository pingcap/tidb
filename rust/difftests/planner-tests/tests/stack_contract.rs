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

//! Dependency-closed vectors for
//! `pkg/planner/cascades/base/task_stack_base.go`.
//!
//! The concrete Go anchors `TestTaskStack` at line 39 and
//! `TestTaskFunctionality` at line 55 in `pkg/planner/cascades/task/task_test.go`
//! are separately owned by the task-stack leaf. These vectors exercise only
//! the base Stack/Task contract with a local mock implementation.

use tidb_planner::stack_contract::{Stack, StackTask};

struct RecordingTask {
    id: u8,
}

impl StackTask for RecordingTask {
    fn execute(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn desc(&self) -> String {
        self.id.to_string()
    }
}

#[derive(Default)]
struct MockStack {
    tasks: Vec<Box<dyn StackTask>>,
}

impl Stack for MockStack {
    fn push(&mut self, task: Box<dyn StackTask>) {
        self.tasks.push(task);
    }

    fn pop(&mut self) -> Option<Box<dyn StackTask>> {
        self.tasks.pop()
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn destroy(&mut self) {
        self.tasks.clear();
    }
}

#[test]
fn stack_contract_preserves_lifo_pop_and_empty_behavior() {
    let mut stack = MockStack::default();
    assert!(Stack::is_empty(&stack));
    assert!(Stack::pop(&mut stack).is_none());

    Stack::push(&mut stack, Box::new(RecordingTask { id: 1 }));
    Stack::push(&mut stack, Box::new(RecordingTask { id: 2 }));
    assert_eq!(Stack::pop(&mut stack).expect("second task").desc(), "2");
    assert_eq!(Stack::pop(&mut stack).expect("first task").desc(), "1");
    assert!(Stack::pop(&mut stack).is_none());
}

#[test]
fn stack_contract_destroy_clears_pending_tasks() {
    let mut stack = MockStack::default();
    Stack::push(&mut stack, Box::new(RecordingTask { id: 1 }));
    Stack::push(&mut stack, Box::new(RecordingTask { id: 2 }));
    Stack::destroy(&mut stack);
    assert!(Stack::is_empty(&stack));
}
