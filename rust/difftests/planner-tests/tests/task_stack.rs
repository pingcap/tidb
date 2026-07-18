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
//! `pkg/planner/cascades/task/task.go`.
//!
//! The direct Go anchors are `TestTaskStack` at line 39 and
//! `TestTaskFunctionality` at line 55 in
//! `pkg/planner/cascades/task/task_test.go`.

use tidb_planner::task_stack::{StackTask, TaskStack};

struct TestTask {
    id: u8,
}

impl StackTask for TestTask {
    fn desc(&self) -> String {
        self.id.to_string()
    }
}

#[test]
fn source_default_stack_shape_and_empty_pop() {
    let mut stack = TaskStack::new();
    assert_eq!(stack.capacity(), 4);
    assert!(stack.is_empty());
    assert_eq!(stack.len(), 0);
    assert!(stack.pop().is_none());
}

#[test]
fn source_stack_is_lifo_and_destroy_reuses_capacity() {
    let mut stack = TaskStack::new();
    stack.push(TestTask { id: 1 });
    stack.push(TestTask { id: 2 });
    assert_eq!(stack.describe(), "1\n2\n");
    assert_eq!(stack.pop().expect("second task").desc(), "2");
    assert_eq!(stack.pop().expect("first task").desc(), "1");
    assert!(stack.pop().is_none());

    stack.push(TestTask { id: 3 });
    stack.push(TestTask { id: 4 });
    stack.push(TestTask { id: 5 });
    stack.push(TestTask { id: 6 });
    let capacity = stack.capacity();
    stack.destroy();
    assert!(stack.is_empty());
    assert_eq!(stack.capacity(), capacity);
}
