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

//! Ports of the two `pkg/planner/cascades/task/task_test.go` loop-shaped
//! microbenchmarks whose exercised surface IS ported here
//! (`pkg/planner.part3` items 121–122 on `origin/master`):
//!
//! | Go function (`task_test.go`) | Rust test | Status |
//! | --- | --- | --- |
//! | `BenchmarkTestStack2Pointer` (:164) | [`stack2_pointer_fill_drains_1000_tasks_lifo`] | ported (functional form) |
//! | `BenchmarkTestStackInterface` (:182) | [`stack_interface_fill_drains_1000_tasks_lifo`] | ported (functional form) |
//!
//! Both benchmarks share the same body (`fill()`): create a stack with
//! initial capacity 1000 (`task.go:84 newTaskStackWithCap`, mirrored by
//! [`TaskStack::with_capacity`]), push 1000 numbered tasks (`task.go:74
//! Push` appends), then pop them all back off (`task.go:64 Pop` removes
//! from the tail, returning nil/None once empty at `task.go:79`). The Go
//! recorded cost comments (24000 B/op / 2000 or 8000 B/op, and a stable
//! ns/op across runs) only hold if draining does NOT shrink or grow the
//! backing array, which is why both Rust ports additionally pin capacity
//! retention across repeated fills. The sibling unit tests
//! `TestTaskStack`/`TestTaskFunctionality` (task_test.go:39/:55,
//! batch part2) cover the pooled-capacity contract, so these stay inside
//! the benchmarks' own workload.

use tidb_planner::task_stack::{StackTask, TaskStack};

/// Go `TestTaskImpl` (task_test.go:31): its `Desc` writes
/// `strconv.Itoa(a)` (task_test.go:35).
struct NumberedTask {
    a: i64,
}

impl StackTask for NumberedTask {
    fn desc(&self) -> String {
        self.a.to_string()
    }
}

/// Pushes `count` ascending-numbered tasks and drains the stack, collecting
/// the descriptions in pop order. Mirrors `fill` from `BenchmarkTestStack*`.
fn fill(stack: &mut TaskStack, count: i64) -> Vec<String> {
    let mut popped_descs = Vec::with_capacity(count as usize);
    for idx in 0..count {
        stack.push(NumberedTask { a: idx });
    }
    while let Some(task) = stack.pop() {
        popped_descs.push(task.desc());
    }
    popped_descs
}

/// GO PORT of `pkg/planner/cascades/task/task_test.go:164
/// BenchmarkTestStack2Pointer`.
///
/// The benchmark fixes the "two pointer" stack shape's steady-state costs;
/// this port pins the invariant those numbers rely on: a 1000-slot stack
/// filled and drained yields 1000 tasks in strict LIFO order (999..0, since
/// `Pop` removes the tail, `task.go:64`), leaves the stack empty
/// (`Empty`, `task.go:79`) and keeps its 1000-slot allocation intact
/// across consecutive fills.
#[test]
fn stack2_pointer_fill_drains_1000_tasks_lifo() {
    let expected: Vec<String> = (0..1000i64).rev().map(|a| a.to_string()).collect();

    let mut stack = TaskStack::with_capacity(1000);
    assert_eq!(fill(&mut stack, 1000), expected);
    assert!(stack.is_empty());

    // Second pass like a Go benchmark iteration would be; popping must not
    // have shrunk the backing allocation.
    assert_eq!(stack.capacity(), 1000);
    assert_eq!(fill(&mut stack, 1000), expected);
    assert_eq!(stack.capacity(), 1000);
}

/// GO PORT of `pkg/planner/cascades/task/task_test.go:182
/// BenchmarkTestStackInterface`.
///
/// Same workload routed through the opaque-interface representation: the
/// Rust `TaskStack` stores `Box<dyn StackTask>` exactly as the Go `Stack`
/// stores `[]base.Task` (`task.go:32`), so description dispatch happens
/// dynamically on every element. Pins both the dynamic-dispatch `desc`
/// path and `Stack.Desc`'s (`task.go:51`) bottom-to-top, newline-per-task
/// rendering while the tasks are still on the stack.
#[test]
fn stack_interface_fill_drains_1000_tasks_lifo() {
    let mut stack = TaskStack::with_capacity(1000);
    for idx in 0..4 {
        stack.push(NumberedTask { a: idx });
    }

    // In-place description before any pop: stack order bottom-to-top with
    // one trailing newline per task (`task.go:51-57`).
    assert_eq!(stack.describe(), "0\n1\n2\n3\n");

    // Drain: tail first, so descriptions come out descending (LIFO).
    let mut drained = Vec::new();
    while let Some(task) = stack.pop() {
        drained.push(task.desc());
    }
    assert_eq!(drained, vec!["3", "2", "1", "0"]);
    assert!(stack.is_empty());
}
