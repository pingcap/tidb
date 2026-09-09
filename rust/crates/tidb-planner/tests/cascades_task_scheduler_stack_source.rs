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

//! Port ledger for `pkg/planner/cascades/task` (`pkg/planner.part2` items
//! 118-120 on `origin/master`).
//!
//! TWO tests are real functional ports over [`tidb_planner::task_scheduler`]
//! (transcreation of `pkg/planner/cascades/task/task_scheduler.go`) and
//! [`tidb_planner::task_stack`] (transcreation of
//! `pkg/planner/cascades/task/task.go`, whose pooled-capacity contract this
//! crate externalizes into explicit constructors — see the part3 sibling
//! `cascades_task_stack_source.rs` for the benchmark shapes). ONE test pins Go
//! runtime memory layout (`unsafe.Sizeof`) that has no honest Rust carrier and
//! stays a documentary gap.

use tidb_planner::task_scheduler::{SimpleTaskScheduler, Task};
use tidb_planner::task_stack::{StackTask, TaskStack};

/// Mock task from task_scheduler_test.go:35-44 / task_test.go:25-32: its
/// description writes `strconv.Itoa(a)`; execute succeeds unless flagged.
struct NumberedTask {
    id: i64,
    fail_when_two: bool,
}

impl Task for NumberedTask {
    fn execute(&mut self) -> Result<(), String> {
        if self.fail_when_two && self.id == 2 {
            // Mirror TestTaskImpl2.Execute at task_scheduler_test.go:40-43.
            return Err("mock error at task id = 2".to_string());
        }
        Ok(())
    }
}

impl StackTask for NumberedTask {
    fn desc(&self) -> String {
        self.id.to_string()
    }
}

/// GO PORT of
/// `pkg/planner/cascades/task/task_scheduler_test.go:57
/// TestSimpleTaskScheduler`.
///
/// Re-derived contract: pushing tasks 1, 2, 3 then executing runs LIFO — 3
/// executes fine, task 2 fails and ITS error stops the scheduler with exactly
/// the message built at :42 (ExecuteTasks pops one task per loop iteration and
/// returns the first error, task_scheduler.go:38-47). Remaining queued work is
/// not drained on failure, so pending length stays at one afterwards.
#[test]
fn simple_task_scheduler_surfaces_first_failing_task_message() {
    let mut scheduler = SimpleTaskScheduler::new();
    scheduler.push_task(NumberedTask { id: 1, fail_when_two: true });
    scheduler.push_task(NumberedTask { id: 2, fail_when_two: true });
    scheduler.push_task(NumberedTask { id: 3, fail_when_two: true });

    let err = scheduler.execute_tasks().unwrap_err();
    assert_eq!(err, "mock error at task id = 2");
    assert_eq!(scheduler.pending_len(), 1);
}

/// GO PORT of `pkg/planner/cascades/task/task_test.go:55
/// TestTaskFunctionality`.
///
/// Re-derived contract over the shared pooled stack shape: a fresh
/// `stackPool.Get()` stack starts len 0 / cap 4 (test :58-60; sync.Pool New =
/// newTaskStack at task.go:25-28 which calls newTaskStackWithCap(4),
/// task.go:84-87, mirrored by TaskStack::new); LIFO pops yield "2" then "1"
/// then nil (:61-74; Pop at task.go:64-72); re-pushing 3..=6 WITHOUT cleaning
/// keeps len 4 / cap 4 across the pool round-trip (:75-84 — contents survive
/// because Go hands back the same dirty object, the observable this port
/// carries by reusing the same stack instance), the four tasks drain 6,5,4,3
/// in order (:85-100), and Destroy() empties while retaining capacity 4 for
/// the next get (:101-106; Destroy clears the slice but keeps its array,
/// task.go:43-49).
#[test]
fn task_stack_pooled_lifecycle_drains_lifo_and_retains_capacity() {
    // Fresh pool object: empty content, four slots reserved.
    let mut ts = TaskStack::new();
    assert_eq!(ts.len(), 0);
    assert_eq!(ts.capacity(), 4);

    ts.push(NumberedTask { id: 1, fail_when_two: false });
    ts.push(NumberedTask { id: 2, fail_when_two: false });
    assert_eq!(ts.pop().expect("non-empty").desc(), "2");
    assert_eq!(ts.pop().expect("non-empty").desc(), "1");
    assert!(ts.pop().is_none());

    // Push four more without cleaning; put back / require again: contents and
    // capacity survive the round-trip.
    for id in 3..=6 {
        ts.push(NumberedTask { id, fail_when_two: false });
    }
    assert_eq!(ts.len(), 4);
    assert_eq!(ts.capacity(), 4);
    for expected in [6, 5, 4, 3] {
        let popped = ts.pop().expect("non-empty");
        assert_eq!(popped.desc(), expected.to_string());
    }
    assert!(ts.pop().is_none());

    // Self destroy: tasks gone, allocation retained for reuse.
    ts.destroy();
    assert_eq!(ts.len(), 0);
    assert_eq!(ts.capacity(), 4);
}

/// GO PORT of `pkg/planner/cascades/task/task_test.go:39 TestTaskStack`.
///
/// Re-derived contract: Go pins pointer/slice-header/interface word sizes via
/// `unsafe.Sizeof` — the Stack POINTER is 8 bytes (:46-47), the slice header
/// holding cap+len+array address is 24 bytes (:48-50), and each interface slot
/// inside `tasks` (two machine words allowing nil entries pushed at :51-53)
/// is 16 bytes (:54-58). These are Go runtime representation contracts; the
/// Rust carrier stores `Vec<Box<dyn StackTask>>` with different layout and no
/// pushable-nil slot, so the assertions have no honest equivalent here.
#[test]
#[ignore = "go-parity-gap: pins Go unsafe.Sizeof values (ptr=8, slice header=24, iface slot=16) plus Push(nil); Rust Vec<Box<dyn StackTask>> representation differs by construction"]
fn task_stack_go_memory_layout_pins_sizeof_values() {
    // Go asserts Sizeof(newSS)==8, Sizeof(newSS.tasks)==24 and per-slot ==16
    // after pushing nil, &TestTaskImpl{1}, nil (task_test.go:45-59).
}
