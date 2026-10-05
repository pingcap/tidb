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

//! Behavioral tests retained from Go. Removed documentary entries are
//! indexed in rust/docs/parity/current-audit/comment-test-cleanup-validation.json.

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
