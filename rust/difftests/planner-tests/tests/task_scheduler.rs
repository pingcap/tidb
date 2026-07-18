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
//! `pkg/planner/cascades/task/task_scheduler.go`.
//!
//! The direct Go anchor is `TestSimpleTaskScheduler` at
//! `pkg/planner/cascades/task/task_scheduler_test.go:57`.

use tidb_planner::task_scheduler::{SimpleTaskScheduler, Task};

struct TestTask {
    id: u8,
}

impl Task for TestTask {
    fn execute(&mut self) -> Result<(), String> {
        if self.id == 2 {
            Err("mock error at task id = 2".to_owned())
        } else {
            Ok(())
        }
    }
}

#[test]
fn scheduler_stops_at_first_lifo_task_error() {
    let mut scheduler = SimpleTaskScheduler::new();
    scheduler.push_task(TestTask { id: 1 });
    scheduler.push_task(TestTask { id: 2 });
    scheduler.push_task(TestTask { id: 3 });

    assert_eq!(
        scheduler.execute_tasks(),
        Err("mock error at task id = 2".to_owned())
    );
    assert_eq!(scheduler.pending_len(), 1);
}

#[test]
fn scheduler_drains_successful_tasks_and_destroy_clears_pending() {
    let mut scheduler = SimpleTaskScheduler::new();
    scheduler.push_task(TestTask { id: 1 });
    scheduler.push_task(TestTask { id: 3 });
    assert_eq!(scheduler.execute_tasks(), Ok(()));
    assert_eq!(scheduler.pending_len(), 0);

    scheduler.push_task(TestTask { id: 1 });
    scheduler.destroy();
    assert_eq!(scheduler.pending_len(), 0);
}
