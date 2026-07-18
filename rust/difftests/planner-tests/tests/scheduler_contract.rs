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
//! `pkg/planner/cascades/base/task_scheduler_base.go`.
//!
//! The concrete Go behavior anchor `TestSimpleTaskScheduler` at
//! `pkg/planner/cascades/task/task_scheduler_test.go:57` is separately owned
//! by the task-scheduler leaf. These tests exercise only the base interface
//! dispatch through `dyn Scheduler`.

use std::cell::RefCell;
use std::rc::Rc;

use tidb_planner::scheduler_contract::Scheduler;
use tidb_planner::task_scheduler::{SimpleTaskScheduler, Task};

struct RecordingTask {
    id: u8,
    seen: Rc<RefCell<Vec<u8>>>,
}

impl Task for RecordingTask {
    fn execute(&mut self) -> Result<(), String> {
        self.seen.borrow_mut().push(self.id);
        Ok(())
    }
}

#[test]
fn scheduler_contract_dispatches_opaque_tasks_in_lifo_order() {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let mut concrete = SimpleTaskScheduler::new();
    let scheduler: &mut dyn Scheduler = &mut concrete;
    for id in [1, 2, 3] {
        Scheduler::push_task(
            scheduler,
            Box::new(RecordingTask {
                id,
                seen: Rc::clone(&seen),
            }),
        );
    }

    assert_eq!(Scheduler::execute_tasks(scheduler), Ok(()));
    assert_eq!(*seen.borrow(), vec![3, 2, 1]);
}

#[test]
fn scheduler_contract_destroy_releases_pending_tasks() {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let mut concrete = SimpleTaskScheduler::new();
    let scheduler: &mut dyn Scheduler = &mut concrete;
    Scheduler::push_task(
        scheduler,
        Box::new(RecordingTask {
            id: 1,
            seen: Rc::clone(&seen),
        }),
    );
    Scheduler::destroy(scheduler);

    assert_eq!(Scheduler::execute_tasks(scheduler), Ok(()));
    assert!(seen.borrow().is_empty());
}
