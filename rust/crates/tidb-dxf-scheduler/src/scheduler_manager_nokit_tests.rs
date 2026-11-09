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

//! Go `scheduler_manager_nokit_test.go`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_dxf::{Task, TaskBase, TaskType, IMPORT_INTO, NODE_RESOURCE_FOR_TEST, TASK_TYPE_EXAMPLE};
use tidb_dxf_storage::StorageError;
use tidb_gocontext::Context;

use super::Manager;
use crate::interface::{
    clear_cleaner_factory, register_cleaner_factory, register_scheduler_clean_up_factory, BatchCleaner, CleanUpRoutine,
    Cleaner, Error,
};
use crate::mock::{MockScheduler, MockServerInfo, MockTaskManager};
use crate::scheduler::plain;
use crate::{Scheduler, TaskManager};

fn new_manager(ctx: &Context, task_mgr: Arc<MockTaskManager>) -> Arc<Manager> {
    Manager::new(ctx, task_mgr as Arc<dyn TaskManager>, "1", NODE_RESOURCE_FOR_TEST.clone(), Arc::new(MockServerInfo::new(&[])))
}

fn task(id: i64, tp: &TaskType) -> Task {
    Task { base: TaskBase { id, tp: tp.clone(), ..TaskBase::default() }, ..Task::default() }
}

fn ids(tasks: &[Task]) -> Vec<i64> {
    tasks.iter().map(|task| task.base.id).collect()
}

/// Go `batchCleanerCallRecorder`.
#[derive(Default)]
struct BatchCleanerCallRecorder {
    calls: Mutex<Vec<i64>>,
    batch_calls: Mutex<Vec<Vec<i64>>>,
    batch_err: Option<Error>,
}

impl Cleaner for BatchCleanerCallRecorder {
    fn clean(&self, _: &Context, task: &mut Task) -> Result<(), Error> {
        self.calls.lock().unwrap().push(task.base.id);
        Ok(())
    }
    fn as_batch_cleaner(&self) -> Option<&dyn BatchCleaner> {
        Some(self)
    }
}

impl BatchCleaner for BatchCleanerCallRecorder {
    fn batch_clean(&self, _: &Context, tasks: &mut [Task]) -> Result<(), Error> {
        self.batch_calls.lock().unwrap().push(ids(tasks));
        self.batch_err.clone().map_or(Ok(()), Err)
    }
}

/// Go `singleCleanerCallRecorder`.
#[derive(Default)]
struct SingleCleanerCallRecorder {
    calls: Mutex<Vec<i64>>,
    fail_task_id: i64,
    cleanup_err: Option<Error>,
}

impl Cleaner for SingleCleanerCallRecorder {
    fn clean(&self, _: &Context, task: &mut Task) -> Result<(), Error> {
        self.calls.lock().unwrap().push(task.base.id);
        if task.base.id == self.fail_task_id {
            return self.cleanup_err.clone().map_or(Ok(()), Err);
        }
        Ok(())
    }
}

/// Go `legacyCleanerCallRecorder`.
#[derive(Default)]
struct LegacyCleanerCallRecorder {
    calls: Mutex<Vec<i64>>,
}

impl CleanUpRoutine for LegacyCleanerCallRecorder {
    fn clean_up(&self, _: &Context, task: &mut Task) -> Result<(), Error> {
        self.calls.lock().unwrap().push(task.base.id);
        Ok(())
    }
}

fn register<C: Cleaner + 'static>(task_type: TaskType, cleaner: &Arc<C>) {
    let cleaner = Arc::clone(cleaner);
    register_cleaner_factory(task_type, Arc::new(move || Arc::clone(&cleaner) as Arc<dyn Cleaner>));
}

/// Go `TestManagerSchedulersOrdered`.
#[test]
fn test_manager_schedulers_ordered() {
    let mgr = new_manager(&Context::background(), Arc::new(MockTaskManager::default()));
    for i in 1..=5 {
        let task = Task { base: TaskBase { id: i * 10, ..TaskBase::default() }, ..Task::default() };
        mgr.add_scheduler(task.base.id, Arc::new(MockScheduler::new(task)));
    }
    let ordered = |schedulers: &[Arc<dyn Scheduler>]| {
        schedulers.windows(2).all(|pair| pair[0].get_task().compare_task(&pair[1].get_task()) == std::cmp::Ordering::Less)
    };
    assert_eq!(mgr.get_schedulers().len(), 5);
    assert!(ordered(&mgr.get_schedulers()));

    let task35 = Task { base: TaskBase { id: 35, ..TaskBase::default() }, ..Task::default() };
    let mock_scheduler35 = Arc::new(MockScheduler::new(task35));

    mgr.del_scheduler(30);
    assert!(!mgr.has_scheduler(30));
    mgr.add_scheduler(35, mock_scheduler35);
    assert!(mgr.has_scheduler(35));
    assert_eq!(mgr.get_schedulers().len(), 5);
    assert!(ordered(&mgr.get_schedulers()));
}

/// Go `TestSchedulerCleanTask`.
#[test]
fn test_scheduler_clean_task() {
    let ok = || -> tidb_dxf_storage::Result<()> { Ok(()) };
    let cleanup = |tasks: Vec<Task>| -> tidb_dxf_storage::Result<Vec<Task>> { Ok(tasks) };

    // processes one bounded batch
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    task_mgr.expect("GetCleanupTasks", cleanup(vec![task(1, &TaskType::new(""))]));
    task_mgr.expect("TransferTasks2History", ok());
    assert!(mgr.process_clean_task_batch());
    assert_eq!(task_mgr.take_calls(), ["TransferTasks2History([1])"]);
    task_mgr.assert_satisfied();

    // drains consecutive bounded batches
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    task_mgr.expect("GetCleanupTasks", cleanup(vec![task(1, &TaskType::new(""))]));
    task_mgr.expect("TransferTasks2History", ok());
    task_mgr.expect("GetCleanupTasks", cleanup(vec![task(2, &TaskType::new(""))]));
    task_mgr.expect("TransferTasks2History", ok());
    task_mgr.expect("GetCleanupTasks", cleanup(Vec::new()));
    mgr.drain_clean_task_batches();
    assert_eq!(task_mgr.take_calls(), ["TransferTasks2History([1])", "TransferTasks2History([2])"]);
    task_mgr.assert_satisfied();

    // stops draining after partial batch cleanup
    clear_cleaner_factory();
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    let clean_task_type = TaskType::new("CleanWithError");
    let cleaner = Arc::new(SingleCleanerCallRecorder { fail_task_id: 2, cleanup_err: Some(plain("cleanup failed")), ..Default::default() });
    register(clean_task_type.clone(), &cleaner);
    task_mgr.expect("GetCleanupTasks", cleanup(vec![task(1, &TaskType::new("")), task(2, &clean_task_type)]));
    task_mgr.expect("TransferTasks2History", ok());
    mgr.drain_clean_task_batches();
    assert_eq!(*cleaner.calls.lock().unwrap(), [2]);
    assert_eq!(task_mgr.take_calls(), ["TransferTasks2History([1])"]);
    task_mgr.assert_satisfied();
    clear_cleaner_factory();

    // stops draining without history transfer progress
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    let cleaner = Arc::new(SingleCleanerCallRecorder { fail_task_id: 1, cleanup_err: Some(plain("cleanup failed")), ..Default::default() });
    register(clean_task_type.clone(), &cleaner);
    task_mgr.expect("GetCleanupTasks", cleanup(vec![task(1, &clean_task_type)]));
    task_mgr.expect("TransferTasks2History", ok());
    mgr.drain_clean_task_batches();
    assert_eq!(*cleaner.calls.lock().unwrap(), [1]);
    assert_eq!(task_mgr.take_calls(), ["TransferTasks2History([])"]);
    task_mgr.assert_satisfied();
    clear_cleaner_factory();

    // stops draining after history transfer failure
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    task_mgr.expect("GetCleanupTasks", cleanup(vec![task(1, &TaskType::new(""))]));
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("TransferTasks2History", Err(StorageError::Other("transfer failed".to_owned())));
    mgr.drain_clean_task_batches();
    task_mgr.assert_satisfied();

    // runs cleanup immediately on startup
    let task_mgr = Arc::new(MockTaskManager::default());
    let (loop_ctx, cancel) = Context::with_cancel(&Context::background());
    let mgr = new_manager(&loop_ctx, Arc::clone(&task_mgr));
    let (started_tx, started_rx) = crossbeam_channel::bounded::<()>(1);
    let answer: Box<dyn FnOnce() -> tidb_dxf_storage::Result<Vec<Task>> + Send> = Box::new(move || {
        started_tx.send(()).unwrap();
        Ok(Vec::new())
    });
    task_mgr.expect("GetCleanupTasks", answer);
    let loop_done = {
        let mgr = Arc::clone(&mgr);
        std::thread::spawn(move || mgr.clean_task_loop())
    };
    started_rx.recv_timeout(Duration::from_secs(3)).expect("cleanup task loop did not run immediately");
    cancel.cancel();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !loop_done.is_finished() {
        assert!(std::time::Instant::now() < deadline, "cleanup task loop did not stop");
        std::thread::sleep(Duration::from_millis(10));
    }
    loop_done.join().unwrap();
    task_mgr.assert_satisfied();
}

/// Go `TestSchedulerCleanFinishedTasks`.
#[test]
fn test_scheduler_clean_finished_tasks() {
    let other_batch_task_type = TaskType::new("OtherBatch");
    let no_cleaner_task_type = TaskType::new("NoCleaner");
    let ok = || -> tidb_dxf_storage::Result<()> { Ok(()) };
    let sorted = |call: String| {
        let inner = call.trim_start_matches("TransferTasks2History([").trim_end_matches("])");
        let mut ids: Vec<i64> = inner.split(", ").filter(|id| !id.is_empty()).map(|id| id.parse().unwrap()).collect();
        ids.sort_unstable();
        ids
    };

    // batch cleanup by capability
    clear_cleaner_factory();
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    let import_cleaner = Arc::new(BatchCleanerCallRecorder::default());
    let other_batch_cleaner = Arc::new(BatchCleanerCallRecorder::default());
    let example_cleaner = Arc::new(SingleCleanerCallRecorder::default());
    register(IMPORT_INTO, &import_cleaner);
    register(other_batch_task_type.clone(), &other_batch_cleaner);
    register(TASK_TYPE_EXAMPLE, &example_cleaner);
    let tasks = vec![
        task(1, &IMPORT_INTO),
        task(2, &other_batch_task_type),
        task(3, &TASK_TYPE_EXAMPLE),
        task(4, &IMPORT_INTO),
        task(5, &other_batch_task_type),
        task(6, &no_cleaner_task_type),
    ];
    task_mgr.expect("TransferTasks2History", ok());
    let transferred_task_count = mgr.clean_finished_tasks(tasks.clone()).unwrap();
    assert_eq!(transferred_task_count, tasks.len());
    assert_eq!(sorted(task_mgr.take_calls().remove(0)), ids(&tasks));
    assert_eq!(*import_cleaner.batch_calls.lock().unwrap(), [vec![1, 4]]);
    assert!(import_cleaner.calls.lock().unwrap().is_empty());
    assert_eq!(*other_batch_cleaner.batch_calls.lock().unwrap(), [vec![2, 5]]);
    assert!(other_batch_cleaner.calls.lock().unwrap().is_empty());
    assert_eq!(*example_cleaner.calls.lock().unwrap(), [3]);
    clear_cleaner_factory();

    // single cleanup failure
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    let batch_cleaner = Arc::new(BatchCleanerCallRecorder::default());
    let single_cleaner =
        Arc::new(SingleCleanerCallRecorder { fail_task_id: 4, cleanup_err: Some(plain("single cleanup failed")), ..Default::default() });
    register(IMPORT_INTO, &batch_cleaner);
    register(TASK_TYPE_EXAMPLE, &single_cleaner);
    let tasks = vec![
        task(1, &no_cleaner_task_type),
        task(2, &TASK_TYPE_EXAMPLE),
        task(3, &IMPORT_INTO),
        task(4, &TASK_TYPE_EXAMPLE),
        task(5, &IMPORT_INTO),
        task(6, &no_cleaner_task_type),
    ];
    task_mgr.expect("TransferTasks2History", ok());
    let transferred_task_count = mgr.clean_finished_tasks(tasks).unwrap();
    assert_eq!(transferred_task_count, 3);
    assert_eq!(sorted(task_mgr.take_calls().remove(0)), [1, 2, 6]);
    assert_eq!(*single_cleaner.calls.lock().unwrap(), [2, 4]);
    assert!(batch_cleaner.batch_calls.lock().unwrap().is_empty());
    clear_cleaner_factory();

    // batch cleanup failure
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    let import_cleaner = Arc::new(BatchCleanerCallRecorder { batch_err: Some(plain("batch cleanup failed")), ..Default::default() });
    let other_batch_cleaner = Arc::new(BatchCleanerCallRecorder { batch_err: Some(plain("batch cleanup failed")), ..Default::default() });
    let single_cleaner = Arc::new(SingleCleanerCallRecorder::default());
    register(IMPORT_INTO, &import_cleaner);
    register(other_batch_task_type.clone(), &other_batch_cleaner);
    register(TASK_TYPE_EXAMPLE, &single_cleaner);
    let tasks = vec![
        task(1, &IMPORT_INTO),
        task(2, &other_batch_task_type),
        task(3, &TASK_TYPE_EXAMPLE),
        task(4, &IMPORT_INTO),
        task(5, &other_batch_task_type),
        task(6, &no_cleaner_task_type),
    ];
    task_mgr.expect("TransferTasks2History", ok());
    let transferred_task_count = mgr.clean_finished_tasks(tasks).unwrap();
    assert_eq!(transferred_task_count, 2);
    assert_eq!(sorted(task_mgr.take_calls().remove(0)), [3, 6]);
    assert_eq!(*single_cleaner.calls.lock().unwrap(), [3]);
    assert_eq!(import_cleaner.batch_calls.lock().unwrap().len() + other_batch_cleaner.batch_calls.lock().unwrap().len(), 1);
    clear_cleaner_factory();

    // history transfer failure
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("TransferTasks2History", Err(StorageError::Other("transfer failed".to_owned())));
    let error = mgr.clean_finished_tasks(vec![task(1, &no_cleaner_task_type)]).unwrap_err();
    assert_eq!(error, StorageError::Other("transfer failed".to_owned()));
    clear_cleaner_factory();

    // legacy cleanup factory compatibility
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    let legacy_cleaner = Arc::new(LegacyCleanerCallRecorder::default());
    {
        let legacy_cleaner = Arc::clone(&legacy_cleaner);
        register_scheduler_clean_up_factory(
            TASK_TYPE_EXAMPLE,
            Arc::new(move || Arc::clone(&legacy_cleaner) as Arc<dyn CleanUpRoutine>),
        );
    }
    task_mgr.expect("TransferTasks2History", ok());
    let transferred_task_count = mgr.clean_finished_tasks(vec![task(1, &TASK_TYPE_EXAMPLE)]).unwrap();
    assert_eq!(transferred_task_count, 1);
    assert_eq!(task_mgr.take_calls(), ["TransferTasks2History([1])"]);
    assert_eq!(*legacy_cleaner.calls.lock().unwrap(), [1]);
    clear_cleaner_factory();
}
