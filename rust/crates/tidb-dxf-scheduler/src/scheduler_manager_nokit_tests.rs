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

use tidb_dxf::{
    Task, TaskBase, TaskState, TaskType, IMPORT_INTO, NODE_RESOURCE_FOR_TEST, TASK_STATE_CANCELLING, TASK_STATE_MODIFYING,
    TASK_STATE_PAUSING, TASK_STATE_PENDING, TASK_STATE_REVERTED, TASK_STATE_REVERTING, TASK_STATE_SUCCEED, TASK_TYPE_EXAMPLE,
};
use tidb_dxf_storage::StorageError;
use tidb_gocontext::Context;

use super::Manager;
use crate::interface::{
    clear_cleaner_factory, register_cleaner_factory, register_scheduler_clean_up_factory, BatchCleaner, CleanUpRoutine,
    Cleaner, Error, Param,
};
use crate::mock::{
    new_session_pool_for_store, MockExtension, MockRuntime, MockRuntimeHandle, MockScheduler, MockServer, MockServerInfo,
    MockSession, MockTaskManager,
};
use crate::scheduler::{plain, BEFORE_REFRESH_TASK};
use crate::{clear_scheduler_factory, register_scheduler_factory, BaseScheduler, Extension, Scheduler, TaskManager};

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

/// Go `newRuntimeWithStore`: a runtime whose store and sessions are of the
/// keyspace.
fn new_runtime_with_store(keyspace: &str) -> Arc<MockRuntime> {
    Arc::new(MockRuntime { keyspace: keyspace.to_owned(), se_pool: Some(new_session_pool_for_store(keyspace)) })
}

/// Go `expectRuntimeFromNewSession`: the sessions are of the runtime's
/// store, so `AcquireTaskRuntime` takes the local-runtime path.
fn expect_runtime_from_new_session(task_mgr: &MockTaskManager, runtime: Arc<MockRuntime>) {
    let keyspace = runtime.keyspace.clone();
    let server = MockServer { runtime: Some(runtime), ..MockServer::default() };
    task_mgr.set_session(Arc::new(MockSession { keyspace, server: Some(Arc::new(server)) }));
}

/// The schedulers Go's `NewBaseScheduler` + `GetTestSchedulerExt` factory
/// made, with the extensions they hold weakly.
type TestSchedulers = Arc<Mutex<Vec<(Arc<BaseScheduler>, Arc<dyn Extension>)>>>;

fn register_test_scheduler_factory() -> TestSchedulers {
    let created: TestSchedulers = Arc::default();
    let schedulers = Arc::clone(&created);
    register_scheduler_factory(
        TASK_TYPE_EXAMPLE.clone(),
        Arc::new(move |ctx, task, param| {
            let mock_scheduler = Arc::new(BaseScheduler::new(ctx, task, param));
            let ext: Arc<dyn Extension> = Arc::new(MockExtension::test_scheduler_ext());
            mock_scheduler.set_extension(Arc::downgrade(&ext));
            schedulers.lock().unwrap().push((Arc::clone(&mock_scheduler), ext));
            mock_scheduler
        }),
    );
    created
}

/// Go `mgr.schedulerWG.Wait()`, failing on a scheduler's panic.
fn wait_schedulers(mgr: &Manager) {
    let handles = std::mem::take(&mut *mgr.scheduler_wg.lock().unwrap());
    for handle in handles {
        if let Err(panic) = handle.join() {
            std::panic::resume_unwind(panic);
        }
    }
}

fn task_base(id: i64, state: TaskState) -> TaskBase {
    TaskBase { id, required_slots: 1, tp: TASK_TYPE_EXAMPLE.clone(), state, ..TaskBase::default() }
}

fn expect_task(task_mgr: &MockTaskManager, task: &TaskBase) {
    task_mgr.expect(format!("GetTaskByID:{}", task.id), Ok::<Task, StorageError>(Task { base: task.clone(), ..Task::default() }));
}

/// Go `TestManagerSchedulerNotAllocateSlots`.
#[test]
fn test_manager_scheduler_not_allocate_slots() {
    // the tests make sure allocatedSlots correct.
    fail::cfg("exitScheduler", "return").unwrap();
    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    expect_runtime_from_new_session(&task_mgr, new_runtime_with_store(""));
    let schedulers = register_test_scheduler_factory();
    *task_mgr.used_slots_any_times.lock().unwrap() = true;
    let tasks = vec![
        task_base(1, TASK_STATE_CANCELLING.clone()),
        task_base(2, TASK_STATE_REVERTING.clone()),
        task_base(3, TASK_STATE_PAUSING.clone()),
    ];
    for task in &tasks {
        expect_task(&task_mgr, task);
        task_mgr.expect(format!("GetTaskBaseByID:{}", task.id), Ok::<TaskBase, StorageError>(task.clone()));
    }

    mgr.start_schedulers(tasks).unwrap();
    assert_eq!(mgr.get_schedulers().len(), 3);
    let schs: Vec<Arc<BaseScheduler>> = schedulers.lock().unwrap().iter().map(|(sch, _)| Arc::clone(sch)).collect();
    assert_eq!(schs.len(), 3);
    for sch in schs {
        assert!(!sch.param.allocated_slots);
        mgr.finish_ch.1.recv().unwrap();
    }
    wait_schedulers(&mgr);
    fail::remove("exitScheduler");
}

/// Go `crossKeyspaceStartCase`.
struct CrossKeyspaceStartCase {
    task_mgr: Arc<MockTaskManager>,
    manager: Arc<Manager>,
    task: Task,
    server: Arc<MockServer>,
}

impl CrossKeyspaceStartCase {
    /// Go `newCrossKeyspaceStartCase`.
    fn new(task_id: i64, task_key: &str) -> Self {
        clear_scheduler_factory();
        let task_mgr = Arc::new(MockTaskManager::default());
        let manager = new_manager(&Context::background(), Arc::clone(&task_mgr));
        let task = Task {
            base: TaskBase {
                id: task_id,
                key: task_key.to_owned(),
                tp: TASK_TYPE_EXAMPLE.clone(),
                keyspace: "user_ks".to_owned(),
                ..TaskBase::default()
            },
            ..Task::default()
        };
        let server = Arc::new(MockServer::default());
        let server_dyn: Arc<dyn tidb_domain_sqlsvrapi::Server<dyn tidb_dxf_storage::SessionContext>> = server.clone();
        task_mgr.set_session(Arc::new(MockSession { keyspace: "SYSTEM".to_owned(), server: Some(server_dyn) }));
        Self { task_mgr, manager, task, server }
    }

    /// Go `holderID`.
    fn holder_id(&self) -> String {
        tidb_dxf_dxfutil::gen_holder_id("scheduler", self.task.base.id)
    }

    fn expect_get_task(&self) {
        expect_task(&self.task_mgr, &self.task.base);
    }

    /// Go `expectRuntimeAcquiredAndReleased`.
    fn expect_runtime_acquired_and_released(&self) -> Arc<MockRuntimeHandle> {
        let runtime_handle = Arc::new(MockRuntimeHandle { keyspace: self.task.base.keyspace.clone(), released: Default::default() });
        self.server.acquire.lock().unwrap().push_back(Ok(Arc::clone(&runtime_handle)));
        runtime_handle
    }

    fn assert_acquired(&self) {
        assert_eq!(*self.server.acquired.lock().unwrap(), vec![(self.task.base.keyspace.clone(), self.holder_id())]);
    }
}

fn same_runtime(param: &Param, handle: &Arc<MockRuntimeHandle>) -> bool {
    let runtime = param.task_runtime.as_ref().expect("the manager hands the task runtime");
    std::ptr::addr_eq(Arc::as_ptr(runtime), Arc::as_ptr(handle))
}

/// Go `TestStartSchedulerCrossKeyspaceRuntime`.
#[test]
fn test_start_scheduler_cross_keyspace_runtime() {
    // acquires cross-keyspace runtime and releases it on exit
    {
        let tc = CrossKeyspaceStartCase::new(101, "cross-ks-scheduler");
        tc.expect_get_task();
        let runtime_handle = tc.expect_runtime_acquired_and_released();
        let (run_tx, run_rx) = crossbeam_channel::bounded::<()>(1);
        let created: Arc<Mutex<Vec<Arc<MockScheduler>>>> = Arc::default();
        {
            let (task_id, runtime_handle, created) = (tc.task.base.id, Arc::clone(&runtime_handle), Arc::clone(&created));
            let run_tx = Mutex::new(Some(run_tx));
            register_scheduler_factory(
                TASK_TYPE_EXAMPLE.clone(),
                Arc::new(move |_, got_task, param| {
                    assert_eq!(got_task.base.id, task_id);
                    assert!(same_runtime(&param, &runtime_handle));
                    assert_eq!(param.task_runtime.as_ref().unwrap().store_keyspace(), "user_ks");
                    let scheduler = Arc::new(MockScheduler::new(got_task));
                    scheduler.ext.expect("Init", Ok::<(), Error>(()));
                    scheduler.ext.expect("ScheduleTask", ());
                    let run_tx = run_tx.lock().unwrap().take();
                    *scheduler.on_schedule_task.lock().unwrap() = Some(Box::new(move || drop(run_tx)));
                    scheduler.ext.expect("Close", ());
                    created.lock().unwrap().push(Arc::clone(&scheduler));
                    scheduler
                }),
            );
        }

        tc.manager.start_scheduler(&tc.task.base, false, "");
        assert!(matches!(
            run_rx.recv_timeout(Duration::from_secs(5)),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected)
        ));
        wait_schedulers(&tc.manager);
        tc.assert_acquired();
        assert_eq!(runtime_handle.released.load(std::sync::atomic::Ordering::SeqCst), 1);
        let created = created.lock().unwrap();
        assert_eq!(created.len(), 1);
        for method in ["Init", "ScheduleTask", "Close"] {
            assert!(!created[0].ext.has_answer(method), "{method} was not called");
        }
        tc.task_mgr.assert_satisfied();
    }

    // releases cross-keyspace runtime when scheduler init fails
    {
        let mut tc = CrossKeyspaceStartCase::new(102, "cross-ks-scheduler-init-fail");
        tc.task.base.state = TASK_STATE_PENDING.clone();
        tc.expect_get_task();
        let runtime_handle = tc.expect_runtime_acquired_and_released();
        tc.task_mgr.expect("FailTask", Ok::<(), StorageError>(()));
        {
            let runtime_handle = Arc::clone(&runtime_handle);
            register_scheduler_factory(
                TASK_TYPE_EXAMPLE.clone(),
                Arc::new(move |_, got_task, param| {
                    assert!(same_runtime(&param, &runtime_handle));
                    assert_eq!(param.task_runtime.as_ref().unwrap().store_keyspace(), "user_ks");
                    let scheduler = MockScheduler::new(got_task);
                    scheduler.ext.expect("Init", Err::<(), Error>(plain("init failed")));
                    Arc::new(scheduler)
                }),
            );
        }

        tc.manager.start_scheduler(&tc.task.base, false, "");
        tc.assert_acquired();
        assert_eq!(runtime_handle.released.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(tc.task_mgr.take_calls(), ["FailTask(102, pending, init failed)"]);
        tc.task_mgr.assert_satisfied();
    }

    // does not start scheduler when cross-keyspace runtime acquisition fails
    {
        let mut tc = CrossKeyspaceStartCase::new(104, "cross-ks-scheduler-acquire-fail");
        tc.task.base.state = TASK_STATE_PENDING.clone();
        tc.expect_get_task();
        tc.server.acquire.lock().unwrap().push_back(Err("acquire failed".to_owned()));
        let factory_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let factory_called = Arc::clone(&factory_called);
            register_scheduler_factory(
                TASK_TYPE_EXAMPLE.clone(),
                Arc::new(move |_, got_task, _| {
                    factory_called.store(true, std::sync::atomic::Ordering::SeqCst);
                    Arc::new(MockScheduler::new(got_task))
                }),
            );
        }

        tc.manager.start_scheduler(&tc.task.base, false, "");

        tc.assert_acquired();
        assert!(!factory_called.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!tc.manager.has_scheduler(tc.task.base.id));
        tc.task_mgr.assert_satisfied();
    }
    clear_scheduler_factory();
}

/// Go `TestFastRespondNoNeedResourceTaskWhenSchedulersReachLimit`.
#[test]
fn test_fast_respond_no_need_resource_task_when_schedulers_reach_limit() {
    let restore = tidb_dxf::set_max_concurrent_task_for_test(1);

    let task_mgr = Arc::new(MockTaskManager::default());
    let mgr = new_manager(&Context::background(), Arc::clone(&task_mgr));
    expect_runtime_from_new_session(&task_mgr, new_runtime_with_store(""));
    task_mgr.expect_get_all_nodes(Ok(vec![tidb_dxf::ManagedNode { cpu_count: 8, ..tidb_dxf::ManagedNode::default() }]));
    mgr.node_mgr.refresh_nodes(&tidb_gocontext::Context::background(), task_mgr.as_ref(), &mgr.slot_mgr);
    let _schedulers = register_test_scheduler_factory();
    for state in [TASK_STATE_CANCELLING, TASK_STATE_REVERTING, TASK_STATE_MODIFYING, TASK_STATE_PAUSING] {
        let (ch_tx, ch_rx) = crossbeam_channel::bounded::<()>(0);
        *BEFORE_REFRESH_TASK.write().unwrap() = Some(Arc::new(move |task: &Task| {
            if task.base.id == 1 {
                let _ = ch_rx.recv();
            }
        }));
        *task_mgr.used_slots_any_times.lock().unwrap() = true;
        let task1 = task_base(1, TASK_STATE_PENDING.clone());
        let task1_success = TaskBase { state: TASK_STATE_SUCCEED.clone(), ..task1.clone() };
        expect_task(&task_mgr, &task1);
        task_mgr.expect("GetTaskBaseByID:1", Ok::<TaskBase, StorageError>(task1_success.clone()));
        expect_task(&task_mgr, &task1_success);

        let task2 = task_base(2, state.clone());
        // we use 'reverted' to finish the task, no matter what state it is.
        let task2_reverted = TaskBase { state: TASK_STATE_REVERTED.clone(), ..task2.clone() };
        let cancel_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        expect_task(&task_mgr, &task2);
        {
            let (cancel_called, task2_reverted) = (Arc::clone(&cancel_called), task2_reverted.clone());
            let answer: Box<dyn FnOnce() -> tidb_dxf_storage::Result<TaskBase> + Send> = Box::new(move || {
                cancel_called.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(task2_reverted)
            });
            task_mgr.expect("GetTaskBaseByID:2", answer);
        }
        expect_task(&task_mgr, &task2_reverted);

        mgr.start_schedulers(vec![task1, task2]).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while !cancel_called.load(std::sync::atomic::Ordering::SeqCst) {
            assert!(std::time::Instant::now() < deadline, "{state}: the no-resource task was not scheduled");
            std::thread::sleep(Duration::from_millis(100));
        }
        drop(ch_tx);
        wait_schedulers(&mgr);
        task_mgr.assert_satisfied();
        *BEFORE_REFRESH_TASK.write().unwrap() = None;
    }
    restore();
}
