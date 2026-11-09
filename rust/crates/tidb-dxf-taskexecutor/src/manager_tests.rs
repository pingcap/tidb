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

//! Go `manager_test.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_dxf::{
    Task, TaskBase, TaskType, NODE_RESOURCE_FOR_TEST, STEP_ONE, TASK_STATE_PAUSING, TASK_STATE_REVERTING,
    TASK_STATE_RUNNING, TASK_TYPE_EXAMPLE,
};
use tidb_dxf_storage::{StorageError, TaskExecInfo};
use tidb_gocontext::{Context, CtxError};

use crate::manager::BEFORE_CALL_START_TASK_EXECUTOR;
use crate::mock::{
    key, storage_err, Controller, MockRuntime, MockRuntimeHandle, MockServer, MockSession, MockTaskExecutor, MockTaskTable,
    Times, ONCE,
};
use crate::task_executor::plain;
use crate::{clear_task_executors, register_task_type, Error, Manager, TaskExecutor};

type StorageResult<T> = Result<T, StorageError>;

fn new_manager(ctx: &Context, id: &str, table: &Arc<MockTaskTable>) -> Arc<Manager> {
    Manager::new(ctx, id, Arc::clone(table) as Arc<dyn crate::TaskTable>, NODE_RESOURCE_FOR_TEST.clone())
}

/// Go `expectRuntimeFromNewSession` with `newRuntimeWithStore`: the
/// sessions are of the runtime's store, so `AcquireTaskRuntime` takes the
/// local-runtime path.
fn expect_runtime_from_new_session(table: &MockTaskTable, keyspace: &str) {
    let runtime = Arc::new(MockRuntime { keyspace: keyspace.to_owned(), se_pool: None });
    let server = MockServer { runtime: Some(runtime), ..MockServer::default() };
    table.set_session(Arc::new(MockSession { keyspace: keyspace.to_owned(), server: Some(Arc::new(server)) }));
}

fn exec_info(task: &TaskBase) -> TaskExecInfo {
    TaskExecInfo { task_base: task.clone(), subtask_concurrency: 0 }
}

fn task_of(base: &TaskBase) -> StorageResult<Task> {
    Ok(Task { base: base.clone(), ..Task::default() })
}

/// Go `require.Eventually`.
fn eventually(timeout: Duration, tick: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "the condition was not met in {timeout:?}");
        std::thread::sleep(tick);
    }
}

fn ctx_canceled(err: &Error) -> bool {
    err.downcast_ref::<CtxError>() == Some(&CtxError::Canceled)
}

/// Go `TestManageTaskExecutor`.
#[test]
fn test_manage_task_executor() {
    let ctrl = Arc::new(Controller::default());
    let mock_task_table = Arc::new(MockTaskTable::new(&ctrl));
    let m = new_manager(&Context::background(), "test", &mock_task_table);
    let task = |id| TaskBase { id, ..TaskBase::default() };

    // add executor 1
    let executor1 = MockTaskExecutor::new(&ctrl, "executor1");
    ctrl.ret(executor1.key("GetTaskBase"), task(1));
    m.add_task_executor(executor1.clone());
    assert_eq!(m.task_executor_count(), 1);
    assert!(m.is_executor_started(1));
    assert!(ctrl.satisfied());
    // add executor 2
    let executor2 = MockTaskExecutor::new(&ctrl, "executor2");
    ctrl.ret(executor2.key("GetTaskBase"), task(2));
    m.add_task_executor(executor2.clone());
    assert!(m.is_executor_started(2));
    assert!(ctrl.satisfied());
    // delete executor 1
    ctrl.ret(executor1.key("GetTaskBase"), task(1));
    m.del_task_executor(executor1.as_ref());
    assert!(!m.is_executor_started(1));
    assert!(ctrl.satisfied());

    // cancel executor 2
    ctrl.ret(executor2.key("Cancel"), ());
    m.cancel_task_executors(&[task(2)]);
    assert!(ctrl.satisfied());
    // cancel running subtask of 2
    ctrl.ret(executor2.key("CancelRunningSubtask"), ());
    m.cancel_running_subtask_of(2);
    assert!(ctrl.satisfied());
    // handle pause
    ctrl.ret(executor1.key("GetTaskBase"), task(1));
    ctrl.expect(executor1.key("Cancel"), Times::N(2), |_| ());
    m.add_task_executor(executor1.clone());
    ctrl.ret("PauseSubtasks|test|1", StorageResult::Ok(()));
    m.handle_pausing_task(1).unwrap();
    ctrl.expect("PauseSubtasks|test|1", ONCE, |_| StorageResult::<()>::Err(storage_err("pause failed")));
    assert!(m.handle_pausing_task(1).unwrap_err().to_string().contains("pause failed"));
    assert!(ctrl.satisfied());

    // handle reverting
    ctrl.ret(executor1.key("GetTaskBase"), task(1));
    ctrl.ret(executor1.key("CancelRunningSubtask"), ());
    m.add_task_executor(executor1.clone());
    ctrl.ret("CancelSubtask|test|1", StorageResult::Ok(()));
    m.handle_reverting_task(1).unwrap();
    assert!(ctrl.satisfied());
    ctrl.finish();
}

/// Go `TestHandleExecutableTasks`.
#[test]
fn test_handle_executable_tasks() {
    let ctrl = Arc::new(Controller::default());
    clear_task_executors();
    let mock_task_table = Arc::new(MockTaskTable::new(&ctrl));
    let mock_internal_executor = MockTaskExecutor::new(&ctrl, "executor");

    let id = "test";
    let task_id = 1;
    let task = TaskBase {
        id: task_id,
        state: TASK_STATE_RUNNING,
        step: STEP_ONE,
        tp: TaskType::new("type"),
        required_slots: 6,
        ..TaskBase::default()
    };
    {
        let task = task.clone();
        ctrl.expect(mock_internal_executor.key("GetTaskBase"), Times::Any, move |_| task.clone());
    }

    let m = new_manager(&Context::background(), id, &mock_task_table);
    m.slot_manager().available.store(16, Ordering::SeqCst);
    expect_runtime_from_new_session(&mock_task_table, "");

    // no task
    m.handle_executable_tasks(&[]);

    // type not found
    ctrl.ret(key::get_task_by_id(task_id), task_of(&task));
    ctrl.ret(key::fail_subtask(id, task_id, "*"), StorageResult::Ok(()));
    m.start_task_executor(&task);
    assert!(ctrl.satisfied());

    {
        let executor = Arc::clone(&mock_internal_executor);
        register_task_type(TaskType::new("type"), Arc::new(move |_, _, _| Arc::clone(&executor) as Arc<dyn TaskExecutor>));
    }

    // executor init failed non retryable
    let executor_err = plain("executor init failed");
    ctrl.ret(mock_internal_executor.key("Init"), Err::<(), Error>(Arc::clone(&executor_err)));
    ctrl.ret(format!("{}|executor init failed", mock_internal_executor.key("IsRetryableError")), false);
    ctrl.ret(key::get_task_by_id(task.id), task_of(&task));
    ctrl.ret(key::fail_subtask(id, task_id, "executor init failed"), StorageResult::Ok(()));
    m.handle_executable_tasks(&[exec_info(&task)]);
    assert!(ctrl.satisfied());

    // executor init failed retryable
    ctrl.ret(mock_internal_executor.key("Init"), Err::<(), Error>(Arc::clone(&executor_err)));
    ctrl.ret(format!("{}|executor init failed", mock_internal_executor.key("IsRetryableError")), true);
    ctrl.ret(key::get_task_by_id(task.id), task_of(&task));
    m.handle_executable_tasks(&[exec_info(&task)]);
    assert!(ctrl.satisfied());

    let (ch_tx, ch_rx) = crossbeam_channel::bounded::<()>(0);
    ctrl.ret(mock_internal_executor.key("Init"), Ok::<(), Error>(()));
    ctrl.expect(mock_internal_executor.key("Run"), ONCE, move |_| {
        let _ = ch_rx.recv();
    });
    ctrl.ret(key::get_task_by_id(task.id), task_of(&task));
    m.handle_executable_tasks(&[exec_info(&task)]);
    eventually(Duration::from_secs(5), Duration::from_millis(100), || ctrl.satisfied());
    assert_eq!(m.slot_manager().available_slots(), 10);
    assert!(m.is_executor_started(task_id));
    ctrl.ret(mock_internal_executor.key("Close"), ());
    drop(ch_tx);
    m.wait_executors();
    assert!(ctrl.satisfied());
    assert_eq!(m.slot_manager().available_slots(), 16);
    assert!(!m.is_executor_started(task_id));
    ctrl.finish();
    clear_task_executors();
}

/// Go `crossKeyspaceStartCase`.
struct CrossKeyspaceStartCase {
    ctrl: Arc<Controller>,
    task_table: Arc<MockTaskTable>,
    manager: Arc<Manager>,
    task: Task,
    server: Arc<MockServer>,
}

impl CrossKeyspaceStartCase {
    /// Go `newCrossKeyspaceStartCase`.
    fn new(task_id: i64, task_key: &str) -> Self {
        clear_task_executors();
        let ctrl = Arc::new(Controller::default());
        let task_table = Arc::new(MockTaskTable::new(&ctrl));
        let manager = new_manager(&Context::background(), "exec-1", &task_table);
        let task = Task {
            base: TaskBase {
                id: task_id,
                key: task_key.to_owned(),
                tp: TASK_TYPE_EXAMPLE,
                required_slots: 1,
                keyspace: "user_ks".to_owned(),
                ..TaskBase::default()
            },
            ..Task::default()
        };
        let server = Arc::new(MockServer::default());
        ctrl.ret(key::get_task_by_id(task.base.id), StorageResult::Ok(task.clone()));
        let server_dyn: Arc<dyn tidb_domain_sqlsvrapi::Server<dyn tidb_dxf_storage::SessionContext>> = server.clone();
        task_table.set_session(Arc::new(MockSession { keyspace: "SYSTEM".to_owned(), server: Some(server_dyn) }));
        Self { ctrl, task_table, manager, task, server }
    }

    /// Go `expectRuntimeAcquiredAndReleased`.
    fn expect_runtime_acquired_and_released(&self) -> Arc<MockRuntimeHandle> {
        let runtime_handle = Arc::new(MockRuntimeHandle { keyspace: self.task.base.keyspace.clone(), released: Default::default() });
        self.server.acquire.lock().unwrap().push_back(Ok(Arc::clone(&runtime_handle)));
        runtime_handle
    }

    /// Go `holderID`.
    fn holder_id(&self) -> String {
        tidb_dxf_dxfutil::gen_holder_id("executor", self.task.base.id)
    }

    fn assert_acquired(&self) {
        assert_eq!(*self.server.acquired.lock().unwrap(), vec![(self.task.base.keyspace.clone(), self.holder_id())]);
    }
}

fn same_runtime(param: &crate::Param, handle: &Arc<MockRuntimeHandle>) -> bool {
    let runtime = param.task_runtime.as_ref().expect("the manager hands the task runtime");
    std::ptr::addr_eq(Arc::as_ptr(runtime), Arc::as_ptr(handle))
}

fn released(handle: &MockRuntimeHandle) -> usize {
    handle.released.load(Ordering::SeqCst)
}

/// Go `TestStartTaskExecutorCrossKeyspaceRuntime`.
#[test]
fn test_start_task_executor_cross_keyspace_runtime() {
    // acquires runtime and releases it when executor exits
    {
        let tc = CrossKeyspaceStartCase::new(201, "cross-ks-executor");
        let runtime_handle = tc.expect_runtime_acquired_and_released();

        let executor = MockTaskExecutor::new(&tc.ctrl, "executor");
        let (run_tx, run_rx) = crossbeam_channel::bounded::<()>(0);
        {
            let (task_id, runtime_handle, executor) = (tc.task.base.id, Arc::clone(&runtime_handle), Arc::clone(&executor));
            register_task_type(
                TASK_TYPE_EXAMPLE,
                Arc::new(move |_, got_task, param| {
                    assert_eq!(got_task.base.id, task_id);
                    assert!(same_runtime(&param, &runtime_handle));
                    assert_eq!(param.task_runtime.as_ref().unwrap().store_keyspace(), "user_ks");
                    Arc::clone(&executor) as Arc<dyn TaskExecutor>
                }),
            );
        }
        tc.ctrl.ret(executor.key("Init"), Ok::<(), Error>(()));
        let run_tx = std::sync::Mutex::new(Some(run_tx));
        tc.ctrl.expect(executor.key("Run"), ONCE, move |_| drop(run_tx.lock().unwrap().take()));
        {
            let task = tc.task.base.clone();
            tc.ctrl.expect(executor.key("GetTaskBase"), Times::Any, move |_| task.clone());
        }
        tc.ctrl.ret(executor.key("Close"), ());

        assert!(tc.manager.start_task_executor(&tc.task.base));
        assert!(matches!(
            run_rx.recv_timeout(Duration::from_secs(5)),
            Err(crossbeam_channel::RecvTimeoutError::Disconnected)
        ));
        tc.manager.wait_executors();
        tc.assert_acquired();
        assert_eq!(released(&runtime_handle), 1);
        tc.ctrl.finish();
    }

    // releases runtime when executor initialization fails
    {
        let tc = CrossKeyspaceStartCase::new(202, "cross-ks-executor-init-fail");
        let runtime_handle = tc.expect_runtime_acquired_and_released();

        let executor = MockTaskExecutor::new(&tc.ctrl, "executor");
        let init_err = plain("init failed");
        {
            let (task_id, runtime_handle, executor) = (tc.task.base.id, Arc::clone(&runtime_handle), Arc::clone(&executor));
            register_task_type(
                TASK_TYPE_EXAMPLE,
                Arc::new(move |_, got_task, param| {
                    assert_eq!(got_task.base.id, task_id);
                    assert!(same_runtime(&param, &runtime_handle));
                    assert_eq!(param.task_runtime.as_ref().unwrap().store_keyspace(), "user_ks");
                    Arc::clone(&executor) as Arc<dyn TaskExecutor>
                }),
            );
        }
        tc.ctrl.ret(executor.key("Init"), Err::<(), Error>(init_err));
        tc.ctrl.ret(format!("{}|init failed", executor.key("IsRetryableError")), false);
        tc.ctrl.ret(key::fail_subtask("exec-1", tc.task.base.id, "init failed"), StorageResult::Ok(()));

        assert!(!tc.manager.start_task_executor(&tc.task.base));
        tc.assert_acquired();
        assert_eq!(released(&runtime_handle), 1);
        tc.ctrl.finish();
    }

    // stops when runtime acquisition fails
    {
        let tc = CrossKeyspaceStartCase::new(203, "cross-ks-executor-acquire-fail");
        tc.server.acquire.lock().unwrap().push_back(Err("acquire failed".to_owned()));
        let factory_called = Arc::new(AtomicBool::new(false));
        {
            let factory_called = Arc::clone(&factory_called);
            register_task_type(
                TASK_TYPE_EXAMPLE,
                Arc::new(move |_, _, _| {
                    factory_called.store(true, Ordering::SeqCst);
                    panic!("task executor factory should not be called when runtime acquisition fails");
                }),
            );
        }

        assert!(!tc.manager.start_task_executor(&tc.task.base));
        tc.assert_acquired();
        assert!(!factory_called.load(Ordering::SeqCst));
        assert!(!tc.manager.is_executor_started(tc.task.base.id));
        assert_eq!(tc.manager.slot_manager().available_slots(), NODE_RESOURCE_FOR_TEST.total_cpu);
        tc.ctrl.finish();
    }

    // releases runtime when task factory is missing
    {
        let tc = CrossKeyspaceStartCase::new(204, "cross-ks-executor-missing-factory");
        let runtime_handle = tc.expect_runtime_acquired_and_released();

        tc.ctrl.ret(key::fail_subtask("exec-1", tc.task.base.id, "*"), StorageResult::Ok(()));

        assert!(!tc.manager.start_task_executor(&tc.task.base));
        tc.assert_acquired();
        assert_eq!(released(&runtime_handle), 1);
        assert!(!tc.manager.is_executor_started(tc.task.base.id));
        assert_eq!(tc.manager.slot_manager().available_slots(), NODE_RESOURCE_FOR_TEST.total_cpu);
        assert_eq!(*tc.task_table.failed_subtask_errs.lock().unwrap(), vec!["task type Example not found".to_owned()]);
        tc.ctrl.finish();
    }
    clear_task_executors();
}

fn register_executors(executors: &HashMap<i64, Arc<MockTaskExecutor>>) {
    let executors = executors.clone();
    register_task_type(
        TaskType::new("type"),
        Arc::new(move |_, task, _| Arc::clone(&executors[&task.base.id]) as Arc<dyn TaskExecutor>),
    );
}

/// Go `TestManager`.
#[test]
fn test_manager() {
    let ctrl = Arc::new(Controller::default());
    let mock_task_table = Arc::new(MockTaskTable::new(&ctrl));
    let mock_internal_executors: HashMap<i64, Arc<MockTaskExecutor>> =
        (1..=3).map(|id| (id, MockTaskExecutor::new(&ctrl, &format!("executor{id}")))).collect();
    register_executors(&mock_internal_executors);
    let id = "test";

    let m = new_manager(&Context::background(), id, &mock_task_table);
    expect_runtime_from_new_session(&mock_task_table, "");

    let task = |id, state| TaskBase { id, state, step: STEP_ONE, tp: TaskType::new("type"), ..TaskBase::default() };
    let task1 = task(1, TASK_STATE_RUNNING);
    let task2 = task(2, TASK_STATE_REVERTING);
    let task3 = task(3, TASK_STATE_PAUSING);

    ctrl.ret("InitMeta|test|", StorageResult::Ok(()));
    ctrl.ret("GetTaskExecInfoByExecID|test", StorageResult::Ok(vec![exec_info(&task1), exec_info(&task2), exec_info(&task3)]));
    ctrl.expect("GetTaskExecInfoByExecID|test", Times::Any, |_| StorageResult::Ok(Vec::<TaskExecInfo>::new()));
    // task1
    ctrl.ret(key::get_task_by_id(task1.id), task_of(&task1));
    let executor1 = &mock_internal_executors[&task1.id];
    {
        let task1 = task1.clone();
        ctrl.expect(executor1.key("GetTaskBase"), Times::N(2), move |_| task1.clone());
    }
    ctrl.ret(executor1.key("Init"), Ok::<(), Error>(()));
    ctrl.ret(executor1.key("Run"), ());
    ctrl.ret(executor1.key("Close"), ());
    // task2
    ctrl.ret(format!("CancelSubtask|{id}|{}", task2.id), StorageResult::Ok(()));
    // task3
    ctrl.expect(format!("PauseSubtasks|{id}|{}", task3.id), Times::Any, |_| StorageResult::Ok(()));

    m.init_meta().unwrap();
    m.start().unwrap();
    eventually(Duration::from_secs(5), Duration::from_millis(100), || ctrl.satisfied());
    m.stop();
    ctrl.finish();
    clear_task_executors();
}

/// Go `TestManagerHandleTasks`.
#[test]
fn test_manager_handle_tasks() {
    let ctrl = Arc::new(Controller::default());
    let mock_task_table = Arc::new(MockTaskTable::new(&ctrl));
    let mock_internal_executor = MockTaskExecutor::new(&ctrl, "executor");
    {
        let executor = Arc::clone(&mock_internal_executor);
        register_task_type(TaskType::new("type"), Arc::new(move |_, _, _| Arc::clone(&executor) as Arc<dyn TaskExecutor>));
    }
    let id = "test";

    let m = new_manager(&Context::background(), id, &mock_task_table);
    m.slot_manager().available.store(16, Ordering::SeqCst);
    expect_runtime_from_new_session(&mock_task_table, "");

    // failed to get tasks
    ctrl.expect("GetTaskExecInfoByExecID|test", ONCE, |_| StorageResult::<Vec<TaskExecInfo>>::Err(storage_err("mock err")));
    assert_eq!(m.task_executor_count(), 0);
    m.handle_tasks();
    assert_eq!(m.task_executor_count(), 0);
    assert!(ctrl.satisfied());

    // handle pausing tasks
    ctrl.ret(
        "GetTaskExecInfoByExecID|test",
        StorageResult::Ok(vec![exec_info(&TaskBase { id: 1, state: TASK_STATE_PAUSING, ..TaskBase::default() })]),
    );
    ctrl.ret("PauseSubtasks|test|1", StorageResult::Ok(()));
    m.handle_tasks();
    assert!(ctrl.satisfied());

    let (ch_tx, ch_rx) = crossbeam_channel::bounded::<()>(0);
    let mut task1 = TaskBase {
        id: 1,
        state: TASK_STATE_RUNNING,
        step: STEP_ONE,
        tp: TaskType::new("type"),
        required_slots: 1,
        ..TaskBase::default()
    };

    {
        let task1 = task1.clone();
        ctrl.expect(mock_internal_executor.key("GetTaskBase"), Times::Any, move |_| task1.clone());
    }
    // handle pending tasks
    ctrl.ret("GetTaskExecInfoByExecID|test", StorageResult::Ok(vec![exec_info(&task1)]));
    ctrl.ret(key::get_task_by_id(task1.id), task_of(&task1));
    ctrl.ret(mock_internal_executor.key("Init"), Ok::<(), Error>(()));
    ctrl.expect(mock_internal_executor.key("Run"), ONCE, move |_| {
        let _ = ch_rx.recv();
    });
    m.handle_tasks();
    eventually(Duration::from_secs(5), Duration::from_millis(100), || ctrl.satisfied());
    assert!(m.is_executor_started(task1.id));

    // handle task1 again, no effects
    ctrl.ret("GetTaskExecInfoByExecID|test", StorageResult::Ok(vec![exec_info(&task1)]));
    m.handle_tasks();
    assert!(ctrl.satisfied());

    // task1 changed to reverting, executor will keep running, but context canceled
    task1.state = TASK_STATE_REVERTING;
    ctrl.ret("GetTaskExecInfoByExecID|test", StorageResult::Ok(vec![exec_info(&task1)]));
    ctrl.ret(mock_internal_executor.key("CancelRunningSubtask"), ());
    ctrl.ret(format!("CancelSubtask|test|{}", task1.id), StorageResult::Ok(()));
    m.handle_tasks();
    assert!(ctrl.satisfied());
    assert!(m.is_executor_started(task1.id));

    // finish task1, executor will be closed
    ctrl.ret(mock_internal_executor.key("Close"), ());
    ch_tx.send(()).unwrap();
    eventually(Duration::from_secs(5), Duration::from_millis(100), || ctrl.satisfied() && !m.is_executor_started(task1.id));

    m.wait_executors();
    ctrl.finish();
    clear_task_executors();
}

/// Go `TestSlotManagerInManager`.
#[test]
fn test_slot_manager_in_manager() {
    let ctrl = Arc::new(Controller::default());
    let mock_task_table = Arc::new(MockTaskTable::new(&ctrl));
    let mock_internal_executors: HashMap<i64, Arc<MockTaskExecutor>> =
        (1..=3).map(|id| (id, MockTaskExecutor::new(&ctrl, &format!("executor{id}")))).collect();
    register_executors(&mock_internal_executors);
    let id = "test";

    let m = new_manager(&Context::background(), id, &mock_task_table);
    m.slot_manager().available.store(10, Ordering::SeqCst);
    expect_runtime_from_new_session(&mock_task_table, "");

    let task = |id, required_slots, priority| TaskBase {
        id,
        state: TASK_STATE_RUNNING,
        required_slots,
        priority,
        step: STEP_ONE,
        tp: TaskType::new("type"),
        ..TaskBase::default()
    };
    let mut task1 = task(1, 10, 0);
    let task2 = task(2, 1, 0);
    let task3 = task(3, 1, -1);
    let executor = |id: i64| Arc::clone(&mock_internal_executors[&id]);
    for task in [&task1, &task2, &task3] {
        let base = task.clone();
        ctrl.expect(executor(task.id).key("GetTaskBase"), Times::Any, move |_| base.clone());
    }

    // init error, allocated slot will be released
    ctrl.ret(key::get_task_by_id(task1.id), task_of(&task1));
    ctrl.ret(executor(1).key("Init"), Err::<(), Error>(plain("some error")));
    ctrl.ret(format!("{}|*", executor(1).key("IsRetryableError")), true);
    m.handle_executable_tasks(&[exec_info(&task1)]);
    assert!(ctrl.satisfied());
    assert!(!m.is_executor_started(task1.id));
    assert_eq!(m.slot_manager().available_slots(), 10);
    assert!(m.slot_manager().executor_tasks().is_empty());

    let (ch_tx, ch_rx) = crossbeam_channel::bounded::<()>(0);
    let expect_run = |id: i64| {
        let ch_rx = ch_rx.clone();
        ctrl.expect(executor(id).key("Run"), ONCE, move |_| {
            let _ = ch_rx.recv();
        });
    };

    // ******** Test task1 alloc success ********
    // 1. task1 alloc success
    // 2. task2 alloc failed
    // 3. task1 run success

    // mock inside startTaskExecutor
    ctrl.ret(executor(1).key("Init"), Ok::<(), Error>(()));
    // task1 start running
    ctrl.ret(key::get_task_by_id(task1.id), task_of(&task1));
    expect_run(1);

    m.handle_executable_tasks(&[exec_info(&task1), exec_info(&task2)]);
    // task1 alloc resource success
    eventually(Duration::from_secs(2), Duration::from_millis(300), || ctrl.satisfied());
    assert!(m.is_executor_started(task1.id));
    assert_eq!(m.slot_manager().available_slots(), 0);
    assert_eq!(m.slot_manager().executor_tasks().len(), 1);
    // task1 succeed
    ctrl.ret(executor(1).key("Close"), ());
    ch_tx.send(()).unwrap();
    m.wait_executors();
    assert_eq!(m.slot_manager().available_slots(), 10);
    assert!(!m.is_executor_started(task1.id));
    assert!(m.slot_manager().executor_tasks().is_empty());
    assert!(ctrl.satisfied());

    // ******** Test task preemption ********
    // task1 start running
    ctrl.ret(key::get_task_by_id(task1.id), task_of(&task1));
    ctrl.ret(executor(1).key("Init"), Ok::<(), Error>(()));
    expect_run(1);

    m.handle_executable_tasks(&[exec_info(&task1), exec_info(&task2)]);
    // task1 alloc resource success
    eventually(Duration::from_secs(2), Duration::from_millis(300), || ctrl.satisfied());
    assert!(m.is_executor_started(task1.id));
    assert_eq!(m.slot_manager().available_slots(), 0);
    assert_eq!(m.slot_manager().executor_tasks().len(), 1);

    // 2. task1 is preempted by task3, task1 start to pausing
    // 3. task3 is waiting for task1 to be released, and task2 can't be allocated
    // the priority of task3 is higher than task2, so task3 is in front of task2
    ctrl.ret(executor(1).key("Cancel"), ());
    m.handle_executable_tasks(&[exec_info(&task3), exec_info(&task2)]);
    assert!(ctrl.satisfied());
    assert_eq!(m.slot_manager().available_slots(), 0);
    assert_eq!(m.slot_manager().executor_tasks().len(), 1);
    assert!(m.is_executor_started(task1.id));

    // 4. task1 is released, task3 alloc success, start to run
    ctrl.ret(executor(1).key("Close"), ());
    ch_tx.send(()).unwrap();
    m.wait_executors();
    assert_eq!(m.slot_manager().available_slots(), 10);
    assert!(m.slot_manager().executor_tasks().is_empty());
    assert!(!m.is_executor_started(task1.id));
    assert!(ctrl.satisfied());

    // 5. available is enough, task3/task2 alloc success,
    ctrl.ret(key::get_task_by_id(task2.id), task_of(&task2));
    ctrl.ret(executor(2).key("Init"), Ok::<(), Error>(()));
    expect_run(2);
    ctrl.ret(key::get_task_by_id(task3.id), task_of(&task3));
    ctrl.ret(executor(3).key("Init"), Ok::<(), Error>(()));
    expect_run(3);

    m.handle_executable_tasks(&[exec_info(&task3), exec_info(&task1), exec_info(&task2)]);
    eventually(Duration::from_secs(2), Duration::from_millis(300), || ctrl.satisfied());
    assert_eq!(m.slot_manager().available_slots(), 8);
    assert_eq!(m.slot_manager().executor_tasks().len(), 2);
    assert!(m.is_executor_started(task2.id));
    assert!(m.is_executor_started(task3.id));

    // 6. task3/task2 run success
    ctrl.ret(executor(2).key("Close"), ());
    ctrl.ret(executor(3).key("Close"), ());
    ch_tx.send(()).unwrap();
    ch_tx.send(()).unwrap();
    m.wait_executors();
    assert_eq!(m.slot_manager().available_slots(), 10);
    assert!(m.slot_manager().executor_tasks().is_empty());
    assert!(ctrl.satisfied());

    // task rank: task3(1), task1(4), task2(1)
    task1.required_slots = 4;
    // task3 exchange to 8 slots, task1 cannot start, and we will skip task2 too.
    ctrl.ret(key::get_task_by_id(task3.id), task_of(&task3));
    ctrl.ret(executor(3).key("Init"), Ok::<(), Error>(()));
    expect_run(3);
    ctrl.ret(key::get_task_by_id(task1.id), task_of(&task1));
    {
        let (m, task1_id, task3) = (Arc::clone(&m), task1.id, task3.clone());
        *BEFORE_CALL_START_TASK_EXECUTOR.write().unwrap() = Some(Arc::new(move |task: &TaskBase| {
            if task.id == task1_id {
                let new_task3 = TaskBase { required_slots: 8, ..task3.clone() };
                assert!(m.slot_manager().exchange(&new_task3));
                assert_eq!(m.slot_manager().available_slots(), 2);
            }
        }));
    }
    m.handle_executable_tasks(&[exec_info(&task3), exec_info(&task1), exec_info(&task2)]);
    *BEFORE_CALL_START_TASK_EXECUTOR.write().unwrap() = None;
    eventually(Duration::from_secs(2), Duration::from_millis(300), || ctrl.satisfied());
    assert_eq!(m.slot_manager().available_slots(), 2);
    assert_eq!(m.slot_manager().executor_tasks().len(), 1);
    assert_eq!(m.slot_manager().executor_tasks()[0].required_slots, 8);
    assert!(m.is_executor_started(task3.id));
    // finish
    ctrl.ret(executor(3).key("Close"), ());
    ch_tx.send(()).unwrap();
    m.wait_executors();
    assert_eq!(m.slot_manager().available_slots(), 10);
    assert!(m.slot_manager().executor_tasks().is_empty());
    assert!(ctrl.satisfied());
    ctrl.finish();
    clear_task_executors();
}

/// Go `TestStartTaskExecutorResolveTaskRuntimeFromTaskKeyspace`.
#[test]
fn test_start_task_executor_resolve_task_runtime_from_task_keyspace() {
    let ctrl = Arc::new(Controller::default());

    const INSTANCE_KS: &str = "instance_ks";
    const TASK_KS: &str = "task_ks";
    let task = Task {
        base: TaskBase {
            id: 1,
            keyspace: TASK_KS.to_owned(),
            tp: TaskType::new("resolve-store"),
            step: STEP_ONE,
            state: TASK_STATE_RUNNING,
            required_slots: 1,
            ..TaskBase::default()
        },
        ..Task::default()
    };
    let mock_task_table = Arc::new(MockTaskTable::new(&ctrl));
    let m = new_manager(&Context::background(), "test", &mock_task_table);

    let runtime_handle = Arc::new(MockRuntimeHandle { keyspace: TASK_KS.to_owned(), released: Default::default() });
    let server = Arc::new(MockServer::default());
    server.acquire.lock().unwrap().push_back(Ok(Arc::clone(&runtime_handle)));

    let mock_executor = MockTaskExecutor::new(&ctrl, "executor");
    let got_store = Arc::new(std::sync::Mutex::new(None::<String>));
    {
        let (mock_executor, got_store) = (Arc::clone(&mock_executor), Arc::clone(&got_store));
        register_task_type(
            task.base.tp.clone(),
            Arc::new(move |_, _, param| {
                *got_store.lock().unwrap() = Some(param.task_runtime.as_ref().unwrap().store_keyspace());
                Arc::clone(&mock_executor) as Arc<dyn TaskExecutor>
            }),
        );
    }
    ctrl.ret(key::get_task_by_id(task.base.id), StorageResult::Ok(task.clone()));
    let server_dyn: Arc<dyn tidb_domain_sqlsvrapi::Server<dyn tidb_dxf_storage::SessionContext>> = server.clone();
    mock_task_table.set_session(Arc::new(MockSession { keyspace: INSTANCE_KS.to_owned(), server: Some(server_dyn) }));
    ctrl.ret(mock_executor.key("Init"), Ok::<(), Error>(()));
    let (run_tx, run_rx) = crossbeam_channel::bounded::<()>(0);
    {
        let task = task.base.clone();
        ctrl.expect(mock_executor.key("GetTaskBase"), Times::Any, move |_| task.clone());
    }
    ctrl.expect(mock_executor.key("Run"), ONCE, move |_| {
        let _ = run_rx.recv();
    });
    ctrl.ret(mock_executor.key("Close"), ());

    assert!(m.start_task_executor(&task.base));
    assert_eq!(got_store.lock().unwrap().as_deref(), Some(TASK_KS));
    assert_eq!(*server.acquired.lock().unwrap(), vec![(TASK_KS.to_owned(), tidb_dxf_dxfutil::gen_holder_id("executor", task.base.id))]);
    drop(run_tx);
    m.wait_executors();
    assert_eq!(released(&runtime_handle), 1);
    ctrl.finish();
    clear_task_executors();
}

/// Go `TestStartTaskExecutorResolveTaskRuntimeError`.
#[test]
fn test_start_task_executor_resolve_task_runtime_error() {
    let ctrl = Arc::new(Controller::default());

    const INSTANCE_KS: &str = "instance_ks";
    const TASK_KS: &str = "task_ks";
    let task = Task {
        base: TaskBase {
            id: 2,
            keyspace: TASK_KS.to_owned(),
            tp: TaskType::new("resolve-store-error"),
            step: STEP_ONE,
            state: TASK_STATE_RUNNING,
            required_slots: 1,
            ..TaskBase::default()
        },
        ..Task::default()
    };
    let mock_task_table = Arc::new(MockTaskTable::new(&ctrl));
    let m = new_manager(&Context::background(), "test", &mock_task_table);

    let server = Arc::new(MockServer::default());
    server.acquire.lock().unwrap().push_back(Err("ks runtime not found".to_owned()));
    let factory_called = Arc::new(AtomicBool::new(false));
    {
        let factory_called = Arc::clone(&factory_called);
        register_task_type(
            task.base.tp.clone(),
            Arc::new(move |_, _, _| {
                factory_called.store(true, Ordering::SeqCst);
                panic!("the factory is not called");
            }),
        );
    }

    ctrl.ret(key::get_task_by_id(task.base.id), StorageResult::Ok(task.clone()));
    let server_dyn: Arc<dyn tidb_domain_sqlsvrapi::Server<dyn tidb_dxf_storage::SessionContext>> = server.clone();
    mock_task_table.set_session(Arc::new(MockSession { keyspace: INSTANCE_KS.to_owned(), server: Some(server_dyn) }));

    assert!(!m.start_task_executor(&task.base));
    assert_eq!(*server.acquired.lock().unwrap(), vec![(TASK_KS.to_owned(), tidb_dxf_dxfutil::gen_holder_id("executor", task.base.id))]);
    assert!(!factory_called.load(Ordering::SeqCst));
    assert!(!m.is_executor_started(task.base.id));
    assert_eq!(m.slot_manager().available_slots(), NODE_RESOURCE_FOR_TEST.total_cpu);
    ctrl.finish();
    clear_task_executors();
}

/// Go `TestManagerInitMeta`.
#[test]
fn test_manager_init_meta() {
    let ctrl = Arc::new(Controller::default());
    let mock_task_table = Arc::new(MockTaskTable::new(&ctrl));
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let m = new_manager(&ctx, "", &mock_task_table);
    ctrl.ret("InitMeta|*|*", StorageResult::Ok(()));
    m.init_meta().unwrap();
    assert!(ctrl.satisfied());
    ctrl.expect("InitMeta|*|*", ONCE, |_| StorageResult::<()>::Err(storage_err("mock err")));
    ctrl.ret("InitMeta|*|*", StorageResult::Ok(()));
    m.init_meta().unwrap();
    assert!(ctrl.satisfied());

    // reduceRetrySQLTimes(t, 1)
    let retry_cnt_bak = tidb_dxf_scheduler::RETRY_SQL_TIMES.swap(1, Ordering::SeqCst);
    ctrl.expect("InitMeta|*|*", ONCE, |_| StorageResult::<()>::Err(storage_err("mock err")));
    assert!(m.init_meta().unwrap_err().to_string().contains("mock err"));
    assert!(ctrl.satisfied());

    cancel.cancel();
    ctrl.expect("InitMeta|*|*", ONCE, |_| StorageResult::<()>::Err(storage_err("mock err")));
    assert!(ctx_canceled(&m.init_meta().unwrap_err()));
    assert!(ctrl.satisfied());
    tidb_dxf_scheduler::RETRY_SQL_TIMES.store(retry_cnt_bak, Ordering::SeqCst);
    ctrl.finish();
}
