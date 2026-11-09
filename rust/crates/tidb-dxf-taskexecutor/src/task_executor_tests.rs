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

//! Go `task_executor_test.go` and `main_test.go`'s `ReduceCheckInterval`.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tidb_dxf::{
    Allocatable, Step, StepResource, Subtask, SubtaskBase, SubtaskState, Task, TaskBase, TaskType, NODE_RESOURCE_FOR_TEST,
    STEP_ONE, STEP_THREE, STEP_TWO, SUBTASK_STATE_CANCELED, SUBTASK_STATE_FAILED, SUBTASK_STATE_PENDING,
    SUBTASK_STATE_RUNNING, TASK_STATE_MODIFYING, TASK_STATE_REVERTING, TASK_STATE_RUNNING, TASK_STATE_SUCCEED,
    TASK_TYPE_EXAMPLE,
};
use tidb_dxf_execute::{StepExecFrameworkInfo, StepExecutor};
use tidb_dxf_storage::StorageError;
use tidb_gocontext::{Context, CtxError};

use crate::manager::{MAX_SUBTASK_CHECK_INTERVAL, SUBTASK_CHECK_INTERVAL, TASK_CHECK_INTERVAL};
use crate::mock::{
    key, new_session_pool_for_store, storage_err, Controller, MockExtension, MockRuntime, MockStepExecutor, MockTaskTable,
    Times, ONCE,
};
use crate::task_executor::{plain, CHECK_BALANCE_SUBTASK_INTERVAL, DETECT_PARAM_MODIFY_INTERVAL};
use crate::{BaseStepExecutor, BaseTaskExecutor, Error, Extension, Param, SlotManager, TaskExecutor, TaskExecutorError};

type StorageResult<T> = Result<T, StorageError>;

/// Go `unfinishedNormalSubtaskStates`.
const UNFINISHED_NORMAL_SUBTASK_STATES: [SubtaskState; 2] = [SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING];

fn set(duration: &std::sync::RwLock<Duration>, value: Duration) {
    *duration.write().unwrap() = value;
}

/// Go `ReduceCheckInterval`.
fn reduce_check_interval() {
    for interval in [&TASK_CHECK_INTERVAL, &MAX_SUBTASK_CHECK_INTERVAL, &SUBTASK_CHECK_INTERVAL] {
        set(interval, Duration::from_millis(1));
    }
}

/// Go `reduceRetrySQLTimes`.
fn reduce_retry_sql_times(target: usize) {
    tidb_dxf_scheduler::RETRY_SQL_TIMES.store(target, Ordering::SeqCst);
}

/// Go `setDetectParamModifyInterval`.
fn set_detect_param_modify_interval(new_interval: Duration) {
    set(&DETECT_PARAM_MODIFY_INTERVAL, new_interval);
}

fn subtask(id: i64, step: Step, state: SubtaskState) -> Subtask {
    Subtask {
        base: SubtaskBase { id, tp: TASK_TYPE_EXAMPLE, step, state, exec_id: "id".to_owned(), ..SubtaskBase::default() },
        ..Subtask::default()
    }
}

/// Go `taskExecutorRunEnv`.
struct TaskExecutorRunEnv {
    ctrl: Arc<Controller>,
    task_table: Arc<MockTaskTable>,
    step_executor: Arc<MockStepExecutor>,
    /// The executor holds its extension weakly.
    _task_exec_ext: Arc<dyn Extension>,
    task_executor: Arc<BaseTaskExecutor>,
    task1: Task,
    succeed_task1: Task,
    reverting_task1: Task,
    pending_subtask1: Subtask,
    running_subtask2: Subtask,
}

/// Go `newTaskExecutorRunEnv`.
fn new_task_executor_run_env() -> TaskExecutorRunEnv {
    let env = new_task_executor_run_env0();
    // DetectParamModify loop is harder to mock for Run, so we prevent it from running.
    set_detect_param_modify_interval(Duration::from_secs(3600));
    env
}

/// Go `newTaskExecutorRunEnv0`.
fn new_task_executor_run_env0() -> TaskExecutorRunEnv {
    let ctrl = Arc::new(Controller::default());
    let task_table = Arc::new(MockTaskTable::new(&ctrl));
    let step_executor = MockStepExecutor::new(&ctrl);
    let task_exec_ext: Arc<dyn Extension> = MockExtension::new(&ctrl);

    let task1 = Task {
        base: TaskBase {
            state: TASK_STATE_RUNNING,
            step: STEP_ONE,
            tp: TASK_TYPE_EXAMPLE,
            id: 1,
            required_slots: 10,
            ..TaskBase::default()
        },
        ..Task::default()
    };
    let task_executor = Arc::new(BaseTaskExecutor::new(
        &Context::background(),
        task1.clone(),
        Param::new_for_test(
            Arc::clone(&task_table) as Arc<dyn crate::TaskTable>,
            Arc::new(SlotManager::new(16)),
            NODE_RESOURCE_FOR_TEST.clone(),
            "id",
        ),
    ));
    task_executor.set_extension(Arc::downgrade(&task_exec_ext));

    reduce_check_interval();

    let mut succeed_task1 = task1.clone();
    succeed_task1.base.state = TASK_STATE_SUCCEED;
    let mut reverting_task1 = task1.clone();
    reverting_task1.base.state = TASK_STATE_REVERTING;
    TaskExecutorRunEnv {
        ctrl,
        task_table,
        step_executor,
        _task_exec_ext: task_exec_ext,
        task_executor,
        task1,
        succeed_task1,
        reverting_task1,
        pending_subtask1: subtask(1, STEP_ONE, SUBTASK_STATE_PENDING),
        running_subtask2: subtask(2, STEP_ONE, SUBTASK_STATE_RUNNING),
    }
}

impl TaskExecutorRunEnv {
    fn expect_get_task(&self, task: &Task) {
        let task = task.clone();
        self.ctrl.expect(key::get_task_by_id(self.task1.base.id), ONCE, move |_| StorageResult::Ok(task.clone()));
    }

    fn expect_get_task_times(&self, task: &Task, times: usize) {
        let task = task.clone();
        self.ctrl.expect(key::get_task_by_id(self.task1.base.id), Times::N(times), move |_| StorageResult::Ok(task.clone()));
    }

    fn expect_get_task_err(&self, err: StorageError) {
        self.ctrl.expect(key::get_task_by_id(self.task1.base.id), ONCE, move |_| StorageResult::<Task>::Err(err.clone()));
    }

    fn first_subtask_key(&self, step: Step) -> String {
        key::get_first_subtask_in_states("id", self.task1.base.id, step, &UNFINISHED_NORMAL_SUBTASK_STATES)
    }

    fn expect_first_subtask(&self, subtask: Option<&Subtask>) {
        self.expect_first_subtask_of_step(STEP_ONE, subtask);
    }

    fn expect_first_subtask_of_step(&self, step: Step, subtask: Option<&Subtask>) {
        let subtask = subtask.cloned();
        self.ctrl.expect(self.first_subtask_key(step), ONCE, move |_| StorageResult::Ok(subtask.clone()));
    }

    fn expect_get_step_executor(&self) {
        let step_executor: Arc<dyn StepExecutor> = self.step_executor.clone();
        self.ctrl.expect("ext.GetStepExecutor", ONCE, move |_| Ok::<_, Error>(Arc::clone(&step_executor)));
    }

    fn expect_init(&self) {
        self.ctrl.ret("step.Init", Ok::<(), String>(()));
    }

    fn expect_get_step(&self, step: Step) {
        self.ctrl.ret("step.GetStep", step);
    }

    fn expect_start_subtask(&self, subtask_id: i64) {
        self.ctrl.ret(key::start_subtask(subtask_id, "id"), StorageResult::Ok(()));
    }

    fn expect_run_subtask(&self, subtask_id: i64, result: Result<(), Error>) {
        self.ctrl.expect(format!("step.RunSubtask|{subtask_id}"), ONCE, move |_| result.clone());
    }

    fn expect_finish_subtask(&self, subtask_id: i64) {
        self.ctrl.ret(key::finish_subtask("id", subtask_id), StorageResult::Ok(()));
    }

    fn expect_cleanup(&self, result: Result<(), String>) {
        self.ctrl.ret("step.Cleanup", result);
    }

    /// Go `mockForCheckBalanceSubtask`.
    fn mock_for_check_balance_subtask(&self) {
        let task_executor = Arc::clone(&self.task_executor);
        self.ctrl.expect(
            key::get_subtasks_by_exec_id_and_step_and_states("id", self.task1.base.id, STEP_ONE, &[SUBTASK_STATE_RUNNING]),
            Times::Any,
            move |_| {
                let id = task_executor.curr_subtask_id.load(Ordering::SeqCst);
                StorageResult::Ok(vec![Subtask { base: SubtaskBase { id, ..SubtaskBase::default() }, ..Subtask::default() }])
            },
        );
    }

    fn run(&self) {
        self.task_executor.run();
    }

    fn finish(&self) {
        assert!(self.ctrl.satisfied());
        self.ctrl.finish();
    }

    fn slot_mgr(&self) -> &Arc<SlotManager> {
        &self.task_executor.param.slot_mgr
    }
}

fn assert_same_task(left: &Task, right: &Task) {
    assert_eq!(left.base, right.base);
    assert_eq!(left.meta, right.meta);
}

/// Go `TestBaseTaskExecutorInitChecksTaskRuntime`.
#[test]
fn test_base_task_executor_init_checks_task_runtime() {
    let ctrl = Arc::new(Controller::default());
    let task = Task {
        base: TaskBase { id: 1, tp: TASK_TYPE_EXAMPLE, keyspace: "task_ks".to_owned(), ..TaskBase::default() },
        ..Task::default()
    };
    let new_executor = |runtime: MockRuntime| {
        let mut param = Param::new_for_test(
            Arc::new(MockTaskTable::new(&ctrl)),
            Arc::new(SlotManager::new(0)),
            NODE_RESOURCE_FOR_TEST.clone(),
            "",
        );
        param.task_runtime = Some(Arc::new(runtime));
        BaseTaskExecutor::new(&Context::background(), task.clone(), param)
    };

    let task_executor = new_executor(MockRuntime { keyspace: "task_ks".to_owned(), se_pool: Some(new_session_pool_for_store("task_ks")) });
    TaskExecutor::init(&task_executor, &Context::background()).unwrap();

    let task_executor = new_executor(MockRuntime { keyspace: "other_ks".to_owned(), se_pool: None });
    let err = TaskExecutor::init(&task_executor, &Context::background()).unwrap_err();
    assert!(err.to_string().contains("store keyspace mismatch with task"), "{err}");

    let task_executor =
        new_executor(MockRuntime { keyspace: "task_ks".to_owned(), se_pool: Some(new_session_pool_for_store("session_ks")) });
    let err = TaskExecutor::init(&task_executor, &Context::background()).unwrap_err();
    assert!(err.to_string().contains("invalid task runtime with mismatched keyspace"), "{err}");
}

/// Go `TestTaskExecutorRun`.
#[test]
fn test_task_executor_run() {
    // context done when run
    {
        let e = new_task_executor_run_env();
        TaskExecutor::cancel(e.task_executor.as_ref());
        e.run();
        e.finish();
    }

    // task not found when run
    {
        let e = new_task_executor_run_env();
        e.expect_get_task_err(StorageError::TaskNotFound);
        e.run();
        e.finish();
    }

    // task state is not running when run
    {
        let e = new_task_executor_run_env();
        e.expect_get_task(&e.succeed_task1);
        e.run();
        assert!(e.ctrl.satisfied());

        e.expect_get_task(&e.reverting_task1);
        e.run();
        e.finish();
    }

    // task state become 'modifying' when run, keeps running
    {
        let e = new_task_executor_run_env();
        let mut modifying_task1 = e.task1.clone();
        modifying_task1.base.state = TASK_STATE_MODIFYING;
        e.expect_get_task(&modifying_task1);
        e.expect_first_subtask(None);
        e.expect_get_task(&e.succeed_task1);
        e.run();
        e.finish();
    }

    // retry on error of GetTaskByID
    {
        let e = new_task_executor_run_env();
        e.expect_get_task_err(storage_err("some err"));
        e.expect_get_task_err(storage_err("some err"));
        e.expect_get_task(&e.succeed_task1);
        e.run();
        e.finish();
    }

    // retry on error of GetFirstSubtaskInStates
    {
        let e = new_task_executor_run_env();
        for _ in 0..3 {
            e.expect_get_task(&e.task1);
            e.ctrl.expect(e.first_subtask_key(STEP_ONE), ONCE, |_| StorageResult::<Option<Subtask>>::Err(storage_err("some err")));
        }
        e.expect_get_task(&e.succeed_task1);
        e.run();
        e.finish();
    }

    // get step executor failed
    {
        let e = new_task_executor_run_env();
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        let task_executor_register_err = plain("constructor of taskExecutor for key not found");
        e.ctrl.ret("ext.GetStepExecutor", Err::<Arc<dyn StepExecutor>, Error>(task_executor_register_err));
        e.ctrl.ret(key::fail_subtask("id", e.task1.base.id, "constructor of taskExecutor for key not found"), StorageResult::Ok(()));
        // used to break the loop, below too
        e.expect_get_task(&e.succeed_task1);
        e.run();
        e.finish();
    }

    // non retryable step executor Init error
    {
        let e = new_task_executor_run_env();
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.ctrl.ret("step.Init", Err::<(), String>("init error".to_owned()));
        e.ctrl.ret("ext.IsRetryableError|*", false);
        e.ctrl.ret(key::fail_subtask("id", e.task1.base.id, "init error"), StorageResult::Ok(()));
        e.expect_get_task(&e.succeed_task1);
        e.run();
        e.finish();
    }

    // retryable step executor Init error
    {
        let e = new_task_executor_run_env();
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.ctrl.ret("step.Init", Err::<(), String>("init error".to_owned()));
        e.ctrl.ret("ext.IsRetryableError|*", true);
        e.expect_get_task(&e.succeed_task1);
        e.run();
        e.finish();
    }

    // run one subtask failed with non-retryable error
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();

        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        e.expect_run_subtask(1, Err(plain("run subtask error")));
        e.ctrl.ret("ext.IsRetryableError|*", false);
        e.ctrl.ret(
            key::update_subtask_state_and_error("id", e.task1.base.id, &SUBTASK_STATE_FAILED, "*"),
            StorageResult::Ok(()),
        );
        e.expect_get_task(&e.succeed_task1);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // run subtask panic, fail the entire task
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();

        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        e.ctrl.expect("step.RunSubtask|1", ONCE, |_| -> Result<(), Error> { panic!("run subtask panic") });
        e.ctrl.ret(key::fail_subtask("id", e.task1.base.id, "*"), StorageResult::Ok(()));
        e.expect_cleanup(Ok(()));
        e.run();
        assert!(e.task_table.failed_subtask_errs.lock().unwrap()[0].contains("run subtask panic"));
        e.finish();
    }

    // run one subtask failed with retryable error, success after retry 3 times
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();

        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        e.expect_run_subtask(1, Err(plain("run subtask error")));
        e.ctrl.ret("ext.IsRetryableError|*", true);
        // already started by prev StartSubtask
        let mut running_subtask = e.pending_subtask1.clone();
        running_subtask.base.state = SUBTASK_STATE_RUNNING;
        for i in 0..3 {
            e.expect_get_task(&e.task1);
            e.expect_first_subtask(Some(&running_subtask));
            e.expect_get_step(STEP_ONE);
            e.ctrl.ret("ext.IsIdempotent|*", true);
            if i < 2 {
                e.expect_run_subtask(1, Err(plain("run subtask error")));
                e.ctrl.ret("ext.IsRetryableError|*", true);
            } else {
                e.expect_run_subtask(1, Ok(()));
                e.expect_finish_subtask(e.pending_subtask1.base.id);
            }
        }
        e.expect_get_task(&e.succeed_task1);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // subtask scheduled away during running, keep running next subtask
    {
        let e = new_task_executor_run_env();
        // mock for checkBalanceSubtask, returns empty subtask list
        e.ctrl.ret(
            key::get_subtasks_by_exec_id_and_step_and_states("id", e.task1.base.id, STEP_ONE, &[SUBTASK_STATE_RUNNING]),
            StorageResult::Ok(Vec::<Subtask>::new()),
        );
        // this subtask is scheduled away during running
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        e.ctrl.expect("step.RunSubtask|1", ONCE, |input| -> Result<(), Error> {
            let ctx = input.downcast_ref::<Context>().unwrap();
            let _ = ctx.done().recv();
            Err(Arc::new(ctx.err().unwrap()))
        });
        // keep running next subtask
        let next_subtask = subtask(2, STEP_ONE, SUBTASK_STATE_PENDING);
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&next_subtask));
        e.expect_get_step(STEP_ONE);
        e.expect_start_subtask(next_subtask.base.id);
        e.expect_run_subtask(next_subtask.base.id, Ok(()));
        e.expect_finish_subtask(next_subtask.base.id);
        // exit
        e.expect_get_task(&e.succeed_task1);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // run one subtask success
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        e.expect_run_subtask(1, Ok(()));
        e.expect_finish_subtask(1);
        e.expect_get_task(&e.succeed_task1);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // run subtasks one by one, and exit due to no subtask to run for a while
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();
        for i in 0..5 {
            let subtask_id = i + 1;
            e.expect_get_task(&e.task1);
            let the_subtask = subtask(subtask_id, STEP_ONE, SUBTASK_STATE_PENDING);
            e.expect_first_subtask(Some(&the_subtask));
            if i == 0 {
                e.expect_get_step_executor();
                e.expect_init();
            } else {
                e.expect_get_step(STEP_ONE);
            }
            e.expect_start_subtask(subtask_id);
            e.expect_run_subtask(subtask_id, Ok(()));
            e.expect_finish_subtask(subtask_id);
        }
        // exit due to no subtask to run for a while
        e.expect_get_task_times(&e.task1, 8);
        e.ctrl.expect(e.first_subtask_key(STEP_ONE), Times::N(8), |_| StorageResult::Ok(None::<Subtask>));
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // run subtasks step by step
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();
        let mut id_alloc = 1;
        let mut curr_step_exec_step = Step(0);
        for (i, (step, count)) in [(STEP_ONE, 5), (STEP_TWO, 1), (STEP_THREE, 3)].into_iter().enumerate() {
            let mut task_of_step = e.task1.clone();
            task_of_step.base.step = step;
            for j in 0..count {
                let subtask_id = id_alloc;
                id_alloc += 1;
                e.expect_get_task(&task_of_step);
                let the_subtask = subtask(subtask_id, step, SUBTASK_STATE_PENDING);
                e.expect_first_subtask_of_step(step, Some(&the_subtask));
                if j == 0 {
                    if i != 0 {
                        e.expect_get_step(curr_step_exec_step);
                        e.expect_cleanup(Ok(()));
                    }
                    e.expect_get_step_executor();
                    e.expect_init();
                    curr_step_exec_step = step;
                } else {
                    e.expect_get_step(curr_step_exec_step);
                }
                e.expect_start_subtask(subtask_id);
                e.expect_run_subtask(subtask_id, Ok(()));
                e.expect_finish_subtask(subtask_id);
            }
        }
        // end the loop
        e.expect_get_task(&e.succeed_task1);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // step executor cleanup failed, keeps running
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        e.expect_run_subtask(1, Ok(()));
        e.expect_finish_subtask(1);
        e.expect_get_task(&e.task1);
        let step2_subtask = subtask(2, STEP_TWO, SUBTASK_STATE_PENDING);
        e.expect_first_subtask(Some(&step2_subtask));
        e.expect_get_step(e.pending_subtask1.base.step);
        e.expect_cleanup(Err("some error".to_owned()));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(step2_subtask.base.id);
        e.expect_run_subtask(step2_subtask.base.id, Ok(()));
        e.expect_finish_subtask(step2_subtask.base.id);
        e.expect_get_task(&e.succeed_task1);
        e.expect_cleanup(Err("some error 2".to_owned()));
        e.run();
        e.finish();
    }

    // run previous left non-idempotent subtask in running state, fail it.
    {
        let e = new_task_executor_run_env();
        let subtask_id = 2;
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.running_subtask2));
        e.expect_get_step_executor();
        e.expect_init();
        e.ctrl.ret("ext.IsIdempotent|*", false);
        e.ctrl.ret(
            key::update_subtask_state_and_error(
                "id",
                subtask_id,
                &SUBTASK_STATE_FAILED,
                &TaskExecutorError::NonIdempotentSubtask.to_string(),
            ),
            StorageResult::Ok(()),
        );
        e.expect_get_task(&e.succeed_task1);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // run previous left idempotent subtask in running state, run it again.
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();
        let subtask_id = 2;
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.running_subtask2));
        e.expect_get_step_executor();
        e.expect_init();
        // first round of the run loop
        e.ctrl.ret("ext.IsIdempotent|*", true);
        e.expect_run_subtask(subtask_id, Ok(()));
        e.expect_finish_subtask(subtask_id);
        // second round of the run loop
        e.expect_get_task(&e.succeed_task1);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // subtask cancelled during running
    {
        let e = new_task_executor_run_env();
        let (core, logs) = tidb_logutil::observer::new(tidb_log::Level::Info);
        *e.task_executor.sample_logger.write().unwrap() = core.into();
        e.mock_for_check_balance_subtask();
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        {
            let task_executor = Arc::clone(&e.task_executor);
            e.ctrl.expect("step.RunSubtask|1", ONCE, move |_| -> Result<(), Error> {
                task_executor.cancel_running_subtask();
                Err(Arc::new(TaskExecutorError::CancelSubtask))
            });
        }
        e.ctrl.ret(
            key::update_subtask_state_and_error("id", e.task1.base.id, &SUBTASK_STATE_CANCELED, "nil"),
            StorageResult::Ok(()),
        );
        e.expect_get_task_err(StorageError::TaskNotFound);
        e.expect_cleanup(Ok(()));
        e.run();
        assert!(logs.filter_message("run subtask failed").is_empty());
        assert_eq!(logs.filter_message("subtask run canceled").len(), 1);
        e.finish();
    }

    // task executor cancelled for graceful shutdown during subtask running
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        {
            let task_executor = Arc::clone(&e.task_executor);
            e.ctrl.expect("step.RunSubtask|1", ONCE, move |_| -> Result<(), Error> {
                TaskExecutor::cancel(task_executor.as_ref());
                Err(Arc::new(CtxError::Canceled))
            });
        }
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // subtask scheduled away right before we start it
    {
        let e = new_task_executor_run_env();
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.ctrl.ret(key::start_subtask(e.pending_subtask1.base.id, "id"), StorageResult::<()>::Err(StorageError::SubtaskNotFound));
        e.expect_get_task(&e.succeed_task1);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // start subtask failed after retry, will try again
    {
        let e = new_task_executor_run_env();
        let retry_cnt_bak = tidb_dxf_scheduler::retry_sql_times();
        reduce_retry_sql_times(1);
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.ctrl.expect(key::start_subtask(e.pending_subtask1.base.id, "id"), ONCE, |_| {
            StorageResult::<()>::Err(storage_err("some error"))
        });
        // second round of the run loop
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step(STEP_ONE);
        e.expect_start_subtask(e.pending_subtask1.base.id);
        e.expect_run_subtask(1, Ok(()));
        e.expect_finish_subtask(e.pending_subtask1.base.id);
        e.expect_get_task(&e.succeed_task1);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
        reduce_retry_sql_times(retry_cnt_bak);
    }

    // no subtask to run, should exit the loop after some time
    {
        let e = new_task_executor_run_env();
        e.expect_get_task_times(&e.task1, 8);
        e.ctrl.expect(e.first_subtask_key(STEP_ONE), Times::N(8), |_| StorageResult::Ok(None::<Subtask>));
        e.run();
        e.finish();
    }

    // no-subtask check counter should be reset after a subtask is run.
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();
        // loop 4 times without subtask, then 1 time with subtask.
        e.expect_get_task_times(&e.task1, 4);
        e.ctrl.expect(e.first_subtask_key(STEP_ONE), Times::N(4), |_| StorageResult::Ok(None::<Subtask>));
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        e.expect_run_subtask(1, Ok(()));
        e.expect_finish_subtask(e.pending_subtask1.base.id);
        // loop for 8 times without subtask, and exit
        e.expect_get_task_times(&e.task1, 8);
        e.ctrl.expect(e.first_subtask_key(STEP_ONE), Times::N(8), |_| StorageResult::Ok(None::<Subtask>));
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // task required slots became smaller
    {
        let e = new_task_executor_run_env();
        let mut required_slots_is4_task1 = e.task1.clone();
        required_slots_is4_task1.base.required_slots = 4;
        let mut required_slots_is2_task1 = e.task1.clone();
        required_slots_is2_task1.base.required_slots = 2;
        let slot_mgr = Arc::clone(e.slot_mgr());
        slot_mgr.alloc(&e.task1.base);
        assert_eq!(slot_mgr.available_slots(), 6);
        // without step executor, required slots modified to 4
        e.expect_get_task(&required_slots_is4_task1);
        {
            let (slot_mgr, task_executor) = (Arc::clone(&slot_mgr), Arc::clone(&e.task_executor));
            e.ctrl.expect(e.first_subtask_key(STEP_ONE), ONCE, move |_| {
                assert_eq!(slot_mgr.available_slots(), 12);
                assert_eq!(task_executor.get_task_base().required_slots, 4);
                StorageResult::Ok(None::<Subtask>)
            });
        }
        // with step executor, required slots modified to 2
        e.mock_for_check_balance_subtask();
        e.expect_get_task(&required_slots_is4_task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        {
            let step_executor = Arc::clone(&e.step_executor);
            e.ctrl.expect("step.RunSubtask|1", ONCE, move |_| -> Result<(), Error> {
                assert_eq!(step_executor.get_resource().cpu.capacity(), 4);
                Ok(())
            });
        }
        e.expect_finish_subtask(e.pending_subtask1.base.id);
        e.expect_get_task(&required_slots_is2_task1);
        {
            let (slot_mgr, task_executor, step_executor) =
                (Arc::clone(&slot_mgr), Arc::clone(&e.task_executor), Arc::clone(&e.step_executor));
            e.ctrl.expect(e.first_subtask_key(STEP_ONE), ONCE, move |_| {
                assert_eq!(slot_mgr.available_slots(), 14);
                assert_eq!(task_executor.get_task_base().required_slots, 2);
                assert_eq!(step_executor.get_resource().cpu.capacity(), 2);
                StorageResult::Ok(None::<Subtask>)
            });
        }
        e.expect_get_task_err(StorageError::TaskNotFound);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // task required slots become larger, but not enough slots for exchange
    {
        let e = new_task_executor_run_env();
        let mut required_slots_is14_task1 = e.task1.clone();
        required_slots_is14_task1.base.required_slots = 14;
        let slot_mgr = Arc::clone(e.slot_mgr());
        slot_mgr.alloc(&e.task1.base);
        // alloc for another task executor
        slot_mgr.alloc(&TaskBase { id: 2, required_slots: 4, ..TaskBase::default() });
        assert_eq!(slot_mgr.available_slots(), 2);
        // without step executor, required slots modified to 4
        e.expect_get_task(&required_slots_is14_task1);
        e.run();
        e.finish();
        assert_eq!(slot_mgr.available_slots(), 2);
    }

    // task required slots become larger, exchange success
    {
        let e = new_task_executor_run_env();
        let mut required_slots_is14_task1 = e.task1.clone();
        required_slots_is14_task1.base.required_slots = 14;
        let slot_mgr = Arc::clone(e.slot_mgr());
        slot_mgr.alloc(&e.task1.base);
        assert_eq!(slot_mgr.available_slots(), 6);
        e.mock_for_check_balance_subtask();
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        {
            let step_executor = Arc::clone(&e.step_executor);
            e.ctrl.expect("step.RunSubtask|1", ONCE, move |_| -> Result<(), Error> {
                assert_eq!(step_executor.get_resource().cpu.capacity(), 10);
                Ok(())
            });
        }
        e.expect_finish_subtask(e.pending_subtask1.base.id);
        e.expect_get_task(&required_slots_is14_task1);
        {
            let (slot_mgr, task_executor, step_executor) =
                (Arc::clone(&slot_mgr), Arc::clone(&e.task_executor), Arc::clone(&e.step_executor));
            e.ctrl.expect(e.first_subtask_key(STEP_ONE), ONCE, move |_| {
                assert_eq!(slot_mgr.available_slots(), 2);
                assert_eq!(task_executor.get_task_base().required_slots, 14);
                assert_eq!(step_executor.get_resource().cpu.capacity(), 14);
                StorageResult::Ok(None::<Subtask>)
            });
        }
        e.expect_get_task_err(StorageError::TaskNotFound);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // task meta modified, no step executor
    {
        let e = new_task_executor_run_env();
        let mut new_meta_task1 = e.task1.clone();
        new_meta_task1.meta = b"modified".to_vec();
        e.expect_get_task(&new_meta_task1);
        e.expect_first_subtask(None);
        e.expect_get_task_err(StorageError::TaskNotFound);
        e.run();
        e.finish();
    }

    // task meta/required slots modified, with step executor, same step, notify failed, will recreate step executor
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();
        let slot_mgr = Arc::clone(e.slot_mgr());
        slot_mgr.alloc(&e.task1.base);
        assert_eq!(slot_mgr.available_slots(), 6);
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        {
            let (step_executor, task_executor) = (Arc::clone(&e.step_executor), Arc::clone(&e.task_executor));
            e.ctrl.expect("step.RunSubtask|1", ONCE, move |_| -> Result<(), Error> {
                assert_eq!(step_executor.get_resource().cpu.capacity(), 10);
                assert!(task_executor.get_task().meta.is_empty());
                Ok(())
            });
        }
        e.expect_finish_subtask(e.pending_subtask1.base.id);
        // second round
        let mut new_meta_task1 = e.task1.clone();
        new_meta_task1.base.required_slots = 3;
        new_meta_task1.meta = b"modified".to_vec();
        let subtask2 = subtask(2, STEP_ONE, SUBTASK_STATE_PENDING);
        e.expect_get_task(&new_meta_task1);
        e.expect_get_step(e.task1.base.step);
        e.ctrl.ret("step.TaskMetaModified|modified", Err::<(), String>("some error".to_owned()));
        {
            let task_executor = Arc::clone(&e.task_executor);
            e.ctrl.expect("step.Cleanup", ONCE, move |_| {
                // still the old one
                assert_eq!(task_executor.get_task_base().required_slots, 10);
                assert!(task_executor.get_task().meta.is_empty());
                Ok::<(), String>(())
            });
        }
        e.expect_get_task(&new_meta_task1);
        e.expect_first_subtask(Some(&subtask2));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(subtask2.base.id);
        {
            let (slot_mgr, task_executor, step_executor) =
                (Arc::clone(&slot_mgr), Arc::clone(&e.task_executor), Arc::clone(&e.step_executor));
            e.ctrl.expect("step.RunSubtask|2", ONCE, move |_| -> Result<(), Error> {
                assert_eq!(slot_mgr.available_slots(), 13);
                assert_eq!(task_executor.get_task_base().required_slots, 3);
                assert_eq!(task_executor.get_task().meta, b"modified");
                assert_eq!(step_executor.get_resource().cpu.capacity(), 3);
                Ok(())
            });
        }
        e.expect_finish_subtask(subtask2.base.id);
        e.expect_get_task_err(StorageError::TaskNotFound);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // task meta/required slots modified, with step executor, same step, notify success
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();
        let slot_mgr = Arc::clone(e.slot_mgr());
        slot_mgr.alloc(&e.task1.base);
        assert_eq!(slot_mgr.available_slots(), 6);
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        {
            let (step_executor, task_executor) = (Arc::clone(&e.step_executor), Arc::clone(&e.task_executor));
            e.ctrl.expect("step.RunSubtask|1", ONCE, move |_| -> Result<(), Error> {
                assert_eq!(step_executor.get_resource().cpu.capacity(), 10);
                assert!(task_executor.get_task().meta.is_empty());
                Ok(())
            });
        }
        e.expect_finish_subtask(e.pending_subtask1.base.id);
        // second round
        let mut new_meta_task1 = e.task1.clone();
        new_meta_task1.base.required_slots = 3;
        new_meta_task1.meta = b"modified".to_vec();
        let subtask2 = subtask(2, STEP_ONE, SUBTASK_STATE_PENDING);
        e.expect_get_task(&new_meta_task1);
        e.expect_get_step(e.task1.base.step);
        e.ctrl.ret("step.TaskMetaModified|modified", Ok::<(), String>(()));
        e.expect_first_subtask(Some(&subtask2));
        e.expect_get_step(e.task1.base.step);
        e.expect_start_subtask(subtask2.base.id);
        {
            let (slot_mgr, task_executor, step_executor) =
                (Arc::clone(&slot_mgr), Arc::clone(&e.task_executor), Arc::clone(&e.step_executor));
            e.ctrl.expect("step.RunSubtask|2", ONCE, move |_| -> Result<(), Error> {
                assert_eq!(slot_mgr.available_slots(), 13);
                assert_eq!(task_executor.get_task_base().required_slots, 3);
                assert_eq!(task_executor.get_task().meta, b"modified");
                assert_eq!(step_executor.get_resource().cpu.capacity(), 3);
                Ok(())
            });
        }
        e.expect_finish_subtask(subtask2.base.id);
        e.expect_get_task_err(StorageError::TaskNotFound);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }

    // task meta/required slots modified, with step executor, but also switch to next step
    {
        let e = new_task_executor_run_env();
        e.mock_for_check_balance_subtask();
        let slot_mgr = Arc::clone(e.slot_mgr());
        slot_mgr.alloc(&e.task1.base);
        assert_eq!(slot_mgr.available_slots(), 6);
        e.expect_get_task(&e.task1);
        e.expect_first_subtask(Some(&e.pending_subtask1));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(e.pending_subtask1.base.id);
        {
            let (step_executor, task_executor) = (Arc::clone(&e.step_executor), Arc::clone(&e.task_executor));
            e.ctrl.expect("step.RunSubtask|1", ONCE, move |_| -> Result<(), Error> {
                assert_eq!(step_executor.get_resource().cpu.capacity(), 10);
                assert!(task_executor.get_task().meta.is_empty());
                Ok(())
            });
        }
        e.expect_finish_subtask(e.pending_subtask1.base.id);
        // second round
        let mut new_meta_task1 = e.task1.clone();
        new_meta_task1.base.required_slots = 6;
        new_meta_task1.meta = b"modified".to_vec();
        new_meta_task1.base.step = STEP_TWO;
        let step2_subtask2 = subtask(2, STEP_TWO, SUBTASK_STATE_PENDING);
        e.expect_get_task(&new_meta_task1);
        e.expect_get_step(e.task1.base.step);
        e.expect_first_subtask_of_step(STEP_TWO, Some(&step2_subtask2));
        e.expect_get_step(e.task1.base.step);
        e.expect_cleanup(Ok(()));
        e.expect_get_step_executor();
        e.expect_init();
        e.expect_start_subtask(step2_subtask2.base.id);
        {
            let (slot_mgr, task_executor, step_executor) =
                (Arc::clone(&slot_mgr), Arc::clone(&e.task_executor), Arc::clone(&e.step_executor));
            e.ctrl.expect("step.RunSubtask|2", ONCE, move |_| -> Result<(), Error> {
                assert_eq!(slot_mgr.available_slots(), 10);
                assert_eq!(task_executor.get_task_base().required_slots, 6);
                assert_eq!(task_executor.get_task().meta, b"modified");
                assert_eq!(step_executor.get_resource().cpu.capacity(), 6);
                Ok(())
            });
        }
        e.expect_finish_subtask(step2_subtask2.base.id);
        e.expect_get_task_err(StorageError::TaskNotFound);
        e.expect_cleanup(Ok(()));
        e.run();
        e.finish();
    }
}

/// Go `TestDetectAndHandleParamModify`.
#[test]
fn test_detect_and_handle_param_modify() {
    let new_test_env = |dur: Duration| {
        let env = new_task_executor_run_env0();
        set_detect_param_modify_interval(dur);
        env
    };

    // loop: break on context cancel
    {
        let env = new_test_env(Duration::from_secs(3600));
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        cancel.cancel();
        env.task_executor.detect_and_handle_param_modify_loop(&ctx);
    }

    // loop: retry on get task fail
    {
        let env = new_test_env(Duration::from_millis(1));
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        for _ in 0..3 {
            env.expect_get_task_err(storage_err("some err"));
        }
        env.ctrl.expect(key::get_task_by_id(env.task1.base.id), ONCE, move |_| {
            cancel.cancel();
            StorageResult::<Task>::Err(storage_err(&CtxError::Canceled.to_string()))
        });
        env.task_executor.detect_and_handle_param_modify_loop(&ctx);
        env.finish();
    }

    // no param modify
    {
        let env = new_task_executor_run_env0();
        env.expect_get_task(&env.task1);
        let old_task = env.task_executor.get_task();
        env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap();
        assert!(Arc::ptr_eq(&old_task, &env.task_executor.get_task()));
        env.finish();
    }

    let with_step_exec = || {
        let env = new_task_executor_run_env0();
        env.task_executor.set_step_exec(env.step_executor.clone());
        env.slot_mgr().alloc(&env.task_executor.get_task_base());
        assert_eq!(env.slot_mgr().available_slots(), 6);
        env
    };
    let alloc_another = |env: &TaskExecutorRunEnv| {
        env.slot_mgr().alloc(&TaskBase {
            state: TASK_STATE_RUNNING,
            step: STEP_ONE,
            tp: TASK_TYPE_EXAMPLE,
            id: 2,
            required_slots: 4,
            ..TaskBase::default()
        });
        assert_eq!(env.slot_mgr().available_slots(), 2);
    };
    let latest = |env: &TaskExecutorRunEnv, required_slots: Option<isize>, meta: Option<&[u8]>| {
        let mut latest_task = env.task_executor.get_task().as_ref().clone();
        if let Some(required_slots) = required_slots {
            latest_task.base.required_slots = required_slots;
        }
        if let Some(meta) = meta {
            latest_task.meta = meta.to_vec();
        }
        env.expect_get_task(&latest_task);
        latest_task
    };

    // required slots become smaller, but failed to reduce resource usage
    {
        let env = with_step_exec();
        let old_task = env.task_executor.get_task();
        latest(&env, Some(4), None);
        env.ctrl.ret("step.ResourceModified", Err::<(), String>("some error".to_owned()));
        env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap();
        assert_same_task(&old_task, &env.task_executor.get_task());
        assert_eq!(env.slot_mgr().available_slots(), 6);
        env.finish();
    }

    // required slots become smaller, apply successfully
    {
        let env = with_step_exec();
        let latest_task = latest(&env, Some(4), None);
        env.ctrl.ret("step.ResourceModified", Ok::<(), String>(()));
        env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap();
        assert_same_task(&latest_task, &env.task_executor.get_task());
        assert_eq!(env.slot_mgr().available_slots(), 12);
        env.finish();
    }

    // required slots become larger, but not enough slots for exchange
    {
        let env = with_step_exec();
        alloc_another(&env);
        let old_task = env.task_executor.get_task();
        latest(&env, Some(14), None);
        env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap();
        assert_same_task(&old_task, &env.task_executor.get_task());
        assert_eq!(env.slot_mgr().available_slots(), 2);
        env.finish();
    }

    // required slots become larger, but failed to notify application
    {
        let env = with_step_exec();
        alloc_another(&env);
        let old_task = env.task_executor.get_task();
        latest(&env, Some(12), None);
        env.ctrl.ret("step.ResourceModified", Err::<(), String>("some error".to_owned()));
        env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap();
        assert_same_task(&old_task, &env.task_executor.get_task());
        assert_eq!(env.slot_mgr().available_slots(), 2);
        env.finish();
    }

    // required slots become larger, apply successfully
    {
        let env = with_step_exec();
        alloc_another(&env);
        let latest_task = latest(&env, Some(12), None);
        env.ctrl.ret("step.ResourceModified", Ok::<(), String>(()));
        env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap();
        assert_same_task(&latest_task, &env.task_executor.get_task());
        assert_eq!(env.slot_mgr().available_slots(), 0);
        env.finish();
    }

    // task meta modified, but failed to notify
    {
        let env = new_task_executor_run_env0();
        env.task_executor.set_step_exec(env.step_executor.clone());
        let old_task = env.task_executor.get_task();
        latest(&env, None, Some(b"modified"));
        env.ctrl.ret("step.TaskMetaModified|modified", Err::<(), String>("some error".to_owned()));
        let err = env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap_err();
        assert!(err.to_string().contains("failed to apply task param modification"), "{err}");
        assert_same_task(&old_task, &env.task_executor.get_task());
        env.finish();
    }

    // task meta modified, apply successfully
    {
        let env = new_task_executor_run_env0();
        env.task_executor.set_step_exec(env.step_executor.clone());
        let latest_task = latest(&env, None, Some(b"modified"));
        env.ctrl.ret("step.TaskMetaModified|modified", Ok::<(), String>(()));
        env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap();
        assert_same_task(&latest_task, &env.task_executor.get_task());
        env.finish();
    }

    // both meta and required slots modified, both failed to apply
    {
        let env = with_step_exec();
        alloc_another(&env);
        let old_task = env.task_executor.get_task();
        latest(&env, Some(12), Some(b"modified"));
        env.ctrl.ret("step.ResourceModified", Err::<(), String>("some error".to_owned()));
        env.ctrl.ret("step.TaskMetaModified|*", Err::<(), String>("some error".to_owned()));
        let err = env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap_err();
        assert!(err.to_string().contains("failed to apply task param modification"), "{err}");
        assert_same_task(&old_task, &env.task_executor.get_task());
        assert_eq!(env.slot_mgr().available_slots(), 2);
        env.finish();
    }

    // both meta and required slots modified, apply required slots success, but failed to apply meta
    {
        let env = with_step_exec();
        alloc_another(&env);
        let old_task = env.task_executor.get_task();
        let latest_task = latest(&env, Some(12), Some(b"modified"));
        env.ctrl.ret("step.ResourceModified", Ok::<(), String>(()));
        env.ctrl.ret("step.TaskMetaModified|*", Err::<(), String>("some error".to_owned()));
        let err = env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap_err();
        assert!(err.to_string().contains("failed to apply task param modification"), "{err}");
        assert_eq!(env.task_executor.get_task_base().required_slots, latest_task.base.required_slots);
        assert!(env.task_executor.get_task().meta.is_empty());
        let mut old_task = old_task.as_ref().clone();
        old_task.base.required_slots = latest_task.base.required_slots;
        assert_same_task(&old_task, &env.task_executor.get_task());
        assert_eq!(env.slot_mgr().available_slots(), 0);
        env.finish();
    }

    // both meta and required slots modified, apply meta success, but failed to apply required slots
    {
        let env = with_step_exec();
        alloc_another(&env);
        let old_task = env.task_executor.get_task();
        let latest_task = latest(&env, Some(12), Some(b"modified"));
        env.ctrl.ret("step.ResourceModified", Err::<(), String>("some error".to_owned()));
        env.ctrl.ret("step.TaskMetaModified|*", Ok::<(), String>(()));
        env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap();
        assert_eq!(env.task_executor.get_task().meta, b"modified");
        assert_eq!(env.task_executor.get_task_base().required_slots, old_task.base.required_slots);
        let mut old_task = old_task.as_ref().clone();
        old_task.meta = latest_task.meta.clone();
        assert_same_task(&old_task, &env.task_executor.get_task());
        assert_eq!(env.slot_mgr().available_slots(), 2);
        env.finish();
    }

    // both meta and required slots modified, both apply success
    {
        let env = with_step_exec();
        alloc_another(&env);
        let latest_task = latest(&env, Some(12), Some(b"modified"));
        env.ctrl.ret("step.ResourceModified", Ok::<(), String>(()));
        env.ctrl.ret("step.TaskMetaModified|*", Ok::<(), String>(()));
        env.task_executor.detect_and_handle_param_modify(&Context::background()).unwrap();
        assert_same_task(&latest_task, &env.task_executor.get_task());
        assert_eq!(env.slot_mgr().available_slots(), 0);
        env.finish();
    }
}

/// Go `TestCheckBalanceSubtask`.
#[test]
fn test_check_balance_subtask() {
    let ctrl = Arc::new(Controller::default());
    let mock_subtask_table = Arc::new(MockTaskTable::new(&ctrl));
    let mock_extension: Arc<dyn Extension> = MockExtension::new(&ctrl);

    let ctx = Context::background();
    let task = Task {
        base: TaskBase { step: STEP_ONE, tp: TaskType::new("type"), id: 1, required_slots: 1, ..TaskBase::default() },
        ..Task::default()
    };
    let task_executor = BaseTaskExecutor::new(
        &ctx,
        task.clone(),
        Param::new_for_test(
            Arc::clone(&mock_subtask_table) as Arc<dyn crate::TaskTable>,
            Arc::new(SlotManager::new(16)),
            NODE_RESOURCE_FOR_TEST.clone(),
            "tidb1",
        ),
    );
    task_executor.set_extension(Arc::downgrade(&mock_extension));

    set(&CHECK_BALANCE_SUBTASK_INTERVAL, Duration::from_millis(100));
    set(&tidb_dxf_scheduler::RETRY_SQL_INTERVAL, Duration::from_millis(1));
    let running_key =
        key::get_subtasks_by_exec_id_and_step_and_states("tidb1", task.base.id, task.base.step, &[SUBTASK_STATE_RUNNING]);
    let failed_key = |subtask_id| {
        key::update_subtask_state_and_error(
            "tidb1",
            subtask_id,
            &SUBTASK_STATE_FAILED,
            &TaskExecutorError::NonIdempotentSubtask.to_string(),
        )
    };

    // context cancelled
    {
        let (canceled_ctx, cancel) = Context::with_cancel(&ctx);
        cancel.cancel();
        task_executor.check_balance_subtask(&canceled_ctx, None);
    }

    // subtask scheduled away
    {
        ctrl.expect(&running_key, ONCE, |_| StorageResult::<Vec<Subtask>>::Err(storage_err("error")));
        ctrl.ret(&running_key, StorageResult::Ok(Vec::<Subtask>::new()));
        let (run_ctx, cancel) = Context::with_cancel(&ctx);
        assert_eq!(run_ctx.err(), None);
        task_executor.check_balance_subtask(&ctx, Some(&cancel));
        assert_eq!(run_ctx.err(), Some(CtxError::Canceled));
        assert!(ctrl.satisfied());
    }

    // non-idempotent running subtask
    {
        let subtasks = vec![Subtask {
            base: SubtaskBase { id: 1, exec_id: "tidb1".to_owned(), ..SubtaskBase::default() },
            ..Subtask::default()
        }];
        // in-idempotent running subtask
        let returned = subtasks.clone();
        ctrl.expect(&running_key, ONCE, move |_| StorageResult::Ok(returned.clone()));
        ctrl.ret("ext.IsIdempotent|1", false);
        ctrl.ret(failed_key(subtasks[0].base.id), StorageResult::Ok(()));
        task_executor.check_balance_subtask(&ctx, None);
        assert!(ctrl.satisfied());

        // if we failed to change state of non-idempotent subtask, will retry
        let retry_cnt_bak = tidb_dxf_scheduler::retry_sql_times();
        reduce_retry_sql_times(1);
        let returned = subtasks.clone();
        ctrl.expect(&running_key, ONCE, move |_| StorageResult::Ok(returned.clone()));
        ctrl.ret("ext.IsIdempotent|1", false);
        ctrl.expect(failed_key(subtasks[0].base.id), ONCE, |_| StorageResult::<()>::Err(storage_err("some error")));
        // retry part
        let returned = subtasks.clone();
        ctrl.expect(&running_key, ONCE, move |_| StorageResult::Ok(returned.clone()));
        ctrl.ret("ext.IsIdempotent|1", false);
        ctrl.ret(failed_key(subtasks[0].base.id), StorageResult::Ok(()));
        task_executor.check_balance_subtask(&ctx, None);
        assert!(ctrl.satisfied());
        reduce_retry_sql_times(retry_cnt_bak);
    }

    // current running subtask is skipped
    {
        assert_eq!(task_executor.curr_subtask_id.load(Ordering::SeqCst), 0);
        task_executor.curr_subtask_id.store(1, Ordering::SeqCst);
        let subtasks: Vec<Subtask> = [1, 2]
            .into_iter()
            .map(|id| Subtask {
                base: SubtaskBase { id, exec_id: "tidb1".to_owned(), ..SubtaskBase::default() },
                ..Subtask::default()
            })
            .collect();
        ctrl.expect(&running_key, ONCE, move |_| StorageResult::Ok(subtasks.clone()));
        ctrl.ret("ext.IsIdempotent|*", true);
        ctrl.ret("RunningSubtasksBack2Pending|2@tidb1", StorageResult::Ok(()));
        // used to break the loop
        ctrl.ret(&running_key, StorageResult::Ok(Vec::<Subtask>::new()));
        task_executor.check_balance_subtask(&ctx, None);
        assert!(ctrl.satisfied());
    }
    ctrl.finish();
}

/// Go `TestInject`.
#[test]
fn test_inject() {
    let e = BaseStepExecutor::default();
    let r = Arc::new(StepResource { cpu: Allocatable::new(1), mem: Allocatable::new(0) });
    let ctrl = Arc::new(Controller::default());
    let task_table = Arc::new(MockTaskTable::new(&ctrl));
    let (update_table, get_table) = (Arc::clone(&task_table), Arc::clone(&task_table));
    tidb_dxf_execute::set_framework_info(
        &e,
        &Task { base: TaskBase { step: STEP_ONE, ..TaskBase::default() }, ..Task::default() },
        Arc::clone(&r),
        Some(Arc::new(move |ctx: &Context, subtask_id, checkpoint: &serde_json::Value| {
            crate::TaskTable::update_subtask_checkpoint(update_table.as_ref(), ctx, subtask_id, checkpoint)
                .map_err(|err| Box::new(err) as tidb_dxf_execute::Error)
        })),
        Some(Arc::new(move |ctx: &Context, subtask_id| {
            crate::TaskTable::get_subtask_checkpoint(get_table.as_ref(), ctx, subtask_id)
                .map_err(|err| Box::new(err) as tidb_dxf_execute::Error)
        })),
    );
    let got = e.get_resource();
    assert!(Arc::ptr_eq(&r, &got));
    ctrl.finish();
}
