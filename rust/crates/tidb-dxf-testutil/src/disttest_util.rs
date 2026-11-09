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

//! Go `disttest_util.go` and `executor_util.go`: the mocks and registrations
//! the framework's integration tests share.

use std::sync::Arc;

use tidb_dxf::{Step, Subtask, Task, TaskBase, TaskType, TASK_STATE_PAUSED, TASK_TYPE_EXAMPLE};
use tidb_dxf_execute::StepExecutor;
use tidb_dxf_mock::{MockCleaner, MockStepExecutor, MockTaskExecutorExtension, StepResult};
use tidb_gocontext::Context;
use tidb_gomock::{Controller, Times};

use crate::TestContext;

/// Go `GetCommonTaskExecutorExt`: no error is retryable.
#[must_use]
pub fn get_common_task_executor_ext(
    ctrl: &Arc<Controller>,
    get_step_exec_fn: impl Fn(&Task) -> Result<Arc<dyn StepExecutor>, tidb_dxf_taskexecutor::Error> + Send + Sync + 'static,
) -> Arc<MockTaskExecutorExtension> {
    get_task_executor_ext(ctrl, get_step_exec_fn, |_| false)
}

/// Go `GetTaskExecutorExt`.
#[must_use]
pub fn get_task_executor_ext(
    ctrl: &Arc<Controller>,
    get_step_exec_fn: impl Fn(&Task) -> Result<Arc<dyn StepExecutor>, tidb_dxf_taskexecutor::Error> + Send + Sync + 'static,
    is_retryable_error_fn: impl Fn(&str) -> bool + Send + Sync + 'static,
) -> Arc<MockTaskExecutorExtension> {
    let executor_ext = MockTaskExecutorExtension::new(ctrl);
    executor_ext.ret_times("IsIdempotent|*", Times::Any, true);
    executor_ext.expect("GetStepExecutor", Times::Any, move |input| {
        get_step_exec_fn(input.downcast_ref::<Task>().expect("GetStepExecutor gets the task"))
    });
    executor_ext.expect("IsRetryableError|*", Times::Any, move |input| {
        is_retryable_error_fn(input.downcast_ref::<String>().expect("IsRetryableError gets the error"))
    });
    executor_ext
}

/// Go `GetCommonStepExecutor`.
#[must_use]
pub fn get_common_step_executor(
    ctrl: &Arc<Controller>,
    step: Step,
    run_subtask_fn: impl Fn(&Context, &Subtask) -> StepResult + Send + Sync + 'static,
) -> Arc<MockStepExecutor> {
    let executor = MockStepExecutor::new(ctrl);
    executor.ret_times("Init", Times::Any, StepResult::Ok(()));
    executor.expect("RunSubtask|*", Times::Any, move |input| {
        let (ctx, subtask) = input.downcast_ref::<(Context, Subtask)>().expect("RunSubtask gets (ctx, subtask)");
        run_subtask_fn(ctx, subtask)
    });
    executor.ret_times("GetStep", Times::Any, step);
    executor.ret_times("Cleanup", Times::Any, StepResult::Ok(()));
    executor.ret_times("RealtimeSummary", Times::Any, None::<Arc<tidb_dxf_execute::SubtaskSummary>>);
    executor
}

/// Go `GetCommonCleaner`.
#[must_use]
pub fn get_common_cleaner(ctrl: &Arc<Controller>) -> Arc<MockCleaner> {
    let r = MockCleaner::new(ctrl);
    r.ret_times("Clean", Times::Any, Ok::<(), tidb_dxf_scheduler::Error>(()));
    r
}

/// The registrations Go's `t.Cleanup` clears: dropped, it clears the
/// scheduler, cleaner and task executor factories.
#[must_use]
pub struct RegisteredTaskType;

impl Drop for RegisteredTaskType {
    fn drop(&mut self) {
        tidb_dxf_scheduler::clear_scheduler_factory();
        tidb_dxf_scheduler::clear_cleaner_factory();
        tidb_dxf_taskexecutor::clear_task_executors();
    }
}

/// Go `RegisterExampleTask`.
pub fn register_example_task(
    scheduler_ext: Arc<dyn tidb_dxf_scheduler::Extension>,
    executor_ext: Arc<dyn tidb_dxf_taskexecutor::Extension>,
    cleaner: Arc<dyn tidb_dxf_scheduler::Cleaner>,
) -> RegisteredTaskType {
    register_task_type(TASK_TYPE_EXAMPLE, scheduler_ext, executor_ext, cleaner)
}

/// Go `registerTaskType`: base schedulers and executors whose extensions
/// are the given mocks (the registrations keep them alive).
fn register_task_type(
    task_type: TaskType,
    scheduler_ext: Arc<dyn tidb_dxf_scheduler::Extension>,
    executor_ext: Arc<dyn tidb_dxf_taskexecutor::Extension>,
    cleaner: Arc<dyn tidb_dxf_scheduler::Cleaner>,
) -> RegisteredTaskType {
    tidb_dxf_scheduler::register_scheduler_factory(
        task_type.clone(),
        Arc::new(move |ctx, task, param| {
            let base_scheduler = Arc::new(tidb_dxf_scheduler::BaseScheduler::new(ctx, task, param));
            base_scheduler.set_extension(Arc::downgrade(&scheduler_ext));
            base_scheduler
        }),
    );

    tidb_dxf_scheduler::register_cleaner_factory(task_type.clone(), Arc::new(move || Arc::clone(&cleaner)));

    register_executor(task_type, executor_ext);
    RegisteredTaskType
}

fn register_executor(task_type: TaskType, executor_ext: Arc<dyn tidb_dxf_taskexecutor::Extension>) {
    tidb_dxf_taskexecutor::register_task_type(
        task_type,
        Arc::new(move |ctx, task, param| {
            let s = Arc::new(tidb_dxf_taskexecutor::BaseTaskExecutor::new(ctx, task, param));
            s.set_extension(Arc::downgrade(&executor_ext));
            s
        }),
    );
}

/// Go `RegisterTaskTypeForRollback`.
pub fn register_task_type_for_rollback(
    ctrl: &Arc<Controller>,
    scheduler_ext: Arc<dyn tidb_dxf_scheduler::Extension>,
    test_context: &Arc<TestContext>,
) -> RegisteredTaskType {
    let executor_ext = step_executor_ext(ctrl, test_context);
    register_example_task(scheduler_ext, executor_ext, get_common_cleaner(ctrl))
}

/// The executor extension whose step executors collect each subtask they
/// run into `test_context`.
fn step_executor_ext(ctrl: &Arc<Controller>, test_context: &Arc<TestContext>) -> Arc<MockTaskExecutorExtension> {
    let (ctrl_for_step, test_context) = (Arc::clone(ctrl), Arc::clone(test_context));
    get_common_task_executor_ext(ctrl, move |task| {
        let test_context = Arc::clone(&test_context);
        let subtask_run_fn = move |_: &Context, subtask: &Subtask| -> StepResult {
            test_context.collect_subtask(subtask);
            Ok(())
        };
        Ok(get_common_step_executor(&ctrl_for_step, task.base.step, subtask_run_fn) as Arc<dyn StepExecutor>)
    })
}

/// Go `InitTaskExecutor` (`executor_util.go`).
pub fn init_task_executor(
    ctrl: &Arc<Controller>,
    run_subtask_fn: impl Fn(&Context, &Subtask) -> StepResult + Send + Sync + Clone + 'static,
) {
    let ctrl_for_step = Arc::clone(ctrl);
    let executor_ext = get_common_task_executor_ext(ctrl, move |task| {
        Ok(get_common_step_executor(&ctrl_for_step, task.base.step, run_subtask_fn.clone()) as Arc<dyn StepExecutor>)
    });
    register_executor(TASK_TYPE_EXAMPLE, executor_ext);
}

/// Go `SubmitAndWaitTask`.
///
/// # Panics
/// The task cannot be submitted or waited for.
pub fn submit_and_wait_task(ctx: &Context, task_key: &str, target_scope: &str, concurrency: isize) -> TaskBase {
    tidb_dxf_handle::submit_task(task_key, &TASK_TYPE_EXAMPLE, &crate::get_task_ks(), concurrency, target_scope, 0, &[]).unwrap();
    wait_task_done_or_paused(ctx, task_key)
}

/// Go `WaitTaskDoneOrPaused`.
#[must_use]
pub fn wait_task_done_or_paused(ctx: &Context, task_key: &str) -> TaskBase {
    wait_task_until(ctx, task_key, |task| task.is_done() || task.state == TASK_STATE_PAUSED)
}

/// Go `WaitTaskDone`.
#[must_use]
pub fn wait_task_done(ctx: &Context, task_key: &str) -> TaskBase {
    wait_task_until(ctx, task_key, TaskBase::is_done)
}

/// Go `waitTaskUntil`.
fn wait_task_until(ctx: &Context, task_key: &str, f: impl Fn(&TaskBase) -> bool) -> TaskBase {
    let task_mgr = tidb_dxf_storage::get_dxf_svc_task_mgr().unwrap();
    let got_task = task_mgr.get_task_base_by_key_with_history(task_key).unwrap();
    tidb_dxf_handle::wait_task(ctx, got_task.id, f).unwrap()
}
