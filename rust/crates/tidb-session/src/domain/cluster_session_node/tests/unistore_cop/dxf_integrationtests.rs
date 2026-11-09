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

//! Go `pkg/dxf/framework/integrationtests` over the embedded unistore node:
//! in-process DXF nodes (`tidb_dxf_testutil::TestDxfContext`) share the
//! node's store.

use super::*;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;
use tidb_dxf::*;
use tidb_dxf_execute::StepExecutor;
use tidb_dxf_mock::{MockStepExecutor, StepResult};
use tidb_dxf_storage::TaskManager;
use tidb_dxf_testutil as testutil;
use tidb_gocontext::Context;
use tidb_gomock::{Controller, Times};

/// Go's test store and its task manager (Go `testkit.CreateMockStore` and a
/// pool of sessions over it).
fn store_and_task_manager() -> (UnistoreClusterStack, Arc<TaskManager>) {
    let (stack, _users) = cop_backed_stack();
    let task_mgr = Arc::new(TaskManager::new(stack.factory.dxf_session_pool()));
    (stack, task_mgr)
}

type RunSubtaskFn = Arc<dyn Fn(&Context, &Subtask) -> StepResult + Send + Sync>;

/// Go `registerExampleTask`.
fn register_example_task(
    ctrl: &Arc<Controller>,
    scheduler_ext: Arc<dyn tidb_dxf_scheduler::Extension>,
    test_context: &Arc<testutil::TestContext>,
    run_subtask_fn: Option<RunSubtaskFn>,
) -> testutil::RegisteredTaskType {
    let run_subtask_fn = run_subtask_fn.unwrap_or_else(|| get_common_subtask_run_fn(test_context));
    let ctrl_for_step = Arc::clone(ctrl);
    let executor_ext = testutil::get_common_task_executor_ext(ctrl, move |task| {
        let run_subtask_fn = Arc::clone(&run_subtask_fn);
        let step_executor =
            testutil::get_common_step_executor(&ctrl_for_step, task.base.step, move |ctx, subtask| run_subtask_fn(ctx, subtask));
        Ok(step_executor as Arc<dyn StepExecutor>)
    });
    testutil::register_example_task(scheduler_ext, executor_ext, testutil::get_common_cleaner(ctrl))
}

/// Go `getCommonSubtaskRunFn`.
fn get_common_subtask_run_fn(test_ctx: &Arc<testutil::TestContext>) -> RunSubtaskFn {
    let test_ctx = Arc::clone(test_ctx);
    Arc::new(move |_, subtask| {
        assert!(subtask.base.step == STEP_ONE || subtask.base.step == STEP_TWO, "invalid step");
        test_ctx.collect_subtask(subtask);
        Ok(())
    })
}

/// Go `submitTaskAndCheckSuccessForBasic`.
fn submit_task_and_check_success_for_basic(ctx: &Context, task_key: &str, test_context: &testutil::TestContext) -> i64 {
    let scope = tidb_dxf_handle::get_target_scope();
    submit_task_and_check_success(ctx, task_key, &scope, test_context, &[(STEP_ONE, 3), (STEP_TWO, 1)])
}

/// Go `submitTaskAndCheckSuccess`.
fn submit_task_and_check_success(
    ctx: &Context,
    task_key: &str,
    target_scope: &str,
    test_context: &testutil::TestContext,
    subtask_cnts: &[(Step, usize)],
) -> i64 {
    let task = testutil::submit_and_wait_task(ctx, task_key, target_scope, 1);
    assert_eq!(task.state, TASK_STATE_SUCCEED);
    for &(step, cnt) in subtask_cnts {
        assert_eq!(test_context.collected_subtask_cnt(task.id, step), cnt);
    }
    task.id
}

/// Go `time.Sleep(time.Duration(random.Int63n(int64(3 * time.Second))))`.
fn sleep_up_to_3s() {
    std::thread::sleep(Duration::from_nanos(rand::random_range(0..3_000_000_000)));
}

/// Go `TestRandomOwnerChangeWithMultipleTasks`.
#[test]
fn test_random_owner_change_with_multiple_tasks() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_test_dxf_context(task_mgr, 5, 16, true);

    let _registered =
        register_example_task(&c.mock_ctrl, testutil::get_mock_basic_scheduler_ext(&c.mock_ctrl), &c.test_context, None);
    std::thread::scope(|wg| {
        for i in 0..10 {
            let c = &c;
            wg.spawn(move || {
                submit_task_and_check_success_for_basic(&c.ctx, &format!("key{i}"), &c.test_context);
            });
        }
        wg.spawn(|| {
            for _ in 0..3 {
                c.change_owner();
                sleep_up_to_3s();
            }
        });
    });
}

/// Go `TestFrameworkScaleInAndOut`.
#[test]
fn test_framework_scale_in_and_out() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_test_dxf_context(task_mgr, 5, 16, true);

    let _registered =
        register_example_task(&c.mock_ctrl, testutil::get_mock_basic_scheduler_ext(&c.mock_ctrl), &c.test_context, None);
    std::thread::scope(|wg| {
        for i in 0..12 {
            let c = &c;
            wg.spawn(move || {
                submit_task_and_check_success_for_basic(&c.ctx, &format!("key{i}"), &c.test_context);
            });
        }
        wg.spawn(|| {
            for _ in 0..3 {
                if rand::random_range(0..2) == 0 {
                    c.scale_out(1);
                } else {
                    c.scale_in(1);
                }
                sleep_up_to_3s();
            }
        });
    });
}

/// Go `TestFrameworkCancelTask`.
#[test]
fn test_framework_cancel_task() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_test_dxf_context(task_mgr, 2, 16, true);

    let _registered =
        register_example_task(&c.mock_ctrl, testutil::get_mock_basic_scheduler_ext(&c.mock_ctrl), &c.test_context, None);
    let counter = Arc::new(AtomicI32::new(0));
    {
        let task_mgr = Arc::clone(&c.task_mgr);
        *tidb_dxf_taskexecutor::AFTER_RUN_SUBTASK.write().unwrap() = Some(Arc::new(move |e, _, _| {
            if counter.fetch_add(1, Ordering::SeqCst) + 1 == 1 {
                task_mgr.cancel_task(tidb_dxf_taskexecutor::TaskExecutor::get_task_base(e).id).unwrap();
            }
        }));
    }
    let scope = tidb_dxf_handle::get_target_scope();
    let task = testutil::submit_and_wait_task(&c.ctx, "key1", &scope, 1);
    *tidb_dxf_taskexecutor::AFTER_RUN_SUBTASK.write().unwrap() = None;
    assert_eq!(task.state, TASK_STATE_REVERTED);
}

/// Go `TestFrameworkSubTaskInitEnvFailed`.
#[test]
fn test_framework_sub_task_init_env_failed() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_test_dxf_context(task_mgr, 1, 16, true);
    let scheduler_ext = testutil::get_mock_basic_scheduler_ext(&c.mock_ctrl);
    let step_exec = MockStepExecutor::new(&c.mock_ctrl);
    step_exec.expect("Init", Times::Any, |_| -> StepResult { Err(Arc::new(std::io::Error::other("mockExecSubtaskInitEnvErr"))) });
    let executor_ext = testutil::get_common_task_executor_ext(&c.mock_ctrl, move |_| {
        Ok(Arc::clone(&step_exec) as Arc<dyn StepExecutor>)
    });
    let _registered = testutil::register_example_task(scheduler_ext, executor_ext, testutil::get_common_cleaner(&c.mock_ctrl));
    let scope = tidb_dxf_handle::get_target_scope();
    let task = testutil::submit_and_wait_task(&c.ctx, "key1", &scope, 1);
    assert_eq!(task.state, TASK_STATE_REVERTED);
}

/// Go `TestFrameworkRunSubtaskCancelOrFailed`.
#[test]
fn test_framework_run_subtask_cancel_or_failed() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_test_dxf_context(task_mgr, 3, 16, true);
    let scope = tidb_dxf_handle::get_target_scope();
    let _registered =
        register_example_task(&c.mock_ctrl, testutil::get_mock_basic_scheduler_ext(&c.mock_ctrl), &c.test_context, None);

    // meet cancel on run subtask
    {
        let counter = AtomicI32::new(0);
        *tidb_dxf_taskexecutor::AFTER_RUN_SUBTASK.write().unwrap() = Some(Arc::new(move |e, err_p, _| {
            if counter.fetch_add(1, Ordering::SeqCst) + 1 == 1 {
                tidb_dxf_taskexecutor::TaskExecutor::cancel_running_subtask(e);
                *err_p = Some(Arc::new(tidb_dxf_taskexecutor::TaskExecutorError::CancelSubtask));
            }
        }));
        let task = testutil::submit_and_wait_task(&c.ctx, "key1", &scope, 1);
        assert_eq!(task.state, TASK_STATE_REVERTED);
    }

    // meet some error on run subtask
    {
        let counter = AtomicI32::new(0);
        *tidb_dxf_taskexecutor::AFTER_RUN_SUBTASK.write().unwrap() = Some(Arc::new(move |_, err_p, _| {
            if counter.fetch_add(1, Ordering::SeqCst) + 1 == 1 {
                *err_p = Some(Arc::new(std::io::Error::other("MockExecutorRunErr")));
            }
        }));
        let task = testutil::submit_and_wait_task(&c.ctx, "key2", &scope, 1);
        assert_eq!(task.state, TASK_STATE_REVERTED);
    }
    *tidb_dxf_taskexecutor::AFTER_RUN_SUBTASK.write().unwrap() = None;
}

/// Go `TestTaskCancelledBeforeUpdateTask`.
#[test]
fn test_task_cancelled_before_update_task() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_test_dxf_context(task_mgr, 1, 16, true);

    let _registered =
        register_example_task(&c.mock_ctrl, testutil::get_mock_basic_scheduler_ext(&c.mock_ctrl), &c.test_context, None);
    let counter = AtomicI32::new(0);
    {
        let task_mgr = Arc::clone(&c.task_mgr);
        *tidb_dxf_scheduler::CANCEL_BEFORE_UPDATE_TASK.write().unwrap() = Some(Arc::new(move |task_id| {
            if counter.fetch_add(1, Ordering::SeqCst) + 1 == 1 {
                task_mgr.cancel_task(task_id).unwrap();
            }
        }));
    }
    let scope = tidb_dxf_handle::get_target_scope();
    let task = testutil::submit_and_wait_task(&c.ctx, "key1", &scope, 1);
    *tidb_dxf_scheduler::CANCEL_BEFORE_UPDATE_TASK.write().unwrap() = None;
    assert_eq!(task.state, TASK_STATE_REVERTED);
}

/// Go `TestOwnerChangeWhenSchedule`.
#[test]
fn test_owner_change_when_schedule() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_test_dxf_context(task_mgr, 3, 16, true);
    let _registered =
        register_example_task(&c.mock_ctrl, testutil::get_mock_basic_scheduler_ext(&c.mock_ctrl), &c.test_context, None);
    let counter = AtomicI32::new(0);
    {
        let c = Arc::clone(&c);
        *tidb_dxf_scheduler::MOCK_OWNER_CHANGE.write().unwrap() = Some(Arc::new(move || {
            if counter.fetch_add(1, Ordering::SeqCst) + 1 == 1 {
                c.async_change_owner();
                std::thread::sleep(Duration::from_secs(1));
            }
        }));
    }
    submit_task_and_check_success_for_basic(&c.ctx, "😊", &c.test_context);
    *tidb_dxf_scheduler::MOCK_OWNER_CHANGE.write().unwrap() = None;
}

/// Go `submitTaskAndCheckSuccessForHA`.
fn submit_task_and_check_success_for_ha(ctx: &Context, task_key: &str, test_context: &testutil::TestContext) {
    let scope = tidb_dxf_handle::get_target_scope();
    submit_task_and_check_success(ctx, task_key, &scope, test_context, &[(STEP_ONE, 10), (STEP_TWO, 5)]);
}

/// Go's `mockTiDBShutdown` call that shuts down the target nodes.
fn shut_down_nodes(
    c: &Arc<testutil::TestDxfContext>,
    target_nodes: impl Fn(&TaskBase) -> Option<std::collections::HashSet<String>> + Send + Sync + 'static,
) {
    let c = Arc::clone(c);
    *tidb_dxf_taskexecutor::MOCK_TIDB_SHUTDOWN.write().unwrap() = Some(Arc::new(move |e, exec_id, task| {
        if target_nodes(task).is_some_and(|nodes| nodes.contains(exec_id)) {
            c.async_shutdown(exec_id);
            tidb_dxf_taskexecutor::TaskExecutor::cancel(e);
        }
    }));
}

/// Go `TestHANodeRandomShutdown`.
#[test]
fn test_ha_node_random_shutdown() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_dxf_context_with_random_nodes(task_mgr, 4, 15);
    let _registered =
        register_example_task(&c.mock_ctrl, testutil::get_mock_ha_test_scheduler_ext(&c.mock_ctrl), &c.test_context, None);

    // we keep [1, 10] nodes running, as we only have 10 subtask at stepOne
    let keep_count = (c.node_count() - 1).min(rand::random_range(0..10) + 1);
    let node_need_down = c.get_rand_node_ids(c.node_count() - keep_count);
    shut_down_nodes(&c, move |_| Some(node_need_down.clone()));
    submit_task_and_check_success_for_ha(&c.ctx, "😊", &c.test_context);
    *tidb_dxf_taskexecutor::MOCK_TIDB_SHUTDOWN.write().unwrap() = None;
}

/// Go `TestHARandomShutdownInDifferentStep`.
#[test]
fn test_ha_random_shutdown_in_different_step() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_dxf_context_with_random_nodes(task_mgr, 6, 15);

    let _registered =
        register_example_task(&c.mock_ctrl, testutil::get_mock_ha_test_scheduler_ext(&c.mock_ctrl), &c.test_context, None);
    // they might overlap, but will leave at least 2 nodes running
    let node_need_down_at_step_one = c.get_rand_node_ids(c.node_count() / 2 - 1);
    let node_need_down_at_step_two = c.get_rand_node_ids(c.node_count() / 2 - 1);
    shut_down_nodes(&c, move |task| {
        if task.step == STEP_ONE {
            Some(node_need_down_at_step_one.clone())
        } else if task.step == STEP_TWO {
            Some(node_need_down_at_step_two.clone())
        } else {
            None
        }
    });
    submit_task_and_check_success_for_ha(&c.ctx, "😊", &c.test_context);
    *tidb_dxf_taskexecutor::MOCK_TIDB_SHUTDOWN.write().unwrap() = None;
}

/// Go `TestHAMultipleOwner`.
#[test]
fn test_ha_multiple_owner() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_dxf_context_with_random_nodes(task_mgr, 4, 8);
    let prev_count = c.node_count();
    let additional_owner_cnt = rand::random_range(0..2) + 1;
    for i in 0..additional_owner_cnt {
        c.scale_out_by(&format!("tidb-{i}"), true);
    }
    assert_eq!(c.node_count(), prev_count + additional_owner_cnt);

    let _registered =
        register_example_task(&c.mock_ctrl, testutil::get_mock_ha_test_scheduler_ext(&c.mock_ctrl), &c.test_context, None);
    std::thread::scope(|wg| {
        for i in 0..10 {
            let c = &c;
            wg.spawn(move || submit_task_and_check_success_for_ha(&c.ctx, &format!("key{i}"), &c.test_context));
        }
    });
}

/// Go `TestFrameworkRollback`.
#[test]
fn test_framework_rollback() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_test_dxf_context(task_mgr, 2, 16, true);
    let _registered = testutil::register_task_type_for_rollback(
        &c.mock_ctrl,
        testutil::get_mock_rollback_scheduler_ext(&c.mock_ctrl),
        &c.test_context,
    );
    let counter = AtomicI32::new(0);
    {
        let task_mgr = Arc::clone(&c.task_mgr);
        *tidb_dxf_scheduler::AFTER_REFRESH_TASK.write().unwrap() = Some(Arc::new(move |task| {
            if counter.fetch_add(1, Ordering::SeqCst) + 1 <= 2 && task.base.state == TASK_STATE_RUNNING {
                task_mgr.cancel_task(task.base.id).unwrap();
            }
        }));
    }

    let scope = tidb_dxf_handle::get_target_scope();
    let task = testutil::submit_and_wait_task(&c.ctx, "key1", &scope, 1);
    *tidb_dxf_scheduler::AFTER_REFRESH_TASK.write().unwrap() = None;
    assert_eq!(task.state, TASK_STATE_REVERTED);
}

/// Go `TestOnTaskError`.
#[test]
fn test_on_task_error() {
    let (stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_test_dxf_context(task_mgr, 2, 16, true);
    let scope = tidb_dxf_handle::get_target_scope();

    // retryable error on OnNextSubtasksBatch
    {
        let _registered = register_example_task(
            &c.mock_ctrl,
            testutil::get_plan_err_scheduler_ext(&c.mock_ctrl, &c.test_context),
            &c.test_context,
            None,
        );
        submit_task_and_check_success_for_basic(&c.ctx, "key1", &c.test_context);
    }

    // non retryable error on OnNextSubtasksBatch
    {
        let registered =
            register_example_task(&c.mock_ctrl, testutil::get_plan_not_retryable_err_scheduler_ext(&c.mock_ctrl), &c.test_context, None);
        let task = testutil::submit_and_wait_task(&c.ctx, "key2-1", &scope, 1);
        assert_eq!(task.state, TASK_STATE_REVERTED);
        drop(registered);
        let _registered = register_example_task(
            &c.mock_ctrl,
            testutil::get_step_two_plan_not_retryable_err_scheduler_ext(&c.mock_ctrl),
            &c.test_context,
            None,
        );
        let task = testutil::submit_and_wait_task(&c.ctx, "key2-2", &scope, 1);
        assert_eq!(task.state, TASK_STATE_REVERTED);
    }

    let prepare_for_awaiting_resolution = |task_key: &str| -> (i64, testutil::RegisteredTaskType) {
        let subtask_err_retryable = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ctrl_for_step = Arc::clone(&c.mock_ctrl);
        let retryable_for_step = Arc::clone(&subtask_err_retryable);
        let retryable_for_check = Arc::clone(&subtask_err_retryable);
        let executor_ext = testutil::get_task_executor_ext(
            &c.mock_ctrl,
            move |task| {
                let retryable = Arc::clone(&retryable_for_step);
                let step_executor = testutil::get_common_step_executor(&ctrl_for_step, task.base.step, move |_, _| {
                    if !retryable.load(Ordering::SeqCst) {
                        return Err(Arc::new(std::io::Error::other("non retryable subtask error")));
                    }
                    Ok(())
                });
                Ok(step_executor as Arc<dyn StepExecutor>)
            },
            move |_| retryable_for_check.load(Ordering::SeqCst),
        );
        let registered = testutil::register_example_task(
            testutil::get_plan_err_scheduler_ext(&c.mock_ctrl, &c.test_context),
            executor_ext,
            testutil::get_common_cleaner(&c.mock_ctrl),
        );
        let tm = tidb_dxf_storage::get_task_manager().unwrap();
        let task_id = tm
            .create_task(
                task_key,
                &TASK_TYPE_EXAMPLE,
                "",
                1,
                &scope,
                2,
                &ExtraParams { manual_recovery: true, ..ExtraParams::default() },
                &[],
            )
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while tm.get_task_by_id(task_id).unwrap().base.state != TASK_STATE_AWAITING_RESOLUTION {
            assert!(std::time::Instant::now() < deadline, "the task did not await resolution");
            std::thread::sleep(Duration::from_millis(100));
        }
        subtask_err_retryable.store(true, Ordering::SeqCst);
        (task_id, registered)
    };

    // task enter awaiting-resolution state if ManualRecovery set, success after manual recover
    {
        let task_key = "key3-1";
        let (task_id, _registered) = prepare_for_awaiting_resolution(task_key);
        let mut session = stack.factory.open_session(session_context(4001)).unwrap();
        rows(
            &mut session,
            &format!("update mysql.tidb_background_subtask set state='pending' where state='failed' and task_key= '{task_id}'"),
        );
        rows(&mut session, &format!("update mysql.tidb_global_task set state='running' where id = {task_id}"));
        let task = testutil::wait_task_done(&c.ctx, task_key);
        assert_eq!(task.state, TASK_STATE_SUCCEED);
    }

    // task enter awaiting-resolution state if ManualRecovery set, cancel also works
    {
        let task_key = "key4-1";
        let (task_id, _registered) = prepare_for_awaiting_resolution(task_key);
        c.task_mgr.cancel_task(task_id).unwrap();
        let task = testutil::wait_task_done(&c.ctx, task_key);
        assert_eq!(task.state, TASK_STATE_REVERTED);
    }
}

/// Go `CheckSubtasksState`.
fn check_subtasks_state(task_id: i64, state: &SubtaskState, expected_cnt: i64) {
    let mgr = tidb_dxf_storage::get_task_manager().unwrap();
    testutil::print_subtask_info(&mgr, task_id);
    let cnt_by_states_step_one = mgr.get_subtask_cnt_group_by_states(task_id, STEP_ONE).unwrap();
    let cnt_by_states_step_two = mgr.get_subtask_cnt_group_by_states(task_id, STEP_TWO).unwrap();
    let history_sub_tasks_cnt = testutil::get_subtasks_from_history_by_task_id(&mgr, task_id).unwrap();
    // all subtasks moved to history.
    if history_sub_tasks_cnt != 0 {
        assert_eq!(history_sub_tasks_cnt as i64, expected_cnt);
    } else {
        let cnt = |by_state: &std::collections::HashMap<SubtaskState, i64>| by_state.get(state).copied().unwrap_or(0);
        assert_eq!(cnt(&cnt_by_states_step_one) + cnt(&cnt_by_states_step_two), expected_cnt);
    }
}

/// Go `TestFrameworkPauseAndResume`.
#[test]
fn test_framework_pause_and_resume() {
    let (_stack, task_mgr) = store_and_task_manager();
    let c = testutil::new_test_dxf_context(task_mgr, 3, 16, true);

    let _registered =
        register_example_task(&c.mock_ctrl, testutil::get_mock_basic_scheduler_ext(&c.mock_ctrl), &c.test_context, None);
    // 1. schedule and pause one running task.
    let counter = Arc::new(AtomicI32::new(0));
    let pause_in = |state: TaskState| {
        let (counter, task_mgr) = (Arc::clone(&counter), Arc::clone(&c.task_mgr));
        counter.store(0, Ordering::SeqCst);
        *tidb_dxf_scheduler::BEFORE_REFRESH_TASK.write().unwrap() = Some(Arc::new(move |task: &Task| {
            if counter.fetch_add(1, Ordering::SeqCst) + 1 <= 2 && task.base.state == state {
                task_mgr.pause_task(&task.base.key).unwrap();
            }
        }));
    };
    pause_in(TASK_STATE_RUNNING);
    let scope = tidb_dxf_handle::get_target_scope();
    let task1 = testutil::submit_and_wait_task(&c.ctx, "key1", &scope, 1);
    assert_eq!(task1.state, TASK_STATE_PAUSED);
    *tidb_dxf_scheduler::BEFORE_REFRESH_TASK.write().unwrap() = None;
    // 4 subtask scheduled.
    tidb_dxf_handle::resume_task("key1").unwrap();
    let task1_base = testutil::wait_task_done(&c.ctx, &task1.key);
    assert_eq!(task1_base.state, TASK_STATE_SUCCEED);
    check_subtasks_state(task1.id, &SUBTASK_STATE_SUCCEED, 4);

    let mgr = tidb_dxf_storage::get_task_manager().unwrap();
    assert!(mgr.get_subtask_errors(task1.id).unwrap().is_empty());

    // 2. pause pending task.
    pause_in(TASK_STATE_PENDING);
    let task2 = testutil::submit_and_wait_task(&c.ctx, "key2", &scope, 1);
    assert_eq!(task2.state, TASK_STATE_PAUSED);
    *tidb_dxf_scheduler::BEFORE_REFRESH_TASK.write().unwrap() = None;
    // 4 subtask scheduled.
    tidb_dxf_handle::resume_task("key2").unwrap();
    let task2_base = testutil::wait_task_done(&c.ctx, &task2.key);
    assert_eq!(task2_base.state, TASK_STATE_SUCCEED);
    check_subtasks_state(task2.id, &SUBTASK_STATE_SUCCEED, 4);

    assert!(mgr.get_subtask_errors(task2.id).unwrap().is_empty());
}

/// Go `getMockBasicSchedulerExtForScope`.
fn get_mock_basic_scheduler_ext_for_scope(ctrl: &Arc<Controller>, subtask_cnt: usize) -> Arc<tidb_dxf_mock::MockSchedulerExtension> {
    testutil::get_mock_scheduler_ext(
        ctrl,
        testutil::SchedulerInfo {
            all_error_retryable: true,
            step_infos: vec![testutil::StepInfo::new(STEP_ONE, subtask_cnt), testutil::StepInfo::new(STEP_TWO, 1)],
        },
    )
}

/// Go `submitTaskAndCheckSuccessForScope`.
fn submit_task_and_check_success_for_scope(
    ctx: &Context,
    task_key: &str,
    node_cnt: usize,
    target_scope: &str,
    test_context: &testutil::TestContext,
) -> i64 {
    submit_task_and_check_success(ctx, task_key, target_scope, test_context, &[(STEP_ONE, node_cnt), (STEP_TWO, 1)])
}

/// Go `checkSubtaskOnNodes`.
fn check_subtask_on_nodes(task_id: i64, mut expected_nodes: Vec<String>) {
    let mgr = tidb_dxf_storage::get_task_manager().unwrap();
    let mut nodes = testutil::get_subtask_nodes(&mgr, task_id).unwrap();
    nodes.sort();
    expected_nodes.sort();
    assert_eq!(nodes, expected_nodes);
}

/// Go `targetScopeCase`.
struct TargetScopeCase {
    scope: String,
    node_scopes: Vec<String>,
}

/// Go `generateScopeCase`.
fn generate_scope_case(node_cnt: usize, scope_cnt: usize) -> TargetScopeCase {
    let scope = format!("scope-{}", rand::random_range(0..100));
    let mut node_scopes = vec![String::new(); node_cnt];
    for node_scope in node_scopes.iter_mut().take(node_cnt - scope_cnt) {
        *node_scope = format!("scope-{}", rand::random_range(0..100));
    }
    for node_scope in node_scopes.iter_mut().skip(node_cnt - scope_cnt) {
        node_scope.clone_from(&scope);
    }
    TargetScopeCase { scope, node_scopes }
}

/// Go `runTargetScopeCase`.
fn run_target_scope_case(c: &testutil::TestDxfContext, tk: &mut Session, test_case: &TargetScopeCase, idx: usize, node_cnt: usize) {
    for (i, node_scope) in test_case.node_scopes.iter().enumerate() {
        rows(
            tk,
            &format!(
                "update mysql.dist_framework_meta set role = \"{node_scope}\" where host = \"{}\"",
                c.get_node_id_by_idx(i)
            ),
        );
    }
    let (ch_tx, ch_rx) = crossbeam_channel::bounded::<()>(0);
    let counter = AtomicI32::new(0);
    *tidb_dxf_scheduler::SYNC_REFRESH.write().unwrap() = Some(Arc::new(move || {
        if counter.fetch_add(1, Ordering::SeqCst) + 1 <= 3 {
            let _ = ch_tx.send(());
        }
    }));
    for _ in 0..3 {
        ch_rx.recv().unwrap();
    }
    let task_id = submit_task_and_check_success_for_scope(&c.ctx, &format!("task{idx}"), node_cnt, &test_case.scope, &c.test_context);
    *tidb_dxf_scheduler::SYNC_REFRESH.write().unwrap() = None;
    let expected = test_case
        .node_scopes
        .iter()
        .enumerate()
        .filter(|(_, scope)| **scope == test_case.scope)
        .map(|(i, _)| c.get_node_id_by_idx(i))
        .collect();
    check_subtask_on_nodes(task_id, expected);
}

/// Go `TestTargetScope`.
#[test]
fn test_target_scope() {
    let (stack, task_mgr) = store_and_task_manager();
    let node_cnt = 10;
    let c = testutil::new_test_dxf_context(task_mgr, node_cnt, 16, true);
    let _registered =
        register_example_task(&c.mock_ctrl, get_mock_basic_scheduler_ext_for_scope(&c.mock_ctrl, node_cnt), &c.test_context, None);
    let mut tk = stack.factory.open_session(session_context(4002)).unwrap();
    let case_num = 10;
    for i in 0..case_num {
        run_target_scope_case(&c, &mut tk, &generate_scope_case(node_cnt, 5), i, node_cnt);
    }
}

/// Go `TestTiDBMaxDistTaskNodesSettings`.
#[test]
fn test_tidb_max_dist_task_nodes_settings() {
    if tidb_config_kerneltype::is_next_gen() {
        // tidb_max_dist_task_nodes in nextgen is not supported to be changed
        return;
    }
    let (stack, _users) = cop_backed_stack();
    let mut tk = stack.factory.open_session(session_context(4003)).unwrap();
    let max_nodes = |tk: &mut Session| displayed(rows(tk, "select @@global.tidb_max_dist_task_nodes"));

    rows(&mut tk, "set global tidb_max_dist_task_nodes=-1");
    assert_eq!(max_nodes(&mut tk), vec![vec!["-1".to_owned()]]);
    rows(&mut tk, "set global tidb_max_dist_task_nodes=1");
    assert_eq!(max_nodes(&mut tk), vec![vec!["1".to_owned()]]);
    rows(&mut tk, "set global tidb_max_dist_task_nodes=128");
    assert_eq!(max_nodes(&mut tk), vec![vec!["128".to_owned()]]);

    let err = tk.node_conn().execute("set global tidb_max_dist_task_nodes=0").err().expect("0 is refused");
    assert!(err.to_string().contains("max_dist_task_nodes should be -1 or [1, 128]"), "{err}");
    rows(&mut tk, "set global tidb_max_dist_task_nodes=129");
    assert_eq!(max_nodes(&mut tk), vec![vec!["128".to_owned()]]);
}
