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

/// Go `require.Eventually`.
fn eventually(timeout: Duration, tick: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + timeout;
    while !condition() {
        assert!(std::time::Instant::now() < deadline, "the condition was not met in {timeout:?}");
        std::thread::sleep(tick);
    }
}

/// Go `collectedRuntimeInfo`.
#[derive(Default)]
struct CollectedRuntimeInfo {
    curr_task_meta: std::sync::Mutex<Option<Vec<u8>>>,
    curr_task_concurrency: std::sync::atomic::AtomicI64,
    active_subtask_count: std::sync::atomic::AtomicI64,
    /// Go `subtaskRuntimeInfo`s: each subtask's step and concurrency.
    subtask_infos: std::sync::Mutex<Vec<(Step, isize)>>,
}

impl CollectedRuntimeInfo {
    /// Go's `*runtimeInfo = collectedRuntimeInfo{}`.
    fn reset(&self) {
        *self.curr_task_meta.lock().unwrap() = None;
        self.curr_task_concurrency.store(0, Ordering::SeqCst);
        self.active_subtask_count.store(0, Ordering::SeqCst);
        self.subtask_infos.lock().unwrap().clear();
    }

    fn meta(&self) -> Vec<u8> {
        self.curr_task_meta.lock().unwrap().clone().expect("a step executor was created")
    }
}

/// What Go's `prepareModifyTaskTest` returns. The channels drop before the
/// context (fields drop in order): a failed assertion then ends the blocked
/// subtasks instead of leaving the context's close waiting on them.
struct ModifyTaskTest {
    subtask_ch: crossbeam_channel::Sender<()>,
    modify_wait_ch: crossbeam_channel::Sender<()>,
    runtime_info: Arc<CollectedRuntimeInfo>,
    test_modify_when_subtask_run: Arc<std::sync::atomic::AtomicBool>,
    c: Arc<testutil::TestDxfContext>,
    _registered: testutil::RegisteredTaskType,
}

/// Go `prepareModifyTaskTest`.
fn prepare_modify_task_test(task_mgr: Arc<TaskManager>, node_count: usize) -> ModifyTaskTest {
    let c = testutil::new_test_dxf_context(task_mgr, node_count, 16, true);
    let scheduler_ext = testutil::get_mock_scheduler_ext(
        &c.mock_ctrl,
        testutil::SchedulerInfo {
            all_error_retryable: true,
            step_infos: vec![testutil::StepInfo::new(STEP_ONE, 2), testutil::StepInfo::new(STEP_TWO, 3)],
        },
    );
    let (subtask_ch, subtask_rx) = crossbeam_channel::bounded::<()>(0);
    let runtime_info = Arc::new(CollectedRuntimeInfo::default());
    let test_modify_when_subtask_run = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (modify_wait_ch, modify_wait_rx) = crossbeam_channel::bounded::<()>(0);

    let run_subtask_fn: RunSubtaskFn = {
        let (runtime_info, test_modify_when_subtask_run) = (Arc::clone(&runtime_info), Arc::clone(&test_modify_when_subtask_run));
        Arc::new(move |ctx, subtask| {
            runtime_info.active_subtask_count.fetch_add(1, Ordering::SeqCst);
            struct Done<'a>(&'a std::sync::atomic::AtomicI64);
            impl Drop for Done<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let _done = Done(&runtime_info.active_subtask_count);
            if test_modify_when_subtask_run.load(Ordering::SeqCst) {
                let _ = modify_wait_rx.recv();
                let _ = modify_wait_rx.recv();
            }
            crossbeam_channel::select! {
                recv(subtask_rx) -> _ => {
                    runtime_info.subtask_infos.lock().unwrap().push((subtask.base.step, subtask.base.concurrency));
                }
                recv(ctx.done()) -> _ => return Err(Arc::new(ctx.err().expect("the context ended"))),
            }
            Ok(())
        })
    };

    let ctrl = Arc::clone(&c.mock_ctrl);
    let info = Arc::clone(&runtime_info);
    let executor_ext = testutil::get_common_task_executor_ext(&c.mock_ctrl, move |task| {
        *info.curr_task_meta.lock().unwrap() = Some(task.meta.clone());
        info.curr_task_concurrency.store(task.base.required_slots as i64, Ordering::SeqCst);
        let executor = MockStepExecutor::new(&ctrl);
        executor.ret_times("Init", Times::Any, StepResult::Ok(()));
        let run_subtask_fn = Arc::clone(&run_subtask_fn);
        executor.expect("RunSubtask|*", Times::Any, move |input| {
            let (ctx, subtask) = input.downcast_ref::<(Context, Subtask)>().expect("RunSubtask gets (ctx, subtask)");
            run_subtask_fn(ctx, subtask)
        });
        executor.ret_times("GetStep", Times::Any, task.base.step);
        executor.ret_times("Cleanup", Times::Any, StepResult::Ok(()));
        executor.ret_times("RealtimeSummary", Times::Any, None::<Arc<tidb_dxf_execute::SubtaskSummary>>);
        let meta_info = Arc::clone(&info);
        executor.expect("TaskMetaModified", Times::Any, move |input| -> StepResult {
            *meta_info.curr_task_meta.lock().unwrap() = Some(input.downcast_ref::<Vec<u8>>().expect("the new meta").clone());
            Ok(())
        });
        let resource_info = Arc::clone(&info);
        executor.expect("ResourceModified", Times::Any, move |input| -> StepResult {
            let (cpu, _) = input.downcast_ref::<(i64, i64)>().expect("the new resource");
            resource_info.curr_task_concurrency.store(*cpu, Ordering::SeqCst);
            Ok(())
        });
        Ok(executor as Arc<dyn StepExecutor>)
    });
    let registered = testutil::register_example_task(scheduler_ext, executor_ext, testutil::get_common_cleaner(&c.mock_ctrl));
    ModifyTaskTest { subtask_ch, modify_wait_ch, runtime_info, test_modify_when_subtask_run, c, _registered: registered }
}

/// Waits until the task's executor started: a subtask runs or has run. Go's
/// running-task modifications race the executor manager, which starts an
/// executor only for a `running` task, against the scheduler's next tick,
/// after which the hook moves the task to `modifying`: if no executor started
/// by then, no subtask ever receives the test's completions. Go's test means
/// the executor to have started when the modification lands; a slow debug
/// build under load loses the race.
fn wait_for_running_subtask(runtime_info: &CollectedRuntimeInfo) {
    eventually(Duration::from_secs(10), Duration::from_millis(10), || {
        runtime_info.active_subtask_count.load(Ordering::SeqCst) >= 1
            || !runtime_info.subtask_infos.lock().unwrap().is_empty()
    });
}

fn modify_required_slots(prev_state: TaskState, to: i64) -> ModifyParam {
    ModifyParam { prev_state, modifications: vec![Modification { tp: MODIFY_REQUIRED_SLOTS, to }] }
}

fn finish_subtasks(subtask_ch: &crossbeam_channel::Sender<()>, n: usize) {
    for _ in 0..n {
        subtask_ch.send(()).unwrap();
    }
}

/// Go `TestModifyTaskConcurrencyAndMeta`.
#[test]
fn test_modify_task_concurrency_and_meta() {
    let (_stack, task_mgr) = store_and_task_manager();
    let t = prepare_modify_task_test(task_mgr, 1);
    let (c, runtime_info, subtask_ch) = (&t.c, &t.runtime_info, &t.subtask_ch);
    let scope = tidb_dxf_handle::get_target_scope();
    let subtask_infos = || runtime_info.subtask_infos.lock().unwrap().clone();
    let all = |concurrencies: [isize; 5]| -> Vec<(Step, isize)> {
        [STEP_ONE, STEP_ONE, STEP_TWO, STEP_TWO, STEP_TWO].into_iter().zip(concurrencies).collect()
    };

    // modify pending task concurrency
    {
        let (modify_sync_ch, modify_sync_rx) = crossbeam_channel::bounded::<()>(0);
        let the_task = Arc::new(std::sync::Mutex::new(None::<Task>));
        {
            let (once, task_mgr, scope, the_task) =
                (std::sync::Once::new(), Arc::clone(&c.task_mgr), scope.clone(), Arc::clone(&the_task));
            *tidb_dxf_scheduler::BEFORE_GET_SCHEDULABLE_TASKS.write().unwrap() = Some(Arc::new(move || {
                once.call_once(|| {
                    let task = tidb_dxf_handle::submit_task("k1", &TASK_TYPE_EXAMPLE, "", 3, &scope, 0, b"init").unwrap();
                    assert_eq!(task.base.required_slots, 3);
                    task_mgr.modify_task_by_id(task.base.id, &modify_required_slots(TASK_STATE_PENDING, 7)).unwrap();
                    let got_task = task_mgr.get_task_base_by_id(task.base.id).unwrap();
                    assert_eq!(got_task.state, TASK_STATE_MODIFYING);
                    assert_eq!(got_task.required_slots, 3);
                    *the_task.lock().unwrap() = Some(task);
                    let _ = modify_sync_rx.recv();
                });
            }));
        }
        modify_sync_ch.send(()).unwrap();
        // finish subtasks
        finish_subtasks(subtask_ch, 5);
        let key = the_task.lock().unwrap().clone().unwrap().base.key;
        let task2_base = testutil::wait_task_done(&c.ctx, &key);
        assert_eq!(task2_base.state, TASK_STATE_SUCCEED);
        assert_eq!(subtask_infos(), all([7, 7, 7, 7, 7]));
        *tidb_dxf_scheduler::BEFORE_GET_SCHEDULABLE_TASKS.write().unwrap() = None;
        runtime_info.reset();
    }

    // modify running task concurrency at step two
    {
        let (modify_sync_ch, modify_sync_rx) = crossbeam_channel::bounded::<()>(0);
        {
            let (once, task_mgr, runtime_info) = (std::sync::Once::new(), Arc::clone(&c.task_mgr), Arc::clone(runtime_info));
            *tidb_dxf_scheduler::BEFORE_REFRESH_TASK.write().unwrap() = Some(Arc::new(move |task: &Task| {
                if task.base.state != TASK_STATE_RUNNING && task.base.step != STEP_TWO {
                    return;
                }
                once.call_once(|| {
                    wait_for_running_subtask(&runtime_info);
                    task_mgr.modify_task_by_id(task.base.id, &modify_required_slots(TASK_STATE_RUNNING, 7)).unwrap();
                    let _ = modify_sync_rx.recv();
                });
            }));
        }
        let task = tidb_dxf_handle::submit_task("k2", &TASK_TYPE_EXAMPLE, "", 3, &scope, 0, &[]).unwrap();
        assert_eq!(task.base.required_slots, 3);
        // finish StepOne
        finish_subtasks(subtask_ch, 2);
        // wait task move to 'modifying' state
        modify_sync_ch.send(()).unwrap();
        // wait task move back to 'running' state
        eventually(Duration::from_secs(10), Duration::from_millis(100), || {
            c.task_mgr.get_task_by_id(task.base.id).unwrap().base.state == TASK_STATE_RUNNING
        });
        // finish StepTwo
        finish_subtasks(subtask_ch, 3);
        let task2_base = testutil::wait_task_done(&c.ctx, &task.base.key);
        assert_eq!(task2_base.state, TASK_STATE_SUCCEED);
        assert_eq!(subtask_infos(), all([3, 3, 7, 7, 7]));
        *tidb_dxf_scheduler::BEFORE_REFRESH_TASK.write().unwrap() = None;
        runtime_info.reset();
    }

    // modify running task concurrency at second subtask of step two
    {
        {
            let (task_mgr, runtime_info) = (Arc::clone(&c.task_mgr), Arc::clone(runtime_info));
            *tidb_dxf_taskexecutor::BEFORE_GET_TASK_BY_ID_IN_RUN.write().unwrap() = Some(Arc::new(move |task_id| {
                if runtime_info.subtask_infos.lock().unwrap().len() == 3 {
                    task_mgr.modify_task_by_id(task_id, &modify_required_slots(TASK_STATE_RUNNING, 7)).unwrap();
                    // wait task move back to 'running' state
                    eventually(Duration::from_secs(10), Duration::from_millis(100), || {
                        task_mgr.get_task_by_id(task_id).unwrap().base.state == TASK_STATE_RUNNING
                    });
                }
            }));
        }
        let task = tidb_dxf_handle::submit_task("k2-2", &TASK_TYPE_EXAMPLE, "", 3, &scope, 0, &[]).unwrap();
        assert_eq!(task.base.required_slots, 3);
        finish_subtasks(subtask_ch, 5);
        let task2_base = testutil::wait_task_done(&c.ctx, &task.base.key);
        assert_eq!(task2_base.state, TASK_STATE_SUCCEED);
        assert_eq!(subtask_infos(), all([3, 3, 3, 7, 7]));
        *tidb_dxf_taskexecutor::BEFORE_GET_TASK_BY_ID_IN_RUN.write().unwrap() = None;
        runtime_info.reset();
    }

    // modify paused task concurrency
    {
        let (sync_ch, sync_rx) = crossbeam_channel::bounded::<()>(0);
        let the_task = Arc::new(std::sync::Mutex::new(None::<Task>));
        {
            let (once, task_mgr, scope, the_task) =
                (std::sync::Once::new(), Arc::clone(&c.task_mgr), scope.clone(), Arc::clone(&the_task));
            *tidb_dxf_scheduler::BEFORE_GET_SCHEDULABLE_TASKS.write().unwrap() = Some(Arc::new(move || {
                once.call_once(|| {
                    let task = tidb_dxf_handle::submit_task("k3", &TASK_TYPE_EXAMPLE, "", 3, &scope, 0, &[]).unwrap();
                    assert_eq!(task.base.required_slots, 3);
                    assert!(task_mgr.pause_task(&task.base.key).unwrap());
                    *the_task.lock().unwrap() = Some(task);
                    let _ = sync_rx.recv();
                });
            }));
        }
        sync_ch.send(()).unwrap();
        let the_task = the_task.lock().unwrap().clone().unwrap();
        let task_base = testutil::wait_task_done_or_paused(&c.ctx, &the_task.base.key);
        assert_eq!(task_base.state, TASK_STATE_PAUSED);
        c.task_mgr.modify_task_by_id(the_task.base.id, &modify_required_slots(TASK_STATE_PAUSED, 7)).unwrap();
        let task_base = testutil::wait_task_done_or_paused(&c.ctx, &the_task.base.key);
        assert_eq!(task_base.state, TASK_STATE_PAUSED);
        assert!(c.task_mgr.resume_task(&the_task.base.key).unwrap());
        // finish subtasks
        finish_subtasks(subtask_ch, 5);
        let task2_base = testutil::wait_task_done(&c.ctx, &the_task.base.key);
        assert_eq!(task2_base.state, TASK_STATE_SUCCEED);
        assert_eq!(subtask_infos(), all([7, 7, 7, 7, 7]));
        *tidb_dxf_scheduler::BEFORE_GET_SCHEDULABLE_TASKS.write().unwrap() = None;
        runtime_info.reset();
    }

    // modify pending task concurrency, but other owner already done it
    {
        let (modify_sync_ch, modify_sync_rx) = crossbeam_channel::bounded::<()>(0);
        let the_task = Arc::new(std::sync::Mutex::new(None::<Task>));
        {
            let (once, task_mgr, scope, the_task) =
                (std::sync::Once::new(), Arc::clone(&c.task_mgr), scope.clone(), Arc::clone(&the_task));
            *tidb_dxf_scheduler::BEFORE_GET_SCHEDULABLE_TASKS.write().unwrap() = Some(Arc::new(move || {
                once.call_once(|| {
                    let task = tidb_dxf_handle::submit_task("k4", &TASK_TYPE_EXAMPLE, "", 3, &scope, 0, &[]).unwrap();
                    assert_eq!(task.base.required_slots, 3);
                    task_mgr.modify_task_by_id(task.base.id, &modify_required_slots(TASK_STATE_PENDING, 7)).unwrap();
                    let got_task = task_mgr.get_task_base_by_id(task.base.id).unwrap();
                    assert_eq!(got_task.state, TASK_STATE_MODIFYING);
                    assert_eq!(got_task.required_slots, 3);
                    *the_task.lock().unwrap() = Some(task);
                });
            }));
        }
        {
            let (once, task_mgr) = (std::sync::Once::new(), Arc::clone(&c.task_mgr));
            *tidb_dxf_scheduler::AFTER_REFRESH_TASK.write().unwrap() = Some(Arc::new(move |task: &Task| {
                once.call_once(|| {
                    assert_eq!(task.base.state, TASK_STATE_MODIFYING);
                    let mut task_clone = task.clone();
                    task_clone.base.required_slots = 7;
                    task_mgr.modified_task(&task_clone).unwrap();
                    let got_task = task_mgr.get_task_base_by_id(task.base.id).unwrap();
                    assert_eq!(got_task.state, TASK_STATE_PENDING);
                    let _ = modify_sync_rx.recv();
                });
            }));
        }
        modify_sync_ch.send(()).unwrap();
        // finish subtasks
        finish_subtasks(subtask_ch, 5);
        let key = the_task.lock().unwrap().clone().unwrap().base.key;
        let task2_base = testutil::wait_task_done(&c.ctx, &key);
        assert_eq!(task2_base.state, TASK_STATE_SUCCEED);
        assert_eq!(subtask_infos(), all([7, 7, 7, 7, 7]));
        *tidb_dxf_scheduler::BEFORE_GET_SCHEDULABLE_TASKS.write().unwrap() = None;
        *tidb_dxf_scheduler::AFTER_REFRESH_TASK.write().unwrap() = None;
        runtime_info.reset();
    }

    // modify pending task meta, only check the scheduler part
    {
        let (modify_sync_ch, modify_sync_rx) = crossbeam_channel::bounded::<()>(0);
        let the_task = Arc::new(std::sync::Mutex::new(None::<Task>));
        {
            let (once, task_mgr, scope, the_task) =
                (std::sync::Once::new(), Arc::clone(&c.task_mgr), scope.clone(), Arc::clone(&the_task));
            *tidb_dxf_scheduler::BEFORE_GET_SCHEDULABLE_TASKS.write().unwrap() = Some(Arc::new(move || {
                once.call_once(|| {
                    let task = tidb_dxf_handle::submit_task("k5", &TASK_TYPE_EXAMPLE, "", 3, &scope, 0, b"init").unwrap();
                    assert_eq!(task.base.required_slots, 3);
                    assert_eq!(task.meta, b"init");
                    let param = ModifyParam {
                        prev_state: TASK_STATE_PENDING,
                        modifications: vec![Modification { tp: MODIFY_MAX_WRITE_SPEED, to: 123 }],
                    };
                    task_mgr.modify_task_by_id(task.base.id, &param).unwrap();
                    let got_task = task_mgr.get_task_base_by_id(task.base.id).unwrap();
                    assert_eq!(got_task.state, TASK_STATE_MODIFYING);
                    assert_eq!(got_task.required_slots, 3);
                    *the_task.lock().unwrap() = Some(task);
                    let _ = modify_sync_rx.recv();
                });
            }));
        }
        modify_sync_ch.send(()).unwrap();
        // finish subtasks
        finish_subtasks(subtask_ch, 5);
        let key = the_task.lock().unwrap().clone().unwrap().base.key;
        let task2_base = testutil::wait_task_done(&c.ctx, &key);
        assert_eq!(task2_base.state, TASK_STATE_SUCCEED);
        assert_eq!(runtime_info.meta(), b"modify_max_write_speed=123");
        *tidb_dxf_scheduler::BEFORE_GET_SCHEDULABLE_TASKS.write().unwrap() = None;
        runtime_info.reset();
    }

    // modify meta and increase/decrease concurrency when subtask is running, and apply success
    for (key, slots, to_slots, speed) in [("k6", 3, 7, 123), ("k7", 9, 5, 456)] {
        t.test_modify_when_subtask_run.store(true, Ordering::SeqCst);
        let task = tidb_dxf_handle::submit_task(key, &TASK_TYPE_EXAMPLE, "", slots, &scope, 0, b"init").unwrap();
        assert_eq!(task.base.required_slots, slots);
        assert_eq!(task.meta, b"init");
        t.modify_wait_ch.send(()).unwrap();
        let param = ModifyParam {
            prev_state: TASK_STATE_RUNNING,
            modifications: vec![
                Modification { tp: MODIFY_REQUIRED_SLOTS, to: to_slots },
                Modification { tp: MODIFY_MAX_WRITE_SPEED, to: speed },
            ],
        };
        c.task_mgr.modify_task_by_id(task.base.id, &param).unwrap();
        let expected_meta = format!("modify_max_write_speed={speed}").into_bytes();
        eventually(Duration::from_secs(10), Duration::from_millis(100), || {
            runtime_info.meta() == expected_meta && runtime_info.curr_task_concurrency.load(Ordering::SeqCst) == to_slots
        });
        t.test_modify_when_subtask_run.store(false, Ordering::SeqCst);
        t.modify_wait_ch.send(()).unwrap();
        // finish subtasks
        finish_subtasks(subtask_ch, 5);
        let task2_base = testutil::wait_task_done(&c.ctx, &task.base.key);
        assert_eq!(task2_base.state, TASK_STATE_SUCCEED);
        assert_eq!(runtime_info.meta(), expected_meta);
        assert_eq!(runtime_info.curr_task_concurrency.load(Ordering::SeqCst), to_slots);
        runtime_info.reset();
    }

    // modify running task max node count
    {
        let (modify_sync_ch, modify_sync_rx) = crossbeam_channel::bounded::<()>(0);
        {
            let (once, task_mgr, runtime_info) = (std::sync::Once::new(), Arc::clone(&c.task_mgr), Arc::clone(runtime_info));
            *tidb_dxf_scheduler::BEFORE_REFRESH_TASK.write().unwrap() = Some(Arc::new(move |task: &Task| {
                if task.base.state != TASK_STATE_RUNNING && task.base.step != STEP_ONE {
                    return;
                }
                once.call_once(|| {
                    wait_for_running_subtask(&runtime_info);
                    let param = ModifyParam {
                        prev_state: TASK_STATE_RUNNING,
                        modifications: vec![Modification { tp: MODIFY_MAX_NODE_COUNT, to: 200 }],
                    };
                    task_mgr.modify_task_by_id(task.base.id, &param).unwrap();
                    let _ = modify_sync_rx.recv();
                });
            }));
        }
        let task = tidb_dxf_handle::submit_task("k8", &TASK_TYPE_EXAMPLE, "", 3, &scope, 1, &[]).unwrap();
        assert_eq!(task.base.required_slots, 3);
        assert_eq!(task.base.max_node_count, 1);
        // finish StepOne
        finish_subtasks(subtask_ch, 2);
        // wait task move to 'modifying' state
        modify_sync_ch.send(()).unwrap();
        // wait task move back to 'running' state, and modified
        eventually(Duration::from_secs(10), Duration::from_millis(100), || {
            let got_task = c.task_mgr.get_task_by_id(task.base.id).unwrap();
            got_task.base.state == TASK_STATE_RUNNING && got_task.base.max_node_count == 200
        });
        // finish StepTwo
        finish_subtasks(subtask_ch, 3);
        let task2_base = testutil::wait_task_done(&c.ctx, &task.base.key);
        assert_eq!(task2_base.state, TASK_STATE_SUCCEED);
        *tidb_dxf_scheduler::BEFORE_REFRESH_TASK.write().unwrap() = None;
        runtime_info.reset();
    }
}

/// Go `TestModifyTaskMaxNodeCountForSubtaskBalance`.
#[test]
fn test_modify_task_max_node_count_for_subtask_balance() {
    let (stack, task_mgr) = store_and_task_manager();
    let t = prepare_modify_task_test(task_mgr, 3);
    let (c, runtime_info, subtask_ch) = (&t.c, &t.runtime_info, &t.subtask_ch);
    let scope = tidb_dxf_handle::get_target_scope();

    // modify running task max node count, task can use more node after balance
    let (modify_sync_ch, modify_sync_rx) = crossbeam_channel::bounded::<()>(0);
    {
        let (once, task_mgr) = (std::sync::Once::new(), Arc::clone(&c.task_mgr));
        *tidb_dxf_scheduler::BEFORE_REFRESH_TASK.write().unwrap() = Some(Arc::new(move |task: &Task| {
            if task.base.state != TASK_STATE_RUNNING && task.base.step != STEP_ONE {
                return;
            }
            once.call_once(|| {
                let _ = modify_sync_rx.recv();
                let param = ModifyParam {
                    prev_state: TASK_STATE_RUNNING,
                    modifications: vec![Modification { tp: MODIFY_MAX_NODE_COUNT, to: 2 }],
                };
                task_mgr.modify_task_by_id(task.base.id, &param).unwrap();
                let _ = modify_sync_rx.recv();
            });
        }));
    }
    let task = tidb_dxf_handle::submit_task("k8", &TASK_TYPE_EXAMPLE, "", 3, &scope, 1, &[]).unwrap();
    assert_eq!(task.base.required_slots, 3);
    assert_eq!(task.base.max_node_count, 1);
    let mut session = stack.factory.open_session(session_context(4004)).unwrap();
    let exec_ids = |session: &mut Session| {
        displayed(rows(
            session,
            &format!("select count(distinct exec_id) from mysql.tidb_background_subtask where task_key = '{}'", task.base.id),
        ))
    };
    // only 1 subtask can be running at the same time
    eventually(Duration::from_secs(10), Duration::from_millis(100), || {
        runtime_info.active_subtask_count.load(Ordering::SeqCst) == 1
    });
    assert_eq!(exec_ids(&mut session), vec![vec!["1".to_owned()]]);
    // task move to 'modifying' state
    modify_sync_ch.send(()).unwrap();
    modify_sync_ch.send(()).unwrap();
    // wait task move back to 'running' state, and modified
    eventually(Duration::from_secs(10), Duration::from_millis(100), || {
        let got_task = c.task_mgr.get_task_by_id(task.base.id).unwrap();
        got_task.base.state == TASK_STATE_RUNNING && got_task.base.max_node_count == 2
    });
    // now 2 subtask can be running at the same time
    eventually(Duration::from_secs(10), Duration::from_millis(100), || {
        runtime_info.active_subtask_count.load(Ordering::SeqCst) == 2
    });
    assert_eq!(exec_ids(&mut session), vec![vec!["2".to_owned()]]);
    // finish StepOne
    finish_subtasks(subtask_ch, 2);
    // finish StepTwo
    finish_subtasks(subtask_ch, 3);
    let task2_base = testutil::wait_task_done(&c.ctx, &task.base.key);
    assert_eq!(task2_base.state, TASK_STATE_SUCCEED);
    *tidb_dxf_scheduler::BEFORE_REFRESH_TASK.write().unwrap() = None;
}

/// Go `TestScopeBasic`. Go's test store publishes `:4000` (its mock
/// infosync), which is also the first DXF node, so the session's
/// `SET GLOBAL tidb_service_scope` writes that node's role; the stack here
/// publishes port 4000 for the same exec ID.
#[test]
fn test_scope_basic() {
    if tidb_config_kerneltype::is_next_gen() {
        // target scope in nextgen is fixed to 'dxf_service'
        return;
    }
    let (stack, _users) = cop_backed_stack_with_config(|config| {
        config.port = 4000;
        config.global_config.port = 4000;
    });
    let task_mgr = Arc::new(TaskManager::new(stack.factory.dxf_session_pool()));
    let node_cnt = 3;
    let c = testutil::new_test_dxf_context(task_mgr, node_cnt, 16, true);

    let _registered =
        register_example_task(&c.mock_ctrl, get_mock_basic_scheduler_ext_for_scope(&c.mock_ctrl, 3), &c.test_context, None);
    let mut tk = stack.factory.open_session(session_context(4005)).unwrap();
    let role = |tk: &mut Session, host: &str| {
        displayed(rows(tk, &format!(r#"select role from mysql.dist_framework_meta where host="{host}""#)))
    };

    // 1. all "" role.
    let task_id = submit_task_and_check_success_for_scope(&c.ctx, "😁", node_cnt, "", &c.test_context);

    check_subtask_on_nodes(task_id, vec![":4000".to_owned(), ":4001".to_owned(), ":4002".to_owned()]);
    for host in [":4000", ":4001", ":4002"] {
        assert_eq!(role(&mut tk, host), [[""]]);
    }

    // 2. one "background" role.
    rows(&mut tk, "set global tidb_service_scope=background");
    assert_eq!(displayed(rows(&mut tk, "select @@global.tidb_service_scope")), [["background"]]);
    assert_eq!(displayed(rows(&mut tk, "select @@tidb_service_scope")), [["background"]]);

    let wait_sync_refresh = || {
        let (ch_tx, ch_rx) = crossbeam_channel::bounded::<()>(0);
        let counter = AtomicI32::new(0);
        *tidb_dxf_scheduler::SYNC_REFRESH.write().unwrap() = Some(Arc::new(move || {
            if counter.fetch_add(1, Ordering::SeqCst) + 1 == 1 {
                let _ = ch_tx.send(());
            }
        }));
        ch_rx.recv().unwrap();
    };
    wait_sync_refresh();
    let task_id = submit_task_and_check_success_for_scope(&c.ctx, "😊", node_cnt, "background", &c.test_context);

    assert_eq!(role(&mut tk, ":4000"), [["background"]]);
    assert_eq!(role(&mut tk, ":4001"), [[""]]);
    assert_eq!(role(&mut tk, ":4002"), [[""]]);
    check_subtask_on_nodes(task_id, vec![":4000".to_owned()]);

    // 3. 2 "background" role.
    rows(&mut tk, r#"update mysql.dist_framework_meta set role = "background" where host = ":4001""#);
    std::thread::sleep(Duration::from_secs(5));
    wait_sync_refresh();
    let task_id = submit_task_and_check_success_for_scope(&c.ctx, "😆", node_cnt, "background", &c.test_context);
    check_subtask_on_nodes(task_id, vec![":4000".to_owned(), ":4001".to_owned()]);
    assert_eq!(role(&mut tk, ":4000"), [["background"]]);
    assert_eq!(role(&mut tk, ":4001"), [["background"]]);
    assert_eq!(role(&mut tk, ":4002"), [[""]]);

    *tidb_dxf_scheduler::SYNC_REFRESH.write().unwrap() = None;
    rows(&mut tk, "set global tidb_service_scope=''");
}
