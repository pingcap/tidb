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

//! Go `pkg/dxf/framework/scheduler`'s tests that need a store
//! (`scheduler_test.go`, `scheduler_manager_test.go`), over the embedded
//! unistore node. The node
//! publishes port 4000, so its exec ID is Go's mock infosync `:4000`.

use super::*;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tidb_dxf::*;
use tidb_dxf_mock::{MockCleaner, MockScheduler, MockSchedulerExtension};
use tidb_dxf_scheduler::Manager;
use tidb_dxf_storage::{set_task_manager, TaskManager};
use tidb_dxf_testutil as testutil;
use tidb_gocontext::Context;
use tidb_gomock::{Controller, Times};
use tidb_sqlescape::SqlArg;

const SUBTASK_CNT: usize = 3;

type SchedulerError = tidb_dxf_scheduler::Error;

fn err(message: &str) -> SchedulerError {
    Arc::new(std::io::Error::other(message.to_owned()))
}

/// Go `testkit.CreateMockStore` with `MockDisableDistTask` (the test stacks
/// do not start the DXF loop); the node's exec ID is `:4000`.
fn mock_store() -> UnistoreClusterStack {
    let (stack, _users) = cop_backed_stack_with_config(|config| {
        config.port = 4000;
        config.global_config.port = 4000;
    });
    stack
}

/// Go `require.Eventually`.
fn eventually(timeout: Duration, tick: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + timeout;
    while !condition() {
        assert!(std::time::Instant::now() < deadline, "the condition was not met in {timeout:?}");
        std::thread::sleep(tick);
    }
}

/// Go `scheduler.GetTestSchedulerExt`.
fn get_test_scheduler_ext(ctrl: &Arc<Controller>) -> Arc<MockSchedulerExtension> {
    let mock_scheduler = MockSchedulerExtension::new(ctrl);
    mock_scheduler.ret_times("OnTick", Times::Any, ());
    mock_scheduler.ret_times("GetEligibleInstances", Times::Any, Ok::<Vec<String>, SchedulerError>(Vec::new()));
    mock_scheduler.ret_times("IsRetryableErr", Times::Any, true);
    mock_scheduler.ret_times("GetNextStep", Times::Any, STEP_DONE);
    mock_scheduler.ret_times("OnNextSubtasksBatch", Times::Any, Ok::<Vec<Vec<u8>>, SchedulerError>(Vec::new()));
    mock_scheduler.ret_times("OnDone", Times::Any, Ok::<(), SchedulerError>(()));
    mock_scheduler
}

/// Go `getNumberExampleSchedulerExt`.
fn get_number_example_scheduler_ext(ctrl: &Arc<Controller>) -> Arc<MockSchedulerExtension> {
    let mock_scheduler = MockSchedulerExtension::new(ctrl);
    mock_scheduler.ret_times("OnTick", Times::Any, ());
    mock_scheduler.ret_times("GetEligibleInstances", Times::Any, Ok::<Vec<String>, SchedulerError>(Vec::new()));
    mock_scheduler.ret_times("IsRetryableErr", Times::Any, true);
    mock_scheduler.expect("GetNextStep", Times::Any, |input| {
        if input.downcast_ref::<TaskBase>().expect("GetNextStep gets the task").step == STEP_INIT {
            STEP_ONE
        } else {
            STEP_DONE
        }
    });
    mock_scheduler.expect("OnNextSubtasksBatch", Times::Any, |input| {
        let (task, _, _) =
            input.downcast_ref::<(Mutex<Task>, Vec<String>, Step)>().expect("OnNextSubtasksBatch gets (task, exec IDs, step)");
        let step = task.lock().unwrap().base.step;
        if step == STEP_INIT {
            let metas = vec![vec![b'1']; SUBTASK_CNT];
            tidb_logutil::bg_logger().info("progress step init", &[]);
            Ok::<Vec<Vec<u8>>, SchedulerError>(metas)
        } else if step == STEP_ONE {
            tidb_logutil::bg_logger().info("progress step one", &[]);
            Ok(Vec::new())
        } else {
            Err(err("unknown step"))
        }
    });

    mock_scheduler.ret_times("OnDone", Times::Any, Ok::<(), SchedulerError>(()));
    mock_scheduler
}

/// Go `MockSchedulerManager`; Go's `"etcd"` context value reads the server
/// infos from the node's infosync.
fn mock_scheduler_manager(
    stack: &UnistoreClusterStack,
    ext: Option<Arc<dyn tidb_dxf_scheduler::Extension>>,
    cleaner: Option<Arc<dyn tidb_dxf_scheduler::Cleaner>>,
) -> (Arc<Manager>, Arc<TaskManager>) {
    let mgr = Arc::new(TaskManager::new(stack.factory.dxf_session_pool()));
    set_task_manager(Some(Arc::clone(&mgr)));
    let sch = Manager::new(
        &Context::background(),
        Arc::clone(&mgr) as Arc<dyn tidb_dxf_scheduler::TaskManager>,
        "host:port",
        *NODE_RESOURCE_FOR_TEST,
        stack.factory.server_info().expect("the node publishes its server info"),
    );
    tidb_dxf_scheduler::register_scheduler_factory(
        TASK_TYPE_EXAMPLE,
        Arc::new(move |ctx, task, param| {
            let mock_scheduler = Arc::new(tidb_dxf_scheduler::BaseScheduler::new(ctx, task, param));
            if let Some(ext) = &ext {
                mock_scheduler.set_extension(Arc::downgrade(ext));
            }
            mock_scheduler
        }),
    );
    if let Some(cleaner) = cleaner {
        tidb_dxf_scheduler::register_cleaner_factory(TASK_TYPE_EXAMPLE, Arc::new(move || Arc::clone(&cleaner)));
    }
    (sch, mgr)
}

/// Go `deleteTasks`.
fn delete_tasks(stack: &UnistoreClusterStack, task_id: i64) {
    let mut tk = stack.factory.open_session(session_context(420)).unwrap();
    rows(&mut tk, &format!("delete from mysql.tidb_global_task where id = {task_id}"));
}

/// Go `TestTaskFailInManager`.
#[test]
fn test_task_fail_in_manager() {
    let stack = mock_store();
    let ctrl = Controller::new();

    let (sch_manager, mgr) = mock_scheduler_manager(&stack, Some(get_test_scheduler_ext(&ctrl)), None);
    mgr.init_meta(":4000", &tidb_dxf_handle::get_target_scope()).unwrap();
    let factory_ctrl = Arc::clone(&ctrl);
    tidb_dxf_scheduler::register_scheduler_factory(
        TASK_TYPE_EXAMPLE,
        Arc::new(move |_, _, _| {
            let mock_scheduler = MockScheduler::new(&factory_ctrl);
            mock_scheduler.ret("Init", Err::<(), SchedulerError>(err("mock scheduler init error")));
            mock_scheduler
        }),
    );
    sch_manager.start();

    // unknown task type
    let task_id = mgr.create_task("test", &TaskType::new("test-type"), "", 1, "", 0, &ExtraParams::default(), &[]).unwrap();
    eventually(Duration::from_secs(10), Duration::from_millis(300), || {
        let task = mgr.get_task_by_id(task_id).unwrap();
        task.base.state == TASK_STATE_FAILED
            && task.error.as_ref().is_some_and(|err| err.to_string().contains("unknown task type"))
    });

    // scheduler init error
    let ks = if tidb_config_kerneltype::is_next_gen() { tidb_keyspace::SYSTEM } else { "" };
    let task_id = mgr.create_task("test2", &TASK_TYPE_EXAMPLE, ks, 1, "", 0, &ExtraParams::default(), &[]).unwrap();
    eventually(Duration::from_secs(10), Duration::from_millis(300), || {
        let task = mgr.get_task_by_id(task_id).unwrap();
        task.base.state == TASK_STATE_FAILED
            && task.error.as_ref().is_some_and(|err| err.to_string().contains("mock scheduler init error"))
    });

    sch_manager.stop();
    ctrl.finish();
}

/// Go `checkSchedule`'s deferred `sch.Stop()`, `restoreMaxConcurrentTask()`
/// and `ctrl.Finish()`.
struct StopScheduler {
    sch: Arc<Manager>,
    restore_max_concurrent_task: Option<Box<dyn FnOnce()>>,
    ctrl: Arc<Controller>,
}

impl Drop for StopScheduler {
    fn drop(&mut self) {
        self.sch.stop();
        if let Some(restore) = self.restore_max_concurrent_task.take() {
            restore();
        }
        if !std::thread::panicking() {
            self.ctrl.finish();
        }
    }
}

/// Go `checkSchedule`.
fn check_schedule(task_cnt: usize, is_succ: bool, is_cancel: bool, is_subtask_cancel: bool, is_pause_and_resume: bool) {
    // test scheduleTaskLoop
    // test parallelism control
    let restore_max_concurrent_task: Option<Box<dyn FnOnce()>> =
        (task_cnt == 1).then(|| Box::new(set_max_concurrent_task_for_test(1)) as Box<dyn FnOnce()>);

    let stack = mock_store();
    let ctrl = Controller::new();

    let keyspace = "";
    let scope = tidb_dxf_handle::get_target_scope();
    let (sch, mgr) = mock_scheduler_manager(&stack, Some(get_number_example_scheduler_ext(&ctrl)), None);
    mgr.init_meta(":4000", &scope).unwrap();
    sch.start();
    let _stop = StopScheduler { sch: Arc::clone(&sch), restore_max_concurrent_task, ctrl: Arc::clone(&ctrl) };

    // 3s
    let check_get_running_task_cnt = |expected: usize| {
        eventually(Duration::from_secs(5), Duration::from_millis(50), || sch.get_running_task_cnt() == expected);
    };

    let check_task_running_cnt = || {
        let mut tasks = Vec::new();
        eventually(Duration::from_secs(5), Duration::from_millis(50), || {
            tasks = mgr.get_task_bases_in_states(&[TASK_STATE_RUNNING]).unwrap();
            tasks.len() == task_cnt
        });
        tasks
    };

    let check_subtask_cnt = |tasks: &[TaskBase], task_ids: &[i64]| {
        for (i, &task_id) in task_ids.iter().enumerate() {
            assert_eq!(task_id, tasks[i].id);
            eventually(Duration::from_secs(5), Duration::from_millis(50), || {
                let cnt_by_states = mgr.get_subtask_cnt_group_by_states(task_id, STEP_ONE).unwrap();
                SUBTASK_CNT as i64 == cnt_by_states.get(&SUBTASK_STATE_PENDING).copied().unwrap_or(0)
            });
        }
    };

    // Mock add tasks.
    let mut task_ids = Vec::with_capacity(task_cnt);
    for i in 0..task_cnt {
        let task_id =
            mgr.create_task(&i.to_string(), &TASK_TYPE_EXAMPLE, keyspace, 0, &scope, 0, &ExtraParams::default(), &[]).unwrap();
        task_ids.push(task_id);
    }
    // test OnNextSubtasksBatch.
    check_get_running_task_cnt(task_cnt);
    let tasks = check_task_running_cnt();
    check_subtask_cnt(&tasks, &task_ids);
    // test parallelism control
    if task_cnt == 1 {
        let task_id = mgr
            .create_task(&task_cnt.to_string(), &TASK_TYPE_EXAMPLE, keyspace, 0, &scope, 0, &ExtraParams::default(), &[])
            .unwrap();
        check_get_running_task_cnt(task_cnt);
        // Clean the task.
        delete_tasks(&stack, task_id);
        sch.del_running_task(task_id);
    }

    // test DetectTaskLoop
    let check_get_task_state = |expected_state: TaskState| {
        eventually(Duration::from_secs(10), Duration::from_millis(100), || {
            let tasks = mgr.get_task_bases_in_states(std::slice::from_ref(&expected_state)).unwrap();
            if tasks.len() == task_cnt {
                return true;
            }
            let history_tasks =
                testutil::get_tasks_from_history_in_states(&mgr, std::slice::from_ref(&expected_state)).unwrap();
            tasks.len() + history_tasks.len() == task_cnt
        });
    };
    // Test all subtasks are successful.
    if is_succ {
        // Mock subtasks succeed.
        for i in 1..=(SUBTASK_CNT * task_cnt) as i64 {
            mgr.update_subtask_state_and_error(":4000", i, &SUBTASK_STATE_SUCCEED, None).unwrap();
        }
        check_get_task_state(TASK_STATE_SUCCEED);
        assert_eq!(tasks.len(), task_cnt);

        check_get_running_task_cnt(0);
        return;
    }

    let subtasks_map: HashMap<i64, Vec<SubtaskBase>> =
        task_ids.iter().map(|&task_id| (task_id, mgr.get_active_subtasks(task_id).unwrap())).collect();

    if is_cancel {
        for &task_id in &task_ids {
            mgr.cancel_task(task_id).unwrap();
        }
    } else if is_pause_and_resume {
        for i in 0..task_cnt {
            let found = mgr.pause_task(&i.to_string()).unwrap();
            assert!(found);
        }
        for sts in subtasks_map.values() {
            for st in sts {
                mgr.update_subtask_state_and_error(":4000", st.id, &SUBTASK_STATE_PAUSED, None).unwrap();
            }
        }
        check_get_task_state(TASK_STATE_PAUSED);
        for i in 0..task_cnt {
            let found = mgr.resume_task(&i.to_string()).unwrap();
            assert!(found);
        }

        // Mock subtasks succeed.
        for sts in subtasks_map.values() {
            for st in sts {
                mgr.update_subtask_state_and_error(":4000", st.id, &SUBTASK_STATE_SUCCEED, None).unwrap();
            }
        }
        check_get_task_state(TASK_STATE_SUCCEED);
        return;
    } else if is_subtask_cancel {
        // Mock a subtask canceled
        for sts in subtasks_map.values() {
            mgr.update_subtask_state_and_error(":4000", sts[0].id, &SUBTASK_STATE_CANCELED, None).unwrap();
        }
    } else {
        // Mock a subtask fails.
        for sts in subtasks_map.values() {
            mgr.update_subtask_state_and_error(":4000", sts[0].id, &SUBTASK_STATE_FAILED, None).unwrap();
        }
    }

    check_get_task_state(TASK_STATE_REVERTING);
    assert_eq!(tasks.len(), task_cnt);
    for task in &tasks {
        let subtasks = mgr
            .get_subtasks_by_exec_id_and_step_and_states(
                ":4000",
                task.id,
                task.step,
                &[SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING],
            )
            .unwrap();
        for subtask in subtasks {
            mgr.update_subtask_state_and_error(":4000", subtask.base.id, &SUBTASK_STATE_CANCELED, None).unwrap();
        }
    }
    check_get_task_state(TASK_STATE_REVERTED);
    assert_eq!(tasks.len(), task_cnt);
}

/// Go `TestSimple`.
#[test]
fn test_simple() {
    check_schedule(1, true, false, false, false);
}

/// Go `TestSimpleErrStage`.
#[test]
fn test_simple_err_stage() {
    check_schedule(1, false, false, false, false);
}

/// Go `TestSimpleCancel`.
#[test]
fn test_simple_cancel() {
    check_schedule(1, false, true, false, false);
}

/// Go `TestSimpleSubtaskCancel`.
#[test]
fn test_simple_subtask_cancel() {
    check_schedule(1, false, false, true, false);
}

/// Go `TestParallel`.
#[test]
fn test_parallel() {
    check_schedule(3, true, false, false, false);
}

/// Go `TestParallelErrStage`.
#[test]
fn test_parallel_err_stage() {
    check_schedule(3, false, false, false, false);
}

/// Go `TestParallelCancel`.
#[test]
fn test_parallel_cancel() {
    check_schedule(3, false, true, false, false);
}

/// Go `TestParallelSubtaskCancel`.
#[test]
fn test_parallel_subtask_cancel() {
    check_schedule(3, false, false, true, false);
}

/// Go `TestPause`.
#[test]
fn test_pause() {
    check_schedule(1, false, false, false, true);
}

/// Go `TestParallelPause`.
#[test]
fn test_parallel_pause() {
    check_schedule(3, false, false, false, true);
}

/// Go `TestManagerScheduleLoop`: the node's own DXF loop (Go's
/// `CreateMockStore` starts it) schedules the tasks. Its next-gen subtest
/// reads `handle.GetScheduleStatus`, unported with the next-gen status.
#[test]
fn test_manager_schedule_loop() {
    // Mock 16 cpu node.
    fail::cfg("mockNumCpu", "return(16)").unwrap();
    let ctrl = Controller::new();

    let stack = mock_store();
    let _dist_task_loop = stack.factory.init_dist_task_loop().unwrap();
    let keyspace = "";
    eventually(Duration::from_secs(10), Duration::from_millis(100), || tidb_dxf_storage::get_task_manager().is_ok());

    let task_mgr = tidb_dxf_storage::get_task_manager().unwrap();

    let concurrencies = [4, 6, 16, 2, 4, 4];
    // The test keeps the senders: unwinding closes the channels, so a
    // scheduler still waiting returns (Go's `<-ch` on a closed channel).
    let (wait_channels, receivers): (HashMap<String, crossbeam_channel::Sender<()>>, HashMap<String, crossbeam_channel::Receiver<()>>) =
        (0..concurrencies.len())
            .map(|i| {
                let (sender, receiver) = crossbeam_channel::bounded(0);
                ((format!("key/{i}"), sender), (format!("key/{i}"), receiver))
            })
            .unzip();
    let factory_ctrl = Arc::clone(&ctrl);
    let factory_task_mgr = Arc::clone(&task_mgr);
    let factory_wait_channels = Arc::new(receivers);
    tidb_dxf_scheduler::register_scheduler_factory(
        TASK_TYPE_EXAMPLE,
        Arc::new(move |_, task, _| {
            let mock_scheduler = MockScheduler::new(&factory_ctrl);
            // below 2 are for balancer loop, it's async, cannot determine how
            // many times it will be called.
            mock_scheduler.ret_times("GetTask", Times::Any, Arc::new(task.clone()));
            mock_scheduler.ret_times("GetEligibleInstances", Times::Any, Ok::<Vec<String>, SchedulerError>(Vec::new()));
            mock_scheduler.ret("Init", Ok::<(), SchedulerError>(()));
            let task_mgr = Arc::clone(&factory_task_mgr);
            let wait_channels = Arc::clone(&factory_wait_channels);
            mock_scheduler.expect("ScheduleTask", tidb_gomock::ONCE, move |_| {
                if task.is_done() {
                    return;
                }
                let set_state = |state: &TaskState, step: Step| {
                    task_mgr
                        .with_new_session(&mut |se| {
                            tidb_dxf_storage::exec_sql(
                                &tidb_sqlexec::BackgroundContext,
                                se,
                                "update mysql.tidb_global_task set state=%?, step=%? where id=%?",
                                &[
                                    SqlArg::String(state.to_string().as_bytes()),
                                    SqlArg::Signed(step.0),
                                    SqlArg::Signed(task.base.id),
                                ],
                            )
                            .map(|_| ())
                        })
                        .unwrap();
                };
                set_state(&TASK_STATE_RUNNING, STEP_ONE);
                let _ = wait_channels[&task.base.key].recv();
                set_state(&TASK_STATE_SUCCEED, STEP_DONE);
            });
            mock_scheduler.ret("Close", ());
            mock_scheduler
        }),
    );
    let get_running_task_keys = || {
        let tasks = task_mgr.get_task_bases_in_states(&[TASK_STATE_RUNNING]).unwrap();
        let mut task_keys: Vec<String> = tasks.into_iter().map(|task| task.key).collect();
        task_keys.sort();
        task_keys
    };

    // in classic kernel, reserve by strips.
    if !tidb_config_kerneltype::is_next_gen() {
        // in this test, we only test scheduler manager, so we add a subtask takes 16
        // slots to avoid reserve by slots, and make sure below test cases works.
        let server_infos = stack.factory.server_info().unwrap().get_all_server_info().unwrap();
        for s in server_infos.values() {
            let exec_id = tidb_util_disttask::generate_exec_id(s);
            testutil::insert_subtask(
                &task_mgr,
                1_000_000,
                STEP_ONE,
                &exec_id,
                Some(b""),
                &SUBTASK_STATE_PENDING,
                &TASK_TYPE_EXAMPLE,
                16,
            );
        }
        for (i, &concurrency) in concurrencies.iter().enumerate() {
            task_mgr
                .create_task(&format!("key/{i}"), &TASK_TYPE_EXAMPLE, keyspace, concurrency, "", 1, &ExtraParams::default(), b"{}")
                .unwrap();
        }
        eventually(Duration::from_secs(10), Duration::from_millis(100), || {
            get_running_task_keys() == ["key/0", "key/1", "key/3", "key/4"]
        });
        // finish the first task, task 'key/5' can be scheduled now
        wait_channels["key/0"].send(()).unwrap();
        eventually(Duration::from_secs(10), Duration::from_millis(100), || {
            get_running_task_keys() == ["key/1", "key/3", "key/4", "key/5"]
        });
        // finish the second task, task 'key/2' can be scheduled now
        // note, we don't preempt task 'key/3'/'key/4'/'key/5' even if 'key/2' reserves
        // all slots, as schedule doesn't take too much resource, it's task executor
        // that will be preempted.
        wait_channels["key/1"].send(()).unwrap();
        eventually(Duration::from_secs(10), Duration::from_millis(100), || {
            get_running_task_keys() == ["key/2", "key/3", "key/4", "key/5"]
        });
        // close others
        for i in 2..concurrencies.len() {
            wait_channels[&format!("key/{i}")].send(()).unwrap();
        }
        eventually(Duration::from_secs(10), Duration::from_millis(100), || get_running_task_keys().is_empty());
    }
    fail::remove("mockNumCpu");
    ctrl.finish();
}

/// Go `TestSchedulerCleaner`'s "drains bounded batches" subtest.
#[test]
fn test_scheduler_cleaner_drains_bounded_batches() {
    let _restore = Restore(Some(Box::new(set_task_cleanup_batch_size_for_test(2))));
    let stack = mock_store();
    let ctrl = Controller::new();
    let mock_cleaner = MockCleaner::new(&ctrl);

    let (sch, mgr) = mock_scheduler_manager(&stack, None, Some(Arc::clone(&mock_cleaner) as Arc<dyn tidb_dxf_scheduler::Cleaner>));
    mgr.init_meta(":4000", &tidb_dxf_handle::get_target_scope()).unwrap();
    mock_cleaner.ret_times("Clean", Times::N(3), Ok::<(), SchedulerError>(()));
    for task_key in ["cleanup-batch-0", "cleanup-batch-1", "cleanup-batch-2"] {
        let task_id = mgr
            .create_task(task_key, &TASK_TYPE_EXAMPLE, "", 1, &tidb_dxf_handle::get_target_scope(), 1, &ExtraParams::default(), &[])
            .unwrap();
        let task = mgr.get_task_by_id(task_id).unwrap();
        mgr.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &[]).unwrap();
        mgr.succeed_task(task_id).unwrap();
    }

    sch.do_clean();
    let history_tasks = testutil::get_tasks_from_history_in_states(&mgr, &[TASK_STATE_SUCCEED]).unwrap();
    assert_eq!(history_tasks.len(), 3);
    let active_tasks = mgr.get_task_bases_in_states(&[TASK_STATE_SUCCEED]).unwrap();
    assert!(active_tasks.is_empty());
    sch.stop();
    ctrl.finish();
}

/// Go `t.Cleanup` of a test-only setting.
struct Restore(Option<Box<dyn FnOnce()>>);

impl Drop for Restore {
    fn drop(&mut self) {
        if let Some(restore) = self.0.take() {
            restore();
        }
    }
}

/// Go `TestSchedulerCleaner`.
#[test]
fn test_scheduler_cleaner() {
    let stack = mock_store();
    let ctrl = Controller::new();
    let mock_cleaner = MockCleaner::new(&ctrl);

    let target_scope = tidb_dxf_handle::get_target_scope();
    let (sch, mgr) = mock_scheduler_manager(
        &stack,
        Some(get_number_example_scheduler_ext(&ctrl)),
        Some(Arc::clone(&mock_cleaner) as Arc<dyn tidb_dxf_scheduler::Cleaner>),
    );
    mgr.init_meta(":4000", &target_scope).unwrap();
    mock_cleaner.ret_times("Clean", Times::Any, Ok::<(), SchedulerError>(()));
    sch.start();
    let task_id = mgr.create_task("test", &TASK_TYPE_EXAMPLE, "", 1, &target_scope, 1, &ExtraParams::default(), &[]).unwrap();

    let check_task_running = || {
        eventually(Duration::from_secs(5), Duration::from_millis(50), || {
            mgr.get_task_base_by_id(task_id).unwrap().state == TASK_STATE_RUNNING
        });
    };

    let check_subtask_cnt = |task_id: i64| {
        eventually(Duration::from_secs(5), Duration::from_millis(50), || {
            let cnt_by_states = mgr.get_subtask_cnt_group_by_states(task_id, STEP_ONE).unwrap();
            SUBTASK_CNT as i64 == cnt_by_states.get(&SUBTASK_STATE_PENDING).copied().unwrap_or(0)
        });
    };

    check_task_running();
    check_subtask_cnt(task_id);
    for i in 1..=SUBTASK_CNT as i64 {
        mgr.update_subtask_state_and_error(":4000", i, &SUBTASK_STATE_SUCCEED, None).unwrap();
    }
    sch.do_clean();
    eventually(Duration::from_secs(50), Duration::from_millis(300), || {
        !testutil::get_tasks_from_history_in_states(&mgr, &[TASK_STATE_SUCCEED]).unwrap().is_empty()
    });
    sch.stop();
    ctrl.finish();
}
