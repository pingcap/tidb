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

//! Go `pkg/dxf/framework/taskexecutor`'s test that needs a store
//! (`task_executor_testkit_test.go`), over the embedded unistore node.

use super::*;
use std::sync::RwLock;
use std::time::Duration;
use tidb_dxf::*;
use tidb_dxf_storage::TaskManager;
use tidb_dxf_testutil as testutil;
use tidb_gocontext::Context;
use tidb_gomock::Controller;

/// Go `runOneTask`.
fn run_one_task(ctx: &Context, mgr: &Arc<TaskManager>, task_key: &str, subtask_cnt: usize) {
    let task_id = mgr.create_task(&ctx, task_key, &TASK_TYPE_EXAMPLE, "", 1, "", 0, &ExtraParams::default(), &[]).unwrap();
    let task = mgr.get_task_by_id(&ctx, task_id).unwrap();

    let check_subtasks = |step: Step, state: SubtaskState| {
        let subtasks = mgr.get_all_subtasks_by_step_and_state(&ctx, task_id, step, &state).unwrap();
        assert_eq!(subtasks.len(), subtask_cnt);
    };
    // 1. stepOne
    mgr.switch_task_step(&ctx, &task, &TASK_STATE_RUNNING, STEP_ONE, &[]).unwrap();
    for _ in 0..subtask_cnt {
        testutil::create_sub_task(mgr, task_id, STEP_ONE, ":4000", None, &TASK_TYPE_EXAMPLE, 1);
    }
    check_subtasks(STEP_ONE, SUBTASK_STATE_PENDING);
    let task = mgr.get_task_by_id(&ctx, task_id).unwrap();
    let factory = tidb_dxf_taskexecutor::get_task_executor_factory(&task.base.tp).expect("the example task executor is registered");
    // Go's `NewParamForTest` passes a nil slot manager: a plain run never
    // exchanges slots.
    let executor = factory(
        ctx,
        task.clone(),
        tidb_dxf_taskexecutor::Param::new_for_test(
            Arc::clone(mgr) as Arc<dyn tidb_dxf_taskexecutor::TaskTable>,
            Arc::new(tidb_dxf_taskexecutor::SlotManager::new(NODE_RESOURCE_FOR_TEST.total_cpu)),
            *NODE_RESOURCE_FOR_TEST,
            ":4000",
        ),
    );
    executor.run();
    check_subtasks(STEP_ONE, SUBTASK_STATE_SUCCEED);
    // 2. stepTwo
    mgr.switch_task_step(&ctx, &task, &TASK_STATE_RUNNING, STEP_TWO, &[]).unwrap();
    for _ in 0..subtask_cnt {
        testutil::create_sub_task(mgr, task_id, STEP_TWO, ":4000", None, &TASK_TYPE_EXAMPLE, 11);
    }
    check_subtasks(STEP_TWO, SUBTASK_STATE_PENDING);
    executor.run();
    check_subtasks(STEP_TWO, SUBTASK_STATE_SUCCEED);
}

/// Go `taskexecutor.ReduceCheckInterval`, restored on drop as Go's
/// `t.Cleanup` does.
struct ReducedCheckInterval(Vec<(&'static RwLock<Duration>, Duration)>);

fn reduce_check_interval() -> ReducedCheckInterval {
    let set = |interval: &'static RwLock<Duration>| {
        (interval, std::mem::replace(&mut *interval.write().unwrap(), Duration::from_millis(1)))
    };
    ReducedCheckInterval(vec![
        set(&tidb_dxf_taskexecutor::TASK_CHECK_INTERVAL),
        set(&tidb_dxf_taskexecutor::MAX_SUBTASK_CHECK_INTERVAL),
        set(&tidb_dxf_taskexecutor::SUBTASK_CHECK_INTERVAL),
    ])
}

impl Drop for ReducedCheckInterval {
    fn drop(&mut self) {
        for (interval, backup) in self.0.drain(..) {
            *interval.write().unwrap() = backup;
        }
    }
}

/// Go `TestTaskExecutorBasic`; the test stacks do not start the DXF loop
/// (Go's `MockDisableDistTask`).
#[test]
fn test_task_executor_basic() {
    let (stack, _users) = cop_backed_stack();
    let ctx = Context::background();
    let ctrl = Controller::new();
    let mgr = Arc::new(TaskManager::new(stack.factory.dxf_session_pool()));

    let _reduced = reduce_check_interval();

    testutil::init_task_executor(&ctrl, |_, subtask| {
        if subtask.base.step == STEP_ONE {
            tidb_logutil::bg_logger().info("run step one", &[]);
        } else if subtask.base.step == STEP_TWO {
            tidb_logutil::bg_logger().info("run step two", &[]);
        } else {
            panic!("invalid step");
        }
        Ok(())
    });
    mgr.init_meta(&ctx, ":4000", "").unwrap();
    for i in 0..10 {
        run_one_task(&ctx, &mgr, &format!("key{i}"), i);
    }
    ctrl.finish();
}
