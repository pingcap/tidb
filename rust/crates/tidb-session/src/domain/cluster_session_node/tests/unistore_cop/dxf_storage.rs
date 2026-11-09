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

//! Go `pkg/dxf/framework/storage`'s tests that need a store
//! (`table_test.go`, `task_state_test.go`), over the embedded unistore
//! node's DXF session pool.

use super::*;
use chrono::{DateTime, FixedOffset, Local, TimeZone as _};
use tidb_dxf::*;
use tidb_dxf_storage::{set_task_manager, TaskManager};
use tidb_dxf_testutil as testutil;

/// Go `testutil.InitTableTest`: a store and a task manager over the node's
/// DXF session pool, set as the instance's task manager. Go mocks the CPU
/// count to 8, the node resource's default.
fn init_table_test() -> (UnistoreClusterStack, Arc<TaskManager>) {
    let (stack, _users) = cop_backed_stack();
    let gm = Arc::new(TaskManager::new(stack.factory.dxf_session_pool()));
    set_task_manager(Arc::clone(&gm));
    (stack, gm)
}

/// Go `time.Unix(time.Now().Unix(), 0)`.
fn now_secs() -> DateTime<FixedOffset> {
    Local.timestamp_opt(Local::now().timestamp(), 0).unwrap().fixed_offset()
}

fn err(message: &str) -> std::io::Error {
    std::io::Error::other(message.to_owned())
}

fn tp(name: &str) -> TaskType {
    TaskType::new(name)
}

/// Go `checkTaskStateStep`.
fn check_task_state_step(task: &Task, state: &TaskState, step: Step) {
    assert_eq!(&task.base.state, state);
    assert_eq!(task.base.step, step);
}

/// Go `TestTaskTable`.
#[test]
fn test_task_table() {
    let (_stack, gm) = init_table_test();

    gm.init_meta(":4000", "").unwrap();

    let error = gm
        .create_task("key1", &tp("test"), "", 999, "", 0, &ExtraParams::default(), b"test")
        .unwrap_err();
    assert!(error.to_string().contains("task required slots(999) larger than cpu count"), "{error}");

    let time_before_create = now_secs();
    let id = gm
        .create_task(
            "key1",
            &tp("test"),
            "test_keyspace",
            4,
            "aaa",
            12,
            &ExtraParams { manual_recovery: true, ..ExtraParams::default() },
            b"testmeta",
        )
        .unwrap();
    assert_eq!(id, 1);

    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.id, 1);
    assert_eq!(task.base.key, "key1");
    assert_eq!(task.base.tp, tp("test"));
    assert_eq!(task.base.state, TASK_STATE_PENDING);
    assert_eq!(task.base.priority, NORMAL_PRIORITY);
    assert_eq!(task.base.required_slots, 4);
    assert_eq!(task.base.step, STEP_INIT);
    assert_eq!(task.base.target_scope, "aaa");
    assert_eq!(task.base.max_node_count, 12);
    assert!(task.base.extra_params.manual_recovery);
    assert_eq!(task.base.keyspace, "test_keyspace");
    assert_eq!(task.meta, b"testmeta");
    assert!(task.base.create_time >= time_before_create);
    assert_eq!(task.start_time, go_zero_time());
    assert_eq!(task.state_update_time, go_zero_time());
    assert!(task.error.is_none());

    let task2 = gm.get_task_by_id(1).unwrap();
    assert_eq!(format!("{task:?}"), format!("{task2:?}"));

    let tasks_base3 = gm.get_task_bases_in_states(&[TASK_STATE_PENDING]).unwrap();
    assert_eq!(tasks_base3.len(), 1);
    assert_eq!(format!("{:?}", task.base), format!("{:?}", tasks_base3[0]));

    let tasks_base4 = gm.get_task_bases_in_states(&[TASK_STATE_PENDING, TASK_STATE_RUNNING]).unwrap();
    assert_eq!(tasks_base4.len(), 1);
    assert_eq!(format!("{:?}", task.base), format!("{:?}", tasks_base4[0]));

    gm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &[]).unwrap();

    let task5 = gm.get_task_by_id(id).unwrap();
    assert_eq!(task5.base.state, TASK_STATE_RUNNING);

    let task6 = gm.get_task_by_key("key1").unwrap();
    assert_eq!(task5.base.state, task6.base.state);

    // test cannot insert task with dup key
    let error = gm.create_task("key1", &tp("test2"), "", 4, "", 0, &ExtraParams::default(), b"test2").unwrap_err();
    assert_eq!(error.to_string(), "[kv:1062]Duplicate entry 'key1' for key 'tidb_global_task.task_key'");

    // test cancel task
    let id = gm.create_task("key2", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    assert!(!testutil::is_task_cancelling(&gm, id).unwrap());
    gm.cancel_task(id).unwrap();
    assert!(testutil::is_task_cancelling(&gm, id).unwrap());

    let id = gm.create_task("key-fail", &tp("test2"), "", 4, "", 0, &ExtraParams::default(), b"test2").unwrap();
    // state not right, update nothing
    gm.fail_task(id, &TASK_STATE_RUNNING, Some(&err("test error"))).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_PENDING);
    assert!(task.error.is_none());
    let cur_time = now_secs();
    gm.fail_task(id, &TASK_STATE_PENDING, Some(&err("test error"))).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_FAILED);
    assert!(task.error.as_ref().unwrap().to_string().contains("test error"));
    let end_time = testutil::get_task_end_time(&gm, id).unwrap();
    assert!(end_time - cur_time <= Local::now().fixed_offset() - cur_time);
    assert!(end_time >= cur_time);

    // succeed a pending task, no effect
    let id = gm.create_task("key-success", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    gm.succeed_task(id).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_PENDING, STEP_INIT);
    // succeed a running task
    gm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &[]).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_RUNNING, STEP_ONE);
    let start_time = now_secs();
    gm.succeed_task(id).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_SUCCEED, STEP_DONE);
    assert!(task.state_update_time >= start_time);

    // reverted a pending task, no effect
    let id = gm.create_task("key-reverted", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    gm.reverted_task(id).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_PENDING, STEP_INIT);
    // reverted a reverting task
    gm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &[]).unwrap();
    let mut task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_RUNNING, STEP_ONE);
    task.base.state = TASK_STATE_REVERTING;
    gm.revert_task(task.base.id, &TASK_STATE_RUNNING, Some(&err("test error"))).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_REVERTING);
    assert!(task.error.as_ref().unwrap().to_string().contains("test error"));
    gm.reverted_task(task.base.id).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_REVERTED);

    // paused
    let id = gm.create_task("key-paused", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    gm.paused_task(id).unwrap();
    let mut task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_PENDING, STEP_INIT);
    // reverted a reverting task
    task.base.state = TASK_STATE_PAUSING;
    let found = gm.pause_task(&task.base.key).unwrap();
    assert!(found);
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_PAUSING);
    gm.paused_task(task.base.id).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_PAUSED);
}
