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

/// Go `checkAfterSwitchStep`.
fn check_after_switch_step(start_time: DateTime<FixedOffset>, task: &Task, subtasks: &[Subtask], step: Step) {
    let tm = tidb_dxf_storage::get_task_manager().unwrap();

    check_task_state_step(task, &TASK_STATE_RUNNING, step);
    assert!(task.start_time >= start_time);
    assert!(task.state_update_time >= start_time);
    let mut got_subtasks =
        tm.get_all_subtasks_by_step_and_state(task.base.id, task.base.step, &SUBTASK_STATE_PENDING).unwrap();
    assert_eq!(got_subtasks.len(), subtasks.len());
    got_subtasks.sort_by_key(|subtask| subtask.base.ordinal);
    for (i, subtask) in got_subtasks.iter().enumerate() {
        assert_eq!(subtask.meta, i.to_string().into_bytes());
        assert_eq!(subtask.base.ordinal, i as isize + 1);
        assert_eq!(subtask.base.concurrency, 11);
        assert_eq!(subtask.base.exec_id, ":4000");
        assert_eq!(subtask.base.tp, TASK_TYPE_EXAMPLE);
        assert!(subtask.base.create_time >= start_time);
    }
}

/// Go's three example subtasks of a step: metas "0".."2", ordinals 1..3.
fn new_subtasks(step: Step, task_id: i64) -> Vec<Subtask> {
    (0..3)
        .map(|i| Subtask::new(step, task_id, TASK_TYPE_EXAMPLE, ":4000", 11, i.to_string().into_bytes(), i + 1))
        .collect()
}

/// Go `TestGetActiveTaskCountsByKeyspace`.
#[test]
fn test_get_active_task_counts_by_keyspace() {
    let (_stack, gm) = init_table_test();
    gm.init_meta(":4000", "").unwrap();

    let summary = gm.get_active_task_counts_by_keyspace().unwrap();
    assert_eq!(summary.total, 0);
    assert!(summary.per_keyspace.is_empty());

    gm.create_task("key-ks1-1", &tp("test"), "ks1", 1, "", 0, &ExtraParams::default(), b"test").unwrap();
    gm.create_task("key-ks1-2", &tp("test"), "ks1", 1, "", 0, &ExtraParams::default(), b"test").unwrap();
    gm.create_task("key-ks2-1", &tp("test"), "ks2", 1, "", 0, &ExtraParams::default(), b"test").unwrap();

    let summary = gm.get_active_task_counts_by_keyspace().unwrap();
    assert_eq!(summary.total, 3);
    assert_eq!(summary.per_keyspace["ks1"], 2);
    assert_eq!(summary.per_keyspace["ks2"], 1);
}

/// Go `TestSwitchTaskStep`.
#[test]
fn test_switch_task_step() {
    let (stack, tm) = init_table_test();
    let mut tk = stack.factory.open_session(session_context(301)).unwrap();

    tm.init_meta(":4000", "").unwrap();
    let task_id = tm.create_task("key1", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    let mut task = tm.get_task_by_id(task_id).unwrap();
    check_task_state_step(&task, &TASK_STATE_PENDING, STEP_INIT);
    let subtasks_step_one = new_subtasks(STEP_ONE, task_id);
    let start_time = now_secs();
    task.meta = b"changed meta".to_vec();
    tm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &subtasks_step_one).unwrap();
    let mut task = tm.get_task_by_id(task_id).unwrap();
    assert_eq!(task.meta, b"changed meta");
    check_after_switch_step(start_time, &task, &subtasks_step_one, STEP_ONE);
    // switch step again, no effect
    // some fields are changed in prev call, change back.
    task.base.state = TASK_STATE_PENDING;
    task.base.step = STEP_INIT;
    tm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &subtasks_step_one).unwrap();
    let task = tm.get_task_by_id(task_id).unwrap();
    check_after_switch_step(start_time, &task, &subtasks_step_one, STEP_ONE);
    // switch step to step two
    std::thread::sleep(Duration::from_secs(1));
    let task_start_time = task.start_time;
    let subtasks_step_two = new_subtasks(STEP_TWO, task_id);
    rows(&mut tk, "set tidb_mem_quota_query = 1024");
    tm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_TWO, &subtasks_step_two).unwrap();
    assert_eq!(displayed(rows(&mut tk, "select @@tidb_mem_quota_query")), [["1024"]]);
    let task = tm.get_task_by_id(task_id).unwrap();
    // start time should not change
    assert_eq!(task.start_time, task_start_time);
    check_after_switch_step(start_time, &task, &subtasks_step_two, STEP_TWO);

    // prepare transition persists fields and exposes zero-row CAS
    let prepare_task_id =
        tm.create_task("prepare-key", &tp("test"), "", 1, "", 1, &ExtraParams::default(), b"init-meta").unwrap();
    let mut prepare_task = tm.get_task_by_id(prepare_task_id).unwrap();
    check_task_state_step(&prepare_task, &TASK_STATE_PENDING, STEP_INIT);

    prepare_task.meta = br#"{"prepare":"done"}"#.to_vec();
    prepare_task.base.required_slots = 8;
    prepare_task.base.max_node_count = 6;
    prepare_task.base.extra_params =
        ExtraParams { manual_recovery: true, prepare_mode: PREPARE_MODE_REQUIRED, ..ExtraParams::default() };
    let switch_time = now_secs();
    assert!(tm.switch_task_step_after_prepare(&prepare_task).unwrap());

    let persisted_task = tm.get_task_by_id(prepare_task_id).unwrap();
    check_task_state_step(&persisted_task, &TASK_STATE_PENDING, STEP_PREPARED);
    assert_eq!(persisted_task.meta, br#"{"prepare":"done"}"#);
    assert_eq!(persisted_task.base.required_slots, 8);
    assert_eq!(persisted_task.base.max_node_count, 6);
    assert_eq!(format!("{:?}", persisted_task.base.extra_params), format!("{:?}", ExtraParams::default()));
    assert_eq!(persisted_task.start_time, go_zero_time());
    assert!(persisted_task.state_update_time >= switch_time);
    assert_eq!(
        displayed(rows(
            &mut tk,
            &format!("select count(1) from mysql.tidb_background_subtask where task_key = '{prepare_task_id}'")
        )),
        [["0"]]
    );

    prepare_task.meta = br#"{"prepare":"stale-owner"}"#.to_vec();
    prepare_task.base.required_slots = 99;
    prepare_task.base.max_node_count = 99;
    prepare_task.base.extra_params = ExtraParams { prepare_mode: PREPARE_MODE_DISABLED, ..ExtraParams::default() };
    assert!(!tm.switch_task_step_after_prepare(&prepare_task).unwrap());

    let persisted_task = tm.get_task_by_id(prepare_task_id).unwrap();
    assert_eq!(persisted_task.meta, br#"{"prepare":"done"}"#);
    assert_eq!(persisted_task.base.required_slots, 8);
    assert_eq!(persisted_task.base.max_node_count, 6);
    assert_eq!(format!("{:?}", persisted_task.base.extra_params), format!("{:?}", ExtraParams::default()));
}

/// Go `TestGetSubtaskSummaries`.
#[test]
fn test_get_subtask_summaries() {
    let (stack, tm) = init_table_test();
    let mut tk = stack.factory.open_session(session_context(302)).unwrap();

    tm.init_meta(":4000", "").unwrap();
    let task_id = tm.create_task("test", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    let subtasks = new_subtasks(STEP_ONE, task_id);

    let task = tm.get_task_by_id(task_id).unwrap();
    tm.switch_task_step_in_batch(&task, &TASK_STATE_RUNNING, STEP_ONE, &subtasks).unwrap();

    let summary = tidb_dxf_execute::SubtaskSummary::default();
    summary.row_cnt.store(100, std::sync::atomic::Ordering::Relaxed);
    summary.processed.store(200, std::sync::atomic::Ordering::Relaxed);
    let bytes = tidb_stringutil::go_json_marshal(&summary);

    rows(
        &mut tk,
        &format!("update mysql.tidb_background_subtask set summary = '{bytes}' where task_key = '{}'", task.base.id),
    );
    let summaries = tm.get_all_subtask_summary_by_step(subtasks[0].base.task_id, STEP_ONE).unwrap();
    assert_eq!(summaries.len(), subtasks.len());
    for summary in &summaries {
        assert_eq!(summary.row_cnt.load(std::sync::atomic::Ordering::Relaxed), 100);
        assert_eq!(summary.processed.load(std::sync::atomic::Ordering::Relaxed), 200);
    }

    // If the JSON value is wrong, we still get an empty summary.
    // This can only happen if the summary field is manually updated.
    // It's acceptable, as the correct summary will be set by the executor later.
    rows(
        &mut tk,
        &format!(
            r#"update mysql.tidb_background_subtask set summary = '{{"wrong_key": 123}}' where task_key = '{}'"#,
            task.base.id
        ),
    );
    let summaries = tm.get_all_subtask_summary_by_step(subtasks[0].base.task_id, STEP_ONE).unwrap();
    for summary in &summaries {
        assert_eq!(summary.row_cnt.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert_eq!(summary.processed.load(std::sync::atomic::Ordering::Relaxed), 0);
    }
}

/// Go's raw insert of one subtask, as another scheduler would.
fn insert_raw_subtask(session: &mut Session, subtask: &Subtask) {
    rows(
        session,
        &format!(
            "
\t\t\tinsert into mysql.tidb_background_subtask(
\t\t\t\tstep, task_key, exec_id, meta, state, type, concurrency, ordinal, create_time, checkpoint, summary)
\t\t\tvalues ({}, {}, '{}', '{}', '{}', {}, {}, {}, CURRENT_TIMESTAMP(), '{{}}', '{{}}')",
            subtask.base.step.0,
            subtask.base.task_id,
            subtask.base.exec_id,
            String::from_utf8_lossy(&subtask.meta),
            TASK_STATE_PENDING,
            type_2_int(&subtask.base.tp),
            subtask.base.concurrency,
            subtask.base.ordinal,
        ),
    );
}

/// Go `TestSwitchTaskStepInBatch`.
#[test]
fn test_switch_task_step_in_batch() {
    let (stack, tm) = init_table_test();
    let mut tk = stack.factory.open_session(session_context(303)).unwrap();

    tm.init_meta(":4000", "").unwrap();
    // normal flow
    let prepare = |task_key: &str| {
        let task_id = tm.create_task(task_key, &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
        let task = tm.get_task_by_id(task_id).unwrap();
        check_task_state_step(&task, &TASK_STATE_PENDING, STEP_INIT);
        (task, new_subtasks(STEP_ONE, task_id))
    };
    let start_time = now_secs();
    let (task1, subtasks1) = prepare("key1");
    tm.switch_task_step_in_batch(&task1, &TASK_STATE_RUNNING, STEP_ONE, &subtasks1).unwrap();
    let task1 = tm.get_task_by_id(task1.base.id).unwrap();
    check_after_switch_step(start_time, &task1, &subtasks1, STEP_ONE);

    // mock another scheduler inserted some subtasks
    fail::cfg("waitBeforeInsertSubtasks", "1*return(true)").unwrap();
    let (task2, subtasks2) = prepare("key2");
    let other = {
        let factory = Arc::clone(&stack.factory);
        let subtask = subtasks2[0].clone();
        std::thread::spawn(move || {
            tidb_dxf_storage::TEST_CHANNEL.0.send(()).unwrap();
            let mut tk2 = factory.open_session(session_context(304)).unwrap();
            insert_raw_subtask(&mut tk2, &subtask);
            tidb_dxf_storage::TEST_CHANNEL.0.send(()).unwrap();
        })
    };
    let error = tm.switch_task_step_in_batch(&task2, &TASK_STATE_RUNNING, STEP_ONE, &subtasks2).unwrap_err();
    other.join().unwrap();
    fail::remove("waitBeforeInsertSubtasks");
    assert!(
        matches!(&error, tidb_dxf_storage::StorageError::Terror(t_err) if *t_err == *tidb_kv::kv_errors::ERR_KEY_EXISTS),
        "{error:?}"
    );
    let task2 = tm.get_task_by_id(task2.base.id).unwrap();
    check_task_state_step(&task2, &TASK_STATE_PENDING, STEP_INIT);
    let got_subtasks = tm.get_all_subtasks_by_step_and_state(task2.base.id, STEP_ONE, &SUBTASK_STATE_PENDING).unwrap();
    assert_eq!(got_subtasks.len(), 1);
    // run again, should success
    tm.switch_task_step_in_batch(&task2, &TASK_STATE_RUNNING, STEP_ONE, &subtasks2).unwrap();
    let task2 = tm.get_task_by_id(task2.base.id).unwrap();
    check_after_switch_step(start_time, &task2, &subtasks2, STEP_ONE);

    // mock subtasks unstable
    let (task3, subtasks3) = prepare("key3");
    for subtask in &subtasks3[..2] {
        insert_raw_subtask(&mut tk, subtask);
    }
    let error = tm.switch_task_step_in_batch(&task3, &TASK_STATE_RUNNING, STEP_ONE, &subtasks3[..1]).unwrap_err();
    assert!(error.is(&tidb_dxf_storage::StorageError::UnstableSubtasks));
    assert!(error.to_string().contains("expected 1, got 2"), "{error}");
}

/// Go `TestGetTopUnfinishedTasks`.
#[test]
fn test_get_top_unfinished_tasks() {
    let (_stack, gm) = init_table_test();

    let restore = set_max_concurrent_task_for_test(4);
    gm.init_meta(":4000", "").unwrap();
    let task_states = [
        TASK_STATE_SUCCEED,
        TASK_STATE_PENDING,
        TASK_STATE_RUNNING,
        TASK_STATE_REVERTING,
        TASK_STATE_CANCELLING,
        TASK_STATE_PAUSING,  // key/5
        TASK_STATE_RESUMING, // key/6
        TASK_STATE_FAILED,
        TASK_STATE_PENDING,
        TASK_STATE_PENDING,
        TASK_STATE_PENDING,
        TASK_STATE_PENDING,
        TASK_STATE_MODIFYING,
    ];
    for (i, state) in task_states.iter().enumerate() {
        let task_key = format!("key/{i}");
        gm.create_task(&task_key, &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
        let state = state.to_string();
        gm.execute_sql_with_new_session(
            "
\t\t\t\tupdate mysql.tidb_global_task set state = %? where task_key = %?",
            &[tidb_sqlescape::SqlArg::String(state.as_bytes()), tidb_sqlescape::SqlArg::String(task_key.as_bytes())],
        )
        .unwrap();
    }
    // adjust task order
    gm.execute_sql_with_new_session(
        "
\t\t\t\tupdate mysql.tidb_global_task set create_time = current_timestamp",
        &[],
    )
    .unwrap();
    gm.execute_sql_with_new_session(
        "
\t\t\t\tupdate mysql.tidb_global_task
\t\t\t\tset create_time = timestampadd(minute, -10, current_timestamp)
\t\t\t\twhere task_key = 'key/5'",
        &[],
    )
    .unwrap();
    gm.execute_sql_with_new_session(
        "
\t\t\t\tupdate mysql.tidb_global_task set priority = 100 where task_key = 'key/6'",
        &[],
    )
    .unwrap();
    let rs = gm
        .execute_sql_with_new_session(
            "
\t\t\t\tselect count(1) from mysql.tidb_global_task",
            &[],
        )
        .unwrap();
    assert_eq!(displayed(rs), [["13"]]);
    let get_task_keys = |tasks: &[TaskBase]| tasks.iter().map(|task| task.key.clone()).collect::<Vec<_>>();
    let tasks = gm.get_top_unfinished_tasks().unwrap();
    assert_eq!(tasks.len(), 8);
    assert_eq!(get_task_keys(&tasks), ["key/6", "key/5", "key/1", "key/2", "key/3", "key/4", "key/8", "key/9"]);

    let restore_max_concurrent_task = set_max_concurrent_task_for_test(6);
    let tasks = gm.get_top_unfinished_tasks().unwrap();
    assert_eq!(tasks.len(), 11);
    assert_eq!(
        get_task_keys(&tasks),
        ["key/6", "key/5", "key/1", "key/2", "key/3", "key/4", "key/8", "key/9", "key/10", "key/11", "key/12"]
    );
    restore_max_concurrent_task();

    let restore_max_concurrent_task = set_max_concurrent_task_for_test(3);
    let tasks = gm.get_top_no_need_resource_tasks().unwrap();
    assert_eq!(get_task_keys(&tasks), ["key/5", "key/3", "key/4", "key/12"]);
    restore_max_concurrent_task();

    let restore_max_concurrent_task = set_max_concurrent_task_for_test(1);
    let tasks = gm.get_top_no_need_resource_tasks().unwrap();
    assert_eq!(get_task_keys(&tasks), ["key/5", "key/3"]);
    restore_max_concurrent_task();
    restore();
}

/// Go `TestGetUsedSlotsOnNodesAndBusyNodes`.
#[test]
fn test_get_used_slots_on_nodes_and_busy_nodes() {
    let (_stack, sm) = init_table_test();

    testutil::insert_subtask(&sm, 1, STEP_ONE, "tidb-1", Some(b""), &SUBTASK_STATE_RUNNING, &tp("test"), 12);
    testutil::insert_subtask(&sm, 1, STEP_ONE, "tidb-2", Some(b""), &SUBTASK_STATE_PENDING, &tp("test"), 12);
    testutil::insert_subtask(&sm, 2, STEP_ONE, "tidb-2", Some(b""), &SUBTASK_STATE_PENDING, &tp("test"), 8);
    testutil::insert_subtask(&sm, 3, STEP_ONE, "tidb-3", Some(b""), &SUBTASK_STATE_PENDING, &tp("test"), 8);
    testutil::insert_subtask(&sm, 4, STEP_ONE, "tidb-3", Some(b""), &SUBTASK_STATE_FAILED, &tp("test"), 8);
    testutil::insert_subtask(&sm, 5, STEP_ONE, "tidb-4", Some(b""), &SUBTASK_STATE_FAILED, &tp("test"), 8);

    let slots_on_nodes = sm.get_used_slots_on_nodes().unwrap();
    assert_eq!(
        slots_on_nodes,
        [("tidb-1".to_owned(), 12), ("tidb-2".to_owned(), 20), ("tidb-3".to_owned(), 8)].into_iter().collect()
    );

    let mut busy_nodes = sm.get_busy_nodes().unwrap();
    busy_nodes.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(
        busy_nodes.iter().map(|node| (node.id.as_str(), node.is_owner)).collect::<Vec<_>>(),
        [("tidb-1", false), ("tidb-2", false), ("tidb-3", false)]
    );
}

/// Go `TestGetActiveSubtasks`.
#[test]
fn test_get_active_subtasks() {
    let (_stack, tm) = init_table_test();
    tm.init_meta(":4000", "").unwrap();
    let id = tm.create_task("key1", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    assert_eq!(id, 1);
    let task = tm.get_task_by_id(id).unwrap();

    let subtasks: Vec<Subtask> =
        (0..3).map(|i| Subtask::new(STEP_ONE, id, tp("test"), format!("tidb{i}"), 8, b"{}}".to_vec(), i + 1)).collect();
    tm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &subtasks).unwrap();
    tm.finish_subtask("tidb0", 1, b"{}}").unwrap();
    tm.start_subtask(2, "tidb1").unwrap();

    let mut active_subtasks = tm.get_active_subtasks(task.base.id).unwrap();
    assert_eq!(active_subtasks.len(), 2);
    active_subtasks.sort_by_key(|subtask| subtask.id);
    assert_eq!(active_subtasks[0].id, 2);
    assert_eq!(active_subtasks[0].state, SUBTASK_STATE_RUNNING);
    assert_eq!(active_subtasks[1].id, 3);
    assert_eq!(active_subtasks[1].state, SUBTASK_STATE_PENDING);

    const ADJACENT_TASK_ID: i64 = 9007199254740992;
    const TARGET_TASK_ID: i64 = 9007199254740993;
    const LARGE_ID_EXEC: &str = "large-id-exec";
    testutil::insert_subtask(&tm, ADJACENT_TASK_ID, STEP_ONE, LARGE_ID_EXEC, None, &SUBTASK_STATE_PENDING, &TASK_TYPE_EXAMPLE, 8);
    testutil::insert_subtask(&tm, TARGET_TASK_ID, STEP_ONE, LARGE_ID_EXEC, None, &SUBTASK_STATE_PENDING, &TASK_TYPE_EXAMPLE, 8);

    let active_subtasks = tm.get_active_subtasks(TARGET_TASK_ID).unwrap();
    assert_eq!(active_subtasks.len(), 1);
    assert_eq!(active_subtasks[0].task_id, TARGET_TASK_ID);

    let filtered_subtasks = tm
        .get_subtasks_by_exec_id_and_step_and_states(LARGE_ID_EXEC, TARGET_TASK_ID, STEP_ONE, &[SUBTASK_STATE_PENDING])
        .unwrap();
    assert_eq!(filtered_subtasks.len(), 1);
    assert_eq!(filtered_subtasks[0].base.task_id, TARGET_TASK_ID);
}

/// Go `TestSubTaskTable`.
#[test]
fn test_sub_task_table() {
    let (_stack, sm) = init_table_test();
    let time_before_create = now_secs();
    sm.init_meta(":4000", "").unwrap();
    let id = sm.create_task("key1", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    assert_eq!(id, 1);
    let task = Task {
        base: TaskBase { id: 1, state: TASK_STATE_PENDING, step: STEP_INIT, ..TaskBase::default() },
        ..Task::default()
    };
    sm.switch_task_step(
        &task,
        &TASK_STATE_RUNNING,
        STEP_ONE,
        &[Subtask::new(STEP_ONE, 1, TASK_TYPE_EXAMPLE, "tidb1", 11, b"test".to_vec(), 1)],
    )
    .unwrap();

    let nil_task = sm.get_first_subtask_in_states("tidb2", 1, STEP_ONE, &[SUBTASK_STATE_PENDING]).unwrap();
    assert!(nil_task.is_none());

    let subtask = sm.get_first_subtask_in_states("tidb1", 1, STEP_ONE, &[SUBTASK_STATE_PENDING]).unwrap().unwrap();
    assert_eq!(subtask.base.step, STEP_ONE);
    assert_eq!(subtask.base.tp, TASK_TYPE_EXAMPLE);
    assert_eq!(subtask.base.task_id, 1);
    assert_eq!(subtask.base.state, SUBTASK_STATE_PENDING);
    assert_eq!(subtask.base.exec_id, "tidb1");
    assert_eq!(subtask.meta, b"test");
    assert_eq!(subtask.base.concurrency, 11);
    assert!(subtask.base.create_time >= time_before_create);
    assert_eq!(subtask.base.ordinal, 1);
    assert_eq!(subtask.base.start_time, go_zero_time());
    assert_eq!(subtask.update_time, go_zero_time());
    assert_eq!(subtask.summary, "{}");

    let subtask2 = sm.get_first_subtask_in_states("tidb1", 1, STEP_ONE, &[SUBTASK_STATE_PENDING]).unwrap().unwrap();
    assert_eq!(format!("{subtask:?}"), format!("{subtask2:?}"));

    let cnt_by_states = sm.get_subtask_cnt_group_by_states(1, STEP_ONE).unwrap();
    assert_eq!(cnt_by_states.len(), 1);
    assert_eq!(cnt_by_states[&SUBTASK_STATE_PENDING], 1);

    let ts = Local::now().fixed_offset();
    std::thread::sleep(Duration::from_secs(1));
    sm.start_subtask(1, "tidb1").unwrap();

    let error = sm.start_subtask(1, "tidb2").unwrap_err();
    assert!(error.is(&tidb_dxf_storage::StorageError::SubtaskNotFound));

    let subtask = sm.get_first_subtask_in_states("tidb1", 1, STEP_ONE, &[SUBTASK_STATE_PENDING]).unwrap();
    assert!(subtask.is_none());

    let subtask = sm.get_first_subtask_in_states("tidb1", 1, STEP_ONE, &[SUBTASK_STATE_RUNNING]).unwrap().unwrap();
    assert_eq!(subtask.base.tp, TASK_TYPE_EXAMPLE);
    assert_eq!(subtask.base.task_id, 1);
    assert_eq!(subtask.base.state, SUBTASK_STATE_RUNNING);
    assert_eq!(subtask.base.exec_id, "tidb1");
    assert_eq!(subtask.meta, b"test");
    assert!(subtask.base.start_time >= ts);
    assert!(subtask.update_time >= ts);

    // check update time after state change to cancel
    std::thread::sleep(Duration::from_secs(1));
    sm.fail_subtask("tidb1", 1, Some(&err("mock err"))).unwrap();
    let subtask2 = sm.get_first_subtask_in_states("tidb1", 1, STEP_ONE, &[SUBTASK_STATE_FAILED]).unwrap().unwrap();
    assert_eq!(subtask2.base.state, SUBTASK_STATE_FAILED);
    assert!(subtask2.update_time > subtask.update_time);

    let cnt_by_states = sm.get_subtask_cnt_group_by_states(1, STEP_ONE).unwrap();
    assert_eq!(cnt_by_states.get(&SUBTASK_STATE_PENDING).copied().unwrap_or(0), 0);

    testutil::delete_subtasks_by_task_id(&sm, 1).unwrap();

    testutil::create_sub_task(&sm, 2, STEP_ONE, "tidb1", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);

    let subtasks = sm.get_all_subtasks_by_step_and_state(2, STEP_ONE, &SUBTASK_STATE_SUCCEED).unwrap();
    assert!(subtasks.is_empty());

    let subtasks = testutil::get_subtasks_by_task_id(&sm, 2).unwrap();
    assert_eq!(subtasks.len(), 1);
    let subtask_id = subtasks[0].base.id;
    sm.finish_subtask("tidb1", subtask_id, &[]).unwrap();

    let subtasks = sm.get_all_subtasks_by_step_and_state(2, STEP_ONE, &SUBTASK_STATE_SUCCEED).unwrap();
    assert_eq!(subtasks.len(), 1);

    let row_count = sm.get_subtask_row_count(2, STEP_ONE).unwrap();
    assert_eq!(row_count, 0);
    let summary = tidb_dxf_execute::SubtaskSummary::default();
    summary.row_cnt.store(100, std::sync::atomic::Ordering::Relaxed);
    sm.update_subtask_summary(subtask_id, &summary).unwrap();
    let row_count = sm.get_subtask_row_count(2, STEP_ONE).unwrap();
    assert_eq!(row_count, 100);

    let get_subtask_base_slice = |sts: &[Subtask]| sts.iter().map(|st| st.base.clone()).collect::<Vec<_>>();
    // test UpdateSubtasksExecIDs
    // 1. update one subtask
    testutil::create_sub_task(&sm, 5, STEP_ONE, "tidb1", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);
    let mut subtasks = sm.get_all_subtasks_by_step_and_state(5, STEP_ONE, &SUBTASK_STATE_PENDING).unwrap();
    subtasks[0].base.exec_id = "tidb2".to_owned();
    sm.update_subtasks_exec_ids(&get_subtask_base_slice(&subtasks)).unwrap();
    let subtasks = sm.get_all_subtasks_by_step_and_state(5, STEP_ONE, &SUBTASK_STATE_PENDING).unwrap();
    assert_eq!(subtasks[0].base.exec_id, "tidb2");
    // 2. update 2 subtasks
    testutil::create_sub_task(&sm, 5, STEP_ONE, "tidb1", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);
    let mut subtasks = sm.get_all_subtasks_by_step_and_state(5, STEP_ONE, &SUBTASK_STATE_PENDING).unwrap();
    subtasks[0].base.exec_id = "tidb3".to_owned();
    sm.update_subtasks_exec_ids(&get_subtask_base_slice(&subtasks)).unwrap();
    let subtasks = sm.get_all_subtasks_by_step_and_state(5, STEP_ONE, &SUBTASK_STATE_PENDING).unwrap();
    assert_eq!(subtasks[0].base.exec_id, "tidb3");
    assert_eq!(subtasks[1].base.exec_id, "tidb1");
    // update fail
    sm.update_subtask_state_and_error("tidb1", subtasks[0].base.id, &SUBTASK_STATE_RUNNING, None).unwrap();
    let mut subtasks = sm.get_all_subtasks_by_step_and_state(5, STEP_ONE, &SUBTASK_STATE_PENDING).unwrap();
    assert_eq!(subtasks[0].base.exec_id, "tidb3");
    subtasks[0].base.exec_id = "tidb2".to_owned();
    // update success
    sm.update_subtasks_exec_ids(&get_subtask_base_slice(&subtasks)).unwrap();
    let subtasks = sm.get_all_subtasks_by_step_and_state(5, STEP_ONE, &SUBTASK_STATE_PENDING).unwrap();
    assert_eq!(subtasks[0].base.exec_id, "tidb2");

    // test GetSubtaskErrors
    testutil::create_sub_task(&sm, 7, STEP_ONE, "tidb1", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);
    let subtasks = sm.get_all_subtasks_by_step_and_state(7, STEP_ONE, &SUBTASK_STATE_PENDING).unwrap();
    assert_eq!(subtasks.len(), 1);
    sm.update_subtask_state_and_error("tidb1", subtasks[0].base.id, &SUBTASK_STATE_FAILED, Some(&err("test err")))
        .unwrap();
    let subtask_errs = sm.get_subtask_errors(7).unwrap();
    assert_eq!(subtask_errs.len(), 1);
    assert!(subtask_errs[0].as_ref().unwrap().to_string().contains("test err"));
}

/// Go `TestBothTaskAndSubTaskTable`.
#[test]
fn test_both_task_and_sub_task_table() {
    let (_stack, sm) = init_table_test();
    sm.init_meta(":4000", "").unwrap();
    let id = sm.create_task("key1", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    assert_eq!(id, 1);

    let task = testutil::get_one_task(&sm).unwrap().unwrap();
    assert_eq!(task.base.state, TASK_STATE_PENDING);

    // isSubTaskRevert: false
    let sub_tasks = [
        Subtask::new(STEP_ONE, task.base.id, TASK_TYPE_EXAMPLE, "instance1", 1, b"m1".to_vec(), 1),
        Subtask::new(STEP_ONE, task.base.id, TASK_TYPE_EXAMPLE, "instance2", 1, b"m2".to_vec(), 2),
    ];
    sm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &sub_tasks).unwrap();

    let task = sm.get_task_by_id(1).unwrap();
    assert_eq!(task.base.state, TASK_STATE_RUNNING);

    let subtask1 = sm.get_first_subtask_in_states("instance1", 1, STEP_ONE, &[SUBTASK_STATE_PENDING]).unwrap().unwrap();
    assert_eq!(subtask1.base.id, 1);
    assert_eq!(subtask1.base.tp, TASK_TYPE_EXAMPLE);
    assert_eq!(subtask1.meta, b"m1");

    let subtask2 = sm.get_first_subtask_in_states("instance2", 1, STEP_ONE, &[SUBTASK_STATE_PENDING]).unwrap().unwrap();
    assert_eq!(subtask2.base.id, 2);
    assert_eq!(subtask2.base.tp, TASK_TYPE_EXAMPLE);
    assert_eq!(subtask2.meta, b"m2");

    let cnt_by_states = sm.get_subtask_cnt_group_by_states(1, STEP_ONE).unwrap();
    assert_eq!(cnt_by_states.len(), 1);
    assert_eq!(cnt_by_states[&SUBTASK_STATE_PENDING], 2);
}

/// Go `TestGetSubtaskCntByStates`.
#[test]
fn test_get_subtask_cnt_by_states() {
    let (_stack, sm) = init_table_test();
    testutil::insert_subtask(&sm, 1, STEP_ONE, "tidb1", None, &SUBTASK_STATE_PENDING, &tp("test"), 1);
    testutil::insert_subtask(&sm, 1, STEP_ONE, "tidb1", None, &SUBTASK_STATE_PENDING, &tp("test"), 1);
    testutil::insert_subtask(&sm, 1, STEP_ONE, "tidb1", None, &SUBTASK_STATE_RUNNING, &tp("test"), 1);
    testutil::insert_subtask(&sm, 1, STEP_ONE, "tidb1", None, &SUBTASK_STATE_SUCCEED, &tp("test"), 1);
    testutil::insert_subtask(&sm, 1, STEP_ONE, "tidb1", None, &SUBTASK_STATE_FAILED, &tp("test"), 1);
    testutil::insert_subtask(&sm, 1, STEP_TWO, "tidb1", None, &SUBTASK_STATE_FAILED, &tp("test"), 1);
    let cnt_by_states = sm.get_subtask_cnt_group_by_states(1, STEP_ONE).unwrap();
    assert_eq!(cnt_by_states.len(), 4);
    assert_eq!(cnt_by_states[&SUBTASK_STATE_PENDING], 2);
    assert_eq!(cnt_by_states[&SUBTASK_STATE_RUNNING], 1);
    assert_eq!(cnt_by_states[&SUBTASK_STATE_SUCCEED], 1);
    assert_eq!(cnt_by_states[&SUBTASK_STATE_FAILED], 1);
    let cnt_by_states = sm.get_subtask_cnt_group_by_states(1, STEP_TWO).unwrap();
    assert_eq!(cnt_by_states.len(), 1);
    assert_eq!(cnt_by_states[&SUBTASK_STATE_FAILED], 1);

    let task_id = 2;
    let failed_with_err = testutil::create_sub_task(&sm, task_id, STEP_ONE, "tidb1", None, &TASK_TYPE_EXAMPLE, 1);
    sm.update_subtask_state_and_error("tidb1", failed_with_err, &SUBTASK_STATE_FAILED, Some(&err("subtask failed")))
        .unwrap();
    let failed_without_err = testutil::create_sub_task(&sm, task_id, STEP_ONE, "tidb2", None, &TASK_TYPE_EXAMPLE, 1);
    sm.update_subtask_state_and_error("tidb2", failed_without_err, &SUBTASK_STATE_FAILED, None).unwrap();
    let canceled_without_err = testutil::create_sub_task(&sm, task_id, STEP_ONE, "tidb3", None, &TASK_TYPE_EXAMPLE, 1);
    sm.update_subtask_state_and_error("tidb3", canceled_without_err, &SUBTASK_STATE_CANCELED, None).unwrap();

    let (cnt_by_states, sub_task_errs) = sm.get_subtask_state_cnt_and_errors_by_step(task_id, STEP_ONE).unwrap();
    assert_eq!(cnt_by_states[&SUBTASK_STATE_FAILED], 2);
    assert_eq!(cnt_by_states[&SUBTASK_STATE_CANCELED], 1);
    assert_eq!(sub_task_errs.len(), 1);
    assert!(sub_task_errs[0].to_string().contains("subtask failed"));
}

fn node(id: &str, role: &str, cpu_count: isize) -> ManagedNode {
    ManagedNode { id: id.to_owned(), role: role.to_owned(), cpu_count }
}

/// Go `TestDistFrameworkMeta`.
#[test]
fn test_dist_framework_meta() {
    use tidb_dxf_storage::{get_node_resource, set_node_resource};

    let (_stack, sm) = init_table_test();
    let origin_node_resource = get_node_resource();

    // when no node
    let error = sm.get_cpu_count_of_node().unwrap_err();
    assert!(error.to_string().contains("no managed nodes"), "{error}");
    set_node_resource(NodeResource::new(0, 0, 0));
    sm.init_meta(":4000", "background").unwrap();
    let error = sm.get_cpu_count_of_node().unwrap_err();
    assert!(error.to_string().contains("no managed node have enough resource"), "{error}");

    set_node_resource(NodeResource::new(100, 100, 100));
    sm.init_meta(":4000", "background").unwrap();
    set_node_resource(NodeResource::new(8, 8, 100));
    sm.init_meta(":4001", "").unwrap();
    sm.init_meta(":4002", "background").unwrap();
    assert_eq!(
        sm.get_all_nodes().unwrap(),
        [node(":4000", "background", 100), node(":4001", "", 8), node(":4002", "background", 8)]
    );

    set_node_resource(NodeResource::new(100, 100, 100));
    sm.init_meta(":4002", "").unwrap();
    sm.init_meta(":4003", "background").unwrap();

    assert_eq!(
        sm.get_all_nodes().unwrap(),
        [
            node(":4000", "background", 100),
            node(":4001", "", 8),
            node(":4002", "", 100),
            node(":4003", "background", 100),
        ]
    );
    assert_eq!(sm.get_cpu_count_of_node().unwrap(), 100);

    sm.init_meta(":4002", "background").unwrap();

    sm.delete_dead_nodes(&[":4000".to_owned()]).unwrap();
    assert_eq!(
        sm.get_all_nodes().unwrap(),
        [node(":4001", "", 8), node(":4002", "background", 100), node(":4003", "background", 100)]
    );

    sm.delete_dead_nodes(&[":4003".to_owned()]).unwrap();
    assert_eq!(sm.get_all_nodes().unwrap(), [node(":4001", "", 8), node(":4002", "background", 100)]);

    sm.delete_dead_nodes(&[":4002".to_owned()]).unwrap();
    assert_eq!(sm.get_all_nodes().unwrap(), [node(":4001", "", 8)]);
    assert_eq!(sm.get_cpu_count_of_node().unwrap(), 8);

    sm.recover_meta(":4002", "background").unwrap();
    assert_eq!(sm.get_all_nodes().unwrap(), [node(":4001", "", 8), node(":4002", "background", 100)]);
    // should not reset role
    sm.recover_meta(":4002", "").unwrap();
    assert_eq!(sm.get_all_nodes().unwrap(), [node(":4001", "", 8), node(":4002", "background", 100)]);

    set_node_resource(NodeResource::new(100, 100, 100));
    sm.init_meta(":4000", "background").unwrap();
    sm.init_meta(":4001", "background").unwrap();
    set_node_resource(NodeResource::new(8, 8, 100));
    sm.init_meta(":4002", "").unwrap();
    sm.init_meta(":4003", "").unwrap();
    assert_eq!(sm.get_cpu_count_of_node().unwrap(), 100);
    assert_eq!(sm.get_cpu_count_of_node_by_role("").unwrap(), 8);
    assert_eq!(sm.get_cpu_count_of_node_by_role("background").unwrap(), 100);

    set_node_resource(NodeResource::new(5, 5, 100));
    sm.init_meta(":4004", "background").unwrap();
    let limited_node = sm.get_all_nodes().unwrap().into_iter().find(|n| n.id == ":4004").unwrap_or_default();
    assert_eq!(limited_node, node(":4004", "background", 5));

    set_node_resource(NodeResource::new(1, 1, 100));
    sm.recover_meta(":4004", "").unwrap();
    let limited_node = sm.get_all_nodes().unwrap().into_iter().find(|n| n.id == ":4004").unwrap_or_default();
    assert_eq!(limited_node, node(":4004", "background", 1));

    set_node_resource(origin_node_resource);
}

/// Go `TestSubtaskHistoryTable`.
#[test]
fn test_subtask_history_table() {
    let (_stack, sm) = init_table_test();

    const TASK_ID: i64 = 1;
    const TASK_ID2: i64 = 2;
    const TIDB1: &str = "tidb1";
    const TIDB2: &str = "tidb2";
    const TIDB3: &str = "tidb3";
    const META: &[u8] = b"test";
    const FINISHED_META: &[u8] = b"finished";

    let sub_task1 = testutil::create_sub_task(&sm, TASK_ID, STEP_INIT, TIDB1, Some(META), &TASK_TYPE_EXAMPLE, 11);
    sm.finish_subtask(TIDB1, sub_task1, FINISHED_META).unwrap();
    let sub_task2 = testutil::create_sub_task(&sm, TASK_ID, STEP_INIT, TIDB2, Some(META), &TASK_TYPE_EXAMPLE, 11);
    sm.update_subtask_state_and_error(TIDB2, sub_task2, &SUBTASK_STATE_CANCELED, None).unwrap();
    let sub_task3 = testutil::create_sub_task(&sm, TASK_ID, STEP_INIT, TIDB3, Some(META), &TASK_TYPE_EXAMPLE, 11);
    sm.update_subtask_state_and_error(TIDB3, sub_task3, &SUBTASK_STATE_FAILED, None).unwrap();

    assert_eq!(testutil::get_subtasks_by_task_id(&sm, TASK_ID).unwrap().len(), 3);
    assert_eq!(testutil::get_subtasks_from_history(&sm).unwrap(), 0);
    assert_eq!(sm.get_subtasks_with_history(TASK_ID, STEP_INIT).unwrap().len(), 3);

    // test TransferSubTasks2History
    testutil::transfer_sub_tasks2_history(&sm, TASK_ID).unwrap();

    assert!(testutil::get_subtasks_by_task_id(&sm, TASK_ID).unwrap().is_empty());
    assert_eq!(testutil::get_subtasks_from_history(&sm).unwrap(), 3);
    assert_eq!(sm.get_subtasks_with_history(TASK_ID, STEP_INIT).unwrap().len(), 3);

    // test GC history table.
    fail::cfg("subtaskHistoryKeepSeconds", "return(1)").unwrap();
    std::thread::sleep(Duration::from_secs(2));

    let sub_task4 = testutil::create_sub_task(&sm, TASK_ID2, STEP_INIT, TIDB1, Some(META), &TASK_TYPE_EXAMPLE, 11);
    sm.update_subtask_state_and_error(TIDB1, sub_task4, &SUBTASK_STATE_FAILED, None).unwrap();
    testutil::transfer_sub_tasks2_history(&sm, TASK_ID2).unwrap();

    sm.gc_subtasks().unwrap();
    fail::remove("subtaskHistoryKeepSeconds");

    assert_eq!(testutil::get_subtasks_from_history(&sm).unwrap(), 1);
}

/// Go `TestTransferTasks2HistoryWithAdjacentLargeTaskIDs`.
#[test]
fn test_transfer_tasks2_history_with_adjacent_large_task_ids() {
    let (stack, tm) = init_table_test();
    let mut tk = stack.factory.open_session(session_context(305)).unwrap();
    tm.init_meta(":4000", "").unwrap();

    // task_key is VARCHAR, so formatting task IDs as unquoted SQL integers makes TiDB
    // use a numeric comparison and cast task_key to DOUBLE. DOUBLE has 53 bits of
    // integer precision: 2^53 is exact, but 2^53+1 rounds to the same value. As a
    // result, task_key IN (9007199254740992) can also match the string
    // "9007199254740993", causing the adjacent task's subtask to be moved and deleted.
    // Quoting the IDs keeps the comparison in the string domain and avoids this collision.
    const FIRST_EXPECTED_TASK_ID: i64 = 1 << 53;
    rows(&mut tk, &format!("alter table mysql.tidb_global_task auto_increment = {FIRST_EXPECTED_TASK_ID}"));
    let first_task_id = tm
        .create_task("first-large-id", &TASK_TYPE_EXAMPLE, "", 1, "", 0, &ExtraParams::default(), b"first-original")
        .unwrap();
    assert_eq!(first_task_id, FIRST_EXPECTED_TASK_ID);
    let second_task_id = tm
        .create_task("second-large-id", &TASK_TYPE_EXAMPLE, "", 1, "", 0, &ExtraParams::default(), b"second-original")
        .unwrap();
    assert_eq!(second_task_id, first_task_id + 1);

    let first_subtask_id = testutil::insert_subtask(
        &tm, first_task_id, STEP_ONE, "tidb1", Some(EMPTY_META), &SUBTASK_STATE_RUNNING, &TASK_TYPE_EXAMPLE, 1,
    );
    let second_subtask_id = testutil::insert_subtask(
        &tm, second_task_id, STEP_ONE, "tidb1", Some(EMPTY_META), &SUBTASK_STATE_RUNNING, &TASK_TYPE_EXAMPLE, 1,
    );
    let mut first_task = tm.get_task_by_id(first_task_id).unwrap();
    first_task.meta = b"first-redacted".to_vec();
    tm.transfer_tasks2_history(&[first_task]).unwrap();

    assert_eq!(
        displayed(rows(&mut tk, "select id, meta from mysql.tidb_global_task_history order by id")),
        [[first_task_id.to_string(), "first-redacted".to_owned()]]
    );
    assert_eq!(
        displayed(rows(&mut tk, &format!("select count(*) from mysql.tidb_global_task where id = {first_task_id}"))),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(&mut tk, &format!("select meta from mysql.tidb_global_task where id = {second_task_id}"))),
        [["second-original"]]
    );
    assert_eq!(
        displayed(rows(
            &mut tk,
            &format!("select count(*) from mysql.tidb_global_task_history where id = {second_task_id}")
        )),
        [["0"]]
    );
    assert_eq!(
        displayed(rows(
            &mut tk,
            &format!("select count(*) from mysql.tidb_background_subtask_history where id = {first_subtask_id}")
        )),
        [["1"]]
    );
    assert_eq!(
        displayed(rows(
            &mut tk,
            &format!("select task_key from mysql.tidb_background_subtask where id = {second_subtask_id}")
        )),
        [[second_task_id.to_string()]]
    );
    assert_eq!(
        displayed(rows(
            &mut tk,
            &format!("select count(*) from mysql.tidb_background_subtask_history where id = {second_subtask_id}")
        )),
        [["0"]]
    );
}

/// Go `TestCleanupTasksAreBatchLimited`.
#[test]
fn test_cleanup_tasks_are_batch_limited() {
    let restore = set_task_cleanup_batch_size_for_test(2);
    let (_stack, gm) = init_table_test();
    gm.init_meta(":4000", "").unwrap();

    let task_ids: Vec<i64> = (0..5)
        .map(|i| {
            gm.create_task(&format!("cleanup-batch-{i}"), &TASK_TYPE_EXAMPLE, "", 1, "", 0, &ExtraParams::default(), b"")
                .unwrap()
        })
        .collect();

    let tasks: Vec<Task> = task_ids.iter().map(|task_id| gm.get_task_by_id(*task_id).unwrap()).collect();
    for (i, task) in tasks.iter().enumerate() {
        match i % 3 {
            0 => gm.fail_task(task.base.id, &TASK_STATE_PENDING, Some(&err("cleanup test"))).unwrap(),
            1 => {
                gm.switch_task_step(task, &TASK_STATE_RUNNING, STEP_ONE, &[]).unwrap();
                gm.succeed_task(task.base.id).unwrap();
            }
            _ => {
                gm.switch_task_step(task, &TASK_STATE_RUNNING, STEP_ONE, &[]).unwrap();
                gm.revert_task(task.base.id, &TASK_STATE_RUNNING, None).unwrap();
                gm.reverted_task(task.base.id).unwrap();
            }
        }
    }
    let pending_task_id =
        gm.create_task("not-ready-for-cleanup", &TASK_TYPE_EXAMPLE, "", 1, "", 0, &ExtraParams::default(), b"").unwrap();

    let mut cleanup_tasks = gm.get_cleanup_tasks().unwrap();
    assert_eq!(cleanup_tasks.len(), 2);
    let mut cleaned_states = Vec::with_capacity(tasks.len());
    while !cleanup_tasks.is_empty() {
        cleaned_states.extend(cleanup_tasks.iter().map(|task| task.base.state.to_string()));
        gm.transfer_tasks2_history(&cleanup_tasks).unwrap();
        cleanup_tasks = gm.get_cleanup_tasks().unwrap();
    }
    cleaned_states.sort();
    let mut expected = [
        TASK_STATE_FAILED.to_string(),
        TASK_STATE_SUCCEED.to_string(),
        TASK_STATE_REVERTED.to_string(),
        TASK_STATE_FAILED.to_string(),
        TASK_STATE_SUCCEED.to_string(),
    ];
    expected.sort();
    assert_eq!(cleaned_states, expected);
    let pending_task = gm.get_task_by_id(pending_task_id).unwrap();
    assert_eq!(pending_task.base.state, TASK_STATE_PENDING);
    restore();
}

/// Go `TestTaskHistoryTable`.
#[test]
fn test_task_history_table() {
    use tidb_terror::{annotate, TerrorCode, TerrorError};

    let (_stack, gm) = init_table_test();

    gm.init_meta(":4000", "").unwrap();
    let first_task_id =
        gm.create_task("1", &TASK_TYPE_EXAMPLE, "", 1, "", 0, &ExtraParams::default(), b"original").unwrap();
    let task_id = gm.create_task("2", &TASK_TYPE_EXAMPLE, "", 1, "", 0, &ExtraParams::default(), b"original").unwrap();

    let mut tasks: Vec<Task> = [first_task_id, task_id].iter().map(|id| gm.get_task_by_id(*id).unwrap()).collect();
    let task_bases = gm.get_task_bases_in_states(&[TASK_STATE_PENDING]).unwrap();
    assert_eq!(format!("{task_bases:?}"), format!("{:?}", [&tasks[0].base, &tasks[1].base].map(Clone::clone)));
    for task in &tasks {
        testutil::insert_subtask(&gm, task.base.id, STEP_ONE, "tidb1", Some(EMPTY_META), &SUBTASK_STATE_RUNNING, &TASK_TYPE_EXAMPLE, 1);
    }
    for (i, task) in tasks.iter_mut().enumerate() {
        task.meta = format!("redacted-{i}").into_bytes();
    }
    let untouched_task_id =
        gm.create_task("3", &TASK_TYPE_EXAMPLE, "", 1, "", 0, &ExtraParams::default(), b"untouched").unwrap();
    testutil::insert_subtask(&gm, untouched_task_id, STEP_ONE, "tidb1", Some(EMPTY_META), &SUBTASK_STATE_RUNNING, &TASK_TYPE_EXAMPLE, 1);
    gm.transfer_tasks2_history(&tasks).unwrap();

    let mut untouched_task = gm.get_task_by_id(untouched_task_id).unwrap();
    assert_eq!(untouched_task.base.state, TASK_STATE_PENDING);
    assert_eq!(testutil::get_tasks_from_history(&gm).unwrap(), 2);
    for (i, old_task) in tasks.iter().enumerate() {
        assert_eq!(testutil::get_subtasks_from_history_by_task_id(&gm, old_task.base.id).unwrap(), 1);

        let history_task = gm.get_task_by_id_with_history(old_task.base.id).unwrap();
        assert_eq!(history_task.meta, format!("redacted-{i}").into_bytes());

        assert!(testutil::get_subtasks_by_task_id(&gm, old_task.base.id).unwrap().is_empty());
    }

    assert_eq!(untouched_task.meta, b"untouched");
    assert_eq!(testutil::get_subtasks_by_task_id(&gm, untouched_task_id).unwrap().len(), 1);
    assert_eq!(testutil::get_subtasks_from_history_by_task_id(&gm, untouched_task_id).unwrap(), 0);

    gm.get_task_by_id_with_history(task_id).unwrap();
    gm.get_task_by_key_with_history("1").unwrap();

    // task with fail transfer
    untouched_task.error = Some(Arc::new(err("mock err")));
    gm.transfer_tasks2_history(&[untouched_task]).unwrap();
    assert_eq!(testutil::get_tasks_from_history(&gm).unwrap(), 3);

    // list history tasks with pagination and keyspace
    for sql in [
        "delete from mysql.tidb_background_subtask",
        "delete from mysql.tidb_background_subtask_history",
        "delete from mysql.tidb_global_task",
        "delete from mysql.tidb_global_task_history",
    ] {
        gm.execute_sql_with_new_session(sql, &[]).unwrap();
    }

    let task_specs: [(&str, &str, Option<std::io::Error>); 5] = [
        ("history-task-1", "ks1", None),
        ("history-task-2", "ks2", None),
        ("history-task-3", "ks1", None),
        ("history-task-4", "ks3", None),
        ("history-task-5", "ks1", Some(err("history task failed"))),
    ];
    let mut all_ids = Vec::with_capacity(task_specs.len());
    let mut tasks_to_transfer = Vec::with_capacity(task_specs.len());
    for (key, keyspace, task_err) in &task_specs {
        let id = gm.create_task(key, &IMPORT_INTO, keyspace, 8, "", 0, &ExtraParams::default(), b"meta").unwrap();
        let task = gm.get_task_by_id(id).unwrap();
        gm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &[]).unwrap();
        match task_err {
            Some(task_err) => gm.fail_task(id, &TASK_STATE_RUNNING, Some(task_err)).unwrap(),
            None => gm.succeed_task(id).unwrap(),
        }
        all_ids.push(id);
        tasks_to_transfer.push(gm.get_task_by_id(id).unwrap());
    }
    gm.transfer_tasks2_history(&tasks_to_transfer).unwrap();

    all_ids.reverse();
    let first_page = gm.list_history_tasks(2, 0, "").unwrap();
    assert_eq!(first_page.items.len(), 2);
    assert_eq!(first_page.approx_total_count, 5);
    assert!(first_page.has_more);
    assert_eq!(first_page.items[0].task_base.id, all_ids[0]);
    assert_eq!(first_page.items[1].task_base.id, all_ids[1]);
    assert_eq!(first_page.next_page_token, all_ids[1]);
    assert_eq!(first_page.items[0].task_base.key, "history-task-5");
    assert_eq!(first_page.items[0].task_base.keyspace, "ks1");
    assert_eq!(first_page.items[0].task_base.state, TASK_STATE_FAILED);
    assert!(first_page.items[0].error_code.is_empty());
    assert_eq!(first_page.items[0].error_category, "failed");
    assert!(first_page.items[1].error_code.is_empty());
    assert!(first_page.items[1].error_category.is_empty());
    assert_ne!(first_page.items[0].task_base.create_time, go_zero_time());
    assert_ne!(first_page.items[0].start_time, go_zero_time());
    assert_ne!(first_page.items[0].state_update_time, go_zero_time());
    assert_ne!(first_page.items[0].end_time, go_zero_time());

    let second_page = gm.list_history_tasks(2, first_page.items[1].task_base.id, "").unwrap();
    assert_eq!(second_page.items.len(), 2);
    assert_eq!(second_page.approx_total_count, 5);
    assert!(second_page.has_more);
    assert_eq!(second_page.items[0].task_base.id, all_ids[2]);
    assert_eq!(second_page.items[1].task_base.id, all_ids[3]);
    assert_eq!(second_page.next_page_token, all_ids[3]);

    let last_page = gm.list_history_tasks(2, second_page.items[1].task_base.id, "").unwrap();
    assert_eq!(last_page.items.len(), 1);
    assert_eq!(last_page.approx_total_count, 5);
    assert!(!last_page.has_more);
    assert_eq!(last_page.items[0].task_base.id, all_ids[4]);
    assert_eq!(last_page.next_page_token, 0);

    let filtered_page = gm.list_history_tasks(1, 0, "ks1").unwrap();
    assert_eq!(filtered_page.items.len(), 1);
    assert_eq!(filtered_page.approx_total_count, 3);
    assert!(filtered_page.has_more);
    assert_eq!(filtered_page.items[0].task_base.keyspace, "ks1");
    assert!(filtered_page.next_page_token > 0);

    let filtered_page_with_token = gm.list_history_tasks(1, filtered_page.items[0].task_base.id, "ks1").unwrap();
    assert_eq!(filtered_page_with_token.items.len(), 1);
    assert_eq!(filtered_page_with_token.approx_total_count, 3);
    assert!(filtered_page_with_token.has_more);
    assert_eq!(filtered_page_with_token.items[0].task_base.keyspace, "ks1");

    for page_size in [0, 201] {
        let error = gm.list_history_tasks(page_size, 0, "").unwrap_err();
        assert!(error.to_string().contains("page size should be within"), "{error}");
    }

    // classify history task errors without messages
    for sql in ["delete from mysql.tidb_global_task", "delete from mysql.tidb_global_task_history"] {
        gm.execute_sql_with_new_session(sql, &[]).unwrap();
    }

    let normalize = |message: &str, rfc_code: &str, code: isize| TerrorError::normalize(message, rfc_code, TerrorCode::new(code));
    let task_specs: Vec<(&str, Box<dyn std::error::Error>, bool)> = vec![
        (
            "named-failure",
            Box::new(annotate(
                Arc::new(
                    normalize("sensitive named failure", "DXF:History:Named", 0)
                        .wrap(Arc::new(err("sensitive inner failure"))),
                ),
                "sensitive annotated context",
            )),
            false,
        ),
        ("two-part-code", Box::new(normalize("sensitive two-part failure", "kv:1062", 0)), false),
        ("code-less-normalized", Box::new(normalize("sensitive code-less failure", "", 0)), false),
        ("plain-failure", Box::new(err("sensitive plain failure")), false),
        ("legacy-kv-code", Box::new(TerrorError::compatible(TerrorCode::new(1062), "sensitive legacy kv failure")), false),
        (
            "numeric-code-only",
            Box::new(TerrorError::compatible(TerrorCode::new(1062), "sensitive numeric-only failure")),
            false,
        ),
        ("cancelled", Box::new(err("cancelled by user")), true),
        ("data-error", Box::new(err("[Lightning:Restore:ErrEncodeKV]Value conversion failed for column 'a'")), true),
        ("ordinary-revert", Box::new(err("sensitive ordinary failure")), true),
    ];
    let mut tasks_to_transfer = Vec::with_capacity(task_specs.len());
    for (key, task_err, reverted) in &task_specs {
        let id = gm.create_task(key, &IMPORT_INTO, "ks1", 8, "", 0, &ExtraParams::default(), b"").unwrap();
        let task = gm.get_task_by_id(id).unwrap();
        gm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &[]).unwrap();
        if *reverted {
            gm.revert_task(id, &TASK_STATE_RUNNING, Some(task_err.as_ref())).unwrap();
            gm.reverted_task(id).unwrap();
        } else {
            gm.fail_task(id, &TASK_STATE_RUNNING, Some(task_err.as_ref())).unwrap();
        }
        tasks_to_transfer.push(gm.get_task_by_id(id).unwrap());
    }
    gm.transfer_tasks2_history(&tasks_to_transfer).unwrap();
    let legacy_errors: [(&str, &[u8]); 2] = [
        ("legacy-kv-code", br#"{"class":8,"code":1062,"message":"sensitive legacy kv failure","rfccode":""}"#),
        ("numeric-code-only", br#"{"class":0,"code":1062,"message":"sensitive legacy numeric failure","rfccode":""}"#),
    ];
    for (key, error_data) in legacy_errors {
        gm.execute_sql_with_new_session(
            "
\t\t\t\tupdate mysql.tidb_global_task_history
\t\t\t\tset error = %?
\t\t\t\twhere task_key = %?",
            &[tidb_sqlescape::SqlArg::Bytes(Some(error_data)), tidb_sqlescape::SqlArg::String(key.as_bytes())],
        )
        .unwrap();
    }

    let page = gm.list_history_tasks(task_specs.len() as isize, 0, "").unwrap();
    assert_eq!(page.items.len(), task_specs.len());
    let items_by_key: std::collections::HashMap<&str, &tidb_dxf_storage::HistoryTaskSummary> =
        page.items.iter().map(|item| (item.task_base.key.as_str(), item)).collect();
    assert_eq!(items_by_key["named-failure"].error_code, "DXF:History:Named");
    assert_eq!(items_by_key["named-failure"].error_category, "failed");
    assert_eq!(items_by_key["two-part-code"].error_code, "kv:1062");
    assert_eq!(items_by_key["two-part-code"].error_category, "failed");
    assert!(items_by_key["code-less-normalized"].error_code.is_empty());
    assert!(items_by_key["plain-failure"].error_code.is_empty());
    assert_eq!(items_by_key["plain-failure"].error_category, "failed");
    assert_eq!(items_by_key["legacy-kv-code"].error_code, "kv:1062");
    assert_eq!(items_by_key["legacy-kv-code"].error_category, "failed");
    assert_eq!(items_by_key["numeric-code-only"].error_code, "1062");
    assert_eq!(items_by_key["numeric-code-only"].error_category, "failed");
    assert_eq!(items_by_key["cancelled"].error_category, "cancelled");
    assert_eq!(items_by_key["data-error"].error_category, "data-error");
    assert_eq!(items_by_key["ordinary-revert"].error_category, "failed");
}

/// Go `TestPauseAndResume`.
#[test]
fn test_pause_and_resume() {
    let (_stack, sm) = init_table_test();

    for _ in 0..3 {
        testutil::create_sub_task(&sm, 1, STEP_INIT, "tidb1", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);
    }
    // 1.1 pause all subtasks.
    sm.pause_subtasks("tidb1", 1).unwrap();
    let cnt_by_states = sm.get_subtask_cnt_group_by_states(1, STEP_INIT).unwrap();
    assert_eq!(cnt_by_states[&SUBTASK_STATE_PAUSED], 3);
    // 1.2 resume all subtasks.
    sm.resume_subtasks(1).unwrap();
    let cnt_by_states = sm.get_subtask_cnt_group_by_states(1, STEP_INIT).unwrap();
    assert_eq!(cnt_by_states[&SUBTASK_STATE_PENDING], 3);

    // 2.1 pause 2 subtasks.
    sm.update_subtask_state_and_error("tidb1", 1, &SUBTASK_STATE_SUCCEED, None).unwrap();
    sm.pause_subtasks("tidb1", 1).unwrap();
    let cnt_by_states = sm.get_subtask_cnt_group_by_states(1, STEP_INIT).unwrap();
    assert_eq!(cnt_by_states[&SUBTASK_STATE_PAUSED], 2);
    // 2.2 resume 2 subtasks.
    sm.resume_subtasks(1).unwrap();
    let cnt_by_states = sm.get_subtask_cnt_group_by_states(1, STEP_INIT).unwrap();
    assert_eq!(cnt_by_states[&SUBTASK_STATE_PENDING], 2);
}

/// Go `TestCancelAndExecIdChanged`, its exec-id half. The cancelled-context
/// half needs Go's context cancellation, which the internal SQL path does
/// not carry (see the DXF ExecPlan).
#[test]
fn test_cancel_and_exec_id_changed() {
    let (_stack, sm) = init_table_test();

    testutil::create_sub_task(&sm, 1, STEP_INIT, "tidb1", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);
    let subtask = sm.get_first_subtask_in_states("tidb1", 1, STEP_INIT, &[SUBTASK_STATE_PENDING]).unwrap().unwrap();

    // 2. change the exec_id
    // exec_id changed
    testutil::update_subtask_exec_id(&sm, "tidb2", subtask.base.id).unwrap();
    // exec_id in memory unchanged, call UpdateSubtaskStateAndError.
    sm.update_subtask_state_and_error(&subtask.base.exec_id, subtask.base.id, &SUBTASK_STATE_FAILED, None).unwrap();
    let subtask = sm.get_first_subtask_in_states("tidb2", 1, STEP_INIT, &[SUBTASK_STATE_PENDING]).unwrap();
    // state unchanged
    assert!(subtask.is_some());
}

/// Go `TestTaskNotFound`.
#[test]
fn test_task_not_found() {
    let (_stack, gm) = init_table_test();
    let not_found = |result: tidb_dxf_storage::Result<Task>| {
        assert!(result.unwrap_err().is(&tidb_dxf_storage::StorageError::TaskNotFound));
    };
    not_found(gm.get_task_by_id(1));
    not_found(gm.get_task_by_id_with_history(1));
    not_found(gm.get_task_by_key("key"));
    not_found(gm.get_task_by_key_with_history("key"));
}

/// Go `TestInitMeta`.
#[test]
fn test_init_meta() {
    let (stack, sm) = init_table_test();
    let mut tk = stack.factory.open_session(session_context(306)).unwrap();
    let role = |tk: &mut Session| displayed(rows(tk, r#"select role from mysql.dist_framework_meta where host="tidb1""#));
    sm.init_meta("tidb1", "").unwrap();
    assert_eq!(role(&mut tk), [[""]]);
    sm.init_meta("tidb1", "background").unwrap();
    assert_eq!(role(&mut tk), [["background"]]);
    rows(&mut tk, r#"set global tidb_service_scope="""#);
    assert_eq!(displayed(rows(&mut tk, "select @@global.tidb_service_scope")), [[""]]);

    // 1. delete then start.
    sm.delete_dead_nodes(&["tidb1".to_owned()]).unwrap();
    sm.init_meta("tidb1", "").unwrap();
    assert_eq!(role(&mut tk), [[""]]);

    sm.delete_dead_nodes(&["tidb1".to_owned()]).unwrap();
    sm.init_meta("tidb1", "background").unwrap();
    assert_eq!(role(&mut tk), [["background"]]);

    // 2. delete then set.
    sm.delete_dead_nodes(&["tidb1".to_owned()]).unwrap();
    rows(&mut tk, r#"set global tidb_service_scope="""#);
    assert_eq!(displayed(rows(&mut tk, "select @@global.tidb_service_scope")), [[""]]);

    sm.delete_dead_nodes(&["tidb1".to_owned()]).unwrap();
    rows(&mut tk, r#"set global tidb_service_scope="background""#);
    assert_eq!(displayed(rows(&mut tk, "select @@global.tidb_service_scope")), [["background"]]);
    rows(&mut tk, r#"set global tidb_service_scope="""#);
}

/// Go `TestSubtaskType`.
#[test]
fn test_subtask_type() {
    let (_stack, sm) = init_table_test();
    let cases = [TASK_TYPE_EXAMPLE, IMPORT_INTO, BACKFILL, tp("")];
    for (i, c) in cases.iter().enumerate() {
        let task_id = i as i64 + 1;
        testutil::insert_subtask(&sm, task_id, STEP_ONE, "tidb-1", Some(b""), &SUBTASK_STATE_RUNNING, c, 12);
        let subtask =
            sm.get_first_subtask_in_states("tidb-1", task_id, STEP_ONE, &[SUBTASK_STATE_RUNNING]).unwrap().unwrap();
        assert_eq!(&subtask.base.tp, c);
    }
}

/// Go `TestRunningSubtasksBack2Pending`.
#[test]
fn test_running_subtasks_back2_pending() {
    let (_stack, sm) = init_table_test();
    // (task id, exec id, state, update time)
    let mut subtasks = vec![
        (1, "tidb-1".to_owned(), SUBTASK_STATE_PENDING.to_string(), go_zero_time()),
        (1, "tidb-1".to_owned(), SUBTASK_STATE_RUNNING.to_string(), go_zero_time()),
        (1, "tidb-2".to_owned(), SUBTASK_STATE_PENDING.to_string(), go_zero_time()),
        (2, "tidb-1".to_owned(), SUBTASK_STATE_PENDING.to_string(), go_zero_time()),
    ];
    for (task_id, exec_id, state, _) in &subtasks {
        testutil::insert_subtask(&sm, *task_id, STEP_ONE, exec_id, Some(b""), &SubtaskState::new(state.clone()), &TASK_TYPE_EXAMPLE, 12);
    }

    let get_all_subtasks = || {
        sm.execute_sql_with_new_session(
            "
\t\t\t\tselect cast(task_key as signed), exec_id, state, state_update_time
\t\t\t\tfrom mysql.tidb_background_subtask
\t\t\t\torder by task_key, exec_id, state",
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|r| {
            let displayed = displayed(vec![r.clone()]).remove(0);
            let update_time = match &r[3] {
                Datum::Null => go_zero_time(),
                Datum::Int(seconds) => Local.timestamp_opt(*seconds, 0).unwrap().fixed_offset(),
                Datum::UInt(seconds) => Local.timestamp_opt(*seconds as i64, 0).unwrap().fixed_offset(),
                other => panic!("state_update_time {other:?}"),
            };
            (displayed[0].parse::<i64>().unwrap(), displayed[1].clone(), displayed[2].clone(), update_time)
        })
        .collect::<Vec<_>>()
    };

    assert_eq!(subtasks, get_all_subtasks());
    sm.running_subtasks_back2_pending(&[]).unwrap();
    assert_eq!(subtasks, get_all_subtasks());

    let active_subtasks = sm.get_active_subtasks(1).unwrap();
    assert_eq!(active_subtasks.len(), 3);
    let start_time = now_secs();
    // this list contains running and pending subtasks, just for test.
    sm.running_subtasks_back2_pending(&active_subtasks).unwrap();
    let mut all_subtasks = get_all_subtasks();
    assert!(all_subtasks[1].3 >= start_time);
    all_subtasks[1].3 = go_zero_time();
    subtasks[1].2 = SUBTASK_STATE_PENDING.to_string();
    assert_eq!(subtasks, all_subtasks);
}

/// Go `TestSubtasksState`.
#[test]
fn test_subtasks_state() {
    let (_stack, sm) = init_table_test();
    let ts = Local::now().fixed_offset();
    std::thread::sleep(Duration::from_secs(1));
    // 1. test FailSubtask do update start/update time
    testutil::create_sub_task(&sm, 3, STEP_INIT, "for_test", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);
    sm.fail_subtask("for_test", 3, Some(&err("fail"))).unwrap();
    let subtask = sm.get_first_subtask_in_states("for_test", 3, STEP_INIT, &[SUBTASK_STATE_FAILED]).unwrap().unwrap();
    assert_eq!(subtask.base.state, SUBTASK_STATE_FAILED);
    assert!(subtask.base.start_time > ts);
    assert!(subtask.update_time > ts);

    let end_time = testutil::get_subtask_end_time(&sm, subtask.base.id).unwrap();
    assert!(end_time > ts);

    // 2. test FinishSubtask do update update time
    testutil::create_sub_task(&sm, 4, STEP_INIT, "for_test1", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);
    let subtask =
        sm.get_first_subtask_in_states("for_test1", 4, STEP_INIT, &[SUBTASK_STATE_PENDING]).unwrap().unwrap();
    sm.start_subtask(subtask.base.id, "for_test1").unwrap();

    let subtask =
        sm.get_first_subtask_in_states("for_test1", 4, STEP_INIT, &[SUBTASK_STATE_RUNNING]).unwrap().unwrap();
    assert!(subtask.base.start_time > ts);
    assert!(subtask.update_time > ts);
    let ts = Local::now().fixed_offset();
    std::thread::sleep(Duration::from_secs(1));
    sm.finish_subtask("for_test1", subtask.base.id, &[]).unwrap();
    let subtask2 =
        sm.get_first_subtask_in_states("for_test1", 4, STEP_INIT, &[SUBTASK_STATE_SUCCEED]).unwrap().unwrap();
    assert_eq!(subtask2.base.start_time, subtask.base.start_time);
    assert!(subtask2.update_time > subtask.update_time);
    let end_time = testutil::get_subtask_end_time(&sm, subtask.base.id).unwrap();
    assert!(end_time > ts);

    // 3. test CancelSubtask
    testutil::create_sub_task(&sm, 3, STEP_INIT, "for_test", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);
    sm.cancel_subtask("for_test", 3).unwrap();
    let subtask = sm.get_first_subtask_in_states("for_test", 3, STEP_INIT, &[SUBTASK_STATE_CANCELED]).unwrap().unwrap();
    assert_eq!(subtask.base.state, SUBTASK_STATE_CANCELED);
    assert!(subtask.base.start_time > ts);
    assert!(subtask.update_time > ts);

    let end_time = testutil::get_subtask_end_time(&sm, subtask.base.id).unwrap();
    assert!(end_time > ts);

    // 4. CancelSubtask should cancel all pending/running subtasks for one exec/task.
    let subtask_id1 = testutil::create_sub_task(&sm, 5, STEP_INIT, "for_test_multi", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);
    testutil::create_sub_task(&sm, 5, STEP_INIT, "for_test_multi", Some(b"test"), &TASK_TYPE_EXAMPLE, 11);
    sm.start_subtask(subtask_id1, "for_test_multi").unwrap();
    sm.cancel_subtask("for_test_multi", 5).unwrap();
    let cnt_by_states = sm.get_subtask_cnt_group_by_states(5, STEP_INIT).unwrap();
    assert_eq!(cnt_by_states[&SUBTASK_STATE_CANCELED], 2);
    assert_eq!(cnt_by_states.get(&SUBTASK_STATE_PENDING).copied().unwrap_or(0), 0);
    assert_eq!(cnt_by_states.get(&SUBTASK_STATE_RUNNING).copied().unwrap_or(0), 0);
}

/// Go `checkBasicTaskEq`.
fn check_basic_task_eq(expected_task: &TaskBase, task: &TaskBase) {
    assert_eq!(expected_task.id, task.id);
    assert_eq!(expected_task.key, task.key);
    assert_eq!(expected_task.tp, task.tp);
    assert_eq!(expected_task.state, task.state);
    assert_eq!(expected_task.step, task.step);
    assert_eq!(expected_task.priority, task.priority);
    assert_eq!(expected_task.required_slots, task.required_slots);
    assert_eq!(expected_task.create_time, task.create_time);
}

/// Go `TestGetActiveTaskExecInfo`.
#[test]
fn test_get_active_task_exec_info() {
    let (_stack, tm) = init_table_test();

    tm.init_meta(":4000", "").unwrap();
    let task_states = [TASK_STATE_RUNNING, TASK_STATE_REVERTING, TASK_STATE_REVERTING, TASK_STATE_PAUSING];
    let mut tasks = Vec::with_capacity(task_states.len());
    for (i, expected_state) in task_states.iter().enumerate() {
        let task_id =
            tm.create_task(&format!("key-{i}"), &TASK_TYPE_EXAMPLE, "", 8, "", 0, &ExtraParams::default(), b"").unwrap();
        let mut task = tm.get_task_by_id(task_id).unwrap();
        tm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_TWO, &[]).unwrap();
        task.base.state = expected_state.clone();
        task.base.step = STEP_TWO;
        if *expected_state == TASK_STATE_REVERTING {
            tm.revert_task(task.base.id, &TASK_STATE_RUNNING, None).unwrap();
        } else if *expected_state == TASK_STATE_PAUSING {
            tm.pause_task(&task.base.key).unwrap();
        }
        tasks.push(task);
    }
    // mock a pending subtask of step 1, this should not happen, just for test
    let insert = |task: &Task, step: Step, exec_id: &str, state: &SubtaskState, concurrency: isize| {
        testutil::insert_subtask(&tm, task.base.id, step, exec_id, Some(b"test"), state, &TASK_TYPE_EXAMPLE, concurrency);
    };
    insert(&tasks[0], STEP_ONE, ":4000", &SUBTASK_STATE_PENDING, 4);
    insert(&tasks[0], STEP_TWO, ":4000", &SUBTASK_STATE_PENDING, 4);
    insert(&tasks[0], STEP_TWO, ":4000", &SUBTASK_STATE_RUNNING, 4);
    insert(&tasks[0], STEP_TWO, ":4001", &SUBTASK_STATE_SUCCEED, 4);
    insert(&tasks[0], STEP_TWO, ":4001", &SUBTASK_STATE_PENDING, 4);
    // task 1 has no subtask
    insert(&tasks[2], STEP_TWO, ":4001", &SUBTASK_STATE_PENDING, 6);
    insert(&tasks[3], STEP_TWO, ":4001", &SUBTASK_STATE_RUNNING, 8);

    tm.get_active_subtasks(1).unwrap();
    // :4000
    let task_exec_infos = tm.get_task_exec_info_by_exec_id(":4000").unwrap();
    assert_eq!(task_exec_infos.len(), 1);
    check_basic_task_eq(&tasks[0].base, &task_exec_infos[0].task_base);
    assert_eq!(task_exec_infos[0].subtask_concurrency, 4);
    // :4001
    let task_exec_infos = tm.get_task_exec_info_by_exec_id(":4001").unwrap();
    assert_eq!(task_exec_infos.len(), 3);
    check_basic_task_eq(&tasks[0].base, &task_exec_infos[0].task_base);
    assert_eq!(task_exec_infos[0].subtask_concurrency, 4);
    check_basic_task_eq(&tasks[2].base, &task_exec_infos[1].task_base);
    assert_eq!(task_exec_infos[1].subtask_concurrency, 6);
    check_basic_task_eq(&tasks[3].base, &task_exec_infos[2].task_base);
    assert_eq!(task_exec_infos[2].subtask_concurrency, 8);
    // :4002, no such subtasks
    assert!(tm.get_task_exec_info_by_exec_id(":4002").unwrap().is_empty());
}

/// Go `TestTaskManagerEntrySize`.
#[test]
fn test_task_manager_entry_size() {
    let (stack, tm) = init_table_test();
    let get_meta = |l: usize| vec![b'a'; l];
    let insert_subtask = |meta: &[u8]| {
        let state = SUBTASK_STATE_PENDING.to_string();
        tm.with_new_session(&mut |se| {
            tidb_dxf_storage::exec_sql(
                &tidb_sqlexec::BackgroundContext,
                se,
                &format!(
                    "
\t\t\tinsert into mysql.tidb_background_subtask({}) values(%?, %?, %?, %?, %?, %?, %?, NULL, CURRENT_TIMESTAMP(), '{{}}', '{{}}')",
                    tidb_dxf_storage::INSERT_SUBTASK_COLUMNS
                ),
                &[
                    tidb_sqlescape::SqlArg::Signed(1),
                    tidb_sqlescape::SqlArg::String(b"1"),
                    tidb_sqlescape::SqlArg::String(b"execID"),
                    tidb_sqlescape::SqlArg::Bytes(Some(meta)),
                    tidb_sqlescape::SqlArg::String(state.as_bytes()),
                    tidb_sqlescape::SqlArg::Signed(type_2_int(&TASK_TYPE_EXAMPLE) as i64),
                    tidb_sqlescape::SqlArg::Signed(1),
                ],
            )
            .map(drop)
        })
    };
    let meta6m = get_meta(6 << 20);
    let error = insert_subtask(&meta6m).unwrap_err();
    assert!(error.to_string().contains("entry too large"), "{error}");
    let mut tk = stack.factory.open_session(session_context(307)).unwrap();
    rows(&mut tk, &format!("set global tidb_txn_entry_size_limit = {}", 7 << 20));
    insert_subtask(&meta6m).unwrap();
    // TiKV also have a limit raftstore.raft-entry-max-size which is 8M by default,
    // we won't test that param here
}

/// Go `TestTaskState` (`task_state_test.go`).
#[test]
fn test_task_state() {
    let (_stack, gm) = init_table_test();

    gm.init_meta(":4000", "").unwrap();

    // 1. cancel task
    let id = gm.create_task("key1", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    gm.cancel_task(id).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_CANCELLING, STEP_INIT);

    // 2. cancel task by key session
    let id = gm.create_task("key2", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    gm.with_new_txn(&mut |se| TaskManager::cancel_task_by_key_session(se, "key2")).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_CANCELLING, STEP_INIT);

    // 3. fail task
    let id = gm.create_task("key3", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    gm.fail_task(id, &TASK_STATE_PENDING, Some(&err("test err"))).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_FAILED, STEP_INIT);
    assert!(task.error.as_ref().unwrap().to_string().contains("test err"));

    // 4. Reverted task
    let id = gm.create_task("key4", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_PENDING, STEP_INIT);
    gm.revert_task(task.base.id, &TASK_STATE_PENDING, None).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_REVERTING, STEP_INIT);

    gm.reverted_task(id).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_REVERTED, STEP_INIT);

    // 5. pause task
    let id = gm.create_task("key5", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    assert!(gm.pause_task("key5").unwrap());
    assert_eq!(gm.get_task_by_id(id).unwrap().base.state, TASK_STATE_PAUSING);

    // 6. paused task
    gm.paused_task(id).unwrap();
    assert_eq!(gm.get_task_by_id(id).unwrap().base.state, TASK_STATE_PAUSED);

    // 7. resume task
    assert!(gm.resume_task("key5").unwrap());
    assert_eq!(gm.get_task_by_id(id).unwrap().base.state, TASK_STATE_RESUMING);
    gm.resumed_task(id).unwrap();
    assert_eq!(gm.get_task_by_id(id).unwrap().base.state, TASK_STATE_RUNNING);

    // 8. succeed task
    let id = gm.create_task("key6", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_PENDING, STEP_INIT);
    gm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &[]).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_RUNNING, STEP_ONE);
    gm.succeed_task(id).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    check_task_state_step(&task, &TASK_STATE_SUCCEED, STEP_DONE);
}

/// Go `TestPauseTaskOnError`.
#[test]
fn test_pause_task_on_error() {
    use tidb_terror::{TerrorCode, TerrorError};

    let (_stack, gm) = init_table_test();
    gm.init_meta(":4000", "").unwrap();

    let id =
        gm.create_task("key-pause-on-error", &tp("test"), "", 4, "", 0, &ExtraParams::default(), b"test").unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    let subtasks = [Subtask::new(
        STEP_ONE,
        task.base.id,
        task.base.tp.clone(),
        ":4000",
        task.base.required_slots,
        EMPTY_META.to_vec(),
        1,
    )];
    gm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &subtasks).unwrap();
    let failed_subtasks = gm.get_all_subtasks_by_step_and_state(task.base.id, STEP_ONE, &SUBTASK_STATE_PENDING).unwrap();
    assert_eq!(failed_subtasks.len(), 1);
    // Go `errdef.ErrKVDiskFull` (`pkg/ingestor/errdef`).
    let err_kv_disk_full = TerrorError::normalize("store disk full", "Ingest:StoreDiskFull", TerrorCode::new(0));
    let task_err = err_kv_disk_full.gen_with_stack("store 1 disk full");
    gm.update_subtask_state_and_error(":4000", failed_subtasks[0].base.id, &SUBTASK_STATE_FAILED, Some(&task_err))
        .unwrap();

    let error = gm.pause_task_on_error(task.base.id, &TASK_STATE_PENDING, STEP_ONE, Some(&task_err)).unwrap_err();
    assert!(error.is(&tidb_dxf_storage::StorageError::TaskChanged));
    let paused_subtasks = gm.get_all_subtasks_by_step_and_state(task.base.id, STEP_ONE, &SUBTASK_STATE_PAUSED).unwrap();
    assert!(paused_subtasks.is_empty());

    gm.pause_task_on_error(task.base.id, &TASK_STATE_RUNNING, STEP_ONE, Some(&task_err)).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_PAUSING);
    let stored = task.error.as_ref().unwrap();
    assert_eq!(stored.downcast_ref::<TerrorError>(), Some(&err_kv_disk_full));
    let paused_subtasks = gm.get_all_subtasks_by_step_and_state(task.base.id, STEP_ONE, &SUBTASK_STATE_PAUSED).unwrap();
    assert_eq!(paused_subtasks.len(), 1);

    gm.paused_task(id).unwrap();
    assert!(gm.resume_task("key-pause-on-error").unwrap());
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_RESUMING);
    assert!(task.error.is_none());
    gm.resume_subtasks(id).unwrap();
    let pending_subtasks = gm.get_all_subtasks_by_step_and_state(task.base.id, STEP_ONE, &SUBTASK_STATE_PENDING).unwrap();
    assert_eq!(pending_subtasks.len(), 1);
    assert!(gm.get_subtask_errors(id).unwrap().is_empty());
}

/// Go `TestUpdateTaskExtraParams`.
#[test]
fn test_update_task_extra_params() {
    let (_stack, gm) = init_table_test();
    gm.init_meta(":4000", "").unwrap();
    let params = ExtraParams { manual_recovery: true, prepare_mode: PREPARE_MODE_REQUIRED, ..ExtraParams::default() };
    let id = gm.create_task("key1", &tp("test"), "", 4, "", 0, &params, b"test").unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(format!("{:?}", task.base.extra_params), format!("{params:?}"));
    let params = ExtraParams { max_runtime_slots: 123, ..ExtraParams::default() };
    gm.update_task_extra_params(id, &params).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(format!("{:?}", task.base.extra_params), format!("{params:?}"));

    let params = ExtraParams { target_steps: vec![STEP_ONE], ..ExtraParams::default() };
    gm.update_task_extra_params(id, &params).unwrap();
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(format!("{:?}", task.base.extra_params), format!("{params:?}"));
}

/// Go `checkTaskAfterModify`.
fn check_task_after_modify(
    gm: &TaskManager,
    task_id: i64,
    expect_concurrency: isize,
    expect_max_node: isize,
    expected_meta: &[u8],
    expected_st_concurrencies: &[isize],
) {
    let task = gm.get_task_by_id(task_id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_RUNNING);
    assert_eq!(task.base.required_slots, expect_concurrency);
    assert_eq!(task.base.max_node_count, expect_max_node);
    assert_eq!(task.meta, expected_meta);
    assert_eq!(format!("{:?}", task.modify_param), format!("{:?}", ModifyParam::default()));
    let mut got_subtasks = gm.get_subtasks_with_history(task.base.id, STEP_ONE).unwrap();
    assert_eq!(got_subtasks.len(), expected_st_concurrencies.len());
    got_subtasks.sort_by_key(|subtask| subtask.base.ordinal);
    for (i, expected) in expected_st_concurrencies.iter().enumerate() {
        assert_eq!(got_subtasks[i].base.concurrency, *expected);
    }
}

/// Go `TestModifyTask`.
#[test]
fn test_modify_task() {
    use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

    let (_stack, gm) = init_table_test();
    gm.init_meta(":4000", "").unwrap();

    let id = gm.create_task("key1", &tp("test"), "", 4, "", 1, &ExtraParams::default(), b"test").unwrap();

    let modify = |task_id: i64, prev_state: TaskState| {
        gm.modify_task_by_id(task_id, &ModifyParam { prev_state, ..ModifyParam::default() })
    };
    assert!(modify(id, TASK_STATE_REVERTING).unwrap_err().is(&tidb_dxf_storage::StorageError::TaskStateNotAllow));
    assert!(modify(123123123, TASK_STATE_PAUSED).unwrap_err().is(&tidb_dxf_storage::StorageError::TaskNotFound));
    assert!(modify(id, TASK_STATE_PAUSED).unwrap_err().is(&tidb_dxf_storage::StorageError::TaskChanged));

    // task changed in middle of modifying
    let (tx, rx) = crossbeam_channel::bounded::<()>(0);
    let counter = Arc::new(AtomicI32::new(0));
    {
        let (counter, rx) = (Arc::clone(&counter), rx.clone());
        fail::cfg_callback("beforeMoveToModifying", move || {
            if counter.fetch_add(1, Ordering::SeqCst) + 1 == 1 {
                rx.recv().unwrap();
                rx.recv().unwrap();
            }
        })
        .unwrap();
    }
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_PENDING);
    assert_eq!(task.base.max_node_count, 1);
    let subtasks: Vec<Subtask> = (0..4)
        .map(|i| {
            Subtask::new(STEP_ONE, task.base.id, task.base.tp.clone(), ":4000", task.base.required_slots, EMPTY_META.to_vec(), i + 1)
        })
        .collect();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            tx.send(()).unwrap();
            gm.switch_task_step(&task, &TASK_STATE_RUNNING, STEP_ONE, &subtasks).unwrap();
            tx.send(()).unwrap();
        });
        assert!(modify(id, TASK_STATE_PENDING).unwrap_err().is(&tidb_dxf_storage::StorageError::TaskChanged));
    });
    fail::remove("beforeMoveToModifying");

    // move to 'modifying' success
    let task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_RUNNING);
    let param = ModifyParam {
        prev_state: TASK_STATE_RUNNING,
        modifications: vec![
            Modification { tp: MODIFY_REQUIRED_SLOTS, to: 2 },
            Modification { tp: MODIFY_MAX_NODE_COUNT, to: 3 },
        ],
    };
    gm.modify_task_by_id(id, &param).unwrap();
    let mut task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_MODIFYING);
    assert_eq!(format!("{:?}", task.modify_param), format!("{param:?}"));

    // modified
    let mut got_subtasks = gm.get_subtasks_with_history(task.base.id, STEP_ONE).unwrap();
    got_subtasks.sort_by_key(|subtask| subtask.base.ordinal);
    assert_eq!(got_subtasks.len(), subtasks.len());
    gm.finish_subtask(&got_subtasks[0].base.exec_id, got_subtasks[0].base.id, &[]).unwrap();
    gm.start_subtask(got_subtasks[1].base.id, &got_subtasks[1].base.exec_id).unwrap();
    gm.execute_sql_with_new_session(
        "update mysql.tidb_background_subtask set state='paused' where id=%?",
        &[tidb_sqlescape::SqlArg::Signed(got_subtasks[2].base.id)],
    )
    .unwrap();
    task.base.required_slots = 2;
    task.base.max_node_count = 3;
    task.meta = b"modified".to_vec();
    gm.modified_task(&task).unwrap();
    check_task_after_modify(&gm, task.base.id, 2, 3, b"modified", &[4, 2, 2, 2]);

    // task state changed before move to 'modified'
    let param = ModifyParam {
        prev_state: TASK_STATE_RUNNING,
        modifications: vec![Modification { tp: MODIFY_REQUIRED_SLOTS, to: 3 }],
    };
    gm.modify_task_by_id(id, &param).unwrap();
    let mut task = gm.get_task_by_id(id).unwrap();
    assert_eq!(task.base.state, TASK_STATE_MODIFYING);
    assert_eq!(format!("{:?}", task.modify_param), format!("{param:?}"));
    let (tx, rx) = crossbeam_channel::bounded::<()>(0);
    let called = Arc::new(AtomicBool::new(false));
    {
        let called = Arc::clone(&called);
        fail::cfg_callback("beforeModifiedTask", move || {
            if called.swap(true, Ordering::SeqCst) {
                return;
            }
            rx.recv().unwrap();
            rx.recv().unwrap();
        })
        .unwrap();
    }
    let mut task_clone = task.clone();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            tx.send(()).unwrap();
            // NOTE: this will NOT happen in real case, because the task can NOT move
            // to 'modifying' again to change modify params.
            // here just to show that if another client finishes modifying, our modify
            // will skip silently.
            task_clone.base.required_slots = 5;
            task_clone.base.max_node_count = 50;
            task_clone.meta = b"modified-other".to_vec();
            gm.modified_task(&task_clone).unwrap();
            tx.send(()).unwrap();
        });
        task.base.required_slots = 3;
        task.base.max_node_count = 30;
        task.meta = b"modified2".to_vec();
        gm.modified_task(&task).unwrap();
    });
    fail::remove("beforeModifiedTask");
    check_task_after_modify(&gm, task.base.id, 5, 50, b"modified-other", &[4, 5, 5, 5]);
}
