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
