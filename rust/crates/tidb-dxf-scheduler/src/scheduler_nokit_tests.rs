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

//! Go `scheduler_nokit_test.go`.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tidb_dxf::{
    ExtraParams, Modification, ModifyParam, Step, SubtaskState, Task, TaskBase, MODIFY_MAX_WRITE_SPEED,
    MODIFY_REQUIRED_SLOTS, PREPARE_MODE_REQUIRED, STEP_DONE, STEP_INIT, STEP_ONE, STEP_PREPARED, STEP_TWO,
    SUBTASK_STATE_CANCELED, SUBTASK_STATE_FAILED, SUBTASK_STATE_PAUSED, SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING,
    SUBTASK_STATE_SUCCEED, TASK_STATE_CANCELLING, TASK_STATE_MODIFYING, TASK_STATE_PAUSED, TASK_STATE_PAUSING,
    TASK_STATE_PENDING, TASK_STATE_RESUMING, TASK_STATE_REVERTED, TASK_STATE_REVERTING, TASK_STATE_RUNNING,
    TASK_STATE_SUCCEED, TASK_TYPE_EXAMPLE,
};
use tidb_dxf_storage::{StorageError, SubtaskError};
use tidb_gocontext::Context;

use super::{get_eligible_nodes, is_step_succeed, plain, should_pause_on_kv_disk_full, BaseScheduler};
use crate::interface::{Error, Extension, Param};
use crate::mock::{MockExtension, MockTaskManager, TaskAnswer, TaskMutAnswer};
use crate::{NodeManager, SlotManager, TaskManager};

/// Go `createScheduler`.
fn create_scheduler(task: &Task, allocated_slots: bool, task_mgr: Arc<MockTaskManager>) -> BaseScheduler {
    let param = Param {
        task_mgr: task_mgr as Arc<dyn TaskManager>,
        node_mgr: Arc::new(NodeManager::new("")),
        slot_mgr: Arc::new(SlotManager::new()),
        server_id: String::new(),
        allocated_slots,
        node_res: None,
        task_runtime: None,
    };
    BaseScheduler::new(&Context::background(), task.clone(), param)
}

fn set_extension(sch: &BaseScheduler, ext: &Arc<MockExtension>) {
    let weak: Weak<dyn Extension> = Arc::downgrade(&(Arc::clone(ext) as Arc<dyn Extension>));
    sch.set_extension(weak);
}

fn err(message: &str) -> Error {
    plain(message)
}

fn cnts(entries: &[(SubtaskState, i64)]) -> HashMap<SubtaskState, i64> {
    entries.iter().cloned().collect()
}

fn same(a: &Task, b: &Task) -> bool {
    format!("{a:?}") == format!("{b:?}")
}

fn task_base(id: i64, state: tidb_dxf::TaskState, step: Step) -> TaskBase {
    TaskBase { id, state, step, ..TaskBase::default() }
}

/// Go `TestSchedulerOnNextStage`.
#[test]
fn test_scheduler_on_next_stage() {
    let task_mgr = Arc::new(MockTaskManager::default());
    let sch_ext = Arc::new(MockExtension::default());
    let task = Task { base: task_base(1, TASK_STATE_PENDING, STEP_INIT), ..Task::default() };
    let sch = create_scheduler(&task, true, Arc::clone(&task_mgr));
    set_extension(&sch, &sch_ext);

    // test next step is done
    sch_ext.expect("GetNextStep", STEP_DONE);
    sch_ext.expect::<Result<(), Error>>("OnDone", Err(err("done err")));
    assert!(sch.switch2_next_step().unwrap_err().to_string().contains("done err"));
    sch_ext.assert_satisfied();
    assert_eq!(sch.get_task().base.step, STEP_INIT);
    sch.store_task(task.clone());
    sch_ext.expect("GetNextStep", STEP_DONE);
    sch_ext.expect::<Result<(), Error>>("OnDone", Ok(()));
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("SucceedTask", Ok(()));
    sch.switch2_next_step().unwrap();
    sch_ext.assert_satisfied();
    task_mgr.assert_satisfied();

    // GetEligibleInstances err
    sch_ext.expect("GetNextStep", STEP_ONE);
    sch_ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Err(err("GetEligibleInstances err")));
    assert!(sch.switch2_next_step().unwrap_err().to_string().contains("GetEligibleInstances err"));
    sch_ext.assert_satisfied();
    // GetEligibleInstances no instance
    sch_ext.expect("GetNextStep", STEP_ONE);
    sch_ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(Vec::new()));
    assert!(sch.switch2_next_step().unwrap_err().to_string().contains("no available TiDB node to dispatch subtasks"));
    sch_ext.assert_satisfied();

    let server_nodes = vec![":4000".to_owned()];
    // OnNextSubtasksBatch err
    sch_ext.expect("GetNextStep", STEP_ONE);
    sch_ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(server_nodes.clone()));
    sch_ext.expect::<Result<Vec<Vec<u8>>, Error>>("OnNextSubtasksBatch", Err(err("OnNextSubtasksBatch err")));
    sch_ext.expect("IsRetryableErr", true);
    assert!(sch.switch2_next_step().unwrap_err().to_string().contains("OnNextSubtasksBatch err"));
    sch_ext.assert_satisfied();

    let bak = tidb_txnkv::txn_total_size_limit();

    // dispatch in batch
    let subtask_metas = vec![br#"{"xx": "1"}"#.to_vec(), br#"{"xx": "2"}"#.to_vec()];
    sch_ext.expect("GetNextStep", STEP_ONE);
    sch_ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(server_nodes.clone()));
    sch_ext.expect::<Result<Vec<Vec<u8>>, Error>>("OnNextSubtasksBatch", Ok(subtask_metas.clone()));
    task_mgr.expect_get_used_slots_on_nodes(Ok(HashMap::new()));
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("SwitchTaskStepInBatch", Ok(()));
    tidb_txnkv::set_txn_total_size_limit(1);
    sch.switch2_next_step().unwrap();
    sch_ext.assert_satisfied();
    task_mgr.assert_satisfied();
    // met unstable subtasks
    sch_ext.expect("GetNextStep", STEP_ONE);
    sch_ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(server_nodes.clone()));
    sch_ext.expect::<Result<Vec<Vec<u8>>, Error>>("OnNextSubtasksBatch", Ok(subtask_metas.clone()));
    task_mgr.expect_get_used_slots_on_nodes(Ok(HashMap::new()));
    task_mgr.expect::<tidb_dxf_storage::Result<()>>(
        "SwitchTaskStepInBatch",
        Err(StorageError::annotate(StorageError::UnstableSubtasks, "expected 2, got 100")),
    );
    tidb_txnkv::set_txn_total_size_limit(1);
    let start_time = std::time::Instant::now();
    let error = sch.switch2_next_step().unwrap_err();
    assert!(error.downcast_ref::<StorageError>().is_some_and(|e| e.is(&StorageError::UnstableSubtasks)));
    assert!(error.to_string().contains("expected 2, got 100"));
    assert!(start_time.elapsed() < std::time::Duration::from_secs(10));
    sch_ext.assert_satisfied();
    task_mgr.assert_satisfied();

    // dispatch in one txn
    sch_ext.expect("GetNextStep", STEP_ONE);
    sch_ext.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(server_nodes.clone()));
    sch_ext.expect::<Result<Vec<Vec<u8>>, Error>>("OnNextSubtasksBatch", Ok(subtask_metas));
    task_mgr.expect_get_used_slots_on_nodes(Ok(HashMap::new()));
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("SwitchTaskStep", Ok(()));
    tidb_txnkv::set_txn_total_size_limit(tidb_txnkv::DEFAULT_TXN_TOTAL_SIZE_LIMIT);
    sch.switch2_next_step().unwrap();
    sch_ext.assert_satisfied();
    task_mgr.assert_satisfied();
    tidb_txnkv::set_txn_total_size_limit(bak);
}

/// Go `TestGetEligibleNodes` (Go's `mock.MockScheduler` is the extension
/// the function asks).
#[test]
fn test_get_eligible_nodes() {
    let ctx = Context::background();
    let mock_sch = MockExtension::default();
    let task = Task { base: TaskBase { id: 1, ..TaskBase::default() }, ..Task::default() };

    mock_sch.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Err(err("mock err")));
    let error = get_eligible_nodes(&ctx, &mock_sch, &task, vec![":4000".to_owned()]).unwrap_err();
    assert!(error.to_string().contains("mock err"));
    mock_sch.assert_satisfied();

    mock_sch.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(vec![":4000".to_owned()]));
    let nodes = get_eligible_nodes(&ctx, &mock_sch, &task, vec![":4000".to_owned(), ":4001".to_owned()]).unwrap();
    assert_eq!(nodes, [":4000"]);
    mock_sch.assert_satisfied();

    mock_sch.expect::<Result<Vec<String>, Error>>("GetEligibleInstances", Ok(Vec::new()));
    let nodes = get_eligible_nodes(&ctx, &mock_sch, &task, vec![":4000".to_owned(), ":4001".to_owned()]).unwrap();
    assert_eq!(nodes, [":4000", ":4001"]);
    mock_sch.assert_satisfied();
}

/// Go `TestSchedulerIsStepSucceed`.
#[test]
fn test_scheduler_is_step_succeed() {
    assert!(is_step_succeed(&HashMap::new()));
    assert!(is_step_succeed(&cnts(&[(SUBTASK_STATE_SUCCEED, 1)])));
    for state in [SUBTASK_STATE_CANCELED, SUBTASK_STATE_FAILED] {
        assert!(!is_step_succeed(&cnts(&[(state, 1)])));
    }
}

/// Go `TestSchedulerAutoPauseOnKVDiskFull`.
#[test]
fn test_scheduler_auto_pause_on_kv_disk_full() {
    let task_mgr = Arc::new(MockTaskManager::default());
    let task_err: SubtaskError = Arc::new(tidb_ingestor_errdef::ERR_KV_DISK_FULL.gen_with_stack("store 1 disk full"));
    let task_err2: SubtaskError = Arc::new(tidb_ingestor_errdef::ERR_KV_DISK_FULL.gen_with_stack("store 2 disk full"));
    let task = Task {
        base: TaskBase {
            extra_params: ExtraParams { pause_on_kv_disk_full: true, ..ExtraParams::default() },
            ..task_base(1, TASK_STATE_RUNNING, STEP_ONE)
        },
        ..Task::default()
    };
    let sch = create_scheduler(&task, true, Arc::clone(&task_mgr));

    let sch_ext = Arc::new(MockExtension::default());
    set_extension(&sch, &sch_ext);
    let cnt_answer = |entries: &[(SubtaskState, i64)]| -> tidb_dxf_storage::Result<HashMap<SubtaskState, i64>> { Ok(cnts(entries)) };
    task_mgr.expect("GetSubtaskCntGroupByStates", cnt_answer(&[(SUBTASK_STATE_PENDING, 1), (SUBTASK_STATE_RUNNING, 1)]));
    sch_ext.expect("OnTick", ());

    sch.on_running().unwrap();
    assert_eq!(sch.get_task().base.state, TASK_STATE_RUNNING);
    task_mgr.assert_satisfied();
    sch_ext.assert_satisfied();

    task_mgr.expect("GetSubtaskCntGroupByStates", cnt_answer(&[(SUBTASK_STATE_FAILED, 2)]));
    task_mgr.expect::<tidb_dxf_storage::Result<Vec<Option<SubtaskError>>>>(
        "GetSubtaskErrors",
        Ok(vec![Some(Arc::clone(&task_err)), Some(Arc::clone(&task_err2))]),
    );
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("PauseTaskOnError", Ok(()));

    sch.on_running().unwrap();
    assert_eq!(sch.get_task().base.state, TASK_STATE_PAUSING);
    assert!(tidb_ingestor_errdef::is_kv_disk_full_error(sch.get_task().error.as_deref().unwrap()));
    assert_eq!(task_mgr.take_calls(), [format!("PauseTaskOnError(1, running, 1, {task_err})")]);
    task_mgr.assert_satisfied();

    task_mgr.expect("GetSubtaskCntGroupByStates", cnt_answer(&[(SUBTASK_STATE_FAILED, 1)]));
    task_mgr.expect::<tidb_dxf_storage::Result<Vec<Option<SubtaskError>>>>("GetSubtaskErrors", Ok(vec![Some(Arc::clone(&task_err))]));
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("PauseTaskOnError", Ok(()));

    sch.on_pausing().unwrap();
    assert_eq!(sch.get_task().base.state, TASK_STATE_PAUSING);
    assert!(tidb_ingestor_errdef::is_kv_disk_full_error(sch.get_task().error.as_deref().unwrap()));
    assert_eq!(task_mgr.take_calls(), [format!("PauseTaskOnError(1, pausing, 1, {task_err})")]);
    task_mgr.assert_satisfied();

    task_mgr.expect("GetSubtaskCntGroupByStates", cnt_answer(&[(SUBTASK_STATE_PAUSED, 1)]));
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("PausedTask", Ok(()));

    sch.on_pausing().unwrap();
    assert_eq!(sch.get_task().base.state, TASK_STATE_PAUSED);
    assert!(tidb_ingestor_errdef::is_kv_disk_full_error(sch.get_task().error.as_deref().unwrap()));
    task_mgr.assert_satisfied();

    let network_error: SubtaskError = Arc::new(StorageError::Other("network error".to_owned()));
    let tests: Vec<(&str, HashMap<SubtaskState, i64>, Vec<Option<Error>>)> = vec![
        (
            "canceled subtasks present",
            cnts(&[(SUBTASK_STATE_FAILED, 1), (SUBTASK_STATE_CANCELED, 1)]),
            vec![Some(Arc::clone(&task_err))],
        ),
        ("mixed error types", cnts(&[(SUBTASK_STATE_FAILED, 2)]), vec![Some(Arc::clone(&task_err)), Some(network_error)]),
        ("failed count and error count mismatch", cnts(&[(SUBTASK_STATE_FAILED, 2)]), vec![Some(Arc::clone(&task_err))]),
    ];
    for (name, cnt_by_state, sub_task_errs) in tests {
        assert!(!should_pause_on_kv_disk_full(&task, &cnt_by_state, &sub_task_errs), "{name}");
    }

    let missing_err_task = Task {
        base: TaskBase {
            extra_params: ExtraParams { pause_on_kv_disk_full: true, ..ExtraParams::default() },
            ..task_base(2, TASK_STATE_RUNNING, STEP_ONE)
        },
        ..Task::default()
    };
    let missing_err_sch = create_scheduler(&missing_err_task, true, Arc::clone(&task_mgr));
    task_mgr.expect("GetSubtaskCntGroupByStates", cnt_answer(&[(SUBTASK_STATE_FAILED, 1)]));
    task_mgr.expect::<tidb_dxf_storage::Result<Vec<Option<SubtaskError>>>>("GetSubtaskErrors", Ok(Vec::new()));
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("RevertTask", Ok(()));
    missing_err_sch.on_running().unwrap();
    let calls = task_mgr.take_calls();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].starts_with("RevertTask(2, running, ") && calls[0].contains("without error"), "{calls:?}");
    assert_eq!(missing_err_sch.get_task().base.state, TASK_STATE_REVERTING);
    assert!(missing_err_sch.get_task().error.as_ref().unwrap().to_string().contains("without error"));
    task_mgr.assert_satisfied();
}

/// Go `TestSchedulerNotAllocateSlots`. Go's mock changes the very task the
/// scheduler holds; here the refresh reads the changed task through
/// `GetTaskByID`, as it does for a task changed in the table.
#[test]
fn test_scheduler_not_allocate_slots() {
    let task_mgr = Arc::new(MockTaskManager::default());
    let base_answer = |task: &Task| -> tidb_dxf_storage::Result<TaskBase> { Ok(task.base.clone()) };
    let task_answer = |task: &Task| -> tidb_dxf_storage::Result<Task> { Ok(task.clone()) };

    // scheduler not allocated slots, task from paused to resuming. Should exit the scheduler.
    let mut task = Task {
        base: TaskBase { id: 1, required_slots: 1, tp: TASK_TYPE_EXAMPLE, state: TASK_STATE_PAUSED, ..TaskBase::default() },
        ..Task::default()
    };
    let sch = create_scheduler(&task, false, Arc::clone(&task_mgr));
    let mut changed = task.clone();
    changed.base.state = TASK_STATE_RESUMING;
    task_mgr.expect("GetTaskBaseByID", base_answer(&changed));
    task_mgr.expect("GetTaskByID", task_answer(&changed));
    sch.schedule_task_loop();
    task_mgr.assert_satisfied();

    // scheduler not allocated slots, task from paused to running. Should exit the scheduler.
    task.base.state = TASK_STATE_PAUSED;
    let sch = create_scheduler(&task, false, Arc::clone(&task_mgr));
    let mut changed = task.clone();
    changed.base.state = TASK_STATE_RUNNING;
    task_mgr.expect("GetTaskBaseByID", base_answer(&changed));
    task_mgr.expect("GetTaskByID", task_answer(&changed));
    sch.schedule_task_loop();
    task_mgr.assert_satisfied();

    // scheduler not allocated slots, but won't exit the scheduler.
    task.base.state = TASK_STATE_REVERTING;
    let sch = create_scheduler(&task, false, Arc::clone(&task_mgr));
    let sch_ext = Arc::new(MockExtension::default());
    set_extension(&sch, &sch_ext);
    sch_ext.expect::<Result<(), Error>>("OnDone", Ok(()));
    task_mgr.expect("GetTaskBaseByID", base_answer(&task));
    task_mgr.expect::<tidb_dxf_storage::Result<HashMap<SubtaskState, i64>>>(
        "GetSubtaskCntGroupByStates",
        Ok(cnts(&[(SUBTASK_STATE_PENDING, 0), (SUBTASK_STATE_RUNNING, 0)])),
    );
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("RevertedTask", Ok(()));
    // the scheduler stored the reverted state itself, so the refresh finds no change.
    let mut reverted = task.clone();
    reverted.base.state = TASK_STATE_REVERTED;
    task_mgr.expect("GetTaskBaseByID", base_answer(&reverted));
    sch.schedule_task_loop();
    task_mgr.assert_satisfied();
    sch_ext.assert_satisfied();

    task.base.state = TASK_STATE_PAUSING;
    let sch = create_scheduler(&task, false, Arc::clone(&task_mgr));
    let sch_ext = Arc::new(MockExtension::default());
    set_extension(&sch, &sch_ext);
    task_mgr.expect("GetTaskBaseByID", base_answer(&task));
    task_mgr.expect::<tidb_dxf_storage::Result<HashMap<SubtaskState, i64>>>(
        "GetSubtaskCntGroupByStates",
        Ok(cnts(&[(SUBTASK_STATE_PENDING, 0), (SUBTASK_STATE_RUNNING, 0)])),
    );
    let mut paused = task.clone();
    paused.base.state = TASK_STATE_PAUSED;
    task_mgr.expect("GetTaskBaseByID", base_answer(&paused));
    task_mgr.expect::<tidb_dxf_storage::Result<()>>("PausedTask", Ok(()));
    sch.schedule_task_loop();
    task_mgr.assert_satisfied();
}

/// Go `TestSchedulerRefreshTask`.
#[test]
fn test_scheduler_refresh_task() {
    let task_mgr = Arc::new(MockTaskManager::default());
    let task = Task { base: task_base(1, TASK_STATE_RUNNING, STEP_ONE), meta: b"aaa".to_vec(), ..Task::default() };
    let scheduler = create_scheduler(&task, false, Arc::clone(&task_mgr));

    // get task base error
    task_mgr.expect::<tidb_dxf_storage::Result<TaskBase>>("GetTaskBaseByID", Err(StorageError::Other("get task err".to_owned())));
    assert!(scheduler.refresh_task_if_needed().unwrap_err().to_string().contains("get task err"));
    task_mgr.assert_satisfied();
    // state/step not changed, no need to refresh
    task_mgr.expect::<tidb_dxf_storage::Result<TaskBase>>("GetTaskBaseByID", Ok(task.base.clone()));
    scheduler.refresh_task_if_needed().unwrap();
    assert!(same(&scheduler.get_task_clone(), &task));
    task_mgr.assert_satisfied();
    // get task by id failed
    let mut tmp_task = task.clone();
    tmp_task.base.state = TASK_STATE_CANCELLING;
    task_mgr.expect::<tidb_dxf_storage::Result<TaskBase>>("GetTaskBaseByID", Ok(tmp_task.base.clone()));
    task_mgr.expect::<tidb_dxf_storage::Result<Task>>("GetTaskByID", Err(StorageError::Other("get task by id err".to_owned())));
    assert!(scheduler.refresh_task_if_needed().unwrap_err().to_string().contains("get task by id err"));
    assert!(same(&scheduler.get_task_clone(), &task));
    task_mgr.assert_satisfied();
    // state changed
    task_mgr.expect::<tidb_dxf_storage::Result<TaskBase>>("GetTaskBaseByID", Ok(tmp_task.base.clone()));
    task_mgr.expect::<tidb_dxf_storage::Result<Task>>("GetTaskByID", Ok(tmp_task.clone()));
    scheduler.refresh_task_if_needed().unwrap();
    assert!(same(&scheduler.get_task_clone(), &tmp_task));
    task_mgr.assert_satisfied();
    // step changed
    scheduler.store_task(task.clone()); // revert
    let mut tmp_task = task.clone();
    tmp_task.base.step = STEP_TWO;
    task_mgr.expect::<tidb_dxf_storage::Result<TaskBase>>("GetTaskBaseByID", Ok(tmp_task.base.clone()));
    task_mgr.expect::<tidb_dxf_storage::Result<Task>>("GetTaskByID", Ok(tmp_task.clone()));
    scheduler.refresh_task_if_needed().unwrap();
    assert!(same(&scheduler.get_task_clone(), &tmp_task));
    task_mgr.assert_satisfied();
}

/// Go `TestSchedulerMaintainTaskFields`.
#[test]
fn test_scheduler_maintain_task_fields() {
    let task_mgr = Arc::new(MockTaskManager::default());
    let sch_ext = Arc::new(MockExtension::default());
    sch_ext.set_next_step(|base: &TaskBase| if base.step == STEP_INIT || base.step == STEP_PREPARED { STEP_ONE } else { STEP_DONE });
    sch_ext.set_eligible_instances(vec![":4000".to_owned()]);

    let task = Task { base: task_base(1, TASK_STATE_PENDING, STEP_INIT), meta: b"aaa".to_vec(), ..Task::default() };
    let scheduler = create_scheduler(&task, false, Arc::clone(&task_mgr));
    set_extension(&scheduler, &sch_ext);
    let ok = || -> tidb_dxf_storage::Result<()> { Ok(()) };
    let fail = |message: &str| -> tidb_dxf_storage::Result<()> { Err(StorageError::Other(message.to_owned())) };
    let no_cnts = || -> tidb_dxf_storage::Result<HashMap<SubtaskState, i64>> { Ok(HashMap::new()) };

    let mut running_task = task.clone();
    running_task.base.state = TASK_STATE_RUNNING;

    // test onPausing
    scheduler.store_task(task.clone());
    task_mgr.expect("GetSubtaskCntGroupByStates", no_cnts());
    task_mgr.expect("PausedTask", fail("pause err"));
    assert!(scheduler.on_pausing().unwrap_err().to_string().contains("pause err"));
    assert!(same(&scheduler.get_task_clone(), &task));
    task_mgr.assert_satisfied();
    // pause task successfully
    task_mgr.expect("GetSubtaskCntGroupByStates", no_cnts());
    task_mgr.expect("PausedTask", ok());
    scheduler.on_pausing().unwrap();
    let mut tmp_task = task.clone();
    tmp_task.base.state = TASK_STATE_PAUSED;
    assert!(same(&scheduler.get_task_clone(), &tmp_task));
    task_mgr.assert_satisfied();

    // test onResuming
    scheduler.store_task(task.clone());
    task_mgr.expect("GetSubtaskCntGroupByStates", no_cnts());
    task_mgr.expect("ResumedTask", fail("resume err"));
    assert!(scheduler.on_resuming().unwrap_err().to_string().contains("resume err"));
    assert!(same(&scheduler.get_task_clone(), &task));
    task_mgr.assert_satisfied();
    // resume task successfully
    task_mgr.expect("GetSubtaskCntGroupByStates", no_cnts());
    task_mgr.expect("ResumedTask", ok());
    scheduler.on_resuming().unwrap();
    let mut tmp_task = task.clone();
    tmp_task.base.state = TASK_STATE_RUNNING;
    assert!(same(&scheduler.get_task_clone(), &tmp_task));
    task_mgr.assert_satisfied();

    // test onReverting
    scheduler.store_task(task.clone());
    task_mgr.expect("GetSubtaskCntGroupByStates", no_cnts());
    sch_ext.expect::<Result<(), Error>>("OnDone", Ok(()));
    task_mgr.expect("RevertedTask", fail("reverted err"));
    assert!(scheduler.on_reverting().unwrap_err().to_string().contains("reverted err"));
    assert!(same(&scheduler.get_task_clone(), &task));
    task_mgr.assert_satisfied();
    // revert task successfully
    task_mgr.expect("GetSubtaskCntGroupByStates", no_cnts());
    sch_ext.expect::<Result<(), Error>>("OnDone", Ok(()));
    task_mgr.expect("RevertedTask", ok());
    scheduler.on_reverting().unwrap();
    let mut tmp_task = task.clone();
    tmp_task.base.state = TASK_STATE_REVERTED;
    assert!(same(&scheduler.get_task_clone(), &tmp_task));
    task_mgr.assert_satisfied();

    // test switch2NextStep
    scheduler.store_task(task.clone());
    // retryable plan error, nothing changes
    sch_ext.expect::<Result<Vec<Vec<u8>>, Error>>("OnNextSubtasksBatch", Err(err("plan err")));
    sch_ext.expect("IsRetryableErr", true);
    assert!(scheduler.switch2_next_step().unwrap_err().to_string().contains("plan err"));
    assert!(same(&scheduler.get_task_clone(), &task));
    sch_ext.assert_satisfied();
    // non-retryable plan error, but failed to revert, task state unchanged
    sch_ext.expect::<Result<Vec<Vec<u8>>, Error>>("OnNextSubtasksBatch", Err(err("plan err")));
    sch_ext.expect("IsRetryableErr", false);
    task_mgr.expect("RevertTask", fail("revert err"));
    assert!(scheduler.switch2_next_step().unwrap_err().to_string().contains("revert err"));
    assert!(same(&scheduler.get_task_clone(), &task));
    task_mgr.assert_satisfied();
    // non-retryable plan error, task state changed to reverting
    sch_ext.expect::<Result<Vec<Vec<u8>>, Error>>("OnNextSubtasksBatch", Err(err("revert err")));
    sch_ext.expect("IsRetryableErr", false);
    task_mgr.expect("RevertTask", ok());
    scheduler.switch2_next_step().unwrap();
    let mut tmp_task = task.clone();
    tmp_task.base.state = TASK_STATE_REVERTING;
    tmp_task.error = Some(err("revert err"));
    assert!(same(&scheduler.get_task_clone(), &tmp_task));
    task_mgr.assert_satisfied();
    task_mgr.take_calls();

    // revert task back
    scheduler.store_task(task.clone());

    // switch to next step, but update failed
    sch_ext.expect::<Result<Vec<Vec<u8>>, Error>>("OnNextSubtasksBatch", Ok(Vec::new()));
    task_mgr.expect_get_used_slots_on_nodes(Err(StorageError::Other("update err".to_owned())));
    assert!(scheduler.switch2_next_step().unwrap_err().to_string().contains("update err"));
    assert!(same(&scheduler.get_task_clone(), &task));
    task_mgr.assert_satisfied();
    // switch to next step successfully
    sch_ext.expect::<Result<Vec<Vec<u8>>, Error>>("OnNextSubtasksBatch", Ok(Vec::new()));
    task_mgr.expect_get_used_slots_on_nodes(Ok(HashMap::new()));
    task_mgr.expect("SwitchTaskStep", ok());
    scheduler.switch2_next_step().unwrap();
    assert_eq!(task_mgr.take_calls(), ["SwitchTaskStep(running, 1, 0)"]);
    let mut tmp_task = task.clone();
    tmp_task.base.state = TASK_STATE_RUNNING;
    tmp_task.base.step = STEP_ONE;
    assert!(same(&scheduler.get_task_clone(), &tmp_task));
    task_mgr.assert_satisfied();

    // task done, but update failed, task state unchanged
    sch_ext.expect::<Result<(), Error>>("OnDone", Ok(()));
    task_mgr.expect("SucceedTask", fail("update err"));
    assert!(scheduler.switch2_next_step().unwrap_err().to_string().contains("update err"));
    assert!(same(&scheduler.get_task_clone(), &tmp_task));
    // task done successfully, task state changed
    sch_ext.expect::<Result<(), Error>>("OnDone", Ok(()));
    task_mgr.expect("SucceedTask", ok());
    scheduler.switch2_next_step().unwrap();
    tmp_task.base.state = TASK_STATE_SUCCEED;
    tmp_task.base.step = STEP_DONE;
    assert!(same(&scheduler.get_task_clone(), &tmp_task));

    // test onPending prepare mode required
    let mut task_with_prepare = task.clone();
    task_with_prepare.base.extra_params.prepare_mode = PREPARE_MODE_REQUIRED;
    scheduler.store_task(task_with_prepare.clone());

    sch_ext.expect::<Result<(), Error>>("OnPrepare", Err(err("prepare err")));
    sch_ext.expect("IsRetryableErr", true);
    assert!(scheduler.on_pending().unwrap_err().to_string().contains("prepare err"));
    assert!(same(&scheduler.get_task(), &task_with_prepare));
    sch_ext.assert_satisfied();
    sch_ext.take_retryable_asked();

    let non_retryable_err = err("prepare fatal err");
    sch_ext.expect::<Result<(), Error>>("OnPrepare", Err(Arc::clone(&non_retryable_err)));
    sch_ext.expect("IsRetryableErr", false);
    task_mgr.expect("RevertTask", fail("revert task err"));
    assert!(scheduler.on_pending().unwrap_err().to_string().contains("revert task err"));
    assert_eq!(sch_ext.take_retryable_asked(), ["prepare fatal err"]);
    assert_eq!(task_mgr.take_calls(), ["RevertTask(1, pending, prepare fatal err)"]);
    assert!(same(&scheduler.get_task(), &task_with_prepare));
    sch_ext.assert_satisfied();

    sch_ext.expect::<Result<(), Error>>("OnPrepare", Err(Arc::clone(&non_retryable_err)));
    sch_ext.expect("IsRetryableErr", false);
    task_mgr.expect("RevertTask", ok());
    scheduler.on_pending().unwrap();
    assert_eq!(task_mgr.take_calls(), ["RevertTask(1, pending, prepare fatal err)"]);
    task_with_prepare.base.state = TASK_STATE_REVERTING;
    task_with_prepare.error = Some(Arc::clone(&non_retryable_err));
    assert!(same(&scheduler.get_task(), &task_with_prepare));
    sch_ext.assert_satisfied();

    task_with_prepare.base.state = TASK_STATE_PENDING;
    task_with_prepare.error = None;
    scheduler.store_task(task_with_prepare.clone());

    let prepare: TaskMutAnswer<Result<(), Error>> = Box::new(|in_task: &mut Task| {
        in_task.meta = br#"{"prepare":"done"}"#.to_vec();
        in_task.base.required_slots = 8;
        in_task.base.max_node_count = 6;
        Ok(())
    });
    sch_ext.expect("OnPrepare", prepare);
    let check: TaskAnswer<tidb_dxf_storage::Result<bool>> = Box::new(|in_task: &Task| {
        assert_eq!(in_task.meta, br#"{"prepare":"done"}"#);
        assert_eq!(in_task.base.required_slots, 8);
        assert_eq!(in_task.base.max_node_count, 6);
        Ok(true)
    });
    task_mgr.expect("SwitchTaskStepAfterPrepare", check);
    sch_ext.expect::<Result<Vec<Vec<u8>>, Error>>("OnNextSubtasksBatch", Ok(Vec::new()));
    task_mgr.expect_get_used_slots_on_nodes(Ok(HashMap::new()));
    task_mgr.expect("SwitchTaskStep", ok());
    scheduler.on_pending().unwrap();
    assert_eq!(task_mgr.take_calls(), ["SwitchTaskStep(running, 1, 0)"]);
    task_with_prepare.base.state = TASK_STATE_RUNNING;
    task_with_prepare.base.step = STEP_ONE;
    task_with_prepare.meta = br#"{"prepare":"done"}"#.to_vec();
    task_with_prepare.base.required_slots = 8;
    task_with_prepare.base.max_node_count = 6;
    assert!(same(&scheduler.get_task(), &task_with_prepare));
    task_mgr.assert_satisfied();
    sch_ext.assert_satisfied();

    task_with_prepare.base.step = STEP_INIT;
    task_with_prepare.meta = br#"{"prepare":"init"}"#.to_vec();
    task_with_prepare.base.required_slots = task.base.required_slots;
    task_with_prepare.base.max_node_count = task.base.max_node_count;
    scheduler.store_task(task_with_prepare.clone());
    let prepare: TaskMutAnswer<Result<(), Error>> = Box::new(|in_task: &mut Task| {
        in_task.meta = br#"{"prepare":"done"}"#.to_vec();
        in_task.base.required_slots = 8;
        in_task.base.max_node_count = 6;
        Ok(())
    });
    sch_ext.expect("OnPrepare", prepare);
    let check: TaskAnswer<tidb_dxf_storage::Result<bool>> = Box::new(|in_task: &Task| {
        assert_eq!(in_task.meta, br#"{"prepare":"done"}"#);
        assert_eq!(in_task.base.required_slots, 8);
        assert_eq!(in_task.base.max_node_count, 6);
        Ok(false)
    });
    task_mgr.expect("SwitchTaskStepAfterPrepare", check);
    scheduler.on_pending().unwrap();
    assert!(same(&scheduler.get_task(), &task_with_prepare));
    task_mgr.assert_satisfied();
    sch_ext.assert_satisfied();

    // test revertTask
    scheduler.store_task(task.clone());
    task_mgr.expect("RevertTask", fail("revert err"));
    assert!(scheduler.revert_task(err("task err")).unwrap_err().to_string().contains("revert err"));
    assert!(same(&scheduler.get_task_clone(), &task));
    task_mgr.assert_satisfied();

    task_mgr.expect("RevertTask", ok());
    scheduler.revert_task(err("task err")).unwrap();
    let mut tmp_task = task.clone();
    tmp_task.base.state = TASK_STATE_REVERTING;
    tmp_task.error = Some(err("task err"));
    assert!(same(&scheduler.get_task_clone(), &tmp_task));
    task_mgr.assert_satisfied();
    task_mgr.take_calls();

    let modifying = |modifications: Vec<Modification>| {
        let mut task_before = running_task.clone();
        task_before.base.state = TASK_STATE_MODIFYING;
        task_before.modify_param = ModifyParam { prev_state: TASK_STATE_RUNNING, modifications };
        task_before
    };
    // test on modifying, failed to update system table
    let task_before = modifying(vec![Modification { tp: MODIFY_REQUIRED_SLOTS, to: 123 }]);
    scheduler.store_task(task_before.clone());
    task_mgr.expect("ModifiedTask", fail("modify err"));
    let error = scheduler.on_modifying().unwrap_err();
    assert!(error.to_string().contains("modify err"));
    assert!(same(&scheduler.get_task(), &task_before));
    task_mgr.assert_satisfied();

    // test on modifying required slots, success
    let task_before = modifying(vec![Modification { tp: MODIFY_REQUIRED_SLOTS, to: 123 }]);
    scheduler.store_task(task_before);
    task_mgr.expect("ModifiedTask", ok());
    assert!(scheduler.on_modifying().unwrap());
    let mut expected_task = running_task.clone();
    expected_task.base.required_slots = 123;
    assert!(same(&scheduler.get_task(), &expected_task));
    task_mgr.assert_satisfied();

    // test on modifying task meta, failed to get new meta
    let task_before = modifying(vec![Modification { tp: MODIFY_MAX_WRITE_SPEED, to: 11111 }]);
    scheduler.store_task(task_before.clone());
    sch_ext.expect::<Result<Vec<u8>, Error>>("ModifyMeta", Err(err("modify meta err")));
    let error = scheduler.on_modifying().unwrap_err();
    assert!(error.to_string().contains("modify meta err"));
    assert!(same(&scheduler.get_task(), &task_before));
    sch_ext.assert_satisfied();

    // test on modifying required slots and task meta, success
    let task_before = modifying(vec![
        Modification { tp: MODIFY_REQUIRED_SLOTS, to: 123 },
        Modification { tp: MODIFY_MAX_WRITE_SPEED, to: 11111 },
    ]);
    scheduler.store_task(task_before);
    sch_ext.expect::<Result<Vec<u8>, Error>>("ModifyMeta", Ok(b"max-11111".to_vec()));
    task_mgr.expect("ModifiedTask", ok());
    assert!(scheduler.on_modifying().unwrap());
    let mut expected_task = running_task.clone();
    expected_task.base.required_slots = 123;
    expected_task.meta = b"max-11111".to_vec();
    assert!(same(&scheduler.get_task(), &expected_task));
    task_mgr.assert_satisfied();
    sch_ext.assert_satisfied();
}

/// Go `TestBaseSchedulerInitChecksTaskRuntime`.
#[test]
fn test_base_scheduler_init_checks_task_runtime() {
    use crate::mock::{new_session_pool_for_store, MockRuntime};

    let task = Task {
        base: TaskBase { id: 1, key: "task".to_owned(), tp: TASK_TYPE_EXAMPLE, keyspace: "task_ks".to_owned(), ..TaskBase::default() },
        ..Task::default()
    };
    let new_scheduler = |runtime: MockRuntime| {
        let param = Param::new_for_test(Arc::new(MockTaskManager::default()), Some(Arc::new(runtime)));
        BaseScheduler::new(&Context::background(), task.clone(), param)
    };

    let sch = new_scheduler(MockRuntime { keyspace: "task_ks".to_owned(), se_pool: Some(new_session_pool_for_store("task_ks")) });
    sch.init().unwrap();

    let sch = new_scheduler(MockRuntime { keyspace: "other_ks".to_owned(), se_pool: None });
    assert!(sch.init().unwrap_err().to_string().contains("store keyspace mismatch with task"));

    let sch = new_scheduler(MockRuntime { keyspace: "task_ks".to_owned(), se_pool: Some(new_session_pool_for_store("session_ks")) });
    assert!(sch.init().unwrap_err().to_string().contains("invalid task runtime with mismatched keyspace"));
}

/// Go `TestOnTaskFinished`. Go swaps in a fresh counter; here the global
/// counter's growth is measured.
#[test]
fn test_on_task_finished() {
    use tidb_dxf::{TASK_STATE_FAILED, TASK_STATE_RUNNING};
    use tidb_terror::{TerrorCode, TerrorError};

    use super::{get_metric_state, on_task_finished, METRIC_STATE_ALL};

    let read = || -> HashMap<String, i64> {
        prometheus::core::Collector::collect(&*tidb_dxf_dxfmetric::FINISHED_TASK_COUNTER)
            .iter()
            .flat_map(|family| family.get_metric().to_vec())
            .map(|metric| (metric.get_label()[0].value().to_owned(), metric.get_counter().get_value() as i64))
            .collect()
    };
    let base = read();
    let collect_metrics_fn = || -> HashMap<String, i64> {
        read()
            .into_iter()
            .map(|(state, value)| {
                let delta = value - base.get(&state).copied().unwrap_or(0);
                (state, delta)
            })
            .filter(|(_, delta)| *delta != 0)
            .collect()
    };
    let expect = |entries: &[(&str, i64)]| -> HashMap<String, i64> {
        entries.iter().map(|(state, n)| ((*state).to_owned(), *n)).collect()
    };
    let e = |message: &str| err(message);
    let finished = |state: &tidb_dxf::TaskState, err: Option<Error>| on_task_finished(state, err.as_deref().map(super::as_dyn));

    finished(&TASK_STATE_SUCCEED, None);
    assert_eq!(collect_metrics_fn(), expect(&[(METRIC_STATE_ALL, 1), ("succeed", 1)]));
    finished(&TASK_STATE_REVERTED, None);
    assert_eq!(collect_metrics_fn(), expect(&[(METRIC_STATE_ALL, 2), ("succeed", 1), ("failed", 1)]));
    finished(&TASK_STATE_REVERTED, Some(e("some err")));
    assert_eq!(collect_metrics_fn(), expect(&[(METRIC_STATE_ALL, 3), ("succeed", 1), ("failed", 2)]));
    finished(&TASK_STATE_REVERTED, Some(e(tidb_dxf_storage::TASK_CANCEL_MESSAGE)));
    assert_eq!(collect_metrics_fn(), expect(&[(METRIC_STATE_ALL, 4), ("succeed", 1), ("failed", 2), ("cancelled", 1)]));
    finished(&TASK_STATE_FAILED, Some(e("some err")));
    assert_eq!(collect_metrics_fn(), expect(&[(METRIC_STATE_ALL, 5), ("succeed", 1), ("failed", 3), ("cancelled", 1)]));
    let value_conversion_err = "[Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk: \
        encode kv error in file orderlab/orderlab.shipment_events.000000000.csv.gz:0 at offset 0: \
        Value conversion failed for column 'event_id'. Expected type: bigint, received value: ?. \
        Reason: [types:1292]Truncated incorrect DOUBLE value: '?'.";
    finished(&TASK_STATE_REVERTED, Some(e(value_conversion_err)));
    assert_eq!(
        collect_metrics_fn(),
        expect(&[(METRIC_STATE_ALL, 6), ("succeed", 1), ("failed", 3), ("cancelled", 1), ("data-error", 1)])
    );
    let datetime_conversion_err = "[Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk: \
        encode kv error in file orderlab/orderlab.shipment_events.000000000.csv.gz:0 at offset 0: \
        Value conversion failed for column 'created_at'. Expected type: datetime, received value: invalid. \
        Reason: [types:1292]Incorrect datetime value: 'invalid' for column 'created_at' at row 1.";
    finished(&TASK_STATE_REVERTED, Some(e(datetime_conversion_err)));
    assert_eq!(
        collect_metrics_fn(),
        expect(&[(METRIC_STATE_ALL, 7), ("succeed", 1), ("failed", 3), ("cancelled", 1), ("data-error", 2)])
    );
    // Go `common.ErrEncodeKV` and `common.ErrCastValue` (pkg/lightning/common).
    let err_encode_kv = TerrorError::normalize("encode kv error in file %s at offset %d", "Lightning:Restore:ErrEncodeKV", TerrorCode::new(0));
    let err_cast_value = TerrorError::normalize(
        "Value conversion failed for column '%s'. Expected type: %s, received value: %s. Reason: %s.",
        "Import:ErrCastValue",
        TerrorCode::new(0),
    );
    let round_trip_import_cast_err = |column: &str, column_type: &str, value: &str, reason: &str| -> Error {
        let cast_err = err_cast_value.fast_gen_by_args(&[column.into(), column_type.into(), value.into(), reason.into()]);
        let encode_err = err_encode_kv.wrap(Arc::new(cast_err)).fast_gen_by_args(&["data.csv".into(), 0.into()]);
        let serialized_err = TerrorError::normalize(
            tidb_terror::get_err_stack_msg(Some(&encode_err)),
            err_encode_kv.id(),
            err_encode_kv.code(),
        );
        let err_bytes = serde_json::to_vec(&serialized_err).unwrap();
        Arc::new(serde_json::from_slice::<TerrorError>(&err_bytes).unwrap())
    };
    let data_too_long_err = round_trip_import_cast_err("name", "varchar(3)", "abcd", "[types:1406]Data Too Long, field len 3, data len 4");
    assert!(!data_too_long_err.to_string().contains("ErrCastValue"));
    assert_eq!(get_metric_state(&TASK_STATE_REVERTED, Some(super::as_dyn(data_too_long_err.as_ref()))), "data-error");
    let not_null_err = round_trip_import_cast_err("name", "varchar(10)", "NULL", "[table:1048]Column 'name' cannot be null");
    assert_eq!(get_metric_state(&TASK_STATE_REVERTED, Some(super::as_dyn(not_null_err.as_ref()))), "data-error");
    let metric_state = |state: &tidb_dxf::TaskState, message: &str| get_metric_state(state, Some(super::as_dyn(e(message).as_ref())));
    let check_constraint_err = "[Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk: \
        encode kv error in file data.csv:0 at offset 0: \
        Check constraint 'positive_id' is violated.";
    assert_eq!(metric_state(&TASK_STATE_REVERTED, check_constraint_err), "data-error");
    let no_partition_err = "[Lightning:Restore:ErrEncodeKV]when encoding 1-th data row in this chunk: \
        encode kv error in file data.csv:0 at offset 0: \
        Table has no partition for value 42";
    assert_eq!(metric_state(&TASK_STATE_REVERTED, no_partition_err), "data-error");
    assert_eq!(
        metric_state(&TASK_STATE_REVERTED, "[executor:8167]Duplicate key conflict found. Please resolve conflicts in the input dataset"),
        "data-error"
    );
    assert_eq!(
        metric_state(&TASK_STATE_REVERTED, "[Lightning:Restore:ErrFoundDataConflictRecords]found data conflict records in table t"),
        "data-error"
    );
    assert_eq!(
        metric_state(&TASK_STATE_REVERTED, "[Lightning:Restore:ErrFoundIndexConflictRecords]found index conflict records in table t"),
        "data-error"
    );
    let parse_err = "[Lightning:Restore:ErrEncodeKV]encode kv error in file data.csv:0 at offset 0: \
        column count mismatch, expected 3, got 2";
    assert_eq!(metric_state(&TASK_STATE_REVERTED, parse_err), TASK_STATE_FAILED.to_string());
    assert_eq!(metric_state(&TASK_STATE_REVERTED, "Value conversion failed for column 'name'"), TASK_STATE_FAILED.to_string());
    finished(&TASK_STATE_REVERTED, Some(e("[kv:1062]Duplicate entry '1' for key 't.idx'")));
    assert_eq!(
        collect_metrics_fn(),
        expect(&[(METRIC_STATE_ALL, 8), ("succeed", 1), ("failed", 3), ("cancelled", 1), ("data-error", 3)])
    );
    // noop for non-finished state.
    finished(&TASK_STATE_RUNNING, None);
    assert_eq!(
        collect_metrics_fn(),
        expect(&[(METRIC_STATE_ALL, 8), ("succeed", 1), ("failed", 3), ("cancelled", 1), ("data-error", 3)])
    );
}
