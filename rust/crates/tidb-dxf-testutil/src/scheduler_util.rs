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

//! Go `scheduler_util.go`: mock scheduler extensions of given steps.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tidb_dxf::{Modification, Step, Task, TaskBase, STEP_DONE, STEP_INIT, STEP_ONE, STEP_TWO};
use tidb_dxf_mock::MockSchedulerExtension;
use tidb_gomock::{Controller, Times, ONCE};

use crate::TestContext;

type SchedulerError = tidb_dxf_scheduler::Error;

fn err(message: &str) -> SchedulerError {
    Arc::new(std::io::Error::other(message.to_owned()))
}

/// Go `SchedulerInfo`.
#[derive(Clone)]
pub struct SchedulerInfo {
    /// Go `AllErrorRetryable`.
    pub all_error_retryable: bool,
    /// Go `StepInfos`.
    pub step_infos: Vec<StepInfo>,
}

/// Go `StepInfo`: for the first `err_repeat_count` calls to
/// `OnNextSubtasksBatch` of the step it returns `err`, then `subtask_cnt`
/// subtasks.
#[derive(Clone)]
pub struct StepInfo {
    /// Go `Step`.
    pub step: Step,
    /// Go `Err`.
    pub err: Option<SchedulerError>,
    /// Go `ErrRepeatCount`.
    pub err_repeat_count: i64,
    /// Go `SubtaskCnt`.
    pub subtask_cnt: usize,
}

impl StepInfo {
    /// A step of `subtask_cnt` subtasks and no error.
    #[must_use]
    pub fn new(step: Step, subtask_cnt: usize) -> Self {
        Self { step, err: None, err_repeat_count: 0, subtask_cnt }
    }
}

/// Go `GetMockBasicSchedulerExt`.
#[must_use]
pub fn get_mock_basic_scheduler_ext(ctrl: &Arc<Controller>) -> Arc<MockSchedulerExtension> {
    get_mock_scheduler_ext(
        ctrl,
        SchedulerInfo { all_error_retryable: true, step_infos: vec![StepInfo::new(STEP_ONE, 3), StepInfo::new(STEP_TWO, 1)] },
    )
}

/// Go `GetMockSchedulerExt`.
///
/// # Panics
/// `scheduler_info` has no step.
#[must_use]
pub fn get_mock_scheduler_ext(ctrl: &Arc<Controller>, scheduler_info: SchedulerInfo) -> Arc<MockSchedulerExtension> {
    let step_infos = scheduler_info.step_infos;
    assert!(!step_infos.is_empty(), "stepInfos should not be empty");
    let mut step_transition = HashMap::with_capacity(step_infos.len() + 1);
    let mut curr_step = STEP_INIT;
    let mut step_info_map = HashMap::with_capacity(step_infos.len());
    for step_info in step_infos {
        step_transition.insert(curr_step, step_info.step);
        curr_step = step_info.step;
        step_info_map.insert(step_info.step, step_info);
    }
    step_transition.insert(curr_step, STEP_DONE);

    let mock_scheduler = MockSchedulerExtension::new(ctrl);
    mock_scheduler.ret_times("OnTick", Times::Any, ());
    mock_scheduler.ret_times("GetEligibleInstances", Times::Any, Ok::<Vec<String>, SchedulerError>(Vec::new()));
    mock_scheduler.ret_times("IsRetryableErr", Times::Any, scheduler_info.all_error_retryable);
    mock_scheduler.expect("GetNextStep", Times::Any, move |input| {
        step_transition[&input.downcast_ref::<TaskBase>().expect("GetNextStep gets the task").step]
    });
    mock_scheduler.expect("OnNextSubtasksBatch", Times::Any, move |input| {
        let (_, _, next_step) =
            input.downcast_ref::<(Mutex<Task>, Vec<String>, Step)>().expect("OnNextSubtasksBatch gets (task, exec IDs, step)");
        // Go reads a copy of the map's value, so the call count it bumps never
        // reaches the map: a step with an error fails every call.
        let Some(step_info) = step_info_map.get(next_step).cloned() else {
            return Ok::<Vec<Vec<u8>>, SchedulerError>(Vec::new());
        };
        let call_count = 0;
        if call_count < step_info.err_repeat_count {
            return Err(step_info.err.clone().expect("a step that fails has an error"));
        }
        Ok((0..step_info.subtask_cnt).map(|i| format!("subtask-{i}").into_bytes()).collect())
    });
    mock_scheduler.expect("ModifyMeta", Times::Any, |input| {
        let (_, modifies) = input.downcast_ref::<(Vec<u8>, Vec<Modification>)>().expect("ModifyMeta gets (meta, modifies)");
        let parts: Vec<String> = modifies.iter().map(|m| format!("{}={}", m.tp, m.to)).collect();
        Ok::<Vec<u8>, SchedulerError>(parts.join(",").into_bytes())
    });

    mock_scheduler.ret_times("OnDone", Times::Any, Ok::<(), SchedulerError>(()));
    mock_scheduler
}

/// Go `GetMockHATestSchedulerExt`.
#[must_use]
pub fn get_mock_ha_test_scheduler_ext(ctrl: &Arc<Controller>) -> Arc<MockSchedulerExtension> {
    get_mock_scheduler_ext(
        ctrl,
        SchedulerInfo { all_error_retryable: true, step_infos: vec![StepInfo::new(STEP_ONE, 10), StepInfo::new(STEP_TWO, 5)] },
    )
}

/// Go `GetPlanNotRetryableErrSchedulerExt`.
#[must_use]
pub fn get_plan_not_retryable_err_scheduler_ext(ctrl: &Arc<Controller>) -> Arc<MockSchedulerExtension> {
    get_mock_scheduler_ext(
        ctrl,
        SchedulerInfo {
            all_error_retryable: false,
            step_infos: vec![StepInfo {
                step: STEP_ONE,
                err: Some(err("not retryable err")),
                err_repeat_count: i64::MAX,
                subtask_cnt: 0,
            }],
        },
    )
}

/// Go `GetStepTwoPlanNotRetryableErrSchedulerExt`.
#[must_use]
pub fn get_step_two_plan_not_retryable_err_scheduler_ext(ctrl: &Arc<Controller>) -> Arc<MockSchedulerExtension> {
    get_mock_scheduler_ext(
        ctrl,
        SchedulerInfo {
            all_error_retryable: false,
            step_infos: vec![
                StepInfo::new(STEP_ONE, 10),
                StepInfo { step: STEP_TWO, err: Some(err("not retryable err")), err_repeat_count: i64::MAX, subtask_cnt: 0 },
            ],
        },
    )
}

/// Go `GetPlanErrSchedulerExt`.
#[must_use]
pub fn get_plan_err_scheduler_ext(ctrl: &Arc<Controller>, test_context: &Arc<TestContext>) -> Arc<MockSchedulerExtension> {
    let mock_scheduler = MockSchedulerExtension::new(ctrl);
    mock_scheduler.ret_times("OnTick", Times::Any, ());
    mock_scheduler.ret_times("GetEligibleInstances", Times::Any, Ok::<Vec<String>, SchedulerError>(Vec::new()));
    mock_scheduler.ret_times("IsRetryableErr", Times::Any, true);
    mock_scheduler.expect("GetNextStep", Times::Any, |input| {
        let task = input.downcast_ref::<TaskBase>().expect("GetNextStep gets the task");
        if task.step == STEP_INIT {
            STEP_ONE
        } else if task.step == STEP_ONE {
            STEP_TWO
        } else {
            STEP_DONE
        }
    });
    let test_context = Arc::clone(test_context);
    mock_scheduler.expect("OnNextSubtasksBatch", Times::Any, move |input| {
        let (task, _, _) =
            input.downcast_ref::<(Mutex<Task>, Vec<String>, Step)>().expect("OnNextSubtasksBatch gets (task, exec IDs, step)");
        let step = task.lock().unwrap().base.step;
        if step == STEP_INIT {
            let mut call_time = test_context.call_time.lock().unwrap();
            if *call_time == 0 {
                *call_time += 1;
                return Err(err("retryable err"));
            }
            return Ok(vec![b"task1".to_vec(), b"task2".to_vec(), b"task3".to_vec()]);
        }
        if step == STEP_ONE {
            return Ok(vec![b"task4".to_vec()]);
        }
        Ok::<Vec<Vec<u8>>, SchedulerError>(Vec::new())
    });

    // gomock.InOrder
    mock_scheduler.expect("OnDone", ONCE, |_| Err::<(), SchedulerError>(err("not retryable err")));
    mock_scheduler.ret_times("OnDone", Times::Any, Ok::<(), SchedulerError>(()));
    mock_scheduler
}

/// Go `GetMockRollbackSchedulerExt`.
#[must_use]
pub fn get_mock_rollback_scheduler_ext(ctrl: &Arc<Controller>) -> Arc<MockSchedulerExtension> {
    get_mock_scheduler_ext(ctrl, SchedulerInfo { all_error_retryable: true, step_infos: vec![StepInfo::new(STEP_ONE, 3)] })
}
