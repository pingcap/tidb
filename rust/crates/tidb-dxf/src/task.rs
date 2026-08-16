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

//! Go `task.go`: the task record, its state vocabulary, the scheduling rank
//! comparison, and the owner-local concurrency knob.

use std::cmp::Ordering;
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::modify::ModifyParam;
use crate::step::{step2str, Step};

go_string_type! {
    /// Go `TaskState`: the state of a task.
    TaskState
}

go_string_type! {
    /// Go `TaskType`: the type of a task.
    TaskType
}

/// Go `TaskStatePending`.
pub const TASK_STATE_PENDING: TaskState = TaskState::from_static("pending");
/// Go `TaskStateRunning`.
pub const TASK_STATE_RUNNING: TaskState = TaskState::from_static("running");
/// Go `TaskStateSucceed`.
pub const TASK_STATE_SUCCEED: TaskState = TaskState::from_static("succeed");
/// Go `TaskStateFailed`.
pub const TASK_STATE_FAILED: TaskState = TaskState::from_static("failed");
/// Go `TaskStateReverting`.
pub const TASK_STATE_REVERTING: TaskState = TaskState::from_static("reverting");
/// Go `TaskStateAwaitingResolution`.
pub const TASK_STATE_AWAITING_RESOLUTION: TaskState = TaskState::from_static("awaiting-resolution");
/// Go `TaskStateReverted`.
pub const TASK_STATE_REVERTED: TaskState = TaskState::from_static("reverted");
/// Go `TaskStateCancelling`.
pub const TASK_STATE_CANCELLING: TaskState = TaskState::from_static("cancelling");
/// Go `TaskStatePausing`.
pub const TASK_STATE_PAUSING: TaskState = TaskState::from_static("pausing");
/// Go `TaskStatePaused`.
pub const TASK_STATE_PAUSED: TaskState = TaskState::from_static("paused");
/// Go `TaskStateResuming`.
pub const TASK_STATE_RESUMING: TaskState = TaskState::from_static("resuming");
/// Go `TaskStateModifying`.
pub const TASK_STATE_MODIFYING: TaskState = TaskState::from_static("modifying");

impl TaskState {
    /// Go `CanMoveToModifying`: whether this state can move to `modifying`.
    #[must_use]
    pub fn can_move_to_modifying(&self) -> bool {
        *self == TASK_STATE_PENDING || *self == TASK_STATE_RUNNING || *self == TASK_STATE_PAUSED
    }
}

/// Go `PrepareMode`: whether a task needs prepare-mode scheduling.
///
/// Go declares this as a bare `int` and its `String` method has an
/// `unknown(%d)` arm, so out-of-range values must survive a round trip. A
/// newtype over `i64` keeps that, and `#[serde(transparent)]` keeps the JSON
/// encoding a bare number as Go's does.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct PrepareMode(pub i64);

/// Go `PrepareModeDisabled`: prepare mode is disabled, the default.
pub const PREPARE_MODE_DISABLED: PrepareMode = PrepareMode(0);
/// Go `PrepareModeRequired`: task scheduling must enter prepare mode.
pub const PREPARE_MODE_REQUIRED: PrepareMode = PrepareMode(1);

impl PrepareMode {
    /// Whether this is the default mode, i.e. the value `omitempty` drops.
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        *self == PREPARE_MODE_DISABLED
    }
}

impl fmt::Display for PrepareMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            PREPARE_MODE_DISABLED => f.write_str("disabled"),
            PREPARE_MODE_REQUIRED => f.write_str("required"),
            PrepareMode(v) => write!(f, "unknown({v})"),
        }
    }
}

/// Go `TaskIDLabelName`: the label name of task id.
pub const TASK_ID_LABEL_NAME: &str = "task_id";
/// Go `NormalPriority`: the normal priority of a task.
pub const NORMAL_PRIORITY: i64 = 512;

/// Go `maxConcurrentTaskLowerBound`: the minimum allowed DXF task concurrency.
pub(crate) const MAX_CONCURRENT_TASK_LOWER_BOUND: i64 = 16;
/// Go `MaxConcurrentTaskUpperBound`: the current safety cap for DXF task
/// concurrency.
pub const MAX_CONCURRENT_TASK_UPPER_BOUND: i64 = 1000;
/// Go `DefaultMaxConcurrentTask`: the default DXF task concurrency.
pub const DEFAULT_MAX_CONCURRENT_TASK: i64 = MAX_CONCURRENT_TASK_LOWER_BOUND;

/// Go `maxConcurrentTask`: an owner-local emergency tuning knob for DXF
/// scheduling.
///
/// It is intentionally kept in memory only: it is not persisted to TiKV, is
/// reset on restart, and only affects the TiDB node that receives the update.
/// Go seeds it from `init()`; here the static's own initializer does that, so
/// there is no separate initialization phase to observe.
static MAX_CONCURRENT_TASK: AtomicI64 = AtomicI64::new(DEFAULT_MAX_CONCURRENT_TASK);

/// Go `GetMaxConcurrentTask`.
#[must_use]
pub fn get_max_concurrent_task() -> i64 {
    MAX_CONCURRENT_TASK.load(AtomicOrdering::SeqCst)
}

/// Go's `fmt.Errorf` value from `SetMaxConcurrentTask`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaxConcurrentTaskError {
    /// The rejected value.
    pub value: i64,
}

impl fmt::Display for MaxConcurrentTaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "max_concurrent_task {} is out of range [{}, {}]",
            self.value, MAX_CONCURRENT_TASK_LOWER_BOUND, MAX_CONCURRENT_TASK_UPPER_BOUND
        )
    }
}

impl std::error::Error for MaxConcurrentTaskError {}

/// Go `SetMaxConcurrentTask`: updates the max concurrency of task.
///
/// # Errors
///
/// Returns [`MaxConcurrentTaskError`] when `value` falls outside
/// `[MAX_CONCURRENT_TASK_LOWER_BOUND, MAX_CONCURRENT_TASK_UPPER_BOUND]`; the
/// stored value is then left untouched.
pub fn set_max_concurrent_task(value: i64) -> Result<(), MaxConcurrentTaskError> {
    if !(MAX_CONCURRENT_TASK_LOWER_BOUND..=MAX_CONCURRENT_TASK_UPPER_BOUND).contains(&value) {
        return Err(MaxConcurrentTaskError { value });
    }
    MAX_CONCURRENT_TASK.store(value, AtomicOrdering::SeqCst);
    Ok(())
}

/// Restores the previous max concurrent task on drop.
///
/// Go's `SetMaxConcurrentTaskForTest` returns a `func()` that callers invoke
/// through `defer`; an RAII guard is how Rust spells the same lifetime.
#[derive(Debug)]
#[must_use = "the previous value is restored when this guard is dropped"]
pub struct MaxConcurrentTaskGuard {
    old: i64,
}

impl Drop for MaxConcurrentTaskGuard {
    fn drop(&mut self) {
        MAX_CONCURRENT_TASK.store(self.old, AtomicOrdering::SeqCst);
    }
}

/// Go `SetMaxConcurrentTaskForTest`: sets the knob, bypassing the range check,
/// and hands back the restore.
pub fn set_max_concurrent_task_for_test(value: i64) -> MaxConcurrentTaskGuard {
    let old = get_max_concurrent_task();
    MAX_CONCURRENT_TASK.store(value, AtomicOrdering::SeqCst);
    MaxConcurrentTaskGuard { old }
}

/// Go `ExtraParams`: the extra params of a task.
///
/// Note: only params that are not used for filter or sort live here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtraParams {
    /// Whether the task can be recovered manually. If enabled, the task enters
    /// `awaiting-resolution` when it fails, and the user can then recover it
    /// manually or fail it if it is not recoverable.
    #[serde(default, skip_serializing_if = "is_false")]
    pub manual_recovery: bool,
    /// Whether the task should be paused instead of reverted when TiKV reports
    /// disk full.
    #[serde(default, skip_serializing_if = "is_false")]
    pub pause_on_kv_disk_full: bool,
    /// The max slots when running subtasks of this task in `target_steps`.
    ///
    /// Normally 0, meaning `required_slots` is used. If set, the effective
    /// slots are the min of `required_slots` and this. It works around an OOM
    /// issue where TiDB might repeatedly restart; the framework does not detect
    /// changes to it, so the newest value applies on restart. `required_slots`
    /// can be modified while this is left alone, so it may exceed it.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub max_runtime_slots: i64,
    /// The steps in which `max_runtime_slots` takes effect. Empty means all
    /// steps. OOM normally happens in a few specific steps, so limiting only
    /// those reduces the impact on overall performance.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_steps: Vec<Step>,
    /// Whether this task requires prepare-mode scheduling. The default is
    /// [`PREPARE_MODE_DISABLED`], for backward compatibility.
    #[serde(default, skip_serializing_if = "PrepareMode::is_disabled")]
    pub prepare_mode: PrepareMode,
}

fn is_false(v: &bool) -> bool {
    !*v
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

/// Go `TaskBase`: the basic information of a task, split out so that the
/// possibly very large task meta need not be loaded into memory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskBase {
    /// Task ID.
    pub id: i64,
    /// Task key, unique across tasks.
    pub key: String,
    /// Task type.
    pub tp: TaskType,
    /// Task state.
    pub state: TaskState,
    /// Current step.
    pub step: Step,
    /// The priority of the task; a smaller value means higher priority. The
    /// valid range is `[1, 1024]` and the default is [`NORMAL_PRIORITY`].
    pub priority: i64,
    /// The required slots of the task.
    ///
    /// Slots are allocated from this when scheduling and when creating the task
    /// executor, but the effective slots while running are decided by
    /// [`TaskBase::get_runtime_slots`]. Normally they are the same; on OOM with
    /// repeated restarts, `ExtraParams::max_runtime_slots` may lower them.
    /// Application code should read [`TaskBase::get_runtime_slots`] rather than
    /// this field. In the system table this lives in the `concurrency` column,
    /// because required slots were introduced later.
    pub required_slots: i64,
    /// The task should run on TiDB nodes carrying the
    /// `tidb_service_scope=TargetScope` label. For compatibility with previous
    /// versions, `""` and `"background"` both first try the `background` scope
    /// and fall back to the `""` scope.
    pub target_scope: String,
    /// Creation time; `None` is Go's zero `time.Time`.
    pub create_time: Option<DateTime<Utc>>,
    /// The max node count of the task.
    pub max_node_count: i64,
    /// The extra params of the task.
    pub extra_params: ExtraParams,
    /// The keyspace the task belongs to; only meaningful for nextgen clusters.
    pub keyspace: String,
}

impl TaskBase {
    /// Go `IsDone`: whether the task reached a terminal state.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.state == TASK_STATE_SUCCEED
            || self.state == TASK_STATE_REVERTED
            || self.state == TASK_STATE_FAILED
    }

    /// Go `CompareTask`: a wrapper of [`TaskBase::compare`].
    #[must_use]
    pub fn compare_task(&self, other: &Task) -> Ordering {
        self.compare(&other.base)
    }

    /// Go `Compare`: compares two tasks by task rank.
    ///
    /// [`Ordering::Less`] means the rank of `self` is higher than `other`.
    #[must_use]
    pub fn compare(&self, other: &TaskBase) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.create_time.cmp(&other.create_time))
            .then_with(|| self.id.cmp(&other.id))
    }

    /// Go `GetRuntimeSlots`: the runtime slots of the current task step, which
    /// the application layer may use as the concurrency of that step.
    #[must_use]
    pub fn get_runtime_slots(&self) -> i64 {
        if self.extra_params.max_runtime_slots > 0 {
            if self.extra_params.target_steps.is_empty() {
                return self.extra_params.max_runtime_slots.min(self.required_slots);
            }
            if self.extra_params.target_steps.contains(&self.step) {
                return self.extra_params.max_runtime_slots.min(self.required_slots);
            }
        }
        self.required_slots
    }
}

impl fmt::Display for TaskBase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{id: {}, key: {}, type: {}, state: {}, step: {}, priority: {}, required slots: {}, target scope: {}, create time: {}}}",
            self.id,
            self.key,
            self.tp,
            self.state,
            step2str(&self.tp, self.step),
            self.priority,
            self.required_slots,
            self.target_scope,
            format_rfc3339_nano(self.create_time),
        )
    }
}

/// Formats a timestamp the way Go's `time.RFC3339Nano` layout does: trailing
/// zeros are trimmed from the fraction, and the dot disappears entirely when
/// the fraction is zero. `None` renders Go's zero `time.Time`.
fn format_rfc3339_nano(t: Option<DateTime<Utc>>) -> String {
    let Some(t) = t else {
        return "0001-01-01T00:00:00Z".to_owned();
    };
    let nanos = t.timestamp_subsec_nanos();
    let head = t.format("%Y-%m-%dT%H:%M:%S").to_string();
    if nanos == 0 {
        return format!("{head}Z");
    }
    let frac = format!("{nanos:09}");
    let frac = frac.trim_end_matches('0');
    format!("{head}.{frac}Z")
}

/// Go `Task`: the task of the distributed framework.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Task {
    /// The embedded [`TaskBase`]; Go embeds it anonymously.
    pub base: TaskBase,
    /// Go `SchedulerID`, which is not used now.
    pub scheduler_id: String,
    /// Start time; `None` is Go's zero `time.Time`.
    pub start_time: Option<DateTime<Utc>>,
    /// Last state update time; `None` is Go's zero `time.Time`.
    pub state_update_time: Option<DateTime<Utc>>,
    /// The metadata of the task. It is read-only in most cases, except when
    /// the task switches to the next step in `Scheduler.OnNextSubtasksBatch`,
    /// when cleanup redacts it, and when 'modifying' changes params inside it.
    pub meta: Vec<u8>,
    /// The task's failure, carried verbatim between the task table and the
    /// scheduler. Go stores an `error`; this package never inspects, wraps, or
    /// matches on it, and the storage column is a plain `BLOB`.
    pub error: Option<String>,
    /// The pending modification of the task.
    pub modify_param: ModifyParam,
}

impl Task {
    /// Go `(*TaskBase).IsDone` reached through the embedded base.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.base.is_done()
    }

    /// Go `(*TaskBase).CompareTask` reached through the embedded base.
    #[must_use]
    pub fn compare_task(&self, other: &Task) -> Ordering {
        self.base.compare_task(other)
    }
}

/// Go `EmptyMeta`: the empty meta of a task or subtask.
pub const EMPTY_META: &[u8] = b"{}";

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::step::{STEP_DONE, STEP_INIT, STEP_ONE, STEP_TWO};

    /// Go `TestTaskStep`.
    #[test]
    fn test_task_step() {
        // make sure we don't change the value of StepInit accidentally
        assert_eq!(STEP_INIT.0, -1);
        assert_eq!(STEP_DONE.0, -2);
        // make sure we don't change prepare mode constants accidentally.
        assert_eq!(PREPARE_MODE_DISABLED.0, 0);
        assert_eq!(PREPARE_MODE_REQUIRED.0, 1);
        assert_eq!(PREPARE_MODE_DISABLED.to_string(), "disabled");
        assert_eq!(PREPARE_MODE_REQUIRED.to_string(), "required");
        assert_eq!(PrepareMode(123).to_string(), "unknown(123)");

        // default prepare mode should be omitted for backward-compatible json payload.
        let data = serde_json::to_string(&ExtraParams::default()).unwrap();
        assert_eq!(data, "{}");

        // existing fields should keep old payload shape when prepare mode is default.
        let data = serde_json::to_string(&ExtraParams {
            manual_recovery: true,
            ..ExtraParams::default()
        })
        .unwrap();
        assert_eq!(data, r#"{"manual_recovery":true}"#);

        let data = serde_json::to_string(&ExtraParams {
            prepare_mode: PREPARE_MODE_REQUIRED,
            ..ExtraParams::default()
        })
        .unwrap();
        assert_eq!(data, r#"{"prepare_mode":1}"#);

        let extra_params: ExtraParams = serde_json::from_str("{}").unwrap();
        assert_eq!(extra_params.prepare_mode, PREPARE_MODE_DISABLED);
        let extra_params: ExtraParams = serde_json::from_str(r#"{"prepare_mode":1}"#).unwrap();
        assert_eq!(extra_params.prepare_mode, PREPARE_MODE_REQUIRED);
    }

    /// Go `TestTaskIsDone`.
    #[test]
    fn test_task_is_done() {
        let cases = [
            (TASK_STATE_PENDING, false),
            (TASK_STATE_RUNNING, false),
            (TASK_STATE_SUCCEED, true),
            (TASK_STATE_REVERTING, false),
            (TASK_STATE_FAILED, true),
            (TASK_STATE_CANCELLING, false),
            (TASK_STATE_PAUSING, false),
            (TASK_STATE_PAUSED, false),
            (TASK_STATE_REVERTED, true),
        ];
        for (state, done) in cases {
            let task = Task {
                base: TaskBase {
                    state,
                    ..TaskBase::default()
                },
                ..Task::default()
            };
            assert_eq!(task.is_done(), done);
        }
    }

    /// Go `TestMaxConcurrentTask`.
    #[test]
    fn test_max_concurrent_task() {
        let _restore = set_max_concurrent_task_for_test(DEFAULT_MAX_CONCURRENT_TASK);

        assert_eq!(get_max_concurrent_task(), DEFAULT_MAX_CONCURRENT_TASK);
        assert_eq!(MAX_CONCURRENT_TASK_UPPER_BOUND, 1000);
        for value in [
            MAX_CONCURRENT_TASK_LOWER_BOUND - 1,
            MAX_CONCURRENT_TASK_UPPER_BOUND + 1,
        ] {
            assert!(set_max_concurrent_task(value).is_err());
            assert_eq!(get_max_concurrent_task(), DEFAULT_MAX_CONCURRENT_TASK);
        }

        assert!(set_max_concurrent_task(128).is_ok());
        assert_eq!(get_max_concurrent_task(), 128);
        assert!(set_max_concurrent_task(MAX_CONCURRENT_TASK_UPPER_BOUND).is_ok());
        assert_eq!(get_max_concurrent_task(), MAX_CONCURRENT_TASK_UPPER_BOUND);
    }

    /// Go `TestTaskCompare`.
    #[test]
    fn test_task_compare() {
        let task_a = Task {
            base: TaskBase {
                id: 100,
                priority: NORMAL_PRIORITY,
                create_time: Some(Utc.with_ymd_and_hms(2023, 12, 5, 15, 53, 30).unwrap()),
                ..TaskBase::default()
            },
            ..Task::default()
        };
        let mut task_b = task_a.clone();
        assert_eq!(task_a.compare_task(&task_b), Ordering::Equal);
        task_b.base.priority = 100;
        assert_eq!(task_a.compare_task(&task_b), Ordering::Greater);
        task_b.base.priority = task_a.base.priority + 100;
        assert_eq!(task_a.compare_task(&task_b), Ordering::Less);

        task_b.base.priority = task_a.base.priority;
        task_b.base.create_time = Some(Utc.with_ymd_and_hms(2023, 12, 5, 15, 53, 10).unwrap());
        assert_eq!(task_a.compare_task(&task_b), Ordering::Greater);
        task_b.base.create_time = Some(Utc.with_ymd_and_hms(2023, 12, 5, 15, 53, 40).unwrap());
        assert_eq!(task_a.compare_task(&task_b), Ordering::Less);

        task_b.base.create_time = task_a.base.create_time;
        task_b.base.id = task_a.base.id - 10;
        assert_eq!(task_a.compare_task(&task_b), Ordering::Greater);
        task_b.base.id = task_a.base.id + 10;
        assert_eq!(task_a.compare_task(&task_b), Ordering::Less);
    }

    /// Go `TestTaskBaseGetRuntimeSlots`.
    #[test]
    fn test_task_base_get_runtime_slots() {
        let mut task = TaskBase {
            required_slots: 4,
            step: STEP_ONE,
            ..TaskBase::default()
        };
        assert_eq!(task.get_runtime_slots(), 4);

        task.extra_params.max_runtime_slots = 2;
        for step in [STEP_ONE, STEP_TWO] {
            task.step = step;
            assert_eq!(task.get_runtime_slots(), 2);
        }
        task.extra_params.target_steps = vec![STEP_ONE];
        task.step = STEP_ONE;
        assert_eq!(task.get_runtime_slots(), 2);
        task.step = STEP_TWO;
        assert_eq!(task.get_runtime_slots(), 4);

        let resource = crate::node::NodeResource::new(16, 1600, 100);
        let limited = resource.limit_dxf_resource(30);
        assert_eq!(limited.total_cpu, 5);
        assert_eq!(limited.total_mem, 500);
        assert_eq!(limited.total_disk, resource.total_disk);

        let full = resource.limit_dxf_resource(100);
        assert_eq!(full.total_cpu, 16);
        assert_eq!(full.total_mem, 1600);
        assert_eq!(full.total_disk, resource.total_disk);

        let small = crate::node::NodeResource::new(2, 200, 100).limit_dxf_resource(10);
        assert_eq!(small.total_cpu, 1);
        assert_eq!(small.total_mem, 100);
    }

    /// Go's `TaskBase.String` uses `time.RFC3339Nano`; this pins the trailing
    /// zero trimming that layout performs and the zero-time rendering that
    /// `Option::None` stands for.
    #[test]
    fn test_task_base_string_time_layout() {
        assert_eq!(format_rfc3339_nano(None), "0001-01-01T00:00:00Z");
        let t = Utc.with_ymd_and_hms(2023, 12, 5, 15, 53, 30).unwrap();
        assert_eq!(format_rfc3339_nano(Some(t)), "2023-12-05T15:53:30Z");
        let t = t + chrono::Duration::nanoseconds(123_400_000);
        assert_eq!(format_rfc3339_nano(Some(t)), "2023-12-05T15:53:30.1234Z");
    }
}
