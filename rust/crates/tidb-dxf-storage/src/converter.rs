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


//! Go `converter.go`: the framework's table rows as `proto` values.

use chrono::{DateTime, FixedOffset, TimeZone};
use tidb_datatype::Datum;
use tidb_dxf::{int_2_type, ExtraParams, ModifyParam, Step, Subtask, SubtaskBase, SubtaskState, Task, TaskBase, TaskState, TaskType};

/// Go's zero `time.Time`, as this port's `proto` values hold it.
fn zero_time() -> DateTime<FixedOffset> {
    DateTime::<FixedOffset>::default()
}

pub(crate) fn is_null(row: &[Datum], idx: usize) -> bool {
    matches!(row.get(idx), None | Some(Datum::Null))
}

pub(crate) fn get_int64(row: &[Datum], idx: usize) -> i64 {
    match row.get(idx) {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}

pub(crate) fn get_bytes(row: &[Datum], idx: usize) -> Vec<u8> {
    match row.get(idx) {
        Some(Datum::String(value)) => value.bytes().to_vec(),
        Some(Datum::Bytes(value) | Datum::Raw(value)) => value.clone(),
        Some(Datum::Json(value)) => value.to_string().into_bytes(),
        _ => Vec::new(),
    }
}

pub(crate) fn get_string(row: &[Datum], idx: usize) -> String {
    String::from_utf8_lossy(&get_bytes(row, idx)).into_owned()
}

/// Go `r.GetJSON(idx).String()`.
pub(crate) fn get_json_string(row: &[Datum], idx: usize) -> String {
    match row.get(idx) {
        Some(Datum::Json(value)) => value.to_string(),
        _ => get_string(row, idx),
    }
}

/// Go `r.GetTime(idx).GoTime(time.Local)`, its error ignored.
fn get_local_time(row: &[Datum], idx: usize) -> DateTime<FixedOffset> {
    match row.get(idx) {
        Some(Datum::Time(value)) => value
            .go_time(&chrono::Local)
            .map_or_else(|_| zero_time(), |time| time.fixed_offset()),
        _ => zero_time(),
    }
}

/// Go `time.Unix(ts, 0)`, in the local zone.
fn unix_local(seconds: i64) -> DateTime<FixedOffset> {
    chrono::Local
        .timestamp_opt(seconds, 0)
        .single()
        .map_or_else(zero_time, |time| time.fixed_offset())
}

/// Go `row2TaskBasic`.
pub(crate) fn row2_task_basic(r: &[Datum]) -> TaskBase {
    let create_time = get_local_time(r, 7);
    let mut extra_params = ExtraParams::default();
    if !is_null(r, 10) {
        let text = get_json_string(r, 10);
        match serde_json::from_str(&text) {
            Ok(params) => extra_params = params,
            Err(error) => tracing::error!(%error, "unmarshal task extra params"),
        }
    }
    TaskBase {
        id: get_int64(r, 0),
        key: get_string(r, 1),
        tp: TaskType::new(get_string(r, 2)),
        state: TaskState::new(get_string(r, 3)),
        step: Step(get_int64(r, 4)),
        priority: get_int64(r, 5) as isize,
        required_slots: get_int64(r, 6) as isize,
        create_time,
        target_scope: get_string(r, 8),
        max_node_count: get_int64(r, 9) as isize,
        extra_params,
        keyspace: get_string(r, 11),
    }
}

/// A task's stored error (Go `errors.Normalize("")` read back from its
/// JSON, or the raw text when that fails).
#[derive(Debug)]
pub struct TaskError(pub String);

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TaskError {}

/// Go `row2TaskError`.
fn row2_task_error(r: &[Datum], idx: usize) -> Option<std::sync::Arc<dyn std::error::Error + Send + Sync>> {
    if is_null(r, idx) {
        return None;
    }
    let bytes = get_bytes(r, idx);
    // Go's `*errors.Error` marshals its message under "message".
    let message = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(value) => value
            .get("message")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .unwrap_or_default(),
        Err(error) => {
            tracing::error!(%error, "unmarshal task error");
            String::from_utf8_lossy(&bytes).into_owned()
        }
    };
    Some(std::sync::Arc::new(TaskError(message)))
}

/// Go `Row2Task`.
#[must_use]
pub fn row2_task(r: &[Datum]) -> Task {
    let base = row2_task_basic(r);
    let start_time = if is_null(r, 12) { zero_time() } else { get_local_time(r, 12) };
    let state_update_time = if is_null(r, 13) { zero_time() } else { get_local_time(r, 13) };
    let mut modify_param = ModifyParam::default();
    if !is_null(r, 17) {
        match serde_json::from_str(&get_json_string(r, 17)) {
            Ok(param) => modify_param = param,
            Err(error) => tracing::error!(%error, "unmarshal task modify param"),
        }
    }
    Task {
        base,
        scheduler_id: get_string(r, 15),
        start_time,
        state_update_time,
        meta: get_bytes(r, 14),
        error: row2_task_error(r, 16),
        modify_param,
    }
}

/// Go `TaskIDToKey`: the decimal key the subtask tables store (their
/// `task_key` column is a varchar holding the task's id).
#[must_use]
pub fn task_id_to_key(task_id: i64) -> String {
    task_id.to_string()
}

/// Go `row2BasicSubTask`.
pub(crate) fn row2_basic_sub_task(r: &[Datum]) -> SubtaskBase {
    let task_id_str = get_string(r, 2);
    let task_id = task_id_str.parse::<i64>().unwrap_or_else(|_| {
        tracing::warn!(subtask_id = %task_id_str, "unexpected subtask id");
        0
    });
    let ordinal = if is_null(r, 8) { 0 } else { get_int64(r, 8) as isize };
    // The subtask table defines start time as bigint.
    let start_time = if is_null(r, 9) { zero_time() } else { unix_local(get_int64(r, 9)) };
    SubtaskBase {
        id: get_int64(r, 0),
        step: Step(get_int64(r, 1)),
        task_id,
        tp: int_2_type(get_int64(r, 3) as isize),
        exec_id: get_string(r, 4),
        state: SubtaskState::new(get_string(r, 5)),
        concurrency: get_int64(r, 6) as isize,
        create_time: get_local_time(r, 7),
        ordinal,
        start_time,
    }
}

/// Go `Row2SubTask`.
#[must_use]
pub fn row2_sub_task(r: &[Datum]) -> Subtask {
    // The subtask table defines update time as bigint.
    let update_time = if is_null(r, 10) { zero_time() } else { unix_local(get_int64(r, 10)) };
    Subtask {
        base: row2_basic_sub_task(r),
        update_time,
        meta: get_bytes(r, 11),
        summary: get_json_string(r, 12),
    }
}
