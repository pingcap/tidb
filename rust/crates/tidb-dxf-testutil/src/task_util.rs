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

//! Go `task_util.go`.

use tidb_datatype::Datum;
use tidb_dxf::{type_2_int, Step, SubtaskState, TaskType, SUBTASK_STATE_PENDING};
use tidb_dxf_execute::SubtaskSummary;
use tidb_dxf_storage::{exec_sql, TaskManager, INSERT_SUBTASK_COLUMNS};
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::BackgroundContext;

fn last_insert_id(rows: &[Vec<Datum>]) -> i64 {
    match rows.first().and_then(|row| row.first()) {
        Some(Datum::Int(id)) => *id,
        Some(Datum::UInt(id)) => *id as i64,
        _ => 0,
    }
}

/// Go `CreateSubTask`: a pending subtask in the subtask table (`None`
/// meta is Go's nil, written as NULL).
///
/// # Panics
/// The insert failed (Go's `require.NoError`).
#[allow(clippy::too_many_arguments)]
pub fn create_sub_task(
    gm: &TaskManager,
    task_id: i64,
    step: Step,
    exec_id: &str,
    meta: Option<&[u8]>,
    tp: &TaskType,
    concurrency: isize,
) -> i64 {
    insert_subtask(gm, task_id, step, exec_id, meta, &SUBTASK_STATE_PENDING, tp, concurrency)
}

/// Go `InsertSubtask`: a subtask of any state in the subtask table.
///
/// # Panics
/// The insert failed (Go's `require.NoError`).
#[allow(clippy::too_many_arguments)]
pub fn insert_subtask(
    gm: &TaskManager,
    task_id: i64,
    step: Step,
    exec_id: &str,
    meta: Option<&[u8]>,
    state: &SubtaskState,
    tp: &TaskType,
    concurrency: isize,
) -> i64 {
    let mut id = 0;
    let state = state.to_string();
    gm.with_new_session(&mut |se| {
        exec_sql(
            &BackgroundContext,
            se,
            &format!(
                "
\t\t\tinsert into mysql.tidb_background_subtask({INSERT_SUBTASK_COLUMNS}) values(%?, %?, %?, %?, %?, %?, %?, NULL, CURRENT_TIMESTAMP(), '{{}}', '{{}}')"
            ),
            &[
                SqlArg::Signed(step.0),
                SqlArg::Signed(task_id),
                SqlArg::String(exec_id.as_bytes()),
                SqlArg::Bytes(meta),
                SqlArg::String(state.as_bytes()),
                SqlArg::Signed(type_2_int(tp) as i64),
                SqlArg::Signed(concurrency as i64),
            ],
        )?;
        id = last_insert_id(&exec_sql(&BackgroundContext, se, "select @@last_insert_id", &[])?);
        Ok(())
    })
    .expect("insert subtask");
    id
}

/// Go `CreateSubTaskWithSummary`.
///
/// # Panics
/// The insert failed (Go's `require.NoError`).
#[allow(clippy::too_many_arguments)]
pub fn create_sub_task_with_summary(
    gm: &TaskManager,
    task_id: i64,
    step: Step,
    exec_id: &str,
    meta: Option<&[u8]>,
    summary: &SubtaskSummary,
    state: &SubtaskState,
    tp: &TaskType,
    concurrency: isize,
) -> i64 {
    insert_subtask_with_summary(gm, task_id, step, exec_id, meta, summary, state, tp, concurrency)
}

/// Go `InsertSubtaskWithSummary`: a subtask of any state with the summary,
/// started now.
///
/// # Panics
/// The insert failed (Go's `require.NoError`).
#[allow(clippy::too_many_arguments)]
pub fn insert_subtask_with_summary(
    gm: &TaskManager,
    task_id: i64,
    step: Step,
    exec_id: &str,
    meta: Option<&[u8]>,
    summary: &SubtaskSummary,
    state: &SubtaskState,
    tp: &TaskType,
    concurrency: isize,
) -> i64 {
    let mut id = 0;
    let state = state.to_string();
    let summary_bytes = tidb_stringutil::go_json_marshal(summary).into_bytes();
    gm.with_new_session(&mut |se| {
        exec_sql(
            &BackgroundContext,
            se,
            &format!(
                "
\t\t\tinsert into mysql.tidb_background_subtask({INSERT_SUBTASK_COLUMNS},start_time) values(%?, %?, %?, %?, %?, %?, %?, NULL, CURRENT_TIMESTAMP(), '{{}}', %?, CURRENT_TIMESTAMP())"
            ),
            &[
                SqlArg::Signed(step.0),
                SqlArg::Signed(task_id),
                SqlArg::String(exec_id.as_bytes()),
                SqlArg::Bytes(meta),
                SqlArg::String(state.as_bytes()),
                SqlArg::Signed(type_2_int(tp) as i64),
                SqlArg::Signed(concurrency as i64),
                SqlArg::Bytes(Some(&summary_bytes)),
            ],
        )?;
        id = last_insert_id(&exec_sql(&BackgroundContext, se, "select @@last_insert_id", &[])?);
        Ok(())
    })
    .expect("insert subtask with summary");
    id
}

/// Go `getTaskKS`: the keyspace tests submit tasks to (Go `keyspace.System`
/// on a next-gen kernel).
#[must_use]
pub(crate) fn get_task_ks() -> String {
    if tidb_config_kerneltype::is_next_gen() {
        return "SYSTEM".to_owned();
    }
    String::new()
}
