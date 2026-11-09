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

//! Go `table_util.go`.

use chrono::{DateTime, FixedOffset};
use tidb_datatype::Datum;
use tidb_dxf::{NodeResource, Subtask, Task, TaskState, TASK_STATE_CANCELLING, TASK_STATE_PENDING};
use tidb_dxf_storage::{
    get_node_resource, row2_sub_task, row2_task, set_node_resource, task_id_to_key, Result, TaskManager,
    SUBTASK_COLUMNS, TASK_COLUMNS,
};
use tidb_sqlescape::SqlArg;

/// Go `GoTime(time.Local)` of a row's time, the zero time for NULL.
fn local_time(row: &[Datum], idx: usize) -> Result<DateTime<FixedOffset>> {
    match row.get(idx) {
        Some(Datum::Time(value)) => value
            .go_time(&chrono::Local)
            .map(|time| time.fixed_offset())
            .map_err(|error| tidb_dxf_storage::StorageError::Other(error.to_string())),
        _ => Ok(tidb_dxf::go_zero_time()),
    }
}

fn get_string(row: &[Datum], idx: usize) -> String {
    match row.get(idx) {
        Some(Datum::String(value)) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Some(Datum::Bytes(value)) => String::from_utf8_lossy(value).into_owned(),
        _ => String::new(),
    }
}

/// Go `GetOneTask`: a pending task from the task table.
///
/// # Errors
/// The statement failed.
pub fn get_one_task(mgr: &TaskManager) -> Result<Option<Task>> {
    let rs = mgr.execute_sql_with_new_session(
        &format!("select {TASK_COLUMNS} from mysql.tidb_global_task t where state = %? limit 1"),
        &[SqlArg::String(TASK_STATE_PENDING.to_string().as_bytes())],
    )?;
    Ok(rs.first().map(|row| row2_task(row)))
}

/// Go `GetSubtasksFromHistory`: the number of history subtasks.
///
/// # Errors
/// The statement failed.
pub fn get_subtasks_from_history(mgr: &TaskManager) -> Result<usize> {
    Ok(mgr.execute_sql_with_new_session("select * from mysql.tidb_background_subtask_history", &[])?.len())
}

/// Go `GetSubtasksFromHistoryByTaskID`.
///
/// # Errors
/// The statement failed.
pub fn get_subtasks_from_history_by_task_id(mgr: &TaskManager, task_id: i64) -> Result<usize> {
    let key = task_id_to_key(task_id);
    Ok(mgr
        .execute_sql_with_new_session(
            &format!("select {SUBTASK_COLUMNS} from mysql.tidb_background_subtask_history where task_key = %?"),
            &[SqlArg::String(key.as_bytes())],
        )?
        .len())
}

/// Go `GetSubtasksByTaskID`.
///
/// # Errors
/// The statement failed.
pub fn get_subtasks_by_task_id(mgr: &TaskManager, task_id: i64) -> Result<Vec<Subtask>> {
    let key = task_id_to_key(task_id);
    let rs = mgr.execute_sql_with_new_session(
        &format!("select {SUBTASK_COLUMNS} from mysql.tidb_background_subtask where task_key = %?"),
        &[SqlArg::String(key.as_bytes())],
    )?;
    Ok(rs.iter().map(|r| row2_sub_task(r)).collect())
}

/// Go `GetTasksFromHistory`: the number of history tasks.
///
/// # Errors
/// The statement failed.
pub fn get_tasks_from_history(mgr: &TaskManager) -> Result<usize> {
    Ok(mgr.execute_sql_with_new_session("select * from mysql.tidb_global_task_history", &[])?.len())
}

/// Go `GetTaskEndTime`.
///
/// # Errors
/// The time does not convert.
pub fn get_task_end_time(mgr: &TaskManager, task_id: i64) -> Result<DateTime<FixedOffset>> {
    let Ok(rs) = mgr.execute_sql_with_new_session(
        "select end_time
\t\tfrom mysql.tidb_global_task
\t    where id = %?",
        &[SqlArg::Signed(task_id)],
    ) else {
        return Ok(tidb_dxf::go_zero_time());
    };
    local_time(&rs[0], 0)
}

/// Go `GetSubtaskEndTime`.
///
/// # Errors
/// The time does not convert.
pub fn get_subtask_end_time(mgr: &TaskManager, subtask_id: i64) -> Result<DateTime<FixedOffset>> {
    let Ok(rs) = mgr.execute_sql_with_new_session(
        "select end_time
\t\tfrom mysql.tidb_background_subtask
\t    where id = %?",
        &[SqlArg::Signed(subtask_id)],
    ) else {
        return Ok(tidb_dxf::go_zero_time());
    };
    local_time(&rs[0], 0)
}

/// Go `GetSubtaskNodes`: the nodes that run or ran the task.
///
/// # Errors
/// The statement failed.
pub fn get_subtask_nodes(mgr: &TaskManager, task_id: i64) -> Result<Vec<String>> {
    let key = task_id_to_key(task_id);
    let rs = mgr.execute_sql_with_new_session(
        "
\t\tselect distinct(exec_id) from mysql.tidb_background_subtask where task_key=%?
\t\tunion
\t\tselect distinct(exec_id) from mysql.tidb_background_subtask_history where task_key=%?",
        &[SqlArg::String(key.as_bytes()), SqlArg::String(key.as_bytes())],
    )?;
    Ok(rs.iter().filter(|r| !matches!(r.first(), None | Some(Datum::Null))).map(|r| get_string(r, 0)).collect())
}

/// Go `UpdateSubtaskExecID`.
///
/// # Errors
/// The statement failed.
pub fn update_subtask_exec_id(mgr: &TaskManager, tidb_id: &str, subtask_id: i64) -> Result<()> {
    mgr.execute_sql_with_new_session(
        "update mysql.tidb_background_subtask
\t\t set exec_id = %?, state_update_time = unix_timestamp()
\t\t where id = %?",
        &[SqlArg::String(tidb_id.as_bytes()), SqlArg::Signed(subtask_id)],
    )?;
    Ok(())
}

/// Go `TransferSubTasks2History`.
///
/// # Errors
/// A statement failed.
pub fn transfer_sub_tasks2_history(mgr: &TaskManager, task_id: i64) -> Result<()> {
    mgr.with_new_session(&mut |se| TaskManager::transfer_subtasks2_history_with_session(se, task_id))
}

/// Go `GetTasksFromHistoryInStates`.
///
/// # Errors
/// The statement failed.
pub fn get_tasks_from_history_in_states(mgr: &TaskManager, states: &[TaskState]) -> Result<Vec<Task>> {
    if states.is_empty() {
        return Ok(Vec::new());
    }
    let states: Vec<String> = states.iter().map(ToString::to_string).collect();
    let args: Vec<SqlArg<'_>> = states.iter().map(|state| SqlArg::String(state.as_bytes())).collect();
    let rs = mgr.execute_sql_with_new_session(
        &format!(
            "select {TASK_COLUMNS} from mysql.tidb_global_task_history t where state in ({}%?)",
            "%?,".repeat(states.len() - 1)
        ),
        &args,
    )?;
    Ok(rs.iter().map(|r| row2_task(r)).collect())
}

/// Go `DeleteSubtasksByTaskID`.
///
/// # Errors
/// The statement failed.
pub fn delete_subtasks_by_task_id(mgr: &TaskManager, task_id: i64) -> Result<()> {
    let key = task_id_to_key(task_id);
    mgr.execute_sql_with_new_session(
        "delete from mysql.tidb_background_subtask
\t\twhere task_key = %?",
        &[SqlArg::String(key.as_bytes())],
    )?;
    Ok(())
}

/// Go `IsTaskCancelling`.
///
/// # Errors
/// The statement failed.
pub fn is_task_cancelling(mgr: &TaskManager, task_id: i64) -> Result<bool> {
    let rs = mgr.execute_sql_with_new_session(
        "select 1 from mysql.tidb_global_task where id=%? and state = %?",
        &[SqlArg::Signed(task_id), SqlArg::String(TASK_STATE_CANCELLING.to_string().as_bytes())],
    )?;
    Ok(!rs.is_empty())
}

/// Go `PrintSubtaskInfo`: logs the task's subtasks.
pub fn print_subtask_info(mgr: &TaskManager, task_id: i64) {
    let key = task_id_to_key(task_id);
    let args = [SqlArg::String(key.as_bytes())];
    let mut rs = mgr
        .execute_sql_with_new_session(
            &format!("select {SUBTASK_COLUMNS} from mysql.tidb_background_subtask_history where task_key = %?"),
            &args,
        )
        .unwrap_or_default();
    rs.extend(
        mgr.execute_sql_with_new_session(
            &format!("select {SUBTASK_COLUMNS} from mysql.tidb_background_subtask where task_key = %?"),
            &args,
        )
        .unwrap_or_default(),
    );
    for r in &rs {
        tracing::info!("subTask: {}\n", row2_sub_task(r).base);
    }
}

/// Go `MockNodeResource`: the node resource with `cpu` cores until the
/// returned guard drops (Go's `t.Cleanup`).
#[must_use]
pub fn mock_node_resource(cpu: isize) -> NodeResourceGuard {
    let origin_node_resource = get_node_resource();
    set_node_resource(NodeResource::new(cpu, cpu as i64 * 2 * (1 << 30), 100 * (1 << 30)));
    NodeResourceGuard(Some(origin_node_resource))
}

/// Restores the node resource [`mock_node_resource`] replaced.
pub struct NodeResourceGuard(Option<NodeResource>);

impl Drop for NodeResourceGuard {
    fn drop(&mut self) {
        if let Some(origin) = self.0.take() {
            set_node_resource(origin);
        }
    }
}
