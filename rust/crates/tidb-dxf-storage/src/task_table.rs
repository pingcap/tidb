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


//! Go `task_table.go`: task submission and the task queries.

use std::collections::HashMap;

use tidb_datatype::Datum;
use tidb_dxf::{
    get_max_concurrent_task, get_task_cleanup_batch_size, ExtraParams, Task, TaskBase, TaskState, TaskType,
    NORMAL_PRIORITY, STEP_INIT, SUBTASK_STATE_PENDING, SUBTASK_STATE_RUNNING, TASK_STATE_CANCELLING,
    TASK_STATE_FAILED, TASK_STATE_MODIFYING, TASK_STATE_PAUSING, TASK_STATE_PENDING, TASK_STATE_RESUMING,
    TASK_STATE_REVERTED, TASK_STATE_REVERTING, TASK_STATE_RUNNING, TASK_STATE_SUCCEED,
};
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::BackgroundContext;

use crate::converter::row2_task_basic;
use crate::{exec_sql, row2_task, Result, SessionContext, StorageError, TaskManager, BASIC_TASK_COLUMNS, INSERT_TASK_COLUMNS, TASK_COLUMNS};

/// Go `TaskExecInfo`: a task's execution information on one node.
#[derive(Debug, Clone)]
pub struct TaskExecInfo {
    /// Go's embedded `*proto.TaskBase`.
    pub task_base: TaskBase,
    /// The concurrency of the task's subtasks in its current step.
    pub subtask_concurrency: isize,
}

/// Owned SQL arguments, borrowed as `SqlArg`s for one statement.
pub(crate) enum Arg {
    Str(String),
    Int(i64),
    Bytes(Vec<u8>),
    Json(Vec<u8>),
}

impl Arg {
    pub(crate) fn sql_arg(&self) -> SqlArg<'_> {
        match self {
            Self::Str(value) => SqlArg::String(value.as_bytes()),
            Self::Int(value) => SqlArg::Signed(*value),
            Self::Bytes(value) => SqlArg::Bytes(Some(value)),
            Self::Json(value) => SqlArg::RawJson(value),
        }
    }
}

/// Go's variadic `args ...any` as one statement's arguments.
pub(crate) fn sql_args(args: &[Arg]) -> Vec<SqlArg<'_>> {
    args.iter().map(Arg::sql_arg).collect()
}

fn state_args(states: &[TaskState]) -> Vec<Arg> {
    states.iter().map(|state| Arg::Str(state.to_string())).collect()
}

/// Go `strings.Repeat("%?,", n-1) + "%?"`.
fn holders(n: usize) -> String {
    vec!["%?"; n].join(",")
}

fn first_task(rows: &[Vec<Datum>]) -> Result<Task> {
    rows.first().map(|row| row2_task(row)).ok_or(StorageError::TaskNotFound)
}

fn first_task_base(rows: &[Vec<Datum>]) -> Result<TaskBase> {
    rows.first().map(|row| row2_task_basic(row)).ok_or(StorageError::TaskNotFound)
}

impl TaskManager {
    /// Go `CreateTask`: adds a task to the task table.
    ///
    /// # Errors
    /// The task needs more slots than a managed node has, or the session
    /// failed.
    #[allow(clippy::too_many_arguments)]
    pub fn create_task(
        &self,
        key: &str,
        tp: &TaskType,
        keyspace: &str,
        required_slots: isize,
        target_scope: &str,
        max_node_cnt: isize,
        extra_params: &ExtraParams,
        meta: &[u8],
    ) -> Result<i64> {
        let mut task_id = 0;
        self.with_new_session(&mut |se| {
            task_id = Self::create_task_with_session(
                se, key, tp, keyspace, required_slots, target_scope, max_node_cnt, extra_params, meta,
            )?;
            Ok(())
        })?;
        Ok(task_id)
    }

    /// Go `CreateTaskWithSession`.
    ///
    /// # Errors
    /// The task needs more slots than a managed node has, or a statement
    /// failed.
    #[allow(clippy::too_many_arguments)]
    pub fn create_task_with_session(
        se: &dyn SessionContext,
        key: &str,
        tp: &TaskType,
        keyspace: &str,
        required_slots: isize,
        target_scope: &str,
        max_node_count: isize,
        extra_params: &ExtraParams,
        meta: &[u8],
    ) -> Result<i64> {
        let cpu_count = Self::get_cpu_count_of_node_by_role_with_session(se, "", true)?;
        if required_slots > cpu_count {
            return Err(StorageError::Other(format!(
                "task required slots({required_slots}) larger than cpu count({cpu_count}) of managed node"
            )));
        }
        let extra_param_bytes =
            serde_json::to_vec(extra_params).map_err(|error| StorageError::Other(error.to_string()))?;
        let args = [
            Arg::Str(key.to_owned()),
            Arg::Str(tp.to_string()),
            Arg::Str(TASK_STATE_PENDING.to_string()),
            Arg::Int(NORMAL_PRIORITY as i64),
            Arg::Int(required_slots as i64),
            Arg::Int(STEP_INIT.0),
            Arg::Bytes(meta.to_vec()),
            Arg::Str(target_scope.to_owned()),
            Arg::Int(max_node_count as i64),
            Arg::Json(extra_param_bytes),
            Arg::Str(keyspace.to_owned()),
        ];
        exec_sql(
            &BackgroundContext,
            se,
            &format!(
                "
			insert into mysql.tidb_global_task({INSERT_TASK_COLUMNS})
			values (%?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), %?, %?, %?, %?)"
            ),
            &sql_args(&args),
        )?;
        let rows = exec_sql(&BackgroundContext, se, "select @@last_insert_id", &[])?;
        Ok(match rows.first().and_then(|row| row.first()) {
            Some(Datum::UInt(id)) => *id as i64,
            Some(Datum::Int(id)) => *id,
            _ => 0,
        })
    }

    /// Go `GetTopUnfinishedTasks`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_top_unfinished_tasks(&self) -> Result<Vec<TaskBase>> {
        self.get_top_tasks(&[
            TASK_STATE_PENDING,
            TASK_STATE_RUNNING,
            TASK_STATE_REVERTING,
            TASK_STATE_CANCELLING,
            TASK_STATE_PAUSING,
            TASK_STATE_RESUMING,
            TASK_STATE_MODIFYING,
        ])
    }

    /// Go `GetTopNoNeedResourceTasks`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_top_no_need_resource_tasks(&self) -> Result<Vec<TaskBase>> {
        self.get_top_tasks(&[TASK_STATE_REVERTING, TASK_STATE_CANCELLING, TASK_STATE_PAUSING, TASK_STATE_MODIFYING])
    }

    /// Go `getTopTasks`.
    fn get_top_tasks(&self, states: &[TaskState]) -> Result<Vec<TaskBase>> {
        let sql = format!(
            "select {BASIC_TASK_COLUMNS} from mysql.tidb_global_task t
		where state in ({})
		order by priority asc, create_time asc, id asc
		limit %?",
            holders(states.len())
        );
        let mut args = state_args(states);
        args.push(Arg::Int(get_max_concurrent_task() as i64 * 2));
        let rows = self.execute_sql_with_new_session(&sql, &sql_args(&args))?;
        Ok(rows.iter().map(|row| row2_task_basic(row)).collect())
    }

    /// Go `GetTaskExecInfoByExecID`: the running, reverting or pausing tasks
    /// with pending or running subtasks on `exec_id`.
    ///
    /// # Errors
    /// The session or a statement failed.
    pub fn get_task_exec_info_by_exec_id(&self, exec_id: &str) -> Result<Vec<TaskExecInfo>> {
        let mut res = Vec::new();
        self.with_new_session(&mut |se| {
            // A task does not move to its next step while subtasks are in
            // these states, so their step is their task's current step.
            let args = [
                Arg::Str(exec_id.to_owned()),
                Arg::Str(SUBTASK_STATE_PENDING.to_string()),
                Arg::Str(SUBTASK_STATE_RUNNING.to_string()),
            ];
            let rows = exec_sql(
                &BackgroundContext,
                se,
                "select st.task_key, max(st.concurrency) from mysql.tidb_background_subtask st
			where st.exec_id = %? and st.state in (%?, %?)
			group by st.task_key",
                &sql_args(&args),
            )?;
            if rows.is_empty() {
                return Ok(());
            }
            let mut max_subtask_con: HashMap<i64, isize> = HashMap::with_capacity(rows.len());
            let mut task_ids = Vec::with_capacity(rows.len());
            for row in &rows {
                let task_id_str = match row.first() {
                    Some(Datum::String(value)) => String::from_utf8_lossy(value.bytes()).into_owned(),
                    Some(Datum::Bytes(value)) => String::from_utf8_lossy(value).into_owned(),
                    _ => String::new(),
                };
                let task_id = task_id_str
                    .parse::<i64>()
                    .map_err(|error| StorageError::Other(error.to_string()))?;
                let concurrency = match row.get(1) {
                    Some(Datum::Int(value)) => *value as isize,
                    Some(Datum::UInt(value)) => *value as isize,
                    _ => 0,
                };
                max_subtask_con.insert(task_id, concurrency);
                task_ids.push(task_id_str);
            }
            let args = [
                Arg::Str(TASK_STATE_RUNNING.to_string()),
                Arg::Str(TASK_STATE_REVERTING.to_string()),
                Arg::Str(TASK_STATE_PAUSING.to_string()),
            ];
            let rows = exec_sql(
                &BackgroundContext,
                se,
                &format!(
                    "select {BASIC_TASK_COLUMNS} from mysql.tidb_global_task t
			where t.id in ({}) and t.state in (%?, %?, %?)
			order by priority asc, create_time asc, id asc",
                    task_ids.join(",")
                ),
                &sql_args(&args),
            )?;
            res = rows
                .iter()
                .map(|row| {
                    let task_base = row2_task_basic(row);
                    let subtask_concurrency = max_subtask_con.get(&task_base.id).copied().unwrap_or(0);
                    TaskExecInfo { task_base, subtask_concurrency }
                })
                .collect();
            Ok(())
        })?;
        Ok(res)
    }

    /// Go `GetCleanupTasks`: finished tasks, as many as the cleanup batch
    /// size.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_cleanup_tasks(&self) -> Result<Vec<Task>> {
        let args = [
            Arg::Str(TASK_STATE_FAILED.to_string()),
            Arg::Str(TASK_STATE_REVERTED.to_string()),
            Arg::Str(TASK_STATE_SUCCEED.to_string()),
            Arg::Int(get_task_cleanup_batch_size() as i64),
        ];
        let rows = self.execute_sql_with_new_session(
            &format!("select {TASK_COLUMNS} from mysql.tidb_global_task t where state in (%?, %?, %?) limit %?"),
            &sql_args(&args),
        )?;
        Ok(rows.iter().map(|row| row2_task(row)).collect())
    }

    /// Go `GetTasksInStates`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_tasks_in_states(&self, states: &[TaskState]) -> Result<Vec<Task>> {
        if states.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self.execute_sql_with_new_session(
            &format!(
                "select {TASK_COLUMNS} from mysql.tidb_global_task t where state in ({}) order by priority asc, create_time asc, id asc",
                holders(states.len())
            ),
            &sql_args(&state_args(states)),
        )?;
        Ok(rows.iter().map(|row| row2_task(row)).collect())
    }

    /// Go `GetTaskBasesInStates`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_task_bases_in_states(&self, states: &[TaskState]) -> Result<Vec<TaskBase>> {
        if states.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self.execute_sql_with_new_session(
            &format!(
                "select {BASIC_TASK_COLUMNS} from mysql.tidb_global_task t where state in ({}) order by priority asc, create_time asc, id asc",
                holders(states.len())
            ),
            &sql_args(&state_args(states)),
        )?;
        Ok(rows.iter().map(|row| row2_task_basic(row)).collect())
    }

    /// Go `GetTaskByID`.
    ///
    /// # Errors
    /// The task is absent, or the session failed.
    pub fn get_task_by_id(&self, task_id: i64) -> Result<Task> {
        let rows = self.execute_sql_with_new_session(
            &format!("select {TASK_COLUMNS} from mysql.tidb_global_task t where id = %?"),
            &[SqlArg::Signed(task_id)],
        )?;
        first_task(&rows)
    }

    /// Go `GetTaskBaseByID`.
    ///
    /// # Errors
    /// The task is absent, or the session failed.
    pub fn get_task_base_by_id(&self, task_id: i64) -> Result<TaskBase> {
        let mut task = None;
        self.with_new_session(&mut |se| {
            task = Some(Self::get_task_base_by_id_with_session(se, task_id)?);
            Ok(())
        })?;
        task.ok_or(StorageError::TaskNotFound)
    }

    /// Go `getTaskBaseByID`.
    pub(crate) fn get_task_base_by_id_with_session(se: &dyn SessionContext, task_id: i64) -> Result<TaskBase> {
        let rows = exec_sql(
            &BackgroundContext,
            se,
            &format!("select {BASIC_TASK_COLUMNS} from mysql.tidb_global_task t where id = %?"),
            &[SqlArg::Signed(task_id)],
        )?;
        first_task_base(&rows)
    }

    /// Go `GetTaskByIDWithHistory`.
    ///
    /// # Errors
    /// The task is in neither table, or the session failed.
    pub fn get_task_by_id_with_history(&self, task_id: i64) -> Result<Task> {
        let rows = self.execute_sql_with_new_session(
            &format!(
                "select {TASK_COLUMNS} from mysql.tidb_global_task t where id = %? union select {TASK_COLUMNS} from mysql.tidb_global_task_history t where id = %?"
            ),
            &[SqlArg::Signed(task_id), SqlArg::Signed(task_id)],
        )?;
        first_task(&rows)
    }

    /// Go `GetTaskBaseByIDWithHistory`.
    ///
    /// # Errors
    /// The task is in neither table, or the session failed.
    pub fn get_task_base_by_id_with_history(&self, task_id: i64) -> Result<TaskBase> {
        let rows = self.execute_sql_with_new_session(
            &format!(
                "select {BASIC_TASK_COLUMNS} from mysql.tidb_global_task t where id = %? union select {BASIC_TASK_COLUMNS} from mysql.tidb_global_task_history t where id = %?"
            ),
            &[SqlArg::Signed(task_id), SqlArg::Signed(task_id)],
        )?;
        first_task_base(&rows)
    }

    /// Go `GetTaskByKey`.
    ///
    /// # Errors
    /// The task is absent, or the session failed.
    pub fn get_task_by_key(&self, key: &str) -> Result<Task> {
        let rows = self.execute_sql_with_new_session(
            &format!("select {TASK_COLUMNS} from mysql.tidb_global_task t where task_key = %?"),
            &[SqlArg::String(key.as_bytes())],
        )?;
        first_task(&rows)
    }

    /// Go `GetTaskByKeyWithHistory`.
    ///
    /// # Errors
    /// The task is in neither table, or the session failed.
    pub fn get_task_by_key_with_history(&self, key: &str) -> Result<Task> {
        let rows = self.execute_sql_with_new_session(
            &format!(
                "select {TASK_COLUMNS} from mysql.tidb_global_task t where task_key = %?union select {TASK_COLUMNS} from mysql.tidb_global_task_history t where task_key = %?"
            ),
            &[SqlArg::String(key.as_bytes()), SqlArg::String(key.as_bytes())],
        )?;
        first_task(&rows)
    }

    /// Go `GetTaskBaseByKeyWithHistory`.
    ///
    /// # Errors
    /// The task is in neither table, or the session failed.
    pub fn get_task_base_by_key_with_history(&self, key: &str) -> Result<TaskBase> {
        let rows = self.execute_sql_with_new_session(
            &format!(
                "select {BASIC_TASK_COLUMNS} from mysql.tidb_global_task t where task_key = %?union select {BASIC_TASK_COLUMNS} from mysql.tidb_global_task_history t where task_key = %?"
            ),
            &[SqlArg::String(key.as_bytes()), SqlArg::String(key.as_bytes())],
        )?;
        first_task_base(&rows)
    }
}
