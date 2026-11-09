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
    get_max_concurrent_task, get_task_cleanup_batch_size, type_2_int, ExtraParams, Step, Subtask, SubtaskBase, Task,
    TaskBase, TaskState, TaskType, NORMAL_PRIORITY, STEP_INIT, STEP_PREPARED, SUBTASK_STATE_PENDING,
    SUBTASK_STATE_RUNNING, TASK_STATE_CANCELLING,
    TASK_STATE_FAILED, TASK_STATE_MODIFYING, TASK_STATE_PAUSING, TASK_STATE_PENDING, TASK_STATE_RESUMING,
    TASK_STATE_REVERTED, TASK_STATE_REVERTING, TASK_STATE_RUNNING, TASK_STATE_SUCCEED,
};
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::BackgroundContext;

use crate::converter::{get_int64, get_string, is_null, row2_basic_sub_task, row2_task_basic};
use crate::{
    exec_sql, row2_sub_task, row2_task, task_id_to_key, Result, SessionContext, StorageError, TaskManager,
    BASIC_SUBTASK_COLUMNS, BASIC_TASK_COLUMNS, INSERT_SUBTASK_COLUMNS, INSERT_TASK_COLUMNS, MAX_SUBTASK_BATCH_SIZE,
    SUBTASK_COLUMNS, TASK_COLUMNS,
};

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
    /// Go's `[]byte` that may be nil, written as NULL.
    OptBytes(Option<Vec<u8>>),
    Json(Vec<u8>),
}

impl Arg {
    pub(crate) fn sql_arg(&self) -> SqlArg<'_> {
        match self {
            Self::Str(value) => SqlArg::String(value.as_bytes()),
            Self::Int(value) => SqlArg::Signed(*value),
            Self::Bytes(value) => SqlArg::Bytes(Some(value)),
            Self::OptBytes(value) => SqlArg::Bytes(value.as_deref()),
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

/// Go `ActiveTaskSummary`: the summary of active tasks in
/// `mysql.tidb_global_task`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActiveTaskSummary {
    /// Go `Total`.
    pub total: i64,
    /// Go `PerKeyspace`.
    pub per_keyspace: HashMap<String, i64>,
}

/// Go `serializeErr`: the error as Go's `*errors.Error` JSON, keeping a
/// terror's code and the messages of the whole chain; `None` for no error.
pub(crate) fn serialize_err(in_err: Option<&(dyn std::error::Error + 'static)>) -> Option<Vec<u8>> {
    let in_err = in_err?;
    // Go `goerrors.As(inErr, &tErr)`.
    let mut cause = Some(in_err);
    let mut found = None;
    while let Some(error) = cause {
        if let Some(t_err) = error.downcast_ref::<tidb_terror::TerrorError>() {
            found = Some(t_err);
            break;
        }
        cause = error.source();
    }
    // We want to keep the original message to have more context info.
    let message = tidb_terror::get_err_stack_msg(Some(in_err));
    let t_err = match found {
        Some(t_err) => tidb_terror::TerrorError::normalize(message, t_err.id(), t_err.code()),
        None => tidb_terror::TerrorError::normalize(message, "", tidb_terror::TerrorCode::new(0)),
    };
    Some(tidb_stringutil::go_json_marshal(&t_err).into_bytes())
}

impl TaskManager {
    /// Go `SwitchTaskStep`: moves the task to the next state and step and
    /// inserts the step's subtasks, in one transaction.
    ///
    /// # Errors
    /// The session or a statement failed.
    pub fn switch_task_step(
        &self,
        task: &Task,
        next_state: &TaskState,
        next_step: Step,
        subtasks: &[Subtask],
    ) -> Result<()> {
        self.with_new_txn(&mut |se| {
            let bak = se.mem_quota_query();
            let raised = bak < tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_QUERY;
            if raised {
                se.set_system_var(
                    tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_QUERY,
                    &tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_QUERY.to_string(),
                )?;
            }
            let outcome = Self::update_task_state_step(se, task, next_state, next_step).and_then(|()| {
                if se.affected_rows() == 0 {
                    // on network partition or owner change, there might be multiple
                    // schedulers for the same task, if other scheduler has switched
                    // the task to next step, skip the update process.
                    // Or when there is no such task.
                    return Ok(());
                }
                Self::insert_subtasks(se, subtasks)
            });
            if raised {
                let _ = se.set_system_var(tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_QUERY, &bak.to_string());
            }
            outcome
        })
    }

    /// Go `SwitchTaskStepAfterPrepare`: atomically persists prepare
    /// completion from pending+init to pending+prepared; whether this call
    /// switched it.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn switch_task_step_after_prepare(&self, task: &Task) -> Result<bool> {
        let mut switched = false;
        self.with_new_txn(&mut |se| {
            let args = [
                Arg::Int(STEP_PREPARED.0),
                Arg::Bytes(task.meta.clone()),
                Arg::Int(task.base.required_slots as i64),
                Arg::Int(task.base.max_node_count as i64),
                Arg::Int(task.base.id),
                Arg::Str(TASK_STATE_PENDING.to_string()),
                Arg::Int(STEP_INIT.0),
            ];
            exec_sql(
                &BackgroundContext,
                se,
                "
			update mysql.tidb_global_task
			set step = %?,
				state_update_time = CURRENT_TIMESTAMP(),
				meta = %?,
				concurrency = %?,
				max_node_count = %?
			where id = %? and state = %? and step = %?",
                &sql_args(&args),
            )?;
            switched = se.affected_rows() > 0;
            Ok(())
        })?;
        Ok(switched)
    }

    /// Go `updateTaskStateStep`.
    fn update_task_state_step(se: &dyn SessionContext, task: &Task, next_state: &TaskState, next_step: Step) -> Result<()> {
        let extra_update_str = if task.base.state == TASK_STATE_PENDING { "start_time = CURRENT_TIMESTAMP()," } else { "" };
        // TODO: during generating subtask, task meta might change, maybe move meta
        // update to another place.
        let args = [
            Arg::Str(next_state.to_string()),
            Arg::Int(next_step.0),
            Arg::Bytes(task.meta.clone()),
            Arg::Int(task.base.id),
            Arg::Str(task.base.state.to_string()),
            Arg::Int(task.base.step.0),
        ];
        exec_sql(
            &BackgroundContext,
            se,
            &format!(
                "
		update mysql.tidb_global_task
		set state = %?,
			step = %?, {extra_update_str}
			state_update_time = CURRENT_TIMESTAMP(),
			meta = %?
		where id = %? and state = %? and step = %?"
            ),
            &sql_args(&args),
        )?;
        Ok(())
    }

    /// Go `insertSubtasks`: one multi-row insert of pending subtasks.
    fn insert_subtasks(se: &dyn SessionContext, subtasks: &[Subtask]) -> Result<()> {
        if subtasks.is_empty() {
            return Ok(());
        }
        let mut args = Vec::with_capacity(subtasks.len() * 8);
        for subtask in subtasks {
            args.extend([
                Arg::Int(subtask.base.step.0),
                Arg::Int(subtask.base.task_id),
                Arg::Str(subtask.base.exec_id.clone()),
                Arg::Bytes(subtask.meta.clone()),
                Arg::Str(SUBTASK_STATE_PENDING.to_string()),
                Arg::Int(type_2_int(&subtask.base.tp) as i64),
                Arg::Int(subtask.base.concurrency as i64),
                Arg::Int(subtask.base.ordinal as i64),
            ]);
        }
        let marker_list = vec!["(%?, %?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), '{}', '{}')"; subtasks.len()];
        let sql = format!(
            "insert into mysql.tidb_background_subtask({INSERT_SUBTASK_COLUMNS}) values {}",
            marker_list.join(",")
        );
        exec_sql(&BackgroundContext, se, &sql, &sql_args(&args))?;
        Ok(())
    }

    /// Go `SwitchTaskStepInBatch`: inserts the step's subtasks in batches,
    /// skipping those another scheduler already inserted, then moves the
    /// task to the next state and step.
    ///
    /// # Errors
    /// More subtasks exist than expected (`UnstableSubtasks`), or the
    /// session or a statement failed.
    pub fn switch_task_step_in_batch(
        &self,
        task: &Task,
        next_state: &TaskState,
        next_step: Step,
        subtasks: &[Subtask],
    ) -> Result<()> {
        self.with_new_session(&mut |se| {
            // some subtasks may be inserted by other schedulers, we can skip them.
            let args = [Arg::Str(task_id_to_key(task.base.id)), Arg::Int(next_step.0)];
            let rs = exec_sql(
                &BackgroundContext,
                se,
                "
			select count(1) from mysql.tidb_background_subtask
			where task_key = %? and step = %?",
                &sql_args(&args),
            )?;
            let existing_task_cnt = rs.first().map_or(0, |row| get_int64(row, 0)) as usize;
            if existing_task_cnt > subtasks.len() {
                return Err(StorageError::annotate(
                    StorageError::UnstableSubtasks,
                    format!("expected {}, got {existing_task_cnt}", subtasks.len()),
                ));
            }
            for batch in Self::split_subtasks(&subtasks[existing_task_cnt..]) {
                Self::insert_subtasks(se, batch)?;
            }
            Self::update_task_state_step(se, task, next_state, next_step)
        })
    }

    /// Go `splitSubtasks`: batches whose metas fit the smaller of the
    /// transaction size limit and `maxSubtaskBatchSize`.
    fn split_subtasks(subtasks: &[Subtask]) -> Vec<&[Subtask]> {
        let mut res = Vec::with_capacity(10);
        let max_size = tidb_txnkv::txn_total_size_limit().min(max_subtask_batch_size() as u64) as usize;
        let (mut start, mut size) = (0, 0);
        for (i, s) in subtasks.iter().enumerate() {
            if size + s.meta.len() > max_size {
                res.push(&subtasks[start..i]);
                start = i;
                size = 0;
            }
            size += s.meta.len();
        }
        if start < subtasks.len() {
            res.push(&subtasks[start..]);
        }
        res
    }

    /// Go `GetSubtasksWithHistory`: the subtasks of a task's step from
    /// `mysql.tidb_background_subtask` and its history table.
    ///
    /// # Errors
    /// The session or a statement failed.
    pub fn get_subtasks_with_history(&self, task_id: i64, step: Step) -> Result<Vec<Subtask>> {
        let mut rs = Vec::new();
        self.with_new_txn(&mut |se| {
            let args = [Arg::Str(task_id_to_key(task_id)), Arg::Int(step.0)];
            rs = exec_sql(
                &BackgroundContext,
                se,
                &format!("select {SUBTASK_COLUMNS} from mysql.tidb_background_subtask where task_key = %? and step = %?"),
                &sql_args(&args),
            )?;
            // To avoid the situation that the subtasks has been `TransferTasks2History`
            // when the user show import jobs, we need to check the history table.
            let rs_from_history = exec_sql(
                &BackgroundContext,
                se,
                &format!(
                    "select {SUBTASK_COLUMNS} from mysql.tidb_background_subtask_history where task_key = %? and step = %?"
                ),
                &sql_args(&args),
            )?;
            rs.extend(rs_from_history);
            Ok(())
        })?;
        Ok(rs.iter().map(|r| row2_sub_task(r)).collect())
    }

    /// Go `GetAllTasks`: all tasks with basic columns.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_all_tasks(&self) -> Result<Vec<TaskBase>> {
        let rs = self.execute_sql_with_new_session(&format!("select {BASIC_TASK_COLUMNS} from mysql.tidb_global_task t"), &[])?;
        Ok(rs.iter().map(|r| row2_task_basic(r)).collect())
    }

    /// Go `GetActiveTaskCountsByKeyspace`: the active task summary grouped
    /// by keyspace.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_active_task_counts_by_keyspace(&self) -> Result<ActiveTaskSummary> {
        let rs = self.execute_sql_with_new_session(
            "select keyspace, count(1) from mysql.tidb_global_task group by keyspace",
            &[],
        )?;
        let mut summary = ActiveTaskSummary { total: 0, per_keyspace: HashMap::with_capacity(rs.len()) };
        for r in &rs {
            let keyspace = get_string(r, 0);
            let cnt = get_int64(r, 1);
            summary.total += cnt;
            summary.per_keyspace.insert(keyspace, cnt);
        }
        Ok(summary)
    }

    /// Go `GetAllSubtasks`: all subtasks with basic columns.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_all_subtasks(&self) -> Result<Vec<SubtaskBase>> {
        let rs = self.execute_sql_with_new_session(&format!("select {BASIC_SUBTASK_COLUMNS} from mysql.tidb_background_subtask"), &[])?;
        Ok(rs.iter().map(|r| row2_basic_sub_task(r)).collect())
    }

    /// Go `AdjustTaskOverflowConcurrency`: lowers every task's concurrency
    /// to the managed nodes' CPU count. A workaround for v7.5.x, whose
    /// hard-coded concurrency of 16 stalls a cluster with fewer CPUs
    /// (<https://github.com/pingcap/tidb/issues/50894>); later versions check
    /// the concurrency on submission, so this is normally a no-op.
    ///
    /// # Errors
    /// A statement failed.
    pub fn adjust_task_overflow_concurrency(&self, se: &dyn SessionContext) -> Result<()> {
        let cpu_count = Self::get_cpu_count_of_node_by_role_with_session(se, "", true)?;
        let args = [Arg::Int(cpu_count as i64), Arg::Int(cpu_count as i64)];
        exec_sql(
            &BackgroundContext,
            se,
            "update mysql.tidb_global_task set concurrency = %? where concurrency > %?;",
            &sql_args(&args),
        )?;
        Ok(())
    }

    /// Go `UpdateSubtaskCheckpoint`: stores the subtask's checkpoint as
    /// JSON.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn update_subtask_checkpoint<T: serde::Serialize + ?Sized>(&self, subtask_id: i64, checkpoint: &T) -> Result<()> {
        let checkpoint_json = tidb_stringutil::go_json_marshal(checkpoint);
        let args = [Arg::Str(checkpoint_json), Arg::Int(subtask_id)];
        self.execute_sql_with_new_session(
            "UPDATE mysql.tidb_background_subtask SET checkpoint = %? WHERE id = %?",
            &sql_args(&args),
        )?;
        Ok(())
    }

    /// Go `GetSubtaskCheckpoint`: the subtask's checkpoint JSON, empty when
    /// there is none.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_subtask_checkpoint(&self, subtask_id: i64) -> Result<String> {
        let rs = self.execute_sql_with_new_session(
            "SELECT checkpoint FROM mysql.tidb_background_subtask WHERE id = %?",
            &[SqlArg::Signed(subtask_id)],
        )?;
        match rs.first() {
            Some(row) if !is_null(row, 0) => Ok(get_string(row, 0)),
            _ => Ok(String::new()),
        }
    }

    /// Go `UpdateTaskExtraParams`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn update_task_extra_params(&self, task_id: i64, extra_params: &ExtraParams) -> Result<()> {
        let extra_param_bytes = tidb_stringutil::go_json_marshal(extra_params).into_bytes();
        self.with_new_txn(&mut |se| {
            let args = [Arg::Json(extra_param_bytes.clone()), Arg::Int(task_id)];
            exec_sql(
                &BackgroundContext,
                se,
                "
			update mysql.tidb_global_task
			set extra_params = %?
			where id = %?",
                &sql_args(&args),
            )?;
            Ok(())
        })
    }
}

/// Go's package variable `maxSubtaskBatchSize`, which tests lower.
static MAX_SUBTASK_BATCH_SIZE_VAR: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(MAX_SUBTASK_BATCH_SIZE);

fn max_subtask_batch_size() -> usize {
    MAX_SUBTASK_BATCH_SIZE_VAR.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tidb_dxf::{Subtask, SubtaskBase};
    use tidb_terror::{annotate, TerrorCode, TerrorError};

    use super::{serialize_err, MAX_SUBTASK_BATCH_SIZE_VAR};
    use crate::converter::TaskError;
    use crate::TaskManager;

    /// Go `TestSerializeErr`.
    #[test]
    fn test_serialize_err() {
        assert!(serialize_err(None).is_none());

        let normalized_err = TerrorError::normalize("named err", "NAMED", TerrorCode::new(0));
        let the_err = annotate(Arc::new(normalized_err.wrap(Arc::new(TaskError("inner err".to_owned())))), "annotated err");
        assert_eq!(
            String::from_utf8(serialize_err(Some(&the_err)).unwrap()).unwrap(),
            r#"{"class":0,"code":0,"message":"annotated err: named err: inner err","rfccode":"NAMED"}"#
        );

        let the_err = annotate(Arc::new(TaskError("some err".to_owned())), "annotated err");
        assert_eq!(
            String::from_utf8(serialize_err(Some(&the_err)).unwrap()).unwrap(),
            r#"{"class":0,"code":0,"message":"annotated err: some err","rfccode":""}"#
        );
    }

    /// Go `fmt.Errorf("wrapped: %w", err)`: no `messenger`, so Go's
    /// `GetErrStackMsg` takes its whole text.
    #[derive(Debug)]
    struct Wrapped(TerrorError);

    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "wrapped: {}", self.0)
        }
    }

    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    /// Bytes of Go's `serializeErr`, printed by an overlay test in Go's
    /// `storage` package.
    #[test]
    fn serialize_err_writes_gos_bytes() {
        let undetermined = tidb_terror::ERR_RESULT_UNDETERMINED.clone();
        let cases: [(Box<dyn std::error::Error>, &str); 5] = [
            (
                Box::new(TaskError("a<b && c>d".to_owned())),
                r#"{"class":0,"code":0,"message":"a\u003cb \u0026\u0026 c\u003ed","rfccode":""}"#,
            ),
            (
                Box::new(undetermined.clone()),
                r#"{"class":21,"code":2,"message":"execution result undetermined","rfccode":"global:2"}"#,
            ),
            (
                Box::new(annotate(Arc::new(undetermined.clone()), "ctx")),
                r#"{"class":21,"code":2,"message":"ctx: execution result undetermined","rfccode":"global:2"}"#,
            ),
            (
                Box::new(TerrorError::compatible(TerrorCode::new(8000), "m")),
                r#"{"class":0,"code":8000,"message":"m","rfccode":"8000"}"#,
            ),
            (
                Box::new(Wrapped(undetermined)),
                r#"{"class":21,"code":2,"message":"wrapped: [global:2]execution result undetermined","rfccode":"global:2"}"#,
            ),
        ];
        for (err, want) in cases {
            assert_eq!(String::from_utf8(serialize_err(Some(err.as_ref())).unwrap()).unwrap(), want);
        }
    }

    /// Go `TestSplitSubtasks`.
    #[test]
    fn test_split_subtasks() {
        let meta_bytes = vec![0_u8; 100];
        let subtasks: Vec<Subtask> = (0..10)
            .map(|i| Subtask {
                base: SubtaskBase { id: i, ..SubtaskBase::default() },
                meta: meta_bytes.clone(),
                ..Subtask::default()
            })
            .collect();
        let ids = |batch: &[Subtask]| batch.iter().map(|s| s.base.id).collect::<Vec<_>>();
        let bak = tidb_txnkv::txn_total_size_limit();

        // Go `config.SuperLargeTxnSize`, 100 TiB.
        tidb_txnkv::set_txn_total_size_limit(100 * 1024 * 1024 * 1024 * 1024);
        let split_subtasks = TaskManager::split_subtasks(&subtasks);
        assert_eq!(split_subtasks.len(), 1);
        assert_eq!(ids(split_subtasks[0]), ids(&subtasks));

        MAX_SUBTASK_BATCH_SIZE_VAR.store(300, std::sync::atomic::Ordering::Relaxed);
        let split_subtasks = TaskManager::split_subtasks(&subtasks);
        assert_eq!(split_subtasks.len(), 4);
        assert_eq!(ids(split_subtasks[0]), ids(&subtasks[..3]));
        assert_eq!(ids(split_subtasks[1]), ids(&subtasks[3..6]));
        assert_eq!(ids(split_subtasks[2]), ids(&subtasks[6..9]));
        assert_eq!(ids(split_subtasks[3]), ids(&subtasks[9..]));
        tidb_txnkv::set_txn_total_size_limit(bak);
    }
}
