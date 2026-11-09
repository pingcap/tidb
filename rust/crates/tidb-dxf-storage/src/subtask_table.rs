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


//! Go `task_table.go`'s subtask queries and updates.

use std::collections::HashMap;

use tidb_dxf::{
    Step, Subtask, SubtaskBase, SubtaskState, SUBTASK_STATE_CANCELED, SUBTASK_STATE_FAILED, SUBTASK_STATE_PENDING,
    SUBTASK_STATE_RUNNING,
};
use tidb_dxf_execute::SubtaskSummary;

use crate::converter::{get_bytes, get_int64, get_json_string, get_string, is_null, row2_basic_sub_task};
use crate::task_table::{sql_args, Arg};
use crate::{exec_sql, row2_sub_task, task_id_to_key, Result, StorageError, TaskManager, BASIC_SUBTASK_COLUMNS, SUBTASK_COLUMNS};

/// Go's subtask error: an `errors.Normalize` read back from its JSON.
pub type SubtaskError = std::sync::Arc<dyn std::error::Error + Send + Sync>;

/// Go `unmarshalSubtaskError`: `None` for a NULL or empty error.
fn unmarshal_subtask_error(err_bytes: &[u8], null: bool) -> Result<Option<SubtaskError>> {
    if null || err_bytes.is_empty() {
        return Ok(None);
    }
    let std_err: tidb_terror::TerrorError =
        serde_json::from_slice(err_bytes).map_err(|error| StorageError::Other(error.to_string()))?;
    Ok(Some(std::sync::Arc::new(std_err)))
}

fn subtask_state_args(states: &[SubtaskState]) -> Vec<Arg> {
    states.iter().map(|state| Arg::Str(state.to_string())).collect()
}

fn holders(n: usize) -> String {
    vec!["%?"; n].join(",")
}

impl TaskManager {
    /// Go `GetSubtasksByExecIDAndStepAndStates`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_subtasks_by_exec_id_and_step_and_states(
        &self, ctx: &tidb_gocontext::Context,
        exec_id: &str,
        task_id: i64,
        step: Step,
        states: &[SubtaskState],
    ) -> Result<Vec<Subtask>> {
        let mut args = vec![Arg::Str(exec_id.to_owned()), Arg::Str(task_id_to_key(task_id)), Arg::Int(step.0)];
        args.extend(subtask_state_args(states));
        let rows = self.execute_sql_with_new_session(ctx, 
            &format!(
                "select {SUBTASK_COLUMNS} from mysql.tidb_background_subtask
		where exec_id = %? and task_key = %? and step = %?
		and state in ({})",
                holders(states.len())
            ),
            &sql_args(&args),
        )?;
        Ok(rows.iter().map(|row| row2_sub_task(row)).collect())
    }

    /// Go `GetFirstSubtaskInStates`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_first_subtask_in_states(
        &self, ctx: &tidb_gocontext::Context,
        tidb_id: &str,
        task_id: i64,
        step: Step,
        states: &[SubtaskState],
    ) -> Result<Option<Subtask>> {
        let mut args = vec![Arg::Str(tidb_id.to_owned()), Arg::Str(task_id_to_key(task_id)), Arg::Int(step.0)];
        args.extend(subtask_state_args(states));
        let rows = self.execute_sql_with_new_session(ctx, 
            &format!(
                "select {SUBTASK_COLUMNS} from mysql.tidb_background_subtask
		where exec_id = %? and task_key = %? and step = %?
		and state in ({}) limit 1",
                holders(states.len())
            ),
            &sql_args(&args),
        )?;
        Ok(rows.first().map(|row| row2_sub_task(row)))
    }

    /// Go `GetActiveSubtasks`: the pending and running subtasks.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_active_subtasks(&self, ctx: &tidb_gocontext::Context, task_id: i64) -> Result<Vec<SubtaskBase>> {
        let args = [
            Arg::Str(task_id_to_key(task_id)),
            Arg::Str(SUBTASK_STATE_PENDING.to_string()),
            Arg::Str(SUBTASK_STATE_RUNNING.to_string()),
        ];
        let rows = self.execute_sql_with_new_session(ctx, 
            &format!(
                "
		select {BASIC_SUBTASK_COLUMNS} from mysql.tidb_background_subtask
		where task_key = %? and state in (%?, %?)"
            ),
            &sql_args(&args),
        )?;
        Ok(rows.iter().map(|row| row2_basic_sub_task(row)).collect())
    }

    /// Go `GetAllSubtasksByStepAndState`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_all_subtasks_by_step_and_state(
        &self, ctx: &tidb_gocontext::Context,
        task_id: i64,
        step: Step,
        state: &SubtaskState,
    ) -> Result<Vec<Subtask>> {
        let args = [Arg::Str(task_id_to_key(task_id)), Arg::Str(state.to_string()), Arg::Int(step.0)];
        let rows = self.execute_sql_with_new_session(ctx, 
            &format!(
                "select {SUBTASK_COLUMNS} from mysql.tidb_background_subtask
		where task_key = %? and state = %? and step = %?"
            ),
            &sql_args(&args),
        )?;
        Ok(rows.iter().map(|row| row2_sub_task(row)).collect())
    }

    /// Go `GetAllSubtaskSummaryByStep`: the summaries of a running step (the
    /// history table is not read).
    ///
    /// # Errors
    /// The session or the statement failed, or a summary is not Go's JSON.
    pub fn get_all_subtask_summary_by_step(&self, ctx: &tidb_gocontext::Context, task_id: i64, step: Step) -> Result<Vec<SubtaskSummary>> {
        let args = [Arg::Str(task_id_to_key(task_id)), Arg::Int(step.0)];
        let rows = self.execute_sql_with_new_session(ctx, 
            "select summary from mysql.tidb_background_subtask
		where task_key = %? and step = %?",
            &sql_args(&args),
        )?;
        rows.iter()
            .map(|row| {
                serde_json::from_str(&get_json_string(row, 0)).map_err(|error| StorageError::Other(error.to_string()))
            })
            .collect()
    }

    /// Go `GetSubtaskRowCount`: the rows a step's subtasks (finished ones
    /// included) processed.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_subtask_row_count(&self, ctx: &tidb_gocontext::Context, task_id: i64, step: Step) -> Result<i64> {
        let key = task_id_to_key(task_id);
        let args = [Arg::Str(key.clone()), Arg::Int(step.0), Arg::Str(key), Arg::Int(step.0)];
        let rows = self.execute_sql_with_new_session(ctx, 
            "select
    	cast(sum(json_extract(summary, '$.row_count')) as signed) as row_count
		from (
			select summary from mysql.tidb_background_subtask where task_key = %? and step = %?
			union all
			select summary from mysql.tidb_background_subtask_history where task_key = %? and step = %?
		) as combined",
            &sql_args(&args),
        )?;
        Ok(rows.first().map_or(0, |row| get_int64(row, 0)))
    }

    /// Go `UpdateSubtaskSummary`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn update_subtask_summary(&self, ctx: &tidb_gocontext::Context, subtask_id: i64, summary: &SubtaskSummary) -> Result<()> {
        let summary_bytes = serde_json::to_string(summary).map_err(|error| StorageError::Other(error.to_string()))?;
        let args = [Arg::Str(summary_bytes), Arg::Int(subtask_id)];
        self.execute_sql_with_new_session(ctx, 
            "update mysql.tidb_background_subtask set summary = %? where id = %?",
            &sql_args(&args),
        )
        .map(drop)
    }

    /// Go `GetSubtaskCntGroupByStates`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn get_subtask_cnt_group_by_states(&self, ctx: &tidb_gocontext::Context, task_id: i64, step: Step) -> Result<HashMap<SubtaskState, i64>> {
        let args = [Arg::Str(task_id_to_key(task_id)), Arg::Int(step.0)];
        let rows = self.execute_sql_with_new_session(ctx, 
            "
		select state, count(*)
		from mysql.tidb_background_subtask
		where task_key = %? and step = %?
		group by state",
            &sql_args(&args),
        )?;
        Ok(rows
            .iter()
            .map(|row| (SubtaskState::new(get_string(row, 0)), get_int64(row, 1)))
            .collect())
    }

    /// Go `GetSubtaskStateCntAndErrorsByStep`: the count of each state and
    /// the failed and canceled subtasks' errors, in one read.
    ///
    /// # Errors
    /// The session or the statement failed, or a stored error is not Go's
    /// JSON.
    pub fn get_subtask_state_cnt_and_errors_by_step(
        &self, ctx: &tidb_gocontext::Context,
        task_id: i64,
        step: Step,
    ) -> Result<(HashMap<SubtaskState, i64>, Vec<SubtaskError>)> {
        let args = [Arg::Str(task_id_to_key(task_id)), Arg::Int(step.0)];
        let rows = self.execute_sql_with_new_session(ctx, 
            "select state, error
			from mysql.tidb_background_subtask
			where task_key = %? and step = %?",
            &sql_args(&args),
        )?;
        let mut cnt_by_states: HashMap<SubtaskState, i64> = HashMap::with_capacity(rows.len());
        let mut errors = Vec::new();
        for row in &rows {
            let state = SubtaskState::new(get_string(row, 0));
            *cnt_by_states.entry(state.clone()).or_default() += 1;
            if state != SUBTASK_STATE_FAILED && state != SUBTASK_STATE_CANCELED {
                continue;
            }
            if let Some(error) = unmarshal_subtask_error(&get_bytes(row, 1), is_null(row, 1))? {
                errors.push(error);
            }
        }
        Ok((cnt_by_states, errors))
    }

    /// Go `GetSubtaskErrors`: the failed and canceled subtasks' errors.
    ///
    /// # Errors
    /// The session or the statement failed, or a stored error is not Go's
    /// JSON.
    pub fn get_subtask_errors(&self, ctx: &tidb_gocontext::Context, task_id: i64) -> Result<Vec<Option<SubtaskError>>> {
        let args = [
            Arg::Str(task_id_to_key(task_id)),
            Arg::Str(SUBTASK_STATE_FAILED.to_string()),
            Arg::Str(SUBTASK_STATE_CANCELED.to_string()),
        ];
        let rows = self.execute_sql_with_new_session(ctx, 
            "select error from mysql.tidb_background_subtask
             where task_key = %? AND state in (%?, %?)",
            &sql_args(&args),
        )?;
        rows.iter()
            .map(|row| unmarshal_subtask_error(&get_bytes(row, 0), is_null(row, 0)))
            .collect()
    }

    /// Go `UpdateSubtasksExecIDs`: moves each subtask to its new node while
    /// it stays in its state.
    ///
    /// # Errors
    /// The transaction or a statement failed.
    pub fn update_subtasks_exec_ids(&self, ctx: &tidb_gocontext::Context, subtasks: &[SubtaskBase]) -> Result<()> {
        if subtasks.is_empty() {
            return Ok(());
        }
        self.with_new_txn(ctx, &mut |se| {
            for subtask in subtasks {
                let args = [
                    Arg::Str(subtask.exec_id.clone()),
                    Arg::Int(subtask.id),
                    Arg::Str(subtask.state.to_string()),
                ];
                exec_sql(ctx,
                    se,
                    "
				update mysql.tidb_background_subtask
				set exec_id = %?
				where id = %? and state = %?",
                    &sql_args(&args),
                )?;
            }
            Ok(())
        })
    }
}
