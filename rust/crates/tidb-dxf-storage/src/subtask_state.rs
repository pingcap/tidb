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

//! Go `subtask_state.go`: the subtask state transitions.

use std::error::Error;

use tidb_dxf::{
    SubtaskBase, SubtaskState, SUBTASK_STATE_CANCELED, SUBTASK_STATE_FAILED, SUBTASK_STATE_PENDING,
    SUBTASK_STATE_RUNNING, SUBTASK_STATE_SUCCEED,
};

use crate::task_table::{serialize_err, sql_args, Arg};
use crate::{exec_sql, task_id_to_key, Result, StorageError, TaskManager};

fn state(state: &SubtaskState) -> Arg {
    Arg::Str(state.to_string())
}

impl TaskManager {
    /// Go `StartSubtask`: the subtask is running.
    ///
    /// # Errors
    /// No such subtask on the executor (`SubtaskNotFound`), or the session
    /// or the statement failed.
    pub fn start_subtask(&self, ctx: &tidb_gocontext::Context, subtask_id: i64, exec_id: &str) -> Result<()> {
        self.with_new_txn(ctx, &mut |se| {
            let args = [state(&SUBTASK_STATE_RUNNING), Arg::Int(subtask_id), Arg::Str(exec_id.to_owned())];
            exec_sql(ctx, se, "update mysql.tidb_background_subtask
			 set state = %?, start_time = unix_timestamp(), state_update_time = unix_timestamp()
			 where id = %? and exec_id = %?", &sql_args(&args))?;
            if se.affected_rows() == 0 {
                return Err(StorageError::SubtaskNotFound);
            }
            Ok(())
        })
    }

    /// Go `FinishSubtask`: stores the subtask meta and marks it succeed.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn finish_subtask(&self, ctx: &tidb_gocontext::Context, exec_id: &str, id: i64, meta: &[u8]) -> Result<()> {
        let args = [
            Arg::Bytes(meta.to_vec()),
            state(&SUBTASK_STATE_SUCCEED),
            Arg::Int(id),
            Arg::Str(exec_id.to_owned()),
        ];
        self.execute_sql_with_new_session(ctx, "update mysql.tidb_background_subtask
		set meta = %?, state = %?, state_update_time = unix_timestamp(), end_time = CURRENT_TIMESTAMP()
		where id = %? and exec_id = %?", &sql_args(&args))?;
        Ok(())
    }

    /// Go `FailSubtask`: marks one of the task's pending or running subtasks
    /// on the executor failed with the error; nothing without an error.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn fail_subtask(&self, ctx: &tidb_gocontext::Context, exec_id: &str, task_id: i64, err: Option<&(dyn Error + 'static)>) -> Result<()> {
        if err.is_none() {
            return Ok(());
        }
        let args = [
            state(&SUBTASK_STATE_FAILED),
            Arg::OptBytes(serialize_err(err)),
            Arg::Str(exec_id.to_owned()),
            Arg::Str(task_id_to_key(task_id)),
            state(&SUBTASK_STATE_PENDING),
            state(&SUBTASK_STATE_RUNNING),
        ];
        self.execute_sql_with_new_session(ctx, "update mysql.tidb_background_subtask
		set state = %?,
		error = %?,
		start_time = unix_timestamp(),
		state_update_time = unix_timestamp(),
		end_time = CURRENT_TIMESTAMP()
		where exec_id = %? and
		task_key = %? and
		state in (%?, %?)
		limit 1;", &sql_args(&args))?;
        Ok(())
    }

    /// Go `CancelSubtask`: the task's pending and running subtasks on the
    /// executor are canceled.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn cancel_subtask(&self, ctx: &tidb_gocontext::Context, exec_id: &str, task_id: i64) -> Result<()> {
        let args = [
            state(&SUBTASK_STATE_CANCELED),
            Arg::Str(exec_id.to_owned()),
            Arg::Str(task_id_to_key(task_id)),
            state(&SUBTASK_STATE_PENDING),
            state(&SUBTASK_STATE_RUNNING),
        ];
        self.execute_sql_with_new_session(ctx, "update mysql.tidb_background_subtask
		set state = %?,
		start_time = unix_timestamp(),
		state_update_time = unix_timestamp(),
		end_time = CURRENT_TIMESTAMP()
		where exec_id = %? and
		task_key = %? and
		state in (%?, %?);", &sql_args(&args))?;
        Ok(())
    }

    /// Go `PauseSubtasks`: the task's running and pending subtasks on the
    /// executor are paused.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn pause_subtasks(&self, ctx: &tidb_gocontext::Context, exec_id: &str, task_id: i64) -> Result<()> {
        let args = [Arg::Str(task_id_to_key(task_id)), Arg::Str(exec_id.to_owned())];
        self.execute_sql_with_new_session(ctx, "update mysql.tidb_background_subtask set state = \"paused\" where task_key = %? and state in (\"running\", \"pending\") and exec_id = %?", &sql_args(&args))?;
        Ok(())
    }

    /// Go `ResumeSubtasks`: the task's paused subtasks are pending again.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn resume_subtasks(&self, ctx: &tidb_gocontext::Context, task_id: i64) -> Result<()> {
        let args = [Arg::Str(task_id_to_key(task_id))];
        self.execute_sql_with_new_session(ctx, "update mysql.tidb_background_subtask set state = \"pending\", error = null where task_key = %? and state = \"paused\"", &sql_args(&args))?;
        Ok(())
    }

    /// Go `RunningSubtasksBack2Pending`: the running subtasks are pending
    /// again, in one transaction.
    ///
    /// # Errors
    /// The session or a statement failed.
    pub fn running_subtasks_back2_pending(&self, ctx: &tidb_gocontext::Context, subtasks: &[SubtaskBase]) -> Result<()> {
        // skip the update process.
        if subtasks.is_empty() {
            return Ok(());
        }
        self.with_new_txn(ctx, &mut |se| {
            for subtask in subtasks {
                let args = [
                    state(&SUBTASK_STATE_PENDING),
                    Arg::Int(subtask.id),
                    Arg::Str(subtask.exec_id.clone()),
                    state(&SUBTASK_STATE_RUNNING),
                ];
                exec_sql(ctx, se, "
				update mysql.tidb_background_subtask
				set state = %?, state_update_time = unix_timestamp()
				where id = %? and exec_id = %? and state = %?", &sql_args(&args))?;
            }
            Ok(())
        })
    }

    /// Go `UpdateSubtaskStateAndError`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn update_subtask_state_and_error(
        &self, ctx: &tidb_gocontext::Context,
        exec_id: &str,
        id: i64,
        subtask_state: &SubtaskState,
        sub_task_err: Option<&(dyn Error + 'static)>,
    ) -> Result<()> {
        let args = [
            state(subtask_state),
            Arg::OptBytes(serialize_err(sub_task_err)),
            Arg::Int(id),
            Arg::Str(exec_id.to_owned()),
        ];
        self.execute_sql_with_new_session(ctx, "update mysql.tidb_background_subtask
		set state = %?, error = %?, state_update_time = unix_timestamp() where id = %? and exec_id = %?", &sql_args(&args))?;
        Ok(())
    }
}
