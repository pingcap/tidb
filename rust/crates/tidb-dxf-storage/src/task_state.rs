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

//! Go `task_state.go`: the task state transitions.

use std::error::Error;

use tidb_dxf::{
    ModifyParam, Step, Task, TaskState, STEP_DONE, SUBTASK_STATE_FAILED, SUBTASK_STATE_PAUSED, SUBTASK_STATE_PENDING,
    SUBTASK_STATE_RUNNING, TASK_STATE_AWAITING_RESOLUTION, TASK_STATE_CANCELLING, TASK_STATE_FAILED,
    TASK_STATE_MODIFYING, TASK_STATE_PAUSED, TASK_STATE_PAUSING, TASK_STATE_PENDING, TASK_STATE_RESUMING,
    TASK_STATE_REVERTED, TASK_STATE_REVERTING, TASK_STATE_RUNNING, TASK_STATE_SUCCEED,
};

use crate::task_table::{serialize_err, sql_args, Arg};
use crate::{exec_sql, task_id_to_key, Result, SessionContext, StorageError, TaskManager};

/// Go `TaskCancelMessage`: identifies user cancellation, because a
/// cancelled task finishes in `TaskStateReverted` rather than a distinct
/// cancelled terminal state.
pub const TASK_CANCEL_MESSAGE: &str = "cancelled by user";

/// Go `IsCancelledErr`: whether the error's message contains
/// [`TASK_CANCEL_MESSAGE`]. The substring match keeps wrapped or annotated
/// cancellation errors recognizable.
#[must_use]
pub fn is_cancelled_err(err: Option<&(dyn Error + 'static)>) -> bool {
    err.is_some_and(|err| err.to_string().contains(TASK_CANCEL_MESSAGE))
}

fn state(state: &TaskState) -> Arg {
    Arg::Str(state.to_string())
}

impl TaskManager {
    /// Go `CancelTask`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn cancel_task(&self, ctx: &tidb_gocontext::Context, task_id: i64) -> Result<()> {
        let args = [
            state(&TASK_STATE_CANCELLING),
            Arg::Int(task_id),
            state(&TASK_STATE_PENDING),
            state(&TASK_STATE_RUNNING),
            state(&TASK_STATE_AWAITING_RESOLUTION),
        ];
        self.execute_sql_with_new_session(ctx, "update mysql.tidb_global_task
		 set state = %?,
			 state_update_time = CURRENT_TIMESTAMP()
		 where id = %? and state in (%?, %?, %?)", &sql_args(&args))?;
        Ok(())
    }

    /// Go `CancelTaskByKeySession`: cancels the task by key on `se`.
    ///
    /// # Errors
    /// The statement failed.
    pub fn cancel_task_by_key_session(ctx: &tidb_gocontext::Context, se: &dyn SessionContext, task_key: &str) -> Result<()> {
        let args = [
            state(&TASK_STATE_CANCELLING),
            Arg::Str(task_key.to_owned()),
            state(&TASK_STATE_PENDING),
            state(&TASK_STATE_RUNNING),
            state(&TASK_STATE_AWAITING_RESOLUTION),
        ];
        exec_sql(ctx, se, "update mysql.tidb_global_task
		 set state = %?,
			 state_update_time = CURRENT_TIMESTAMP()
		 where task_key = %? and state in (%?, %?, %?)", &sql_args(&args))?;
        Ok(())
    }

    /// Go `FailTask`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn fail_task(&self, ctx: &tidb_gocontext::Context, task_id: i64, current_state: &TaskState, task_err: Option<&(dyn Error + 'static)>) -> Result<()> {
        let args = [
            state(&TASK_STATE_FAILED),
            Arg::OptBytes(serialize_err(task_err)),
            Arg::Int(task_id),
            state(current_state),
        ];
        self.execute_sql_with_new_session(ctx, "update mysql.tidb_global_task
		 set state = %?,
			 error = %?,
			 state_update_time = CURRENT_TIMESTAMP(),
			 end_time = CURRENT_TIMESTAMP()
		 where id = %? and state = %?", &sql_args(&args))?;
        Ok(())
    }

    /// Go `RevertTask`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn revert_task(&self, ctx: &tidb_gocontext::Context, task_id: i64, task_state: &TaskState, task_err: Option<&(dyn Error + 'static)>) -> Result<()> {
        self.transit_task_state_on_err(ctx, task_id, task_state, &TASK_STATE_REVERTING, task_err)
    }

    /// Go `transitTaskStateOnErr`.
    fn transit_task_state_on_err(
        &self, ctx: &tidb_gocontext::Context,
        task_id: i64,
        curr_state: &TaskState,
        target_state: &TaskState,
        task_err: Option<&(dyn Error + 'static)>,
    ) -> Result<()> {
        let args = [state(target_state), Arg::OptBytes(serialize_err(task_err)), Arg::Int(task_id), state(curr_state)];
        self.execute_sql_with_new_session(ctx, "
		update mysql.tidb_global_task
		set state = %?,
			error = %?,
			state_update_time = CURRENT_TIMESTAMP()
		where id = %? and state = %?", &sql_args(&args))?;
        Ok(())
    }

    /// Go `PauseTaskOnError`: moves the task to pausing with the error, and
    /// its failed subtasks of the step to paused.
    ///
    /// # Errors
    /// The task changed meanwhile (`TaskChanged`), or the session or a
    /// statement failed.
    pub fn pause_task_on_error(
        &self, ctx: &tidb_gocontext::Context,
        task_id: i64,
        task_state: &TaskState,
        step: Step,
        task_err: Option<&(dyn Error + 'static)>,
    ) -> Result<()> {
        let err_bytes = serialize_err(task_err);
        self.with_new_txn(ctx, &mut |se| {
            let args =
                [state(&TASK_STATE_PAUSING), Arg::OptBytes(err_bytes.clone()), Arg::Int(task_id), state(task_state)];
            exec_sql(ctx, se, "
			update mysql.tidb_global_task
			set state = %?,
				error = %?,
				state_update_time = CURRENT_TIMESTAMP()
			where id = %? and state = %?", &sql_args(&args))?;
            if se.affected_rows() == 0 {
                return Err(StorageError::TaskChanged);
            }
            let args = [
                Arg::Str(SUBTASK_STATE_PAUSED.to_string()),
                Arg::Str(task_id_to_key(task_id)),
                Arg::Int(step.0),
                Arg::Str(SUBTASK_STATE_FAILED.to_string()),
            ];
            exec_sql(ctx, se, "
			update mysql.tidb_background_subtask
			set state = %?,
				state_update_time = unix_timestamp(),
				end_time = null
			where task_key = %? and step = %? and state = %?", &sql_args(&args))?;
            Ok(())
        })
    }

    /// Go `AwaitingResolveTask`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn awaiting_resolve_task(
        &self, ctx: &tidb_gocontext::Context,
        task_id: i64,
        task_state: &TaskState,
        task_err: Option<&(dyn Error + 'static)>,
    ) -> Result<()> {
        self.transit_task_state_on_err(ctx, task_id, task_state, &TASK_STATE_AWAITING_RESOLUTION, task_err)
    }

    /// Go `RevertedTask`.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn reverted_task(&self, ctx: &tidb_gocontext::Context, task_id: i64) -> Result<()> {
        let args = [state(&TASK_STATE_REVERTED), Arg::Int(task_id), state(&TASK_STATE_REVERTING)];
        self.execute_sql_with_new_session(ctx, "update mysql.tidb_global_task
		 set state = %?,
			 state_update_time = CURRENT_TIMESTAMP(),
			 end_time = CURRENT_TIMESTAMP()
		 where id = %? and state = %?", &sql_args(&args))?;
        Ok(())
    }

    /// Go `PauseTask`: whether a pending or running task was found and
    /// moved to pausing.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn pause_task(&self, ctx: &tidb_gocontext::Context, task_key: &str) -> Result<bool> {
        let mut found = false;
        self.with_new_session(&mut |se| {
            let args = [
                state(&TASK_STATE_PAUSING),
                Arg::Str(task_key.to_owned()),
                state(&TASK_STATE_PENDING),
                state(&TASK_STATE_RUNNING),
            ];
            exec_sql(ctx, se, "update mysql.tidb_global_task
			 set state = %?,
				 state_update_time = CURRENT_TIMESTAMP()
			 where task_key = %? and state in (%?, %?)", &sql_args(&args))?;
            if se.affected_rows() != 0 {
                found = true;
            }
            Ok(())
        })?;
        Ok(found)
    }

    /// Go `PausedTask`: from pausing to paused.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn paused_task(&self, ctx: &tidb_gocontext::Context, task_id: i64) -> Result<()> {
        let args = [state(&TASK_STATE_PAUSED), Arg::Int(task_id), state(&TASK_STATE_PAUSING)];
        self.execute_sql_with_new_session(ctx, "update mysql.tidb_global_task
		 set state = %?,
			 state_update_time = CURRENT_TIMESTAMP()
		 where id = %? and state = %?", &sql_args(&args))?;
        Ok(())
    }

    /// Go `ResumeTask`: whether a paused task was found and moved to
    /// resuming.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn resume_task(&self, ctx: &tidb_gocontext::Context, task_key: &str) -> Result<bool> {
        let mut found = false;
        self.with_new_session(&mut |se| {
            let args = [state(&TASK_STATE_RESUMING), Arg::Str(task_key.to_owned()), state(&TASK_STATE_PAUSED)];
            exec_sql(ctx, se, "update mysql.tidb_global_task
		     set state = %?,
			     error = null,
			     state_update_time = CURRENT_TIMESTAMP()
		     where task_key = %? and state = %?", &sql_args(&args))?;
            if se.affected_rows() != 0 {
                found = true;
            }
            Ok(())
        })?;
        Ok(found)
    }

    /// Go `ResumedTask`: from resuming to running.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn resumed_task(&self, ctx: &tidb_gocontext::Context, task_id: i64) -> Result<()> {
        let args = [state(&TASK_STATE_RUNNING), Arg::Int(task_id), state(&TASK_STATE_RESUMING)];
        self.execute_sql_with_new_session(ctx, "
		update mysql.tidb_global_task
		set state = %?,
			state_update_time = CURRENT_TIMESTAMP()
		where id = %? and state = %?", &sql_args(&args))?;
        Ok(())
    }

    /// Go `ModifyTaskByID`: moves the task to modifying with the
    /// modification.
    ///
    /// # Errors
    /// The previous state cannot move to modifying (`TaskStateNotAllow`),
    /// the task changed (`TaskChanged`), or the session or a statement
    /// failed.
    pub fn modify_task_by_id(&self, ctx: &tidb_gocontext::Context, task_id: i64, param: &ModifyParam) -> Result<()> {
        if !param.prev_state.can_move_to_modifying() {
            return Err(StorageError::TaskStateNotAllow);
        }
        let bytes = tidb_stringutil::go_json_marshal(param).into_bytes();
        self.with_new_txn(ctx, &mut |se| {
            let task = Self::get_task_base_by_id_with_session(ctx, se, task_id)?;
            if task.state != param.prev_state {
                return Err(StorageError::TaskChanged);
            }
            #[cfg(feature = "failpoints")]
            tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/dxf/framework/storage/beforeMoveToModifying", &mut ());
            let args = [state(&TASK_STATE_MODIFYING), Arg::Json(bytes.clone()), Arg::Int(task_id), state(&param.prev_state)];
            exec_sql(ctx, se, "
			update mysql.tidb_global_task
			set state = %?, modify_params = %?, state_update_time = CURRENT_TIMESTAMP()
			where id = %? and state = %?", &sql_args(&args))?;
            if se.affected_rows() == 0 {
                // the txn is pessimistic, it's possible that another txn has
                // changed the task state before this txn commits and there is no
                // write-conflict.
                return Err(StorageError::TaskChanged);
            }
            Ok(())
        })
    }

    /// Go `ModifiedTask`: applies the modified task and returns it to its
    /// previous state.
    ///
    /// # Errors
    /// The session or a statement failed.
    pub fn modified_task(&self, ctx: &tidb_gocontext::Context, task: &Task) -> Result<()> {
        let prev_state = &task.modify_param.prev_state;
        self.with_new_txn(ctx, &mut |se| {
            #[cfg(feature = "failpoints")]
            tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/dxf/framework/storage/beforeModifiedTask", &mut ());
            let args = [
                state(prev_state),
                Arg::Int(task.base.required_slots as i64),
                Arg::Int(task.base.max_node_count as i64),
                Arg::Bytes(task.meta.clone()),
                Arg::Int(task.base.id),
                state(&TASK_STATE_MODIFYING),
            ];
            exec_sql(ctx, se, "
			update mysql.tidb_global_task
			set state = %?,
			    concurrency = %?,
				max_node_count = %?,
				meta = %?,
			    modify_params = null,
				state_update_time = CURRENT_TIMESTAMP()
			where id = %? and state = %?", &sql_args(&args))?;
            if se.affected_rows() == 0 {
                // might be handled by other owner nodes, skip.
                return Ok(());
            }
            // subtask in final state are not changed.
            // subtask might have different concurrency later, see TaskExecInfo, we
            // need to handle it too, but ok for now.
            let args = [
                Arg::Int(task.base.required_slots as i64),
                Arg::Str(task_id_to_key(task.base.id)),
                Arg::Str(SUBTASK_STATE_PENDING.to_string()),
                Arg::Str(SUBTASK_STATE_RUNNING.to_string()),
                Arg::Str(SUBTASK_STATE_PAUSED.to_string()),
            ];
            exec_sql(ctx, se, "
			update mysql.tidb_background_subtask
			set concurrency = %?, state_update_time = unix_timestamp()
			where task_key = %? and state in (%?, %?, %?)", &sql_args(&args))?;
            Ok(())
        })
    }

    /// Go `SucceedTask`: from running to succeed.
    ///
    /// # Errors
    /// The session or the statement failed.
    pub fn succeed_task(&self, ctx: &tidb_gocontext::Context, task_id: i64) -> Result<()> {
        self.with_new_session(&mut |se| {
            let args = [state(&TASK_STATE_SUCCEED), Arg::Int(STEP_DONE.0), Arg::Int(task_id), state(&TASK_STATE_RUNNING)];
            exec_sql(ctx, se, "
			update mysql.tidb_global_task
			set state = %?,
			    step = %?,
			    state_update_time = CURRENT_TIMESTAMP(),
			    end_time = CURRENT_TIMESTAMP()
			where id = %? and state = %?", &sql_args(&args))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::is_cancelled_err;
    use crate::converter::TaskError;

    /// Go `TestIsCancelledErr`.
    #[test]
    fn test_is_cancelled_err() {
        let err = |message: &str| TaskError(message.to_owned());
        assert!(!is_cancelled_err(None));
        assert!(!is_cancelled_err(Some(&err("some err"))));
        // Go `context.Canceled`.
        assert!(!is_cancelled_err(Some(&err("context canceled"))));
        assert!(is_cancelled_err(Some(&err("cancelled by user"))));
    }
}
