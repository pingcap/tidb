// Copyright 2024 PingCAP, Inc.
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

package storage

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// CancelTask cancels task.
func (mgr *TaskManager) CancelTask(ctx context.Context, taskID int64) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_global_task
		 set state = %?,
			 state_update_time = CURRENT_TIMESTAMP()
		 where id = %? and state in (%?, %?)`,
		proto.TaskStateCancelling, taskID, proto.TaskStatePending, proto.TaskStateRunning,
	)
	return err
}

// CancelTaskByKeySession cancels task by key using input session.
func (*TaskManager) CancelTaskByKeySession(ctx context.Context, se sessionctx.Context, taskKey string) error {
	_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
		`update mysql.tidb_global_task
		 set state = %?,
			 state_update_time = CURRENT_TIMESTAMP()
		 where task_key = %? and state in (%?, %?)`,
		proto.TaskStateCancelling, taskKey, proto.TaskStatePending, proto.TaskStateRunning)
	return err
}

// FailTask implements the scheduler.TaskManager interface.
func (mgr *TaskManager) FailTask(ctx context.Context, taskID int64, currentState proto.TaskState, taskErr error) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_global_task
		 set state = %?,
			 error = %?,
			 state_update_time = CURRENT_TIMESTAMP(),
			 end_time = CURRENT_TIMESTAMP()
		 where id = %? and state = %?`,
		proto.TaskStateFailed, serializeErr(taskErr), taskID, currentState,
	)
	return err
}

// RevertTask implements the scheduler.TaskManager interface.
func (mgr *TaskManager) RevertTask(ctx context.Context, taskID int64, taskState proto.TaskState, taskErr error) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx, `
		update mysql.tidb_global_task
		set state = %?,
			error = %?,
			state_update_time = CURRENT_TIMESTAMP()
		where id = %? and state = %?`,
		proto.TaskStateReverting, serializeErr(taskErr), taskID, taskState,
	)
	return err
}

// RevertedTask implements the scheduler.TaskManager interface.
func (mgr *TaskManager) RevertedTask(ctx context.Context, taskID int64) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_global_task
		 set state = %?,
			 state_update_time = CURRENT_TIMESTAMP(),
			 end_time = CURRENT_TIMESTAMP()
		 where id = %? and state = %?`,
		proto.TaskStateReverted, taskID, proto.TaskStateReverting,
	)
	return err
}

// PauseTask pauses the task.
func (mgr *TaskManager) PauseTask(ctx context.Context, taskKey string) (bool, error) {
	found := false
	err := mgr.WithNewSession(func(se sessionctx.Context) error {
		_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			`update mysql.tidb_global_task
			 set state = %?,
				 state_update_time = CURRENT_TIMESTAMP()
			 where task_key = %? and state in (%?, %?)`,
			proto.TaskStatePausing, taskKey, proto.TaskStatePending, proto.TaskStateRunning,
		)
		if err != nil {
			return err
		}
		if se.GetSessionVars().StmtCtx.AffectedRows() != 0 {
			found = true
		}
		return err
	})
	if err != nil {
		return found, err
	}
	return found, nil
}

// PausedTask update the task state from pausing to paused.
func (mgr *TaskManager) PausedTask(ctx context.Context, taskID int64) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_global_task
		 set state = %?,
			 state_update_time = CURRENT_TIMESTAMP()
		 where id = %? and state = %?`,
		proto.TaskStatePaused, taskID, proto.TaskStatePausing,
	)
	return err
}

// ResumeTask resumes the task.
func (mgr *TaskManager) ResumeTask(ctx context.Context, taskKey string) (bool, error) {
	found := false
	err := mgr.WithNewSession(func(se sessionctx.Context) error {
		_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			`update mysql.tidb_global_task
		     set state = %?,
			     state_update_time = CURRENT_TIMESTAMP()
		     where task_key = %? and state = %?`,
			proto.TaskStateResuming, taskKey, proto.TaskStatePaused,
		)
		if err != nil {
			return err
		}
		if se.GetSessionVars().StmtCtx.AffectedRows() != 0 {
			found = true
		}
		return err
	})
	if err != nil {
		return found, err
	}
	return found, nil
}

// ResumedTask implements the scheduler.TaskManager interface.
func (mgr *TaskManager) ResumedTask(ctx context.Context, taskID int64) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx, `
		update mysql.tidb_global_task
		set state = %?,
			state_update_time = CURRENT_TIMESTAMP()
		where id = %? and state = %?`,
		proto.TaskStateRunning, taskID, proto.TaskStateResuming,
	)
	return err
}

// ModifyTaskByID modifies the task by the task ID.
func (mgr *TaskManager) ModifyTaskByID(ctx context.Context, taskID int64, param *proto.ModifyParam) error {
	if !param.PrevState.CanMoveToModifying() {
		return ErrTaskStateNotAllow
	}
	bytes, err := json.Marshal(param)
	if err != nil {
		return errors.Trace(err)
	}
	return mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		task, err2 := mgr.getTaskBaseByID(ctx, se.GetSQLExecutor(), taskID)
		if err2 != nil {
			return err2
		}
		if task.State != param.PrevState {
			return ErrTaskChanged
		}
		failpoint.InjectCall("beforeMoveToModifying")
		_, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			update mysql.tidb_global_task
			set state = %?, modify_params = %?, state_update_time = CURRENT_TIMESTAMP()
			where id = %? and state = %?`,
			proto.TaskStateModifying, json.RawMessage(bytes), taskID, param.PrevState,
		)
		if err != nil {
			return err
		}
		if se.GetSessionVars().StmtCtx.AffectedRows() == 0 {
			// the txn is pessimistic, it's possible that another txn has
			// changed the task state before this txn commits and there is no
			// write-conflict.
			return ErrTaskChanged
		}
		return nil
	})
}

// ModifiedTask implements the scheduler.TaskManager interface.
func (mgr *TaskManager) ModifiedTask(ctx context.Context, task *proto.Task) error {
	prevState := task.ModifyParam.PrevState
	return mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		failpoint.InjectCall("beforeModifiedTask")
		_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			update mysql.tidb_global_task
			set state = %?,
			    concurrency = %?,
				meta = %?,
			    modify_params = null,
				state_update_time = CURRENT_TIMESTAMP()
			where id = %? and state = %?`,
			prevState, task.Concurrency, task.Meta, task.ID, proto.TaskStateModifying,
		)
		if err != nil {
			return err
		}
		if se.GetSessionVars().StmtCtx.AffectedRows() == 0 {
			// might be handled by other owner nodes, skip.
			return nil
		}
		// subtask in final state are not changed.
		// subtask might have different concurrency later, see TaskExecInfo, we
		// need to handle it too, but ok for now.
		_, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			update mysql.tidb_background_subtask
			set concurrency = %?, state_update_time = unix_timestamp()
			where task_key = %? and state in (%?, %?, %?)`,
			task.Concurrency, task.ID,
			proto.SubtaskStatePending, proto.SubtaskStateRunning, proto.SubtaskStatePaused)
		return err
	})
}

// SucceedTask update task state from running to succeed.
func (mgr *TaskManager) SucceedTask(ctx context.Context, taskID int64) error {
	return mgr.WithNewSession(func(se sessionctx.Context) error {
		_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			update mysql.tidb_global_task
			set state = %?,
			    step = %?,
			    state_update_time = CURRENT_TIMESTAMP(),
			    end_time = CURRENT_TIMESTAMP()
			where id = %? and state = %?`,
			proto.TaskStateSucceed, proto.StepDone, taskID, proto.TaskStateRunning,
		)
		return err
	})
}
