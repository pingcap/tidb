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

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// StartSubtask updates the subtask state to running.
func (mgr *TaskManager) StartSubtask(ctx context.Context, subtaskID int64, execID string) error {
	err := mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		vars := se.GetSessionVars()
		_, err := sqlexec.ExecSQL(ctx,
			se.GetSQLExecutor(),
			`update mysql.tidb_background_subtask
			 set state = %?, start_time = unix_timestamp(), state_update_time = unix_timestamp()
			 where id = %? and exec_id = %?`,
			proto.SubtaskStateRunning,
			subtaskID,
			execID)
		if err != nil {
			return err
		}
		if vars.StmtCtx.AffectedRows() == 0 {
			return ErrSubtaskNotFound
		}
		return nil
	})
	return err
}

// FinishSubtask updates the subtask meta and mark state to succeed.
func (mgr *TaskManager) FinishSubtask(ctx context.Context, execID string, id int64, meta []byte) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx, `update mysql.tidb_background_subtask
		set meta = %?, state = %?, state_update_time = unix_timestamp(), end_time = CURRENT_TIMESTAMP()
		where id = %? and exec_id = %?`,
		meta, proto.SubtaskStateSucceed, id, execID)
	return err
}

// FailSubtask update the task's subtask state to failed and set the err.
func (mgr *TaskManager) FailSubtask(ctx context.Context, execID string, taskID int64, err error) error {
	if err == nil {
		return nil
	}
	_, err1 := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask
		set state = %?,
		error = %?,
		start_time = unix_timestamp(),
		state_update_time = unix_timestamp(),
		end_time = CURRENT_TIMESTAMP()
		where exec_id = %? and
		task_key = %? and
		state in (%?, %?)
		limit 1;`,
		proto.SubtaskStateFailed,
		serializeErr(err),
		execID,
		taskID,
		proto.SubtaskStatePending,
		proto.SubtaskStateRunning)
	return err1
}

// CancelSubtask update the task's subtasks' state to canceled.
func (mgr *TaskManager) CancelSubtask(ctx context.Context, execID string, taskID int64) error {
	_, err1 := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask
		set state = %?,
		start_time = unix_timestamp(),
		state_update_time = unix_timestamp(),
		end_time = CURRENT_TIMESTAMP()
		where exec_id = %? and
		task_key = %? and
		state in (%?, %?)
		limit 1;`,
		proto.SubtaskStateCanceled,
		execID,
		taskID,
		proto.SubtaskStatePending,
		proto.SubtaskStateRunning)
	return err1
}

// PauseSubtasks update all running/pending subtasks to pasued state.
func (mgr *TaskManager) PauseSubtasks(ctx context.Context, execID string, taskID int64) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask set state = "paused" where task_key = %? and state in ("running", "pending") and exec_id = %?`, taskID, execID)
	return err
}

// ResumeSubtasks update all paused subtasks to pending state.
func (mgr *TaskManager) ResumeSubtasks(ctx context.Context, taskID int64) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask set state = "pending", error = null where task_key = %? and state = "paused"`, taskID)
	return err
}

// RunningSubtasksBack2Pending implements the taskexecutor.TaskTable interface.
func (mgr *TaskManager) RunningSubtasksBack2Pending(ctx context.Context, subtasks []*proto.SubtaskBase) error {
	// skip the update process.
	if len(subtasks) == 0 {
		return nil
	}
	err := mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		for _, subtask := range subtasks {
			_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
				update mysql.tidb_background_subtask
				set state = %?, state_update_time = unix_timestamp()
				where id = %? and exec_id = %? and state = %?`,
				proto.SubtaskStatePending, subtask.ID, subtask.ExecID, proto.SubtaskStateRunning)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// UpdateSubtaskStateAndError updates the subtask state.
func (mgr *TaskManager) UpdateSubtaskStateAndError(
	ctx context.Context,
	execID string,
	id int64, state proto.SubtaskState, subTaskErr error) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx, `update mysql.tidb_background_subtask
		set state = %?, error = %?, state_update_time = unix_timestamp() where id = %? and exec_id = %?`,
		state, serializeErr(subTaskErr), id, execID)
	return err
}
