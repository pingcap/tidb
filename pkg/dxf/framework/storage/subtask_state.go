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
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// StartSubtask updates the subtask state to running.
func (mgr *TaskManager) StartSubtask(ctx context.Context, subtaskID int64, execID string) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	err := mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		vars := se.GetSessionVars()
		_, err := sqlexec.ExecSQL(ctx,
			se.GetSQLExecutor(),
			`update mysql.tidb_background_subtask
			 set state = %?, start_time = unix_timestamp(), state_update_time = unix_timestamp()
			 where id = %? and exec_id = %? and state = %?`,
			proto.SubtaskStateRunning,
			subtaskID,
			execID,
			proto.SubtaskStatePending)
		if err != nil {
			return err
		}
		if vars.StmtCtx.AffectedRows() == 0 {
			rows, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
				`select state from mysql.tidb_background_subtask where id = %? and exec_id = %?`,
				subtaskID, execID)
			if err != nil {
				return err
			}
			if len(rows) > 0 && rows[0].GetString(0) == string(proto.SubtaskStateRunning) {
				return nil
			}
			return ErrSubtaskNotFound
		}
		return nil
	})
	return err
}

// FinishSubtask updates the subtask meta and mark state to succeed.
func (mgr *TaskManager) FinishSubtask(ctx context.Context, execID string, id int64, meta []byte) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
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
		TaskIDToKey(taskID),
		proto.SubtaskStatePending,
		proto.SubtaskStateRunning)
	return err1
}

// CancelSubtask update the task's subtasks' state to canceled.
func (mgr *TaskManager) CancelSubtask(ctx context.Context, execID string, taskID int64) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	_, err1 := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask
		set state = %?,
		start_time = unix_timestamp(),
		state_update_time = unix_timestamp(),
		end_time = CURRENT_TIMESTAMP()
		where exec_id = %? and
		task_key = %? and
		state in (%?, %?);`,
		proto.SubtaskStateCanceled,
		execID,
		TaskIDToKey(taskID),
		proto.SubtaskStatePending,
		proto.SubtaskStateRunning)
	return err1
}

// PauseSubtasks update all running/pending subtasks to paused state.
func (mgr *TaskManager) PauseSubtasks(ctx context.Context, execID string, taskID int64) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	failpoint.InjectCall("beforePauseSubtasks")
	return mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		// Lock and recheck the parent task so PauseSubtasks serializes with
		// concurrent cancellation transitions and does not pause subtasks after cancel commits.
		rs, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			`select state from mysql.tidb_global_task where id = %? for update`,
			taskID,
		)
		if err != nil {
			return err
		}
		if len(rs) == 0 || rs[0].GetString(0) != string(proto.TaskStatePausing) {
			return nil
		}
		failpoint.InjectCall("beforePauseSubtasksUpdate")
		_, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			`update mysql.tidb_background_subtask
			 set state = %?, state_update_time = unix_timestamp()
			 where task_key = %? and state in (%?, %?) and exec_id = %?`,
			proto.SubtaskStatePaused, TaskIDToKey(taskID),
			proto.SubtaskStateRunning, proto.SubtaskStatePending, execID,
		)
		return err
	})
}

// CancelPausedSubtasks cancels all paused subtasks of taskID in bounded,
// retryable transactions so large subtask metadata does not trigger ErrTxnTooLarge.
func (mgr *TaskManager) CancelPausedSubtasks(ctx context.Context, taskID int64) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	taskKey := TaskIDToKey(taskID)
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		`select id, coalesce(length(meta), 0) + coalesce(length(checkpoint), 0) + coalesce(length(summary), 0) + coalesce(length(error), 0)
		 from mysql.tidb_background_subtask
		 where task_key = %? and state = %?
		 order by id`,
		taskKey, proto.SubtaskStatePaused,
	)
	if err != nil || len(rs) == 0 {
		return err
	}

	const pausedSubtaskRowOverhead = 1024
	// Keep each update transaction comfortably below TxnTotalSizeLimit because
	// UPDATE rewrites full row values and index entries in the transaction buffer.
	maxBatchBytes := max(min(kv.TxnTotalSizeLimit.Load(), uint64(maxSubtaskBatchSize))/2, uint64(1))
	ids := make([]int64, 0, len(rs))
	batches := make([][]int64, 0, 1)
	var batchStart int
	var currBytes uint64
	for _, r := range rs {
		id := r.GetInt64(0)
		size := uint64(r.GetInt64(1)) + pausedSubtaskRowOverhead
		if len(ids) > batchStart && currBytes+size > maxBatchBytes {
			batches = append(batches, ids[batchStart:len(ids)])
			batchStart = len(ids)
			currBytes = 0
		}
		ids = append(ids, id)
		currBytes += size
	}
	if len(ids) > batchStart {
		batches = append(batches, ids[batchStart:])
	}

	var cancelBatch func(ids []int64) error
	cancelBatch = func(ids []int64) error {
		if len(ids) == 0 {
			return nil
		}
		var injectedErr error
		failpoint.InjectCall("beforeCancelPausedSubtasksBatch", ids, &injectedErr)
		if injectedErr != nil {
			return injectedErr
		}
		err := mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
			var sb strings.Builder
			args := make([]any, 0, 2+len(ids))
			sb.WriteString(`update mysql.tidb_background_subtask
				set state = %?,
					state_update_time = unix_timestamp(),
					end_time = CURRENT_TIMESTAMP()
				where state = %? and id in (`)
			args = append(args, proto.SubtaskStateCanceled, proto.SubtaskStatePaused)
			for i, id := range ids {
				if i > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString("%?")
				args = append(args, id)
			}
			sb.WriteByte(')')
			_, execErr := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), sb.String(), args...)
			return execErr
		})
		if err != nil && kv.ErrTxnTooLarge.Equal(err) && len(ids) > 1 {
			mid := len(ids) / 2
			if err2 := cancelBatch(ids[:mid]); err2 != nil {
				return err2
			}
			return cancelBatch(ids[mid:])
		}
		return err
	}

	for _, batch := range batches {
		if err := cancelBatch(batch); err != nil {
			return err
		}
	}
	return nil
}

// ResumeSubtasks update all paused subtasks to pending state.
func (mgr *TaskManager) ResumeSubtasks(ctx context.Context, taskID int64) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	_, err := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask set state = "pending", error = null where task_key = %? and state = "paused"`, TaskIDToKey(taskID))
	return err
}

// RunningSubtasksBack2Pending implements the taskexecutor.TaskTable interface.
func (mgr *TaskManager) RunningSubtasksBack2Pending(ctx context.Context, subtasks []*proto.SubtaskBase) error {
	// skip the update process.
	if len(subtasks) == 0 {
		return nil
	}
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
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
