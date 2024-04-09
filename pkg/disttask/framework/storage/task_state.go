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
