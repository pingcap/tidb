// Copyright 2023 PingCAP, Inc.
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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// TaskManager is the manager of global/sub task.
type TaskManager struct {
	ctx    context.Context
	sePool *pools.ResourcePool
}

var taskManagerInstance atomic.Pointer[TaskManager]

var (
	// TestLastTaskID is used for test to set the last task ID.
	TestLastTaskID atomic.Int64
)

// NewTaskManager creates a new task manager.
func NewTaskManager(ctx context.Context, sePool *pools.ResourcePool) *TaskManager {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	return &TaskManager{
		ctx:    ctx,
		sePool: sePool,
	}
}

// GetTaskManager gets the task manager.
func GetTaskManager() (*TaskManager, error) {
	v := taskManagerInstance.Load()
	if v == nil {
		return nil, errors.New("global task manager is not initialized")
	}
	return v, nil
}

// SetTaskManager sets the task manager.
func SetTaskManager(is *TaskManager) {
	taskManagerInstance.Store(is)
}

// execSQL executes the sql and returns the result.
// TODO: consider retry.
func execSQL(ctx context.Context, se sessionctx.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	rs, err := se.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	if rs != nil {
		rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
		if err != nil {
			return nil, err
		}
		err = rs.Close()
		if err != nil {
			return nil, err
		}
		return rows, err
	}
	return nil, nil
}

// row2GlobeTask converts a row to a global task.
func row2GlobeTask(r chunk.Row) *proto.Task {
	task := &proto.Task{
		ID:           r.GetInt64(0),
		Key:          r.GetString(1),
		Type:         r.GetString(2),
		DispatcherID: r.GetString(3),
		State:        r.GetString(4),
		Meta:         r.GetBytes(7),
		Concurrency:  uint64(r.GetInt64(8)),
		Step:         r.GetInt64(9),
		Error:        r.GetBytes(10),
	}
	// TODO: convert to local time.
	task.StartTime, _ = r.GetTime(5).GoTime(time.UTC)
	task.StateUpdateTime, _ = r.GetTime(6).GoTime(time.UTC)
	return task
}

func (stm *TaskManager) withNewSession(fn func(se sessionctx.Context) error) error {
	se, err := stm.sePool.Get()
	if err != nil {
		return err
	}
	defer stm.sePool.Put(se)
	return fn(se.(sessionctx.Context))
}

func (stm *TaskManager) withNewTxn(fn func(se sessionctx.Context) error) error {
	return stm.withNewSession(func(se sessionctx.Context) (err error) {
		_, err = execSQL(stm.ctx, se, "begin")
		if err != nil {
			return err
		}

		success := false
		defer func() {
			sql := "rollback"
			if success {
				sql = "commit"
			}
			_, commitErr := execSQL(stm.ctx, se, sql)
			if err == nil && commitErr != nil {
				err = commitErr
			}
		}()

		if err = fn(se); err != nil {
			return err
		}

		success = true
		return nil
	})
}

func (stm *TaskManager) executeSQLWithNewSession(ctx context.Context, sql string, args ...interface{}) (rs []chunk.Row, err error) {
	err = stm.withNewSession(func(se sessionctx.Context) error {
		rs, err = execSQL(ctx, se, sql, args...)
		return err
	})

	if err != nil {
		return nil, err
	}

	return
}

// AddNewGlobalTask adds a new task to global task table.
func (stm *TaskManager) AddNewGlobalTask(key, tp string, concurrency int, meta []byte) (taskID int64, err error) {
	err = stm.withNewSession(func(se sessionctx.Context) error {
		_, err = execSQL(stm.ctx, se, "insert into mysql.tidb_global_task(task_key, type, state, concurrency, meta, state_update_time) values (%?, %?, %?, %?, %?, %?)", key, tp, proto.TaskStatePending, concurrency, meta, time.Now().UTC().String())
		if err != nil {
			return err
		}

		rs, err := execSQL(stm.ctx, se, "select @@last_insert_id")
		if err != nil {
			return err
		}

		taskID, err = strconv.ParseInt(rs[0].GetString(0), 10, 64)
		if err != nil {
			return err
		}
		failpoint.Inject("testSetLastTaskID", func() { TestLastTaskID.Store(taskID) })

		return nil
	})

	if err != nil {
		return 0, err
	}
	return
}

// GetNewGlobalTask get a new task from global task table, it's used by dispatcher only.
func (stm *TaskManager) GetNewGlobalTask() (task *proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step, error from mysql.tidb_global_task where state = %? limit 1", proto.TaskStatePending)
	if err != nil {
		return task, err
	}

	if len(rs) == 0 {
		return nil, nil
	}

	return row2GlobeTask(rs[0]), nil
}

// GetGlobalTasksInStates gets the tasks in the states.
func (stm *TaskManager) GetGlobalTasksInStates(states ...interface{}) (task []*proto.Task, err error) {
	if len(states) == 0 {
		return task, nil
	}

	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step, error from mysql.tidb_global_task where state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", states...)
	if err != nil {
		return task, err
	}

	for _, r := range rs {
		task = append(task, row2GlobeTask(r))
	}
	return task, nil
}

// GetGlobalTaskByID gets the task by the global task ID.
func (stm *TaskManager) GetGlobalTaskByID(taskID int64) (task *proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step, error from mysql.tidb_global_task where id = %?", taskID)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	return row2GlobeTask(rs[0]), nil
}

// GetGlobalTaskByKey gets the task by the task key
func (stm *TaskManager) GetGlobalTaskByKey(key string) (task *proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step, error from mysql.tidb_global_task where task_key = %?", key)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	return row2GlobeTask(rs[0]), nil
}

// row2SubTask converts a row to a subtask.
func row2SubTask(r chunk.Row) *proto.Subtask {
	task := &proto.Subtask{
		ID:          r.GetInt64(0),
		Step:        r.GetInt64(1),
		Type:        proto.Int2Type(int(r.GetInt64(5))),
		SchedulerID: r.GetString(6),
		State:       r.GetString(8),
		Meta:        r.GetBytes(12),
		StartTime:   r.GetUint64(10),
	}
	tid, err := strconv.Atoi(r.GetString(3))
	if err != nil {
		logutil.BgLogger().Warn("unexpected task ID", zap.String("task ID", r.GetString(3)))
	}
	task.TaskID = int64(tid)
	return task
}

// AddNewSubTask adds a new task to subtask table.
func (stm *TaskManager) AddNewSubTask(globalTaskID int64, step int64, designatedTiDBID string, meta []byte, tp string, isRevert bool) error {
	st := proto.TaskStatePending
	if isRevert {
		st = proto.TaskStateRevertPending
	}

	_, err := stm.executeSQLWithNewSession(stm.ctx, "insert into mysql.tidb_background_subtask(task_key, step, exec_id, meta, state, type, checkpoint) values (%?, %?, %?, %?, %?, %?, %?)", globalTaskID, step, designatedTiDBID, meta, st, proto.Type2Int(tp), []byte{})
	if err != nil {
		return err
	}

	return nil
}

// GetSubtaskInStates gets the subtask in the states.
func (stm *TaskManager) GetSubtaskInStates(tidbID string, taskID int64, states ...interface{}) (*proto.Subtask, error) {
	args := []interface{}{tidbID, taskID}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select * from mysql.tidb_background_subtask where exec_id = %? and task_key = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	return row2SubTask(rs[0]), nil
}

// GetSucceedSubtasksByStep gets the subtask in the success state.
func (stm *TaskManager) GetSucceedSubtasksByStep(taskID int64, step int64) ([]*proto.Subtask, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select * from mysql.tidb_background_subtask where task_key = %? and state = %? and step = %?", taskID, proto.TaskStateSucceed, step)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}
	subtasks := make([]*proto.Subtask, 0, len(rs))
	for _, r := range rs {
		subtasks = append(subtasks, row2SubTask(r))
	}
	return subtasks, nil
}

// GetSubtaskInStatesCnt gets the subtask count in the states.
func (stm *TaskManager) GetSubtaskInStatesCnt(taskID int64, states ...interface{}) (int64, error) {
	args := []interface{}{taskID}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select count(*) from mysql.tidb_background_subtask where task_key = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return 0, err
	}

	return rs[0].GetInt64(0), nil
}

// CollectSubTaskError collects the subtask error.
func (stm *TaskManager) CollectSubTaskError(taskID int64) ([][]byte, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select error from mysql.tidb_background_subtask where task_key = %? AND state = %?", taskID, proto.TaskStateFailed)
	if err != nil {
		return nil, err
	}

	subTaskErrors := make([][]byte, 0, len(rs))
	for _, err := range rs {
		subTaskErrors = append(subTaskErrors, err.GetBytes(0))
	}

	return subTaskErrors, nil
}

// HasSubtasksInStates checks if there are subtasks in the states.
func (stm *TaskManager) HasSubtasksInStates(tidbID string, taskID int64, states ...interface{}) (bool, error) {
	args := []interface{}{tidbID, taskID}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select 1 from mysql.tidb_background_subtask where exec_id = %? and task_key = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?) limit 1", args...)
	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}

// UpdateSubtaskStateAndError updates the subtask state.
func (stm *TaskManager) UpdateSubtaskStateAndError(id int64, state string, subTaskErr string) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, "update mysql.tidb_background_subtask set state = %?, error = %? where id = %?", state, subTaskErr, id)
	return err
}

// FinishSubtask updates the subtask meta and mark state to succeed.
func (stm *TaskManager) FinishSubtask(id int64, meta []byte) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, "update mysql.tidb_background_subtask set meta = %?, state = %? where id = %?", meta, proto.TaskStateSucceed, id)
	return err
}

// UpdateSubtaskHeartbeat updates the heartbeat of the subtask.
func (stm *TaskManager) UpdateSubtaskHeartbeat(instanceID string, taskID int64, heartbeat time.Time) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, "update mysql.tidb_background_subtask set exec_expired = %? where exec_id = %? and task_key = %?", heartbeat.String(), instanceID, taskID)
	return err
}

// DeleteSubtasksByTaskID deletes the subtask of the given global task ID.
func (stm *TaskManager) DeleteSubtasksByTaskID(taskID int64) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, "delete from mysql.tidb_background_subtask where task_key = %?", taskID)
	if err != nil {
		return err
	}

	return nil
}

// GetSchedulerIDsByTaskID gets the scheduler IDs of the given global task ID.
func (stm *TaskManager) GetSchedulerIDsByTaskID(taskID int64) ([]string, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select distinct(exec_id) from mysql.tidb_background_subtask where task_key = %?", taskID)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	instanceIDs := make([]string, 0, len(rs))
	for _, r := range rs {
		id := r.GetString(0)
		instanceIDs = append(instanceIDs, id)
	}

	return instanceIDs, nil
}

// UpdateGlobalTaskAndAddSubTasks update the global task and add new subtasks
func (stm *TaskManager) UpdateGlobalTaskAndAddSubTasks(gTask *proto.Task, subtasks []*proto.Subtask, isSubtaskRevert bool) error {
	return stm.withNewTxn(func(se sessionctx.Context) error {
		_, err := execSQL(stm.ctx, se, "update mysql.tidb_global_task set state = %?, dispatcher_id = %?, step = %?, state_update_time = %?, concurrency = %?, meta = %?, error = %? where id = %?",
			gTask.State, gTask.DispatcherID, gTask.Step, gTask.StateUpdateTime.UTC().String(), gTask.Concurrency, gTask.Meta, gTask.Error, gTask.ID)
		if err != nil {
			return err
		}

		failpoint.Inject("MockUpdateTaskErr", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.New("updateTaskErr"))
			}
		})

		subtaskState := proto.TaskStatePending
		if isSubtaskRevert {
			subtaskState = proto.TaskStateRevertPending
		}

		for _, subtask := range subtasks {
			// TODO: insert subtasks in batch
			_, err = execSQL(stm.ctx, se, "insert into mysql.tidb_background_subtask(step, task_key, exec_id, meta, state, type, checkpoint) values (%?, %?, %?, %?, %?, %?, %?)",
				gTask.Step, gTask.ID, subtask.SchedulerID, subtask.Meta, subtaskState, proto.Type2Int(subtask.Type), []byte{})
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// CancelGlobalTask cancels global task
func (stm *TaskManager) CancelGlobalTask(taskID int64) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, "update mysql.tidb_global_task set state=%? where id=%? and state in (%?, %?)",
		proto.TaskStateCancelling, taskID, proto.TaskStatePending, proto.TaskStateRunning,
	)
	return err
}

// IsGlobalTaskCancelling checks whether the task state is cancelling
func (stm *TaskManager) IsGlobalTaskCancelling(taskID int64) (bool, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select 1 from mysql.tidb_global_task where id=%? and state = %?",
		taskID, proto.TaskStateCancelling,
	)

	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}
