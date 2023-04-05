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
	"sync"
	"sync/atomic"
	"time"

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
	ctx context.Context
	se  sessionctx.Context
	mu  sync.Mutex
}

var taskManagerInstance atomic.Pointer[TaskManager]

// NewTaskManager creates a new task manager.
func NewTaskManager(ctx context.Context, se sessionctx.Context) *TaskManager {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	return &TaskManager{
		ctx: ctx,
		se:  se,
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
	}
	// TODO: convert to local time.
	task.StartTime, _ = r.GetTime(5).GoTime(time.UTC)
	task.StateUpdateTime, _ = r.GetTime(6).GoTime(time.UTC)
	return task
}

// AddNewGlobalTask adds a new task to global task table.
func (stm *TaskManager) AddNewGlobalTask(key, tp string, concurrency int, meta []byte) (int64, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "insert into mysql.tidb_global_task(task_key, type, state, concurrency, meta, state_update_time) values (%?, %?, %?, %?, %?, %?)", key, tp, proto.TaskStatePending, concurrency, meta, time.Now().UTC().String())
	if err != nil {
		return 0, err
	}

	rs, err := execSQL(stm.ctx, stm.se, "select @@last_insert_id")
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(rs[0].GetString(0), 10, 64)
}

// GetNewGlobalTask get a new task from global task table, it's used by dispatcher only.
func (stm *TaskManager) GetNewGlobalTask() (task *proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := execSQL(stm.ctx, stm.se, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step from mysql.tidb_global_task where state = %? limit 1", proto.TaskStatePending)
	if err != nil {
		return task, err
	}

	if len(rs) == 0 {
		return nil, nil
	}

	return row2GlobeTask(rs[0]), nil
}

// UpdateGlobalTask updates the global task.
func (stm *TaskManager) UpdateGlobalTask(task *proto.Task) error {
	failpoint.Inject("MockUpdateTaskErr", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("updateTaskErr"))
		}
	})
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "update mysql.tidb_global_task set state = %?, dispatcher_id = %?, step = %?, state_update_time = %?, concurrency = %? where id = %?",
		task.State, task.DispatcherID, task.Step, task.StateUpdateTime.UTC().String(), task.Concurrency, task.ID)
	if err != nil {
		return err
	}

	return nil
}

// GetGlobalTasksInStates gets the tasks in the states.
func (stm *TaskManager) GetGlobalTasksInStates(states ...interface{}) (task []*proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	if len(states) == 0 {
		return task, nil
	}

	rs, err := execSQL(stm.ctx, stm.se, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step from mysql.tidb_global_task where state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", states...)
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
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := execSQL(stm.ctx, stm.se, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step from mysql.tidb_global_task where id = %?", taskID)
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
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := execSQL(stm.ctx, stm.se, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step from mysql.tidb_global_task where task_key = %?", key)
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
		Type:        proto.Int2Type(int(r.GetInt64(4))),
		SchedulerID: r.GetString(5),
		State:       r.GetString(7),
		Meta:        r.GetBytes(11),
		StartTime:   r.GetUint64(9),
	}
	tid, err := strconv.Atoi(r.GetString(2))
	if err != nil {
		logutil.BgLogger().Warn("unexpected task ID", zap.String("task ID", r.GetString(2)))
	}
	task.TaskID = int64(tid)
	return task
}

// AddNewSubTask adds a new task to subtask table.
func (stm *TaskManager) AddNewSubTask(globalTaskID int64, designatedTiDBID string, meta []byte, tp string, isRevert bool) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	st := proto.TaskStatePending
	if isRevert {
		st = proto.TaskStateRevertPending
	}

	_, err := execSQL(stm.ctx, stm.se, "insert into mysql.tidb_background_subtask(task_key, exec_id, meta, state, type, checkpoint) values (%?, %?, %?, %?, %?, %?)", globalTaskID, designatedTiDBID, meta, st, proto.Type2Int(tp), []byte{})
	if err != nil {
		return err
	}

	return nil
}

// GetSubtaskInStates gets the subtask in the states.
func (stm *TaskManager) GetSubtaskInStates(tidbID string, taskID int64, states ...interface{}) (*proto.Subtask, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	args := []interface{}{tidbID, taskID}
	args = append(args, states...)
	rs, err := execSQL(stm.ctx, stm.se, "select * from mysql.tidb_background_subtask where exec_id = %? and task_key = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	return row2SubTask(rs[0]), nil
}

// GetSubtaskInStatesCnt gets the subtask count in the states.
func (stm *TaskManager) GetSubtaskInStatesCnt(taskID int64, states ...interface{}) (int64, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	args := []interface{}{taskID}
	args = append(args, states...)
	rs, err := execSQL(stm.ctx, stm.se, "select count(*) from mysql.tidb_background_subtask where task_key = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return 0, err
	}

	return rs[0].GetInt64(0), nil
}

// HasSubtasksInStates checks if there are subtasks in the states.
func (stm *TaskManager) HasSubtasksInStates(tidbID string, taskID int64, states ...interface{}) (bool, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	args := []interface{}{tidbID, taskID}
	args = append(args, states...)
	rs, err := execSQL(stm.ctx, stm.se, "select 1 from mysql.tidb_background_subtask where exec_id = %? and task_key = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?) limit 1", args...)
	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}

// UpdateSubtaskState updates the subtask state.
func (stm *TaskManager) UpdateSubtaskState(id int64, state string) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "update mysql.tidb_background_subtask set state = %? where id = %?", state, id)
	return err
}

// UpdateSubtaskHeartbeat updates the heartbeat of the subtask.
func (stm *TaskManager) UpdateSubtaskHeartbeat(instanceID string, taskID int64, heartbeat time.Time) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "update mysql.tidb_background_subtask set exec_expired = %? where exec_id = %? and task_key = %?", heartbeat.String(), instanceID, taskID)
	return err
}

// DeleteSubtasksByTaskID deletes the subtask of the given global task ID.
func (stm *TaskManager) DeleteSubtasksByTaskID(taskID int64) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "delete from mysql.tidb_background_subtask where task_key = %?", taskID)
	if err != nil {
		return err
	}

	return nil
}

// GetSchedulerIDsByTaskID gets the scheduler IDs of the given global task ID.
func (stm *TaskManager) GetSchedulerIDsByTaskID(taskID int64) ([]string, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := execSQL(stm.ctx, stm.se, "select distinct(exec_id) from mysql.tidb_background_subtask where task_key = %?", taskID)
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
