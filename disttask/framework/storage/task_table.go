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
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// GlobalTaskManager is the manager of global task.
type GlobalTaskManager struct {
	ctx context.Context
	se  sessionctx.Context
	mu  sync.Mutex
}

var globalTaskManagerInstance atomic.Pointer[GlobalTaskManager]
var subTaskManagerInstance atomic.Pointer[SubTaskManager]

// NewGlobalTaskManager creates a new global task manager.
func NewGlobalTaskManager(ctx context.Context, se sessionctx.Context) *GlobalTaskManager {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	return &GlobalTaskManager{
		ctx: ctx,
		se:  se,
	}
}

// NewSubTaskManager creates a new sub task manager.
func NewSubTaskManager(ctx context.Context, se sessionctx.Context) *SubTaskManager {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	return &SubTaskManager{
		ctx: ctx,
		se:  se,
	}
}

// GetGlobalTaskManager gets the global task manager.
func GetGlobalTaskManager() (*GlobalTaskManager, error) {
	v := globalTaskManagerInstance.Load()
	if v == nil {
		return nil, errors.New("global task manager is not initialized")
	}
	return v, nil
}

// SetGlobalTaskManager sets the global task manager.
func SetGlobalTaskManager(is *GlobalTaskManager) {
	globalTaskManagerInstance.Store(is)
}

// GetSubTaskManager gets the sub task manager.
func GetSubTaskManager() (*SubTaskManager, error) {
	v := subTaskManagerInstance.Load()
	if v == nil {
		return nil, errors.New("subTask manager is not initialized")
	}
	return v, nil
}

// SetSubTaskManager sets the sub task manager.
func SetSubTaskManager(is *SubTaskManager) {
	subTaskManagerInstance.Store(is)
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
		Type:         r.GetString(1),
		DispatcherID: r.GetString(2),
		State:        r.GetString(3),
		Meta:         r.GetBytes(6),
		Concurrency:  uint64(r.GetInt64(7)),
		Step:         r.GetInt64(8),
	}
	// TODO: convert to local time.
	task.StartTime, _ = r.GetTime(4).GoTime(time.UTC)
	task.StateUpdateTime, _ = r.GetTime(5).GoTime(time.UTC)
	return task
}

// AddNewTask adds a new task to global task table.
func (stm *GlobalTaskManager) AddNewTask(tp string, concurrency int, meta []byte) (int64, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "insert into mysql.tidb_global_task(type, state, concurrency, meta, state_update_time) values (%?, %?, %?, %?, %?)", tp, proto.TaskStatePending, concurrency, meta, time.Now().UTC().String())
	if err != nil {
		return 0, err
	}

	rs, err := execSQL(stm.ctx, stm.se, "select @@last_insert_id")
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(rs[0].GetString(0), 10, 64)
}

// GetNewTask get a new task from global task table, it's used by dispatcher only.
func (stm *GlobalTaskManager) GetNewTask() (task *proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := execSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where state = %? limit 1", proto.TaskStatePending)
	if err != nil {
		return task, err
	}

	if len(rs) == 0 {
		return nil, nil
	}

	return row2GlobeTask(rs[0]), nil
}

// UpdateTask updates the global task.
func (stm *GlobalTaskManager) UpdateTask(task *proto.Task) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "update mysql.tidb_global_task set state = %?, dispatcher_id = %?, step = %?, state_update_time = %?, concurrency = %? where id = %?", task.State, task.DispatcherID, task.Step, task.StateUpdateTime.UTC().String(), task.Concurrency, task.ID)
	if err != nil {
		return err
	}

	return nil
}

// GetTasksInStates gets the tasks in the states.
func (stm *GlobalTaskManager) GetTasksInStates(states ...interface{}) (task []*proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	if len(states) == 0 {
		return task, nil
	}

	rs, err := execSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", states...)
	if err != nil {
		return task, err
	}

	for _, r := range rs {
		task = append(task, row2GlobeTask(r))
	}
	return task, nil
}

// GetTaskByID gets the task by the global task ID.
func (stm *GlobalTaskManager) GetTaskByID(taskID int64) (task *proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := execSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where id = %?", taskID)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	return row2GlobeTask(rs[0]), nil
}

// SubTaskManager is the manager of subtask.
type SubTaskManager struct {
	ctx context.Context
	se  sessionctx.Context
	mu  sync.Mutex
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

// AddNewTask adds a new task to subtask table.
func (stm *SubTaskManager) AddNewTask(globalTaskID int64, designatedTiDBID string, meta []byte, tp string, isRevert bool) error {
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
func (stm *SubTaskManager) GetSubtaskInStates(tidbID string, taskID int64, states ...interface{}) (*proto.Subtask, error) {
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
func (stm *SubTaskManager) GetSubtaskInStatesCnt(taskID int64, states ...interface{}) (int64, error) {
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
func (stm *SubTaskManager) HasSubtasksInStates(tidbID string, taskID int64, states ...interface{}) (bool, error) {
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
func (stm *SubTaskManager) UpdateSubtaskState(id int64, state string) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "update mysql.tidb_background_subtask set state = %? where id = %?", state, id)
	return err
}

// UpdateHeartbeat updates the heartbeat of the subtask.
func (stm *SubTaskManager) UpdateHeartbeat(instanceID string, taskID int64, heartbeat time.Time) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "update mysql.tidb_background_subtask set exec_expired = %? where exec_id = %? and task_key = %?", heartbeat.String(), instanceID, taskID)
	return err
}

// DeleteTasks deletes the subtask of the given global task ID.
func (stm *SubTaskManager) DeleteTasks(taskID int64) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := execSQL(stm.ctx, stm.se, "delete from mysql.tidb_background_subtask where task_key = %?", taskID)
	if err != nil {
		return err
	}

	return nil
}

// GetSchedulerIDs gets the scheduler IDs of the given global task ID.
func (stm *SubTaskManager) GetSchedulerIDs(taskID int64) ([]string, error) {
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
