// Copyright 2022 PingCAP, Inc.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/util"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/distribute_framework/proto"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

type GlobalTaskManager struct {
	ctx context.Context
	se  sessionctx.Context
	mu  sync.Mutex
}

var globalTaskManagerInstance atomic.Value
var subTaskManagerInstance atomic.Value

func NewGlobalTaskManager(ctx context.Context, se sessionctx.Context) *GlobalTaskManager {
	ctx = util.WithInternalSourceType(ctx, "distribute_framework")
	return &GlobalTaskManager{
		ctx: ctx,
		se:  se,
	}
}

func NewSubTaskManager(ctx context.Context, se sessionctx.Context) *SubTaskManager {
	return &SubTaskManager{
		ctx: ctx,
		se:  se,
	}
}

func GetGlobalTaskManager() (*GlobalTaskManager, error) {
	v := globalTaskManagerInstance.Load()
	if v == nil {
		return nil, errors.New("global task manager is not initialized")
	}
	return v.(*GlobalTaskManager), nil
}

func SetGlobalTaskManager(is *GlobalTaskManager) {
	globalTaskManagerInstance.Store(is)
}

func GetSubTaskManager() (*SubTaskManager, error) {
	v := subTaskManagerInstance.Load()
	if v == nil {
		return nil, errors.New("subTask manager is not initialized")
	}
	return v.(*SubTaskManager), nil
}

func SetSubTaskManager(is *SubTaskManager) {
	subTaskManagerInstance.Store(is)
}

func ExecSQL(ctx context.Context, se sessionctx.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	//logutil.BgLogger().Info("exec sql", zap.String("sql", sql), zap.Reflect("args", args))
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

func (stm *GlobalTaskManager) AddNewTask(tp proto.TaskType, concurrency int, meta []byte) (proto.TaskID, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "insert into mysql.tidb_global_task(type, state, concurrency, meta) values (%?, %?, %?, %?)", string(tp), string(proto.TaskStatePending), concurrency, meta)
	if err != nil {
		return 0, err
	}

	rs, err := ExecSQL(stm.ctx, stm.se, "select @@last_insert_id")
	if err != nil {
		return 0, err
	}

	return proto.TaskID(rs[0].GetInt64(0)), nil
}

// GetNewTask get a new task from global task table, it's used by dispatcher only.
func (stm *GlobalTaskManager) GetNewTask() (task *proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := ExecSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where state = %? limit 1", string(proto.TaskStatePending))
	if err != nil {
		return task, err
	}

	if len(rs) == 0 {
		return nil, nil
	}

	task = &proto.Task{
		ID:           proto.TaskID(rs[0].GetInt64(0)),
		Type:         proto.TaskType(rs[0].GetString(1)),
		DispatcherID: rs[0].GetString(2),
		State:        proto.TaskState(rs[0].GetString(3)),
		Meta:         proto.UnSerializeGlobalTaskMeta(rs[0].GetBytes(5)),
		Concurrency:  uint64(rs[0].GetInt64(6)),
		Step:         proto.TaskStep(rs[0].GetInt64(7)),
	}
	task.StartTime, _ = rs[0].GetTime(4).GoTime(time.UTC)

	return task, nil
}

func (stm *GlobalTaskManager) UpdateTask(task *proto.Task) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "update mysql.tidb_global_task set state = %?, dispatcher_id = %?, start_time = %?, step = %? where id = %?", string(task.State), task.DispatcherID, task.StartTime.String(), int64(task.Step), uint64(task.ID))
	if err != nil {
		return err
	}

	return nil
}

func (stm *GlobalTaskManager) GetRunnableTask() (task *proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := ExecSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where state = %? limit 1", string(proto.TaskStatePending))
	if err != nil {
		return task, err
	}

	t := &proto.Task{
		ID:           proto.TaskID(rs[0].GetInt64(0)),
		Type:         proto.TaskType(rs[0].GetString(1)),
		DispatcherID: rs[0].GetString(2),
		State:        proto.TaskState(rs[0].GetString(3)),
		Meta:         proto.UnSerializeGlobalTaskMeta(rs[0].GetBytes(5)),
		Concurrency:  uint64(rs[0].GetInt64(6)),
		Step:         proto.TaskStep(rs[0].GetInt64(7)),
	}
	t.StartTime, _ = rs[0].GetTime(4).GoTime(time.UTC)

	return task, nil
}

func (stm *GlobalTaskManager) GetTasksInStates(states ...interface{}) (task []*proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	if len(states) == 0 {
		return task, nil
	}

	rs, err := ExecSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", states...)
	if err != nil {
		return task, err
	}

	for _, r := range rs {
		t := &proto.Task{
			ID:           proto.TaskID(r.GetInt64(0)),
			Type:         proto.TaskType(r.GetString(1)),
			DispatcherID: r.GetString(2),
			State:        proto.TaskState(r.GetString(3)),
			Meta:         proto.UnSerializeGlobalTaskMeta(rs[0].GetBytes(5)),
			Concurrency:  uint64(r.GetInt64(6)),
			Step:         proto.TaskStep(rs[0].GetInt64(7)),
		}
		t.StartTime, _ = r.GetTime(4).GoTime(time.UTC)
		task = append(task, t)
	}
	return task, nil
}

func (stm *GlobalTaskManager) GetTaskByID(taskID proto.TaskID) (task *proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := ExecSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where id = %?", uint64(taskID))
	if err != nil {
		return task, err
	}

	task = &proto.Task{
		ID:           proto.TaskID(rs[0].GetInt64(0)),
		Type:         proto.TaskType(rs[0].GetString(1)),
		DispatcherID: rs[0].GetString(2),
		State:        proto.TaskState(rs[0].GetString(3)),
		Meta:         proto.UnSerializeGlobalTaskMeta(rs[0].GetBytes(5)),
		Concurrency:  uint64(rs[0].GetInt64(6)),
		Step:         proto.TaskStep(rs[0].GetInt64(7)),
	}
	task.StartTime, _ = rs[0].GetTime(4).GoTime(time.UTC)

	return task, nil
}

type SubTaskManager struct {
	ctx context.Context
	se  sessionctx.Context
	mu  sync.Mutex
}

func (stm *SubTaskManager) AddNewTask(globalTaskID proto.TaskID, designatedTiDBID proto.InstanceID, meta []byte, tp proto.TaskType) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "insert into mysql.tidb_sub_task(task_id, designate_tidb_id, meta, state, type) values (%?, %?, %?, %?, %?)", uint64(globalTaskID), string(designatedTiDBID), meta, string(proto.TaskStatePending), string(tp))
	if err != nil {
		return err
	}

	return nil
}

func (stm *SubTaskManager) GetSubaskByTiDBID(InstanceID proto.InstanceID) (*proto.Subtask, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := ExecSQL(stm.ctx, stm.se, "select id, meta from mysql.tidb_sub_task where designate_tidb_id = %? limit 1", string(InstanceID))
	if err != nil {
		return nil, err
	}

	t := &proto.Subtask{
		ID:          proto.SubtaskID(rs[0].GetInt64(0)),
		Type:        proto.TaskType(rs[0].GetString(1)),
		TaskID:      proto.TaskID(rs[0].GetInt64(2)),
		SchedulerID: proto.InstanceID(rs[0].GetString(3)),
		State:       proto.TaskState(rs[0].GetString(4)),
		Meta:        proto.UnSerializeSubTaskMeta(rs[0].GetBytes(6)),
	}
	t.StartTime, _ = rs[0].GetTime(5).GoTime(time.UTC)

	return t, nil
}

func (stm *SubTaskManager) UpdateTask(subTaskID proto.SubtaskID, meta []byte) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	logutil.BgLogger().Info("update subtask meta")

	_, err := ExecSQL(stm.ctx, stm.se, "update mysql.tidb_sub_task set meta = %? where id = %?", meta, uint64(subTaskID))
	if err != nil {
		return err
	}

	return nil
}

func (stm *SubTaskManager) GetSubtasksInStates(InstanceID proto.InstanceID, taskID proto.TaskID, states ...interface{}) (subtasks []*proto.Subtask, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	args := []interface{}{string(InstanceID), uint64(taskID)}
	args = append(args, states...)
	rs, err := ExecSQL(stm.ctx, stm.se, "select * from mysql.tidb_sub_task where designate_tidb_id = %? and task_id = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return subtasks, err
	}

	for _, r := range rs {
		t := &proto.Subtask{
			ID:          proto.SubtaskID(r.GetInt64(0)),
			Type:        proto.TaskType(r.GetString(1)),
			TaskID:      proto.TaskID(r.GetInt64(2)),
			SchedulerID: proto.InstanceID(r.GetString(3)),
			State:       proto.TaskState(r.GetString(4)),
			Meta:        proto.UnSerializeSubTaskMeta(r.GetBytes(6)),
		}
		t.StartTime, _ = r.GetTime(5).GoTime(time.UTC)
		subtasks = append(subtasks, t)
	}

	return subtasks, nil
}

func (stm *SubTaskManager) HasSubtasksInStates(InstanceID proto.InstanceID, taskID proto.TaskID, states ...interface{}) (bool, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	args := []interface{}{string(InstanceID), uint64(taskID)}
	args = append(args, states...)
	rs, err := ExecSQL(stm.ctx, stm.se, "select 1 from mysql.tidb_sub_task where designate_tidb_id = %? and task_id = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?) limit 1", args...)
	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}

func (stm *SubTaskManager) UpdateSubtaskState(id proto.SubtaskID, state proto.TaskState) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "update mysql.tidb_sub_task set state = %? where id = %?", string(state), uint64(id))
	return err
}

func (stm *SubTaskManager) UpdateHeartbeat(TiDB proto.InstanceID, taskID proto.TaskID, heartbeat time.Time) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "update mysql.tidb_sub_task set heartbeat = %? where designate_tidb_id = %? and task_id = %?", heartbeat.String(), string(TiDB), uint64(taskID))
	return err
}

func (stm *SubTaskManager) DeleteTask(subTaskID int64) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "delete from mysql.tidb_sub_task where where id = %?", subTaskID)
	if err != nil {
		return err
	}

	return nil
}

func (stm *SubTaskManager) DeleteTasks(globelTaskID proto.TaskID) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "delete mysql.tidb_sub_task where global_task_id = %?", uint64(globelTaskID))
	if err != nil {
		return err
	}

	return nil
}

func (stm *SubTaskManager) CheckTaskState(globalTaskID proto.TaskID, state proto.TaskState, eq bool) (cnt int64, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	query := "select count(*) from mysql.tidb_sub_task where task_id = %? and state = %?"
	if !eq {
		query = "select count(*) from mysql.tidb_sub_task where task_id = %? and state != %?"
	}
	rs, err := ExecSQL(stm.ctx, stm.se, query, uint64(globalTaskID), string(state))
	if err != nil {
		return 0, err
	}

	return rs[0].GetInt64(0), nil
}

func (stm *SubTaskManager) IsFinishedTask(globalTaskID proto.TaskID) (isFinished bool, err error) {
	return isFinished, nil
}
