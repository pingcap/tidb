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
	"github.com/pingcap/tidb/dist-task/proto"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
)

type GlobalTaskManager struct {
	ctx context.Context
	se  sessionctx.Context
	mu  sync.Mutex
}

var globalTaskManagerInstance atomic.Value
var subTaskManagerInstance atomic.Value

func NewGlobalTaskManager(ctx context.Context, se sessionctx.Context) *GlobalTaskManager {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	return &GlobalTaskManager{
		ctx: ctx,
		se:  se,
	}
}

func NewSubTaskManager(ctx context.Context, se sessionctx.Context) *SubTaskManager {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
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

func (stm *GlobalTaskManager) AddNewTask(tp string, concurrency int, meta []byte) (int64, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "insert into mysql.tidb_global_task(type, state, concurrency, meta) values (%?, %?, %?, %?)", tp, proto.TaskStatePending, concurrency, meta)
	if err != nil {
		return 0, err
	}

	rs, err := ExecSQL(stm.ctx, stm.se, "select @@last_insert_id")
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(rs[0].GetString(0), 10, 64)
}

// GetNewTask get a new task from global task table, it's used by dispatcher only.
func (stm *GlobalTaskManager) GetNewTask() (task *proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := ExecSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where state = %? limit 1", proto.TaskStatePending)
	if err != nil {
		return task, err
	}

	if len(rs) == 0 {
		return nil, nil
	}

	task = &proto.Task{
		ID:           rs[0].GetInt64(0),
		Type:         rs[0].GetString(1),
		DispatcherID: rs[0].GetString(2),
		State:        rs[0].GetString(3),
		Meta:         proto.UnSerializeGlobalTaskMeta(rs[0].GetBytes(5)),
		Concurrency:  uint64(rs[0].GetInt64(6)),
		Step:         rs[0].GetInt64(7),
	}
	task.StartTime, _ = rs[0].GetTime(4).GoTime(time.UTC)

	return task, nil
}

func (stm *GlobalTaskManager) UpdateTask(task *proto.Task) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "update mysql.tidb_global_task set state = %?, dispatcher_id = %?, step = %? where id = %?", task.State, task.DispatcherID, task.Step, task.ID)
	if err != nil {
		return err
	}

	return nil
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
			ID:           r.GetInt64(0),
			Type:         r.GetString(1),
			DispatcherID: r.GetString(2),
			State:        r.GetString(3),
			Meta:         proto.UnSerializeGlobalTaskMeta(rs[0].GetBytes(5)),
			Concurrency:  uint64(r.GetInt64(6)),
			Step:         r.GetInt64(7),
		}
		t.StartTime, _ = r.GetTime(4).GoTime(time.UTC)
		task = append(task, t)
	}
	return task, nil
}

func (stm *GlobalTaskManager) GetTaskByID(taskID int64) (task *proto.Task, err error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := ExecSQL(stm.ctx, stm.se, "select * from mysql.tidb_global_task where id = %?", taskID)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	task = &proto.Task{
		ID:           rs[0].GetInt64(0),
		Type:         rs[0].GetString(1),
		DispatcherID: rs[0].GetString(2),
		State:        rs[0].GetString(3),
		Meta:         proto.UnSerializeGlobalTaskMeta(rs[0].GetBytes(5)),
		Concurrency:  uint64(rs[0].GetInt64(6)),
		Step:         rs[0].GetInt64(7),
	}
	task.StartTime, _ = rs[0].GetTime(4).GoTime(time.UTC)

	return task, nil
}

type SubTaskManager struct {
	ctx context.Context
	se  sessionctx.Context
	mu  sync.Mutex
}

func (stm *SubTaskManager) AddNewTask(globalTaskID int64, designatedTiDBID string, meta []byte, tp string, isRevert bool) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	st := proto.TaskStatePending
	if isRevert {
		st = proto.TaskStateRevertPending
	}

	_, err := ExecSQL(stm.ctx, stm.se, "insert into mysql.tidb_sub_task(task_id, designate_tidb_id, meta, state, type) values (%?, %?, %?, %?, %?)", globalTaskID, designatedTiDBID, meta, st, tp)
	if err != nil {
		return err
	}

	return nil
}

func (stm *SubTaskManager) GetSubtaskInStates(InstanceID string, taskID int64, states ...interface{}) (*proto.Subtask, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	args := []interface{}{InstanceID, taskID}
	args = append(args, states...)
	rs, err := ExecSQL(stm.ctx, stm.se, "select * from mysql.tidb_sub_task where designate_tidb_id = %? and task_id = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	t := &proto.Subtask{
		ID:          rs[0].GetInt64(0),
		Type:        rs[0].GetString(1),
		TaskID:      rs[0].GetInt64(2),
		SchedulerID: rs[0].GetString(3),
		State:       rs[0].GetString(4),
		Meta:        proto.UnSerializeSubTaskMeta(rs[0].GetBytes(6)),
	}
	t.StartTime, _ = rs[0].GetTime(5).GoTime(time.UTC)

	return t, nil
}

func (stm *SubTaskManager) GetSubtaskInStatesCnt(taskID int64, states ...interface{}) (int64, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	args := []interface{}{taskID}
	args = append(args, states...)
	rs, err := ExecSQL(stm.ctx, stm.se, "select count(*) from mysql.tidb_sub_task where task_id = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return 0, err
	}

	return rs[0].GetInt64(0), nil
}

func (stm *SubTaskManager) HasSubtasksInStates(InstanceID string, taskID int64, states ...interface{}) (bool, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	args := []interface{}{InstanceID, taskID}
	args = append(args, states...)
	rs, err := ExecSQL(stm.ctx, stm.se, "select 1 from mysql.tidb_sub_task where designate_tidb_id = %? and task_id = %? and state in ("+strings.Repeat("%?,", len(states)-1)+"%?) limit 1", args...)
	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}

func (stm *SubTaskManager) UpdateSubtaskState(id int64, state string) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "update mysql.tidb_sub_task set state = %? where id = %?", state, id)
	return err
}

func (stm *SubTaskManager) UpdateHeartbeat(instanceID string, taskID int64, heartbeat time.Time) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "update mysql.tidb_sub_task set heartbeat = %? where designate_tidb_id = %? and task_id = %?", heartbeat.String(), instanceID, taskID)
	return err
}

func (stm *SubTaskManager) DeleteTasks(globalTaskID int64) error {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	_, err := ExecSQL(stm.ctx, stm.se, "delete mysql.tidb_sub_task where global_task_id = %?", globalTaskID)
	if err != nil {
		return err
	}

	return nil
}

func (stm *SubTaskManager) GetSchedulerIDs(taskID int64) ([]string, error) {
	stm.mu.Lock()
	defer stm.mu.Unlock()

	rs, err := ExecSQL(stm.ctx, stm.se, "select distinct(designate_tidb_id) from mysql.tidb_sub_task where task_id = %?", taskID)
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
