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
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const defaultSubtaskKeepDays = 14

// SessionExecutor defines the interface for executing SQLs in a session.
type SessionExecutor interface {
	// WithNewSession executes the function with a new session.
	WithNewSession(fn func(se sessionctx.Context) error) error
	// WithNewTxn executes the fn in a new transaction.
	WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error
}

// TaskManager is the manager of global/sub task.
type TaskManager struct {
	ctx    context.Context
	sePool sessionPool
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(resource pools.Resource)
}

var _ SessionExecutor = &TaskManager{}

var taskManagerInstance atomic.Pointer[TaskManager]

var (
	// TestLastTaskID is used for test to set the last task ID.
	TestLastTaskID atomic.Int64
)

// NewTaskManager creates a new task manager.
func NewTaskManager(ctx context.Context, sePool sessionPool) *TaskManager {
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

// ExecSQL executes the sql and returns the result.
// TODO: consider retry.
func ExecSQL(ctx context.Context, se sessionctx.Context, sql string, args ...interface{}) ([]chunk.Row, error) {
	rs, err := se.(sqlexec.SQLExecutor).ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	if rs != nil {
		defer terror.Call(rs.Close)
		return sqlexec.DrainRecordSet(ctx, rs, 1024)
	}
	return nil, nil
}

// row2GlobeTask converts a row to a global task.
func row2GlobeTask(r chunk.Row) *proto.Task {
	task := &proto.Task{
		ID:           r.GetInt64(0),
		Key:          r.GetString(1),
		Type:         proto.TaskType(r.GetString(2)),
		DispatcherID: r.GetString(3),
		State:        proto.TaskState(r.GetString(4)),
		Meta:         r.GetBytes(7),
		Concurrency:  uint64(r.GetInt64(8)),
		Step:         proto.Step(r.GetInt64(9)),
	}
	if !r.IsNull(10) {
		errBytes := r.GetBytes(10)
		stdErr := errors.Normalize("")
		err := stdErr.UnmarshalJSON(errBytes)
		if err != nil {
			logutil.BgLogger().Error("unmarshal error", zap.Error(err))
			task.Error = err
		} else {
			task.Error = stdErr
		}
	}
	// TODO: convert to local time.
	task.StartTime, _ = r.GetTime(5).GoTime(time.UTC)
	task.StateUpdateTime, _ = r.GetTime(6).GoTime(time.UTC)
	return task
}

// WithNewSession executes the function with a new session.
func (stm *TaskManager) WithNewSession(fn func(se sessionctx.Context) error) error {
	se, err := stm.sePool.Get()
	if err != nil {
		return err
	}
	defer stm.sePool.Put(se)
	return fn(se.(sessionctx.Context))
}

// WithNewTxn executes the fn in a new transaction.
func (stm *TaskManager) WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	return stm.WithNewSession(func(se sessionctx.Context) (err error) {
		_, err = ExecSQL(ctx, se, "begin")
		if err != nil {
			return err
		}

		success := false
		defer func() {
			sql := "rollback"
			if success {
				sql = "commit"
			}
			_, commitErr := ExecSQL(ctx, se, sql)
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
	err = stm.WithNewSession(func(se sessionctx.Context) error {
		rs, err = ExecSQL(ctx, se, sql, args...)
		return err
	})

	if err != nil {
		return nil, err
	}

	return
}

// AddNewGlobalTask adds a new task to global task table.
func (stm *TaskManager) AddNewGlobalTask(key string, tp proto.TaskType, concurrency int, meta []byte) (taskID int64, err error) {
	err = stm.WithNewSession(func(se sessionctx.Context) error {
		var err2 error
		taskID, err2 = stm.AddGlobalTaskWithSession(se, key, tp, concurrency, meta)
		return err2
	})
	return
}

// AddGlobalTaskWithSession adds a new task to global task table with session.
func (stm *TaskManager) AddGlobalTaskWithSession(se sessionctx.Context, key string, tp proto.TaskType, concurrency int, meta []byte) (taskID int64, err error) {
	_, err = ExecSQL(stm.ctx, se,
		`insert into mysql.tidb_global_task(task_key, type, state, concurrency, step, meta, start_time, state_update_time)
		values (%?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`,
		key, tp, proto.TaskStatePending, concurrency, proto.StepInit, meta)
	if err != nil {
		return 0, err
	}

	rs, err := ExecSQL(stm.ctx, se, "select @@last_insert_id")
	if err != nil {
		return 0, err
	}

	taskID = int64(rs[0].GetUint64(0))
	failpoint.Inject("testSetLastTaskID", func() { TestLastTaskID.Store(taskID) })

	return taskID, nil
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

// GetGlobalTasksFromHistoryInStates gets the tasks in history table in the states.
func (stm *TaskManager) GetGlobalTasksFromHistoryInStates(states ...interface{}) (task []*proto.Task, err error) {
	if len(states) == 0 {
		return task, nil
	}

	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step, error from mysql.tidb_global_task_history where state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", states...)
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

// GetTaskByIDWithHistory gets the task by the global task ID from both tidb_global_task and tidb_global_task_history.
func (stm *TaskManager) GetTaskByIDWithHistory(taskID int64) (task *proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step, error from mysql.tidb_global_task where id = %? "+
		"union select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step, error from mysql.tidb_global_task_history where id = %?", taskID, taskID)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	return row2GlobeTask(rs[0]), nil
}

// GetGlobalTaskByKey gets the task by the task key.
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

// GetGlobalTaskByKeyWithHistory gets the task from history table by the task key.
func (stm *TaskManager) GetGlobalTaskByKeyWithHistory(key string) (task *proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step, error from mysql.tidb_global_task where task_key = %?"+
		"union select id, task_key, type, dispatcher_id, state, start_time, state_update_time, meta, concurrency, step, error from mysql.tidb_global_task_history where task_key = %?", key, key)
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
	// subtask defines start/update time as bigint, to ensure backward compatible,
	// we keep it that way, and we convert it here.
	var startTime, updateTime time.Time
	if !r.IsNull(10) {
		ts := r.GetInt64(10)
		startTime = time.Unix(ts, 0)
	}
	if !r.IsNull(11) {
		ts := r.GetInt64(11)
		updateTime = time.Unix(ts, 0)
	}
	task := &proto.Subtask{
		ID:          r.GetInt64(0),
		Step:        proto.Step(r.GetInt64(1)),
		Type:        proto.Int2Type(int(r.GetInt64(5))),
		SchedulerID: r.GetString(6),
		State:       proto.TaskState(r.GetString(8)),
		Meta:        r.GetBytes(12),
		Summary:     r.GetString(14),
		StartTime:   startTime,
		UpdateTime:  updateTime,
	}
	tid, err := strconv.Atoi(r.GetString(3))
	if err != nil {
		logutil.BgLogger().Warn("unexpected task ID", zap.String("task ID", r.GetString(3)))
	}
	task.TaskID = int64(tid)
	return task
}

// AddNewSubTask adds a new task to subtask table.
func (stm *TaskManager) AddNewSubTask(globalTaskID int64, step proto.Step, designatedTiDBID string, meta []byte, tp proto.TaskType, isRevert bool) error {
	st := proto.TaskStatePending
	if isRevert {
		st = proto.TaskStateRevertPending
	}

	_, err := stm.executeSQLWithNewSession(stm.ctx, `insert into mysql.tidb_background_subtask
		(task_key, step, exec_id, meta, state, type, checkpoint, summary)
		values (%?, %?, %?, %?, %?, %?, %?, %?)`,
		globalTaskID, step, designatedTiDBID, meta, st, proto.Type2Int(tp), []byte{}, "{}")
	if err != nil {
		return err
	}

	return nil
}

// GetSubtasksInStates gets all subtasks by given states.
func (stm *TaskManager) GetSubtasksInStates(tidbID string, taskID int64, step proto.Step, states ...interface{}) ([]*proto.Subtask, error) {
	args := []interface{}{tidbID, taskID, step}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(stm.ctx, `select * from mysql.tidb_background_subtask
		where exec_id = %? and task_key = %? and step = %?
		and state in (`+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return nil, err
	}

	subtasks := make([]*proto.Subtask, len(rs))
	for i, row := range rs {
		subtasks[i] = row2SubTask(row)
	}
	return subtasks, nil
}

// GetFirstSubtaskInStates gets the first subtask by given states.
func (stm *TaskManager) GetFirstSubtaskInStates(tidbID string, taskID int64, step proto.Step, states ...interface{}) (*proto.Subtask, error) {
	args := []interface{}{tidbID, taskID, step}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(stm.ctx, `select * from mysql.tidb_background_subtask
		where exec_id = %? and task_key = %? and step = %?
		and state in (`+strings.Repeat("%?,", len(states)-1)+"%?) limit 1", args...)
	if err != nil {
		return nil, err
	}

	if len(rs) == 0 {
		return nil, nil
	}
	return row2SubTask(rs[0]), nil
}

// UpdateErrorToSubtask updates the error to subtask.
func (stm *TaskManager) UpdateErrorToSubtask(tidbID string, taskID int64, err error) error {
	if err == nil {
		return nil
	}
	_, err1 := stm.executeSQLWithNewSession(stm.ctx, `update mysql.tidb_background_subtask
		set state = %?, error = %?, start_time = unix_timestamp(), state_update_time = unix_timestamp()
		where exec_id = %? and task_key = %? and state in (%?, %?) limit 1;`,
		proto.TaskStateFailed, serializeErr(err), tidbID, taskID, proto.TaskStatePending, proto.TaskStateRunning)
	return err1
}

// PrintSubtaskInfo log the subtask info by taskKey. Only used for UT.
func (stm *TaskManager) PrintSubtaskInfo(taskID int64) {
	rs, _ := stm.executeSQLWithNewSession(stm.ctx,
		"select * from mysql.tidb_background_subtask_history where task_key = %?", taskID)
	rs2, _ := stm.executeSQLWithNewSession(stm.ctx,
		"select * from mysql.tidb_background_subtask where task_key = %?", taskID)
	rs = append(rs, rs2...)

	for _, r := range rs {
		errBytes := r.GetBytes(13)
		var err error
		if len(errBytes) > 0 {
			stdErr := errors.Normalize("")
			err1 := stdErr.UnmarshalJSON(errBytes)
			if err1 != nil {
				err = err1
			} else {
				err = stdErr
			}
		}
		logutil.BgLogger().Info(fmt.Sprintf("subTask: %v\n", row2SubTask(r)), zap.Error(err))
	}
}

// GetSucceedSubtasksByStep gets the subtask in the success state.
func (stm *TaskManager) GetSucceedSubtasksByStep(taskID int64, step proto.Step) ([]*proto.Subtask, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, `select * from mysql.tidb_background_subtask
		where task_key = %? and state = %? and step = %?`,
		taskID, proto.TaskStateSucceed, step)
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

// GetSubtaskRowCount gets the subtask row count.
func (stm *TaskManager) GetSubtaskRowCount(taskID int64, step proto.Step) (int64, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, `select
    	cast(sum(json_extract(summary, '$.row_count')) as signed) as row_count
		from mysql.tidb_background_subtask where task_key = %? and step = %?`,
		taskID, step)
	if err != nil {
		return 0, err
	}
	if len(rs) == 0 {
		return 0, nil
	}
	return rs[0].GetInt64(0), nil
}

// UpdateSubtaskRowCount updates the subtask row count.
func (stm *TaskManager) UpdateSubtaskRowCount(subtaskID int64, rowCount int64) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, `update mysql.tidb_background_subtask
		set summary = json_set(summary, '$.row_count', %?) where id = %?`,
		rowCount, subtaskID)
	return err
}

// GetSubtaskInStatesCnt gets the subtask count in the states.
func (stm *TaskManager) GetSubtaskInStatesCnt(taskID int64, states ...interface{}) (int64, error) {
	args := []interface{}{taskID}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(stm.ctx, `select count(*) from mysql.tidb_background_subtask
		where task_key = %? and state in (`+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return 0, err
	}

	return rs[0].GetInt64(0), nil
}

// CollectSubTaskError collects the subtask error.
func (stm *TaskManager) CollectSubTaskError(taskID int64) ([]error, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx,
		`select error from mysql.tidb_background_subtask
             where task_key = %? AND state in (%?, %?)`, taskID, proto.TaskStateFailed, proto.TaskStateCanceled)
	if err != nil {
		return nil, err
	}
	subTaskErrors := make([]error, 0, len(rs))
	for _, row := range rs {
		if row.IsNull(0) {
			subTaskErrors = append(subTaskErrors, nil)
			continue
		}
		errBytes := row.GetBytes(0)
		if len(errBytes) == 0 {
			subTaskErrors = append(subTaskErrors, nil)
			continue
		}
		stdErr := errors.Normalize("")
		err := stdErr.UnmarshalJSON(errBytes)
		if err != nil {
			return nil, err
		}
		subTaskErrors = append(subTaskErrors, stdErr)
	}

	return subTaskErrors, nil
}

// HasSubtasksInStates checks if there are subtasks in the states.
func (stm *TaskManager) HasSubtasksInStates(tidbID string, taskID int64, step proto.Step, states ...interface{}) (bool, error) {
	args := []interface{}{tidbID, taskID, step}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(stm.ctx, `select 1 from mysql.tidb_background_subtask
		where exec_id = %? and task_key = %? and step = %?
			and state in (`+strings.Repeat("%?,", len(states)-1)+"%?) limit 1", args...)
	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}

// StartSubtask updates the subtask state to running.
func (stm *TaskManager) StartSubtask(subtaskID int64) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, `update mysql.tidb_background_subtask
		set state = %?, start_time = unix_timestamp(), state_update_time = unix_timestamp()
		where id = %?`,
		proto.TaskStateRunning, subtaskID)
	return err
}

// StartManager insert the manager information into dist_framework_meta.
func (stm *TaskManager) StartManager(tidbID string, role string) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, `insert into mysql.dist_framework_meta values(%?, %?, DEFAULT)
        on duplicate key update role = %?`, tidbID, role, role)
	return err
}

// UpdateSubtaskStateAndError updates the subtask state.
func (stm *TaskManager) UpdateSubtaskStateAndError(id int64, state proto.TaskState, subTaskErr error) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, `update mysql.tidb_background_subtask
		set state = %?, error = %?, state_update_time = unix_timestamp() where id = %?`,
		state, serializeErr(subTaskErr), id)
	return err
}

// FinishSubtask updates the subtask meta and mark state to succeed.
func (stm *TaskManager) FinishSubtask(id int64, meta []byte) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, `update mysql.tidb_background_subtask
		set meta = %?, state = %?, state_update_time = unix_timestamp() where id = %?`,
		meta, proto.TaskStateSucceed, id)
	return err
}

// DeleteSubtasksByTaskID deletes the subtask of the given global task ID.
func (stm *TaskManager) DeleteSubtasksByTaskID(taskID int64) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx, `delete from mysql.tidb_background_subtask
		where task_key = %?`, taskID)
	if err != nil {
		return err
	}

	return nil
}

// GetSchedulerIDsByTaskID gets the scheduler IDs of the given global task ID.
func (stm *TaskManager) GetSchedulerIDsByTaskID(taskID int64) ([]string, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, `select distinct(exec_id) from mysql.tidb_background_subtask
		where task_key = %?`, taskID)
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

// GetSchedulerIDsByTaskIDAndStep gets the scheduler IDs of the given global task ID and step.
func (stm *TaskManager) GetSchedulerIDsByTaskIDAndStep(taskID int64, step proto.Step) ([]string, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, `select distinct(exec_id) from mysql.tidb_background_subtask
		where task_key = %? and step = %?`, taskID, step)
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

// IsSchedulerCanceled checks if subtask 'execID' of task 'taskID' has been canceled somehow.
func (stm *TaskManager) IsSchedulerCanceled(execID string, taskID int64) (bool, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select 1 from mysql.tidb_background_subtask where task_key = %? and exec_id = %?", taskID, execID)
	if err != nil {
		return false, err
	}
	return len(rs) == 0, nil
}

// UpdateFailedSchedulerIDs replace failed scheduler nodes with alive nodes.
func (stm *TaskManager) UpdateFailedSchedulerIDs(taskID int64, replaceNodes map[string]string) error {
	// skip
	if len(replaceNodes) == 0 {
		return nil
	}

	sql := new(strings.Builder)
	if err := sqlexec.FormatSQL(sql, "update mysql.tidb_background_subtask set state = %? ,exec_id = (case ", proto.TaskStatePending); err != nil {
		return err
	}
	for k, v := range replaceNodes {
		if err := sqlexec.FormatSQL(sql, "when exec_id = %? then %? ", k, v); err != nil {
			return err
		}
	}
	if err := sqlexec.FormatSQL(sql, " end) where task_key = %? and state != \"succeed\" and exec_id in (", taskID); err != nil {
		return err
	}
	i := 0
	for k := range replaceNodes {
		if i != 0 {
			if err := sqlexec.FormatSQL(sql, ","); err != nil {
				return err
			}
		}
		if err := sqlexec.FormatSQL(sql, "%?", k); err != nil {
			return err
		}
		i++
	}
	if err := sqlexec.FormatSQL(sql, ")"); err != nil {
		return err
	}

	_, err := stm.executeSQLWithNewSession(stm.ctx, sql.String())
	return err
}

// PauseSubtasks update all running/pending subtasks to pasued state.
func (stm *TaskManager) PauseSubtasks(tidbID string, taskID int64) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx,
		`update mysql.tidb_background_subtask set state = "paused" where task_key = %? and state in ("running", "pending") and exec_id = %?`, taskID, tidbID)
	return err
}

// ResumeSubtasks update all paused subtasks to pending state.
func (stm *TaskManager) ResumeSubtasks(taskID int64) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx,
		`update mysql.tidb_background_subtask set state = "pending", error = null where task_key = %? and state = "paused"`, taskID)
	return err
}

// UpdateGlobalTaskAndAddSubTasks update the global task and add new subtasks
func (stm *TaskManager) UpdateGlobalTaskAndAddSubTasks(gTask *proto.Task, subtasks []*proto.Subtask, prevState proto.TaskState) (bool, error) {
	retryable := true
	err := stm.WithNewTxn(stm.ctx, func(se sessionctx.Context) error {
		_, err := ExecSQL(stm.ctx, se, "update mysql.tidb_global_task "+
			"set state = %?, dispatcher_id = %?, step = %?, concurrency = %?, meta = %?, error = %?, state_update_time = CURRENT_TIMESTAMP()"+
			"where id = %? and state = %?",
			gTask.State, gTask.DispatcherID, gTask.Step, gTask.Concurrency, gTask.Meta, serializeErr(gTask.Error), gTask.ID, prevState)
		if err != nil {
			return err
		}
		// When AffectedRows == 0, means other admin command have changed the task state, it's illegal to dispatch subtasks.
		if se.GetSessionVars().StmtCtx.AffectedRows() == 0 {
			if !intest.InTest {
				// task state have changed by other admin command
				retryable = false
				return errors.New("invalid task state transform, state already changed")
			}
			// TODO: remove it, when OnNextSubtasksBatch returns subtasks, just insert subtasks without updating tidb_global_task.
			// Currently the business running on distributed task framework will update proto.Task in OnNextSubtasksBatch.
			// So when dispatching subtasks, framework needs to update global task and insert subtasks in one Txn.
			//
			// In future, it's needed to restrict changes of task in OnNextSubtasksBatch.
			// If OnNextSubtasksBatch won't update any fields in proto.Task, we can insert subtasks only.
			//
			// For now, we update nothing in proto.Task in UT's OnNextSubtasksBatch, so the AffectedRows will be 0. So UT can't fully compatible
			// with current UpdateGlobalTaskAndAddSubTasks implementation.
			rs, err := ExecSQL(stm.ctx, se, "select id from mysql.tidb_global_task where id = %? and state = %?", gTask.ID, prevState)
			if err != nil {
				return err
			}
			// state have changed.
			if len(rs) == 0 {
				retryable = false
				return errors.New("invalid task state transform, state already changed")
			}
		}

		failpoint.Inject("MockUpdateTaskErr", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.New("updateTaskErr"))
			}
		})
		if len(subtasks) > 0 {
			subtaskState := proto.TaskStatePending
			if gTask.State == proto.TaskStateReverting {
				subtaskState = proto.TaskStateRevertPending
			}

			sql := new(strings.Builder)
			if err := sqlexec.FormatSQL(sql, "insert into mysql.tidb_background_subtask \n"+
				"(step, task_key, exec_id, meta, state, type, checkpoint, summary) values "); err != nil {
				return err
			}
			for i, subtask := range subtasks {
				if i != 0 {
					if err := sqlexec.FormatSQL(sql, ","); err != nil {
						return err
					}
				}
				if err := sqlexec.FormatSQL(sql, "(%?, %?, %?, %?, %?, %?, %?, %?)",
					subtask.Step, gTask.ID, subtask.SchedulerID, subtask.Meta, subtaskState, proto.Type2Int(subtask.Type), []byte{}, "{}"); err != nil {
					return err
				}
			}
			_, err := ExecSQL(stm.ctx, se, sql.String())
			if err != nil {
				return nil
			}
		}
		return nil
	})

	return retryable, err
}

func serializeErr(err error) []byte {
	if err == nil {
		return nil
	}
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*errors.Error)
	if !ok {
		tErr = errors.Normalize(originErr.Error())
	}
	errBytes, err := tErr.MarshalJSON()
	if err != nil {
		return nil
	}
	return errBytes
}

// CancelGlobalTask cancels global task.
func (stm *TaskManager) CancelGlobalTask(taskID int64) error {
	_, err := stm.executeSQLWithNewSession(stm.ctx,
		"update mysql.tidb_global_task set state=%?, state_update_time = CURRENT_TIMESTAMP() "+
			"where id=%? and state in (%?, %?)",
		proto.TaskStateCancelling, taskID, proto.TaskStatePending, proto.TaskStateRunning,
	)
	return err
}

// CancelGlobalTaskByKeySession cancels global task by key using input session.
func (stm *TaskManager) CancelGlobalTaskByKeySession(se sessionctx.Context, taskKey string) error {
	_, err := ExecSQL(stm.ctx, se,
		"update mysql.tidb_global_task set state=%?, state_update_time = CURRENT_TIMESTAMP() "+
			"where task_key=%? and state in (%?, %?)",
		proto.TaskStateCancelling, taskKey, proto.TaskStatePending, proto.TaskStateRunning)
	return err
}

// IsGlobalTaskCancelling checks whether the task state is cancelling.
func (stm *TaskManager) IsGlobalTaskCancelling(taskID int64) (bool, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx, "select 1 from mysql.tidb_global_task where id=%? and state = %?",
		taskID, proto.TaskStateCancelling,
	)

	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}

// PauseTask pauses the task.
func (stm *TaskManager) PauseTask(taskKey string) (bool, error) {
	found := false
	err := stm.WithNewSession(func(se sessionctx.Context) error {
		_, err := ExecSQL(stm.ctx, se,
			"update mysql.tidb_global_task set state=%?, state_update_time = CURRENT_TIMESTAMP() "+
				"where task_key = %? and state in (%?, %?)",
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

// ResumeTask resumes the task.
func (stm *TaskManager) ResumeTask(taskKey string) (bool, error) {
	found := false
	err := stm.WithNewSession(func(se sessionctx.Context) error {
		_, err := ExecSQL(stm.ctx, se,
			"update mysql.tidb_global_task set state=%?, state_update_time = CURRENT_TIMESTAMP() "+
				"where task_key = %? and state = %?",
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

// GetSubtasksForImportInto gets the subtasks for import into(show import jobs).
func (stm *TaskManager) GetSubtasksForImportInto(taskID int64, step proto.Step) ([]*proto.Subtask, error) {
	var (
		rs  []chunk.Row
		err error
	)
	err = stm.WithNewTxn(stm.ctx, func(se sessionctx.Context) error {
		rs, err = ExecSQL(stm.ctx, se,
			"select * from mysql.tidb_background_subtask where task_key = %? and step = %?",
			taskID, step,
		)
		if err != nil {
			return err
		}

		// To avoid the situation that the subtasks has been `TransferSubTasks2History`
		// when the user show import jobs, we need to check the history table.
		rsFromHistory, err := ExecSQL(stm.ctx, se,
			"select * from mysql.tidb_background_subtask_history where task_key = %? and step = %?",
			taskID, step,
		)
		if err != nil {
			return err
		}

		rs = append(rs, rsFromHistory...)
		return nil
	})

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

// TransferSubTasks2History move all the finished subTask to tidb_background_subtask_history by taskID
func (stm *TaskManager) TransferSubTasks2History(taskID int64) error {
	return stm.WithNewTxn(stm.ctx, func(se sessionctx.Context) error {
		_, err := ExecSQL(stm.ctx, se, "insert into mysql.tidb_background_subtask_history select * from mysql.tidb_background_subtask where task_key = %?", taskID)
		if err != nil {
			return err
		}

		// delete taskID subtask
		_, err = ExecSQL(stm.ctx, se, "delete from mysql.tidb_background_subtask where task_key = %?", taskID)
		return err
	})
}

// GCSubtasks deletes the history subtask which is older than the given days.
func (stm *TaskManager) GCSubtasks() error {
	subtaskHistoryKeepSeconds := defaultSubtaskKeepDays * 24 * 60 * 60
	failpoint.Inject("subtaskHistoryKeepSeconds", func(val failpoint.Value) {
		if val, ok := val.(int); ok {
			subtaskHistoryKeepSeconds = val
		}
	})
	_, err := stm.executeSQLWithNewSession(
		stm.ctx,
		fmt.Sprintf("DELETE FROM mysql.tidb_background_subtask_history WHERE state_update_time < UNIX_TIMESTAMP() - %d ;", subtaskHistoryKeepSeconds),
	)
	return err
}

// TransferTasks2History transfer the selected tasks into tidb_global_task_history table by taskIDs.
func (stm *TaskManager) TransferTasks2History(tasks []*proto.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	return stm.WithNewTxn(stm.ctx, func(se sessionctx.Context) error {
		insertSQL := new(strings.Builder)
		if err := sqlexec.FormatSQL(insertSQL, "replace into mysql.tidb_global_task_history"+
			"(id, task_key, type, dispatcher_id, state, start_time, state_update_time,"+
			"meta, concurrency, step, error) values"); err != nil {
			return err
		}

		for i, task := range tasks {
			if i != 0 {
				if err := sqlexec.FormatSQL(insertSQL, ","); err != nil {
					return err
				}
			}
			if err := sqlexec.FormatSQL(insertSQL, "(%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)",
				task.ID, task.Key, task.Type, task.DispatcherID,
				task.State, task.StartTime, task.StateUpdateTime,
				task.Meta, task.Concurrency, task.Step, serializeErr(task.Error)); err != nil {
				return err
			}
		}
		_, err := ExecSQL(stm.ctx, se, insertSQL.String())
		if err != nil {
			return err
		}

		// delete taskIDs tasks
		deleteSQL := new(strings.Builder)
		if err := sqlexec.FormatSQL(deleteSQL, "delete from mysql.tidb_global_task where id in("); err != nil {
			return err
		}
		deleteElems := make([]string, 0, len(tasks))
		for _, task := range tasks {
			deleteElems = append(deleteElems, fmt.Sprintf("%d", task.ID))
		}

		deleteSQL.WriteString(strings.Join(deleteElems, ", "))
		deleteSQL.WriteString(")")
		_, err = ExecSQL(stm.ctx, se, deleteSQL.String())
		return err
	})
}

// GetNodesByRole gets nodes map from dist_framework_meta by role.
func (stm *TaskManager) GetNodesByRole(role string) (map[string]bool, error) {
	rs, err := stm.executeSQLWithNewSession(stm.ctx,
		"select host from mysql.dist_framework_meta where role = %?", role)
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]bool, len(rs))
	for _, r := range rs {
		nodes[r.GetString(0)] = true
	}
	return nodes, nil
}
