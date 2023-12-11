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
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	defaultSubtaskKeepDays = 14

	basicTaskColumns = `id, task_key, type, state, step, priority, concurrency, create_time`
	taskColumns      = basicTaskColumns + `, start_time, state_update_time, meta, dispatcher_id, error`
	// InsertTaskColumns is the columns used in insert task.
	InsertTaskColumns = `task_key, type, state, priority, concurrency, step, meta, create_time, start_time, state_update_time`

	subtaskColumns = `id, step, task_key, type, exec_id, state, concurrency, create_time,
				start_time, state_update_time, meta, summary`
	// InsertSubtaskColumns is the columns used in insert subtask.
	InsertSubtaskColumns = `step, task_key, exec_id, meta, state, type, concurrency, create_time, checkpoint, summary`
)

// SessionExecutor defines the interface for executing SQLs in a session.
type SessionExecutor interface {
	// WithNewSession executes the function with a new session.
	WithNewSession(fn func(se sessionctx.Context) error) error
	// WithNewTxn executes the fn in a new transaction.
	WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error
}

// TaskManager is the manager of task and subtask.
type TaskManager struct {
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
func NewTaskManager(sePool sessionPool) *TaskManager {
	return &TaskManager{
		sePool: sePool,
	}
}

// GetTaskManager gets the task manager.
func GetTaskManager() (*TaskManager, error) {
	v := taskManagerInstance.Load()
	if v == nil {
		return nil, errors.New("task manager is not initialized")
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

func row2TaskBasic(r chunk.Row) *proto.Task {
	task := &proto.Task{
		ID:          r.GetInt64(0),
		Key:         r.GetString(1),
		Type:        proto.TaskType(r.GetString(2)),
		State:       proto.TaskState(r.GetString(3)),
		Step:        proto.Step(r.GetInt64(4)),
		Priority:    int(r.GetInt64(5)),
		Concurrency: int(r.GetInt64(6)),
	}
	task.CreateTime, _ = r.GetTime(7).GoTime(time.Local)
	return task
}

// row2Task converts a row to a task.
func row2Task(r chunk.Row) *proto.Task {
	task := row2TaskBasic(r)
	task.StartTime, _ = r.GetTime(8).GoTime(time.Local)
	task.StateUpdateTime, _ = r.GetTime(9).GoTime(time.Local)
	task.Meta = r.GetBytes(10)
	task.DispatcherID = r.GetString(11)
	if !r.IsNull(12) {
		errBytes := r.GetBytes(12)
		stdErr := errors.Normalize("")
		err := stdErr.UnmarshalJSON(errBytes)
		if err != nil {
			logutil.BgLogger().Error("unmarshal task error", zap.Error(err))
			task.Error = errors.New(string(errBytes))
		} else {
			task.Error = stdErr
		}
	}
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

// CreateTask adds a new task to task table.
func (stm *TaskManager) CreateTask(ctx context.Context, key string, tp proto.TaskType, concurrency int, meta []byte) (taskID int64, err error) {
	err = stm.WithNewSession(func(se sessionctx.Context) error {
		var err2 error
		taskID, err2 = stm.CreateTaskWithSession(ctx, se, key, tp, concurrency, meta)
		return err2
	})
	return
}

// CreateTaskWithSession adds a new task to task table with session.
func (*TaskManager) CreateTaskWithSession(ctx context.Context, se sessionctx.Context, key string, tp proto.TaskType, concurrency int, meta []byte) (taskID int64, err error) {
	_, err = ExecSQL(ctx, se, `
			insert into mysql.tidb_global_task(`+InsertTaskColumns+`)
			values (%?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`,
		key, tp, proto.TaskStatePending, proto.NormalPriority, concurrency, proto.StepInit, meta)
	if err != nil {
		return 0, err
	}

	rs, err := ExecSQL(ctx, se, "select @@last_insert_id")
	if err != nil {
		return 0, err
	}

	taskID = int64(rs[0].GetUint64(0))
	failpoint.Inject("testSetLastTaskID", func() { TestLastTaskID.Store(taskID) })

	return taskID, nil
}

// GetOneTask get a task from task table, it's used by dispatcher only.
func (stm *TaskManager) GetOneTask(ctx context.Context) (task *proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(ctx, "select "+taskColumns+" from mysql.tidb_global_task where state = %? limit 1", proto.TaskStatePending)
	if err != nil {
		return task, err
	}

	if len(rs) == 0 {
		return nil, nil
	}

	return row2Task(rs[0]), nil
}

// GetTopUnfinishedTasks implements the dispatcher.TaskManager interface.
func (stm *TaskManager) GetTopUnfinishedTasks(ctx context.Context) (task []*proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(ctx,
		`select `+basicTaskColumns+` from mysql.tidb_global_task
		where state in (%?, %?, %?, %?, %?, %?)
		order by priority asc, create_time asc, id asc
		limit %?`,
		proto.TaskStatePending,
		proto.TaskStateRunning,
		proto.TaskStateReverting,
		proto.TaskStateCancelling,
		proto.TaskStatePausing,
		proto.TaskStateResuming,
		proto.MaxConcurrentTask*2,
	)
	if err != nil {
		return task, err
	}

	for _, r := range rs {
		task = append(task, row2TaskBasic(r))
	}
	return task, nil
}

// GetTasksInStates gets the tasks in the states.
func (stm *TaskManager) GetTasksInStates(ctx context.Context, states ...interface{}) (task []*proto.Task, err error) {
	if len(states) == 0 {
		return task, nil
	}

	rs, err := stm.executeSQLWithNewSession(ctx, "select "+taskColumns+" from mysql.tidb_global_task where state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", states...)
	if err != nil {
		return task, err
	}

	for _, r := range rs {
		task = append(task, row2Task(r))
	}
	return task, nil
}

// GetTasksFromHistoryInStates gets the tasks in history table in the states.
func (stm *TaskManager) GetTasksFromHistoryInStates(ctx context.Context, states ...interface{}) (task []*proto.Task, err error) {
	if len(states) == 0 {
		return task, nil
	}

	rs, err := stm.executeSQLWithNewSession(ctx, "select "+taskColumns+" from mysql.tidb_global_task_history where state in ("+strings.Repeat("%?,", len(states)-1)+"%?)", states...)
	if err != nil {
		return task, err
	}

	for _, r := range rs {
		task = append(task, row2Task(r))
	}
	return task, nil
}

// GetTaskByID gets the task by the task ID.
func (stm *TaskManager) GetTaskByID(ctx context.Context, taskID int64) (task *proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(ctx, "select "+taskColumns+" from mysql.tidb_global_task where id = %?", taskID)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	return row2Task(rs[0]), nil
}

// GetTaskByIDWithHistory gets the task by the task ID from both tidb_global_task and tidb_global_task_history.
func (stm *TaskManager) GetTaskByIDWithHistory(ctx context.Context, taskID int64) (task *proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(ctx, "select "+taskColumns+" from mysql.tidb_global_task where id = %? "+
		"union select "+taskColumns+" from mysql.tidb_global_task_history where id = %?", taskID, taskID)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	return row2Task(rs[0]), nil
}

// GetTaskByKey gets the task by the task key.
func (stm *TaskManager) GetTaskByKey(ctx context.Context, key string) (task *proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(ctx, "select "+taskColumns+" from mysql.tidb_global_task where task_key = %?", key)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	return row2Task(rs[0]), nil
}

// GetTaskByKeyWithHistory gets the task from history table by the task key.
func (stm *TaskManager) GetTaskByKeyWithHistory(ctx context.Context, key string) (task *proto.Task, err error) {
	rs, err := stm.executeSQLWithNewSession(ctx, "select "+taskColumns+" from mysql.tidb_global_task where task_key = %?"+
		"union select "+taskColumns+" from mysql.tidb_global_task_history where task_key = %?", key, key)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, nil
	}

	return row2Task(rs[0]), nil
}

// FailTask implements the dispatcher.TaskManager interface.
func (stm *TaskManager) FailTask(ctx context.Context, taskID int64, currentState proto.TaskState, taskErr error) error {
	_, err := stm.executeSQLWithNewSession(ctx,
		`update mysql.tidb_global_task
			set state=%?,
			    error = %?,
			    state_update_time = CURRENT_TIMESTAMP()
			where id=%? and state=%?`,
		proto.TaskStateFailed, serializeErr(taskErr), taskID, currentState,
	)
	return err
}

// GetUsedSlotsOnNodes implements the dispatcher.TaskManager interface.
func (stm *TaskManager) GetUsedSlotsOnNodes(ctx context.Context) (map[string]int, error) {
	// concurrency of subtasks of some step is the same, we use max(concurrency)
	// to make group by works.
	rs, err := stm.executeSQLWithNewSession(ctx, `
		select
			exec_id, sum(concurrency)
		from (
			select exec_id, task_key, max(concurrency) concurrency
			from mysql.tidb_background_subtask
			where state in (%?, %?)
			group by exec_id, task_key
		) a
		group by exec_id`,
		proto.TaskStatePending, proto.TaskStateRunning,
	)
	if err != nil {
		return nil, err
	}

	slots := make(map[string]int, len(rs))
	for _, r := range rs {
		val, _ := r.GetMyDecimal(1).ToInt()
		slots[r.GetString(0)] = int(val)
	}
	return slots, nil
}

// row2SubTask converts a row to a subtask.
func row2SubTask(r chunk.Row) *proto.Subtask {
	// subtask defines start/update time as bigint, to ensure backward compatible,
	// we keep it that way, and we convert it here.
	createTime, _ := r.GetTime(7).GoTime(time.Local)
	var startTime, updateTime time.Time
	if !r.IsNull(8) {
		ts := r.GetInt64(8)
		startTime = time.Unix(ts, 0)
	}
	if !r.IsNull(9) {
		ts := r.GetInt64(9)
		updateTime = time.Unix(ts, 0)
	}
	subtask := &proto.Subtask{
		ID:          r.GetInt64(0),
		Step:        proto.Step(r.GetInt64(1)),
		Type:        proto.Int2Type(int(r.GetInt64(3))),
		ExecID:      r.GetString(4),
		State:       proto.TaskState(r.GetString(5)),
		Concurrency: int(r.GetInt64(6)),
		CreateTime:  createTime,
		StartTime:   startTime,
		UpdateTime:  updateTime,
		Meta:        r.GetBytes(10),
		Summary:     r.GetJSON(11).String(),
	}
	taskIDStr := r.GetString(2)
	tid, err := strconv.Atoi(taskIDStr)
	if err != nil {
		logutil.BgLogger().Warn("unexpected subtask id", zap.String("subtask-id", taskIDStr))
	}
	subtask.TaskID = int64(tid)
	return subtask
}

// GetSubtasksInStates gets all subtasks by given states.
func (stm *TaskManager) GetSubtasksInStates(ctx context.Context, tidbID string, taskID int64, step proto.Step, states ...interface{}) ([]*proto.Subtask, error) {
	args := []interface{}{tidbID, taskID, step}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(ctx, `select `+subtaskColumns+` from mysql.tidb_background_subtask
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

// GetSubtasksByExecIdsAndStepAndState gets all subtasks by given taskID, exec_id, step and state.
func (stm *TaskManager) GetSubtasksByExecIdsAndStepAndState(ctx context.Context, tidbIDs []string, taskID int64, step proto.Step, state proto.TaskState) ([]*proto.Subtask, error) {
	args := []interface{}{taskID, step, state}
	for _, tidbID := range tidbIDs {
		args = append(args, tidbID)
	}
	rs, err := stm.executeSQLWithNewSession(ctx, `select `+subtaskColumns+` from mysql.tidb_background_subtask
		where task_key = %? and step = %? and state = %?
		and exec_id in (`+strings.Repeat("%?,", len(tidbIDs)-1)+"%?)", args...)
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
func (stm *TaskManager) GetFirstSubtaskInStates(ctx context.Context, tidbID string, taskID int64, step proto.Step, states ...interface{}) (*proto.Subtask, error) {
	args := []interface{}{tidbID, taskID, step}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(ctx, `select `+subtaskColumns+` from mysql.tidb_background_subtask
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

// UpdateSubtaskExecID updates the subtask's exec_id, used for testing now.
func (stm *TaskManager) UpdateSubtaskExecID(ctx context.Context, tidbID string, subtaskID int64) error {
	_, err := stm.executeSQLWithNewSession(ctx, `update mysql.tidb_background_subtask
		set exec_id = %?, state_update_time = unix_timestamp() where id = %?`,
		tidbID, subtaskID)
	return err
}

// UpdateErrorToSubtask updates the error to subtask.
func (stm *TaskManager) UpdateErrorToSubtask(ctx context.Context, tidbID string, taskID int64, err error) error {
	if err == nil {
		return nil
	}
	_, err1 := stm.executeSQLWithNewSession(ctx, `update mysql.tidb_background_subtask
		set state = %?, error = %?, start_time = unix_timestamp(), state_update_time = unix_timestamp()
		where exec_id = %? and task_key = %? and state in (%?, %?) limit 1;`,
		proto.TaskStateFailed, serializeErr(err), tidbID, taskID, proto.TaskStatePending, proto.TaskStateRunning)
	return err1
}

// PrintSubtaskInfo log the subtask info by taskKey. Only used for UT.
func (stm *TaskManager) PrintSubtaskInfo(ctx context.Context, taskID int64) {
	rs, _ := stm.executeSQLWithNewSession(ctx,
		`select `+subtaskColumns+` from mysql.tidb_background_subtask_history where task_key = %?`, taskID)
	rs2, _ := stm.executeSQLWithNewSession(ctx,
		`select `+subtaskColumns+` from mysql.tidb_background_subtask where task_key = %?`, taskID)
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

// GetSubtasksByStepAndState gets the subtask by step and state.
func (stm *TaskManager) GetSubtasksByStepAndState(ctx context.Context, taskID int64, step proto.Step, state proto.TaskState) ([]*proto.Subtask, error) {
	rs, err := stm.executeSQLWithNewSession(ctx, `select `+subtaskColumns+` from mysql.tidb_background_subtask
		where task_key = %? and state = %? and step = %?`,
		taskID, state, step)
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
func (stm *TaskManager) GetSubtaskRowCount(ctx context.Context, taskID int64, step proto.Step) (int64, error) {
	rs, err := stm.executeSQLWithNewSession(ctx, `select
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
func (stm *TaskManager) UpdateSubtaskRowCount(ctx context.Context, subtaskID int64, rowCount int64) error {
	_, err := stm.executeSQLWithNewSession(ctx, `update mysql.tidb_background_subtask
		set summary = json_set(summary, '$.row_count', %?) where id = %?`,
		rowCount, subtaskID)
	return err
}

// GetSubtaskInStatesCnt gets the subtask count in the states.
func (stm *TaskManager) GetSubtaskInStatesCnt(ctx context.Context, taskID int64, states ...interface{}) (int64, error) {
	args := []interface{}{taskID}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(ctx, `select count(*) from mysql.tidb_background_subtask
		where task_key = %? and state in (`+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return 0, err
	}

	return rs[0].GetInt64(0), nil
}

// CollectSubTaskError collects the subtask error.
func (stm *TaskManager) CollectSubTaskError(ctx context.Context, taskID int64) ([]error, error) {
	rs, err := stm.executeSQLWithNewSession(ctx,
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
func (stm *TaskManager) HasSubtasksInStates(ctx context.Context, tidbID string, taskID int64, step proto.Step, states ...interface{}) (bool, error) {
	args := []interface{}{tidbID, taskID, step}
	args = append(args, states...)
	rs, err := stm.executeSQLWithNewSession(ctx, `select 1 from mysql.tidb_background_subtask
		where exec_id = %? and task_key = %? and step = %?
			and state in (`+strings.Repeat("%?,", len(states)-1)+"%?) limit 1", args...)
	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}

// StartSubtask updates the subtask state to running.
func (stm *TaskManager) StartSubtask(ctx context.Context, subtaskID int64) error {
	_, err := stm.executeSQLWithNewSession(ctx, `update mysql.tidb_background_subtask
		set state = %?, start_time = unix_timestamp(), state_update_time = unix_timestamp()
		where id = %?`,
		proto.TaskStateRunning, subtaskID)
	return err
}

// StartManager insert the manager information into dist_framework_meta.
func (stm *TaskManager) StartManager(ctx context.Context, tidbID string, role string) error {
	_, err := stm.executeSQLWithNewSession(ctx, `replace into mysql.dist_framework_meta values(%?, %?, DEFAULT)`, tidbID, role)
	return err
}

// UpdateSubtaskStateAndError updates the subtask state.
func (stm *TaskManager) UpdateSubtaskStateAndError(ctx context.Context, tidbID string, id int64, state proto.TaskState, subTaskErr error) error {
	_, err := stm.executeSQLWithNewSession(ctx, `update mysql.tidb_background_subtask
		set state = %?, error = %?, state_update_time = unix_timestamp() where id = %? and exec_id = %?`,
		state, serializeErr(subTaskErr), id, tidbID)
	return err
}

// FinishSubtask updates the subtask meta and mark state to succeed.
func (stm *TaskManager) FinishSubtask(ctx context.Context, tidbID string, id int64, meta []byte) error {
	_, err := stm.executeSQLWithNewSession(ctx, `update mysql.tidb_background_subtask
		set meta = %?, state = %?, state_update_time = unix_timestamp() where id = %? and exec_id = %?`,
		meta, proto.TaskStateSucceed, id, tidbID)
	return err
}

// DeleteSubtasksByTaskID deletes the subtask of the given task ID.
func (stm *TaskManager) DeleteSubtasksByTaskID(ctx context.Context, taskID int64) error {
	_, err := stm.executeSQLWithNewSession(ctx, `delete from mysql.tidb_background_subtask
		where task_key = %?`, taskID)
	if err != nil {
		return err
	}

	return nil
}

// GetTaskExecutorIDsByTaskID gets the task executor IDs of the given task ID.
func (stm *TaskManager) GetTaskExecutorIDsByTaskID(ctx context.Context, taskID int64) ([]string, error) {
	rs, err := stm.executeSQLWithNewSession(ctx, `select distinct(exec_id) from mysql.tidb_background_subtask
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

// GetTaskExecutorIDsByTaskIDAndStep gets the task executor IDs of the given global task ID and step.
func (stm *TaskManager) GetTaskExecutorIDsByTaskIDAndStep(ctx context.Context, taskID int64, step proto.Step) ([]string, error) {
	rs, err := stm.executeSQLWithNewSession(ctx, `select distinct(exec_id) from mysql.tidb_background_subtask
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

// IsTaskExecutorCanceled checks if subtask 'execID' of task 'taskID' has been canceled somehow.
func (stm *TaskManager) IsTaskExecutorCanceled(ctx context.Context, execID string, taskID int64) (bool, error) {
	rs, err := stm.executeSQLWithNewSession(ctx, "select 1 from mysql.tidb_background_subtask where task_key = %? and exec_id = %?", taskID, execID)
	if err != nil {
		return false, err
	}
	return len(rs) == 0, nil
}

// UpdateSubtasksExecIDs update subtasks' execID.
func (stm *TaskManager) UpdateSubtasksExecIDs(ctx context.Context, taskID int64, subtasks []*proto.Subtask) error {
	// skip the update process.
	if len(subtasks) == 0 {
		return nil
	}
	err := stm.WithNewTxn(ctx, func(se sessionctx.Context) error {
		for _, subtask := range subtasks {
			_, err := ExecSQL(ctx, se,
				"update mysql.tidb_background_subtask set exec_id = %? where id = %? and state = %? and task_key = %?",
				subtask.ExecID,
				subtask.ID,
				subtask.State,
				taskID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// CleanUpMeta cleanup the outdated row in dist_framework_meta when some tidb down.
func (stm *TaskManager) CleanUpMeta(ctx context.Context, nodes []string) error {
	if len(nodes) == 0 {
		return nil
	}
	return stm.WithNewTxn(ctx, func(se sessionctx.Context) error {
		deleteSQL := new(strings.Builder)
		if err := sqlescape.FormatSQL(deleteSQL, "delete from mysql.dist_framework_meta where host in("); err != nil {
			return err
		}
		deleteElems := make([]string, 0, len(nodes))
		for _, node := range nodes {
			deleteElems = append(deleteElems, fmt.Sprintf(`"%s"`, node))
		}

		deleteSQL.WriteString(strings.Join(deleteElems, ", "))
		deleteSQL.WriteString(")")
		_, err := ExecSQL(ctx, se, deleteSQL.String())
		return err
	})
}

// PauseSubtasks update all running/pending subtasks to pasued state.
func (stm *TaskManager) PauseSubtasks(ctx context.Context, tidbID string, taskID int64) error {
	_, err := stm.executeSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask set state = "paused" where task_key = %? and state in ("running", "pending") and exec_id = %?`, taskID, tidbID)
	return err
}

// ResumeSubtasks update all paused subtasks to pending state.
func (stm *TaskManager) ResumeSubtasks(ctx context.Context, taskID int64) error {
	_, err := stm.executeSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask set state = "pending", error = null where task_key = %? and state = "paused"`, taskID)
	return err
}

// UpdateTaskAndAddSubTasks update the task and add new subtasks
func (stm *TaskManager) UpdateTaskAndAddSubTasks(ctx context.Context, task *proto.Task, subtasks []*proto.Subtask, prevState proto.TaskState) (bool, error) {
	retryable := true
	err := stm.WithNewTxn(ctx, func(se sessionctx.Context) error {
		_, err := ExecSQL(ctx, se, "update mysql.tidb_global_task "+
			"set state = %?, dispatcher_id = %?, step = %?, concurrency = %?, meta = %?, error = %?, state_update_time = CURRENT_TIMESTAMP()"+
			"where id = %? and state = %?",
			task.State, task.DispatcherID, task.Step, task.Concurrency, task.Meta, serializeErr(task.Error), task.ID, prevState)
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
			// So when dispatching subtasks, framework needs to update task and insert subtasks in one Txn.
			//
			// In future, it's needed to restrict changes of task in OnNextSubtasksBatch.
			// If OnNextSubtasksBatch won't update any fields in proto.Task, we can insert subtasks only.
			//
			// For now, we update nothing in proto.Task in UT's OnNextSubtasksBatch, so the AffectedRows will be 0. So UT can't fully compatible
			// with current UpdateTaskAndAddSubTasks implementation.
			rs, err := ExecSQL(ctx, se, "select id from mysql.tidb_global_task where id = %? and state = %?", task.ID, prevState)
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
			if task.State == proto.TaskStateReverting {
				subtaskState = proto.TaskStateRevertPending
			}

			sql := new(strings.Builder)
			if err := sqlescape.FormatSQL(sql, `insert into mysql.tidb_background_subtask(`+InsertSubtaskColumns+`) values`); err != nil {
				return err
			}
			for i, subtask := range subtasks {
				if i != 0 {
					if err := sqlescape.FormatSQL(sql, ","); err != nil {
						return err
					}
				}
				if err := sqlescape.FormatSQL(sql, "(%?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), '{}', '{}')",
					subtask.Step, task.ID, subtask.ExecID, subtask.Meta, subtaskState, proto.Type2Int(subtask.Type), subtask.Concurrency); err != nil {
					return err
				}
			}
			_, err := ExecSQL(ctx, se, sql.String())
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

// CancelTask cancels task.
func (stm *TaskManager) CancelTask(ctx context.Context, taskID int64) error {
	_, err := stm.executeSQLWithNewSession(ctx,
		"update mysql.tidb_global_task set state=%?, state_update_time = CURRENT_TIMESTAMP() "+
			"where id=%? and state in (%?, %?)",
		proto.TaskStateCancelling, taskID, proto.TaskStatePending, proto.TaskStateRunning,
	)
	return err
}

// CancelTaskByKeySession cancels task by key using input session.
func (*TaskManager) CancelTaskByKeySession(ctx context.Context, se sessionctx.Context, taskKey string) error {
	_, err := ExecSQL(ctx, se,
		"update mysql.tidb_global_task set state=%?, state_update_time = CURRENT_TIMESTAMP() "+
			"where task_key=%? and state in (%?, %?)",
		proto.TaskStateCancelling, taskKey, proto.TaskStatePending, proto.TaskStateRunning)
	return err
}

// IsTaskCancelling checks whether the task state is cancelling.
func (stm *TaskManager) IsTaskCancelling(ctx context.Context, taskID int64) (bool, error) {
	rs, err := stm.executeSQLWithNewSession(ctx, "select 1 from mysql.tidb_global_task where id=%? and state = %?",
		taskID, proto.TaskStateCancelling,
	)

	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}

// PauseTask pauses the task.
func (stm *TaskManager) PauseTask(ctx context.Context, taskKey string) (bool, error) {
	found := false
	err := stm.WithNewSession(func(se sessionctx.Context) error {
		_, err := ExecSQL(ctx, se,
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
func (stm *TaskManager) ResumeTask(ctx context.Context, taskKey string) (bool, error) {
	found := false
	err := stm.WithNewSession(func(se sessionctx.Context) error {
		_, err := ExecSQL(ctx, se,
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
func (stm *TaskManager) GetSubtasksForImportInto(ctx context.Context, taskID int64, step proto.Step) ([]*proto.Subtask, error) {
	var (
		rs  []chunk.Row
		err error
	)
	err = stm.WithNewTxn(ctx, func(se sessionctx.Context) error {
		rs, err = ExecSQL(ctx, se,
			`select `+subtaskColumns+` from mysql.tidb_background_subtask where task_key = %? and step = %?`,
			taskID, step,
		)
		if err != nil {
			return err
		}

		// To avoid the situation that the subtasks has been `TransferSubTasks2History`
		// when the user show import jobs, we need to check the history table.
		rsFromHistory, err := ExecSQL(ctx, se,
			`select `+subtaskColumns+` from mysql.tidb_background_subtask_history where task_key = %? and step = %?`,
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
func (stm *TaskManager) TransferSubTasks2History(ctx context.Context, taskID int64) error {
	return stm.WithNewTxn(ctx, func(se sessionctx.Context) error {
		_, err := ExecSQL(ctx, se, `insert into mysql.tidb_background_subtask_history select * from mysql.tidb_background_subtask where task_key = %?`, taskID)
		if err != nil {
			return err
		}

		// delete taskID subtask
		_, err = ExecSQL(ctx, se, "delete from mysql.tidb_background_subtask where task_key = %?", taskID)
		return err
	})
}

// GCSubtasks deletes the history subtask which is older than the given days.
func (stm *TaskManager) GCSubtasks(ctx context.Context) error {
	subtaskHistoryKeepSeconds := defaultSubtaskKeepDays * 24 * 60 * 60
	failpoint.Inject("subtaskHistoryKeepSeconds", func(val failpoint.Value) {
		if val, ok := val.(int); ok {
			subtaskHistoryKeepSeconds = val
		}
	})
	_, err := stm.executeSQLWithNewSession(
		ctx,
		fmt.Sprintf("DELETE FROM mysql.tidb_background_subtask_history WHERE state_update_time < UNIX_TIMESTAMP() - %d ;", subtaskHistoryKeepSeconds),
	)
	return err
}

// TransferTasks2History transfer the selected tasks into tidb_global_task_history table by taskIDs.
func (stm *TaskManager) TransferTasks2History(ctx context.Context, tasks []*proto.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	return stm.WithNewTxn(ctx, func(se sessionctx.Context) error {
		insertSQL := new(strings.Builder)
		if err := sqlescape.FormatSQL(insertSQL, "replace into mysql.tidb_global_task_history"+
			"(id, task_key, type, dispatcher_id, state, priority, start_time, state_update_time,"+
			"meta, concurrency, step, error) values"); err != nil {
			return err
		}

		for i, task := range tasks {
			if i != 0 {
				if err := sqlescape.FormatSQL(insertSQL, ","); err != nil {
					return err
				}
			}
			if err := sqlescape.FormatSQL(insertSQL, "(%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)",
				task.ID, task.Key, task.Type, task.DispatcherID,
				task.State, task.Priority, task.StartTime, task.StateUpdateTime,
				task.Meta, task.Concurrency, task.Step, serializeErr(task.Error)); err != nil {
				return err
			}
		}
		_, err := ExecSQL(ctx, se, insertSQL.String())
		if err != nil {
			return err
		}

		// delete taskIDs tasks
		deleteSQL := new(strings.Builder)
		if err := sqlescape.FormatSQL(deleteSQL, "delete from mysql.tidb_global_task where id in("); err != nil {
			return err
		}
		deleteElems := make([]string, 0, len(tasks))
		for _, task := range tasks {
			deleteElems = append(deleteElems, fmt.Sprintf("%d", task.ID))
		}

		deleteSQL.WriteString(strings.Join(deleteElems, ", "))
		deleteSQL.WriteString(")")
		_, err = ExecSQL(ctx, se, deleteSQL.String())
		return err
	})
}

// GetManagedNodes implements dispatcher.TaskManager interface.
func (stm *TaskManager) GetManagedNodes(ctx context.Context) ([]string, error) {
	rs, err := stm.executeSQLWithNewSession(ctx, `
		select host, role
		from mysql.dist_framework_meta
		where role = 'background' or role = ''
		order by host`)
	if err != nil {
		return nil, err
	}
	nodes := make(map[string][]string, 2)
	for _, r := range rs {
		role := r.GetString(1)
		nodes[role] = append(nodes[role], r.GetString(0))
	}
	if len(nodes["background"]) == 0 {
		return nodes[""], nil
	}
	return nodes["background"], nil
}

// GetAllNodes gets nodes in dist_framework_meta.
func (stm *TaskManager) GetAllNodes(ctx context.Context) ([]string, error) {
	rs, err := stm.executeSQLWithNewSession(ctx,
		"select host from mysql.dist_framework_meta")
	if err != nil {
		return nil, err
	}
	nodes := make([]string, 0, len(rs))
	for _, r := range rs {
		nodes = append(nodes, r.GetString(0))
	}
	return nodes, nil
}
