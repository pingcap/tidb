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

	"github.com/docker/go-units"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
)

const (
	defaultSubtaskKeepDays = 14

	basicTaskColumns = `t.id, t.task_key, t.type, t.state, t.step, t.priority, t.concurrency, t.create_time, t.target_scope`
	// TaskColumns is the columns for task.
	// TODO: dispatcher_id will update to scheduler_id later
	TaskColumns = basicTaskColumns + `, t.start_time, t.state_update_time, t.meta, t.dispatcher_id, t.error`
	// InsertTaskColumns is the columns used in insert task.
	InsertTaskColumns   = `task_key, type, state, priority, concurrency, step, meta, create_time, target_scope`
	basicSubtaskColumns = `id, step, task_key, type, exec_id, state, concurrency, create_time, ordinal, start_time`
	// SubtaskColumns is the columns for subtask.
	SubtaskColumns = basicSubtaskColumns + `, state_update_time, meta, summary`
	// InsertSubtaskColumns is the columns used in insert subtask.
	InsertSubtaskColumns = `step, task_key, exec_id, meta, state, type, concurrency, ordinal, create_time, checkpoint, summary`
)

var (
	maxSubtaskBatchSize = 16 * units.MiB

	// ErrUnstableSubtasks is the error when we detected that the subtasks are
	// unstable, i.e. count, order and content of the subtasks are changed on
	// different call.
	ErrUnstableSubtasks = errors.New("unstable subtasks")

	// ErrTaskNotFound is the error when we can't found task.
	// i.e. TransferTasks2History move task from tidb_global_task to tidb_global_task_history.
	ErrTaskNotFound = errors.New("task not found")

	// ErrTaskAlreadyExists is the error when we submit a task with the same task key.
	// i.e. SubmitTask in handle may submit a task twice.
	ErrTaskAlreadyExists = errors.New("task already exists")

	// ErrSubtaskNotFound is the error when can't find subtask by subtask_id and execId,
	// i.e. scheduler change the subtask's execId when subtask need to balance to other nodes.
	ErrSubtaskNotFound = errors.New("subtask not found")
)

// TaskExecInfo is the execution information of a task, on some exec node.
type TaskExecInfo struct {
	*proto.TaskBase
	// SubtaskConcurrency is the concurrency of subtask in current task step.
	// TODO: will be used when support subtask have smaller concurrency than task,
	// TODO: such as post-process of import-into.
	// TODO: we might need create one task executor for each step in this case, to alloc
	// TODO: minimal resource
	SubtaskConcurrency int
}

// SessionExecutor defines the interface for executing SQLs in a session.
type SessionExecutor interface {
	// WithNewSession executes the function with a new session.
	WithNewSession(fn func(se sessionctx.Context) error) error
	// WithNewTxn executes the fn in a new transaction.
	WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error
}

// TaskHandle provides the interface for operations needed by Scheduler.
// Then we can use scheduler's function in Scheduler interface.
type TaskHandle interface {
	// GetPreviousSubtaskMetas gets previous subtask metas.
	GetPreviousSubtaskMetas(taskID int64, step proto.Step) ([][]byte, error)
	SessionExecutor
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

// WithNewSession executes the function with a new session.
func (mgr *TaskManager) WithNewSession(fn func(se sessionctx.Context) error) error {
	se, err := mgr.sePool.Get()
	if err != nil {
		return err
	}
	defer mgr.sePool.Put(se)
	return fn(se.(sessionctx.Context))
}

// WithNewTxn executes the fn in a new transaction.
func (mgr *TaskManager) WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error {
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	return mgr.WithNewSession(func(se sessionctx.Context) (err error) {
		_, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), "begin")
		if err != nil {
			return err
		}

		success := false
		defer func() {
			sql := "rollback"
			if success {
				sql = "commit"
			}
			_, commitErr := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), sql)
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

// ExecuteSQLWithNewSession executes one SQL with new session.
func (mgr *TaskManager) ExecuteSQLWithNewSession(ctx context.Context, sql string, args ...any) (rs []chunk.Row, err error) {
	err = mgr.WithNewSession(func(se sessionctx.Context) error {
		rs, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), sql, args...)
		return err
	})

	if err != nil {
		return nil, err
	}

	return
}

// CreateTask adds a new task to task table.
func (mgr *TaskManager) CreateTask(ctx context.Context, key string, tp proto.TaskType, concurrency int, targetScope string, meta []byte) (taskID int64, err error) {
	err = mgr.WithNewSession(func(se sessionctx.Context) error {
		var err2 error
		taskID, err2 = mgr.CreateTaskWithSession(ctx, se, key, tp, concurrency, targetScope, meta)
		return err2
	})
	return
}

// CreateTaskWithSession adds a new task to task table with session.
func (mgr *TaskManager) CreateTaskWithSession(
	ctx context.Context,
	se sessionctx.Context,
	key string,
	tp proto.TaskType,
	concurrency int,
	targetScope string,
	meta []byte,
) (taskID int64, err error) {
	cpuCount, err := mgr.getCPUCountOfNode(ctx, se)
	if err != nil {
		return 0, err
	}
	if concurrency > cpuCount {
		return 0, errors.Errorf("task concurrency(%d) larger than cpu count(%d) of managed node", concurrency, cpuCount)
	}
	_, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			insert into mysql.tidb_global_task(`+InsertTaskColumns+`)
			values (%?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), %?)`,
		key, tp, proto.TaskStatePending, proto.NormalPriority, concurrency, proto.StepInit, meta, targetScope)
	if err != nil {
		return 0, err
	}

	rs, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), "select @@last_insert_id")
	if err != nil {
		return 0, err
	}

	taskID = int64(rs[0].GetUint64(0))
	failpoint.Inject("testSetLastTaskID", func() { TestLastTaskID.Store(taskID) })

	return taskID, nil
}

// GetTopUnfinishedTasks implements the scheduler.TaskManager interface.
func (mgr *TaskManager) GetTopUnfinishedTasks(ctx context.Context) ([]*proto.TaskBase, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		`select `+basicTaskColumns+` from mysql.tidb_global_task t
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
		return nil, err
	}

	tasks := make([]*proto.TaskBase, 0, len(rs))
	for _, r := range rs {
		tasks = append(tasks, row2TaskBasic(r))
	}
	return tasks, nil
}

// GetTaskExecInfoByExecID implements the scheduler.TaskManager interface.
func (mgr *TaskManager) GetTaskExecInfoByExecID(ctx context.Context, execID string) ([]*TaskExecInfo, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		`select `+basicTaskColumns+`, max(st.concurrency)
			from mysql.tidb_global_task t join mysql.tidb_background_subtask st
				on t.id = st.task_key and t.step = st.step
			where t.state in (%?, %?, %?) and st.state in (%?, %?) and st.exec_id = %?
			group by t.id
			order by priority asc, create_time asc, id asc`,
		proto.TaskStateRunning, proto.TaskStateReverting, proto.TaskStatePausing,
		proto.SubtaskStatePending, proto.SubtaskStateRunning, execID)
	if err != nil {
		return nil, err
	}

	res := make([]*TaskExecInfo, 0, len(rs))
	for _, r := range rs {
		res = append(res, &TaskExecInfo{
			TaskBase:           row2TaskBasic(r),
			SubtaskConcurrency: int(r.GetInt64(9)),
		})
	}
	return res, nil
}

// GetTasksInStates gets the tasks in the states(order by priority asc, create_time acs, id asc).
func (mgr *TaskManager) GetTasksInStates(ctx context.Context, states ...any) (task []*proto.Task, err error) {
	if len(states) == 0 {
		return task, nil
	}

	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		"select "+TaskColumns+" from mysql.tidb_global_task t "+
			"where state in ("+strings.Repeat("%?,", len(states)-1)+"%?)"+
			" order by priority asc, create_time asc, id asc", states...)
	if err != nil {
		return task, err
	}

	for _, r := range rs {
		task = append(task, Row2Task(r))
	}
	return task, nil
}

// GetTaskByID gets the task by the task ID.
func (mgr *TaskManager) GetTaskByID(ctx context.Context, taskID int64) (task *proto.Task, err error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, "select "+TaskColumns+" from mysql.tidb_global_task t where id = %?", taskID)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, ErrTaskNotFound
	}

	return Row2Task(rs[0]), nil
}

// GetTaskBaseByID implements the TaskManager.GetTaskBaseByID interface.
func (mgr *TaskManager) GetTaskBaseByID(ctx context.Context, taskID int64) (task *proto.TaskBase, err error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, "select "+basicTaskColumns+" from mysql.tidb_global_task t where id = %?", taskID)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, ErrTaskNotFound
	}

	return row2TaskBasic(rs[0]), nil
}

// GetTaskByIDWithHistory gets the task by the task ID from both tidb_global_task and tidb_global_task_history.
func (mgr *TaskManager) GetTaskByIDWithHistory(ctx context.Context, taskID int64) (task *proto.Task, err error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, "select "+TaskColumns+" from mysql.tidb_global_task t where id = %? "+
		"union select "+TaskColumns+" from mysql.tidb_global_task_history t where id = %?", taskID, taskID)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, ErrTaskNotFound
	}

	return Row2Task(rs[0]), nil
}

// GetTaskBaseByIDWithHistory gets the task by the task ID from both tidb_global_task and tidb_global_task_history.
func (mgr *TaskManager) GetTaskBaseByIDWithHistory(ctx context.Context, taskID int64) (task *proto.TaskBase, err error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, "select "+basicTaskColumns+" from mysql.tidb_global_task t where id = %? "+
		"union select "+basicTaskColumns+" from mysql.tidb_global_task_history t where id = %?", taskID, taskID)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, ErrTaskNotFound
	}

	return row2TaskBasic(rs[0]), nil
}

// GetTaskByKey gets the task by the task key.
func (mgr *TaskManager) GetTaskByKey(ctx context.Context, key string) (task *proto.Task, err error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, "select "+TaskColumns+" from mysql.tidb_global_task t where task_key = %?", key)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, ErrTaskNotFound
	}

	return Row2Task(rs[0]), nil
}

// GetTaskByKeyWithHistory gets the task from history table by the task key.
func (mgr *TaskManager) GetTaskByKeyWithHistory(ctx context.Context, key string) (task *proto.Task, err error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, "select "+TaskColumns+" from mysql.tidb_global_task t where task_key = %?"+
		"union select "+TaskColumns+" from mysql.tidb_global_task_history t where task_key = %?", key, key)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, ErrTaskNotFound
	}

	return Row2Task(rs[0]), nil
}

// GetTaskBaseByKeyWithHistory gets the task base from history table by the task key.
func (mgr *TaskManager) GetTaskBaseByKeyWithHistory(ctx context.Context, key string) (task *proto.TaskBase, err error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, "select "+basicTaskColumns+" from mysql.tidb_global_task t where task_key = %?"+
		"union select "+basicTaskColumns+" from mysql.tidb_global_task_history t where task_key = %?", key, key)
	if err != nil {
		return task, err
	}
	if len(rs) == 0 {
		return nil, ErrTaskNotFound
	}

	return row2TaskBasic(rs[0]), nil
}

// GetSubtasksByExecIDAndStepAndStates gets all subtasks by given states on one node.
func (mgr *TaskManager) GetSubtasksByExecIDAndStepAndStates(ctx context.Context, execID string, taskID int64, step proto.Step, states ...proto.SubtaskState) ([]*proto.Subtask, error) {
	args := []any{execID, taskID, step}
	for _, state := range states {
		args = append(args, state)
	}
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `select `+SubtaskColumns+` from mysql.tidb_background_subtask
		where exec_id = %? and task_key = %? and step = %?
		and state in (`+strings.Repeat("%?,", len(states)-1)+"%?)", args...)
	if err != nil {
		return nil, err
	}

	subtasks := make([]*proto.Subtask, len(rs))
	for i, row := range rs {
		subtasks[i] = Row2SubTask(row)
	}
	return subtasks, nil
}

// GetFirstSubtaskInStates gets the first subtask by given states.
func (mgr *TaskManager) GetFirstSubtaskInStates(ctx context.Context, tidbID string, taskID int64, step proto.Step, states ...proto.SubtaskState) (*proto.Subtask, error) {
	args := []any{tidbID, taskID, step}
	for _, state := range states {
		args = append(args, state)
	}
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `select `+SubtaskColumns+` from mysql.tidb_background_subtask
		where exec_id = %? and task_key = %? and step = %?
		and state in (`+strings.Repeat("%?,", len(states)-1)+"%?) limit 1", args...)
	if err != nil {
		return nil, err
	}

	if len(rs) == 0 {
		return nil, nil
	}
	return Row2SubTask(rs[0]), nil
}

// GetActiveSubtasks implements TaskManager.GetActiveSubtasks.
func (mgr *TaskManager) GetActiveSubtasks(ctx context.Context, taskID int64) ([]*proto.SubtaskBase, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `
		select `+basicSubtaskColumns+` from mysql.tidb_background_subtask
		where task_key = %? and state in (%?, %?)`,
		taskID, proto.SubtaskStatePending, proto.SubtaskStateRunning)
	if err != nil {
		return nil, err
	}
	subtasks := make([]*proto.SubtaskBase, 0, len(rs))
	for _, r := range rs {
		subtasks = append(subtasks, row2BasicSubTask(r))
	}
	return subtasks, nil
}

// GetAllSubtasksByStepAndState gets the subtask by step and state.
func (mgr *TaskManager) GetAllSubtasksByStepAndState(ctx context.Context, taskID int64, step proto.Step, state proto.SubtaskState) ([]*proto.Subtask, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `select `+SubtaskColumns+` from mysql.tidb_background_subtask
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
		subtasks = append(subtasks, Row2SubTask(r))
	}
	return subtasks, nil
}

// GetSubtaskRowCount gets the subtask row count.
func (mgr *TaskManager) GetSubtaskRowCount(ctx context.Context, taskID int64, step proto.Step) (int64, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `select
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
func (mgr *TaskManager) UpdateSubtaskRowCount(ctx context.Context, subtaskID int64, rowCount int64) error {
	_, err := mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask
		set summary = json_set(summary, '$.row_count', %?) where id = %?`,
		rowCount, subtaskID)
	return err
}

// GetSubtaskCntGroupByStates gets the subtask count by states.
func (mgr *TaskManager) GetSubtaskCntGroupByStates(ctx context.Context, taskID int64, step proto.Step) (map[proto.SubtaskState]int64, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `
		select state, count(*)
		from mysql.tidb_background_subtask
		where task_key = %? and step = %?
		group by state`,
		taskID, step)
	if err != nil {
		return nil, err
	}

	res := make(map[proto.SubtaskState]int64, len(rs))
	for _, r := range rs {
		state := proto.SubtaskState(r.GetString(0))
		res[state] = r.GetInt64(1)
	}

	return res, nil
}

// GetSubtaskErrors gets subtasks' errors.
func (mgr *TaskManager) GetSubtaskErrors(ctx context.Context, taskID int64) ([]error, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		`select error from mysql.tidb_background_subtask
             where task_key = %? AND state in (%?, %?)`, taskID, proto.SubtaskStateFailed, proto.SubtaskStateCanceled)
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
func (mgr *TaskManager) HasSubtasksInStates(ctx context.Context, tidbID string, taskID int64, step proto.Step, states ...proto.SubtaskState) (bool, error) {
	args := []any{tidbID, taskID, step}
	for _, state := range states {
		args = append(args, state)
	}
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `select 1 from mysql.tidb_background_subtask
		where exec_id = %? and task_key = %? and step = %?
			and state in (`+strings.Repeat("%?,", len(states)-1)+"%?) limit 1", args...)
	if err != nil {
		return false, err
	}

	return len(rs) > 0, nil
}

// UpdateSubtasksExecIDs update subtasks' execID.
func (mgr *TaskManager) UpdateSubtasksExecIDs(ctx context.Context, subtasks []*proto.SubtaskBase) error {
	// skip the update process.
	if len(subtasks) == 0 {
		return nil
	}
	err := mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		for _, subtask := range subtasks {
			_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
				update mysql.tidb_background_subtask
				set exec_id = %?
				where id = %? and state = %?`,
				subtask.ExecID, subtask.ID, subtask.State)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// SwitchTaskStep implements the scheduler.TaskManager interface.
func (mgr *TaskManager) SwitchTaskStep(
	ctx context.Context,
	task *proto.Task,
	nextState proto.TaskState,
	nextStep proto.Step,
	subtasks []*proto.Subtask,
) error {
	return mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		vars := se.GetSessionVars()
		if vars.MemQuotaQuery < variable.DefTiDBMemQuotaQuery {
			bak := vars.MemQuotaQuery
			if err := vars.SetSystemVar(variable.TiDBMemQuotaQuery,
				strconv.Itoa(variable.DefTiDBMemQuotaQuery)); err != nil {
				return err
			}
			defer func() {
				_ = vars.SetSystemVar(variable.TiDBMemQuotaQuery, strconv.Itoa(int(bak)))
			}()
		}
		err := mgr.updateTaskStateStep(ctx, se, task, nextState, nextStep)
		if err != nil {
			return err
		}
		if vars.StmtCtx.AffectedRows() == 0 {
			// on network partition or owner change, there might be multiple
			// schedulers for the same task, if other scheduler has switched
			// the task to next step, skip the update process.
			// Or when there is no such task.
			return nil
		}
		return mgr.insertSubtasks(ctx, se, subtasks)
	})
}

func (*TaskManager) updateTaskStateStep(ctx context.Context, se sessionctx.Context,
	task *proto.Task, nextState proto.TaskState, nextStep proto.Step) error {
	var extraUpdateStr string
	if task.State == proto.TaskStatePending {
		extraUpdateStr = `start_time = CURRENT_TIMESTAMP(),`
	}
	// TODO: during generating subtask, task meta might change, maybe move meta
	// update to another place.
	_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
		update mysql.tidb_global_task
		set state = %?,
			step = %?, `+extraUpdateStr+`
			state_update_time = CURRENT_TIMESTAMP(),
			meta = %?
		where id = %? and state = %? and step = %?`,
		nextState, nextStep, task.Meta, task.ID, task.State, task.Step)
	return err
}

// TestChannel is used for test.
var TestChannel = make(chan struct{})

func (*TaskManager) insertSubtasks(ctx context.Context, se sessionctx.Context, subtasks []*proto.Subtask) error {
	if len(subtasks) == 0 {
		return nil
	}
	failpoint.Inject("waitBeforeInsertSubtasks", func() {
		<-TestChannel
		<-TestChannel
	})
	var (
		sb         strings.Builder
		markerList = make([]string, 0, len(subtasks))
		args       = make([]any, 0, len(subtasks)*7)
	)
	sb.WriteString(`insert into mysql.tidb_background_subtask(` + InsertSubtaskColumns + `) values `)
	for _, subtask := range subtasks {
		markerList = append(markerList, "(%?, %?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), '{}', '{}')")
		args = append(args, subtask.Step, subtask.TaskID, subtask.ExecID, subtask.Meta,
			proto.SubtaskStatePending, proto.Type2Int(subtask.Type), subtask.Concurrency, subtask.Ordinal)
	}
	sb.WriteString(strings.Join(markerList, ","))
	_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), sb.String(), args...)
	return err
}

// SwitchTaskStepInBatch implements the scheduler.TaskManager interface.
func (mgr *TaskManager) SwitchTaskStepInBatch(
	ctx context.Context,
	task *proto.Task,
	nextState proto.TaskState,
	nextStep proto.Step,
	subtasks []*proto.Subtask,
) error {
	return mgr.WithNewSession(func(se sessionctx.Context) error {
		// some subtasks may be inserted by other schedulers, we can skip them.
		rs, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			select count(1) from mysql.tidb_background_subtask
			where task_key = %? and step = %?`, task.ID, nextStep)
		if err != nil {
			return err
		}
		existingTaskCnt := int(rs[0].GetInt64(0))
		if existingTaskCnt > len(subtasks) {
			return errors.Annotatef(ErrUnstableSubtasks, "expected %d, got %d",
				len(subtasks), existingTaskCnt)
		}
		subtaskBatches := mgr.splitSubtasks(subtasks[existingTaskCnt:])
		for _, batch := range subtaskBatches {
			if err = mgr.insertSubtasks(ctx, se, batch); err != nil {
				return err
			}
		}
		return mgr.updateTaskStateStep(ctx, se, task, nextState, nextStep)
	})
}

func (*TaskManager) splitSubtasks(subtasks []*proto.Subtask) [][]*proto.Subtask {
	var (
		res       = make([][]*proto.Subtask, 0, 10)
		currBatch = make([]*proto.Subtask, 0, 10)
		size      int
	)
	maxSize := int(min(kv.TxnTotalSizeLimit.Load(), uint64(maxSubtaskBatchSize)))
	for _, s := range subtasks {
		if size+len(s.Meta) > maxSize {
			res = append(res, currBatch)
			currBatch = nil
			size = 0
		}
		currBatch = append(currBatch, s)
		size += len(s.Meta)
	}
	if len(currBatch) > 0 {
		res = append(res, currBatch)
	}
	return res
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

// GetSubtasksWithHistory gets the subtasks from tidb_global_task and tidb_global_task_history.
func (mgr *TaskManager) GetSubtasksWithHistory(ctx context.Context, taskID int64, step proto.Step) ([]*proto.Subtask, error) {
	var (
		rs  []chunk.Row
		err error
	)
	err = mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		rs, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			`select `+SubtaskColumns+` from mysql.tidb_background_subtask where task_key = %? and step = %?`,
			taskID, step,
		)
		if err != nil {
			return err
		}

		// To avoid the situation that the subtasks has been `TransferTasks2History`
		// when the user show import jobs, we need to check the history table.
		rsFromHistory, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			`select `+SubtaskColumns+` from mysql.tidb_background_subtask_history where task_key = %? and step = %?`,
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
		subtasks = append(subtasks, Row2SubTask(r))
	}
	return subtasks, nil
}

// GetAllSubtasks gets all subtasks with basic columns.
func (mgr *TaskManager) GetAllSubtasks(ctx context.Context) ([]*proto.SubtaskBase, error) {
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `select `+basicSubtaskColumns+` from mysql.tidb_background_subtask`)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}
	subtasks := make([]*proto.SubtaskBase, 0, len(rs))
	for _, r := range rs {
		subtasks = append(subtasks, row2BasicSubTask(r))
	}
	return subtasks, nil
}

// AdjustTaskOverflowConcurrency change the task concurrency to a max value supported by current cluster.
// This is a workaround for an upgrade bug: in v7.5.x, the task concurrency is hard-coded to 16, resulting in
// a stuck issue if the new version TiDB has less than 16 CPU count.
// We don't adjust the concurrency in subtask table because this field does not exist in v7.5.0.
// For details, see https://github.com/pingcap/tidb/issues/50894.
// For the following versions, there is a check when submitting a new task. This function should be a no-op.
func (mgr *TaskManager) AdjustTaskOverflowConcurrency(ctx context.Context, se sessionctx.Context) error {
	cpuCount, err := mgr.getCPUCountOfNode(ctx, se)
	if err != nil {
		return err
	}
	sql := "update mysql.tidb_global_task set concurrency = %? where concurrency > %?;"
	_, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), sql, cpuCount, cpuCount)
	return err
}
