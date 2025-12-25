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
	"encoding/json"
	goerrors "errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/tracing"
	clitutil "github.com/tikv/client-go/v2/util"
)

const (
	defaultSubtaskKeepDays = 14

	basicTaskColumns = `t.id, t.task_key, t.type, t.state, t.step, t.priority, t.concurrency, t.create_time, t.target_scope, t.max_node_count, t.extra_params, t.keyspace`
	// TaskColumns is the columns for task.
	// TODO: dispatcher_id will update to scheduler_id later
	TaskColumns = basicTaskColumns + `, t.start_time, t.state_update_time, t.meta, t.dispatcher_id, t.error, t.modify_params`
	// InsertTaskColumns is the columns used in insert task.
	InsertTaskColumns   = `task_key, type, state, priority, concurrency, step, meta, create_time, target_scope, max_node_count, extra_params, keyspace`
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
	ErrUnstableSubtasks = goerrors.New("unstable subtasks")

	// ErrTaskNotFound is the error when we can't found task.
	// i.e. TransferTasks2History move task from tidb_global_task to tidb_global_task_history.
	ErrTaskNotFound = goerrors.New("task not found")

	// ErrTaskAlreadyExists is the error when we submit a task with the same task key.
	// i.e. SubmitTask in handle may submit a task twice.
	ErrTaskAlreadyExists = goerrors.New("task already exists")

	// ErrTaskStateNotAllow is the error when the task state is not allowed to do the operation.
	ErrTaskStateNotAllow = goerrors.New("task state not allow to do the operation")

	// ErrTaskChanged is the error when task changed by other operation.
	ErrTaskChanged = goerrors.New("task changed by other operation")

	// ErrSubtaskNotFound is the error when can't find subtask by subtask_id and execId,
	// i.e. scheduler change the subtask's execId when subtask need to balance to other nodes.
	ErrSubtaskNotFound = goerrors.New("subtask not found")
)

// Manager is the interface for task manager.
// those methods are used by application side, we expose them through interface
// to make tests easier.
type Manager interface {
	GetCPUCountOfNode(ctx context.Context) (int, error)
	GetTaskByID(ctx context.Context, taskID int64) (task *proto.Task, err error)
	ModifyTaskByID(ctx context.Context, taskID int64, param *proto.ModifyParam) error
}

// TaskExecInfo is the execution information of a task, on some exec node.
type TaskExecInfo struct {
	*proto.TaskBase
	// SubtaskConcurrency is the concurrency of subtask in current task step.
	// TODO: will be used when support subtask have smaller concurrency than task,
	// TODO: such as post-process of import-into. Also remember the 'modifying' state
	// also update subtask concurrency.
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

	// GetPreviousSubtaskSummary gets previous subtask summaries.
	GetPreviousSubtaskSummary(taskID int64, step proto.Step) ([]*execute.SubtaskSummary, error)

	SessionExecutor
}

// TaskManager is the manager of task and subtask.
type TaskManager struct {
	sePool util.SessionPool
}

var _ SessionExecutor = &TaskManager{}

var taskManagerInstance atomic.Pointer[TaskManager]

// this one is only used on nextgen, and it's only initialized in user ks and
// point to SYSTEM KS
var dxfSvcTaskMgr atomic.Pointer[TaskManager]

var (
	// TestLastTaskID is used for test to set the last task ID.
	TestLastTaskID atomic.Int64
)

// NewTaskManager creates a new task manager.
func NewTaskManager(sePool util.SessionPool) *TaskManager {
	return &TaskManager{
		sePool: sePool,
	}
}

// GetTaskManager gets the task manager.
// this task manager always point to the storage of this TiDB instance, not only
// DXF uses it, add-index and import-into also use this manager to access internal
// sessions.
// in nextgen, DXF service only runs on SYSTEM ks, to access it, use GetDXFSvcTaskMgr
// instead.
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

// GetDXFSvcTaskMgr returns the task manager to access DXF service.
func GetDXFSvcTaskMgr() (*TaskManager, error) {
	if kerneltype.IsNextGen() && config.GetGlobalKeyspaceName() != keyspace.System {
		v := dxfSvcTaskMgr.Load()
		if v == nil {
			return nil, errors.New("DXF service task manager is not initialized")
		}
		return v, nil
	}
	return GetTaskManager()
}

// SetDXFSvcTaskMgr sets the task manager for DXF service.
func SetDXFSvcTaskMgr(mgr *TaskManager) {
	dxfSvcTaskMgr.Store(mgr)
}

// WithNewSession executes the function with a new session.
func (mgr *TaskManager) WithNewSession(fn func(se sessionctx.Context) error) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePerThousand(); err != nil {
		return err
	}
	v, err := mgr.sePool.Get()
	if err != nil {
		return err
	}
	// when using global sort, the subtask meta might quite large as it include
	// filenames of all the generated kv/stat files.
	se := v.(sessionctx.Context)
	limitBak := se.GetSessionVars().TxnEntrySizeLimit
	defer func() {
		se.GetSessionVars().TxnEntrySizeLimit = limitBak
		mgr.sePool.Put(v)
	}()
	se.GetSessionVars().TxnEntrySizeLimit = vardef.TxnEntrySizeLimit.Load()
	return fn(se)
}

// WithNewTxn executes the fn in a new transaction.
func (mgr *TaskManager) WithNewTxn(ctx context.Context, fn func(se sessionctx.Context) error) error {
	ctx = clitutil.WithInternalSourceType(ctx, kv.InternalDistTask)
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
func (mgr *TaskManager) CreateTask(
	ctx context.Context,
	key string,
	tp proto.TaskType,
	keyspace string,
	requiredSlots int,
	targetScope string,
	maxNodeCnt int,
	extraParams proto.ExtraParams,
	meta []byte,
) (taskID int64, err error) {
	err = mgr.WithNewSession(func(se sessionctx.Context) error {
		var err2 error
		taskID, err2 = mgr.CreateTaskWithSession(ctx, se, key, tp, keyspace, requiredSlots, targetScope, maxNodeCnt, extraParams, meta)
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
	keyspace string,
	requiredSlots int,
	targetScope string,
	maxNodeCount int,
	extraParams proto.ExtraParams,
	meta []byte,
) (taskID int64, err error) {
	cpuCount, err := mgr.getCPUCountOfNodeByRole(ctx, se, "", true)
	if err != nil {
		return 0, err
	}
	if requiredSlots > cpuCount {
		return 0, errors.Errorf("task required slots(%d) larger than cpu count(%d) of managed node", requiredSlots, cpuCount)
	}
	extraParamBytes, err := json.Marshal(extraParams)
	if err != nil {
		return 0, errors.Trace(err)
	}
	_, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			insert into mysql.tidb_global_task(`+InsertTaskColumns+`)
			values (%?, %?, %?, %?, %?, %?, %?, CURRENT_TIMESTAMP(), %?, %?, %?, %?)`,
		key, tp, proto.TaskStatePending, proto.NormalPriority, requiredSlots,
		proto.StepInit, meta, targetScope, maxNodeCount, json.RawMessage(extraParamBytes), keyspace)
	if err != nil {
		return 0, err
	}

	rs, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), "select @@last_insert_id")
	if err != nil {
		return 0, err
	}

	taskID = int64(rs[0].GetUint64(0))
	if _, _err_ := failpoint.Eval(_curpkg_("testSetLastTaskID")); _err_ == nil {
		TestLastTaskID.Store(taskID)
	}

	return taskID, nil
}

// GetTopUnfinishedTasks implements the scheduler.TaskManager interface.
func (mgr *TaskManager) GetTopUnfinishedTasks(ctx context.Context) ([]*proto.TaskBase, error) {
	return mgr.getTopTasks(ctx,
		proto.TaskStatePending,
		proto.TaskStateRunning,
		proto.TaskStateReverting,
		proto.TaskStateCancelling,
		proto.TaskStatePausing,
		proto.TaskStateResuming,
		proto.TaskStateModifying,
	)
}

// GetTopNoNeedResourceTasks implements the scheduler.TaskManager interface.
func (mgr *TaskManager) GetTopNoNeedResourceTasks(ctx context.Context) ([]*proto.TaskBase, error) {
	return mgr.getTopTasks(ctx,
		proto.TaskStateReverting,
		proto.TaskStateCancelling,
		proto.TaskStatePausing,
		proto.TaskStateModifying,
	)
}

func (mgr *TaskManager) getTopTasks(ctx context.Context, states ...proto.TaskState) ([]*proto.TaskBase, error) {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
	var holders strings.Builder
	for i := range states {
		if i > 0 {
			holders.WriteString(",")
		}
		holders.WriteString("%?")
	}
	sql := fmt.Sprintf(`select %s from mysql.tidb_global_task t
		where state in (%s)
		order by priority asc, create_time asc, id asc
		limit %%?`, basicTaskColumns, holders.String())
	args := make([]any, 0, len(states)+1)
	for _, s := range states {
		args = append(args, s)
	}
	args = append(args, proto.MaxConcurrentTask*2)
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, sql, args...)
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
	r := tracing.StartRegion(ctx, "TaskManager.GetTaskExecInfoByExecID")
	defer r.End()
	var res []*TaskExecInfo
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
	err := mgr.WithNewSession(func(se sessionctx.Context) error {
		// as the task will not go into next step when there are subtasks in those
		// states, so their steps will be current step of their corresponding task,
		// so we don't need to query by step here.
		rs, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			`select st.task_key, max(st.concurrency) from mysql.tidb_background_subtask st
			where st.exec_id = %? and st.state in (%?, %?)
			group by st.task_key`,
			execID, proto.SubtaskStatePending, proto.SubtaskStateRunning)
		if err != nil {
			return err
		}
		if len(rs) == 0 {
			return nil
		}
		maxSubtaskCon := make(map[int64]int, len(rs))
		var taskIDsBuf strings.Builder
		for i, r := range rs {
			taskIDStr := r.GetString(0)
			taskID, err2 := strconv.ParseInt(taskIDStr, 10, 0)
			if err2 != nil {
				return errors.Trace(err2)
			}
			maxSubtaskCon[taskID] = int(r.GetInt64(1))
			if i > 0 {
				taskIDsBuf.WriteString(",")
			}
			taskIDsBuf.WriteString(taskIDStr)
		}
		rs, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(),
			`select `+basicTaskColumns+` from mysql.tidb_global_task t
			where t.id in (`+taskIDsBuf.String()+`) and t.state in (%?, %?, %?)
			order by priority asc, create_time asc, id asc`,
			proto.TaskStateRunning, proto.TaskStateReverting, proto.TaskStatePausing)
		if err != nil {
			return err
		}
		res = make([]*TaskExecInfo, 0, len(rs))
		for _, r := range rs {
			taskBase := row2TaskBasic(r)
			res = append(res, &TaskExecInfo{
				TaskBase:           taskBase,
				SubtaskConcurrency: maxSubtaskCon[taskBase.ID],
			})
		}
		return nil
	})
	return res, err
}

// GetTasksInStates gets the tasks in the states(order by priority asc, create_time acs, id asc).
func (mgr *TaskManager) GetTasksInStates(ctx context.Context, states ...any) (task []*proto.Task, err error) {
	if len(states) == 0 {
		return task, nil
	}

	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
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

// GetTaskBasesInStates gets the task bases in the states(order by priority asc, create_time acs, id asc).
func (mgr *TaskManager) GetTaskBasesInStates(ctx context.Context, states ...any) (task []*proto.TaskBase, err error) {
	if len(states) == 0 {
		return task, nil
	}

	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		"select "+basicTaskColumns+" from mysql.tidb_global_task t "+
			"where state in ("+strings.Repeat("%?,", len(states)-1)+"%?)"+
			" order by priority asc, create_time asc, id asc", states...)
	if err != nil {
		return task, err
	}

	for _, r := range rs {
		task = append(task, row2TaskBasic(r))
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
	err = mgr.WithNewSession(func(se sessionctx.Context) error {
		var err2 error
		task, err2 = mgr.getTaskBaseByID(ctx, se.GetSQLExecutor(), taskID)
		return err2
	})
	return
}

func (*TaskManager) getTaskBaseByID(ctx context.Context, exec sqlexec.SQLExecutor, taskID int64) (task *proto.TaskBase, err error) {
	rs, err := sqlexec.ExecSQL(ctx, exec, "select "+basicTaskColumns+" from mysql.tidb_global_task t where id = %?", taskID)
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
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

// GetAllSubtaskSummaryByStep gets the subtask summaries by step
// Since it's only used for running jobs, we don't need to read from history table.
func (mgr *TaskManager) GetAllSubtaskSummaryByStep(
	ctx context.Context, taskID int64, step proto.Step,
) ([]*execute.SubtaskSummary, error) {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		`select summary from mysql.tidb_background_subtask
		where task_key = %? and step = %?`,
		taskID, step)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}
	summaries := make([]*execute.SubtaskSummary, 0, len(rs))
	for _, r := range rs {
		summary := &execute.SubtaskSummary{}
		if err := json.Unmarshal(hack.Slice(r.GetJSON(0).String()), summary); err != nil {
			return nil, errors.Trace(err)
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

// GetSubtaskRowCount gets the subtask row count.
func (mgr *TaskManager) GetSubtaskRowCount(ctx context.Context, taskID int64, step proto.Step) (int64, error) {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return 0, err
	}
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `select
    	cast(sum(json_extract(summary, '$.row_count')) as signed) as row_count
		from (
			select summary from mysql.tidb_background_subtask where task_key = %? and step = %?
			union all
			select summary from mysql.tidb_background_subtask_history where task_key = %? and step = %?
		) as combined`,
		taskID, step, taskID, step)
	if err != nil {
		return 0, err
	}
	if len(rs) == 0 {
		return 0, nil
	}
	return rs[0].GetInt64(0), nil
}

// UpdateSubtaskSummary updates the subtask summary.
func (mgr *TaskManager) UpdateSubtaskSummary(ctx context.Context, subtaskID int64, summary *execute.SubtaskSummary) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}

	summaryBytes, err := json.Marshal(summary)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = mgr.ExecuteSQLWithNewSession(ctx,
		`update mysql.tidb_background_subtask set summary = %? where id = %?`,
		hack.String(summaryBytes), subtaskID)
	return err
}

// GetSubtaskCntGroupByStates gets the subtask count by states.
func (mgr *TaskManager) GetSubtaskCntGroupByStates(ctx context.Context, taskID int64, step proto.Step) (map[proto.SubtaskState]int64, error) {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
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

// UpdateSubtasksExecIDs update subtasks' execID.
func (mgr *TaskManager) UpdateSubtasksExecIDs(ctx context.Context, subtasks []*proto.SubtaskBase) error {
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	return mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		vars := se.GetSessionVars()
		if vars.MemQuotaQuery < vardef.DefTiDBMemQuotaQuery {
			bak := vars.MemQuotaQuery
			if err := vars.SetSystemVar(vardef.TiDBMemQuotaQuery,
				strconv.Itoa(vardef.DefTiDBMemQuotaQuery)); err != nil {
				return err
			}
			defer func() {
				_ = vars.SetSystemVar(vardef.TiDBMemQuotaQuery, strconv.Itoa(int(bak)))
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
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
	if _, _err_ := failpoint.Eval(_curpkg_("waitBeforeInsertSubtasks")); _err_ == nil {
		<-TestChannel
		<-TestChannel
	}
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
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

func serializeErr(inErr error) []byte {
	if inErr == nil {
		return nil
	}
	var tErr *errors.Error
	if goerrors.As(inErr, &tErr) {
		// we want to keep the original message to have more context info
		tErr = errors.Normalize(errors.GetErrStackMsg(inErr), errors.RFCCodeText(string(tErr.RFCCode())),
			errors.MySQLErrorCode(int(tErr.Code())))
	} else {
		tErr = errors.Normalize(inErr.Error())
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
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

// GetAllTasks gets all tasks with basic columns.
func (mgr *TaskManager) GetAllTasks(ctx context.Context) ([]*proto.TaskBase, error) {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `select `+basicTaskColumns+` from mysql.tidb_global_task t`)
	if err != nil {
		return nil, err
	}
	if len(rs) == 0 {
		return nil, nil
	}
	tasks := make([]*proto.TaskBase, 0, len(rs))
	for _, r := range rs {
		tasks = append(tasks, row2TaskBasic(r))
	}
	return tasks, nil
}

// GetAllSubtasks gets all subtasks with basic columns.
func (mgr *TaskManager) GetAllSubtasks(ctx context.Context) ([]*proto.SubtaskBase, error) {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
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
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	cpuCount, err := mgr.getCPUCountOfNodeByRole(ctx, se, "", true)
	if err != nil {
		return err
	}
	sql := "update mysql.tidb_global_task set concurrency = %? where concurrency > %?;"
	_, err = sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), sql, cpuCount, cpuCount)
	return err
}

// UpdateSubtaskCheckpoint updates the checkpoint of a subtask.
func (mgr *TaskManager) UpdateSubtaskCheckpoint(ctx context.Context, subtaskID int64, checkpoint any) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}

	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	checkpointJSON := string(data)

	_, err = mgr.ExecuteSQLWithNewSession(ctx,
		`UPDATE mysql.tidb_background_subtask SET checkpoint = %? WHERE id = %?`,
		checkpointJSON, subtaskID)
	return err
}

// GetSubtaskCheckpoint gets the checkpoint of a subtask.
func (mgr *TaskManager) GetSubtaskCheckpoint(ctx context.Context, subtaskID int64) (string, error) {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return "", err
	}

	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		"SELECT checkpoint FROM mysql.tidb_background_subtask WHERE id = %?", subtaskID)
	if err != nil {
		return "", err
	}
	if len(rs) == 0 || rs[0].IsNull(0) {
		return "", nil
	}

	return rs[0].GetString(0), nil
}
