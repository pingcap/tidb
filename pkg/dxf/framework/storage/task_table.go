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
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
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
	failpoint.InjectCall("beforeSubmitTask", &requiredSlots, &extraParams)
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
	failpoint.Inject("testSetLastTaskID", func() { TestLastTaskID.Store(taskID) })

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


