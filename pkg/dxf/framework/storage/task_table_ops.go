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
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

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
	failpoint.Inject("waitBeforeInsertSubtasks", func() {
		<-TestChannel
		<-TestChannel
	})
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

// ActiveTaskSummary is the summary of active tasks in `mysql.tidb_global_task`.
type ActiveTaskSummary struct {
	Total       int64            `json:"total"`
	PerKeyspace map[string]int64 `json:"per_keyspace"`
}

// GetActiveTaskCountsByKeyspace gets active task summary grouped by keyspace.
func (mgr *TaskManager) GetActiveTaskCountsByKeyspace(ctx context.Context) (*ActiveTaskSummary, error) {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return nil, err
	}
	rs, err := mgr.ExecuteSQLWithNewSession(ctx,
		`select keyspace, count(1) from mysql.tidb_global_task group by keyspace`)
	if err != nil {
		return nil, err
	}
	summary := &ActiveTaskSummary{
		PerKeyspace: make(map[string]int64, len(rs)),
	}
	for _, r := range rs {
		keyspace := r.GetString(0)
		cnt := r.GetInt64(1)
		summary.Total += cnt
		summary.PerKeyspace[keyspace] = cnt
	}
	return summary, nil
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

// UpdateTaskExtraParams update the extra params of a task.
func (mgr *TaskManager) UpdateTaskExtraParams(ctx context.Context, taskID int64, extraParams proto.ExtraParams) error {
	if err := injectfailpoint.DXFRandomErrorWithOnePercent(); err != nil {
		return err
	}
	extraParamBytes, err := json.Marshal(&extraParams)
	if err != nil {
		return errors.Trace(err)
	}
	return mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
			update mysql.tidb_global_task
			set extra_params = %?
			where id = %?`, json.RawMessage(extraParamBytes), taskID)
		if err != nil {
			return err
		}
		return nil
	})
}
