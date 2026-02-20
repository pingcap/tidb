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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/injectfailpoint"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)
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
