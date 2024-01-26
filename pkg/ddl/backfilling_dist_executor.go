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

package ddl

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type backfillDistExecutor struct {
	*taskexecutor.BaseTaskExecutor
	d            *ddl
	task         *proto.Task
	taskTable    taskexecutor.TaskTable
	jobID        int64
	unique       bool
	pdLeaderAddr string
}

func newBackfillDistExecutor(ctx context.Context, id string, task *proto.Task, taskTable taskexecutor.TaskTable, d *ddl) taskexecutor.TaskExecutor {
	e := &backfillDistExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, id, task, taskTable),
		d:                d,
		task:             task,
		taskTable:        taskTable,
	}
	e.BaseTaskExecutor.Extension = e
	return e
}

func (e *backfillDistExecutor) Init(ctx context.Context) error {
	err := e.BaseTaskExecutor.Init(ctx)
	if err != nil {
		return err
	}
	bgm := &BackfillTaskMeta{}
	err = json.Unmarshal(e.task.Meta, bgm)
	if err != nil {
		return errors.Trace(err)
	}
	job := &bgm.Job
	e.unique, err = decodeIndexUniqueness(job)
	if err != nil {
		return err
	}
	e.pdLeaderAddr = e.d.store.(tikv.Storage).GetRegionCache().PDClient().GetLeaderAddr()
	e.jobID = job.ID
	return nil
}

func decodeIndexUniqueness(job *model.Job) (bool, error) {
	unique := make([]bool, 1)
	err := job.DecodeArgs(&unique[0])
	if err != nil {
		err = job.DecodeArgs(&unique)
	}
	if err != nil {
		return false, errors.Trace(err)
	}
	// We only support adding multiple unique indexes or multiple non-unique indexes,
	// we use the first index uniqueness here.
	return unique[0], nil
}

func (e *backfillDistExecutor) GetStepExecutor(ctx context.Context, task *proto.Task, summary *execute.Summary, _ *proto.StepResource) (execute.StepExecutor, error) {
	switch task.Step {
	case proto.BackfillStepReadIndex, proto.BackfillStepMergeSort, proto.BackfillStepWriteAndIngest:
		return e.newBackfillSubtaskExecutor(ctx, task.Meta, task.Step, summary)
	default:
		return nil, errors.Errorf("unknown backfill step %d for task %d", task.Step, task.ID)
	}
}

// newBackfillSubtaskExecutor creates a new backfill subtask executor.
func (e *backfillDistExecutor) newBackfillSubtaskExecutor(_ context.Context, taskMeta []byte, stage proto.Step, summary *execute.Summary) (execute.StepExecutor, error) {
	bgm := &BackfillTaskMeta{}
	err := json.Unmarshal(taskMeta, bgm)
	if err != nil {
		return nil, err
	}
	jobMeta := &bgm.Job

	_, tbl, err := e.d.getTableByTxn((*asAutoIDRequirement)(e.d.ddlCtx), jobMeta.SchemaID, jobMeta.TableID)
	if err != nil {
		return nil, err
	}
	indexInfos := make([]*model.IndexInfo, 0, len(bgm.EleIDs))
	for _, eid := range bgm.EleIDs {
		indexInfo := model.FindIndexInfoByID(tbl.Meta().Indices, eid)
		if indexInfo == nil {
			logutil.BgLogger().Warn("index info not found", zap.String("category", "ddl-ingest"),
				zap.Int64("table ID", tbl.Meta().ID), zap.Int64("index ID", eid))
			return nil, errors.Errorf("index info not found: %d", eid)
		}
		indexInfos = append(indexInfos, indexInfo)
	}

	switch stage {
	case proto.BackfillStepReadIndex:
		jc := e.d.jobContext(jobMeta.ID, jobMeta.ReorgMeta)
		e.d.setDDLLabelForTopSQL(jobMeta.ID, jobMeta.Query)
		e.d.setDDLSourceForDiagnosis(jobMeta.ID, jobMeta.Type)
		return newReadIndexExecutor(
			e.d, &bgm.Job, indexInfos, tbl.(table.PhysicalTable), jc, e.unique, e.pdLeaderAddr, summary, bgm.CloudStorageURI), nil
	case proto.BackfillStepMergeSort:
		return newMergeSortExecutor(jobMeta.ID, len(indexInfos), tbl.(table.PhysicalTable), bgm.CloudStorageURI)
	case proto.BackfillStepWriteAndIngest:
		if len(bgm.CloudStorageURI) > 0 {
			return newCloudImportExecutor(
				e.d, &bgm.Job, jobMeta.ID, indexInfos[0], tbl.(table.PhysicalTable), bgm.CloudStorageURI)
		}
		return nil, errors.Errorf("local import does not have write & ingest step")
	default:
		return nil, errors.Errorf("unknown step %d for job %d", stage, jobMeta.ID)
	}
}

func (*backfillDistExecutor) IsIdempotent(*proto.Subtask) bool {
	return true
}

func isRetryableError(err error) bool {
	originErr := errors.Cause(err)
	if tErr, ok := originErr.(*terror.Error); ok {
		sqlErr := terror.ToSQLError(tErr)
		_, ok := dbterror.ReorgRetryableErrCodes[sqlErr.Code]
		return ok
	}
	// can't retry Unknown err.
	return false
}

func (*backfillDistExecutor) IsRetryableError(err error) bool {
	return common.IsRetryableError(err) || isRetryableError(err)
}

func (e *backfillDistExecutor) Close() {
	e.BaseTaskExecutor.Close()
}
