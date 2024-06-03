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
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// BackfillTaskMeta is the dist task meta for backfilling index.
type BackfillTaskMeta struct {
	Job model.Job `json:"job"`
	// EleIDs stands for the index/column IDs to backfill with distributed framework.
	EleIDs []int64 `json:"ele_ids"`
	// EleTypeKey is the type of the element to backfill with distributed framework.
	// For now, only index type is supported.
	EleTypeKey []byte `json:"ele_type_key"`

	CloudStorageURI string `json:"cloud_storage_uri"`
	EstimateRowSize int    `json:"estimate_row_size"`
}

// BackfillSubTaskMeta is the sub-task meta for backfilling index.
type BackfillSubTaskMeta struct {
	PhysicalTableID int64 `json:"physical_table_id"`

	// Used by read index step.
	RowStart []byte `json:"row_start"`
	RowEnd   []byte `json:"row_end"`

	// Used by global sort write & ingest step.
	RangeSplitKeys [][]byte `json:"range_split_keys,omitempty"`
	DataFiles      []string `json:"data-files,omitempty"`
	StatFiles      []string `json:"stat-files,omitempty"`
	TS             uint64   `json:"ts,omitempty"`
	// Each group of MetaGroups represents a different index kvs meta.
	MetaGroups []*external.SortedKVMeta `json:"meta_groups,omitempty"`
	// Only used for adding one single index.
	// Keep this for compatibility with v7.5.
	external.SortedKVMeta `json:",inline"`
}

func decodeBackfillSubTaskMeta(raw []byte) (*BackfillSubTaskMeta, error) {
	var subtask BackfillSubTaskMeta
	err := json.Unmarshal(raw, &subtask)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// For compatibility with old version TiDB.
	if len(subtask.RowStart) == 0 {
		subtask.RowStart = subtask.SortedKVMeta.StartKey
		subtask.RowEnd = subtask.SortedKVMeta.EndKey
	}
	if len(subtask.MetaGroups) == 0 {
		m := subtask.SortedKVMeta
		subtask.MetaGroups = []*external.SortedKVMeta{&m}
	}
	return &subtask, nil
}

func (s *backfillDistExecutor) newBackfillSubtaskExecutor(
	stage proto.Step,
) (execute.StepExecutor, error) {
	jobMeta := &s.taskMeta.Job
	ddlObj := s.d

	// TODO getTableByTxn is using DDL ctx which is never cancelled except when shutdown.
	// we should move this operation out of GetStepExecutor, and put into Init.
	_, tblIface, err := ddlObj.getTableByTxn((*asAutoIDRequirement)(ddlObj.ddlCtx), jobMeta.SchemaID, jobMeta.TableID)
	if err != nil {
		return nil, err
	}
	tbl := tblIface.(table.PhysicalTable)
	eleIDs := s.taskMeta.EleIDs
	indexInfos := make([]*model.IndexInfo, 0, len(eleIDs))
	for _, eid := range eleIDs {
		indexInfo := model.FindIndexInfoByID(tbl.Meta().Indices, eid)
		if indexInfo == nil {
			logutil.DDLIngestLogger().Warn("index info not found",
				zap.Int64("table ID", tbl.Meta().ID),
				zap.Int64("index ID", eid))
			return nil, errors.Errorf("index info not found: %d", eid)
		}
		indexInfos = append(indexInfos, indexInfo)
	}
	cloudStorageURI := s.taskMeta.CloudStorageURI
	estRowSize := s.taskMeta.EstimateRowSize

	switch stage {
	case proto.BackfillStepReadIndex:
		jc := ddlObj.jobContext(jobMeta.ID, jobMeta.ReorgMeta)
		ddlObj.setDDLLabelForTopSQL(jobMeta.ID, jobMeta.Query)
		ddlObj.setDDLSourceForDiagnosis(jobMeta.ID, jobMeta.Type)
		return newReadIndexExecutor(ddlObj, jobMeta, indexInfos, tbl, jc, s.getBackendCtx, cloudStorageURI, estRowSize)
	case proto.BackfillStepMergeSort:
		return newMergeSortExecutor(jobMeta.ID, len(indexInfos), tbl, cloudStorageURI)
	case proto.BackfillStepWriteAndIngest:
		if len(cloudStorageURI) == 0 {
			return nil, errors.Errorf("local import does not have write & ingest step")
		}
		return newCloudImportExecutor(jobMeta, indexInfos[0], tbl, s.getBackendCtx, cloudStorageURI)
	default:
		// should not happen, caller has checked the stage
		return nil, errors.Errorf("unknown step %d for job %d", stage, jobMeta.ID)
	}
}

func (s *backfillDistExecutor) getBackendCtx() (ingest.BackendCtx, error) {
	job := &s.taskMeta.Job
	unique, err := decodeIndexUniqueness(job)
	if err != nil {
		return nil, err
	}
	ddlObj := s.d
	discovery := ddlObj.store.(tikv.Storage).GetRegionCache().PDClient().GetServiceDiscovery()

	return ingest.LitBackCtxMgr.Register(s.BaseTaskExecutor.Ctx(), job.ID, unique, ddlObj.etcdCli, discovery, job.ReorgMeta.ResourceGroupName)
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

type backfillDistExecutor struct {
	*taskexecutor.BaseTaskExecutor
	d         *ddl
	task      *proto.Task
	taskTable taskexecutor.TaskTable
	taskMeta  *BackfillTaskMeta
	jobID     int64
}

func newBackfillDistExecutor(ctx context.Context, id string, task *proto.Task, taskTable taskexecutor.TaskTable, d *ddl) taskexecutor.TaskExecutor {
	s := &backfillDistExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, id, task, taskTable),
		d:                d,
		task:             task,
		taskTable:        taskTable,
	}
	s.BaseTaskExecutor.Extension = s
	return s
}

func (s *backfillDistExecutor) Init(ctx context.Context) error {
	err := s.BaseTaskExecutor.Init(ctx)
	if err != nil {
		return err
	}

	bgm := &BackfillTaskMeta{}
	err = json.Unmarshal(s.task.Meta, bgm)
	if err != nil {
		return errors.Trace(err)
	}

	s.taskMeta = bgm
	return nil
}

func (s *backfillDistExecutor) GetStepExecutor(task *proto.Task) (execute.StepExecutor, error) {
	switch task.Step {
	case proto.BackfillStepReadIndex, proto.BackfillStepMergeSort, proto.BackfillStepWriteAndIngest:
		return s.newBackfillSubtaskExecutor(task.Step)
	default:
		return nil, errors.Errorf("unknown backfill step %d for task %d", task.Step, task.ID)
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

func (s *backfillDistExecutor) Close() {
	s.BaseTaskExecutor.Close()
}
