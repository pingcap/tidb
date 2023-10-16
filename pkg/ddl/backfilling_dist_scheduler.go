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
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// BackfillGlobalMeta is the global task meta for backfilling index.
type BackfillGlobalMeta struct {
	Job model.Job `json:"job"`
	// EleIDs stands for the index/column IDs to backfill with distributed framework.
	EleIDs []int64 `json:"ele_ids"`
	// EleTypeKey is the type of the element to backfill with distributed framework.
	// For now, only index type is supported.
	EleTypeKey []byte `json:"ele_type_key"`

	CloudStorageURI string `json:"cloud_storage_uri"`
}

// BackfillSubTaskMeta is the sub-task meta for backfilling index.
type BackfillSubTaskMeta struct {
	PhysicalTableID int64  `json:"physical_table_id"`
	StartKey        []byte `json:"start_key"`
	EndKey          []byte `json:"end_key"`

	RangeSplitKeys        [][]byte `json:"range_split_keys"`
	external.SortedKVMeta `json:",inline"`
}

// NewBackfillSubtaskExecutor creates a new backfill subtask executor.
func NewBackfillSubtaskExecutor(_ context.Context, taskMeta []byte, d *ddl,
	bc ingest.BackendCtx, stage proto.Step, summary *execute.Summary) (execute.SubtaskExecutor, error) {
	bgm := &BackfillGlobalMeta{}
	err := json.Unmarshal(taskMeta, bgm)
	if err != nil {
		return nil, err
	}
	jobMeta := &bgm.Job

	_, tbl, err := d.getTableByTxn(d.store, jobMeta.SchemaID, jobMeta.TableID)
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
	case proto.StepOne:
		jc := d.jobContext(jobMeta.ID, jobMeta.ReorgMeta)
		d.setDDLLabelForTopSQL(jobMeta.ID, jobMeta.Query)
		d.setDDLSourceForDiagnosis(jobMeta.ID, jobMeta.Type)
		return newReadIndexExecutor(
			d, &bgm.Job, indexInfos, tbl.(table.PhysicalTable), jc, bc, summary, bgm.CloudStorageURI), nil
	case proto.StepTwo:
		return newMergeSortExecutor(jobMeta.ID, indexInfos[0], tbl.(table.PhysicalTable), bc, bgm.CloudStorageURI)
	case proto.StepThree:
		if len(bgm.CloudStorageURI) > 0 {
			return newCloudImportExecutor(&bgm.Job, jobMeta.ID, indexInfos[0], tbl.(table.PhysicalTable), bc, bgm.CloudStorageURI)
		}
		return newImportFromLocalStepExecutor(jobMeta.ID, indexInfos, tbl.(table.PhysicalTable), bc), nil
	default:
		return nil, errors.Errorf("unknown step %d for job %d", stage, jobMeta.ID)
	}
}

type backfillDistScheduler struct {
	*scheduler.BaseScheduler
	d          *ddl
	task       *proto.Task
	taskTable  scheduler.TaskTable
	backendCtx ingest.BackendCtx
	jobID      int64
}

func newBackfillDistScheduler(ctx context.Context, id string, task *proto.Task, taskTable scheduler.TaskTable, d *ddl) scheduler.Scheduler {
	s := &backfillDistScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, id, task.ID, taskTable),
		d:             d,
		task:          task,
		taskTable:     taskTable,
	}
	s.BaseScheduler.Extension = s
	return s
}

func (s *backfillDistScheduler) Init(ctx context.Context) error {
	err := s.BaseScheduler.Init(ctx)
	if err != nil {
		return err
	}
	d := s.d

	bgm := &BackfillGlobalMeta{}
	err = json.Unmarshal(s.task.Meta, bgm)
	if err != nil {
		return errors.Trace(err)
	}
	job := &bgm.Job
	_, tbl, err := d.getTableByTxn(d.store, job.SchemaID, job.TableID)
	if err != nil {
		return errors.Trace(err)
	}
	// We only support adding multiple unique indexes or multiple non-unique indexes,
	// we use the first index uniqueness here.
	idx := model.FindIndexInfoByID(tbl.Meta().Indices, bgm.EleIDs[0])
	if idx == nil {
		return errors.Trace(errors.Errorf("index info not found: %d", bgm.EleIDs[0]))
	}
	bc, err := ingest.LitBackCtxMgr.Register(ctx, idx.Unique, job.ID, d.etcdCli, job.ReorgMeta.ResourceGroupName)
	if err != nil {
		return errors.Trace(err)
	}
	s.backendCtx = bc
	s.jobID = job.ID
	return nil
}

func (s *backfillDistScheduler) GetSubtaskExecutor(ctx context.Context, task *proto.Task, summary *execute.Summary) (execute.SubtaskExecutor, error) {
	switch task.Step {
	case proto.StepOne, proto.StepTwo, proto.StepThree:
		return NewBackfillSubtaskExecutor(ctx, task.Meta, s.d, s.backendCtx, task.Step, summary)
	default:
		return nil, errors.Errorf("unknown backfill step %d for task %d", task.Step, task.ID)
	}
}

func (*backfillDistScheduler) IsIdempotent(*proto.Subtask) bool {
	return true
}

func (s *backfillDistScheduler) Close() {
	if s.backendCtx != nil {
		ingest.LitBackCtxMgr.Unregister(s.jobID)
	}
	s.BaseScheduler.Close()
}
