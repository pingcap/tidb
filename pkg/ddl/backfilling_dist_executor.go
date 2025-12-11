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
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"go.uber.org/zap"
	"strings"
)

// Version constants for BackfillTaskMeta.
const (
	BackfillTaskMetaVersion0 = iota
	BackfillTaskMetaVersion1
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
	MergeTempIndex  bool   `json:"merge_temp_index"`

	Version int `json:"version,omitempty"`
}

// BackfillSubTaskMeta is the sub-task meta for backfilling index.
type BackfillSubTaskMeta struct {
	external.BaseExternalMeta

	PhysicalTableID int64 `json:"physical_table_id"`

	// Used by read index step.
	RowStart []byte `json:"row_start"`
	RowEnd   []byte `json:"row_end"`

	// Used by global sort write & ingest step.
	RangeJobKeys   [][]byte `json:"range_job_keys,omitempty" external:"true"`
	RangeSplitKeys [][]byte `json:"range_split_keys,omitempty" external:"true"`
	DataFiles      []string `json:"data-files,omitempty" external:"true"`
	StatFiles      []string `json:"stat-files,omitempty" external:"true"`
	// TS is used to make sure subtasks are idempotent.
	// TODO(tangenta): support local sort.
	TS uint64 `json:"ts,omitempty"`
	// Each group of MetaGroups represents a different index kvs meta.
	MetaGroups []*external.SortedKVMeta `json:"meta_groups,omitempty" external:"true"`
	// EleIDs stands for the index/column IDs to backfill with distributed framework.
	// After the subtask is finished, EleIDs should have the same length as
	// MetaGroups, and they are in the same order.
	EleIDs []int64 `json:"ele_ids,omitempty" external:"true"`

	// Only used for adding one single index.
	// Keep this for compatibility with v7.5.
	external.SortedKVMeta `json:",inline" external:"true"`
}

// Marshal marshals the backfill subtask meta to JSON.
func (m *BackfillSubTaskMeta) Marshal() ([]byte, error) {
	return m.BaseExternalMeta.Marshal(m)
}

func decodeBackfillSubTaskMeta(ctx context.Context, extStore storage.ExternalStorage, raw []byte) (*BackfillSubTaskMeta, error) {
	var subtask BackfillSubTaskMeta
	err := json.Unmarshal(raw, &subtask)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if extStore != nil && subtask.ExternalPath != "" {
		// read external meta to storage when using global sort
		if err := subtask.ReadJSONFromExternalStorage(ctx, extStore, &subtask); err != nil {
			return nil, errors.Trace(err)
		}
		for _, g := range subtask.MetaGroups {
			var multipleFiles []external.MultipleFilesStat
			for _, f := range g.MultipleFilesStats {
				include := true
				for _, fileName := range f.Filenames {
					if strings.Contains(fileName[0], disttaskutil.StatsSampled) {
						include = false
						break
					}
				}
				if include {
					multipleFiles = append(multipleFiles, f)
				}
			}
			g.MultipleFilesStats = multipleFiles
		}
	}
	logutil.DDLLogger().Info("decode backfill subtask meta from external storage", zap.Any("subtask", subtask))

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

func writeExternalBackfillSubTaskMeta(ctx context.Context, extStore storage.ExternalStorage, subtask *BackfillSubTaskMeta, externalPath string) error {
	if extStore == nil {
		return nil
	}
	subtask.ExternalPath = externalPath
	return subtask.WriteJSONToExternalStorage(ctx, extStore, subtask)
}

func (s *backfillDistExecutor) newBackfillStepExecutor(
	stage proto.Step,
) (execute.StepExecutor, error) {
	jobMeta := &s.taskMeta.Job
	ddlObj := s.d

	store := ddlObj.store
	sessPool := ddlObj.sessPool
	taskKS := s.task.Keyspace
	if ddlObj.store.GetKeyspace() != taskKS {
		var err error
		err = s.GetTaskTable().WithNewSession(func(se sessionctx.Context) error {
			svr := se.GetSQLServer()
			store, err = svr.GetKSStore(taskKS)
			if err != nil {
				return err
			}
			sp, err := svr.GetKSSessPool(taskKS)
			sessPool = sess.NewSessionPool(sp)
			return err
		})
		if err != nil {
			return nil, err
		}
	}
	// TODO getTableByTxn is using DDL ctx which is never cancelled except when shutdown.
	// we should move this operation out of GetStepExecutor, and put into Init.
	_, tblIface, err := getTableByTxn(ddlObj.ctx, store, jobMeta.SchemaID, jobMeta.TableID)
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
		return newReadIndexExecutor(store, sessPool, ddlObj.etcdCli, jobMeta, indexInfos, tbl, jc, cloudStorageURI, estRowSize)
	case proto.BackfillStepMergeSort:
		return newMergeSortExecutor(store, jobMeta.ID, indexInfos, tbl, cloudStorageURI)
	case proto.BackfillStepWriteAndIngest:
		if len(cloudStorageURI) == 0 {
			return nil, errors.Errorf("local import does not have write & ingest step")
		}
		return newCloudImportExecutor(jobMeta, store, indexInfos, tbl, cloudStorageURI, s.GetTaskBase().Concurrency)
	case proto.BackfillStepMergeTempIndex:
		return newMergeTempIndexExecutor(jobMeta, store, tbl)
	default:
		// should not happen, caller has checked the stage
		return nil, errors.Errorf("unknown step %d for job %d", stage, jobMeta.ID)
	}
}

type backfillDistExecutor struct {
	*taskexecutor.BaseTaskExecutor
	d        *ddl
	task     *proto.Task
	taskMeta *BackfillTaskMeta
}

func newBackfillDistExecutor(
	ctx context.Context,
	task *proto.Task,
	param taskexecutor.Param,
	d *ddl) taskexecutor.TaskExecutor {
	s := &backfillDistExecutor{
		BaseTaskExecutor: taskexecutor.NewBaseTaskExecutor(ctx, task, param),
		d:                d,
		task:             task,
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
	case proto.BackfillStepReadIndex,
		proto.BackfillStepMergeSort,
		proto.BackfillStepWriteAndIngest,
		proto.BackfillStepMergeTempIndex:
		return s.newBackfillStepExecutor(task.Step)
	default:
		return nil, errors.Errorf("unknown backfill step %d for task %d", task.Step, task.ID)
	}
}

func (*backfillDistExecutor) IsIdempotent(*proto.Subtask) bool {
	return true
}

func (*backfillDistExecutor) IsRetryableError(err error) bool {
	return common.IsRetryableError(err) || isRetryableError(err)
}

func (s *backfillDistExecutor) Close() {
	s.BaseTaskExecutor.Close()
}
