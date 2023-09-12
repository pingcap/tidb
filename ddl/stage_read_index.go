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
	"encoding/hex"
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/disttask/operator"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type readIndexStage struct {
	d     *ddl
	job   *model.Job
	index *model.IndexInfo
	ptbl  table.PhysicalTable
	jc    *JobContext

	cloudStorageURI string

	bc      ingest.BackendCtx
	summary *execute.Summary

	subtaskSummary sync.Map // subtaskID => readIndexSummary
}

type readIndexSummary struct {
	minKey    []byte
	maxKey    []byte
	totalSize uint64
	dataFiles []string
	statFiles []string
	mu        sync.Mutex
}

func newReadIndexStage(
	d *ddl,
	job *model.Job,
	index *model.IndexInfo,
	ptbl table.PhysicalTable,
	jc *JobContext,
	bc ingest.BackendCtx,
	summary *execute.Summary,
	cloudStorageURI string,
) *readIndexStage {
	return &readIndexStage{
		d:               d,
		job:             job,
		index:           index,
		ptbl:            ptbl,
		jc:              jc,
		bc:              bc,
		summary:         summary,
		cloudStorageURI: cloudStorageURI,
	}
}

func (*readIndexStage) Init(_ context.Context) error {
	logutil.BgLogger().Info("read index stage init subtask exec env",
		zap.String("category", "ddl"))
	return nil
}

func (r *readIndexStage) SplitSubtask(ctx context.Context, subtask *proto.Subtask) ([]proto.MinimalTask, error) {
	logutil.BgLogger().Info("read index stage run subtask",
		zap.String("category", "ddl"))

	r.subtaskSummary.Store(subtask.ID, &readIndexSummary{})

	d := r.d
	sm := &BackfillSubTaskMeta{}
	err := json.Unmarshal(subtask.Meta, sm)
	if err != nil {
		logutil.BgLogger().Error("unmarshal error",
			zap.String("category", "ddl"),
			zap.Error(err))
		return nil, err
	}

	startKey, endKey, tbl, err := r.getTableStartEndKey(sm)
	if err != nil {
		return nil, err
	}

	sessCtx, err := newSessCtx(
		d.store, r.job.ReorgMeta.SQLMode, r.job.ReorgMeta.Location, r.job.ReorgMeta.ResourceGroupName)
	if err != nil {
		return nil, err
	}

	opCtx := NewOperatorCtx(ctx)
	defer opCtx.Cancel()
	totalRowCount := &atomic.Int64{}

	var pipe *operator.AsyncPipeline
	if len(r.cloudStorageURI) > 0 {
		pipe, err = r.buildExternalStorePipeline(opCtx, d, subtask.ID, sessCtx, tbl, startKey, endKey, totalRowCount)
	} else {
		pipe, err = r.buildLocalStorePipeline(opCtx, d, sessCtx, tbl, startKey, endKey, totalRowCount)
	}
	if err != nil {
		return nil, err
	}

	err = pipe.Execute()
	if err != nil {
		return nil, err
	}
	err = pipe.Close()
	if opCtx.OperatorErr() != nil {
		return nil, opCtx.OperatorErr()
	}
	if err != nil {
		return nil, err
	}

	r.summary.UpdateRowCount(subtask.ID, totalRowCount.Load())
	return nil, nil
}

func (r *readIndexStage) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("read index stage cleanup subtask exec env",
		zap.String("category", "ddl"))
	if _, ok := r.ptbl.(table.PartitionedTable); ok {
		ingest.LitBackCtxMgr.Unregister(r.job.ID)
	}
	return nil
}

// MockDMLExecutionAddIndexSubTaskFinish is used to mock DML execution during distributed add index.
var MockDMLExecutionAddIndexSubTaskFinish func()

func (r *readIndexStage) OnFinished(ctx context.Context, subtask *proto.Subtask) error {
	failpoint.Inject("mockDMLExecutionAddIndexSubTaskFinish", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecutionAddIndexSubTaskFinish != nil {
			MockDMLExecutionAddIndexSubTaskFinish()
		}
	})
	if len(r.cloudStorageURI) == 0 {
		return nil
	}
	// Rewrite the subtask meta to record statistics.
	var subtaskMeta BackfillSubTaskMeta
	err := json.Unmarshal(subtask.Meta, &subtaskMeta)
	if err != nil {
		return err
	}
	sum, _ := r.subtaskSummary.LoadAndDelete(subtask.ID)
	s := sum.(*readIndexSummary)
	subtaskMeta.MinKey = s.minKey
	subtaskMeta.MaxKey = s.maxKey
	subtaskMeta.TotalKVSize = s.totalSize
	subtaskMeta.DataFiles = s.dataFiles
	subtaskMeta.StatFiles = s.statFiles
	logutil.Logger(ctx).Info("get key boundary on subtask finished",
		zap.String("min", hex.EncodeToString(s.minKey)),
		zap.String("max", hex.EncodeToString(s.maxKey)),
		zap.Int("fileCount", len(s.dataFiles)),
		zap.Uint64("totalSize", s.totalSize))
	meta, err := json.Marshal(subtaskMeta)
	if err != nil {
		return err
	}
	subtask.Meta = meta
	return nil
}

func (r *readIndexStage) Rollback(ctx context.Context) error {
	logutil.Logger(ctx).Info("read index stage rollback backfill add index task",
		zap.String("category", "ddl"), zap.Int64("jobID", r.job.ID))
	ingest.LitBackCtxMgr.Unregister(r.job.ID)
	return nil
}

func (r *readIndexStage) getTableStartEndKey(sm *BackfillSubTaskMeta) (
	start, end kv.Key, tbl table.PhysicalTable, err error) {
	currentVer, err1 := getValidCurrentVersion(r.d.store)
	if err1 != nil {
		return nil, nil, nil, errors.Trace(err1)
	}
	if parTbl, ok := r.ptbl.(table.PartitionedTable); ok {
		pid := sm.PhysicalTableID
		start, end, err = getTableRange(r.jc, r.d.ddlCtx, parTbl.GetPartition(pid), currentVer.Ver, r.job.Priority)
		if err != nil {
			logutil.BgLogger().Error("get table range error",
				zap.String("category", "ddl"),
				zap.Error(err))
			return nil, nil, nil, err
		}
		tbl = parTbl.GetPartition(pid)
	} else {
		start, end = sm.StartKey, sm.EndKey
		tbl = r.ptbl
	}
	return start, end, tbl, nil
}

func (r *readIndexStage) buildLocalStorePipeline(
	opCtx *OperatorCtx,
	d *ddl,
	sessCtx sessionctx.Context,
	tbl table.PhysicalTable,
	start, end kv.Key,
	totalRowCount *atomic.Int64,
) (*operator.AsyncPipeline, error) {
	ei, err := r.bc.Register(r.job.ID, r.index.ID, r.job.SchemaName, r.job.TableName)
	if err != nil {
		logutil.Logger(opCtx).Warn("cannot register new engine", zap.Error(err),
			zap.Int64("job ID", r.job.ID), zap.Int64("index ID", r.index.ID))
		return nil, err
	}
	counter := metrics.BackfillTotalCounter.WithLabelValues(
		metrics.GenerateReorgLabel("add_idx_rate", r.job.SchemaName, tbl.Meta().Name.O))
	return NewAddIndexIngestPipeline(
		opCtx, d.store, d.sessPool, r.bc, ei, sessCtx, tbl, r.index, start, end, totalRowCount, counter)
}

func (r *readIndexStage) buildExternalStorePipeline(
	opCtx *OperatorCtx,
	d *ddl,
	subtaskID int64,
	sessCtx sessionctx.Context,
	tbl table.PhysicalTable,
	start, end kv.Key,
	totalRowCount *atomic.Int64,
) (*operator.AsyncPipeline, error) {
	onClose := func(summary *external.WriterSummary) {
		sum, _ := r.subtaskSummary.Load(subtaskID)
		s := sum.(*readIndexSummary)
		s.mu.Lock()
		if len(s.minKey) == 0 || summary.Min.Cmp(s.minKey) < 0 {
			s.minKey = summary.Min.Clone()
		}
		if len(s.maxKey) == 0 || summary.Max.Cmp(s.maxKey) > 0 {
			s.maxKey = summary.Max.Clone()
		}
		s.totalSize += summary.TotalSize
		for _, f := range summary.MultipleFilesStats {
			for _, filename := range f.Filenames {
				s.dataFiles = append(s.dataFiles, filename[0])
				s.statFiles = append(s.statFiles, filename[1])
			}
		}
		s.mu.Unlock()
	}
	return NewWriteIndexToExternalStoragePipeline(
		opCtx, d.store, r.cloudStorageURI, r.d.sessPool, sessCtx, r.job.ID, subtaskID,
		tbl, r.index, start, end, totalRowCount, onClose)
}
