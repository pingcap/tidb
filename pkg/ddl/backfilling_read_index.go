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
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type readIndexExecutor struct {
	d       *ddl
	job     *model.Job
	indexes []*model.IndexInfo
	ptbl    table.PhysicalTable
	jc      *JobContext

	cloudStorageURI string

	bc          ingest.BackendCtx
	curRowCount *atomic.Int64

	subtaskSummary sync.Map // subtaskID => readIndexSummary
}

type readIndexSummary struct {
	metaGroups []*external.SortedKVMeta
	mu         sync.Mutex
}

func newReadIndexExecutor(
	d *ddl,
	job *model.Job,
	indexes []*model.IndexInfo,
	ptbl table.PhysicalTable,
	jc *JobContext,
	bcGetter func() (ingest.BackendCtx, error),
	cloudStorageURI string,
) (*readIndexExecutor, error) {
	bc, err := bcGetter()
	if err != nil {
		return nil, err
	}
	return &readIndexExecutor{
		d:               d,
		job:             job,
		indexes:         indexes,
		ptbl:            ptbl,
		jc:              jc,
		bc:              bc,
		cloudStorageURI: cloudStorageURI,
		curRowCount:     &atomic.Int64{},
	}, nil
}

func (*readIndexExecutor) Init(_ context.Context) error {
	logutil.BgLogger().Info("read index executor init subtask exec env",
		zap.String("category", "ddl"))
	return nil
}

func (r *readIndexExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.BgLogger().Info("read index executor run subtask",
		zap.String("category", "ddl"),
		zap.Bool("use cloud", len(r.cloudStorageURI) > 0))

	r.subtaskSummary.Store(subtask.ID, &readIndexSummary{
		metaGroups: make([]*external.SortedKVMeta, len(r.indexes)),
	})

	sm, err := decodeBackfillSubTaskMeta(subtask.Meta)
	if err != nil {
		return err
	}

	startKey, endKey, tbl, err := r.getTableStartEndKey(sm)
	if err != nil {
		return err
	}

	sessCtx, err := newSessCtx(
		r.d.store, r.job.ReorgMeta.SQLMode, r.job.ReorgMeta.Location, r.job.ReorgMeta.ResourceGroupName)
	if err != nil {
		return err
	}

	opCtx := NewOperatorCtx(ctx, subtask.TaskID, subtask.ID)
	defer opCtx.Cancel()
	r.curRowCount.Store(0)

	var pipe *operator.AsyncPipeline
	if len(r.cloudStorageURI) > 0 {
		pipe, err = r.buildExternalStorePipeline(opCtx, subtask.ID, sessCtx, tbl, startKey, endKey, r.curRowCount)
	} else {
		pipe, err = r.buildLocalStorePipeline(opCtx, sessCtx, tbl, startKey, endKey, r.curRowCount)
	}
	if err != nil {
		return err
	}

	err = pipe.Execute()
	if err != nil {
		return err
	}
	err = pipe.Close()
	if opCtx.OperatorErr() != nil {
		return opCtx.OperatorErr()
	}
	if err != nil {
		return err
	}

	r.bc.ResetWorkers(r.job.ID)
	return nil
}

func (r *readIndexExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return &execute.SubtaskSummary{
		RowCount: r.curRowCount.Load(),
	}
}

func (r *readIndexExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("read index executor cleanup subtask exec env",
		zap.String("category", "ddl"))
	// cleanup backend context
	ingest.LitBackCtxMgr.Unregister(r.job.ID)
	return nil
}

// MockDMLExecutionAddIndexSubTaskFinish is used to mock DML execution during distributed add index.
var MockDMLExecutionAddIndexSubTaskFinish func()

func (r *readIndexExecutor) OnFinished(ctx context.Context, subtask *proto.Subtask) error {
	failpoint.Inject("mockDMLExecutionAddIndexSubTaskFinish", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			MockDMLExecutionAddIndexSubTaskFinish()
		}
	})
	if len(r.cloudStorageURI) == 0 {
		return nil
	}
	// Rewrite the subtask meta to record statistics.
	sm, err := decodeBackfillSubTaskMeta(subtask.Meta)
	if err != nil {
		return err
	}
	sum, _ := r.subtaskSummary.LoadAndDelete(subtask.ID)
	s := sum.(*readIndexSummary)
	all := external.SortedKVMeta{}
	for _, g := range s.metaGroups {
		all.Merge(g)
	}
	sm.MetaGroups = s.metaGroups

	logutil.Logger(ctx).Info("get key boundary on subtask finished",
		zap.String("start", hex.EncodeToString(all.StartKey)),
		zap.String("end", hex.EncodeToString(all.EndKey)),
		zap.Int("fileCount", len(all.MultipleFilesStats)),
		zap.Uint64("totalKVSize", all.TotalKVSize))
	meta, err := json.Marshal(sm)
	if err != nil {
		return err
	}
	subtask.Meta = meta
	return nil
}

func (r *readIndexExecutor) getTableStartEndKey(sm *BackfillSubTaskMeta) (
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
		start, end = sm.RowStart, sm.RowEnd
		tbl = r.ptbl
	}
	return start, end, tbl, nil
}

func (r *readIndexExecutor) buildLocalStorePipeline(
	opCtx *OperatorCtx,
	sessCtx sessionctx.Context,
	tbl table.PhysicalTable,
	start, end kv.Key,
	totalRowCount *atomic.Int64,
) (*operator.AsyncPipeline, error) {
	d := r.d
	engines := make([]ingest.Engine, 0, len(r.indexes))
	for _, index := range r.indexes {
		ei, err := r.bc.Register(r.job.ID, index.ID, r.job.SchemaName, r.job.TableName)
		if err != nil {
			logutil.Logger(opCtx).Warn("cannot register new engine", zap.Error(err),
				zap.Int64("job ID", r.job.ID), zap.Int64("index ID", index.ID))
			return nil, err
		}
		engines = append(engines, ei)
	}
	counter := metrics.BackfillTotalCounter.WithLabelValues(
		metrics.GenerateReorgLabel("add_idx_rate", r.job.SchemaName, tbl.Meta().Name.O))
	return NewAddIndexIngestPipeline(
		opCtx, d.store, d.sessPool, r.bc, engines, sessCtx, tbl, r.indexes, start, end, totalRowCount, counter, r.job.ReorgMeta)
}

func (r *readIndexExecutor) buildExternalStorePipeline(
	opCtx *OperatorCtx,
	subtaskID int64,
	sessCtx sessionctx.Context,
	tbl table.PhysicalTable,
	start, end kv.Key,
	totalRowCount *atomic.Int64,
) (*operator.AsyncPipeline, error) {
	d := r.d
	onClose := func(summary *external.WriterSummary) {
		sum, _ := r.subtaskSummary.Load(subtaskID)
		s := sum.(*readIndexSummary)
		s.mu.Lock()
		kvMeta := s.metaGroups[summary.GroupOffset]
		if kvMeta == nil {
			kvMeta = &external.SortedKVMeta{}
			s.metaGroups[summary.GroupOffset] = kvMeta
		}
		kvMeta.MergeSummary(summary)
		s.mu.Unlock()
	}
	counter := metrics.BackfillTotalCounter.WithLabelValues(
		metrics.GenerateReorgLabel("add_idx_rate", r.job.SchemaName, tbl.Meta().Name.O))
	return NewWriteIndexToExternalStoragePipeline(
		opCtx,
		d.store,
		r.cloudStorageURI,
		r.d.sessPool,
		sessCtx,
		r.job.ID,
		subtaskID,
		tbl,
		r.indexes,
		start,
		end,
		totalRowCount,
		counter,
		onClose,
		r.job.ReorgMeta)
}
