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
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/kv"
<<<<<<< HEAD
=======
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	lightningmetric "github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/meta/model"
>>>>>>> e217a01f117 (addindex: add import speed metric (#60904))
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

	bc      ingest.BackendCtx
	summary *execute.Summary

	subtaskSummary sync.Map // subtaskID => readIndexSummary
<<<<<<< HEAD
=======
	backendCfg     *local.BackendConfig
	backend        *local.Backend
	// pipeline of current running subtask, it's nil when no subtask is running.
	currPipe atomic.Pointer[operator.AsyncPipeline]

	metric *lightningmetric.Common
>>>>>>> e217a01f117 (addindex: add import speed metric (#60904))
}

type readIndexSummary struct {
	minKey    []byte
	maxKey    []byte
	totalSize uint64
	stats     []external.MultipleFilesStat
	mu        sync.Mutex
}

func newReadIndexExecutor(
	d *ddl,
	job *model.Job,
	indexes []*model.IndexInfo,
	ptbl table.PhysicalTable,
	jc *JobContext,
	bc ingest.BackendCtx,
	summary *execute.Summary,
	cloudStorageURI string,
) *readIndexExecutor {
	return &readIndexExecutor{
		d:               d,
		job:             job,
		indexes:         indexes,
		ptbl:            ptbl,
		jc:              jc,
		bc:              bc,
		summary:         summary,
		cloudStorageURI: cloudStorageURI,
	}
}

<<<<<<< HEAD
func (*readIndexExecutor) Init(_ context.Context) error {
	logutil.BgLogger().Info("read index executor init subtask exec env",
		zap.String("category", "ddl"))
=======
func (r *readIndexStepExecutor) Init(ctx context.Context) error {
	logutil.DDLLogger().Info("read index executor init subtask exec env")
	cfg := config.GetGlobalConfig()
	if cfg.Store == config.StoreTypeTiKV {
		if !r.isGlobalSort() {
			r.metric = metrics.RegisterLightningCommonMetricsForDDL(r.job.ID)
			ctx = lightningmetric.WithCommonMetric(ctx, r.metric)
		}
		cfg, bd, err := ingest.CreateLocalBackend(ctx, r.d.store, r.job, false, 0)
		if err != nil {
			return errors.Trace(err)
		}
		r.backendCfg = cfg
		r.backend = bd
	}
>>>>>>> e217a01f117 (addindex: add import speed metric (#60904))
	return nil
}

func (r *readIndexExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.BgLogger().Info("read index executor run subtask",
		zap.String("category", "ddl"),
		zap.Bool("use cloud", len(r.cloudStorageURI) > 0))

	r.subtaskSummary.Store(subtask.ID, &readIndexSummary{})

	sm := &BackfillSubTaskMeta{}
	err := json.Unmarshal(subtask.Meta, sm)
	if err != nil {
		logutil.BgLogger().Error("unmarshal error",
			zap.String("category", "ddl"),
			zap.Error(err))
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

	opCtx := NewOperatorCtx(ctx)
	defer opCtx.Cancel()
	totalRowCount := &atomic.Int64{}

	var pipe *operator.AsyncPipeline
	if len(r.cloudStorageURI) > 0 {
		pipe, err = r.buildExternalStorePipeline(opCtx, subtask.ID, sessCtx, tbl, startKey, endKey, totalRowCount)
	} else {
		pipe, err = r.buildLocalStorePipeline(opCtx, sessCtx, tbl, startKey, endKey, totalRowCount)
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
	r.summary.UpdateRowCount(subtask.ID, totalRowCount.Load())
	return nil
}

func (*readIndexExecutor) Cleanup(ctx context.Context) error {
	logutil.Logger(ctx).Info("read index executor cleanup subtask exec env",
		zap.String("category", "ddl"))
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
<<<<<<< HEAD
	if len(r.cloudStorageURI) == 0 {
=======

	sm, err := decodeBackfillSubTaskMeta(ctx, r.cloudStorageURI, subtask.Meta)
	if err != nil {
		return err
	}

	opCtx, cancel := NewDistTaskOperatorCtx(ctx, subtask.TaskID, subtask.ID)
	defer cancel()
	r.curRowCount.Store(0)

	concurrency := int(r.GetResource().CPU.Capacity())
	if r.isGlobalSort() {
		return r.runGlobalPipeline(ctx, opCtx, subtask, sm, concurrency)
	}
	return r.runLocalPipeline(ctx, opCtx, subtask, sm, concurrency)
}

func (r *readIndexStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return &execute.SubtaskSummary{
		RowCount: r.curRowCount.Load(),
	}
}

func (r *readIndexStepExecutor) Cleanup(ctx context.Context) error {
	tidblogutil.Logger(ctx).Info("read index executor cleanup subtask exec env")
	if r.backend != nil {
		r.backend.Close()
	}
	if !r.isGlobalSort() {
		metrics.UnregisterLightningCommonMetricsForDDL(r.job.ID, r.metric)
	}
	return nil
}

func (r *readIndexStepExecutor) TaskMetaModified(_ context.Context, newMeta []byte) error {
	newTaskMeta := &BackfillTaskMeta{}
	if err := json.Unmarshal(newMeta, newTaskMeta); err != nil {
		return errors.Trace(err)
	}
	newBatchSize := newTaskMeta.Job.ReorgMeta.GetBatchSize()
	if newBatchSize != r.job.ReorgMeta.GetBatchSize() {
		r.job.ReorgMeta.SetBatchSize(newBatchSize)
	}
	// Only local sort need modify write speed in this step.
	if !r.isGlobalSort() {
		newMaxWriteSpeed := newTaskMeta.Job.ReorgMeta.GetMaxWriteSpeed()
		if newMaxWriteSpeed != r.job.ReorgMeta.GetMaxWriteSpeed() {
			r.job.ReorgMeta.SetMaxWriteSpeed(newMaxWriteSpeed)
			if r.backend != nil {
				r.backend.UpdateWriteSpeedLimit(newMaxWriteSpeed)
			}
		}
	}
	return nil
}

func (r *readIndexStepExecutor) ResourceModified(_ context.Context, newResource *proto.StepResource) error {
	pipe := r.currPipe.Load()
	if pipe == nil {
		// let framework retry
		return goerrors.New("no subtask running")
	}
	reader, writer := pipe.GetReaderAndWriter()
	targetReaderCnt, targetWriterCnt := expectedIngestWorkerCnt(int(newResource.CPU.Capacity()), r.avgRowSize)
	currentReaderCnt, currentWriterCnt := reader.GetWorkerPoolSize(), writer.GetWorkerPoolSize()
	if int32(targetReaderCnt) != currentReaderCnt {
		reader.TuneWorkerPoolSize(int32(targetReaderCnt), true)
	}
	if int32(targetWriterCnt) != currentWriterCnt {
		writer.TuneWorkerPoolSize(int32(targetWriterCnt), true)
	}
	return nil
}

func (r *readIndexStepExecutor) onFinished(ctx context.Context, subtask *proto.Subtask) error {
	failpoint.InjectCall("mockDMLExecutionAddIndexSubTaskFinish", r.backend)
	if !r.isGlobalSort() {
>>>>>>> e217a01f117 (addindex: add import speed metric (#60904))
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
	subtaskMeta.StartKey = s.minKey
	subtaskMeta.EndKey = kv.Key(s.maxKey).Next()
	subtaskMeta.TotalKVSize = s.totalSize
	subtaskMeta.MultipleFilesStats = s.stats
	fileCnt := 0
	for _, stat := range s.stats {
		fileCnt += len(stat.Filenames)
	}
	logutil.Logger(ctx).Info("get key boundary on subtask finished",
		zap.String("min", hex.EncodeToString(s.minKey)),
		zap.String("max", hex.EncodeToString(s.maxKey)),
		zap.Int("fileCount", fileCnt),
		zap.Uint64("totalSize", s.totalSize))
	meta, err := json.Marshal(subtaskMeta)
	if err != nil {
		return err
	}
	subtask.Meta = meta
	return nil
}

func (r *readIndexExecutor) Rollback(ctx context.Context) error {
	logutil.Logger(ctx).Info("read index executor rollback backfill add index task",
		zap.String("category", "ddl"), zap.Int64("jobID", r.job.ID))
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
		start, end = sm.StartKey, sm.EndKey
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
		if len(s.minKey) == 0 || summary.Min.Cmp(s.minKey) < 0 {
			s.minKey = summary.Min.Clone()
		}
		if len(s.maxKey) == 0 || summary.Max.Cmp(s.maxKey) > 0 {
			s.maxKey = summary.Max.Clone()
		}
		s.totalSize += summary.TotalSize
		s.stats = append(s.stats, summary.MultipleFilesStats...)
		s.mu.Unlock()
	}
	counter := metrics.BackfillTotalCounter.WithLabelValues(
		metrics.GenerateReorgLabel("add_idx_rate", r.job.SchemaName, tbl.Meta().Name.O))
	return NewWriteIndexToExternalStoragePipeline(
		opCtx, d.store, r.cloudStorageURI, r.d.sessPool, sessCtx, r.job.ID, subtaskID,
		tbl, r.indexes, start, end, totalRowCount, counter, onClose, r.job.ReorgMeta)
}
