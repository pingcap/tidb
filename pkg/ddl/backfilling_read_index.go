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
	goerrors "errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/metering"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/dxf/operator"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	lightningmetric "github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore/recording"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/table"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type readIndexStepExecutor struct {
	taskexecutor.BaseStepExecutor
	store    kv.Storage
	etcdCli  *clientv3.Client
	sessPool *sess.Pool
	job      *model.Job
	indexes  []*model.IndexInfo
	ptbl     table.PhysicalTable
	jc       *ReorgContext

	avgRowSize      int
	cloudStorageURI string

	summary *execute.SubtaskSummary

	summaryMap sync.Map // subtaskID => readIndexSummary
	backendCfg *local.BackendConfig
	backend    *local.Backend
	// pipeline of current running subtask, it's nil when no subtask is running.
	currPipe atomic.Pointer[operator.AsyncPipeline]

	metric *lightningmetric.Common
}

type readIndexSummary struct {
	metaGroups []*external.SortedKVMeta
	mu         sync.Mutex
}

var cleanupAllLocalEnginesForReadIndex = func(backend *local.Backend) error {
	// Use Background so best-effort cleanup isn't skipped by cancellation.
	return backend.CleanupAllLocalEngines(context.Background())
}

type readIndexLocalBackendGetter interface {
	GetLocalBackend() *local.Backend
}

type readIndexEngineRegistrar interface {
	readIndexLocalBackendGetter
	Register(indexIDs []int64, uniques []bool, tbl table.Table) ([]ingest.Engine, error)
}

const pebbleLockHeldByCurrentProcessMsg = "lock held by current process"

func isPebbleLockHeldByCurrentProcess(err error) bool {
	// Pebble's vfs.Lock returns a plain error containing `pebbleLockHeldByCurrentProcessMsg`
	// when the lock is already held by the current process (see pebble/vfs/file_lock_unix.go).
	// Pebble does not export a sentinel error/type for this case, so we must match on the
	// message.
	return err != nil && strings.Contains(err.Error(), pebbleLockHeldByCurrentProcessMsg)
}

func newReadIndexExecutor(
	store kv.Storage,
	sessPool *sess.Pool,
	etcdCli *clientv3.Client,
	job *model.Job,
	indexes []*model.IndexInfo,
	ptbl table.PhysicalTable,
	jc *ReorgContext,
	cloudStorageURI string,
	avgRowSize int,
) (*readIndexStepExecutor, error) {
	return &readIndexStepExecutor{
		store:           store,
		etcdCli:         etcdCli,
		sessPool:        sessPool,
		job:             job,
		indexes:         indexes,
		ptbl:            ptbl,
		jc:              jc,
		cloudStorageURI: cloudStorageURI,
		avgRowSize:      avgRowSize,
		summary:         &execute.SubtaskSummary{},
	}, nil
}

func (r *readIndexStepExecutor) Init(ctx context.Context) error {
	logutil.DDLLogger().Info("read index executor init subtask exec env")
	cfg := config.GetGlobalConfig()
	if cfg.Store == config.StoreTypeTiKV {
		if !r.isGlobalSort() {
			r.metric = metrics.RegisterLightningCommonMetricsForDDL(r.job.ID)
			ctx = lightningmetric.WithCommonMetric(ctx, r.metric)
		}
		cfg, bd, err := ingest.CreateLocalBackend(ctx, r.store, r.job, hasUniqueIndex(r.indexes), false, 0)
		if err != nil {
			return errors.Trace(err)
		}
		r.backendCfg = cfg
		r.backend = bd
	}
	return nil
}

func (r *readIndexStepExecutor) runGlobalPipeline(
	ctx context.Context,
	wctx *workerpool.Context,
	subtask *proto.Subtask,
	sm *BackfillSubTaskMeta,
	concurrency int,
	extStore storeapi.Storage,
) error {
	pipe, err := r.buildExternalStorePipeline(wctx, extStore, subtask.TaskID, subtask.ID, sm, concurrency)
	if err != nil {
		return err
	}

	r.currPipe.Store(pipe)
	defer func() {
		r.currPipe.Store(nil)
	}()

	if err = executeAndClosePipeline(wctx, pipe, nil, nil, r.avgRowSize); err != nil {
		return errors.Trace(err)
	}
	return r.onFinished(ctx, subtask, sm, extStore)
}

func (r *readIndexStepExecutor) runLocalPipeline(
	ctx context.Context,
	wctx *workerpool.Context,
	subtask *proto.Subtask,
	sm *BackfillSubTaskMeta,
	concurrency int,
) (retErr error) {
	bCtx, err := ingest.NewBackendCtxBuilder(ctx, r.store, r.job).
		WithImportDistributedLock(r.etcdCli, sm.TS).
		WithDistTaskCheckpointManagerParam(
			subtask.ID,
			r.ptbl.GetPhysicalID(),
			r.GetCheckpointUpdateFunc(),
			r.GetCheckpointFunc(),
		).
		Build(r.backendCfg, r.backend)
	if err != nil {
		return err
	}
	defer bCtx.Close()
	defer func() {
		if retErr == nil {
			return
		}
		// For dist task local based ingest, checkpoint is unsupported.
		// If there is an error we should keep local sort dir clean.
		if err1 := bCtx.FinishAndUnregisterEngines(ingest.OptCleanData); err1 != nil {
			logutil.DDLLogger().Warn("read index executor unregister engine failed", zap.Error(err1))
		}
		// Best-effort cleanup all local engines to release the Pebble directory lock.
		cleanupReadIndexLocalEngines(r.job.ID, subtask.ID, bCtx)
	}()

	pipe, err := r.buildLocalStorePipeline(wctx, bCtx, sm, concurrency)
	if err != nil {
		return err
	}
	r.currPipe.Store(pipe)
	defer func() {
		r.currPipe.Store(nil)
	}()
	err = executeAndClosePipeline(wctx, pipe, nil, nil, r.avgRowSize)
	if err != nil {
		return err
	}

	if err = bCtx.FinishAndUnregisterEngines(ingest.OptCleanData | ingest.OptCheckDup); err != nil {
		return errors.Trace(err)
	}
	return r.onFinished(ctx, subtask, sm, nil)
}

func (r *readIndexStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) error {
	logutil.DDLLogger().Info("read index executor run subtask",
		zap.Bool("use cloud", r.isGlobalSort()))

	r.summaryMap.Store(subtask.ID, &readIndexSummary{
		metaGroups: make([]*external.SortedKVMeta, len(r.indexes)),
	})
	r.summary.Reset()
	var err error
	failpoint.InjectCall("beforeReadIndexStepExecRunSubtask", &err)
	if err != nil {
		return err
	}

	var (
		accessRec = &recording.AccessStats{}
		objStore  storeapi.Storage
	)
	if r.isGlobalSort() {
		accessRec, objStore, err = handle.NewObjStoreWithRecording(ctx, r.cloudStorageURI)
		if err != nil {
			return err
		}
		defer func() {
			objStore.Close()
			r.summary.MergeObjStoreRequests(&accessRec.Requests)
			r.GetMeterRecorder().MergeObjStoreAccess(accessRec)
		}()
	}
	sm, err := decodeBackfillSubTaskMeta(ctx, objStore, subtask.Meta)
	if err != nil {
		return err
	}

	wctx := workerpool.NewContext(ctx)
	defer wctx.Cancel()

	concurrency := int(r.GetResource().CPU.Capacity())
	if r.isGlobalSort() {
		return r.runGlobalPipeline(ctx, wctx, subtask, sm, concurrency, objStore)
	}
	return r.runLocalPipeline(ctx, wctx, subtask, sm, concurrency)
}

func (r *readIndexStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	return r.summary
}

func (r *readIndexStepExecutor) ResetSummary() {
	r.summary.Reset()
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
	targetReaderCnt, targetWriterCnt := expectedIngestWorkerCnt(int(newResource.CPU.Capacity()), r.avgRowSize, r.job.ReorgMeta.UseCloudStorage)
	currentReaderCnt, currentWriterCnt := reader.GetWorkerPoolSize(), writer.GetWorkerPoolSize()
	if int32(targetReaderCnt) != currentReaderCnt {
		reader.TuneWorkerPoolSize(int32(targetReaderCnt), true)
	}
	if int32(targetWriterCnt) != currentWriterCnt {
		writer.TuneWorkerPoolSize(int32(targetWriterCnt), true)
	}
	return nil
}

func (r *readIndexStepExecutor) onFinished(ctx context.Context, subtask *proto.Subtask, sm *BackfillSubTaskMeta, extStore storeapi.Storage) error {
	failpoint.InjectCall("mockDMLExecutionAddIndexSubTaskFinish", r.backend)
	if !r.isGlobalSort() {
		return nil
	}
	// Rewrite the subtask meta to record statistics.
	sum, _ := r.summaryMap.LoadAndDelete(subtask.ID)
	s := sum.(*readIndexSummary)
	sm.MetaGroups = s.metaGroups
	sm.EleIDs = make([]int64, 0, len(r.indexes))
	for _, index := range r.indexes {
		sm.EleIDs = append(sm.EleIDs, index.ID)
	}

	all := external.SortedKVMeta{}
	for _, g := range s.metaGroups {
		all.Merge(g)
	}
	tidblogutil.Logger(ctx).Info("get key boundary on subtask finished",
		zap.String("start", hex.EncodeToString(all.StartKey)),
		zap.String("end", hex.EncodeToString(all.EndKey)),
		zap.Int("fileCount", len(all.MultipleFilesStats)),
		zap.Uint64("totalKVSize", all.TotalKVSize))

	// write external meta to storage when using global sort
	if r.isGlobalSort() {
		if err := writeExternalBackfillSubTaskMeta(ctx, extStore, sm, external.SubtaskMetaPath(subtask.TaskID, subtask.ID)); err != nil {
			return err
		}
	}

	meta, err := sm.Marshal()
	if err != nil {
		return err
	}
	subtask.Meta = meta
	return nil
}

func (r *readIndexStepExecutor) isGlobalSort() bool {
	return len(r.cloudStorageURI) > 0
}

func (r *readIndexStepExecutor) getTableStartEndKey(sm *BackfillSubTaskMeta) (
	start, end kv.Key, tbl table.PhysicalTable, err error) {
	if parTbl, ok := r.ptbl.(table.PartitionedTable); ok {
		pid := sm.PhysicalTableID
		tbl = parTbl.GetPartition(pid)
		if len(sm.RowStart) == 0 {
			// Handle upgrade compatibility
			currentVer, err1 := getValidCurrentVersion(r.store)
			if err1 != nil {
				return nil, nil, nil, errors.Trace(err1)
			}
			start, end, err = getTableRange(r.jc, r.store, parTbl.GetPartition(pid), currentVer.Ver, r.job.Priority)
			if err != nil {
				logutil.DDLLogger().Error("get table range error",
					zap.Error(err))
				return nil, nil, nil, err
			}
			return start, end, tbl, nil
		}
	} else {
		tbl = r.ptbl
	}
	return sm.RowStart, sm.RowEnd, tbl, nil
}

func (r *readIndexStepExecutor) buildLocalStorePipeline(
	wctx *workerpool.Context,
	backendCtx ingest.BackendCtx,
	sm *BackfillSubTaskMeta,
	concurrency int,
) (*operator.AsyncPipeline, error) {
	start, end, tbl, err := r.getTableStartEndKey(sm)
	if err != nil {
		return nil, err
	}
	indexIDs := make([]int64, 0, len(r.indexes))
	uniques := make([]bool, 0, len(r.indexes))
	var idxNames strings.Builder
	for _, index := range r.indexes {
		indexIDs = append(indexIDs, index.ID)
		uniques = append(uniques, index.Unique)
		if idxNames.Len() > 0 {
			idxNames.WriteByte('+')
		}
		idxNames.WriteString(index.Name.O)
	}
	engines, err := registerReadIndexEngines(wctx, r.job.ID, backendCtx, indexIDs, uniques, r.ptbl)
	if err != nil {
		tidblogutil.Logger(wctx).Error("cannot register new engine",
			zap.Error(err),
			zap.Int64("job ID", r.job.ID),
			zap.Int64s("index IDs", indexIDs))
		return nil, err
	}
	rowCntCollector := newDistTaskRowCntCollector(r.summary, r.job.SchemaName, tbl.Meta().Name.O, idxNames.String(), r.GetMeterRecorder())
	return NewAddIndexIngestPipeline(
		wctx,
		r.store,
		r.sessPool,
		backendCtx,
		engines,
		r.job.ID,
		tbl,
		r.indexes,
		start,
		end,
		r.job.ReorgMeta,
		r.avgRowSize,
		concurrency,
		rowCntCollector,
	)
}

func (r *readIndexStepExecutor) buildExternalStorePipeline(
	wctx *workerpool.Context,
	extStore storeapi.Storage,
	taskID int64,
	subtaskID int64,
	sm *BackfillSubTaskMeta,
	concurrency int,
) (*operator.AsyncPipeline, error) {
	start, end, tbl, err := r.getTableStartEndKey(sm)
	if err != nil {
		return nil, err
	}

	onWriterClose := func(summary *external.WriterSummary) {
		sum, _ := r.summaryMap.Load(subtaskID)
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
	var idxNames strings.Builder
	for _, idx := range r.indexes {
		if idxNames.Len() > 0 {
			idxNames.WriteByte('+')
		}
		idxNames.WriteString(idx.Name.O)
	}
	rowCntCollector := newDistTaskRowCntCollector(r.summary, r.job.SchemaName, tbl.Meta().Name.O, idxNames.String(), r.GetMeterRecorder())
	return NewWriteIndexToExternalStoragePipeline(
		wctx,
		r.store,
		extStore,
		r.sessPool,
		taskID,
		subtaskID,
		tbl,
		r.indexes,
		start,
		end,
		onWriterClose,
		r.job.ReorgMeta,
		r.avgRowSize,
		concurrency,
		r.GetResource(),
		rowCntCollector,
		r.backend.GetTiKVCodec(),
	)
}

func cleanupReadIndexLocalEngines(jobID, subtaskID int64, backendCtx readIndexLocalBackendGetter) {
	if backendCtx == nil {
		return
	}
	backend := backendCtx.GetLocalBackend()
	if backend == nil {
		return
	}
	if cleanupErr := cleanupAllLocalEnginesForReadIndex(backend); cleanupErr != nil {
		logutil.DDLLogger().Warn("read index executor cleanup engines failed",
			zap.Error(cleanupErr),
			zap.Int64("job ID", jobID),
			zap.Int64("subtask ID", subtaskID),
		)
	}
}

func registerReadIndexEngines(
	wctx *workerpool.Context,
	jobID int64,
	backendCtx readIndexEngineRegistrar,
	indexIDs []int64,
	uniques []bool,
	tbl table.Table,
) ([]ingest.Engine, error) {
	engines, err := backendCtx.Register(indexIDs, uniques, tbl)
	if err != nil && isPebbleLockHeldByCurrentProcess(err) {
		// Cleanup happens in the caller's error-path defer; dist-task retry will re-run.
		tidblogutil.Logger(wctx).Warn("register ingest engine got lock held",
			zap.Error(err),
			zap.Int64("job ID", jobID),
			zap.Int64s("index IDs", indexIDs),
		)
	}
	return engines, err
}

type distTaskRowCntCollector struct {
	summary  *execute.SubtaskSummary
	counter  prometheus.Counter
	meterRec *metering.Recorder
}

func newDistTaskRowCntCollector(
	summary *execute.SubtaskSummary,
	dbName, tblName, idxName string,
	meterRec *metering.Recorder,
) *distTaskRowCntCollector {
	counter := metrics.GetBackfillTotalByLabel(metrics.LblAddIdxRate, dbName, tblName, idxName)
	return &distTaskRowCntCollector{
		summary:  summary,
		counter:  counter,
		meterRec: meterRec,
	}
}

func (d *distTaskRowCntCollector) Accepted(bytes int64) {
	d.summary.ReadBytes.Add(bytes)
	d.meterRec.IncClusterReadBytes(uint64(bytes))
}

func (d *distTaskRowCntCollector) Processed(bytes, rowCnt int64) {
	d.summary.Bytes.Add(bytes)
	d.summary.RowCnt.Add(rowCnt)
	d.counter.Add(float64(rowCnt))
}
