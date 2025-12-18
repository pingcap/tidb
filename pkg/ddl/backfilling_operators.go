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
	"fmt"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/tikv/client-go/v2/tikv"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	_ operator.Operator                = (*TableScanTaskSource)(nil)
	_ operator.WithSink[TableScanTask] = (*TableScanTaskSource)(nil)

	_ operator.WithSource[TableScanTask]  = (*TableScanOperator)(nil)
	_ operator.Operator                   = (*TableScanOperator)(nil)
	_ operator.WithSink[IndexRecordChunk] = (*TableScanOperator)(nil)

	_ operator.WithSource[IndexRecordChunk] = (*IndexIngestOperator)(nil)
	_ operator.Operator                     = (*IndexIngestOperator)(nil)
	_ operator.WithSink[IndexWriteResult]   = (*IndexIngestOperator)(nil)

	_ operator.WithSource[IndexWriteResult] = (*indexWriteResultSink)(nil)
	_ operator.Operator                     = (*indexWriteResultSink)(nil)
)

type opSessPool interface {
	Get() (sessionctx.Context, error)
	Put(sessionctx.Context)
}

// OperatorCtx is the context for AddIndexIngestPipeline.
// This is used to cancel the pipeline and collect errors.
type OperatorCtx struct {
	context.Context
	cancel context.CancelFunc
	err    atomic.Pointer[error]
}

// NewDistTaskOperatorCtx is used for adding index with dist framework.
func NewDistTaskOperatorCtx(ctx context.Context) (*OperatorCtx, context.CancelFunc) {
	opCtx, cancel := context.WithCancel(ctx)
	return &OperatorCtx{
		Context: opCtx,
		cancel:  cancel,
	}, cancel
}

// NewLocalOperatorCtx is used for adding index with local ingest mode.
func NewLocalOperatorCtx(ctx context.Context, jobID int64) (*OperatorCtx, context.CancelFunc) {
	opCtx, cancel := context.WithCancel(ctx)
	opCtx = logutil.WithFields(opCtx, zap.Int64("jobID", jobID))
	return &OperatorCtx{
		Context: opCtx,
		cancel:  cancel,
	}, cancel
}

func (ctx *OperatorCtx) onError(err error) {
	tracedErr := errors.Trace(err)
	ctx.err.CompareAndSwap(nil, &tracedErr)
	ctx.cancel()
}

// OperatorErr returns the error of the operator.
func (ctx *OperatorCtx) OperatorErr() error {
	err := ctx.err.Load()
	if err == nil {
		return nil
	}
	return *err
}

var (
	_ execute.Collector = (*distTaskRowCntCollector)(nil)
	_ execute.Collector = (*localRowCntCollector)(nil)
)

// NewAddIndexIngestPipeline creates a pipeline for adding index in ingest mode.
func NewAddIndexIngestPipeline(
	ctx *OperatorCtx,
	store kv.Storage,
	sessPool opSessPool,
	backendCtx ingest.BackendCtx,
	engines []ingest.Engine,
	jobID int64,
	tbl table.PhysicalTable,
	idxInfos []*model.IndexInfo,
	startKey, endKey kv.Key,
	reorgMeta *model.DDLReorgMeta,
	avgRowSize int,
	concurrency int,
	collector execute.Collector,
) (*operator.AsyncPipeline, error) {
	indexes := make([]table.Index, 0, len(idxInfos))
	for _, idxInfo := range idxInfos {
		index := tables.NewIndex(tbl.GetPhysicalID(), tbl.Meta(), idxInfo)
		indexes = append(indexes, index)
	}
	reqSrc := getDDLRequestSource(model.ActionAddIndex)
	copCtx, err := NewReorgCopContext(store, reorgMeta, tbl.Meta(), idxInfos, reqSrc)
	if err != nil {
		return nil, err
	}
	srcChkPool := createChunkPool(copCtx, reorgMeta)
	readerCnt, writerCnt := expectedIngestWorkerCnt(concurrency, avgRowSize, reorgMeta.IsDistReorg)

	failpoint.InjectCall("beforeAddIndexScan")

	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey, backendCtx)
	scanOp := NewTableScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt,
		reorgMeta.GetBatchSize(), reorgMeta, backendCtx, collector)
	ingestOp := NewIndexIngestOperator(ctx, copCtx, sessPool,
		tbl, indexes, engines, srcChkPool, writerCnt, reorgMeta, collector)
	sinkOp := newIndexWriteResultSink(ctx, backendCtx, tbl, indexes, collector)

	operator.Compose(srcOp, scanOp)
	operator.Compose(scanOp, ingestOp)
	operator.Compose(ingestOp, sinkOp)

	logutil.Logger(ctx).Info("build add index local storage operators",
		zap.Int64("jobID", jobID),
		zap.Int("avgRowSize", avgRowSize),
		zap.Int("reader", readerCnt),
		zap.Int("writer", writerCnt))

	return operator.NewAsyncPipeline(
		srcOp, scanOp, ingestOp, sinkOp,
	), nil
}

// NewWriteIndexToExternalStoragePipeline creates a pipeline for writing index to external storage.
func NewWriteIndexToExternalStoragePipeline(
	ctx *OperatorCtx,
	store kv.Storage,
	extStore storage.ExternalStorage,
	sessPool opSessPool,
	taskID, subtaskID int64,
	tbl table.PhysicalTable,
	idxInfos []*model.IndexInfo,
	startKey, endKey kv.Key,
	onClose external.OnWriterCloseFunc,
	reorgMeta *model.DDLReorgMeta,
	avgRowSize int,
	concurrency int,
	resource *proto.StepResource,
	collector execute.Collector,
	tikvCodec tikv.Codec,
) (*operator.AsyncPipeline, error) {
	indexes := make([]table.Index, 0, len(idxInfos))
	for _, idxInfo := range idxInfos {
		index := tables.NewIndex(tbl.GetPhysicalID(), tbl.Meta(), idxInfo)
		indexes = append(indexes, index)
	}
	reqSrc := getDDLRequestSource(model.ActionAddIndex)
	copCtx, err := NewReorgCopContext(store, reorgMeta, tbl.Meta(), idxInfos, reqSrc)
	if err != nil {
		return nil, err
	}
	srcChkPool := createChunkPool(copCtx, reorgMeta)
	readerCnt, writerCnt := expectedIngestWorkerCnt(concurrency, avgRowSize, reorgMeta.IsDistReorg)

	memCap := resource.Mem.Capacity()
	memSizePerIndex := uint64(memCap / int64(writerCnt*2*len(idxInfos)))
	failpoint.Inject("mockWriterMemSizeInKB", func(val failpoint.Value) {
		if v, ok := val.(int); ok {
			memSizePerIndex = uint64(v) * size.KB
		}
	})

	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey, nil)
	scanOp := NewTableScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt,
		reorgMeta.GetBatchSize(), reorgMeta, nil, collector)
	writeOp := NewWriteExternalStoreOperator(
		ctx, copCtx, sessPool, taskID, subtaskID,
		tbl, indexes, extStore, srcChkPool, writerCnt,
		onClose, memSizePerIndex, reorgMeta, tikvCodec,
		collector,
	)
	sinkOp := newIndexWriteResultSink(ctx, nil, tbl, indexes, collector)

	operator.Compose(srcOp, scanOp)
	operator.Compose(scanOp, writeOp)
	operator.Compose(writeOp, sinkOp)

	logutil.Logger(ctx).Info("build add index cloud storage operators",
		zap.Int64("taskID", taskID),
		zap.String("memCap", units.BytesSize(float64(memCap))),
		zap.String("memSizePerIdx", units.BytesSize(float64(memSizePerIndex))),
		zap.Int("avgRowSize", avgRowSize),
		zap.Int("reader", readerCnt),
		zap.Int("writer", writerCnt))

	return operator.NewAsyncPipeline(
		srcOp, scanOp, writeOp, sinkOp,
	), nil
}

func createChunkPool(copCtx copr.CopContext, reorgMeta *model.DDLReorgMeta) *sync.Pool {
	return &sync.Pool{
		New: func() any {
			return chunk.NewChunkWithCapacity(copCtx.GetBase().FieldTypes,
				reorgMeta.GetBatchSize())
		},
	}
}

// TableScanTask contains the start key and the end key of a region.
type TableScanTask struct {
	ID    int
	Start kv.Key
	End   kv.Key

	ctx *OperatorCtx
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (t TableScanTask) RecoverArgs() (metricsLabel string, funcInfo string, recoverFn func(), quit bool) {
	return metrics.LblAddIndex, "RecoverArgs", func() {
		t.ctx.onError(dbterror.ErrReorgPanic)
	}, false
}

// String implement fmt.Stringer interface.
func (t TableScanTask) String() string {
	return fmt.Sprintf("TableScanTask: id=%d, startKey=%s, endKey=%s",
		t.ID, hex.EncodeToString(t.Start), hex.EncodeToString(t.End))
}

// IndexRecordChunk contains one of the chunk read from corresponding TableScanTask.
type IndexRecordChunk struct {
	ID    int
	Chunk *chunk.Chunk
	Err   error
	Done  bool
	ctx   *OperatorCtx
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (t IndexRecordChunk) RecoverArgs() (metricsLabel string, funcInfo string, recoverFn func(), quit bool) {
	return metrics.LblAddIndex, "RecoverArgs", func() {
		t.ctx.onError(dbterror.ErrReorgPanic)
	}, false
}

// TableScanTaskSource produces TableScanTask by splitting table records into ranges.
type TableScanTaskSource struct {
	ctx *OperatorCtx

	errGroup errgroup.Group
	sink     operator.DataChannel[TableScanTask]

	tbl      table.PhysicalTable
	store    kv.Storage
	startKey kv.Key
	endKey   kv.Key

	cpOp ingest.CheckpointOperator
}

// NewTableScanTaskSource creates a new TableScanTaskSource.
func NewTableScanTaskSource(
	ctx *OperatorCtx,
	store kv.Storage,
	physicalTable table.PhysicalTable,
	startKey kv.Key,
	endKey kv.Key,
	cpOp ingest.CheckpointOperator,
) *TableScanTaskSource {
	return &TableScanTaskSource{
		ctx:      ctx,
		errGroup: errgroup.Group{},
		tbl:      physicalTable,
		store:    store,
		startKey: startKey,
		endKey:   endKey,
		cpOp:     cpOp,
	}
}

// SetSink implements WithSink interface.
func (src *TableScanTaskSource) SetSink(sink operator.DataChannel[TableScanTask]) {
	src.sink = sink
}

// Open implements Operator interface.
func (src *TableScanTaskSource) Open() error {
	src.errGroup.Go(src.generateTasks)
	return nil
}

// adjustStartKey adjusts the start key so that we can skip the ranges that have been processed
// according to the information of checkpoint manager.
func (src *TableScanTaskSource) adjustStartKey(start, end kv.Key) (adjusted kv.Key, done bool) {
	if src.cpOp == nil {
		return start, false
	}
	cpKey := src.cpOp.NextStartKey()
	if len(cpKey) == 0 {
		return start, false
	}
	if cpKey.Cmp(start) < 0 || cpKey.Cmp(end) > 0 {
		logutil.Logger(src.ctx).Error("invalid checkpoint key",
			zap.String("last_process_key", hex.EncodeToString(cpKey)),
			zap.String("start", hex.EncodeToString(start)),
			zap.String("end", hex.EncodeToString(end)),
		)
		if intest.EnableInternalCheck {
			panic("invalid checkpoint key")
		}
		return start, false
	}
	if cpKey.Cmp(end) == 0 {
		return cpKey, true
	}
	return cpKey, false
}

func (src *TableScanTaskSource) generateTasks() error {
	taskIDAlloc := newTaskIDAllocator()
	defer src.sink.Finish()

	startKey, done := src.adjustStartKey(src.startKey, src.endKey)
	if done {
		// All table data are done.
		return nil
	}
	for {
		kvRanges, err := loadTableRanges(
			src.ctx,
			src.tbl.GetPhysicalID(),
			src.store,
			startKey,
			src.endKey,
			nil,
			backfillTaskChanSize,
		)
		if err != nil {
			return err
		}
		if len(kvRanges) == 0 {
			break
		}

		batchTasks := src.getBatchTableScanTask(kvRanges, taskIDAlloc)
		for _, task := range batchTasks {
			select {
			case <-src.ctx.Done():
				return src.ctx.Err()
			case src.sink.Channel() <- task:
			}
		}
		startKey = kvRanges[len(kvRanges)-1].EndKey
		if startKey.Cmp(src.endKey) >= 0 {
			break
		}
	}
	return nil
}

func (src *TableScanTaskSource) getBatchTableScanTask(
	kvRanges []kv.KeyRange,
	taskIDAlloc *taskIDAllocator,
) []TableScanTask {
	batchTasks := make([]TableScanTask, 0, len(kvRanges))
	prefix := src.tbl.RecordPrefix()
	// Build reorg tasks.
	for _, keyRange := range kvRanges {
		taskID := taskIDAlloc.alloc()
		startKey := keyRange.StartKey
		if len(startKey) == 0 {
			startKey = prefix
		}
		endKey := keyRange.EndKey
		if len(endKey) == 0 {
			endKey = prefix.PrefixNext()
		}

		task := TableScanTask{
			ID:    taskID,
			Start: startKey,
			End:   endKey,
			ctx:   src.ctx,
		}
		batchTasks = append(batchTasks, task)
	}
	return batchTasks
}

// Close implements Operator interface.
func (src *TableScanTaskSource) Close() error {
	return src.errGroup.Wait()
}

// String implements fmt.Stringer interface.
func (*TableScanTaskSource) String() string {
	return "TableScanTaskSource"
}

// TableScanOperator scans table records in given key ranges from kv store.
type TableScanOperator struct {
	*operator.AsyncOperator[TableScanTask, IndexRecordChunk]
	logger     *zap.Logger
	totalCount *atomic.Int64
}

// NewTableScanOperator creates a new TableScanOperator.
func NewTableScanOperator(
	ctx *OperatorCtx,
	sessPool opSessPool,
	copCtx copr.CopContext,
	srcChkPool *sync.Pool,
	concurrency int,
	hintBatchSize int,
	reorgMeta *model.DDLReorgMeta,
	cpOp ingest.CheckpointOperator,
	collector execute.Collector,
) *TableScanOperator {
	totalCount := new(atomic.Int64)
	pool := workerpool.NewWorkerPool(
		"TableScanOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[TableScanTask, IndexRecordChunk] {
			return &tableScanWorker{
				ctx:           ctx,
				copCtx:        copCtx,
				sessPool:      sessPool,
				se:            nil,
				srcChkPool:    srcChkPool,
				cpOp:          cpOp,
				hintBatchSize: hintBatchSize,
				totalCount:    totalCount,
				reorgMeta:     reorgMeta,
				collector:     collector,
			}
		})
	return &TableScanOperator{
		AsyncOperator: operator.NewAsyncOperator(ctx, pool),
		logger:        logutil.Logger(ctx),
		totalCount:    totalCount,
	}
}

// Close implements operator.Operator interface.
func (o *TableScanOperator) Close() error {
	defer func() {
		o.logger.Info("table scan operator total count", zap.Int64("count", o.totalCount.Load()))
	}()
	return o.AsyncOperator.Close()
}

type tableScanWorker struct {
	ctx        *OperatorCtx
	copCtx     copr.CopContext
	sessPool   opSessPool
	se         *session.Session
	srcChkPool *sync.Pool

	cpOp          ingest.CheckpointOperator
	reorgMeta     *model.DDLReorgMeta
	hintBatchSize int
	totalCount    *atomic.Int64
	collector     execute.Collector
}

func (w *tableScanWorker) HandleTask(task TableScanTask, sender func(IndexRecordChunk)) {
	failpoint.Inject("injectPanicForTableScan", func() {
		panic("mock panic")
	})
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			logutil.Logger(w.ctx).Error("tableScanWorker get session from pool failed", zap.Error(err))
			w.ctx.onError(err)
			return
		}
		w.se = session.NewSession(sessCtx)
	}
	w.scanRecords(task, sender)
}

func (w *tableScanWorker) Close() {
	if w.se != nil {
		w.sessPool.Put(w.se.Context)
	}
}

func (w *tableScanWorker) scanRecords(task TableScanTask, sender func(IndexRecordChunk)) {
	logutil.Logger(w.ctx).Info("start a table scan task",
		zap.Int("id", task.ID), zap.Stringer("task", task))

	var (
		idxResults  []IndexRecordChunk
		execDetails kvutil.ExecDetails
	)
	var scanCtx context.Context = w.ctx
	if scanCtx.Value(kvutil.ExecDetailsKey) == nil {
		scanCtx = context.WithValue(w.ctx, kvutil.ExecDetailsKey, &execDetails)
	}
	err := wrapInBeginRollback(w.se, func(startTS uint64) error {
		failpoint.Inject("mockScanRecordError", func() {
			failpoint.Return(errors.New("mock scan record error"))
		})
		failpoint.InjectCall("scanRecordExec", w.reorgMeta)
		rs, err := buildTableScan(scanCtx, w.copCtx.GetBase(), startTS, task.Start, task.End)
		if err != nil {
			return err
		}
		if w.cpOp != nil {
			w.cpOp.AddChunk(task.ID, task.End)
		}
		var done bool
		for !done {
			failpoint.InjectCall("beforeGetChunk")
			srcChk := w.getChunk()
			done, err = fetchTableScanResult(scanCtx, w.copCtx.GetBase(), rs, srcChk)
			if err != nil || scanCtx.Err() != nil {
				w.recycleChunk(srcChk)
				terror.Call(rs.Close)
				return err
			}
			w.collector.Accepted(execDetails.UnpackedBytesReceivedKVTotal)
			execDetails = kvutil.ExecDetails{}
			idxResults = append(idxResults, IndexRecordChunk{ID: task.ID, Chunk: srcChk, Done: done, ctx: w.ctx})
		}
		return rs.Close()
	})
	if err != nil {
		w.ctx.onError(err)
	}
	for i, idxResult := range idxResults {
		sender(idxResult)
		rowCnt := idxResult.Chunk.NumRows()
		if w.cpOp != nil {
			done := i == len(idxResults)-1
			w.cpOp.UpdateChunk(task.ID, rowCnt, done)
		}
		w.totalCount.Add(int64(rowCnt))
	}
}

func (w *tableScanWorker) getChunk() *chunk.Chunk {
	targetCap := ingest.CopReadBatchSize(w.hintBatchSize)
	if w.reorgMeta != nil {
		targetCap = ingest.CopReadBatchSize(w.reorgMeta.GetBatchSize())
	}
	chk := w.srcChkPool.Get().(*chunk.Chunk)
	if chk.Capacity() != targetCap {
		chk = chunk.NewChunkWithCapacity(w.copCtx.GetBase().FieldTypes, targetCap)
		logutil.Logger(w.ctx).Info("adjust ddl job config success", zap.Int("current batch size", chk.Capacity()))
	}
	chk.Reset()
	return chk
}

func (w *tableScanWorker) recycleChunk(chk *chunk.Chunk) {
	w.srcChkPool.Put(chk)
}

// WriteExternalStoreOperator writes index records to external storage.
type WriteExternalStoreOperator struct {
	*operator.AsyncOperator[IndexRecordChunk, IndexWriteResult]
	logger     *zap.Logger
	totalCount *atomic.Int64
}

// NewWriteExternalStoreOperator creates a new WriteExternalStoreOperator.
func NewWriteExternalStoreOperator(
	ctx *OperatorCtx,
	copCtx copr.CopContext,
	sessPool opSessPool,
	taskID int64,
	subtaskID int64,
	tbl table.PhysicalTable,
	indexes []table.Index,
	store storage.ExternalStorage,
	srcChunkPool *sync.Pool,
	concurrency int,
	onClose external.OnWriterCloseFunc,
	memoryQuota uint64,
	reorgMeta *model.DDLReorgMeta,
	tikvCodec tikv.Codec,
	collector execute.Collector,
) *WriteExternalStoreOperator {
	onDuplicateKey := engineapi.OnDuplicateKeyError
	failpoint.Inject("ignoreReadIndexDupKey", func() {
		onDuplicateKey = engineapi.OnDuplicateKeyIgnore
	})

	totalCount := new(atomic.Int64)
	blockSize := external.GetAdjustedBlockSize(memoryQuota, external.DefaultBlockSize)
	pool := workerpool.NewWorkerPool(
		"WriteExternalStoreOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[IndexRecordChunk, IndexWriteResult] {
			writers := make([]ingest.Writer, 0, len(indexes))
			for i := range indexes {
				builder := external.NewWriterBuilder().
					SetOnCloseFunc(onClose).
					SetMemorySizeLimit(memoryQuota).
					SetTiKVCodec(tikvCodec).
					SetBlockSize(blockSize).
					SetGroupOffset(i).
					SetOnDup(onDuplicateKey)
				writerID := uuid.New().String()
				prefix := path.Join(strconv.Itoa(int(taskID)), strconv.Itoa(int(subtaskID)))
				writer := builder.Build(store, prefix, writerID)
				writers = append(writers, writer)
			}

			return &indexIngestWorker{
				ctx:          ctx,
				tbl:          tbl,
				indexes:      indexes,
				copCtx:       copCtx,
				se:           nil,
				sessPool:     sessPool,
				writers:      writers,
				srcChunkPool: srcChunkPool,
				reorgMeta:    reorgMeta,
				totalCount:   totalCount,
				collector:    collector,
			}
		})
	return &WriteExternalStoreOperator{
		AsyncOperator: operator.NewAsyncOperator(ctx, pool),
		logger:        logutil.Logger(ctx),
		totalCount:    totalCount,
	}
}

// Close implements operator.Operator interface.
func (o *WriteExternalStoreOperator) Close() error {
	err := o.AsyncOperator.Close()
	o.logger.Info("write external storage operator total count",
		zap.Int64("count", o.totalCount.Load()))
	return err
}

// IndexWriteResult contains the result of writing index records to ingest engine.
type IndexWriteResult struct {
	ID     int
	RowCnt int
}

// IndexIngestOperator writes index records to ingest engine.
type IndexIngestOperator struct {
	*operator.AsyncOperator[IndexRecordChunk, IndexWriteResult]
}

// NewIndexIngestOperator creates a new IndexIngestOperator.
func NewIndexIngestOperator(
	ctx *OperatorCtx,
	copCtx copr.CopContext,
	sessPool opSessPool,
	tbl table.PhysicalTable,
	indexes []table.Index,
	engines []ingest.Engine,
	srcChunkPool *sync.Pool,
	concurrency int,
	reorgMeta *model.DDLReorgMeta,
	collector execute.Collector,
) *IndexIngestOperator {
	writerCfg := getLocalWriterConfig(len(indexes), concurrency)

	var writerIDAlloc atomic.Int32
	pool := workerpool.NewWorkerPool(
		"indexIngestOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[IndexRecordChunk, IndexWriteResult] {
			writers := make([]ingest.Writer, 0, len(indexes))
			for i := range indexes {
				writerID := int(writerIDAlloc.Add(1))
				writer, err := engines[i].CreateWriter(writerID, writerCfg)
				if err != nil {
					logutil.Logger(ctx).Error("create index ingest worker failed", zap.Error(err))
					ctx.onError(err)
					return nil
				}
				writers = append(writers, writer)
			}

			return &indexIngestWorker{
				ctx:     ctx,
				tbl:     tbl,
				indexes: indexes,
				copCtx:  copCtx,

				se:           nil,
				sessPool:     sessPool,
				writers:      writers,
				srcChunkPool: srcChunkPool,
				reorgMeta:    reorgMeta,
				collector:    collector,
			}
		})
	return &IndexIngestOperator{
		AsyncOperator: operator.NewAsyncOperator(ctx, pool),
	}
}

type indexIngestWorker struct {
	ctx *OperatorCtx

	tbl       table.PhysicalTable
	indexes   []table.Index
	reorgMeta *model.DDLReorgMeta

	copCtx   copr.CopContext
	sessPool opSessPool
	se       *session.Session
	restore  func(sessionctx.Context)

	writers      []ingest.Writer
	srcChunkPool *sync.Pool
	// only available in global sort
	totalCount *atomic.Int64
	collector  execute.Collector
}

func (w *indexIngestWorker) HandleTask(ck IndexRecordChunk, send func(IndexWriteResult)) {
	defer func() {
		if ck.Chunk != nil {
			w.srcChunkPool.Put(ck.Chunk)
		}
	}()
	failpoint.InjectCall("mockIndexIngestWorkerFault")

	result := IndexWriteResult{
		ID: ck.ID,
	}
	w.initSessCtx()
	count, bytes, err := w.WriteChunk(&ck)
	if err != nil {
		w.ctx.onError(err)
		return
	}
	w.collector.Processed(int64(bytes), int64(count))
	if count == 0 {
		logutil.Logger(w.ctx).Info("finish a index ingest task", zap.Int("id", ck.ID))
		return
	}
	if w.totalCount != nil {
		w.totalCount.Add(int64(count))
	}
	result.RowCnt = count
	if ResultCounterForTest != nil {
		ResultCounterForTest.Add(1)
	}
	send(result)
}

func (w *indexIngestWorker) initSessCtx() {
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			w.ctx.onError(err)
			return
		}
		w.restore = restoreSessCtx(sessCtx)
		if err := initSessCtx(sessCtx, w.reorgMeta); err != nil {
			w.ctx.onError(err)
			return
		}
		w.se = session.NewSession(sessCtx)
	}
}

func (w *indexIngestWorker) Close() {
	// TODO(lance6716): unify the real write action for engineInfo and external
	// writer.
	for i, writer := range w.writers {
		ew, ok := writer.(*external.Writer)
		if !ok {
			break
		}
		err := ew.Close(w.ctx)
		if err != nil {
			err = ingest.TryConvertToKeyExistsErr(err, w.indexes[i].Meta(), w.tbl.Meta())
			w.ctx.onError(err)
		}
	}
	if w.se != nil {
		w.restore(w.se.Context)
		w.sessPool.Put(w.se.Context)
	}
}

// WriteChunk will write index records to lightning engine.
func (w *indexIngestWorker) WriteChunk(rs *IndexRecordChunk) (count int, bytes int, err error) {
	failpoint.Inject("mockWriteLocalError", func(_ failpoint.Value) {
		failpoint.Return(0, 0, errors.New("mock write local error"))
	})
	failpoint.InjectCall("writeLocalExec", rs.Done)

	oprStartTime := time.Now()
	vars := w.se.GetSessionVars() //nolint:forbidigo
	sc := vars.StmtCtx
	cnt, kvBytes, err := writeChunk(w.ctx, w.writers, w.indexes, w.copCtx, sc.TimeZone(), sc.ErrCtx(), vars.GetWriteStmtBufs(), rs.Chunk, w.tbl.Meta())
	if err != nil || cnt == 0 {
		return 0, 0, err
	}
	logSlowOperations(time.Since(oprStartTime), "writeChunk", 3000)
	return cnt, kvBytes, nil
}

type indexWriteResultSink struct {
	ctx        *OperatorCtx
	backendCtx ingest.BackendCtx
	tbl        table.PhysicalTable
	indexes    []table.Index

	collector execute.Collector

	errGroup errgroup.Group
	source   operator.DataChannel[IndexWriteResult]
}

func newIndexWriteResultSink(
	ctx *OperatorCtx,
	backendCtx ingest.BackendCtx,
	tbl table.PhysicalTable,
	indexes []table.Index,
	collector execute.Collector,
) *indexWriteResultSink {
	return &indexWriteResultSink{
		ctx:        ctx,
		backendCtx: backendCtx,
		tbl:        tbl,
		indexes:    indexes,
		errGroup:   errgroup.Group{},
		collector:  collector,
	}
}

func (s *indexWriteResultSink) SetSource(source operator.DataChannel[IndexWriteResult]) {
	s.source = source
}

func (s *indexWriteResultSink) Open() error {
	s.errGroup.Go(s.collectResult)
	return nil
}

func (s *indexWriteResultSink) collectResult() error {
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case rs, ok := <-s.source.Channel():
			if !ok {
				err := s.flush()
				if err != nil {
					s.ctx.onError(err)
				}
				if s.backendCtx != nil { // for local sort only
					total := s.backendCtx.TotalKeyCount()
					if total > 0 {
						if lc, ok := s.collector.(*localRowCntCollector); ok {
							lc.SetTotal(total)
						}
					}
				}
				return err
			}
			if s.backendCtx != nil { // for local sort only
				err := s.backendCtx.IngestIfQuotaExceeded(s.ctx, rs.ID, rs.RowCnt)
				if err != nil {
					s.ctx.onError(err)
					return err
				}
			}
		}
	}
}

func (s *indexWriteResultSink) flush() error {
	if s.backendCtx == nil {
		return nil
	}
	failpoint.Inject("mockFlushError", func(_ failpoint.Value) {
		failpoint.Return(errors.New("mock flush error"))
	})
	return s.backendCtx.Ingest(s.ctx)
}

func (s *indexWriteResultSink) Close() error {
	return s.errGroup.Wait()
}

func (*indexWriteResultSink) String() string {
	return "indexWriteResultSink"
}

// tempIndexScanTask contains the start key and end key of a temp index region.
type tempIndexScanTask struct {
	ID    int
	Start kv.Key
	End   kv.Key

	ctx *OperatorCtx
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (t tempIndexScanTask) RecoverArgs() (metricsLabel string, funcInfo string, recoverFn func(), quit bool) {
	return metrics.LblAddIndex, "RecoverArgs", func() {
		t.ctx.onError(dbterror.ErrReorgPanic)
	}, false
}

// String implement fmt.Stringer interface.
func (t tempIndexScanTask) String() string {
	return fmt.Sprintf("TempIndexScanTask: id=%d, startKey=%s, endKey=%s",
		t.ID, hex.EncodeToString(t.Start), hex.EncodeToString(t.End))
}

// TempIndexScanTaskSource produces TempIndexScanTask by splitting regions of a temp index range.
type TempIndexScanTaskSource struct {
	ctx *OperatorCtx

	errGroup errgroup.Group
	sink     operator.DataChannel[tempIndexScanTask]

	tbl      table.PhysicalTable
	store    kv.Storage
	startKey kv.Key
	endKey   kv.Key
}

// NewTempIndexScanTaskSource creates a new TempIndexScanTaskSource.
func NewTempIndexScanTaskSource(
	ctx *OperatorCtx,
	store kv.Storage,
	physicalTable table.PhysicalTable,
	startKey kv.Key,
	endKey kv.Key,
) *TempIndexScanTaskSource {
	return &TempIndexScanTaskSource{
		ctx:      ctx,
		errGroup: errgroup.Group{},
		tbl:      physicalTable,
		store:    store,
		startKey: startKey,
		endKey:   endKey,
	}
}

// SetSink implements WithSink interface.
func (src *TempIndexScanTaskSource) SetSink(sink operator.DataChannel[tempIndexScanTask]) {
	src.sink = sink
}

// Open implements Operator interface.
func (src *TempIndexScanTaskSource) Open() error {
	src.errGroup.Go(src.generateTasks)
	return nil
}

// Close implements Operator interface.
func (src *TempIndexScanTaskSource) Close() error {
	return src.errGroup.Wait()
}

// String implements fmt.Stringer interface.
func (*TempIndexScanTaskSource) String() string {
	return "TempIndexScanTaskSource"
}

func (src *TempIndexScanTaskSource) generateTasks() error {
	taskIDAlloc := newTaskIDAllocator()
	defer src.sink.Finish()

	startKey := src.startKey
	for {
		kvRanges, err := loadTableRanges(
			src.ctx,
			src.tbl.GetPhysicalID(),
			src.store,
			startKey,
			src.endKey,
			nil,
			backfillTaskChanSize,
		)
		if err != nil {
			return err
		}
		if len(kvRanges) == 0 {
			break
		}

		batchTasks := src.getBatchTempIndexScanTask(kvRanges, taskIDAlloc)
		for _, task := range batchTasks {
			select {
			case <-src.ctx.Done():
				return src.ctx.Err()
			case src.sink.Channel() <- task:
			}
		}
		startKey = kvRanges[len(kvRanges)-1].EndKey
		if startKey.Cmp(src.endKey) >= 0 {
			break
		}
	}
	return nil
}

func (src *TempIndexScanTaskSource) getBatchTempIndexScanTask(
	kvRanges []kv.KeyRange,
	taskIDAlloc *taskIDAllocator,
) []tempIndexScanTask {
	batchTasks := make([]tempIndexScanTask, 0, len(kvRanges))
	prefix := tablecodec.GenTableIndexPrefix(src.tbl.GetPhysicalID())

	// Build reorg tasks.
	for _, keyRange := range kvRanges {
		taskID := taskIDAlloc.alloc()
		startKey := keyRange.StartKey
		if len(startKey) == 0 {
			startKey = prefix
		}
		endKey := keyRange.EndKey
		if len(endKey) == 0 {
			endKey = prefix.PrefixNext()
		}

		task := tempIndexScanTask{
			ID:    taskID,
			Start: startKey,
			End:   endKey,
			ctx:   src.ctx,
		}
		batchTasks = append(batchTasks, task)
	}
	return batchTasks
}

// MergeTempIndexOperator merges the temporary index records into the original index.
type MergeTempIndexOperator struct {
	*operator.AsyncOperator[tempIndexScanTask, tempIdxResult]
	logger     *zap.Logger
	totalCount *atomic.Int64
}

// NewMergeTempIndexOperator creates a new MergeTempIndexOperator.
func NewMergeTempIndexOperator(
	ctx *OperatorCtx,
	store kv.Storage,
	ptbl table.PhysicalTable,
	idxInfo *model.IndexInfo,
	jobID int64,
	concurrency int,
	batchSize int,
	reorgMeta *model.DDLReorgMeta,
) *MergeTempIndexOperator {
	totalCount := new(atomic.Int64)
	pool := workerpool.NewWorkerPool(
		"MergeTempIndexOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[tempIndexScanTask, tempIdxResult] {
			return &mergeTempIndexWorker{
				ctx:        ctx,
				store:      store,
				ptbl:       ptbl,
				idxInfo:    idxInfo,
				jobID:      jobID,
				batchCnt:   batchSize,
				totalCount: totalCount,
				reorgMeta:  reorgMeta,
				buffers:    newTempIdxBuffers(batchSize),
			}
		})
	return &MergeTempIndexOperator{
		AsyncOperator: operator.NewAsyncOperator(ctx, pool),
		logger:        logutil.Logger(ctx),
		totalCount:    totalCount,
	}
}

// Close implements operator.Operator interface.
func (o *MergeTempIndexOperator) Close() error {
	defer func() {
		o.logger.Info("merge temp index operator total count", zap.Int64("count", o.totalCount.Load()))
	}()
	return o.AsyncOperator.Close()
}

type mergeTempIndexWorker struct {
	ctx       *OperatorCtx
	store     kv.Storage
	ptbl      table.PhysicalTable
	idxInfo   *model.IndexInfo
	reorgMeta *model.DDLReorgMeta
	jobID     int64

	batchCnt   int
	buffers    *tempIdxBuffers
	totalCount *atomic.Int64
}

func (w *mergeTempIndexWorker) HandleTask(task tempIndexScanTask, sender func(tempIdxResult)) {
	failpoint.Inject("injectPanicForTableScan", func() {
		panic("mock panic")
	})
	start := task.Start
	done := false
	for !done {
		task.Start = start
		rs, err := w.handleOneRange(task)
		if err != nil {
			w.ctx.onError(err)
			return
		}
		sender(rs)
		done = rs.done
		start = rs.nextKey
	}
}

func (*mergeTempIndexWorker) Close() {}

func (w *mergeTempIndexWorker) handleOneRange(
	task tempIndexScanTask,
) (tempIdxResult, error) {
	var currentTxnStartTS uint64
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(w.ctx, "ddl_merge_temp_index", kvutil.ExplicitTypeDDL)
	originBatchCnt := w.batchCnt
	defer func() {
		w.batchCnt = originBatchCnt
	}()
	jobCtx := NewReorgContext()
	jobCtx.tp = "ddl_merge_temp_index"
	jobCtx.getResourceGroupTaggerForTopSQL()
	jobCtx.resourceGroupName = w.reorgMeta.ResourceGroupName

	start, end := task.Start, task.End
	attempts := 0
	var result tempIdxResult
	for {
		attempts++
		err := kv.RunInNewTxn(ctx, w.store, false, func(_ context.Context, txn kv.Transaction) error {
			currentTxnStartTS = txn.StartTS()
			updateTxnEntrySizeLimitIfNeeded(txn)
			rs, err := fetchTempIndexVals(jobCtx, w.store, w.ptbl, w.idxInfo, txn, start, end, w.batchCnt, w.buffers)
			if err != nil {
				return errors.Trace(err)
			}
			result = rs
			err = batchCheckTemporaryUniqueKey(txn, w.ptbl, w.idxInfo, w.buffers.originIdxKeys, w.buffers.tmpIdxRecords)
			if err != nil {
				return errors.Trace(err)
			}

			for i, idxRecord := range w.buffers.tmpIdxRecords {
				// The index is already exists, we skip it, no needs to backfill it.
				// The following update, delete, insert on these rows, TiDB can handle it correctly.
				// If all batch are skipped, update first index key to make txn commit to release lock.
				if idxRecord.skip {
					continue
				}

				originIdxKey := w.buffers.originIdxKeys[i]
				if idxRecord.delete {
					err = txn.GetMemBuffer().Delete(originIdxKey)
				} else {
					err = txn.GetMemBuffer().Set(originIdxKey, idxRecord.vals)
				}
				if err != nil {
					return err
				}

				err = txn.GetMemBuffer().Delete(w.buffers.tmpIdxKeys[i])
				if err != nil {
					return err
				}

				failpoint.InjectCall("mockDMLExecutionMergingInTxn")

				result.addCount++
			}
			return nil
		})
		if err != nil {
			if kv.IsTxnRetryableError(err) {
				if w.batchCnt > 1 {
					w.batchCnt /= 2
				}
				backoff := kv.BackOff(uint(attempts))
				logutil.Logger(ctx).Warn("temp index merge worker retry",
					zap.Int64("jobID", w.jobID),
					zap.Int("batchCnt", w.batchCnt),
					zap.Int("attempts", attempts),
					zap.Duration("backoff", time.Duration(backoff)),
					zap.Uint64("startTS", currentTxnStartTS),
					zap.Error(err))
				continue
			}
			w.ctx.onError(err)
			return result, err
		}
		break
	}

	metrics.DDLSetTempIndexScanAndMerge(w.ptbl.GetPhysicalID(), uint64(result.scanCount), uint64(result.addCount))
	failpoint.Inject("mockDMLExecutionMerging", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecutionMerging != nil {
			MockDMLExecutionMerging()
		}
	})
	logSlowOperations(time.Since(oprStartTime), "mergeTempIndexExecutorHandleOneRange", 3000)
	w.totalCount.Add(int64(result.scanCount))
	return result, nil
}

type tempIndexResultSink struct {
	ctx       *OperatorCtx
	tbl       table.PhysicalTable
	collector execute.Collector
	errGroup  errgroup.Group
	source    operator.DataChannel[tempIdxResult]
}

func newTempIndexResultSink(
	ctx *OperatorCtx,
	tbl table.PhysicalTable,
	collector execute.Collector,
) *tempIndexResultSink {
	return &tempIndexResultSink{
		ctx:       ctx,
		tbl:       tbl,
		errGroup:  errgroup.Group{},
		collector: collector,
	}
}

func (s *tempIndexResultSink) SetSource(source operator.DataChannel[tempIdxResult]) {
	s.source = source
}

func (s *tempIndexResultSink) Open() error {
	s.errGroup.Go(s.collectResult)
	return nil
}

func (s *tempIndexResultSink) collectResult() error {
	for {
		select {
		case <-s.ctx.Done():
			logutil.BgLogger().Info("temp index result sink context done", zap.Error(s.ctx.Err()))
			return s.ctx.Err()
		case rs, ok := <-s.source.Channel():
			if !ok {
				return nil
			}
			s.collector.Processed(0, int64(rs.addCount))
		}
	}
}

func (s *tempIndexResultSink) Close() error {
	return s.errGroup.Wait()
}

func (*tempIndexResultSink) String() string {
	return "tempIndexResultSink"
}
