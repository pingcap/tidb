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
	"github.com/pingcap/tidb/pkg/disttask/operator"
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
func NewDistTaskOperatorCtx(
	ctx context.Context,
	taskID, subtaskID int64,
) (*OperatorCtx, context.CancelFunc) {
	opCtx, cancel := context.WithCancel(ctx)
	opCtx = logutil.WithFields(opCtx,
		zap.Int64("task-id", taskID),
		zap.Int64("subtask-id", subtaskID))
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
	_ RowCountListener = (*EmptyRowCntListener)(nil)
	_ RowCountListener = (*distTaskRowCntListener)(nil)
	_ RowCountListener = (*localRowCntListener)(nil)
)

// RowCountListener is invoked when some index records are flushed to disk or imported to TiKV.
type RowCountListener interface {
	Written(rowCnt int)
	SetTotal(total int)
}

// EmptyRowCntListener implements a noop RowCountListener.
type EmptyRowCntListener struct{}

// Written implements RowCountListener.
func (*EmptyRowCntListener) Written(_ int) {}

// SetTotal implements RowCountListener.
func (*EmptyRowCntListener) SetTotal(_ int) {}

// MockDMLExecutionBeforeScan is only used for test.
var MockDMLExecutionBeforeScan func()

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
	rowCntListener RowCountListener,
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
	readerCnt, writerCnt := expectedIngestWorkerCnt(concurrency, avgRowSize)
	rm := reorgMeta
	if rm.UseCloudStorage {
		// param cannot be modified at runtime for global sort right now.
		rm = nil
	}

	failpoint.Inject("mockDMLExecutionBeforeScan", func(_ failpoint.Value) {
		if MockDMLExecutionBeforeScan != nil {
			MockDMLExecutionBeforeScan()
		}
	})

	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey, backendCtx)
	scanOp := NewTableScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt,
		reorgMeta.GetBatchSize(), rm, backendCtx)
	ingestOp := NewIndexIngestOperator(ctx, copCtx, sessPool,
		tbl, indexes, engines, srcChkPool, writerCnt, reorgMeta)
	sinkOp := newIndexWriteResultSink(ctx, backendCtx, tbl, indexes, rowCntListener)

	operator.Compose[TableScanTask](srcOp, scanOp)
	operator.Compose[IndexRecordChunk](scanOp, ingestOp)
	operator.Compose[IndexWriteResult](ingestOp, sinkOp)

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
	extStoreURI string,
	sessPool opSessPool,
	jobID, subtaskID int64,
	tbl table.PhysicalTable,
	idxInfos []*model.IndexInfo,
	startKey, endKey kv.Key,
	onClose external.OnCloseFunc,
	reorgMeta *model.DDLReorgMeta,
	avgRowSize int,
	concurrency int,
	resource *proto.StepResource,
	rowCntListener RowCountListener,
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
	readerCnt, writerCnt := expectedIngestWorkerCnt(concurrency, avgRowSize)

	backend, err := storage.ParseBackend(extStoreURI, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return nil, err
	}
	memCap := resource.Mem.Capacity()
	memSizePerIndex := uint64(memCap / int64(writerCnt*2*len(idxInfos)))
	failpoint.Inject("mockWriterMemSize", func() {
		memSizePerIndex = 1 * size.GB
	})

	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey, nil)
	scanOp := NewTableScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt,
		reorgMeta.GetBatchSize(), nil, nil)
	writeOp := NewWriteExternalStoreOperator(
		ctx, copCtx, sessPool, jobID, subtaskID,
		tbl, indexes, extStore, srcChkPool, writerCnt,
		onClose, memSizePerIndex, reorgMeta, tikvCodec,
	)
	sinkOp := newIndexWriteResultSink(ctx, nil, tbl, indexes, rowCntListener)

	operator.Compose[TableScanTask](srcOp, scanOp)
	operator.Compose[IndexRecordChunk](scanOp, writeOp)
	operator.Compose[IndexWriteResult](writeOp, sinkOp)

	logutil.Logger(ctx).Info("build add index cloud storage operators",
		zap.Int64("jobID", jobID),
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
		if intest.InTest {
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
			src.tbl,
			src.store,
			startKey,
			src.endKey,
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
			}
		})
	return &TableScanOperator{
		AsyncOperator: operator.NewAsyncOperator[TableScanTask, IndexRecordChunk](ctx, pool),
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

	var idxResult IndexRecordChunk
	err := wrapInBeginRollback(w.se, func(startTS uint64) error {
		failpoint.Inject("mockScanRecordError", func() {
			failpoint.Return(errors.New("mock scan record error"))
		})
		failpoint.InjectCall("scanRecordExec", w.reorgMeta)
		rs, err := buildTableScan(w.ctx, w.copCtx.GetBase(), startTS, task.Start, task.End)
		if err != nil {
			return err
		}
		if w.cpOp != nil {
			w.cpOp.AddChunk(task.ID, task.End)
		}
		var done bool
		for !done {
			srcChk := w.getChunk()
			done, err = fetchTableScanResult(w.ctx, w.copCtx.GetBase(), rs, srcChk)
			if err != nil || w.ctx.Err() != nil {
				w.recycleChunk(srcChk)
				terror.Call(rs.Close)
				return err
			}
			idxResult = IndexRecordChunk{ID: task.ID, Chunk: srcChk, Done: done, ctx: w.ctx}
			if w.cpOp != nil {
				w.cpOp.UpdateChunk(task.ID, srcChk.NumRows(), done)
			}
			w.totalCount.Add(int64(srcChk.NumRows()))
			sender(idxResult)
		}
		return rs.Close()
	})
	if err != nil {
		w.ctx.onError(err)
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
	jobID int64,
	subtaskID int64,
	tbl table.PhysicalTable,
	indexes []table.Index,
	store storage.ExternalStorage,
	srcChunkPool *sync.Pool,
	concurrency int,
	onClose external.OnCloseFunc,
	memoryQuota uint64,
	reorgMeta *model.DDLReorgMeta,
	tikvCodec tikv.Codec,
) *WriteExternalStoreOperator {
	// due to multi-schema-change, we may merge processing multiple indexes into one
	// local backend.
	hasUnique := false
	for _, index := range indexes {
		if index.Meta().Unique {
			hasUnique = true
			break
		}
	}

	totalCount := new(atomic.Int64)
	blockSize := external.GetAdjustedBlockSize(memoryQuota)
	pool := workerpool.NewWorkerPool(
		"WriteExternalStoreOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[IndexRecordChunk, IndexWriteResult] {
			writers := make([]ingest.Writer, 0, len(indexes))
			for i := range indexes {
				builder := external.NewWriterBuilder().
					SetOnCloseFunc(onClose).
					SetKeyDuplicationEncoding(hasUnique).
					SetMemorySizeLimit(memoryQuota).
					SetTiKVCodec(tikvCodec).
					SetBlockSize(blockSize).
					SetGroupOffset(i)
				writerID := uuid.New().String()
				prefix := path.Join(strconv.Itoa(int(jobID)), strconv.Itoa(int(subtaskID)))
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
			}
		})
	return &WriteExternalStoreOperator{
		AsyncOperator: operator.NewAsyncOperator[IndexRecordChunk, IndexWriteResult](ctx, pool),
		logger:        logutil.Logger(ctx),
		totalCount:    totalCount,
	}
}

// Close implements operator.Operator interface.
func (o *WriteExternalStoreOperator) Close() error {
	o.logger.Info("write external storage operator total count",
		zap.Int64("count", o.totalCount.Load()))
	return o.AsyncOperator.Close()
}

// IndexWriteResult contains the result of writing index records to ingest engine.
type IndexWriteResult struct {
	ID    int
	Added int
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
			}
		})
	return &IndexIngestOperator{
		AsyncOperator: operator.NewAsyncOperator[IndexRecordChunk, IndexWriteResult](ctx, pool),
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
}

func (w *indexIngestWorker) HandleTask(ck IndexRecordChunk, send func(IndexWriteResult)) {
	defer func() {
		if ck.Chunk != nil {
			w.srcChunkPool.Put(ck.Chunk)
		}
	}()
	failpoint.Inject("injectPanicForIndexIngest", func() {
		panic("mock panic")
	})

	result := IndexWriteResult{
		ID: ck.ID,
	}
	w.initSessCtx()
	count, _, err := w.WriteChunk(&ck)
	if err != nil {
		w.ctx.onError(err)
		return
	}
	if count == 0 {
		logutil.Logger(w.ctx).Info("finish a index ingest task", zap.Int("id", ck.ID))
		return
	}
	if w.totalCount != nil {
		w.totalCount.Add(int64(count))
	}
	result.Added = count
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
	for _, writer := range w.writers {
		ew, ok := writer.(*external.Writer)
		if !ok {
			break
		}
		err := ew.Close(w.ctx)
		if err != nil {
			w.ctx.onError(err)
		}
	}
	if w.se != nil {
		w.restore(w.se.Context)
		w.sessPool.Put(w.se.Context)
	}
}

// WriteChunk will write index records to lightning engine.
func (w *indexIngestWorker) WriteChunk(rs *IndexRecordChunk) (count int, nextKey kv.Key, err error) {
	failpoint.Inject("mockWriteLocalError", func(_ failpoint.Value) {
		failpoint.Return(0, nil, errors.New("mock write local error"))
	})
	failpoint.InjectCall("writeLocalExec", rs.Done)

	oprStartTime := time.Now()
	vars := w.se.GetSessionVars()
	sc := vars.StmtCtx
	cnt, lastHandle, err := writeChunkToLocal(w.ctx, w.writers, w.indexes, w.copCtx, sc.TimeZone(), sc.ErrCtx(), vars.GetWriteStmtBufs(), rs.Chunk)
	if err != nil || cnt == 0 {
		return 0, nil, err
	}
	logSlowOperations(time.Since(oprStartTime), "writeChunkToLocal", 3000)
	nextKey = tablecodec.EncodeRecordKey(w.tbl.RecordPrefix(), lastHandle)
	return cnt, nextKey, nil
}

type indexWriteResultSink struct {
	ctx        *OperatorCtx
	backendCtx ingest.BackendCtx
	tbl        table.PhysicalTable
	indexes    []table.Index

	rowCntListener RowCountListener

	errGroup errgroup.Group
	source   operator.DataChannel[IndexWriteResult]
}

func newIndexWriteResultSink(
	ctx *OperatorCtx,
	backendCtx ingest.BackendCtx,
	tbl table.PhysicalTable,
	indexes []table.Index,
	rowCntListener RowCountListener,
) *indexWriteResultSink {
	return &indexWriteResultSink{
		ctx:            ctx,
		backendCtx:     backendCtx,
		tbl:            tbl,
		indexes:        indexes,
		errGroup:       errgroup.Group{},
		rowCntListener: rowCntListener,
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
						s.rowCntListener.SetTotal(total)
					}
				}
				return err
			}
			s.rowCntListener.Written(rs.Added)
			if s.backendCtx != nil { // for local sort only
				err := s.backendCtx.IngestIfQuotaExceeded(s.ctx, rs.ID, rs.Added)
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
