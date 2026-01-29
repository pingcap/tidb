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
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
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

// NewLocalWorkerCtx is used for adding index with local ingest mode.
func NewLocalWorkerCtx(ctx context.Context, jobID int64) *workerpool.Context {
	ctx = logutil.WithFields(ctx, zap.Int64("jobID", jobID))
	return workerpool.NewContext(ctx)
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
	ctx *workerpool.Context,
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

	failpoint.Inject("mockDMLExecutionBeforeScan", func(_ failpoint.Value) {
		if MockDMLExecutionBeforeScan != nil {
			MockDMLExecutionBeforeScan()
		}
	})
	failpoint.InjectCall("mockDMLExecutionBeforeScanV2")
	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey, backendCtx)
	scanOp := NewTableScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt,
		reorgMeta.GetBatchSizeOrDefault(int(variable.GetDDLReorgBatchSize())), reorgMeta, backendCtx)
	ingestOp := NewIndexIngestOperator(ctx, copCtx, backendCtx, sessPool,
		tbl, indexes, engines, srcChkPool, writerCnt, reorgMeta, rowCntListener)
	sinkOp := newIndexWriteResultSink(ctx, backendCtx, tbl, indexes, rowCntListener)

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
	ctx *workerpool.Context,
	store kv.Storage,
	extStoreURI string,
	sessPool opSessPool,
	taskID, subtaskID int64,
	tbl table.PhysicalTable,
	idxInfos []*model.IndexInfo,
	startKey, endKey kv.Key,
	onClose external.OnCloseFunc,
	reorgMeta *model.DDLReorgMeta,
	avgRowSize int,
	concurrency int,
	resource *proto.StepResource,
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
	failpoint.Inject("mockWriterMemSizeInKB", func(val failpoint.Value) {
		if v, ok := val.(int); ok {
			memSizePerIndex = uint64(v) * size.KB
		}
	})

	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey, nil)
	scanOp := NewTableScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt,
		reorgMeta.GetBatchSizeOrDefault(int(variable.GetDDLReorgBatchSize())), reorgMeta, nil)
	writeOp := NewWriteExternalStoreOperator(
		ctx, copCtx, sessPool, taskID, subtaskID,
		tbl, indexes, extStore, srcChkPool, writerCnt,
		onClose, memSizePerIndex, reorgMeta, rowCntListener,
	)
	sinkOp := newIndexWriteResultSink(ctx, nil, tbl, indexes, rowCntListener)

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
				reorgMeta.GetBatchSizeOrDefault(int(variable.GetDDLReorgBatchSize())))
		},
	}
}

// TableScanTask contains the start key and the end key of a region.
type TableScanTask struct {
	ID    int
	Start kv.Key
	End   kv.Key

	ctx *workerpool.Context
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (TableScanTask) RecoverArgs() (metricsLabel string, funcInfo string, err error) {
	return metrics.LblAddIndex, "TableScanTask", dbterror.ErrReorgPanic
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
}

// RecoverArgs implements workerpool.TaskMayPanic interface.
func (IndexRecordChunk) RecoverArgs() (metricsLabel string, funcInfo string, err error) {
	return metrics.LblAddIndex, "IndexRecordChunk", dbterror.ErrReorgPanic
}

// TableScanTaskSource produces TableScanTask by splitting table records into ranges.
type TableScanTaskSource struct {
	ctx *workerpool.Context

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
	ctx *workerpool.Context,
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
	ctx *workerpool.Context,
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
	ctx        *workerpool.Context
	copCtx     copr.CopContext
	sessPool   opSessPool
	se         *session.Session
	srcChkPool *sync.Pool

	cpOp          ingest.CheckpointOperator
	reorgMeta     *model.DDLReorgMeta
	hintBatchSize int
	totalCount    *atomic.Int64
}

func (w *tableScanWorker) HandleTask(task TableScanTask, sender func(IndexRecordChunk)) error {
	failpoint.Inject("injectPanicForTableScan", func() {
		panic("mock panic")
	})
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			logutil.Logger(w.ctx).Error("tableScanWorker get session from pool failed", zap.Error(err))
			return err
		}
		w.se = session.NewSession(sessCtx)
	}

	return w.scanRecords(task, sender)
}

func (w *tableScanWorker) Close() error {
	if w.se != nil {
		w.sessPool.Put(w.se.Context)
	}

	return nil
}

func (w *tableScanWorker) scanRecords(task TableScanTask, sender func(IndexRecordChunk)) error {
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
			idxResult = IndexRecordChunk{ID: task.ID, Chunk: srcChk, Done: done}
			if w.cpOp != nil {
				w.cpOp.UpdateChunk(task.ID, srcChk.NumRows(), done)
			}
			w.totalCount.Add(int64(srcChk.NumRows()))
			sender(idxResult)
		}
		return rs.Close()
	})

	return err
}

func (w *tableScanWorker) getChunk() *chunk.Chunk {
	targetCap := ingest.CopReadBatchSize(w.hintBatchSize)
	if w.reorgMeta != nil {
		targetCap = ingest.CopReadBatchSize(w.reorgMeta.GetBatchSizeOrDefault(int(variable.GetDDLReorgBatchSize())))
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
	ctx *workerpool.Context,
	copCtx copr.CopContext,
	sessPool opSessPool,
	taskID int64,
	subtaskID int64,
	tbl table.PhysicalTable,
	indexes []table.Index,
	store storage.ExternalStorage,
	srcChunkPool *sync.Pool,
	concurrency int,
	onClose external.OnCloseFunc,
	memoryQuota uint64,
	reorgMeta *model.DDLReorgMeta,
	rowCntListener RowCountListener,
) *WriteExternalStoreOperator {
	onDuplicateKey := common.OnDuplicateKeyError
	failpoint.Inject("ignoreReadIndexDupKey", func() {
		onDuplicateKey = common.OnDuplicateKeyIgnore
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
					SetBlockSize(blockSize).
					SetGroupOffset(i).
					SetOnDup(onDuplicateKey)
				writerID := uuid.New().String()
				prefix := path.Join(strconv.Itoa(int(taskID)), strconv.Itoa(int(subtaskID)))
				writer := builder.Build(store, prefix, writerID)
				writers = append(writers, writer)
			}

			return &indexIngestExternalWorker{
				indexIngestBaseWorker: indexIngestBaseWorker{
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
				},
				rowCntListener: rowCntListener,
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
	o.logger.Info("write external storage operator total count",
		zap.Int64("count", o.totalCount.Load()))
	return o.AsyncOperator.Close()
}

// IndexWriteResult contains the result of writing index records to ingest engine.
type IndexWriteResult struct {
	ID    int
	Added int
	Total int
}

// IndexIngestOperator writes index records to ingest engine.
type IndexIngestOperator struct {
	*operator.AsyncOperator[IndexRecordChunk, IndexWriteResult]
}

// NewIndexIngestOperator creates a new IndexIngestOperator.
func NewIndexIngestOperator(
	ctx *workerpool.Context,
	copCtx copr.CopContext,
	backendCtx ingest.BackendCtx,
	sessPool opSessPool,
	tbl table.PhysicalTable,
	indexes []table.Index,
	engines []ingest.Engine,
	srcChunkPool *sync.Pool,
	concurrency int,
	reorgMeta *model.DDLReorgMeta,
	rowCntListener RowCountListener,
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
					ctx.OnError(err)
					return nil
				}
				writers = append(writers, writer)
			}

			return &indexIngestLocalWorker{
				indexIngestBaseWorker: indexIngestBaseWorker{
					ctx:     ctx,
					tbl:     tbl,
					indexes: indexes,
					copCtx:  copCtx,

					se:           nil,
					sessPool:     sessPool,
					writers:      writers,
					srcChunkPool: srcChunkPool,
					reorgMeta:    reorgMeta,
				},
				backendCtx:     backendCtx,
				rowCntListener: rowCntListener,
			}
		})
	return &IndexIngestOperator{
		AsyncOperator: operator.NewAsyncOperator[IndexRecordChunk, IndexWriteResult](ctx, pool),
	}
}

type indexIngestExternalWorker struct {
	indexIngestBaseWorker
	rowCntListener RowCountListener
}

func (w *indexIngestExternalWorker) HandleTask(ck IndexRecordChunk, send func(IndexWriteResult)) error {
	defer func() {
		if ck.Chunk != nil {
			w.srcChunkPool.Put(ck.Chunk)
		}
	}()
	rs, err := w.indexIngestBaseWorker.HandleTask(ck)
	if err != nil {
		return err
	}
	w.rowCntListener.Written(rs.Added)
	send(rs)
	return nil
}

type indexIngestLocalWorker struct {
	indexIngestBaseWorker
	backendCtx     ingest.BackendCtx
	rowCntListener RowCountListener
}

func (w *indexIngestLocalWorker) HandleTask(ck IndexRecordChunk, send func(IndexWriteResult)) error {
	defer func() {
		if ck.Chunk != nil {
			w.srcChunkPool.Put(ck.Chunk)
		}
	}()
	rs, err := w.indexIngestBaseWorker.HandleTask(ck)
	if err != nil {
		return err
	}
	if rs.Added == 0 {
		return nil
	}
	w.rowCntListener.Written(rs.Added)
	err = w.backendCtx.IngestIfQuotaExceeded(w.ctx, ck.ID, rs.Added)
	if err != nil {
		return err
	}
	rs.Total = w.backendCtx.TotalKeyCount()
	send(rs)
	return nil
}

type indexIngestBaseWorker struct {
	ctx *workerpool.Context

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

func (w *indexIngestBaseWorker) HandleTask(rs IndexRecordChunk) (IndexWriteResult, error) {
	failpoint.InjectCall("mockIndexIngestWorkerFault")

	result := IndexWriteResult{
		ID: rs.ID,
	}
	if err := w.initSessCtx(); err != nil {
		return result, err
	}
	count, _, err := w.WriteChunk(&rs)
	if err != nil {
		return result, err
	}
	if count == 0 {
		logutil.Logger(w.ctx).Info("finish a index ingest task", zap.Int("id", rs.ID))
		return result, nil
	}
	if w.totalCount != nil {
		w.totalCount.Add(int64(count))
	}
	result.Added = count
	if ResultCounterForTest != nil {
		ResultCounterForTest.Add(1)
	}
	return result, nil
}

func (w *indexIngestBaseWorker) initSessCtx() error {
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			return err
		}
		w.restore = restoreSessCtx(sessCtx)
		if err := initSessCtx(sessCtx, w.reorgMeta); err != nil {
			return err
		}
		w.se = session.NewSession(sessCtx)
	}

	return nil
}

func (w *indexIngestBaseWorker) Close() error {
	// TODO(lance6716): unify the real write action for engineInfo and external
	// writer.
	var gerr error
	for i, writer := range w.writers {
		ew, ok := writer.(*external.Writer)
		if !ok {
			break
		}
		if err := ew.Close(w.ctx); err != nil {
			gerr = ingest.TryConvertToKeyExistsErr(err, w.indexes[i].Meta(), w.tbl.Meta())
		}
	}
	if w.se != nil {
		w.restore(w.se.Context)
		w.sessPool.Put(w.se.Context)
	}

	return gerr
}

// WriteChunk will write index records to lightning engine.
func (w *indexIngestBaseWorker) WriteChunk(rs *IndexRecordChunk) (count int, nextKey kv.Key, err error) {
	failpoint.Inject("mockWriteLocalError", func(_ failpoint.Value) {
		failpoint.Return(0, nil, errors.New("mock write local error"))
	})
	failpoint.InjectCall("writeLocalExec", rs.Done)

	oprStartTime := time.Now()
	vars := w.se.GetSessionVars()
	sc := vars.StmtCtx
	cnt, lastHandle, err := writeChunk(w.ctx, w.writers, w.indexes, w.copCtx, sc.TimeZone(), sc.ErrCtx(), vars.GetWriteStmtBufs(), rs.Chunk, w.tbl.Meta())
	if err != nil || cnt == 0 {
		return 0, nil, err
	}
	logSlowOperations(time.Since(oprStartTime), "writeChunk", 3000)
	nextKey = tablecodec.EncodeRecordKey(w.tbl.RecordPrefix(), lastHandle)
	return cnt, nextKey, nil
}

type indexWriteResultSink struct {
	ctx        *workerpool.Context
	backendCtx ingest.BackendCtx
	tbl        table.PhysicalTable
	indexes    []table.Index

	rowCntListener RowCountListener

	errGroup errgroup.Group
	source   operator.DataChannel[IndexWriteResult]
}

func newIndexWriteResultSink(
	ctx *workerpool.Context,
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
		case _, ok := <-s.source.Channel():
			if !ok {
				err := s.flush()
				if err != nil {
					s.ctx.OnError(err)
				}
				if s.backendCtx != nil {
					total := s.backendCtx.TotalKeyCount()
					if total > 0 {
						s.rowCntListener.SetTotal(total)
					}
				}
				return err
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
