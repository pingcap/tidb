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
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/internal/session"
	util2 "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/disttask/operator"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/prometheus/client_golang/prometheus"
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

// NewOperatorCtx creates a new OperatorCtx.
func NewOperatorCtx(ctx context.Context) *OperatorCtx {
	opCtx, cancel := context.WithCancel(ctx)
	return &OperatorCtx{
		Context: opCtx,
		cancel:  cancel,
	}
}

func (ctx *OperatorCtx) onError(err error) {
	tracedErr := errors.Trace(err)
	ctx.cancel()
	ctx.err.CompareAndSwap(nil, &tracedErr)
}

// Cancel cancels the pipeline.
func (ctx *OperatorCtx) Cancel() {
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

func getWriterMemSize(idxNum int) (uint64, error) {
	failpoint.Inject("mockWriterMemSize", func() {
		failpoint.Return(1*size.GB, nil)
	})
	_, writerCnt := expectedIngestWorkerCnt()
	memTotal, err := memory.MemTotal()
	if err != nil {
		return 0, err
	}
	memUsed, err := memory.MemUsed()
	if err != nil {
		return 0, err
	}
	memAvailable := memTotal - memUsed
	memSize := (memAvailable / 2) / uint64(writerCnt) / uint64(idxNum)
	logutil.BgLogger().Info("build operators that write index to cloud storage", zap.Uint64("memory total", memTotal), zap.Uint64("memory used", memUsed), zap.Uint64("memory size", memSize))
	return memSize, nil
}

func getMergeSortPartSize(concurrency int, idxNum int) (uint64, error) {
	writerMemSize, err := getWriterMemSize(idxNum)
	if err != nil {
		return 0, nil
	}
	return writerMemSize / uint64(concurrency) / 10, nil
}

// NewAddIndexIngestPipeline creates a pipeline for adding index in ingest mode.
func NewAddIndexIngestPipeline(
	ctx *OperatorCtx,
	store kv.Storage,
	sessPool opSessPool,
	backendCtx ingest.BackendCtx,
	engines []ingest.Engine,
	sessCtx sessionctx.Context,
	tbl table.PhysicalTable,
	idxInfos []*model.IndexInfo,
	startKey, endKey kv.Key,
	totalRowCount *atomic.Int64,
	metricCounter prometheus.Counter,
	reorgMeta *model.DDLReorgMeta,
) (*operator.AsyncPipeline, error) {
	indexes := make([]table.Index, 0, len(idxInfos))
	for _, idxInfo := range idxInfos {
		index := tables.NewIndex(tbl.GetPhysicalID(), tbl.Meta(), idxInfo)
		indexes = append(indexes, index)
	}
	reqSrc := getDDLRequestSource(model.ActionAddIndex)
	copCtx, err := copr.NewCopContext(tbl.Meta(), idxInfos, sessCtx, reqSrc)
	if err != nil {
		return nil, err
	}
	poolSize := copReadChunkPoolSize()
	srcChkPool := make(chan *chunk.Chunk, poolSize)
	for i := 0; i < poolSize; i++ {
		srcChkPool <- chunk.NewChunkWithCapacity(copCtx.GetBase().FieldTypes, copReadBatchSize())
	}
	readerCnt, writerCnt := expectedIngestWorkerCnt()

	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey)
	scanOp := NewTableScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt)
	ingestOp := NewIndexIngestOperator(ctx, copCtx, sessPool, tbl, indexes, engines, srcChkPool, writerCnt, reorgMeta)
	sinkOp := newIndexWriteResultSink(ctx, backendCtx, tbl, indexes, totalRowCount, metricCounter)

	operator.Compose[TableScanTask](srcOp, scanOp)
	operator.Compose[IndexRecordChunk](scanOp, ingestOp)
	operator.Compose[IndexWriteResult](ingestOp, sinkOp)

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
	sessCtx sessionctx.Context,
	jobID, subtaskID int64,
	tbl table.PhysicalTable,
	idxInfos []*model.IndexInfo,
	startKey, endKey kv.Key,
	totalRowCount *atomic.Int64,
	metricCounter prometheus.Counter,
	onClose external.OnCloseFunc,
	reorgMeta *model.DDLReorgMeta,
) (*operator.AsyncPipeline, error) {
	indexes := make([]table.Index, 0, len(idxInfos))
	for _, idxInfo := range idxInfos {
		index := tables.NewIndex(tbl.GetPhysicalID(), tbl.Meta(), idxInfo)
		indexes = append(indexes, index)
	}
	reqSrc := getDDLRequestSource(model.ActionAddIndex)
	copCtx, err := copr.NewCopContext(tbl.Meta(), idxInfos, sessCtx, reqSrc)
	if err != nil {
		return nil, err
	}
	poolSize := copReadChunkPoolSize()
	srcChkPool := make(chan *chunk.Chunk, poolSize)
	for i := 0; i < poolSize; i++ {
		srcChkPool <- chunk.NewChunkWithCapacity(copCtx.GetBase().FieldTypes, copReadBatchSize())
	}
	readerCnt, writerCnt := expectedIngestWorkerCnt()

	backend, err := storage.ParseBackend(extStoreURI, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := storage.NewWithDefaultOpt(ctx, backend)
	if err != nil {
		return nil, err
	}

	memSize, err := getWriterMemSize(len(indexes))
	if err != nil {
		return nil, err
	}

	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey)
	scanOp := NewTableScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt)
	writeOp := NewWriteExternalStoreOperator(
		ctx, copCtx, sessPool, jobID, subtaskID, tbl, indexes, extStore, srcChkPool, writerCnt, onClose, memSize, reorgMeta)
	sinkOp := newIndexWriteResultSink(ctx, nil, tbl, indexes, totalRowCount, metricCounter)

	operator.Compose[TableScanTask](srcOp, scanOp)
	operator.Compose[IndexRecordChunk](scanOp, writeOp)
	operator.Compose[IndexWriteResult](writeOp, sinkOp)

	return operator.NewAsyncPipeline(
		srcOp, scanOp, writeOp, sinkOp,
	), nil
}

// TableScanTask contains the start key and the end key of a region.
type TableScanTask struct {
	ID    int
	Start kv.Key
	End   kv.Key
}

// String implement fmt.Stringer interface.
func (t *TableScanTask) String() string {
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

// TableScanTaskSource produces TableScanTask by splitting table records into ranges.
type TableScanTaskSource struct {
	ctx context.Context

	errGroup errgroup.Group
	sink     operator.DataChannel[TableScanTask]

	tbl      table.PhysicalTable
	store    kv.Storage
	startKey kv.Key
	endKey   kv.Key
}

// NewTableScanTaskSource creates a new TableScanTaskSource.
func NewTableScanTaskSource(
	ctx context.Context,
	store kv.Storage,
	physicalTable table.PhysicalTable,
	startKey kv.Key,
	endKey kv.Key,
) *TableScanTaskSource {
	return &TableScanTaskSource{
		ctx:      ctx,
		errGroup: errgroup.Group{},
		tbl:      physicalTable,
		store:    store,
		startKey: startKey,
		endKey:   endKey,
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

func (src *TableScanTaskSource) generateTasks() error {
	taskIDAlloc := newTaskIDAllocator()
	defer src.sink.Finish()
	startKey := src.startKey
	endKey := src.endKey
	for {
		kvRanges, err := splitTableRanges(
			src.tbl,
			src.store,
			startKey,
			endKey,
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
		if startKey.Cmp(endKey) >= 0 {
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
}

// NewTableScanOperator creates a new TableScanOperator.
func NewTableScanOperator(
	ctx *OperatorCtx,
	sessPool opSessPool,
	copCtx copr.CopContext,
	srcChkPool chan *chunk.Chunk,
	concurrency int,
) *TableScanOperator {
	pool := workerpool.NewWorkerPool(
		"TableScanOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[TableScanTask, IndexRecordChunk] {
			return &tableScanWorker{
				ctx:        ctx,
				copCtx:     copCtx,
				sessPool:   sessPool,
				se:         nil,
				srcChkPool: srcChkPool,
			}
		})
	return &TableScanOperator{
		AsyncOperator: operator.NewAsyncOperator[TableScanTask, IndexRecordChunk](ctx, pool),
	}
}

type tableScanWorker struct {
	ctx        *OperatorCtx
	copCtx     copr.CopContext
	sessPool   opSessPool
	se         *session.Session
	srcChkPool chan *chunk.Chunk
}

func (w *tableScanWorker) HandleTask(task TableScanTask, sender func(IndexRecordChunk)) {
	defer tidbutil.Recover(metrics.LblAddIndex, "handleTableScanTaskWithRecover", func() {
		w.ctx.onError(errors.New("met panic in tableScanWorker"))
	}, false)

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

// OperatorCallBackForTest is used for test to mock scan record error.
var OperatorCallBackForTest func()

func (w *tableScanWorker) scanRecords(task TableScanTask, sender func(IndexRecordChunk)) {
	logutil.Logger(w.ctx).Info("start a table scan task",
		zap.Int("id", task.ID), zap.String("task", task.String()))

	var idxResult IndexRecordChunk
	err := wrapInBeginRollback(w.se, func(startTS uint64) error {
		failpoint.Inject("mockScanRecordError", func(_ failpoint.Value) {
			failpoint.Return(errors.New("mock scan record error"))
		})
		failpoint.Inject("scanRecordExec", func(_ failpoint.Value) {
			OperatorCallBackForTest()
		})
		rs, err := buildTableScan(w.ctx, w.copCtx.GetBase(), startTS, task.Start, task.End)
		if err != nil {
			return err
		}
		var done bool
		for !done {
			srcChk := w.getChunk()
			done, err = fetchTableScanResult(w.ctx, w.copCtx.GetBase(), rs, srcChk)
			if err != nil || util2.IsContextDone(w.ctx) {
				w.recycleChunk(srcChk)
				terror.Call(rs.Close)
				return err
			}
			idxResult = IndexRecordChunk{ID: task.ID, Chunk: srcChk, Done: done}
			sender(idxResult)
		}
		return rs.Close()
	})
	if err != nil {
		w.ctx.onError(err)
	}
}

func (w *tableScanWorker) getChunk() *chunk.Chunk {
	chk := <-w.srcChkPool
	newCap := copReadBatchSize()
	if chk.Capacity() != newCap {
		chk = chunk.NewChunkWithCapacity(w.copCtx.GetBase().FieldTypes, newCap)
	}
	chk.Reset()
	return chk
}

func (w *tableScanWorker) recycleChunk(chk *chunk.Chunk) {
	w.srcChkPool <- chk
}

// WriteExternalStoreOperator writes index records to external storage.
type WriteExternalStoreOperator struct {
	*operator.AsyncOperator[IndexRecordChunk, IndexWriteResult]
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
	srcChunkPool chan *chunk.Chunk,
	concurrency int,
	onClose external.OnCloseFunc,
	memoryQuota uint64,
	reorgMeta *model.DDLReorgMeta,
) *WriteExternalStoreOperator {
	pool := workerpool.NewWorkerPool(
		"WriteExternalStoreOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[IndexRecordChunk, IndexWriteResult] {
			writers := make([]ingest.Writer, 0, len(indexes))
			for _, index := range indexes {
				builder := external.NewWriterBuilder().
					SetOnCloseFunc(onClose).
					SetKeyDuplicationEncoding(index.Meta().Unique).
					SetMemorySizeLimit(memoryQuota)
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
			}
		})
	return &WriteExternalStoreOperator{
		AsyncOperator: operator.NewAsyncOperator[IndexRecordChunk, IndexWriteResult](ctx, pool),
	}
}

// IndexWriteResult contains the result of writing index records to ingest engine.
type IndexWriteResult struct {
	ID    int
	Added int
	Total int
	Next  kv.Key
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
	srcChunkPool chan *chunk.Chunk,
	concurrency int,
	reorgMeta *model.DDLReorgMeta,
) *IndexIngestOperator {
	var writerIDAlloc atomic.Int32
	pool := workerpool.NewWorkerPool(
		"indexIngestOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[IndexRecordChunk, IndexWriteResult] {
			writers := make([]ingest.Writer, 0, len(indexes))
			for i := range indexes {
				writerID := int(writerIDAlloc.Add(1))
				writer, err := engines[i].CreateWriter(writerID)
				if err != nil {
					logutil.Logger(ctx).Error("create index ingest worker failed", zap.Error(err))
					return nil
				}
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
	srcChunkPool chan *chunk.Chunk
}

func (w *indexIngestWorker) HandleTask(rs IndexRecordChunk, send func(IndexWriteResult)) {
	defer func() {
		if rs.Chunk != nil {
			w.srcChunkPool <- rs.Chunk
		}
	}()
	defer tidbutil.Recover(metrics.LblAddIndex, "handleIndexIngtestTaskWithRecover", func() {
		w.ctx.onError(errors.New("met panic in indexIngestWorker"))
	}, false)

	failpoint.Inject("injectPanicForIndexIngest", func() {
		panic("mock panic")
	})

	result := IndexWriteResult{
		ID: rs.ID,
	}
	w.initSessCtx()
	count, nextKey, err := w.WriteLocal(&rs)
	if err != nil {
		w.ctx.onError(err)
		return
	}
	if count == 0 {
		logutil.Logger(w.ctx).Info("finish a index ingest task", zap.Int("id", rs.ID))
		send(result)
		return
	}
	result.Added = count
	result.Next = nextKey
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
		if err := initSessCtx(sessCtx,
			w.reorgMeta.SQLMode,
			w.reorgMeta.Location,
			w.reorgMeta.ResourceGroupName); err != nil {
			w.ctx.onError(err)
			return
		}
		w.se = session.NewSession(sessCtx)
	}
}

func (w *indexIngestWorker) Close() {
	for _, writer := range w.writers {
		err := writer.Close(w.ctx)
		if err != nil {
			w.ctx.onError(err)
		}
	}
	if w.se != nil {
		w.restore(w.se.Context)
		w.sessPool.Put(w.se.Context)
	}
}

// WriteLocal will write index records to lightning engine.
func (w *indexIngestWorker) WriteLocal(rs *IndexRecordChunk) (count int, nextKey kv.Key, err error) {
	failpoint.Inject("mockWriteLocalError", func(_ failpoint.Value) {
		failpoint.Return(0, nil, errors.New("mock write local error"))
	})
	failpoint.Inject("writeLocalExec", func(_ failpoint.Value) {
		OperatorCallBackForTest()
	})

	oprStartTime := time.Now()
	vars := w.se.GetSessionVars()
	cnt, lastHandle, err := writeChunkToLocal(w.ctx, w.writers, w.indexes, w.copCtx, vars, rs.Chunk)
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

	rowCount      *atomic.Int64
	metricCounter prometheus.Counter

	errGroup errgroup.Group
	source   operator.DataChannel[IndexWriteResult]
}

func newIndexWriteResultSink(
	ctx *OperatorCtx,
	backendCtx ingest.BackendCtx,
	tbl table.PhysicalTable,
	indexes []table.Index,
	rowCount *atomic.Int64,
	metricCounter prometheus.Counter,
) *indexWriteResultSink {
	return &indexWriteResultSink{
		ctx:           ctx,
		backendCtx:    backendCtx,
		tbl:           tbl,
		indexes:       indexes,
		rowCount:      rowCount,
		metricCounter: metricCounter,
		errGroup:      errgroup.Group{},
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
		case result, ok := <-s.source.Channel():
			if !ok {
				err := s.flush()
				if err != nil {
					s.ctx.onError(err)
				}
				return err
			}
			s.rowCount.Add(int64(result.Added))
			if s.metricCounter != nil {
				s.metricCounter.Add(float64(result.Added))
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
	for _, index := range s.indexes {
		idxInfo := index.Meta()
		_, _, err := s.backendCtx.Flush(idxInfo.ID, ingest.FlushModeForceGlobal)
		if err != nil {
			if common.ErrFoundDuplicateKeys.Equal(err) {
				err = convertToKeyExistsErr(err, idxInfo, s.tbl.Meta())
				return err
			}
			logutil.BgLogger().Error("flush error",
				zap.String("category", "ddl"), zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *indexWriteResultSink) Close() error {
	return s.errGroup.Wait()
}

func (*indexWriteResultSink) String() string {
	return "indexWriteResultSink"
}
