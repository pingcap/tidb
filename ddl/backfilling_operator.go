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
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/ddl/internal/session"
	"github.com/pingcap/tidb/disttask/operator"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
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

// NewAddIndexIngestPipeline creates a pipeline for adding index in ingest mode.
// TODO(tangenta): add failpoint tests for these operators to ensure the robustness.
func NewAddIndexIngestPipeline(
	ctx context.Context,
	store kv.Storage,
	sessPool opSessPool,
	engine ingest.Engine,
	sessCtx sessionctx.Context,
	tbl table.PhysicalTable,
	idxInfo *model.IndexInfo,
	startKey, endKey kv.Key,
) (*operator.AsyncPipeline, error) {
	index := tables.NewIndex(tbl.GetPhysicalID(), tbl.Meta(), idxInfo)
	copCtx, err := NewCopContext(tbl.Meta(), idxInfo, sessCtx)
	if err != nil {
		return nil, err
	}
	poolSize := copReadChunkPoolSize()
	srcChkPool := make(chan *chunk.Chunk, poolSize)
	for i := 0; i < poolSize; i++ {
		srcChkPool <- chunk.NewChunkWithCapacity(copCtx.fieldTps, copReadBatchSize())
	}
	readerCnt, writerCnt := expectedIngestWorkerCnt()

	srcOp := NewTableScanTaskSource(ctx, store, tbl, startKey, endKey)
	scanOp := NewTableScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt)
	ingestOp := NewIndexIngestOperator(ctx, copCtx, sessPool, tbl, index, engine, srcChkPool, writerCnt)
	sinkOp := newIndexWriteResultSink(ctx)

	operator.Compose[TableScanTask](srcOp, scanOp)
	operator.Compose[IndexRecordChunk](scanOp, ingestOp)
	operator.Compose[IndexWriteResult](ingestOp, sinkOp)

	return operator.NewAsyncPipeline(
		srcOp, scanOp, ingestOp, sinkOp,
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
	ctx context.Context,
	sessPool opSessPool,
	copCtx *copContext,
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
	ctx        context.Context
	copCtx     *copContext
	sessPool   opSessPool
	se         *session.Session
	srcChkPool chan *chunk.Chunk
}

func (w *tableScanWorker) HandleTask(task TableScanTask, sender func(IndexRecordChunk)) {
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			logutil.Logger(w.ctx).Error("tableScanWorker get session from pool failed", zap.Error(err))
			sender(IndexRecordChunk{Err: err})
			return
		}
		w.se = session.NewSession(sessCtx)
	}
	w.scanRecords(task, sender)
}

func (w *tableScanWorker) Close() {
	w.sessPool.Put(w.se.Context)
}

func (w *tableScanWorker) scanRecords(task TableScanTask, sender func(IndexRecordChunk)) {
	logutil.Logger(w.ctx).Info("start a table scan task",
		zap.Int("id", task.ID), zap.String("task", task.String()))

	var idxResult IndexRecordChunk
	err := wrapInBeginRollback(w.se, func(startTS uint64) error {
		rs, err := w.copCtx.buildTableScan(w.ctx, startTS, task.Start, task.End)
		if err != nil {
			return err
		}
		var done bool
		for !done {
			srcChk := w.getChunk()
			done, err = w.copCtx.fetchTableScanResult(w.ctx, rs, srcChk)
			if err != nil {
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
		// TODO(tangenta): cancel operator instead of sending error to sink.
		idxResult.Err = err
		sender(idxResult)
	}
}

func (w *tableScanWorker) getChunk() *chunk.Chunk {
	chk := <-w.srcChkPool
	newCap := copReadBatchSize()
	if chk.Capacity() != newCap {
		chk = chunk.NewChunkWithCapacity(w.copCtx.fieldTps, newCap)
	}
	chk.Reset()
	return chk
}

func (w *tableScanWorker) recycleChunk(chk *chunk.Chunk) {
	w.srcChkPool <- chk
}

// IndexWriteResult contains the result of writing index records to ingest engine.
type IndexWriteResult struct {
	ID    int
	Added int
	Total int
	Next  kv.Key
	Err   error
}

// IndexIngestOperator writes index records to ingest engine.
type IndexIngestOperator struct {
	*operator.AsyncOperator[IndexRecordChunk, IndexWriteResult]
}

// NewIndexIngestOperator creates a new IndexIngestOperator.
func NewIndexIngestOperator(
	ctx context.Context,
	copCtx *copContext,
	sessPool opSessPool,
	tbl table.PhysicalTable,
	index table.Index,
	engine ingest.Engine,
	srcChunkPool chan *chunk.Chunk,
	concurrency int,
) *IndexIngestOperator {
	var writerIDAlloc atomic.Int32
	pool := workerpool.NewWorkerPool(
		"indexIngestOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[IndexRecordChunk, IndexWriteResult] {
			writerID := int(writerIDAlloc.Add(1))
			writer, err := engine.CreateWriter(writerID, index.Meta().Unique)
			if err != nil {
				logutil.Logger(ctx).Error("create index ingest worker failed", zap.Error(err))
				return nil
			}
			return &indexIngestWorker{
				ctx:          ctx,
				tbl:          tbl,
				index:        index,
				copCtx:       copCtx,
				se:           nil,
				sessPool:     sessPool,
				writer:       writer,
				srcChunkPool: srcChunkPool,
			}
		})
	return &IndexIngestOperator{
		AsyncOperator: operator.NewAsyncOperator[IndexRecordChunk, IndexWriteResult](ctx, pool),
	}
}

type indexIngestWorker struct {
	ctx context.Context

	tbl   table.PhysicalTable
	index table.Index

	copCtx   *copContext
	sessPool opSessPool
	se       *session.Session

	writer       ingest.Writer
	srcChunkPool chan *chunk.Chunk
}

func (w *indexIngestWorker) HandleTask(rs IndexRecordChunk, send func(IndexWriteResult)) {
	defer func() {
		if rs.Chunk != nil {
			w.srcChunkPool <- rs.Chunk
		}
	}()
	result := IndexWriteResult{
		ID:  rs.ID,
		Err: rs.Err,
	}
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			result.Err = err
			send(result)
			return
		}
		w.se = session.NewSession(sessCtx)
	}
	if result.Err != nil {
		logutil.Logger(w.ctx).Error("encounter error when handle index chunk",
			zap.Int("id", rs.ID), zap.Error(rs.Err))
		send(result)
		return
	}
	count, nextKey, err := w.WriteLocal(&rs)
	if err != nil {
		result.Err = err
		send(result)
		return
	}
	if count == 0 {
		logutil.Logger(w.ctx).Info("finish a index ingest task", zap.Int("id", rs.ID))
		send(result)
		return
	}
	result.Added = count
	result.Next = nextKey
	if ResultCounterForTest != nil && result.Err == nil {
		ResultCounterForTest.Add(1)
	}
	send(result)
}

func (*indexIngestWorker) Close() {
}

// WriteLocal will write index records to lightning engine.
func (w *indexIngestWorker) WriteLocal(rs *IndexRecordChunk) (count int, nextKey kv.Key, err error) {
	oprStartTime := time.Now()
	vars := w.se.GetSessionVars()
	cnt, lastHandle, err := writeChunkToLocal(w.writer, w.index, w.copCtx, vars, rs.Chunk)
	if err != nil || cnt == 0 {
		return 0, nil, err
	}
	logSlowOperations(time.Since(oprStartTime), "writeChunkToLocal", 3000)
	nextKey = tablecodec.EncodeRecordKey(w.tbl.RecordPrefix(), lastHandle)
	return cnt, nextKey, nil
}

type indexWriteResultSink struct {
	ctx context.Context

	errGroup errgroup.Group
	source   operator.DataChannel[IndexWriteResult]
}

func newIndexWriteResultSink(
	ctx context.Context,
) *indexWriteResultSink {
	return &indexWriteResultSink{
		ctx:      ctx,
		errGroup: errgroup.Group{},
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
	// TODO(tangenta): use results to update reorg info and metrics.
	for {
		select {
		case <-s.ctx.Done():
			return nil
		case result, ok := <-s.source.Channel():
			if !ok {
				return nil
			}
			if result.Err != nil {
				return result.Err
			}
		}
	}
}

func (s *indexWriteResultSink) Close() error {
	return s.errGroup.Wait()
}

func (*indexWriteResultSink) String() string {
	return "indexWriteResultSink"
}
