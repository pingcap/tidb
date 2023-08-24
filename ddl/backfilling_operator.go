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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/ingest"
	sess "github.com/pingcap/tidb/ddl/internal/session"
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
	_ operator.Operator                  = (*OpSourceTableScanTask)(nil)
	_ operator.WithSink[OpTaskTableScan] = (*OpSourceTableScanTask)(nil)

	_ operator.WithSource[OpTaskTableScan] = (*tableScanOperator)(nil)
	_ operator.Operator                    = (*tableScanOperator)(nil)
	_ operator.WithSink[idxRecResult]      = (*tableScanOperator)(nil)

	_ operator.WithSource[idxRecResult] = (*indexIngestOperator)(nil)
	_ operator.Operator                 = (*indexIngestOperator)(nil)
	_ operator.WithSink[backfillResult] = (*indexIngestOperator)(nil)

	_ operator.WithSource[backfillResult] = (*backfillResultSink)(nil)
	_ operator.Operator                   = (*backfillResultSink)(nil)
)

// NewAddIndexIngestPipeline creates a pipeline for adding index in ingest mode.
func NewAddIndexIngestPipeline(
	ctx context.Context,
	store kv.Storage,
	sessPool *sess.Pool,
	engine ingest.Engine,
	sessCtx sessionctx.Context,
	tbl table.PhysicalTable,
	idxInfo *model.IndexInfo,
	startKey, endKey kv.Key,
) (*operator.AsyncPipeline, error) {
	index := tables.NewIndex(tbl.GetPhysicalID(), tbl.Meta(), idxInfo)
	copCtx, err := newCopContext(tbl.Meta(), idxInfo, sessCtx)
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
	scanOp := newTableScanOperator(ctx, sessPool, copCtx, srcChkPool, readerCnt)
	ingestOp := newIndexIngestOperator(ctx, copCtx, sessPool, tbl, index, engine, srcChkPool, writerCnt)
	sinkOp := newBackfillResultSink(ctx)

	operator.Compose[OpTaskTableScan](srcOp, scanOp)
	operator.Compose[idxRecResult](scanOp, ingestOp)
	operator.Compose[backfillResult](ingestOp, sinkOp)

	return operator.NewAsyncPipeline(
		srcOp, scanOp, ingestOp, sinkOp,
	), nil
}

type OpTaskTableScan struct {
	ID    int
	Start kv.Key
	End   kv.Key
}

func (t *OpTaskTableScan) String() string {
	return fmt.Sprintf("OpTaskTableScan: id=%d, startKey=%s, endKey=%s",
		t.ID, hex.EncodeToString(t.Start), hex.EncodeToString(t.End))
}

type idxRecResult struct {
	id    int
	chunk *chunk.Chunk
	err   error
	done  bool
}

type OpSourceTableScanTask struct {
	ctx context.Context

	errGroup errgroup.Group
	sink     operator.DataChannel[OpTaskTableScan]

	physicalTable table.PhysicalTable
	store         kv.Storage
	startKey      kv.Key
	endKey        kv.Key
}

func NewTableScanTaskSource(
	ctx context.Context,
	store kv.Storage,
	physicalTable table.PhysicalTable,
	startKey kv.Key,
	endKey kv.Key,
) *OpSourceTableScanTask {
	return &OpSourceTableScanTask{
		ctx:           ctx,
		errGroup:      errgroup.Group{},
		sink:          nil,
		physicalTable: physicalTable,
		store:         store,
		startKey:      startKey,
		endKey:        endKey,
	}
}

func (src *OpSourceTableScanTask) SetSink(sink operator.DataChannel[OpTaskTableScan]) {
	src.sink = sink
}

func (src *OpSourceTableScanTask) Open() error {
	src.errGroup.Go(func() error {
		taskIDAlloc := newTaskIDAllocator()
		defer src.sink.Finish()
		startKey := src.startKey
		endKey := src.endKey
		for {
			kvRanges, err := splitTableRanges(
				src.physicalTable,
				src.store,
				src.startKey,
				src.endKey,
				backfillTaskChanSize,
			)
			if err != nil {
				return err
			}
			if len(kvRanges) == 0 {
				break
			}

			batchTasks := getBatchTableScanTask(src.physicalTable, kvRanges, taskIDAlloc)
			for _, task := range batchTasks {
				select {
				case <-src.ctx.Done():
					return nil
				case src.sink.Channel() <- task:
				}
			}
			startKey = kvRanges[len(kvRanges)-1].EndKey
			if startKey.Cmp(endKey) >= 0 {
				break
			}
		}
		return nil
	})
	return nil
}

func getBatchTableScanTask(
	t table.PhysicalTable,
	kvRanges []kv.KeyRange,
	taskIDAlloc *taskIDAllocator,
) []OpTaskTableScan {
	batchTasks := make([]OpTaskTableScan, 0, len(kvRanges))
	prefix := t.RecordPrefix()
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

		task := OpTaskTableScan{
			ID:    taskID,
			Start: startKey,
			End:   endKey,
		}
		batchTasks = append(batchTasks, task)
	}
	return batchTasks
}

func (src *OpSourceTableScanTask) Close() error {
	return src.errGroup.Wait()
}

func (src *OpSourceTableScanTask) String() string {
	return "OpSourceTableScanTask"
}

type tableScanOperator struct {
	*operator.AsyncOperator[OpTaskTableScan, idxRecResult]
}

func newTableScanOperator(
	ctx context.Context,
	sessPool *sess.Pool,
	copCtx *copContext,
	srcChkPool chan *chunk.Chunk,
	concurrency int,
) *tableScanOperator {
	pool := workerpool.NewWorkerPool(
		"tableScanOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[OpTaskTableScan, idxRecResult] {
			return &tableScanWorker{
				ctx:        ctx,
				copCtx:     copCtx,
				sessPool:   sessPool,
				se:         nil,
				srcChkPool: srcChkPool,
			}
		})
	return &tableScanOperator{
		AsyncOperator: operator.NewAsyncOperator[OpTaskTableScan, idxRecResult](ctx, pool),
	}
}

type tableScanWorker struct {
	ctx        context.Context
	copCtx     *copContext
	sessPool   *sess.Pool
	se         *sess.Session
	srcChkPool chan *chunk.Chunk
}

func (w *tableScanWorker) HandleTask(task OpTaskTableScan) idxRecResult {
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			logutil.Logger(w.ctx).Error("copReqSender get session from pool failed", zap.Error(err))
			return idxRecResult{err: err}
		}
		w.se = sess.NewSession(sessCtx)
	}
	return w.scanRecords(task)
}

func (w *tableScanWorker) Close() {
	w.sessPool.Put(w.se.Context)
}

func (w *tableScanWorker) scanRecords(task OpTaskTableScan) idxRecResult {
	logutil.Logger(w.ctx).Info("start a cop-request task",
		zap.Int("id", task.ID), zap.String("task", task.String()))

	var idxResult idxRecResult
	err := wrapInBeginRollback(w.se, func(startTS uint64) error {
		rs, err := w.copCtx.buildTableScan(w.ctx, startTS, task.Start, task.End)
		if err != nil {
			return err
		}
		failpoint.Inject("mockCopSenderPanic", func(val failpoint.Value) {
			if val.(bool) {
				panic("mock panic")
			}
		})
		var done bool
		for !done {
			srcChk := w.getChunk()
			done, err = w.copCtx.fetchTableScanResult(w.ctx, rs, srcChk)
			if err != nil {
				w.recycleChunk(srcChk)
				terror.Call(rs.Close)
				return err
			}
			idxResult = idxRecResult{id: task.ID, chunk: srcChk, done: done}
			failpoint.Inject("mockCopSenderError", func() {
				idxResult.err = errors.New("mock cop error")
			})
		}
		terror.Call(rs.Close)
		return nil
	})
	idxResult.err = err
	return idxResult
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

type indexIngestOperator struct {
	*operator.AsyncOperator[idxRecResult, backfillResult]
}

func newIndexIngestOperator(
	ctx context.Context,
	copCtx *copContext,
	sessPool *sess.Pool,
	tbl table.PhysicalTable,
	index table.Index,
	engine ingest.Engine,
	srcChunkPool chan *chunk.Chunk,
	concurrency int,
) *indexIngestOperator {
	var writerIDAlloc atomic.Int32
	pool := workerpool.NewWorkerPool(
		"indexIngestOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[idxRecResult, backfillResult] {
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
	return &indexIngestOperator{
		AsyncOperator: operator.NewAsyncOperator[idxRecResult, backfillResult](ctx, pool),
	}
}

type indexIngestWorker struct {
	ctx context.Context

	tbl   table.PhysicalTable
	index table.Index

	copCtx   *copContext
	sessPool *sess.Pool
	se       *sess.Session

	writer       ingest.Writer
	srcChunkPool chan *chunk.Chunk
}

func (w *indexIngestWorker) HandleTask(rs idxRecResult) backfillResult {
	result := backfillResult{
		taskID: rs.id,
		err:    rs.err,
	}
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			result.err = err
			return result
		}
		w.se = sess.NewSession(sessCtx)
	}
	defer func() {
		w.srcChunkPool <- rs.chunk
	}()
	if result.err != nil {
		logutil.Logger(w.ctx).Error("encounter error when handle index chunk",
			zap.Int("id", rs.id), zap.Error(rs.err))
		return result
	}
	count, nextKey, err := w.WriteLocal(&rs)
	if err != nil {
		result.err = err
		return result
	}
	if count == 0 {
		logutil.Logger(w.ctx).Info("finish a cop-request task", zap.Int("id", rs.id))
		return result
	}
	result.addedCount = count
	result.scanCount = count
	result.nextKey = nextKey
	if ResultCounterForTest != nil && result.err == nil {
		ResultCounterForTest.Add(1)
	}
	return result
}

func (w *indexIngestWorker) Close() {
}

// WriteLocal will write index records to lightning engine.
func (w *indexIngestWorker) WriteLocal(rs *idxRecResult) (count int, nextKey kv.Key, err error) {
	oprStartTime := time.Now()
	vars := w.se.GetSessionVars()
	cnt, lastHandle, err := writeChunkToLocal(w.writer, w.index, w.copCtx, vars, rs.chunk)
	if err != nil || cnt == 0 {
		return 0, nil, err
	}
	logSlowOperations(time.Since(oprStartTime), "writeChunkToLocal", 3000)
	nextKey = tablecodec.EncodeRecordKey(w.tbl.RecordPrefix(), lastHandle)
	return cnt, nextKey, nil
}

type backfillResultSink struct {
	ctx context.Context

	errGroup errgroup.Group
	source   operator.DataChannel[backfillResult]
}

func newBackfillResultSink(
	ctx context.Context,
) *backfillResultSink {
	return &backfillResultSink{
		ctx:      ctx,
		errGroup: errgroup.Group{},
	}
}

func (s *backfillResultSink) SetSource(source operator.DataChannel[backfillResult]) {
	s.source = source
}

func (s *backfillResultSink) Open() error {
	s.errGroup.Go(func() error {
		for {
			select {
			case <-s.ctx.Done():
				return nil
			case result, ok := <-s.source.Channel():
				if !ok {
					return nil
				}
				if result.err != nil {
					return result.err
				}
			}
		}
	})
	return nil
}

func (s *backfillResultSink) Close() error {
	return s.errGroup.Wait()
}

func (s *backfillResultSink) String() string {
	return "backfillResultSink"
}
