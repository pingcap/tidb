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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/ingest"
	sess "github.com/pingcap/tidb/ddl/internal/session"
	"github.com/pingcap/tidb/disttask/operator"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	_ operator.Operator                = (*tableScanTaskSource)(nil)
	_ operator.WithSink[tableScanTask] = (*tableScanTaskSource)(nil)

	_ operator.WithSource[tableScanTask] = (*tableScanOperator)(nil)
	_ operator.Operator                  = (*tableScanOperator)(nil)
	_ operator.WithSink[idxRecResult]    = (*tableScanOperator)(nil)

	_ operator.WithSource[idxRecResult] = (*indexIngestOperator)(nil)
	_ operator.Operator                 = (*indexIngestOperator)(nil)
	_ operator.WithSink[backfillResult] = (*indexIngestOperator)(nil)

	_ operator.WithSource[backfillResult] = (*backfillResultSink)(nil)
	_ operator.Operator                   = (*backfillResultSink)(nil)
)

type tableScanTask struct {
	id       int
	startKey kv.Key
	endKey   kv.Key
}

func (t *tableScanTask) String() string {
	return fmt.Sprintf("tableScanTask: id=%d, startKey=%s, endKey=%s",
		t.id, hex.EncodeToString(t.startKey), hex.EncodeToString(t.endKey))
}

type idxRecResult struct {
	id    int
	chunk *chunk.Chunk
	err   error
	done  bool
}

type tableScanTaskSource struct {
	ctx context.Context

	errGroup errgroup.Group
	sink     operator.DataChannel[tableScanTask]

	physicalTable table.PhysicalTable
	reorgInfo     *reorgInfo
	store         kv.Storage
	startKey      kv.Key
	endKey        kv.Key
}

func newTableScanTaskSource(
	ctx context.Context,
	physicalTable table.PhysicalTable,
	reorgInfo *reorgInfo,
	store kv.Storage,
	startKey kv.Key,
	endKey kv.Key,
) *tableScanTaskSource {
	return &tableScanTaskSource{
		ctx:           ctx,
		errGroup:      errgroup.Group{},
		sink:          nil,
		physicalTable: physicalTable,
		reorgInfo:     reorgInfo,
		store:         store,
		startKey:      startKey,
		endKey:        endKey,
	}
}

func (src *tableScanTaskSource) SetSink(sink operator.DataChannel[tableScanTask]) {
	src.sink = sink
}

func (src *tableScanTaskSource) Open() error {
	src.errGroup.Go(func() error {
		taskIDAlloc := newTaskIDAllocator()
		defer src.sink.Finish()
		for {
			kvRanges, err := splitTableRanges(src.physicalTable, src.store, src.startKey, src.endKey, backfillTaskChanSize)
			if err != nil {
				return err
			}
			if len(kvRanges) == 0 {
				break
			}

			batchTasks := getBatchTableScanTask(src.physicalTable, src.reorgInfo, kvRanges, taskIDAlloc)
			for _, task := range batchTasks {
				select {
				case <-src.ctx.Done():
					return nil
				case src.sink.Channel() <- task:
				}
			}
		}
		return nil
	})
	return nil
}

func getBatchTableScanTask(
	t table.PhysicalTable,
	reorgInfo *reorgInfo,
	kvRanges []kv.KeyRange,
	taskIDAlloc *taskIDAllocator,
) []tableScanTask {
	batchTasks := make([]tableScanTask, 0, len(kvRanges))
	prefix := t.RecordPrefix()
	// Build reorg tasks.
	job := reorgInfo.Job
	jobCtx := reorgInfo.d.jobContext(job.ID)
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
		endK, err := GetRangeEndKey(jobCtx, reorgInfo.d.store, job.Priority, prefix, startKey, endKey)
		if err != nil {
			logutil.BgLogger().Info("get backfill range task, get reverse key failed", zap.String("category", "ddl"), zap.Error(err))
		} else {
			logutil.BgLogger().Info("get backfill range task, change end key", zap.String("category", "ddl"),
				zap.Int("id", taskID), zap.Int64("pTbl", t.GetPhysicalID()),
				zap.String("end key", hex.EncodeToString(endKey)), zap.String("current end key", hex.EncodeToString(endK)))
			endKey = endK
		}

		task := tableScanTask{
			id:       taskID,
			startKey: startKey,
			endKey:   endKey,
		}
		batchTasks = append(batchTasks, task)
	}
	return batchTasks
}

func (src *tableScanTaskSource) Close() error {
	return src.errGroup.Wait()
}

func (src *tableScanTaskSource) String() string {
	return "tableScanTaskSource"
}

type tableScanOperator struct {
	*operator.AsyncOperator[tableScanTask, idxRecResult]
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
		func() workerpool.Worker[tableScanTask, idxRecResult] {
			return &tableScanWorker{
				ctx:        ctx,
				copCtx:     copCtx,
				sessPool:   sessPool,
				se:         nil,
				srcChkPool: srcChkPool,
			}
		})
	return &tableScanOperator{
		AsyncOperator: operator.NewAsyncOperator[tableScanTask, idxRecResult](ctx, pool),
	}
}

type tableScanWorker struct {
	ctx        context.Context
	copCtx     *copContext
	sessPool   *sess.Pool
	se         *sess.Session
	srcChkPool chan *chunk.Chunk
}

func (w *tableScanWorker) HandleTask(task tableScanTask) idxRecResult {
	if w.se == nil {
		sessCtx, err := w.sessPool.Get()
		if err != nil {
			logutil.Logger(w.ctx).Error("copReqSender get session from pool failed", zap.Error(err))
			return idxRecResult{err: err}
		}
		w.se = sess.NewSession(sessCtx)
	}
	return w.transform(task)
}

func (w *tableScanWorker) Close() {
	w.sessPool.Put(w.se.Context)
}

func (w *tableScanWorker) transform(task tableScanTask) idxRecResult {
	logutil.Logger(w.ctx).Info("start a cop-request task",
		zap.Int("id", task.id), zap.String("task", task.String()))

	var idxResult idxRecResult
	err := wrapInBeginRollback(w.se, func(startTS uint64) error {
		rs, err := w.copCtx.buildTableScan(w.ctx, startTS, task.startKey, task.endKey)
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
			idxResult = idxRecResult{id: task.id, chunk: srcChk, done: done}
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
	concurrency int,
	tbl table.PhysicalTable,
	index table.Index,
	copCtx *copContext,
	sessPool *sess.Pool,
	writer ingest.Writer,
	srcChunkPool chan *chunk.Chunk,
	metricCounter prometheus.Counter,
) *indexIngestOperator {
	pool := workerpool.NewWorkerPool(
		"indexIngestOperator",
		util.DDL,
		concurrency,
		func() workerpool.Worker[idxRecResult, backfillResult] {
			return &indexIngestWorker{
				ctx:           ctx,
				tbl:           tbl,
				index:         index,
				copCtx:        copCtx,
				se:            nil,
				sessPool:      sessPool,
				writer:        writer,
				srcChunkPool:  srcChunkPool,
				metricCounter: metricCounter,
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

	metricCounter prometheus.Counter
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
	w.metricCounter.Add(float64(cnt))
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
