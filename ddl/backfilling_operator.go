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
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	_ operator.Operator = (*tableScanTaskSource)(nil)
	_ operator.Operator = (*tableScanOperator)(nil)
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
	ctx    context.Context
	cancel context.CancelFunc

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
	ctxWithCancel, cancel := context.WithCancel(ctx)
	return &tableScanTaskSource{
		ctx:           ctxWithCancel,
		cancel:        cancel,
		errGroup:      errgroup.Group{},
		sink:          nil,
		physicalTable: physicalTable,
		reorgInfo:     reorgInfo,
		store:         store,
		startKey:      startKey,
		endKey:        endKey,
	}
}

func (src *tableScanTaskSource) Open() error {
	src.errGroup.Go(func() error {
		taskIDAlloc := newTaskIDAllocator()
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
					src.sink.Finish()
					return nil
				case src.sink.Channel() <- task:
				}

			}
		}
		src.sink.Finish()
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

func (src *tableScanTaskSource) Close(force bool) error {
	if force {
		src.cancel()
	}
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
		"tableScanWorkerPool",
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
		AsyncOperator: operator.NewAsyncOperator[tableScanTask, idxRecResult](pool),
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

type ingestWriterOperator struct {
	operator.AsyncOperator[idxRecResult, backfillResult]

	reorgInfo   *reorgInfo
	poolErr     error
	backendCtx  ingest.BackendCtx
	writerMaxID int
	ctx         context.Context
	tbl         table.PhysicalTable
}

func (w *indexIngestWorker) Close(force bool) {

}
