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
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/ingest"
	sess "github.com/pingcap/tidb/ddl/internal/session"
	"github.com/pingcap/tidb/disttask/framework/operator"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
	"sync/atomic"
)

func newAddIndexPipeline(
	ctx context.Context,
	dc *ddlCtx,
	info *reorgInfo,
	sessPool *sess.Pool,
	tbl table.PhysicalTable, start kv.Key) (*operator.AsyncPipeline, *tableScanOperator, error) {
	job := info.Job
	bc, ok := ingest.LitBackCtxMgr.Load(job.ID)
	if !ok {
		logutil.Logger(ctx).Error(ingest.LitErrGetBackendFail, zap.Int64("job ID", job.ID))
		return nil, nil, errors.Trace(errors.New("cannot get lightning backend"))
	}
	mgr := bc.GetCheckpointManager()
	if mgr != nil {
		mgr.Reset(tbl.GetPhysicalID(), info.StartKey, info.EndKey)
	}
	readerCnt, writerCnt := expectedWorkerSize()
	// create consume op.
	consumeOp, err := newConsumerOperator(dc, info, start)
	if err != nil {
		return nil, nil, errors.Trace(errors.New("cannot create consume operator"))
	}

	// create ingest op.
	ingestOp, err := newIngestWriterOperator(ctx, info, mgr, bc, consumeOp.Source.(operator.DataSink[*backfillResult]), tbl, writerCnt)
	if err != nil {
		return nil, nil, errors.Trace(errors.New("cannot create ingest operator"))
	}

	// create scan op.
	scanOp, err := newTableScanOperator(ctx, tbl, tbl.Meta().Indices, info, sessPool, mgr, ingestOp.Source.(operator.DataSink[idxRecResult]), readerCnt)
	if err != nil {
		return nil, nil, errors.Trace(errors.New("cannot create table scan operator"))
	}

	ingestOp.tableScan = scanOp
	res := &operator.AsyncPipeline{}
	res.AddOperator(scanOp)
	res.AddOperator(ingestOp)
	res.AddOperator(consumeOp)
	return res, scanOp, nil
}

type tableScanWorker struct {
	sink    operator.DataSink[idxRecResult]
	op      *tableScanOperator
	se      *sess.Session
	sessCtx sessionctx.Context
}

// HandleTask define the basic running process for each operator.
func (tw *tableScanWorker) HandleTask(task *reorgBackfillTask) {
	logutil.BgLogger().Info("table scan worker handle task")
	err := tw.scanRecords(task)
	if err != nil {
		tw.sink.Write(idxRecResult{id: task.id, err: err})
		return
	}
}

func (tw *tableScanWorker) scanRecords(task *reorgBackfillTask) error {
	logutil.Logger(tw.op.ctx).Info("start a cop-request task",
		zap.Int("id", task.id), zap.String("task", task.String()))
	return wrapInBeginRollback(tw.se, func(startTS uint64) error {
		rs, err := tw.op.copCtx.buildTableScan(tw.op.ctx, startTS, task.startKey, task.endKey)
		if err != nil {
			return err
		}
		failpoint.Inject("mockCopSenderPanic", func(val failpoint.Value) {
			if val.(bool) {
				panic("mock panic")
			}
		})
		if tw.op.checkpointMgr != nil {
			tw.op.checkpointMgr.Register(task.id, task.endKey)
		}
		var done bool
		for !done {
			srcChk := tw.op.getChunk()
			done, err = tw.op.copCtx.fetchTableScanResult(tw.op.ctx, rs, srcChk)
			if err != nil {
				tw.op.recycleChunk(srcChk)
				terror.Call(rs.Close)
				return err
			}
			if tw.op.checkpointMgr != nil {
				tw.op.checkpointMgr.UpdateTotal(task.id, srcChk.NumRows(), done)
			}
			idxRs := idxRecResult{id: task.id, chunk: srcChk, done: done}
			failpoint.Inject("mockCopSenderError", func() {
				idxRs.err = errors.New("mock cop error")
			})
			tw.sink.Write(idxRs)
		}
		terror.Call(rs.Close)
		return nil
	})
}

// Close implement the Close interface for workerpool.
func (tw *tableScanWorker) Close() {
	tw.op.sessPool.Put(tw.sessCtx)
}

type tableScanOperator struct {
	operator.BaseOperator[*reorgBackfillTask, idxRecResult]
	pool *workerpool.WorkerPool[*reorgBackfillTask]

	checkpointMgr *ingest.CheckpointManager
	sessPool      *sess.Pool
	ctx           context.Context
	copCtx        *copContext
	store         kv.Storage
	closed        bool
	poolErr       error
	srcChkPool    chan *chunk.Chunk
	se            *sess.Session
	workerSize    int64
}

func (t *tableScanOperator) getChunk() *chunk.Chunk {
	chk := <-t.srcChkPool
	newCap := copReadBatchSize()
	if chk.Capacity() != newCap {
		chk = chunk.NewChunkWithCapacity(t.copCtx.fieldTps, newCap)
	}
	chk.Reset()
	return chk
}

// recycleChunk puts the index record slice and the chunk back to the pool for reuse.
func (t *tableScanOperator) recycleChunk(chk *chunk.Chunk) {
	if chk == nil {
		return
	}
	t.srcChkPool <- chk
}

// Open implements AsyncOperator.
func (t *tableScanOperator) Open() error {
	t.pool.SetCreateWorker(
		func() workerpool.Worker[*reorgBackfillTask] {
			logutil.BgLogger().Info("ywq test open table scan worker")
			sessCtx, err := t.sessPool.Get()
			if err != nil {
				logutil.Logger(t.ctx).Error("copReqSender get session from pool failed", zap.Error(err))
				_ = t.Sink.Write(idxRecResult{err: err})
				return nil
			}
			se := sess.NewSession(sessCtx)
			return &tableScanWorker{t.Sink, t, se, sessCtx}
		},
	)
	t.pool.Start()
	return nil
}

// Close implements AsyncOperator.
func (t *tableScanOperator) Close() {
	if t.closed {
		return
	}
	logutil.Logger(t.ctx).Info("close cop-request sender operator")
	// Wait for all cop-req senders to exit.
	t.Source.(*operator.AsyncDataChannel[*reorgBackfillTask]).Channel.ReleaseAndWait()
	t.closed = true
}

// Display implements AsyncOperator.
func (t *tableScanOperator) Display() string {
	return "tableScanOperator{ source: " + t.Source.Display() + ", sink: " + t.Sink.Display() + "}"
}

func newTableScanOperator(
	ctx context.Context,
	tbl table.PhysicalTable,
	indices []*model.IndexInfo,
	info *reorgInfo,
	sessPool *sess.Pool,
	checkpointMgr *ingest.CheckpointManager,
	sink operator.DataSink[idxRecResult],
	workerSize int) (*tableScanOperator, error) {
	indexInfo := model.FindIndexInfoByID(indices, info.currElement.ID)
	if indexInfo == nil {
		logutil.Logger(ctx).Warn("cannot init cop request sender",
			zap.Int64("table ID", tbl.Meta().ID), zap.Int64("index ID", info.currElement.ID))
		return nil, errors.New("cannot find index info")
	}
	sessCtx, err := newSessCtx(info)
	if err != nil {
		logutil.Logger(ctx).Warn("cannot init cop request sender", zap.Error(err))
		return nil, err
	}
	copCtx, err := newCopContext(tbl.Meta(), indexInfo, sessCtx)
	if err != nil {
		logutil.Logger(ctx).Warn("cannot init cop request sender", zap.Error(err))
		return nil, err
	}
	res := &tableScanOperator{
		ctx:           ctx,
		copCtx:        copCtx,
		store:         sessCtx.GetStore(),
		sessPool:      sessPool,
		checkpointMgr: checkpointMgr,
		closed:        false,
	}
	// init src chk pool.
	poolSize := copReadChunkPoolSize()
	srcChkPool := make(chan *chunk.Chunk, poolSize)
	for i := 0; i < poolSize; i++ {
		srcChkPool <- chunk.NewChunkWithCapacity(copCtx.fieldTps, copReadBatchSize())
	}
	res.srcChkPool = srcChkPool

	// create worker pool.
	pool, _ := workerpool.NewWorkerPoolWithoutCreateWorker[*reorgBackfillTask]("tableScan", poolutil.DDL, workerSize)
	source := &operator.AsyncDataChannel[*reorgBackfillTask]{Channel: pool}

	// set operator source and sink.
	res.pool = pool
	res.Source = source
	res.Sink = sink
	return res, nil
}

type ingestWriterOperator struct {
	operator.BaseOperator[idxRecResult, *backfillResult] // source: idxRecResult, sink: backfillResult.
	reorgInfo                                            *reorgInfo
	poolErr                                              error
	backendCtx                                           ingest.BackendCtx
	writerMaxID                                          int
	ctx                                                  context.Context
	tbl                                                  table.PhysicalTable
	checkpointMgr                                        *ingest.CheckpointManager
	tableScan                                            *tableScanOperator
	pool                                                 *workerpool.WorkerPool[idxRecResult]
}

// Open implements AsyncOperator.
func (w *ingestWriterOperator) Open() error {
	w.pool.SetCreateWorker(
		func() workerpool.Worker[idxRecResult] {
			logutil.BgLogger().Info("ywq test open ingest writer worker")

			reorgInfo := w.reorgInfo
			job := reorgInfo.Job
			sessCtx, err := newSessCtx(reorgInfo)
			if err != nil {
				w.poolErr = err
				return nil
			}
			bcCtx := w.backendCtx
			ei, err := bcCtx.Register(job.ID, w.reorgInfo.currElement.ID, job.SchemaName, job.TableName)
			if err != nil {
				// Return an error only if it is the first worker.
				if w.writerMaxID == 0 {
					w.poolErr = err
					return nil
				}
				logutil.Logger(w.ctx).Warn("cannot create new writer", zap.Error(err),
					zap.Int64("job ID", reorgInfo.ID), zap.Int64("index ID", w.reorgInfo.currElement.ID))
				return nil
			}
			worker, err := newAddIndexIngestWorker(w.ctx, w.tbl, reorgInfo.d, ei, nil, job.ID,
				reorgInfo.SchemaName, w.reorgInfo.currElement.ID, w.writerMaxID,
				nil, sessCtx, w.checkpointMgr, true)
			worker.tableScan = w.tableScan
			worker.sink = w.Sink
			if err != nil {
				// Return an error only if it is the first worker.
				if w.writerMaxID == 0 {
					w.poolErr = err
					return nil
				}
				logutil.Logger(w.ctx).Warn("cannot create new writer", zap.Error(err),
					zap.Int64("job ID", reorgInfo.ID), zap.Int64("index ID", w.reorgInfo.currElement.ID))
				return nil
			}
			w.writerMaxID++
			return worker
		},
	)
	w.pool.Start()
	return w.poolErr
}

// Close implements AsyncOperator.
func (w *ingestWriterOperator) Close() {
	if w.pool != nil {
		w.pool.ReleaseAndWait()
	}
	if w.checkpointMgr != nil {
		w.checkpointMgr.Sync()
		// Get the latest status after all workers are closed so that the result is more accurate.
		cnt, nextKey := w.checkpointMgr.Status()
		_ = w.Sink.Write(&backfillResult{
			totalCount: cnt,
			nextKey:    nextKey,
		})
	}

	jobID := w.reorgInfo.ID
	indexID := w.reorgInfo.currElement.ID
	w.backendCtx.ResetWorkers(jobID, indexID)
}

// Display implements AsyncOperator.
func (w *ingestWriterOperator) Display() string {
	return "ingestWriterOperator{ source: " + w.Source.Display() + ", sink: " + w.Sink.Display() + "}"
}

func newIngestWriterOperator(
	ctx context.Context,
	info *reorgInfo,
	checkpointMgr *ingest.CheckpointManager,
	backendCtx ingest.BackendCtx,
	sink operator.DataSink[*backfillResult],
	tbl table.PhysicalTable,
	writerCnt int) (*ingestWriterOperator, error) {
	res := &ingestWriterOperator{
		ctx:           ctx,
		reorgInfo:     info,
		backendCtx:    backendCtx,
		writerMaxID:   0,
		tbl:           tbl,
		checkpointMgr: checkpointMgr,
	}
	// create worker pool.
	skipReg := workerpool.OptionSkipRegister[idxRecResult]{}
	pool, err := workerpool.NewWorkerPoolWithoutCreateWorker[idxRecResult]("ingest_writer", poolutil.DDL, writerCnt, skipReg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	source := &operator.AsyncDataChannel[idxRecResult]{Channel: pool}
	res.Source = source
	res.pool = pool
	res.Sink = sink
	return res, nil
}

type consumerOperator struct {
	operator.BaseOperator[*backfillResult, int64] // source: backfillResult, sink: totalRowCnt.
	dc                                            *ddlCtx
	err                                           error
	hasError                                      *atomic.Bool
	reorgInfo                                     *reorgInfo // reorgInfo is used to update the reorg handle.
	keeper                                        *doneTaskKeeper
	pool                                          *workerpool.WorkerPool[*backfillResult]
	start                                         kv.Key
}

// Open implements AsyncOperator.
func (c *consumerOperator) Open() error {
	c.pool.SetCreateWorker(func() workerpool.Worker[*backfillResult] {
		logutil.BgLogger().Info("ywq test open consume worker")
		return &consumeWorker{consumerOperator: c, sink: c.Sink, keeper: c.keeper}
	})
	c.pool.Start()
	return nil
}

// Close implements AsyncOperator.
func (c *consumerOperator) Close() {
	if c.pool != nil {
		c.pool.ReleaseAndWait()
	}
	if c.hasError.Load() {
		// ywq todo add error
		logutil.BgLogger().Warn("backfill worker handle tasks failed", zap.String("category", "ddl"),
			zap.String("start key", hex.EncodeToString(c.start)))
	}
}

// Display implements AsyncOperator.
func (c *consumerOperator) Display() string {
	return "consumerOperator{ source: " + c.Source.Display() + ", sink: " + c.Sink.Display() + "}"
}

func newConsumerOperator(
	dc *ddlCtx, reorgInfo *reorgInfo, start kv.Key) (*consumerOperator, error) {
	res := &consumerOperator{
		dc:        dc,
		hasError:  &atomic.Bool{},
		reorgInfo: reorgInfo,
		start:     start,
	}
	keeper := newDoneTaskKeeper(start)
	// create worker pool.
	skipReg := workerpool.OptionSkipRegister[*backfillResult]{}
	pool, err := workerpool.NewWorkerPoolWithoutCreateWorker[*backfillResult]("ingest_writer", poolutil.DDL, 1, skipReg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	source := &operator.AsyncDataChannel[*backfillResult]{Channel: pool}
	res.Sink = &totalAddedCountSink{atomic.Int64{}}
	res.Source = source
	res.pool = pool
	res.keeper = keeper
	return res, nil
}

type consumeWorker struct {
	firstErr         error
	consumerOperator *consumerOperator
	sink             operator.DataSink[int64]
	keeper           *doneTaskKeeper
}

// HandleTask define the basic running process for each operator.
func (cw *consumeWorker) HandleTask(result *backfillResult) {
	logutil.BgLogger().Info("consume worker handle task")
	err := cw.handleOneResult(result)
	if err != nil && cw.firstErr == nil {
		cw.consumerOperator.hasError.Store(true)
		cw.firstErr = err
	}
}

func (cw *consumeWorker) handleOneResult(result *backfillResult) error {
	reorgInfo := cw.consumerOperator.reorgInfo
	if result.err != nil {
		logutil.BgLogger().Warn("backfill worker failed", zap.String("category", "ddl"),
			zap.Int64("job ID", reorgInfo.ID),
			zap.String("result next key", hex.EncodeToString(result.nextKey)),
			zap.Error(result.err))

		// Todo
		//scheduler.drainTasks() // Make it quit early.
		return result.err
	}
	if result.totalCount > 0 {
		_ = cw.sink.Write(int64(result.totalCount))
	} else {
		_ = cw.sink.Write(int64(result.addedCount))
	}
	cw.keeper.updateNextKey(result.taskID, result.nextKey)
	// TODO adjust worker size.
	return nil
}

// Close implement the Close interface for workerpool.
func (*consumeWorker) Close() {}

type totalAddedCountSink struct {
	cnt atomic.Int64
}

func (s *totalAddedCountSink) Write(cnt int64) error {
	s.cnt.Add(cnt)
	return nil
}

func (s *totalAddedCountSink) Display() string {
	return "totalAddedCountSink"
}

func expectedWorkerSize() (readerSize int, writerSize int) {
	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	readerSize = mathutil.Min(workerCnt/2, maxBackfillWorkerSize)
	readerSize = mathutil.Max(readerSize, 1)
	writerSize = mathutil.Min(workerCnt/2+2, maxBackfillWorkerSize)
	return readerSize, writerSize
}
