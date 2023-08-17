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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
	"sync/atomic"
)

func NewAddIndexPipeline(
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
	// consume op.
	consumeOp, err := newConsumerOperator(dc, info, sessPool, start)
	if err != nil {
		return nil, nil, errors.Trace(errors.New("cannot create consume operator"))
	}

	// ingest op.
	ingestOp, err := newIngestWriterOperator(ctx, info, mgr, bc, consumeOp.Source.(operator.DataSink[*backfillResult]), tbl, writerCnt)
	if err != nil {
		return nil, nil, errors.Trace(errors.New("cannot create ingest operator"))
	}

	// scan op.
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

type tableScanOperator struct {
	operator.BaseOperator[*reorgBackfillTask, idxRecResult]
	checkpointMgr *ingest.CheckpointManager
	sessPool      *sess.Pool
	ctx           context.Context
	copCtx        *copContext
	store         kv.Storage
	closed        bool
	srcChkPool    chan *chunk.Chunk
	se            *sess.Session
	workerSize    int64
}

func (oi *tableScanOperator) getChunk() *chunk.Chunk {
	chk := <-oi.srcChkPool
	newCap := copReadBatchSize()
	if chk.Capacity() != newCap {
		chk = chunk.NewChunkWithCapacity(oi.copCtx.fieldTps, newCap)
	}
	chk.Reset()
	return chk
}

// recycleChunk puts the index record slice and the chunk back to the pool for reuse.
func (oi *tableScanOperator) recycleChunk(chk *chunk.Chunk) {
	if chk == nil {
		return
	}
	oi.srcChkPool <- chk
}

type readCopWorker struct {
	sink operator.DataSink[idxRecResult]
	op   *tableScanOperator
	se   *sess.Session
}

// HandleTask define the basic running process for each operator.
func (rw *readCopWorker) HandleTask(task *reorgBackfillTask) {
	logutil.BgLogger().Info("read cop worker handle task")
	err := rw.scanRecords(task)
	if err != nil {
		rw.sink.Write(idxRecResult{id: task.id, err: err})
		return
	}
}

func (rw *readCopWorker) scanRecords(task *reorgBackfillTask) error {
	logutil.Logger(rw.op.ctx).Info("start a cop-request task",
		zap.Int("id", task.id), zap.String("task", task.String()))
	return wrapInBeginRollback(rw.se, func(startTS uint64) error {
		rs, err := rw.op.copCtx.buildTableScan(rw.op.ctx, startTS, task.startKey, task.endKey)
		if err != nil {
			return err
		}
		failpoint.Inject("mockCopSenderPanic", func(val failpoint.Value) {
			if val.(bool) {
				panic("mock panic")
			}
		})
		if rw.op.checkpointMgr != nil {
			rw.op.checkpointMgr.Register(task.id, task.endKey)
		}
		var done bool
		for !done {
			srcChk := rw.op.getChunk()
			done, err = rw.op.copCtx.fetchTableScanResult(rw.op.ctx, rs, srcChk)
			if err != nil {
				rw.op.recycleChunk(srcChk)
				terror.Call(rs.Close)
				return err
			}
			if rw.op.checkpointMgr != nil {
				rw.op.checkpointMgr.UpdateTotal(task.id, srcChk.NumRows(), done)
			}
			idxRs := idxRecResult{id: task.id, chunk: srcChk, done: done}
			failpoint.Inject("mockCopSenderError", func() {
				idxRs.err = errors.New("mock cop error")
			})
			rw.sink.Write(idxRs)
		}
		terror.Call(rs.Close)
		return nil
	})
}

// Close implement the Close interface for workerpool.
func (*readCopWorker) Close() {}

// Open implements AsyncOperator.
func (oi *tableScanOperator) Open() error {
	oi.Source.(*operator.AsyncDataChannel[*reorgBackfillTask]).Channel.SetCreateWorker(
		func() workerpool.Worker[*reorgBackfillTask] {
			logutil.BgLogger().Info("ywq test open table scan worker")
			sessCtx, err := oi.sessPool.Get()
			if err != nil {
				logutil.Logger(oi.ctx).Error("copReqSender get session from pool failed", zap.Error(err))
				_ = oi.Sink.Write(idxRecResult{err: err})
				return nil
			}
			se := sess.NewSession(sessCtx)
			return &readCopWorker{oi.Sink, oi, se}
		},
	)
	oi.Source.(*operator.AsyncDataChannel[*reorgBackfillTask]).Channel.Start()
	return nil
}

// Display implements AsyncOperator.
func (oi *tableScanOperator) Display() string {
	return "tableScanOperator{ source: " + oi.Source.Display() + ", sink: " + oi.Sink.Display() + "}"
}

// Close implements AsyncOperator.
func (oi *tableScanOperator) Close() {
	if oi.closed {
		return
	}
	logutil.Logger(oi.ctx).Info("close cop-request sender operator")
	// Wait for all cop-req senders to exit.
	oi.Source.(*operator.AsyncDataChannel[*reorgBackfillTask]).Channel.ReleaseAndWait()
	oi.closed = true
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
	res.Source = source
	res.Sink = sink
	return res, nil
}

type ingestWriterOperator struct {
	operator.BaseOperator[idxRecResult, *backfillResult] // source: idxRecResult, sink: backfillResult.
	reorgInfo                                            *reorgInfo
	poolErr                                              chan error
	backendCtx                                           ingest.BackendCtx
	writerMaxID                                          int
	ctx                                                  context.Context
	tbl                                                  table.PhysicalTable
	checkpointMgr                                        *ingest.CheckpointManager
	tableScan                                            *tableScanOperator
	pool                                                 *workerpool.WorkerPool[idxRecResult]
}

// Open implements AsyncOperator.
func (oi *ingestWriterOperator) Open() error {
	oi.pool.SetCreateWorker(
		func() workerpool.Worker[idxRecResult] {
			logutil.BgLogger().Info("ywq test open ingest writer worker")

			reorgInfo := oi.reorgInfo
			job := reorgInfo.Job
			sessCtx, err := newSessCtx(reorgInfo)
			if err != nil {
				oi.poolErr <- err
				return nil
			}
			bcCtx := oi.backendCtx
			ei, err := bcCtx.Register(job.ID, oi.reorgInfo.currElement.ID, job.SchemaName, job.TableName)
			if err != nil {
				// Return an error only if it is the first worker.
				if oi.writerMaxID == 0 {
					oi.poolErr <- err
					return nil
				}
				logutil.Logger(oi.ctx).Warn("cannot create new writer", zap.Error(err),
					zap.Int64("job ID", reorgInfo.ID), zap.Int64("index ID", oi.reorgInfo.currElement.ID))
				return nil
			}
			worker, err := newAddIndexIngestWorker(oi.ctx, oi.tbl, reorgInfo.d, ei, nil, job.ID,
				reorgInfo.SchemaName, oi.reorgInfo.currElement.ID, oi.writerMaxID,
				nil, sessCtx, oi.checkpointMgr, true)
			worker.tableScan = oi.tableScan
			worker.sink = oi.Sink
			if err != nil {
				// Return an error only if it is the first worker.
				if oi.writerMaxID == 0 {
					oi.poolErr <- err
					return nil
				}
				logutil.Logger(oi.ctx).Warn("cannot create new writer", zap.Error(err),
					zap.Int64("job ID", reorgInfo.ID), zap.Int64("index ID", oi.reorgInfo.currElement.ID))
				return nil
			}
			oi.writerMaxID++
			return worker
		},
	)
	oi.pool.Start()
	return nil
}

// Close implements AsyncOperator.
func (oi *ingestWriterOperator) Close() {
	if oi.pool != nil {
		oi.pool.ReleaseAndWait()
	}
	if oi.checkpointMgr != nil {
		oi.checkpointMgr.Sync()
		// Get the latest status after all workers are closed so that the result is more accurate.
		cnt, nextKey := oi.checkpointMgr.Status()
		_ = oi.Sink.Write(&backfillResult{
			totalCount: cnt,
			nextKey:    nextKey,
		})
	}

	jobID := oi.reorgInfo.ID
	indexID := oi.reorgInfo.currElement.ID
	oi.backendCtx.ResetWorkers(jobID, indexID)
}

// Display implements AsyncOperator.
func (oi *ingestWriterOperator) Display() string {
	return "ingestWriterOperator{ source: " + oi.Source.Display() + ", sink: " + oi.Sink.Display() + "}"
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
	sessPool                                      *sess.Pool // sessPool is used to get the session to update the reorg handle.
	keeper                                        *doneTaskKeeper
	pool                                          *workerpool.WorkerPool[*backfillResult]
	start                                         kv.Key
}

// Open implements AsyncOperator.
func (oi *consumerOperator) Open() error {
	oi.pool.SetCreateWorker(func() workerpool.Worker[*backfillResult] {
		logutil.BgLogger().Info("ywq test open consume worker")
		return &consumeWorker{consumerOperator: oi, sink: oi.Sink, keeper: oi.keeper}
	})
	oi.pool.Start()
	return nil
}

// Close implements AsyncOperator.
func (oi *consumerOperator) Close() {
	if oi.pool != nil {
		oi.pool.ReleaseAndWait()
	}
	if oi.hasError.Load() {
		// ywq todo add error
		logutil.BgLogger().Warn("backfill worker handle tasks failed", zap.String("category", "ddl"),
			zap.String("start key", hex.EncodeToString(oi.start)))
	}
}

// Display implements AsyncOperator.
func (oi *consumerOperator) Display() string {
	return "consumerOperator{ source: " + oi.Source.Display() + ", sink: " + oi.Sink.Display() + "}"
}

func newConsumerOperator(
	dc *ddlCtx, reorgInfo *reorgInfo, sessPool *sess.Pool, start kv.Key) (*consumerOperator, error) {
	res := &consumerOperator{
		dc:        dc,
		hasError:  &atomic.Bool{},
		reorgInfo: reorgInfo,
		sessPool:  sessPool,
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
	res.Sink = &TotalAddedCountSink{0}
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

		// ywq todo
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

type TotalAddedCountSink struct {
	cnt int64
}

func (s *TotalAddedCountSink) Write(data int64) error {
	s.cnt += data
	return nil
}

func (s *TotalAddedCountSink) Display() string {
	return "TotalAddedCountSink"
}

func expectedWorkerSize() (readerSize int, writerSize int) {
	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	readerSize = mathutil.Min(workerCnt/2, maxBackfillWorkerSize)
	readerSize = mathutil.Max(readerSize, 1)
	writerSize = mathutil.Min(workerCnt/2+2, maxBackfillWorkerSize)
	return readerSize, writerSize
}
