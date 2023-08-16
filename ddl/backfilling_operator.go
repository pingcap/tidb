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
)

//type ingestBackfillBuilder struct {
//	ctx       context.Context
//	reorgInfo *reorgInfo
//	sessPool  *sess.Pool
//	tbl       table.PhysicalTable
//	closed    bool
//	//taskCh   chan *reorgBackfillTask
//	//resultCh chan *backfillResult
//	//copReqSenderPool *copReqSenderPool
//	//writerPool    *workerpool.WorkerPool[idxRecResult]
//	writerMaxID   int
//	poolErr       chan error
//	backendCtx    ingest.BackendCtx
//	checkpointMgr *ingest.CheckpointManager
//}

func NewAddIndexPipeline(
	ctx context.Context,
	info *reorgInfo,
	sessPool *sess.Pool,
	tbl table.PhysicalTable) (*operator.AsyncPipeline, error) {

	res := &operator.AsyncPipeline{}

	return res
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
func (*readCopWorker) Close() {

}

// Open implements AsyncOperator.
func (oi *tableScanOperator) Open() error {
	oi.Source.(*operator.AsyncDataChannel[*reorgBackfillTask]).Channel.SetCreateWorker(
		func() workerpool.Worker[*reorgBackfillTask] {
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
	checkpointMgr *ingest.CheckpointManager, workerSize int,
	sink operator.DataSink[idxRecResult]) (*tableScanOperator, error) {
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
	operator.BaseOperator[idxRecResult, backfillResult] // source: idxRecResult, sink: backfillResult.
	reorgInfo                                           *reorgInfo
	poolErr                                             chan error
	backendCtx                                          ingest.BackendCtx
	writerMaxID                                         int
	ctx                                                 context.Context
	tbl                                                 table.PhysicalTable
	resultCh                                            chan *backfillResult
	checkpointMgr                                       *ingest.CheckpointManager
	tableScan                                           *tableScanOperator
	pool                                                *workerpool.WorkerPool[idxRecResult]
}

// Open implements AsyncOperator.
func (oi *ingestWriterOperator) Open() error {
	oi.pool.SetCreateWorker(
		func() workerpool.Worker[idxRecResult] {
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
			worker, err := newAddIndexIngestWorker(oi.ctx, oi.tbl, reorgInfo.d, ei, oi.resultCh, job.ID,
				reorgInfo.SchemaName, oi.reorgInfo.currElement.ID, oi.writerMaxID,
				nil, sessCtx, oi.checkpointMgr, true)
			worker.tableScan = oi.tableScan
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
	return nil
}

// Close implements AsyncOperator.
func (oi *ingestWriterOperator) Close() {
	if oi.pool != nil {
		oi.pool.ReleaseAndWait()
	}
}

// Display implements AsyncOperator.
func (oi *ingestWriterOperator) Display() string {
	return "ingestWriterOperator{ source: " + oi.Source.Display() + ", sink: " + oi.Sink.Display() + "}"
}

func newIngestWriterOperator(
	ctx context.Context,
	tbl table.PhysicalTable,
	info *reorgInfo,
	checkpointMgr *ingest.CheckpointManager,
	backendCtx ingest.BackendCtx,
	tableScan *tableScanOperator,
	writerCnt int) (*ingestWriterOperator, error) {

	res := &ingestWriterOperator{
		ctx:           ctx,
		reorgInfo:     info,
		backendCtx:    backendCtx,
		writerMaxID:   0,
		checkpointMgr: checkpointMgr,
		tableScan:     tableScan,
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
	// sink need to decide...
	return res, nil
}

func expectedWorkerSize() (readerSize int, writerSize int) {
	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	readerSize = mathutil.Min(workerCnt/2, maxBackfillWorkerSize)
	readerSize = mathutil.Max(readerSize, 1)
	writerSize = mathutil.Min(workerCnt/2+2, maxBackfillWorkerSize)
	return readerSize, writerSize
}

// todo can add it..
//type splitKVRangeOperator struct {
//	operator.BaseOperator[[]kv.KeyRange, *reorgBackfillTask]
//}
