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
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/ingest"
	sess "github.com/pingcap/tidb/ddl/internal/session"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"go.uber.org/zap"
)

// backfillScheduler is used to manage the lifetime of backfill workers.
type backfillScheduler interface {
	setupWorkers() error
	close(force bool)

	sendTask(task *reorgBackfillTask)
	drainTasks()
	receiveResult() (*backfillResult, bool)

	currentWorkerSize() int
	adjustWorkerSize() error
}

var (
	_ backfillScheduler = &txnBackfillScheduler{}
	_ backfillScheduler = &ingestBackfillScheduler{}
)

const maxBackfillWorkerSize = 16

type txnBackfillScheduler struct {
	ctx          context.Context
	reorgInfo    *reorgInfo
	sessPool     *sess.Pool
	tp           backfillerType
	tbl          table.PhysicalTable
	decodeColMap map[int64]decoder.Column
	jobCtx       *JobContext

	workers []*backfillWorker
	wg      sync.WaitGroup

	taskCh   chan *reorgBackfillTask
	resultCh chan *backfillResult
	closed   bool
}

func newBackfillScheduler(ctx context.Context, info *reorgInfo, sessPool *sess.Pool,
	tp backfillerType, tbl table.PhysicalTable, sessCtx sessionctx.Context,
	jobCtx *JobContext) (backfillScheduler, error) {
	if tp == typeAddIndexWorker && info.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
		ctx = logutil.WithCategory(ctx, "ddl-ingest")
		return newIngestBackfillScheduler(ctx, info, sessPool, tbl, false), nil
	}
	return newTxnBackfillScheduler(ctx, info, sessPool, tp, tbl, sessCtx, jobCtx)
}

func newTxnBackfillScheduler(ctx context.Context, info *reorgInfo, sessPool *sess.Pool,
	tp backfillerType, tbl table.PhysicalTable, sessCtx sessionctx.Context,
	jobCtx *JobContext) (backfillScheduler, error) {
	decColMap, err := makeupDecodeColMap(sessCtx, info.dbInfo.Name, tbl)
	if err != nil {
		return nil, err
	}
	return &txnBackfillScheduler{
		ctx:          ctx,
		reorgInfo:    info,
		sessPool:     sessPool,
		tp:           tp,
		tbl:          tbl,
		decodeColMap: decColMap,
		jobCtx:       jobCtx,
		workers:      make([]*backfillWorker, 0, variable.GetDDLReorgWorkerCounter()),
		taskCh:       make(chan *reorgBackfillTask, backfillTaskChanSize),
		resultCh:     make(chan *backfillResult, backfillTaskChanSize),
	}, nil
}

func (b *txnBackfillScheduler) setupWorkers() error {
	return b.adjustWorkerSize()
}

func (b *txnBackfillScheduler) sendTask(task *reorgBackfillTask) {
	b.taskCh <- task
}

func (b *txnBackfillScheduler) drainTasks() {
	for len(b.taskCh) > 0 {
		<-b.taskCh
	}
}

func (b *txnBackfillScheduler) receiveResult() (*backfillResult, bool) {
	ret, ok := <-b.resultCh
	return ret, ok
}

func newSessCtx(reorgInfo *reorgInfo) (sessionctx.Context, error) {
	sessCtx := newContext(reorgInfo.d.store)
	if err := initSessCtx(sessCtx, reorgInfo.ReorgMeta.SQLMode, reorgInfo.ReorgMeta.Location); err != nil {
		return nil, errors.Trace(err)
	}
	return sessCtx, nil
}

func initSessCtx(sessCtx sessionctx.Context, sqlMode mysql.SQLMode, tzLocation *model.TimeZoneLocation) error {
	// Unify the TimeZone settings in newContext.
	if sessCtx.GetSessionVars().StmtCtx.TimeZone == nil {
		tz := *time.UTC
		sessCtx.GetSessionVars().StmtCtx.TimeZone = &tz
	}
	sessCtx.GetSessionVars().StmtCtx.IsDDLJobInQueue = true
	// Set the row encode format version.
	rowFormat := variable.GetDDLReorgRowFormat()
	sessCtx.GetSessionVars().RowEncoder.Enable = rowFormat != variable.DefTiDBRowFormatV1
	// Simulate the sql mode environment in the worker sessionCtx.
	sessCtx.GetSessionVars().SQLMode = sqlMode
	if err := setSessCtxLocation(sessCtx, tzLocation); err != nil {
		return errors.Trace(err)
	}
	sessCtx.GetSessionVars().StmtCtx.BadNullAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.TruncateAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.OverflowAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.AllowInvalidDate = sqlMode.HasAllowInvalidDatesMode()
	sessCtx.GetSessionVars().StmtCtx.DividedByZeroAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.IgnoreZeroInDate = !sqlMode.HasStrictMode() || sqlMode.HasAllowInvalidDatesMode()
	sessCtx.GetSessionVars().StmtCtx.NoZeroDate = sqlMode.HasStrictMode()
	// Prevent initializing the mock context in the workers concurrently.
	// For details, see https://github.com/pingcap/tidb/issues/40879.
	_ = sessCtx.GetDomainInfoSchema()
	return nil
}

func (*txnBackfillScheduler) expectedWorkerSize() (size int) {
	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	return mathutil.Min(workerCnt, maxBackfillWorkerSize)
}

func (b *txnBackfillScheduler) currentWorkerSize() int {
	return len(b.workers)
}

func (b *txnBackfillScheduler) adjustWorkerSize() error {
	reorgInfo := b.reorgInfo
	job := reorgInfo.Job
	jc := b.jobCtx
	if err := loadDDLReorgVars(b.ctx, b.sessPool); err != nil {
		logutil.BgLogger().Error("load DDL reorganization variable failed", zap.String("category", "ddl"), zap.Error(err))
	}
	workerCnt := b.expectedWorkerSize()
	// Increase the worker.
	for i := len(b.workers); i < workerCnt; i++ {
		sessCtx, err := newSessCtx(b.reorgInfo)
		if err != nil {
			return err
		}
		var (
			runner *backfillWorker
			worker backfiller
		)
		switch b.tp {
		case typeAddIndexWorker:
			backfillCtx := newBackfillCtx(reorgInfo.d, i, sessCtx, job.SchemaName, b.tbl, jc, "add_idx_rate", false)
			idxWorker, err := newAddIndexTxnWorker(b.decodeColMap, b.tbl, backfillCtx,
				job.ID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey)
			if err != nil {
				return err
			}
			runner = newBackfillWorker(jc.ddlJobCtx, idxWorker)
			worker = idxWorker
		case typeAddIndexMergeTmpWorker:
			backfillCtx := newBackfillCtx(reorgInfo.d, i, sessCtx, job.SchemaName, b.tbl, jc, "merge_tmp_idx_rate", false)
			tmpIdxWorker := newMergeTempIndexWorker(backfillCtx, b.tbl, reorgInfo.currElement.ID)
			runner = newBackfillWorker(jc.ddlJobCtx, tmpIdxWorker)
			worker = tmpIdxWorker
		case typeUpdateColumnWorker:
			// Setting InCreateOrAlterStmt tells the difference between SELECT casting and ALTER COLUMN casting.
			sessCtx.GetSessionVars().StmtCtx.InCreateOrAlterStmt = true
			updateWorker := newUpdateColumnWorker(sessCtx, i, b.tbl, b.decodeColMap, reorgInfo, jc)
			runner = newBackfillWorker(jc.ddlJobCtx, updateWorker)
			worker = updateWorker
		case typeCleanUpIndexWorker:
			idxWorker := newCleanUpIndexWorker(sessCtx, i, b.tbl, b.decodeColMap, reorgInfo, jc)
			runner = newBackfillWorker(jc.ddlJobCtx, idxWorker)
			worker = idxWorker
		case typeReorgPartitionWorker:
			partWorker, err := newReorgPartitionWorker(sessCtx, i, b.tbl, b.decodeColMap, reorgInfo, jc)
			if err != nil {
				return err
			}
			runner = newBackfillWorker(jc.ddlJobCtx, partWorker)
			worker = partWorker
		default:
			return errors.New("unknown backfill type")
		}
		runner.taskCh = b.taskCh
		runner.resultCh = b.resultCh
		runner.wg = &b.wg
		b.workers = append(b.workers, runner)
		b.wg.Add(1)
		go runner.run(reorgInfo.d, worker, job)
	}
	// Decrease the worker.
	if len(b.workers) > workerCnt {
		workers := b.workers[workerCnt:]
		b.workers = b.workers[:workerCnt]
		closeBackfillWorkers(workers)
	}
	return injectCheckBackfillWorkerNum(len(b.workers), b.tp == typeAddIndexMergeTmpWorker)
}

func (b *txnBackfillScheduler) close(force bool) {
	if b.closed {
		return
	}
	close(b.taskCh)
	if force {
		closeBackfillWorkers(b.workers)
	}
	b.wg.Wait()
	close(b.resultCh)
	b.closed = true
}

type ingestBackfillScheduler struct {
	ctx        context.Context
	reorgInfo  *reorgInfo
	sessPool   *sess.Pool
	tbl        table.PhysicalTable
	distribute bool

	closed bool

	taskCh   chan *reorgBackfillTask
	resultCh chan *backfillResult

	copReqSenderPool *copReqSenderPool

	writerPool    *workerpool.WorkerPool[IndexRecordChunk, workerpool.None]
	writerMaxID   int
	poolErr       chan error
	backendCtx    ingest.BackendCtx
	checkpointMgr *ingest.CheckpointManager
}

func newIngestBackfillScheduler(ctx context.Context, info *reorgInfo,
	sessPool *sess.Pool, tbl table.PhysicalTable, distribute bool) *ingestBackfillScheduler {
	return &ingestBackfillScheduler{
		ctx:        ctx,
		reorgInfo:  info,
		sessPool:   sessPool,
		tbl:        tbl,
		taskCh:     make(chan *reorgBackfillTask, backfillTaskChanSize),
		resultCh:   make(chan *backfillResult, backfillTaskChanSize),
		poolErr:    make(chan error),
		distribute: distribute,
	}
}

func (b *ingestBackfillScheduler) setupWorkers() error {
	job := b.reorgInfo.Job
	bc, ok := ingest.LitBackCtxMgr.Load(job.ID)
	if !ok {
		logutil.Logger(b.ctx).Error(ingest.LitErrGetBackendFail, zap.Int64("job ID", job.ID))
		return errors.Trace(errors.New("cannot get lightning backend"))
	}
	b.backendCtx = bc
	mgr := bc.GetCheckpointManager()
	if mgr != nil {
		mgr.Reset(b.tbl.GetPhysicalID(), b.reorgInfo.StartKey, b.reorgInfo.EndKey)
		b.checkpointMgr = mgr
	}
	copReqSenderPool, err := b.createCopReqSenderPool()
	if err != nil {
		return errors.Trace(err)
	}
	b.copReqSenderPool = copReqSenderPool
	readerCnt, writerCnt := b.expectedWorkerSize()
	writerPool := workerpool.NewWorkerPool[IndexRecordChunk]("ingest_writer",
		poolutil.DDL, writerCnt, b.createWorker)
	writerPool.Start(b.ctx)
	b.writerPool = writerPool
	b.copReqSenderPool.chunkSender = writerPool
	b.copReqSenderPool.adjustSize(readerCnt)
	return nil
}

func (b *ingestBackfillScheduler) close(force bool) {
	if b.closed {
		return
	}
	close(b.taskCh)
	if b.copReqSenderPool != nil {
		b.copReqSenderPool.close(force)
	}
	if b.writerPool != nil {
		b.writerPool.ReleaseAndWait()
	}
	if b.checkpointMgr != nil {
		b.checkpointMgr.Sync()
		// Get the latest status after all workers are closed so that the result is more accurate.
		cnt, nextKey := b.checkpointMgr.Status()
		b.resultCh <- &backfillResult{
			totalCount: cnt,
			nextKey:    nextKey,
		}
	}
	close(b.resultCh)
	if intest.InTest && len(b.copReqSenderPool.srcChkPool) != copReadChunkPoolSize() {
		panic(fmt.Sprintf("unexpected chunk size %d", len(b.copReqSenderPool.srcChkPool)))
	}
	if !force {
		jobID := b.reorgInfo.ID
		indexID := b.reorgInfo.currElement.ID
		b.backendCtx.ResetWorkers(jobID, indexID)
	}
	b.closed = true
}

func (b *ingestBackfillScheduler) sendTask(task *reorgBackfillTask) {
	b.taskCh <- task
}

func (b *ingestBackfillScheduler) drainTasks() {
	for len(b.taskCh) > 0 {
		<-b.taskCh
	}
}

func (b *ingestBackfillScheduler) receiveResult() (*backfillResult, bool) {
	select {
	case err := <-b.poolErr:
		return &backfillResult{err: err}, true
	case rs, ok := <-b.resultCh:
		return rs, ok
	}
}

func (b *ingestBackfillScheduler) currentWorkerSize() int {
	return int(b.writerPool.Cap())
}

func (b *ingestBackfillScheduler) adjustWorkerSize() error {
	readerCnt, writer := b.expectedWorkerSize()
	b.writerPool.Tune(int32(writer))
	b.copReqSenderPool.adjustSize(readerCnt)
	return nil
}

func (b *ingestBackfillScheduler) createWorker() workerpool.Worker[IndexRecordChunk, workerpool.None] {
	reorgInfo := b.reorgInfo
	job := reorgInfo.Job
	sessCtx, err := newSessCtx(reorgInfo)
	if err != nil {
		b.poolErr <- err
		return nil
	}
	bcCtx := b.backendCtx
	ei, err := bcCtx.Register(job.ID, b.reorgInfo.currElement.ID, job.SchemaName, job.TableName)
	if err != nil {
		// Return an error only if it is the first worker.
		if b.writerMaxID == 0 {
			b.poolErr <- err
			return nil
		}
		logutil.Logger(b.ctx).Warn("cannot create new writer", zap.Error(err),
			zap.Int64("job ID", reorgInfo.ID), zap.Int64("index ID", b.reorgInfo.currElement.ID))
		return nil
	}
	worker, err := newAddIndexIngestWorker(b.ctx, b.tbl, reorgInfo.d, ei, b.resultCh, job.ID,
		reorgInfo.SchemaName, b.reorgInfo.currElement.ID, b.writerMaxID,
		b.copReqSenderPool, sessCtx, b.checkpointMgr, b.distribute)
	if err != nil {
		// Return an error only if it is the first worker.
		if b.writerMaxID == 0 {
			b.poolErr <- err
			return nil
		}
		logutil.Logger(b.ctx).Warn("cannot create new writer", zap.Error(err),
			zap.Int64("job ID", reorgInfo.ID), zap.Int64("index ID", b.reorgInfo.currElement.ID))
		return nil
	}
	b.writerMaxID++
	return worker
}

func (b *ingestBackfillScheduler) createCopReqSenderPool() (*copReqSenderPool, error) {
	indexInfo := model.FindIndexInfoByID(b.tbl.Meta().Indices, b.reorgInfo.currElement.ID)
	if indexInfo == nil {
		logutil.Logger(b.ctx).Warn("cannot init cop request sender",
			zap.Int64("table ID", b.tbl.Meta().ID), zap.Int64("index ID", b.reorgInfo.currElement.ID))
		return nil, errors.New("cannot find index info")
	}
	sessCtx, err := newSessCtx(b.reorgInfo)
	if err != nil {
		logutil.Logger(b.ctx).Warn("cannot init cop request sender", zap.Error(err))
		return nil, err
	}
	copCtx, err := NewCopContext(b.tbl.Meta(), indexInfo, sessCtx)
	if err != nil {
		logutil.Logger(b.ctx).Warn("cannot init cop request sender", zap.Error(err))
		return nil, err
	}
	return newCopReqSenderPool(b.ctx, copCtx, sessCtx.GetStore(), b.taskCh, b.sessPool, b.checkpointMgr), nil
}

func (*ingestBackfillScheduler) expectedWorkerSize() (readerSize int, writerSize int) {
	return expectedIngestWorkerCnt()
}

func expectedIngestWorkerCnt() (readerCnt, writerCnt int) {
	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	readerCnt = mathutil.Min(workerCnt/2, maxBackfillWorkerSize)
	readerCnt = mathutil.Max(readerCnt, 1)
	writerCnt = mathutil.Min(workerCnt/2+2, maxBackfillWorkerSize)
	return readerCnt, writerCnt
}

func (w *addIndexIngestWorker) HandleTask(rs IndexRecordChunk, _ func(workerpool.None)) {
	defer util.Recover(metrics.LabelDDL, "ingestWorker.HandleTask", func() {
		w.resultCh <- &backfillResult{taskID: rs.ID, err: dbterror.ErrReorgPanic}
	}, false)
	defer w.copReqSenderPool.recycleChunk(rs.Chunk)
	result := &backfillResult{
		taskID: rs.ID,
		err:    rs.Err,
	}
	if result.err != nil {
		logutil.Logger(w.ctx).Error("encounter error when handle index chunk",
			zap.Int("id", rs.ID), zap.Error(rs.Err))
		w.resultCh <- result
		return
	}
	if !w.distribute {
		err := w.d.isReorgRunnable(w.jobID, false)
		if err != nil {
			result.err = err
			w.resultCh <- result
			return
		}
	}
	count, nextKey, err := w.WriteLocal(&rs)
	if err != nil {
		result.err = err
		w.resultCh <- result
		return
	}
	if count == 0 {
		logutil.Logger(w.ctx).Info("finish a cop-request task", zap.Int("id", rs.ID))
		return
	}
	if w.checkpointMgr != nil {
		cnt, nextKey := w.checkpointMgr.Status()
		result.totalCount = cnt
		result.nextKey = nextKey
		result.err = w.checkpointMgr.UpdateCurrent(rs.ID, count)
	} else {
		result.addedCount = count
		result.scanCount = count
		result.nextKey = nextKey
	}
	if ResultCounterForTest != nil && result.err == nil {
		ResultCounterForTest.Add(1)
	}
	w.resultCh <- result
}

func (*addIndexIngestWorker) Close() {}

type taskIDAllocator struct {
	id int
}

func newTaskIDAllocator() *taskIDAllocator {
	return &taskIDAllocator{}
}

func (a *taskIDAllocator) alloc() int {
	a.id++
	return a.id
}
