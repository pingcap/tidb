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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/ingest"
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
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"go.uber.org/zap"
)

// backfillScheduler is used to manage the lifetime of backfill workers.
type backfillScheduler interface {
	setupWorkers() error
	close(force bool)

	sendTask(task *reorgBackfillTask) error
	drainTasks()
	receiveResult() (*backfillResult, bool)

	setMaxWorkerSize(maxSize int)
	currentWorkerSize() int
	adjustWorkerSize() error
}

var (
	_ backfillScheduler = &txnBackfillScheduler{}
	_ backfillScheduler = &ingestBackfillScheduler{}
)

type txnBackfillScheduler struct {
	ctx          context.Context
	reorgInfo    *reorgInfo
	sessPool     *sessionPool
	tp           backfillerType
	tbl          table.PhysicalTable
	decodeColMap map[int64]decoder.Column
	jobCtx       *JobContext

	workers []*backfillWorker
	wg      sync.WaitGroup
	maxSize int

	taskCh   chan *reorgBackfillTask
	resultCh chan *backfillResult
	closed   bool
}

func newBackfillScheduler(ctx context.Context, info *reorgInfo, sessPool *sessionPool,
	tp backfillerType, tbl table.PhysicalTable, sessCtx sessionctx.Context,
	jobCtx *JobContext) (backfillScheduler, error) {
	if tp == typeAddIndexWorker && info.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
		return newIngestBackfillScheduler(ctx, info, tbl), nil
	}
	return newTxnBackfillScheduler(ctx, info, sessPool, tp, tbl, sessCtx, jobCtx)
}

func newTxnBackfillScheduler(ctx context.Context, info *reorgInfo, sessPool *sessionPool,
	tp backfillerType, tbl table.PhysicalTable, sessCtx sessionctx.Context,
	jobCtx *JobContext) (backfillScheduler, error) {
	decodeColMap, err := makeupDecodeColMap(sessCtx, info.dbInfo.Name, tbl)
	if err != nil {
		return nil, err
	}
	return &txnBackfillScheduler{
		ctx:          ctx,
		reorgInfo:    info,
		sessPool:     sessPool,
		tp:           tp,
		tbl:          tbl,
		decodeColMap: decodeColMap,
		jobCtx:       jobCtx,
		workers:      make([]*backfillWorker, 0, variable.GetDDLReorgWorkerCounter()),
		taskCh:       make(chan *reorgBackfillTask, backfillTaskChanSize),
		resultCh:     make(chan *backfillResult, backfillTaskChanSize),
	}, nil
}

func (b *txnBackfillScheduler) setupWorkers() error {
	return b.adjustWorkerSize()
}

func (b *txnBackfillScheduler) sendTask(task *reorgBackfillTask) error {
	b.taskCh <- task
	return nil
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

func (b *txnBackfillScheduler) setMaxWorkerSize(maxSize int) {
	b.maxSize = maxSize
}

func (b *txnBackfillScheduler) expectedWorkerSize() (size int) {
	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	return mathutil.Min(workerCnt, b.maxSize)
}

func (b *txnBackfillScheduler) currentWorkerSize() int {
	return len(b.workers)
}

func (b *txnBackfillScheduler) adjustWorkerSize() error {
	reorgInfo := b.reorgInfo
	job := reorgInfo.Job
	jc := b.jobCtx
	if err := loadDDLReorgVars(b.ctx, b.sessPool); err != nil {
		logutil.BgLogger().Error("[ddl] load DDL reorganization variable failed", zap.Error(err))
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

func canSkipError(jobID int64, workerCnt int, err error) bool {
	if workerCnt > 0 {
		// The error can be skipped because the rest workers can handle the tasks.
		return true
	}
	logutil.BgLogger().Warn("[ddl] create add index backfill worker failed",
		zap.Int("current worker count", workerCnt),
		zap.Int64("job ID", jobID), zap.Error(err))
	return false
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
	ctx       context.Context
	reorgInfo *reorgInfo
	tbl       table.PhysicalTable

	maxSize int
	closed  bool

	taskCh   chan *reorgBackfillTask
	resultCh chan *backfillResult

	copReqSenderPool *copReqSenderPool

	writerPool   *workerpool.WorkerPool[idxRecResult]
	writerMaxID  atomic.Int64
	writerChkCnt atomic.Int64
	poolErr      chan error
	engineInfo   *ingest.EngineInfo
}

func newIngestBackfillScheduler(ctx context.Context, info *reorgInfo, tbl table.PhysicalTable) *ingestBackfillScheduler {
	return &ingestBackfillScheduler{
		ctx:         ctx,
		reorgInfo:   info,
		tbl:         tbl,
		taskCh:      make(chan *reorgBackfillTask, backfillTaskChanSize),
		resultCh:    make(chan *backfillResult, backfillTaskChanSize),
		writerMaxID: atomic.Int64{},
		poolErr:     make(chan error),
	}
}

func (b *ingestBackfillScheduler) setupWorkers() error {
	job := b.reorgInfo.Job
	bc, ok := ingest.LitBackCtxMgr.Load(job.ID)
	if !ok {
		return errors.Trace(errors.New(ingest.LitErrGetBackendFail))
	}
	ei, err := bc.EngMgr.Register(bc, job.ID, b.reorgInfo.currElement.ID, job.SchemaName, job.TableName)
	if err != nil {
		return errors.Trace(err)
	}
	b.engineInfo = ei
	b.copReqSenderPool, err = b.createCopReqSenderPool()
	if err != nil {
		return errors.Trace(err)
	}
	_, writerCnt := b.expectedWorkerSize()
	skipReg := workerpool.OptionSkipRegister[idxRecResult]{}
	writerPool, err := workerpool.NewWorkerPool[idxRecResult]("ingest_writer",
		poolutil.DDL, writerCnt, b.createWorker, skipReg)
	if err != nil {
		return errors.Trace(err)
	}
	b.reorgInfo.d.setDDLLabelForTopSQL(job.ID, job.Query)
	b.writerPool = writerPool
	b.copReqSenderPool.resultsCh = writerPool
	return b.adjustWorkerSize()
}

func (b *ingestBackfillScheduler) close(force bool) {
	if b.closed {
		return
	}
	close(b.taskCh)
	b.copReqSenderPool.close(force)
	b.writerPool.ReleaseAndWait()
	close(b.resultCh)
	if !force {
		jobID := b.reorgInfo.ID
		indexID := b.reorgInfo.currElement.ID
		if bc, ok := ingest.LitBackCtxMgr.Load(jobID); ok {
			bc.EngMgr.ResetWorkers(bc, jobID, indexID)
		}
	}
	b.closed = true
}

func (b *ingestBackfillScheduler) sendTask(task *reorgBackfillTask) error {
	select {
	case b.taskCh <- task:
		return nil
	case err := <-b.poolErr:
		return err
	}
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
	case rs, err := <-b.resultCh:
		return rs, err
	}
}

func (b *ingestBackfillScheduler) setMaxWorkerSize(maxSize int) {
	b.maxSize = maxSize
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

func (b *ingestBackfillScheduler) createWorker() workerpool.Worker[idxRecResult] {
	reorgInfo := b.reorgInfo
	job := reorgInfo.Job
	writerID := int(b.writerMaxID.Load())
	sessCtx, err := newSessCtx(reorgInfo)
	if err != nil {
		b.poolErr <- err
		return nil
	}
	worker, err := newAddIndexIngestWorker(b.tbl, reorgInfo.d, b.engineInfo, b.resultCh, job.ID,
		reorgInfo.SchemaName, b.reorgInfo.currElement.ID, writerID, b.copReqSenderPool, sessCtx)
	if err != nil {
		if b.writerMaxID.Load() == 0 {
			b.poolErr <- err
			return nil
		}
		logutil.BgLogger().Warn("[ddl-ingest] cannot create new writer", zap.Error(err),
			zap.Int64("job ID", reorgInfo.ID), zap.Int64("index ID", b.reorgInfo.currElement.ID))
	} else {
		b.writerMaxID.Add(1)
	}
	return worker
}

func (b *ingestBackfillScheduler) createCopReqSenderPool() (*copReqSenderPool, error) {
	indexInfo := model.FindIndexInfoByID(b.tbl.Meta().Indices, b.reorgInfo.currElement.ID)
	if indexInfo == nil {
		logutil.BgLogger().Warn("[ddl-ingest] cannot init cop request sender",
			zap.Int64("table ID", b.tbl.Meta().ID), zap.Int64("index ID", b.reorgInfo.currElement.ID))
		return nil, errors.New("cannot find index info")
	}
	sessCtx, err := newSessCtx(b.reorgInfo)
	if err != nil {
		logutil.BgLogger().Warn("[ddl-ingest] cannot init cop request sender", zap.Error(err))
		return nil, err
	}
	copCtx, err := newCopContext(b.tbl.Meta(), indexInfo, sessCtx)
	if err != nil {
		logutil.BgLogger().Warn("[ddl-ingest] cannot init cop request sender", zap.Error(err))
		return nil, err
	}
	return newCopReqSenderPool(b.ctx, copCtx, sessCtx.GetStore(), b.taskCh), nil
}

func (b *ingestBackfillScheduler) expectedWorkerSize() (readerSize int, writerSize int) {
	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	readerSize = mathutil.Min(workerCnt/2, b.maxSize)
	readerSize = mathutil.Max(readerSize, 1)
	writerSize = mathutil.Min(workerCnt/2+2, b.maxSize)
	return readerSize, writerSize
}

func (w *addIndexIngestWorker) HandleTask(rs idxRecResult) {
	defer util.Recover(metrics.LabelDDL, "ingestWorker.HandleTask", func() {
		w.resultCh <- &backfillResult{taskID: rs.id, err: dbterror.ErrReorgPanic}
	}, false)

	result := &backfillResult{
		taskID:     rs.id,
		err:        rs.err,
		addedCount: 0,
		nextKey:    rs.end,
	}
	if result.err != nil {
		logutil.BgLogger().Error("[ddl-ingest] finish a cop-request task with error",
			zap.Int("id", rs.id), zap.Error(rs.err))
		w.resultCh <- result
		return
	}
	err := w.d.isReorgRunnable(w.jobID, false)
	if err != nil {
		result.err = err
		w.resultCh <- result
		return
	}
	taskCtx, err := w.WriteLocal(&rs)
	if err != nil {
		result.err = err
		w.resultCh <- result
		return
	}
	w.metricCounter.Add(float64(taskCtx.addedCount))
	mergeBackfillCtxToResult(&taskCtx, result)
	if ResultCounterForTest != nil && result.err == nil {
		ResultCounterForTest.Add(1)
	}
	w.resultCh <- result
	if rs.done {
		logutil.BgLogger().Info("[ddl-ingest] finish a cop-request task", zap.Int("id", rs.id))
		if bc, ok := ingest.LitBackCtxMgr.Load(w.jobID); ok {
			err := bc.Flush(w.index.Meta().ID)
			if err != nil {
				result.err = err
				w.resultCh <- result
			}
		}
	}
}

func (w *addIndexIngestWorker) Close() {}
