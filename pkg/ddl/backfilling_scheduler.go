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
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/mock"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"go.uber.org/zap"
)

// backfillScheduler is used to manage the lifetime of backfill workers.
type backfillScheduler interface {
	setupWorkers() error
	close(force bool)

	sendTask(*reorgBackfillTask) error
	resultChan() <-chan *backfillResult

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

func newBackfillScheduler(
	ctx context.Context,
	info *reorgInfo,
	sessPool *sess.Pool,
	tp backfillerType,
	tbl table.PhysicalTable,
	sessCtx sessionctx.Context,
	jobCtx *JobContext,
) (backfillScheduler, error) {
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

func (b *txnBackfillScheduler) sendTask(task *reorgBackfillTask) error {
	select {
	case <-b.ctx.Done():
		return b.ctx.Err()
	case b.taskCh <- task:
		return nil
	}
}

func (b *txnBackfillScheduler) resultChan() <-chan *backfillResult {
	return b.resultCh
}

func newSessCtx(
	store kv.Storage,
	sqlMode mysql.SQLMode,
	tzLocation *model.TimeZoneLocation,
	resourceGroupName string,
) (sessionctx.Context, error) {
	sessCtx := newReorgSessCtx(store)
	if err := initSessCtx(sessCtx, sqlMode, tzLocation, resourceGroupName); err != nil {
		return nil, errors.Trace(err)
	}
	return sessCtx, nil
}

// initSessCtx initializes the session context. Be careful to the timezone.
func initSessCtx(
	sessCtx sessionctx.Context,
	sqlMode mysql.SQLMode,
	tzLocation *model.TimeZoneLocation,
	resGroupName string,
) error {
	// Correct the initial timezone.
	tz := *time.UTC
	sessCtx.GetSessionVars().TimeZone = &tz
	sessCtx.GetSessionVars().StmtCtx.SetTimeZone(&tz)
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
	sessCtx.GetSessionVars().StmtCtx.NoDefaultAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.TruncateAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.OverflowAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.AllowInvalidDate = sqlMode.HasAllowInvalidDatesMode()
	sessCtx.GetSessionVars().StmtCtx.DividedByZeroAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.IgnoreZeroInDate = !sqlMode.HasStrictMode() || sqlMode.HasAllowInvalidDatesMode()
	sessCtx.GetSessionVars().StmtCtx.NoZeroDate = sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.ResourceGroupName = resGroupName
	// Prevent initializing the mock context in the workers concurrently.
	// For details, see https://github.com/pingcap/tidb/issues/40879.
	if _, ok := sessCtx.(*mock.Context); ok {
		_ = sessCtx.GetDomainInfoSchema()
	}
	return nil
}

func restoreSessCtx(sessCtx sessionctx.Context) func(sessCtx sessionctx.Context) {
	sv := sessCtx.GetSessionVars()
	rowEncoder := sv.RowEncoder.Enable
	sqlMode := sv.SQLMode
	var timezone *time.Location
	if sv.TimeZone != nil {
		// Copy the content of timezone instead of pointer because it may be changed.
		tz := *sv.TimeZone
		timezone = &tz
	}
	badNullAsWarn := sv.StmtCtx.BadNullAsWarning
	noDefaultAsWarn := sv.StmtCtx.NoDefaultAsWarning
	overflowAsWarn := sv.StmtCtx.OverflowAsWarning
	dividedZeroAsWarn := sv.StmtCtx.DividedByZeroAsWarning
	ignoreZeroInDate := sv.StmtCtx.IgnoreZeroInDate
	noZeroDate := sv.StmtCtx.NoZeroDate
	resGroupName := sv.StmtCtx.ResourceGroupName
	return func(usedSessCtx sessionctx.Context) {
		uv := usedSessCtx.GetSessionVars()
		uv.RowEncoder.Enable = rowEncoder
		uv.SQLMode = sqlMode
		uv.TimeZone = timezone
		uv.StmtCtx.BadNullAsWarning = badNullAsWarn
		uv.StmtCtx.NoDefaultAsWarning = noDefaultAsWarn
		uv.StmtCtx.OverflowAsWarning = overflowAsWarn
		uv.StmtCtx.DividedByZeroAsWarning = dividedZeroAsWarn
		uv.StmtCtx.IgnoreZeroInDate = ignoreZeroInDate
		uv.StmtCtx.NoZeroDate = noZeroDate
		uv.StmtCtx.ResourceGroupName = resGroupName
	}
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
		sessCtx, err := newSessCtx(reorgInfo.d.store, reorgInfo.ReorgMeta.SQLMode, reorgInfo.ReorgMeta.Location, reorgInfo.ReorgMeta.ResourceGroupName)
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
				job.ID, reorgInfo.elements, reorgInfo.currElement.TypeKey)
			if err != nil {
				return err
			}
			runner = newBackfillWorker(b.ctx, idxWorker)
			worker = idxWorker
		case typeAddIndexMergeTmpWorker:
			backfillCtx := newBackfillCtx(reorgInfo.d, i, sessCtx, job.SchemaName, b.tbl, jc, "merge_tmp_idx_rate", false)
			tmpIdxWorker := newMergeTempIndexWorker(backfillCtx, b.tbl, reorgInfo.elements)
			runner = newBackfillWorker(b.ctx, tmpIdxWorker)
			worker = tmpIdxWorker
		case typeUpdateColumnWorker:
			// Setting InCreateOrAlterStmt tells the difference between SELECT casting and ALTER COLUMN casting.
			sessCtx.GetSessionVars().StmtCtx.InCreateOrAlterStmt = true
			updateWorker := newUpdateColumnWorker(sessCtx, i, b.tbl, b.decodeColMap, reorgInfo, jc)
			runner = newBackfillWorker(b.ctx, updateWorker)
			worker = updateWorker
		case typeCleanUpIndexWorker:
			idxWorker := newCleanUpIndexWorker(sessCtx, i, b.tbl, b.decodeColMap, reorgInfo, jc)
			runner = newBackfillWorker(b.ctx, idxWorker)
			worker = idxWorker
		case typeReorgPartitionWorker:
			partWorker, err := newReorgPartitionWorker(sessCtx, i, b.tbl, b.decodeColMap, reorgInfo, jc)
			if err != nil {
				return err
			}
			runner = newBackfillWorker(b.ctx, partWorker)
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
	b.closed = true
	close(b.taskCh)
	if force {
		closeBackfillWorkers(b.workers)
	}
	b.wg.Wait()
	close(b.resultCh)
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
	b.closed = true
	close(b.taskCh)
	if b.copReqSenderPool != nil {
		b.copReqSenderPool.close(force)
	}
	if b.writerPool != nil {
		b.writerPool.ReleaseAndWait()
	}
	if b.checkpointMgr != nil {
		b.checkpointMgr.Flush()
		// Get the latest status after all workers are closed so that the result is more accurate.
		cnt, nextKey := b.checkpointMgr.Status()
		b.sendResult(&backfillResult{
			totalCount: cnt,
			nextKey:    nextKey,
		})
	}
	close(b.resultCh)
	if intest.InTest && len(b.copReqSenderPool.srcChkPool) != copReadChunkPoolSize() {
		panic(fmt.Sprintf("unexpected chunk size %d", len(b.copReqSenderPool.srcChkPool)))
	}
	if !force {
		jobID := b.reorgInfo.ID
		b.backendCtx.ResetWorkers(jobID)
	}
}

func (b *ingestBackfillScheduler) sendTask(task *reorgBackfillTask) error {
	select {
	case <-b.ctx.Done():
		return b.ctx.Err()
	case b.taskCh <- task:
		return nil
	}
}

func (b *ingestBackfillScheduler) sendResult(res *backfillResult) {
	select {
	case <-b.ctx.Done():
	case b.resultCh <- res:
	}
}

func (b *ingestBackfillScheduler) resultChan() <-chan *backfillResult {
	return b.resultCh
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
	sessCtx, err := newSessCtx(reorgInfo.d.store, reorgInfo.ReorgMeta.SQLMode, reorgInfo.ReorgMeta.Location, reorgInfo.ReorgMeta.ResourceGroupName)
	if err != nil {
		b.sendResult(&backfillResult{err: err})
		return nil
	}
	bcCtx := b.backendCtx
	indexIDs := make([]int64, 0, len(reorgInfo.elements))
	engines := make([]ingest.Engine, 0, len(reorgInfo.elements))
	for _, elem := range reorgInfo.elements {
		ei, err := bcCtx.Register(job.ID, elem.ID, job.SchemaName, job.TableName)
		if err != nil {
			// Return an error only if it is the first worker.
			if b.writerMaxID == 0 {
				b.sendResult(&backfillResult{err: err})
				return nil
			}
			logutil.Logger(b.ctx).Warn("cannot create new writer", zap.Error(err),
				zap.Int64("job ID", reorgInfo.ID), zap.Int64("index ID", elem.ID))
			return nil
		}
		indexIDs = append(indexIDs, elem.ID)
		engines = append(engines, ei)
	}

	worker, err := newAddIndexIngestWorker(
		b.ctx, b.tbl, reorgInfo.d, engines, b.resultCh, job.ID,
		reorgInfo.SchemaName, indexIDs, b.writerMaxID,
		b.copReqSenderPool, sessCtx, b.checkpointMgr, b.distribute)
	if err != nil {
		// Return an error only if it is the first worker.
		if b.writerMaxID == 0 {
			b.sendResult(&backfillResult{err: err})
			return nil
		}
		logutil.Logger(b.ctx).Warn("cannot create new writer", zap.Error(err),
			zap.Int64("job ID", reorgInfo.ID), zap.Int64s("index IDs", indexIDs))
		return nil
	}
	b.writerMaxID++
	return worker
}

func (b *ingestBackfillScheduler) createCopReqSenderPool() (*copReqSenderPool, error) {
	ri := b.reorgInfo
	allIndexInfos := make([]*model.IndexInfo, 0, len(ri.elements))
	for _, elem := range ri.elements {
		indexInfo := model.FindIndexInfoByID(b.tbl.Meta().Indices, elem.ID)
		if indexInfo == nil {
			logutil.Logger(b.ctx).Warn("cannot init cop request sender",
				zap.Int64("table ID", b.tbl.Meta().ID), zap.Int64("index ID", elem.ID))
			return nil, errors.New("cannot find index info")
		}
		allIndexInfos = append(allIndexInfos, indexInfo)
	}
	sessCtx, err := newSessCtx(ri.d.store, ri.ReorgMeta.SQLMode, ri.ReorgMeta.Location, ri.ReorgMeta.ResourceGroupName)
	if err != nil {
		logutil.Logger(b.ctx).Warn("cannot init cop request sender", zap.Error(err))
		return nil, err
	}
	reqSrc := getDDLRequestSource(model.ActionAddIndex)
	copCtx, err := copr.NewCopContext(b.tbl.Meta(), allIndexInfos, sessCtx, reqSrc)
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

func (w *addIndexIngestWorker) sendResult(res *backfillResult) {
	select {
	case <-w.ctx.Done():
	case w.resultCh <- res:
	}
}

func (w *addIndexIngestWorker) HandleTask(rs IndexRecordChunk, _ func(workerpool.None)) {
	defer util.Recover(metrics.LabelDDL, "ingestWorker.HandleTask", func() {
		w.sendResult(&backfillResult{taskID: rs.ID, err: dbterror.ErrReorgPanic})
	}, false)
	defer w.copReqSenderPool.recycleChunk(rs.Chunk)
	result := &backfillResult{
		taskID: rs.ID,
		err:    rs.Err,
	}
	if result.err != nil {
		logutil.Logger(w.ctx).Error("encounter error when handle index chunk",
			zap.Int("id", rs.ID), zap.Error(rs.Err))
		w.sendResult(result)
		return
	}
	if !w.distribute {
		err := w.d.isReorgRunnable(w.jobID, false)
		if err != nil {
			result.err = err
			w.sendResult(result)
			return
		}
	}
	count, nextKey, err := w.WriteLocal(&rs)
	if err != nil {
		result.err = err
		w.sendResult(result)
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
		result.err = w.checkpointMgr.UpdateWrittenKeys(rs.ID, count)
	} else {
		result.addedCount = count
		result.scanCount = count
		result.nextKey = nextKey
	}
	if ResultCounterForTest != nil && result.err == nil {
		ResultCounterForTest.Add(1)
	}
	w.sendResult(result)
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
