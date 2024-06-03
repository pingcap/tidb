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
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	ddllogutil "github.com/pingcap/tidb/pkg/ddl/logutil"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	tikvstore "github.com/tikv/client-go/v2/kv"
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
	jobCtx *JobContext,
) (backfillScheduler, error) {
	if tp == typeAddIndexWorker && info.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
		ctx = logutil.WithCategory(ctx, "ddl-ingest")
		return newIngestBackfillScheduler(ctx, info, sessPool, tbl)
	}
	return newTxnBackfillScheduler(ctx, info, sessPool, tp, tbl, jobCtx)
}

func newTxnBackfillScheduler(ctx context.Context, info *reorgInfo, sessPool *sess.Pool,
	tp backfillerType, tbl table.PhysicalTable,
	jobCtx *JobContext) (backfillScheduler, error) {
	decColMap, err := makeupDecodeColMap(info.dbInfo.Name, tbl)
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

// NewReorgCopContext creates a CopContext for reorg
func NewReorgCopContext(
	store kv.Storage,
	reorgMeta *model.DDLReorgMeta,
	tblInfo *model.TableInfo,
	allIdxInfo []*model.IndexInfo,
	requestSource string,
) (copr.CopContext, error) {
	sessCtx, err := newSessCtx(store, reorgMeta)
	if err != nil {
		return nil, err
	}
	return copr.NewCopContext(
		sessCtx.GetExprCtx(),
		sessCtx.GetDistSQLCtx(),
		sessCtx.GetSessionVars().StmtCtx.PushDownFlags(),
		tblInfo,
		allIdxInfo,
		requestSource,
	)
}

func newSessCtx(store kv.Storage, reorgMeta *model.DDLReorgMeta) (sessionctx.Context, error) {
	sessCtx := newReorgSessCtx(store)
	if err := initSessCtx(sessCtx, reorgMeta); err != nil {
		return nil, errors.Trace(err)
	}
	return sessCtx, nil
}

func newDefaultReorgDistSQLCtx(kvClient kv.Client) *distsqlctx.DistSQLContext {
	warnHandler := contextutil.NewStaticWarnHandler(0)
	var sqlKiller sqlkiller.SQLKiller
	var execDetails execdetails.SyncExecDetails
	return &distsqlctx.DistSQLContext{
		WarnHandler:                          warnHandler,
		Client:                               kvClient,
		EnableChunkRPC:                       true,
		EnabledRateLimitAction:               variable.DefTiDBEnableRateLimitAction,
		KVVars:                               tikvstore.NewVariables(&sqlKiller.Signal),
		SessionMemTracker:                    memory.NewTracker(memory.LabelForSession, -1),
		Location:                             time.UTC,
		SQLKiller:                            &sqlKiller,
		ErrCtx:                               errctx.NewContextWithLevels(stmtctx.DefaultStmtErrLevels, warnHandler),
		TiFlashReplicaRead:                   tiflash.GetTiFlashReplicaReadByStr(variable.DefTiFlashReplicaRead),
		TiFlashMaxThreads:                    variable.DefTiFlashMaxThreads,
		TiFlashMaxBytesBeforeExternalJoin:    variable.DefTiFlashMaxBytesBeforeExternalJoin,
		TiFlashMaxBytesBeforeExternalGroupBy: variable.DefTiFlashMaxBytesBeforeExternalGroupBy,
		TiFlashMaxBytesBeforeExternalSort:    variable.DefTiFlashMaxBytesBeforeExternalSort,
		TiFlashMaxQueryMemoryPerNode:         variable.DefTiFlashMemQuotaQueryPerNode,
		TiFlashQuerySpillRatio:               variable.DefTiFlashQuerySpillRatio,
		ExecDetails:                          &execDetails,
	}
}

// initSessCtx initializes the session context. Be careful to the timezone.
func initSessCtx(sessCtx sessionctx.Context, reorgMeta *model.DDLReorgMeta) error {
	// Correct the initial timezone.
	tz := *time.UTC
	sessCtx.GetSessionVars().TimeZone = &tz
	sessCtx.GetSessionVars().StmtCtx.SetTimeZone(&tz)

	// Set the row encode format version.
	rowFormat := variable.GetDDLReorgRowFormat()
	sessCtx.GetSessionVars().RowEncoder.Enable = rowFormat != variable.DefTiDBRowFormatV1
	// Simulate the sql mode environment in the worker sessionCtx.
	sqlMode := reorgMeta.SQLMode
	sessCtx.GetSessionVars().SQLMode = sqlMode
	loc, err := reorgTimeZoneWithTzLoc(reorgMeta.Location)
	if err != nil {
		return errors.Trace(err)
	}
	sessCtx.GetSessionVars().TimeZone = loc
	sessCtx.GetSessionVars().StmtCtx.SetTimeZone(loc)

	errLevels := reorgErrLevelsWithSQLMode(sqlMode)
	sessCtx.GetSessionVars().StmtCtx.SetErrLevels(errLevels)

	typeFlags := reorgTypeFlagsWithSQLMode(sqlMode)
	sessCtx.GetSessionVars().StmtCtx.SetTypeFlags(typeFlags)
	sessCtx.GetSessionVars().StmtCtx.ResourceGroupName = reorgMeta.ResourceGroupName

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
	typeFlags := sv.StmtCtx.TypeFlags()
	errLevels := sv.StmtCtx.ErrLevels()
	resGroupName := sv.StmtCtx.ResourceGroupName
	return func(usedSessCtx sessionctx.Context) {
		uv := usedSessCtx.GetSessionVars()
		uv.RowEncoder.Enable = rowEncoder
		uv.SQLMode = sqlMode
		uv.TimeZone = timezone
		uv.StmtCtx.SetTypeFlags(typeFlags)
		uv.StmtCtx.SetErrLevels(errLevels)
		uv.StmtCtx.ResourceGroupName = resGroupName
	}
}

func (*txnBackfillScheduler) expectedWorkerSize() (size int) {
	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	return min(workerCnt, maxBackfillWorkerSize)
}

func (b *txnBackfillScheduler) currentWorkerSize() int {
	return len(b.workers)
}

func (b *txnBackfillScheduler) adjustWorkerSize() error {
	reorgInfo := b.reorgInfo
	job := reorgInfo.Job
	jc := b.jobCtx
	if err := loadDDLReorgVars(b.ctx, b.sessPool); err != nil {
		ddllogutil.DDLLogger().Error("load DDL reorganization variable failed", zap.Error(err))
	}
	workerCnt := b.expectedWorkerSize()
	// Increase the worker.
	for i := len(b.workers); i < workerCnt; i++ {
		var (
			runner *backfillWorker
			worker backfiller
		)
		switch b.tp {
		case typeAddIndexWorker:
			backfillCtx, err := newBackfillCtx(i, reorgInfo, job.SchemaName, b.tbl, jc, "add_idx_rate", false)
			if err != nil {
				return err
			}

			idxWorker, err := newAddIndexTxnWorker(b.decodeColMap, b.tbl, backfillCtx,
				job.ID, reorgInfo.elements, reorgInfo.currElement.TypeKey)
			if err != nil {
				return err
			}
			runner = newBackfillWorker(b.ctx, idxWorker)
			worker = idxWorker
		case typeAddIndexMergeTmpWorker:
			backfillCtx, err := newBackfillCtx(i, reorgInfo, job.SchemaName, b.tbl, jc, "merge_tmp_idx_rate", false)
			if err != nil {
				return err
			}
			tmpIdxWorker := newMergeTempIndexWorker(backfillCtx, b.tbl, reorgInfo.elements)
			runner = newBackfillWorker(b.ctx, tmpIdxWorker)
			worker = tmpIdxWorker
		case typeUpdateColumnWorker:
			updateWorker, err := newUpdateColumnWorker(i, b.tbl, b.decodeColMap, reorgInfo, jc)
			if err != nil {
				return err
			}
			runner = newBackfillWorker(b.ctx, updateWorker)
			worker = updateWorker
		case typeCleanUpIndexWorker:
			idxWorker, err := newCleanUpIndexWorker(i, b.tbl, b.decodeColMap, reorgInfo, jc)
			if err != nil {
				return err
			}
			runner = newBackfillWorker(b.ctx, idxWorker)
			worker = idxWorker
		case typeReorgPartitionWorker:
			partWorker, err := newReorgPartitionWorker(i, b.tbl, b.decodeColMap, reorgInfo, jc)
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
	avgRowSize int

	closed bool

	taskCh   chan *reorgBackfillTask
	resultCh chan *backfillResult

	copReqSenderPool *copReqSenderPool

	writerPool    *workerpool.WorkerPool[IndexRecordChunk, workerpool.None]
	writerMaxID   int
	backendCtx    ingest.BackendCtx
	checkpointMgr *ingest.CheckpointManager
}

func newIngestBackfillScheduler(
	ctx context.Context,
	info *reorgInfo,
	sessPool *sess.Pool,
	tbl table.PhysicalTable,
) (*ingestBackfillScheduler, error) {
	sctx, err := sessPool.Get()
	if err != nil {
		return nil, err
	}
	defer sessPool.Put(sctx)
	avgRowSize := estimateTableRowSize(ctx, info.d.store, sctx.GetRestrictedSQLExecutor(), tbl)
	return &ingestBackfillScheduler{
		ctx:        ctx,
		reorgInfo:  info,
		sessPool:   sessPool,
		tbl:        tbl,
		avgRowSize: avgRowSize,
		taskCh:     make(chan *reorgBackfillTask, backfillTaskChanSize),
		resultCh:   make(chan *backfillResult, backfillTaskChanSize),
	}, nil
}

func (b *ingestBackfillScheduler) importStarted() bool {
	job := b.reorgInfo.Job
	bc, ok := ingest.LitBackCtxMgr.Load(job.ID)
	if !ok {
		return false
	}
	return bc.ImportStarted()
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

	indexIDs := make([]int64, 0, len(b.reorgInfo.elements))
	for _, e := range b.reorgInfo.elements {
		indexIDs = append(indexIDs, e.ID)
	}
	engines, err := b.backendCtx.Register(indexIDs, job.TableName)
	if err != nil {
		return errors.Trace(err)
	}

	b.copReqSenderPool = copReqSenderPool
	readerCnt, writerCnt := b.expectedWorkerSize()
	writerPool := workerpool.NewWorkerPool[IndexRecordChunk](
		"ingest_writer",
		poolutil.DDL,
		writerCnt,
		func() workerpool.Worker[IndexRecordChunk, workerpool.None] {
			return b.createWorker(indexIDs, engines)
		},
	)
	writerPool.Start(b.ctx)
	b.writerPool = writerPool
	b.copReqSenderPool.chunkSender = writerPool
	b.copReqSenderPool.adjustSize(readerCnt)
	logutil.Logger(b.ctx).Info("setup ingest backfill workers",
		zap.Int64("jobID", job.ID), zap.Int("reader", readerCnt), zap.Int("writer", writerCnt))
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
	if intest.InTest && b.copReqSenderPool != nil && len(b.copReqSenderPool.srcChkPool) != copReadChunkPoolSize() {
		panic(fmt.Sprintf("unexpected chunk size %d", len(b.copReqSenderPool.srcChkPool)))
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

func (b *ingestBackfillScheduler) createWorker(
	indexIDs []int64,
	engines []ingest.Engine,
) workerpool.Worker[IndexRecordChunk, workerpool.None] {
	reorgInfo := b.reorgInfo
	job := reorgInfo.Job
	worker, err := newAddIndexIngestWorker(
		b.ctx, b.tbl, reorgInfo, engines, b.resultCh, job.ID,
		indexIDs, b.writerMaxID,
		b.copReqSenderPool, b.checkpointMgr)
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
	reqSrc := getDDLRequestSource(model.ActionAddIndex)
	copCtx, err := NewReorgCopContext(ri.d.store, ri.ReorgMeta, b.tbl.Meta(), allIndexInfos, reqSrc)
	if err != nil {
		logutil.Logger(b.ctx).Warn("cannot init cop request sender", zap.Error(err))
		return nil, err
	}
	return newCopReqSenderPool(b.ctx, copCtx, ri.d.store, b.taskCh, b.sessPool, b.checkpointMgr), nil
}

func (b *ingestBackfillScheduler) expectedWorkerSize() (readerSize int, writerSize int) {
	return expectedIngestWorkerCnt(int(variable.GetDDLReorgWorkerCounter()), b.avgRowSize)
}

func expectedIngestWorkerCnt(concurrency, avgRowSize int) (readerCnt, writerCnt int) {
	workerCnt := concurrency
	if avgRowSize == 0 {
		// Statistic data not exist, use default concurrency.
		readerCnt = min(workerCnt/2, maxBackfillWorkerSize)
		readerCnt = max(readerCnt, 1)
		writerCnt = min(workerCnt/2+2, maxBackfillWorkerSize)
		return readerCnt, writerCnt
	}

	readerRatio := []float64{0.5, 1, 2, 4, 8}
	rowSize := []uint64{200, 500, 1000, 3000, math.MaxUint64}
	for i, s := range rowSize {
		if uint64(avgRowSize) <= s {
			readerCnt = max(int(float64(workerCnt)*readerRatio[i]), 1)
			writerCnt = max(workerCnt, 1)
			break
		}
	}
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
	err := w.d.isReorgRunnable(w.jobID, false)
	if err != nil {
		result.err = err
		w.sendResult(result)
		return
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
