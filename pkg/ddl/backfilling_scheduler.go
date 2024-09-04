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
	"math"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	ddllogutil "github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/execdetails"
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
)

const maxBackfillWorkerSize = 16

type txnBackfillScheduler struct {
	ctx          context.Context
	reorgInfo    *reorgInfo
	sessPool     *sess.Pool
	tp           backfillerType
	tbl          table.PhysicalTable
	decodeColMap map[int64]decoder.Column
	jobCtx       *ReorgContext

	workers []*backfillWorker
	wg      sync.WaitGroup

	taskCh   chan *reorgBackfillTask
	resultCh chan *backfillResult
	closed   bool
}

func newTxnBackfillScheduler(ctx context.Context, info *reorgInfo, sessPool *sess.Pool,
	tp backfillerType, tbl table.PhysicalTable,
	jobCtx *ReorgContext) (backfillScheduler, error) {
	decColMap, err := makeupDecodeColMap(info.dbInfo.Name, tbl)
	if err != nil {
		return nil, err
	}
	workerCnt := info.ReorgMeta.GetConcurrencyOrDefault(int(variable.GetDDLReorgWorkerCounter()))
	return &txnBackfillScheduler{
		ctx:          ctx,
		reorgInfo:    info,
		sessPool:     sessPool,
		tp:           tp,
		tbl:          tbl,
		decodeColMap: decColMap,
		jobCtx:       jobCtx,
		workers:      make([]*backfillWorker, 0, workerCnt),
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

func (b *txnBackfillScheduler) expectedWorkerSize() (size int) {
	workerCnt := b.reorgInfo.ReorgMeta.GetConcurrencyOrDefault(int(variable.GetDDLReorgWorkerCounter()))
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
		go runner.run(reorgInfo.jobCtx.oldDDLCtx, worker, job)
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
