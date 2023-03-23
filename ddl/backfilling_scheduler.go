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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"go.uber.org/zap"
)

// backfillScheduler is used to manage the lifetime of backfill workers.
type backfillScheduler struct {
	ctx          context.Context
	reorgInfo    *reorgInfo
	sessPool     *sessionPool
	tp           backfillerType
	tbl          table.PhysicalTable
	decodeColMap map[int64]decoder.Column
	jobCtx       *JobContext

	workers []*backfillWorker
	maxSize int

	taskCh   chan *reorgBackfillTask
	resultCh chan *backfillResult

	copReqSenderPool *copReqSenderPool // for add index in ingest way.
}

func newBackfillScheduler(ctx context.Context, info *reorgInfo, sessPool *sessionPool,
	tp backfillerType, tbl table.PhysicalTable, decColMap map[int64]decoder.Column,
	jobCtx *JobContext) *backfillScheduler {
	return &backfillScheduler{
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
	}
}

func (b *backfillScheduler) newSessCtx() (sessionctx.Context, error) {
	reorgInfo := b.reorgInfo
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

func (b *backfillScheduler) setMaxWorkerSize(maxSize int) {
	b.maxSize = maxSize
}

func (b *backfillScheduler) expectedWorkerSize() (readerSize int, writerSize int) {
	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	if b.tp == typeAddIndexWorker && b.reorgInfo.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
		readerSize = mathutil.Min(workerCnt/2, b.maxSize)
		readerSize = mathutil.Max(readerSize, 1)
		writerSize = mathutil.Min(workerCnt/2+2, b.maxSize)
		return readerSize, writerSize
	}
	workerCnt = mathutil.Min(workerCnt, b.maxSize)
	return workerCnt, workerCnt
}

func (b *backfillScheduler) workerSize() int {
	return len(b.workers)
}

func (b *backfillScheduler) adjustWorkerSize() error {
	b.initCopReqSenderPool()
	reorgInfo := b.reorgInfo
	job := reorgInfo.Job
	jc := b.jobCtx
	if err := loadDDLReorgVars(b.ctx, b.sessPool); err != nil {
		logutil.BgLogger().Error("[ddl] load DDL reorganization variable failed", zap.Error(err))
	}
	readerCnt, writerCnt := b.expectedWorkerSize()
	// Increase the worker.
	for i := len(b.workers); i < writerCnt; i++ {
		sessCtx, err := b.newSessCtx()
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
			if reorgInfo.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
				idxWorker, err := newAddIndexIngestWorker(b.tbl, backfillCtx,
					job.ID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey)
				if err != nil {
					if canSkipError(b.reorgInfo.ID, len(b.workers), err) {
						continue
					}
					return err
				}
				idxWorker.copReqSenderPool = b.copReqSenderPool
				runner = newBackfillWorker(jc.ddlJobCtx, idxWorker)
				worker = idxWorker
			} else {
				idxWorker, err := newAddIndexTxnWorker(b.decodeColMap, b.tbl, backfillCtx,
					job.ID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey)
				if err != nil {
					return err
				}
				runner = newBackfillWorker(jc.ddlJobCtx, idxWorker)
				worker = idxWorker
			}
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
		b.workers = append(b.workers, runner)
		go runner.run(reorgInfo.d, worker, job)
	}
	// Decrease the worker.
	if len(b.workers) > writerCnt {
		workers := b.workers[writerCnt:]
		b.workers = b.workers[:writerCnt]
		closeBackfillWorkers(workers)
	}
	if b.copReqSenderPool != nil {
		b.copReqSenderPool.adjustSize(readerCnt)
	}
	return injectCheckBackfillWorkerNum(len(b.workers), b.tp == typeAddIndexMergeTmpWorker)
}

func (b *backfillScheduler) initCopReqSenderPool() {
	if b.tp != typeAddIndexWorker || b.reorgInfo.ReorgMeta.ReorgTp != model.ReorgTypeLitMerge ||
		b.copReqSenderPool != nil || len(b.workers) > 0 {
		return
	}
	indexInfo := model.FindIndexInfoByID(b.tbl.Meta().Indices, b.reorgInfo.currElement.ID)
	if indexInfo == nil {
		logutil.BgLogger().Warn("[ddl-ingest] cannot init cop request sender",
			zap.Int64("table ID", b.tbl.Meta().ID), zap.Int64("index ID", b.reorgInfo.currElement.ID))
		return
	}
	sessCtx, err := b.newSessCtx()
	if err != nil {
		logutil.BgLogger().Warn("[ddl-ingest] cannot init cop request sender", zap.Error(err))
		return
	}
	copCtx, err := newCopContext(b.tbl.Meta(), indexInfo, sessCtx)
	if err != nil {
		logutil.BgLogger().Warn("[ddl-ingest] cannot init cop request sender", zap.Error(err))
		return
	}
	b.copReqSenderPool = newCopReqSenderPool(b.ctx, copCtx, sessCtx.GetStore())
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

func (b *backfillScheduler) Close() {
	if b.copReqSenderPool != nil {
		b.copReqSenderPool.close()
	}
	closeBackfillWorkers(b.workers)
	close(b.taskCh)
	close(b.resultCh)
}
