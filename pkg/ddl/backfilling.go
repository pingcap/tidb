// Copyright 2020 PingCAP, Inc.
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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/expression"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/topsql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/tikv"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

type backfillerType byte

const (
	typeAddIndexWorker         backfillerType = 0
	typeUpdateColumnWorker     backfillerType = 1
	typeCleanUpIndexWorker     backfillerType = 2
	typeAddIndexMergeTmpWorker backfillerType = 3
	typeReorgPartitionWorker   backfillerType = 4
)

func (bT backfillerType) String() string {
	switch bT {
	case typeAddIndexWorker:
		return "add index"
	case typeUpdateColumnWorker:
		return "update column"
	case typeCleanUpIndexWorker:
		return "clean up index"
	case typeAddIndexMergeTmpWorker:
		return "merge temporary index"
	case typeReorgPartitionWorker:
		return "reorganize partition"
	default:
		return "unknown"
	}
}

// By now the DDL jobs that need backfilling include:
// 1: add-index
// 2: modify-column-type
// 3: clean-up global index
// 4: reorganize partition
//
// They all have a write reorganization state to back fill data into the rows existed.
// Backfilling is time consuming, to accelerate this process, TiDB has built some sub
// workers to do this in the DDL owner node.
//
//                       DDL owner thread (also see comments before runReorgJob func)
//                                      ^
//                                      | (reorgCtx.doneCh)
//                                      |
//                                worker master
//                                      ^ (waitTaskResults)
//                                      |
//                                      |
//                                      v (sendRangeTask)
//       +--------------------+---------+---------+------------------+--------------+
//       |                    |                   |                  |              |
// backfillworker1     backfillworker2     backfillworker3     backfillworker4     ...
//
// The worker master is responsible for scaling the backfilling workers according to the
// system variable "tidb_ddl_reorg_worker_cnt". Essentially, reorg job is mainly based
// on the [start, end] range of the table to backfill data. We did not do it all at once,
// there were several ddl rounds.
//
// [start1---end1 start2---end2 start3---end3 start4---end4 ...         ...         ]
//    |       |     |       |     |       |     |       |
//    +-------+     +-------+     +-------+     +-------+   ...         ...
//        |             |             |             |
//     bfworker1    bfworker2     bfworker3     bfworker4   ...         ...
//        |             |             |             |       |            |
//        +---------------- (round1)----------------+       +--(round2)--+
//
// The main range [start, end] will be split into small ranges.
// Each small range corresponds to a region and it will be delivered to a backfillworker.
// Each worker can only be assigned with one range at one round, those remaining ranges
// will be cached until all the backfill workers have had their previous range jobs done.
//
//                [ region start --------------------- region end ]
//                                        |
//                                        v
//                [ batch ] [ batch ] [ batch ] [ batch ] ...
//                    |         |         |         |
//                    v         v         v         v
//                (a kv txn)   ->        ->        ->
//
// For a single range, backfill worker doesn't backfill all the data in one kv transaction.
// Instead, it is divided into batches, each time a kv transaction completes the backfilling
// of a partial batch.

// backfillTaskContext is the context of the batch adding indices or updating column values.
// After finishing the batch adding indices or updating column values, result in backfillTaskContext will be merged into backfillResult.
type backfillTaskContext struct {
	nextKey       kv.Key
	done          bool
	addedCount    int
	scanCount     int
	warnings      map[errors.ErrorID]*terror.Error
	warningsCount map[errors.ErrorID]int64
	finishTS      uint64
}

type backfillCtx struct {
	id int
	*ddlCtx
	sessCtx       sessionctx.Context
	warnings      contextutil.WarnHandlerExt
	loc           *time.Location
	exprCtx       exprctx.BuildContext
	tblCtx        table.MutateContext
	schemaName    string
	table         table.Table
	batchCnt      int
	jobContext    *JobContext
	metricCounter prometheus.Counter
}

func newBackfillCtx(id int, rInfo *reorgInfo,
	schemaName string, tbl table.Table, jobCtx *JobContext, label string, isDistributed bool) (*backfillCtx, error) {
	sessCtx, err := newSessCtx(rInfo.d.store, rInfo.ReorgMeta)
	if err != nil {
		return nil, err
	}

	if isDistributed {
		id = int(backfillContextID.Add(1))
	}

	exprCtx := sessCtx.GetExprCtx()
	return &backfillCtx{
		id:         id,
		ddlCtx:     rInfo.d,
		sessCtx:    sessCtx,
		warnings:   sessCtx.GetSessionVars().StmtCtx.WarnHandler,
		exprCtx:    exprCtx,
		tblCtx:     sessCtx.GetTableCtx(),
		loc:        exprCtx.GetEvalCtx().Location(),
		schemaName: schemaName,
		table:      tbl,
		batchCnt:   int(variable.GetDDLReorgBatchSize()),
		jobContext: jobCtx,
		metricCounter: metrics.BackfillTotalCounter.WithLabelValues(
			metrics.GenerateReorgLabel(label, schemaName, tbl.Meta().Name.String())),
	}, nil
}

func updateTxnEntrySizeLimitIfNeeded(txn kv.Transaction) {
	if entrySizeLimit := variable.TxnEntrySizeLimit.Load(); entrySizeLimit > 0 {
		txn.SetOption(kv.SizeLimits, kv.TxnSizeLimits{
			Entry: entrySizeLimit,
			Total: kv.TxnTotalSizeLimit.Load(),
		})
	}
}

type backfiller interface {
	BackfillData(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, err error)
	AddMetricInfo(float64)
	GetCtx() *backfillCtx
	String() string
}

type backfillResult struct {
	taskID     int
	addedCount int
	scanCount  int
	totalCount int
	nextKey    kv.Key
	err        error
}

type reorgBackfillTask struct {
	physicalTable table.PhysicalTable

	// TODO: Remove the following fields after remove the function of run.
	id       int
	startKey kv.Key
	endKey   kv.Key
	jobID    int64
	sqlQuery string
	priority int
}

func (r *reorgBackfillTask) getJobID() int64 {
	return r.jobID
}

func (r *reorgBackfillTask) String() string {
	pID := r.physicalTable.GetPhysicalID()
	start := hex.EncodeToString(r.startKey)
	end := hex.EncodeToString(r.endKey)
	jobID := r.getJobID()
	return fmt.Sprintf("taskID: %d, physicalTableID: %d, range: [%s, %s), jobID: %d", r.id, pID, start, end, jobID)
}

// mergeBackfillCtxToResult merge partial result in taskCtx into result.
func mergeBackfillCtxToResult(taskCtx *backfillTaskContext, result *backfillResult) {
	result.nextKey = taskCtx.nextKey
	result.addedCount += taskCtx.addedCount
	result.scanCount += taskCtx.scanCount
}

type backfillWorker struct {
	backfiller
	taskCh   chan *reorgBackfillTask
	resultCh chan *backfillResult
	ctx      context.Context
	cancel   func()
	wg       *sync.WaitGroup
}

func newBackfillWorker(ctx context.Context, bf backfiller) *backfillWorker {
	bfCtx, cancel := context.WithCancel(ctx)
	return &backfillWorker{
		backfiller: bf,
		taskCh:     make(chan *reorgBackfillTask, 1),
		resultCh:   make(chan *backfillResult, 1),
		ctx:        bfCtx,
		cancel:     cancel,
	}
}

func (w *backfillWorker) String() string {
	return fmt.Sprintf("backfill-worker %d, tp %s", w.GetCtx().id, w.backfiller.String())
}

func (w *backfillWorker) Close() {
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
}

func closeBackfillWorkers(workers []*backfillWorker) {
	for _, worker := range workers {
		worker.Close()
	}
}

// ResultCounterForTest is used for test.
var ResultCounterForTest *atomic.Int32

// handleBackfillTask backfills range [task.startHandle, task.endHandle) handle's index to table.
func (w *backfillWorker) handleBackfillTask(d *ddlCtx, task *reorgBackfillTask, bf backfiller) *backfillResult {
	handleRange := *task
	result := &backfillResult{
		taskID:     task.id,
		err:        nil,
		addedCount: 0,
		nextKey:    handleRange.startKey,
	}
	lastLogCount := 0
	lastLogTime := time.Now()
	startTime := lastLogTime
	jobID := task.getJobID()
	rc := d.getReorgCtx(jobID)

	for {
		// Give job chance to be canceled or paused, if we not check it here,
		// we will never cancel the job once there is panic in bf.BackfillData.
		// Because reorgRecordTask may run a long time,
		// we should check whether this ddl job is still runnable.
		err := d.isReorgRunnable(jobID, false)
		if err != nil {
			result.err = err
			return result
		}

		taskCtx, err := bf.BackfillData(handleRange)
		if err != nil {
			result.err = err
			return result
		}

		bf.AddMetricInfo(float64(taskCtx.addedCount))
		mergeBackfillCtxToResult(&taskCtx, result)

		// Although `handleRange` is for data in one region, but back fill worker still split it into many
		// small reorg batch size slices and reorg them in many different kv txn.
		// If a task failed, it may contained some committed small kv txn which has already finished the
		// small range reorganization.
		// In the next round of reorganization, the target handle range may overlap with last committed
		// small ranges. This will cause the `redo` action in reorganization.
		// So for added count and warnings collection, it is recommended to collect the statistics in every
		// successfully committed small ranges rather than fetching it in the total result.
		rc.increaseRowCount(int64(taskCtx.addedCount))
		rc.mergeWarnings(taskCtx.warnings, taskCtx.warningsCount)

		if num := result.scanCount - lastLogCount; num >= 90000 {
			lastLogCount = result.scanCount
			logutil.DDLLogger().Info("backfill worker back fill index", zap.Stringer("worker", w),
				zap.Int("addedCount", result.addedCount), zap.Int("scanCount", result.scanCount),
				zap.String("next key", hex.EncodeToString(taskCtx.nextKey)),
				zap.Float64("speed(rows/s)", float64(num)/time.Since(lastLogTime).Seconds()))
			lastLogTime = time.Now()
		}

		handleRange.startKey = taskCtx.nextKey
		if taskCtx.done {
			break
		}
	}
	logutil.DDLLogger().Info("backfill worker finish task",
		zap.Stringer("worker", w), zap.Stringer("task", task),
		zap.Int("added count", result.addedCount),
		zap.Int("scan count", result.scanCount),
		zap.String("next key", hex.EncodeToString(result.nextKey)),
		zap.Stringer("take time", time.Since(startTime)))
	if ResultCounterForTest != nil && result.err == nil {
		ResultCounterForTest.Add(1)
	}
	return result
}

func (w *backfillWorker) sendResult(result *backfillResult) {
	select {
	case <-w.ctx.Done():
	case w.resultCh <- result:
	}
}

func (w *backfillWorker) run(d *ddlCtx, bf backfiller, job *model.Job) {
	logger := logutil.DDLLogger().With(zap.Stringer("worker", w), zap.Int64("jobID", job.ID))
	var (
		curTaskID int
		task      *reorgBackfillTask
		ok        bool
	)

	defer w.wg.Done()
	defer util.Recover(metrics.LabelDDL, "backfillWorker.run", func() {
		w.sendResult(&backfillResult{taskID: curTaskID, err: dbterror.ErrReorgPanic})
	}, false)
	for {
		select {
		case <-w.ctx.Done():
			logger.Info("backfill worker exit on context done")
			return
		case task, ok = <-w.taskCh:
		}
		if !ok {
			logger.Info("backfill worker exit")
			return
		}
		curTaskID = task.id
		d.setDDLLabelForTopSQL(job.ID, job.Query)

		logger.Debug("backfill worker got task", zap.Int("workerID", w.GetCtx().id), zap.Stringer("task", task))
		failpoint.Inject("mockBackfillRunErr", func() {
			if w.GetCtx().id == 0 {
				result := &backfillResult{taskID: task.id, addedCount: 0, nextKey: nil, err: errors.Errorf("mock backfill error")}
				w.sendResult(result)
				failpoint.Continue()
			}
		})

		failpoint.Inject("mockHighLoadForAddIndex", func() {
			sqlPrefixes := []string{"alter"}
			topsql.MockHighCPULoad(job.Query, sqlPrefixes, 5)
		})

		failpoint.Inject("mockBackfillSlow", func() {
			time.Sleep(100 * time.Millisecond)
		})

		// Change the batch size dynamically.
		w.GetCtx().batchCnt = int(variable.GetDDLReorgBatchSize())
		result := w.handleBackfillTask(d, task, bf)
		w.sendResult(result)

		if result.err != nil {
			logger.Info("backfill worker exit on error",
				zap.Error(result.err))
			return
		}
	}
}

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up backfilling data in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(
	ctx context.Context,
	t table.PhysicalTable,
	store kv.Storage,
	startKey, endKey kv.Key,
	limit int,
) ([]kv.KeyRange, error) {
	logutil.DDLLogger().Info("split table range from PD",
		zap.Int64("physicalTableID", t.GetPhysicalID()),
		zap.String("start key", hex.EncodeToString(startKey)),
		zap.String("end key", hex.EncodeToString(endKey)))
	if len(startKey) == 0 && len(endKey) == 0 {
		logutil.DDLLogger().Info("split table range from PD, get noop table range",
			zap.Int64("physicalTableID", t.GetPhysicalID()))
		return []kv.KeyRange{}, nil
	}

	kvRange := kv.KeyRange{StartKey: startKey, EndKey: endKey}
	s, ok := store.(tikv.Storage)
	if !ok {
		// Only support split ranges in tikv.Storage now.
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := backoff.NewBackofferWithVars(ctx, maxSleep, nil)
	rc := copr.NewRegionCache(s.GetRegionCache())
	ranges, err := rc.SplitRegionRanges(bo, []kv.KeyRange{kvRange}, limit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(ranges) == 0 {
		errMsg := fmt.Sprintf("cannot find region in range [%s, %s]", startKey.String(), endKey.String())
		return nil, errors.Trace(dbterror.ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg))
	}
	return ranges, nil
}

func getBatchTasks(t table.Table, reorgInfo *reorgInfo, kvRanges []kv.KeyRange,
	taskIDAlloc *taskIDAllocator) []*reorgBackfillTask {
	batchTasks := make([]*reorgBackfillTask, 0, len(kvRanges))
	var prefix kv.Key
	if reorgInfo.mergingTmpIdx {
		prefix = t.IndexPrefix()
	} else {
		prefix = t.RecordPrefix()
	}
	// Build reorg tasks.
	job := reorgInfo.Job
	//nolint:forcetypeassert
	phyTbl := t.(table.PhysicalTable)
	jobCtx := reorgInfo.NewJobContext()
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
			logutil.DDLLogger().Info("get backfill range task, get reverse key failed", zap.Error(err))
		} else {
			logutil.DDLLogger().Info("get backfill range task, change end key",
				zap.Int("id", taskID), zap.Int64("pTbl", phyTbl.GetPhysicalID()),
				zap.String("end key", hex.EncodeToString(endKey)), zap.String("current end key", hex.EncodeToString(endK)))
			endKey = endK
		}

		task := &reorgBackfillTask{
			id:            taskID,
			jobID:         reorgInfo.Job.ID,
			physicalTable: phyTbl,
			priority:      reorgInfo.Priority,
			startKey:      startKey,
			endKey:        endKey,
		}
		batchTasks = append(batchTasks, task)
	}
	return batchTasks
}

// sendTasks sends tasks to workers, and returns remaining kvRanges that is not handled.
func sendTasks(
	scheduler backfillScheduler,
	t table.PhysicalTable,
	kvRanges []kv.KeyRange,
	reorgInfo *reorgInfo,
	taskIDAlloc *taskIDAllocator,
) error {
	batchTasks := getBatchTasks(t, reorgInfo, kvRanges, taskIDAlloc)
	for _, task := range batchTasks {
		if err := scheduler.sendTask(task); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

var (
	// TestCheckWorkerNumCh use for test adjust backfill worker.
	TestCheckWorkerNumCh = make(chan *sync.WaitGroup)
	// TestCheckWorkerNumber use for test adjust backfill worker.
	TestCheckWorkerNumber = int32(variable.DefTiDBDDLReorgWorkerCount)
	// TestCheckReorgTimeout is used to mock timeout when reorg data.
	TestCheckReorgTimeout = int32(0)
)

func loadDDLReorgVars(ctx context.Context, sessPool *sess.Pool) error {
	// Get sessionctx from context resource pool.
	sCtx, err := sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer sessPool.Put(sCtx)
	return ddlutil.LoadDDLReorgVars(ctx, sCtx)
}

func makeupDecodeColMap(dbName model.CIStr, t table.Table) (map[int64]decoder.Column, error) {
	writableColInfos := make([]*model.ColumnInfo, 0, len(t.WritableCols()))
	for _, col := range t.WritableCols() {
		writableColInfos = append(writableColInfos, col.ColumnInfo)
	}
	exprCols, _, err := expression.ColumnInfos2ColumnsAndNames(newReorgExprCtx(), dbName, t.Meta().Name, writableColInfos, t.Meta())
	if err != nil {
		return nil, err
	}
	mockSchema := expression.NewSchema(exprCols...)

	decodeColMap := decoder.BuildFullDecodeColMap(t.WritableCols(), mockSchema)

	return decodeColMap, nil
}

var backfillTaskChanSize = 128

// SetBackfillTaskChanSizeForTest is only used for test.
func SetBackfillTaskChanSizeForTest(n int) {
	backfillTaskChanSize = n
}

// writePhysicalTableRecord handles the "add index" or "modify/change column" reorganization state for a non-partitioned table or a partition.
// For a partitioned table, it should be handled partition by partition.
//
// How to "add index" or "update column value" in reorganization state?
// Concurrently process the @@tidb_ddl_reorg_worker_cnt tasks. Each task deals with a handle range of the index/row record.
// The handle range is split from PD regions now. Each worker deal with a region table key range one time.
// Each handle range by estimation, concurrent processing needs to perform after the handle range has been acquired.
// The operation flow is as follows:
//  1. Open numbers of defaultWorkers goroutines.
//  2. Split table key range from PD regions.
//  3. Send tasks to running workers by workers's task channel. Each task deals with a region key ranges.
//  4. Wait all these running tasks finished, then continue to step 3, until all tasks is done.
//
// The above operations are completed in a transaction.
// Finally, update the concurrent processing of the total number of rows, and store the completed handle value.
func (dc *ddlCtx) writePhysicalTableRecord(
	ctx context.Context,
	sessPool *sess.Pool,
	t table.PhysicalTable,
	bfWorkerType backfillerType,
	reorgInfo *reorgInfo,
) error {
	startKey, endKey := reorgInfo.StartKey, reorgInfo.EndKey

	if err := dc.isReorgRunnable(reorgInfo.Job.ID, false); err != nil {
		return errors.Trace(err)
	}

	failpoint.Inject("MockCaseWhenParseFailure", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			failpoint.Return(errors.New("job.ErrCount:" + strconv.Itoa(int(reorgInfo.Job.ErrorCount)) + ", mock unknown type: ast.whenClause."))
		}
	})

	jc := reorgInfo.NewJobContext()

	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)

	scheduler, err := newBackfillScheduler(egCtx, reorgInfo, sessPool, bfWorkerType, t, jc)
	if err != nil {
		return errors.Trace(err)
	}
	defer scheduler.close(true)
	if lit, ok := scheduler.(*ingestBackfillScheduler); ok {
		if lit.importStarted() {
			return nil
		}
	}

	err = scheduler.setupWorkers()
	if err != nil {
		return errors.Trace(err)
	}

	// process result goroutine
	eg.Go(func() error {
		totalAddedCount := reorgInfo.Job.GetRowCount()
		keeper := newDoneTaskKeeper(startKey)
		cnt := 0

		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case result, ok := <-scheduler.resultChan():
				if !ok {
					logutil.DDLLogger().Info("backfill workers successfully processed",
						zap.Stringer("element", reorgInfo.currElement),
						zap.Int64("total added count", totalAddedCount),
						zap.String("start key", hex.EncodeToString(startKey)))
					return nil
				}
				cnt++

				if result.err != nil {
					logutil.DDLLogger().Warn("backfill worker failed",
						zap.Int64("job ID", reorgInfo.ID),
						zap.Int64("total added count", totalAddedCount),
						zap.String("start key", hex.EncodeToString(startKey)),
						zap.String("result next key", hex.EncodeToString(result.nextKey)),
						zap.Error(result.err))
					return result.err
				}

				if result.totalCount > 0 {
					totalAddedCount = int64(result.totalCount)
				} else {
					totalAddedCount += int64(result.addedCount)
				}
				dc.getReorgCtx(reorgInfo.Job.ID).setRowCount(totalAddedCount)

				keeper.updateNextKey(result.taskID, result.nextKey)

				if cnt%(scheduler.currentWorkerSize()*4) == 0 {
					err2 := reorgInfo.UpdateReorgMeta(keeper.nextKey, sessPool)
					if err2 != nil {
						logutil.DDLLogger().Warn("update reorg meta failed",
							zap.Int64("job ID", reorgInfo.ID),
							zap.Error(err2))
					}
					// We try to adjust the worker size regularly to reduce
					// the overhead of loading the DDL related global variables.
					err2 = scheduler.adjustWorkerSize()
					if err2 != nil {
						logutil.DDLLogger().Warn("cannot adjust backfill worker size",
							zap.Int64("job ID", reorgInfo.ID),
							zap.Error(err2))
					}
				}
			}
		}
	})

	// generate task goroutine
	eg.Go(func() error {
		// we will modify the startKey in this goroutine, so copy them to avoid race.
		start, end := startKey, endKey
		taskIDAlloc := newTaskIDAllocator()
		for {
			kvRanges, err2 := splitTableRanges(egCtx, t, reorgInfo.d.store, start, end, backfillTaskChanSize)
			if err2 != nil {
				return errors.Trace(err2)
			}
			if len(kvRanges) == 0 {
				break
			}
			logutil.DDLLogger().Info("start backfill workers to reorg record",
				zap.Stringer("type", bfWorkerType),
				zap.Int("workerCnt", scheduler.currentWorkerSize()),
				zap.Int("regionCnt", len(kvRanges)),
				zap.String("startKey", hex.EncodeToString(start)),
				zap.String("endKey", hex.EncodeToString(end)))

			err2 = sendTasks(scheduler, t, kvRanges, reorgInfo, taskIDAlloc)
			if err2 != nil {
				return errors.Trace(err2)
			}

			start = kvRanges[len(kvRanges)-1].EndKey
			if start.Cmp(end) >= 0 {
				break
			}
		}

		scheduler.close(false)
		return nil
	})

	return eg.Wait()
}

func injectCheckBackfillWorkerNum(curWorkerSize int, isMergeWorker bool) error {
	if isMergeWorker {
		return nil
	}
	failpoint.Inject("checkBackfillWorkerNum", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			num := int(atomic.LoadInt32(&TestCheckWorkerNumber))
			if num != 0 {
				if num != curWorkerSize {
					failpoint.Return(errors.Errorf("expected backfill worker num: %v, actual record num: %v", num, curWorkerSize))
				}
				var wg sync.WaitGroup
				wg.Add(1)
				TestCheckWorkerNumCh <- &wg
				wg.Wait()
			}
		}
	})
	return nil
}

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(h kv.Handle, rowKey kv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotKeys(ctx *JobContext, store kv.Storage, priority int, keyPrefix kv.Key, version uint64,
	startKey kv.Key, endKey kv.Key, fn recordIterFunc) error {
	isRecord := tablecodec.IsRecordKey(keyPrefix.Next())
	var firstKey kv.Key
	if startKey == nil {
		firstKey = keyPrefix
	} else {
		firstKey = startKey
	}

	var upperBound kv.Key
	if endKey == nil {
		upperBound = keyPrefix.PrefixNext()
	} else {
		upperBound = endKey.PrefixNext()
	}

	ver := kv.Version{Ver: version}
	snap := store.GetSnapshot(ver)
	snap.SetOption(kv.Priority, priority)
	snap.SetOption(kv.RequestSourceInternal, true)
	snap.SetOption(kv.RequestSourceType, ctx.ddlJobSourceType())
	snap.SetOption(kv.ExplicitRequestSourceType, kvutil.ExplicitTypeDDL)
	if tagger := ctx.getResourceGroupTaggerForTopSQL(); tagger != nil {
		snap.SetOption(kv.ResourceGroupTagger, tagger)
	}
	snap.SetOption(kv.ResourceGroupName, ctx.resourceGroupName)

	it, err := snap.Iter(firstKey, upperBound)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	for it.Valid() {
		if !it.Key().HasPrefix(keyPrefix) {
			break
		}

		var handle kv.Handle
		if isRecord {
			handle, err = tablecodec.DecodeRowKey(it.Key())
			if err != nil {
				return errors.Trace(err)
			}
		}

		more, err := fn(handle, it.Key(), it.Value())
		if !more || err != nil {
			return errors.Trace(err)
		}

		err = kv.NextUntil(it, util.RowKeyPrefixFilter(it.Key()))
		if err != nil {
			if kv.ErrNotExist.Equal(err) {
				break
			}
			return errors.Trace(err)
		}
	}

	return nil
}

// GetRangeEndKey gets the actual end key for the range of [startKey, endKey).
func GetRangeEndKey(ctx *JobContext, store kv.Storage, priority int, keyPrefix kv.Key, startKey, endKey kv.Key) (kv.Key, error) {
	snap := store.GetSnapshot(kv.MaxVersion)
	snap.SetOption(kv.Priority, priority)
	if tagger := ctx.getResourceGroupTaggerForTopSQL(); tagger != nil {
		snap.SetOption(kv.ResourceGroupTagger, tagger)
	}
	snap.SetOption(kv.ResourceGroupName, ctx.resourceGroupName)
	snap.SetOption(kv.RequestSourceInternal, true)
	snap.SetOption(kv.RequestSourceType, ctx.ddlJobSourceType())
	snap.SetOption(kv.ExplicitRequestSourceType, kvutil.ExplicitTypeDDL)
	it, err := snap.IterReverse(endKey, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer it.Close()

	if !it.Valid() || !it.Key().HasPrefix(keyPrefix) {
		return startKey.Next(), nil
	}
	if it.Key().Cmp(startKey) < 0 {
		return startKey.Next(), nil
	}

	return it.Key().Next(), nil
}

func mergeWarningsAndWarningsCount(partWarnings, totalWarnings map[errors.ErrorID]*terror.Error, partWarningsCount, totalWarningsCount map[errors.ErrorID]int64) (map[errors.ErrorID]*terror.Error, map[errors.ErrorID]int64) {
	for _, warn := range partWarnings {
		if _, ok := totalWarningsCount[warn.ID()]; ok {
			totalWarningsCount[warn.ID()] += partWarningsCount[warn.ID()]
		} else {
			totalWarningsCount[warn.ID()] = partWarningsCount[warn.ID()]
			totalWarnings[warn.ID()] = warn
		}
	}
	return totalWarnings, totalWarningsCount
}

func logSlowOperations(elapsed time.Duration, slowMsg string, threshold uint32) {
	if threshold == 0 {
		threshold = atomic.LoadUint32(&variable.DDLSlowOprThreshold)
	}

	if elapsed >= time.Duration(threshold)*time.Millisecond {
		logutil.DDLLogger().Info("slow operations",
			zap.Duration("takeTimes", elapsed),
			zap.String("msg", slowMsg))
	}
}

// doneTaskKeeper keeps the done tasks and update the latest next key.
type doneTaskKeeper struct {
	doneTaskNextKey map[int]kv.Key
	current         int
	nextKey         kv.Key
}

func newDoneTaskKeeper(start kv.Key) *doneTaskKeeper {
	return &doneTaskKeeper{
		doneTaskNextKey: make(map[int]kv.Key),
		current:         0,
		nextKey:         start,
	}
}

func (n *doneTaskKeeper) updateNextKey(doneTaskID int, next kv.Key) {
	if doneTaskID == n.current {
		n.current++
		n.nextKey = next
		for {
			nKey, ok := n.doneTaskNextKey[n.current]
			if !ok {
				break
			}
			delete(n.doneTaskNextKey, n.current)
			n.current++
			n.nextKey = nKey
		}
		return
	}
	n.doneTaskNextKey[doneTaskID] = next
}
