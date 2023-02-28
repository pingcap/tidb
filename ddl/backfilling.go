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
	"github.com/pingcap/tidb/ddl/ingest"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/driver/backoff"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tidb/util/topsql"
	"github.com/tikv/client-go/v2/tikv"
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
	reorgTp    model.ReorgType
	sessCtx    sessionctx.Context
	schemaName string
	table      table.Table
	batchCnt   int
}

func newBackfillCtx(ctx *ddlCtx, id int, sessCtx sessionctx.Context, reorgTp model.ReorgType,
	schemaName string, tbl table.Table, isDistributed bool) *backfillCtx {
	if isDistributed {
		id = int(backfillContextID.Add(1))
	}
	return &backfillCtx{
		id:         id,
		ddlCtx:     ctx,
		sessCtx:    sessCtx,
		reorgTp:    reorgTp,
		schemaName: schemaName,
		table:      tbl,
		batchCnt:   int(variable.GetDDLReorgBatchSize()),
	}
}

type backfiller interface {
	BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error)
	AddMetricInfo(float64)
	GetTasks() ([]*BackfillJob, error)
	UpdateTask(bfJob *BackfillJob) error
	FinishTask(bfJob *BackfillJob) error
	GetCtx() *backfillCtx
	String() string
}

type backfillResult struct {
	taskID     int
	addedCount int
	scanCount  int
	nextKey    kv.Key
	err        error
}

type reorgBackfillTask struct {
	bfJob         *BackfillJob
	physicalTable table.PhysicalTable

	// TODO: Remove the following fields after remove the function of run.
	id         int
	startKey   kv.Key
	endKey     kv.Key
	endInclude bool
	jobID      int64
	sqlQuery   string
	priority   int
}

func (r *reorgBackfillTask) getJobID() int64 {
	jobID := r.jobID
	if r.bfJob != nil {
		jobID = r.bfJob.JobID
	}
	return jobID
}

func (r *reorgBackfillTask) excludedEndKey() kv.Key {
	if r.endInclude {
		return r.endKey.Next()
	}
	return r.endKey
}

func (r *reorgBackfillTask) String() string {
	pID := r.physicalTable.GetPhysicalID()
	start := hex.EncodeToString(r.startKey)
	end := hex.EncodeToString(r.endKey)
	inclusion := ")"
	jobID := r.getJobID()
	if r.endInclude {
		inclusion = "]"
	}
	return fmt.Sprintf("taskID: %d, physicalTableID: %d, range: [%s, %s%s, jobID: %d", r.id, pID, start, end, inclusion, jobID)
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

func (w *backfillWorker) updateLease(execID string, bfJob *BackfillJob, nextKey kv.Key) error {
	leaseTime, err := GetOracleTime(w.GetCtx().store)
	if err != nil {
		return err
	}
	bfJob.Meta.CurrKey = nextKey
	bfJob.InstanceID = execID
	bfJob.InstanceLease = GetLeaseGoTime(leaseTime, InstanceLease)
	return w.backfiller.UpdateTask(bfJob)
}

func (w *backfillWorker) finishJob(bfJob *BackfillJob) error {
	return w.backfiller.FinishTask(bfJob)
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
	batchStartTime := time.Now()
	lastLogCount := 0
	lastLogTime := time.Now()
	startTime := lastLogTime
	jobID := task.getJobID()
	rc := d.getReorgCtx(jobID)

	isDistReorg := task.bfJob != nil
	if isDistReorg {
		w.initPartitionIndexInfo(task)
	}
	for {
		// Give job chance to be canceled, if we not check it here,
		// if there is panic in bf.BackfillDataInTxn we will never cancel the job.
		// Because reorgRecordTask may run a long time,
		// we should check whether this ddl job is still runnable.
		err := d.isReorgRunnable(jobID, isDistReorg)
		if err != nil {
			result.err = err
			return result
		}

		taskCtx, err := bf.BackfillDataInTxn(handleRange)
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
			logutil.BgLogger().Info("[ddl] backfill worker back fill index", zap.Stringer("worker", w),
				zap.Int("addedCount", result.addedCount), zap.Int("scanCount", result.scanCount),
				zap.String("next key", hex.EncodeToString(taskCtx.nextKey)),
				zap.Float64("speed(rows/s)", float64(num)/time.Since(lastLogTime).Seconds()))
			lastLogTime = time.Now()
		}

		handleRange.startKey = taskCtx.nextKey
		if taskCtx.done {
			break
		}

		if isDistReorg {
			// TODO: Adjust the updating lease frequency by batch processing time carefully.
			if time.Since(batchStartTime) < updateInstanceLease {
				continue
			}
			batchStartTime = time.Now()
			if err := w.updateLease(w.GetCtx().uuid, task.bfJob, result.nextKey); err != nil {
				logutil.BgLogger().Info("[ddl] backfill worker handle task, update lease failed", zap.Stringer("worker", w),
					zap.Stringer("task", task), zap.String("backfill job", task.bfJob.AbbrStr()), zap.Error(err))
				result.err = err
				return result
			}
		}
	}
	logutil.BgLogger().Info("[ddl] backfill worker finish task",
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

func (w *backfillWorker) initPartitionIndexInfo(task *reorgBackfillTask) {
	if pt, ok := w.GetCtx().table.(table.PartitionedTable); ok {
		if addIdxWorker, ok := w.backfiller.(*addIndexWorker); ok {
			indexInfo := model.FindIndexInfoByID(pt.Meta().Indices, task.bfJob.EleID)
			addIdxWorker.index = tables.NewIndex(task.bfJob.PhysicalTableID, pt.Meta(), indexInfo)
		}
	}
}

func (w *backfillWorker) runTask(task *reorgBackfillTask) (result *backfillResult) {
	logutil.BgLogger().Info("[ddl] backfill worker start", zap.Stringer("worker", w), zap.String("task", task.String()))
	defer util.Recover(metrics.LabelDDL, "backfillWorker.runTask", func() {
		result = &backfillResult{taskID: task.id, err: dbterror.ErrReorgPanic}
	}, false)
	defer w.GetCtx().setDDLLabelForTopSQL(task.jobID, task.sqlQuery)

	failpoint.Inject("mockBackfillRunErr", func() {
		if w.GetCtx().id == 0 {
			result := &backfillResult{taskID: task.id, addedCount: 0, nextKey: nil, err: errors.Errorf("mock backfill error")}
			failpoint.Return(result)
		}
	})
	failpoint.Inject("mockHighLoadForAddIndex", func() {
		sqlPrefixes := []string{"alter"}
		topsql.MockHighCPULoad(task.sqlQuery, sqlPrefixes, 5)
	})
	failpoint.Inject("mockBackfillSlow", func() {
		time.Sleep(100 * time.Millisecond)
	})

	// Change the batch size dynamically.
	w.GetCtx().batchCnt = int(variable.GetDDLReorgBatchSize())
	result = w.handleBackfillTask(w.GetCtx().ddlCtx, task, w.backfiller)
	task.bfJob.Meta.RowCount = int64(result.addedCount)
	if result.err != nil {
		logutil.BgLogger().Warn("[ddl] backfill worker runTask failed",
			zap.Stringer("worker", w), zap.String("backfillJob", task.bfJob.AbbrStr()), zap.Error(result.err))
		if dbterror.ErrDDLJobNotFound.Equal(result.err) {
			result.err = nil
			return result
		}
		task.bfJob.State = model.JobStateCancelled
		task.bfJob.Meta.Error = toTError(result.err)
		if err := w.finishJob(task.bfJob); err != nil {
			logutil.BgLogger().Info("[ddl] backfill worker runTask, finishJob failed",
				zap.Stringer("worker", w), zap.String("backfillJob", task.bfJob.AbbrStr()), zap.Error(err))
			result.err = err
		}
	} else {
		task.bfJob.State = model.JobStateDone
		result.err = w.finishJob(task.bfJob)
	}
	return result
}

func (w *backfillWorker) run(d *ddlCtx, bf backfiller, job *model.Job) {
	logutil.BgLogger().Info("[ddl] backfill worker start", zap.Stringer("worker", w))
	var curTaskID int
	defer util.Recover(metrics.LabelDDL, "backfillWorker.run", func() {
		w.resultCh <- &backfillResult{taskID: curTaskID, err: dbterror.ErrReorgPanic}
	}, false)
	for {
		if util.HasCancelled(w.ctx) {
			logutil.BgLogger().Info("[ddl] backfill worker exit on context done", zap.Stringer("worker", w))
			return
		}
		task, more := <-w.taskCh
		if !more {
			logutil.BgLogger().Info("[ddl] backfill worker exit", zap.Stringer("worker", w))
			return
		}
		curTaskID = task.id
		d.setDDLLabelForTopSQL(job.ID, job.Query)

		logutil.BgLogger().Debug("[ddl] backfill worker got task", zap.Int("workerID", w.GetCtx().id), zap.String("task", task.String()))
		failpoint.Inject("mockBackfillRunErr", func() {
			if w.GetCtx().id == 0 {
				result := &backfillResult{taskID: task.id, addedCount: 0, nextKey: nil, err: errors.Errorf("mock backfill error")}
				w.resultCh <- result
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
		w.resultCh <- result
		if result.err != nil {
			logutil.BgLogger().Info("[ddl] backfill worker exit on error",
				zap.Stringer("worker", w), zap.Error(result.err))
			return
		}
	}
}

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up backfilling data in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(t table.PhysicalTable, store kv.Storage, startKey, endKey kv.Key, limit int) ([]kv.KeyRange, error) {
	logutil.BgLogger().Info("[ddl] split table range from PD",
		zap.Int64("physicalTableID", t.GetPhysicalID()),
		zap.String("start key", hex.EncodeToString(startKey)),
		zap.String("end key", hex.EncodeToString(endKey)))
	kvRange := kv.KeyRange{StartKey: startKey, EndKey: endKey}
	s, ok := store.(tikv.Storage)
	if !ok {
		// Only support split ranges in tikv.Storage now.
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := backoff.NewBackofferWithVars(context.Background(), maxSleep, nil)
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

func waitTaskResults(scheduler *backfillScheduler, batchTasks []*reorgBackfillTask,
	totalAddedCount *int64) (kv.Key, int64, error) {
	var (
		firstErr   error
		addedCount int64
	)
	keeper := newDoneTaskKeeper(batchTasks[0].startKey)
	taskSize := len(batchTasks)
	for i := 0; i < taskSize; i++ {
		result := <-scheduler.resultCh
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			logutil.BgLogger().Warn("[ddl] backfill worker failed",
				zap.String("result next key", hex.EncodeToString(result.nextKey)),
				zap.Error(result.err))
			// Drain tasks.
			cnt := drainTasks(scheduler.taskCh)
			// We need to wait all the tasks to finish before closing it
			// to prevent send on closed channel error.
			taskSize -= cnt
			continue
		}
		*totalAddedCount += int64(result.addedCount)
		addedCount += int64(result.addedCount)
		keeper.updateNextKey(result.taskID, result.nextKey)
		if i%scheduler.workerSize()*4 == 0 {
			// We try to adjust the worker size regularly to reduce
			// the overhead of loading the DDL related global variables.
			err := scheduler.adjustWorkerSize()
			if err != nil {
				logutil.BgLogger().Warn("[ddl] cannot adjust backfill worker size", zap.Error(err))
			}
		}
	}
	return keeper.nextKey, addedCount, errors.Trace(firstErr)
}

func drainTasks(taskCh chan *reorgBackfillTask) int {
	cnt := 0
	for len(taskCh) > 0 {
		<-taskCh
		cnt++
	}
	return cnt
}

// sendTasksAndWait sends tasks to workers, and waits for all the running workers to return results,
// there are taskCnt running workers.
func (dc *ddlCtx) sendTasksAndWait(scheduler *backfillScheduler, totalAddedCount *int64,
	batchTasks []*reorgBackfillTask) error {
	reorgInfo := scheduler.reorgInfo
	for _, task := range batchTasks {
		if scheduler.copReqSenderPool != nil {
			scheduler.copReqSenderPool.sendTask(task)
		}
		scheduler.taskCh <- task
	}

	startKey := batchTasks[0].startKey
	startTime := time.Now()
	nextKey, taskAddedCount, err := waitTaskResults(scheduler, batchTasks, totalAddedCount)
	elapsedTime := time.Since(startTime)
	if err == nil {
		err = dc.isReorgRunnable(reorgInfo.Job.ID, false)
	}

	// Update the reorg handle that has been processed.
	err1 := reorgInfo.UpdateReorgMeta(nextKey, scheduler.sessPool)

	if err != nil {
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblError).Observe(elapsedTime.Seconds())
		logutil.BgLogger().Warn("[ddl] backfill worker handle batch tasks failed",

			zap.Int64("total added count", *totalAddedCount),
			zap.String("start key", hex.EncodeToString(startKey)),
			zap.String("next key", hex.EncodeToString(nextKey)),
			zap.Int64("batch added count", taskAddedCount),
			zap.String("task failed error", err.Error()),
			zap.String("take time", elapsedTime.String()),
			zap.NamedError("updateHandleError", err1))
		failpoint.Inject("MockGetIndexRecordErr", func() {
			// Make sure this job didn't failed because by the "Write conflict" error.
			if dbterror.ErrNotOwner.Equal(err) {
				time.Sleep(50 * time.Millisecond)
			}
		})
		return errors.Trace(err)
	}

	dc.getReorgCtx(reorgInfo.Job.ID).setNextKey(nextKey)
	metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblOK).Observe(elapsedTime.Seconds())
	logutil.BgLogger().Info("[ddl] backfill workers successfully processed batch",
		zap.Stringer("element", reorgInfo.currElement),
		zap.Int64("total added count", *totalAddedCount),
		zap.String("start key", hex.EncodeToString(startKey)),
		zap.String("next key", hex.EncodeToString(nextKey)),
		zap.Int64("batch added count", taskAddedCount),
		zap.String("take time", elapsedTime.String()),
		zap.NamedError("updateHandleError", err1))
	return nil
}

func getBatchTasks(t table.Table, reorgInfo *reorgInfo, kvRanges []kv.KeyRange, batch int) []*reorgBackfillTask {
	batchTasks := make([]*reorgBackfillTask, 0, batch)
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
	jobCtx := reorgInfo.d.jobContext(job.ID)
	for i, keyRange := range kvRanges {
		startKey := keyRange.StartKey
		endKey := keyRange.EndKey
		endK, err := getRangeEndKey(jobCtx, reorgInfo.d.store, job.Priority, prefix, keyRange.StartKey, endKey)
		if err != nil {
			logutil.BgLogger().Info("[ddl] get backfill range task, get reverse key failed", zap.Error(err))
		} else {
			logutil.BgLogger().Info("[ddl] get backfill range task, change end key", zap.Int64("pTbl", phyTbl.GetPhysicalID()),
				zap.String("end key", hex.EncodeToString(endKey)), zap.String("current end key", hex.EncodeToString(endK)))
			endKey = endK
		}
		if len(startKey) == 0 {
			startKey = prefix
		}
		if len(endKey) == 0 {
			endKey = prefix.PrefixNext()
		}

		task := &reorgBackfillTask{
			id:            i,
			jobID:         reorgInfo.Job.ID,
			physicalTable: phyTbl,
			priority:      reorgInfo.Priority,
			startKey:      startKey,
			endKey:        endKey,
			// If the boundaries overlap, we should ignore the preceding endKey.
			endInclude: endK.Cmp(keyRange.EndKey) != 0 || i == len(kvRanges)-1}
		batchTasks = append(batchTasks, task)

		if len(batchTasks) >= batch {
			break
		}
	}
	return batchTasks
}

// handleRangeTasks sends tasks to workers, and returns remaining kvRanges that is not handled.
func (dc *ddlCtx) handleRangeTasks(scheduler *backfillScheduler, t table.PhysicalTable,
	totalAddedCount *int64, kvRanges []kv.KeyRange) ([]kv.KeyRange, error) {
	batchTasks := getBatchTasks(t, scheduler.reorgInfo, kvRanges, backfillTaskChanSize)
	if len(batchTasks) == 0 {
		return nil, nil
	}

	// Wait tasks finish.
	err := dc.sendTasksAndWait(scheduler, totalAddedCount, batchTasks)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(batchTasks) < len(kvRanges) {
		// There are kvRanges not handled.
		remains := kvRanges[len(batchTasks):]
		return remains, nil
	}

	return nil, nil
}

var (
	// TestCheckWorkerNumCh use for test adjust backfill worker.
	TestCheckWorkerNumCh = make(chan *sync.WaitGroup)
	// TestCheckWorkerNumber use for test adjust backfill worker.
	TestCheckWorkerNumber = int32(1)
	// TestCheckReorgTimeout is used to mock timeout when reorg data.
	TestCheckReorgTimeout = int32(0)
)

func loadDDLReorgVars(ctx context.Context, sessPool *sessionPool) error {
	// Get sessionctx from context resource pool.
	sCtx, err := sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer sessPool.put(sCtx)
	return ddlutil.LoadDDLReorgVars(ctx, sCtx)
}

func makeupDecodeColMap(sessCtx sessionctx.Context, dbName model.CIStr, t table.Table) (map[int64]decoder.Column, error) {
	writableColInfos := make([]*model.ColumnInfo, 0, len(t.WritableCols()))
	for _, col := range t.WritableCols() {
		writableColInfos = append(writableColInfos, col.ColumnInfo)
	}
	exprCols, _, err := expression.ColumnInfos2ColumnsAndNames(sessCtx, dbName, t.Meta().Name, writableColInfos, t.Meta())
	if err != nil {
		return nil, err
	}
	mockSchema := expression.NewSchema(exprCols...)

	decodeColMap := decoder.BuildFullDecodeColMap(t.WritableCols(), mockSchema)

	return decodeColMap, nil
}

func setSessCtxLocation(sctx sessionctx.Context, tzLocation *model.TimeZoneLocation) error {
	// It is set to SystemLocation to be compatible with nil LocationInfo.
	tz := *timeutil.SystemLocation()
	if sctx.GetSessionVars().TimeZone == nil {
		sctx.GetSessionVars().TimeZone = &tz
	} else {
		*sctx.GetSessionVars().TimeZone = tz
	}
	if tzLocation != nil {
		loc, err := tzLocation.GetLocation()
		if err != nil {
			return errors.Trace(err)
		}
		*sctx.GetSessionVars().TimeZone = *loc
	}
	return nil
}

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

var backfillTaskChanSize = 1024

// SetBackfillTaskChanSizeForTest is only used for test.
func SetBackfillTaskChanSizeForTest(n int) {
	backfillTaskChanSize = n
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
	workerCnt := int(variable.GetDDLReorgWorkerCounter())
	if b.copReqSenderPool != nil {
		workerCnt = mathutil.Min(workerCnt/2+1, b.maxSize)
	} else {
		workerCnt = mathutil.Min(workerCnt, b.maxSize)
	}
	// Increase the worker.
	for i := len(b.workers); i < workerCnt; i++ {
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
			backfillCtx := newBackfillCtx(reorgInfo.d, i, sessCtx, reorgInfo.ReorgMeta.ReorgTp, job.SchemaName, b.tbl, false)
			idxWorker, err := newAddIndexWorker(b.decodeColMap, b.tbl, backfillCtx,
				jc, job.ID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey)
			if err != nil {
				if canSkipError(b.reorgInfo.ID, len(b.workers), err) {
					continue
				}
				return err
			}
			idxWorker.copReqSenderPool = b.copReqSenderPool
			runner = newBackfillWorker(jc.ddlJobCtx, idxWorker)
			worker = idxWorker
		case typeAddIndexMergeTmpWorker:
			backfillCtx := newBackfillCtx(reorgInfo.d, i, sessCtx, reorgInfo.ReorgMeta.ReorgTp, job.SchemaName, b.tbl, false)
			tmpIdxWorker := newMergeTempIndexWorker(backfillCtx, i, b.tbl, reorgInfo.currElement.ID, jc)
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
	if len(b.workers) > workerCnt {
		workers := b.workers[workerCnt:]
		b.workers = b.workers[:workerCnt]
		closeBackfillWorkers(workers)
	}
	if b.copReqSenderPool != nil {
		b.copReqSenderPool.adjustSize(len(b.workers))
	}
	return injectCheckBackfillWorkerNum(len(b.workers), b.tp == typeAddIndexMergeTmpWorker)
}

func (b *backfillScheduler) initCopReqSenderPool() {
	if b.tp != typeAddIndexWorker || b.reorgInfo.Job.ReorgMeta.ReorgTp != model.ReorgTypeLitMerge ||
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
func (dc *ddlCtx) writePhysicalTableRecord(sessPool *sessionPool, t table.PhysicalTable, bfWorkerType backfillerType, reorgInfo *reorgInfo) error {
	job := reorgInfo.Job
	totalAddedCount := job.GetRowCount()

	startKey, endKey := reorgInfo.StartKey, reorgInfo.EndKey
	sessCtx := newContext(reorgInfo.d.store)
	decodeColMap, err := makeupDecodeColMap(sessCtx, reorgInfo.dbInfo.Name, t)
	if err != nil {
		return errors.Trace(err)
	}

	if err := dc.isReorgRunnable(reorgInfo.Job.ID, false); err != nil {
		return errors.Trace(err)
	}
	if startKey == nil && endKey == nil {
		return nil
	}

	failpoint.Inject("MockCaseWhenParseFailure", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			failpoint.Return(errors.New("job.ErrCount:" + strconv.Itoa(int(job.ErrorCount)) + ", mock unknown type: ast.whenClause."))
		}
	})

	jc := dc.jobContext(job.ID)
	scheduler := newBackfillScheduler(dc.ctx, reorgInfo, sessPool, bfWorkerType, t, decodeColMap, jc)
	defer scheduler.Close()

	var ingestBeCtx *ingest.BackendContext
	if bfWorkerType == typeAddIndexWorker && job.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
		if bc, ok := ingest.LitBackCtxMgr.Load(job.ID); ok {
			ingestBeCtx = bc
		} else {
			return errors.New(ingest.LitErrGetBackendFail)
		}
	}

	for {
		kvRanges, err := splitTableRanges(t, reorgInfo.d.store, startKey, endKey, backfillTaskChanSize)
		if err != nil {
			return errors.Trace(err)
		}
		if len(kvRanges) == 0 {
			break
		}

		scheduler.setMaxWorkerSize(len(kvRanges))
		err = scheduler.adjustWorkerSize()
		if err != nil {
			return errors.Trace(err)
		}

		logutil.BgLogger().Info("[ddl] start backfill workers to reorg record",
			zap.Stringer("type", bfWorkerType),
			zap.Int("workerCnt", scheduler.workerSize()),
			zap.Int("regionCnt", len(kvRanges)),
			zap.String("startKey", hex.EncodeToString(startKey)),
			zap.String("endKey", hex.EncodeToString(endKey)))

		if ingestBeCtx != nil {
			err := ingestBeCtx.Flush(reorgInfo.currElement.ID)
			if err != nil {
				return errors.Trace(err)
			}
		}
		remains, err := dc.handleRangeTasks(scheduler, t, &totalAddedCount, kvRanges)
		if err != nil {
			return errors.Trace(err)
		}
		if len(remains) > 0 {
			startKey = remains[0].StartKey
		} else {
			rangeEndKey := kvRanges[len(kvRanges)-1].EndKey
			startKey = rangeEndKey.Next()
		}
		if startKey.Cmp(endKey) >= 0 {
			break
		}
	}
	if ingestBeCtx != nil {
		ingestBeCtx.EngMgr.ResetWorkers(ingestBeCtx, job.ID, reorgInfo.currElement.ID)
	}
	return nil
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
	if tagger := ctx.getResourceGroupTaggerForTopSQL(); tagger != nil {
		snap.SetOption(kv.ResourceGroupTagger, tagger)
	}

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

// getRegionEndKey gets the actual end key for the range of [startKey, endKey].
func getRangeEndKey(ctx *JobContext, store kv.Storage, priority int, keyPrefix kv.Key, startKey, endKey kv.Key) (kv.Key, error) {
	snap := store.GetSnapshot(kv.MaxVersion)
	snap.SetOption(kv.Priority, priority)
	if tagger := ctx.getResourceGroupTaggerForTopSQL(); tagger != nil {
		snap.SetOption(kv.ResourceGroupTagger, tagger)
	}
	snap.SetOption(kv.RequestSourceInternal, true)
	snap.SetOption(kv.RequestSourceType, ctx.ddlJobSourceType())
	it, err := snap.IterReverse(endKey.Next())
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer it.Close()

	if !it.Valid() || !it.Key().HasPrefix(keyPrefix) {
		return startKey, nil
	}
	if it.Key().Cmp(startKey) < 0 {
		return startKey, nil
	}

	return it.Key(), nil
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
		logutil.BgLogger().Info("[ddl] slow operations", zap.Duration("takeTimes", elapsed), zap.String("msg", slowMsg))
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
			if nKey, ok := n.doneTaskNextKey[n.current]; ok {
				delete(n.doneTaskNextKey, n.current)
				n.current++
				n.nextKey = nKey
			} else {
				break
			}
		}
		return
	}
	n.doneTaskNextKey[doneTaskID] = next
}
