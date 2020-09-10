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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"go.uber.org/zap"
)

type backfillWorkerType byte

const (
	typeAddIndexWorker     backfillWorkerType = 0
	typeUpdateColumnWorker backfillWorkerType = 1
)

func (bWT backfillWorkerType) String() string {
	switch bWT {
	case typeAddIndexWorker:
		return "add index"
	case typeUpdateColumnWorker:
		return "update column"
	default:
		return "unknown"
	}
}

type backfiller interface {
	BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error)
	AddMetricInfo(float64)
}

type backfillResult struct {
	addedCount int
	scanCount  int
	nextHandle kv.Handle
	err        error
}

// backfillTaskContext is the context of the batch adding indices or updating column values.
// After finishing the batch adding indices or updating column values, result in backfillTaskContext will be merged into backfillResult.
type backfillTaskContext struct {
	nextHandle kv.Handle
	done       bool
	addedCount int
	scanCount  int
}

type backfillWorker struct {
	id        int
	ddlWorker *worker
	batchCnt  int
	sessCtx   sessionctx.Context
	taskCh    chan *reorgBackfillTask
	resultCh  chan *backfillResult
	table     table.Table
	closed    bool
	priority  int
}

func newBackfillWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable) *backfillWorker {
	return &backfillWorker{
		id:        id,
		table:     t,
		ddlWorker: worker,
		batchCnt:  int(variable.GetDDLReorgBatchSize()),
		sessCtx:   sessCtx,
		taskCh:    make(chan *reorgBackfillTask, 1),
		resultCh:  make(chan *backfillResult, 1),
		priority:  kv.PriorityLow,
	}
}

func (w *backfillWorker) Close() {
	if !w.closed {
		w.closed = true
		close(w.taskCh)
	}
}

func closeBackfillWorkers(workers []*backfillWorker) {
	for _, worker := range workers {
		worker.Close()
	}
}

type reorgBackfillTask struct {
	physicalTableID int64
	startHandle     kv.Handle
	endHandle       kv.Handle
	// endIncluded indicates whether the range include the endHandle.
	// When the last handle is math.MaxInt64, set endIncluded to true to
	// tell worker backfilling index of endHandle.
	endIncluded bool
}

func (r *reorgBackfillTask) String() string {
	rightParenthesis := ")"
	if r.endIncluded {
		rightParenthesis = "]"
	}
	return "physicalTableID" + strconv.FormatInt(r.physicalTableID, 10) + "_" + "[" + r.startHandle.String() + "," + r.endHandle.String() + rightParenthesis
}

func logSlowOperations(elapsed time.Duration, slowMsg string, threshold uint32) {
	if threshold == 0 {
		threshold = atomic.LoadUint32(&variable.DDLSlowOprThreshold)
	}

	if elapsed >= time.Duration(threshold)*time.Millisecond {
		logutil.BgLogger().Info("[ddl] slow operations", zap.Duration("takeTimes", elapsed), zap.String("msg", slowMsg))
	}
}

// mergeBackfillCtxToResult merge partial result in taskCtx into result.
func mergeBackfillCtxToResult(taskCtx *backfillTaskContext, result *backfillResult) {
	result.nextHandle = taskCtx.nextHandle
	result.addedCount += taskCtx.addedCount
	result.scanCount += taskCtx.scanCount
}

// handleBackfillTask backfills range [task.startHandle, task.endHandle) handle's index to table.
func (w *backfillWorker) handleBackfillTask(d *ddlCtx, task *reorgBackfillTask, bf backfiller) *backfillResult {
	handleRange := *task
	result := &backfillResult{addedCount: 0, nextHandle: handleRange.startHandle, err: nil}
	lastLogCount := 0
	lastLogTime := time.Now()
	startTime := lastLogTime

	for {
		// Give job chance to be canceled, if we not check it here,
		// if there is panic in bf.BackfillDataInTxn we will never cancel the job.
		// Because reorgRecordTask may run a long time,
		// we should check whether this ddl job is still runnable.
		err := w.ddlWorker.isReorgRunnable(d)
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
		w.ddlWorker.reorgCtx.increaseRowCount(int64(taskCtx.addedCount))

		if num := result.scanCount - lastLogCount; num >= 30000 {
			lastLogCount = result.scanCount
			logutil.BgLogger().Info("[ddl] backfill worker back fill index", zap.Int("workerID", w.id), zap.Int("addedCount", result.addedCount),
				zap.Int("scanCount", result.scanCount), zap.String("nextHandle", toString(taskCtx.nextHandle)), zap.Float64("speed(rows/s)", float64(num)/time.Since(lastLogTime).Seconds()))
			lastLogTime = time.Now()
		}

		handleRange.startHandle = taskCtx.nextHandle
		if taskCtx.done {
			break
		}
	}
	logutil.BgLogger().Info("[ddl] backfill worker finish task", zap.Int("workerID", w.id),
		zap.String("task", task.String()), zap.Int("addedCount", result.addedCount),
		zap.Int("scanCount", result.scanCount), zap.String("nextHandle", toString(result.nextHandle)),
		zap.String("takeTime", time.Since(startTime).String()))
	return result
}

func (w *backfillWorker) run(d *ddlCtx, bf backfiller) {
	logutil.BgLogger().Info("[ddl] backfill worker start", zap.Int("workerID", w.id))
	defer func() {
		w.resultCh <- &backfillResult{err: errReorgPanic}
	}()
	defer util.Recover(metrics.LabelDDL, "backfillWorker.run", nil, false)
	for {
		task, more := <-w.taskCh
		if !more {
			break
		}

		logutil.BgLogger().Debug("[ddl] backfill worker got task", zap.Int("workerID", w.id), zap.String("task", task.String()))
		failpoint.Inject("mockBackfillRunErr", func() {
			if w.id == 0 {
				result := &backfillResult{addedCount: 0, nextHandle: nil, err: errors.Errorf("mock backfill error")}
				w.resultCh <- result
				failpoint.Continue()
			}
		})

		// Dynamic change batch size.
		w.batchCnt = int(variable.GetDDLReorgBatchSize())
		result := w.handleBackfillTask(d, task, bf)
		w.resultCh <- result
	}
	logutil.BgLogger().Info("[ddl] backfill worker exit", zap.Int("workerID", w.id))
}

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up backfilling data in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(t table.PhysicalTable, store kv.Storage, startHandle, endHandle kv.Handle) ([]kv.KeyRange, error) {
	startRecordKey := t.RecordKey(startHandle)
	endRecordKey := t.RecordKey(endHandle)

	logutil.BgLogger().Info("[ddl] split table range from PD", zap.Int64("physicalTableID", t.GetPhysicalID()),
		zap.String("startHandle", toString(startHandle)), zap.String("endHandle", toString(endHandle)))
	kvRange := kv.KeyRange{StartKey: startRecordKey, EndKey: endRecordKey}
	s, ok := store.(tikv.Storage)
	if !ok {
		// Only support split ranges in tikv.Storage now.
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := tikv.NewBackofferWithVars(context.Background(), maxSleep, nil)
	ranges, err := tikv.SplitRegionRanges(bo, s.GetRegionCache(), []kv.KeyRange{kvRange})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(ranges) == 0 {
		return nil, errors.Trace(errInvalidSplitRegionRanges)
	}
	return ranges, nil
}

func (w *worker) waitTaskResults(workers []*backfillWorker, taskCnt int, totalAddedCount *int64, startHandle kv.Handle) (kv.Handle, int64, error) {
	var (
		addedCount int64
		nextHandle = startHandle
		firstErr   error
	)
	for i := 0; i < taskCnt; i++ {
		worker := workers[i]
		result := <-worker.resultCh
		if firstErr == nil && result.err != nil {
			firstErr = result.err
			// We should wait all working workers exits, any way.
			continue
		}

		if result.err != nil {
			logutil.BgLogger().Warn("[ddl] backfill worker failed", zap.Int("workerID", worker.id),
				zap.Error(result.err))
		}

		if firstErr == nil {
			*totalAddedCount += int64(result.addedCount)
			addedCount += int64(result.addedCount)
			nextHandle = result.nextHandle
		}
	}

	return nextHandle, addedCount, errors.Trace(firstErr)
}

// handleReorgTasks sends tasks to workers, and waits for all the running workers to return results,
// there are taskCnt running workers.
func (w *worker) handleReorgTasks(reorgInfo *reorgInfo, totalAddedCount *int64, workers []*backfillWorker, batchTasks []*reorgBackfillTask) error {
	for i, task := range batchTasks {
		workers[i].taskCh <- task
	}

	startHandle := batchTasks[0].startHandle
	taskCnt := len(batchTasks)
	startTime := time.Now()
	nextHandle, taskAddedCount, err := w.waitTaskResults(workers, taskCnt, totalAddedCount, startHandle)
	elapsedTime := time.Since(startTime)
	if err == nil {
		err = w.isReorgRunnable(reorgInfo.d)
	}

	if err != nil {
		// Update the reorg handle that has been processed.
		err1 := reorgInfo.UpdateReorgMeta()
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblError).Observe(elapsedTime.Seconds())
		logutil.BgLogger().Warn("[ddl] backfill worker handle batch tasks failed",
			zap.ByteString("elementType", reorgInfo.currElement.TypeKey), zap.Int64("elementID", reorgInfo.currElement.ID),
			zap.Int64("totalAddedCount", *totalAddedCount), zap.String("startHandle", toString(startHandle)),
			zap.String("nextHandle", toString(nextHandle)), zap.Int64("batchAddedCount", taskAddedCount),
			zap.String("taskFailedError", err.Error()), zap.String("takeTime", elapsedTime.String()),
			zap.NamedError("updateHandleError", err1))
		return errors.Trace(err)
	}

	// nextHandle will be updated periodically in runReorgJob, so no need to update it here.
	w.reorgCtx.setNextHandle(nextHandle)
	metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblOK).Observe(elapsedTime.Seconds())
	logutil.BgLogger().Info("[ddl] backfill worker handle batch tasks successful",
		zap.ByteString("elementType", reorgInfo.currElement.TypeKey), zap.Int64("elementID", reorgInfo.currElement.ID),
		zap.Int64("totalAddedCount", *totalAddedCount), zap.String("startHandle", toString(startHandle)),
		zap.String("nextHandle", toString(nextHandle)), zap.Int64("batchAddedCount", taskAddedCount), zap.String("takeTime", elapsedTime.String()))
	return nil
}

func decodeHandleRange(keyRange kv.KeyRange) (kv.Handle, kv.Handle, error) {
	startHandle, err := tablecodec.DecodeRowKey(keyRange.StartKey)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	endHandle, err := tablecodec.DecodeRowKey(keyRange.EndKey)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return startHandle, endHandle, nil
}

// sendRangeTaskToWorkers sends tasks to workers, and returns remaining kvRanges that is not handled.
func (w *worker) sendRangeTaskToWorkers(workers []*backfillWorker, reorgInfo *reorgInfo,
	totalAddedCount *int64, kvRanges []kv.KeyRange, globalEndHandle kv.Handle) ([]kv.KeyRange, error) {
	batchTasks := make([]*reorgBackfillTask, 0, len(workers))
	physicalTableID := reorgInfo.PhysicalTableID

	// Build reorg tasks.
	for _, keyRange := range kvRanges {
		startHandle, endHandle, err := decodeHandleRange(keyRange)
		if err != nil {
			return nil, errors.Trace(err)
		}

		endIncluded := false
		if endHandle.Equal(globalEndHandle) {
			endIncluded = true
		}
		task := &reorgBackfillTask{physicalTableID, startHandle, endHandle, endIncluded}
		batchTasks = append(batchTasks, task)

		if len(batchTasks) >= len(workers) {
			break
		}
	}

	if len(batchTasks) == 0 {
		return nil, nil
	}

	// Wait tasks finish.
	err := w.handleReorgTasks(reorgInfo, totalAddedCount, workers, batchTasks)
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
	TestCheckWorkerNumCh = make(chan struct{})
	// TestCheckWorkerNumber use for test adjust backfill worker.
	TestCheckWorkerNumber = int32(16)
)

func loadDDLReorgVars(w *worker) error {
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)
	return ddlutil.LoadDDLReorgVars(ctx)
}

func makeupDecodeColMap(sessCtx sessionctx.Context, t table.Table) (map[int64]decoder.Column, error) {
	dbName := model.NewCIStr(sessCtx.GetSessionVars().CurrentDB)
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

// writePhysicalTableRecord handles the "add index" or "modify/change column" reorganization state for a non-partitioned table or a partition.
// For a partitioned table, it should be handled partition by partition.
//
// How to "add index" or "update column value" in reorganization state?
// Concurrently process the @@tidb_ddl_reorg_worker_cnt tasks. Each task deals with a handle range of the index/row record.
// The handle range is split from PD regions now. Each worker deal with a region table key range one time.
// Each handle range by estimation, concurrent processing needs to perform after the handle range has been acquired.
// The operation flow is as follows:
//	1. Open numbers of defaultWorkers goroutines.
//	2. Split table key range from PD regions.
//	3. Send tasks to running workers by workers's task channel. Each task deals with a region key ranges.
//	4. Wait all these running tasks finished, then continue to step 3, until all tasks is done.
// The above operations are completed in a transaction.
// Finally, update the concurrent processing of the total number of rows, and store the completed handle value.
func (w *worker) writePhysicalTableRecord(t table.PhysicalTable, bfWorkerType backfillWorkerType, indexInfo *model.IndexInfo, oldColInfo, colInfo *model.ColumnInfo, reorgInfo *reorgInfo) error {
	job := reorgInfo.Job
	totalAddedCount := job.GetRowCount()

	startHandle, endHandle := reorgInfo.StartHandle, reorgInfo.EndHandle
	sessCtx := newContext(reorgInfo.d.store)
	decodeColMap, err := makeupDecodeColMap(sessCtx, t)
	if err != nil {
		return errors.Trace(err)
	}

	if err := w.isReorgRunnable(reorgInfo.d); err != nil {
		return errors.Trace(err)
	}
	if startHandle == nil && endHandle == nil {
		return nil
	}

	failpoint.Inject("MockCaseWhenParseFailure", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("job.ErrCount:" + strconv.Itoa(int(job.ErrorCount)) + ", mock unknown type: ast.whenClause."))
		}
	})

	// variable.ddlReorgWorkerCounter can be modified by system variable "tidb_ddl_reorg_worker_cnt".
	workerCnt := variable.GetDDLReorgWorkerCounter()
	backfillWorkers := make([]*backfillWorker, 0, workerCnt)
	defer func() {
		closeBackfillWorkers(backfillWorkers)
	}()

	for {
		kvRanges, err := splitTableRanges(t, reorgInfo.d.store, startHandle, endHandle)
		if err != nil {
			return errors.Trace(err)
		}

		// For dynamic adjust backfill worker number.
		if err := loadDDLReorgVars(w); err != nil {
			logutil.BgLogger().Error("[ddl] load DDL reorganization variable failed", zap.Error(err))
		}
		workerCnt = variable.GetDDLReorgWorkerCounter()
		// If only have 1 range, we can only start 1 worker.
		if len(kvRanges) < int(workerCnt) {
			workerCnt = int32(len(kvRanges))
		}
		// Enlarge the worker size.
		for i := len(backfillWorkers); i < int(workerCnt); i++ {
			sessCtx := newContext(reorgInfo.d.store)
			sessCtx.GetSessionVars().StmtCtx.IsDDLJobInQueue = true

			if bfWorkerType == typeAddIndexWorker {
				idxWorker := newAddIndexWorker(sessCtx, w, i, t, indexInfo, decodeColMap)
				idxWorker.priority = job.Priority
				backfillWorkers = append(backfillWorkers, idxWorker.backfillWorker)
				go idxWorker.backfillWorker.run(reorgInfo.d, idxWorker)
			} else {
				updateWorker := newUpdateColumnWorker(sessCtx, w, i, t, oldColInfo, colInfo, decodeColMap)
				updateWorker.priority = job.Priority
				backfillWorkers = append(backfillWorkers, updateWorker.backfillWorker)
				go updateWorker.backfillWorker.run(reorgInfo.d, updateWorker)
			}
		}
		// Shrink the worker size.
		if len(backfillWorkers) > int(workerCnt) {
			workers := backfillWorkers[workerCnt:]
			backfillWorkers = backfillWorkers[:workerCnt]
			closeBackfillWorkers(workers)
		}

		failpoint.Inject("checkBackfillWorkerNum", func(val failpoint.Value) {
			if val.(bool) {
				num := int(atomic.LoadInt32(&TestCheckWorkerNumber))
				if num != 0 {
					if num > len(kvRanges) {
						if len(backfillWorkers) != len(kvRanges) {
							failpoint.Return(errors.Errorf("check backfill worker num error, len kv ranges is: %v, check backfill worker num is: %v, actual record num is: %v", len(kvRanges), num, len(backfillWorkers)))
						}
					} else if num != len(backfillWorkers) {
						failpoint.Return(errors.Errorf("check backfill worker num error, len kv ranges is: %v, check backfill worker num is: %v, actual record num is: %v", len(kvRanges), num, len(backfillWorkers)))
					}
					TestCheckWorkerNumCh <- struct{}{}
				}
			}
		})

		logutil.BgLogger().Info("[ddl] start backfill workers to reorg record", zap.Int("workerCnt", len(backfillWorkers)),
			zap.Int("regionCnt", len(kvRanges)), zap.String("startHandle", toString(startHandle)), zap.String("endHandle", toString(endHandle)))
		remains, err := w.sendRangeTaskToWorkers(backfillWorkers, reorgInfo, &totalAddedCount, kvRanges, endHandle)
		if err != nil {
			return errors.Trace(err)
		}

		if len(remains) == 0 {
			break
		}
		startHandle, _, err = decodeHandleRange(remains[0])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(h kv.Handle, rowKey kv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotRows(store kv.Storage, priority int, t table.Table, version uint64, startHandle kv.Handle, endHandle kv.Handle, endIncluded bool, fn recordIterFunc) error {
	var firstKey kv.Key
	if startHandle == nil {
		firstKey = t.RecordPrefix()
	} else {
		firstKey = t.RecordKey(startHandle)
	}

	var upperBound kv.Key
	if endHandle == nil {
		upperBound = t.RecordPrefix().PrefixNext()
	} else {
		if endIncluded {
			if endHandle.IsInt() && endHandle.IntValue() == math.MaxInt64 {
				upperBound = t.RecordKey(endHandle).PrefixNext()
			} else {
				upperBound = t.RecordKey(endHandle.Next())
			}
		} else {
			upperBound = t.RecordKey(endHandle)
		}
	}

	ver := kv.Version{Ver: version}
	snap, err := store.GetSnapshot(ver)
	snap.SetOption(kv.Priority, priority)
	if err != nil {
		return errors.Trace(err)
	}

	it, err := snap.Iter(firstKey, upperBound)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	for it.Valid() {
		if !it.Key().HasPrefix(t.RecordPrefix()) {
			break
		}

		var handle kv.Handle
		handle, err = tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}
		rk := t.RecordKey(handle)

		more, err := fn(handle, rk, it.Value())
		if !more || err != nil {
			return errors.Trace(err)
		}

		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			if kv.ErrNotExist.Equal(err) {
				break
			}
			return errors.Trace(err)
		}
	}

	return nil
}
