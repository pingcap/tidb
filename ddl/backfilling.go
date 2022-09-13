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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/driver/backoff"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tidb/util/topsql"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const (
	instanceLease = 60 // s
	// TODO: control the behavior
	enableDistReorg = true
)

var backfillWorkerID int64

func genBackfillWorkerID() int64 {
	return atomic.AddInt64(&backfillWorkerID, 1)
}

// By now the DDL jobs that need backfilling include:
// 1: add-index
// 2: modify-column-type
// 3: clean-up global index
//
// They all have a write reorganization state to back fill data into the rows existed.
// Backfilling is time consuming, to accelerate this process, TiDB has built some sub
// workers to do this in the DDL owner node.
//
//                                DDL owner thread
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

type backfiller interface {
	BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error)
	AddMetricInfo(float64)
	GetTask() (*model.BackfillJob, error)
	UpdateTask(job *model.BackfillJob) error
	FinishTask(bj *model.BackfillJob) error
}

type backfillResult struct {
	addedCount int
	scanCount  int
	nextKey    kv.Key
	err        error
}

// type backfillJobContext struct {
// 	tbl table.PhysicalTable
// 	// below fields are cache for top sql
// 	cacheSQL           string
// 	cacheNormalizedSQL string
// 	cacheDigest        *parser.Digest
// }
//
// // newBackfillJobContext returns a new backfill job context.
// func newBackfillJobContext(sql string) *backfillJobContext {
// 	return &backfillJobContext{
// 		cacheSQL:           sql,
// 		cacheNormalizedSQL: "",
// 		cacheDigest:        nil,
// 	}
// }

// backfillTaskContext is the context of the batch adding indices or updating column values.
// After finishing the batch adding indices or updating column values, result in backfillTaskContext will be merged into backfillResult.
type backfillTaskContext struct {
	nextKey       kv.Key
	done          bool
	addedCount    int
	scanCount     int
	warnings      map[errors.ErrorID]*terror.Error
	warningsCount map[errors.ErrorID]int64
}

type reorgBackfillTask struct {
	bfJob           *model.BackfillJob
	physicalTableID int64
	startKey        kv.Key
	endKey          kv.Key
	endInclude      bool
	priority        int
}

func (r *reorgBackfillTask) String() string {
	physicalID := strconv.FormatInt(r.physicalTableID, 10)
	startKey := tryDecodeToHandleString(r.startKey)
	endKey := tryDecodeToHandleString(r.endKey)
	rangeStr := "physicalTableID_" + physicalID + "_" + "[" + startKey + "," + endKey
	if r.endInclude {
		return rangeStr + "]"
	}
	return rangeStr + ")"
}

// mergeBackfillCtxToResult merge partial result in taskCtx into result.
func mergeBackfillCtxToResult(taskCtx *backfillTaskContext, result *backfillResult) {
	result.nextKey = taskCtx.nextKey
	result.addedCount += taskCtx.addedCount
	result.scanCount += taskCtx.scanCount
}

type backfillCtx struct {
	dCtx     *ddlCtx
	sessCtx  sessionctx.Context
	table    table.Table
	batchCnt int
}

func newBackfillCtx(ctx *ddlCtx, sessCtx sessionctx.Context, tbl table.Table, batchCnt int) *backfillCtx {
	return &backfillCtx{
		dCtx:     ctx,
		sessCtx:  sessCtx,
		table:    tbl,
		batchCnt: batchCnt,
	}
}

type backfillWorker struct {
	*backfillCtx
	ddlID       []byte
	id          int
	backfillJob *model.BackfillJob
	taskCh      chan *reorgBackfillTask
	resultCh    chan *backfillResult
	closed      bool
}

func newBackfillWorker(sessCtx sessionctx.Context, id int, t table.PhysicalTable, dCtx *ddlCtx) *backfillWorker {
	return &backfillWorker{
		backfillCtx: newBackfillCtx(dCtx, sessCtx, t, int(variable.GetDDLReorgBatchSize())),
		ddlID:       []byte(dCtx.uuid),
		id:          id,
		taskCh:      make(chan *reorgBackfillTask, 1),
		resultCh:    make(chan *backfillResult, 1),
	}
}

func (w *backfillWorker) String() string {
	return fmt.Sprintf("worker %d, tp backfill", w.id)
}

func (w *backfillWorker) initInfo(t table.PhysicalTable) {
	w.table = t
}

func (w *backfillWorker) closeAndClean() {
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

func getOracleTime(store kv.Storage) (time.Time, error) {
	if !enableDistReorg {
		return time.Time{}, nil
	}

	currentVer, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	return oracle.GetTimeFromTS(currentVer.Ver), nil
}

func (w *backfillWorker) updateLease(exec_id string, bf backfiller) error {
	if !enableDistReorg {
		return nil
	}

	leaseTime, err := getOracleTime(w.sessCtx.GetStore())
	if err != nil {
		return err
	}
	w.backfillJob.Instance_ID = exec_id
	// TODO: w.backfillJob.Instance_Lease = types.NewTime(types.FromGoTime(lease), mysql.TypeTimestamp, types.DefaultFsp)
	w.backfillJob.Instance_Lease = leaseTime.Add(instanceLease * time.Second)
	return bf.UpdateTask(w.backfillJob)
}

func (w *backfillWorker) finishJob(bf backfiller) error {
	if !enableDistReorg {
		return nil
	}

	w.backfillJob.State = model.JobStateDone
	return bf.FinishTask(w.backfillJob)
}

func (w *backfillWorker) releaseJob(bf backfiller) error {
	if !enableDistReorg {
		return nil
	}

	w.backfillJob.Instance_ID = string(w.ddlID)
	return bf.UpdateTask(w.backfillJob)
}

// handleBackfillTask backfills range [task.startHandle, task.endHandle) handle's index to table.
func (w *backfillWorker) handleBackfillTask(d *ddlCtx, task *reorgBackfillTask, bf backfiller) *backfillResult {
	handleRange := *task
	result := &backfillResult{
		err:        nil,
		addedCount: 0,
		nextKey:    handleRange.startKey,
	}
	lastLogCount := 0
	lastLogTime := time.Now()
	startTime := lastLogTime
	// rc := d.getReorgCtx(task.bfJob.JobID)

	for {
		// Give job chance to be canceled, if we not check it here,
		// if there is panic in bf.BackfillDataInTxn we will never cancel the job.
		// Because reorgRecordTask may run a long time,
		// we should check whether this ddl job is still runnable.
		err := d.isReorgRunnable(task.bfJob.JobID, 1)
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
		// TODO:
		//	rc.increaseRowCount(int64(taskCtx.addedCount))
		//	rc.mergeWarnings(taskCtx.warnings, taskCtx.warningsCount)

		if num := result.scanCount - lastLogCount; num >= 30000 {
			lastLogCount = result.scanCount
			logutil.BgLogger().Info("[ddl] backfill worker back fill index",
				zap.Int("workerID", w.id),
				zap.Int("addedCount", result.addedCount),
				zap.Int("scanCount", result.scanCount),
				zap.String("nextHandle", tryDecodeToHandleString(taskCtx.nextKey)),
				zap.Float64("speed(rows/s)", float64(num)/time.Since(lastLogTime).Seconds()))
			lastLogTime = time.Now()
		}

		handleRange.startKey = taskCtx.nextKey
		if taskCtx.done {
			break
		}
	}
	logutil.BgLogger().Info("[ddl] backfill worker finish task", zap.Int("workerID", w.id),
		zap.String("task", task.String()),
		zap.Int("addedCount", result.addedCount),
		zap.Int("scanCount", result.scanCount),
		zap.String("nextHandle", tryDecodeToHandleString(result.nextKey)),
		zap.String("takeTime", time.Since(startTime).String()))
	return result
}

func (w *backfillWorker) run(d *ddlCtx, bf backfiller, jobID int64, jobQuery string) {
	defer func() {
		w.resultCh <- &backfillResult{err: dbterror.ErrReorgPanic}
	}()
	defer util.Recover(metrics.LabelDDL, "backfillWorker.run", nil, false)
	for {
		task, more := <-w.taskCh
		if !more {
			break
		}
		logutil.BgLogger().Info("[ddl] run backfill worker",
			zap.Bool("more", more), zap.Int("workerID", w.id), zap.String("task", task.String()), zap.String("bj", task.bfJob.IDStr()))
		d.setDDLLabelForTopSQL(jobID, jobQuery)

		w.backfillJob = task.bfJob
		if err := w.updateLease(d.uuid, bf); err != nil {
			logutil.BgLogger().Info("[ddl] run backfill worker got task failed",
				zap.Int("workerID", w.id), zap.String("task", task.String()), zap.String("bj", task.bfJob.IDStr()), zap.Error(err))
			return
		}

		logutil.BgLogger().Debug("[ddl] run backfill worker got task", zap.Int("workerID", w.id), zap.String("task", task.String()))
		failpoint.Inject("mockBackfillRunErr", func() {
			if w.id == 0 {
				result := &backfillResult{addedCount: 0, nextKey: nil, err: errors.Errorf("mock backfill error")}
				w.resultCh <- result
				failpoint.Continue()
			}
		})
		failpoint.Inject("mockHighLoadForAddIndex", func() {
			sqlPrefixes := []string{"alter"}
			topsql.MockHighCPULoad(jobQuery, sqlPrefixes, 5)
		})
		failpoint.Inject("mockBackfillSlow", func() {
			time.Sleep(100 * time.Millisecond)
		})

		// Dynamic change batch size.
		// TODO:
		// w.batchCnt = int(variable.GetDDLReorgBatchSize())
		w.batchCnt = 1024
		result := w.handleBackfillTask(d, task, bf)

		w.resultCh <- result
		w.finishJob(bf)
	}
	logutil.BgLogger().Info("[ddl] backfill worker exit", zap.Int("workerID", w.id))
}

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up backfilling data in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(t table.PhysicalTable, store kv.Storage, startKey, endKey kv.Key) ([]kv.KeyRange, error) {
	logutil.BgLogger().Info("[ddl] split table range from PD",
		zap.Int64("physicalTableID", t.GetPhysicalID()),
		zap.String("startHandle", tryDecodeToHandleString(startKey)),
		zap.String("endHandle", tryDecodeToHandleString(endKey)))
	kvRange := kv.KeyRange{StartKey: startKey, EndKey: endKey}
	s, ok := store.(tikv.Storage)
	if !ok {
		// Only support split ranges in tikv.Storage now.
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := backoff.NewBackofferWithVars(context.Background(), maxSleep, nil)
	rc := copr.NewRegionCache(s.GetRegionCache())
	ranges, err := rc.SplitRegionRanges(bo, []kv.KeyRange{kvRange})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(ranges) == 0 {
		errMsg := fmt.Sprintf("cannot find region in range [%s, %s]", startKey.String(), endKey.String())
		return nil, errors.Trace(dbterror.ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg))
	}
	return ranges, nil
}

func waitTaskResults(workers []*backfillWorker, taskCnt int,
	totalAddedCount *int64, startKey kv.Key) (kv.Key, int64, error) {
	var (
		addedCount int64
		nextKey    = startKey
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
			nextKey = result.nextKey
		}
	}

	return nextKey, addedCount, errors.Trace(firstErr)
}

// sendTasksAndWait sends tasks to workers, and waits for all the running workers to return results,
// there are taskCnt running workers.
func (dc *ddlCtx) sendTasksAndWait(sessPool *sessionPool, reorgInfo *reorgInfo, totalAddedCount *int64, workers []*backfillWorker, batchTasks []*reorgBackfillTask) error {
	for i, task := range batchTasks {
		workers[i].taskCh <- task
	}

	startKey := batchTasks[0].startKey
	taskCnt := len(batchTasks)
	startTime := time.Now()
	nextKey, taskAddedCount, err := waitTaskResults(workers, taskCnt, totalAddedCount, startKey)
	elapsedTime := time.Since(startTime)
	if err == nil {
		err = dc.isReorgRunnable(reorgInfo.Job.ID)
	}

	if err != nil {
		// Update the reorg handle that has been processed.
		err1 := reorgInfo.UpdateReorgMeta(nextKey, sessPool)
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblError).Observe(elapsedTime.Seconds())
		logutil.BgLogger().Warn("[ddl] backfill worker handle batch tasks failed",
			zap.ByteString("elementType", reorgInfo.currElement.TypeKey),
			zap.Int64("elementID", reorgInfo.currElement.ID),
			zap.Int64("totalAddedCount", *totalAddedCount),
			zap.String("startHandle", tryDecodeToHandleString(startKey)),
			zap.String("nextHandle", tryDecodeToHandleString(nextKey)),
			zap.Int64("batchAddedCount", taskAddedCount),
			zap.String("taskFailedError", err.Error()),
			zap.String("takeTime", elapsedTime.String()),
			zap.NamedError("updateHandleError", err1))
		return errors.Trace(err)
	}

	// nextHandle will be updated periodically in runReorgJob, so no need to update it here.
	dc.getReorgCtx(reorgInfo.Job.ID).setNextKey(nextKey)
	metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblOK).Observe(elapsedTime.Seconds())
	logutil.BgLogger().Info("[ddl] backfill workers successfully processed batch",
		zap.ByteString("elementType", reorgInfo.currElement.TypeKey),
		zap.Int64("elementID", reorgInfo.currElement.ID),
		zap.Int64("totalAddedCount", *totalAddedCount),
		zap.String("startHandle", tryDecodeToHandleString(startKey)),
		zap.String("nextHandle", tryDecodeToHandleString(nextKey)),
		zap.Int64("batchAddedCount", taskAddedCount),
		zap.String("takeTime", elapsedTime.String()))
	return nil
}

func tryDecodeToHandleString(key kv.Key) string {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Warn("tryDecodeToHandleString panic",
				zap.Any("recover()", r),
				zap.Binary("key", key))
		}
	}()
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		recordPrefixIdx := bytes.Index(key, []byte("_r"))
		if recordPrefixIdx == -1 {
			return fmt.Sprintf("key: %x", key)
		}
		handleBytes := key[recordPrefixIdx+2:]
		terminatedWithZero := len(handleBytes) > 0 && handleBytes[len(handleBytes)-1] == 0
		if terminatedWithZero {
			handle, err := tablecodec.DecodeRowKey(key[:len(key)-1])
			if err == nil {
				return handle.String() + ".next"
			}
		}
		return fmt.Sprintf("%x", handleBytes)
	}
	return handle.String()
}

func getBatchTasks(t table.Table, reorgInfo *reorgInfo, kvRanges []kv.KeyRange, batchCnt int) []*reorgBackfillTask {
	batchTasks := make([]*reorgBackfillTask, 0, batchCnt)
	physicalTableID := reorgInfo.PhysicalTableID
	// Build reorg tasks.
	for i, keyRange := range kvRanges {
		endKey := keyRange.EndKey
		endK, err := getRangeEndKey(reorgInfo.d.jobContext(reorgInfo.Job.ID), reorgInfo.d.store, reorgInfo.Job.Priority, t, keyRange.StartKey, endKey)
		if err != nil {
			logutil.BgLogger().Info("[ddl] get range task, get reverse key failed", zap.Error(err))
		} else {
			logutil.BgLogger().Info("[ddl] get range task, change end key",
				zap.String("end key", tryDecodeToHandleString(endKey)), zap.String("current end key", tryDecodeToHandleString(endK)))
			endKey = endK
		}

		task := &reorgBackfillTask{
			physicalTableID: physicalTableID,
			startKey:        keyRange.StartKey,
			endKey:          endKey,
			priority:        reorgInfo.Priority,
			// If the boundaries overlap, we should ignore the preceding endKey.
			endInclude: endK.Cmp(keyRange.EndKey) != 0 || i == len(kvRanges)-1}
		batchTasks = append(batchTasks, task)

		if len(batchTasks) >= batchCnt {
			break
		}
	}

	return batchTasks
}

// handleRangeTasks sends tasks to workers, and returns remaining kvRanges that is not handled.
func (dc *ddlCtx) handleRangeTasks(sessPool *sessionPool, t table.Table, workers []*backfillWorker, reorgInfo *reorgInfo,
	totalAddedCount *int64, kvRanges []kv.KeyRange) ([]kv.KeyRange, error) {

	batchTasks := getBatchTasks(t, reorgInfo, kvRanges, len(workers))
	if len(batchTasks) == 0 {
		return nil, nil
	}

	// Wait tasks finish.
	err := dc.sendTasksAndWait(sessPool, reorgInfo, totalAddedCount, workers, batchTasks)
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
	TestCheckWorkerNumber = int32(16)
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

func setSessCtxLocation(sctx sessionctx.Context, info *reorgInfo) error {
	// It is set to SystemLocation to be compatible with nil LocationInfo.
	*sctx.GetSessionVars().TimeZone = *timeutil.SystemLocation()
	if info.ReorgMeta.Location != nil {
		loc, err := info.ReorgMeta.Location.GetLocation()
		if err != nil {
			return errors.Trace(err)
		}
		*sctx.GetSessionVars().TimeZone = *loc
	}
	return nil
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
func (dc *ddlCtx) writePhysicalTableRecord(sessPool *sessionPool, t table.PhysicalTable, bfWorkerType model.BackfillType, reorgInfo *reorgInfo) error {
	if enableDistReorg {
		sCtx, err := sessPool.get()
		if err != nil {
			return errors.Trace(err)
		}
		defer sessPool.put(sCtx)
		return dc.controlWritePhysicalTableRecord(newSession(sCtx), t, bfWorkerType, reorgInfo)
	}
	job := reorgInfo.Job
	totalAddedCount := job.GetRowCount()

	startKey, endKey := reorgInfo.StartKey, reorgInfo.EndKey
	sessCtx := newContext(reorgInfo.d.store)
	decodeColMap, err := makeupDecodeColMap(sessCtx, t)
	if err != nil {
		return errors.Trace(err)
	}

	if err := dc.isReorgRunnable(reorgInfo.Job.ID); err != nil {
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

	// variable.ddlReorgWorkerCounter can be modified by system variable "tidb_ddl_reorg_worker_cnt".
	workerCnt := variable.GetDDLReorgWorkerCounter()
	backfillWorkers := make([]*backfillWorker, 0, workerCnt)
	defer func() {
		closeBackfillWorkers(backfillWorkers)
	}()
	jc := dc.jobContext(job.ID)

	for {
		kvRanges, err := splitTableRanges(t, reorgInfo.d.store, startKey, endKey)
		if err != nil {
			return errors.Trace(err)
		}

		// For dynamic adjust backfill worker number.
		if err := loadDDLReorgVars(dc.ctx, sessPool); err != nil {
			logutil.BgLogger().Error("[ddl] load DDL reorganization variable failed", zap.Error(err))
		}
		workerCnt = variable.GetDDLReorgWorkerCounter()
		rowFormat := variable.GetDDLReorgRowFormat()
		// If only have 1 range, we can only start 1 worker.
		if len(kvRanges) < int(workerCnt) {
			workerCnt = int32(len(kvRanges))
		}
		// Enlarge the worker size.
		for i := len(backfillWorkers); i < int(workerCnt); i++ {
			sessCtx := newContext(reorgInfo.d.store)
			sessCtx.GetSessionVars().StmtCtx.IsDDLJobInQueue = true
			// Set the row encode format version.
			sessCtx.GetSessionVars().RowEncoder.Enable = rowFormat != variable.DefTiDBRowFormatV1
			// Simulate the sql mode environment in the worker sessionCtx.
			sqlMode := reorgInfo.ReorgMeta.SQLMode
			sessCtx.GetSessionVars().SQLMode = sqlMode
			if err := setSessCtxLocation(sessCtx, reorgInfo); err != nil {
				return errors.Trace(err)
			}

			sessCtx.GetSessionVars().StmtCtx.BadNullAsWarning = !sqlMode.HasStrictMode()
			sessCtx.GetSessionVars().StmtCtx.TruncateAsWarning = !sqlMode.HasStrictMode()
			sessCtx.GetSessionVars().StmtCtx.OverflowAsWarning = !sqlMode.HasStrictMode()
			sessCtx.GetSessionVars().StmtCtx.AllowInvalidDate = sqlMode.HasAllowInvalidDatesMode()
			sessCtx.GetSessionVars().StmtCtx.DividedByZeroAsWarning = !sqlMode.HasStrictMode()
			sessCtx.GetSessionVars().StmtCtx.IgnoreZeroInDate = !sqlMode.HasStrictMode() || sqlMode.HasAllowInvalidDatesMode()
			sessCtx.GetSessionVars().StmtCtx.NoZeroDate = sqlMode.HasStrictMode()

			switch bfWorkerType {
			case model.TypeAddIndexBackfill:
				backfillWorker := newBackfillWorker(sessCtx, i, t, reorgInfo.d)
				idxWorker := newAddIndexWorker(decodeColMap, newBackfillCtx(reorgInfo.d, sessCtx, t, int(variable.GetDDLReorgBatchSize())),
					jc, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey, reorgInfo.Query)
				backfillWorkers = append(backfillWorkers, backfillWorker)
				go backfillWorker.run(reorgInfo.d, idxWorker, job.ID, job.Query)
			case model.TypeUpdateColumnBackfill:
				// Setting InCreateOrAlterStmt tells the difference between SELECT casting and ALTER COLUMN casting.
				sessCtx.GetSessionVars().StmtCtx.InCreateOrAlterStmt = true
				backfillWorker := newBackfillWorker(sessCtx, i, t, reorgInfo.d)
				// TODO: remove the argument of reorgInfo
				updateWorker := newUpdateColumnWorker(reorgInfo, backfillWorker.backfillCtx, decodeColMap, jc)
				backfillWorkers = append(backfillWorkers, backfillWorker)
				go backfillWorker.run(reorgInfo.d, updateWorker, job.ID, job.Query)
			case model.TypeCleanUpIndexBackfill:
				backfillWorker := newBackfillWorker(sessCtx, i, t, reorgInfo.d)
				idxWorker := newCleanUpIndexWorker(backfillWorker.backfillCtx, decodeColMap, jc)
				backfillWorkers = append(backfillWorkers, backfillWorker)
				go backfillWorker.run(reorgInfo.d, idxWorker, job.ID, job.Query)
			default:
				return errors.New("unknow backfill type")
			}
		}
		// Shrink the worker size.
		if len(backfillWorkers) > int(workerCnt) {
			workers := backfillWorkers[workerCnt:]
			backfillWorkers = backfillWorkers[:workerCnt]
			closeBackfillWorkers(workers)
		}

		failpoint.Inject("checkBackfillWorkerNum", func(val failpoint.Value) {
			//nolint:forcetypeassert
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
					var wg sync.WaitGroup
					wg.Add(1)
					TestCheckWorkerNumCh <- &wg
					wg.Wait()
				}
			}
		})

		logutil.BgLogger().Info("[ddl] start backfill workers to reorg record",
			zap.Int("workerCnt", len(backfillWorkers)),
			zap.Int("regionCnt", len(kvRanges)),
			zap.String("startHandle", tryDecodeToHandleString(startKey)),
			zap.String("endHandle", tryDecodeToHandleString(endKey)))
		remains, err := dc.handleRangeTasks(sessPool, t, backfillWorkers, reorgInfo, &totalAddedCount, kvRanges)
		if err != nil {
			return errors.Trace(err)
		}

		if len(remains) == 0 {
			break
		}
		startKey = remains[0].StartKey
	}
	return nil
}

func (dc *ddlCtx) backfillJob2Task(t table.Table, bfJob *model.BackfillJob, priority int) *reorgBackfillTask {
	// Build reorg tasks.
	endKey := bfJob.Mate.EndKey
	endK, err := getRangeEndKey(dc.jobContext(bfJob.JobID), dc.store, priority, t, bfJob.Mate.StartKey, endKey)
	if err != nil {
		logutil.BgLogger().Info("[ddl] send range task to worker, get reverse key failed", zap.String("backfill job ID", bfJob.IDStr()), zap.Error(err))
	} else {
		logutil.BgLogger().Info("[ddl] send range task to worker, change end key",
			zap.String("backfill job ID", bfJob.IDStr()), zap.String("end key", tryDecodeToHandleString(endKey)), zap.String("current end key", tryDecodeToHandleString(endK)))
		endKey = endK
	}

	return &reorgBackfillTask{
		bfJob:           bfJob,
		physicalTableID: bfJob.PhysicalID,
		startKey:        bfJob.Mate.StartKey,
		endKey:          endKey,
		endInclude:      bfJob.Mate.EndInclude,
		priority:        priority}
}

func (dc *ddlCtx) sendBackfillJob(worker *backfillWorker, bfJob *model.BackfillJob, t table.Table, priority int) {
	task := dc.backfillJob2Task(t, bfJob, priority)
	logutil.BgLogger().Warn("--------------------------- send backfill job " + worker.String() + " bfJob:" + task.bfJob.IDStr())
	// Wait task finish.
	worker.taskCh <- task
}

func (dc *ddlCtx) waitBackfillJob(worker *backfillWorker, bfJob *model.BackfillJob) error {
	startTime := time.Now()
	result := <-worker.resultCh
	logutil.BgLogger().Warn("--------------------------- get backfill job result " + worker.String())
	addedCount := int64(result.addedCount)
	err := result.err

	elapsedTime := time.Since(startTime)
	if err != nil {
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblError).Observe(elapsedTime.Seconds())
	} else {
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblOK).Observe(elapsedTime.Seconds())
	}
	nextKey := result.nextKey
	logutil.BgLogger().Warn("[ddl] backfill worker handle batch tasks failed",
		zap.String("elementType", bfJob.Tp.String()),
		zap.Int64("elementID", bfJob.EleID),
		zap.String("nextHandle", tryDecodeToHandleString(nextKey)),
		zap.Int64("batchAddedCount", addedCount),
		zap.String("takeTime", elapsedTime.String()),
		zap.Error(err))
	return errors.Trace(err)
}

func (dc *ddlCtx) controlWritePhysicalTableRecord(sess *session, t table.PhysicalTable, bfWorkerType model.BackfillType, reorgInfo *reorgInfo) error {
	startKey, endKey := reorgInfo.StartKey, reorgInfo.EndKey
	if startKey == nil && endKey == nil {
		return nil
	}

	if err := dc.isReorgRunnable(reorgInfo.Job.ID); err != nil {
		return errors.Trace(err)
	}

	existBackfillJob, err := checkBackfillJobState(sess, reorgInfo.Job.ID, reorgInfo.currElement.ID)
	if err != nil {
		return errors.Trace(err)
	}

	logutil.BgLogger().Info("[ddl] control write physical table record ----------------- 00", zap.Bool("exist bJob", existBackfillJob))
	if !existBackfillJob {
		// TODO: set id to a global variable.
		id := int64(1)
		for {
			kvRanges, err := splitTableRanges(t, reorgInfo.d.store, startKey, endKey)
			if err != nil {
				return errors.Trace(err)
			}
			// TODO: split the ranges.
			batchCnt := len(kvRanges)
			batchTasks := getBatchTasks(t, reorgInfo, kvRanges, batchCnt)
			bJobs := make([]*model.BackfillJob, 0, len(batchTasks))
			for _, task := range batchTasks {
				bm := &model.BackfillMeta{
					CurrKey:    task.startKey,
					StartKey:   task.startKey,
					EndKey:     task.endKey,
					EndInclude: task.endInclude,
					// TODO: StartTS
					SQLMode:  reorgInfo.ReorgMeta.SQLMode,
					Location: reorgInfo.ReorgMeta.Location,
				}
				bj := &model.BackfillJob{
					ID:         id,
					JobID:      reorgInfo.Job.ID,
					EleID:      reorgInfo.currElement.ID,
					EleKey:     reorgInfo.currElement.TypeKey,
					PhysicalID: t.Meta().ID,
					Tp:         bfWorkerType,
					State:      model.JobStateNone,
					Mate:       bm,
				}
				id++
				bJobs = append(bJobs, bj)
				logutil.BgLogger().Info("[ddl] control write physical table record ----------------- 11 " + fmt.Sprintf("job:%#v", bj))
			}
			if err = addBackfillJobs(sess, bJobs); err != nil {
				return errors.Trace(err)
			}

			logutil.BgLogger().Info("[ddl] split backfill jobs to the backfill table",
				zap.Int("batchTasksCnt", len(batchTasks)),
				zap.Int("regionCnt", len(kvRanges)),
				zap.String("startHandle", tryDecodeToHandleString(startKey)),
				zap.String("endHandle", tryDecodeToHandleString(endKey)))

			remains := kvRanges[len(batchTasks):]
			if len(remains) == 0 {
				break
			}
			startKey = remains[0].StartKey
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		if err := dc.isReorgRunnable(reorgInfo.Job.ID); err != nil {
			return errors.Trace(err)
		}

		select {
		case <-ticker.C:
			isExist, err := checkBackfillJobState(sess, reorgInfo.Job.ID, reorgInfo.currElement.ID)
			logutil.BgLogger().Info("[ddl] *** control write physical table record ----------------- 22", zap.Bool("isExist", isExist))
			if err != nil {
				logutil.BgLogger().Info("[ddl] finish backfill jobs")
				return errors.Trace(err)
			}
			if !isExist {
				return nil
			}
		case <-dc.ctx.Done():
			return dbterror.ErrInvalidWorker.GenWithStack("worker is closed")
		}
	}

	return nil
}

func checkBackfillJobState(sess *session, jobID, currEleID int64) (bool, error) {
	isIncluded := true
	runningJobIDs := []int64{jobID}

	bJobs, err := getInterruptBackfillJobsForOneEle(sess, jobID, currEleID)
	if err != nil {
		return false, errors.Trace(err)
	}
	// logutil.BgLogger().Info("[ddl] check backfill job state -----------------22", zap.Int("interrupt backfill job cnt", len(bJobs)))
	if len(bJobs) != 0 {
		// TODO: bJobs[0].Mate errMsg
		if err = finishFailedBackfillJobs(sess, jobID); err != nil {
			return true, errors.Trace(err)
		}
		return true, errors.Errorf(bJobs[0].Mate.ErrMsg)
	}

	bJobs, err = getBackfillJobsForOneEle(sess, 1, isIncluded, runningJobIDs, 10*time.Second)
	if err != nil {
		return false, errors.Trace(err)
	}
	// logutil.BgLogger().Info("[ddl] check backfill job state -----------------22", zap.Int("all backfill job cnt", len(bJobs)))
	if len(bJobs) != 0 {
		return true, nil
	}

	return false, nil
}

func finishFailedBackfillJobs(sess sessionctx.Context, runningJobID int64) error {
	isIncluded := true
	// TODO: set batch value
	batch := 128
	for {
		bJobs, err := getBackfillJobsForOneEle(sess.(*session), batch, isIncluded, []int64{runningJobID}, 10*time.Second)
		if err != nil {
			return errors.Trace(err)
		}
		if len(bJobs) == 0 {
			return nil
		}

		for _, job := range bJobs {
			err := removeBackfillJob(nil, job)
			if err != nil {
				return err
			}
			return addBackfillHistoryJob(nil, job)
		}
	}
}

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(h kv.Handle, rowKey kv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotRows(ctx *JobContext, store kv.Storage, priority int, t table.Table, version uint64,
	startKey kv.Key, endKey kv.Key, fn recordIterFunc) error {
	var firstKey kv.Key
	if startKey == nil {
		firstKey = t.RecordPrefix()
	} else {
		firstKey = startKey
	}

	var upperBound kv.Key
	if endKey == nil {
		upperBound = t.RecordPrefix().PrefixNext()
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
		if !it.Key().HasPrefix(t.RecordPrefix()) {
			break
		}

		var handle kv.Handle
		handle, err = tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
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
func getRangeEndKey(ctx *JobContext, store kv.Storage, priority int, t table.Table, startKey, endKey kv.Key) (kv.Key, error) {
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

	if !it.Valid() || !it.Key().HasPrefix(t.RecordPrefix()) {
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
