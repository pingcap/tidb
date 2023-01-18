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
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tidb/util/topsql"
	"github.com/tikv/client-go/v2/oracle"
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

	// InstanceLease is the instance lease.
	InstanceLease       = 1 * time.Minute
	updateInstanceLease = 25 * time.Second
	genTaskBatch        = 4096
	minGenTaskBatch     = 1024
	minDistTaskCnt      = 16
	retrySQLTimes       = 3
	retrySQLInterval    = 500 * time.Millisecond
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

// BackfillJob is for a tidb_ddl_backfill table's record.
type BackfillJob struct {
	ID            int64
	JobID         int64
	EleID         int64
	EleKey        []byte
	Tp            backfillerType
	State         model.JobState
	StoreID       int64
	InstanceID    string
	InstanceLease types.Time
	// range info
	CurrKey  []byte
	StartKey []byte
	EndKey   []byte

	StartTS  uint64
	FinishTS uint64
	RowCount int64
	Meta     *model.BackfillMeta
}

// AbbrStr returns the BackfillJob's info without the Meta info.
func (bj *BackfillJob) AbbrStr() string {
	return fmt.Sprintf("ID:%d, JobID:%d, EleID:%d, Type:%s, State:%s, InstanceID:%s, InstanceLease:%s",
		bj.ID, bj.JobID, bj.EleID, bj.Tp, bj.State, bj.InstanceID, bj.InstanceLease)
}

// GetOracleTimeWithStartTS returns the current time with txn's startTS.
func GetOracleTimeWithStartTS(se *session) (time.Time, error) {
	txn, err := se.Txn(true)
	if err != nil {
		return time.Time{}, err
	}
	return oracle.GetTimeFromTS(txn.StartTS()).UTC(), nil
}

// GetOracleTime returns the current time from TS.
func GetOracleTime(store kv.Storage) (time.Time, error) {
	currentVer, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	return oracle.GetTimeFromTS(currentVer.Ver).UTC(), nil
}

// GetLeaseGoTime returns a types.Time by adding a lease.
func GetLeaseGoTime(currTime time.Time, lease time.Duration) types.Time {
	leaseTime := currTime.Add(lease)
	return types.NewTime(types.FromGoTime(leaseTime.In(time.UTC)), mysql.TypeTimestamp, types.MaxFsp)
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

type backfillCtx struct {
	*ddlCtx
	reorgTp    model.ReorgType
	sessCtx    sessionctx.Context
	schemaName string
	table      table.Table
	batchCnt   int
}

func newBackfillCtx(ctx *ddlCtx, sessCtx sessionctx.Context, reorgTp model.ReorgType,
	schemaName string, tbl table.Table) *backfillCtx {
	return &backfillCtx{
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
	GetTask() (*BackfillJob, error)
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
	bfJob         *BackfillJob
	physicalTable table.PhysicalTable

	// TODO: Remove the following fields after remove the function of run.
	id              int
	physicalTableID int64
	startKey        kv.Key
	endKey          kv.Key
	endInclude      bool
	jobID           int64
	sqlQuery        string
	priority        int
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
	physicalID := strconv.FormatInt(r.physicalTableID, 10)
	startKey := hex.EncodeToString(r.startKey)
	endKey := hex.EncodeToString(r.endKey)
	rangeStr := "taskID_" + strconv.Itoa(r.id) + "_physicalTableID_" + physicalID + "_" + "[" + startKey + "," + endKey
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

type backfillWorker struct {
	id int
	backfiller
	taskCh   chan *reorgBackfillTask
	resultCh chan *backfillResult
	ctx      context.Context
	cancel   func()
}

func newBackfillWorker(ctx context.Context, id int, bf backfiller) *backfillWorker {
	bfCtx, cancel := context.WithCancel(ctx)
	return &backfillWorker{
		backfiller: bf,
		id:         id,
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
	bfJob.CurrKey = nextKey
	bfJob.InstanceID = execID
	bfJob.InstanceLease = GetLeaseGoTime(leaseTime, InstanceLease)
	return w.backfiller.UpdateTask(bfJob)
}

func (w *backfillWorker) finishJob(bfJob *BackfillJob) error {
	bfJob.State = model.JobStateDone
	return w.backfiller.FinishTask(bfJob)
}

func (w *backfillWorker) String() string {
	if w.backfiller == nil {
		return fmt.Sprintf("worker %d", w.id)
	}
	return fmt.Sprintf("worker %d, tp %s", w.id, w.backfiller.String())
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

	for {
		// Give job chance to be canceled, if we not check it here,
		// if there is panic in bf.BackfillDataInTxn we will never cancel the job.
		// Because reorgRecordTask may run a long time,
		// we should check whether this ddl job is still runnable.
		err := d.isReorgRunnable(jobID)
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
			logutil.BgLogger().Info("[ddl] backfill worker back fill index",
				zap.Int("addedCount", result.addedCount), zap.Int("scanCount", result.scanCount),
				zap.String("next key", hex.EncodeToString(taskCtx.nextKey)),
				zap.Float64("speed(rows/s)", float64(num)/time.Since(lastLogTime).Seconds()))
			lastLogTime = time.Now()
		}

		handleRange.startKey = taskCtx.nextKey
		if taskCtx.done {
			break
		}

		if task.bfJob != nil {
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
		zap.Stringer("worker", w),
		zap.Stringer("task", task),
		zap.Int("added count", result.addedCount),
		zap.Int("scan count", result.scanCount),
		zap.String("next key", hex.EncodeToString(result.nextKey)),
		zap.String("take time", time.Since(startTime).String()))
	if ResultCounterForTest != nil && result.err == nil {
		ResultCounterForTest.Add(1)
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
			logutil.BgLogger().Info("[ddl] backfill worker exit on context done",
				zap.Stringer("worker", w), zap.Int("workerID", w.id))
			return
		}
		task, more := <-w.taskCh
		if !more {
			logutil.BgLogger().Info("[ddl] backfill worker exit",
				zap.Stringer("worker", w), zap.Int("workerID", w.id))
			return
		}
		curTaskID = task.id
		d.setDDLLabelForTopSQL(job.ID, job.Query)

		logutil.BgLogger().Debug("[ddl] backfill worker got task", zap.Int("workerID", w.id), zap.String("task", task.String()))
		failpoint.Inject("mockBackfillRunErr", func() {
			if w.id == 0 {
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
				zap.Stringer("worker", w), zap.Int("workerID", w.id), zap.Error(result.err))
			return
		}
	}
}

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up backfilling data in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(t table.PhysicalTable, store kv.Storage, startKey, endKey kv.Key) ([]kv.KeyRange, error) {
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
		err = dc.isReorgRunnable(reorgInfo.Job.ID)
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

	// nextHandle will be updated periodically in runReorgJob, so no need to update it here.
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
	physicalTableID := reorgInfo.PhysicalTableID
	var prefix kv.Key
	if reorgInfo.mergingTmpIdx {
		prefix = t.IndexPrefix()
	} else {
		prefix = t.RecordPrefix()
	}
	// Build reorg tasks.
	job := reorgInfo.Job
	jobCtx := reorgInfo.d.jobContext(reorgInfo.Job.ID)
	for i, keyRange := range kvRanges {
		startKey := keyRange.StartKey
		endKey := keyRange.EndKey
		endK, err := getRangeEndKey(jobCtx, reorgInfo.d.store, job.Priority, prefix, keyRange.StartKey, endKey)
		if err != nil {
			logutil.BgLogger().Info("[ddl] get backfill range task, get reverse key failed", zap.Error(err))
		} else {
			logutil.BgLogger().Info("[ddl] get backfill range task, change end key",
				zap.String("end key", hex.EncodeToString(endKey)), zap.String("current end key", hex.EncodeToString(endK)))
			endKey = endK
		}
		if len(startKey) == 0 {
			startKey = prefix
		}
		if len(endKey) == 0 {
			endKey = prefix.PrefixNext()
		}

		//nolint:forcetypeassert
		phyTbl := t.(table.PhysicalTable)
		task := &reorgBackfillTask{
			id:              i,
			jobID:           reorgInfo.Job.ID,
			physicalTableID: physicalTableID,
			physicalTable:   phyTbl,
			priority:        reorgInfo.Priority,
			startKey:        startKey,
			endKey:          endKey,
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

const backfillTaskChanSize = 1024

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
	sessCtx.GetSessionVars().StmtCtx.IsDDLJobInQueue = true
	// Set the row encode format version.
	rowFormat := variable.GetDDLReorgRowFormat()
	sessCtx.GetSessionVars().RowEncoder.Enable = rowFormat != variable.DefTiDBRowFormatV1
	// Simulate the sql mode environment in the worker sessionCtx.
	sqlMode := reorgInfo.ReorgMeta.SQLMode
	sessCtx.GetSessionVars().SQLMode = sqlMode
	if err := setSessCtxLocation(sessCtx, reorgInfo); err != nil {
		return nil, errors.Trace(err)
	}
	sessCtx.GetSessionVars().StmtCtx.BadNullAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.TruncateAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.OverflowAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.AllowInvalidDate = sqlMode.HasAllowInvalidDatesMode()
	sessCtx.GetSessionVars().StmtCtx.DividedByZeroAsWarning = !sqlMode.HasStrictMode()
	sessCtx.GetSessionVars().StmtCtx.IgnoreZeroInDate = !sqlMode.HasStrictMode() || sqlMode.HasAllowInvalidDatesMode()
	sessCtx.GetSessionVars().StmtCtx.NoZeroDate = sqlMode.HasStrictMode()
	return sessCtx, nil
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
			backfillCtx := newBackfillCtx(reorgInfo.d, sessCtx, reorgInfo.ReorgMeta.ReorgTp, job.SchemaName, b.tbl)
			idxWorker, err := newAddIndexWorker(b.decodeColMap, i, b.tbl, backfillCtx,
				jc, job.ID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey)
			if err != nil {
				if b.canSkipError(err) {
					continue
				}
				return err
			}
			idxWorker.copReqSenderPool = b.copReqSenderPool
			runner = newBackfillWorker(jc.ddlJobCtx, i, idxWorker)
			worker = idxWorker
		case typeAddIndexMergeTmpWorker:
			backfillCtx := newBackfillCtx(reorgInfo.d, sessCtx, reorgInfo.ReorgMeta.ReorgTp, job.SchemaName, b.tbl)
			tmpIdxWorker := newMergeTempIndexWorker(backfillCtx, i, b.tbl, reorgInfo.currElement.ID, jc)
			runner = newBackfillWorker(jc.ddlJobCtx, i, tmpIdxWorker)
			worker = tmpIdxWorker
		case typeUpdateColumnWorker:
			// Setting InCreateOrAlterStmt tells the difference between SELECT casting and ALTER COLUMN casting.
			sessCtx.GetSessionVars().StmtCtx.InCreateOrAlterStmt = true
			updateWorker := newUpdateColumnWorker(sessCtx, b.tbl, b.decodeColMap, reorgInfo, jc)
			runner = newBackfillWorker(jc.ddlJobCtx, i, updateWorker)
			worker = updateWorker
		case typeCleanUpIndexWorker:
			idxWorker := newCleanUpIndexWorker(sessCtx, b.tbl, b.decodeColMap, reorgInfo, jc)
			runner = newBackfillWorker(jc.ddlJobCtx, i, idxWorker)
			worker = idxWorker
		case typeReorgPartitionWorker:
			partWorker, err := newReorgPartitionWorker(sessCtx, b.tbl, b.decodeColMap, reorgInfo, jc)
			if err != nil {
				return err
			}
			runner = newBackfillWorker(jc.ddlJobCtx, i, partWorker)
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

func (b *backfillScheduler) canSkipError(err error) bool {
	if len(b.workers) > 0 {
		// The error can be skipped because the rest workers can handle the tasks.
		return true
	}
	logutil.BgLogger().Warn("[ddl] create add index backfill worker failed",
		zap.Int("current worker count", len(b.workers)),
		zap.Int64("job ID", b.reorgInfo.ID), zap.Error(err))
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
		kvRanges, err := splitTableRanges(t, reorgInfo.d.store, startKey, endKey)
		if err != nil {
			return errors.Trace(err)
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

		if len(remains) == 0 {
			if ingestBeCtx != nil {
				ingestBeCtx.EngMgr.ResetWorkers(ingestBeCtx, job.ID, reorgInfo.currElement.ID)
			}
			break
		}
		startKey = remains[0].StartKey
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

func addBatchBackfillJobs(sess *session, bfWorkerType backfillerType, reorgInfo *reorgInfo, notDistTask bool,
	batchTasks []*reorgBackfillTask, bJobs []*BackfillJob, isUnique bool, id *int64) error {
	bJobs = bJobs[:0]
	instanceID := ""
	if notDistTask {
		instanceID = reorgInfo.d.uuid
	}
	// TODO: Adjust the number of ranges(region) for each task.
	for _, task := range batchTasks {
		bm := &model.BackfillMeta{
			PhysicalTableID: reorgInfo.PhysicalTableID,
			IsUnique:        isUnique,
			EndInclude:      task.endInclude,
			ReorgTp:         reorgInfo.Job.ReorgMeta.ReorgTp,
			SQLMode:         reorgInfo.ReorgMeta.SQLMode,
			Location:        reorgInfo.ReorgMeta.Location,
			JobMeta: &model.JobMeta{
				SchemaID: reorgInfo.Job.SchemaID,
				TableID:  reorgInfo.Job.TableID,
				Query:    reorgInfo.Job.Query,
			},
		}
		bj := &BackfillJob{
			ID:         *id,
			JobID:      reorgInfo.Job.ID,
			EleID:      reorgInfo.currElement.ID,
			EleKey:     reorgInfo.currElement.TypeKey,
			Tp:         bfWorkerType,
			State:      model.JobStateNone,
			InstanceID: instanceID,
			CurrKey:    task.startKey,
			StartKey:   task.startKey,
			EndKey:     task.endKey,
			Meta:       bm,
		}
		*id++
		bJobs = append(bJobs, bj)
	}
	if err := AddBackfillJobs(sess, bJobs); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (*ddlCtx) splitTableToBackfillJobs(sess *session, reorgInfo *reorgInfo, pTbl table.PhysicalTable, isUnique bool,
	bfWorkerType backfillerType, startKey kv.Key, currBackfillJobID int64) error {
	endKey := reorgInfo.EndKey
	isFirstOps := true
	bJobs := make([]*BackfillJob, 0, genTaskBatch)
	for {
		kvRanges, err := splitTableRanges(pTbl, reorgInfo.d.store, startKey, endKey)
		if err != nil {
			return errors.Trace(err)
		}
		batchTasks := getBatchTasks(pTbl, reorgInfo, kvRanges, genTaskBatch)
		if len(batchTasks) == 0 {
			break
		}
		notNeedDistProcess := isFirstOps && (len(kvRanges) < minDistTaskCnt)
		if err = addBatchBackfillJobs(sess, bfWorkerType, reorgInfo, notNeedDistProcess, batchTasks, bJobs, isUnique, &currBackfillJobID); err != nil {
			return errors.Trace(err)
		}
		isFirstOps = false

		remains := kvRanges[len(batchTasks):]
		// TODO: After adding backfillCh do asyncNotify(dc.backfillJobCh).
		logutil.BgLogger().Info("[ddl] split backfill jobs to the backfill table",
			zap.Int("batchTasksCnt", len(batchTasks)),
			zap.Int("totalRegionCnt", len(kvRanges)),
			zap.Int("remainRegionCnt", len(remains)),
			zap.String("startHandle", hex.EncodeToString(startKey)),
			zap.String("endHandle", hex.EncodeToString(endKey)))

		if len(remains) == 0 {
			break
		}

		for {
			bJobCnt, err := checkBackfillJobCount(sess, reorgInfo.Job.ID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey)
			if err != nil {
				return errors.Trace(err)
			}
			if bJobCnt < minGenTaskBatch {
				break
			}
			time.Sleep(retrySQLInterval)
		}
		startKey = remains[0].StartKey
	}
	return nil
}

func (dc *ddlCtx) controlWritePhysicalTableRecord(sess *session, t table.PhysicalTable, bfWorkerType backfillerType, reorgInfo *reorgInfo) error {
	startKey, endKey := reorgInfo.StartKey, reorgInfo.EndKey
	if startKey == nil && endKey == nil {
		return nil
	}

	if err := dc.isReorgRunnable(reorgInfo.Job.ID); err != nil {
		return errors.Trace(err)
	}

	currBackfillJobID := int64(1)
	err := checkAndHandleInterruptedBackfillJobs(sess, reorgInfo.Job.ID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey)
	if err != nil {
		return errors.Trace(err)
	}
	maxBfJob, err := GetMaxBackfillJob(sess, reorgInfo.Job.ID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey)
	if err != nil {
		return errors.Trace(err)
	}
	if maxBfJob != nil {
		startKey = maxBfJob.EndKey
		currBackfillJobID = maxBfJob.ID + 1
	}

	var isUnique bool
	if bfWorkerType == typeAddIndexWorker {
		idxInfo := model.FindIndexInfoByID(t.Meta().Indices, reorgInfo.currElement.ID)
		isUnique = idxInfo.Unique
	}
	err = dc.splitTableToBackfillJobs(sess, reorgInfo, t, isUnique, bfWorkerType, startKey, currBackfillJobID)
	if err != nil {
		return errors.Trace(err)
	}

	var backfillJobFinished bool
	jobID := reorgInfo.Job.ID
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := dc.isReorgRunnable(reorgInfo.Job.ID); err != nil {
			return errors.Trace(err)
		}

		select {
		case <-ticker.C:
			if !backfillJobFinished {
				err := checkAndHandleInterruptedBackfillJobs(sess, jobID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey)
				if err != nil {
					logutil.BgLogger().Warn("[ddl] finish interrupted backfill jobs", zap.Int64("job ID", jobID), zap.Error(err))
					return errors.Trace(err)
				}

				bfJob, err := getBackfillJobWithRetry(sess, BackfillTable, jobID, reorgInfo.currElement.ID, reorgInfo.currElement.TypeKey, false)
				if err != nil {
					logutil.BgLogger().Info("[ddl] getBackfillJobWithRetry failed", zap.Int64("job ID", jobID), zap.Error(err))
					return errors.Trace(err)
				}
				if bfJob == nil {
					backfillJobFinished = true
					logutil.BgLogger().Info("[ddl] finish backfill jobs", zap.Int64("job ID", jobID))
				}
			}
			if backfillJobFinished {
				// TODO: Consider whether these backfill jobs are always out of sync.
				isSynced, err := checkJobIsSynced(sess, jobID)
				if err != nil {
					logutil.BgLogger().Warn("[ddl] checkJobIsSynced failed", zap.Int64("job ID", jobID), zap.Error(err))
					return errors.Trace(err)
				}
				if isSynced {
					logutil.BgLogger().Info("[ddl] sync backfill jobs", zap.Int64("job ID", jobID))
					return nil
				}
			}
		case <-dc.ctx.Done():
			return dc.ctx.Err()
		}
	}
}

func checkJobIsSynced(sess *session, jobID int64) (bool, error) {
	var err error
	var unsyncedInstanceIDs []string
	for i := 0; i < retrySQLTimes; i++ {
		unsyncedInstanceIDs, err = getUnsyncedInstanceIDs(sess, jobID, "check_backfill_history_job_sync")
		if err == nil && len(unsyncedInstanceIDs) == 0 {
			return true, nil
		}

		logutil.BgLogger().Info("[ddl] checkJobIsSynced failed",
			zap.Strings("unsyncedInstanceIDs", unsyncedInstanceIDs), zap.Int("tryTimes", i), zap.Error(err))
		time.Sleep(retrySQLInterval)
	}

	return false, errors.Trace(err)
}

func checkAndHandleInterruptedBackfillJobs(sess *session, jobID, currEleID int64, currEleKey []byte) (err error) {
	var bJobs []*BackfillJob
	for i := 0; i < retrySQLTimes; i++ {
		bJobs, err = GetInterruptedBackfillJobsForOneEle(sess, jobID, currEleID, currEleKey)
		if err == nil {
			break
		}
		logutil.BgLogger().Info("[ddl] getInterruptedBackfillJobsForOneEle failed", zap.Error(err))
		time.Sleep(retrySQLInterval)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if len(bJobs) == 0 {
		return nil
	}

	for i := 0; i < retrySQLTimes; i++ {
		err = MoveBackfillJobsToHistoryTable(sess, bJobs[0])
		if err == nil {
			return errors.Errorf(bJobs[0].Meta.ErrMsg)
		}
		logutil.BgLogger().Info("[ddl] MoveBackfillJobsToHistoryTable failed", zap.Error(err))
		time.Sleep(retrySQLInterval)
	}
	return errors.Trace(err)
}

func checkBackfillJobCount(sess *session, jobID, currEleID int64, currEleKey []byte) (backfillJobCnt int, err error) {
	err = checkAndHandleInterruptedBackfillJobs(sess, jobID, currEleID, currEleKey)
	if err != nil {
		return 0, errors.Trace(err)
	}

	backfillJobCnt, err = GetBackfillJobCount(sess, BackfillTable, fmt.Sprintf("ddl_job_id = %d and ele_id = %d and ele_key = '%s'",
		jobID, currEleID, currEleKey), "check_backfill_job_count")
	if err != nil {
		return 0, errors.Trace(err)
	}

	return backfillJobCnt, nil
}

func getBackfillJobWithRetry(sess *session, tableName string, jobID, currEleID int64, currEleKey []byte, isDesc bool) (*BackfillJob, error) {
	var err error
	var bJobs []*BackfillJob
	descStr := ""
	if isDesc {
		descStr = "order by id desc"
	}
	for i := 0; i < retrySQLTimes; i++ {
		bJobs, err = GetBackfillJobs(sess, tableName, fmt.Sprintf("ddl_job_id = %d and ele_id = %d and ele_key = '%s' %s limit 1",
			jobID, currEleID, currEleKey, descStr), "check_backfill_job_state")
		if err != nil {
			logutil.BgLogger().Warn("[ddl] GetBackfillJobs failed", zap.Error(err))
			continue
		}

		if len(bJobs) != 0 {
			return bJobs[0], nil
		}
		break
	}
	return nil, errors.Trace(err)
}

// GetMaxBackfillJob gets the max backfill job in BackfillTable and BackfillHistoryTable.
func GetMaxBackfillJob(sess *session, jobID, currEleID int64, currEleKey []byte) (*BackfillJob, error) {
	bfJob, err := getBackfillJobWithRetry(sess, BackfillTable, jobID, currEleID, currEleKey, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hJob, err := getBackfillJobWithRetry(sess, BackfillHistoryTable, jobID, currEleID, currEleKey, true)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if bfJob == nil {
		return hJob, nil
	}
	if hJob == nil {
		return bfJob, nil
	}
	if bfJob.ID > hJob.ID {
		return bfJob, nil
	}
	return hJob, nil
}

// MoveBackfillJobsToHistoryTable moves backfill table jobs to the backfill history table.
func MoveBackfillJobsToHistoryTable(sctx sessionctx.Context, bfJob *BackfillJob) error {
	s, ok := sctx.(*session)
	if !ok {
		return errors.Errorf("sess ctx:%#v convert session failed", sctx)
	}

	return s.runInTxn(func(se *session) error {
		// TODO: Consider batch by batch update backfill jobs and insert backfill history jobs.
		bJobs, err := GetBackfillJobs(se, BackfillTable, fmt.Sprintf("ddl_job_id = %d and ele_id = %d and ele_key = '%s'",
			bfJob.JobID, bfJob.EleID, bfJob.EleKey), "update_backfill_job")
		if err != nil {
			return errors.Trace(err)
		}
		if len(bJobs) == 0 {
			return nil
		}

		txn, err := se.txn()
		if err != nil {
			return errors.Trace(err)
		}
		startTS := txn.StartTS()
		err = RemoveBackfillJob(se, true, bJobs[0])
		if err == nil {
			for _, bj := range bJobs {
				bj.State = model.JobStateCancelled
				bj.FinishTS = startTS
			}
			err = AddBackfillHistoryJob(se, bJobs)
		}
		logutil.BgLogger().Info("[ddl] move backfill jobs to history table", zap.Int("job count", len(bJobs)))
		return errors.Trace(err)
	})
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
