// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// intersectionCollectWorker is used to dispatch index-merge-table-task to original workCh and resultCh.
// a kind of interceptor to control the pushed down limit restriction. (should be no performance impact)
type intersectionCollectWorker struct {
	pushedLimit *physicalop.PushedDownLimit
	collectCh   chan *indexMergeTableTask
	limitDone   chan struct{}
}

func (w *intersectionCollectWorker) doIntersectionLimitAndDispatch(ctx context.Context, workCh chan<- *indexMergeTableTask,
	resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	var (
		ok   bool
		task *indexMergeTableTask
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case task, ok = <-w.collectCh:
			if !ok {
				return
			}
			// receive a new intersection task here, adding limit restriction logic
			if w.pushedLimit != nil {
				if w.pushedLimit.Count == 0 {
					// close limitDone channel to notify intersectionProcessWorkers * N to exit.
					close(w.limitDone)
					return
				}
				next, handles := pushedLimitCountingDown(w.pushedLimit, task.handles)
				if next {
					continue
				}
				task.handles = handles
			}
			// dispatch the new task to workCh and resultCh.
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case workCh <- task:
				select {
				case <-ctx.Done():
					return
				case <-finished:
					return
				case resultCh <- task:
				}
			}
		}
	}
}

type intersectionProcessWorker struct {
	// key: parTblIdx, val: HandleMap
	// Value of MemAwareHandleMap is *int to avoid extra Get().
	handleMapsPerWorker map[int]*kv.MemAwareHandleMap[*int]
	workerID            int
	workerCh            chan *indexMergeTableTask
	indexMerge          *IndexMergeReaderExecutor
	memTracker          *memory.Tracker
	batchSize           int

	// When rowDelta == memConsumeBatchSize, Consume(memUsage)
	rowDelta      int64
	mapUsageDelta int64

	partitionIDMap map[int64]int
}

func (w *intersectionProcessWorker) consumeMemDelta() {
	w.memTracker.Consume(w.mapUsageDelta + w.rowDelta*int64(unsafe.Sizeof(int(0))))
	w.mapUsageDelta = 0
	w.rowDelta = 0
}

// doIntersectionPerPartition fetch all the task from workerChannel, and after that, then do the intersection pruning, which
// will cause wasting a lot of time waiting for all the fetch task done.
func (w *intersectionProcessWorker) doIntersectionPerPartition(ctx context.Context, workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished, limitDone <-chan struct{}) {
	failpoint.Inject("testIndexMergePanicPartitionTableIntersectionWorker", nil)
	defer w.memTracker.Detach()

	for task := range w.workerCh {
		var ok bool
		var hMap *kv.MemAwareHandleMap[*int]
		if hMap, ok = w.handleMapsPerWorker[task.parTblIdx]; !ok {
			hMap = kv.NewMemAwareHandleMap[*int]()
			w.handleMapsPerWorker[task.parTblIdx] = hMap
		}
		var mapDelta, rowDelta int64
		for _, h := range task.handles {
			if w.indexMerge.hasGlobalIndex {
				if ph, ok := h.(kv.PartitionHandle); ok {
					if v, exists := w.partitionIDMap[ph.PartitionID]; exists {
						if hMap, ok = w.handleMapsPerWorker[v]; !ok {
							hMap = kv.NewMemAwareHandleMap[*int]()
							w.handleMapsPerWorker[v] = hMap
						}
					}
				} else {
					h = kv.NewPartitionHandle(task.partitionTable.GetPhysicalID(), h)
				}
			}
			// Use *int to avoid Get() again.
			if cntPtr, ok := hMap.Get(h); ok {
				(*cntPtr)++
			} else {
				cnt := 1
				mapDelta += hMap.Set(h, &cnt) + int64(h.ExtraMemSize())
				rowDelta++
			}
		}

		logutil.BgLogger().Debug("intersectionProcessWorker handle tasks", zap.Int("workerID", w.workerID),
			zap.Int("task.handles", len(task.handles)), zap.Int64("rowDelta", rowDelta))

		w.mapUsageDelta += mapDelta
		w.rowDelta += rowDelta
		if w.rowDelta >= int64(w.batchSize) {
			w.consumeMemDelta()
		}
		failpoint.Inject("testIndexMergeIntersectionWorkerPanic", nil)
	}
	if w.rowDelta > 0 {
		w.consumeMemDelta()
	}

	// We assume the result of intersection is small, so no need to track memory.
	intersectedMap := make(map[int][]kv.Handle, len(w.handleMapsPerWorker))
	for parTblIdx, hMap := range w.handleMapsPerWorker {
		hMap.Range(func(h kv.Handle, val *int) bool {
			if *(val) == len(w.indexMerge.partialPlans) {
				// Means all partial paths have this handle.
				intersectedMap[parTblIdx] = append(intersectedMap[parTblIdx], h)
			}
			return true
		})
	}

	tasks := make([]*indexMergeTableTask, 0, len(w.handleMapsPerWorker))
	for parTblIdx, intersected := range intersectedMap {
		// Split intersected[parTblIdx] to avoid task is too large.
		for len(intersected) > 0 {
			length := min(w.batchSize, len(intersected))
			task := &indexMergeTableTask{
				lookupTableTask: lookupTableTask{
					handles: intersected[:length],
					doneCh:  make(chan error, 1),
				},
			}
			intersected = intersected[length:]
			if w.indexMerge.partitionTableMode {
				task.partitionTable = w.indexMerge.prunedPartitions[parTblIdx]
			}
			tasks = append(tasks, task)
			logutil.BgLogger().Debug("intersectionProcessWorker build tasks",
				zap.Int("parTblIdx", parTblIdx), zap.Int("task.handles", len(task.handles)))
		}
	}
	failpoint.Inject("testIndexMergeProcessWorkerIntersectionHang", func(_ failpoint.Value) {
		if resultCh != nil {
			for range cap(resultCh) {
				select {
				case resultCh <- &indexMergeTableTask{}:
				default:
				}
			}
		}
	})
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case <-limitDone:
			// limitDone has signal means the collectWorker has collected enough results, shutdown process workers quickly here.
			return
		case workCh <- task:
			// resultCh != nil means there is no collectWorker, and we should send task to resultCh too by ourselves here.
			if resultCh != nil {
				select {
				case <-ctx.Done():
					return
				case <-finished:
					return
				case resultCh <- task:
				}
			}
		}
	}
}

// for every index merge process worker, it should be feed on a sortedSelectResult for every partial index plan (constructed on all
// table partition ranges results on that index plan path). Since every partial index path is a sorted select result, we can utilize
// K-way merge to accelerate the intersection process.
//
// partialIndexPlan-1 ---> SSR --->  +
// partialIndexPlan-2 ---> SSR --->  + ---> SSR K-way Merge ---> output IndexMergeTableTask
// partialIndexPlan-3 ---> SSR --->  +
// ...                               +
// partialIndexPlan-N ---> SSR --->  +
//
// K-way merge detail: for every partial index plan, output one row as current its representative row. Then, comparing the N representative
// rows together:
//
// Loop start:
//
//	case 1: they are all the same, intersection succeed. --- Record current handle down (already in index order).
//	case 2: distinguish among them, for the minimum value/values corresponded index plan/plans. --- Discard current representative row, fetch next.
//
// goto Loop start:
//
// encapsulate all the recorded handles (already in index order) as index merge table tasks, sending them out.
func (*indexMergeProcessWorker) fetchLoopIntersectionWithOrderBy(_ context.Context, _ <-chan *indexMergeTableTask,
	_ chan<- *indexMergeTableTask, _ chan<- *indexMergeTableTask, _ <-chan struct{}) {
	// todo: pushed sort property with partial index plan and limit.
}

// For each partition(dynamic mode), a map is used to do intersection. Key of the map is handle, and value is the number of times it occurs.
// If the value of handle equals the number of partial paths, it should be sent to final_table_scan_worker.
// To avoid too many goroutines, each intersectionProcessWorker can handle multiple partitions.
func (w *indexMergeProcessWorker) fetchLoopIntersection(ctx context.Context, fetchCh <-chan *indexMergeTableTask,
	workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	defer close(workCh)

	if w.stats != nil {
		start := time.Now()
		defer func() {
			w.stats.IndexMergeProcess += time.Since(start)
		}()
	}

	failpoint.Inject("testIndexMergePanicProcessWorkerIntersection", nil)

	// One goroutine may handle one or multiple partitions.
	// Max number of partition number is 8192, we use ExecutorConcurrency to avoid too many goroutines.
	maxWorkerCnt := w.indexMerge.Ctx().GetSessionVars().IndexMergeIntersectionConcurrency()
	maxChannelSize := atomic.LoadInt32(&LookupTableTaskChannelSize)
	batchSize := w.indexMerge.Ctx().GetSessionVars().IndexLookupSize

	partCnt := 1
	// To avoid multi-threaded access the handle map, we only use one worker for indexMerge with global index.
	if w.indexMerge.partitionTableMode && !w.indexMerge.hasGlobalIndex {
		partCnt = len(w.indexMerge.prunedPartitions)
	}
	workerCnt := min(partCnt, maxWorkerCnt)
	failpoint.Inject("testIndexMergeIntersectionConcurrency", func(val failpoint.Value) {
		con := val.(int)
		if con != workerCnt {
			panic(fmt.Sprintf("unexpected workerCnt, expect %d, got %d", con, workerCnt))
		}
	})

	partitionIDMap := make(map[int64]int)
	if w.indexMerge.hasGlobalIndex {
		for i, p := range w.indexMerge.prunedPartitions {
			partitionIDMap[p.GetPhysicalID()] = i
		}
	}

	workers := make([]*intersectionProcessWorker, 0, workerCnt)
	var collectWorker *intersectionCollectWorker
	wg := util.WaitGroupWrapper{}
	wg2 := util.WaitGroupWrapper{}
	errCh := make(chan bool, workerCnt)
	var limitDone chan struct{}
	if w.indexMerge.pushedLimit != nil {
		// no memory cost for this code logic.
		collectWorker = &intersectionCollectWorker{
			// same size of workCh/resultCh
			collectCh:   make(chan *indexMergeTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize)),
			pushedLimit: w.indexMerge.pushedLimit.Clone(),
			limitDone:   make(chan struct{}),
		}
		limitDone = collectWorker.limitDone
		wg2.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "IndexMergeIntersectionProcessWorker").End()
			collectWorker.doIntersectionLimitAndDispatch(ctx, workCh, resultCh, finished)
		}, handleWorkerPanic(ctx, finished, nil, resultCh, errCh, partTblIntersectionWorkerType))
	}
	for i := range workerCnt {
		tracker := memory.NewTracker(w.indexMerge.ID(), -1)
		tracker.AttachTo(w.indexMerge.memTracker)
		worker := &intersectionProcessWorker{
			workerID:            i,
			handleMapsPerWorker: make(map[int]*kv.MemAwareHandleMap[*int]),
			workerCh:            make(chan *indexMergeTableTask, maxChannelSize),
			indexMerge:          w.indexMerge,
			memTracker:          tracker,
			batchSize:           batchSize,
			partitionIDMap:      partitionIDMap,
		}
		wg.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "IndexMergeIntersectionProcessWorker").End()
			if collectWorker != nil {
				// workflow:
				// intersectionProcessWorker-1  --+                       (limit restriction logic)
				// intersectionProcessWorker-2  --+--------- collectCh--> intersectionCollectWorker +--> workCh --> table worker
				// ...                          --+  <--- limitDone to shut inputs ------+          +-> resultCh --> upper parent
				// intersectionProcessWorker-N  --+
				worker.doIntersectionPerPartition(ctx, collectWorker.collectCh, nil, finished, collectWorker.limitDone)
			} else {
				// workflow:
				// intersectionProcessWorker-1  --------------------------+--> workCh   --> table worker
				// intersectionProcessWorker-2  ---(same as above)        +--> resultCh --> upper parent
				// ...                          ---(same as above)
				// intersectionProcessWorker-N  ---(same as above)
				worker.doIntersectionPerPartition(ctx, workCh, resultCh, finished, nil)
			}
		}, handleWorkerPanic(ctx, finished, limitDone, resultCh, errCh, partTblIntersectionWorkerType))
		workers = append(workers, worker)
	}
	defer func() {
		for _, processWorker := range workers {
			close(processWorker.workerCh)
		}
		wg.Wait()
		// only after all the possible writer closed, can we shut down the collectCh.
		if collectWorker != nil {
			// you don't need to clear the channel before closing it, so discard all the remain tasks.
			close(collectWorker.collectCh)
		}
		wg2.Wait()
	}()
	for {
		var ok bool
		var task *indexMergeTableTask
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case task, ok = <-fetchCh:
			if !ok {
				return
			}
		}

		select {
		case err := <-task.doneCh:
			// If got error from partialIndexWorker/partialTableWorker, stop processing.
			if err != nil {
				syncErr(ctx, finished, resultCh, err)
				return
			}
		default:
		}

		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case workers[task.parTblIdx%workerCnt].workerCh <- task:
		case <-errCh:
			// If got error from intersectionProcessWorker, stop processing.
			return
		}
	}
}

type partialIndexWorker struct {
	stats              *IndexMergeRuntimeStat
	sc                 sessionctx.Context
	idxID              int
	batchSize          int
	maxBatchSize       int
	maxChunkSize       int
	memTracker         *memory.Tracker
	partitionTableMode bool
	prunedPartitions   []table.PhysicalTable
	byItems            []*plannerutil.ByItems
	scannedKeys        uint64
	pushedLimit        *physicalop.PushedDownLimit
	dagPB              *tipb.DAGRequest
	plan               []base.PhysicalPlan
}

func syncErr(ctx context.Context, finished <-chan struct{}, errCh chan<- *indexMergeTableTask, err error) {
	logutil.BgLogger().Error("IndexMergeReaderExecutor.syncErr", zap.Error(err))
	doneCh := make(chan error, 1)
	doneCh <- err
	task := &indexMergeTableTask{
		lookupTableTask: lookupTableTask{
			doneCh: doneCh,
		},
	}

	// ctx.Done and finished is to avoid write channel is stuck.
	select {
	case <-ctx.Done():
		return
	case <-finished:
		return
	case errCh <- task:
		return
	}
}

// needPartitionHandle indicates whether we need create a partitionHandle or not.
// If the schema from planner part contains ExtraPhysTblID,
// we need create a partitionHandle, otherwise create a normal handle.
// In TableRowIDScan, the partitionHandle will be used to create key ranges.
func (w *partialIndexWorker) needPartitionHandle() (bool, error) {
	cols := w.plan[0].Schema().Columns
	outputOffsets := w.dagPB.OutputOffsets
	col := cols[outputOffsets[len(outputOffsets)-1]]

	is := w.plan[0].(*physicalop.PhysicalIndexScan)
	needPartitionHandle := w.partitionTableMode && len(w.byItems) > 0 || is.Index.Global
	hasExtraCol := col.ID == model.ExtraPhysTblID

	// There will be two needPartitionHandle != hasExtraCol situations.
	// Only `needPartitionHandle` == true and `hasExtraCol` == false are not allowed.
	// `ExtraPhysTblID` will be used in `SelectLock` when `needPartitionHandle` == false and `hasExtraCol` == true.
	if needPartitionHandle && !hasExtraCol {
		return needPartitionHandle, errors.Errorf("Internal error, needPartitionHandle != ret")
	}
	return needPartitionHandle, nil
}

func (w *partialIndexWorker) fetchHandles(
	ctx context.Context,
	results []distsql.SelectResult,
	exitCh <-chan struct{},
	fetchCh chan<- *indexMergeTableTask,
	finished <-chan struct{},
	handleCols plannerutil.HandleCols,
	partialPlanIndex int) (count int64, err error) {
	tps := w.getRetTpsForIndexScan(handleCols)
	chk := chunk.NewChunkWithCapacity(tps, w.maxChunkSize)
	for i := 0; i < len(results); {
		start := time.Now()
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, results[i], handleCols)
		if err != nil {
			syncErr(ctx, finished, fetchCh, err)
			return count, err
		}
		if len(handles) == 0 {
			i++
			continue
		}
		count += int64(len(handles))
		task := w.buildTableTask(handles, retChunk, i, partialPlanIndex)
		if w.stats != nil {
			atomic.AddInt64(&w.stats.FetchIdxTime, int64(time.Since(start)))
		}
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-exitCh:
			return count, nil
		case <-finished:
			return count, nil
		case fetchCh <- task:
		}
	}
	return count, nil
}

func (w *partialIndexWorker) getRetTpsForIndexScan(handleCols plannerutil.HandleCols) []*types.FieldType {
	var tps []*types.FieldType
	if len(w.byItems) != 0 {
		for _, item := range w.byItems {
			tps = append(tps, item.Expr.GetType(w.sc.GetExprCtx().GetEvalCtx()))
		}
	}
	tps = append(tps, handleCols.GetFieldsTypes()...)
	if ok, _ := w.needPartitionHandle(); ok {
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}
	return tps
}

func (w *partialIndexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult, handleCols plannerutil.HandleCols) (
	handles []kv.Handle, retChk *chunk.Chunk, err error) {
	handles = make([]kv.Handle, 0, w.batchSize)
	if len(w.byItems) != 0 {
		retChk = chunk.NewChunkWithCapacity(w.getRetTpsForIndexScan(handleCols), w.batchSize)
	}
	var memUsage int64
	var chunkRowOffset int
	defer w.memTracker.Consume(-memUsage)
	for len(handles) < w.batchSize {
		requiredRows := w.batchSize - len(handles)
		if w.pushedLimit != nil {
			if w.pushedLimit.Offset+w.pushedLimit.Count <= w.scannedKeys {
				return handles, retChk, nil
			}
			requiredRows = min(int(w.pushedLimit.Offset+w.pushedLimit.Count-w.scannedKeys), requiredRows)
		}
		chk.SetRequiredRows(requiredRows, w.maxChunkSize)
		start := time.Now()
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return nil, nil, err
		}
		if w.stats != nil && w.idxID != 0 {
			w.sc.GetSessionVars().StmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(w.idxID, false).Record(time.Since(start), chk.NumRows())
		}
		if chk.NumRows() == 0 {
			failpoint.Inject("testIndexMergeErrorPartialIndexWorker", func(v failpoint.Value) {
				failpoint.Return(handles, nil, errors.New(v.(string)))
			})
			return handles, retChk, nil
		}
		memDelta := chk.MemoryUsage()
		memUsage += memDelta
		w.memTracker.Consume(memDelta)
		// We want both to reset chunkRowOffset and keep it up-to-date after the loop
		// nolint:intrange
		for chunkRowOffset = 0; chunkRowOffset < chk.NumRows(); chunkRowOffset++ {
			if w.pushedLimit != nil {
				w.scannedKeys++
				if w.scannedKeys > (w.pushedLimit.Offset + w.pushedLimit.Count) {
					// Skip the handles after Offset+Count.
					break
				}
			}
			var handle kv.Handle
			ok, err1 := w.needPartitionHandle()
			if err1 != nil {
				return nil, nil, err1
			}
			if ok {
				handle, err = handleCols.BuildPartitionHandleFromIndexRow(w.sc.GetSessionVars().StmtCtx, chk.GetRow(chunkRowOffset))
			} else {
				handle, err = handleCols.BuildHandleFromIndexRow(w.sc.GetSessionVars().StmtCtx, chk.GetRow(chunkRowOffset))
			}
			if err != nil {
				return nil, nil, err
			}
			handles = append(handles, handle)
		}
		// used for order by
		if len(w.byItems) != 0 {
			retChk.Append(chk, 0, chunkRowOffset)
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (w *partialIndexWorker) buildTableTask(handles []kv.Handle, retChk *chunk.Chunk, parTblIdx int, partialPlanID int) *indexMergeTableTask {
	task := &indexMergeTableTask{
		lookupTableTask: lookupTableTask{
			handles: handles,
			idxRows: retChk,
		},
		parTblIdx:     parTblIdx,
		partialPlanID: partialPlanID,
	}

	if w.prunedPartitions != nil {
		task.partitionTable = w.prunedPartitions[parTblIdx]
	}

	task.doneCh = make(chan error, 1)
	return task
}

type indexMergeTableScanWorker struct {
	stats          *IndexMergeRuntimeStat
	workCh         <-chan *indexMergeTableTask
	finished       <-chan struct{}
	indexMergeExec *IndexMergeReaderExecutor
	tblPlans       []base.PhysicalPlan

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker
}

func (w *indexMergeTableScanWorker) pickAndExecTask(ctx context.Context, task **indexMergeTableTask) {
	var ok bool
	for {
		waitStart := time.Now()
		select {
		case <-ctx.Done():
			return
		case <-w.finished:
			return
		case *task, ok = <-w.workCh:
			if !ok {
				return
			}
		}
		// Make sure panic failpoint is after fetch task from workCh.
		// Otherwise, cannot send error to task.doneCh.
		failpoint.Inject("testIndexMergePanicTableScanWorker", nil)
		execStart := time.Now()
		err := w.executeTask(ctx, *task)
		if w.stats != nil {
			atomic.AddInt64(&w.stats.WaitTime, int64(execStart.Sub(waitStart)))
			atomic.AddInt64(&w.stats.FetchRow, int64(time.Since(execStart)))
			atomic.AddInt64(&w.stats.TableTaskNum, 1)
		}
		failpoint.Inject("testIndexMergePickAndExecTaskPanic", nil)
		select {
		case <-ctx.Done():
			return
		case <-w.finished:
			return
		case (*task).doneCh <- err:
		}
	}
}

func (*indexMergeTableScanWorker) handleTableScanWorkerPanic(ctx context.Context, finished <-chan struct{}, task **indexMergeTableTask, worker string) func(r any) {
	return func(r any) {
		if r == nil {
			logutil.BgLogger().Debug("worker finish without panic", zap.String("worker", worker))
			return
		}

		err4Panic := errors.Errorf("%s: %v", worker, r)
		logutil.Logger(ctx).Warn(err4Panic.Error())
		if *task != nil {
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case (*task).doneCh <- err4Panic:
				return
			}
		}
	}
}

func (w *indexMergeTableScanWorker) executeTask(ctx context.Context, task *indexMergeTableTask) error {
	tbl := w.indexMergeExec.table
	if w.indexMergeExec.partitionTableMode && task.partitionTable != nil {
		tbl = task.partitionTable
	}
	tableReader, err := w.indexMergeExec.buildFinalTableReader(ctx, tbl, task.handles)
	if err != nil {
		logutil.Logger(ctx).Error("build table reader failed", zap.Error(err))
		return err
	}
	defer func() { terror.Log(exec.Close(tableReader)) }()
	task.memTracker = w.memTracker
	memUsage := int64(cap(task.handles) * 8)
	task.memUsage = memUsage
	task.memTracker.Consume(memUsage)
	handleCnt := len(task.handles)
	task.rows = make([]chunk.Row, 0, handleCnt)
	for {
		chk := exec.TryNewCacheChunk(tableReader)
		err = exec.Next(ctx, tableReader, chk)
		if err != nil {
			logutil.Logger(ctx).Warn("table reader fetch next chunk failed", zap.Error(err))
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		memUsage = chk.MemoryUsage()
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			task.rows = append(task.rows, row)
		}
	}

	if w.indexMergeExec.keepOrder {
		// Because len(outputOffsets) == tableScan.Schema().Len(),
		// so we could use row.GetInt64(idx) to get partition ID here.
		// TODO: We could add plannercore.PartitionHandleCols to unify them.
		physicalTableIDIdx := -1
		for i, c := range w.indexMergeExec.Schema().Columns {
			if c.ID == model.ExtraPhysTblID {
				physicalTableIDIdx = i
				break
			}
		}
		task.rowIdx = make([]int, 0, len(task.rows))
		for _, row := range task.rows {
			handle, err := w.indexMergeExec.handleCols.BuildHandle(w.indexMergeExec.Ctx().GetSessionVars().StmtCtx, row)
			if err != nil {
				return err
			}
			if w.indexMergeExec.partitionTableMode && physicalTableIDIdx != -1 {
				handle = kv.NewPartitionHandle(row.GetInt64(physicalTableIDIdx), handle)
			}
			rowIdx, _ := task.indexOrder.Get(handle)
			task.rowIdx = append(task.rowIdx, rowIdx.(int))
		}
		sort.Sort(task)
	}

	memUsage = int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Row{}))
	task.memUsage += memUsage
	task.memTracker.Consume(memUsage)
	if handleCnt != len(task.rows) && len(w.tblPlans) == 1 {
		return errors.Errorf("handle count %d isn't equal to value count %d", handleCnt, len(task.rows))
	}
	return nil
}

// IndexMergeRuntimeStat record the indexMerge runtime stat
type IndexMergeRuntimeStat struct {
	IndexMergeProcess time.Duration
	FetchIdxTime      int64
	WaitTime          int64
	FetchRow          int64
	TableTaskNum      int64
	Concurrency       int
}

func (e *IndexMergeRuntimeStat) String() string {
	var buf bytes.Buffer
	if e.FetchIdxTime != 0 {
		buf.WriteString(fmt.Sprintf("index_task:{fetch_handle:%s", time.Duration(e.FetchIdxTime)))
		if e.IndexMergeProcess != 0 {
			buf.WriteString(fmt.Sprintf(", merge:%s", e.IndexMergeProcess))
		}
		buf.WriteByte('}')
	}
	if e.FetchRow != 0 {
		if buf.Len() > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, " table_task:{num:%d, concurrency:%d, fetch_row:%s, wait_time:%s}", e.TableTaskNum, e.Concurrency, time.Duration(e.FetchRow), time.Duration(e.WaitTime))
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *IndexMergeRuntimeStat) Clone() execdetails.RuntimeStats {
	newRs := *e
	return &newRs
}

// Merge implements the RuntimeStats interface.
func (e *IndexMergeRuntimeStat) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*IndexMergeRuntimeStat)
	if !ok {
		return
	}
	e.IndexMergeProcess += tmp.IndexMergeProcess
	e.FetchIdxTime += tmp.FetchIdxTime
	e.FetchRow += tmp.FetchRow
	e.WaitTime += e.WaitTime
	e.TableTaskNum += tmp.TableTaskNum
}

// Tp implements the RuntimeStats interface.
func (*IndexMergeRuntimeStat) Tp() int {
	return execdetails.TpIndexMergeRunTimeStats
}
