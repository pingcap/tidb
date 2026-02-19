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
	"container/heap"
	"context"
	"runtime/trace"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

type partialTableWorker struct {
	stats              *IndexMergeRuntimeStat
	sc                 sessionctx.Context
	batchSize          int
	maxBatchSize       int
	maxChunkSize       int
	tableReader        exec.Executor
	memTracker         *memory.Tracker
	partitionTableMode bool
	prunedPartitions   []table.PhysicalTable
	byItems            []*plannerutil.ByItems
	scannedKeys        uint64
	pushedLimit        *physicalop.PushedDownLimit
}

// needPartitionHandle indicates whether we need create a partitionHandle or not.
// If the schema from planner part contains ExtraPhysTblID,
// we need create a partitionHandle, otherwise create a normal handle.
// In TableRowIDScan, the partitionHandle will be used to create key ranges.
func (w *partialTableWorker) needPartitionHandle() (bool, error) {
	cols := w.tableReader.(*TableReaderExecutor).plans[0].Schema().Columns
	outputOffsets := w.tableReader.(*TableReaderExecutor).dagPB.OutputOffsets
	col := cols[outputOffsets[len(outputOffsets)-1]]

	needPartitionHandle := w.partitionTableMode && len(w.byItems) > 0
	hasExtraCol := col.ID == model.ExtraPhysTblID

	// There will be two needPartitionHandle != hasExtraCol situations.
	// Only `needPartitionHandle` == true and `hasExtraCol` == false are not allowed.
	// `ExtraPhysTblID` will be used in `SelectLock` when `needPartitionHandle` == false and `hasExtraCol` == true.
	if needPartitionHandle && !hasExtraCol {
		return needPartitionHandle, errors.Errorf("Internal error, needPartitionHandle != ret")
	}
	return needPartitionHandle, nil
}

func (w *partialTableWorker) fetchHandles(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *indexMergeTableTask,
	finished <-chan struct{}, handleCols plannerutil.HandleCols, parTblIdx int, partialPlanIndex int) (count int64, err error) {
	chk := w.tableReader.NewChunkWithCapacity(w.getRetTpsForTableScan(), w.maxChunkSize, w.maxBatchSize)
	for {
		start := time.Now()
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, handleCols)
		if err != nil {
			syncErr(ctx, finished, fetchCh, err)
			return count, err
		}
		if len(handles) == 0 {
			return count, nil
		}
		count += int64(len(handles))
		task := w.buildTableTask(handles, retChunk, parTblIdx, partialPlanIndex)
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
}

func (w *partialTableWorker) getRetTpsForTableScan() []*types.FieldType {
	return exec.RetTypes(w.tableReader)
}

func (w *partialTableWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, handleCols plannerutil.HandleCols) (
	handles []kv.Handle, retChk *chunk.Chunk, err error) {
	handles = make([]kv.Handle, 0, w.batchSize)
	if len(w.byItems) != 0 {
		retChk = chunk.NewChunkWithCapacity(w.getRetTpsForTableScan(), w.batchSize)
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
		err = errors.Trace(w.tableReader.Next(ctx, chk))
		if err != nil {
			return nil, nil, err
		}
		if w.tableReader != nil && w.tableReader.RuntimeStats() != nil {
			w.tableReader.RuntimeStats().Record(time.Since(start), chk.NumRows())
		}
		if chk.NumRows() == 0 {
			failpoint.Inject("testIndexMergeErrorPartialTableWorker", func(v failpoint.Value) {
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

func (w *partialTableWorker) buildTableTask(handles []kv.Handle, retChk *chunk.Chunk, parTblIdx int, partialPlanID int) *indexMergeTableTask {
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

func (e *IndexMergeReaderExecutor) startIndexMergeTableScanWorker(ctx context.Context, workCh <-chan *indexMergeTableTask) {
	lookupConcurrencyLimit := e.Ctx().GetSessionVars().IndexLookupConcurrency()
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for range lookupConcurrencyLimit {
		worker := &indexMergeTableScanWorker{
			stats:          e.stats,
			workCh:         workCh,
			finished:       e.finished,
			indexMergeExec: e,
			tblPlans:       e.tblPlans,
			memTracker:     e.memTracker,
		}
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			defer trace.StartRegion(ctx, tableScanWorkerType).End()
			var task *indexMergeTableTask
			util.WithRecovery(
				// Note we use the address of `task` as the argument of both `pickAndExecTask` and `handleTableScanWorkerPanic`
				// because `task` is expected to be assigned in `pickAndExecTask`, and this assignment should also be visible
				// in `handleTableScanWorkerPanic` since it will get `doneCh` from `task`. Golang always pass argument by value,
				// so if we don't use the address of `task` as the argument, the assignment to `task` in `pickAndExecTask` is
				// not visible in `handleTableScanWorkerPanic`
				func() { worker.pickAndExecTask(ctx1, &task) },
				worker.handleTableScanWorkerPanic(ctx1, e.finished, &task, tableScanWorkerType),
			)
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
}

func (e *IndexMergeReaderExecutor) buildFinalTableReader(ctx context.Context, tbl table.Table, handles []kv.Handle) (_ exec.Executor, err error) {
	tableReaderExec := &TableReaderExecutor{
		BaseExecutorV2:             exec.NewBaseExecutorV2(e.Ctx().GetSessionVars(), e.Schema(), e.getTablePlanRootID()),
		tableReaderExecutorContext: newTableReaderExecutorContext(e.Ctx()),
		table:                      tbl,
		dagPB:                      e.tableRequest,
		startTS:                    e.startTS,
		txnScope:                   e.txnScope,
		readReplicaScope:           e.readReplicaScope,
		isStaleness:                e.isStaleness,
		columns:                    e.columns,
		plans:                      e.tblPlans,
		netDataSize:                e.dataAvgRowSize * float64(len(handles)),
	}
	tableReaderExec.buildVirtualColumnInfo()
	// Reorder handles because SplitKeyRangesByLocationsWith/WithoutBuckets() requires startKey of kvRanges is ordered.
	// Also it's good for performance.
	tableReader, err := e.dataReaderBuilder.buildTableReaderFromHandles(ctx, tableReaderExec, handles, true)
	if err != nil {
		logutil.Logger(ctx).Error("build table reader from handles failed", zap.Error(err))
		return nil, err
	}
	return tableReader, nil
}

// Next implements Executor Next interface.
func (e *IndexMergeReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.workerStarted {
		if err := e.startWorkers(ctx); err != nil {
			return err
		}
	}

	req.Reset()
	for {
		resultTask, err := e.getResultTask(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if resultTask == nil {
			return nil
		}
		if resultTask.cursor < len(resultTask.rows) {
			numToAppend := min(len(resultTask.rows)-resultTask.cursor, e.MaxChunkSize()-req.NumRows())
			req.AppendRows(resultTask.rows[resultTask.cursor : resultTask.cursor+numToAppend])
			resultTask.cursor += numToAppend
			if req.NumRows() >= e.MaxChunkSize() {
				return nil
			}
		}
	}
}

func (e *IndexMergeReaderExecutor) getResultTask(ctx context.Context) (*indexMergeTableTask, error) {
	failpoint.Inject("testIndexMergeMainReturnEarly", func(_ failpoint.Value) {
		// To make sure processWorker make resultCh to be full.
		// When main goroutine close finished, processWorker may be stuck when writing resultCh.
		time.Sleep(time.Second * 20)
		failpoint.Return(nil, errors.New("failpoint testIndexMergeMainReturnEarly"))
	})
	if e.resultCurr != nil && e.resultCurr.cursor < len(e.resultCurr.rows) {
		return e.resultCurr, nil
	}
	task, ok := <-e.resultCh
	if !ok {
		return nil, nil
	}

	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	case err := <-task.doneCh:
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Release the memory usage of last task before we handle a new task.
	if e.resultCurr != nil {
		e.resultCurr.memTracker.Consume(-e.resultCurr.memUsage)
	}
	e.resultCurr = task
	return e.resultCurr, nil
}

func handleWorkerPanic(ctx context.Context, finished, limitDone <-chan struct{}, ch chan<- *indexMergeTableTask, extraNotifyCh chan bool, worker string) func(r any) {
	return func(r any) {
		if worker == processWorkerType {
			// There is only one processWorker, so it's safe to close here.
			// No need to worry about "close on closed channel" error.
			defer close(ch)
		}
		if r == nil {
			logutil.BgLogger().Debug("worker finish without panic", zap.String("worker", worker))
			return
		}

		if extraNotifyCh != nil {
			extraNotifyCh <- true
		}

		err4Panic := util.GetRecoverError(r)
		logutil.Logger(ctx).Warn(err4Panic.Error())
		doneCh := make(chan error, 1)
		doneCh <- err4Panic
		task := &indexMergeTableTask{
			lookupTableTask: lookupTableTask{
				doneCh: doneCh,
			},
		}
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case <-limitDone:
			// once the process worker recovered from panic, once finding the limitDone signal, actually we can return.
			return
		case ch <- task:
			return
		}
	}
}

// Close implements Exec Close interface.
func (e *IndexMergeReaderExecutor) Close() error {
	if e.stats != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}
	if e.indexUsageReporter != nil {
		for _, p := range e.partialPlans {
			switch p := p[0].(type) {
			case *physicalop.PhysicalTableScan:
				e.indexUsageReporter.ReportCopIndexUsageForHandle(e.table, p.ID())
			case *physicalop.PhysicalIndexScan:
				e.indexUsageReporter.ReportCopIndexUsageForTable(e.table, p.Index.ID, p.ID())
			}
		}
	}
	if e.finished == nil {
		return nil
	}
	close(e.finished)
	e.tblWorkerWg.Wait()
	e.idxWorkerWg.Wait()
	e.processWorkerWg.Wait()
	e.finished = nil
	e.workerStarted = false
	return nil
}

type indexMergeProcessWorker struct {
	indexMerge *IndexMergeReaderExecutor
	stats      *IndexMergeRuntimeStat
}

type rowIdx struct {
	partialID int
	taskID    int
	rowID     int
}

type handleHeap struct {
	// requiredCnt == 0 means need all handles
	requiredCnt uint64
	tracker     *memory.Tracker
	taskMap     map[int][]*indexMergeTableTask

	idx         []rowIdx
	compareFunc []chunk.CompareFunc
	byItems     []*plannerutil.ByItems
}

func (h handleHeap) Len() int {
	return len(h.idx)
}

func (h handleHeap) Less(i, j int) bool {
	rowI := h.taskMap[h.idx[i].partialID][h.idx[i].taskID].idxRows.GetRow(h.idx[i].rowID)
	rowJ := h.taskMap[h.idx[j].partialID][h.idx[j].taskID].idxRows.GetRow(h.idx[j].rowID)

	for i, compFunc := range h.compareFunc {
		cmp := compFunc(rowI, i, rowJ, i)
		if !h.byItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

func (h handleHeap) Swap(i, j int) {
	h.idx[i], h.idx[j] = h.idx[j], h.idx[i]
}

func (h *handleHeap) Push(x any) {
	idx := x.(rowIdx)
	h.idx = append(h.idx, idx)
	if h.tracker != nil {
		h.tracker.Consume(int64(unsafe.Sizeof(h.idx)))
	}
}

func (h *handleHeap) Pop() any {
	idxRet := h.idx[len(h.idx)-1]
	h.idx = h.idx[:len(h.idx)-1]
	if h.tracker != nil {
		h.tracker.Consume(-int64(unsafe.Sizeof(h.idx)))
	}
	return idxRet
}

func (w *indexMergeProcessWorker) NewHandleHeap(taskMap map[int][]*indexMergeTableTask, memTracker *memory.Tracker) *handleHeap {
	compareFuncs := make([]chunk.CompareFunc, 0, len(w.indexMerge.byItems))
	for _, item := range w.indexMerge.byItems {
		keyType := item.Expr.GetType(w.indexMerge.Ctx().GetExprCtx().GetEvalCtx())
		compareFuncs = append(compareFuncs, chunk.GetCompareFunc(keyType))
	}

	requiredCnt := uint64(0)
	if w.indexMerge.pushedLimit != nil {
		// Pre-allocate up to 1024 to avoid oom
		requiredCnt = min(1024, w.indexMerge.pushedLimit.Count+w.indexMerge.pushedLimit.Offset)
	}
	return &handleHeap{
		requiredCnt: requiredCnt,
		tracker:     memTracker,
		taskMap:     taskMap,
		idx:         make([]rowIdx, 0, requiredCnt),
		compareFunc: compareFuncs,
		byItems:     w.indexMerge.byItems,
	}
}

// pruneTableWorkerTaskIdxRows prune idxRows and only keep columns that will be used in byItems.
// e.g. the common handle is (`b`, `c`) and order by with column `c`, we should make column `c` at the first.
func (w *indexMergeProcessWorker) pruneTableWorkerTaskIdxRows(task *indexMergeTableTask) {
	if task.idxRows == nil {
		return
	}
	// IndexScan no need to prune retChk, Columns required by byItems are always first.
	if plan, ok := w.indexMerge.partialPlans[task.partialPlanID][0].(*physicalop.PhysicalTableScan); ok {
		prune := make([]int, 0, len(w.indexMerge.byItems))
		for _, item := range plan.ByItems {
			c, _ := item.Expr.(*expression.Column)
			idx := plan.Schema().ColumnIndex(c)
			// couldn't equals to -1 here, if idx == -1, just let it panic
			prune = append(prune, idx)
		}
		task.idxRows = task.idxRows.Prune(prune)
	}
}

func (w *indexMergeProcessWorker) fetchLoopUnionWithOrderBy(ctx context.Context, fetchCh <-chan *indexMergeTableTask,
	workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	memTracker := memory.NewTracker(w.indexMerge.ID(), -1)
	memTracker.AttachTo(w.indexMerge.memTracker)
	defer memTracker.Detach()
	defer close(workCh)

	if w.stats != nil {
		start := time.Now()
		defer func() {
			w.stats.IndexMergeProcess += time.Since(start)
		}()
	}

	distinctHandles := kv.NewHandleMap()
	taskMap := make(map[int][]*indexMergeTableTask)
	uselessMap := make(map[int]struct{})
	taskHeap := w.NewHandleHeap(taskMap, memTracker)

	for task := range fetchCh {
		select {
		case err := <-task.doneCh:
			// If got error from partialIndexWorker/partialTableWorker, stop processing.
			if err != nil {
				syncErr(ctx, finished, resultCh, err)
				return
			}
		default:
		}
		if _, ok := uselessMap[task.partialPlanID]; ok {
			continue
		}
		if _, ok := taskMap[task.partialPlanID]; !ok {
			taskMap[task.partialPlanID] = make([]*indexMergeTableTask, 0, 1)
		}
		w.pruneTableWorkerTaskIdxRows(task)
		taskMap[task.partialPlanID] = append(taskMap[task.partialPlanID], task)
		for i, h := range task.handles {
			if _, ok := distinctHandles.Get(h); !ok {
				distinctHandles.Set(h, true)
				heap.Push(taskHeap, rowIdx{task.partialPlanID, len(taskMap[task.partialPlanID]) - 1, i})
				if int(taskHeap.requiredCnt) != 0 && taskHeap.Len() > int(taskHeap.requiredCnt) {
					top := heap.Pop(taskHeap).(rowIdx)
					if top.partialID == task.partialPlanID && top.taskID == len(taskMap[task.partialPlanID])-1 && top.rowID == i {
						uselessMap[task.partialPlanID] = struct{}{}
						task.handles = task.handles[:i]
						break
					}
				}
			}
			memTracker.Consume(int64(h.MemUsage()))
		}
		memTracker.Consume(task.idxRows.MemoryUsage())
		if len(uselessMap) == len(w.indexMerge.partialPlans) {
			// consume reset tasks
			go func() {
				channel.Clear(fetchCh)
			}()
			break
		}
	}

	needCount := taskHeap.Len()
	if w.indexMerge.pushedLimit != nil {
		needCount = max(0, taskHeap.Len()-int(w.indexMerge.pushedLimit.Offset))
	}
	if needCount == 0 {
		return
	}
	fhs := make([]kv.Handle, needCount)
	for i := needCount - 1; i >= 0; i-- {
		idx := heap.Pop(taskHeap).(rowIdx)
		fhs[i] = taskMap[idx.partialID][idx.taskID].handles[idx.rowID]
	}

	batchSize := w.indexMerge.Ctx().GetSessionVars().IndexLookupSize
	tasks := make([]*indexMergeTableTask, 0, len(fhs)/batchSize+1)
	for len(fhs) > 0 {
		l := min(len(fhs), batchSize)
		// Save the index order.
		indexOrder := kv.NewHandleMap()
		for i, h := range fhs[:l] {
			indexOrder.Set(h, i)
		}
		tasks = append(tasks, &indexMergeTableTask{
			lookupTableTask: lookupTableTask{
				handles:    fhs[:l],
				indexOrder: indexOrder,
				doneCh:     make(chan error, 1),
			},
		})
		fhs = fhs[l:]
	}
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case resultCh <- task:
			failpoint.Inject("testCancelContext", func() {
				IndexMergeCancelFuncForTest()
			})
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case workCh <- task:
				continue
			}
		}
	}
}

func pushedLimitCountingDown(pushedLimit *physicalop.PushedDownLimit, handles []kv.Handle) (next bool, res []kv.Handle) {
	fhsLen := uint64(len(handles))
	// The number of handles is less than the offset, discard all handles.
	if fhsLen <= pushedLimit.Offset {
		pushedLimit.Offset -= fhsLen
		return true, nil
	}
	handles = handles[pushedLimit.Offset:]
	pushedLimit.Offset = 0

	fhsLen = uint64(len(handles))
	// The number of handles is greater than the limit, only keep limit count.
	if fhsLen > pushedLimit.Count {
		handles = handles[:pushedLimit.Count]
	}
	pushedLimit.Count -= min(pushedLimit.Count, fhsLen)
	return false, handles
}

func (w *indexMergeProcessWorker) fetchLoopUnion(ctx context.Context, fetchCh <-chan *indexMergeTableTask,
	workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	failpoint.Inject("testIndexMergeResultChCloseEarly", func(_ failpoint.Value) {
		failpoint.Return()
	})
	memTracker := memory.NewTracker(w.indexMerge.ID(), -1)
	memTracker.AttachTo(w.indexMerge.memTracker)
	defer memTracker.Detach()
	defer close(workCh)
	failpoint.Inject("testIndexMergePanicProcessWorkerUnion", nil)

	var pushedLimit *physicalop.PushedDownLimit
	if w.indexMerge.pushedLimit != nil {
		pushedLimit = w.indexMerge.pushedLimit.Clone()
	}
	hMap := kv.NewHandleMap()
	for {
		var ok bool
		var task *indexMergeTableTask
		if pushedLimit != nil && pushedLimit.Count == 0 {
			return
		}
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
		start := time.Now()
		handles := task.handles
		fhs := make([]kv.Handle, 0, 8)

		memTracker.Consume(int64(cap(task.handles) * 8))
		for _, h := range handles {
			if w.indexMerge.partitionTableMode {
				if _, ok := h.(kv.PartitionHandle); !ok {
					h = kv.NewPartitionHandle(task.partitionTable.GetPhysicalID(), h)
				}
			}
			if _, ok := hMap.Get(h); !ok {
				fhs = append(fhs, h)
				hMap.Set(h, true)
			}
		}
		if len(fhs) == 0 {
			continue
		}
		if pushedLimit != nil {
			next, res := pushedLimitCountingDown(pushedLimit, fhs)
			if next {
				continue
			}
			fhs = res
		}
		task = &indexMergeTableTask{
			lookupTableTask: lookupTableTask{
				handles: fhs,
				doneCh:  make(chan error, 1),

				partitionTable: task.partitionTable,
			},
		}
		if w.stats != nil {
			w.stats.IndexMergeProcess += time.Since(start)
		}
		failpoint.Inject("testIndexMergeProcessWorkerUnionHang", func(_ failpoint.Value) {
			for range cap(resultCh) {
				select {
				case resultCh <- &indexMergeTableTask{}:
				default:
				}
			}
		})
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case resultCh <- task:
			failpoint.Inject("testCancelContext", func() {
				IndexMergeCancelFuncForTest()
			})
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case workCh <- task:
			}
		}
	}
}
