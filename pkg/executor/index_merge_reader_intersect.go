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
	"context"
	"fmt"
	"runtime/trace"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
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
