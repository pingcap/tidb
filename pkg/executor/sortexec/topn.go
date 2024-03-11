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

package sortexec

import (
	"container/heap"
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// TopNExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the table, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopNExec struct {
	SortExec
	Limit *plannercore.PhysicalLimit

	// It's useful when spill is triggered and the fetcher could know when workers finish their works.
	fetcherAndWorkerSyncer *sync.WaitGroup
	resultChannel          chan rowWithError
	chunkChannel           chan *chunk.Chunk

	finishCh chan struct{}

	chkHeap *topNChunkHeap

	spillHelper *topNSpillHelper
	spillAction *topNSpillAction
}

func (e *TopNExec) keyColumnsCompare(i, j chunk.RowPtr) int {
	rowI := e.chkHeap.rowChunks.GetRow(i)
	rowJ := e.chkHeap.rowChunks.GetRow(j)
	return e.compareRow(rowI, rowJ)
}

// Open implements the Executor Open interface.
func (e *TopNExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	e.fetched = &atomic.Bool{}
	e.fetched.Store(false)
	e.chkHeap = &topNChunkHeap{memTracker: e.memTracker}
	e.chkHeap.idx = 0

	e.finishCh = make(chan struct{}, 1)
	e.resultChannel = make(chan rowWithError, e.MaxChunkSize())

	if variable.EnableTmpStorageOnOOM.Load() {
		e.diskTracker = disk.NewTracker(e.ID(), -1)
		e.diskTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.DiskTracker)
		e.fetcherAndWorkerSyncer = &sync.WaitGroup{}

		concurrency := e.Ctx().GetSessionVars().Concurrency.ExecutorConcurrency
		workers := make([]*topNWorker, concurrency)
		for i := range workers {
			chkHeap := &topNChunkHeap{}
			chkHeap.init(e, e.Limit.Offset+e.Limit.Count, int(e.Limit.Offset), e.ByItems, e.keyColumns, e.keyCmpFuncs)
			workers[i] = newTopNWorker(e.fetcherAndWorkerSyncer, e.resultChannel, e.finishCh, chkHeap, e)
		}

		e.spillHelper = newTopNSpillerHelper(
			e.finishCh,
			e.resultChannel,
			e.keyColumnsCompare,
			e.memTracker,
			e.diskTracker,
			exec.RetTypes(e),
			exec.TryNewCacheChunk(e.Children(0)),
			workers,
			concurrency,
		)
		e.spillAction = &topNSpillAction{spillHelper: e.spillHelper}
		e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.spillAction)
	}

	return exec.Open(ctx, e.Children(0))
}

// Close implements the Executor Close interface.，
func (e *TopNExec) Close() error {
	close(e.finishCh)
	if e.fetched.CompareAndSwap(false, true) {
		close(e.Parallel.resultChannel)
		return nil
	}

	if e.chunkChannel != nil {
		for range e.chunkChannel {
			e.fetcherAndWorkerSyncer.Done()
		}
	}

	e.chkHeap = nil
	e.spillHelper = nil
	e.spillAction = nil
	return nil
}

// Next implements the Executor Next interface.
func (e *TopNExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.fetched.CompareAndSwap(false, true) {
		err := e.fetchChunks(ctx)
		if err != nil {
			return err
		}
	}

	if !req.IsFull() {
		numToAppend := req.RequiredRows() - req.NumRows()
		for i := 0; i < numToAppend; i++ {
			row, ok := <-e.resultChannel
			if !ok || row.err != nil {
				return row.err
			}
			req.AppendRow(row.row)
		}
	}
	return nil
}

func (e *TopNExec) fetchChunks(ctx context.Context) error {
	err := e.loadChunksUntilTotalLimit(ctx)
	if err != nil {
		close(e.resultChannel)
		return err
	}
	go e.executeTopN(ctx)
	return nil
}

func (e *TopNExec) loadChunksUntilTotalLimit(ctx context.Context) error {
	e.initCompareFuncs()
	e.buildKeyColumns()
	e.chkHeap.init(e, e.Limit.Offset+e.Limit.Count, int(e.Limit.Offset), e.ByItems, e.keyColumns, e.keyCmpFuncs)
	for uint64(e.chkHeap.rowChunks.Len()) < e.chkHeap.totalLimit {
		srcChk := exec.TryNewCacheChunk(e.Children(0))
		// adjust required rows by total limit
		srcChk.SetRequiredRows(int(e.chkHeap.totalLimit-uint64(e.chkHeap.rowChunks.Len())), e.MaxChunkSize())
		err := exec.Next(ctx, e.Children(0), srcChk)
		if err != nil {
			return err
		}
		if srcChk.NumRows() == 0 {
			break
		}
		e.chkHeap.rowChunks.Add(srcChk)
		if e.spillHelper.isSpillNeeded() {
			break
		}
	}

	e.chkHeap.initPtrs()
	return nil
}

const topNCompactionFactor = 4

func (e *TopNExec) executeTopNNoSpill(ctx context.Context) error {
	childRowChk := exec.TryNewCacheChunk(e.Children(0))
	for {
		if e.spillHelper.isSpillNeeded() {
			return nil
		}

		err := exec.Next(ctx, e.Children(0), childRowChk)
		if err != nil {
			return err
		}

		if childRowChk.NumRows() == 0 {
			break
		}

		e.chkHeap.processChkNoSpill(childRowChk)

		if e.chkHeap.rowChunks.Len() > len(e.chkHeap.rowPtrs)*topNCompactionFactor {
			err = e.chkHeap.doCompaction(e)
			if err != nil {
				return err
			}
		}
	}

	slices.SortFunc(e.chkHeap.rowPtrs, e.keyColumnsCompare)
	return nil
}

func (e *TopNExec) spillRemainingRowsWhenNeeded() error {
	if e.spillHelper.isSpillTriggered() {
		return e.spillHelper.spill()
	}
	return nil
}

func (e *TopNExec) checkSpillAndExecute() error {
	if e.spillHelper.isSpillNeeded() {
		// Wait for the stop of all workers
		e.fetcherAndWorkerSyncer.Wait()
		return e.spillHelper.spill()
	}
	return nil
}

func (e *TopNExec) fetchChunksFromChild(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(e.resultChannel, r)

			e.fetcherAndWorkerSyncer.Wait()
			err := e.spillRemainingRowsWhenNeeded()
			if err != nil {
				e.resultChannel <- rowWithError{err: err}
			}

			close(e.chunkChannel)
		}
	}()

	for {
		chk := exec.TryNewCacheChunk(e.Children(0))
		err := exec.Next(ctx, e.Children(0), chk)
		if err != nil {
			e.resultChannel <- rowWithError{err: err}
			return
		}

		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}

		e.fetcherAndWorkerSyncer.Add(1)
		select {
		case <-e.finishCh:
			e.Parallel.fetcherAndWorkerSyncer.Done()
			return
		case e.chunkChannel <- chk:
		}

		err = e.checkSpillAndExecute()
		if err != nil {
			e.resultChannel <- rowWithError{err: err}
			return
		}
	}
}

func (e *TopNExec) spillHeapInTopNExec() error {
	e.spillHelper.setInSpilling()
	defer e.spillHelper.cond.Broadcast()
	defer e.spillHelper.setNotSpilled()

	err := e.spillHelper.spillHeap(e.chkHeap)
	if err != nil {
		return err
	}
	return nil
}

func (e *TopNExec) executeTopNWithSpill(ctx context.Context) error {
	err := e.spillHeapInTopNExec()
	if err != nil {
		return err
	}

	e.chunkChannel = make(chan *chunk.Chunk, len(e.spillHelper.workers))

	// Wait for the finish of chunk fetcher
	fetcherWaiter := util.WaitGroupWrapper{}
	// Wait for the finish of all workers
	workersWaiter := util.WaitGroupWrapper{}

	// Fetch chunks from child and put chunks into chunkChannel
	fetcherWaiter.Run(func() {
		e.fetchChunksFromChild(ctx)
	})

	for i := range e.spillHelper.workers {
		worker := e.spillHelper.workers[i]
		worker.setChunkChannel(e.chunkChannel)
		workersWaiter.Run(func() {
			worker.run()
		})
	}

	fetcherWaiter.Wait()
	workersWaiter.Wait()
	return nil
}

func (e *TopNExec) executeTopN(ctx context.Context) {
	defer close(e.resultChannel)

	heap.Init(e.chkHeap)
	for uint64(len(e.chkHeap.rowPtrs)) > e.chkHeap.totalLimit {
		// The number of rows we loaded may exceeds total limit, remove greatest rows by Pop.
		heap.Pop(e.chkHeap)
	}

	if err := e.executeTopNNoSpill(ctx); err != nil {
		e.resultChannel <- rowWithError{err: err}
		return
	}

	if e.spillHelper.isSpillNeeded() {
		if err := e.executeTopNWithSpill(ctx); err != nil {
			e.resultChannel <- rowWithError{err: err}
			return
		}
	}

	e.generateTopNResults()
}

// Return true when spill is triggered
func (e *TopNExec) generateTopNResultsWithNoSpill() bool {
	rowPtrNum := len(e.chkHeap.rowPtrs)
	for ; e.chkHeap.idx < rowPtrNum; e.chkHeap.idx++ {
		if e.chkHeap.idx%1000 == 0 && e.spillHelper.isSpillTriggered() {
			return true
		}
		e.resultChannel <- rowWithError{row: e.chkHeap.rowChunks.GetRow(e.chkHeap.rowPtrs[e.chkHeap.idx])}
	}
	return false
}

func (e *TopNExec) generateTopNResultWhenSpillTriggeredOnlyOnce() {

}

func (e *TopNExec) generateTopNResultWhenSpillTriggeredWithMulWayMerge() {

}

func (e *TopNExec) generateTopNResultsWithSpill() {
	inDiskNum := len(e.spillHelper.sortedRowsInDisk)
	if inDiskNum == 0 {
		panic("inDiskNum can't be 0 when we generate result with spill triggered")
	}

	if inDiskNum == 1 {

		return
	}
}

func (e *TopNExec) generateTopNResults() {
	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(e.resultChannel, r)
		}

		close(e.resultChannel)
	}()

	if !e.spillHelper.isSpillTriggered() {
		if !e.generateTopNResultsWithNoSpill() {
			return
		}

		err := e.spillHeapInTopNExec()
		if err != nil {
			e.resultChannel <- rowWithError{err: err}
		}
	}

	e.generateTopNResultsWithSpill()
}
