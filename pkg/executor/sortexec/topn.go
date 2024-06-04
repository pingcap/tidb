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
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
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

	// Normally, heap will be stored in memory after it has been built.
	// However, other executors may trigger topn spill after the heap is built
	// and inMemoryThenSpillFlag will be set to true at this time.
	inMemoryThenSpillFlag bool

	// Topn executor has two stage:
	//  1. Building heap, in this stage all received rows will be inserted into heap.
	//  2. Updating heap, in this stage only rows that is smaller than the heap top could be inserted and we will drop the heap top.
	//
	// This variable is only used for test.
	isSpillTriggeredInStage1ForTest bool
	isSpillTriggeredInStage2ForTest bool

	Concurrency int
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
	e.chunkChannel = make(chan *chunk.Chunk, e.Concurrency)
	e.inMemoryThenSpillFlag = false
	e.isSpillTriggeredInStage1ForTest = false
	e.isSpillTriggeredInStage2ForTest = false

	if variable.EnableTmpStorageOnOOM.Load() {
		e.diskTracker = disk.NewTracker(e.ID(), -1)
		diskTracker := e.Ctx().GetSessionVars().StmtCtx.DiskTracker
		if diskTracker != nil {
			e.diskTracker.AttachTo(diskTracker)
		}
		e.fetcherAndWorkerSyncer = &sync.WaitGroup{}

		workers := make([]*topNWorker, e.Concurrency)
		for i := range workers {
			chkHeap := &topNChunkHeap{}
			// Offset of heap in worker should be 0, as we need to spill all data
			chkHeap.init(e, e.memTracker, e.Limit.Offset+e.Limit.Count, 0, e.greaterRow, e.RetFieldTypes())
			workers[i] = newTopNWorker(i, e.chunkChannel, e.fetcherAndWorkerSyncer, e.resultChannel, e.finishCh, e, chkHeap, e.memTracker)
		}

		e.spillHelper = newTopNSpillerHelper(
			e,
			e.finishCh,
			e.resultChannel,
			e.memTracker,
			e.diskTracker,
			exec.RetTypes(e),
			workers,
			e.Concurrency,
		)
		e.spillAction = &topNSpillAction{spillHelper: e.spillHelper}
		e.Ctx().GetSessionVars().MemTracker.FallbackOldAndSetNewAction(e.spillAction)
	}

	return exec.Open(ctx, e.Children(0))
}

// Close implements the Executor Close interface.
func (e *TopNExec) Close() error {
	// `e.finishCh == nil` means that `Open` is not called.
	if e.finishCh == nil {
		return exec.Close(e.Children(0))
	}

	close(e.finishCh)
	if e.fetched.CompareAndSwap(false, true) {
		close(e.resultChannel)
		return exec.Close(e.Children(0))
	}

	// Wait for the finish of all tasks
	channel.Clear(e.resultChannel)

	e.chkHeap = nil
	e.spillAction = nil

	if e.spillHelper != nil {
		e.spillHelper.close()
		e.spillHelper = nil
	}

	if e.memTracker != nil {
		e.memTracker.ReplaceBytesUsed(0)
	}

	return exec.Close(e.Children(0))
}

func (e *TopNExec) greaterRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp > 0 {
			return true
		} else if cmp < 0 {
			return false
		}
	}
	return false
}

// Next implements the Executor Next interface.
//
// The following picture shows the procedure of topn when spill is triggered.
/*
Spill Stage:
                        ┌─────────┐
                        │  Child  │
                        └────▲────┘
                             │
                           Fetch
                             │
                     ┌───────┴───────┐
                     │ Chunk Fetcher │
                     └───────┬───────┘
                             │
                             │
                             ▼
                        Check Spill──────►Spill Triggered─────────►Spill
                             │                                       │
                             ▼                                       │
                     Spill Not Triggered                             │
                             │                                       │
                             ▼                                       │
                         Push Chunk◄─────────────────────────────────┘
                             │
                             ▼
        ┌────────────────►Channel◄───────────────────┐
        │                    ▲                       │
        │                    │                       │
      Fetch                Fetch                   Fetch
        │                    │                       │
   ┌────┴───┐            ┌───┴────┐              ┌───┴────┐
   │ Worker │            │ Worker │   ......     │ Worker │
   └────┬───┘            └───┬────┘              └───┬────┘
        │                    │                       │
        │                    │                       │
        │                    ▼                       │
        └───────────► Multi-way Merge◄───────────────┘
                             │
                             │
                             ▼
                          Output

Restore Stage:
   ┌────────┐            ┌────────┐              ┌────────┐
   │  Heap  │            │  Heap  │   ......     │  Heap  │
   └────┬───┘            └───┬────┘              └───┬────┘
        │                    │                       │
        │                    │                       │
        │                    ▼                       │
        └───────────► Multi-way Merge◄───────────────┘
                             │
                             │
                             ▼
                          Output

*/
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
	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(e.resultChannel, r)
			close(e.resultChannel)
		}
	}()

	err := e.loadChunksUntilTotalLimit(ctx)
	if err != nil {
		close(e.resultChannel)
		return err
	}
	go e.executeTopN(ctx)
	return nil
}

func (e *TopNExec) loadChunksUntilTotalLimit(ctx context.Context) error {
	e.initCompareFuncs(e.Ctx().GetExprCtx().GetEvalCtx())
	e.buildKeyColumns()
	e.chkHeap.init(e, e.memTracker, e.Limit.Offset+e.Limit.Count, int(e.Limit.Offset), e.greaterRow, e.RetFieldTypes())
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
			e.isSpillTriggeredInStage1ForTest = true
			break
		}

		injectTopNRandomFail(1)
	}

	e.chkHeap.initPtrs()
	return nil
}

const topNCompactionFactor = 4

func (e *TopNExec) executeTopNWhenNoSpillTriggered(ctx context.Context) error {
	if e.spillHelper.isSpillNeeded() {
		e.isSpillTriggeredInStage2ForTest = true
		return nil
	}

	childRowChk := exec.TryNewCacheChunk(e.Children(0))
	for {
		if e.spillHelper.isSpillNeeded() {
			e.isSpillTriggeredInStage2ForTest = true
			return nil
		}

		err := exec.Next(ctx, e.Children(0), childRowChk)
		if err != nil {
			return err
		}

		if childRowChk.NumRows() == 0 {
			break
		}

		e.chkHeap.processChk(childRowChk)

		if e.chkHeap.rowChunks.Len() > len(e.chkHeap.rowPtrs)*topNCompactionFactor {
			err = e.chkHeap.doCompaction(e)
			if err != nil {
				return err
			}
		}
		injectTopNRandomFail(10)
	}

	slices.SortFunc(e.chkHeap.rowPtrs, e.chkHeap.keyColumnsCompare)
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
		}

		e.fetcherAndWorkerSyncer.Wait()
		err := e.spillRemainingRowsWhenNeeded()
		if err != nil {
			e.resultChannel <- rowWithError{err: err}
		}

		close(e.chunkChannel)
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
			e.fetcherAndWorkerSyncer.Done()
			return
		case e.chunkChannel <- chk:
		}

		injectTopNRandomFail(10)

		err = e.checkSpillAndExecute()
		if err != nil {
			e.resultChannel <- rowWithError{err: err}
			return
		}
	}
}

// Spill the heap which is in TopN executor
func (e *TopNExec) spillTopNExecHeap() error {
	e.spillHelper.setInSpilling()
	defer e.spillHelper.cond.Broadcast()
	defer e.spillHelper.setNotSpilled()

	err := e.spillHelper.spillHeap(e.chkHeap)
	if err != nil {
		return err
	}
	return nil
}

func (e *TopNExec) executeTopNWhenSpillTriggered(ctx context.Context) error {
	// idx need to be set to 0 as we need to spill all data
	e.chkHeap.idx = 0
	err := e.spillTopNExecHeap()
	if err != nil {
		return err
	}

	// Wait for the finish of chunk fetcher
	fetcherWaiter := util.WaitGroupWrapper{}
	// Wait for the finish of all workers
	workersWaiter := util.WaitGroupWrapper{}

	for i := range e.spillHelper.workers {
		worker := e.spillHelper.workers[i]
		worker.initWorker()
		workersWaiter.Run(func() {
			worker.run()
		})
	}

	// Fetch chunks from child and put chunks into chunkChannel
	fetcherWaiter.Run(func() {
		e.fetchChunksFromChild(ctx)
	})

	fetcherWaiter.Wait()
	workersWaiter.Wait()
	return nil
}

func (e *TopNExec) executeTopN(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			processPanicAndLog(e.resultChannel, r)
		}

		close(e.resultChannel)
	}()

	heap.Init(e.chkHeap)
	for uint64(len(e.chkHeap.rowPtrs)) > e.chkHeap.totalLimit {
		// The number of rows we loaded may exceeds total limit, remove greatest rows by Pop.
		heap.Pop(e.chkHeap)
	}

	if err := e.executeTopNWhenNoSpillTriggered(ctx); err != nil {
		e.resultChannel <- rowWithError{err: err}
		return
	}

	if e.spillHelper.isSpillNeeded() {
		if err := e.executeTopNWhenSpillTriggered(ctx); err != nil {
			e.resultChannel <- rowWithError{err: err}
			return
		}
	}

	e.generateTopNResults()
}

// Return true when spill is triggered
func (e *TopNExec) generateTopNResultsWhenNoSpillTriggered() bool {
	rowPtrNum := len(e.chkHeap.rowPtrs)
	for ; e.chkHeap.idx < rowPtrNum; e.chkHeap.idx++ {
		if e.chkHeap.idx%10 == 0 && e.spillHelper.isSpillNeeded() {
			return true
		}
		e.resultChannel <- rowWithError{row: e.chkHeap.rowChunks.GetRow(e.chkHeap.rowPtrs[e.chkHeap.idx])}
	}
	return false
}

func (e *TopNExec) generateResultWithMultiWayMerge(offset int64, limit int64) error {
	multiWayMerge := newMultiWayMerger(&diskSource{sortedRowsInDisk: e.spillHelper.sortedRowsInDisk}, e.lessRow)

	err := multiWayMerge.init()
	if err != nil {
		return err
	}

	outputRowNum := int64(0)
	for {
		if outputRowNum >= limit {
			return nil
		}

		row, err := multiWayMerge.next()
		if err != nil {
			return err
		}

		if row.IsEmpty() {
			return nil
		}

		if outputRowNum >= offset {
			select {
			case <-e.finishCh:
				return nil
			case e.resultChannel <- rowWithError{row: row}:
			}
		}
		outputRowNum++
		injectParallelSortRandomFail(1)
	}
}

// GenerateTopNResultsWhenSpillOnlyOnce generates results with this function when we trigger spill only once.
// It's a public function as we need to test it in ut.
func (e *TopNExec) GenerateTopNResultsWhenSpillOnlyOnce() error {
	inDisk := e.spillHelper.sortedRowsInDisk[0]
	chunkNum := inDisk.NumChunks()
	skippedRowNum := uint64(0)
	offset := e.Limit.Offset
	for i := 0; i < chunkNum; i++ {
		chk, err := inDisk.GetChunk(i)
		if err != nil {
			return err
		}

		injectTopNRandomFail(10)

		rowNum := chk.NumRows()
		j := 0
		if !e.inMemoryThenSpillFlag {
			// When e.inMemoryThenSpillFlag == false, we need to manually set j
			// because rows that should be ignored before offset have also been
			// spilled to disk.
			if skippedRowNum < offset {
				rowNumNeedSkip := offset - skippedRowNum
				if rowNum <= int(rowNumNeedSkip) {
					// All rows in this chunk should be skipped
					skippedRowNum += uint64(rowNum)
					continue
				}
				j += int(rowNumNeedSkip)
				skippedRowNum += rowNumNeedSkip
			}
		}

		for ; j < rowNum; j++ {
			select {
			case <-e.finishCh:
				return nil
			case e.resultChannel <- rowWithError{row: chk.GetRow(j)}:
			}
		}
	}
	return nil
}

func (e *TopNExec) generateTopNResultsWhenSpillTriggered() error {
	inDiskNum := len(e.spillHelper.sortedRowsInDisk)
	if inDiskNum == 0 {
		panic("inDiskNum can't be 0 when we generate result with spill triggered")
	}

	if inDiskNum == 1 {
		return e.GenerateTopNResultsWhenSpillOnlyOnce()
	}
	return e.generateResultWithMultiWayMerge(int64(e.Limit.Offset), int64(e.Limit.Offset+e.Limit.Count))
}

func (e *TopNExec) generateTopNResults() {
	if !e.spillHelper.isSpillTriggered() {
		if !e.generateTopNResultsWhenNoSpillTriggered() {
			return
		}

		err := e.spillTopNExecHeap()
		if err != nil {
			e.resultChannel <- rowWithError{err: err}
		}

		e.inMemoryThenSpillFlag = true
	}

	err := e.generateTopNResultsWhenSpillTriggered()
	if err != nil {
		e.resultChannel <- rowWithError{err: err}
	}
}

// IsSpillTriggeredForTest shows if spill is triggered, used for test.
func (e *TopNExec) IsSpillTriggeredForTest() bool {
	return e.spillHelper.isSpillTriggered()
}

// GetIsSpillTriggeredInStage1ForTest shows if spill is triggered in stage 1, only used for test.
func (e *TopNExec) GetIsSpillTriggeredInStage1ForTest() bool {
	return e.isSpillTriggeredInStage1ForTest
}

// GetIsSpillTriggeredInStage2ForTest shows if spill is triggered in stage 2, only used for test.
func (e *TopNExec) GetIsSpillTriggeredInStage2ForTest() bool {
	return e.isSpillTriggeredInStage2ForTest
}

// GetInMemoryThenSpillFlagForTest shows if results are in memory before they are spilled, only used for test
func (e *TopNExec) GetInMemoryThenSpillFlagForTest() bool {
	return e.inMemoryThenSpillFlag
}

func injectTopNRandomFail(triggerFactor int32) {
	failpoint.Inject("TopNRandomFail", func(val failpoint.Value) {
		if val.(bool) {
			randNum := rand.Int31n(10000)
			if randNum < triggerFactor {
				panic("panic is triggered by random fail")
			}
		}
	})
}

// InitTopNExecForTest initializes TopN executors, only for test.
func InitTopNExecForTest(topnExec *TopNExec, offset uint64, sortedRowsInDisk *chunk.DataInDiskByChunks) {
	topnExec.inMemoryThenSpillFlag = false
	topnExec.finishCh = make(chan struct{}, 1)
	topnExec.resultChannel = make(chan rowWithError, 10000)
	topnExec.Limit.Offset = offset
	topnExec.spillHelper = &topNSpillHelper{}
	topnExec.spillHelper.sortedRowsInDisk = []*chunk.DataInDiskByChunks{sortedRowsInDisk}
}

// GetResultForTest gets result, only for test.
func GetResultForTest(topnExec *TopNExec) []int64 {
	close(topnExec.resultChannel)
	result := make([]int64, 0, 100)
	for {
		row, ok := <-topnExec.resultChannel
		if !ok {
			return result
		}
		result = append(result, row.row.GetInt64(0))
	}
}
