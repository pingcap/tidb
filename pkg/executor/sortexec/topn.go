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
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
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

	finishCh chan struct{} // TODO check this channel in proper position

	chkHeap *topNChunkHeap

	spillHelper *topNSpillHelper // TODO initialize it
	spillAction *topNSpillAction // TODO initialize it
}

// topNChunkHeap implements heap.Interface.
type topNChunkHeap struct {
	byItems []*plannerutil.ByItems

	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc

	// rowChunks is the chunks to store row values.
	rowChunks *chunk.List
	// rowPointer store the chunk index and row index for each row.
	rowPtrs []chunk.RowPtr

	isInitialized bool
	isRowPtrsInit bool

	memTracker *memory.Tracker

	// We record max value row in each spill, so that the heap
	// could filter some useless rows as we only need rows that
	// is smaller than maxRow.
	maxRow *chunk.Row

	totalLimit uint64
	idx        int
}

func (h *topNChunkHeap) init(topnExec *TopNExec, totalLimit uint64, idx int, byItems []*plannerutil.ByItems, keyColumns []int, keyCmpFuncs []chunk.CompareFunc) {
	h.rowChunks = chunk.NewList(exec.RetTypes(topnExec), topnExec.InitCap(), topnExec.MaxChunkSize())
	h.rowChunks.GetMemTracker().AttachTo(h.memTracker)
	h.rowChunks.GetMemTracker().SetLabel(memory.LabelForRowChunks)

	h.byItems = byItems
	h.keyColumns = keyColumns
	h.keyCmpFuncs = keyCmpFuncs

	h.totalLimit = totalLimit
	h.idx = idx
	h.isInitialized = true
}

func (h *topNChunkHeap) initPtrs() {
	h.memTracker.Consume(int64(chunk.RowPtrSize * h.rowChunks.Len()))
	h.initPtrsImpl()
}

func (h *topNChunkHeap) initPtrsImpl() {
	h.rowPtrs = make([]chunk.RowPtr, 0, h.rowChunks.Len())
	for chkIdx := 0; chkIdx < h.rowChunks.NumChunks(); chkIdx++ {
		rowChk := h.rowChunks.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < rowChk.NumRows(); rowIdx++ {
			h.rowPtrs = append(h.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
	h.isRowPtrsInit = true
}

func (h *topNChunkHeap) clear() {
	h.rowChunks.Clear()
	h.memTracker.Consume(int64(-chunk.RowPtrSize * len(h.rowPtrs)))
	h.rowPtrs = nil
	h.isRowPtrsInit = false
	h.isInitialized = false
	h.idx = 0
}

func (h *topNChunkHeap) update(heapMaxRow chunk.Row, newRow chunk.Row) {
	if h.greaterRow(heapMaxRow, newRow) {
		// Evict heap max, keep the next row.
		h.rowPtrs[0] = h.rowChunks.AppendRow(newRow)
		heap.Fix(h, 0)
	}
}

func (h *topNChunkHeap) processChkNoSpill(chk *chunk.Chunk) {
	for i := 0; i < chk.NumRows(); i++ {
		heapMaxRow := h.rowChunks.GetRow(h.rowPtrs[0])
		newRow := chk.GetRow(i)
		h.update(heapMaxRow, newRow)
	}
}

func (h *topNChunkHeap) processChkWithSpill(chk *chunk.Chunk) {
	for i := 0; i < chk.NumRows(); i++ {
		newRow := chk.GetRow(i)

		// Filter some useless rows
		if h.greaterRow(newRow, *h.maxRow) {
			continue
		}

		heapMaxRow := h.rowChunks.GetRow(h.rowPtrs[0])
		h.update(heapMaxRow, newRow)
	}
}

// doCompaction rebuild the chunks and row pointers to release memory.
// If we don't do compaction, in a extreme case like the child data is already ascending sorted
// but we want descending top N, then we will keep all data in memory.
// But if data is distributed randomly, this function will be called log(n) times.
func (h *topNChunkHeap) doCompaction(topnExec *TopNExec) error {
	newRowChunks := chunk.NewList(exec.RetTypes(topnExec), topnExec.InitCap(), topnExec.MaxChunkSize())
	newRowPtrs := make([]chunk.RowPtr, 0, h.rowChunks.Len())
	for _, rowPtr := range h.rowPtrs {
		newRowPtr := newRowChunks.AppendRow(h.rowChunks.GetRow(rowPtr))
		newRowPtrs = append(newRowPtrs, newRowPtr)
	}
	newRowChunks.GetMemTracker().SetLabel(memory.LabelForRowChunks)
	h.memTracker.ReplaceChild(h.rowChunks.GetMemTracker(), newRowChunks.GetMemTracker())
	h.rowChunks = newRowChunks

	h.memTracker.Consume(int64(chunk.RowPtrSize * (len(newRowPtrs) - len(h.rowPtrs))))
	h.rowPtrs = newRowPtrs
	return nil
}

// Less implement heap.Interface, but since we mantains a max heap,
// this function returns true if row i is greater than row j.
func (h *topNChunkHeap) Less(i, j int) bool {
	rowI := h.rowChunks.GetRow(h.rowPtrs[i])
	rowJ := h.rowChunks.GetRow(h.rowPtrs[j])
	return h.greaterRow(rowI, rowJ)
}

func (h *topNChunkHeap) greaterRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range h.keyColumns {
		cmpFunc := h.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if h.byItems[i].Desc {
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

func (h *topNChunkHeap) Len() int {
	return len(h.rowPtrs)
}

func (*topNChunkHeap) Push(any) {
	// Should never be called.
}

func (h *topNChunkHeap) Pop() any {
	h.rowPtrs = h.rowPtrs[:len(h.rowPtrs)-1]
	// We don't need the popped value, return nil to avoid memory allocation.
	return nil
}

func (h *topNChunkHeap) Swap(i, j int) {
	h.rowPtrs[i], h.rowPtrs[j] = h.rowPtrs[j], h.rowPtrs[i]
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

	e.resultChannel = make(chan rowWithError, e.MaxChunkSize())
	return exec.Open(ctx, e.Children(0))
}

// Close implements the Executor Close interface.
func (e *TopNExec) Close() error {
	// TODO implement it
	return nil // TODO
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

func (e *TopNExec) executeTopNWithSpill(ctx context.Context) error {
	err := e.spillHelper.spillHeap(e.chkHeap)
	if err != nil {
		return err
	}

	// Wait for the finish of chunk fetcher
	fetcherWaiter := util.WaitGroupWrapper{}
	// Wait for the finish of all workers
	workersWaiter := util.WaitGroupWrapper{}

	// Fetch chunks from child and put chunks into chunkChannel
	fetcherWaiter.Run(func() {
		e.fetchChunksFromChild(ctx)
	})

	for i := range e.spillHelper.workers {
		// e.spillHelper.workers[i] = newTopNWorker() // TODO
		worker := e.spillHelper.workers[i]
		workersWaiter.Run(func() {
			worker.run()
		})
	}

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

	// TODO we may send result rows in spill mode
	// Send result rows
	rowPtrNum := len(e.chkHeap.rowPtrs)
	for ; e.chkHeap.idx < rowPtrNum; e.chkHeap.idx++ {
		e.resultChannel <- rowWithError{row: e.chkHeap.rowChunks.GetRow(e.chkHeap.rowPtrs[e.chkHeap.idx])}
	}
}
