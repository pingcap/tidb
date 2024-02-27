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
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// TopNExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the table, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopNExec struct {
	SortExec
	Limit      *plannercore.PhysicalLimit
	totalLimit uint64

	chkHeap *topNChunkHeap
}

// topNChunkHeap implements heap.Interface.
type topNChunkHeap struct {
	*TopNExec

	// rowChunks is the chunks to store row values.
	rowChunks *chunk.List
	// rowPointer store the chunk index and row index for each row.
	rowPtrs []chunk.RowPtr

	Idx int
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
		if h.ByItems[i].Desc {
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

func (e *TopNExec) initPointers() {
	e.chkHeap.rowPtrs = make([]chunk.RowPtr, 0, e.chkHeap.rowChunks.Len())
	e.memTracker.Consume(int64(8 * e.chkHeap.rowChunks.Len()))
	for chkIdx := 0; chkIdx < e.chkHeap.rowChunks.NumChunks(); chkIdx++ {
		rowChk := e.chkHeap.rowChunks.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < rowChk.NumRows(); rowIdx++ {
			e.chkHeap.rowPtrs = append(e.chkHeap.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
}

// Open implements the Executor Open interface.
func (e *TopNExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)

	e.fetched = &atomic.Bool{}
	e.fetched.Store(false)
	e.chkHeap = &topNChunkHeap{TopNExec: e}
	e.chkHeap.Idx = 0

	return exec.Open(ctx, e.Children(0))
}

// Next implements the Executor Next interface.
func (e *TopNExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.fetched.CompareAndSwap(false, true) {
		e.totalLimit = e.Limit.Offset + e.Limit.Count
		e.chkHeap.Idx = int(e.Limit.Offset)
		err := e.loadChunksUntilTotalLimit(ctx)
		if err != nil {
			return err
		}
		err = e.executeTopN(ctx)
		if err != nil {
			return err
		}
	}
	if e.chkHeap.Idx >= len(e.chkHeap.rowPtrs) {
		return nil
	}
	if !req.IsFull() {
		numToAppend := min(len(e.chkHeap.rowPtrs)-e.chkHeap.Idx, req.RequiredRows()-req.NumRows())
		rows := make([]chunk.Row, numToAppend)
		for index := 0; index < numToAppend; index++ {
			rows[index] = e.chkHeap.rowChunks.GetRow(e.chkHeap.rowPtrs[e.chkHeap.Idx])
			e.chkHeap.Idx++
		}
		req.AppendRows(rows)
	}
	return nil
}

func (e *TopNExec) loadChunksUntilTotalLimit(ctx context.Context) error {
	e.chkHeap.rowChunks = chunk.NewList(exec.RetTypes(e), e.InitCap(), e.MaxChunkSize())
	e.chkHeap.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.chkHeap.rowChunks.GetMemTracker().SetLabel(memory.LabelForRowChunks)
	for uint64(e.chkHeap.rowChunks.Len()) < e.totalLimit {
		srcChk := exec.TryNewCacheChunk(e.Children(0))
		// adjust required rows by total limit
		srcChk.SetRequiredRows(int(e.totalLimit-uint64(e.chkHeap.rowChunks.Len())), e.MaxChunkSize())
		err := exec.Next(ctx, e.Children(0), srcChk)
		if err != nil {
			return err
		}
		if srcChk.NumRows() == 0 {
			break
		}
		e.chkHeap.rowChunks.Add(srcChk)
	}
	e.initPointers()
	e.initCompareFuncs()
	e.buildKeyColumns()
	return nil
}

const topNCompactionFactor = 4

func (e *TopNExec) executeTopN(ctx context.Context) error {
	heap.Init(e.chkHeap)
	for uint64(len(e.chkHeap.rowPtrs)) > e.totalLimit {
		// The number of rows we loaded may exceeds total limit, remove greatest rows by Pop.
		heap.Pop(e.chkHeap)
	}
	childRowChk := exec.TryNewCacheChunk(e.Children(0))
	for {
		err := exec.Next(ctx, e.Children(0), childRowChk)
		if err != nil {
			return err
		}
		if childRowChk.NumRows() == 0 {
			break
		}
		err = e.processChildChk(childRowChk)
		if err != nil {
			return err
		}
		if e.chkHeap.rowChunks.Len() > len(e.chkHeap.rowPtrs)*topNCompactionFactor {
			err = e.doCompaction(e.chkHeap)
			if err != nil {
				return err
			}
		}
	}
	slices.SortFunc(e.chkHeap.rowPtrs, e.keyColumnsCompare)
	return nil
}

func (e *TopNExec) processChildChk(childRowChk *chunk.Chunk) error {
	for i := 0; i < childRowChk.NumRows(); i++ {
		heapMaxPtr := e.chkHeap.rowPtrs[0]
		var heapMax, next chunk.Row
		heapMax = e.chkHeap.rowChunks.GetRow(heapMaxPtr)
		next = childRowChk.GetRow(i)
		if e.chkHeap.greaterRow(heapMax, next) {
			// Evict heap max, keep the next row.
			e.chkHeap.rowPtrs[0] = e.chkHeap.rowChunks.AppendRow(childRowChk.GetRow(i))
			heap.Fix(e.chkHeap, 0)
		}
	}
	return nil
}

// doCompaction rebuild the chunks and row pointers to release memory.
// If we don't do compaction, in a extreme case like the child data is already ascending sorted
// but we want descending top N, then we will keep all data in memory.
// But if data is distributed randomly, this function will be called log(n) times.
func (e *TopNExec) doCompaction(chkHeap *topNChunkHeap) error {
	newRowChunks := chunk.NewList(exec.RetTypes(e), e.InitCap(), e.MaxChunkSize())
	newRowPtrs := make([]chunk.RowPtr, 0, chkHeap.rowChunks.Len())
	for _, rowPtr := range chkHeap.rowPtrs {
		newRowPtr := newRowChunks.AppendRow(chkHeap.rowChunks.GetRow(rowPtr))
		newRowPtrs = append(newRowPtrs, newRowPtr)
	}
	newRowChunks.GetMemTracker().SetLabel(memory.LabelForRowChunks)
	e.memTracker.ReplaceChild(chkHeap.rowChunks.GetMemTracker(), newRowChunks.GetMemTracker())
	chkHeap.rowChunks = newRowChunks

	e.memTracker.Consume(int64(8 * (len(newRowPtrs) - len(chkHeap.rowPtrs))))
	chkHeap.rowPtrs = newRowPtrs
	return nil
}
