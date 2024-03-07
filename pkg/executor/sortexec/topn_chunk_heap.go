// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

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
