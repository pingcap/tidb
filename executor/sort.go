// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"container/heap"
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

var rowChunksLabel fmt.Stringer = stringutil.StringerStr("rowChunks")

// SortExec represents sorting executor.
type SortExec struct {
	baseExecutor

	ByItems []*plannercore.ByItems
	Idx     int
	fetched bool
	schema  *expression.Schema

	keyExprs []expression.Expression
	keyTypes []*types.FieldType
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc
	// rowChunks is the chunks to store row values.
	rowChunks *chunk.List
	// rowPointer store the chunk index and row index for each row.
	rowPtrs []chunk.RowPtr

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	// rowChunksInDisk is the chunks to store row values in disk.
	rowChunksInDisk *chunk.ListInDisk
	// rowPtrsInDisk store the disk-chunk index and row index for each row.
	rowPtrsInDisk []chunk.RowPtr
	// partitionList is the chunks to store row values in disk for partitions.
	partitionList []*chunk.ListInDisk
	// partitionRowPtrs store the disk-chunk index and row index for each row for partitions.
	partitionRowPtrs [][]chunk.RowPtr
	// exceeded indicates that records have exceeded memQuota during
	// adding this chunk and we should spill now.
	exceeded uint32
	// spilled indicates that records have spilled out into disk.
	spilled uint32
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	if e.alreadySpilled() {
		if e.rowChunksInDisk != nil {
			if err := e.rowChunksInDisk.Close(); err != nil {
				return err
			}
		}
		for _, chunkInDisk := range e.partitionList {
			if chunkInDisk != nil {
				if err := chunkInDisk.Close(); err != nil {
					return err
				}
			}
		}
		e.rowChunksInDisk = nil
		e.partitionList = e.partitionList[:0]

		e.memTracker.Consume(int64(-8 * cap(e.rowPtrsInDisk)))
		e.rowPtrsInDisk = nil
		for _, partitionPtrs := range e.partitionRowPtrs {
			e.memTracker.Consume(int64(-8 * cap(partitionPtrs)))
		}
		e.partitionRowPtrs = nil
	}
	if e.rowChunks != nil {
		e.memTracker.Consume(-e.rowChunks.GetMemTracker().BytesConsumed())
		e.rowChunks = nil
	}
	e.memTracker.Consume(int64(-8 * cap(e.rowPtrs)))
	e.rowPtrs = nil
	e.memTracker = nil
	e.diskTracker = nil
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	e.fetched = false
	e.Idx = 0

	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaSort)
		e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
		e.diskTracker = memory.NewTracker(e.id, -1)
		e.diskTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.DiskTracker)
	}
	e.exceeded = 0
	e.spilled = 0
	e.rowChunksInDisk = nil
	e.rowPtrsInDisk = e.rowPtrsInDisk[:0]
	e.partitionList = e.partitionList[:0]
	e.partitionRowPtrs = e.partitionRowPtrs[:0]
	return e.children[0].Open(ctx)
}

// Next implements the Executor Next interface.
func (e *SortExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		err := e.fetchRowChunks(ctx)
		if err != nil {
			return err
		}
		if e.alreadySpilled() {
			err = e.prepareExternalSorting()
			if err != nil {
				return err
			}
			e.fetched = true
		} else {
			e.initPointers()
			e.initCompareFuncs()
			e.buildKeyColumns()
			sort.Slice(e.rowPtrs, e.keyColumnsLess)
			e.fetched = true
		}
	}

	if e.alreadySpilled() {
		for !req.IsFull() && e.Idx < len(e.partitionRowPtrs[0]) {
			rowPtr := e.partitionRowPtrs[0][e.Idx]
			row, err := e.partitionList[0].GetRow(rowPtr)
			if err != nil {
				return err
			}
			req.AppendRow(row)
			e.Idx++
		}
	} else {
		for !req.IsFull() && e.Idx < len(e.rowPtrs) {
			rowPtr := e.rowPtrs[e.Idx]
			req.AppendRow(e.rowChunks.GetRow(rowPtr))
			e.Idx++
		}
	}
	return nil
}

func (e *SortExec) prepareExternalSorting() (err error) {
	e.initCompareFuncs()
	e.buildKeyColumns()
	e.rowPtrsInDisk = e.initPointersForListInDisk(e.rowChunksInDisk)
	// partition sort
	// Now only have one partition.
	// The partition will be adjusted in the next pr.
	err = e.readPartition(e.rowChunksInDisk, e.rowPtrsInDisk)
	if err != nil {
		return err
	}
	e.initPointers()
	sort.Slice(e.rowPtrs, e.keyColumnsLess)
	listInDisk, err := e.spillToDiskByRowPtr()
	if err != nil {
		return err
	}
	e.memTracker.Consume(-e.rowChunks.GetMemTracker().BytesConsumed())
	e.rowChunks = nil
	e.partitionList = append(e.partitionList, listInDisk)
	e.partitionRowPtrs = append(e.partitionRowPtrs, e.initPointersForListInDisk(listInDisk))
	return err
}

func (e *SortExec) fetchRowChunks(ctx context.Context) error {
	fields := retTypes(e)
	e.rowChunks = chunk.NewList(fields, e.initCap, e.maxChunkSize)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel(rowChunksLabel)
	for {
		chk := newFirstChunk(e.children[0])
		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return err
		}
		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}
		if e.alreadySpilled() {
			// append chk to disk.
			err := e.rowChunksInDisk.Add(chk)
			if err != nil {
				return err
			}
		} else {
			e.rowChunks.Add(chk)
			if atomic.LoadUint32(&e.exceeded) == 1 {
				e.rowChunksInDisk, err = e.spillToDisk()
				if err != nil {
					return err
				}
				e.memTracker.Consume(-e.rowChunks.GetMemTracker().BytesConsumed())
				e.rowChunks = nil // GC its internal chunks.
				atomic.StoreUint32(&e.spilled, 1)
			}
		}
	}
	return nil
}

func (e *SortExec) initPointers() {
	if e.rowPtrs != nil {
		e.memTracker.Consume(int64(-8 * cap(e.rowPtrs)))
		e.rowPtrs = e.rowPtrs[:0]
	} else {
		e.rowPtrs = make([]chunk.RowPtr, 0, e.rowChunks.Len())
	}
	for chkIdx := 0; chkIdx < e.rowChunks.NumChunks(); chkIdx++ {
		rowChk := e.rowChunks.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < rowChk.NumRows(); rowIdx++ {
			e.rowPtrs = append(e.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
	e.memTracker.Consume(int64(8 * cap(e.rowPtrs)))
}

func (e *SortExec) initPointersForListInDisk(disk *chunk.ListInDisk) []chunk.RowPtr {
	rowPtrsInDisk := make([]chunk.RowPtr, 0)
	for chkIdx := 0; chkIdx < disk.NumChunks(); chkIdx++ {
		for rowIdx := 0; rowIdx < disk.NumRowsOfChunk(chkIdx); rowIdx++ {
			rowPtrsInDisk = append(rowPtrsInDisk, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
	e.memTracker.Consume(int64(8 * len(rowPtrsInDisk)))
	return rowPtrsInDisk
}

func (e *SortExec) initCompareFuncs() {
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *SortExec) buildKeyColumns() {
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		col := by.Expr.(*expression.Column)
		e.keyColumns = append(e.keyColumns, col.Index)
	}
}

func (e *SortExec) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
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

// keyColumnsLess is the less function for key columns.
func (e *SortExec) keyColumnsLess(i, j int) bool {
	rowI := e.rowChunks.GetRow(e.rowPtrs[i])
	rowJ := e.rowChunks.GetRow(e.rowPtrs[j])
	return e.lessRow(rowI, rowJ)
}

func (e *SortExec) readPartition(disk *chunk.ListInDisk, rowPtrs []chunk.RowPtr) error {
	e.rowChunks = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel(rowChunksLabel)
	for _, rowPtr := range rowPtrs {
		rowPtr, err := disk.GetRow(rowPtr)
		if err != nil {
			return err
		}
		e.rowChunks.AppendRow(rowPtr)
	}
	return nil
}

// alreadySpilled indicates that records have spilled out into disk.
func (e *SortExec) alreadySpilled() bool { return e.rowChunksInDisk != nil }

// alreadySpilledSafe indicates that records have spilled out into disk. It's thread-safe.
func (e *SortExec) alreadySpilledSafe() bool { return atomic.LoadUint32(&e.spilled) == 1 }

func (e *SortExec) spillToDisk() (disk *chunk.ListInDisk, err error) {
	N := e.rowChunks.NumChunks()
	rowChunksInDisk := chunk.NewListInDisk(e.retFieldTypes)
	rowChunksInDisk.GetDiskTracker().AttachTo(e.diskTracker)
	for i := 0; i < N; i++ {
		chk := e.rowChunks.GetChunk(i)
		err = rowChunksInDisk.Add(chk)
		if err != nil {
			return nil, err
		}
	}
	return rowChunksInDisk, nil
}

func (e *SortExec) spillToDiskByRowPtr() (disk *chunk.ListInDisk, err error) {
	rowChunksInDisk := chunk.NewListInDisk(e.retFieldTypes)
	rowChunksInDisk.GetDiskTracker().AttachTo(e.diskTracker)
	chk := newFirstChunk(e)
	for _, rowPtr := range e.rowPtrs {
		chk.AppendRow(e.rowChunks.GetRow(rowPtr))
		if chk.IsFull() {
			err := rowChunksInDisk.Add(chk)
			if err != nil {
				return nil, err
			}
			chk = newFirstChunk(e)
		}
	}
	if chk.NumRows() != 0 {
		if err := rowChunksInDisk.Add(chk); err != nil {
			return nil, err
		}
	}
	return rowChunksInDisk, nil
}

// TopNExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the table, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopNExec struct {
	SortExec
	limit      *plannercore.PhysicalLimit
	totalLimit uint64

	chkHeap *topNChunkHeap
}

// topNChunkHeap implements heap.Interface.
type topNChunkHeap struct {
	*TopNExec
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

func (h *topNChunkHeap) Push(x interface{}) {
	// Should never be called.
}

func (h *topNChunkHeap) Pop() interface{} {
	h.rowPtrs = h.rowPtrs[:len(h.rowPtrs)-1]
	// We don't need the popped value, return nil to avoid memory allocation.
	return nil
}

func (h *topNChunkHeap) Swap(i, j int) {
	h.rowPtrs[i], h.rowPtrs[j] = h.rowPtrs[j], h.rowPtrs[i]
}

// Open implements the Executor Open interface.
func (e *TopNExec) Open(ctx context.Context) error {
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaTopn)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	return e.SortExec.Open(ctx)
}

// Next implements the Executor Next interface.
func (e *TopNExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		e.totalLimit = e.limit.Offset + e.limit.Count
		e.Idx = int(e.limit.Offset)
		err := e.loadChunksUntilTotalLimit(ctx)
		if err != nil {
			return err
		}
		err = e.executeTopN(ctx)
		if err != nil {
			return err
		}
		e.fetched = true
	}
	if e.Idx >= len(e.rowPtrs) {
		return nil
	}
	for !req.IsFull() && e.Idx < len(e.rowPtrs) {
		row := e.rowChunks.GetRow(e.rowPtrs[e.Idx])
		req.AppendRow(row)
		e.Idx++
	}
	return nil
}

func (e *TopNExec) loadChunksUntilTotalLimit(ctx context.Context) error {
	e.chkHeap = &topNChunkHeap{e}
	e.rowChunks = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	e.rowChunks.GetMemTracker().SetLabel(rowChunksLabel)
	for uint64(e.rowChunks.Len()) < e.totalLimit {
		srcChk := newFirstChunk(e.children[0])
		// adjust required rows by total limit
		srcChk.SetRequiredRows(int(e.totalLimit-uint64(e.rowChunks.Len())), e.maxChunkSize)
		err := Next(ctx, e.children[0], srcChk)
		if err != nil {
			return err
		}
		if srcChk.NumRows() == 0 {
			break
		}
		e.rowChunks.Add(srcChk)
	}
	e.initPointers()
	e.initCompareFuncs()
	e.buildKeyColumns()
	return nil
}

const topNCompactionFactor = 4

func (e *TopNExec) executeTopN(ctx context.Context) error {
	heap.Init(e.chkHeap)
	for uint64(len(e.rowPtrs)) > e.totalLimit {
		// The number of rows we loaded may exceeds total limit, remove greatest rows by Pop.
		heap.Pop(e.chkHeap)
	}
	childRowChk := newFirstChunk(e.children[0])
	for {
		err := Next(ctx, e.children[0], childRowChk)
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
		if e.rowChunks.Len() > len(e.rowPtrs)*topNCompactionFactor {
			err = e.doCompaction()
			if err != nil {
				return err
			}
		}
	}
	sort.Slice(e.rowPtrs, e.keyColumnsLess)
	return nil
}

func (e *TopNExec) processChildChk(childRowChk *chunk.Chunk) error {
	for i := 0; i < childRowChk.NumRows(); i++ {
		heapMaxPtr := e.rowPtrs[0]
		var heapMax, next chunk.Row
		heapMax = e.rowChunks.GetRow(heapMaxPtr)
		next = childRowChk.GetRow(i)
		if e.chkHeap.greaterRow(heapMax, next) {
			// Evict heap max, keep the next row.
			e.rowPtrs[0] = e.rowChunks.AppendRow(childRowChk.GetRow(i))
			heap.Fix(e.chkHeap, 0)
		}
	}
	return nil
}

// doCompaction rebuild the chunks and row pointers to release memory.
// If we don't do compaction, in a extreme case like the child data is already ascending sorted
// but we want descending top N, then we will keep all data in memory.
// But if data is distributed randomly, this function will be called log(n) times.
func (e *TopNExec) doCompaction() error {
	newRowChunks := chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
	newRowPtrs := make([]chunk.RowPtr, 0, e.rowChunks.Len())
	for _, rowPtr := range e.rowPtrs {
		newRowPtr := newRowChunks.AppendRow(e.rowChunks.GetRow(rowPtr))
		newRowPtrs = append(newRowPtrs, newRowPtr)
	}
	newRowChunks.GetMemTracker().SetLabel(rowChunksLabel)
	e.memTracker.ReplaceChild(e.rowChunks.GetMemTracker(), newRowChunks.GetMemTracker())
	e.rowChunks = newRowChunks

	e.memTracker.Consume(int64(-8 * len(e.rowPtrs)))
	e.memTracker.Consume(int64(8 * len(newRowPtrs)))
	e.rowPtrs = newRowPtrs
	return nil
}
