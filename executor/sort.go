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
	"sort"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	goctx "golang.org/x/net/context"
)

// orderByRow binds a row to its order values, so it can be sorted.
type orderByRow struct {
	key []*types.Datum
	row Row
}

// SortExec represents sorting executor.
type SortExec struct {
	baseExecutor

	ByItems []*plan.ByItems
	Rows    []*orderByRow
	Idx     int
	fetched bool
	err     error
	schema  *expression.Schema

	keyExprs []expression.Expression
	keyTypes []*types.FieldType
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc
	// keyChunks is used to store ByItems values when not all ByItems are column.
	keyChunks *chunk.List
	// rowChunks is the chunks to store row values.
	rowChunks *chunk.List
	// rowPointer store the chunk index and row index for each row.
	rowPtrs []chunk.RowPtr
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	e.Rows = nil
	return errors.Trace(e.children[0].Close())
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(goCtx goctx.Context) error {
	e.fetched = false
	e.Idx = 0
	e.Rows = nil
	return errors.Trace(e.children[0].Open(goCtx))
}

// Len returns the number of rows.
func (e *SortExec) Len() int {
	return len(e.Rows)
}

// Swap implements sort.Interface Swap interface.
func (e *SortExec) Swap(i, j int) {
	e.Rows[i], e.Rows[j] = e.Rows[j], e.Rows[i]
}

// Less implements sort.Interface Less interface.
func (e *SortExec) Less(i, j int) bool {
	sc := e.ctx.GetSessionVars().StmtCtx
	for index, by := range e.ByItems {
		v1 := e.Rows[i].key[index]
		v2 := e.Rows[j].key[index]

		ret, err := v1.CompareDatum(sc, v2)
		if err != nil {
			e.err = errors.Trace(err)
			return true
		}

		if by.Desc {
			ret = -ret
		}

		if ret < 0 {
			return true
		} else if ret > 0 {
			return false
		}
	}

	return false
}

// Next implements the Executor Next interface.
func (e *SortExec) Next(goCtx goctx.Context) (Row, error) {
	if !e.fetched {
		for {
			srcRow, err := e.children[0].Next(goCtx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if srcRow == nil {
				break
			}
			orderRow := &orderByRow{
				row: srcRow,
				key: make([]*types.Datum, len(e.ByItems)),
			}
			for i, byItem := range e.ByItems {
				key, err := byItem.Expr.Eval(srcRow)
				if err != nil {
					return nil, errors.Trace(err)
				}
				orderRow.key[i] = &key
			}
			e.Rows = append(e.Rows, orderRow)
		}
		sort.Sort(e)
		e.fetched = true
	}
	if e.err != nil {
		return nil, errors.Trace(e.err)
	}
	if e.Idx >= len(e.Rows) {
		return nil, nil
	}
	row := e.Rows[e.Idx].row
	e.Idx++
	return row, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *SortExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if !e.fetched {
		err := e.fetchRowChunks(goCtx)
		if err != nil {
			return errors.Trace(err)
		}
		e.initPointers()
		e.initCompareFuncs()
		allColumnExpr := e.buildKeyColumns()
		if allColumnExpr {
			sort.Slice(e.rowPtrs, e.keyColumnsLess)
		} else {
			e.buildKeyExprsAndTypes()
			err = e.buildKeyChunks()
			if err != nil {
				return errors.Trace(err)
			}
			sort.Slice(e.rowPtrs, e.keyChunksLess)
		}
		e.fetched = true
	}
	for chk.NumRows() < e.maxChunkSize {
		if e.Idx >= len(e.rowPtrs) {
			return nil
		}
		rowPtr := e.rowPtrs[e.Idx]
		chk.AppendRow(0, e.rowChunks.GetRow(rowPtr))
		e.Idx++
	}
	return nil
}

func (e *SortExec) fetchRowChunks(goCtx goctx.Context) error {
	fields := e.schema.GetTypes()
	e.rowChunks = chunk.NewList(fields, e.maxChunkSize)
	for {
		chk := chunk.NewChunk(fields)
		err := e.children[0].NextChunk(goCtx, chk)
		if err != nil {
			return errors.Trace(err)
		}
		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}
		e.rowChunks.Add(chk)
	}
	return nil
}

func (e *SortExec) initPointers() {
	e.rowPtrs = make([]chunk.RowPtr, 0, e.rowChunks.Len())
	for chkIdx := 0; chkIdx < e.rowChunks.NumChunks(); chkIdx++ {
		rowChk := e.rowChunks.GetChunk(chkIdx)
		for rowIdx := 0; rowIdx < rowChk.NumRows(); rowIdx++ {
			e.rowPtrs = append(e.rowPtrs, chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)})
		}
	}
}

func (e *SortExec) initCompareFuncs() {
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *SortExec) buildKeyColumns() (allColumnExpr bool) {
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		if col, ok := by.Expr.(*expression.Column); ok {
			e.keyColumns = append(e.keyColumns, col.Index)
		} else {
			e.keyColumns = e.keyColumns[:0]
			for i := range e.ByItems {
				e.keyColumns = append(e.keyColumns, i)
			}
			return false
		}
	}
	return true
}

func (e *SortExec) buildKeyExprsAndTypes() {
	keyLen := len(e.ByItems)
	e.keyTypes = make([]*types.FieldType, keyLen)
	e.keyExprs = make([]expression.Expression, keyLen)
	for keyColIdx := range e.ByItems {
		e.keyExprs[keyColIdx] = e.ByItems[keyColIdx].Expr
		e.keyTypes[keyColIdx] = e.ByItems[keyColIdx].Expr.GetType()
	}
}

func (e *SortExec) buildKeyChunks() error {
	e.keyChunks = chunk.NewList(e.keyTypes, e.maxChunkSize)
	for chkIdx := 0; chkIdx < e.rowChunks.NumChunks(); chkIdx++ {
		keyChk := chunk.NewChunk(e.keyTypes)
		err := expression.VectorizedExecute(e.ctx, e.keyExprs, e.rowChunks.GetChunk(chkIdx), keyChk)
		if err != nil {
			return errors.Trace(err)
		}
		e.keyChunks.Add(keyChk)
	}
	return nil
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

// keyChunksLess is the less function for key chunk.
func (e *SortExec) keyChunksLess(i, j int) bool {
	keyRowI := e.keyChunks.GetRow(e.rowPtrs[i])
	keyRowJ := e.keyChunks.GetRow(e.rowPtrs[j])
	return e.lessRow(keyRowI, keyRowJ)
}

// TopNExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the table, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopNExec struct {
	SortExec
	limit      *plan.PhysicalLimit
	totalLimit int
	heapSize   int

	chkHeap *topNChunkHeap
}

// Less implements heap.Interface Less interface.
func (e *TopNExec) Less(i, j int) bool {
	sc := e.ctx.GetSessionVars().StmtCtx
	for index, by := range e.ByItems {
		v1 := e.Rows[i].key[index]
		v2 := e.Rows[j].key[index]

		ret, err := v1.CompareDatum(sc, v2)
		if err != nil {
			e.err = errors.Trace(err)
			return true
		}

		if by.Desc {
			ret = -ret
		}

		if ret > 0 {
			return true
		} else if ret < 0 {
			return false
		}
	}

	return false
}

// Len implements heap.Interface Len interface.
func (e *TopNExec) Len() int {
	return e.heapSize
}

// Push implements heap.Interface Push interface.
func (e *TopNExec) Push(x interface{}) {
	e.Rows = append(e.Rows, x.(*orderByRow))
	e.heapSize++
}

// Pop implements heap.Interface Pop interface.
func (e *TopNExec) Pop() interface{} {
	e.heapSize--
	return nil
}

// Next implements the Executor Next interface.
func (e *TopNExec) Next(goCtx goctx.Context) (Row, error) {
	if !e.fetched {
		e.Idx = int(e.limit.Offset)
		e.totalLimit = int(e.limit.Offset + e.limit.Count)
		cap := e.totalLimit + 1
		if cap > 1024 {
			cap = 1024
		}
		e.Rows = make([]*orderByRow, 0, cap)
		e.heapSize = 0
		for {
			srcRow, err := e.children[0].Next(goCtx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if srcRow == nil {
				break
			}
			// build orderRow from srcRow.
			orderRow := &orderByRow{
				row: srcRow,
				key: make([]*types.Datum, len(e.ByItems)),
			}
			for i, byItem := range e.ByItems {
				key, err := byItem.Expr.Eval(srcRow)
				if err != nil {
					return nil, errors.Trace(err)
				}
				orderRow.key[i] = &key
			}
			if e.totalLimit == e.heapSize {
				// An equivalent of Push and Pop. We don't use the standard Push and Pop
				// to reduce the number of comparisons.
				e.Rows = append(e.Rows, orderRow)
				if e.Less(0, e.heapSize) {
					e.Swap(0, e.heapSize)
					heap.Fix(e, 0)
				}
				e.Rows = e.Rows[:e.heapSize]
			} else {
				heap.Push(e, orderRow)
			}
		}
		if e.limit.Offset == 0 {
			sort.Sort(&e.SortExec)
		} else {
			for i := 0; i < int(e.limit.Count) && e.Len() > 0; i++ {
				heap.Pop(e)
			}
		}
		e.fetched = true
	}
	if e.Idx >= len(e.Rows) {
		return nil, nil
	}
	row := e.Rows[e.Idx].row
	e.Idx++
	return row, nil
}

// topNChunkHeap implements heap.Interface.
type topNChunkHeap struct {
	*TopNExec
}

// Less implement heap.Interface, but since we mantains a max heap,
// this function returns true if row i is greater than row j.
func (h *topNChunkHeap) Less(i, j int) bool {
	if h.keyChunks != nil {
		return h.keyChunksGreater(i, j)
	}
	return h.keyColumnsGreater(i, j)
}

func (h *topNChunkHeap) keyChunksGreater(i, j int) bool {
	keyRowI := h.keyChunks.GetRow(h.rowPtrs[i])
	keyRowJ := h.keyChunks.GetRow(h.rowPtrs[j])
	return h.greaterRow(keyRowI, keyRowJ)
}

func (h *topNChunkHeap) keyColumnsGreater(i, j int) bool {
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

// NextChunk implements the Executor NextChunk interface.
func (e *TopNExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if !e.fetched {
		e.totalLimit = int(e.limit.Offset + e.limit.Count)
		e.Idx = int(e.limit.Offset)
		err := e.loadChunksUntilTotalLimit(goCtx)
		if err != nil {
			return errors.Trace(err)
		}
		err = e.executeTopN(goCtx)
		if err != nil {
			return errors.Trace(err)
		}
		e.fetched = true
	}
	if e.Idx >= len(e.rowPtrs) {
		return nil
	}
	for chk.NumRows() < e.maxChunkSize && e.Idx < len(e.rowPtrs) {
		row := e.rowChunks.GetRow(e.rowPtrs[e.Idx])
		chk.AppendRow(0, row)
		e.Idx++
	}
	return nil
}

func (e *TopNExec) loadChunksUntilTotalLimit(goCtx goctx.Context) error {
	e.chkHeap = &topNChunkHeap{e}
	e.rowChunks = chunk.NewList(e.schema.GetTypes(), e.maxChunkSize)
	for e.rowChunks.Len() < e.totalLimit {
		srcChk := e.children[0].newChunk()
		err := e.children[0].NextChunk(goCtx, srcChk)
		if err != nil {
			return errors.Trace(err)
		}
		if srcChk.NumRows() == 0 {
			break
		}
		e.rowChunks.Add(srcChk)
	}
	e.initPointers()
	e.initCompareFuncs()
	allColumnExpr := e.buildKeyColumns()
	if !allColumnExpr {
		e.buildKeyExprsAndTypes()
		err := e.buildKeyChunks()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

const topNCompactionFactor = 4

func (e *TopNExec) executeTopN(goCtx goctx.Context) error {
	heap.Init(e.chkHeap)
	for len(e.rowPtrs) > e.totalLimit {
		// The number of rows we loaded may exceeds total limit, remove greatest rows by Pop.
		heap.Pop(e.chkHeap)
	}
	var childKeyChk *chunk.Chunk
	if e.keyChunks != nil {
		childKeyChk = chunk.NewChunk(e.keyTypes)
	}
	childRowChk := e.children[0].newChunk()
	for {
		err := e.children[0].NextChunk(goCtx, childRowChk)
		if err != nil {
			return errors.Trace(err)
		}
		if childRowChk.NumRows() == 0 {
			break
		}
		err = e.processChildChk(childRowChk, childKeyChk)
		if err != nil {
			return errors.Trace(err)
		}
		if e.rowChunks.Len() > len(e.rowPtrs)*topNCompactionFactor {
			err = e.doCompaction()
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	if e.keyChunks != nil {
		sort.Slice(e.rowPtrs, e.keyChunksLess)
	} else {
		sort.Slice(e.rowPtrs, e.keyColumnsLess)
	}
	return nil
}

func (e *TopNExec) processChildChk(childRowChk, childKeyChk *chunk.Chunk) error {
	if childKeyChk != nil {
		childKeyChk.Reset()
		err := expression.VectorizedExecute(e.ctx, e.keyExprs, childRowChk, childKeyChk)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i := 0; i < childRowChk.NumRows(); i++ {
		heapMaxPtr := e.rowPtrs[0]
		var heapMax, next chunk.Row
		if childKeyChk != nil {
			heapMax = e.keyChunks.GetRow(heapMaxPtr)
			next = childKeyChk.GetRow(i)
		} else {
			heapMax = e.rowChunks.GetRow(heapMaxPtr)
			next = childRowChk.GetRow(i)
		}
		if e.chkHeap.greaterRow(heapMax, next) {
			// Evict heap max, keep the next row.
			e.rowPtrs[0] = e.rowChunks.AppendRow(childRowChk.GetRow(i))
			if childKeyChk != nil {
				e.keyChunks.AppendRow(childKeyChk.GetRow(i))
			}
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
	newRowChunks := chunk.NewList(e.schema.GetTypes(), e.maxChunkSize)
	newRowPtrs := make([]chunk.RowPtr, 0, e.rowChunks.Len())
	for _, rowPtr := range e.rowPtrs {
		newRowPtr := newRowChunks.AppendRow(e.rowChunks.GetRow(rowPtr))
		newRowPtrs = append(newRowPtrs, newRowPtr)
	}
	e.rowChunks = newRowChunks
	e.rowPtrs = newRowPtrs
	if e.keyChunks != nil {
		err := e.buildKeyChunks()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
