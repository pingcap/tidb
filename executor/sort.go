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
	keyChunks []*chunk.Chunk
	// rowChunks is the chunks to store row values.
	rowChunks []*chunk.Chunk
	// rowPointer store the chunk index and row index for each row.
	rowPointers []rowPointer
	// totalCount is calculated from rowChunks and used to initialize rowPointers.
	totalCount int
}

// rowPointer stores the address of a row in rowChunks by its chunk index and row index.
type rowPointer struct {
	chkIdx uint32
	rowIdx uint32
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
func (e *SortExec) NextChunk(chk *chunk.Chunk) error {
	chk.Reset()
	if !e.fetched {
		err := e.fetchRowChunks()
		if err != nil {
			return errors.Trace(err)
		}
		e.initPointers()
		e.initCompareFuncs()
		allColumnExpr := e.buildKeyColumns()
		if allColumnExpr {
			sort.Slice(e.rowPointers, e.keyColumnsLess)
		} else {
			err = e.buildKeyChunks()
			if err != nil {
				return errors.Trace(err)
			}
			sort.Slice(e.rowPointers, e.keyChunksLess)
		}
		e.fetched = true
	}
	for chk.NumRows() < e.ctx.GetSessionVars().MaxChunkSize {
		if e.Idx >= len(e.rowPointers) {
			return nil
		}
		rowPtr := e.rowPointers[e.Idx]
		chk.AppendRow(0, e.rowChunks[rowPtr.chkIdx].GetRow(int(rowPtr.rowIdx)))
		e.Idx++
	}
	return nil
}

func (e *SortExec) fetchRowChunks() error {
	fields := e.schema.GetTypes()
	for {
		chk := chunk.NewChunk(fields)
		err := e.children[0].NextChunk(chk)
		if err != nil {
			return errors.Trace(err)
		}
		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}
		e.rowChunks = append(e.rowChunks, chk)
		e.totalCount += rowCount
	}
	return nil
}

func (e *SortExec) initPointers() {
	e.rowPointers = make([]rowPointer, 0, e.totalCount)
	for chkIdx, rowChk := range e.rowChunks {
		for rowIdx := 0; rowIdx < rowChk.NumRows(); rowIdx++ {
			e.rowPointers = append(e.rowPointers, rowPointer{chkIdx: uint32(chkIdx), rowIdx: uint32(rowIdx)})
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
		keyExpr := e.ByItems[keyColIdx].Expr
		keyType := keyExpr.GetType()
		e.keyExprs[keyColIdx] = keyExpr
		e.keyTypes[keyColIdx] = keyType
	}
}

func (e *SortExec) buildKeyChunks() error {
	e.keyChunks = make([]*chunk.Chunk, len(e.rowChunks))
	for chkIdx, rowChk := range e.rowChunks {
		keyChk := chunk.NewChunk(e.keyTypes)
		e.keyChunks[chkIdx] = keyChk
		err := expression.VectorizedExecute(e.ctx, e.keyExprs, rowChk, keyChk)
		if err != nil {
			return errors.Trace(err)
		}
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
	ptrI := e.rowPointers[i]
	ptrJ := e.rowPointers[j]
	rowI := e.rowChunks[ptrI.chkIdx].GetRow(int(ptrI.rowIdx))
	rowJ := e.rowChunks[ptrJ.chkIdx].GetRow(int(ptrJ.rowIdx))
	return e.lessRow(rowI, rowJ)
}

// keyChunksLess is the less function for key chunk.
func (e *SortExec) keyChunksLess(i, j int) bool {
	ptrI := e.rowPointers[i]
	ptrJ := e.rowPointers[j]
	keyRowI := e.keyChunks[ptrI.chkIdx].GetRow(int(ptrI.rowIdx))
	keyRowJ := e.keyChunks[ptrJ.chkIdx].GetRow(int(ptrJ.rowIdx))
	return e.lessRow(keyRowI, keyRowJ)
}

// TopNExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the table, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopNExec struct {
	SortExec
	limit      *plan.Limit
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
	if len(h.keyChunks) != 0 {
		return h.keyChunksGreater(i, j)
	}
	return h.keyColumnsGreater(i, j)
}

func (h *topNChunkHeap) keyChunksGreater(i, j int) bool {
	ptrI := h.rowPointers[i]
	ptrJ := h.rowPointers[j]
	keyRowI := h.keyChunks[ptrI.chkIdx].GetRow(int(ptrI.rowIdx))
	keyRowJ := h.keyChunks[ptrJ.chkIdx].GetRow(int(ptrJ.rowIdx))
	return h.greaterRow(keyRowI, keyRowJ)
}

func (h *topNChunkHeap) keyColumnsGreater(i, j int) bool {
	ptrI := h.rowPointers[i]
	ptrJ := h.rowPointers[j]
	rowI := h.rowChunks[ptrI.chkIdx].GetRow(int(ptrI.rowIdx))
	rowJ := h.rowChunks[ptrJ.chkIdx].GetRow(int(ptrJ.rowIdx))
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
	return len(h.rowPointers)
}

func (h *topNChunkHeap) Push(x interface{}) {
	// Should never be called.
}

func (h *topNChunkHeap) Pop() interface{} {
	h.rowPointers = h.rowPointers[:len(h.rowPointers)-1]
	// We don't need the popped value, return nil to avoid memory allocation.
	return nil
}

func (h *topNChunkHeap) Swap(i, j int) {
	h.rowPointers[i], h.rowPointers[j] = h.rowPointers[j], h.rowPointers[i]
}

func (e *TopNExec) NextChunk(chk *chunk.Chunk) error {
	chk.Reset()
	if !e.fetched {
		e.totalLimit = int(e.limit.Offset + e.limit.Count)
		e.Idx = int(e.limit.Offset)
		err := e.loadChunksUntilTotalLimit()
		if err != nil {
			return errors.Trace(err)
		}
		err = e.executeTopN()
		if err != nil {
			return errors.Trace(err)
		}
		e.fetched = true
	}
	if e.Idx >= len(e.rowPointers) {
		return nil
	}
	maxChkSize := e.ctx.GetSessionVars().MaxChunkSize
	for chk.NumRows() < maxChkSize && e.Idx < len(e.rowPointers) {
		rowPointer := e.rowPointers[e.Idx]
		row := e.rowChunks[rowPointer.chkIdx].GetRow(int(rowPointer.rowIdx))
		chk.AppendRow(0, row)
		e.Idx++
	}
	return nil
}

func (e *TopNExec) loadChunksUntilTotalLimit() error {
	e.chkHeap = &topNChunkHeap{e}
	for e.totalCount < e.totalLimit {
		srcChk := e.children[0].newChunk()
		err := e.children[0].NextChunk(srcChk)
		if err != nil {
			return errors.Trace(err)
		}
		if srcChk.NumRows() == 0 {
			break
		}
		e.rowChunks = append(e.rowChunks, srcChk)
		e.totalCount += srcChk.NumRows()
	}
	e.initPointers()
	e.initCompareFuncs()
	allColumnExpr := e.buildKeyColumns()
	if !allColumnExpr {
		e.buildKeyExprsAndTypes()
		e.buildKeyChunks()
	}
	return nil
}

const topNCompactionfactor = 4

func (e *TopNExec) executeTopN() error {
	heap.Init(e.chkHeap)
	for len(e.rowPointers) > e.totalLimit {
		// The number of rows we loaded may exceeds total limit, remove greatest rows by Pop.
		heap.Pop(e.chkHeap)
	}
	var childKeyChk *chunk.Chunk
	if len(e.keyChunks) > 0 {
		childKeyChk = chunk.NewChunk(e.keyTypes)
	}
	childRowChk := e.children[0].newChunk()
	for {
		err := e.children[0].NextChunk(childRowChk)
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
		if e.totalCount > len(e.rowPointers)*topNCompactionfactor {
			e.doCompaction()
		}
	}
	if len(e.keyChunks) != 0 {
		sort.Slice(e.rowPointers, e.keyChunksLess)
	} else {
		sort.Slice(e.rowPointers, e.keyColumnsLess)
	}
	return nil
}

func (e *TopNExec) buildKeyExprsAndChildKeyChunk() ([]expression.Expression, *chunk.Chunk) {
	keyLen := len(e.ByItems)
	keyTypes := make([]*types.FieldType, keyLen)
	keyExprs := make([]expression.Expression, keyLen)
	for keyColIdx := range e.ByItems {
		keyExpr := e.ByItems[keyColIdx].Expr
		keyType := keyExpr.GetType()
		keyExprs[keyColIdx] = keyExpr
		keyTypes[keyColIdx] = keyType
	}
	return keyExprs, chunk.NewChunk(keyTypes)
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
		heapMaxPtr := e.rowPointers[0]
		var heapMax, next chunk.Row
		if childKeyChk != nil {
			heapMax = e.keyChunks[heapMaxPtr.chkIdx].GetRow(int(heapMaxPtr.rowIdx))
			next = childKeyChk.GetRow(i)
		} else {
			heapMax = e.rowChunks[heapMaxPtr.chkIdx].GetRow(int(heapMaxPtr.rowIdx))
			next = childRowChk.GetRow(i)
		}
		if e.chkHeap.greaterRow(heapMax, next) {
			// Evict heap max, keep the next row.
			e.appendRowChunk(childRowChk.GetRow(i))
			if childKeyChk != nil {
				e.appendKeyChunk(childKeyChk.GetRow(i))
			}
			e.totalCount++
			chkIdx := len(e.rowChunks) - 1
			e.rowPointers[0].chkIdx = uint32(chkIdx)
			e.rowPointers[0].rowIdx = uint32(e.rowChunks[chkIdx].NumRows() - 1)
			heap.Fix(e.chkHeap, 0)
		}
	}
	return nil
}

func (e *TopNExec) appendRowChunk(row chunk.Row) {
	lastChkIdx := len(e.rowChunks) - 1
	if lastChkIdx == -1 || e.rowChunks[lastChkIdx].NumRows() >= e.ctx.GetSessionVars().MaxChunkSize {
		e.rowChunks = append(e.rowChunks, e.children[0].newChunk())
		lastChkIdx++
	}
	e.rowChunks[lastChkIdx].AppendRow(0, row)
}

func (e *TopNExec) appendKeyChunk(keyRow chunk.Row) {
	lastChkIdx := len(e.keyChunks) - 1
	if lastChkIdx == -1 || e.keyChunks[lastChkIdx].NumRows() >= e.ctx.GetSessionVars().MaxChunkSize {
		e.keyChunks = append(e.keyChunks, chunk.NewChunk(e.keyTypes))
		lastChkIdx++
	}
	e.keyChunks[lastChkIdx].AppendRow(0, keyRow)
}

// doCompaction rebuild the chunks and row pointers to release memory.
// If we don't do compaction, in a extreme case like the child data is already ascending sorted
// but we want descending top N, then we will keep all data in memory.
// But if data is distributed randomly, this function will be called log(n) times.
func (e *TopNExec) doCompaction() {
	newRowChunks := make([]*chunk.Chunk, 0, len(e.rowChunks))
	newRowChunks = append(newRowChunks, e.children[0].newChunk())
	maxChunkSize := e.ctx.GetSessionVars().MaxChunkSize
	for _, rowPtr := range e.rowPointers {
		lastChkIdx := len(newRowChunks) - 1
		if newRowChunks[lastChkIdx].NumRows() > maxChunkSize {
			newRowChunks = append(newRowChunks, e.children[0].newChunk())
			lastChkIdx++
		}
		row := e.rowChunks[rowPtr.chkIdx].GetRow(int(rowPtr.rowIdx))
		newRowChunks[lastChkIdx].AppendRow(0, row)
	}
	e.rowChunks = newRowChunks
	e.initPointers()
	if len(e.keyChunks) != 0 {
		e.buildKeyChunks()
	}
	e.totalCount = len(e.rowPointers)
}
