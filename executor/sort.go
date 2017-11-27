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

	// keyColumns is an optimization that when all by items are column, we don't need to evaluate them to keyChunks.
	// just use the row chunks instead.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc
	// keyChunks is used to store ByItems values.
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
		allCols := e.tryBuildKeyColumns()
		if allCols {
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
	for chk.NumRows() < chunk.MaxCapacity {
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

func (e *SortExec) tryBuildKeyColumns() bool {
	keyColumns := make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		if col, ok := by.Expr.(*expression.Column); ok {
			keyColumns = append(keyColumns, col.Index)
		} else {
			return false
		}
	}
	e.keyColumns = keyColumns
	return true
}

// keyColumnsLess is the less function for key columns.
func (e *SortExec) keyColumnsLess(i, j int) bool {
	ptrI := e.rowPointers[i]
	ptrJ := e.rowPointers[j]
	rowI := e.rowChunks[ptrI.chkIdx].GetRow(int(ptrI.rowIdx))
	rowJ := e.rowChunks[ptrJ.chkIdx].GetRow(int(ptrJ.rowIdx))
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

func (e *SortExec) buildKeyChunks() error {
	e.keyChunks = make([]*chunk.Chunk, len(e.rowChunks))
	keyLen := len(e.ByItems)
	keyTypes := make([]*types.FieldType, keyLen)
	e.keyCmpFuncs = make([]chunk.CompareFunc, keyLen)
	keyExprs := make([]expression.Expression, keyLen)
	for keyColIdx := range e.ByItems {
		keyExpr := e.ByItems[keyColIdx].Expr
		keyType := keyExpr.GetType()
		keyExprs[keyColIdx] = keyExpr
		keyTypes[keyColIdx] = keyType
		e.keyCmpFuncs[keyColIdx] = chunk.GetCompareFunc(keyType)
	}
	for chkIdx, rowChk := range e.rowChunks {
		keyChk := chunk.NewChunk(keyTypes)
		e.keyChunks[chkIdx] = keyChk
		err := expression.VectorizedExecute(e.ctx, keyExprs, rowChk, keyChk)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// keyChunksLess is the less function for key chunk.
func (e *SortExec) keyChunksLess(i, j int) bool {
	ptrI := e.rowPointers[i]
	ptrJ := e.rowPointers[j]
	keyRowI := e.keyChunks[ptrI.chkIdx].GetRow(int(ptrI.rowIdx))
	keyRowJ := e.keyChunks[ptrJ.chkIdx].GetRow(int(ptrJ.rowIdx))
	for colIdx := 0; colIdx < keyRowI.Len(); colIdx++ {
		cmpFunc := e.keyCmpFuncs[colIdx]
		cmp := cmpFunc(keyRowI, colIdx, keyRowJ, colIdx)
		if e.ByItems[colIdx].Desc {
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

// TopNExec implements a Top-N algorithm and it is built from a SELECT statement with ORDER BY and LIMIT.
// Instead of sorting all the rows fetched from the table, it keeps the Top-N elements only in a heap to reduce memory usage.
type TopNExec struct {
	SortExec
	limit      *plan.Limit
	totalCount int
	heapSize   int
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
		e.totalCount = int(e.limit.Offset + e.limit.Count)
		cap := e.totalCount + 1
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
			if e.totalCount == e.heapSize {
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
