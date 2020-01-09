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
	"context"
	"fmt"

	"github.com/pingcap/errors"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

// MergeJoinExec implements the merge join algorithm.
// This operator assumes that two iterators of both sides
// will provide required order on join condition:
// 1. For equal-join, one of the join key from each side
// matches the order given.
// 2. For other cases its preferred not to use SMJ and operator
// will throw error.
type MergeJoinExec struct {
	baseExecutor

	stmtCtx      *stmtctx.StatementContext
	compareFuncs []expression.CompareFunc
	joiner       joiner
	isOuterJoin  bool

	prepared bool
	outerIdx int

	innerTable *mergeJoinInnerTable
	outerTable *mergeJoinOuterTable

	innerIter4Row chunk.Iterator

	childrenResults []*chunk.Chunk

	memTracker *memory.Tracker
}

type mergeJoinOuterTable struct {
	reader Executor
	filter []expression.Expression
	keys   []*expression.Column

	chk      *chunk.Chunk
	selected []bool

	iter     *chunk.Iterator4Chunk
	row      chunk.Row
	hasMatch bool
	hasNull  bool
}

// mergeJoinInnerTable represents the inner table of merge join.
// All the inner rows which have the same join key are returned when function
// "rowsWithSameKey()" being called.
type mergeJoinInnerTable struct {
	reader   Executor
	joinKeys []*expression.Column
	ctx      context.Context

	// for chunk executions
	rowContainer *chunk.RowContainer
	keyCmpFuncs  []chunk.CompareFunc
	firstRow4Key chunk.Row
	curRow       chunk.Row
	curChk       *chunk.Chunk
	curSel       []int
	curIter      *chunk.Iterator4Chunk
	curChkInUse  bool

	memTracker *memory.Tracker
}

func (t *mergeJoinInnerTable) init(ctx context.Context, sctx sessionctx.Context, chk4Reader *chunk.Chunk) (err error) {
	if t.reader == nil || ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	t.ctx = ctx
	t.curChk = chk4Reader
	t.curIter = chunk.NewIterator4Chunk(t.curChk)
	t.curRow = t.curIter.End()
	t.curChkInUse = false
	t.memTracker.Consume(chk4Reader.MemoryUsage())
	// t.rowContainer needs to be closed when exit, it is done by the MergeJoinExec, see MergeJoinExec.Close
	t.rowContainer = chunk.NewRowContainer(t.reader.base().retFieldTypes, t.curChk.Capacity())
	t.rowContainer.GetMemTracker().AttachTo(sctx.GetSessionVars().StmtCtx.MemTracker)
	t.rowContainer.GetDiskTracker().AttachTo(sctx.GetSessionVars().StmtCtx.DiskTracker)
	if config.GetGlobalConfig().OOMUseTmpStorage {
		actionSpill := t.rowContainer.ActionSpill()
		sctx.GetSessionVars().StmtCtx.MemTracker.SetActionOnExceed(actionSpill)
	}
	t.firstRow4Key, err = t.nextRow()
	t.keyCmpFuncs = make([]chunk.CompareFunc, 0, len(t.joinKeys))
	for i := range t.joinKeys {
		t.keyCmpFuncs = append(t.keyCmpFuncs, chunk.GetCompareFunc(t.joinKeys[i].RetType))
	}
	return err
}

func (t *mergeJoinInnerTable) selectRow(row chunk.Row) {
	t.curSel = append(t.curSel, row.Idx())
}

func (t *mergeJoinInnerTable) selectedRowsIter() chunk.Iterator {
	var iters []chunk.Iterator
	if t.rowContainer.NumChunks() != 0 {
		iters = append(iters, chunk.NewIterator4RowContainer(t.rowContainer))
	}
	if t.curSel != nil {
		t.curChk.SetSel(t.curSel)
		iters = append(iters, chunk.NewIterator4Chunk(t.curChk))
		t.curSel = nil
	}
	if len(iters) == 1 {
		return iters[0]
	}
	// If any of iters has zero length it will be discarded in the following function.
	// If all of them are empty, the returned iterator is also empty.
	// Check out the implementation of multiIterator for more details.
	return chunk.NewMultiIterator(iters...)
}

func (t *mergeJoinInnerTable) rowsWithSameKeyIter() (chunk.Iterator, error) {
	// t.curSel sets to nil, so that it won't overwrite Sel in chunks in RowContainer,
	// it might be unnecessary since merge join only runs single thread. However since we want to hand
	// over the management of a chunk to the RowContainer, to keep the semantic consistent, we set it
	// to nil.
	t.curSel = nil
	t.curChk.SetSel(nil)
	err := t.rowContainer.Reset()
	if err != nil {
		return nil, err
	}

	// no more data.
	if t.firstRow4Key == t.curIter.End() {
		// the iterator is a blank iterator only as a place holder
		return chunk.NewIterator4Chunk(t.curChk), nil
	}
	t.selectRow(t.firstRow4Key)
	for {
		selectedRow, err := t.nextRow()
		// error happens or no more data.
		if err != nil || selectedRow == t.curIter.End() {
			t.firstRow4Key = t.curIter.End()
			return t.selectedRowsIter(), err
		}
		compareResult := compareChunkRow(t.keyCmpFuncs, selectedRow, t.firstRow4Key, t.joinKeys, t.joinKeys)
		if compareResult == 0 {
			t.selectRow(selectedRow)
		} else {
			t.firstRow4Key = selectedRow
			return t.selectedRowsIter(), nil
		}
	}
}

func (t *mergeJoinInnerTable) nextRow() (chunk.Row, error) {
	for {
		if t.curRow == t.curIter.End() {
			err := t.reallocCurChkForReader()
			if err != nil {
				t.curRow = t.curIter.End()
				return t.curRow, err
			}
			oldMemUsage := t.curChk.MemoryUsage()
			err = Next(t.ctx, t.reader, t.curChk)
			// error happens or no more data.
			if err != nil || t.curChk.NumRows() == 0 {
				t.curRow = t.curIter.End()
				return t.curRow, err
			}
			newMemUsage := t.curChk.MemoryUsage()
			t.memTracker.Consume(newMemUsage - oldMemUsage)
			t.curRow = t.curIter.Begin()
		}

		result := t.curRow
		t.curChkInUse = true
		t.curRow = t.curIter.Next()

		if !t.hasNullInJoinKey(result) {
			return result, nil
		}
	}
}

func (t *mergeJoinInnerTable) hasNullInJoinKey(row chunk.Row) bool {
	for _, col := range t.joinKeys {
		ordinal := col.Index
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
}

// reallocCurChkForNext resets "t.curChk" to an empty Chunk to buffer the result of "t.reader".
// It saves the curChk to RowContainer and then allocates a new one from RowContainer.
func (t *mergeJoinInnerTable) reallocCurChkForReader() (err error) {
	if !t.curChkInUse || t.curSel == nil {
		// If "t.curResult" is not in use, we can just reuse it.
		// Note: t.curSel should never be nil. There is a case the chunk is in use but curSel is nil,
		// and it would cause panic, so we add the condition here to avoid it. The case is that when
		// init is called, and the firstRow4Key is set up by nextRow(), however the first several rows
		// contain null value and are skipped, and in the next time the reallocCurChkFOrReader is called
		// the curChkInUse would be set however the curSel is still nil.
		t.curChk.Reset()
		return
	}

	newChk := t.rowContainer.AllocChunk()
	t.memTracker.Consume(newChk.MemoryUsage() - t.curChk.MemoryUsage())

	// hand over the management to the RowContainer, therefore needs to reserve the first row by CopyConstruct
	t.firstRow4Key = t.firstRow4Key.CopyConstruct()
	// curSel be never be nil, since the chunk is in use.
	t.curChk.SetSel(t.curSel)
	err = t.rowContainer.Add(t.curChk)
	if err != nil {
		return err
	}
	t.curChk = newChk
	t.curChk.Reset()
	t.curSel = nil
	t.curChkInUse = false
	t.curIter = chunk.NewIterator4Chunk(t.curChk)
	return
}

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	e.childrenResults = nil
	if e.innerTable.curChk != nil {
		e.innerTable.memTracker.Consume(-e.innerTable.curChk.MemoryUsage())
	}
	e.memTracker = nil
	if e.innerTable.rowContainer != nil {
		if err := e.innerTable.rowContainer.Close(); err != nil {
			return err
		}
	}

	return e.baseExecutor.Close()
}

var innerTableLabel fmt.Stringer = stringutil.StringerStr("innerTable")

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaMergeJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.childrenResults = make([]*chunk.Chunk, 0, len(e.children))
	for _, child := range e.children {
		e.childrenResults = append(e.childrenResults, newFirstChunk(child))
	}

	e.innerTable.memTracker = memory.NewTracker(innerTableLabel, -1)
	e.innerTable.memTracker.AttachTo(e.memTracker)

	return nil
}

func compareChunkRow(cmpFuncs []chunk.CompareFunc, lhsRow, rhsRow chunk.Row, lhsKey, rhsKey []*expression.Column) int {
	for i := range lhsKey {
		cmp := cmpFuncs[i](lhsRow, lhsKey[i].Index, rhsRow, rhsKey[i].Index)
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (e *MergeJoinExec) prepare(ctx context.Context, requiredRows int) error {
	err := e.innerTable.init(ctx, e.ctx, e.childrenResults[e.outerIdx^1])
	if err != nil {
		return err
	}

	err = e.fetchNextInnerRows()
	if err != nil {
		return err
	}

	// init outer table.
	e.outerTable.chk = e.childrenResults[e.outerIdx]
	e.outerTable.iter = chunk.NewIterator4Chunk(e.outerTable.chk)
	e.outerTable.selected = make([]bool, 0, e.maxChunkSize)

	err = e.fetchNextOuterRows(ctx, requiredRows)
	if err != nil {
		return err
	}

	e.prepared = true
	return nil
}

// Next implements the Executor Next interface.
func (e *MergeJoinExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.prepared {
		if err := e.prepare(ctx, req.RequiredRows()); err != nil {
			return err
		}
	}

	for !req.IsFull() {
		hasMore, err := e.joinToChunk(ctx, req)
		if err != nil || !hasMore {
			return err
		}
	}
	return nil
}

func (e *MergeJoinExec) joinToChunk(ctx context.Context, chk *chunk.Chunk) (hasMore bool, err error) {
	for {
		if e.outerTable.row == e.outerTable.iter.End() {
			err = e.fetchNextOuterRows(ctx, chk.RequiredRows()-chk.NumRows())
			if err != nil || e.outerTable.chk.NumRows() == 0 {
				return false, err
			}
		}

		cmpResult := -1
		if e.outerTable.selected[e.outerTable.row.Idx()] && e.innerIter4Row.Len() > 0 {
			cmpResult, err = e.compare(e.outerTable.row, e.innerIter4Row.Current())
			if err != nil {
				return false, err
			}
		}

		if cmpResult > 0 {
			if err = e.fetchNextInnerRows(); err != nil {
				return false, err
			}
			continue
		}

		if cmpResult < 0 {
			e.joiner.onMissMatch(false, e.outerTable.row, chk)
			if err != nil {
				return false, err
			}

			e.outerTable.row = e.outerTable.iter.Next()
			e.outerTable.hasMatch = false
			e.outerTable.hasNull = false

			if chk.IsFull() {
				return true, nil
			}
			continue
		}

		matched, isNull, err := e.joiner.tryToMatchInners(e.outerTable.row, e.innerIter4Row, chk)
		if err != nil {
			return false, err
		}
		e.outerTable.hasMatch = e.outerTable.hasMatch || matched
		e.outerTable.hasNull = e.outerTable.hasNull || isNull

		if e.innerIter4Row.Current() == e.innerIter4Row.End() {
			if !e.outerTable.hasMatch {
				e.joiner.onMissMatch(e.outerTable.hasNull, e.outerTable.row, chk)
			}
			e.outerTable.row = e.outerTable.iter.Next()
			e.outerTable.hasMatch = false
			e.outerTable.hasNull = false
			e.innerIter4Row.Begin()
		}

		if chk.IsFull() {
			return true, err
		}
	}
}

func (e *MergeJoinExec) compare(outerRow, innerRow chunk.Row) (int, error) {
	outerJoinKeys := e.outerTable.keys
	innerJoinKeys := e.innerTable.joinKeys
	for i := range outerJoinKeys {
		cmp, _, err := e.compareFuncs[i](e.ctx, outerJoinKeys[i], innerJoinKeys[i], outerRow, innerRow)
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return int(cmp), nil
		}
	}
	return 0, nil
}

// fetchNextInnerRows fetches the next join group, within which all the rows
// have the same join key, from the inner table.
func (e *MergeJoinExec) fetchNextInnerRows() (err error) {
	e.innerIter4Row, err = e.innerTable.rowsWithSameKeyIter()
	if err != nil {
		return err
	}
	e.innerIter4Row.Begin()
	return nil
}

// fetchNextOuterRows fetches the next Chunk of outer table. Rows in a Chunk
// may not all belong to the same join key, but are guaranteed to be sorted
// according to the join key.
func (e *MergeJoinExec) fetchNextOuterRows(ctx context.Context, requiredRows int) (err error) {
	// It's hard to calculate selectivity if there is any filter or it's inner join,
	// so we just push the requiredRows down when it's outer join and has no filter.
	if e.isOuterJoin && len(e.outerTable.filter) == 0 {
		e.outerTable.chk.SetRequiredRows(requiredRows, e.maxChunkSize)
	}

	err = Next(ctx, e.outerTable.reader, e.outerTable.chk)
	if err != nil {
		return err
	}

	e.outerTable.iter.Begin()
	e.outerTable.selected, err = expression.VectorizedFilter(e.ctx, e.outerTable.filter, e.outerTable.iter, e.outerTable.selected)
	if err != nil {
		return err
	}
	e.outerTable.row = e.outerTable.iter.Begin()
	return nil
}
