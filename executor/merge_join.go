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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	goctx "golang.org/x/net/context"
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

	stmtCtx  *stmtctx.StatementContext
	prepared bool

	outerKeys   []*expression.Column
	innerKeys   []*expression.Column
	outerIter   *readerIterator
	innerIter   *readerIterator
	outerRows   []Row
	innerRows   []Row
	outerFilter []expression.Expression

	resultGenerator joinResultGenerator
	resultBuffer    []Row
	resultCursor    int

	// for chunk execution.
	compareFuncs   []chunk.CompareFunc
	outerIdx       int
	outerChunkRows []chunk.Row
	innerChunkRows []chunk.Row
	outerIter4Row  chunk.Iterator
	innerIter4Row  chunk.Iterator
}

const rowBufferSize = 4096

// readerIterator represents a row block with the same join keys
type readerIterator struct {
	stmtCtx   *stmtctx.StatementContext
	ctx       sessionctx.Context
	reader    Executor
	filter    []expression.Expression
	joinKeys  []*expression.Column
	peekedRow types.Row
	rowCache  []Row
	goCtx     goctx.Context

	// for chunk executions
	sameKeyRows    []chunk.Row
	compareFuncs   []chunk.CompareFunc
	firstRow4Key   chunk.Row
	curRow         chunk.Row
	curResult      *chunk.Chunk
	curIter        *chunk.Iterator4Chunk
	curResultInUse bool
	curSelected    []bool
	resultQueue    []*chunk.Chunk
	resourceQueue  []*chunk.Chunk

	// joinResultGenerator is "nil" means this iterator works on the inner table,
	// otherwise it works on the outer table and is used to emits the un-matched
	// outer rows to "joinResult".
	joinResultGenerator joinResultGenerator
	joinResult          *chunk.Chunk
}

func (ri *readerIterator) init() error {
	if ri.reader == nil || ri.joinKeys == nil || len(ri.joinKeys) == 0 || ri.ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	ri.stmtCtx = ri.ctx.GetSessionVars().StmtCtx
	var err error
	ri.peekedRow, err = ri.nextRow()
	if err != nil {
		return errors.Trace(err)
	}
	ri.rowCache = make([]Row, 0, rowBufferSize)
	return nil
}

func (ri *readerIterator) nextRow() (types.Row, error) {
	for {
		row, err := ri.reader.Next(ri.goCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			return nil, nil
		}
		if ri.filter != nil {
			matched, err := expression.EvalBool(ri.ctx, ri.filter, row)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !matched {
				continue
			}
		}
		return row, nil
	}
}

func (ri *readerIterator) nextBlock() ([]Row, error) {
	if ri.peekedRow == nil {
		return nil, nil
	}
	rowCache := ri.rowCache[0:0:rowBufferSize]
	rowCache = append(rowCache, ri.peekedRow.(types.DatumRow))
	for {
		curRow, err := ri.nextRow()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if curRow == nil {
			ri.peekedRow = nil
			return rowCache, nil
		}
		compareResult, err := compareKeys(ri.stmtCtx, curRow, ri.joinKeys, ri.peekedRow, ri.joinKeys)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if compareResult == 0 {
			rowCache = append(rowCache, curRow.(types.DatumRow))
		} else {
			ri.peekedRow = curRow
			return rowCache, nil
		}
	}
}

func (ri *readerIterator) initForChunk(chk4Reader *chunk.Chunk) (err error) {
	if ri.reader == nil || ri.joinKeys == nil || len(ri.joinKeys) == 0 || ri.ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	ri.stmtCtx = ri.ctx.GetSessionVars().StmtCtx
	ri.curResult = chk4Reader
	ri.curIter = chunk.NewIterator4Chunk(ri.curResult)
	ri.curRow = ri.curIter.End()
	ri.curSelected = make([]bool, 0, ri.ctx.GetSessionVars().MaxChunkSize)
	ri.curResultInUse = false
	ri.resultQueue = append(ri.resultQueue, chk4Reader)
	ri.firstRow4Key, err = ri.nextSelectedRow()
	ri.compareFuncs = make([]chunk.CompareFunc, 0, len(ri.joinKeys))
	for i := range ri.joinKeys {
		ri.compareFuncs = append(ri.compareFuncs, chunk.GetCompareFunc(ri.joinKeys[i].RetType))
	}
	return errors.Trace(err)
}

func (ri *readerIterator) rowsWithSameKey() ([]chunk.Row, error) {
	lastResultIdx := len(ri.resultQueue) - 1
	ri.resourceQueue = append(ri.resourceQueue, ri.resultQueue[0:lastResultIdx]...)
	ri.resultQueue = ri.resultQueue[lastResultIdx:]
	// no more data.
	if ri.firstRow4Key == ri.curIter.End() {
		return nil, nil
	}
	ri.sameKeyRows = ri.sameKeyRows[:0]
	ri.sameKeyRows = append(ri.sameKeyRows, ri.firstRow4Key)
	for {
		selectedRow, err := ri.nextSelectedRow()
		// error happens or no more data.
		if err != nil || selectedRow == ri.curIter.End() {
			ri.firstRow4Key = ri.curIter.End()
			return ri.sameKeyRows, errors.Trace(err)
		}
		compareResult := compareChunkRow(ri.compareFuncs, selectedRow, ri.firstRow4Key, ri.joinKeys, ri.joinKeys)
		if compareResult == 0 {
			ri.sameKeyRows = append(ri.sameKeyRows, selectedRow)
		} else {
			ri.firstRow4Key = selectedRow
			return ri.sameKeyRows, nil
		}
	}
}

func (ri *readerIterator) nextSelectedRow() (chunk.Row, error) {
	for {
		for ; ri.curRow != ri.curIter.End(); ri.curRow = ri.curIter.Next() {
			if ri.curSelected[ri.curRow.Idx()] {
				result := ri.curRow
				ri.curResultInUse = true
				ri.curRow = ri.curIter.Next()
				return result, nil
			} else if ri.joinResultGenerator != nil {
				// If this iterator works on the outer table, we should emit the un-matched outer row to result Chunk.
				err := ri.joinResultGenerator.emitToChunk(ri.curRow, nil, ri.joinResult)
				if err != nil {
					return ri.curIter.End(), errors.Trace(err)
				}
			}
		}
		ri.reallocReaderResult()
		err := ri.reader.NextChunk(ri.goCtx, ri.curResult)
		// error happens or no more data.
		if err != nil || ri.curResult.NumRows() == 0 {
			ri.curRow = ri.curIter.End()
			return ri.curRow, errors.Trace(err)
		}
		ri.curSelected, err = expression.VectorizedFilter(ri.ctx, ri.filter, ri.curIter, ri.curSelected)
		if err != nil {
			ri.curRow = ri.curIter.End()
			return ri.curRow, errors.Trace(err)
		}
		ri.curRow = ri.curIter.Begin()
	}
}

// reallocReaderResult resets "ri.curResult" to an empty Chunk to buffer the result of "ri.reader".
// It pops a Chunk from "ri.resourceQueue" and push it into "ri.resultQueue" immediately.
func (ri *readerIterator) reallocReaderResult() {
	if !ri.curResultInUse {
		// If "ri.curResult" is not in use, we can just reuse it.
		ri.curResult.Reset()
		return
	}

	// Create a new Chunk and append it to "resourceQueue" if there is no more
	// available chunk in "resourceQueue".
	if len(ri.resourceQueue) == 0 {
		ri.resourceQueue = append(ri.resourceQueue, ri.reader.newChunk())
	}

	// NOTE: "ri.curResult" is always the last element of "resultQueue".
	ri.curResult = ri.resourceQueue[0]
	ri.curIter = chunk.NewIterator4Chunk(ri.curResult)
	ri.resourceQueue = ri.resourceQueue[1:]
	ri.resultQueue = append(ri.resultQueue, ri.curResult)
	ri.curResult.Reset()
	ri.curResultInUse = false
}

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	e.resultBuffer = nil
	if err := e.baseExecutor.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open(goCtx goctx.Context) error {
	if err := e.baseExecutor.Open(goCtx); err != nil {
		return errors.Trace(err)
	}
	e.prepared = false
	e.resultCursor = 0
	e.resultBuffer = make([]Row, 0, rowBufferSize)
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

func compareKeys(stmtCtx *stmtctx.StatementContext,
	leftRow types.Row, leftKeys []*expression.Column,
	rightRow types.Row, rightKeys []*expression.Column) (int, error) {
	for i, leftKey := range leftKeys {
		lVal, err := leftKey.Eval(leftRow)
		if err != nil {
			return 0, errors.Trace(err)
		}

		rVal, err := rightKeys[i].Eval(rightRow)
		if err != nil {
			return 0, errors.Trace(err)
		}

		ret, err := lVal.CompareDatum(stmtCtx, &rVal)
		if err != nil {
			return 0, errors.Trace(err)
		}

		if ret != 0 {
			return ret, nil
		}
	}
	return 0, nil
}

func (e *MergeJoinExec) doJoin() (err error) {
	for _, outer := range e.outerRows {
		if e.outerFilter != nil {
			matched, err1 := expression.EvalBool(e.ctx, e.outerFilter, outer)
			if err1 != nil {
				return errors.Trace(err1)
			}
			if !matched {
				e.resultBuffer, err1 = e.resultGenerator.emit(outer, nil, e.resultBuffer)
				if err1 != nil {
					return errors.Trace(err1)
				}
				continue
			}
		}

		e.resultBuffer, err = e.resultGenerator.emit(outer, e.innerRows, e.resultBuffer)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (e *MergeJoinExec) computeJoin() (bool, error) {
	var compareResult int
	var err error
	e.resultBuffer = e.resultBuffer[0:0:rowBufferSize]
	for {
		if e.outerRows == nil || e.innerRows == nil {
			if e.outerRows == nil {
				return false, nil
			}
			compareResult = -1
		} else {
			compareResult, err = compareKeys(e.stmtCtx, e.outerRows[0], e.outerKeys, e.innerRows[0], e.innerKeys)
			if err != nil {
				return false, errors.Trace(err)
			}
		}

		if compareResult > 0 {
			e.innerRows, err = e.innerIter.nextBlock()
			if err != nil {
				return false, errors.Trace(err)
			}
			continue
		}

		initLen := len(e.resultBuffer)
		if compareResult < 0 {
			for _, unMatchedOuter := range e.outerRows {
				e.resultBuffer, err = e.resultGenerator.emit(unMatchedOuter, nil, e.resultBuffer)
				if err != nil {
					return false, errors.Trace(err)
				}
			}
		} else {
			err = e.doJoin()
			if err != nil {
				return false, errors.Trace(err)
			}
			e.innerRows, err = e.innerIter.nextBlock()
			if err != nil {
				return false, errors.Trace(err)
			}
		}
		e.outerRows, err = e.outerIter.nextBlock()
		if err != nil {
			return false, errors.Trace(err)
		}
		if initLen < len(e.resultBuffer) {
			return true, nil
		}
	}
}

func (e *MergeJoinExec) prepare(goCtx goctx.Context, chk *chunk.Chunk) error {
	e.outerIter.goCtx = goCtx
	e.innerIter.goCtx = goCtx
	// prepare for chunk-oriented execution.
	if chk != nil {
		e.outerIter.filter = e.outerFilter
		e.outerIter.joinResultGenerator = e.resultGenerator
		e.outerIter.joinResult = chk
		err := e.outerIter.initForChunk(e.childrenResults[e.outerIdx])
		if err != nil {
			return errors.Trace(err)
		}
		err = e.innerIter.initForChunk(e.childrenResults[e.outerIdx^1])
		if err != nil {
			return errors.Trace(err)
		}

		e.outerChunkRows, err = e.outerIter.rowsWithSameKey()
		if err != nil {
			return errors.Trace(err)
		}
		if e.outerChunkRows != nil {
			e.outerIter4Row = chunk.NewIterator4Slice(e.outerChunkRows)
			e.outerIter4Row.Begin()
		}
		e.innerChunkRows, err = e.innerIter.rowsWithSameKey()
		if err != nil {
			return errors.Trace(err)
		}
		if e.innerChunkRows != nil {
			e.innerIter4Row = chunk.NewIterator4Slice(e.innerChunkRows)
			e.innerIter4Row.Begin()
		}
		e.compareFuncs = e.outerIter.compareFuncs
		e.prepared = true
		return nil
	}

	// prepare for row-oriented execution.
	err := e.outerIter.init()
	if err != nil {
		return errors.Trace(err)
	}
	err = e.innerIter.init()
	if err != nil {
		return errors.Trace(err)
	}

	e.outerRows, err = e.outerIter.nextBlock()
	if err != nil {
		return errors.Trace(err)
	}
	e.innerRows, err = e.innerIter.nextBlock()
	if err != nil {
		return errors.Trace(err)
	}
	e.prepared = true
	return nil
}

// Next implements the Executor Next interface.
func (e *MergeJoinExec) Next(goCtx goctx.Context) (Row, error) {
	if !e.prepared {
		if err := e.prepare(goCtx, nil); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if e.resultCursor >= len(e.resultBuffer) {
		hasMore, err := e.computeJoin()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !hasMore {
			return nil, nil
		}
		e.resultCursor = 0
	}

	result := e.resultBuffer[e.resultCursor]
	e.resultCursor++
	return result, nil
}

// NextChunk implements the Executor NextChunk interface.
func (e *MergeJoinExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if !e.prepared {
		if err := e.prepare(goCtx, chk); err != nil {
			return errors.Trace(err)
		}
	}
	for {
		if chk.NumRows() == e.maxChunkSize {
			return nil
		}

		// reach here means there is no more data in "e.resultChunk"
		hasMore, err := e.joinToChunk(chk)
		if err != nil || !hasMore {
			return errors.Trace(err)
		}
	}
}

func (e *MergeJoinExec) joinToChunk(chk *chunk.Chunk) (hasMore bool, err error) {
	for e.outerChunkRows != nil {
		// Here we set default cmpResult to -1 to emit unmatched outer rows.
		cmpResult := -1
		if e.innerChunkRows != nil {
			cmpResult = compareChunkRow(e.compareFuncs, e.outerChunkRows[0], e.innerChunkRows[0], e.outerKeys, e.innerKeys)
		}
		if cmpResult > 0 {
			if err = e.fetchNextInnerRows(); err != nil {
				return false, errors.Trace(err)
			}
			continue
		}
		if cmpResult < 0 {
			for e.outerIter4Row.Current() != e.outerIter4Row.End() {
				if err = e.resultGenerator.emitToChunk(e.outerIter4Row.Current(), nil, chk); err != nil {
					return false, errors.Trace(err)
				}
				e.outerIter4Row.Next()
				if chk.NumRows() == e.maxChunkSize {
					return true, errors.Trace(err)
				}
			}
			if err = e.fetchNextOuterRows(); err != nil {
				return false, errors.Trace(err)
			}
			continue
		}
		for e.outerIter4Row.Current() != e.outerIter4Row.End() {
			if err = e.resultGenerator.emitToChunk(e.outerIter4Row.Current(), e.innerIter4Row, chk); err != nil {
				return false, errors.Trace(err)
			}
			if e.innerIter4Row.Current() == e.innerIter4Row.End() {
				e.outerIter4Row.Next()
				e.innerIter4Row.Begin()
			}
			if chk.NumRows() == e.maxChunkSize {
				return true, errors.Trace(err)
			}
		}
		if err = e.fetchNextInnerRows(); err != nil {
			return false, errors.Trace(err)
		}
		if err = e.fetchNextOuterRows(); err != nil {
			return false, errors.Trace(err)
		}

	}
	return false, nil
}

func (e *MergeJoinExec) fetchNextInnerRows() (err error) {
	e.innerChunkRows, err = e.innerIter.rowsWithSameKey()
	if err != nil {
		return errors.Trace(err)
	}
	e.innerIter4Row = chunk.NewIterator4Slice(e.innerChunkRows)
	e.innerIter4Row.Begin()
	return nil
}

func (e *MergeJoinExec) fetchNextOuterRows() (err error) {
	e.outerChunkRows, err = e.outerIter.rowsWithSameKey()
	if err != nil {
		return errors.Trace(err)
	}
	e.outerIter4Row = chunk.NewIterator4Slice(e.outerChunkRows)
	e.outerIter4Row.Begin()
	return nil
}
