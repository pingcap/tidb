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
	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
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
	outerIter   *rowBlockIterator
	innerIter   *rowBlockIterator
	outerRows   []Row
	innerRows   []Row
	outerFilter []expression.Expression

	resultGenerator joinResultGenerator
	resultBuffer    []Row
	resultCursor    int

	// for chunk execution.
	resultChunk    *chunk.Chunk
	outerChunkRows []chunk.Row
	innerChunkRows []chunk.Row
}

const rowBufferSize = 4096

// rowBlockIterator represents a row block with the same join keys
type rowBlockIterator struct {
	stmtCtx   *stmtctx.StatementContext
	ctx       context.Context
	reader    Executor
	filter    []expression.Expression
	joinKeys  []*expression.Column
	peekedRow types.Row
	rowCache  []Row

	// for chunk executions
	firstRowForKey  chunk.Row
	curRow          chunk.Row
	curChunk        *chunk.Chunk
	curChunkInUse   bool
	curSelected     []bool
	resultPool      []*chunk.Chunk
	resourcePool    []*chunk.Chunk
	resultGenerator joinResultGenerator
	isOuter         bool
}

func (rb *rowBlockIterator) init() error {
	if rb.reader == nil || rb.joinKeys == nil || len(rb.joinKeys) == 0 || rb.ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	rb.stmtCtx = rb.ctx.GetSessionVars().StmtCtx
	var err error
	rb.peekedRow, err = rb.nextRow()
	if err != nil {
		return errors.Trace(err)
	}
	rb.rowCache = make([]Row, 0, rowBufferSize)
	return nil
}

func (rb *rowBlockIterator) nextRow() (types.Row, error) {
	goCtx := goctx.TODO()
	for {
		row, err := rb.reader.Next(goCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			return nil, nil
		}
		if rb.filter != nil {
			matched, err := expression.EvalBool(rb.filter, row, rb.ctx)
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

func (rb *rowBlockIterator) nextBlock() ([]Row, error) {
	if rb.peekedRow == nil {
		return nil, nil
	}
	rowCache := rb.rowCache[0:0:rowBufferSize]
	rowCache = append(rowCache, rb.peekedRow.(types.DatumRow))
	for {
		curRow, err := rb.nextRow()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if curRow == nil {
			rb.peekedRow = nil
			return rowCache, nil
		}
		compareResult, err := compareKeys(rb.stmtCtx, curRow, rb.joinKeys, rb.peekedRow, rb.joinKeys)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if compareResult == 0 {
			rowCache = append(rowCache, curRow.(types.DatumRow))
		} else {
			rb.peekedRow = curRow
			return rowCache, nil
		}
	}
}

func (rb *rowBlockIterator) initForChunk(chk4Reader, chk4UnSelectedOuters *chunk.Chunk) (err error) {
	if rb.reader == nil || rb.joinKeys == nil || len(rb.joinKeys) == 0 || rb.ctx == nil {
		return errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	rb.stmtCtx = rb.ctx.GetSessionVars().StmtCtx
	rb.curChunk = chk4Reader
	rb.curSelected = make([]bool, 0, rb.ctx.GetSessionVars().MaxChunkSize)
	rb.curRow = chk4Reader.End()
	rb.curChunkInUse = false
	rb.resultPool = append(rb.resultPool, chk4Reader)
	rb.firstRowForKey, err = rb.nextSelectedRow(chk4UnSelectedOuters)
	return errors.Trace(err)
}

func (rb *rowBlockIterator) rowsWithSameKey(chk4UnSelectedOuters *chunk.Chunk) (rows []chunk.Row, err error) {
	rb.resourcePool = append(rb.resourcePool, rb.resultPool[0:len(rb.resultPool)-1]...)
	rb.resultPool = rb.resultPool[len(rb.resultPool)-1:]
	// no more data.
	if rb.firstRowForKey == rb.curChunk.End() {
		return nil, nil
	}
	rows = append(rows, rb.firstRowForKey)
	for {
		selectedRow, err := rb.nextSelectedRow(chk4UnSelectedOuters)
		// error happens or no more data.
		if err != nil || selectedRow == rb.curChunk.End() {
			rb.firstRowForKey = rb.curChunk.End()
			return rows, errors.Trace(err)
		}
		compareResult, err := compareKeys(rb.stmtCtx, selectedRow, rb.joinKeys, rb.firstRowForKey, rb.joinKeys)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if compareResult == 0 {
			rows = append(rows, selectedRow)
		} else {
			rb.firstRowForKey = selectedRow
			return rows, nil
		}
	}
}

func (rb *rowBlockIterator) nextSelectedRow(chk4UnSelectedOuters *chunk.Chunk) (chunk.Row, error) {
	for {
		for ; rb.curRow != rb.curChunk.End(); rb.curRow = rb.curRow.Next() {
			if rb.curSelected[rb.curRow.Idx()] {
				result := rb.curRow
				rb.curChunkInUse = true
				rb.curRow = rb.curRow.Next()
				return result, nil
			} else if rb.isOuter {
				rb.resultGenerator.emitUnMatchedOuterToChunk(rb.curRow, chk4UnSelectedOuters)
			}
		}
		if !rb.curChunkInUse {
			rb.resourcePool = append(rb.resourcePool, rb.resultPool[len(rb.resultPool)-1])
			rb.resultPool = rb.resultPool[:len(rb.resultPool)-1]
		}
		if len(rb.resourcePool) == 0 {
			rb.resourcePool = append(rb.resourcePool, rb.reader.newChunk())
		}
		rb.curChunk = rb.resourcePool[0]
		rb.resourcePool = rb.resourcePool[1:]
		rb.resultPool = append(rb.resultPool, rb.curChunk)
		rb.curChunkInUse = false
		rb.curRow = rb.curChunk.Begin()
		rb.curChunk.Reset()
		err := rb.reader.NextChunk(rb.curChunk)
		// error happens or no more data.
		if err != nil || rb.curChunk.NumRows() == 0 {
			return rb.curChunk.End(), errors.Trace(err)
		}
		rb.curSelected, err = expression.VectorizedFilter(rb.ctx, rb.filter, rb.curChunk, rb.curSelected)
		if err != nil {
			return rb.curChunk.End(), errors.Trace(err)
		}
	}
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
			matched, err1 := expression.EvalBool(e.outerFilter, outer, e.ctx)
			if err1 != nil {
				return errors.Trace(err1)
			}
			if !matched {
				e.resultBuffer = e.resultGenerator.emitUnMatchedOuter(outer, e.resultBuffer)
				continue
			}
		}

		matched := false
		e.resultBuffer, matched, err = e.resultGenerator.emitMatchedInners(outer, e.innerRows, e.resultBuffer)
		if err != nil {
			return errors.Trace(err)
		}
		if !matched {
			e.resultBuffer = e.resultGenerator.emitUnMatchedOuter(outer, e.resultBuffer)
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
			e.resultBuffer = e.resultGenerator.emitUnMatchedOuters(e.outerRows, e.resultBuffer)
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

func (e *MergeJoinExec) prepare(forChunk bool) error {
	// prepare for chunk-oriented execution.
	if forChunk {
		e.resultChunk = e.newChunk()
		e.outerIter.filter = e.outerFilter
		e.outerIter.resultGenerator = e.resultGenerator
		e.outerIter.isOuter = true
		e.innerIter.isOuter = false
		outerIdx := e.resultGenerator.outerIdx()
		err := e.outerIter.initForChunk(e.childrenResults[outerIdx], e.resultChunk)
		if err != nil {
			return errors.Trace(err)
		}
		err = e.innerIter.initForChunk(e.childrenResults[outerIdx^1], nil)
		if err != nil {
			return errors.Trace(err)
		}

		e.outerChunkRows, err = e.outerIter.rowsWithSameKey(e.resultChunk)
		if err != nil {
			return errors.Trace(err)
		}
		e.innerChunkRows, err = e.innerIter.rowsWithSameKey(nil)
		if err != nil {
			return errors.Trace(err)
		}
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
		if err := e.prepare(false); err != nil {
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
func (e *MergeJoinExec) NextChunk(chk *chunk.Chunk) error {
	chk.Reset()
	if !e.prepared {
		if err := e.prepare(true); err != nil {
			return errors.Trace(err)
		}
	}
	for {
		numRequiredRows := e.ctx.GetSessionVars().MaxChunkSize - chk.NumRows()
		numRetainedRows := e.resultChunk.NumRows()
		numAppendedRows := mathutil.Min(numRequiredRows, numRetainedRows)

		chk.Append(e.resultChunk, e.resultChunk.NumRows()-numAppendedRows, e.resultChunk.NumRows())
		e.resultChunk.TruncateTo(e.resultChunk.NumRows() - numAppendedRows)

		if chk.NumRows() == e.ctx.GetSessionVars().MaxChunkSize {
			return nil
		}

		// reach here means there no more data in "e.resultChunk"
		hasMore, err := e.joinToChunk(e.resultChunk)
		if err != nil || !hasMore {
			return errors.Trace(err)
		}
	}
}

func (e *MergeJoinExec) joinToChunk(chk *chunk.Chunk) (bool, error) {
	var (
		cmpResult int
		err       error
	)
	for {
		if e.outerChunkRows == nil {
			return false, nil
		}

		if e.innerChunkRows == nil {
			// here we set cmpResult to -1 to emit unmatched outer rows.
			cmpResult = -1
		} else {
			cmpResult, err = compareKeys(e.stmtCtx, e.outerChunkRows[0], e.outerKeys, e.innerChunkRows[0], e.innerKeys)
			if err != nil {
				return false, errors.Trace(err)
			}
			if cmpResult > 0 {
				e.innerChunkRows, err = e.innerIter.rowsWithSameKey(nil)
				if err != nil {
					return false, errors.Trace(err)
				}
				continue
			}
		}

		// reach here, cmpResult <= 0 is guaranteed.
		if cmpResult < 0 {
			e.resultGenerator.emitUnMatchedOutersToChunk(e.outerChunkRows, chk)
		} else {
			for _, outer := range e.outerChunkRows {
				matched, err1 := e.resultGenerator.emitMatchedInnersToChunk(outer, e.innerChunkRows, chk)
				if err1 != nil {
					return false, errors.Trace(err1)
				}
				if !matched {
					e.resultGenerator.emitUnMatchedOuterToChunk(outer, chk)
				}
			}
			e.innerChunkRows, err = e.innerIter.rowsWithSameKey(nil)
			if err != nil {
				return false, errors.Trace(err)
			}
		}
		e.outerChunkRows, err = e.outerIter.rowsWithSameKey(chk)
		if err != nil {
			return false, errors.Trace(err)
		}
		if chk.NumRows() > 0 {
			return true, nil
		}
	}
}
