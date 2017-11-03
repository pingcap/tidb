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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
)

// MergeJoinExec implements the merge join algorithm.
// This operator assumes that two iterators of both sides
// will provide required order on join condition:
// 1. For equal-join, one of the join key from each side
// matches the order given.
// 2. For other cases its preferred not to use SMJ and operator
// will throw error.
type MergeJoinExec struct {
	ctx      context.Context
	stmtCtx  *variable.StatementContext
	schema   *expression.Schema
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
}

const rowBufferSize = 4096

// rowBlockIterator represents a row block with the same join keys
type rowBlockIterator struct {
	stmtCtx   *variable.StatementContext
	ctx       context.Context
	reader    Executor
	filter    []expression.Expression
	joinKeys  []*expression.Column
	peekedRow Row
	rowCache  []Row
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

func (rb *rowBlockIterator) nextRow() (Row, error) {
	for {
		row, err := rb.reader.Next()
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
	rowCache = append(rowCache, rb.peekedRow)
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
			rowCache = append(rowCache, curRow)
		} else {
			rb.peekedRow = curRow
			return rowCache, nil
		}
	}
}

// Close implements the Executor Close interface.
func (e *MergeJoinExec) Close() error {
	e.resultBuffer = nil

	lErr := e.outerIter.reader.Close()
	if lErr != nil {
		terror.Log(errors.Trace(e.innerIter.reader.Close()))
		return errors.Trace(lErr)
	}
	rErr := e.innerIter.reader.Close()
	if rErr != nil {
		return errors.Trace(rErr)
	}

	return nil
}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open() error {
	e.prepared = false
	e.resultCursor = 0
	e.resultBuffer = nil

	err := e.outerIter.reader.Open()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(e.innerIter.reader.Open())
}

// Schema implements the Executor Schema interface.
func (e *MergeJoinExec) Schema() *expression.Schema {
	return e.schema
}

func compareKeys(stmtCtx *variable.StatementContext,
	leftRow Row, leftKeys []*expression.Column,
	rightRow Row, rightKeys []*expression.Column) (int, error) {
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

		initLen := len(e.resultBuffer)
		e.resultBuffer, err = e.resultGenerator.emitMatchedInners(outer, e.innerRows, e.resultBuffer)
		if err != nil {
			return errors.Trace(err)
		}

		if initLen == len(e.resultBuffer) {
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

func (e *MergeJoinExec) prepare() error {
	e.stmtCtx = e.ctx.GetSessionVars().StmtCtx
	e.resultBuffer = make([]Row, 0, rowBufferSize)

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
func (e *MergeJoinExec) Next() (Row, error) {
	if !e.prepared {
		if err := e.prepare(); err != nil {
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
