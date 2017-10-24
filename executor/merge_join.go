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
	"github.com/pingcap/tidb/plan"
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
	// Left is always the driver side

	ctx           context.Context
	stmtCtx       *variable.StatementContext
	leftJoinKeys  []*expression.Column
	rightJoinKeys []*expression.Column
	prepared      bool
	leftFilter    []expression.Expression
	otherFilter   []expression.Expression
	schema        *expression.Schema
	cursor        int

	// Default for both side in case full join

	defaultRightRow Row
	outputBuf       []Row
	leftRowBlock    *rowBlockIterator
	rightRowBlock   *rowBlockIterator
	leftRows        []Row
	rightRows       []Row
	desc            bool
	joinType        plan.JoinType

	// for semi join
	isAntiMode bool
	outputer   joinResultOutputer
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
	var err error
	peekedRow := rb.peekedRow
	var curRow Row
	if peekedRow == nil {
		return nil, nil
	}
	rowCache := rb.rowCache[0:0:rowBufferSize]
	rowCache = append(rowCache, peekedRow)
	for {
		curRow, err = rb.nextRow()
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
	e.outputBuf = nil

	lErr := e.leftRowBlock.reader.Close()
	if lErr != nil {
		terror.Log(errors.Trace(e.rightRowBlock.reader.Close()))
		return errors.Trace(lErr)
	}
	rErr := e.rightRowBlock.reader.Close()
	if rErr != nil {
		return errors.Trace(rErr)
	}

	return nil
}

// Open implements the Executor Open interface.
func (e *MergeJoinExec) Open() error {
	e.prepared = false
	e.cursor = 0
	e.outputBuf = nil
	e.outputer = newMergeJoinOutputer(e.ctx, e.joinType, e.isAntiMode, e.defaultRightRow, e.otherFilter)

	err := e.leftRowBlock.reader.Open()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(e.rightRowBlock.reader.Open())
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

func (e *MergeJoinExec) computeCrossProduct() error {
	var err error
	for _, lRow := range e.leftRows {
		// make up for outer join since we ignored single table conditions previously
		if e.leftFilter != nil {
			var matched bool
			matched, err = expression.EvalBool(e.leftFilter, lRow, e.ctx)
			if err != nil {
				return errors.Trace(err)
			}
			if !matched {
				e.outputer.emitUnMatchedOuter(lRow, e.outputBuf)
				continue
			}
		}
		// Do the real cross product calculation
		initInnerLen := len(e.outputBuf)
		e.outputBuf, err = e.outputer.emitMatchedInners(lRow, e.rightRows, e.outputBuf)
		if err != nil {
			return errors.Trace(err)
		}
		// Even if caught up for left filter
		// no matching but it's outer join
		if initInnerLen == len(e.outputBuf) {
			e.outputBuf = e.outputer.emitUnMatchedOuter(lRow, e.outputBuf)
		}
	}

	return nil
}

func (e *MergeJoinExec) computeJoin() (bool, error) {
	e.outputBuf = e.outputBuf[0:0:rowBufferSize]

	for {
		var compareResult int
		var err error
		if e.leftRows == nil || e.rightRows == nil {
			if e.leftRows != nil && e.rightRows == nil {
				switch e.joinType {
				case plan.LeftOuterJoin, plan.RightOuterJoin, plan.SemiJoin, plan.LeftOuterSemiJoin:
					// left remains and left outer join
					// -1 will make loop continue for left
					compareResult = -1
				}
			} else {
				// inner join or left is nil
				return false, nil
			}
		} else {
			// no nil for either side, compare by first elements in row buffer since its guaranteed
			compareResult, err = compareKeys(e.stmtCtx, e.leftRows[0], e.leftJoinKeys, e.rightRows[0], e.rightJoinKeys)

			if err != nil {
				return false, errors.Trace(err)
			}
			if e.desc {
				compareResult = -compareResult
			}
		}

		// Before moving on, in case of outer join, output the side of the row
		if compareResult > 0 {
			e.rightRows, err = e.rightRowBlock.nextBlock()
			if err != nil {
				return false, errors.Trace(err)
			}
		} else if compareResult < 0 {
			initLen := len(e.outputBuf)
			e.outputBuf = e.outputer.emitUnMatchedOuters(e.leftRows, e.outputBuf)
			e.leftRows, err = e.leftRowBlock.nextBlock()
			if err != nil {
				return false, errors.Trace(err)
			}
			if initLen < len(e.outputBuf) {
				return true, nil
			}
		} else { // key matched, try join with other conditions
			initLen := len(e.outputBuf)

			// Compute cross product when both sides matches
			err := e.computeCrossProduct()
			if err != nil {
				return false, errors.Trace(err)
			}

			e.leftRows, err = e.leftRowBlock.nextBlock()
			if err != nil {
				return false, errors.Trace(err)
			}
			e.rightRows, err = e.rightRowBlock.nextBlock()
			if err != nil {
				return false, errors.Trace(err)
			}
			if initLen < len(e.outputBuf) {
				return true, nil
			}
		}
	}
}

func (e *MergeJoinExec) prepare() error {
	e.stmtCtx = e.ctx.GetSessionVars().StmtCtx
	err := e.leftRowBlock.init()
	if err != nil {
		return errors.Trace(err)
	}
	err = e.rightRowBlock.init()
	if err != nil {
		return errors.Trace(err)
	}
	e.outputBuf = make([]Row, 0, rowBufferSize)

	e.leftRows, err = e.leftRowBlock.nextBlock()
	if err != nil {
		return errors.Trace(err)
	}
	e.rightRows, err = e.rightRowBlock.nextBlock()
	if err != nil {
		return errors.Trace(err)
	}

	e.prepared = true
	return nil
}

// Next implements the Executor Next interface.
func (e *MergeJoinExec) Next() (Row, error) {
	var err error
	var hasMore bool
	if !e.prepared {
		if err = e.prepare(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	if e.cursor >= len(e.outputBuf) {
		hasMore, err = e.computeJoin()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !hasMore {
			return nil, nil
		}
		e.cursor = 0
	}
	row := e.outputBuf[e.cursor]
	e.cursor++
	return row, nil
}
