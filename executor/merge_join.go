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
	"github.com/pingcap/tidb/util/types"
	"sync/atomic"
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
	leftFilter    expression.Expression
	rightFilter   expression.Expression
	otherFilter   expression.Expression
	schema        *expression.Schema
	preserveLeft  bool
	preserveRight bool
	cursor        int
	defaultValues []types.Datum
	// Default for both side in case full join
	defaultLeftRow  *Row
	defaultRightRow *Row
	outputBuf       []*Row
	finished        atomic.Value

	leftRowBlock  rowBlockIterator
	rightRowBlock rowBlockIterator
	leftRows      []*Row
	rightRows     []*Row
	desc          bool
}

const rowBufferSize = 4096

// NewMergeJoinExec is the Constructor for mergeJoinExec
func NewMergeJoinExec(
	ctx context.Context,
	leftJoinKeys []*expression.Column,
	rightJoinKeys []*expression.Column,
	leftReader Executor,
	rightReader Executor,
	leftFilter expression.Expression,
	rightFilter expression.Expression,
	otherFilter expression.Expression,
	schema *expression.Schema,
	joinType plan.JoinType,
	desc bool,
	defaultValues []types.Datum) *MergeJoinExec {
	exec := new(MergeJoinExec)
	exec.ctx = ctx
	exec.leftJoinKeys = leftJoinKeys
	exec.rightJoinKeys = rightJoinKeys
	exec.otherFilter = otherFilter
	exec.schema = schema
	exec.desc = desc

	exec.leftRowBlock = rowBlockIterator{
		ctx:      ctx,
		reader:   leftReader,
		filter:   leftFilter,
		joinKeys: leftJoinKeys,
	}

	exec.rightRowBlock = rowBlockIterator{
		ctx:      ctx,
		reader:   rightReader,
		filter:   rightFilter,
		joinKeys: rightJoinKeys,
	}

	exec.preserveLeft = false
	exec.preserveRight = false
	switch joinType {
	case plan.LeftOuterJoin:
		exec.leftRowBlock.filter = nil
		exec.leftFilter = leftFilter
		exec.preserveLeft = true
		exec.defaultRightRow = &Row{Data: defaultValues}
	case plan.RightOuterJoin:
		exec.rightRowBlock.filter = nil
		exec.rightFilter = rightFilter
		exec.preserveRight = true
		exec.defaultLeftRow = &Row{Data: defaultValues}
	case plan.InnerJoin:
		exec.preserveLeft = false
		exec.preserveRight = false
	default:
		exec.preserveLeft = true
		exec.preserveRight = true
		panic("Full Join not implemented for Merge Join Strategy.")
	}
	return exec
}

// Represent a row block with the same join keys
type rowBlockIterator struct {
	stmtCtx   *variable.StatementContext
	ctx       context.Context
	reader    Executor
	filter    expression.Expression
	joinKeys  []*expression.Column
	peekedRow *Row
}

func (rb *rowBlockIterator) init() (bool, error) {
	if rb.reader == nil || rb.joinKeys == nil || len(rb.joinKeys) == 0 || rb.ctx == nil {
		return false, errors.Errorf("Invalid arguments: Empty arguments detected.")
	}
	rb.stmtCtx = rb.ctx.GetSessionVars().StmtCtx
	var err error
	rb.peekedRow, err = rb.nextRow()
	if err != nil {
		return false, errors.Trace(err)
	}

	return rb.peekedRow == nil, nil
}

func (rb *rowBlockIterator) nextRow() (*Row, error) {
	for {
		row, err := rb.reader.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			rb.reader.Close()
			return nil, nil
		}
		if rb.filter != nil {
			var matched bool
			matched, err = expression.EvalBool(rb.filter, row.Data, rb.ctx)
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

func (rb *rowBlockIterator) nextBlock() ([]*Row, error) {
	var err error
	peekedRow := rb.peekedRow
	var curRow *Row
	if peekedRow == nil {
		return nil, nil
	}
	rowCache := []*Row{peekedRow}
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
	e.finished.Store(true)

	e.prepared = false
	e.cursor = 0
	e.outputBuf = nil

	lErr := e.leftRowBlock.reader.Close()
	rErr := e.rightRowBlock.reader.Close()
	if lErr != nil {
		return lErr
	}
	return rErr
}

// Schema implements the Executor Schema interface.
func (e *MergeJoinExec) Schema() *expression.Schema {
	return e.schema
}

func compareKeys(stmtCtx *variable.StatementContext,
	leftRow *Row, leftKeys []*expression.Column,
	rightRow *Row, rightKeys []*expression.Column) (int, error) {
	for i, leftKey := range leftKeys {
		lVal, err := leftKey.Eval(leftRow.Data)
		if err != nil {
			return 0, errors.Trace(err)
		}

		rVal, err := rightKeys[i].Eval(rightRow.Data)
		if err != nil {
			return 0, errors.Trace(err)
		}

		ret, err := lVal.CompareDatum(stmtCtx, rVal)
		if err != nil {
			return 0, errors.Trace(err)
		}

		if ret != 0 {
			return ret, nil
		}
	}
	return 0, nil
}

func (e *MergeJoinExec) outputJoinRow(leftRow *Row, rightRow *Row) error {
	var err error
	joinedRow := makeJoinRow(leftRow, rightRow)
	if e.otherFilter != nil {
		matched, err := expression.EvalBool(e.otherFilter, joinedRow.Data, e.ctx)
		if err != nil && matched {
			e.outputBuf = append(e.outputBuf, joinedRow)
		}
	} else {
		e.outputBuf = append(e.outputBuf, joinedRow)
	}
	return err
}

func (e *MergeJoinExec) computeJoin() (bool, error) {
	e.outputBuf = e.outputBuf[0 : 0 : rowBufferSize]

	for {
		var compareResult int
		var err error
		if e.leftRows == nil || e.rightRows == nil {
			if e.leftRows == nil && e.rightRows != nil && e.preserveRight {
				// right remains and right/full outer join
				compareResult = 1
			} else if e.rightRows == nil && e.leftRows != nil && e.preserveLeft {
				// left remains and left/full outer join
				compareResult = -1
			} else {
				return false, nil
			}
		} else {
			compareResult, err = compareKeys(e.stmtCtx, e.leftRows[0], e.leftJoinKeys, e.rightRows[0], e.rightJoinKeys)

			if e.desc {
				compareResult = -compareResult
			}
			if err != nil {
				return false, errors.Trace(err)
			}
		}

		// Before moving on, in case of outer join, output the side of the row
		if compareResult > 0 {
			needReturn := false
			if e.preserveRight {
				for _, rRow := range e.rightRows {
					err = e.outputJoinRow(e.defaultLeftRow, rRow)
					if err != nil {
						return false, err
					}
				}
				needReturn = true
			}
			e.rightRows, err = e.rightRowBlock.nextBlock()
			if err != nil {
				return false, errors.Trace(err)
			}
			if needReturn {
				return true, nil
			}
			continue
		} else if compareResult < 0 {
			needReturn := false
			if e.preserveLeft {
				for _, lRow := range e.leftRows {
					err = e.outputJoinRow(lRow, e.defaultRightRow)
					if err != nil {
						return false, err
					}
				}
				needReturn = true
			}
			e.leftRows, err = e.leftRowBlock.nextBlock()
			if err != nil {
				return false, errors.Trace(err)
			}
			if needReturn {
				return true, nil
			}
			continue
		} else {
			for _, lRow := range e.leftRows {
				// make up for outer join since we ignored single table conditions previously
				if e.leftFilter != nil {
					var matched bool
					matched, err = expression.EvalBool(e.leftFilter, lRow.Data, e.ctx)
					if err != nil {
						return false, errors.Trace(err)
					}
					if !matched {
						continue
					}
				}
				for _, rRow := range e.rightRows {
					if e.rightFilter != nil {
						var matched bool
						matched, err = expression.EvalBool(e.rightFilter, rRow.Data, e.ctx)
						if err != nil {
							return false, errors.Trace(err)
						}
						if !matched {
							continue
						}
					}
					err = e.outputJoinRow(lRow, rRow)
					if err != nil {
						return false, err
					}
				}
			}
			e.leftRows, err = e.leftRowBlock.nextBlock()
			if err != nil {
				return false, err
			}
			e.rightRows, err = e.rightRowBlock.nextBlock()
			if err != nil {
				return false, err
			}
			return true, nil
		}
	}
}

func (e *MergeJoinExec) prepare() error {
	e.finished.Store(false)
	e.stmtCtx = e.ctx.GetSessionVars().StmtCtx
	e.leftRowBlock.init()
	e.rightRowBlock.init()
	e.outputBuf = make([]*Row, 0, rowBufferSize)

	var err error
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
func (e *MergeJoinExec) Next() (*Row, error) {
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
			e.finished.Store(true)
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
