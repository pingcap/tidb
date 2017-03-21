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
	"os/exec"
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
	preserveLeft  bool // To perserve left side of the relation as in left outer join
	preserveRight bool // To perserve right side of the relation as in right outer join
	cursor        int
	defaultValues []types.Datum
	// Default for both side in case full join
	defaultLeftRow  *Row
	defaultRightRow *Row
	outputBuf       []*Row

	leftRowBlock  rowBlockIterator
	rightRowBlock rowBlockIterator
	leftRows      []*Row
	rightRows     []*Row
	desc          bool
}

const rowBufferSize = 4096

type joinBuilder struct {
	context       context.Context
	leftChild     Executor
	rightChild    Executor
	eqConditions  []*expression.ScalarFunction
	leftFilter    expression.Expression
	rightFilter   expression.Expression
	otherFilter   expression.Expression
	schema        *expression.Schema
	joinType      plan.JoinType
	defaultValues []types.Datum
}

func (b *joinBuilder) Context(context context.Context) *joinBuilder {
	b.context = context
	return b
}

func (b *joinBuilder) EqualConditions(conds []*expression.ScalarFunction) *joinBuilder {
	b.eqConditions = conds
	return b
}

func (b *joinBuilder) LeftChild(exec Executor) *joinBuilder {
	b.leftChild = exec
	return b
}

func (b *joinBuilder) RightChild(exec Executor) *joinBuilder {
	b.rightChild = exec
	return b
}

func (b *joinBuilder) LeftFilter(expr expression.Expression) *joinBuilder {
	b.leftFilter = expr
	return b
}

func (b *joinBuilder) RightFilter(expr expression.Expression) *joinBuilder {
	b.rightFilter = expr
	return b
}

func (b *joinBuilder) OtherFilter(expr expression.Expression) *joinBuilder {
	b.otherFilter = expr
	return b
}

func (b *joinBuilder) Schema(schema *expression.Schema) *joinBuilder {
	b.schema = schema
	return b
}

func (b *joinBuilder) JoinType(joinType plan.JoinType) *joinBuilder {
	b.joinType = joinType
	return b
}

func (b *joinBuilder) DefaultVals(defaultValues []types.Datum) *joinBuilder {
	b.defaultValues = defaultValues
	return b
}

func (b *joinBuilder) BuildMergeJoin(assumeSortedDesc bool) (*MergeJoinExec, error) {
	var leftJoinKeys, rightJoinKeys []*expression.Column
	for _, eqCond := range b.eqConditions {
		lKey, _ := eqCond.GetArgs()[0].(*expression.Column)
		rKey, _ := eqCond.GetArgs()[1].(*expression.Column)
		leftJoinKeys = append(leftJoinKeys, lKey)
		rightJoinKeys = append(rightJoinKeys, rKey)
	}
	exec := &MergeJoinExec{
		ctx:			b.context,
		leftJoinKeys:	leftJoinKeys,
		rightJoinKeys:	rightJoinKeys,
		otherFilter:	b.otherFilter,
		schema:			b.schema,
		desc:			assumeSortedDesc,
	}

	exec.leftRowBlock = rowBlockIterator{
		ctx:      b.context,
		reader:   b.leftChild,
		filter:   b.leftFilter,
		joinKeys: leftJoinKeys,
	}

	exec.rightRowBlock = rowBlockIterator{
		ctx:      b.context,
		reader:   b.rightChild,
		filter:   b.rightFilter,
		joinKeys: rightJoinKeys,
	}

	switch b.joinType {
	case plan.LeftOuterJoin:
		exec.leftRowBlock.filter = nil
		exec.leftFilter = b.leftFilter
		exec.preserveLeft = true
		exec.defaultRightRow = &Row{Data: b.defaultValues}
	case plan.RightOuterJoin:
		exec.rightRowBlock.filter = nil
		exec.rightFilter = b.rightFilter
		exec.preserveRight = true
		exec.defaultLeftRow = &Row{Data: b.defaultValues}
	case plan.InnerJoin:
		exec.preserveLeft = false
		exec.preserveRight = false
	default:
		exec.preserveLeft = true
		exec.preserveRight = true
		panic("Full Join not implemented for Merge Join Strategy.")
	}
	return exec, nil
}

// Represent a row block with the same join keys
type rowBlockIterator struct {
	stmtCtx   *variable.StatementContext
	ctx       context.Context
	reader    Executor
	filter    expression.Expression
	joinKeys  []*expression.Column
	peekedRow *Row
	rowCache  []*Row
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
	rb.rowCache = make([]*Row, 0, rowBufferSize)

	return rb.peekedRow == nil, nil
}

func (rb *rowBlockIterator) nextRow() (*Row, error) {
	for {
		row, err := rb.reader.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			err = rb.reader.Close()
			return nil, err
		}
		if rb.filter != nil {
			matched, err := expression.EvalBool(rb.filter, row.Data, rb.ctx)
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
	e.outputBuf = e.outputBuf[0:0:rowBufferSize]

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
			initLen := len(e.outputBuf)
			if e.preserveRight {
				for _, rRow := range e.rightRows {
					err = e.outputJoinRow(e.defaultLeftRow, rRow)
					if err != nil {
						return false, err
					}
				}
			}
			e.rightRows, err = e.rightRowBlock.nextBlock()
			if err != nil {
				return false, errors.Trace(err)
			}
			if initLen < len(e.outputBuf) {
				return true, nil
			}
			continue
		} else if compareResult < 0 {
			initLen := len(e.outputBuf)
			if e.preserveLeft {
				for _, lRow := range e.leftRows {
					err = e.outputJoinRow(lRow, e.defaultRightRow)
					if err != nil {
						return false, err
					}
				}
			}
			e.leftRows, err = e.leftRowBlock.nextBlock()
			if err != nil {
				return false, errors.Trace(err)
			}
			if initLen < len(e.outputBuf) {
				return true, nil
			}
			continue
		} else {
			initLen := len(e.outputBuf)
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
			if initLen < len(e.outputBuf) {
				return true, nil
			}
			return false, nil
		}
	}
}

func (e *MergeJoinExec) prepare() error {
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
