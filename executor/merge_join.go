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
	preserveLeft  bool // To preserve left side of the relation as in left outer join
	cursor        int
	defaultValues []types.Datum

	// Default for both side in case full join

	defaultRightRow *Row
	outputBuf       []*Row
	leftRowBlock    *rowBlockIterator
	rightRowBlock   *rowBlockIterator
	leftRows        []*Row
	rightRows       []*Row
	desc            bool
	flipSide        bool
}

const rowBufferSize = 4096

type joinBuilder struct {
	context       context.Context
	leftChild     Executor
	rightChild    Executor
	eqConditions  []*expression.ScalarFunction
	leftFilter    []expression.Expression
	rightFilter   []expression.Expression
	otherFilter   []expression.Expression
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

func (b *joinBuilder) LeftFilter(expr []expression.Expression) *joinBuilder {
	b.leftFilter = expr
	return b
}

func (b *joinBuilder) RightFilter(expr []expression.Expression) *joinBuilder {
	b.rightFilter = expr
	return b
}

func (b *joinBuilder) OtherFilter(expr []expression.Expression) *joinBuilder {
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
		if len(eqCond.GetArgs()) != 2 {
			return nil, errors.Annotate(ErrBuildExecutor, "invalid join key for equal condition")
		}
		lKey, ok := eqCond.GetArgs()[0].(*expression.Column)
		if !ok {
			return nil, errors.Annotate(ErrBuildExecutor, "left side of join key must be column for merge join")
		}
		rKey, ok := eqCond.GetArgs()[1].(*expression.Column)
		if !ok {
			return nil, errors.Annotate(ErrBuildExecutor, "right side of join key must be column for merge join")
		}
		leftJoinKeys = append(leftJoinKeys, lKey)
		rightJoinKeys = append(rightJoinKeys, rKey)
	}
	leftRowBlock := &rowBlockIterator{
		ctx:      b.context,
		reader:   b.leftChild,
		filter:   b.leftFilter,
		joinKeys: leftJoinKeys,
	}

	rightRowBlock := &rowBlockIterator{
		ctx:      b.context,
		reader:   b.rightChild,
		filter:   b.rightFilter,
		joinKeys: rightJoinKeys,
	}

	exec := &MergeJoinExec{
		ctx:           b.context,
		leftJoinKeys:  leftJoinKeys,
		rightJoinKeys: rightJoinKeys,
		leftRowBlock:  leftRowBlock,
		rightRowBlock: rightRowBlock,
		otherFilter:   b.otherFilter,
		schema:        b.schema,
		desc:          assumeSortedDesc,
	}

	switch b.joinType {
	case plan.LeftOuterJoin:
		exec.leftRowBlock.filter = nil
		exec.leftFilter = b.leftFilter
		exec.preserveLeft = true
		exec.defaultRightRow = &Row{Data: b.defaultValues}
	case plan.RightOuterJoin:
		exec.leftRowBlock = rightRowBlock
		exec.rightRowBlock = leftRowBlock
		exec.leftRowBlock.filter = nil
		exec.leftFilter = b.leftFilter
		exec.preserveLeft = true
		exec.defaultRightRow = &Row{Data: b.defaultValues}
		exec.flipSide = true
		exec.leftJoinKeys = rightJoinKeys
		exec.rightJoinKeys = leftJoinKeys
	case plan.InnerJoin:
	default:
		return nil, errors.Annotate(ErrBuildExecutor, "unknown join type")
	}
	return exec, nil
}

// rowBlockIterator represents a row block with the same join keys
type rowBlockIterator struct {
	stmtCtx   *variable.StatementContext
	ctx       context.Context
	reader    Executor
	filter    []expression.Expression
	joinKeys  []*expression.Column
	peekedRow *Row
	rowCache  []*Row
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
	rb.rowCache = make([]*Row, 0, rowBufferSize)

	return nil
}

func (rb *rowBlockIterator) nextRow() (*Row, error) {
	for {
		row, err := rb.reader.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row == nil {
			return nil, nil
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
	if lErr != nil {
		e.rightRowBlock.reader.Close()
		return errors.Trace(lErr)
	}
	rErr := e.rightRowBlock.reader.Close()
	if rErr != nil {
		return errors.Trace(rErr)
	}

	return nil
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

func (e *MergeJoinExec) outputJoinRow(leftRow *Row, rightRow *Row) {
	var joinedRow *Row
	if e.flipSide {
		joinedRow = makeJoinRow(rightRow, leftRow)
	} else {
		joinedRow = makeJoinRow(leftRow, rightRow)
	}
	e.outputBuf = append(e.outputBuf, joinedRow)
}

func (e *MergeJoinExec) outputFilteredJoinRow(leftRow *Row, rightRow *Row) error {
	var joinedRow *Row
	if e.flipSide {
		joinedRow = makeJoinRow(rightRow, leftRow)
	} else {
		joinedRow = makeJoinRow(leftRow, rightRow)
	}

	if e.otherFilter != nil {
		matched, err := expression.EvalBool(e.otherFilter, joinedRow.Data, e.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !matched {
			return nil
		}
	}
	e.outputBuf = append(e.outputBuf, joinedRow)
	return nil
}

func (e *MergeJoinExec) tryOutputLeftRows() error {
	if e.preserveLeft {
		for _, lRow := range e.leftRows {
			e.outputJoinRow(lRow, e.defaultRightRow)
		}
	}
	return nil
}

func (e *MergeJoinExec) computeCrossProduct() error {
	var err error
	for _, lRow := range e.leftRows {
		// make up for outer join since we ignored single table conditions previously
		if e.leftFilter != nil {
			matched, err := expression.EvalBool(e.leftFilter, lRow.Data, e.ctx)
			if err != nil {
				return errors.Trace(err)
			}
			if !matched {
				// as all right join converted to left, we only output left side if no match and continue
				if e.preserveLeft {
					e.outputJoinRow(lRow, e.defaultRightRow)
				}
				continue
			}
		}
		// Do the real cross product calculation
		initInnerLen := len(e.outputBuf)
		for _, rRow := range e.rightRows {
			err = e.outputFilteredJoinRow(lRow, rRow)
			if err != nil {
				return errors.Trace(err)
			}
		}
		// Even if caught up for left filter
		// no matching but it's outer join
		if e.preserveLeft && initInnerLen == len(e.outputBuf) {
			e.outputJoinRow(lRow, e.defaultRightRow)
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
			if e.leftRows != nil && e.rightRows == nil && e.preserveLeft {
				// left remains and left outer join
				// -1 will make loop continue for left
				compareResult = -1
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
			err := e.tryOutputLeftRows()
			if err != nil {
				return false, errors.Trace(err)
			}
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
	e.outputBuf = make([]*Row, 0, rowBufferSize)

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
