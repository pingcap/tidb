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
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	_ joinResultGenerator = &semiJoinResultGenerator{}
	_ joinResultGenerator = &antiSemiJoinResultGenerator{}
	_ joinResultGenerator = &leftOuterSemiJoinResultGenerator{}
	_ joinResultGenerator = &antiLeftOuterSemiJoinResultGenerator{}
	_ joinResultGenerator = &leftOuterJoinResultGenerator{}
	_ joinResultGenerator = &rightOuterJoinResultGenerator{}
	_ joinResultGenerator = &innerJoinResultGenerator{}
)

// joinResultGenerator is used to generate join results according the join type, see every implementor for detailed information.
type joinResultGenerator interface {
	// emit tries to join an outer row with a batch of inner rows.
	// When inners == nil or inners.Len() == 0, it means that the outer row can not be joined with any inner row:
	//     1. SemiJoin:              unmatched outer row is ignored.
	//     2. AntiSemiJoin:          unmatched outer row is appended to the result buffer.
	//     3. LeftOuterSemiJoin:     unmatched outer row is appended with 0 and appended to the result buffer.
	//     4. AntiLeftOuterSemiJoin: unmatched outer row is appended with 1 and appended to the result buffer.
	//     5. LeftOuterJoin:         unmatched outer row is joined with a row of NULLs and appended to the result buffer.
	//     6. RightOuterJoin:        unmatched outer row is joined with a row of NULLs and appended to the result buffer.
	//     7. InnerJoin:             unmatched outer row is ignored.
	// When inners.Len != 0 but all the joined rows are filtered, this means that the outer row is unmatched and the above action is tacked as well.
	// Otherwise, the outer row is matched and some joined rows is appended to the `chk`.
	// The size of `chk` is MaxChunkSize at most.
	emit(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) error
}

func newJoinResultGenerator(ctx sessionctx.Context, joinType plan.JoinType,
	outerIsRight bool, defaultInner Row, filter []expression.Expression,
	lhsColTypes, rhsColTypes []*types.FieldType) joinResultGenerator {
	base := baseJoinResultGenerator{
		ctx:          ctx,
		conditions:   filter,
		outerIsRight: outerIsRight,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
	colTypes := make([]*types.FieldType, 0, len(lhsColTypes)+len(rhsColTypes))
	colTypes = append(colTypes, lhsColTypes...)
	colTypes = append(colTypes, rhsColTypes...)
	base.chk = chunk.NewChunk(colTypes)
	base.selected = make([]bool, 0, chunk.InitialCapacity)
	if joinType == plan.LeftOuterJoin || joinType == plan.RightOuterJoin {
		innerColTypes := lhsColTypes
		if !outerIsRight {
			innerColTypes = rhsColTypes
		}
		base.initDefaultInner(innerColTypes, defaultInner)
	}
	switch joinType {
	case plan.SemiJoin:
		return &semiJoinResultGenerator{base}
	case plan.AntiSemiJoin:
		return &antiSemiJoinResultGenerator{base}
	case plan.LeftOuterSemiJoin:
		return &leftOuterSemiJoinResultGenerator{base}
	case plan.AntiLeftOuterSemiJoin:
		return &antiLeftOuterSemiJoinResultGenerator{base}
	case plan.LeftOuterJoin:
		return &leftOuterJoinResultGenerator{base}
	case plan.RightOuterJoin:
		return &rightOuterJoinResultGenerator{base}
	case plan.InnerJoin:
		return &innerJoinResultGenerator{base}
	}
	panic("unsupported join type in func newJoinResultGenerator()")
}

// baseJoinResultGenerator is not thread-safe,
// so we should build individual generator for every join goroutine.
type baseJoinResultGenerator struct {
	ctx          sessionctx.Context
	conditions   []expression.Expression
	defaultInner chunk.Row
	outerIsRight bool
	chk          *chunk.Chunk
	selected     []bool
	maxChunkSize int
}

func (outputer *baseJoinResultGenerator) initDefaultInner(innerTypes []*types.FieldType, defaultInner Row) {
	mutableRow := chunk.MutRowFromTypes(innerTypes)
	mutableRow.SetDatums(defaultInner[:len(innerTypes)]...)
	outputer.defaultInner = mutableRow.ToRow()
}

func (outputer *baseJoinResultGenerator) makeJoinRowToChunk(chk *chunk.Chunk, lhs, rhs chunk.Row) {
	// Call AppendRow() first to increment the virtual rows.
	// Fix: https://github.com/pingcap/tidb/issues/5771
	chk.AppendRow(lhs)
	chk.AppendPartialRow(lhs.Len(), rhs)
}

func (outputer *baseJoinResultGenerator) filter(input, output *chunk.Chunk) (matched bool, err error) {
	outputer.selected, err = expression.VectorizedFilter(outputer.ctx, outputer.conditions, chunk.NewIterator4Chunk(input), outputer.selected)
	if err != nil {
		return false, errors.Trace(err)
	}
	for i := 0; i < len(outputer.selected); i++ {
		if !outputer.selected[i] {
			continue
		}
		matched = true
		output.AppendRow(input.GetRow(i))
	}
	return matched, nil
}

type semiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *semiJoinResultGenerator) emit(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) error {
	if inners == nil || inners.Len() == 0 {
		return nil
	}
	defer inners.ReachEnd()
	if len(outputer.conditions) == 0 {
		chk.AppendPartialRow(0, outer)
		return nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputer.chk.Reset()
		if outputer.outerIsRight {
			outputer.makeJoinRowToChunk(outputer.chk, inner, outer)
		} else {
			outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		}
		selected, err := expression.EvalBool(outputer.ctx, outputer.conditions, outputer.chk.GetRow(0))
		if err != nil {
			return errors.Trace(err)
		}
		if selected {
			chk.AppendRow(outer)
			return nil
		}
	}
	return nil
}

type antiSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) emit(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) error {
	if inners == nil || inners.Len() == 0 {
		chk.AppendRow(outer)
		return nil
	}
	defer inners.ReachEnd()
	if len(outputer.conditions) == 0 {
		return nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputer.chk.Reset()
		if outputer.outerIsRight {
			outputer.makeJoinRowToChunk(outputer.chk, inner, outer)
		} else {
			outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		}

		matched, err := expression.EvalBool(outputer.ctx, outputer.conditions, outputer.chk.GetRow(0))
		if err != nil {
			return errors.Trace(err)
		}
		if matched {
			return nil
		}
	}
	chk.AppendRow(outer)
	return nil
}

type leftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) emit(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) error {
	if inners == nil || inners.Len() == 0 {
		chk.AppendPartialRow(0, outer)
		chk.AppendInt64(outer.Len(), 0)
		return nil
	}

	defer inners.ReachEnd()
	if len(outputer.conditions) == 0 {
		chk.AppendPartialRow(0, outer)
		chk.AppendInt64(outer.Len(), 1)
		return nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputer.chk.Reset()
		outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		matched, err := expression.EvalBool(outputer.ctx, outputer.conditions, outputer.chk.GetRow(0))
		if err != nil {
			return errors.Trace(err)
		}
		if matched {
			chk.AppendPartialRow(0, outer)
			chk.AppendInt64(outer.Len(), 1)
			return nil
		}
	}
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
	return nil
}

type antiLeftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) emit(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) error {
	// outer row can not be joined with any inner row.
	if inners == nil || inners.Len() == 0 {
		chk.AppendPartialRow(0, outer)
		chk.AppendInt64(outer.Len(), 1)
		return nil
	}

	defer inners.ReachEnd()
	// outer row can be joined with an inner row.
	if len(outputer.conditions) == 0 {
		chk.AppendPartialRow(0, outer)
		chk.AppendInt64(outer.Len(), 0)
		return nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputer.chk.Reset()
		outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		matched, err := expression.EvalBool(outputer.ctx, outputer.conditions, outputer.chk.GetRow(0))
		if err != nil {
			return errors.Trace(err)
		}
		// outer row can be joined with an inner row.
		if matched {
			chk.AppendPartialRow(0, outer)
			chk.AppendInt64(outer.Len(), 0)
			return nil
		}
	}

	// outer row can not be joined with any inner row.
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
	return nil
}

type leftOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) emit(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) error {
	// outer row can not be joined with any inner row.
	if inners == nil || inners.Len() == 0 {
		chk.AppendPartialRow(0, outer)
		chk.AppendPartialRow(outer.Len(), outputer.defaultInner)
		return nil
	}
	outputer.chk.Reset()
	chkForJoin := outputer.chk
	if len(outputer.conditions) == 0 {
		chkForJoin = chk
	}
	numToAppend := outputer.maxChunkSize - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		outputer.makeJoinRowToChunk(chkForJoin, outer, inners.Current())
		inners.Next()
	}
	if len(outputer.conditions) == 0 {
		return nil
	}
	// reach here, chkForJoin is outputer.chk
	matched, err := outputer.filter(chkForJoin, chk)
	if err != nil {
		return errors.Trace(err)
	}
	chkForJoin.Reset()
	if !matched {
		// outer row can not be joined with any inner row.
		chk.AppendPartialRow(0, outer)
		chk.AppendPartialRow(outer.Len(), outputer.defaultInner)
	}
	return nil
}

type rightOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) emit(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) error {
	// outer row can not be joined with any inner row.
	if inners == nil || inners.Len() == 0 {
		chk.AppendPartialRow(0, outputer.defaultInner)
		chk.AppendPartialRow(outputer.defaultInner.Len(), outer)
		return nil
	}
	outputer.chk.Reset()
	chkForJoin := outputer.chk
	if len(outputer.conditions) == 0 {
		chkForJoin = chk
	}
	numToAppend := outputer.maxChunkSize - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		outputer.makeJoinRowToChunk(chkForJoin, inners.Current(), outer)
		inners.Next()
	}
	if len(outputer.conditions) == 0 {
		return nil
	}
	// reach here, chkForJoin is outputer.chk
	matched, err := outputer.filter(chkForJoin, chk)
	if err != nil {
		return errors.Trace(err)
	}
	chkForJoin.Reset()
	// outer row can not be joined with any inner row.
	if !matched {
		chk.AppendPartialRow(0, outputer.defaultInner)
		chk.AppendPartialRow(outputer.defaultInner.Len(), outer)
	}
	return nil
}

type innerJoinResultGenerator struct {
	baseJoinResultGenerator
}

// emit implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) emit(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) error {
	if inners == nil || inners.Len() == 0 {
		return nil
	}
	outputer.chk.Reset()
	chkForJoin := outputer.chk
	if len(outputer.conditions) == 0 {
		chkForJoin = chk
	}
	inner, numToAppend := inners.Current(), outputer.maxChunkSize-chk.NumRows()
	for ; inner != inners.End() && numToAppend > 0; inner, numToAppend = inners.Next(), numToAppend-1 {
		if outputer.outerIsRight {
			outputer.makeJoinRowToChunk(chkForJoin, inner, outer)
		} else {
			outputer.makeJoinRowToChunk(chkForJoin, outer, inner)
		}
	}
	if len(outputer.conditions) == 0 {
		return nil
	}
	// reach here, chkForJoin is outputer.chk
	_, err := outputer.filter(chkForJoin, chk)
	if err != nil {
		return errors.Trace(err)
	}
	chkForJoin.Reset()

	return nil
}
