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

// joinResultGenerator is used to generate join results according to the join
// type. A typical instruction flow is:
//
//     hasMatch := false
//     for innerIter.Current() != innerIter.End() {
//         matched, err := g.tryToMatch(outer, innerIter, chk)
//         // handle err
//         hasMatch = hasMatch || matched
//     }
//     if !hasMatch {
//         g.onMissMatch(outer)
//     }
//
// NOTE: This interface is **not** thread-safe.
type joinResultGenerator interface {
	// tryToMatch tries to join an outer row with a batch of inner rows. When
	// 'inners.Len != 0' but all the joined rows are filtered, the outer row is
	// considered unmatched. Otherwise, the outer row is matched and some joined
	// rows are appended to `chk`. The size of `chk` is limited to MaxChunkSize.
	//
	// NOTE: Callers need to call this function multiple times to consume all
	// the inner rows for an outer row, and dicide whether the outer row can be
	// matched with at lease one inner row.
	tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error)

	// onMissMatch operates on the unmatched outer row according to the join
	// type. An outer row can be considered miss matched if:
	//   1. it can not pass the filter on the outer table side.
	//   2. there is no inner row with the same join key.
	//   3. all the joined rows can not pass the filter on the join result.
	//
	// On these conditions, the caller calls this function to handle the
	// unmatched outer rows according to the current join type:
	//   1. 'SemiJoin': ignores the unmatched outer row.
	//   2. 'AntiSemiJoin': appends the unmatched outer row to the result buffer.
	//   3. 'LeftOuterSemiJoin': concats the unmatched outer row with 0 and
	//      appends it to the result buffer.
	//   4. 'AntiLeftOuterSemiJoin': concats the unmatched outer row with 0 and
	//      appends it to the result buffer.
	//   5. 'LeftOuterJoin': concats the unmatched outer row with a row of NULLs
	//      and appends it to the result buffer.
	//   6. 'RightOuterJoin': concats the unmatched outer row with a row of NULLs
	//      and appends it to the result buffer.
	//   7. 'InnerJoin': ignores the unmatched outer row.
	onMissMatch(outer chunk.Row, chk *chunk.Chunk)
}

func newJoinResultGenerator(ctx sessionctx.Context, joinType plan.JoinType,
	outerIsRight bool, defaultInner []types.Datum, filter []expression.Expression,
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
	base.chk = chunk.NewChunkWithCapacity(colTypes, ctx.GetSessionVars().MaxChunkSize)
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

type baseJoinResultGenerator struct {
	ctx          sessionctx.Context
	conditions   []expression.Expression
	defaultInner chunk.Row
	outerIsRight bool
	chk          *chunk.Chunk
	selected     []bool
	maxChunkSize int
}

func (outputer *baseJoinResultGenerator) initDefaultInner(innerTypes []*types.FieldType, defaultInner []types.Datum) {
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

func (outputer *semiJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	if len(outputer.conditions) == 0 {
		chk.AppendPartialRow(0, outer)
		inners.ReachEnd()
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputer.chk.Reset()
		if outputer.outerIsRight {
			outputer.makeJoinRowToChunk(outputer.chk, inner, outer)
		} else {
			outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		}

		matched, err = expression.EvalBool(outputer.ctx, outputer.conditions, outputer.chk.GetRow(0))
		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			chk.AppendPartialRow(0, outer)
			inners.ReachEnd()
			return true, nil
		}
	}
	return false, nil
}

func (outputer *semiJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
}

type antiSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputer *antiSemiJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	if len(outputer.conditions) == 0 {
		inners.ReachEnd()
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputer.chk.Reset()
		if outputer.outerIsRight {
			outputer.makeJoinRowToChunk(outputer.chk, inner, outer)
		} else {
			outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		}

		matched, err = expression.EvalBool(outputer.ctx, outputer.conditions, outputer.chk.GetRow(0))
		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			inners.ReachEnd()
			return true, nil
		}
	}
	return false, nil
}

func (outputer *antiSemiJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendRow(outer)
}

type leftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputer *leftOuterSemiJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	if len(outputer.conditions) == 0 {
		outputer.onMatch(outer, chk)
		inners.ReachEnd()
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputer.chk.Reset()
		outputer.makeJoinRowToChunk(outputer.chk, outer, inner)

		matched, err = expression.EvalBool(outputer.ctx, outputer.conditions, outputer.chk.GetRow(0))
		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			outputer.onMatch(outer, chk)
			inners.ReachEnd()
			return true, nil
		}
	}
	return false, nil
}

func (outputer *leftOuterSemiJoinResultGenerator) onMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
}

func (outputer *leftOuterSemiJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
}

type antiLeftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputer *antiLeftOuterSemiJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	if len(outputer.conditions) == 0 {
		outputer.onMatch(outer, chk)
		inners.ReachEnd()
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputer.chk.Reset()
		outputer.makeJoinRowToChunk(outputer.chk, outer, inner)
		matched, err := expression.EvalBool(outputer.ctx, outputer.conditions, outputer.chk.GetRow(0))

		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			outputer.onMatch(outer, chk)
			inners.ReachEnd()
			return true, nil
		}
	}
	return false, nil
}

func (outputer *antiLeftOuterSemiJoinResultGenerator) onMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
}

func (outputer *antiLeftOuterSemiJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
}

type leftOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputer *leftOuterJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
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
		return true, nil
	}

	// reach here, chkForJoin is outputer.chk
	matched, err := outputer.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (outputer *leftOuterJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendPartialRow(outer.Len(), outputer.defaultInner)
}

type rightOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputer *rightOuterJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
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
		return true, nil
	}

	// reach here, chkForJoin is outputer.chk
	matched, err := outputer.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (outputer *rightOuterJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outputer.defaultInner)
	chk.AppendPartialRow(outputer.defaultInner.Len(), outer)
}

type innerJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputer *innerJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
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
		return true, nil
	}

	// reach here, chkForJoin is outputer.chk
	matched, err := outputer.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (outputer *innerJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
}
