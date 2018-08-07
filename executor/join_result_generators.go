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

// joinResultGenerator is used to generate join results according the join type.
// A typical instruction flow is:
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
	// rows is appended to `chk`. The size of `chk` is limited to MaxChunkSize.
	//
	// NOTE: Callers need to call this function multiple times to consume all
	// the inner rows for an outer row, and dicide whether the outer row can be
	// matched with at lease one inner row.
	tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error)

	// onMissMatch operates on the unmatched outer row according to the join
	// type. An outer row can be considered miss matched if:
	//   1. it can not pass the filter on the outer table side.
	//   2. there does not exist an inner row with the same join key.
	//   3. all the joined rows can not pass the filter on the join result.
	//
	// On these conditions, the caller calls this function to handle the
	// unmatched outer rows according to the current join type:
	//   1. 'SemiJoin': ignores the unmatched outer row.
	//   2. 'AntiSemiJoin': appends the unmatched outer row to the result buffer.
	//   3. 'LeftOuterSemiJoin': concates the unmatched outer row with 0 and
	//      appended it to the result buffer.
	//   4. 'AntiLeftOuterSemiJoin': concates the unmatched outer row with 0 and
	//      appended to the result buffer.
	//   5. 'LeftOuterJoin': concates the unmatched outer row with a row of NULLs
	//      and appended to the result buffer.
	//   6. 'RightOuterJoin': concates the unmatched outer row with a row of NULLs
	//      and appended to the result buffer.
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

func (outputor *baseJoinResultGenerator) initDefaultInner(innerTypes []*types.FieldType, defaultInner []types.Datum) {
	mutableRow := chunk.MutRowFromTypes(innerTypes)
	mutableRow.SetDatums(defaultInner[:len(innerTypes)]...)
	outputor.defaultInner = mutableRow.ToRow()
}

func (outputor *baseJoinResultGenerator) makeJoinRowToChunk(chk *chunk.Chunk, lhs, rhs chunk.Row) {
	// Call AppendRow() first to increment the virtual rows.
	// Fix: https://github.com/pingcap/tidb/issues/5771
	chk.AppendRow(lhs)
	chk.AppendPartialRow(lhs.Len(), rhs)
}

func (outputor *baseJoinResultGenerator) filter(input, output *chunk.Chunk) (matched bool, err error) {
	outputor.selected, err = expression.VectorizedFilter(outputor.ctx, outputor.conditions, chunk.NewIterator4Chunk(input), outputor.selected)
	if err != nil {
		return false, errors.Trace(err)
	}
	for i := 0; i < len(outputor.selected); i++ {
		if !outputor.selected[i] {
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

func (outputor *semiJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	defer func() {
		if !(err == nil && matched) {
			return
		}
		// here we handle the matched outer.
		chk.AppendPartialRow(0, outer)
		inners.ReachEnd()
	}()

	if len(outputor.conditions) == 0 {
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputor.chk.Reset()
		if outputor.outerIsRight {
			outputor.makeJoinRowToChunk(outputor.chk, inner, outer)
		} else {
			outputor.makeJoinRowToChunk(outputor.chk, outer, inner)
		}
		matched, err = expression.EvalBool(outputor.ctx, outputor.conditions, outputor.chk.GetRow(0))
		if err != nil || matched {
			return true, errors.Trace(err)
		}
	}
	return false, nil
}

func (outputor *semiJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	return
}

type antiSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputor *antiSemiJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	defer inners.ReachEnd()

	if len(outputor.conditions) == 0 {
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputor.chk.Reset()
		if outputor.outerIsRight {
			outputor.makeJoinRowToChunk(outputor.chk, inner, outer)
		} else {
			outputor.makeJoinRowToChunk(outputor.chk, outer, inner)
		}

		matched, err = expression.EvalBool(outputor.ctx, outputor.conditions, outputor.chk.GetRow(0))
		if err != nil || matched {
			return true, errors.Trace(err)
		}
	}
	return false, nil
}

func (outputor *antiSemiJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendRow(outer)
	return
}

type leftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputor *leftOuterSemiJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	defer func() {
		if !(err == nil && matched) {
			return
		}
		// here we handle the matched outer.
		chk.AppendPartialRow(0, outer)
		chk.AppendInt64(outer.Len(), 1)
		inners.ReachEnd()
	}()

	if len(outputor.conditions) == 0 {
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputor.chk.Reset()
		outputor.makeJoinRowToChunk(outputor.chk, outer, inner)
		matched, err = expression.EvalBool(outputor.ctx, outputor.conditions, outputor.chk.GetRow(0))
		if err != nil || matched {
			return true, errors.Trace(err)
		}
	}
	return false, nil
}

func (outputor *leftOuterSemiJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
	return
}

type antiLeftOuterSemiJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputor *antiLeftOuterSemiJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	defer func() {
		if !(err == nil && matched) {
			return
		}
		// here we handle the matched outer.
		chk.AppendPartialRow(0, outer)
		chk.AppendInt64(outer.Len(), 0)
		inners.ReachEnd()
	}()

	if len(outputor.conditions) == 0 {
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		outputor.chk.Reset()
		outputor.makeJoinRowToChunk(outputor.chk, outer, inner)
		matched, err := expression.EvalBool(outputor.ctx, outputor.conditions, outputor.chk.GetRow(0))
		if err != nil || matched {
			return true, errors.Trace(err)
		}
	}
	return false, nil
}

func (outputor *antiLeftOuterSemiJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
	return
}

type leftOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputor *leftOuterJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
	}

	outputor.chk.Reset()
	chkForJoin := outputor.chk
	if len(outputor.conditions) == 0 {
		chkForJoin = chk
	}

	numToAppend := outputor.maxChunkSize - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		outputor.makeJoinRowToChunk(chkForJoin, outer, inners.Current())
		inners.Next()
	}
	if len(outputor.conditions) == 0 {
		return true, nil
	}

	// reach here, chkForJoin is outputor.chk
	matched, err := outputor.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (outputor *leftOuterJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendPartialRow(outer.Len(), outputor.defaultInner)
	return
}

type rightOuterJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputor *rightOuterJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
	}

	outputor.chk.Reset()
	chkForJoin := outputor.chk
	if len(outputor.conditions) == 0 {
		chkForJoin = chk
	}

	numToAppend := outputor.maxChunkSize - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		outputor.makeJoinRowToChunk(chkForJoin, inners.Current(), outer)
		inners.Next()
	}
	if len(outputor.conditions) == 0 {
		return true, nil
	}

	// reach here, chkForJoin is outputor.chk
	matched, err := outputor.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (outputor *rightOuterJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outputor.defaultInner)
	chk.AppendPartialRow(outputor.defaultInner.Len(), outer)
	return
}

type innerJoinResultGenerator struct {
	baseJoinResultGenerator
}

// tryToMatch implements joinResultGenerator interface.
func (outputor *innerJoinResultGenerator) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
	}
	outputor.chk.Reset()
	chkForJoin := outputor.chk
	if len(outputor.conditions) == 0 {
		chkForJoin = chk
	}
	inner, numToAppend := inners.Current(), outputor.maxChunkSize-chk.NumRows()
	for ; inner != inners.End() && numToAppend > 0; inner, numToAppend = inners.Next(), numToAppend-1 {
		if outputor.outerIsRight {
			outputor.makeJoinRowToChunk(chkForJoin, inner, outer)
		} else {
			outputor.makeJoinRowToChunk(chkForJoin, outer, inner)
		}
	}
	if len(outputor.conditions) == 0 {
		return true, nil
	}

	// reach here, chkForJoin is outputor.chk
	matched, err := outputor.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (outputor *innerJoinResultGenerator) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	return
}
