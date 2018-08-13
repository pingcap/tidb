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
	_ joiner = &semiJoiner{}
	_ joiner = &antiSemiJoiner{}
	_ joiner = &leftOuterSemiJoiner{}
	_ joiner = &antiLeftOuterSemiJoiner{}
	_ joiner = &leftOuterJoiner{}
	_ joiner = &rightOuterJoiner{}
	_ joiner = &innerJoiner{}
)

// joiner is used to generate join results according to the join type.
// A typical instruction flow is:
//
//     hasMatch := false
//     for innerIter.Current() != innerIter.End() {
//         matched, err := j.tryToMatch(outer, innerIter, chk)
//         // handle err
//         hasMatch = hasMatch || matched
//     }
//     if !hasMatch {
//         j.onMissMatch(outer)
//     }
//
// NOTE: This interface is **not** thread-safe.
type joiner interface {
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

func newJoiner(ctx sessionctx.Context, joinType plan.JoinType,
	outerIsRight bool, defaultInner []types.Datum, filter []expression.Expression,
	lhsColTypes, rhsColTypes []*types.FieldType) joiner {
	base := baseJoiner{
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
		return &semiJoiner{base}
	case plan.AntiSemiJoin:
		return &antiSemiJoiner{base}
	case plan.LeftOuterSemiJoin:
		return &leftOuterSemiJoiner{base}
	case plan.AntiLeftOuterSemiJoin:
		return &antiLeftOuterSemiJoiner{base}
	case plan.LeftOuterJoin:
		return &leftOuterJoiner{base}
	case plan.RightOuterJoin:
		return &rightOuterJoiner{base}
	case plan.InnerJoin:
		return &innerJoiner{base}
	}
	panic("unsupported join type in func newJoiner()")
}

type baseJoiner struct {
	ctx          sessionctx.Context
	conditions   []expression.Expression
	defaultInner chunk.Row
	outerIsRight bool
	chk          *chunk.Chunk
	selected     []bool
	maxChunkSize int
}

func (j *baseJoiner) initDefaultInner(innerTypes []*types.FieldType, defaultInner []types.Datum) {
	mutableRow := chunk.MutRowFromTypes(innerTypes)
	mutableRow.SetDatums(defaultInner[:len(innerTypes)]...)
	j.defaultInner = mutableRow.ToRow()
}

func (j *baseJoiner) makeJoinRowToChunk(chk *chunk.Chunk, lhs, rhs chunk.Row) {
	// Call AppendRow() first to increment the virtual rows.
	// Fix: https://github.com/pingcap/tidb/issues/5771
	chk.AppendRow(lhs)
	chk.AppendPartialRow(lhs.Len(), rhs)
}

func (j *baseJoiner) filter(input, output *chunk.Chunk) (matched bool, err error) {
	j.selected, err = expression.VectorizedFilter(j.ctx, j.conditions, chunk.NewIterator4Chunk(input), j.selected)
	if err != nil {
		return false, errors.Trace(err)
	}
	for i := 0; i < len(j.selected); i++ {
		if !j.selected[i] {
			continue
		}
		matched = true
		output.AppendRow(input.GetRow(i))
	}
	return matched, nil
}

type semiJoiner struct {
	baseJoiner
}

func (j *semiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	if len(j.conditions) == 0 {
		chk.AppendPartialRow(0, outer)
		inners.ReachEnd()
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.chk.Reset()
		if j.outerIsRight {
			j.makeJoinRowToChunk(j.chk, inner, outer)
		} else {
			j.makeJoinRowToChunk(j.chk, outer, inner)
		}

		matched, err = expression.EvalBool(j.ctx, j.conditions, j.chk.GetRow(0))
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

func (j *semiJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
}

type antiSemiJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *antiSemiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	if len(j.conditions) == 0 {
		inners.ReachEnd()
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.chk.Reset()
		if j.outerIsRight {
			j.makeJoinRowToChunk(j.chk, inner, outer)
		} else {
			j.makeJoinRowToChunk(j.chk, outer, inner)
		}

		matched, err = expression.EvalBool(j.ctx, j.conditions, j.chk.GetRow(0))
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

func (j *antiSemiJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendRow(outer)
}

type leftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *leftOuterSemiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	if len(j.conditions) == 0 {
		j.onMatch(outer, chk)
		inners.ReachEnd()
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.chk.Reset()
		j.makeJoinRowToChunk(j.chk, outer, inner)

		matched, err = expression.EvalBool(j.ctx, j.conditions, j.chk.GetRow(0))
		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			j.onMatch(outer, chk)
			inners.ReachEnd()
			return true, nil
		}
	}
	return false, nil
}

func (j *leftOuterSemiJoiner) onMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
}

func (j *leftOuterSemiJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
}

type antiLeftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *antiLeftOuterSemiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	if len(j.conditions) == 0 {
		j.onMatch(outer, chk)
		inners.ReachEnd()
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.chk.Reset()
		j.makeJoinRowToChunk(j.chk, outer, inner)
		matched, err := expression.EvalBool(j.ctx, j.conditions, j.chk.GetRow(0))

		if err != nil {
			return false, errors.Trace(err)
		}
		if matched {
			j.onMatch(outer, chk)
			inners.ReachEnd()
			return true, nil
		}
	}
	return false, nil
}

func (j *antiLeftOuterSemiJoiner) onMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
}

func (j *antiLeftOuterSemiJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
}

type leftOuterJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *leftOuterJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
	}

	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}

	numToAppend := j.maxChunkSize - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		j.makeJoinRowToChunk(chkForJoin, outer, inners.Current())
		inners.Next()
	}
	if len(j.conditions) == 0 {
		return true, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err := j.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (j *leftOuterJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendPartialRow(outer.Len(), j.defaultInner)
}

type rightOuterJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *rightOuterJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
	}

	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}

	numToAppend := j.maxChunkSize - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		j.makeJoinRowToChunk(chkForJoin, inners.Current(), outer)
		inners.Next()
	}
	if len(j.conditions) == 0 {
		return true, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err := j.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (j *rightOuterJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, j.defaultInner)
	chk.AppendPartialRow(j.defaultInner.Len(), outer)
}

type innerJoiner struct {
	baseJoiner
}

// tryToMatch implements joiner interface.
func (j *innerJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
	}
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}
	inner, numToAppend := inners.Current(), j.maxChunkSize-chk.NumRows()
	for ; inner != inners.End() && numToAppend > 0; inner, numToAppend = inners.Next(), numToAppend-1 {
		if j.outerIsRight {
			j.makeJoinRowToChunk(chkForJoin, inner, outer)
		} else {
			j.makeJoinRowToChunk(chkForJoin, outer, inner)
		}
	}
	if len(j.conditions) == 0 {
		return true, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err := j.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (j *innerJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
}
