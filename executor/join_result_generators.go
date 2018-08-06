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
	_ recordJoiner = &semiJoiner{}
	_ recordJoiner = &antiSemiJoiner{}
	_ recordJoiner = &leftOuterSemiJoiner{}
	_ recordJoiner = &antiLeftOuterSemiJoiner{}
	_ recordJoiner = &leftOuterJoiner{}
	_ recordJoiner = &rightOuterJoiner{}
	_ recordJoiner = &innerJoiner{}
)

// recordJoiner is used to generate join results according the join type.
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
type recordJoiner interface {
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

func newRecordJoiner(ctx sessionctx.Context, joinType plan.JoinType,
	outerIsRight bool, defaultInner types.DatumRow, filter []expression.Expression,
	lhsColTypes, rhsColTypes []*types.FieldType) recordJoiner {
	base := baseJoiner{
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
	panic("unsupported join type in func newRecordJoiner()")
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

func (joiner *baseJoiner) initDefaultInner(innerTypes []*types.FieldType, defaultInner types.DatumRow) {
	mutableRow := chunk.MutRowFromTypes(innerTypes)
	mutableRow.SetDatums(defaultInner[:len(innerTypes)]...)
	joiner.defaultInner = mutableRow.ToRow()
}

func (joiner *baseJoiner) makeJoinRowToChunk(chk *chunk.Chunk, lhs, rhs chunk.Row) {
	// Call AppendRow() first to increment the virtual rows.
	// Fix: https://github.com/pingcap/tidb/issues/5771
	chk.AppendRow(lhs)
	chk.AppendPartialRow(lhs.Len(), rhs)
}

func (joiner *baseJoiner) filter(input, output *chunk.Chunk) (matched bool, err error) {
	joiner.selected, err = expression.VectorizedFilter(joiner.ctx, joiner.conditions, chunk.NewIterator4Chunk(input), joiner.selected)
	if err != nil {
		return false, errors.Trace(err)
	}
	for i := 0; i < len(joiner.selected); i++ {
		if !joiner.selected[i] {
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

func (joiner *semiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
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

	if len(joiner.conditions) == 0 {
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		joiner.chk.Reset()
		if joiner.outerIsRight {
			joiner.makeJoinRowToChunk(joiner.chk, inner, outer)
		} else {
			joiner.makeJoinRowToChunk(joiner.chk, outer, inner)
		}
		matched, err = expression.EvalBool(joiner.ctx, joiner.conditions, joiner.chk.GetRow(0))
		if err != nil || matched {
			return true, errors.Trace(err)
		}
	}
	return false, nil
}

func (joiner *semiJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	return
}

type antiSemiJoiner struct {
	baseJoiner
}

// tryToMatch implements recordJoiner interface.
func (joiner *antiSemiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
	if inners.Len() == 0 {
		return false, nil
	}

	defer inners.ReachEnd()

	if len(joiner.conditions) == 0 {
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		joiner.chk.Reset()
		if joiner.outerIsRight {
			joiner.makeJoinRowToChunk(joiner.chk, inner, outer)
		} else {
			joiner.makeJoinRowToChunk(joiner.chk, outer, inner)
		}

		matched, err = expression.EvalBool(joiner.ctx, joiner.conditions, joiner.chk.GetRow(0))
		if err != nil || matched {
			return true, errors.Trace(err)
		}
	}
	return false, nil
}

func (joiner *antiSemiJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendRow(outer)
	return
}

type leftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatch implements recordJoiner interface.
func (joiner *leftOuterSemiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
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

	if len(joiner.conditions) == 0 {
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		joiner.chk.Reset()
		joiner.makeJoinRowToChunk(joiner.chk, outer, inner)
		matched, err = expression.EvalBool(joiner.ctx, joiner.conditions, joiner.chk.GetRow(0))
		if err != nil || matched {
			return true, errors.Trace(err)
		}
	}
	return false, nil
}

func (joiner *leftOuterSemiJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
	return
}

type antiLeftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatch implements recordJoiner interface.
func (joiner *antiLeftOuterSemiJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, err error) {
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

	if len(joiner.conditions) == 0 {
		return true, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		joiner.chk.Reset()
		joiner.makeJoinRowToChunk(joiner.chk, outer, inner)
		matched, err := expression.EvalBool(joiner.ctx, joiner.conditions, joiner.chk.GetRow(0))
		if err != nil || matched {
			return true, errors.Trace(err)
		}
	}
	return false, nil
}

func (joiner *antiLeftOuterSemiJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
	return
}

type leftOuterJoiner struct {
	baseJoiner
}

// tryToMatch implements recordJoiner interface.
func (joiner *leftOuterJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
	}

	joiner.chk.Reset()
	chkForJoin := joiner.chk
	if len(joiner.conditions) == 0 {
		chkForJoin = chk
	}

	numToAppend := joiner.maxChunkSize - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		joiner.makeJoinRowToChunk(chkForJoin, outer, inners.Current())
		inners.Next()
	}
	if len(joiner.conditions) == 0 {
		return true, nil
	}

	// reach here, chkForJoin is joiner.chk
	matched, err := joiner.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (joiner *leftOuterJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendPartialRow(outer.Len(), joiner.defaultInner)
	return
}

type rightOuterJoiner struct {
	baseJoiner
}

// tryToMatch implements recordJoiner interface.
func (joiner *rightOuterJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
	}

	joiner.chk.Reset()
	chkForJoin := joiner.chk
	if len(joiner.conditions) == 0 {
		chkForJoin = chk
	}

	numToAppend := joiner.maxChunkSize - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		joiner.makeJoinRowToChunk(chkForJoin, inners.Current(), outer)
		inners.Next()
	}
	if len(joiner.conditions) == 0 {
		return true, nil
	}

	// reach here, chkForJoin is joiner.chk
	matched, err := joiner.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (joiner *rightOuterJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, joiner.defaultInner)
	chk.AppendPartialRow(joiner.defaultInner.Len(), outer)
	return
}

type innerJoiner struct {
	baseJoiner
}

// tryToMatch implements recordJoiner interface.
func (joiner *innerJoiner) tryToMatch(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (bool, error) {
	if inners.Len() == 0 {
		return false, nil
	}
	joiner.chk.Reset()
	chkForJoin := joiner.chk
	if len(joiner.conditions) == 0 {
		chkForJoin = chk
	}
	inner, numToAppend := inners.Current(), joiner.maxChunkSize-chk.NumRows()
	for ; inner != inners.End() && numToAppend > 0; inner, numToAppend = inners.Next(), numToAppend-1 {
		if joiner.outerIsRight {
			joiner.makeJoinRowToChunk(chkForJoin, inner, outer)
		} else {
			joiner.makeJoinRowToChunk(chkForJoin, outer, inner)
		}
	}
	if len(joiner.conditions) == 0 {
		return true, nil
	}

	// reach here, chkForJoin is joiner.chk
	matched, err := joiner.filter(chkForJoin, chk)
	return matched, errors.Trace(err)
}

func (joiner *innerJoiner) onMissMatch(outer chunk.Row, chk *chunk.Chunk) {
	return
}
