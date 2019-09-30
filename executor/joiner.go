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
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
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
//     hasMatch, hasNull := false, false
//     for innerIter.Current() != innerIter.End() {
//         matched, isNull, err := j.tryToMatchInners(outer, innerIter, chk)
//         // handle err
//         hasMatch = hasMatch || matched
//         hasNull = hasNull || isNull
//     }
//     if !hasMatch {
//         j.onMissMatch(hasNull, outer, chk)
//     }
//
// NOTE: This interface is **not** thread-safe.
type joiner interface {
	// tryToMatchInners tries to join an outer row with a batch of inner rows. When
	// 'inners.Len != 0' but all the joined rows are filtered, the outer row is
	// considered unmatched. Otherwise, the outer row is matched and some joined
	// rows are appended to `chk`. The size of `chk` is limited to MaxChunkSize.
	// Note that when the outer row is considered unmatched, we need to differentiate
	// whether the join conditions return null or false, because that matters for
	// AntiSemiJoin/LeftOuterSemiJoin/AntiLeftOuterSemiJoin, by setting the return
	// value isNull; for other join types, isNull is always false.
	//
	// NOTE: Callers need to call this function multiple times to consume all
	// the inner rows for an outer row, and decide whether the outer row can be
	// matched with at lease one inner row.
	tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, isNull bool, err error)

	// tryToMatchOuters tries to join a batch of outer rows with one inner row.
	// It's used when the join is an outer join and the hash table is built
	// using the outer side.
	tryToMatchOuters(outer chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error)

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
	//   4. 'AntiLeftOuterSemiJoin': concats the unmatched outer row with 1 and
	//      appends it to the result buffer.
	//   5. 'LeftOuterJoin': concats the unmatched outer row with a row of NULLs
	//      and appends it to the result buffer.
	//   6. 'RightOuterJoin': concats the unmatched outer row with a row of NULLs
	//      and appends it to the result buffer.
	//   7. 'InnerJoin': ignores the unmatched outer row.
	//
	// Note that, for LeftOuterSemiJoin, AntiSemiJoin and AntiLeftOuterSemiJoin,
	// we need to know the reason of outer row being treated as unmatched:
	// whether the join condition returns false, or returns null, because
	// it decides if this outer row should be outputted, hence we have a `hasNull`
	// parameter passed to `onMissMatch`.
	onMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk)

	// Clone deep copies a joiner.
	Clone() joiner
}

func newJoiner(ctx sessionctx.Context, joinType plannercore.JoinType,
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
	base.selected = make([]bool, 0, chunk.InitialCapacity)
	base.isNull = make([]bool, 0, chunk.InitialCapacity)
	if joinType == plannercore.LeftOuterJoin || joinType == plannercore.RightOuterJoin {
		innerColTypes := lhsColTypes
		if !outerIsRight {
			innerColTypes = rhsColTypes
		}
		base.initDefaultInner(innerColTypes, defaultInner)
	}
	switch joinType {
	case plannercore.SemiJoin:
		base.shallowRow = chunk.MutRowFromTypes(colTypes)
		return &semiJoiner{base}
	case plannercore.AntiSemiJoin:
		base.shallowRow = chunk.MutRowFromTypes(colTypes)
		return &antiSemiJoiner{base}
	case plannercore.LeftOuterSemiJoin:
		base.shallowRow = chunk.MutRowFromTypes(colTypes)
		return &leftOuterSemiJoiner{base}
	case plannercore.AntiLeftOuterSemiJoin:
		base.shallowRow = chunk.MutRowFromTypes(colTypes)
		return &antiLeftOuterSemiJoiner{base}
	case plannercore.LeftOuterJoin:
		base.chk = chunk.NewChunkWithCapacity(colTypes, ctx.GetSessionVars().MaxChunkSize)
		return &leftOuterJoiner{base}
	case plannercore.RightOuterJoin:
		base.chk = chunk.NewChunkWithCapacity(colTypes, ctx.GetSessionVars().MaxChunkSize)
		return &rightOuterJoiner{base}
	case plannercore.InnerJoin:
		base.chk = chunk.NewChunkWithCapacity(colTypes, ctx.GetSessionVars().MaxChunkSize)
		return &innerJoiner{base}
	}
	panic("unsupported join type in func newJoiner()")
}

type outerRowStatusFlag byte

const (
	outerRowUnmatched outerRowStatusFlag = iota
	outerRowMatched
	outerRowHasNull
)

type baseJoiner struct {
	ctx          sessionctx.Context
	conditions   []expression.Expression
	defaultInner chunk.Row
	outerIsRight bool
	chk          *chunk.Chunk
	shallowRow   chunk.MutRow
	selected     []bool
	isNull       []bool
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

// makeShallowJoinRow shallow copies `inner` and `outer` into `shallowRow`.
func (j *baseJoiner) makeShallowJoinRow(isRightJoin bool, inner, outer chunk.Row) {
	if !isRightJoin {
		inner, outer = outer, inner
	}
	j.shallowRow.ShallowCopyPartialRow(0, inner)
	j.shallowRow.ShallowCopyPartialRow(inner.Len(), outer)
}

// filter is used to filter the result constructed by tryToMatchInners, the result is
// built by one outer row and multiple inner rows. The returned bool value
// indicates whether the outer row matches any inner rows.
func (j *baseJoiner) filter(input, output *chunk.Chunk, outerColsLen int) (bool, error) {
	var err error
	j.selected, err = expression.VectorizedFilter(j.ctx, j.conditions, chunk.NewIterator4Chunk(input), j.selected)
	if err != nil {
		return false, err
	}
	// Batch copies selected rows to output chunk.
	innerColOffset, outerColOffset := 0, input.NumCols()-outerColsLen
	if !j.outerIsRight {
		innerColOffset, outerColOffset = outerColsLen, 0
	}
	return chunk.CopySelectedJoinRows(input, innerColOffset, outerColOffset, j.selected, output)
}

// filterAndCheckOuterRowStatus is used to filter the result constructed by
// tryToMatchOuters, the result is built by multiple outer rows and one inner
// row. The returned outerRowStatusFlag slice value indicates the status of
// each outer row (matched/unmatched/hasNull).
func (j *baseJoiner) filterAndCheckOuterRowStatus(input, output *chunk.Chunk, innerColsLen int, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, _ error) {
	var err error
	j.selected, j.isNull, err = expression.VectorizedFilterConsiderNull(j.ctx, j.conditions, chunk.NewIterator4Chunk(input), j.selected, j.isNull)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(j.selected); i++ {
		if j.isNull[i] {
			outerRowStatus[i] = outerRowHasNull
		} else if !j.selected[i] {
			outerRowStatus[i] = outerRowUnmatched
		}
	}

	// Batch copies selected rows to output chunk.
	innerColOffset, outerColOffset := 0, innerColsLen
	if !j.outerIsRight {
		innerColOffset, outerColOffset = input.NumCols()-innerColsLen, 0
	}

	_, err = chunk.CopySelectedJoinRows(input, innerColOffset, outerColOffset, j.selected, output)
	return outerRowStatus, err
}

func (j *baseJoiner) Clone() baseJoiner {
	base := baseJoiner{
		ctx:          j.ctx,
		conditions:   make([]expression.Expression, 0, len(j.conditions)),
		outerIsRight: j.outerIsRight,
		maxChunkSize: j.maxChunkSize,
		selected:     make([]bool, 0, len(j.selected)),
		isNull:       make([]bool, 0, len(j.isNull)),
	}
	for _, con := range j.conditions {
		base.conditions = append(base.conditions, con.Clone())
	}
	if j.chk != nil {
		base.chk = j.chk.CopyConstruct()
	} else {
		base.shallowRow = chunk.MutRow(j.shallowRow.ToRow())
	}
	if !j.defaultInner.IsEmpty() {
		base.defaultInner = j.defaultInner.CopyConstruct()
	}
	return base
}

type semiJoiner struct {
	baseJoiner
}

func (j *semiJoiner) tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		chk.AppendPartialRow(0, outer)
		inners.ReachEnd()
		return true, false, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinRow(j.outerIsRight, inner, outer)

		// For SemiJoin, we can safely treat null result of join conditions as false,
		// so we ignore the nullness returned by EvalBool here.
		matched, _, err = expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return false, false, err
		}
		if matched {
			chk.AppendPartialRow(0, outer)
			inners.ReachEnd()
			return true, false, nil
		}
	}
	return false, false, nil
}

func (j *semiJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	outerRowStatus = outerRowStatus[:0]
	outer, numToAppend := outers.Current(), chk.RequiredRows()-chk.NumRows()
	if len(j.conditions) == 0 {
		for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
			chk.AppendPartialRow(0, outer)
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		}
		return outerRowStatus, nil
	}
	for outer := outers.Current(); outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
		j.makeShallowJoinRow(j.outerIsRight, inner, outer)
		// For SemiJoin, we can safely treat null result of join conditions as false,
		// so we ignore the nullness returned by EvalBool here.
		matched, _, err := expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return outerRowStatus, err
		}
		outerRowStatus = append(outerRowStatus, outerRowMatched)
		if matched {
			chk.AppendPartialRow(0, outer)
		}
	}
	return outerRowStatus, nil
}

func (j *semiJoiner) onMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
}

// Clone implements joiner interface.
func (j *semiJoiner) Clone() joiner {
	return &semiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type antiSemiJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *antiSemiJoiner) tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		inners.ReachEnd()
		return true, false, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinRow(j.outerIsRight, inner, outer)

		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return false, false, err
		}
		if matched {
			inners.ReachEnd()
			return true, false, nil
		}
		hasNull = hasNull || isNull
	}
	return false, hasNull, nil
}

func (j *antiSemiJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	outerRowStatus = outerRowStatus[:0]
	numToAppend := chk.RequiredRows() - chk.NumRows()
	if len(j.conditions) == 0 {
		for ; outers.Current() != outers.End(); outers.Next() {
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		}
		return outerRowStatus, nil
	}
	for outer := outers.Current(); outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
		j.makeShallowJoinRow(j.outerIsRight, inner, outer)
		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return outerRowStatus, err
		}
		if matched {
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		} else if isNull {
			outerRowStatus = append(outerRowStatus, outerRowHasNull)
		} else {
			outerRowStatus = append(outerRowStatus, outerRowUnmatched)
		}
	}
	return outerRowStatus, nil
}

func (j *antiSemiJoiner) onMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk) {
	if !hasNull {
		chk.AppendRow(outer)
	}
}

func (j *antiSemiJoiner) Clone() joiner {
	return &antiSemiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type leftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *leftOuterSemiJoiner) tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		j.onMatch(outer, chk)
		inners.ReachEnd()
		return true, false, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinRow(false, inner, outer)

		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return false, false, err
		}
		if matched {
			j.onMatch(outer, chk)
			inners.ReachEnd()
			return true, false, nil
		}
		hasNull = hasNull || isNull
	}
	return false, hasNull, nil
}

func (j *leftOuterSemiJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	outerRowStatus = outerRowStatus[:0]
	outer, numToAppend := outers.Current(), chk.RequiredRows()-chk.NumRows()
	if len(j.conditions) == 0 {
		for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
			j.onMatch(outer, chk)
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		}
		return outerRowStatus, nil
	}

	for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
		j.makeShallowJoinRow(false, inner, outer)
		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return nil, err
		}
		if matched {
			j.onMatch(outer, chk)
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		} else if isNull {
			outerRowStatus = append(outerRowStatus, outerRowHasNull)
		} else {
			outerRowStatus = append(outerRowStatus, outerRowUnmatched)
		}
	}
	return outerRowStatus, nil
}

func (j *leftOuterSemiJoiner) onMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 1)
}

func (j *leftOuterSemiJoiner) onMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	if hasNull {
		chk.AppendNull(outer.Len())
	} else {
		chk.AppendInt64(outer.Len(), 0)
	}
}

func (j *leftOuterSemiJoiner) Clone() joiner {
	return &leftOuterSemiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type antiLeftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *antiLeftOuterSemiJoiner) tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		j.onMatch(outer, chk)
		inners.ReachEnd()
		return true, false, nil
	}

	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinRow(false, inner, outer)

		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return false, false, err
		}
		if matched {
			j.onMatch(outer, chk)
			inners.ReachEnd()
			return true, false, nil
		}
		hasNull = hasNull || isNull
	}
	return false, hasNull, nil
}

func (j *antiLeftOuterSemiJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	outerRowStatus = outerRowStatus[:0]
	outer, numToAppend := outers.Current(), chk.RequiredRows()-chk.NumRows()
	if len(j.conditions) == 0 {
		for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
			j.onMatch(outer, chk)
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		}
		return outerRowStatus, nil
	}

	for i := 0; outer != outers.End() && numToAppend > 0; outer, numToAppend, i = outers.Next(), numToAppend-1, i+1 {
		j.makeShallowJoinRow(false, inner, outer)
		matched, isNull, err := expression.EvalBool(j.ctx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return nil, err
		}
		if matched {
			j.onMatch(outer, chk)
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		} else if isNull {
			outerRowStatus = append(outerRowStatus, outerRowHasNull)
		} else {
			outerRowStatus = append(outerRowStatus, outerRowUnmatched)
		}
	}
	return outerRowStatus, nil
}

func (j *antiLeftOuterSemiJoiner) onMatch(outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendInt64(outer.Len(), 0)
}

func (j *antiLeftOuterSemiJoiner) onMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	if hasNull {
		chk.AppendNull(outer.Len())
	} else {
		chk.AppendInt64(outer.Len(), 1)
	}
}

func (j *antiLeftOuterSemiJoiner) Clone() joiner {
	return &antiLeftOuterSemiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type leftOuterJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *leftOuterJoiner) tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}

	numToAppend := chk.RequiredRows() - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		j.makeJoinRowToChunk(chkForJoin, outer, inners.Current())
		inners.Next()
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err = j.filter(chkForJoin, chk, outer.Len())
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *leftOuterJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}

	outer, numToAppend, cursor := outers.Current(), chk.RequiredRows()-chk.NumRows(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		j.makeJoinRowToChunk(chkForJoin, outer, inner)
	}
	outerRowStatus = outerRowStatus[:0]
	for i := 0; i < cursor; i++ {
		outerRowStatus = append(outerRowStatus, outerRowMatched)
	}
	if len(j.conditions) == 0 {
		return outerRowStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterRowStatus(chkForJoin, chk, inner.Len(), outerRowStatus)
}

func (j *leftOuterJoiner) onMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, outer)
	chk.AppendPartialRow(outer.Len(), j.defaultInner)
}

func (j *leftOuterJoiner) Clone() joiner {
	return &leftOuterJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type rightOuterJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *rightOuterJoiner) tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}

	numToAppend := chk.RequiredRows() - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		j.makeJoinRowToChunk(chkForJoin, inners.Current(), outer)
		inners.Next()
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	matched, err = j.filter(chkForJoin, chk, outer.Len())
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *rightOuterJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}

	outer, numToAppend, cursor := outers.Current(), chk.RequiredRows()-chk.NumRows(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		j.makeJoinRowToChunk(chkForJoin, inner, outer)
	}
	outerRowStatus = outerRowStatus[:0]
	for i := 0; i < cursor; i++ {
		outerRowStatus = append(outerRowStatus, outerRowMatched)
	}
	if len(j.conditions) == 0 {
		return outerRowStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterRowStatus(chkForJoin, chk, inner.Len(), outerRowStatus)
}

func (j *rightOuterJoiner) onMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendPartialRow(0, j.defaultInner)
	chk.AppendPartialRow(j.defaultInner.Len(), outer)
}

func (j *rightOuterJoiner) Clone() joiner {
	return &rightOuterJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type innerJoiner struct {
	baseJoiner
}

// tryToMatchInners implements joiner interface.
func (j *innerJoiner) tryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}
	inner, numToAppend := inners.Current(), chk.RequiredRows()-chk.NumRows()
	for ; inner != inners.End() && numToAppend > 0; inner, numToAppend = inners.Next(), numToAppend-1 {
		if j.outerIsRight {
			j.makeJoinRowToChunk(chkForJoin, inner, outer)
		} else {
			j.makeJoinRowToChunk(chkForJoin, outer, inner)
		}
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err = j.filter(chkForJoin, chk, outer.Len())
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *innerJoiner) tryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	j.chk.Reset()
	chkForJoin := j.chk
	if len(j.conditions) == 0 {
		chkForJoin = chk
	}
	outer, numToAppend, cursor := outers.Current(), chk.RequiredRows()-chk.NumRows(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		if j.outerIsRight {
			j.makeJoinRowToChunk(chkForJoin, inner, outer)
		} else {
			j.makeJoinRowToChunk(chkForJoin, outer, inner)
		}
	}
	outerRowStatus = outerRowStatus[:0]
	for i := 0; i < cursor; i++ {
		outerRowStatus = append(outerRowStatus, outerRowMatched)
	}
	if len(j.conditions) == 0 {
		return outerRowStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterRowStatus(chkForJoin, chk, inner.Len(), outerRowStatus)
}

func (j *innerJoiner) onMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
}

func (j *innerJoiner) Clone() joiner {
	return &innerJoiner{baseJoiner: j.baseJoiner.Clone()}
}
