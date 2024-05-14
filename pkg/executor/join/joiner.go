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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package join

import (
	"github.com/pingcap/tidb/pkg/expression"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	_ Joiner = &semiJoiner{}
	_ Joiner = &antiSemiJoiner{}
	_ Joiner = &nullAwareAntiSemiJoiner{}
	_ Joiner = &leftOuterSemiJoiner{}
	_ Joiner = &antiLeftOuterSemiJoiner{}
	_ Joiner = &nullAwareAntiLeftOuterSemiJoiner{}
	_ Joiner = &leftOuterJoiner{}
	_ Joiner = &rightOuterJoiner{}
	_ Joiner = &innerJoiner{}
)

// Joiner is used to generate join results according to the join type.
// A typical instruction flow is:
//
//	hasMatch, HasNull := false, false
//	for innerIter.Current() != innerIter.End() {
//	    matched, isNull, err := j.TryToMatchInners(Outer, innerIter, chk)
//	    // handle err
//	    hasMatch = hasMatch || matched
//	    HasNull = HasNull || isNull
//	}
//	if !hasMatch {
//	    j.OnMissMatch(HasNull, Outer, chk)
//	}
//
// NOTE: This interface is **not** thread-safe.
// TODO: unit test
// for all join type
//  1. no filter, no inline projection
//  2. no filter, inline projection
//  3. no filter, inline projection to empty column
//  4. filter, no inline projection
//  5. filter, inline projection
//  6. filter, inline projection to empty column
type Joiner interface {
	// TryToMatchInners tries to join an outer row with a batch of inner rows. When
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
	TryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk, opt ...NAAJType) (matched bool, isNull bool, err error)

	// TryToMatchOuters tries to join a batch of outer rows with one inner row.
	// It's used when the join is an outer join and the hash table is built
	// using the outer side.
	TryToMatchOuters(outer chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error)

	// OnMissMatch operates on the unmatched outer row according to the join
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
	// parameter passed to `OnMissMatch`.
	OnMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk)

	// isSemiJoinWithoutCondition returns if it's a semi join and has no condition.
	// If true, at most one matched row is needed to match inners, which can optimize a lot when
	// there are a lot of matched rows.
	isSemiJoinWithoutCondition() bool

	// Clone deep copies a Joiner.
	Clone() Joiner
}

// JoinerType returns the join type of a Joiner.
func JoinerType(j Joiner) plannercore.JoinType {
	switch j.(type) {
	case *semiJoiner:
		return plannercore.SemiJoin
	case *antiSemiJoiner:
		return plannercore.AntiSemiJoin
	case *leftOuterSemiJoiner:
		return plannercore.LeftOuterSemiJoin
	case *antiLeftOuterSemiJoiner:
		return plannercore.AntiLeftOuterSemiJoin
	case *leftOuterJoiner:
		return plannercore.LeftOuterJoin
	case *rightOuterJoiner:
		return plannercore.RightOuterJoin
	default:
		return plannercore.InnerJoin
	}
}

// NewJoiner create a joiner
func NewJoiner(ctx sessionctx.Context, joinType plannercore.JoinType,
	outerIsRight bool, defaultInner []types.Datum, filter []expression.Expression,
	lhsColTypes, rhsColTypes []*types.FieldType, childrenUsed [][]int, isNA bool) Joiner {
	base := baseJoiner{
		ctx:          ctx,
		conditions:   filter,
		outerIsRight: outerIsRight,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
	base.selected = make([]bool, 0, chunk.InitialCapacity)
	base.isNull = make([]bool, 0, chunk.InitialCapacity)
	// lused and rused should be followed with its original order.
	// the case is that is join schema rely on the reversed order
	// of child's schema, here we should keep it original order.
	if childrenUsed != nil {
		base.lUsed = make([]int, 0, len(childrenUsed[0])) // make it non-nil
		base.lUsed = append(base.lUsed, childrenUsed[0]...)
		base.rUsed = make([]int, 0, len(childrenUsed[1])) // make it non-nil
		base.rUsed = append(base.rUsed, childrenUsed[1]...)
		logutil.BgLogger().Debug("InlineProjection",
			zap.Ints("lUsed", base.lUsed), zap.Ints("rUsed", base.rUsed),
			zap.Int("lCount", len(lhsColTypes)), zap.Int("rCount", len(rhsColTypes)))
	}
	if joinType == plannercore.LeftOuterJoin || joinType == plannercore.RightOuterJoin {
		innerColTypes := lhsColTypes
		if !outerIsRight {
			innerColTypes = rhsColTypes
		}
		base.initDefaultInner(innerColTypes, defaultInner)
	}
	// shallowRowType may be different with the output columns because output columns may
	// be pruned inline, while shallow row should not be because each column may need
	// be used in filter.
	shallowRowType := make([]*types.FieldType, 0, len(lhsColTypes)+len(rhsColTypes))
	shallowRowType = append(shallowRowType, lhsColTypes...)
	shallowRowType = append(shallowRowType, rhsColTypes...)
	switch joinType {
	case plannercore.SemiJoin:
		base.shallowRow = chunk.MutRowFromTypes(shallowRowType)
		return &semiJoiner{base}
	case plannercore.AntiSemiJoin:
		base.shallowRow = chunk.MutRowFromTypes(shallowRowType)
		if isNA {
			return &nullAwareAntiSemiJoiner{baseJoiner: base}
		}
		return &antiSemiJoiner{base}
	case plannercore.LeftOuterSemiJoin:
		base.shallowRow = chunk.MutRowFromTypes(shallowRowType)
		return &leftOuterSemiJoiner{base}
	case plannercore.AntiLeftOuterSemiJoin:
		base.shallowRow = chunk.MutRowFromTypes(shallowRowType)
		if isNA {
			return &nullAwareAntiLeftOuterSemiJoiner{baseJoiner: base}
		}
		return &antiLeftOuterSemiJoiner{base}
	case plannercore.LeftOuterJoin, plannercore.RightOuterJoin, plannercore.InnerJoin:
		if len(base.conditions) > 0 {
			base.chk = chunk.NewChunkWithCapacity(shallowRowType, ctx.GetSessionVars().MaxChunkSize)
		}
		switch joinType {
		case plannercore.LeftOuterJoin:
			return &leftOuterJoiner{base}
		case plannercore.RightOuterJoin:
			return &rightOuterJoiner{base}
		case plannercore.InnerJoin:
			return &innerJoiner{base}
		}
	}
	panic("unsupported join type in func NewJoiner()")
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

	// lUsed/rUsed show which columns are used by father for left child and right child.
	// NOTE:
	// 1. every columns are used if lUsed/rUsed is nil.
	// 2. no columns are used if lUsed/rUsed is not nil but the size of lUsed/rUsed is 0.
	lUsed, rUsed []int
}

func (j *baseJoiner) initDefaultInner(innerTypes []*types.FieldType, defaultInner []types.Datum) {
	mutableRow := chunk.MutRowFromTypes(innerTypes)
	mutableRow.SetDatums(defaultInner[:len(innerTypes)]...)
	j.defaultInner = mutableRow.ToRow()
}

func (*baseJoiner) makeJoinRowToChunk(chk *chunk.Chunk, lhs, rhs chunk.Row, lUsed, rUsed []int) {
	// Call AppendRow() first to increment the virtual rows.
	// Fix: https://github.com/pingcap/tidb/issues/5771
	lWide := chk.AppendRowByColIdxs(lhs, lUsed)
	chk.AppendPartialRowByColIdxs(lWide, rhs, rUsed)
}

// makeShallowJoinRow shallow copies `inner` and `outer` into `shallowRow`.
// It should not consider `j.lUsed` and `j.rUsed`, because the columns which
// need to be used in `j.conditions` may not exist in outputs.
func (j *baseJoiner) makeShallowJoinRow(isRightJoin bool, inner, outer chunk.Row) {
	if !isRightJoin {
		inner, outer = outer, inner
	}
	j.shallowRow.ShallowCopyPartialRow(0, inner)
	j.shallowRow.ShallowCopyPartialRow(inner.Len(), outer)
}

// filter is used to filter the result constructed by TryToMatchInners, the result is
// built by one outer row and multiple inner rows. The returned bool value
// indicates whether the outer row matches any inner rows.
func (j *baseJoiner) filter(input, output *chunk.Chunk, outerColLen int, lUsed, rUsed []int) (bool, error) {
	var err error
	j.selected, err = expression.VectorizedFilter(j.ctx.GetExprCtx().GetEvalCtx(), j.ctx.GetSessionVars().EnableVectorizedExpression, j.conditions, chunk.NewIterator4Chunk(input), j.selected)
	if err != nil {
		return false, err
	}
	// Batch copies selected rows to output chunk.
	innerColOffset, outerColOffset := 0, input.NumCols()-outerColLen
	innerColLen := input.NumCols() - outerColLen
	if !j.outerIsRight {
		innerColOffset, outerColOffset = outerColLen, 0
	}
	if lUsed != nil || rUsed != nil {
		lSize := outerColOffset
		if !j.outerIsRight {
			lSize = innerColOffset
		}
		used := make([]int, len(lUsed)+len(rUsed))
		copy(used, lUsed)
		for i := range rUsed {
			used[i+len(lUsed)] = rUsed[i] + lSize
		}
		input = input.Prune(used)

		innerColOffset, outerColOffset = 0, len(lUsed)
		innerColLen, outerColLen = len(lUsed), len(rUsed)
		if !j.outerIsRight {
			innerColOffset, outerColOffset = len(lUsed), 0
			innerColLen, outerColLen = outerColLen, innerColLen
		}
	}
	return chunk.CopySelectedJoinRowsWithSameOuterRows(input, innerColOffset, innerColLen, outerColOffset, outerColLen, j.selected, output)
}

// filterAndCheckOuterRowStatus is used to filter the result constructed by
// TryToMatchOuters, the result is built by multiple outer rows and one inner
// row. The returned outerRowStatusFlag slice value indicates the status of
// each outer row (matched/unmatched/hasNull).
func (j *baseJoiner) filterAndCheckOuterRowStatus(
	input, output *chunk.Chunk, innerColsLen int, outerRowStatus []outerRowStatusFlag,
	lUsed, rUsed []int) ([]outerRowStatusFlag, error) {
	var err error
	j.selected, j.isNull, err = expression.VectorizedFilterConsiderNull(j.ctx.GetExprCtx().GetEvalCtx(), j.ctx.GetSessionVars().EnableVectorizedExpression, j.conditions, chunk.NewIterator4Chunk(input), j.selected, j.isNull)
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

	if lUsed != nil || rUsed != nil {
		lSize := innerColsLen
		if !j.outerIsRight {
			lSize = input.NumCols() - innerColsLen
		}
		used := make([]int, len(lUsed)+len(rUsed))
		copy(used, lUsed)
		for i := range rUsed {
			used[i+len(lUsed)] = rUsed[i] + lSize
		}
		input = input.Prune(used)
	}
	// Batch copies selected rows to output chunk.
	_, err = chunk.CopySelectedJoinRowsDirect(input, j.selected, output)
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
		base.shallowRow = j.shallowRow.Clone()
	}
	if !j.defaultInner.IsEmpty() {
		base.defaultInner = j.defaultInner.CopyConstruct()
	}
	if j.lUsed != nil {
		base.lUsed = make([]int, len(j.lUsed))
		copy(base.lUsed, j.lUsed)
	}
	if j.rUsed != nil {
		base.rUsed = make([]int, len(j.rUsed))
		copy(base.rUsed, j.rUsed)
	}
	return base
}

type semiJoiner struct {
	baseJoiner
}

func (j *semiJoiner) TryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk, _ ...NAAJType) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		chk.AppendRowByColIdxs(outer, j.lUsed) // TODO: need test numVirtualRow
		inners.ReachEnd()
		return true, false, nil
	}

	evalCtx := j.ctx.GetExprCtx().GetEvalCtx()
	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinRow(j.outerIsRight, inner, outer)

		// For SemiJoin, we can safely treat null result of join conditions as false,
		// so we ignore the nullness returned by EvalBool here.
		matched, _, err = expression.EvalBool(evalCtx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return false, false, err
		}
		if matched {
			chk.AppendRowByColIdxs(outer, j.lUsed)
			inners.ReachEnd()
			return true, false, nil
		}
	}
	err = inners.Error()
	return false, false, err
}

func (j *semiJoiner) TryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	outerRowStatus = outerRowStatus[:0]
	outer, numToAppend := outers.Current(), chk.RequiredRows()-chk.NumRows()
	if len(j.conditions) == 0 {
		for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
			chk.AppendRowByColIdxs(outer, j.lUsed)
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		}
		return outerRowStatus, nil
	}
	exprCtx := j.ctx.GetExprCtx()
	for outer := outers.Current(); outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
		j.makeShallowJoinRow(j.outerIsRight, inner, outer)
		// For SemiJoin, we can safely treat null result of join conditions as false,
		// so we ignore the nullness returned by EvalBool here.
		matched, _, err := expression.EvalBool(exprCtx.GetEvalCtx(), j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return outerRowStatus, err
		}
		if matched {
			outerRowStatus = append(outerRowStatus, outerRowMatched)
			chk.AppendRowByColIdxs(outer, j.lUsed)
		} else {
			outerRowStatus = append(outerRowStatus, outerRowUnmatched)
		}
	}
	err = outers.Error()
	return outerRowStatus, err
}

func (*semiJoiner) OnMissMatch(bool, chunk.Row, *chunk.Chunk) {}

func (j *semiJoiner) isSemiJoinWithoutCondition() bool {
	return len(j.conditions) == 0
}

// Clone implements Joiner interface.
func (j *semiJoiner) Clone() Joiner {
	return &semiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

// NAAJType is join detail type only used by null-aware AntiLeftOuterSemiJoin.
type NAAJType byte

const (
	// Unknown for those default value.
	Unknown NAAJType = 0
	// LeftHasNullRightNotNull means lhs is a null key, and rhs is not a null key.
	LeftHasNullRightNotNull NAAJType = 1
	// LeftHasNullRightHasNull means lhs is a null key, and rhs is a null key.
	LeftHasNullRightHasNull NAAJType = 2
	// LeftNotNullRightNotNull means lhs is in not a null key, and rhs is not a null key.
	LeftNotNullRightNotNull NAAJType = 3
	// LeftNotNullRightHasNull means lhs is in not a null key, and rhs is a null key.
	LeftNotNullRightHasNull NAAJType = 4
)

type nullAwareAntiSemiJoiner struct {
	baseJoiner
}

// tryToMatchInners implements Joiner interface.
func (naaj *nullAwareAntiSemiJoiner) TryToMatchInners(outer chunk.Row, inners chunk.Iterator, _ *chunk.Chunk, _ ...NAAJType) (matched bool, hasNull bool, err error) {
	// Step1: inner rows come from NULL-bucket OR Same-Key bucket. (no rows mean not matched)
	if inners.Len() == 0 {
		return false, false, nil
	}
	// Step2: conditions come from other condition.
	if len(naaj.conditions) == 0 {
		// once there is no other condition, that means right ride has non-empty valid rows. (all matched)
		inners.ReachEnd()
		return true, false, nil
	}
	evalCtx := naaj.ctx.GetExprCtx().GetEvalCtx()
	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		naaj.makeShallowJoinRow(naaj.outerIsRight, inner, outer)
		valid, _, err := expression.EvalBool(evalCtx, naaj.conditions, naaj.shallowRow.ToRow())
		if err != nil {
			return false, false, err
		}
		// since other condition is only from inner where clause, here we can say:
		// for x NOT IN (y set) semantics, once we found an x in y set, it's determined already. (refuse probe row, append nothing)
		if valid {
			inners.ReachEnd()
			return true, false, nil
		}
		// false or null means that this merged row can't pass the other condition, not a valid right side row. (continue)
	}
	err = inners.Error()
	return false, false, err
}

func (*nullAwareAntiSemiJoiner) TryToMatchOuters(_ chunk.Iterator, _ chunk.Row, _ *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	// todo: use the Outer build.
	return outerRowStatus, err
}

func (naaj *nullAwareAntiSemiJoiner) OnMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
	chk.AppendRowByColIdxs(outer, naaj.lUsed)
}

func (naaj *nullAwareAntiSemiJoiner) isSemiJoinWithoutCondition() bool {
	return len(naaj.conditions) == 0
}

func (naaj *nullAwareAntiSemiJoiner) Clone() Joiner {
	return &nullAwareAntiSemiJoiner{baseJoiner: naaj.baseJoiner.Clone()}
}

type antiSemiJoiner struct {
	baseJoiner
}

// tryToMatchInners implements Joiner interface.
func (j *antiSemiJoiner) TryToMatchInners(outer chunk.Row, inners chunk.Iterator, _ *chunk.Chunk, _ ...NAAJType) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		inners.ReachEnd()
		return true, false, nil
	}

	evalCtx := j.ctx.GetExprCtx().GetEvalCtx()
	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinRow(j.outerIsRight, inner, outer)

		matched, isNull, err := expression.EvalBool(evalCtx, j.conditions, j.shallowRow.ToRow())
		if err != nil {
			return false, false, err
		}
		if matched {
			inners.ReachEnd()
			return true, false, nil
		}
		hasNull = hasNull || isNull
	}
	err = inners.Error()
	return false, hasNull, err
}

func (j *antiSemiJoiner) TryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	outerRowStatus = outerRowStatus[:0]
	numToAppend := chk.RequiredRows() - chk.NumRows()
	if len(j.conditions) == 0 {
		for ; outers.Current() != outers.End(); outers.Next() {
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		}
		return outerRowStatus, nil
	}
	evalCtx := j.ctx.GetExprCtx().GetEvalCtx()
	for outer := outers.Current(); outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
		j.makeShallowJoinRow(j.outerIsRight, inner, outer)
		matched, isNull, err := expression.EvalBool(evalCtx, j.conditions, j.shallowRow.ToRow())
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
	err = outers.Error()
	return outerRowStatus, err
}

func (j *antiSemiJoiner) OnMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk) {
	if !hasNull {
		chk.AppendRowByColIdxs(outer, j.lUsed)
	}
}

func (j *antiSemiJoiner) isSemiJoinWithoutCondition() bool {
	return len(j.conditions) == 0
}

func (j *antiSemiJoiner) Clone() Joiner {
	return &antiSemiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type leftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatchInners implements Joiner interface.
func (j *leftOuterSemiJoiner) TryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk, _ ...NAAJType) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		j.onMatch(outer, chk)
		inners.ReachEnd()
		return true, false, nil
	}

	evalCtx := j.ctx.GetExprCtx().GetEvalCtx()
	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinRow(false, inner, outer)

		matched, isNull, err := expression.EvalBool(evalCtx, j.conditions, j.shallowRow.ToRow())
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
	err = inners.Error()
	return false, hasNull, err
}

func (j *leftOuterSemiJoiner) TryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	outerRowStatus = outerRowStatus[:0]
	outer, numToAppend := outers.Current(), chk.RequiredRows()-chk.NumRows()
	if len(j.conditions) == 0 {
		for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
			j.onMatch(outer, chk)
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		}
		return outerRowStatus, nil
	}

	evalCtx := j.ctx.GetExprCtx().GetEvalCtx()
	for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
		j.makeShallowJoinRow(false, inner, outer)
		matched, isNull, err := expression.EvalBool(evalCtx, j.conditions, j.shallowRow.ToRow())
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
	err = outers.Error()
	return outerRowStatus, err
}

func (j *leftOuterSemiJoiner) onMatch(outer chunk.Row, chk *chunk.Chunk) {
	lWide := chk.AppendRowByColIdxs(outer, j.lUsed)
	chk.AppendInt64(lWide, 1)
}

func (j *leftOuterSemiJoiner) OnMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk) {
	lWide := chk.AppendRowByColIdxs(outer, j.lUsed)
	if hasNull {
		chk.AppendNull(lWide)
	} else {
		chk.AppendInt64(lWide, 0)
	}
}

func (j *leftOuterSemiJoiner) isSemiJoinWithoutCondition() bool {
	return len(j.conditions) == 0
}

func (j *leftOuterSemiJoiner) Clone() Joiner {
	return &leftOuterSemiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type nullAwareAntiLeftOuterSemiJoiner struct {
	baseJoiner
}

func (naal *nullAwareAntiLeftOuterSemiJoiner) TryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk, opt ...NAAJType) (matched bool, _ bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	// Difference between nullAwareAntiLeftOuterSemiJoiner and AntiLeftOuterSemiJoiner.
	// AntiLeftOuterSemiJoiner conditions contain NA-EQ and inner filters. In EvalBool, once either side has a null value in NA-EQ
	//     column operand, it will lead a false matched, and a true value of isNull. (which only admit not-null same key match)
	// nullAwareAntiLeftOuterSemiJoiner conditions only contain inner filters. in EvalBool, any filter null or false will contribute
	//     to false matched, in other words, the isNull is permanently false.
	if len(naal.conditions) == 0 {
		// no inner filter other condition means all matched. (inners are valid source)
		naal.onMatch(outer, chk, opt...)
		inners.ReachEnd()
		return true, false, nil
	}

	evalCtx := naal.ctx.GetExprCtx().GetEvalCtx()
	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		naal.makeShallowJoinRow(false, inner, outer)

		valid, _, err := expression.EvalBool(evalCtx, naal.conditions, naal.shallowRow.ToRow())
		if err != nil {
			return false, false, err
		}
		if valid {
			// once find a valid inner row, we can determine the result already.
			naal.onMatch(outer, chk, opt...)
			inners.ReachEnd()
			return true, false, nil
		}
	}
	err = inners.Error()
	return false, false, err
}

func (naal *nullAwareAntiLeftOuterSemiJoiner) onMatch(outer chunk.Row, chk *chunk.Chunk, opt ...NAAJType) {
	switch opt[0] {
	case LeftNotNullRightNotNull:
		// either side are not null. (x NOT IN (x...)) --> (rhs, 0)
		lWide := chk.AppendRowByColIdxs(outer, naal.lUsed)
		chk.AppendInt64(lWide, 0)
	case LeftNotNullRightHasNull:
		// right side has a null NA-EQ key. (x NOT IN (null...)) --> (rhs, null)
		lWide := chk.AppendRowByColIdxs(outer, naal.lUsed)
		chk.AppendNull(lWide)
	case LeftHasNullRightHasNull, LeftHasNullRightNotNull:
		// left side has a null NA-EQ key. (null NOT IN (what ever valid inner)) --(rhs, null)
		lWide := chk.AppendRowByColIdxs(outer, naal.lUsed)
		chk.AppendNull(lWide)
	}
}

func (naal *nullAwareAntiLeftOuterSemiJoiner) OnMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
	// once come to here, it means we couldn't make it in previous short paths.
	// cases like:
	// 1: null/x NOT IN (empty set)
	// 2: x NOT IN (non-empty set without x and null)
	lWide := chk.AppendRowByColIdxs(outer, naal.lUsed)
	chk.AppendInt64(lWide, 1)
}

func (*nullAwareAntiLeftOuterSemiJoiner) TryToMatchOuters(chunk.Iterator, chunk.Row, *chunk.Chunk, []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	// todo:
	return nil, err
}

func (naal *nullAwareAntiLeftOuterSemiJoiner) isSemiJoinWithoutCondition() bool {
	return len(naal.conditions) == 0
}

func (naal *nullAwareAntiLeftOuterSemiJoiner) Clone() Joiner {
	return &nullAwareAntiLeftOuterSemiJoiner{baseJoiner: naal.baseJoiner.Clone()}
}

type antiLeftOuterSemiJoiner struct {
	baseJoiner
}

// tryToMatchInners implements Joiner interface.
func (j *antiLeftOuterSemiJoiner) TryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk, _ ...NAAJType) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}

	if len(j.conditions) == 0 {
		j.onMatch(outer, chk)
		inners.ReachEnd()
		return true, false, nil
	}

	evalCtx := j.ctx.GetExprCtx().GetEvalCtx()
	for inner := inners.Current(); inner != inners.End(); inner = inners.Next() {
		j.makeShallowJoinRow(false, inner, outer)

		matched, isNull, err := expression.EvalBool(evalCtx, j.conditions, j.shallowRow.ToRow())
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
	err = inners.Error()
	return false, hasNull, err
}

func (j *antiLeftOuterSemiJoiner) TryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	outerRowStatus = outerRowStatus[:0]
	outer, numToAppend := outers.Current(), chk.RequiredRows()-chk.NumRows()
	if len(j.conditions) == 0 {
		for ; outer != outers.End() && numToAppend > 0; outer, numToAppend = outers.Next(), numToAppend-1 {
			j.onMatch(outer, chk)
			outerRowStatus = append(outerRowStatus, outerRowMatched)
		}
		return outerRowStatus, nil
	}

	evalCtx := j.ctx.GetExprCtx().GetEvalCtx()
	for i := 0; outer != outers.End() && numToAppend > 0; outer, numToAppend, i = outers.Next(), numToAppend-1, i+1 {
		j.makeShallowJoinRow(false, inner, outer)
		matched, isNull, err := expression.EvalBool(evalCtx, j.conditions, j.shallowRow.ToRow())
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
	err = outers.Error()
	if err != nil {
		return
	}
	return outerRowStatus, nil
}

func (j *antiLeftOuterSemiJoiner) onMatch(outer chunk.Row, chk *chunk.Chunk) {
	lWide := chk.AppendRowByColIdxs(outer, j.lUsed)
	chk.AppendInt64(lWide, 0)
}

func (j *antiLeftOuterSemiJoiner) OnMissMatch(hasNull bool, outer chunk.Row, chk *chunk.Chunk) {
	lWide := chk.AppendRowByColIdxs(outer, j.lUsed)
	if hasNull {
		chk.AppendNull(lWide)
	} else {
		chk.AppendInt64(lWide, 1)
	}
}

func (j *antiLeftOuterSemiJoiner) isSemiJoinWithoutCondition() bool {
	return len(j.conditions) == 0
}

func (j *antiLeftOuterSemiJoiner) Clone() Joiner {
	return &antiLeftOuterSemiJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type leftOuterJoiner struct {
	baseJoiner
}

// tryToMatchInners implements Joiner interface.
func (j *leftOuterJoiner) TryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk, _ ...NAAJType) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	numToAppend := chk.RequiredRows() - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		j.makeJoinRowToChunk(chkForJoin, outer, inners.Current(), lUsed, rUsed)
		inners.Next()
	}
	err = inners.Error()
	if err != nil {
		return false, false, err
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err = j.filter(chkForJoin, chk, outer.Len(), lUsedForFilter, rUsedForFilter)
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *leftOuterJoiner) TryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	outer, numToAppend, cursor := outers.Current(), chk.RequiredRows()-chk.NumRows(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		j.makeJoinRowToChunk(chkForJoin, outer, inner, lUsed, rUsed)
	}
	err = outers.Error()
	if err != nil {
		return
	}
	outerRowStatus = outerRowStatus[:0]
	for i := 0; i < cursor; i++ {
		outerRowStatus = append(outerRowStatus, outerRowMatched)
	}
	if len(j.conditions) == 0 {
		return outerRowStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterRowStatus(chkForJoin, chk, inner.Len(), outerRowStatus, lUsedForFilter, rUsedForFilter)
}

func (j *leftOuterJoiner) OnMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
	lWide := chk.AppendRowByColIdxs(outer, j.lUsed)
	chk.AppendPartialRowByColIdxs(lWide, j.defaultInner, j.rUsed)
}

func (*leftOuterJoiner) isSemiJoinWithoutCondition() bool {
	return false
}

func (j *leftOuterJoiner) Clone() Joiner {
	return &leftOuterJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type rightOuterJoiner struct {
	baseJoiner
}

// tryToMatchInners implements Joiner interface.
func (j *rightOuterJoiner) TryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk, _ ...NAAJType) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	numToAppend := chk.RequiredRows() - chk.NumRows()
	for ; inners.Current() != inners.End() && numToAppend > 0; numToAppend-- {
		j.makeJoinRowToChunk(chkForJoin, inners.Current(), outer, lUsed, rUsed)
		inners.Next()
	}
	err = inners.Error()
	if err != nil {
		return false, false, err
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	matched, err = j.filter(chkForJoin, chk, outer.Len(), lUsedForFilter, rUsedForFilter)
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *rightOuterJoiner) TryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	outer, numToAppend, cursor := outers.Current(), chk.RequiredRows()-chk.NumRows(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		j.makeJoinRowToChunk(chkForJoin, inner, outer, lUsed, rUsed)
	}

	outerRowStatus = outerRowStatus[:0]
	for i := 0; i < cursor; i++ {
		outerRowStatus = append(outerRowStatus, outerRowMatched)
	}
	if len(j.conditions) == 0 {
		return outerRowStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterRowStatus(chkForJoin, chk, inner.Len(), outerRowStatus, lUsedForFilter, rUsedForFilter)
}

func (j *rightOuterJoiner) OnMissMatch(_ bool, outer chunk.Row, chk *chunk.Chunk) {
	lWide := chk.AppendRowByColIdxs(j.defaultInner, j.lUsed)
	chk.AppendPartialRowByColIdxs(lWide, outer, j.rUsed)
}

func (*rightOuterJoiner) isSemiJoinWithoutCondition() bool {
	return false
}

func (j *rightOuterJoiner) Clone() Joiner {
	return &rightOuterJoiner{baseJoiner: j.baseJoiner.Clone()}
}

type innerJoiner struct {
	baseJoiner
}

// tryToMatchInners implements Joiner interface.
func (j *innerJoiner) TryToMatchInners(outer chunk.Row, inners chunk.Iterator, chk *chunk.Chunk, _ ...NAAJType) (matched bool, hasNull bool, err error) {
	if inners.Len() == 0 {
		return false, false, nil
	}
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	inner, numToAppend := inners.Current(), chk.RequiredRows()-chk.NumRows()
	for ; inner != inners.End() && numToAppend > 0; inner, numToAppend = inners.Next(), numToAppend-1 {
		if j.outerIsRight {
			j.makeJoinRowToChunk(chkForJoin, inner, outer, lUsed, rUsed)
		} else {
			j.makeJoinRowToChunk(chkForJoin, outer, inner, lUsed, rUsed)
		}
	}
	err = inners.Error()
	if err != nil {
		return false, false, err
	}
	if len(j.conditions) == 0 {
		return true, false, nil
	}

	// reach here, chkForJoin is j.chk
	matched, err = j.filter(chkForJoin, chk, outer.Len(), lUsedForFilter, rUsedForFilter)
	if err != nil {
		return false, false, err
	}
	return matched, false, nil
}

func (j *innerJoiner) TryToMatchOuters(outers chunk.Iterator, inner chunk.Row, chk *chunk.Chunk, outerRowStatus []outerRowStatusFlag) (_ []outerRowStatusFlag, err error) {
	chkForJoin := chk
	lUsed, rUsed := j.lUsed, j.rUsed
	var lUsedForFilter, rUsedForFilter []int
	if len(j.conditions) > 0 {
		j.chk.Reset()
		chkForJoin = j.chk
		lUsed, rUsed = nil, nil
		lUsedForFilter, rUsedForFilter = j.lUsed, j.rUsed
	}

	outer, numToAppend, cursor := outers.Current(), chk.RequiredRows()-chk.NumRows(), 0
	for ; outer != outers.End() && cursor < numToAppend; outer, cursor = outers.Next(), cursor+1 {
		if j.outerIsRight {
			j.makeJoinRowToChunk(chkForJoin, inner, outer, lUsed, rUsed)
		} else {
			j.makeJoinRowToChunk(chkForJoin, outer, inner, lUsed, rUsed)
		}
	}
	err = outers.Error()
	if err != nil {
		return
	}
	outerRowStatus = outerRowStatus[:0]
	for i := 0; i < cursor; i++ {
		outerRowStatus = append(outerRowStatus, outerRowMatched)
	}
	if len(j.conditions) == 0 {
		return outerRowStatus, nil
	}
	// reach here, chkForJoin is j.chk
	return j.filterAndCheckOuterRowStatus(chkForJoin, chk, inner.Len(), outerRowStatus, lUsedForFilter, rUsedForFilter)
}

func (*innerJoiner) OnMissMatch(bool, chunk.Row, *chunk.Chunk) {}

func (*innerJoiner) isSemiJoinWithoutCondition() bool {
	return false
}

func (j *innerJoiner) Clone() Joiner {
	return &innerJoiner{baseJoiner: j.baseJoiner.Clone()}
}
