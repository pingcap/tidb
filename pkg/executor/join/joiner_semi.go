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
	"github.com/pingcap/tidb/pkg/util/chunk"
)

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

