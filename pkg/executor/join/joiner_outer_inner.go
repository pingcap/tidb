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
	"github.com/pingcap/tidb/pkg/util/chunk"
)

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
	for range cursor {
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
	for range cursor {
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
	for range cursor {
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
