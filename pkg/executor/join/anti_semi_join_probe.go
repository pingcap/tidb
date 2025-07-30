// Copyright 2024 PingCAP, Inc.
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
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type antiSemiJoinProbe struct {
	baseSemiJoin
}

func newAntiSemiJoinProbe(base baseJoinProbe, isLeftSideBuild bool) *antiSemiJoinProbe {
	ret := &antiSemiJoinProbe{
		baseSemiJoin: *newBaseSemiJoin(base, isLeftSideBuild),
	}

	if ret.ctx.hasOtherCondition() {
		ret.isNulls = make([]bool, 0, chunk.InitialCapacity)
	}

	return ret
}

func (a *antiSemiJoinProbe) InitForScanRowTable() {
	if !a.isLeftSideBuild {
		panic("should not reach here")
	}
	a.rowIter = commonInitForScanRowTable(&a.baseJoinProbe)
}

func (a *antiSemiJoinProbe) NeedScanRowTable() bool {
	return a.isLeftSideBuild
}

func (a *antiSemiJoinProbe) IsScanRowTableDone() bool {
	if !a.isLeftSideBuild {
		panic("should not reach here")
	}
	return a.rowIter.isEnd()
}

func (a *antiSemiJoinProbe) ScanRowTable(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) *hashjoinWorkerResult {
	if !a.isLeftSideBuild {
		panic("should not reach here")
	}
	if joinResult.chk.IsFull() {
		return joinResult
	}
	if a.rowIter == nil {
		panic("scanRowTable before init")
	}
	a.nextCachedBuildRowIndex = 0
	meta := a.ctx.hashTableMeta
	insertedRows := 0
	remainCap := joinResult.chk.RequiredRows() - joinResult.chk.NumRows()
	for insertedRows < remainCap && !a.rowIter.isEnd() {
		currentRow := a.rowIter.getValue()
		if !meta.isCurrentRowUsed(currentRow) {
			// append build side of this row
			a.appendBuildRowToCachedBuildRowsV1(0, currentRow, joinResult.chk, 0, false)
			insertedRows++
		}
		a.rowIter.next()
	}
	err := checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		joinResult.err = err
		return joinResult
	}
	if a.nextCachedBuildRowIndex > 0 {
		a.batchConstructBuildRows(joinResult.chk, 0, false)
	}
	return joinResult
}

func (a *antiSemiJoinProbe) ResetProbe() {
	a.rowIter = nil
	a.baseJoinProbe.ResetProbe()
}

func (a *antiSemiJoinProbe) resetProbeState() {
	a.baseSemiJoin.resetProbeState()

	if !a.isLeftSideBuild {
		if a.ctx.spillHelper.isSpillTriggered() {
			for _, idx := range a.spilledIdx {
				// We see rows that have be spilled as matched rows
				a.isMatchedRows[idx] = true
			}
		}
	}
}

func (a *antiSemiJoinProbe) SetChunkForProbe(chk *chunk.Chunk) (err error) {
	err = a.baseJoinProbe.SetChunkForProbe(chk)
	if err != nil {
		return err
	}

	a.resetProbeState()
	return nil
}

func (a *antiSemiJoinProbe) SetRestoredChunkForProbe(chk *chunk.Chunk) error {
	err := a.baseJoinProbe.SetRestoredChunkForProbe(chk)
	if err != nil {
		return err
	}

	a.resetProbeState()
	return nil
}

func (a *antiSemiJoinProbe) Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, _ *hashjoinWorkerResult) {
	if joinResult.chk.IsFull() {
		return true, joinResult
	}

	joinedChk, remainCap, err := a.prepareForProbe(joinResult.chk)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}

	hasOtherCondition := a.ctx.hasOtherCondition()
	if a.isLeftSideBuild {
		if hasOtherCondition {
			err = a.probeForLeftSideBuildHasOtherCondition(joinedChk, sqlKiller)
		} else {
			err = a.probeForLeftSideBuildNoOtherCondition(sqlKiller)
		}
	} else {
		if hasOtherCondition {
			err = a.probeForRightSideBuildHasOtherCondition(joinResult.chk, joinedChk, remainCap, sqlKiller)
		} else {
			err = a.probeForRightSideBuildNoOtherCondition(joinResult.chk, remainCap, sqlKiller)
		}
	}
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	return true, joinResult
}

func (a *antiSemiJoinProbe) probeForLeftSideBuildHasOtherCondition(joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) (err error) {
	err = a.concatenateProbeAndBuildRows(joinedChk, sqlKiller, false)
	if err != nil {
		return err
	}

	if a.unFinishedProbeRowIdxQueue.IsEmpty() {
		// To avoid `Previous chunk is not probed yet` error
		a.currentProbeRow = a.chunkRows
	}

	meta := a.ctx.hashTableMeta
	if joinedChk.NumRows() > 0 {
		a.selected, a.isNulls, err = expression.VecEvalBool(a.ctx.SessCtx.GetExprCtx().GetEvalCtx(), a.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, a.ctx.OtherCondition, joinedChk, a.selected, a.isNulls)
		if err != nil {
			return err
		}

		for index, result := range a.selected {
			if result || a.isNulls[index] {
				meta.setUsedFlag(*(*unsafe.Pointer)(unsafe.Pointer(&a.rowIndexInfos[index].buildRowStart)))
			}
		}
	}

	return
}

func (a *antiSemiJoinProbe) probeForLeftSideBuildNoOtherCondition(sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := a.ctx.hashTableMeta
	tagHelper := a.ctx.hashTableContext.tagHelper

	loopCnt := 0

	for a.currentProbeRow < a.chunkRows {
		if a.matchedRowsHeaders[a.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(a.matchedRowsHeaders[a.currentProbeRow])
			if !meta.isCurrentRowUsedWithAtomic(candidateRow) {
				if isKeyMatched(meta.keyMode, a.serializedKeys[a.currentProbeRow], candidateRow, meta) {
					meta.setUsedFlag(candidateRow)
				} else {
					a.probeCollision++
				}
			}
			a.matchedRowsHeaders[a.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, a.matchedRowsHashValue[a.currentProbeRow])
		} else {
			a.currentProbeRow++
		}

		loopCnt++
		if loopCnt%2000 == 0 {
			err = checkSQLKiller(sqlKiller, "killedDuringProbe")
			if err != nil {
				return err
			}
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	return err
}

func (a *antiSemiJoinProbe) produceResult(joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) (err error) {
	err = a.concatenateProbeAndBuildRows(joinedChk, sqlKiller, true)
	if err != nil {
		return err
	}

	if joinedChk.NumRows() > 0 {
		a.selected, a.isNulls, err = expression.VecEvalBool(a.ctx.SessCtx.GetExprCtx().GetEvalCtx(), a.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, a.ctx.OtherCondition, joinedChk, a.selected, a.isNulls)
		if err != nil {
			return err
		}

		length := len(a.selected)
		for i := range length {
			if a.selected[i] || a.isNulls[i] {
				probeRowIdx := a.rowIndexInfos[i].probeRowIndex
				a.isMatchedRows[probeRowIdx] = true
			}
		}
	}
	return
}

func (a *antiSemiJoinProbe) probeForRightSideBuildHasOtherCondition(chk, joinedChk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	if !a.unFinishedProbeRowIdxQueue.IsEmpty() {
		err = a.produceResult(joinedChk, sqlKiller)
		if err != nil {
			return err
		}
		a.currentProbeRow = 0
	}

	if a.unFinishedProbeRowIdxQueue.IsEmpty() {
		a.generateResultChkForRightBuildWithOtherCondition(remainCap, chk, a.isMatchedRows, false)
	}

	return
}

func (a *antiSemiJoinProbe) probeForRightSideBuildNoOtherCondition(chk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := a.ctx.hashTableMeta
	tagHelper := a.ctx.hashTableContext.tagHelper
	matched := false

	if cap(a.offsets) == 0 {
		a.offsets = make([]int, 0, remainCap)
	}

	a.offsets = a.offsets[:0]

	for remainCap > 0 && a.currentProbeRow < a.chunkRows {
		if a.matchedRowsHeaders[a.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(a.matchedRowsHeaders[a.currentProbeRow])
			if isKeyMatched(meta.keyMode, a.serializedKeys[a.currentProbeRow], candidateRow, meta) {
				matched = true
				a.matchedRowsHeaders[a.currentProbeRow] = 0
			} else {
				a.probeCollision++
				a.matchedRowsHeaders[a.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, a.matchedRowsHashValue[a.currentProbeRow])
			}
		} else {
			if a.ctx.spillHelper.isSpillTriggered() && a.isMatchedRows[a.currentProbeRow] {
				// We see rows that have be spilled as matched rows
				matched = true
			}

			if !matched {
				remainCap--
				a.offsets = append(a.offsets, a.usedRows[a.currentProbeRow])
			}

			matched = false
			a.currentProbeRow++
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}

	a.generateResultChkForRightBuildNoOtherCondition(chk)
	return
}

func (a *antiSemiJoinProbe) IsCurrentChunkProbeDone() bool {
	if a.ctx.hasOtherCondition() && !a.unFinishedProbeRowIdxQueue.IsEmpty() {
		return false
	}
	return a.baseJoinProbe.IsCurrentChunkProbeDone()
}
