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

// TODO merge common codes in semi join and anti-semi join
type antiSemiJoinProbe struct {
	baseJoinProbe
	isLeftSideBuild bool

	groupMark          []int
	matchedProbeRowIdx map[int]struct{}

	// used when left side is build side
	rowIter *rowIter

	isNulls []bool
}

func newAntiSemiJoinProbe(base baseJoinProbe, isLeftSideBuild bool) *antiSemiJoinProbe {
	ret := &antiSemiJoinProbe{
		baseJoinProbe:   base,
		isLeftSideBuild: isLeftSideBuild,
	}

	if !ret.isLeftSideBuild && ret.ctx.hasOtherCondition() {
		ret.groupMark = make([]int, 0, ret.ctx.SessCtx.GetSessionVars().MaxChunkSize)
		ret.isNulls = make([]bool, 0, ret.ctx.SessCtx.GetSessionVars().MaxChunkSize)
		ret.matchedProbeRowIdx = make(map[int]struct{})
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

func (a *antiSemiJoinProbe) Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, _ *hashjoinWorkerResult) {
	if joinResult.chk.IsFull() {
		return true, joinResult
	}

	joinedChk, remainCap, err := a.prepareForProbe(joinResult.chk)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}

	isInCompleteChunk := joinedChk.IsInCompleteChunk()
	// in case that virtual rows is not maintained correctly
	joinedChk.SetNumVirtualRows(joinedChk.NumRows())
	// always set in complete chunk during probe
	joinedChk.SetInCompleteChunk(true)
	defer joinedChk.SetInCompleteChunk(isInCompleteChunk)

	hasOtherCondition := a.ctx.hasOtherCondition()
	if a.isLeftSideBuild {
		if hasOtherCondition {
			err = a.probeForLeftSideBuildHasOtherCondition(joinedChk, sqlKiller)
		} else {
			err = a.probeForLeftSideBuildNoOtherCondition(sqlKiller)
		}
	} else {
		if hasOtherCondition {
			a.groupMark = a.groupMark[:0]
			clear(a.matchedProbeRowIdx)
			err = a.probeForRightSideBuildHasOtherCondition(joinResult.chk, joinedChk, sqlKiller)
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
	meta := a.ctx.hashTableMeta
	tagHelper := a.ctx.hashTableContext.tagHelper
	remainCap := joinedChk.Capacity()

	for remainCap > 0 && a.currentProbeRow < a.chunkRows {
		if a.matchedRowsHeaders[a.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(a.matchedRowsHeaders[a.currentProbeRow])
			if isKeyMatched(meta.keyMode, a.serializedKeys[a.currentProbeRow], candidateRow, meta) {
				a.appendBuildRowToCachedBuildRowsV1(a.currentProbeRow, candidateRow, joinedChk, 0, true)
				a.matchedRowsForCurrentProbeRow++
			} else {
				a.probeCollision++
			}
			a.matchedRowsHeaders[a.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, a.matchedRowsHashValue[a.currentProbeRow])
		} else {
			a.finishLookupCurrentProbeRow()
			remainCap--
			a.currentProbeRow++
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}
	a.finishCurrentLookupLoop(joinedChk)

	if joinedChk.NumRows() > 0 {
		a.selected, err = expression.VectorizedFilter(a.ctx.SessCtx.GetExprCtx().GetEvalCtx(), a.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, a.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), a.selected)
		if err != nil {
			return err
		}

		for index, result := range a.selected {
			if result {
				meta.setUsedFlag(*(*unsafe.Pointer)(unsafe.Pointer(&a.rowIndexInfos[index].buildRowStart)))
			}
		}
	}

	return
}

func (a *antiSemiJoinProbe) probeForLeftSideBuildNoOtherCondition(sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := a.ctx.hashTableMeta
	tagHelper := a.ctx.hashTableContext.tagHelper

	for a.currentProbeRow < a.chunkRows {
		if a.matchedRowsHeaders[a.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(a.matchedRowsHeaders[a.currentProbeRow])
			if isKeyMatched(meta.keyMode, a.serializedKeys[a.currentProbeRow], candidateRow, meta) {
				meta.setUsedFlag(candidateRow)
			} else {
				a.probeCollision++
			}
			a.matchedRowsHeaders[a.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, a.matchedRowsHashValue[a.currentProbeRow])
		} else {
			a.currentProbeRow++
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	return err
}

func (a *antiSemiJoinProbe) probeForRightSideBuildHasOtherCondition(chk, joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := a.ctx.hashTableMeta
	tagHelper := a.ctx.hashTableContext.tagHelper
	for a.currentProbeRow < a.chunkRows {
		joinedChk.Reset()
		a.groupMark = a.groupMark[:0]
		remainCap := joinedChk.Capacity()
		for remainCap > 0 && a.currentProbeRow < a.chunkRows {
			if a.matchedRowsHeaders[a.currentProbeRow] != 0 {
				candidateRow := tagHelper.toUnsafePointer(a.matchedRowsHeaders[a.currentProbeRow])
				if isKeyMatched(meta.keyMode, a.serializedKeys[a.currentProbeRow], candidateRow, meta) {
					a.appendBuildRowToCachedBuildRowsV1(a.currentProbeRow, candidateRow, joinedChk, 0, true)
					remainCap--
					a.matchedRowsForCurrentProbeRow++
					a.groupMark = append(a.groupMark, a.currentProbeRow)
				} else {
					a.probeCollision++
				}
				a.matchedRowsHeaders[a.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, a.matchedRowsHashValue[a.currentProbeRow])
			} else {
				a.finishLookupCurrentProbeRow()
				a.currentProbeRow++
			}
		}

		err = checkSQLKiller(sqlKiller, "killedDuringProbe")
		if err != nil {
			return err
		}

		a.finishCurrentLookupLoop(joinedChk)

		if joinedChk.NumRows() > 0 {
			a.selected, a.isNulls, err = expression.VecEvalBool(a.ctx.SessCtx.GetExprCtx().GetEvalCtx(), a.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, a.ctx.OtherCondition, joinedChk, a.selected, a.isNulls)
			if err != nil {
				return err
			}

			length := len(a.selected)
			for i := 0; i < length; i++ {
				if a.selected[i] {
					a.matchedProbeRowIdx[a.groupMark[i]] = struct{}{}
				}

				if a.isNulls[i] {
					a.matchedProbeRowIdx[a.groupMark[i]] = struct{}{}
				}
			}
		}
	}

	if cap(a.selected) < a.chunkRows {
		a.selected = make([]bool, a.chunkRows)
	} else {
		a.selected = a.selected[:a.chunkRows]
	}

	a.currentProbeRow = 0
	for ; a.currentProbeRow < a.chunkRows; a.currentProbeRow++ {
		_, ok := a.matchedProbeRowIdx[a.currentProbeRow]
		a.selected[a.currentProbeRow] = !ok
	}

	for index, usedColIdx := range a.lUsed {
		dstCol := chk.Column(index)
		srcCol := a.currentChunk.Column(usedColIdx)
		chunk.CopySelectedRows(dstCol, srcCol, a.selected)
	}
	return
}

func (a *antiSemiJoinProbe) probeForRightSideBuildNoOtherCondition(chk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := a.ctx.hashTableMeta
	tagHelper := a.ctx.hashTableContext.tagHelper
	matched := false

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
			if !matched {
				remainCap--
				a.matchedRowsForCurrentProbeRow = 1
				a.finishLookupCurrentProbeRow()
			}
			matched = false // reset
			a.currentProbeRow++
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}

	a.finishLookupCurrentProbeRow()
	generateResultChk(chk, a.currentChunk, a.lUsed, a.offsetAndLengthArray)
	return
}
