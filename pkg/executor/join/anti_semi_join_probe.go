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
}

func newAntiSemiJoinProbe(base baseJoinProbe, isLeftSideBuild bool) *antiSemiJoinProbe {
	ret := &antiSemiJoinProbe{
		baseJoinProbe:   base,
		isLeftSideBuild: isLeftSideBuild,
	}

	if ret.isLeftSideBuild && ret.ctx.hasOtherCondition() {
		ret.groupMark = make([]int, 0, ret.ctx.SessCtx.GetSessionVars().MaxChunkSize)
		ret.matchedProbeRowIdx = make(map[int]struct{})
	}
	return ret
}

func (j *antiSemiJoinProbe) InitForScanRowTable() {
	if !j.isLeftSideBuild {
		panic("should not reach here")
	}
	totalRowCount := j.ctx.hashTableContext.hashTable.totalRowCount()
	concurrency := j.ctx.Concurrency
	workID := uint64(j.workID)
	avgRowPerWorker := totalRowCount / uint64(concurrency)
	startIndex := workID * avgRowPerWorker
	endIndex := (workID + 1) * avgRowPerWorker
	if workID == uint64(concurrency-1) {
		endIndex = totalRowCount
	}
	if endIndex > totalRowCount {
		endIndex = totalRowCount
	}
	j.rowIter = j.ctx.hashTableContext.hashTable.createRowIter(startIndex, endIndex)
}

func (j *antiSemiJoinProbe) NeedScanRowTable() bool {
	return j.isLeftSideBuild
}

func (j *antiSemiJoinProbe) IsScanRowTableDone() bool {
	if !j.isLeftSideBuild {
		panic("should not reach here")
	}
	return j.rowIter.isEnd()
}

func (j *antiSemiJoinProbe) ScanRowTable(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) *hashjoinWorkerResult {
	if !j.isLeftSideBuild {
		panic("should not reach here")
	}
	if joinResult.chk.IsFull() {
		return joinResult
	}
	if j.rowIter == nil {
		panic("scanRowTable before init")
	}
	j.nextCachedBuildRowIndex = 0
	meta := j.ctx.hashTableMeta
	insertedRows := 0
	remainCap := joinResult.chk.RequiredRows() - joinResult.chk.NumRows()
	for insertedRows < remainCap && !j.rowIter.isEnd() {
		currentRow := j.rowIter.getValue()
		if !meta.isCurrentRowUsed(currentRow) {
			// append build side of this row
			j.appendBuildRowToCachedBuildRowsV1(0, currentRow, joinResult.chk, 0, false)
			insertedRows++
		}
		j.rowIter.next()
	}
	err := checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		joinResult.err = err
		return joinResult
	}
	if j.nextCachedBuildRowIndex > 0 {
		j.batchConstructBuildRows(joinResult.chk, 0, false)
	}
	return joinResult
}

func (j *antiSemiJoinProbe) ResetProbe() {
	j.rowIter = nil
	j.baseJoinProbe.ResetProbe()
}

func (j *antiSemiJoinProbe) Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, _ *hashjoinWorkerResult) {
	if joinResult.chk.IsFull() {
		return true, joinResult
	}

	joinedChk, remainCap, err := j.prepareForProbe(joinResult.chk)
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

	hasOtherCondition := j.ctx.hasOtherCondition()
	if j.isLeftSideBuild {
		if hasOtherCondition {
			err = j.probeForLeftSideBuildHasOtherCondition(joinedChk, sqlKiller)
		} else {
			err = j.probeForLeftSideBuildNoOtherCondition(sqlKiller)
		}
	} else {
		if hasOtherCondition {
			j.groupMark = j.groupMark[:0]
			clear(j.matchedProbeRowIdx)
			err = j.probeForRightSideBuildHasOtherCondition(joinResult.chk, joinedChk, sqlKiller)
		} else {
			err = j.probeForRightSideBuildNoOtherCondition(joinResult.chk, remainCap, sqlKiller)
		}
	}
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	return true, joinResult
}

func (j *antiSemiJoinProbe) probeForLeftSideBuildHasOtherCondition(joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := j.ctx.hashTableMeta
	tagHelper := j.ctx.hashTableContext.tagHelper
	remainCap := joinedChk.Capacity()

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(j.matchedRowsHeaders[j.currentProbeRow])
			if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				j.matchedRowsForCurrentProbeRow++
			} else {
				j.probeCollision++
			}
			j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, j.matchedRowsHashValue[j.currentProbeRow])
		} else {
			remainCap--
			j.currentProbeRow++
			j.finishLookupCurrentProbeRow()
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}
	j.finishCurrentLookupLoop(joinedChk)

	if joinedChk.NumRows() > 0 {
		j.selected, err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx().GetEvalCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
		if err != nil {
			return err
		}

		for index, result := range j.selected {
			if result {
				meta.setUsedFlag(*(*unsafe.Pointer)(unsafe.Pointer(&j.rowIndexInfos[index].buildRowStart)))
			}
		}
	}

	return
}

func (j *antiSemiJoinProbe) probeForLeftSideBuildNoOtherCondition(sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := j.ctx.hashTableMeta
	tagHelper := j.ctx.hashTableContext.tagHelper

	for j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(j.matchedRowsHeaders[j.currentProbeRow])
			if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				meta.setUsedFlag(candidateRow)
				j.matchedRowsHeaders[j.currentProbeRow] = 0
			} else {
				j.probeCollision++
				j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, j.matchedRowsHashValue[j.currentProbeRow])
			}
		} else {
			j.currentProbeRow++
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	return err
}

func (j *antiSemiJoinProbe) probeForRightSideBuildHasOtherCondition(chk, joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := j.ctx.hashTableMeta
	tagHelper := j.ctx.hashTableContext.tagHelper
	for j.currentProbeRow < j.chunkRows {
		joinedChk.Reset()
		remainCap := joinedChk.Capacity()
		for remainCap > 0 && j.currentProbeRow < j.chunkRows {
			if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
				candidateRow := tagHelper.toUnsafePointer(j.matchedRowsHeaders[j.currentProbeRow])
				if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
					remainCap--
					j.matchedRowsForCurrentProbeRow++
					j.groupMark = append(j.groupMark, j.currentProbeRow)
				} else {
					j.probeCollision++
				}
				j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, j.matchedRowsHashValue[j.currentProbeRow])
			} else {
				j.finishLookupCurrentProbeRow()
				j.currentProbeRow++
			}
		}

		err = checkSQLKiller(sqlKiller, "killedDuringProbe")
		if err != nil {
			return err
		}

		j.finishCurrentLookupLoop(joinedChk)

		if joinedChk.NumRows() > 0 {
			j.selected = j.selected[:0]
			j.selected, err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx().GetEvalCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
			if err != nil {
				return err
			}

			length := len(j.selected)
			for i := 0; i < length; i++ {
				if j.selected[i] {
					j.matchedProbeRowIdx[j.groupMark[i]] = struct{}{}
				}
			}
		}
	}

	j.currentProbeRow = 0
	for j.currentProbeRow < j.chunkRows {
		_, ok := j.matchedProbeRowIdx[j.currentProbeRow]
		if ok {
			continue
		}

		j.matchedRowsForCurrentProbeRow = 1
		j.finishLookupCurrentProbeRow()
	}
	j.finishCurrentLookupLoop(chk)
	return
}

func (j *antiSemiJoinProbe) probeForRightSideBuildNoOtherCondition(chk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := j.ctx.hashTableMeta
	tagHelper := j.ctx.hashTableContext.tagHelper
	matched := false

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(j.matchedRowsHeaders[j.currentProbeRow])
			if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				matched = true
				j.matchedRowsHeaders[j.currentProbeRow] = 0
			} else {
				j.probeCollision++
				j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, j.matchedRowsHashValue[j.currentProbeRow])
			}
		} else {
			if !matched {
				remainCap--
				j.matchedRowsForCurrentProbeRow++
				j.finishLookupCurrentProbeRow()
			}
			matched = false // reset
			j.currentProbeRow++
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}

	j.finishCurrentLookupLoop(chk)
	return
}
