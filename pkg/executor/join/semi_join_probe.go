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
	"math"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type semiJoinProbe struct {
	baseSemiJoin

	// Used for right side build with other condition
	offset int

	// Used for right side build without other condition
	offsets []int
}

func newSemiJoinProbe(base baseJoinProbe, isLeftSideBuild bool) *semiJoinProbe {
	ret := &semiJoinProbe{
		baseSemiJoin: *newBaseSemiJoin(base, isLeftSideBuild),
		offset:       0,
	}
	return ret
}

func (s *semiJoinProbe) InitForScanRowTable() {
	if !s.isLeftSideBuild {
		panic("should not reach here")
	}
	s.rowIter = commonInitForScanRowTable(&s.baseJoinProbe)
}

func (s *semiJoinProbe) resetProbeState() {
	s.offset = 0
	s.baseSemiJoin.resetProbeState()
}

func (s *semiJoinProbe) SetChunkForProbe(chk *chunk.Chunk) (err error) {
	err = s.baseJoinProbe.SetChunkForProbe(chk)
	if err != nil {
		return err
	}

	s.resetProbeState()
	return nil
}

func (s *semiJoinProbe) SetRestoredChunkForProbe(chk *chunk.Chunk) error {
	err := s.baseJoinProbe.SetRestoredChunkForProbe(chk)
	if err != nil {
		return err
	}

	s.resetProbeState()
	return nil
}

func (s *semiJoinProbe) NeedScanRowTable() bool {
	return s.isLeftSideBuild
}

func (s *semiJoinProbe) IsScanRowTableDone() bool {
	if !s.isLeftSideBuild {
		panic("should not reach here")
	}
	return s.rowIter.isEnd()
}

func (s *semiJoinProbe) ScanRowTable(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) *hashjoinWorkerResult {
	if !s.isLeftSideBuild {
		panic("should not reach here")
	}
	if joinResult.chk.IsFull() {
		return joinResult
	}
	if s.rowIter == nil {
		panic("scanRowTable before init")
	}
	s.nextCachedBuildRowIndex = 0
	meta := s.ctx.hashTableMeta
	insertedRows := 0
	remainCap := joinResult.chk.RequiredRows() - joinResult.chk.NumRows()
	for insertedRows < remainCap && !s.rowIter.isEnd() {
		currentRow := s.rowIter.getValue()
		if meta.isCurrentRowUsed(currentRow) {
			// append build side of this row
			s.appendBuildRowToCachedBuildRowsV1(0, currentRow, joinResult.chk, 0, false)
			insertedRows++
		}
		s.rowIter.next()
	}
	err := checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		joinResult.err = err
		return joinResult
	}
	if s.nextCachedBuildRowIndex > 0 {
		s.batchConstructBuildRows(joinResult.chk, 0, false)
	}
	return joinResult
}

func (s *semiJoinProbe) ResetProbe() {
	s.rowIter = nil
	s.baseJoinProbe.ResetProbe()
}

func (s *semiJoinProbe) Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, _ *hashjoinWorkerResult) {
	if joinResult.chk.IsFull() {
		return true, joinResult
	}

	joinedChk, remainCap, err := s.prepareForProbe(joinResult.chk)
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

	hasOtherCondition := s.ctx.hasOtherCondition()
	if s.isLeftSideBuild {
		if hasOtherCondition {
			err = s.probeForLeftSideBuildHasOtherCondition(joinedChk, sqlKiller)
		} else {
			err = s.probeForLeftSideBuildNoOtherCondition(sqlKiller)
		}
	} else {
		if hasOtherCondition {
			err = s.probeForRightSideBuildHasOtherCondition(joinResult.chk, joinedChk, remainCap, sqlKiller)
		} else {
			err = s.probeForRightSideBuildNoOtherCondition(joinResult.chk, remainCap, sqlKiller)
		}
	}
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	return true, joinResult
}

func (s *semiJoinProbe) setIsMatchedRows() {
	for i, res := range s.selected {
		if !res {
			continue
		}

		s.isMatchedRows[s.rowIndexInfos[i].probeRowIndex] = true
	}
}

func (s *semiJoinProbe) probeForLeftSideBuildHasOtherCondition(joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) (err error) {
	err = s.concatenateProbeAndBuildRows(joinedChk, sqlKiller, false)
	if err != nil {
		return err
	}

	if s.unFinishedProbeRowIdxQueue.IsEmpty() {
		// To avoid `Previous chunk is not probed yet` error
		s.currentProbeRow = s.chunkRows
	}

	meta := s.ctx.hashTableMeta
	if joinedChk.NumRows() > 0 {
		s.selected, err = expression.VectorizedFilter(s.ctx.SessCtx.GetExprCtx().GetEvalCtx(), s.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, s.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), s.selected)
		if err != nil {
			return err
		}

		for index, result := range s.selected {
			if result {
				meta.setUsedFlag(*(*unsafe.Pointer)(unsafe.Pointer(&s.rowIndexInfos[index].buildRowStart)))
			}
		}
	}

	return
}

func (s *semiJoinProbe) probeForLeftSideBuildNoOtherCondition(sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := s.ctx.hashTableMeta
	tagHelper := s.ctx.hashTableContext.tagHelper

	loopCnt := 0

	for s.currentProbeRow < s.chunkRows {
		if s.matchedRowsHeaders[s.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(s.matchedRowsHeaders[s.currentProbeRow])
			if !meta.isCurrentRowUsedWithAtomic(candidateRow) && isKeyMatched(meta.keyMode, s.serializedKeys[s.currentProbeRow], candidateRow, meta) {
				meta.setUsedFlag(candidateRow)
			} else {
				s.probeCollision++
			}
			s.matchedRowsHeaders[s.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, s.matchedRowsHashValue[s.currentProbeRow])
		} else {
			s.currentProbeRow++
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
	if err != nil {
		return err
	}

	return
}

func (s *semiJoinProbe) probeForRightSideBuildHasOtherCondition(chk, joinedChk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	err = s.concatenateProbeAndBuildRows(joinedChk, sqlKiller, true)
	if err != nil {
		return err
	}

	defer func() {
		if s.unFinishedProbeRowIdxQueue.IsEmpty() && s.offset == s.chunkRows {
			// To avoid `Previous chunk is not probed yet` error
			s.currentProbeRow = s.chunkRows
		}
	}()

	if joinedChk.NumRows() > 0 {
		s.selected = s.selected[:0]
		s.selected, err = expression.VectorizedFilter(s.ctx.SessCtx.GetExprCtx().GetEvalCtx(), s.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, s.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), s.selected)
		if err != nil {
			return err
		}

		s.setIsMatchedRows()
	}

	if s.unFinishedProbeRowIdxQueue.IsEmpty() {
		for remainCap > 0 && (s.offset < s.chunkRows) {
			rowNumToAppend := int(math.Min(float64(remainCap), float64(s.chunkRows-s.offset)))

			for index, usedColIdx := range s.lUsed {
				dstCol := chk.Column(index)
				srcCol := s.currentChunk.Column(usedColIdx)
				chunk.CopySelectedRowsWithRowIDFunc(dstCol, srcCol, s.isMatchedRows, s.offset, s.offset+rowNumToAppend, func(i int) int {
					return i
				})
			}

			s.offset += rowNumToAppend
			remainCap -= rowNumToAppend
		}
	}
	return
}

func (s *semiJoinProbe) probeForRightSideBuildNoOtherCondition(chk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := s.ctx.hashTableMeta
	tagHelper := s.ctx.hashTableContext.tagHelper

	if cap(s.offsets) == 0 {
		s.offsets = make([]int, 0, remainCap)
	}

	s.offsets = s.offsets[:0]
	matched := false

	for remainCap > 0 && s.currentProbeRow < s.chunkRows {
		if s.matchedRowsHeaders[s.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(s.matchedRowsHeaders[s.currentProbeRow])
			if isKeyMatched(meta.keyMode, s.serializedKeys[s.currentProbeRow], candidateRow, meta) {
				s.matchedRowsHeaders[s.currentProbeRow] = 0
				matched = true
				remainCap--
			} else {
				s.probeCollision++
				s.matchedRowsHeaders[s.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, s.matchedRowsHashValue[s.currentProbeRow])
			}
		} else {
			if matched {
				s.offsets = append(s.offsets, s.usedRows[s.currentProbeRow])
				matched = false
			}
			s.currentProbeRow++
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}

	if s.currentProbeRow < s.chunkRows && matched {
		s.offsets = append(s.offsets, s.usedRows[s.currentProbeRow])
	}
	generateResultChk(chk, s.currentChunk, s.lUsed, s.offsets)
	return
}

func generateResultChk(resultChk *chunk.Chunk, probeChk *chunk.Chunk, lUsed []int, offsets []int) {
	for index, colIndex := range lUsed {
		srcCol := probeChk.Column(colIndex)
		dstCol := resultChk.Column(index)
		for _, offset := range offsets {
			dstCol.AppendCellNTimes(srcCol, offset, 1)
		}
	}
}
