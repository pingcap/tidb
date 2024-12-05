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

type semiJoinProbe struct {
	baseSemiJoin

	// Used for right side build without other condition
	offsets []int
}

func newSemiJoinProbe(base baseJoinProbe, isLeftSideBuild bool) *semiJoinProbe {
	ret := &semiJoinProbe{
		baseSemiJoin: *newBaseSemiJoin(base, isLeftSideBuild),
	}
	return ret
}

func (s *semiJoinProbe) InitForScanRowTable() {
	if !s.isLeftSideBuild {
		panic("should not reach here")
	}
	s.rowIter = commonInitForScanRowTable(&s.baseJoinProbe)
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

	if s.unFinishedProbeRowIdxQueue.IsEmpty() {
		// To avoid `Previous chunk is not probed yet` error
		s.currentProbeRow = s.chunkRows
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
			if !meta.isCurrentRowUsedWithAtomic(candidateRow) {
				if isKeyMatched(meta.keyMode, s.serializedKeys[s.currentProbeRow], candidateRow, meta) {
					meta.setUsedFlag(candidateRow)
				} else {
					s.probeCollision++
				}
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

func (s *semiJoinProbe) produceResult(joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) (err error) {
	err = s.concatenateProbeAndBuildRows(joinedChk, sqlKiller, true)
	if err != nil {
		return err
	}

	if joinedChk.NumRows() > 0 {
		s.selected = s.selected[:0]
		s.selected, err = expression.VectorizedFilter(s.ctx.SessCtx.GetExprCtx().GetEvalCtx(), s.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, s.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), s.selected)
		if err != nil {
			return err
		}

		s.setIsMatchedRows()
	}
	return nil
}

func (s *semiJoinProbe) probeForRightSideBuildHasOtherCondition(chk, joinedChk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	if !s.unFinishedProbeRowIdxQueue.IsEmpty() {
		err = s.produceResult(joinedChk, sqlKiller)
		if err != nil {
			return err
		}
		s.currentProbeRow = 0
	}

	if s.unFinishedProbeRowIdxQueue.IsEmpty() {
		for remainCap > 0 && (s.currentProbeRow < s.chunkRows) {
			rowNumToTryAppend := min(remainCap, s.chunkRows-s.currentProbeRow)
			start := s.currentProbeRow
			end := s.currentProbeRow + rowNumToTryAppend

			for index, usedColIdx := range s.lUsed {
				dstCol := chk.Column(index)
				srcCol := s.currentChunk.Column(usedColIdx)
				chunk.CopySelectedRowsWithRowIDFunc(dstCol, srcCol, s.isMatchedRows, start, end, func(i int) int {
					return s.usedRows[i]
				})
			}

			if len(s.lUsed) == 0 {
				// For calculating virtual row num
				virtualRowNum := chk.GetNumVirtualRows()
				for i := start; i < end; i++ {
					if s.isMatchedRows[i] {
						virtualRowNum++
					}
				}

				// When `len(s.lUsed) == 0`, column number in chk is 0
				// We need to manually calculate virtual row number.
				chk.SetNumVirtualRows(virtualRowNum)
			} else {
				chk.SetNumVirtualRows(chk.NumRows())
			}

			s.currentProbeRow += rowNumToTryAppend
			remainCap = chk.RequiredRows() - chk.NumRows()
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

	for remainCap > 0 && s.currentProbeRow < s.chunkRows {
		if s.matchedRowsHeaders[s.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(s.matchedRowsHeaders[s.currentProbeRow])
			if isKeyMatched(meta.keyMode, s.serializedKeys[s.currentProbeRow], candidateRow, meta) {
				s.matchedRowsHeaders[s.currentProbeRow] = 0
				s.offsets = append(s.offsets, s.usedRows[s.currentProbeRow])
				remainCap--
				s.currentProbeRow++
			} else {
				s.probeCollision++
				s.matchedRowsHeaders[s.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, s.matchedRowsHashValue[s.currentProbeRow])
			}
		} else {
			s.currentProbeRow++
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}

	s.generateResultChkForRightBuildNoOtherCondition(chk)
	return
}

func (s *semiJoinProbe) generateResultChkForRightBuildNoOtherCondition(resultChk *chunk.Chunk) {
	if len(s.offsets) == 0 {
		return
	}

	for index, colIndex := range s.lUsed {
		srcCol := s.currentChunk.Column(colIndex)
		dstCol := resultChk.Column(index)
		chunk.CopyRows(dstCol, srcCol, s.offsets)
	}

	if len(s.lUsed) == 0 {
		resultChk.SetNumVirtualRows(resultChk.NumRows() + len(s.offsets))
	} else {
		resultChk.SetNumVirtualRows(resultChk.NumRows())
	}
}

func (s *semiJoinProbe) IsCurrentChunkProbeDone() bool {
	if s.ctx.hasOtherCondition() && !s.unFinishedProbeRowIdxQueue.IsEmpty() {
		return false
	}
	return s.baseJoinProbe.IsCurrentChunkProbeDone()
}
