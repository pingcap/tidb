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

	// One probe row may match several build rows and generate result rows which consists of probe row and build row.
	// This struct records which rows are success.
	otherConditionSuccessSet map[int][]int
}

func newSemiJoinProbe(base baseJoinProbe, isLeftSideBuild bool) *semiJoinProbe {
	ret := &semiJoinProbe{
		baseSemiJoin:             *newBaseSemiJoin(base, isLeftSideBuild),
		otherConditionSuccessSet: make(map[int][]int),
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

	s.initProbe(chk.NumRows())
	return nil
}

func (s *semiJoinProbe) SetRestoredChunkForProbe(chk *chunk.Chunk) error {
	err := s.baseJoinProbe.SetRestoredChunkForProbe(chk)
	if err != nil {
		return err
	}

	s.initProbe(chk.NumRows())
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
			err = s.probeForRightSideBuildHasOtherCondition(joinResult.chk, joinedChk, sqlKiller)
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

func (s *semiJoinProbe) removeMatchedProbeRow() {
	for idx, selected := range s.selected {
		if selected {
			delete(s.undeterminedProbeRowsIdx, s.rowIndexInfos[idx].probeRowIndex)
		}
	}
}

func (s *semiJoinProbe) truncateSelect() {
	clear(s.otherConditionSuccessSet)

	for i, res := range s.selected {
		if !res {
			continue
		}

		groupID := s.rowIndexInfos[i].probeRowIndex
		s.otherConditionSuccessSet[groupID] = append(s.otherConditionSuccessSet[groupID], i)
	}

	for _, idxs := range s.otherConditionSuccessSet {
		idxsLen := len(idxs)
		if idxsLen <= 1 {
			continue
		}

		for i := 1; i < idxsLen; i++ {
			s.selected[idxs[i]] = false
		}
	}
}

func (s *semiJoinProbe) probeForLeftSideBuildHasOtherCondition(joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) (err error) {
	err = s.concatenateProbeAndBuildRows(joinedChk, sqlKiller, false)
	if err != nil {
		return err
	}

	if len(s.undeterminedProbeRowsIdx) == 0 {
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
			if isKeyMatched(meta.keyMode, s.serializedKeys[s.currentProbeRow], candidateRow, meta) {
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

func (s *semiJoinProbe) probeForRightSideBuildHasOtherCondition(chk, joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) (err error) {
	err = s.concatenateProbeAndBuildRows(joinedChk, sqlKiller, true)
	if err != nil {
		return err
	}

	defer func() {
		if len(s.undeterminedProbeRowsIdx) == 0 {
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

		s.truncateSelect()
		s.removeMatchedProbeRow()
		return s.buildResultAfterOtherCondition(chk, joinedChk)
	}
	return
}

func (s *semiJoinProbe) probeForRightSideBuildNoOtherCondition(chk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := s.ctx.hashTableMeta
	tagHelper := s.ctx.hashTableContext.tagHelper

	for remainCap > 0 && s.currentProbeRow < s.chunkRows {
		if s.matchedRowsHeaders[s.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(s.matchedRowsHeaders[s.currentProbeRow])
			if isKeyMatched(meta.keyMode, s.serializedKeys[s.currentProbeRow], candidateRow, meta) {
				s.matchedRowsHeaders[s.currentProbeRow] = 0
				s.matchedRowsForCurrentProbeRow++
				remainCap--
			} else {
				s.probeCollision++
				s.matchedRowsHeaders[s.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, s.matchedRowsHashValue[s.currentProbeRow])
			}
		} else {
			s.finishLookupCurrentProbeRow()
			s.currentProbeRow++
		}
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}

	s.finishLookupCurrentProbeRow()
	generateResultChk(chk, s.currentChunk, s.lUsed, s.offsetAndLengthArray)
	return
}

func generateResultChk(resultChk *chunk.Chunk, probeChk *chunk.Chunk, lUsed []int, offsetAndLengthArray []offsetAndLength) {
	for index, colIndex := range lUsed {
		srcCol := probeChk.Column(colIndex)
		dstCol := resultChk.Column(index)
		for _, offsetAndLength := range offsetAndLengthArray {
			dstCol.AppendCellNTimes(srcCol, offsetAndLength.offset, offsetAndLength.length)
		}
	}
}
