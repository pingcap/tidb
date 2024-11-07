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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/queue"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type leftOuterSemiJoinProbe struct {
	baseJoinProbe

	// isMatchedRows marks whether the left side row is matched
	isMatchedRows []bool
	// isNullRows marks whether the left side row matched result is null
	isNullRows []bool

	// buffer isNull for other condition evaluation
	isNulls []bool

	// used in other condition to record which rows need to be processed
	unFinishedProbeRowIdxQueue *queue.Queue[int]
}

var _ ProbeV2 = &leftOuterSemiJoinProbe{}

func newLeftOuterSemiJoinProbe(base baseJoinProbe) *leftOuterSemiJoinProbe {
	probe := &leftOuterSemiJoinProbe{
		baseJoinProbe: base,
	}
	if base.ctx.hasOtherCondition() {
		probe.unFinishedProbeRowIdxQueue = queue.NewQueue[int](32)
	}
	return probe
}

func (j *leftOuterSemiJoinProbe) SetChunkForProbe(chunk *chunk.Chunk) (err error) {
	err = j.baseJoinProbe.SetChunkForProbe(chunk)
	if err != nil {
		return err
	}
	j.resetProbeState()
	return nil
}

func (j *leftOuterSemiJoinProbe) SetRestoredChunkForProbe(chunk *chunk.Chunk) (err error) {
	err = j.baseJoinProbe.SetRestoredChunkForProbe(chunk)
	if err != nil {
		return err
	}
	j.resetProbeState()
	return nil
}

func (j *leftOuterSemiJoinProbe) resetProbeState() {
	j.isMatchedRows = j.isMatchedRows[:0]
	for i := 0; i < j.chunkRows; i++ {
		j.isMatchedRows = append(j.isMatchedRows, false)
	}
	j.isNullRows = j.isNullRows[:0]
	for i := 0; i < j.chunkRows; i++ {
		j.isNullRows = append(j.isNullRows, false)
	}
	if j.ctx.hasOtherCondition() {
		j.unFinishedProbeRowIdxQueue.Clear()
		for i := 0; i < j.chunkRows; i++ {
			if j.matchedRowsHeaders[i] != 0 {
				j.unFinishedProbeRowIdxQueue.Push(i)
			}
		}
	}
}

func (*leftOuterSemiJoinProbe) NeedScanRowTable() bool {
	return false
}

func (*leftOuterSemiJoinProbe) IsScanRowTableDone() bool {
	panic("should not reach here")
}

func (*leftOuterSemiJoinProbe) InitForScanRowTable() {
	panic("should not reach here")
}

func (*leftOuterSemiJoinProbe) ScanRowTable(joinResult *hashjoinWorkerResult, _ *sqlkiller.SQLKiller) *hashjoinWorkerResult {
	return joinResult
}

func (j *leftOuterSemiJoinProbe) Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, _ *hashjoinWorkerResult) {
	joinedChk, remainCap, err := j.prepareForProbe(joinResult.chk)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}

	if j.ctx.hasOtherCondition() {
		err = j.probeWithOtherCondition(joinResult.chk, joinedChk, remainCap, sqlKiller)
	} else {
		err = j.probeWithoutOtherCondition(joinResult.chk, joinedChk, remainCap, sqlKiller)
	}
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	return true, joinResult
}

func (j *leftOuterSemiJoinProbe) probeWithOtherCondition(chk, joinedChk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	if !j.unFinishedProbeRowIdxQueue.IsEmpty() {
		err = j.produceResult(joinedChk, sqlKiller)
		if err != nil {
			return err
		}
		j.currentProbeRow = 0
	}

	if j.unFinishedProbeRowIdxQueue.IsEmpty() {
		startProbeRow := j.currentProbeRow
		j.currentProbeRow = min(startProbeRow+remainCap, j.chunkRows)
		j.buildResult(chk, startProbeRow)
	}
	return
}

func (j *leftOuterSemiJoinProbe) produceResult(joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) (err error) {
	err = j.concatenateProbeAndBuildRows(joinedChk, sqlKiller)
	if err != nil {
		return err
	}

	if joinedChk.NumRows() > 0 {
		j.selected, j.isNulls, err = expression.VecEvalBool(j.ctx.SessCtx.GetExprCtx().GetEvalCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.OtherCondition, joinedChk, j.selected, j.isNulls)
		if err != nil {
			return err
		}

		for i := 0; i < joinedChk.NumRows(); i++ {
			if j.selected[i] {
				j.isMatchedRows[j.rowIndexInfos[i].probeRowIndex] = true
			}
			if j.isNulls[i] {
				j.isNullRows[j.rowIndexInfos[i].probeRowIndex] = true
			}
		}
	}
	return nil
}

func (j *leftOuterSemiJoinProbe) probeWithoutOtherCondition(_, joinedChk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := j.ctx.hashTableMeta
	startProbeRow := j.currentProbeRow
	tagHelper := j.ctx.hashTableContext.tagHelper

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(j.matchedRowsHeaders[j.currentProbeRow])
			if !isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				j.probeCollision++
				j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, j.matchedRowsHashValue[j.currentProbeRow])
				continue
			}
			j.isMatchedRows[j.currentProbeRow] = true
		}
		j.matchedRowsHeaders[j.currentProbeRow] = 0
		remainCap--
		j.currentProbeRow++
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")

	if err != nil {
		return err
	}

	j.buildResult(joinedChk, startProbeRow)
	return nil
}

func (j *leftOuterSemiJoinProbe) buildResult(chk *chunk.Chunk, startProbeRow int) {
	var selected []bool
	if startProbeRow == 0 && j.currentProbeRow == j.chunkRows && j.currentChunk.Sel() == nil && chk.NumRows() == 0 && len(j.spilledIdx) == 0 {
		// TODO: Can do a shallow copy by directly copying the Column pointers
		for index, colIndex := range j.lUsed {
			j.currentChunk.Column(colIndex).CopyConstruct(chk.Column(index))
		}
	} else {
		selected = make([]bool, j.chunkRows)
		for i := startProbeRow; i < j.currentProbeRow; i++ {
			selected[i] = true
		}
		for _, spilledIdx := range j.spilledIdx {
			selected[spilledIdx] = false // ignore spilled rows
		}
		for index, colIndex := range j.lUsed {
			dstCol := chk.Column(index)
			srcCol := j.currentChunk.Column(colIndex)
			chunk.CopySelectedRowsWithRowIDFunc(dstCol, srcCol, selected, 0, len(selected), func(i int) int {
				return j.usedRows[i]
			})
		}
	}

	for i := startProbeRow; i < j.currentProbeRow; i++ {
		if selected != nil && !selected[i] {
			continue
		}
		if j.isMatchedRows[i] {
			chk.AppendInt64(len(j.lUsed), 1)
		} else if j.isNullRows[i] {
			chk.AppendNull(len(j.lUsed))
		} else {
			chk.AppendInt64(len(j.lUsed), 0)
		}
	}
	chk.SetNumVirtualRows(chk.NumRows())
}

var maxMatchedRowNum = 4

func (j *leftOuterSemiJoinProbe) matchMultiBuildRows(joinedChk *chunk.Chunk, joinedChkRemainCap *int) {
	tagHelper := j.ctx.hashTableContext.tagHelper
	meta := j.ctx.hashTableMeta
	for j.matchedRowsHeaders[j.currentProbeRow] != 0 && *joinedChkRemainCap > 0 && j.matchedRowsForCurrentProbeRow < maxMatchedRowNum {
		candidateRow := tagHelper.toUnsafePointer(j.matchedRowsHeaders[j.currentProbeRow])
		if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
			j.appendBuildRowToCachedBuildRowsV1(j.currentProbeRow, candidateRow, joinedChk, 0, true)
			j.matchedRowsForCurrentProbeRow++
			*joinedChkRemainCap--
		} else {
			j.probeCollision++
		}
		j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, j.matchedRowsHashValue[j.currentProbeRow])
	}

	j.finishLookupCurrentProbeRow()
}

func (j *leftOuterSemiJoinProbe) concatenateProbeAndBuildRows(joinedChk *chunk.Chunk, sqlKiller *sqlkiller.SQLKiller) error {
	joinedChkRemainCap := joinedChk.Capacity() - joinedChk.NumRows()

	for joinedChkRemainCap > 0 && !j.unFinishedProbeRowIdxQueue.IsEmpty() {
		probeRowIdx := j.unFinishedProbeRowIdxQueue.Pop()
		if j.isMatchedRows[probeRowIdx] {
			continue
		}
		j.currentProbeRow = probeRowIdx
		j.matchMultiBuildRows(joinedChk, &joinedChkRemainCap)
		if j.matchedRowsHeaders[probeRowIdx] == 0 {
			continue
		}
		j.unFinishedProbeRowIdxQueue.Push(probeRowIdx)
	}

	err := checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}

	j.finishCurrentLookupLoop(joinedChk)
	return nil
}

func (j *leftOuterSemiJoinProbe) IsCurrentChunkProbeDone() bool {
	if j.ctx.hasOtherCondition() && !j.unFinishedProbeRowIdxQueue.IsEmpty() {
		return false
	}
	return j.baseJoinProbe.IsCurrentChunkProbeDone()
}
