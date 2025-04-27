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
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type leftOuterSemiJoinProbe struct {
	baseSemiJoin

	// isNullRows marks whether the left side row matched result is null
	isNullRows []bool
	// isAnti marks whether the join is anti semi join
	isAnti bool
}

var _ ProbeV2 = &leftOuterSemiJoinProbe{}

func newLeftOuterSemiJoinProbe(base baseJoinProbe, isAnti bool) *leftOuterSemiJoinProbe {
	probe := &leftOuterSemiJoinProbe{
		baseSemiJoin: *newBaseSemiJoin(base, false),
		isAnti:       isAnti,
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
	j.isNullRows = j.isNullRows[:0]
	for i := 0; i < j.chunkRows; i++ {
		j.isNullRows = append(j.isNullRows, false)
	}
	j.baseSemiJoin.resetProbeState()
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
	// The third parameter is always true, as left outer semi join now only support right side as build
	err = j.concatenateProbeAndBuildRows(joinedChk, sqlKiller, true)
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

	if j.isAnti {
		for i := startProbeRow; i < j.currentProbeRow; i++ {
			if selected != nil && !selected[i] {
				continue
			}
			if j.isMatchedRows[i] {
				chk.AppendInt64(len(j.lUsed), 0)
			} else if j.isNullRows[i] {
				chk.AppendNull(len(j.lUsed))
			} else {
				chk.AppendInt64(len(j.lUsed), 1)
			}
		}
	} else {
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
	}

	chk.SetNumVirtualRows(chk.NumRows())
}

func (j *leftOuterSemiJoinProbe) IsCurrentChunkProbeDone() bool {
	if j.ctx.hasOtherCondition() && !j.unFinishedProbeRowIdxQueue.IsEmpty() {
		return false
	}
	return j.baseJoinProbe.IsCurrentChunkProbeDone()
}
