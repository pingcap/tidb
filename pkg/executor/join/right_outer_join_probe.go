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
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type rightOuterJoinProbe struct {
	baseOuterJoinProbe
}

func (j *rightOuterJoinProbe) SetChunkForProbe(chunk *chunk.Chunk) (err error) {
	return j.baseOuterJoinProbe.SetChunkForProbe(chunk)
}

func (j *rightOuterJoinProbe) NeedScanRowTable() bool {
	return j.baseOuterJoinProbe.NeedScanRowTable()
}

func (j *rightOuterJoinProbe) IsScanRowTableDone() bool {
	return j.baseOuterJoinProbe.IsScanRowTableDone()
}

func (j *rightOuterJoinProbe) InitForScanRowTable() {
	j.baseOuterJoinProbe.InitForScanRowTable()
}

func (j *rightOuterJoinProbe) ScanRowTable(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) *hashjoinWorkerResult {
	return j.baseOuterJoinProbe.ScanRowTable(joinResult, sqlKiller)
}

func (j *rightOuterJoinProbe) buildResultForMatchedRowsAfterOtherCondition(chk, joinedChk *chunk.Chunk) {
	j.baseOuterJoinProbe.buildResultForMatchedRowsAfterOtherCondition(chk, joinedChk, len(j.lUsed), j.rUsed, j.currentChunk.NumCols(), 0, j.lUsed, 0)
}

func (j *rightOuterJoinProbe) buildResultForNotMatchedRows(chk *chunk.Chunk, startProbeRow int) {
	j.baseOuterJoinProbe.buildResultForNotMatchedRows(chk, startProbeRow, len(j.lUsed), j.rUsed, 0, j.lUsed)
}

func (j *rightOuterJoinProbe) probeForLeftBuild(chk, joinedChk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := j.ctx.hashTableMeta
	startProbeRow := j.currentProbeRow
	hasOtherCondition := j.ctx.hasOtherCondition()

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			// hash value match
			candidateRow := *(*unsafe.Pointer)(unsafe.Pointer(&j.matchedRowsHeaders[j.currentProbeRow]))
			if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				// join key match
				j.appendBuildRowToCachedBuildRowsAndConstructBuildRowsIfNeeded(createMatchRowInfo(j.currentProbeRow, candidateRow), joinedChk, 0, hasOtherCondition)
				if !hasOtherCondition {
					// has no other condition, key match mean join match
					j.isNotMatchedRows[j.currentProbeRow] = false
				}
				j.matchedRowsForCurrentProbeRow++
			} else {
				if j.ctx.stats != nil {
					atomic.AddInt64(&j.ctx.stats.hashStat.probeCollision, 1)
				}
			}
			j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow)
		} else {
			// it could be
			// 1. no match when lookup the hash table
			// 2. filter by probeFilter
			j.finishLookupCurrentProbeRow()
			j.currentProbeRow++
		}
		remainCap--
	}

	err = checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		return err
	}

	j.finishCurrentLookupLoop(joinedChk)

	if hasOtherCondition {
		if joinedChk.NumRows() > 0 {
			j.selected = j.selected[:0]
			j.selected, err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx().GetEvalCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
			if err != nil {
				return err
			}
			j.buildResultForMatchedRowsAfterOtherCondition(chk, joinedChk)
		}
		// append the not matched rows
		j.buildResultForNotMatchedRows(chk, startProbeRow)
	} else {
		// if no the condition, chk == joinedChk, and the matched rows are already in joinedChk
		j.buildResultForNotMatchedRows(joinedChk, startProbeRow)
	}
	return
}

func (j *rightOuterJoinProbe) probeForRightBuild(chk, joinedChk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := j.ctx.hashTableMeta
	hasOtherCondition := j.ctx.hasOtherCondition()

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			// hash value match
			candidateRow := *(*unsafe.Pointer)(unsafe.Pointer(&j.matchedRowsHeaders[j.currentProbeRow]))
			if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				// join key match
				j.appendBuildRowToCachedBuildRowsAndConstructBuildRowsIfNeeded(createMatchRowInfo(j.currentProbeRow, candidateRow), joinedChk, 0, hasOtherCondition)
				if !hasOtherCondition {
					// has no other condition, key match means join match
					meta.setUsedFlag(candidateRow)
				}
				j.matchedRowsForCurrentProbeRow++
				remainCap--
			} else {
				if j.ctx.stats != nil {
					atomic.AddInt64(&j.ctx.stats.hashStat.probeCollision, 1)
				}
			}
			j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow)
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

	if j.ctx.hasOtherCondition() && joinedChk.NumRows() > 0 {
		j.selected, err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx().GetEvalCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
		if err != nil {
			return err
		}
		err = j.buildResultAfterOtherCondition(chk, joinedChk)
		for index, result := range j.selected {
			if result {
				meta.setUsedFlag(*(*unsafe.Pointer)(unsafe.Pointer(&j.rowIndexInfos[index].buildRowStart)))
			}
		}
	}
	return
}

func (j *rightOuterJoinProbe) Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, _ *hashjoinWorkerResult) {
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
	if j.rightAsBuildSide {
		err = j.probeForRightBuild(joinResult.chk, joinedChk, remainCap, sqlKiller)
	} else {
		err = j.probeForLeftBuild(joinResult.chk, joinedChk, remainCap, sqlKiller)
	}
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	return true, joinResult
}
