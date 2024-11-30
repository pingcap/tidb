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

type innerJoinProbe struct {
	baseJoinProbe
}

func (j *innerJoinProbe) Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, _ *hashjoinWorkerResult) {
	if joinResult.chk.IsFull() {
		return true, joinResult
	}
	hasOtherCondition := j.ctx.hasOtherCondition()
	joinedChk, remainCap, err := j.prepareForProbe(joinResult.chk)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	meta := j.ctx.hashTableMeta
	isInCompleteChunk := joinedChk.IsInCompleteChunk()
	// in case that virtual rows is not maintained correctly
	joinedChk.SetNumVirtualRows(joinedChk.NumRows())
	// always set in complete chunk during probe
	joinedChk.SetInCompleteChunk(true)
	defer joinedChk.SetInCompleteChunk(isInCompleteChunk)

	tagHelper := j.ctx.hashTableContext.tagHelper
	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			candidateRow := tagHelper.toUnsafePointer(j.matchedRowsHeaders[j.currentProbeRow])
			if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				// key matched, convert row to column for build side
				j.appendBuildRowToCachedBuildRowsV1(j.currentProbeRow, candidateRow, joinedChk, 0, hasOtherCondition)
				j.matchedRowsForCurrentProbeRow++
				remainCap--
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
		joinResult.err = err
		return false, joinResult
	}

	j.finishCurrentLookupLoop(joinedChk)

	if j.ctx.hasOtherCondition() && joinedChk.NumRows() > 0 {
		// eval other condition, and construct final chunk
		j.selected = j.selected[:0]
		j.selected, joinResult.err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx().GetEvalCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
		if joinResult.err != nil {
			return false, joinResult
		}
		joinResult.err = j.buildResultAfterOtherCondition(joinResult.chk, joinedChk)
	}
	// if there is no other condition, the joinedChk is the final result
	if joinResult.err != nil {
		return false, joinResult
	}
	return true, joinResult
}

func (*innerJoinProbe) NeedScanRowTable() bool {
	return false
}

func (*innerJoinProbe) ScanRowTable(*hashjoinWorkerResult, *sqlkiller.SQLKiller) *hashjoinWorkerResult {
	panic("should not reach here")
}

func (*innerJoinProbe) InitForScanRowTable() {
	panic("should not reach here")
}

func (*innerJoinProbe) IsScanRowTableDone() bool {
	panic("should not reach here")
}
