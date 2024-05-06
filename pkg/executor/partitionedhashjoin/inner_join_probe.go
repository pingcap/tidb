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

package partitionedhashjoin

import (
	"github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"sync/atomic"
)

type innerJoinProbe struct {
	baseJoinProbe
}

func (j *innerJoinProbe) Probe(joinResult *util.HashjoinWorkerResult, sqlKiller sqlkiller.SQLKiller) (ok bool, _ *util.HashjoinWorkerResult) {
	if joinResult.Chk.IsFull() {
		return true, joinResult
	}
	hasOtherCondition := j.ctx.hasOtherCondition()
	joinedChk, remainCap, err := j.prepareForProbe(joinResult.Chk)
	if err != nil {
		joinResult.Err = err
		return false, joinResult
	}
	meta := j.ctx.hashTableMeta

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			candidateRow := j.matchedRowsHeaders[j.currentProbeRow]
			if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				// key matched, convert row to column for build side
				j.appendBuildRowToCachedBuildRowsAndConstructBuildRowsIfNeeded(rowIndexInfo{probeRowIndex: j.currentProbeRow, buildRowStart: candidateRow}, joinedChk, 0, hasOtherCondition)
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

	err = j.checkSqlKiller(sqlKiller)
	if err != nil {
		joinResult.Err = err
		return false, joinResult
	}

	j.finishCurrentLookupLoop(joinedChk)

	if j.ctx.hasOtherCondition() && joinedChk.NumRows() > 0 {
		// eval other condition, and construct final chunk
		j.selected = j.selected[:0]
		j.selected, joinResult.Err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx().GetEvalCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
		if joinResult.Err != nil {
			return false, joinResult
		}
		joinResult.Err = j.buildResultAfterOtherCondition(joinResult.Chk, joinedChk)
	}
	// if there is no other condition, the joinedChk is the final result
	if joinResult.Err != nil {
		return false, joinResult
	}
	return true, joinResult
}

func (j *innerJoinProbe) NeedScanRowTable() bool {
	return false
}

func (j *innerJoinProbe) ScanRowTable(*util.HashjoinWorkerResult) *util.HashjoinWorkerResult {
	panic("should not reach here")
}

func (j *innerJoinProbe) InitForScanRowTable() {
	panic("should not reach here")
}

func (j *innerJoinProbe) IsScanRowTableDone() bool {
	panic("should not reach here")
}
