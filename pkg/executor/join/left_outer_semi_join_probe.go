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
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type leftOuterSemiJoinProbe struct {
	baseJoinProbe
	// used when use inner side to build, isNotMatchedRows is indexed by logical row index
	isNotMatchedRows []bool
	// build/probe side used columns and offset in result chunk
	buildColUsed              []int
	buildColOffsetInResultChk int
	probeColUsed              []int
	probeColOffsetInResultChk int
}

var _ ProbeV2 = &leftOuterSemiJoinProbe{}

func newLeftOuterSemiJoinProbe(base baseJoinProbe) *leftOuterSemiJoinProbe {
	probe := &leftOuterSemiJoinProbe{
		baseJoinProbe: base,
	}
	probe.buildColUsed = base.lUsed
	probe.buildColOffsetInResultChk = 0
	probe.probeColUsed = base.rUsed
	probe.probeColOffsetInResultChk = len(base.lUsed)
	return probe
}

func (j *leftOuterSemiJoinProbe) SetChunkForProbe(chunk *chunk.Chunk) (err error) {
	err = j.baseJoinProbe.SetChunkForProbe(chunk)
	if err != nil {
		return err
	}
	j.isNotMatchedRows = j.isNotMatchedRows[:0]
	for i := 0; i < j.chunkRows; i++ {
		j.isNotMatchedRows = append(j.isNotMatchedRows, true)
	}
	return nil
}

func (j *leftOuterSemiJoinProbe) NeedScanRowTable() bool {
	return false
}

func (j *leftOuterSemiJoinProbe) IsScanRowTableDone() bool {
	panic("should not reach here")
}

func (j *leftOuterSemiJoinProbe) InitForScanRowTable() {
	panic("should not reach here")
}

func (j *leftOuterSemiJoinProbe) ClearProbeState() {
	j.baseJoinProbe.ClearProbeState()
}

func (j *leftOuterSemiJoinProbe) ScanRowTable(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) *hashjoinWorkerResult {
	return joinResult
}

func (j *leftOuterSemiJoinProbe) Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, _ *hashjoinWorkerResult) {
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

	err = j.probeForInnerSideBuild(joinResult.chk, joinedChk, remainCap, sqlKiller)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	return true, joinResult
}

func (j *leftOuterSemiJoinProbe) probeForInnerSideBuild(chk, joinedChk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := j.ctx.hashTableMeta
	startProbeRow := j.currentProbeRow
	hasOtherCondition := j.ctx.hasOtherCondition()
	tagHelper := j.ctx.hashTableContext.tagHelper

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			// hash value match
			candidateRow := tagHelper.toUnsafePointer(j.matchedRowsHeaders[j.currentProbeRow])
			if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				if hasOtherCondition { // check othercond
					j.isNotMatchedRows[j.currentProbeRow] = false
					j.matchedRowsHeaders[j.currentProbeRow] = 0
				}
				j.matchedRowsForCurrentProbeRow++
			} else {
				j.probeCollision++
			}
			j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow, tagHelper, j.matchedRowsHashValue[j.currentProbeRow])
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
	return
}
