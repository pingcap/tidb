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
)

type innerJoinProbe struct {
	baseJoinProbe
}

func (j *innerJoinProbe) Probe(joinResult *util.HashjoinWorkerResult) (ok bool, _ *util.HashjoinWorkerResult) {
	if joinResult.Chk.IsFull() {
		return true, joinResult
	}
	joinedChk, remainCap, err := j.prepareForProbe(joinResult.Chk)
	if err != nil {
		joinResult.Err = err
		return false, joinResult
	}
	length := 0
	meta := j.ctx.hashTableMeta

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != nil {
			candidateRow := j.matchedRowsHeaders[j.currentProbeRow]
			if isKeyMatched(j.ctx.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				// key matched, convert row to column for build side
				rowInfo := &rowInfo{rowStart: candidateRow, rowData: nil, currentColumnIndex: 0}
				currentRowData := j.appendBuildRowToChunk(joinedChk, rowInfo)
				if j.ctx.hasOtherCondition() {
					j.rowIndexInfos = append(j.rowIndexInfos, rowIndexInfo{probeRowIndex: j.currentProbeRow, buildRowStart: candidateRow, buildRowData: currentRowData})
				}
				length++
				remainCap--
				joinedChk.IncNumVirtualRows()
			}
			j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow)
		} else {
			j.appendOffsetAndLength(j.currentProbeRow, length)
			length = 0
			j.currentProbeRow++
		}
	}

	j.appendOffsetAndLength(j.currentProbeRow, length)
	j.appendProbeRowToChunk(joinedChk, j.currentChunk)

	if j.ctx.hasOtherCondition() && joinedChk.NumRows() > 0 {
		// eval other condition, and construct final chunk
		j.selected = j.selected[:0]
		j.selected, joinResult.Err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
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

func (j *innerJoinProbe) IsScanRowTableDone() bool {
	panic("should not reach here")
}

func (j *innerJoinProbe) buildResultAfterOtherCondition(chk *chunk.Chunk, joinedChk *chunk.Chunk) (err error) {
	// construct the return chunk based on joinedChk and selected, there are 3 kinds of columns
	// 1. columns already in joinedChk
	// 2. columns from build side, but not in joinedChk
	// 3. columns from probe side, but not in joinedChk
	probeUsedColumns, probeColOffset, probeColOffsetInJoinedChk := j.lUsed, 0, 0
	if !j.rightAsBuildSide {
		probeUsedColumns, probeColOffset, probeColOffsetInJoinedChk = j.rUsed, len(j.lUsed), j.ctx.hashTableMeta.totalColumnNumber
	}

	for index, colIndex := range probeUsedColumns {
		dstCol := chk.Column(index + probeColOffset)
		if joinedChk.Column(colIndex+probeColOffsetInJoinedChk).Rows() > 0 {
			// probe column that is already in joinedChk
			srcCol := joinedChk.Column(colIndex + probeColOffsetInJoinedChk)
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			// probe column that is not in joinedChk
			srcCol := j.currentChunk.Column(colIndex)
			chunk.CopySelectedRowsWithRowIdFunc(dstCol, srcCol, j.selected, 0, len(j.selected), func(i int) int {
				return j.rowIndexInfos[i].probeRowIndex
			})
		}
	}
	buildUsedColumns, buildColOffset, buildColOffsetInJoinedChk := j.rUsed, len(j.lUsed), j.currentChunk.NumCols()
	if !j.rightAsBuildSide {
		buildUsedColumns, buildColOffset, buildColOffsetInJoinedChk = j.lUsed, 0, 0
	}
	hasRemainCols := false
	for index, colIndex := range buildUsedColumns {
		dstCol := chk.Column(index + buildColOffset)
		srcCol := joinedChk.Column(colIndex + buildColOffsetInJoinedChk)
		if srcCol.Rows() > 0 {
			// build column that is already in joinedChk
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			hasRemainCols = true
		}
	}
	if hasRemainCols {
		// build column that is not in joinedChk
		for index, result := range j.selected {
			if result {
				j.appendBuildRowToChunk(chk, &rowInfo{
					rowStart:           j.rowIndexInfos[index].buildRowStart,
					rowData:            j.rowIndexInfos[index].buildRowData,
					currentColumnIndex: j.ctx.hashTableMeta.columnCountNeededForOtherCondition,
				})
			}
		}
	}
	return
}
