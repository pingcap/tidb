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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type leftOuterJoinProbe struct {
	baseJoinProbe
	isNotMatchedRows []bool
}

func (j *leftOuterJoinProbe) prepareForProbe(chk *chunk.Chunk, info *probeProcessInfo) (*chunk.Chunk, int, error) {
	if !info.init && j.rightAsBuildSide {
		j.isNotMatchedRows = j.isNotMatchedRows[:0]
		for i := 0; i < info.chunk.NumRows(); i++ {
			j.isNotMatchedRows = append(j.isNotMatchedRows, false)
		}
	}
	joinedChk, remainCap, err := j.baseJoinProbe.prepareForProbe(chk, info)
	if err != nil {
		return nil, 0, err
	}
	return joinedChk, remainCap, nil
}

func (j *leftOuterJoinProbe) needScanHT() bool {
	return !j.rightAsBuildSide
}

func (j *leftOuterJoinProbe) scanHT(chunk2 *chunk.Chunk) (err error) {
	panic("not supported yet")
}

func (j *leftOuterJoinProbe) buildResultForMatchedRowsAfterOtherCondition(chk, joinedChk *chunk.Chunk, info *probeProcessInfo) {
	markedJoined := false
	for index, colIndex := range j.lUsed {
		dstCol := chk.Column(index)
		if joinedChk.Column(colIndex).Rows() > 0 {
			// probe column that is already in joinedChk
			srcCol := joinedChk.Column(colIndex)
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			markedJoined = true
			srcCol := info.chunk.Column(colIndex)
			chunk.CopySelectedRowsWithRowIdFunc(dstCol, srcCol, j.selected, 0, len(j.selected), func(i int) int {
				ret := j.rowIndexInfos[i].probeRowIndex
				j.isNotMatchedRows[ret] = false
				return ret
			})
		}
	}
	hasRemainCols := false
	for index, colIndex := range j.rUsed {
		dstCol := chk.Column(index + len(j.lUsed))
		srcCol := joinedChk.Column(colIndex + info.chunk.NumCols())
		if srcCol.Rows() > 0 {
			// build column that is already in joinedChk
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			hasRemainCols = true
		}
	}
	if hasRemainCols {
		markedJoined = true
		for index, result := range j.selected {
			if result {
				rowIndexInfo := j.rowIndexInfos[index]
				j.isNotMatchedRows[rowIndexInfo.probeRowIndex] = true
				j.appendBuildRowToChunk(chk, &rowInfo{
					rowStart:           rowIndexInfo.buildRowStart,
					rowData:            rowIndexInfo.buildRowData,
					currentColumnIndex: j.ctx.hashTableMeta.columnCountNeededForOtherCondition,
				}, info)
			}
		}
	}
	if !markedJoined {
		for index, result := range j.selected {
			if result {
				j.isNotMatchedRows[j.rowIndexInfos[index].probeRowIndex] = true
			}
		}
	}
}

func (j *leftOuterJoinProbe) buildResultForNotMatchedRows(chk *chunk.Chunk, startProbeRow int, info *probeProcessInfo) {
	// append not matched rows
	// for not matched rows, probe col is appended using original cols, and build column is appended using nulls
	prevRows, afterRows := 0, 0
	for index, colIndex := range j.lUsed {
		dstCol := chk.Column(index)
		srcCol := info.chunk.Column(colIndex)
		prevRows = dstCol.Rows()
		chunk.CopyRangeSelectedRows(dstCol, srcCol, startProbeRow, info.currentProbeRow, j.isNotMatchedRows)
		afterRows = dstCol.Rows()
	}
	nullRows := afterRows - prevRows
	if len(j.lUsed) == 0 {
		for i := startProbeRow; i < info.currentProbeRow; i++ {
			if j.isNotMatchedRows[i] == true {
				nullRows++
			}
		}
	}
	if nullRows > 0 {
		colOffset := len(j.lUsed)
		for index := range j.rUsed {
			dstCol := chk.Column(colOffset + index)
			dstCol.AppendNNulls(nullRows)
		}
	}
}

func (j *leftOuterJoinProbe) probeForRightBuild(chk, joinedChk *chunk.Chunk, remainCap int, info *probeProcessInfo) (err error) {
	meta := j.ctx.hashTableMeta
	length := 0
	startProbeRow := info.currentProbeRow

	for remainCap > 0 && info.currentProbeRow < info.chunk.NumRows() {
		if info.matchedRowsHeaders[info.currentProbeRow] != nil {
			// hash value match
			candidateRow := info.matchedRowsHeaders[info.currentProbeRow]
			if isKeyMatched(j.ctx.keyMode, info.serializedKeys[info.currentProbeRow], candidateRow, meta) {
				// join key match
				rowInfo := &rowInfo{rowStart: candidateRow, rowData: nil, currentColumnIndex: 0}
				currentRowData := j.appendBuildRowToChunk(joinedChk, rowInfo, info)
				if j.ctx.hasOtherCondition() {
					j.rowIndexInfos = append(j.rowIndexInfos, rowIndexInfo{probeRowIndex: info.currentProbeRow, buildRowStart: candidateRow, buildRowData: currentRowData})
				} else {
					// has no other condition, key match mean join match
					j.isNotMatchedRows[info.currentProbeRow] = false
				}
				length++
				joinedChk.IncNumVirtualRows()
			}
		} else {
			if length > 0 {
				// length > 0 mean current row has at least one key matched build rows
				j.offsetAndLengthArray = append(j.offsetAndLengthArray, offsetAndLength{offset: info.currentProbeRow, length: length})
				length = 0
			}
			info.currentProbeRow++
		}
		remainCap--
	}
	if length > 0 {
		j.offsetAndLengthArray = append(j.offsetAndLengthArray, offsetAndLength{offset: info.currentProbeRow, length: length})
	}
	j.appendProbeRowToChunk(joinedChk, info.chunk)

	if j.ctx.hasOtherCondition() {
		if joinedChk.NumRows() > 0 {
			j.selected = j.selected[:0]
			j.selected, err = expression.VectorizedFilter(j.ctx.sessCtx, j.ctx.otherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
			if err != nil {
				return err
			}
			j.buildResultForMatchedRowsAfterOtherCondition(chk, joinedChk, info)
		}
		// append the not matched rows
		j.buildResultForNotMatchedRows(chk, startProbeRow, info)
	} else {
		// if no the condition, chk == joinedChk, and the matched rows are already in joinedChk
		j.buildResultForNotMatchedRows(joinedChk, startProbeRow, info)
	}
	return
}

func (j *leftOuterJoinProbe) probeForLeftBuild(chk, joinedChk *chunk.Chunk, remainCap int, info *probeProcessInfo) (err error) {
	return
}

func (j *leftOuterJoinProbe) probe(chk *chunk.Chunk, info *probeProcessInfo) (err error) {
	if chk.IsFull() {
		return nil
	}
	joinedChk, remainCap, err1 := j.prepareForProbe(chk, info)
	if err1 != nil {
		return err1
	}
	if j.rightAsBuildSide {
		return j.probeForRightBuild(chk, joinedChk, remainCap, info)
	}
	return j.probeForLeftBuild(chk, joinedChk, remainCap, info)
}
