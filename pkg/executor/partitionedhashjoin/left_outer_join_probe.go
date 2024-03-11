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

type leftOuterJoinProbe struct {
	innerJoinProbe
	// used when build right side
	isNotMatchedRows []bool
	// used when build left side
	currentRowIter *rowIter
	endRowIter     *rowIter
}

func (j *leftOuterJoinProbe) setChunkForProbe(chunk *chunk.Chunk) (err error) {
	err = j.innerJoinProbe.SetChunkForProbe(chunk)
	if err != nil {
		return err
	}
	if j.rightAsBuildSide {
		j.isNotMatchedRows = j.isNotMatchedRows[:0]
		for i := 0; i < j.chunkRows; i++ {
			j.isNotMatchedRows = append(j.isNotMatchedRows, false)
		}
	}
	return nil
}

func (j *leftOuterJoinProbe) needScanRowTable() bool {
	return !j.rightAsBuildSide
}

func (j *leftOuterJoinProbe) isScanRowTableDone() bool {
	if j.rightAsBuildSide {
		panic("should not reach here")
	}
	return j.currentRowIter.equals(j.endRowIter)
}

func (j *leftOuterJoinProbe) InitForScanRowTable() {
	if j.rightAsBuildSide {
		panic("should not reach here")
	}
	totalRowCount := j.ctx.joinHashTable.totalRowCount()
	concurrency := j.ctx.Concurrency
	workId := uint64(j.workID)
	avgRowPerWorker := totalRowCount / uint64(concurrency)
	startIndex := workId * avgRowPerWorker
	endIndex := (workId + 1) * avgRowPerWorker
	if workId == uint64(concurrency-1) {
		endIndex = totalRowCount
	}
	if endIndex > totalRowCount {
		endIndex = totalRowCount
	}
	j.currentRowIter = j.ctx.joinHashTable.createRowIter(startIndex)
	j.endRowIter = j.ctx.joinHashTable.createRowIter(endIndex)
}

func (j *leftOuterJoinProbe) ScanRowTable(joinResult *util.HashjoinWorkerResult) *util.HashjoinWorkerResult {
	if j.rightAsBuildSide {
		panic("should not reach here")
	}
	if joinResult.Chk.IsFull() {
		return joinResult
	}
	if j.currentRowIter == nil {
		panic("scanRowTable before init")
	}
	meta := j.ctx.hashTableMeta
	insertedRows := 0
	remainCap := joinResult.Chk.RequiredRows() - joinResult.Chk.NumRows()
	for insertedRows < remainCap && !j.currentRowIter.equals(j.endRowIter) {
		currentRow := j.currentRowIter.getValue()
		if !meta.isCurrentRowUsed(currentRow) {
			// append build side of this row
			j.appendBuildRowToChunkInternal(joinResult.Chk, j.lUsed, &rowInfo{rowStart: currentRow, rowData: nil, currentColumnIndex: 0}, -1, 0)
			joinResult.Chk.IncNumVirtualRows()
			insertedRows++
		}
		j.currentRowIter.next()
	}
	// append probe side in batch
	colOffset := len(j.lUsed)
	for index := range j.rUsed {
		joinResult.Chk.Column(index + colOffset).AppendNNulls(insertedRows)
	}
	return joinResult
}

func (j *leftOuterJoinProbe) buildResultForMatchedRowsAfterOtherCondition(chk, joinedChk *chunk.Chunk) {
	markedJoined := false
	for index, colIndex := range j.lUsed {
		dstCol := chk.Column(index)
		if joinedChk.Column(colIndex).Rows() > 0 {
			// probe column that is already in joinedChk
			srcCol := joinedChk.Column(colIndex)
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			markedJoined = true
			srcCol := j.currentChunk.Column(colIndex)
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
		srcCol := joinedChk.Column(colIndex + j.currentChunk.NumCols())
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
				})
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

func (j *leftOuterJoinProbe) buildResultForNotMatchedRows(chk *chunk.Chunk, startProbeRow int) {
	// append not matched rows
	// for not matched rows, probe col is appended using original cols, and build column is appended using nulls
	prevRows, afterRows := 0, 0
	for index, colIndex := range j.lUsed {
		dstCol := chk.Column(index)
		srcCol := j.currentChunk.Column(colIndex)
		prevRows = dstCol.Rows()
		chunk.CopyRangeSelectedRows(dstCol, srcCol, startProbeRow, j.currentProbeRow, j.isNotMatchedRows)
		afterRows = dstCol.Rows()
	}
	nullRows := afterRows - prevRows
	if len(j.lUsed) == 0 {
		for i := startProbeRow; i < j.currentProbeRow; i++ {
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

func (j *leftOuterJoinProbe) probeForRightBuild(chk, joinedChk *chunk.Chunk, remainCap int) (err error) {
	meta := j.ctx.hashTableMeta
	length := 0
	startProbeRow := j.currentProbeRow

	for remainCap > 0 && j.currentProbeRow < j.currentChunk.NumRows() {
		if j.matchedRowsHeaders[j.currentProbeRow] != nil {
			// hash value match
			candidateRow := j.matchedRowsHeaders[j.currentProbeRow]
			if isKeyMatched(j.ctx.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				// join key match
				rowInfo := &rowInfo{rowStart: candidateRow, rowData: nil, currentColumnIndex: 0}
				currentRowData := j.appendBuildRowToChunk(joinedChk, rowInfo)
				if j.ctx.hasOtherCondition() {
					j.rowIndexInfos = append(j.rowIndexInfos, rowIndexInfo{probeRowIndex: j.currentProbeRow, buildRowStart: candidateRow, buildRowData: currentRowData})
				} else {
					// has no other condition, key match mean join match
					j.isNotMatchedRows[j.currentProbeRow] = false
				}
				length++
				joinedChk.IncNumVirtualRows()
			}
			j.matchedRowsHeaders[j.currentProbeRow] = getNextRowAddress(candidateRow)
		} else {
			j.appendOffsetAndLength(j.currentProbeRow, length)
			length = 0
			j.currentProbeRow++
		}
		remainCap--
	}
	j.appendOffsetAndLength(j.currentProbeRow, length)
	j.appendProbeRowToChunk(joinedChk, j.currentChunk)

	if j.ctx.hasOtherCondition() {
		if joinedChk.NumRows() > 0 {
			j.selected = j.selected[:0]
			j.selected, err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
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

func (j *leftOuterJoinProbe) probeForLeftBuild(chk, joinedChk *chunk.Chunk, remainCap int) (err error) {
	meta := j.ctx.hashTableMeta
	length := 0

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != nil {
			// hash value match
			candidateRow := j.matchedRowsHeaders[j.currentProbeRow]
			if isKeyMatched(j.ctx.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				// join key match
				rowInfo := &rowInfo{rowStart: candidateRow, rowData: nil, currentColumnIndex: 0}
				currentRowData := j.appendBuildRowToChunk(joinedChk, rowInfo)
				if j.ctx.hasOtherCondition() {
					j.rowIndexInfos = append(j.rowIndexInfos, rowIndexInfo{probeRowIndex: j.currentProbeRow, buildRowStart: candidateRow, buildRowData: currentRowData})
				} else {
					// has no other condition, key match means join match
					meta.setUsedFlag(currentRowData)
				}
				length++
				joinedChk.IncNumVirtualRows()
				remainCap--
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
		j.selected, err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.OtherCondition, chunk.NewIterator4Chunk(joinedChk), j.selected)
		if err != nil {
			return err
		}
		err = j.buildResultAfterOtherCondition(chk, joinedChk)
	}
	return
}

func (j *leftOuterJoinProbe) probe(chk *chunk.Chunk) (err error) {
	if chk.IsFull() {
		return nil
	}
	joinedChk, remainCap, err1 := j.prepareForProbe(chk)
	if err1 != nil {
		return err1
	}
	if j.rightAsBuildSide {
		return j.probeForRightBuild(chk, joinedChk, remainCap)
	}
	return j.probeForLeftBuild(chk, joinedChk, remainCap)
}
