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

type baseOuterJoinProbe struct {
	baseJoinProbe
	// isLeftOuterJoinProbe is true means the probe side is left side, otherwise is right side
	isLeftOuterJoinProbe bool
	// isOuterSideBuild
	isOuterSideBuild bool
	// used when build left side, isNotMatchedRows is indexed by logical row index
	isNotMatchedRows []bool
	// used when build right side
	rowIter *rowIter
}

func (j *baseOuterJoinProbe) SetChunkForProbe(chunk *chunk.Chunk) (err error) {
	err = j.baseJoinProbe.SetChunkForProbe(chunk)
	if err != nil {
		return err
	}
	if !j.isOuterSideBuild {
		j.isNotMatchedRows = j.isNotMatchedRows[:0]
		for i := 0; i < j.chunkRows; i++ {
			j.isNotMatchedRows = append(j.isNotMatchedRows, true)
		}
	}
	return nil
}

func (j *baseOuterJoinProbe) NeedScanRowTable() bool {
	return j.isOuterSideBuild
}

func (j *baseOuterJoinProbe) IsScanRowTableDone() bool {
	if !j.isOuterSideBuild {
		panic("should not reach here")
	}
	return j.rowIter.isEnd()
}

func (j *baseOuterJoinProbe) InitForScanRowTable() {
	if !j.isOuterSideBuild {
		panic("should not reach here")
	}
	totalRowCount := j.ctx.hashTableContext.hashTable.totalRowCount()
	concurrency := j.ctx.Concurrency
	workID := uint64(j.workID)
	avgRowPerWorker := totalRowCount / uint64(concurrency)
	startIndex := workID * avgRowPerWorker
	endIndex := (workID + 1) * avgRowPerWorker
	if workID == uint64(concurrency-1) {
		endIndex = totalRowCount
	}
	if endIndex > totalRowCount {
		endIndex = totalRowCount
	}
	j.rowIter = j.ctx.hashTableContext.hashTable.createRowIter(startIndex, endIndex)
}

func (j *baseOuterJoinProbe) ScanRowTable(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) *hashjoinWorkerResult {
	if !j.isOuterSideBuild {
		panic("should not reach here")
	}
	if joinResult.chk.IsFull() {
		return joinResult
	}
	if j.rowIter == nil {
		panic("scanRowTable before init")
	}
	j.cachedBuildRows = j.cachedBuildRows[:0]
	meta := j.ctx.hashTableMeta
	insertedRows := 0
	remainCap := joinResult.chk.RequiredRows() - joinResult.chk.NumRows()
	for insertedRows < remainCap && !j.rowIter.isEnd() {
		currentRow := j.rowIter.getValue()
		if !meta.isCurrentRowUsed(currentRow) {
			// append build side of this row
			j.appendBuildRowToCachedBuildRowsAndConstructBuildRowsIfNeeded(createMatchRowInfo(0, currentRow), joinResult.chk, 0, false)
			insertedRows++
		}
		j.rowIter.next()
	}
	err := checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		joinResult.err = err
		return joinResult
	}
	if len(j.cachedBuildRows) > 0 {
		j.batchConstructBuildRows(joinResult.chk, 0, false)
	}
	if j.isLeftOuterJoinProbe {
		// append probe side in batch
		colOffset := len(j.lUsed)
		for index := range j.rUsed {
			joinResult.chk.Column(index + colOffset).AppendNNulls(insertedRows)
		}
	} else {
		// append probe side in batch
		for index := range j.lUsed {
			joinResult.chk.Column(index).AppendNNulls(insertedRows)
		}
	}
	return joinResult
}

func (j *baseOuterJoinProbe) buildResultForMatchedRowsAfterOtherCondition(chk, joinedChk *chunk.Chunk,
	probeSideOffset int, probeSide []int, probeSideCurrentChunkOffset int,
	buildSideOffset int, buildSide []int, buildSideCurrentChunkOffset int) {
	rowCount := chk.NumRows()
	markedJoined := false
	for index, colIndex := range probeSide {
		dstCol := chk.Column(probeSideOffset + index)
		if joinedChk.Column(colIndex+probeSideCurrentChunkOffset).Rows() > 0 {
			// probe column that is already in joinedChk
			srcCol := joinedChk.Column(colIndex + probeSideCurrentChunkOffset)
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			markedJoined = true
			srcCol := j.currentChunk.Column(colIndex)
			chunk.CopySelectedRowsWithRowIDFunc(dstCol, srcCol, j.selected, 0, len(j.selected), func(i int) int {
				ret := j.rowIndexInfos[i].probeRowIndex
				j.isNotMatchedRows[ret] = false
				return j.usedRows[ret]
			})
		}
	}
	hasRemainCols := false
	for index, colIndex := range buildSide {
		dstCol := chk.Column(buildSideOffset + index)
		srcCol := joinedChk.Column(buildSideCurrentChunkOffset + colIndex)
		if srcCol.Rows() > 0 {
			// build column that is already in joinedChk
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			hasRemainCols = true
		}
	}
	if hasRemainCols {
		j.cachedBuildRows = j.cachedBuildRows[:0]
		markedJoined = true
		meta := j.ctx.hashTableMeta
		for index, result := range j.selected {
			if result {
				rowIndexInfo := j.rowIndexInfos[index]
				j.isNotMatchedRows[rowIndexInfo.probeRowIndex] = false
				j.appendBuildRowToCachedBuildRowsAndConstructBuildRowsIfNeeded(rowIndexInfo, chk, meta.columnCountNeededForOtherCondition, false)
			}
		}
		if len(j.cachedBuildRows) > 0 {
			j.batchConstructBuildRows(chk, meta.columnCountNeededForOtherCondition, false)
		}
	}
	if !markedJoined {
		for index, result := range j.selected {
			if result {
				j.isNotMatchedRows[j.rowIndexInfos[index].probeRowIndex] = false
			}
		}
	}
	rowsAdded := 0
	for _, result := range j.selected {
		if result {
			rowsAdded++
		}
	}
	chk.SetNumVirtualRows(rowCount + rowsAdded)
}

func (j *baseOuterJoinProbe) buildResultForNotMatchedRows(chk *chunk.Chunk, startProbeRow int,
	probeSideOffset int, probeSide []int,
	buildSideOffset int, buildSide []int) {
	// append not matched rows
	// for not matched rows, probe col is appended using original cols, and build column is appended using nulls
	prevRows := chk.NumRows()
	afterRows := prevRows
	for index, colIndex := range probeSide {
		dstCol := chk.Column(probeSideOffset + index)
		srcCol := j.currentChunk.Column(colIndex)
		chunk.CopySelectedRowsWithRowIDFunc(dstCol, srcCol, j.isNotMatchedRows, startProbeRow, j.currentProbeRow, func(i int) int {
			return j.usedRows[i]
		})
		afterRows = dstCol.Rows()
	}
	nullRows := afterRows - prevRows
	if len(probeSide) == 0 {
		for i := startProbeRow; i < j.currentProbeRow; i++ {
			if j.isNotMatchedRows[i] {
				nullRows++
			}
		}
	}
	if nullRows > 0 {
		for index := range buildSide {
			dstCol := chk.Column(buildSideOffset + index)
			dstCol.AppendNNulls(nullRows)
		}
		chk.SetNumVirtualRows(prevRows + nullRows)
	}
}
