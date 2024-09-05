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
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type outerJoinProbe struct {
	baseJoinProbe
	// isOuterSideBuild is true means the outer side is build side, otherwise is probe side.
	// Outer side is the all-fetched side, and inner side is the null-append side.
	// For left out join, left side is outer side, and right side is inner side.
	// For right out join, right side is outer side, and left side is inner side.
	isOuterSideBuild bool
	// used when use inner side to build, isNotMatchedRows is indexed by logical row index
	isNotMatchedRows []bool
	// used when use outer side to build
	rowIter *rowIter
	// build/probe side used columns and offset in result chunk
	buildColUsed              []int
	buildColOffsetInResultChk int
	probeColUsed              []int
	probeColOffsetInResultChk int
}

func newOuterJoinProbe(base baseJoinProbe, isOuterSideBuild bool, isRightSideBuild bool) *outerJoinProbe {
	probe := &outerJoinProbe{
		baseJoinProbe:    base,
		isOuterSideBuild: isOuterSideBuild,
	}
	if isRightSideBuild {
		probe.buildColUsed = base.rUsed
		probe.buildColOffsetInResultChk = len(base.lUsed)
		probe.probeColUsed = base.lUsed
		probe.probeColOffsetInResultChk = 0
	} else {
		probe.buildColUsed = base.lUsed
		probe.buildColOffsetInResultChk = 0
		probe.probeColUsed = base.rUsed
		probe.probeColOffsetInResultChk = len(base.lUsed)
	}
	return probe
}

func (j *outerJoinProbe) SetChunkForProbe(chunk *chunk.Chunk) (err error) {
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

func (j *outerJoinProbe) NeedScanRowTable() bool {
	return j.isOuterSideBuild
}

func (j *outerJoinProbe) IsScanRowTableDone() bool {
	if !j.isOuterSideBuild {
		panic("should not reach here")
	}
	return j.rowIter.isEnd()
}

func (j *outerJoinProbe) InitForScanRowTable() {
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

func (j *outerJoinProbe) ScanRowTable(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) *hashjoinWorkerResult {
	if !j.isOuterSideBuild {
		panic("should not reach here")
	}
	if joinResult.chk.IsFull() {
		return joinResult
	}
	if j.rowIter == nil {
		panic("scanRowTable before init")
	}
	j.nextCachedBuildRowIndex = 0
	meta := j.ctx.hashTableMeta
	insertedRows := 0
	remainCap := joinResult.chk.RequiredRows() - joinResult.chk.NumRows()
	for insertedRows < remainCap && !j.rowIter.isEnd() {
		currentRow := j.rowIter.getValue()
		if !meta.isCurrentRowUsed(currentRow) {
			// append build side of this row
			j.appendBuildRowToCachedBuildRowsV1(0, currentRow, joinResult.chk, 0, false)
			insertedRows++
		}
		j.rowIter.next()
	}
	err := checkSQLKiller(sqlKiller, "killedDuringProbe")
	if err != nil {
		joinResult.err = err
		return joinResult
	}
	if j.nextCachedBuildRowIndex > 0 {
		j.batchConstructBuildRows(joinResult.chk, 0, false)
	}
	// append probe side in batch
	for index := range j.probeColUsed {
		joinResult.chk.Column(index + j.probeColOffsetInResultChk).AppendNNulls(insertedRows)
	}
	return joinResult
}

func (j *outerJoinProbe) buildResultForMatchedRowsAfterOtherCondition(chk, joinedChk *chunk.Chunk) {
	probeColOffsetInJoinedChunk, buildColOffsetInJoinedChunk := j.currentChunk.NumCols(), 0
	if j.rightAsBuildSide {
		probeColOffsetInJoinedChunk, buildColOffsetInJoinedChunk = 0, j.currentChunk.NumCols()
	}
	rowCount := chk.NumRows()
	markedJoined := false
	for index, colIndex := range j.probeColUsed {
		dstCol := chk.Column(j.probeColOffsetInResultChk + index)
		if joinedChk.Column(colIndex+probeColOffsetInJoinedChunk).Rows() > 0 {
			// probe column that is already in joinedChk
			srcCol := joinedChk.Column(colIndex + probeColOffsetInJoinedChunk)
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
	for index, colIndex := range j.buildColUsed {
		dstCol := chk.Column(j.buildColOffsetInResultChk + index)
		srcCol := joinedChk.Column(buildColOffsetInJoinedChunk + colIndex)
		if srcCol.Rows() > 0 {
			// build column that is already in joinedChk
			chunk.CopySelectedRows(dstCol, srcCol, j.selected)
		} else {
			hasRemainCols = true
		}
	}
	if hasRemainCols {
		j.nextCachedBuildRowIndex = 0
		markedJoined = true
		meta := j.ctx.hashTableMeta
		for index, result := range j.selected {
			if result {
				rowIndexInfo := j.rowIndexInfos[index]
				j.isNotMatchedRows[rowIndexInfo.probeRowIndex] = false
				j.appendBuildRowToCachedBuildRowsV2(&rowIndexInfo, chk, meta.columnCountNeededForOtherCondition, false)
			}
		}
		if j.nextCachedBuildRowIndex > 0 {
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

func (j *outerJoinProbe) buildResultForNotMatchedRows(chk *chunk.Chunk, startProbeRow int) {
	// append not matched rows
	// for not matched rows, probe col is appended using original cols, and build column is appended using nulls
	prevRows := chk.NumRows()
	afterRows := prevRows
	for index, colIndex := range j.probeColUsed {
		dstCol := chk.Column(j.probeColOffsetInResultChk + index)
		srcCol := j.currentChunk.Column(colIndex)
		chunk.CopySelectedRowsWithRowIDFunc(dstCol, srcCol, j.isNotMatchedRows, startProbeRow, j.currentProbeRow, func(i int) int {
			return j.usedRows[i]
		})
		afterRows = dstCol.Rows()
	}
	nullRows := afterRows - prevRows
	if len(j.probeColUsed) == 0 {
		for i := startProbeRow; i < j.currentProbeRow; i++ {
			if j.isNotMatchedRows[i] {
				nullRows++
			}
		}
	}
	if nullRows > 0 {
		for index := range j.buildColUsed {
			dstCol := chk.Column(j.buildColOffsetInResultChk + index)
			dstCol.AppendNNulls(nullRows)
		}
		chk.SetNumVirtualRows(prevRows + nullRows)
	}
}

func (j *outerJoinProbe) probeForInnerSideBuild(chk, joinedChk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := j.ctx.hashTableMeta
	startProbeRow := j.currentProbeRow
	hasOtherCondition := j.ctx.hasOtherCondition()
	tagHelper := j.ctx.hashTableContext.tagHelper

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			// hash value match
			candidateRow := tagHelper.toUnsafePointer(j.matchedRowsHeaders[j.currentProbeRow])
			if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				// join key match
				j.appendBuildRowToCachedBuildRowsV1(j.currentProbeRow, candidateRow, joinedChk, 0, hasOtherCondition)
				if !hasOtherCondition {
					// has no other condition, key match mean join match
					j.isNotMatchedRows[j.currentProbeRow] = false
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

func (j *outerJoinProbe) probeForOuterSideBuild(chk, joinedChk *chunk.Chunk, remainCap int, sqlKiller *sqlkiller.SQLKiller) (err error) {
	meta := j.ctx.hashTableMeta
	hasOtherCondition := j.ctx.hasOtherCondition()
	tagHelper := j.ctx.hashTableContext.tagHelper

	for remainCap > 0 && j.currentProbeRow < j.chunkRows {
		if j.matchedRowsHeaders[j.currentProbeRow] != 0 {
			// hash value match
			candidateRow := tagHelper.toUnsafePointer(j.matchedRowsHeaders[j.currentProbeRow])
			if isKeyMatched(meta.keyMode, j.serializedKeys[j.currentProbeRow], candidateRow, meta) {
				// join key match
				j.appendBuildRowToCachedBuildRowsV1(j.currentProbeRow, candidateRow, joinedChk, 0, hasOtherCondition)
				if !hasOtherCondition {
					// has no other condition, key match means join match
					meta.setUsedFlag(candidateRow)
				}
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

func (j *outerJoinProbe) Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, _ *hashjoinWorkerResult) {
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
	if j.isOuterSideBuild {
		err = j.probeForOuterSideBuild(joinResult.chk, joinedChk, remainCap, sqlKiller)
	} else {
		err = j.probeForInnerSideBuild(joinResult.chk, joinedChk, remainCap, sqlKiller)
	}
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	return true, joinResult
}
