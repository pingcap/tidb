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

	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
)

func (j *baseJoinProbe) appendBuildRowToCachedBuildRowsV2(rowInfo *matchedRowInfo, chk *chunk.Chunk, currentColumnIndexInRow int, forOtherCondition bool) {
	j.cachedBuildRows[j.nextCachedBuildRowIndex] = *rowInfo
	j.nextCachedBuildRowIndex++
	if j.nextCachedBuildRowIndex == batchBuildRowSize {
		j.batchConstructBuildRows(chk, currentColumnIndexInRow, forOtherCondition)
	}
}

func (j *baseJoinProbe) appendBuildRowToCachedBuildRowsV1(probeRowIndex int, buildRowStart unsafe.Pointer, chk *chunk.Chunk, currentColumnIndexInRow int, forOtherCondition bool) {
	j.cachedBuildRows[j.nextCachedBuildRowIndex].probeRowIndex = probeRowIndex
	j.cachedBuildRows[j.nextCachedBuildRowIndex].buildRowOffset = 0
	*(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[j.nextCachedBuildRowIndex].buildRowStart)) = buildRowStart
	j.nextCachedBuildRowIndex++
	if j.nextCachedBuildRowIndex == batchBuildRowSize {
		j.batchConstructBuildRows(chk, currentColumnIndexInRow, forOtherCondition)
	}
}

func (j *baseJoinProbe) batchConstructBuildRows(chk *chunk.Chunk, currentColumnIndexInRow int, forOtherCondition bool) {
	j.appendBuildRowToChunk(chk, currentColumnIndexInRow, forOtherCondition)
	if forOtherCondition {
		j.rowIndexInfos = append(j.rowIndexInfos, j.cachedBuildRows[0:j.nextCachedBuildRowIndex]...)
	}
	j.nextCachedBuildRowIndex = 0
}

func (j *baseJoinProbe) prepareForProbe(chk *chunk.Chunk) (joinedChk *chunk.Chunk, remainCap int, err error) {
	j.offsetAndLengthArray = j.offsetAndLengthArray[:0]
	j.nextCachedBuildRowIndex = 0
	j.matchedRowsForCurrentProbeRow = 0
	joinedChk = chk
	if j.ctx.hasOtherCondition() {
		j.tmpChk.Reset()
		j.rowIndexInfos = j.rowIndexInfos[:0]
		j.selected = j.selected[:0]
		joinedChk = j.tmpChk
	}
	return joinedChk, chk.RequiredRows() - chk.NumRows(), nil
}

func (j *baseJoinProbe) appendBuildRowToChunk(chk *chunk.Chunk, currentColumnIndexInRow int, forOtherCondition bool) {
	if j.rightAsBuildSide {
		if forOtherCondition {
			j.appendBuildRowToChunkInternal(chk, j.rUsedInOtherCondition, true, j.currentChunk.NumCols(), currentColumnIndexInRow)
		} else {
			j.appendBuildRowToChunkInternal(chk, j.rUsed, false, len(j.lUsed), currentColumnIndexInRow)
		}
	} else {
		if forOtherCondition {
			j.appendBuildRowToChunkInternal(chk, j.lUsedInOtherCondition, true, 0, currentColumnIndexInRow)
		} else {
			j.appendBuildRowToChunkInternal(chk, j.lUsed, false, 0, currentColumnIndexInRow)
		}
	}
}

func (j *baseJoinProbe) appendBuildRowToChunkInternal(chk *chunk.Chunk, usedCols []int, forOtherCondition bool, colOffset int, currentColumnInRow int) {
	chkRows := chk.NumRows()
	needUpdateVirtualRow := currentColumnInRow == 0
	if len(usedCols) == 0 || j.nextCachedBuildRowIndex == 0 {
		if needUpdateVirtualRow {
			chk.SetNumVirtualRows(chkRows + j.nextCachedBuildRowIndex)
		}
		return
	}
	for i := range j.nextCachedBuildRowIndex {
		if j.cachedBuildRows[i].buildRowOffset == 0 {
			j.ctx.hashTableMeta.advanceToRowData(&j.cachedBuildRows[i])
		}
	}
	colIndexMap := make(map[int]int)
	for index, value := range usedCols {
		if forOtherCondition {
			colIndexMap[value] = value + colOffset
		} else {
			colIndexMap[value] = index + colOffset
		}
	}
	meta := j.ctx.hashTableMeta
	columnsToAppend := len(meta.rowColumnsOrder)
	if forOtherCondition {
		columnsToAppend = meta.columnCountNeededForOtherCondition
		if j.ctx.RightAsBuildSide {
			for _, value := range j.rUsed {
				colIndexMap[value] = value + colOffset
			}
		} else {
			for _, value := range j.lUsed {
				colIndexMap[value] = value + colOffset
			}
		}
	}
	for columnIndex := currentColumnInRow; columnIndex < len(meta.rowColumnsOrder) && columnIndex < columnsToAppend; columnIndex++ {
		indexInDstChk, ok := colIndexMap[meta.rowColumnsOrder[columnIndex]]
		var currentColumn *chunk.Column
		if ok {
			currentColumn = chk.Column(indexInDstChk)
			readNullMapThreadSafe := meta.isReadNullMapThreadSafe(columnIndex)
			if readNullMapThreadSafe {
				for index := range j.nextCachedBuildRowIndex {
					currentColumn.AppendNullBitmap(!meta.isColumnNull(*(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[index].buildRowStart)), columnIndex))
					j.cachedBuildRows[index].buildRowOffset = chunk.AppendCellFromRawData(currentColumn, *(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[index].buildRowStart)), j.cachedBuildRows[index].buildRowOffset)
				}
			} else {
				for index := range j.nextCachedBuildRowIndex {
					currentColumn.AppendNullBitmap(!meta.isColumnNullThreadSafe(*(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[index].buildRowStart)), columnIndex))
					j.cachedBuildRows[index].buildRowOffset = chunk.AppendCellFromRawData(currentColumn, *(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[index].buildRowStart)), j.cachedBuildRows[index].buildRowOffset)
				}
			}
		} else {
			// not used so don't need to insert into chk, but still need to advance rowData
			if meta.columnsSize[columnIndex] < 0 {
				for index := range j.nextCachedBuildRowIndex {
					size := *(*uint32)(unsafe.Add(*(*unsafe.Pointer)(unsafe.Pointer(&j.cachedBuildRows[index].buildRowStart)), j.cachedBuildRows[index].buildRowOffset))
					j.cachedBuildRows[index].buildRowOffset += sizeOfElementSize + int(size)
				}
			} else {
				for index := range j.nextCachedBuildRowIndex {
					j.cachedBuildRows[index].buildRowOffset += meta.columnsSize[columnIndex]
				}
			}
		}
	}
	if needUpdateVirtualRow {
		chk.SetNumVirtualRows(chkRows + j.nextCachedBuildRowIndex)
	}
}

func (j *baseJoinProbe) appendProbeRowToChunk(chk *chunk.Chunk, probeChk *chunk.Chunk) {
	if j.rightAsBuildSide {
		if j.ctx.hasOtherCondition() {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.lUsedInOtherCondition, 0, true)
		} else {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.lUsed, 0, false)
		}
	} else {
		if j.ctx.hasOtherCondition() {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.rUsedInOtherCondition, j.ctx.hashTableMeta.totalColumnNumber, true)
		} else {
			j.appendProbeRowToChunkInternal(chk, probeChk, j.rUsed, len(j.lUsed), false)
		}
	}
}

func (j *baseJoinProbe) appendProbeRowToChunkInternal(chk *chunk.Chunk, probeChk *chunk.Chunk, used []int, collOffset int, forOtherCondition bool) {
	if len(used) == 0 || len(j.offsetAndLengthArray) == 0 {
		return
	}

	totalTimes := 0
	preAllocMemForCol := func(srcCol *chunk.Column, dstCol *chunk.Column) {
		dataMemTotalLenDelta := int64(0)

		if totalTimes == 0 {
			for _, offsetAndLength := range j.offsetAndLengthArray {
				totalTimes += offsetAndLength.length
			}
		}

		offsetTotalLenDelta := int64(0)
		nullBitmapTotalLenDelta := dstCol.CalculateLenDeltaForAppendCellNTimesForNullBitMap(totalTimes)
		if dstCol.IsFixed() {
			dataMemTotalLenDelta = dstCol.CalculateLenDeltaForAppendCellNTimesForFixedElem(srcCol, totalTimes)
		} else {
			for _, offsetAndLength := range j.offsetAndLengthArray {
				dataMemTotalLenDelta += dstCol.CalculateLenDeltaForAppendCellNTimesForVarElem(srcCol, offsetAndLength.offset, offsetAndLength.length)
			}
			offsetTotalLenDelta = int64(totalTimes)
		}

		dstCol.Reserve(nullBitmapTotalLenDelta, dataMemTotalLenDelta, offsetTotalLenDelta)
	}

	if forOtherCondition {
		usedColumnMap := make(map[int]struct{})
		for _, colIndex := range used {
			if _, ok := usedColumnMap[colIndex]; !ok {
				srcCol := probeChk.Column(colIndex)
				dstCol := chk.Column(colIndex + collOffset)

				preAllocMemForCol(srcCol, dstCol)

				nullBitmapCapBefore := 0
				offsetCapBefore := 0
				dataCapBefore := 0
				if intest.InTest {
					nullBitmapCapBefore = dstCol.GetNullBitmapCap()
					offsetCapBefore = dstCol.GetOffsetCap()
					dataCapBefore = dstCol.GetDataCap()
				}

				for _, offsetAndLength := range j.offsetAndLengthArray {
					dstCol.AppendCellNTimes(srcCol, offsetAndLength.offset, offsetAndLength.length)
				}
				usedColumnMap[colIndex] = struct{}{}

				if intest.InTest {
					if nullBitmapCapBefore != dstCol.GetNullBitmapCap() {
						panic("Don't reserve enough memory")
					}

					if offsetCapBefore != dstCol.GetOffsetCap() {
						panic("Don't reserve enough memory")
					}

					if dataCapBefore != dstCol.GetDataCap() {
						panic("Don't reserve enough memory")
					}
				}
			}
		}
	} else {
		for index, colIndex := range used {
			srcCol := probeChk.Column(colIndex)
			dstCol := chk.Column(index + collOffset)

			preAllocMemForCol(srcCol, dstCol)

			for _, offsetAndLength := range j.offsetAndLengthArray {
				dstCol.AppendCellNTimes(srcCol, offsetAndLength.offset, offsetAndLength.length)
			}
		}
	}
}

func (j *baseJoinProbe) buildResultAfterOtherCondition(chk *chunk.Chunk, joinedChk *chunk.Chunk) (err error) {
	// construct the return chunk based on joinedChk and selected, there are 3 kinds of columns
	// 1. columns already in joinedChk
	// 2. columns from build side, but not in joinedChk
	// 3. columns from probe side, but not in joinedChk
	rowCount := chk.NumRows()
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
			chunk.CopySelectedRowsWithRowIDFunc(dstCol, srcCol, j.selected, 0, len(j.selected), func(i int) int {
				return j.usedRows[j.rowIndexInfos[i].probeRowIndex]
			})
		}
	}
	buildUsedColumns, buildColOffset, buildColOffsetInJoinedChk := j.lUsed, 0, 0
	if j.rightAsBuildSide {
		buildUsedColumns, buildColOffset, buildColOffsetInJoinedChk = j.rUsed, len(j.lUsed), j.currentChunk.NumCols()
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
		j.nextCachedBuildRowIndex = 0
		// build column that is not in joinedChk
		for index, result := range j.selected {
			if result {
				j.appendBuildRowToCachedBuildRowsV2(&j.rowIndexInfos[index], chk, j.ctx.hashTableMeta.columnCountNeededForOtherCondition, false)
			}
		}
		if j.nextCachedBuildRowIndex > 0 {
			j.batchConstructBuildRows(chk, j.ctx.hashTableMeta.columnCountNeededForOtherCondition, false)
		}
	}
	rowsAdded := 0
	for _, result := range j.selected {
		if result {
			rowsAdded++
		}
	}
	chk.SetNumVirtualRows(rowCount + rowsAdded)
	return
}

