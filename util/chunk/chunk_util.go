// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

// CopySelectedJoinRows uses for join to batch copy inner rows and outer row to chunk.
// This function optimize for join. To be exact, `appendOuterRows` optimizes copy outer row to `dst` chunk.
// Because the outer row in join is always same. so we can use batch copy for outer row data.
func CopySelectedJoinRows(src *Chunk, innerColOffset, outerColOffset int, selected []bool, dst *Chunk) bool {
	if src.NumRows() == 0 {
		return false
	}

	selectedRowNum := appendInnerRows(innerColOffset, outerColOffset, src, selected, dst)
	appendOuterRows(innerColOffset, outerColOffset, src, selectedRowNum, dst)
	dst.numVirtualRows += selectedRowNum
	return selectedRowNum > 0
}

// appendInnerRows appends multiple different rows to the chunk.
func appendInnerRows(innerColOffset, outerColOffset int, chkForJoin *Chunk, selected []bool, dst *Chunk) int {
	oldLen := dst.columns[innerColOffset].length
	var columns []*column
	if innerColOffset == 0 {
		columns = chkForJoin.columns[:outerColOffset]
	} else {
		columns = chkForJoin.columns[innerColOffset:]
	}
	for j, rowCol := range columns {
		chkCol := dst.columns[innerColOffset+j]
		for i := 0; i < len(selected); i++ {
			if !selected[i] {
				continue
			}
			chkCol.appendNullBitmap(!rowCol.isNull(i))
			chkCol.length++

			if rowCol.isFixed() {
				elemLen := len(rowCol.elemBuf)
				offset := i * elemLen
				chkCol.data = append(chkCol.data, rowCol.data[offset:offset+elemLen]...)
			} else {
				start, end := rowCol.offsets[i], rowCol.offsets[i+1]
				chkCol.data = append(chkCol.data, rowCol.data[start:end]...)
				chkCol.offsets = append(chkCol.offsets, int32(len(chkCol.data)))
			}
		}
	}
	return dst.columns[innerColOffset].length - oldLen
}

// appendOuterRows appends same outer row to the chunk with `numRows` times.
func appendOuterRows(innerColOffset, outerColOffset int, src *Chunk, numRows int, dst *Chunk) {
	row := src.GetRow(0)
	var columns []*column
	if innerColOffset == 0 {
		columns = src.columns[outerColOffset:]
	} else {
		columns = src.columns[:innerColOffset]
	}
	for i, rowCol := range columns {
		chkCol := dst.columns[outerColOffset+i]
		chkCol.appendMultiSameNullBitmap(!rowCol.isNull(row.idx), numRows)
		chkCol.length += numRows
		if rowCol.isFixed() {
			elemLen := len(rowCol.elemBuf)
			start := row.idx * elemLen
			end := start + numRows*elemLen
			chkCol.data = append(chkCol.data, rowCol.data[start:end]...)
		} else {
			start, end := rowCol.offsets[row.idx], rowCol.offsets[row.idx+numRows]
			chkCol.data = append(chkCol.data, rowCol.data[start:end]...)
			offsets := chkCol.offsets
			l := rowCol.offsets[row.idx+1] - rowCol.offsets[row.idx]
			for j := 0; j < numRows; j++ {
				offsets = append(offsets, int32(offsets[len(offsets)-1]+l))
			}
			chkCol.offsets = offsets
		}
	}
}
