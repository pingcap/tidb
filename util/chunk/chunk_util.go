// Copyright 2018 PingCAP, Inc.
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

import "github.com/pingcap/errors"

// CopySelectedJoinRowsDirect directly copies the selected joined rows from the source Chunk
// to the destination Chunk.
// Return true if at least one joined row was selected.
func CopySelectedJoinRowsDirect(src *Chunk, selected []bool, dst *Chunk) (bool, error) {
	if src.NumRows() == 0 {
		return false, nil
	}
	if src.sel != nil || dst.sel != nil {
		return false, errors.New(msgErrSelNotNil)
	}
	if len(src.columns) == 0 {
		numSelected := 0
		for _, s := range selected {
			if s {
				numSelected++
			}
		}
		dst.numVirtualRows += numSelected
		return numSelected > 0, nil
	}

	oldLen := dst.columns[0].length
	for j, srcCol := range src.columns {
		dstCol := dst.columns[j]
		if srcCol.isFixed() {
			for i := 0; i < len(selected); i++ {
				if !selected[i] {
					continue
				}
				dstCol.appendNullBitmap(!srcCol.IsNull(i))
				dstCol.length++

				elemLen := len(srcCol.elemBuf)
				offset := i * elemLen
				dstCol.data = append(dstCol.data, srcCol.data[offset:offset+elemLen]...)
			}
		} else {
			for i := 0; i < len(selected); i++ {
				if !selected[i] {
					continue
				}
				dstCol.appendNullBitmap(!srcCol.IsNull(i))
				dstCol.length++

				start, end := srcCol.offsets[i], srcCol.offsets[i+1]
				dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
				dstCol.offsets = append(dstCol.offsets, int64(len(dstCol.data)))
			}
		}
	}
	numSelected := dst.columns[0].length - oldLen
	dst.numVirtualRows += numSelected
	return numSelected > 0, nil
}

// CopySelectedJoinRowsWithSameOuterRows copies the selected joined rows from the source Chunk
// to the destination Chunk.
// Return true if at least one joined row was selected.
//
// NOTE: All the outer rows in the source Chunk should be the same.
func CopySelectedJoinRowsWithSameOuterRows(src *Chunk, innerColOffset, innerColLen, outerColOffset, outerColLen int, selected []bool, dst *Chunk) (bool, error) {
	if src.NumRows() == 0 {
		return false, nil
	}
	if src.sel != nil || dst.sel != nil {
		return false, errors.New(msgErrSelNotNil)
	}

	numSelected := copySelectedInnerRows(innerColOffset, innerColLen, src, selected, dst)
	copySameOuterRows(outerColOffset, outerColLen, src, numSelected, dst)
	dst.numVirtualRows += numSelected
	return numSelected > 0, nil
}

// copySelectedInnerRows copies the selected inner rows from the source Chunk
// to the destination Chunk.
// return the number of rows which is selected.
func copySelectedInnerRows(innerColOffset, innerColLen int, src *Chunk, selected []bool, dst *Chunk) int {
	srcCols := src.columns[innerColOffset : innerColOffset+innerColLen]
	if len(srcCols) == 0 {
		numSelected := 0
		for _, s := range selected {
			if s {
				numSelected++
			}
		}
		return numSelected
	}
	oldLen := dst.columns[innerColOffset].length
	for j, srcCol := range srcCols {
		dstCol := dst.columns[innerColOffset+j]
		if srcCol.isFixed() {
			for i := 0; i < len(selected); i++ {
				if !selected[i] {
					continue
				}
				dstCol.appendNullBitmap(!srcCol.IsNull(i))
				dstCol.length++

				elemLen := len(srcCol.elemBuf)
				offset := i * elemLen
				dstCol.data = append(dstCol.data, srcCol.data[offset:offset+elemLen]...)
			}
		} else {
			for i := 0; i < len(selected); i++ {
				if !selected[i] {
					continue
				}
				dstCol.appendNullBitmap(!srcCol.IsNull(i))
				dstCol.length++

				start, end := srcCol.offsets[i], srcCol.offsets[i+1]
				dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
				dstCol.offsets = append(dstCol.offsets, int64(len(dstCol.data)))
			}
		}
	}
	return dst.columns[innerColOffset].length - oldLen
}

// copySameOuterRows copies the continuous 'numRows' outer rows in the source Chunk
// to the destination Chunk.
func copySameOuterRows(outerColOffset, outerColLen int, src *Chunk, numRows int, dst *Chunk) {
	if numRows <= 0 || outerColLen <= 0 {
		return
	}
	row := src.GetRow(0)
	srcCols := src.columns[outerColOffset : outerColOffset+outerColLen]
	for i, srcCol := range srcCols {
		dstCol := dst.columns[outerColOffset+i]
		dstCol.appendMultiSameNullBitmap(!srcCol.IsNull(row.idx), numRows)
		dstCol.length += numRows
		if srcCol.isFixed() {
			elemLen := len(srcCol.elemBuf)
			start := row.idx * elemLen
			end := start + numRows*elemLen
			dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
		} else {
			start, end := srcCol.offsets[row.idx], srcCol.offsets[row.idx+numRows]
			dstCol.data = append(dstCol.data, srcCol.data[start:end]...)
			offsets := dstCol.offsets
			elemLen := srcCol.offsets[row.idx+1] - srcCol.offsets[row.idx]
			for j := 0; j < numRows; j++ {
				offsets = append(offsets, int64(offsets[len(offsets)-1]+elemLen))
			}
			dstCol.offsets = offsets
		}
	}
}
