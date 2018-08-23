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

import "github.com/pingcap/tidb/types"

// MutChunk represents a mutable Chunk.
// The underlying columns only contains one row and not exposed to the user.
type MutChunk = Chunk

// NewMutChunk return a chunk with capacity 1.
func NewMutChunk(colTypes []*types.FieldType) *MutChunk {
	return NewChunkWithCapacity(colTypes, 1)
}

// MutChkInit init chk for ShadowCopyPartialRow.
// The chk chunk will only contain one row, so initial the nullBitMap , offsets and length first for performance.
func MutChkInit(chk *MutChunk) {
	chk.Reset()
	for _, c := range chk.columns {
		c.nullBitmap = append(c.nullBitmap, 0)
		c.offsets = append(c.offsets, 0)
		c.length = 1
	}
}

// ShadowCopyPartialRow use shadow copy to instead of AppendPartialRow,
// ShadowCopyPartialRow copies the data of row to the first row of dst.
func ShadowCopyPartialRow(colIdx int, row Row, dst *MutChunk) {
	for i, rowCol := range row.c.columns {
		chkCol := dst.columns[colIdx+i]
		if !rowCol.isNull(row.idx) {
			chkCol.nullBitmap[0] = 1
		} else {
			chkCol.nullBitmap[0] = 0
		}

		if rowCol.isFixed() {
			elemLen := len(rowCol.elemBuf)
			offset := row.idx * elemLen
			chkCol.data = rowCol.data[offset : offset+elemLen]
		} else {
			start, end := rowCol.offsets[row.idx], rowCol.offsets[row.idx+1]
			chkCol.data = rowCol.data[start:end]
			chkCol.offsets[1] = int32(len(chkCol.data))
		}
	}
}
