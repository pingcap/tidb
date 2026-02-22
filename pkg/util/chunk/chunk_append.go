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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"unsafe"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
)

func (c *Chunk) AppendRow(row Row) {
	c.AppendPartialRow(0, row)
	c.numVirtualRows++
}

// AppendPartialRow appends a row to the chunk, starting from colOff.
func (c *Chunk) AppendPartialRow(colOff int, row Row) {
	c.appendSel(colOff)
	for i, rowCol := range row.c.columns {
		chkCol := c.columns[colOff+i]
		appendCellByCell(chkCol, rowCol, row.idx)
	}
}

// AppendRowsByColIdxs appends multiple rows by its colIdxs to the chunk.
// 1. every columns are used if colIdxs is nil.
// 2. no columns are used if colIdxs is not nil but the size of colIdxs is 0.
func (c *Chunk) AppendRowsByColIdxs(rows []Row, colIdxs []int) (wide int) {
	if colIdxs == nil {
		if len(rows) == 0 {
			wide = 0
			return
		}
		c.AppendRows(rows)
		wide = rows[0].Len() * len(rows)
		return
	}
	for _, srcRow := range rows {
		c.appendSel(0)
		for i, colIdx := range colIdxs {
			appendCellByCell(c.columns[i], srcRow.c.columns[colIdx], srcRow.idx)
		}
	}
	c.numVirtualRows += len(rows)
	wide = len(colIdxs) * len(rows)
	return
}

// AppendRowByColIdxs appends a row to the chunk, using the row's columns specified by colIdxs.
// 1. every columns are used if colIdxs is nil.
// 2. no columns are used if colIdxs is not nil but the size of colIdxs is 0.
func (c *Chunk) AppendRowByColIdxs(row Row, colIdxs []int) (wide int) {
	wide = c.AppendPartialRowByColIdxs(0, row, colIdxs)
	c.numVirtualRows++
	return
}

// AppendPartialRowByColIdxs appends a row to the chunk starting from colOff,
// using the row's columns specified by colIdxs.
// 1. every columns are used if colIdxs is nil.
// 2. no columns are used if colIdxs is not nil but the size of colIdxs is 0.
func (c *Chunk) AppendPartialRowByColIdxs(colOff int, row Row, colIdxs []int) (wide int) {
	if colIdxs == nil {
		c.AppendPartialRow(colOff, row)
		return row.Len()
	}

	c.appendSel(colOff)
	for i, colIdx := range colIdxs {
		rowCol := row.c.columns[colIdx]
		chkCol := c.columns[colOff+i]
		appendCellByCell(chkCol, rowCol, row.idx)
	}
	return len(colIdxs)
}

// appendCellByCell appends the cell with rowIdx of src into dst.
func appendCellByCell(dst *Column, src *Column, rowIdx int) {
	dst.appendNullBitmap(!src.IsNull(rowIdx))
	if src.IsFixed() {
		elemLen := len(src.elemBuf)
		offset := rowIdx * elemLen
		dst.data = append(dst.data, src.data[offset:offset+elemLen]...)
	} else {
		start, end := src.offsets[rowIdx], src.offsets[rowIdx+1]
		dst.data = append(dst.data, src.data[start:end]...)
		dst.offsets = append(dst.offsets, int64(len(dst.data)))
	}
	dst.length++
}

// AppendCellFromRawData appends the cell from raw data
func AppendCellFromRawData(dst *Column, rowData unsafe.Pointer, currentOffset int) int {
	if dst.IsFixed() {
		elemLen := len(dst.elemBuf)
		dst.data = append(dst.data, hack.GetBytesFromPtr(unsafe.Add(rowData, currentOffset), elemLen)...)
		currentOffset += elemLen
	} else {
		elemLen := *(*uint32)(unsafe.Add(rowData, currentOffset))
		if elemLen > 0 {
			dst.data = append(dst.data, hack.GetBytesFromPtr(unsafe.Add(rowData, currentOffset+sizeUint32), int(elemLen))...)
		}
		dst.offsets = append(dst.offsets, int64(len(dst.data)))
		currentOffset += int(elemLen + uint32(sizeUint32))
	}
	dst.length++
	return currentOffset
}

// Append appends rows in [begin, end) in another Chunk to a Chunk.
func (c *Chunk) Append(other *Chunk, begin, end int) {
	for colID, src := range other.columns {
		dst := c.columns[colID]
		if src.IsFixed() {
			elemLen := len(src.elemBuf)
			dst.data = append(dst.data, src.data[begin*elemLen:end*elemLen]...)
		} else {
			beginOffset, endOffset := src.offsets[begin], src.offsets[end]
			dst.data = append(dst.data, src.data[beginOffset:endOffset]...)
			lastOffset := dst.offsets[len(dst.offsets)-1]
			for i := begin; i < end; i++ {
				lastOffset += src.offsets[i+1] - src.offsets[i]
				dst.offsets = append(dst.offsets, lastOffset)
			}
		}
		for i := begin; i < end; i++ {
			c.appendSel(colID)
			dst.appendNullBitmap(!src.IsNull(i))
			dst.length++
		}
	}
	c.numVirtualRows += end - begin
}

// TruncateTo truncates rows from tail to head in a Chunk to "numRows" rows.
func (c *Chunk) TruncateTo(numRows int) {
	c.Reconstruct()
	for _, col := range c.columns {
		if col.IsFixed() {
			elemLen := len(col.elemBuf)
			col.data = col.data[:numRows*elemLen]
		} else {
			col.data = col.data[:col.offsets[numRows]]
			col.offsets = col.offsets[:numRows+1]
		}
		col.length = numRows
		bitmapLen := (col.length + 7) / 8
		col.nullBitmap = col.nullBitmap[:bitmapLen]
		if col.length%8 != 0 {
			// When we append null, we simply increment the nullCount,
			// so we need to clear the unused bits in the last bitmap byte.
			lastByte := col.nullBitmap[bitmapLen-1]
			unusedBitsLen := 8 - uint(col.length%8)
			lastByte <<= unusedBitsLen
			lastByte >>= unusedBitsLen
			col.nullBitmap[bitmapLen-1] = lastByte
		}
	}
	c.numVirtualRows = numRows
}

// AppendNull appends a null value to the chunk.
func (c *Chunk) AppendNull(colIdx int) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendNull()
}

// AppendInt64 appends a int64 value to the chunk.
func (c *Chunk) AppendInt64(colIdx int, i int64) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendInt64(i)
}

// AppendUint64 appends a uint64 value to the chunk.
func (c *Chunk) AppendUint64(colIdx int, u uint64) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendUint64(u)
}

// AppendFloat32 appends a float32 value to the chunk.
func (c *Chunk) AppendFloat32(colIdx int, f float32) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendFloat32(f)
}

// AppendFloat64 appends a float64 value to the chunk.
func (c *Chunk) AppendFloat64(colIdx int, f float64) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendFloat64(f)
}

// AppendString appends a string value to the chunk.
func (c *Chunk) AppendString(colIdx int, str string) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendString(str)
}

// AppendBytes appends a bytes value to the chunk.
func (c *Chunk) AppendBytes(colIdx int, b []byte) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendBytes(b)
}

// AppendTime appends a Time value to the chunk.
func (c *Chunk) AppendTime(colIdx int, t types.Time) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendTime(t)
}

// AppendDuration appends a Duration value to the chunk.
// Fsp is ignored.
func (c *Chunk) AppendDuration(colIdx int, dur types.Duration) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendDuration(dur)
}

// AppendMyDecimal appends a MyDecimal value to the chunk.
func (c *Chunk) AppendMyDecimal(colIdx int, dec *types.MyDecimal) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendMyDecimal(dec)
}

// AppendEnum appends an Enum value to the chunk.
func (c *Chunk) AppendEnum(colIdx int, enum types.Enum) {
	c.appendSel(colIdx)
	c.columns[colIdx].appendNameValue(enum.Name, enum.Value)
}

// AppendSet appends a Set value to the chunk.
func (c *Chunk) AppendSet(colIdx int, set types.Set) {
	c.appendSel(colIdx)
	c.columns[colIdx].appendNameValue(set.Name, set.Value)
}

// AppendJSON appends a JSON value to the chunk.
func (c *Chunk) AppendJSON(colIdx int, j types.BinaryJSON) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendJSON(j)
}

// AppendVectorFloat32 appends a VectorFloat32 value to the chunk.
func (c *Chunk) AppendVectorFloat32(colIdx int, v types.VectorFloat32) {
	c.appendSel(colIdx)
	c.columns[colIdx].AppendVectorFloat32(v)
}

func (c *Chunk) appendSel(colIdx int) {
	if colIdx == 0 && c.sel != nil { // use column 0 as standard
		c.sel = append(c.sel, c.columns[0].length)
	}
}

// AppendDatum appends a datum into the chunk.
func (c *Chunk) AppendDatum(colIdx int, d *types.Datum) {
	switch d.Kind() {
	case types.KindNull:
		c.AppendNull(colIdx)
	case types.KindInt64:
		c.AppendInt64(colIdx, d.GetInt64())
	case types.KindUint64:
		c.AppendUint64(colIdx, d.GetUint64())
	case types.KindFloat32:
		c.AppendFloat32(colIdx, d.GetFloat32())
	case types.KindFloat64:
		c.AppendFloat64(colIdx, d.GetFloat64())
	case types.KindString, types.KindBytes, types.KindBinaryLiteral, types.KindRaw, types.KindMysqlBit:
		c.AppendBytes(colIdx, d.GetBytes())
	case types.KindMysqlDecimal:
		c.AppendMyDecimal(colIdx, d.GetMysqlDecimal())
	case types.KindMysqlDuration:
		c.AppendDuration(colIdx, d.GetMysqlDuration())
	case types.KindMysqlEnum:
		c.AppendEnum(colIdx, d.GetMysqlEnum())
	case types.KindMysqlSet:
		c.AppendSet(colIdx, d.GetMysqlSet())
	case types.KindMysqlTime:
		c.AppendTime(colIdx, d.GetMysqlTime())
	case types.KindMysqlJSON:
		c.AppendJSON(colIdx, d.GetMysqlJSON())
	case types.KindVectorFloat32:
		c.AppendVectorFloat32(colIdx, d.GetVectorFloat32())
	}
}

// Column returns the specific column.
func (c *Chunk) Column(colIdx int) *Column {
	return c.columns[colIdx]
}

// SetCol sets the colIdx Column to col and returns the old Column.
func (c *Chunk) SetCol(colIdx int, col *Column) *Column {
	if col == c.columns[colIdx] {
		return nil
	}
	old := c.columns[colIdx]
	c.columns[colIdx] = col
	return old
}

// Sel returns Sel of this Chunk.
func (c *Chunk) Sel() []int {
	return c.sel
}

// SetSel sets a Sel for this Chunk.
func (c *Chunk) SetSel(sel []int) {
	c.sel = sel
}

// CloneEmpty returns an empty chunk that has the same schema with current chunk
func (c *Chunk) CloneEmpty(maxCapacity int) *Chunk {
	return renewWithCapacity(c, maxCapacity, maxCapacity)
}

// Reconstruct removes all filtered rows in this Chunk.
func (c *Chunk) Reconstruct() {
	if c.sel == nil {
		return
	}
	for _, col := range c.columns {
		col.reconstruct(c.sel)
	}
	c.numVirtualRows = len(c.sel)
	c.sel = nil
}

// ToString returns all the values in a chunk.
func (c *Chunk) ToString(ft []*types.FieldType) string {
	buf := make([]byte, 0, c.NumRows()*2)
	for rowIdx := range c.NumRows() {
		row := c.GetRow(rowIdx)
		buf = append(buf, row.ToString(ft)...)
		buf = append(buf, '\n')
	}
	return string(buf)
}

// AppendRows appends multiple rows to the chunk.
func (c *Chunk) AppendRows(rows []Row) {
	c.AppendPartialRows(0, rows)
	c.numVirtualRows += len(rows)
}

// AppendPartialRows appends multiple rows to the chunk.
func (c *Chunk) AppendPartialRows(colOff int, rows []Row) {
	columns := c.columns[colOff:]
	for i, dstCol := range columns {
		for _, srcRow := range rows {
			if i == 0 {
				c.appendSel(colOff)
			}
			appendCellByCell(dstCol, srcRow.c.columns[i], srcRow.idx)
		}
	}
}

// Destroy is to destroy the Chunk and put Chunk into the pool
func (c *Chunk) Destroy(initCap int, fields []*types.FieldType) {
	putChunkFromPool(initCap, fields, c)
}
