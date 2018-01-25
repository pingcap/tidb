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

import (
	"encoding/binary"
	"unsafe"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
)

var _ types.Row = Row{}

// Chunk stores multiple rows of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
// Values are appended in compact format and can be directly accessed without decoding.
// When the chunk is done processing, we can reuse the allocated memory by resetting it.
type Chunk struct {
	columns []*column
	// numVirtualRows indicates the number of virtual rows, witch have zero columns.
	// It is used only when this Chunk doesn't hold any data, i.e. "len(columns)==0".
	numVirtualRows int
}

// Capacity constants.
const (
	InitialCapacity = 32
)

// NewChunk creates a new chunk with field types.
func NewChunk(fields []*types.FieldType) *Chunk {
	return NewChunkWithCapacity(fields, InitialCapacity)
}

// NewChunkWithCapacity creates a new chunk with field types and capacity.
func NewChunkWithCapacity(fields []*types.FieldType, cap int) *Chunk {
	chk := new(Chunk)
	chk.columns = make([]*column, 0, len(fields))
	chk.numVirtualRows = 0
	for _, f := range fields {
		chk.addColumnByFieldType(f, cap)
	}
	return chk
}

// MemoryUsage returns the total memory usage of a Chunk in B.
// We ignore the size of column.length and column.nullCount
// since they have little effect of the total memory usage.
func (c *Chunk) MemoryUsage() (sum int64) {
	for _, col := range c.columns {
		curColMemUsage := int64(unsafe.Sizeof(*col)) + int64(cap(col.nullBitmap)) + int64(cap(col.offsets)*4) + int64(cap(col.data)) + int64(cap(col.elemBuf))
		sum += curColMemUsage
	}
	return
}

// addFixedLenColumn adds a fixed length column with elemLen and initial data capacity.
func (c *Chunk) addFixedLenColumn(elemLen, initCap int) {
	c.columns = append(c.columns, &column{
		elemBuf:    make([]byte, elemLen),
		data:       make([]byte, 0, initCap*elemLen),
		nullBitmap: make([]byte, 0, initCap>>3),
	})
}

// addVarLenColumn adds a variable length column with initial data capacity.
func (c *Chunk) addVarLenColumn(initCap int) {
	c.columns = append(c.columns, &column{
		offsets:    make([]int32, 1, initCap+1),
		data:       make([]byte, 0, initCap*4),
		nullBitmap: make([]byte, 0, initCap>>3),
	})
}

// addColumnByFieldType adds a column by field type.
func (c *Chunk) addColumnByFieldType(fieldTp *types.FieldType, initCap int) {
	switch fieldTp.Tp {
	case mysql.TypeFloat:
		c.addFixedLenColumn(4, initCap)
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeDouble, mysql.TypeYear:
		c.addFixedLenColumn(8, initCap)
	case mysql.TypeDuration:
		c.addFixedLenColumn(16, initCap)
	case mysql.TypeNewDecimal:
		c.addFixedLenColumn(types.MyDecimalStructSize, initCap)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		c.addFixedLenColumn(16, initCap)
	default:
		c.addVarLenColumn(initCap)
	}
}

// MakeRef makes column in "dstColIdx" reference to column in "srcColIdx".
func (c *Chunk) MakeRef(srcColIdx, dstColIdx int) {
	c.columns[dstColIdx] = c.columns[srcColIdx]
}

// SwapColumn swaps column "c.columns[colIdx]" with column "other.columns[otherIdx]".
func (c *Chunk) SwapColumn(colIdx int, other *Chunk, otherIdx int) {
	c.columns[colIdx], other.columns[otherIdx] = other.columns[otherIdx], c.columns[colIdx]
}

// SwapColumns swaps columns with another Chunk.
func (c *Chunk) SwapColumns(other *Chunk) {
	c.columns, other.columns = other.columns, c.columns
	c.numVirtualRows, other.numVirtualRows = other.numVirtualRows, c.numVirtualRows
}

// SetNumVirtualRows sets the virtual row number for a Chunk.
// It should only be used when there exists no column in the Chunk.
func (c *Chunk) SetNumVirtualRows(numVirtualRows int) {
	c.numVirtualRows = numVirtualRows
}

// Reset resets the chunk, so the memory it allocated can be reused.
// Make sure all the data in the chunk is not used anymore before you reuse this chunk.
func (c *Chunk) Reset() {
	for _, c := range c.columns {
		c.reset()
	}
	c.numVirtualRows = 0
}

// NumCols returns the number of columns in the chunk.
func (c *Chunk) NumCols() int {
	return len(c.columns)
}

// NumRows returns the number of rows in the chunk.
func (c *Chunk) NumRows() int {
	if c.NumCols() == 0 {
		return c.numVirtualRows
	}
	return c.columns[0].length
}

// GetRow gets the Row in the chunk with the row index.
func (c *Chunk) GetRow(idx int) Row {
	return Row{c: c, idx: idx}
}

// AppendRow appends a row to the chunk.
func (c *Chunk) AppendRow(row Row) {
	c.AppendPartialRow(0, row)
	c.numVirtualRows++
}

// AppendPartialRow appends a row to the chunk.
func (c *Chunk) AppendPartialRow(colIdx int, row Row) {
	for i, rowCol := range row.c.columns {
		chkCol := c.columns[colIdx+i]
		chkCol.appendNullBitmap(!rowCol.isNull(row.idx))
		if rowCol.isFixed() {
			elemLen := len(rowCol.elemBuf)
			offset := row.idx * elemLen
			chkCol.data = append(chkCol.data, rowCol.data[offset:offset+elemLen]...)
		} else {
			start, end := rowCol.offsets[row.idx], rowCol.offsets[row.idx+1]
			chkCol.data = append(chkCol.data, rowCol.data[start:end]...)
			chkCol.offsets = append(chkCol.offsets, int32(len(chkCol.data)))
		}
		chkCol.length++
	}
}

// Append appends rows in [begin, end) in another Chunk to a Chunk.
func (c *Chunk) Append(other *Chunk, begin, end int) {
	for colID, src := range other.columns {
		dst := c.columns[colID]
		if src.isFixed() {
			elemLen := len(src.elemBuf)
			dst.data = append(dst.data, src.data[begin*elemLen:end*elemLen]...)
		} else {
			beginOffset, endOffset := src.offsets[begin], src.offsets[end]
			dst.data = append(dst.data, src.data[beginOffset:endOffset]...)
			for i := begin; i < end; i++ {
				dst.offsets = append(dst.offsets, dst.offsets[len(dst.offsets)-1]+src.offsets[i+1]-src.offsets[i])
			}
		}
		for i := begin; i < end; i++ {
			dst.appendNullBitmap(!src.isNull(i))
			dst.length++
		}
	}
	c.numVirtualRows += end - begin
}

// TruncateTo truncates rows from tail to head in a Chunk to "numRows" rows.
func (c *Chunk) TruncateTo(numRows int) {
	for _, col := range c.columns {
		if col.isFixed() {
			elemLen := len(col.elemBuf)
			col.data = col.data[:numRows*elemLen]
		} else {
			col.data = col.data[:col.offsets[numRows]]
			col.offsets = col.offsets[:numRows+1]
		}
		for i := numRows; i < col.length; i++ {
			if col.isNull(i) {
				col.nullCount--
			}
		}
		col.length = numRows
		col.nullBitmap = col.nullBitmap[:(col.length>>3)+1]
	}
	c.numVirtualRows = numRows
}

// AppendNull appends a null value to the chunk.
func (c *Chunk) AppendNull(colIdx int) {
	c.columns[colIdx].appendNull()
}

// AppendInt64 appends a int64 value to the chunk.
func (c *Chunk) AppendInt64(colIdx int, i int64) {
	c.columns[colIdx].appendInt64(i)
}

// AppendUint64 appends a uint64 value to the chunk.
func (c *Chunk) AppendUint64(colIdx int, u uint64) {
	c.columns[colIdx].appendUint64(u)
}

// AppendFloat32 appends a float32 value to the chunk.
func (c *Chunk) AppendFloat32(colIdx int, f float32) {
	c.columns[colIdx].appendFloat32(f)
}

// AppendFloat64 appends a float64 value to the chunk.
func (c *Chunk) AppendFloat64(colIdx int, f float64) {
	c.columns[colIdx].appendFloat64(f)
}

// AppendString appends a string value to the chunk.
func (c *Chunk) AppendString(colIdx int, str string) {
	c.columns[colIdx].appendString(str)
}

// AppendBytes appends a bytes value to the chunk.
func (c *Chunk) AppendBytes(colIdx int, b []byte) {
	c.columns[colIdx].appendBytes(b)
}

// AppendTime appends a Time value to the chunk.
// TODO: change the time structure so it can be directly written to memory.
func (c *Chunk) AppendTime(colIdx int, t types.Time) {
	c.columns[colIdx].appendTime(t)
}

// AppendDuration appends a Duration value to the chunk.
func (c *Chunk) AppendDuration(colIdx int, dur types.Duration) {
	c.columns[colIdx].appendDuration(dur)
}

// AppendMyDecimal appends a MyDecimal value to the chunk.
func (c *Chunk) AppendMyDecimal(colIdx int, dec *types.MyDecimal) {
	c.columns[colIdx].appendMyDecimal(dec)
}

// AppendEnum appends an Enum value to the chunk.
func (c *Chunk) AppendEnum(colIdx int, enum types.Enum) {
	c.columns[colIdx].appendNameValue(enum.Name, enum.Value)
}

// AppendSet appends a Set value to the chunk.
func (c *Chunk) AppendSet(colIdx int, set types.Set) {
	c.columns[colIdx].appendNameValue(set.Name, set.Value)
}

// AppendJSON appends a JSON value to the chunk.
func (c *Chunk) AppendJSON(colIdx int, j json.BinaryJSON) {
	c.columns[colIdx].appendJSON(j)
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
	}
}

type column struct {
	length     int
	nullCount  int
	nullBitmap []byte
	offsets    []int32
	data       []byte
	elemBuf    []byte
}

func (c *column) isFixed() bool {
	return c.elemBuf != nil
}

func (c *column) reset() {
	c.length = 0
	c.nullCount = 0
	c.nullBitmap = c.nullBitmap[:0]
	if len(c.offsets) > 0 {
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	c.data = c.data[:0]
}

func (c *column) isNull(rowIdx int) bool {
	nullByte := c.nullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

func (c *column) appendNullBitmap(on bool) {
	idx := c.length >> 3
	if idx >= len(c.nullBitmap) {
		c.nullBitmap = append(c.nullBitmap, 0)
	}
	if on {
		pos := uint(c.length) & 7
		c.nullBitmap[idx] |= byte(1 << pos)
	} else {
		c.nullCount++
	}
}

func (c *column) appendNull() {
	c.appendNullBitmap(false)
	if c.isFixed() {
		c.data = append(c.data, c.elemBuf...)
	} else {
		c.offsets = append(c.offsets, c.offsets[c.length])
	}
	c.length++
}

func (c *column) finishAppendFixed() {
	c.data = append(c.data, c.elemBuf...)
	c.appendNullBitmap(true)
	c.length++
}

func (c *column) appendInt64(i int64) {
	*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = i
	c.finishAppendFixed()
}

func (c *column) appendUint64(u uint64) {
	*(*uint64)(unsafe.Pointer(&c.elemBuf[0])) = u
	c.finishAppendFixed()
}

func (c *column) appendFloat32(f float32) {
	*(*float32)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *column) appendFloat64(f float64) {
	*(*float64)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *column) finishAppendVar() {
	c.appendNullBitmap(true)
	c.offsets = append(c.offsets, int32(len(c.data)))
	c.length++
}

func (c *column) appendString(str string) {
	c.data = append(c.data, str...)
	c.finishAppendVar()
}

func (c *column) appendBytes(b []byte) {
	c.data = append(c.data, b...)
	c.finishAppendVar()
}

func (c *column) appendTime(t types.Time) {
	writeTime(c.elemBuf, t)
	c.finishAppendFixed()
}

func writeTime(buf []byte, t types.Time) {
	binary.BigEndian.PutUint16(buf, uint16(t.Time.Year()))
	buf[2] = uint8(t.Time.Month())
	buf[3] = uint8(t.Time.Day())
	buf[4] = uint8(t.Time.Hour())
	buf[5] = uint8(t.Time.Minute())
	buf[6] = uint8(t.Time.Second())
	binary.BigEndian.PutUint32(buf[8:], uint32(t.Time.Microsecond()))
	buf[12] = t.Type
	buf[13] = uint8(t.Fsp)
}

func (c *column) appendDuration(dur types.Duration) {
	*(*types.Duration)(unsafe.Pointer(&c.elemBuf[0])) = dur
	c.finishAppendFixed()
}

func (c *column) appendMyDecimal(dec *types.MyDecimal) {
	*(*types.MyDecimal)(unsafe.Pointer(&c.elemBuf[0])) = *dec
	c.finishAppendFixed()
}

func (c *column) appendNameValue(name string, val uint64) {
	var buf [8]byte
	*(*uint64)(unsafe.Pointer(&buf[0])) = val
	c.data = append(c.data, buf[:]...)
	c.data = append(c.data, name...)
	c.finishAppendVar()
}

func (c *column) appendJSON(j json.BinaryJSON) {
	c.data = append(c.data, j.TypeCode)
	c.data = append(c.data, j.Value...)
	c.finishAppendVar()
}

// Row represents a row of data, can be used to assess values.
type Row struct {
	c   *Chunk
	idx int
}

// Idx returns the row index of Chunk.
func (r Row) Idx() int {
	return r.idx
}

// Len returns the number of values in the row.
func (r Row) Len() int {
	return r.c.NumCols()
}

// GetInt64 returns the int64 value with the colIdx.
func (r Row) GetInt64(colIdx int) int64 {
	col := r.c.columns[colIdx]
	return *(*int64)(unsafe.Pointer(&col.data[r.idx*8]))
}

// GetUint64 returns the uint64 value with the colIdx.
func (r Row) GetUint64(colIdx int) uint64 {
	col := r.c.columns[colIdx]
	return *(*uint64)(unsafe.Pointer(&col.data[r.idx*8]))
}

// GetFloat32 returns the float64 value with the colIdx.
func (r Row) GetFloat32(colIdx int) float32 {
	col := r.c.columns[colIdx]
	return *(*float32)(unsafe.Pointer(&col.data[r.idx*4]))
}

// GetFloat64 returns the float64 value with the colIdx.
func (r Row) GetFloat64(colIdx int) float64 {
	col := r.c.columns[colIdx]
	return *(*float64)(unsafe.Pointer(&col.data[r.idx*8]))
}

// GetString returns the string value with the colIdx.
func (r Row) GetString(colIdx int) string {
	col := r.c.columns[colIdx]
	start, end := col.offsets[r.idx], col.offsets[r.idx+1]
	return hack.String(col.data[start:end])
}

// GetBytes returns the bytes value with the colIdx.
func (r Row) GetBytes(colIdx int) []byte {
	col := r.c.columns[colIdx]
	start, end := col.offsets[r.idx], col.offsets[r.idx+1]
	return col.data[start:end]
}

// GetTime returns the Time value with the colIdx.
// TODO: use Time structure directly.
func (r Row) GetTime(colIdx int) types.Time {
	col := r.c.columns[colIdx]
	return readTime(col.data[r.idx*16:])
}

func readTime(buf []byte) types.Time {
	year := int(binary.BigEndian.Uint16(buf))
	month := int(buf[2])
	day := int(buf[3])
	hour := int(buf[4])
	minute := int(buf[5])
	second := int(buf[6])
	microseconds := int(binary.BigEndian.Uint32(buf[8:]))
	tp := buf[12]
	fsp := int(buf[13])
	return types.Time{
		Time: types.FromDate(year, month, day, hour, minute, second, microseconds),
		Type: tp,
		Fsp:  fsp,
	}
}

// GetDuration returns the Duration value with the colIdx.
func (r Row) GetDuration(colIdx int) types.Duration {
	col := r.c.columns[colIdx]
	return *(*types.Duration)(unsafe.Pointer(&col.data[r.idx*16]))
}

func (r Row) getNameValue(colIdx int) (string, uint64) {
	col := r.c.columns[colIdx]
	start, end := col.offsets[r.idx], col.offsets[r.idx+1]
	if start == end {
		return "", 0
	}
	val := *(*uint64)(unsafe.Pointer(&col.data[start]))
	name := hack.String(col.data[start+8 : end])
	return name, val
}

// GetEnum returns the Enum value with the colIdx.
func (r Row) GetEnum(colIdx int) types.Enum {
	name, val := r.getNameValue(colIdx)
	return types.Enum{Name: name, Value: val}
}

// GetSet returns the Set value with the colIdx.
func (r Row) GetSet(colIdx int) types.Set {
	name, val := r.getNameValue(colIdx)
	return types.Set{Name: name, Value: val}
}

// GetMyDecimal returns the MyDecimal value with the colIdx.
func (r Row) GetMyDecimal(colIdx int) *types.MyDecimal {
	col := r.c.columns[colIdx]
	return (*types.MyDecimal)(unsafe.Pointer(&col.data[r.idx*types.MyDecimalStructSize]))
}

// GetJSON returns the JSON value with the colIdx.
func (r Row) GetJSON(colIdx int) json.BinaryJSON {
	col := r.c.columns[colIdx]
	start, end := col.offsets[r.idx], col.offsets[r.idx+1]
	return json.BinaryJSON{TypeCode: col.data[start], Value: col.data[start+1 : end]}
}

// GetDatumRow converts chunk.Row to types.DatumRow.
// Keep in mind that GetDatumRow has a reference to r.c, which is a chunk,
// this function works only if the underlying chunk is valid or unchanged.
func (r Row) GetDatumRow(fields []*types.FieldType) types.DatumRow {
	datumRow := make(types.DatumRow, 0, r.c.NumCols())
	for colIdx := 0; colIdx < r.c.NumCols(); colIdx++ {
		datum := r.GetDatum(colIdx, fields[colIdx])
		datumRow = append(datumRow, datum)
	}
	return datumRow
}

// GetDatum implements the types.Row interface.
func (r Row) GetDatum(colIdx int, tp *types.FieldType) types.Datum {
	var d types.Datum
	switch tp.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		if !r.IsNull(colIdx) {
			if mysql.HasUnsignedFlag(tp.Flag) {
				d.SetUint64(r.GetUint64(colIdx))
			} else {
				d.SetInt64(r.GetInt64(colIdx))
			}
		}
	case mysql.TypeFloat:
		if !r.IsNull(colIdx) {
			d.SetFloat32(r.GetFloat32(colIdx))
		}
	case mysql.TypeDouble:
		if !r.IsNull(colIdx) {
			d.SetFloat64(r.GetFloat64(colIdx))
		}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		if !r.IsNull(colIdx) {
			d.SetBytes(r.GetBytes(colIdx))
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		if !r.IsNull(colIdx) {
			d.SetMysqlTime(r.GetTime(colIdx))
		}
	case mysql.TypeDuration:
		if !r.IsNull(colIdx) {
			d.SetMysqlDuration(r.GetDuration(colIdx))
		}
	case mysql.TypeNewDecimal:
		if !r.IsNull(colIdx) {
			d.SetMysqlDecimal(r.GetMyDecimal(colIdx))
			d.SetLength(tp.Flen)
			d.SetFrac(tp.Decimal)
		}
	case mysql.TypeEnum:
		if !r.IsNull(colIdx) {
			d.SetMysqlEnum(r.GetEnum(colIdx))
		}
	case mysql.TypeSet:
		if !r.IsNull(colIdx) {
			d.SetMysqlSet(r.GetSet(colIdx))
		}
	case mysql.TypeBit:
		if !r.IsNull(colIdx) {
			d.SetMysqlBit(r.GetBytes(colIdx))
		}
	case mysql.TypeJSON:
		if !r.IsNull(colIdx) {
			d.SetMysqlJSON(r.GetJSON(colIdx))
		}
	}
	return d
}

// IsNull implements the types.Row interface.
func (r Row) IsNull(colIdx int) bool {
	return r.c.columns[colIdx].isNull(r.idx)
}
