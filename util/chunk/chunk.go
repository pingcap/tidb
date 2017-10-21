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
	"unsafe"

	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
)

const (
	nullVal = 0
	objVal  = 1
)

// Chunk uses a single byte slice and a single int32 slice to store many rows of data.
// Values are appended in compact format and can be directly accessed without decoding.
// When the chunk is done processing, we can reuse the allocated memory by resetting it.
type Chunk struct {
	// nCols represents the number of columns in the chunk.
	nCols int

	// objMap stores complex object that cannot be directly converted to a byte slice.
	objMap map[int]interface{}

	// offsets contains values that starts at offset in data.
	// if value is null or object, the offset would be negative.
	offsets []int32

	// data stores all the values except for object types.
	data []byte
}

// NewChunk creates a new Chunk with number of columns.
func NewChunk(nCols int) *Chunk {
	return &Chunk{
		nCols: nCols,
	}
}

// Reset resets the chunk, so the memory it allocated can be reused.
// Make sure all the data in the chunk is not used anymore before you reuse this chunk.
func (c *Chunk) Reset(nCols int) {
	c.nCols = nCols
	c.objMap = nil
	c.offsets = c.offsets[:0]
	c.data = c.data[:0]
}

func (c *Chunk) initMap() {
	if c.objMap == nil {
		c.objMap = make(map[int]interface{})
	}
}

// NumCols returns the number of columns in the chunk.
func (c *Chunk) NumCols() int {
	return c.nCols
}

// NumRows returns the number of rows in the chunk.
func (c *Chunk) NumRows() int {
	return len(c.offsets) / c.nCols
}

// DataSize returns the size of the data, can be used to determine wether to finish appending the chunk.
func (c *Chunk) DataSize() int {
	return len(c.data)
}

// GetRow gets the Row in the chunk with the row index.
func (c *Chunk) GetRow(i int) Row {
	return Row{
		chunk: c,
		idx:   c.nCols * i,
	}
}

// ApppendRow appends a row to the chunk.
func (c *Chunk) AppendRow(row Row) {
	rowOffset := row.getRealOffset(row.idx)
	chunkOffset := int32(len(c.data))
	offsetDiff := chunkOffset - rowOffset
	endIdx := row.idx + c.nCols
	for i := row.idx; i < endIdx; i++ {
		colOff := row.chunk.offsets[i]
		if colOff >= 0 {
			c.offsets = append(c.offsets, colOff+offsetDiff)
		} else {
			c.offsets = append(c.offsets, colOff-offsetDiff)
			validOff := -(colOff + 1)
			if row.chunk.data[validOff] == objVal {
				c.initMap()
				c.objMap[len(c.offsets)] = row.chunk.objMap[i]
			}
		}
	}
	rowEnd := row.getRealOffset(row.idx + c.nCols)
	c.data = append(c.data, row.chunk.data[rowOffset:rowEnd]...)
}

// ApppendNull appends a null value to the chunk.
func (c *Chunk) AppendNull() {
	c.offsets = append(c.offsets, -int32(len(c.data))-1)
	c.data = append(c.data, nullVal)
}

// ApppendInt64 appends a int64 value to the chunk.
func (c *Chunk) AppendInt64(i int64) {
	c.offsets = append(c.offsets, int32(len(c.data)))
	var buf [8]byte
	*(*int64)(unsafe.Pointer(&buf[0])) = i
	c.data = append(c.data, buf[:]...)
}

// ApppendUint64 appends a uint64 value to the chunk.
func (c *Chunk) AppendUint64(u uint64) {
	c.offsets = append(c.offsets, int32(len(c.data)))
	var buf [8]byte
	*(*uint64)(unsafe.Pointer(&buf[0])) = u
	c.data = append(c.data, buf[:]...)
}

// ApppendFloat32 appends a float32 value to the chunk.
func (c *Chunk) AppendFloat32(f float32) {
	c.offsets = append(c.offsets, int32(len(c.data)))
	var buf [4]byte
	*(*float32)(unsafe.Pointer(&buf[0])) = f
	c.data = append(c.data, buf[:]...)
}

// ApppendFloat64 appends a float64 value to the chunk.
func (c *Chunk) AppendFloat64(f float64) {
	c.offsets = append(c.offsets, int32(len(c.data)))
	var buf [8]byte
	*(*float64)(unsafe.Pointer(&buf[0])) = f
	c.data = append(c.data, buf[:]...)
}

// ApppendString appends a string value to the chunk.
func (c *Chunk) AppendString(s string) {
	c.offsets = append(c.offsets, int32(len(c.data)))
	c.data = append(c.data, s...)
}

// ApppendBytes appends a bytes value to the chunk.
func (c *Chunk) AppendBytes(bin []byte) {
	c.offsets = append(c.offsets, int32(len(c.data)))
	c.data = append(c.data, bin...)
}

func (c *Chunk) appendObject(o interface{}) {
	c.initMap()
	c.objMap[len(c.offsets)] = o
	c.offsets = append(c.offsets, -int32(len(c.data))-1)
	c.data = append(c.data, objVal)
}

// AppendTime appends a Time value to the chunk.
// TODO: change the time structure so it can be directly written to memory.
func (c *Chunk) AppendTime(t types.Time) {
	c.appendObject(t)
}

// AppendDurations appends a Duration value to the chunk.
func (c *Chunk) AppendDuration(dur types.Duration) {
	c.offsets = append(c.offsets, int32(len(c.data)))
	var buf [16]byte
	*(*types.Duration)(unsafe.Pointer(&buf[0])) = dur
	c.data = append(c.data, buf[:]...)
}

// ApppendMyDecimal appends a MyDecimal value to the chunk.
func (c *Chunk) AppendMyDecimal(dec *types.MyDecimal) {
	c.offsets = append(c.offsets, int32(len(c.data)))
	var buf [types.MyDecimalStructSize]byte
	*(*types.MyDecimal)(unsafe.Pointer(&buf[0])) = *dec
	c.data = append(c.data, buf[:]...)
}

func (c *Chunk) appendNameValue(name string, val uint64) {
	c.offsets = append(c.offsets, int32(len(c.data)))
	var buf [8]byte
	*(*uint64)(unsafe.Pointer(&buf[0])) = val
	c.data = append(c.data, buf[:]...)
	c.data = append(c.data, name...)
}

// AppendEnum appends an Enum value to the chunk.
func (c *Chunk) AppendEnum(enum types.Enum) {
	c.appendNameValue(enum.Name, enum.Value)
}

// AppendSet appends a Set value to the chunk.
func (c *Chunk) AppendSet(set types.Set) {
	c.appendNameValue(set.Name, set.Value)
}

// AppendJSON appends a JSON value to the chunk.
func (c *Chunk) AppendJSON(j json.JSON) {
	c.appendObject(j)
}

// Row represent a row in a chunk, has methods to access the values.
type Row struct {
	chunk *Chunk
	idx   int
}

// GetInt64 returns the int64 value and isNull with the colIdx.
func (r Row) GetInt64(colIdx int) (int64, bool) {
	off := r.chunk.offsets[r.idx+colIdx]
	if off < 0 {
		return 0, true
	}
	return *(*int64)(unsafe.Pointer(&r.chunk.data[off])), false
}

// GetUint64 returns the uint64 value and isNull with the colIdx.
func (r Row) GetUint64(colIdx int) (uint64, bool) {
	off := r.chunk.offsets[r.idx+colIdx]
	if off < 0 {
		return 0, true
	}
	return *(*uint64)(unsafe.Pointer(&r.chunk.data[off])), false
}

// GetFloat32 returns the float64 value and isNull with the colIdx.
func (r Row) GetFloat32(colIdx int) (float32, bool) {
	off := r.chunk.offsets[r.idx+colIdx]
	if off < 0 {
		return 0, true
	}
	return *(*float32)(unsafe.Pointer(&r.chunk.data[off])), false
}

// GetFloat64 returns the float64 value and isNull with the colIdx.
func (r Row) GetFloat64(colIdx int) (float64, bool) {
	off := r.chunk.offsets[r.idx+colIdx]
	if off < 0 {
		return 0, true
	}
	return *(*float64)(unsafe.Pointer(&r.chunk.data[off])), false
}

// getRealOffset gets the real offset at offIdx.
func (r Row) getRealOffset(offIdx int) int32 {
	if offIdx == len(r.chunk.offsets) {
		return int32(len(r.chunk.data))
	}
	off := r.chunk.offsets[offIdx]
	if off < 0 {
		return -(off + 1)
	}
	return off
}

// GetString returns the string value and isNull with the colIdx.
func (r Row) GetString(colIdx int) (string, bool) {
	offIdx := colIdx + r.idx
	off := r.chunk.offsets[offIdx]
	if off < 0 {
		return "", true
	}
	end := r.getRealOffset(offIdx + 1)
	return hack.String(r.chunk.data[off:end]), false
}

// GetBytes returns the bytes value and isNull with the colIdx.
func (r Row) GetBytes(colIdx int) ([]byte, bool) {
	offIdx := r.idx + colIdx
	off := r.chunk.offsets[offIdx]
	if off < 0 {
		return nil, true
	}
	end := r.getRealOffset(offIdx + 1)
	return r.chunk.data[off:end], false
}

// GetTime returns the Time value and is isNull with the colIdx.
func (r Row) GetTime(colIdx int) (types.Time, bool) {
	if r.chunk.offsets[r.idx+colIdx] < 0 {
		t, ok := r.chunk.objMap[r.idx+colIdx].(types.Time)
		return t, !ok
	}
	return types.Time{}, true
}

// GetDuration returns the Duration value and isNull with the colIdx.
func (r Row) GetDuration(colIdx int) (types.Duration, bool) {
	offIdx := colIdx + r.idx
	off := r.chunk.offsets[offIdx]
	if off < 0 {
		return types.Duration{}, true
	}
	return *(*types.Duration)(unsafe.Pointer(&r.chunk.data[off])), false
}

func (r Row) getNameValue(colIdx int) (string, uint64, bool) {
	offIdx := colIdx + r.idx
	off := r.chunk.offsets[r.idx+colIdx]
	if off < 0 {
		return "", 0, true
	}
	val := *(*uint64)(unsafe.Pointer(&r.chunk.data[off]))
	end := r.getRealOffset(offIdx + 1)
	name := hack.String(r.chunk.data[off+8 : end])
	return name, val, false
}

// GetEnum returns the Enum value and isNull with the colIdx.
func (r Row) GetEnum(colIdx int) (types.Enum, bool) {
	name, val, isNull := r.getNameValue(colIdx)
	return types.Enum{Name: name, Value: val}, isNull
}

// GetSet returns the Set value and isNull with the colIdx.
func (r Row) GetSet(colIdx int) (types.Set, bool) {
	name, val, isNull := r.getNameValue(colIdx)
	return types.Set{Name: name, Value: val}, isNull
}

// GetMyDecimal returns the MyDecimal value and isNull with the colIdx.
func (r Row) GetMyDecimal(colIdx int) (*types.MyDecimal, bool) {
	offIdx := colIdx + r.idx
	off := r.chunk.offsets[offIdx]
	if off < 0 {
		return nil, true
	}
	return (*types.MyDecimal)(unsafe.Pointer(&r.chunk.data[off])), false
}

// GetJSON returns the JSON value and isNull with the colIdx.
func (r Row) GetJSON(colIdx int) (json.JSON, bool) {
	if r.chunk.offsets[r.idx+colIdx] < 0 {
		js, ok := r.chunk.objMap[r.idx+colIdx].(json.JSON)
		return js, !ok
	}
	return json.JSON{}, true
}
