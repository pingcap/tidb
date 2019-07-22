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

import (
	"reflect"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/hack"
)

// AppendDuration appends a duration value into this Column.
func (c *Column) AppendDuration(dur types.Duration) {
	c.AppendInt64(int64(dur.Duration))
}

// AppendMyDecimal appends a MyDecimal value into this Column.
func (c *Column) AppendMyDecimal(dec *types.MyDecimal) {
	*(*types.MyDecimal)(unsafe.Pointer(&c.elemBuf[0])) = *dec
	c.finishAppendFixed()
}

func (c *Column) appendNameValue(name string, val uint64) {
	var buf [8]byte
	*(*uint64)(unsafe.Pointer(&buf[0])) = val
	c.data = append(c.data, buf[:]...)
	c.data = append(c.data, name...)
	c.finishAppendVar()
}

// AppendJSON appends a BinaryJSON value into this Column.
func (c *Column) AppendJSON(j json.BinaryJSON) {
	c.data = append(c.data, j.TypeCode)
	c.data = append(c.data, j.Value...)
	c.finishAppendVar()
}

// Column stores one column of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
type Column struct {
	length     int
	nullCount  int
	nullBitmap []byte
	offsets    []int64
	data       []byte
	elemBuf    []byte
}

// NewColumn creates a new column with the specific length and capacity.
func NewColumn(ft *types.FieldType, cap int) *Column {
	typeSize := getFixedLen(ft)
	if typeSize == varElemLen {
		return newFixedLenColumn(typeSize, cap)
	} else {
		return newVarLenColumn(cap, nil)
	}
}

func (c *Column) isFixed() bool {
	return c.elemBuf != nil
}

// Reset resets this Column.
func (c *Column) Reset() {
	c.length = 0
	c.nullCount = 0
	c.nullBitmap = c.nullBitmap[:0]
	if len(c.offsets) > 0 {
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	c.data = c.data[:0]
}

func (c *Column) isNull(rowIdx int) bool {
	nullByte := c.nullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

func (c *Column) copyConstruct() *Column {
	newCol := &Column{length: c.length, nullCount: c.nullCount}
	newCol.nullBitmap = append(newCol.nullBitmap, c.nullBitmap...)
	newCol.offsets = append(newCol.offsets, c.offsets...)
	newCol.data = append(newCol.data, c.data...)
	newCol.elemBuf = append(newCol.elemBuf, c.elemBuf...)
	return newCol
}

func (c *Column) appendNullBitmap(notNull bool) {
	idx := c.length >> 3
	if idx >= len(c.nullBitmap) {
		c.nullBitmap = append(c.nullBitmap, 0)
	}
	if notNull {
		pos := uint(c.length) & 7
		c.nullBitmap[idx] |= byte(1 << pos)
	} else {
		c.nullCount++
	}
}

// appendMultiSameNullBitmap appends multiple same bit value to `nullBitMap`.
// notNull means not null.
// num means the number of bits that should be appended.
func (c *Column) appendMultiSameNullBitmap(notNull bool, num int) {
	numNewBytes := ((c.length + num + 7) >> 3) - len(c.nullBitmap)
	b := byte(0)
	if notNull {
		b = 0xff
	}
	for i := 0; i < numNewBytes; i++ {
		c.nullBitmap = append(c.nullBitmap, b)
	}
	if !notNull {
		c.nullCount += num
		return
	}
	// 1. Set all the remaining bits in the last slot of old c.numBitMap to 1.
	numRemainingBits := uint(c.length % 8)
	bitMask := byte(^((1 << numRemainingBits) - 1))
	c.nullBitmap[c.length/8] |= bitMask
	// 2. Set all the redundant bits in the last slot of new c.numBitMap to 0.
	numRedundantBits := uint(len(c.nullBitmap)*8 - c.length - num)
	bitMask = byte(1<<(8-numRedundantBits)) - 1
	c.nullBitmap[len(c.nullBitmap)-1] &= bitMask
}

// AppendNull appends a null value into this Column.
func (c *Column) AppendNull() {
	c.appendNullBitmap(false)
	if c.isFixed() {
		c.data = append(c.data, c.elemBuf...)
	} else {
		c.offsets = append(c.offsets, c.offsets[c.length])
	}
	c.length++
}

func (c *Column) finishAppendFixed() {
	c.data = append(c.data, c.elemBuf...)
	c.appendNullBitmap(true)
	c.length++
}

// AppendInt64 appends an int64 value into this Column.
func (c *Column) AppendInt64(i int64) {
	*(*int64)(unsafe.Pointer(&c.elemBuf[0])) = i
	c.finishAppendFixed()
}

// AppendUint64 appends a uint64 value into this Column.
func (c *Column) AppendUint64(u uint64) {
	*(*uint64)(unsafe.Pointer(&c.elemBuf[0])) = u
	c.finishAppendFixed()
}

// AppendFloat32 appends a float32 value into this Column.
func (c *Column) AppendFloat32(f float32) {
	*(*float32)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

// AppendFloat64 appends a float64 value into this Column.
func (c *Column) AppendFloat64(f float64) {
	*(*float64)(unsafe.Pointer(&c.elemBuf[0])) = f
	c.finishAppendFixed()
}

func (c *Column) finishAppendVar() {
	c.appendNullBitmap(true)
	c.offsets = append(c.offsets, int64(len(c.data)))
	c.length++
}

// AppendString appends a string value into this Column.
func (c *Column) AppendString(str string) {
	c.data = append(c.data, str...)
	c.finishAppendVar()
}

// AppendBytes appends a byte slice into this Column.
func (c *Column) AppendBytes(b []byte) {
	c.data = append(c.data, b...)
	c.finishAppendVar()
}

// AppendTime appends a time value into this Column.
func (c *Column) AppendTime(t types.Time) {
	writeTime(c.elemBuf, t)
	c.finishAppendFixed()
}

const (
	sizeInt64     = int(unsafe.Sizeof(int64(0)))
	sizeUint64    = int(unsafe.Sizeof(uint64(0)))
	sizeFloat32   = int(unsafe.Sizeof(float32(0)))
	sizeFloat64   = int(unsafe.Sizeof(float64(0)))
	sizeTime      = int(unsafe.Sizeof(types.Time{}))
	sizeDuration  = int(unsafe.Sizeof(types.Duration{}))
	sizeMyDecimal = int(unsafe.Sizeof(types.MyDecimal{}))
)

// Int64s returns an int64 slice stored in this Column.
func (c *Column) Int64s() []int64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&c.data))
	var res []int64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = c.length
	s.Cap = h.Cap / sizeInt64
	return res
}

// Uint64s returns a uint64 slice stored in this Column.
func (c *Column) Uint64s() []uint64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&c.data))
	var res []uint64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = c.length
	s.Cap = h.Cap / sizeUint64
	return res
}

// Float32s returns a float32 slice stored in this Column.
func (c *Column) Float32s() []float32 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&c.data))
	var res []float32
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = c.length
	s.Cap = h.Cap / sizeFloat32
	return res
}

// Float64s returns a float64 slice stored in this Column.
func (c *Column) Float64s() []float64 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&c.data))
	var res []float64
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = c.length
	s.Cap = h.Cap / sizeFloat64
	return res
}

// MyDecimals returns a MyDecimal slice stored in this Column.
func (c *Column) MyDecimals() []types.MyDecimal {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&c.data))
	var res []types.MyDecimal
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = c.length
	s.Cap = h.Cap / sizeMyDecimal
	return res
}

// GetString returns the string in the specific row.
func (c *Column) GetString(rowID int) string {
	return string(hack.String(c.data[c.offsets[rowID]:c.offsets[rowID+1]]))
}

// GetString returns the JSON in the specific row.
func (c *Column) GetJSON(rowID int) json.BinaryJSON {
	start := c.offsets[rowID]
	return json.BinaryJSON{TypeCode: c.data[start], Value: c.data[start+1 : c.offsets[rowID+1]]}
}

// GetBytes returns the byte slice in the specific row.
func (c *Column) GetBytes(rowID int) []byte {
	return c.data[c.offsets[rowID]:c.offsets[rowID+1]]
}

// GetEnum returns the Enum in the specific row.
func (c *Column) GetEnum(rowID int) types.Enum {
	name, val := c.getNameValue(rowID)
	return types.Enum{Name: name, Value: val}
}

// GetSet returns the Set in the specific row.
func (c *Column) GetSet(rowID int) types.Set {
	name, val := c.getNameValue(rowID)
	return types.Set{Name: name, Value: val}
}

// GetTime returns the Time in the specific row.
func (c *Column) GetTime(rowID int) types.Time {
	return readTime(c.data[rowID*16:])
}

// GetDuration returns the Duration in the specific row.
func (c *Column) GetDuration(rowID int, fillFsp int) types.Duration {
	dur := *(*int64)(unsafe.Pointer(&c.data[rowID*8]))
	return types.Duration{Duration: time.Duration(dur), Fsp: fillFsp}
}

// GetString returns the byte slice in the specific row.
func (c *Column) getNameValue(rowID int) (string, uint64) {
	start, end := c.offsets[rowID], c.offsets[rowID+1]
	if start == end {
		return "", 0
	}
	return string(hack.String(c.data[start+8 : end])), *(*uint64)(unsafe.Pointer(&c.data[start]))
}
