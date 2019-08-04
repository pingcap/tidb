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
	"math/bits"
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

// AppendSet appends a Set value into this Column.
func (c *Column) AppendSet(set types.Set) {
	c.appendNameValue(set.Name, set.Value)
}

// Column stores one column of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
type Column struct {
	length     int
	nullBitmap []byte // bit 0 is null, 1 is not null
	offsets    []int64
	data       []byte
	elemBuf    []byte
}

// NewColumn creates a new column with the specific length and capacity.
func NewColumn(ft *types.FieldType, cap int) *Column {
	return newColumn(getFixedLen(ft), cap)
}

func newColumn(typeSize, cap int) *Column {
	var col *Column
	if typeSize == varElemLen {
		col = newVarLenColumn(cap, nil)
	} else {
		col = newFixedLenColumn(typeSize, cap)
	}
	return col
}

func (c *Column) typeSize() int {
	if len(c.elemBuf) > 0 {
		return len(c.elemBuf)
	}
	return varElemLen
}

func (c *Column) isFixed() bool {
	return c.elemBuf != nil
}

// Reset resets this Column.
func (c *Column) Reset() {
	c.length = 0
	c.nullBitmap = c.nullBitmap[:0]
	if len(c.offsets) > 0 {
		// The first offset is always 0, it makes slicing the data easier, we need to keep it.
		c.offsets = c.offsets[:1]
	}
	c.data = c.data[:0]
}

// IsNull returns if this row is null.
func (c *Column) IsNull(rowIdx int) bool {
	nullByte := c.nullBitmap[rowIdx/8]
	return nullByte&(1<<(uint(rowIdx)&7)) == 0
}

// CopyConstruct copies this Column to dst.
// If dst is nil, it creates a new Column and returns it.
func (c *Column) CopyConstruct(dst *Column) *Column {
	if dst != nil {
		dst.length = c.length
		dst.nullBitmap = append(dst.nullBitmap[:0], c.nullBitmap...)
		dst.offsets = append(dst.offsets[:0], c.offsets...)
		dst.data = append(dst.data[:0], c.data...)
		dst.elemBuf = append(dst.elemBuf[:0], c.elemBuf...)
		return dst
	}
	newCol := &Column{length: c.length}
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

// AppendEnum appends a Enum value into this Column.
func (c *Column) AppendEnum(enum types.Enum) {
	c.appendNameValue(enum.Name, enum.Value)
}

const (
	sizeInt64     = int(unsafe.Sizeof(int64(0)))
	sizeUint64    = int(unsafe.Sizeof(uint64(0)))
	sizeFloat32   = int(unsafe.Sizeof(float32(0)))
	sizeFloat64   = int(unsafe.Sizeof(float64(0)))
	sizeMyDecimal = int(unsafe.Sizeof(types.MyDecimal{}))
)

// preAlloc allocates space for a fixed-length-type slice and resets all slots to null.
func (c *Column) preAlloc(length, typeSize int) {
	nData := length * typeSize
	if len(c.data) >= nData {
		c.data = c.data[:nData]
	} else {
		c.data = make([]byte, nData)
	}

	nBitmap := (length + 7) >> 3
	if len(c.nullBitmap) >= nBitmap {
		c.nullBitmap = c.nullBitmap[:nBitmap]
		for i := range c.nullBitmap {
			// resets all slots to null.
			c.nullBitmap[i] = 0
		}
	} else {
		c.nullBitmap = make([]byte, nBitmap)
	}

	if c.elemBuf != nil && len(c.elemBuf) >= typeSize {
		c.elemBuf = c.elemBuf[:typeSize]
	} else {
		c.elemBuf = make([]byte, typeSize)
	}

	c.length = length
}

// SetNull sets the rowIdx to null.
func (c *Column) SetNull(rowIdx int, isNull bool) {
	if isNull {
		c.nullBitmap[rowIdx>>3] &= ^(1 << uint(rowIdx&7))
	} else {
		c.nullBitmap[rowIdx>>3] |= 1 << uint(rowIdx&7)
	}
}

// SetNulls sets rows in [begin, end) to null.
func (c *Column) SetNulls(begin, end int, isNull bool) {
	i := ((begin + 7) >> 3) << 3
	for ; begin < i && begin < end; begin++ {
		c.SetNull(begin, isNull)
	}
	var v uint8
	if !isNull {
		v = (1 << 8) - 1
	}
	for ; begin+8 <= end; begin += 8 {
		c.nullBitmap[begin>>3] = v
	}
	for ; begin < end; begin++ {
		c.SetNull(begin, isNull)
	}
}

// nullCount returns the number of nulls in this Column.
func (c *Column) nullCount() int {
	var cnt, i int
	for ; i+8 <= c.length; i += 8 {
		// 0 is null and 1 is not null
		cnt += 8 - bits.OnesCount8(uint8(c.nullBitmap[i>>3]))
	}
	for ; i < c.length; i++ {
		if c.IsNull(i) {
			cnt++
		}
	}
	return cnt
}

// PreAllocInt64 allocates space for an int64 slice and resets all slots to null.
func (c *Column) PreAllocInt64(length int) {
	c.preAlloc(length, sizeInt64)
}

// PreAllocUint64 allocates space for a uint64 slice and resets all slots to null.
func (c *Column) PreAllocUint64(length int) {
	c.preAlloc(length, sizeUint64)
}

// PreAllocFloat32 allocates space for a float32 slice and resets all slots to null.
func (c *Column) PreAllocFloat32(length int) {
	c.preAlloc(length, sizeFloat32)
}

// PreAllocFloat64 allocates space for a float64 slice and resets all slots to null.
func (c *Column) PreAllocFloat64(length int) {
	c.preAlloc(length, sizeFloat64)
}

// PreAllocDecimal allocates space for a decimal slice and resets all slots to null.
func (c *Column) PreAllocDecimal(length int) {
	c.preAlloc(length, sizeMyDecimal)
}

func (c *Column) castSliceHeader(header *reflect.SliceHeader, typeSize int) {
	header.Data = uintptr(unsafe.Pointer(&c.data[0]))
	header.Len = c.length
	header.Cap = cap(c.data) / typeSize
}

// Int64s returns an int64 slice stored in this Column.
func (c *Column) Int64s() []int64 {
	var res []int64
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeInt64)
	return res
}

// Uint64s returns a uint64 slice stored in this Column.
func (c *Column) Uint64s() []uint64 {
	var res []uint64
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeUint64)
	return res
}

// Float32s returns a float32 slice stored in this Column.
func (c *Column) Float32s() []float32 {
	var res []float32
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeFloat32)
	return res
}

// Float64s returns a float64 slice stored in this Column.
func (c *Column) Float64s() []float64 {
	var res []float64
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeFloat64)
	return res
}

// Decimals returns a MyDecimal slice stored in this Column.
func (c *Column) Decimals() []types.MyDecimal {
	var res []types.MyDecimal
	c.castSliceHeader((*reflect.SliceHeader)(unsafe.Pointer(&res)), sizeMyDecimal)
	return res
}

// GetInt64 returns the int64 in the specific row.
func (c *Column) GetInt64(rowID int) int64 {
	return *(*int64)(unsafe.Pointer(&c.data[rowID*8]))
}

// GetUint64 returns the uint64 in the specific row.
func (c *Column) GetUint64(rowID int) uint64 {
	return *(*uint64)(unsafe.Pointer(&c.data[rowID*8]))
}

// GetFloat32 returns the float32 in the specific row.
func (c *Column) GetFloat32(rowID int) float32 {
	return *(*float32)(unsafe.Pointer(&c.data[rowID*4]))
}

// GetFloat64 returns the float64 in the specific row.
func (c *Column) GetFloat64(rowID int) float64 {
	return *(*float64)(unsafe.Pointer(&c.data[rowID*8]))
}

// GetDecimal returns the decimal in the specific row.
func (c *Column) GetDecimal(rowID int) *types.MyDecimal {
	return (*types.MyDecimal)(unsafe.Pointer(&c.data[rowID*types.MyDecimalStructSize]))
}

// GetString returns the string in the specific row.
func (c *Column) GetString(rowID int) string {
	return string(hack.String(c.data[c.offsets[rowID]:c.offsets[rowID+1]]))
}

// GetJSON returns the JSON in the specific row.
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

func (c *Column) getNameValue(rowID int) (string, uint64) {
	start, end := c.offsets[rowID], c.offsets[rowID+1]
	if start == end {
		return "", 0
	}
	return string(hack.String(c.data[start+8 : end])), *(*uint64)(unsafe.Pointer(&c.data[start]))
}

// reconstruct reconstructs this Column by removing all filtered rows in it according to sel.
func (c *Column) reconstruct(sel []int) {
	if sel == nil {
		return
	}
	if c.isFixed() {
		elemLen := len(c.elemBuf)
		for dst, src := range sel {
			idx := dst >> 3
			pos := uint16(dst & 7)
			if c.IsNull(src) {
				c.nullBitmap[idx] &= ^byte(1 << pos)
			} else {
				copy(c.data[dst*elemLen:dst*elemLen+elemLen], c.data[src*elemLen:src*elemLen+elemLen])
				c.nullBitmap[idx] |= byte(1 << pos)
			}
		}
		c.data = c.data[:len(sel)*elemLen]
	} else {
		tail := 0
		for dst, src := range sel {
			idx := dst >> 3
			pos := uint(dst & 7)
			if c.IsNull(src) {
				c.nullBitmap[idx] &= ^byte(1 << pos)
				c.offsets[dst+1] = int64(tail)
			} else {
				start, end := c.offsets[src], c.offsets[src+1]
				copy(c.data[tail:], c.data[start:end])
				tail += int(end - start)
				c.offsets[dst+1] = int64(tail)
				c.nullBitmap[idx] |= byte(1 << pos)
			}
		}
		c.data = c.data[:tail]
		c.offsets = c.offsets[:len(sel)+1]
	}
	c.length = len(sel)

	// clean nullBitmap
	c.nullBitmap = c.nullBitmap[:(len(sel)+7)>>3]
	idx := len(sel) >> 3
	if idx < len(c.nullBitmap) {
		pos := uint16(len(sel) & 7)
		c.nullBitmap[idx] &= byte((1 << pos) - 1)
	}
}

// CopyReconstruct copies this Column to dst and removes unselected rows.
// If dst is nil, it creates a new Column and returns it.
func (c *Column) CopyReconstruct(sel []int, dst *Column) *Column {
	if sel == nil {
		return c.CopyConstruct(dst)
	}

	if dst == nil {
		dst = newColumn(c.typeSize(), len(sel))
	} else {
		dst.Reset()
	}

	if c.isFixed() {
		elemLen := len(c.elemBuf)
		for _, i := range sel {
			dst.appendNullBitmap(!c.IsNull(i))
			dst.data = append(dst.data, c.data[i*elemLen:i*elemLen+elemLen]...)
			dst.length++
		}
	} else {
		for _, i := range sel {
			dst.appendNullBitmap(!c.IsNull(i))
			start, end := c.offsets[i], c.offsets[i+1]
			dst.data = append(dst.data, c.data[start:end]...)
			dst.offsets = append(dst.offsets, int64(len(dst.data)))
			dst.length++
		}
	}
	return dst
}
