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
	"fmt"
	"math"
	"math/rand"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
)

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
	return *(*types.Time)(unsafe.Pointer(&c.data[rowID*sizeTime]))
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
	var val uint64
	copy((*[8]byte)(unsafe.Pointer(&val))[:], c.data[start:])
	return string(hack.String(c.data[start+8 : end])), val
}

// GetRaw returns the underlying raw bytes in the specific row.
func (c *Column) GetRaw(rowID int) []byte {
	var data []byte
	if c.IsFixed() {
		elemLen := len(c.elemBuf)
		data = c.data[rowID*elemLen : rowID*elemLen+elemLen]
	} else {
		data = c.data[c.offsets[rowID]:c.offsets[rowID+1]]
	}
	return data
}

// GetRawLength returns the length of the raw
func (c *Column) GetRawLength(rowID int) int {
	if c.IsFixed() {
		return len(c.elemBuf)
	}
	return int(c.offsets[rowID+1] - c.offsets[rowID])
}

// SetRaw sets the raw bytes for the rowIdx-th element.
// NOTE: Two conditions must be satisfied before calling this function:
// 1. The column should be stored with variable-length elements.
// 2. The length of the new element should be exactly the same as the old one.
func (c *Column) SetRaw(rowID int, bs []byte) {
	copy(c.data[c.offsets[rowID]:c.offsets[rowID+1]], bs)
}

// reconstruct reconstructs this Column by removing all filtered rows in it according to sel.
func (c *Column) reconstruct(sel []int) {
	if sel == nil {
		return
	}
	if c.IsFixed() {
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

	selLength := len(sel)
	if selLength == c.length {
		// The variable 'ascend' is used to check if the sel array is in ascending order
		ascend := true
		for i := 1; i < selLength; i++ {
			if sel[i] < sel[i-1] {
				ascend = false
				break
			}
		}
		if ascend {
			return c.CopyConstruct(dst)
		}
	}

	if dst == nil {
		dst = newColumn(c.typeSize(), len(sel))
	} else {
		dst.reset()
	}

	if c.IsFixed() {
		elemLen := len(c.elemBuf)
		dst.elemBuf = make([]byte, elemLen)
		for _, i := range sel {
			dst.appendNullBitmap(!c.IsNull(i))
			dst.data = append(dst.data, c.data[i*elemLen:i*elemLen+elemLen]...)
			dst.length++
		}
	} else {
		dst.elemBuf = nil
		if len(dst.offsets) == 0 {
			dst.offsets = append(dst.offsets, 0)
		}
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

// MergeNulls merges these columns' null bitmaps.
// For a row, if any column of it is null, the result is null.
// It works like: if col1.IsNull || col2.IsNull || col3.IsNull.
// The caller should ensure that all these columns have the same
// length, and data stored in the result column is fixed-length type.
func (c *Column) MergeNulls(cols ...*Column) {
	if !c.IsFixed() {
		panic("result column should be fixed-length type")
	}
	for _, col := range cols {
		if c.length != col.length {
			panic(fmt.Sprintf("should ensure all columns have the same length, expect %v, but got %v", c.length, col.length))
		}
	}
	for _, col := range cols {
		for i := range c.nullBitmap {
			// bit 0 is null, 1 is not null, so do AND operations here.
			c.nullBitmap[i] &= col.nullBitmap[i]
		}
	}
}

// DestroyDataForTest destroies data in the column so that
// we can test if data in column are deeply copied.
func (c *Column) DestroyDataForTest() {
	dataByteNum := len(c.data)
	for i := range dataByteNum {
		c.data[i] = byte(rand.Intn(256))
	}
}

// ContainsVeryLargeElement checks if the column contains element whose length is greater than math.MaxUint32.
func (c *Column) ContainsVeryLargeElement() bool {
	if c.length == 0 {
		return false
	}
	if c.IsFixed() {
		return false
	}
	if c.offsets[c.length] <= math.MaxUint32 {
		return false
	}
	for i := range c.length {
		if c.offsets[i+1]-c.offsets[i] > math.MaxUint32 {
			return true
		}
	}
	return false
}
