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
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

type Codec struct {
	// colTypes is only used when decoding a Chunk.
	colTypes []*types.FieldType

	// notNullBitmap is only used when decoding a Chunk.
	notNullBitmap []byte
}

func NewCodec(colTypes []*types.FieldType) *Codec {
	return &Codec{colTypes, nil}
}

func (c *Codec) Encode(chk *Chunk) []byte {
	buffer := make([]byte, 0, chk.MemoryUsage())
	for _, col := range chk.columns {
		if col.isFixed() {
			buffer = c.encodeFixLenColumn(buffer, col)
		} else {
			buffer = c.encodeVarLenColumn(buffer, col)
		}
	}
	return buffer
}

func (c *Codec) encodeFixLenColumn(buffer []byte, col *column) []byte {
	startOffset := len(buffer)

	// encode column
	var lenBuffer [4]byte
	buffer = append(buffer, lenBuffer[:]...)
	buffer = c.encodeColumn(buffer, lenBuffer[:], col)

	// supplement the length info of this column in the front
	numEncodedBytes := len(buffer) - startOffset
	binary.LittleEndian.PutUint32(lenBuffer[:], uint32(numEncodedBytes))
	copy(buffer[startOffset:], lenBuffer[:4])

	return buffer
}

func (c *Codec) encodeVarLenColumn(buffer []byte, col *column) []byte {
	startOffset := len(buffer)

	// encode column
	var lenBuffer [4]byte
	buffer = append(buffer, lenBuffer[:]...)
	buffer = c.encodeColumn(buffer, lenBuffer[:], col)

	// encode offsets
	offsetBytes := c.i32SliceToBytes(col.offsets)
	binary.LittleEndian.PutUint32(lenBuffer[:], uint32(len(offsetBytes)))
	buffer = append(buffer, lenBuffer[:4]...)
	buffer = append(buffer, offsetBytes...)

	// supplement the length info of this column in the front
	numEncodedBytes := len(buffer) - startOffset
	binary.LittleEndian.PutUint32(lenBuffer[:], uint32(numEncodedBytes))
	copy(buffer[startOffset:], lenBuffer[:4])

	return buffer
}

func (c *Codec) encodeColumn(buffer, lenBuffer []byte, col *column) []byte {
	// encode length
	binary.LittleEndian.PutUint32(lenBuffer, uint32(col.length))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullCount
	binary.LittleEndian.PutUint32(lenBuffer, uint32(col.nullCount))
	buffer = append(buffer, lenBuffer[:4]...)

	fmt.Printf("encodeColumn: col.nullCount=%v, col.length=%v\n", col.nullCount, col.length)

	// encode nullBitmap
	if col.nullCount > 0 {
		binary.LittleEndian.PutUint32(lenBuffer, uint32(len(col.nullBitmap)))
		buffer = append(buffer, lenBuffer[:4]...)
		buffer = append(buffer, col.nullBitmap...)
	}

	// encode data
	binary.LittleEndian.PutUint32(lenBuffer, uint32(len(col.data)))
	buffer = append(buffer, lenBuffer[:4]...)
	buffer = append(buffer, col.data...)

	return buffer
}

func (c *Codec) i32SliceToBytes(i32s []int32) (b []byte) {
	if len(i32s) == 0 {
		return nil
	}
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(i32s) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&i32s[0]))
	return b
}

func (c *Codec) Decode(buffer []byte) *Chunk {
	chk := &Chunk{}
	for ordinal := 0; len(buffer) > 0; ordinal++ {
		col := &column{}
		buffer = c.decodeColumn(buffer, col, ordinal)
		chk.columns = append(chk.columns, col)
	}
	return chk
}

func (c *Codec) DecodeToChunk(buffer []byte, chk *Chunk) (remained []byte) {
	for i := 0; i < len(chk.columns); i++ {
		buffer = c.decodeColumn(buffer, chk.columns[i], i)
	}
	return buffer
}

func (c *Codec) decodeColumn(buffer []byte, col *column, ordinal int) (remained []byte) {
	numEatenBytes := uint32(0)

	// decode number of the total bytes of a column.
	numTotalBytes := binary.LittleEndian.Uint32(buffer[numEatenBytes:])
	numEatenBytes += 4

	// decode length.
	col.length = int(binary.LittleEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4

	// decode nullCount.
	col.nullCount = int(binary.LittleEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4

	// decode nullBitmap.
	fmt.Printf("decodeColumn: col.nullCount=%v, col.length=%v\n", col.nullCount, col.length)
	if col.nullCount > 0 {
		numBitmapBytes := uint32(binary.LittleEndian.Uint32(buffer[numEatenBytes:]))
		numEatenBytes += 4
		col.nullBitmap = append(col.nullBitmap[:0], buffer[numEatenBytes:numEatenBytes+numBitmapBytes]...)
		numEatenBytes += numBitmapBytes
	} else {
		col.nullBitmap = append(col.nullBitmap[:0], c.allNotNull(col.length)...)
	}

	// decode data.
	numDataBytes := uint32(binary.LittleEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4
	col.data = append(col.data[:0], buffer[numEatenBytes:numEatenBytes+numDataBytes]...)
	numEatenBytes += numDataBytes

	numFixedBytes := getFixedLen(c.colTypes[ordinal])
	if numFixedBytes != -1 {
		if cap(col.elemBuf) < numFixedBytes {
			col.elemBuf = make([]byte, numFixedBytes)
		}
		return buffer[numTotalBytes:]
	}
	if numEatenBytes == numTotalBytes {
		return buffer[numTotalBytes:]
	}

	// decode offsets.
	numOffsetBytes := binary.LittleEndian.Uint32(buffer[numEatenBytes:])
	numEatenBytes += 4
	col.offsets = append(col.offsets[:0], c.bytesToI32Slice(buffer[numEatenBytes:numEatenBytes+numOffsetBytes])...)

	return buffer[numTotalBytes:]
}

func (c *Codec) allNotNull(numElements int) []byte {
	numBytes := (numElements + 7) >> 3
	if len(c.notNullBitmap) < numBytes {
		newNotNullBitmap := make([]byte, numBytes)
		copy(newNotNullBitmap, c.notNullBitmap)
		for i := len(c.notNullBitmap); i < numBytes; i++ {
			newNotNullBitmap[i] = byte(0xFF)
		}
		c.notNullBitmap = newNotNullBitmap
	}
	return c.notNullBitmap[:numBytes]
}

func (c *Codec) bytesToI32Slice(b []byte) (i32s []int32) {
	if len(b) == 0 {
		return nil
	}
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&i32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return i32s
}

func getFixedLen(colType *types.FieldType) int {
	switch colType.Tp {
	case mysql.TypeFloat:
		return 4
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong,
		mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeYear:
		return 8
	case mysql.TypeDuration, mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return 16
	case mysql.TypeNewDecimal:
		return types.MyDecimalStructSize
	default:
		return -1
	}
}
