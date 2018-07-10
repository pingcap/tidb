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
	"reflect"
	"unsafe"

	"github.com/pingcap/tidb/types"
)

type Codec struct {
	// colTypes is only used when decode a Chunk.
	colTypes []*types.FieldType
}

func NewCodec(colTypes []*types.FieldType) *Codec {
	return &Codec{colTypes}
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
	var lenBuffer [8]byte
	buffer = append(buffer, lenBuffer[:]...)
	buffer = c.encodeColumn(buffer, lenBuffer[:], col)

	// supplement the length info of this column in the front
	numEncodedBytes := len(buffer) - startOffset - 8
	binary.LittleEndian.PutUint64(lenBuffer[:], uint64(numEncodedBytes))
	copy(buffer[startOffset:], lenBuffer[:8])

	return buffer
}

func (c *Codec) encodeVarLenColumn(buffer []byte, col *column) []byte {
	startOffset := len(buffer)

	// encode column
	var lenBuffer [8]byte
	buffer = append(buffer, lenBuffer[:]...)
	buffer = c.encodeColumn(buffer, lenBuffer[:], col)

	// encode offsets
	offsetBytes := c.i32SliceToBytes(col.offsets)
	binary.LittleEndian.PutUint64(lenBuffer[:], uint64(len(offsetBytes)))
	buffer = append(buffer, lenBuffer[:8]...)
	buffer = append(buffer, offsetBytes...)
	numPaddingBytes := c.calcNumPaddingBytes(len(offsetBytes))
	buffer = append(buffer, lenBuffer[:numPaddingBytes]...)

	// supplement the length info of this column in the front
	numEncodedBytes := len(buffer) - startOffset - 8
	binary.LittleEndian.PutUint64(lenBuffer[:], uint64(numEncodedBytes))
	copy(buffer[startOffset:], lenBuffer[:8])

	return buffer
}

func (c *Codec) encodeColumn(buffer, lenBuffer []byte, col *column) []byte {
	// encode length
	binary.LittleEndian.PutUint32(lenBuffer, uint32(col.length))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullCount
	binary.LittleEndian.PutUint32(lenBuffer, uint32(col.nullCount))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullBitmap
	binary.LittleEndian.PutUint64(lenBuffer, uint64(len(col.nullBitmap)))
	buffer = append(buffer, lenBuffer[:8]...)
	buffer = append(buffer, col.nullBitmap...)
	numPaddingBytes := c.calcNumPaddingBytes(len(col.nullBitmap))
	buffer = append(buffer, lenBuffer[:numPaddingBytes]...)

	// encode data
	binary.LittleEndian.PutUint64(lenBuffer, uint64(len(col.data)))
	buffer = append(buffer, lenBuffer[:4]...)
	buffer = append(buffer, col.data...)
	numPaddingBytes = c.calcNumPaddingBytes(len(col.data))
	buffer = append(buffer, lenBuffer[:numPaddingBytes]...)

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

func (c *Codec) calcNumPaddingBytes(numAppendedBytes int) int {
	numTailBytes := numAppendedBytes & 0x7
	return (8 - numTailBytes) & 0x7
}

func (c *Codec) Decode(buffer []byte) *Chunk {
	chk := &Chunk{}
	for len(buffer) > 0 {
		col := &column{}
		buffer = c.decodeColumn(buffer, col)
		chk.columns = append(chk.columns, col)
	}
	return chk
}

func (c *Codec) decodeColumn(buffer []byte, col *column) []byte {
	numEatenBytes := uint64(0)

	// decode number of the total bytes of a column.
	numTotalBytes := binary.LittleEndian.Uint64(buffer[numEatenBytes:])
	numEatenBytes += 8

	// decode length.
	col.length = int(binary.LittleEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4

	// decode nullCount.
	col.nullCount = int(binary.LittleEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4

	// decode nullBitmap.
	numBitmapBytes := uint64(binary.LittleEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4
	col.nullBitmap = buffer[numEatenBytes : numEatenBytes+numBitmapBytes]
	numEatenBytes += numBitmapBytes

	// decode data.
	numDataBytes := uint64(binary.LittleEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4
	col.data = buffer[numEatenBytes : numEatenBytes+numDataBytes]
	numEatenBytes += numDataBytes

	if numEatenBytes == numTotalBytes {
		return buffer[:numTotalBytes]
	}

	// decode offsets.
	numOffsetBytes := binary.LittleEndian.Uint32(buffer[numEatenBytes:])
	numEatenBytes += 4
	col.offsets = make([]int32, 0, numOffsetBytes/4)
	for numEatenBytes < numTotalBytes {
		offset := binary.LittleEndian.Uint32(buffer[numEatenBytes:])
		col.offsets = append(col.offsets, int32(offset))
		numEatenBytes += 4
	}

	return buffer[:numTotalBytes]
}
