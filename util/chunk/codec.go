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
)

func Encode(chk *Chunk) []byte {
	buffer := make([]byte, 0, chk.MemoryUsage())
	for _, col := range chk.columns {
		if col.isFixed() {
			buffer = encodeFixLenColumn(buffer, col)
		} else {
			buffer = encodeVarLenColumn(buffer, col)
		}
	}
	return buffer
}

func encodeColumn(buffer, lenBuffer []byte, col *column) []byte {
	// encode length
	binary.BigEndian.PutUint32(lenBuffer, uint32(col.length))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullCount
	binary.BigEndian.PutUint32(lenBuffer, uint32(col.nullCount))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullBitmap
	binary.BigEndian.PutUint32(lenBuffer, uint32(len(col.nullBitmap)))
	buffer = append(buffer, lenBuffer[:4]...)
	buffer = append(buffer, col.nullBitmap...)

	// encode data
	binary.BigEndian.PutUint32(lenBuffer, uint32(len(col.data)))
	buffer = append(buffer, lenBuffer[:4]...)
	buffer = append(buffer, col.data...)

	return buffer
}

func encodeFixLenColumn(buffer []byte, col *column) []byte {
	colOffset := uint64(len(buffer))

	// encode column
	var lenBuffer [8]byte
	buffer = append(buffer, lenBuffer[:]...)
	buffer = encodeColumn(buffer, lenBuffer[:], col)

	// supplement the length info of this column in the front
	numBytes := uint64(len(buffer)) - colOffset - 8
	binary.BigEndian.PutUint64(lenBuffer[:], numBytes)
	copy(buffer[:colOffset], lenBuffer[:8])

	return buffer
}

func encodeVarLenColumn(buffer []byte, col *column) []byte {
	colOffset := uint64(len(buffer))

	// encode column
	var lenBuffer [8]byte
	buffer = append(buffer, lenBuffer[:]...)
	buffer = encodeColumn(buffer, lenBuffer[:], col)

	// encode offsets
	binary.BigEndian.PutUint32(lenBuffer[:], uint32(4*len(col.offsets)))
	buffer = append(buffer, lenBuffer[:4]...)
	for i := range col.offsets {
		binary.BigEndian.PutUint32(lenBuffer[:], uint32(col.offsets[i]))
		buffer = append(buffer, lenBuffer[:4]...)
	}

	// supplement the length info of this column in the front
	numBytes := uint64(len(buffer)) - colOffset - 8
	binary.BigEndian.PutUint64(lenBuffer[:], numBytes)
	copy(buffer[:colOffset], lenBuffer[:8])

	return buffer
}

func Decode(buffer []byte) *Chunk {
	chk := &Chunk{}
	for len(buffer) > 0 {
		col := &column{}
		buffer = decodeColumn(buffer, col)
		chk.columns = append(chk.columns, col)
	}
	return chk
}

func decodeColumn(buffer []byte, col *column) []byte {
	numEatenBytes := uint64(0)

	// decode number of the total bytes of a column.
	numTotalBytes := binary.BigEndian.Uint64(buffer[numEatenBytes:])
	numEatenBytes += 8

	// decode length.
	col.length = int(binary.BigEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4

	// decode nullCount.
	col.nullCount = int(binary.BigEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4

	// decode nullBitmap.
	numBitmapBytes := uint64(binary.BigEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4
	col.nullBitmap = buffer[numEatenBytes : numEatenBytes+numBitmapBytes]
	numEatenBytes += numBitmapBytes

	// decode data.
	numDataBytes := uint64(binary.BigEndian.Uint32(buffer[numEatenBytes:]))
	numEatenBytes += 4
	col.data = buffer[numEatenBytes : numEatenBytes+numDataBytes]
	numEatenBytes += numDataBytes

	if numEatenBytes == numTotalBytes {
		return buffer[:numTotalBytes]
	}

	// decode offsets.
	numOffsetBytes := binary.BigEndian.Uint32(buffer[numEatenBytes:])
	numEatenBytes += 4
	col.offsets = make([]int32, 0, numOffsetBytes/4)
	for numEatenBytes < numTotalBytes {
		offset := binary.BigEndian.Uint32(buffer[numEatenBytes:])
		col.offsets = append(col.offsets, int32(offset))
		numEatenBytes += 4
	}

	return buffer[:numTotalBytes]
}
