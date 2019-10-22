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

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

// Codec is used to:
// 1. encode a Chunk to a byte slice.
// 2. decode a Chunk from a byte slice.
type Codec struct {
	// colTypes is used to check whether a Column is fixed sized and what the
	// fixed size for every element.
	// NOTE: It's only used for decoding.
	colTypes []*types.FieldType
}

// NewCodec creates a new Codec object for encode or decode a Chunk.
func NewCodec(colTypes []*types.FieldType) *Codec {
	return &Codec{colTypes}
}

// Encode encodes a Chunk to a byte slice.
func (c *Codec) Encode(chk *Chunk) []byte {
	buffer := make([]byte, 0, chk.MemoryUsage())
	for _, col := range chk.columns {
		buffer = c.encodeColumn(buffer, col)
	}
	return buffer
}

func (c *Codec) encodeColumn(buffer []byte, col *Column) []byte {
	var lenBuffer [4]byte
	// encode length.
	binary.LittleEndian.PutUint32(lenBuffer[:], uint32(col.length))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullCount.
	binary.LittleEndian.PutUint32(lenBuffer[:], uint32(col.nullCount()))
	buffer = append(buffer, lenBuffer[:4]...)

	// encode nullBitmap.
	if col.nullCount() > 0 {
		numNullBitmapBytes := (col.length + 7) / 8
		buffer = append(buffer, col.nullBitmap[:numNullBitmapBytes]...)
	}

	// encode offsets.
	if !col.isFixed() {
		numOffsetBytes := (col.length + 1) * 8
		offsetBytes := i64SliceToBytes(col.offsets)
		buffer = append(buffer, offsetBytes[:numOffsetBytes]...)
	}

	// encode data.
	buffer = append(buffer, col.data...)
	return buffer
}

func i64SliceToBytes(i64s []int64) (b []byte) {
	if len(i64s) == 0 {
		return nil
	}
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = len(i64s) * 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&i64s[0]))
	return b
}

// Decode decodes a Chunk from a byte slice, return the remained unused bytes.
func (c *Codec) Decode(buffer []byte) (*Chunk, []byte) {
	chk := &Chunk{}
	for ordinal := 0; len(buffer) > 0; ordinal++ {
		col := &Column{}
		buffer = c.decodeColumn(buffer, col, ordinal)
		chk.columns = append(chk.columns, col)
	}
	return chk, buffer
}

// DecodeToChunk decodes a Chunk from a byte slice, return the remained unused bytes.
func (c *Codec) DecodeToChunk(buffer []byte, chk *Chunk) (remained []byte) {
	for i := 0; i < len(chk.columns); i++ {
		buffer = c.decodeColumn(buffer, chk.columns[i], i)
	}
	return buffer
}

// decodeColumn decodes a Column from a byte slice, return the remained unused bytes.
func (c *Codec) decodeColumn(buffer []byte, col *Column, ordinal int) (remained []byte) {
	// Todo(Shenghui Wu): Optimize all data is null.
	// decode length.
	col.length = int(binary.LittleEndian.Uint32(buffer))
	buffer = buffer[4:]

	// decode nullCount.
	nullCount := int(binary.LittleEndian.Uint32(buffer))
	buffer = buffer[4:]

	// decode nullBitmap.
	if nullCount > 0 {
		numNullBitmapBytes := (col.length + 7) / 8
		col.nullBitmap = buffer[:numNullBitmapBytes]
		buffer = buffer[numNullBitmapBytes:]
	} else {
		c.setAllNotNull(col)
	}

	// decode offsets.
	numFixedBytes := getFixedLen(c.colTypes[ordinal])
	numDataBytes := int64(numFixedBytes * col.length)
	if numFixedBytes == -1 {
		numOffsetBytes := (col.length + 1) * 8
		col.offsets = bytesToI64Slice(buffer[:numOffsetBytes])
		buffer = buffer[numOffsetBytes:]
		numDataBytes = col.offsets[col.length]
	} else if cap(col.elemBuf) < numFixedBytes {
		col.elemBuf = make([]byte, numFixedBytes)
	}

	// decode data.
	col.data = buffer[:numDataBytes]
	return buffer[numDataBytes:]
}

var allNotNullBitmap [128]byte

func (c *Codec) setAllNotNull(col *Column) {
	numNullBitmapBytes := (col.length + 7) / 8
	col.nullBitmap = col.nullBitmap[:0]
	for i := 0; i < numNullBitmapBytes; {
		numAppendBytes := mathutil.Min(numNullBitmapBytes-i, cap(allNotNullBitmap))
		col.nullBitmap = append(col.nullBitmap, allNotNullBitmap[:numAppendBytes]...)
		i += numAppendBytes
	}
}

func bytesToI64Slice(b []byte) (i64s []int64) {
	if len(b) == 0 {
		return nil
	}
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&i64s))
	hdr.Len = len(b) / 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return i64s
}

// varElemLen indicates this Column is a variable length Column.
const varElemLen = -1

func getFixedLen(colType *types.FieldType) int {
	switch colType.Tp {
	case mysql.TypeFloat:
		return 4
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong,
		mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeYear, mysql.TypeDuration:
		return 8
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		return sizeTime
	case mysql.TypeNewDecimal:
		return types.MyDecimalStructSize
	default:
		return varElemLen
	}
}

func init() {
	for i := 0; i < 128; i++ {
		allNotNullBitmap[i] = 0xFF
	}
}

// Decoder is used to:
// 1. decode a Chunk from a byte slice.
// How Decoder works:
// 1. Initialization phase: Decode a whole input byte slice to Decoder.intermChk using Codec.Decode. intermChk is
//    introduced to simplify the implementation of decode phase. This phase uses pointer operations with less CPU andF
//    memory cost.
// 2. Decode phase:
//    2.1 Set the number of rows that should be decoded to a multiple of 8 greater than
//        targetChk.RequiredRows()-targetChk.NumRows(), this can reduce the cost when copying the srcCol.nullBitMap into destCol.nullBitMap.
//    2.2 Append srcCol.offsets to destCol.offsets when the elements is of var-length type. And further adjust the
//        offsets according to descCol.offsets[destCol.length]-srcCol.offsets[0].
//    2.3 Append srcCol.nullBitMap to destCol.nullBitMap.
// 3. Go to step 1 when the input byte slice is consumed.
type Decoder struct {
	intermChk    *Chunk
	colTypes     []*types.FieldType
	remainedRows int
}

// NewDecoder creates a new Decoder object for decode a Chunk.
func NewDecoder(chk *Chunk, colTypes []*types.FieldType) *Decoder {
	return &Decoder{intermChk: chk, colTypes: colTypes, remainedRows: 0}
}

// Decode decodes a Chunk from Decoder, save the remained unused bytes.
func (c *Decoder) Decode(target *Chunk) {
	requiredRows := target.RequiredRows() - target.NumRows()
	// Floor requiredRows to the next multiple of 8
	requiredRows = (requiredRows + 7) >> 3 << 3
	if requiredRows > c.remainedRows {
		requiredRows = c.remainedRows
	}
	for i := 0; i < len(c.colTypes); i++ {
		c.decodeColumn(target, i, requiredRows)
	}
	c.remainedRows -= requiredRows
}

// ResetAndInit resets Decoder, and use data byte slice to init it.
func (c *Decoder) ResetAndInit(data []byte) {
	codec := NewCodec(c.colTypes)
	codec.DecodeToChunk(data, c.intermChk)
	c.remainedRows = c.intermChk.NumRows()
}

// IsFinished indicate the data byte slice is consumed.
func (c *Decoder) IsFinished() bool {
	return c.remainedRows == 0
}

func (c *Decoder) decodeColumn(target *Chunk, ordinal int, requiredRows int) {
	elemLen := getFixedLen(c.colTypes[ordinal])
	numDataBytes := int64(elemLen * requiredRows)
	srcCol := c.intermChk.columns[ordinal]
	destCol := target.columns[ordinal]

	if elemLen == varElemLen {
		numDataBytes = srcCol.offsets[requiredRows] - srcCol.offsets[0]
		deltaOffset := destCol.offsets[destCol.length] - srcCol.offsets[0]
		destCol.offsets = append(destCol.offsets, srcCol.offsets[1:requiredRows+1]...)

		for i := destCol.length + 1; i <= destCol.length+requiredRows; i++ {
			destCol.offsets[i] = destCol.offsets[i] + deltaOffset
		}
		srcCol.offsets = srcCol.offsets[requiredRows:]
	} else if cap(destCol.elemBuf) < elemLen {
		destCol.elemBuf = make([]byte, elemLen)
	}

	numNullBitmapBytes := (requiredRows + 7) >> 3
	if destCol.length%8 == 0 {
		destCol.nullBitmap = append(destCol.nullBitmap, srcCol.nullBitmap[:numNullBitmapBytes]...)
	} else {
		destCol.appendMultiSameNullBitmap(false, requiredRows)
		bitMapLen := len(destCol.nullBitmap)
		// bitOffset indicates the number of elements in destCol.nullBitmap's last byte.
		bitOffset := destCol.length % 8
		startIdx := (destCol.length - 1) >> 3
		for i := 0; i < numNullBitmapBytes; i++ {
			destCol.nullBitmap[startIdx+i] |= srcCol.nullBitmap[i] << bitOffset
			// the total number of elements maybe is small than bitMapLen * 8, and last some bits in srcCol.nullBitmap are useless.
			if bitMapLen > startIdx+i+1 {
				destCol.nullBitmap[startIdx+i+1] |= srcCol.nullBitmap[i] >> (8 - bitOffset)
			}
		}
	}
	srcCol.nullBitmap = srcCol.nullBitmap[numNullBitmapBytes:]
	destCol.length += requiredRows

	destCol.data = append(destCol.data, srcCol.data[:numDataBytes]...)
	srcCol.data = srcCol.data[numDataBytes:]
}

// DecodeToChunkTest decodes a Chunk from a byte slice, return the remained unused bytes. (Only test, will remove before I merge this pr)
func (c *Codec) DecodeToChunkTest(buffer []byte, chk *Chunk) (remained []byte) {
	for i := 0; i < len(chk.columns); i++ {
		buffer = c.decodeColumnTest(buffer, chk.columns[i], i)
	}
	return buffer
}

// decodeColumn decodes a Column from a byte slice, return the remained unused bytes.
func (c *Codec) decodeColumnTest(buffer []byte, col *Column, ordinal int) (remained []byte) {
	// Todo(Shenghui Wu): Optimize all data is null.
	// decode length.
	col.length = int(binary.LittleEndian.Uint32(buffer))
	buffer = buffer[4:]

	// decode nullCount.
	nullCount := int(binary.LittleEndian.Uint32(buffer))
	buffer = buffer[4:]

	// decode nullBitmap.
	if nullCount > 0 {
		numNullBitmapBytes := (col.length + 7) / 8
		col.nullBitmap = append(col.nullBitmap[:0], buffer[:numNullBitmapBytes]...)
		buffer = buffer[numNullBitmapBytes:]
	} else {
		c.setAllNotNull(col)
	}

	// decode offsets.
	numFixedBytes := getFixedLen(c.colTypes[ordinal])
	numDataBytes := int64(numFixedBytes * col.length)
	if numFixedBytes == -1 {
		numOffsetBytes := (col.length + 1) * 8
		col.offsets = append(col.offsets[:0], bytesToI64Slice(buffer[:numOffsetBytes])...)
		buffer = buffer[numOffsetBytes:]
		numDataBytes = col.offsets[col.length]
	} else if cap(col.elemBuf) < numFixedBytes {
		col.elemBuf = make([]byte, numFixedBytes)
	}

	// decode data.
	col.data = append(col.data[:0], buffer[:numDataBytes]...)
	return buffer[numDataBytes:]
}
