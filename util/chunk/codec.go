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
	"encoding/binary"
	"reflect"
	"unsafe"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mathutil"
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
		col.nullBitmap = buffer[:numNullBitmapBytes:numNullBitmapBytes]
		buffer = buffer[numNullBitmapBytes:]
	} else {
		c.setAllNotNull(col)
	}

	// decode offsets.
	numFixedBytes := getFixedLen(c.colTypes[ordinal])
	numDataBytes := int64(numFixedBytes * col.length)
	if numFixedBytes == -1 {
		numOffsetBytes := (col.length + 1) * 8
		col.offsets = bytesToI64Slice(buffer[:numOffsetBytes:numOffsetBytes])
		buffer = buffer[numOffsetBytes:]
		numDataBytes = col.offsets[col.length]
	} else if cap(col.elemBuf) < numFixedBytes {
		col.elemBuf = make([]byte, numFixedBytes)
	}

	// decode data.
	col.data = buffer[:numDataBytes:numDataBytes]
	// The column reference the data of the grpc response, the memory of the grpc message cannot be GCed if we reuse
	// this column. Thus, we set `avoidReusing` to true.
	col.avoidReusing = true
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
	switch colType.GetType() {
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

// GetFixedLen get the memory size of a fixed-length type.
// if colType is not fixed-length, it returns varElemLen, aka -1.
func GetFixedLen(colType *types.FieldType) int {
	return getFixedLen(colType)
}

// EstimateTypeWidth estimates the average width of values of the type.
// This is used by the planner, which doesn't require absolutely correct results;
// it's OK (and expected) to guess if we don't know for sure.
//
// mostly study from https://github.com/postgres/postgres/blob/REL_12_STABLE/src/backend/utils/cache/lsyscache.c#L2356
func EstimateTypeWidth(colType *types.FieldType) int {
	colLen := getFixedLen(colType)
	// Easy if it's a fixed-width type
	if colLen != varElemLen {
		return colLen
	}

	colLen = colType.GetFlen()
	if colLen > 0 {
		if colLen <= 32 {
			return colLen
		}
		if colLen < 1000 {
			return 32 + (colLen-32)/2 // assume 50%
		}
		/*
		 * Beyond 1000, assume we're looking at something like
		 * "varchar(10000)" where the limit isn't actually reached often, and
		 * use a fixed estimate.
		 */
		return 32 + (1000-32)/2
	}
	// Oops, we have no idea ... wild guess time.
	return 32
}

func init() {
	for i := 0; i < 128; i++ {
		allNotNullBitmap[i] = 0xFF
	}
}

// Decoder decodes the data returned from the coprocessor and stores the result in Chunk.
// How Decoder works:
// 1. Initialization phase: Decode a whole input byte slice to Decoder.intermChk(intermediate chunk) using Codec.Decode.
//    intermChk is introduced to simplify the implementation of decode phase. This phase uses pointer operations with
//    less CPU and memory cost.
// 2. Decode phase:
//    2.1 Set the number of rows to be decoded to a value that is a multiple of 8 and greater than
//        `chk.RequiredRows() - chk.NumRows()`. This reduces the overhead of copying the srcCol.nullBitMap into
//        destCol.nullBitMap.
//    2.2 Append srcCol.offsets to destCol.offsets when the elements is of var-length type. And further adjust the
//        offsets according to descCol.offsets[destCol.length]-srcCol.offsets[0].
//    2.3 Append srcCol.nullBitMap to destCol.nullBitMap.
// 3. Go to step 1 when the input byte slice is consumed.
type Decoder struct {
	intermChk    *Chunk
	codec        *Codec
	remainedRows int
}

// NewDecoder creates a new Decoder object for decode a Chunk.
func NewDecoder(chk *Chunk, colTypes []*types.FieldType) *Decoder {
	return &Decoder{intermChk: chk, codec: NewCodec(colTypes), remainedRows: 0}
}

// Decode decodes multiple rows of Decoder.intermChk and stores the result in chk.
func (c *Decoder) Decode(chk *Chunk) {
	requiredRows := chk.RequiredRows() - chk.NumRows()
	// Set the requiredRows to a multiple of 8.
	requiredRows = (requiredRows + 7) >> 3 << 3
	if requiredRows > c.remainedRows {
		requiredRows = c.remainedRows
	}
	for i := 0; i < chk.NumCols(); i++ {
		c.decodeColumn(chk, i, requiredRows)
	}
	c.remainedRows -= requiredRows
}

// Reset decodes data and store the result in Decoder.intermChk. This decode phase uses pointer operations with less
// CPU and memory costs.
func (c *Decoder) Reset(data []byte) {
	c.codec.DecodeToChunk(data, c.intermChk)
	c.remainedRows = c.intermChk.NumRows()
}

// IsFinished indicates whether Decoder.intermChk has been dried up.
func (c *Decoder) IsFinished() bool {
	return c.remainedRows == 0
}

// RemainedRows indicates Decoder.intermChk has remained rows.
func (c *Decoder) RemainedRows() int {
	return c.remainedRows
}

// ReuseIntermChk swaps `Decoder.intermChk` with `chk` directly when `Decoder.intermChk.NumRows()` is no less
// than `chk.requiredRows * factor` where `factor` is 0.8 now. This can avoid the overhead of appending the
// data from `Decoder.intermChk` to `chk`. Moreover, the column.offsets needs to be further adjusted
// according to column.offset[0].
func (c *Decoder) ReuseIntermChk(chk *Chunk) {
	for i, col := range c.intermChk.columns {
		col.length = c.remainedRows
		elemLen := getFixedLen(c.codec.colTypes[i])
		if elemLen == varElemLen {
			// For var-length types, we need to adjust the offsets before reuse.
			if deltaOffset := col.offsets[0]; deltaOffset != 0 {
				for j := 0; j < len(col.offsets); j++ {
					col.offsets[j] -= deltaOffset
				}
			}
		}
	}
	chk.SwapColumns(c.intermChk)
	c.remainedRows = 0
}

func (c *Decoder) decodeColumn(chk *Chunk, ordinal int, requiredRows int) {
	elemLen := getFixedLen(c.codec.colTypes[ordinal])
	numDataBytes := int64(elemLen * requiredRows)
	srcCol := c.intermChk.columns[ordinal]
	destCol := chk.columns[ordinal]

	if elemLen == varElemLen {
		// For var-length types, we need to adjust the offsets after appending to destCol.
		numDataBytes = srcCol.offsets[requiredRows] - srcCol.offsets[0]
		deltaOffset := destCol.offsets[destCol.length] - srcCol.offsets[0]
		destCol.offsets = append(destCol.offsets, srcCol.offsets[1:requiredRows+1]...)
		for i := destCol.length + 1; i <= destCol.length+requiredRows; i++ {
			destCol.offsets[i] = destCol.offsets[i] + deltaOffset
		}
		srcCol.offsets = srcCol.offsets[requiredRows:]
	}

	numNullBitmapBytes := (requiredRows + 7) >> 3
	if destCol.length%8 == 0 {
		destCol.nullBitmap = append(destCol.nullBitmap, srcCol.nullBitmap[:numNullBitmapBytes]...)
	} else {
		destCol.appendMultiSameNullBitmap(false, requiredRows)
		bitMapLen := len(destCol.nullBitmap)
		// bitOffset indicates the number of valid bits in destCol.nullBitmap's last byte.
		bitOffset := destCol.length % 8
		startIdx := (destCol.length - 1) >> 3
		for i := 0; i < numNullBitmapBytes; i++ {
			destCol.nullBitmap[startIdx+i] |= srcCol.nullBitmap[i] << bitOffset
			// The high order 8-bitOffset bits in `srcCol.nullBitmap[i]` should be appended to the low order of the next slot.
			if startIdx+i+1 < bitMapLen {
				destCol.nullBitmap[startIdx+i+1] |= srcCol.nullBitmap[i] >> (8 - bitOffset)
			}
		}
	}
	// Set all the redundant bits in the last slot of destCol.nullBitmap to 0.
	numRedundantBits := uint(len(destCol.nullBitmap)*8 - destCol.length - requiredRows)
	bitMask := byte(1<<(8-numRedundantBits)) - 1
	destCol.nullBitmap[len(destCol.nullBitmap)-1] &= bitMask

	srcCol.nullBitmap = srcCol.nullBitmap[numNullBitmapBytes:]
	destCol.length += requiredRows

	destCol.data = append(destCol.data, srcCol.data[:numDataBytes]...)
	srcCol.data = srcCol.data[numDataBytes:]
}
