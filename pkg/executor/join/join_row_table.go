// Copyright 2024 PingCAP, Inc.
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

package join

import (
	"unsafe"

	"github.com/pingcap/tidb/pkg/util/serialization"
)

const sizeOfNextPtr = int(unsafe.Sizeof(uintptr(0)))
const sizeOfLengthField = int(unsafe.Sizeof(uint64(1)))
const sizeOfUnsafePointer = int(unsafe.Sizeof(unsafe.Pointer(nil)))
const sizeOfUintptr = int(unsafe.Sizeof(uintptr(0)))

var (
	fakeAddrPlaceHolder = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	usedFlagMask        uint32
	bitMaskInUint32     [32]uint32
)

func init() {
	// In nullmap, each bit represents a column in current row is null or not null. nullmap is designed to be read/write at the
	// unit of byte(uint8). Some joins(for example, left outer join use left side to build) need an extra bit to represent if
	// current row is matched or not. This bit is called used flag, and in the implementation, it actually use the first bit in
	// nullmap as the used flag. There will be concurrent read/write for the used flag, so need to use atomic read/write when
	// accessing the used flag. However, the minimum atomic read/write unit in go is uint32, so nullmap need to be read/write as
	// uint32 in these cases. Read/write uint32 need to consider the endianess, for example, for a piece of memory containing
	// continuous 32 bits, we want to set the first bit to 1, the memory after set should be
	// 0x70 0x00 0x00 0x00
	// when interprete the 32 bit as uint32
	// in big endian system, it is 0x70000000
	// in little endian system, it is 0x00000070
	// useFlagMask and bitMaskInUint32 is used to hide these difference in big endian/small endian system
	// and init function is used to init usedFlagMask and bitMaskInUint32 based on endianness of current env
	endiannessTest := uint32(1) << 7
	low8Value := *(*uint8)(unsafe.Pointer(&endiannessTest))
	if uint32(low8Value) == endiannessTest {
		// Little-endian system: the lowest byte (at the lowest address) stores the least significant byte (LSB) of the integer
		initializeBitMasks(true)
	} else {
		// Big-endian system: the lowest byte (at the lowest address) stores the most significant byte (MSB) of the integer
		initializeBitMasks(false)
	}
	usedFlagMask = bitMaskInUint32[0]
}

// initializeBitMasks encapsulates the bit-shifting logic to set the bitMaskInUint32 array based on endianness
// The parameter isLittleEndian indicates the system's endianness
//   - If the system is little-endian, the bit mask for each byte starts from the most significant bit (bit 7) and decrements sequentially
//   - If the system is big-endian, the bit masks are set sequentially from the highest bit (bit 31) to the lowest bit (bit 0),
//     ensuring that atomic operations can be performed correctly on different endian systems
func initializeBitMasks(isLittleEndian bool) {
	for i := 0; i < 32; i++ {
		if isLittleEndian {
			// On little-endian systems, bit masks are arranged in order from high to low within each byte
			bitMaskInUint32[i] = uint32(1) << (7 - (i % 8) + (i/8)*8)
		} else {
			// On big-endian systems, bit masks are arranged from the highest bit (bit 31) to the lowest bit (bit 0)
			bitMaskInUint32[i] = uint32(1) << (31 - i)
		}
	}
}

//go:linkname heapObjectsCanMove runtime.heapObjectsCanMove
func heapObjectsCanMove() bool

type rowTableSegment struct {
	/*
	   The row storage used in hash join, the layout is
	   |---------------------|-----------------|----------------------|-------------------------------|
	              |                   |                   |                           |
	              V                   V                   V                           V
	        next_row_ptr          null_map     serialized_key/key_length           row_data
	   next_row_ptr: the ptr to link to the next row, used in hash table build, it will make all the rows of the same hash value a linked list
	   null_map(optional): null_map actually includes two parts: the null_flag for each column in current row, the used_flag which is used in
	                       right semi/outer join. This field is optional, if all the column from build side is not null and used_flag is not used
	                       this field is not needed.
	   serialized_key/key_length(optional): if the join key is inlined, and the key has variable length, this field is used to record the key length
	                       of current row, if the join key is not inlined, this field is the serialized representation of the join keys, used to quick
	                       join key compare during probe stage. This field is optional, for join keys that can be inlined in the row_data(for example,
	                       join key with one integer) and has fixed length, this field is not needed.
	   row_data: the data for all the columns of current row
	   The columns in row_data is variable length. For elements that has fixed length(e.g. int64), it will be saved directly, for elements has a
	   variable length(e.g. string related elements), it will first save the size followed by the raw data(todo check if address of the size need to be 8 byte aligned).
	   Since the row_data is variable length, it is designed to access the column data in order. In order to avoid random access of the column data in row_data,
	   the column order in the row_data will be adjusted to fit the usage order, more specifically the column order will be
	   * join key is inlined + have other conditions: join keys, column used in other condition, rest columns that will be used as join output
	   * join key is inlined + no other conditions: join keys, rest columns that will be used as join output
	   * join key is not inlined + have other conditions: columns used in other condition, rest columns that will be used as join output
	   * join key is not inlined + no other conditions: columns that will be used as join output
	*/
	rawData         []byte   // the chunk of memory to save the row data
	hashValues      []uint64 // the hash value of each rows
	rowStartOffset  []uint64 // the start address of each row
	validJoinKeyPos []int    // the pos of rows that need to be inserted into hash table, used in hash table build
	finalized       bool     // after finalized is set to true, no further modification is allowed
	// taggedBits is the bit that can be used to tag for all pointer in rawData, it use the MSB to tag, so if the n MSB is all 0, the taggedBits is n
	taggedBits uint8
}

func (rts *rowTableSegment) totalUsedBytes() int64 {
	ret := int64(cap(rts.rawData))
	ret += int64(cap(rts.hashValues) * int(serialization.Uint64Len))
	ret += int64(cap(rts.rowStartOffset) * int(serialization.Uint64Len))
	ret += int64(cap(rts.validJoinKeyPos) * int(serialization.IntLen))
	return ret
}

func (rts *rowTableSegment) getRowPointer(index int) unsafe.Pointer {
	return unsafe.Pointer(&rts.rawData[rts.rowStartOffset[index]])
}

func (rts *rowTableSegment) initTaggedBits() {
	startPtr := uintptr(0)
	*(*unsafe.Pointer)(unsafe.Pointer(&startPtr)) = rts.getRowPointer(0)
	endPtr := uintptr(0)
	*(*unsafe.Pointer)(unsafe.Pointer(&endPtr)) = rts.getRowPointer(len(rts.rowStartOffset) - 1)
	rts.taggedBits = getTaggedBitsFromUintptr(endPtr | startPtr)
}

// This variable should be const, but we need to modify it for test
var maxRowTableSegmentSize = int64(1024)

// 64 MB
const maxRowTableSegmentByteSize = 64 * 1024 * 1024

func newRowTableSegment(rowSizeHint uint) *rowTableSegment {
	return &rowTableSegment{
		rawData:         make([]byte, 0),
		hashValues:      make([]uint64, 0, rowSizeHint),
		rowStartOffset:  make([]uint64, 0, rowSizeHint),
		validJoinKeyPos: make([]int, 0, rowSizeHint),
	}
}

func (rts *rowTableSegment) rowCount() int64 {
	return int64(len(rts.rowStartOffset))
}

func (rts *rowTableSegment) validKeyCount() uint64 {
	return uint64(len(rts.validJoinKeyPos))
}

func (rts *rowTableSegment) getRowNum() int {
	return len(rts.hashValues)
}

func (rts *rowTableSegment) getRowBytes(idx int) []byte {
	rowNum := rts.getRowNum()
	if idx == rowNum-1 {
		return rts.rawData[rts.rowStartOffset[idx]:]
	}
	return rts.rawData[rts.rowStartOffset[idx]:rts.rowStartOffset[idx+1]]
}

func setNextRowAddress(rowStart unsafe.Pointer, nextRowAddress taggedPtr) {
	*(*taggedPtr)(rowStart) = nextRowAddress
}

func getNextRowAddress(rowStart unsafe.Pointer, tagHelper *tagPtrHelper, hashValue uint64) taggedPtr {
	ret := *(*taggedPtr)(rowStart)
	hashTagValue := tagHelper.getTaggedValue(hashValue)
	if uint64(ret)&hashTagValue != hashTagValue {
		return 0
	}
	return ret
}

type rowTable struct {
	segments []*rowTableSegment
}

func (rt *rowTable) getTotalMemoryUsage() int64 {
	totalMemoryUsage := int64(0)
	for _, seg := range rt.segments {
		if seg.finalized {
			totalMemoryUsage += seg.totalUsedBytes()
		}
	}
	return totalMemoryUsage
}

func (rt *rowTable) getSegments() []*rowTableSegment {
	return rt.segments
}

func (rt *rowTable) clearSegments() {
	rt.segments = nil
}

// used for test
func (rt *rowTable) getRowPointer(rowIndex int) unsafe.Pointer {
	for segIndex := 0; segIndex < len(rt.segments); segIndex++ {
		if rowIndex < len(rt.segments[segIndex].rowStartOffset) {
			return rt.segments[segIndex].getRowPointer(rowIndex)
		}
		rowIndex -= len(rt.segments[segIndex].rowStartOffset)
	}
	return nil
}

func (rt *rowTable) getValidJoinKeyPos(rowIndex int) int {
	startOffset := 0
	for segIndex := 0; segIndex < len(rt.segments); segIndex++ {
		if rowIndex < len(rt.segments[segIndex].validJoinKeyPos) {
			return startOffset + rt.segments[segIndex].validJoinKeyPos[rowIndex]
		}
		rowIndex -= len(rt.segments[segIndex].validJoinKeyPos)
		startOffset += len(rt.segments[segIndex].rowStartOffset)
	}
	return -1
}

func (rt *rowTable) getTotalUsedBytesInSegments() int64 {
	totalUsedBytes := int64(0)
	for _, seg := range rt.segments {
		if seg.finalized {
			totalUsedBytes += seg.totalUsedBytes()
		}
	}
	return totalUsedBytes
}

func newRowTable() *rowTable {
	return &rowTable{
		segments: make([]*rowTableSegment, 0),
	}
}

func (rt *rowTable) merge(other *rowTable) {
	rt.segments = append(rt.segments, other.segments...)
}

func (rt *rowTable) rowCount() uint64 {
	ret := uint64(0)
	for _, s := range rt.segments {
		ret += uint64(s.rowCount())
	}
	return ret
}

func (rt *rowTable) validKeyCount() uint64 {
	ret := uint64(0)
	for _, s := range rt.segments {
		ret += s.validKeyCount()
	}
	return ret
}
