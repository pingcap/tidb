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
	"hash/fnv"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
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

const maxRowTableSegmentSize = 1024

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

func setNextRowAddress(rowStart unsafe.Pointer, nextRowAddress unsafe.Pointer) {
	// Save unsafe.Pointer into current Row header. Generally speaking it is unsafe or even illegal in go
	// since after save the value of unsafe.Pointer into row header, it has no pointer semantics the value
	// in the row header may become invalid after GC. It is ok to save unsafe.Pointer so far because
	// 1. the check of heapObjectsCanMove makes sure that if the object is in heap, the address will not be changed after GC
	// 2. `rowStart` only points to a valid address in `rawData`. `rawData` is a slice in `rowTableSegment`, and it will be used by multiple goroutines,
	//    and its size will be runtime expanded, this kind of slice will always be allocated in heap
	*(*unsafe.Pointer)(rowStart) = nextRowAddress
}

func getNextRowAddress(rowStart unsafe.Pointer) uintptr {
	return uintptr(*(*unsafe.Pointer)(rowStart))
}

// TableMeta is the join table meta used in hash join v2
type TableMeta struct {
	// if the row has fixed length
	isFixedLength bool
	// the row length if the row is fixed length
	rowLength int
	// if the join keys has fixed length
	isJoinKeysFixedLength bool
	// the join keys length if it is fixed length
	joinKeysLength int
	// is the join key inlined in the row data, the join key can be inlined if and only if
	// 1. keyProb.canBeInlined returns true for all the keys
	// 2. there is no duplicate join keys
	isJoinKeysInlined bool
	// the length of null map, the null map include null bit for each column in the row and the used flag for right semi/outer join
	nullMapLength int
	// the column order in row layout, as described above, the save column order maybe different from the column order in build schema
	// for example, the build schema maybe [col1, col2, col3], and the column order in row maybe [col2, col1, col3], then this array
	// is [1, 0, 2]
	rowColumnsOrder []int
	// the column size of each column, -1 mean variable column, the order is the same as rowColumnsOrder
	columnsSize []int
	// the serialize mode for each key
	serializeModes []codec.SerializeMode
	// the first n columns in row is used for other condition, if a join has other condition, we only need to extract
	// first n columns from the RowTable to evaluate other condition
	columnCountNeededForOtherCondition int
	// total column numbers for build side chunk, this is used to construct the chunk if there is join other condition
	totalColumnNumber int
	// column index offset in null map, will be 1 when if there is usedFlag and 0 if there is no usedFlag
	colOffsetInNullMap int
	// keyMode is the key mode, it can be OneInt/FixedSerializedKey/VariableSerializedKey
	keyMode keyMode
	// offset to rowData, -1 for variable length, non-inlined key
	rowDataOffset int
	// fakeKeyByte is used as the fake key when current join need keep invalid key rows
	fakeKeyByte []byte
}

func (meta *TableMeta) getSerializedKeyLength(rowStart unsafe.Pointer) uint64 {
	return *(*uint64)(unsafe.Add(rowStart, sizeOfNextPtr+meta.nullMapLength))
}

// used in tests
func (meta *TableMeta) getKeyBytes(rowStart unsafe.Pointer) []byte {
	switch meta.keyMode {
	case OneInt64:
		return hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr), int(serialization.Uint64Len))
	case FixedSerializedKey:
		return hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr), meta.joinKeysLength)
	case VariableSerializedKey:
		return hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr+sizeOfLengthField), int(meta.getSerializedKeyLength(rowStart)))
	default:
		panic("unknown key match type")
	}
}

func (meta *TableMeta) advanceToRowData(matchedRowInfo *matchedRowInfo) {
	if meta.rowDataOffset == -1 {
		// variable length, non-inlined key
		matchedRowInfo.buildRowOffset = sizeOfNextPtr + meta.nullMapLength + sizeOfLengthField + int(meta.getSerializedKeyLength(*(*unsafe.Pointer)(unsafe.Pointer(&matchedRowInfo.buildRowStart))))
	} else {
		matchedRowInfo.buildRowOffset = meta.rowDataOffset
	}
}

func (meta *TableMeta) isColumnNull(rowStart unsafe.Pointer, columnIndex int) bool {
	byteIndex := (columnIndex + meta.colOffsetInNullMap) / 8
	bitIndex := (columnIndex + meta.colOffsetInNullMap) % 8
	return *(*uint8)(unsafe.Add(rowStart, sizeOfNextPtr+byteIndex))&(uint8(1)<<(7-bitIndex)) != uint8(0)
}

// for join that need to set UsedFlag during probe stage, read from nullMap is not thread safe for the first 32 bit of nullMap, atomic.LoadUint32 is used to avoid read-write conflict
func (*TableMeta) isColumnNullThreadSafe(rowStart unsafe.Pointer, columnIndex int) bool {
	return atomic.LoadUint32((*uint32)(unsafe.Add(rowStart, sizeOfNextPtr)))&bitMaskInUint32[columnIndex+1] != uint32(0)
}

func (*TableMeta) setUsedFlag(rowStart unsafe.Pointer) {
	addr := (*uint32)(unsafe.Add(rowStart, sizeOfNextPtr))
	value := atomic.LoadUint32(addr)
	value |= usedFlagMask
	atomic.StoreUint32(addr, value)
}

func (*TableMeta) isCurrentRowUsed(rowStart unsafe.Pointer) bool {
	return (*(*uint32)(unsafe.Add(rowStart, sizeOfNextPtr)) & usedFlagMask) == usedFlagMask
}

type rowTable struct {
	meta     *TableMeta
	segments []*rowTableSegment
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

type keyProp struct {
	canBeInlined  bool
	keyLength     int
	isKeyInteger  bool
	isKeyUnsigned bool
}

func getKeyProp(tp *types.FieldType) *keyProp {
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear,
		mysql.TypeDuration:
		isKeyUnsigned := mysql.HasUnsignedFlag(tp.GetFlag())
		if tp.GetType() == mysql.TypeYear {
			// year type is always unsigned
			isKeyUnsigned = true
		} else if tp.GetType() == mysql.TypeDuration {
			// duration type is always signed
			isKeyUnsigned = false
		}
		return &keyProp{canBeInlined: true, keyLength: chunk.GetFixedLen(tp), isKeyInteger: true, isKeyUnsigned: isKeyUnsigned}
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		collator := collate.GetCollator(tp.GetCollate())
		return &keyProp{canBeInlined: collate.CanUseRawMemAsKey(collator), keyLength: chunk.VarElemLen, isKeyInteger: false, isKeyUnsigned: false}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		// date related type will use uint64 as serialized key
		return &keyProp{canBeInlined: false, keyLength: int(serialization.Uint64Len), isKeyInteger: true, isKeyUnsigned: true}
	case mysql.TypeFloat:
		// float will use float64 as serialized key
		return &keyProp{canBeInlined: false, keyLength: int(serialization.Float64Len), isKeyInteger: false, isKeyUnsigned: false}
	case mysql.TypeNewDecimal:
		// Although decimal is fixed length, but its key is not fixed length
		return &keyProp{canBeInlined: false, keyLength: chunk.VarElemLen, isKeyInteger: false, isKeyUnsigned: false}
	case mysql.TypeEnum:
		if mysql.HasEnumSetAsIntFlag(tp.GetFlag()) {
			// enum int type is always unsigned
			return &keyProp{canBeInlined: false, keyLength: int(serialization.Uint64Len), isKeyInteger: true, isKeyUnsigned: true}
		}
		return &keyProp{canBeInlined: false, keyLength: chunk.VarElemLen, isKeyInteger: false, isKeyUnsigned: false}
	case mysql.TypeBit:
		// bit type is always unsigned
		return &keyProp{canBeInlined: false, keyLength: int(serialization.Uint64Len), isKeyInteger: true, isKeyUnsigned: true}
	default:
		keyLength := chunk.GetFixedLen(tp)
		return &keyProp{canBeInlined: false, keyLength: keyLength, isKeyInteger: false, isKeyUnsigned: false}
	}
}

// buildKeyIndex is the build key column index based on buildSchema, should not be nil
// otherConditionColIndex is the column index that will be used in other condition, if no other condition, will be nil
// columnsNeedConvertToRow is the column index that need to be converted to row, should not be nil
// needUsedFlag is true for outer/semi join that use outer to build
func newTableMeta(buildKeyIndex []int, buildTypes, buildKeyTypes, probeKeyTypes []*types.FieldType, columnsUsedByOtherCondition []int, outputColumns []int, needUsedFlag bool) *TableMeta {
	meta := &TableMeta{}
	meta.isFixedLength = true
	meta.rowLength = 0
	meta.totalColumnNumber = len(buildTypes)

	columnsNeedToBeSaved := make(map[int]struct{}, len(buildTypes))
	updateMeta := func(index int) {
		if _, ok := columnsNeedToBeSaved[index]; !ok {
			columnsNeedToBeSaved[index] = struct{}{}
			length := chunk.GetFixedLen(buildTypes[index])
			if length == chunk.VarElemLen {
				meta.isFixedLength = false
			} else {
				meta.rowLength += length
			}
		}
	}
	if outputColumns == nil {
		// outputColumns = nil means all the column is needed
		for index := range buildTypes {
			updateMeta(index)
		}
	} else {
		for _, index := range outputColumns {
			updateMeta(index)
		}
		for _, index := range columnsUsedByOtherCondition {
			updateMeta(index)
		}
	}

	meta.isJoinKeysFixedLength = true
	meta.joinKeysLength = 0
	meta.isJoinKeysInlined = true
	keyIndexMap := make(map[int]struct{})
	meta.serializeModes = make([]codec.SerializeMode, 0, len(buildKeyIndex))
	isAllKeyInteger := true
	hasFixedSizeKeyColumn := false
	varLengthKeyNumber := 0
	for index, keyIndex := range buildKeyIndex {
		keyType := buildKeyTypes[index]
		prop := getKeyProp(keyType)
		if prop.keyLength != chunk.VarElemLen {
			meta.joinKeysLength += prop.keyLength
			hasFixedSizeKeyColumn = true
		} else {
			meta.isJoinKeysFixedLength = false
			varLengthKeyNumber++
		}
		if !prop.canBeInlined {
			meta.isJoinKeysInlined = false
		}
		if prop.isKeyInteger {
			buildUnsigned := prop.isKeyUnsigned
			probeKeyProp := getKeyProp(probeKeyTypes[index])
			if !probeKeyProp.isKeyInteger {
				panic("build key is integer but probe key is not integer, should not happens")
			}
			probeUnsigned := probeKeyProp.isKeyUnsigned
			if (buildUnsigned && !probeUnsigned) || (probeUnsigned && !buildUnsigned) {
				meta.serializeModes = append(meta.serializeModes, codec.NeedSignFlag)
				meta.isJoinKeysInlined = false
				if meta.isJoinKeysFixedLength {
					// an extra sign flag is needed in this case
					meta.joinKeysLength++
				}
			} else {
				meta.serializeModes = append(meta.serializeModes, codec.Normal)
			}
		} else {
			if !prop.isKeyInteger {
				isAllKeyInteger = false
			}
			if prop.keyLength == chunk.VarElemLen {
				// keep var column by default for var length column
				meta.serializeModes = append(meta.serializeModes, codec.KeepVarColumnLength)
			} else {
				meta.serializeModes = append(meta.serializeModes, codec.Normal)
			}
		}
		keyIndexMap[keyIndex] = struct{}{}
	}
	if !meta.isJoinKeysFixedLength {
		meta.joinKeysLength = -1
	}
	if len(buildKeyIndex) != len(keyIndexMap) {
		// has duplicated key, can not be inlined
		meta.isJoinKeysInlined = false
	}
	if !meta.isJoinKeysInlined {
		if varLengthKeyNumber == 1 {
			// if key is not inlined and there is only one var-length key, then don't need to record the var length
			for i := 0; i < len(buildKeyIndex); i++ {
				if meta.serializeModes[i] == codec.KeepVarColumnLength {
					meta.serializeModes[i] = codec.Normal
				}
			}
		}
	} else {
		for _, index := range buildKeyIndex {
			updateMeta(index)
		}
	}
	if !meta.isFixedLength {
		meta.rowLength = 0
	}
	// construct the column order
	meta.rowColumnsOrder = make([]int, 0, len(columnsNeedToBeSaved))
	meta.columnsSize = make([]int, 0, len(columnsNeedToBeSaved))
	usedColumnMap := make(map[int]struct{}, len(columnsNeedToBeSaved))

	updateColumnOrder := func(index int) {
		if _, ok := usedColumnMap[index]; !ok {
			meta.rowColumnsOrder = append(meta.rowColumnsOrder, index)
			meta.columnsSize = append(meta.columnsSize, chunk.GetFixedLen(buildTypes[index]))
			usedColumnMap[index] = struct{}{}
		}
	}
	if meta.isJoinKeysInlined {
		// if join key is inlined, the join key will be the first columns
		for _, index := range buildKeyIndex {
			updateColumnOrder(index)
		}
	}
	meta.columnCountNeededForOtherCondition = 0
	if len(columnsUsedByOtherCondition) > 0 {
		// if join has other condition, the columns used by other condition is appended to row layout after the key
		for _, index := range columnsUsedByOtherCondition {
			updateColumnOrder(index)
		}
		meta.columnCountNeededForOtherCondition = len(usedColumnMap)
	}
	if outputColumns == nil {
		// outputColumns = nil means all the column is needed
		for index := range buildTypes {
			updateColumnOrder(index)
		}
	} else {
		for _, index := range outputColumns {
			updateColumnOrder(index)
		}
	}
	if isAllKeyInteger && len(buildKeyIndex) == 1 && meta.serializeModes[0] != codec.NeedSignFlag {
		meta.keyMode = OneInt64
	} else {
		if meta.isJoinKeysFixedLength {
			meta.keyMode = FixedSerializedKey
		} else {
			meta.keyMode = VariableSerializedKey
		}
	}
	if needUsedFlag {
		meta.colOffsetInNullMap = 1
		// the total row length should be larger than 4 byte since the smallest unit of atomic.LoadXXX is UInt32
		if len(columnsNeedToBeSaved) > 0 {
			// the smallest length of a column is 4 byte, so the total row length is enough
			meta.nullMapLength = (len(columnsNeedToBeSaved) + 1 + 7) / 8
		} else {
			// if no columns need to be converted to row format, then the key is not inlined
			// 1. if any of the key columns is fixed length, then the row length is larger than 4 bytes(since the smallest length of a fixed length column is 4 bytes)
			// 2. if all the key columns are variable length, there is no guarantee that the row length is larger than 4 byte, the nullmap should be 4 bytes alignment
			if hasFixedSizeKeyColumn {
				meta.nullMapLength = (len(columnsNeedToBeSaved) + 1 + 7) / 8
			} else {
				meta.nullMapLength = ((len(columnsNeedToBeSaved) + 1 + 31) / 32) * 4
			}
		}
	} else {
		meta.colOffsetInNullMap = 0
		meta.nullMapLength = (len(columnsNeedToBeSaved) + 7) / 8
	}
	meta.rowDataOffset = -1
	if meta.isJoinKeysInlined {
		if meta.isJoinKeysFixedLength {
			meta.rowDataOffset = sizeOfNextPtr + meta.nullMapLength
		} else {
			meta.rowDataOffset = sizeOfNextPtr + meta.nullMapLength + sizeOfLengthField
		}
	} else {
		if meta.isJoinKeysFixedLength {
			meta.rowDataOffset = sizeOfNextPtr + meta.nullMapLength + meta.joinKeysLength
		}
	}
	if meta.isJoinKeysFixedLength && !meta.isJoinKeysInlined {
		meta.fakeKeyByte = make([]byte, meta.joinKeysLength)
	}
	return meta
}

type rowTableBuilder struct {
	buildKeyIndex    []int
	buildKeyTypes    []*types.FieldType
	hasNullableKey   bool
	hasFilter        bool
	keepFilteredRows bool

	serializedKeyVectorBuffer [][]byte
	partIdxVector             []int
	selRows                   []int
	usedRows                  []int
	hashValue                 []uint64
	firstSegRowSizeHint       uint
	// filterVector and nullKeyVector is indexed by physical row index because the return vector of VectorizedFilter is based on physical row index
	filterVector  []bool // if there is filter before probe, filterVector saves the filter result
	nullKeyVector []bool // nullKeyVector[i] = true if any of the key is null

	rowNumberInCurrentRowTableSeg []int64
}

func createRowTableBuilder(buildKeyIndex []int, buildKeyTypes []*types.FieldType, partitionNumber uint, hasNullableKey bool, hasFilter bool, keepFilteredRows bool) *rowTableBuilder {
	builder := &rowTableBuilder{
		buildKeyIndex:                 buildKeyIndex,
		buildKeyTypes:                 buildKeyTypes,
		rowNumberInCurrentRowTableSeg: make([]int64, partitionNumber),
		hasNullableKey:                hasNullableKey,
		hasFilter:                     hasFilter,
		keepFilteredRows:              keepFilteredRows,
	}
	return builder
}

func (b *rowTableBuilder) initHashValueAndPartIndexForOneChunk(partitionMaskOffset int, partitionNumber uint) {
	h := fnv.New64()
	fakePartIndex := uint64(0)
	for logicalRowIndex, physicalRowIndex := range b.usedRows {
		if (b.filterVector != nil && !b.filterVector[physicalRowIndex]) || (b.nullKeyVector != nil && b.nullKeyVector[physicalRowIndex]) {
			b.hashValue[logicalRowIndex] = fakePartIndex
			b.partIdxVector[logicalRowIndex] = int(fakePartIndex)
			fakePartIndex = (fakePartIndex + 1) % uint64(partitionNumber)
			continue
		}
		h.Write(b.serializedKeyVectorBuffer[logicalRowIndex])
		hash := h.Sum64()
		b.hashValue[logicalRowIndex] = hash
		b.partIdxVector[logicalRowIndex] = int(hash >> partitionMaskOffset)
		h.Reset()
	}
}

func (b *rowTableBuilder) processOneChunk(chk *chunk.Chunk, typeCtx types.Context, hashJoinCtx *HashJoinCtxV2, workerID int) error {
	b.ResetBuffer(chk)
	b.firstSegRowSizeHint = max(uint(1), uint(float64(len(b.usedRows))/float64(hashJoinCtx.partitionNumber)*float64(1.2)))
	var err error
	if b.hasFilter {
		b.filterVector, err = expression.VectorizedFilter(hashJoinCtx.SessCtx.GetExprCtx().GetEvalCtx(), hashJoinCtx.SessCtx.GetSessionVars().EnableVectorizedExpression, hashJoinCtx.BuildFilter, chunk.NewIterator4Chunk(chk), b.filterVector)
		if err != nil {
			return err
		}
	}
	err = checkSQLKiller(&hashJoinCtx.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuild")
	if err != nil {
		return err
	}
	// 1. split partition
	for index, colIdx := range b.buildKeyIndex {
		err := codec.SerializeKeys(typeCtx, chk, b.buildKeyTypes[index], colIdx, b.usedRows, b.filterVector, b.nullKeyVector, hashJoinCtx.hashTableMeta.serializeModes[index], b.serializedKeyVectorBuffer)
		if err != nil {
			return err
		}
	}
	err = checkSQLKiller(&hashJoinCtx.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuild")
	if err != nil {
		return err
	}

	b.initHashValueAndPartIndexForOneChunk(hashJoinCtx.partitionMaskOffset, hashJoinCtx.partitionNumber)

	// 2. build rowtable
	return b.appendToRowTable(chk, hashJoinCtx, workerID)
}

func resizeSlice[T int | uint64 | bool](s []T, newSize int) []T {
	if cap(s) >= newSize {
		s = s[:newSize]
	} else {
		s = make([]T, newSize)
	}
	return s
}

func (b *rowTableBuilder) ResetBuffer(chk *chunk.Chunk) {
	b.usedRows = chk.Sel()
	logicalRows := chk.NumRows()
	physicalRows := chk.Column(0).Rows()

	if b.usedRows == nil {
		b.selRows = resizeSlice(b.selRows, logicalRows)
		for i := 0; i < logicalRows; i++ {
			b.selRows[i] = i
		}
		b.usedRows = b.selRows
	}
	b.partIdxVector = resizeSlice(b.partIdxVector, logicalRows)
	b.hashValue = resizeSlice(b.hashValue, logicalRows)
	if b.hasFilter {
		b.filterVector = resizeSlice(b.filterVector, physicalRows)
	}
	if b.hasNullableKey {
		b.nullKeyVector = resizeSlice(b.nullKeyVector, physicalRows)
		for i := 0; i < physicalRows; i++ {
			b.nullKeyVector[i] = false
		}
	}
	if cap(b.serializedKeyVectorBuffer) >= logicalRows {
		b.serializedKeyVectorBuffer = b.serializedKeyVectorBuffer[:logicalRows]
		for i := 0; i < logicalRows; i++ {
			b.serializedKeyVectorBuffer[i] = b.serializedKeyVectorBuffer[i][:0]
		}
	} else {
		b.serializedKeyVectorBuffer = make([][]byte, logicalRows)
	}
}

func newRowTable(meta *TableMeta) *rowTable {
	return &rowTable{
		meta:     meta,
		segments: make([]*rowTableSegment, 0),
	}
}

func (b *rowTableBuilder) appendRemainingRowLocations(workerID int, htCtx *hashTableContext) {
	for partID := 0; partID < int(htCtx.hashTable.partitionNumber); partID++ {
		if b.rowNumberInCurrentRowTableSeg[partID] > 0 {
			htCtx.finalizeCurrentSeg(workerID, partID, b)
		}
	}
}

func fillNullMap(rowTableMeta *TableMeta, row *chunk.Row, seg *rowTableSegment) int {
	if nullMapLength := rowTableMeta.nullMapLength; nullMapLength > 0 {
		bitmap := make([]byte, nullMapLength)
		for colIndexInRowTable, colIndexInRow := range rowTableMeta.rowColumnsOrder {
			colIndexInBitMap := colIndexInRowTable + rowTableMeta.colOffsetInNullMap
			if row.IsNull(colIndexInRow) {
				bitmap[colIndexInBitMap/8] |= 1 << (7 - colIndexInBitMap%8)
			}
		}
		seg.rawData = append(seg.rawData, bitmap...)
		return nullMapLength
	}
	return 0
}

func fillNextRowPtr(seg *rowTableSegment) int {
	seg.rawData = append(seg.rawData, fakeAddrPlaceHolder...)
	return sizeOfNextPtr
}

func (b *rowTableBuilder) fillSerializedKeyAndKeyLengthIfNeeded(rowTableMeta *TableMeta, hasValidKey bool, logicalRowIndex int, seg *rowTableSegment) int {
	appendRowLength := 0
	// 1. fill key length if needed
	if !rowTableMeta.isJoinKeysFixedLength {
		// if join_key is not fixed length: `key_length` need to be written in rawData
		// even the join keys is inlined, for example if join key is 2 binary string
		// then the inlined join key should be: col1_size + col1_data + col2_size + col2_data
		// and len(col1_size + col1_data + col2_size + col2_data) need to be written before the inlined join key
		length := uint64(0)
		if hasValidKey {
			length = uint64(len(b.serializedKeyVectorBuffer[logicalRowIndex]))
		} else {
			length = 0
		}
		seg.rawData = append(seg.rawData, unsafe.Slice((*byte)(unsafe.Pointer(&length)), sizeOfLengthField)...)
		appendRowLength += sizeOfLengthField
	}
	// 2. fill serialized key if needed
	if !rowTableMeta.isJoinKeysInlined {
		// if join_key is not inlined: `serialized_key` need to be written in rawData
		if hasValidKey {
			seg.rawData = append(seg.rawData, b.serializedKeyVectorBuffer[logicalRowIndex]...)
			appendRowLength += len(b.serializedKeyVectorBuffer[logicalRowIndex])
		} else {
			// if there is no valid key, and the key is fixed length, then write a fake key
			if rowTableMeta.isJoinKeysFixedLength {
				seg.rawData = append(seg.rawData, rowTableMeta.fakeKeyByte...)
				appendRowLength += rowTableMeta.joinKeysLength
			}
			// otherwise don't need to write since length is 0
		}
	}
	return appendRowLength
}

func fillRowData(rowTableMeta *TableMeta, row *chunk.Row, seg *rowTableSegment) int {
	appendRowLength := 0
	for index, colIdx := range rowTableMeta.rowColumnsOrder {
		if rowTableMeta.columnsSize[index] > 0 {
			// fixed size
			seg.rawData = append(seg.rawData, row.GetRaw(colIdx)...)
			appendRowLength += rowTableMeta.columnsSize[index]
		} else {
			// length, raw_data
			raw := row.GetRaw(colIdx)
			length := uint64(len(raw))
			seg.rawData = append(seg.rawData, unsafe.Slice((*byte)(unsafe.Pointer(&length)), sizeOfLengthField)...)
			appendRowLength += sizeOfLengthField
			seg.rawData = append(seg.rawData, raw...)
			appendRowLength += int(length)
		}
	}
	return appendRowLength
}

func (b *rowTableBuilder) appendToRowTable(chk *chunk.Chunk, hashJoinCtx *HashJoinCtxV2, workerID int) error {
	rowTableMeta := hashJoinCtx.hashTableMeta
	for logicalRowIndex, physicalRowIndex := range b.usedRows {
		if logicalRowIndex%10 == 0 || logicalRowIndex == len(b.usedRows)-1 {
			err := checkSQLKiller(&hashJoinCtx.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuild")
			if err != nil {
				return err
			}
		}
		hasValidKey := (!b.hasFilter || b.filterVector[physicalRowIndex]) && (!b.hasNullableKey || !b.nullKeyVector[physicalRowIndex])
		if !hasValidKey && !b.keepFilteredRows {
			continue
		}
		// need append the row to rowTable
		var (
			row     = chk.GetRow(logicalRowIndex)
			partIdx = b.partIdxVector[logicalRowIndex]
			seg     *rowTableSegment
		)
		seg = hashJoinCtx.hashTableContext.getCurrentRowSegment(workerID, partIdx, hashJoinCtx.hashTableMeta, true, b.firstSegRowSizeHint)
		// first check if current seg is full
		if b.rowNumberInCurrentRowTableSeg[partIdx] >= maxRowTableSegmentSize || len(seg.rawData) >= maxRowTableSegmentByteSize {
			// finalize current seg and create a new seg
			hashJoinCtx.hashTableContext.finalizeCurrentSeg(workerID, partIdx, b)
			seg = hashJoinCtx.hashTableContext.getCurrentRowSegment(workerID, partIdx, hashJoinCtx.hashTableMeta, true, b.firstSegRowSizeHint)
		}
		if hasValidKey {
			seg.validJoinKeyPos = append(seg.validJoinKeyPos, len(seg.hashValues))
		}
		seg.hashValues = append(seg.hashValues, b.hashValue[logicalRowIndex])
		seg.rowStartOffset = append(seg.rowStartOffset, uint64(len(seg.rawData)))
		rowLength := 0
		// fill next_row_ptr field
		rowLength += fillNextRowPtr(seg)
		// fill null_map
		rowLength += fillNullMap(rowTableMeta, &row, seg)
		// fill serialized key and key length if needed
		rowLength += b.fillSerializedKeyAndKeyLengthIfNeeded(rowTableMeta, hasValidKey, logicalRowIndex, seg)
		// fill row data
		rowLength += fillRowData(rowTableMeta, &row, seg)
		// to make sure rowLength is 8 bit alignment
		if rowLength%8 != 0 {
			seg.rawData = append(seg.rawData, fakeAddrPlaceHolder[:8-rowLength%8]...)
		}
		b.rowNumberInCurrentRowTableSeg[partIdx]++
	}
	return nil
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
