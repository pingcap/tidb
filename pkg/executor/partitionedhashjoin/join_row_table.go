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

package partitionedhashjoin

import (
	"encoding/binary"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const SizeOfNextPtr = int(unsafe.Sizeof(*new(unsafe.Pointer)))
const SizeOfKeyLengthField = int(unsafe.Sizeof(uint64(1)))

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
	   variable length(e.g. string related elements), it will first save the size followed by the raw data. Since the row_data is variable length,
	   it is designed to access the column data in order. In order to avoid random access of the column data in row_data, the column order in the
	   row_data will be adjusted to fit the usage order, more specifically the column order will be
	   * join key is inlined + have other conditions: join keys, column used in other condition, rest columns that will be used as join output
	   * join key is inlined + no other conditions: join keys, rest columns that will be used as join output
	   * join key is not inlined + have other conditions: columns used in other condition, rest columns that will be used as join output
	   * join key is not inlined + no other conditions: columns that will be used as join output
	*/
	rawData         []byte           // the chunk of memory to save the row data
	hashValues      []uint64         // the hash value of each rows
	rowLocations    []unsafe.Pointer // the start address of each row
	validJoinKeyPos []int            // the pos of rows that need to be inserted into hash table, used in hash table build
}

const MAX_ROW_TABLE_SEGMENT_SIZE = 1024

func newRowTableSegment() *rowTableSegment {
	return &rowTableSegment{
		// TODO: @XuHuaiyu if joinKeyIsInlined, the cap of rawData can be calculated
		rawData:         make([]byte, 0),
		hashValues:      make([]uint64, 0, MAX_ROW_TABLE_SEGMENT_SIZE),
		rowLocations:    make([]unsafe.Pointer, 0, MAX_ROW_TABLE_SEGMENT_SIZE),
		validJoinKeyPos: make([]int, 0, MAX_ROW_TABLE_SEGMENT_SIZE),
	}
}

func (rts *rowTableSegment) rowCount() int64 {
	return int64(len(rts.rowLocations))
}

func (rts *rowTableSegment) validKeyCount() uint64 {
	return uint64(len(rts.validJoinKeyPos))
}

func setNextRowAddress(rowStart unsafe.Pointer, nextRowAddress unsafe.Pointer) {
	*(*unsafe.Pointer)(rowStart) = nextRowAddress
}

func getNextRowAddress(rowStart unsafe.Pointer) unsafe.Pointer {
	return *(*unsafe.Pointer)(rowStart)
}

type JoinTableMeta struct {
	// if the row has fixed length
	isFixedLength bool
	// the row length if the row is fixed length
	rowLength int
	// if the join keys has fixed length
	isJoinKeysFixedLength bool
	// the join keys length if it is fixed length
	joinKeysLength int
	// is the join key inlined in the row data
	isJoinKeysInlined bool
	// the length of null map, the null map include null bit for each column in the row and the used flag for right semi/outer join
	nullMapLength int
	// the column order in row layout, as described above, the save column order maybe different from the column order in build schema
	// for example, the build schema maybe [col1, col2, col3], and the column order in row maybe [col2, col1, col3], then this array
	// is [1, 0, 2]
	rowColumnsOrder []int
	// the column size of each column, -1 mean variable column
	columnsSize []int
	// when serialize integer column, should ignore unsigned flag or not, if the join key is <signed, signed> or <unsigned, unsigned>,
	// the unsigned flag can be ignored, if the join key is <unsigned, signed> or <signed, unsigned> the unsigned flag can not be ignored
	// can if the unsigned flag can not be ignored, the key can not be inlined
	ignoreIntegerKeySignFlag []bool
	// the first n columns in row is used for other condition, if a join has other condition, we only need to extract
	// first n columns from the RowTable to evaluate other condition
	columnCountNeededForOtherCondition int
	// total column numbers for build side chunk, this is used to construct the chunk if there is join other condition
	totalColumnNumber int
	// a mask to set used flag
	setUsedFlagMask uint32
	// column index offset in null map, will be 1 when if there is usedFlag and 0 if there is no usedFlag
	colOffsetInNullMap int
}

func (meta *JoinTableMeta) getSerializedKeyLength(rowStart unsafe.Pointer) uint64 {
	return *(*uint64)(unsafe.Add(rowStart, SizeOfNextPtr+meta.nullMapLength))
}

func (meta *JoinTableMeta) advanceToRowData(rowStart unsafe.Pointer) unsafe.Pointer {
	if meta.isJoinKeysInlined {
		// join key is inlined
		if meta.isJoinKeysFixedLength {
			return unsafe.Add(rowStart, SizeOfNextPtr+meta.nullMapLength)
		}
		return unsafe.Add(rowStart, SizeOfNextPtr+meta.nullMapLength+SizeOfKeyLengthField)
	}
	// join key is not inlined
	return unsafe.Add(rowStart, SizeOfNextPtr+meta.nullMapLength+SizeOfKeyLengthField+int(meta.getSerializedKeyLength(rowStart)))
}

func (meta *JoinTableMeta) isColumnNull(rowStart unsafe.Pointer, columnIndex int) bool {
	byteIndex := (columnIndex + 1) / 8
	bitIndex := (columnIndex + 1) % 8
	return *(*uint8)(unsafe.Add(rowStart, SizeOfNextPtr+byteIndex))&(uint8(1)<<(7-bitIndex)) != uint8(0)
}

func (meta *JoinTableMeta) setUsedFlag(rowStart unsafe.Pointer) {
	addr := (*uint32)(unsafe.Add(rowStart, SizeOfNextPtr))
	value := atomic.LoadUint32(addr)
	value |= meta.setUsedFlagMask
	atomic.StoreUint32(addr, value)
}

func (meta *JoinTableMeta) isCurrentRowUsed(rowStart unsafe.Pointer) bool {
	return (*(*uint32)(unsafe.Add(rowStart, SizeOfNextPtr)) | meta.setUsedFlagMask) == meta.setUsedFlagMask
}

type rowTable struct {
	meta     *JoinTableMeta
	segments []*rowTableSegment
}

func canBeInlinedAsJoinKey(tp *types.FieldType) bool {
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear,
		mysql.TypeDuration:
		return true
	// todo 1. check if date/datetime/timestamp can be inlined
	//      2. string related type with binCollator can be inlined
	default:
		return false
	}
}

// buildKeyIndex is the build key column index based on buildSchema, should not be nil
// otherConditionColIndex is the column index that will be used in other condition, if no other condition, will be nil
// columnsNeedConvertToRow is the column index that need to be converted to row, should not be nil
// needUsedFlag is true for outer/semi join that use outer to build
func newTableMeta(buildKeyIndex []int, buildTypes, buildKeyTypes, probeKeyTypes []*types.FieldType, columnsUsedByOtherCondition []int, outputColumns []int, needUsedFlag bool) *JoinTableMeta {
	meta := &JoinTableMeta{}
	meta.isFixedLength = true
	meta.rowLength = 0
	savedColumnCount := 0
	meta.totalColumnNumber = len(buildTypes)
	updateMeta := func(colType *types.FieldType) {
		length := chunk.GetFixedLen(colType)
		if length == chunk.VarElemLen {
			meta.isFixedLength = false
		} else {
			meta.rowLength += length
		}
		savedColumnCount++
	}
	if outputColumns == nil {
		// outputColumns = nil means all the column is needed
		for _, colType := range buildTypes {
			updateMeta(colType)
		}
	} else {
		usedColumnMap := make(map[int]struct{}, len(outputColumns))
		for _, index := range outputColumns {
			updateMeta(buildTypes[index])
			usedColumnMap[index] = struct{}{}
		}
		if columnsUsedByOtherCondition != nil {
			for _, index := range columnsUsedByOtherCondition {
				if _, ok := usedColumnMap[index]; !ok {
					updateMeta(buildTypes[index])
					usedColumnMap[index] = struct{}{}
				}
			}
		}
	}
	if !meta.isFixedLength {
		meta.rowLength = 0
	}
	// nullmap is 8 byte alignment
	if needUsedFlag {
		meta.nullMapLength = (savedColumnCount + 1 + 63) / 64
		meta.setUsedFlagMask = uint32(1) << 31
	} else {
		meta.nullMapLength = (savedColumnCount + 63) / 64
	}

	meta.isJoinKeysFixedLength = true
	meta.joinKeysLength = 0
	meta.isJoinKeysInlined = true
	keyIndexMap := make(map[int]struct{})
	meta.ignoreIntegerKeySignFlag = make([]bool, 0, len(buildKeyIndex))
	for index, keyIndex := range buildKeyIndex {
		keyType := buildKeyTypes[index]
		keyLength := chunk.GetFixedLen(keyType)
		if keyLength == chunk.VarElemLen {
			meta.isJoinKeysFixedLength = false
		} else {
			meta.joinKeysLength += keyLength
		}
		if !canBeInlinedAsJoinKey(keyType) {
			meta.isJoinKeysInlined = false
		}
		if mysql.IsIntegerType(keyType.GetType()) {
			buildUnsigned := mysql.HasUnsignedFlag(keyType.GetFlag())
			probeUnsigned := mysql.HasUnsignedFlag(probeKeyTypes[index].GetFlag())
			if (buildUnsigned && !probeUnsigned) || (probeUnsigned && !buildUnsigned) {
				meta.ignoreIntegerKeySignFlag = append(meta.ignoreIntegerKeySignFlag, false)
				meta.isJoinKeysInlined = false
			} else {
				meta.ignoreIntegerKeySignFlag = append(meta.ignoreIntegerKeySignFlag, true)
			}
		} else {
			meta.ignoreIntegerKeySignFlag = append(meta.ignoreIntegerKeySignFlag, true)
		}
		keyIndexMap[keyIndex] = struct{}{}
	}
	if !meta.isJoinKeysFixedLength {
		meta.joinKeysLength = 0
	}
	if len(buildKeyIndex) != len(keyIndexMap) {
		// has duplicated key, can not be inlined
		meta.isJoinKeysInlined = false
	}
	// construct the column order
	meta.rowColumnsOrder = make([]int, savedColumnCount)
	meta.columnsSize = make([]int, savedColumnCount)
	usedColumnMap := make(map[int]struct{}, savedColumnCount)

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
	for _, index := range outputColumns {
		updateColumnOrder(index)
	}
	return meta
}

type rowTableBuilder struct {
	buildKeyIndex           []int
	buildSchema             expression.Schema
	probeKeyIndex           []int
	probeSchema             expression.Schema
	otherConditionColIndex  []int
	columnsNeedConvertToRow []int
	needUsedFlag            bool

	serializedKeyVectorBuffer [][]byte
	rowTables                 []*rowTable
	crrntSizeOfRowTable       []int64
	// store the start position of each row in the rawData,
	// we'll use this temp array to get the address of each row at the end
	startPosInRawData [][]uint64
	partIdxVector     []int
	hashValue         []uint32
}

func newRowTable(meta *JoinTableMeta) *rowTable {
	return &rowTable{
		meta:     meta,
		segments: make([]*rowTableSegment, 0),
	}
}

func (builder *rowTableBuilder) ClearBuffer(rowCnt int) {
	builder.serializedKeyVectorBuffer = builder.serializedKeyVectorBuffer[:rowCnt]
	for i := range builder.serializedKeyVectorBuffer {
		builder.serializedKeyVectorBuffer[i] = builder.serializedKeyVectorBuffer[i][:0]
	}
	builder.serializedKeyVectorBuffer = builder.serializedKeyVectorBuffer[:]
}

func (builder *rowTableBuilder) appendToRowTable(typeCtx types.Context, chk *chunk.Chunk, rowTableMeta *JoinTableMeta) {
	fakeAddrByte := make([]byte, 8)
	for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
		var (
			row               = chk.GetRow(rowIdx)
			partIdx           = builder.partIdxVector[rowIdx]
			startPosInRawData = builder.startPosInRawData[partIdx]
			seg               *rowTableSegment
		)
		if builder.rowTables[partIdx] == nil {
			builder.rowTables[partIdx] = newRowTable(rowTableMeta)
			seg = newRowTableSegment()
			builder.rowTables[partIdx].segments = append(builder.rowTables[partIdx].segments, seg)
		} else if builder.crrntSizeOfRowTable[partIdx]%MAX_ROW_TABLE_SEGMENT_SIZE == 0 {
			for _, pos := range startPosInRawData {
				seg.rowLocations = append(seg.rowLocations, unsafe.Pointer(&seg.rawData[pos]))
			}
			builder.startPosInRawData[partIdx] = builder.startPosInRawData[partIdx][:]
			startPosInRawData = builder.startPosInRawData[partIdx]
			seg = newRowTableSegment()
			builder.rowTables[partIdx].segments = append(builder.rowTables[partIdx].segments, seg)
		}

		seg.hashValues = append(seg.hashValues, uint64(builder.hashValue[rowIdx]))
		// TODO: @XuHuaiyu validJoinKeyPos
		seg.validJoinKeyPos = append(seg.validJoinKeyPos, rowIdx)

		startPosInRawData = append(startPosInRawData, uint64(len(seg.rawData)))
		// next_row_ptr
		seg.rawData = append(seg.rawData, fakeAddrByte...)
		// TODO: @XuHuaiyu 补充 null_map
		if len := rowTableMeta.nullMapLength; len > 0 {
			seg.rawData = append(seg.rawData, make([]byte, len)...)
		}
		// if join_key is not inlined: `key_length + serialized_key`
		// if join_key is inlined: `join_key`` (NOTE: use `key_length + join_key` if the key with variable length can be inlined)
		if rowTableMeta.isJoinKeysInlined {
			for _, colIdx := range builder.buildKeyIndex {
				seg.rawData = append(seg.rawData, row.GetRaw(colIdx)...)
			}
		} else {
			var buf [binary.MaxVarintLen64]byte
			n := binary.PutUvarint(buf[:], uint64(len(builder.serializedKeyVectorBuffer[rowIdx])))
			seg.rawData = append(seg.rawData, buf[:n]...)
			seg.rawData = append(seg.rawData, builder.serializedKeyVectorBuffer[rowIdx]...)
		}
		// append the column data used in other condition, and the column data that will be used as join output
		for _, colIdx := range builder.otherConditionColIndex {
			seg.rawData = append(seg.rawData, row.GetRaw(colIdx)...)
		}
		for _, colIdx := range builder.columnsNeedConvertToRow {
			seg.rawData = append(seg.rawData, row.GetRaw(colIdx)...)
		}
		builder.crrntSizeOfRowTable[partIdx]++
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

// func (rt *rowTable) insert(typeCtx types.Context, chk *chunk.Chunk, builder *rowTableBuilder) error {
// 	builder.appendToRowTable(typeCtx, chk, rt.meta)
// 	return nil
// }
