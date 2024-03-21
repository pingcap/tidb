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
	"github.com/pingcap/tidb/pkg/util/codec"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
)

const SizeOfNextPtr = int(unsafe.Sizeof(*new(unsafe.Pointer)))
const SizeOfLengthField = int(unsafe.Sizeof(uint64(1)))

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
	// the column size of each column, -1 mean variable column, the order is the same as rowColumnsOrder
	columnsSize []int
	// the serialize mode for each key
	serializeModes []codec.SerializeMode
	// the first n columns in row is used for other condition, if a join has other condition, we only need to extract
	// first n columns from the RowTable to evaluate other condition
	columnCountNeededForOtherCondition int
	// total column numbers for build side chunk, this is used to construct the chunk if there is join other condition
	totalColumnNumber int
	// a mask to set used flag
	setUsedFlagMask uint32
	// column index offset in null map, will be 1 when if there is usedFlag and 0 if there is no usedFlag
	colOffsetInNullMap int
	// keyMode is the key mode, it can be OneInt/FixedSerializedKey/VariableSerializedKey
	keyMode keyMode
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
		return unsafe.Add(rowStart, SizeOfNextPtr+meta.nullMapLength+SizeOfLengthField)
	}
	// join key is not inlined
	if meta.isJoinKeysFixedLength {
		return unsafe.Add(rowStart, SizeOfNextPtr+meta.nullMapLength+meta.joinKeysLength)
	}
	return unsafe.Add(rowStart, SizeOfNextPtr+meta.nullMapLength+SizeOfLengthField+int(meta.getSerializedKeyLength(rowStart)))
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
	return (*(*uint32)(unsafe.Add(rowStart, SizeOfNextPtr)) & meta.setUsedFlagMask) == meta.setUsedFlagMask
}

type rowTable struct {
	meta     *JoinTableMeta
	segments []*rowTableSegment
}

func canBeInlinedAsJoinKey(tp *types.FieldType) (bool, bool) {
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear,
		mysql.TypeDuration:
		return true, false
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		collator := collate.GetCollator(tp.GetCollate())
		return collate.CanUseRawMemAsKey(collator), true
	// todo check if date/datetime/timestamp can be inlined
	default:
		return false, false
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
		meta.nullMapLength = ((savedColumnCount + 1 + 63) / 64) * 8
		meta.setUsedFlagMask = uint32(1) << 31
	} else {
		meta.nullMapLength = ((savedColumnCount + 63) / 64) * 8
	}

	meta.isJoinKeysFixedLength = true
	meta.joinKeysLength = 0
	meta.isJoinKeysInlined = true
	keyIndexMap := make(map[int]struct{})
	meta.serializeModes = make([]codec.SerializeMode, 0, len(buildKeyIndex))
	isAllKeyInteger := false
	for index, keyIndex := range buildKeyIndex {
		keyType := buildKeyTypes[index]
		keyLength := chunk.GetFixedLen(keyType)
		if keyLength == chunk.VarElemLen {
			meta.isJoinKeysFixedLength = false
		} else {
			meta.joinKeysLength += keyLength
		}
		canBeInlined, isStringKey := canBeInlinedAsJoinKey(keyType)
		if !canBeInlined {
			meta.isJoinKeysInlined = false
		}
		if mysql.IsIntegerType(keyType.GetType()) {
			buildUnsigned := mysql.HasUnsignedFlag(keyType.GetFlag())
			probeUnsigned := mysql.HasUnsignedFlag(probeKeyTypes[index].GetFlag())
			if (buildUnsigned && !probeUnsigned) || (probeUnsigned && !buildUnsigned) {
				meta.serializeModes = append(meta.serializeModes, codec.None)
				meta.isJoinKeysInlined = false
			} else {
				meta.serializeModes = append(meta.serializeModes, codec.IgnoreIntegerSign)
			}
		} else {
			isAllKeyInteger = false
			if meta.isJoinKeysInlined && isStringKey {
				meta.serializeModes = append(meta.serializeModes, codec.KeepStringLength)
			} else {
				meta.serializeModes = append(meta.serializeModes, codec.None)
			}
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
	if !meta.isJoinKeysInlined {
		for i := 0; i < len(buildKeyIndex); i++ {
			if meta.serializeModes[i] == codec.KeepStringLength {
				meta.serializeModes[i] = codec.None
			}
		}
	}
	// construct the column order
	meta.rowColumnsOrder = make([]int, 0, savedColumnCount)
	meta.columnsSize = make([]int, 0, savedColumnCount)
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
	if isAllKeyInteger && len(buildKeyIndex) == 1 {
		meta.keyMode = OneInt64
	} else {
		if meta.isJoinKeysFixedLength {
			meta.keyMode = FixedSerializedKey
		} else {
			meta.keyMode = VariableSerializedKey
		}
	}
	return meta
}

type rowTableBuilder struct {
	buildKeyIndex   []int
	buildSchema     *expression.Schema
	rowColumnsOrder []int
	columnsSize     []int
	needUsedFlag    bool
	hasNullableKey  bool
	hasFilter       bool

	serializedKeyVectorBuffer [][]byte
	partIdxVector             []int
	hashValue                 []uint64
	filterVector              []bool // if there is filter before probe, filterVector saves the filter result
	nullKeyVector             []bool // nullKeyVector[i] = true if any of the key is null

	crrntSizeOfRowTable []int64
	// store the start position of each row in the rawData,
	// we'll use this temp array to get the address of each row at the end
	startPosInRawData [][]uint64
}

func (b *rowTableBuilder) initBuffer() {
	b.serializedKeyVectorBuffer = make([][]byte, chunk.InitialCapacity)
	b.partIdxVector = make([]int, 0, chunk.InitialCapacity)
	b.hashValue = make([]uint64, 0, chunk.InitialCapacity)
	if b.hasFilter {
		b.filterVector = make([]bool, 0, chunk.InitialCapacity)
	}
	if b.hasNullableKey {
		b.nullKeyVector = make([]bool, 0, chunk.InitialCapacity)
		for i := 0; i < chunk.InitialCapacity; i++ {
			b.nullKeyVector = append(b.nullKeyVector, false)
		}
	}
}

func (b *rowTableBuilder) ResetBuffer(rows int) {
	if cap(b.serializedKeyVectorBuffer) >= rows {
		b.serializedKeyVectorBuffer = b.serializedKeyVectorBuffer[:rows]
		for i := 0; i < rows; i++ {
			b.serializedKeyVectorBuffer[i] = b.serializedKeyVectorBuffer[i][:0]
		}
	} else {
		b.serializedKeyVectorBuffer = make([][]byte, rows)
	}
	if cap(b.partIdxVector) >= rows {
		b.partIdxVector = b.partIdxVector[:0]
	} else {
		b.partIdxVector = make([]int, 0, rows)
	}
	if cap(b.hashValue) >= rows {
		b.hashValue = b.hashValue[:0]
	} else {
		b.hashValue = make([]uint64, 0, rows)
	}
	if b.hasFilter {
		if cap(b.filterVector) >= rows {
			b.filterVector = b.filterVector[:0]
		} else {
			b.filterVector = make([]bool, 0, rows)
		}
	}
	if b.hasNullableKey {
		if cap(b.nullKeyVector) >= rows {
			b.nullKeyVector = b.nullKeyVector[:0]
		} else {
			b.nullKeyVector = make([]bool, 0, rows)
		}
		for i := 0; i < rows; i++ {
			b.nullKeyVector = append(b.nullKeyVector, false)
		}
	}
}

func newRowTable(meta *JoinTableMeta) *rowTable {
	return &rowTable{
		meta:     meta,
		segments: make([]*rowTableSegment, 0),
	}
}

func (builder *rowTableBuilder) appendRemainingRowLocations(rowTables []*rowTable) {
	for partId := 0; partId < len(rowTables); partId++ {
		startPosInRawData := builder.startPosInRawData[partId]
		if len(startPosInRawData) > 0 {
			seg := rowTables[partId].segments[len(rowTables[partId].segments)-1]
			for _, pos := range startPosInRawData {
				seg.rowLocations = append(seg.rowLocations, unsafe.Pointer(&seg.rawData[pos]))
			}
			builder.startPosInRawData[partId] = builder.startPosInRawData[partId][:]
		}
	}
}

func (builder *rowTableBuilder) appendToRowTable(typeCtx types.Context, chk *chunk.Chunk, rowTables []*rowTable, rowTableMeta *JoinTableMeta) {
	fakeAddrByte := make([]byte, 8)
	for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
		var (
			row     = chk.GetRow(rowIdx)
			partIdx = builder.partIdxVector[rowIdx]
			seg     *rowTableSegment
		)
		if rowTables[partIdx] == nil {
			rowTables[partIdx] = newRowTable(rowTableMeta)
			seg = newRowTableSegment()
			rowTables[partIdx].segments = append(rowTables[partIdx].segments, seg)
			builder.startPosInRawData[partIdx] = builder.startPosInRawData[partIdx][:0]
		} else {
			seg = rowTables[partIdx].segments[len(rowTables[partIdx].segments)-1]
			if builder.crrntSizeOfRowTable[partIdx] >= MAX_ROW_TABLE_SEGMENT_SIZE {
				for _, pos := range builder.startPosInRawData[partIdx] {
					seg.rowLocations = append(seg.rowLocations, unsafe.Pointer(&seg.rawData[pos]))
				}
				builder.crrntSizeOfRowTable[partIdx] = 0
				builder.startPosInRawData[partIdx] = builder.startPosInRawData[partIdx][:0]
				seg = newRowTableSegment()
				rowTables[partIdx].segments = append(rowTables[partIdx].segments, seg)
			}
		}
		seg.hashValues = append(seg.hashValues, builder.hashValue[rowIdx])
		if (!builder.hasFilter || builder.filterVector[rowIdx]) && (!builder.hasNullableKey || !builder.nullKeyVector[rowIdx]) {
			seg.validJoinKeyPos = append(seg.validJoinKeyPos, rowIdx)
		}
		builder.startPosInRawData[partIdx] = append(builder.startPosInRawData[partIdx], uint64(len(seg.rawData)))
		// next_row_ptr
		seg.rawData = append(seg.rawData, fakeAddrByte...)
		if len := rowTableMeta.nullMapLength; len > 0 {
			seg.rawData = append(seg.rawData, make([]byte, len)...)
		}
		length := uint64(0)
		// if join_key is not fixed length: `key_length` need to be written in rawData
		if !rowTableMeta.isJoinKeysFixedLength {
			length = uint64(len(builder.serializedKeyVectorBuffer[rowIdx]))
			seg.rawData = append(seg.rawData, unsafe.Slice((*byte)(unsafe.Pointer(&length)), SizeOfLengthField)...)
		}
		if !rowTableMeta.isJoinKeysInlined {
			// if join_key is not inlined: `serialized_key` need to be written in rawData
			seg.rawData = append(seg.rawData, builder.serializedKeyVectorBuffer[rowIdx]...)
		}

		for index, colIdx := range builder.rowColumnsOrder {
			if builder.columnsSize[index] > 0 {
				// fixed size
				seg.rawData = append(seg.rawData, row.GetRaw(colIdx)...)
			} else {
				// length, raw_data
				raw := row.GetRaw(colIdx)
				length = uint64(len(raw))
				seg.rawData = append(seg.rawData, unsafe.Slice((*byte)(unsafe.Pointer(&length)), SizeOfLengthField)...)
				seg.rawData = append(seg.rawData, raw...)
			}
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
