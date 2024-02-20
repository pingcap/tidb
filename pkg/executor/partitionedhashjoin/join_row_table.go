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

func (rts *rowTableSegment) rowCount() int64 {
	return int64(len(rts.rowLocations))
}

func (rts *rowTableSegment) validKeyCount() int64 {
	return int64(len(rts.validJoinKeyPos))
}

func setNextRowAddress(rowStart unsafe.Pointer, nextRowAddress unsafe.Pointer) {
	*(*unsafe.Pointer)(rowStart) = nextRowAddress
}

func getNextRowAddress(rowStart unsafe.Pointer) unsafe.Pointer {
	return *(*unsafe.Pointer)(rowStart)
}

type tableMeta struct {
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
	// the first n columns in row is used for other condition, if a join has other condition, we only need to extract
	// first n columns from the RowTable to evaluate other condition
	columnCountNeededForOtherCondition int
}

func (meta *tableMeta) getSerializedKeyLength(rowStart unsafe.Pointer) uint64 {
	return *(*uint64)(unsafe.Add(rowStart, SizeOfNextPtr+meta.nullMapLength))
}

func (meta *tableMeta) advanceToRowData(rowStart unsafe.Pointer) unsafe.Pointer {
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

func (meta *tableMeta) isColumnNull(rowStart unsafe.Pointer, columnIndex int) bool {
	byteIndex := columnIndex / 8
	bitIndex := columnIndex % 8
	return *(*uint8)(unsafe.Add(rowStart, SizeOfNextPtr+byteIndex))&(uint8(1)<<(8-bitIndex)) != uint8(0)
}

type rowTable struct {
	meta     *tableMeta
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
func newTableMeta(buildKeyIndex []int, otherConditionColIndex []int, columnsNeedConvertToRow []int, buildSchema expression.Schema, needUsedFlag bool) *tableMeta {
	meta := &tableMeta{}
	meta.isFixedLength = true
	meta.rowLength = 0
	nullableColumnCount := 0
	if needUsedFlag {
		nullableColumnCount = 1
	}
	if columnsNeedConvertToRow == nil {
		for _, col := range buildSchema.Columns {
			length := chunk.GetFixedLen(col.RetType)
			if length == chunk.VarElemLen {
				meta.isFixedLength = false
			} else {
				meta.rowLength += length
			}
			if !mysql.HasNotNullFlag(col.RetType.GetFlag()) {
				nullableColumnCount++
			}
		}
	} else {
		for _, index := range columnsNeedConvertToRow {
			length := chunk.GetFixedLen(buildSchema.Columns[index].RetType)
			if length == chunk.VarElemLen {
				meta.isFixedLength = false
			} else {
				meta.rowLength += length
			}
			if !mysql.HasNotNullFlag(buildSchema.Columns[index].RetType.GetFlag()) {
				nullableColumnCount++
			}
		}
	}
	if !meta.isFixedLength {
		meta.rowLength = 0
	}
	meta.nullMapLength = (nullableColumnCount + 7) / 8

	meta.isJoinKeysFixedLength = true
	meta.joinKeysLength = 0
	meta.isJoinKeysInlined = true
	keyIndexMap := make(map[int]struct{})
	for _, index := range buildKeyIndex {
		keyType := buildSchema.Columns[index].RetType
		keyLength := chunk.GetFixedLen(keyType)
		if keyLength == chunk.VarElemLen {
			meta.isJoinKeysFixedLength = false
		} else {
			meta.joinKeysLength += keyLength
		}
		if !canBeInlinedAsJoinKey(keyType) {
			meta.isJoinKeysInlined = false
		}
		keyIndexMap[index] = struct{}{}
	}
	if !meta.isJoinKeysFixedLength {
		meta.joinKeysLength = 0
	}
	if len(buildKeyIndex) != len(keyIndexMap) {
		// has duplicated key, can not be inlined
		meta.isJoinKeysInlined = false
	}
	// construct the column order
	meta.rowColumnsOrder = make([]int, len(columnsNeedConvertToRow))
	meta.columnsSize = make([]int, len(columnsNeedConvertToRow))
	usedColumnMap := make(map[int]struct{}, len(columnsNeedConvertToRow))

	updateColumnOrder := func(index int) {
		if _, ok := usedColumnMap[index]; !ok {
			meta.rowColumnsOrder = append(meta.rowColumnsOrder, index)
			meta.columnsSize = append(meta.columnsSize, chunk.GetFixedLen(buildSchema.Columns[index].RetType))
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
	if len(otherConditionColIndex) > 0 {
		// if join has other condition, the columns used by other condition is appended to row layout after the key
		for _, index := range otherConditionColIndex {
			updateColumnOrder(index)
		}
		meta.columnCountNeededForOtherCondition = len(usedColumnMap)
	}
	for _, index := range columnsNeedConvertToRow {
		updateColumnOrder(index)
	}
	return meta
}

func newRowTable(buildKeyIndex []int, otherConditionColIndex []int, columnsNeedConvertToRow []int, buildSchema expression.Schema, needUsedFlag bool) *rowTable {
	return &rowTable{
		meta:     newTableMeta(buildKeyIndex, otherConditionColIndex, columnsNeedConvertToRow, buildSchema, needUsedFlag),
		segments: make([]*rowTableSegment, 0),
	}
}

func (rt *rowTable) merge(other *rowTable) {
	rt.segments = append(rt.segments, other.segments...)
}

func (rt *rowTable) rowCount() int64 {
	ret := int64(0)
	for _, s := range rt.segments {
		ret += s.rowCount()
	}
	return ret
}

func (rt *rowTable) validKeyCount() int64 {
	ret := int64(0)
	for _, s := range rt.segments {
		ret += s.validKeyCount()
	}
	return ret
}
