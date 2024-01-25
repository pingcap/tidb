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
)

const SizeOfNextPtr = int(unsafe.Sizeof(*new(unsafe.Pointer)))
const SizeOfHashValue = int(unsafe.Sizeof(uint64(0)))

type rowTableSegment struct {
	/*
	   The row storage used in hash join, the layout is
	   |---------------------|--------------------|-----------------|----------------------|-------------------------------|
	              |                     |                  |                   |                           |
	              V                     V                  V                   V                           V
	        next_row_ptr            hash_value         null_map           serialized_key                row_data
	   next_row_ptr: the ptr to link to the next row, used in hash table build, it will make all the rows of the same hash value a linked list
	   hash_value: the hash value of the join keys for current row, used in hash table build
	   null_map(optional): null_map actually includes two parts: the null_flag for each column in current row, the used_flag which is used in
	                       right semi/outer join. This field is optional, if all the column from build side is not null and used_flag is not used
	                       this field can be removed.
	   serialized_key(optional): the serialized representation of the join keys, used to quick join key compare during probe stage. This field is
	                             optional, for join keys that can be inlined in the row_data(for example, join key with one integer), this field
	                             can be removed.
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
	rowLocations    []unsafe.Pointer // the start address of each row
	validJoinKeyPos []int            // the pos of rows that have valid join keys, used in hash table build
}

func (rts *rowTableSegment) rowCount() int64 {
	return int64(len(rts.rowLocations))
}

func (rts *rowTableSegment) validKeyCount() int64 {
	return int64(len(rts.validJoinKeyPos))
}

func setHashValue(rowStart unsafe.Pointer, hashValue uint64) {
	*(*uint64)(unsafe.Add(rowStart, SizeOfNextPtr)) = hashValue
}

func getHashValue(rowStart unsafe.Pointer) uint64 {
	return *(*uint64)(unsafe.Add(rowStart, SizeOfNextPtr))
}

func setNextRowOffset(rowStart unsafe.Pointer, nextRowOffset unsafe.Pointer) {
	*(*unsafe.Pointer)(rowStart) = nextRowOffset
}

func getNextRowOffset(rowStart unsafe.Pointer) unsafe.Pointer {
	return *(*unsafe.Pointer)(rowStart)
}

type tableMeta struct {
	isFixedLength       bool            // is the row layout has fixed length
	rowLength           int32           // the row length if the row is fixed length
	isJoinKeysFixedSize bool            // if the join keys has fixed size
	joinKeysLength      int32           // the join keys length if it is fixed length
	isJoinKeysInlined   bool            // is the join key inlined in the row data
	nullMapLength       uint16          // the length of null map, the null map include null bit for each column in the row and the used flag for right semi/outer join
	columnIndexMap      map[int32]int32 // the map of the column index in row format to the column index of the original column in build block
	columnSize          []int32         // the column size of each column, -1 mean variable column
}

type rowTable struct {
	meta     *tableMeta
	segments []*rowTableSegment
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
