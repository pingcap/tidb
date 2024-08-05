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
	"sync/atomic"
	"unsafe"
)

type subTable struct {
	rowData *rowTable
	// the taggedPtr is used to save the row address, during hash join build stage
	// it will convert the chunk data into row format, each row there is an unsafe.Pointer
	// pointing the start address of the row. The unsafe.Pointer will be converted to
	// taggedPtr and saved in hashTable.
	// Generally speaking it is unsafe or even illegal in go to save unsafe.Pointer
	// into uintptr, and later convert uintptr back to unsafe.Pointer since after save
	// the value of unsafe.Pointer into uintptr, it has no pointer semantics, and may
	// become invalid after GC. But it is ok to do this in hash join so far because
	// 1. the check of heapObjectsCanMove makes sure that if the object is in heap, the address will not be changed after GC
	// 2. row address only points to a valid address in `rowTableSegment.rawData`. `rawData` is a slice in `rowTableSegment`, and it will be used by multiple goroutines,
	//    and its size will be runtime expanded, this kind of slice will always be allocated in heap
	hashTable        []taggedPtr
	posMask          uint64
	isRowTableEmpty  bool
	isHashTableEmpty bool
}

func (st *subTable) lookup(hashValue uint64, tagHelper *tagPtrHelper) taggedPtr {
	ret := st.hashTable[hashValue&st.posMask]
	hashTagValue := tagHelper.getTaggedValue(hashValue)
	if uint64(ret)&hashTagValue != hashTagValue {
		// if tag value not match, the key will not be matched
		return 0
	}
	return ret
}

func nextPowerOfTwo(value uint64) uint64 {
	ret := uint64(2)
	round := 1
	for ; ret <= value && round <= 64; ret = ret << 1 {
		round++
	}
	if round > 64 {
		panic("input value is too large")
	}
	return ret
}

func newSubTable(table *rowTable) *subTable {
	ret := &subTable{
		rowData:          table,
		isHashTableEmpty: false,
		isRowTableEmpty:  false,
	}
	if table.rowCount() == 0 {
		ret.isRowTableEmpty = true
	}
	if table.validKeyCount() == 0 {
		ret.isHashTableEmpty = true
	}
	hashTableLength := max(nextPowerOfTwo(table.validKeyCount()), uint64(32))
	ret.hashTable = make([]taggedPtr, hashTableLength)
	ret.posMask = hashTableLength - 1
	return ret
}

func (st *subTable) updateHashValue(hashValue uint64, rowAddress unsafe.Pointer, tagHelper *tagPtrHelper) {
	pos := hashValue & st.posMask
	prev := st.hashTable[pos]
	tagValue := tagHelper.getTaggedValue(hashValue | uint64(prev))
	taggedAddress := tagHelper.toTaggedPtr(tagValue, rowAddress)
	st.hashTable[pos] = taggedAddress
	setNextRowAddress(rowAddress, prev)
}

func (st *subTable) atomicUpdateHashValue(hashValue uint64, rowAddress unsafe.Pointer, tagHelper *tagPtrHelper) {
	pos := hashValue & st.posMask
	for {
		prev := taggedPtr(atomic.LoadUintptr((*uintptr)(unsafe.Pointer(&st.hashTable[pos]))))
		tagValue := tagHelper.getTaggedValue(hashValue | uint64(prev))
		taggedAddress := tagHelper.toTaggedPtr(tagValue, rowAddress)
		if atomic.CompareAndSwapUintptr((*uintptr)(unsafe.Pointer(&st.hashTable[pos])), uintptr(prev), uintptr(taggedAddress)) {
			setNextRowAddress(rowAddress, prev)
			break
		}
	}
}

func (st *subTable) build(startSegmentIndex int, endSegmentIndex int, tagHelper *tagPtrHelper) {
	if startSegmentIndex == 0 && endSegmentIndex == len(st.rowData.segments) {
		for i := startSegmentIndex; i < endSegmentIndex; i++ {
			for _, index := range st.rowData.segments[i].validJoinKeyPos {
				rowAddress := st.rowData.segments[i].getRowPointer(index)
				hashValue := st.rowData.segments[i].hashValues[index]
				st.updateHashValue(hashValue, rowAddress, tagHelper)
			}
		}
	} else {
		for i := startSegmentIndex; i < endSegmentIndex; i++ {
			for _, index := range st.rowData.segments[i].validJoinKeyPos {
				rowAddress := st.rowData.segments[i].getRowPointer(index)
				hashValue := st.rowData.segments[i].hashValues[index]
				st.atomicUpdateHashValue(hashValue, rowAddress, tagHelper)
			}
		}
	}
}

type hashTableV2 struct {
	tables          []*subTable
	partitionNumber uint64
}

type rowPos struct {
	subTableIndex   int
	rowSegmentIndex int
	rowIndex        uint64
}

type rowIter struct {
	table      *hashTableV2
	currentPos *rowPos
	endPos     *rowPos
}

func (ri *rowIter) getValue() unsafe.Pointer {
	return ri.table.tables[ri.currentPos.subTableIndex].rowData.segments[ri.currentPos.rowSegmentIndex].getRowPointer(int(ri.currentPos.rowIndex))
}

func (ri *rowIter) next() {
	ri.currentPos.rowIndex++
	if ri.currentPos.rowIndex == uint64(ri.table.tables[ri.currentPos.subTableIndex].rowData.segments[ri.currentPos.rowSegmentIndex].rowCount()) {
		ri.currentPos.rowSegmentIndex++
		ri.currentPos.rowIndex = 0
		for ri.currentPos.rowSegmentIndex == len(ri.table.tables[ri.currentPos.subTableIndex].rowData.segments) {
			ri.currentPos.subTableIndex++
			ri.currentPos.rowSegmentIndex = 0
			if ri.currentPos.subTableIndex == int(ri.table.partitionNumber) {
				break
			}
		}
	}
}

func (ri *rowIter) isEnd() bool {
	return !(ri.currentPos.subTableIndex < ri.endPos.subTableIndex || ri.currentPos.rowSegmentIndex < ri.endPos.rowSegmentIndex || ri.currentPos.rowIndex < ri.endPos.rowIndex)
}

func newJoinHashTableForTest(partitionedRowTables []*rowTable) *hashTableV2 {
	// first make sure there is no nil rowTable
	jht := &hashTableV2{
		tables:          make([]*subTable, len(partitionedRowTables)),
		partitionNumber: uint64(len(partitionedRowTables)),
	}
	for i, rowTable := range partitionedRowTables {
		jht.tables[i] = newSubTable(rowTable)
	}
	return jht
}

func (jht *hashTableV2) createRowPos(pos uint64) *rowPos {
	if pos > jht.totalRowCount() {
		panic("invalid call to createRowPos, the input pos should be in [0, totalRowCount]")
	}
	if pos == jht.totalRowCount() {
		return &rowPos{
			subTableIndex:   len(jht.tables),
			rowSegmentIndex: 0,
			rowIndex:        0,
		}
	}
	subTableIndex := 0
	for pos >= jht.tables[subTableIndex].rowData.rowCount() {
		pos -= jht.tables[subTableIndex].rowData.rowCount()
		subTableIndex++
	}
	rowSegmentIndex := 0
	for pos >= uint64(jht.tables[subTableIndex].rowData.segments[rowSegmentIndex].rowCount()) {
		pos -= uint64(jht.tables[subTableIndex].rowData.segments[rowSegmentIndex].rowCount())
		rowSegmentIndex++
	}
	return &rowPos{
		subTableIndex:   subTableIndex,
		rowSegmentIndex: rowSegmentIndex,
		rowIndex:        pos,
	}
}
func (jht *hashTableV2) createRowIter(start, end uint64) *rowIter {
	if start > end {
		start = end
	}
	return &rowIter{
		table:      jht,
		currentPos: jht.createRowPos(start),
		endPos:     jht.createRowPos(end),
	}
}

func (jht *hashTableV2) isHashTableEmpty() bool {
	for _, subTable := range jht.tables {
		if !subTable.isHashTableEmpty {
			return false
		}
	}
	return true
}

func (jht *hashTableV2) totalRowCount() uint64 {
	ret := uint64(0)
	for _, table := range jht.tables {
		ret += table.rowData.rowCount()
	}
	return ret
}
