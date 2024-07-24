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
	rowData          *rowTable
	hashTable        []uintptr
	posMask          uint64
	isRowTableEmpty  bool
	isHashTableEmpty bool
}

func (st *subTable) lookup(hashValue uint64) uintptr {
	return st.hashTable[hashValue&st.posMask]
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
	ret.hashTable = make([]uintptr, hashTableLength)
	ret.posMask = hashTableLength - 1
	return ret
}

func (st *subTable) updateHashValue(pos uint64, rowAddress unsafe.Pointer) {
	prev := *(*unsafe.Pointer)(unsafe.Pointer(&st.hashTable[pos]))
	*(*unsafe.Pointer)(unsafe.Pointer(&st.hashTable[pos])) = rowAddress
	setNextRowAddress(rowAddress, prev)
}

func (st *subTable) atomicUpdateHashValue(pos uint64, rowAddress unsafe.Pointer) {
	for {
		prev := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&st.hashTable[pos])))
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&st.hashTable[pos])), prev, rowAddress) {
			setNextRowAddress(rowAddress, prev)
			break
		}
	}
}

func (st *subTable) build(startSegmentIndex int, endSegmentIndex int) {
	if startSegmentIndex == 0 && endSegmentIndex == len(st.rowData.segments) {
		for i := startSegmentIndex; i < endSegmentIndex; i++ {
			for _, index := range st.rowData.segments[i].validJoinKeyPos {
				rowAddress := st.rowData.segments[i].getRowPointer(index)
				hashValue := st.rowData.segments[i].hashValues[index]
				pos := hashValue & st.posMask
				st.updateHashValue(pos, rowAddress)
			}
		}
	} else {
		for i := startSegmentIndex; i < endSegmentIndex; i++ {
			for _, index := range st.rowData.segments[i].validJoinKeyPos {
				rowAddress := st.rowData.segments[i].getRowPointer(index)
				hashValue := st.rowData.segments[i].hashValues[index]
				pos := hashValue & st.posMask
				st.atomicUpdateHashValue(pos, rowAddress)
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

func (jht *hashTableV2) buildHashTableForTest(partitionIndex int, startSegmentIndex int, segmentStep int) {
	jht.tables[partitionIndex].build(startSegmentIndex, segmentStep)
}
