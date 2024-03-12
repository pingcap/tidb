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
	"fmt"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cznic/mathutil"
)

type subTable struct {
	rowData          *rowTable
	hashTable        []unsafe.Pointer
	posMask          uint64
	isRowTableEmpty  bool
	isHashTableEmpty bool
}

type hashStatistic struct {
	// NOTE: probeCollision may be accessed from multiple goroutines concurrently.
	probeCollision   int64
	buildTableElapse time.Duration
}

func (s *hashStatistic) String() string {
	return fmt.Sprintf("probe_collision:%v, build:%v", s.probeCollision, execdetails.FormatDuration(s.buildTableElapse))
}

func (st *subTable) lookup(hashValue uint64) unsafe.Pointer {
	return st.hashTable[hashValue&st.posMask]
}

func nextPowerOfTwo(value uint64) uint64 {
	ret := uint64(2)
	for ; ret < value; ret = ret << 1 {
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
	capacity := mathutil.MaxUint64(nextPowerOfTwo(uint64(table.validKeyCount())), uint64(1024))
	ret.hashTable = make([]unsafe.Pointer, capacity)
	ret.posMask = capacity - 1
	return ret
}

func (st *subTable) updateHashValue(pos uint64, rowAddress unsafe.Pointer) {
	prev := st.hashTable[pos]
	st.hashTable[pos] = rowAddress
	setNextRowAddress(rowAddress, prev)
}

func (st *subTable) atomicUpdateHashValue(pos uint64, rowAddress unsafe.Pointer) {
	for {
		prev := atomic.LoadPointer(&st.hashTable[pos])
		if atomic.CompareAndSwapPointer(&st.hashTable[pos], prev, rowAddress) {
			setNextRowAddress(rowAddress, prev)
			break
		}
	}
}

func (st *subTable) build(startSegmentIndex int, segmentStep int) {
	if startSegmentIndex == 0 && segmentStep == 1 {
		for i := startSegmentIndex; i < len(st.rowData.segments); i += segmentStep {
			for index := range st.rowData.segments[i].validJoinKeyPos {
				rowAddress := st.rowData.segments[i].rowLocations[index]
				hashValue := st.rowData.segments[i].hashValues[index]
				pos := hashValue & st.posMask
				st.updateHashValue(pos, rowAddress)
			}
		}
	} else {
		for i := startSegmentIndex; i < len(st.rowData.segments); i += segmentStep {
			for index := range st.rowData.segments[i].validJoinKeyPos {
				rowAddress := st.rowData.segments[i].rowLocations[index]
				hashValue := st.rowData.segments[i].hashValues[index]
				pos := hashValue & st.posMask
				st.atomicUpdateHashValue(pos, rowAddress)
			}
		}
	}
}

type JoinHashTable struct {
	isThreadSafe    bool
	tables          []*subTable
	partitionNumber uint64
}

type rowPos struct {
	subTableIndex   int
	rowSegmentIndex int
	rowIndex        uint64
}

type rowIter struct {
	table      *JoinHashTable
	currentPos *rowPos
	endPos     *rowPos
}

func (ri *rowIter) getValue() unsafe.Pointer {
	return ri.table.tables[ri.currentPos.subTableIndex].rowData.segments[ri.currentPos.rowSegmentIndex].rowLocations[ri.currentPos.rowIndex]
}

func (ri *rowIter) next() {
	ri.currentPos.rowIndex++
	if ri.currentPos.rowIndex == uint64(ri.table.tables[ri.currentPos.subTableIndex].rowData.segments[ri.currentPos.rowSegmentIndex].rowCount()) {
		ri.currentPos.rowSegmentIndex++
		ri.currentPos.rowIndex = 0
		if ri.currentPos.rowSegmentIndex == len(ri.table.tables[ri.currentPos.subTableIndex].rowData.segments) {
			ri.currentPos.subTableIndex++
			ri.currentPos.rowSegmentIndex = 0
		}
	}
}

func (ri *rowIter) hasNext() bool {
	return ri.currentPos.subTableIndex < ri.endPos.subTableIndex || ri.currentPos.rowSegmentIndex < ri.endPos.rowSegmentIndex || ri.currentPos.rowIndex < ri.endPos.rowIndex
}

func newJoinHashTable(partitionedRowTables []*rowTable) *JoinHashTable {
	jht := &JoinHashTable{
		tables:          make([]*subTable, len(partitionedRowTables)),
		partitionNumber: uint64(len(partitionedRowTables)),
	}
	for i, rowTable := range partitionedRowTables {
		jht.tables[i] = newSubTable(rowTable)
	}
	return jht
}

func (jht *JoinHashTable) createRowPos(pos uint64) *rowPos {
	if pos < 0 || pos > jht.totalRowCount() {
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
func (jht *JoinHashTable) createRowIter(start, end uint64) *rowIter {
	if start > end {
		start = end
	}
	return &rowIter{
		table:      jht,
		currentPos: jht.createRowPos(start),
		endPos:     jht.createRowPos(end),
	}
}

func (jht *JoinHashTable) isHashTableEmpty() bool {
	for _, subTable := range jht.tables {
		if !subTable.isHashTableEmpty {
			return false
		}
	}
	return true
}

func (jht *JoinHashTable) totalRowCount() uint64 {
	ret := uint64(0)
	for _, table := range jht.tables {
		ret += table.rowData.rowCount()
	}
	return ret
}

func (jht *JoinHashTable) buildHashTable(partitionIndex int, startSegmentIndex int, segmentStep int) {
	jht.tables[partitionIndex].build(startSegmentIndex, segmentStep)
}

func (jht *JoinHashTable) lookup(hashValue uint64) unsafe.Pointer {
	partitionIndex := hashValue % jht.partitionNumber
	return jht.tables[partitionIndex].lookup(hashValue)
}
