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

func (st *subTable) build(threadSafe bool, startSegmentIndex int, segmentStep int) {
	if threadSafe {
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

func newJoinHashTable(isThreadSafe bool, partitionedRowTables []*rowTable) *JoinHashTable {
	jht := &JoinHashTable{
		isThreadSafe:    isThreadSafe,
		tables:          make([]*subTable, len(partitionedRowTables)),
		partitionNumber: uint64(len(partitionedRowTables)),
	}
	for i, rowTable := range partitionedRowTables {
		jht.tables[i] = newSubTable(rowTable)
	}
	return jht
}

func (jht *JoinHashTable) isHashTableEmpty() bool {
	for _, subTable := range jht.tables {
		if !subTable.isHashTableEmpty {
			return false
		}
	}
	return true
}

func (jht *JoinHashTable) buildHashTable(partitionIndex int, startSegmentIndex int, segmentStep int) {
	jht.tables[partitionIndex].build(jht.isThreadSafe, startSegmentIndex, segmentStep)
}

func (jht *JoinHashTable) lookup(hashValue uint64) unsafe.Pointer {
	partitionIndex := hashValue % jht.partitionNumber
	return jht.tables[partitionIndex].lookup(hashValue)
}
