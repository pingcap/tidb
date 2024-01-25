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
	"sync/atomic"
	"unsafe"

	"github.com/cznic/mathutil"
)

type subTable struct {
	rowData   *rowTable
	hashTable []unsafe.Pointer
	posMask   uint64
}

func nextPowerOfTwo(value uint64) uint64 {
	ret := uint64(2)
	for ; ret < value; ret = ret << 1 {
	}
	return ret
}

func newSubTable(table *rowTable) *subTable {
	ret := &subTable{
		rowData: table,
	}
	capacity := mathutil.MaxUint64(nextPowerOfTwo(uint64(table.validKeyCount())), uint64(1024))
	ret.hashTable = make([]unsafe.Pointer, capacity)
	ret.posMask = capacity - 1
	return ret
}

func (st *subTable) build(threadSafe bool, startSegmentIndex int, segmentStep int) {
	updateHashValue := func(pos uint64, rowAddress unsafe.Pointer) {
		prev := st.hashTable[pos]
		st.hashTable[pos] = rowAddress
		setNextRowOffset(rowAddress, prev)
	}
	if !threadSafe {
		updateHashValue = func(pos uint64, rowAddress unsafe.Pointer) {
			for true {
				prev := atomic.LoadPointer(&st.hashTable[pos])
				if atomic.CompareAndSwapPointer(&st.hashTable[pos], prev, rowAddress) {
					setNextRowOffset(rowAddress, prev)
					break
				}
			}
		}
	}
	for i := startSegmentIndex; i < len(st.rowData.segments); i += segmentStep {
		for index := range st.rowData.segments[i].validJoinKeyPos {
			rowAddress := st.rowData.segments[i].rowLocations[index]
			hashValue := getHashValue(rowAddress)
			pos := hashValue & st.posMask
			updateHashValue(pos, rowAddress)
		}
	}
}

type joinHashTable struct {
	isThreadSafe bool
	tables       []*subTable
}

func newJoinHashTable(isThreadSafe bool, partitionedRowTables []*rowTable) *joinHashTable {
	jht := &joinHashTable{
		isThreadSafe: isThreadSafe,
		tables:       make([]*subTable, len(partitionedRowTables)),
	}
	for i, rowTable := range partitionedRowTables {
		jht.tables[i] = newSubTable(rowTable)
	}
	return jht
}

func (jht *joinHashTable) buildHashTable(partitionIndex int, startSegmentIndex int, segmentStep int) {
	jht.tables[partitionIndex].build(jht.isThreadSafe, startSegmentIndex, segmentStep)
}
