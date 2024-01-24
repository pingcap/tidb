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

import "unsafe"

type subTable struct {
	rowData   *rowTable
	hashTable []unsafe.Pointer
}

func newSubTable(table *rowTable) *subTable {
	return &subTable{
		rowData:   table,
		hashTable: make([]unsafe.Pointer, table.rowCount()),
	}
}

func (st *subTable) build(threadSafe bool, startSegmentIndex int, segmentStep int) {
	for i := startSegmentIndex; i < len(st.rowData.segments); i += segmentStep {
		for index := range st.rowData.segments[i].validJoinKeyPos {
			hashValue = st.rowData.get
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
}
