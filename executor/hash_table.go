// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"github.com/pingcap/tidb/util/chunk"
)

const maxEntrySliceLen = 8 * 1024

type entry struct {
	ptr  chunk.RowPtr
	next entryAddr
}

type entryStore struct {
	slices   [][]entry
	sliceIdx uint32
	sliceLen uint32
}

func (es *entryStore) put(e entry) entryAddr {
	if es.sliceLen == maxEntrySliceLen {
		es.slices = append(es.slices, make([]entry, 0, maxEntrySliceLen))
		es.sliceLen = 0
		es.sliceIdx++
	}
	addr := entryAddr{sliceIdx: es.sliceIdx, offset: es.sliceLen}
	es.slices[es.sliceIdx] = append(es.slices[es.sliceIdx], e)
	es.sliceLen++
	return addr
}

func (es *entryStore) get(addr entryAddr) entry {
	return es.slices[addr.sliceIdx][addr.offset]
}

type entryAddr struct {
	sliceIdx uint32
	offset   uint32
}

var nullEntryAddr = entryAddr{}

// rowHashMap stores multiple rowPtr of rows for a given key with minimum GC overhead.
// A given key can store multiple values.
// It is not thread-safe, should only be used in one goroutine.
type rowHashMap struct {
	entryStore entryStore
	hashTable  map[uint64]entryAddr
	length     int
}

// newRowHashMap creates a new rowHashMap.
func newRowHashMap() *rowHashMap {
	m := new(rowHashMap)
	// TODO(fengliyuan): initialize the size of map from the estimated row count for better performance.
	m.hashTable = make(map[uint64]entryAddr)
	m.entryStore.slices = [][]entry{make([]entry, 0, 64)}
	// Reserve the first empty entry, so entryAddr{} can represent nullEntryAddr.
	m.entryStore.put(entry{})
	return m
}

// Put puts the key/rowPtr pairs to the rowHashMap, multiple rowPtrs are stored in a list.
func (m *rowHashMap) Put(hashKey uint64, rowPtr chunk.RowPtr) {
	oldEntryAddr := m.hashTable[hashKey]
	e := entry{
		ptr:  rowPtr,
		next: oldEntryAddr,
	}
	newEntryAddr := m.entryStore.put(e)
	m.hashTable[hashKey] = newEntryAddr
	m.length++
}

// Get gets the values of the "key" and appends them to "values".
func (m *rowHashMap) Get(hashKey uint64) (rowPtrs []chunk.RowPtr) {
	entryAddr := m.hashTable[hashKey]
	for entryAddr != nullEntryAddr {
		e := m.entryStore.get(entryAddr)
		entryAddr = e.next
		rowPtrs = append(rowPtrs, e.ptr)
	}
	// Keep the order of input.
	for i := 0; i < len(rowPtrs)/2; i++ {
		j := len(rowPtrs) - 1 - i
		rowPtrs[i], rowPtrs[j] = rowPtrs[j], rowPtrs[i]
	}
	return
}

// Len returns the number of rowPtrs in the rowHashMap, the number of keys may be less than Len
// if the same key is put more than once.
func (m *rowHashMap) Len() int { return m.length }
