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
	"hash"
	"hash/fnv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
)

// hashRowContainer handles the rows and the hash map of a table.
type hashRowContainer struct {
	records   *chunk.List
	hashTable *rowHashMap

	sc        *stmtctx.StatementContext
	allTypes  []*types.FieldType
	keyColIdx []int
	h         hash.Hash64
	buf       [1]byte
}

func newHashRowContainer(
	sc *stmtctx.StatementContext, statCount int,
	allTypes []*types.FieldType, keyColIdx []int, initCap, maxChunkSize int) *hashRowContainer {

	c := &hashRowContainer{
		hashTable: newRowHashMapWithStatCount(statCount),
		sc:        sc,
		allTypes:  allTypes,
		keyColIdx: keyColIdx,
		h:         fnv.New64(),
	}
	c.records = chunk.NewList(allTypes, initCap, maxChunkSize)
	return c
}

func (c *hashRowContainer) GetMemTracker() *memory.Tracker {
	return c.records.GetMemTracker()
}

// GetMatchedRows get matched rows from probeRow. It can be called
// in multiple goroutines while each goroutine should keep its own
// h and buf.
func (c *hashRowContainer) GetMatchedRows(probeRow chunk.Row, joinKeysTypes []*types.FieldType, keyColIdx []int, h hash.Hash64, buf []byte) (matched []chunk.Row, hasNull bool, err error) {

	var key uint64
	hasNull, key, err = c.getJoinKeyFromChkRow(c.sc, probeRow, joinKeysTypes, keyColIdx, h, buf)
	if err != nil {
		return
	}
	if hasNull {
		return
	}
	innerPtrs := c.hashTable.Get(key)
	if len(innerPtrs) == 0 {
		hasNull = true
		return
	}
	matched = make([]chunk.Row, 0, len(innerPtrs))
	for _, ptr := range innerPtrs {
		matchedRow := c.records.GetRow(ptr)
		var ok bool
		ok, err = c.matchJoinKey(matchedRow, probeRow, joinKeysTypes, keyColIdx)
		if err != nil {
			return
		}
		if !ok {
			continue
		}
		matched = append(matched, matchedRow)
	}
	if len(matched) == 0 { // TODO(fengliyuan): add test case
		hasNull = true
	}
	return
}

// matchJoinKey checks if join keys of buildRow and probeRow are logically equal.
func (c *hashRowContainer) matchJoinKey(buildRow, probeRow chunk.Row, probeAllTypes []*types.FieldType, probeColIdx []int) (ok bool, err error) {
	return codec.EqualChunkRow(c.sc,
		buildRow, c.allTypes, c.keyColIdx,
		probeRow, probeAllTypes, probeColIdx)
}

// PutChunk puts a chunk into hashRowContainer and build hash map. It's not thread-safe.
// key of hash table: hash value of key columns
// value of hash table: RowPtr of the corresponded row
func (c *hashRowContainer) PutChunk(chk *chunk.Chunk) error {
	chkIdx := uint32(c.records.NumChunks())
	c.records.Add(chk)
	var (
		hasNull bool
		err     error
		key     uint64
	)
	numRows := chk.NumRows()
	for j := 0; j < numRows; j++ {
		hasNull, key, err = c.getJoinKeyFromChkRow(c.sc, chk.GetRow(j), c.allTypes, c.keyColIdx, c.h, c.buf[:])
		if err != nil {
			return errors.Trace(err)
		}
		if hasNull {
			continue
		}
		rowPtr := chunk.RowPtr{ChkIdx: chkIdx, RowIdx: uint32(j)}
		c.hashTable.Put(key, rowPtr)
	}
	return nil
}

// getJoinKeyFromChkRow fetches join keys from row and calculate the hash value.
func (*hashRowContainer) getJoinKeyFromChkRow(
	sc *stmtctx.StatementContext,
	row chunk.Row, allTypes []*types.FieldType, keyColIdx []int, h hash.Hash64, buf []byte) (hasNull bool, key uint64, err error) {
	for _, i := range keyColIdx {
		if row.IsNull(i) {
			return true, 0, nil
		}
	}
	h.Reset()
	err = codec.HashChunkRow(sc, h, row, allTypes, keyColIdx, buf)
	return false, h.Sum64(), err
}

func (c hashRowContainer) Len() int {
	return c.hashTable.Len()
}

const (
	initialEntrySliceLen = 64
	maxEntrySliceLen     = 8 * 1024
)

type entry struct {
	ptr  chunk.RowPtr
	next entryAddr
}

type entryStore struct {
	slices [][]entry
}

func (es *entryStore) init() {
	es.slices = [][]entry{make([]entry, 0, initialEntrySliceLen)}
	// Reserve the first empty entry, so entryAddr{} can represent nullEntryAddr.
	reserved := es.put(entry{})
	if reserved != nullEntryAddr {
		panic("entryStore: first entry is not nullEntryAddr")
	}
}

func (es *entryStore) put(e entry) entryAddr {
	sliceIdx := uint32(len(es.slices) - 1)
	slice := es.slices[sliceIdx]
	if len(slice) == cap(slice) {
		// TODO: add test here.
		size := cap(slice) * 2
		if size >= maxEntrySliceLen {
			size = maxEntrySliceLen
		}
		slice = make([]entry, 0, size)
		es.slices = append(es.slices, slice)
		sliceIdx++
	}
	addr := entryAddr{sliceIdx: sliceIdx, offset: uint32(len(slice))}
	es.slices[sliceIdx] = append(slice, e)
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
// TODO(fengliyuan): add unit test for this.
type rowHashMap struct {
	entryStore entryStore
	hashTable  map[uint64]entryAddr
	length     int
}

// newRowHashMap creates a new rowHashMap.
func newRowHashMapWithStatCount(statCount int) *rowHashMap {
	m := new(rowHashMap)
	m.hashTable = make(map[uint64]entryAddr, statCount)
	m.entryStore.init()
	return m
}

func newRowHashMap() *rowHashMap {
	return newRowHashMapWithStatCount(0)
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
