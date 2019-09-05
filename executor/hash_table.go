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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
)

const (
	// estCountMaxFactor defines the factor of estCountMax with maxChunkSize.
	// estCountMax is maxChunkSize * estCountMaxFactor, the maximum threshold of estCount.
	// if estCount is larger than estCountMax, set estCount to estCountMax.
	// Set this threshold to prevent innerEstCount being too large and causing a performance and memory regression.
	estCountMaxFactor = 10 * 1024

	// estCountMinFactor defines the factor of estCountMin with maxChunkSize.
	// estCountMin is maxChunkSize * estCountMinFactor, the minimum threshold of estCount.
	// If estCount is smaller than estCountMin, set estCount to 0.
	// Set this threshold to prevent innerEstCount being too small and causing a performance regression.
	estCountMinFactor = 8

	// estCountDivisor defines the divisor of innerEstCount.
	// Set this divisor to prevent innerEstCount being too large and causing a performance regression.
	estCountDivisor = 8
)

// hashContext keeps the needed hash context of a db table in hash join.
type hashContext struct {
	allTypes  []*types.FieldType
	keyColIdx []int
	h         hash.Hash64
	buf       []byte
}

// hashRowContainer handles the rows and the hash map of a table.
// TODO: support spilling out to disk when memory is limited.
type hashRowContainer struct {
	records   *chunk.List
	hashTable *rowHashMap

	sc   *stmtctx.StatementContext
	hCtx *hashContext
}

func newHashRowContainer(sctx sessionctx.Context, estCount int, hCtx *hashContext, initList *chunk.List) *hashRowContainer {
	maxChunkSize := sctx.GetSessionVars().MaxChunkSize
	// The estCount from cost model is not quite accurate and we need
	// to avoid that it's too large to consume redundant memory.
	// So I invent a rough protection, firstly divide it by estCountDivisor
	// then set a maximum threshold and a minimum threshold.
	estCount /= estCountDivisor
	if estCount > maxChunkSize*estCountMaxFactor {
		estCount = maxChunkSize * estCountMaxFactor
	}
	if estCount < maxChunkSize*estCountMinFactor {
		estCount = 0
	}
	c := &hashRowContainer{
		records:   initList,
		hashTable: newRowHashMap(estCount),

		sc:   sctx.GetSessionVars().StmtCtx,
		hCtx: hCtx,
	}
	return c
}

func (c *hashRowContainer) GetMemTracker() *memory.Tracker {
	return c.records.GetMemTracker()
}

// GetMatchedRows get matched rows from probeRow. It can be called
// in multiple goroutines while each goroutine should keep its own
// h and buf.
func (c *hashRowContainer) GetMatchedRows(probeRow chunk.Row, hCtx *hashContext) (matched []chunk.Row, err error) {
	hasNull, key, err := c.getJoinKeyFromChkRow(c.sc, probeRow, hCtx)
	if err != nil || hasNull {
		return
	}
	innerPtrs := c.hashTable.Get(key)
	if len(innerPtrs) == 0 {
		return
	}
	matched = make([]chunk.Row, 0, len(innerPtrs))
	for _, ptr := range innerPtrs {
		matchedRow := c.records.GetRow(ptr)
		var ok bool
		ok, err = c.matchJoinKey(matchedRow, probeRow, hCtx)
		if err != nil {
			return
		}
		if !ok {
			continue
		}
		matched = append(matched, matchedRow)
	}
	/* TODO(fengliyuan): add test case in this case
	if len(matched) == 0 {
		// noop
	}
	*/
	return
}

// matchJoinKey checks if join keys of buildRow and probeRow are logically equal.
func (c *hashRowContainer) matchJoinKey(buildRow, probeRow chunk.Row, probeHCtx *hashContext) (ok bool, err error) {
	return codec.EqualChunkRow(c.sc,
		buildRow, c.hCtx.allTypes, c.hCtx.keyColIdx,
		probeRow, probeHCtx.allTypes, probeHCtx.keyColIdx)
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
		hasNull, key, err = c.getJoinKeyFromChkRow(c.sc, chk.GetRow(j), c.hCtx)
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
func (*hashRowContainer) getJoinKeyFromChkRow(sc *stmtctx.StatementContext, row chunk.Row, hCtx *hashContext) (hasNull bool, key uint64, err error) {
	for _, i := range hCtx.keyColIdx {
		if row.IsNull(i) {
			return true, 0, nil
		}
	}
	hCtx.h.Reset()
	err = codec.HashChunkRow(sc, hCtx.h, row, hCtx.allTypes, hCtx.keyColIdx, hCtx.buf)
	return false, hCtx.h.Sum64(), err
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

// newRowHashMap creates a new rowHashMap. estCount means the estimated size of the hashMap.
// If unknown, set it to 0.
func newRowHashMap(estCount int) *rowHashMap {
	m := new(rowHashMap)
	m.hashTable = make(map[uint64]entryAddr, estCount)
	m.entryStore.init()
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
