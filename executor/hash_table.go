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
	"fmt"
	"hash"
	"hash/fnv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
)

const (
	// estCountMaxFactor defines the factor of estCountMax with maxChunkSize.
	// estCountMax is maxChunkSize * estCountMaxFactor, the maximum threshold of estCount.
	// if estCount is larger than estCountMax, set estCount to estCountMax.
	// Set this threshold to prevent buildSideEstCount being too large and causing a performance and memory regression.
	estCountMaxFactor = 10 * 1024

	// estCountMinFactor defines the factor of estCountMin with maxChunkSize.
	// estCountMin is maxChunkSize * estCountMinFactor, the minimum threshold of estCount.
	// If estCount is smaller than estCountMin, set estCount to 0.
	// Set this threshold to prevent buildSideEstCount being too small and causing a performance regression.
	estCountMinFactor = 8

	// estCountDivisor defines the divisor of buildSideEstCount.
	// Set this divisor to prevent buildSideEstCount being too large and causing a performance regression.
	estCountDivisor = 8
)

// hashContext keeps the needed hash context of a db table in hash join.
type hashContext struct {
	allTypes  []*types.FieldType
	keyColIdx []int
	buf       []byte
	hashVals  []hash.Hash64
	hasNull   []bool
}

func (hc *hashContext) initHash(rows int) {
	if hc.buf == nil {
		hc.buf = make([]byte, 1)
	}

	if len(hc.hashVals) < rows {
		hc.hasNull = make([]bool, rows)
		hc.hashVals = make([]hash.Hash64, rows)
		for i := 0; i < rows; i++ {
			hc.hashVals[i] = fnv.New64()
		}
	} else {
		for i := 0; i < rows; i++ {
			hc.hasNull[i] = false
			hc.hashVals[i].Reset()
		}
	}
}

type hashStatistic struct {
	probeCollision   int
	buildTableElapse time.Duration
}

func (s *hashStatistic) String() string {
	return fmt.Sprintf("probe collision:%v, build:%v", s.probeCollision, s.buildTableElapse)
}

// hashRowContainer handles the rows and the hash map of a table.
type hashRowContainer struct {
	sc   *stmtctx.StatementContext
	hCtx *hashContext
	stat hashStatistic

	// hashTable stores the map of hashKey and RowPtr
	hashTable *rowHashMap

	rowContainer *chunk.RowContainer
}

func newHashRowContainer(sCtx sessionctx.Context, estCount int, hCtx *hashContext) *hashRowContainer {
	maxChunkSize := sCtx.GetSessionVars().MaxChunkSize
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
	rc := chunk.NewRowContainer(hCtx.allTypes, maxChunkSize)
	c := &hashRowContainer{
		sc:           sCtx.GetSessionVars().StmtCtx,
		hCtx:         hCtx,
		hashTable:    newRowHashMap(estCount),
		rowContainer: rc,
	}

	return c
}

// GetMatchedRowsAndPtrs get matched rows and Ptrs from probeRow. It can be called
// in multiple goroutines while each goroutine should keep its own
// h and buf.
func (c *hashRowContainer) GetMatchedRowsAndPtrs(matched *[]chunk.Row, matchedPtrs *[]chunk.RowPtr, probeKey uint64, probeRow chunk.Row, hCtx *hashContext) (err error) {
	*matched = (*matched)[:0]
	*matchedPtrs = (*matchedPtrs)[:0]
	innerPtrs := c.hashTable.Get(probeKey)
	lenInnerPtrs := len(innerPtrs)
	if lenInnerPtrs == 0 {
		return
	}
	if matched == nil || cap(*matched) < lenInnerPtrs {
		*matched = make([]chunk.Row, 0, lenInnerPtrs)
	}
	if matchedPtrs == nil || cap(*matchedPtrs) < lenInnerPtrs {
		*matchedPtrs = make([]chunk.RowPtr, 0, lenInnerPtrs)
	}
	var matchedRow chunk.Row
	for _, ptr := range innerPtrs {
		matchedRow, err = c.rowContainer.GetRow(ptr)
		if err != nil {
			return
		}
		var ok bool
		ok, err = c.matchJoinKey(matchedRow, probeRow, hCtx)
		if err != nil {
			return
		}
		if !ok {
			c.stat.probeCollision++
			continue
		}
		*matched = append(*matched, matchedRow)
		*matchedPtrs = append(*matchedPtrs, ptr)
	}
	return
}

// matchJoinKey checks if join keys of buildRow and probeRow are logically equal.
func (c *hashRowContainer) matchJoinKey(buildRow, probeRow chunk.Row, probeHCtx *hashContext) (ok bool, err error) {
	return codec.EqualChunkRow(c.sc,
		buildRow, c.hCtx.allTypes, c.hCtx.keyColIdx,
		probeRow, probeHCtx.allTypes, probeHCtx.keyColIdx)
}

// alreadySpilled indicates that records have spilled out into disk.
func (c *hashRowContainer) alreadySpilled() bool { return c.rowContainer.AlreadySpilled() }

// alreadySpilledSafe indicates that records have spilled out into disk. It's thread-safe.
func (c *hashRowContainer) alreadySpilledSafe() bool { return c.rowContainer.AlreadySpilledSafe() }

// PutChunk puts a chunk into hashRowContainer and build hash map. It's not thread-safe.
// key of hash table: hash value of key columns
// value of hash table: RowPtr of the corresponded row
func (c *hashRowContainer) PutChunk(chk *chunk.Chunk) error {
	return c.PutChunkSelected(chk, nil)
}

// PutChunkSelected selectively puts a chunk into hashRowContainer and build hash map. It's not thread-safe.
// key of hash table: hash value of key columns
// value of hash table: RowPtr of the corresponded row
func (c *hashRowContainer) PutChunkSelected(chk *chunk.Chunk, selected []bool) error {
	start := time.Now()
	defer func() { c.stat.buildTableElapse += time.Since(start) }()

	chkIdx := uint32(c.rowContainer.NumChunks())
	err := c.rowContainer.Add(chk)
	if err != nil {
		return err
	}
	numRows := chk.NumRows()
	c.hCtx.initHash(numRows)

	hCtx := c.hCtx
	for _, colIdx := range c.hCtx.keyColIdx {
		err := codec.HashChunkSelected(c.sc, hCtx.hashVals, chk, hCtx.allTypes[colIdx], colIdx, hCtx.buf, hCtx.hasNull, selected)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i := 0; i < numRows; i++ {
		if (selected != nil && !selected[i]) || c.hCtx.hasNull[i] {
			continue
		}
		key := c.hCtx.hashVals[i].Sum64()
		rowPtr := chunk.RowPtr{ChkIdx: chkIdx, RowIdx: uint32(i)}
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
	hCtx.initHash(1)
	err = codec.HashChunkRow(sc, hCtx.hashVals[0], row, hCtx.allTypes, hCtx.keyColIdx, hCtx.buf)
	return false, hCtx.hashVals[0].Sum64(), err
}

// NumChunks returns the number of chunks in the rowContainer
func (c *hashRowContainer) NumChunks() int {
	return c.rowContainer.NumChunks()
}

// NumRowsOfChunk returns the number of rows of a chunk
func (c *hashRowContainer) NumRowsOfChunk(chkID int) int {
	return c.rowContainer.NumRowsOfChunk(chkID)
}

// GetChunk returns chkIdx th chunk of in memory records, only works if rowContainer is not spilled
func (c *hashRowContainer) GetChunk(chkIdx int) *chunk.Chunk {
	return c.rowContainer.GetChunk(chkIdx)
}

// GetRow returns the row the ptr pointed to in the rowContainer
func (c *hashRowContainer) GetRow(ptr chunk.RowPtr) (chunk.Row, error) {
	return c.rowContainer.GetRow(ptr)
}

// Len returns number of records in the hash table.
func (c *hashRowContainer) Len() int {
	return c.hashTable.Len()
}

func (c *hashRowContainer) Close() error {
	return c.rowContainer.Close()
}

// GetMemTracker returns the underlying memory usage tracker in hashRowContainer.
func (c *hashRowContainer) GetMemTracker() *memory.Tracker { return c.rowContainer.GetMemTracker() }

// GetDiskTracker returns the underlying disk usage tracker in hashRowContainer.
func (c *hashRowContainer) GetDiskTracker() *disk.Tracker { return c.rowContainer.GetDiskTracker() }

// ActionSpill returns a memory.ActionOnExceed for spilling over to disk.
func (c *hashRowContainer) ActionSpill() memory.ActionOnExceed {
	return c.rowContainer.ActionSpill()
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
