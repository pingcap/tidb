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
	"github.com/pingcap/tidb/util/bitmap"
	"hash"
	"hash/fnv"
	"sync"
	"sync/atomic"
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
	hCtx []*hashContext
	stat hashStatistic

	// hashTable stores the map of hashKey and RowPtr
	hashTable baseHashTable

	rowContainer       *chunk.RowContainer
	outerMatchedStatus []*bitmap.ConcurrentBitmap
	lock               sync.Mutex
}

func newHashRowContainer(sCtx sessionctx.Context, estCount int, hCtx []*hashContext) *hashRowContainer {
	maxChunkSize := sCtx.GetSessionVars().MaxChunkSize
	rc := chunk.NewRowContainer(hCtx[0].allTypes, maxChunkSize)
	c := &hashRowContainer{
		sc:           sCtx.GetSessionVars().StmtCtx,
		hCtx:         hCtx,
		hashTable:    newConcurrentMapHashTable(len(hCtx)),
		rowContainer: rc,
	}
	return c
}

// GetMatchedRowsAndPtrs get matched rows and Ptrs from probeRow. It can be called
// in multiple goroutines while each goroutine should keep its own
// h and buf.
func (c *hashRowContainer) GetMatchedRowsAndPtrs(probeKey uint64, probeRow chunk.Row, hCtx *hashContext) (matched []chunk.Row, matchedPtrs []chunk.RowPtr, err error) {
	innerPtrs := c.hashTable.Get(probeKey)
	if len(innerPtrs) == 0 {
		return
	}
	matched = make([]chunk.Row, 0, len(innerPtrs))
	var matchedRow chunk.Row
	matchedPtrs = make([]chunk.RowPtr, 0, len(innerPtrs))
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
		matched = append(matched, matchedRow)
		matchedPtrs = append(matchedPtrs, ptr)
	}
	return
}

// matchJoinKey checks if join keys of buildRow and probeRow are logically equal.
func (c *hashRowContainer) matchJoinKey(buildRow, probeRow chunk.Row, probeHCtx *hashContext) (ok bool, err error) {
	return codec.EqualChunkRow(c.sc,
		buildRow, c.hCtx[0].allTypes, c.hCtx[0].keyColIdx,
		probeRow, probeHCtx.allTypes, probeHCtx.keyColIdx)
}

// alreadySpilledSafeForTest indicates that records have spilled out into disk. It's thread-safe.
func (c *hashRowContainer) alreadySpilledSafeForTest() bool {
	return c.rowContainer.AlreadySpilledSafeForTest()
}

// PutChunk puts a chunk into hashRowContainer and build hash map. It's not thread-safe.
// key of hash table: hash value of key columns
// value of hash table: RowPtr of the corresponded row
func (c *hashRowContainer) PutChunk(chk *chunk.Chunk, ignoreNulls []bool, workID uint, useOuterToBuild bool) error {
	return c.PutChunkSelected(chk, nil, ignoreNulls, workID, useOuterToBuild)
}

// PutChunkSelected selectively puts a chunk into hashRowContainer and build hash map. It's not thread-safe.
// key of hash table: hash value of key columns
// value of hash table: RowPtr of the corresponded row
func (c *hashRowContainer) PutChunkSelected(chk *chunk.Chunk, selected, ignoreNulls []bool, workID uint, useOuterToBuild bool) error {
	// safely add chunks using locks
	c.lock.Lock()
	if useOuterToBuild {
		var bitMap = bitmap.NewConcurrentBitmap(chk.NumRows())
		c.outerMatchedStatus = append(c.outerMatchedStatus, bitMap)
		c.GetMemTracker().Consume(bitMap.BytesConsumed())
	}
	chkIdx := uint32(c.rowContainer.NumChunks())
	err := c.rowContainer.Add(chk)
	c.lock.Unlock()
	if err != nil {
		return err
	}

	// concurrently insert tuples into the concurrent hash table
	numRows := chk.NumRows()
	hCtx := c.hCtx[workID]
	hCtx.initHash(numRows)
	for keyIdx, colIdx := range hCtx.keyColIdx {
		ignoreNull := len(ignoreNulls) > keyIdx && ignoreNulls[keyIdx]
		err := codec.HashChunkSelected(c.sc, hCtx.hashVals, chk, hCtx.allTypes[colIdx], colIdx, hCtx.buf, hCtx.hasNull, selected, ignoreNull)
		if err != nil {
			return errors.Trace(err)
		}
	}
	for i := 0; i < numRows; i++ {
		if (selected != nil && !selected[i]) || hCtx.hasNull[i] {
			continue
		}
		key := hCtx.hashVals[i].Sum64()
		rowPtr := chunk.RowPtr{ChkIdx: chkIdx, RowIdx: uint32(i)}
		c.hashTable.Put(key, rowPtr, int(workID))
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
func (c *hashRowContainer) GetChunk(chkIdx int) (*chunk.Chunk, error) {
	return c.rowContainer.GetChunk(chkIdx)
}

// GetRow returns the row the ptr pointed to in the rowContainer
func (c *hashRowContainer) GetRow(ptr chunk.RowPtr) (chunk.Row, error) {
	return c.rowContainer.GetRow(ptr)
}

// Len returns number of records in the hash table.
func (c *hashRowContainer) Len() uint64 {
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
	maxEntrySliceLen     = 8192
)

type entry struct {
	ptr  chunk.RowPtr
	next *entry
}

type entryStore struct {
	slices [][]entry
	cursor int
}

func newEntryStore() *entryStore {
	es := new(entryStore)
	es.slices = [][]entry{make([]entry, initialEntrySliceLen)}
	es.cursor = 0
	return es
}

func (es *entryStore) GetStore() (e *entry) {
	sliceIdx := uint32(len(es.slices) - 1)
	slice := es.slices[sliceIdx]
	if es.cursor >= cap(slice) {
		size := cap(slice) * 2
		if size >= maxEntrySliceLen {
			size = maxEntrySliceLen
		}
		slice = make([]entry, size)
		es.slices = append(es.slices, slice)
		sliceIdx++
		es.cursor = 0
	}
	e = &es.slices[sliceIdx][es.cursor]
	es.cursor++
	return
}

type baseHashTable interface {
	Put(hashKey uint64, rowPtr chunk.RowPtr, workID int)
	Get(hashKey uint64) (rowPtrs []chunk.RowPtr)
	Len() uint64
}

// TODO (fangzhuhe) remove unsafeHashTable later if it not used anymore
// unsafeHashTable stores multiple rowPtr of rows for a given key with minimum GC overhead.
// A given key can store multiple values.
// It is not thread-safe, should only be used in one goroutine.
type unsafeHashTable struct {
	hashMap    map[uint64]*entry
	entryStore *entryStore
	length     uint64
}

// newUnsafeHashTable creates a new unsafeHashTable. estCount means the estimated size of the hashMap.
// If unknown, set it to 0.
func newUnsafeHashTable(estCount int) *unsafeHashTable {
	ht := new(unsafeHashTable)
	ht.hashMap = make(map[uint64]*entry, estCount)
	ht.entryStore = newEntryStore()
	return ht
}

// Put puts the key/rowPtr pairs to the unsafeHashTable, multiple rowPtrs are stored in a list.
func (ht *unsafeHashTable) Put(hashKey uint64, rowPtr chunk.RowPtr, workID int) {
	oldEntry := ht.hashMap[hashKey]
	newEntry := ht.entryStore.GetStore()
	newEntry.ptr = rowPtr
	newEntry.next = oldEntry
	ht.hashMap[hashKey] = newEntry
	ht.length++
}

// Get gets the values of the "key" and appends them to "values".
func (ht *unsafeHashTable) Get(hashKey uint64) (rowPtrs []chunk.RowPtr) {
	entryAddr := ht.hashMap[hashKey]
	for entryAddr != nil {
		rowPtrs = append(rowPtrs, entryAddr.ptr)
		entryAddr = entryAddr.next
	}
	return
}

// Len returns the number of rowPtrs in the unsafeHashTable, the number of keys may be less than Len
// if the same key is put more than once.
func (ht *unsafeHashTable) Len() uint64 { return ht.length }

// concurrentMapHashTable is a concurrent hash table built on concurrentMap
type concurrentMapHashTable struct {
	hashMap    concurrentMap
	entryStore []*entryStore
	length     uint64
}

// newConcurrentMapHashTable creates a concurrentMapHashTable
func newConcurrentMapHashTable(concurrency int) *concurrentMapHashTable {
	ht := new(concurrentMapHashTable)
	ht.hashMap = newConcurrentMap()
	for i := 0; i < concurrency; i++ {
		newES := newEntryStore()
		ht.entryStore = append(ht.entryStore, newES)
	}
	ht.length = 0
	return ht
}

// Len return the number of rowPtrs in the concurrentMapHashTable
func (ht *concurrentMapHashTable) Len() uint64 {
	return ht.length
}

// Put puts the key/rowPtr pairs to the concurrentMapHashTable, multiple rowPtrs are stored in a list.
func (ht *concurrentMapHashTable) Put(hashKey uint64, rowPtr chunk.RowPtr, workID int) {
	newEntry := ht.entryStore[workID].GetStore()
	newEntry.ptr = rowPtr
	newEntry.next = nil
	ht.hashMap.Insert(hashKey, newEntry)
	atomic.AddUint64(&ht.length, 1)
}

// Get gets the values of the "key" and appends them to "values".
func (ht *concurrentMapHashTable) Get(hashKey uint64) (rowPtrs []chunk.RowPtr) {
	entryAddr, _ := ht.hashMap.Get(hashKey)
	for entryAddr != nil {
		rowPtrs = append(rowPtrs, entryAddr.ptr)
		entryAddr = entryAddr.next
	}
	return
}
