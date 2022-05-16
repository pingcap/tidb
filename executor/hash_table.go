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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"fmt"
	"hash"
	"hash/fnv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/memory"
)

// hashContext keeps the needed hash context of a db table in hash join.
type hashContext struct {
	// allTypes one-to-one correspondence with keyColIdx
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
	// NOTE: probeCollision may be accessed from multiple goroutines concurrently.
	probeCollision   int64
	buildTableElapse time.Duration
}

func (s *hashStatistic) String() string {
	return fmt.Sprintf("probe_collision:%v, build:%v", s.probeCollision, execdetails.FormatDuration(s.buildTableElapse))
}

// hashRowContainer handles the rows and the hash map of a table.
// NOTE: a hashRowContainer may be shallow copied by the invoker, define all the
// member attributes as pointer type to avoid unexpected problems.
type hashRowContainer struct {
	sc   *stmtctx.StatementContext
	hCtx *hashContext
	stat *hashStatistic

	// hashTable stores the map of hashKey and RowPtr
	hashTable baseHashTable

	rowContainer *chunk.RowContainer
	memTracker   *memory.Tracker
}

func newHashRowContainer(sCtx sessionctx.Context, estCount int, hCtx *hashContext, allTypes []*types.FieldType) *hashRowContainer {
	maxChunkSize := sCtx.GetSessionVars().MaxChunkSize
	rc := chunk.NewRowContainer(allTypes, maxChunkSize)
	c := &hashRowContainer{
		sc:           sCtx.GetSessionVars().StmtCtx,
		hCtx:         hCtx,
		stat:         new(hashStatistic),
		hashTable:    newConcurrentMapHashTable(),
		rowContainer: rc,
		memTracker:   memory.NewTracker(memory.LabelForRowContainer, -1),
	}
	rc.GetMemTracker().AttachTo(c.GetMemTracker())
	return c
}

func (c *hashRowContainer) ShallowCopy() *hashRowContainer {
	newHRC := *c
	newHRC.rowContainer = c.rowContainer.ShallowCopyWithNewMutex()
	return &newHRC
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
			atomic.AddInt64(&c.stat.probeCollision, 1)
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
		buildRow, c.hCtx.allTypes, c.hCtx.keyColIdx,
		probeRow, probeHCtx.allTypes, probeHCtx.keyColIdx)
}

// alreadySpilledSafeForTest indicates that records have spilled out into disk. It's thread-safe.
// nolint: unused
func (c *hashRowContainer) alreadySpilledSafeForTest() bool {
	return c.rowContainer.AlreadySpilledSafeForTest()
}

// PutChunk puts a chunk into hashRowContainer and build hash map. It's not thread-safe.
// key of hash table: hash value of key columns
// value of hash table: RowPtr of the corresponded row
func (c *hashRowContainer) PutChunk(chk *chunk.Chunk, ignoreNulls []bool) error {
	return c.PutChunkSelected(chk, nil, ignoreNulls)
}

// PutChunkSelected selectively puts a chunk into hashRowContainer and build hash map. It's not thread-safe.
// key of hash table: hash value of key columns
// value of hash table: RowPtr of the corresponded row
func (c *hashRowContainer) PutChunkSelected(chk *chunk.Chunk, selected, ignoreNulls []bool) error {
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
	for keyIdx, colIdx := range c.hCtx.keyColIdx {
		ignoreNull := len(ignoreNulls) > keyIdx && ignoreNulls[keyIdx]
		err := codec.HashChunkSelected(c.sc, hCtx.hashVals, chk, hCtx.allTypes[keyIdx], colIdx, hCtx.buf, hCtx.hasNull, selected, ignoreNull)
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
	c.GetMemTracker().Consume(c.hashTable.GetAndCleanMemoryDelta())
	return nil
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
	defer c.memTracker.Detach()
	return c.rowContainer.Close()
}

// GetMemTracker returns the underlying memory usage tracker in hashRowContainer.
func (c *hashRowContainer) GetMemTracker() *memory.Tracker { return c.memTracker }

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

func (es *entryStore) GetStore() (e *entry, memDelta int64) {
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
		memDelta = int64(unsafe.Sizeof(entry{})) * int64(size)
	}
	e = &es.slices[sliceIdx][es.cursor]
	es.cursor++
	return
}

type baseHashTable interface {
	Put(hashKey uint64, rowPtr chunk.RowPtr)
	Get(hashKey uint64) (rowPtrs []chunk.RowPtr)
	Len() uint64
	// GetAndCleanMemoryDelta gets and cleans the memDelta of the baseHashTable. Memory delta will be cleared after each fetch.
	// It indicates the memory delta of the baseHashTable since the last calling GetAndCleanMemoryDelta().
	GetAndCleanMemoryDelta() int64
}

// TODO (fangzhuhe) remove unsafeHashTable later if it not used anymore
// unsafeHashTable stores multiple rowPtr of rows for a given key with minimum GC overhead.
// A given key can store multiple values.
// It is not thread-safe, should only be used in one goroutine.
type unsafeHashTable struct {
	hashMap    map[uint64]*entry
	entryStore *entryStore
	length     uint64

	bInMap   int64 // indicate there are 2^bInMap buckets in hashMap
	memDelta int64 // the memory delta of the unsafeHashTable since the last calling GetAndCleanMemoryDelta()
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
func (ht *unsafeHashTable) Put(hashKey uint64, rowPtr chunk.RowPtr) {
	oldEntry := ht.hashMap[hashKey]
	newEntry, memDelta := ht.entryStore.GetStore()
	newEntry.ptr = rowPtr
	newEntry.next = oldEntry
	ht.hashMap[hashKey] = newEntry
	if len(ht.hashMap) > (1<<ht.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta += hack.DefBucketMemoryUsageForMapIntToPtr * (1 << ht.bInMap)
		ht.bInMap++
	}
	ht.length++
	ht.memDelta += memDelta
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

// GetAndCleanMemoryDelta gets and cleans the memDelta of the unsafeHashTable.
func (ht *unsafeHashTable) GetAndCleanMemoryDelta() int64 {
	memDelta := ht.memDelta
	ht.memDelta = 0
	return memDelta
}

// concurrentMapHashTable is a concurrent hash table built on concurrentMap
type concurrentMapHashTable struct {
	hashMap    concurrentMap
	entryStore *entryStore
	length     uint64
	memDelta   int64 // the memory delta of the concurrentMapHashTable since the last calling GetAndCleanMemoryDelta()
}

// newConcurrentMapHashTable creates a concurrentMapHashTable
func newConcurrentMapHashTable() *concurrentMapHashTable {
	ht := new(concurrentMapHashTable)
	ht.hashMap = newConcurrentMap()
	ht.entryStore = newEntryStore()
	ht.length = 0
	ht.memDelta = hack.DefBucketMemoryUsageForMapIntToPtr + int64(unsafe.Sizeof(entry{}))*initialEntrySliceLen
	return ht
}

// Len return the number of rowPtrs in the concurrentMapHashTable
func (ht *concurrentMapHashTable) Len() uint64 {
	return ht.length
}

// Put puts the key/rowPtr pairs to the concurrentMapHashTable, multiple rowPtrs are stored in a list.
func (ht *concurrentMapHashTable) Put(hashKey uint64, rowPtr chunk.RowPtr) {
	newEntry, memDelta := ht.entryStore.GetStore()
	newEntry.ptr = rowPtr
	newEntry.next = nil
	memDelta += ht.hashMap.Insert(hashKey, newEntry)
	if memDelta != 0 {
		atomic.AddInt64(&ht.memDelta, memDelta)
	}
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

// GetAndCleanMemoryDelta gets and cleans the memDelta of the concurrentMapHashTable. Memory delta will be cleared after each fetch.
func (ht *concurrentMapHashTable) GetAndCleanMemoryDelta() int64 {
	var memDelta int64
	for {
		memDelta = atomic.LoadInt64(&ht.memDelta)
		if atomic.CompareAndSwapInt64(&ht.memDelta, memDelta, 0) {
			break
		}
	}
	return memDelta
}
