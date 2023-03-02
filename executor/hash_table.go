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
	"github.com/pingcap/tidb/util/bitmap"
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
	allTypes        []*types.FieldType
	keyColIdx       []int
	naKeyColIdx     []int
	buf             []byte
	hashVals        []hash.Hash64
	hasNull         []bool
	naHasNull       []bool
	naColNullBitMap []*bitmap.ConcurrentBitmap
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
	if len(hc.naKeyColIdx) > 0 {
		// isNAAJ
		if len(hc.naColNullBitMap) < rows {
			hc.naHasNull = make([]bool, rows)
			hc.naColNullBitMap = make([]*bitmap.ConcurrentBitmap, rows)
			for i := 0; i < rows; i++ {
				hc.naColNullBitMap[i] = bitmap.NewConcurrentBitmap(len(hc.naKeyColIdx))
			}
		} else {
			for i := 0; i < rows; i++ {
				hc.naHasNull[i] = false
				hc.naColNullBitMap[i].Reset(len(hc.naKeyColIdx))
			}
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

type hashNANullBucket struct {
	entries []*naEntry
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
	// hashNANullBucket stores the rows with any null value in NAAJ join key columns.
	// After build process, NANUllBucket is read only here for multi probe worker.
	hashNANullBucket *hashNANullBucket

	rowContainer *chunk.RowContainer
	memTracker   *memory.Tracker

	// chkBuf buffer the data reads from the disk if rowContainer is spilled.
	chkBuf *chunk.Chunk
}

func newHashRowContainer(sCtx sessionctx.Context, hCtx *hashContext, allTypes []*types.FieldType) *hashRowContainer {
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
	if isNAAJ := len(hCtx.naKeyColIdx) > 0; isNAAJ {
		c.hashNANullBucket = &hashNANullBucket{}
	}
	rc.GetMemTracker().AttachTo(c.GetMemTracker())
	return c
}

func (c *hashRowContainer) ShallowCopy() *hashRowContainer {
	newHRC := *c
	newHRC.rowContainer = c.rowContainer.ShallowCopyWithNewMutex()
	// multi hashRowContainer ref to one single NA-NULL bucket slice.
	// newHRC.hashNANullBucket = c.hashNANullBucket
	return &newHRC
}

// GetMatchedRows get matched rows from probeRow. It can be called
// in multiple goroutines while each goroutine should keep its own
// h and buf.
func (c *hashRowContainer) GetMatchedRows(probeKey uint64, probeRow chunk.Row, hCtx *hashContext, matched []chunk.Row) ([]chunk.Row, error) {
	matchedRows, _, err := c.GetMatchedRowsAndPtrs(probeKey, probeRow, hCtx, matched, nil, false)
	return matchedRows, err
}

func (c *hashRowContainer) GetAllMatchedRows(probeHCtx *hashContext, probeSideRow chunk.Row,
	probeKeyNullBits *bitmap.ConcurrentBitmap, matched []chunk.Row, needCheckBuildRowPos, needCheckProbeRowPos []int) ([]chunk.Row, error) {
	// for NAAJ probe row with null, we should match them with all build rows.
	var (
		ok        bool
		err       error
		innerPtrs []chunk.RowPtr
	)
	c.hashTable.Iter(
		func(_ uint64, e *entry) {
			entryAddr := e
			for entryAddr != nil {
				innerPtrs = append(innerPtrs, entryAddr.ptr)
				entryAddr = entryAddr.next
			}
		})
	matched = matched[:0]
	if len(innerPtrs) == 0 {
		return matched, nil
	}
	// all built bucket rows come from hash table, their bitmap are all nil (doesn't contain any null). so
	// we could only use the probe null bits to filter valid rows.
	if probeKeyNullBits != nil && len(probeHCtx.naKeyColIdx) > 1 {
		// if len(probeHCtx.naKeyColIdx)=1
		//     that means the NA-Join probe key is directly a (null) <-> (fetch all buckets), nothing to do.
		// else like
		//	   (null, 1, 2), we should use the not-null probe bit to filter rows. Only fetch rows like
		//     (  ? , 1, 2), that exactly with value as 1 and 2 in the second and third join key column.
		needCheckProbeRowPos = needCheckProbeRowPos[:0]
		needCheckBuildRowPos = needCheckBuildRowPos[:0]
		keyColLen := len(c.hCtx.naKeyColIdx)
		for i := 0; i < keyColLen; i++ {
			// since all bucket is from hash table (Not Null), so the buildSideNullBits check is eliminated.
			if probeKeyNullBits.UnsafeIsSet(i) {
				continue
			}
			needCheckBuildRowPos = append(needCheckBuildRowPos, c.hCtx.naKeyColIdx[i])
			needCheckProbeRowPos = append(needCheckProbeRowPos, probeHCtx.naKeyColIdx[i])
		}
	}
	var mayMatchedRow chunk.Row
	for _, ptr := range innerPtrs {
		mayMatchedRow, c.chkBuf, err = c.rowContainer.GetRowAndAppendToChunk(ptr, c.chkBuf)
		if err != nil {
			return nil, err
		}
		if probeKeyNullBits != nil && len(probeHCtx.naKeyColIdx) > 1 {
			// check the idxs-th value of the join columns.
			ok, err = codec.EqualChunkRow(c.sc, mayMatchedRow, c.hCtx.allTypes, needCheckBuildRowPos, probeSideRow, probeHCtx.allTypes, needCheckProbeRowPos)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			// once ok. just append the (maybe) valid build row for latter other conditions check if any.
		}
		matched = append(matched, mayMatchedRow)
	}
	return matched, nil
}

// GetMatchedRowsAndPtrs get matched rows and Ptrs from probeRow. It can be called
// in multiple goroutines while each goroutine should keep its own
// h and buf.
func (c *hashRowContainer) GetMatchedRowsAndPtrs(probeKey uint64, probeRow chunk.Row, hCtx *hashContext, matched []chunk.Row, matchedPtrs []chunk.RowPtr, needPtr bool) ([]chunk.Row, []chunk.RowPtr, error) {
	var err error
	innerPtrs := c.hashTable.Get(probeKey)
	if len(innerPtrs) == 0 {
		return nil, nil, err
	}
	matched = matched[:0]
	var matchedRow chunk.Row
	matchedPtrs = matchedPtrs[:0]
	for _, ptr := range innerPtrs {
		matchedRow, c.chkBuf, err = c.rowContainer.GetRowAndAppendToChunk(ptr, c.chkBuf)
		if err != nil {
			return nil, nil, err
		}
		var ok bool
		ok, err = c.matchJoinKey(matchedRow, probeRow, hCtx)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			atomic.AddInt64(&c.stat.probeCollision, 1)
			continue
		}
		matched = append(matched, matchedRow)
		if needPtr {
			matchedPtrs = append(matchedPtrs, ptr)
		}
	}
	return matched, matchedPtrs, err
}

func (c *hashRowContainer) GetNullBucketRows(probeHCtx *hashContext, probeSideRow chunk.Row,
	probeKeyNullBits *bitmap.ConcurrentBitmap, matched []chunk.Row, needCheckBuildRowPos, needCheckProbeRowPos []int) ([]chunk.Row, error) {
	var (
		ok            bool
		err           error
		mayMatchedRow chunk.Row
	)
	matched = matched[:0]
	for _, nullEntry := range c.hashNANullBucket.entries {
		mayMatchedRow, c.chkBuf, err = c.rowContainer.GetRowAndAppendToChunk(nullEntry.ptr, c.chkBuf)
		if err != nil {
			return nil, err
		}
		// since null bucket is a unified bucket. cases like below:
		// case1: left side (probe side) has null
		//    left side key <1,null>, actually we can fetch all bucket <1, ?> and filter 1 at the first join key, once
		//    got a valid right row after other condition, then we can just return.
		// case2: left side (probe side) don't have null
		//    left side key <1, 2>, actually we should fetch <1,null>, <null, 2>, <null, null> from the null bucket because
		//    case like <3,null> is obviously not matched with the probe key.
		needCheckProbeRowPos = needCheckProbeRowPos[:0]
		needCheckBuildRowPos = needCheckBuildRowPos[:0]
		keyColLen := len(c.hCtx.naKeyColIdx)
		if probeKeyNullBits != nil {
			// when the probeKeyNullBits is not nil, it means the probe key has null values, where we should distinguish
			// whether is empty set or not. In other words, we should fetch at least a valid from the null bucket here.
			// for values at the same index of the join key in which they are both not null, the values should be exactly the same.
			//
			// step: probeKeyNullBits & buildKeyNullBits, for those bits with 0, we should check if both values are the same.
			// we can just use the UnsafeIsSet here, because insert action of the build side has all finished.
			//
			// 1 0 1 0 means left join key  : null ? null ?
			// 1 0 0 0 means right join key : null ?   ?  ?
			// ---------------------------------------------
			// left & right: 1 0 1 0: just do the explicit column value check for whose bit is 0. (means no null from both side)
			for i := 0; i < keyColLen; i++ {
				if probeKeyNullBits.UnsafeIsSet(i) || nullEntry.nullBitMap.UnsafeIsSet(i) {
					continue
				}
				needCheckBuildRowPos = append(needCheckBuildRowPos, c.hCtx.naKeyColIdx[i])
				needCheckProbeRowPos = append(needCheckProbeRowPos, probeHCtx.naKeyColIdx[i])
			}
			// check the idxs-th value of the join columns.
			ok, err = codec.EqualChunkRow(c.sc, mayMatchedRow, c.hCtx.allTypes, needCheckBuildRowPos, probeSideRow, probeHCtx.allTypes, needCheckProbeRowPos)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		} else {
			// when the probeKeyNullBits is nil, it means the probe key is not null. But in the process of matching the null bucket,
			// we still need to do the non-null (explicit) value check.
			//
			// eg: the probe key is <1,2>, we only get <2, null> in the null bucket, even we can take the null as a wildcard symbol,
			// the first value of this two tuple is obviously not a match. So we need filter it here.
			for i := 0; i < keyColLen; i++ {
				if nullEntry.nullBitMap.UnsafeIsSet(i) {
					continue
				}
				needCheckBuildRowPos = append(needCheckBuildRowPos, c.hCtx.naKeyColIdx[i])
				needCheckProbeRowPos = append(needCheckProbeRowPos, probeHCtx.naKeyColIdx[i])
			}
			// check the idxs-th value of the join columns.
			ok, err = codec.EqualChunkRow(c.sc, mayMatchedRow, c.hCtx.allTypes, needCheckBuildRowPos, probeSideRow, probeHCtx.allTypes, needCheckProbeRowPos)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
		}
		// once ok. just append the (maybe) valid build row for latter other conditions check if any.
		matched = append(matched, mayMatchedRow)
	}
	return matched, err
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
	// By now, the combination of 1 and 2 can't take a run at same time.
	// 1: write the row data of join key to hashVals. (normal EQ key should ignore the null values.) null-EQ for Except statement is an exception.
	for keyIdx, colIdx := range c.hCtx.keyColIdx {
		ignoreNull := len(ignoreNulls) > keyIdx && ignoreNulls[keyIdx]
		err := codec.HashChunkSelected(c.sc, hCtx.hashVals, chk, hCtx.allTypes[keyIdx], colIdx, hCtx.buf, hCtx.hasNull, selected, ignoreNull)
		if err != nil {
			return errors.Trace(err)
		}
	}
	// 2: write the row data of NA join key to hashVals. (NA EQ key should collect all rows including null value as one bucket.)
	isNAAJ := len(c.hCtx.naKeyColIdx) > 0
	hasNullMark := make([]bool, len(hCtx.hasNull))
	for keyIdx, colIdx := range c.hCtx.naKeyColIdx {
		// NAAJ won't ignore any null values, but collect them as one hash bucket.
		err := codec.HashChunkSelected(c.sc, hCtx.hashVals, chk, hCtx.allTypes[keyIdx], colIdx, hCtx.buf, hCtx.hasNull, selected, false)
		if err != nil {
			return errors.Trace(err)
		}
		// todo: we can collect the bitmap in codec.HashChunkSelected to avoid loop here, but the params modification is quite big.
		// after fetch one NA column, collect the null value to null bitmap for every row. (use hasNull flag to accelerate)
		// eg: if a NA Join cols is (a, b, c), for every build row here we maintained a 3-bit map to mark which column are null for them.
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if hCtx.hasNull[rowIdx] {
				hCtx.naColNullBitMap[rowIdx].UnsafeSet(keyIdx)
				// clean and try fetch next NA join col.
				hCtx.hasNull[rowIdx] = false
				// just a mark variable for whether there is a null in at least one NA join column.
				hasNullMark[rowIdx] = true
			}
		}
	}
	for i := 0; i < numRows; i++ {
		if isNAAJ {
			if selected != nil && !selected[i] {
				continue
			}
			if hasNullMark[i] {
				// collect the null rows to slice.
				rowPtr := chunk.RowPtr{ChkIdx: chkIdx, RowIdx: uint32(i)}
				// do not directly ref the null bits map here, because the bit map will be reset and reused in next batch of chunk data.
				c.hashNANullBucket.entries = append(c.hashNANullBucket.entries, &naEntry{rowPtr, c.hCtx.naColNullBitMap[i].Clone()})
			} else {
				// insert the not-null rows to hash table.
				key := c.hCtx.hashVals[i].Sum64()
				rowPtr := chunk.RowPtr{ChkIdx: chkIdx, RowIdx: uint32(i)}
				c.hashTable.Put(key, rowPtr)
			}
		} else {
			if (selected != nil && !selected[i]) || c.hCtx.hasNull[i] {
				continue
			}
			key := c.hCtx.hashVals[i].Sum64()
			rowPtr := chunk.RowPtr{ChkIdx: chkIdx, RowIdx: uint32(i)}
			c.hashTable.Put(key, rowPtr)
		}
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
	c.chkBuf = nil
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

type naEntry struct {
	ptr        chunk.RowPtr
	nullBitMap *bitmap.ConcurrentBitmap
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
	Iter(func(uint64, *entry))
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

func (ht *unsafeHashTable) Iter(traverse func(key uint64, e *entry)) {
	for k := range ht.hashMap {
		entryAddr := ht.hashMap[k]
		traverse(k, entryAddr)
	}
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

// Iter gets the every value of the hash table.
func (ht *concurrentMapHashTable) Iter(traverse func(key uint64, e *entry)) {
	ht.hashMap.IterCb(traverse)
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
