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
	"context"
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mvmap"
	log "github.com/sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

var (
	_ Executor = &RadixHashJoinExec{}
)

// RadixHashJoinExec implements the radix partition-based hash join algorithm.
// It will partition the input relations into small pairs of partitions where
// one of the partitions typically fits into one of the caches. The overall goal
// of this method is to minimize the number of cache misses when building and
// probing hash tables.
type RadixHashJoinExec struct {
	*HashJoinExec

	// radixBits indicates the bits using for radix partitioning. Inner relation
	// will be split to 2^radixBitsNumber sub-relations before building the hash
	// tables. If the complete inner relation can be hold in L2Cache in which
	// case radixBits will be 1, we can skip the partition phase.
	// Note: We actually check whether `size of sub inner relation < 3/4 * L2
	// cache size` to make sure one inner sub-relation, hash table, one outer
	// sub-relation and join result of the sub-relations can be totally loaded
	// in L2 cache size. `3/4` is a magic number, we may adjust it after
	// benchmark.
	radixBits       uint32
	innerParts      []partition
	numNonEmptyPart int
	// innerRowPrts indicates the position in corresponding partition of every
	// row in innerResult.
	innerRowPrts [][]partRowPtr
	// hashTables stores the hash tables built from the inner relation, if there
	// is no partition phase, a global hash table will be stored in
	// hashTables[0].
	hashTables []*mvmap.MVMap
}

// partition stores the sub-relations of inner relation and outer relation after
// partition phase. Every partition can be fully stored in L2 cache thus can
// reduce the cache miss ratio when building and probing the hash table.
type partition = *chunk.Chunk

// partRowPtr stores the actual index in `innerParts` or `outerParts`.
type partRowPtr struct {
	partitionIdx uint32
	rowIdx       uint32
}

// partPtr4NullKey indicates a partition pointer which points to a row with null-join-key.
var partPtr4NullKey = partRowPtr{math.MaxUint32, math.MaxUint32}

// Next implements the Executor Next interface.
// radix hash join constructs the result following these steps:
// step 1. fetch data from inner child
// step 2. parallel partition the inner relation into sub-relations and build an
// individual hash table for every partition
// step 3. fetch data from outer child in a background goroutine and partition
// it into sub-relations
// step 4. probe the corresponded sub-hash-table for every sub-outer-relation in
// multiple join workers
func (e *RadixHashJoinExec) Next(ctx context.Context, req *chunk.RecordBatch) (err error) {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), req.NumRows()) }()
	}
	if !e.prepared {
		e.innerFinished = make(chan error, 1)
		go util.WithRecovery(func() { e.partitionInnerAndBuildHashTables(ctx) }, e.handleFetchInnerAndBuildHashTablePanic)
		// TODO: parallel fetch outer rows, partition them and do parallel join
		e.prepared = true
	}
	return <-e.innerFinished
}

// partitionInnerRows re-order e.innerResults into sub-relations.
func (e *RadixHashJoinExec) partitionInnerRows() error {
	e.evalRadixBit()
	if err := e.preAlloc4InnerParts(); err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	defer wg.Wait()
	wg.Add(int(e.concurrency))
	for i := 0; i < int(e.concurrency); i++ {
		workerID := i
		go util.WithRecovery(func() {
			defer wg.Done()
			e.doInnerPartition(workerID)
		}, e.handlePartitionPanic)
	}
	return nil
}

func (e *RadixHashJoinExec) handlePartitionPanic(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
}

// doInnerPartition runs concurrently, partitions and copies the inner relation
// to several pre-allocated data partitions. The input inner Chunk idx for each
// partitioner is workerId + x*numPartitioners.
func (e *RadixHashJoinExec) doInnerPartition(workerID int) {
	chkIdx, chkNum := workerID, e.innerResult.NumChunks()
	for ; chkIdx < chkNum; chkIdx += int(e.concurrency) {
		if e.finished.Load().(bool) {
			return
		}
		chk := e.innerResult.GetChunk(chkIdx)
		for srcRowIdx, partPtr := range e.innerRowPrts[chkIdx] {
			if partPtr == partPtr4NullKey {
				continue
			}
			partIdx, destRowIdx := partPtr.partitionIdx, partPtr.rowIdx
			part := e.innerParts[partIdx]
			part.Insert(int(destRowIdx), chk.GetRow(srcRowIdx))
		}
	}
}

// preAlloc4InnerParts evaluates partRowPtr and pre-alloc the memory space
// for every inner row to help re-order the inner relation.
// TODO: we need to evaluate the skewness for the partitions size, if the
// skewness exceeds a threshold, we do not use partition phase.
func (e *RadixHashJoinExec) preAlloc4InnerParts() (err error) {
	hasNull, keyBuf := false, make([]byte, 0, 64)
	for chkIdx, chkNum := 0, e.innerResult.NumChunks(); chkIdx < chkNum; chkIdx++ {
		chk := e.innerResult.GetChunk(chkIdx)
		partPtrs := make([]partRowPtr, chk.NumRows())
		for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
			row := chk.GetRow(rowIdx)
			hasNull, keyBuf, err = e.getJoinKeyFromChkRow(false, row, keyBuf)
			if err != nil {
				return err
			}
			if hasNull {
				partPtrs[rowIdx] = partPtr4NullKey
				continue
			}
			joinHash := murmur3.Sum32(keyBuf)
			partIdx := e.radixBits & joinHash
			partPtrs[rowIdx].partitionIdx = partIdx
			partPtrs[rowIdx].rowIdx = e.getPartition(partIdx).PreAlloc(row)
		}
		e.innerRowPrts = append(e.innerRowPrts, partPtrs)
	}
	if e.numNonEmptyPart < len(e.innerParts) {
		numTotalPart := len(e.innerParts)
		numEmptyPart := numTotalPart - e.numNonEmptyPart
		log.Debugf("[EMPTY_PART_IN_RADIX_HASH_JOIN] txn_start_ts:%v, num_empty_parts:%v, "+
			"num_total_parts:%v, empty_ratio:%v", e.ctx.GetSessionVars().TxnCtx.StartTS,
			numEmptyPart, numTotalPart, float64(numEmptyPart)/float64(numTotalPart))
	}
	return
}

func (e *RadixHashJoinExec) getPartition(idx uint32) partition {
	if e.innerParts[idx] == nil {
		e.numNonEmptyPart++
		e.innerParts[idx] = chunk.New(e.innerExec.retTypes(), e.initCap, e.maxChunkSize)
	}
	return e.innerParts[idx]
}

// evalRadixBit evaluates the radix bit numbers.
// To ensure that one partition of inner relation, one hash table, one partition
// of outer relation and the join result of these two partitions fit into the L2
// cache when the input data obeys the uniform distribution, we suppose every
// sub-partition of inner relation using three quarters of the L2 cache size.
func (e *RadixHashJoinExec) evalRadixBit() {
	sv := e.ctx.GetSessionVars()
	innerResultSize := float64(e.innerResult.GetMemTracker().BytesConsumed())
	l2CacheSize := float64(sv.L2CacheSize) * 3 / 4
	radixBitsNum := math.Ceil(math.Log2(innerResultSize / l2CacheSize))
	if radixBitsNum <= 0 {
		radixBitsNum = 1
	}
	// Take the rightmost radixBitsNum bits as the bitmask.
	e.radixBits = ^(math.MaxUint32 << uint(radixBitsNum))
	e.innerParts = make([]partition, 1<<uint(radixBitsNum))
}

// partitionInnerAndBuildHashTables fetches all the inner rows into memory,
// partition them into sub-relations, and build individual hash tables for every
// sub-relations.
func (e *RadixHashJoinExec) partitionInnerAndBuildHashTables(ctx context.Context) {
	if err := e.fetchInnerRows(ctx); err != nil {
		e.innerFinished <- err
		return
	}

	if err := e.partitionInnerRows(); err != nil {
		e.innerFinished <- err
		return
	}
	if err := e.buildHashTable4Partitions(); err != nil {
		e.innerFinished <- err
	}
}

func (e *RadixHashJoinExec) wait4BuildHashTable(wg *sync.WaitGroup, finishedCh chan error) {
	wg.Wait()
	close(finishedCh)
}

func (e *RadixHashJoinExec) buildHashTable4Partitions() error {
	e.hashTables = make([]*mvmap.MVMap, len(e.innerParts))
	buildFinishedCh := make(chan error, e.concurrency)
	wg := &sync.WaitGroup{}
	wg.Add(int(e.concurrency))
	go e.wait4BuildHashTable(wg, buildFinishedCh)
	for i := 0; i < int(e.concurrency); i++ {
		workerID := i
		go util.WithRecovery(func() {
			defer wg.Done()
			e.doBuild(workerID, buildFinishedCh)
		}, nil)
	}
	return <-buildFinishedCh
}

func (e *RadixHashJoinExec) doBuild(workerID int, finishedCh chan error) {
	var err error
	keyBuf, valBuf := make([]byte, 0, 64), make([]byte, 4)
	for i := workerID; i < len(e.innerParts); i += int(e.concurrency) {
		if e.innerParts[i] == nil {
			continue
		}
		e.hashTables[i] = mvmap.NewMVMap()
		keyBuf = keyBuf[:0]
		for rowIdx, numRows := 0, e.innerParts[i].NumRows(); rowIdx < numRows; rowIdx++ {
			// Join-key can be promised to be NOT NULL in a partition(see `partPtr4NullKey`), so we do not check it.
			_, keyBuf, err = e.getJoinKeyFromChkRow(false, e.innerParts[i].GetRow(rowIdx), keyBuf)
			if err != nil {
				e.finished.Store(true)
				finishedCh <- err
				return
			}
			*(*uint32)(unsafe.Pointer(&valBuf[0])) = uint32(rowIdx)
			e.hashTables[i].Put(keyBuf, valBuf)
		}
	}
}
