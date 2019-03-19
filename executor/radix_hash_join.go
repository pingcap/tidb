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

	// outerBatchSize indicates the number of outer rows used for probing every time.
	outerBatchSize       int
	wait4EvalRadixBitsCh chan struct{}
	outerChkResourceCh   chan *chunk.Chunk
	//outerPartitionsResourceCh chan outerPartitionsResource
	partitionTaskCh chan *partitionTask
	probeTaskCh     chan *probeTask
	partitionWg     *sync.WaitGroup
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

func (e *RadixHashJoinExec) init4Prepare() {
	e.wait4EvalRadixBitsCh = make(chan struct{}, 1)
	numChunk := e.outerBatchSize/e.maxChunkSize + 1
	e.outerChkResourceCh = make(chan *chunk.Chunk, numChunk)
	for i := 0; i < numChunk; i++ {
		e.outerChkResourceCh <- chunk.New(e.outerExec.retTypes(), e.initCap, e.maxChunkSize)
	}
	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join worker goroutines.
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- e.newFirstChunk()
	}
	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)
	e.outerKeyColIdx = make([]int, len(e.outerKeys))
	for i := range e.outerKeys {
		e.outerKeyColIdx[i] = e.outerKeys[i].Index
	}
	e.partitionWg = &sync.WaitGroup{}
	e.probeTaskCh = make(chan *probeTask, e.concurrency)
	e.startPartitionWorkers()
}

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
		e.init4Prepare()
		e.partitionWg.Add(2)
		go util.WithRecovery(func() { e.partitionInnerAndBuildHashTables(ctx) }, e.handlePartitionInnerAndBuildHashTablePanic)
		go util.WithRecovery(func() { e.fetchOuterAndPartition(ctx) }, e.handleFetchOuterAndPartitionPanic)
		go e.wait4InnerAndOuterPartitionFinish()
		e.prepared = true
	}
	req.Reset()
	if e.joinResultCh == nil {
		return nil
	}
	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		e.finished.Store(true)
		return errors.Trace(result.err)
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

func (e *RadixHashJoinExec) handlePartitionInnerAndBuildHashTablePanic(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
}

func (e *RadixHashJoinExec) wait4InnerAndOuterPartitionFinish() {
	e.partitionWg.Wait()
	close(e.partitionTaskCh)
}

func (e *RadixHashJoinExec) startPartitionWorkers() {
	e.partitionTaskCh = make(chan *partitionTask, e.concurrency)
	wg := &sync.WaitGroup{}
	wg.Add(int(e.concurrency))
	for i := 0; i < int(e.concurrency); i++ {
		workerID := i
		go util.WithRecovery(func() {
			e.doPartition(workerID, wg)
		}, e.handleFetchOuterAndPartitionPanic)
	}
	go e.wait4PartitionWorkers(wg)
}

func (e *RadixHashJoinExec) wait4PartitionWorkers(wg *sync.WaitGroup) {
	wg.Wait()
	close(e.probeTaskCh)
}

// doPartition runs concurrently, partitions and copies the `task.rawData` to
// several pre-allocated data partitions. The input Chunk idx for each
// partitioner is workerId + x*numPartitioners.
func (e *RadixHashJoinExec) doPartition(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	var task *partitionTask
	var ok bool
	for {
		if e.finished.Load().(bool) {
			return
		}
		select {
		case task, ok = <-e.partitionTaskCh:
			if !ok {
				return
			}
		case <-e.closeCh:
			return
		}
		task.Lock()
		startOffset := task.startOffset
		task.startOffset += 1
		task.Unlock()
		chkIdx, chkNum := startOffset, task.rawData.NumChunks()
		for ; chkIdx < chkNum; chkIdx += int(e.concurrency) {
			chk := task.rawData.GetChunk(chkIdx)
			for srcRowIdx, partPtr := range task.rowPtrs[chkIdx] {
				if partPtr == partPtr4NullKey {
					continue
				}
				partIdx, destRowIdx := partPtr.partitionIdx, partPtr.rowIdx
				part := task.parts[partIdx]
				part.Insert(int(destRowIdx), chk.GetRow(srcRowIdx))
			}
		}
		task.Lock()
		task.numFinished += 1
		allFinished := task.numFinished == int(e.concurrency)
		task.Unlock()
		if !task.isOuter {
			if allFinished {
				e.partitionWg.Done()
			}
			continue
		}
		// Give task.rawData back to outerFetcher.
		for chkIdx = startOffset; chkIdx < chkNum; chkIdx += int(e.concurrency) {
			e.outerChkResourceCh <- task.rawData.GetChunk(chkIdx)
		}
		if allFinished {
			e.probeTaskCh <- &probeTask{
				outerParts: task.parts,
			}
		}
	}
}

func (e *RadixHashJoinExec) wait4ProbeWorkers(wg *sync.WaitGroup) {
	wg.Wait()
	close(e.joinResultCh)
}

func (e *RadixHashJoinExec) doProbe(workerID uint, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		task      *probeTask
		ok        bool
		selected  = make([]bool, 0, e.maxChunkSize)
		hashTable = &hashTable4HashJoin{}
	)
	for {
		if e.finished.Load().(bool) {
			return
		}
		select {
		case task, ok = <-e.probeTaskCh:
			if !ok {
				return
			}
		case <-e.closeCh:
			return
		}
		ok, joinResult := e.getNewJoinResult(workerID)
		if !ok {
			return
		}
		for i, part := range task.outerParts {
			if part != nil && part.NumRows() != 0 {
				hashTable.MVMap = e.hashTables[i]
				hashTable.srcChk = e.innerParts[i]
				ok, joinResult = e.join2Chunk(workerID, part, hashTable, joinResult, selected)
				if !ok {
					break
				}
			}
		}
		if joinResult == nil {
			return
		} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
			e.joinResultCh <- joinResult
		}
	}
}

func (e *RadixHashJoinExec) startProbeWorkers() {
	wg := &sync.WaitGroup{}
	wg.Add(int(e.concurrency))
	for i := uint(0); i < e.concurrency; i++ {
		workerID := i
		go util.WithRecovery(func() {
			e.doProbe(workerID, wg)
		}, e.handleFetchOuterAndPartitionPanic)
	}
	go e.wait4ProbeWorkers(wg)
}

func (e *RadixHashJoinExec) handleFetchOuterAndPartitionPanic(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
}

func (e *RadixHashJoinExec) getPartition(parts []partition, idx uint32, isOuter bool) partition {
	if parts[idx] == nil {
		retType := e.innerExec.retTypes()
		if isOuter {
			retType = e.outerExec.retTypes()
		}
		e.numNonEmptyPart++
		parts[idx] = chunk.New(retType, e.initCap, e.maxChunkSize)
	}
	return parts[idx]
}

// evalRadixBit evaluates the radix bit numbers.
// To ensure that one partition of inner relation, one hash table, one partition
// of outer relation and the join result of these two partitions fit into the L2
// cache when the input data obeys the uniform distribution, we suppose every
// sub-partition of inner relation using three quarters of the L2 cache size.
func (e *RadixHashJoinExec) evalRadixBit() {
	defer func() {
		close(e.wait4EvalRadixBitsCh)
	}()
	sv := e.ctx.GetSessionVars()
	innerResultSize := float64(e.innerResult.GetMemTracker().BytesConsumed())
	l2CacheSize := float64(sv.L2CacheSize) * 3 / 4
	radixBitsNum := math.Ceil(math.Log2(innerResultSize / l2CacheSize))
	if radixBitsNum <= 0 {
		radixBitsNum = 1
	}
	// Take the rightmost radixBitsNum bits as the bitmask.
	e.radixBits = ^(math.MaxUint32 << uint(radixBitsNum))
	// The last slot is an extra slot, which stores nothing for inner relation.
	// We keep an empty slot here to make the length of `innerParts` equals to
	// the length `outerParts`. Thus we can avoid the checking of boundary when
	// do probing.
	e.innerParts = make([]partition, 1<<uint(radixBitsNum)+1)
	e.wait4EvalRadixBitsCh <- struct{}{}
}

// partitionInnerAndBuildHashTables fetches all the inner rows into memory,
// partition them into sub-relations, and build individual hash tables for every
// sub-relations.
func (e *RadixHashJoinExec) partitionInnerAndBuildHashTables(ctx context.Context) {
	if err := e.fetchInnerRows(ctx); err != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: err}
		return
	}
	e.evalRadixBit()
	task := &partitionTask{
		rawData: e.innerResult,
		isOuter: false,
		parts:   e.innerParts,
	}
	if err := e.partitionRawData(task); err != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: err}
		return
	}
	// This loop is used for waiting the inner-part phase.
	// TODO: find a better way to solve this.
	for task.numFinished < int(e.concurrency) {
	}
	if err := e.buildHashTable4Partitions(); err != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: err}
		return
	}
	// We start probe workers here because that probe workers should wait for
	// all the hash tables for every inner sub-relation been built.
	e.startProbeWorkers()
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

// partitionTask indicates a task which is created by innerFetcher or
// outerFetcher, and sent to partition workers.
type partitionTask struct {
	isOuter bool
	rawData *chunk.List
	rowPtrs [][]partRowPtr
	parts   []partition

	sync.Mutex
	// startOffset indicates the offset of `parts` that a partition worker
	// should start from when parallel writing raw data in.
	startOffset int
	// numFinished indicates the count of the partition workers that has
	// finished writing data into `parts`. When numFinished == e.concurrency is
	// true, it means that all the partitions are ready for probing.
	numFinished int
}

// probeTask indicates a task which is created by outerFetcher, and sent to
// probe workers.
type probeTask struct {
	outerParts []partition
}

// preAlloc4RawData evaluates partRowPtr and pre-alloc the memory space for
// every row of `task.rawData` to help re-order it.
func (e *RadixHashJoinExec) preAlloc4RawData(task *partitionTask) (err error) {
	var (
		hasNull bool
		keyBuf  = make([]byte, 0, 64)
		rawData = task.rawData
	)
	if task.isOuter {
		// The last slot of outerTask.parts is for the rows with null-key.
		task.parts = make([]partition, len(e.innerParts))
	}
	for chkIdx, chkNum := 0, rawData.NumChunks(); chkIdx < chkNum; chkIdx++ {
		var (
			chk      = rawData.GetChunk(chkIdx)
			partPtrs = make([]partRowPtr, chk.NumRows())
			partIdx  uint32
		)
		for rowIdx := 0; rowIdx < chk.NumRows(); rowIdx++ {
			row := chk.GetRow(rowIdx)
			hasNull, keyBuf, err = e.getJoinKeyFromChkRow(task.isOuter, row, keyBuf)
			if err != nil {
				return err
			}
			switch {
			case hasNull && !task.isOuter:
				partPtrs[rowIdx] = partPtr4NullKey
				continue
			case hasNull && task.isOuter:
				partIdx = uint32(len(task.parts)) - 1
			default:
				joinHash := murmur3.Sum32(keyBuf)
				partIdx = e.radixBits & joinHash
			}
			partPtrs[rowIdx].partitionIdx = partIdx
			partPtrs[rowIdx].rowIdx = e.getPartition(task.parts, partIdx, task.isOuter).PreAlloc(row)
		}
		task.rowPtrs = append(task.rowPtrs, partPtrs)
	}
	return
}

// partitionRawData re-order raw outer relation or inner relation into sub-relations.
func (e *RadixHashJoinExec) partitionRawData(task *partitionTask) (err error) {
	err = e.preAlloc4RawData(task)
	if err != nil {
		return err
	}
	for i := 0; i < int(e.concurrency); i++ {
		e.partitionTaskCh <- task
	}
	return
}

func (e *RadixHashJoinExec) fetchOuterAndPartition(ctx context.Context) {
	defer e.partitionWg.Done()
	var (
		err           error
		ok            bool
		isRadixBitsOk bool
		task          = &partitionTask{
			rawData: chunk.NewList(e.outerExec.retTypes(), e.initCap, e.maxChunkSize),
			isOuter: true,
		}
	)
	for {
		var chk *chunk.Chunk
		if e.finished.Load().(bool) {
			return
		}
		select {
		case <-e.closeCh:
			return
		case chk, ok = <-e.outerChkResourceCh:
			if !ok {
				return
			}
			chk.Reset()
		}
		err = e.outerExec.Next(ctx, chunk.NewRecordBatch(chk))
		if err != nil {
			break
		}
		if chk.NumRows() == 0 {
			if task.rawData.Len() > 0 {
				if !isRadixBitsOk {
					select {
					case _, ok = <-e.wait4EvalRadixBitsCh:
						if !ok {
							return
						}
					case <-e.closeCh:
						return
					}
				}
				err = e.partitionRawData(task)
			}
			break
		}
		task.rawData.Add(chk)
		if task.rawData.Len() >= e.outerBatchSize {
			if !isRadixBitsOk {
				select {
				case _, ok = <-e.wait4EvalRadixBitsCh:
					if !ok {
						return
					}
					isRadixBitsOk = true
				case <-e.closeCh:
					return
				}
			}
			err = e.partitionRawData(task)
			if err != nil {
				break
			}
			task = &partitionTask{
				rawData: chunk.NewList(e.outerExec.retTypes(), e.initCap, e.maxChunkSize),
				isOuter: true,
			}
		}
	}
	if err != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: err}
		return
	}
}

// Close implements the Executor Close interface.
func (e *RadixHashJoinExec) Close() error {
	close(e.closeCh)
	e.finished.Store(true)
	if e.prepared {
		if e.joinResultCh != nil {
			for range e.joinResultCh {
			}
		}
		if e.outerChkResourceCh != nil {
			close(e.outerChkResourceCh)
			for range e.outerChkResourceCh {
			}
			e.outerChkResourceCh = nil
		}
		for i := range e.outerResultChs {
			for range e.outerResultChs[i] {
			}
		}
		for i := range e.joinChkResourceCh {
			close(e.joinChkResourceCh[i])
			for range e.joinChkResourceCh[i] {
			}
			e.joinChkResourceCh[i] = nil
		}
	}
	e.memTracker.Detach()
	e.memTracker = nil

	err := e.baseExecutor.Close()
	return errors.Trace(err)
}
