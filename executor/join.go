// Copyright 2016 PingCAP, Inc.
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
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mvmap"
	"golang.org/x/net/context"
)

var (
	_ Executor = &HashJoinExec{}
	_ Executor = &NestedLoopApplyExec{}
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	baseExecutor

	outerExec   Executor
	innerExec   Executor
	outerFilter expression.CNFExprs
	outerKeys   []*expression.Column
	innerKeys   []*expression.Column

	prepared        bool
	concurrency     uint // concurrency is number of concurrent channels and join workers.
	hashTable       *mvmap.MVMap
	innerFinished   chan error
	hashJoinBuffers []*hashJoinBuffer
	workerWaitGroup sync.WaitGroup // workerWaitGroup is for sync multiple join workers.
	finished        atomic.Value
	closeCh         chan struct{} // closeCh add a lock for closing executor.
	joinType        plannercore.JoinType
	innerIdx        int

	// We build individual joiner for each join worker when use chunk-based execution,
	// to avoid the concurrency of joiner.chk and joiner.selected.
	joiners []joiner

	outerKeyColIdx     []int
	innerKeyColIdx     []int
	innerResult        *chunk.List
	outerChkResourceCh chan *outerChkResource
	outerResultChs     []chan *chunk.Chunk
	joinChkResourceCh  []chan *chunk.Chunk
	joinResultCh       chan *hashjoinWorkerResult
	hashTableValBufs   [][][]byte

	memTracker *memory.Tracker // track memory usage.

	// radixBits indicates the bit number using for radix partitioning. Inner
	// relation will be split to 2^radixBits sub-relations before building the
	// hash tables. If the complete inner relation can be hold in L2Cache in
	// which case radixBits will be 0, we can skip the partition phase.
	radixBits int
}

// outerChkResource stores the result of the join outer fetch worker,
// `dest` is for Chunk reuse: after join workers process the outer chunk which is read from `dest`,
// they'll store the used chunk as `chk`, and then the outer fetch worker will put new data into `chk` and write `chk` into dest.
type outerChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
}

// hashjoinWorkerResult stores the result of join workers,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
// and push new data into this chunk.
type hashjoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

type hashJoinBuffer struct {
	data  []types.Datum
	bytes []byte
}

// Close implements the Executor Close interface.
func (e *HashJoinExec) Close() error {
	close(e.closeCh)
	e.finished.Store(true)
	if e.prepared {
		if e.innerFinished != nil {
			for range e.innerFinished {
			}
		}
		if e.joinResultCh != nil {
			for range e.joinResultCh {
			}
		}
		if e.outerChkResourceCh != nil {
			close(e.outerChkResourceCh)
			for range e.outerChkResourceCh {
			}
		}
		for i := range e.outerResultChs {
			for range e.outerResultChs[i] {
			}
		}
		for i := range e.joinChkResourceCh {
			close(e.joinChkResourceCh[i])
			for range e.joinChkResourceCh[i] {
			}
		}
		e.outerChkResourceCh = nil
		e.joinChkResourceCh = nil
	}
	e.memTracker.Detach()
	e.memTracker = nil

	err := e.baseExecutor.Close()
	return errors.Trace(err)
}

// Open implements the Executor Open interface.
func (e *HashJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaHashJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.hashTableValBufs = make([][][]byte, e.concurrency)
	e.hashJoinBuffers = make([]*hashJoinBuffer, 0, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		buffer := &hashJoinBuffer{
			data:  make([]types.Datum, len(e.outerKeys)),
			bytes: make([]byte, 0, 10000),
		}
		e.hashJoinBuffers = append(e.hashJoinBuffers, buffer)
	}

	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.workerWaitGroup = sync.WaitGroup{}
	return nil
}

func (e *HashJoinExec) getJoinKeyFromChkRow(isOuterKey bool, row chunk.Row, keyBuf []byte) (hasNull bool, _ []byte, err error) {
	var keyColIdx []int
	var allTypes []*types.FieldType
	if isOuterKey {
		keyColIdx = e.outerKeyColIdx
		allTypes = e.outerExec.retTypes()
	} else {
		keyColIdx = e.innerKeyColIdx
		allTypes = e.innerExec.retTypes()
	}

	for _, i := range keyColIdx {
		if row.IsNull(i) {
			return true, keyBuf, nil
		}
	}

	keyBuf = keyBuf[:0]
	keyBuf, err = codec.HashChunkRow(e.ctx.GetSessionVars().StmtCtx, keyBuf, row, allTypes, keyColIdx)
	if err != nil {
		err = errors.Trace(err)
	}
	return false, keyBuf, err
}

// fetchOuterChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchOuterChunks(ctx context.Context) {
	hasWaitedForInner := false
	for {
		if e.finished.Load().(bool) {
			return
		}
		var outerResource *outerChkResource
		ok := true
		select {
		case <-e.closeCh:
			return
		case outerResource, ok = <-e.outerChkResourceCh:
			if !ok {
				return
			}
		}
		outerResult := outerResource.chk
		err := e.outerExec.Next(ctx, outerResult)
		if err != nil {
			e.joinResultCh <- &hashjoinWorkerResult{
				err: errors.Trace(err),
			}
			return
		}
		if !hasWaitedForInner {
			if outerResult.NumRows() == 0 {
				e.finished.Store(true)
				return
			}
			jobFinished, innerErr := e.wait4Inner()
			if innerErr != nil {
				e.joinResultCh <- &hashjoinWorkerResult{
					err: errors.Trace(innerErr),
				}
				return
			} else if jobFinished {
				return
			}
			hasWaitedForInner = true
		}

		if outerResult.NumRows() == 0 {
			return
		}
		outerResource.dest <- outerResult
	}
}

func (e *HashJoinExec) wait4Inner() (finished bool, err error) {
	select {
	case <-e.closeCh:
		return true, nil
	case err, _ := <-e.innerFinished:
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	if e.hashTable.Len() == 0 && (e.joinType == plannercore.InnerJoin || e.joinType == plannercore.SemiJoin) {
		return true, nil
	}
	return false, nil
}

// fetchInnerRows fetches all rows from inner executor,
// and append them to e.innerResult.
func (e *HashJoinExec) fetchInnerRows(ctx context.Context, chkCh chan<- *chunk.Chunk, doneCh chan struct{}) {
	defer close(chkCh)
	e.innerResult = chunk.NewList(e.innerExec.retTypes(), e.initCap, e.maxChunkSize)
	e.innerResult.GetMemTracker().AttachTo(e.memTracker)
	e.innerResult.GetMemTracker().SetLabel("innerResult")
	var err error
	for {
		select {
		case <-doneCh:
			return
		case <-e.closeCh:
			return
		default:
			if e.finished.Load().(bool) {
				return
			}
			chk := e.children[e.innerIdx].newFirstChunk()
			err = e.innerExec.Next(ctx, chk)
			if err != nil {
				e.innerFinished <- errors.Trace(err)
				return
			}
			if chk.NumRows() == 0 {
				e.evalRadixBitNum()
				return
			}
			select {
			case chkCh <- chk:
				break
			case <-e.closeCh:
				return
			}
			e.innerResult.Add(chk)
		}
	}
}

// evalRadixBitNum evaluates the radix bit numbers.
func (e *HashJoinExec) evalRadixBitNum() {
	sv := e.ctx.GetSessionVars()
	// Calculate the bit number needed when using radix partition.
	if !sv.EnableRadixJoin {
		return
	}
	innerResultSize := float64(e.innerResult.GetMemTracker().BytesConsumed())
	// To ensure that one partition of inner relation, one hash
	// table and one partition of outer relation fit into the L2
	// cache when the input data obeys the uniform distribution,
	// we suppose every sub-partition of inner relation using
	// three quarters of the L2 cache size.
	l2CacheSize := float64(sv.L2CacheSize) * 3 / 4
	e.radixBits = int(math.Log2(innerResultSize / l2CacheSize))
}

func (e *HashJoinExec) initializeForProbe() {
	// e.outerResultChs is for transmitting the chunks which store the data of outerExec,
	// it'll be written by outer worker goroutine, and read by join workers.
	e.outerResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.outerResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.outerChkResourceCh is for transmitting the used outerExec chunks from join workers to outerExec worker.
	e.outerChkResourceCh = make(chan *outerChkResource, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.outerChkResourceCh <- &outerChkResource{
			chk:  e.outerExec.newFirstChunk(),
			dest: e.outerResultChs[i],
		}
	}

	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join worker goroutines.
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- e.newFirstChunk()
	}

	// e.joinResultCh is for transmitting the join result chunks to the main thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)

	e.outerKeyColIdx = make([]int, len(e.outerKeys))
	for i := range e.outerKeys {
		e.outerKeyColIdx[i] = e.outerKeys[i].Index
	}
}

func (e *HashJoinExec) fetchOuterAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	e.workerWaitGroup.Add(1)
	go util.WithRecovery(func() { e.fetchOuterChunks(ctx) }, e.finishOuterFetcher)

	// Start e.concurrency join workers to probe hash table and join inner and outer rows.
	for i := uint(0); i < e.concurrency; i++ {
		e.workerWaitGroup.Add(1)
		workID := i
		go util.WithRecovery(func() { e.runJoinWorker(workID) }, e.finishJoinWorker)
	}
	go util.WithRecovery(e.waitJoinWorkersAndCloseResultChan, nil)
}

func (e *HashJoinExec) finishOuterFetcher(r interface{}) {
	for i := range e.outerResultChs {
		close(e.outerResultChs[i])
	}
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.workerWaitGroup.Done()
}

func (e *HashJoinExec) finishJoinWorker(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.workerWaitGroup.Done()
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.workerWaitGroup.Wait()
	close(e.joinResultCh)
}

func (e *HashJoinExec) runJoinWorker(workerID uint) {
	var (
		outerResult *chunk.Chunk
		selected    = make([]bool, 0, chunk.InitialCapacity)
	)
	ok, joinResult := e.getNewJoinResult(workerID)
	if !ok {
		return
	}

	// Read and filter outerResult, and join the outerResult with the inner rows.
	emptyOuterResult := &outerChkResource{
		dest: e.outerResultChs[workerID],
	}
	for ok := true; ok; {
		if e.finished.Load().(bool) {
			break
		}
		select {
		case <-e.closeCh:
			return
		case outerResult, ok = <-e.outerResultChs[workerID]:
		}
		if !ok {
			break
		}
		ok, joinResult = e.join2Chunk(workerID, outerResult, joinResult, selected)
		if !ok {
			break
		}
		outerResult.Reset()
		emptyOuterResult.chk = outerResult
		e.outerChkResourceCh <- emptyOuterResult
	}
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		e.joinResultCh <- joinResult
	}
}

func (e *HashJoinExec) joinMatchedOuterRow2Chunk(workerID uint, outerRow chunk.Row,
	joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	buffer := e.hashJoinBuffers[workerID]
	hasNull, joinKey, err := e.getJoinKeyFromChkRow(true, outerRow, buffer.bytes)
	if err != nil {
		joinResult.err = errors.Trace(err)
		return false, joinResult
	}
	if hasNull {
		e.joiners[workerID].onMissMatch(outerRow, joinResult.chk)
		return true, joinResult
	}
	e.hashTableValBufs[workerID] = e.hashTable.Get(joinKey, e.hashTableValBufs[workerID][:0])
	innerPtrs := e.hashTableValBufs[workerID]
	if len(innerPtrs) == 0 {
		e.joiners[workerID].onMissMatch(outerRow, joinResult.chk)
		return true, joinResult
	}
	innerRows := make([]chunk.Row, 0, len(innerPtrs))
	for _, b := range innerPtrs {
		ptr := *(*chunk.RowPtr)(unsafe.Pointer(&b[0]))
		matchedInner := e.innerResult.GetRow(ptr)
		innerRows = append(innerRows, matchedInner)
	}
	iter := chunk.NewIterator4Slice(innerRows)
	hasMatch := false
	for iter.Begin(); iter.Current() != iter.End(); {
		matched, err := e.joiners[workerID].tryToMatch(outerRow, iter, joinResult.chk)
		if err != nil {
			joinResult.err = errors.Trace(err)
			return false, joinResult
		}
		hasMatch = hasMatch || matched

		if joinResult.chk.NumRows() == e.maxChunkSize {
			ok := true
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	if !hasMatch {
		e.joiners[workerID].onMissMatch(outerRow, joinResult.chk)
	}
	return true, joinResult
}

func (e *HashJoinExec) getNewJoinResult(workerID uint) (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		src: e.joinChkResourceCh[workerID],
	}
	ok := true
	select {
	case <-e.closeCh:
		ok = false
	case joinResult.chk, ok = <-e.joinChkResourceCh[workerID]:
	}
	return ok, joinResult
}

func (e *HashJoinExec) join2Chunk(workerID uint, outerChk *chunk.Chunk, joinResult *hashjoinWorkerResult,
	selected []bool) (ok bool, _ *hashjoinWorkerResult) {
	var err error
	selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(outerChk), selected)
	if err != nil {
		joinResult.err = errors.Trace(err)
		return false, joinResult
	}
	for i := range selected {
		if !selected[i] { // process unmatched outer rows
			e.joiners[workerID].onMissMatch(outerChk.GetRow(i), joinResult.chk)
		} else { // process matched outer rows
			ok, joinResult = e.joinMatchedOuterRow2Chunk(workerID, outerChk.GetRow(i), joinResult)
			if !ok {
				return false, joinResult
			}
		}
		if joinResult.chk.NumRows() == e.maxChunkSize {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from inner child and build a hash table;
// step 2. fetch data from outer child in a background goroutine and probe the hash table in multiple join workers.
func (e *HashJoinExec) Next(ctx context.Context, chk *chunk.Chunk) (err error) {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	if !e.prepared {
		e.innerFinished = make(chan error, 1)
		go util.WithRecovery(func() { e.fetchInnerAndBuildHashTable(ctx) }, e.finishFetchInnerAndBuildHashTable)
		e.fetchOuterAndProbeHashTable(ctx)
		e.prepared = true
	}
	chk.Reset()
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
	chk.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

func (e *HashJoinExec) finishFetchInnerAndBuildHashTable(r interface{}) {
	if r != nil {
		e.innerFinished <- errors.Errorf("%v", r)
	}
	close(e.innerFinished)
}

func (e *HashJoinExec) fetchInnerAndBuildHashTable(ctx context.Context) {
	// innerResultCh transfer inner result chunk from inner fetch to build hash table.
	innerResultCh := make(chan *chunk.Chunk, e.concurrency)
	doneCh := make(chan struct{})
	go util.WithRecovery(func() { e.fetchInnerRows(ctx, innerResultCh, doneCh) }, nil)

	// TODO: Parallel build hash table. Currently not support because `mvmap` is not thread-safe.
	err := e.buildHashTableForList(innerResultCh)
	if err != nil {
		e.innerFinished <- errors.Trace(err)
		close(doneCh)
	}
	// wait fetchInnerRows be finished.
	for range innerResultCh {
	}
}

// buildHashTableForList builds hash table from `list`.
// key of hash table: hash value of key columns
// value of hash table: RowPtr of the corresponded row
func (e *HashJoinExec) buildHashTableForList(innerResultCh chan *chunk.Chunk) error {
	e.hashTable = mvmap.NewMVMap()
	e.innerKeyColIdx = make([]int, len(e.innerKeys))
	for i := range e.innerKeys {
		e.innerKeyColIdx[i] = e.innerKeys[i].Index
	}
	var (
		hasNull bool
		err     error
		keyBuf  = make([]byte, 0, 64)
		valBuf  = make([]byte, 8)
	)

	chkIdx := uint32(0)
	for chk := range innerResultCh {
		if e.finished.Load().(bool) {
			return nil
		}
		numRows := chk.NumRows()
		for j := 0; j < numRows; j++ {
			hasNull, keyBuf, err = e.getJoinKeyFromChkRow(false, chk.GetRow(j), keyBuf)
			if err != nil {
				return errors.Trace(err)
			}
			if hasNull {
				continue
			}
			rowPtr := chunk.RowPtr{ChkIdx: chkIdx, RowIdx: uint32(j)}
			*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
			e.hashTable.Put(keyBuf, valBuf)
		}
		chkIdx++
	}
	return nil
}

// NestedLoopApplyExec is the executor for apply.
type NestedLoopApplyExec struct {
	baseExecutor

	innerRows   []chunk.Row
	cursor      int
	innerExec   Executor
	outerExec   Executor
	innerFilter expression.CNFExprs
	outerFilter expression.CNFExprs
	outer       bool

	joiner joiner

	outerSchema []*expression.CorrelatedColumn

	outerChunk       *chunk.Chunk
	outerChunkCursor int
	outerSelected    []bool
	innerList        *chunk.List
	innerChunk       *chunk.Chunk
	innerSelected    []bool
	innerIter        chunk.Iterator
	outerRow         *chunk.Row
	hasMatch         bool

	memTracker *memory.Tracker // track memory usage.
}

// Close implements the Executor interface.
func (e *NestedLoopApplyExec) Close() error {
	e.innerRows = nil

	e.memTracker.Detach()
	e.memTracker = nil
	return errors.Trace(e.outerExec.Close())
}

// Open implements the Executor interface.
func (e *NestedLoopApplyExec) Open(ctx context.Context) error {
	err := e.outerExec.Open(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	e.cursor = 0
	e.innerRows = e.innerRows[:0]
	e.outerChunk = e.outerExec.newFirstChunk()
	e.innerChunk = e.innerExec.newFirstChunk()
	e.innerList = chunk.NewList(e.innerExec.retTypes(), e.initCap, e.maxChunkSize)

	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaNestedLoopApply)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.innerList.GetMemTracker().SetLabel("innerList")
	e.innerList.GetMemTracker().AttachTo(e.memTracker)

	return nil
}

func (e *NestedLoopApplyExec) fetchSelectedOuterRow(ctx context.Context, chk *chunk.Chunk) (*chunk.Row, error) {
	outerIter := chunk.NewIterator4Chunk(e.outerChunk)
	for {
		if e.outerChunkCursor >= e.outerChunk.NumRows() {
			err := e.outerExec.Next(ctx, e.outerChunk)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.outerChunk.NumRows() == 0 {
				return nil, nil
			}
			e.outerSelected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, outerIter, e.outerSelected)
			if err != nil {
				return nil, errors.Trace(err)
			}
			e.outerChunkCursor = 0
		}
		outerRow := e.outerChunk.GetRow(e.outerChunkCursor)
		selected := e.outerSelected[e.outerChunkCursor]
		e.outerChunkCursor++
		if selected {
			return &outerRow, nil
		} else if e.outer {
			e.joiner.onMissMatch(outerRow, chk)
			if chk.NumRows() == e.maxChunkSize {
				return nil, nil
			}
		}
	}
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *NestedLoopApplyExec) fetchAllInners(ctx context.Context) error {
	err := e.innerExec.Open(ctx)
	defer terror.Call(e.innerExec.Close)
	if err != nil {
		return errors.Trace(err)
	}
	e.innerList.Reset()
	innerIter := chunk.NewIterator4Chunk(e.innerChunk)
	for {
		err := e.innerExec.Next(ctx, e.innerChunk)
		if err != nil {
			return errors.Trace(err)
		}
		if e.innerChunk.NumRows() == 0 {
			return nil
		}

		e.innerSelected, err = expression.VectorizedFilter(e.ctx, e.innerFilter, innerIter, e.innerSelected)
		if err != nil {
			return errors.Trace(err)
		}
		for row := innerIter.Begin(); row != innerIter.End(); row = innerIter.Next() {
			if e.innerSelected[row.Idx()] {
				e.innerList.AppendRow(row)
			}
		}
	}
}

// Next implements the Executor interface.
func (e *NestedLoopApplyExec) Next(ctx context.Context, chk *chunk.Chunk) (err error) {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.Reset()
	for {
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			if e.outerRow != nil && !e.hasMatch {
				e.joiner.onMissMatch(*e.outerRow, chk)
			}
			e.outerRow, err = e.fetchSelectedOuterRow(ctx, chk)
			if e.outerRow == nil || err != nil {
				return errors.Trace(err)
			}
			e.hasMatch = false

			for _, col := range e.outerSchema {
				*col.Data = e.outerRow.GetDatum(col.Index, col.RetType)
			}
			err = e.fetchAllInners(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			e.innerIter = chunk.NewIterator4List(e.innerList)
			e.innerIter.Begin()
		}

		matched, err := e.joiner.tryToMatch(*e.outerRow, e.innerIter, chk)
		e.hasMatch = e.hasMatch || matched

		if err != nil || chk.NumRows() == e.maxChunkSize {
			return errors.Trace(err)
		}
	}
}
