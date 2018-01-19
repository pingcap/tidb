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
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mvmap"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
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
	innerFilter expression.CNFExprs
	outerKeys   []*expression.Column
	innerKeys   []*expression.Column

	prepared        bool
	concurrency     int // concurrency is number of concurrent channels and join workers.
	hashTable       *mvmap.MVMap
	hashJoinBuffers []*hashJoinBuffer
	outerBufferChs  []chan *execResult
	workerWaitGroup sync.WaitGroup // workerWaitGroup is for sync multiple join workers.
	finished        atomic.Value
	closeCh         chan struct{} // closeCh add a lock for closing executor.
	joinType        plan.JoinType
	innerIdx        int

	// We build individual resultGenerator for each join worker when use chunk-based execution,
	// to avoid the concurrency of joinResultGenerator.chk and joinResultGenerator.selected.
	resultGenerators []joinResultGenerator

	resultBufferCh chan *execResult // Channels for output.
	resultBuffer   []Row
	resultCursor   int

	// for chunk execution
	outerKeyColIdx     []int
	innerKeyColIdx     []int
	innerResult        *chunk.List
	outerChkResourceCh chan *outerChkResource
	outerResultChs     []chan *chunk.Chunk
	joinChkResourceCh  []chan *chunk.Chunk
	joinResultCh       chan *hashjoinWorkerResult
	hashTableValBufs   [][][]byte
	totalMemoryUsage   int64
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
		if e.resultBufferCh != nil {
			for range e.resultBufferCh {
			}
		} else {
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
	}
	e.resultBuffer = nil

	err := e.baseExecutor.Close()
	return errors.Trace(err)
}

// Open implements the Executor Open interface.
func (e *HashJoinExec) Open(goCtx goctx.Context) error {
	if err := e.baseExecutor.Open(goCtx); err != nil {
		return errors.Trace(err)
	}

	e.prepared = false

	e.hashTableValBufs = make([][][]byte, e.concurrency)
	e.hashJoinBuffers = make([]*hashJoinBuffer, 0, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		buffer := &hashJoinBuffer{
			data:  make([]types.Datum, len(e.outerKeys)),
			bytes: make([]byte, 0, 10000),
		}
		e.hashJoinBuffers = append(e.hashJoinBuffers, buffer)
	}

	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.workerWaitGroup = sync.WaitGroup{}
	e.resultCursor = 0
	return nil
}

// makeJoinRow simply creates a new row that appends row b to row a.
func makeJoinRow(a Row, b Row) Row {
	ret := make([]types.Datum, 0, len(a)+len(b))
	ret = append(ret, a...)
	ret = append(ret, b...)
	return ret
}

func (e *HashJoinExec) encodeRow(b []byte, row Row) ([]byte, error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	for _, datum := range row {
		tmp, err := tablecodec.EncodeValue(sc, datum)
		if err != nil {
			return nil, errors.Trace(err)
		}
		b = append(b, tmp...)
	}
	return b, nil
}

func (e *HashJoinExec) decodeRow(data []byte) (Row, error) {
	values := make([]types.Datum, e.innerExec.Schema().Len())
	err := codec.SetRawValues(data, values)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = decodeRawValues(values, e.innerExec.Schema(), e.ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return values, nil
}

// getJoinKey gets the hash key when given a row and hash columns.
// It will return a boolean value representing if the hash key has null, a byte slice representing the result hash code.
func getJoinKey(sc *stmtctx.StatementContext, cols []*expression.Column, row Row, vals []types.Datum, bytes []byte) (bool, []byte, error) {
	var err error
	for i, col := range cols {
		vals[i], err = col.Eval(row)
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		if vals[i].IsNull() {
			return true, nil, nil
		}
	}
	if len(vals) == 0 {
		return false, nil, nil
	}
	bytes, err = codec.HashValues(sc, bytes, vals...)
	return false, bytes, errors.Trace(err)
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

// fetchOuterRows fetches rows from the big table in a background goroutine
// and sends the rows to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchOuterRows(goCtx goctx.Context) {
	defer func() {
		for _, outerBufferCh := range e.outerBufferChs {
			close(outerBufferCh)
		}
		e.workerWaitGroup.Done()
	}()

	bufferCapacity, maxBufferCapacity := 1, 128
	for i, noMoreData := 0, false; !noMoreData; i = (i + 1) % e.concurrency {
		outerBuffer := &execResult{rows: make([]Row, 0, bufferCapacity)}

		for !noMoreData && len(outerBuffer.rows) < bufferCapacity {
			if e.finished.Load().(bool) {
				return
			}

			outerRow, err := e.outerExec.Next(goCtx)
			if err != nil || outerRow == nil {
				outerBuffer.err = errors.Trace(err)
				noMoreData = true
				break
			}

			outerBuffer.rows = append(outerBuffer.rows, outerRow)
		}

		if noMoreData && len(outerBuffer.rows) == 0 && outerBuffer.err == nil {
			break
		}

		select {
		// TODO: Recover the code.
		// case <-e.ctx.GoCtx().Done():
		// 	return
		case e.outerBufferChs[i] <- outerBuffer:
			if !noMoreData && bufferCapacity < maxBufferCapacity {
				bufferCapacity <<= 1
			}
		}
	}
}

// fetchOuterChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchOuterChunks(goCtx goctx.Context) {
	defer func() {
		for i := range e.outerResultChs {
			close(e.outerResultChs[i])
		}
		e.workerWaitGroup.Done()
	}()
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
		err := e.outerExec.NextChunk(goCtx, outerResult)
		if err != nil {
			e.joinResultCh <- &hashjoinWorkerResult{
				err: errors.Trace(err),
			}
			return
		}
		if outerResult.NumRows() == 0 {
			return
		}
		outerResource.dest <- outerResult
	}
}

// fetchSelectedInnerRows fetches all the selected rows from inner executor,
// and append them to e.innerResult.
func (e *HashJoinExec) fetchSelectedInnerRows(goCtx goctx.Context) (err error) {
	innerExecChk := e.childrenResults[e.innerIdx]
	innerIter := chunk.NewIterator4Chunk(innerExecChk)
	selected := make([]bool, 0, chunk.InitialCapacity)
	innerResult := chunk.NewList(e.innerExec.retTypes(), e.maxChunkSize)
	memExceedThreshold, execMemThreshold := false, e.ctx.GetSessionVars().MemThreshold
	for {
		innerExecChk.Reset()
		err = e.innerExec.NextChunk(goCtx, innerExecChk)
		if err != nil {
			return errors.Trace(err)
		}
		if innerExecChk.NumRows() == 0 {
			break
		}
		selected, err = expression.VectorizedFilter(e.ctx, e.innerFilter, innerIter, selected)
		if err != nil {
			return errors.Trace(err)
		}
		for idx := range selected {
			if selected[idx] {
				innerResult.AppendRow(innerExecChk.GetRow(idx))
			}
		}
		innerMemUsage := innerResult.MemoryUsage()
		if !memExceedThreshold && innerMemUsage > execMemThreshold {
			memExceedThreshold = true
			log.Warnf(ErrMemExceedThreshold.GenByArgs("HashJoin", innerMemUsage, execMemThreshold).Error())
		}
	}
	e.innerResult = innerResult
	return nil
}

func (e *HashJoinExec) initializeForProbe() {
	// e.outerResultChs is for transmitting the chunks which store the data of outerExec,
	// it'll be written by outer worker goroutine, and read by join workers.
	e.outerResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		e.outerResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.outerChkResourceCh is for transmitting the used outerExec chunks from join workers to outerExec worker.
	e.outerChkResourceCh = make(chan *outerChkResource, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		e.outerChkResourceCh <- &outerChkResource{
			chk:  e.outerExec.newChunk(),
			dest: e.outerResultChs[i],
		}
	}

	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join worker goroutines.
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := 0; i < e.concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- e.newChunk()
	}

	// e.joinResultCh is for transmitting the join result chunks to the main thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)

	e.outerKeyColIdx = make([]int, len(e.outerKeys))
	for i := range e.outerKeys {
		e.outerKeyColIdx[i] = e.outerKeys[i].Index
	}
}

func (e *HashJoinExec) fetchOuterAndProbeHashTable(goCtx goctx.Context) {
	if e.hashTable.Len() == 0 && e.joinType == plan.InnerJoin {
		return
	}
	e.initializeForProbe()
	e.workerWaitGroup.Add(1)
	go e.fetchOuterChunks(goCtx)

	// Start e.concurrency join workers to probe hash table and join inner and outer rows.
	for i := 0; i < e.concurrency; i++ {
		e.workerWaitGroup.Add(1)
		go e.runJoinWorker4Chunk(i)
	}
	go e.waitJoinWorkersAndCloseResultChan(true)
}

// prepare4Rows runs the first time when 'Next' is called,
// it first starts one goroutine to reads all data from the small table to build a hash table,
// then starts one worker goroutine to fetch rows/chunk from the big table,
// and, then starts multiple join worker goroutines.
func (e *HashJoinExec) prepare4Row(goCtx goctx.Context) error {
	e.resultGenerators = e.resultGenerators[:1]
	e.hashTable = mvmap.NewMVMap()
	var buffer []byte
	for {
		innerRow, err := e.innerExec.Next(goCtx)
		if err != nil {
			return errors.Trace(err)
		}
		if innerRow == nil {
			break
		}

		matched, err := expression.EvalBool(e.innerFilter, innerRow, e.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !matched {
			continue
		}

		hasNull, joinKey, err := getJoinKey(e.ctx.GetSessionVars().StmtCtx, e.innerKeys, innerRow, e.hashJoinBuffers[0].data, nil)
		if err != nil {
			return errors.Trace(err)
		}
		if hasNull {
			continue
		}

		buffer = buffer[:0]
		buffer, err = e.encodeRow(buffer, innerRow)
		if err != nil {
			return errors.Trace(err)
		}
		e.hashTable.Put(joinKey, buffer)
	}

	e.prepared = true
	e.resultBufferCh = make(chan *execResult, e.concurrency)

	// If it's inner join and the small table is filtered out, there is no need to fetch big table and
	// start join workers to do the join work. Otherwise, we start one goroutine to fetch outer rows
	// and e.concurrency goroutines to concatenate the matched inner and outer rows and filter the result.
	if !(e.hashTable.Len() == 0 && e.joinType == plan.InnerJoin) {
		e.outerBufferChs = make([]chan *execResult, e.concurrency)
		for i := 0; i < e.concurrency; i++ {
			e.outerBufferChs[i] = make(chan *execResult, e.concurrency)
		}

		// Start a worker to fetch outer rows and partition them to join workers.
		e.workerWaitGroup.Add(1)
		go e.fetchOuterRows(goCtx)

		// Start e.concurrency join workers to probe hash table and join inner and outer rows.
		for i := 0; i < e.concurrency; i++ {
			e.workerWaitGroup.Add(1)
			go e.runJoinWorker(i)
		}
	}

	// start a goroutine to wait join workers finish their job and close channels.
	go e.waitJoinWorkersAndCloseResultChan(false)
	return nil
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan(forChunk bool) {
	e.workerWaitGroup.Wait()
	if !forChunk {
		close(e.resultBufferCh)
	} else {
		close(e.joinResultCh)
	}
}

// filterOuters filters the outer rows stored in "outerBuffer" and move all the matched rows ahead of the unmatched rows.
// The number of matched outer rows is returned as the first value.
func (e *HashJoinExec) filterOuters(outerBuffer *execResult, outerFilterResult []bool) (int, error) {
	if e.outerFilter == nil {
		return len(outerBuffer.rows), nil
	}

	outerFilterResult = outerFilterResult[:0]
	for _, outerRow := range outerBuffer.rows {
		matched, err := expression.EvalBool(e.outerFilter, outerRow, e.ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		outerFilterResult = append(outerFilterResult, matched)
	}

	i, j := 0, len(outerBuffer.rows)-1
	for i <= j {
		for i <= j && outerFilterResult[i] {
			i++
		}
		for i <= j && !outerFilterResult[j] {
			j--
		}
		if i <= j {
			outerFilterResult[i], outerFilterResult[j] = outerFilterResult[j], outerFilterResult[i]
			outerBuffer.rows[i], outerBuffer.rows[j] = outerBuffer.rows[j], outerBuffer.rows[i]
		}
	}
	return i, nil
}

// runJoinWorker does join job in one goroutine.
func (e *HashJoinExec) runJoinWorker(workerID int) {
	bufferCapacity := 1024
	resultBuffer := &execResult{rows: make([]Row, 0, bufferCapacity)}
	outerFilterResult := make([]bool, 0, bufferCapacity)

	var outerBuffer *execResult
	for ok := true; ok; {
		select {
		// TODO: Recover the code.
		// case <-e.ctx.GoCtx().Done():
		// 	ok = false
		case outerBuffer, ok = <-e.outerBufferChs[workerID]:
		}

		if !ok || e.finished.Load().(bool) {
			break
		}
		if outerBuffer.err != nil {
			resultBuffer.err = errors.Trace(outerBuffer.err)
			break
		}

		numMatchedOuters, err := e.filterOuters(outerBuffer, outerFilterResult)
		if err != nil {
			outerBuffer.err = errors.Trace(err)
			break
		}
		// process unmatched outer rows.
		for _, unMatchedOuter := range outerBuffer.rows[numMatchedOuters:] {
			resultBuffer.rows, resultBuffer.err = e.resultGenerators[0].emit(unMatchedOuter, nil, resultBuffer.rows)
			if resultBuffer.err != nil {
				resultBuffer.err = errors.Trace(resultBuffer.err)
				break
			}
		}
		if resultBuffer.err != nil {
			break
		}
		// process matched outer rows.
		for _, outerRow := range outerBuffer.rows[:numMatchedOuters] {
			if len(resultBuffer.rows) >= bufferCapacity {
				e.resultBufferCh <- resultBuffer
				resultBuffer = &execResult{rows: make([]Row, 0, bufferCapacity)}
			}
			ok = e.joinOuterRow(workerID, outerRow, resultBuffer)
			if !ok {
				break
			}
		}
	}

	if len(resultBuffer.rows) > 0 || resultBuffer.err != nil {
		e.resultBufferCh <- resultBuffer
	}
	e.workerWaitGroup.Done()
}

func (e *HashJoinExec) runJoinWorker4Chunk(workerID int) {
	defer e.workerWaitGroup.Done()
	var (
		outerResult *chunk.Chunk
		selected    = make([]bool, 0, chunk.InitialCapacity)
	)
	ok, joinResult := e.getNewJoinResult(workerID)
	if !ok {
		return
	}

	// Read and filter outerResult, and join the outerResult with the inner rows.
	joinResultBuffer := e.newChunk()
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
		ok, joinResult = e.join2Chunk(workerID, outerResult, joinResultBuffer, joinResult, selected)
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

// joinOuterRow creates result rows from a row in a big table and sends them to resultRows channel.
// Every matching row generates a result row.
// If there are no matching rows and it is outer join, a null filled result row is created.
func (e *HashJoinExec) joinOuterRow(workerID int, outerRow Row, resultBuffer *execResult) bool {
	buffer := e.hashJoinBuffers[workerID]
	hasNull, joinKey, err := getJoinKey(e.ctx.GetSessionVars().StmtCtx, e.outerKeys, outerRow, buffer.data, buffer.bytes[:0:cap(buffer.bytes)])
	if err != nil {
		resultBuffer.err = errors.Trace(err)
		return false
	}

	if hasNull {
		resultBuffer.rows, resultBuffer.err = e.resultGenerators[0].emit(outerRow, nil, resultBuffer.rows)
		resultBuffer.err = errors.Trace(resultBuffer.err)
		return true
	}

	e.hashTableValBufs[workerID] = e.hashTable.Get(joinKey, e.hashTableValBufs[workerID][:0])
	values := e.hashTableValBufs[workerID]
	if len(values) == 0 {
		resultBuffer.rows, resultBuffer.err = e.resultGenerators[0].emit(outerRow, nil, resultBuffer.rows)
		resultBuffer.err = errors.Trace(resultBuffer.err)
		return true
	}

	innerRows := make([]Row, 0, len(values))
	for _, value := range values {
		innerRow, err1 := e.decodeRow(value)
		if err1 != nil {
			resultBuffer.rows = nil
			resultBuffer.err = errors.Trace(err1)
			return false
		}
		innerRows = append(innerRows, innerRow)
	}

	resultBuffer.rows, resultBuffer.err = e.resultGenerators[0].emit(outerRow, innerRows, resultBuffer.rows)
	if resultBuffer.err != nil {
		resultBuffer.err = errors.Trace(resultBuffer.err)
		return false
	}
	return true
}

func (e *HashJoinExec) joinMatchedOuterRow2Chunk(workerID int, outerRow chunk.Row, chk *chunk.Chunk) error {
	buffer := e.hashJoinBuffers[workerID]
	hasNull, joinKey, err := e.getJoinKeyFromChkRow(true, outerRow, buffer.bytes)
	if err != nil {
		return errors.Trace(err)
	}
	if hasNull {
		err = e.resultGenerators[workerID].emitToChunk(outerRow, nil, chk)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	e.hashTableValBufs[workerID] = e.hashTable.Get(joinKey, e.hashTableValBufs[workerID][:0])
	innerPtrs := e.hashTableValBufs[workerID]
	if len(innerPtrs) == 0 {
		err = e.resultGenerators[workerID].emitToChunk(outerRow, nil, chk)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	innerRows := make([]chunk.Row, 0, len(innerPtrs))
	for _, b := range innerPtrs {
		ptr := *(*chunk.RowPtr)(unsafe.Pointer(&b[0]))
		matchedInner := e.innerResult.GetRow(ptr)
		innerRows = append(innerRows, matchedInner)
	}

	err = e.resultGenerators[workerID].emitToChunk(outerRow, chunk.NewIterator4Slice(innerRows), chk)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (e *HashJoinExec) getNewJoinResult(workerID int) (bool, *hashjoinWorkerResult) {
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

func (e *HashJoinExec) join2Chunk(workerID int, outerChk *chunk.Chunk, joinResultChkBuffer *chunk.Chunk, joinResult *hashjoinWorkerResult, selected []bool) (ok bool, _ *hashjoinWorkerResult) {
	var err error
	selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(outerChk), selected)
	if err != nil {
		joinResult.err = errors.Trace(err)
		return false, joinResult
	}
	for i := range selected {
		joinResultChkBuffer.Reset()
		if !selected[i] { // process unmatched outer rows
			err = e.resultGenerators[workerID].emitToChunk(outerChk.GetRow(i), nil, joinResultChkBuffer)
		} else { // process matched outer rows
			err = e.joinMatchedOuterRow2Chunk(workerID, outerChk.GetRow(i), joinResultChkBuffer)
		}
		if err != nil {
			joinResult.err = errors.Trace(err)
			return false, joinResult
		}
		// Splitting the joinResultChkBuffer into chunks that reach e.maxChunkSize.
		numAppended := 0
		for numAppended < joinResultChkBuffer.NumRows() {
			if joinResult.chk.NumRows() == e.maxChunkSize {
				e.joinResultCh <- joinResult
				ok, joinResult = e.getNewJoinResult(workerID)
				if !ok {
					return false, joinResult
				}
			}
			numBatchSize := mathutil.Min(e.maxChunkSize-joinResult.chk.NumRows(), joinResultChkBuffer.NumRows()-numAppended)
			joinResult.chk.Append(joinResultChkBuffer, numAppended, numAppended+numBatchSize)
			numAppended += numBatchSize
		}
	}
	return true, joinResult
}

// Next implements the Executor Next interface.
func (e *HashJoinExec) Next(goCtx goctx.Context) (Row, error) {
	if !e.prepared {
		if err := e.prepare4Row(goCtx); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if e.resultCursor >= len(e.resultBuffer) {
		e.resultCursor = 0
		select {
		case resultBuffer, ok := <-e.resultBufferCh:
			if !ok {
				return nil, nil
			}
			if resultBuffer.err != nil {
				e.finished.Store(true)
				return nil, errors.Trace(resultBuffer.err)
			}
			e.resultBuffer = resultBuffer.rows
			// TODO: Recover the code.
			// case <-e.ctx.GoCtx().Done():
			// 	return nil, nil
		}
	}

	// len(e.resultBuffer) > 0 is guaranteed in the above "select".
	result := e.resultBuffer[e.resultCursor]
	e.resultCursor++
	return result, nil
}

// NextChunk implements the Executor NextChunk interface.
// hash join constructs the result following these steps:
// step 1. fetch data from inner child and build a hash table;
// step 2. fetch data from outer child in a background goroutine and probe the hash table in multiple join workers.
func (e *HashJoinExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) (err error) {
	if !e.prepared {
		if err = e.fetchSelectedInnerRows(goCtx); err != nil {
			return errors.Trace(err)
		}
		if err = e.buildHashTableForList(); err != nil {
			return errors.Trace(err)
		}
		e.fetchOuterAndProbeHashTable(goCtx)
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

// NestedLoopApplyExec is the executor for apply.
type NestedLoopApplyExec struct {
	baseExecutor

	innerRows   []Row
	cursor      int
	resultRows  []Row
	innerExec   Executor
	outerExec   Executor
	innerFilter expression.CNFExprs
	outerFilter expression.CNFExprs
	outer       bool

	resultGenerator joinResultGenerator

	outerSchema []*expression.CorrelatedColumn

	outerChunk       *chunk.Chunk
	outerChunkCursor int
	outerSelected    []bool
	innerList        *chunk.List
	innerChunk       *chunk.Chunk
	innerSelected    []bool
	resultChunk      *chunk.Chunk
}

// Close implements the Executor interface.
func (e *NestedLoopApplyExec) Close() error {
	e.resultRows = nil
	e.innerRows = nil
	return errors.Trace(e.outerExec.Close())
}

// Open implements the Executor interface.
func (e *NestedLoopApplyExec) Open(goCtx goctx.Context) error {
	e.cursor = 0
	e.resultRows = e.resultRows[:0]
	e.innerRows = e.innerRows[:0]
	return errors.Trace(e.outerExec.Open(goCtx))
}

func (e *NestedLoopApplyExec) fetchOuterRow(goCtx goctx.Context) (Row, bool, error) {
	for {
		outerRow, err := e.outerExec.Next(goCtx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if outerRow == nil {
			return nil, false, nil
		}

		matched, err := expression.EvalBool(e.outerFilter, outerRow, e.ctx)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		if matched {
			return outerRow, true, nil
		} else if e.outer {
			return outerRow, false, nil
		}
	}
}

func (e *NestedLoopApplyExec) fetchSelectedOuterRow(goCtx goctx.Context) (*chunk.Row, error) {
	outerIter := chunk.NewIterator4Chunk(e.outerChunk)
	for {
		if e.outerChunkCursor >= e.outerChunk.NumRows() {
			err := e.outerExec.NextChunk(goCtx, e.outerChunk)
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
			err := e.resultGenerator.emitToChunk(outerRow, nil, e.resultChunk)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
}

// prepare reads all data from the inner table and stores them in a slice.
func (e *NestedLoopApplyExec) prepare(goCtx goctx.Context) error {
	err := e.innerExec.Open(goctx.TODO())
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(e.innerExec.Close)
	e.innerRows = e.innerRows[:0]
	for {
		row, err := e.innerExec.Next(goCtx)
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			return nil
		}

		matched, err := expression.EvalBool(e.innerFilter, row, e.ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if matched {
			e.innerRows = append(e.innerRows, row)
		}
	}
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *NestedLoopApplyExec) fetchAllInners(goCtx goctx.Context) error {
	err := e.innerExec.Open(goCtx)
	defer terror.Call(e.innerExec.Close)
	if err != nil {
		return errors.Trace(err)
	}
	e.innerList.Reset()
	innerIter := chunk.NewIterator4Chunk(e.innerChunk)
	for {
		err := e.innerExec.NextChunk(goCtx, e.innerChunk)
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

func (e *NestedLoopApplyExec) doJoin(outerRow Row, match bool) ([]Row, error) {
	e.resultRows = e.resultRows[0:0]
	var err error
	if !match && e.outer {
		e.resultRows, err = e.resultGenerator.emit(outerRow, nil, e.resultRows)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return e.resultRows, nil
	}
	e.resultRows, err = e.resultGenerator.emit(outerRow, e.innerRows, e.resultRows)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return e.resultRows, nil
}

// Next implements the Executor interface.
func (e *NestedLoopApplyExec) Next(goCtx goctx.Context) (Row, error) {
	for {
		if e.cursor < len(e.resultRows) {
			row := e.resultRows[e.cursor]
			e.cursor++
			return row, nil
		}
		outerRow, match, err := e.fetchOuterRow(goCtx)
		if outerRow == nil || err != nil {
			return nil, errors.Trace(err)
		}
		for _, col := range e.outerSchema {
			*col.Data = outerRow[col.Index]
		}
		err = e.prepare(goCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.resultRows, err = e.doJoin(outerRow, match)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.cursor = 0
	}
}

// buildHashTableForList builds hash table from `list`.
// key of hash table: hash value of key columns
// value of hash table: RowPtr of the corresponded row
func (e *HashJoinExec) buildHashTableForList() error {
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
	for i := 0; i < e.innerResult.NumChunks(); i++ {
		chk := e.innerResult.GetChunk(i)
		for j := 0; j < chk.NumRows(); j++ {
			hasNull, keyBuf, err = e.getJoinKeyFromChkRow(false, chk.GetRow(j), keyBuf)
			if err != nil {
				return errors.Trace(err)
			}
			if hasNull {
				continue
			}
			rowPtr := chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)}
			*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
			e.hashTable.Put(keyBuf, valBuf)
		}
	}
	return nil
}

// NextChunk implements the Executor interface.
func (e *NestedLoopApplyExec) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for {
		appendSize := mathutil.Min(e.resultChunk.NumRows()-e.cursor, e.maxChunkSize)
		if appendSize > 0 {
			chk.Append(e.resultChunk, e.cursor, e.cursor+appendSize)
			e.cursor += appendSize
			if chk.NumRows() == e.maxChunkSize {
				return nil
			}
		}
		outerRow, err := e.fetchSelectedOuterRow(goCtx)
		if outerRow == nil || err != nil {
			return errors.Trace(err)
		}
		for _, col := range e.outerSchema {
			*col.Data = outerRow.GetDatum(col.Index, col.RetType)
		}
		err = e.fetchAllInners(goCtx)
		if err != nil {
			return errors.Trace(err)
		}
		e.resultChunk.Reset()
		iter := chunk.NewIterator4List(e.innerList)
		err = e.resultGenerator.emitToChunk(*outerRow, iter, e.resultChunk)
		if err != nil {
			return errors.Trace(err)
		}
		e.cursor = 0
	}
}
