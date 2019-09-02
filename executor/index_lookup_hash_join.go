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
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
)

// IndexNestedLoopHashJoin employs one outer worker and N inner workers to
// execute concurrently. The output order is not promised.
//
// The execution flow is very similar to IndexLookUpReader:
// 1. The outer worker reads N outer rows, builds a task and sends it to the
// inner worker channel.
// 2. The inner worker receives the tasks and does 3 things for every task:
//    1. builds hash table from the outer rows
//    2. builds key ranges from outer rows and fetches inner rows
//    3. probes the hash table and sends the join result to the main thread channel.
//    Note: step 1 and step 2 runs concurrently.
// 3. The main thread receives the join results.
type IndexNestedLoopHashJoin struct {
	IndexLookUpJoin
	resultCh          chan *indexHashJoinResult
	joinChkResourceCh []chan *chunk.Chunk
}

type indexHashJoinOuterWorker struct {
	outerWorker
	innerCh chan *indexHashJoinTask
}

type indexHashJoinInnerWorker struct {
	innerWorker
	matchedOuterPtrs  [][]byte
	joiner            joiner
	joinChkResourceCh chan *chunk.Chunk
	resultCh          chan *indexHashJoinResult
	taskCh            <-chan *indexHashJoinTask
	wg                *sync.WaitGroup
	finished          atomic.Value
}

type indexHashJoinResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

type outerRowStatusFlag byte

const (
	_ outerRowStatusFlag = iota // outerRowUnmatched
	outerRowMatched
	outerRowHasNull
)

type indexHashJoinTask struct {
	*lookUpJoinTask
	outerRowStatus []outerRowStatusFlag
	err            error
}

// Open implements the IndexNestedLoopHashJoin Executor interface.
func (e *IndexNestedLoopHashJoin) Open(ctx context.Context) error {
	// Be careful, very dirty hack in this line!!!
	// IndexLookUpJoin need to rebuild executor (the dataReaderBuilder) during
	// executing. However `executor.Next()` is lazy evaluation when the RecordSet
	// result is drained.
	// Lazy evaluation means the saved session context may change during executor's
	// building and its running.
	// A specific sequence for example:
	//
	// e := buildExecutor()   // txn at build time
	// recordSet := runStmt(e)
	// session.CommitTxn()    // txn closed
	// recordSet.Next()
	// e.dataReaderBuilder.Build() // txn is used again, which is already closed
	//
	// The trick here is `getStartTS` will cache start ts in the dataReaderBuilder,
	// so even txn is destroyed later, the dataReaderBuilder could still use the
	// cached start ts to construct DAG.
	_, err := e.innerCtx.readerBuilder.getStartTS()
	if err != nil {
		return err
	}

	err = e.children[0].Open(ctx)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaIndexLookupJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.innerPtrBytes = make([][]byte, 0, 8)
	e.startWorkers(ctx)
	return nil
}

func (e *IndexNestedLoopHashJoin) startWorkers(ctx context.Context) {
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *indexHashJoinTask, concurrency)
	e.workerWg.Add(1)
	ow := e.newOuterWorker(innerCh)
	go util.WithRecovery(func() { ow.run(workerCtx) }, e.finishJoinWorkers)

	e.resultCh = make(chan *indexHashJoinResult, concurrency+1)
	e.joinChkResourceCh = make([]chan *chunk.Chunk, concurrency)
	for i := int(0); i < concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}
	e.workerWg.Add(concurrency)
	for i := int(0); i < concurrency; i++ {
		workerID := i
		go util.WithRecovery(func() { e.newInnerWorker(innerCh, workerID).run(workerCtx) }, e.finishJoinWorkers)
	}
	go e.wait4JoinWorkers()
}

func (e *IndexNestedLoopHashJoin) finishJoinWorkers(r interface{}) {
	if r != nil {
		e.resultCh <- &indexHashJoinResult{err: errors.Errorf("%v", r)}
	}
	e.workerWg.Done()
}

func (e *IndexNestedLoopHashJoin) wait4JoinWorkers() {
	e.workerWg.Wait()
	close(e.resultCh)
}

// Next implements the IndexNestedLoopHashJoin Executor interface.
func (e *IndexNestedLoopHashJoin) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	result, ok := <-e.resultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		return result.err
	}
	req.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the IndexNestedLoopHashJoin Executor interface.
func (e *IndexNestedLoopHashJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
		e.cancelFunc = nil
	}
	if e.resultCh != nil {
		for range e.resultCh {
		}
		e.resultCh = nil
	}
	for i := range e.joinChkResourceCh {
		close(e.joinChkResourceCh[i])
	}
	e.joinChkResourceCh = nil
	return e.children[0].Close()
}

func (ow *indexHashJoinOuterWorker) run(ctx context.Context) {
	defer close(ow.innerCh)
	for {
		task, err := ow.buildTask(ctx)
		if task == nil {
			return
		}
		if err != nil {
			task.err = err
			ow.pushToChan(ctx, task, ow.innerCh)
			return
		}
		if finished := ow.pushToChan(ctx, task, ow.innerCh); finished {
			return
		}
	}
}

func (ow *indexHashJoinOuterWorker) buildTask(ctx context.Context) (*indexHashJoinTask, error) {
	task, err := ow.outerWorker.buildTask(ctx)
	if task == nil || err != nil {
		return nil, err
	}
	return &indexHashJoinTask{
		lookUpJoinTask: task,
		outerRowStatus: make([]outerRowStatusFlag, task.outerResult.NumRows()),
	}, nil
}

func (ow *indexHashJoinOuterWorker) pushToChan(ctx context.Context, task *indexHashJoinTask, dst chan<- *indexHashJoinTask) bool {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

func (e *IndexNestedLoopHashJoin) newOuterWorker(innerCh chan *indexHashJoinTask) *indexHashJoinOuterWorker {
	ow := &indexHashJoinOuterWorker{
		outerWorker: outerWorker{
			outerCtx:         e.outerCtx,
			ctx:              e.ctx,
			executor:         e.children[0],
			executorChk:      chunk.NewChunkWithCapacity(e.outerCtx.rowTypes, e.maxChunkSize),
			batchSize:        32,
			maxBatchSize:     e.ctx.GetSessionVars().IndexJoinBatchSize,
			parentMemTracker: e.memTracker,
			lookup:           &e.IndexLookUpJoin,
		},
		innerCh: innerCh,
	}
	return ow
}

func (e *IndexNestedLoopHashJoin) newInnerWorker(taskCh chan *indexHashJoinTask, workerID int) *indexHashJoinInnerWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	iw := &indexHashJoinInnerWorker{
		innerWorker: innerWorker{
			innerCtx:      e.innerCtx,
			outerCtx:      e.outerCtx,
			ctx:           e.ctx,
			executorChk:   chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize),
			indexRanges:   copiedRanges,
			keyOff2IdxOff: e.keyOff2IdxOff,
		},
		taskCh:            taskCh,
		joiner:            e.joiner,
		joinChkResourceCh: e.joinChkResourceCh[workerID],
		resultCh:          e.resultCh,
		matchedOuterPtrs:  make([][]byte, 0, 8),
	}
	iw.finished.Store(false)
	return iw
}

func (iw *indexHashJoinInnerWorker) run(ctx context.Context) {
	var task *indexHashJoinTask
	joinResult, ok := iw.getNewJoinResult(ctx)
	if !ok {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok = <-iw.taskCh:
		}
		if !ok {
			break
		}
		if task.err != nil {
			joinResult.err = task.err
			break
		}
		err := iw.handleTask(ctx, task, joinResult)
		if err != nil {
			joinResult.err = err
			break
		}
	}
	if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		select {
		case iw.resultCh <- joinResult:
		case <-ctx.Done():
			return
		}
	}
}

func (iw *indexHashJoinInnerWorker) getNewJoinResult(ctx context.Context) (*indexHashJoinResult, bool) {
	joinResult := &indexHashJoinResult{
		src: iw.joinChkResourceCh,
	}
	ok := true
	select {
	case joinResult.chk, ok = <-iw.joinChkResourceCh:
	case <-ctx.Done():
		return nil, false
	}
	return joinResult, ok
}

func (iw *indexHashJoinInnerWorker) buildHashTableForOuterResult(ctx context.Context, task *indexHashJoinTask) {
	var (
		keyBuf, valBuf  = make([]byte, 0, 64), make([]byte, 8)
		rowIdx, numRows = 0, task.outerResult.NumRows()
		err             error
	)
OUTER:
	for ; rowIdx < numRows; rowIdx++ {
		if task.outerMatch != nil && !task.outerMatch[rowIdx] {
			continue
		}
		row := task.outerResult.GetRow(rowIdx)
		keyColIdx := iw.outerCtx.keyCols
		for _, i := range keyColIdx {
			if row.IsNull(i) {
				continue OUTER
			}
		}
		keyBuf, err = codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx, keyBuf[:0], row, iw.outerCtx.rowTypes, keyColIdx)
		if err != nil {
			iw.finished.Store(true)
			result, ok := iw.getNewJoinResult(ctx)
			if !ok {
				return
			}
			result.err = err
			select {
			case iw.resultCh <- result:
			case <-ctx.Done():
			}
		}
		*(*int)(unsafe.Pointer(&valBuf[0])) = rowIdx
		task.lookupMap.Put(keyBuf, valBuf)
	}
	return
}

func (iw *indexHashJoinInnerWorker) fetchInnerResults(ctx context.Context, task *lookUpJoinTask) error {
	lookUpContents, err := iw.constructLookupContent(task)
	if err != nil {
		return err
	}
	lookUpContents = iw.sortAndDedupLookUpContents(lookUpContents)
	if iw.finished.Load().(bool) {
		return nil
	}
	return iw.innerWorker.fetchInnerResults(ctx, task, lookUpContents)
}

func (iw *indexHashJoinInnerWorker) handleHashJoinInnerWorkerPanic(r interface{}) {
	if r != nil {
		iw.resultCh <- &indexHashJoinResult{err: errors.Errorf("%v", r)}
	}
	iw.wg.Done()
}

func (iw *indexHashJoinInnerWorker) handleTask(ctx context.Context, task *indexHashJoinTask, joinResult *indexHashJoinResult) error {
	iw.wg = &sync.WaitGroup{}
	iw.wg.Add(1)
	// TODO(XuHuaiyu): we may always use the smaller side to build the hashtable.
	go util.WithRecovery(func() { iw.buildHashTableForOuterResult(ctx, task) }, iw.handleHashJoinInnerWorkerPanic)
	err := iw.fetchInnerResults(ctx, task.lookUpJoinTask)
	if err != nil {
		return err
	}
	iw.wg.Wait()
	if iw.finished.Load().(bool) {
		return nil
	}
	return iw.doJoin(ctx, task, joinResult)
}

func (iw *indexHashJoinInnerWorker) doJoin(ctx context.Context, task *indexHashJoinTask, joinResult *indexHashJoinResult) error {
	var ok bool
	iter := chunk.NewIterator4List(task.innerResult)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		ok, joinResult = iw.joinMatchedInnerRow2Chunk(ctx, row, task, joinResult)
		if !ok {
			return errors.New("indexHashJoinInnerWorker.handleTask failed")
		}
	}
	for rowIdx, val := range task.outerRowStatus {
		if val == outerRowMatched {
			continue
		}
		iw.joiner.onMissMatch(val == outerRowHasNull, task.outerResult.GetRow(rowIdx), joinResult.chk)
		if joinResult.chk.IsFull() {
			iw.resultCh <- joinResult
			joinResult, ok = iw.getNewJoinResult(ctx)
			if !ok {
				return errors.New("indexHashJoinInnerWorker.handleTask failed")
			}
		}
	}
	return nil
}

func (iw *indexHashJoinInnerWorker) joinMatchedInnerRow2Chunk(ctx context.Context, innerRow chunk.Row, task *indexHashJoinTask,
	joinResult *indexHashJoinResult) (bool, *indexHashJoinResult) {
	var err error
	keyBuf := make([]byte, 0, 64)
	keyBuf, err = codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx, keyBuf, innerRow, iw.rowTypes, iw.keyCols)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	iw.matchedOuterPtrs = task.lookupMap.Get(keyBuf, iw.matchedOuterPtrs[:0])
	if len(iw.matchedOuterPtrs) == 0 {
		return true, joinResult
	}
	innerIter := chunk.NewIterator4Slice([]chunk.Row{innerRow})
	var ok bool
	for _, ptr := range iw.matchedOuterPtrs {
		innerIter.Begin()
		rowIdx := *(*int)(unsafe.Pointer(&ptr[0]))
		outerRow := task.outerResult.GetRow(rowIdx)
		matched, isNull, err := iw.joiner.tryToMatch(outerRow, innerIter, joinResult.chk)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		if matched {
			task.outerRowStatus[rowIdx] = outerRowMatched
		}
		if isNull {
			task.outerRowStatus[rowIdx] = outerRowHasNull
		}
		if joinResult.chk.IsFull() {
			iw.resultCh <- joinResult
			joinResult, ok = iw.getNewJoinResult(ctx)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}
