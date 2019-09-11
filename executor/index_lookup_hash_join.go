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
	"hash"
	"hash/fnv"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
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

	// We build individual joiner for each inner worker when using chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiners []joiner
}

type indexHashJoinOuterWorker struct {
	outerWorker
	innerCh chan *indexHashJoinTask
}

type indexHashJoinInnerWorker struct {
	innerWorker
	matchedOuterPtrs  []chunk.RowPtr
	joiner            joiner
	joinChkResourceCh chan *chunk.Chunk
	resultCh          chan *indexHashJoinResult
	taskCh            <-chan *indexHashJoinTask
	wg                *sync.WaitGroup
	joinKeyBuf        []byte
	outerRowStatus    []outerRowStatusFlag
}

type indexHashJoinResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

type indexHashJoinTask struct {
	*lookUpJoinTask
	outerRowStatus []outerRowStatusFlag
	lookupMap      *rowHashMap
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
	go util.WithRecovery(func() { ow.run(workerCtx, cancelFunc) }, e.finishJoinWorkers)

	e.resultCh = make(chan *indexHashJoinResult, concurrency)
	e.joinChkResourceCh = make([]chan *chunk.Chunk, concurrency)
	for i := int(0); i < concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}
	e.workerWg.Add(concurrency)
	for i := int(0); i < concurrency; i++ {
		workerID := i
		go util.WithRecovery(func() { e.newInnerWorker(innerCh, workerID).run(workerCtx, cancelFunc) }, e.finishJoinWorkers)
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
	var (
		result *indexHashJoinResult
		ok     bool
	)
	select {
	case result, ok = <-e.resultCh:
		if !ok {
			return nil
		}
		if result.err != nil {
			return result.err
		}
	case <-ctx.Done():
		return nil
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
	return e.baseExecutor.Close()
}

func (ow *indexHashJoinOuterWorker) run(ctx context.Context, cancelFunc context.CancelFunc) {
	defer close(ow.innerCh)
	for {
		task, err := ow.buildTask(ctx)
		if task == nil {
			return
		}
		if err != nil {
			cancelFunc()
			logutil.Logger(ctx).Error("indexHashJoinOuterWorker.run failed", zap.Error(err))
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
		joiner:            e.joiners[workerID],
		joinChkResourceCh: e.joinChkResourceCh[workerID],
		resultCh:          e.resultCh,
		matchedOuterPtrs:  make([]chunk.RowPtr, 0, e.maxChunkSize),
		joinKeyBuf:        make([]byte, 1),
		outerRowStatus:    make([]outerRowStatusFlag, 0, e.maxChunkSize),
	}
	return iw
}

func (iw *indexHashJoinInnerWorker) run(ctx context.Context, cancelFunc context.CancelFunc) {
	var task *indexHashJoinTask
	joinResult, ok := iw.getNewJoinResult(ctx)
	if !ok {
		cancelFunc()
		return
	}
	h := fnv.New64()
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
		err := iw.handleTask(ctx, cancelFunc, task, joinResult, h)
		if err != nil {
			joinResult.err = err
			break
		}
	}
	if joinResult.err != nil {
		cancelFunc()
		logutil.Logger(ctx).Error("indexHashJoinInnerWorker.run failed", zap.Error(joinResult.err))
		return
	}
	if joinResult.chk != nil && joinResult.chk.NumRows() > 0 {
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

func (iw *indexHashJoinInnerWorker) buildHashTableForOuterResult(ctx context.Context, cancelFunc context.CancelFunc, task *indexHashJoinTask, h hash.Hash64) {
	rowIdx, numRows := 0, task.outerResult.NumRows()
	buf := make([]byte, 1)
	task.lookupMap = newRowHashMap(numRows)
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
		h.Reset()
		err := codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx, h, row, iw.outerCtx.rowTypes, keyColIdx, buf)
		if err != nil {
			cancelFunc()
			logutil.Logger(ctx).Error("indexHashJoinInnerWorker.buildHashTableForOuterResult failed", zap.Error(err))
			return
		}
		rowPtr := chunk.RowPtr{ChkIdx: 0, RowIdx: uint32(rowIdx)}
		task.lookupMap.Put(h.Sum64(), rowPtr)
	}
	return
}

func (iw *indexHashJoinInnerWorker) fetchInnerResults(ctx context.Context, task *lookUpJoinTask) error {
	lookUpContents, err := iw.constructLookupContent(task)
	if err != nil {
		return err
	}
	lookUpContents = iw.sortAndDedupLookUpContents(lookUpContents)
	return iw.innerWorker.fetchInnerResults(ctx, task, lookUpContents)
}

func (iw *indexHashJoinInnerWorker) handleHashJoinInnerWorkerPanic(r interface{}) {
	if r != nil {
		iw.resultCh <- &indexHashJoinResult{err: errors.Errorf("%v", r)}
	}
	iw.wg.Done()
}

func (iw *indexHashJoinInnerWorker) handleTask(ctx context.Context, cancelFunc context.CancelFunc, task *indexHashJoinTask, joinResult *indexHashJoinResult, h hash.Hash64) error {
	iw.wg = &sync.WaitGroup{}
	iw.wg.Add(1)
	// TODO(XuHuaiyu): we may always use the smaller side to build the hashtable.
	go util.WithRecovery(func() { iw.buildHashTableForOuterResult(ctx, cancelFunc, task, h) }, iw.handleHashJoinInnerWorkerPanic)
	err := iw.fetchInnerResults(ctx, task.lookUpJoinTask)
	if err != nil {
		return err
	}
	iw.wg.Wait()
	return iw.doJoin(ctx, task, joinResult, h)
}

func (iw *indexHashJoinInnerWorker) doJoin(ctx context.Context, task *indexHashJoinTask, joinResult *indexHashJoinResult, h hash.Hash64) error {
	var ok bool
	iter := chunk.NewIterator4List(task.innerResult)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		ok, joinResult = iw.joinMatchedInnerRow2Chunk(ctx, row, task, joinResult, h, iw.joinKeyBuf)
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
			select {
			case iw.resultCh <- joinResult:
			case <-ctx.Done():
			}
			joinResult, ok = iw.getNewJoinResult(ctx)
			if !ok {
				return errors.New("indexHashJoinInnerWorker.handleTask failed")
			}
		}
	}
	return nil
}

func (iw *indexHashJoinInnerWorker) getMatchedOuterRows(innerRow chunk.Row, task *indexHashJoinTask, h hash.Hash64, buf []byte) (matchedRows []chunk.Row, matchedRowIdx []int, err error) {
	h.Reset()
	err = codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx, h, innerRow, iw.rowTypes, iw.keyCols, buf)
	if err != nil {
		return nil, nil, err
	}
	iw.matchedOuterPtrs = task.lookupMap.Get(h.Sum64())
	if len(iw.matchedOuterPtrs) == 0 {
		return nil, nil, nil
	}
	matchedRows = make([]chunk.Row, 0, len(iw.matchedOuterPtrs))
	matchedRowIdx = make([]int, 0, len(iw.matchedOuterPtrs))
	for _, ptr := range iw.matchedOuterPtrs {
		rowIdx := int(ptr.RowIdx)
		outerRow := task.outerResult.GetRow(rowIdx)
		ok, err := codec.EqualChunkRow(iw.ctx.GetSessionVars().StmtCtx, innerRow, iw.rowTypes, iw.keyCols, outerRow, iw.outerCtx.rowTypes, iw.outerCtx.keyCols)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			continue
		}
		matchedRows = append(matchedRows, outerRow)
		matchedRowIdx = append(matchedRowIdx, rowIdx)
	}
	return matchedRows, matchedRowIdx, nil
}

func (iw *indexHashJoinInnerWorker) joinMatchedInnerRow2Chunk(ctx context.Context, innerRow chunk.Row, task *indexHashJoinTask,
	joinResult *indexHashJoinResult, h hash.Hash64, buf []byte) (bool, *indexHashJoinResult) {
	matchedOuterRows, matchedOuterRowIdx, err := iw.getMatchedOuterRows(innerRow, task, h, buf)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(matchedOuterRows) == 0 {
		return true, joinResult
	}
	var (
		ok     bool
		iter   = chunk.NewIterator4Slice(matchedOuterRows)
		cursor = 0
	)
	for iter.Begin(); iter.Current() != iter.End(); {
		iw.outerRowStatus, err = iw.joiner.tryToMatchOuters(iter, innerRow, joinResult.chk, iw.outerRowStatus)
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		for _, status := range iw.outerRowStatus {
			outerRowIdx := matchedOuterRowIdx[cursor]
			if status == outerRowMatched || task.outerRowStatus[outerRowIdx] == outerRowUnmatched {
				task.outerRowStatus[outerRowIdx] = status
			}
			cursor++
		}
		if joinResult.chk.IsFull() {
			select {
			case iw.resultCh <- joinResult:
			case <-ctx.Done():
			}
			joinResult, ok = iw.getNewJoinResult(ctx)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}
