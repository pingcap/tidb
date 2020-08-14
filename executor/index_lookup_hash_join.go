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
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

// numResChkHold indicates the number of resource chunks that an inner worker
// holds at the same time.
// It's used in 2 cases individually:
// 1. IndexMergeJoin
// 2. IndexNestedLoopHashJoin:
//    It's used when IndexNestedLoopHashJoin.keepOuterOrder is true.
//    Otherwise, there will be at most `concurrency` resource chunks throughout
//    the execution of IndexNestedLoopHashJoin.
const numResChkHold = 4

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
	joiners        []joiner
	keepOuterOrder bool
	curTask        *indexHashJoinTask
	// taskCh is only used when `keepOuterOrder` is true.
	taskCh chan *indexHashJoinTask
}

type indexHashJoinOuterWorker struct {
	outerWorker
	innerCh        chan *indexHashJoinTask
	keepOuterOrder bool
	// taskCh is only used when the outer order needs to be promised.
	taskCh chan *indexHashJoinTask
}

type indexHashJoinInnerWorker struct {
	innerWorker
	matchedOuterPtrs  []chunk.RowPtr
	joiner            joiner
	joinChkResourceCh chan *chunk.Chunk
	// resultCh is valid only when indexNestedLoopHashJoin do not need to keep
	// order. Otherwise, it will be nil.
	resultCh       chan *indexHashJoinResult
	taskCh         <-chan *indexHashJoinTask
	wg             *sync.WaitGroup
	joinKeyBuf     []byte
	outerRowStatus []outerRowStatusFlag
}

type indexHashJoinResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

type indexHashJoinTask struct {
	*lookUpJoinTask
	outerRowStatus [][]outerRowStatusFlag
	lookupMap      *rowHashMap
	err            error
	keepOuterOrder bool
	// resultCh is only used when the outer order needs to be promised.
	resultCh chan *indexHashJoinResult
	// matchedInnerRowPtrs is only valid when the outer order needs to be
	// promised. Otherwise, it will be nil.
	// len(matchedInnerRowPtrs) equals to
	// lookUpJoinTask.outerResult.NumChunks(), and the elements of every
	// matchedInnerRowPtrs[chkIdx][rowIdx] indicates the matched inner row ptrs
	// of the corresponding outer row.
	matchedInnerRowPtrs [][][]chunk.RowPtr
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
	// The trick here is `getSnapshotTS` will cache snapshot ts in the dataReaderBuilder,
	// so even txn is destroyed later, the dataReaderBuilder could still use the
	// cached snapshot ts to construct DAG.
	_, err := e.innerCtx.readerBuilder.getSnapshotTS()
	if err != nil {
		return err
	}

	err = e.children[0].Open(ctx)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.innerPtrBytes = make([][]byte, 0, 8)
	e.startWorkers(ctx)
	return nil
}

func (e *IndexNestedLoopHashJoin) startWorkers(ctx context.Context) {
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency()
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *indexHashJoinTask, concurrency)
	if e.keepOuterOrder {
		e.taskCh = make(chan *indexHashJoinTask, concurrency)
	}
	e.workerWg.Add(1)
	ow := e.newOuterWorker(innerCh)
	go util.WithRecovery(func() { ow.run(workerCtx, cancelFunc) }, e.finishJoinWorkers)

	if !e.keepOuterOrder {
		e.resultCh = make(chan *indexHashJoinResult, concurrency)
	} else {
		// When `keepOuterOrder` is true, each task holds their own `resultCh`
		// individually, thus we do not need a global resultCh.
		e.resultCh = nil
	}
	e.joinChkResourceCh = make([]chan *chunk.Chunk, concurrency)
	for i := 0; i < concurrency; i++ {
		if !e.keepOuterOrder {
			e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
			e.joinChkResourceCh[i] <- newFirstChunk(e)
		} else {
			e.joinChkResourceCh[i] = make(chan *chunk.Chunk, numResChkHold)
			for j := 0; j < numResChkHold; j++ {
				e.joinChkResourceCh[i] <- newFirstChunk(e)
			}
		}
	}

	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		workerID := i
		go util.WithRecovery(func() { e.newInnerWorker(innerCh, workerID).run(workerCtx, cancelFunc) }, e.finishJoinWorkers)
	}
	go e.wait4JoinWorkers()
}

func (e *IndexNestedLoopHashJoin) finishJoinWorkers(r interface{}) {
	if r != nil {
		logutil.BgLogger().Error("IndexNestedLoopHashJoin failed", zap.Error(errors.Errorf("%v", r)))
		if e.cancelFunc != nil {
			e.cancelFunc()
		}
	}
	e.workerWg.Done()
}

func (e *IndexNestedLoopHashJoin) wait4JoinWorkers() {
	e.workerWg.Wait()
	if e.resultCh != nil {
		close(e.resultCh)
	}
	if e.taskCh != nil {
		close(e.taskCh)
	}
}

// Next implements the IndexNestedLoopHashJoin Executor interface.
func (e *IndexNestedLoopHashJoin) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.keepOuterOrder {
		return e.runInOrder(ctx, req)
	}
	// unordered run
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

func (e *IndexNestedLoopHashJoin) runInOrder(ctx context.Context, req *chunk.Chunk) error {
	var (
		result *indexHashJoinResult
		ok     bool
	)
	for {
		if e.isDryUpTasks(ctx) {
			return nil
		}
		select {
		case result, ok = <-e.curTask.resultCh:
			if !ok {
				e.curTask = nil
				continue
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
}

// isDryUpTasks indicates whether all the tasks have been processed.
func (e *IndexNestedLoopHashJoin) isDryUpTasks(ctx context.Context) bool {
	if e.curTask != nil {
		return false
	}
	var ok bool
	select {
	case e.curTask, ok = <-e.taskCh:
		if !ok {
			return true
		}
	case <-ctx.Done():
		return true
	}
	return false
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
	if e.taskCh != nil {
		for range e.taskCh {
		}
		e.taskCh = nil
	}
	if e.runtimeStats != nil {
		concurrency := cap(e.joinChkResourceCh)
		e.runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", concurrency))
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
		if ow.keepOuterOrder {
			if finished := ow.pushToChan(ctx, task, ow.taskCh); finished {
				return
			}
		}
	}
}

func (ow *indexHashJoinOuterWorker) buildTask(ctx context.Context) (*indexHashJoinTask, error) {
	task, err := ow.outerWorker.buildTask(ctx)
	if task == nil || err != nil {
		return nil, err
	}
	var (
		resultCh            chan *indexHashJoinResult
		matchedInnerRowPtrs [][][]chunk.RowPtr
	)
	if ow.keepOuterOrder {
		resultCh = make(chan *indexHashJoinResult, numResChkHold)
		matchedInnerRowPtrs = make([][][]chunk.RowPtr, task.outerResult.NumChunks())
		for i := range matchedInnerRowPtrs {
			matchedInnerRowPtrs[i] = make([][]chunk.RowPtr, task.outerResult.GetChunk(i).NumRows())
		}
	}
	numChks := task.outerResult.NumChunks()
	outerRowStatus := make([][]outerRowStatusFlag, numChks)
	for i := 0; i < numChks; i++ {
		outerRowStatus[i] = make([]outerRowStatusFlag, task.outerResult.GetChunk(i).NumRows())
	}
	return &indexHashJoinTask{
		lookUpJoinTask:      task,
		outerRowStatus:      outerRowStatus,
		keepOuterOrder:      ow.keepOuterOrder,
		resultCh:            resultCh,
		matchedInnerRowPtrs: matchedInnerRowPtrs,
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
			batchSize:        32,
			maxBatchSize:     e.ctx.GetSessionVars().IndexJoinBatchSize,
			parentMemTracker: e.memTracker,
			lookup:           &e.IndexLookUpJoin,
		},
		innerCh:        innerCh,
		keepOuterOrder: e.keepOuterOrder,
		taskCh:         e.taskCh,
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
	if e.lastColHelper != nil {
		// nextCwf.TmpConstant needs to be reset for every individual
		// inner worker to avoid data race when the inner workers is running
		// concurrently.
		nextCwf := *e.lastColHelper
		nextCwf.TmpConstant = make([]*expression.Constant, len(e.lastColHelper.TmpConstant))
		for i := range e.lastColHelper.TmpConstant {
			nextCwf.TmpConstant[i] = &expression.Constant{RetType: nextCwf.TargetCol.RetType}
		}
		iw.nextColCompareFilters = &nextCwf
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
	h, resultCh := fnv.New64(), iw.resultCh
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
		if task.keepOuterOrder {
			resultCh = task.resultCh
		}
		err := iw.handleTask(ctx, cancelFunc, task, joinResult, h, resultCh)
		if err != nil {
			joinResult.err = err
			break
		}
		if task.keepOuterOrder {
			// We need to get a new result holder here because the old
			// `joinResult` hash been sent to the `resultCh` or to the
			// `joinChkResourceCh`.
			joinResult, ok = iw.getNewJoinResult(ctx)
			if !ok {
				cancelFunc()
				return
			}
		}
	}
	if joinResult.err != nil {
		cancelFunc()
		logutil.Logger(ctx).Error("indexHashJoinInnerWorker.run failed", zap.Error(joinResult.err))
		return
	}
	// When task.keepOuterOrder is TRUE(resultCh != iw.resultCh), the last
	// joinResult will be checked when the a task has been processed, thus we do
	// not need to check it here again.
	if resultCh == iw.resultCh && joinResult.chk != nil && joinResult.chk.NumRows() > 0 {
		select {
		case resultCh <- joinResult:
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
	buf, numChks := make([]byte, 1), task.outerResult.NumChunks()
	task.lookupMap = newRowHashMap(task.outerResult.Len())
	for chkIdx := 0; chkIdx < numChks; chkIdx++ {
		chk := task.outerResult.GetChunk(chkIdx)
		numRows := chk.NumRows()
	OUTER:
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if task.outerMatch != nil && !task.outerMatch[chkIdx][rowIdx] {
				continue
			}
			row := chk.GetRow(rowIdx)
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
			rowPtr := chunk.RowPtr{ChkIdx: uint32(chkIdx), RowIdx: uint32(rowIdx)}
			task.lookupMap.Put(h.Sum64(), rowPtr)
		}
	}
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

func (iw *indexHashJoinInnerWorker) handleTask(ctx context.Context, cancelFunc context.CancelFunc, task *indexHashJoinTask, joinResult *indexHashJoinResult, h hash.Hash64, resultCh chan *indexHashJoinResult) error {
	iw.wg = &sync.WaitGroup{}
	iw.wg.Add(1)
	// TODO(XuHuaiyu): we may always use the smaller side to build the hashtable.
	go util.WithRecovery(func() { iw.buildHashTableForOuterResult(ctx, cancelFunc, task, h) }, iw.handleHashJoinInnerWorkerPanic)
	err := iw.fetchInnerResults(ctx, task.lookUpJoinTask)
	if err != nil {
		return err
	}
	iw.wg.Wait()
	if !task.keepOuterOrder {
		return iw.doJoinUnordered(ctx, task, joinResult, h, resultCh)
	}
	return iw.doJoinInOrder(ctx, task, joinResult, h, resultCh)
}

func (iw *indexHashJoinInnerWorker) doJoinUnordered(ctx context.Context, task *indexHashJoinTask, joinResult *indexHashJoinResult, h hash.Hash64, resultCh chan *indexHashJoinResult) error {
	var ok bool
	iter := chunk.NewIterator4List(task.innerResult)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		ok, joinResult = iw.joinMatchedInnerRow2Chunk(ctx, row, task, joinResult, h, iw.joinKeyBuf)
		if !ok {
			return errors.New("indexHashJoinInnerWorker.doJoinUnordered failed")
		}
	}
	for chkIdx, outerRowStatus := range task.outerRowStatus {
		chk := task.outerResult.GetChunk(chkIdx)
		for rowIdx, val := range outerRowStatus {
			if val == outerRowMatched {
				continue
			}
			iw.joiner.onMissMatch(val == outerRowHasNull, chk.GetRow(rowIdx), joinResult.chk)
			if joinResult.chk.IsFull() {
				select {
				case resultCh <- joinResult:
				case <-ctx.Done():
				}
				joinResult, ok = iw.getNewJoinResult(ctx)
				if !ok {
					return errors.New("indexHashJoinInnerWorker.doJoinUnordered failed")
				}
			}
		}
	}
	return nil
}

func (iw *indexHashJoinInnerWorker) getMatchedOuterRows(innerRow chunk.Row, task *indexHashJoinTask, h hash.Hash64, buf []byte) (matchedRows []chunk.Row, matchedRowPtr []chunk.RowPtr, err error) {
	h.Reset()
	err = codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx, h, innerRow, iw.rowTypes, iw.keyCols, buf)
	if err != nil {
		return nil, nil, err
	}
	iw.matchedOuterPtrs = task.lookupMap.Get(h.Sum64())
	if len(iw.matchedOuterPtrs) == 0 {
		return nil, nil, nil
	}
	joinType := JoinerType(iw.joiner)
	isSemiJoin := joinType == plannercore.SemiJoin || joinType == plannercore.LeftOuterSemiJoin
	matchedRows = make([]chunk.Row, 0, len(iw.matchedOuterPtrs))
	matchedRowPtr = make([]chunk.RowPtr, 0, len(iw.matchedOuterPtrs))
	for _, ptr := range iw.matchedOuterPtrs {
		outerRow := task.outerResult.GetRow(ptr)
		ok, err := codec.EqualChunkRow(iw.ctx.GetSessionVars().StmtCtx, innerRow, iw.rowTypes, iw.keyCols, outerRow, iw.outerCtx.rowTypes, iw.outerCtx.keyCols)
		if err != nil {
			return nil, nil, err
		}
		if !ok || (task.outerRowStatus[ptr.ChkIdx][ptr.RowIdx] == outerRowMatched && isSemiJoin) {
			continue
		}
		matchedRows = append(matchedRows, outerRow)
		matchedRowPtr = append(matchedRowPtr, chunk.RowPtr{ChkIdx: ptr.ChkIdx, RowIdx: ptr.RowIdx})
	}
	return matchedRows, matchedRowPtr, nil
}

func (iw *indexHashJoinInnerWorker) joinMatchedInnerRow2Chunk(ctx context.Context, innerRow chunk.Row, task *indexHashJoinTask,
	joinResult *indexHashJoinResult, h hash.Hash64, buf []byte) (bool, *indexHashJoinResult) {
	matchedOuterRows, matchedOuterRowPtr, err := iw.getMatchedOuterRows(innerRow, task, h, buf)
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
			chkIdx, rowIdx := matchedOuterRowPtr[cursor].ChkIdx, matchedOuterRowPtr[cursor].RowIdx
			if status == outerRowMatched || task.outerRowStatus[chkIdx][rowIdx] == outerRowUnmatched {
				task.outerRowStatus[chkIdx][rowIdx] = status
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

func (iw *indexHashJoinInnerWorker) collectMatchedInnerPtrs4OuterRows(ctx context.Context, innerRow chunk.Row, innerRowPtr chunk.RowPtr,
	task *indexHashJoinTask, h hash.Hash64, buf []byte) error {
	_, matchedOuterRowIdx, err := iw.getMatchedOuterRows(innerRow, task, h, buf)
	if err != nil {
		return err
	}
	for _, outerRowPtr := range matchedOuterRowIdx {
		chkIdx, rowIdx := outerRowPtr.ChkIdx, outerRowPtr.RowIdx
		task.matchedInnerRowPtrs[chkIdx][rowIdx] = append(task.matchedInnerRowPtrs[chkIdx][rowIdx], innerRowPtr)
	}
	return nil
}

// doJoinInOrder follows the following steps:
// 1. collect all the matched inner row ptrs for every outer row
// 2. do the join work
//   2.1 collect all the matched inner rows using the collected ptrs for every outer row
//   2.2 call tryToMatchInners for every outer row
//   2.3 call onMissMatch when no inner rows are matched
func (iw *indexHashJoinInnerWorker) doJoinInOrder(ctx context.Context, task *indexHashJoinTask, joinResult *indexHashJoinResult, h hash.Hash64, resultCh chan *indexHashJoinResult) (err error) {
	defer func() {
		if err == nil && joinResult.chk != nil {
			if joinResult.chk.NumRows() > 0 {
				select {
				case resultCh <- joinResult:
				case <-ctx.Done():
					return
				}
			} else {
				joinResult.src <- joinResult.chk
			}
		}
		close(resultCh)
	}()
	for i, numChunks := 0, task.innerResult.NumChunks(); i < numChunks; i++ {
		for j, chk := 0, task.innerResult.GetChunk(i); j < chk.NumRows(); j++ {
			row := chk.GetRow(j)
			ptr := chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)}
			err = iw.collectMatchedInnerPtrs4OuterRows(ctx, row, ptr, task, h, iw.joinKeyBuf)
			if err != nil {
				return err
			}
		}
	}
	// TODO: matchedInnerRowPtrs and matchedInnerRows can be moved to inner worker.
	matchedInnerRows := make([]chunk.Row, len(task.matchedInnerRowPtrs))
	var hasMatched, hasNull, ok bool
	for chkIdx, innerRowPtrs4Chk := range task.matchedInnerRowPtrs {
		for outerRowIdx, innerRowPtrs := range innerRowPtrs4Chk {
			matchedInnerRows, hasMatched, hasNull = matchedInnerRows[:0], false, false
			outerRow := task.outerResult.GetChunk(chkIdx).GetRow(outerRowIdx)
			for _, ptr := range innerRowPtrs {
				matchedInnerRows = append(matchedInnerRows, task.innerResult.GetRow(ptr))
			}
			iter := chunk.NewIterator4Slice(matchedInnerRows)
			for iter.Begin(); iter.Current() != iter.End(); {
				matched, isNull, err := iw.joiner.tryToMatchInners(outerRow, iter, joinResult.chk)
				if err != nil {
					return err
				}
				hasMatched, hasNull = matched || hasMatched, isNull || hasNull
				if joinResult.chk.IsFull() {
					select {
					case resultCh <- joinResult:
					case <-ctx.Done():
						return nil
					}
					joinResult, ok = iw.getNewJoinResult(ctx)
					if !ok {
						return errors.New("indexHashJoinInnerWorker.doJoinInOrder failed")
					}
				}
			}
			if !hasMatched {
				iw.joiner.onMissMatch(hasNull, outerRow, joinResult.chk)
			}
		}
	}
	return nil
}
