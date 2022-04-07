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
	"context"
	"fmt"
	"hash"
	"hash/fnv"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
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

	stats    *indexLookUpJoinRuntimeStats
	prepared bool
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
	lookupMap      baseHashTable
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
	err := e.children[0].Open(ctx)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.cancelFunc = nil
	e.innerPtrBytes = make([][]byte, 0, 8)
	if e.runtimeStats != nil {
		e.stats = &indexLookUpJoinRuntimeStats{}
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
	}
	e.finished.Store(false)
	return nil
}

func (e *IndexNestedLoopHashJoin) startWorkers(ctx context.Context) {
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency()
	if e.stats != nil {
		e.stats.concurrency = concurrency
	}
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *indexHashJoinTask, concurrency)
	if e.keepOuterOrder {
		e.taskCh = make(chan *indexHashJoinTask, concurrency)
		// When `keepOuterOrder` is true, each task holds their own `resultCh`
		// individually, thus we do not need a global resultCh.
		e.resultCh = nil
	} else {
		e.resultCh = make(chan *indexHashJoinResult, concurrency)
	}
	e.joinChkResourceCh = make([]chan *chunk.Chunk, concurrency)
	e.workerWg.Add(1)
	ow := e.newOuterWorker(innerCh)
	go util.WithRecovery(func() { ow.run(workerCtx) }, e.finishJoinWorkers)

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
		e.IndexLookUpJoin.finished.Store(true)
		err := errors.New(fmt.Sprintf("%v", r))
		if !e.keepOuterOrder {
			e.resultCh <- &indexHashJoinResult{err: err}
		} else {
			task := &indexHashJoinTask{err: err}
			e.taskCh <- task
		}
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
	if !e.prepared {
		e.startWorkers(ctx)
		e.prepared = true
	}
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
		return ctx.Err()
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
		if e.curTask.err != nil {
			return e.curTask.err
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
			return ctx.Err()
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
	for i := range e.joinChkResourceCh {
		close(e.joinChkResourceCh[i])
	}
	e.joinChkResourceCh = nil
	e.finished.Store(false)
	e.prepared = false
	return e.baseExecutor.Close()
}

func (ow *indexHashJoinOuterWorker) run(ctx context.Context) {
	defer trace.StartRegion(ctx, "IndexHashJoinOuterWorker").End()
	defer close(ow.innerCh)
	for {
		failpoint.Inject("TestIssue30211", nil)
		task, err := ow.buildTask(ctx)
		failpoint.Inject("testIndexHashJoinOuterWorkerErr", func() {
			err = errors.New("mockIndexHashJoinOuterWorkerErr")
		})
		if err != nil {
			task = &indexHashJoinTask{err: err}
			if ow.keepOuterOrder {
				// The outerBuilder and innerFetcher run concurrently, we may
				// get 2 errors at simultaneously. Thus the capacity of task.resultCh
				// needs to be initialized to 2 to avoid waiting.
				task.keepOuterOrder, task.resultCh = true, make(chan *indexHashJoinResult, 2)
				ow.pushToChan(ctx, task, ow.taskCh)
			}
			ow.pushToChan(ctx, task, ow.innerCh)
			return
		}
		if task == nil {
			return
		}
		if finished := ow.pushToChan(ctx, task, ow.innerCh); finished {
			return
		}
		if ow.keepOuterOrder {
			failpoint.Inject("testIssue20779", func() {
				panic("testIssue20779")
			})
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
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges.Range()))
	for _, ran := range e.indexRanges.Range() {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	var innerStats *innerWorkerRuntimeStats
	if e.stats != nil {
		innerStats = &e.stats.innerWorker
	}
	iw := &indexHashJoinInnerWorker{
		innerWorker: innerWorker{
			innerCtx:      e.innerCtx,
			outerCtx:      e.outerCtx,
			ctx:           e.ctx,
			executorChk:   chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize),
			indexRanges:   copiedRanges,
			keyOff2IdxOff: e.keyOff2IdxOff,
			stats:         innerStats,
			lookup:        &e.IndexLookUpJoin,
			memTracker:    memory.NewTracker(memory.LabelForIndexJoinInnerWorker, -1),
		},
		taskCh:            taskCh,
		joiner:            e.joiners[workerID],
		joinChkResourceCh: e.joinChkResourceCh[workerID],
		resultCh:          e.resultCh,
		matchedOuterPtrs:  make([]chunk.RowPtr, 0, e.maxChunkSize),
		joinKeyBuf:        make([]byte, 1),
		outerRowStatus:    make([]outerRowStatusFlag, 0, e.maxChunkSize),
	}
	iw.memTracker.AttachTo(e.memTracker)
	if len(copiedRanges) != 0 {
		// We should not consume this memory usage in `iw.memTracker`. The
		// memory usage of inner worker will be reset the end of iw.handleTask.
		// While the life cycle of this memory consumption exists throughout the
		// whole active period of inner worker.
		e.ctx.GetSessionVars().StmtCtx.MemTracker.Consume(2 * types.EstimatedMemUsage(copiedRanges[0].LowVal, len(copiedRanges)))
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
	defer trace.StartRegion(ctx, "IndexHashJoinInnerWorker").End()
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
		// We need to init resultCh before the err is returned.
		if task.keepOuterOrder {
			resultCh = task.resultCh
		}
		if task.err != nil {
			joinResult.err = task.err
			break
		}
		err := iw.handleTask(ctx, task, joinResult, h, resultCh)
		if err != nil && !task.keepOuterOrder {
			// Only need check non-keep-outer-order case because the
			// `joinResult` had been sent to the `resultCh` when err != nil.
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
	failpoint.Inject("testIndexHashJoinInnerWorkerErr", func() {
		joinResult.err = errors.New("mockIndexHashJoinInnerWorkerErr")
	})
	// When task.keepOuterOrder is TRUE (resultCh != iw.resultCh):
	//   - the last joinResult will be handled when the task has been processed,
	//     thus we DO NOT need to check it here again.
	//   - we DO NOT check the error here neither, because:
	//     - if the error is from task.err, the main thread will check the error of each task
	//     - if the error is from handleTask, the error will be handled in handleTask
	// We should not check `task != nil && !task.keepOuterOrder` here since it's
	// possible that `join.chk.NumRows > 0` is true even if task == nil.
	if resultCh == iw.resultCh {
		if joinResult.err != nil {
			resultCh <- joinResult
			return
		}
		if joinResult.chk != nil && joinResult.chk.NumRows() > 0 {
			select {
			case resultCh <- joinResult:
			case <-ctx.Done():
				return
			}
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
		return joinResult, false
	}
	return joinResult, ok
}

func (iw *indexHashJoinInnerWorker) buildHashTableForOuterResult(ctx context.Context, task *indexHashJoinTask, h hash.Hash64) {
	failpoint.Inject("IndexHashJoinBuildHashTablePanic", nil)
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.build, int64(time.Since(start)))
		}()
	}
	buf, numChks := make([]byte, 1), task.outerResult.NumChunks()
	task.lookupMap = newUnsafeHashTable(task.outerResult.Len())
	for chkIdx := 0; chkIdx < numChks; chkIdx++ {
		chk := task.outerResult.GetChunk(chkIdx)
		numRows := chk.NumRows()
		if iw.lookup.finished.Load().(bool) {
			return
		}
	OUTER:
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			if task.outerMatch != nil && !task.outerMatch[chkIdx][rowIdx] {
				continue
			}
			row := chk.GetRow(rowIdx)
			hashColIdx := iw.outerCtx.hashCols
			for _, i := range hashColIdx {
				if row.IsNull(i) {
					continue OUTER
				}
			}
			h.Reset()
			err := codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx, h, row, iw.outerCtx.hashTypes, hashColIdx, buf)
			failpoint.Inject("testIndexHashJoinBuildErr", func() {
				err = errors.New("mockIndexHashJoinBuildErr")
			})
			if err != nil {
				// This panic will be recovered by the invoker.
				panic(err.Error())
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
	return iw.innerWorker.fetchInnerResults(ctx, task, lookUpContents)
}

func (iw *indexHashJoinInnerWorker) handleHashJoinInnerWorkerPanic(resultCh chan *indexHashJoinResult, err error) {
	defer func() {
		iw.wg.Done()
		iw.lookup.workerWg.Done()
	}()
	if err != nil {
		resultCh <- &indexHashJoinResult{err: err}
	}
}

func (iw *indexHashJoinInnerWorker) handleTask(ctx context.Context, task *indexHashJoinTask, joinResult *indexHashJoinResult, h hash.Hash64, resultCh chan *indexHashJoinResult) (err error) {
	defer func() {
		iw.memTracker.Consume(-iw.memTracker.BytesConsumed())
		if task.keepOuterOrder {
			if err != nil {
				joinResult.err = err
				resultCh <- joinResult
			}
			close(resultCh)
		}
	}()
	var joinStartTime time.Time
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			endTime := time.Now()
			atomic.AddInt64(&iw.stats.totalTime, int64(endTime.Sub(start)))
			atomic.AddInt64(&iw.stats.join, int64(endTime.Sub(joinStartTime)))
		}()
	}

	iw.wg = &sync.WaitGroup{}
	iw.wg.Add(1)
	// TODO(XuHuaiyu): we may always use the smaller side to build the hashtable.
	go util.WithRecovery(
		func() {
			iw.lookup.workerWg.Add(1)
			iw.buildHashTableForOuterResult(ctx, task, h)
		},
		func(r interface{}) {
			var err error
			if r != nil {
				err = errors.Errorf("%v", r)
			}
			iw.handleHashJoinInnerWorkerPanic(resultCh, err)
		},
	)
	err = iw.fetchInnerResults(ctx, task.lookUpJoinTask)
	iw.wg.Wait()
	// check error after wg.Wait to make sure error message can be sent to
	// resultCh even if panic happen in buildHashTableForOuterResult.
	failpoint.Inject("IndexHashJoinFetchInnerResultsErr", func() {
		err = errors.New("IndexHashJoinFetchInnerResultsErr")
	})
	if err != nil {
		return err
	}

	joinStartTime = time.Now()
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
			return joinResult.err
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
					return ctx.Err()
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
	err = codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx, h, innerRow, iw.hashTypes, iw.hashCols, buf)
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
		ok, err := codec.EqualChunkRow(iw.ctx.GetSessionVars().StmtCtx, innerRow, iw.hashTypes, iw.hashCols, outerRow, iw.outerCtx.hashTypes, iw.outerCtx.hashCols)
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
	}()
	for i, numChunks := 0, task.innerResult.NumChunks(); i < numChunks; i++ {
		for j, chk := 0, task.innerResult.GetChunk(i); j < chk.NumRows(); j++ {
			row := chk.GetRow(j)
			ptr := chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)}
			err = iw.collectMatchedInnerPtrs4OuterRows(ctx, row, ptr, task, h, iw.joinKeyBuf)
			failpoint.Inject("TestIssue31129", func() {
				err = errors.New("TestIssue31129")
			})
			if err != nil {
				return err
			}
		}
	}
	// TODO: matchedInnerRowPtrs and matchedInnerRows can be moved to inner worker.
	matchedInnerRows := make([]chunk.Row, 0, len(task.matchedInnerRowPtrs))
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
						return ctx.Err()
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
