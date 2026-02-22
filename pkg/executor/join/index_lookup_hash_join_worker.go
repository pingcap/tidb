// Copyright 2020 PingCAP, Inc.
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

package join

import (
	"context"
	"hash"
	"hash/fnv"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func (e *IndexNestedLoopHashJoin) newOuterWorker(innerCh chan *indexHashJoinTask, initBatchSize int) *indexHashJoinOuterWorker {
	maxBatchSize := e.Ctx().GetSessionVars().IndexJoinBatchSize
	batchSize := min(initBatchSize, maxBatchSize)
	ow := &indexHashJoinOuterWorker{
		outerWorker: outerWorker{
			OuterCtx:         e.OuterCtx,
			ctx:              e.Ctx(),
			executor:         e.Children(0),
			batchSize:        batchSize,
			maxBatchSize:     maxBatchSize,
			parentMemTracker: e.memTracker,
			lookup:           &e.IndexLookUpJoin,
		},
		innerCh:        innerCh,
		keepOuterOrder: e.KeepOuterOrder,
		taskCh:         e.taskCh,
	}
	return ow
}

func (e *IndexNestedLoopHashJoin) supportIncrementalLookUp() bool {
	return !e.KeepOuterOrder &&
		(JoinerType(e.Joiners[0]) == base.InnerJoin ||
			JoinerType(e.Joiners[0]) == base.LeftOuterJoin ||
			JoinerType(e.Joiners[0]) == base.RightOuterJoin ||
			JoinerType(e.Joiners[0]) == base.AntiSemiJoin)
}

func (e *IndexNestedLoopHashJoin) newInnerWorker(taskCh chan *indexHashJoinTask, workerID int) *indexHashJoinInnerWorker {
	// Since multiple inner workers run concurrently, we should copy join's IndexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.IndexRanges.Range()))
	for _, ran := range e.IndexRanges.Range() {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	var innerStats *innerWorkerRuntimeStats
	if e.stats != nil {
		innerStats = &e.stats.innerWorker
	}
	var maxRows int
	// If the joiner supports incremental look up, we can fetch inner results in batches,
	// retrieving partial results multiple times until all are fetched.
	// Otherwise, we fetch all inner results in one batch.
	if e.supportIncrementalLookUp() {
		maxRows = maxRowsPerFetch
	}
	iw := &indexHashJoinInnerWorker{
		innerWorker: innerWorker{
			InnerCtx:      e.InnerCtx,
			outerCtx:      e.OuterCtx,
			ctx:           e.Ctx(),
			maxFetchSize:  maxRows,
			indexRanges:   copiedRanges,
			keyOff2IdxOff: e.KeyOff2IdxOff,
			stats:         innerStats,
			lookup:        &e.IndexLookUpJoin,
			memTracker:    memory.NewTracker(memory.LabelForIndexJoinInnerWorker, -1),
		},
		taskCh:            taskCh,
		joiner:            e.Joiners[workerID],
		joinChkResourceCh: e.joinChkResourceCh[workerID],
		resultCh:          e.resultCh,
		joinKeyBuf:        make([]byte, 1),
		outerRowStatus:    make([]outerRowStatusFlag, 0, e.MaxChunkSize()),
		rowIter:           chunk.NewIterator4Slice([]chunk.Row{}),
	}
	iw.memTracker.AttachTo(e.memTracker)
	if len(copiedRanges) != 0 {
		// We should not consume this memory usage in `iw.memTracker`. The
		// memory usage of inner worker will be reset the end of iw.handleTask.
		// While the life cycle of this memory consumption exists throughout the
		// whole active period of inner worker.
		e.Ctx().GetSessionVars().StmtCtx.MemTracker.Consume(2 * types.EstimatedMemUsage(copiedRanges[0].LowVal, len(copiedRanges)))
	}
	if e.LastColHelper != nil {
		// nextCwf.TmpConstant needs to be reset for every individual
		// inner worker to avoid data race when the inner workers is running
		// concurrently.
		nextCwf := *e.LastColHelper
		nextCwf.TmpConstant = make([]*expression.Constant, len(e.LastColHelper.TmpConstant))
		for i := range e.LastColHelper.TmpConstant {
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
		// The previous task has been processed, so release the occupied memory
		if task != nil {
			task.memTracker.Detach()
		}
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
	// When task.KeepOuterOrder is TRUE (resultCh != iw.resultCh):
	//   - the last joinResult will be handled when the task has been processed,
	//     thus we DO NOT need to check it here again.
	//   - we DO NOT check the error here neither, because:
	//     - if the error is from task.err, the main thread will check the error of each task
	//     - if the error is from handleTask, the error will be handled in handleTask
	// We should not check `task != nil && !task.KeepOuterOrder` here since it's
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
		joinResult.err = ctx.Err()
		return joinResult, false
	}
	return joinResult, ok
}

func (iw *indexHashJoinInnerWorker) buildHashTableForOuterResult(task *indexHashJoinTask, h hash.Hash64) {
	failpoint.Inject("IndexHashJoinBuildHashTablePanic", nil)
	failpoint.Inject("ConsumeRandomPanic", nil)
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			atomic.AddInt64(&iw.stats.build, int64(time.Since(start)))
		}()
	}
	buf, numChks := make([]byte, 1), task.outerResult.NumChunks()
	task.lookupMap = newUnsafeHashTable(task.outerResult.Len())
	for chkIdx := range numChks {
		chk := task.outerResult.GetChunk(chkIdx)
		numRows := chk.NumRows()
		if iw.lookup.Finished.Load().(bool) {
			return
		}
	OUTER:
		for rowIdx := range numRows {
			if task.outerMatch != nil && !task.outerMatch[chkIdx][rowIdx] {
				continue
			}
			row := chk.GetRow(rowIdx)
			hashColIdx := iw.outerCtx.HashCols
			for i, colIdx := range hashColIdx {
				if row.IsNull(colIdx) && !iw.HashIsNullEQ[i] {
					continue OUTER
				}
			}
			h.Reset()
			err := codec.HashChunkRow(iw.ctx.GetSessionVars().StmtCtx.TypeCtx(), h, row, iw.outerCtx.HashTypes, hashColIdx, buf)
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

func (iw *indexHashJoinInnerWorker) handleHashJoinInnerWorkerPanic(resultCh chan *indexHashJoinResult, err error) {
	defer func() {
		iw.wg.Done()
		iw.lookup.WorkerWg.Done()
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
				select {
				case <-ctx.Done():
				case resultCh <- joinResult:
				}
			}
			close(resultCh)
		}
	}()
	failpoint.Inject("testIssue54055_2", func(val failpoint.Value) {
		if val.(bool) {
			time.Sleep(10 * time.Millisecond)
			panic("testIssue54055_2")
		}
	})
	var joinStartTime time.Time
	if iw.stats != nil {
		start := time.Now()
		defer func() {
			endTime := time.Now()
			atomic.AddInt64(&iw.stats.totalTime, int64(endTime.Sub(start)))
			// Only used for doJoinInOrder. doJoinUnordered will calculate join time in itself.
			// FetchInnerResults maybe return err and return, so joinStartTime is not initialized.
			if !joinStartTime.IsZero() {
				atomic.AddInt64(&iw.stats.join, int64(endTime.Sub(joinStartTime)))
			}
		}()
	}

	iw.wg = &sync.WaitGroup{}
	iw.wg.Add(1)
	iw.lookup.WorkerWg.Add(1)
	var buildHashTableErr error
	// TODO(XuHuaiyu): we may always use the smaller side to build the hashtable.
	go util.WithRecovery(
		func() {
			iw.buildHashTableForOuterResult(task, h)
		},
		func(r any) {
			if r != nil {
				buildHashTableErr = errors.Errorf("%v", r)
			}
			iw.handleHashJoinInnerWorkerPanic(resultCh, buildHashTableErr)
		},
	)
	lookUpContents, err := iw.constructLookupContent(task.lookUpJoinTask)
	if err == nil {
		err = iw.innerWorker.fetchInnerResults(ctx, task.lookUpJoinTask, lookUpContents)
	}
	iw.wg.Wait()
	// check error after wg.Wait to make sure error message can be sent to
	// resultCh even if panic happen in buildHashTableForOuterResult.
	failpoint.Inject("IndexHashJoinFetchInnerResultsErr", func() {
		err = errors.New("IndexHashJoinFetchInnerResultsErr")
	})
	failpoint.Inject("ConsumeRandomPanic", nil)
	if err != nil {
		return err
	}

	if buildHashTableErr != nil {
		return buildHashTableErr
	}

	if !task.keepOuterOrder {
		for {
			err = iw.doJoinUnordered(ctx, task, joinResult, h, resultCh)
			if err != nil {
				if task.innerExec != nil {
					terror.Log(exec.Close(task.innerExec))
					task.innerExec = nil
				}
				return err
			}
			// innerExec not finished, we need to fetch inner results and do join again.
			if task.innerExec != nil {
				err = iw.innerWorker.fetchInnerResults(ctx, task.lookUpJoinTask, lookUpContents)
				if err != nil {
					return err
				}
				continue
			}
			break
		}
		return nil
	}
	joinStartTime = time.Now()
	return iw.doJoinInOrder(ctx, task, joinResult, h, resultCh)
}
