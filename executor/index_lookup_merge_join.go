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
	"fmt"
	"runtime"
	"runtime/trace"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
)

// IndexLookUpMergeJoin realizes IndexLookUpJoin by merge join
// It preserves the order of the outer table and support batch lookup.
//
// The execution flow is very similar to IndexLookUpReader:
// 1. outerWorker read N outer rows, build a task and send it to result channel and inner worker channel.
// 2. The innerWorker receives the task, builds key ranges from outer rows and fetch inner rows, then do merge join.
// 3. main thread receives the task and fetch results from the channel in task one by one.
// 4. If channel has been closed, main thread receives the next task.
type IndexLookUpMergeJoin struct {
	baseExecutor

	resultCh   <-chan *lookUpMergeJoinTask
	cancelFunc context.CancelFunc
	workerWg   *sync.WaitGroup

	outerMergeCtx outerMergeCtx
	innerMergeCtx innerMergeCtx

	joiners           []joiner
	joinChkResourceCh []chan *chunk.Chunk
	isOuterJoin       bool

	requiredRows int64

	task *lookUpMergeJoinTask

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int

	// lastColHelper store the information for last col if there's complicated filter like col > x_col and col < x_col + 100.
	lastColHelper *plannercore.ColWithCmpFuncManager

	memTracker *memory.Tracker // track memory usage
}

type outerMergeCtx struct {
	rowTypes      []*types.FieldType
	joinKeys      []*expression.Column
	keyCols       []int
	filter        expression.CNFExprs
	needOuterSort bool
	compareFuncs  []expression.CompareFunc
}

type innerMergeCtx struct {
	readerBuilder           *dataReaderBuilder
	rowTypes                []*types.FieldType
	joinKeys                []*expression.Column
	keyCols                 []int
	compareFuncs            []expression.CompareFunc
	colLens                 []int
	desc                    bool
	keyOff2KeyOffOrderByIdx []int
}

type lookUpMergeJoinTask struct {
	outerResult   *chunk.List
	outerOrderIdx []chunk.RowPtr

	innerResult *chunk.Chunk
	innerIter   chunk.Iterator

	sameKeyInnerRows []chunk.Row
	sameKeyIter      chunk.Iterator

	doneErr error
	results chan *indexMergeJoinResult

	memTracker *memory.Tracker
}

type outerMergeWorker struct {
	outerMergeCtx

	lookup *IndexLookUpMergeJoin

	ctx      sessionctx.Context
	executor Executor

	maxBatchSize int
	batchSize    int

	nextColCompareFilters *plannercore.ColWithCmpFuncManager

	resultCh chan<- *lookUpMergeJoinTask
	innerCh  chan<- *lookUpMergeJoinTask

	parentMemTracker *memory.Tracker
}

type innerMergeWorker struct {
	innerMergeCtx

	taskCh            <-chan *lookUpMergeJoinTask
	joinChkResourceCh chan *chunk.Chunk
	outerMergeCtx     outerMergeCtx
	ctx               sessionctx.Context
	innerExec         Executor
	joiner            joiner
	retFieldTypes     []*types.FieldType

	maxChunkSize          int
	indexRanges           []*ranger.Range
	nextColCompareFilters *plannercore.ColWithCmpFuncManager
	keyOff2IdxOff         []int
}

type indexMergeJoinResult struct {
	chk *chunk.Chunk
	src chan<- *chunk.Chunk
}

// Open implements the Executor interface
func (e *IndexLookUpMergeJoin) Open(ctx context.Context) error {
	// Be careful, very dirty hack in this line!!!
	// IndexLookMergeUpJoin need to rebuild executor (the dataReaderBuilder) during
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
	_, err := e.innerMergeCtx.readerBuilder.getSnapshotTS()
	if err != nil {
		return err
	}

	err = e.children[0].Open(ctx)
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.startWorkers(ctx)
	return nil
}

func (e *IndexLookUpMergeJoin) startWorkers(ctx context.Context) {
	// TODO: consider another session currency variable for index merge join.
	// Because its parallelization is not complete.
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency()
	resultCh := make(chan *lookUpMergeJoinTask, concurrency)
	e.resultCh = resultCh
	e.joinChkResourceCh = make([]chan *chunk.Chunk, concurrency)
	for i := 0; i < concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, numResChkHold)
		for j := 0; j < numResChkHold; j++ {
			e.joinChkResourceCh[i] <- chunk.NewChunkWithCapacity(e.retFieldTypes, e.maxChunkSize)
		}
	}
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *lookUpMergeJoinTask, concurrency)
	e.workerWg.Add(1)
	go e.newOuterWorker(resultCh, innerCh).run(workerCtx, e.workerWg, e.cancelFunc)
	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.newInnerMergeWorker(innerCh, i).run(workerCtx, e.workerWg, e.cancelFunc)
	}
}

func (e *IndexLookUpMergeJoin) newOuterWorker(resultCh, innerCh chan *lookUpMergeJoinTask) *outerMergeWorker {
	omw := &outerMergeWorker{
		outerMergeCtx:         e.outerMergeCtx,
		ctx:                   e.ctx,
		lookup:                e,
		executor:              e.children[0],
		resultCh:              resultCh,
		innerCh:               innerCh,
		batchSize:             32,
		maxBatchSize:          e.ctx.GetSessionVars().IndexJoinBatchSize,
		parentMemTracker:      e.memTracker,
		nextColCompareFilters: e.lastColHelper,
	}
	failpoint.Inject("testIssue18068", func() {
		omw.batchSize = 1
	})
	return omw
}

func (e *IndexLookUpMergeJoin) newInnerMergeWorker(taskCh chan *lookUpMergeJoinTask, workID int) *innerMergeWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	imw := &innerMergeWorker{
		innerMergeCtx:     e.innerMergeCtx,
		outerMergeCtx:     e.outerMergeCtx,
		taskCh:            taskCh,
		ctx:               e.ctx,
		indexRanges:       copiedRanges,
		keyOff2IdxOff:     e.keyOff2IdxOff,
		joiner:            e.joiners[workID],
		joinChkResourceCh: e.joinChkResourceCh[workID],
		retFieldTypes:     e.retFieldTypes,
		maxChunkSize:      e.maxChunkSize,
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
		imw.nextColCompareFilters = &nextCwf
	}
	return imw
}

// Next implements the Executor interface
func (e *IndexLookUpMergeJoin) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.isOuterJoin {
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()
	if e.task == nil {
		e.getFinishedTask(ctx)
	}
	for e.task != nil {
		select {
		case result, ok := <-e.task.results:
			if !ok {
				if e.task.doneErr != nil {
					return e.task.doneErr
				}
				e.getFinishedTask(ctx)
				continue
			}
			req.SwapColumns(result.chk)
			result.src <- result.chk
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (e *IndexLookUpMergeJoin) getFinishedTask(ctx context.Context) {
	select {
	case e.task = <-e.resultCh:
	case <-ctx.Done():
		e.task = nil
	}

	// TODO: reuse the finished task memory to build tasks.
}

func (omw *outerMergeWorker) run(ctx context.Context, wg *sync.WaitGroup, cancelFunc context.CancelFunc) {
	defer trace.StartRegion(ctx, "IndexLookupMergeJoinOuterWorker").End()
	defer func() {
		if r := recover(); r != nil {
			task := &lookUpMergeJoinTask{
				doneErr: errors.New(fmt.Sprintf("%v", r)),
				results: make(chan *indexMergeJoinResult, numResChkHold),
			}
			close(task.results)
			omw.resultCh <- task
			cancelFunc()
		}
		close(omw.resultCh)
		close(omw.innerCh)
		wg.Done()
	}()
	for {
		task, err := omw.buildTask(ctx)
		if err != nil {
			task.doneErr = err
			close(task.results)
			omw.pushToChan(ctx, task, omw.resultCh)
			return
		}
		failpoint.Inject("mockIndexMergeJoinOOMPanic", nil)
		if task == nil {
			return
		}

		if finished := omw.pushToChan(ctx, task, omw.innerCh); finished {
			return
		}

		if finished := omw.pushToChan(ctx, task, omw.resultCh); finished {
			return
		}
	}
}

func (omw *outerMergeWorker) pushToChan(ctx context.Context, task *lookUpMergeJoinTask, dst chan<- *lookUpMergeJoinTask) (finished bool) {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

// buildTask builds a lookUpMergeJoinTask and read outer rows.
// When err is not nil, task must not be nil to send the error to the main thread via task
func (omw *outerMergeWorker) buildTask(ctx context.Context) (*lookUpMergeJoinTask, error) {
	task := &lookUpMergeJoinTask{
		results:     make(chan *indexMergeJoinResult, numResChkHold),
		outerResult: chunk.NewList(omw.rowTypes, omw.executor.base().initCap, omw.executor.base().maxChunkSize),
	}
	task.memTracker = memory.NewTracker(memory.LabelForSimpleTask, -1)
	task.memTracker.AttachTo(omw.parentMemTracker)

	omw.increaseBatchSize()
	requiredRows := omw.batchSize
	if omw.lookup.isOuterJoin {
		requiredRows = int(atomic.LoadInt64(&omw.lookup.requiredRows))
	}
	if requiredRows <= 0 || requiredRows > omw.maxBatchSize {
		requiredRows = omw.maxBatchSize
	}
	for requiredRows > 0 {
		execChk := newFirstChunk(omw.executor)
		err := Next(ctx, omw.executor, execChk)
		if err != nil {
			return task, err
		}
		if execChk.NumRows() == 0 {
			break
		}

		task.outerResult.Add(execChk)
		requiredRows -= execChk.NumRows()
		task.memTracker.Consume(execChk.MemoryUsage())
	}

	if task.outerResult.Len() == 0 {
		return nil, nil
	}

	return task, nil
}

func (omw *outerMergeWorker) increaseBatchSize() {
	if omw.batchSize < omw.maxBatchSize {
		omw.batchSize *= 2
	}
	if omw.batchSize > omw.maxBatchSize {
		omw.batchSize = omw.maxBatchSize
	}
}

func (imw *innerMergeWorker) run(ctx context.Context, wg *sync.WaitGroup, cancelFunc context.CancelFunc) {
	defer trace.StartRegion(ctx, "IndexLookupMergeJoinInnerWorker").End()
	var task *lookUpMergeJoinTask
	defer func() {
		wg.Done()
		if r := recover(); r != nil {
			if task != nil {
				task.doneErr = errors.Errorf("%v", r)
				close(task.results)
			}
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("innerMergeWorker panicked", zap.String("stack", string(buf)))
			cancelFunc()
		}
	}()

	for ok := true; ok; {
		select {
		case task, ok = <-imw.taskCh:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}

		err := imw.handleTask(ctx, task)
		task.doneErr = err
		close(task.results)
	}
}

func (imw *innerMergeWorker) handleTask(ctx context.Context, task *lookUpMergeJoinTask) (err error) {
	numOuterChks := task.outerResult.NumChunks()
	var outerMatch [][]bool
	if imw.outerMergeCtx.filter != nil {
		outerMatch = make([][]bool, numOuterChks)
		for i := 0; i < numOuterChks; i++ {
			chk := task.outerResult.GetChunk(i)
			outerMatch[i] = make([]bool, chk.NumRows())
			outerMatch[i], err = expression.VectorizedFilter(imw.ctx, imw.outerMergeCtx.filter, chunk.NewIterator4Chunk(chk), outerMatch[i])
			if err != nil {
				return err
			}
		}
	}
	task.outerOrderIdx = make([]chunk.RowPtr, 0, task.outerResult.Len())
	for i := 0; i < numOuterChks; i++ {
		numRow := task.outerResult.GetChunk(i).NumRows()
		for j := 0; j < numRow; j++ {
			if len(outerMatch) == 0 || outerMatch[i][j] {
				task.outerOrderIdx = append(task.outerOrderIdx, chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)})
			}
		}
	}
	task.memTracker.Consume(int64(cap(task.outerOrderIdx)))
	failpoint.Inject("IndexMergeJoinMockOOM", func(val failpoint.Value) {
		if val.(bool) {
			panic("OOM test index merge join doesn't hang here.")
		}
	})
	// needOuterSort means the outer side property items can't guarantee the order of join keys.
	// Because the necessary condition of merge join is both outer and inner keep order of join keys.
	// In this case, we need sort the outer side.
	if imw.outerMergeCtx.needOuterSort {
		sort.Slice(task.outerOrderIdx, func(i, j int) bool {
			idxI, idxJ := task.outerOrderIdx[i], task.outerOrderIdx[j]
			rowI, rowJ := task.outerResult.GetRow(idxI), task.outerResult.GetRow(idxJ)
			var cmp int64
			var err error
			for _, keyOff := range imw.keyOff2KeyOffOrderByIdx {
				joinKey := imw.outerMergeCtx.joinKeys[keyOff]
				cmp, _, err = imw.outerMergeCtx.compareFuncs[keyOff](imw.ctx, joinKey, joinKey, rowI, rowJ)
				terror.Log(err)
				if cmp != 0 {
					break
				}
			}
			if cmp != 0 || imw.nextColCompareFilters == nil {
				return (cmp < 0 && !imw.desc) || (cmp > 0 && imw.desc)
			}
			cmp = int64(imw.nextColCompareFilters.CompareRow(rowI, rowJ))
			return (cmp < 0 && !imw.desc) || (cmp > 0 && imw.desc)
		})
	}
	dLookUpKeys, err := imw.constructDatumLookupKeys(task)
	if err != nil {
		return err
	}
	dLookUpKeys = imw.dedupDatumLookUpKeys(dLookUpKeys)
	// If the order requires descending, the deDupedLookUpContents is keep descending order before.
	// So at the end, we should generate the ascending deDupedLookUpContents to build the correct range for inner read.
	if imw.desc {
		lenKeys := len(dLookUpKeys)
		for i := 0; i < lenKeys/2; i++ {
			dLookUpKeys[i], dLookUpKeys[lenKeys-i-1] = dLookUpKeys[lenKeys-i-1], dLookUpKeys[i]
		}
	}
	imw.innerExec, err = imw.readerBuilder.buildExecutorForIndexJoin(ctx, dLookUpKeys, imw.indexRanges, imw.keyOff2IdxOff, imw.nextColCompareFilters)
	if err != nil {
		return err
	}
	defer terror.Call(imw.innerExec.Close)
	_, err = imw.fetchNextInnerResult(ctx, task)
	if err != nil {
		return err
	}
	err = imw.doMergeJoin(ctx, task)
	return err
}

func (imw *innerMergeWorker) fetchNewChunkWhenFull(ctx context.Context, task *lookUpMergeJoinTask, chk **chunk.Chunk) (continueJoin bool) {
	if !(*chk).IsFull() {
		return true
	}
	select {
	case task.results <- &indexMergeJoinResult{*chk, imw.joinChkResourceCh}:
	case <-ctx.Done():
		return false
	}
	var ok bool
	select {
	case *chk, ok = <-imw.joinChkResourceCh:
		if !ok {
			return false
		}
	case <-ctx.Done():
		return false
	}
	(*chk).Reset()
	return true
}

func (imw *innerMergeWorker) doMergeJoin(ctx context.Context, task *lookUpMergeJoinTask) (err error) {
	chk := <-imw.joinChkResourceCh
	defer func() {
		if chk == nil {
			return
		}
		if chk.NumRows() > 0 {
			select {
			case task.results <- &indexMergeJoinResult{chk, imw.joinChkResourceCh}:
			case <-ctx.Done():
				return
			}
		} else {
			imw.joinChkResourceCh <- chk
		}
	}()

	initCmpResult := 1
	if imw.innerMergeCtx.desc {
		initCmpResult = -1
	}
	noneInnerRowsRemain := task.innerResult.NumRows() == 0

	for _, outerIdx := range task.outerOrderIdx {
		outerRow := task.outerResult.GetRow(outerIdx)
		hasMatch, hasNull, cmpResult := false, false, initCmpResult
		// If it has iterated out all inner rows and the inner rows with same key is empty,
		// that means the outer row needn't match any inner rows.
		if noneInnerRowsRemain && len(task.sameKeyInnerRows) == 0 {
			goto missMatch
		}
		if len(task.sameKeyInnerRows) > 0 {
			cmpResult, err = imw.compare(outerRow, task.sameKeyIter.Begin())
			if err != nil {
				return err
			}
		}
		if (cmpResult > 0 && !imw.innerMergeCtx.desc) || (cmpResult < 0 && imw.innerMergeCtx.desc) {
			if noneInnerRowsRemain {
				task.sameKeyInnerRows = task.sameKeyInnerRows[:0]
				goto missMatch
			}
			noneInnerRowsRemain, err = imw.fetchInnerRowsWithSameKey(ctx, task, outerRow)
			if err != nil {
				return err
			}
		}

		for task.sameKeyIter.Current() != task.sameKeyIter.End() {
			matched, isNull, err := imw.joiner.tryToMatchInners(outerRow, task.sameKeyIter, chk)
			if err != nil {
				return err
			}
			hasMatch = hasMatch || matched
			hasNull = hasNull || isNull
			if !imw.fetchNewChunkWhenFull(ctx, task, &chk) {
				return nil
			}
		}

	missMatch:
		if !hasMatch {
			imw.joiner.onMissMatch(hasNull, outerRow, chk)
			if !imw.fetchNewChunkWhenFull(ctx, task, &chk) {
				return nil
			}
		}
	}

	return nil
}

// fetchInnerRowsWithSameKey collects the inner rows having the same key with one outer row.
func (imw *innerMergeWorker) fetchInnerRowsWithSameKey(ctx context.Context, task *lookUpMergeJoinTask, key chunk.Row) (noneInnerRows bool, err error) {
	task.sameKeyInnerRows = task.sameKeyInnerRows[:0]
	curRow := task.innerIter.Current()
	var cmpRes int
	for cmpRes, err = imw.compare(key, curRow); ((cmpRes >= 0 && !imw.desc) || (cmpRes <= 0 && imw.desc)) && err == nil; cmpRes, err = imw.compare(key, curRow) {
		if cmpRes == 0 {
			task.sameKeyInnerRows = append(task.sameKeyInnerRows, curRow)
		}
		curRow = task.innerIter.Next()
		if curRow == task.innerIter.End() {
			curRow, err = imw.fetchNextInnerResult(ctx, task)
			if err != nil || task.innerResult.NumRows() == 0 {
				break
			}
		}
	}
	task.sameKeyIter = chunk.NewIterator4Slice(task.sameKeyInnerRows)
	task.sameKeyIter.Begin()
	noneInnerRows = task.innerResult.NumRows() == 0
	return
}

func (imw *innerMergeWorker) compare(outerRow, innerRow chunk.Row) (int, error) {
	for _, keyOff := range imw.innerMergeCtx.keyOff2KeyOffOrderByIdx {
		cmp, _, err := imw.innerMergeCtx.compareFuncs[keyOff](imw.ctx, imw.outerMergeCtx.joinKeys[keyOff], imw.innerMergeCtx.joinKeys[keyOff], outerRow, innerRow)
		if err != nil || cmp != 0 {
			return int(cmp), err
		}
	}
	return 0, nil
}

func (imw *innerMergeWorker) constructDatumLookupKeys(task *lookUpMergeJoinTask) ([]*indexJoinLookUpContent, error) {
	numRows := len(task.outerOrderIdx)
	dLookUpKeys := make([]*indexJoinLookUpContent, 0, numRows)
	for i := 0; i < numRows; i++ {
		dLookUpKey, err := imw.constructDatumLookupKey(task, task.outerOrderIdx[i])
		if err != nil {
			return nil, err
		}
		if dLookUpKey == nil {
			continue
		}
		dLookUpKeys = append(dLookUpKeys, dLookUpKey)
	}

	return dLookUpKeys, nil
}

func (imw *innerMergeWorker) constructDatumLookupKey(task *lookUpMergeJoinTask, rowIdx chunk.RowPtr) (*indexJoinLookUpContent, error) {
	outerRow := task.outerResult.GetRow(rowIdx)
	sc := imw.ctx.GetSessionVars().StmtCtx
	keyLen := len(imw.keyCols)
	dLookupKey := make([]types.Datum, 0, keyLen)
	for i, keyCol := range imw.outerMergeCtx.keyCols {
		outerValue := outerRow.GetDatum(keyCol, imw.outerMergeCtx.rowTypes[keyCol])
		// Join-on-condition can be promised to be equal-condition in
		// IndexNestedLoopJoin, thus the filter will always be false if
		// outerValue is null, and we don't need to lookup it.
		if outerValue.IsNull() {
			return nil, nil
		}
		innerColType := imw.rowTypes[imw.keyCols[i]]
		innerValue, err := outerValue.ConvertTo(sc, innerColType)
		if err != nil {
			// If the converted outerValue overflows, we don't need to lookup it.
			if terror.ErrorEqual(err, types.ErrOverflow) {
				return nil, nil
			}
			if terror.ErrorEqual(err, types.ErrTruncated) && (innerColType.Tp == mysql.TypeSet || innerColType.Tp == mysql.TypeEnum) {
				return nil, nil
			}
			return nil, err
		}
		cmp, err := outerValue.CompareDatum(sc, &innerValue)
		if err != nil {
			return nil, err
		}
		if cmp != 0 {
			// If the converted outerValue is not equal to the origin outerValue, we don't need to lookup it.
			return nil, nil
		}
		dLookupKey = append(dLookupKey, innerValue)
	}
	return &indexJoinLookUpContent{keys: dLookupKey, row: task.outerResult.GetRow(rowIdx)}, nil
}

func (imw *innerMergeWorker) dedupDatumLookUpKeys(lookUpContents []*indexJoinLookUpContent) []*indexJoinLookUpContent {
	if len(lookUpContents) < 2 {
		return lookUpContents
	}
	sc := imw.ctx.GetSessionVars().StmtCtx
	deDupedLookUpContents := lookUpContents[:1]
	for i := 1; i < len(lookUpContents); i++ {
		cmp := compareRow(sc, lookUpContents[i].keys, lookUpContents[i-1].keys)
		if cmp != 0 || (imw.nextColCompareFilters != nil && imw.nextColCompareFilters.CompareRow(lookUpContents[i].row, lookUpContents[i-1].row) != 0) {
			deDupedLookUpContents = append(deDupedLookUpContents, lookUpContents[i])
		}
	}
	return deDupedLookUpContents
}

// fetchNextInnerResult collects a chunk of inner results from inner child executor.
func (imw *innerMergeWorker) fetchNextInnerResult(ctx context.Context, task *lookUpMergeJoinTask) (beginRow chunk.Row, err error) {
	task.innerResult = chunk.NewChunkWithCapacity(retTypes(imw.innerExec), imw.ctx.GetSessionVars().MaxChunkSize)
	err = Next(ctx, imw.innerExec, task.innerResult)
	task.innerIter = chunk.NewIterator4Chunk(task.innerResult)
	beginRow = task.innerIter.Begin()
	return
}

// Close implements the Executor interface.
func (e *IndexLookUpMergeJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
		e.cancelFunc = nil
	}
	if e.resultCh != nil {
		for range e.resultCh {
		}
		e.resultCh = nil
	}
	e.joinChkResourceCh = nil
	// joinChkResourceCh is to recycle result chunks, used by inner worker.
	// resultCh is the main thread get the results, used by main thread and inner worker.
	// cancelFunc control the outer worker and outer worker close the task channel.
	e.workerWg.Wait()
	e.memTracker = nil
	if e.runtimeStats != nil {
		concurrency := cap(e.resultCh)
		runtimeStats := &execdetails.RuntimeStatsWithConcurrencyInfo{BasicRuntimeStats: e.runtimeStats}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", concurrency))
		e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, runtimeStats)
	}
	return e.baseExecutor.Close()
}
