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

package join

import (
	"context"
	"runtime/trace"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
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
	exec.BaseExecutor

	resultCh   <-chan *lookUpMergeJoinTask
	cancelFunc context.CancelFunc
	WorkerWg   *sync.WaitGroup

	OuterMergeCtx OuterMergeCtx
	InnerMergeCtx InnerMergeCtx

	Joiners           []Joiner
	joinChkResourceCh []chan *chunk.Chunk
	IsOuterJoin       bool

	requiredRows int64

	task *lookUpMergeJoinTask

	IndexRanges   ranger.MutableRanges
	KeyOff2IdxOff []int

	// LastColHelper store the information for last col if there's complicated filter like col > x_col and col < x_col + 100.
	LastColHelper *plannercore.ColWithCmpFuncManager

	memTracker *memory.Tracker // track memory usage
	prepared   bool
}

// OuterMergeCtx is the outer side ctx of merge join
type OuterMergeCtx struct {
	RowTypes      []*types.FieldType
	JoinKeys      []*expression.Column
	KeyCols       []int
	Filter        expression.CNFExprs
	NeedOuterSort bool
	CompareFuncs  []expression.CompareFunc
}

// InnerMergeCtx is the inner side ctx of merge join
type InnerMergeCtx struct {
	ReaderBuilder           IndexJoinExecutorBuilder
	RowTypes                []*types.FieldType
	JoinKeys                []*expression.Column
	KeyCols                 []int
	KeyCollators            []collate.Collator
	CompareFuncs            []expression.CompareFunc
	ColLens                 []int
	Desc                    bool
	KeyOff2KeyOffOrderByIdx []int
}

type lookUpMergeJoinTask struct {
	outerResult   *chunk.List
	outerMatch    [][]bool
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
	OuterMergeCtx

	lookup *IndexLookUpMergeJoin

	ctx      sessionctx.Context
	executor exec.Executor

	maxBatchSize int
	batchSize    int

	nextColCompareFilters *plannercore.ColWithCmpFuncManager

	resultCh chan<- *lookUpMergeJoinTask
	innerCh  chan<- *lookUpMergeJoinTask

	parentMemTracker *memory.Tracker
}

type innerMergeWorker struct {
	InnerMergeCtx

	taskCh            <-chan *lookUpMergeJoinTask
	joinChkResourceCh chan *chunk.Chunk
	outerMergeCtx     OuterMergeCtx
	ctx               sessionctx.Context
	innerExec         exec.Executor
	joiner            Joiner
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
	err := exec.Open(ctx, e.Children(0))
	if err != nil {
		return err
	}
	e.memTracker = memory.NewTracker(e.ID(), -1)
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
	return nil
}

func (e *IndexLookUpMergeJoin) startWorkers(ctx context.Context) {
	// TODO: consider another session currency variable for index merge join.
	// Because its parallelization is not complete.
	concurrency := e.Ctx().GetSessionVars().IndexLookupJoinConcurrency()
	if e.RuntimeStats() != nil {
		runtimeStats := &execdetails.RuntimeStatsWithConcurrencyInfo{}
		runtimeStats.SetConcurrencyInfo(execdetails.NewConcurrencyInfo("Concurrency", concurrency))
		e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), runtimeStats)
	}

	resultCh := make(chan *lookUpMergeJoinTask, concurrency)
	e.resultCh = resultCh
	e.joinChkResourceCh = make([]chan *chunk.Chunk, concurrency)
	for i := 0; i < concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, numResChkHold)
		for j := 0; j < numResChkHold; j++ {
			e.joinChkResourceCh[i] <- chunk.NewChunkWithCapacity(e.RetFieldTypes(), e.MaxChunkSize())
		}
	}
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *lookUpMergeJoinTask, concurrency)
	e.WorkerWg.Add(1)
	go e.newOuterWorker(resultCh, innerCh).run(workerCtx, e.WorkerWg, e.cancelFunc)
	e.WorkerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.newInnerMergeWorker(innerCh, i).run(workerCtx, e.WorkerWg, e.cancelFunc)
	}
}

func (e *IndexLookUpMergeJoin) newOuterWorker(resultCh, innerCh chan *lookUpMergeJoinTask) *outerMergeWorker {
	omw := &outerMergeWorker{
		OuterMergeCtx:         e.OuterMergeCtx,
		ctx:                   e.Ctx(),
		lookup:                e,
		executor:              e.Children(0),
		resultCh:              resultCh,
		innerCh:               innerCh,
		batchSize:             32,
		maxBatchSize:          e.Ctx().GetSessionVars().IndexJoinBatchSize,
		parentMemTracker:      e.memTracker,
		nextColCompareFilters: e.LastColHelper,
	}
	failpoint.Inject("testIssue18068", func() {
		omw.batchSize = 1
	})
	return omw
}

func (e *IndexLookUpMergeJoin) newInnerMergeWorker(taskCh chan *lookUpMergeJoinTask, workID int) *innerMergeWorker {
	// Since multiple inner workers run concurrently, we should copy join's IndexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.IndexRanges.Range()))
	for _, ran := range e.IndexRanges.Range() {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	imw := &innerMergeWorker{
		InnerMergeCtx:     e.InnerMergeCtx,
		outerMergeCtx:     e.OuterMergeCtx,
		taskCh:            taskCh,
		ctx:               e.Ctx(),
		indexRanges:       copiedRanges,
		keyOff2IdxOff:     e.KeyOff2IdxOff,
		joiner:            e.Joiners[workID],
		joinChkResourceCh: e.joinChkResourceCh[workID],
		retFieldTypes:     e.RetFieldTypes(),
		maxChunkSize:      e.MaxChunkSize(),
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
		imw.nextColCompareFilters = &nextCwf
	}
	return imw
}

// Next implements the Executor interface
func (e *IndexLookUpMergeJoin) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.prepared {
		e.startWorkers(ctx)
		e.prepared = true
	}
	if e.IsOuterJoin {
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()
	if e.task == nil {
		e.loadFinishedTask(ctx)
	}
	for e.task != nil {
		select {
		case result, ok := <-e.task.results:
			if !ok {
				if e.task.doneErr != nil {
					return e.task.doneErr
				}
				e.loadFinishedTask(ctx)
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

// TODO: reuse the Finished task memory to build tasks.
func (e *IndexLookUpMergeJoin) loadFinishedTask(ctx context.Context) {
	select {
	case e.task = <-e.resultCh:
	case <-ctx.Done():
		e.task = nil
	}
}

func (omw *outerMergeWorker) run(ctx context.Context, wg *sync.WaitGroup, cancelFunc context.CancelFunc) {
	defer trace.StartRegion(ctx, "IndexLookupMergeJoinOuterWorker").End()
	defer func() {
		if r := recover(); r != nil {
			task := &lookUpMergeJoinTask{
				doneErr: util.GetRecoverError(r),
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

func (*outerMergeWorker) pushToChan(ctx context.Context, task *lookUpMergeJoinTask, dst chan<- *lookUpMergeJoinTask) (finished bool) {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

// buildTask builds a lookUpMergeJoinTask and read Outer rows.
// When err is not nil, task must not be nil to send the error to the main thread via task
func (omw *outerMergeWorker) buildTask(ctx context.Context) (*lookUpMergeJoinTask, error) {
	task := &lookUpMergeJoinTask{
		results:     make(chan *indexMergeJoinResult, numResChkHold),
		outerResult: chunk.NewList(omw.RowTypes, omw.executor.InitCap(), omw.executor.MaxChunkSize()),
	}
	task.memTracker = memory.NewTracker(memory.LabelForSimpleTask, -1)
	task.memTracker.AttachTo(omw.parentMemTracker)

	omw.increaseBatchSize()
	requiredRows := omw.batchSize
	if omw.lookup.IsOuterJoin {
		requiredRows = int(atomic.LoadInt64(&omw.lookup.requiredRows))
	}
	if requiredRows <= 0 || requiredRows > omw.maxBatchSize {
		requiredRows = omw.maxBatchSize
	}
	for requiredRows > 0 {
		execChk := exec.TryNewCacheChunk(omw.executor)
		err := exec.Next(ctx, omw.executor, execChk)
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
				task.doneErr = util.GetRecoverError(r)
				close(task.results)
			}
			logutil.Logger(ctx).Error("innerMergeWorker panicked", zap.Any("recover", r), zap.Stack("stack"))
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
	if imw.outerMergeCtx.Filter != nil {
		task.outerMatch = make([][]bool, numOuterChks)
		exprCtx := imw.ctx.GetExprCtx()
		for i := 0; i < numOuterChks; i++ {
			chk := task.outerResult.GetChunk(i)
			task.outerMatch[i] = make([]bool, chk.NumRows())
			task.outerMatch[i], err = expression.VectorizedFilter(exprCtx.GetEvalCtx(), imw.ctx.GetSessionVars().EnableVectorizedExpression, imw.outerMergeCtx.Filter, chunk.NewIterator4Chunk(chk), task.outerMatch[i])
			if err != nil {
				return err
			}
		}
	}
	task.memTracker.Consume(int64(cap(task.outerMatch)))
	task.outerOrderIdx = make([]chunk.RowPtr, 0, task.outerResult.Len())
	for i := 0; i < numOuterChks; i++ {
		numRow := task.outerResult.GetChunk(i).NumRows()
		for j := 0; j < numRow; j++ {
			task.outerOrderIdx = append(task.outerOrderIdx, chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)})
		}
	}
	task.memTracker.Consume(int64(cap(task.outerOrderIdx)))
	failpoint.Inject("IndexMergeJoinMockOOM", func(val failpoint.Value) {
		if val.(bool) {
			panic("OOM test index merge join doesn't hang here.")
		}
	})
	// NeedOuterSort means the outer side property items can't guarantee the order of join keys.
	// Because the necessary condition of merge join is both outer and inner keep order of join keys.
	// In this case, we need sort the outer side.
	if imw.outerMergeCtx.NeedOuterSort {
		exprCtx := imw.ctx.GetExprCtx()
		slices.SortFunc(task.outerOrderIdx, func(idxI, idxJ chunk.RowPtr) int {
			rowI, rowJ := task.outerResult.GetRow(idxI), task.outerResult.GetRow(idxJ)
			var c int64
			var err error
			for _, keyOff := range imw.KeyOff2KeyOffOrderByIdx {
				joinKey := imw.outerMergeCtx.JoinKeys[keyOff]
				c, _, err = imw.outerMergeCtx.CompareFuncs[keyOff](exprCtx.GetEvalCtx(), joinKey, joinKey, rowI, rowJ)
				terror.Log(err)
				if c != 0 {
					break
				}
			}
			if c != 0 || imw.nextColCompareFilters == nil {
				if imw.Desc {
					return int(-c)
				}
				return int(c)
			}
			c = int64(imw.nextColCompareFilters.CompareRow(rowI, rowJ))
			if imw.Desc {
				return int(-c)
			}
			return int(c)
		})
	}
	dLookUpKeys, err := imw.constructDatumLookupKeys(task)
	if err != nil {
		return err
	}
	dLookUpKeys = imw.dedupDatumLookUpKeys(dLookUpKeys)
	// If the order requires descending, the deDupedLookUpContents is keep descending order before.
	// So at the end, we should generate the ascending deDupedLookUpContents to build the correct range for inner read.
	if imw.Desc {
		lenKeys := len(dLookUpKeys)
		for i := 0; i < lenKeys/2; i++ {
			dLookUpKeys[i], dLookUpKeys[lenKeys-i-1] = dLookUpKeys[lenKeys-i-1], dLookUpKeys[i]
		}
	}
	imw.innerExec, err = imw.ReaderBuilder.BuildExecutorForIndexJoin(ctx, dLookUpKeys, imw.indexRanges, imw.keyOff2IdxOff, imw.nextColCompareFilters, false, nil, nil)
	if imw.innerExec != nil {
		defer func() { terror.Log(exec.Close(imw.innerExec)) }()
	}
	if err != nil {
		return err
	}
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
	var chk *chunk.Chunk
	select {
	case chk = <-imw.joinChkResourceCh:
	case <-ctx.Done():
		return
	}
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
	if imw.InnerMergeCtx.Desc {
		initCmpResult = -1
	}
	noneInnerRowsRemain := task.innerResult.NumRows() == 0

	for _, outerIdx := range task.outerOrderIdx {
		outerRow := task.outerResult.GetRow(outerIdx)
		hasMatch, hasNull, cmpResult := false, false, initCmpResult
		if task.outerMatch != nil && !task.outerMatch[outerIdx.ChkIdx][outerIdx.RowIdx] {
			goto missMatch
		}
		// If it has iterated out all inner rows and the inner rows with same key is empty,
		// that means the Outer Row needn't match any inner rows.
		if noneInnerRowsRemain && len(task.sameKeyInnerRows) == 0 {
			goto missMatch
		}
		if len(task.sameKeyInnerRows) > 0 {
			cmpResult, err = imw.compare(outerRow, task.sameKeyIter.Begin())
			if err != nil {
				return err
			}
		}
		if (cmpResult > 0 && !imw.InnerMergeCtx.Desc) || (cmpResult < 0 && imw.InnerMergeCtx.Desc) {
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
			matched, isNull, err := imw.joiner.TryToMatchInners(outerRow, task.sameKeyIter, chk)
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
			imw.joiner.OnMissMatch(hasNull, outerRow, chk)
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
	for cmpRes, err = imw.compare(key, curRow); ((cmpRes >= 0 && !imw.Desc) || (cmpRes <= 0 && imw.Desc)) && err == nil; cmpRes, err = imw.compare(key, curRow) {
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
	exprCtx := imw.ctx.GetExprCtx()
	for _, keyOff := range imw.InnerMergeCtx.KeyOff2KeyOffOrderByIdx {
		cmp, _, err := imw.InnerMergeCtx.CompareFuncs[keyOff](exprCtx.GetEvalCtx(), imw.outerMergeCtx.JoinKeys[keyOff], imw.InnerMergeCtx.JoinKeys[keyOff], outerRow, innerRow)
		if err != nil || cmp != 0 {
			return int(cmp), err
		}
	}
	return 0, nil
}

func (imw *innerMergeWorker) constructDatumLookupKeys(task *lookUpMergeJoinTask) ([]*IndexJoinLookUpContent, error) {
	numRows := len(task.outerOrderIdx)
	dLookUpKeys := make([]*IndexJoinLookUpContent, 0, numRows)
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

func (imw *innerMergeWorker) constructDatumLookupKey(task *lookUpMergeJoinTask, idx chunk.RowPtr) (*IndexJoinLookUpContent, error) {
	if task.outerMatch != nil && !task.outerMatch[idx.ChkIdx][idx.RowIdx] {
		return nil, nil
	}
	outerRow := task.outerResult.GetRow(idx)
	sc := imw.ctx.GetSessionVars().StmtCtx
	keyLen := len(imw.KeyCols)
	dLookupKey := make([]types.Datum, 0, keyLen)
	for i, keyCol := range imw.outerMergeCtx.KeyCols {
		outerValue := outerRow.GetDatum(keyCol, imw.outerMergeCtx.RowTypes[keyCol])
		// Join-on-condition can be promised to be equal-condition in
		// IndexNestedLoopJoin, thus the Filter will always be false if
		// outerValue is null, and we don't need to lookup it.
		if outerValue.IsNull() {
			return nil, nil
		}
		innerColType := imw.RowTypes[imw.KeyCols[i]]
		innerValue, err := outerValue.ConvertTo(sc.TypeCtx(), innerColType)
		if err != nil {
			// If the converted outerValue overflows, we don't need to lookup it.
			if terror.ErrorEqual(err, types.ErrOverflow) || terror.ErrorEqual(err, types.ErrWarnDataOutOfRange) {
				return nil, nil
			}
			if terror.ErrorEqual(err, types.ErrTruncated) && (innerColType.GetType() == mysql.TypeSet || innerColType.GetType() == mysql.TypeEnum) {
				return nil, nil
			}
			return nil, err
		}
		cmp, err := outerValue.Compare(sc.TypeCtx(), &innerValue, imw.KeyCollators[i])
		if err != nil {
			return nil, err
		}
		if cmp != 0 {
			// If the converted outerValue is not equal to the origin outerValue, we don't need to lookup it.
			return nil, nil
		}
		dLookupKey = append(dLookupKey, innerValue)
	}
	return &IndexJoinLookUpContent{Keys: dLookupKey, Row: task.outerResult.GetRow(idx)}, nil
}

func (imw *innerMergeWorker) dedupDatumLookUpKeys(lookUpContents []*IndexJoinLookUpContent) []*IndexJoinLookUpContent {
	if len(lookUpContents) < 2 {
		return lookUpContents
	}
	sc := imw.ctx.GetSessionVars().StmtCtx
	deDupedLookUpContents := lookUpContents[:1]
	for i := 1; i < len(lookUpContents); i++ {
		cmp := compareRow(sc, lookUpContents[i].Keys, lookUpContents[i-1].Keys, imw.KeyCollators)
		if cmp != 0 || (imw.nextColCompareFilters != nil && imw.nextColCompareFilters.CompareRow(lookUpContents[i].Row, lookUpContents[i-1].Row) != 0) {
			deDupedLookUpContents = append(deDupedLookUpContents, lookUpContents[i])
		}
	}
	return deDupedLookUpContents
}

// fetchNextInnerResult collects a chunk of inner results from inner child executor.
func (imw *innerMergeWorker) fetchNextInnerResult(ctx context.Context, task *lookUpMergeJoinTask) (beginRow chunk.Row, err error) {
	task.innerResult = imw.innerExec.NewChunkWithCapacity(imw.innerExec.RetFieldTypes(), imw.innerExec.MaxChunkSize(), imw.innerExec.MaxChunkSize())
	err = exec.Next(ctx, imw.innerExec, task.innerResult)
	task.innerIter = chunk.NewIterator4Chunk(task.innerResult)
	beginRow = task.innerIter.Begin()
	return
}

// Close implements the Executor interface.
func (e *IndexLookUpMergeJoin) Close() error {
	if e.RuntimeStats() != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.RuntimeStats())
	}
	if e.cancelFunc != nil {
		e.cancelFunc()
		e.cancelFunc = nil
	}
	if e.resultCh != nil {
		channel.Clear(e.resultCh)
		e.resultCh = nil
	}
	e.joinChkResourceCh = nil
	// joinChkResourceCh is to recycle result chunks, used by inner worker.
	// resultCh is the main thread get the results, used by main thread and inner worker.
	// cancelFunc control the outer worker and outer worker close the task channel.
	e.WorkerWg.Wait()
	e.memTracker = nil
	e.prepared = false
	return e.BaseExecutor.Close()
}
