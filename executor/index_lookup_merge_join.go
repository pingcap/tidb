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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
)

// IndexLookUpMergeJoin realizes IndexLookUpJoin by merge join
// It preserves the order of the outer table and support batch lookup.
//
// The execution flow is very similar to IndexLookUpReader:
// 1. outerWorker read N outer rows, build a task and send it to result channel and inner worker channel.
// 2. The innerWorker receives the task, builds key ranges from outer rows and fetch inner rows, then do merge join.
// 3. main thread receives the task and fetch results from the channel in task one by one. If channel has been closed,
// 4. main thread receives the next task.
type IndexLookUpMergeJoin struct {
	baseExecutor

	resultCh   <-chan *lookUpMergeJoinTask
	cancelFunc context.CancelFunc
	workerWg   *sync.WaitGroup

	outerMergeCtx outerMergeCtx
	innerMergeCtx innerMergeCtx

	joiners     []joiner
	isOuterJoin bool

	requiredRows int64

	task *lookUpMergeJoinTask

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
	innerPtrBytes [][]byte

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
	readerBuilder *dataReaderBuilder
	rowTypes      []*types.FieldType
	joinKeys      []*expression.Column
	keyCols       []int
	compareFuncs  []expression.CompareFunc
	colLens       []int
	hasPrefixCol  bool
	desc          bool
}

type lookUpMergeJoinTask struct {
	outerResult *chunk.Chunk
	outerMatch  []bool
	outerIter   chunk.Iterator

	innerResult *chunk.Chunk
	innerIter   chunk.Iterator

	sameKeyRows []chunk.Row
	sameKeyIter chunk.Iterator

	doneErr  chan error
	hasMatch bool
	hasNull  bool

	results chan *chunk.Chunk

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

	taskCh        <-chan *lookUpMergeJoinTask
	outerMergeCtx outerMergeCtx
	ctx           sessionctx.Context
	innerExec     Executor
	joiner        joiner
	retFieldTypes []*types.FieldType

	maxChunkSize          int
	indexRanges           []*ranger.Range
	nextColCompareFilters *plannercore.ColWithCmpFuncManager
	keyOff2IdxOff         []int
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
	// The trick here is `getStartTS` will cache start ts in the dataReaderBuilder,
	// so even txn is destroyed later, the dataReaderBuilder could still use the
	// cached start ts to construct DAG.
	_, err := e.innerMergeCtx.readerBuilder.getStartTS()
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

func (e *IndexLookUpMergeJoin) startWorkers(ctx context.Context) {
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	resultCh := make(chan *lookUpMergeJoinTask, concurrency)
	e.resultCh = resultCh
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *lookUpMergeJoinTask, concurrency)
	e.workerWg.Add(1)
	go e.newOuterWorker(resultCh, innerCh).run(workerCtx, e.workerWg)
	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.newInnerMergeWorker(innerCh, i).run(workerCtx, e.workerWg)
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
	return omw
}

func (e *IndexLookUpMergeJoin) newInnerMergeWorker(taskCh chan *lookUpMergeJoinTask, workID int) *innerMergeWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	imw := &innerMergeWorker{
		innerMergeCtx:         e.innerMergeCtx,
		outerMergeCtx:         e.outerMergeCtx,
		taskCh:                taskCh,
		ctx:                   e.ctx,
		indexRanges:           copiedRanges,
		nextColCompareFilters: e.lastColHelper,
		keyOff2IdxOff:         e.keyOff2IdxOff,
		joiner:                e.joiners[workID],
		retFieldTypes:         e.retFieldTypes,
		maxChunkSize:          e.maxChunkSize,
	}
	return imw
}

// Next implements the Executor interface
func (e *IndexLookUpMergeJoin) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() {
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	if e.isOuterJoin {
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()
	task := e.getFinishedTask(ctx)
	for task != nil {
		select {
		case chk := <-task.results:
			req.Append(chk, 0, chk.NumRows())
			return nil
		case err := <-task.doneErr:
			if err != nil {
				return err
			}
			task = e.getFinishedTask(ctx)
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

func (e *IndexLookUpMergeJoin) getFinishedTask(ctx context.Context) *lookUpMergeJoinTask {
	task := e.task
	if task != nil {
		if len(task.results) > 0 {
			return task
		}
	}

	select {
	case task = <-e.resultCh:
	case <-ctx.Done():
		return nil
	}
	if task == nil {
		return nil
	}

	e.task = task
	return task
}

func (omw *outerMergeWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			err := errors.Errorf("%v", r)
			logutil.Logger(context.Background()).Error("outerMergeWorker panic in the recoverable goroutine", zap.Error(err))
			task := &lookUpMergeJoinTask{
				results: make(chan *chunk.Chunk, 1),
				doneErr: make(chan error, 1),
			}
			task.doneErr <- err
			omw.pushToChan(ctx, task, omw.resultCh)
		}
		close(omw.resultCh)
		close(omw.innerCh)
		wg.Done()
	}()
	for {
		task, err := omw.buildTask(ctx)
		if err != nil {
			task.doneErr <- err
			omw.pushToChan(ctx, task, omw.resultCh)
			return
		}
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

func (omw *outerMergeWorker) pushToChan(ctx context.Context, task *lookUpMergeJoinTask, dst chan<- *lookUpMergeJoinTask) bool {
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
	newFirstChunk(omw.executor)
	task := &lookUpMergeJoinTask{
		results:     make(chan *chunk.Chunk, 4),
		doneErr:     make(chan error, 1),
		outerResult: newFirstChunk(omw.executor),
	}
	task.memTracker = memory.NewTracker(stringutil.MemoizeStr(func() string { return fmt.Sprintf("lookup join task %p", task) }), -1)
	task.memTracker.AttachTo(omw.parentMemTracker)

	omw.increaseBatchSize()
	if omw.lookup.isOuterJoin { // if is outerJoin, push the requiredRows down
		requiredRows := int(atomic.LoadInt64(&omw.lookup.requiredRows))
		task.outerResult.SetRequiredRows(requiredRows, omw.maxBatchSize)
	} else {
		task.outerResult.SetRequiredRows(omw.batchSize, omw.maxBatchSize)
	}

	task.memTracker.Consume(task.outerResult.MemoryUsage())
	oldMemUsage := task.outerResult.MemoryUsage()
	err := Next(ctx, omw.executor, task.outerResult)
	if err != nil {
		return task, err
	}

	newMemUsage := task.outerResult.MemoryUsage()
	task.memTracker.Consume(newMemUsage - oldMemUsage)
	if task.outerResult == nil || task.outerResult.NumRows() == 0 {
		return nil, nil
	}
	if omw.outerMergeCtx.needOuterSort {
		c := newFirstChunk(omw.executor)
		var rows []chunk.Row
		for i := 0; i < task.outerResult.NumRows(); i++ {
			rows = append(rows, task.outerResult.GetRow(i))
		}
		sort.Slice(rows, func(i, j int) bool {
			for id, joinKey := range omw.joinKeys {
				cmp, _, err := omw.compareFuncs[id](omw.ctx, joinKey, joinKey, rows[i], rows[j])
				terror.Log(err)
				if cmp != 0 || omw.nextColCompareFilters == nil {
					return cmp < 0
				}
				return omw.nextColCompareFilters.CompareRow(rows[i], rows[j]) < 0
			}
			return false
		})
		for i := 0; i < len(rows); i++ {
			c.AppendRow(rows[i])
		}
		task.outerResult = c
	}

	if omw.filter != nil {
		var err error
		task.outerMatch, err = expression.VectorizedFilter(omw.ctx, omw.filter, chunk.NewIterator4Chunk(task.outerResult), task.outerMatch)
		if err != nil {
			return task, err
		}
		task.memTracker.Consume(int64(cap(task.outerMatch)))
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

func (imw *innerMergeWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	var task *lookUpMergeJoinTask
	defer func() {
		if r := recover(); r != nil {
			err := errors.Errorf("%v", r)
			logutil.Logger(context.Background()).Error("innerMergeWorker panic in the recoverable goroutine", zap.Error(err))
			task.doneErr = make(chan error, 1)
			task.doneErr <- err
		}
		wg.Done()
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
		task.doneErr <- err
	}
}

func (imw *innerMergeWorker) handleTask(ctx context.Context, task *lookUpMergeJoinTask) error {
	dLookUpKeys, err := imw.constructDatumLookupKeys(task)
	if err != nil {
		return err
	}
	dLookUpKeys = imw.dedupDatumLookUpKeys(dLookUpKeys)
	imw.innerExec, err = imw.readerBuilder.buildExecutorForIndexJoin(ctx, dLookUpKeys, imw.indexRanges, imw.keyOff2IdxOff, imw.nextColCompareFilters)
	if err != nil {
		return err
	}
	defer terror.Call(imw.innerExec.Close)
	_, err = imw.fetchInnerResult(ctx, task)
	if err != nil {
		return err
	}
	err = imw.doMergeJoin(ctx, task)
	return err
}

func (imw *innerMergeWorker) checkChunkIsFull(ctx context.Context, task *lookUpMergeJoinTask, chk **chunk.Chunk) bool {
	if (*chk).IsFull() {
		select {
		case task.results <- *chk:
		case <-ctx.Done():
			return true
		}
		*chk = chunk.NewChunkWithCapacity(imw.retFieldTypes, imw.maxChunkSize)
	}
	return false
}

func (imw *innerMergeWorker) doMergeJoin(ctx context.Context, task *lookUpMergeJoinTask) (err error) {
	chk := chunk.NewChunkWithCapacity(imw.retFieldTypes, imw.maxChunkSize)
	defer func() {
		if chk.NumRows() > 0 {
			select {
			case task.results <- chk:
			case <-ctx.Done():
				return
			}
		}
	}()

	task.outerIter = chunk.NewIterator4Chunk(task.outerResult)
	if task.innerResult == nil || task.innerResult.NumRows() == 0 {
		for outerRow := task.outerIter.Begin(); outerRow != task.outerIter.End(); outerRow = task.outerIter.Next() {
			imw.joiner.onMissMatch(false, outerRow, chk)
			if done := imw.checkChunkIsFull(ctx, task, &chk); done {
				return nil
			}
		}
		return
	}

	task.sameKeyIter = chunk.NewIterator4Slice(task.sameKeyRows)
	initCmpResult := 1
	if imw.innerMergeCtx.desc {
		initCmpResult = -1
	}

	for outerRow := task.outerIter.Begin(); outerRow != task.outerIter.End(); outerRow = task.outerIter.Next() {
		if len(task.outerMatch) > 0 && !task.outerMatch[outerRow.Idx()] {
			continue
		}
		cmpResult := initCmpResult
		if len(task.sameKeyRows) > 0 {
			cmpResult, err = imw.compare(outerRow, task.sameKeyIter.Begin())
			if err != nil {
				return err
			}
		}
		if (cmpResult > 0 && !imw.innerMergeCtx.desc) || (cmpResult < 0 && imw.innerMergeCtx.desc) {
			err = imw.fetchNextInnerRows(ctx, task, outerRow)
			if err != nil {
				return err
			}
		}

		for task.sameKeyIter.Current() != task.sameKeyIter.End() {
			matched, isNull, err := imw.joiner.tryToMatchInners(outerRow, task.sameKeyIter, chk)
			if err != nil {
				return err
			}

			task.hasMatch = task.hasMatch || matched
			task.hasNull = task.hasNull || isNull
			if done := imw.checkChunkIsFull(ctx, task, &chk); done {
				return nil
			}
		}

		if !task.hasMatch {
			imw.joiner.onMissMatch(task.hasNull, outerRow, chk)
		}
		task.hasMatch = false
		task.hasNull = false
		if done := imw.checkChunkIsFull(ctx, task, &chk); done {
			return nil
		}
	}

	return nil
}

func (imw *innerMergeWorker) fetchNextInnerRows(ctx context.Context, task *lookUpMergeJoinTask, key chunk.Row) (err error) {
	if task.innerResult == nil {
		return nil
	}
	task.sameKeyRows = task.sameKeyRows[:0]
	defer func() {
		task.sameKeyIter = chunk.NewIterator4Slice(task.sameKeyRows)
		task.sameKeyIter.Begin()
	}()

	curRow := task.innerIter.Current()
	if curRow == task.innerIter.End() {
		curRow, err = imw.fetchInnerResult(ctx, task)
		if err != nil || task.innerResult == nil || task.innerResult.NumRows() == 0 {
			return
		}
		curRow = task.innerIter.Current()
	}

	var cmpRes int
	for cmpRes, err = imw.compare(key, curRow); ((cmpRes >= 0 && !imw.desc) || (cmpRes <= 0 && imw.desc)) && err == nil; cmpRes, err = imw.compare(key, curRow) {
		if cmpRes == 0 {
			task.sameKeyRows = append(task.sameKeyRows, curRow)
		}
		if curRow = task.innerIter.Next(); curRow == task.innerIter.End() {
			curRow, err = imw.fetchInnerResult(ctx, task)
			if err != nil || task.innerResult == nil || task.innerResult.NumRows() == 0 {
				break
			}
		}
	}
	return
}

func (imw *innerMergeWorker) compare(outerRow, innerRow chunk.Row) (int, error) {
	for i := 0; i < len(imw.outerMergeCtx.joinKeys); i++ {
		cmp, _, err := imw.innerMergeCtx.compareFuncs[i](imw.ctx, imw.outerMergeCtx.joinKeys[i], imw.innerMergeCtx.joinKeys[i], outerRow, innerRow)
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return int(cmp), nil
		}
	}
	return 0, nil
}

func (imw *innerMergeWorker) constructDatumLookupKeys(task *lookUpMergeJoinTask) ([]*indexJoinLookUpContent, error) {
	dLookUpKeys := make([]*indexJoinLookUpContent, 0, task.outerResult.NumRows())
	for i := 0; i < task.outerResult.NumRows(); i++ {
		dLookUpKey, err := imw.constructDatumLookupKey(task, i)
		if err != nil {
			return nil, err
		}
		if dLookUpKey == nil {
			continue
		}

		if imw.hasPrefixCol {
			for i := range imw.outerMergeCtx.keyCols {
				// If it's a prefix column. Try to fix it.
				if imw.colLens[i] != types.UnspecifiedLength {
					ranger.CutDatumByPrefixLen(&dLookUpKey.keys[i], imw.colLens[i], imw.rowTypes[imw.keyCols[i]])
				}
			}
		}
		dLookUpKeys = append(dLookUpKeys, dLookUpKey)
	}

	return dLookUpKeys, nil
}

func (imw *innerMergeWorker) constructDatumLookupKey(task *lookUpMergeJoinTask, rowIdx int) (*indexJoinLookUpContent, error) {
	if task.outerMatch != nil && !task.outerMatch[rowIdx] {
		return nil, nil
	}
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
	// If the order requires descent, the deDupedLookUpContents is keep descent order before.
	// So at the end, we should generate the ascent deDupedLookUpContents to build the correct range for inner read.
	if !imw.outerMergeCtx.needOuterSort && imw.desc {
		lenKeys := len(deDupedLookUpContents)
		for i := 0; i < lenKeys/2; i++ {
			deDupedLookUpContents[i], deDupedLookUpContents[lenKeys-i-1] = deDupedLookUpContents[lenKeys-i-1], deDupedLookUpContents[i]
		}
	}
	return deDupedLookUpContents
}

func (imw *innerMergeWorker) fetchInnerResult(ctx context.Context, task *lookUpMergeJoinTask) (beginRow chunk.Row, err error) {
	task.innerResult = chunk.NewChunkWithCapacity(retTypes(imw.innerExec), imw.ctx.GetSessionVars().MaxChunkSize)
	err = Next(ctx, imw.innerExec, task.innerResult)
	if task.innerResult != nil {
		task.innerIter = chunk.NewIterator4Chunk(task.innerResult)
		beginRow = task.innerIter.Begin()
	}
	return
}

// Close implements the Executor interface.
func (e *IndexLookUpMergeJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.workerWg.Wait()
	e.memTracker = nil
	return e.baseExecutor.Close()
}
