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
	contxt "context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

// IndexLookUpMergeJoin realizes IndexLookUpJoin by merge join
type IndexLookUpMergeJoin struct {
	baseExecutor

	resultCh   <-chan *lookUpMergeJoinTask
	cancelFunc context.CancelFunc
	workerWg   *sync.WaitGroup

	outerMergeCtx outerMergeCtx
	innerMergeCtx innerMergeCtx

	joiner joiner

	task *lookUpMergeJoinTask

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
	innerPtrBytes [][]byte

	memTracker *memory.Tracker // track memory usage
}

type outerMergeCtx struct {
	rowTypes []*types.FieldType
	joinKeys []*expression.Column
	keyCols  []int
	filter   expression.CNFExprs
}

type innerMergeCtx struct {
	readerBuilder  *dataReaderBuilder
	rowTypes       []*types.FieldType
	joinKeys       []*expression.Column
	keyCols        []int
	compareFuncs   []expression.CompareFunc
	keepOuterOrder bool
}

type lookUpMergeJoinTask struct {
	outerResult *chunk.Chunk
	outerMatch  []bool

	innerResult *chunk.List
	innerIter   chunk.Iterator

	sameKeyRows []chunk.Row
	sameKeyIter chunk.Iterator

	doneErr     chan error
	done        bool
	cursor      int
	innerCursor int
	hasMatch    bool
	hasNull     bool

	results chan *chunk.Chunk

	memTracker *memory.Tracker
}

type outerMergeWorker struct {
	outerMergeCtx

	ctx      sessionctx.Context
	executor Executor

	executorChk *chunk.Chunk

	maxBatchSize int
	batchSize    int

	resultCh chan<- *lookUpMergeJoinTask
	innerCh  chan<- *lookUpMergeJoinTask

	parentMemTracker *memory.Tracker
}

type innerMergeWorker struct {
	innerMergeCtx

	taskCh        <-chan *lookUpMergeJoinTask
	outerMergeCtx outerMergeCtx
	ctx           sessionctx.Context
	executorChk   *chunk.Chunk
	joiner        joiner
	retFieldTypes []*types.FieldType

	maxChunkSize  int
	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
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
		return errors.Trace(err)
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
		go e.newInnerMergeWorker(innerCh).run(workerCtx, e.workerWg)
	}
}

func (e *IndexLookUpMergeJoin) newOuterWorker(resultCh, innerCh chan *lookUpMergeJoinTask) *outerMergeWorker {
	omw := &outerMergeWorker{
		outerMergeCtx:    e.outerMergeCtx,
		ctx:              e.ctx,
		executor:         e.children[0],
		executorChk:      chunk.NewChunkWithCapacity(e.outerMergeCtx.rowTypes, e.maxChunkSize),
		resultCh:         resultCh,
		innerCh:          innerCh,
		batchSize:        32,
		maxBatchSize:     e.ctx.GetSessionVars().IndexJoinBatchSize,
		parentMemTracker: e.memTracker,
	}
	return omw
}

func (e *IndexLookUpMergeJoin) newInnerMergeWorker(taskCh chan *lookUpMergeJoinTask) *innerMergeWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	imw := &innerMergeWorker{
		innerMergeCtx: e.innerMergeCtx,
		outerMergeCtx: e.outerMergeCtx,
		taskCh:        taskCh,
		ctx:           e.ctx,
		executorChk:   chunk.NewChunkWithCapacity(e.innerMergeCtx.rowTypes, e.maxChunkSize),
		indexRanges:   copiedRanges,
		keyOff2IdxOff: e.keyOff2IdxOff,
		joiner:        e.joiner,
		retFieldTypes: e.retFieldTypes,
		maxChunkSize:  e.maxChunkSize,
	}
	return imw
}

// Next implements the Executor interface
func (e *IndexLookUpMergeJoin) Next(ctx context.Context, req *chunk.RecordBatch) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() {
			e.runtimeStats.Record(time.Since(start), req.NumRows())
		}()
	}
	req.Reset()
	task := e.getFinishedTask(ctx)
	breakFlag := false
	for task != nil && !breakFlag {
		select {
		case chk := <-task.results:
			req.Append(chk, 0, chk.NumRows())
			breakFlag = true
		case err := <-task.doneErr:
			task.done = true
			if err != nil {
				return errors.Trace(err)
			}
			task = e.getFinishedTask(ctx)
		case <-ctx.Done():
			breakFlag = true
		}
	}

	return nil
}

func (e *IndexLookUpMergeJoin) getFinishedTask(ctx context.Context) *lookUpMergeJoinTask {
	task := e.task
	if task != nil && (!task.done || len(task.results) > 0) {
		return task
	}

	select {
	case task = <-e.resultCh:
	case <-ctx.Done():
		return nil
	}
	if task == nil {
		return nil
	}

	if e.task != nil {
		e.task.memTracker.Detach()
	}
	e.task = task
	return task
}

func (omw *outerMergeWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			err := errors.Errorf("%v", r)
			logutil.Logger(contxt.Background()).Error("outerMergeWorker panic in the recoverable goroutine", zap.Error(err))
			task := &lookUpMergeJoinTask{
				results: make(chan *chunk.Chunk, 1),
				doneErr: make(chan error, 1),
				done:    true,
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
			task.doneErr <- errors.Trace(err)
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
	omw.executor.newFirstChunk()

	task := &lookUpMergeJoinTask{
		results:     make(chan *chunk.Chunk, 1),
		doneErr:     make(chan error, 1),
		outerResult: omw.executor.newFirstChunk(),
	}
	task.memTracker = memory.NewTracker(fmt.Sprintf("lookup join task %p", task), -1)
	task.memTracker.AttachTo(omw.parentMemTracker)

	omw.increaseBatchSize()

	task.memTracker.Consume(task.outerResult.MemoryUsage())
	for task.outerResult.NumRows() < omw.batchSize {
		err := omw.executor.Next(ctx, chunk.NewRecordBatch(omw.executorChk))
		if err != nil {
			return task, errors.Trace(err)
		}
		if omw.executorChk.NumRows() == 0 {
			break
		}

		oldMemUsage := task.outerResult.MemoryUsage()
		task.outerResult.Append(omw.executorChk, 0, omw.executorChk.NumRows())
		newMemUsage := task.outerResult.MemoryUsage()
		task.memTracker.Consume(newMemUsage - oldMemUsage)
	}
	if task.outerResult.NumRows() == 0 {
		return nil, nil
	}

	if omw.filter != nil {
		outerMatch := make([]bool, 0, task.outerResult.NumRows())
		var err error
		task.outerMatch, err = expression.VectorizedFilter(omw.ctx, omw.filter, chunk.NewIterator4Chunk(task.outerResult), outerMatch)
		if err != nil {
			return task, errors.Trace(err)
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
			logutil.Logger(contxt.Background()).Error("innerMergeWorker panic in the recoverable goroutine", zap.Error(err))
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
		return errors.Trace(err)
	}
	if imw.innerMergeCtx.keepOuterOrder == false {
		sc := imw.ctx.GetSessionVars().StmtCtx
		sort.Slice(dLookUpKeys, func(i, j int) bool {
			cmp := compareRow(sc, dLookUpKeys[i], dLookUpKeys[j])
			return cmp < 0
		})
	}
	dLookUpKeys = imw.dedupDatumLookUpKeys(dLookUpKeys)
	err = imw.fetchInnerResults(ctx, task, dLookUpKeys)
	if err != nil {
		return errors.Trace(err)
	}
	if err != nil {
		return errors.Trace(err)
	}

	task.results = make(chan *chunk.Chunk, task.innerResult.NumChunks()+1)
	err = imw.handleMergeJoin(ctx, task)
	return err
}

func (imw *innerMergeWorker) handleMergeJoin(ctx context.Context, task *lookUpMergeJoinTask) error {
	task.innerIter = chunk.NewIterator4Chunk(task.innerResult.GetChunk(task.innerCursor))
	task.innerIter.Begin()

	chk := chunk.NewChunkWithCapacity(imw.retFieldTypes, imw.maxChunkSize)
	var err error
	for task.cursor < task.outerResult.NumRows() {
		outerRow := task.outerResult.GetRow(task.cursor)
		if task.sameKeyIter == nil || task.sameKeyIter.Current() == task.sameKeyIter.End() {
			cmpResult := -1
			if len(task.outerMatch) > 0 && !task.outerMatch[task.cursor] {
				task.cursor++
				continue
			}
			if task.innerIter.Current() != task.innerIter.End() {
				cmpResult, err = imw.compare(outerRow, task.innerIter.Current())
				if err != nil {
					return nil
				}
			}
			if cmpResult >= 0 {
				err = imw.fetchNextOuterRows(task, outerRow)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				imw.joiner.onMissMatch(false, outerRow, chk)

				task.cursor++
				task.hasMatch = false
				task.hasNull = false

				if chk.NumRows() >= imw.maxChunkSize {
					select {
					case task.results <- chk:
					case <-ctx.Done():
						return nil
					}
					chk = chunk.NewChunkWithCapacity(imw.retFieldTypes, imw.maxChunkSize)
				}
				continue
			}
		}

		for task.sameKeyIter.Current() != task.sameKeyIter.End() {
			matched, isNull, err := imw.joiner.tryToMatch(outerRow, task.sameKeyIter, chk)
			if err != nil {
				return errors.Trace(err)
			}

			task.hasMatch = task.hasMatch || matched
			task.hasNull = task.hasNull || isNull

			if chk.NumRows() >= imw.maxChunkSize {
				select {
				case task.results <- chk:
				case <-ctx.Done():
					return nil
				}
				chk = chunk.NewChunkWithCapacity(imw.retFieldTypes, imw.maxChunkSize)
				break
			}
		}

		if task.sameKeyIter.Current() == task.sameKeyIter.End() {
			if !task.hasMatch {
				imw.joiner.onMissMatch(task.hasNull, outerRow, chk)
			}
			task.cursor++
			task.hasMatch = false
			task.hasNull = false
		}

		if chk.NumRows() >= imw.maxChunkSize {
			select {
			case task.results <- chk:
			case <-ctx.Done():
				return nil
			}
			chk = chunk.NewChunkWithCapacity(imw.retFieldTypes, imw.maxChunkSize)
		}
	}
	if chk.NumRows() > 0 {
		select {
		case task.results <- chk:
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

func (imw *innerMergeWorker) fetchNextOuterRows(task *lookUpMergeJoinTask, key chunk.Row) error {
	task.sameKeyRows = task.sameKeyRows[:0]
	task.sameKeyIter = chunk.NewIterator4Slice(task.sameKeyRows)
	task.sameKeyIter.Begin()
	if task.innerCursor >= task.innerResult.NumChunks() {
		return nil
	}

	var err error
	var cmpRes int
	for cmpRes, err = imw.compare(key, task.innerIter.Current()); cmpRes >= 0 && err == nil; cmpRes, err = imw.compare(key, task.innerIter.Current()) {
		if cmpRes == 0 {
			task.sameKeyRows = append(task.sameKeyRows, task.innerIter.Current())
		}
		task.innerIter.Next()
		if task.innerIter.Current() == task.innerIter.End() {
			task.innerCursor++
			if task.innerCursor >= task.innerResult.NumChunks() {
				break
			}
			task.innerIter = chunk.NewIterator4Chunk(task.innerResult.GetChunk(task.innerCursor))
			task.innerIter.Begin()
		}
	}
	task.sameKeyIter = chunk.NewIterator4Slice(task.sameKeyRows)
	task.sameKeyIter.Begin()

	return errors.Trace(err)
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

func (imw *innerMergeWorker) constructDatumLookupKeys(task *lookUpMergeJoinTask) ([][]types.Datum, error) {
	dLookUpKeys := make([][]types.Datum, 0, task.outerResult.NumRows())
	for i := 0; i < task.outerResult.NumRows(); i++ {
		dLookUpKey, err := imw.constructDatumLookupKey(task, i)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if dLookUpKey == nil {
			continue
		}
		dLookUpKeys = append(dLookUpKeys, dLookUpKey)
	}

	return dLookUpKeys, nil
}

func (imw *innerMergeWorker) constructDatumLookupKey(task *lookUpMergeJoinTask, rowIdx int) ([]types.Datum, error) {
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
			return nil, errors.Trace(err)
		}
		cmp, err := outerValue.CompareDatum(sc, &innerValue)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if cmp != 0 {
			// If the converted outerValue is not equal to the origin outerValue, we don't need to lookup it.
			return nil, nil
		}
		dLookupKey = append(dLookupKey, innerValue)
	}
	return dLookupKey, nil
}

func (imw *innerMergeWorker) dedupDatumLookUpKeys(dLookUpKeys [][]types.Datum) [][]types.Datum {
	if len(dLookUpKeys) < 2 {
		return dLookUpKeys
	}
	sc := imw.ctx.GetSessionVars().StmtCtx
	deDupedLookupKeys := dLookUpKeys[:1]
	for i := 1; i < len(dLookUpKeys); i++ {
		cmp := compareRow(sc, dLookUpKeys[i], dLookUpKeys[i-1])
		if cmp != 0 {
			deDupedLookupKeys = append(deDupedLookupKeys, dLookUpKeys[i])
		}
	}
	return deDupedLookupKeys
}

func (imw *innerMergeWorker) fetchInnerResults(ctx context.Context, task *lookUpMergeJoinTask, dLookUpKeys [][]types.Datum) error {
	innerExec, err := imw.readerBuilder.buildExecutorForIndexJoin(ctx, dLookUpKeys, imw.indexRanges, imw.keyOff2IdxOff)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(innerExec.Close)
	innerResult := chunk.NewList(innerExec.retTypes(), imw.ctx.GetSessionVars().MaxChunkSize, imw.ctx.GetSessionVars().MaxChunkSize)
	innerResult.GetMemTracker().SetLabel("inner result")
	innerResult.GetMemTracker().AttachTo(task.memTracker)
	for {
		err := innerExec.Next(ctx, chunk.NewRecordBatch(imw.executorChk))
		if err != nil {
			return errors.Trace(err)
		}
		if imw.executorChk.NumRows() == 0 {
			break
		}
		innerResult.Add(imw.executorChk)
		imw.executorChk = innerExec.newFirstChunk()
	}
	task.innerResult = innerResult
	return nil
}

// Close implements the Executor interface.
func (e *IndexLookUpMergeJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.workerWg.Wait()
	e.memTracker.Detach()
	e.memTracker = nil
	return errors.Trace(e.children[0].Close())
}
