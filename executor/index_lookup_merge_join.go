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
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	log "github.com/sirupsen/logrus"
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
	keyCols  []int
	filter   expression.CNFExprs
}

type innerMergeCtx struct {
	readerBuilder *dataReaderBuilder
	rowTypes      []*types.FieldType
	keyCols       []int
}

type lookUpMergeJoinTask struct {
	outerResult *chunk.Chunk
	outerMatch  []bool

	innerResult *chunk.List
	innerIter   chunk.Iterator

	sameKeyRows []chunk.Row
	sameKeyIter chunk.Iterator

	doneCh      chan error
	cursor      int
	innerCursor int
	hasMatch    bool
	hasNull     bool

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
	task, err := e.getFinishedTask(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for {
		if task == nil {
			return nil
		}
		if task.cursor >= task.outerResult.NumRows() {
			task, err = e.getFinishedTask(ctx)
			if err != nil || task == nil {
				return errors.Trace(err)
			}
		}

		if task.innerIter == nil {
			task.innerIter = chunk.NewIterator4Chunk(task.innerResult.GetChunk(task.innerCursor))
			task.innerIter.Begin()
		}

		outerRow := task.outerResult.GetRow(task.cursor)
		if task.sameKeyIter == nil || task.sameKeyIter.Current() == task.sameKeyIter.End() {
			cmpResult := -1
			if len(task.outerMatch) > 0 && !task.outerMatch[task.cursor] {
				task.cursor++
				continue
			}
			if task.innerIter.Current() != task.innerIter.End() {
				cmpResult, err = e.compare(outerRow, task.innerIter.Current())
				if err != nil {
					return nil
				}
			}
			if cmpResult >= 0 {
				err = e.fetchNextOuterRows(task, outerRow)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				e.joiner.onMissMatch(false, outerRow, req.Chunk)

				task.cursor++
				task.hasMatch = false
				task.hasNull = false

				if req.NumRows() >= e.maxChunkSize {
					return nil
				}
				continue
			}
		}

		for task.sameKeyIter.Current() != task.sameKeyIter.End() {
			matched, isNull, err := e.joiner.tryToMatch(outerRow, task.sameKeyIter, req.Chunk)
			if err != nil {
				return errors.Trace(err)
			}

			task.hasMatch = task.hasMatch || matched
			task.hasNull = task.hasNull || isNull

			if req.NumRows() >= e.maxChunkSize {
				break
			}
		}

		if task.sameKeyIter.Current() == task.sameKeyIter.End() {
			if !task.hasMatch {
				e.joiner.onMissMatch(task.hasNull, outerRow, req.Chunk)
			}
			task.cursor++
			task.hasMatch = false
			task.hasNull = false
		}

		if req.Chunk.NumRows() >= e.maxChunkSize {
			return nil
		}
	}
}

func (e *IndexLookUpMergeJoin) fetchNextOuterRows(task *lookUpMergeJoinTask, key chunk.Row) error {
	task.sameKeyRows = task.sameKeyRows[:0]
	task.sameKeyIter = chunk.NewIterator4Slice(task.sameKeyRows)
	task.sameKeyIter.Begin()
	if task.innerCursor >= task.innerResult.NumChunks() {
		return nil
	}

	var err error
	var cmpRes int
	for cmpRes, err = e.compare(key, task.innerIter.Current()); cmpRes >= 0 && err == nil; cmpRes, err = e.compare(key, task.innerIter.Current()) {
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

func (e *IndexLookUpMergeJoin) compare(outerRow, innerRow chunk.Row) (int, error) {
	outerKeyCols := e.outerMergeCtx.keyCols
	innerKeyCols := e.innerMergeCtx.keyCols
	for i := 0; i < len(outerKeyCols); i++ {
		outerValue := outerRow.GetDatum(outerKeyCols[i], e.outerMergeCtx.rowTypes[outerKeyCols[i]])
		innerValue := innerRow.GetDatum(innerKeyCols[i], e.innerMergeCtx.rowTypes[innerKeyCols[i]])
		cmp, err := outerValue.CompareDatum(nil, &innerValue)
		if err != nil {
			return 0, err
		}

		if cmp != 0 {
			return int(cmp), nil
		}
	}
	return 0, nil
}

func (e *IndexLookUpMergeJoin) getFinishedTask(ctx context.Context) (*lookUpMergeJoinTask, error) {
	task := e.task
	if task != nil && task.cursor < task.outerResult.NumRows() {
		return task, nil
	}

	select {
	case task = <-e.resultCh:
	case <-ctx.Done():
		return nil, nil
	}
	if task == nil {
		return nil, nil
	}

	select {
	case err := <-task.doneCh:
		if err != nil {
			return nil, errors.Trace(err)
		}
	case <-ctx.Done():
		return nil, nil
	}

	if e.task != nil {
		e.task.memTracker.Detach()
	}
	e.task = task
	task.cursor = 0
	return task, nil
}

func (omw *outerMergeWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("outerWorker panic stack is:\n%s", buf)
			task := &lookUpMergeJoinTask{doneCh: make(chan error, 1)}
			task.doneCh <- errors.Errorf("%v", r)
			omw.pushToChan(ctx, task, omw.resultCh)
		}
		close(omw.resultCh)
		close(omw.innerCh)
		wg.Done()
	}()
	for {
		task, err := omw.buildTask(ctx)
		if err != nil {
			task.doneCh <- errors.Trace(err)
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
		doneCh:      make(chan error, 1),
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
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("innerMergeWorker panic stack is:\n%s", buf)
			task.doneCh <- errors.Errorf("%v", r)
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
		task.doneCh <- errors.Trace(err)
	}
}

func (imw *innerMergeWorker) handleTask(ctx context.Context, task *lookUpMergeJoinTask) error {
	dLookUpKeys, err := imw.constructDatumLookupKeys(task)
	if err != nil {
		return errors.Trace(err)
	}
	dLookUpKeys = imw.sortAndDedupDatumLookUpKeys(dLookUpKeys)
	err = imw.fetchInnerResults(ctx, task, dLookUpKeys)
	if err != nil {
		return errors.Trace(err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
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

func (imw *innerMergeWorker) sortAndDedupDatumLookUpKeys(dLookUpKeys [][]types.Datum) [][]types.Datum {
	if len(dLookUpKeys) < 2 {
		return dLookUpKeys
	}
	sc := imw.ctx.GetSessionVars().StmtCtx
	sort.Slice(dLookUpKeys, func(i, j int) bool {
		cmp := compareRow(sc, dLookUpKeys[i], dLookUpKeys[j])
		return cmp < 0
	})
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
