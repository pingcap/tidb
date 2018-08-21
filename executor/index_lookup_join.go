// Copyright 2017 PingCAP, Inc.
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
	"unsafe"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/pingcap/tidb/util/ranger"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

var _ Executor = &IndexLookUpJoin{}

// IndexLookUpJoin employs one outer worker and N innerWorkers to execute concurrently.
// It preserves the order of the outer table and support batch lookup.
//
// The execution flow is very similar to IndexLookUpReader:
// 1. outerWorker read N outer rows, build a task and send it to result channel and inner worker channel.
// 2. The innerWorker receives the task, builds key ranges from outer rows and fetch inner rows, builds inner row hash map.
// 3. main thread receives the task, waits for inner worker finish handling the task.
// 4. main thread join each outer row by look up the inner rows hash map in the task.
type IndexLookUpJoin struct {
	baseExecutor

	resultCh   <-chan *lookUpJoinTask
	cancelFunc context.CancelFunc
	workerWg   *sync.WaitGroup

	outerCtx outerCtx
	innerCtx innerCtx

	task       *lookUpJoinTask
	joinResult *chunk.Chunk
	innerIter  chunk.Iterator

	joiner joiner

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
	innerPtrBytes [][]byte

	memTracker *memory.Tracker // track memory usage.
}

type outerCtx struct {
	rowTypes []*types.FieldType
	keyCols  []int
	filter   expression.CNFExprs
}

type innerCtx struct {
	readerBuilder *dataReaderBuilder
	rowTypes      []*types.FieldType
	keyCols       []int
}

type lookUpJoinTask struct {
	outerResult *chunk.Chunk
	outerMatch  []bool

	innerResult       *chunk.List
	encodedLookUpKeys *chunk.Chunk
	lookupMap         *mvmap.MVMap
	matchedInners     []chunk.Row

	doneCh   chan error
	cursor   int
	hasMatch bool

	memTracker *memory.Tracker // track memory usage.
}

type outerWorker struct {
	outerCtx

	ctx      sessionctx.Context
	executor Executor

	executorChk *chunk.Chunk

	maxBatchSize int
	batchSize    int

	resultCh chan<- *lookUpJoinTask
	innerCh  chan<- *lookUpJoinTask

	parentMemTracker *memory.Tracker
}

type innerWorker struct {
	innerCtx

	taskCh      <-chan *lookUpJoinTask
	outerCtx    outerCtx
	ctx         sessionctx.Context
	executorChk *chunk.Chunk

	indexRanges   []*ranger.Range
	keyOff2IdxOff []int
}

// Open implements the Executor interface.
func (e *IndexLookUpJoin) Open(ctx context.Context) error {
	err := e.children[0].Open(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaIndexLookupJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	e.innerPtrBytes = make([][]byte, 0, 8)
	e.startWorkers(ctx)
	return nil
}

func (e *IndexLookUpJoin) startWorkers(ctx context.Context) {
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency
	resultCh := make(chan *lookUpJoinTask, concurrency)
	e.resultCh = resultCh
	workerCtx, cancelFunc := context.WithCancel(ctx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *lookUpJoinTask, concurrency)
	e.workerWg.Add(1)
	go e.newOuterWorker(resultCh, innerCh).run(workerCtx, e.workerWg)
	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.newInnerWorker(innerCh).run(workerCtx, e.workerWg)
	}
}

func (e *IndexLookUpJoin) newOuterWorker(resultCh, innerCh chan *lookUpJoinTask) *outerWorker {
	ow := &outerWorker{
		outerCtx:         e.outerCtx,
		ctx:              e.ctx,
		executor:         e.children[0],
		executorChk:      chunk.NewChunkWithCapacity(e.outerCtx.rowTypes, e.maxChunkSize),
		resultCh:         resultCh,
		innerCh:          innerCh,
		batchSize:        32,
		maxBatchSize:     e.ctx.GetSessionVars().IndexJoinBatchSize,
		parentMemTracker: e.memTracker,
	}
	return ow
}

func (e *IndexLookUpJoin) newInnerWorker(taskCh chan *lookUpJoinTask) *innerWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.Range, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	iw := &innerWorker{
		innerCtx:      e.innerCtx,
		outerCtx:      e.outerCtx,
		taskCh:        taskCh,
		ctx:           e.ctx,
		executorChk:   chunk.NewChunkWithCapacity(e.innerCtx.rowTypes, e.maxChunkSize),
		indexRanges:   copiedRanges,
		keyOff2IdxOff: e.keyOff2IdxOff,
	}
	return iw
}

// Next implements the Executor interface.
func (e *IndexLookUpJoin) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	e.joinResult.Reset()
	for {
		task, err := e.getFinishedTask(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if task == nil {
			return nil
		}
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			e.lookUpMatchedInners(task, task.cursor)
			e.innerIter = chunk.NewIterator4Slice(task.matchedInners)
			e.innerIter.Begin()
		}

		outerRow := task.outerResult.GetRow(task.cursor)
		if e.innerIter.Current() != e.innerIter.End() {
			matched, err := e.joiner.tryToMatch(outerRow, e.innerIter, chk)
			if err != nil {
				return errors.Trace(err)
			}
			task.hasMatch = task.hasMatch || matched
		}
		if e.innerIter.Current() == e.innerIter.End() {
			if !task.hasMatch {
				e.joiner.onMissMatch(outerRow, chk)
			}
			task.cursor++
			task.hasMatch = false
		}
		if chk.NumRows() == e.maxChunkSize {
			return nil
		}
	}
}

func (e *IndexLookUpJoin) getFinishedTask(ctx context.Context) (*lookUpJoinTask, error) {
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
	return task, nil
}

func (e *IndexLookUpJoin) lookUpMatchedInners(task *lookUpJoinTask, rowIdx int) {
	outerKey := task.encodedLookUpKeys.GetRow(rowIdx).GetBytes(0)
	e.innerPtrBytes = task.lookupMap.Get(outerKey, e.innerPtrBytes[:0])
	task.matchedInners = task.matchedInners[:0]

	for _, b := range e.innerPtrBytes {
		ptr := *(*chunk.RowPtr)(unsafe.Pointer(&b[0]))
		matchedInner := task.innerResult.GetRow(ptr)
		task.matchedInners = append(task.matchedInners, matchedInner)
	}
}

func (ow *outerWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("outerWorker panic stack is:\n%s", buf)
			task := &lookUpJoinTask{doneCh: make(chan error, 1)}
			task.doneCh <- errors.Errorf("%v", r)
			ow.pushToChan(ctx, task, ow.resultCh)
		}
		close(ow.resultCh)
		close(ow.innerCh)
		wg.Done()
	}()
	for {
		task, err := ow.buildTask(ctx)
		if err != nil {
			task.doneCh <- errors.Trace(err)
			ow.pushToChan(ctx, task, ow.resultCh)
			return
		}
		if task == nil {
			return
		}

		if finished := ow.pushToChan(ctx, task, ow.innerCh); finished {
			return
		}

		if finished := ow.pushToChan(ctx, task, ow.resultCh); finished {
			return
		}
	}
}

func (ow *outerWorker) pushToChan(ctx context.Context, task *lookUpJoinTask, dst chan<- *lookUpJoinTask) bool {
	select {
	case <-ctx.Done():
		return true
	case dst <- task:
	}
	return false
}

// buildTask builds a lookUpJoinTask and read outer rows.
// When err is not nil, task must not be nil to send the error to the main thread via task.
func (ow *outerWorker) buildTask(ctx context.Context) (*lookUpJoinTask, error) {
	ow.executor.newChunk()

	task := &lookUpJoinTask{
		doneCh:            make(chan error, 1),
		outerResult:       ow.executor.newChunk(),
		encodedLookUpKeys: chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)}, ow.ctx.GetSessionVars().MaxChunkSize),
		lookupMap:         mvmap.NewMVMap(),
	}
	task.memTracker = memory.NewTracker(fmt.Sprintf("lookup join task %p", task), -1)
	task.memTracker.AttachTo(ow.parentMemTracker)

	ow.increaseBatchSize()

	task.memTracker.Consume(task.outerResult.MemoryUsage())
	for task.outerResult.NumRows() < ow.batchSize {
		err := ow.executor.Next(ctx, ow.executorChk)
		if err != nil {
			return task, errors.Trace(err)
		}
		if ow.executorChk.NumRows() == 0 {
			break
		}

		oldMemUsage := task.outerResult.MemoryUsage()
		task.outerResult.Append(ow.executorChk, 0, ow.executorChk.NumRows())
		newMemUsage := task.outerResult.MemoryUsage()
		task.memTracker.Consume(newMemUsage - oldMemUsage)
	}
	if task.outerResult.NumRows() == 0 {
		return nil, nil
	}

	if ow.filter != nil {
		outerMatch := make([]bool, 0, task.outerResult.NumRows())
		var err error
		task.outerMatch, err = expression.VectorizedFilter(ow.ctx, ow.filter, chunk.NewIterator4Chunk(task.outerResult), outerMatch)
		if err != nil {
			return task, errors.Trace(err)
		}
		task.memTracker.Consume(int64(cap(task.outerMatch)))
	}
	return task, nil
}

func (ow *outerWorker) increaseBatchSize() {
	if ow.batchSize < ow.maxBatchSize {
		ow.batchSize *= 2
	}
	if ow.batchSize > ow.maxBatchSize {
		ow.batchSize = ow.maxBatchSize
	}
}

func (iw *innerWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	var task *lookUpJoinTask
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("innerWorker panic stack is:\n%s", buf)
			// "task != nil" is guaranteed when panic happened.
			task.doneCh <- errors.Errorf("%v", r)
		}
		wg.Done()
	}()

	for ok := true; ok; {
		select {
		case task, ok = <-iw.taskCh:
			if !ok {
				return
			}
		case <-ctx.Done():
			return
		}

		err := iw.handleTask(ctx, task)
		task.doneCh <- errors.Trace(err)
	}
}

func (iw *innerWorker) handleTask(ctx context.Context, task *lookUpJoinTask) error {
	dLookUpKeys, err := iw.constructDatumLookupKeys(task)
	if err != nil {
		return errors.Trace(err)
	}
	dLookUpKeys = iw.sortAndDedupDatumLookUpKeys(dLookUpKeys)
	err = iw.fetchInnerResults(ctx, task, dLookUpKeys)
	if err != nil {
		return errors.Trace(err)
	}
	err = iw.buildLookUpMap(task)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (iw *innerWorker) constructDatumLookupKeys(task *lookUpJoinTask) ([][]types.Datum, error) {
	dLookUpKeys := make([][]types.Datum, 0, task.outerResult.NumRows())
	keyBuf := make([]byte, 0, 64)
	for i := 0; i < task.outerResult.NumRows(); i++ {
		dLookUpKey, err := iw.constructDatumLookupKey(task, i)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if dLookUpKey == nil {
			// Append null to make looUpKeys the same length as outer Result.
			task.encodedLookUpKeys.AppendNull(0)
			continue
		}
		keyBuf = keyBuf[:0]
		keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, dLookUpKey...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Store the encoded lookup key in chunk, so we can use it to lookup the matched inners directly.
		task.encodedLookUpKeys.AppendBytes(0, keyBuf)
		dLookUpKeys = append(dLookUpKeys, dLookUpKey)
	}

	task.memTracker.Consume(task.encodedLookUpKeys.MemoryUsage())
	return dLookUpKeys, nil
}

func (iw *innerWorker) constructDatumLookupKey(task *lookUpJoinTask, rowIdx int) ([]types.Datum, error) {
	if task.outerMatch != nil && !task.outerMatch[rowIdx] {
		return nil, nil
	}
	outerRow := task.outerResult.GetRow(rowIdx)
	sc := iw.ctx.GetSessionVars().StmtCtx
	keyLen := len(iw.keyCols)
	dLookupKey := make([]types.Datum, 0, keyLen)
	for i, keyCol := range iw.outerCtx.keyCols {
		outerValue := outerRow.GetDatum(keyCol, iw.outerCtx.rowTypes[keyCol])

		innerColType := iw.rowTypes[iw.keyCols[i]]
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

func (iw *innerWorker) sortAndDedupDatumLookUpKeys(dLookUpKeys [][]types.Datum) [][]types.Datum {
	if len(dLookUpKeys) < 2 {
		return dLookUpKeys
	}
	sc := iw.ctx.GetSessionVars().StmtCtx
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

func compareRow(sc *stmtctx.StatementContext, left, right []types.Datum) int {
	for idx := 0; idx < len(left); idx++ {
		cmp, err := left[idx].CompareDatum(sc, &right[idx])
		// We only compare rows with the same type, no error to return.
		terror.Log(err)
		if cmp > 0 {
			return 1
		} else if cmp < 0 {
			return -1
		}
	}
	return 0
}

func (iw *innerWorker) fetchInnerResults(ctx context.Context, task *lookUpJoinTask, dLookUpKeys [][]types.Datum) error {
	innerExec, err := iw.readerBuilder.buildExecutorForIndexJoin(ctx, dLookUpKeys, iw.indexRanges, iw.keyOff2IdxOff)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(innerExec.Close)
	innerResult := chunk.NewList(innerExec.retTypes(), iw.ctx.GetSessionVars().MaxChunkSize)
	innerResult.GetMemTracker().SetLabel("inner result")
	innerResult.GetMemTracker().AttachTo(task.memTracker)
	for {
		err := innerExec.Next(ctx, iw.executorChk)
		if err != nil {
			return errors.Trace(err)
		}
		if iw.executorChk.NumRows() == 0 {
			break
		}
		innerResult.Add(iw.executorChk)
		iw.executorChk = innerExec.newChunk()
	}
	task.innerResult = innerResult
	return nil
}

func (iw *innerWorker) buildLookUpMap(task *lookUpJoinTask) error {
	keyBuf := make([]byte, 0, 64)
	valBuf := make([]byte, 8)
	for i := 0; i < task.innerResult.NumChunks(); i++ {
		chk := task.innerResult.GetChunk(i)
		for j := 0; j < chk.NumRows(); j++ {
			innerRow := chk.GetRow(j)
			if iw.hasNullInJoinKey(innerRow) {
				continue
			}

			keyBuf = keyBuf[:0]
			for _, keyCol := range iw.keyCols {
				d := innerRow.GetDatum(keyCol, iw.rowTypes[keyCol])
				var err error
				keyBuf, err = codec.EncodeKey(iw.ctx.GetSessionVars().StmtCtx, keyBuf, d)
				if err != nil {
					return errors.Trace(err)
				}
			}
			rowPtr := chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(j)}
			*(*chunk.RowPtr)(unsafe.Pointer(&valBuf[0])) = rowPtr
			task.lookupMap.Put(keyBuf, valBuf)
		}
	}
	return nil
}

func (iw *innerWorker) hasNullInJoinKey(row chunk.Row) bool {
	for _, ordinal := range iw.keyCols {
		if row.IsNull(ordinal) {
			return true
		}
	}
	return false
}

// Close implements the Executor interface.
func (e *IndexLookUpJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.workerWg.Wait()
	e.memTracker.Detach()
	e.memTracker = nil
	return errors.Trace(e.children[0].Close())
}
