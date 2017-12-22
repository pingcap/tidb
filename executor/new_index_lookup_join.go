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
	"sort"
	"sync"
	"unsafe"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/mvmap"
	"github.com/pingcap/tidb/util/ranger"
	goctx "golang.org/x/net/context"
)

var _ Executor = &NewIndexLookUpJoin{}

// NewIndexLookUpJoin employs one outer worker and N innerWorkers to execute concurrently.
// It preserves the order of the outer table and support batch lookup.
//
// The execution flow is very similar to IndexLookUpReader:
// 1. outerWorker read N outer rows, build a task and send it to result channel and inner worker channel.
// 2. The innerWorker receives the task, builds key ranges from outer rows and fetch inner rows, builds inner row hash map.
// 3. main thread receives the task, waits for inner worker finish handling the task.
// 4. main thread join each outer row by look up the inner rows hash map in the task.
type NewIndexLookUpJoin struct {
	baseExecutor

	resultCh   <-chan *lookUpJoinTask
	cancelFunc goctx.CancelFunc
	workerWg   *sync.WaitGroup

	outerCtx outerCtx
	innerCtx innerCtx

	task             *lookUpJoinTask
	joinResult       *chunk.Chunk
	joinResultCursor int

	resultGenerator joinResultGenerator

	indexRanges   []*ranger.IndexRange
	keyOff2IdxOff []int
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
	filter        expression.CNFExprs
}

type lookUpJoinTask struct {
	outerResult *chunk.Chunk
	outerMatch  []bool

	innerResult       *chunk.List
	encodedLookUpKeys *chunk.Chunk
	lookupMap         *mvmap.MVMap
	matchedInners     []chunk.Row

	doneCh chan error
	cursor int
}

type outerWorker struct {
	outerCtx

	ctx      context.Context
	executor Executor

	executorChk *chunk.Chunk

	maxBatchSize int
	batchSize    int

	resultCh chan<- *lookUpJoinTask
	innerCh  chan<- *lookUpJoinTask
}

type innerWorker struct {
	innerCtx

	taskCh      <-chan *lookUpJoinTask
	outerCtx    outerCtx
	ctx         context.Context
	executorChk *chunk.Chunk

	indexRanges   []*ranger.IndexRange
	keyOff2IdxOff []int
}

// Open implements the Executor interface.
func (e *NewIndexLookUpJoin) Open(goCtx goctx.Context) error {
	err := e.children[0].Open(goCtx)
	if err != nil {
		return errors.Trace(err)
	}
	e.startWorkers(goCtx)
	return nil
}

func (e *NewIndexLookUpJoin) startWorkers(goCtx goctx.Context) {
	concurrency := e.ctx.GetSessionVars().IndexLookupConcurrency
	resultCh := make(chan *lookUpJoinTask, concurrency)
	e.resultCh = resultCh
	workerCtx, cancelFunc := goctx.WithCancel(goCtx)
	e.cancelFunc = cancelFunc
	innerCh := make(chan *lookUpJoinTask, concurrency)
	e.workerWg.Add(1)
	go e.newOuterWorker(resultCh, innerCh).run(workerCtx, e.workerWg)
	e.workerWg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go e.newInnerWorker(innerCh).run(workerCtx, e.workerWg)
	}
}

func (e *NewIndexLookUpJoin) newOuterWorker(resultCh, innerCh chan *lookUpJoinTask) *outerWorker {
	ow := &outerWorker{
		outerCtx:     e.outerCtx,
		ctx:          e.ctx,
		executor:     e.children[0],
		executorChk:  chunk.NewChunk(e.outerCtx.rowTypes),
		resultCh:     resultCh,
		innerCh:      innerCh,
		batchSize:    32,
		maxBatchSize: e.ctx.GetSessionVars().IndexJoinBatchSize,
	}
	return ow
}

func (e *NewIndexLookUpJoin) newInnerWorker(taskCh chan *lookUpJoinTask) *innerWorker {
	// Since multiple inner workers run concurrently, we should copy join's indexRanges for every worker to avoid data race.
	copiedRanges := make([]*ranger.IndexRange, 0, len(e.indexRanges))
	for _, ran := range e.indexRanges {
		copiedRanges = append(copiedRanges, ran.Clone())
	}
	iw := &innerWorker{
		innerCtx:      e.innerCtx,
		outerCtx:      e.outerCtx,
		taskCh:        taskCh,
		ctx:           e.ctx,
		executorChk:   chunk.NewChunk(e.innerCtx.rowTypes),
		indexRanges:   copiedRanges,
		keyOff2IdxOff: e.keyOff2IdxOff,
	}
	return iw
}

// NextChunk implements the Executor interface.
func (e *NewIndexLookUpJoin) NextChunk(goCtx goctx.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for {
		err := e.prepareJoinResult(goCtx)
		if err != nil {
			return errors.Trace(err)
		}
		if e.joinResult.NumRows() == 0 {
			return nil
		}
		for e.joinResultCursor < e.joinResult.NumRows() {
			if chk.NumRows() == e.maxChunkSize {
				return nil
			}
			chk.AppendRow(0, e.joinResult.GetRow(e.joinResultCursor))
			e.joinResultCursor++
		}
	}
}

// Next implements the Executor interface.
// Even though we only support read children in chunk mode, but we are not sure if our parents
// support chunk, so we have to implement Next.
func (e *NewIndexLookUpJoin) Next(goCtx goctx.Context) (Row, error) {
	for {
		err := e.prepareJoinResult(goCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if e.joinResult.NumRows() == 0 {
			return nil, nil
		}
		row := e.joinResult.GetRow(e.joinResultCursor)
		datumRow := make(types.DatumRow, row.Len())
		for i := 0; i < row.Len(); i++ {
			// Here if some datum is KindBytes/KindString and we don't copy it, the datums' data could be in the same space
			// since row.GetDatum() and datum.SetBytes() don't alloc new space thus causing wrong result.
			d := row.GetDatum(i, e.schema.Columns[i].RetType)
			datumRow[i] = *d.Copy()
		}
		e.joinResultCursor++
		return datumRow, nil
	}
}

func (e *NewIndexLookUpJoin) prepareJoinResult(goCtx goctx.Context) error {
	if e.joinResultCursor < e.joinResult.NumRows() {
		return nil
	}
	e.joinResult.Reset()
	e.joinResultCursor = 0
	for {
		task, err := e.getFinishedTask(goCtx)
		if err != nil {
			return errors.Trace(err)
		}
		if task == nil {
			return nil
		}
		for task.cursor < task.outerResult.NumRows() {
			e.lookUpMatchedInners(task, task.cursor)
			outerRow := task.outerResult.GetRow(task.cursor)
			task.cursor++
			err = e.resultGenerator.emitToChunk(outerRow, task.matchedInners, e.joinResult)
			if err != nil {
				return errors.Trace(err)
			}
			if e.joinResult.NumRows() > 0 {
				return nil
			}
		}
	}
}

func (e *NewIndexLookUpJoin) getFinishedTask(goCtx goctx.Context) (*lookUpJoinTask, error) {
	task := e.task
	if task != nil && task.cursor < task.outerResult.NumRows() {
		return task, nil
	}
	select {
	case task = <-e.resultCh:
	case <-goCtx.Done():
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
	case <-goCtx.Done():
		return nil, nil
	}
	e.task = task
	return task, nil
}

func (e *NewIndexLookUpJoin) lookUpMatchedInners(task *lookUpJoinTask, rowIdx int) {
	outerKey := task.encodedLookUpKeys.GetRow(rowIdx).GetBytes(0)
	innerPtrBytes := task.lookupMap.Get(outerKey)
	task.matchedInners = task.matchedInners[:0]
	for _, b := range innerPtrBytes {
		ptr := *(*chunk.RowPtr)(unsafe.Pointer(&b[0]))
		matchedInner := task.innerResult.GetRow(ptr)
		task.matchedInners = append(task.matchedInners, matchedInner)
	}
}

func (ow *outerWorker) run(goCtx goctx.Context, wg *sync.WaitGroup) {
	defer func() {
		close(ow.resultCh)
		close(ow.innerCh)
		wg.Done()
	}()
	for {
		task, err := ow.buildTask(goCtx)
		if err != nil {
			task.doneCh <- errors.Trace(err)
			return
		}
		if task == nil {
			return
		}
		select {
		case <-goCtx.Done():
			return
		case ow.resultCh <- task:
		}
		select {
		case <-goCtx.Done():
			return
		case ow.innerCh <- task:
		}
	}
}

// buildTask builds a lookUpJoinTask and read outer rows.
// When err is not nil, task must not be nil to send the error to the main thread via task.
func (ow *outerWorker) buildTask(goCtx goctx.Context) (*lookUpJoinTask, error) {
	ow.executor.newChunk()
	task := new(lookUpJoinTask)
	task.doneCh = make(chan error, 1)
	task.outerResult = ow.executor.newChunk()
	task.encodedLookUpKeys = chunk.NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeBlob)})
	task.lookupMap = mvmap.NewMVMap()
	ow.increaseBatchSize()

	for task.outerResult.NumRows() < ow.batchSize {
		err := ow.executor.NextChunk(goCtx, ow.executorChk)
		if err != nil {
			return task, errors.Trace(err)
		}
		if ow.executorChk.NumRows() == 0 {
			break
		}
		task.outerResult.Append(ow.executorChk, 0, ow.executorChk.NumRows())
	}
	if task.outerResult.NumRows() == 0 {
		return nil, nil
	}

	if ow.filter != nil {
		outerMatch := make([]bool, 0, task.outerResult.NumRows())
		var err error
		task.outerMatch, err = expression.VectorizedFilter(ow.ctx, ow.filter, task.outerResult, outerMatch)
		if err != nil {
			return task, errors.Trace(err)
		}
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

func (iw *innerWorker) run(goCtx goctx.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case task, ok := <-iw.taskCh:
			if !ok {
				return
			}
			err := iw.handleTask(goCtx, task)
			task.doneCh <- errors.Trace(err)
			if err != nil {
				return
			}
		case <-goCtx.Done():
			return
		}
	}
}

func (iw *innerWorker) handleTask(goCtx goctx.Context, task *lookUpJoinTask) error {
	dLookUpKeys, err := iw.constructDatumLookupKeys(task)
	if err != nil {
		return errors.Trace(err)
	}
	dLookUpKeys = iw.sortAndDedupDatumLookUpKeys(dLookUpKeys)
	err = iw.fetchInnerResults(goCtx, task, dLookUpKeys)
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
		keyBuf, err = codec.EncodeKey(keyBuf, dLookUpKey...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Store the encoded lookup key in chunk, so we can use it to lookup the matched inners directly.
		task.encodedLookUpKeys.AppendBytes(0, keyBuf)
		dLookUpKeys = append(dLookUpKeys, dLookUpKey)
	}
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

func (iw *innerWorker) fetchInnerResults(goCtx goctx.Context, task *lookUpJoinTask, dLookUpKeys [][]types.Datum) error {
	innerExec, err := iw.readerBuilder.buildExecutorForIndexJoin(goCtx, dLookUpKeys, iw.indexRanges, iw.keyOff2IdxOff)
	if err != nil {
		return errors.Trace(err)
	}
	defer terror.Call(innerExec.Close)
	innerResult := chunk.NewList(innerExec.Schema().GetTypes(), iw.ctx.GetSessionVars().MaxChunkSize)
	for {
		err := innerExec.NextChunk(goCtx, iw.executorChk)
		if err != nil {
			return errors.Trace(err)
		}
		if iw.executorChk.NumRows() == 0 {
			break
		}
		for row := iw.executorChk.Begin(); row != iw.executorChk.End(); row = row.Next() {
			if iw.filter != nil {
				matched, err := expression.EvalBool(iw.filter, row, iw.ctx)
				if err != nil {
					return errors.Trace(err)
				}
				if matched {
					innerResult.AppendRow(row)
				}
			} else {
				innerResult.AppendRow(row)
			}
		}
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
			keyBuf = keyBuf[:0]
			for _, keyCol := range iw.keyCols {
				d := innerRow.GetDatum(keyCol, iw.rowTypes[keyCol])
				var err error
				keyBuf, err = codec.EncodeKey(keyBuf, d)
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

// Close implements the Executor interface.
func (e *NewIndexLookUpJoin) Close() error {
	if e.cancelFunc != nil {
		e.cancelFunc()
	}
	e.workerWg.Wait()
	return e.children[0].Close()
}
