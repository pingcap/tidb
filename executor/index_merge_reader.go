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
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/set"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ Executor = &IndexMergeReaderExecutor{}
)

// IndexMergeReaderExecutor accesses a table with multiple index/table scan.
// There are three types of workers:
// 1. partialTableWorker/partialIndexWorker, which are used to fetch the handles
// 2. indexMergeProcessWorker, which is used to do the `Union` operation.
// 3. indexMergeTableScanWorker, which is used to get the table tuples with the given handles.
//
// The execution flow is really like IndexLookUpReader. However, it uses multiple index scans
// or table scans to get the handles:
// 1. use the partialTableWorkers and partialIndexWorkers to fetch the handles (a batch per time)
//    and send them to the indexMergeProcessWorker.
// 2. indexMergeProcessWorker do the `Union` operation for a batch of handles it have got.
//    For every handle in the batch:
//	  1. check whether it has been accessed.
//    2. if not, record it and send it to the indexMergeTableScanWorker.
//    3. if accessed, just ignore it.
type IndexMergeReaderExecutor struct {
	baseExecutor

	table        table.Table
	indexes      []*model.IndexInfo
	descs        []bool
	ranges       [][]*ranger.Range
	dagPBs       []*tipb.DAGRequest
	startTS      uint64
	tableRequest *tipb.DAGRequest
	// columns are only required by union scan.
	columns           []*model.ColumnInfo
	partialStreamings []bool
	tableStreaming    bool
	*dataReaderBuilder
	// All fields above are immutable.

	tblWorkerWg    sync.WaitGroup
	processWokerWg sync.WaitGroup
	finished       chan struct{}

	workerStarted bool
	keyRanges     [][]kv.KeyRange

	resultCh   chan *lookupTableTask
	resultCurr *lookupTableTask
	feedbacks  []*statistics.QueryFeedback

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// checkIndexValue is used to check the consistency of the index data.
	*checkIndexValue

	corColInIdxSide bool
	partialPlans    [][]plannercore.PhysicalPlan
	corColInTblSide bool
	tblPlans        []plannercore.PhysicalPlan
	corColInAccess  bool
	idxCols         [][]*expression.Column
	colLens         [][]int
}

// Open implements the Executor Open interface
func (e *IndexMergeReaderExecutor) Open(ctx context.Context) error {
	e.keyRanges = make([][]kv.KeyRange, 0, len(e.partialPlans))
	for i, plan := range e.partialPlans {
		_, ok := plan[0].(*plannercore.PhysicalIndexScan)
		if !ok {
			e.keyRanges = append(e.keyRanges, nil)
			continue
		}
		keyRange, err := distsql.IndexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, getPhysicalTableID(e.table), e.indexes[i].ID, e.ranges[i], e.feedbacks[i])
		if err != nil {
			return err
		}
		e.keyRanges = append(e.keyRanges, keyRange)
	}
	e.finished = make(chan struct{})
	e.resultCh = make(chan *lookupTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))
	return nil
}

func (e *IndexMergeReaderExecutor) startWorkers(ctx context.Context) error {
	exitCh := make(chan struct{})
	workCh := make(chan *lookupTableTask, 1)
	fetchCh := make(chan *lookupTableTask, len(e.keyRanges))

	e.startIndexMergeProcessWorker(ctx, workCh, fetchCh)

	var err error
	var partialWorkerWg sync.WaitGroup
	for i := 0; i < len(e.keyRanges); i++ {
		partialWorkerWg.Add(1)
		if e.indexes[i] != nil {
			err = e.startPartialIndexWorker(ctx, exitCh, fetchCh, i, &partialWorkerWg, e.keyRanges[i])
		} else {
			err = e.startPartialTableWorker(ctx, exitCh, fetchCh, i, &partialWorkerWg)
		}
		if err != nil {
			partialWorkerWg.Done()
			break
		}
	}
	go e.waitPartialWorkersAndCloseFetchChan(&partialWorkerWg, fetchCh)
	if err != nil {
		close(exitCh)
		return err
	}
	e.startIndexMergeTableScanWorker(ctx, workCh)
	e.workerStarted = true
	return nil
}

func (e *IndexMergeReaderExecutor) waitPartialWorkersAndCloseFetchChan(partialWorkerWg *sync.WaitGroup, fetchCh chan *lookupTableTask) {
	partialWorkerWg.Wait()
	close(fetchCh)
}

func (e *IndexMergeReaderExecutor) startIndexMergeProcessWorker(ctx context.Context, workCh chan<- *lookupTableTask, fetch <-chan *lookupTableTask) {
	idxMergeProcessWorker := &indexMergeProcessWorker{}
	e.processWokerWg.Add(1)
	go func() {
		util.WithRecovery(
			func() {
				idxMergeProcessWorker.fetchLoop(ctx, fetch, workCh, e.resultCh, e.finished)
			},
			idxMergeProcessWorker.handleLoopFetcherPanic(ctx, e.resultCh),
		)
		e.processWokerWg.Done()
	}()
}

func (e *IndexMergeReaderExecutor) startPartialIndexWorker(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *lookupTableTask, workID int, partialWorkerWg *sync.WaitGroup, keyRange []kv.KeyRange) error {
	if e.runtimeStats != nil {
		collExec := true
		e.dagPBs[workID].CollectExecutionSummaries = &collExec
	}

	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(keyRange).
		SetDAGRequest(e.dagPBs[workID]).
		SetStartTS(e.startTS).
		SetDesc(e.descs[workID]).
		SetKeepOrder(false).
		SetStreaming(e.partialStreamings[workID]).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetMemTracker(e.memTracker).
		Build()
	if err != nil {
		return err
	}

	result, err := distsql.SelectWithRuntimeStats(ctx, e.ctx, kvReq, []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, e.feedbacks[workID], getPhysicalPlanIDs(e.partialPlans[workID]), e.id)
	if err != nil {
		return err
	}

	result.Fetch(ctx)
	worker := &partialIndexWorker{
		batchSize:    e.maxChunkSize,
		maxBatchSize: e.ctx.GetSessionVars().IndexLookupSize,
		maxChunkSize: e.maxChunkSize,
	}

	if worker.batchSize > worker.maxBatchSize {
		worker.batchSize = worker.maxBatchSize
	}

	failpoint.Inject("startPartialIndexWorkerErr", func() error {
		return errors.New("inject an error before start partialIndexWorker")
	})

	go func() {
		defer partialWorkerWg.Done()
		ctx1, cancel := context.WithCancel(ctx)
		var err error
		util.WithRecovery(
			func() {
				_, err = worker.fetchHandles(ctx1, result, exitCh, fetchCh, e.resultCh, e.finished)
			},
			e.handleHandlesFetcherPanic(ctx, e.resultCh, "partialIndexWorker"),
		)
		if err != nil {
			e.feedbacks[workID].Invalidate()
		}
		cancel()
		if err := result.Close(); err != nil {
			logutil.Logger(ctx).Error("close Select result failed:", zap.Error(err))
		}
		e.ctx.StoreQueryFeedback(e.feedbacks[workID])
	}()

	return nil
}

func (e *IndexMergeReaderExecutor) buildPartialTableReader(ctx context.Context, workID int) Executor {
	tableReaderExec := &TableReaderExecutor{
		baseExecutor: newBaseExecutor(e.ctx, e.schema, stringutil.MemoizeStr(func() string { return e.id.String() + "_tableReader" })),
		table:        e.table,
		dagPB:        e.dagPBs[workID],
		startTS:      e.startTS,
		streaming:    e.partialStreamings[workID],
		feedback:     statistics.NewQueryFeedback(0, nil, 0, false),
		plans:        e.partialPlans[workID],
		ranges:       e.ranges[workID],
	}
	return tableReaderExec
}

func (e *IndexMergeReaderExecutor) startPartialTableWorker(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *lookupTableTask, workID int,
	partialWorkerWg *sync.WaitGroup) error {
	partialTableReader := e.buildPartialTableReader(ctx, workID)
	err := partialTableReader.Open(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("open Select result failed:", zap.Error(err))
		return err
	}
	tableInfo := e.partialPlans[workID][0].(*plannercore.PhysicalTableScan).Table
	worker := &partialTableWorker{
		batchSize:    e.maxChunkSize,
		maxBatchSize: e.ctx.GetSessionVars().IndexLookupSize,
		maxChunkSize: e.maxChunkSize,
		tableReader:  partialTableReader,
		tableInfo:    tableInfo,
	}

	if worker.batchSize > worker.maxBatchSize {
		worker.batchSize = worker.maxBatchSize
	}
	go func() {
		defer partialWorkerWg.Done()
		ctx1, cancel := context.WithCancel(ctx)
		var err error
		util.WithRecovery(
			func() {
				_, err = worker.fetchHandles(ctx1, exitCh, fetchCh, e.resultCh, e.finished)
			},
			e.handleHandlesFetcherPanic(ctx, e.resultCh, "partialTableWorker"),
		)
		if err != nil {
			e.feedbacks[workID].Invalidate()
		}
		cancel()
		if err := worker.tableReader.Close(); err != nil {
			logutil.Logger(ctx).Error("close Select result failed:", zap.Error(err))
		}
		e.ctx.StoreQueryFeedback(e.feedbacks[workID])
	}()
	return nil
}

type partialTableWorker struct {
	batchSize    int
	maxBatchSize int
	maxChunkSize int
	tableReader  Executor
	tableInfo    *model.TableInfo
}

func (w *partialTableWorker) fetchHandles(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *lookupTableTask, resultCh chan<- *lookupTableTask,
	finished <-chan struct{}) (count int64, err error) {
	var chk *chunk.Chunk
	handleOffset := -1
	if w.tableInfo.PKIsHandle {
		handleCol := w.tableInfo.GetPkColInfo()
		columns := w.tableInfo.Columns
		for i := 0; i < len(columns); i++ {
			if columns[i].Name.L == handleCol.Name.L {
				handleOffset = i
				break
			}
		}
	} else {
		return 0, errors.Errorf("cannot find the column for handle")
	}

	chk = chunk.NewChunkWithCapacity(retTypes(w.tableReader), w.maxChunkSize)
	for {
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, handleOffset)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- err
			resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			return count, err
		}
		if len(handles) == 0 {
			return count, nil
		}
		count += int64(len(handles))
		task := w.buildTableTask(handles, retChunk)
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-exitCh:
			return count, nil
		case <-finished:
			return count, nil
		case fetchCh <- task:
		}
	}
}

func (w *partialTableWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, handleOffset int) (
	handles []int64, retChk *chunk.Chunk, err error) {
	handles = make([]int64, 0, w.batchSize)
	for len(handles) < w.batchSize {
		chk.SetRequiredRows(w.batchSize-len(handles), w.maxChunkSize)
		err = errors.Trace(w.tableReader.Next(ctx, chk))
		if err != nil {
			return handles, nil, err
		}
		if chk.NumRows() == 0 {
			return handles, retChk, nil
		}
		for i := 0; i < chk.NumRows(); i++ {
			h := chk.GetRow(i).GetInt64(handleOffset)
			handles = append(handles, h)
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (w *partialTableWorker) buildTableTask(handles []int64, retChk *chunk.Chunk) *lookupTableTask {
	var indexOrder map[int64]int
	var duplicatedIndexOrder map[int64]int
	task := &lookupTableTask{
		handles:              handles,
		indexOrder:           indexOrder,
		duplicatedIndexOrder: duplicatedIndexOrder,
		idxRows:              retChk,
	}

	task.doneCh = make(chan error, 1)
	return task
}

func (e *IndexMergeReaderExecutor) startIndexMergeTableScanWorker(ctx context.Context, workCh <-chan *lookupTableTask) {
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		worker := &indexMergeTableScanWorker{
			workCh:         workCh,
			finished:       e.finished,
			buildTblReader: e.buildFinalTableReader,
			tblPlans:       e.tblPlans,
			memTracker:     memory.NewTracker(stringutil.MemoizeStr(func() string { return "TableWorker_" + strconv.Itoa(i) }), -1),
		}
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			var task *lookupTableTask
			util.WithRecovery(
				func() { task = worker.pickAndExecTask(ctx1) },
				worker.handlePickAndExecTaskPanic(ctx1, task),
			)
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
}

func (e *IndexMergeReaderExecutor) buildFinalTableReader(ctx context.Context, handles []int64) (Executor, error) {
	tableReaderExec := &TableReaderExecutor{
		baseExecutor: newBaseExecutor(e.ctx, e.schema, stringutil.MemoizeStr(func() string { return e.id.String() + "_tableReader" })),
		table:        e.table,
		dagPB:        e.tableRequest,
		startTS:      e.startTS,
		streaming:    e.tableStreaming,
		columns:      e.columns,
		feedback:     statistics.NewQueryFeedback(0, nil, 0, false),
		plans:        e.tblPlans,
	}
	tableReaderExec.buildVirtualColumnInfo()
	tableReader, err := e.dataReaderBuilder.buildTableReaderFromHandles(ctx, tableReaderExec, handles)
	if err != nil {
		logutil.Logger(ctx).Error("build table reader from handles failed", zap.Error(err))
		return nil, err
	}
	return tableReader, nil
}

// Next implements Executor Next interface.
func (e *IndexMergeReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.workerStarted {
		if err := e.startWorkers(ctx); err != nil {
			return err
		}
	}

	req.Reset()
	for {
		resultTask, err := e.getResultTask()
		if err != nil {
			return errors.Trace(err)
		}
		if resultTask == nil {
			return nil
		}
		for resultTask.cursor < len(resultTask.rows) {
			req.AppendRow(resultTask.rows[resultTask.cursor])
			resultTask.cursor++
			if req.NumRows() >= e.maxChunkSize {
				return nil
			}
		}
	}
}

func (e *IndexMergeReaderExecutor) getResultTask() (*lookupTableTask, error) {
	if e.resultCurr != nil && e.resultCurr.cursor < len(e.resultCurr.rows) {
		return e.resultCurr, nil
	}
	task, ok := <-e.resultCh
	if !ok {
		return nil, nil
	}
	if err := <-task.doneCh; err != nil {
		return nil, errors.Trace(err)
	}

	// Release the memory usage of last task before we handle a new task.
	if e.resultCurr != nil {
		e.resultCurr.memTracker.Consume(-e.resultCurr.memUsage)
	}
	e.resultCurr = task
	return e.resultCurr, nil
}

func (e *IndexMergeReaderExecutor) handleHandlesFetcherPanic(ctx context.Context, resultCh chan<- *lookupTableTask, worker string) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			return
		}

		err4Panic := errors.Errorf("panic in IndexMergeReaderExecutor %s: %v", worker, r)
		logutil.Logger(ctx).Error(err4Panic.Error())
		doneCh := make(chan error, 1)
		doneCh <- err4Panic
		resultCh <- &lookupTableTask{
			doneCh: doneCh,
		}
	}
}

// Close implements Exec Close interface.
func (e *IndexMergeReaderExecutor) Close() error {
	if e.finished == nil {
		return nil
	}
	close(e.finished)
	e.processWokerWg.Wait()
	e.tblWorkerWg.Wait()
	e.finished = nil
	e.workerStarted = false
	// TODO: how to store e.feedbacks
	return nil
}

type indexMergeProcessWorker struct {
}

func (w *indexMergeProcessWorker) fetchLoop(ctx context.Context, fetchCh <-chan *lookupTableTask,
	workCh chan<- *lookupTableTask, resultCh chan<- *lookupTableTask, finished <-chan struct{}) {
	defer func() {
		close(workCh)
		close(resultCh)
	}()

	distinctHandles := set.NewInt64Set()

	for task := range fetchCh {
		handles := task.handles
		fhs := make([]int64, 0, 8)
		for _, h := range handles {
			if !distinctHandles.Exist(h) {
				fhs = append(fhs, h)
				distinctHandles.Insert(h)
			}
		}
		if len(fhs) == 0 {
			continue
		}
		task := &lookupTableTask{
			handles: fhs,
			doneCh:  make(chan error, 1),
		}
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case workCh <- task:
			resultCh <- task
		}
	}
}

func (w *indexMergeProcessWorker) handleLoopFetcherPanic(ctx context.Context, resultCh chan<- *lookupTableTask) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			return
		}

		err4Panic := errors.Errorf("panic in IndexMergeReaderExecutor indexMergeTableWorker: %v", r)
		logutil.Logger(ctx).Error(err4Panic.Error())
		doneCh := make(chan error, 1)
		doneCh <- err4Panic
		resultCh <- &lookupTableTask{
			doneCh: doneCh,
		}
	}
}

type partialIndexWorker struct {
	batchSize    int
	maxBatchSize int
	maxChunkSize int
}

func (w *partialIndexWorker) fetchHandles(ctx context.Context, result distsql.SelectResult, exitCh <-chan struct{}, fetchCh chan<- *lookupTableTask, resultCh chan<- *lookupTableTask, finished <-chan struct{}) (count int64, err error) {
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, w.maxChunkSize)
	for {
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, result)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- err
			resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			return count, err
		}
		if len(handles) == 0 {
			return count, nil
		}
		count += int64(len(handles))
		task := w.buildTableTask(handles, retChunk)
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-exitCh:
			return count, nil
		case <-finished:
			return count, nil
		case fetchCh <- task:
		}
	}
}

func (w *partialIndexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult) (
	handles []int64, retChk *chunk.Chunk, err error) {
	handleOffset := chk.NumCols() - 1
	handles = make([]int64, 0, w.batchSize)
	for len(handles) < w.batchSize {
		chk.SetRequiredRows(w.batchSize-len(handles), w.maxChunkSize)
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return handles, nil, err
		}
		if chk.NumRows() == 0 {
			return handles, retChk, nil
		}
		for i := 0; i < chk.NumRows(); i++ {
			h := chk.GetRow(i).GetInt64(handleOffset)
			handles = append(handles, h)
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (w *partialIndexWorker) buildTableTask(handles []int64, retChk *chunk.Chunk) *lookupTableTask {
	var indexOrder map[int64]int
	var duplicatedIndexOrder map[int64]int
	task := &lookupTableTask{
		handles:              handles,
		indexOrder:           indexOrder,
		duplicatedIndexOrder: duplicatedIndexOrder,
		idxRows:              retChk,
	}

	task.doneCh = make(chan error, 1)
	return task
}

type indexMergeTableScanWorker struct {
	workCh         <-chan *lookupTableTask
	finished       <-chan struct{}
	buildTblReader func(ctx context.Context, handles []int64) (Executor, error)
	tblPlans       []plannercore.PhysicalPlan

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker
}

func (w *indexMergeTableScanWorker) pickAndExecTask(ctx context.Context) (task *lookupTableTask) {
	var ok bool
	for {
		select {
		case task, ok = <-w.workCh:
			if !ok {
				return
			}
		case <-w.finished:
			return
		}
		err := w.executeTask(ctx, task)
		task.doneCh <- err
	}
}

func (w *indexMergeTableScanWorker) handlePickAndExecTaskPanic(ctx context.Context, task *lookupTableTask) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			return
		}

		err4Panic := errors.Errorf("panic in IndexMergeReaderExecutor indexMergeTableWorker: %v", r)
		logutil.Logger(ctx).Error(err4Panic.Error())
		task.doneCh <- err4Panic
	}
}

func (w *indexMergeTableScanWorker) executeTask(ctx context.Context, task *lookupTableTask) error {
	tableReader, err := w.buildTblReader(ctx, task.handles)
	if err != nil {
		logutil.Logger(ctx).Error("build table reader failed", zap.Error(err))
		return err
	}
	defer terror.Call(tableReader.Close)
	task.memTracker = w.memTracker
	memUsage := int64(cap(task.handles) * 8)
	task.memUsage = memUsage
	task.memTracker.Consume(memUsage)
	handleCnt := len(task.handles)
	task.rows = make([]chunk.Row, 0, handleCnt)
	for {
		chk := newFirstChunk(tableReader)
		err = Next(ctx, tableReader, chk)
		if err != nil {
			logutil.Logger(ctx).Error("table reader fetch next chunk failed", zap.Error(err))
			return err
		}
		if chk.NumRows() == 0 {
			break
		}
		memUsage = chk.MemoryUsage()
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			task.rows = append(task.rows, row)
		}
	}

	memUsage = int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Row{}))
	task.memUsage += memUsage
	task.memTracker.Consume(memUsage)
	if handleCnt != len(task.rows) && len(w.tblPlans) == 1 {
		return errors.Errorf("handle count %d isn't equal to value count %d", handleCnt, len(task.rows))
	}
	return nil
}
