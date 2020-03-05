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
	"github.com/pingcap/tidb/sessionctx"
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
// There are two types of workers:
// 1. partialTableWorker/partialIndexWorker, which are used to fetch the handles
// 2. indexMergeTableScanWorker, which is used to get the table tuples with the given handles.
//
// The execution flow is really like IndexLookUpReader. However, it uses multiple index scans
// or table scans to get the handles:
// 1. use the partialTableWorkers and partialIndexWorkers to fetch the handles (a batch per time)
//    and send them to the handleMerger.
// 2. handleMerger do the `Union` operation for a batch of handles it have got.
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

	tblWorkerWg sync.WaitGroup

	workerStarted bool
	keyRanges     [][]kv.KeyRange
	merger        handleMerger

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
	e.resultCh = make(chan *lookupTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))
	return nil
}

func (e *IndexMergeReaderExecutor) startWorkers(ctx context.Context) error {
	var w partialWorker
	var err error
	workers := make([]partialWorker, 0, len(e.keyRanges))
	for i := 0; i < len(e.keyRanges); i++ {
		if e.indexes[i] != nil {
			w, err = e.createPartialIndexWorker(ctx, i, e.keyRanges[i])
		} else {
			w, err = e.createPartialTableWorker(ctx, i)
		}
		if err != nil {
			break
		}
		workers = append(workers, w)
	}
	if err != nil {
		for _, w := range workers {
			w.close(ctx)
		}
		return err
	}

	workCh := make(chan *lookupTableTask, 1)
	e.merger = &unionMerger{
		workers:         len(workers),
		workCh:          workCh,
		resultCh:        e.resultCh,
		distinctHandles: set.NewInt64Set(),
	}
	e.startIndexMergeTableScanWorker(ctx, workCh)
	for i, w := range workers {
		w.start(ctx, e.merger, e.feedbacks[i], e.ctx)
	}
	e.workerStarted = true
	return nil
}

func (e *IndexMergeReaderExecutor) createPartialIndexWorker(ctx context.Context, workID int, keyRange []kv.KeyRange) (partialWorker, error) {
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
		Build()
	if err != nil {
		return nil, err
	}

	result, err := distsql.SelectWithRuntimeStats(ctx, e.ctx, kvReq, []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, e.feedbacks[workID], getPhysicalPlanIDs(e.partialPlans[workID]), e.id)
	if err != nil {
		return nil, err
	}

	result.Fetch(ctx)
	worker := &partialIndexWorker{
		result:       result,
		batchSize:    e.maxChunkSize,
		maxBatchSize: e.ctx.GetSessionVars().IndexLookupSize,
		maxChunkSize: e.maxChunkSize,
	}

	if worker.batchSize > worker.maxBatchSize {
		worker.batchSize = worker.maxBatchSize
	}

	failpoint.Inject("startPartialIndexWorkerErr", func() {
		failpoint.Return(nil, errors.New("inject an error before start partialIndexWorker"))
	})

	return worker, nil
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

func (e *IndexMergeReaderExecutor) createPartialTableWorker(ctx context.Context, workID int) (partialWorker, error) {
	partialTableReader := e.buildPartialTableReader(ctx, workID)
	err := partialTableReader.Open(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("open Select result failed:", zap.Error(err))
		return nil, err
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
	return worker, nil
}

type partialTableWorker struct {
	partialWorker

	batchSize    int
	maxBatchSize int
	maxChunkSize int
	tableReader  Executor
	tableInfo    *model.TableInfo
}

func (w *partialTableWorker) start(ctx context.Context, merger handleMerger, feedback *statistics.QueryFeedback, sctx sessionctx.Context) {
	go func() {
		defer w.close(ctx)
		ctx1, cancel := context.WithCancel(ctx)
		var err error
		util.WithRecovery(
			func() { err = w.fetchHandles(ctx1, merger) },
			handleHandlesFetcherPanic(ctx, "partialTableWorker", merger),
		)
		if err != nil {
			feedback.Invalidate()
		}
		cancel()
		sctx.StoreQueryFeedback(feedback)
	}()
}

func (w *partialTableWorker) close(ctx context.Context) {
	if w.tableReader == nil {
		return
	}
	if err := w.tableReader.Close(); err != nil {
		logutil.Logger(ctx).Error("close Select result failed:", zap.Error(err))
	}
	w.tableReader = nil
}

func (w *partialTableWorker) fetchHandles(ctx context.Context, merger handleMerger) error {
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
		err := errors.Errorf("cannot find the column for handle")
		merger.finish(err)
		return err
	}

	chk = chunk.NewChunkWithCapacity(retTypes(w.tableReader), w.maxChunkSize)
	for {
		handles, _, err := w.extractTaskHandles(ctx, chk, handleOffset)
		if err != nil || len(handles) == 0 || !merger.merge(handles) {
			merger.finish(err)
			return err
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

func (e *IndexMergeReaderExecutor) startIndexMergeTableScanWorker(ctx context.Context, workCh <-chan *lookupTableTask) {
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		worker := &indexMergeTableScanWorker{
			workCh:         workCh,
			buildTblReader: e.buildFinalTableReader,
			tblPlans:       e.tblPlans,
			memTracker: memory.NewTracker(stringutil.MemoizeStr(func() string { return "TableWorker_" + strconv.Itoa(i) }),
				e.ctx.GetSessionVars().MemQuotaIndexLookupReader),
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
		feedback:     statistics.NewQueryFeedback(0, nil, 0, false),
		plans:        e.tblPlans,
	}
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

func handleHandlesFetcherPanic(ctx context.Context, worker string, merger handleMerger) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			return
		}

		err4Panic := errors.Errorf("panic in IndexMergeReaderExecutor %s: %v", worker, r)
		logutil.Logger(ctx).Error(err4Panic.Error())
		merger.finish(err4Panic)
	}
}

// Close implements Exec Close interface.
func (e *IndexMergeReaderExecutor) Close() error {
	if e.merger != nil {
		e.merger.close()
	}
	e.tblWorkerWg.Wait()
	e.workerStarted = false
	// TODO: how to store e.feedbacks
	return nil
}

type partialWorker interface {
	start(ctx context.Context, merger handleMerger, feedback *statistics.QueryFeedback, sctx sessionctx.Context)
	close(ctx context.Context)
}

type partialIndexWorker struct {
	partialWorker

	result distsql.SelectResult

	batchSize    int
	maxBatchSize int
	maxChunkSize int
}

func (w *partialIndexWorker) start(ctx context.Context, merger handleMerger, feedback *statistics.QueryFeedback, sctx sessionctx.Context) {
	go func() {
		defer w.close(ctx)

		ctx1, cancel := context.WithCancel(ctx)
		var err error
		util.WithRecovery(
			func() { err = w.fetchHandles(ctx1, merger) },
			handleHandlesFetcherPanic(ctx, "partialIndexWorker", merger),
		)
		if err != nil {
			feedback.Invalidate()
		}
		cancel()
		sctx.StoreQueryFeedback(feedback)
	}()
}

func (w *partialIndexWorker) close(ctx context.Context) {
	if w.result == nil {
		return
	}
	if err := w.result.Close(); err != nil {
		logutil.Logger(ctx).Error("close Select result failed:", zap.Error(err))
	}
	w.result = nil
}

func (w *partialIndexWorker) fetchHandles(ctx context.Context, merger handleMerger) error {
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, w.maxChunkSize)
	for {
		handles, _, err := w.extractTaskHandles(ctx, chk, w.result)
		if err != nil || len(handles) == 0 || !merger.merge(handles) {
			merger.finish(err)
			return err
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

type indexMergeTableScanWorker struct {
	workCh         <-chan *lookupTableTask
	buildTblReader func(ctx context.Context, handles []int64) (Executor, error)
	tblPlans       []plannercore.PhysicalPlan

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker
}

func (w *indexMergeTableScanWorker) pickAndExecTask(ctx context.Context) (task *lookupTableTask) {
	for task = range w.workCh {
		err := w.executeTask(ctx, task)
		task.doneCh <- err
	}
	return task
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

// handleMerger merge handles retrieved by `partialIndexWorker` or
// `partialTableWorker`, and pass the merge result to `indexMergeTableScanWorker`.
//
// The merge procedure can be a union or intersect operation. Currently, only the union
// operation was implemented in `unionMerger`.
//
// `merge` returns `true` on success and `false` on error.
//
// `finish` must be called before a partial worker quit its goroutine, either because
// finish retrieving handles or an error encountered.
//
// Once an error passed to `finish`, or `close` be called, all successive calls to `merge`
// will fail immediately, i.e, `merge` will return false.
//
// All methods should be thread-safe, no external synchronization is needed.
type handleMerger interface {
	merge(handles []int64) bool
	finish(err error)
	close()
}

// unionMerger implemented the `handleMerger` interface to do the duplicate elimination
// procedure, aka `union`.
type unionMerger struct {
	sync.Mutex
	handleMerger

	err             error
	distinctHandles set.Int64Set
	workers         int
	workCh          chan<- *lookupTableTask
	resultCh        chan<- *lookupTableTask
}

func (o *unionMerger) union(handles []int64) ([]int64, bool) {
	if o.err != nil {
		return nil, false
	}
	fhs := make([]int64, 0, 8)
	for _, h := range handles {
		if !o.distinctHandles.Exist(h) {
			fhs = append(fhs, h)
			o.distinctHandles.Insert(h)
		}
	}
	return fhs, true
}

func (o *unionMerger) merge(handles []int64) bool {
	o.Lock()
	defer o.Unlock()

	fhs, ok := o.union(handles)
	if !ok || len(fhs) == 0 {
		return ok
	}
	task := &lookupTableTask{
		handles: fhs,
		doneCh:  make(chan error, 1),
	}
	o.workCh <- task
	o.resultCh <- task
	return true
}

func (o *unionMerger) finish(err error) {
	o.Lock()
	defer o.Unlock()

	o.workers--
	if err != nil && o.err == nil {
		o.err = err
		doneCh := make(chan error, 1)
		doneCh <- err
		o.resultCh <- &lookupTableTask{
			doneCh: doneCh,
		}
	}
	if o.workers == 0 {
		close(o.resultCh)
		close(o.workCh)
	}
}

func (o *unionMerger) close() {
	o.Lock()
	defer o.Unlock()
	o.err = errors.New("closed")
}
