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
	"io"
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

	tblWorkerWg sync.WaitGroup

	workerStarted bool
	keyRanges     [][]kv.KeyRange

	resultReader queueReader
	resultCurr   *lookupTableTask
	feedbacks    []*statistics.QueryFeedback

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
	return nil
}

func (e *IndexMergeReaderExecutor) startWorkers(ctx context.Context) error {
	partialReader, err := e.startPartialWorkers(ctx)
	if err != nil {
		return err
	}
	unionReader, err := e.startIndexMergeProcessWorker(ctx, partialReader)
	if err != nil {
		partialReader.Close(err)
		return err
	}
	finalReader, err := e.startIndexMergeTableScanWorker(ctx, unionReader)
	if err != nil {
		unionReader.Close(err)
		return err
	}
	e.resultReader = finalReader
	e.workerStarted = true
	return nil
}

func (e *IndexMergeReaderExecutor) startPartialWorkers(ctx context.Context) (queueReader, error) {
	var err error
	downstreamReader, downstreamWriter := newTaskQueue(len(e.keyRanges))
	downstreamWriter = multiCloser(downstreamWriter, len(e.keyRanges))
	for i := 0; i < len(e.keyRanges); i++ {
		if e.indexes[i] != nil {
			err = e.startPartialIndexWorker(ctx, downstreamWriter, i, e.keyRanges[i])
		} else {
			err = e.startPartialTableWorker(ctx, downstreamWriter, i)
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		downstreamReader.Close(err)
		return nil, err
	}
	return downstreamReader, nil
}

func (e *IndexMergeReaderExecutor) startIndexMergeProcessWorker(ctx context.Context, upstream queueReader) (queueReader, error) {
	idxMergeProcessWorker := &indexMergeProcessWorker{}
	downstreamReader, downstreamWriter := newTaskQueue(1)
	go func() {
		err := idxMergeProcessWorker.fetchLoopWithRecovery(ctx, upstream, downstreamWriter)
		downstreamWriter.Close(err)
		upstream.Close(err)
	}()
	return downstreamReader, nil
}

func (e *IndexMergeReaderExecutor) startPartialIndexWorker(ctx context.Context, downstream queueWriter, workID int, keyRange []kv.KeyRange) error {
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

	failpoint.Inject("startPartialIndexWorkerErr", func() {
		failpoint.Return(errors.New("inject an error before start partialIndexWorker"))
	})

	go func() {
		ctx1, cancel := context.WithCancel(ctx)
		err := worker.fetchHandlesWithRecovery(ctx1, result, downstream)
		if err != nil {
			e.feedbacks[workID].Invalidate()
		}
		downstream.Close(err)
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

func (e *IndexMergeReaderExecutor) startPartialTableWorker(ctx context.Context, downstream queueWriter, workID int) error {
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
		ctx1, cancel := context.WithCancel(ctx)
		err := worker.fetchHandlesWithRecovery(ctx1, downstream)
		if err != nil {
			e.feedbacks[workID].Invalidate()
		}
		downstream.Close(err)
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

func (w *partialTableWorker) fetchHandlesWithRecovery(ctx context.Context, downstream queueWriter) (err error) {
	util.WithRecovery(
		func() { err = w.fetchHandles(ctx, downstream) },
		handlePanic(&err, "partialTableWorker"),
	)
	return
}

func (w *partialTableWorker) fetchHandles(ctx context.Context, downstream queueWriter) error {
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
		return errors.Errorf("cannot find the column for handle")
	}

	chk = chunk.NewChunkWithCapacity(retTypes(w.tableReader), w.maxChunkSize)
	for {
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, handleOffset)
		if err != nil || len(handles) == 0 {
			return err
		}
		task := w.buildTableTask(handles, retChunk)
		if err = downstream.Write(task); err != nil {
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

func (e *IndexMergeReaderExecutor) startIndexMergeTableScanWorker(ctx context.Context, upstream queueReader) (queueReader, error) {
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency
	capacity := int(atomic.LoadInt32(&LookupTableTaskChannelSize))
	downstreamReader, downstreamWriter := newTaskQueue(capacity)
	downstreamWriter = multiCloser(downstreamWriter, lookupConcurrencyLimit)
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		worker := &indexMergeTableScanWorker{
			upstream:       upstream,
			downstream:     downstreamWriter,
			buildTblReader: e.buildFinalTableReader,
			tblPlans:       e.tblPlans,
			memTracker:     memory.NewTracker(stringutil.MemoizeStr(func() string { return "TableWorker_" + strconv.Itoa(i) }), -1),
		}
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			err := worker.execWithRecovery(ctx1)
			worker.downstream.Close(err)
			if err != nil {
				worker.upstream.Close(err)
			}
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
	return downstreamReader, nil
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
	task, err := e.resultReader.Read()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
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

func handlePanic(err *error, worker string) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			return
		}
		*err = errors.Errorf("panic in %s: %v", worker, r)
	}
}

// Close implements Exec Close interface.
func (e *IndexMergeReaderExecutor) Close() error {
	if e.workerStarted {
		e.resultReader.Close(nil)
	}
	e.tblWorkerWg.Wait()
	e.workerStarted = false
	// TODO: how to store e.feedbacks
	return nil
}

type indexMergeProcessWorker struct {
}

func (w *indexMergeProcessWorker) fetchLoopWithRecovery(ctx context.Context, upstream queueReader, downstream queueWriter) (err error) {
	util.WithRecovery(
		func() { err = w.fetchLoop(ctx, upstream, downstream) },
		handlePanic(&err, "indexMergeProcessWorker"),
	)
	return
}

func (w *indexMergeProcessWorker) fetchLoop(ctx context.Context, upstream queueReader, downstream queueWriter) error {
	distinctHandles := set.NewInt64Set()
	failpoint.Inject("indexMergeProcessWorkerPanic", nil)
	for {
		task, err := upstream.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		fhs := make([]int64, 0, 8)
		for _, h := range task.handles {
			if !distinctHandles.Exist(h) {
				fhs = append(fhs, h)
				distinctHandles.Insert(h)
			}
		}
		if len(fhs) == 0 {
			continue
		}
		task = &lookupTableTask{
			handles: fhs,
			doneCh:  make(chan error, 1),
		}
		if err = downstream.Write(task); err != nil {
			return err
		}
	}
}

type partialIndexWorker struct {
	batchSize    int
	maxBatchSize int
	maxChunkSize int
}

func (w *partialIndexWorker) fetchHandlesWithRecovery(ctx context.Context, result distsql.SelectResult, downstream queueWriter) (err error) {
	util.WithRecovery(
		func() { err = w.fetchHandles(ctx, result, downstream) },
		handlePanic(&err, "partialIndexWorker"),
	)
	return
}

func (w *partialIndexWorker) fetchHandles(ctx context.Context, result distsql.SelectResult, downstream queueWriter) error {
	failpoint.Inject("indexMergePartialIndexWorkerPanic", nil)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, w.maxChunkSize)
	for {
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, result)
		if err != nil || len(handles) == 0 {
			return err
		}
		task := w.buildTableTask(handles, retChunk)
		if err = downstream.Write(task); err != nil {
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
	upstream       queueReader
	downstream     queueWriter
	buildTblReader func(ctx context.Context, handles []int64) (Executor, error)
	tblPlans       []plannercore.PhysicalPlan

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker
}

func (w *indexMergeTableScanWorker) execWithRecovery(ctx context.Context) (err error) {
	util.WithRecovery(
		func() { err = w.pickAndExecTask(ctx) },
		handlePanic(&err, "indexMergeTableScanWorker"),
	)
	return
}

func (w *indexMergeTableScanWorker) pickAndExecTask(ctx context.Context) error {
	failpoint.Inject("indexMergeTableScanWorkerPanic", nil)
	for {
		task, err := w.upstream.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = w.downstream.Write(task); err != nil {
			return err
		}
		err = w.executeTask(ctx, task)
		task.doneCh <- err
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

// onceError is an object that will only store an error once.
type onceError struct {
	sync.Mutex // guards following
	err        error
}

func (a *onceError) Store(err error) {
	a.Lock()
	defer a.Unlock()
	if a.err != nil {
		return
	}
	a.err = err
}

func (a *onceError) Load() error {
	a.Lock()
	defer a.Unlock()
	return a.err
}

var errClosedQueue = errors.New("closed i/o task queue")

type taskQueue struct {
	wrChOnce sync.Once // Protects closing wrCh
	wrCh     chan *lookupTableTask

	doneOnce sync.Once // Protects closing done
	done     chan struct{}

	rerr onceError
	werr onceError
}

func (p *taskQueue) readCloseError() error {
	rerr := p.rerr.Load()
	if werr := p.werr.Load(); rerr == nil && werr != nil {
		return werr
	}
	return errClosedQueue
}

func (p *taskQueue) Read() (*lookupTableTask, error) {
	task, ok := <-p.wrCh
	if !ok {
		return nil, p.readCloseError()
	}
	return task, nil
}

func (p *taskQueue) CloseWrite(err error) {
	if err == nil {
		err = io.EOF
	}
	p.werr.Store(err)
	p.wrChOnce.Do(func() { close(p.wrCh) })
}

func (p *taskQueue) writeCloseError() error {
	werr := p.werr.Load()
	if rerr := p.rerr.Load(); werr == nil && rerr != nil {
		return rerr
	}
	return errClosedQueue
}

func (p *taskQueue) CloseRead(err error) {
	if err == nil {
		err = errClosedQueue
	}
	p.rerr.Store(err)
	p.doneOnce.Do(func() { close(p.done) })
}

func (p *taskQueue) Write(t *lookupTableTask) error {
	select {
	case p.wrCh <- t:
	case <-p.done:
		return p.writeCloseError()
	}
	return nil
}

// queueReader is the read half of a queue.
type queueReader interface {
	Read() (t *lookupTableTask, err error)
	Close(err error)
}

type defaultReader struct {
	q *taskQueue
}

// Read reads data from the queue, blocking until a writer
// arrives or the write end is closed.
// If the write end is closed with an error, that error is
// returned as err; otherwise err is EOF.
func (r *defaultReader) Read() (t *lookupTableTask, err error) {
	return r.q.Read()
}

// Close closes the reader; subsequent writes to the to the
// write half of the queue will return the error err, or errClosedPipe
// if err is nil.
//
// close never overwrites the previous error if it exists.
func (r *defaultReader) Close(err error) {
	r.q.CloseRead(err)
}

// queueWriter is the write half of a task queue.
type queueWriter interface {
	Write(task *lookupTableTask) error
	Close(err error)
}

type defaultWriter struct {
	q *taskQueue
}

// Write writes data to the queue, blocking until the read end
// is closed if queue full.
// If the read end is closed with an error, that err is
// returned as err; otherwise err is ErrClosedPipe.
func (w *defaultWriter) Write(task *lookupTableTask) error {
	return w.q.Write(task)
}

// Close closes the writer; ssubsequent reads from the read half
// of the queue will return nil and the error err, or EOF if
// err is nil.
func (w *defaultWriter) Close(err error) {
	w.q.CloseWrite(err)
}

type multiCloseWriter struct {
	n int64
	w queueWriter
}

func (w *multiCloseWriter) Write(t *lookupTableTask) error {
	return w.w.Write(t)
}

func (w *multiCloseWriter) Close(err error) {
	if atomic.AddInt64(&w.n, -1) == 0 || err != nil {
		w.w.Close(err)
	}
}

func multiCloser(w queueWriter, n int) queueWriter {
	return &multiCloseWriter{n: int64(n), w: w}
}

func newTaskQueue(cap int) (queueReader, queueWriter) {
	q := &taskQueue{
		wrCh: make(chan *lookupTableTask, cap),
		done: make(chan struct{}),
	}
	return &defaultReader{q}, &defaultWriter{q}
}
