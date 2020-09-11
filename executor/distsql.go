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
	"context"
	"fmt"
	"math"
	"runtime"
	"runtime/trace"
	"sort"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
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
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ Executor = &TableReaderExecutor{}
	_ Executor = &IndexReaderExecutor{}
	_ Executor = &IndexLookUpExecutor{}
)

// LookupTableTaskChannelSize represents the channel size of the index double read taskChan.
var LookupTableTaskChannelSize int32 = 50

// lookupTableTask is created from a partial result of an index request which
// contains the handles in those index keys.
type lookupTableTask struct {
	handles []kv.Handle
	rowIdx  []int // rowIdx represents the handle index for every row. Only used when keep order.
	rows    []chunk.Row
	idxRows *chunk.Chunk
	cursor  int

	doneCh chan error

	// indexOrder map is used to save the original index order for the handles.
	// Without this map, the original index order might be lost.
	// The handles fetched from index is originally ordered by index, but we need handles to be ordered by itself
	// to do table request.
	indexOrder *kv.HandleMap
	// duplicatedIndexOrder map likes indexOrder. But it's used when checkIndexValue isn't nil and
	// the same handle of index has multiple values.
	duplicatedIndexOrder *kv.HandleMap

	// memUsage records the memory usage of this task calculated by table worker.
	// memTracker is used to release memUsage after task is done and unused.
	//
	// The sequence of function calls are:
	//   1. calculate task.memUsage.
	//   2. task.memTracker = tableWorker.memTracker
	//   3. task.memTracker.Consume(task.memUsage)
	//   4. task.memTracker.Consume(-task.memUsage)
	//
	// Step 1~3 are completed in "tableWorker.executeTask".
	// Step 4   is  completed in "IndexLookUpExecutor.Next".
	memUsage   int64
	memTracker *memory.Tracker
}

func (task *lookupTableTask) Len() int {
	return len(task.rows)
}

func (task *lookupTableTask) Less(i, j int) bool {
	return task.rowIdx[i] < task.rowIdx[j]
}

func (task *lookupTableTask) Swap(i, j int) {
	task.rowIdx[i], task.rowIdx[j] = task.rowIdx[j], task.rowIdx[i]
	task.rows[i], task.rows[j] = task.rows[j], task.rows[i]
}

// Closeable is a interface for closeable structures.
type Closeable interface {
	// Close closes the object.
	Close() error
}

// closeAll closes all objects even if an object returns an error.
// If multiple objects returns error, the first error will be returned.
func closeAll(objs ...Closeable) error {
	var err error
	for _, obj := range objs {
		if obj != nil {
			err1 := obj.Close()
			if err == nil && err1 != nil {
				err = err1
			}
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// handleIsExtra checks whether this column is a extra handle column generated during plan building phase.
func handleIsExtra(col *expression.Column) bool {
	if col != nil && col.ID == model.ExtraHandleID {
		return true
	}
	return false
}

func splitRanges(ranges []*ranger.Range, keepOrder bool, desc bool) ([]*ranger.Range, []*ranger.Range) {
	if len(ranges) == 0 || ranges[0].LowVal[0].Kind() == types.KindInt64 {
		return ranges, nil
	}
	idx := sort.Search(len(ranges), func(i int) bool { return ranges[i].HighVal[0].GetUint64() > math.MaxInt64 })
	if idx == len(ranges) {
		return ranges, nil
	}
	if ranges[idx].LowVal[0].GetUint64() > math.MaxInt64 {
		signedRanges := ranges[0:idx]
		unsignedRanges := ranges[idx:]
		if !keepOrder {
			return append(unsignedRanges, signedRanges...), nil
		}
		if desc {
			return unsignedRanges, signedRanges
		}
		return signedRanges, unsignedRanges
	}
	signedRanges := make([]*ranger.Range, 0, idx+1)
	unsignedRanges := make([]*ranger.Range, 0, len(ranges)-idx)
	signedRanges = append(signedRanges, ranges[0:idx]...)
	if !(ranges[idx].LowVal[0].GetUint64() == math.MaxInt64 && ranges[idx].LowExclude) {
		signedRanges = append(signedRanges, &ranger.Range{
			LowVal:     ranges[idx].LowVal,
			LowExclude: ranges[idx].LowExclude,
			HighVal:    []types.Datum{types.NewUintDatum(math.MaxInt64)},
		})
	}
	if !(ranges[idx].HighVal[0].GetUint64() == math.MaxInt64+1 && ranges[idx].HighExclude) {
		unsignedRanges = append(unsignedRanges, &ranger.Range{
			LowVal:      []types.Datum{types.NewUintDatum(math.MaxInt64 + 1)},
			HighVal:     ranges[idx].HighVal,
			HighExclude: ranges[idx].HighExclude,
		})
	}
	if idx < len(ranges) {
		unsignedRanges = append(unsignedRanges, ranges[idx+1:]...)
	}
	if !keepOrder {
		return append(unsignedRanges, signedRanges...), nil
	}
	if desc {
		return unsignedRanges, signedRanges
	}
	return signedRanges, unsignedRanges
}

// rebuildIndexRanges will be called if there's correlated column in access conditions. We will rebuild the range
// by substitute correlated column with the constant.
func rebuildIndexRanges(ctx sessionctx.Context, is *plannercore.PhysicalIndexScan, idxCols []*expression.Column, colLens []int) (ranges []*ranger.Range, err error) {
	access := make([]expression.Expression, 0, len(is.AccessCondition))
	for _, cond := range is.AccessCondition {
		newCond, err1 := expression.SubstituteCorCol2Constant(cond)
		if err1 != nil {
			return nil, err1
		}
		access = append(access, newCond)
	}
	ranges, _, err = ranger.DetachSimpleCondAndBuildRangeForIndex(ctx, access, idxCols, colLens)
	return ranges, err
}

// IndexReaderExecutor sends dag request and reads index data from kv layer.
type IndexReaderExecutor struct {
	baseExecutor

	// For a partitioned table, the IndexReaderExecutor works on a partition, so
	// the type of this table field is actually `table.PhysicalTable`.
	table           table.Table
	index           *model.IndexInfo
	physicalTableID int64
	ranges          []*ranger.Range
	// kvRanges are only used for union scan.
	kvRanges []kv.KeyRange
	dagPB    *tipb.DAGRequest
	startTS  uint64

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result distsql.SelectResult
	// columns are only required by union scan.
	columns []*model.ColumnInfo
	// outputColumns are only required by union scan.
	outputColumns []*expression.Column

	feedback  *statistics.QueryFeedback
	streaming bool

	keepOrder bool
	desc      bool

	corColInFilter bool
	corColInAccess bool
	idxCols        []*expression.Column
	colLens        []int
	plans          []plannercore.PhysicalPlan

	memTracker *memory.Tracker

	selectResultHook // for testing
}

// Close clears all resources hold by current object.
func (e *IndexReaderExecutor) Close() error {
	err := e.result.Close()
	e.result = nil
	e.ctx.StoreQueryFeedback(e.feedback)
	return err
}

// Next implements the Executor Next interface.
func (e *IndexReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	err := e.result.Next(ctx, req)
	if err != nil {
		e.feedback.Invalidate()
	}
	return err
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open(ctx context.Context) error {
	var err error
	if e.corColInAccess {
		e.ranges, err = rebuildIndexRanges(e.ctx, e.plans[0].(*plannercore.PhysicalIndexScan), e.idxCols, e.colLens)
		if err != nil {
			return err
		}
	}
	kvRanges, err := distsql.IndexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.physicalTableID, e.index.ID, e.ranges, e.feedback)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	return e.open(ctx, kvRanges)
}

func (e *IndexReaderExecutor) open(ctx context.Context, kvRanges []kv.KeyRange) error {
	var err error
	if e.corColInFilter {
		e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.plans)
		if err != nil {
			return err
		}
	}

	if e.runtimeStats != nil {
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}
	e.kvRanges = kvRanges

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetDAGRequest(e.dagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetMemTracker(e.memTracker).
		Build()
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	e.result, err = e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	e.result.Fetch(ctx)
	return nil
}

// IndexLookUpExecutor implements double read for index scan.
type IndexLookUpExecutor struct {
	baseExecutor

	table   table.Table
	index   *model.IndexInfo
	ranges  []*ranger.Range
	dagPB   *tipb.DAGRequest
	startTS uint64
	// handleIdx is the index of handle, which is only used for case of keeping order.
	handleIdx       []int
	handleCols      []*expression.Column
	primaryKeyIndex *model.IndexInfo
	tableRequest    *tipb.DAGRequest
	// columns are only required by union scan.
	columns []*model.ColumnInfo
	*dataReaderBuilder
	// All fields above are immutable.

	idxWorkerWg sync.WaitGroup
	tblWorkerWg sync.WaitGroup
	finished    chan struct{}

	resultCh   chan *lookupTableTask
	resultCurr *lookupTableTask
	feedback   *statistics.QueryFeedback

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// checkIndexValue is used to check the consistency of the index data.
	*checkIndexValue

	kvRanges      []kv.KeyRange
	workerStarted bool

	keepOrder bool
	desc      bool

	indexStreaming bool
	tableStreaming bool

	corColInIdxSide bool
	corColInTblSide bool
	corColInAccess  bool
	idxPlans        []plannercore.PhysicalPlan
	tblPlans        []plannercore.PhysicalPlan
	idxCols         []*expression.Column
	colLens         []int
	// PushedLimit is used to skip the preceding and tailing handles when Limit is sunk into IndexLookUpReader.
	PushedLimit *plannercore.PushedDownLimit
}

type getHandleType bool

const (
	getHandleFromIndex = true
	getHandleFromTable = false
)

type checkIndexValue struct {
	idxColTps  []*types.FieldType
	idxTblCols []*table.Column
}

// Open implements the Executor Open interface.
func (e *IndexLookUpExecutor) Open(ctx context.Context) error {
	var err error
	if e.corColInAccess {
		e.ranges, err = rebuildIndexRanges(e.ctx, e.idxPlans[0].(*plannercore.PhysicalIndexScan), e.idxCols, e.colLens)
		if err != nil {
			return err
		}
	}
	sc := e.ctx.GetSessionVars().StmtCtx
	physicalID := getPhysicalTableID(e.table)
	if e.index.ID == -1 {
		e.kvRanges, err = distsql.CommonHandleRangesToKVRanges(sc, physicalID, e.ranges)
	} else {
		e.kvRanges, err = distsql.IndexRangesToKVRanges(sc, physicalID, e.index.ID, e.ranges, e.feedback)
	}
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	err = e.open(ctx)
	if err != nil {
		e.feedback.Invalidate()
	}
	return err
}

func (e *IndexLookUpExecutor) open(ctx context.Context) error {
	// We have to initialize "memTracker" and other execution resources in here
	// instead of in function "Open", because this "IndexLookUpExecutor" may be
	// constructed by a "IndexLookUpJoin" and "Open" will not be called in that
	// situation.
	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.finished = make(chan struct{})
	e.resultCh = make(chan *lookupTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))

	var err error
	if e.corColInIdxSide {
		e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.idxPlans)
		if err != nil {
			return err
		}
	}

	if e.corColInTblSide {
		e.tableRequest.Executors, _, err = constructDistExec(e.ctx, e.tblPlans)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *IndexLookUpExecutor) startWorkers(ctx context.Context, initBatchSize int) error {
	// indexWorker will write to workCh and tableWorker will read from workCh,
	// so fetching index and getting table data can run concurrently.
	workCh := make(chan *lookupTableTask, 1)
	if err := e.startIndexWorker(ctx, e.kvRanges, workCh, initBatchSize); err != nil {
		return err
	}
	e.startTableWorker(ctx, workCh)
	e.workerStarted = true
	return nil
}

func (e *IndexLookUpExecutor) isCommonHandle() bool {
	return !(len(e.handleCols) == 1 && e.handleCols[0].ID == model.ExtraHandleID) && e.table.Meta() != nil && e.table.Meta().IsCommonHandle
}

func (e *IndexLookUpExecutor) getRetTpsByHandle() []*types.FieldType {
	var tps []*types.FieldType
	if e.isCommonHandle() {
		for _, handleCol := range e.handleCols {
			tps = append(tps, handleCol.RetType)
		}
	} else {
		tps = []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	}
	if e.checkIndexValue != nil {
		tps = e.idxColTps
	}
	return tps
}

// startIndexWorker launch a background goroutine to fetch handles, send the results to workCh.
func (e *IndexLookUpExecutor) startIndexWorker(ctx context.Context, kvRanges []kv.KeyRange, workCh chan<- *lookupTableTask, initBatchSize int) error {
	if e.runtimeStats != nil {
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}

	tracker := memory.NewTracker(memory.LabelForIndexWorker, -1)
	tracker.AttachTo(e.memTracker)
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetDAGRequest(e.dagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.indexStreaming).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetMemTracker(tracker).
		Build()
	if err != nil {
		return err
	}
	tps := e.getRetTpsByHandle()
	// Since the first read only need handle information. So its returned col is only 1.
	result, err := distsql.SelectWithRuntimeStats(ctx, e.ctx, kvReq, tps, e.feedback, getPhysicalPlanIDs(e.idxPlans), e.id)
	if err != nil {
		return err
	}
	result.Fetch(ctx)
	worker := &indexWorker{
		idxLookup:       e,
		workCh:          workCh,
		finished:        e.finished,
		resultCh:        e.resultCh,
		keepOrder:       e.keepOrder,
		batchSize:       initBatchSize,
		checkIndexValue: e.checkIndexValue,
		maxBatchSize:    e.ctx.GetSessionVars().IndexLookupSize,
		maxChunkSize:    e.maxChunkSize,
		PushedLimit:     e.PushedLimit,
	}
	if worker.batchSize > worker.maxBatchSize {
		worker.batchSize = worker.maxBatchSize
	}
	e.idxWorkerWg.Add(1)
	go func() {
		defer trace.StartRegion(ctx, "IndexLookUpIndexWorker").End()
		ctx1, cancel := context.WithCancel(ctx)
		_, err := worker.fetchHandles(ctx1, result)
		if err != nil {
			e.feedback.Invalidate()
		}
		cancel()
		if err := result.Close(); err != nil {
			logutil.Logger(ctx).Error("close Select result failed", zap.Error(err))
		}
		e.ctx.StoreQueryFeedback(e.feedback)
		close(workCh)
		close(e.resultCh)
		e.idxWorkerWg.Done()
	}()
	return nil
}

// startTableWorker launchs some background goroutines which pick tasks from workCh and execute the task.
func (e *IndexLookUpExecutor) startTableWorker(ctx context.Context, workCh <-chan *lookupTableTask) {
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency()
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		workerID := i
		worker := &tableWorker{
			idxLookup:       e,
			workCh:          workCh,
			finished:        e.finished,
			buildTblReader:  e.buildTableReader,
			keepOrder:       e.keepOrder,
			handleIdx:       e.handleIdx,
			checkIndexValue: e.checkIndexValue,
			memTracker:      memory.NewTracker(workerID, -1),
		}
		worker.memTracker.AttachTo(e.memTracker)
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			defer trace.StartRegion(ctx1, "IndexLookUpTableWorker").End()
			worker.pickAndExecTask(ctx1)
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
}

func (e *IndexLookUpExecutor) buildTableReader(ctx context.Context, handles []kv.Handle) (Executor, error) {
	tableReaderExec := &TableReaderExecutor{
		baseExecutor:   newBaseExecutor(e.ctx, e.schema, 0),
		table:          e.table,
		dagPB:          e.tableRequest,
		startTS:        e.startTS,
		columns:        e.columns,
		streaming:      e.tableStreaming,
		feedback:       statistics.NewQueryFeedback(0, nil, 0, false),
		corColInFilter: e.corColInTblSide,
		plans:          e.tblPlans,
	}
	tableReaderExec.buildVirtualColumnInfo()
	tableReader, err := e.dataReaderBuilder.buildTableReaderFromHandles(ctx, tableReaderExec, handles)
	if err != nil {
		logutil.Logger(ctx).Error("build table reader from handles failed", zap.Error(err))
		return nil, err
	}
	return tableReader, nil
}

// Close implements Exec Close interface.
func (e *IndexLookUpExecutor) Close() error {
	if !e.workerStarted || e.finished == nil {
		return nil
	}

	close(e.finished)
	// Drain the resultCh and discard the result, in case that Next() doesn't fully
	// consume the data, background worker still writing to resultCh and block forever.
	for range e.resultCh {
	}
	e.idxWorkerWg.Wait()
	e.tblWorkerWg.Wait()
	e.finished = nil
	e.workerStarted = false
	e.memTracker = nil
	e.resultCurr = nil
	return nil
}

// Next implements Exec Next interface.
func (e *IndexLookUpExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.workerStarted {
		if err := e.startWorkers(ctx, req.RequiredRows()); err != nil {
			return err
		}
	}
	req.Reset()
	for {
		resultTask, err := e.getResultTask()
		if err != nil {
			return err
		}
		if resultTask == nil {
			return nil
		}
		for resultTask.cursor < len(resultTask.rows) {
			req.AppendRow(resultTask.rows[resultTask.cursor])
			resultTask.cursor++
			if req.IsFull() {
				return nil
			}
		}
	}
}

func (e *IndexLookUpExecutor) getResultTask() (*lookupTableTask, error) {
	if e.resultCurr != nil && e.resultCurr.cursor < len(e.resultCurr.rows) {
		return e.resultCurr, nil
	}
	task, ok := <-e.resultCh
	if !ok {
		return nil, nil
	}
	if err := <-task.doneCh; err != nil {
		return nil, err
	}

	// Release the memory usage of last task before we handle a new task.
	if e.resultCurr != nil {
		e.resultCurr.memTracker.Consume(-e.resultCurr.memUsage)
	}
	e.resultCurr = task
	return e.resultCurr, nil
}

// indexWorker is used by IndexLookUpExecutor to maintain index lookup background goroutines.
type indexWorker struct {
	idxLookup *IndexLookUpExecutor
	workCh    chan<- *lookupTableTask
	finished  <-chan struct{}
	resultCh  chan<- *lookupTableTask
	keepOrder bool

	// batchSize is for lightweight startup. It will be increased exponentially until reaches the max batch size value.
	batchSize    int
	maxBatchSize int
	maxChunkSize int

	// checkIndexValue is used to check the consistency of the index data.
	*checkIndexValue
	// PushedLimit is used to skip the preceding and tailing handles when Limit is sunk into IndexLookUpReader.
	PushedLimit *plannercore.PushedDownLimit
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are sent to workCh to be further processed by tableWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (w *indexWorker) fetchHandles(ctx context.Context, result distsql.SelectResult) (count uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("indexWorker in IndexLookupExecutor panicked", zap.String("stack", string(buf)))
			err4Panic := errors.Errorf("%v", r)
			doneCh := make(chan error, 1)
			doneCh <- err4Panic
			w.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			if err != nil {
				err = errors.Trace(err4Panic)
			}
		}
	}()
	retTps := w.idxLookup.getRetTpsByHandle()
	chk := chunk.NewChunkWithCapacity(retTps, w.idxLookup.maxChunkSize)
	for {
		handles, retChunk, scannedKeys, err := w.extractTaskHandles(ctx, chk, result, count)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- err
			w.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			return count, err
		}
		count += scannedKeys
		if len(handles) == 0 {
			return count, nil
		}
		task := w.buildTableTask(handles, retChunk)
		select {
		case <-ctx.Done():
			return count, nil
		case <-w.finished:
			return count, nil
		case w.workCh <- task:
			w.resultCh <- task
		}
	}
}

func (w *indexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult, count uint64) (
	handles []kv.Handle, retChk *chunk.Chunk, scannedKeys uint64, err error) {
	var handleOffset []int
	for i := range w.idxLookup.handleCols {
		handleOffset = append(handleOffset, chk.NumCols()-len(w.idxLookup.handleCols)+i)
	}
	if len(handleOffset) == 0 {
		handleOffset = []int{chk.NumCols() - 1}
	}
	handles = make([]kv.Handle, 0, w.batchSize)
	// PushedLimit would always be nil for CheckIndex or CheckTable, we add this check just for insurance.
	checkLimit := (w.PushedLimit != nil) && (w.checkIndexValue == nil)
	for len(handles) < w.batchSize {
		requiredRows := w.batchSize - len(handles)
		if checkLimit {
			if w.PushedLimit.Offset+w.PushedLimit.Count <= scannedKeys+count {
				return handles, nil, scannedKeys, nil
			}
			leftCnt := w.PushedLimit.Offset + w.PushedLimit.Count - scannedKeys - count
			if uint64(requiredRows) > leftCnt {
				requiredRows = int(leftCnt)
			}
		}
		chk.SetRequiredRows(requiredRows, w.maxChunkSize)
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return handles, nil, scannedKeys, err
		}
		if chk.NumRows() == 0 {
			return handles, retChk, scannedKeys, nil
		}
		for i := 0; i < chk.NumRows(); i++ {
			scannedKeys++
			if checkLimit {
				if (count + scannedKeys) <= w.PushedLimit.Offset {
					// Skip the preceding Offset handles.
					continue
				}
				if (count + scannedKeys) > (w.PushedLimit.Offset + w.PushedLimit.Count) {
					// Skip the handles after Offset+Count.
					return handles, nil, scannedKeys, nil
				}
			}
			h, err := w.idxLookup.getHandle(chk.GetRow(i), handleOffset, w.idxLookup.isCommonHandle(), getHandleFromIndex)
			if err != nil {
				return handles, retChk, scannedKeys, err
			}
			handles = append(handles, h)
		}
		if w.checkIndexValue != nil {
			if retChk == nil {
				retChk = chunk.NewChunkWithCapacity(w.idxColTps, w.batchSize)
			}
			retChk.Append(chk, 0, chk.NumRows())
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, scannedKeys, nil
}

func (w *indexWorker) buildTableTask(handles []kv.Handle, retChk *chunk.Chunk) *lookupTableTask {
	var indexOrder *kv.HandleMap
	var duplicatedIndexOrder *kv.HandleMap
	if w.keepOrder {
		// Save the index order.
		indexOrder = kv.NewHandleMap()
		for i, h := range handles {
			indexOrder.Set(h, i)
		}
	}

	if w.checkIndexValue != nil {
		// Save the index order.
		indexOrder = kv.NewHandleMap()
		duplicatedIndexOrder = kv.NewHandleMap()
		for i, h := range handles {
			if _, ok := indexOrder.Get(h); ok {
				duplicatedIndexOrder.Set(h, i)
			} else {
				indexOrder.Set(h, i)
			}
		}
	}

	task := &lookupTableTask{
		handles:              handles,
		indexOrder:           indexOrder,
		duplicatedIndexOrder: duplicatedIndexOrder,
		idxRows:              retChk,
	}

	task.doneCh = make(chan error, 1)
	return task
}

// tableWorker is used by IndexLookUpExecutor to maintain table lookup background goroutines.
type tableWorker struct {
	idxLookup      *IndexLookUpExecutor
	workCh         <-chan *lookupTableTask
	finished       <-chan struct{}
	buildTblReader func(ctx context.Context, handles []kv.Handle) (Executor, error)
	keepOrder      bool
	handleIdx      []int

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// checkIndexValue is used to check the consistency of the index data.
	*checkIndexValue
}

// pickAndExecTask picks tasks from workCh, and execute them.
func (w *tableWorker) pickAndExecTask(ctx context.Context) {
	var task *lookupTableTask
	var ok bool
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("tableWorker in IndexLookUpExecutor panicked", zap.String("stack", string(buf)))
			task.doneCh <- errors.Errorf("%v", r)
		}
	}()
	for {
		// Don't check ctx.Done() on purpose. If background worker get the signal and all
		// exit immediately, session's goroutine doesn't know this and still calling Next(),
		// it may block reading task.doneCh forever.
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

func (e *IndexLookUpExecutor) getHandle(row chunk.Row, handleIdx []int,
	isCommonHandle bool, tp getHandleType) (handle kv.Handle, err error) {
	if isCommonHandle {
		var handleEncoded []byte
		var datums []types.Datum
		for i, idx := range handleIdx {
			// If the new collation is enabled and the handle contains non-binary string,
			// we change the collation to "utf8mb4_bin" to avoid encode sortKey again.
			rtp := e.handleCols[i].RetType
			if collate.NewCollationEnabled() && rtp.EvalType() == types.ETString &&
				!mysql.HasBinaryFlag(rtp.Flag) && tp == getHandleFromIndex {
				origin := rtp.Collate
				rtp.Collate = charset.CollationUTF8MB4
				datums = append(datums, row.GetDatum(idx, e.handleCols[i].RetType))
				rtp.Collate = origin
				continue
			}
			datums = append(datums, row.GetDatum(idx, e.handleCols[i].RetType))
		}
		tablecodec.TruncateIndexValues(e.table.Meta(), e.primaryKeyIndex, datums)
		handleEncoded, err = codec.EncodeKey(e.ctx.GetSessionVars().StmtCtx, nil, datums...)
		if err != nil {
			return nil, err
		}
		handle, err = kv.NewCommonHandle(handleEncoded)
		if err != nil {
			return nil, err
		}
	} else {
		if len(handleIdx) == 0 {
			handle = kv.IntHandle(row.GetInt64(0))
		} else {
			handle = kv.IntHandle(row.GetInt64(handleIdx[0]))
		}
	}
	return
}

func (w *tableWorker) compareData(ctx context.Context, task *lookupTableTask, tableReader Executor) error {
	chk := newFirstChunk(tableReader)
	tblInfo := w.idxLookup.table.Meta()
	vals := make([]types.Datum, 0, len(w.idxTblCols))
	for {
		err := Next(ctx, tableReader, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			task.indexOrder.Range(func(h kv.Handle, val interface{}) bool {
				idxRow := task.idxRows.GetRow(val.(int))
				err = errors.Errorf("handle %#v, index:%#v != record:%#v", h, idxRow.GetDatum(0, w.idxColTps[0]), nil)
				return false
			})
			if err != nil {
				return err
			}
			break
		}

		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			handle, err := w.idxLookup.getHandle(row, w.handleIdx, w.idxLookup.isCommonHandle(), getHandleFromTable)
			if err != nil {
				return err
			}
			v, ok := task.indexOrder.Get(handle)
			if !ok {
				v, _ = task.duplicatedIndexOrder.Get(handle)
			}
			offset, _ := v.(int)
			task.indexOrder.Delete(handle)
			idxRow := task.idxRows.GetRow(offset)
			vals = vals[:0]
			for i, col := range w.idxTblCols {
				vals = append(vals, row.GetDatum(i, &col.FieldType))
			}
			tablecodec.TruncateIndexValues(tblInfo, w.idxLookup.index, vals)
			for i, val := range vals {
				col := w.idxTblCols[i]
				tp := &col.FieldType
				ret := chunk.Compare(idxRow, i, &val)
				if ret != 0 {
					return errors.Errorf("col %s, handle %#v, index:%#v != record:%#v", col.Name, handle, idxRow.GetDatum(i, tp), val)
				}
			}
		}
	}

	return nil
}

// executeTask executes the table look up tasks. We will construct a table reader and send request by handles.
// Then we hold the returning rows and finish this task.
func (w *tableWorker) executeTask(ctx context.Context, task *lookupTableTask) error {
	tableReader, err := w.buildTblReader(ctx, task.handles)
	if err != nil {
		logutil.Logger(ctx).Error("build table reader failed", zap.Error(err))
		return err
	}
	defer terror.Call(tableReader.Close)

	if w.checkIndexValue != nil {
		return w.compareData(ctx, task, tableReader)
	}

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

	defer trace.StartRegion(ctx, "IndexLookUpTableCompute").End()
	memUsage = int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Row{}))
	task.memUsage += memUsage
	task.memTracker.Consume(memUsage)
	if w.keepOrder {
		task.rowIdx = make([]int, 0, len(task.rows))
		for i := range task.rows {
			handle, err := w.idxLookup.getHandle(task.rows[i], w.handleIdx, w.idxLookup.isCommonHandle(), getHandleFromTable)
			if err != nil {
				return err
			}
			rowIdx, _ := task.indexOrder.Get(handle)
			task.rowIdx = append(task.rowIdx, rowIdx.(int))
		}
		memUsage = int64(cap(task.rowIdx) * 4)
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
		sort.Sort(task)
	}

	if handleCnt != len(task.rows) && !util.HasCancelled(ctx) {
		if len(w.idxLookup.tblPlans) == 1 {
			obtainedHandlesMap := kv.NewHandleMap()
			for _, row := range task.rows {
				handle, err := w.idxLookup.getHandle(row, w.handleIdx, w.idxLookup.isCommonHandle(), getHandleFromTable)
				if err != nil {
					return err
				}
				obtainedHandlesMap.Set(handle, true)
			}

			logutil.Logger(ctx).Error("inconsistent index handles", zap.String("index", w.idxLookup.index.Name.O),
				zap.Int("index_cnt", handleCnt), zap.Int("table_cnt", len(task.rows)),
				zap.String("missing_handles", fmt.Sprint(GetLackHandles(task.handles, obtainedHandlesMap))),
				zap.String("total_handles", fmt.Sprint(task.handles)))

			// table scan in double read can never has conditions according to convertToIndexScan.
			// if this table scan has no condition, the number of rows it returns must equal to the length of handles.
			return errors.Errorf("inconsistent index %s handle count %d isn't equal to value count %d",
				w.idxLookup.index.Name.O, handleCnt, len(task.rows))
		}
	}

	return nil
}

// GetLackHandles gets the handles in expectedHandles but not in obtainedHandlesMap.
func GetLackHandles(expectedHandles []kv.Handle, obtainedHandlesMap *kv.HandleMap) []kv.Handle {
	diffCnt := len(expectedHandles) - obtainedHandlesMap.Len()
	diffHandles := make([]kv.Handle, 0, diffCnt)
	var cnt int
	for _, handle := range expectedHandles {
		isExist := false
		if _, ok := obtainedHandlesMap.Get(handle); ok {
			obtainedHandlesMap.Delete(handle)
			isExist = true
		}
		if !isExist {
			diffHandles = append(diffHandles, handle)
			cnt++
			if cnt == diffCnt {
				break
			}
		}
	}

	return diffHandles
}

func getPhysicalPlanIDs(plans []plannercore.PhysicalPlan) []int {
	planIDs := make([]int, 0, len(plans))
	for _, p := range plans {
		planIDs = append(planIDs, p.ID())
	}
	return planIDs
}
