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
	"sync/atomic"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

var (
	_ Executor = &TableReaderExecutor{}
	_ Executor = &IndexReaderExecutor{}
	_ Executor = &IndexLookUpExecutor{}
)

// DataReader can send requests which ranges are constructed by datums.
type DataReader interface {
	Executor

	doRequestForDatums(datums [][]types.Datum, goCtx goctx.Context) error
}

// handleIsExtra checks whether this column is a extra handle column generated during plan building phase.
func handleIsExtra(col *expression.Column) bool {
	if col != nil && col.ID == model.ExtraHandleID {
		return true
	}
	return false
}

// TableReaderExecutor sends dag request and reads table data from kv layer.
type TableReaderExecutor struct {
	table     table.Table
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []types.IntColumnRange
	dagPB     *tipb.DAGRequest
	ctx       context.Context
	schema    *expression.Schema
	// columns are only required by union scan.
	columns []*model.ColumnInfo

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result        distsql.NewSelectResult
	partialResult distsql.NewPartialResult
	priority      int
}

// Schema implements the Executor Schema interface.
func (e *TableReaderExecutor) Schema() *expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *TableReaderExecutor) Close() error {
	err := closeAll(e.result, e.partialResult)
	e.result = nil
	e.partialResult = nil
	return errors.Trace(err)
}

// Next implements the Executor Next interface.
func (e *TableReaderExecutor) Next() (Row, error) {
	for {
		// Get partial result.
		if e.partialResult == nil {
			var err error
			e.partialResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.partialResult == nil {
				// Finished.
				return nil, nil
			}
		}
		// Get a row from partial result.
		rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			// Finish the current partial result and get the next one.
			err = e.partialResult.Close()
			terror.Log(errors.Trace(err))
			e.partialResult = nil
			continue
		}
		err = decodeRawValues(rowData, e.schema, e.ctx.GetSessionVars().GetTimeZone())
		if err != nil {
			return nil, errors.Trace(err)
		}
		return rowData, nil
	}
}

// Open implements the Executor Open interface.
func (e *TableReaderExecutor) Open() error {
	var builder requestBuilder
	kvReq, err := builder.SetTableRanges(e.tableID, e.ranges).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.NewSelectDAG(goctx.Background(), e.ctx.GetClient(), kvReq, e.schema.Len())
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}

// doRequestForHandles constructs kv ranges by handles. It is used by index look up executor.
func (e *TableReaderExecutor) doRequestForHandles(handles []int64, goCtx goctx.Context) error {
	sort.Sort(int64Slice(handles))
	var builder requestBuilder
	kvReq, err := builder.SetTableHandles(e.tableID, handles).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.NewSelectDAG(goCtx, e.ctx.GetClient(), kvReq, e.schema.Len())
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(goCtx)
	return nil
}

// doRequestForDatums constructs kv ranges by Datums. It is used by index look up join.
// Every lens for `datums` will always be one and must be type of int64.
func (e *TableReaderExecutor) doRequestForDatums(datums [][]types.Datum, goCtx goctx.Context) error {
	handles := make([]int64, 0, len(datums))
	for _, datum := range datums {
		handles = append(handles, datum[0].GetInt64())
	}
	return errors.Trace(e.doRequestForHandles(handles, goCtx))
}

// IndexReaderExecutor sends dag request and reads index data from kv layer.
type IndexReaderExecutor struct {
	table     table.Table
	index     *model.IndexInfo
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []*types.IndexRange
	dagPB     *tipb.DAGRequest
	ctx       context.Context
	schema    *expression.Schema

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result        distsql.NewSelectResult
	partialResult distsql.NewPartialResult
	// columns are only required by union scan.
	columns  []*model.ColumnInfo
	priority int
}

// Schema implements the Executor Schema interface.
func (e *IndexReaderExecutor) Schema() *expression.Schema {
	return e.schema
}

// Close implements the Executor Close interface.
func (e *IndexReaderExecutor) Close() error {
	err := closeAll(e.result, e.partialResult)
	e.result = nil
	e.partialResult = nil
	return errors.Trace(err)
}

// Next implements the Executor Next interface.
func (e *IndexReaderExecutor) Next() (Row, error) {
	for {
		// Get partial result.
		if e.partialResult == nil {
			var err error
			e.partialResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.partialResult == nil {
				// Finished.
				return nil, nil
			}
		}
		// Get a row from partial result.
		rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			// Finish the current partial result and get the next one.
			err = e.partialResult.Close()
			terror.Log(errors.Trace(err))
			e.partialResult = nil
			continue
		}
		err = decodeRawValues(rowData, e.schema, e.ctx.GetSessionVars().GetTimeZone())
		if err != nil {
			return nil, errors.Trace(err)
		}
		return rowData, nil
	}
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open() error {
	fieldTypes := make([]*types.FieldType, len(e.index.Columns))
	for i, v := range e.index.Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	var builder requestBuilder
	kvReq, err := builder.SetIndexRanges(e.ctx.GetSessionVars().StmtCtx, e.tableID, e.index.ID, e.ranges, fieldTypes).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.NewSelectDAG(e.ctx.GoCtx(), e.ctx.GetClient(), kvReq, e.schema.Len())
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}

// doRequestForDatums constructs kv ranges by datums. It is used by index look up executor.
func (e *IndexReaderExecutor) doRequestForDatums(values [][]types.Datum, goCtx goctx.Context) error {
	var builder requestBuilder
	kvReq, err := builder.SetIndexValues(e.tableID, e.index.ID, values).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.NewSelectDAG(e.ctx.GoCtx(), e.ctx.GetClient(), kvReq, e.schema.Len())
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(goCtx)
	return nil
}

// IndexLookUpExecutor implements double read for index scan.
type IndexLookUpExecutor struct {
	table     table.Table
	index     *model.IndexInfo
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []*types.IndexRange
	dagPB     *tipb.DAGRequest
	ctx       context.Context
	schema    *expression.Schema
	// This is the column that represent the handle, we can use handleCol.Index to know its position.
	handleCol    *expression.Column
	tableRequest *tipb.DAGRequest
	// When we need to sort the data in the second read, we must use handle to do this,
	// In this case, schema that the table reader use is different with executor's schema.
	// TODO: store it in table plan's schema. Not store it here.
	tableReaderSchema *expression.Schema
	// columns are only required by union scan.
	columns  []*model.ColumnInfo
	priority int
	// All fields above is immutable.

	indexWorker
	tableWorker
	finished chan struct{}

	resultCh   chan *lookupTableTask
	resultCurr *lookupTableTask
}

// indexWorker is used by IndexLookUpExecutor to maintain index lookup background goroutines.
type indexWorker struct {
	wg sync.WaitGroup
}

// startIndexWorker launch a background goroutine to fetch handles, send the results to workCh.
func (e *IndexLookUpExecutor) startIndexWorker(kvRanges []kv.KeyRange, workCh chan<- *lookupTableTask, finished <-chan struct{}) error {
	var builder requestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	// Since the first read only need handle information. So its returned col is only 1.
	result, err := distsql.NewSelectDAG(e.ctx.GoCtx(), e.ctx.GetClient(), kvReq, 1)
	if err != nil {
		return errors.Trace(err)
	}
	result.Fetch(e.ctx.GoCtx())
	worker := &e.indexWorker
	worker.wg.Add(1)
	go func() {
		ctx, cancel := goctx.WithCancel(e.ctx.GoCtx())
		worker.fetchHandles(e, result, workCh, ctx, finished)
		cancel()
		if err := result.Close(); err != nil {
			log.Error("close SelectDAG result failed:", errors.ErrorStack(err))
		}
		close(workCh)
		close(e.resultCh)
		worker.wg.Done()
	}()
	return nil
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are sent to workCh to be further processed by tableWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (worker *indexWorker) fetchHandles(e *IndexLookUpExecutor, result distsql.NewSelectResult, workCh chan<- *lookupTableTask, ctx goctx.Context, finished <-chan struct{}) {
	for {
		handles, finish, err := extractHandlesFromNewIndexResult(result)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- errors.Trace(err)
			e.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			return
		}
		if finish {
			return
		}
		tasks := e.buildTableTasks(handles)
		for _, task := range tasks {
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case workCh <- task:
				e.resultCh <- task
			}
		}
	}
}

func (worker *indexWorker) close() {
	worker.wg.Wait()
}

// tableWorker is used by IndexLookUpExecutor to maintain table lookup background goroutines.
type tableWorker struct {
	wg sync.WaitGroup
}

// startTableWorker launch some background goroutines which pick tasks from workCh and execute the task.
func (e *IndexLookUpExecutor) startTableWorker(workCh <-chan *lookupTableTask, finished <-chan struct{}) {
	worker := &e.tableWorker
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency
	worker.wg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		ctx, cancel := goctx.WithCancel(e.ctx.GoCtx())
		go func() {
			worker.pickAndExecTask(e, workCh, ctx, finished)
			cancel()
			worker.wg.Done()
		}()
	}
}

// pickAndExecTask picks tasks from workCh, and execute them.
func (worker *tableWorker) pickAndExecTask(e *IndexLookUpExecutor, workCh <-chan *lookupTableTask, ctx goctx.Context, finished <-chan struct{}) {
	for {
		// Don't check ctx.Done() on purpose. If background worker get the signal and all
		// exit immediately, session's goroutine doesn't know this and still calling Next(),
		// it may block reading task.doneCh forever.
		select {
		case task, ok := <-workCh:
			if !ok {
				return
			}
			e.executeTask(task, ctx)
		case <-finished:
			return
		}
	}
}

func (worker *tableWorker) close() {
	worker.wg.Wait()
}

// Open implements the Executor Open interface.
func (e *IndexLookUpExecutor) Open() error {
	kvRanges, err := e.indexRangesToKVRanges()
	if err != nil {
		return errors.Trace(err)
	}
	return e.open(kvRanges)
}

func (e *IndexLookUpExecutor) open(kvRanges []kv.KeyRange) error {
	e.finished = make(chan struct{})
	e.indexWorker = indexWorker{}
	e.tableWorker = tableWorker{}
	e.resultCh = make(chan *lookupTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))

	// indexWorker will write to workCh and tableWorker will read from workCh,
	// so fetching index and getting table data can run concurrently.
	workCh := make(chan *lookupTableTask, 1)
	err := e.startIndexWorker(kvRanges, workCh, e.finished)
	if err != nil {
		return errors.Trace(err)
	}
	e.startTableWorker(workCh, e.finished)
	return nil
}

func (e *IndexLookUpExecutor) indexRangesToKVRanges() ([]kv.KeyRange, error) {
	fieldTypes := make([]*types.FieldType, len(e.index.Columns))
	for i, v := range e.index.Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	return indexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.tableID, e.index.ID, e.ranges, fieldTypes)
}

// doRequestForDatums constructs kv ranges by datums. It is used by index look up join.
func (e *IndexLookUpExecutor) doRequestForDatums(values [][]types.Datum, goCtx goctx.Context) error {
	kvRanges, err := indexValuesToKVRanges(e.tableID, e.index.ID, values)
	if err != nil {
		return errors.Trace(err)
	}
	return e.open(kvRanges)
}

// executeTask executes the table look up tasks. We will construct a table reader and send request by handles.
// Then we hold the returning rows and finish this task.
func (e *IndexLookUpExecutor) executeTask(task *lookupTableTask, goCtx goctx.Context) {
	var err error
	defer func() {
		task.doneCh <- errors.Trace(err)
	}()
	var schema *expression.Schema
	if e.tableReaderSchema != nil {
		schema = e.tableReaderSchema
	} else {
		schema = e.schema
	}
	tableReader := &TableReaderExecutor{
		table:   e.table,
		tableID: e.tableID,
		dagPB:   e.tableRequest,
		schema:  schema,
		ctx:     e.ctx,
	}
	err = tableReader.doRequestForHandles(task.handles, goCtx)
	if err != nil {
		return
	}
	defer terror.Call(tableReader.Close)
	for {
		var row Row
		row, err = tableReader.Next()
		if err != nil || row == nil {
			break
		}
		task.rows = append(task.rows, row)
	}
	if e.keepOrder {
		// Restore the index order.
		sorter := &rowsSorter{order: task.indexOrder, rows: task.rows, handleIdx: e.handleCol.Index}
		sort.Sort(sorter)
		if e.tableReaderSchema != nil {
			for i, row := range task.rows {
				task.rows[i] = row[:len(row)-1]
			}
		}
	}
}

func (e *IndexLookUpExecutor) buildTableTasks(handles []int64) []*lookupTableTask {
	// Build tasks with increasing batch size.
	var taskSizes []int
	total := len(handles)
	batchSize := e.ctx.GetSessionVars().IndexLookupSize
	for total > 0 {
		if batchSize > total {
			batchSize = total
		}
		taskSizes = append(taskSizes, batchSize)
		total -= batchSize
	}

	var indexOrder map[int64]int
	if e.keepOrder {
		// Save the index order.
		indexOrder = make(map[int64]int, len(handles))
		for i, h := range handles {
			indexOrder[h] = i
		}
	}

	tasks := make([]*lookupTableTask, len(taskSizes))
	for i, size := range taskSizes {
		task := &lookupTableTask{
			handles:    handles[:size],
			indexOrder: indexOrder,
		}
		task.doneCh = make(chan error, 1)
		handles = handles[size:]
		tasks[i] = task
	}
	return tasks
}

// Schema implements Exec Schema interface.
func (e *IndexLookUpExecutor) Schema() *expression.Schema {
	return e.schema
}

// Close implements Exec Close interface.
func (e *IndexLookUpExecutor) Close() error {
	if e.finished != nil {
		close(e.finished)
		// Drain the resultCh and discard the result, in case that Next() doesn't fully
		// consume the data, background worker still writing to resultCh and block forever.
		for range e.resultCh {
		}
		e.indexWorker.close()
		e.tableWorker.close()
		e.finished = nil
	}
	return nil
}

// Next implements Exec Next interface.
func (e *IndexLookUpExecutor) Next() (Row, error) {
	for {
		if e.resultCurr == nil {
			resultCurr, ok := <-e.resultCh
			if !ok {
				return nil, nil
			}
			e.resultCurr = resultCurr
		}
		row, err := e.resultCurr.getRow()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			return row, nil
		}
		e.resultCurr = nil
	}
}

type requestBuilder struct {
	kv.Request
	err error
}

func (builder *requestBuilder) Build() (*kv.Request, error) {
	return &builder.Request, errors.Trace(builder.err)
}

func (builder *requestBuilder) SetTableRanges(tid int64, tableRanges []types.IntColumnRange) *requestBuilder {
	builder.Request.KeyRanges = tableRangesToKVRanges(tid, tableRanges)
	return builder
}

func (builder *requestBuilder) SetIndexRanges(sc *variable.StatementContext, tid, idxID int64, ranges []*types.IndexRange, fieldTypes []*types.FieldType) *requestBuilder {
	if builder.err != nil {
		return builder
	}
	builder.Request.KeyRanges, builder.err = indexRangesToKVRanges(sc, tid, idxID, ranges, fieldTypes)
	return builder
}

func (builder *requestBuilder) SetTableHandles(tid int64, handles []int64) *requestBuilder {
	builder.Request.KeyRanges = tableHandlesToKVRanges(tid, handles)
	return builder
}

func (builder *requestBuilder) SetIndexValues(tid, idxID int64, values [][]types.Datum) *requestBuilder {
	if builder.err != nil {
		return builder
	}
	builder.Request.KeyRanges, builder.err = indexValuesToKVRanges(tid, idxID, values)
	return builder
}

func (builder *requestBuilder) SetDAGRequest(dag *tipb.DAGRequest) *requestBuilder {
	if builder.err != nil {
		return builder
	}

	builder.Request.Tp = kv.ReqTypeDAG
	builder.Request.StartTs = dag.StartTs
	builder.Request.Data, builder.err = dag.Marshal()
	return builder
}

func (builder *requestBuilder) SetAnalyzeRequest(ana *tipb.AnalyzeReq) *requestBuilder {
	if builder.err != nil {
		return builder
	}

	builder.Request.Tp = kv.ReqTypeAnalyze
	builder.Request.StartTs = ana.StartTs
	builder.Request.Data, builder.err = ana.Marshal()
	builder.Request.NotFillCache = true
	return builder
}

func (builder *requestBuilder) SetKeyRanges(keyRanges []kv.KeyRange) *requestBuilder {
	builder.Request.KeyRanges = keyRanges
	return builder
}

func (builder *requestBuilder) SetDesc(desc bool) *requestBuilder {
	builder.Request.Desc = desc
	return builder
}

func (builder *requestBuilder) SetKeepOrder(order bool) *requestBuilder {
	builder.Request.KeepOrder = order
	return builder
}

func (builder *requestBuilder) SetFromSessionVars(sv *variable.SessionVars) *requestBuilder {
	builder.Request.Concurrency = sv.DistSQLScanConcurrency
	builder.Request.IsolationLevel = getIsolationLevel(sv)
	builder.Request.NotFillCache = sv.StmtCtx.NotFillCache
	return builder
}

func (builder *requestBuilder) SetPriority(priority int) *requestBuilder {
	builder.Request.Priority = priority
	return builder
}
