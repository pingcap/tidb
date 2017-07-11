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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
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

// TableReaderExecutor sends dag request and reads table data from kv layer.
type TableReaderExecutor struct {
	asName    *model.CIStr
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
	result        distsql.SelectResult
	partialResult distsql.PartialResult
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
func (e *TableReaderExecutor) Next() (*Row, error) {
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
		h, rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			// Finish the current partial result and get the next one.
			e.partialResult.Close()
			e.partialResult = nil
			continue
		}
		values := make([]types.Datum, e.schema.Len())
		err = codec.SetRawValues(rowData, values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = decodeRawValues(values, e.schema, e.ctx.GetSessionVars().GetTimeZone())
		if err != nil {
			return nil, errors.Trace(err)
		}
		return resultRowToRow(e.table, h, values, e.asName), nil
	}
}

// Open implements the Executor Open interface.
func (e *TableReaderExecutor) Open() error {
	kvRanges := tableRangesToKVRanges(e.tableID, e.ranges)
	var err error
	e.result, err = distsql.SelectDAG(e.ctx.GetClient(), goctx.Background(), e.dagPB, kvRanges, e.ctx.GetSessionVars().DistSQLScanConcurrency, e.keepOrder, e.desc)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}

// doRequestForHandles constructs kv ranges by handles. It is used by index look up executor.
func (e *TableReaderExecutor) doRequestForHandles(handles []int64, goCtx goctx.Context) error {
	kvRanges := tableHandlesToKVRanges(e.tableID, handles)
	var err error
	e.result, err = distsql.SelectDAG(e.ctx.GetClient(), goCtx, e.dagPB, kvRanges, e.ctx.GetSessionVars().DistSQLScanConcurrency, e.keepOrder, e.desc)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(goCtx)
	return nil
}

// doRequestForDatums constructs kv ranges by Datums. It is used by index look up executor.
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
	asName    *model.CIStr
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
	result        distsql.SelectResult
	partialResult distsql.PartialResult
	// columns are only required by union scan.
	columns []*model.ColumnInfo
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
func (e *IndexReaderExecutor) Next() (*Row, error) {
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
		h, rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			// Finish the current partial result and get the next one.
			e.partialResult.Close()
			e.partialResult = nil
			continue
		}
		values := make([]types.Datum, e.schema.Len())
		err = codec.SetRawValues(rowData, values)
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = decodeRawValues(values, e.schema, e.ctx.GetSessionVars().GetTimeZone())
		if err != nil {
			return nil, errors.Trace(err)
		}
		return resultRowToRow(e.table, h, values, e.asName), nil
	}
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open() error {
	fieldTypes := make([]*types.FieldType, len(e.index.Columns))
	for i, v := range e.index.Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	kvRanges, err := indexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.tableID, e.index.ID, e.ranges, fieldTypes)
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.SelectDAG(e.ctx.GetClient(), e.ctx.GoCtx(), e.dagPB, kvRanges, e.ctx.GetSessionVars().DistSQLScanConcurrency, e.keepOrder, e.desc)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())
	return nil
}

// doRequestForDatums constructs kv ranges by datums. It is used by index look up executor.
func (e *IndexReaderExecutor) doRequestForDatums(values [][]types.Datum, goCtx goctx.Context) error {
	kvRanges, err := indexValuesToKVRanges(e.tableID, e.index.ID, values)
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.SelectDAG(e.ctx.GetClient(), e.ctx.GoCtx(), e.dagPB, kvRanges, e.ctx.GetSessionVars().DistSQLScanConcurrency, e.keepOrder, e.desc)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(goCtx)
	return nil
}

// IndexLookUpExecutor implements double read for index scan.
type IndexLookUpExecutor struct {
	asName    *model.CIStr
	table     table.Table
	index     *model.IndexInfo
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []*types.IndexRange
	dagPB     *tipb.DAGRequest
	ctx       context.Context
	schema    *expression.Schema

	// result returns one or more distsql.PartialResult.
	result distsql.SelectResult

	taskChan chan *lookupTableTask
	tasksErr error
	taskCurr *lookupTableTask

	tableRequest *tipb.DAGRequest
	// columns are only required by union scan.
	columns []*model.ColumnInfo
}

// Open implements the Executor Open interface.
func (e *IndexLookUpExecutor) Open() error {
	fieldTypes := make([]*types.FieldType, len(e.index.Columns))
	for i, v := range e.index.Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	kvRanges, err := indexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.tableID, e.index.ID, e.ranges, fieldTypes)
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.SelectDAG(e.ctx.GetClient(), e.ctx.GoCtx(), e.dagPB, kvRanges, e.ctx.GetSessionVars().DistSQLScanConcurrency, e.keepOrder, e.desc)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(e.ctx.GoCtx())

	// Use a background goroutine to fetch index and put the result in e.taskChan.
	// e.taskChan serves as a pipeline, so fetching index and getting table data can
	// run concurrently.
	e.taskChan = make(chan *lookupTableTask, LookupTableTaskChannelSize)
	go e.fetchHandlesAndStartWorkers()
	return nil
}

// doRequestForDatums constructs kv ranges by datums. It is used by index look up executor.
func (e *IndexLookUpExecutor) doRequestForDatums(values [][]types.Datum, goCtx goctx.Context) error {
	kvRanges, err := indexValuesToKVRanges(e.tableID, e.index.ID, values)
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.SelectDAG(e.ctx.GetClient(), e.ctx.GoCtx(), e.dagPB, kvRanges, e.ctx.GetSessionVars().DistSQLScanConcurrency, e.keepOrder, e.desc)
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(goCtx)
	e.taskChan = make(chan *lookupTableTask, LookupTableTaskChannelSize)
	go e.fetchHandlesAndStartWorkers()
	return nil
}

// executeTask executes the table look up tasks. We will construct a table reader and send request by handles.
// Then we hold the returning rows and finish this task.
func (e *IndexLookUpExecutor) executeTask(task *lookupTableTask, goCtx goctx.Context) {
	var err error
	defer func() {
		task.doneCh <- errors.Trace(err)
	}()
	tableReader := &TableReaderExecutor{
		asName:  e.asName,
		table:   e.table,
		tableID: e.tableID,
		dagPB:   e.tableRequest,
		schema:  e.schema,
		ctx:     e.ctx,
	}
	err = tableReader.doRequestForHandles(task.handles, goCtx)
	if err != nil {
		return
	}
	for {
		var row *Row
		row, err = tableReader.Next()
		if err != nil || row == nil {
			return
		}
		task.rows = append(task.rows, row)
	}
}

func (e *IndexLookUpExecutor) pickAndExecTask(workCh <-chan *lookupTableTask, txnCtx goctx.Context) {
	childCtx, cancel := goctx.WithCancel(txnCtx)
	defer cancel()
	for {
		select {
		case task := <-workCh:
			if task == nil {
				return
			}
			e.executeTask(task, childCtx)
		case <-childCtx.Done():
			return
		}
	}
}

// fetchHandlesAndStartWorkers fetches a batch of handles from index data and builds the index lookup tasks.
// We initialize some workers to execute this tasks concurrently and put the task to taskCh by order.
func (e *IndexLookUpExecutor) fetchHandlesAndStartWorkers() {
	// The tasks in workCh will be consumed by workers. When all workers are busy, we should stop to push tasks to channel.
	// So its length is one.
	workCh := make(chan *lookupTableTask, 1)
	defer func() {
		close(workCh)
		close(e.taskChan)
	}()

	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency
	txnCtx := e.ctx.GoCtx()
	for i := 0; i < lookupConcurrencyLimit; i++ {
		go e.pickAndExecTask(workCh, txnCtx)
	}

	for {
		handles, finish, err := extractHandlesFromIndexResult(e.result)
		if err != nil || finish {
			e.tasksErr = errors.Trace(err)
			return
		}
		tasks := e.buildTableTasks(handles)
		for _, task := range tasks {
			select {
			case <-txnCtx.Done():
				return
			case workCh <- task:
			}
			e.taskChan <- task
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
	// TODO: It's better to notify fetchHandles to close instead of fetching all index handle.
	// Consume the task channel in case channel is full.
	for range e.taskChan {
	}
	e.taskChan = nil
	err := e.result.Close()
	e.result = nil
	return errors.Trace(err)
}

// Next implements Exec Next interface.
func (e *IndexLookUpExecutor) Next() (*Row, error) {
	for {
		if e.taskCurr == nil {
			taskCurr, ok := <-e.taskChan
			if !ok {
				return nil, e.tasksErr
			}
			e.taskCurr = taskCurr
		}
		row, err := e.taskCurr.getRow()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			return row, nil
		}
		e.taskCurr = nil
	}
}
