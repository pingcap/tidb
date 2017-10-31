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
	"sync/atomic"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

// IndexLookUpExecutor implements double read for index scan.
type IndexLookUpExecutor struct {
	baseExecutor
	table     table.Table
	index     *model.IndexInfo
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []*types.IndexRange
	dagPB     *tipb.DAGRequest
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
func (e *IndexLookUpExecutor) doRequestForDatums(goCtx goctx.Context, values [][]types.Datum) error {
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
	err = tableReader.doRequestForHandles(goCtx, task.handles)
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
