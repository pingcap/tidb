// Copyright 2016 PingCAP, Inc.
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
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xapi"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ NewXExecutor = &NewXSelectTableExec{}
	_ NewXExecutor = &NewXSelectIndexExec{}
)

// NewXExecutor defines some interfaces used by dist-sql.
type NewXExecutor interface {
	// AddAggregate adds aggregate info into an executor.
	AddAggregate(funcs []*tipb.Expr, byItems []*tipb.ByItem, fields []*types.FieldType)
	// GetTable gets the TableInfo of this XExecutor.
	GetTable() *model.TableInfo
	// AddLimit try to add limit to NewXExecutor. If success, return true.
	AddLimit(l *plan.Limit) bool
}

// Closeable is a interface for closeable structures.
type Closeable interface {
	// Close closes the object.
	Close() error
}

func closeAll(objs ...Closeable) error {
	for _, obj := range objs {
		if obj != nil {
			err := obj.Close()
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// NewXSelectIndexExec represents XAPI select index executor without result fields.
type NewXSelectIndexExec struct {
	tableInfo     *model.TableInfo
	table         table.Table
	asName        *model.CIStr
	ctx           context.Context
	supportDesc   bool
	isMemDB       bool
	result        xapi.SelectResult
	partialResult xapi.PartialResult
	where         *tipb.Expr
	txn           kv.Transaction

	tasks    chan *lookupTableTask
	tasksErr error // not nil if tasks closed due to error.
	taskCurr *lookupTableTask

	indexPlan *plan.PhysicalIndexScan

	returnedRows uint64 // returned row count

	/*
		The following attributes are used for aggregation push down.
		aggFuncs is the aggregation functions in protobuf format. They will be added to xapi request msg.
		byItem is the groupby items in protobuf format. They will be added to xapi request msg.
		aggFields is used to decode returned rows from xapi.
		aggregate indicates of the executor is handling aggregate result.
		It is more convenient to use a single varible than use a long condition.
	*/
	aggFuncs  []*tipb.Expr
	byItems   []*tipb.ByItem
	aggFields []*types.FieldType
	aggregate bool
}

// AddAggregate implements NewXExecutor interface.
func (e *NewXSelectIndexExec) AddAggregate(funcs []*tipb.Expr, byItems []*tipb.ByItem, fields []*types.FieldType) {
	e.aggFuncs = funcs
	e.byItems = byItems
	e.aggFields = fields
	e.aggregate = true
	client := e.txn.GetClient()
	if !client.SupportRequestType(kv.ReqTypeIndex, kv.ReqSubTypeGroupBy) {
		e.indexPlan.DoubleRead = true
	}
}

// AddLimit implements NewXExecutor interface.
func (e *NewXSelectIndexExec) AddLimit(limit *plan.Limit) bool {
	cnt := int64(limit.Offset + limit.Count)
	if e.indexPlan.LimitCount == nil {
		e.indexPlan.LimitCount = &cnt
		return true
	}
	return false
}

// GetTable implements NewXExecutor interface.
func (e *NewXSelectIndexExec) GetTable() *model.TableInfo {
	return e.tableInfo
}

// Fields implements Exec Fields interface.
func (e *NewXSelectIndexExec) Fields() []*ast.ResultField {
	return nil
}

// Schema implements Exec Schema interface.
func (e *NewXSelectIndexExec) Schema() expression.Schema {
	return e.indexPlan.GetSchema()
}

// Close implements Exec Close interface.
func (e *NewXSelectIndexExec) Close() error {
	err := closeAll(e.result, e.partialResult)
	if err != nil {
		return errors.Trace(err)
	}
	e.result = nil
	e.partialResult = nil
	e.taskCurr = nil
	e.tasks = nil
	e.returnedRows = 0
	return nil
}

// Next implements Executor Next interface.
func (e *NewXSelectIndexExec) Next() (*Row, error) {
	if e.indexPlan.LimitCount != nil && e.returnedRows >= uint64(*e.indexPlan.LimitCount) {
		return nil, nil
	}
	e.returnedRows++
	if e.indexPlan.DoubleRead {
		return e.nextForDoubleRead()
	}
	return e.nextForSingleRead()
}

func (e *NewXSelectIndexExec) nextForSingleRead() (*Row, error) {
	if e.result == nil {
		var err error
		e.result, err = e.doIndexRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if e.aggregate {
			// The returned rows should be aggregate partial result.
			e.result.SetFields(e.aggFields)
		}
		e.result.Fetch()
	}
	for {
		if e.partialResult == nil {
			var err error
			e.partialResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.partialResult == nil {
				return nil, nil
			}
		}
		h, rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			e.partialResult = nil
			continue
		}
		if e.aggregate {
			return &Row{Data: rowData}, nil
		}
		rowData = e.indexRowToTableRow(h, rowData)
		return resultRowToRow(e.table, h, rowData, e.asName), nil
	}
}

func (e *NewXSelectIndexExec) indexRowToTableRow(handle int64, indexRow []types.Datum) []types.Datum {
	tableRow := make([]types.Datum, len(e.indexPlan.Columns))
	for i, tblCol := range e.indexPlan.Columns {
		if mysql.HasPriKeyFlag(tblCol.Flag) && e.indexPlan.Table.PKIsHandle {
			tableRow[i] = types.NewIntDatum(handle)
			continue
		}
		for j, idxCol := range e.indexPlan.Index.Columns {
			if tblCol.Name.L == idxCol.Name.L {
				tableRow[i] = indexRow[j]
				break
			}
		}
	}
	return tableRow
}

func (e *NewXSelectIndexExec) nextForDoubleRead() (*Row, error) {
	var startTs time.Time
	if e.tasks == nil {
		startTs = time.Now()
		idxResult, err := e.doIndexRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxResult.Fetch()

		// Use a background goroutine to fetch index, put the result in e.tasks.
		// e.tasks serves as a pipeline, so fetch index and get table data would
		// run concurrency.
		e.tasks = make(chan *lookupTableTask, 50)
		go e.fetchHandles(idxResult, e.tasks)
	}

	for {
		if e.taskCurr == nil {
			taskCurr, ok := <-e.tasks
			if !ok {
				log.Debugf("[TIME_INDEX_TABLE_SCAN] time: %v", time.Now().Sub(startTs))
				return nil, e.tasksErr
			}
			e.taskCurr = taskCurr
		}

		row, err := e.taskCurr.getRow()
		if err != nil || row != nil {
			return row, errors.Trace(err)
		}
		e.taskCurr = nil
	}
}

const concurrencyLimit int32 = 1024

func addWorker(e *NewXSelectIndexExec, ch chan *lookupTableTask, concurrency *int32) {
	if atomic.LoadInt32(concurrency) <= concurrencyLimit {
		go e.pickAndExecTask(ch)
		atomic.AddInt32(concurrency, 1)
	}
}

func (e *NewXSelectIndexExec) fetchHandles(idxResult xapi.SelectResult, ch chan<- *lookupTableTask) {
	defer close(ch)

	workCh := make(chan *lookupTableTask, 1)
	defer close(workCh)

	var concurrency int32
	addWorker(e, workCh, &concurrency)

	totalHandles := 0
	startTs := time.Now()
	for {
		handles, err := extractHandlesFromIndexResult(idxResult)
		if err != nil || handles == nil {
			e.tasksErr = errors.Trace(err)
			log.Debugf("[TIME_INDEX_SCAN] time: %v handles: %d concurrency: %d",
				time.Now().Sub(startTs),
				totalHandles,
				atomic.LoadInt32(&concurrency))
			return
		}

		totalHandles += len(handles)
		tasks := e.buildTableTasks(handles)
		for _, task := range tasks {
			if int(atomic.LoadInt32(&concurrency)) < len(tasks) {
				addWorker(e, workCh, &concurrency)
			}

			select {
			case workCh <- task:
			default:
				addWorker(e, workCh, &concurrency)
				workCh <- task
			}
			ch <- task
		}
	}
}

func (e *NewXSelectIndexExec) doIndexRequest() (xapi.SelectResult, error) {
	selIdxReq := new(tipb.SelectRequest)
	selIdxReq.StartTs = e.txn.StartTS()
	selIdxReq.IndexInfo = xapi.IndexToProto(e.table.Meta(), e.indexPlan.Index)
	selIdxReq.Limit = e.indexPlan.LimitCount
	if e.indexPlan.Desc {
		selIdxReq.OrderBy = append(selIdxReq.OrderBy, &tipb.ByItem{Desc: e.indexPlan.Desc})
	}
	fieldTypes := make([]*types.FieldType, len(e.indexPlan.Index.Columns))
	for i, v := range e.indexPlan.Index.Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	var err error
	selIdxReq.Ranges, err = indexRangesToPBRanges(e.indexPlan.Ranges, fieldTypes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	concurrency := 1
	if !e.indexPlan.DoubleRead {
		concurrency = defaultConcurrency
		selIdxReq.Aggregates = e.aggFuncs
		selIdxReq.GroupBy = e.byItems
		selIdxReq.Where = e.where
	} else if e.indexPlan.OutOfOrder {
		concurrency = defaultConcurrency
	}
	return xapi.Select(e.txn.GetClient(), selIdxReq, concurrency, !e.indexPlan.OutOfOrder)
}

func (e *NewXSelectIndexExec) buildTableTasks(handles []int64) []*lookupTableTask {
	// Build tasks with increasing batch size.
	var taskSizes []int
	total := len(handles)
	batchSize := BaseLookupTableTaskSize
	for total > 0 {
		if batchSize > total {
			batchSize = total
		}
		taskSizes = append(taskSizes, batchSize)
		total -= batchSize
		if batchSize < MaxLookupTableTaskSize {
			batchSize *= 2
		}
	}

	var indexOrder map[int64]int
	if !e.indexPlan.OutOfOrder {
		// Save the index order.
		indexOrder = make(map[int64]int)
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

// pickAndExecTask is a worker function, the common usage is
// go e.pickAndExecTask(ch)
func (e *NewXSelectIndexExec) pickAndExecTask(ch <-chan *lookupTableTask) {
	for task := range ch {
		err := e.executeTask(task)
		task.doneCh <- err
	}
}

func (e *NewXSelectIndexExec) executeTask(task *lookupTableTask) error {
	sort.Sort(int64Slice(task.handles))
	tblResult, err := e.doTableRequest(task.handles)
	if err != nil {
		return errors.Trace(err)
	}
	task.rows, err = e.extractRowsFromTableResult(e.table, tblResult)
	if err != nil {
		return errors.Trace(err)
	}
	if !e.indexPlan.OutOfOrder {
		// Restore the index order.
		sorter := &rowsSorter{order: task.indexOrder, rows: task.rows}
		if e.indexPlan.Desc && !e.supportDesc {
			sort.Sort(sort.Reverse(sorter))
		} else {
			sort.Sort(sorter)
		}
	}
	return nil
}

func (e *NewXSelectIndexExec) extractRowsFromTableResult(t table.Table, tblResult xapi.SelectResult) ([]*Row, error) {
	var rows []*Row
	for {
		partialResult, err := tblResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if partialResult == nil {
			break
		}
		subRows, err := e.extractRowsFromPartialResult(t, partialResult)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rows = append(rows, subRows...)
	}
	return rows, nil
}

func (e *NewXSelectIndexExec) extractRowsFromPartialResult(t table.Table, partialResult xapi.PartialResult) ([]*Row, error) {
	var rows []*Row
	for {
		h, rowData, err := partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			break
		}
		row := resultRowToRow(t, h, rowData, e.indexPlan.TableAsName)
		rows = append(rows, row)
	}
	return rows, nil
}

func (e *NewXSelectIndexExec) doTableRequest(handles []int64) (xapi.SelectResult, error) {
	// The handles are not in original index order, so we can't push limit here.
	selTableReq := new(tipb.SelectRequest)
	selTableReq.StartTs = e.txn.StartTS()
	selTableReq.TableInfo = &tipb.TableInfo{
		TableId: e.table.Meta().ID,
	}
	selTableReq.TableInfo.Columns = xapi.ColumnsToProto(e.indexPlan.Columns, e.table.Meta().PKIsHandle)
	for _, h := range handles {
		if h == math.MaxInt64 {
			// We can't convert MaxInt64 into an left closed, right open range.
			continue
		}
		pbRange := new(tipb.KeyRange)
		pbRange.Low = codec.EncodeInt(nil, h)
		pbRange.High = kv.Key(pbRange.Low).PrefixNext()
		selTableReq.Ranges = append(selTableReq.Ranges, pbRange)
	}
	selTableReq.Where = e.where
	// Aggregate Info
	selTableReq.Aggregates = e.aggFuncs
	selTableReq.GroupBy = e.byItems
	// Aggregate Info
	resp, err := xapi.Select(e.txn.GetClient(), selTableReq, defaultConcurrency, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if e.aggregate {
		// The returned rows should be aggregate partial result.
		resp.SetFields(e.aggFields)
	}
	resp.Fetch()
	return resp, nil
}

// NewXSelectTableExec represents XAPI select executor without result fields.
type NewXSelectTableExec struct {
	tableInfo     *model.TableInfo
	table         table.Table
	asName        *model.CIStr
	ctx           context.Context
	supportDesc   bool
	isMemDB       bool
	result        xapi.SelectResult
	partialResult xapi.PartialResult
	where         *tipb.Expr
	Columns       []*model.ColumnInfo
	schema        expression.Schema
	ranges        []plan.TableRange
	desc          bool
	limitCount    *int64
	returnedRows  uint64 // returned rowCount
	keepOrder     bool
	txn           kv.Transaction

	/*
		The following attributes are used for aggregation push down.
		aggFuncs is the aggregation functions in protobuf format. They will be added to xapi request msg.
		byItem is the groupby items in protobuf format. They will be added to xapi request msg.
		aggFields is used to decode returned rows from xapi.
		aggregate indicates of the executor is handling aggregate result.
		It is more convenient to use a single varible than use a long condition.
	*/
	aggFuncs  []*tipb.Expr
	byItems   []*tipb.ByItem
	aggFields []*types.FieldType
	aggregate bool
}

// AddLimit implements NewXExecutor interface.
func (e *NewXSelectTableExec) AddLimit(limit *plan.Limit) bool {
	cnt := int64(limit.Offset + limit.Count)
	if e.limitCount == nil {
		e.limitCount = &cnt
		return true
	}
	return false
}

// Schema implements Executor Schema interface.
func (e *NewXSelectTableExec) Schema() expression.Schema {
	return e.schema
}

func (e *NewXSelectTableExec) doRequest() error {
	var err error
	selReq := new(tipb.SelectRequest)
	selReq.StartTs = e.txn.StartTS()
	selReq.Where = e.where
	selReq.Ranges = tableRangesToPBRanges(e.ranges)
	columns := e.Columns
	selReq.TableInfo = &tipb.TableInfo{
		TableId: e.tableInfo.ID,
	}
	if e.supportDesc && e.desc {
		selReq.OrderBy = append(selReq.OrderBy, &tipb.ByItem{Desc: e.desc})
	}
	selReq.Limit = e.limitCount
	selReq.TableInfo.Columns = xapi.ColumnsToProto(columns, e.tableInfo.PKIsHandle)
	// Aggregate Info
	selReq.Aggregates = e.aggFuncs
	selReq.GroupBy = e.byItems
	e.result, err = xapi.Select(e.txn.GetClient(), selReq, defaultConcurrency, e.keepOrder)
	if err != nil {
		return errors.Trace(err)
	}
	//if len(selReq.Aggregates) > 0 || len(selReq.GroupBy) > 0 {
	if e.aggregate {
		// The returned rows should be aggregate partial result.
		e.result.SetFields(e.aggFields)
	}
	e.result.Fetch()
	return nil
}

// Close implements Executor Close interface.
func (e *NewXSelectTableExec) Close() error {
	err := closeAll(e.result, e.partialResult)
	if err != nil {
		return errors.Trace(err)
	}
	e.result = nil
	e.partialResult = nil
	e.returnedRows = 0
	return nil
}

// Next implements Executor interface.
func (e *NewXSelectTableExec) Next() (*Row, error) {
	if e.limitCount != nil && e.returnedRows >= uint64(*e.limitCount) {
		return nil, nil
	}
	if e.result == nil {
		err := e.doRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for {
		if e.partialResult == nil {
			var err error
			startTs := time.Now()
			e.partialResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.partialResult == nil {
				return nil, nil
			}
			log.Infof("[TIME_TABLE_SCAN] %v", time.Now().Sub(startTs))
		}
		h, rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			e.partialResult = nil
			continue
		}
		e.returnedRows++
		if e.aggregate {
			// compose aggreagte row
			return &Row{Data: rowData}, nil
		}
		return resultRowToRow(e.table, h, rowData, e.asName), nil
	}
}

// AddAggregate implements NewXExecutor interface.
func (e *NewXSelectTableExec) AddAggregate(funcs []*tipb.Expr, byItems []*tipb.ByItem, fields []*types.FieldType) {
	e.aggFuncs = funcs
	e.byItems = byItems
	e.aggFields = fields
	e.aggregate = true
}

// GetTable implements NewXExecutor interface.
func (e *NewXSelectTableExec) GetTable() *model.TableInfo {
	return e.tableInfo
}

// Fields implements Executor interface.
func (e *NewXSelectTableExec) Fields() []*ast.ResultField {
	return nil
}
