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
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/distsql/xeval"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	minLogDuration = 50 * time.Millisecond
)

func resultRowToRow(t table.Table, h int64, data []types.Datum, tableAsName *model.CIStr) *Row {
	entry := &RowKeyEntry{
		Handle:      h,
		Tbl:         t,
		TableAsName: tableAsName,
	}
	return &Row{Data: data, RowKeys: []*RowKeyEntry{entry}}
}

// BaseLookupTableTaskSize represents base number of handles for a lookupTableTask.
var BaseLookupTableTaskSize = 1024

// MaxLookupTableTaskSize represents max number of handles for a lookupTableTask.
var MaxLookupTableTaskSize = 20480

// lookupTableTask is created from a partial result of an index request which
// contains the handles in those index keys.
type lookupTableTask struct {
	handles []int64
	rows    []*Row
	cursor  int
	done    bool
	doneCh  chan error

	// The handles fetched from index is originally ordered by index, but we need handles to be ordered by itself
	// to do table request.
	// The indexOrder map is used to save the original index order for the handles.
	// Without this map, the original index order might be lost.
	indexOrder map[int64]int
}

func (task *lookupTableTask) getRow() (*Row, error) {
	if !task.done {
		err := <-task.doneCh
		if err != nil {
			return nil, errors.Trace(err)
		}
		task.done = true
	}

	if task.cursor < len(task.rows) {
		row := task.rows[task.cursor]
		task.cursor++
		return row, nil
	}

	return nil, nil
}

// rowsSorter sorts the rows by its index order.
type rowsSorter struct {
	order map[int64]int
	rows  []*Row
}

func (s *rowsSorter) Less(i, j int) bool {
	x := s.order[s.rows[i].RowKeys[0].Handle]
	y := s.order[s.rows[j].RowKeys[0].Handle]
	return x < y
}

func (s *rowsSorter) Len() int {
	return len(s.rows)
}

func (s *rowsSorter) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

func tableRangesToKVRanges(tid int64, tableRanges []plan.TableRange) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(tableRanges))
	for _, tableRange := range tableRanges {
		startKey := tablecodec.EncodeRowKeyWithHandle(tid, tableRange.LowVal)
		hi := tableRange.HighVal
		if hi != math.MaxInt64 {
			hi++
		}
		endKey := tablecodec.EncodeRowKeyWithHandle(tid, hi)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs
}

/*
 * Convert sorted handle to kv ranges.
 * For continuous handles, we should merge them to a single key range.
 */
func tableHandlesToKVRanges(tid int64, handles []int64) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(handles))
	i := 0
	for i < len(handles) {
		h := handles[i]
		if h == math.MaxInt64 {
			// We can't convert MaxInt64 into an left closed, right open range.
			i++
			continue
		}
		j := i + 1
		endHandle := h + 1
		for ; j < len(handles); j++ {
			if handles[j] == endHandle {
				endHandle = handles[j] + 1
				continue
			}
			break
		}
		startKey := tablecodec.EncodeRowKeyWithHandle(tid, h)
		endKey := tablecodec.EncodeRowKeyWithHandle(tid, endHandle)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		i = j
	}
	return krs
}

func indexRangesToKVRanges(sc *variable.StatementContext, tid, idxID int64, ranges []*plan.IndexRange, fieldTypes []*types.FieldType) ([]kv.KeyRange, error) {
	krs := make([]kv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
		err := convertIndexRangeTypes(sc, ran, fieldTypes)
		if err != nil {
			return nil, errors.Trace(err)
		}

		low, err := codec.EncodeKey(nil, ran.LowVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ran.LowExclude {
			low = []byte(kv.Key(low).PrefixNext())
		}
		high, err := codec.EncodeKey(nil, ran.HighVal...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !ran.HighExclude {
			high = []byte(kv.Key(high).PrefixNext())
		}
		startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
		endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
		krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
	}
	return krs, nil
}

func convertIndexRangeTypes(sc *variable.StatementContext, ran *plan.IndexRange, fieldTypes []*types.FieldType) error {
	for i := range ran.LowVal {
		if ran.LowVal[i].Kind() == types.KindMinNotNull {
			ran.LowVal[i].SetBytes([]byte{})
			continue
		}
		converted, err := ran.LowVal[i].ConvertTo(sc, fieldTypes[i])
		if err != nil {
			return errors.Trace(err)
		}
		cmp, err := converted.CompareDatum(sc, ran.LowVal[i])
		if err != nil {
			return errors.Trace(err)
		}
		ran.LowVal[i] = converted
		if cmp == 0 {
			continue
		}
		if cmp < 0 && !ran.LowExclude {
			// For int column a, a >= 1.1 is converted to a > 1.
			ran.LowExclude = true
		} else if cmp > 0 && ran.LowExclude {
			// For int column a, a > 1.9 is converted to a >= 2.
			ran.LowExclude = false
		}
		// The converted value has changed, the other column values doesn't matter.
		// For equal condition, converted value changed means there will be no match.
		// For non equal condition, this column would be the last one to build the range.
		// Break here to prevent the rest columns modify LowExclude again.
		break
	}
	for i := range ran.HighVal {
		if ran.HighVal[i].Kind() == types.KindMaxValue {
			continue
		}
		converted, err := ran.HighVal[i].ConvertTo(sc, fieldTypes[i])
		if err != nil {
			return errors.Trace(err)
		}
		cmp, err := converted.CompareDatum(sc, ran.HighVal[i])
		if err != nil {
			return errors.Trace(err)
		}
		ran.HighVal[i] = converted
		if cmp == 0 {
			continue
		}
		// For int column a, a < 1.1 is converted to a <= 1.
		if cmp < 0 && ran.HighExclude {
			ran.HighExclude = false
		}
		// For int column a, a <= 1.9 is converted to a < 2.
		if cmp > 0 && !ran.HighExclude {
			ran.HighExclude = true
		}
		break
	}
	return nil
}

// extractHandlesFromIndexResult gets some handles from SelectResult.
// It should be called in a loop until finished or error happened.
func extractHandlesFromIndexResult(idxResult distsql.SelectResult) (handles []int64, finish bool, err error) {
	subResult, e0 := idxResult.Next()
	if e0 != nil {
		err = errors.Trace(e0)
		return
	}
	if subResult == nil {
		finish = true
		return
	}
	handles, err = extractHandlesFromIndexSubResult(subResult)
	if err != nil {
		err = errors.Trace(err)
	}
	return
}

func extractHandlesFromIndexSubResult(subResult distsql.PartialResult) ([]int64, error) {
	var handles []int64
	for {
		h, data, err := subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data == nil {
			break
		}
		handles = append(handles, h)
	}
	return handles, nil
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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

// XSelectIndexExec represents the DistSQL select index executor.
// There are two execution modes. One is single-read, in which case we only need to read index keys.
// The other one is double-read, in which case we first do index request to get handles, we use each
// partial result to build a lookupTableTask.
//
// Each lookupTableTask works like XSelectTableExec. It sorts the handles, sends an *tipb.SelectRequest, then
// gets distsql.SelectResult which returns multiple distsql.PartialResults, we fetch all the rows from
// each distsql.PartialResult, then sort the rows by the original index order.
//
// So there might be many tasks built from index request, each task do its own table request.
// If we do it one by one, the execution might be very slow.
//
// To speed up the execution, index request or table request is done concurrently. The concurrency is controlled
// by kv.Client, we only need to pass the concurrency parameter.
//
// We also make a higher level of concurrency by doing index request in a background goroutine. The index goroutine
// starts multple worker goroutines and fetches handles from each index partial request, builds lookup table tasks
// and sends the task to 'workerCh'.
//
// Each worker goroutine receives tasks through the 'workerCh', then executes the task.
// After finishing the task, the workers send the task to a taskChan. At the outer most Executor.Next method,
// we receive the finished task through taskChan, and return each row in that task until no more tasks to receive.
type XSelectIndexExec struct {
	tableInfo     *model.TableInfo
	table         table.Table
	asName        *model.CIStr
	ctx           context.Context
	supportDesc   bool
	isMemDB       bool
	result        distsql.SelectResult
	partialResult distsql.PartialResult
	where         *tipb.Expr
	startTS       uint64

	taskChan chan *lookupTableTask
	tasksErr error // not nil if tasks closed due to error.
	taskCurr *lookupTableTask

	indexPlan      *plan.PhysicalIndexScan
	singleReadMode bool

	returnedRows uint64 // returned row count

	mu sync.Mutex

	/*
	   The following attributes are used for aggregation push down.
	   aggFuncs is the aggregation functions in protobuf format. They will be added to distsql request msg.
	   byItem is the groupby items in protobuf format. They will be added to distsql request msg.
	   aggFields is used to decode returned rows from distsql.
	   aggregate indicates of the executor is handling aggregate result.
	   It is more convenient to use a single varible than use a long condition.
	*/
	aggFuncs  []*tipb.Expr
	byItems   []*tipb.ByItem
	aggFields []*types.FieldType
	aggregate bool

	scanConcurrency int
	execStart       time.Time
	partialCount    int
}

// Schema implements Exec Schema interface.
func (e *XSelectIndexExec) Schema() expression.Schema {
	return e.indexPlan.GetSchema()
}

// Close implements Exec Close interface.
func (e *XSelectIndexExec) Close() error {
	err := closeAll(e.result, e.partialResult)
	if err != nil {
		return errors.Trace(err)
	}
	e.result = nil
	e.partialResult = nil
	e.taskCurr = nil
	e.taskChan = nil
	e.returnedRows = 0
	e.partialCount = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *XSelectIndexExec) Next() (*Row, error) {
	if e.indexPlan.LimitCount != nil && e.returnedRows >= uint64(*e.indexPlan.LimitCount) {
		return nil, nil
	}
	e.returnedRows++
	if e.singleReadMode {
		return e.nextForSingleRead()
	}
	return e.nextForDoubleRead()
}

func (e *XSelectIndexExec) nextForSingleRead() (*Row, error) {
	if e.result == nil {
		e.execStart = time.Now()
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
		// Get partial result.
		if e.partialResult == nil {
			var err error
			e.partialResult, err = e.result.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			if e.partialResult == nil {
				// Finished.
				duration := time.Since(e.execStart)
				if duration > minLogDuration {
					connID := e.ctx.GetSessionVars().ConnectionID
					log.Infof("[%d] [TIME_INDEX_SINGLE] %s", connID, e.slowQueryInfo(duration))
				}
				return nil, nil
			}
			e.partialCount++
		}
		// Get a row from partial result.
		h, rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			// Finish current partial result and get the next one.
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

func (e *XSelectIndexExec) indexRowToTableRow(handle int64, indexRow []types.Datum) []types.Datum {
	tableRow := make([]types.Datum, len(e.indexPlan.Columns))
	for i, tblCol := range e.indexPlan.Columns {
		if table.ToColumn(tblCol).IsPKHandleColumn(e.indexPlan.Table) {
			if mysql.HasUnsignedFlag(tblCol.FieldType.Flag) {
				tableRow[i] = types.NewUintDatum(uint64(handle))
			} else {
				tableRow[i] = types.NewIntDatum(handle)
			}
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

func (e *XSelectIndexExec) nextForDoubleRead() (*Row, error) {
	if e.taskChan == nil {
		e.execStart = time.Now()
		idxResult, err := e.doIndexRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxResult.IgnoreData()
		idxResult.Fetch()

		// Use a background goroutine to fetch index and put the result in e.taskChan.
		// e.taskChan serves as a pipeline, so fetching index and getting table data can
		// run concurrently.
		e.taskChan = make(chan *lookupTableTask, 50)
		go e.fetchHandles(idxResult, e.taskChan)
	}

	for {
		if e.taskCurr == nil {
			taskCurr, ok := <-e.taskChan
			if !ok {
				duration := time.Since(e.execStart)
				if duration > minLogDuration {
					connID := e.ctx.GetSessionVars().ConnectionID
					log.Infof("[%d] [TIME_INDEX_DOUBLE] %s", connID, e.slowQueryInfo(duration))
				}
				return nil, e.tasksErr
			}
			e.partialCount++
			e.taskCurr = taskCurr
		}
		row, err := e.taskCurr.getRow()
		if err != nil || row != nil {
			return row, errors.Trace(err)
		}
		e.taskCurr = nil
	}
}

func (e *XSelectIndexExec) slowQueryInfo(duration time.Duration) string {
	return fmt.Sprintf("time: %v, table: %s(%d), index: %s(%d), partials: %d, concurrency: %d, rows: %d",
		duration, e.tableInfo.Name, e.tableInfo.ID, e.indexPlan.Index.Name, e.indexPlan.Index.ID,
		e.partialCount, e.scanConcurrency, e.returnedRows)
}

const concurrencyLimit int = 30

// addWorker adds a worker for lookupTableTask.
// It's not thread-safe and should be called in fetchHandles goroutine only.
func addWorker(e *XSelectIndexExec, ch chan *lookupTableTask, concurrency *int) {
	if *concurrency <= concurrencyLimit {
		go e.pickAndExecTask(ch)
		*concurrency = *concurrency + 1
	}
}

func (e *XSelectIndexExec) fetchHandles(idxResult distsql.SelectResult, ch chan<- *lookupTableTask) {
	defer close(ch)

	workCh := make(chan *lookupTableTask, 1)
	defer close(workCh)

	var concurrency int
	addWorker(e, workCh, &concurrency)

	for {
		handles, finish, err := extractHandlesFromIndexResult(idxResult)
		if err != nil || finish {
			e.tasksErr = errors.Trace(err)
			return
		}
		tasks := e.buildTableTasks(handles)
		for _, task := range tasks {
			if concurrency < len(tasks) {
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

func getScanConcurrency(ctx context.Context) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := sessionVars.GetTiDBSystemVar(variable.DistSQLScanConcurrencyVar)
	if err != nil {
		return 0, errors.Trace(err)
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	log.Debugf("[%d] [DistSQL] Scan with concurrency %d", sessionVars.ConnectionID, c)
	return int(c), errors.Trace(err)
}

func (e *XSelectIndexExec) doIndexRequest() (distsql.SelectResult, error) {
	selIdxReq := new(tipb.SelectRequest)
	selIdxReq.StartTs = e.startTS
	selIdxReq.TimeZoneOffset = timeZoneOffset()
	selIdxReq.Flags = statementContextToFlags(e.ctx.GetSessionVars().StmtCtx)
	selIdxReq.IndexInfo = distsql.IndexToProto(e.table.Meta(), e.indexPlan.Index)
	if len(e.indexPlan.SortItemsPB) > 0 {
		selIdxReq.OrderBy = e.indexPlan.SortItemsPB
	} else if e.indexPlan.Desc {
		selIdxReq.OrderBy = []*tipb.ByItem{{Desc: e.indexPlan.Desc}}
	}
	if e.singleReadMode || e.where == nil {
		// TODO: when where condition is all index columns limit can be pushed too.
		selIdxReq.Limit = e.indexPlan.LimitCount
	}
	selIdxReq.Where = e.indexPlan.IndexConditionPBExpr
	if e.singleReadMode {
		selIdxReq.Aggregates = e.aggFuncs
		selIdxReq.GroupBy = e.byItems
	} else if !e.indexPlan.OutOfOrder {
		// The cost of index scan double-read is higher than single-read. Usually ordered index scan has a limit
		// which may not have been pushed down, so we set concurrency to 1 to avoid fetching unnecessary data.
		e.scanConcurrency = 1
	}
	fieldTypes := make([]*types.FieldType, len(e.indexPlan.Index.Columns))
	for i, v := range e.indexPlan.Index.Columns {
		fieldTypes[i] = &(e.table.Cols()[v.Offset].FieldType)
	}
	sc := e.ctx.GetSessionVars().StmtCtx
	keyRanges, err := indexRangesToKVRanges(sc, e.table.Meta().ID, e.indexPlan.Index.ID, e.indexPlan.Ranges, fieldTypes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return distsql.Select(e.ctx.GetClient(), selIdxReq, keyRanges, e.scanConcurrency, !e.indexPlan.OutOfOrder)
}

func (e *XSelectIndexExec) buildTableTasks(handles []int64) []*lookupTableTask {
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

// pickAndExecTask is a worker function, the common usage is
// go e.pickAndExecTask(ch)
func (e *XSelectIndexExec) pickAndExecTask(ch <-chan *lookupTableTask) {
	for task := range ch {
		err := e.executeTask(task)
		task.doneCh <- err
	}
}

// ExecuteTask executes a lookup table task.
// It works like executing an XSelectTableExec, except that the ranges are built from a slice of handles
// rather than table ranges. It sends the request to all the regions containing those handles.
func (e *XSelectIndexExec) executeTask(task *lookupTableTask) error {
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

func (e *XSelectIndexExec) extractRowsFromTableResult(t table.Table, tblResult distsql.SelectResult) ([]*Row, error) {
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

func (e *XSelectIndexExec) extractRowsFromPartialResult(t table.Table, partialResult distsql.PartialResult) ([]*Row, error) {
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

func (e *XSelectIndexExec) doTableRequest(handles []int64) (distsql.SelectResult, error) {
	// The handles are not in original index order, so we can't push limit here.
	selTableReq := new(tipb.SelectRequest)
	if e.indexPlan.OutOfOrder {
		selTableReq.Limit = e.indexPlan.LimitCount
	}
	selTableReq.StartTs = e.startTS
	selTableReq.TimeZoneOffset = timeZoneOffset()
	selTableReq.Flags = statementContextToFlags(e.ctx.GetSessionVars().StmtCtx)
	selTableReq.TableInfo = &tipb.TableInfo{
		TableId: e.table.Meta().ID,
	}
	selTableReq.TableInfo.Columns = distsql.ColumnsToProto(e.indexPlan.Columns, e.table.Meta().PKIsHandle)
	selTableReq.Where = e.where
	// Aggregate Info
	selTableReq.Aggregates = e.aggFuncs
	selTableReq.GroupBy = e.byItems
	keyRanges := tableHandlesToKVRanges(e.table.Meta().ID, handles)

	resp, err := distsql.Select(e.ctx.GetClient(), selTableReq, keyRanges, e.scanConcurrency, false)
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

// XSelectTableExec represents the DistSQL select table executor.
// Its execution is pushed down to KV layer.
type XSelectTableExec struct {
	tableInfo   *model.TableInfo
	table       table.Table
	asName      *model.CIStr
	ctx         context.Context
	supportDesc bool
	isMemDB     bool

	// result returns one or more distsql.PartialResult and each PartialResult is return by one region.
	result        distsql.SelectResult
	partialResult distsql.PartialResult

	where        *tipb.Expr
	Columns      []*model.ColumnInfo
	schema       expression.Schema
	ranges       []plan.TableRange
	desc         bool
	limitCount   *int64
	returnedRows uint64 // returned rowCount
	keepOrder    bool
	startTS      uint64
	orderByList  []*tipb.ByItem

	/*
	   The following attributes are used for aggregation push down.
	   aggFuncs is the aggregation functions in protobuf format. They will be added to distsql request msg.
	   byItem is the groupby items in protobuf format. They will be added to distsql request msg.
	   aggFields is used to decode returned rows from distsql.
	   aggregate indicates of the executor is handling aggregate result.
	   It is more convenient to use a single varible than use a long condition.
	*/
	aggFuncs  []*tipb.Expr
	byItems   []*tipb.ByItem
	aggFields []*types.FieldType
	aggregate bool

	scanConcurrency int
	execStart       time.Time
	partialCount    int
}

// Schema implements the Executor Schema interface.
func (e *XSelectTableExec) Schema() expression.Schema {
	return e.schema
}

// doRequest sends a *tipb.SelectRequest via kv.Client and gets the distsql.SelectResult.
func (e *XSelectTableExec) doRequest() error {
	var err error
	selReq := new(tipb.SelectRequest)
	selReq.StartTs = e.startTS
	selReq.TimeZoneOffset = timeZoneOffset()
	selReq.Flags = statementContextToFlags(e.ctx.GetSessionVars().StmtCtx)
	selReq.Where = e.where
	selReq.TableInfo = &tipb.TableInfo{
		TableId: e.tableInfo.ID,
		Columns: distsql.ColumnsToProto(e.Columns, e.tableInfo.PKIsHandle),
	}
	if len(e.orderByList) > 0 {
		selReq.OrderBy = e.orderByList
	} else if e.supportDesc && e.desc {
		selReq.OrderBy = []*tipb.ByItem{{Desc: e.desc}}
	}
	selReq.Limit = e.limitCount
	// Aggregate Info
	selReq.Aggregates = e.aggFuncs
	selReq.GroupBy = e.byItems

	kvRanges := tableRangesToKVRanges(e.table.Meta().ID, e.ranges)
	e.result, err = distsql.Select(e.ctx.GetClient(), selReq, kvRanges, e.scanConcurrency, e.keepOrder)
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

// Close implements the Executor Close interface.
func (e *XSelectTableExec) Close() error {
	err := closeAll(e.result, e.partialResult)
	if err != nil {
		return errors.Trace(err)
	}
	e.result = nil
	e.partialResult = nil
	e.returnedRows = 0
	e.partialCount = 0
	return nil
}

// Next implements the Executor interface.
func (e *XSelectTableExec) Next() (*Row, error) {
	if e.limitCount != nil && e.returnedRows >= uint64(*e.limitCount) {
		return nil, nil
	}
	if e.result == nil {
		e.execStart = time.Now()
		err := e.doRequest()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
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
				duration := time.Since(e.execStart)
				if duration > minLogDuration {
					connID := e.ctx.GetSessionVars().ConnectionID
					log.Infof("[%d] [TIME_TABLE_SCAN] %s", connID, e.slowQueryInfo(duration))
				}
				return nil, nil
			}
			e.partialCount++
		}
		// Get a row from partial result.
		h, rowData, err := e.partialResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rowData == nil {
			// Finish the current partial result and get the next one.
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

func (e *XSelectTableExec) slowQueryInfo(duration time.Duration) string {
	return fmt.Sprintf("time: %v, table: %s(%d), partials: %d, concurrency: %d, start_ts: %d, rows: %d",
		duration, e.tableInfo.Name, e.tableInfo.ID, e.partialCount, e.scanConcurrency, e.startTS, e.returnedRows)
}

// timeZoneOffset returns the local time zone offset in seconds.
func timeZoneOffset() int64 {
	_, offset := time.Now().Zone()
	return int64(offset)
}

// statementContextToFlags converts StatementContext to tipb.SelectRequest.Flags.
func statementContextToFlags(sc *variable.StatementContext) uint64 {
	var flags uint64
	if sc.IgnoreTruncate {
		flags |= xeval.FlagIgnoreTruncate
	} else if sc.TruncateAsWarning {
		flags |= xeval.FlagTruncateAsWarning
	}
	return flags
}
