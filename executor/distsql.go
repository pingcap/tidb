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
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
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
	handles []int64
	rows    []Row
	cursor  int

	done   bool
	doneCh chan error

	// indexOrder map is used to save the original index order for the handles.
	// Without this map, the original index order might be lost.
	// The handles fetched from index is originally ordered by index, but we need handles to be ordered by itself
	// to do table request.
	indexOrder map[int64]int
}

func (task *lookupTableTask) getRow() (Row, error) {
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
	order     map[int64]int
	rows      []Row
	handleIdx int
}

func (s *rowsSorter) Less(i, j int) bool {
	x := s.order[s.rows[i][s.handleIdx].GetInt64()]
	y := s.order[s.rows[j][s.handleIdx].GetInt64()]
	return x < y
}

func (s *rowsSorter) Len() int {
	return len(s.rows)
}

func (s *rowsSorter) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

func tableRangesToKVRanges(tid int64, tableRanges []types.IntColumnRange) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(tableRanges))
	for _, tableRange := range tableRanges {
		startKey := tablecodec.EncodeRowKeyWithHandle(tid, tableRange.LowVal)

		var endKey kv.Key
		if tableRange.HighVal != math.MaxInt64 {
			endKey = tablecodec.EncodeRowKeyWithHandle(tid, tableRange.HighVal+1)
		} else {
			endKey = tablecodec.EncodeRowKeyWithHandle(tid, tableRange.HighVal).Next()
		}
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

// indexValuesToKVRanges will convert the index datums to kv ranges.
func indexValuesToKVRanges(tid, idxID int64, values [][]types.Datum) ([]kv.KeyRange, error) {
	krs := make([]kv.KeyRange, 0, len(values))
	for _, vals := range values {
		// TODO: We don't process the case that equal key has different types.
		valKey, err := codec.EncodeKey(nil, vals...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		valKeyNext := []byte(kv.Key(valKey).PrefixNext())
		rangeBeginKey := tablecodec.EncodeIndexSeekKey(tid, idxID, valKey)
		rangeEndKey := tablecodec.EncodeIndexSeekKey(tid, idxID, valKeyNext)
		krs = append(krs, kv.KeyRange{StartKey: rangeBeginKey, EndKey: rangeEndKey})
	}
	return krs, nil
}

func indexRangesToKVRanges(tid, idxID int64, ranges []*types.IndexRange) ([]kv.KeyRange, error) {
	krs := make([]kv.KeyRange, 0, len(ranges))
	for _, ran := range ranges {
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
	defer terror.Call(subResult.Close)
	var (
		handles     []int64
		handleDatum types.Datum
	)
	handleType := types.NewFieldType(mysql.TypeLonglong)
	for {
		data, err := subResult.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data == nil {
			break
		}
		handleDatum, err = tablecodec.DecodeColumnValue(data[0].GetRaw(), handleType, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		handles = append(handles, handleDatum.GetInt64())
	}
	return handles, nil
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
	return errors.Trace(err)
}

func decodeRawValues(values []types.Datum, schema *expression.Schema, loc *time.Location) error {
	var err error
	for i := 0; i < schema.Len(); i++ {
		if values[i].Kind() == types.KindRaw {
			values[i], err = tablecodec.DecodeColumnValue(values[i].GetRaw(), schema.Columns[i].RetType, loc)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// timeZoneOffset returns the local time zone offset in seconds.
func timeZoneOffset(ctx context.Context) int64 {
	loc := ctx.GetSessionVars().GetTimeZone()
	_, offset := time.Now().In(loc).Zone()
	return int64(offset)
}

// Flags are used by tipb.SelectRequest.Flags to handle execution mode, like how to handle truncate error.
const (
	// FlagIgnoreTruncate indicates if truncate error should be ignored.
	// Read-only statements should ignore truncate error, write statements should not ignore truncate error.
	FlagIgnoreTruncate uint64 = 1
	// FlagTruncateAsWarning indicates if truncate error should be returned as warning.
	// This flag only matters if FlagIgnoreTruncate is not set, in strict sql mode, truncate error should
	// be returned as error, in non-strict sql mode, truncate error should be saved as warning.
	FlagTruncateAsWarning uint64 = 1 << 1

	// FlagPadCharToFullLength indicates if sql_mode 'PAD_CHAR_TO_FULL_LENGTH' is set.
	FlagPadCharToFullLength uint64 = 1 << 2
)

// statementContextToFlags converts StatementContext to tipb.SelectRequest.Flags.
func statementContextToFlags(sc *stmtctx.StatementContext) uint64 {
	var flags uint64
	if sc.IgnoreTruncate {
		flags |= FlagIgnoreTruncate
	} else if sc.TruncateAsWarning {
		flags |= FlagTruncateAsWarning
	}
	if sc.PadCharToFullLength {
		flags |= FlagPadCharToFullLength
	}
	return flags
}

func setPBColumnsDefaultValue(ctx context.Context, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
	for i, c := range columns {
		if c.OriginDefaultValue == nil {
			continue
		}

		sessVars := ctx.GetSessionVars()
		originStrict := sessVars.StrictSQLMode
		sessVars.StrictSQLMode = false
		d, err := table.GetColOriginDefaultValue(ctx, c)
		sessVars.StrictSQLMode = originStrict
		if err != nil {
			return errors.Trace(err)
		}

		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(d, ctx.GetSessionVars().GetTimeZone())
		if err != nil {
			return errors.Trace(err)
		}
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

// TableReaderExecutor sends dag request and reads table data from kv layer.
type TableReaderExecutor struct {
	baseExecutor

	table     table.Table
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []types.IntColumnRange
	dagPB     *tipb.DAGRequest
	// columns are only required by union scan.
	columns []*model.ColumnInfo

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result        distsql.SelectResult
	partialResult distsql.PartialResult
	priority      int
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

// NextChunk implements the Executor NextChunk interface.
func (e *TableReaderExecutor) NextChunk(chk *chunk.Chunk) error {
	return e.result.NextChunk(chk)
}

// Open implements the Executor Open interface.
func (e *TableReaderExecutor) Open(goCtx goctx.Context) error {
	span, goCtx := startSpanFollowsContext(goCtx, "executor.TableReader.Open")
	defer span.Finish()

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
	e.result, err = distsql.SelectDAG(goCtx, e.ctx.GetClient(), kvReq, e.schema.GetTypes(), e.ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(goCtx)
	return nil
}

// startSpanFollowContext is similar to opentracing.StartSpanFromContext, but the span reference use FollowsFrom option.
func startSpanFollowsContext(goCtx goctx.Context, operationName string) (opentracing.Span, goctx.Context) {
	span := opentracing.SpanFromContext(goCtx)
	if span != nil {
		span = opentracing.StartSpan(operationName, opentracing.FollowsFrom(span.Context()))
	} else {
		span = opentracing.StartSpan(operationName)
	}

	return span, opentracing.ContextWithSpan(goCtx, span)
}

// IndexReaderExecutor sends dag request and reads index data from kv layer.
type IndexReaderExecutor struct {
	baseExecutor

	table     table.Table
	index     *model.IndexInfo
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []*types.IndexRange
	dagPB     *tipb.DAGRequest

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result        distsql.SelectResult
	partialResult distsql.PartialResult
	// columns are only required by union scan.
	columns  []*model.ColumnInfo
	priority int
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

// NextChunk implements the Executor NextChunk interface.
func (e *IndexReaderExecutor) NextChunk(chk *chunk.Chunk) error {
	return e.result.NextChunk(chk)
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open(goCtx goctx.Context) error {
	span, goCtx := startSpanFollowsContext(goCtx, "executor.IndexReader.Open")
	defer span.Finish()

	var builder requestBuilder
	kvReq, err := builder.SetIndexRanges(e.tableID, e.index.ID, e.ranges).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return errors.Trace(err)
	}
	e.result, err = distsql.SelectDAG(goCtx, e.ctx.GetClient(), kvReq, e.schema.GetTypes(), e.ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return errors.Trace(err)
	}
	e.result.Fetch(goCtx)
	return nil
}

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
	// handleIdx is the index of handle, which is only used for case of keeping order.
	handleIdx    int
	tableRequest *tipb.DAGRequest
	// columns are only required by union scan.
	columns  []*model.ColumnInfo
	priority int
	*dataReaderBuilder
	// All fields above is immutable.

	indexWorker
	tableWorker
	finished chan struct{}

	// batchSize is for slow startup. It will slowly increase to the max batch size value.
	batchSize int

	resultCh   chan *lookupTableTask
	resultCurr *lookupTableTask
}

// indexWorker is used by IndexLookUpExecutor to maintain index lookup background goroutines.
type indexWorker struct {
	wg sync.WaitGroup
}

// startIndexWorker launch a background goroutine to fetch handles, send the results to workCh.
func (e *IndexLookUpExecutor) startIndexWorker(goCtx goctx.Context, kvRanges []kv.KeyRange, workCh chan<- *lookupTableTask, finished <-chan struct{}) error {
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
	result, err := distsql.SelectDAG(goCtx, e.ctx.GetClient(), kvReq, []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, e.ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return errors.Trace(err)
	}
	result.Fetch(goCtx)
	worker := &e.indexWorker
	worker.wg.Add(1)
	go func() {
		goCtx1, cancel := goctx.WithCancel(goCtx)
		worker.fetchHandles(e, result, workCh, goCtx1, finished)
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
func (worker *indexWorker) fetchHandles(e *IndexLookUpExecutor, result distsql.SelectResult, workCh chan<- *lookupTableTask, goCtx goctx.Context, finished <-chan struct{}) {
	for {
		handles, finish, err := extractHandlesFromIndexResult(result)
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
			case <-goCtx.Done():
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
func (e *IndexLookUpExecutor) startTableWorker(goCtx goctx.Context, workCh <-chan *lookupTableTask, finished <-chan struct{}) {
	worker := &e.tableWorker
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency
	worker.wg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		goCtx1, cancel := goctx.WithCancel(goCtx)
		go func() {
			worker.pickAndExecTask(e, workCh, goCtx1, finished)
			cancel()
			worker.wg.Done()
		}()
	}
}

// pickAndExecTask picks tasks from workCh, and execute them.
func (worker *tableWorker) pickAndExecTask(e *IndexLookUpExecutor, workCh <-chan *lookupTableTask, goCtx goctx.Context, finished <-chan struct{}) {
	for {
		// Don't check ctx.Done() on purpose. If background worker get the signal and all
		// exit immediately, session's goroutine doesn't know this and still calling Next(),
		// it may block reading task.doneCh forever.
		select {
		case task, ok := <-workCh:
			if !ok {
				return
			}
			e.executeTask(task, goCtx)
		case <-finished:
			return
		}
	}
}

func (worker *tableWorker) close() {
	worker.wg.Wait()
}

// Open implements the Executor Open interface.
func (e *IndexLookUpExecutor) Open(goCtx goctx.Context) error {
	kvRanges, err := e.indexRangesToKVRanges()
	if err != nil {
		return errors.Trace(err)
	}
	return e.open(goCtx, kvRanges)
}

func (e *IndexLookUpExecutor) open(goCtx goctx.Context, kvRanges []kv.KeyRange) error {
	span, goCtx := startSpanFollowsContext(goCtx, "executor.IndexLookUp.Open")
	defer span.Finish()

	e.finished = make(chan struct{})
	e.indexWorker = indexWorker{}
	e.tableWorker = tableWorker{}
	e.resultCh = make(chan *lookupTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))

	// indexWorker will write to workCh and tableWorker will read from workCh,
	// so fetching index and getting table data can run concurrently.
	workCh := make(chan *lookupTableTask, 1)
	err := e.startIndexWorker(goCtx, kvRanges, workCh, e.finished)
	if err != nil {
		return errors.Trace(err)
	}
	e.startTableWorker(goCtx, workCh, e.finished)
	return nil
}

func (e *IndexLookUpExecutor) indexRangesToKVRanges() ([]kv.KeyRange, error) {
	return indexRangesToKVRanges(e.tableID, e.index.ID, e.ranges)
}

// executeTask executes the table look up tasks. We will construct a table reader and send request by handles.
// Then we hold the returning rows and finish this task.
func (e *IndexLookUpExecutor) executeTask(task *lookupTableTask, goCtx goctx.Context) {
	var err error
	defer func() {
		task.doneCh <- errors.Trace(err)
	}()

	var tableReader Executor
	tableReader, err = e.dataReaderBuilder.buildTableReaderFromHandles(goCtx, &TableReaderExecutor{
		baseExecutor: newBaseExecutor(e.schema, e.ctx),
		table:        e.table,
		tableID:      e.tableID,
		dagPB:        e.tableRequest,
	}, task.handles)
	if err != nil {
		log.Error(err)
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
		sorter := &rowsSorter{order: task.indexOrder, rows: task.rows, handleIdx: e.handleIdx}
		sort.Sort(sorter)
	}
}

func (e *IndexLookUpExecutor) buildTableTasks(handles []int64) []*lookupTableTask {
	// Build tasks with increasing batch size.
	var taskSizes []int
	for remained := len(handles); remained > 0; e.batchSize *= 2 {
		if e.batchSize > e.ctx.GetSessionVars().IndexLookupSize {
			e.batchSize = e.ctx.GetSessionVars().IndexLookupSize
		}
		if e.batchSize > remained {
			taskSizes = append(taskSizes, remained)
			break
		}
		taskSizes = append(taskSizes, e.batchSize)
		remained -= e.batchSize
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

func (builder *requestBuilder) SetIndexRanges(tid, idxID int64, ranges []*types.IndexRange) *requestBuilder {
	if builder.err != nil {
		return builder
	}
	builder.Request.KeyRanges, builder.err = indexRangesToKVRanges(tid, idxID, ranges)
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

func getIsolationLevel(sv *variable.SessionVars) kv.IsoLevel {
	if sv.Systems[variable.TxnIsolation] == ast.ReadCommitted {
		return kv.RC
	}
	return kv.SI
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
