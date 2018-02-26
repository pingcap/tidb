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

	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
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
	rowIdx  []int // rowIdx represents the handle index for every row. Only used when keep order.
	rows    []chunk.Row
	cursor  int

	doneCh chan error

	// indexOrder map is used to save the original index order for the handles.
	// Without this map, the original index order might be lost.
	// The handles fetched from index is originally ordered by index, but we need handles to be ordered by itself
	// to do table request.
	indexOrder map[int64]int
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

func (task *lookupTableTask) getRow(schema *expression.Schema) (Row, error) {
	if task.cursor < len(task.rows) {
		row := task.rows[task.cursor]
		task.cursor++
		datumRow := make(types.DatumRow, row.Len())
		for i := 0; i < len(datumRow); i++ {
			datumRow[i] = row.GetDatum(i, schema.Columns[i].RetType)
		}
		return datumRow, nil
	}

	return nil, nil
}

func (w *indexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult) (handles []int64, err error) {
	handles = make([]int64, 0, w.batchSize)
	for len(handles) < w.batchSize {
		e0 := idxResult.NextChunk(ctx, chk)
		if e0 != nil {
			err = errors.Trace(e0)
			return
		}
		if chk.NumRows() == 0 {
			return
		}
		for i := 0; i < chk.NumRows(); i++ {
			handles = append(handles, chk.GetRow(i).GetInt64(0))
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return
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
func timeZoneOffset(ctx sessionctx.Context) int64 {
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

func setPBColumnsDefaultValue(ctx sessionctx.Context, pbColumns []*tipb.ColumnInfo, columns []*model.ColumnInfo) error {
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

		pbColumns[i].DefaultVal, err = tablecodec.EncodeValue(ctx.GetSessionVars().StmtCtx, d)
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
	ranges    []*ranger.NewRange
	dagPB     *tipb.DAGRequest
	// columns are only required by union scan.
	columns []*model.ColumnInfo

	// partialResult store the result from one region.
	partialResult distsql.PartialResult
	// resultHandler handles the order of the result. Since (MAXInt64, MAXUint64] stores before [0, MaxInt64] physically
	// for unsigned int.
	resultHandler *tableResultHandler
	priority      int
	feedback      *statistics.QueryFeedback
}

// Close implements the Executor Close interface.
func (e *TableReaderExecutor) Close() error {
	e.feedback.SetIntRanges(e.ranges).SetActual(e.resultHandler.totalCount())
	e.ctx.StoreQueryFeedback(e.feedback)
	err := closeAll(e.resultHandler, e.partialResult)
	e.partialResult = nil
	return errors.Trace(err)
}

// Next returns next available Row. In its process, any error will be returned.
// It first try to fetch a partial result if current partial result is nil.
// If any error appears during this stage, it simply return a error to its caller.
// If it successfully initialize its partial result, it will use this to get next
// available row.
func (e *TableReaderExecutor) Next(ctx context.Context) (Row, error) {
	for {
		// Get partial result.
		if e.partialResult == nil {
			var err error
			e.partialResult, err = e.resultHandler.next(ctx)
			if err != nil {
				e.feedback.Invalidate()
				return nil, errors.Trace(err)
			}
			if e.partialResult == nil {
				// Finished.
				return nil, nil
			}
		}
		// Get a row from partial result.
		rowData, err := e.partialResult.Next(ctx)
		if err != nil {
			e.feedback.Invalidate()
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
			e.feedback.Invalidate()
			return nil, errors.Trace(err)
		}
		return rowData, nil
	}
}

// NextChunk fills data into the chunk passed by its caller.
// The task was actually done by tableReaderHandler.
func (e *TableReaderExecutor) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	err := e.resultHandler.nextChunk(ctx, chk)
	if err != nil {
		e.feedback.Invalidate()
	}
	return errors.Trace(err)
}

// Open initialzes necessary variables for using this executor.
func (e *TableReaderExecutor) Open(ctx context.Context) error {
	span, ctx := startSpanFollowsContext(ctx, "executor.TableReader.Open")
	defer span.Finish()

	e.resultHandler = &tableResultHandler{}
	firstPartRanges, secondPartRanges := splitRanges(e.ranges, e.keepOrder)
	firstResult, err := e.buildResp(ctx, firstPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(ctx, secondPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	e.resultHandler.open(firstResult, secondResult)
	return nil
}

// buildResp first build request and send it to tikv using distsql.Select. It uses SelectResut returned by the callee
// to fetch all results.
func (e *TableReaderExecutor) buildResp(ctx context.Context, ranges []*ranger.NewRange) (distsql.SelectResult, error) {
	var builder distsql.RequestBuilder
	kvReq, err := builder.SetTableRanges(e.tableID, ranges).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		return nil, errors.Trace(err)
	}
	result, err := distsql.Select(ctx, e.ctx, kvReq, e.retTypes())
	if err != nil {
		return nil, errors.Trace(err)
	}
	result.Fetch(ctx)
	return result, nil
}

func splitRanges(ranges []*ranger.NewRange, keepOrder bool) ([]*ranger.NewRange, []*ranger.NewRange) {
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
		return signedRanges, unsignedRanges
	}
	signedRanges := make([]*ranger.NewRange, 0, idx+1)
	unsignedRanges := make([]*ranger.NewRange, 0, len(ranges)-idx)
	signedRanges = append(signedRanges, ranges[0:idx]...)
	signedRanges = append(signedRanges, &ranger.NewRange{
		LowVal:     ranges[idx].LowVal,
		LowExclude: ranges[idx].LowExclude,
		HighVal:    []types.Datum{types.NewUintDatum(math.MaxInt64)},
	})
	unsignedRanges = append(unsignedRanges, &ranger.NewRange{
		LowVal:      []types.Datum{types.NewUintDatum(math.MaxInt64 + 1)},
		HighVal:     ranges[idx].HighVal,
		HighExclude: ranges[idx].HighExclude,
	})
	if idx < len(ranges) {
		unsignedRanges = append(unsignedRanges, ranges[idx+1:]...)
	}
	if !keepOrder {
		return append(unsignedRanges, signedRanges...), nil
	}
	return signedRanges, unsignedRanges
}

// startSpanFollowContext is similar to opentracing.StartSpanFromContext, but the span reference use FollowsFrom option.
func startSpanFollowsContext(ctx context.Context, operationName string) (opentracing.Span, context.Context) {
	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span = opentracing.StartSpan(operationName, opentracing.FollowsFrom(span.Context()))
	} else {
		span = opentracing.StartSpan(operationName)
	}

	return span, opentracing.ContextWithSpan(ctx, span)
}

// IndexReaderExecutor sends dag request and reads index data from kv layer.
type IndexReaderExecutor struct {
	baseExecutor

	table     table.Table
	index     *model.IndexInfo
	tableID   int64
	keepOrder bool
	desc      bool
	ranges    []*ranger.NewRange
	dagPB     *tipb.DAGRequest

	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result        distsql.SelectResult
	partialResult distsql.PartialResult
	// columns are only required by union scan.
	columns  []*model.ColumnInfo
	priority int
	feedback *statistics.QueryFeedback
}

// Close clears all resources hold by current object.
func (e *IndexReaderExecutor) Close() error {
	e.feedback.SetIndexRanges(e.ranges).SetActual(e.result.ScanKeys())
	e.ctx.StoreQueryFeedback(e.feedback)
	err := closeAll(e.result, e.partialResult)
	e.result = nil
	e.partialResult = nil
	return errors.Trace(err)
}

// Next returns next available Row. In its process, any error will be returned.
// It first try to fetch a partial result if current partial result is nil.
// If any error appears during this stage, it simply return a error to its caller.
// If it successfully initialize its partial result, it will use this to get next
// available row.
func (e *IndexReaderExecutor) Next(ctx context.Context) (Row, error) {
	for {
		// Get partial result.
		if e.partialResult == nil {
			var err error
			e.partialResult, err = e.result.Next(ctx)
			if err != nil {
				e.feedback.Invalidate()
				return nil, errors.Trace(err)
			}
			if e.partialResult == nil {
				// Finished.
				return nil, nil
			}
		}
		// Get a row from partial result.
		rowData, err := e.partialResult.Next(ctx)
		if err != nil {
			e.feedback.Invalidate()
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
			e.feedback.Invalidate()
			return nil, errors.Trace(err)
		}
		return rowData, nil
	}
}

// NextChunk implements the Executor NextChunk interface.
func (e *IndexReaderExecutor) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	err := e.result.NextChunk(ctx, chk)
	if err != nil {
		e.feedback.Invalidate()
	}
	return errors.Trace(err)
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open(ctx context.Context) error {
	kvRanges, err := distsql.IndexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.tableID, e.index.ID, e.ranges)
	if err != nil {
		return errors.Trace(err)
	}
	return e.open(ctx, kvRanges)
}

func (e *IndexReaderExecutor) open(ctx context.Context, kvRanges []kv.KeyRange) error {
	span, ctx := startSpanFollowsContext(ctx, "executor.IndexReader.Open")
	defer span.Finish()

	var builder distsql.RequestBuilder
	kvReq, err := builder.SetKeyRanges(kvRanges).
		SetDAGRequest(e.dagPB).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetPriority(e.priority).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		Build()
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	e.result, err = distsql.Select(ctx, e.ctx, kvReq, e.retTypes())
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	e.result.Fetch(ctx)
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
	ranges    []*ranger.NewRange
	dagPB     *tipb.DAGRequest
	// handleIdx is the index of handle, which is only used for case of keeping order.
	handleIdx    int
	tableRequest *tipb.DAGRequest
	// columns are only required by union scan.
	columns  []*model.ColumnInfo
	priority int
	*dataReaderBuilder
	// All fields above are immutable.

	idxWorkerWg sync.WaitGroup
	tblWorkerWg sync.WaitGroup
	finished    chan struct{}

	resultCh   chan *lookupTableTask
	resultCurr *lookupTableTask
	feedback   *statistics.QueryFeedback
}

// indexWorker is used by IndexLookUpExecutor to maintain index lookup background goroutines.
type indexWorker struct {
	workCh    chan<- *lookupTableTask
	finished  <-chan struct{}
	resultCh  chan<- *lookupTableTask
	keepOrder bool

	// batchSize is for lightweight startup. It will be increased exponentially until reaches the max batch size value.
	batchSize    int
	maxBatchSize int
}

// startIndexWorker launch a background goroutine to fetch handles, send the results to workCh.
func (e *IndexLookUpExecutor) startIndexWorker(ctx context.Context, kvRanges []kv.KeyRange, workCh chan<- *lookupTableTask) error {
	var builder distsql.RequestBuilder
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
	result, err := distsql.Select(ctx, e.ctx, kvReq, []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)})
	if err != nil {
		return errors.Trace(err)
	}
	result.Fetch(ctx)
	worker := &indexWorker{
		workCh:       workCh,
		finished:     e.finished,
		resultCh:     e.resultCh,
		keepOrder:    e.keepOrder,
		batchSize:    e.maxChunkSize,
		maxBatchSize: e.ctx.GetSessionVars().IndexLookupSize,
	}
	if worker.batchSize > worker.maxBatchSize {
		worker.batchSize = worker.maxBatchSize
	}
	e.idxWorkerWg.Add(1)
	go func() {
		ctx1, cancel := context.WithCancel(ctx)
		err := worker.fetchHandles(ctx1, result)
		scanCount := result.ScanKeys()
		if err != nil {
			scanCount = -1
		}
		e.feedback.SetIndexRanges(e.ranges).SetActual(scanCount)
		e.ctx.StoreQueryFeedback(e.feedback)
		cancel()
		if err := result.Close(); err != nil {
			log.Error("close Select result failed:", errors.ErrorStack(err))
		}
		close(workCh)
		close(e.resultCh)
		e.idxWorkerWg.Done()
	}()
	return nil
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are sent to workCh to be further processed by tableWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (w *indexWorker) fetchHandles(ctx context.Context, result distsql.SelectResult) error {
	chk := chunk.NewChunk([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)})
	for {
		handles, err := w.extractTaskHandles(ctx, chk, result)
		if err != nil {
			doneCh := make(chan error, 1)
			doneCh <- errors.Trace(err)
			w.resultCh <- &lookupTableTask{
				doneCh: doneCh,
			}
			return err
		}
		if len(handles) == 0 {
			return nil
		}
		task := w.buildTableTask(handles)
		select {
		case <-ctx.Done():
			return nil
		case <-w.finished:
			return nil
		case w.workCh <- task:
			w.resultCh <- task
		}
	}
}

// tableWorker is used by IndexLookUpExecutor to maintain table lookup background goroutines.
type tableWorker struct {
	workCh         <-chan *lookupTableTask
	finished       <-chan struct{}
	buildTblReader func(ctx context.Context, handles []int64) (Executor, error)
	keepOrder      bool
	handleIdx      int
}

func (e *IndexLookUpExecutor) buildTableReader(ctx context.Context, handles []int64) (Executor, error) {
	tableReader, err := e.dataReaderBuilder.buildTableReaderFromHandles(ctx, &TableReaderExecutor{
		baseExecutor: newBaseExecutor(e.ctx, e.schema, e.id+"_tableReader"),
		table:        e.table,
		tableID:      e.tableID,
		dagPB:        e.tableRequest,
		priority:     e.priority,
		feedback:     statistics.NewQueryFeedback(0, 0, false, 0, 0),
	}, handles)
	if err != nil {
		log.Error(err)
		return nil, errors.Trace(err)
	}
	return tableReader, nil
}

// startTableWorker launchs some background goroutines which pick tasks from workCh and execute the task.
func (e *IndexLookUpExecutor) startTableWorker(ctx context.Context, workCh <-chan *lookupTableTask) {
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		worker := &tableWorker{
			workCh:         workCh,
			finished:       e.finished,
			buildTblReader: e.buildTableReader,
			keepOrder:      e.keepOrder,
			handleIdx:      e.handleIdx,
		}
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			worker.pickAndExecTask(ctx1)
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
}

// pickAndExecTask picks tasks from workCh, and execute them.
func (w *tableWorker) pickAndExecTask(ctx context.Context) {
	for {
		// Don't check ctx.Done() on purpose. If background worker get the signal and all
		// exit immediately, session's goroutine doesn't know this and still calling Next(),
		// it may block reading task.doneCh forever.
		select {
		case task, ok := <-w.workCh:
			if !ok {
				return
			}
			w.executeTask(ctx, task)
		case <-w.finished:
			return
		}
	}
}

// Open implements the Executor Open interface.
func (e *IndexLookUpExecutor) Open(ctx context.Context) error {
	kvRanges, err := distsql.IndexRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, e.tableID, e.index.ID, e.ranges)
	if err != nil {
		e.feedback.Invalidate()
		return errors.Trace(err)
	}
	err = e.open(ctx, kvRanges)
	if err != nil {
		e.feedback.Invalidate()
	}
	return errors.Trace(err)
}

func (e *IndexLookUpExecutor) open(ctx context.Context, kvRanges []kv.KeyRange) error {
	span, ctx := startSpanFollowsContext(ctx, "executor.IndexLookUp.Open")
	defer span.Finish()

	e.finished = make(chan struct{})
	e.resultCh = make(chan *lookupTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))

	// indexWorker will write to workCh and tableWorker will read from workCh,
	// so fetching index and getting table data can run concurrently.
	workCh := make(chan *lookupTableTask, 1)
	err := e.startIndexWorker(ctx, kvRanges, workCh)
	if err != nil {
		return errors.Trace(err)
	}
	e.startTableWorker(ctx, workCh)
	return nil
}

// executeTask executes the table look up tasks. We will construct a table reader and send request by handles.
// Then we hold the returning rows and finish this task.
func (w *tableWorker) executeTask(ctx context.Context, task *lookupTableTask) {
	var err error
	defer func() {
		task.doneCh <- errors.Trace(err)
	}()
	var tableReader Executor
	tableReader, err = w.buildTblReader(ctx, task.handles)
	if err != nil {
		log.Error(err)
		return
	}
	defer terror.Call(tableReader.Close)
	task.rows = make([]chunk.Row, 0, len(task.handles))
	for {
		chk := tableReader.newChunk()
		err = tableReader.NextChunk(ctx, chk)
		if err != nil {
			log.Error(err)
			return
		}
		if chk.NumRows() == 0 {
			break
		}
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			task.rows = append(task.rows, row)
		}
	}
	if w.keepOrder {
		task.rowIdx = make([]int, 0, len(task.rows))
		for i := range task.rows {
			handle := task.rows[i].GetInt64(w.handleIdx)
			task.rowIdx = append(task.rowIdx, task.indexOrder[handle])
		}
		sort.Sort(task)
	}
}

func (w *indexWorker) buildTableTask(handles []int64) *lookupTableTask {
	var indexOrder map[int64]int
	if w.keepOrder {
		// Save the index order.
		indexOrder = make(map[int64]int, len(handles))
		for i, h := range handles {
			indexOrder[h] = i
		}
	}
	task := &lookupTableTask{
		handles:    handles,
		indexOrder: indexOrder,
	}
	task.doneCh = make(chan error, 1)
	return task
}

// Close implements Exec Close interface.
func (e *IndexLookUpExecutor) Close() error {
	if e.finished != nil {
		close(e.finished)
		// Drain the resultCh and discard the result, in case that Next() doesn't fully
		// consume the data, background worker still writing to resultCh and block forever.
		for range e.resultCh {
		}
		e.idxWorkerWg.Wait()
		e.tblWorkerWg.Wait()
		e.finished = nil
	}
	return nil
}

// Next implements Exec Next interface.
func (e *IndexLookUpExecutor) Next(ctx context.Context) (Row, error) {
	for {
		resultTask, err := e.getResultTask()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if resultTask == nil {
			return nil, nil
		}
		row, err := resultTask.getRow(e.schema)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			return row, nil
		}
	}
}

// NextChunk implements Exec NextChunk interface.
func (e *IndexLookUpExecutor) NextChunk(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for {
		resultTask, err := e.getResultTask()
		if err != nil {
			return errors.Trace(err)
		}
		if resultTask == nil {
			return nil
		}
		for resultTask.cursor < len(resultTask.rows) {
			chk.AppendRow(resultTask.rows[resultTask.cursor])
			resultTask.cursor++
			if chk.NumRows() >= e.maxChunkSize {
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
	err := <-task.doneCh
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.resultCurr = task
	return e.resultCurr, nil
}

type tableResultHandler struct {
	// If the pk is unsigned and we have KeepOrder=true.
	// optionalResult handles the request whose range is in signed int range.
	// result handles the request whose range is exceed signed int range.
	// Otherwise, we just set optionalFinished true and the result handles the whole ranges.
	optionalResult distsql.SelectResult
	result         distsql.SelectResult

	optionalFinished bool
}

func (tr *tableResultHandler) open(optionalResult, result distsql.SelectResult) {
	if optionalResult == nil {
		tr.optionalFinished = true
		tr.result = result
		return
	}
	tr.optionalResult = optionalResult
	tr.result = result
	tr.optionalFinished = false
}

func (tr *tableResultHandler) next(ctx context.Context) (partialResult distsql.PartialResult, err error) {
	if !tr.optionalFinished {
		partialResult, err = tr.optionalResult.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if partialResult != nil {
			return partialResult, nil
		}
		tr.optionalFinished = true
	}
	partialResult, err = tr.result.Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return partialResult, nil
}

func (tr *tableResultHandler) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	if !tr.optionalFinished {
		err := tr.optionalResult.NextChunk(ctx, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() > 0 {
			return nil
		}
		tr.optionalFinished = true
	}
	return tr.result.NextChunk(ctx, chk)
}

func (tr *tableResultHandler) nextRaw(ctx context.Context) (data []byte, err error) {
	if !tr.optionalFinished {
		data, err = tr.optionalResult.NextRaw(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if data != nil {
			return data, nil
		}
		tr.optionalFinished = true
	}
	data, err = tr.result.NextRaw(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

func (tr *tableResultHandler) Close() error {
	err := closeAll(tr.optionalResult, tr.result)
	tr.optionalResult, tr.result = nil, nil
	return errors.Trace(err)
}

func (tr *tableResultHandler) totalCount() (count int64) {
	if tr.optionalResult != nil {
		count += tr.optionalResult.ScanKeys()
	}
	count += tr.result.ScanKeys()
	return count
}
