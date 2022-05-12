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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/logutil/consistency"
	"github.com/pingcap/tidb/util/mathutil"
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

	// partitionTable indicates whether this task belongs to a partition table and which partition table it is.
	partitionTable table.PhysicalTable

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
	partitions      []table.PhysicalTable
	partRangeMap    map[int64][]*ranger.Range // each partition may have different ranges

	// kvRanges are only used for union scan.
	kvRanges         []kv.KeyRange
	dagPB            *tipb.DAGRequest
	startTS          uint64
	readReplicaScope string
	isStaleness      bool
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

	// If dummy flag is set, this is not a real IndexReader, it just provides the KV ranges for UnionScan.
	// Used by the temporary table, cached table.
	dummy bool
}

// Table implements the dataSourceExecutor interface.
func (e *IndexReaderExecutor) Table() table.Table {
	return e.table
}

func (e *IndexReaderExecutor) setDummy() {
	e.dummy = true
}

// Close clears all resources hold by current object.
func (e *IndexReaderExecutor) Close() (err error) {
	if e.result != nil {
		err = e.result.Close()
	}
	e.result = nil
	e.kvRanges = e.kvRanges[:0]
	if e.dummy {
		return nil
	}
	e.ctx.StoreQueryFeedback(e.feedback)
	return err
}

// Next implements the Executor Next interface.
func (e *IndexReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.dummy {
		req.Reset()
		return nil
	}

	err := e.result.Next(ctx, req)
	if err != nil {
		e.feedback.Invalidate()
	}
	return err
}

func (e *IndexReaderExecutor) buildKeyRanges(sc *stmtctx.StatementContext, ranges []*ranger.Range, physicalID int64) ([]kv.KeyRange, error) {
	if e.index.ID == -1 {
		return distsql.CommonHandleRangesToKVRanges(sc, []int64{physicalID}, ranges)
	}
	return distsql.IndexRangesToKVRanges(sc, physicalID, e.index.ID, ranges, e.feedback)
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

	sc := e.ctx.GetSessionVars().StmtCtx
	var kvRanges []kv.KeyRange
	if len(e.partitions) > 0 {
		for _, p := range e.partitions {
			partRange := e.ranges
			if pRange, ok := e.partRangeMap[p.GetPhysicalID()]; ok {
				partRange = pRange
			}
			kvRange, err := e.buildKeyRanges(sc, partRange, p.GetPhysicalID())
			if err != nil {
				return err
			}
			kvRanges = append(kvRanges, kvRange...)
		}
	} else {
		kvRanges, err = e.buildKeyRanges(sc, e.ranges, e.physicalTableID)
	}
	if err != nil {
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
	// Treat temporary table as dummy table, avoid sending distsql request to TiKV.
	// In a test case IndexReaderExecutor is mocked and e.table is nil.
	// Avoid sending distsql request to TIKV.
	if e.dummy {
		return nil
	}

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	var builder distsql.RequestBuilder
	builder.SetKeyRanges(kvRanges).
		SetDAGRequest(e.dagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetReadReplicaScope(e.readReplicaScope).
		SetIsStaleness(e.isStaleness).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetFromInfoSchema(e.ctx.GetInfoSchema()).
		SetMemTracker(e.memTracker)
	kvReq, err := builder.Build()
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	e.result, err = e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
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

	// fields about accessing partition tables
	partitionTableMode bool                  // if this executor is accessing a partition table
	prunedPartitions   []table.PhysicalTable // partition tables need to access
	partitionRangeMap  map[int64][]*ranger.Range
	partitionKVRanges  [][]kv.KeyRange // kvRanges of each prunedPartitions

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
	indexPaging    bool

	corColInIdxSide bool
	corColInTblSide bool
	corColInAccess  bool
	idxPlans        []plannercore.PhysicalPlan
	tblPlans        []plannercore.PhysicalPlan
	idxCols         []*expression.Column
	colLens         []int
	// PushedLimit is used to skip the preceding and tailing handles when Limit is sunk into IndexLookUpReader.
	PushedLimit *plannercore.PushedDownLimit

	stats *IndexLookUpRunTimeStats

	// cancelFunc is called when close the executor
	cancelFunc context.CancelFunc

	// If dummy flag is set, this is not a real IndexLookUpReader, it just provides the KV ranges for UnionScan.
	// Used by the temporary table, cached table.
	dummy bool
}

type getHandleType int8

const (
	getHandleFromIndex getHandleType = iota
	getHandleFromTable
)

// nolint:structcheck
type checkIndexValue struct {
	idxColTps  []*types.FieldType
	idxTblCols []*table.Column
}

// Table implements the dataSourceExecutor interface.
func (e *IndexLookUpExecutor) Table() table.Table {
	return e.table
}

func (e *IndexLookUpExecutor) setDummy() {
	e.dummy = true
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
	err = e.buildTableKeyRanges()
	if err != nil {
		e.feedback.Invalidate()
		return err
	}

	// Treat temporary table as dummy table, avoid sending distsql request to TiKV.
	if e.dummy {
		return nil
	}

	err = e.open(ctx)
	if err != nil {
		e.feedback.Invalidate()
	}
	return err
}

func (e *IndexLookUpExecutor) buildTableKeyRanges() (err error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	if e.partitionTableMode {
		if e.keepOrder { // this case should be prevented by the optimizer
			return errors.New("invalid execution plan: cannot keep order when accessing a partition table by IndexLookUpReader")
		}
		e.feedback.Invalidate() // feedback for partition tables is not ready
		e.partitionKVRanges = make([][]kv.KeyRange, 0, len(e.prunedPartitions))
		for _, p := range e.prunedPartitions {
			// TODO: prune and adjust e.ranges for each partition again, since not all e.ranges are suitable for all e.prunedPartitions.
			// For example, a table partitioned by range(a), and p0=(1, 10), p1=(11, 20), for the condition "(a>1 and a<10) or (a>11 and a<20)",
			// the first range is only suitable to p0 and the second is to p1, but now we'll also build kvRange for range0+p1 and range1+p0.
			physicalID := p.GetPhysicalID()
			ranges := e.ranges
			if e.partitionRangeMap != nil && e.partitionRangeMap[physicalID] != nil {
				ranges = e.partitionRangeMap[physicalID]
			}
			var kvRange []kv.KeyRange
			if e.index.ID == -1 {
				kvRange, err = distsql.CommonHandleRangesToKVRanges(sc, []int64{physicalID}, ranges)
			} else {
				kvRange, err = distsql.IndexRangesToKVRanges(sc, physicalID, e.index.ID, ranges, e.feedback)
			}
			if err != nil {
				return err
			}
			e.partitionKVRanges = append(e.partitionKVRanges, kvRange)
		}
	} else {
		physicalID := getPhysicalTableID(e.table)
		if e.index.ID == -1 {
			e.kvRanges, err = distsql.CommonHandleRangesToKVRanges(sc, []int64{physicalID}, e.ranges)
		} else {
			e.kvRanges, err = distsql.IndexRangesToKVRanges(sc, physicalID, e.index.ID, e.ranges, e.feedback)
		}
	}
	return err
}

func (e *IndexLookUpExecutor) open(ctx context.Context) error {
	// We have to initialize "memTracker" and other execution resources in here
	// instead of in function "Open", because this "IndexLookUpExecutor" may be
	// constructed by a "IndexLookUpJoin" and "Open" will not be called in that
	// situation.
	e.initRuntimeStats()
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
	ctx, cancel := context.WithCancel(ctx)
	e.cancelFunc = cancel
	workCh := make(chan *lookupTableTask, 1)
	if err := e.startIndexWorker(ctx, workCh, initBatchSize); err != nil {
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
	if e.index.Global {
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}
	if e.checkIndexValue != nil {
		tps = e.idxColTps
	}
	return tps
}

// startIndexWorker launch a background goroutine to fetch handles, send the results to workCh.
func (e *IndexLookUpExecutor) startIndexWorker(ctx context.Context, workCh chan<- *lookupTableTask, initBatchSize int) error {
	if e.runtimeStats != nil {
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}
	tracker := memory.NewTracker(memory.LabelForIndexWorker, -1)
	tracker.AttachTo(e.memTracker)

	kvRanges := [][]kv.KeyRange{e.kvRanges}
	if e.partitionTableMode {
		kvRanges = e.partitionKVRanges
	}
	tps := e.getRetTpsByHandle()
	idxID := e.getIndexPlanRootID()
	e.idxWorkerWg.Add(1)
	go func() {
		defer trace.StartRegion(ctx, "IndexLookUpIndexWorker").End()
		worker := &indexWorker{
			idxLookup:       e,
			workCh:          workCh,
			finished:        e.finished,
			resultCh:        e.resultCh,
			keepOrder:       e.keepOrder,
			checkIndexValue: e.checkIndexValue,
			maxBatchSize:    e.ctx.GetSessionVars().IndexLookupSize,
			maxChunkSize:    e.maxChunkSize,
			PushedLimit:     e.PushedLimit,
		}
		var builder distsql.RequestBuilder
		builder.SetDAGRequest(e.dagPB).
			SetStartTS(e.startTS).
			SetDesc(e.desc).
			SetKeepOrder(e.keepOrder).
			SetStreaming(e.indexStreaming).
			SetPaging(e.indexPaging).
			SetReadReplicaScope(e.readReplicaScope).
			SetIsStaleness(e.isStaleness).
			SetFromSessionVars(e.ctx.GetSessionVars()).
			SetFromInfoSchema(e.ctx.GetInfoSchema()).
			SetMemTracker(tracker)

		for partTblIdx, kvRange := range kvRanges {
			// check if executor is closed
			finished := false
			select {
			case <-e.finished:
				finished = true
			default:
			}
			if finished {
				break
			}
			if worker.PushedLimit != nil && worker.scannedKeys >= worker.PushedLimit.Count+worker.PushedLimit.Offset {
				break
			}

			// init kvReq, result and worker for this partition
			kvReq, err := builder.SetKeyRanges(kvRange).Build()
			if err != nil {
				worker.syncErr(err)
				break
			}
			result, err := distsql.SelectWithRuntimeStats(ctx, e.ctx, kvReq, tps, e.feedback, getPhysicalPlanIDs(e.idxPlans), idxID)
			if err != nil {
				worker.syncErr(err)
				break
			}
			worker.batchSize = initBatchSize
			if worker.batchSize > worker.maxBatchSize {
				worker.batchSize = worker.maxBatchSize
			}
			if e.partitionTableMode {
				worker.partitionTable = e.prunedPartitions[partTblIdx]
			}

			// fetch data from this partition
			ctx1, cancel := context.WithCancel(ctx)
			fetchErr := worker.fetchHandles(ctx1, result)
			if fetchErr != nil { // this error is synced in fetchHandles(), don't sync it again
				e.feedback.Invalidate()
			}
			cancel()
			if err := result.Close(); err != nil {
				logutil.Logger(ctx).Error("close Select result failed", zap.Error(err))
			}
			e.ctx.StoreQueryFeedback(e.feedback)
			if fetchErr != nil {
				break // if any error occurs, exit after releasing all resources
			}
		}
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

func (e *IndexLookUpExecutor) buildTableReader(ctx context.Context, task *lookupTableTask) (Executor, error) {
	table := e.table
	if e.partitionTableMode && task.partitionTable != nil {
		table = task.partitionTable
	}
	tableReaderExec := &TableReaderExecutor{
		baseExecutor:     newBaseExecutor(e.ctx, e.schema, e.getTableRootPlanID()),
		table:            table,
		dagPB:            e.tableRequest,
		startTS:          e.startTS,
		readReplicaScope: e.readReplicaScope,
		isStaleness:      e.isStaleness,
		columns:          e.columns,
		streaming:        e.tableStreaming,
		feedback:         statistics.NewQueryFeedback(0, nil, 0, false),
		corColInFilter:   e.corColInTblSide,
		plans:            e.tblPlans,
	}
	tableReaderExec.buildVirtualColumnInfo()
	tableReader, err := e.dataReaderBuilder.buildTableReaderFromHandles(ctx, tableReaderExec, task.handles, true)
	if err != nil {
		logutil.Logger(ctx).Error("build table reader from handles failed", zap.Error(err))
		return nil, err
	}
	return tableReader, nil
}

// Close implements Exec Close interface.
func (e *IndexLookUpExecutor) Close() error {
	e.kvRanges = e.kvRanges[:0]
	if e.dummy {
		return nil
	}

	if !e.workerStarted || e.finished == nil {
		return nil
	}

	if e.cancelFunc != nil {
		e.cancelFunc()
		e.cancelFunc = nil
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
	if e.dummy {
		req.Reset()
		return nil
	}

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
		if resultTask.cursor < len(resultTask.rows) {
			numToAppend := mathutil.Min(len(resultTask.rows)-resultTask.cursor, req.RequiredRows()-req.NumRows())
			req.AppendRows(resultTask.rows[resultTask.cursor : resultTask.cursor+numToAppend])
			resultTask.cursor += numToAppend
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

func (e *IndexLookUpExecutor) initRuntimeStats() {
	if e.runtimeStats != nil {
		if e.stats == nil {
			e.stats = &IndexLookUpRunTimeStats{
				indexScanBasicStats: &execdetails.BasicRuntimeStats{},
				Concurrency:         e.ctx.GetSessionVars().IndexLookupConcurrency(),
			}
			e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
		}
	}
}

func (e *IndexLookUpExecutor) getIndexPlanRootID() int {
	if len(e.idxPlans) > 0 {
		return e.idxPlans[len(e.idxPlans)-1].ID()
	}
	return e.id
}

func (e *IndexLookUpExecutor) getTableRootPlanID() int {
	if len(e.tblPlans) > 0 {
		return e.tblPlans[len(e.tblPlans)-1].ID()
	}
	return e.id
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
	// scannedKeys indicates how many keys be scanned
	scannedKeys uint64
	// partitionTable indicates if this worker is accessing a particular partition table.
	partitionTable table.PhysicalTable
}

func (w *indexWorker) syncErr(err error) {
	doneCh := make(chan error, 1)
	doneCh <- err
	w.resultCh <- &lookupTableTask{
		doneCh: doneCh,
	}
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are sent to workCh to be further processed by tableWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (w *indexWorker) fetchHandles(ctx context.Context, result distsql.SelectResult) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("indexWorker in IndexLookupExecutor panicked", zap.Any("recover", r), zap.Stack("stack"))
			err4Panic := errors.Errorf("%v", r)
			w.syncErr(err4Panic)
			if err != nil {
				err = errors.Trace(err4Panic)
			}
		}
	}()
	retTps := w.idxLookup.getRetTpsByHandle()
	chk := chunk.NewChunkWithCapacity(retTps, w.idxLookup.maxChunkSize)
	idxID := w.idxLookup.getIndexPlanRootID()
	if w.idxLookup.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		if idxID != w.idxLookup.id && w.idxLookup.stats != nil {
			w.idxLookup.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(idxID, w.idxLookup.stats.indexScanBasicStats)
		}
	}
	for {
		startTime := time.Now()
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, result)
		finishFetch := time.Now()
		if err != nil {
			w.syncErr(err)
			return err
		}
		if len(handles) == 0 {
			return nil
		}
		task := w.buildTableTask(handles, retChunk)
		finishBuild := time.Now()
		select {
		case <-ctx.Done():
			return nil
		case <-w.finished:
			return nil
		case w.workCh <- task:
			w.resultCh <- task
		}
		if w.idxLookup.stats != nil {
			atomic.AddInt64(&w.idxLookup.stats.FetchHandle, int64(finishFetch.Sub(startTime)))
			atomic.AddInt64(&w.idxLookup.stats.TaskWait, int64(time.Since(finishBuild)))
			atomic.AddInt64(&w.idxLookup.stats.FetchHandleTotal, int64(time.Since(startTime)))
		}
	}
}

func (w *indexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult) (
	handles []kv.Handle, retChk *chunk.Chunk, err error) {
	numColsWithoutPid := chk.NumCols()
	if w.idxLookup.index.Global {
		numColsWithoutPid = numColsWithoutPid - 1
	}
	handleOffset := make([]int, 0, len(w.idxLookup.handleCols))
	for i := range w.idxLookup.handleCols {
		handleOffset = append(handleOffset, numColsWithoutPid-len(w.idxLookup.handleCols)+i)
	}
	if len(handleOffset) == 0 {
		handleOffset = []int{numColsWithoutPid - 1}
	}
	handles = make([]kv.Handle, 0, w.batchSize)
	// PushedLimit would always be nil for CheckIndex or CheckTable, we add this check just for insurance.
	checkLimit := (w.PushedLimit != nil) && (w.checkIndexValue == nil)
	for len(handles) < w.batchSize {
		requiredRows := w.batchSize - len(handles)
		if checkLimit {
			if w.PushedLimit.Offset+w.PushedLimit.Count <= w.scannedKeys {
				return handles, nil, nil
			}
			leftCnt := w.PushedLimit.Offset + w.PushedLimit.Count - w.scannedKeys
			if uint64(requiredRows) > leftCnt {
				requiredRows = int(leftCnt)
			}
		}
		chk.SetRequiredRows(requiredRows, w.maxChunkSize)
		startTime := time.Now()
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return handles, nil, err
		}
		if w.idxLookup.stats != nil {
			w.idxLookup.stats.indexScanBasicStats.Record(time.Since(startTime), chk.NumRows())
		}
		if chk.NumRows() == 0 {
			return handles, retChk, nil
		}
		for i := 0; i < chk.NumRows(); i++ {
			w.scannedKeys++
			if checkLimit {
				if w.scannedKeys <= w.PushedLimit.Offset {
					continue
				}
				if w.scannedKeys > (w.PushedLimit.Offset + w.PushedLimit.Count) {
					// Skip the handles after Offset+Count.
					return handles, nil, nil
				}
			}
			h, err := w.idxLookup.getHandle(chk.GetRow(i), handleOffset, w.idxLookup.isCommonHandle(), getHandleFromIndex)
			if err != nil {
				return handles, retChk, err
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
	return handles, retChk, nil
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
		partitionTable:       w.partitionTable,
	}

	task.doneCh = make(chan error, 1)
	return task
}

// tableWorker is used by IndexLookUpExecutor to maintain table lookup background goroutines.
type tableWorker struct {
	idxLookup *IndexLookUpExecutor
	workCh    <-chan *lookupTableTask
	finished  <-chan struct{}
	keepOrder bool
	handleIdx []int

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
			logutil.Logger(ctx).Error("tableWorker in IndexLookUpExecutor panicked", zap.Any("recover", r), zap.Stack("stack"))
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
		startTime := time.Now()
		err := w.executeTask(ctx, task)
		if w.idxLookup.stats != nil {
			atomic.AddInt64(&w.idxLookup.stats.TableRowScan, int64(time.Since(startTime)))
			atomic.AddInt64(&w.idxLookup.stats.TableTaskNum, 1)
		}
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
			// the handle in the index is encoded as "sortKey". So we cannot restore its
			// original value(the primary key) here.
			// We use a trick to avoid encoding the "sortKey" again by changing the charset
			// collation to `binary`.
			rtp := e.handleCols[i].RetType
			if collate.NewCollationEnabled() && e.table.Meta().CommonHandleVersion == 0 && rtp.EvalType() == types.ETString &&
				!mysql.HasBinaryFlag(rtp.GetFlag()) && tp == getHandleFromIndex {
				rtp = rtp.Clone()
				rtp.SetCollate(charset.CollationBin)
				datums = append(datums, row.GetDatum(idx, rtp))
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
	if e.index.Global {
		pidOffset := row.Len() - 1
		pid := row.GetInt64(pidOffset)
		handle = kv.NewPartitionHandle(pid, handle)
	}
	return
}

// IndexLookUpRunTimeStats record the indexlookup runtime stat
type IndexLookUpRunTimeStats struct {
	// indexScanBasicStats uses to record basic runtime stats for index scan.
	indexScanBasicStats *execdetails.BasicRuntimeStats
	FetchHandleTotal    int64
	FetchHandle         int64
	TaskWait            int64
	TableRowScan        int64
	TableTaskNum        int64
	Concurrency         int
}

func (e *IndexLookUpRunTimeStats) String() string {
	var buf bytes.Buffer
	fetchHandle := atomic.LoadInt64(&e.FetchHandleTotal)
	indexScan := atomic.LoadInt64(&e.FetchHandle)
	taskWait := atomic.LoadInt64(&e.TaskWait)
	tableScan := atomic.LoadInt64(&e.TableRowScan)
	tableTaskNum := atomic.LoadInt64(&e.TableTaskNum)
	concurrency := e.Concurrency
	if indexScan != 0 {
		buf.WriteString(fmt.Sprintf("index_task: {total_time: %s, fetch_handle: %s, build: %s, wait: %s}",
			execdetails.FormatDuration(time.Duration(fetchHandle)),
			execdetails.FormatDuration(time.Duration(indexScan)),
			execdetails.FormatDuration(time.Duration(fetchHandle-indexScan-taskWait)),
			execdetails.FormatDuration(time.Duration(taskWait))))
	}
	if tableScan != 0 {
		if buf.Len() > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf(" table_task: {total_time: %v, num: %d, concurrency: %d}", execdetails.FormatDuration(time.Duration(tableScan)), tableTaskNum, concurrency))
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *IndexLookUpRunTimeStats) Clone() execdetails.RuntimeStats {
	newRs := *e
	return &newRs
}

// Merge implements the RuntimeStats interface.
func (e *IndexLookUpRunTimeStats) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*IndexLookUpRunTimeStats)
	if !ok {
		return
	}
	e.FetchHandleTotal += tmp.FetchHandleTotal
	e.FetchHandle += tmp.FetchHandle
	e.TaskWait += tmp.TaskWait
	e.TableRowScan += tmp.TableRowScan
	e.TableTaskNum += tmp.TableTaskNum
	e.Concurrency += tmp.Concurrency
}

// Tp implements the RuntimeStats interface.
func (e *IndexLookUpRunTimeStats) Tp() int {
	return execdetails.TpIndexLookUpRunTimeStats
}

func (w *tableWorker) compareData(ctx context.Context, task *lookupTableTask, tableReader Executor) error {
	chk := newFirstChunk(tableReader)
	tblInfo := w.idxLookup.table.Meta()
	vals := make([]types.Datum, 0, len(w.idxTblCols))

	// Prepare collator for compare.
	collators := make([]collate.Collator, 0, len(w.idxColTps))
	for _, tp := range w.idxColTps {
		collators = append(collators, collate.GetCollator(tp.GetCollate()))
	}

	ir := func() *consistency.Reporter {
		return &consistency.Reporter{
			HandleEncode: func(handle kv.Handle) kv.Key {
				return tablecodec.EncodeRecordKey(w.idxLookup.table.RecordPrefix(), handle)
			},
			IndexEncode: func(idxRow *consistency.RecordData) kv.Key {
				var idx table.Index
				for _, v := range w.idxLookup.table.Indices() {
					if strings.EqualFold(v.Meta().Name.String(), w.idxLookup.index.Name.O) {
						idx = v
						break
					}
				}
				if idx == nil {
					return nil
				}
				k, _, err := idx.GenIndexKey(w.idxLookup.ctx.GetSessionVars().StmtCtx, idxRow.Values[:len(idx.Meta().Columns)], idxRow.Handle, nil)
				if err != nil {
					return nil
				}
				return k
			},
			Tbl:  tblInfo,
			Idx:  w.idxLookup.index,
			Sctx: w.idxLookup.ctx,
		}
	}

	for {
		err := Next(ctx, tableReader, chk)
		if err != nil {
			return errors.Trace(err)
		}

		// If ctx is cancelled, `Next` may return empty result when the actual data is not empty. To avoid producing
		// false-positive error logs that cause confusion, exit in this case.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if chk.NumRows() == 0 {
			task.indexOrder.Range(func(h kv.Handle, val interface{}) bool {
				idxRow := task.idxRows.GetRow(val.(int))
				err = ir().ReportAdminCheckInconsistent(ctx, h, &consistency.RecordData{Handle: h, Values: getDatumRow(&idxRow, w.idxColTps)}, nil)
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
			sctx := w.idxLookup.ctx.GetSessionVars().StmtCtx
			for i := range vals {
				col := w.idxTblCols[i]
				tp := &col.FieldType
				idxVal := idxRow.GetDatum(i, tp)
				tablecodec.TruncateIndexValue(&idxVal, w.idxLookup.index.Columns[i], col.ColumnInfo)
				cmpRes, err := idxVal.Compare(sctx, &vals[i], collators[i])
				if err != nil {
					fts := make([]*types.FieldType, 0, len(w.idxTblCols))
					for _, c := range w.idxTblCols {
						fts = append(fts, &c.FieldType)
					}
					return ir().ReportAdminCheckInconsistentWithColInfo(ctx,
						handle,
						col.Name.O,
						idxRow.GetDatum(i, tp),
						vals[i],
						err,
						&consistency.RecordData{Handle: handle, Values: getDatumRow(&idxRow, fts)},
					)
				}
				if cmpRes != 0 {
					fts := make([]*types.FieldType, 0, len(w.idxTblCols))
					for _, c := range w.idxTblCols {
						fts = append(fts, &c.FieldType)
					}
					return ir().ReportAdminCheckInconsistentWithColInfo(ctx,
						handle,
						col.Name.O,
						idxRow.GetDatum(i, tp),
						vals[i],
						err,
						&consistency.RecordData{Handle: handle, Values: getDatumRow(&idxRow, fts)},
					)
				}
			}
		}
	}
	return nil
}

func getDatumRow(r *chunk.Row, fields []*types.FieldType) []types.Datum {
	datumRow := make([]types.Datum, 0, r.Chunk().NumCols())
	for colIdx := 0; colIdx < r.Chunk().NumCols(); colIdx++ {
		if colIdx >= len(fields) {
			break
		}
		datum := r.GetDatum(colIdx, fields[colIdx])
		datumRow = append(datumRow, datum)
	}
	return datumRow
}

// executeTask executes the table look up tasks. We will construct a table reader and send request by handles.
// Then we hold the returning rows and finish this task.
func (w *tableWorker) executeTask(ctx context.Context, task *lookupTableTask) error {
	tableReader, err := w.idxLookup.buildTableReader(ctx, task)
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

	if handleCnt != len(task.rows) && !util.HasCancelled(ctx) &&
		!w.idxLookup.ctx.GetSessionVars().StmtCtx.WeakConsistency {
		if len(w.idxLookup.tblPlans) == 1 {
			obtainedHandlesMap := kv.NewHandleMap()
			for _, row := range task.rows {
				handle, err := w.idxLookup.getHandle(row, w.handleIdx, w.idxLookup.isCommonHandle(), getHandleFromTable)
				if err != nil {
					return err
				}
				obtainedHandlesMap.Set(handle, true)
			}
			missHds := GetLackHandles(task.handles, obtainedHandlesMap)
			return (&consistency.Reporter{
				HandleEncode: func(hd kv.Handle) kv.Key {
					return tablecodec.EncodeRecordKey(w.idxLookup.table.RecordPrefix(), hd)
				},
				Tbl:  w.idxLookup.table.Meta(),
				Idx:  w.idxLookup.index,
				Sctx: w.idxLookup.ctx,
			}).ReportLookupInconsistent(ctx,
				handleCnt,
				len(task.rows),
				missHds,
				task.handles,
				nil,
				//missRecords,
			)
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
