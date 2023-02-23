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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bytes"
	"context"
	"fmt"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var (
	_ Executor = &IndexMergeReaderExecutor{}
)

const (
	partialIndexWorkerType        = "IndexMergePartialIndexWorker"
	partialTableWorkerType        = "IndexMergePartialTableWorker"
	processWorkerType             = "IndexMergeProcessWorker"
	partTblIntersectionWorkerType = "IndexMergePartTblIntersectionWorker"
	tableScanWorkerType           = "IndexMergeTableScanWorker"
)

// IndexMergeReaderExecutor accesses a table with multiple index/table scan.
// There are three types of workers:
// 1. partialTableWorker/partialIndexWorker, which are used to fetch the handles
// 2. indexMergeProcessWorker, which is used to do the `Union` operation.
// 3. indexMergeTableScanWorker, which is used to get the table tuples with the given handles.
//
// The execution flow is really like IndexLookUpReader. However, it uses multiple index scans
// or table scans to get the handles:
//  1. use the partialTableWorkers and partialIndexWorkers to fetch the handles (a batch per time)
//     and send them to the indexMergeProcessWorker.
//  2. indexMergeProcessWorker do the `Union` operation for a batch of handles it have got.
//     For every handle in the batch:
//  1. check whether it has been accessed.
//  2. if not, record it and send it to the indexMergeTableScanWorker.
//  3. if accessed, just ignore it.
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
	columns []*model.ColumnInfo
	*dataReaderBuilder

	// fields about accessing partition tables
	partitionTableMode bool                  // if this IndexMerge is accessing a partition table
	prunedPartitions   []table.PhysicalTable // pruned partition tables need to access
	partitionKeyRanges [][][]kv.KeyRange     // [partitionIdx][partialIndex][ranges]

	// All fields above are immutable.

	tblWorkerWg     sync.WaitGroup
	idxWorkerWg     sync.WaitGroup
	processWorkerWg sync.WaitGroup
	finished        chan struct{}

	workerStarted bool
	keyRanges     [][]kv.KeyRange

	resultCh   chan *indexMergeTableTask
	resultCurr *indexMergeTableTask
	feedbacks  []*statistics.QueryFeedback

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker
	paging     bool

	// checkIndexValue is used to check the consistency of the index data.
	*checkIndexValue // nolint:unused

	partialPlans        [][]plannercore.PhysicalPlan
	tblPlans            []plannercore.PhysicalPlan
	partialNetDataSizes []float64
	dataAvgRowSize      float64

	handleCols plannercore.HandleCols
	stats      *IndexMergeRuntimeStat

	// Indicates whether there is correlated column in filter or table/index range.
	// We need to refresh dagPBs before send DAGReq to storage.
	isCorColInPartialFilters []bool
	isCorColInTableFilter    bool
	isCorColInPartialAccess  []bool

	// Whether it's intersection or union.
	isIntersection bool
}

type indexMergeTableTask struct {
	lookupTableTask

	// parTblIdx are only used in indexMergeProcessWorker.fetchLoopIntersection.
	parTblIdx int
}

// Table implements the dataSourceExecutor interface.
func (e *IndexMergeReaderExecutor) Table() table.Table {
	return e.table
}

// Open implements the Executor Open interface
func (e *IndexMergeReaderExecutor) Open(ctx context.Context) (err error) {
	e.keyRanges = make([][]kv.KeyRange, 0, len(e.partialPlans))
	e.initRuntimeStats()
	if e.isCorColInTableFilter {
		e.tableRequest.Executors, err = constructDistExec(e.ctx, e.tblPlans)
		if err != nil {
			return err
		}
	}
	if err = e.rebuildRangeForCorCol(); err != nil {
		return err
	}

	if !e.partitionTableMode {
		if e.keyRanges, err = e.buildKeyRangesForTable(e.table); err != nil {
			return err
		}
	} else {
		for _, feedback := range e.feedbacks {
			feedback.Invalidate() // feedback is not ready for partition tables
		}
		e.partitionKeyRanges = make([][][]kv.KeyRange, len(e.prunedPartitions))
		for i, p := range e.prunedPartitions {
			if e.partitionKeyRanges[i], err = e.buildKeyRangesForTable(p); err != nil {
				return err
			}
		}
	}
	e.finished = make(chan struct{})
	e.resultCh = make(chan *indexMergeTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))
	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.id, -1)
	}
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
	return nil
}

func (e *IndexMergeReaderExecutor) rebuildRangeForCorCol() (err error) {
	len1 := len(e.partialPlans)
	len2 := len(e.isCorColInPartialAccess)
	if len1 != len2 {
		return errors.Errorf("unexpect length for partialPlans(%d) and isCorColInPartialAccess(%d)", len1, len2)
	}
	for i, plan := range e.partialPlans {
		if e.isCorColInPartialAccess[i] {
			switch x := plan[0].(type) {
			case *plannercore.PhysicalIndexScan:
				e.ranges[i], err = rebuildIndexRanges(e.ctx, x, x.IdxCols, x.IdxColLens)
			case *plannercore.PhysicalTableScan:
				e.ranges[i], err = x.ResolveCorrelatedColumns()
			default:
				err = errors.Errorf("unsupported plan type %T", plan[0])
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *IndexMergeReaderExecutor) buildKeyRangesForTable(tbl table.Table) (ranges [][]kv.KeyRange, err error) {
	sc := e.ctx.GetSessionVars().StmtCtx
	for i, plan := range e.partialPlans {
		_, ok := plan[0].(*plannercore.PhysicalIndexScan)
		if !ok {
			firstPartRanges, secondPartRanges := distsql.SplitRangesAcrossInt64Boundary(e.ranges[i], false, e.descs[i], tbl.Meta().IsCommonHandle)
			firstKeyRanges, err := distsql.TableHandleRangesToKVRanges(sc, []int64{getPhysicalTableID(tbl)}, tbl.Meta().IsCommonHandle, firstPartRanges, nil)
			if err != nil {
				return nil, err
			}
			secondKeyRanges, err := distsql.TableHandleRangesToKVRanges(sc, []int64{getPhysicalTableID(tbl)}, tbl.Meta().IsCommonHandle, secondPartRanges, nil)
			if err != nil {
				return nil, err
			}
			keyRanges := append(firstKeyRanges.FirstPartitionRange(), secondKeyRanges.FirstPartitionRange()...)
			ranges = append(ranges, keyRanges)
			continue
		}
		keyRange, err := distsql.IndexRangesToKVRanges(sc, getPhysicalTableID(tbl), e.indexes[i].ID, e.ranges[i], e.feedbacks[i])
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, keyRange.FirstPartitionRange())
	}
	return ranges, nil
}

func (e *IndexMergeReaderExecutor) startWorkers(ctx context.Context) error {
	exitCh := make(chan struct{})
	workCh := make(chan *indexMergeTableTask, 1)
	fetchCh := make(chan *indexMergeTableTask, len(e.keyRanges))

	e.startIndexMergeProcessWorker(ctx, workCh, fetchCh)

	var err error
	for i := 0; i < len(e.partialPlans); i++ {
		e.idxWorkerWg.Add(1)
		if e.indexes[i] != nil {
			err = e.startPartialIndexWorker(ctx, exitCh, fetchCh, i)
		} else {
			err = e.startPartialTableWorker(ctx, exitCh, fetchCh, i)
		}
		if err != nil {
			e.idxWorkerWg.Done()
			break
		}
	}
	go e.waitPartialWorkersAndCloseFetchChan(fetchCh)
	if err != nil {
		close(exitCh)
		return err
	}
	e.startIndexMergeTableScanWorker(ctx, workCh)
	e.workerStarted = true
	return nil
}

func (e *IndexMergeReaderExecutor) waitPartialWorkersAndCloseFetchChan(fetchCh chan *indexMergeTableTask) {
	e.idxWorkerWg.Wait()
	close(fetchCh)
}

func (e *IndexMergeReaderExecutor) startIndexMergeProcessWorker(ctx context.Context, workCh chan<- *indexMergeTableTask, fetch <-chan *indexMergeTableTask) {
	idxMergeProcessWorker := &indexMergeProcessWorker{
		indexMerge: e,
		stats:      e.stats,
	}
	e.processWorkerWg.Add(1)
	go func() {
		defer trace.StartRegion(ctx, "IndexMergeProcessWorker").End()
		util.WithRecovery(
			func() {
				if e.isIntersection {
					idxMergeProcessWorker.fetchLoopIntersection(ctx, fetch, workCh, e.resultCh, e.finished)
				} else {
					idxMergeProcessWorker.fetchLoopUnion(ctx, fetch, workCh, e.resultCh, e.finished)
				}
			},
			handleWorkerPanic(ctx, e.finished, e.resultCh, nil, processWorkerType),
		)
		e.processWorkerWg.Done()
	}()
}

func (e *IndexMergeReaderExecutor) startPartialIndexWorker(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *indexMergeTableTask, workID int) error {
	failpoint.Inject("testIndexMergeResultChCloseEarly", func(_ failpoint.Value) {
		// Wait for processWorker to close resultCh.
		time.Sleep(time.Second * 2)
		// Should use fetchCh instead of resultCh to send error.
		syncErr(ctx, e.finished, fetchCh, errors.New("testIndexMergeResultChCloseEarly"))
	})
	if e.runtimeStats != nil {
		collExec := true
		e.dagPBs[workID].CollectExecutionSummaries = &collExec
	}

	var keyRanges [][]kv.KeyRange
	if e.partitionTableMode {
		for _, pKeyRanges := range e.partitionKeyRanges { // get all keyRanges related to this PartialIndex
			keyRanges = append(keyRanges, pKeyRanges[workID])
		}
	} else {
		keyRanges = [][]kv.KeyRange{e.keyRanges[workID]}
	}

	failpoint.Inject("startPartialIndexWorkerErr", func() error {
		return errors.New("inject an error before start partialIndexWorker")
	})

	go func() {
		defer trace.StartRegion(ctx, "IndexMergePartialIndexWorker").End()
		defer e.idxWorkerWg.Done()
		util.WithRecovery(
			func() {
				failpoint.Inject("testIndexMergePanicPartialIndexWorker", nil)
				failpoint.Inject("mockSleepBeforeStartTableReader", func(_ failpoint.Value) {
					select {
					case <-ctx.Done():
						failpoint.Return()
					case <-e.finished:
						failpoint.Return()
					case <-exitCh:
						failpoint.Return()
					}
				})
				worker := &partialIndexWorker{
					stats:        e.stats,
					idxID:        e.getPartitalPlanID(workID),
					sc:           e.ctx,
					batchSize:    e.maxChunkSize,
					maxBatchSize: e.ctx.GetSessionVars().IndexLookupSize,
					maxChunkSize: e.maxChunkSize,
					memTracker:   e.memTracker,
				}

				if e.isCorColInPartialFilters[workID] {
					// We got correlated column, so need to refresh Selection operator.
					var err error
					if e.dagPBs[workID].Executors, err = constructDistExec(e.ctx, e.partialPlans[workID]); err != nil {
						syncErr(ctx, e.finished, fetchCh, err)
						return
					}
				}

				var builder distsql.RequestBuilder
				builder.SetDAGRequest(e.dagPBs[workID]).
					SetStartTS(e.startTS).
					SetDesc(e.descs[workID]).
					SetKeepOrder(false).
					SetTxnScope(e.txnScope).
					SetReadReplicaScope(e.readReplicaScope).
					SetIsStaleness(e.isStaleness).
					SetFromSessionVars(e.ctx.GetSessionVars()).
					SetMemTracker(e.memTracker).
					SetPaging(e.paging).
					SetFromInfoSchema(e.ctx.GetInfoSchema()).
					SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.ctx, &builder.Request, e.partialNetDataSizes[workID]))

				var notClosedSelectResult distsql.SelectResult
				defer func() {
					// To make sure SelectResult.Close() is called even got panic in fetchHandles().
					if notClosedSelectResult != nil {
						terror.Call(notClosedSelectResult.Close)
					}
				}()
				for parTblIdx, keyRange := range keyRanges {
					// check if this executor is closed
					select {
					case <-ctx.Done():
						return
					case <-e.finished:
						return
					default:
					}

					// init kvReq and worker for this partition
					// The key ranges should be ordered.
					slices.SortFunc(keyRange, func(i, j kv.KeyRange) bool {
						return bytes.Compare(i.StartKey, j.StartKey) < 0
					})
					kvReq, err := builder.SetKeyRanges(keyRange).Build()
					if err != nil {
						syncErr(ctx, e.finished, fetchCh, err)
						return
					}
					result, err := distsql.SelectWithRuntimeStats(ctx, e.ctx, kvReq, e.handleCols.GetFieldsTypes(), e.feedbacks[workID], getPhysicalPlanIDs(e.partialPlans[workID]), e.getPartitalPlanID(workID))
					if err != nil {
						syncErr(ctx, e.finished, fetchCh, err)
						return
					}
					notClosedSelectResult = result
					failpoint.Inject("testIndexMergePartialIndexWorkerCoprLeak", nil)
					worker.batchSize = e.maxChunkSize
					if worker.batchSize > worker.maxBatchSize {
						worker.batchSize = worker.maxBatchSize
					}
					if e.partitionTableMode {
						worker.partition = e.prunedPartitions[parTblIdx]
					}

					// fetch all data from this partition
					ctx1, cancel := context.WithCancel(ctx)
					_, fetchErr := worker.fetchHandles(ctx1, result, exitCh, fetchCh, e.finished, e.handleCols, parTblIdx)
					if fetchErr != nil { // this error is synced in fetchHandles(), don't sync it again
						e.feedbacks[workID].Invalidate()
					}
					notClosedSelectResult = nil
					if err := result.Close(); err != nil {
						logutil.Logger(ctx).Error("close Select result failed:", zap.Error(err))
					}
					cancel()
					e.ctx.StoreQueryFeedback(e.feedbacks[workID])
					if fetchErr != nil {
						break
					}
				}
			},
			handleWorkerPanic(ctx, e.finished, fetchCh, nil, partialIndexWorkerType),
		)
	}()

	return nil
}

func (e *IndexMergeReaderExecutor) startPartialTableWorker(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *indexMergeTableTask, workID int) error {
	ts := e.partialPlans[workID][0].(*plannercore.PhysicalTableScan)

	tbls := make([]table.Table, 0, 1)
	if e.partitionTableMode {
		for _, p := range e.prunedPartitions {
			tbls = append(tbls, p)
		}
	} else {
		tbls = append(tbls, e.table)
	}

	go func() {
		defer trace.StartRegion(ctx, "IndexMergePartialTableWorker").End()
		defer e.idxWorkerWg.Done()
		util.WithRecovery(
			func() {
				failpoint.Inject("testIndexMergePanicPartialTableWorker", nil)
				failpoint.Inject("mockSleepBeforeStartTableReader", func(_ failpoint.Value) {
					select {
					case <-ctx.Done():
						failpoint.Return()
					case <-e.finished:
						failpoint.Return()
					case <-exitCh:
						failpoint.Return()
					}
				})
				var err error
				partialTableReader := &TableReaderExecutor{
					baseExecutor:     newBaseExecutor(e.ctx, ts.Schema(), e.getPartitalPlanID(workID)),
					dagPB:            e.dagPBs[workID],
					startTS:          e.startTS,
					txnScope:         e.txnScope,
					readReplicaScope: e.readReplicaScope,
					isStaleness:      e.isStaleness,
					feedback:         statistics.NewQueryFeedback(0, nil, 0, false),
					plans:            e.partialPlans[workID],
					ranges:           e.ranges[workID],
					netDataSize:      e.partialNetDataSizes[workID],
				}

				worker := &partialTableWorker{
					stats:        e.stats,
					sc:           e.ctx,
					batchSize:    e.maxChunkSize,
					maxBatchSize: e.ctx.GetSessionVars().IndexLookupSize,
					maxChunkSize: e.maxChunkSize,
					tableReader:  partialTableReader,
					memTracker:   e.memTracker,
				}

				if e.isCorColInPartialFilters[workID] {
					if e.dagPBs[workID].Executors, err = constructDistExec(e.ctx, e.partialPlans[workID]); err != nil {
						syncErr(ctx, e.finished, fetchCh, err)
						return
					}
					partialTableReader.dagPB = e.dagPBs[workID]
				}

				var tableReaderClosed bool
				defer func() {
					// To make sure SelectResult.Close() is called even got panic in fetchHandles().
					if !tableReaderClosed {
						terror.Call(worker.tableReader.Close)
					}
				}()
				for parTblIdx, tbl := range tbls {
					// check if this executor is closed
					select {
					case <-ctx.Done():
						return
					case <-e.finished:
						return
					default:
					}

					// init partialTableReader and partialTableWorker again for the next table
					partialTableReader.table = tbl
					if err = partialTableReader.Open(ctx); err != nil {
						logutil.Logger(ctx).Error("open Select result failed:", zap.Error(err))
						syncErr(ctx, e.finished, fetchCh, err)
						break
					}
					failpoint.Inject("testIndexMergePartialTableWorkerCoprLeak", nil)
					tableReaderClosed = false
					worker.batchSize = e.maxChunkSize
					if worker.batchSize > worker.maxBatchSize {
						worker.batchSize = worker.maxBatchSize
					}
					if e.partitionTableMode {
						worker.partition = tbl.(table.PhysicalTable)
					}

					// fetch all handles from this table
					ctx1, cancel := context.WithCancel(ctx)
					_, fetchErr := worker.fetchHandles(ctx1, exitCh, fetchCh, e.finished, e.handleCols, parTblIdx)
					if fetchErr != nil { // this error is synced in fetchHandles, so don't sync it again
						e.feedbacks[workID].Invalidate()
					}

					// release related resources
					cancel()
					tableReaderClosed = true
					if err = worker.tableReader.Close(); err != nil {
						logutil.Logger(ctx).Error("close Select result failed:", zap.Error(err))
					}
					e.ctx.StoreQueryFeedback(e.feedbacks[workID])
					if fetchErr != nil {
						break
					}
				}
			},
			handleWorkerPanic(ctx, e.finished, fetchCh, nil, partialTableWorkerType),
		)
	}()
	return nil
}

func (e *IndexMergeReaderExecutor) initRuntimeStats() {
	if e.runtimeStats != nil {
		e.stats = &IndexMergeRuntimeStat{
			Concurrency: e.ctx.GetSessionVars().IndexLookupConcurrency(),
		}
	}
}

func (e *IndexMergeReaderExecutor) getPartitalPlanID(workID int) int {
	if len(e.partialPlans[workID]) > 0 {
		return e.partialPlans[workID][len(e.partialPlans[workID])-1].ID()
	}
	return 0
}

func (e *IndexMergeReaderExecutor) getTablePlanRootID() int {
	if len(e.tblPlans) > 0 {
		return e.tblPlans[len(e.tblPlans)-1].ID()
	}
	return e.id
}

type partialTableWorker struct {
	stats        *IndexMergeRuntimeStat
	sc           sessionctx.Context
	batchSize    int
	maxBatchSize int
	maxChunkSize int
	tableReader  Executor
	partition    table.PhysicalTable // it indicates if this worker is accessing a particular partition table
	memTracker   *memory.Tracker
}

func (w *partialTableWorker) fetchHandles(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *indexMergeTableTask,
	finished <-chan struct{}, handleCols plannercore.HandleCols, parTblIdx int) (count int64, err error) {
	chk := w.sc.GetSessionVars().GetNewChunkWithCapacity(retTypes(w.tableReader), w.maxChunkSize, w.maxChunkSize, w.tableReader.base().AllocPool)
	var basic *execdetails.BasicRuntimeStats
	if be := w.tableReader.base(); be != nil && be.runtimeStats != nil {
		basic = be.runtimeStats
	}
	for {
		start := time.Now()
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, handleCols)
		if err != nil {
			syncErr(ctx, finished, fetchCh, err)
			return count, err
		}
		if len(handles) == 0 {
			return count, nil
		}
		count += int64(len(handles))
		task := w.buildTableTask(handles, retChunk, parTblIdx)
		if w.stats != nil {
			atomic.AddInt64(&w.stats.FetchIdxTime, int64(time.Since(start)))
		}
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-exitCh:
			return count, nil
		case <-finished:
			return count, nil
		case fetchCh <- task:
		}
		if basic != nil {
			basic.Record(time.Since(start), chk.NumRows())
		}
	}
}

func (w *partialTableWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, handleCols plannercore.HandleCols) (
	handles []kv.Handle, retChk *chunk.Chunk, err error) {
	handles = make([]kv.Handle, 0, w.batchSize)
	var memUsage int64
	defer w.memTracker.Consume(-memUsage)
	for len(handles) < w.batchSize {
		chk.SetRequiredRows(w.batchSize-len(handles), w.maxChunkSize)
		err = errors.Trace(w.tableReader.Next(ctx, chk))
		if err != nil {
			return handles, nil, err
		}
		if chk.NumRows() == 0 {
			failpoint.Inject("testIndexMergeErrorPartialTableWorker", func(v failpoint.Value) {
				failpoint.Return(handles, nil, errors.New(v.(string)))
			})
			return handles, retChk, nil
		}
		memDelta := chk.MemoryUsage()
		memUsage += memDelta
		w.memTracker.Consume(memDelta)
		for i := 0; i < chk.NumRows(); i++ {
			handle, err := handleCols.BuildHandle(chk.GetRow(i))
			if err != nil {
				return nil, nil, err
			}
			handles = append(handles, handle)
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (w *partialTableWorker) buildTableTask(handles []kv.Handle, retChk *chunk.Chunk, parTblIdx int) *indexMergeTableTask {
	task := &indexMergeTableTask{
		lookupTableTask: lookupTableTask{
			handles: handles,
			idxRows: retChk,

			partitionTable: w.partition,
		},
		parTblIdx: parTblIdx,
	}

	task.doneCh = make(chan error, 1)
	return task
}

func (e *IndexMergeReaderExecutor) startIndexMergeTableScanWorker(ctx context.Context, workCh <-chan *indexMergeTableTask) {
	lookupConcurrencyLimit := e.ctx.GetSessionVars().IndexLookupConcurrency()
	e.tblWorkerWg.Add(lookupConcurrencyLimit)
	for i := 0; i < lookupConcurrencyLimit; i++ {
		worker := &indexMergeTableScanWorker{
			stats:          e.stats,
			workCh:         workCh,
			finished:       e.finished,
			indexMergeExec: e,
			tblPlans:       e.tblPlans,
			memTracker:     e.memTracker,
		}
		ctx1, cancel := context.WithCancel(ctx)
		go func() {
			defer trace.StartRegion(ctx, "IndexMergeTableScanWorker").End()
			var task *indexMergeTableTask
			util.WithRecovery(
				// Note we use the address of `task` as the argument of both `pickAndExecTask` and `handleTableScanWorkerPanic`
				// because `task` is expected to be assigned in `pickAndExecTask`, and this assignment should also be visible
				// in `handleTableScanWorkerPanic` since it will get `doneCh` from `task`. Golang always pass argument by value,
				// so if we don't use the address of `task` as the argument, the assignment to `task` in `pickAndExecTask` is
				// not visible in `handleTableScanWorkerPanic`
				func() { worker.pickAndExecTask(ctx1, &task) },
				worker.handleTableScanWorkerPanic(ctx1, e.finished, &task, tableScanWorkerType),
			)
			cancel()
			e.tblWorkerWg.Done()
		}()
	}
}

func (e *IndexMergeReaderExecutor) buildFinalTableReader(ctx context.Context, tbl table.Table, handles []kv.Handle) (_ Executor, err error) {
	tableReaderExec := &TableReaderExecutor{
		baseExecutor:     newBaseExecutor(e.ctx, e.schema, e.getTablePlanRootID()),
		table:            tbl,
		dagPB:            e.tableRequest,
		startTS:          e.startTS,
		txnScope:         e.txnScope,
		readReplicaScope: e.readReplicaScope,
		isStaleness:      e.isStaleness,
		columns:          e.columns,
		feedback:         statistics.NewQueryFeedback(0, nil, 0, false),
		plans:            e.tblPlans,
		netDataSize:      e.dataAvgRowSize * float64(len(handles)),
	}
	tableReaderExec.buildVirtualColumnInfo()
	// Reorder handles because SplitKeyRangesByLocations() requires startKey of kvRanges is ordered.
	// Also it's good for performance.
	tableReader, err := e.dataReaderBuilder.buildTableReaderFromHandles(ctx, tableReaderExec, handles, true)
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
		if resultTask.cursor < len(resultTask.rows) {
			numToAppend := mathutil.Min(len(resultTask.rows)-resultTask.cursor, e.maxChunkSize-req.NumRows())
			req.AppendRows(resultTask.rows[resultTask.cursor : resultTask.cursor+numToAppend])
			resultTask.cursor += numToAppend
			if req.NumRows() >= e.maxChunkSize {
				return nil
			}
		}
	}
}

func (e *IndexMergeReaderExecutor) getResultTask() (*indexMergeTableTask, error) {
	failpoint.Inject("testIndexMergeMainReturnEarly", func(_ failpoint.Value) {
		// To make sure processWorker make resultCh to be full.
		// When main goroutine close finished, processWorker may be stuck when writing resultCh.
		time.Sleep(time.Second * 20)
		failpoint.Return(nil, errors.New("failpoint testIndexMergeMainReturnEarly"))
	})
	if e.resultCurr != nil && e.resultCurr.cursor < len(e.resultCurr.rows) {
		return e.resultCurr, nil
	}
	task, ok := <-e.resultCh
	if !ok {
		return nil, nil
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

func handleWorkerPanic(ctx context.Context, finished <-chan struct{}, ch chan<- *indexMergeTableTask, extraNotifyCh chan bool, worker string) func(r interface{}) {
	return func(r interface{}) {
		if worker == processWorkerType {
			// There is only one processWorker, so it's safe to close here.
			// No need to worry about "close on closed channel" error.
			defer close(ch)
		}
		if r == nil {
			logutil.BgLogger().Info("worker finish without panic", zap.Any("worker", worker))
			return
		}

		if extraNotifyCh != nil {
			extraNotifyCh <- true
		}

		err4Panic := errors.Errorf("%s: %v", worker, r)
		logutil.Logger(ctx).Error(err4Panic.Error())
		doneCh := make(chan error, 1)
		doneCh <- err4Panic
		task := &indexMergeTableTask{
			lookupTableTask: lookupTableTask{
				doneCh: doneCh,
			},
		}
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case ch <- task:
			return
		}
	}
}

// Close implements Exec Close interface.
func (e *IndexMergeReaderExecutor) Close() error {
	if e.stats != nil {
		defer e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.id, e.stats)
	}
	if e.finished == nil {
		return nil
	}
	close(e.finished)
	e.tblWorkerWg.Wait()
	e.idxWorkerWg.Wait()
	e.processWorkerWg.Wait()
	e.finished = nil
	e.workerStarted = false
	// TODO: how to store e.feedbacks
	return nil
}

type indexMergeProcessWorker struct {
	indexMerge *IndexMergeReaderExecutor
	stats      *IndexMergeRuntimeStat
}

func (w *indexMergeProcessWorker) fetchLoopUnion(ctx context.Context, fetchCh <-chan *indexMergeTableTask,
	workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	failpoint.Inject("testIndexMergeResultChCloseEarly", func(_ failpoint.Value) {
		failpoint.Return()
	})
	memTracker := memory.NewTracker(w.indexMerge.id, -1)
	memTracker.AttachTo(w.indexMerge.memTracker)
	defer memTracker.Detach()
	defer close(workCh)
	failpoint.Inject("testIndexMergePanicProcessWorkerUnion", nil)

	distinctHandles := make(map[int64]*kv.HandleMap)
	for {
		var ok bool
		var task *indexMergeTableTask
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case task, ok = <-fetchCh:
			if !ok {
				return
			}
		}

		select {
		case err := <-task.doneCh:
			// If got error from partialIndexWorker/partialTableWorker, stop processing.
			if err != nil {
				syncErr(ctx, finished, resultCh, err)
				return
			}
		default:
		}
		start := time.Now()
		handles := task.handles
		fhs := make([]kv.Handle, 0, 8)

		memTracker.Consume(int64(cap(task.handles) * 8))

		var tblID int64
		if w.indexMerge.partitionTableMode {
			tblID = getPhysicalTableID(task.partitionTable)
		} else {
			tblID = getPhysicalTableID(w.indexMerge.table)
		}
		if _, ok := distinctHandles[tblID]; !ok {
			distinctHandles[tblID] = kv.NewHandleMap()
		}
		hMap := distinctHandles[tblID]

		for _, h := range handles {
			if _, ok := hMap.Get(h); !ok {
				fhs = append(fhs, h)
				hMap.Set(h, true)
			}
		}
		if len(fhs) == 0 {
			continue
		}
		task = &indexMergeTableTask{
			lookupTableTask: lookupTableTask{
				handles: fhs,
				doneCh:  make(chan error, 1),

				partitionTable: task.partitionTable,
			},
		}
		if w.stats != nil {
			w.stats.IndexMergeProcess += time.Since(start)
		}
		failpoint.Inject("testIndexMergeProcessWorkerUnionHang", func(_ failpoint.Value) {
			for i := 0; i < cap(resultCh); i++ {
				select {
				case resultCh <- &indexMergeTableTask{}:
				default:
				}
			}
		})
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case workCh <- task:
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case resultCh <- task:
			}
		}
	}
}

type intersectionProcessWorker struct {
	// key: parTblIdx, val: HandleMap
	// Value of MemAwareHandleMap is *int to avoid extra Get().
	handleMapsPerWorker map[int]*kv.MemAwareHandleMap[*int]
	workerID            int
	workerCh            chan *indexMergeTableTask
	indexMerge          *IndexMergeReaderExecutor
	memTracker          *memory.Tracker
	batchSize           int

	// When rowDelta == memConsumeBatchSize, Consume(memUsage)
	rowDelta      int64
	mapUsageDelta int64
}

func (w *intersectionProcessWorker) consumeMemDelta() {
	w.memTracker.Consume(w.mapUsageDelta + w.rowDelta*int64(unsafe.Sizeof(int(0))))
	w.mapUsageDelta = 0
	w.rowDelta = 0
}

func (w *intersectionProcessWorker) doIntersectionPerPartition(ctx context.Context, workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	failpoint.Inject("testIndexMergePanicPartitionTableIntersectionWorker", nil)
	defer w.memTracker.Detach()

	for task := range w.workerCh {
		var ok bool
		var hMap *kv.MemAwareHandleMap[*int]
		if hMap, ok = w.handleMapsPerWorker[task.parTblIdx]; !ok {
			hMap = kv.NewMemAwareHandleMap[*int]()
			w.handleMapsPerWorker[task.parTblIdx] = hMap
		}
		var mapDelta int64
		var rowDelta int64
		for _, h := range task.handles {
			// Use *int to avoid Get() again.
			if cntPtr, ok := hMap.Get(h); ok {
				(*cntPtr)++
			} else {
				cnt := 1
				mapDelta += hMap.Set(h, &cnt) + int64(h.ExtraMemSize())
				rowDelta += 1
			}
		}

		logutil.BgLogger().Debug("intersectionProcessWorker handle tasks", zap.Int("workerID", w.workerID),
			zap.Int("task.handles", len(task.handles)), zap.Int64("rowDelta", rowDelta))

		w.mapUsageDelta += mapDelta
		w.rowDelta += rowDelta
		if w.rowDelta >= int64(w.batchSize) {
			w.consumeMemDelta()
		}
		failpoint.Inject("testIndexMergeIntersectionWorkerPanic", nil)
	}
	if w.rowDelta > 0 {
		w.consumeMemDelta()
	}

	// We assume the result of intersection is small, so no need to track memory.
	intersectedMap := make(map[int][]kv.Handle, len(w.handleMapsPerWorker))
	for parTblIdx, hMap := range w.handleMapsPerWorker {
		hMap.Range(func(h kv.Handle, val interface{}) bool {
			if *(val.(*int)) == len(w.indexMerge.partialPlans) {
				// Means all partial paths have this handle.
				intersectedMap[parTblIdx] = append(intersectedMap[parTblIdx], h)
			}
			return true
		})
	}

	tasks := make([]*indexMergeTableTask, 0, len(w.handleMapsPerWorker))
	for parTblIdx, intersected := range intersectedMap {
		// Split intersected[parTblIdx] to avoid task is too large.
		for len(intersected) > 0 {
			length := w.batchSize
			if length > len(intersected) {
				length = len(intersected)
			}
			task := &indexMergeTableTask{
				lookupTableTask: lookupTableTask{
					handles: intersected[:length],
					doneCh:  make(chan error, 1),
				},
			}
			intersected = intersected[length:]
			if w.indexMerge.partitionTableMode {
				task.partitionTable = w.indexMerge.prunedPartitions[parTblIdx]
			}
			tasks = append(tasks, task)
			logutil.BgLogger().Debug("intersectionProcessWorker build tasks",
				zap.Int("parTblIdx", parTblIdx), zap.Int("task.handles", len(task.handles)))
		}
	}
	failpoint.Inject("testIndexMergeProcessWorkerIntersectionHang", func(_ failpoint.Value) {
		for i := 0; i < cap(resultCh); i++ {
			select {
			case resultCh <- &indexMergeTableTask{}:
			default:
			}
		}
	})
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case workCh <- task:
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case resultCh <- task:
			}
		}
	}
}

// For each partition(dynamic mode), a map is used to do intersection. Key of the map is handle, and value is the number of times it occurs.
// If the value of handle equals the number of partial paths, it should be sent to final_table_scan_worker.
// To avoid too many goroutines, each intersectionProcessWorker can handle multiple partitions.
func (w *indexMergeProcessWorker) fetchLoopIntersection(ctx context.Context, fetchCh <-chan *indexMergeTableTask,
	workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	defer close(workCh)

	if w.stats != nil {
		start := time.Now()
		defer func() {
			w.stats.IndexMergeProcess += time.Since(start)
		}()
	}

	failpoint.Inject("testIndexMergePanicProcessWorkerIntersection", nil)

	// One goroutine may handle one or multiple partitions.
	// Max number of partition number is 8192, we use ExecutorConcurrency to avoid too many goroutines.
	maxWorkerCnt := w.indexMerge.ctx.GetSessionVars().IndexMergeIntersectionConcurrency()
	maxChannelSize := atomic.LoadInt32(&LookupTableTaskChannelSize)
	batchSize := w.indexMerge.ctx.GetSessionVars().IndexLookupSize

	partCnt := 1
	if w.indexMerge.partitionTableMode {
		partCnt = len(w.indexMerge.prunedPartitions)
	}
	workerCnt := mathutil.Min(partCnt, maxWorkerCnt)
	failpoint.Inject("testIndexMergeIntersectionConcurrency", func(val failpoint.Value) {
		con := val.(int)
		if con != workerCnt {
			panic(fmt.Sprintf("unexpected workerCnt, expect %d, got %d", con, workerCnt))
		}
	})

	workers := make([]*intersectionProcessWorker, 0, workerCnt)
	wg := util.WaitGroupWrapper{}
	errCh := make(chan bool, workerCnt)
	for i := 0; i < workerCnt; i++ {
		tracker := memory.NewTracker(w.indexMerge.id, -1)
		tracker.AttachTo(w.indexMerge.memTracker)
		worker := &intersectionProcessWorker{
			workerID:            i,
			handleMapsPerWorker: make(map[int]*kv.MemAwareHandleMap[*int]),
			workerCh:            make(chan *indexMergeTableTask, maxChannelSize),
			indexMerge:          w.indexMerge,
			memTracker:          tracker,
			batchSize:           batchSize,
		}
		wg.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "IndexMergeIntersectionProcessWorker").End()
			worker.doIntersectionPerPartition(ctx, workCh, resultCh, finished)
		}, handleWorkerPanic(ctx, finished, resultCh, errCh, partTblIntersectionWorkerType))
		workers = append(workers, worker)
	}
	defer func() {
		for _, processWorker := range workers {
			close(processWorker.workerCh)
		}
		wg.Wait()
	}()
	for {
		var ok bool
		var task *indexMergeTableTask
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case task, ok = <-fetchCh:
			if !ok {
				return
			}
		}

		select {
		case err := <-task.doneCh:
			// If got error from partialIndexWorker/partialTableWorker, stop processing.
			if err != nil {
				syncErr(ctx, finished, resultCh, err)
				return
			}
		default:
		}

		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case workers[task.parTblIdx%workerCnt].workerCh <- task:
		case <-errCh:
			// If got error from intersectionProcessWorker, stop processing.
			return
		}
	}
}

type partialIndexWorker struct {
	stats        *IndexMergeRuntimeStat
	sc           sessionctx.Context
	idxID        int
	batchSize    int
	maxBatchSize int
	maxChunkSize int
	partition    table.PhysicalTable // it indicates if this worker is accessing a particular partition table
	memTracker   *memory.Tracker
}

func syncErr(ctx context.Context, finished <-chan struct{}, errCh chan<- *indexMergeTableTask, err error) {
	logutil.BgLogger().Error("IndexMergeReaderExecutor.syncErr", zap.Error(err))
	doneCh := make(chan error, 1)
	doneCh <- err
	task := &indexMergeTableTask{
		lookupTableTask: lookupTableTask{
			doneCh: doneCh,
		},
	}

	// ctx.Done and finished is to avoid write channel is stuck.
	select {
	case <-ctx.Done():
		return
	case <-finished:
		return
	case errCh <- task:
		return
	}
}

func (w *partialIndexWorker) fetchHandles(
	ctx context.Context,
	result distsql.SelectResult,
	exitCh <-chan struct{},
	fetchCh chan<- *indexMergeTableTask,
	finished <-chan struct{},
	handleCols plannercore.HandleCols,
	parTblIdx int) (count int64, err error) {
	chk := chunk.NewChunkWithCapacity(handleCols.GetFieldsTypes(), w.maxChunkSize)
	var basicStats *execdetails.BasicRuntimeStats
	if w.stats != nil {
		if w.idxID != 0 {
			basicStats = w.sc.GetSessionVars().StmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(w.idxID)
		}
	}
	for {
		start := time.Now()
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, result, handleCols)
		if err != nil {
			syncErr(ctx, finished, fetchCh, err)
			return count, err
		}
		if len(handles) == 0 {
			if basicStats != nil {
				basicStats.Record(time.Since(start), chk.NumRows())
			}
			return count, nil
		}
		count += int64(len(handles))
		task := w.buildTableTask(handles, retChunk, parTblIdx)
		if w.stats != nil {
			atomic.AddInt64(&w.stats.FetchIdxTime, int64(time.Since(start)))
		}
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-exitCh:
			return count, nil
		case <-finished:
			return count, nil
		case fetchCh <- task:
		}
		if basicStats != nil {
			basicStats.Record(time.Since(start), chk.NumRows())
		}
	}
}

func (w *partialIndexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult, handleCols plannercore.HandleCols) (
	handles []kv.Handle, retChk *chunk.Chunk, err error) {
	handles = make([]kv.Handle, 0, w.batchSize)
	var memUsage int64
	defer w.memTracker.Consume(-memUsage)
	for len(handles) < w.batchSize {
		chk.SetRequiredRows(w.batchSize-len(handles), w.maxChunkSize)
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return handles, nil, err
		}
		if chk.NumRows() == 0 {
			failpoint.Inject("testIndexMergeErrorPartialIndexWorker", func(v failpoint.Value) {
				failpoint.Return(handles, nil, errors.New(v.(string)))
			})
			return handles, retChk, nil
		}
		memDelta := chk.MemoryUsage()
		memUsage += memDelta
		w.memTracker.Consume(memDelta)
		for i := 0; i < chk.NumRows(); i++ {
			handle, err := handleCols.BuildHandleFromIndexRow(chk.GetRow(i))
			if err != nil {
				return nil, nil, err
			}
			handles = append(handles, handle)
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (w *partialIndexWorker) buildTableTask(handles []kv.Handle, retChk *chunk.Chunk, parTblIdx int) *indexMergeTableTask {
	task := &indexMergeTableTask{
		lookupTableTask: lookupTableTask{
			handles: handles,
			idxRows: retChk,

			partitionTable: w.partition,
		},
		parTblIdx: parTblIdx,
	}

	task.doneCh = make(chan error, 1)
	return task
}

type indexMergeTableScanWorker struct {
	stats          *IndexMergeRuntimeStat
	workCh         <-chan *indexMergeTableTask
	finished       <-chan struct{}
	indexMergeExec *IndexMergeReaderExecutor
	tblPlans       []plannercore.PhysicalPlan

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker
}

func (w *indexMergeTableScanWorker) pickAndExecTask(ctx context.Context, task **indexMergeTableTask) {
	var ok bool
	for {
		waitStart := time.Now()
		select {
		case <-ctx.Done():
			return
		case <-w.finished:
			return
		case *task, ok = <-w.workCh:
			if !ok {
				return
			}
		}
		// Make sure panic failpoint is after fetch task from workCh.
		// Otherwise cannot send error to task.doneCh.
		failpoint.Inject("testIndexMergePanicTableScanWorker", nil)
		failpoint.Inject("mockSleepBeforeStartTableReader", func(_ failpoint.Value) {
			select {
			case <-ctx.Done():
				failpoint.Return()
			case <-w.finished:
				failpoint.Return()
			}
		})
		execStart := time.Now()
		err := w.executeTask(ctx, *task)
		if w.stats != nil {
			atomic.AddInt64(&w.stats.WaitTime, int64(execStart.Sub(waitStart)))
			atomic.AddInt64(&w.stats.FetchRow, int64(time.Since(execStart)))
			atomic.AddInt64(&w.stats.TableTaskNum, 1)
		}
		failpoint.Inject("testIndexMergePickAndExecTaskPanic", nil)
		select {
		case <-ctx.Done():
			return
		case <-w.finished:
			return
		case (*task).doneCh <- err:
		}
	}
}

func (w *indexMergeTableScanWorker) handleTableScanWorkerPanic(ctx context.Context, finished <-chan struct{}, task **indexMergeTableTask, worker string) func(r interface{}) {
	return func(r interface{}) {
		if r == nil {
			logutil.BgLogger().Info("worker finish without panic", zap.Any("worker", worker))
			return
		}

		err4Panic := errors.Errorf("%s: %v", worker, r)
		logutil.Logger(ctx).Error(err4Panic.Error())
		if *task != nil {
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case (*task).doneCh <- err4Panic:
				return
			}
		}
	}
}

func (w *indexMergeTableScanWorker) executeTask(ctx context.Context, task *indexMergeTableTask) error {
	tbl := w.indexMergeExec.table
	if w.indexMergeExec.partitionTableMode {
		tbl = task.partitionTable
	}
	tableReader, err := w.indexMergeExec.buildFinalTableReader(ctx, tbl, task.handles)
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
		chk := tryNewCacheChunk(tableReader)
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

// IndexMergeRuntimeStat record the indexMerge runtime stat
type IndexMergeRuntimeStat struct {
	IndexMergeProcess time.Duration
	FetchIdxTime      int64
	WaitTime          int64
	FetchRow          int64
	TableTaskNum      int64
	Concurrency       int
}

func (e *IndexMergeRuntimeStat) String() string {
	var buf bytes.Buffer
	if e.FetchIdxTime != 0 {
		buf.WriteString(fmt.Sprintf("index_task:{fetch_handle:%s", time.Duration(e.FetchIdxTime)))
		if e.IndexMergeProcess != 0 {
			buf.WriteString(fmt.Sprintf(", merge:%s", e.IndexMergeProcess))
		}
		buf.WriteByte('}')
	}
	if e.FetchRow != 0 {
		if buf.Len() > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf(" table_task:{num:%d, concurrency:%d, fetch_row:%s, wait_time:%s}", e.TableTaskNum, e.Concurrency, time.Duration(e.FetchRow), time.Duration(e.WaitTime)))
	}
	return buf.String()
}

// Clone implements the RuntimeStats interface.
func (e *IndexMergeRuntimeStat) Clone() execdetails.RuntimeStats {
	newRs := *e
	return &newRs
}

// Merge implements the RuntimeStats interface.
func (e *IndexMergeRuntimeStat) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*IndexMergeRuntimeStat)
	if !ok {
		return
	}
	e.IndexMergeProcess += tmp.IndexMergeProcess
	e.FetchIdxTime += tmp.FetchIdxTime
	e.FetchRow += tmp.FetchRow
	e.WaitTime += e.WaitTime
	e.TableTaskNum += tmp.TableTaskNum
}

// Tp implements the RuntimeStats interface.
func (e *IndexMergeRuntimeStat) Tp() int {
	return execdetails.TpIndexMergeRunTimeStats
}
