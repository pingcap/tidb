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
	"cmp"
	"container/heap"
	"context"
	"fmt"
	"runtime/trace"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ exec.Executor = &IndexMergeReaderExecutor{}

	// IndexMergeCancelFuncForTest is used just for test
	IndexMergeCancelFuncForTest func()
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
	exec.BaseExecutor
	indexUsageReporter *exec.IndexUsageReporter

	table        table.Table
	indexes      []*model.IndexInfo
	descs        []bool
	ranges       [][]*ranger.Range
	dagPBs       []*tipb.DAGRequest
	startTS      uint64
	tableRequest *tipb.DAGRequest

	keepOrder   bool
	pushedLimit *plannercore.PushedDownLimit
	byItems     []*plannerutil.ByItems

	// columns are only required by union scan.
	columns []*model.ColumnInfo
	// partitionIDMap are only required by union scan with global index.
	partitionIDMap map[int64]struct{}
	*dataReaderBuilder

	// fields about accessing partition tables
	partitionTableMode bool                  // if this IndexMerge is accessing a partition table
	prunedPartitions   []table.PhysicalTable // pruned partition tables need to access
	partitionKeyRanges [][][]kv.KeyRange     // [partialIndex][partitionIdx][ranges]

	// All fields above are immutable.

	tblWorkerWg     sync.WaitGroup
	idxWorkerWg     sync.WaitGroup
	processWorkerWg sync.WaitGroup
	finished        chan struct{}

	workerStarted bool
	keyRanges     [][]kv.KeyRange

	resultCh   chan *indexMergeTableTask
	resultCurr *indexMergeTableTask

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker
	paging     bool

	partialPlans        [][]base.PhysicalPlan
	tblPlans            []base.PhysicalPlan
	partialNetDataSizes []float64
	dataAvgRowSize      float64

	handleCols plannerutil.HandleCols
	stats      *IndexMergeRuntimeStat

	// Indicates whether there is correlated column in filter or table/index range.
	// We need to refresh dagPBs before send DAGReq to storage.
	isCorColInPartialFilters []bool
	isCorColInTableFilter    bool
	isCorColInPartialAccess  []bool

	// Whether it's intersection or union.
	isIntersection bool

	hasGlobalIndex bool
}

type indexMergeTableTask struct {
	lookupTableTask

	// parTblIdx are only used in indexMergeProcessWorker.fetchLoopIntersection.
	parTblIdx int

	// partialPlanID are only used for indexMergeProcessWorker.fetchLoopUnionWithOrderBy.
	partialPlanID int
}

// Table implements the dataSourceExecutor interface.
func (e *IndexMergeReaderExecutor) Table() table.Table {
	return e.table
}

// Open implements the Executor Open interface
func (e *IndexMergeReaderExecutor) Open(_ context.Context) (err error) {
	e.keyRanges = make([][]kv.KeyRange, 0, len(e.partialPlans))
	e.initRuntimeStats()
	if e.isCorColInTableFilter {
		e.tableRequest.Executors, err = builder.ConstructListBasedDistExec(e.Ctx().GetBuildPBCtx(), e.tblPlans)
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
		e.partitionKeyRanges = make([][][]kv.KeyRange, len(e.indexes))
		tmpPartitionKeyRanges := make([][][]kv.KeyRange, len(e.prunedPartitions))
		for i, p := range e.prunedPartitions {
			if tmpPartitionKeyRanges[i], err = e.buildKeyRangesForTable(p); err != nil {
				return err
			}
		}
		for i, idx := range e.indexes {
			if idx != nil && idx.Global {
				keyRange, _ := distsql.IndexRangesToKVRanges(e.ctx.GetDistSQLCtx(), e.table.Meta().ID, idx.ID, e.ranges[i])
				e.partitionKeyRanges[i] = [][]kv.KeyRange{keyRange.FirstPartitionRange()}
			} else {
				for _, pKeyRanges := range tmpPartitionKeyRanges {
					e.partitionKeyRanges[i] = append(e.partitionKeyRanges[i], pKeyRanges[i])
				}
			}
		}
	}
	e.finished = make(chan struct{})
	e.resultCh = make(chan *indexMergeTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))
	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.Ctx().GetSessionVars().StmtCtx.MemTracker)
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
				e.ranges[i], err = rebuildIndexRanges(e.Ctx(), x, x.IdxCols, x.IdxColLens)
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
	dctx := e.Ctx().GetDistSQLCtx()
	for i, plan := range e.partialPlans {
		_, ok := plan[0].(*plannercore.PhysicalIndexScan)
		if !ok {
			firstPartRanges, secondPartRanges := distsql.SplitRangesAcrossInt64Boundary(e.ranges[i], false, e.descs[i], tbl.Meta().IsCommonHandle)
			firstKeyRanges, err := distsql.TableHandleRangesToKVRanges(dctx, []int64{getPhysicalTableID(tbl)}, tbl.Meta().IsCommonHandle, firstPartRanges)
			if err != nil {
				return nil, err
			}
			secondKeyRanges, err := distsql.TableHandleRangesToKVRanges(dctx, []int64{getPhysicalTableID(tbl)}, tbl.Meta().IsCommonHandle, secondPartRanges)
			if err != nil {
				return nil, err
			}
			keyRanges := append(firstKeyRanges.FirstPartitionRange(), secondKeyRanges.FirstPartitionRange()...)
			ranges = append(ranges, keyRanges)
			continue
		}
		keyRange, err := distsql.IndexRangesToKVRanges(dctx, getPhysicalTableID(tbl), e.indexes[i].ID, e.ranges[i])
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
					if e.keepOrder {
						// todo: implementing fetchLoopIntersectionWithOrderBy if necessary.
						panic("Not support intersection with keepOrder = true")
					}
					idxMergeProcessWorker.fetchLoopIntersection(ctx, fetch, workCh, e.resultCh, e.finished)
				} else if len(e.byItems) != 0 {
					idxMergeProcessWorker.fetchLoopUnionWithOrderBy(ctx, fetch, workCh, e.resultCh, e.finished)
				} else {
					idxMergeProcessWorker.fetchLoopUnion(ctx, fetch, workCh, e.resultCh, e.finished)
				}
			},
			handleWorkerPanic(ctx, e.finished, nil, e.resultCh, nil, processWorkerType),
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
	if e.RuntimeStats() != nil {
		collExec := true
		e.dagPBs[workID].CollectExecutionSummaries = &collExec
	}

	var keyRanges [][]kv.KeyRange
	if e.partitionTableMode {
		keyRanges = e.partitionKeyRanges[workID]
	} else {
		keyRanges = [][]kv.KeyRange{e.keyRanges[workID]}
	}
	failpoint.Inject("startPartialIndexWorkerErr", func() error {
		return errors.New("inject an error before start partialIndexWorker")
	})

	// for union case, the push-downLimit can be utilized to limit index fetched handles.
	// for intersection case, the push-downLimit can only be conducted after all index path/table finished.
	pushedIndexLimit := e.pushedLimit
	if e.isIntersection {
		pushedIndexLimit = nil
	}

	go func() {
		defer trace.StartRegion(ctx, "IndexMergePartialIndexWorker").End()
		defer e.idxWorkerWg.Done()
		util.WithRecovery(
			func() {
				failpoint.Inject("testIndexMergePanicPartialIndexWorker", nil)
				is := e.partialPlans[workID][0].(*plannercore.PhysicalIndexScan)
				worker := &partialIndexWorker{
					stats:              e.stats,
					idxID:              e.getPartitalPlanID(workID),
					sc:                 e.Ctx(),
					dagPB:              e.dagPBs[workID],
					plan:               e.partialPlans[workID],
					batchSize:          e.MaxChunkSize(),
					maxBatchSize:       e.Ctx().GetSessionVars().IndexLookupSize,
					maxChunkSize:       e.MaxChunkSize(),
					memTracker:         e.memTracker,
					partitionTableMode: e.partitionTableMode,
					prunedPartitions:   e.prunedPartitions,
					byItems:            is.ByItems,
					pushedLimit:        pushedIndexLimit,
				}
				if e.isCorColInPartialFilters[workID] {
					// We got correlated column, so need to refresh Selection operator.
					var err error
					if e.dagPBs[workID].Executors, err = builder.ConstructListBasedDistExec(e.Ctx().GetBuildPBCtx(), e.partialPlans[workID]); err != nil {
						syncErr(ctx, e.finished, fetchCh, err)
						return
					}
				}
				var builder distsql.RequestBuilder
				builder.SetDAGRequest(e.dagPBs[workID]).
					SetStartTS(e.startTS).
					SetDesc(e.descs[workID]).
					SetKeepOrder(e.keepOrder).
					SetTxnScope(e.txnScope).
					SetReadReplicaScope(e.readReplicaScope).
					SetIsStaleness(e.isStaleness).
					SetFromSessionVars(e.Ctx().GetDistSQLCtx()).
					SetMemTracker(e.memTracker).
					SetPaging(e.paging).
					SetFromInfoSchema(e.Ctx().GetInfoSchema()).
					SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.Ctx().GetDistSQLCtx(), &builder.Request, e.partialNetDataSizes[workID])).
					SetConnIDAndConnAlias(e.Ctx().GetSessionVars().ConnectionID, e.Ctx().GetSessionVars().SessionAlias)

				tps := worker.getRetTpsForIndexScan(e.handleCols)
				results := make([]distsql.SelectResult, 0, len(keyRanges))
				defer func() {
					// To make sure SelectResult.Close() is called even got panic in fetchHandles().
					for _, result := range results {
						if err := result.Close(); err != nil {
							logutil.Logger(ctx).Error("close Select result failed", zap.Error(err))
						}
					}
				}()
				for _, keyRange := range keyRanges {
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
					slices.SortFunc(keyRange, func(i, j kv.KeyRange) int {
						return bytes.Compare(i.StartKey, j.StartKey)
					})
					kvReq, err := builder.SetKeyRanges(keyRange).Build()
					if err != nil {
						syncErr(ctx, e.finished, fetchCh, err)
						return
					}
					result, err := distsql.SelectWithRuntimeStats(ctx, e.Ctx().GetDistSQLCtx(), kvReq, tps, getPhysicalPlanIDs(e.partialPlans[workID]), e.getPartitalPlanID(workID))
					if err != nil {
						syncErr(ctx, e.finished, fetchCh, err)
						return
					}
					results = append(results, result)
					failpoint.Inject("testIndexMergePartialIndexWorkerCoprLeak", nil)
				}
				worker.batchSize = min(e.MaxChunkSize(), worker.maxBatchSize)
				if len(results) > 1 && len(e.byItems) != 0 {
					// e.Schema() not the output schema for partialIndexReader, and we put byItems related column at first in `buildIndexReq`, so use nil here.
					ssr := distsql.NewSortedSelectResults(e.Ctx().GetExprCtx().GetEvalCtx(), results, nil, e.byItems, e.memTracker)
					results = []distsql.SelectResult{ssr}
				}
				ctx1, cancel := context.WithCancel(ctx)
				// this error is reported in fetchHandles(), so ignore it here.
				_, _ = worker.fetchHandles(ctx1, results, exitCh, fetchCh, e.finished, e.handleCols, workID)
				cancel()
			},
			handleWorkerPanic(ctx, e.finished, nil, fetchCh, nil, partialIndexWorkerType),
		)
	}()

	return nil
}

func (e *IndexMergeReaderExecutor) startPartialTableWorker(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *indexMergeTableTask, workID int) error {
	ts := e.partialPlans[workID][0].(*plannercore.PhysicalTableScan)

	tbls := make([]table.Table, 0, 1)
	if e.partitionTableMode && len(e.byItems) == 0 {
		for _, p := range e.prunedPartitions {
			tbls = append(tbls, p)
		}
	} else {
		tbls = append(tbls, e.table)
	}

	// for union case, the push-downLimit can be utilized to limit index fetched handles.
	// for intersection case, the push-downLimit can only be conducted after all index/table path finished.
	pushedTableLimit := e.pushedLimit
	if e.isIntersection {
		pushedTableLimit = nil
	}

	go func() {
		defer trace.StartRegion(ctx, "IndexMergePartialTableWorker").End()
		defer e.idxWorkerWg.Done()
		util.WithRecovery(
			func() {
				failpoint.Inject("testIndexMergePanicPartialTableWorker", nil)
				var err error
				partialTableReader := &TableReaderExecutor{
					BaseExecutorV2:             exec.NewBaseExecutorV2(e.Ctx().GetSessionVars(), ts.Schema(), e.getPartitalPlanID(workID)),
					tableReaderExecutorContext: newTableReaderExecutorContext(e.Ctx()),
					dagPB:                      e.dagPBs[workID],
					startTS:                    e.startTS,
					txnScope:                   e.txnScope,
					readReplicaScope:           e.readReplicaScope,
					isStaleness:                e.isStaleness,
					plans:                      e.partialPlans[workID],
					ranges:                     e.ranges[workID],
					netDataSize:                e.partialNetDataSizes[workID],
					keepOrder:                  ts.KeepOrder,
					byItems:                    ts.ByItems,
				}

				worker := &partialTableWorker{
					stats:              e.stats,
					sc:                 e.Ctx(),
					batchSize:          e.MaxChunkSize(),
					maxBatchSize:       e.Ctx().GetSessionVars().IndexLookupSize,
					maxChunkSize:       e.MaxChunkSize(),
					tableReader:        partialTableReader,
					memTracker:         e.memTracker,
					partitionTableMode: e.partitionTableMode,
					prunedPartitions:   e.prunedPartitions,
					byItems:            ts.ByItems,
					pushedLimit:        pushedTableLimit,
				}

				if len(e.prunedPartitions) != 0 && len(e.byItems) != 0 {
					slices.SortFunc(worker.prunedPartitions, func(i, j table.PhysicalTable) int {
						return cmp.Compare(i.GetPhysicalID(), j.GetPhysicalID())
					})
					partialTableReader.kvRangeBuilder = kvRangeBuilderFromRangeAndPartition{
						sctx:       e.Ctx(),
						partitions: worker.prunedPartitions,
					}
				}

				if e.isCorColInPartialFilters[workID] {
					if e.dagPBs[workID].Executors, err = builder.ConstructListBasedDistExec(e.Ctx().GetBuildPBCtx(), e.partialPlans[workID]); err != nil {
						syncErr(ctx, e.finished, fetchCh, err)
						return
					}
					partialTableReader.dagPB = e.dagPBs[workID]
				}

				var tableReaderClosed bool
				defer func() {
					// To make sure SelectResult.Close() is called even got panic in fetchHandles().
					if !tableReaderClosed {
						terror.Log(exec.Close(worker.tableReader))
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
					if err = exec.Open(ctx, partialTableReader); err != nil {
						logutil.Logger(ctx).Error("open Select result failed:", zap.Error(err))
						syncErr(ctx, e.finished, fetchCh, err)
						break
					}
					failpoint.Inject("testIndexMergePartialTableWorkerCoprLeak", nil)
					tableReaderClosed = false
					worker.batchSize = e.MaxChunkSize()
					if worker.batchSize > worker.maxBatchSize {
						worker.batchSize = worker.maxBatchSize
					}

					// fetch all handles from this table
					ctx1, cancel := context.WithCancel(ctx)
					_, fetchErr := worker.fetchHandles(ctx1, exitCh, fetchCh, e.finished, e.handleCols, parTblIdx, workID)
					// release related resources
					cancel()
					tableReaderClosed = true
					if err = exec.Close(worker.tableReader); err != nil {
						logutil.Logger(ctx).Error("close Select result failed:", zap.Error(err))
					}
					// this error is reported in fetchHandles(), so ignore it here.
					if fetchErr != nil {
						break
					}
				}
			},
			handleWorkerPanic(ctx, e.finished, nil, fetchCh, nil, partialTableWorkerType),
		)
	}()
	return nil
}

func (e *IndexMergeReaderExecutor) initRuntimeStats() {
	if e.RuntimeStats() != nil {
		e.stats = &IndexMergeRuntimeStat{
			Concurrency: e.Ctx().GetSessionVars().IndexLookupConcurrency(),
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
	return e.ID()
}

type partialTableWorker struct {
	stats              *IndexMergeRuntimeStat
	sc                 sessionctx.Context
	batchSize          int
	maxBatchSize       int
	maxChunkSize       int
	tableReader        exec.Executor
	memTracker         *memory.Tracker
	partitionTableMode bool
	prunedPartitions   []table.PhysicalTable
	byItems            []*plannerutil.ByItems
	scannedKeys        uint64
	pushedLimit        *plannercore.PushedDownLimit
}

// needPartitionHandle indicates whether we need create a partitionHandle or not.
// If the schema from planner part contains ExtraPhysTblID,
// we need create a partitionHandle, otherwise create a normal handle.
// In TableRowIDScan, the partitionHandle will be used to create key ranges.
func (w *partialTableWorker) needPartitionHandle() (bool, error) {
	cols := w.tableReader.(*TableReaderExecutor).plans[0].Schema().Columns
	outputOffsets := w.tableReader.(*TableReaderExecutor).dagPB.OutputOffsets
	col := cols[outputOffsets[len(outputOffsets)-1]]

	needPartitionHandle := w.partitionTableMode && len(w.byItems) > 0
	// no ExtraPidColID here, because a clustered index couldn't be a global index.
	hasExtraCol := col.ID == model.ExtraPhysTblID

	// There will be two needPartitionHandle != hasExtraCol situations.
	// Only `needPartitionHandle` == true and `hasExtraCol` == false are not allowed.
	// `ExtraPhysTblID` will be used in `SelectLock` when `needPartitionHandle` == false and `hasExtraCol` == true.
	if needPartitionHandle && !hasExtraCol {
		return needPartitionHandle, errors.Errorf("Internal error, needPartitionHandle != ret")
	}
	return needPartitionHandle, nil
}

func (w *partialTableWorker) fetchHandles(ctx context.Context, exitCh <-chan struct{}, fetchCh chan<- *indexMergeTableTask,
	finished <-chan struct{}, handleCols plannerutil.HandleCols, parTblIdx int, partialPlanIndex int) (count int64, err error) {
	chk := w.tableReader.NewChunkWithCapacity(w.getRetTpsForTableScan(), w.maxChunkSize, w.maxBatchSize)
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
		task := w.buildTableTask(handles, retChunk, parTblIdx, partialPlanIndex)
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
	}
}

func (w *partialTableWorker) getRetTpsForTableScan() []*types.FieldType {
	return exec.RetTypes(w.tableReader)
}

func (w *partialTableWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, handleCols plannerutil.HandleCols) (
	handles []kv.Handle, retChk *chunk.Chunk, err error) {
	handles = make([]kv.Handle, 0, w.batchSize)
	if len(w.byItems) != 0 {
		retChk = chunk.NewChunkWithCapacity(w.getRetTpsForTableScan(), w.batchSize)
	}
	var memUsage int64
	var chunkRowOffset int
	defer w.memTracker.Consume(-memUsage)
	for len(handles) < w.batchSize {
		requiredRows := w.batchSize - len(handles)
		if w.pushedLimit != nil {
			if w.pushedLimit.Offset+w.pushedLimit.Count <= w.scannedKeys {
				return handles, retChk, nil
			}
			requiredRows = min(int(w.pushedLimit.Offset+w.pushedLimit.Count-w.scannedKeys), requiredRows)
		}
		chk.SetRequiredRows(requiredRows, w.maxChunkSize)
		start := time.Now()
		err = errors.Trace(w.tableReader.Next(ctx, chk))
		if err != nil {
			return nil, nil, err
		}
		if w.tableReader != nil && w.tableReader.RuntimeStats() != nil {
			w.tableReader.RuntimeStats().Record(time.Since(start), chk.NumRows())
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
		for chunkRowOffset = 0; chunkRowOffset < chk.NumRows(); chunkRowOffset++ {
			if w.pushedLimit != nil {
				w.scannedKeys++
				if w.scannedKeys > (w.pushedLimit.Offset + w.pushedLimit.Count) {
					// Skip the handles after Offset+Count.
					break
				}
			}
			var handle kv.Handle
			ok, err1 := w.needPartitionHandle()
			if err1 != nil {
				return nil, nil, err1
			}
			if ok {
				handle, err = handleCols.BuildPartitionHandleFromIndexRow(chk.GetRow(chunkRowOffset))
			} else {
				handle, err = handleCols.BuildHandleFromIndexRow(chk.GetRow(chunkRowOffset))
			}
			if err != nil {
				return nil, nil, err
			}
			handles = append(handles, handle)
		}
		// used for order by
		if len(w.byItems) != 0 {
			retChk.Append(chk, 0, chunkRowOffset)
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (w *partialTableWorker) buildTableTask(handles []kv.Handle, retChk *chunk.Chunk, parTblIdx int, partialPlanID int) *indexMergeTableTask {
	task := &indexMergeTableTask{
		lookupTableTask: lookupTableTask{
			handles: handles,
			idxRows: retChk,
		},
		parTblIdx:     parTblIdx,
		partialPlanID: partialPlanID,
	}

	if w.prunedPartitions != nil {
		task.partitionTable = w.prunedPartitions[parTblIdx]
	}

	task.doneCh = make(chan error, 1)
	return task
}

func (e *IndexMergeReaderExecutor) startIndexMergeTableScanWorker(ctx context.Context, workCh <-chan *indexMergeTableTask) {
	lookupConcurrencyLimit := e.Ctx().GetSessionVars().IndexLookupConcurrency()
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

func (e *IndexMergeReaderExecutor) buildFinalTableReader(ctx context.Context, tbl table.Table, handles []kv.Handle) (_ exec.Executor, err error) {
	tableReaderExec := &TableReaderExecutor{
		BaseExecutorV2:             exec.NewBaseExecutorV2(e.Ctx().GetSessionVars(), e.Schema(), e.getTablePlanRootID()),
		tableReaderExecutorContext: newTableReaderExecutorContext(e.Ctx()),
		table:                      tbl,
		dagPB:                      e.tableRequest,
		startTS:                    e.startTS,
		txnScope:                   e.txnScope,
		readReplicaScope:           e.readReplicaScope,
		isStaleness:                e.isStaleness,
		columns:                    e.columns,
		plans:                      e.tblPlans,
		netDataSize:                e.dataAvgRowSize * float64(len(handles)),
	}
	tableReaderExec.buildVirtualColumnInfo()
	// Reorder handles because SplitKeyRangesByLocationsWith/WithoutBuckets() requires startKey of kvRanges is ordered.
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
		resultTask, err := e.getResultTask(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if resultTask == nil {
			return nil
		}
		if resultTask.cursor < len(resultTask.rows) {
			numToAppend := min(len(resultTask.rows)-resultTask.cursor, e.MaxChunkSize()-req.NumRows())
			req.AppendRows(resultTask.rows[resultTask.cursor : resultTask.cursor+numToAppend])
			resultTask.cursor += numToAppend
			if req.NumRows() >= e.MaxChunkSize() {
				return nil
			}
		}
	}
}

func (e *IndexMergeReaderExecutor) getResultTask(ctx context.Context) (*indexMergeTableTask, error) {
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

	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	case err := <-task.doneCh:
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Release the memory usage of last task before we handle a new task.
	if e.resultCurr != nil {
		e.resultCurr.memTracker.Consume(-e.resultCurr.memUsage)
	}
	e.resultCurr = task
	return e.resultCurr, nil
}

func handleWorkerPanic(ctx context.Context, finished, limitDone <-chan struct{}, ch chan<- *indexMergeTableTask, extraNotifyCh chan bool, worker string) func(r any) {
	return func(r any) {
		if worker == processWorkerType {
			// There is only one processWorker, so it's safe to close here.
			// No need to worry about "close on closed channel" error.
			defer close(ch)
		}
		if r == nil {
			logutil.BgLogger().Debug("worker finish without panic", zap.Any("worker", worker))
			return
		}

		if extraNotifyCh != nil {
			extraNotifyCh <- true
		}

		err4Panic := util.GetRecoverError(r)
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
		case <-limitDone:
			// once the process worker recovered from panic, once finding the limitDone signal, actually we can return.
			return
		case ch <- task:
			return
		}
	}
}

// Close implements Exec Close interface.
func (e *IndexMergeReaderExecutor) Close() error {
	if e.stats != nil {
		defer e.Ctx().GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}
	if e.indexUsageReporter != nil {
		for _, p := range e.partialPlans {
			is, ok := p[0].(*plannercore.PhysicalIndexScan)
			if !ok {
				continue
			}

			e.indexUsageReporter.ReportCopIndexUsageForTable(e.table, is.Index.ID, is.ID())
		}
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
	return nil
}

type indexMergeProcessWorker struct {
	indexMerge *IndexMergeReaderExecutor
	stats      *IndexMergeRuntimeStat
}

type rowIdx struct {
	partialID int
	taskID    int
	rowID     int
}

type handleHeap struct {
	// requiredCnt == 0 means need all handles
	requiredCnt uint64
	tracker     *memory.Tracker
	taskMap     map[int][]*indexMergeTableTask

	idx         []rowIdx
	compareFunc []chunk.CompareFunc
	byItems     []*plannerutil.ByItems
}

func (h handleHeap) Len() int {
	return len(h.idx)
}

func (h handleHeap) Less(i, j int) bool {
	rowI := h.taskMap[h.idx[i].partialID][h.idx[i].taskID].idxRows.GetRow(h.idx[i].rowID)
	rowJ := h.taskMap[h.idx[j].partialID][h.idx[j].taskID].idxRows.GetRow(h.idx[j].rowID)

	for i, compFunc := range h.compareFunc {
		cmp := compFunc(rowI, i, rowJ, i)
		if !h.byItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}

func (h handleHeap) Swap(i, j int) {
	h.idx[i], h.idx[j] = h.idx[j], h.idx[i]
}

func (h *handleHeap) Push(x any) {
	idx := x.(rowIdx)
	h.idx = append(h.idx, idx)
	if h.tracker != nil {
		h.tracker.Consume(int64(unsafe.Sizeof(h.idx)))
	}
}

func (h *handleHeap) Pop() any {
	idxRet := h.idx[len(h.idx)-1]
	h.idx = h.idx[:len(h.idx)-1]
	if h.tracker != nil {
		h.tracker.Consume(-int64(unsafe.Sizeof(h.idx)))
	}
	return idxRet
}

func (w *indexMergeProcessWorker) NewHandleHeap(taskMap map[int][]*indexMergeTableTask, memTracker *memory.Tracker) *handleHeap {
	compareFuncs := make([]chunk.CompareFunc, 0, len(w.indexMerge.byItems))
	for _, item := range w.indexMerge.byItems {
		keyType := item.Expr.GetType(w.indexMerge.Ctx().GetExprCtx().GetEvalCtx())
		compareFuncs = append(compareFuncs, chunk.GetCompareFunc(keyType))
	}

	requiredCnt := uint64(0)
	if w.indexMerge.pushedLimit != nil {
		// Pre-allocate up to 1024 to avoid oom
		requiredCnt = min(1024, w.indexMerge.pushedLimit.Count+w.indexMerge.pushedLimit.Offset)
	}
	return &handleHeap{
		requiredCnt: requiredCnt,
		tracker:     memTracker,
		taskMap:     taskMap,
		idx:         make([]rowIdx, 0, requiredCnt),
		compareFunc: compareFuncs,
		byItems:     w.indexMerge.byItems,
	}
}

// pruneTableWorkerTaskIdxRows prune idxRows and only keep columns that will be used in byItems.
// e.g. the common handle is (`b`, `c`) and order by with column `c`, we should make column `c` at the first.
func (w *indexMergeProcessWorker) pruneTableWorkerTaskIdxRows(task *indexMergeTableTask) {
	if task.idxRows == nil {
		return
	}
	// IndexScan no need to prune retChk, Columns required by byItems are always first.
	if plan, ok := w.indexMerge.partialPlans[task.partialPlanID][0].(*plannercore.PhysicalTableScan); ok {
		prune := make([]int, 0, len(w.indexMerge.byItems))
		for _, item := range plan.ByItems {
			c, _ := item.Expr.(*expression.Column)
			idx := plan.Schema().ColumnIndex(c)
			// couldn't equals to -1 here, if idx == -1, just let it panic
			prune = append(prune, idx)
		}
		task.idxRows = task.idxRows.Prune(prune)
	}
}

func (w *indexMergeProcessWorker) fetchLoopUnionWithOrderBy(ctx context.Context, fetchCh <-chan *indexMergeTableTask,
	workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	memTracker := memory.NewTracker(w.indexMerge.ID(), -1)
	memTracker.AttachTo(w.indexMerge.memTracker)
	defer memTracker.Detach()
	defer close(workCh)

	if w.stats != nil {
		start := time.Now()
		defer func() {
			w.stats.IndexMergeProcess += time.Since(start)
		}()
	}

	distinctHandles := kv.NewHandleMap()
	taskMap := make(map[int][]*indexMergeTableTask)
	uselessMap := make(map[int]struct{})
	taskHeap := w.NewHandleHeap(taskMap, memTracker)

	for task := range fetchCh {
		select {
		case err := <-task.doneCh:
			// If got error from partialIndexWorker/partialTableWorker, stop processing.
			if err != nil {
				syncErr(ctx, finished, resultCh, err)
				return
			}
		default:
		}
		if _, ok := uselessMap[task.partialPlanID]; ok {
			continue
		}
		if _, ok := taskMap[task.partialPlanID]; !ok {
			taskMap[task.partialPlanID] = make([]*indexMergeTableTask, 0, 1)
		}
		w.pruneTableWorkerTaskIdxRows(task)
		taskMap[task.partialPlanID] = append(taskMap[task.partialPlanID], task)
		for i, h := range task.handles {
			if _, ok := distinctHandles.Get(h); !ok {
				distinctHandles.Set(h, true)
				heap.Push(taskHeap, rowIdx{task.partialPlanID, len(taskMap[task.partialPlanID]) - 1, i})
				if int(taskHeap.requiredCnt) != 0 && taskHeap.Len() > int(taskHeap.requiredCnt) {
					top := heap.Pop(taskHeap).(rowIdx)
					if top.partialID == task.partialPlanID && top.taskID == len(taskMap[task.partialPlanID])-1 && top.rowID == i {
						uselessMap[task.partialPlanID] = struct{}{}
						task.handles = task.handles[:i]
						break
					}
				}
			}
			memTracker.Consume(int64(h.MemUsage()))
		}
		memTracker.Consume(task.idxRows.MemoryUsage())
		if len(uselessMap) == len(w.indexMerge.partialPlans) {
			// consume reset tasks
			go func() {
				channel.Clear(fetchCh)
			}()
			break
		}
	}

	needCount := taskHeap.Len()
	if w.indexMerge.pushedLimit != nil {
		needCount = max(0, taskHeap.Len()-int(w.indexMerge.pushedLimit.Offset))
	}
	if needCount == 0 {
		return
	}
	fhs := make([]kv.Handle, needCount)
	for i := needCount - 1; i >= 0; i-- {
		idx := heap.Pop(taskHeap).(rowIdx)
		fhs[i] = taskMap[idx.partialID][idx.taskID].handles[idx.rowID]
	}

	batchSize := w.indexMerge.Ctx().GetSessionVars().IndexLookupSize
	tasks := make([]*indexMergeTableTask, 0, len(fhs)/batchSize+1)
	for len(fhs) > 0 {
		l := min(len(fhs), batchSize)
		// Save the index order.
		indexOrder := kv.NewHandleMap()
		for i, h := range fhs[:l] {
			indexOrder.Set(h, i)
		}
		tasks = append(tasks, &indexMergeTableTask{
			lookupTableTask: lookupTableTask{
				handles:    fhs[:l],
				indexOrder: indexOrder,
				doneCh:     make(chan error, 1),
			},
		})
		fhs = fhs[l:]
	}
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case resultCh <- task:
			failpoint.Inject("testCancelContext", func() {
				IndexMergeCancelFuncForTest()
			})
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case workCh <- task:
				continue
			}
		}
	}
}

func pushedLimitCountingDown(pushedLimit *plannercore.PushedDownLimit, handles []kv.Handle) (next bool, res []kv.Handle) {
	fhsLen := uint64(len(handles))
	// The number of handles is less than the offset, discard all handles.
	if fhsLen <= pushedLimit.Offset {
		pushedLimit.Offset -= fhsLen
		return true, nil
	}
	handles = handles[pushedLimit.Offset:]
	pushedLimit.Offset = 0

	fhsLen = uint64(len(handles))
	// The number of handles is greater than the limit, only keep limit count.
	if fhsLen > pushedLimit.Count {
		handles = handles[:pushedLimit.Count]
	}
	pushedLimit.Count -= min(pushedLimit.Count, fhsLen)
	return false, handles
}

func (w *indexMergeProcessWorker) fetchLoopUnion(ctx context.Context, fetchCh <-chan *indexMergeTableTask,
	workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	failpoint.Inject("testIndexMergeResultChCloseEarly", func(_ failpoint.Value) {
		failpoint.Return()
	})
	memTracker := memory.NewTracker(w.indexMerge.ID(), -1)
	memTracker.AttachTo(w.indexMerge.memTracker)
	defer memTracker.Detach()
	defer close(workCh)
	failpoint.Inject("testIndexMergePanicProcessWorkerUnion", nil)

	var pushedLimit *plannercore.PushedDownLimit
	if w.indexMerge.pushedLimit != nil {
		pushedLimit = w.indexMerge.pushedLimit.Clone()
	}
	hMap := kv.NewHandleMap()
	for {
		var ok bool
		var task *indexMergeTableTask
		if pushedLimit != nil && pushedLimit.Count == 0 {
			return
		}
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
		for _, h := range handles {
			if w.indexMerge.partitionTableMode {
				if _, ok := h.(kv.PartitionHandle); !ok {
					h = kv.NewPartitionHandle(task.partitionTable.GetPhysicalID(), h)
				}
			}
			if _, ok := hMap.Get(h); !ok {
				fhs = append(fhs, h)
				hMap.Set(h, true)
			}
		}
		if len(fhs) == 0 {
			continue
		}
		if pushedLimit != nil {
			next, res := pushedLimitCountingDown(pushedLimit, fhs)
			if next {
				continue
			}
			fhs = res
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
		case resultCh <- task:
			failpoint.Inject("testCancelContext", func() {
				IndexMergeCancelFuncForTest()
			})
			select {
			case <-ctx.Done():
				return
			case <-finished:
				return
			case workCh <- task:
			}
		}
	}
}

// intersectionCollectWorker is used to dispatch index-merge-table-task to original workCh and resultCh.
// a kind of interceptor to control the pushed down limit restriction. (should be no performance impact)
type intersectionCollectWorker struct {
	pushedLimit *plannercore.PushedDownLimit
	collectCh   chan *indexMergeTableTask
	limitDone   chan struct{}
}

func (w *intersectionCollectWorker) doIntersectionLimitAndDispatch(ctx context.Context, workCh chan<- *indexMergeTableTask,
	resultCh chan<- *indexMergeTableTask, finished <-chan struct{}) {
	var (
		ok   bool
		task *indexMergeTableTask
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case task, ok = <-w.collectCh:
			if !ok {
				return
			}
			// receive a new intersection task here, adding limit restriction logic
			if w.pushedLimit != nil {
				if w.pushedLimit.Count == 0 {
					// close limitDone channel to notify intersectionProcessWorkers * N to exit.
					close(w.limitDone)
					return
				}
				next, handles := pushedLimitCountingDown(w.pushedLimit, task.handles)
				if next {
					continue
				}
				task.handles = handles
			}
			// dispatch the new task to workCh and resultCh.
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

	partitionIDMap map[int64]int
}

func (w *intersectionProcessWorker) consumeMemDelta() {
	w.memTracker.Consume(w.mapUsageDelta + w.rowDelta*int64(unsafe.Sizeof(int(0))))
	w.mapUsageDelta = 0
	w.rowDelta = 0
}

// doIntersectionPerPartition fetch all the task from workerChannel, and after that, then do the intersection pruning, which
// will cause wasting a lot of time waiting for all the fetch task done.
func (w *intersectionProcessWorker) doIntersectionPerPartition(ctx context.Context, workCh chan<- *indexMergeTableTask, resultCh chan<- *indexMergeTableTask, finished, limitDone <-chan struct{}) {
	failpoint.Inject("testIndexMergePanicPartitionTableIntersectionWorker", nil)
	defer w.memTracker.Detach()

	for task := range w.workerCh {
		var ok bool
		var hMap *kv.MemAwareHandleMap[*int]
		if hMap, ok = w.handleMapsPerWorker[task.parTblIdx]; !ok {
			hMap = kv.NewMemAwareHandleMap[*int]()
			w.handleMapsPerWorker[task.parTblIdx] = hMap
		}
		var mapDelta, rowDelta int64
		for _, h := range task.handles {
			if w.indexMerge.hasGlobalIndex {
				if ph, ok := h.(kv.PartitionHandle); ok {
					if v, exists := w.partitionIDMap[ph.PartitionID]; exists {
						if hMap, ok = w.handleMapsPerWorker[v]; !ok {
							hMap = kv.NewMemAwareHandleMap[*int]()
							w.handleMapsPerWorker[v] = hMap
						}
					}
				} else {
					h = kv.NewPartitionHandle(task.partitionTable.GetPhysicalID(), h)
				}
			}
			// Use *int to avoid Get() again.
			if cntPtr, ok := hMap.Get(h); ok {
				(*cntPtr)++
			} else {
				cnt := 1
				mapDelta += hMap.Set(h, &cnt) + int64(h.ExtraMemSize())
				rowDelta++
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
		hMap.Range(func(h kv.Handle, val *int) bool {
			if *(val) == len(w.indexMerge.partialPlans) {
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
		if resultCh != nil {
			for i := 0; i < cap(resultCh); i++ {
				select {
				case resultCh <- &indexMergeTableTask{}:
				default:
				}
			}
		}
	})
	for _, task := range tasks {
		select {
		case <-ctx.Done():
			return
		case <-finished:
			return
		case <-limitDone:
			// limitDone has signal means the collectWorker has collected enough results, shutdown process workers quickly here.
			return
		case workCh <- task:
			// resultCh != nil means there is no collectWorker, and we should send task to resultCh too by ourselves here.
			if resultCh != nil {
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
}

// for every index merge process worker, it should be feed on a sortedSelectResult for every partial index plan (constructed on all
// table partition ranges results on that index plan path). Since every partial index path is a sorted select result, we can utilize
// K-way merge to accelerate the intersection process.
//
// partialIndexPlan-1 ---> SSR --->  +
// partialIndexPlan-2 ---> SSR --->  + ---> SSR K-way Merge ---> output IndexMergeTableTask
// partialIndexPlan-3 ---> SSR --->  +
// ...                               +
// partialIndexPlan-N ---> SSR --->  +
//
// K-way merge detail: for every partial index plan, output one row as current its representative row. Then, comparing the N representative
// rows together:
//
// Loop start:
//
//	case 1: they are all the same, intersection succeed. --- Record current handle down (already in index order).
//	case 2: distinguish among them, for the minimum value/values corresponded index plan/plans. --- Discard current representative row, fetch next.
//
// goto Loop start:
//
// encapsulate all the recorded handles (already in index order) as index merge table tasks, sending them out.
func (*indexMergeProcessWorker) fetchLoopIntersectionWithOrderBy(_ context.Context, _ <-chan *indexMergeTableTask,
	_ chan<- *indexMergeTableTask, _ chan<- *indexMergeTableTask, _ <-chan struct{}) {
	// todo: pushed sort property with partial index plan and limit.
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
	maxWorkerCnt := w.indexMerge.Ctx().GetSessionVars().IndexMergeIntersectionConcurrency()
	maxChannelSize := atomic.LoadInt32(&LookupTableTaskChannelSize)
	batchSize := w.indexMerge.Ctx().GetSessionVars().IndexLookupSize

	partCnt := 1
	// To avoid multi-threaded access the handle map, we only use one worker for indexMerge with global index.
	if w.indexMerge.partitionTableMode && !w.indexMerge.hasGlobalIndex {
		partCnt = len(w.indexMerge.prunedPartitions)
	}
	workerCnt := min(partCnt, maxWorkerCnt)
	failpoint.Inject("testIndexMergeIntersectionConcurrency", func(val failpoint.Value) {
		con := val.(int)
		if con != workerCnt {
			panic(fmt.Sprintf("unexpected workerCnt, expect %d, got %d", con, workerCnt))
		}
	})

	partitionIDMap := make(map[int64]int)
	if w.indexMerge.hasGlobalIndex {
		for i, p := range w.indexMerge.prunedPartitions {
			partitionIDMap[p.GetPhysicalID()] = i
		}
	}

	workers := make([]*intersectionProcessWorker, 0, workerCnt)
	var collectWorker *intersectionCollectWorker
	wg := util.WaitGroupWrapper{}
	wg2 := util.WaitGroupWrapper{}
	errCh := make(chan bool, workerCnt)
	var limitDone chan struct{}
	if w.indexMerge.pushedLimit != nil {
		// no memory cost for this code logic.
		collectWorker = &intersectionCollectWorker{
			// same size of workCh/resultCh
			collectCh:   make(chan *indexMergeTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize)),
			pushedLimit: w.indexMerge.pushedLimit.Clone(),
			limitDone:   make(chan struct{}),
		}
		limitDone = collectWorker.limitDone
		wg2.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "IndexMergeIntersectionProcessWorker").End()
			collectWorker.doIntersectionLimitAndDispatch(ctx, workCh, resultCh, finished)
		}, handleWorkerPanic(ctx, finished, nil, resultCh, errCh, partTblIntersectionWorkerType))
	}
	for i := 0; i < workerCnt; i++ {
		tracker := memory.NewTracker(w.indexMerge.ID(), -1)
		tracker.AttachTo(w.indexMerge.memTracker)
		worker := &intersectionProcessWorker{
			workerID:            i,
			handleMapsPerWorker: make(map[int]*kv.MemAwareHandleMap[*int]),
			workerCh:            make(chan *indexMergeTableTask, maxChannelSize),
			indexMerge:          w.indexMerge,
			memTracker:          tracker,
			batchSize:           batchSize,
			partitionIDMap:      partitionIDMap,
		}
		wg.RunWithRecover(func() {
			defer trace.StartRegion(ctx, "IndexMergeIntersectionProcessWorker").End()
			if collectWorker != nil {
				// workflow:
				// intersectionProcessWorker-1  --+                       (limit restriction logic)
				// intersectionProcessWorker-2  --+--------- collectCh--> intersectionCollectWorker +--> workCh --> table worker
				// ...                          --+  <--- limitDone to shut inputs ------+          +-> resultCh --> upper parent
				// intersectionProcessWorker-N  --+
				worker.doIntersectionPerPartition(ctx, collectWorker.collectCh, nil, finished, collectWorker.limitDone)
			} else {
				// workflow:
				// intersectionProcessWorker-1  --------------------------+--> workCh   --> table worker
				// intersectionProcessWorker-2  ---(same as above)        +--> resultCh --> upper parent
				// ...                          ---(same as above)
				// intersectionProcessWorker-N  ---(same as above)
				worker.doIntersectionPerPartition(ctx, workCh, resultCh, finished, nil)
			}
		}, handleWorkerPanic(ctx, finished, limitDone, resultCh, errCh, partTblIntersectionWorkerType))
		workers = append(workers, worker)
	}
	defer func() {
		for _, processWorker := range workers {
			close(processWorker.workerCh)
		}
		wg.Wait()
		// only after all the possible writer closed, can we shut down the collectCh.
		if collectWorker != nil {
			// you don't need to clear the channel before closing it, so discard all the remain tasks.
			close(collectWorker.collectCh)
		}
		wg2.Wait()
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
	stats              *IndexMergeRuntimeStat
	sc                 sessionctx.Context
	idxID              int
	batchSize          int
	maxBatchSize       int
	maxChunkSize       int
	memTracker         *memory.Tracker
	partitionTableMode bool
	prunedPartitions   []table.PhysicalTable
	byItems            []*plannerutil.ByItems
	scannedKeys        uint64
	pushedLimit        *plannercore.PushedDownLimit
	dagPB              *tipb.DAGRequest
	plan               []base.PhysicalPlan
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

// needPartitionHandle indicates whether we need create a partitionHandle or not.
// If the schema from planner part contains ExtraPidColID or ExtraPhysTblID,
// we need create a partitionHandle, otherwise create a normal handle.
// In TableRowIDScan, the partitionHandle will be used to create key ranges.
func (w *partialIndexWorker) needPartitionHandle() (bool, error) {
	cols := w.plan[0].Schema().Columns
	outputOffsets := w.dagPB.OutputOffsets
	col := cols[outputOffsets[len(outputOffsets)-1]]

	needPartitionHandle := w.partitionTableMode && len(w.byItems) > 0
	hasExtraCol := col.ID == model.ExtraPidColID || col.ID == model.ExtraPhysTblID

	// There will be two needPartitionHandle != hasExtraCol situations.
	// Only `needPartitionHandle` == true and `hasExtraCol` == false are not allowed.
	// `ExtraPhysTblID` will be used in `SelectLock` when `needPartitionHandle` == false and `hasExtraCol` == true.
	if needPartitionHandle && !hasExtraCol {
		return needPartitionHandle, errors.Errorf("Internal error, needPartitionHandle != ret")
	}
	return needPartitionHandle || (col.ID == model.ExtraPidColID), nil
}

func (w *partialIndexWorker) fetchHandles(
	ctx context.Context,
	results []distsql.SelectResult,
	exitCh <-chan struct{},
	fetchCh chan<- *indexMergeTableTask,
	finished <-chan struct{},
	handleCols plannerutil.HandleCols,
	partialPlanIndex int) (count int64, err error) {
	tps := w.getRetTpsForIndexScan(handleCols)
	chk := chunk.NewChunkWithCapacity(tps, w.maxChunkSize)
	for i := 0; i < len(results); {
		start := time.Now()
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, results[i], handleCols)
		if err != nil {
			syncErr(ctx, finished, fetchCh, err)
			return count, err
		}
		if len(handles) == 0 {
			i++
			continue
		}
		count += int64(len(handles))
		task := w.buildTableTask(handles, retChunk, i, partialPlanIndex)
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
	}
	return count, nil
}

func (w *partialIndexWorker) getRetTpsForIndexScan(handleCols plannerutil.HandleCols) []*types.FieldType {
	var tps []*types.FieldType
	if len(w.byItems) != 0 {
		for _, item := range w.byItems {
			tps = append(tps, item.Expr.GetType(w.sc.GetExprCtx().GetEvalCtx()))
		}
	}
	tps = append(tps, handleCols.GetFieldsTypes()...)
	if ok, _ := w.needPartitionHandle(); ok {
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}
	return tps
}

func (w *partialIndexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult, handleCols plannerutil.HandleCols) (
	handles []kv.Handle, retChk *chunk.Chunk, err error) {
	handles = make([]kv.Handle, 0, w.batchSize)
	if len(w.byItems) != 0 {
		retChk = chunk.NewChunkWithCapacity(w.getRetTpsForIndexScan(handleCols), w.batchSize)
	}
	var memUsage int64
	var chunkRowOffset int
	defer w.memTracker.Consume(-memUsage)
	for len(handles) < w.batchSize {
		requiredRows := w.batchSize - len(handles)
		if w.pushedLimit != nil {
			if w.pushedLimit.Offset+w.pushedLimit.Count <= w.scannedKeys {
				return handles, retChk, nil
			}
			requiredRows = min(int(w.pushedLimit.Offset+w.pushedLimit.Count-w.scannedKeys), requiredRows)
		}
		chk.SetRequiredRows(requiredRows, w.maxChunkSize)
		start := time.Now()
		err = errors.Trace(idxResult.Next(ctx, chk))
		if err != nil {
			return nil, nil, err
		}
		if w.stats != nil && w.idxID != 0 {
			w.sc.GetSessionVars().StmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(w.idxID).Record(time.Since(start), chk.NumRows())
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
		for chunkRowOffset = 0; chunkRowOffset < chk.NumRows(); chunkRowOffset++ {
			if w.pushedLimit != nil {
				w.scannedKeys++
				if w.scannedKeys > (w.pushedLimit.Offset + w.pushedLimit.Count) {
					// Skip the handles after Offset+Count.
					break
				}
			}
			var handle kv.Handle
			ok, err1 := w.needPartitionHandle()
			if err1 != nil {
				return nil, nil, err1
			}
			if ok {
				handle, err = handleCols.BuildPartitionHandleFromIndexRow(chk.GetRow(chunkRowOffset))
			} else {
				handle, err = handleCols.BuildHandleFromIndexRow(chk.GetRow(chunkRowOffset))
			}
			if err != nil {
				return nil, nil, err
			}
			handles = append(handles, handle)
		}
		// used for order by
		if len(w.byItems) != 0 {
			retChk.Append(chk, 0, chunkRowOffset)
		}
	}
	w.batchSize *= 2
	if w.batchSize > w.maxBatchSize {
		w.batchSize = w.maxBatchSize
	}
	return handles, retChk, nil
}

func (w *partialIndexWorker) buildTableTask(handles []kv.Handle, retChk *chunk.Chunk, parTblIdx int, partialPlanID int) *indexMergeTableTask {
	task := &indexMergeTableTask{
		lookupTableTask: lookupTableTask{
			handles: handles,
			idxRows: retChk,
		},
		parTblIdx:     parTblIdx,
		partialPlanID: partialPlanID,
	}

	if w.prunedPartitions != nil {
		task.partitionTable = w.prunedPartitions[parTblIdx]
	}

	task.doneCh = make(chan error, 1)
	return task
}

type indexMergeTableScanWorker struct {
	stats          *IndexMergeRuntimeStat
	workCh         <-chan *indexMergeTableTask
	finished       <-chan struct{}
	indexMergeExec *IndexMergeReaderExecutor
	tblPlans       []base.PhysicalPlan

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
		// Otherwise, cannot send error to task.doneCh.
		failpoint.Inject("testIndexMergePanicTableScanWorker", nil)
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

func (*indexMergeTableScanWorker) handleTableScanWorkerPanic(ctx context.Context, finished <-chan struct{}, task **indexMergeTableTask, worker string) func(r any) {
	return func(r any) {
		if r == nil {
			logutil.BgLogger().Debug("worker finish without panic", zap.Any("worker", worker))
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
	if w.indexMergeExec.partitionTableMode && task.partitionTable != nil {
		tbl = task.partitionTable
	}
	tableReader, err := w.indexMergeExec.buildFinalTableReader(ctx, tbl, task.handles)
	if err != nil {
		logutil.Logger(ctx).Error("build table reader failed", zap.Error(err))
		return err
	}
	defer func() { terror.Log(exec.Close(tableReader)) }()
	task.memTracker = w.memTracker
	memUsage := int64(cap(task.handles) * 8)
	task.memUsage = memUsage
	task.memTracker.Consume(memUsage)
	handleCnt := len(task.handles)
	task.rows = make([]chunk.Row, 0, handleCnt)
	for {
		chk := exec.TryNewCacheChunk(tableReader)
		err = exec.Next(ctx, tableReader, chk)
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

	if w.indexMergeExec.keepOrder {
		// Because len(outputOffsets) == tableScan.Schema().Len(),
		// so we could use row.GetInt64(idx) to get partition ID here.
		// TODO: We could add plannercore.PartitionHandleCols to unify them.
		physicalTableIDIdx := -1
		for i, c := range w.indexMergeExec.Schema().Columns {
			if c.ID == model.ExtraPhysTblID {
				physicalTableIDIdx = i
				break
			}
		}
		task.rowIdx = make([]int, 0, len(task.rows))
		for _, row := range task.rows {
			handle, err := w.indexMergeExec.handleCols.BuildHandle(row)
			if err != nil {
				return err
			}
			if w.indexMergeExec.partitionTableMode && physicalTableIDIdx != -1 {
				handle = kv.NewPartitionHandle(row.GetInt64(physicalTableIDIdx), handle)
			}
			rowIdx, _ := task.indexOrder.Get(handle)
			task.rowIdx = append(task.rowIdx, rowIdx.(int))
		}
		sort.Sort(task)
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
		fmt.Fprintf(&buf, " table_task:{num:%d, concurrency:%d, fetch_row:%s, wait_time:%s}", e.TableTaskNum, e.Concurrency, time.Duration(e.FetchRow), time.Duration(e.WaitTime))
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
func (*IndexMergeRuntimeStat) Tp() int {
	return execdetails.TpIndexMergeRunTimeStats
}
