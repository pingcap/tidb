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
	"context"
	"runtime/trace"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
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
	pushedLimit *physicalop.PushedDownLimit
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
			case *physicalop.PhysicalIndexScan:
				e.ranges[i], err = rebuildIndexRanges(e.Ctx().GetExprCtx(), e.Ctx().GetRangerCtx(), x, x.IdxCols, x.IdxColLens)
			case *physicalop.PhysicalTableScan:
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
		_, ok := plan[0].(*physicalop.PhysicalIndexScan)
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
	for i := range e.partialPlans {
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
						syncErr(ctx, e.finished, e.resultCh, errors.New("index merge intersection with keepOrder = true is not supported"))
						return
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
				is := e.partialPlans[workID][0].(*physicalop.PhysicalIndexScan)
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
				if worker.stats != nil && worker.idxID != 0 {
					worker.sc.GetSessionVars().StmtCtx.RuntimeStatsColl.GetBasicRuntimeStats(worker.idxID, true)
				}
				if e.isCorColInPartialFilters[workID] {
					// We got correlated column, so need to refresh Selection operator.
					var err error
					if e.dagPBs[workID].Executors, err = builder.ConstructListBasedDistExec(e.Ctx().GetBuildPBCtx(), e.partialPlans[workID]); err != nil {
						syncErr(ctx, e.finished, fetchCh, err)
						return
					}
				}
				worker.batchSize = CalculateBatchSize(int(is.StatsCount()), e.MaxChunkSize(), worker.maxBatchSize)
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
						SetFromInfoSchema(e.Ctx().GetInfoSchema()).
						SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.Ctx().GetDistSQLCtx(), &builder.Request, e.partialNetDataSizes[workID])).
						SetConnIDAndConnAlias(e.Ctx().GetSessionVars().ConnectionID, e.Ctx().GetSessionVars().SessionAlias)

					if builder.Request.Paging.Enable && builder.Request.Paging.MinPagingSize < uint64(worker.batchSize) {
						// when paging enabled and Paging.MinPagingSize less than initBatchSize, change Paging.MinPagingSize to
						// initial batchSize to avoid redundant paging RPC, see more detail in https://github.com/pingcap/tidb/issues/54066
						builder.Request.Paging.MinPagingSize = uint64(worker.batchSize)
						if builder.Request.Paging.MaxPagingSize < uint64(worker.batchSize) {
							builder.Request.Paging.MaxPagingSize = uint64(worker.batchSize)
						}
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
	ts := e.partialPlans[workID][0].(*physicalop.PhysicalTableScan)

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
					worker.batchSize = min(e.MaxChunkSize(), worker.maxBatchSize)

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

