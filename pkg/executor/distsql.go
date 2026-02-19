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
	"runtime/trace"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/expression"
	isctx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ exec.Executor = &TableReaderExecutor{}
	_ exec.Executor = &IndexReaderExecutor{}
	_ exec.Executor = &IndexLookUpExecutor{}
)

// LookupTableTaskChannelSize represents the channel size of the index double read taskChan.
var LookupTableTaskChannelSize int32 = 50

// lookupTableTask is created from a partial result of an index request which
// contains the handles in those index keys.
type lookupTableTask struct {
	id      int
	handles []kv.Handle
	rowIdx  []int // rowIdx represents the handle index for every row. Only used when keep order.
	rows    []chunk.Row
	idxRows *chunk.Chunk
	cursor  int

	// after the cop task is built, buildDone will be set to the current instant, for Next wait duration statistic.
	buildDoneTime time.Time
	doneCh        chan error

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
// by substituting correlated column with the constant.
func rebuildIndexRanges(ectx expression.BuildContext, rctx *rangerctx.RangerContext, is *physicalop.PhysicalIndexScan, idxCols []*expression.Column, colLens []int) (ranges []*ranger.Range, err error) {
	access := make([]expression.Expression, 0, len(is.AccessCondition))
	for _, cond := range is.AccessCondition {
		newCond, err1 := expression.SubstituteCorCol2Constant(ectx, cond)
		if err1 != nil {
			return nil, err1
		}
		access = append(access, newCond)
	}
	// All of access conditions must be used to build ranges, so we don't limit range memory usage.
	ranges, _, err = ranger.DetachSimpleCondAndBuildRangeForIndex(rctx, access, idxCols, colLens, 0)
	return ranges, err
}

type indexReaderExecutorContext struct {
	rctx       *rangerctx.RangerContext
	dctx       *distsqlctx.DistSQLContext
	ectx       expression.BuildContext
	infoSchema isctx.MetaOnlyInfoSchema
	buildPBCtx *planctx.BuildPBContext

	stmtMemTracker *memory.Tracker
}

func newIndexReaderExecutorContext(sctx sessionctx.Context) indexReaderExecutorContext {
	pctx := sctx.GetPlanCtx()

	return indexReaderExecutorContext{
		rctx:           pctx.GetRangerCtx(),
		dctx:           sctx.GetDistSQLCtx(),
		ectx:           sctx.GetExprCtx(),
		infoSchema:     pctx.GetInfoSchema(),
		buildPBCtx:     pctx.GetBuildPBCtx(),
		stmtMemTracker: sctx.GetSessionVars().StmtCtx.MemTracker,
	}
}

// IndexReaderExecutor sends dag request and reads index data from kv layer.
type IndexReaderExecutor struct {
	indexReaderExecutorContext
	exec.BaseExecutorV2
	indexUsageReporter *exec.IndexUsageReporter

	// For a partitioned table, the IndexReaderExecutor works on a partition, so
	// the type of this table field is actually `table.PhysicalTable`.
	table           table.Table
	index           *model.IndexInfo
	physicalTableID int64
	ranges          []*ranger.Range
	// groupedRanges is from AccessPath.groupedRanges, please see the comment there for more details.
	// In brief, it splits IndexReaderExecutor.ranges into groups. When it's set, we need to access them respectively
	// and use a merge sort to combine them.
	groupedRanges [][]*ranger.Range
	partitions    []table.PhysicalTable
	partRangeMap  map[int64][]*ranger.Range // each partition may have different ranges

	// kvRanges are only used for union scan.
	kvRanges         []kv.KeyRange
	dagPB            *tipb.DAGRequest
	startTS          uint64
	txnScope         string
	readReplicaScope string
	isStaleness      bool
	netDataSize      float64
	// result returns one or more distsql.PartialResult and each PartialResult is returned by one region.
	result distsql.SelectResult
	// columns are only required by union scan.
	columns []*model.ColumnInfo
	// outputColumns are only required by union scan.
	outputColumns []*expression.Column
	// partitionIDMap are only required by union scan with global index.
	partitionIDMap map[int64]struct{}

	paging bool

	keepOrder bool
	desc      bool
	// byItems only for partition table with orderBy + pushedLimit
	byItems []*plannerutil.ByItems

	corColInFilter bool
	corColInAccess bool
	idxCols        []*expression.Column
	colLens        []int
	plans          []base.PhysicalPlan

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
	if e.indexUsageReporter != nil {
		e.indexUsageReporter.ReportCopIndexUsageForTable(e.table, e.index.ID, e.plans[0].ID())
	}

	if e.result != nil {
		err = e.result.Close()
	}
	e.result = nil
	e.kvRanges = e.kvRanges[:0]
	if e.dummy {
		return nil
	}
	return err
}

// Next implements the Executor Next interface.
func (e *IndexReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.dummy {
		req.Reset()
		return nil
	}

	return e.result.Next(ctx, req)
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open(ctx context.Context) error {
	var err error
	if e.corColInAccess {
		is := e.plans[0].(*physicalop.PhysicalIndexScan)
		e.ranges, err = rebuildIndexRanges(e.ectx, e.rctx, is, e.idxCols, e.colLens)
		if err != nil {
			return err
		}
		// Rebuild groupedRanges if it was originally set
		if len(is.GroupByColIdxs) != 0 {
			e.groupedRanges, err = plannercore.GroupRangesByCols(e.ranges, is.GroupByColIdxs)
			if err != nil {
				return err
			}
		}
	}

	// partRangeMap comes from the index join code path, while groupedRanges will not be set in that case.
	// They are two different sources of ranges, and should not appear together.
	intest.Assert(!(len(e.partRangeMap) > 0 && len(e.groupedRanges) > 0), "partRangeMap and groupedRanges should not appear together")

	// Build kvRanges considering both partitions and groupedRanges
	kvRanges, err := e.buildKVRangesForIndexReader()
	if err != nil {
		return err
	}

	return e.open(ctx, kvRanges)
}

// buildKVRangesForIndexReader builds kvRanges for IndexReaderExecutor considering both partitions and groupedRanges.
func (e *IndexReaderExecutor) buildKVRangesForIndexReader() ([]kv.KeyRange, error) {
	tableIDs := make([]int64, 0, len(e.partitions))
	for _, p := range e.partitions {
		tableIDs = append(tableIDs, p.GetPhysicalID())
	}
	if len(e.partitions) == 0 {
		tableIDs = append(tableIDs, e.physicalTableID)
	}

	groupedRanges := e.groupedRanges
	if len(groupedRanges) == 0 {
		groupedRanges = [][]*ranger.Range{e.ranges}
	}

	results := make([]kv.KeyRange, 0, len(groupedRanges))
	for _, ranges := range groupedRanges {
		kvRanges, err := buildKeyRanges(e.dctx, ranges, e.partRangeMap, tableIDs, e.index.ID, nil)
		if err != nil {
			return nil, err
		}
		for _, kvRange := range kvRanges {
			results = append(results, kvRange...)
		}
	}
	return results, nil
}

func (e *IndexReaderExecutor) buildKVReq(r []kv.KeyRange) (*kv.Request, error) {
	var builder distsql.RequestBuilder
	builder.SetKeyRanges(r).
		SetDAGRequest(e.dagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetTxnScope(e.txnScope).
		SetReadReplicaScope(e.readReplicaScope).
		SetIsStaleness(e.isStaleness).
		SetFromSessionVars(e.dctx).
		SetFromInfoSchema(e.infoSchema).
		SetMemTracker(e.memTracker).
		SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.dctx, &builder.Request, e.netDataSize)).
		SetConnIDAndConnAlias(e.dctx.ConnectionID, e.dctx.SessionAlias)
	kvReq, err := builder.Build()
	return kvReq, err
}

func (e *IndexReaderExecutor) open(ctx context.Context, kvRanges []kv.KeyRange) error {
	var err error
	if e.corColInFilter {
		e.dagPB.Executors, err = builder.ConstructListBasedDistExec(e.buildPBCtx, e.plans)
		if err != nil {
			return err
		}
	}

	if e.RuntimeStats() != nil {
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

	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.stmtMemTracker)
	slices.SortFunc(kvRanges, func(i, j kv.KeyRange) int {
		return bytes.Compare(i.StartKey, j.StartKey)
	})
	if !needMergeSort(e.byItems, len(kvRanges)) {
		kvReq, err := e.buildKVReq(kvRanges)
		if err != nil {
			return err
		}
		e.result, err = e.SelectResult(ctx, e.dctx, kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
		if err != nil {
			return err
		}
	} else {
		// Use sortedSelectResults for merge sort
		kvReqs := make([]*kv.Request, 0, len(kvRanges))
		for _, kvRange := range kvRanges {
			kvReq, err := e.buildKVReq([]kv.KeyRange{kvRange})
			if err != nil {
				return err
			}
			kvReqs = append(kvReqs, kvReq)
		}
		var results []distsql.SelectResult
		for _, kvReq := range kvReqs {
			result, err := e.SelectResult(ctx, e.dctx, kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		e.result = distsql.NewSortedSelectResults(e.ectx.GetEvalCtx(), results, e.Schema(), e.byItems, e.memTracker)
	}
	return nil
}

type indexLookUpExecutorContext struct {
	tableReaderExecutorContext

	stmtRuntimeStatsColl *execdetails.RuntimeStatsColl

	indexLookupSize        int
	indexLookupConcurrency int
	enableRedactLog        string
	storage                kv.Storage
	weakConsistency        bool
}

func newIndexLookUpExecutorContext(sctx sessionctx.Context) indexLookUpExecutorContext {
	return indexLookUpExecutorContext{
		tableReaderExecutorContext: newTableReaderExecutorContext(sctx),

		stmtRuntimeStatsColl: sctx.GetSessionVars().StmtCtx.RuntimeStatsColl,

		indexLookupSize:        sctx.GetSessionVars().IndexLookupSize,
		indexLookupConcurrency: sctx.GetSessionVars().IndexLookupConcurrency(),
		enableRedactLog:        sctx.GetSessionVars().EnableRedactLog,
		storage:                sctx.GetStore(),
		weakConsistency:        sctx.GetSessionVars().StmtCtx.WeakConsistency,
	}
}

// IndexLookUpExecutor implements double read for index scan.
type IndexLookUpExecutor struct {
	indexLookUpExecutorContext
	exec.BaseExecutorV2
	indexUsageReporter *exec.IndexUsageReporter

	table  table.Table
	index  *model.IndexInfo
	ranges []*ranger.Range
	// groupedRanges is from AccessPath.groupedRanges, please see the comment there for more details.
	// In brief, it splits IndexLookUpExecutor.ranges into groups. When it's set, we need to access them respectively
	// and use a merge sort to combine them.
	groupedRanges [][]*ranger.Range
	dagPB         *tipb.DAGRequest
	startTS       uint64
	// handleIdx is the index of handle, which is only used for case of keeping order.
	handleIdx       []int
	handleCols      []*expression.Column
	primaryKeyIndex *model.IndexInfo
	tableRequest    *tipb.DAGRequest

	// columns are only required by union scan.
	columns []*model.ColumnInfo
	// partitionIDMap are only required by union scan with global index.
	partitionIDMap map[int64]struct{}

	*dataReaderBuilder
	idxNetDataSize float64
	avgRowSize     float64

	// fields about accessing partition tables
	partitionTableMode bool                  // if this executor is accessing a local index with partition table
	prunedPartitions   []table.PhysicalTable // partition tables need to access
	partitionRangeMap  map[int64][]*ranger.Range

	// All fields above are immutable.

	idxWorkerWg *sync.WaitGroup
	tblWorkerWg *sync.WaitGroup
	finished    chan struct{}

	resultCh   chan *lookupTableTask
	resultCurr *lookupTableTask

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// checkIndexValue is used to check the consistency of the index data.
	*checkIndexValue

	// groupedKVRanges is built from ranger.Range and needed to access tikv. It's a unified form that considers both
	// ranges and groupedRanges, and also considers the partitioned table.
	// The extra PhysicalTableID is needed by the memIndexLookUpReader because it can't get it from PartitionHandle like
	// the IndexLookUpExecutor here.
	groupedKVRanges []*kvRangesWithPhysicalTblID

	workerStarted bool

	byItems   []*plannerutil.ByItems
	keepOrder bool
	desc      bool

	indexPaging bool

	corColInIdxSide bool
	corColInTblSide bool
	corColInAccess  bool
	idxPlans        []base.PhysicalPlan
	tblPlans        []base.PhysicalPlan
	idxCols         []*expression.Column
	colLens         []int
	// PushedLimit is used to skip the preceding and tailing handles when Limit is sunk into IndexLookUpReader.
	PushedLimit *physicalop.PushedDownLimit

	stats *IndexLookUpRunTimeStats

	// cancelFunc is called when close the executor
	cancelFunc context.CancelFunc
	workerCtx  context.Context
	pool       *workerPool

	// If dummy flag is set, this is not a real IndexLookUpReader, it just provides the KV ranges for UnionScan.
	// Used by the temporary table, cached table.
	dummy bool

	// Whether to push down the index lookup to TiKV
	indexLookUpPushDown bool
}

type kvRangesWithPhysicalTblID struct {
	PhysicalTableID int64
	KeyRanges       []kv.KeyRange
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
		is := e.idxPlans[0].(*physicalop.PhysicalIndexScan)
		e.ranges, err = rebuildIndexRanges(e.ectx, e.rctx, is, e.idxCols, e.colLens)
		if err != nil {
			return err
		}
		// Rebuild groupedRanges if it was originally set
		if len(is.GroupByColIdxs) != 0 {
			e.groupedRanges, err = plannercore.GroupRangesByCols(e.ranges, is.GroupByColIdxs)
			if err != nil {
				return err
			}
		}
	}

	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.stmtMemTracker)

	// partitionRangeMap comes from the index join code path, while groupedRanges will not be set in that case.
	// They are two different sources of ranges, and should not appear together.
	intest.Assert(!(len(e.partitionRangeMap) > 0 && len(e.groupedRanges) > 0),
		"partitionRangeMap and groupedRanges should not appear together")

	err = e.buildTableKeyRanges()
	if err != nil {
		return err
	}

	// Treat temporary table as dummy table, avoid sending distsql request to TiKV.
	if e.dummy {
		return nil
	}

	return e.open(ctx)
}

func buildKeyRanges(dctx *distsqlctx.DistSQLContext,
	ranges []*ranger.Range,
	rangeOverrideForPartitionID map[int64][]*ranger.Range,
	physicalIDs []int64,
	indexID int64,
	memTracker *memory.Tracker,
) ([][]kv.KeyRange, error) {
	results := make([][]kv.KeyRange, 0, len(physicalIDs))
	for _, physicalID := range physicalIDs {
		if pRange, ok := rangeOverrideForPartitionID[physicalID]; ok {
			ranges = pRange
		}
		if indexID == -1 {
			rRanges, err := distsql.CommonHandleRangesToKVRanges(dctx, []int64{physicalID}, ranges)
			if err != nil {
				return nil, err
			}
			results = append(results, rRanges.FirstPartitionRange())
		} else {
			singleRanges, err := distsql.IndexRangesToKVRangesWithInterruptSignal(dctx, physicalID, indexID, ranges, memTracker, nil)
			if err != nil {
				return nil, err
			}
			results = append(results, singleRanges.FirstPartitionRange())
		}
	}
	return results, nil
}

func (e *IndexLookUpExecutor) buildTableKeyRanges() (err error) {
	tableIDs := make([]int64, 0, len(e.prunedPartitions))
	if e.partitionTableMode {
		for _, p := range e.prunedPartitions {
			tableIDs = append(tableIDs, p.GetPhysicalID())
		}
	} else {
		tableIDs = append(tableIDs, getPhysicalTableID(e.table))
	}

	groupedRanges := e.groupedRanges
	if len(groupedRanges) == 0 {
		groupedRanges = [][]*ranger.Range{e.ranges}
	}

	kvRanges := make([][]kv.KeyRange, 0, len(groupedRanges))
	physicalTblIDsForPartitionKVRanges := make([]int64, 0, len(tableIDs)*len(groupedRanges))
	for _, ranges := range groupedRanges {
		kvRange, err := buildKeyRanges(e.dctx, ranges, e.partitionRangeMap, tableIDs, e.index.ID, e.memTracker)
		if err != nil {
			return err
		}
		kvRanges = append(kvRanges, kvRange...)
		physicalTblIDsForPartitionKVRanges = append(physicalTblIDsForPartitionKVRanges, tableIDs...)
	}

	if len(kvRanges) > 1 {
		// If there are more than one kv ranges, it must come from the partitioned table, or groupedRanges, or both.
		intest.Assert(e.partitionTableMode || len(e.groupedRanges) > 0)
	}
	e.groupedKVRanges = make([]*kvRangesWithPhysicalTblID, 0, len(kvRanges))
	for i, kvRange := range kvRanges {
		partitionKVRange := &kvRangesWithPhysicalTblID{
			PhysicalTableID: physicalTblIDsForPartitionKVRanges[i],
			KeyRanges:       kvRange,
		}
		e.groupedKVRanges = append(e.groupedKVRanges, partitionKVRange)
	}

	return nil
}

func (e *IndexLookUpExecutor) open(_ context.Context) error {
	// We have to initialize "memTracker" and other execution resources in here
	// instead of in function "Open", because this "IndexLookUpExecutor" may be
	// constructed by a "IndexLookUpJoin" and "Open" will not be called in that
	// situation.
	e.initRuntimeStats()
	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.stmtMemTracker)

	e.finished = make(chan struct{})
	e.resultCh = make(chan *lookupTableTask, atomic.LoadInt32(&LookupTableTaskChannelSize))

	var err error
	if e.corColInIdxSide {
		e.dagPB.Executors, err = builder.ConstructListBasedDistExec(e.buildPBCtx, e.idxPlans)
		if err != nil {
			return err
		}
	}

	if e.corColInTblSide {
		e.tableRequest.Executors, err = builder.ConstructListBasedDistExec(e.buildPBCtx, e.tblPlans)
		if err != nil {
			return err
		}
	}

	e.idxWorkerWg = &sync.WaitGroup{}
	e.tblWorkerWg = &sync.WaitGroup{}
	return nil
}

func (e *IndexLookUpExecutor) startWorkers(ctx context.Context, initBatchSize int) error {
	// indexWorker will submit lookup-table tasks (processed by tableWorker) to the pool,
	// so fetching index and getting table data can run concurrently.
	e.workerCtx, e.cancelFunc = context.WithCancel(ctx)
	e.pool = &workerPool{
		needSpawn: func(workers, tasks uint32) bool {
			return workers < uint32(e.indexLookupConcurrency) && tasks > 1
		},
	}
	if err := e.startIndexWorker(ctx, initBatchSize); err != nil {
		return err
	}
	e.workerStarted = true
	return nil
}

func (e *IndexLookUpExecutor) needPartitionHandle(tp getHandleType) (bool, error) {
	if e.indexLookUpPushDown {
		// For index lookup push down, needPartitionHandle should always return false because
		// global index or keep order for partition table is not supported now.
		intest.Assert(!e.index.Global && !e.keepOrder)
		return false, nil
	}

	var col *expression.Column
	var needPartitionHandle bool
	if tp == getHandleFromIndex {
		cols := e.idxPlans[0].Schema().Columns
		outputOffsets := e.dagPB.OutputOffsets
		col = cols[outputOffsets[len(outputOffsets)-1]]
		// For indexScan, need partitionHandle when global index or keepOrder with partitionTable
		needPartitionHandle = e.index.Global || e.partitionTableMode && e.keepOrder
	} else {
		cols := e.tblPlans[0].Schema().Columns
		outputOffsets := e.tableRequest.OutputOffsets
		col = cols[outputOffsets[len(outputOffsets)-1]]

		// For TableScan, need partitionHandle in `indexOrder` when e.keepOrder == true or execute `admin check [table|index]` with global index
		needPartitionHandle = ((e.index.Global || e.partitionTableMode) && e.keepOrder) || (e.index.Global && e.checkIndexValue != nil)
	}
	hasExtraCol := col.ID == model.ExtraPhysTblID

	// There will be two needPartitionHandle != hasExtraCol situations.
	// Only `needPartitionHandle` == true and `hasExtraCol` == false are not allowed.
	// `ExtraPhysTblID` will be used in `SelectLock` when `needPartitionHandle` == false and `hasExtraCol` == true.
	if needPartitionHandle && !hasExtraCol {
		return needPartitionHandle, errors.Errorf("Internal error, needPartitionHandle != ret, tp(%d)", tp)
	}
	return needPartitionHandle, nil
}

func (e *IndexLookUpExecutor) isCommonHandle() bool {
	return !(len(e.handleCols) == 1 && e.handleCols[0].ID == model.ExtraHandleID) && e.table.Meta() != nil && e.table.Meta().IsCommonHandle
}

func (e *IndexLookUpExecutor) getRetTpsForIndexReader() []*types.FieldType {
	if e.checkIndexValue != nil {
		return e.idxColTps
	}
	var tps []*types.FieldType
	if len(e.byItems) != 0 {
		for _, item := range e.byItems {
			tps = append(tps, item.Expr.GetType(e.ectx.GetEvalCtx()))
		}
	}
	if e.isCommonHandle() {
		for _, handleCol := range e.handleCols {
			tps = append(tps, handleCol.RetType)
		}
	} else {
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}
	if ok, _ := e.needPartitionHandle(getHandleFromIndex); ok {
		tps = append(tps, types.NewFieldType(mysql.TypeLonglong))
	}
	return tps
}

// startIndexWorker launch a background goroutine to fetch handles, submit lookup-table tasks to the pool.
func (e *IndexLookUpExecutor) startIndexWorker(ctx context.Context, initBatchSize int) error {
	if e.RuntimeStats() != nil {
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}
	tracker := memory.NewTracker(memory.LabelForIndexWorker, -1)
	tracker.AttachTo(e.memTracker)

	kvRanges := make([][]kv.KeyRange, 0, len(e.groupedKVRanges))
	for _, ranges := range e.groupedKVRanges {
		kvRanges = append(kvRanges, ranges.KeyRanges)
	}

	// When len(kvrange) = 1, no sorting is required,
	// so remove byItems and non-necessary output columns
	if len(kvRanges) == 1 {
		e.dagPB.OutputOffsets = e.dagPB.OutputOffsets[len(e.byItems):]
		e.byItems = nil
	}
	var tps []*types.FieldType
	tblScanIdxForRewritePartitionID := -1
	if e.indexLookUpPushDown {
		tps = e.RetFieldTypes()
		if e.partitionTableMode {
			for idx, executor := range e.dagPB.Executors {
				if executor.Tp == tipb.ExecType_TypeTableScan {
					tblScanIdxForRewritePartitionID = idx
					break
				}
			}
			if tblScanIdxForRewritePartitionID < 0 {
				intest.Assert(false)
				return errors.New("cannot find table scan executor in for partition index lookup push down")
			}
		}
	} else {
		tps = e.getRetTpsForIndexReader()
	}
	idxID := e.getIndexPlanRootID()
	e.idxWorkerWg.Add(1)
	e.pool.submit(func() {
		defer trace.StartRegion(ctx, "IndexLookUpIndexTask").End()
		growWorkerStack16K()
		worker := &indexWorker{
			idxLookup:       e,
			finished:        e.finished,
			resultCh:        e.resultCh,
			keepOrder:       e.keepOrder,
			checkIndexValue: e.checkIndexValue,
			maxBatchSize:    e.indexLookupSize,
			maxChunkSize:    e.MaxChunkSize(),
			PushedLimit:     e.PushedLimit,
		}
		worker.batchSize = e.calculateBatchSize(initBatchSize, worker.maxBatchSize)

		results := make([]distsql.SelectResult, 0, len(kvRanges))
		for idx, kvRange := range kvRanges {
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

			if tblScanIdxForRewritePartitionID >= 0 {
				// We should set the TblScan's TableID to the partition physical ID to make sure
				// the push-down index lookup can encode the table handle key correctly.
				e.dagPB.Executors[tblScanIdxForRewritePartitionID].TblScan.TableId = e.prunedPartitions[idx].GetPhysicalID()
			}

			var builder distsql.RequestBuilder
			builder.SetDAGRequest(e.dagPB).
				SetStartTS(e.startTS).
				SetDesc(e.desc).
				SetKeepOrder(e.keepOrder).
				SetTxnScope(e.txnScope).
				SetReadReplicaScope(e.readReplicaScope).
				SetIsStaleness(e.isStaleness).
				SetFromSessionVars(e.dctx).
				SetFromInfoSchema(e.infoSchema).
				SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.dctx, &builder.Request, e.idxNetDataSize/float64(len(kvRanges)))).
				SetMemTracker(tracker).
				SetConnIDAndConnAlias(e.dctx.ConnectionID, e.dctx.SessionAlias)

			if e.indexLookUpPushDown {
				// Paging and Cop-cache is not supported in index lookup push down.
				builder.Request.Paging.Enable = false
				builder.Request.Cacheable = false
			}

			if builder.Request.Paging.Enable && builder.Request.Paging.MinPagingSize < uint64(worker.batchSize) {
				// when paging enabled and Paging.MinPagingSize less than initBatchSize, change Paging.MinPagingSize to
				// initBatchSize to avoid redundant paging RPC, see more detail in https://github.com/pingcap/tidb/issues/53827
				builder.Request.Paging.MinPagingSize = uint64(worker.batchSize)
				if builder.Request.Paging.MaxPagingSize < uint64(worker.batchSize) {
					builder.Request.Paging.MaxPagingSize = uint64(worker.batchSize)
				}
			}

			// init kvReq, result and worker for this partition
			// The key ranges should be ordered.
			slices.SortFunc(kvRange, func(i, j kv.KeyRange) int {
				return bytes.Compare(i.StartKey, j.StartKey)
			})
			kvReq, err := builder.SetKeyRanges(kvRange).Build()
			if err != nil {
				worker.syncErr(err)
				break
			}
			result, err := distsql.SelectWithRuntimeStats(ctx, e.dctx, kvReq, tps, getPhysicalPlanIDs(e.idxPlans), idxID)
			if err != nil {
				worker.syncErr(err)
				break
			}
			results = append(results, result)
		}
		if needMergeSort(e.byItems, len(results)) {
			// e.Schema() not the output schema for indexReader, and we put byItems related column at first in `buildIndexReq`, so use nil here.
			ssr := distsql.NewSortedSelectResults(e.ectx.GetEvalCtx(), results, nil, e.byItems, e.memTracker)
			results = []distsql.SelectResult{ssr}
		}
		ctx1, cancel := context.WithCancel(ctx)
		// this error is synced in fetchHandles(), don't sync it again
		var selResultList selectResultList
		indexTypes := e.getRetTpsForIndexReader()
		if e.indexLookUpPushDown {
			var err error
			if selResultList, err = newSelectResultRowIterList(results, [][]*types.FieldType{indexTypes}); err != nil {
				cancel()
				worker.syncErr(err)
				return
			}
		} else {
			selResultList = newSelectResultList(results)
		}
		_ = worker.fetchHandles(ctx1, selResultList, indexTypes)
		cancel()
		selResultList.Close()
		close(e.resultCh)
		e.idxWorkerWg.Done()
	})
	return nil
}

// calculateBatchSize calculates a suitable initial batch size.
func (e *IndexLookUpExecutor) calculateBatchSize(initBatchSize, maxBatchSize int) int {
	if e.indexPaging {
		// If indexPaging is true means this query has limit, so use initBatchSize to avoid scan some unnecessary data.
		return min(initBatchSize, maxBatchSize)
	}
	var estRows int
	if len(e.idxPlans) > 0 {
		estRows = int(e.idxPlans[0].StatsCount())
	}
	return CalculateBatchSize(estRows, initBatchSize, maxBatchSize)
}

// CalculateBatchSize calculates a suitable initial batch size. It exports for testing.
func CalculateBatchSize(estRows, initBatchSize, maxBatchSize int) int {
	batchSize := min(initBatchSize, maxBatchSize)
	if estRows >= maxBatchSize {
		return maxBatchSize
	}
	for batchSize < estRows {
		// If batchSize less than estRows, increase batch size to avoid unnecessary rpc.
		batchSize = batchSize * 2
		if batchSize >= maxBatchSize {
			return maxBatchSize
		}
	}
	return batchSize
}

func (e *IndexLookUpExecutor) buildTableReader(ctx context.Context, task *lookupTableTask) (*TableReaderExecutor, error) {
	table := e.table
	if e.partitionTableMode && task.partitionTable != nil {
		table = task.partitionTable
	}
	tableReaderExec := &TableReaderExecutor{
		BaseExecutorV2:             e.BuildNewBaseExecutorV2(e.stmtRuntimeStatsColl, e.Schema(), e.getTableRootPlanID()),
		tableReaderExecutorContext: e.tableReaderExecutorContext,
		table:                      table,
		dagPB:                      e.tableRequest,
		startTS:                    e.startTS,
		txnScope:                   e.txnScope,
		readReplicaScope:           e.readReplicaScope,
		isStaleness:                e.isStaleness,
		columns:                    e.columns,
		corColInFilter:             e.corColInTblSide,
		plans:                      e.tblPlans,
		netDataSize:                e.avgRowSize * float64(len(task.handles)),
		byItems:                    e.byItems,
	}
	tableReaderExec.buildVirtualColumnInfo()
	tableReader, err := e.dataReaderBuilder.buildTableReaderFromHandles(ctx, tableReaderExec, task.handles, true)
	if err != nil {
		if ctx.Err() != context.Canceled {
			logutil.Logger(ctx).Error("build table reader from handles failed", zap.Error(err))
		}
		return nil, err
	}
	return tableReader, nil
}

// Close implements Exec Close interface.
func (e *IndexLookUpExecutor) Close() error {
	if e.stats != nil {
		defer func() {
			e.stmtRuntimeStatsColl.RegisterStats(e.ID(), e.stats)
			indexScanCopTasks, _ := e.stmtRuntimeStatsColl.GetCopCountAndRows(e.getIndexPlanRootID())
			if e.indexLookUpPushDown {
				metrics.IndexLookUpExecutorWithPushDownEnabledRowNumber.Observe(float64(e.stats.indexScanBasicStats.GetActRows()))
				metrics.IndexLookUpIndexScanCopTasksWithPushDownEnabled.Add(float64(indexScanCopTasks))
			} else {
				metrics.IndexLookUpIndexScanCopTasksNormal.Add(float64(indexScanCopTasks))
			}
		}()
	}

	if stats := e.RuntimeStats(); stats != nil {
		if e.indexLookUpPushDown {
			defer func() {
				metrics.IndexLookUpExecutorWithPushDownEnabledDuration.Observe(time.Duration(stats.GetTime()).Seconds())
			}()
		}
	}

	if e.indexUsageReporter != nil {
		e.indexUsageReporter.ReportCopIndexUsageForTable(
			e.table,
			e.index.ID,
			e.idxPlans[0].ID())
	}
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
	channel.Clear(e.resultCh)
	e.idxWorkerWg.Wait()
	e.tblWorkerWg.Wait()
	e.finished = nil
	e.workerStarted = false
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
			numToAppend := min(len(resultTask.rows)-resultTask.cursor, req.RequiredRows()-req.NumRows())
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
	var (
		enableStats         = e.stats != nil
		start               time.Time
		indexFetchedInstant time.Time
	)
	if enableStats {
		start = time.Now()
	}
	task, ok := <-e.resultCh
	if !ok {
		return nil, nil
	}
	if enableStats {
		indexFetchedInstant = time.Now()
	}
	if err := <-task.doneCh; err != nil {
		return nil, err
	}
	if enableStats {
		e.stats.NextWaitIndexScan += indexFetchedInstant.Sub(start)
		if task.buildDoneTime.After(indexFetchedInstant) {
			e.stats.NextWaitTableLookUpBuild += task.buildDoneTime.Sub(indexFetchedInstant)
			indexFetchedInstant = task.buildDoneTime
		}
		e.stats.NextWaitTableLookUpResp += time.Since(indexFetchedInstant)
	}

	// Release the memory usage of last task before we handle a new task.
	if e.resultCurr != nil {
		e.resultCurr.memTracker.Consume(-e.resultCurr.memUsage)
	}
	e.resultCurr = task
	return e.resultCurr, nil
}

func (e *IndexLookUpExecutor) initRuntimeStats() {
	if e.RuntimeStats() != nil {
		e.stats = &IndexLookUpRunTimeStats{
			indexScanBasicStats: &execdetails.BasicRuntimeStats{},
			Concurrency:         e.indexLookupConcurrency,
		}
	}
}

func (e *IndexLookUpExecutor) getIndexPlanRootID() int {
	if len(e.idxPlans) > 0 {
		return e.idxPlans[len(e.idxPlans)-1].ID()
	}
	return e.ID()
}

func (e *IndexLookUpExecutor) getTableRootPlanID() int {
	if len(e.tblPlans) > 0 {
		return e.tblPlans[len(e.tblPlans)-1].ID()
	}
	return e.ID()
}

// indexWorker is used by IndexLookUpExecutor to maintain index lookup background goroutines.


func getPhysicalPlanIDs(plans []base.PhysicalPlan) []int {
	planIDs := make([]int, 0, len(plans))
	for _, p := range plans {
		planIDs = append(planIDs, p.ID())
	}
	return planIDs
}

func needMergeSort(byItems []*plannerutil.ByItems, kvRangesCount int) bool {
	return len(byItems) > 0 && kvRangesCount > 1
}
