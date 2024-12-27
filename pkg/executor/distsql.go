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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	isctx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/channel"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/size"
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
func rebuildIndexRanges(ectx expression.BuildContext, rctx *rangerctx.RangerContext, is *plannercore.PhysicalIndexScan, idxCols []*expression.Column, colLens []int) (ranges []*ranger.Range, err error) {
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
	partitions      []table.PhysicalTable
	partRangeMap    map[int64][]*ranger.Range // each partition may have different ranges

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

// TODO: cleanup this method.
func (e *IndexReaderExecutor) buildKeyRanges(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range, physicalID int64) ([]kv.KeyRange, error) {
	var (
		rRanges *kv.KeyRanges
		err     error
	)
	if e.index.ID == -1 {
		rRanges, err = distsql.CommonHandleRangesToKVRanges(dctx, []int64{physicalID}, ranges)
	} else {
		rRanges, err = distsql.IndexRangesToKVRanges(dctx, physicalID, e.index.ID, ranges)
	}
	return rRanges.FirstPartitionRange(), err
}

// Open implements the Executor Open interface.
func (e *IndexReaderExecutor) Open(ctx context.Context) error {
	var err error
	if e.corColInAccess {
		e.ranges, err = rebuildIndexRanges(e.ectx, e.rctx, e.plans[0].(*plannercore.PhysicalIndexScan), e.idxCols, e.colLens)
		if err != nil {
			return err
		}
	}

	var kvRanges []kv.KeyRange
	if len(e.partitions) > 0 {
		for _, p := range e.partitions {
			partRange := e.ranges
			if pRange, ok := e.partRangeMap[p.GetPhysicalID()]; ok {
				partRange = pRange
			}
			kvRange, err := e.buildKeyRanges(e.dctx, partRange, p.GetPhysicalID())
			if err != nil {
				return err
			}
			kvRanges = append(kvRanges, kvRange...)
		}
	} else {
		kvRanges, err = e.buildKeyRanges(e.dctx, e.ranges, e.physicalTableID)
	}
	if err != nil {
		return err
	}

	return e.open(ctx, kvRanges)
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
	// use sortedSelectResults only when byItems pushed down and partition numbers > 1
	if e.byItems == nil || len(e.partitions) <= 1 {
		kvReq, err := e.buildKVReq(kvRanges)
		if err != nil {
			return err
		}
		e.result, err = e.SelectResult(ctx, e.dctx, kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
		if err != nil {
			return err
		}
	} else {
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
	// partitionIDMap are only required by union scan with global index.
	partitionIDMap map[int64]struct{}

	*dataReaderBuilder
	idxNetDataSize float64
	avgRowSize     float64

	// fields about accessing partition tables
	partitionTableMode bool                  // if this executor is accessing a local index with partition table
	prunedPartitions   []table.PhysicalTable // partition tables need to access
	partitionRangeMap  map[int64][]*ranger.Range
	partitionKVRanges  [][]kv.KeyRange // kvRanges of each prunedPartitions

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

	kvRanges      []kv.KeyRange
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
	PushedLimit *plannercore.PushedDownLimit

	stats *IndexLookUpRunTimeStats

	// cancelFunc is called when close the executor
	cancelFunc context.CancelFunc
	workerCtx  context.Context
	pool       *workerPool

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
		e.ranges, err = rebuildIndexRanges(e.ectx, e.rctx, e.idxPlans[0].(*plannercore.PhysicalIndexScan), e.idxCols, e.colLens)
		if err != nil {
			return err
		}
	}

	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.stmtMemTracker)

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

func (e *IndexLookUpExecutor) buildTableKeyRanges() (err error) {
	dctx := e.dctx
	if e.partitionTableMode {
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
			var kvRange *kv.KeyRanges
			if e.index.ID == -1 {
				kvRange, err = distsql.CommonHandleRangesToKVRanges(dctx, []int64{physicalID}, ranges)
			} else {
				kvRange, err = distsql.IndexRangesToKVRangesWithInterruptSignal(dctx, physicalID, e.index.ID, ranges, e.memTracker, nil)
			}
			if err != nil {
				return err
			}
			e.partitionKVRanges = append(e.partitionKVRanges, kvRange.FirstPartitionRange())
		}
	} else {
		physicalID := getPhysicalTableID(e.table)
		var kvRanges *kv.KeyRanges
		if e.index.ID == -1 {
			kvRanges, err = distsql.CommonHandleRangesToKVRanges(dctx, []int64{physicalID}, e.ranges)
		} else {
			kvRanges, err = distsql.IndexRangesToKVRangesWithInterruptSignal(dctx, physicalID, e.index.ID, e.ranges, e.memTracker, nil)
		}
		e.kvRanges = kvRanges.FirstPartitionRange()
	}
	return err
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
		TolerablePendingTasks: 1,
		MaxWorkers:            int32(max(1, e.indexLookupConcurrency)),
	}
	if err := e.startIndexWorker(ctx, initBatchSize); err != nil {
		return err
	}
	e.workerStarted = true
	return nil
}

func (e *IndexLookUpExecutor) needPartitionHandle(tp getHandleType) (bool, error) {
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

	kvRanges := [][]kv.KeyRange{e.kvRanges}
	if e.partitionTableMode {
		kvRanges = e.partitionKVRanges
	}
	// When len(kvrange) = 1, no sorting is required,
	// so remove byItems and non-necessary output columns
	if len(kvRanges) == 1 {
		e.dagPB.OutputOffsets = e.dagPB.OutputOffsets[len(e.byItems):]
		e.byItems = nil
	}
	tps := e.getRetTpsForIndexReader()
	idxID := e.getIndexPlanRootID()
	e.idxWorkerWg.Add(1)
	e.pool.submit(func() {
		defer trace.StartRegion(ctx, "IndexLookUpIndexTask").End()
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

		worker.batchSize = e.calculateBatchSize(initBatchSize, worker.maxBatchSize)
		if builder.Request.Paging.Enable && builder.Request.Paging.MinPagingSize < uint64(worker.batchSize) {
			// when paging enabled and Paging.MinPagingSize less than initBatchSize, change Paging.MinPagingSize to
			// initBatchSize to avoid redundant paging RPC, see more detail in https://github.com/pingcap/tidb/issues/53827
			builder.Request.Paging.MinPagingSize = uint64(worker.batchSize)
			if builder.Request.Paging.MaxPagingSize < uint64(worker.batchSize) {
				builder.Request.Paging.MaxPagingSize = uint64(worker.batchSize)
			}
		}
		results := make([]distsql.SelectResult, 0, len(kvRanges))
		for _, kvRange := range kvRanges {
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
		if len(results) > 1 && len(e.byItems) != 0 {
			// e.Schema() not the output schema for indexReader, and we put byItems related column at first in `buildIndexReq`, so use nil here.
			ssr := distsql.NewSortedSelectResults(e.ectx.GetEvalCtx(), results, nil, e.byItems, e.memTracker)
			results = []distsql.SelectResult{ssr}
		}
		ctx1, cancel := context.WithCancel(ctx)
		// this error is synced in fetchHandles(), don't sync it again
		_ = worker.fetchHandles(ctx1, results)
		cancel()
		for _, result := range results {
			if err := result.Close(); err != nil {
				logutil.Logger(ctx).Error("close Select result failed", zap.Error(err))
			}
		}
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
		logutil.Logger(ctx).Error("build table reader from handles failed", zap.Error(err))
		return nil, err
	}
	return tableReader, nil
}

// Close implements Exec Close interface.
func (e *IndexLookUpExecutor) Close() error {
	if e.stats != nil {
		defer e.stmtRuntimeStatsColl.RegisterStats(e.ID(), e.stats)
	}
	if e.indexUsageReporter != nil {
		e.indexUsageReporter.ReportCopIndexUsageForTable(
			e.table,
			e.index.ID,
			e.idxPlans[0].ID())
	}
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
type indexWorker struct {
	idxLookup *IndexLookUpExecutor
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
}

func (w *indexWorker) syncErr(err error) {
	doneCh := make(chan error, 1)
	doneCh <- err
	w.resultCh <- &lookupTableTask{
		doneCh: doneCh,
	}
}

// fetchHandles fetches a batch of handles from index data and builds the index lookup tasks.
// The tasks are submitted to the pool and processed by tableWorker, and sent to e.resultCh
// at the same time to keep data ordered.
func (w *indexWorker) fetchHandles(ctx context.Context, results []distsql.SelectResult) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("indexWorker in IndexLookupExecutor panicked", zap.Any("recover", r), zap.Stack("stack"))
			err4Panic := util.GetRecoverError(r)
			w.syncErr(err4Panic)
			if err != nil {
				err = errors.Trace(err4Panic)
			}
		}
	}()
	chk := w.idxLookup.AllocPool.Alloc(w.idxLookup.getRetTpsForIndexReader(), w.idxLookup.MaxChunkSize(), w.idxLookup.MaxChunkSize())
	idxID := w.idxLookup.getIndexPlanRootID()
	if w.idxLookup.stmtRuntimeStatsColl != nil {
		if idxID != w.idxLookup.ID() && w.idxLookup.stats != nil {
			w.idxLookup.stats.indexScanBasicStats = w.idxLookup.stmtRuntimeStatsColl.GetBasicRuntimeStats(idxID, true)
		}
	}
	taskID := 0
	for i := 0; i < len(results); {
		result := results[i]
		if w.PushedLimit != nil && w.scannedKeys >= w.PushedLimit.Count+w.PushedLimit.Offset {
			break
		}
		startTime := time.Now()
		handles, retChunk, err := w.extractTaskHandles(ctx, chk, result)
		finishFetch := time.Now()
		if err != nil {
			w.syncErr(err)
			return err
		}
		if len(handles) == 0 {
			i++
			continue
		}
		task := w.buildTableTask(handles, retChunk)
		task.id = taskID
		taskID++
		finishBuild := time.Now()
		if w.idxLookup.partitionTableMode {
			task.partitionTable = w.idxLookup.prunedPartitions[i]
		}
		select {
		case <-ctx.Done():
			return nil
		case <-w.finished:
			return nil
		default:
			e := w.idxLookup
			e.tblWorkerWg.Add(1)
			e.pool.submit(func() {
				defer e.tblWorkerWg.Done()
				select {
				case <-e.finished:
					return
				default:
					execTableTask(e, task)
				}
			})
			w.resultCh <- task
		}
		if w.idxLookup.stats != nil {
			atomic.AddInt64(&w.idxLookup.stats.FetchHandle, int64(finishFetch.Sub(startTime)))
			atomic.AddInt64(&w.idxLookup.stats.TaskWait, int64(time.Since(finishBuild)))
			atomic.AddInt64(&w.idxLookup.stats.FetchHandleTotal, int64(time.Since(startTime)))
		}
	}
	return nil
}

func (w *indexWorker) extractTaskHandles(ctx context.Context, chk *chunk.Chunk, idxResult distsql.SelectResult) (
	handles []kv.Handle, retChk *chunk.Chunk, err error) {
	numColsWithoutPid := chk.NumCols()
	ok, err := w.idxLookup.needPartitionHandle(getHandleFromIndex)
	if err != nil {
		return nil, nil, err
	}
	if ok {
		numColsWithoutPid = numColsWithoutPid - 1
	}
	handleOffset := make([]int, 0, len(w.idxLookup.handleCols))
	for i := range w.idxLookup.handleCols {
		handleOffset = append(handleOffset, numColsWithoutPid-len(w.idxLookup.handleCols)+i)
	}
	if len(handleOffset) == 0 {
		handleOffset = []int{numColsWithoutPid - 1}
	}
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
		if handles == nil {
			handles = make([]kv.Handle, 0, chk.NumRows())
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
	}

	task.doneCh = make(chan error, 1)
	return task
}

func execTableTask(e *IndexLookUpExecutor, task *lookupTableTask) {
	var (
		ctx    = e.workerCtx
		region *trace.Region
	)
	if trace.IsEnabled() {
		region = trace.StartRegion(ctx, "IndexLookUpTableTask"+strconv.Itoa(task.id))
	}
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("TableWorker in IndexLookUpExecutor panicked", zap.Any("recover", r), zap.Stack("stack"))
			err := util.GetRecoverError(r)
			task.doneCh <- err
		}
		if region != nil {
			region.End()
		}
	}()
	tracker := memory.NewTracker(task.id, -1)
	tracker.AttachTo(e.memTracker)
	w := &tableWorker{
		idxLookup:       e,
		finished:        e.finished,
		keepOrder:       e.keepOrder,
		handleIdx:       e.handleIdx,
		checkIndexValue: e.checkIndexValue,
		memTracker:      tracker,
	}
	startTime := time.Now()
	err := w.executeTask(ctx, task)
	if e.stats != nil {
		atomic.AddInt64(&e.stats.TableRowScan, int64(time.Since(startTime)))
		atomic.AddInt64(&e.stats.TableTaskNum, 1)
	}
	task.doneCh <- err
}

// tableWorker is used by IndexLookUpExecutor to maintain table lookup background goroutines.
type tableWorker struct {
	idxLookup *IndexLookUpExecutor
	finished  <-chan struct{}
	keepOrder bool
	handleIdx []int

	// memTracker is used to track the memory usage of this executor.
	memTracker *memory.Tracker

	// checkIndexValue is used to check the consistency of the index data.
	*checkIndexValue
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
		ectx := e.ectx.GetEvalCtx()
		handleEncoded, err = codec.EncodeKey(ectx.Location(), nil, datums...)
		errCtx := ectx.ErrCtx()
		err = errCtx.HandleError(err)
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
	ok, err := e.needPartitionHandle(tp)
	if err != nil {
		return nil, err
	}
	if ok {
		pid := row.GetInt64(row.Len() - 1)
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
	// Record the `Next` call affected wait duration details.
	NextWaitIndexScan        time.Duration
	NextWaitTableLookUpBuild time.Duration
	NextWaitTableLookUpResp  time.Duration
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
	if e.NextWaitIndexScan > 0 || e.NextWaitTableLookUpBuild > 0 || e.NextWaitTableLookUpResp > 0 {
		if buf.Len() > 0 {
			buf.WriteByte(',')
			fmt.Fprintf(&buf, " next: {wait_index: %s, wait_table_lookup_build: %s, wait_table_lookup_resp: %s}",
				execdetails.FormatDuration(e.NextWaitIndexScan),
				execdetails.FormatDuration(e.NextWaitTableLookUpBuild),
				execdetails.FormatDuration(e.NextWaitTableLookUpResp))
		}
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
	e.NextWaitIndexScan += tmp.NextWaitIndexScan
	e.NextWaitTableLookUpBuild += tmp.NextWaitTableLookUpBuild
	e.NextWaitTableLookUpResp += tmp.NextWaitTableLookUpResp
}

// Tp implements the RuntimeStats interface.
func (*IndexLookUpRunTimeStats) Tp() int {
	return execdetails.TpIndexLookUpRunTimeStats
}

func (w *tableWorker) compareData(ctx context.Context, task *lookupTableTask, tableReader exec.Executor) error {
	chk := exec.TryNewCacheChunk(tableReader)
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
				ectx := w.idxLookup.ectx.GetEvalCtx()
				k, _, err := idx.GenIndexKey(ectx.ErrCtx(), ectx.Location(), idxRow.Values[:len(idx.Meta().Columns)], idxRow.Handle, nil)
				if err != nil {
					return nil
				}
				return k
			},
			Tbl:             tblInfo,
			Idx:             w.idxLookup.index,
			EnableRedactLog: w.idxLookup.enableRedactLog,
			Storage:         w.idxLookup.storage,
		}
	}

	for {
		err := exec.Next(ctx, tableReader, chk)
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
			task.indexOrder.Range(func(h kv.Handle, val any) bool {
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
			tc := w.idxLookup.ectx.GetEvalCtx().TypeCtx()
			for i := range vals {
				col := w.idxTblCols[i]
				idxVal := idxRow.GetDatum(i, w.idxColTps[i])
				tablecodec.TruncateIndexValue(&idxVal, w.idxLookup.index.Columns[i], col.ColumnInfo)
				cmpRes, err := tables.CompareIndexAndVal(tc, vals[i], idxVal, collators[i], col.FieldType.IsArray() && vals[i].Kind() == types.KindMysqlJSON)
				if err != nil {
					return ir().ReportAdminCheckInconsistentWithColInfo(ctx,
						handle,
						col.Name.O,
						idxVal,
						vals[i],
						err,
						&consistency.RecordData{Handle: handle, Values: getDatumRow(&idxRow, w.idxColTps)},
					)
				}
				if cmpRes != 0 {
					return ir().ReportAdminCheckInconsistentWithColInfo(ctx,
						handle,
						col.Name.O,
						idxRow.GetDatum(i, w.idxColTps[i]),
						vals[i],
						err,
						&consistency.RecordData{Handle: handle, Values: getDatumRow(&idxRow, w.idxColTps)},
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
	task.buildDoneTime = time.Now()
	if err != nil {
		logutil.Logger(ctx).Error("build table reader failed", zap.Error(err))
		return err
	}
	defer func() { terror.Log(exec.Close(tableReader)) }()

	if w.checkIndexValue != nil {
		return w.compareData(ctx, task, tableReader)
	}

	{
		task.memTracker = w.memTracker
		memUsage := int64(cap(task.handles))*size.SizeOfInterface + tableReader.memUsage()
		for _, h := range task.handles {
			memUsage += int64(h.MemUsage())
		}
		if task.indexOrder != nil {
			memUsage += task.indexOrder.MemUsage()
		}
		if task.duplicatedIndexOrder != nil {
			memUsage += task.duplicatedIndexOrder.MemUsage()
		}
		memUsage += task.idxRows.MemoryUsage()
		task.memUsage = memUsage
		task.memTracker.Consume(memUsage)
	}
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
		{
			memUsage := chk.MemoryUsage()
			task.memUsage += memUsage
			task.memTracker.Consume(memUsage)
		}
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			task.rows = append(task.rows, row)
		}
	}

	defer trace.StartRegion(ctx, "IndexLookUpTableCompute").End()
	{
		memUsage := int64(cap(task.rows)) * int64(unsafe.Sizeof(chunk.Row{}))
		task.memUsage += memUsage
		task.memTracker.Consume(memUsage)
	}
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
		{
			memUsage := int64(cap(task.rowIdx) * int(size.SizeOfInt))
			task.memUsage += memUsage
			task.memTracker.Consume(memUsage)
		}
		sort.Sort(task)
	}

	if handleCnt != len(task.rows) && !util.HasCancelled(ctx) &&
		!w.idxLookup.weakConsistency {
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
				Tbl:             w.idxLookup.table.Meta(),
				Idx:             w.idxLookup.index,
				EnableRedactLog: w.idxLookup.enableRedactLog,
				Storage:         w.idxLookup.storage,
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

func getPhysicalPlanIDs(plans []base.PhysicalPlan) []int {
	planIDs := make([]int, 0, len(plans))
	for _, p := range plans {
		planIDs = append(planIDs, p.ID())
	}
	return planIDs
}
