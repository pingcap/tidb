// Copyright 2018 PingCAP, Inc.
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
	"slices"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	internalutil "github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	isctx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ranger"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
)

// make sure `TableReaderExecutor` implements `Executor`.
var _ exec.Executor = &TableReaderExecutor{}

// selectResultHook is used to hack distsql.SelectWithRuntimeStats safely for testing.
type selectResultHook struct {
	selectResultFunc func(ctx context.Context, dctx *distsqlctx.DistSQLContext, kvReq *kv.Request,
		fieldTypes []*types.FieldType, copPlanIDs []int) (distsql.SelectResult, error)
}

func (sr selectResultHook) SelectResult(ctx context.Context, dctx *distsqlctx.DistSQLContext, kvReq *kv.Request,
	fieldTypes []*types.FieldType, copPlanIDs []int, rootPlanID int) (distsql.SelectResult, error) {
	if sr.selectResultFunc == nil {
		return distsql.SelectWithRuntimeStats(ctx, dctx, kvReq, fieldTypes, copPlanIDs, rootPlanID)
	}
	return sr.selectResultFunc(ctx, dctx, kvReq, fieldTypes, copPlanIDs)
}

type kvRangeBuilder interface {
	buildKeyRange(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range) ([][]kv.KeyRange, error)
	buildKeyRangeSeparately(dctx *distsqlctx.DistSQLContext, ranges []*ranger.Range) ([]int64, [][]kv.KeyRange, error)
}

// tableReaderExecutorContext is the execution context for the `TableReaderExecutor`
type tableReaderExecutorContext struct {
	dctx       *distsqlctx.DistSQLContext
	rctx       *rangerctx.RangerContext
	buildPBCtx *planctx.BuildPBContext
	ectx       exprctx.BuildContext

	stmtMemTracker *memory.Tracker

	infoSchema  isctx.MetaOnlyInfoSchema
	getDDLOwner func(context.Context) (*serverinfo.ServerInfo, error)
}

func (treCtx *tableReaderExecutorContext) GetInfoSchema() isctx.MetaOnlyInfoSchema {
	return treCtx.infoSchema
}

func (treCtx *tableReaderExecutorContext) GetDDLOwner(ctx context.Context) (*serverinfo.ServerInfo, error) {
	if treCtx.getDDLOwner != nil {
		return treCtx.getDDLOwner(ctx)
	}

	return nil, errors.New("GetDDLOwner in a context without DDL")
}

func newTableReaderExecutorContext(sctx sessionctx.Context) tableReaderExecutorContext {
	// Explicitly get `ownerManager` out of the closure to show that the `tableReaderExecutorContext` itself doesn't
	// depend on `sctx` directly.
	// The context of some tests don't have `DDL`, so make it optional
	var getDDLOwner func(ctx context.Context) (*serverinfo.ServerInfo, error)
	dom := domain.GetDomain(sctx)
	if dom != nil && dom.DDL() != nil {
		ddl := dom.DDL()
		ownerManager := ddl.OwnerManager()
		getDDLOwner = func(ctx context.Context) (*serverinfo.ServerInfo, error) {
			ddlOwnerID, err := ownerManager.GetOwnerID(ctx)
			if err != nil {
				return nil, err
			}
			return infosync.GetServerInfoByID(ctx, ddlOwnerID)
		}
	}

	pctx := sctx.GetPlanCtx()
	return tableReaderExecutorContext{
		dctx:           sctx.GetDistSQLCtx(),
		rctx:           pctx.GetRangerCtx(),
		buildPBCtx:     pctx.GetBuildPBCtx(),
		ectx:           sctx.GetExprCtx(),
		stmtMemTracker: sctx.GetSessionVars().StmtCtx.MemTracker,
		infoSchema:     pctx.GetInfoSchema(),
		getDDLOwner:    getDDLOwner,
	}
}

// TableReaderExecutor sends DAG request and reads table data from kv layer.
type TableReaderExecutor struct {
	tableReaderExecutorContext
	exec.BaseExecutorV2
	indexUsageReporter *exec.IndexUsageReporter

	table table.Table

	// The source of key ranges varies from case to case.
	// It may be calculated from PhysicalPlan by executorBuilder, or calculated from argument by dataBuilder;
	// It may be calculated from ranger.Ranger, or calculated from handles.
	// The table ID may also change because of the partition table, and causes the key range to change.
	// So instead of keeping a `range` struct field, it's better to define a interface.
	kvRangeBuilder
	// TODO: remove this field, use the kvRangeBuilder interface.
	ranges []*ranger.Range

	// groupedRanges and groupByColIdxs are from AccessPath.groupedRanges, please see the comment there for more details
	// In brief, it splits TableReaderExecutor.ranges into groups. When it's set, we need to access them respectively
	// and use a merge sort to combine them.

	groupedRanges  [][]*ranger.Range
	groupByColIdxs []int

	// kvRanges are only use for union scan.
	kvRanges         []kv.KeyRange
	dagPB            *tipb.DAGRequest
	startTS          uint64
	txnScope         string
	readReplicaScope string
	isStaleness      bool
	// FIXME: in some cases the data size can be more accurate after get the handles count,
	// but we keep things simple as it needn't to be that accurate for now.
	netDataSize float64
	// columns are only required by union scan and virtual column.
	columns []*model.ColumnInfo

	// resultHandler handles the order of the result. Since (MAXInt64, MAXUint64] stores before [0, MaxInt64] physically
	// for unsigned int.
	resultHandler *tableResultHandler
	plans         []base.PhysicalPlan
	tablePlan     base.PhysicalPlan

	memTracker       *memory.Tracker
	selectResultHook // for testing

	keepOrder bool
	desc      bool
	// byItems only for partition table with orderBy + pushedLimit
	byItems   []*util.ByItems
	paging    bool
	storeType kv.StoreType
	// corColInFilter tells whether there's correlated column in filter (both conditions in PhysicalSelection and LateMaterializationFilterCondition in PhysicalTableScan)
	// If true, we will need to revise the dagPB (fill correlated column value in filter) each time call Open().
	corColInFilter bool
	// corColInAccess tells whether there's correlated column in access conditions.
	corColInAccess bool
	// virtualColumnIndex records all the indices of virtual columns and sort them in definition
	// to make sure we can compute the virtual column in right order.
	virtualColumnIndex []int
	// virtualColumnRetFieldTypes records the RetFieldTypes of virtual columns.
	virtualColumnRetFieldTypes []*types.FieldType
	// batchCop indicates whether use super batch coprocessor request, only works for TiFlash engine.
	batchCop bool

	// If dummy flag is set, this is not a real TableReader, it just provides the KV ranges for UnionScan.
	// Used by the temporary table, cached table.
	dummy bool
}

// Table implements the dataSourceExecutor interface.
func (e *TableReaderExecutor) Table() table.Table {
	return e.table
}

func (e *TableReaderExecutor) setDummy() {
	e.dummy = true
}

func (e *TableReaderExecutor) memUsage() int64 {
	const sizeofTableReaderExecutor = int64(unsafe.Sizeof(*e))

	res := sizeofTableReaderExecutor
	res += size.SizeOfPointer * int64(cap(e.ranges))
	for _, v := range e.ranges {
		res += v.MemUsage()
	}
	res += kv.KeyRangeSliceMemUsage(e.kvRanges)
	res += int64(e.dagPB.Size())
	// TODO: add more statistics
	return res
}

// Open initializes necessary variables for using this executor.
func (e *TableReaderExecutor) Open(ctx context.Context) error {
	r, ctx := tracing.StartRegionEx(ctx, "TableReaderExecutor.Open")
	defer r.End()
	failpoint.Inject("mockSleepInTableReaderNext", func(v failpoint.Value) {
		ms := v.(int)
		time.Sleep(time.Millisecond * time.Duration(ms))
	})

	if e.memTracker != nil {
		e.memTracker.Reset()
	} else {
		e.memTracker = memory.NewTracker(e.ID(), -1)
	}
	e.memTracker.AttachTo(e.stmtMemTracker)

	var err error
	if e.corColInFilter {
		// If there's correlated column in filter, need to rewrite dagPB
		if e.storeType == kv.TiFlash {
			execs, err := builder.ConstructTreeBasedDistExec(e.buildPBCtx, e.tablePlan)
			if err != nil {
				return err
			}
			e.dagPB.RootExecutor = execs[0]
		} else {
			e.dagPB.Executors, err = builder.ConstructListBasedDistExec(e.buildPBCtx, e.plans)
			if err != nil {
				return err
			}
		}
	}
	if e.dctx.RuntimeStatsColl != nil {
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}
	if e.corColInAccess {
		ts := e.plans[0].(*physicalop.PhysicalTableScan)
		e.ranges, err = ts.ResolveCorrelatedColumns()
		if err != nil {
			return err
		}
		// Rebuild groupedRanges if it was originally set
		if len(e.groupByColIdxs) != 0 {
			e.groupedRanges, err = plannercore.GroupRangesByCols(e.ranges, e.groupByColIdxs)
			if err != nil {
				return err
			}
		}
	}

	e.resultHandler = &tableResultHandler{}

	// To make the code more clean, we use a unified `groupedRanges` to represent the ranges to be read, no matter
	// whether they are from `e.ranges` or `e.groupedRanges`.
	var groupedRanges [][]*ranger.Range
	if len(e.groupedRanges) > 0 {
		groupedRanges = e.groupedRanges
	} else if len(e.ranges) > 0 {
		groupedRanges = [][]*ranger.Range{e.ranges}
	}

	var firstPartGroupedRanges, secondPartGroupedRanges [][]*ranger.Range
	for _, ranges := range groupedRanges {
		signedRanges, unsignedRanges := distsql.SplitRangesAcrossInt64Boundary(ranges, e.keepOrder, e.desc, e.table.Meta() != nil && e.table.Meta().IsCommonHandle)
		if len(signedRanges) > 0 {
			firstPartGroupedRanges = append(firstPartGroupedRanges, signedRanges)
		}
		if len(unsignedRanges) > 0 {
			secondPartGroupedRanges = append(secondPartGroupedRanges, unsignedRanges)
		}
	}

	// Treat temporary table as dummy table, avoid sending distsql request to TiKV.
	// Calculate the kv ranges here, UnionScan rely on this kv ranges.
	// cached table and temporary table are similar
	if e.dummy {
		if e.desc && len(secondPartGroupedRanges) != 0 {
			// TiKV support reverse scan and the `resultHandler` process the range order.
			// While in UnionScan, it doesn't use reverse scan and reverse the final result rows manually.
			// So things are differ, we need to reverse the kv range here.
			// TODO: If we refactor UnionScan to use reverse scan, update the code here.
			// [9734095886065816708 9734095886065816709] | [1 3] [65535 9734095886065816707] => before the following change
			// [1 3] [65535 9734095886065816707] | [9734095886065816708 9734095886065816709] => ranges part reverse here
			// [1 3  65535 9734095886065816707 9734095886065816708 9734095886065816709] => scan (normal order) in UnionScan
			// [9734095886065816709 9734095886065816708 9734095886065816707 65535 3  1] => rows reverse in UnionScan
			firstPartGroupedRanges, secondPartGroupedRanges = secondPartGroupedRanges, firstPartGroupedRanges
		}
		for _, ranges := range firstPartGroupedRanges {
			kvReq, err := e.buildKVReq(ctx, ranges)
			if err != nil {
				return err
			}
			e.kvRanges = kvReq.KeyRanges.AppendSelfTo(e.kvRanges)
		}
		for _, ranges := range secondPartGroupedRanges {
			kvReq, err := e.buildKVReq(ctx, ranges)
			if err != nil {
				return err
			}
			e.kvRanges = kvReq.KeyRanges.AppendSelfTo(e.kvRanges)
		}
		return nil
	}

	var firstResult distsql.SelectResult
	firstResult, err = e.buildRespForGroupedRanges(ctx, firstPartGroupedRanges)
	if err != nil {
		return err
	}
	if len(secondPartGroupedRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildRespForGroupedRanges(ctx, secondPartGroupedRanges)
	if err != nil {
		return err
	}
	e.resultHandler.open(firstResult, secondResult)
	return nil
}

// Next fills data into the chunk passed by its caller.
// The task was actually done by tableReaderHandler.
func (e *TableReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.dummy {
		// Treat temporary table as dummy table, avoid sending distsql request to TiKV.
		req.Reset()
		return nil
	}

	logutil.Eventf(ctx, "table scan table: %s, range: %v", stringutil.MemoizeStr(func() string {
		var tableName string
		if meta := e.table.Meta(); meta != nil {
			tableName = meta.Name.L
		}
		return tableName
	}), e.ranges)
	if err := e.resultHandler.nextChunk(ctx, req); err != nil {
		return err
	}

	err := table.FillVirtualColumnValue(e.virtualColumnRetFieldTypes, e.virtualColumnIndex, e.Schema().Columns, e.columns, e.ectx, req)
	if err != nil {
		return err
	}

	return nil
}

// Close implements the Executor Close interface.
func (e *TableReaderExecutor) Close() error {
	if e.indexUsageReporter != nil {
		e.indexUsageReporter.ReportCopIndexUsageForHandle(e.table, e.plans[0].ID())
	}

	var err error
	if e.resultHandler != nil {
		err = e.resultHandler.Close()
	}
	e.kvRanges = e.kvRanges[:0]
	if e.dummy {
		return nil
	}
	return err
}

// buildRespForGroupedRanges builds the input ranger.Range into SelectResult.
// Note that no matter the range is from e.ranges or e.groupedRanges, they are all passed in as groupedRanges here.
func (e *TableReaderExecutor) buildRespForGroupedRanges(ctx context.Context, groupedRanges [][]*ranger.Range) (distsql.SelectResult, error) {
	// Accessing partitioned table on TiFlash is slightly different right now, we use a different code path for it.
	if e.storeType == kv.TiFlash && e.kvRangeBuilder != nil {
		// Since accessing partitioned table on TiFlash doesn't support keep order, so e.groupedRanges must be empty.
		intest.Assert(len(groupedRanges) == 1 && len(e.groupedRanges) == 0)
	}
	if e.storeType == kv.TiFlash && e.kvRangeBuilder != nil && len(groupedRanges) == 1 && len(e.groupedRanges) == 0 {
		ranges := groupedRanges[0]
		if !e.batchCop {
			// TiFlash cannot support to access multiple tables/partitions within one KVReq, so we have to build KVReq for each partition separately.
			kvReqs, err := e.buildKVReqSeparately(ctx, ranges)
			if err != nil {
				return nil, err
			}
			e.kvRanges = sortAndGetKVRangesFromReqs(kvReqs)
			var results []distsql.SelectResult
			for _, kvReq := range kvReqs {
				result, err := e.SelectResult(ctx, e.dctx, kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
				if err != nil {
					return nil, err
				}
				results = append(results, result)
			}
			return distsql.NewSerialSelectResults(results), nil
		}
		// Use PartitionTable Scan
		kvReq, err := e.buildKVReqForPartitionTableScan(ctx, ranges)
		if err != nil {
			return nil, err
		}
		e.kvRanges = sortAndGetKVRangesFromReqs([]*kv.Request{kvReq})
		result, err := e.SelectResult(ctx, e.dctx, kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	kvReqs, err := e.buildKVReqSeparatelyForGroupedRanges(ctx, groupedRanges)
	if err != nil {
		return nil, err
	}
	// Even if it's an empty request, we still need to build a kvReq to make the following execution logic work.
	// Otherwise, there will be panic.
	if len(kvReqs) == 0 {
		kvReq, err := e.buildKVReq(ctx, nil)
		if err != nil {
			return nil, err
		}
		kvReqs = append(kvReqs, kvReq)
	}
	e.kvRanges = sortAndGetKVRangesFromReqs(kvReqs)
	results := make([]distsql.SelectResult, 0, len(kvReqs))
	for _, kvReq := range kvReqs {
		result, err := e.SelectResult(ctx, e.dctx, kvReq, exec.RetTypes(e), getPhysicalPlanIDs(e.plans), e.ID())
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	if len(results) == 1 {
		return results[0], nil
	}

	intest.Assert(len(e.byItems) > 0,
		"In current logic, if there are more than one result, len(e.byItems) must be > 0")
	return distsql.NewSortedSelectResults(e.ectx.GetEvalCtx(), results, e.Schema(), e.byItems, e.memTracker), nil
}

func (e *TableReaderExecutor) buildKVReqSeparatelyForGroupedRanges(ctx context.Context, groupedRanges [][]*ranger.Range) ([]*kv.Request, error) {
	var kvReqs []*kv.Request
	for _, ranges := range groupedRanges {
		if e.kvRangeBuilder != nil && e.byItems != nil {
			reqs, err := e.buildKVReqSeparately(ctx, ranges)
			if err != nil {
				return nil, err
			}
			kvReqs = append(kvReqs, reqs...)
		} else {
			kvReq, err := e.buildKVReq(ctx, ranges)
			if err != nil {
				return nil, err
			}
			kvReqs = append(kvReqs, kvReq)
		}
	}
	return kvReqs, nil
}

func sortAndGetKVRangesFromReqs(kvReqs []*kv.Request) []kv.KeyRange {
	kvRanges := make([]kv.KeyRange, 0, len(kvReqs))
	for _, kvReq := range kvReqs {
		kvReq.KeyRanges.SortByFunc(func(i, j kv.KeyRange) int {
			return bytes.Compare(i.StartKey, j.StartKey)
		})
		kvRanges = kvReq.KeyRanges.AppendSelfTo(kvRanges)
	}
	slices.SortFunc(kvRanges, func(i, j kv.KeyRange) int {
		return bytes.Compare(i.StartKey, j.StartKey)
	})
	return kvRanges
}

func (e *TableReaderExecutor) buildKVReqSeparately(ctx context.Context, ranges []*ranger.Range) ([]*kv.Request, error) {
	pids, kvRanges, err := e.kvRangeBuilder.buildKeyRangeSeparately(e.dctx, ranges)
	if err != nil {
		return nil, err
	}
	kvReqs := make([]*kv.Request, 0, len(kvRanges))
	for i, kvRange := range kvRanges {
		if err := internalutil.UpdateExecutorTableID(ctx, e.dagPB.RootExecutor, true, []int64{pids[i]}); err != nil {
			return nil, err
		}
		var builder distsql.RequestBuilder
		reqBuilder := builder.SetKeyRanges(kvRange)
		kvReq, err := reqBuilder.
			SetDAGRequest(e.dagPB).
			SetStartTS(e.startTS).
			SetDesc(e.desc).
			SetKeepOrder(e.keepOrder).
			SetTxnScope(e.txnScope).
			SetReadReplicaScope(e.readReplicaScope).
			SetFromSessionVars(e.dctx).
			SetFromInfoSchema(e.GetInfoSchema()).
			SetMemTracker(e.memTracker).
			SetStoreType(e.storeType).
			SetPaging(e.paging).
			SetAllowBatchCop(e.batchCop).
			SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.dctx, &reqBuilder.Request, e.netDataSize)).
			SetConnIDAndConnAlias(e.dctx.ConnectionID, e.dctx.SessionAlias).
			Build()
		if err != nil {
			return nil, err
		}
		kvReqs = append(kvReqs, kvReq)
	}
	return kvReqs, nil
}

func (e *TableReaderExecutor) buildKVReqForPartitionTableScan(ctx context.Context, ranges []*ranger.Range) (*kv.Request, error) {
	pids, kvRanges, err := e.kvRangeBuilder.buildKeyRangeSeparately(e.dctx, ranges)
	if err != nil {
		return nil, err
	}
	partitionIDAndRanges := make([]kv.PartitionIDAndRanges, 0, len(pids))
	for i, kvRange := range kvRanges {
		partitionIDAndRanges = append(partitionIDAndRanges, kv.PartitionIDAndRanges{
			ID:        pids[i],
			KeyRanges: kvRange,
		})
	}
	if err := internalutil.UpdateExecutorTableID(ctx, e.dagPB.RootExecutor, true, pids); err != nil {
		return nil, err
	}
	var builder distsql.RequestBuilder
	reqBuilder := builder.SetPartitionIDAndRanges(partitionIDAndRanges)
	kvReq, err := reqBuilder.
		SetDAGRequest(e.dagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetTxnScope(e.txnScope).
		SetReadReplicaScope(e.readReplicaScope).
		SetFromSessionVars(e.dctx).
		SetFromInfoSchema(e.GetInfoSchema()).
		SetMemTracker(e.memTracker).
		SetStoreType(e.storeType).
		SetPaging(e.paging).
		SetAllowBatchCop(e.batchCop).
		SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.dctx, &reqBuilder.Request, e.netDataSize)).
		SetConnIDAndConnAlias(e.dctx.ConnectionID, e.dctx.SessionAlias).
		Build()
	if err != nil {
		return nil, err
	}
	return kvReq, nil
}

func (e *TableReaderExecutor) buildKVReq(ctx context.Context, ranges []*ranger.Range) (*kv.Request, error) {
	var builder distsql.RequestBuilder
	var reqBuilder *distsql.RequestBuilder
	if e.kvRangeBuilder != nil {
		kvRange, err := e.kvRangeBuilder.buildKeyRange(e.dctx, ranges)
		if err != nil {
			return nil, err
		}
		reqBuilder = builder.SetPartitionKeyRanges(kvRange)
	} else {
		reqBuilder = builder.SetHandleRanges(e.dctx, getPhysicalTableID(e.table), e.table.Meta() != nil && e.table.Meta().IsCommonHandle, ranges)
	}
	if e.table != nil && e.table.Type().IsClusterTable() {
		copDestination := infoschema.GetClusterTableCopDestination(e.table.Meta().Name.L)
		if copDestination == infoschema.DDLOwner {
			serverInfo, err := e.GetDDLOwner(ctx)
			if err != nil {
				return nil, err
			}
			reqBuilder.SetTiDBServerID(serverInfo.ServerIDGetter())
		}
	}
	reqBuilder.
		SetDAGRequest(e.dagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetTxnScope(e.txnScope).
		SetReadReplicaScope(e.readReplicaScope).
		SetIsStaleness(e.isStaleness).
		SetFromSessionVars(e.dctx).
		SetFromInfoSchema(e.GetInfoSchema()).
		SetMemTracker(e.memTracker).
		SetStoreType(e.storeType).
		SetAllowBatchCop(e.batchCop).
		SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.dctx, &reqBuilder.Request, e.netDataSize)).
		SetPaging(e.paging).
		SetConnIDAndConnAlias(e.dctx.ConnectionID, e.dctx.SessionAlias)
	return reqBuilder.Build()
}

func buildVirtualColumnIndex(schema *expression.Schema, columns []*model.ColumnInfo) []int {
	virtualColumnIndex := make([]int, 0, len(columns))
	for i, col := range schema.Columns {
		if col.VirtualExpr != nil {
			virtualColumnIndex = append(virtualColumnIndex, i)
		}
	}
	slices.SortFunc(virtualColumnIndex, func(i, j int) int {
		return cmp.Compare(model.FindColumnInfoByID(columns, schema.Columns[i].ID).Offset,
			model.FindColumnInfoByID(columns, schema.Columns[j].ID).Offset)
	})
	return virtualColumnIndex
}

// buildVirtualColumnInfo saves virtual column indices and sort them in definition order
func (e *TableReaderExecutor) buildVirtualColumnInfo() {
	e.virtualColumnIndex, e.virtualColumnRetFieldTypes = buildVirtualColumnInfo(e.Schema(), e.columns)
}

// buildVirtualColumnInfo saves virtual column indices and sort them in definition order
func buildVirtualColumnInfo(schema *expression.Schema, columns []*model.ColumnInfo) (colIndexs []int, retTypes []*types.FieldType) {
	colIndexs = buildVirtualColumnIndex(schema, columns)
	if len(colIndexs) > 0 {
		retTypes = make([]*types.FieldType, len(colIndexs))
		for i, idx := range colIndexs {
			retTypes[i] = schema.Columns[idx].RetType
		}
	}
	return colIndexs, retTypes
}

type tableResultHandler struct {
	// If the pk is unsigned and we have KeepOrder=true and want ascending order,
	// `optionalResult` will handles the request whose range is in signed int range, and
	// `result` will handle the request whose range is exceed signed int range.
	// If we want descending order, `optionalResult` will handles the request whose range is exceed signed, and
	// the `result` will handle the request whose range is in signed.
	// Otherwise, we just set `optionalFinished` true and the `result` handles the whole ranges.
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

func (tr *tableResultHandler) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	if !tr.optionalFinished {
		err := tr.optionalResult.Next(ctx, chk)
		if err != nil {
			return err
		}
		if chk.NumRows() > 0 {
			return nil
		}
		tr.optionalFinished = true
	}
	return tr.result.Next(ctx, chk)
}

func (tr *tableResultHandler) nextRaw(ctx context.Context) (data []byte, err error) {
	if !tr.optionalFinished {
		data, err = tr.optionalResult.NextRaw(ctx)
		if err != nil {
			return nil, normalizeCtxErrWithCause(ctx, err)
		}
		if data != nil {
			return data, nil
		}
		tr.optionalFinished = true
	}
	data, err = tr.result.NextRaw(ctx)
	if err != nil {
		return nil, normalizeCtxErrWithCause(ctx, err)
	}
	return data, nil
}

func (tr *tableResultHandler) Close() error {
	err := closeAll(tr.optionalResult, tr.result)
	tr.optionalResult, tr.result = nil, nil
	return err
}
