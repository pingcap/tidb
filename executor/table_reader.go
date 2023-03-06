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
	"context"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/exp/slices"
)

// make sure `TableReaderExecutor` implements `Executor`.
var _ Executor = &TableReaderExecutor{}

// selectResultHook is used to hack distsql.SelectWithRuntimeStats safely for testing.
type selectResultHook struct {
	selectResultFunc func(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
		fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copPlanIDs []int) (distsql.SelectResult, error)
}

func (sr selectResultHook) SelectResult(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copPlanIDs []int, rootPlanID int) (distsql.SelectResult, error) {
	if sr.selectResultFunc == nil {
		return distsql.SelectWithRuntimeStats(ctx, sctx, kvReq, fieldTypes, fb, copPlanIDs, rootPlanID)
	}
	return sr.selectResultFunc(ctx, sctx, kvReq, fieldTypes, fb, copPlanIDs)
}

type kvRangeBuilder interface {
	buildKeyRange(ranges []*ranger.Range) ([][]kv.KeyRange, error)
	buildKeyRangeSeparately(ranges []*ranger.Range) ([]int64, [][]kv.KeyRange, error)
}

// TableReaderExecutor sends DAG request and reads table data from kv layer.
type TableReaderExecutor struct {
	baseExecutor

	table table.Table

	// The source of key ranges varies from case to case.
	// It may be calculated from PhysicalPlan by executorBuilder, or calculated from argument by dataBuilder;
	// It may be calculated from ranger.Ranger, or calculated from handles.
	// The table ID may also change because of the partition table, and causes the key range to change.
	// So instead of keeping a `range` struct field, it's better to define a interface.
	kvRangeBuilder
	// TODO: remove this field, use the kvRangeBuilder interface.
	ranges []*ranger.Range

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
	feedback      *statistics.QueryFeedback
	plans         []plannercore.PhysicalPlan
	tablePlan     plannercore.PhysicalPlan

	memTracker       *memory.Tracker
	selectResultHook // for testing

	keepOrder bool
	desc      bool
	paging    bool
	storeType kv.StoreType
	// corColInFilter tells whether there's correlated column in filter.
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
		e.memTracker = memory.NewTracker(e.id, -1)
	}
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	var err error
	if e.corColInFilter {
		if e.storeType == kv.TiFlash {
			execs, err := constructDistExecForTiFlash(e.ctx, e.tablePlan)
			if err != nil {
				return err
			}
			e.dagPB.RootExecutor = execs[0]
		} else {
			e.dagPB.Executors, err = constructDistExec(e.ctx, e.plans)
			if err != nil {
				return err
			}
		}
	}
	if e.runtimeStats != nil {
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}
	if e.corColInAccess {
		ts := e.plans[0].(*plannercore.PhysicalTableScan)
		e.ranges, err = ts.ResolveCorrelatedColumns()
		if err != nil {
			return err
		}
	}

	e.resultHandler = &tableResultHandler{}
	if e.feedback != nil && e.feedback.Hist != nil {
		// EncodeInt don't need *statement.Context.
		var ok bool
		e.ranges, ok = e.feedback.Hist.SplitRange(nil, e.ranges, false)
		if !ok {
			e.feedback.Invalidate()
		}
	}
	firstPartRanges, secondPartRanges := distsql.SplitRangesAcrossInt64Boundary(e.ranges, e.keepOrder, e.desc, e.table.Meta() != nil && e.table.Meta().IsCommonHandle)

	// Treat temporary table as dummy table, avoid sending distsql request to TiKV.
	// Calculate the kv ranges here, UnionScan rely on this kv ranges.
	// cached table and temporary table are similar
	if e.dummy {
		if e.desc && len(secondPartRanges) != 0 {
			// TiKV support reverse scan and the `resultHandler` process the range order.
			// While in UnionScan, it doesn't use reverse scan and reverse the final result rows manually.
			// So things are differ, we need to reverse the kv range here.
			// TODO: If we refactor UnionScan to use reverse scan, update the code here.
			// [9734095886065816708 9734095886065816709] | [1 3] [65535 9734095886065816707] => before the following change
			// [1 3] [65535 9734095886065816707] | [9734095886065816708 9734095886065816709] => ranges part reverse here
			// [1 3  65535 9734095886065816707 9734095886065816708 9734095886065816709] => scan (normal order) in UnionScan
			// [9734095886065816709 9734095886065816708 9734095886065816707 65535 3  1] => rows reverse in UnionScan
			firstPartRanges, secondPartRanges = secondPartRanges, firstPartRanges
		}
		kvReq, err := e.buildKVReq(ctx, firstPartRanges)
		if err != nil {
			return err
		}
		e.kvRanges = kvReq.KeyRanges.AppendSelfTo(e.kvRanges)
		if len(secondPartRanges) != 0 {
			kvReq, err = e.buildKVReq(ctx, secondPartRanges)
			if err != nil {
				return err
			}
			e.kvRanges = kvReq.KeyRanges.AppendSelfTo(e.kvRanges)
		}
		return nil
	}

	firstResult, err := e.buildResp(ctx, firstPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(ctx, secondPartRanges)
	if err != nil {
		e.feedback.Invalidate()
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
		e.feedback.Invalidate()
		return err
	}

	err := table.FillVirtualColumnValue(e.virtualColumnRetFieldTypes, e.virtualColumnIndex, e.schema.Columns, e.columns, e.ctx, req)
	if err != nil {
		return err
	}

	return nil
}

// Close implements the Executor Close interface.
func (e *TableReaderExecutor) Close() error {
	var err error
	if e.resultHandler != nil {
		err = e.resultHandler.Close()
	}
	e.kvRanges = e.kvRanges[:0]
	if e.dummy {
		return nil
	}
	e.ctx.StoreQueryFeedback(e.feedback)
	return err
}

// buildResp first builds request and sends it to tikv using distsql.Select. It uses SelectResult returned by the callee
// to fetch all results.
func (e *TableReaderExecutor) buildResp(ctx context.Context, ranges []*ranger.Range) (distsql.SelectResult, error) {
	if e.storeType == kv.TiFlash && e.kvRangeBuilder != nil {
		if !e.batchCop {
			// TiFlash cannot support to access multiple tables/partitions within one KVReq, so we have to build KVReq for each partition separately.
			kvReqs, err := e.buildKVReqSeparately(ctx, ranges)
			if err != nil {
				return nil, err
			}
			var results []distsql.SelectResult
			for _, kvReq := range kvReqs {
				result, err := e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
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
		result, err := e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	kvReq, err := e.buildKVReq(ctx, ranges)
	if err != nil {
		return nil, err
	}
	kvReq.KeyRanges.SortByFunc(func(i, j kv.KeyRange) bool {
		return bytes.Compare(i.StartKey, j.StartKey) < 0
	})
	e.kvRanges = kvReq.KeyRanges.AppendSelfTo(e.kvRanges)

	result, err := e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (e *TableReaderExecutor) buildKVReqSeparately(ctx context.Context, ranges []*ranger.Range) ([]*kv.Request, error) {
	pids, kvRanges, err := e.kvRangeBuilder.buildKeyRangeSeparately(ranges)
	if err != nil {
		return nil, err
	}
	kvReqs := make([]*kv.Request, 0, len(kvRanges))
	for i, kvRange := range kvRanges {
		e.kvRanges = append(e.kvRanges, kvRange...)
		if err := updateExecutorTableID(ctx, e.dagPB.RootExecutor, true, []int64{pids[i]}); err != nil {
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
			SetFromSessionVars(e.ctx.GetSessionVars()).
			SetFromInfoSchema(e.ctx.GetInfoSchema()).
			SetMemTracker(e.memTracker).
			SetStoreType(e.storeType).
			SetPaging(e.paging).
			SetAllowBatchCop(e.batchCop).
			SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.ctx, &reqBuilder.Request, e.netDataSize)).
			Build()
		if err != nil {
			return nil, err
		}
		kvReqs = append(kvReqs, kvReq)
	}
	return kvReqs, nil
}

func (e *TableReaderExecutor) buildKVReqForPartitionTableScan(ctx context.Context, ranges []*ranger.Range) (*kv.Request, error) {
	pids, kvRanges, err := e.kvRangeBuilder.buildKeyRangeSeparately(ranges)
	if err != nil {
		return nil, err
	}
	partitionIDAndRanges := make([]kv.PartitionIDAndRanges, 0, len(pids))
	for i, kvRange := range kvRanges {
		e.kvRanges = append(e.kvRanges, kvRange...)
		partitionIDAndRanges = append(partitionIDAndRanges, kv.PartitionIDAndRanges{
			ID:        pids[i],
			KeyRanges: kvRange,
		})
	}
	if err := updateExecutorTableID(ctx, e.dagPB.RootExecutor, true, pids); err != nil {
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
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetFromInfoSchema(e.ctx.GetInfoSchema()).
		SetMemTracker(e.memTracker).
		SetStoreType(e.storeType).
		SetPaging(e.paging).
		SetAllowBatchCop(e.batchCop).
		SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.ctx, &reqBuilder.Request, e.netDataSize)).
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
		kvRange, err := e.kvRangeBuilder.buildKeyRange(ranges)
		if err != nil {
			return nil, err
		}
		reqBuilder = builder.SetPartitionKeyRanges(kvRange)
	} else {
		reqBuilder = builder.SetHandleRanges(e.ctx.GetSessionVars().StmtCtx, getPhysicalTableID(e.table), e.table.Meta() != nil && e.table.Meta().IsCommonHandle, ranges, e.feedback)
	}
	if e.table != nil && e.table.Type().IsClusterTable() {
		copDestination := infoschema.GetClusterTableCopDestination(e.table.Meta().Name.L)
		if copDestination == infoschema.DDLOwner {
			ownerManager := domain.GetDomain(e.ctx).DDL().OwnerManager()
			ddlOwnerID, err := ownerManager.GetOwnerID(ctx)
			if err != nil {
				return nil, err
			}
			serverInfo, err := infosync.GetServerInfoByID(ctx, ddlOwnerID)
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
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetFromInfoSchema(sessiontxn.GetTxnManager(e.ctx).GetTxnInfoSchema()).
		SetMemTracker(e.memTracker).
		SetStoreType(e.storeType).
		SetAllowBatchCop(e.batchCop).
		SetClosestReplicaReadAdjuster(newClosestReadAdjuster(e.ctx, &reqBuilder.Request, e.netDataSize)).
		SetPaging(e.paging)
	return reqBuilder.Build()
}

func buildVirtualColumnIndex(schema *expression.Schema, columns []*model.ColumnInfo) []int {
	virtualColumnIndex := make([]int, 0, len(columns))
	for i, col := range schema.Columns {
		if col.VirtualExpr != nil {
			virtualColumnIndex = append(virtualColumnIndex, i)
		}
	}
	slices.SortFunc(virtualColumnIndex, func(i, j int) bool {
		return plannercore.FindColumnInfoByID(columns, schema.Columns[i].ID).Offset <
			plannercore.FindColumnInfoByID(columns, schema.Columns[j].ID).Offset
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
			return nil, err
		}
		if data != nil {
			return data, nil
		}
		tr.optionalFinished = true
	}
	data, err = tr.result.NextRaw(ctx)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (tr *tableResultHandler) Close() error {
	err := closeAll(tr.optionalResult, tr.result)
	tr.optionalResult, tr.result = nil, nil
	return err
}
