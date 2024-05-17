// Copyright 2024 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ base.Task = &RootTask{}
	_ base.Task = &MppTask{}
	_ base.Task = &CopTask{}
)

// ************************************* RootTask Start ******************************************

// RootTask is the final sink node of a plan graph. It should be a single goroutine on tidb.
type RootTask struct {
	p       base.PhysicalPlan
	isEmpty bool // isEmpty indicates if this task contains a dual table and returns empty data.
	// TODO: The flag 'isEmpty' is only checked by Projection and UnionAll. We should support more cases in the future.
}

// GetPlan returns the root task's plan.
func (t *RootTask) GetPlan() base.PhysicalPlan {
	return t.p
}

// SetPlan sets the root task' plan.
func (t *RootTask) SetPlan(p base.PhysicalPlan) {
	t.p = p
}

// IsEmpty indicates whether root task is empty.
func (t *RootTask) IsEmpty() bool {
	return t.isEmpty
}

// SetEmpty set the root task as empty.
func (t *RootTask) SetEmpty(x bool) {
	t.isEmpty = x
}

// Copy implements Task interface.
func (t *RootTask) Copy() base.Task {
	return &RootTask{
		p: t.p,
	}
}

// ConvertToRootTask implements Task interface.
func (t *RootTask) ConvertToRootTask(_ base.PlanContext) base.Task {
	return t.Copy().(*RootTask)
}

// Invalid implements Task interface.
func (t *RootTask) Invalid() bool {
	return t.p == nil
}

// Count implements Task interface.
func (t *RootTask) Count() float64 {
	return t.p.StatsInfo().RowCount
}

// Plan implements Task interface.
func (t *RootTask) Plan() base.PhysicalPlan {
	return t.p
}

// MemoryUsage return the memory usage of rootTask
func (t *RootTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}
	sum = size.SizeOfInterface + size.SizeOfBool
	if t.p != nil {
		sum += t.p.MemoryUsage()
	}
	return sum
}

// ************************************* RootTask End ******************************************

// ************************************* MPPTask Start ******************************************

// MppTask can not :
// 1. keep order
// 2. support double read
// 3. consider virtual columns.
// 4. TODO: partition prune after close
type MppTask struct {
	p base.PhysicalPlan

	partTp   property.MPPPartitionType
	hashCols []*property.MPPPartitionColumn

	// rootTaskConds record filters of TableScan that cannot be pushed down to TiFlash.

	// For logical plan like: HashAgg -> Selection -> TableScan, if filters in Selection cannot be pushed to TiFlash.
	// Planner will generate physical plan like: PhysicalHashAgg -> PhysicalSelection -> TableReader -> PhysicalTableScan(cop tiflash)
	// Because planner will make mppTask invalid directly then use copTask directly.

	// But in DisaggregatedTiFlash mode, cop and batchCop protocol is disabled, so we have to consider this situation for mppTask.
	// When generating PhysicalTableScan, if prop.TaskTp is RootTaskType, mppTask will be converted to rootTask,
	// and filters in rootTaskConds will be added in a Selection which will be executed in TiDB.
	// So physical plan be like: PhysicalHashAgg -> PhysicalSelection -> TableReader -> ExchangeSender -> PhysicalTableScan(mpp tiflash)
	rootTaskConds []expression.Expression
	tblColHists   *statistics.HistColl
}

// Count implements Task interface.
func (t *MppTask) Count() float64 {
	return t.p.StatsInfo().RowCount
}

// Copy implements Task interface.
func (t *MppTask) Copy() base.Task {
	nt := *t
	return &nt
}

// Plan implements Task interface.
func (t *MppTask) Plan() base.PhysicalPlan {
	return t.p
}

// Invalid implements Task interface.
func (t *MppTask) Invalid() bool {
	return t.p == nil
}

// ConvertToRootTask implements Task interface.
func (t *MppTask) ConvertToRootTask(ctx base.PlanContext) base.Task {
	return t.Copy().(*MppTask).ConvertToRootTaskImpl(ctx)
}

// MemoryUsage return the memory usage of mppTask
func (t *MppTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfInterface + size.SizeOfInt + size.SizeOfSlice + int64(cap(t.hashCols))*size.SizeOfPointer
	if t.p != nil {
		sum += t.p.MemoryUsage()
	}
	return
}

// ConvertToRootTaskImpl implements Task interface.
func (t *MppTask) ConvertToRootTaskImpl(ctx base.PlanContext) *RootTask {
	// In disaggregated-tiflash mode, need to consider generated column.
	tryExpandVirtualColumn(t.p)
	sender := PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_PassThrough,
	}.Init(ctx, t.p.StatsInfo())
	sender.SetChildren(t.p)

	p := PhysicalTableReader{
		tablePlan: sender,
		StoreType: kv.TiFlash,
	}.Init(ctx, t.p.QueryBlockOffset())
	p.SetStats(t.p.StatsInfo())
	collectPartitionInfosFromMPPPlan(p, t.p)
	rt := &RootTask{}
	rt.SetPlan(p)

	if len(t.rootTaskConds) > 0 {
		// Some Filter cannot be pushed down to TiFlash, need to add Selection in rootTask,
		// so this Selection will be executed in TiDB.
		_, isTableScan := t.p.(*PhysicalTableScan)
		_, isSelection := t.p.(*PhysicalSelection)
		if isSelection {
			_, isTableScan = t.p.Children()[0].(*PhysicalTableScan)
		}
		if !isTableScan {
			// Need to make sure oriTaskPlan is TableScan, because rootTaskConds is part of TableScan.FilterCondition.
			// It's only for TableScan. This is ensured by converting mppTask to rootTask just after TableScan is built,
			// so no other operators are added into this mppTask.
			logutil.BgLogger().Error("expect Selection or TableScan for mppTask.p", zap.String("mppTask.p", t.p.TP()))
			return base.InvalidTask.(*RootTask)
		}
		selectivity, _, err := cardinality.Selectivity(ctx, t.tblColHists, t.rootTaskConds, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = cost.SelectionFactor
		}
		sel := PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, rt.GetPlan().StatsInfo().Scale(selectivity), rt.GetPlan().QueryBlockOffset())
		sel.fromDataSource = true
		sel.SetChildren(rt.GetPlan())
		rt.SetPlan(sel)
	}
	return rt
}

// ************************************* MPPTask End ******************************************

// ************************************* CopTask Start ******************************************

// CopTask is a task that runs in a distributed kv store.
// TODO: In future, we should split copTask to indexTask and tableTask.
type CopTask struct {
	indexPlan base.PhysicalPlan
	tablePlan base.PhysicalPlan
	// indexPlanFinished means we have finished index plan.
	indexPlanFinished bool
	// keepOrder indicates if the plan scans data by order.
	keepOrder bool
	// needExtraProj means an extra prune is needed because
	// in double read / index merge cases, they may output one more column for handle(row id).
	needExtraProj bool
	// originSchema is the target schema to be projected to when needExtraProj is true.
	originSchema *expression.Schema

	extraHandleCol   *expression.Column
	commonHandleCols []*expression.Column
	// tblColHists stores the original stats of DataSource, it is used to get
	// average row width when computing network cost.
	tblColHists *statistics.HistColl
	// tblCols stores the original columns of DataSource before being pruned, it
	// is used to compute average row width when computing scan cost.
	tblCols []*expression.Column

	idxMergePartPlans      []base.PhysicalPlan
	idxMergeIsIntersection bool
	idxMergeAccessMVIndex  bool

	// rootTaskConds stores select conditions containing virtual columns.
	// These conditions can't push to TiKV, so we have to add a selection for rootTask
	rootTaskConds []expression.Expression

	// For table partition.
	physPlanPartInfo PhysPlanPartInfo

	// expectCnt is the expected row count of upper task, 0 for unlimited.
	// It's used for deciding whether using paging distsql.
	expectCnt uint64
}

// Invalid implements Task interface.
func (t *CopTask) Invalid() bool {
	return t.tablePlan == nil && t.indexPlan == nil && len(t.idxMergePartPlans) == 0
}

// Count implements Task interface.
func (t *CopTask) Count() float64 {
	if t.indexPlanFinished {
		return t.tablePlan.StatsInfo().RowCount
	}
	return t.indexPlan.StatsInfo().RowCount
}

// Copy implements Task interface.
func (t *CopTask) Copy() base.Task {
	nt := *t
	return &nt
}

// Plan implements Task interface.
// copTask plan should be careful with indexMergeReader, whose real plan is stored in
// idxMergePartPlans, when its indexPlanFinished is marked with false.
func (t *CopTask) Plan() base.PhysicalPlan {
	if t.indexPlanFinished {
		return t.tablePlan
	}
	return t.indexPlan
}

// MemoryUsage return the memory usage of copTask
func (t *CopTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfInterface*(2+int64(cap(t.idxMergePartPlans)+cap(t.rootTaskConds))) + size.SizeOfBool*3 + size.SizeOfUint64 +
		size.SizeOfPointer*(3+int64(cap(t.commonHandleCols)+cap(t.tblCols))) + size.SizeOfSlice*4 + t.physPlanPartInfo.MemoryUsage()
	if t.indexPlan != nil {
		sum += t.indexPlan.MemoryUsage()
	}
	if t.tablePlan != nil {
		sum += t.tablePlan.MemoryUsage()
	}
	if t.originSchema != nil {
		sum += t.originSchema.MemoryUsage()
	}
	if t.extraHandleCol != nil {
		sum += t.extraHandleCol.MemoryUsage()
	}

	for _, col := range t.commonHandleCols {
		sum += col.MemoryUsage()
	}
	for _, col := range t.tblCols {
		sum += col.MemoryUsage()
	}
	for _, p := range t.idxMergePartPlans {
		sum += p.MemoryUsage()
	}
	for _, expr := range t.rootTaskConds {
		sum += expr.MemoryUsage()
	}
	return
}

// ConvertToRootTask implements Task interface.
func (t *CopTask) ConvertToRootTask(ctx base.PlanContext) base.Task {
	// copy one to avoid changing itself.
	return t.Copy().(*CopTask).convertToRootTaskImpl(ctx)
}

func (t *CopTask) convertToRootTaskImpl(ctx base.PlanContext) *RootTask {
	// copTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistSQLScanConcurrency.
	t.finishIndexPlan()
	// Network cost of transferring rows of table scan to TiDB.
	if t.tablePlan != nil {
		tp := t.tablePlan
		for len(tp.Children()) > 0 {
			if len(tp.Children()) == 1 {
				tp = tp.Children()[0]
			} else {
				join := tp.(*PhysicalHashJoin)
				tp = join.children[1-join.InnerChildIdx]
			}
		}
		ts := tp.(*PhysicalTableScan)
		prevColumnLen := len(ts.Columns)
		prevSchema := ts.schema.Clone()
		ts.Columns = ExpandVirtualColumn(ts.Columns, ts.schema, ts.Table.Columns)
		if !t.needExtraProj && len(ts.Columns) > prevColumnLen {
			// Add a projection to make sure not to output extract columns.
			t.needExtraProj = true
			t.originSchema = prevSchema
		}
	}
	newTask := &RootTask{}
	if t.idxMergePartPlans != nil {
		p := PhysicalIndexMergeReader{
			partialPlans:       t.idxMergePartPlans,
			tablePlan:          t.tablePlan,
			IsIntersectionType: t.idxMergeIsIntersection,
			AccessMVIndex:      t.idxMergeAccessMVIndex,
			KeepOrder:          t.keepOrder,
		}.Init(ctx, t.idxMergePartPlans[0].QueryBlockOffset())
		p.PlanPartInfo = t.physPlanPartInfo
		setTableScanToTableRowIDScan(p.tablePlan)
		newTask.SetPlan(p)
		t.handleRootTaskConds(ctx, newTask)
		if t.needExtraProj {
			schema := t.originSchema
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.StatsInfo(), t.idxMergePartPlans[0].QueryBlockOffset(), nil)
			proj.SetSchema(schema)
			proj.SetChildren(p)
			newTask.SetPlan(proj)
		}
		return newTask
	}
	if t.indexPlan != nil && t.tablePlan != nil {
		newTask = buildIndexLookUpTask(ctx, t)
	} else if t.indexPlan != nil {
		p := PhysicalIndexReader{indexPlan: t.indexPlan}.Init(ctx, t.indexPlan.QueryBlockOffset())
		p.PlanPartInfo = t.physPlanPartInfo
		p.SetStats(t.indexPlan.StatsInfo())
		newTask.SetPlan(p)
	} else {
		tp := t.tablePlan
		for len(tp.Children()) > 0 {
			if len(tp.Children()) == 1 {
				tp = tp.Children()[0]
			} else {
				join := tp.(*PhysicalHashJoin)
				tp = join.children[1-join.InnerChildIdx]
			}
		}
		ts := tp.(*PhysicalTableScan)
		p := PhysicalTableReader{
			tablePlan:      t.tablePlan,
			StoreType:      ts.StoreType,
			IsCommonHandle: ts.Table.IsCommonHandle,
		}.Init(ctx, t.tablePlan.QueryBlockOffset())
		p.PlanPartInfo = t.physPlanPartInfo
		p.SetStats(t.tablePlan.StatsInfo())

		// If agg was pushed down in Attach2Task(), the partial agg was placed on the top of tablePlan, the final agg was
		// placed above the PhysicalTableReader, and the schema should have been set correctly for them, the schema of
		// partial agg contains the columns needed by the final agg.
		// If we add the projection here, the projection will be between the final agg and the partial agg, then the
		// schema will be broken, the final agg will fail to find needed columns in ResolveIndices().
		// Besides, the agg would only be pushed down if it doesn't contain virtual columns, so virtual column should not be affected.
		aggPushedDown := false
		switch p.tablePlan.(type) {
		case *PhysicalHashAgg, *PhysicalStreamAgg:
			aggPushedDown = true
		}

		if t.needExtraProj && !aggPushedDown {
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(t.originSchema.Columns)}.Init(ts.SCtx(), ts.StatsInfo(), ts.QueryBlockOffset(), nil)
			proj.SetSchema(t.originSchema)
			proj.SetChildren(p)
			newTask.SetPlan(proj)
		} else {
			newTask.SetPlan(p)
		}
	}

	t.handleRootTaskConds(ctx, newTask)
	return newTask
}

// ************************************* CopTask End ******************************************
