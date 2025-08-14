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
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ base.Task = &MppTask{}
	_ base.Task = &CopTask{}
)

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
	physPlanPartInfo *physicalop.PhysPlanPartInfo

	// expectCnt is the expected row count of upper task, 0 for unlimited.
	// It's used for deciding whether using paging distsql.
	expectCnt uint64

	// For copTask and rootTask, when we compose physical tree bottom-up, index join need some special info
	// fetched from underlying ds which built index range or table range based on these runtime constant.
	IndexJoinInfo *IndexJoinInfo

	// warnings passed through different task copy attached with more upper operator specific warnings. (not concurrent safe)
	warnings simpleWarnings
}

// AppendWarning appends a warning
func (t *CopTask) AppendWarning(err error) {
	t.warnings.AppendWarning(err)
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
	// since *t will reuse the same warnings slice, we need to copy it out.
	// cause different task instance should have different warning slice.
	nt.warnings.Copy(&t.warnings)
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

func (t *CopTask) convertToRootTaskImpl(ctx base.PlanContext) (rt *RootTask) {
	defer func() {
		if t.IndexJoinInfo != nil {
			// return indexJoinInfo upward, when copTask is converted to rootTask.
			rt.IndexJoinInfo = t.IndexJoinInfo
		}
		if t.warnings.WarningCount() > 0 {
			rt.warnings.CopyFrom(&t.warnings)
		}
	}()
	// copTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistSQLScanConcurrency.
	t.finishIndexPlan()
	// Network cost of transferring rows of table scan to TiDB.
	if t.tablePlan != nil {
		tp := t.tablePlan
		for len(tp.Children()) > 0 {
			tp = tp.Children()[0]
		}
		ts := tp.(*physicalop.PhysicalTableScan)
		prevColumnLen := len(ts.Columns)
		prevSchema := ts.Schema().Clone()
		ts.Columns = ExpandVirtualColumn(ts.Columns, ts.Schema(), ts.Table.Columns)
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
		newTask.SetPlan(p)
		if t.needExtraProj {
			schema := t.originSchema
			proj := physicalop.PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.StatsInfo(), t.idxMergePartPlans[0].QueryBlockOffset(), nil)
			proj.SetSchema(schema)
			proj.SetChildren(p)
			newTask.SetPlan(proj)
		}
		t.handleRootTaskConds(ctx, newTask)
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
			tp = tp.Children()[0]
		}
		ts := tp.(*physicalop.PhysicalTableScan)
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
		case *physicalop.PhysicalHashAgg, *physicalop.PhysicalStreamAgg:
			aggPushedDown = true
		}

		if t.needExtraProj && !aggPushedDown {
			proj := physicalop.PhysicalProjection{Exprs: expression.Column2Exprs(t.originSchema.Columns)}.Init(ts.SCtx(), ts.StatsInfo(), ts.QueryBlockOffset(), nil)
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
