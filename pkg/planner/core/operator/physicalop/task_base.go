// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/context"
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

// SimpleWarnings is a simple implementation of Warnings interface.
type SimpleWarnings struct {
	warnings []*context.SQLWarn
}

// WarningCount returns the number of warnings.
func (s *SimpleWarnings) WarningCount() int {
	return len(s.warnings)
}

// Copy implemented the simple warnings copy to avoid use the same warnings slice for different task instance.
func (s *SimpleWarnings) Copy(src *SimpleWarnings) {
	warnings := make([]*context.SQLWarn, 0, len(src.warnings))
	warnings = append(warnings, src.warnings...)
	s.warnings = warnings
}

// CopyFrom copy the warnings from src to s.
func (s *SimpleWarnings) CopyFrom(src ...*SimpleWarnings) {
	if src == nil {
		return
	}
	length := 0
	for _, one := range src {
		if one == nil {
			continue
		}
		length += one.WarningCount()
	}
	s.warnings = make([]*context.SQLWarn, 0, length)
	for _, one := range src {
		if one == nil {
			continue
		}
		s.warnings = append(s.warnings, one.warnings...)
	}
}

// AppendWarning appends a warning to the warnings slice.
func (s *SimpleWarnings) AppendWarning(warn error) {
	if len(s.warnings) < math.MaxUint16 {
		s.warnings = append(s.warnings, &context.SQLWarn{Level: context.WarnLevelWarning, Err: warn})
	}
}

// AppendNote appends a note to the warnings slice.
func (s *SimpleWarnings) AppendNote(note error) {
	if len(s.warnings) < math.MaxUint16 {
		s.warnings = append(s.warnings, &context.SQLWarn{Level: context.WarnLevelNote, Err: note})
	}
}

// GetWarnings returns the internal all stored warnings.
func (s *SimpleWarnings) GetWarnings() []context.SQLWarn {
	// we just reuse and reorganize pointer of warning elem across different level's
	// task warnings slice to avoid copy them totally leading mem cost.
	// when best task is finished and final warnings is determined, we should convert
	// pointer to struct to append it to session context.
	warnings := make([]context.SQLWarn, 0, len(s.warnings))
	for _, w := range s.warnings {
		warnings = append(warnings, *w)
	}
	return warnings
}

// ************************************* RootTask Start ******************************************

// RootTask is the final sink node of a plan graph. It should be a single goroutine on tidb.
type RootTask struct {
	p base.PhysicalPlan

	// For copTask and rootTask, when we compose physical tree bottom-up, index join need some special info
	// fetched from underlying ds which built index range or table range based on these runtime constant.
	IndexJoinInfo *IndexJoinInfo

	// Warnings passed through different task copy attached with more upper operator specific Warnings. (not concurrent safe)
	Warnings SimpleWarnings
}

// GetPlan returns the root task's plan.
func (t *RootTask) GetPlan() base.PhysicalPlan {
	return t.p
}

// SetPlan sets the root task's plan.
func (t *RootTask) SetPlan(p base.PhysicalPlan) {
	t.p = p
}

// Copy implements Task interface.
func (t *RootTask) Copy() base.Task {
	nt := &RootTask{
		p: t.p,

		// when copying, just copy it out.
		IndexJoinInfo: t.IndexJoinInfo,
	}
	// since *t will reuse the same warnings slice, we need to copy it out.
	// because different task instance should have different warning slice.
	nt.Warnings.Copy(&t.Warnings)
	return nt
}

// ConvertToRootTask implements Task interface.
func (t *RootTask) ConvertToRootTask(_ base.PlanContext) base.Task {
	// root -> root, only copy another one instance.
	// *p: a new pointer to pointer current task's physic plan
	// warnings: a new slice to store current task-bound(p-bound) warnings.
	// *indexInfo: a new pointer to inherit the index join info upward if necessary.
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

// AppendWarning appends a warning
func (t *RootTask) AppendWarning(err error) {
	t.Warnings.AppendWarning(err)
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
	HashCols []*property.MPPPartitionColumn

	// rootTaskConds record filters of TableScan that cannot be pushed down to TiFlash.

	// For logical plan like: HashAgg -> Selection -> TableScan, if filters in Selection cannot be pushed to TiFlash.
	// Planner will generate physical plan like: PhysicalHashAgg -> PhysicalSelection -> TableReader -> PhysicalTableScan(cop tiflash)
	// Because planner will make mppTask invalid directly then use copTask directly.

	// But in DisaggregatedTiFlash mode, cop and batchCop protocol is disabled, so we have to consider this situation for mppTask.
	// When generating PhysicalTableScan, if prop.TaskTp is RootTaskType, mppTask will be converted to rootTask,
	// and filters in RootTaskConds will be added in a Selection which will be executed in TiDB.
	// So physical plan be like: PhysicalHashAgg -> PhysicalSelection -> TableReader -> ExchangeSender -> PhysicalTableScan(mpp tiflash)
	RootTaskConds []expression.Expression
	tblColHists   *statistics.HistColl

	// Warnings passed through different task copy attached with more upper operator specific Warnings. (not concurrent safe)
	Warnings SimpleWarnings
}

// NewMppTask creates a new mpp task.
func NewMppTask(p base.PhysicalPlan, partTp property.MPPPartitionType, hashCols []*property.MPPPartitionColumn, tblColHists *statistics.HistColl, warnings ...*SimpleWarnings) *MppTask {
	mt := &MppTask{
		p:           p,
		partTp:      partTp,
		HashCols:    hashCols,
		tblColHists: tblColHists,
	}
	mt.Warnings.CopyFrom(warnings...)
	return mt
}

// GetPartitionType returns the partition type of the mpp task.
func (t *MppTask) GetPartitionType() property.MPPPartitionType {
	return t.partTp
}

// GetHashCols returns the hash columns of the mpp task.
func (t *MppTask) GetHashCols() []*property.MPPPartitionColumn {
	return t.HashCols
}

// GetWarnings returns the warnings of the mpp task.
func (t *MppTask) GetWarnings() *SimpleWarnings {
	return &t.Warnings
}

// GetTblColHists returns the table column statistics of the mpp task.
func (t *MppTask) GetTblColHists() *statistics.HistColl {
	return t.tblColHists
}

// Count implements Task interface.
func (t *MppTask) Count() float64 {
	return t.p.StatsInfo().RowCount
}

// Copy implements Task interface.
func (t *MppTask) Copy() base.Task {
	nt := *t
	// since *t will reuse the same warnings slice, we need to copy it out.
	// cause different task instance should have different warning slice.
	nt.Warnings.Copy(&t.Warnings)
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

	sum = size.SizeOfInterface + size.SizeOfInt + size.SizeOfSlice + int64(cap(t.HashCols))*size.SizeOfPointer
	if t.p != nil {
		sum += t.p.MemoryUsage()
	}
	return
}

// AppendWarning appends a warning
func (t *MppTask) AppendWarning(err error) {
	t.Warnings.AppendWarning(err)
}

// ConvertToRootTaskImpl implements Task interface.
func (t *MppTask) ConvertToRootTaskImpl(ctx base.PlanContext) (rt *RootTask) {
	defer func() {
		// mppTask should inherit the indexJoinInfo upward.
		// because mpp task bottom doesn't form the indexJoin related cop task.
		if t.Warnings.WarningCount() > 0 {
			rt.Warnings.CopyFrom(&t.Warnings)
		}
	}()
	// In disaggregated-tiflash mode, need to consider generated column.
	tryExpandVirtualColumn(t.p)
	sender := PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_PassThrough,
	}.Init(ctx, t.p.StatsInfo())
	sender.SetChildren(t.p)

	p := PhysicalTableReader{
		TablePlan: sender,
		StoreType: kv.TiFlash,
	}.Init(ctx, t.p.QueryBlockOffset())
	p.SetStats(t.p.StatsInfo())
	collectPartitionInfosFromMPPPlan(p, t.p)
	rt = &RootTask{}
	rt.SetPlan(p)

	if len(t.RootTaskConds) > 0 {
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
		selectivity, _, err := cardinality.Selectivity(ctx, t.tblColHists, t.RootTaskConds, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = cost.SelectionFactor
		}
		sel := PhysicalSelection{Conditions: t.RootTaskConds}.Init(ctx, rt.GetPlan().StatsInfo().Scale(ctx.GetSessionVars(), selectivity), rt.GetPlan().QueryBlockOffset())
		sel.FromDataSource = true
		sel.SetChildren(rt.GetPlan())
		rt.SetPlan(sel)
	}
	return rt
}

// SetPlan sets the mpp task's plan.
func (t *MppTask) SetPlan(p base.PhysicalPlan) {
	t.p = p
}

// ************************************* MPPTask End ******************************************

// ************************************* CopTask Start ******************************************

// CopTask is a task that runs in a distributed kv store.
// TODO: In future, we should split copTask to indexTask and tableTask.
type CopTask struct {
	IndexPlan base.PhysicalPlan
	TablePlan base.PhysicalPlan
	// Whether tries to push down index lookup to TiKV and where this action comes
	IndexLookUpPushDownBy util.IndexLookUpPushDownByType
	// IndexPlanFinished means we have finished index plan.
	IndexPlanFinished bool
	// KeepOrder indicates if the plan scans data by order.
	KeepOrder bool
	// NeedExtraProj means an extra prune is needed because
	// in double read / index merge cases, they may output one more column for handle(row id).
	NeedExtraProj bool
	// OriginSchema is the target schema to be projected to when NeedExtraProj is true.
	OriginSchema *expression.Schema

	ExtraHandleCol   *expression.Column
	CommonHandleCols []*expression.Column
	// TblColHists stores the original stats of DataSource, it is used to get
	// average row width when computing network cost.
	TblColHists *statistics.HistColl
	// TblCols stores the original columns of DataSource before being pruned, it
	// is used to compute average row width when computing scan cost.
	TblCols []*expression.Column

	IdxMergePartPlans      []base.PhysicalPlan
	IdxMergeIsIntersection bool
	IdxMergeAccessMVIndex  bool

	// RootTaskConds stores select conditions containing virtual columns.
	// These conditions can't push to TiKV, so we have to add a selection for rootTask
	RootTaskConds []expression.Expression

	// For table partition.
	PhysPlanPartInfo *PhysPlanPartInfo

	// ExpectCnt is the expected row count of upper task, 0 for unlimited.
	// It's used for deciding whether using paging distsql.
	ExpectCnt uint64

	// For copTask and rootTask, when we compose physical tree bottom-up, index join need some special info
	// fetched from underlying ds which built index range or table range based on these runtime constant.
	IndexJoinInfo *IndexJoinInfo

	// Warnings passed through different task copy attached with more upper operator specific Warnings. (not concurrent safe)
	Warnings SimpleWarnings
}

// AppendWarning appends a warning
func (t *CopTask) AppendWarning(err error) {
	t.Warnings.AppendWarning(err)
}

// Invalid implements Task interface.
func (t *CopTask) Invalid() bool {
	return t.TablePlan == nil && t.IndexPlan == nil && len(t.IdxMergePartPlans) == 0
}

// Count implements Task interface.
func (t *CopTask) Count() float64 {
	if t.IndexPlanFinished {
		return t.TablePlan.StatsInfo().RowCount
	}
	return t.IndexPlan.StatsInfo().RowCount
}

// Copy implements Task interface.
func (t *CopTask) Copy() base.Task {
	nt := *t
	// since *t will reuse the same warnings slice, we need to copy it out.
	// cause different task instance should have different warning slice.
	nt.Warnings.Copy(&t.Warnings)
	return &nt
}

// Plan implements Task interface.
// copTask plan should be careful with indexMergeReader, whose real plan is stored in
// idxMergePartPlans, when its indexPlanFinished is marked with false.
func (t *CopTask) Plan() base.PhysicalPlan {
	if t.IndexPlanFinished {
		return t.TablePlan
	}
	return t.IndexPlan
}

// MemoryUsage return the memory usage of copTask
func (t *CopTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfInterface*(2+int64(cap(t.IdxMergePartPlans)+cap(t.RootTaskConds))) + size.SizeOfBool*3 + size.SizeOfUint64 +
		size.SizeOfPointer*(3+int64(cap(t.CommonHandleCols)+cap(t.TblCols))) + size.SizeOfSlice*4 + t.PhysPlanPartInfo.MemoryUsage()
	if t.IndexPlan != nil {
		sum += t.IndexPlan.MemoryUsage()
	}
	if t.TablePlan != nil {
		sum += t.TablePlan.MemoryUsage()
	}
	if t.OriginSchema != nil {
		sum += t.OriginSchema.MemoryUsage()
	}
	if t.ExtraHandleCol != nil {
		sum += t.ExtraHandleCol.MemoryUsage()
	}

	for _, col := range t.CommonHandleCols {
		sum += col.MemoryUsage()
	}
	for _, col := range t.TblCols {
		sum += col.MemoryUsage()
	}
	for _, p := range t.IdxMergePartPlans {
		sum += p.MemoryUsage()
	}
	for _, expr := range t.RootTaskConds {
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
		if t.Warnings.WarningCount() > 0 {
			rt.Warnings.CopyFrom(&t.Warnings)
		}
	}()
	// copTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistSQLScanConcurrency.
	t.FinishIndexPlan()
	// Network cost of transferring rows of table scan to TiDB.
	if t.TablePlan != nil {
		tp := t.TablePlan
		for len(tp.Children()) > 0 {
			tp = tp.Children()[0]
		}
		ts := tp.(*PhysicalTableScan)
		prevColumnLen := len(ts.Columns)
		prevSchema := ts.Schema().Clone()
		ts.Columns = ExpandVirtualColumn(ts.Columns, ts.Schema(), ts.Table.Columns)
		if !t.NeedExtraProj && len(ts.Columns) > prevColumnLen {
			// Add a projection to make sure not to output extract columns.
			t.NeedExtraProj = true
			t.OriginSchema = prevSchema
		}
	}
	newTask := &RootTask{}
	if t.IdxMergePartPlans != nil {
		p := PhysicalIndexMergeReader{
			PartialPlansRaw:    t.IdxMergePartPlans,
			TablePlan:          t.TablePlan,
			IsIntersectionType: t.IdxMergeIsIntersection,
			AccessMVIndex:      t.IdxMergeAccessMVIndex,
			KeepOrder:          t.KeepOrder,
		}.Init(ctx, t.IdxMergePartPlans[0].QueryBlockOffset())
		p.PlanPartInfo = t.PhysPlanPartInfo
		newTask.SetPlan(p)
		if t.NeedExtraProj {
			schema := t.OriginSchema
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.StatsInfo(), t.IdxMergePartPlans[0].QueryBlockOffset(), nil)
			proj.SetSchema(schema)
			proj.SetChildren(p)
			newTask.SetPlan(proj)
		}
		t.handleRootTaskConds(ctx, newTask)
		return newTask
	}
	if t.IndexPlan != nil && t.TablePlan != nil {
		newTask = BuildIndexLookUpTask(ctx, t)
	} else if t.IndexPlan != nil {
		p := PhysicalIndexReader{IndexPlan: t.IndexPlan}.Init(ctx, t.IndexPlan.QueryBlockOffset())
		p.PlanPartInfo = t.PhysPlanPartInfo
		p.SetStats(t.IndexPlan.StatsInfo())
		newTask.SetPlan(p)
	} else {
		tp := t.TablePlan
		for len(tp.Children()) > 0 {
			tp = tp.Children()[0]
		}
		ts := tp.(*PhysicalTableScan)
		p := PhysicalTableReader{
			TablePlan:      t.TablePlan,
			StoreType:      ts.StoreType,
			IsCommonHandle: ts.Table.IsCommonHandle,
		}.Init(ctx, t.TablePlan.QueryBlockOffset())
		p.PlanPartInfo = t.PhysPlanPartInfo
		p.SetStats(t.TablePlan.StatsInfo())

		// If agg was pushed down in Attach2Task(), the partial agg was placed on the top of tablePlan, the final agg was
		// placed above the PhysicalTableReader, and the schema should have been set correctly for them, the schema of
		// partial agg contains the columns needed by the final agg.
		// If we add the projection here, the projection will be between the final agg and the partial agg, then the
		// schema will be broken, the final agg will fail to find needed columns in ResolveIndices().
		// Besides, the agg would only be pushed down if it doesn't contain virtual columns, so virtual column should not be affected.
		aggPushedDown := false
		switch p.TablePlan.(type) {
		case *PhysicalHashAgg, *PhysicalStreamAgg:
			aggPushedDown = true
		}

		if t.NeedExtraProj && !aggPushedDown {
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(t.OriginSchema.Columns)}.Init(ts.SCtx(), ts.StatsInfo(), ts.QueryBlockOffset(), nil)
			proj.SetSchema(t.OriginSchema)
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
