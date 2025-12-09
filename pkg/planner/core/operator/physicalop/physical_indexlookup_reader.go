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
	"maps"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

// PhysicalIndexLookUpReader is the index look up reader in tidb. It's used in case of double reading.
type PhysicalIndexLookUpReader struct {
	PhysicalSchemaProducer

	// IndexLookUpPushDown indicates whether the index lookup should be pushed down.
	IndexLookUpPushDown bool

	IndexPlan base.PhysicalPlan
	TablePlan base.PhysicalPlan
	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []base.PhysicalPlan
	// IndexPlansUnNatureOrders is not empty if LookUpPushDown is true.
	// It indicates a map from childIndex => parentIndex if the parent is not located as the next of the child.
	IndexPlansUnNatureOrders map[int]int
	// TablePlans flats the tablePlan to construct executor pb.
	TablePlans []base.PhysicalPlan
	Paging     bool

	ExtraHandleCol *expression.Column
	// PushedLimit is used to avoid unnecessary table scan tasks of IndexLookUpReader.
	PushedLimit *PushedDownLimit

	CommonHandleCols []*expression.Column

	// Used by partition table.
	PlanPartInfo *PhysPlanPartInfo

	// required by cost calculation
	ExpectedCnt uint64
	KeepOrder   bool
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalIndexLookUpReader)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.IndexLookUpPushDown = p.IndexLookUpPushDown
	cloned.IndexPlansUnNatureOrders = maps.Clone(p.IndexPlansUnNatureOrders)
	if cloned.IndexPlans, err = ClonePhysicalPlan(newCtx, p.IndexPlans); err != nil {
		return nil, err
	}
	if cloned.TablePlans, err = ClonePhysicalPlan(newCtx, p.TablePlans); err != nil {
		return nil, err
	}
	if cloned.IndexPlan, err = p.IndexPlan.Clone(newCtx); err != nil {
		return nil, err
	}
	if cloned.TablePlan, err = p.TablePlan.Clone(newCtx); err != nil {
		return nil, err
	}
	if p.ExtraHandleCol != nil {
		cloned.ExtraHandleCol = p.ExtraHandleCol.Clone().(*expression.Column)
	}
	if p.PushedLimit != nil {
		cloned.PushedLimit = p.PushedLimit.Clone()
	}
	if len(p.CommonHandleCols) != 0 {
		cloned.CommonHandleCols = make([]*expression.Column, 0, len(p.CommonHandleCols))
		for _, col := range p.CommonHandleCols {
			cloned.CommonHandleCols = append(cloned.CommonHandleCols, col.Clone().(*expression.Column))
		}
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) ExtractCorrelatedCols() (corCols []*expression.CorrelatedColumn) {
	for _, child := range p.TablePlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	for _, child := range p.IndexPlans {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4PhysicalPlan(child)...)
	}
	return corCols
}

// GetIndexNetDataSize return the estimated total size in bytes via network transfer.
func (p *PhysicalIndexLookUpReader) GetIndexNetDataSize() float64 {
	return cardinality.GetAvgRowSize(p.SCtx(), GetTblStats(p.IndexPlan), p.IndexPlan.Schema().Columns, true, false) * p.IndexPlan.StatsCount()
}

// GetAvgTableRowSize return the average row size of each final row.
func (p *PhysicalIndexLookUpReader) GetAvgTableRowSize() float64 {
	return cardinality.GetAvgRowSize(p.SCtx(), GetTblStats(p.TablePlan), p.TablePlan.Schema().Columns, false, false)
}

// MemoryUsage return the memory usage of PhysicalIndexLookUpReader
func (p *PhysicalIndexLookUpReader) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() +
		size.SizeOfBool*3 +
		p.PlanPartInfo.MemoryUsage() +
		size.SizeOfUint64 +
		size.SizeOfInt*int64(len(p.IndexPlansUnNatureOrders))

	if p.IndexPlan != nil {
		sum += p.IndexPlan.MemoryUsage()
	}
	if p.TablePlan != nil {
		sum += p.TablePlan.MemoryUsage()
	}
	if p.ExtraHandleCol != nil {
		sum += p.ExtraHandleCol.MemoryUsage()
	}
	if p.PushedLimit != nil {
		sum += p.PushedLimit.MemoryUsage()
	}

	// since IndexPlans and TablePlans are the flats of indexPlan and tablePlan, so we don't count it
	for _, col := range p.CommonHandleCols {
		sum += col.MemoryUsage()
	}
	return
}

// LoadTableStats preloads the stats data for the physical table
func (p *PhysicalIndexLookUpReader) LoadTableStats(ctx sessionctx.Context) {
	ts := p.TablePlans[0].(*PhysicalTableScan)
	stats.LoadTableStats(ctx, ts.Table, ts.PhysicalTableID)
}

// AccessObject implements PartitionAccesser interface.
func (p *PhysicalIndexLookUpReader) AccessObject(sctx base.PlanContext) base.AccessObject {
	is := p.IndexPlans[0].(*PhysicalIndexScan)
	if !sctx.GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return access.DynamicPartitionAccessObjects(nil)
	}
	asName := ""
	if is.TableAsName != nil && len(is.TableAsName.O) > 0 {
		asName = is.TableAsName.O
	}
	res := GetDynamicAccessPartition(sctx, is.Table, p.PlanPartInfo, asName)
	if res == nil {
		return access.DynamicPartitionAccessObjects(nil)
	}
	return access.DynamicPartitionAccessObjects{res}
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexLookUpReader) ExplainInfo() string {
	var str strings.Builder
	// The children can be inferred by the relation symbol.
	if p.PushedLimit != nil {
		str.WriteString("limit embedded(offset:")
		str.WriteString(strconv.FormatUint(p.PushedLimit.Offset, 10))
		str.WriteString(", count:")
		str.WriteString(strconv.FormatUint(p.PushedLimit.Count, 10))
		str.WriteString(")")
	}
	return str.String()
}

// Init initializes PhysicalIndexLookUpReader.
func (p PhysicalIndexLookUpReader) Init(ctx base.PlanContext, offset int, tryPushDownIndexLookUp bool) *PhysicalIndexLookUpReader {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeIndexLookUp, &p, offset)
	p.SetSchema(p.TablePlan.Schema())
	p.SetStats(p.TablePlan.StatsInfo())
	if tryPushDownIndexLookUp {
		p.tryPushDownLookUp(ctx)
	}
	p.TablePlans = FlattenListPushDownPlan(p.TablePlan)
	p.IndexPlans, p.IndexPlansUnNatureOrders = FlattenTreePushDownPlan(p.IndexPlan)
	return &p
}

// tryPushDownLookUp tries to push down the index lookup to TiKV.
func (p *PhysicalIndexLookUpReader) tryPushDownLookUp(ctx base.PlanContext) {
	intest.Assert(!p.IndexLookUpPushDown)
	if p.KeepOrder {
		// Though most of the index-lookup push-down constraints should be checked in
		// `checkIndexLookUpPushDownSupported` if possible,
		// however, the keep order cannot be determined until the final plan is constructed.
		// So we have to check the keep order here, and if it is required, we should not push down it and use
		// the normal index-lookup instead.
		return
	}

	indexLookUpPlan, err := buildPushDownIndexLookUpPlan(ctx, p.IndexPlan, p.TablePlan, len(p.CommonHandleCols) > 0)
	if err != nil {
		// This should not happen, but if it happens, we just log a warning and continue to use the original plan.
		intest.AssertNoError(err)
		logutil.BgLogger().Warn("try to push down index lookup failed", zap.Error(err))
		return
	}
	p.IndexPlan = indexLookUpPlan
	// Currently, it's hard to estimate how many rows can be looked up locally when push-down.
	// So we just use the row count as 0 of tablePlan in TiDB side which displays all lookup
	// can be performed in the TiKV side.
	resetRowCountAsZeroRecursively(ctx.GetSessionVars(), p.TablePlan)
	// The status info of IndexLookupReader should be the same as indexPlan in the push-down mode if
	// all lookup can be performed in the TiKV side.
	p.SetStats(p.IndexPlan.StatsInfo())
	p.IndexLookUpPushDown = true
}

func resetRowCountAsZeroRecursively(vars *variable.SessionVars, p base.PhysicalPlan) {
	p.SetStats(p.StatsInfo().Scale(vars, 0))
	for _, child := range p.Children() {
		resetRowCountAsZeroRecursively(vars, child)
	}
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexLookUpReader) ResolveIndices() (err error) {
	return utilfuncp.ResolveIndices4PhysicalIndexLookUpReader(p)
}

// GetCost computes the cost of apply operator.
func (p *PhysicalIndexLookUpReader) GetCost(costFlag uint64) float64 {
	return utilfuncp.GetCost4PhysicalIndexLookUpReader(p, costFlag)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexLookUpReader) GetPlanCostVer1(taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalIndexLookUpReader(p, taskType, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
func (p *PhysicalIndexLookUpReader) GetPlanCostVer2(taskType property.TaskType,
	option *costusage.PlanCostOption, args ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalIndexLookUpReader(p, taskType, option, args...)
}

// BuildIndexLookUpTask builds a index look up task from a cop task.
func BuildIndexLookUpTask(ctx base.PlanContext, t *CopTask) *RootTask {
	newTask := &RootTask{}
	p := PhysicalIndexLookUpReader{
		TablePlan:        t.TablePlan,
		IndexPlan:        t.IndexPlan,
		ExtraHandleCol:   t.ExtraHandleCol,
		CommonHandleCols: t.CommonHandleCols,
		ExpectedCnt:      t.ExpectCnt,
		KeepOrder:        t.KeepOrder,
		PlanPartInfo:     t.PhysPlanPartInfo,
	}.Init(ctx, t.TablePlan.QueryBlockOffset(), t.IndexLookUpPushDown)
	// Do not inject the extra Projection even if t.needExtraProj is set, or the schema between the phase-1 agg and
	// the final agg would be broken. Please reference comments for the similar logic in
	// (*copTask).convertToRootTaskImpl() for the PhysicalTableReader case.
	// We need to refactor these logics.
	aggPushedDown := false
	switch p.TablePlan.(type) {
	case *PhysicalHashAgg, *PhysicalStreamAgg:
		aggPushedDown = true
	}

	if t.NeedExtraProj && !aggPushedDown {
		schema := t.OriginSchema
		proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.StatsInfo(), t.TablePlan.QueryBlockOffset(), nil)
		proj.SetSchema(schema)
		proj.SetChildren(p)
		newTask.SetPlan(proj)
	} else {
		newTask.SetPlan(p)
	}
	return newTask
}
