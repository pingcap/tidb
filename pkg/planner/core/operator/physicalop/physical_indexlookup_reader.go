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
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/access"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// PhysicalIndexLookUpReader is the index look up reader in tidb. It's used in case of double reading.
type PhysicalIndexLookUpReader struct {
	PhysicalSchemaProducer

	IndexPlan base.PhysicalPlan
	TablePlan base.PhysicalPlan
	// IndexPlans flats the indexPlan to construct executor pb.
	IndexPlans []base.PhysicalPlan
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

// BuildPlanTrace implements op.PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) BuildPlanTrace() *tracing.PlanTrace {
	rp := p.BasePhysicalPlan.BuildPlanTrace()
	if p.IndexPlan != nil {
		rp.Children = append(rp.Children, p.IndexPlan.BuildPlanTrace())
	}
	if p.TablePlan != nil {
		rp.Children = append(rp.Children, p.TablePlan.BuildPlanTrace())
	}
	return rp
}

// AppendChildCandidate implements PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) AppendChildCandidate(op *optimizetrace.PhysicalOptimizeOp) {
	p.BasePhysicalPlan.AppendChildCandidate(op)
	if p.IndexPlan != nil {
		AppendChildCandidate(p, p.IndexPlan, op)
	}
	if p.TablePlan != nil {
		AppendChildCandidate(p, p.TablePlan, op)
	}
}

// MemoryUsage return the memory usage of PhysicalIndexLookUpReader
func (p *PhysicalIndexLookUpReader) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfBool*2 + p.PlanPartInfo.MemoryUsage() + size.SizeOfUint64

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
	utilfuncp.LoadTableStats(ctx, ts.Table, ts.PhysicalTableID)
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
func (p PhysicalIndexLookUpReader) Init(ctx base.PlanContext, offset int) *PhysicalIndexLookUpReader {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeIndexLookUp, &p, offset)
	p.TablePlans = FlattenPushDownPlan(p.TablePlan)
	p.IndexPlans = FlattenPushDownPlan(p.IndexPlan)
	p.SetSchema(p.TablePlan.Schema())
	return &p
}

// ResolveIndices implements Plan interface.
func (p *PhysicalIndexLookUpReader) ResolveIndices() (err error) {
	return utilfuncp.ResolveIndices4PhysicalIndexLookUpReader(p)
}

// CloneForPlanCache implements the base.Plan interface.
func (p *PhysicalIndexLookUpReader) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalIndexLookUpReader)
	*cloned = *p
	basePlan, baseOK := p.PhysicalSchemaProducer.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.PhysicalSchemaProducer = *basePlan
	if p.IndexPlan != nil {
		indexPlan, ok := p.IndexPlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.IndexPlan = indexPlan.(base.PhysicalPlan)
	}
	if p.TablePlan != nil {
		tablePlan, ok := p.TablePlan.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		cloned.TablePlan = tablePlan.(base.PhysicalPlan)
	}
	cloned.IndexPlans = FlattenPushDownPlan(cloned.IndexPlan)
	cloned.TablePlans = FlattenPushDownPlan(cloned.TablePlan)
	if p.ExtraHandleCol != nil {
		if p.ExtraHandleCol.SafeToShareAcrossSession() {
			cloned.ExtraHandleCol = p.ExtraHandleCol
		} else {
			cloned.ExtraHandleCol = p.ExtraHandleCol.Clone().(*expression.Column)
		}
	}
	cloned.PushedLimit = p.PushedLimit.Clone()
	cloned.CommonHandleCols = utilfuncp.CloneColumnsForPlanCache(p.CommonHandleCols, nil)
	cloned.PlanPartInfo = p.PlanPartInfo.CloneForPlanCache()
	return cloned, true
}

// GetCost computes the cost of apply operator.
func (p *PhysicalIndexLookUpReader) GetCost(costFlag uint64) float64 {
	return utilfuncp.GetCost4PhysicalIndexLookUpReader(p, costFlag)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexLookUpReader) GetPlanCostVer1(taskType property.TaskType,
	option *optimizetrace.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalIndexLookUpReader(p, taskType, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
func (p *PhysicalIndexLookUpReader) GetPlanCostVer2(taskType property.TaskType,
	option *optimizetrace.PlanCostOption, args ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalIndexLookUpReader(p, taskType, option, args...)
}
