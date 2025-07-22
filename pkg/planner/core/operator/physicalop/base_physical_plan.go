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

package physicalop

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/baseimpl"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
)

// BasePhysicalPlan is the common structure that used in physical plan.
type BasePhysicalPlan struct {
	baseimpl.Plan

	childrenReqProps []*property.PhysicalProperty `plan-cache-clone:"shallow"`
	Self             base.PhysicalPlan
	children         []base.PhysicalPlan

	// used by the new cost interface
	PlanCostInit bool
	PlanCost     float64
	PlanCostVer2 costusage.CostVer2 `plan-cache-clone:"shallow"`

	// probeParents records the IndexJoins and Applys with this operator in their inner children.
	// Please see comments in op.PhysicalPlan for details.
	probeParents []base.PhysicalPlan `plan-cache-clone:"shallow"`

	// Only for MPP. If TiFlashFineGrainedShuffleStreamCount > 0:
	// 1. For ExchangeSender, means its output will be partitioned by hash key.
	// 2. For ExchangeReceiver/Window/Sort, means its input is already partitioned.
	TiFlashFineGrainedShuffleStreamCount uint64
}

// ******************************* start implementation of Plan interface *******************************

// ExplainInfo implements Plan ExplainInfo interface.
func (*BasePhysicalPlan) ExplainInfo() string {
	return ""
}

// Schema implements Plan Schema interface.
func (p *BasePhysicalPlan) Schema() *expression.Schema {
	return p.children[0].Schema()
}

// BuildPlanTrace implements Plan BuildPlanTrace interface.
func (p *BasePhysicalPlan) BuildPlanTrace() *tracing.PlanTrace {
	tp := ""
	info := ""
	if p.Self != nil {
		tp = p.Self.TP()
		info = p.Self.ExplainInfo()
	}

	planTrace := &tracing.PlanTrace{ID: p.ID(), TP: tp, ExplainInfo: info}
	for _, child := range p.Children() {
		planTrace.Children = append(planTrace.Children, child.BuildPlanTrace())
	}
	return planTrace
}

// ******************************* end implementation of Plan interface *********************************

// *************************** start implementation of PhysicalPlan interface ***************************

// GetPlanCostVer1 implements the base.PhysicalPlan.<0th> interface.
// which calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *BasePhysicalPlan) GetPlanCostVer1(taskType property.TaskType, option *optimizetrace.PlanCostOption) (float64, error) {
	costFlag := option.CostFlag
	if p.PlanCostInit && !costusage.HasCostFlag(costFlag, costusage.CostFlagRecalculate) {
		// just calculate the cost once and always reuse it
		return p.PlanCost, nil
	}
	p.PlanCost = 0 // the default implementation, the operator have no cost
	for _, child := range p.children {
		childCost, err := child.GetPlanCostVer1(taskType, option)
		if err != nil {
			return 0, err
		}
		p.PlanCost += childCost
	}
	p.PlanCostInit = true
	return p.PlanCost, nil
}

// GetPlanCostVer2 implements the base.PhysicalPlan.<1st> interface.
// which calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *BasePhysicalPlan) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption) (costusage.CostVer2, error) {
	if p.PlanCostInit && !costusage.HasCostFlag(option.CostFlag, costusage.CostFlagRecalculate) {
		return p.PlanCostVer2, nil
	}
	childCosts := make([]costusage.CostVer2, 0, len(p.children))
	for _, child := range p.children {
		childCost, err := child.GetPlanCostVer2(taskType, option)
		if err != nil {
			return costusage.ZeroCostVer2, err
		}
		childCosts = append(childCosts, childCost)
	}
	if len(childCosts) == 0 {
		p.PlanCostVer2 = costusage.NewZeroCostVer2(costusage.TraceCost(option))
	} else {
		p.PlanCostVer2 = costusage.SumCostVer2(childCosts...)
	}
	p.PlanCostInit = true
	return p.PlanCostVer2, nil
}

// Attach2Task implements the base.PhysicalPlan.<2nd> interface.
func (p *BasePhysicalPlan) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].ConvertToRootTask(p.SCtx())
	return utilfuncp.AttachPlan2Task(p.Self, t)
}

// ToPB implements the base.PhysicalPlan.<3rd> interface.
func (p *BasePhysicalPlan) ToPB(_ *base.BuildPBContext, _ kv.StoreType) (*tipb.Executor, error) {
	return nil, errors.Errorf("plan %s fails converts to PB", p.Plan.ExplainID())
}

// GetChildReqProps implements the base.PhysicalPlan.<4th> interface.
func (p *BasePhysicalPlan) GetChildReqProps(idx int) *property.PhysicalProperty {
	return p.childrenReqProps[idx]
}

// StatsCount implements the base.PhysicalPlan.<5th> interface.
func (p *BasePhysicalPlan) StatsCount() float64 {
	return p.StatsInfo().RowCount
}

// ExtractCorrelatedCols implements the base.PhysicalPlan.<6th> interface.
func (*BasePhysicalPlan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	return nil
}

// Children implements the base.PhysicalPlan.<7th> interface.
func (p *BasePhysicalPlan) Children() []base.PhysicalPlan {
	return p.children
}

// SetChildren implements the base.PhysicalPlan.<8th> interface.
func (p *BasePhysicalPlan) SetChildren(children ...base.PhysicalPlan) {
	p.children = children
}

// SetChild implements the base.PhysicalPlan.<9th> interface.
func (p *BasePhysicalPlan) SetChild(i int, child base.PhysicalPlan) {
	p.children[i] = child
}

// ResolveIndices implements the base.PhysicalPlan.<10th> interface.
func (p *BasePhysicalPlan) ResolveIndices() (err error) {
	for _, child := range p.children {
		err = child.ResolveIndices()
		if err != nil {
			return err
		}
	}
	return
}

// StatsInfo inherits the BasePhysicalPlan.Plan's implementation for <11th>.

// SetStats inherits the BasePhysicalPlan.Plan's implementation for <12th>.

// ExplainNormalizedInfo implements the base.PhysicalPlan.<13th> interface.
func (*BasePhysicalPlan) ExplainNormalizedInfo() string {
	return ""
}

// Clone implements the base.PhysicalPlan.<14th> interface.
func (p *BasePhysicalPlan) Clone(base.PlanContext) (base.PhysicalPlan, error) {
	return nil, errors.Errorf("%T doesn't support cloning", p.Self)
}

// AppendChildCandidate implements the base.PhysicalPlan.<15th> interface.
func (p *BasePhysicalPlan) AppendChildCandidate(op *optimizetrace.PhysicalOptimizeOp) {
	if len(p.Children()) < 1 {
		return
	}
	childrenID := make([]int, 0)
	for _, child := range p.Children() {
		childCandidate := &tracing.CandidatePlanTrace{
			PlanTrace: &tracing.PlanTrace{TP: child.TP(), ID: child.ID(),
				ExplainInfo: child.ExplainInfo()},
		}
		op.AppendCandidate(childCandidate)
		child.AppendChildCandidate(op)
		childrenID = append(childrenID, child.ID())
	}
	op.GetTracer().Candidates[p.ID()].PlanTrace.AppendChildrenID(childrenID...)
}

// MemoryUsage implements the base.PhysicalPlan.<16th> interface.
func (p *BasePhysicalPlan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.Plan.MemoryUsage() + size.SizeOfSlice + int64(cap(p.childrenReqProps))*size.SizeOfPointer +
		size.SizeOfSlice + int64(cap(p.children)+1)*size.SizeOfInterface + size.SizeOfFloat64 +
		size.SizeOfUint64 + size.SizeOfBool

	for _, prop := range p.childrenReqProps {
		sum += prop.MemoryUsage()
	}
	for _, plan := range p.children {
		sum += plan.MemoryUsage()
	}
	return
}

// SetProbeParents implements base.PhysicalPlan.<17th> interface.
func (p *BasePhysicalPlan) SetProbeParents(probeParents []base.PhysicalPlan) {
	p.probeParents = probeParents
}

// GetEstRowCountForDisplay implements base.PhysicalPlan.<18th> interface.
func (p *BasePhysicalPlan) GetEstRowCountForDisplay() float64 {
	if p == nil {
		return 0
	}
	return p.StatsInfo().RowCount * utilfuncp.GetEstimatedProbeCntFromProbeParents(p.probeParents)
}

// GetActualProbeCnt implements base.PhysicalPlan.<19th> interface.
func (p *BasePhysicalPlan) GetActualProbeCnt(statsColl *execdetails.RuntimeStatsColl) int64 {
	if p == nil {
		return 1
	}
	return utilfuncp.GetActualProbeCntFromProbeParents(p.probeParents, statsColl)
}

// *************************** end implementation of PhysicalPlan interface *****************************

// CloneForPlanCacheWithSelf clones the plan with new self.
func (p *BasePhysicalPlan) CloneForPlanCacheWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*BasePhysicalPlan, bool) {
	cloned := new(BasePhysicalPlan)
	*cloned = *p
	cloned.SetSCtx(newCtx)
	cloned.Self = newSelf
	cloned.children = make([]base.PhysicalPlan, 0, len(p.children))
	for _, child := range p.children {
		clonedChild, ok := child.CloneForPlanCache(newCtx)
		if !ok {
			return nil, false
		}
		clonedPP, ok := clonedChild.(base.PhysicalPlan)
		if !ok {
			return nil, false
		}
		cloned.children = append(cloned.children, clonedPP)
	}
	return cloned, true
}

// CloneWithSelf clones the plan with new self.
func (p *BasePhysicalPlan) CloneWithSelf(newCtx base.PlanContext, newSelf base.PhysicalPlan) (*BasePhysicalPlan, error) {
	base := &BasePhysicalPlan{
		Plan:                                 p.Plan,
		Self:                                 newSelf,
		TiFlashFineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		probeParents:                         p.probeParents,
	}
	base.SetSCtx(newCtx)
	for _, child := range p.children {
		cloned, err := child.Clone(newCtx)
		if err != nil {
			return nil, err
		}
		base.children = append(base.children, cloned)
	}
	for _, prop := range p.childrenReqProps {
		if prop == nil {
			continue
		}
		base.childrenReqProps = append(base.childrenReqProps, prop.CloneEssentialFields())
	}
	return base, nil
}

// SetChildrenReqProps set the BasePhysicalPlan's childrenReqProps.
func (p *BasePhysicalPlan) SetChildrenReqProps(reqProps []*property.PhysicalProperty) {
	p.childrenReqProps = reqProps
}

// SetXthChildReqProps set the BasePhysicalPlan's x-th child as required property.
func (p *BasePhysicalPlan) SetXthChildReqProps(x int, reqProps *property.PhysicalProperty) {
	p.childrenReqProps[x] = reqProps
}

// NewBasePhysicalPlan creates a new BasePhysicalPlan.
func NewBasePhysicalPlan(ctx base.PlanContext, tp string, self base.PhysicalPlan, offset int) BasePhysicalPlan {
	return BasePhysicalPlan{
		Plan: baseimpl.NewBasePlan(ctx, tp, offset),
		Self: self,
	}
}
