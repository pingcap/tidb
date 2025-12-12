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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalUnionAll is the physical operator of UnionAll.
type PhysicalUnionAll struct {
	PhysicalSchemaProducer

	Mpp bool
}

// Init initializes PhysicalUnionAll.
func (p PhysicalUnionAll) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalUnionAll {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeUnion, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalUnionAll) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalUnionAll)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalUnionAll
func (p *PhysicalUnionAll) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.PhysicalSchemaProducer.MemoryUsage() + size.SizeOfBool
}

// Attach2Task implements base.PhysicalPlan interface.
func (p *PhysicalUnionAll) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalUnionAll(p, tasks...)
}

// GetPlanCostVer1 implements base.PhysicalPlan interface.
func (p *PhysicalUnionAll) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalUnionAll(p, taskType, option)
}

// GetPlanCostVer2 implements base.PhysicalPlan interface.
func (p *PhysicalUnionAll) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalUnionAll(p, taskType, option)
}

// ExhaustPhysicalPlans4LogicalUnionAll generates PhysicalUnionAll plans from LogicalUnionAll.
func ExhaustPhysicalPlans4LogicalUnionAll(p *logicalop.LogicalUnionAll, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	// TODO: UnionAll can not pass any order, but we can change it to sort merge to keep order.
	if !prop.IsSortItemEmpty() || (prop.IsFlashProp() && prop.TaskTp != property.MppTaskType) {
		return nil, true, nil
	}
	// TODO: UnionAll can pass partition info, but for briefness, we prevent it from pushing down.
	if prop.TaskTp == property.MppTaskType && prop.MPPPartitionTp != property.AnyType {
		return nil, true, nil
	}
	// when arrived here, operator itself has already checked checkOpSelfSatisfyPropTaskTypeRequirement, we only need to feel allowMPP here.
	canUseMpp := p.SCtx().GetSessionVars().IsMPPAllowed()
	chReqProps := make([]*property.PhysicalProperty, 0, p.ChildLen())
	for range p.Children() {
		if canUseMpp && prop.TaskTp == property.MppTaskType {
			chReqProps = append(chReqProps, &property.PhysicalProperty{
				ExpectedCnt:       prop.ExpectedCnt,
				TaskTp:            property.MppTaskType,
				CTEProducerStatus: prop.CTEProducerStatus,
				NoCopPushDown:     prop.NoCopPushDown,
			})
		} else {
			chReqProps = append(chReqProps, &property.PhysicalProperty{ExpectedCnt: prop.ExpectedCnt,
				CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown})
		}
	}
	ua := PhysicalUnionAll{
		Mpp: canUseMpp && prop.TaskTp == property.MppTaskType,
	}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
	ua.SetSchema(p.Schema())
	if canUseMpp && prop.TaskTp == property.RootTaskType {
		chReqProps = make([]*property.PhysicalProperty, 0, p.ChildLen())
		for range p.Children() {
			chReqProps = append(chReqProps, &property.PhysicalProperty{
				ExpectedCnt:       prop.ExpectedCnt,
				TaskTp:            property.MppTaskType,
				CTEProducerStatus: prop.CTEProducerStatus,
				NoCopPushDown:     prop.NoCopPushDown,
			})
		}
		mppUA := PhysicalUnionAll{Mpp: true}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), chReqProps...)
		mppUA.SetSchema(p.Schema())
		return []base.PhysicalPlan{ua, mppUA}, true, nil
	}
	return []base.PhysicalPlan{ua}, true, nil
}

// ExhaustPhysicalPlans4LogicalPartitionUnionAll generates PhysicalUnionAll plans from LogicalPartitionUnionAll.
func ExhaustPhysicalPlans4LogicalPartitionUnionAll(p *logicalop.LogicalPartitionUnionAll, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	uas, flagHint, err := ExhaustPhysicalPlans4LogicalUnionAll(&p.LogicalUnionAll, prop)
	if err != nil {
		return nil, false, err
	}
	for _, ua := range uas {
		ua.(*PhysicalUnionAll).SetTP(plancodec.TypePartitionUnion)
	}
	return uas, flagHint, nil
}
