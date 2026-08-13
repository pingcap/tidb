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
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalIndexHashJoin represents the plan of index look up hash join.
type PhysicalIndexHashJoin struct {
	PhysicalIndexJoin
	// KeepOuterOrder indicates whether keeping the output result order as the
	// outer side.
	KeepOuterOrder bool
}

// Init initializes PhysicalIndexHashJoin.
func (p PhysicalIndexHashJoin) Init(ctx base.PlanContext) *PhysicalIndexHashJoin {
	p.SetTP(plancodec.TypeIndexHashJoin)
	p.SetID(int(ctx.GetSessionVars().PlanID.Add(1)))
	p.SetSCtx(ctx)
	p.Self = &p
	return &p
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalIndexHashJoin) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalIndexHashJoin)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalJoin.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalJoin = *base
	physicalIndexJoin, err := p.PhysicalIndexJoin.Clone(newCtx)
	if err != nil {
		return nil, err
	}
	indexJoin, ok := physicalIndexJoin.(*PhysicalIndexJoin)
	intest.Assert(ok)
	cloned.PhysicalIndexJoin = *indexJoin
	cloned.KeepOuterOrder = p.KeepOuterOrder
	return cloned, nil
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalIndexHashJoin) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalIndexHashJoin(p, tasks...)
}

// GetCost computes the cost of index merge join operator and its children.
func (p *PhysicalIndexHashJoin) GetCost(outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64 {
	return utilfuncp.GetCost4PhysicalIndexHashJoin(p, outerCnt, innerCnt, outerCost, innerCost, costFlag)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexHashJoin) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer1PhysicalIndexHashJoin(p, taskType, option)
}

// GetPlanCostVer2 implements PhysicalPlan interface.
func (p *PhysicalIndexHashJoin) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption,
	_ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetIndexJoinCostVer24PhysicalIndexJoin(&p.PhysicalIndexJoin, taskType, option, 1)
}

// MemoryUsage return the memory usage of PhysicalIndexHashJoin
func (p *PhysicalIndexHashJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.PhysicalIndexJoin.MemoryUsage() + size.SizeOfBool
}
