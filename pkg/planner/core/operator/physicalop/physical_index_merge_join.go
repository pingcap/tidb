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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalIndexMergeJoin represents the plan of index look up merge join.
type PhysicalIndexMergeJoin struct {
	PhysicalIndexJoin

	// KeyOff2KeyOffOrderByIdx maps the offsets in join keys to the offsets in join keys order by index.
	KeyOff2KeyOffOrderByIdx []int
	// CompareFuncs store the compare functions for outer join keys and inner join key.
	CompareFuncs []expression.CompareFunc
	// OuterCompareFuncs store the compare functions for outer join keys and outer join
	// keys, it's for outer rows sort's convenience.
	OuterCompareFuncs []expression.CompareFunc
	// NeedOuterSort means whether outer rows should be sorted to build range.
	NeedOuterSort bool
	// Desc means whether inner child keep desc order.
	Desc bool
}

// Init initializes PhysicalIndexMergeJoin.
func (p PhysicalIndexMergeJoin) Init(ctx base.PlanContext) *PhysicalIndexMergeJoin {
	p.SetTP(plancodec.TypeIndexMergeJoin)
	p.SetID(int(ctx.GetSessionVars().PlanID.Add(1)))
	p.SetSCtx(ctx)
	p.Self = &p
	return &p
}

// MemoryUsage return the memory usage of PhysicalIndexMergeJoin
func (p *PhysicalIndexMergeJoin) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalIndexJoin.MemoryUsage() + size.SizeOfSlice*3 + int64(cap(p.KeyOff2KeyOffOrderByIdx))*size.SizeOfInt +
		int64(cap(p.CompareFuncs)+cap(p.OuterCompareFuncs))*size.SizeOfFunc + size.SizeOfBool*2
	return
}

// ExplainInfo implements Plan interface.
func (p *PhysicalIndexMergeJoin) ExplainInfo() string {
	return p.PhysicalIndexJoin.ExplainInfoInternal(false, true)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalIndexMergeJoin) ExplainNormalizedInfo() string {
	return p.PhysicalIndexJoin.ExplainInfoInternal(true, true)
}

// GetCost computes the cost of index merge join operator and its children.
func (p *PhysicalIndexMergeJoin) GetCost(outerCnt, innerCnt, outerCost, innerCost float64, costFlag uint64) float64 {
	return utilfuncp.GetCost4PhysicalIndexMergeJoin(p, outerCnt, innerCnt, outerCost, innerCost, costFlag)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalIndexMergeJoin) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalIndexMergeJoin(p, taskType, option)
}

// GetPlanCostVer2 implements PhysicalPlan interface.
func (p *PhysicalIndexMergeJoin) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetIndexJoinCostVer24PhysicalIndexJoin(&p.PhysicalIndexJoin, taskType, option, 2)
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalIndexMergeJoin) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalIndexMergeJoin(p, tasks...)
}
