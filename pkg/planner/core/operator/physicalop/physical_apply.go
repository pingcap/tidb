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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalApply represents apply plan, only used for subquery.
type PhysicalApply struct {
	PhysicalHashJoin

	CanUseCache bool
	Concurrency int
	OuterSchema []*expression.CorrelatedColumn
}

// Init initializes PhysicalApply.
func (p PhysicalApply) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalApply {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeApply, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalApply) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalApply(p, tasks...)
}

// PhysicalJoinImplement has an extra bool return value compared with PhysicalJoin interface.
// This will override BasePhysicalJoin.PhysicalJoinImplement() and make PhysicalApply not an implementation of
// base.PhysicalJoin interface.
func (*PhysicalApply) PhysicalJoinImplement() bool { return false }

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalApply) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalApply)
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalHashJoin.Clone(newCtx)
	if err != nil {
		return nil, err
	}
	hj := base.(*PhysicalHashJoin)
	cloned.PhysicalHashJoin = *hj
	cloned.CanUseCache = p.CanUseCache
	cloned.Concurrency = p.Concurrency
	for _, col := range p.OuterSchema {
		cloned.OuterSchema = append(cloned.OuterSchema, col.Clone().(*expression.CorrelatedColumn))
	}
	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalApply) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := p.PhysicalHashJoin.ExtractCorrelatedCols()
	return slices.DeleteFunc(corCols, func(col *expression.CorrelatedColumn) bool {
		return p.Children()[0].Schema().Contains(&col.Column)
	})
}

// GetCost computes the cost of apply operator.
func (p *PhysicalApply) GetCost(lCount, rCount, lCost, rCost float64) float64 {
	return utilfuncp.GetCost4PhysicalApply(p, lCount, rCount, lCost, rCost)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalApply) GetPlanCostVer1(taskType property.TaskType,
	option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalApply(p, taskType, option)
}

// GetPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
func (p *PhysicalApply) GetPlanCostVer2(taskType property.TaskType,
	option *costusage.PlanCostOption, _ ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalApply(p, taskType, option)
}

// MemoryUsage return the memory usage of PhysicalApply
func (p *PhysicalApply) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.PhysicalHashJoin.MemoryUsage() + size.SizeOfBool + size.SizeOfBool + size.SizeOfSlice +
		int64(cap(p.OuterSchema))*size.SizeOfPointer
	for _, corrCol := range p.OuterSchema {
		sum += corrCol.MemoryUsage()
	}
	return
}

// ResolveIndices implements Plan interface.
func (p *PhysicalApply) ResolveIndices() (err error) {
	err = p.PhysicalHashJoin.ResolveIndices()
	if err != nil {
		return err
	}
	// p.OuterSchema may have duplicated CorrelatedColumns,
	// we deduplicate it here.
	dedupCols := make(map[int64]*expression.CorrelatedColumn, len(p.OuterSchema))
	for _, col := range p.OuterSchema {
		dedupCols[col.UniqueID] = col
	}
	p.OuterSchema = make([]*expression.CorrelatedColumn, 0, len(dedupCols))
	for _, col := range dedupCols {
		newCol, _, err := col.Column.ResolveIndices(p.Children()[0].Schema(), false)
		if err != nil {
			return err
		}
		col.Column = *newCol.(*expression.Column)
		p.OuterSchema = append(p.OuterSchema, col)
	}
	// Resolve index for equal conditions again, because apply is different from
	// hash join on the fact that equal conditions are evaluated against the join result,
	// so columns from equal conditions come from merged schema of children, instead of
	// single child's schema.
	joinedSchema := expression.MergeSchema(p.Children()[0].Schema(), p.Children()[1].Schema())
	for i, cond := range p.PhysicalHashJoin.EqualConditions {
		// todo double check if it can allowLazyCopy?
		newSf, _, err := cond.ResolveIndices(joinedSchema, false)
		if err != nil {
			return err
		}
		p.PhysicalHashJoin.EqualConditions[i] = newSf.(*expression.ScalarFunction)
	}
	for i, cond := range p.PhysicalHashJoin.NAEqualConditions {
		newSf, _, err := cond.ResolveIndices(joinedSchema, false)
		if err != nil {
			return err
		}
		p.PhysicalHashJoin.NAEqualConditions[i] = newSf.(*expression.ScalarFunction)
	}
	return
}
