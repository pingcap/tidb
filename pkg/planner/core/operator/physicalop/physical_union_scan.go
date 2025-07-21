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
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// PhysicalUnionScan represents a union scan operator.
type PhysicalUnionScan struct {
	BasePhysicalPlan

	Conditions []expression.Expression

	HandleCols util.HandleCols
}

// Init initializes PhysicalUnionScan.
func (p PhysicalUnionScan) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalUnionScan {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeUnionScan, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalUnionScan) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0)
	for _, cond := range p.Conditions {
		corCols = append(corCols, expression.ExtractCorColumns(cond)...)
	}
	return corCols
}

// MemoryUsage return the memory usage of PhysicalUnionScan
func (p *PhysicalUnionScan) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfSlice
	if p.HandleCols != nil {
		sum += p.HandleCols.MemoryUsage()
	}
	for _, cond := range p.Conditions {
		sum += cond.MemoryUsage()
	}
	return
}

// ExplainInfo implements Plan interface.
func (p *PhysicalUnionScan) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.SCtx().GetExprCtx().GetEvalCtx(), p.Conditions))
}

// CloneForPlanCache implements the base.Plan interface.
func (p *PhysicalUnionScan) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalUnionScan)
	*cloned = *p
	basePlan, baseOK := p.BasePhysicalPlan.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.BasePhysicalPlan = *basePlan
	cloned.Conditions = utilfuncp.CloneExpressionsForPlanCache(p.Conditions, nil)
	if p.HandleCols != nil {
		cloned.HandleCols = p.HandleCols.Clone()
	}
	return cloned, true
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalUnionScan) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalUnionScan(p, tasks...)
}

// ResolveIndices implements Plan interface.
func (p *PhysicalUnionScan) ResolveIndices() (err error) {
	return utilfuncp.ResolveIndices4PhysicalUnionScan(p)
}
