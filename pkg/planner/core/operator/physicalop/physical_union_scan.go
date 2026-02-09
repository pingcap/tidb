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
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
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

// ExhaustPhysicalPlans4LogicalUnionScan will be called by LogicalUnionScan in logicalOp pkg.
func ExhaustPhysicalPlans4LogicalUnionScan(p *logicalop.LogicalUnionScan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	if prop.IsFlashProp() {
		p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
			"MPP mode may be blocked because operator `UnionScan` is not supported now.")
		return nil, true, nil
	}
	childProp := prop.CloneEssentialFields()
	childProp = admitIndexJoinProp(childProp, prop, true)
	if childProp == nil {
		// even hint can not work with this. index join prop is not satisfied in mpp task type.
		return nil, false, nil
	}
	// here we just pass down the keep order property to the child.
	// cuz, in union scan exec, it will feel the underlying tableReader or indexReader to get the keepOrder.
	us := PhysicalUnionScan{
		Conditions: p.Conditions,
		HandleCols: p.HandleCols,
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset(), childProp)
	return []base.PhysicalPlan{us}, true, nil
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

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalUnionScan) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalUnionScan(p, tasks...)
}

// ResolveIndices implements Plan interface.
func (p *PhysicalUnionScan) ResolveIndices() (err error) {
	return utilfuncp.ResolveIndices4PhysicalUnionScan(p)
}
