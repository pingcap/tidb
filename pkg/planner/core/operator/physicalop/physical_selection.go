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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalSelection represents a filter.
type PhysicalSelection struct {
	BasePhysicalPlan

	Conditions []expression.Expression

	// The flag indicates whether this Selection is from a DataSource.
	// The flag is only used by cost model for compatibility and will be removed later.
	// Please see https://github.com/pingcap/tidb/issues/36243 for more details.
	FromDataSource bool

	// todo Since the feature of adding filter operators has not yet been implemented,
	// the following code for this function will not be used for now.
	// The flag indicates whether this Selection is used for RuntimeFilter
	// True: Used for RuntimeFilter
	// False: Only for normal conditions
	// hasRFConditions bool
}

// ExhaustPhysicalPlans4LogicalSelection will be called by LogicalSelection in logicalOp pkg.
func ExhaustPhysicalPlans4LogicalSelection(p *logicalop.LogicalSelection, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	newProps := make([]*property.PhysicalProperty, 0, 2)
	childProp := prop.CloneEssentialFields()
	newProps = append(newProps, childProp)
	// we lift the p.CanPushDown(kv.TiFlash) check here, which may depend on the children.
	canPushDownToTiFlash := !expression.ContainVirtualColumn(p.Conditions) &&
		expression.CanExprsPushDown(util.GetPushDownCtx(p.SCtx()), p.Conditions, kv.TiFlash)

	if prop.TaskTp != property.MppTaskType &&
		p.SCtx().GetSessionVars().IsMPPAllowed() &&
		canPushDownToTiFlash {
		childPropMpp := prop.CloneEssentialFields()
		childPropMpp.TaskTp = property.MppTaskType
		newProps = append(newProps, childPropMpp)
	}

	ret := make([]base.PhysicalPlan, 0, len(newProps))
	newProps = admitIndexJoinProps(newProps, prop)
	for _, newProp := range newProps {
		sel := PhysicalSelection{
			Conditions: p.Conditions,
		}.Init(p.SCtx(), p.StatsInfo().ScaleByExpectCnt(p.SCtx().GetSessionVars(), prop.ExpectedCnt), p.QueryBlockOffset(), newProp)
		ret = append(ret, sel)
	}
	return ret, true, nil
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalSelection) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalSelection)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalPlan.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalPlan = *base
	cloned.Conditions = util.CloneExprs(p.Conditions)
	return cloned, nil
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalSelection) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.Conditions))
	for _, cond := range p.Conditions {
		corCols = append(corCols, expression.ExtractCorColumns(cond)...)
	}
	return corCols
}

// MemoryUsage return the memory usage of PhysicalSelection
func (p *PhysicalSelection) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfBool
	for _, expr := range p.Conditions {
		sum += expr.MemoryUsage()
	}
	return
}

// ExplainInfo implements Plan interface.
func (p *PhysicalSelection) ExplainInfo() string {
	exprStr := string(expression.SortedExplainExpressionList(p.SCtx().GetExprCtx().GetEvalCtx(), p.Conditions))
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		exprStr += fmt.Sprintf(", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return exprStr
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalSelection) ExplainNormalizedInfo() string {
	if vardef.IgnoreInlistPlanDigest.Load() {
		return string(expression.SortedExplainExpressionListIgnoreInlist(p.Conditions))
	}
	return string(expression.SortedExplainNormalizedExpressionList(p.Conditions))
}

// Init initializes PhysicalSelection.
func (p PhysicalSelection) Init(ctx base.PlanContext, stats *property.StatsInfo, qbOffset int, props ...*property.PhysicalProperty) *PhysicalSelection {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeSel, &p, qbOffset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// GetPlanCostVer2 implements the base.PhysicalPlan interface.
func (p *PhysicalSelection) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalSelection(p, taskType, option, isChildOfINL...)
}

// ResolveIndices implements base.PhysicalPlan interface.
func (p *PhysicalSelection) ResolveIndices() (err error) {
	return utilfuncp.ResolveIndices4PhysicalSelection(p)
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalSelection) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalSelection(p, tasks...)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalSelection) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalSelection(p, taskType, option)
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalSelection) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	conditions, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.Conditions, client)
	if err != nil {
		return nil, err
	}
	selExec := &tipb.Selection{
		Conditions: conditions,
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		selExec.Child, err = p.Children()[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeSelection, Selection: selExec, ExecutorId: &executorID}, nil
}
