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
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalProjection is the physical operator of projection.
type PhysicalProjection struct {
	PhysicalSchemaProducer

	Exprs            []expression.Expression
	CalculateNoDelay bool

	// AvoidColumnEvaluator is ONLY used to avoid building columnEvaluator
	// for the expressions of Projection which is child of Union operator.
	// Related issue: TiDB#8141(https://github.com/pingcap/tidb/issues/8141)
	AvoidColumnEvaluator bool
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalProjection) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalProjection)
	*cloned = *p
	cloned.SetSCtx(newCtx)
	base, err := p.PhysicalSchemaProducer.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.PhysicalSchemaProducer = *base
	cloned.Exprs = util.CloneExprs(p.Exprs)
	return cloned, err
}

// ExtractCorrelatedCols implements op.PhysicalPlan interface.
func (p *PhysicalProjection) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.Exprs))
	for _, expr := range p.Exprs {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// MemoryUsage return the memory usage of PhysicalProjection
func (p *PhysicalProjection) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = p.BasePhysicalPlan.MemoryUsage() + size.SizeOfBool*2
	for _, expr := range p.Exprs {
		sum += expr.MemoryUsage()
	}
	return
}

// ExplainInfo implements Plan interface.
func (p *PhysicalProjection) ExplainInfo() string {
	evalCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	enableRedactLog := p.SCtx().GetSessionVars().EnableRedactLog
	exprStr := expression.ExplainExpressionList(evalCtx, p.Exprs, p.Schema(), enableRedactLog)
	if p.TiFlashFineGrainedShuffleStreamCount > 0 {
		exprStr += fmt.Sprintf(", stream_count: %d", p.TiFlashFineGrainedShuffleStreamCount)
	}
	return exprStr
}

// Init initializes PhysicalProjection.
func (p PhysicalProjection) Init(ctx base.PlanContext, stats *property.StatsInfo, offset int, props ...*property.PhysicalProperty) *PhysicalProjection {
	p.BasePhysicalPlan = NewBasePhysicalPlan(ctx, plancodec.TypeProj, &p, offset)
	p.SetChildrenReqProps(props)
	p.SetStats(stats)
	return &p
}

// ResolveIndices implements Plan interface.
func (p *PhysicalProjection) ResolveIndices() (err error) {
	return utilfuncp.ResolveIndices4PhysicalProjection(p)
}

// ExplainNormalizedInfo implements Plan interface.
func (p *PhysicalProjection) ExplainNormalizedInfo() string {
	if vardef.IgnoreInlistPlanDigest.Load() {
		return string(expression.SortedExplainExpressionListIgnoreInlist(p.Exprs))
	}
	return string(expression.SortedExplainNormalizedExpressionList(p.Exprs))
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalProjection) GetPlanCostVer1(taskType property.TaskType, option *optimizetrace.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalProjection(p, taskType, option)
}

// CloneForPlanCache implements the base.Plan interface.
func (p *PhysicalProjection) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalProjection)
	*cloned = *p
	basePlan, baseOK := p.PhysicalSchemaProducer.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.PhysicalSchemaProducer = *basePlan
	cloned.Exprs = utilfuncp.CloneExpressionsForPlanCache(p.Exprs, nil)
	return cloned, true
}

func (p *PhysicalProjection) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalProjection(p, taskType, option, isChildOfINL...)
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalProjection) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	exprs, err := expression.ProjectionExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.Exprs, client)
	if err != nil {
		return nil, err
	}
	projExec := &tipb.Projection{
		Exprs: exprs,
	}
	executorID := ""
	if !(storeType == kv.TiFlash || storeType == kv.TiKV) {
		return nil, errors.Errorf("the projection can only be pushed down to TiFlash or TiKV now, not %s", storeType.Name())
	}
	projExec.Child, err = p.Children()[0].ToPB(ctx, storeType)
	if err != nil {
		return nil, errors.Trace(err)
	}
	executorID = p.ExplainID().String()
	return &tipb.Executor{Tp: tipb.ExecType_TypeProjection, Projection: projExec, ExecutorId: &executorID}, nil
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalProjection) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalProjection(p, tasks...)
}
