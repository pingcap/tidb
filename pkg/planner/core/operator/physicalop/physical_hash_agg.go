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
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	util2 "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tipb/go-tipb"
	"math"
)

// PhysicalHashAgg is hash operator of aggregate.
type PhysicalHashAgg struct {
	BasePhysicalAgg
	TiflashPreAggMode string
}

// GetPointer return the base physical agg.
func (p *PhysicalHashAgg) GetPointer() *BasePhysicalAgg {
	return &p.BasePhysicalAgg
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalHashAgg) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalHashAgg)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalAgg.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalAgg = *base
	cloned.TiflashPreAggMode = p.TiflashPreAggMode
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalHashAgg
func (p *PhysicalHashAgg) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.BasePhysicalAgg.MemoryUsage()
}

// CloneForPlanCache implements the base.Plan interface.
func (op *PhysicalHashAgg) CloneForPlanCache(newCtx base.PlanContext) (base.Plan, bool) {
	cloned := new(PhysicalHashAgg)
	*cloned = *op
	basePlan, baseOK := op.BasePhysicalAgg.CloneForPlanCacheWithSelf(newCtx, cloned)
	if !baseOK {
		return nil, false
	}
	cloned.BasePhysicalAgg = *basePlan
	return cloned, true
}

// CpuCostDivisor computes the concurrency to which we would amortize CPU cost
// for hash aggregation.
func (p *PhysicalHashAgg) CpuCostDivisor(hasDistinct bool) (divisor, con float64) {
	if hasDistinct {
		return 0, 0
	}
	sessionVars := p.SCtx().GetSessionVars()
	finalCon, partialCon := sessionVars.HashAggFinalConcurrency(), sessionVars.HashAggPartialConcurrency()
	// According to `ValidateSetSystemVar`, `finalCon` and `partialCon` cannot be less than or equal to 0.
	if finalCon == 1 && partialCon == 1 {
		return 0, 0
	}
	// It is tricky to decide which concurrency we should use to amortize CPU cost. Since cost of hash
	// aggregation is tend to be under-estimated as explained in `attach2Task`, we choose the smaller
	// concurrecy to make some compensation.
	return math.Min(float64(finalCon), float64(partialCon)), float64(finalCon + partialCon)
}

// GetCost computes the cost of hash aggregation considering CPU/memory.
func (p *PhysicalHashAgg) GetCost(inputRows float64, isRoot, isMPP bool, costFlag uint64) float64 {
	return utilfuncp.GetCost4PhysicalHashAgg(p, inputRows, isRoot, isMPP, costFlag)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalHashAgg) GetPlanCostVer1(taskType property.TaskType, option *optimizetrace.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalHashAgg(p, taskType, option)
}

// Attach2Task implements the PhysicalPlan interface.
func (p *PhysicalHashAgg) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalHashAgg(p, tasks...)
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalHashAgg) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	groupByExprs, err := expression.ExpressionsToPBList(ctx.GetExprCtx().GetEvalCtx(), p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggExec := &tipb.Aggregation{
		GroupBy: groupByExprs,
	}
	pushDownCtx := util2.GetPushDownCtx(p.SCtx())
	for _, aggFunc := range p.AggFuncs {
		agg, err := aggregation.AggFuncToPBExpr(pushDownCtx, aggFunc, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		aggExec.AggFunc = append(aggExec.AggFunc, agg)
	}
	executorID := ""
	if storeType == kv.TiFlash {
		var err error
		aggExec.Child, err = p.Children()[0].ToPB(ctx, storeType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		executorID = p.ExplainID().String()
		// If p.TiflashPreAggMode is empty, means no need to consider preagg mode.
		// For example it's the the second stage of hashagg.
		if len(p.TiflashPreAggMode) != 0 {
			if preAggModeVal, ok := variable.ToTiPBTiFlashPreAggMode(p.TiflashPreAggMode); !ok {
				err = fmt.Errorf("unexpected tiflash pre agg mode: %v", p.TiflashPreAggMode)
			} else {
				aggExec.PreAggMode = &preAggModeVal
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return &tipb.Executor{
		Tp:                            tipb.ExecType_TypeAggregation,
		Aggregation:                   aggExec,
		ExecutorId:                    &executorID,
		FineGrainedShuffleStreamCount: p.TiFlashFineGrainedShuffleStreamCount,
		FineGrainedShuffleBatchSize:   ctx.TiFlashFineGrainedShuffleBatchSize,
	}, nil
}

// GetPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalHashAgg) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalHashAgg(p, taskType, option, isChildOfINL...)
}

// NewPhysicalHashAgg creates a new PhysicalHashAgg from a LogicalAggregation.
func NewPhysicalHashAgg(la *logicalop.LogicalAggregation, newStats *property.StatsInfo, prop *property.PhysicalProperty) *PhysicalHashAgg {
	newGbyItems := make([]expression.Expression, len(la.GroupByItems))
	copy(newGbyItems, la.GroupByItems)
	newAggFuncs := make([]*aggregation.AggFuncDesc, len(la.AggFuncs))
	// There's some places that rewrites the aggFunc in-place.
	// I clone it first.
	// It needs a well refactor to make sure that the physical optimize should not change the things of logical plan.
	// It's bad for cascades
	for i, aggFunc := range la.AggFuncs {
		newAggFuncs[i] = aggFunc.Clone()
	}
	agg := &BasePhysicalAgg{
		GroupByItems: newGbyItems,
		AggFuncs:     newAggFuncs,
	}
	hashAgg := agg.InitForHash(la.SCtx(), newStats, la.QueryBlockOffset(), nil, prop)
	return hashAgg.(*PhysicalHashAgg)
}
