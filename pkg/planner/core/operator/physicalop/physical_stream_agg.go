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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	util2 "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalStreamAgg is stream operator of aggregate.
type PhysicalStreamAgg struct {
	BasePhysicalAgg
}

// GetPointer return inner base physical agg.
func (p *PhysicalStreamAgg) GetPointer() *BasePhysicalAgg {
	return &p.BasePhysicalAgg
}

// Clone implements op.PhysicalPlan interface.
func (p *PhysicalStreamAgg) Clone(newCtx base.PlanContext) (base.PhysicalPlan, error) {
	cloned := new(PhysicalStreamAgg)
	cloned.SetSCtx(newCtx)
	base, err := p.BasePhysicalAgg.CloneWithSelf(newCtx, cloned)
	if err != nil {
		return nil, err
	}
	cloned.BasePhysicalAgg = *base
	return cloned, nil
}

// MemoryUsage return the memory usage of PhysicalStreamAgg
func (p *PhysicalStreamAgg) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	return p.BasePhysicalAgg.MemoryUsage()
}

// ToPB implements PhysicalPlan ToPB interface.
func (p *PhysicalStreamAgg) ToPB(ctx *base.BuildPBContext, storeType kv.StoreType) (*tipb.Executor, error) {
	client := ctx.GetClient()
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	pushDownCtx := util2.GetPushDownCtxFromBuildPBContext(ctx)
	groupByExprs, err := expression.ExpressionsToPBList(evalCtx, p.GroupByItems, client)
	if err != nil {
		return nil, err
	}
	aggExec := &tipb.Aggregation{
		GroupBy: groupByExprs,
	}
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
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeStreamAgg, Aggregation: aggExec, ExecutorId: &executorID}, nil
}

// GetCost computes cost of stream aggregation considering CPU/memory.
func (p *PhysicalStreamAgg) GetCost(inputRows float64, isRoot bool, costFlag uint64) float64 {
	return utilfuncp.GetCost4PhysicalStreamAgg(p, inputRows, isRoot, costFlag)
}

// GetPlanCostVer1 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *PhysicalStreamAgg) GetPlanCostVer1(taskType property.TaskType, option *optimizetrace.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalStreamAgg(p, taskType, option)
}

// GetPlanCostVer2 implements the physical plan interface.
func (p *PhysicalStreamAgg) GetPlanCostVer2(taskType property.TaskType, option *optimizetrace.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalStreamAgg(p, taskType, option, isChildOfINL...)
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalStreamAgg) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalStreamAgg(p, tasks...)
}
