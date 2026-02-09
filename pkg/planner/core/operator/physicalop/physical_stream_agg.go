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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	util2 "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalStreamAgg is stream operator of aggregate.
type PhysicalStreamAgg struct {
	BasePhysicalAgg
}

func getEnforcedStreamAggs(la *logicalop.LogicalAggregation, prop *property.PhysicalProperty) []base.PhysicalPlan {
	if prop.IsFlashProp() {
		return nil
	}
	if prop.IndexJoinProp != nil {
		// since this stream agg is in the inner side of an index join, the
		// enforced sort operator couldn't be built by executor layer now.
		return nil
	}
	_, desc := prop.AllSameOrder()
	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	enforcedAggs := make([]base.PhysicalPlan, 0, len(allTaskTypes))
	childProp := &property.PhysicalProperty{
		ExpectedCnt:    math.Max(prop.ExpectedCnt*la.InputCount/la.StatsInfo().RowCount, prop.ExpectedCnt),
		CanAddEnforcer: true,
		SortItems:      property.SortItemsFromCols(la.GetGroupByCols(), desc),
		NoCopPushDown:  prop.NoCopPushDown,
	}
	if !prop.IsPrefix(childProp) {
		// empty
		return enforcedAggs
	}
	childProp = admitIndexJoinProp(childProp, prop, true)
	if childProp == nil {
		// empty
		return enforcedAggs
	}
	taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType, property.RootTaskType}
	// only admit special types for index join prop
	taskTypes = admitIndexJoinTypes(taskTypes, prop)
	for _, taskTp := range taskTypes {
		copiedChildProperty := new(property.PhysicalProperty)
		*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
		copiedChildProperty.TaskTp = taskTp

		newGbyItems := make([]expression.Expression, len(la.GroupByItems))
		copy(newGbyItems, la.GroupByItems)
		newAggFuncs := make([]*aggregation.AggFuncDesc, len(la.AggFuncs))
		copy(newAggFuncs, la.AggFuncs)

		agg := BasePhysicalAgg{
			GroupByItems: newGbyItems,
			AggFuncs:     newAggFuncs,
		}
		streamAgg := agg.InitForStream(la.SCtx(), la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt), la.QueryBlockOffset(), la.Schema().Clone(), copiedChildProperty)
		enforcedAggs = append(enforcedAggs, streamAgg)
	}
	return enforcedAggs
}

func getStreamAggs(lp base.LogicalPlan, prop *property.PhysicalProperty) []base.PhysicalPlan {
	la := lp.(*logicalop.LogicalAggregation)
	// TODO: support CopTiFlash task type in stream agg
	if prop.IsFlashProp() {
		return nil
	}
	all, desc := prop.AllSameOrder()
	if !all {
		return nil
	}

	for _, aggFunc := range la.AggFuncs {
		if aggFunc.Mode == aggregation.FinalMode {
			return nil
		}
	}
	// group by a + b is not interested in any order.
	groupByCols := la.GetGroupByCols()
	if len(groupByCols) != len(la.GroupByItems) {
		return nil
	}

	allTaskTypes := prop.GetAllPossibleChildTaskTypes()
	streamAggs := make([]base.PhysicalPlan, 0, len(la.PossibleProperties)*(len(allTaskTypes)-1)+len(allTaskTypes))
	childProp := &property.PhysicalProperty{
		ExpectedCnt:   math.Max(prop.ExpectedCnt*la.InputCount/la.StatsInfo().RowCount, prop.ExpectedCnt),
		NoCopPushDown: prop.NoCopPushDown,
	}
	childProp = admitIndexJoinProp(childProp, prop, true)
	if childProp == nil {
		return nil
	}
	for _, possibleChildProperty := range la.PossibleProperties {
		childProp.SortItems = property.SortItemsFromCols(possibleChildProperty[:len(groupByCols)], desc)
		if !prop.IsPrefix(childProp) {
			continue
		}
		// The table read of "CopDoubleReadTaskType" can't promises the sort
		// property that the stream aggregation required, no need to consider.
		taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.RootTaskType}
		// aggregation has a special case that it can be pushed down to TiKV which is indicated by the prop.NoCopPushDown
		if prop.NoCopPushDown {
			taskTypes = []property.TaskType{property.RootTaskType}
		}
		if la.HasDistinct() && la.SCtx().GetSessionVars().AllowDistinctAggPushDown && !la.DistinctArgsMeetsProperty() {
			// if distinct agg push down is allowed, while the distinct args doesn't meet the required property, continue
			// to next possible property check.
			continue
		}
		taskTypes = admitIndexJoinTypes(taskTypes, prop)
		for _, taskTp := range taskTypes {
			copiedChildProperty := new(property.PhysicalProperty)
			*copiedChildProperty = *childProp // It's ok to not deep copy the "cols" field.
			copiedChildProperty.TaskTp = taskTp

			newGbyItems := make([]expression.Expression, len(la.GroupByItems))
			copy(newGbyItems, la.GroupByItems)
			newAggFuncs := make([]*aggregation.AggFuncDesc, len(la.AggFuncs))
			copy(newAggFuncs, la.AggFuncs)

			baseAgg := &BasePhysicalAgg{
				GroupByItems: newGbyItems,
				AggFuncs:     newAggFuncs,
			}
			streamAgg := baseAgg.InitForStream(la.SCtx(), la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt), la.QueryBlockOffset(), la.Schema().Clone(), copiedChildProperty)
			streamAggs = append(streamAggs, streamAgg)
		}
	}
	// If STREAM_AGG hint is existed, it should consider enforce stream aggregation,
	// because we can't trust possibleChildProperty completely.
	if (la.PreferAggType & h.PreferStreamAgg) > 0 {
		streamAggs = append(streamAggs, getEnforcedStreamAggs(la, prop)...)
	}
	return streamAggs
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
func (p *PhysicalStreamAgg) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
	return utilfuncp.GetPlanCostVer14PhysicalStreamAgg(p, taskType, option)
}

// GetPlanCostVer2 implements the physical plan interface.
func (p *PhysicalStreamAgg) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
	return utilfuncp.GetPlanCostVer24PhysicalStreamAgg(p, taskType, option, isChildOfINL...)
}

// Attach2Task implements PhysicalPlan interface.
func (p *PhysicalStreamAgg) Attach2Task(tasks ...base.Task) base.Task {
	return utilfuncp.Attach2Task4PhysicalStreamAgg(p, tasks...)
}
