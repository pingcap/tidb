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
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	util2 "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tipb/go-tipb"
)

// PhysicalHashAgg is hash operator of aggregate.
type PhysicalHashAgg struct {
	BasePhysicalAgg
	TiflashPreAggMode string
}

// getHashAggs will generate some kinds of taskType here, which finally converted to different task plan.
// when deciding whether to add a kind of taskType, there is a rule here. [Not is Not, Yes is not Sure]
// eg: which means
//
//	1: when you find something here that block hashAgg to be pushed down to XXX, just skip adding the XXXTaskType.
//	2: when you find nothing here to block hashAgg to be pushed down to XXX, just add the XXXTaskType here.
//	for 2, the final result for this physical operator enumeration is chosen or rejected is according to more factors later (hint/variable/partition/virtual-col/cost)
//
// That is to say, the non-complete positive judgement of canPushDownToMPP/canPushDownToTiFlash/canPushDownToTiKV is not that for sure here.
func getHashAggs(lp base.LogicalPlan, prop *property.PhysicalProperty) []base.PhysicalPlan {
	la := lp.(*logicalop.LogicalAggregation)
	if !prop.IsSortItemEmpty() {
		return nil
	}
	if prop.TaskTp == property.MppTaskType && !checkCanPushDownToMPP(la) {
		return nil
	}
	hashAggs := make([]base.PhysicalPlan, 0, len(prop.GetAllPossibleChildTaskTypes()))
	taskTypes := []property.TaskType{property.CopSingleReadTaskType, property.CopMultiReadTaskType, property.RootTaskType}
	// aggregation has a special case that it can be pushed down to TiKV which is indicated by the prop.NoCopPushDown
	if prop.NoCopPushDown {
		taskTypes = []property.TaskType{property.RootTaskType}
	}
	// lift the recursive check of canPushToCop(tiFlash)
	canPushDownToMPP := la.SCtx().GetSessionVars().IsMPPAllowed() && checkCanPushDownToMPP(la)
	if canPushDownToMPP {
		taskTypes = append(taskTypes, property.MppTaskType)
	} else {
		hasMppHints := false
		var errMsg string
		if la.PreferAggType&h.PreferMPP1PhaseAgg > 0 {
			errMsg = "The agg can not push down to the MPP side, the MPP_1PHASE_AGG() hint is invalid"
			hasMppHints = true
		}
		if la.PreferAggType&h.PreferMPP2PhaseAgg > 0 {
			errMsg = "The agg can not push down to the MPP side, the MPP_2PHASE_AGG() hint is invalid"
			hasMppHints = true
		}
		if hasMppHints {
			la.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		}
	}
	if prop.IsFlashProp() {
		taskTypes = []property.TaskType{prop.TaskTp}
	}
	taskTypes = admitIndexJoinTypes(taskTypes, prop)
	for _, taskTp := range taskTypes {
		if taskTp == property.MppTaskType {
			mppAggs := tryToGetMppHashAggs(la, prop)
			if len(mppAggs) > 0 {
				hashAggs = append(hashAggs, mppAggs...)
			}
		} else {
			childProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, TaskTp: taskTp, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown}
			// mainly to fill indexJoinProp to childProp.
			childProp = admitIndexJoinProp(childProp, prop)
			if childProp == nil {
				continue
			}
			agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt), childProp)
			agg.SetSchema(la.Schema().Clone())
			hashAggs = append(hashAggs, agg)
		}
	}
	return hashAggs
}

func tryToGetMppHashAggs(la *logicalop.LogicalAggregation, prop *property.PhysicalProperty) (hashAggs []base.PhysicalPlan) {
	if !prop.IsSortItemEmpty() {
		return nil
	}
	if prop.TaskTp != property.RootTaskType && prop.TaskTp != property.MppTaskType {
		return nil
	}
	if prop.MPPPartitionTp == property.BroadcastType {
		return nil
	}

	// Is this aggregate a final stage aggregate?
	// Final agg can't be split into multi-stage aggregate
	hasFinalAgg := len(la.AggFuncs) > 0 && la.AggFuncs[0].Mode == aggregation.FinalMode
	// count final agg should become sum for MPP execution path.
	// In the traditional case, TiDB take up the final agg role and push partial agg to TiKV,
	// while TiDB can tell the partialMode and do the sum computation rather than counting but MPP doesn't
	finalAggAdjust := func(aggFuncs []*aggregation.AggFuncDesc) {
		for i, agg := range aggFuncs {
			if agg.Mode == aggregation.FinalMode && agg.Name == ast.AggFuncCount {
				oldFT := agg.RetTp
				aggFuncs[i], _ = aggregation.NewAggFuncDesc(la.SCtx().GetExprCtx(), ast.AggFuncSum, agg.Args, false)
				aggFuncs[i].TypeInfer4FinalCount(oldFT)
			}
		}
	}
	// ref: https://github.com/pingcap/tiflash/blob/3ebb102fba17dce3d990d824a9df93d93f1ab
	// 766/dbms/src/Flash/Coprocessor/AggregationInterpreterHelper.cpp#L26
	validMppAgg := func(mppAgg *PhysicalHashAgg) bool {
		isFinalAgg := true
		if mppAgg.AggFuncs[0].Mode != aggregation.FinalMode && mppAgg.AggFuncs[0].Mode != aggregation.CompleteMode {
			isFinalAgg = false
		}
		for _, one := range mppAgg.AggFuncs[1:] {
			otherIsFinalAgg := one.Mode == aggregation.FinalMode || one.Mode == aggregation.CompleteMode
			if isFinalAgg != otherIsFinalAgg {
				// different agg mode detected in mpp side.
				return false
			}
		}
		return true
	}

	if len(la.GroupByItems) > 0 {
		partitionCols := la.GetPotentialPartitionKeys()
		// trying to match the required partitions.
		if prop.MPPPartitionTp == property.HashType {
			// partition key required by upper layer is subset of current layout.
			matches := prop.IsSubsetOf(partitionCols)
			if len(matches) == 0 {
				// do not satisfy the property of its parent, so return empty
				return nil
			}
			partitionCols = property.ChoosePartitionKeys(partitionCols, matches)
		} else if prop.MPPPartitionTp != property.AnyType {
			return nil
		}
		// TODO: permute various partition columns from group-by columns
		// 1-phase agg
		// If there are no available partition cols, but still have group by items, that means group by items are all expressions or constants.
		// To avoid mess, we don't do any one-phase aggregation in this case.
		// If this is a skew distinct group agg, skip generating 1-phase agg, because skew data will cause performance issue
		//
		// Rollup can't be 1-phase agg: cause it will append grouping_id to the schema, and expand each row as multi rows with different grouping_id.
		// In a general, group items should also append grouping_id as its group layout, let's say 1-phase agg has grouping items as <a,b,c>, and
		// lower OP can supply <a,b> as original partition layout, when we insert Expand logic between them:
		// <a,b>             -->    after fill null in Expand    --> and this shown two rows should be shuffled to the same node (the underlying partition is not satisfied yet)
		// <1,1> in node A           <1,null,gid=1> in node A
		// <1,2> in node B           <1,null,gid=1> in node B
		if len(partitionCols) != 0 && !la.SCtx().GetSessionVars().EnableSkewDistinctAgg {
			childProp := &property.PhysicalProperty{
				TaskTp:            property.MppTaskType,
				ExpectedCnt:       math.MaxFloat64,
				MPPPartitionTp:    property.HashType,
				MPPPartitionCols:  partitionCols,
				CanAddEnforcer:    true,
				CTEProducerStatus: prop.CTEProducerStatus,
				NoCopPushDown:     prop.NoCopPushDown,
			}
			agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt), childProp)
			agg.SetSchema(la.Schema().Clone())
			agg.MppRunMode = Mpp1Phase
			finalAggAdjust(agg.AggFuncs)
			if validMppAgg(agg) {
				hashAggs = append(hashAggs, agg)
			}
		}

		// Final agg can't be split into multi-stage aggregate, so exit early
		if hasFinalAgg {
			return
		}

		// 2-phase agg
		// no partition property down，record partition cols inside agg itself, enforce shuffler latter.
		childProp := &property.PhysicalProperty{TaskTp: property.MppTaskType,
			ExpectedCnt:       math.MaxFloat64,
			MPPPartitionTp:    property.AnyType,
			CTEProducerStatus: prop.CTEProducerStatus,
			NoCopPushDown:     prop.NoCopPushDown,
		}
		agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt), childProp)
		agg.SetSchema(la.Schema().Clone())
		agg.MppRunMode = Mpp2Phase
		agg.MppPartitionCols = partitionCols
		if validMppAgg(agg) {
			hashAggs = append(hashAggs, agg)
		}

		// agg runs on TiDB with a partial agg on TiFlash if possible
		if prop.TaskTp == property.RootTaskType {
			childProp := &property.PhysicalProperty{
				TaskTp:            property.MppTaskType,
				ExpectedCnt:       math.MaxFloat64,
				CTEProducerStatus: prop.CTEProducerStatus,
				NoCopPushDown:     prop.NoCopPushDown,
			}
			agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt), childProp)
			agg.SetSchema(la.Schema().Clone())
			agg.MppRunMode = MppTiDB
			hashAggs = append(hashAggs, agg)
		}
	} else if !hasFinalAgg {
		// TODO: support scalar agg in MPP, merge the final result to one node
		childProp := &property.PhysicalProperty{TaskTp: property.MppTaskType,
			ExpectedCnt:       math.MaxFloat64,
			CTEProducerStatus: prop.CTEProducerStatus,
			NoCopPushDown:     prop.NoCopPushDown,
		}

		agg := NewPhysicalHashAgg(la, la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt), childProp)
		agg.SetSchema(la.Schema().Clone())
		if la.HasDistinct() || la.HasOrderBy() {
			// mpp scalar mode means the data will be pass through to only one tiFlash node at last.
			agg.MppRunMode = MppScalar
		} else {
			agg.MppRunMode = MppTiDB
		}
		hashAggs = append(hashAggs, agg)
	}

	// handle MPP Agg hints
	var preferMode AggMppRunMode
	var prefer bool
	if la.PreferAggType&h.PreferMPP1PhaseAgg > 0 {
		preferMode, prefer = Mpp1Phase, true
	} else if la.PreferAggType&h.PreferMPP2PhaseAgg > 0 {
		preferMode, prefer = Mpp2Phase, true
	}
	if prefer {
		var preferPlans []base.PhysicalPlan
		for _, agg := range hashAggs {
			if hg, ok := agg.(*PhysicalHashAgg); ok && hg.MppRunMode == preferMode {
				preferPlans = append(preferPlans, hg)
			}
		}
		hashAggs = preferPlans
	}
	return
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

// CPUCostDivisor computes the concurrency to which we would amortize CPU cost
// for hash aggregation.
func (p *PhysicalHashAgg) CPUCostDivisor(hasDistinct bool) (divisor, con float64) {
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
func (p *PhysicalHashAgg) GetPlanCostVer1(taskType property.TaskType, option *costusage.PlanCostOption) (float64, error) {
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
func (p *PhysicalHashAgg) GetPlanCostVer2(taskType property.TaskType, option *costusage.PlanCostOption, isChildOfINL ...bool) (costusage.CostVer2, error) {
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
