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

package impl

import (
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/debugtrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// impl pkg is mainly keep the compatibility of cascades based physical optimization and traditional volcano based
// physical optimization. The start point of this is try to make enumeration of all physical plans from single logical
// operator to all logical alternatives inside a group, After which we could easily identify and cache the low-cost
// physical plan based on Group Unit prop-accordingly.
//
// Seems it's more like a mutation or so-called intermediate product of cascades and old tidb physical opt? For
// implementation perspective, it is. While choosing this way could make this product evolution more stable and solid.
// In our blueprint of post-phase, ImplementGroupAndCost function call can be encapsulated as a kind of schedule task,
// special for Group implementation and Cost evaluation to find the more optimal one. It can not only be called in the
// root group down, but also when new group expression generated from middle level of a memo structure, we could choose
// to physicalize the new subtree halfway to see if it can generate a more optimal physical plan according the history
// props.
//
// For current phase here, logical transformation and physical implementation is separated. After the logical phase is
// done, ImplementMemoAndCost is portal for implementing the entire memo and find the most cost-effective physical plan.
// And since memo structure is made up from tons of Group inside and linked with group expression with more group as its
// input child. so for each Group unit, ImplementGroupAndCost is responsible for implementing current group, mainly iter
// all logical alternative inside and try to enumerate possible physical plan for each of them.
//
//       tradition:
//                                                       _______ DataSource.FindBestTask()
//                                    (logicalOp)      / _______ ...                                                                               (child logicalOp)
//     physicalOptimize()  --->  logic.FindBestTask() ----- BaseLogicalPlan.FindBestTask() -----> iteratePhysicalPlan4BaseLogical() ---> logic.FindBestTask()
//                                       ^             \____ ...                                                                                   |
//                                       |              \__ LogicalCTETable.FindBestTask()                                                         |
//                                       +---------------------------------------------------------------------------------------------------------+
//
//       cascades:
//                                                                  _______ findBestTask4DataSource(ge, ...)
//                                   (Group)     router&pass GE   / _______ ...                                                                                       (Child Group)
//    ImplementMemoAndCost ---> ImplementGroupAndCost ---------------- findBestTask4BaseLogicalPlan(ge, ...) ----> iteratePhysicalPlan4GroupExpression() ---> ImplementGroupAndCost
//                                       ^                        \____ ...                                                                                             |
//                                       |                         \__ findBestTask4LogicalCTETable(ge, ...)                                                            |
//                                       +------------------------------------------------------------------------------------------------------------------------------+

// ImplementMemoAndCost is the cascades physicalization and cost PORTAL, it's quite same as physicalOptimize().
func ImplementMemoAndCost(rootGroup *memo.Group, planCounter *base.PlanCounterTp) (plan base.PhysicalPlan, cost float64, err error) {
	sctx := rootGroup.GetLogicalExpressions().Front().Value.(base.LogicalPlan).SCtx()
	if sctx.GetSessionVars().StmtCtx.EnableOptimizerDebugTrace {
		debugtrace.EnterContextCommon(sctx)
		defer debugtrace.LeaveContextCommon(sctx)
	}
	// currently, we will derive each node's stats regardless of group logical prop's stats in memo.copyIn, so
	// we don't have to prepare logical op's stats each as what we do in the previous way.

	// prepare root prop.
	rootProp := &property.PhysicalProperty{
		TaskTp:      property.RootTaskType,
		ExpectedCnt: math.MaxFloat64,
	}

	// prepare physical optimizer tracer.
	opt := optimizetrace.DefaultPhysicalOptimizeOption()
	stmtCtx := sctx.GetSessionVars().StmtCtx
	if stmtCtx.EnableOptimizeTrace {
		tracer := &tracing.PhysicalOptimizeTracer{
			PhysicalPlanCostDetails: make(map[string]*tracing.PhysicalPlanCostDetail),
			Candidates:              make(map[int]*tracing.CandidatePlanTrace),
		}
		opt = opt.WithEnableOptimizeTracer(tracer)
		defer func() {
			r := recover()
			if r != nil {
				panic(r) /* pass panic to upper function to handle */
			}
			if err == nil {
				tracer.RecordFinalPlanTrace(plan.BuildPlanTrace())
				stmtCtx.OptimizeTracer.Physical = tracer
			}
		}()
	}

	task, _, implErr := ImplementGroupAndCost(rootGroup, rootProp, math.MaxFloat64, planCounter, opt)
	if implErr != nil {
		return nil, 0, implErr
	}

	sctx.GetSessionVars().StmtCtx.TaskMapBakTS = 0
	if *planCounter > 0 {
		sctx.GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The parameter of nth_plan() is out of range"))
	}
	if task.Invalid() {
		errMsg := "Can't find a proper physical plan for this query"
		if config.GetGlobalConfig().DisaggregatedTiFlash && !sctx.GetSessionVars().IsMPPAllowed() {
			errMsg += ": cop and batchCop are not allowed in disaggregated tiflash mode, you should turn on tidb_allow_mpp switch"
		}
		return nil, 0, plannererrors.ErrInternal.GenWithStackByArgs(errMsg)
	}
	if err = task.Plan().ResolveIndices(); err != nil {
		return nil, 0, err
	}
	cost, err = utilfuncp.GetPlanCost(task.Plan(), property.RootTaskType, optimizetrace.NewDefaultPlanCostOption())
	return task.Plan(), cost, err
}

// ImplementGroupAndCost is the implementation and cost logic based on ONE group unit.
func ImplementGroupAndCost(group *memo.Group, prop *property.PhysicalProperty, costLimit float64,
	planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error) {
	// Check whether the child group is already optimized for the physical property.
	task := group.GetBestTask(prop)
	if task != nil {
		taskCost, invalid, err := utilfuncp.GetTaskPlanCost(task, opt)
		if err != nil || invalid {
			return base.InvalidTask, 0, err
		}
		if taskCost <= costLimit {
			// the optimized group has a valid cost plan according to this physical prop.
			return task, 1, nil
		}
		// the optimized task from this group is out of aimed cost limit, quite fall over.
		return nil, 0, nil
	}

	// the group hasn't been optimized, physic it.
	var (
		implErr  error
		cntPlan  = int64(0)
		bestTask = base.InvalidTask
	)
	group.ForEachGE(func(ge *memo.GroupExpression) bool {
		// for each group expression inside un-optimized group, try to find the best physical plan prop-accordingly.
		// GroupExpression overrides the base.LogicalPlan's FindBestTask to do the router job, And this is because
		// if we call ge.LogicalOperator.FindBestTask, the function receiver will be logical operator himself rather
		// than the group expression as we expected. ge.FindBestTask will directly call the logicalOp's findBestTask4xxx
		// for the same effect while pass the ge as the first the parameter because ge also implement the LogicalPlan
		// interface as well.
		task, cnt, err := ge.FindBestTask(prop, planCounter, opt)
		if err != nil {
			implErr = err
			return false
		}
		cntPlan += cnt
		// update the best task across the logical alternatives.
		if curIsBetter, err := utilfuncp.CompareTaskCost(task, bestTask, opt); err != nil {
			implErr = err
			return false
		} else if curIsBetter {
			bestTask = task
		}
		// continue to next group expression.
		return true
	})
	if implErr != nil {
		return nil, 0, implErr
	}
	// store the best task into the group prop-accordingly.
	group.SetBestTask(prop, bestTask)
	return bestTask, cntPlan, nil
}
