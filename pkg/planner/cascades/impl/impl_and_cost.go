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

// implementMemoAndCost is the cascades physicalization and cost portal, it's quite same as physicalOptimize().
func implementMemoAndCost(group *memo.Group, planCounter *base.PlanCounterTp) (plan base.PhysicalPlan, cost float64, err error) {
	sctx := group.GetLogicalExpressions().Front().Value.(base.LogicalPlan).SCtx()
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

	task, _, implErr := implementGroupAndCost(group, rootProp, math.MaxFloat64, planCounter, opt)
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

// implementGroupAndCost is the implementation and cost logic based on ONE group unit.
func implementGroupAndCost(group *memo.Group, prop *property.PhysicalProperty, costLimit float64,
	planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error) {
	// cache the invalid task for the group.
	if prop.TaskTp != property.RootTaskType && !prop.IsFlashProp() {
		// Currently all plan cannot totally push down to TiKV.
		group.SetBestTask(prop, base.InvalidTask)
		return base.InvalidTask, 0, nil
	}
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
		} else {
			// the optimized task from this group is out of aimed cost limit, quite fall over.
			return nil, 0, nil
		}
	}

	// the group hasn't been optimized, physic it.
	var (
		implErr  error
		cntPlan  = int64(0)
		bestTask = base.InvalidTask
	)
	group.ForEachGE(func(ge *memo.GroupExpression) bool {
		// for each group expression inside un-optimized group, try to find the best physical plan prop-accordingly.
		// GroupExpression overrides the base.LogicalPlan's FindBestTask to do the router job.
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
