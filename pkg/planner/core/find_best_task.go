// Copyright 2017 PingCAP, Inc.
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

package core

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

// PlanCounterDisabled is the default value of PlanCounterTp, indicating that optimizer needn't force a plan.
var PlanCounterDisabled base.PlanCounterTp = -1

// GetPropByOrderByItems will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns.
func GetPropByOrderByItems(items []*util.ByItems) (*property.PhysicalProperty, bool) {
	propItems := make([]property.SortItem, 0, len(items))
	for _, item := range items {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		propItems = append(propItems, property.SortItem{Col: col, Desc: item.Desc})
	}
	return &property.PhysicalProperty{SortItems: propItems}, true
}

// GetPropByOrderByItemsContainScalarFunc will check if this sort property can be pushed or not. In order to simplify the
// problem, we only consider the case that all expression are columns or some special scalar functions.
func GetPropByOrderByItemsContainScalarFunc(items []*util.ByItems) (*property.PhysicalProperty, bool, bool) {
	propItems := make([]property.SortItem, 0, len(items))
	onlyColumn := true
	for _, item := range items {
		switch expr := item.Expr.(type) {
		case *expression.Column:
			propItems = append(propItems, property.SortItem{Col: expr, Desc: item.Desc})
		case *expression.ScalarFunction:
			col, desc := expr.GetSingleColumn(item.Desc)
			if col == nil {
				return nil, false, false
			}
			propItems = append(propItems, property.SortItem{Col: col, Desc: desc})
			onlyColumn = false
		default:
			return nil, false, false
		}
	}
	return &property.PhysicalProperty{SortItems: propItems}, true, onlyColumn
}

// FindBestTask implements the LogicalPlan.
func (p *LogicalTableDual) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error) {
	// If the required property is not empty and the row count > 1,
	// we cannot ensure this required property.
	// But if the row count is 0 or 1, we don't need to care about the property.
	if (!prop.IsSortItemEmpty() && p.RowCount > 1) || planCounter.Empty() {
		return base.InvalidTask, 0, nil
	}
	dual := PhysicalTableDual{
		RowCount: p.RowCount,
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())
	dual.SetSchema(p.schema)
	planCounter.Dec(1)
	utilfuncp.AppendCandidate4PhysicalOptimizeOp(opt, p, dual, prop)
	rt := &RootTask{}
	rt.SetPlan(dual)
	rt.SetEmpty(p.RowCount == 0)
	return rt, 1, nil
}

// FindBestTask implements the LogicalPlan interface.
func (p *LogicalShow) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp, _ *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error) {
	if !prop.IsSortItemEmpty() || planCounter.Empty() {
		return base.InvalidTask, 0, nil
	}
	pShow := PhysicalShow{ShowContents: p.ShowContents, Extractor: p.Extractor}.Init(p.SCtx())
	pShow.SetSchema(p.schema)
	planCounter.Dec(1)
	rt := &RootTask{}
	rt.SetPlan(pShow)
	return rt, 1, nil
}

// FindBestTask implements the LogicalPlan interface.
func (p *LogicalShowDDLJobs) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp, _ *optimizetrace.PhysicalOptimizeOp) (base.Task, int64, error) {
	if !prop.IsSortItemEmpty() || planCounter.Empty() {
		return base.InvalidTask, 0, nil
	}
	pShow := PhysicalShowDDLJobs{JobNumber: p.JobNumber}.Init(p.SCtx())
	pShow.SetSchema(p.schema)
	planCounter.Dec(1)
	rt := &RootTask{}
	rt.SetPlan(pShow)
	return rt, 1, nil
}

// rebuildChildTasks rebuilds the childTasks to make the clock_th combination.
func rebuildChildTasks(p *logicalop.BaseLogicalPlan, childTasks *[]base.Task, pp base.PhysicalPlan, childCnts []int64, planCounter int64, ts uint64, opt *optimizetrace.PhysicalOptimizeOp) error {
	// The taskMap of children nodes should be rolled back first.
	for _, child := range p.Children() {
		child.RollBackTaskMap(ts)
	}

	multAll := int64(1)
	var curClock base.PlanCounterTp
	for _, x := range childCnts {
		multAll *= x
	}
	*childTasks = (*childTasks)[:0]
	for j, child := range p.Children() {
		multAll /= childCnts[j]
		curClock = base.PlanCounterTp((planCounter-1)/multAll + 1)
		childTask, _, err := child.FindBestTask(pp.GetChildReqProps(j), &curClock, opt)
		planCounter = (planCounter-1)%multAll + 1
		if err != nil {
			return err
		}
		if curClock != 0 {
			return errors.Errorf("PlanCounterTp planCounter is not handled")
		}
		if childTask != nil && childTask.Invalid() {
			return errors.Errorf("The current plan is invalid, please skip this plan")
		}
		*childTasks = append(*childTasks, childTask)
	}
	return nil
}

func enumeratePhysicalPlans4Task(
	p *logicalop.BaseLogicalPlan,
	physicalPlans []base.PhysicalPlan,
	prop *property.PhysicalProperty,
	addEnforcer bool,
	planCounter *base.PlanCounterTp,
	opt *optimizetrace.PhysicalOptimizeOp,
) (base.Task, int64, error) {
	var bestTask base.Task = base.InvalidTask
	var curCntPlan, cntPlan int64
	var err error
	childTasks := make([]base.Task, 0, p.ChildLen())
	childCnts := make([]int64, p.ChildLen())
	cntPlan = 0
	iteration := iteratePhysicalPlan4BaseLogical
	if _, ok := p.Self().(*LogicalSequence); ok {
		iteration = iterateChildPlan4LogicalSequence
	}

	for _, pp := range physicalPlans {
		timeStampNow := p.GetLogicalTS4TaskMap()
		savedPlanID := p.SCtx().GetSessionVars().PlanID.Load()

		childTasks, curCntPlan, childCnts, err = iteration(p, pp, childTasks, childCnts, prop, opt)
		if err != nil {
			return nil, 0, err
		}

		// This check makes sure that there is no invalid child task.
		if len(childTasks) != p.ChildLen() {
			continue
		}

		// If the target plan can be found in this physicalPlan(pp), rebuild childTasks to build the corresponding combination.
		if planCounter.IsForce() && int64(*planCounter) <= curCntPlan {
			p.SCtx().GetSessionVars().PlanID.Store(savedPlanID)
			curCntPlan = int64(*planCounter)
			err := rebuildChildTasks(p, &childTasks, pp, childCnts, int64(*planCounter), timeStampNow, opt)
			if err != nil {
				return nil, 0, err
			}
		}

		// Combine the best child tasks with parent physical plan.
		curTask := pp.Attach2Task(childTasks...)
		if curTask.Invalid() {
			continue
		}

		// An optimal task could not satisfy the property, so it should be converted here.
		if _, ok := curTask.(*RootTask); !ok && prop.TaskTp == property.RootTaskType {
			curTask = curTask.ConvertToRootTask(p.SCtx())
		}

		// Enforce curTask property
		if addEnforcer {
			curTask = enforceProperty(prop, curTask, p.Plan.SCtx())
		}

		// Optimize by shuffle executor to running in parallel manner.
		if _, isMpp := curTask.(*MppTask); !isMpp && prop.IsSortItemEmpty() {
			// Currently, we do not regard shuffled plan as a new plan.
			curTask = optimizeByShuffle(curTask, p.Plan.SCtx())
		}

		cntPlan += curCntPlan
		planCounter.Dec(curCntPlan)

		if planCounter.Empty() {
			bestTask = curTask
			break
		}
		utilfuncp.AppendCandidate4PhysicalOptimizeOp(opt, p, curTask.Plan(), prop)
		// Get the most efficient one.
		if curIsBetter, err := compareTaskCost(curTask, bestTask, opt); err != nil {
			return nil, 0, err
		} else if curIsBetter {
			bestTask = curTask
		}
	}
	return bestTask, cntPlan, nil
}

// iteratePhysicalPlan4BaseLogical is used to iterate the physical plan and get all child tasks.
func iteratePhysicalPlan4BaseLogical(
	p *logicalop.BaseLogicalPlan,
	selfPhysicalPlan base.PhysicalPlan,
	childTasks []base.Task,
	childCnts []int64,
	_ *property.PhysicalProperty,
	opt *optimizetrace.PhysicalOptimizeOp,
) ([]base.Task, int64, []int64, error) {
	// Find best child tasks firstly.
	childTasks = childTasks[:0]
	// The curCntPlan records the number of possible plans for pp
	curCntPlan := int64(1)
	for j, child := range p.Children() {
		childProp := selfPhysicalPlan.GetChildReqProps(j)
		childTask, cnt, err := child.FindBestTask(childProp, &PlanCounterDisabled, opt)
		childCnts[j] = cnt
		if err != nil {
			return nil, 0, childCnts, err
		}
		curCntPlan = curCntPlan * cnt
		if childTask != nil && childTask.Invalid() {
			return nil, 0, childCnts, nil
		}
		childTasks = append(childTasks, childTask)
	}

	// This check makes sure that there is no invalid child task.
	if len(childTasks) != p.ChildLen() {
		return nil, 0, childCnts, nil
	}
	return childTasks, curCntPlan, childCnts, nil
}

// iterateChildPlan4LogicalSequence does the special part for sequence. We need to iterate its child one by one to check whether the former child is a valid plan and then go to the nex
func iterateChildPlan4LogicalSequence(
	p *logicalop.BaseLogicalPlan,
	selfPhysicalPlan base.PhysicalPlan,
	childTasks []base.Task,
	childCnts []int64,
	prop *property.PhysicalProperty,
	opt *optimizetrace.PhysicalOptimizeOp,
) ([]base.Task, int64, []int64, error) {
	// Find best child tasks firstly.
	childTasks = childTasks[:0]
	// The curCntPlan records the number of possible plans for pp
	curCntPlan := int64(1)
	lastIdx := p.ChildLen() - 1
	for j := 0; j < lastIdx; j++ {
		child := p.Children()[j]
		childProp := selfPhysicalPlan.GetChildReqProps(j)
		childTask, cnt, err := child.FindBestTask(childProp, &PlanCounterDisabled, opt)
		childCnts[j] = cnt
		if err != nil {
			return nil, 0, nil, err
		}
		curCntPlan = curCntPlan * cnt
		if childTask != nil && childTask.Invalid() {
			return nil, 0, nil, nil
		}
		_, isMpp := childTask.(*MppTask)
		if !isMpp && prop.IsFlashProp() {
			break
		}
		childTasks = append(childTasks, childTask)
	}
	// This check makes sure that there is no invalid child task.
	if len(childTasks) != p.ChildLen()-1 {
		return nil, 0, nil, nil
	}

	lastChildProp := selfPhysicalPlan.GetChildReqProps(lastIdx).CloneEssentialFields()
	if lastChildProp.IsFlashProp() {
		lastChildProp.CTEProducerStatus = property.AllCTECanMpp
	}
	lastChildTask, cnt, err := p.Children()[lastIdx].FindBestTask(lastChildProp, &PlanCounterDisabled, opt)
	childCnts[lastIdx] = cnt
	if err != nil {
		return nil, 0, nil, err
	}
	curCntPlan = curCntPlan * cnt
	if lastChildTask != nil && lastChildTask.Invalid() {
		return nil, 0, nil, nil
	}

	if _, ok := lastChildTask.(*MppTask); !ok && lastChildProp.CTEProducerStatus == property.AllCTECanMpp {
		return nil, 0, nil, nil
	}

	childTasks = append(childTasks, lastChildTask)
	return childTasks, curCntPlan, childCnts, nil
}

// compareTaskCost compares cost of curTask and bestTask and returns whether curTask's cost is smaller than bestTask's.
func compareTaskCost(curTask, bestTask base.Task, op *optimizetrace.PhysicalOptimizeOp) (curIsBetter bool, err error) {
	curCost, curInvalid, err := utilfuncp.GetTaskPlanCost(curTask, op)
	if err != nil {
		return false, err
	}
	bestCost, bestInvalid, err := utilfuncp.GetTaskPlanCost(bestTask, op)
	if err != nil {
		return false, err
	}
	if curInvalid {
		return false, nil
	}
	if bestInvalid {
		return true, nil
	}
	return curCost < bestCost, nil
}

// getTaskPlanCost returns the cost of this task.
// The new cost interface will be used if EnableNewCostInterface is true.
// The second returned value indicates whether this task is valid.
func getTaskPlanCost(t base.Task, pop *optimizetrace.PhysicalOptimizeOp) (float64, bool, error) {
	if t.Invalid() {
		return math.MaxFloat64, true, nil
	}

	// use the new cost interface
	var (
		taskType         property.TaskType
		indexPartialCost float64
	)
	switch t.(type) {
	case *RootTask:
		taskType = property.RootTaskType
	case *CopTask: // no need to know whether the task is single-read or double-read, so both CopSingleReadTaskType and CopDoubleReadTaskType are OK
		cop := t.(*CopTask)
		if cop.indexPlan != nil && cop.tablePlan != nil { // handle IndexLookup specially
			taskType = property.CopMultiReadTaskType
			// keep compatible with the old cost interface, for CopMultiReadTask, the cost is idxCost + tblCost.
			if !cop.indexPlanFinished { // only consider index cost in this case
				idxCost, err := getPlanCost(cop.indexPlan, taskType, optimizetrace.NewDefaultPlanCostOption().WithOptimizeTracer(pop))
				return idxCost, false, err
			}
			// consider both sides
			idxCost, err := getPlanCost(cop.indexPlan, taskType, optimizetrace.NewDefaultPlanCostOption().WithOptimizeTracer(pop))
			if err != nil {
				return 0, false, err
			}
			tblCost, err := getPlanCost(cop.tablePlan, taskType, optimizetrace.NewDefaultPlanCostOption().WithOptimizeTracer(pop))
			if err != nil {
				return 0, false, err
			}
			return idxCost + tblCost, false, nil
		}

		taskType = property.CopSingleReadTaskType

		// TiFlash can run cop task as well, check whether this cop task will run on TiKV or TiFlash.
		if cop.tablePlan != nil {
			leafNode := cop.tablePlan
			for len(leafNode.Children()) > 0 {
				leafNode = leafNode.Children()[0]
			}
			if tblScan, isScan := leafNode.(*PhysicalTableScan); isScan && tblScan.StoreType == kv.TiFlash {
				taskType = property.MppTaskType
			}
		}

		// Detail reason ref about comment in function `convertToIndexMergeScan`
		// for cop task with {indexPlan=nil, tablePlan=xxx, idxMergePartPlans=[x,x,x], indexPlanFinished=true} we should
		// plus the partial index plan cost into the final cost. Because t.plan() the below code used only calculate the
		// cost about table plan.
		if cop.indexPlanFinished && len(cop.idxMergePartPlans) != 0 {
			for _, partialScan := range cop.idxMergePartPlans {
				partialCost, err := getPlanCost(partialScan, taskType, optimizetrace.NewDefaultPlanCostOption().WithOptimizeTracer(pop))
				if err != nil {
					return 0, false, err
				}
				indexPartialCost += partialCost
			}
		}
	case *MppTask:
		taskType = property.MppTaskType
	default:
		return 0, false, errors.New("unknown task type")
	}
	if t.Plan() == nil {
		// It's a very special case for index merge case.
		// t.plan() == nil in index merge COP case, it means indexPlanFinished is false in other words.
		cost := 0.0
		copTsk := t.(*CopTask)
		for _, partialScan := range copTsk.idxMergePartPlans {
			partialCost, err := getPlanCost(partialScan, taskType, optimizetrace.NewDefaultPlanCostOption().WithOptimizeTracer(pop))
			if err != nil {
				return 0, false, err
			}
			cost += partialCost
		}
		return cost, false, nil
	}
	cost, err := getPlanCost(t.Plan(), taskType, optimizetrace.NewDefaultPlanCostOption().WithOptimizeTracer(pop))
	return cost + indexPartialCost, false, err
}

func appendCandidate4PhysicalOptimizeOp(pop *optimizetrace.PhysicalOptimizeOp, lp base.LogicalPlan, pp base.PhysicalPlan, prop *property.PhysicalProperty) {
	if pop == nil || pop.GetTracer() == nil || pp == nil {
		return
	}
	candidate := &tracing.CandidatePlanTrace{
		PlanTrace: &tracing.PlanTrace{TP: pp.TP(), ID: pp.ID(),
			ExplainInfo: pp.ExplainInfo(), ProperType: prop.String()},
		MappingLogicalPlan: tracing.CodecPlanName(lp.TP(), lp.ID())}
	pop.GetTracer().AppendCandidate(candidate)

	// for PhysicalIndexMergeJoin/PhysicalIndexHashJoin/PhysicalIndexJoin, it will use innerTask as a child instead of calling findBestTask,
	// and innerTask.plan() will be appended to planTree in appendChildCandidate using empty MappingLogicalPlan field, so it won't mapping with the logic plan,
	// that will cause no physical plan when the logic plan got selected.
	// the fix to add innerTask.plan() to planTree and mapping correct logic plan
	index := -1
	var plan base.PhysicalPlan
	switch join := pp.(type) {
	case *PhysicalIndexMergeJoin:
		index = join.InnerChildIdx
		plan = join.innerTask.Plan()
	case *PhysicalIndexHashJoin:
		index = join.InnerChildIdx
		plan = join.innerTask.Plan()
	case *PhysicalIndexJoin:
		index = join.InnerChildIdx
		plan = join.innerTask.Plan()
	}
	if index != -1 {
		child := lp.(*logicalop.BaseLogicalPlan).Children()[index]
		candidate := &tracing.CandidatePlanTrace{
			PlanTrace: &tracing.PlanTrace{TP: plan.TP(), ID: plan.ID(),
				ExplainInfo: plan.ExplainInfo(), ProperType: prop.String()},
			MappingLogicalPlan: tracing.CodecPlanName(child.TP(), child.ID())}
		pop.GetTracer().AppendCandidate(candidate)
	}
	pp.AppendChildCandidate(pop)
}

func appendPlanCostDetail4PhysicalOptimizeOp(pop *optimizetrace.PhysicalOptimizeOp, detail *tracing.PhysicalPlanCostDetail) {
	if pop == nil || pop.GetTracer() == nil {
		return
	}
	pop.GetTracer().PhysicalPlanCostDetails[fmt.Sprintf("%v_%v", detail.GetPlanType(), detail.GetPlanID())] = detail
}

// findBestTask is key workflow that drive logic plan tree to generate optimal physical ones.
// The logic inside it is mainly about physical plan numeration and task encapsulation, it should
// be defined in core pkg, and be called by logic plan in their logic interface implementation.
func findBestTask(lp base.LogicalPlan, prop *property.PhysicalProperty, planCounter *base.PlanCounterTp,
	opt *optimizetrace.PhysicalOptimizeOp) (bestTask base.Task, cntPlan int64, err error) {
	p := lp.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan)
	// If p is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		return nil, 1, nil
	}
	// Look up the task with this prop in the task map.
	// It's used to reduce double counting.
	bestTask = p.GetTask(prop)
	if bestTask != nil {
		planCounter.Dec(1)
		return bestTask, 1, nil
	}

	canAddEnforcer := prop.CanAddEnforcer

	if prop.TaskTp != property.RootTaskType && !prop.IsFlashProp() {
		// Currently all plan cannot totally push down to TiKV.
		p.StoreTask(prop, base.InvalidTask)
		return base.InvalidTask, 0, nil
	}

	cntPlan = 0
	// prop should be read only because its cached hashcode might be not consistent
	// when it is changed. So we clone a new one for the temporary changes.
	newProp := prop.CloneEssentialFields()
	var plansFitsProp, plansNeedEnforce []base.PhysicalPlan
	var hintWorksWithProp bool
	// Maybe the plan can satisfy the required property,
	// so we try to get the task without the enforced sort first.
	plansFitsProp, hintWorksWithProp, err = p.Self().ExhaustPhysicalPlans(newProp)
	if err != nil {
		return nil, 0, err
	}
	if !hintWorksWithProp && !newProp.IsSortItemEmpty() {
		// If there is a hint in the plan and the hint cannot satisfy the property,
		// we enforce this property and try to generate the PhysicalPlan again to
		// make sure the hint can work.
		canAddEnforcer = true
	}

	if canAddEnforcer {
		// Then, we use the empty property to get physicalPlans and
		// try to get the task with an enforced sort.
		newProp.SortItems = []property.SortItem{}
		newProp.SortItemsForPartition = []property.SortItem{}
		newProp.ExpectedCnt = math.MaxFloat64
		newProp.MPPPartitionCols = nil
		newProp.MPPPartitionTp = property.AnyType
		var hintCanWork bool
		plansNeedEnforce, hintCanWork, err = p.Self().ExhaustPhysicalPlans(newProp)
		if err != nil {
			return nil, 0, err
		}
		if hintCanWork && !hintWorksWithProp {
			// If the hint can work with the empty property, but cannot work with
			// the required property, we give up `plansFitProp` to make sure the hint
			// can work.
			plansFitsProp = nil
		}
		if !hintCanWork && !hintWorksWithProp && !prop.CanAddEnforcer {
			// If the original property is not enforced and hint cannot
			// work anyway, we give up `plansNeedEnforce` for efficiency,
			plansNeedEnforce = nil
		}
		newProp = prop
	}

	var cnt int64
	var curTask base.Task
	if bestTask, cnt, err = enumeratePhysicalPlans4Task(p, plansFitsProp, newProp, false, planCounter, opt); err != nil {
		return nil, 0, err
	}
	cntPlan += cnt
	if planCounter.Empty() {
		goto END
	}

	curTask, cnt, err = enumeratePhysicalPlans4Task(p, plansNeedEnforce, newProp, true, planCounter, opt)
	if err != nil {
		return nil, 0, err
	}
	cntPlan += cnt
	if planCounter.Empty() {
		bestTask = curTask
		goto END
	}
	utilfuncp.AppendCandidate4PhysicalOptimizeOp(opt, p, curTask.Plan(), prop)
	if curIsBetter, err := compareTaskCost(curTask, bestTask, opt); err != nil {
		return nil, 0, err
	} else if curIsBetter {
		bestTask = curTask
	}

END:
	p.StoreTask(prop, bestTask)
	return bestTask, cntPlan, nil
}

// FindBestTask implements the LogicalPlan interface.
func (p *LogicalMemTable) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error) {
	if prop.MPPPartitionTp != property.AnyType {
		return base.InvalidTask, 0, nil
	}

	// If prop.CanAddEnforcer is true, the prop.SortItems need to be set nil for p.findBestTask.
	// Before function return, reset it for enforcing task prop.
	oldProp := prop.CloneEssentialFields()
	if prop.CanAddEnforcer {
		// First, get the bestTask without enforced prop
		prop.CanAddEnforcer = false
		cnt := int64(0)
		t, cnt, err = p.FindBestTask(prop, planCounter, opt)
		if err != nil {
			return nil, 0, err
		}
		prop.CanAddEnforcer = true
		if t != base.InvalidTask {
			cntPlan = cnt
			return
		}
		// Next, get the bestTask with enforced prop
		prop.SortItems = []property.SortItem{}
	}
	defer func() {
		if err != nil {
			return
		}
		if prop.CanAddEnforcer {
			*prop = *oldProp
			t = enforceProperty(prop, t, p.Plan.SCtx())
			prop.CanAddEnforcer = true
		}
	}()

	if !prop.IsSortItemEmpty() || planCounter.Empty() {
		return base.InvalidTask, 0, nil
	}
	memTable := PhysicalMemTable{
		DBName:         p.DBName,
		Table:          p.TableInfo,
		Columns:        p.Columns,
		Extractor:      p.Extractor,
		QueryTimeRange: p.QueryTimeRange,
	}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())
	memTable.SetSchema(p.schema)
	planCounter.Dec(1)
	utilfuncp.AppendCandidate4PhysicalOptimizeOp(opt, p, memTable, prop)
	rt := &RootTask{}
	rt.SetPlan(memTable)
	return rt, 1, nil
}

// tryToGetDualTask will check if the push down predicate has false constant. If so, it will return table dual.
func (ds *DataSource) tryToGetDualTask() (base.Task, error) {
	for _, cond := range ds.pushedDownConds {
		if con, ok := cond.(*expression.Constant); ok && con.DeferredExpr == nil && con.ParamMarker == nil {
			result, _, err := expression.EvalBool(ds.SCtx().GetExprCtx().GetEvalCtx(), []expression.Expression{cond}, chunk.Row{})
			if err != nil {
				return nil, err
			}
			if !result {
				dual := PhysicalTableDual{}.Init(ds.SCtx(), ds.StatsInfo(), ds.QueryBlockOffset())
				dual.SetSchema(ds.schema)
				rt := &RootTask{}
				rt.SetPlan(dual)
				return rt, nil
			}
		}
	}
	return nil, nil
}

// candidatePath is used to maintain required info for skyline pruning.
type candidatePath struct {
	path              *util.AccessPath
	accessCondsColMap util.Col2Len // accessCondsColMap maps Column.UniqueID to column length for the columns in AccessConds.
	indexCondsColMap  util.Col2Len // indexCondsColMap maps Column.UniqueID to column length for the columns in AccessConds and indexFilters.
	isMatchProp       bool
}

func compareBool(l, r bool) int {
	if l == r {
		return 0
	}
	if !l {
		return -1
	}
	return 1
}

func compareIndexBack(lhs, rhs *candidatePath) (int, bool) {
	result := compareBool(lhs.path.IsSingleScan, rhs.path.IsSingleScan)
	if result == 0 && !lhs.path.IsSingleScan {
		// if both lhs and rhs need to access table after IndexScan, we utilize the set of columns that occurred in AccessConds and IndexFilters
		// to compare how many table rows will be accessed.
		return util.CompareCol2Len(lhs.indexCondsColMap, rhs.indexCondsColMap)
	}
	return result, true
}

// compareCandidates is the core of skyline pruning, which is used to decide which candidate path is better.
// The return value is 1 if lhs is better, -1 if rhs is better, 0 if they are equivalent or not comparable.
func compareCandidates(sctx base.PlanContext, prop *property.PhysicalProperty, lhs, rhs *candidatePath) int {
	// Due to #50125, full scan on MVIndex has been disabled, so MVIndex path might lead to 'can't find a proper plan' error at the end.
	// Avoid MVIndex path to exclude all other paths and leading to 'can't find a proper plan' error, see #49438 for an example.
	if isMVIndexPath(lhs.path) || isMVIndexPath(rhs.path) {
		return 0
	}

	// This rule is empirical but not always correct.
	// If x's range row count is significantly lower than y's, for example, 1000 times, we think x is better.
	if lhs.path.CountAfterAccess > 100 && rhs.path.CountAfterAccess > 100 && // to prevent some extreme cases, e.g. 0.01 : 10
		len(lhs.path.PartialIndexPaths) == 0 && len(rhs.path.PartialIndexPaths) == 0 && // not IndexMerge since its row count estimation is not accurate enough
		prop.ExpectedCnt == math.MaxFloat64 { // Limit may affect access row count
		threshold := float64(fixcontrol.GetIntWithDefault(sctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix45132, 1000))
		if threshold > 0 { // set it to 0 to disable this rule
			if lhs.path.CountAfterAccess/rhs.path.CountAfterAccess > threshold {
				return -1
			}
			if rhs.path.CountAfterAccess/lhs.path.CountAfterAccess > threshold {
				return 1
			}
		}
	}

	// Below compares the two candidate paths on three dimensions:
	// (1): the set of columns that occurred in the access condition,
	// (2): does it require a double scan,
	// (3): whether or not it matches the physical property.
	// If `x` is not worse than `y` at all factors,
	// and there exists one factor that `x` is better than `y`, then `x` is better than `y`.
	accessResult, comparable1 := util.CompareCol2Len(lhs.accessCondsColMap, rhs.accessCondsColMap)
	if !comparable1 {
		return 0
	}
	scanResult, comparable2 := compareIndexBack(lhs, rhs)
	if !comparable2 {
		return 0
	}
	matchResult := compareBool(lhs.isMatchProp, rhs.isMatchProp)
	sum := accessResult + scanResult + matchResult
	if accessResult >= 0 && scanResult >= 0 && matchResult >= 0 && sum > 0 {
		return 1
	}
	if accessResult <= 0 && scanResult <= 0 && matchResult <= 0 && sum < 0 {
		return -1
	}
	return 0
}

func (ds *DataSource) isMatchProp(path *util.AccessPath, prop *property.PhysicalProperty) bool {
	var isMatchProp bool
	if path.IsIntHandlePath {
		pkCol := ds.getPKIsHandleCol()
		if len(prop.SortItems) == 1 && pkCol != nil {
			isMatchProp = prop.SortItems[0].Col.EqualColumn(pkCol)
			if path.StoreType == kv.TiFlash {
				isMatchProp = isMatchProp && !prop.SortItems[0].Desc
			}
		}
		return isMatchProp
	}
	all, _ := prop.AllSameOrder()
	// When the prop is empty or `all` is false, `isMatchProp` is better to be `false` because
	// it needs not to keep order for index scan.

	// Basically, if `prop.SortItems` is the prefix of `path.IdxCols`, then `isMatchProp` is true. However, we need to consider
	// the situations when some columns of `path.IdxCols` are evaluated as constant. For example:
	// ```
	// create table t(a int, b int, c int, d int, index idx_a_b_c(a, b, c), index idx_d_c_b_a(d, c, b, a));
	// select * from t where a = 1 order by b, c;
	// select * from t where b = 1 order by a, c;
	// select * from t where d = 1 and b = 2 order by c, a;
	// select * from t where d = 1 and b = 2 order by c, b, a;
	// ```
	// In the first two `SELECT` statements, `idx_a_b_c` matches the sort order. In the last two `SELECT` statements, `idx_d_c_b_a`
	// matches the sort order. Hence, we use `path.ConstCols` to deal with the above situations.
	if !prop.IsSortItemEmpty() && all && len(path.IdxCols) >= len(prop.SortItems) {
		isMatchProp = true
		i := 0
		for _, sortItem := range prop.SortItems {
			found := false
			for ; i < len(path.IdxCols); i++ {
				if path.IdxColLens[i] == types.UnspecifiedLength && sortItem.Col.EqualColumn(path.IdxCols[i]) {
					found = true
					i++
					break
				}
				if path.ConstCols == nil || i >= len(path.ConstCols) || !path.ConstCols[i] {
					break
				}
			}
			if !found {
				isMatchProp = false
				break
			}
		}
	}
	return isMatchProp
}

// matchPropForIndexMergeAlternatives will match the prop with inside PartialAlternativeIndexPaths, and choose
// 1 matched alternative to be a determined index merge partial path for each dimension in PartialAlternativeIndexPaths.
// finally, after we collected the all decided index merge partial paths, we will output a concrete index merge path
// with field PartialIndexPaths is fulfilled here.
//
// as we mentioned before, after deriveStats is done, the normal index OR path will be generated like below:
//
//	    `create table t (a int, b int, c int, key a(a), key b(b), key ac(a, c), key bc(b, c))`
//		`explain format='verbose' select * from t where a=1 or b=1 order by c`
//
// like the case here:
// normal index merge OR path should be:
// for a=1, it has two partial alternative paths: [a, ac]
// for b=1, it has two partial alternative paths: [b, bc]
// and the index merge path:
//
//	indexMergePath: {
//	    PartialIndexPaths: empty                          // 1D array here, currently is not decided yet.
//	    PartialAlternativeIndexPaths: [[a, ac], [b, bc]]  // 2D array here, each for one DNF item choices.
//	}
//
// let's say we have a prop requirement like sort by [c] here, we will choose the better one [ac] (because it can keep
// order) for the first batch [a, ac] from PartialAlternativeIndexPaths; and choose the better one [bc] (because it can
// keep order too) for the second batch [b, bc] from PartialAlternativeIndexPaths. Finally we output a concrete index
// merge path as
//
//	indexMergePath: {
//	    PartialIndexPaths: [ac, bc]                       // just collected since they match the prop.
//	    ...
//	}
//
// how about the prop is empty? that means the choice to be decided from [a, ac] and [b, bc] is quite random just according
// to their countAfterAccess. That's why we use a slices.SortFunc(matchIdxes, func(a, b int){}) inside there. After sort,
// the ASC order of matchIdxes of matched paths are ordered by their countAfterAccess, choosing the first one is straight forward.
//
// there is another case shown below, just the pick the first one after matchIdxes is ordered is not always right, as shown:
// special logic for alternative paths:
//
//	index merge:
//	   matched paths-1: {pk, index1}
//	   matched paths-2: {pk}
//
// if we choose first one as we talked above, says pk here in the first matched paths, then path2 has no choice(avoiding all same
// index logic inside) but pk, this will result in all single index failure. so we need to sort the matchIdxes again according to
// their matched paths length, here mean:
//
//	index merge:
//	   matched paths-1: {pk, index1}
//	   matched paths-2: {pk}
//
// and let matched paths-2 to be the first to make their determination --- choosing pk here, then next turn is matched paths-1 to
// make their choice, since pk is occupied, avoiding-all-same-index-logic inside will try to pick index1 here, so work can be done.
//
// at last, according to determinedIndexPartialPaths to rewrite their real countAfterAccess, this part is move from deriveStats to
// here.
func (ds *DataSource) matchPropForIndexMergeAlternatives(path *util.AccessPath, prop *property.PhysicalProperty) (*util.AccessPath, bool) {
	// target:
	//	1: index merge case, try to match the every alternative partial path to the order property as long as
	//	possible, and generate that property-matched index merge path out if any.
	//	2: If the prop is empty (means no sort requirement), we will generate a random index partial combination
	//	path from all alternatives in case that no index merge path comes out.

	// Execution part doesn't support the merge operation for intersection case yet.
	if path.IndexMergeIsIntersection {
		return nil, false
	}

	noSortItem := prop.IsSortItemEmpty()
	allSame, _ := prop.AllSameOrder()
	if !allSame {
		return nil, false
	}
	// step1: match the property from all the index partial alternative paths.
	determinedIndexPartialPaths := make([]*util.AccessPath, 0, len(path.PartialAlternativeIndexPaths))
	usedIndexMap := make(map[int64]struct{}, 1)
	type idxWrapper struct {
		// matchIdx is those match alternative paths from one alternative paths set.
		// like we said above, for a=1, it has two partial alternative paths: [a, ac]
		// if we met an empty property here, matchIdx from [a, ac] for a=1 will be both. = [0,1]
		// if we met an sort[c] property here, matchIdx from [a, ac] for a=1 will be both. = [1]
		matchIdx []int
		// pathIdx actually is original position offset indicates where current matchIdx is
		// computed from. eg: [[a, ac], [b, bc]] for sort[c] property:
		//     idxWrapper{[ac], 0}, 0 is the offset in first dimension of PartialAlternativeIndexPaths
		//     idxWrapper{[bc], 1}, 1 is the offset in first dimension of PartialAlternativeIndexPaths
		pathIdx int
	}
	allMatchIdxes := make([]idxWrapper, 0, len(path.PartialAlternativeIndexPaths))
	// special logic for alternative paths:
	// index merge:
	//  path1: {pk, index1}
	//  path2: {pk}
	// if we choose pk in the first path, then path2 has no choice but pk, this will result in all single index failure.
	// so we should collect all match prop paths down, stored as matchIdxes here.
	for pathIdx, oneItemAlternatives := range path.PartialAlternativeIndexPaths {
		matchIdxes := make([]int, 0, 1)
		for i, oneIndexAlternativePath := range oneItemAlternatives {
			// if there is some sort items and this path doesn't match this prop, continue.
			if !noSortItem && !ds.isMatchProp(oneIndexAlternativePath, prop) {
				continue
			}
			// two possibility here:
			// 1. no sort items requirement.
			// 2. matched with sorted items.
			matchIdxes = append(matchIdxes, i)
		}
		if len(matchIdxes) == 0 {
			// if all index alternative of one of the cnf item's couldn't match the sort property,
			// the entire index merge union path can be ignored for this sort property, return false.
			return nil, false
		}
		if len(matchIdxes) > 1 {
			// if matchIdxes greater than 1, we should sort this match alternative path by its CountAfterAccess.
			tmpOneItemAlternatives := oneItemAlternatives
			slices.SortStableFunc(matchIdxes, func(a, b int) int {
				lhsCountAfter := tmpOneItemAlternatives[a].CountAfterAccess
				if len(tmpOneItemAlternatives[a].IndexFilters) > 0 {
					lhsCountAfter = tmpOneItemAlternatives[a].CountAfterIndex
				}
				rhsCountAfter := tmpOneItemAlternatives[b].CountAfterAccess
				if len(tmpOneItemAlternatives[b].IndexFilters) > 0 {
					rhsCountAfter = tmpOneItemAlternatives[b].CountAfterIndex
				}
				return cmp.Compare(lhsCountAfter, rhsCountAfter)
			})
		}
		allMatchIdxes = append(allMatchIdxes, idxWrapper{matchIdxes, pathIdx})
	}
	// sort allMatchIdxes by its element length.
	// index merge:                index merge:
	//  path1: {pk, index1}  ==>    path2: {pk}
	//  path2: {pk}                 path1: {pk, index1}
	// here for the fixed choice pk of path2, let it be the first one to choose, left choice of index1 to path1.
	slices.SortStableFunc(allMatchIdxes, func(a, b idxWrapper) int {
		lhsLen := len(a.matchIdx)
		rhsLen := len(b.matchIdx)
		return cmp.Compare(lhsLen, rhsLen)
	})
	for _, matchIdxes := range allMatchIdxes {
		// since matchIdxes are ordered by matchIdxes's length,
		// we should use matchIdxes.pathIdx to locate where it comes from.
		alternatives := path.PartialAlternativeIndexPaths[matchIdxes.pathIdx]
		found := false
		// pick a most suitable index partial alternative from all matched alternative paths according to asc CountAfterAccess,
		// By this way, a distinguished one is better.
		for _, oneIdx := range matchIdxes.matchIdx {
			var indexID int64
			if alternatives[oneIdx].IsTablePath() {
				indexID = -1
			} else {
				indexID = alternatives[oneIdx].Index.ID
			}
			if _, ok := usedIndexMap[indexID]; !ok {
				// try to avoid all index partial paths are all about a single index.
				determinedIndexPartialPaths = append(determinedIndexPartialPaths, alternatives[oneIdx].Clone())
				usedIndexMap[indexID] = struct{}{}
				found = true
				break
			}
		}
		if !found {
			// just pick the same name index (just using the first one is ok), in case that there may be some other
			// picked distinctive index path for other partial paths latter.
			determinedIndexPartialPaths = append(determinedIndexPartialPaths, alternatives[matchIdxes.matchIdx[0]].Clone())
			// uedIndexMap[oneItemAlternatives[oneIdx].Index.ID] = struct{}{} must already be colored.
		}
	}
	if len(usedIndexMap) == 1 {
		// if all partial path are using a same index, meaningless and fail over.
		return nil, false
	}
	// step2: gen a new **concrete** index merge path.
	indexMergePath := &util.AccessPath{
		PartialIndexPaths:        determinedIndexPartialPaths,
		IndexMergeIsIntersection: false,
		// inherit those determined can't pushed-down table filters.
		TableFilters: path.TableFilters,
	}
	// path.ShouldBeKeptCurrentFilter record that whether there are some part of the cnf item couldn't be pushed down to tikv already.
	shouldKeepCurrentFilter := path.KeepIndexMergeORSourceFilter
	pushDownCtx := GetPushDownCtx(ds.SCtx())
	for _, path := range determinedIndexPartialPaths {
		// If any partial path contains table filters, we need to keep the whole DNF filter in the Selection.
		if len(path.TableFilters) > 0 {
			if !expression.CanExprsPushDown(pushDownCtx, path.TableFilters, kv.TiKV) {
				// if this table filters can't be pushed down, all of them should be kept in the table side, cleaning the lookup side here.
				path.TableFilters = nil
			}
			shouldKeepCurrentFilter = true
		}
		// If any partial path's index filter cannot be pushed to TiKV, we should keep the whole DNF filter.
		if len(path.IndexFilters) != 0 && !expression.CanExprsPushDown(pushDownCtx, path.IndexFilters, kv.TiKV) {
			shouldKeepCurrentFilter = true
			// Clear IndexFilter, the whole filter will be put in indexMergePath.TableFilters.
			path.IndexFilters = nil
		}
	}
	// Keep this filter as a part of table filters for safety if it has any parameter.
	if expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), []expression.Expression{path.IndexMergeORSourceFilter}) {
		shouldKeepCurrentFilter = true
	}
	if shouldKeepCurrentFilter {
		// add the cnf expression back as table filer.
		indexMergePath.TableFilters = append(indexMergePath.TableFilters, path.IndexMergeORSourceFilter)
	}

	// step3: after the index merge path is determined, compute the countAfterAccess as usual.
	accessConds := make([]expression.Expression, 0, len(determinedIndexPartialPaths))
	for _, p := range determinedIndexPartialPaths {
		indexCondsForP := p.AccessConds[:]
		indexCondsForP = append(indexCondsForP, p.IndexFilters...)
		if len(indexCondsForP) > 0 {
			accessConds = append(accessConds, expression.ComposeCNFCondition(ds.SCtx().GetExprCtx(), indexCondsForP...))
		}
	}
	accessDNF := expression.ComposeDNFCondition(ds.SCtx().GetExprCtx(), accessConds...)
	sel, _, err := cardinality.Selectivity(ds.SCtx(), ds.tableStats.HistColl, []expression.Expression{accessDNF}, nil)
	if err != nil {
		logutil.BgLogger().Debug("something wrong happened, use the default selectivity", zap.Error(err))
		sel = cost.SelectionFactor
	}
	indexMergePath.CountAfterAccess = sel * ds.tableStats.RowCount
	if noSortItem {
		// since there is no sort property, index merge case is generated by random combination, each alternative with the lower/lowest
		// countAfterAccess, here the returned matchProperty should be false.
		return indexMergePath, false
	}
	return indexMergePath, true
}

func (ds *DataSource) isMatchPropForIndexMerge(path *util.AccessPath, prop *property.PhysicalProperty) bool {
	// Execution part doesn't support the merge operation for intersection case yet.
	if path.IndexMergeIsIntersection {
		return false
	}
	allSame, _ := prop.AllSameOrder()
	if !allSame {
		return false
	}
	for _, partialPath := range path.PartialIndexPaths {
		if !ds.isMatchProp(partialPath, prop) {
			return false
		}
	}
	return true
}

func (ds *DataSource) getTableCandidate(path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.isMatchProp = ds.isMatchProp(path, prop)
	candidate.accessCondsColMap = util.ExtractCol2Len(ds.SCtx().GetExprCtx().GetEvalCtx(), path.AccessConds, nil, nil)
	return candidate
}

func (ds *DataSource) getIndexCandidate(path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.isMatchProp = ds.isMatchProp(path, prop)
	candidate.accessCondsColMap = util.ExtractCol2Len(ds.SCtx().GetExprCtx().GetEvalCtx(), path.AccessConds, path.IdxCols, path.IdxColLens)
	candidate.indexCondsColMap = util.ExtractCol2Len(ds.SCtx().GetExprCtx().GetEvalCtx(), append(path.AccessConds, path.IndexFilters...), path.FullIdxCols, path.FullIdxColLens)
	return candidate
}

func (ds *DataSource) convergeIndexMergeCandidate(path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	// since the all index path alternative paths is collected and undetermined, and we should determine a possible and concrete path for this prop.
	possiblePath, match := ds.matchPropForIndexMergeAlternatives(path, prop)
	if possiblePath == nil {
		return nil
	}
	candidate := &candidatePath{path: possiblePath, isMatchProp: match}
	return candidate
}

func (ds *DataSource) getIndexMergeCandidate(path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.isMatchProp = ds.isMatchPropForIndexMerge(path, prop)
	return candidate
}

// skylinePruning prunes access paths according to different factors. An access path can be pruned only if
// there exists a path that is not worse than it at all factors and there is at least one better factor.
func (ds *DataSource) skylinePruning(prop *property.PhysicalProperty) []*candidatePath {
	candidates := make([]*candidatePath, 0, 4)
	for _, path := range ds.possibleAccessPaths {
		// We should check whether the possible access path is valid first.
		if path.StoreType != kv.TiFlash && prop.IsFlashProp() {
			continue
		}
		if len(path.PartialAlternativeIndexPaths) > 0 {
			// OR normal index merge path, try to determine every index partial path for this property.
			candidate := ds.convergeIndexMergeCandidate(path, prop)
			if candidate != nil {
				candidates = append(candidates, candidate)
			}
			continue
		}
		if path.PartialIndexPaths != nil {
			candidates = append(candidates, ds.getIndexMergeCandidate(path, prop))
			continue
		}
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.Ranges) == 0 {
			return []*candidatePath{{path: path}}
		}
		var currentCandidate *candidatePath
		if path.IsTablePath() {
			currentCandidate = ds.getTableCandidate(path, prop)
		} else {
			if !(len(path.AccessConds) > 0 || !prop.IsSortItemEmpty() || path.Forced || path.IsSingleScan) {
				continue
			}
			// We will use index to generate physical plan if any of the following conditions is satisfied:
			// 1. This path's access cond is not nil.
			// 2. We have a non-empty prop to match.
			// 3. This index is forced to choose.
			// 4. The needed columns are all covered by index columns(and handleCol).
			currentCandidate = ds.getIndexCandidate(path, prop)
		}
		pruned := false
		for i := len(candidates) - 1; i >= 0; i-- {
			if candidates[i].path.StoreType == kv.TiFlash {
				continue
			}
			result := compareCandidates(ds.SCtx(), prop, candidates[i], currentCandidate)
			if result == 1 {
				pruned = true
				// We can break here because the current candidate cannot prune others anymore.
				break
			} else if result == -1 {
				candidates = append(candidates[:i], candidates[i+1:]...)
			}
		}
		if !pruned {
			candidates = append(candidates, currentCandidate)
		}
	}

	if ds.SCtx().GetSessionVars().GetAllowPreferRangeScan() && len(candidates) > 1 {
		// If a candidate path is TiFlash-path or forced-path, we just keep them. For other candidate paths, if there exists
		// any range scan path, we remove full scan paths and keep range scan paths.
		preferredPaths := make([]*candidatePath, 0, len(candidates))
		var hasRangeScanPath bool
		for _, c := range candidates {
			if c.path.Forced || c.path.StoreType == kv.TiFlash {
				preferredPaths = append(preferredPaths, c)
				continue
			}
			var unsignedIntHandle bool
			if c.path.IsIntHandlePath && ds.tableInfo.PKIsHandle {
				if pkColInfo := ds.tableInfo.GetPkColInfo(); pkColInfo != nil {
					unsignedIntHandle = mysql.HasUnsignedFlag(pkColInfo.GetFlag())
				}
			}
			if !ranger.HasFullRange(c.path.Ranges, unsignedIntHandle) {
				preferredPaths = append(preferredPaths, c)
				hasRangeScanPath = true
			}
		}
		if hasRangeScanPath {
			return preferredPaths
		}
	}

	return candidates
}

func (ds *DataSource) getPruningInfo(candidates []*candidatePath, prop *property.PhysicalProperty) string {
	if len(candidates) == len(ds.possibleAccessPaths) {
		return ""
	}
	if len(candidates) == 1 && len(candidates[0].path.Ranges) == 0 {
		// For TableDual, we don't need to output pruning info.
		return ""
	}
	names := make([]string, 0, len(candidates))
	var tableName string
	if ds.TableAsName.O == "" {
		tableName = ds.tableInfo.Name.O
	} else {
		tableName = ds.TableAsName.O
	}
	getSimplePathName := func(path *util.AccessPath) string {
		if path.IsTablePath() {
			if path.StoreType == kv.TiFlash {
				return tableName + "(tiflash)"
			}
			return tableName
		}
		return path.Index.Name.O
	}
	for _, cand := range candidates {
		if cand.path.PartialIndexPaths != nil {
			partialNames := make([]string, 0, len(cand.path.PartialIndexPaths))
			for _, partialPath := range cand.path.PartialIndexPaths {
				partialNames = append(partialNames, getSimplePathName(partialPath))
			}
			names = append(names, fmt.Sprintf("IndexMerge{%s}", strings.Join(partialNames, ",")))
		} else {
			names = append(names, getSimplePathName(cand.path))
		}
	}
	items := make([]string, 0, len(prop.SortItems))
	for _, item := range prop.SortItems {
		items = append(items, item.String())
	}
	return fmt.Sprintf("[%s] remain after pruning paths for %s given Prop{SortItems: [%s], TaskTp: %s}",
		strings.Join(names, ","), tableName, strings.Join(items, " "), prop.TaskTp)
}

func (ds *DataSource) isPointGetConvertableSchema() bool {
	for _, col := range ds.Columns {
		if col.Name.L == model.ExtraHandleName.L {
			continue
		}

		// Only handle tables that all columns are public.
		if col.State != model.StatePublic {
			return false
		}
	}
	return true
}

// exploreEnforcedPlan determines whether to explore enforced plans for this DataSource if it has already found an unenforced plan.
// See #46177 for more information.
func (ds *DataSource) exploreEnforcedPlan() bool {
	// default value is false to keep it compatible with previous versions.
	return fixcontrol.GetBoolWithDefault(ds.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix46177, false)
}

// FindBestTask implements the PhysicalPlan interface.
// It will enumerate all the available indices and choose a plan with least cost.
func (ds *DataSource) FindBestTask(prop *property.PhysicalProperty, planCounter *base.PlanCounterTp, opt *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error) {
	// If ds is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		planCounter.Dec(1)
		return nil, 1, nil
	}
	if ds.isForUpdateRead && ds.SCtx().GetSessionVars().TxnCtx.IsExplicit {
		hasPointGetPath := false
		for _, path := range ds.possibleAccessPaths {
			if ds.isPointGetPath(path) {
				hasPointGetPath = true
				break
			}
		}
		tblName := ds.tableInfo.Name
		ds.possibleAccessPaths, err = filterPathByIsolationRead(ds.SCtx(), ds.possibleAccessPaths, tblName, ds.DBName)
		if err != nil {
			return nil, 1, err
		}
		if hasPointGetPath {
			newPaths := make([]*util.AccessPath, 0)
			for _, path := range ds.possibleAccessPaths {
				// if the path is the point get range path with for update lock, we should forbid tiflash as it's store path (#39543)
				if path.StoreType != kv.TiFlash {
					newPaths = append(newPaths, path)
				}
			}
			ds.possibleAccessPaths = newPaths
		}
	}
	t = ds.GetTask(prop)
	if t != nil {
		cntPlan = 1
		planCounter.Dec(1)
		return
	}
	var cnt int64
	var unenforcedTask base.Task
	// If prop.CanAddEnforcer is true, the prop.SortItems need to be set nil for ds.findBestTask.
	// Before function return, reset it for enforcing task prop and storing map<prop,task>.
	oldProp := prop.CloneEssentialFields()
	if prop.CanAddEnforcer {
		// First, get the bestTask without enforced prop
		prop.CanAddEnforcer = false
		unenforcedTask, cnt, err = ds.FindBestTask(prop, planCounter, opt)
		if err != nil {
			return nil, 0, err
		}
		if !unenforcedTask.Invalid() && !ds.exploreEnforcedPlan() {
			ds.StoreTask(prop, unenforcedTask)
			return unenforcedTask, cnt, nil
		}

		// Then, explore the bestTask with enforced prop
		prop.CanAddEnforcer = true
		cntPlan += cnt
		prop.SortItems = []property.SortItem{}
		prop.MPPPartitionTp = property.AnyType
	} else if prop.MPPPartitionTp != property.AnyType {
		return base.InvalidTask, 0, nil
	}
	defer func() {
		if err != nil {
			return
		}
		if prop.CanAddEnforcer {
			*prop = *oldProp
			t = enforceProperty(prop, t, ds.Plan.SCtx())
			prop.CanAddEnforcer = true
		}

		if unenforcedTask != nil && !unenforcedTask.Invalid() {
			curIsBest, cerr := compareTaskCost(unenforcedTask, t, opt)
			if cerr != nil {
				err = cerr
				return
			}
			if curIsBest {
				t = unenforcedTask
			}
		}

		ds.StoreTask(prop, t)
		err = validateTableSamplePlan(ds, t, err)
	}()

	t, err = ds.tryToGetDualTask()
	if err != nil || t != nil {
		planCounter.Dec(1)
		if t != nil {
			appendCandidate(ds, t, prop, opt)
		}
		return t, 1, err
	}

	t = base.InvalidTask
	candidates := ds.skylinePruning(prop)
	pruningInfo := ds.getPruningInfo(candidates, prop)
	defer func() {
		if err == nil && t != nil && !t.Invalid() && pruningInfo != "" {
			warnErr := errors.NewNoStackError(pruningInfo)
			if ds.SCtx().GetSessionVars().StmtCtx.InVerboseExplain {
				ds.SCtx().GetSessionVars().StmtCtx.AppendNote(warnErr)
			} else {
				ds.SCtx().GetSessionVars().StmtCtx.AppendExtraNote(warnErr)
			}
		}
	}()

	cntPlan = 0
	for _, candidate := range candidates {
		path := candidate.path
		if path.PartialIndexPaths != nil {
			idxMergeTask, err := ds.convertToIndexMergeScan(prop, candidate, opt)
			if err != nil {
				return nil, 0, err
			}
			if !idxMergeTask.Invalid() {
				cntPlan++
				planCounter.Dec(1)
			}
			appendCandidate(ds, idxMergeTask, prop, opt)

			curIsBetter, err := compareTaskCost(idxMergeTask, t, opt)
			if err != nil {
				return nil, 0, err
			}
			if curIsBetter || planCounter.Empty() {
				t = idxMergeTask
			}
			if planCounter.Empty() {
				return t, cntPlan, nil
			}
			continue
		}
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.Ranges) == 0 {
			// We should uncache the tableDual plan.
			if expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), path.AccessConds) {
				ds.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("get a TableDual plan")
			}
			dual := PhysicalTableDual{}.Init(ds.SCtx(), ds.StatsInfo(), ds.QueryBlockOffset())
			dual.SetSchema(ds.schema)
			cntPlan++
			planCounter.Dec(1)
			t := &RootTask{}
			t.SetPlan(dual)
			appendCandidate(ds, t, prop, opt)
			return t, cntPlan, nil
		}

		canConvertPointGet := len(path.Ranges) > 0 && path.StoreType == kv.TiKV && ds.isPointGetConvertableSchema()

		if canConvertPointGet && path.Index != nil && path.Index.MVIndex {
			canConvertPointGet = false // cannot use PointGet upon MVIndex
		}

		if canConvertPointGet && !path.IsIntHandlePath {
			// We simply do not build [batch] point get for prefix indexes. This can be optimized.
			canConvertPointGet = path.Index.Unique && !path.Index.HasPrefixIndex()
			// If any range cannot cover all columns of the index, we cannot build [batch] point get.
			idxColsLen := len(path.Index.Columns)
			for _, ran := range path.Ranges {
				if len(ran.LowVal) != idxColsLen {
					canConvertPointGet = false
					break
				}
			}
		}
		if canConvertPointGet && ds.table.Meta().GetPartitionInfo() != nil {
			// partition table with dynamic prune not support batchPointGet
			// Due to sorting?
			if canConvertPointGet && len(path.Ranges) > 1 && ds.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
				canConvertPointGet = false
			}
			if canConvertPointGet && len(path.Ranges) > 1 {
				// TODO: This is now implemented, but to decrease
				// the impact of supporting plan cache for patitioning,
				// this is not yet enabled.
				// TODO: just remove this if block and update/add tests...
				// We can only build batch point get for hash partitions on a simple column now. This is
				// decided by the current implementation of `BatchPointGetExec::initialize()`, specifically,
				// the `getPhysID()` function. Once we optimize that part, we can come back and enable
				// BatchPointGet plan for more cases.
				hashPartColName := getHashOrKeyPartitionColumnName(ds.SCtx(), ds.table.Meta())
				if hashPartColName == nil {
					canConvertPointGet = false
				}
			}
			if canConvertPointGet {
				// If the schema contains ExtraPidColID, do not convert to point get.
				// Because the point get executor can not handle the extra partition ID column now.
				// I.e. Global Index is used
				for _, col := range ds.schema.Columns {
					if col.ID == model.ExtraPidColID {
						canConvertPointGet = false
						break
					}
				}
				if path != nil && path.Index != nil && path.Index.Global {
					// Don't convert to point get during ddl
					// TODO: Revisit truncate partition and global index
					if len(ds.tableInfo.GetPartitionInfo().DroppingDefinitions) > 0 ||
						len(ds.tableInfo.GetPartitionInfo().AddingDefinitions) > 0 {
						canConvertPointGet = false
					}
				}
			}
		}
		if canConvertPointGet {
			allRangeIsPoint := true
			tc := ds.SCtx().GetSessionVars().StmtCtx.TypeCtx()
			for _, ran := range path.Ranges {
				if !ran.IsPointNonNullable(tc) {
					// unique indexes can have duplicated NULL rows so we cannot use PointGet if there is NULL
					allRangeIsPoint = false
					break
				}
			}
			if allRangeIsPoint {
				var pointGetTask base.Task
				if len(path.Ranges) == 1 {
					pointGetTask = ds.convertToPointGet(prop, candidate)
				} else {
					pointGetTask = ds.convertToBatchPointGet(prop, candidate)
				}

				// Batch/PointGet plans may be over-optimized, like `a>=1(?) and a<=1(?)` --> `a=1` --> PointGet(a=1).
				// For safety, prevent these plans from the plan cache here.
				if !pointGetTask.Invalid() && expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), candidate.path.AccessConds) && !isSafePointGetPath4PlanCache(ds.SCtx(), candidate.path) {
					ds.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("Batch/PointGet plans may be over-optimized")
				}

				appendCandidate(ds, pointGetTask, prop, opt)
				if !pointGetTask.Invalid() {
					cntPlan++
					planCounter.Dec(1)
				}
				curIsBetter, cerr := compareTaskCost(pointGetTask, t, opt)
				if cerr != nil {
					return nil, 0, cerr
				}
				if curIsBetter || planCounter.Empty() {
					t = pointGetTask
					if planCounter.Empty() {
						return
					}
					continue
				}
			}
		}
		if path.IsTablePath() {
			if ds.preferStoreType&h.PreferTiFlash != 0 && path.StoreType == kv.TiKV {
				continue
			}
			if ds.preferStoreType&h.PreferTiKV != 0 && path.StoreType == kv.TiFlash {
				continue
			}
			var tblTask base.Task
			if ds.SampleInfo != nil {
				tblTask, err = ds.convertToSampleTable(prop, candidate, opt)
			} else {
				tblTask, err = ds.convertToTableScan(prop, candidate, opt)
			}
			if err != nil {
				return nil, 0, err
			}
			if !tblTask.Invalid() {
				cntPlan++
				planCounter.Dec(1)
			}
			appendCandidate(ds, tblTask, prop, opt)
			curIsBetter, err := compareTaskCost(tblTask, t, opt)
			if err != nil {
				return nil, 0, err
			}
			if curIsBetter || planCounter.Empty() {
				t = tblTask
			}
			if planCounter.Empty() {
				return t, cntPlan, nil
			}
			continue
		}
		// TiFlash storage do not support index scan.
		if ds.preferStoreType&h.PreferTiFlash != 0 {
			continue
		}
		idxTask, err := ds.convertToIndexScan(prop, candidate, opt)
		if err != nil {
			return nil, 0, err
		}
		if !idxTask.Invalid() {
			cntPlan++
			planCounter.Dec(1)
		}
		appendCandidate(ds, idxTask, prop, opt)
		curIsBetter, err := compareTaskCost(idxTask, t, opt)
		if err != nil {
			return nil, 0, err
		}
		if curIsBetter || planCounter.Empty() {
			t = idxTask
		}
		if planCounter.Empty() {
			return t, cntPlan, nil
		}
	}

	return
}

// convertToIndexMergeScan builds the index merge scan for intersection or union cases.
func (ds *DataSource) convertToIndexMergeScan(prop *property.PhysicalProperty, candidate *candidatePath, _ *optimizetrace.PhysicalOptimizeOp) (task base.Task, err error) {
	if prop.IsFlashProp() || prop.TaskTp == property.CopSingleReadTaskType {
		return base.InvalidTask, nil
	}
	// lift the limitation of that double read can not build index merge **COP** task with intersection.
	// that means we can output a cop task here without encapsulating it as root task, for the convenience of attaching limit to its table side.

	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return base.InvalidTask, nil
	}
	// while for now, we still can not push the sort prop to the intersection index plan side, temporarily banned here.
	if !prop.IsSortItemEmpty() && candidate.path.IndexMergeIsIntersection {
		return base.InvalidTask, nil
	}
	failpoint.Inject("forceIndexMergeKeepOrder", func(_ failpoint.Value) {
		if len(candidate.path.PartialIndexPaths) > 0 && !candidate.path.IndexMergeIsIntersection {
			if prop.IsSortItemEmpty() {
				failpoint.Return(base.InvalidTask, nil)
			}
		}
	})
	path := candidate.path
	scans := make([]base.PhysicalPlan, 0, len(path.PartialIndexPaths))
	cop := &CopTask{
		indexPlanFinished: false,
		tblColHists:       ds.TblColHists,
	}
	cop.physPlanPartInfo = PhysPlanPartInfo{
		PruningConds:   pushDownNot(ds.SCtx().GetExprCtx(), ds.allConds),
		PartitionNames: ds.partitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.names,
	}
	// Add sort items for index scan for merge-sort operation between partitions.
	byItems := make([]*util.ByItems, 0, len(prop.SortItems))
	for _, si := range prop.SortItems {
		byItems = append(byItems, &util.ByItems{
			Expr: si.Col,
			Desc: si.Desc,
		})
	}
	for _, partPath := range path.PartialIndexPaths {
		var scan base.PhysicalPlan
		if partPath.IsTablePath() {
			scan = ds.convertToPartialTableScan(prop, partPath, candidate.isMatchProp, byItems)
		} else {
			scan, err = ds.convertToPartialIndexScan(&cop.physPlanPartInfo, prop, partPath, candidate.isMatchProp, byItems)
			if err != nil {
				return base.InvalidTask, err
			}
		}
		scans = append(scans, scan)
	}
	totalRowCount := path.CountAfterAccess
	if prop.ExpectedCnt < ds.StatsInfo().RowCount {
		totalRowCount *= prop.ExpectedCnt / ds.StatsInfo().RowCount
	}
	ts, remainingFilters, moreColumn, err := ds.buildIndexMergeTableScan(path.TableFilters, totalRowCount, candidate.isMatchProp)
	if err != nil {
		return base.InvalidTask, err
	}
	if prop.TaskTp != property.RootTaskType && len(remainingFilters) > 0 {
		return base.InvalidTask, nil
	}
	cop.keepOrder = candidate.isMatchProp
	cop.tablePlan = ts
	cop.idxMergePartPlans = scans
	cop.idxMergeIsIntersection = path.IndexMergeIsIntersection
	cop.idxMergeAccessMVIndex = path.IndexMergeAccessMVIndex
	if moreColumn {
		cop.needExtraProj = true
		cop.originSchema = ds.Schema()
	}
	if remainingFilters != nil {
		cop.rootTaskConds = remainingFilters
	}
	// after we lift the limitation of intersection and cop-type task in the code in this
	// function above, we could set its index plan finished as true once we found its table
	// plan is pure table scan below.
	// And this will cause cost underestimation when we estimate the cost of the entire cop
	// task plan in function `getTaskPlanCost`.
	if prop.TaskTp == property.RootTaskType {
		cop.indexPlanFinished = true
		task = cop.ConvertToRootTask(ds.SCtx())
	} else {
		_, pureTableScan := ts.(*PhysicalTableScan)
		if !pureTableScan {
			cop.indexPlanFinished = true
		}
		task = cop
	}
	return task, nil
}

func (ds *DataSource) convertToPartialIndexScan(physPlanPartInfo *PhysPlanPartInfo, prop *property.PhysicalProperty, path *util.AccessPath, matchProp bool, byItems []*util.ByItems) (base.PhysicalPlan, error) {
	is := ds.getOriginalPhysicalIndexScan(prop, path, matchProp, false)
	// TODO: Consider using isIndexCoveringColumns() to avoid another TableRead
	indexConds := path.IndexFilters
	if matchProp {
		if is.Table.GetPartitionInfo() != nil && !is.Index.Global && is.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
			is.Columns, is.schema, _ = AddExtraPhysTblIDColumn(is.SCtx(), is.Columns, is.schema)
		}
		// Add sort items for index scan for merge-sort operation between partitions.
		is.ByItems = byItems
	}

	// Add a `Selection` for `IndexScan` with global index.
	// It should pushdown to TiKV, DataSource schema doesn't contain partition id column.
	indexConds, err := is.addSelectionConditionForGlobalIndex(ds, physPlanPartInfo, indexConds)
	if err != nil {
		return nil, err
	}

	if len(indexConds) > 0 {
		var selectivity float64
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		rowCount := is.StatsInfo().RowCount * selectivity
		stats := &property.StatsInfo{RowCount: rowCount}
		stats.StatsVersion = ds.statisticTable.Version
		if ds.statisticTable.Pseudo {
			stats.StatsVersion = statistics.PseudoVersion
		}
		indexPlan := PhysicalSelection{Conditions: indexConds}.Init(is.SCtx(), stats, ds.QueryBlockOffset())
		indexPlan.SetChildren(is)
		return indexPlan, nil
	}
	return is, nil
}

func checkColinSchema(cols []*expression.Column, schema *expression.Schema) bool {
	for _, col := range cols {
		if schema.ColumnIndex(col) == -1 {
			return false
		}
	}
	return true
}

func (ds *DataSource) convertToPartialTableScan(prop *property.PhysicalProperty, path *util.AccessPath, matchProp bool, byItems []*util.ByItems) (tablePlan base.PhysicalPlan) {
	ts, rowCount := ds.getOriginalPhysicalTableScan(prop, path, matchProp)
	overwritePartialTableScanSchema(ds, ts)
	// remove ineffetive filter condition after overwriting physicalscan schema
	newFilterConds := make([]expression.Expression, 0, len(path.TableFilters))
	for _, cond := range ts.filterCondition {
		cols := expression.ExtractColumns(cond)
		if checkColinSchema(cols, ts.schema) {
			newFilterConds = append(newFilterConds, cond)
		}
	}
	ts.filterCondition = newFilterConds
	if matchProp {
		if ts.Table.GetPartitionInfo() != nil && ts.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
			ts.Columns, ts.schema, _ = AddExtraPhysTblIDColumn(ts.SCtx(), ts.Columns, ts.schema)
		}
		ts.ByItems = byItems
	}
	if len(ts.filterCondition) > 0 {
		selectivity, _, err := cardinality.Selectivity(ds.SCtx(), ds.tableStats.HistColl, ts.filterCondition, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = cost.SelectionFactor
		}
		tablePlan = PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.SCtx(), ts.StatsInfo().ScaleByExpectCnt(selectivity*rowCount), ds.QueryBlockOffset())
		tablePlan.SetChildren(ts)
		return tablePlan
	}
	tablePlan = ts
	return tablePlan
}

// overwritePartialTableScanSchema change the schema of partial table scan to handle columns.
func overwritePartialTableScanSchema(ds *DataSource, ts *PhysicalTableScan) {
	handleCols := ds.handleCols
	if handleCols == nil {
		handleCols = util.NewIntHandleCols(ds.newExtraHandleSchemaCol())
	}
	hdColNum := handleCols.NumCols()
	exprCols := make([]*expression.Column, 0, hdColNum)
	infoCols := make([]*model.ColumnInfo, 0, hdColNum)
	for i := 0; i < hdColNum; i++ {
		col := handleCols.GetCol(i)
		exprCols = append(exprCols, col)
		if c := model.FindColumnInfoByID(ds.TableInfo().Columns, col.ID); c != nil {
			infoCols = append(infoCols, c)
		} else {
			infoCols = append(infoCols, col.ToInfo())
		}
	}
	ts.schema = expression.NewSchema(exprCols...)
	ts.Columns = infoCols
}

// setIndexMergeTableScanHandleCols set the handle columns of the table scan.
func setIndexMergeTableScanHandleCols(ds *DataSource, ts *PhysicalTableScan) (err error) {
	handleCols := ds.handleCols
	if handleCols == nil {
		handleCols = util.NewIntHandleCols(ds.newExtraHandleSchemaCol())
	}
	hdColNum := handleCols.NumCols()
	exprCols := make([]*expression.Column, 0, hdColNum)
	for i := 0; i < hdColNum; i++ {
		col := handleCols.GetCol(i)
		exprCols = append(exprCols, col)
	}
	ts.HandleCols, err = handleCols.ResolveIndices(expression.NewSchema(exprCols...))
	return
}

// buildIndexMergeTableScan() returns Selection that will be pushed to TiKV.
// Filters that cannot be pushed to TiKV are also returned, and an extra Selection above IndexMergeReader will be constructed later.
func (ds *DataSource) buildIndexMergeTableScan(tableFilters []expression.Expression,
	totalRowCount float64, matchProp bool) (base.PhysicalPlan, []expression.Expression, bool, error) {
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         slices.Clone(ds.Columns),
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.partitionDefIdx != nil,
		physicalTableID: ds.physicalTableID,
		HandleCols:      ds.handleCols,
		tblCols:         ds.TblCols,
		tblColHists:     ds.TblColHists,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	ts.SetSchema(ds.schema.Clone())
	err := setIndexMergeTableScanHandleCols(ds, ts)
	if err != nil {
		return nil, nil, false, err
	}
	ts.SetStats(ds.tableStats.ScaleByExpectCnt(totalRowCount))
	usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(ts.physicalTableID) != nil {
		ts.usedStatsInfo = usedStats.GetUsedInfo(ts.physicalTableID)
	}
	if ds.statisticTable.Pseudo {
		ts.StatsInfo().StatsVersion = statistics.PseudoVersion
	}
	var currentTopPlan base.PhysicalPlan = ts
	if len(tableFilters) > 0 {
		pushedFilters, remainingFilters := extractFiltersForIndexMerge(GetPushDownCtx(ds.SCtx()), tableFilters)
		pushedFilters1, remainingFilters1 := SplitSelCondsWithVirtualColumn(pushedFilters)
		pushedFilters = pushedFilters1
		remainingFilters = append(remainingFilters, remainingFilters1...)
		if len(pushedFilters) != 0 {
			selectivity, _, err := cardinality.Selectivity(ds.SCtx(), ds.tableStats.HistColl, pushedFilters, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = cost.SelectionFactor
			}
			sel := PhysicalSelection{Conditions: pushedFilters}.Init(ts.SCtx(), ts.StatsInfo().ScaleByExpectCnt(selectivity*totalRowCount), ts.QueryBlockOffset())
			sel.SetChildren(ts)
			currentTopPlan = sel
		}
		if len(remainingFilters) > 0 {
			return currentTopPlan, remainingFilters, false, nil
		}
	}
	// If we don't need to use ordered scan, we don't need do the following codes for adding new columns.
	if !matchProp {
		return currentTopPlan, nil, false, nil
	}

	// Add the row handle into the schema.
	columnAdded := false
	if ts.Table.PKIsHandle {
		pk := ts.Table.GetPkColInfo()
		pkCol := expression.ColInfo2Col(ts.tblCols, pk)
		if !ts.schema.Contains(pkCol) {
			ts.schema.Append(pkCol)
			ts.Columns = append(ts.Columns, pk)
			columnAdded = true
		}
	} else if ts.Table.IsCommonHandle {
		idxInfo := ts.Table.GetPrimaryKey()
		for _, idxCol := range idxInfo.Columns {
			col := ts.tblCols[idxCol.Offset]
			if !ts.schema.Contains(col) {
				columnAdded = true
				ts.schema.Append(col)
				ts.Columns = append(ts.Columns, col.ToInfo())
			}
		}
	} else if !ts.schema.Contains(ts.HandleCols.GetCol(0)) {
		ts.schema.Append(ts.HandleCols.GetCol(0))
		ts.Columns = append(ts.Columns, model.NewExtraHandleColInfo())
		columnAdded = true
	}

	// For the global index of the partitioned table, we also need the PhysicalTblID to identify the rows from each partition.
	if ts.Table.GetPartitionInfo() != nil && ts.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		var newColAdded bool
		ts.Columns, ts.schema, newColAdded = AddExtraPhysTblIDColumn(ts.SCtx(), ts.Columns, ts.schema)
		columnAdded = columnAdded || newColAdded
	}
	return currentTopPlan, nil, columnAdded, nil
}

// extractFiltersForIndexMerge returns:
// `pushed`: exprs that can be pushed to TiKV.
// `remaining`: exprs that can NOT be pushed to TiKV but can be pushed to other storage engines.
// Why do we need this func?
// IndexMerge only works on TiKV, so we need to find all exprs that cannot be pushed to TiKV, and add a new Selection above IndexMergeReader.
//
//	But the new Selection should exclude the exprs that can NOT be pushed to ALL the storage engines.
//	Because these exprs have already been put in another Selection(check rule_predicate_push_down).
func extractFiltersForIndexMerge(ctx expression.PushDownContext, filters []expression.Expression) (pushed []expression.Expression, remaining []expression.Expression) {
	for _, expr := range filters {
		if expression.CanExprsPushDown(ctx, []expression.Expression{expr}, kv.TiKV) {
			pushed = append(pushed, expr)
			continue
		}
		if expression.CanExprsPushDown(ctx, []expression.Expression{expr}, kv.UnSpecified) {
			remaining = append(remaining, expr)
		}
	}
	return
}

func isIndexColsCoveringCol(sctx expression.EvalContext, col *expression.Column, indexCols []*expression.Column, idxColLens []int, ignoreLen bool) bool {
	for i, indexCol := range indexCols {
		if indexCol == nil || !col.EqualByExprAndID(sctx, indexCol) {
			continue
		}
		if ignoreLen || idxColLens[i] == types.UnspecifiedLength || idxColLens[i] == col.RetType.GetFlen() {
			return true
		}
	}
	return false
}

func (ds *DataSource) indexCoveringColumn(column *expression.Column, indexColumns []*expression.Column, idxColLens []int, ignoreLen bool) bool {
	if ds.tableInfo.PKIsHandle && mysql.HasPriKeyFlag(column.RetType.GetFlag()) {
		return true
	}
	if column.ID == model.ExtraHandleID || column.ID == model.ExtraPhysTblID {
		return true
	}
	evalCtx := ds.SCtx().GetExprCtx().GetEvalCtx()
	coveredByPlainIndex := isIndexColsCoveringCol(evalCtx, column, indexColumns, idxColLens, ignoreLen)
	coveredByClusteredIndex := isIndexColsCoveringCol(evalCtx, column, ds.commonHandleCols, ds.commonHandleLens, ignoreLen)
	if !coveredByPlainIndex && !coveredByClusteredIndex {
		return false
	}
	isClusteredNewCollationIdx := collate.NewCollationEnabled() &&
		column.GetType(evalCtx).EvalType() == types.ETString &&
		!mysql.HasBinaryFlag(column.GetType(evalCtx).GetFlag())
	if !coveredByPlainIndex && coveredByClusteredIndex && isClusteredNewCollationIdx && ds.table.Meta().CommonHandleVersion == 0 {
		return false
	}
	return true
}

func (ds *DataSource) isIndexCoveringColumns(columns, indexColumns []*expression.Column, idxColLens []int) bool {
	for _, col := range columns {
		if !ds.indexCoveringColumn(col, indexColumns, idxColLens, false) {
			return false
		}
	}
	return true
}

func (ds *DataSource) isIndexCoveringCondition(condition expression.Expression, indexColumns []*expression.Column, idxColLens []int) bool {
	switch v := condition.(type) {
	case *expression.Column:
		return ds.indexCoveringColumn(v, indexColumns, idxColLens, false)
	case *expression.ScalarFunction:
		// Even if the index only contains prefix `col`, the index can cover `col is null`.
		if v.FuncName.L == ast.IsNull {
			if col, ok := v.GetArgs()[0].(*expression.Column); ok {
				return ds.indexCoveringColumn(col, indexColumns, idxColLens, true)
			}
		}
		for _, arg := range v.GetArgs() {
			if !ds.isIndexCoveringCondition(arg, indexColumns, idxColLens) {
				return false
			}
		}
		return true
	}
	return true
}

func (ds *DataSource) isSingleScan(indexColumns []*expression.Column, idxColLens []int) bool {
	if !ds.SCtx().GetSessionVars().OptPrefixIndexSingleScan || ds.colsRequiringFullLen == nil {
		// ds.colsRequiringFullLen is set at (*DataSource).PruneColumns. In some cases we don't reach (*DataSource).PruneColumns
		// and ds.colsRequiringFullLen is nil, so we fall back to ds.isIndexCoveringColumns(ds.schema.Columns, indexColumns, idxColLens).
		return ds.isIndexCoveringColumns(ds.schema.Columns, indexColumns, idxColLens)
	}
	if !ds.isIndexCoveringColumns(ds.colsRequiringFullLen, indexColumns, idxColLens) {
		return false
	}
	for _, cond := range ds.allConds {
		if !ds.isIndexCoveringCondition(cond, indexColumns, idxColLens) {
			return false
		}
	}
	return true
}

// If there is a table reader which needs to keep order, we should append a pk to table scan.
func (ts *PhysicalTableScan) appendExtraHandleCol(ds *DataSource) (*expression.Column, bool) {
	handleCols := ds.handleCols
	if handleCols != nil {
		return handleCols.GetCol(0), false
	}
	handleCol := ds.newExtraHandleSchemaCol()
	ts.schema.Append(handleCol)
	ts.Columns = append(ts.Columns, model.NewExtraHandleColInfo())
	return handleCol, true
}

// convertToIndexScan converts the DataSource to index scan with idx.
func (ds *DataSource) convertToIndexScan(prop *property.PhysicalProperty,
	candidate *candidatePath, _ *optimizetrace.PhysicalOptimizeOp) (task base.Task, err error) {
	if candidate.path.Index.MVIndex {
		// MVIndex is special since different index rows may return the same _row_id and this can break some assumptions of IndexReader.
		// Currently only support using IndexMerge to access MVIndex instead of IndexReader.
		// TODO: make IndexReader support accessing MVIndex directly.
		return base.InvalidTask, nil
	}
	if !candidate.path.IsSingleScan {
		// If it's parent requires single read task, return max cost.
		if prop.TaskTp == property.CopSingleReadTaskType {
			return base.InvalidTask, nil
		}
	} else if prop.TaskTp == property.CopMultiReadTaskType {
		// If it's parent requires double read task, return max cost.
		return base.InvalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return base.InvalidTask, nil
	}
	// If we need to keep order for the index scan, we should forbid the non-keep-order index scan when we try to generate the path.
	if prop.IsSortItemEmpty() && candidate.path.ForceKeepOrder {
		return base.InvalidTask, nil
	}
	// If we don't need to keep order for the index scan, we should forbid the non-keep-order index scan when we try to generate the path.
	if !prop.IsSortItemEmpty() && candidate.path.ForceNoKeepOrder {
		return base.InvalidTask, nil
	}
	path := candidate.path
	is := ds.getOriginalPhysicalIndexScan(prop, path, candidate.isMatchProp, candidate.path.IsSingleScan)
	cop := &CopTask{
		indexPlan:   is,
		tblColHists: ds.TblColHists,
		tblCols:     ds.TblCols,
		expectCnt:   uint64(prop.ExpectedCnt),
	}
	cop.physPlanPartInfo = PhysPlanPartInfo{
		PruningConds:   pushDownNot(ds.SCtx().GetExprCtx(), ds.allConds),
		PartitionNames: ds.partitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.names,
	}
	if !candidate.path.IsSingleScan {
		// On this way, it's double read case.
		ts := PhysicalTableScan{
			Columns:         util.CloneColInfos(ds.Columns),
			Table:           is.Table,
			TableAsName:     ds.TableAsName,
			DBName:          ds.DBName,
			isPartition:     ds.partitionDefIdx != nil,
			physicalTableID: ds.physicalTableID,
			tblCols:         ds.TblCols,
			tblColHists:     ds.TblColHists,
		}.Init(ds.SCtx(), is.QueryBlockOffset())
		ts.SetSchema(ds.schema.Clone())
		// We set `StatsVersion` here and fill other fields in `(*copTask).finishIndexPlan`. Since `copTask.indexPlan` may
		// change before calling `(*copTask).finishIndexPlan`, we don't know the stats information of `ts` currently and on
		// the other hand, it may be hard to identify `StatsVersion` of `ts` in `(*copTask).finishIndexPlan`.
		ts.SetStats(&property.StatsInfo{StatsVersion: ds.tableStats.StatsVersion})
		usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
		if usedStats != nil && usedStats.GetUsedInfo(ts.physicalTableID) != nil {
			ts.usedStatsInfo = usedStats.GetUsedInfo(ts.physicalTableID)
		}
		cop.tablePlan = ts
	}
	task = cop
	if cop.tablePlan != nil && ds.tableInfo.IsCommonHandle {
		cop.commonHandleCols = ds.commonHandleCols
		commonHandle := ds.handleCols.(*util.CommonHandleCols)
		for _, col := range commonHandle.GetColumns() {
			if ds.schema.ColumnIndex(col) == -1 {
				ts := cop.tablePlan.(*PhysicalTableScan)
				ts.Schema().Append(col)
				ts.Columns = append(ts.Columns, col.ToInfo())
				cop.needExtraProj = true
			}
		}
	}
	if candidate.isMatchProp {
		cop.keepOrder = true
		if cop.tablePlan != nil && !ds.tableInfo.IsCommonHandle {
			col, isNew := cop.tablePlan.(*PhysicalTableScan).appendExtraHandleCol(ds)
			cop.extraHandleCol = col
			cop.needExtraProj = cop.needExtraProj || isNew
		}

		if ds.tableInfo.GetPartitionInfo() != nil {
			// Add sort items for index scan for merge-sort operation between partitions, only required for local index.
			if !is.Index.Global {
				byItems := make([]*util.ByItems, 0, len(prop.SortItems))
				for _, si := range prop.SortItems {
					byItems = append(byItems, &util.ByItems{
						Expr: si.Col,
						Desc: si.Desc,
					})
				}
				cop.indexPlan.(*PhysicalIndexScan).ByItems = byItems
			}
			if cop.tablePlan != nil && ds.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
				if !is.Index.Global {
					is.Columns, is.schema, _ = AddExtraPhysTblIDColumn(is.SCtx(), is.Columns, is.Schema())
				}
				var succ bool
				// global index for tableScan with keepOrder also need PhysicalTblID
				ts := cop.tablePlan.(*PhysicalTableScan)
				ts.Columns, ts.schema, succ = AddExtraPhysTblIDColumn(ts.SCtx(), ts.Columns, ts.Schema())
				cop.needExtraProj = cop.needExtraProj || succ
			}
		}
	}
	if cop.needExtraProj {
		cop.originSchema = ds.schema
	}
	// prop.IsSortItemEmpty() would always return true when coming to here,
	// so we can just use prop.ExpectedCnt as parameter of addPushedDownSelection.
	finalStats := ds.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt)
	if err = is.addPushedDownSelection(cop, ds, path, finalStats); err != nil {
		return base.InvalidTask, err
	}
	if prop.TaskTp == property.RootTaskType {
		task = task.ConvertToRootTask(ds.SCtx())
	} else if _, ok := task.(*RootTask); ok {
		return base.InvalidTask, nil
	}
	return task, nil
}

func (is *PhysicalIndexScan) getScanRowSize() float64 {
	idx := is.Index
	scanCols := make([]*expression.Column, 0, len(idx.Columns)+1)
	// If `initSchema` has already appended the handle column in schema, just use schema columns, otherwise, add extra handle column.
	if len(idx.Columns) == len(is.schema.Columns) {
		scanCols = append(scanCols, is.schema.Columns...)
		handleCol := is.pkIsHandleCol
		if handleCol != nil {
			scanCols = append(scanCols, handleCol)
		}
	} else {
		scanCols = is.schema.Columns
	}
	return cardinality.GetIndexAvgRowSize(is.SCtx(), is.tblColHists, scanCols, is.Index.Unique)
}

// initSchema is used to set the schema of PhysicalIndexScan. Before calling this,
// make sure the following field of PhysicalIndexScan are initialized:
//
//	PhysicalIndexScan.Table         *model.TableInfo
//	PhysicalIndexScan.Index         *model.IndexInfo
//	PhysicalIndexScan.Index.Columns []*IndexColumn
//	PhysicalIndexScan.IdxCols       []*expression.Column
//	PhysicalIndexScan.Columns       []*model.ColumnInfo
func (is *PhysicalIndexScan) initSchema(idxExprCols []*expression.Column, isDoubleRead bool) {
	indexCols := make([]*expression.Column, len(is.IdxCols), len(is.Index.Columns)+1)
	copy(indexCols, is.IdxCols)

	for i := len(is.IdxCols); i < len(is.Index.Columns); i++ {
		if idxExprCols[i] != nil {
			indexCols = append(indexCols, idxExprCols[i])
		} else {
			// TODO: try to reuse the col generated when building the DataSource.
			indexCols = append(indexCols, &expression.Column{
				ID:       is.Table.Columns[is.Index.Columns[i].Offset].ID,
				RetType:  &is.Table.Columns[is.Index.Columns[i].Offset].FieldType,
				UniqueID: is.SCtx().GetSessionVars().AllocPlanColumnID(),
			})
		}
	}
	is.NeedCommonHandle = is.Table.IsCommonHandle

	if is.NeedCommonHandle {
		for i := len(is.Index.Columns); i < len(idxExprCols); i++ {
			indexCols = append(indexCols, idxExprCols[i])
		}
	}
	setHandle := len(indexCols) > len(is.Index.Columns)
	if !setHandle {
		for i, col := range is.Columns {
			if (mysql.HasPriKeyFlag(col.GetFlag()) && is.Table.PKIsHandle) || col.ID == model.ExtraHandleID {
				indexCols = append(indexCols, is.dataSourceSchema.Columns[i])
				setHandle = true
				break
			}
		}
	}

	if isDoubleRead || is.Index.Global {
		// If it's double read case, the first index must return handle. So we should add extra handle column
		// if there isn't a handle column.
		if !setHandle {
			if !is.Table.IsCommonHandle {
				indexCols = append(indexCols, &expression.Column{
					RetType:  types.NewFieldType(mysql.TypeLonglong),
					ID:       model.ExtraHandleID,
					UniqueID: is.SCtx().GetSessionVars().AllocPlanColumnID(),
					OrigName: model.ExtraHandleName.O,
				})
			}
		}
		// If it's global index, handle and PidColID columns has to be added, so that needed pids can be filtered.
		if is.Index.Global {
			indexCols = append(indexCols, &expression.Column{
				RetType:  types.NewFieldType(mysql.TypeLonglong),
				ID:       model.ExtraPidColID,
				UniqueID: is.SCtx().GetSessionVars().AllocPlanColumnID(),
				OrigName: model.ExtraPartitionIdName.O,
			})
		}
	}

	// If `dataSouceSchema` contains `model.ExtraPhysTblID`, we should add it into `indexScan.schema`
	for _, col := range is.dataSourceSchema.Columns {
		if col.ID == model.ExtraPhysTblID {
			indexCols = append(indexCols, col.Clone().(*expression.Column))
			break
		}
	}

	is.SetSchema(expression.NewSchema(indexCols...))
}

func (is *PhysicalIndexScan) addSelectionConditionForGlobalIndex(p *DataSource, physPlanPartInfo *PhysPlanPartInfo, conditions []expression.Expression) ([]expression.Expression, error) {
	if !is.Index.Global {
		return conditions, nil
	}
	args := make([]expression.Expression, 0, len(p.partitionNames)+1)
	for _, col := range is.schema.Columns {
		if col.ID == model.ExtraPidColID {
			args = append(args, col.Clone())
			break
		}
	}

	if len(args) != 1 {
		return nil, errors.Errorf("Can't find column %s in schema %s", model.ExtraPartitionIdName.O, is.schema)
	}

	// For SQL like 'select x from t partition(p0, p1) use index(idx)',
	// we will add a `Selection` like `in(t._tidb_pid, p0, p1)` into the plan.
	// For truncate/drop partitions, we should only return indexes where partitions still in public state.
	idxArr, err := PartitionPruning(p.SCtx(), p.table.GetPartitionedTable(),
		physPlanPartInfo.PruningConds,
		physPlanPartInfo.PartitionNames,
		physPlanPartInfo.Columns,
		physPlanPartInfo.ColumnNames)
	if err != nil {
		return nil, err
	}
	needNot := false
	pInfo := p.TableInfo().GetPartitionInfo()
	if len(idxArr) == 1 && idxArr[0] == FullRange {
		// Only filter adding and dropping partitions.
		if len(pInfo.AddingDefinitions) == 0 && len(pInfo.DroppingDefinitions) == 0 {
			return conditions, nil
		}
		needNot = true
		for _, p := range pInfo.AddingDefinitions {
			args = append(args, expression.NewInt64Const(p.ID))
		}
		for _, p := range pInfo.DroppingDefinitions {
			args = append(args, expression.NewInt64Const(p.ID))
		}
	} else if len(idxArr) == 0 {
		// add an invalid pid as param for `IN` function
		args = append(args, expression.NewInt64Const(-1))
	} else {
		// `PartitionPruning`` func does not return adding and dropping partitions
		for _, idx := range idxArr {
			args = append(args, expression.NewInt64Const(pInfo.Definitions[idx].ID))
		}
	}
	condition, err := expression.NewFunction(p.SCtx().GetExprCtx(), ast.In, types.NewFieldType(mysql.TypeLonglong), args...)
	if err != nil {
		return nil, err
	}
	if needNot {
		condition, err = expression.NewFunction(p.SCtx().GetExprCtx(), ast.UnaryNot, types.NewFieldType(mysql.TypeLonglong), condition)
		if err != nil {
			return nil, err
		}
	}
	return append(conditions, condition), nil
}

func (is *PhysicalIndexScan) addPushedDownSelection(copTask *CopTask, p *DataSource, path *util.AccessPath, finalStats *property.StatsInfo) error {
	// Add filter condition to table plan now.
	indexConds, tableConds := path.IndexFilters, path.TableFilters
	tableConds, copTask.rootTaskConds = SplitSelCondsWithVirtualColumn(tableConds)

	var newRootConds []expression.Expression
	pctx := GetPushDownCtx(is.SCtx())
	indexConds, newRootConds = expression.PushDownExprs(pctx, indexConds, kv.TiKV)
	copTask.rootTaskConds = append(copTask.rootTaskConds, newRootConds...)

	tableConds, newRootConds = expression.PushDownExprs(pctx, tableConds, kv.TiKV)
	copTask.rootTaskConds = append(copTask.rootTaskConds, newRootConds...)

	// Add a `Selection` for `IndexScan` with global index.
	// It should pushdown to TiKV, DataSource schema doesn't contain partition id column.
	indexConds, err := is.addSelectionConditionForGlobalIndex(p, &copTask.physPlanPartInfo, indexConds)
	if err != nil {
		return err
	}

	if indexConds != nil {
		var selectivity float64
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		count := is.StatsInfo().RowCount * selectivity
		stats := p.tableStats.ScaleByExpectCnt(count)
		indexSel := PhysicalSelection{Conditions: indexConds}.Init(is.SCtx(), stats, is.QueryBlockOffset())
		indexSel.SetChildren(is)
		copTask.indexPlan = indexSel
	}
	if len(tableConds) > 0 {
		copTask.finishIndexPlan()
		tableSel := PhysicalSelection{Conditions: tableConds}.Init(is.SCtx(), finalStats, is.QueryBlockOffset())
		if len(copTask.rootTaskConds) != 0 {
			selectivity, _, err := cardinality.Selectivity(is.SCtx(), copTask.tblColHists, tableConds, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = cost.SelectionFactor
			}
			tableSel.SetStats(copTask.Plan().StatsInfo().Scale(selectivity))
		}
		tableSel.SetChildren(copTask.tablePlan)
		copTask.tablePlan = tableSel
	}
	return nil
}

// NeedExtraOutputCol is designed for check whether need an extra column for
// pid or physical table id when build indexReq.
func (is *PhysicalIndexScan) NeedExtraOutputCol() bool {
	if is.Table.Partition == nil {
		return false
	}
	// has global index, should return pid
	if is.Index.Global {
		return true
	}
	// has embedded limit, should return physical table id
	if len(is.ByItems) != 0 && is.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
		return true
	}
	return false
}

// SplitSelCondsWithVirtualColumn filter the select conditions which contain virtual column
func SplitSelCondsWithVirtualColumn(conds []expression.Expression) (withoutVirt []expression.Expression, withVirt []expression.Expression) {
	for i := range conds {
		if expression.ContainVirtualColumn(conds[i : i+1]) {
			withVirt = append(withVirt, conds[i])
		} else {
			withoutVirt = append(withoutVirt, conds[i])
		}
	}
	return withoutVirt, withVirt
}

func matchIndicesProp(sctx base.PlanContext, idxCols []*expression.Column, colLens []int, propItems []property.SortItem) bool {
	if len(idxCols) < len(propItems) {
		return false
	}
	for i, item := range propItems {
		if colLens[i] != types.UnspecifiedLength || !item.Col.EqualByExprAndID(sctx.GetExprCtx().GetEvalCtx(), idxCols[i]) {
			return false
		}
	}
	return true
}

func (ds *DataSource) splitIndexFilterConditions(conditions []expression.Expression, indexColumns []*expression.Column,
	idxColLens []int) (indexConds, tableConds []expression.Expression) {
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		var covered bool
		if ds.SCtx().GetSessionVars().OptPrefixIndexSingleScan {
			covered = ds.isIndexCoveringCondition(cond, indexColumns, idxColLens)
		} else {
			covered = ds.isIndexCoveringColumns(expression.ExtractColumns(cond), indexColumns, idxColLens)
		}
		if covered {
			indexConditions = append(indexConditions, cond)
		} else {
			tableConditions = append(tableConditions, cond)
		}
	}
	return indexConditions, tableConditions
}

// GetPhysicalScan returns PhysicalTableScan for the LogicalTableScan.
func (s *LogicalTableScan) GetPhysicalScan(schema *expression.Schema, stats *property.StatsInfo) *PhysicalTableScan {
	ds := s.Source
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         ds.Columns,
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.partitionDefIdx != nil,
		physicalTableID: ds.physicalTableID,
		Ranges:          s.Ranges,
		AccessCondition: s.AccessConds,
		tblCols:         ds.TblCols,
		tblColHists:     ds.TblColHists,
	}.Init(s.SCtx(), s.QueryBlockOffset())
	ts.SetStats(stats)
	ts.SetSchema(schema.Clone())
	return ts
}

// GetPhysicalIndexScan returns PhysicalIndexScan for the logical IndexScan.
func (s *LogicalIndexScan) GetPhysicalIndexScan(_ *expression.Schema, stats *property.StatsInfo) *PhysicalIndexScan {
	ds := s.Source
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          s.Columns,
		Index:            s.Index,
		IdxCols:          s.IdxCols,
		IdxColLens:       s.IdxColLens,
		AccessCondition:  s.AccessConds,
		Ranges:           s.Ranges,
		dataSourceSchema: ds.schema,
		isPartition:      ds.partitionDefIdx != nil,
		physicalTableID:  ds.physicalTableID,
		tblColHists:      ds.TblColHists,
		pkIsHandleCol:    ds.getPKIsHandleCol(),
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	is.SetStats(stats)
	is.initSchema(s.FullIdxCols, s.IsDoubleRead)
	return is
}

// isPointGetPath indicates whether the conditions are point-get-able.
// eg: create table t(a int, b int,c int unique, primary (a,b))
// select * from t where a = 1 and b = 1 and c =1;
// the datasource can access by primary key(a,b) or unique key c which are both point-get-able
func (ds *DataSource) isPointGetPath(path *util.AccessPath) bool {
	if len(path.Ranges) < 1 {
		return false
	}
	if !path.IsIntHandlePath {
		if path.Index == nil {
			return false
		}
		if !path.Index.Unique || path.Index.HasPrefixIndex() {
			return false
		}
		idxColsLen := len(path.Index.Columns)
		for _, ran := range path.Ranges {
			if len(ran.LowVal) != idxColsLen {
				return false
			}
		}
	}
	tc := ds.SCtx().GetSessionVars().StmtCtx.TypeCtx()
	for _, ran := range path.Ranges {
		if !ran.IsPointNonNullable(tc) {
			return false
		}
	}
	return true
}

// convertToTableScan converts the DataSource to table scan.
func (ds *DataSource) convertToTableScan(prop *property.PhysicalProperty, candidate *candidatePath, _ *optimizetrace.PhysicalOptimizeOp) (base.Task, error) {
	// It will be handled in convertToIndexScan.
	if prop.TaskTp == property.CopMultiReadTaskType {
		return base.InvalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return base.InvalidTask, nil
	}
	// If we need to keep order for the index scan, we should forbid the non-keep-order index scan when we try to generate the path.
	if prop.IsSortItemEmpty() && candidate.path.ForceKeepOrder {
		return base.InvalidTask, nil
	}
	// If we don't need to keep order for the index scan, we should forbid the non-keep-order index scan when we try to generate the path.
	if !prop.IsSortItemEmpty() && candidate.path.ForceNoKeepOrder {
		return base.InvalidTask, nil
	}
	ts, _ := ds.getOriginalPhysicalTableScan(prop, candidate.path, candidate.isMatchProp)
	if ts.KeepOrder && ts.StoreType == kv.TiFlash && (ts.Desc || ds.SCtx().GetSessionVars().TiFlashFastScan) {
		// TiFlash fast mode(https://github.com/pingcap/tidb/pull/35851) does not keep order in TableScan
		return base.InvalidTask, nil
	}
	if ts.StoreType == kv.TiFlash {
		for _, col := range ts.Columns {
			if col.IsVirtualGenerated() {
				col.AddFlag(mysql.GeneratedColumnFlag)
			}
		}
	}
	// In disaggregated tiflash mode, only MPP is allowed, cop and batchCop is deprecated.
	// So if prop.TaskTp is RootTaskType, have to use mppTask then convert to rootTask.
	isTiFlashPath := ts.StoreType == kv.TiFlash
	canMppConvertToRoot := prop.TaskTp == property.RootTaskType && ds.SCtx().GetSessionVars().IsMPPAllowed() && isTiFlashPath
	canMppConvertToRootForDisaggregatedTiFlash := config.GetGlobalConfig().DisaggregatedTiFlash && canMppConvertToRoot
	canMppConvertToRootForWhenTiFlashCopIsBanned := ds.SCtx().GetSessionVars().IsTiFlashCopBanned() && canMppConvertToRoot
	if prop.TaskTp == property.MppTaskType || canMppConvertToRootForDisaggregatedTiFlash || canMppConvertToRootForWhenTiFlashCopIsBanned {
		if ts.KeepOrder {
			return base.InvalidTask, nil
		}
		if prop.MPPPartitionTp != property.AnyType {
			return base.InvalidTask, nil
		}
		// ********************************** future deprecated start **************************/
		var hasVirtualColumn bool
		for _, col := range ts.schema.Columns {
			if col.VirtualExpr != nil {
				ds.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because column `" + col.OrigName + "` is a virtual column which is not supported now.")
				hasVirtualColumn = true
				break
			}
		}
		// in general, since MPP has supported the Gather operator to fill the virtual column, we should full lift restrictions here.
		// we left them here, because cases like:
		// parent-----+
		//            V  (when parent require a root task type here, we need convert mpp task to root task)
		//    projection [mpp task] [a]
		//      table-scan [mpp task] [a(virtual col as: b+1), b]
		// in the process of converting mpp task to root task, the encapsulated table reader will use its first children schema [a]
		// as its schema, so when we resolve indices later, the virtual column 'a' itself couldn't resolve itself anymore.
		//
		if hasVirtualColumn && !canMppConvertToRootForDisaggregatedTiFlash && !canMppConvertToRootForWhenTiFlashCopIsBanned {
			return base.InvalidTask, nil
		}
		// ********************************** future deprecated end **************************/
		mppTask := &MppTask{
			p:           ts,
			partTp:      property.AnyType,
			tblColHists: ds.TblColHists,
		}
		ts.PlanPartInfo = PhysPlanPartInfo{
			PruningConds:   pushDownNot(ds.SCtx().GetExprCtx(), ds.allConds),
			PartitionNames: ds.partitionNames,
			Columns:        ds.TblCols,
			ColumnNames:    ds.names,
		}
		mppTask = ts.addPushedDownSelectionToMppTask(mppTask, ds.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt))
		var task base.Task = mppTask
		if !mppTask.Invalid() {
			if prop.TaskTp == property.MppTaskType && len(mppTask.rootTaskConds) > 0 {
				// If got filters cannot be pushed down to tiflash, we have to make sure it will be executed in TiDB,
				// So have to return a rootTask, but prop requires mppTask, cannot meet this requirement.
				task = base.InvalidTask
			} else if prop.TaskTp == property.RootTaskType {
				// When got here, canMppConvertToRootX is true.
				// This is for situations like cannot generate mppTask for some operators.
				// Such as when the build side of HashJoin is Projection,
				// which cannot pushdown to tiflash(because TiFlash doesn't support some expr in Proj)
				// So HashJoin cannot pushdown to tiflash. But we still want TableScan to run on tiflash.
				task = mppTask
				task = task.ConvertToRootTask(ds.SCtx())
			}
		}
		return task, nil
	}
	if isTiFlashPath && config.GetGlobalConfig().DisaggregatedTiFlash || isTiFlashPath && ds.SCtx().GetSessionVars().IsTiFlashCopBanned() {
		// prop.TaskTp is cop related, just return base.InvalidTask.
		return base.InvalidTask, nil
	}
	copTask := &CopTask{
		tablePlan:         ts,
		indexPlanFinished: true,
		tblColHists:       ds.TblColHists,
	}
	copTask.physPlanPartInfo = PhysPlanPartInfo{
		PruningConds:   pushDownNot(ds.SCtx().GetExprCtx(), ds.allConds),
		PartitionNames: ds.partitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.names,
	}
	ts.PlanPartInfo = copTask.physPlanPartInfo
	var task base.Task = copTask
	if candidate.isMatchProp {
		copTask.keepOrder = true
		if ds.tableInfo.GetPartitionInfo() != nil {
			// TableScan on partition table on TiFlash can't keep order.
			if ts.StoreType == kv.TiFlash {
				return base.InvalidTask, nil
			}
			// Add sort items for table scan for merge-sort operation between partitions.
			byItems := make([]*util.ByItems, 0, len(prop.SortItems))
			for _, si := range prop.SortItems {
				byItems = append(byItems, &util.ByItems{
					Expr: si.Col,
					Desc: si.Desc,
				})
			}
			ts.ByItems = byItems
		}
	}
	ts.addPushedDownSelection(copTask, ds.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt))
	if prop.IsFlashProp() && len(copTask.rootTaskConds) != 0 {
		return base.InvalidTask, nil
	}
	if prop.TaskTp == property.RootTaskType {
		task = task.ConvertToRootTask(ds.SCtx())
	} else if _, ok := task.(*RootTask); ok {
		return base.InvalidTask, nil
	}
	return task, nil
}

func (ds *DataSource) convertToSampleTable(prop *property.PhysicalProperty,
	candidate *candidatePath, _ *optimizetrace.PhysicalOptimizeOp) (base.Task, error) {
	if prop.TaskTp == property.CopMultiReadTaskType {
		return base.InvalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return base.InvalidTask, nil
	}
	if candidate.isMatchProp {
		// Disable keep order property for sample table path.
		return base.InvalidTask, nil
	}
	p := PhysicalTableSample{
		TableSampleInfo: ds.SampleInfo,
		TableInfo:       ds.table,
		PhysicalTableID: ds.physicalTableID,
		Desc:            candidate.isMatchProp && prop.SortItems[0].Desc,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	p.schema = ds.schema
	rt := &RootTask{}
	rt.SetPlan(p)
	return rt, nil
}

func (ds *DataSource) convertToPointGet(prop *property.PhysicalProperty, candidate *candidatePath) base.Task {
	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return base.InvalidTask
	}
	if prop.TaskTp == property.CopMultiReadTaskType && candidate.path.IsSingleScan ||
		prop.TaskTp == property.CopSingleReadTaskType && !candidate.path.IsSingleScan {
		return base.InvalidTask
	}

	if tidbutil.IsMemDB(ds.DBName.L) {
		return base.InvalidTask
	}

	accessCnt := math.Min(candidate.path.CountAfterAccess, float64(1))
	pointGetPlan := PointGetPlan{
		ctx:              ds.SCtx(),
		AccessConditions: candidate.path.AccessConds,
		schema:           ds.schema.Clone(),
		dbName:           ds.DBName.L,
		TblInfo:          ds.TableInfo(),
		outputNames:      ds.OutputNames(),
		LockWaitTime:     ds.SCtx().GetSessionVars().LockWaitTimeout,
		Columns:          ds.Columns,
	}.Init(ds.SCtx(), ds.tableStats.ScaleByExpectCnt(accessCnt), ds.QueryBlockOffset())
	if ds.partitionDefIdx != nil {
		pointGetPlan.PartitionIdx = ds.partitionDefIdx
	}
	pointGetPlan.PartitionNames = ds.partitionNames
	rTsk := &RootTask{}
	rTsk.SetPlan(pointGetPlan)
	if candidate.path.IsIntHandlePath {
		pointGetPlan.Handle = kv.IntHandle(candidate.path.Ranges[0].LowVal[0].GetInt64())
		pointGetPlan.UnsignedHandle = mysql.HasUnsignedFlag(ds.handleCols.GetCol(0).RetType.GetFlag())
		pointGetPlan.accessCols = ds.TblCols
		found := false
		for i := range ds.Columns {
			if ds.Columns[i].ID == ds.handleCols.GetCol(0).ID {
				pointGetPlan.HandleColOffset = ds.Columns[i].Offset
				found = true
				break
			}
		}
		if !found {
			return base.InvalidTask
		}
		// Add filter condition to table plan now.
		if len(candidate.path.TableFilters) > 0 {
			sel := PhysicalSelection{
				Conditions: candidate.path.TableFilters,
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(pointGetPlan)
			rTsk.SetPlan(sel)
		}
	} else {
		pointGetPlan.IndexInfo = candidate.path.Index
		pointGetPlan.IdxCols = candidate.path.IdxCols
		pointGetPlan.IdxColLens = candidate.path.IdxColLens
		pointGetPlan.IndexValues = candidate.path.Ranges[0].LowVal
		if candidate.path.IsSingleScan {
			pointGetPlan.accessCols = candidate.path.IdxCols
		} else {
			pointGetPlan.accessCols = ds.TblCols
		}
		// Add index condition to table plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.TableFilters) > 0 {
			sel := PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.TableFilters...),
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(pointGetPlan)
			rTsk.SetPlan(sel)
		}
	}

	return rTsk
}

func (ds *DataSource) convertToBatchPointGet(prop *property.PhysicalProperty, candidate *candidatePath) base.Task {
	if !prop.IsSortItemEmpty() && !candidate.isMatchProp {
		return base.InvalidTask
	}
	if prop.TaskTp == property.CopMultiReadTaskType && candidate.path.IsSingleScan ||
		prop.TaskTp == property.CopSingleReadTaskType && !candidate.path.IsSingleScan {
		return base.InvalidTask
	}

	accessCnt := math.Min(candidate.path.CountAfterAccess, float64(len(candidate.path.Ranges)))
	batchPointGetPlan := &BatchPointGetPlan{
		ctx:              ds.SCtx(),
		dbName:           ds.DBName.L,
		AccessConditions: candidate.path.AccessConds,
		TblInfo:          ds.TableInfo(),
		KeepOrder:        !prop.IsSortItemEmpty(),
		Columns:          ds.Columns,
		PartitionNames:   ds.partitionNames,
	}
	if ds.partitionDefIdx != nil {
		batchPointGetPlan.SinglePartition = true
		batchPointGetPlan.PartitionIdxs = []int{*ds.partitionDefIdx}
	}
	if batchPointGetPlan.KeepOrder {
		batchPointGetPlan.Desc = prop.SortItems[0].Desc
	}
	rTsk := &RootTask{}
	if candidate.path.IsIntHandlePath {
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.Handles = append(batchPointGetPlan.Handles, kv.IntHandle(ran.LowVal[0].GetInt64()))
		}
		batchPointGetPlan.accessCols = ds.TblCols
		batchPointGetPlan.HandleColOffset = ds.handleCols.GetCol(0).Index
		// Add filter condition to table plan now.
		if len(candidate.path.TableFilters) > 0 {
			batchPointGetPlan.Init(ds.SCtx(), ds.tableStats.ScaleByExpectCnt(accessCnt), ds.schema.Clone(), ds.names, ds.QueryBlockOffset())
			sel := PhysicalSelection{
				Conditions: candidate.path.TableFilters,
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(batchPointGetPlan)
			rTsk.SetPlan(sel)
		}
	} else {
		batchPointGetPlan.IndexInfo = candidate.path.Index
		batchPointGetPlan.IdxCols = candidate.path.IdxCols
		batchPointGetPlan.IdxColLens = candidate.path.IdxColLens
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.IndexValues = append(batchPointGetPlan.IndexValues, ran.LowVal)
		}
		if !prop.IsSortItemEmpty() {
			batchPointGetPlan.KeepOrder = true
			batchPointGetPlan.Desc = prop.SortItems[0].Desc
		}
		if candidate.path.IsSingleScan {
			batchPointGetPlan.accessCols = candidate.path.IdxCols
		} else {
			batchPointGetPlan.accessCols = ds.TblCols
		}
		// Add index condition to table plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.TableFilters) > 0 {
			batchPointGetPlan.Init(ds.SCtx(), ds.tableStats.ScaleByExpectCnt(accessCnt), ds.schema.Clone(), ds.names, ds.QueryBlockOffset())
			sel := PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.TableFilters...),
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(batchPointGetPlan)
			rTsk.SetPlan(sel)
		}
	}
	if rTsk.GetPlan() == nil {
		tmpP := batchPointGetPlan.Init(ds.SCtx(), ds.tableStats.ScaleByExpectCnt(accessCnt), ds.schema.Clone(), ds.names, ds.QueryBlockOffset())
		rTsk.SetPlan(tmpP)
	}

	return rTsk
}

func (ts *PhysicalTableScan) addPushedDownSelectionToMppTask(mpp *MppTask, stats *property.StatsInfo) *MppTask {
	filterCondition, rootTaskConds := SplitSelCondsWithVirtualColumn(ts.filterCondition)
	var newRootConds []expression.Expression
	filterCondition, newRootConds = expression.PushDownExprs(GetPushDownCtx(ts.SCtx()), filterCondition, ts.StoreType)
	mpp.rootTaskConds = append(rootTaskConds, newRootConds...)

	ts.filterCondition = filterCondition
	// Add filter condition to table plan now.
	if len(ts.filterCondition) > 0 {
		sel := PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.SCtx(), stats, ts.QueryBlockOffset())
		sel.SetChildren(ts)
		mpp.p = sel
	}
	return mpp
}

func (ts *PhysicalTableScan) addPushedDownSelection(copTask *CopTask, stats *property.StatsInfo) {
	ts.filterCondition, copTask.rootTaskConds = SplitSelCondsWithVirtualColumn(ts.filterCondition)
	var newRootConds []expression.Expression
	ts.filterCondition, newRootConds = expression.PushDownExprs(GetPushDownCtx(ts.SCtx()), ts.filterCondition, ts.StoreType)
	copTask.rootTaskConds = append(copTask.rootTaskConds, newRootConds...)

	// Add filter condition to table plan now.
	if len(ts.filterCondition) > 0 {
		sel := PhysicalSelection{Conditions: ts.filterCondition}.Init(ts.SCtx(), stats, ts.QueryBlockOffset())
		if len(copTask.rootTaskConds) != 0 {
			selectivity, _, err := cardinality.Selectivity(ts.SCtx(), copTask.tblColHists, ts.filterCondition, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = cost.SelectionFactor
			}
			sel.SetStats(ts.StatsInfo().Scale(selectivity))
		}
		sel.SetChildren(ts)
		copTask.tablePlan = sel
	}
}

func (ts *PhysicalTableScan) getScanRowSize() float64 {
	if ts.StoreType == kv.TiKV {
		return cardinality.GetTableAvgRowSize(ts.SCtx(), ts.tblColHists, ts.tblCols, ts.StoreType, true)
	}
	// If `ts.handleCol` is nil, then the schema of tableScan doesn't have handle column.
	// This logic can be ensured in column pruning.
	return cardinality.GetTableAvgRowSize(ts.SCtx(), ts.tblColHists, ts.Schema().Columns, ts.StoreType, ts.HandleCols != nil)
}

func (ds *DataSource) getOriginalPhysicalTableScan(prop *property.PhysicalProperty, path *util.AccessPath, isMatchProp bool) (*PhysicalTableScan, float64) {
	ts := PhysicalTableScan{
		Table:           ds.tableInfo,
		Columns:         slices.Clone(ds.Columns),
		TableAsName:     ds.TableAsName,
		DBName:          ds.DBName,
		isPartition:     ds.partitionDefIdx != nil,
		physicalTableID: ds.physicalTableID,
		Ranges:          path.Ranges,
		AccessCondition: path.AccessConds,
		StoreType:       path.StoreType,
		HandleCols:      ds.handleCols,
		tblCols:         ds.TblCols,
		tblColHists:     ds.TblColHists,
		constColsByCond: path.ConstCols,
		prop:            prop,
		filterCondition: slices.Clone(path.TableFilters),
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	ts.SetSchema(ds.schema.Clone())
	rowCount := path.CountAfterAccess
	if prop.ExpectedCnt < ds.StatsInfo().RowCount {
		rowCount = cardinality.AdjustRowCountForTableScanByLimit(ds.SCtx(),
			ds.StatsInfo(), ds.tableStats, ds.statisticTable,
			path, prop.ExpectedCnt, isMatchProp && prop.SortItems[0].Desc)
	}
	// We need NDV of columns since it may be used in cost estimation of join. Precisely speaking,
	// we should track NDV of each histogram bucket, and sum up the NDV of buckets we actually need
	// to scan, but this would only help improve accuracy of NDV for one column, for other columns,
	// we still need to assume values are uniformly distributed. For simplicity, we use uniform-assumption
	// for all columns now, as we do in `deriveStatsByFilter`.
	ts.SetStats(ds.tableStats.ScaleByExpectCnt(rowCount))
	usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(ts.physicalTableID) != nil {
		ts.usedStatsInfo = usedStats.GetUsedInfo(ts.physicalTableID)
	}
	if isMatchProp {
		ts.Desc = prop.SortItems[0].Desc
		ts.KeepOrder = true
	}
	return ts, rowCount
}

func (ds *DataSource) getOriginalPhysicalIndexScan(prop *property.PhysicalProperty, path *util.AccessPath, isMatchProp bool, isSingleScan bool) *PhysicalIndexScan {
	idx := path.Index
	is := PhysicalIndexScan{
		Table:            ds.tableInfo,
		TableAsName:      ds.TableAsName,
		DBName:           ds.DBName,
		Columns:          util.CloneColInfos(ds.Columns),
		Index:            idx,
		IdxCols:          path.IdxCols,
		IdxColLens:       path.IdxColLens,
		AccessCondition:  path.AccessConds,
		Ranges:           path.Ranges,
		dataSourceSchema: ds.schema,
		isPartition:      ds.partitionDefIdx != nil,
		physicalTableID:  ds.physicalTableID,
		tblColHists:      ds.TblColHists,
		pkIsHandleCol:    ds.getPKIsHandleCol(),
		constColsByCond:  path.ConstCols,
		prop:             prop,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	rowCount := path.CountAfterAccess
	is.initSchema(append(path.FullIdxCols, ds.commonHandleCols...), !isSingleScan)

	// If (1) there exists an index whose selectivity is smaller than the threshold,
	// and (2) there is Selection on the IndexScan, we don't use the ExpectedCnt to
	// adjust the estimated row count of the IndexScan.
	ignoreExpectedCnt := ds.accessPathMinSelectivity < ds.SCtx().GetSessionVars().OptOrderingIdxSelThresh &&
		len(path.IndexFilters)+len(path.TableFilters) > 0

	if (isMatchProp || prop.IsSortItemEmpty()) && prop.ExpectedCnt < ds.StatsInfo().RowCount && !ignoreExpectedCnt {
		rowCount = cardinality.AdjustRowCountForIndexScanByLimit(ds.SCtx(),
			ds.StatsInfo(), ds.tableStats, ds.statisticTable,
			path, prop.ExpectedCnt, isMatchProp && prop.SortItems[0].Desc)
	}
	// ScaleByExpectCnt only allows to scale the row count smaller than the table total row count.
	// But for MV index, it's possible that the IndexRangeScan row count is larger than the table total row count.
	// Please see the Case 2 in CalcTotalSelectivityForMVIdxPath for an example.
	if idx.MVIndex && rowCount > ds.tableStats.RowCount {
		is.SetStats(ds.tableStats.Scale(rowCount / ds.tableStats.RowCount))
	} else {
		is.SetStats(ds.tableStats.ScaleByExpectCnt(rowCount))
	}
	usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
	if usedStats != nil && usedStats.GetUsedInfo(is.physicalTableID) != nil {
		is.usedStatsInfo = usedStats.GetUsedInfo(is.physicalTableID)
	}
	if isMatchProp {
		is.Desc = prop.SortItems[0].Desc
		is.KeepOrder = true
	}
	return is
}

// FindBestTask implements the LogicalPlan interface.
func (p *LogicalCTE) FindBestTask(prop *property.PhysicalProperty, counter *base.PlanCounterTp, pop *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error) {
	if p.ChildLen() > 0 {
		return p.BaseLogicalPlan.FindBestTask(prop, counter, pop)
	}
	if !prop.IsSortItemEmpty() && !prop.CanAddEnforcer {
		return base.InvalidTask, 1, nil
	}
	// The physical plan has been build when derive stats.
	pcte := PhysicalCTE{SeedPlan: p.cte.seedPartPhysicalPlan, RecurPlan: p.cte.recursivePartPhysicalPlan, CTE: p.cte, cteAsName: p.cteAsName, cteName: p.cteName}.Init(p.SCtx(), p.StatsInfo())
	pcte.SetSchema(p.schema)
	if prop.IsFlashProp() && prop.CTEProducerStatus == property.AllCTECanMpp {
		pcte.readerReceiver = PhysicalExchangeReceiver{IsCTEReader: true}.Init(p.SCtx(), p.StatsInfo())
		if prop.MPPPartitionTp != property.AnyType {
			return base.InvalidTask, 1, nil
		}
		t = &MppTask{
			p:           pcte,
			partTp:      prop.MPPPartitionTp,
			hashCols:    prop.MPPPartitionCols,
			tblColHists: p.StatsInfo().HistColl,
		}
	} else {
		rt := &RootTask{}
		rt.SetPlan(pcte)
		rt.SetEmpty(false)
		t = rt
	}
	if prop.CanAddEnforcer {
		t = enforceProperty(prop, t, p.Plan.SCtx())
	}
	return t, 1, nil
}

// FindBestTask implements the LogicalPlan interface.
func (p *LogicalCTETable) FindBestTask(prop *property.PhysicalProperty, _ *base.PlanCounterTp, _ *optimizetrace.PhysicalOptimizeOp) (t base.Task, cntPlan int64, err error) {
	if !prop.IsSortItemEmpty() {
		return nil, 1, nil
	}

	pcteTable := PhysicalCTETable{IDForStorage: p.idForStorage}.Init(p.SCtx(), p.StatsInfo())
	pcteTable.SetSchema(p.schema)
	rt := &RootTask{}
	rt.SetPlan(pcteTable)
	t = rt
	return t, 1, nil
}

func appendCandidate(lp base.LogicalPlan, task base.Task, prop *property.PhysicalProperty, opt *optimizetrace.PhysicalOptimizeOp) {
	if task == nil || task.Invalid() {
		return
	}
	utilfuncp.AppendCandidate4PhysicalOptimizeOp(opt, lp, task.Plan(), prop)
}

// PushDownNot here can convert condition 'not (a != 1)' to 'a = 1'. When we build range from conds, the condition like
// 'not (a != 1)' would not be handled so we need to convert it to 'a = 1', which can be handled when building range.
func pushDownNot(ctx expression.BuildContext, conds []expression.Expression) []expression.Expression {
	for i, cond := range conds {
		conds[i] = expression.PushDownNot(ctx, cond)
	}
	return conds
}

func validateTableSamplePlan(ds *DataSource, t base.Task, err error) error {
	if err != nil {
		return err
	}
	if ds.SampleInfo != nil && !t.Invalid() {
		if _, ok := t.Plan().(*PhysicalTableSample); !ok {
			return expression.ErrInvalidTableSample.GenWithStackByArgs("plan not supported")
		}
	}
	return nil
}
