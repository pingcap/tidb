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
	"fmt"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/cascades/impl"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/util/chunk"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

func prepareIterationDownElems(super base.LogicalPlan) (*memo.GroupExpression, *logicalop.BaseLogicalPlan, int, iterFunc, base.LogicalPlan) {
	// get the possible group expression and logical operator from common lp pointer.
	ge, self := getGEAndSelf(super)
	childLen := len(self.Children())
	iterObj := self.GetBaseLogicalPlan()
	iterFunc := iteratePhysicalPlan4BaseLogical
	if _, ok := self.(*logicalop.LogicalSequence); ok {
		iterFunc = iterateChildPlan4LogicalSequence
	}
	if ge != nil {
		iterObj = ge
		childLen = len(ge.Inputs)
		iterFunc = iteratePhysicalPlan4GroupExpression
		if _, ok := self.(*logicalop.LogicalSequence); ok {
			iterFunc = iterateChildPlan4LogicalSequenceGE
		}
	}
	return ge, self.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan), childLen, iterFunc, iterObj
}

type enumerateState struct {
	topNCopExist  bool
	limitCopExist bool
}

// The reason the physical plan is a slice of slices is to allow preferring a specific type of plan within a slice,
// while still performing cost comparison between slices.
// There are three types of task in the following code: hintTask, normalPreferTask, and normalIterTask.
// The purpose of hintTask is to always have the highest priority, both **within and across slices**,
// meaning it will always be chosen if available, regardless of cost comparison.
// For non-hint tasks, i.e., normalIterTask and normalPreferTask, normalPreferTask has the highest priority **within** a slice.
// normalIterTask has the lowest priority and always depends on cost comparison to choose the least expensive task.
func enumeratePhysicalPlans4Task(
	super base.LogicalPlan,
	physicalPlansSlice [][]base.PhysicalPlan,
	prop *property.PhysicalProperty,
	addEnforcer bool,
) (base.Task, bool, error) {
	if len(physicalPlansSlice) == 0 {
		return base.InvalidTask, false, nil
	}

	var normalTask, hintTask = base.InvalidTask, base.InvalidTask
	for _, ops := range physicalPlansSlice {
		if len(ops) == 0 {
			continue
		}
		curTask, curHintCanWork, curErr := enumeratePhysicalPlans4TaskHelper(super, ops, prop, addEnforcer)
		if curErr != nil {
			return nil, false, curErr
		}

		if curHintCanWork {
			if hintTask.Invalid() {
				hintTask = curTask
			} else if curIsBetter, err := compareTaskCost(curTask, hintTask); err != nil {
				return nil, false, err
			} else if curIsBetter {
				hintTask = curTask
			}
		} else {
			if normalTask.Invalid() {
				normalTask = curTask
			} else if curIsBetter, err := compareTaskCost(curTask, normalTask); err != nil {
				return nil, false, err
			} else if curIsBetter {
				normalTask = curTask
			}
		}
	}
	if !hintTask.Invalid() {
		return hintTask, true, nil
	}
	return normalTask, false, nil
}

func enumeratePhysicalPlans4TaskHelper(
	super base.LogicalPlan,
	physicalPlans []base.PhysicalPlan,
	prop *property.PhysicalProperty,
	addEnforcer bool,
) (base.Task, bool, error) {
	var err error
	var normalIterTask, normalPreferTask, hintTask = base.InvalidTask, base.InvalidTask, base.InvalidTask
	initState := &enumerateState{}
	_, baseLP, childLen, iteration, iterObj := prepareIterationDownElems(super)
	childTasks := make([]base.Task, 0, childLen)

	var fd *funcdep.FDSet
	if addEnforcer && len(physicalPlans) != 0 {
		switch logicalPlan := baseLP.Self().(type) {
		case *logicalop.LogicalJoin, *logicalop.LogicalAggregation:
			// TODO(hawkingrei): FD should be maintained as logical prop instead of constructing it in physical phase
			fd = logicalPlan.ExtractFD()
		}
	}

	for _, pp := range physicalPlans {
		childTasks, err = iteration(iterObj, pp, childTasks, prop)
		if err != nil {
			return nil, false, err
		}

		// This check makes sure that there is no invalid child task.
		if len(childTasks) != childLen {
			continue
		}

		// Combine the best child tasks with parent physical plan.
		curTask := pp.Attach2Task(childTasks...)
		if curTask.Invalid() {
			continue
		}

		// An optimal task could not satisfy the property, so it should be converted here.
		if _, ok := curTask.(*physicalop.RootTask); !ok && prop.TaskTp == property.RootTaskType {
			curTask = curTask.ConvertToRootTask(baseLP.SCtx())
		}

		// we need to check the hint is applicable before enforcing the property. otherwise
		// what we get is Sort ot Exchanger kind of operators.
		// todo: extend applyLogicalJoinHint to be a normal logicalOperator's interface to handle the hint related stuff.
		hintApplicable := applyLogicalHintVarEigen(baseLP.Self(), pp, childTasks)

		// Enforce curTask property
		if addEnforcer {
			curTask = physicalop.EnforceProperty(prop, curTask, baseLP.Plan.SCtx(), fd)
		}

		// Optimize by shuffle executor to running in parallel manner.
		if _, isMpp := curTask.(*physicalop.MppTask); !isMpp && prop.IsSortItemEmpty() {
			// Currently, we do not regard shuffled plan as a new plan.
			curTask = optimizeByShuffle(curTask, baseLP.Plan.SCtx())
		}

		if hintApplicable {
			if hintTask.Invalid() {
				hintTask = curTask
			} else if curIsBetter, err := compareTaskCost(curTask, hintTask); err != nil {
				return nil, false, err
			} else if curIsBetter {
				hintTask = curTask
			}
		}

		if hintTask.Invalid() && hasNormalPreferTask(baseLP.Self(), initState, pp, childTasks) {
			if normalPreferTask.Invalid() {
				normalPreferTask = curTask
			} else if curIsBetter, err := compareTaskCost(curTask, normalPreferTask); err != nil {
				return nil, false, err
			} else if curIsBetter {
				normalPreferTask = curTask
			}
		}

		if hintTask.Invalid() && normalPreferTask.Invalid() {
			if normalIterTask.Invalid() {
				normalIterTask = curTask
			} else if curIsBetter, err := compareTaskCost(curTask, normalIterTask); err != nil {
				return nil, false, err
			} else if curIsBetter {
				normalIterTask = curTask
			}
		}
	}

	returnedTask := base.InvalidTask
	var hintCanWork bool
	if !hintTask.Invalid() {
		returnedTask = hintTask
		hintCanWork = true
	} else if !normalPreferTask.Invalid() {
		returnedTask = normalPreferTask
	} else if !normalIterTask.Invalid() {
		returnedTask = normalIterTask
	}

	if !hintCanWork && returnedTask != nil && !returnedTask.Invalid() {
		// It means there is no hint or hint is not applicable.
		// So record hint warning if necessary.
		if warn := recordWarnings(baseLP.Self(), prop, addEnforcer); warn != nil {
			returnedTask.AppendWarning(warn)
		}
	}
	return returnedTask, hintCanWork, nil
}

// TODO: remove the taskTypeSatisfied function, it is only used to check the task type in the root, cop, mpp task.
func taskTypeSatisfied(propRequired *property.PhysicalProperty, childTask base.Task) bool {
	// check the root, cop, mpp task type matched the required property.
	if childTask == nil || propRequired == nil {
		// index join v1 may occur that propRequired is nil, and return task is nil too. Return true
		// to make sure let it walk through the following logic.
		return true
	}
	_, isRoot := childTask.(*physicalop.RootTask)
	_, isCop := childTask.(*physicalop.CopTask)
	_, isMpp := childTask.(*physicalop.MppTask)
	switch propRequired.TaskTp {
	case property.RootTaskType:
		// If the required property is root task type, root, cop, and mpp task are all satisfied.
		return isRoot || isCop || isMpp
	case property.CopSingleReadTaskType, property.CopMultiReadTaskType:
		return isCop
	case property.MppTaskType:
		return isMpp
	default:
		// shouldn't be here
		return false
	}
}

// iteratePhysicalPlan4GroupExpression is used to pin current picked physical plan and try to physicalize all its children.
func iteratePhysicalPlan4GroupExpression(
	geLP base.LogicalPlan,
	selfPhysicalPlan base.PhysicalPlan,
	childTasks []base.Task,
	_ *property.PhysicalProperty,
) ([]base.Task, error) {
	ge := geLP.(*memo.GroupExpression)
	// Find the best child tasks firstly.
	childTasks = childTasks[:0]
	for j, childG := range ge.Inputs {
		childProp := selfPhysicalPlan.GetChildReqProps(j)
		childTask, err := impl.ImplementGroupAndCost(childG, childProp, math.MaxFloat64)
		if err != nil {
			return nil, err
		}
		if !taskTypeSatisfied(childProp, childTask) {
			// If the task type is not satisfied, we should skip this plan.
			return nil, nil
		}
		if childTask != nil && childTask.Invalid() {
			return nil, nil
		}
		childTasks = append(childTasks, childTask)
	}

	// This check makes sure that there is no invalid child task.
	if len(childTasks) != len(ge.Inputs) {
		return nil, nil
	}
	return childTasks, nil
}

type iterFunc func(
	baseLP base.LogicalPlan,
	selfPhysicalPlan base.PhysicalPlan,
	childTasks []base.Task,
	_ *property.PhysicalProperty,
) ([]base.Task, error)

// iteratePhysicalPlan4BaseLogical is used to iterate the physical plan and get all child tasks.
func iteratePhysicalPlan4BaseLogical(
	baseLP base.LogicalPlan,
	selfPhysicalPlan base.PhysicalPlan,
	childTasks []base.Task,
	_ *property.PhysicalProperty,
) ([]base.Task, error) {
	p := baseLP.(*logicalop.BaseLogicalPlan)
	// Find best child tasks firstly.
	childTasks = childTasks[:0]
	for j, child := range p.Children() {
		childProp := selfPhysicalPlan.GetChildReqProps(j)
		childTask, err := physicalop.FindBestTask(child, childProp)
		if err != nil {
			return nil, err
		}
		if !taskTypeSatisfied(childProp, childTask) {
			// If the task type is not satisfied, we should skip this plan.
			return nil, nil
		}
		if childTask != nil && childTask.Invalid() {
			return nil, nil
		}
		childTasks = append(childTasks, childTask)
	}

	// This check makes sure that there is no invalid child task.
	if len(childTasks) != p.ChildLen() {
		return nil, nil
	}
	return childTasks, nil
}

// iterateChildPlan4LogicalSequenceGE does the special part for sequence. We need to iterate its child one by one to check whether the former child is a valid plan and then go to the nex
func iterateChildPlan4LogicalSequenceGE(
	geLP base.LogicalPlan,
	selfPhysicalPlan base.PhysicalPlan,
	childTasks []base.Task,
	prop *property.PhysicalProperty,
) ([]base.Task, error) {
	ge := geLP.(*memo.GroupExpression)
	// Find best child tasks firstly.
	childTasks = childTasks[:0]
	lastIdx := len(ge.Inputs) - 1
	for j := range lastIdx {
		childG := ge.Inputs[j]
		childProp := selfPhysicalPlan.GetChildReqProps(j)
		childTask, err := impl.ImplementGroupAndCost(childG, childProp, math.MaxFloat64)
		if err != nil {
			return nil, err
		}
		if !taskTypeSatisfied(childProp, childTask) {
			// If the task type is not satisfied, we should skip this plan.
			return nil, nil
		}
		if childTask != nil && childTask.Invalid() {
			return nil, nil
		}
		_, isMpp := childTask.(*physicalop.MppTask)
		if !isMpp && prop.IsFlashProp() {
			break
		}
		childTasks = append(childTasks, childTask)
	}
	// This check makes sure that there is no invalid child task.
	if len(childTasks) != len(ge.Inputs)-1 {
		return nil, nil
	}

	lastChildProp := selfPhysicalPlan.GetChildReqProps(lastIdx).CloneEssentialFields()
	if lastChildProp.IsFlashProp() {
		lastChildProp.CTEProducerStatus = property.AllCTECanMpp
	}
	lastChildG := ge.Inputs[lastIdx]
	lastChildTask, err := impl.ImplementGroupAndCost(lastChildG, lastChildProp, math.MaxFloat64)
	if err != nil {
		return nil, err
	}
	if lastChildTask != nil && lastChildTask.Invalid() {
		return nil, nil
	}

	if _, ok := lastChildTask.(*physicalop.MppTask); !ok && lastChildProp.CTEProducerStatus == property.AllCTECanMpp {
		return nil, nil
	}

	childTasks = append(childTasks, lastChildTask)
	return childTasks, nil
}

// iterateChildPlan4LogicalSequence does the special part for sequence. We need to iterate its child one by one to check whether the former child is a valid plan and then go to the nex
func iterateChildPlan4LogicalSequence(
	baseLP base.LogicalPlan,
	selfPhysicalPlan base.PhysicalPlan,
	childTasks []base.Task,
	prop *property.PhysicalProperty,
) ([]base.Task, error) {
	p := baseLP.(*logicalop.BaseLogicalPlan)
	// Find best child tasks firstly.
	childTasks = childTasks[:0]
	lastIdx := p.ChildLen() - 1
	for j := range lastIdx {
		child := p.Children()[j]
		childProp := selfPhysicalPlan.GetChildReqProps(j)
		childTask, err := physicalop.FindBestTask(child, childProp)
		if err != nil {
			return nil, err
		}
		if !taskTypeSatisfied(childProp, childTask) {
			// If the task type is not satisfied, we should skip this plan.
			return nil, nil
		}
		if childTask != nil && childTask.Invalid() {
			return nil, nil
		}
		_, isMpp := childTask.(*physicalop.MppTask)
		if !isMpp && prop.IsFlashProp() {
			break
		}
		childTasks = append(childTasks, childTask)
	}
	// This check makes sure that there is no invalid child task.
	if len(childTasks) != p.ChildLen()-1 {
		return nil, nil
	}

	lastChildProp := selfPhysicalPlan.GetChildReqProps(lastIdx).CloneEssentialFields()
	if lastChildProp.IsFlashProp() {
		lastChildProp.CTEProducerStatus = property.AllCTECanMpp
	}
	lastChildTask, err := physicalop.FindBestTask(p.Children()[lastIdx], lastChildProp)
	if err != nil {
		return nil, err
	}
	if lastChildTask != nil && lastChildTask.Invalid() {
		return nil, nil
	}

	if _, ok := lastChildTask.(*physicalop.MppTask); !ok && lastChildProp.CTEProducerStatus == property.AllCTECanMpp {
		return nil, nil
	}

	childTasks = append(childTasks, lastChildTask)
	return childTasks, nil
}

// compareTaskCost compares cost of curTask and bestTask and returns whether curTask's cost is smaller than bestTask's.
func compareTaskCost(curTask, bestTask base.Task) (curIsBetter bool, err error) {
	curCost, curInvalid, err := getTaskPlanCost(curTask)
	if err != nil {
		return false, err
	}
	bestCost, bestInvalid, err := getTaskPlanCost(bestTask)
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
// The second returned value indicates whether this task is valid.
func getTaskPlanCost(t base.Task) (float64, bool, error) {
	if t.Invalid() {
		return math.MaxFloat64, true, nil
	}

	// use the new cost interface
	var (
		taskType         property.TaskType
		indexPartialCost float64
	)
	switch t.(type) {
	case *physicalop.RootTask:
		taskType = property.RootTaskType
	case *physicalop.CopTask: // no need to know whether the task is single-read or double-read, so both CopSingleReadTaskType and CopDoubleReadTaskType are OK
		cop := t.(*physicalop.CopTask)
		if cop.IndexPlan != nil && cop.TablePlan != nil { // handle IndexLookup specially
			taskType = property.CopMultiReadTaskType
			// keep compatible with the old cost interface, for CopMultiReadTask, the cost is idxCost + tblCost.
			if !cop.IndexPlanFinished { // only consider index cost in this case
				idxCost, err := getPlanCost(cop.IndexPlan, taskType, costusage.NewDefaultPlanCostOption())
				return idxCost, false, err
			}
			// consider both sides
			idxCost, err := getPlanCost(cop.IndexPlan, taskType, costusage.NewDefaultPlanCostOption())
			if err != nil {
				return 0, false, err
			}
			tblCost, err := getPlanCost(cop.TablePlan, taskType, costusage.NewDefaultPlanCostOption())
			if err != nil {
				return 0, false, err
			}
			return idxCost + tblCost, false, nil
		}

		taskType = property.CopSingleReadTaskType

		// TiFlash can run cop task as well, check whether this cop task will run on TiKV or TiFlash.
		if cop.TablePlan != nil {
			leafNode := cop.TablePlan
			for len(leafNode.Children()) > 0 {
				leafNode = leafNode.Children()[0]
			}
			if tblScan, isScan := leafNode.(*physicalop.PhysicalTableScan); isScan && tblScan.StoreType == kv.TiFlash {
				taskType = property.MppTaskType
			}
		}

		// Detail reason ref about comment in function `convertToIndexMergeScan`
		// for cop task with {indexPlan=nil, tablePlan=xxx, idxMergePartPlans=[x,x,x], indexPlanFinished=true} we should
		// plus the partial index plan cost into the final cost. Because t.plan() the below code used only calculate the
		// cost about table plan.
		if cop.IndexPlanFinished && len(cop.IdxMergePartPlans) != 0 {
			for _, partialScan := range cop.IdxMergePartPlans {
				partialCost, err := getPlanCost(partialScan, taskType, costusage.NewDefaultPlanCostOption())
				if err != nil {
					return 0, false, err
				}
				indexPartialCost += partialCost
			}
		}
	case *physicalop.MppTask:
		taskType = property.MppTaskType
	default:
		return 0, false, errors.New("unknown task type")
	}
	if t.Plan() == nil {
		// It's a very special case for index merge case.
		// t.plan() == nil in index merge COP case, it means indexPlanFinished is false in other words.
		cost := 0.0
		copTsk := t.(*physicalop.CopTask)
		for _, partialScan := range copTsk.IdxMergePartPlans {
			partialCost, err := getPlanCost(partialScan, taskType, costusage.NewDefaultPlanCostOption())
			if err != nil {
				return 0, false, err
			}
			cost += partialCost
		}
		return cost, false, nil
	}
	cost, err := getPlanCost(t.Plan(), taskType, costusage.NewDefaultPlanCostOption())
	return cost + indexPartialCost, false, err
}

// get the possible group expression and logical operator from common super pointer.
func getGEAndSelf(super base.LogicalPlan) (ge *memo.GroupExpression, self base.LogicalPlan) {
	switch x := super.(type) {
	case *logicalop.BaseLogicalPlan:
		// previously, wrapped BaseLogicalPlan serve as the common part, so we need to use self()
		// to downcast as the every specific logical operator.
		self = x.Self()
	case *memo.GroupExpression:
		// currently, since GroupExpression wrap a LogicalPlan as its first field, we GE itself is
		// naturally can be referred as a LogicalPlan, and we need to use GetWrappedLogicalPlan to
		// get the specific logical operator inside.
		ge = x
		self = ge.GetWrappedLogicalPlan()
	default:
		// x itself must be a specific logical operator.
		self = x
	}
	return ge, self
}

// findBestTask is key workflow that drive logic plan tree to generate optimal physical ones.
// The logic inside it is mainly about physical plan numeration and task encapsulation, it should
// be defined in core pkg, and be called by logic plan in their logic interface implementation.
func findBestTask(super base.LogicalPlan, prop *property.PhysicalProperty) (bestTask base.Task, err error) {
	// get the possible group expression and logical operator from common lp pointer.
	ge, self := getGEAndSelf(super)
	p := self.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan)
	// If p is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		return nil, nil
	}
	// Look up the task with this prop in the task map.
	// It's used to reduce double counting.
	// only get the physical cache from logicalOp under volcano mode.
	if ge == nil {
		bestTask = p.GetTask(prop)
		if bestTask != nil {
			return bestTask, nil
		}
	}
	// if prop is require an index join's probe side, check the inner pattern admission here.
	if prop.IndexJoinProp != nil {
		pass := admitIndexJoinInnerChildPattern(self, prop.IndexJoinProp)
		if !pass {
			// even enforce hint can not work with this.
			return base.InvalidTask, nil
		}
	}
	if !checkOpSelfSatisfyPropTaskTypeRequirement(p.Self(), prop) {
		// Currently all plan cannot totally push down to TiKV.
		p.StoreTask(prop, base.InvalidTask)
		return base.InvalidTask, nil
	}

	canAddEnforcer := prop.CanAddEnforcer

	if prop.TaskTp != property.RootTaskType && !prop.IsFlashProp() {
		// Currently all plan cannot totally push down to TiKV.
		// only save the physical to logicalOp under volcano mode.
		if ge == nil {
			p.StoreTask(prop, base.InvalidTask)
		}
		return base.InvalidTask, nil
	}

	// prop should be read only because its cached hashcode might be not consistent
	// when it is changed. So we clone a new one for the temporary changes.
	newProp := prop.CloneEssentialFields()
	// here newProp is used as another complete copy for enforcer, fill indexJoinProp manually.
	// for childProp := prop.CloneEssentialFields(), we do not clone indexJoinProp childProp for by default.
	// and only call admitIndexJoinProp to inherit the indexJoinProp for special pattern operators.
	newProp.IndexJoinProp = prop.IndexJoinProp
	var plansFitsProp, plansNeedEnforce [][]base.PhysicalPlan
	var hintWorksWithProp bool
	// Maybe the plan can satisfy the required property,
	// so we try to get the task without the enforced sort first.
	exhaustObj := self
	if ge != nil {
		exhaustObj = ge
	}
	// make sure call ExhaustPhysicalPlans over GE or Self, rather than the BaseLogicalPlan.
	plansFitsProp, hintWorksWithProp, err = exhaustPhysicalPlans(exhaustObj, newProp)
	if err != nil {
		return nil, err
	}
	if !hintWorksWithProp && !newProp.IsSortItemEmpty() && newProp.IndexJoinProp == nil {
		// If there is a hint in the plan and the hint cannot satisfy the property,
		// we enforce this property and try to generate the PhysicalPlan again to
		// make sure the hint can work.
		// Since index join hint can only be known as worked or not after physic implementation,
		// once indexJoinProp is not nil, it means it can not be enforced.
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
		plansNeedEnforce, hintCanWork, err = exhaustPhysicalPlans(self, newProp)
		if err != nil {
			return nil, err
		}
		// since index join hint can only be known as worked or not after physic implementation.
		// it means hintCanWork and hintWorksWithProp is not determined here. if we try to eliminate
		// some physical alternatives here like clean plansFitsProp or plansNeedEnforce we need to
		// guarantee that no index join is conclude in the both.
		//
		// so we need to check the plansFitsProp or plansNeedEnforce both to find the low-cost and
		// hint applicable plan.
		if !enumerationContainIndexJoin(plansFitsProp) && !enumerationContainIndexJoin(plansNeedEnforce) {
			if hintCanWork && !hintWorksWithProp {
				// If the hint can work with the empty property, but cannot work with
				// the required property, we give up `plansFitProp` to make sure the hint
				// can work.
				plansFitsProp = nil
			}
			if !hintCanWork && !hintWorksWithProp && !prop.CanAddEnforcer {
				// If the original property is not enforced and hint cannot
				// work anyway, we give up `plansNeedEnforce` for efficiency.
				//
				// for special case, once we empty the sort item here, the more possible index join can be enumerated, which
				// may lead the hint work only after child is built up under index join build mode v2. so here we tried
				plansNeedEnforce = nil
			}
		}
		newProp = prop
	}
	var prefer bool
	var curTask base.Task
	if bestTask, prefer, err = enumeratePhysicalPlans4Task(super, plansFitsProp, newProp, false); err != nil {
		return nil, err
	}
	if bestTask != nil && !bestTask.Invalid() && prefer {
		goto END
	}

	curTask, prefer, err = enumeratePhysicalPlans4Task(super, plansNeedEnforce, newProp, true)
	if err != nil {
		return nil, err
	}
	// preferred valid one should have it priority.
	if curTask != nil && !curTask.Invalid() && prefer {
		bestTask = curTask
		goto END
	}
	if curIsBetter, err := compareTaskCost(curTask, bestTask); err != nil {
		return nil, err
	} else if curIsBetter {
		bestTask = curTask
	}

END:
	// only save the physical to logicalOp under volcano mode.
	if ge == nil {
		p.StoreTask(prop, bestTask)
	}
	return bestTask, nil
}

// tryToGetDualTask will check if the push down predicate has false constant. If so, it will return table dual.
func tryToGetDualTask(ds *logicalop.DataSource) (base.Task, error) {
	for _, cond := range ds.PushedDownConds {
		if con, ok := cond.(*expression.Constant); ok && con.DeferredExpr == nil && con.ParamMarker == nil {
			result, _, err := expression.EvalBool(ds.SCtx().GetExprCtx().GetEvalCtx(), []expression.Expression{cond}, chunk.Row{})
			if err != nil {
				return nil, err
			}
			if !result {
				dual := physicalop.PhysicalTableDual{}.Init(ds.SCtx(), ds.StatsInfo(), ds.QueryBlockOffset())
				dual.SetSchema(ds.Schema())
				rt := &physicalop.RootTask{}
				rt.SetPlan(dual)
				return rt, nil
			}
		}
	}
	return nil, nil
}
func isPointGetConvertableSchema(ds *logicalop.DataSource) bool {
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
func exploreEnforcedPlan(ds *logicalop.DataSource) bool {
	// default value is true which is different than original implementation.
	return fixcontrol.GetBoolWithDefault(ds.SCtx().GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix46177, true)
}

func findBestTask4LogicalDataSource(super base.LogicalPlan, prop *property.PhysicalProperty) (t base.Task, err error) {
	// get the possible group expression and logical operator from common lp pointer.
	ge, ds := base.GetGEAndLogicalOp[*logicalop.DataSource](super)
	// If ds is an inner plan in an IndexJoin, the IndexJoin will generate an inner plan by itself,
	// and set inner child prop nil, so here we do nothing.
	if prop == nil {
		return nil, nil
	}
	sessionVars := ds.SCtx().GetSessionVars()
	_, isolationReadEnginesHasTiKV := sessionVars.GetIsolationReadEngines()[kv.TiKV]
	if ds.IsForUpdateRead && sessionVars.TxnCtx.IsExplicit && isolationReadEnginesHasTiKV {
		hasPointGetPath := false
		for _, path := range ds.PossibleAccessPaths {
			if isPointGetPath(ds, path) {
				hasPointGetPath = true
				break
			}
		}
		tblName := ds.TableInfo.Name
		ds.PossibleAccessPaths, err = util.FilterPathByIsolationRead(ds.SCtx(), ds.PossibleAccessPaths, tblName, ds.DBName)
		if err != nil {
			return nil, err
		}
		if hasPointGetPath {
			newPaths := make([]*util.AccessPath, 0)
			for _, path := range ds.PossibleAccessPaths {
				// if the path is the point get range path with for update lock, we should forbid tiflash as it's store path (#39543)
				if path.StoreType != kv.TiFlash {
					newPaths = append(newPaths, path)
				}
			}
			ds.PossibleAccessPaths = newPaths
		}
	}
	// only get the physical cache from logicalOp under volcano mode.
	if ge == nil {
		t = ds.GetTask(prop)
		if t != nil {
			return
		}
	}
	// if prop is require an index join's probe side, check the inner pattern admission here.
	if prop.IndexJoinProp != nil {
		pass := admitIndexJoinInnerChildPattern(ds, prop.IndexJoinProp)
		if !pass {
			// even enforce hint can not work with this.
			return base.InvalidTask, nil
		}
		// cache the physical for indexJoinProp
		defer func() {
			ds.StoreTask(prop, t)
		}()
		// when datasource leaf is in index join's inner side, build the task out with old
		// index join build logic, we can't merge this with normal datasource's index range
		// because normal index range is built on expression EQ/IN. while index join's inner
		// has its special runtime constants detecting and filling logic.
		return getBestIndexJoinInnerTaskByProp(ds, prop)
	}
	var unenforcedTask base.Task
	// If prop.CanAddEnforcer is true, the prop.SortItems need to be set nil for ds.findBestTask.
	// Before function return, reset it for enforcing task prop and storing map<prop,task>.
	oldProp := prop.CloneEssentialFields()
	if prop.CanAddEnforcer {
		// First, get the bestTask without enforced prop
		prop.CanAddEnforcer = false
		unenforcedTask, err = physicalop.FindBestTask(ds, prop)
		if err != nil {
			return nil, err
		}
		if !unenforcedTask.Invalid() && !exploreEnforcedPlan(ds) {
			// only save the physical to logicalOp under volcano mode.
			if ge == nil {
				ds.StoreTask(prop, unenforcedTask)
			}
			return unenforcedTask, nil
		}

		// Then, explore the bestTask with enforced prop
		prop.CanAddEnforcer = true
		prop.SortItems = []property.SortItem{}
		prop.MPPPartitionTp = property.AnyType
	} else if prop.MPPPartitionTp != property.AnyType {
		return base.InvalidTask, nil
	}
	defer func() {
		if err != nil {
			return
		}
		if prop.CanAddEnforcer {
			*prop = *oldProp
			t = physicalop.EnforceProperty(prop, t, ds.Plan.SCtx(), nil)
			prop.CanAddEnforcer = true
		}

		if unenforcedTask != nil && !unenforcedTask.Invalid() {
			curIsBest, cerr := compareTaskCost(unenforcedTask, t)
			if cerr != nil {
				err = cerr
				return
			}
			if curIsBest {
				t = unenforcedTask
			}
		}
		// only save the physical to logicalOp under volcano mode.
		if ge == nil {
			ds.StoreTask(prop, t)
		}
		err = validateTableSamplePlan(ds, t, err)
	}()

	t, err = tryToGetDualTask(ds)
	if err != nil || t != nil {
		return t, err
	}

	t = base.InvalidTask
	candidates := skylinePruning(ds, prop)
	pruningInfo := getPruningInfo(ds, candidates, prop)
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

	for _, candidate := range candidates {
		path := candidate.path
		if path.PartialIndexPaths != nil {
			// prefer tiflash, while current table path is tikv, skip it.
			if ds.PreferStoreType&h.PreferTiFlash != 0 && path.StoreType == kv.TiKV {
				continue
			}
			// prefer tikv, while current table path is tiflash, skip it.
			if ds.PreferStoreType&h.PreferTiKV != 0 && path.StoreType == kv.TiFlash {
				continue
			}
			idxMergeTask, err := convertToIndexMergeScan(ds, prop, candidate)
			if err != nil {
				return nil, err
			}
			curIsBetter, err := compareTaskCost(idxMergeTask, t)
			if err != nil {
				return nil, err
			}
			if curIsBetter {
				t = idxMergeTask
			}
			continue
		}
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.Ranges) == 0 {
			// We should uncache the tableDual plan.
			if expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), path.AccessConds...) {
				ds.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("get a TableDual plan")
			}
			dual := physicalop.PhysicalTableDual{}.Init(ds.SCtx(), ds.StatsInfo(), ds.QueryBlockOffset())
			dual.SetSchema(ds.Schema())
			t := &physicalop.RootTask{}
			t.SetPlan(dual)
			return t, nil
		}

		canConvertPointGet := len(path.Ranges) > 0 && path.StoreType == kv.TiKV && isPointGetConvertableSchema(ds)
		if fixcontrol.GetBoolWithDefault(ds.SCtx().GetSessionVars().OptimizerFixControl, fixcontrol.Fix52592, false) {
			canConvertPointGet = false
		}

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
		if canConvertPointGet && ds.Table.Meta().GetPartitionInfo() != nil {
			// partition table with dynamic prune not support batchPointGet
			// Due to sorting?
			// Please make sure handle `where _tidb_rowid in (xx, xx)` correctly when delete this if statements.
			if canConvertPointGet && len(path.Ranges) > 1 && ds.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
				canConvertPointGet = false
			}
			if canConvertPointGet && len(path.Ranges) > 1 {
				// if LIST COLUMNS/RANGE COLUMNS partitioned, and any of the partitioning columns are string based
				// and having non-binary collations, then we currently cannot support BatchPointGet,
				// since the candidate.path.Ranges already have been converted to SortKey, meaning we cannot use it
				// for PartitionLookup/PartitionPruning currently.

				// TODO: This is now implemented, but to decrease
				// the impact of supporting plan cache for patitioning,
				// this is not yet enabled.
				// TODO: just remove this if block and update/add tests...
				// We can only build batch point get for hash partitions on a simple column now. This is
				// decided by the current implementation of `BatchPointGetExec::initialize()`, specifically,
				// the `getPhysID()` function. Once we optimize that part, we can come back and enable
				// BatchPointGet plan for more cases.
				hashPartColName := getHashOrKeyPartitionColumnName(ds.SCtx(), ds.Table.Meta())
				if hashPartColName == nil {
					canConvertPointGet = false
				}
			}
			// Partition table can't use `_tidb_rowid` to generate PointGet Plan unless one partition is explicitly specified.
			if canConvertPointGet && path.IsIntHandlePath && !ds.Table.Meta().PKIsHandle && len(ds.PartitionNames) != 1 {
				canConvertPointGet = false
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
					pointGetTask = convertToPointGet(ds, prop, candidate)
				} else {
					pointGetTask = convertToBatchPointGet(ds, prop, candidate)
				}

				// Batch/PointGet plans may be over-optimized, like `a>=1(?) and a<=1(?)` --> `a=1` --> PointGet(a=1).
				// For safety, prevent these plans from the plan cache here.
				if !pointGetTask.Invalid() && expression.MaybeOverOptimized4PlanCache(ds.SCtx().GetExprCtx(), candidate.path.AccessConds...) && !isSafePointGetPath4PlanCache(ds.SCtx(), candidate.path) {
					ds.SCtx().GetSessionVars().StmtCtx.SetSkipPlanCache("Batch/PointGet plans may be over-optimized")
				}

				curIsBetter, cerr := compareTaskCost(pointGetTask, t)
				if cerr != nil {
					return nil, cerr
				}
				if curIsBetter {
					t = pointGetTask
					continue
				}
			}
		}
		if path.IsTablePath() {
			// prefer tiflash, while current table path is tikv, skip it.
			if ds.PreferStoreType&h.PreferTiFlash != 0 && path.StoreType == kv.TiKV {
				continue
			}
			// prefer tikv, while current table path is tiflash, skip it.
			if ds.PreferStoreType&h.PreferTiKV != 0 && path.StoreType == kv.TiFlash {
				continue
			}
			var tblTask base.Task
			if ds.SampleInfo != nil {
				tblTask, err = convertToSampleTable(ds, prop, candidate)
			} else {
				tblTask, err = convertToTableScan(ds, prop, candidate)
			}
			if err != nil {
				return nil, err
			}
			curIsBetter, err := compareTaskCost(tblTask, t)
			if err != nil {
				return nil, err
			}
			if curIsBetter {
				t = tblTask
			}
			continue
		}
		// TiFlash storage do not support index scan.
		if ds.PreferStoreType&h.PreferTiFlash != 0 {
			continue
		}
		// TableSample do not support index scan.
		if ds.SampleInfo != nil {
			continue
		}
		idxTask, err := convertToIndexScan(ds, prop, candidate)
		if err != nil {
			return nil, err
		}
		curIsBetter, err := compareTaskCost(idxTask, t)
		if err != nil {
			return nil, err
		}
		if curIsBetter {
			t = idxTask
		}
	}

	return
}

func (p mockLogicalPlan4Test) Init(ctx base.PlanContext) *mockLogicalPlan4Test {
	p.BaseLogicalPlan = logicalop.NewBaseLogicalPlan(ctx, "mockPlan", &p, 0)
	return &p
}

func (p *mockLogicalPlan4Test) getPhysicalPlan1(prop *property.PhysicalProperty) base.PhysicalPlan {
	physicalPlan1 := mockPhysicalPlan4Test{planType: 1}.Init(p.SCtx())
	physicalPlan1.SetStats(&property.StatsInfo{RowCount: 1})
	physicalPlan1.SetChildrenReqProps(make([]*property.PhysicalProperty, 1))
	physicalPlan1.SetXthChildReqProps(0, prop.CloneEssentialFields())
	return physicalPlan1
}

func (p *mockLogicalPlan4Test) getPhysicalPlan2(prop *property.PhysicalProperty) base.PhysicalPlan {
	physicalPlan2 := mockPhysicalPlan4Test{planType: 2}.Init(p.SCtx())
	physicalPlan2.SetStats(&property.StatsInfo{RowCount: 1})
	physicalPlan2.SetChildrenReqProps(make([]*property.PhysicalProperty, 1))
	physicalPlan2.SetXthChildReqProps(0, property.NewPhysicalProperty(prop.TaskTp, nil, false, prop.ExpectedCnt, false))
	return physicalPlan2
}

// ExhaustPhysicalPlans4MockLogicalPlan iterate physical implementation over mock logic plan.
func ExhaustPhysicalPlans4MockLogicalPlan(p *mockLogicalPlan4Test, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	plan1 := make([]base.PhysicalPlan, 0, 1)
	plan2 := make([]base.PhysicalPlan, 0, 1)
	if prop.IsSortItemEmpty() && p.canGeneratePlan2 {
		// Generate PhysicalPlan2 when the property is empty.
		plan2 = append(plan2, p.getPhysicalPlan2(prop))
		if p.hasHintForPlan2 {
			return plan2, true, nil
		}
	}
	if all, _ := prop.AllSameOrder(); all {
		// Generate PhysicalPlan1 when properties are the same order.
		plan1 = append(plan1, p.getPhysicalPlan1(prop))
	}
	if p.hasHintForPlan2 {
		// The hint cannot work.
		if prop.IsSortItemEmpty() {
			p.SCtx().GetSessionVars().StmtCtx.AppendWarning(fmt.Errorf("the hint is inapplicable for plan2"))
		}
		return plan1, false, nil
	}
	return append(plan1, plan2...), true, nil
}

type mockPhysicalPlan4Test struct {
	physicalop.BasePhysicalPlan
	// 1 or 2 for physicalPlan1 or physicalPlan2.
	// See the comment of mockLogicalPlan4Test.
	planType int
}

func (p mockPhysicalPlan4Test) Init(ctx base.PlanContext) *mockPhysicalPlan4Test {
	p.BasePhysicalPlan = physicalop.NewBasePhysicalPlan(ctx, "mockPlan", &p, 0)
	return &p
}

// Attach2Task implements the PhysicalPlan interface.
func (p *mockPhysicalPlan4Test) Attach2Task(tasks ...base.Task) base.Task {
	t := tasks[0].Copy()
	attachPlan2Task(p, t)
	return t
}

// MemoryUsage of mockPhysicalPlan4Test is only for testing
func (*mockPhysicalPlan4Test) MemoryUsage() (sum int64) {
	return
}
