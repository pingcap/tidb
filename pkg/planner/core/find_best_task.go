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
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/cascades/impl"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/ranger"
	sliceutil "github.com/pingcap/tidb/pkg/util/slice"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
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
		pass := admitIndexJoinInnerChildPattern(self)
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

// candidatePath is used to maintain required info for skyline pruning.
type candidatePath struct {
	path              *util.AccessPath
	accessCondsColMap util.Col2Len // accessCondsColMap maps Column.UniqueID to column length for the columns in AccessConds.
	indexCondsColMap  util.Col2Len // indexCondsColMap maps Column.UniqueID to column length for the columns in AccessConds and indexFilters.
	matchPropResult   property.PhysicalPropMatchResult

	indexJoinCols int // how many index columns are used in access conditions in this IndexJoin.
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

func compareGlobalIndex(lhs, rhs *candidatePath) int {
	if lhs.path.IsTablePath() || rhs.path.IsTablePath() ||
		len(lhs.path.PartialIndexPaths) != 0 || len(rhs.path.PartialIndexPaths) != 0 {
		return 0
	}
	return compareBool(lhs.path.Index.Global, rhs.path.Index.Global)
}

func compareRiskRatio(lhs, rhs *candidatePath) (int, float64) {
	lhsRiskRatio, rhsRiskRatio := 0.0, 0.0
	// MaxCountAfterAccess tracks the worst case "CountAfterAccess", accounting for scenarios that could
	// increase our row estimation, thus lhs/rhsRiskRatio represents the "risk" of the CountAfterAccess value.
	// Lower value means less risk that the actual row count is higher than the estimated one.
	if lhs.path.MaxCountAfterAccess > lhs.path.CountAfterAccess && lhs.path.CountAfterAccess > 0 {
		lhsRiskRatio = lhs.path.MaxCountAfterAccess / lhs.path.CountAfterAccess
	}
	if rhs.path.MaxCountAfterAccess > rhs.path.CountAfterAccess && rhs.path.CountAfterAccess > 0 {
		rhsRiskRatio = rhs.path.MaxCountAfterAccess / rhs.path.CountAfterAccess
	}
	sumLHS := lhs.path.CountAfterAccess + lhs.path.MaxCountAfterAccess
	sumRHS := rhs.path.CountAfterAccess + rhs.path.MaxCountAfterAccess
	// lhs has lower risk
	if lhsRiskRatio < rhsRiskRatio {
		if lhs.path.CountAfterAccess <= rhs.path.CountAfterAccess {
			return 1, lhsRiskRatio
		}
		if sumLHS < sumRHS && lhs.path.MinCountAfterAccess > 0 &&
			(lhs.path.MinCountAfterAccess <= rhs.path.MinCountAfterAccess || lhs.path.CountAfterIndex <= rhs.path.CountAfterIndex) {
			return 1, lhsRiskRatio
		}
	}
	// rhs has lower risk
	if rhsRiskRatio < lhsRiskRatio {
		if rhs.path.CountAfterAccess <= lhs.path.CountAfterAccess {
			return -1, rhsRiskRatio
		}
		if sumRHS < sumLHS && rhs.path.MinCountAfterAccess > 0 &&
			(rhs.path.MinCountAfterAccess <= lhs.path.MinCountAfterAccess || rhs.path.CountAfterIndex <= lhs.path.CountAfterIndex) {
			return -1, rhsRiskRatio
		}
	}
	return 0, 0
}

// compareCandidates is the core of skyline pruning, which is used to decide which candidate path is better.
// The first return value is 1 if lhs is better, -1 if rhs is better, 0 if they are equivalent or not comparable.
// The 2nd return value indicates whether the "better path" is missing statistics or not.
func compareCandidates(sctx base.PlanContext, statsTbl *statistics.Table, prop *property.PhysicalProperty, lhs, rhs *candidatePath, preferRange bool) (int, bool) {
	// Due to #50125, full scan on MVIndex has been disabled, so MVIndex path might lead to 'can't find a proper plan' error at the end.
	// Avoid MVIndex path to exclude all other paths and leading to 'can't find a proper plan' error, see #49438 for an example.
	if isMVIndexPath(lhs.path) || isMVIndexPath(rhs.path) {
		return 0, false
	}
	// lhsPseudo == lhs has pseudo (no) stats for the table or index for the lhs path.
	// rhsPseudo == rhs has pseudo (no) stats for the table or index for the rhs path.
	//
	// For the return value - if lhs wins (1), we return lhsPseudo. If rhs wins (-1), we return rhsPseudo.
	// If there is no winner (0), we return false.
	//
	// This return value is used later in SkyLinePruning to determine whether we should preference an index scan
	// over a table scan. Allowing indexes without statistics to survive means they can win via heuristics where
	// they otherwise would have lost on cost.
	lhsPseudo, rhsPseudo, tablePseudo := false, false, false
	if statsTbl != nil {
		tablePseudo = statsTbl.HistColl.Pseudo
		lhsPseudo, rhsPseudo = isCandidatesPseudo(lhs, rhs, statsTbl)
	}
	// matchResult: comparison result of whether LHS vs RHS matches the required properties (1=LHS better, -1=RHS better, 0=equal)
	// globalResult: comparison result of global index vs local index preference (1=LHS better, -1=RHS better, 0=equal)
	matchResult, globalResult := compareBool(lhs.matchPropResult.Matched(), rhs.matchPropResult.Matched()), compareGlobalIndex(lhs, rhs)
	// accessResult: comparison result of access condition coverage (1=LHS better, -1=RHS better, 0=equal)
	// comparable1: whether the access conditions are comparable between LHS and RHS
	accessResult, comparable1 := util.CompareCol2Len(lhs.accessCondsColMap, rhs.accessCondsColMap)
	// scanResult: comparison result of index back scan efficiency (1=LHS better, -1=RHS better, 0=equal)
	//             scanResult will always be true for a table scan (because it is a single scan).
	//             This has the effect of allowing the table scan plan to not be pruned.
	// comparable2: whether the index back scan characteristics are comparable between LHS and RHS
	scanResult, comparable2 := compareIndexBack(lhs, rhs)
	// riskResult: comparison result of risk factor (1=LHS better, -1=RHS better, 0=equal)
	riskResult, _ := compareRiskRatio(lhs, rhs)
	// eqOrInResult: comparison result of equal/IN predicate coverage (1=LHS better, -1=RHS better, 0=equal)
	eqOrInResult, lhsEqOrInCount, rhsEqOrInCount := compareEqOrIn(lhs, rhs)

	// predicateResult is separated out. An index may "win" because it has a better
	// accessResult - but that access has high risk.
	// accessResult does not differentiate between range or equal/IN predicates.
	// Summing these 3 metrics ensures that a "high risk" index wont win ONLY on
	// accessResult. The high risk will negate that accessResult with erOrIn being the
	// tiebreaker or equalizer.
	predicateResult := accessResult + riskResult + eqOrInResult

	// totalSum is the aggregate score of all comparison metrics
	totalSum := accessResult + scanResult + matchResult + globalResult

	pseudoResult := 0
	// Determine winner if one index doesn't have statistics and another has statistics
	if (lhsPseudo || rhsPseudo) && !tablePseudo && // At least one index doesn't have statistics
		(lhsEqOrInCount > 0 || rhsEqOrInCount > 0) { // At least one index has equal/IN predicates
		lhsFullMatch := isFullIndexMatch(lhs)
		rhsFullMatch := isFullIndexMatch(rhs)
		pseudoResult = comparePseudo(lhsPseudo, rhsPseudo, lhsFullMatch, rhsFullMatch, eqOrInResult, lhsEqOrInCount, rhsEqOrInCount, preferRange)
		if pseudoResult > 0 && totalSum >= 0 {
			return pseudoResult, lhsPseudo
		}
		if pseudoResult < 0 && totalSum <= 0 {
			return pseudoResult, rhsPseudo
		}
	}

	// This rule is empirical but not always correct.
	// If x's range row count is significantly lower than y's, for example, 1000 times, we think x is better.
	if lhs.path.CountAfterAccess > 100 && rhs.path.CountAfterAccess > 100 && // to prevent some extreme cases, e.g. 0.01 : 10
		len(lhs.path.PartialIndexPaths) == 0 && len(rhs.path.PartialIndexPaths) == 0 && // not IndexMerge since its row count estimation is not accurate enough
		prop.ExpectedCnt == math.MaxFloat64 { // Limit may affect access row count
		threshold := float64(fixcontrol.GetIntWithDefault(sctx.GetSessionVars().OptimizerFixControl, fixcontrol.Fix45132, 1000))
		sctx.GetSessionVars().RecordRelevantOptFix(fixcontrol.Fix45132)
		if threshold > 0 { // set it to 0 to disable this rule
			// corrResult is included to ensure we don't preference to a higher risk plan given that
			// this rule does not check the other criteria included below.
			if lhs.path.CountAfterAccess/rhs.path.CountAfterAccess > threshold && riskResult <= 0 {
				return -1, rhsPseudo // right wins - also return whether it has statistics (pseudo) or not
			}
			if rhs.path.CountAfterAccess/lhs.path.CountAfterAccess > threshold && riskResult >= 0 {
				return 1, lhsPseudo // left wins - also return whether it has statistics (pseudo) or not
			}
		}
	}

	leftDidNotLose := predicateResult >= 0 && scanResult >= 0 && matchResult >= 0 && globalResult >= 0
	rightDidNotLose := predicateResult <= 0 && scanResult <= 0 && matchResult <= 0 && globalResult <= 0
	if !comparable1 || !comparable2 {
		// These aren't comparable - meaning that they have different combinations of columns in
		// the access conditions or filters.
		// One or more predicates could carry high risk - so we want to compare that risk and other
		// metrics to see if we can determine a clear winner.
		// The 2 key metrics here are riskResult and predicateResult.
		// - riskResult tells us which candidate has lower risk
		// - predicateResult already includes risk - we need ">1" or "<-1" to counteract the risk factor.
		// "DidNotLose" and totalSum are also factored in to ensure that the winner is better overall."
		if riskResult > 0 && leftDidNotLose && totalSum >= 0 && predicateResult > 1 {
			return 1, lhsPseudo // left wins - also return whether it has statistics (pseudo) or not
		}
		if riskResult < 0 && rightDidNotLose && totalSum <= 0 && predicateResult < -1 {
			return -1, rhsPseudo // right wins - also return whether it has statistics (pseudo) or not
		}
		return 0, false // No winner (0). Do not return the pseudo result
	}
	// leftDidNotLose, but one of the metrics is a win
	if leftDidNotLose && totalSum > 0 {
		return 1, lhsPseudo // left wins - also return whether it has statistics (pseudo) or not
	}
	// rightDidNotLose, but one of the metrics is a win
	if rightDidNotLose && totalSum < 0 {
		return -1, rhsPseudo // right wins - also return whether it has statistics (pseudo) or not
	}
	return 0, false // No winner (0). Do not return the pseudo result
}

func isCandidatesPseudo(lhs, rhs *candidatePath, statsTbl *statistics.Table) (lhsPseudo, rhsPseudo bool) {
	lhsPseudo, rhsPseudo = statsTbl.HistColl.Pseudo, statsTbl.HistColl.Pseudo
	if len(lhs.path.PartialIndexPaths) == 0 && len(rhs.path.PartialIndexPaths) == 0 {
		if !lhs.path.IsTablePath() && lhs.path.Index != nil {
			if statsTbl.ColAndIdxExistenceMap.HasAnalyzed(lhs.path.Index.ID, true) {
				lhsPseudo = false // We have statistics for the lhs index
			} else {
				lhsPseudo = true
			}
		}
		if !rhs.path.IsTablePath() && rhs.path.Index != nil {
			if statsTbl.ColAndIdxExistenceMap.HasAnalyzed(rhs.path.Index.ID, true) {
				rhsPseudo = false // We have statistics on the rhs index
			} else {
				rhsPseudo = true
			}
		}
	}
	return lhsPseudo, rhsPseudo
}

func comparePseudo(lhsPseudo, rhsPseudo, lhsFullMatch, rhsFullMatch bool, eqOrInResult, lhsEqOrInCount, rhsEqOrInCount int, preferRange bool) int {
	// TO-DO: Consider a separate set of rules for global indexes.
	// If one index has statistics and the other does not, choose the index with statistics if it
	// has the same or higher number of equal/IN predicates.
	if !lhsPseudo && lhsEqOrInCount > 0 && eqOrInResult >= 0 {
		return 1 // left wins
	}
	if !rhsPseudo && rhsEqOrInCount > 0 && eqOrInResult <= 0 {
		return -1 // right wins
	}
	if preferRange {
		// keep an index without statistics if that index has more equal/IN predicates, AND:
		// 1) there are at least 2 equal/INs
		// 2) OR - it's a full index match for all index predicates
		if lhsPseudo && eqOrInResult > 0 &&
			(lhsEqOrInCount > 1 || lhsFullMatch) {
			return 1 // left wins
		}
		if rhsPseudo && eqOrInResult < 0 &&
			(rhsEqOrInCount > 1 || rhsFullMatch) {
			return -1 // right wins
		}
	}
	return 0
}

// Return the index with the higher EqOrInCondCount as winner (1 for lhs, -1 for rhs, 0 for tie),
// and the count for each. For example:
//
//	where a=1 and b=1 and c=1 and d=1
//	lhs == idx(a, b, e) <-- lhsEqOrInCount == 2 (loser)
//	rhs == idx(d, c, b) <-- rhsEqOrInCount == 3 (winner)
func compareEqOrIn(lhs, rhs *candidatePath) (predCompare, lhsEqOrInCount, rhsEqOrInCount int) {
	if len(lhs.path.PartialIndexPaths) > 0 || len(rhs.path.PartialIndexPaths) > 0 {
		// If either path has partial index paths, we cannot reliably compare EqOrIn conditions.
		return 0, 0, 0
	}
	lhsEqOrInCount = lhs.equalPredicateCount()
	rhsEqOrInCount = rhs.equalPredicateCount()
	if lhsEqOrInCount > rhsEqOrInCount {
		return 1, lhsEqOrInCount, rhsEqOrInCount
	}
	if lhsEqOrInCount < rhsEqOrInCount {
		return -1, lhsEqOrInCount, rhsEqOrInCount
	}
	// We didn't find a winner, but return both counts for use by the caller
	return 0, lhsEqOrInCount, rhsEqOrInCount
}

func isFullIndexMatch(candidate *candidatePath) bool {
	// Check if the DNF condition is a full match
	if candidate.path.IsDNFCond && candidate.hasOnlyEqualPredicatesInDNF() {
		return candidate.path.MinAccessCondsForDNFCond >= len(candidate.path.Index.Columns)
	}
	// Check if the index covers all access conditions for non-DNF conditions
	return candidate.path.EqOrInCondCount > 0 && len(candidate.indexCondsColMap) >= len(candidate.path.Index.Columns)
}

func matchProperty(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) property.PhysicalPropMatchResult {
	if ds.Table.Type().IsClusterTable() && !prop.IsSortItemEmpty() {
		// TableScan with cluster table can't keep order.
		return property.PropNotMatched
	}
	if prop.VectorProp.VSInfo != nil && path.Index != nil && path.Index.VectorInfo != nil {
		if ds.TableInfo.Columns[path.Index.Columns[0].Offset].ID != prop.VectorProp.Column.ID {
			return property.PropNotMatched
		}

		if model.IndexableFnNameToDistanceMetric[prop.VectorProp.DistanceFnName.L] != path.Index.VectorInfo.DistanceMetric {
			return property.PropNotMatched
		}
		return property.PropMatched
	}
	if path.IsIntHandlePath {
		pkCol := ds.GetPKIsHandleCol()
		if len(prop.SortItems) != 1 || pkCol == nil {
			return property.PropNotMatched
		}
		item := prop.SortItems[0]
		if !item.Col.EqualColumn(pkCol) ||
			path.StoreType == kv.TiFlash && item.Desc {
			return property.PropNotMatched
		}
		return property.PropMatched
	}
	all, _ := prop.AllSameOrder()
	// When the prop is empty or `all` is false, `matchProperty` is better to be `PropNotMatched` because
	// it needs not to keep order for index scan.
	if prop.IsSortItemEmpty() || !all || len(path.IdxCols) < len(prop.SortItems) {
		return property.PropNotMatched
	}

	// Basically, if `prop.SortItems` is the prefix of `path.IdxCols`, then the property is matched.
	// However, we need to consider the situations when some columns of `path.IdxCols` are evaluated as constant.
	// For example:
	// ```
	// create table t(a int, b int, c int, d int, index idx_a_b_c(a, b, c), index idx_d_c_b_a(d, c, b, a));
	// select * from t where a = 1 order by b, c;
	// select * from t where b = 1 order by a, c;
	// select * from t where d = 1 and b = 2 order by c, a;
	// select * from t where d = 1 and b = 2 order by c, b, a;
	// ```
	// In the first two `SELECT` statements, `idx_a_b_c` matches the sort order.
	// In the last two `SELECT` statements, `idx_d_c_b_a` matches the sort order.
	// Hence, we use `path.ConstCols` to deal with the above situations. This corresponds to Case 2 in the code below.
	//
	// Moreover, we also need to consider the situation when some columns of `path.IdxCols` are a list of constant
	// values. For example, for query `SELECT * FROM t WHERE a IN (1,2,3) ORDER BY b, c`, we can use `idx_a_b_c` to
	// access 3 ranges and use a merge sort to satisfy `ORDER BY b, c`. This corresponds to Case 3 in the code below.
	matchResult := property.PropMatched
	groupByColIdxs := make([]int, 0)
	colIdx := 0
	for _, sortItem := range prop.SortItems {
		found := false
		for ; colIdx < len(path.IdxCols); colIdx++ {
			// Case 1: this sort item is satisfied by the index column, go to match the next sort item.
			if path.IdxColLens[colIdx] == types.UnspecifiedLength && sortItem.Col.EqualColumn(path.IdxCols[colIdx]) {
				found = true
				colIdx++
				break
			}
			// Below: this sort item is not satisfied by the index column

			// Case 2: the accessed range on this index column is a single constant value, so we can skip this
			// column and try to use the next index column to satisfy this sort item.
			if path.ConstCols != nil && colIdx < len(path.ConstCols) && path.ConstCols[colIdx] {
				continue
			}

			// Check if the accessed range on this index column is a single constant value **for each range**.
			allRangesPoint := true
			for _, ran := range path.Ranges {
				if len(ran.LowVal) <= colIdx || len(ran.HighVal) <= colIdx {
					allRangesPoint = false
					break
				}
				cmpResult, err := ran.LowVal[colIdx].Compare(
					ds.SCtx().GetSessionVars().StmtCtx.TypeCtx(),
					&ran.HighVal[colIdx],
					ran.Collators[colIdx],
				)
				if err != nil || cmpResult != 0 {
					allRangesPoint = false
					break
				}
			}
			// Case 3: the accessed range on this index column is a constant value in each range respectively,
			// we can also skip this column and try the next index column. But we need to record this column
			// as a "group by" column, because we need to do a merge sort on these ranges later.
			// So `len(groupByColIdxs) > 0` means we need to do a merge sort to satisfy the required property.
			if allRangesPoint {
				groupByColIdxs = append(groupByColIdxs, colIdx)
				continue
			}
			// Case 4: cannot satisfy this sort item, the path cannot match the required property.
			intest.Assert(!found)
			break
		}
		if !found {
			matchResult = property.PropNotMatched
			break
		}
	}
	if len(groupByColIdxs) > 0 && matchResult == property.PropMatched {
		groups, err := GroupRangesByCols(path.Ranges, groupByColIdxs)
		if err != nil {
			return property.PropNotMatched
		}
		// Multiple ranges can be put into one cop task. But when we group them, they are forced to be split into
		// multiple cop tasks. This can cause large overhead besides the merge sort operation itself.
		// After some performance tests, we set a threshold here to limit the max number of groups to prevent this
		// overhead becomes too large unexpectedly.
		const maxGroupCount = 2048
		if len(groups) > maxGroupCount {
			return property.PropNotMatched
		}
		if len(groups) > 0 {
			path.GroupedRanges = groups
			path.GroupByColIdxs = groupByColIdxs
			return property.PropMatchedNeedMergeSort
		}
	}
	return matchResult
}

// GroupRangesByCols groups the ranges by the values of the columns specified by groupByColIdxs.
func GroupRangesByCols(ranges []*ranger.Range, groupByColIdxs []int) ([][]*ranger.Range, error) {
	groups := make(map[string][]*ranger.Range)
	for _, ran := range ranges {
		var datums []types.Datum
		for _, idx := range groupByColIdxs {
			datums = append(datums, ran.LowVal[idx])
		}
		// We just use it to group the values, so any time zone is ok.
		keyBytes, err := codec.EncodeValue(time.UTC, nil, datums...)
		intest.AssertNoError(err)
		if err != nil {
			return nil, err
		}
		key := string(keyBytes)
		groups[key] = append(groups[key], ran)
	}

	// Convert map to slice
	result := slices.Collect(maps.Values(groups))
	return result, nil
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
func matchPropForIndexMergeAlternatives(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) (*util.AccessPath, property.PhysicalPropMatchResult) {
	// target:
	//	1: index merge case, try to match the every alternative partial path to the order property as long as
	//	possible, and generate that property-matched index merge path out if any.
	//	2: If the prop is empty (means no sort requirement), we will generate a random index partial combination
	//	path from all alternatives in case that no index merge path comes out.

	// Execution part doesn't support the merge operation for intersection case yet.
	if path.IndexMergeIsIntersection {
		return nil, property.PropNotMatched
	}

	noSortItem := prop.IsSortItemEmpty()
	allSame, _ := prop.AllSameOrder()
	if !allSame {
		return nil, property.PropNotMatched
	}
	// step1: match the property from all the index partial alternative paths.
	determinedIndexPartialPaths := make([]*util.AccessPath, 0, len(path.PartialAlternativeIndexPaths))
	usedIndexMap := make(map[int64]struct{}, 1)
	useMVIndex := false
	for _, oneORBranch := range path.PartialAlternativeIndexPaths {
		matchIdxes := make([]int, 0, 1)
		for i, oneAlternative := range oneORBranch {
			// if there is some sort items and this path doesn't match this prop, continue.
			match := true
			for _, oneAccessPath := range oneAlternative {
				// Satisfying the property by a merge sort is not supported for partial paths of index merge.
				if !noSortItem && matchProperty(ds, oneAccessPath, prop) != property.PropMatched {
					match = false
				}
			}
			if !match {
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
			return nil, property.PropNotMatched
		}
		if len(matchIdxes) > 1 {
			// if matchIdxes greater than 1, we should sort this match alternative path by its CountAfterAccess.
			alternatives := oneORBranch
			slices.SortStableFunc(matchIdxes, func(a, b int) int {
				res := cmpAlternatives(ds.SCtx().GetSessionVars())(alternatives[a], alternatives[b])
				if res != 0 {
					return res
				}
				// If CountAfterAccess is same, any path is global index should be the first one.
				var lIsGlobalIndex, rIsGlobalIndex int
				if !alternatives[a][0].IsTablePath() && alternatives[a][0].Index.Global {
					lIsGlobalIndex = 1
				}
				if !alternatives[b][0].IsTablePath() && alternatives[b][0].Index.Global {
					rIsGlobalIndex = 1
				}
				return -cmp.Compare(lIsGlobalIndex, rIsGlobalIndex)
			})
		}
		lowestCountAfterAccessIdx := matchIdxes[0]
		determinedIndexPartialPaths = append(determinedIndexPartialPaths, sliceutil.DeepClone(oneORBranch[lowestCountAfterAccessIdx])...)
		// record the index usage info to avoid choosing a single index for all partial paths
		var indexID int64
		if oneORBranch[lowestCountAfterAccessIdx][0].IsTablePath() {
			indexID = -1
		} else {
			indexID = oneORBranch[lowestCountAfterAccessIdx][0].Index.ID
			// record mv index because it's not affected by the all single index limitation.
			if oneORBranch[lowestCountAfterAccessIdx][0].Index.MVIndex {
				useMVIndex = true
			}
		}
		// record the lowestCountAfterAccessIdx's chosen index.
		usedIndexMap[indexID] = struct{}{}
	}
	// since all the choice is done, check the all single index limitation, skip check for mv index.
	// since ds index merge hints will prune other path ahead, lift the all single index limitation here.
	if len(usedIndexMap) == 1 && !useMVIndex && len(ds.IndexMergeHints) <= 0 {
		// if all partial path are using a same index, meaningless and fail over.
		return nil, property.PropNotMatched
	}
	// step2: gen a new **concrete** index merge path.
	indexMergePath := &util.AccessPath{
		IndexMergeAccessMVIndex:  useMVIndex,
		PartialIndexPaths:        determinedIndexPartialPaths,
		IndexMergeIsIntersection: false,
		// inherit those determined can't pushed-down table filters.
		TableFilters: path.TableFilters,
	}
	// path.ShouldBeKeptCurrentFilter record that whether there are some part of the cnf item couldn't be pushed down to tikv already.
	shouldKeepCurrentFilter := path.KeepIndexMergeORSourceFilter
	for _, p := range determinedIndexPartialPaths {
		if p.KeepIndexMergeORSourceFilter {
			shouldKeepCurrentFilter = true
			break
		}
	}
	if shouldKeepCurrentFilter {
		// add the cnf expression back as table filer.
		indexMergePath.TableFilters = append(indexMergePath.TableFilters, path.IndexMergeORSourceFilter)
	}

	// step3: after the index merge path is determined, compute the countAfterAccess as usual.
	indexMergePath.CountAfterAccess = estimateCountAfterAccessForIndexMergeOR(ds, determinedIndexPartialPaths)
	if noSortItem {
		// since there is no sort property, index merge case is generated by random combination, each alternative with the lower/lowest
		// countAfterAccess, here the returned matchProperty should be PropNotMatched.
		return indexMergePath, property.PropNotMatched
	}
	return indexMergePath, property.PropMatched
}

func isMatchPropForIndexMerge(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) property.PhysicalPropMatchResult {
	// Execution part doesn't support the merge operation for intersection case yet.
	if path.IndexMergeIsIntersection {
		return property.PropNotMatched
	}
	allSame, _ := prop.AllSameOrder()
	if !allSame {
		return property.PropNotMatched
	}
	for _, partialPath := range path.PartialIndexPaths {
		// Satisfying the property by a merge sort is not supported for partial paths of index merge.
		if matchProperty(ds, partialPath, prop) != property.PropMatched {
			return property.PropNotMatched
		}
	}
	return property.PropMatched
}

func getTableCandidate(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.matchPropResult = matchProperty(ds, path, prop)
	candidate.accessCondsColMap = util.ExtractCol2Len(ds.SCtx().GetExprCtx().GetEvalCtx(), path.AccessConds, nil, nil)
	return candidate
}

func getIndexCandidate(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.matchPropResult = matchProperty(ds, path, prop)
	candidate.accessCondsColMap = util.ExtractCol2Len(ds.SCtx().GetExprCtx().GetEvalCtx(), path.AccessConds, path.IdxCols, path.IdxColLens)
	candidate.indexCondsColMap = util.ExtractCol2Len(ds.SCtx().GetExprCtx().GetEvalCtx(), append(path.AccessConds, path.IndexFilters...), path.FullIdxCols, path.FullIdxColLens)
	return candidate
}

func getIndexCandidateForIndexJoin(sctx planctx.PlanContext, path *util.AccessPath, indexJoinCols int) *candidatePath {
	candidate := &candidatePath{path: path, indexJoinCols: indexJoinCols}
	candidate.matchPropResult = property.PropNotMatched
	candidate.accessCondsColMap = util.ExtractCol2Len(sctx.GetExprCtx().GetEvalCtx(), path.AccessConds, path.IdxCols, path.IdxColLens)
	candidate.indexCondsColMap = util.ExtractCol2Len(sctx.GetExprCtx().GetEvalCtx(), append(path.AccessConds, path.IndexFilters...), path.FullIdxCols, path.FullIdxColLens)
	// AccessConds could miss some predicates since the DataSource can't see join predicates.
	// For example, `where t1.a=t2.a and t2.b=1`, `t1=a=t2.a` is not pushed down to t2's accessConds since it's a join
	// predicate. We need to set columns used as join keys to accessCondsColMap/indexCondsColMap manually.
	for i := range indexJoinCols {
		candidate.accessCondsColMap[path.IdxCols[i].UniqueID] = path.IdxColLens[i]
		candidate.indexCondsColMap[path.IdxCols[i].UniqueID] = path.IdxColLens[i]
	}
	return candidate
}

func convergeIndexMergeCandidate(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	// since the all index path alternative paths is collected and undetermined, and we should determine a possible and concrete path for this prop.
	possiblePath, match := matchPropForIndexMergeAlternatives(ds, path, prop)
	if possiblePath == nil {
		return nil
	}
	candidate := &candidatePath{path: possiblePath, matchPropResult: match}
	return candidate
}

func getIndexMergeCandidate(ds *logicalop.DataSource, path *util.AccessPath, prop *property.PhysicalProperty) *candidatePath {
	candidate := &candidatePath{path: path}
	candidate.matchPropResult = isMatchPropForIndexMerge(ds, path, prop)
	return candidate
}

// skylinePruning prunes access paths according to different factors. An access path can be pruned only if
// there exists a path that is not worse than it at all factors and there is at least one better factor.
func skylinePruning(ds *logicalop.DataSource, prop *property.PhysicalProperty) []*candidatePath {
	candidates := make([]*candidatePath, 0, 4)
	idxMissingStats := false
	// tidb_opt_prefer_range_scan is the master switch to control index preferencing
	preferRange := ds.SCtx().GetSessionVars().GetAllowPreferRangeScan()
	for _, path := range ds.PossibleAccessPaths {
		// We should check whether the possible access path is valid first.
		if path.StoreType != kv.TiFlash && prop.IsFlashProp() {
			continue
		}
		if len(path.PartialAlternativeIndexPaths) > 0 {
			// OR normal index merge path, try to determine every index partial path for this property.
			candidate := convergeIndexMergeCandidate(ds, path, prop)
			if candidate != nil {
				candidates = append(candidates, candidate)
			}
			continue
		}
		if path.PartialIndexPaths != nil {
			candidates = append(candidates, getIndexMergeCandidate(ds, path, prop))
			continue
		}
		// if we already know the range of the scan is empty, just return a TableDual
		if len(path.Ranges) == 0 {
			return []*candidatePath{{path: path}}
		}
		var currentCandidate *candidatePath
		if path.IsTablePath() {
			currentCandidate = getTableCandidate(ds, path, prop)
		} else {
			if !(len(path.AccessConds) > 0 || !prop.IsSortItemEmpty() || path.Forced || path.IsSingleScan) {
				continue
			}
			// We will use index to generate physical plan if any of the following conditions is satisfied:
			// 1. This path's access cond is not nil.
			// 2. We have a non-empty prop to match.
			// 3. This index is forced to choose.
			// 4. The needed columns are all covered by index columns(and handleCol).
			currentCandidate = getIndexCandidate(ds, path, prop)
		}
		pruned := false
		for i := len(candidates) - 1; i >= 0; i-- {
			if candidates[i].path.StoreType == kv.TiFlash {
				continue
			}
			result, missingStats := compareCandidates(ds.SCtx(), ds.StatisticTable, prop, candidates[i], currentCandidate, preferRange)
			if missingStats {
				idxMissingStats = true // Ensure that we track idxMissingStats across all iterations
			}
			if result == 1 {
				pruned = true
				// We can break here because the current candidate lost to another plan.
				// This means that we won't add it to the candidates below.
				break
			} else if result == -1 {
				// The current candidate is better - so remove the old one from "candidates"
				candidates = slices.Delete(candidates, i, i+1)
			}
		}
		if !pruned {
			candidates = append(candidates, currentCandidate)
		}
	}

	// If we've forced an index merge - we want to keep these plans
	preferMerge := len(ds.IndexMergeHints) > 0 || fixcontrol.GetBoolWithDefault(
		ds.SCtx().GetSessionVars().GetOptimizerFixControlMap(),
		fixcontrol.Fix52869,
		false,
	)
	if preferRange {
		// Override preferRange with the following limitations to scope
		preferRange = preferMerge || idxMissingStats || ds.TableStats.HistColl.Pseudo || ds.TableStats.RowCount < 1
	}
	if preferRange && len(candidates) > 1 {
		// If a candidate path is TiFlash-path or forced-path or MV index or global index, we just keep them. For other
		// candidate paths, if there exists any range scan path, we remove full scan paths and keep range scan paths.
		preferredPaths := make([]*candidatePath, 0, len(candidates))
		var hasRangeScanPath, hasMultiRange bool
		for _, c := range candidates {
			if len(c.path.Ranges) > 1 {
				hasMultiRange = true
			}
			if c.path.Forced || c.path.StoreType == kv.TiFlash || (c.path.Index != nil && (c.path.Index.Global || c.path.Index.MVIndex)) {
				preferredPaths = append(preferredPaths, c)
				continue
			}
			// Preference plans with equals/IN predicates or where there is more filtering in the index than against the table
			indexFilters := c.equalPredicateCount() > 0 || len(c.path.TableFilters) < len(c.path.IndexFilters)
			if preferMerge || ((c.path.IsSingleScan || indexFilters) && (prop.IsSortItemEmpty() || c.matchPropResult.Matched())) {
				if !c.path.IsFullScanRange(ds.TableInfo) {
					preferredPaths = append(preferredPaths, c)
					hasRangeScanPath = true
				}
			}
		}
		if hasMultiRange {
			// Only log the fix control if we had multiple ranges
			ds.SCtx().GetSessionVars().RecordRelevantOptFix(fixcontrol.Fix52869)
		}
		if hasRangeScanPath {
			return preferredPaths
		}
	}

	return candidates
}

// hasOnlyEqualPredicatesInDNF checks if all access conditions in DNF form contain at least one equal predicate
func (c *candidatePath) hasOnlyEqualPredicatesInDNF() bool {
	// Helper function to check if a condition is an equal/IN predicate or a LogicOr of equal/IN predicates
	var isEqualPredicateOrOr func(expr expression.Expression) bool
	isEqualPredicateOrOr = func(expr expression.Expression) bool {
		sf, ok := expr.(*expression.ScalarFunction)
		if !ok {
			return false
		}
		switch sf.FuncName.L {
		case ast.UnaryNot:
			// Reject NOT operators - they can make predicates non-equal
			return false
		case ast.LogicOr, ast.LogicAnd:
			for _, arg := range sf.GetArgs() {
				if !isEqualPredicateOrOr(arg) {
					return false
				}
			}
			return true
		case ast.EQ, ast.In:
			// Check if it's an equal predicate (eq) or IN predicate (in)
			// Also reject any other comparison operators that are not equal/IN
			return true
		default:
			// Reject all other comparison operators (LT, GT, LE, GE, NE, etc.)
			// and any other functions that are not equal/IN predicates
			return false
		}
	}

	// Check all access conditions
	for _, cond := range c.path.AccessConds {
		if !isEqualPredicateOrOr(cond) {
			return false
		}
	}
	return true
}

func (c *candidatePath) equalPredicateCount() int {
	if c.indexJoinCols > 0 { // this candidate path is for Index Join
		// Specially handle indexes under Index Join, since these indexes can't see the join key themselves, we can't
		// use path.EqOrInCondCount here.
		// For example, for "where t1.a=t2.a and t2.b=1" and index "idx(t2, a, b)", since the DataSource of t2 can't
		// see the join key "t1.a=t2.a", it only considers "t2.b=1" as access condition, so its EqOrInCondCount is 0,
		// but actually it should be 2.
		return c.indexJoinCols
	}

	// Exit if this isn't a DNF condition or has no access conditions
	if !c.path.IsDNFCond || len(c.path.AccessConds) == 0 {
		return c.path.EqOrInCondCount
	}
	if c.hasOnlyEqualPredicatesInDNF() {
		return c.path.MinAccessCondsForDNFCond
	}
	return max(0, c.path.MinAccessCondsForDNFCond-1)
}

func getPruningInfo(ds *logicalop.DataSource, candidates []*candidatePath, prop *property.PhysicalProperty) string {
	if len(candidates) == len(ds.PossibleAccessPaths) {
		return ""
	}
	if len(candidates) == 1 && len(candidates[0].path.Ranges) == 0 {
		// For TableDual, we don't need to output pruning info.
		return ""
	}
	names := make([]string, 0, len(candidates))
	var tableName string
	if ds.TableAsName.O == "" {
		tableName = ds.TableInfo.Name.O
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
	if ds.IsForUpdateRead && ds.SCtx().GetSessionVars().TxnCtx.IsExplicit {
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
		pass := admitIndexJoinInnerChildPattern(ds)
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

// convertToIndexMergeScan builds the index merge scan for intersection or union cases.
func convertToIndexMergeScan(ds *logicalop.DataSource, prop *property.PhysicalProperty, candidate *candidatePath) (task base.Task, err error) {
	if prop.IsFlashProp() || prop.TaskTp == property.CopSingleReadTaskType {
		return base.InvalidTask, nil
	}
	// lift the limitation of that double read can not build index merge **COP** task with intersection.
	// that means we can output a cop task here without encapsulating it as root task, for the convenience of attaching limit to its table side.

	intest.Assert(candidate.matchPropResult != property.PropMatchedNeedMergeSort,
		"index merge path should not match property using merge sort")

	if !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() {
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
	cop := &physicalop.CopTask{
		IndexPlanFinished: false,
		TblColHists:       ds.TblColHists,
	}
	cop.PhysPlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   ds.AllConds,
		PartitionNames: ds.PartitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.OutputNames(),
	}
	// Add sort items for index scan for merge-sort operation between partitions.
	byItems := make([]*util.ByItems, 0, len(prop.SortItems))
	for _, si := range prop.SortItems {
		byItems = append(byItems, &util.ByItems{
			Expr: si.Col,
			Desc: si.Desc,
		})
	}
	globalRemainingFilters := make([]expression.Expression, 0, 3)
	for _, partPath := range path.PartialIndexPaths {
		var scan base.PhysicalPlan
		if partPath.IsTablePath() {
			scan = convertToPartialTableScan(ds, prop, partPath, candidate.matchPropResult, byItems)
		} else {
			var remainingFilters []expression.Expression
			scan, remainingFilters, err = physicalop.ConvertToPartialIndexScan(ds, cop.PhysPlanPartInfo, prop, partPath, candidate.matchPropResult, byItems)
			if err != nil {
				return base.InvalidTask, err
			}
			if prop.TaskTp != property.RootTaskType && len(remainingFilters) > 0 {
				return base.InvalidTask, nil
			}
			globalRemainingFilters = append(globalRemainingFilters, remainingFilters...)
		}
		scans = append(scans, scan)
	}
	totalRowCount := path.CountAfterAccess
	// Add an arbitrary tolerance factor to account for comparison with floating point
	if (prop.ExpectedCnt + cost.ToleranceFactor) < ds.StatsInfo().RowCount {
		totalRowCount *= prop.ExpectedCnt / ds.StatsInfo().RowCount
	}
	ts, remainingFilters2, moreColumn, err := physicalop.BuildIndexMergeTableScan(ds, path.TableFilters, totalRowCount, candidate.matchPropResult == property.PropMatched)
	if err != nil {
		return base.InvalidTask, err
	}
	if prop.TaskTp != property.RootTaskType && len(remainingFilters2) > 0 {
		return base.InvalidTask, nil
	}
	globalRemainingFilters = append(globalRemainingFilters, remainingFilters2...)
	cop.KeepOrder = candidate.matchPropResult == property.PropMatched
	cop.TablePlan = ts
	cop.IdxMergePartPlans = scans
	cop.IdxMergeIsIntersection = path.IndexMergeIsIntersection
	cop.IdxMergeAccessMVIndex = path.IndexMergeAccessMVIndex
	if moreColumn {
		cop.NeedExtraProj = true
		cop.OriginSchema = ds.Schema()
	}
	if len(globalRemainingFilters) != 0 {
		cop.RootTaskConds = globalRemainingFilters
	}
	// after we lift the limitation of intersection and cop-type task in the code in this
	// function above, we could set its index plan finished as true once we found its table
	// plan is pure table scan below.
	// And this will cause cost underestimation when we estimate the cost of the entire cop
	// task plan in function `getTaskPlanCost`.
	if prop.TaskTp == property.RootTaskType {
		cop.IndexPlanFinished = true
		task = cop.ConvertToRootTask(ds.SCtx())
	} else {
		_, pureTableScan := ts.(*physicalop.PhysicalTableScan)
		if !pureTableScan {
			cop.IndexPlanFinished = true
		}
		task = cop
	}
	return task, nil
}

func checkColinSchema(cols []*expression.Column, schema *expression.Schema) bool {
	for _, col := range cols {
		if schema.ColumnIndex(col) == -1 {
			return false
		}
	}
	return true
}

func convertToPartialTableScan(ds *logicalop.DataSource, prop *property.PhysicalProperty, path *util.AccessPath, matchProp property.PhysicalPropMatchResult, byItems []*util.ByItems) (tablePlan base.PhysicalPlan) {
	intest.Assert(matchProp != property.PropMatchedNeedMergeSort,
		"partial paths of index merge path should not match property using merge sort")
	ts, rowCount := physicalop.GetOriginalPhysicalTableScan(ds, prop, path, matchProp.Matched())
	overwritePartialTableScanSchema(ds, ts)
	// remove ineffetive filter condition after overwriting physicalscan schema
	newFilterConds := make([]expression.Expression, 0, len(path.TableFilters))
	for _, cond := range ts.FilterCondition {
		cols := expression.ExtractColumns(cond)
		if checkColinSchema(cols, ts.Schema()) {
			newFilterConds = append(newFilterConds, cond)
		}
	}
	ts.FilterCondition = newFilterConds
	if matchProp.Matched() {
		if ts.Table.GetPartitionInfo() != nil && ts.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
			tmpColumns, tmpSchema, _ := physicalop.AddExtraPhysTblIDColumn(ts.SCtx(), ts.Columns, ts.Schema())
			ts.Columns = tmpColumns
			ts.SetSchema(tmpSchema)
		}
		ts.ByItems = byItems
	}
	if len(ts.FilterCondition) > 0 {
		selectivity, _, err := cardinality.Selectivity(ds.SCtx(), ds.TableStats.HistColl, ts.FilterCondition, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = cost.SelectionFactor
		}
		tablePlan = physicalop.PhysicalSelection{Conditions: ts.FilterCondition}.Init(ts.SCtx(), ts.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), selectivity*rowCount), ds.QueryBlockOffset())
		tablePlan.SetChildren(ts)
		return tablePlan
	}
	tablePlan = ts
	return tablePlan
}

// overwritePartialTableScanSchema change the schema of partial table scan to handle columns.
func overwritePartialTableScanSchema(ds *logicalop.DataSource, ts *physicalop.PhysicalTableScan) {
	handleCols := ds.HandleCols
	if handleCols == nil {
		handleCols = util.NewIntHandleCols(ds.NewExtraHandleSchemaCol())
	}
	hdColNum := handleCols.NumCols()
	exprCols := make([]*expression.Column, 0, hdColNum)
	infoCols := make([]*model.ColumnInfo, 0, hdColNum)
	for i := range hdColNum {
		col := handleCols.GetCol(i)
		exprCols = append(exprCols, col)
		if c := model.FindColumnInfoByID(ds.TableInfo.Columns, col.ID); c != nil {
			infoCols = append(infoCols, c)
		} else {
			infoCols = append(infoCols, col.ToInfo())
		}
	}
	ts.SetSchema(expression.NewSchema(exprCols...))
	ts.Columns = infoCols
}

// convertToIndexScan converts the DataSource to index scan with idx.
func convertToIndexScan(ds *logicalop.DataSource, prop *property.PhysicalProperty,
	candidate *candidatePath) (task base.Task, err error) {
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
	if !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() {
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
	is := physicalop.GetOriginalPhysicalIndexScan(ds, prop, path, candidate.matchPropResult.Matched(), candidate.path.IsSingleScan)
	cop := &physicalop.CopTask{
		IndexPlan:   is,
		TblColHists: ds.TblColHists,
		TblCols:     ds.TblCols,
		ExpectCnt:   uint64(prop.ExpectedCnt),
	}
	cop.PhysPlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   ds.AllConds,
		PartitionNames: ds.PartitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.OutputNames(),
	}
	if !candidate.path.IsSingleScan {
		// On this way, it's double read case.
		ts := physicalop.PhysicalTableScan{
			Columns:         sliceutil.DeepClone(ds.Columns),
			Table:           is.Table,
			TableAsName:     ds.TableAsName,
			DBName:          ds.DBName,
			PhysicalTableID: ds.PhysicalTableID,
			TblCols:         ds.TblCols,
			TblColHists:     ds.TblColHists,
		}.Init(ds.SCtx(), is.QueryBlockOffset())
		ts.SetIsPartition(ds.PartitionDefIdx != nil)
		ts.SetSchema(ds.Schema().Clone())
		// We set `StatsVersion` here and fill other fields in `(*copTask).finishIndexPlan`. Since `copTask.indexPlan` may
		// change before calling `(*copTask).finishIndexPlan`, we don't know the stats information of `ts` currently and on
		// the other hand, it may be hard to identify `StatsVersion` of `ts` in `(*copTask).finishIndexPlan`.
		ts.SetStats(&property.StatsInfo{StatsVersion: ds.TableStats.StatsVersion})
		usedStats := ds.SCtx().GetSessionVars().StmtCtx.GetUsedStatsInfo(false)
		if usedStats != nil && usedStats.GetUsedInfo(ts.PhysicalTableID) != nil {
			ts.UsedStatsInfo = usedStats.GetUsedInfo(ts.PhysicalTableID)
		}
		cop.TablePlan = ts
		cop.IndexLookUpPushDown = candidate.path.IsIndexLookUpPushDown
	}
	task = cop
	if cop.TablePlan != nil && ds.TableInfo.IsCommonHandle {
		cop.CommonHandleCols = ds.CommonHandleCols
		commonHandle := ds.HandleCols.(*util.CommonHandleCols)
		for _, col := range commonHandle.GetColumns() {
			if ds.Schema().ColumnIndex(col) == -1 {
				ts := cop.TablePlan.(*physicalop.PhysicalTableScan)
				ts.Schema().Append(col)
				ts.Columns = append(ts.Columns, col.ToInfo())
				cop.NeedExtraProj = true
			}
		}
	}
	if candidate.matchPropResult.Matched() {
		cop.KeepOrder = true
		if cop.TablePlan != nil && !ds.TableInfo.IsCommonHandle {
			col, isNew := cop.TablePlan.(*physicalop.PhysicalTableScan).AppendExtraHandleCol(ds)
			cop.ExtraHandleCol = col
			cop.NeedExtraProj = cop.NeedExtraProj || isNew
		}

		// Add sort items for index scan for the merge-sort operation
		// Case 1: keep order between partitions (only required for local index)
		// Case 2: keep order between range groups (see comments of PropMatchedNeedMergeSort and
		// AccessPath.GroupedRanges for details)
		// Case 3: both
		if (ds.TableInfo.GetPartitionInfo() != nil && !is.Index.Global) ||
			candidate.matchPropResult == property.PropMatchedNeedMergeSort {
			byItems := make([]*util.ByItems, 0, len(prop.SortItems))
			for _, si := range prop.SortItems {
				byItems = append(byItems, &util.ByItems{
					Expr: si.Col,
					Desc: si.Desc,
				})
			}
			cop.IndexPlan.(*physicalop.PhysicalIndexScan).ByItems = byItems
		}
		if candidate.matchPropResult == property.PropMatchedNeedMergeSort {
			is.GroupedRanges = candidate.path.GroupedRanges
			is.GroupByColIdxs = candidate.path.GroupByColIdxs
		}
		if ds.TableInfo.GetPartitionInfo() != nil {
			if cop.TablePlan != nil && ds.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune() {
				if !is.Index.Global {
					tmpColumns, tmpSchema, _ := physicalop.AddExtraPhysTblIDColumn(is.SCtx(), is.Columns, is.Schema())
					is.Columns = tmpColumns
					is.SetSchema(tmpSchema)
				}
				// global index for tableScan with keepOrder also need PhysicalTblID
				ts := cop.TablePlan.(*physicalop.PhysicalTableScan)
				tmpColumns, tmpSchema, succ := physicalop.AddExtraPhysTblIDColumn(ts.SCtx(), ts.Columns, ts.Schema())
				ts.Columns = tmpColumns
				ts.SetSchema(tmpSchema)
				cop.NeedExtraProj = cop.NeedExtraProj || succ
			}
		}
	}
	if cop.NeedExtraProj {
		cop.OriginSchema = ds.Schema()
	}
	// prop.IsSortItemEmpty() would always return true when coming to here,
	// so we can just use prop.ExpectedCnt as parameter of AddPushedDownSelection.
	finalStats := ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt)
	if err = addPushedDownSelection4PhysicalIndexScan(is, cop, ds, path, finalStats); err != nil {
		return base.InvalidTask, err
	}
	if prop.TaskTp == property.RootTaskType {
		task = task.ConvertToRootTask(ds.SCtx())
	} else if _, ok := task.(*physicalop.RootTask); ok {
		return base.InvalidTask, nil
	}
	return task, nil
}

// addPushedDownSelection is to add pushdown selection
func addPushedDownSelection4PhysicalIndexScan(is *physicalop.PhysicalIndexScan, copTask *physicalop.CopTask, p *logicalop.DataSource, path *util.AccessPath, finalStats *property.StatsInfo) error {
	// Add filter condition to table plan now.
	indexConds, tableConds := path.IndexFilters, path.TableFilters
	tableConds, copTask.RootTaskConds = physicalop.SplitSelCondsWithVirtualColumn(tableConds)

	var newRootConds []expression.Expression
	pctx := util.GetPushDownCtx(is.SCtx())
	indexConds, newRootConds = expression.PushDownExprs(pctx, indexConds, kv.TiKV)
	copTask.RootTaskConds = append(copTask.RootTaskConds, newRootConds...)

	tableConds, newRootConds = expression.PushDownExprs(pctx, tableConds, kv.TiKV)
	copTask.RootTaskConds = append(copTask.RootTaskConds, newRootConds...)

	// Add a `Selection` for `IndexScan` with global index.
	// It should pushdown to TiKV, DataSource schema doesn't contain partition id column.
	indexConds, err := is.AddSelectionConditionForGlobalIndex(p, copTask.PhysPlanPartInfo, indexConds)
	if err != nil {
		return err
	}

	if len(indexConds) != 0 {
		var selectivity float64
		if path.CountAfterAccess > 0 {
			selectivity = path.CountAfterIndex / path.CountAfterAccess
		}
		count := is.StatsInfo().RowCount * selectivity
		stats := p.TableStats.ScaleByExpectCnt(p.SCtx().GetSessionVars(), count)
		indexSel := physicalop.PhysicalSelection{Conditions: indexConds}.Init(is.SCtx(), stats, is.QueryBlockOffset())
		indexSel.SetChildren(is)
		copTask.IndexPlan = indexSel
	}
	if len(tableConds) > 0 {
		copTask.FinishIndexPlan()
		tableSel := physicalop.PhysicalSelection{Conditions: tableConds}.Init(is.SCtx(), finalStats, is.QueryBlockOffset())
		if len(copTask.RootTaskConds) != 0 {
			selectivity, _, err := cardinality.Selectivity(is.SCtx(), copTask.TblColHists, tableConds, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = cost.SelectionFactor
			}
			tableSel.SetStats(copTask.Plan().StatsInfo().Scale(is.SCtx().GetSessionVars(), selectivity))
		}
		tableSel.SetChildren(copTask.TablePlan)
		copTask.TablePlan = tableSel
	}
	return nil
}

func splitIndexFilterConditions(ds *logicalop.DataSource, conditions []expression.Expression, indexColumns []*expression.Column,
	idxColLens []int) (indexConds, tableConds []expression.Expression) {
	var indexConditions, tableConditions []expression.Expression
	for _, cond := range conditions {
		var covered bool
		if ds.SCtx().GetSessionVars().OptPrefixIndexSingleScan {
			covered = ds.IsIndexCoveringCondition(cond, indexColumns, idxColLens)
		} else {
			covered = ds.IsIndexCoveringColumns(expression.ExtractColumns(cond), indexColumns, idxColLens)
		}
		if covered {
			indexConditions = append(indexConditions, cond)
		} else {
			tableConditions = append(tableConditions, cond)
		}
	}
	return indexConditions, tableConditions
}

// isPointGetPath indicates whether the conditions are point-get-able.
// eg: create table t(a int, b int,c int unique, primary (a,b))
// select * from t where a = 1 and b = 1 and c =1;
// the datasource can access by primary key(a,b) or unique key c which are both point-get-able
func isPointGetPath(ds *logicalop.DataSource, path *util.AccessPath) bool {
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
func convertToTableScan(ds *logicalop.DataSource, prop *property.PhysicalProperty, candidate *candidatePath) (base.Task, error) {
	// It will be handled in convertToIndexScan.
	if prop.TaskTp == property.CopMultiReadTaskType {
		return base.InvalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() {
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
	ts, _ := physicalop.GetOriginalPhysicalTableScan(ds, prop, candidate.path, candidate.matchPropResult.Matched())
	// In disaggregated tiflash mode, only MPP is allowed, cop and batchCop is deprecated.
	// So if prop.TaskTp is RootTaskType, have to use mppTask then convert to rootTask.
	isTiFlashPath := ts.StoreType == kv.TiFlash
	canMppConvertToRoot := prop.TaskTp == property.RootTaskType && ds.SCtx().GetSessionVars().IsMPPAllowed() && isTiFlashPath
	canMppConvertToRootForDisaggregatedTiFlash := config.GetGlobalConfig().DisaggregatedTiFlash && canMppConvertToRoot
	canMppConvertToRootForWhenTiFlashCopIsBanned := ds.SCtx().GetSessionVars().IsTiFlashCopBanned() && canMppConvertToRoot

	// Fast checks
	if isTiFlashPath && ts.KeepOrder && (ts.Desc || ds.SCtx().GetSessionVars().TiFlashFastScan) {
		// TiFlash fast mode(https://github.com/pingcap/tidb/pull/35851) does not support keep order in TableScan
		return base.InvalidTask, nil
	}
	if prop.TaskTp == property.MppTaskType || canMppConvertToRootForDisaggregatedTiFlash || canMppConvertToRootForWhenTiFlashCopIsBanned {
		if ts.KeepOrder {
			return base.InvalidTask, nil
		}
		if prop.MPPPartitionTp != property.AnyType {
			return base.InvalidTask, nil
		}
		// If it has vector property, we need to check the candidate.matchProperty.
		if candidate.path.Index != nil && candidate.path.Index.VectorInfo != nil && !candidate.matchPropResult.Matched() {
			return base.InvalidTask, nil
		}
	} else {
		if isTiFlashPath && config.GetGlobalConfig().DisaggregatedTiFlash || isTiFlashPath && ds.SCtx().GetSessionVars().IsTiFlashCopBanned() {
			// prop.TaskTp is cop related, just return base.InvalidTask.
			return base.InvalidTask, nil
		}
		if isTiFlashPath && candidate.matchPropResult.Matched() && ds.TableInfo.GetPartitionInfo() != nil {
			// TableScan on partition table on TiFlash can't keep order.
			return base.InvalidTask, nil
		}
	}

	// MPP task
	if prop.TaskTp == property.MppTaskType || canMppConvertToRootForDisaggregatedTiFlash || canMppConvertToRootForWhenTiFlashCopIsBanned {
		if candidate.path.Index != nil && candidate.path.Index.VectorInfo != nil {
			// Only the corresponding index can generate a valid task.
			intest.Assert(ts.Table.Columns[candidate.path.Index.Columns[0].Offset].ID == prop.VectorProp.Column.ID, "The passed vector column is not matched with the index")
			distanceMetric := model.IndexableFnNameToDistanceMetric[prop.VectorProp.DistanceFnName.L]
			distanceMetricPB := tipb.VectorDistanceMetric_value[string(distanceMetric)]
			intest.Assert(distanceMetricPB != 0, "unexpected distance metric")

			ts.UsedColumnarIndexes = append(ts.UsedColumnarIndexes, buildVectorIndexExtra(
				candidate.path.Index,
				tipb.ANNQueryType_OrderBy,
				tipb.VectorDistanceMetric(distanceMetricPB),
				prop.VectorProp.TopK,
				ts.Table.Columns[candidate.path.Index.Columns[0].Offset].Name.L,
				prop.VectorProp.Vec.SerializeTo(nil),
				tidbutil.ColumnToProto(prop.VectorProp.Column.ToInfo(), false, false),
			))
			ts.SetStats(property.DeriveLimitStats(ts.StatsInfo(), float64(prop.VectorProp.TopK)))
		}
		// ********************************** future deprecated start **************************/
		var hasVirtualColumn bool
		for _, col := range ts.Schema().Columns {
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
		mppTask := physicalop.NewMppTask(ts, property.AnyType, nil, ds.TblColHists, nil)
		ts.PlanPartInfo = &physicalop.PhysPlanPartInfo{
			PruningConds:   ds.AllConds,
			PartitionNames: ds.PartitionNames,
			Columns:        ds.TblCols,
			ColumnNames:    ds.OutputNames(),
		}
		mppTask = addPushedDownSelectionToMppTask4PhysicalTableScan(ts, mppTask, ds.StatsInfo().ScaleByExpectCnt(ts.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.AstIndexHints)
		var task base.Task = mppTask
		if !mppTask.Invalid() {
			if prop.TaskTp == property.MppTaskType && len(mppTask.RootTaskConds) > 0 {
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
	// Cop task
	copTask := &physicalop.CopTask{
		TablePlan:         ts,
		IndexPlanFinished: true,
		TblColHists:       ds.TblColHists,
	}
	copTask.PhysPlanPartInfo = &physicalop.PhysPlanPartInfo{
		PruningConds:   ds.AllConds,
		PartitionNames: ds.PartitionNames,
		Columns:        ds.TblCols,
		ColumnNames:    ds.OutputNames(),
	}
	ts.PlanPartInfo = copTask.PhysPlanPartInfo
	var task base.Task = copTask
	if candidate.matchPropResult.Matched() {
		copTask.KeepOrder = true
		if ds.TableInfo.GetPartitionInfo() != nil || candidate.matchPropResult == property.PropMatchedNeedMergeSort {
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
		if candidate.matchPropResult == property.PropMatchedNeedMergeSort {
			ts.GroupedRanges = candidate.path.GroupedRanges
			ts.GroupByColIdxs = candidate.path.GroupByColIdxs
		}
	}
	addPushedDownSelection4PhysicalTableScan(ts, copTask, ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.AstIndexHints)
	if prop.IsFlashProp() && len(copTask.RootTaskConds) != 0 {
		return base.InvalidTask, nil
	}
	if prop.TaskTp == property.RootTaskType {
		task = task.ConvertToRootTask(ds.SCtx())
	} else if _, ok := task.(*physicalop.RootTask); ok {
		return base.InvalidTask, nil
	}
	return task, nil
}

func convertToSampleTable(ds *logicalop.DataSource, prop *property.PhysicalProperty,
	candidate *candidatePath) (base.Task, error) {
	if prop.TaskTp == property.CopMultiReadTaskType {
		return base.InvalidTask, nil
	}
	if !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() {
		return base.InvalidTask, nil
	}
	if candidate.matchPropResult.Matched() {
		// Disable keep order property for sample table path.
		return base.InvalidTask, nil
	}
	p := physicalop.PhysicalTableSample{
		TableSampleInfo: ds.SampleInfo,
		TableInfo:       ds.Table,
		PhysicalTableID: ds.PhysicalTableID,
		Desc:            candidate.matchPropResult.Matched() && prop.SortItems[0].Desc,
	}.Init(ds.SCtx(), ds.QueryBlockOffset())
	p.SetSchema(ds.Schema())
	rt := &physicalop.RootTask{}
	rt.SetPlan(p)
	return rt, nil
}

func convertToPointGet(ds *logicalop.DataSource, prop *property.PhysicalProperty, candidate *candidatePath) base.Task {
	if !prop.IsSortItemEmpty() && !candidate.matchPropResult.Matched() {
		return base.InvalidTask
	}
	if prop.TaskTp == property.CopMultiReadTaskType && candidate.path.IsSingleScan ||
		prop.TaskTp == property.CopSingleReadTaskType && !candidate.path.IsSingleScan {
		return base.InvalidTask
	}

	if metadef.IsMemDB(ds.DBName.L) {
		return base.InvalidTask
	}

	accessCnt := math.Min(candidate.path.CountAfterAccess, float64(1))
	pointGetPlan := &physicalop.PointGetPlan{
		AccessConditions: candidate.path.AccessConds,
		DBName:           ds.DBName.L,
		TblInfo:          ds.TableInfo,
		LockWaitTime:     ds.SCtx().GetSessionVars().LockWaitTimeout,
		Columns:          ds.Columns,
	}
	pointGetPlan.SetSchema(ds.Schema().Clone())
	pointGetPlan.SetOutputNames(ds.OutputNames())
	pointGetPlan = pointGetPlan.Init(ds.SCtx(), ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), accessCnt), ds.QueryBlockOffset())
	if ds.PartitionDefIdx != nil {
		pointGetPlan.PartitionIdx = ds.PartitionDefIdx
	}
	pointGetPlan.PartitionNames = ds.PartitionNames
	rTsk := &physicalop.RootTask{}
	rTsk.SetPlan(pointGetPlan)
	if candidate.path.IsIntHandlePath {
		pointGetPlan.Handle = kv.IntHandle(candidate.path.Ranges[0].LowVal[0].GetInt64())
		pointGetPlan.UnsignedHandle = mysql.HasUnsignedFlag(ds.HandleCols.GetCol(0).RetType.GetFlag())
		pointGetPlan.SetAccessCols(ds.TblCols)
		found := false
		for i := range ds.Columns {
			if ds.Columns[i].ID == ds.HandleCols.GetCol(0).ID {
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
			sel := physicalop.PhysicalSelection{
				Conditions: candidate.path.TableFilters,
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(pointGetPlan)
			rTsk.SetPlan(sel)
		}
	} else {
		pointGetPlan.IndexInfo = candidate.path.Index
		pointGetPlan.IdxCols = candidate.path.IdxCols
		pointGetPlan.IdxColLens = candidate.path.IdxColLens
		pointGetPlan.IndexValues = candidate.path.Ranges[0].LowVal
		if candidate.path.IsSingleScan {
			pointGetPlan.SetAccessCols(candidate.path.IdxCols)
		} else {
			pointGetPlan.SetAccessCols(ds.TblCols)
		}
		// Add index condition to table plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.TableFilters) > 0 {
			sel := physicalop.PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.TableFilters...),
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(pointGetPlan)
			rTsk.SetPlan(sel)
		}
	}

	return rTsk
}

func convertToBatchPointGet(ds *logicalop.DataSource, prop *property.PhysicalProperty, candidate *candidatePath) base.Task {
	// For batch point get, we don't try to satisfy the sort property with an extra merge sort,
	// so only PropMatched is allowed.
	if !prop.IsSortItemEmpty() && candidate.matchPropResult != property.PropMatched {
		return base.InvalidTask
	}
	if prop.TaskTp == property.CopMultiReadTaskType && candidate.path.IsSingleScan ||
		prop.TaskTp == property.CopSingleReadTaskType && !candidate.path.IsSingleScan {
		return base.InvalidTask
	}

	accessCnt := math.Min(candidate.path.CountAfterAccess, float64(len(candidate.path.Ranges)))
	batchPointGetPlan := &physicalop.BatchPointGetPlan{
		DBName:           ds.DBName.L,
		AccessConditions: candidate.path.AccessConds,
		TblInfo:          ds.TableInfo,
		KeepOrder:        !prop.IsSortItemEmpty(),
		Columns:          ds.Columns,
		PartitionNames:   ds.PartitionNames,
	}
	batchPointGetPlan.SetCtx(ds.SCtx())
	if ds.PartitionDefIdx != nil {
		batchPointGetPlan.SinglePartition = true
		batchPointGetPlan.PartitionIdxs = []int{*ds.PartitionDefIdx}
	}
	if batchPointGetPlan.KeepOrder {
		batchPointGetPlan.Desc = prop.SortItems[0].Desc
	}
	rTsk := &physicalop.RootTask{}
	if candidate.path.IsIntHandlePath {
		for _, ran := range candidate.path.Ranges {
			batchPointGetPlan.Handles = append(batchPointGetPlan.Handles, kv.IntHandle(ran.LowVal[0].GetInt64()))
		}
		batchPointGetPlan.SetAccessCols(ds.TblCols)
		found := false
		for i := range ds.Columns {
			if ds.Columns[i].ID == ds.HandleCols.GetCol(0).ID {
				batchPointGetPlan.HandleColOffset = ds.Columns[i].Offset
				found = true
				break
			}
		}
		if !found {
			return base.InvalidTask
		}

		// Add filter condition to table plan now.
		if len(candidate.path.TableFilters) > 0 {
			batchPointGetPlan.Init(ds.SCtx(), ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), accessCnt), ds.Schema().Clone(), ds.OutputNames(), ds.QueryBlockOffset())
			sel := physicalop.PhysicalSelection{
				Conditions: candidate.path.TableFilters,
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.QueryBlockOffset())
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
			batchPointGetPlan.SetAccessCols(candidate.path.IdxCols)
		} else {
			batchPointGetPlan.SetAccessCols(ds.TblCols)
		}
		// Add index condition to table plan now.
		if len(candidate.path.IndexFilters)+len(candidate.path.TableFilters) > 0 {
			batchPointGetPlan.Init(ds.SCtx(), ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), accessCnt), ds.Schema().Clone(), ds.OutputNames(), ds.QueryBlockOffset())
			sel := physicalop.PhysicalSelection{
				Conditions: append(candidate.path.IndexFilters, candidate.path.TableFilters...),
			}.Init(ds.SCtx(), ds.StatsInfo().ScaleByExpectCnt(ds.SCtx().GetSessionVars(), prop.ExpectedCnt), ds.QueryBlockOffset())
			sel.SetChildren(batchPointGetPlan)
			rTsk.SetPlan(sel)
		}
	}
	if rTsk.GetPlan() == nil {
		tmpP := batchPointGetPlan.Init(ds.SCtx(), ds.TableStats.ScaleByExpectCnt(ds.SCtx().GetSessionVars(), accessCnt), ds.Schema().Clone(), ds.OutputNames(), ds.QueryBlockOffset())
		rTsk.SetPlan(tmpP)
	}

	return rTsk
}

func addPushedDownSelectionToMppTask4PhysicalTableScan(ts *physicalop.PhysicalTableScan, mpp *physicalop.MppTask, stats *property.StatsInfo, indexHints []*ast.IndexHint) *physicalop.MppTask {
	filterCondition, rootTaskConds := physicalop.SplitSelCondsWithVirtualColumn(ts.FilterCondition)
	var newRootConds []expression.Expression
	filterCondition, newRootConds = expression.PushDownExprs(util.GetPushDownCtx(ts.SCtx()), filterCondition, ts.StoreType)
	mpp.RootTaskConds = append(rootTaskConds, newRootConds...)

	ts.FilterCondition = filterCondition
	// Add filter condition to table plan now.
	if len(ts.FilterCondition) > 0 {
		if sel := ts.BuildPushedDownSelection(stats, indexHints); sel != nil {
			sel.SetChildren(ts)
			mpp.SetPlan(sel)
		} else {
			mpp.SetPlan(ts)
		}
	}
	return mpp
}

func addPushedDownSelection4PhysicalTableScan(ts *physicalop.PhysicalTableScan, copTask *physicalop.CopTask, stats *property.StatsInfo, indexHints []*ast.IndexHint) {
	ts.FilterCondition, copTask.RootTaskConds = physicalop.SplitSelCondsWithVirtualColumn(ts.FilterCondition)
	var newRootConds []expression.Expression
	ts.FilterCondition, newRootConds = expression.PushDownExprs(util.GetPushDownCtx(ts.SCtx()), ts.FilterCondition, ts.StoreType)
	copTask.RootTaskConds = append(copTask.RootTaskConds, newRootConds...)

	// Add filter condition to table plan now.
	if len(ts.FilterCondition) > 0 {
		sel := ts.BuildPushedDownSelection(stats, indexHints)
		if sel == nil {
			copTask.TablePlan = ts
			return
		}
		if len(copTask.RootTaskConds) != 0 {
			selectivity, _, err := cardinality.Selectivity(ts.SCtx(), copTask.TblColHists, sel.Conditions, nil)
			if err != nil {
				logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
				selectivity = cost.SelectionFactor
			}
			sel.SetStats(ts.StatsInfo().Scale(ts.SCtx().GetSessionVars(), selectivity))
		}
		sel.SetChildren(ts)
		copTask.TablePlan = sel
	}
}

func validateTableSamplePlan(ds *logicalop.DataSource, t base.Task, err error) error {
	if err != nil {
		return err
	}
	if ds.SampleInfo != nil && !t.Invalid() {
		if _, ok := t.Plan().(*physicalop.PhysicalTableSample); !ok {
			return expression.ErrInvalidTableSample.GenWithStackByArgs("plan not supported")
		}
	}
	return nil
}

// mockLogicalPlan4Test is a LogicalPlan which is used for unit test.
// The basic assumption:
//  1. mockLogicalPlan4Test can generate tow kinds of physical plan: physicalPlan1 and
//     physicalPlan2. physicalPlan1 can pass the property only when they are the same
//     order; while physicalPlan2 cannot match any of the property(in other words, we can
//     generate it only when then property is empty).
//  2. We have a hint for physicalPlan2.
//  3. If the property is empty, we still need to check `canGeneratePlan2` to decide
//     whether it can generate physicalPlan2.
type mockLogicalPlan4Test struct {
	logicalop.BaseLogicalPlan
	// hasHintForPlan2 indicates whether this mockPlan contains hint.
	// This hint is used to generate physicalPlan2. See the implementation
	// of ExhaustPhysicalPlans().
	hasHintForPlan2 bool
	// canGeneratePlan2 indicates whether this plan can generate physicalPlan2.
	canGeneratePlan2 bool
	// costOverflow indicates whether this plan will generate physical plan whose cost is overflowed.
	costOverflow bool
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
