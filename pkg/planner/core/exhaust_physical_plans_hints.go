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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

func recordWarnings(lp base.LogicalPlan, prop *property.PhysicalProperty, inEnforce bool) error {
	switch x := lp.(type) {
	case *logicalop.LogicalAggregation:
		return recordAggregationHintWarnings(x)
	case *logicalop.LogicalTopN, *logicalop.LogicalLimit:
		return recordLimitToCopWarnings(lp)
	case *logicalop.LogicalJoin:
		return recordIndexJoinHintWarnings(x, prop, inEnforce)
	default:
		// no warnings to record
		return nil
	}
}

func recordAggregationHintWarnings(la *logicalop.LogicalAggregation) error {
	if la.PreferAggToCop {
		return plannererrors.ErrInternal.FastGen("Optimizer Hint AGG_TO_COP is inapplicable")
	}
	return nil
}

func recordLimitToCopWarnings(lp base.LogicalPlan) error {
	var preferPushDown *bool
	switch lp := lp.(type) {
	case *logicalop.LogicalTopN:
		preferPushDown = &lp.PreferLimitToCop
	case *logicalop.LogicalLimit:
		preferPushDown = &lp.PreferLimitToCop
	default:
		return nil
	}
	if *preferPushDown {
		return plannererrors.ErrInternal.FastGen("Optimizer Hint LIMIT_TO_COP is inapplicable")
	}
	return nil
}

// recordIndexJoinHintWarnings records the warnings msg if no valid preferred physic are picked.
// todo: extend recordIndexJoinHintWarnings to support all kind of operator's warnings handling.
func recordIndexJoinHintWarnings(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, inEnforce bool) error {
	// handle mpp join hints first.
	if (p.PreferJoinType&h.PreferShuffleJoin) > 0 || (p.PreferJoinType&h.PreferBCJoin) > 0 {
		var errMsg string
		if (p.PreferJoinType & h.PreferShuffleJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the shuffle_join() hint is invalid"
		} else {
			errMsg = "The join can not push down to the MPP side, the broadcast_join() hint is invalid"
		}
		return plannererrors.ErrInternal.FastGen(errMsg)
	}
	// handle index join hints.
	if !p.PreferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return nil // no force index join hints
	}
	// Cannot find any valid index join plan with these force hints.
	// Print warning message if any hints cannot work.
	// If the required property is not empty, we will enforce it and try the hint again.
	// So we only need to generate warning message when the property is empty.
	//
	// but for warnings handle inside findBestTask here, even the not-empty prop
	// will be reset to get the planNeedEnforce plans, but the prop passed down here will
	// still be the same, so here we change the admission to both.
	if prop.IsSortItemEmpty() || inEnforce {
		var indexJoinTables, indexHashJoinTables, indexMergeJoinTables []h.HintedTable
		if p.HintInfo != nil {
			t := p.HintInfo.IndexJoin
			indexJoinTables, indexHashJoinTables, indexMergeJoinTables = t.INLJTables, t.INLHJTables, t.INLMJTables
		}
		var errMsg string
		switch {
		case p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner): // prefer index join
			errMsg = fmt.Sprintf("Optimizer Hint %s or %s is inapplicable", h.Restore2JoinHint(h.HintINLJ, indexJoinTables), h.Restore2JoinHint(h.TiDBIndexNestedLoopJoin, indexJoinTables))
		case p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner): // prefer index hash join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLHJ, indexHashJoinTables))
		case p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner): // prefer index merge join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLMJ, indexMergeJoinTables))
		default:
			// only record warnings for index join hint not working now.
			return nil
		}
		// Append inapplicable reason.
		if len(p.EqualConditions) == 0 {
			errMsg += " without column equal ON condition"
		}
		// Generate warning message to client.
		return plannererrors.ErrInternal.FastGen(errMsg)
	}
	return nil
}

func applyLogicalHintVarEigen(lp base.LogicalPlan, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	return applyLogicalJoinHint(lp, pp) ||
		applyLogicalTopNAndLimitHint(lp, pp, childTasks) ||
		applyLogicalAggregationHint(lp, pp, childTasks)
}

// Get the most preferred and efficient one by hint and low-cost priority.
// since hint applicable plan may greater than 1, like inl_join can suit for:
// index_join, index_hash_join, index_merge_join, we should chase the most efficient
// one among them.
// applyLogicalJoinHint is used to handle logic hint/prefer/variable, which is not a strong guide for optimization phase.
// It is changed from handleForceIndexJoinHints to handle the preferred join hint among several valid physical plan choices.
// It will return true if the hint can be applied when saw a real physic plan successfully built and returned up from child.
// we cache the most preferred one among this valid and preferred physic plans. If there is no preferred physic applicable
// for the logic hint, we will return false and the optimizer will continue to return the normal low-cost one.
func applyLogicalJoinHint(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	return preferMergeJoin(lp, physicPlan) || preferIndexJoinFamily(lp, physicPlan) ||
		preferHashJoin(lp, physicPlan)
}

func applyLogicalAggregationHint(lp base.LogicalPlan, physicPlan base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	la, ok := lp.(*logicalop.LogicalAggregation)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	if la.HasDistinct() {
		// TODO: remove after the cost estimation of distinct pushdown is implemented.
		if la.SCtx().GetSessionVars().AllowDistinctAggPushDown {
			// when AllowDistinctAggPushDown is true, we will not consider root task type as before.
			if _, ok := childTasks[0].(*physicalop.CopTask); ok {
				return true
			}
		} else {
			switch childTasks[0].(type) {
			case *physicalop.RootTask:
				// If the distinct agg can't be allowed to push down, we will consider root task type.
				// which is try to get the same behavior as before like types := {RootTask} only.
				return true
			case *physicalop.MppTask:
				// If the distinct agg can't be allowed to push down, we will consider mpp task type too --- RootTask vs MPPTask
				// which is try to get the same behavior as before like types := {RootTask} and appended {MPPTask}.
				return true
			default:
				return false
			}
		}
	} else if la.PreferAggToCop {
		// If the aggregation is preferred to be pushed down to coprocessor, we will prefer it.
		if _, ok := childTasks[0].(*physicalop.CopTask); ok {
			return true
		}
	}
	return false
}

func applyLogicalTopNAndLimitHint(lp base.LogicalPlan, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	hintPrefer, _ := pushLimitOrTopNForcibly(lp, pp)
	if hintPrefer {
		// if there is a user hint control, try to get the copTask as the prior.
		// here we don't assert task itself, because when topN attach 2 cop task, it will become root type automatically.
		if _, ok := childTasks[0].(*physicalop.CopTask); ok {
			return true
		}
		return false
	}
	return false
}

func hasNormalPreferTask(lp base.LogicalPlan, state *enumerateState, pp base.PhysicalPlan, childTasks []base.Task) (preferred bool) {
	_, meetThreshold := pushLimitOrTopNForcibly(lp, pp)
	if meetThreshold {
		// previously, we set meetThreshold for pruning root task type but mpp task type. so:
		// 1: when one copTask exists, we will ignore root task type.
		// 2: when one copTask exists, another copTask should be cost compared with.
		// 3: mppTask is always in the cbo comparing.
		// 4: when none copTask exists, we will consider rootTask vs mppTask.
		// the following check priority logic is compatible with former pushLimitOrTopNForcibly prop pruning logic.
		_, isTopN := pp.(*physicalop.PhysicalTopN)
		if isTopN {
			if state.topNCopExist {
				if _, ok := childTasks[0].(*physicalop.RootTask); ok {
					return false
				}
				// peer cop task should compare the cost with each other.
				if _, ok := childTasks[0].(*physicalop.CopTask); ok {
					return true
				}
			} else {
				if _, ok := childTasks[0].(*physicalop.CopTask); ok {
					state.topNCopExist = true
					return true
				}
				// when we encounter rootTask type while still see no topNCopExist.
				// that means there is no copTask valid before, we will consider rootTask here.
				if _, ok := childTasks[0].(*physicalop.RootTask); ok {
					return true
				}
			}
			if _, ok := childTasks[0].(*physicalop.MppTask); ok {
				return true
			}
			// shouldn't be here
			return false
		}
		// limit case:
		if state.limitCopExist {
			if _, ok := childTasks[0].(*physicalop.RootTask); ok {
				return false
			}
			// peer cop task should compare the cost with each other.
			if _, ok := childTasks[0].(*physicalop.CopTask); ok {
				return true
			}
		} else {
			if _, ok := childTasks[0].(*physicalop.CopTask); ok {
				state.limitCopExist = true
				return true
			}
			// when we encounter rootTask type while still see no limitCopExist.
			// that means there is no copTask valid before, we will consider rootTask here.
			if _, ok := childTasks[0].(*physicalop.RootTask); ok {
				return true
			}
		}
		if _, ok := childTasks[0].(*physicalop.MppTask); ok {
			return true
		}
	}
	return false
}

// hash join has two types:
// one is hash join type: normal hash join, shuffle join, broadcast join
// another is the build side hint type: prefer left as build side, prefer right as build side
// the first one is used to control the join type, the second one is used to control the build side of hash join.
// the priority is:
// once the join type is set, we should respect them first, not this type are all ignored.
// after we see all the joins under this type, then we only consider the build side hints satisfied or not.
//
// for the priority among the hash join types, we will respect the join fine-grained hints first, then the normal hash join type,
// that is, the priority is: shuffle join / broadcast join > normal hash join.
func preferHashJoin(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	p, ok := lp.(*logicalop.LogicalJoin)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	forceLeftToBuild := ((p.PreferJoinType & h.PreferLeftAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferRightAsHJProbe) > 0)
	forceRightToBuild := ((p.PreferJoinType & h.PreferRightAsHJBuild) > 0) || ((p.PreferJoinType & h.PreferLeftAsHJProbe) > 0)
	if forceLeftToBuild && forceRightToBuild {
		// for build hint conflict, restore all of them
		forceLeftToBuild = false
		forceRightToBuild = false
	}
	physicalHashJoin, ok := physicPlan.(*physicalop.PhysicalHashJoin)
	if !ok {
		return false
	}
	// If the hint is set, we should prefer MPP shuffle join.
	preferShuffle := (p.PreferJoinType & h.PreferShuffleJoin) > 0
	preferBCJ := (p.PreferJoinType & h.PreferBCJoin) > 0
	if preferShuffle {
		if physicalHashJoin.StoreTp == kv.TiFlash && physicalHashJoin.MppShuffleJoin {
			// first: respect the shuffle join hint.
			// BCJ build side hint are handled in the enumeration phase.
			return true
		}
		return false
	}
	if preferBCJ {
		if physicalHashJoin.StoreTp == kv.TiFlash && !physicalHashJoin.MppShuffleJoin {
			// first: respect the broadcast join hint.
			// BCJ build side hint are handled in the enumeration phase.
			return true
		}
		return false
	}
	// Respect the join type and join side hints.
	if p.PreferJoinType&h.PreferHashJoin > 0 {
		// first: normal hash join hint are set.
		if forceLeftToBuild || forceRightToBuild {
			// second: respect the join side if join side hints are set.
			return (forceRightToBuild && physicalHashJoin.InnerChildIdx == 1) ||
				(forceLeftToBuild && physicalHashJoin.InnerChildIdx == 0)
		}
		// second: no join side hints are set, respect the join type is enough.
		return true
	}
	// no hash join type hint is set, we only need to respect the hash join side hints.
	return (forceRightToBuild && physicalHashJoin.InnerChildIdx == 1) ||
		(forceLeftToBuild && physicalHashJoin.InnerChildIdx == 0)
}

func preferMergeJoin(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	p, ok := lp.(*logicalop.LogicalJoin)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	_, ok = physicPlan.(*physicalop.PhysicalMergeJoin)
	return ok && p.PreferJoinType&h.PreferMergeJoin > 0
}

func preferIndexJoinFamily(lp base.LogicalPlan, physicPlan base.PhysicalPlan) (preferred bool) {
	p, ok := lp.(*logicalop.LogicalJoin)
	if !ok {
		return false
	}
	if physicPlan == nil {
		return false
	}
	if !p.PreferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return false // no force index join hints
	}
	innerSide, joinMethod, ok := getIndexJoinSideAndMethod(physicPlan)
	if !ok {
		return false
	}
	if (p.PreferAny(h.PreferLeftAsINLJInner) && innerSide == joinLeft && joinMethod == indexJoinMethod) ||
		(p.PreferAny(h.PreferRightAsINLJInner) && innerSide == joinRight && joinMethod == indexJoinMethod) ||
		(p.PreferAny(h.PreferLeftAsINLHJInner) && innerSide == joinLeft && joinMethod == indexHashJoinMethod) ||
		(p.PreferAny(h.PreferRightAsINLHJInner) && innerSide == joinRight && joinMethod == indexHashJoinMethod) ||
		(p.PreferAny(h.PreferLeftAsINLMJInner) && innerSide == joinLeft && joinMethod == indexMergeJoinMethod) ||
		(p.PreferAny(h.PreferRightAsINLMJInner) && innerSide == joinRight && joinMethod == indexMergeJoinMethod) {
		// valid physic for the hint
		return true
	}
	return false
}

// handleForceIndexJoinHints handles the force index join hints and returns all plans that can satisfy the hints.
func handleForceIndexJoinHints(p *logicalop.LogicalJoin, prop *property.PhysicalProperty, candidates []base.PhysicalPlan) (indexJoins []base.PhysicalPlan, canForced bool) {
	if !p.PreferAny(h.PreferRightAsINLJInner, h.PreferRightAsINLHJInner, h.PreferRightAsINLMJInner,
		h.PreferLeftAsINLJInner, h.PreferLeftAsINLHJInner, h.PreferLeftAsINLMJInner) {
		return candidates, false // no force index join hints
	}
	forced := make([]base.PhysicalPlan, 0, len(candidates))
	for _, candidate := range candidates {
		innerSide, joinMethod, ok := getIndexJoinSideAndMethod(candidate)
		if !ok {
			continue
		}
		if (p.PreferAny(h.PreferLeftAsINLJInner) && innerSide == joinLeft && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLJInner) && innerSide == joinRight && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferLeftAsINLHJInner) && innerSide == joinLeft && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLHJInner) && innerSide == joinRight && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferLeftAsINLMJInner) && innerSide == joinLeft && joinMethod == indexMergeJoinMethod) ||
			(p.PreferAny(h.PreferRightAsINLMJInner) && innerSide == joinRight && joinMethod == indexMergeJoinMethod) {
			forced = append(forced, candidate)
		}
	}

	if len(forced) > 0 {
		return forced, true
	}
	// Cannot find any valid index join plan with these force hints.
	// Print warning message if any hints cannot work.
	// If the required property is not empty, we will enforce it and try the hint again.
	// So we only need to generate warning message when the property is empty.
	if prop.IsSortItemEmpty() {
		var indexJoinTables, indexHashJoinTables, indexMergeJoinTables []h.HintedTable
		if p.HintInfo != nil {
			t := p.HintInfo.IndexJoin
			indexJoinTables, indexHashJoinTables, indexMergeJoinTables = t.INLJTables, t.INLHJTables, t.INLMJTables
		}
		var errMsg string
		switch {
		case p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner): // prefer index join
			errMsg = fmt.Sprintf("Optimizer Hint %s or %s is inapplicable", h.Restore2JoinHint(h.HintINLJ, indexJoinTables), h.Restore2JoinHint(h.TiDBIndexNestedLoopJoin, indexJoinTables))
		case p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner): // prefer index hash join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLHJ, indexHashJoinTables))
		case p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner): // prefer index merge join
			errMsg = fmt.Sprintf("Optimizer Hint %s is inapplicable", h.Restore2JoinHint(h.HintINLMJ, indexMergeJoinTables))
		}
		// Append inapplicable reason.
		if len(p.EqualConditions) == 0 {
			errMsg += " without column equal ON condition"
		}
		// Generate warning message to client.
		p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
	}
	return candidates, false
}
