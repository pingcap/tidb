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
	"math"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

func tryToEnumerateIndexJoin(super base.LogicalPlan, prop *property.PhysicalProperty) []base.PhysicalPlan {
	_, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)
	// supportLeftOuter and supportRightOuter indicates whether this type of join
	// supports the left side or right side to be the outer side.
	var supportLeftOuter, supportRightOuter bool
	switch p.JoinType {
	case base.SemiJoin, base.AntiSemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin, base.LeftOuterJoin:
		supportLeftOuter = true
	case base.RightOuterJoin:
		supportRightOuter = true
	case base.InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}
	// according join type to enumerate index join with inner children's indexJoinProp.
	candidates := make([]base.PhysicalPlan, 0, 2)
	if supportLeftOuter {
		candidates = append(candidates, enumerateIndexJoinByOuterIdx(super, prop, 0)...)
	}
	if supportRightOuter {
		candidates = append(candidates, enumerateIndexJoinByOuterIdx(super, prop, 1)...)
	}
	// Pre-Handle hints and variables about index join, which try to detect the contradictory hint and variables
	// The priority is: force hints like TIDB_INLJ > filter hints like NO_INDEX_JOIN > variables and rec warns.
	stmtCtx := p.SCtx().GetSessionVars().StmtCtx
	if p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner) && p.PreferAny(h.PreferNoIndexJoin) {
		stmtCtx.SetHintWarning("Some INL_JOIN and NO_INDEX_JOIN hints conflict, NO_INDEX_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner) && p.PreferAny(h.PreferNoIndexHashJoin) {
		stmtCtx.SetHintWarning("Some INL_HASH_JOIN and NO_INDEX_HASH_JOIN hints conflict, NO_INDEX_HASH_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner) && p.PreferAny(h.PreferNoIndexMergeJoin) {
		stmtCtx.SetHintWarning("Some INL_MERGE_JOIN and NO_INDEX_MERGE_JOIN hints conflict, NO_INDEX_MERGE_JOIN may be ignored")
	}
	// previously we will think about force index join hints here, but we have to wait the inner plans to be a valid
	// physical one/ones. Because indexJoinProp may not be admitted by its inner patterns, so we innovatively move all
	// hint related handling to the findBestTask function when we see the entire inner physical-ized plan tree. See xxx
	// for details.
	//
	// handleFilterIndexJoinHints is trying to avoid generating index join or index hash join when no-index-join related
	// hint is specified in the query. So we can do it in physic enumeration phase here.
	return handleFilterIndexJoinHints(p, candidates)
}

// tryToGetIndexJoin returns all available index join plans, and the second returned value indicates whether this plan is enforced by hints.
func tryToGetIndexJoin(p *logicalop.LogicalJoin, prop *property.PhysicalProperty) (indexJoins []base.PhysicalPlan, canForced bool) {
	// supportLeftOuter and supportRightOuter indicates whether this type of join
	// supports the left side or right side to be the outer side.
	var supportLeftOuter, supportRightOuter bool
	switch p.JoinType {
	case base.SemiJoin, base.AntiSemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin, base.LeftOuterJoin:
		supportLeftOuter = true
	case base.RightOuterJoin:
		supportRightOuter = true
	case base.InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}
	candidates := make([]base.PhysicalPlan, 0, 2)
	if supportLeftOuter {
		candidates = append(candidates, getIndexJoinByOuterIdx(p, prop, 0)...)
	}
	if supportRightOuter {
		candidates = append(candidates, getIndexJoinByOuterIdx(p, prop, 1)...)
	}

	// Handle hints and variables about index join.
	// The priority is: force hints like TIDB_INLJ > filter hints like NO_INDEX_JOIN > variables.
	// Handle hints conflict first.
	stmtCtx := p.SCtx().GetSessionVars().StmtCtx
	if p.PreferAny(h.PreferLeftAsINLJInner, h.PreferRightAsINLJInner) && p.PreferAny(h.PreferNoIndexJoin) {
		stmtCtx.SetHintWarning("Some INL_JOIN and NO_INDEX_JOIN hints conflict, NO_INDEX_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLHJInner, h.PreferRightAsINLHJInner) && p.PreferAny(h.PreferNoIndexHashJoin) {
		stmtCtx.SetHintWarning("Some INL_HASH_JOIN and NO_INDEX_HASH_JOIN hints conflict, NO_INDEX_HASH_JOIN may be ignored")
	}
	if p.PreferAny(h.PreferLeftAsINLMJInner, h.PreferRightAsINLMJInner) && p.PreferAny(h.PreferNoIndexMergeJoin) {
		stmtCtx.SetHintWarning("Some INL_MERGE_JOIN and NO_INDEX_MERGE_JOIN hints conflict, NO_INDEX_MERGE_JOIN may be ignored")
	}

	candidates, canForced = handleForceIndexJoinHints(p, prop, candidates)
	if canForced {
		return candidates, canForced
	}
	candidates = handleFilterIndexJoinHints(p, candidates)
	// todo: if any variables banned it, why bother to generate it first?
	return filterIndexJoinBySessionVars(p.SCtx(), candidates), false
}

func enumerationContainIndexJoin(candidates [][]base.PhysicalPlan) bool {
	return slices.ContainsFunc(candidates, func(candidate []base.PhysicalPlan) bool {
		return slices.ContainsFunc(candidate, func(op base.PhysicalPlan) bool {
			_, _, ok := getIndexJoinSideAndMethod(op)
			return ok
		})
	})
}

// handleFilterIndexJoinHints is trying to avoid generating index join or index hash join when no-index-join related
// hint is specified in the query. So we can do it in physic enumeration phase.
func handleFilterIndexJoinHints(p *logicalop.LogicalJoin, candidates []base.PhysicalPlan) []base.PhysicalPlan {
	if !p.PreferAny(h.PreferNoIndexJoin, h.PreferNoIndexHashJoin, h.PreferNoIndexMergeJoin) {
		return candidates // no filter index join hints
	}
	filtered := make([]base.PhysicalPlan, 0, len(candidates))
	for _, candidate := range candidates {
		_, joinMethod, ok := getIndexJoinSideAndMethod(candidate)
		if !ok {
			continue
		}
		if (p.PreferAny(h.PreferNoIndexJoin) && joinMethod == indexJoinMethod) ||
			(p.PreferAny(h.PreferNoIndexHashJoin) && joinMethod == indexHashJoinMethod) ||
			(p.PreferAny(h.PreferNoIndexMergeJoin) && joinMethod == indexMergeJoinMethod) {
			continue
		}
		filtered = append(filtered, candidate)
	}
	return filtered
}


// it can generates hash join, index join and sort merge join.
// Firstly we check the hint, if hint is figured by user, we force to choose the corresponding physical plan.
// If the hint is not matched, it will get other candidates.
// If the hint is not figured, we will pick all candidates.
func exhaustPhysicalPlans4LogicalJoin(super base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	ge, p := base.GetGEAndLogicalOp[*logicalop.LogicalJoin](super)

	if !isJoinHintSupportedInMPPMode(p.PreferJoinType) {
		if hasMPPJoinHints(p.PreferJoinType) {
			// If there are MPP hints but has some conflicts join method hints, all the join hints are invalid.
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning("The MPP join hints are in conflict, and you can only specify join method hints that are currently supported by MPP mode now")
			p.PreferJoinType = 0
		} else {
			// If there are no MPP hints but has some conflicts join method hints, the MPP mode will be blocked.
			p.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced("MPP mode may be blocked because you have used hint to specify a join algorithm which is not supported by mpp now.")
			if prop.IsFlashProp() {
				return nil, false, nil
			}
		}
	}
	if prop.MPPPartitionTp == property.BroadcastType {
		return nil, false, nil
	}
	joins := make([]base.PhysicalPlan, 0, 8)
	// we lift the p.canPushToTiFlash check here, because we want to generate all the plans to be decided by the attachment layer.
	if p.SCtx().GetSessionVars().IsMPPAllowed() {
		// prefer hint should be handled in the attachment layer. because the enumerated mpp join may couldn't be built bottom-up.
		if hasMPPJoinHints(p.PreferJoinType) {
			// generate them all for later attachment prefer picking. cause underlying ds may not have tiFlash path.
			// even all mpp join is invalid, they can still resort to root joins as an alternative.
			joins = append(joins, tryToGetMppHashJoin(super, prop, true)...)
			joins = append(joins, tryToGetMppHashJoin(super, prop, false)...)
		} else {
			// join don't have a mpp join hints, only generate preferMppBCJ mpp joins.
			if preferMppBCJ(super) {
				joins = append(joins, tryToGetMppHashJoin(super, prop, true)...)
			} else {
				joins = append(joins, tryToGetMppHashJoin(super, prop, false)...)
			}
		}
	} else {
		hasMppHints := false
		var errMsg string
		if (p.PreferJoinType & h.PreferShuffleJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the shuffle_join() hint is invalid"
			hasMppHints = true
		}
		if (p.PreferJoinType & h.PreferBCJoin) > 0 {
			errMsg = "The join can not push down to the MPP side, the broadcast_join() hint is invalid"
			hasMppHints = true
		}
		if hasMppHints {
			p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(errMsg)
		}
	}
	if prop.IsFlashProp() {
		return joins, true, nil
	}

	if !p.IsNAAJ() {
		// naaj refuse merge join and index join.
		stats0, stats1, _, _ := getJoinChildStatsAndSchema(ge, p)
		mergeJoins := physicalop.GetMergeJoin(p, prop, p.Schema(), p.StatsInfo(), stats0, stats1)
		if (p.PreferJoinType&h.PreferMergeJoin) > 0 && len(mergeJoins) > 0 {
			return mergeJoins, true, nil
		}
		joins = append(joins, mergeJoins...)

		if p.SCtx().GetSessionVars().EnhanceIndexJoinBuildV2 {
			indexJoins := tryToEnumerateIndexJoin(super, prop)
			joins = append(joins, indexJoins...)

			failpoint.Inject("MockOnlyEnableIndexHashJoinV2", func(val failpoint.Value) {
				if val.(bool) && !p.SCtx().GetSessionVars().InRestrictedSQL {
					indexHashJoin := make([]base.PhysicalPlan, 0, len(indexJoins))
					for _, one := range indexJoins {
						if _, ok := one.(*physicalop.PhysicalIndexHashJoin); ok {
							indexHashJoin = append(indexHashJoin, one)
						}
					}
					failpoint.Return(indexHashJoin, true, nil)
				}
			})
		} else {
			indexJoins, forced := tryToGetIndexJoin(p, prop)
			if forced {
				return indexJoins, true, nil
			}
			joins = append(joins, indexJoins...)
		}
	}

	hashJoins, forced := getHashJoins(super, prop)
	if forced && len(hashJoins) > 0 {
		return hashJoins, true, nil
	}
	joins = append(joins, hashJoins...)

	if p.PreferJoinType > 0 {
		// If we reach here, it means we have a hint that doesn't work.
		// It might be affected by the required property, so we enforce
		// this property and try the hint again.
		return joins, false, nil
	}
	return joins, true, nil
}

func pushLimitOrTopNForcibly(p base.LogicalPlan, pp base.PhysicalPlan) (preferPushDown bool, meetThreshold bool) {
	switch lp := p.(type) {
	case *logicalop.LogicalTopN:
		preferPushDown = lp.PreferLimitToCop
		if _, isPhysicalLimit := pp.(*physicalop.PhysicalLimit); isPhysicalLimit {
			// For query using orderby + limit, the physicalop can be PhysicalLimit
			// when its corresponding logicalop is LogicalTopn.
			// And for PhysicalLimit, it's always better to let it pushdown to tikv.
			meetThreshold = true
		} else {
			meetThreshold = lp.Count+lp.Offset <= uint64(lp.SCtx().GetSessionVars().LimitPushDownThreshold)
		}
	case *logicalop.LogicalLimit:
		preferPushDown = lp.PreferLimitToCop
		// Always push Limit down in this case since it has no side effect
		meetThreshold = true
	default:
		return
	}

	// we remove the child subTree check, each logical operator only focus on themselves.
	// for current level, they prefer a push-down copTask.
	return
}

// GetHashJoin is public for cascades planner.
func GetHashJoin(ge base.GroupExpression, la *logicalop.LogicalApply, prop *property.PhysicalProperty) *physicalop.PhysicalHashJoin {
	return getHashJoin(ge, &la.LogicalJoin, prop, 1, false)
}

// exhaustPhysicalPlans4LogicalApply generates the physical plan for a logical apply.
func exhaustPhysicalPlans4LogicalApply(super base.LogicalPlan, prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	ge, la := base.GetGEAndLogicalOp[*logicalop.LogicalApply](super)
	_, _, schema0, _ := getJoinChildStatsAndSchema(ge, la)
	if !prop.AllColsFromSchema(schema0) || prop.IsFlashProp() { // for convenient, we don't pass through any prop
		la.SCtx().GetSessionVars().RaiseWarningWhenMPPEnforced(
			"MPP mode may be blocked because operator `Apply` is not supported now.")
		return nil, true, nil
	}
	if !prop.IsSortItemEmpty() && la.SCtx().GetSessionVars().EnableParallelApply {
		la.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("Parallel Apply rejects the possible order properties of its outer child currently"))
		return nil, true, nil
	}
	join := GetHashJoin(ge, la, prop)
	var columns = make([]*expression.Column, 0, len(la.CorCols))
	for _, colColumn := range la.CorCols {
		// fix the liner warning.
		tmp := colColumn
		columns = append(columns, &tmp.Column)
	}
	cacheHitRatio := 0.0
	if la.StatsInfo().RowCount != 0 {
		ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(la.SCtx(), columns, la.Schema(), la.StatsInfo())
		// for example, if there are 100 rows and the number of distinct values of these correlated columns
		// are 70, then we can assume 30 rows can hit the cache so the cache hit ratio is 1 - (70/100) = 0.3
		cacheHitRatio = 1 - (ndv / la.StatsInfo().RowCount)
	}

	var canUseCache bool
	if cacheHitRatio > 0.1 && la.SCtx().GetSessionVars().MemQuotaApplyCache > 0 {
		canUseCache = true
	} else {
		canUseCache = false
	}

	apply := physicalop.PhysicalApply{
		PhysicalHashJoin: *join,
		OuterSchema:      la.CorCols,
		CanUseCache:      canUseCache,
	}.Init(la.SCtx(),
		la.StatsInfo().ScaleByExpectCnt(la.SCtx().GetSessionVars(), prop.ExpectedCnt),
		la.QueryBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, SortItems: prop.SortItems, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: true},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, CTEProducerStatus: prop.CTEProducerStatus, NoCopPushDown: prop.NoCopPushDown})
	apply.SetSchema(la.Schema())
	return []base.PhysicalPlan{apply}, true, nil
}
