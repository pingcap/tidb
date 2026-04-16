// Copyright 2018 PingCAP, Inc.
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
	"math"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type joinReorderGreedySolver struct {
	allInnerJoin bool // allInnerJoin indicates whether all joins in the current join group are inner joins.
	*baseSingleGroupJoinOrderSolver
}

// solve reorders the join nodes in the group based on a greedy algorithm.
//
// For each node having a join equal condition with the current join tree in
// the group, calculate the cumulative join cost of that node and the join
// tree, choose the node with the smallest cumulative cost to join with the
// current join tree.
//
// cumulative join cost = CumCount(lhs) + CumCount(rhs) + RowCount(join)
//
//	For base node, its CumCount equals to the sum of the count of its subtree.
//	See baseNodeCumCost for more details.
//
// TODO: this formula can be changed to real physical cost in future.
//
// For the nodes and join trees which don't have a join equal condition to
// connect them, we make a bushy join tree to do the cartesian joins finally.
func (s *joinReorderGreedySolver) solve(joinNodePlans []base.LogicalPlan) (base.LogicalPlan, error) {
	var err error
	s.curJoinGroup, err = s.generateJoinOrderNode(joinNodePlans)
	if err != nil {
		return nil, err
	}
	var leadingJoinNodes []*jrNode
	if s.leadingJoinGroup != nil {
		// We have a leading hint to let some tables join first. The result is stored in the s.leadingJoinGroup.
		// We generate jrNode separately for it.
		leadingJoinNodes, err = s.generateJoinOrderNode([]base.LogicalPlan{s.leadingJoinGroup})
		if err != nil {
			return nil, err
		}
	}
	// Sort plans by cost
	slices.SortStableFunc(s.curJoinGroup, func(i, j *jrNode) int {
		return cmp.Compare(i.cumCost, j.cumCost)
	})

	// joinNodeNum indicates the number of join nodes except leading join nodes in the current join group
	joinNodeNum := len(s.curJoinGroup)
	if leadingJoinNodes != nil {
		// The leadingJoinNodes should be the first element in the s.curJoinGroup.
		// So it can be joined first.
		leadingJoinNodes := append(leadingJoinNodes, s.curJoinGroup...)
		s.curJoinGroup = leadingJoinNodes
	}
	var cartesianGroup []base.LogicalPlan
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.constructConnectedJoinTree()
		if err != nil {
			return nil, err
		}
		if joinNodeNum > 0 && len(s.curJoinGroup) == joinNodeNum {
			// Getting here means that there is no join condition between the table used in the leading hint and other tables
			// For example: select /*+ leading(t3) */ * from t1 join t2 on t1.a=t2.a cross join t3
			// We can not let table t3 join first.
			// TODO(hawkingrei): we find the problem in the TestHint.
			// 	`select * from t1, t2, t3 union all select /*+ leading(t3, t2) */ * from t1, t2, t3 union all select * from t1, t2, t3`
			//  this sql should not return the warning. but It will not affect the result. so we will fix it as soon as possible.
			s.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check if the leading hint table has join conditions with other tables")
		}
		cartesianGroup = append(cartesianGroup, newNode.p)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

func (s *joinReorderGreedySolver) constructConnectedJoinTree() (*jrNode, error) {
	cartesianThreshold := s.ctx.GetSessionVars().CartesianJoinOrderThreshold
	// Mirror the advanced joinorder path: if enabled, pick the initial greedy
	// seed by cost before falling back to the historical smallest-node start.
	curJoinTree, seeded, err := s.seedConnectedJoinTreeByCost(cartesianThreshold)
	if err != nil {
		return nil, err
	}
	if !seeded {
		curJoinTree = s.curJoinGroup[0]
		s.curJoinGroup = s.curJoinGroup[1:]
	}
	for {
		bestCost := math.MaxFloat64
		bestIdx, whateverValidOneIdx, bestIsCartesian := -1, -1, false
		var finalRemainOthers, remainOthersOfWhateverValidOne []expression.Expression
		var bestJoin, whateverValidOne base.LogicalPlan
		var newJoin base.LogicalPlan
		var remainOthers []expression.Expression
		var isCartesian bool
		for i, node := range s.curJoinGroup {
			newJoin, remainOthers, isCartesian, err = s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
			if err != nil {
				return nil, err
			}
			if isCartesian {
				s.ctx.GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptCartesianJoinOrderThreshold)
			}
			if newJoin == nil || // can't yield a valid join
				(cartesianThreshold <= 0 && isCartesian) { // disable cartesian join
				continue
			}
			_, _, err := newJoin.RecursiveDeriveStats(nil)
			if err != nil {
				return nil, err
			}
			whateverValidOne = newJoin
			whateverValidOneIdx = i
			remainOthersOfWhateverValidOne = remainOthers
			curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)

			// Cartesian join is risky but skipping it brutally may lead to bad join orders, see #63290.
			// To trade-off, we use a ratio as penalty to control the preference.
			// Only select a cartesian join when cost(cartesian)*ratio < cost(non-cartesian).
			if betterLegacyGreedyCandidate(curCost, isCartesian, bestCost, bestIsCartesian, cartesianThreshold) {
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
				finalRemainOthers = remainOthers
				bestIsCartesian = isCartesian
			}
		}
		// If we could find more join node, meaning that the sub connected graph have been totally explored.
		if bestJoin == nil {
			if whateverValidOne == nil {
				break
			}
			// This branch is for the unexpected case.
			// We throw assertion in test env. And create a valid join to avoid wrong result in the production env.
			intest.Assert(false, "Join reorder should find one valid join but failed.")
			bestJoin = whateverValidOne
			bestCost = math.MaxFloat64
			bestIdx = whateverValidOneIdx
			finalRemainOthers = remainOthersOfWhateverValidOne
		}
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
			leafCnt: curJoinTree.leafCnt + s.curJoinGroup[bestIdx].leafCnt,
		}
		s.curJoinGroup = slices.Delete(s.curJoinGroup, bestIdx, bestIdx+1)
		s.otherConds = finalRemainOthers
	}
	return curJoinTree, nil
}

// seedConnectedJoinTreeByCost is the legacy-path counterpart of
// seedGreedyJoinComponentByCost in pkg/planner/core/joinorder. It only changes
// the first pair selection and keeps the rest of the legacy greedy expansion.
func (s *joinReorderGreedySolver) seedConnectedJoinTreeByCost(cartesianThreshold float64) (*jrNode, bool, error) {
	if !s.ctx.GetSessionVars().TiDBOptGreedyJoinSeedByCost || len(s.curJoinGroup) < 2 {
		return nil, false, nil
	}
	// LEADING only fixes the component that currently starts with the hinted
	// subtree. Later disconnected components should still be allowed to reseed.
	if s.leadingJoinGroup != nil && s.curJoinGroup[0].p == s.leadingJoinGroup {
		return nil, false, nil
	}

	componentIdxs := s.collectConnectedSeedComponentIndices()
	if len(componentIdxs) < 2 {
		return nil, false, nil
	}

	bestCost := math.MaxFloat64
	bestLeftIdx, bestRightIdx, bestLeafCnt := -1, -1, 0
	bestIsCartesian := false
	var bestJoin base.LogicalPlan
	var bestRemainOthers []expression.Expression
	var newJoin base.LogicalPlan
	var remainOthers []expression.Expression
	var isCartesian bool
	var err error

	for leftPos := range len(componentIdxs) - 1 {
		leftIdx := componentIdxs[leftPos]
		for rightPos := leftPos + 1; rightPos < len(componentIdxs); rightPos++ {
			rightIdx := componentIdxs[rightPos]
			newJoin, remainOthers, isCartesian, err = s.checkConnectionAndMakeJoin(s.curJoinGroup[leftIdx].p, s.curJoinGroup[rightIdx].p)
			if err != nil {
				return nil, false, err
			}
			if isCartesian {
				s.ctx.GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptCartesianJoinOrderThreshold)
			}
			// Keep the historical greedy attach loop's cartesian behavior, but do
			// not let the reseed heuristic start a connected component from a
			// pure cartesian pair.
			if newJoin == nil || isCartesian {
				continue
			}
			_, _, err := newJoin.RecursiveDeriveStats(nil)
			if err != nil {
				return nil, false, err
			}
			curCost := s.calcJoinCumCost(newJoin, s.curJoinGroup[leftIdx], s.curJoinGroup[rightIdx])
			if betterLegacyGreedyCandidate(curCost, isCartesian, bestCost, bestIsCartesian, cartesianThreshold) {
				bestCost = curCost
				bestJoin = newJoin
				bestLeftIdx = leftIdx
				bestRightIdx = rightIdx
				bestLeafCnt = s.curJoinGroup[leftIdx].leafCnt + s.curJoinGroup[rightIdx].leafCnt
				bestRemainOthers = remainOthers
				bestIsCartesian = isCartesian
			}
		}
	}
	if bestJoin == nil {
		return nil, false, nil
	}

	// Collapse the chosen leaf-leaf seed pair into one subtree and leave the
	// remaining nodes untouched for the normal greedy attach loop.
	remaining := make([]*jrNode, 0, len(s.curJoinGroup)-2)
	for idx, node := range s.curJoinGroup {
		if idx == bestLeftIdx || idx == bestRightIdx {
			continue
		}
		remaining = append(remaining, node)
	}
	s.curJoinGroup = remaining
	s.otherConds = bestRemainOthers
	return &jrNode{
		p:       bestJoin,
		cumCost: bestCost,
		leafCnt: bestLeafCnt,
	}, true, nil
}

func (s *joinReorderGreedySolver) collectConnectedSeedComponentIndices() []int {
	// The legacy path also reseeds only inside the component currently being
	// expanded. Treating a pure cartesian edge as connectivity here would let a
	// cheaper pair from a later disconnected component jump ahead.
	componentIdxs := []int{0}
	seen := make([]bool, len(s.curJoinGroup))
	seen[0] = true
	for head := 0; head < len(componentIdxs); head++ {
		leftIdx := componentIdxs[head]
		for rightIdx := range len(s.curJoinGroup) {
			if seen[rightIdx] || s.curJoinGroup[rightIdx].leafCnt != 1 {
				continue
			}
			if s.hasNonCartesianSeedConnection(s.curJoinGroup[leftIdx].p, s.curJoinGroup[rightIdx].p) {
				seen[rightIdx] = true
				componentIdxs = append(componentIdxs, rightIdx)
			}
		}
	}
	return componentIdxs
}

func (s *joinReorderGreedySolver) hasNonCartesianSeedConnection(leftPlan, rightPlan base.LogicalPlan) bool {
	return s.probeConnection(leftPlan, rightPlan).HasJoinCondition()
}

// betterLegacyGreedyCandidate preserves the existing cartesian penalty policy
// so the new seed heuristic compares candidates the same way as the old loop.
func betterLegacyGreedyCandidate(curCost float64, curIsCartesian bool, bestCost float64, bestIsCartesian bool, cartesianThreshold float64) bool {
	if !bestIsCartesian && curIsCartesian {
		return curCost*cartesianThreshold < bestCost
	}
	if bestIsCartesian && !curIsCartesian {
		return curCost < bestCost*cartesianThreshold
	}
	return curCost < bestCost
}

func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftPlan, rightPlan base.LogicalPlan) (base.LogicalPlan, []expression.Expression, bool, error) {
	probe := s.probeConnection(leftPlan, rightPlan)
	isCartesian := probe.IsCartesian()
	if isCartesian && // cartesian join
		(!s.allInnerJoin || // not all joins are inner joins
			s.ctx.GetSessionVars().CartesianJoinOrderThreshold <= 0) { // cartesian join is disabled
		// For outer joins like `t1 left join t2 left join t3`, we have to ensure t1 participates join before
		// t2 and t3, and cartesian join between t2 and t3 might lead to incorrect results.
		// For safety we don't allow cartesian outer join here.
		// For inner joins like `t1 join t2 join t3`, we can reorder them freely, so we allow cartesian join here.
		return nil, nil, false, nil
	}
	join, otherConds, err := s.buildJoinFromProbe(probe)
	if err != nil {
		return nil, nil, false, err
	}
	return join, otherConds, isCartesian, nil
}
