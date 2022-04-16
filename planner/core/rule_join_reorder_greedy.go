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
	"math"
	"sort"

	"github.com/pingcap/tidb/expression"
)

type joinReorderGreedySolver struct {
	*baseSingleGroupJoinOrderSolver
	eqEdges   []*expression.ScalarFunction
	joinTypes []JoinType
	// Maintain the order for plans of the outerJoin. [a,b] indicate a must join before b
	directedEdges []directedEdge
}

// solve reorders the join nodes in the group based on a greedy algorithm.
//
// For each node having a join equal condition with the current join tree in
// the group, calculate the cumulative join cost of that node and the join
// tree, choose the node with the smallest cumulative cost to join with the
// current join tree.
//
// cumulative join cost = CumCount(lhs) + CumCount(rhs) + RowCount(join)
//   For base node, its CumCount equals to the sum of the count of its subtree.
//   See baseNodeCumCost for more details.
// TODO: this formula can be changed to real physical cost in future.
//
// For the nodes and join trees which don't have a join equal condition to
// connect them, we make a bushy join tree to do the cartesian joins finally.
func (s *joinReorderGreedySolver) solve(joinNodePlans []*joinNode, tracer *joinReorderTrace) (LogicalPlan, error) {
	joinNodeCount := 0
	for _, node := range joinNodePlans {
		_, err := node.p.recursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		cost := s.baseNodeCumCost(node.p)
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			id:      joinNodeCount,
			p:       node.p,
			cumCost: cost,
		})
		node.id = joinNodeCount
		joinNodeCount++
		tracer.appendLogicalJoinCost(node.p, cost)
	}

	s.priorityMap = s.buildPriorityMap(joinNodeCount)

	// Sort plans by cost and join order
	sort.SliceStable(s.curJoinGroup, func(i, j int) bool {
		return s.curJoinGroup[i].cumCost < s.curJoinGroup[j].cumCost
	})

	var cartesianGroup []LogicalPlan
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.constructConnectedJoinTree(tracer)
		if err != nil {
			return nil, err
		}
		cartesianGroup = append(cartesianGroup, newNode.p)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

// Build directed graph by driectedEdges
func (s *joinReorderGreedySolver) buildPriorityMap(joinNodeCount int) [][]byte {
	// 1.init graph
	priorityMap := make([][]byte, joinNodeCount)
	for _, edge := range s.directedEdges {
		if !edge.isImplicit {
			continue
		}
		nodeBitSet := priorityMap[edge.from.id]
		if len(nodeBitSet) == 0 {
			nodeBitSet = make([]byte, (joinNodeCount+7)>>3)
			priorityMap[edge.from.id] = nodeBitSet
		}
		rightOffset := byte(1 << (7 & edge.to.id))
		nodeBitSet[edge.to.id>>3] |= rightOffset
	}

	// todo 2.
	for isPropagated := true; isPropagated; {
		isPropagated = false
		for idx, nodeBitSet := range priorityMap {
			for k1 := range nodeBitSet {
				for i := 0; i < 8; i++ {
					mask := nodeBitSet[k1] & (1 << i)
					if mask > 0 {
						isChange := s.merge(nodeBitSet, priorityMap[k1<<3+i])
						priorityMap[idx] = nodeBitSet
						if isChange {
							isPropagated = true
						}
					}
				}
			}
		}
	}
	return priorityMap
}

func (s *joinReorderGreedySolver) merge(dest []byte, src []byte) (isChange bool) {
	isChange = false
	for i := range src {
		if src[i] > 0 {
			tmp := dest[i] | src[i]
			if tmp != src[i] {
				isChange = true
			}
			dest[i] = dest[i] | src[i]
		}
	}
	return
}

func (s *joinReorderGreedySolver) constructConnectedJoinTree(tracer *joinReorderTrace) (*jrNode, error) {
	// We need the node with the least cost and is the starting node
	// of the directGraph built using the nodes in curJoinGroup
	startIdx := 0
	for i := range s.curJoinGroup {
		found := true
		for j := range s.curJoinGroup {
			if s.comparePriority(s.curJoinGroup[j].id, s.curJoinGroup[i].id) > 0 {
				found = false
			}
		}
		if found {
			startIdx = i
			break
		}
	}

	curJoinTree := s.curJoinGroup[startIdx]
	s.curJoinGroup = append(s.curJoinGroup[0:startIdx], s.curJoinGroup[startIdx+1:len(s.curJoinGroup)]...)
	for {
		bestCost := math.MaxFloat64
		bestIdx := -1
		var finalRemainOthers []expression.Expression
		var bestJoin LogicalPlan
		for i := range s.curJoinGroup {
			node := s.curJoinGroup[i]
			newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree, node)
			if newJoin == nil {
				continue
			}
			_, err := newJoin.recursiveDeriveStats(nil)
			if err != nil {
				return nil, err
			}
			curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)
			tracer.appendLogicalJoinCost(newJoin, curCost)
			if bestCost > curCost {
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
				finalRemainOthers = remainOthers
			}
		}
		// If we could find more join node, meaning that the sub connected graph have been totally explored.
		if bestJoin == nil {
			break
		}
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
		}
		s.curJoinGroup = append(s.curJoinGroup[:bestIdx], s.curJoinGroup[bestIdx+1:]...)
		s.otherConds = finalRemainOthers
	}
	return curJoinTree, nil
}

func (s *joinReorderGreedySolver) comparePriority(joinNodeId1, joinNodeId2 int) int {
	if len(s.priorityMap) == 0 {
		return 0
	}
	if bitmap := s.priorityMap[joinNodeId1]; len(bitmap) > 0 && bitmap[joinNodeId2>>3]&(1<<(7&joinNodeId2)) > 0 {
		return 1
	} else if bitmap = s.priorityMap[joinNodeId2]; len(bitmap) > 0 && bitmap[joinNodeId1>>3]&(1<<(7&joinNodeId1)) > 0 {
		return -1
	}
	return 0
}

func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftNode, rightNode *jrNode) (LogicalPlan, []expression.Expression) {
	for i := range s.curJoinGroup {
		if s.comparePriority(s.curJoinGroup[i].id, rightNode.id) > 0 {
			return nil, nil
		}
	}
	var usedEdges []*expression.ScalarFunction
	leftPlan := leftNode.p
	rightPlan := rightNode.p
	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
	joinType := InnerJoin
	for idx, edge := range s.eqEdges {
		lCol := edge.GetArgs()[0].(*expression.Column)
		rCol := edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) && rightPlan.Schema().Contains(rCol) {
			usedEdges = append(usedEdges, edge)
			joinType = s.joinTypes[idx]
		} else if rightPlan.Schema().Contains(lCol) && leftPlan.Schema().Contains(rCol) {
			usedEdges = append(usedEdges, edge)
			joinType = s.joinTypes[idx]
			rightPlan, leftPlan = leftPlan, rightPlan
		}

	}
	if len(usedEdges) == 0 {
		return nil, nil
	}
	var otherConds []expression.Expression
	var leftConds []expression.Expression
	var rightConds []expression.Expression
	mergedSchema := expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema())

	remainOtherConds, leftConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, leftPlan.Schema()) && !expression.ExprFromSchema(expr, rightPlan.Schema())
	})
	remainOtherConds, rightConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, rightPlan.Schema()) && !expression.ExprFromSchema(expr, leftPlan.Schema())
	})
	remainOtherConds, otherConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, mergedSchema)
	})
	return s.newJoinWithEdges(leftPlan, rightPlan, usedEdges, otherConds, leftConds, rightConds, joinType), remainOtherConds
}
