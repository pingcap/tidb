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
		cost := s.baseNodeCumCost(node)
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			id:      joinNodeCount,
			p:       node.p,
			cumCost: cost,
		})
		node.id = joinNodeCount
		joinNodeCount++
		tracer.appendLogicalJoinCost(node, cost)
	}

	s.directGraph = make([][]byte, joinNodeCount)
	for _, edge := range s.directedEdges {
		if !edge.isImplicit && edge.joinType == InnerJoin {
			continue
		}
		nodeBitSet := s.directGraph[edge.left.id]
		if nodeBitSet == nil {
			nodeBitSet = make([]byte, (joinNodeCount+7)>>3)
			s.directGraph[edge.left.id] = nodeBitSet
		}
		rightOffset := byte(1 << (7 & edge.right.id))
		nodeBitSet[edge.right.id>>3] |= rightOffset
	}

	for isPropagated := true; isPropagated; {
		isPropagated = false
		for idx, nodeBitSet := range s.directGraph {
			baseV := 0
			for k1 := range nodeBitSet {
				for i := 0; i < 8; i++ {
					mask := nodeBitSet[k1] & (1 << i)
					if mask > 0 {
						isChange := s.merge(nodeBitSet, s.directGraph[k1<<3+i])
						s.directGraph[idx] = nodeBitSet
						if isChange {
							isPropagated = true
						}
					}
				}
				baseV += 8
			}
		}
	}

	// Sort plans by cost and join order
	sort.SliceStable(s.curJoinGroup, func(i, j int) bool {
		joinGroup1 := s.curJoinGroup[i]
		joinGroup2 := s.curJoinGroup[j]
		if s.directGraph[joinGroup1.id] != nil && ((s.directGraph[joinGroup1.id][joinGroup2.id>>3] & (1 << byte(7&joinGroup2.id))) > 0) {
			return true
		}

		if s.directGraph[joinGroup2.id] != nil && ((s.directGraph[joinGroup2.id][joinGroup1.id>>3] & (1 << byte(7&joinGroup1.id))) > 0) {
			return false
		}

		return joinGroup1.cumCost < joinGroup2.cumCost
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

func (s *joinReorderGreedySolver) merge(dest []byte, src []byte) (isChange bool) {
	isChange = false
	for i := range src {
		if src[i] > 0 {
			if dest[i]&src[i] == src[i] {
				isChange = true
			}
			dest[i] = dest[i] | src[i]
		}
	}
	return
}

func (s *joinReorderGreedySolver) constructConnectedJoinTree(tracer *joinReorderTrace) (*jrNode, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
	s.remainJoinGroup = s.remainJoinGroup[:0]
	s.remainJoinGroup = append(s.remainJoinGroup, s.curJoinGroup...)
	for {
		bestCost := math.MaxFloat64
		bestIdx := -1
		var finalRemainOthers []expression.Expression
		var bestJoin LogicalPlan
		for i, node := range s.curJoinGroup {
			newJoin, remainOthers := s.checkConnectionAndMakeJoin(curJoinTree.p, node.p)
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
		s.remainJoinGroup = append(s.remainJoinGroup[0:bestIdx], s.remainJoinGroup[bestIdx+1:]...)
	}
	return curJoinTree, nil
}

func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftNode, rightNode LogicalPlan) (LogicalPlan, []expression.Expression) {
	var usedEdges []*expression.ScalarFunction

	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
	joinType := InnerJoin
	for idx, edge := range s.eqEdges {
		lCol := edge.GetArgs()[0].(*expression.Column)
		rCol := edge.GetArgs()[1].(*expression.Column)
		if leftNode.Schema().Contains(lCol) && rightNode.Schema().Contains(rCol) {
			usedEdges = append(usedEdges, edge)
			joinType = s.joinTypes[idx]
		} else if rightNode.Schema().Contains(lCol) && leftNode.Schema().Contains(rCol) {
			usedEdges = append(usedEdges, edge)
			joinType = s.joinTypes[idx]
			rightNode, leftNode = leftNode, rightNode
		}

	}
	if len(usedEdges) == 0 {
		return nil, nil
	}
	var otherConds []expression.Expression
	var leftConds []expression.Expression
	var rightConds []expression.Expression
	mergedSchema := expression.MergeSchema(leftNode.Schema(), rightNode.Schema())

	remainOtherConds, leftConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, leftNode.Schema()) && !expression.ExprFromSchema(expr, rightNode.Schema())
	})
	remainOtherConds, rightConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, rightNode.Schema()) && !expression.ExprFromSchema(expr, leftNode.Schema())
	})
	remainOtherConds, otherConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, mergedSchema)
	})
	return s.newJoinWithEdges(leftNode, rightNode, usedEdges, otherConds, leftConds, rightConds, joinType), remainOtherConds
}
