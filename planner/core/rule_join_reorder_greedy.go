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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"math"
	"sort"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
)

type joinReorderGreedySolver struct {
	*baseSingleGroupJoinOrderSolver
	eqEdges   []*expression.ScalarFunction
	joinTypes []JoinType
	// Maintain the order for plans of the outerJoin. [a,b] indicate a must join before b
	directedEdges [][]LogicalPlan
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
func (s *joinReorderGreedySolver) solve(joinNodePlans []LogicalPlan) (LogicalPlan, error) {
	s.directMap = make(map[LogicalPlan]map[LogicalPlan]struct{}, len(s.directedEdges))
	for _, node := range joinNodePlans {
		_, err := node.recursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p:       node,
			cumCost: s.baseNodeCumCost(node),
		})
	}

	for _, edge := range s.directedEdges {
		nodeSet := s.directMap[edge[0]]
		if nodeSet == nil {
			nodeSet = make(map[LogicalPlan]struct{})
			s.directMap[edge[0]] = nodeSet
		}
		nodeSet[edge[1]] = struct{}{}
	}

	isChanged := true
	for isChanged {
		isChanged = false
		for _, set := range s.directMap {
			oldSize := len(set)
			newSet := make(map[LogicalPlan]struct{})
			for k1 := range set {
				for k2 := range s.directMap[k1] {
					newSet[k2] = struct{}{}
				}
			}

			for k2 := range newSet {
				set[k2] = struct{}{}
			}
			newSize := len(set)
			if oldSize != newSize {
				isChanged = true
			}
		}
	}

	// First sort plans by cost
	sort.SliceStable(s.curJoinGroup, func(i, j int) bool {
		return s.curJoinGroup[i].cumCost < s.curJoinGroup[j].cumCost
	})

	// Then adjust the order for the plans according to join order that maintained in the directMap.
	for key, set := range s.directMap {
		keyIdx := -1
		lowerStart := -1
		for idx, node := range s.curJoinGroup {
			if key == node.p {
				keyIdx = idx
			}
			if _, ok := set[node.p]; ok && lowerStart < 0 {
				lowerStart = idx
			}
		}
		if keyIdx > lowerStart {
			selectNode := s.curJoinGroup[keyIdx]
			s.curJoinGroup = append(s.curJoinGroup[0:keyIdx], s.curJoinGroup[keyIdx+1:]...)
			s.curJoinGroup = append(s.curJoinGroup[0:lowerStart], append([]*jrNode{selectNode}, s.curJoinGroup[lowerStart:]...)...)
		}
	}

	var cartesianGroup []LogicalPlan
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.constructConnectedJoinTree()
		if err != nil {
			return nil, err
		}
		cartesianGroup = append(cartesianGroup, newNode.p)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

func (s *joinReorderGreedySolver) constructConnectedJoinTree() (*jrNode, error) {
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
	// Check whether all of these plans that must be joining before left node or right have already are joined .
	// If not then return directly.
	for _, node := range []LogicalPlan{leftNode, rightNode} {
		for _, remainPlan := range s.remainJoinGroup {
			if _, ok := s.directMap[remainPlan.p][node]; ok {
				return nil, nil
			}
		}
	}

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
			newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
			usedEdges = append(usedEdges, newSf)
			joinType = s.joinTypes[idx]
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
