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
	"github.com/pingcap/tidb/parser/ast"
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
	for _, node := range joinNodePlans {
		_, err := node.p.recursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		cost := s.baseNodeCumCost(node.p)
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p:       node.p,
			cumCost: cost,
		})
		tracer.appendLogicalJoinCost(node.p, cost)
	}

	s.priorityMap = s.buildPriorityMap()

	// Sort plans by cost and join order
	sort.SliceStable(s.curJoinGroup, func(i, j int) bool {
		return s.curJoinGroup[i].cumCost < s.curJoinGroup[j].cumCost
	})

	var cartesianGroup []LogicalPlan
	//for len(s.curJoinGroup) > 0 {
	err := s.constructConnectedJoinTree(tracer)
	if err != nil {
		return nil, err
	}
	//}
	for i, _ := range s.curJoinGroup {
		cartesianGroup = append(cartesianGroup, s.curJoinGroup[i].p)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

// Build directed graph by driectedEdges
func (s *joinReorderGreedySolver) buildPriorityMap() map[*expression.ScalarFunction]map[*expression.ScalarFunction]struct{} {
	// 1.Init graph
	priorityMap := make(map[*expression.ScalarFunction]map[*expression.ScalarFunction]struct{})
	for _, edge := range s.directedEdges {
		if !edge.isImplicit {
			continue
		}

		if len(priorityMap[edge.toEqCond]) == 0 {
			priorityMap[edge.toEqCond] = make(map[*expression.ScalarFunction]struct{})
		}
		priorityMap[edge.toEqCond][edge.fromEqCond] = struct{}{}
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

func (s *joinReorderGreedySolver) constructConnectedJoinTree(tracer *joinReorderTrace) error {
	curIndex := 0
	for curIndex < len(s.curJoinGroup)-1 {
		curJoinTree := s.curJoinGroup[curIndex]
		s.curJoinGroup = append(s.curJoinGroup[0:curIndex], s.curJoinGroup[curIndex:]...)
		bestCost := math.MaxFloat64
		bestIdx := -1
		var finalRemainOthers []expression.Expression
		var bestJoin LogicalPlan
		var finalUsedEdges []*expression.ScalarFunction
		for i := curIndex + 1; i < len(s.curJoinGroup); i++ {
			node := s.curJoinGroup[i]
			newJoin, remainOthers, joinedEdges := s.checkConnectionAndMakeJoin(curJoinTree, node)
			if newJoin == nil {
				continue
			}
			_, err := newJoin.recursiveDeriveStats(nil)
			if err != nil {
				return err
			}
			curCost := s.calcJoinCumCost(newJoin, curJoinTree, node)
			tracer.appendLogicalJoinCost(newJoin, curCost)
			if bestCost > curCost {
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
				finalRemainOthers = remainOthers
				finalUsedEdges = joinedEdges
			}
		}
		// If we could find more join node, meaning that the sub connected graph have been totally explored.
		if bestJoin == nil {
			curIndex++
			continue
		}
		curJoinTree = &jrNode{
			p:       bestJoin,
			cumCost: bestCost,
		}
		s.curJoinGroup = append(s.curJoinGroup[:bestIdx], s.curJoinGroup[bestIdx+1:]...)
		s.curJoinGroup = append(s.curJoinGroup[:curIndex], s.curJoinGroup[curIndex+1:]...)
		s.insertIntoCurJoinGroup(curJoinTree)
		s.otherConds = finalRemainOthers
		// delete from priorityMap
		for _, edge := range finalUsedEdges {
			for _, edgeSet := range s.priorityMap {
				if _, ok := edgeSet[edge]; ok {
					curIndex = 0
					delete(edgeSet, edge)
				}
			}
		}
	}
	return nil
}

func (s *joinReorderGreedySolver) checkConnectionAndMakeJoin(leftNode, rightNode *jrNode) (LogicalPlan, []expression.Expression, []*expression.ScalarFunction) {
	var usedEdges []*expression.ScalarFunction
	var joinedEdges []*expression.ScalarFunction
	leftPlan := leftNode.p
	rightPlan := rightNode.p
	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
	joinType := InnerJoin
	for idx, edge := range s.eqEdges {
		lCol := edge.GetArgs()[0].(*expression.Column)
		rCol := edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) && rightPlan.Schema().Contains(rCol) {
			if len(s.priorityMap[edge]) > 0 {
				return nil, nil, nil
			}
			usedEdges = append(usedEdges, edge)
			joinedEdges = append(joinedEdges, edge)
			joinType = s.joinTypes[idx]
		} else if rightPlan.Schema().Contains(lCol) && leftPlan.Schema().Contains(rCol) {
			if len(s.priorityMap[edge]) > 0 {
				return nil, nil, nil
			}
			joinType = s.joinTypes[idx]
			if joinType != InnerJoin {
				rightPlan, leftPlan = leftPlan, rightPlan
				usedEdges = append(usedEdges, edge)
			} else {
				newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
				usedEdges = append(usedEdges, newSf)
			}
			joinedEdges = append(joinedEdges, edge)
		}
	}
	if len(usedEdges) == 0 {
		return nil, nil, nil
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
	return s.newJoinWithEdges(leftPlan, rightPlan, usedEdges, otherConds, leftConds, rightConds, joinType), remainOtherConds, joinedEdges
}

func (s *joinReorderGreedySolver) insertIntoCurJoinGroup(node *jrNode) {
	insertIdx := 0
	for insertIdx, _ = range s.curJoinGroup {
		if s.curJoinGroup[insertIdx].cumCost > node.cumCost {
			break
		}
	}
	s.curJoinGroup = append(s.curJoinGroup[:insertIdx], append([]*jrNode{node}, s.curJoinGroup[insertIdx:]...)...)
}
