// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// GreedyJoinOrderSolver implements greedy join order optimization
type GreedyJoinOrderSolver struct {
	*BaseSingleGroupJoinOrderSolver
	leadingJoinGroup base.LogicalPlan
}

// NewGreedyJoinOrderSolver creates a new greedy join order solver
func NewGreedyJoinOrderSolver(ctx base.PlanContext, group *JoinGroupResult) JoinOrderSolver {
	base := NewBaseSingleGroupJoinOrderSolver(ctx, group)
	return &GreedyJoinOrderSolver{
		BaseSingleGroupJoinOrderSolver: base,
	}
}

// Solve implements JoinOrderSolver.Solve using greedy algorithm
func (s *GreedyJoinOrderSolver) Solve() (base.LogicalPlan, error) {
	// Initialize statistics for all nodes
	for _, node := range s.group.Group {
		_, _, err := node.RecursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
	}
	
	// Start with the leading join group if available
	var remainingNodes []base.LogicalPlan
	var currentJoin base.LogicalPlan
	
	if s.leadingJoinGroup != nil {
		currentJoin = s.leadingJoinGroup
		// Find remaining nodes that are not part of leading join
		for _, node := range s.group.Group {
			if !s.isNodeInPlan(node, currentJoin) {
				remainingNodes = append(remainingNodes, node)
			}
		}
	} else {
		// Start with the smallest table
		minCost := float64(-1)
		minIdx := -1
		for i, node := range s.group.Group {
			cost := s.BaseNodeCumCost(node)
			if minCost < 0 || cost < minCost {
				minCost = cost
				minIdx = i
			}
		}
		
		currentJoin = s.group.Group[minIdx]
		for i, node := range s.group.Group {
			if i != minIdx {
				remainingNodes = append(remainingNodes, node)
			}
		}
	}
	
	// Greedily add remaining nodes
	for len(remainingNodes) > 0 {
		bestIdx := -1
		bestCost := float64(-1)
		var bestUsedEdges []*expression.ScalarFunction
		var bestJoinType *JoinTypeWithExtMsg
		
		// Find the best node to join next
		for i, node := range remainingNodes {
			_, _, usedEdges, joinType := s.CheckConnection(currentJoin, node)
			if len(usedEdges) == 0 {
				continue // Skip if no connection
			}
			
			tempJoin, _ := s.MakeJoin(currentJoin, node, usedEdges, joinType)
			cost := s.BaseNodeCumCost(tempJoin)
			
			if bestIdx == -1 || cost < bestCost {
				bestIdx = i
				bestCost = cost
				bestUsedEdges = usedEdges
				bestJoinType = joinType
			}
		}
		
		if bestIdx == -1 {
			// No more connections found, create cartesian product with remaining nodes
			break
		}
		
		// Update current join and remove the selected node
		currentJoin, s.otherConds = s.MakeJoin(currentJoin, remainingNodes[bestIdx], bestUsedEdges, bestJoinType)
		remainingNodes = append(remainingNodes[:bestIdx], remainingNodes[bestIdx+1:]...)
	}
	
	// Handle remaining nodes with cartesian products
	if len(remainingNodes) > 0 {
		allNodes := append([]base.LogicalPlan{currentJoin}, remainingNodes...)
		currentJoin = s.makeBushyJoin(allNodes, s.otherConds)
	}
	
	return currentJoin, nil
}

// isNodeInPlan checks if a node is part of the given plan
func (s *GreedyJoinOrderSolver) isNodeInPlan(node, plan base.LogicalPlan) bool {
	if node == plan {
		return true
	}
	
	for _, child := range plan.Children() {
		if s.isNodeInPlan(node, child) {
			return true
		}
	}
	return false
}

// makeBushyJoin creates bushy tree for cartesian joins (same as DP solver)
func (s *GreedyJoinOrderSolver) makeBushyJoin(cartesianJoinGroup []base.LogicalPlan, otherConds []expression.Expression) base.LogicalPlan {
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup := make([]base.LogicalPlan, 0, len(cartesianJoinGroup))
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			
			mergedSchema := expression.MergeSchema(cartesianJoinGroup[i].Schema(), cartesianJoinGroup[i+1].Schema())
			var usedOtherConds []expression.Expression
			otherConds, usedOtherConds = expression.FilterOutInPlace(otherConds, func(expr expression.Expression) bool {
				return expression.ExprFromSchema(expr, mergedSchema)
			})
			resultJoinGroup = append(resultJoinGroup, s.newJoinWithEdges(cartesianJoinGroup[i], cartesianJoinGroup[i+1], nil, usedOtherConds, nil, nil, 0)) // 0 = InnerJoin
		}
		cartesianJoinGroup = resultJoinGroup
	}
	return cartesianJoinGroup[0]
} 