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
	"math/bits"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

// DPJoinOrderSolver implements dynamic programming join order optimization
type DPJoinOrderSolver struct {
	*BaseSingleGroupJoinOrderSolver
	curJoinGroup []*JRNode
	newJoin      func(lChild, rChild base.LogicalPlan, eqConds []*expression.ScalarFunction, otherConds, leftConds, rightConds []expression.Expression, joinType logicalop.JoinType) base.LogicalPlan
}

// JRNode represents a join reorder node
type JRNode struct {
	P       base.LogicalPlan
	CumCost float64
}

// JoinGroupEqEdge represents an equality edge in join group
type JoinGroupEqEdge struct {
	NodeIDs []int
	Edge    *expression.ScalarFunction
}

// JoinGroupNonEqEdge represents a non-equality edge in join group
type JoinGroupNonEqEdge struct {
	NodeIDs    []int
	NodeIDMask uint
	Expr       expression.Expression
}

// NewDPJoinOrderSolver creates a new DP join order solver
func NewDPJoinOrderSolver(ctx base.PlanContext, group *JoinGroupResult) JoinOrderSolver {
	base := NewBaseSingleGroupJoinOrderSolver(ctx, group)
	solver := &DPJoinOrderSolver{
		BaseSingleGroupJoinOrderSolver: base,
		curJoinGroup:                   make([]*JRNode, 0),
	}
	solver.newJoin = solver.newJoinWithEdges
	return solver
}

// Solve implements JoinOrderSolver.Solve
func (s *DPJoinOrderSolver) Solve() (base.LogicalPlan, error) {
	eqConds := expression.ScalarFuncs2Exprs(s.eqEdges)
	
	// Initialize join group nodes
	for _, node := range s.group.Group {
		_, _, err := node.RecursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		cost := s.BaseNodeCumCost(node)
		s.curJoinGroup = append(s.curJoinGroup, &JRNode{
			P:       node,
			CumCost: cost,
		})
	}
	
	// Build adjacency graph
	adjacents := make([][]int, len(s.curJoinGroup))
	totalEqEdges := make([]JoinGroupEqEdge, 0, len(eqConds))
	
	addEqEdge := func(node1, node2 int, edgeContent *expression.ScalarFunction) {
		totalEqEdges = append(totalEqEdges, JoinGroupEqEdge{
			NodeIDs: []int{node1, node2},
			Edge:    edgeContent,
		})
		adjacents[node1] = append(adjacents[node1], node2)
		adjacents[node2] = append(adjacents[node2], node1)
	}
	
	// Build graph for join group
	for _, cond := range eqConds {
		sf := cond.(*expression.ScalarFunction)
		lCol := sf.GetArgs()[0].(*expression.Column)
		rCol := sf.GetArgs()[1].(*expression.Column)
		lIdx, err := s.findNodeIndexInGroup(s.group.Group, lCol)
		if err != nil {
			return nil, err
		}
		rIdx, err := s.findNodeIndexInGroup(s.group.Group, rCol)
		if err != nil {
			return nil, err
		}
		addEqEdge(lIdx, rIdx, sf)
	}
	
	// Handle non-equality conditions
	totalNonEqEdges := make([]JoinGroupNonEqEdge, 0, len(s.otherConds))
	for _, cond := range s.otherConds {
		cols := expression.ExtractColumns(cond)
		mask := uint(0)
		ids := make([]int, 0, len(cols))
		for _, col := range cols {
			idx, err := s.findNodeIndexInGroup(s.group.Group, col)
			if err != nil {
				return nil, err
			}
			ids = append(ids, idx)
			mask |= 1 << uint(idx)
		}
		totalNonEqEdges = append(totalNonEqEdges, JoinGroupNonEqEdge{
			NodeIDs:    ids,
			NodeIDMask: mask,
			Expr:       cond,
		})
	}
	
	// Perform DP optimization
	visited := make([]bool, len(s.group.Group))
	nodeID2VisitID := make([]int, len(s.group.Group))
	joins := make([]base.LogicalPlan, 0, len(s.group.Group))
	
	// BFS the tree
	for i := range s.group.Group {
		if visited[i] {
			continue
		}
		visitID2NodeID := s.bfsGraph(i, visited, adjacents, nodeID2VisitID)
		nodeIDMask := uint(0)
		for _, nodeID := range visitID2NodeID {
			nodeIDMask |= 1 << uint(nodeID)
		}
		
		var subNonEqEdges []JoinGroupNonEqEdge
		for i := len(totalNonEqEdges) - 1; i >= 0; i-- {
			if totalNonEqEdges[i].NodeIDMask&nodeIDMask != totalNonEqEdges[i].NodeIDMask {
				continue
			}
			newMask := uint(0)
			for _, nodeID := range totalNonEqEdges[i].NodeIDs {
				newMask |= 1 << uint(nodeID2VisitID[nodeID])
			}
			totalNonEqEdges[i].NodeIDMask = newMask
			subNonEqEdges = append(subNonEqEdges, totalNonEqEdges[i])
			totalNonEqEdges = slices.Delete(totalNonEqEdges, i, i+1)
		}
		
		// Do DP on each sub graph
		join, err := s.dpGraph(visitID2NodeID, nodeID2VisitID, s.group.Group, totalEqEdges, subNonEqEdges)
		if err != nil {
			return nil, err
		}
		joins = append(joins, join)
	}
	
	// Build bushy tree for cartesian joins
	remainedOtherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	for _, edge := range totalNonEqEdges {
		remainedOtherConds = append(remainedOtherConds, edge.Expr)
	}
	
	return s.makeBushyJoin(joins, remainedOtherConds), nil
}

// findNodeIndexInGroup finds the index of a node containing the given column
func (s *DPJoinOrderSolver) findNodeIndexInGroup(group []base.LogicalPlan, col *expression.Column) (int, error) {
	for i, plan := range group {
		if plan.Schema().Contains(col) {
			return i, nil
		}
	}
	return -1, plannererrors.ErrUnknownColumn.GenWithStackByArgs(col.String(), "join reorder")
}

// bfsGraph performs BFS on the join graph
func (s *DPJoinOrderSolver) bfsGraph(startNode int, visited []bool, adjacents [][]int, nodeID2VisitID []int) []int {
	queue := []int{startNode}
	visited[startNode] = true
	var visitID2NodeID []int
	
	for len(queue) > 0 {
		curNodeID := queue[0]
		queue = queue[1:]
		nodeID2VisitID[curNodeID] = len(visitID2NodeID)
		visitID2NodeID = append(visitID2NodeID, curNodeID)
		
		for _, adjNodeID := range adjacents[curNodeID] {
			if !visited[adjNodeID] {
				visited[adjNodeID] = true
				queue = append(queue, adjNodeID)
			}
		}
	}
	return visitID2NodeID
}

// dpGraph performs dynamic programming on a connected subgraph
func (s *DPJoinOrderSolver) dpGraph(visitID2NodeID, nodeID2VisitID []int, joinGroup []base.LogicalPlan, totalEqEdges []JoinGroupEqEdge, totalNonEqEdges []JoinGroupNonEqEdge) (base.LogicalPlan, error) {
	nodeCnt := uint(len(visitID2NodeID))
	if nodeCnt == 1 {
		return joinGroup[visitID2NodeID[0]], nil
	}
	
	// DP table: dp[mask] represents the best plan for the subset represented by mask
	dp := make([]base.LogicalPlan, 1<<nodeCnt)
	
	// Initialize single nodes
	for i := uint(0); i < nodeCnt; i++ {
		dp[1<<i] = joinGroup[visitID2NodeID[i]]
	}
	
	// Fill DP table
	for mask := uint(1); mask < (1 << nodeCnt); mask++ {
		if bits.OnesCount(mask) == 1 {
			continue
		}
		
		// Try all possible splits
		for sub := mask; sub > 0; sub = (sub - 1) & mask {
			if sub == mask || dp[sub] == nil {
				continue
			}
			remain := mask ^ sub
			if dp[remain] == nil {
				continue
			}
			
			// Check if these two subsets can be joined
			usedEqEdges, otherConds := s.nodesAreConnected(sub, remain, nodeID2VisitID, totalEqEdges, totalNonEqEdges)
			if len(usedEqEdges) == 0 {
				continue
			}
			
			join, err := s.newJoinWithEdge(dp[sub], dp[remain], usedEqEdges, otherConds)
			if err != nil {
				return nil, err
			}
			
			curCost := s.calcJoinCumCost(join, &JRNode{P: dp[sub], CumCost: s.BaseNodeCumCost(dp[sub])}, 
				&JRNode{P: dp[remain], CumCost: s.BaseNodeCumCost(dp[remain])})
			
			if dp[mask] == nil || s.BaseNodeCumCost(dp[mask]) > curCost {
				dp[mask] = join
			}
		}
	}
	
	return dp[(1<<nodeCnt)-1], nil
}

// nodesAreConnected checks if two node sets are connected by edges
func (s *DPJoinOrderSolver) nodesAreConnected(leftMask, rightMask uint, oldPos2NewPos []int,
	totalEqEdges []JoinGroupEqEdge, totalNonEqEdges []JoinGroupNonEqEdge) ([]JoinGroupEqEdge, []expression.Expression) {
	
	var usedEqEdges []JoinGroupEqEdge
	for _, edge := range totalEqEdges {
		lIdx := uint(oldPos2NewPos[edge.NodeIDs[0]])
		rIdx := uint(oldPos2NewPos[edge.NodeIDs[1]])
		if ((leftMask&(1<<lIdx)) > 0 && (rightMask&(1<<rIdx)) > 0) || 
		   ((leftMask&(1<<rIdx)) > 0 && (rightMask&(1<<lIdx)) > 0) {
			usedEqEdges = append(usedEqEdges, edge)
		}
	}
	
	otherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	for _, edge := range totalNonEqEdges {
		if edge.NodeIDMask&(leftMask|rightMask) != edge.NodeIDMask {
			continue
		}
		if edge.NodeIDMask&leftMask == 0 || edge.NodeIDMask&rightMask == 0 {
			continue
		}
		otherConds = append(otherConds, edge.Expr)
	}
	
	return usedEqEdges, otherConds
}

// newJoinWithEdge creates a new join with the specified edges
func (s *DPJoinOrderSolver) newJoinWithEdge(leftPlan, rightPlan base.LogicalPlan, edges []JoinGroupEqEdge, otherConds []expression.Expression) (base.LogicalPlan, error) {
	var eqConds []*expression.ScalarFunction
	for _, edge := range edges {
		lCol := edge.Edge.GetArgs()[0].(*expression.Column)
		rCol := edge.Edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) {
			eqConds = append(eqConds, edge.Edge)
		} else {
			newSf := expression.NewFunctionInternal(s.ctx.GetExprCtx(), ast.EQ, edge.Edge.GetStaticType(), rCol, lCol).(*expression.ScalarFunction)
			eqConds = append(eqConds, newSf)
		}
	}
	join := s.newJoin(leftPlan, rightPlan, eqConds, otherConds, nil, nil, logicalop.InnerJoin)
	_, _, err := join.RecursiveDeriveStats(nil)
	return join, err
}

// makeBushyJoin creates bushy tree for cartesian joins
func (s *DPJoinOrderSolver) makeBushyJoin(cartesianJoinGroup []base.LogicalPlan, otherConds []expression.Expression) base.LogicalPlan {
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
			resultJoinGroup = append(resultJoinGroup, s.newJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1], nil, usedOtherConds, nil, nil, logicalop.InnerJoin))
		}
		cartesianJoinGroup = resultJoinGroup
	}
	return cartesianJoinGroup[0]
}

// calcJoinCumCost calculates the cumulative cost of a join
func (s *DPJoinOrderSolver) calcJoinCumCost(join base.LogicalPlan, lNode, rNode *JRNode) float64 {
	return join.StatsInfo().RowCount + lNode.CumCost + rNode.CumCost
} 