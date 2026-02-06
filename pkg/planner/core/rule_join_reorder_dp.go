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
	"math/bits"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
)

type joinReorderDPSolver struct {
	*baseSingleGroupJoinOrderSolver
	newJoin func(lChild, rChild base.LogicalPlan, eqConds []*expression.ScalarFunction, otherConds, leftConds, rightConds []expression.Expression, joinType base.JoinType) base.LogicalPlan
}

type joinGroupEqEdge struct {
	nodeIDs []int
	edge    *expression.ScalarFunction
}

type joinGroupNonEqEdge struct {
	nodeIDs    []int
	nodeIDMask uint
	expr       expression.Expression
}

func (s *joinReorderDPSolver) solve(joinGroup []base.LogicalPlan) (base.LogicalPlan, error) {
	eqConds := expression.ScalarFuncs2Exprs(s.eqEdges)

	// Build the join graph. DP join reorder only uses equality edges as join connectors.
	adjacents := make([][]int, len(joinGroup))
	totalEqEdges := make([]joinGroupEqEdge, 0, len(eqConds))
	addEqEdge := func(node1, node2 int, edgeContent *expression.ScalarFunction) {
		totalEqEdges = append(totalEqEdges, joinGroupEqEdge{
			nodeIDs: []int{node1, node2},
			edge:    edgeContent,
		})
		adjacents[node1] = append(adjacents[node1], node2)
		adjacents[node2] = append(adjacents[node2], node1)
	}

	// Build Graph for join group.
	// After extractJoinGroup, eqEdges may contain substituted expressions where
	// derived columns have been replaced by their defining expressions.
	// We need to extract columns from both sides to find which nodes they belong to.
	for _, cond := range eqConds {
		sf := cond.(*expression.ScalarFunction)
		lArg := sf.GetArgs()[0]
		rArg := sf.GetArgs()[1]

		// Extract columns from the left argument (may be Column or ScalarFunction after substitution)
		lCols := expression.ExtractColumns(lArg)
		// Extract columns from the right argument
		rCols := expression.ExtractColumns(rArg)

		// With projection inlining restrictions, join equality edges should always reference
		// columns on both sides. If not, this edge can't be a join connector.
		intest.Assert(len(lCols) > 0 && len(rCols) > 0)
		if len(lCols) == 0 || len(rCols) == 0 {
			return nil, plannererrors.ErrInternal.GenWithStack("join reorder dp: eq edge has empty column list")
		}

		// Find the node index for left side - all columns should be from the same node.
		// After extractJoinGroup substitution, all columns are base table columns.
		lIdx, err := findNodeIndexForColumns(joinGroup, lCols)
		if err != nil {
			return nil, err
		}

		// Find the node index for right side.
		rIdx, err := findNodeIndexForColumns(joinGroup, rCols)
		if err != nil {
			return nil, err
		}

		intest.Assert(lIdx != rIdx)
		if lIdx == rIdx {
			// For join reorder, eqEdges are expected to be join connectors.
			return nil, plannererrors.ErrInternal.GenWithStack("join reorder dp: eq edge doesn't connect two join-group nodes")
		}

		addEqEdge(lIdx, rIdx, sf)
	}

	// DP join reorder only attaches predicates that span both sides of a join during the DP phase.
	totalNonEqEdges := make([]joinGroupNonEqEdge, 0, len(s.otherConds))
	for _, cond := range s.otherConds {
		cols := expression.ExtractColumns(cond)
		mask := uint(0)
		ids := make([]int, 0, len(cols))
		for _, col := range cols {
			// After extractJoinGroup substitution, all columns are base table columns
			idx, err := findNodeIndexInGroup(joinGroup, col)
			if err != nil {
				return nil, err
			}
			ids = append(ids, idx)
			mask |= 1 << uint(idx)
		}
		totalNonEqEdges = append(totalNonEqEdges, joinGroupNonEqEdge{
			nodeIDs:    ids,
			nodeIDMask: mask,
			expr:       cond,
		})
	}

	// Now derive stats/costs for the (possibly Selection-wrapped) join group nodes.
	joinGroupNodes, err := s.generateJoinOrderNode(joinGroup)
	if err != nil {
		return nil, err
	}
	s.curJoinGroup = joinGroupNodes

	visited := make([]bool, len(joinGroup))
	nodeID2VisitID := make([]int, len(joinGroup))
	joins := make([]base.LogicalPlan, 0, len(joinGroup))
	// BFS the tree.
	for i := range joinGroup {
		if visited[i] {
			continue
		}
		// Reset the visited ID map.
		for i := range nodeID2VisitID {
			nodeID2VisitID[i] = -1
		}
		visitID2NodeID := s.bfsGraph(i, visited, adjacents, nodeID2VisitID)
		nodeIDMask := uint(0)
		for _, nodeID := range visitID2NodeID {
			nodeIDMask |= 1 << uint(nodeID)
		}
		var subNonEqEdges []joinGroupNonEqEdge
		for i := len(totalNonEqEdges) - 1; i >= 0; i-- {
			// If this edge is not the subset of the current sub graph.
			if totalNonEqEdges[i].nodeIDMask&nodeIDMask != totalNonEqEdges[i].nodeIDMask {
				continue
			}
			newMask := uint(0)
			for _, nodeID := range totalNonEqEdges[i].nodeIDs {
				newMask |= 1 << uint(nodeID2VisitID[nodeID])
			}
			totalNonEqEdges[i].nodeIDMask = newMask
			subNonEqEdges = append(subNonEqEdges, totalNonEqEdges[i])
			totalNonEqEdges = slices.Delete(totalNonEqEdges, i, i+1)
		}
		// Do DP on each sub graph.
		join, err := s.dpGraph(visitID2NodeID, nodeID2VisitID, joinGroup, totalEqEdges, subNonEqEdges)
		if err != nil {
			return nil, err
		}
		joins = append(joins, join)
	}
	remainedOtherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	for _, edge := range totalNonEqEdges {
		remainedOtherConds = append(remainedOtherConds, edge.expr)
	}
	// Build bushy tree for cartesian joins.
	return s.makeBushyJoin(joins, remainedOtherConds), nil
}

// bfsGraph bfs a sub graph starting at startPos. And relabel its label for future use.
func (*joinReorderDPSolver) bfsGraph(startNode int, visited []bool, adjacents [][]int, nodeID2VisitID []int) []int {
	queue := []int{startNode}
	visited[startNode] = true
	var visitID2NodeID []int
	for len(queue) > 0 {
		curNodeID := queue[0]
		queue = queue[1:]
		nodeID2VisitID[curNodeID] = len(visitID2NodeID)
		visitID2NodeID = append(visitID2NodeID, curNodeID)
		for _, adjNodeID := range adjacents[curNodeID] {
			if visited[adjNodeID] {
				continue
			}
			queue = append(queue, adjNodeID)
			visited[adjNodeID] = true
		}
	}
	return visitID2NodeID
}

// dpGraph is the core part of this algorithm.
// It implements the traditional join reorder algorithm: DP by subset using the following formula:
//
//	bestPlan[S:set of node] = the best one among Join(bestPlan[S1:subset of S], bestPlan[S2: S/S1])
func (s *joinReorderDPSolver) dpGraph(visitID2NodeID, nodeID2VisitID []int, _ []base.LogicalPlan,
	totalEqEdges []joinGroupEqEdge, totalNonEqEdges []joinGroupNonEqEdge) (base.LogicalPlan, error) {
	nodeCnt := uint(len(visitID2NodeID))
	bestPlan := make([]*jrNode, 1<<nodeCnt)
	// bestPlan[s] is nil can be treated as bestCost[s] = +inf.
	for i := range nodeCnt {
		bestPlan[1<<i] = s.curJoinGroup[visitID2NodeID[i]]
	}
	// Enumerate the nodeBitmap from small to big, make sure that S1 must be enumerated before S2 if S1 belongs to S2.
	for nodeBitmap := uint(1); nodeBitmap < (1 << nodeCnt); nodeBitmap++ {
		if bits.OnesCount(nodeBitmap) == 1 {
			continue
		}
		// This loop can iterate all its subset.
		for sub := (nodeBitmap - 1) & nodeBitmap; sub > 0; sub = (sub - 1) & nodeBitmap {
			remain := nodeBitmap ^ sub
			if sub > remain {
				continue
			}
			// If this subset is not connected skip it.
			if bestPlan[sub] == nil || bestPlan[remain] == nil {
				continue
			}
			// Get the edge connecting the two parts.
			usedEdges, otherConds := s.nodesAreConnected(sub, remain, nodeID2VisitID, totalEqEdges, totalNonEqEdges)
			// Here we only check equal condition currently.
			if len(usedEdges) == 0 {
				continue
			}
			join, err := s.newJoinWithEdge(bestPlan[sub].p, bestPlan[remain].p, usedEdges, otherConds)
			if err != nil {
				return nil, err
			}
			curCost := s.calcJoinCumCost(join, bestPlan[sub], bestPlan[remain])
			if bestPlan[nodeBitmap] == nil {
				bestPlan[nodeBitmap] = &jrNode{
					p:       join,
					cumCost: curCost,
				}
			} else if bestPlan[nodeBitmap].cumCost > curCost {
				bestPlan[nodeBitmap].p = join
				bestPlan[nodeBitmap].cumCost = curCost
			}
		}
	}
	return bestPlan[(1<<nodeCnt)-1].p, nil
}

func (*joinReorderDPSolver) nodesAreConnected(leftMask, rightMask uint, oldPos2NewPos []int,
	totalEqEdges []joinGroupEqEdge, totalNonEqEdges []joinGroupNonEqEdge) ([]joinGroupEqEdge, []expression.Expression) {
	var usedEqEdges []joinGroupEqEdge
	for _, edge := range totalEqEdges {
		// If one of the two nodes are not in the current subgraph, skip it.
		if oldPos2NewPos[edge.nodeIDs[0]] < 0 || oldPos2NewPos[edge.nodeIDs[1]] < 0 {
			continue
		}
		lIdx := uint(oldPos2NewPos[edge.nodeIDs[0]])
		rIdx := uint(oldPos2NewPos[edge.nodeIDs[1]])
		if ((leftMask&(1<<lIdx)) > 0 && (rightMask&(1<<rIdx)) > 0) || ((leftMask&(1<<rIdx)) > 0 && (rightMask&(1<<lIdx)) > 0) {
			usedEqEdges = append(usedEqEdges, edge)
		}
	}
	otherConds := make([]expression.Expression, 0, len(totalNonEqEdges))
	for _, edge := range totalNonEqEdges {
		// If the result is false, means that the current group hasn't covered the columns involved in the expression.
		if edge.nodeIDMask&(leftMask|rightMask) != edge.nodeIDMask {
			continue
		}
		// Check whether this expression is only built from one side of the join.
		if edge.nodeIDMask&leftMask == 0 || edge.nodeIDMask&rightMask == 0 {
			continue
		}
		otherConds = append(otherConds, edge.expr)
	}
	return usedEqEdges, otherConds
}

func (s *joinReorderDPSolver) newJoinWithEdge(leftPlan, rightPlan base.LogicalPlan, edges []joinGroupEqEdge, otherConds []expression.Expression) (base.LogicalPlan, error) {
	eqConds := make([]*expression.ScalarFunction, 0, len(edges))
	var expr2Col map[string]*expression.Column
	for _, edge := range edges {
		lArg := edge.edge.GetArgs()[0]
		rArg := edge.edge.GetArgs()[1]

		// After extractJoinGroup substitution, arguments may be ScalarFunctions instead of simple Columns.
		// We align the arguments to (leftPlan, rightPlan) and let buildJoinEdge materialize expressions
		// into columns when needed.
		lExpr, rExpr, _, ok := alignJoinEdgeArgs(lArg, rArg, leftPlan.Schema(), rightPlan.Schema())
		if !ok {
			// Best-effort optimization: if projection inlining produced an edge we can't attribute to
			// left/right here, return an error so the caller can fallback safely.
			return nil, plannererrors.ErrInternal.GenWithStack("join reorder dp: eq edge doesn't connect left/right plans")
		}

		newEdge, newExpr2Col := s.buildJoinEdge(edge.edge, lExpr, rExpr, &leftPlan, &rightPlan)
		expr2Col = mergeMap(expr2Col, newExpr2Col)
		eqConds = append(eqConds, newEdge)
	}
	if len(expr2Col) > 0 && len(otherConds) > 0 {
		otherConds = substituteExprsWithColsInExprs(otherConds, expr2Col)
	}
	join := s.newJoin(leftPlan, rightPlan, eqConds, otherConds, nil, nil, base.InnerJoin)
	_, _, err := join.RecursiveDeriveStats(nil)
	return join, err
}

// Make cartesian join as bushy tree.
func (s *joinReorderDPSolver) makeBushyJoin(cartesianJoinGroup []base.LogicalPlan, otherConds []expression.Expression) base.LogicalPlan {
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup := make([]base.LogicalPlan, 0, len(cartesianJoinGroup))
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			// TODO:Since the other condition may involve more than two tables, e.g. t1.a = t2.b+t3.c.
			//  So We'll need a extra stage to deal with it.
			// Currently, we just add it when building cartesianJoinGroup.
			mergedSchema := expression.MergeSchema(cartesianJoinGroup[i].Schema(), cartesianJoinGroup[i+1].Schema())
			var usedOtherConds []expression.Expression
			otherConds, usedOtherConds = expression.FilterOutInPlace(otherConds, func(expr expression.Expression) bool {
				return expression.ExprFromSchema(expr, mergedSchema)
			})
			resultJoinGroup = append(resultJoinGroup, s.newJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1], nil, usedOtherConds, nil, nil, base.InnerJoin))
		}
		cartesianJoinGroup = resultJoinGroup
	}
	return cartesianJoinGroup[0]
}

func findNodeIndexInGroup(group []base.LogicalPlan, col *expression.Column) (int, error) {
	for i, plan := range group {
		if plan.Schema().Contains(col) {
			return i, nil
		}
	}
	return -1, plannererrors.ErrUnknownColumn.GenWithStackByArgs(col, "JOIN REORDER RULE")
}

// findNodeIndexForColumns finds the node index for a set of columns.
// All columns must be from the same node; returns error if they span multiple nodes.
// This function is called after extractJoinGroup has already substituted derived columns
// with their defining expressions, so all columns should be base table columns.
func findNodeIndexForColumns(group []base.LogicalPlan, cols []*expression.Column) (int, error) {
	if len(cols) == 0 {
		return -1, plannererrors.ErrUnknownColumn.GenWithStackByArgs("empty column list", "JOIN REORDER RULE")
	}

	// Find the node index for the first column
	firstIdx, err := findNodeIndexInGroup(group, cols[0])
	if err != nil {
		return -1, err
	}

	// Verify all other columns are from the same node
	for _, col := range cols[1:] {
		idx, err := findNodeIndexInGroup(group, col)
		if err != nil {
			return -1, err
		}
		if idx != firstIdx {
			// Columns span multiple nodes - this shouldn't happen for single-table expressions
			// (which is what canInlineProjection allows), but handle it gracefully.
			return -1, plannererrors.ErrUnknownColumn.GenWithStackByArgs(col, "JOIN REORDER RULE: columns span multiple nodes")
		}
	}

	return firstIdx, nil
}
