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
	"math/bits"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
)

type joinReorderDPSolver struct {
	ctx     sessionctx.Context
	newJoin func(lChild, rChild LogicalPlan, eqConds []*expression.ScalarFunction) LogicalPlan
}

type joinGroupEdge struct {
	nodeIDs []int
	edge    *expression.ScalarFunction
}

func (s *joinReorderDPSolver) solve(joinGroup []LogicalPlan, conds []expression.Expression) (LogicalPlan, error) {
	adjacents := make([][]int, len(joinGroup))
	totalEdges := make([]joinGroupEdge, 0, len(conds))
	addEdge := func(node1, node2 int, edgeContent *expression.ScalarFunction) {
		totalEdges = append(totalEdges, joinGroupEdge{
			nodeIDs: []int{node1, node2},
			edge:    edgeContent,
		})
		adjacents[node1] = append(adjacents[node1], node2)
		adjacents[node2] = append(adjacents[node2], node1)
	}
	// Build Graph for join group
	for _, cond := range conds {
		sf := cond.(*expression.ScalarFunction)
		lCol := sf.GetArgs()[0].(*expression.Column)
		rCol := sf.GetArgs()[1].(*expression.Column)
		lIdx, err := findNodeIndexInGroup(joinGroup, lCol)
		if err != nil {
			return nil, err
		}
		rIdx, err := findNodeIndexInGroup(joinGroup, rCol)
		if err != nil {
			return nil, err
		}
		addEdge(lIdx, rIdx, sf)
	}
	visited := make([]bool, len(joinGroup))
	nodeID2VisitID := make([]int, len(joinGroup))
	var joins []LogicalPlan
	// BFS the tree.
	for i := 0; i < len(joinGroup); i++ {
		if visited[i] {
			continue
		}
		visitID2NodeID := s.bfsGraph(i, visited, adjacents, nodeID2VisitID)
		// Do DP on each sub graph.
		join, err := s.dpGraph(visitID2NodeID, nodeID2VisitID, joinGroup, totalEdges)
		if err != nil {
			return nil, err
		}
		joins = append(joins, join)
	}
	// Build bushy tree for cartesian joins.
	return s.makeBushyJoin(joins), nil
}

// bfsGraph bfs a sub graph starting at startPos. And relabel its label for future use.
func (s *joinReorderDPSolver) bfsGraph(startNode int, visited []bool, adjacents [][]int, nodeID2VistID []int) []int {
	queue := []int{startNode}
	visited[startNode] = true
	var visitID2NodeID []int
	for len(queue) > 0 {
		curNodeID := queue[0]
		queue = queue[1:]
		nodeID2VistID[curNodeID] = len(visitID2NodeID)
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

func (s *joinReorderDPSolver) dpGraph(newPos2OldPos, oldPos2NewPos []int, joinGroup []LogicalPlan, totalEdges []joinGroupEdge) (LogicalPlan, error) {
	nodeCnt := uint(len(newPos2OldPos))
	bestPlan := make([]LogicalPlan, 1<<nodeCnt)
	bestCost := make([]int64, 1<<nodeCnt)
	// bestPlan[s] is nil can be treated as bestCost[s] = +inf.
	for i := uint(0); i < nodeCnt; i++ {
		bestPlan[1<<i] = joinGroup[newPos2OldPos[i]]
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
			usedEdges := s.nodesAreConnected(sub, remain, oldPos2NewPos, totalEdges)
			if len(usedEdges) == 0 {
				continue
			}
			join, err := s.newJoinWithEdge(bestPlan[sub], bestPlan[remain], usedEdges)
			if err != nil {
				return nil, err
			}
			if bestPlan[nodeBitmap] == nil || bestCost[nodeBitmap] > join.statsInfo().Count()+bestCost[remain]+bestCost[sub] {
				bestPlan[nodeBitmap] = join
				bestCost[nodeBitmap] = join.statsInfo().Count() + bestCost[remain] + bestCost[sub]
			}
		}
	}
	return bestPlan[(1<<nodeCnt)-1], nil
}

func (s *joinReorderDPSolver) nodesAreConnected(leftMask, rightMask uint, oldPos2NewPos []int, totalEdges []joinGroupEdge) []joinGroupEdge {
	var usedEdges []joinGroupEdge
	for _, edge := range totalEdges {
		lIdx := uint(oldPos2NewPos[edge.nodeIDs[0]])
		rIdx := uint(oldPos2NewPos[edge.nodeIDs[1]])
		if (leftMask&(1<<lIdx)) > 0 && (rightMask&(1<<rIdx)) > 0 {
			usedEdges = append(usedEdges, edge)
		} else if (leftMask&(1<<rIdx)) > 0 && (rightMask&(1<<lIdx)) > 0 {
			usedEdges = append(usedEdges, edge)
		}
	}
	return usedEdges
}

func (s *joinReorderDPSolver) newJoinWithEdge(leftPlan, rightPlan LogicalPlan, edges []joinGroupEdge) (LogicalPlan, error) {
	var eqConds []*expression.ScalarFunction
	for _, edge := range edges {
		lCol := edge.edge.GetArgs()[0].(*expression.Column)
		rCol := edge.edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) {
			eqConds = append(eqConds, edge.edge)
		} else {
			newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
			eqConds = append(eqConds, newSf)
		}
	}
	join := s.newJoin(leftPlan, rightPlan, eqConds)
	_, err := join.recursiveDeriveStats()
	return join, err
}

// Make cartesian join as bushy tree.
func (s *joinReorderDPSolver) makeBushyJoin(cartesianJoinGroup []LogicalPlan) LogicalPlan {
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup := make([]LogicalPlan, 0, len(cartesianJoinGroup))
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			resultJoinGroup = append(resultJoinGroup, s.newJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1], nil))
		}
		cartesianJoinGroup = resultJoinGroup
	}
	return cartesianJoinGroup[0]
}

func findNodeIndexInGroup(group []LogicalPlan, col *expression.Column) (int, error) {
	for i, plan := range group {
		if plan.Schema().Contains(col) {
			return i, nil
		}
	}
	return -1, ErrUnknownColumn.GenWithStackByArgs(col, "JOIN REORDER RULE")
}
