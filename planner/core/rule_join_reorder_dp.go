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

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
)

type joinReorderDPSolver struct {
	ctx     sessionctx.Context
	id2Node map[int]LogicalPlan
	newJoin func(lChild, rChild LogicalPlan, eqConds []*expression.ScalarFunction) LogicalPlan
}

type joinGroupEdge struct {
	nodeIDs []int
	edge    *expression.ScalarFunction
}

func (s *joinReorderDPSolver) solve(joinGroup []LogicalPlan, conds []expression.Expression) (LogicalPlan, error) {
	s.id2Node = make(map[int]LogicalPlan)
	edges := make([][]int, len(joinGroup))
	totalEdges := make([]joinGroupEdge, 0, len(conds))
	addEdge := func(node1, node2 int, edgeContent *expression.ScalarFunction) {
		totalEdges = append(totalEdges, joinGroupEdge{
			nodeIDs: []int{node1, node2},
			edge:    edgeContent,
		})
	}
	// Build Graph for join group
	for _, cond := range conds {
		sf := cond.(*expression.ScalarFunction)
		lCol := sf.GetArgs()[0].(*expression.Column)
		rCol := sf.GetArgs()[1].(*expression.Column)
		lIdx := findColumnIndexByGroup(joinGroup, lCol)
		rIdx := findColumnIndexByGroup(joinGroup, rCol)
		edges[lIdx] = append(edges[lIdx], rIdx)
		edges[rIdx] = append(edges[rIdx], lIdx)
		addEdge(lIdx, rIdx, sf)
	}
	visited := make([]bool, len(joinGroup))
	groupID2bfsID := make([]int, len(joinGroup))
	var joins []LogicalPlan
	// BFS the tree.
	for i := 0; i < len(joinGroup); i++ {
		if visited[i] {
			continue
		}
		newPos2OldPos := s.bfsGraph(i, visited, edges, groupID2bfsID)
		// Do DP on each sub graph.
		join, err := s.dpGraph(newPos2OldPos, groupID2bfsID, joinGroup, totalEdges)
		if err != nil {
			return nil, err
		}
		joins = append(joins, join)
	}
	// Build bushy tree for cartesian joins.
	return s.makeBushyJoin(joins), nil
}

// bfsGraph bfs a sub graph starting at startPos. And relabel its label for future use.
func (s *joinReorderDPSolver) bfsGraph(startPos int, visited []bool, edges [][]int, originalID2bfsID []int) []int {
	queue := []int{startPos}
	visited[startPos] = true
	var newPos2OldPos []int
	for len(queue) > 0 {
		now := queue[0]
		queue = queue[1:]
		originalID2bfsID[now] = len(newPos2OldPos)
		newPos2OldPos = append(newPos2OldPos, now)
		for _, next := range edges[now] {
			if visited[next] {
				continue
			}
			queue = append(queue, next)
			visited[next] = true
		}
	}
	return newPos2OldPos
}

func (s *joinReorderDPSolver) dpGraph(newPos2OldPos, oldPos2NewPos []int, joinGroup []LogicalPlan, totalEdges []joinGroupEdge) (LogicalPlan, error) {
	nodeCnt := len(newPos2OldPos)
	bestPlan := make([]LogicalPlan, 1<<uint(nodeCnt))
	bestCost := make([]int64, 1<<uint(nodeCnt))
	// bestPlan[s] is nil can be treated as bestCost[s] = +inf.
	for i := 0; i < nodeCnt; i++ {
		bestPlan[1<<uint(i)] = joinGroup[newPos2OldPos[i]]
	}
	// Enumerate the state from small to big, make sure that S1 must be enumerated before S2 if S1 belongs to S2.
	for state := uint(1); state < (1 << uint(nodeCnt)); state++ {
		if bits.OnesCount(state) == 1 {
			continue
		}
		// This loop can iterate all its subset.
		for sub := (state - 1) & state; sub > 0; sub = (sub - 1) & state {
			remain := state ^ sub
			if sub > remain {
				continue
			}
			// If this subset is not connected skip it.
			if bestPlan[sub] == nil || bestPlan[remain] == nil {
				continue
			}
			// Get the edge connecting the two parts.
			usedEdges := s.nodesAreConnected(sub, remain, oldPos2NewPos, newPos2OldPos, totalEdges)
			if len(usedEdges) == 0 {
				continue
			}
			join, err := s.newJoinWithEdge(bestPlan[sub], bestPlan[remain], usedEdges)
			if err != nil {
				return nil, err
			}
			if bestPlan[state] == nil || bestCost[state] > join.statsInfo().Count()+bestCost[remain]+bestCost[sub] {
				bestPlan[state] = join
				bestCost[state] = join.statsInfo().Count() + bestCost[remain] + bestCost[sub]
			}
		}
	}
	return bestPlan[(1<<uint(nodeCnt))-1], nil
}

func (s *joinReorderDPSolver) nodesAreConnected(leftMask, rightMask uint, oldPos2NewPos, newPos2OldPos []int, totalEdges []joinGroupEdge) []joinGroupEdge {
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
	_, err := join.deriveStats()
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

func (s *joinReorderDPSolver) newJoinWithCond(lChild, rChild LogicalPlan, eqConds []*expression.ScalarFunction) LogicalPlan {
	join := LogicalJoin{
		JoinType:  InnerJoin,
		reordered: true,
	}.init(s.ctx)
	join.EqualConditions = eqConds
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	return join
}
