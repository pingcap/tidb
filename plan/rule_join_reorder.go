// Copyright 2016 PingCAP, Inc.
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

package plan

import (
	"sort"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	log "github.com/sirupsen/logrus"
)

// getCartesianJoinGroup collects all the inner join tables of a left deep join
// tree. The traversal of join tree is stopped and returns a nil group if:
// 1. reach a reordered join node, or:
// 2. reach a non-cartesian join node, or:
// 3. reach a join node which has a preferred join algorithm.
// 4. reach a straight join node.
//
// An example of left deep join tree is:
//
//    "cartesian join 1"
//      	|	\
//      	|	"right child 1"
//      	|
//    "cartesian join 2"
//      	|	\
//      	|	"right child 2"
//      	|
//    "cartesian join ..."
//      	|	\
//      	|	"right child ..."
//      	|
//    "cartesian join n"
//      	|	\
//      	|	"right child n"
//      	|
//    "left deep child"
//
// The result of getCartesianJoinGroup is:
// {"left deep child", "right child n", ..., "right child 2", "right child 1"}
func getCartesianJoinGroup(p *LogicalJoin) []LogicalPlan {
	if p.reordered || !p.cartesianJoin || p.preferJoinType > uint(0) || p.StraightJoin {
		return nil
	}

	lChild := p.children[0]
	rChild := p.children[1]

	lhsJoinTree, ok := lChild.(*LogicalJoin)
	if !ok {
		return []LogicalPlan{lChild, rChild}
	}

	lhsJoinGroup := getCartesianJoinGroup(lhsJoinTree)
	if lhsJoinGroup == nil {
		return nil
	}

	return append(lhsJoinGroup, rChild)
}

func findColumnIndexByGroup(groups []LogicalPlan, col *expression.Column) int {
	for i, plan := range groups {
		if plan.Schema().Contains(col) {
			return i
		}
	}
	log.Errorf("Unknown columns %s, position %d", col, col.UniqueID)
	return -1
}

type joinReOrderSolver struct {
	graph      []edgeList
	group      []LogicalPlan
	visited    []bool
	resultJoin LogicalPlan
	groupRank  []*rankInfo
	ctx        sessionctx.Context
}

type edgeList []*rankInfo

func (l edgeList) Len() int {
	return len(l)
}

func (l edgeList) Less(i, j int) bool {
	return l[i].rate < l[j].rate
}

func (l edgeList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type rankInfo struct {
	nodeID int
	rate   float64
}

func (e *joinReOrderSolver) Less(i, j int) bool {
	return e.groupRank[i].rate < e.groupRank[j].rate
}

func (e *joinReOrderSolver) Swap(i, j int) {
	e.groupRank[i], e.groupRank[j] = e.groupRank[j], e.groupRank[i]
}

func (e *joinReOrderSolver) Len() int {
	return len(e.groupRank)
}

// reorderJoin implements a simple join reorder algorithm. It will extract all the equal conditions and compose them to a graph.
// Then walk through the graph and pick the nodes connected by some edges to compose a join tree.
// We will pick the node with least result set as early as possible.
func (e *joinReOrderSolver) reorderJoin(group []LogicalPlan, conds []expression.Expression) {
	e.graph = make([]edgeList, len(group))
	e.group = group
	e.visited = make([]bool, len(group))
	e.resultJoin = nil
	e.groupRank = make([]*rankInfo, len(group))
	for i := 0; i < len(e.groupRank); i++ {
		e.groupRank[i] = &rankInfo{
			nodeID: i,
			rate:   1.0,
		}
	}
	for _, cond := range conds {
		if f, ok := cond.(*expression.ScalarFunction); ok {
			if f.FuncName.L == ast.EQ {
				lCol, lok := f.GetArgs()[0].(*expression.Column)
				rCol, rok := f.GetArgs()[1].(*expression.Column)
				if lok && rok {
					lID := findColumnIndexByGroup(group, lCol)
					rID := findColumnIndexByGroup(group, rCol)
					if lID != rID {
						e.graph[lID] = append(e.graph[lID], &rankInfo{nodeID: rID})
						e.graph[rID] = append(e.graph[rID], &rankInfo{nodeID: lID})
						continue
					}
				}
			}
			id := -1
			rate := 1.0
			cols := expression.ExtractColumns(f)
			for _, col := range cols {
				idx := findColumnIndexByGroup(group, col)
				if id == -1 {
					switch f.FuncName.L {
					case ast.EQ:
						rate *= 0.1
					case ast.LT, ast.LE, ast.GE, ast.GT:
						rate *= 0.3
					// TODO: Estimate it more precisely in future.
					default:
						rate *= 0.9
					}
					id = idx
				} else {
					id = -1
					break
				}
			}
			if id != -1 {
				e.groupRank[id].rate *= rate
			}
		}
	}
	for _, node := range e.graph {
		for _, edge := range node {
			edge.rate = e.groupRank[edge.nodeID].rate
		}
	}
	sort.Sort(e)
	for _, edge := range e.graph {
		sort.Sort(edge)
	}
	var cartesianJoinGroup []LogicalPlan
	for j := 0; j < len(e.groupRank); j++ {
		i := e.groupRank[j].nodeID
		if !e.visited[i] {
			e.resultJoin = e.group[i]
			e.walkGraphAndComposeJoin(i)
			cartesianJoinGroup = append(cartesianJoinGroup, e.resultJoin)
		}
	}
	e.makeBushyJoin(cartesianJoinGroup)
}

// Make cartesian join as bushy tree.
func (e *joinReOrderSolver) makeBushyJoin(cartesianJoinGroup []LogicalPlan) {
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup := make([]LogicalPlan, 0, len(cartesianJoinGroup))
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			resultJoinGroup = append(resultJoinGroup, e.newJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1]))
		}
		cartesianJoinGroup = resultJoinGroup
	}
	e.resultJoin = cartesianJoinGroup[0]
}

func (e *joinReOrderSolver) newJoin(lChild, rChild LogicalPlan) *LogicalJoin {
	join := LogicalJoin{
		JoinType:  InnerJoin,
		reordered: true,
	}.init(e.ctx)
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	return join
}

// walkGraph implements a dfs algorithm. Each time it picks a edge with lowest rate, which has been sorted before.
func (e *joinReOrderSolver) walkGraphAndComposeJoin(u int) {
	e.visited[u] = true
	for _, edge := range e.graph[u] {
		v := edge.nodeID
		if !e.visited[v] {
			e.resultJoin = e.newJoin(e.resultJoin, e.group[v])
			e.walkGraphAndComposeJoin(v)
		}
	}
}
