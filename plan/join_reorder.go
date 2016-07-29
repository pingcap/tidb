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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
)

func tryToGetJoinGroups(j *Join) ([]LogicalPlan, bool) {
	if j.reordered || !j.crossJoin {
		return nil, false
	}
	lChild := j.GetChildByIndex(0).(LogicalPlan)
	rChild := j.GetChildByIndex(1).(LogicalPlan)
	if nj, ok := lChild.(*Join); ok {
		plans, valid := tryToGetJoinGroups(nj)
		return append(plans, rChild), valid
	}
	return []LogicalPlan{lChild, rChild}, true
}

func findColumnIndexByGroup(groups []LogicalPlan, col *expression.Column) int {
	for i, plan := range groups {
		idx := plan.GetSchema().GetIndex(col)
		if idx != -1 {
			return i
		}
	}
	log.Errorf("Unknown columns %s, from id %s, position %d", col.ToString(), col.FromID, col.Position)
	return -1
}

type joinReOrderSolver struct {
	graph      [][]int
	groups     []LogicalPlan
	visited    []bool
	resultJoin LogicalPlan
	allocator  *idAllocator
}

func (e *joinReOrderSolver) reorderJoin(groups []LogicalPlan, conds []expression.Expression) {
	e.graph = make([][]int, len(groups))
	e.groups = groups
	e.visited = make([]bool, len(groups))
	e.resultJoin = nil
	for _, cond := range conds {
		if f, ok := cond.(*expression.ScalarFunction); ok && f.FuncName.L == ast.EQ {
			lCol, lok := f.Args[0].(*expression.Column)
			rCol, rok := f.Args[1].(*expression.Column)
			if lok && rok {
				lID := findColumnIndexByGroup(groups, lCol)
				rID := findColumnIndexByGroup(groups, rCol)
				e.graph[lID] = append(e.graph[lID], rID)
				e.graph[rID] = append(e.graph[rID], lID)
			}
		}
	}
	var crossJoinGroup []LogicalPlan
	for i := 0; i < len(groups); i++ {
		if !e.visited[i] {
			e.resultJoin = e.groups[i]
			e.walkGraph(i)
			crossJoinGroup = append(crossJoinGroup, e.resultJoin)
		}
	}
	e.makeBushyJoin(crossJoinGroup)
}

func (e *joinReOrderSolver) makeBushyJoin(crossJoinGroup []LogicalPlan) {
	for len(crossJoinGroup) > 1 {
		resultJoinGroup := make([]LogicalPlan, 0, len(crossJoinGroup))
		for i := 0; i < len(crossJoinGroup); i += 2 {
			if i+1 == len(crossJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, crossJoinGroup[i])
				break
			}
			resultJoinGroup = append(resultJoinGroup, e.newJoin(crossJoinGroup[i], crossJoinGroup[i+1]))
		}
		crossJoinGroup = resultJoinGroup
	}
	e.resultJoin = crossJoinGroup[0]
}

func (e *joinReOrderSolver) newJoin(lChild, rChild LogicalPlan) *Join {
	join := &Join{
		JoinType:        InnerJoin,
		reordered:       true,
		baseLogicalPlan: newBaseLogicalPlan(Jn, e.allocator),
	}
	join.initID()
	join.SetChildren(lChild, rChild)
	join.SetSchema(append(lChild.GetSchema().DeepCopy(), rChild.GetSchema().DeepCopy()...))
	lChild.SetParents(join)
	rChild.SetParents(join)
	return join
}

func (e *joinReOrderSolver) walkGraph(u int) {
	e.visited[u] = true
	for _, v := range e.graph[u] {
		if !e.visited[v] {
			e.resultJoin = e.newJoin(e.resultJoin, e.groups[v])
			e.walkGraph(v)
		}
	}
}
