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
	"github.com/pingcap/tidb/sessionctx"
)

func extractInnerJoinGroup(p *LogicalJoin) ([]LogicalPlan, []*expression.ScalarFunction, []expression.Expression) {
	if p.reordered || p.preferJoinType > uint(0) || p.JoinType != InnerJoin || p.StraightJoin {
		return nil, nil, nil
	}
	lChild := p.children[0]
	rChild := p.children[1]
	lJoin, ok := lChild.(*LogicalJoin)
	if !ok {
		eqConds := make([]*expression.ScalarFunction, len(p.EqualConditions))
		otherConds := make([]expression.Expression, len(p.OtherConditions))
		copy(eqConds, p.EqualConditions)
		copy(otherConds, p.OtherConditions)
		return []LogicalPlan{lChild, rChild}, eqConds, otherConds
	}
	lhsJoinGroup, eqEdges, otherConds := extractInnerJoinGroup(lJoin)
	if lhsJoinGroup == nil {
		return nil, nil, nil
	}
	return append(lhsJoinGroup, rChild), append(eqEdges, p.EqualConditions...), append(otherConds, p.OtherConditions...)
}

type joinReOrderGreedySolver struct {
	ctx          sessionctx.Context
	curJoinGroup []LogicalPlan
	eqEdges      []*expression.ScalarFunction
	otherConds   []expression.Expression
}

func (s *joinReOrderGreedySolver) solve() (LogicalPlan, error) {
	for _, node := range s.curJoinGroup {
		_, err := node.deriveStats()
		if err != nil {
			return nil, err
		}
	}
	sort.Slice(s.curJoinGroup, func(i, j int) bool {
		return s.curJoinGroup[i].statsInfo().RowCount < s.curJoinGroup[j].statsInfo().RowCount
	})

	var cartesianGroup []LogicalPlan
	for len(s.curJoinGroup) > 0 {
		newNode, err := s.enlargeJoinTree()
		if err != nil {
			return nil, err
		}
		cartesianGroup = append(cartesianGroup, newNode)
	}

	return s.makeBushyJoin(cartesianGroup), nil
}

func (s *joinReOrderGreedySolver) enlargeJoinTree() (LogicalPlan, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
	for {
		bestCost := math.MaxFloat64
		bestIdx := -1
		var bestJoin LogicalPlan
		for i, node := range s.curJoinGroup {
			newJoin := s.connectedAndNewNode(curJoinTree, node)
			if newJoin == nil {
				continue
			}
			_, err := newJoin.deriveStats()
			if err != nil {
				return nil, err
			}
			curCost := curJoinTree.statsInfo().RowCount + newJoin.statsInfo().RowCount + node.statsInfo().RowCount
			if bestCost > curCost {
				bestCost = curCost
				bestJoin = newJoin
				bestIdx = i
			}
		}
		if bestJoin == nil {
			break
		}
		curJoinTree = bestJoin
		s.curJoinGroup = append(s.curJoinGroup[:bestIdx], s.curJoinGroup[bestIdx+1:]...)
	}
	return curJoinTree, nil
}

func (s *joinReOrderGreedySolver) connectedAndNewNode(leftNode, rightNode LogicalPlan) LogicalPlan {
	var usedEdges []*expression.ScalarFunction
	for _, edge := range s.eqEdges {
		lCol := edge.GetArgs()[0].(*expression.Column)
		rCol := edge.GetArgs()[1].(*expression.Column)
		if leftNode.Schema().Contains(lCol) && rightNode.Schema().Contains(rCol) {
			usedEdges = append(usedEdges, edge)
		} else if rightNode.Schema().Contains(lCol) && leftNode.Schema().Contains(rCol) {
			newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
			usedEdges = append(usedEdges, newSf)
		}
	}
	if len(usedEdges) == 0 {
		return nil
	}
	newJoin := s.newJoin(leftNode, rightNode)
	newJoin.EqualConditions = usedEdges
	for _, eqCond := range newJoin.EqualConditions {
		newJoin.LeftJoinKeys = append(newJoin.LeftJoinKeys, eqCond.GetArgs()[0].(*expression.Column))
		newJoin.RightJoinKeys = append(newJoin.RightJoinKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	for i := len(s.otherConds) - 1; i >= 0; i-- {
		cols := expression.ExtractColumns(s.otherConds[i])
		if newJoin.schema.ColumnsIndices(cols) != nil {
			newJoin.OtherConditions = append(newJoin.OtherConditions, s.otherConds[i])
			s.otherConds = append(s.otherConds[:i], s.otherConds[i+1:]...)
		}
	}
	return newJoin
}

func (s *joinReOrderGreedySolver) makeBushyJoin(cartesianJoinGroup []LogicalPlan) LogicalPlan {
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup := make([]LogicalPlan, 0, len(cartesianJoinGroup))
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			resultJoinGroup = append(resultJoinGroup, s.newJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1]))
		}
		cartesianJoinGroup = resultJoinGroup
	}
	return cartesianJoinGroup[0]
}

func (s *joinReOrderGreedySolver) newJoin(lChild, rChild LogicalPlan) *LogicalJoin {
	join := LogicalJoin{
		JoinType:  InnerJoin,
		reordered: true,
	}.Init(s.ctx)
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	return join
}

func (s *joinReOrderGreedySolver) optimize(p LogicalPlan) (LogicalPlan, error) {
	s.ctx = p.context()
	return s.optimizeRecursive(p)
}

func (s *joinReOrderGreedySolver) optimizeRecursive(p LogicalPlan) (LogicalPlan, error) {
	if join, ok := p.(*LogicalJoin); ok {
		s.curJoinGroup, s.eqEdges, s.otherConds = extractInnerJoinGroup(join)
		if len(s.curJoinGroup) != 0 {
			var err error
			p, err = s.solve()
			if err != nil {
				return nil, err
			}
		}
	}
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := s.optimizeRecursive(child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	return p, nil
}
