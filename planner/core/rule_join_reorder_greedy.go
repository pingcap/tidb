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

// extractJoinGroup extracts all the join nodes connected with continuous
// InnerJoins to construct a join group. This join group is further used to
// construct a new join order based on a greedy algorithm.
//
// For example: "InnerJoin(InnerJoin(a, b), LeftJoin(c, d))"
// results in a join group {a, b, LeftJoin(c, d)}.
func extractJoinGroup(p LogicalPlan) (group []LogicalPlan, eqEdges []*expression.ScalarFunction, otherConds []expression.Expression) {
	join, isJoin := p.(*LogicalJoin)
	if !isJoin || join.preferJoinType > uint(0) || join.JoinType != InnerJoin || join.StraightJoin {
		return []LogicalPlan{p}, nil, nil
	}

	lhsGroup, lhsEqualConds, lhsOtherConds := extractJoinGroup(join.children[0])
	rhsGroup, rhsEqualConds, rhsOtherConds := extractJoinGroup(join.children[1])

	group = append(group, lhsGroup...)
	group = append(group, rhsGroup...)
	eqEdges = append(eqEdges, join.EqualConditions...)
	eqEdges = append(eqEdges, lhsEqualConds...)
	eqEdges = append(eqEdges, rhsEqualConds...)
	otherConds = append(otherConds, join.OtherConditions...)
	otherConds = append(otherConds, lhsOtherConds...)
	otherConds = append(otherConds, rhsOtherConds...)
	return group, eqEdges, otherConds
}

type joinReOrderSolver struct {
}

type joinReorderGreedySingleGroupSolver struct {
	ctx          sessionctx.Context
	curJoinGroup []*jrNode
	eqEdges      []*expression.ScalarFunction
	otherConds   []expression.Expression
}

type jrNode struct {
	p       LogicalPlan
	cumCost float64
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
func (s *joinReorderGreedySingleGroupSolver) solve(joinNodePlans []LogicalPlan) (LogicalPlan, error) {
	for _, node := range joinNodePlans {
		_, err := node.recursiveDeriveStats()
		if err != nil {
			return nil, err
		}
		s.curJoinGroup = append(s.curJoinGroup, &jrNode{
			p:       node,
			cumCost: s.baseNodeCumCost(node),
		})
	}
	sort.SliceStable(s.curJoinGroup, func(i, j int) bool {
		return s.curJoinGroup[i].cumCost < s.curJoinGroup[j].cumCost
	})

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

func (s *joinReorderGreedySingleGroupSolver) baseNodeCumCost(groupNode LogicalPlan) float64 {
	cost := groupNode.statsInfo().RowCount
	for _, child := range groupNode.Children() {
		cost += s.baseNodeCumCost(child)
	}
	return cost
}

func (s *joinReorderGreedySingleGroupSolver) constructConnectedJoinTree() (*jrNode, error) {
	curJoinTree := s.curJoinGroup[0]
	s.curJoinGroup = s.curJoinGroup[1:]
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
			_, err := newJoin.recursiveDeriveStats()
			if err != nil {
				return nil, err
			}
			curCost := curJoinTree.cumCost + newJoin.statsInfo().RowCount + node.cumCost
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
	}
	return curJoinTree, nil
}

func (s *joinReorderGreedySingleGroupSolver) checkConnectionAndMakeJoin(leftNode, rightNode LogicalPlan) (LogicalPlan, []expression.Expression) {
	var usedEdges []*expression.ScalarFunction
	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
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
		return nil, nil
	}
	newJoin := s.newCartesianJoin(leftNode, rightNode)
	newJoin.EqualConditions = usedEdges
	for _, eqCond := range newJoin.EqualConditions {
		newJoin.LeftJoinKeys = append(newJoin.LeftJoinKeys, eqCond.GetArgs()[0].(*expression.Column))
		newJoin.RightJoinKeys = append(newJoin.RightJoinKeys, eqCond.GetArgs()[1].(*expression.Column))
	}
	for i := len(remainOtherConds) - 1; i >= 0; i-- {
		cols := expression.ExtractColumns(remainOtherConds[i])
		if newJoin.schema.ColumnsIndices(cols) != nil {
			newJoin.OtherConditions = append(newJoin.OtherConditions, remainOtherConds[i])
			remainOtherConds = append(remainOtherConds[:i], remainOtherConds[i+1:]...)
		}
	}
	return newJoin, remainOtherConds
}

func (s *joinReorderGreedySingleGroupSolver) makeBushyJoin(cartesianJoinGroup []LogicalPlan) LogicalPlan {
	resultJoinGroup := make([]LogicalPlan, 0, (len(cartesianJoinGroup)+1)/2)
	for len(cartesianJoinGroup) > 1 {
		resultJoinGroup = resultJoinGroup[:0]
		for i := 0; i < len(cartesianJoinGroup); i += 2 {
			if i+1 == len(cartesianJoinGroup) {
				resultJoinGroup = append(resultJoinGroup, cartesianJoinGroup[i])
				break
			}
			newJoin := s.newCartesianJoin(cartesianJoinGroup[i], cartesianJoinGroup[i+1])
			for i := len(s.otherConds) - 1; i >= 0; i-- {
				cols := expression.ExtractColumns(s.otherConds[i])
				if newJoin.schema.ColumnsIndices(cols) != nil {
					newJoin.OtherConditions = append(newJoin.OtherConditions, s.otherConds[i])
					s.otherConds = append(s.otherConds[:i], s.otherConds[i+1:]...)
				}
			}
			resultJoinGroup = append(resultJoinGroup, newJoin)
		}
		cartesianJoinGroup, resultJoinGroup = resultJoinGroup, cartesianJoinGroup
	}
	return cartesianJoinGroup[0]
}

func (s *joinReorderGreedySingleGroupSolver) newCartesianJoin(lChild, rChild LogicalPlan) *LogicalJoin {
	join := LogicalJoin{
		JoinType:  InnerJoin,
		reordered: true,
	}.Init(s.ctx)
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	return join
}

func (s *joinReOrderSolver) optimize(p LogicalPlan) (LogicalPlan, error) {
	return s.optimizeRecursive(p.context(), p)
}

func (s *joinReOrderSolver) optimizeRecursive(ctx sessionctx.Context, p LogicalPlan) (LogicalPlan, error) {
	var err error
	curJoinGroup, eqEdges, otherConds := extractJoinGroup(p)
	if len(curJoinGroup) > 1 {
		for i := range curJoinGroup {
			curJoinGroup[i], err = s.optimizeRecursive(ctx, curJoinGroup[i])
			if err != nil {
				return nil, err
			}
		}
		if len(curJoinGroup) > ctx.GetSessionVars().TiDBOptJoinOrderAlgoThreshold {
			groupSolver := &joinReorderGreedySingleGroupSolver{
				ctx:        ctx,
				eqEdges:    eqEdges,
				otherConds: otherConds,
			}
			p, err = groupSolver.solve(curJoinGroup)
			if err != nil {
				return nil, err
			}
			return p, nil
		}
		dpSolver := &joinReorderDPSolver{
			ctx: ctx,
		}
		dpSolver.newJoin = dpSolver.newJoinWithConds
		p, err = dpSolver.solve(curJoinGroup, expression.ScalarFuncs2Exprs(eqEdges), otherConds)
		if err != nil {
			return nil, err
		}
		return p, nil
	}
	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := s.optimizeRecursive(ctx, child)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	return p, nil
}
