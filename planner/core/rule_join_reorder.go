// Copyright 2019 PingCAP, Inc.
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
	"context"
	"sort"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
)

type leftJoinInnerTab struct {
	p         LogicalPlan
	eqCond    []*expression.ScalarFunction
	leftCond  []expression.Expression
	otherCond []expression.Expression
}

// extractJoinGroup extracts all the join nodes connected with continuous
// InnerJoins to construct a join group. This join group is further used to
// construct a new join order based on a reorder algorithm.
//
// For example: "InnerJoin(InnerJoin(a, b), LeftJoin(c, d))"
// results in a join group {a, b, LeftJoin(c, d)}.
func extractJoinGroup(p LogicalPlan) (
	group []LogicalPlan,
	eqEdges []*expression.ScalarFunction,
	otherConds []expression.Expression,
	leftJoinTabs []*leftJoinInnerTab,
) {
	join, isJoin := p.(*LogicalJoin)
	if !isJoin || join.preferJoinType > uint(0) || (join.JoinType != InnerJoin && join.JoinType != LeftOuterJoin) || join.StraightJoin {
		return []LogicalPlan{p}, nil, nil, nil
	}

	if join.JoinType == LeftOuterJoin {
		tab := &leftJoinInnerTab{
			p:         join.children[1],
			eqCond:    join.EqualConditions,
			leftCond:  join.LeftConditions,
			otherCond: join.OtherConditions,
		}
		leftJoinTabs = append(leftJoinTabs, tab)
		lhsGroup, lhsEqualConds, lhsOtherConds, lhsJoinTabs := extractJoinGroup(join.children[0])
		group = append(group, lhsGroup...)
		eqEdges = append(eqEdges, lhsEqualConds...)
		otherConds = append(otherConds, lhsOtherConds...)
		leftJoinTabs = append(leftJoinTabs, lhsJoinTabs...)
		return
	}

	lhsGroup, lhsEqualConds, lhsOtherConds, lhsJoinTabs := extractJoinGroup(join.children[0])
	rhsGroup, rhsEqualConds, rhsOtherConds, rhsJoinTabs := extractJoinGroup(join.children[1])

	group = append(group, lhsGroup...)
	group = append(group, rhsGroup...)
	eqEdges = append(eqEdges, join.EqualConditions...)
	eqEdges = append(eqEdges, lhsEqualConds...)
	eqEdges = append(eqEdges, rhsEqualConds...)
	otherConds = append(otherConds, join.OtherConditions...)
	otherConds = append(otherConds, lhsOtherConds...)
	otherConds = append(otherConds, rhsOtherConds...)
	leftJoinTabs = append(leftJoinTabs, lhsJoinTabs...)
	leftJoinTabs = append(leftJoinTabs, rhsJoinTabs...)
	return
}

type joinReOrderSolver struct {
}

type jrNode struct {
	p       LogicalPlan
	cumCost float64
}

func (s *joinReOrderSolver) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	return s.optimizeRecursive(p.SCtx(), p)
}

// optimizeRecursive recursively collects join groups and applies join reorder algorithm for each group.
func (s *joinReOrderSolver) optimizeRecursive(ctx sessionctx.Context, p LogicalPlan) (LogicalPlan, error) {
	var err error
	curJoinGroup, eqEdges, otherConds, leftJoinTabs := extractJoinGroup(p)
	if len(curJoinGroup)+len(leftJoinTabs) > 1 {
		for i := range curJoinGroup {
			curJoinGroup[i], err = s.optimizeRecursive(ctx, curJoinGroup[i])
			if err != nil {
				return nil, err
			}
		}
		for i := range leftJoinTabs {
			leftJoinTabs[i].p, err = s.optimizeRecursive(ctx, leftJoinTabs[i].p)
			if err != nil {
				return nil, err
			}
		}
		baseGroupSolver := &baseSingleGroupJoinOrderSolver{
			ctx:        ctx,
			otherConds: otherConds,
		}
		if len(curJoinGroup) > ctx.GetSessionVars().TiDBOptJoinReorderThreshold {
			groupSolver := &joinReorderGreedySolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
				eqEdges:                        eqEdges,
			}
			p, err = groupSolver.solve(curJoinGroup)
		} else {
			dpSolver := &joinReorderDPSolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
			}
			dpSolver.newJoin = dpSolver.newJoinWithEdges
			p, err = dpSolver.solve(curJoinGroup, expression.ScalarFuncs2Exprs(eqEdges))
		}
		if err != nil {
			return nil, err
		}
		for _, tab := range leftJoinTabs {
			_, err := tab.p.recursiveDeriveStats(nil)
			if err != nil {
				return nil, err
			}
		}
		// Sort the left join tabs by the following rule:
		//   1. If there're join keys on the j-th one and the key can be found in i-th, means that the j-th should be executed after the i-th
		//      e.g. select * from t1 left join t2 on t1.a=t2.a left join t3 on t2.b=t3.b; In this case, t3 should be joined after t2.
		//   2. If j-th one has no join key and the i-th one has, the i-th one should be joined before the j-th one.
		//   3. Otherwise we compare the size of two tab, the smaller one first.
		sort.Slice(leftJoinTabs, func(i, j int) bool {
			if len(leftJoinTabs[j].eqCond) > 0 && leftJoinTabs[i].p.Schema().ColumnIndex(leftJoinTabs[j].eqCond[0].GetArgs()[0].(*expression.Column)) >= 0 {
				return true
			}
			if len(leftJoinTabs[i].eqCond) > 0 && len(leftJoinTabs[j].eqCond) == 0 {
				return true
			}
			return leftJoinTabs[i].p.statsInfo().RowCount < leftJoinTabs[j].p.statsInfo().RowCount
		})
		for _, joinTab := range leftJoinTabs {
			p = s.newLeftJoin(ctx, p, joinTab.p, joinTab.eqCond, joinTab.leftCond, joinTab.otherCond)
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

func (s *joinReOrderSolver) newLeftJoin(
	ctx sessionctx.Context,
	lChild, rChild LogicalPlan,
	eqCond []*expression.ScalarFunction,
	leftCond []expression.Expression,
	otherCond []expression.Expression,
) *LogicalJoin {
	offset := lChild.SelectBlockOffset()
	if offset != rChild.SelectBlockOffset() {
		offset = -1
	}
	join := LogicalJoin{
		JoinType:        LeftOuterJoin,
		EqualConditions: eqCond,
		LeftConditions:  leftCond,
		OtherConditions: otherCond,
		reordered:       true,
	}.Init(ctx, offset)
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	return join
}

type baseSingleGroupJoinOrderSolver struct {
	ctx          sessionctx.Context
	curJoinGroup []*jrNode
	otherConds   []expression.Expression
}

// baseNodeCumCost calculate the cumulative cost of the node in the join group.
func (s *baseSingleGroupJoinOrderSolver) baseNodeCumCost(groupNode LogicalPlan) float64 {
	cost := groupNode.statsInfo().RowCount
	for _, child := range groupNode.Children() {
		cost += s.baseNodeCumCost(child)
	}
	return cost
}

// makeBushyJoin build bushy tree for the nodes which have no equal condition to connect them.
func (s *baseSingleGroupJoinOrderSolver) makeBushyJoin(cartesianJoinGroup []LogicalPlan) LogicalPlan {
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

func (s *baseSingleGroupJoinOrderSolver) newCartesianJoin(lChild, rChild LogicalPlan) *LogicalJoin {
	offset := lChild.SelectBlockOffset()
	if offset != rChild.SelectBlockOffset() {
		offset = -1
	}
	join := LogicalJoin{
		JoinType:  InnerJoin,
		reordered: true,
	}.Init(s.ctx, offset)
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	return join
}

func (s *baseSingleGroupJoinOrderSolver) newJoinWithEdges(lChild, rChild LogicalPlan, eqEdges []*expression.ScalarFunction, otherConds []expression.Expression) LogicalPlan {
	newJoin := s.newCartesianJoin(lChild, rChild)
	newJoin.EqualConditions = eqEdges
	newJoin.OtherConditions = otherConds
	return newJoin
}

// calcJoinCumCost calculates the cumulative cost of the join node.
func (s *baseSingleGroupJoinOrderSolver) calcJoinCumCost(join LogicalPlan, lNode, rNode *jrNode) float64 {
	return join.statsInfo().RowCount + lNode.cumCost + rNode.cumCost
}

func (*joinReOrderSolver) name() string {
	return "join_reorder"
}
