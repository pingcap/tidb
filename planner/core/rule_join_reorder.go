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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
)

// extractJoinGroup extracts all the join nodes connected with continuous
// InnerJoins to construct a join group. This join group is further used to
// construct a new join order based on a reorder algorithm.
//
// For example: "InnerJoin(InnerJoin(a, b), LeftJoin(c, d))"
// results in a join group {a, b, c, d}.
func extractJoinGroup(p LogicalPlan) (group []LogicalPlan, eqEdges []*expression.ScalarFunction, otherConds []expression.Expression, directedEdges []directedEdge, joinTypes []JoinType) {
	join, isJoin := p.(*LogicalJoin)
	directedEdges = make([]directedEdge, 0)
	if !isJoin || join.preferJoinType > uint(0) || join.StraightJoin ||
		(join.JoinType != InnerJoin && join.JoinType != LeftOuterJoin && join.JoinType != RightOuterJoin) ||
		((join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin) && join.EqualConditions == nil) {
		return []LogicalPlan{p}, nil, nil, directedEdges, nil
	}

	lhsGroup, lhsEqualConds, lhsOtherConds, lhsDirectedEdges, lhsJoinTypes := extractJoinGroup(join.children[0])
	rhsGroup, rhsEqualConds, rhsOtherConds, rhsDirectedEdges, rhsJoinTypes := extractJoinGroup(join.children[1])

	// Collect the order for plans of the outerJoin
	// For example: a left join b indicate a must join before b
	//              a right join b indicate a must join after b
	var leftPlan LogicalPlan
	var rightPlan LogicalPlan
	for _, eqCond := range join.EqualConditions {
		arg0 := eqCond.GetArgs()[0].(*expression.Column)
		arg1 := eqCond.GetArgs()[1].(*expression.Column)
		for _, leftNode := range lhsGroup {
			if leftNode.Schema().Contains(arg0) {
				leftPlan = leftNode
				break
			}
		}
		for _, rightNode := range rhsGroup {
			if rightNode.Schema().Contains(arg1) {
				rightPlan = rightNode
				break
			}
		}
		edge := directedEdge{
			left:     leftPlan,
			right:    rightPlan,
			joinType: join.JoinType,
		}
		directedEdges = append(directedEdges, edge)

		for idx := range lhsDirectedEdges {
			lhsEdge := lhsDirectedEdges[idx]
			implicitEdges := extractNatureDirectedEdges(lhsEdge, edge)
			directedEdges = append(directedEdges, implicitEdges...)
		}
		for idx := range rhsDirectedEdges {
			rhsEdge := rhsDirectedEdges[idx]
			implicitEdges := extractNatureDirectedEdges(rhsEdge, edge)
			directedEdges = append(directedEdges, implicitEdges...)
		}
	}
	directedEdges = append(directedEdges, lhsDirectedEdges...)
	directedEdges = append(directedEdges, rhsDirectedEdges...)
	group = append(group, lhsGroup...)
	group = append(group, rhsGroup...)
	eqEdges = append(eqEdges, join.EqualConditions...)
	eqEdges = append(eqEdges, lhsEqualConds...)
	eqEdges = append(eqEdges, rhsEqualConds...)
	otherConds = append(otherConds, join.OtherConditions...)
	otherConds = append(otherConds, join.LeftConditions...)
	otherConds = append(otherConds, join.RightConditions...)
	otherConds = append(otherConds, lhsOtherConds...)
	otherConds = append(otherConds, rhsOtherConds...)
	for range join.EqualConditions {
		joinTypes = append(joinTypes, join.JoinType)
	}
	joinTypes = append(joinTypes, lhsJoinTypes...)
	joinTypes = append(joinTypes, rhsJoinTypes...)
	return group, eqEdges, otherConds, directedEdges, joinTypes
}

// Extract implicit join order
func extractNatureDirectedEdges(edge1 directedEdge, edge2 directedEdge) (implicitEdges []directedEdge) {
	implicitEdges = make([]directedEdge, 0)
	if edge2.joinType == LeftOuterJoin {
		if (edge1.joinType == InnerJoin || edge1.joinType == RightOuterJoin) && edge1.left == edge2.right {
			implicitEdges = append(implicitEdges, directedEdge{
				left:       edge1.right,
				right:      edge2.left,
				isImplicit: true,
			})
		} else if (edge1.joinType == InnerJoin || edge1.joinType == LeftOuterJoin) && edge1.right == edge2.right {
			implicitEdges = append(implicitEdges, directedEdge{
				left:       edge1.left,
				right:      edge2.left,
				isImplicit: true,
			})
		}
	} else if edge2.joinType == RightOuterJoin {
		if (edge1.joinType == InnerJoin || edge1.joinType == RightOuterJoin) && edge1.left == edge2.left {
			implicitEdges = append(implicitEdges, directedEdge{
				left:       edge1.right,
				right:      edge2.left,
				isImplicit: true,
			})
		} else if (edge1.joinType == InnerJoin || edge1.joinType == LeftOuterJoin) && edge1.right == edge2.left {
			implicitEdges = append(implicitEdges, directedEdge{
				left:       edge1.left,
				right:      edge2.right,
				isImplicit: true,
			})
		}
	} else if edge2.joinType == InnerJoin {
		if edge1.joinType == RightOuterJoin && edge1.left == edge2.left {
			implicitEdges = append(implicitEdges, directedEdge{
				left:       edge1.right,
				right:      edge2.right,
				isImplicit: true,
			})
		} else if edge1.joinType == LeftOuterJoin && edge1.right == edge2.left {
			implicitEdges = append(implicitEdges, directedEdge{
				left:       edge1.left,
				right:      edge2.right,
				isImplicit: true,
			})
		}
	}
	return
}

type joinReOrderSolver struct {
}

type directedEdge struct {
	left       LogicalPlan
	right      LogicalPlan
	joinType   JoinType
	isImplicit bool
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
	curJoinGroup, eqEdges, otherConds, directedEdges, joinTypes := extractJoinGroup(p)
	if len(curJoinGroup) > 1 {
		for i := range curJoinGroup {
			curJoinGroup[i], err = s.optimizeRecursive(ctx, curJoinGroup[i])
			if err != nil {
				return nil, err
			}
		}
		baseGroupSolver := &baseSingleGroupJoinOrderSolver{
			ctx:        ctx,
			otherConds: otherConds,
		}
		originalSchema := p.Schema()

		// Not support outer join reorder with pd
		isSupportDP := true
		for _, joinType := range joinTypes {
			if joinType != InnerJoin {
				isSupportDP = false
				break
			}
		}
		if len(curJoinGroup) > ctx.GetSessionVars().TiDBOptJoinReorderThreshold || !isSupportDP {
			groupSolver := &joinReorderGreedySolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
				eqEdges:                        eqEdges,
				joinTypes:                      joinTypes,
				directedEdges:                  directedEdges,
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
		schemaChanged := false
		if len(p.Schema().Columns) != len(originalSchema.Columns) {
			schemaChanged = true
		} else {
			for i, col := range p.Schema().Columns {
				if !col.Equal(nil, originalSchema.Columns[i]) {
					schemaChanged = true
					break
				}
			}
		}
		if schemaChanged {
			proj := LogicalProjection{
				Exprs: expression.Column2Exprs(originalSchema.Columns),
			}.Init(p.SCtx(), p.SelectBlockOffset())
			proj.SetSchema(originalSchema)
			proj.SetChildren(p)
			p = proj
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

// nolint:structcheck
type baseSingleGroupJoinOrderSolver struct {
	ctx             sessionctx.Context
	curJoinGroup    []*jrNode
	remainJoinGroup []*jrNode
	otherConds      []expression.Expression
	// A map maintain plan and plans which must join after the plan
	directMap map[LogicalPlan]map[LogicalPlan]struct{}
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

func (s *baseSingleGroupJoinOrderSolver) newJoinWithEdges(lChild, rChild LogicalPlan,
	eqEdges []*expression.ScalarFunction, otherConds, leftConds, rightConds []expression.Expression, joinType JoinType) LogicalPlan {
	newJoin := s.newCartesianJoin(lChild, rChild)
	newJoin.EqualConditions = eqEdges
	newJoin.OtherConditions = otherConds
	newJoin.LeftConditions = leftConds
	newJoin.RightConditions = rightConds
	newJoin.JoinType = joinType
	return newJoin
}

// calcJoinCumCost calculates the cumulative cost of the join node.
func (s *baseSingleGroupJoinOrderSolver) calcJoinCumCost(join LogicalPlan, lNode, rNode *jrNode) float64 {
	return join.statsInfo().RowCount + lNode.cumCost + rNode.cumCost
}

func (*joinReOrderSolver) name() string {
	return "join_reorder"
}
