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
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/tracing"
)

// extractJoinGroup extracts all the join nodes connected with continuous
// InnerJoins to construct a join group. This join group is further used to
// construct a new join order based on a reorder algorithm.
//
// For example: "InnerJoin(InnerJoin(a, b), LeftJoin(c, d))"
// results in a join group {a, b, c, d}.
func extractJoinGroup(p LogicalPlan) (group []*joinNode, eqEdges []*expression.ScalarFunction, otherConds []expression.Expression, directedEdges []directedEdge, joinTypes []JoinType) {
	join, isJoin := p.(*LogicalJoin)
	directedEdges = make([]directedEdge, 0)
	jNode := &joinNode{
		p: p,
	}
	if !isJoin || join.preferJoinType > uint(0) || join.StraightJoin ||
		(join.JoinType != InnerJoin && join.JoinType != LeftOuterJoin && join.JoinType != RightOuterJoin) ||
		((join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin) && join.EqualConditions == nil) {
		return []*joinNode{jNode}, nil, nil, directedEdges, nil
	}

	lhsGroup, lhsEqualConds, lhsOtherConds, lhsDirectedEdges, lhsJoinTypes := extractJoinGroup(join.children[0])
	rhsGroup, rhsEqualConds, rhsOtherConds, rhsDirectedEdges, rhsJoinTypes := extractJoinGroup(join.children[1])

	// Collect the order for plans of the outerJoin
	// For example: a left join b indicate a must join before b
	//              a right join b indicate a must join after b
	var leftNode *joinNode
	var rightNode *joinNode
	for _, eqCond := range join.EqualConditions {
		arg0 := eqCond.GetArgs()[0].(*expression.Column)
		arg1 := eqCond.GetArgs()[1].(*expression.Column)
		for _, ld := range lhsGroup {
			if ld.p.Schema().Contains(arg0) {
				leftNode = ld
				break
			}
		}
		for _, ld := range rhsGroup {
			if ld.p.Schema().Contains(arg1) {
				rightNode = ld
				break
			}
		}
		edge := directedEdge{
			left:     leftNode,
			right:    rightNode,
			joinType: join.JoinType,
		}
		directedEdges = append(directedEdges, edge)

		for idx := range lhsDirectedEdges {
			lhsEdge := lhsDirectedEdges[idx]
			implicitEdges := extractImplictDirectedEdges(lhsEdge, edge)
			directedEdges = append(directedEdges, implicitEdges...)
		}
		for idx := range rhsDirectedEdges {
			rhsEdge := rhsDirectedEdges[idx]
			implicitEdges := extractImplictDirectedEdges(rhsEdge, edge)
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

// extractImplictDirectedEdges constructs implicit directed edges to guarantee join order
// when the implicit join order is not directly represented by the "join" expression.
//
// For example `A right join B right join C`,
// there is an implicit meaning that B must join A before C,
// so there will be a directed edge `B->C`
func extractImplictDirectedEdges(edge1 directedEdge, edge2 directedEdge) (implicitEdges []directedEdge) {
	if edge1.joinType == InnerJoin && edge2.joinType == InnerJoin {
		return nil
	}

	// Construct a directed graph of joinNode
	joinNodeGraph := make(map[*joinNode][]*joinNode, 4)
	mergeToGraph(edge1, joinNodeGraph)
	mergeToGraph(edge2, joinNodeGraph)
	// Find endpoints from the same source in a directed edge
	endPoints, ok := getCommonImplictEndPoint(joinNodeGraph)

	if ok {
		for i := range endPoints {
			if endPoints[i] == edge1.left || endPoints[i] == edge1.right {
				// Construct implicit edges
				implicitEdges = append(implicitEdges, directedEdge{
					left:       endPoints[i],
					right:      endPoints[i^1],
					isImplicit: true,
				})
				return
			}
		}
	}
	return
}

// mergeToGraph merge directed edge to construct graph
func mergeToGraph(edge directedEdge, joinNodeGraph map[*joinNode][]*joinNode) {
	if edge.joinType == LeftOuterJoin {
		joinNodeGraph[edge.right] = append(joinNodeGraph[edge.right], edge.left)
	} else if edge.joinType == RightOuterJoin {
		joinNodeGraph[edge.left] = append(joinNodeGraph[edge.left], edge.right)
	} else if edge.joinType == InnerJoin {
		joinNodeGraph[edge.left] = append(joinNodeGraph[edge.left], edge.right)
		joinNodeGraph[edge.right] = append(joinNodeGraph[edge.right], edge.left)
	}
}

// getCommonImplictEndPoint find endpoints from the same source in a directed edge
// For example there is a directed graph `A->B,A->C`, we will return points B,A
func getCommonImplictEndPoint(joinNodeGraph map[*joinNode][]*joinNode) ([]*joinNode, bool) {
	for _, endPoints := range joinNodeGraph {
		if len(endPoints) == 2 {
			return endPoints, true
		}
	}
	return nil, false
}

type joinReOrderSolver struct {
}

type directedEdge struct {
	left       *joinNode
	right      *joinNode
	joinType   JoinType
	isImplicit bool
}

type joinNode struct {
	id int
	p  LogicalPlan
}

type jrNode struct {
	id      int
	p       LogicalPlan
	cumCost float64
}

func (s *joinReOrderSolver) optimize(ctx context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	tracer := &joinReorderTrace{cost: map[string]float64{}, opt: opt}
	tracer.traceJoinReorder(p)
	p, err := s.optimizeRecursive(p.SCtx(), p, tracer)
	tracer.traceJoinReorder(p)
	appendJoinReorderTraceStep(tracer, p, opt)
	return p, err
}

// optimizeRecursive recursively collects join groups and applies join reorder algorithm for each group.
func (s *joinReOrderSolver) optimizeRecursive(ctx sessionctx.Context, p LogicalPlan, tracer *joinReorderTrace) (LogicalPlan, error) {
	var err error
	curJoinGroup, eqEdges, otherConds, directedEdges, joinTypes := extractJoinGroup(p)
	if len(curJoinGroup) > 1 {
		for i := range curJoinGroup {
			curJoinGroup[i].p, err = s.optimizeRecursive(ctx, curJoinGroup[i].p, tracer)
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
			p, err = groupSolver.solve(curJoinGroup, tracer)
		} else {
			dpSolver := &joinReorderDPSolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
			}
			dpSolver.newJoin = dpSolver.newJoinWithEdges
			p, err = dpSolver.solve(curJoinGroup, expression.ScalarFuncs2Exprs(eqEdges), tracer)
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
		newChild, err := s.optimizeRecursive(ctx, child, tracer)
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
	directGraph [][]byte
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

func appendJoinReorderTraceStep(tracer *joinReorderTrace, plan LogicalPlan, opt *logicalOptimizeOp) {
	if len(tracer.initial) < 1 || len(tracer.final) < 1 {
		return
	}
	action := func() string {
		return fmt.Sprintf("join order becomes %v from original %v", tracer.final, tracer.initial)
	}
	reason := func() string {
		buffer := bytes.NewBufferString("join cost during reorder: [")
		var joins []string
		for join := range tracer.cost {
			joins = append(joins, join)
		}
		sort.Strings(joins)
		for i, join := range joins {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(fmt.Sprintf("[%s, cost:%v]", join, tracer.cost[join]))
		}
		buffer.WriteString("]")
		return buffer.String()
	}
	opt.appendStepToCurrent(plan.ID(), plan.TP(), reason, action)
}

func allJoinOrderToString(tt []*tracing.PlanTrace) string {
	if len(tt) == 1 {
		return joinOrderToString(tt[0])
	}
	buffer := bytes.NewBufferString("[")
	for i, t := range tt {
		if i > 0 {
			buffer.WriteString(",")
		}
		buffer.WriteString(joinOrderToString(t))
	}
	buffer.WriteString("]")
	return buffer.String()
}

// joinOrderToString let Join(DataSource, DataSource) become '(t1*t2)'
func joinOrderToString(t *tracing.PlanTrace) string {
	if t.TP == plancodec.TypeJoin {
		buffer := bytes.NewBufferString("(")
		for i, child := range t.Children {
			if i > 0 {
				buffer.WriteString("*")
			}
			buffer.WriteString(joinOrderToString(child))
		}
		buffer.WriteString(")")
		return buffer.String()
	} else if t.TP == plancodec.TypeDataSource {
		return t.ExplainInfo[6:]
	}
	return ""
}

// extractJoinAndDataSource will only keep join and dataSource operator and remove other operators.
// For example: Proj->Join->(Proj->DataSource, DataSource) will become Join->(DataSource, DataSource)
func extractJoinAndDataSource(t *tracing.PlanTrace) []*tracing.PlanTrace {
	roots := findRoots(t)
	if len(roots) < 1 {
		return nil
	}
	rr := make([]*tracing.PlanTrace, 0, len(roots))
	for _, root := range roots {
		simplify(root)
		rr = append(rr, root)
	}
	return rr
}

// simplify only keeps Join and DataSource operators, and discard other operators.
func simplify(node *tracing.PlanTrace) {
	if len(node.Children) < 1 {
		return
	}
	for valid := false; !valid; {
		valid = true
		newChildren := make([]*tracing.PlanTrace, 0)
		for _, child := range node.Children {
			if child.TP != plancodec.TypeDataSource && child.TP != plancodec.TypeJoin {
				newChildren = append(newChildren, child.Children...)
				valid = false
			} else {
				newChildren = append(newChildren, child)
			}
		}
		node.Children = newChildren
	}
	for _, child := range node.Children {
		simplify(child)
	}
}

func findRoots(t *tracing.PlanTrace) []*tracing.PlanTrace {
	if t.TP == plancodec.TypeJoin || t.TP == plancodec.TypeDataSource {
		return []*tracing.PlanTrace{t}
	}
	var r []*tracing.PlanTrace
	for _, child := range t.Children {
		r = append(r, findRoots(child)...)
	}
	return r
}

type joinReorderTrace struct {
	opt     *logicalOptimizeOp
	initial string
	final   string
	cost    map[string]float64
}

func (t *joinReorderTrace) traceJoinReorder(p LogicalPlan) {
	if t == nil || t.opt == nil || t.opt.tracer == nil {
		return
	}
	if len(t.initial) > 0 {
		t.final = allJoinOrderToString(extractJoinAndDataSource(p.buildPlanTrace()))
		return
	}
	t.initial = allJoinOrderToString(extractJoinAndDataSource(p.buildPlanTrace()))
}

func (t *joinReorderTrace) appendLogicalJoinCost(join LogicalPlan, cost float64) {
	if t == nil || t.opt == nil || t.opt.tracer == nil {
		return
	}
	joinMapKey := allJoinOrderToString(extractJoinAndDataSource(join.buildPlanTrace()))
	t.cost[joinMapKey] = cost
}
