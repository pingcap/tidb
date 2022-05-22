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
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/tracing"
)

// extractJoinGroup extracts all the join nodes connected with continuous
// Joins to construct a join group. This join group is further used to
// construct a new join order based on a reorder algorithm.
//
// For example: "InnerJoin(InnerJoin(a, b), LeftJoin(c, d))"
// results in a join group {a, b, c, d}.
func extractJoinGroup(p LogicalPlan) (group []LogicalPlan, eqEdges []*expression.ScalarFunction,
	otherConds []expression.Expression, joinTypes []JoinType, hintInfo []*tableHintInfo, hasOuterJoin bool) {
	join, isJoin := p.(*LogicalJoin)
	if isJoin && join.preferJoinOrder {
		// When there is a leading hint, the hint may not take effect for other reasons.
		// For example, the join type is cross join or straight join, or exists the join algorithm hint, etc.
		// We need to return the hint information to warn
		hintInfo = append(hintInfo, join.hintInfo)
	}
	if !isJoin || join.preferJoinType > uint(0) || join.StraightJoin ||
		(join.JoinType != InnerJoin && join.JoinType != LeftOuterJoin && join.JoinType != RightOuterJoin) ||
		((join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin) && join.EqualConditions == nil) {
		if hintInfo != nil {
			// The leading hint can not work for some reasons. So clear it in the join node.
			join.hintInfo = nil
		}
		return []LogicalPlan{p}, nil, nil, nil, hintInfo, false
	}
	// If the session var is set to off, we will still reject the outer joins.
	if !p.SCtx().GetSessionVars().EnableOuterJoinReorder && (join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin) {
		return []LogicalPlan{p}, nil, nil, nil, hintInfo, false
	}
	hasOuterJoin = hasOuterJoin || (join.JoinType != InnerJoin)
	if join.JoinType != RightOuterJoin {
		lhsGroup, lhsEqualConds, lhsOtherConds, lhsJoinTypes, lhsHintInfo, lhsHasOuterJoin := extractJoinGroup(join.children[0])
		noExpand := false
		// If the filters of the outer join is related with multiple leaves of the outer join side. We don't reorder it for now.
		if join.JoinType == LeftOuterJoin {
			extractedCols := make([]*expression.Column, 0, 8)
			extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, join.OtherConditions, nil)
			extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, join.LeftConditions, nil)
			extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, expression.ScalarFuncs2Exprs(join.EqualConditions), nil)
			affectedGroups := 0
			for i := range lhsGroup {
				for _, col := range extractedCols {
					if lhsGroup[i].Schema().Contains(col) {
						affectedGroups++
						break
					}
				}
				if affectedGroups > 1 {
					noExpand = true
					break
				}
			}
		}
		if noExpand {
			return []LogicalPlan{p}, nil, nil, nil, nil, false
		}
		group = append(group, lhsGroup...)
		eqEdges = append(eqEdges, lhsEqualConds...)
		otherConds = append(otherConds, lhsOtherConds...)
		joinTypes = append(joinTypes, lhsJoinTypes...)
		hintInfo = append(hintInfo, lhsHintInfo...)
		hasOuterJoin = hasOuterJoin || lhsHasOuterJoin
	} else {
		group = append(group, join.children[0])
	}

	if join.JoinType != LeftOuterJoin {
		rhsGroup, rhsEqualConds, rhsOtherConds, rhsJoinTypes, rhsHintInfo, rhsHasOuterJoin := extractJoinGroup(join.children[1])
		noExpand := false
		// If the filters of the outer join is related with multiple leaves of the outer join side. We don't reorder it for now.
		if join.JoinType == RightOuterJoin {
			extractedCols := make([]*expression.Column, 0, 8)
			expression.ExtractColumnsFromExpressions(extractedCols, join.OtherConditions, nil)
			expression.ExtractColumnsFromExpressions(extractedCols, join.RightConditions, nil)
			expression.ExtractColumnsFromExpressions(extractedCols, expression.ScalarFuncs2Exprs(join.EqualConditions), nil)
			affectedGroups := 0
			for i := range rhsGroup {
				for _, col := range extractedCols {
					if rhsGroup[i].Schema().Contains(col) {
						affectedGroups++
						break
					}
				}
				if affectedGroups > 1 {
					noExpand = true
					break
				}
			}
		}
		if noExpand {
			return []LogicalPlan{p}, nil, nil, nil, nil, false
		}
		group = append(group, rhsGroup...)
		eqEdges = append(eqEdges, rhsEqualConds...)
		otherConds = append(otherConds, rhsOtherConds...)
		joinTypes = append(joinTypes, rhsJoinTypes...)
		hintInfo = append(hintInfo, rhsHintInfo...)
		hasOuterJoin = hasOuterJoin || rhsHasOuterJoin
	} else {
		group = append(group, join.children[1])
	}

	eqEdges = append(eqEdges, join.EqualConditions...)
	otherConds = append(otherConds, join.OtherConditions...)
	otherConds = append(otherConds, join.LeftConditions...)
	otherConds = append(otherConds, join.RightConditions...)
	for range join.EqualConditions {
		joinTypes = append(joinTypes, join.JoinType)
	}
	return group, eqEdges, otherConds, joinTypes, hintInfo, hasOuterJoin
}

type joinReOrderSolver struct {
}

type jrNode struct {
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

	curJoinGroup, eqEdges, otherConds, joinTypes, hintInfo, hasOuterJoin := extractJoinGroup(p)
	if len(curJoinGroup) > 1 {
		for i := range curJoinGroup {
			curJoinGroup[i], err = s.optimizeRecursive(ctx, curJoinGroup[i], tracer)
			if err != nil {
				return nil, err
			}
		}
		originalSchema := p.Schema()

		// Not support outer join reorder when using the DP algorithm
		isSupportDP := true
		for _, joinType := range joinTypes {
			if joinType != InnerJoin {
				isSupportDP = false
				break
			}
		}

		baseGroupSolver := &baseSingleGroupJoinOrderSolver{
			ctx:        ctx,
			otherConds: otherConds,
			eqEdges:    eqEdges,
			joinTypes:  joinTypes,
		}

		joinGroupNum := len(curJoinGroup)
		useGreedy := joinGroupNum > ctx.GetSessionVars().TiDBOptJoinReorderThreshold || !isSupportDP

		leadingHintInfo, hasDiffLeadingHint := checkAndGenerateLeadingHint(hintInfo)
		if hasDiffLeadingHint {
			ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack("We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid"))
		}

		if leadingHintInfo != nil && leadingHintInfo.leadingJoinOrder != nil {
			if hasOuterJoin {
				ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack("leading hint is inapplicable when we have outer join"))
			} else {
				if useGreedy {
					ok, leftJoinGroup := baseGroupSolver.generateLeadingJoinGroup(curJoinGroup, leadingHintInfo)
					if !ok {
						ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack("leading hint is inapplicable, check if the leading hint table is valid"))
					} else {
						curJoinGroup = leftJoinGroup
					}
				} else {
					ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack("leading hint is inapplicable for the DP join reorder algorithm"))
				}
			}
		}

		if useGreedy {
			groupSolver := &joinReorderGreedySolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
			}
			p, err = groupSolver.solve(curJoinGroup, tracer)
		} else {
			dpSolver := &joinReorderDPSolver{
				baseSingleGroupJoinOrderSolver: baseGroupSolver,
			}
			dpSolver.newJoin = dpSolver.newJoinWithEdges
			p, err = dpSolver.solve(curJoinGroup, tracer)
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
	if len(curJoinGroup) == 1 && hintInfo != nil {
		ctx.GetSessionVars().StmtCtx.AppendWarning(ErrInternal.GenWithStack("leading hint is inapplicable, check the join type or the join algorithm hint"))
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

// checkAndGenerateLeadingHint used to check and generate the valid leading hint.
// We are allowed to use at most one leading hint in a join group. When more than one,
// all leading hints in the current join group will be invalid.
// For example: select /*+ leading(t3) */ * from (select /*+ leading(t1) */ t2.b from t1 join t2 on t1.a=t2.a) t4 join t3 on t4.b=t3.b
// The Join Group {t1, t2, t3} contains two leading hints includes leading(t3) and leading(t1).
// Although they are in different query blocks, they are conflicting.
// In addition, the table alias 't4' cannot be recognized because of the join group.
func checkAndGenerateLeadingHint(hintInfo []*tableHintInfo) (*tableHintInfo, bool) {
	leadingHintNum := len(hintInfo)
	var leadingHintInfo *tableHintInfo
	hasDiffLeadingHint := false
	if leadingHintNum > 0 {
		leadingHintInfo = hintInfo[0]
		// One join group has one leading hint at most. Check whether there are different join order hints.
		for i := 1; i < leadingHintNum; i++ {
			if hintInfo[i] != hintInfo[i-1] {
				hasDiffLeadingHint = true
				break
			}
		}
		if hasDiffLeadingHint {
			leadingHintInfo = nil
		}
	}
	return leadingHintInfo, hasDiffLeadingHint
}

// nolint:structcheck
type baseSingleGroupJoinOrderSolver struct {
	ctx              sessionctx.Context
	curJoinGroup     []*jrNode
	otherConds       []expression.Expression
	eqEdges          []*expression.ScalarFunction
	joinTypes        []JoinType
	leadingJoinGroup LogicalPlan
}

func (s *baseSingleGroupJoinOrderSolver) generateLeadingJoinGroup(curJoinGroup []LogicalPlan, hintInfo *tableHintInfo) (bool, []LogicalPlan) {
	var leadingJoinGroup []LogicalPlan
	leftJoinGroup := make([]LogicalPlan, len(curJoinGroup))
	copy(leftJoinGroup, curJoinGroup)
	for _, hintTbl := range hintInfo.leadingJoinOrder {
		for i, joinGroup := range leftJoinGroup {
			tableAlias := extractTableAlias(joinGroup, joinGroup.SelectBlockOffset())
			if tableAlias == nil {
				continue
			}
			if hintTbl.dbName.L == tableAlias.dbName.L && hintTbl.tblName.L == tableAlias.tblName.L && hintTbl.selectOffset == tableAlias.selectOffset {
				leadingJoinGroup = append(leadingJoinGroup, joinGroup)
				leftJoinGroup = append(leftJoinGroup[:i], leftJoinGroup[i+1:]...)
				break
			}
		}
	}
	if len(leadingJoinGroup) != len(hintInfo.leadingJoinOrder) || leadingJoinGroup == nil {
		return false, nil
	}
	leadingJoin := leadingJoinGroup[0]
	leadingJoinGroup = leadingJoinGroup[1:]
	for len(leadingJoinGroup) > 0 {
		var usedEdges []*expression.ScalarFunction
		var joinType JoinType
		leadingJoin, leadingJoinGroup[0], usedEdges, joinType = s.checkConnection(leadingJoin, leadingJoinGroup[0])
		leadingJoin, s.otherConds = s.makeJoin(leadingJoin, leadingJoinGroup[0], usedEdges, joinType)
		leadingJoinGroup = leadingJoinGroup[1:]
	}
	s.leadingJoinGroup = leadingJoin
	return true, leftJoinGroup
}

// generateJoinOrderNode used to derive the stats for the joinNodePlans and generate the jrNode groups based on the cost.
func (s *baseSingleGroupJoinOrderSolver) generateJoinOrderNode(joinNodePlans []LogicalPlan, tracer *joinReorderTrace) ([]*jrNode, error) {
	joinGroup := make([]*jrNode, 0, len(joinNodePlans))
	for _, node := range joinNodePlans {
		_, err := node.recursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		cost := s.baseNodeCumCost(node)
		joinGroup = append(joinGroup, &jrNode{
			p:       node,
			cumCost: cost,
		})
		tracer.appendLogicalJoinCost(node, cost)
	}
	return joinGroup, nil
}

// baseNodeCumCost calculate the cumulative cost of the node in the join group.
func (s *baseSingleGroupJoinOrderSolver) baseNodeCumCost(groupNode LogicalPlan) float64 {
	cost := groupNode.statsInfo().RowCount
	for _, child := range groupNode.Children() {
		cost += s.baseNodeCumCost(child)
	}
	return cost
}

// checkConnection used to check whether two nodes have equal conditions or not.
func (s *baseSingleGroupJoinOrderSolver) checkConnection(leftPlan, rightPlan LogicalPlan) (leftNode, rightNode LogicalPlan, usedEdges []*expression.ScalarFunction, joinType JoinType) {
	joinType = InnerJoin
	leftNode, rightNode = leftPlan, rightPlan
	for idx, edge := range s.eqEdges {
		lCol := edge.GetArgs()[0].(*expression.Column)
		rCol := edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) && rightPlan.Schema().Contains(rCol) {
			joinType = s.joinTypes[idx]
			usedEdges = append(usedEdges, edge)
		} else if rightPlan.Schema().Contains(lCol) && leftPlan.Schema().Contains(rCol) {
			joinType = s.joinTypes[idx]
			if joinType != InnerJoin {
				rightNode, leftNode = leftPlan, rightPlan
				usedEdges = append(usedEdges, edge)
			} else {
				newSf := expression.NewFunctionInternal(s.ctx, ast.EQ, edge.GetType(), rCol, lCol).(*expression.ScalarFunction)
				usedEdges = append(usedEdges, newSf)
			}
		}
	}
	return
}

// makeJoin build join tree for the nodes which have equal conditions to connect them.
func (s *baseSingleGroupJoinOrderSolver) makeJoin(leftPlan, rightPlan LogicalPlan, eqEdges []*expression.ScalarFunction, joinType JoinType) (LogicalPlan, []expression.Expression) {
	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
	var otherConds []expression.Expression
	var leftConds []expression.Expression
	var rightConds []expression.Expression
	mergedSchema := expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema())

	remainOtherConds, leftConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, leftPlan.Schema()) && !expression.ExprFromSchema(expr, rightPlan.Schema())
	})
	remainOtherConds, rightConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, rightPlan.Schema()) && !expression.ExprFromSchema(expr, leftPlan.Schema())
	})
	remainOtherConds, otherConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, mergedSchema)
	})
	return s.newJoinWithEdges(leftPlan, rightPlan, eqEdges, otherConds, leftConds, rightConds, joinType), remainOtherConds
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
