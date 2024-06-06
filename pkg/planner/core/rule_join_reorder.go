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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/tracing"
)

// extractJoinGroup extracts all the join nodes connected with continuous
// Joins to construct a join group. This join group is further used to
// construct a new join order based on a reorder algorithm.
//
// For example: "InnerJoin(InnerJoin(a, b), LeftJoin(c, d))"
// results in a join group {a, b, c, d}.
func extractJoinGroup(p base.LogicalPlan) *joinGroupResult {
	joinMethodHintInfo := make(map[int]*joinMethodHint)
	var (
		group             []base.LogicalPlan
		joinOrderHintInfo []*h.PlanHints
		eqEdges           []*expression.ScalarFunction
		otherConds        []expression.Expression
		joinTypes         []*joinTypeWithExtMsg
		hasOuterJoin      bool
	)
	join, isJoin := p.(*LogicalJoin)
	if isJoin && join.preferJoinOrder {
		// When there is a leading hint, the hint may not take effect for other reasons.
		// For example, the join type is cross join or straight join, or exists the join algorithm hint, etc.
		// We need to return the hint information to warn
		joinOrderHintInfo = append(joinOrderHintInfo, join.hintInfo)
	}
	// If the variable `tidb_opt_advanced_join_hint` is false and the join node has the join method hint, we will not split the current join node to join reorder process.
	if !isJoin || (join.preferJoinType > uint(0) && !p.SCtx().GetSessionVars().EnableAdvancedJoinHint) || join.StraightJoin ||
		(join.JoinType != InnerJoin && join.JoinType != LeftOuterJoin && join.JoinType != RightOuterJoin) ||
		((join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin) && join.EqualConditions == nil) {
		if joinOrderHintInfo != nil {
			// The leading hint can not work for some reasons. So clear it in the join node.
			join.hintInfo = nil
		}
		return &joinGroupResult{
			group:              []base.LogicalPlan{p},
			joinOrderHintInfo:  joinOrderHintInfo,
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}
	}
	// If the session var is set to off, we will still reject the outer joins.
	if !p.SCtx().GetSessionVars().EnableOuterJoinReorder && (join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin) {
		return &joinGroupResult{
			group:              []base.LogicalPlan{p},
			joinOrderHintInfo:  joinOrderHintInfo,
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}
	}
	// `leftHasHint` and `rightHasHint` are used to record whether the left child and right child are set by the join method hint.
	leftHasHint, rightHasHint := false, false
	if isJoin && p.SCtx().GetSessionVars().EnableAdvancedJoinHint && join.preferJoinType > uint(0) {
		// If the current join node has the join method hint, we should store the hint information and restore it when we have finished the join reorder process.
		if join.leftPreferJoinType > uint(0) {
			joinMethodHintInfo[join.Children()[0].ID()] = &joinMethodHint{join.leftPreferJoinType, join.hintInfo}
			leftHasHint = true
		}
		if join.rightPreferJoinType > uint(0) {
			joinMethodHintInfo[join.Children()[1].ID()] = &joinMethodHint{join.rightPreferJoinType, join.hintInfo}
			rightHasHint = true
		}
	}
	hasOuterJoin = hasOuterJoin || (join.JoinType != InnerJoin)
	// If the left child has the hint, it means there are some join method hints want to specify the join method based on the left child.
	// For example: `select .. from t1 join t2 join (select .. from t3 join t4) t5 where ..;` If there are some join method hints related to `t5`, we can't split `t5` into `t3` and `t4`.
	// So we don't need to split the left child part. The right child part is the same.
	if join.JoinType != RightOuterJoin && !leftHasHint {
		lhsJoinGroupResult := extractJoinGroup(join.Children()[0])
		lhsGroup, lhsEqualConds, lhsOtherConds, lhsJoinTypes, lhsJoinOrderHintInfo, lhsJoinMethodHintInfo, lhsHasOuterJoin := lhsJoinGroupResult.group, lhsJoinGroupResult.eqEdges, lhsJoinGroupResult.otherConds, lhsJoinGroupResult.joinTypes, lhsJoinGroupResult.joinOrderHintInfo, lhsJoinGroupResult.joinMethodHintInfo, lhsJoinGroupResult.hasOuterJoin
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
			return &joinGroupResult{
				group:              []base.LogicalPlan{p},
				basicJoinGroupInfo: &basicJoinGroupInfo{},
			}
		}
		group = append(group, lhsGroup...)
		eqEdges = append(eqEdges, lhsEqualConds...)
		otherConds = append(otherConds, lhsOtherConds...)
		joinTypes = append(joinTypes, lhsJoinTypes...)
		joinOrderHintInfo = append(joinOrderHintInfo, lhsJoinOrderHintInfo...)
		for ID, joinMethodHint := range lhsJoinMethodHintInfo {
			joinMethodHintInfo[ID] = joinMethodHint
		}
		hasOuterJoin = hasOuterJoin || lhsHasOuterJoin
	} else {
		group = append(group, join.Children()[0])
	}

	// You can see the comments in the upside part which we try to split the left child part. It's the same here.
	if join.JoinType != LeftOuterJoin && !rightHasHint {
		rhsJoinGroupResult := extractJoinGroup(join.Children()[1])
		rhsGroup, rhsEqualConds, rhsOtherConds, rhsJoinTypes, rhsJoinOrderHintInfo, rhsJoinMethodHintInfo, rhsHasOuterJoin := rhsJoinGroupResult.group, rhsJoinGroupResult.eqEdges, rhsJoinGroupResult.otherConds, rhsJoinGroupResult.joinTypes, rhsJoinGroupResult.joinOrderHintInfo, rhsJoinGroupResult.joinMethodHintInfo, rhsJoinGroupResult.hasOuterJoin
		noExpand := false
		// If the filters of the outer join is related with multiple leaves of the outer join side. We don't reorder it for now.
		if join.JoinType == RightOuterJoin {
			extractedCols := make([]*expression.Column, 0, 8)
			extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, join.OtherConditions, nil)
			extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, join.RightConditions, nil)
			extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, expression.ScalarFuncs2Exprs(join.EqualConditions), nil)
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
			return &joinGroupResult{
				group:              []base.LogicalPlan{p},
				basicJoinGroupInfo: &basicJoinGroupInfo{},
			}
		}
		group = append(group, rhsGroup...)
		eqEdges = append(eqEdges, rhsEqualConds...)
		otherConds = append(otherConds, rhsOtherConds...)
		joinTypes = append(joinTypes, rhsJoinTypes...)
		joinOrderHintInfo = append(joinOrderHintInfo, rhsJoinOrderHintInfo...)
		for ID, joinMethodHint := range rhsJoinMethodHintInfo {
			joinMethodHintInfo[ID] = joinMethodHint
		}
		hasOuterJoin = hasOuterJoin || rhsHasOuterJoin
	} else {
		group = append(group, join.Children()[1])
	}

	eqEdges = append(eqEdges, join.EqualConditions...)
	tmpOtherConds := make(expression.CNFExprs, 0, len(join.OtherConditions)+len(join.LeftConditions)+len(join.RightConditions))
	tmpOtherConds = append(tmpOtherConds, join.OtherConditions...)
	tmpOtherConds = append(tmpOtherConds, join.LeftConditions...)
	tmpOtherConds = append(tmpOtherConds, join.RightConditions...)
	if join.JoinType == LeftOuterJoin || join.JoinType == RightOuterJoin || join.JoinType == LeftOuterSemiJoin || join.JoinType == AntiLeftOuterSemiJoin {
		for range join.EqualConditions {
			abType := &joinTypeWithExtMsg{JoinType: join.JoinType}
			// outer join's other condition should be bound with the connecting edge.
			// although we bind the outer condition to **anyone** of the join type, it will be extracted **only once** when make a new join.
			abType.outerBindCondition = tmpOtherConds
			joinTypes = append(joinTypes, abType)
		}
	} else {
		for range join.EqualConditions {
			abType := &joinTypeWithExtMsg{JoinType: join.JoinType}
			joinTypes = append(joinTypes, abType)
		}
		otherConds = append(otherConds, tmpOtherConds...)
	}
	return &joinGroupResult{
		group:             group,
		hasOuterJoin:      hasOuterJoin,
		joinOrderHintInfo: joinOrderHintInfo,
		basicJoinGroupInfo: &basicJoinGroupInfo{
			eqEdges:            eqEdges,
			otherConds:         otherConds,
			joinTypes:          joinTypes,
			joinMethodHintInfo: joinMethodHintInfo,
		},
	}
}

type joinReOrderSolver struct {
}

type jrNode struct {
	p       base.LogicalPlan
	cumCost float64
}

type joinTypeWithExtMsg struct {
	JoinType
	outerBindCondition []expression.Expression
}

func (s *joinReOrderSolver) optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	tracer := &joinReorderTrace{cost: map[string]float64{}, opt: opt}
	tracer.traceJoinReorder(p)
	p, err := s.optimizeRecursive(p.SCtx(), p, tracer)
	tracer.traceJoinReorder(p)
	appendJoinReorderTraceStep(tracer, p, opt)
	return p, planChanged, err
}

// optimizeRecursive recursively collects join groups and applies join reorder algorithm for each group.
func (s *joinReOrderSolver) optimizeRecursive(ctx base.PlanContext, p base.LogicalPlan, tracer *joinReorderTrace) (base.LogicalPlan, error) {
	if _, ok := p.(*LogicalCTE); ok {
		return p, nil
	}

	var err error

	result := extractJoinGroup(p)
	curJoinGroup, joinTypes, joinOrderHintInfo, hasOuterJoin := result.group, result.joinTypes, result.joinOrderHintInfo, result.hasOuterJoin
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
			if joinType.JoinType != InnerJoin {
				isSupportDP = false
				break
			}
		}

		baseGroupSolver := &baseSingleGroupJoinOrderSolver{
			ctx:                ctx,
			basicJoinGroupInfo: result.basicJoinGroupInfo,
		}

		joinGroupNum := len(curJoinGroup)
		useGreedy := joinGroupNum > ctx.GetSessionVars().TiDBOptJoinReorderThreshold || !isSupportDP

		leadingHintInfo, hasDiffLeadingHint := checkAndGenerateLeadingHint(joinOrderHintInfo)
		if hasDiffLeadingHint {
			ctx.GetSessionVars().StmtCtx.SetHintWarning(
				"We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid")
		}

		if leadingHintInfo != nil && leadingHintInfo.LeadingJoinOrder != nil {
			if useGreedy {
				ok, leftJoinGroup := baseGroupSolver.generateLeadingJoinGroup(curJoinGroup, leadingHintInfo, hasOuterJoin)
				if !ok {
					ctx.GetSessionVars().StmtCtx.SetHintWarning(
						"leading hint is inapplicable, check if the leading hint table is valid")
				} else {
					curJoinGroup = leftJoinGroup
				}
			} else {
				ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable for the DP join reorder algorithm")
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
				if !col.EqualColumn(originalSchema.Columns[i]) {
					schemaChanged = true
					break
				}
			}
		}
		if schemaChanged {
			proj := LogicalProjection{
				Exprs: expression.Column2Exprs(originalSchema.Columns),
			}.Init(p.SCtx(), p.QueryBlockOffset())
			// Clone the schema here, because the schema may be changed by column pruning rules.
			proj.SetSchema(originalSchema.Clone())
			proj.SetChildren(p)
			p = proj
		}
		return p, nil
	}
	if len(curJoinGroup) == 1 && joinOrderHintInfo != nil {
		ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check the join type or the join algorithm hint")
	}
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
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
func checkAndGenerateLeadingHint(hintInfo []*h.PlanHints) (*h.PlanHints, bool) {
	leadingHintNum := len(hintInfo)
	var leadingHintInfo *h.PlanHints
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

type joinMethodHint struct {
	preferredJoinMethod uint
	joinMethodHintInfo  *h.PlanHints
}

// basicJoinGroupInfo represents basic information for a join group in the join reorder process.
type basicJoinGroupInfo struct {
	eqEdges    []*expression.ScalarFunction
	otherConds []expression.Expression
	joinTypes  []*joinTypeWithExtMsg
	// `joinMethodHintInfo` is used to map the sub-plan's ID to the join method hint.
	// The sub-plan will join the join reorder process to build the new plan.
	// So after we have finished the join reorder process, we can reset the join method hint based on the sub-plan's ID.
	joinMethodHintInfo map[int]*joinMethodHint
}

type joinGroupResult struct {
	group             []base.LogicalPlan
	hasOuterJoin      bool
	joinOrderHintInfo []*h.PlanHints
	*basicJoinGroupInfo
}

// nolint:structcheck
type baseSingleGroupJoinOrderSolver struct {
	ctx              base.PlanContext
	curJoinGroup     []*jrNode
	leadingJoinGroup base.LogicalPlan
	*basicJoinGroupInfo
}

func (s *baseSingleGroupJoinOrderSolver) generateLeadingJoinGroup(curJoinGroup []base.LogicalPlan, hintInfo *h.PlanHints, hasOuterJoin bool) (bool, []base.LogicalPlan) {
	var leadingJoinGroup []base.LogicalPlan
	leftJoinGroup := make([]base.LogicalPlan, len(curJoinGroup))
	copy(leftJoinGroup, curJoinGroup)
	var queryBlockNames []ast.HintTable
	if p := s.ctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
		queryBlockNames = *p
	}
	for _, hintTbl := range hintInfo.LeadingJoinOrder {
		match := false
		for i, joinGroup := range leftJoinGroup {
			tableAlias := extractTableAlias(joinGroup, joinGroup.QueryBlockOffset())
			if tableAlias == nil {
				continue
			}
			if (hintTbl.DBName.L == tableAlias.DBName.L || hintTbl.DBName.L == "*") && hintTbl.TblName.L == tableAlias.TblName.L && hintTbl.SelectOffset == tableAlias.SelectOffset {
				match = true
				leadingJoinGroup = append(leadingJoinGroup, joinGroup)
				leftJoinGroup = append(leftJoinGroup[:i], leftJoinGroup[i+1:]...)
				break
			}
		}
		if match {
			continue
		}

		// consider query block alias: select /*+ leading(t1, t2) */ * from (select ...) t1, t2 ...
		groupIdx := -1
		for i, joinGroup := range leftJoinGroup {
			blockOffset := joinGroup.QueryBlockOffset()
			if blockOffset > 1 && blockOffset < len(queryBlockNames) {
				blockName := queryBlockNames[blockOffset]
				if hintTbl.DBName.L == blockName.DBName.L && hintTbl.TblName.L == blockName.TableName.L {
					// this can happen when multiple join groups are from the same block, for example:
					//   select /*+ leading(tx) */ * from (select * from t1, t2 ...) tx, ...
					// `tx` is split to 2 join groups `t1` and `t2`, and they have the same block offset.
					// TODO: currently we skip this case for simplification, we can support it in the future.
					if groupIdx != -1 {
						groupIdx = -1
						break
					}
					groupIdx = i
				}
			}
		}
		if groupIdx != -1 {
			leadingJoinGroup = append(leadingJoinGroup, leftJoinGroup[groupIdx])
			leftJoinGroup = append(leftJoinGroup[:groupIdx], leftJoinGroup[groupIdx+1:]...)
		}
	}
	if len(leadingJoinGroup) != len(hintInfo.LeadingJoinOrder) || leadingJoinGroup == nil {
		return false, nil
	}
	leadingJoin := leadingJoinGroup[0]
	leadingJoinGroup = leadingJoinGroup[1:]
	for len(leadingJoinGroup) > 0 {
		var usedEdges []*expression.ScalarFunction
		var joinType *joinTypeWithExtMsg
		leadingJoin, leadingJoinGroup[0], usedEdges, joinType = s.checkConnection(leadingJoin, leadingJoinGroup[0])
		if hasOuterJoin && usedEdges == nil {
			// If the joinGroups contain the outer join, we disable the cartesian product.
			return false, nil
		}
		leadingJoin, s.otherConds = s.makeJoin(leadingJoin, leadingJoinGroup[0], usedEdges, joinType)
		leadingJoinGroup = leadingJoinGroup[1:]
	}
	s.leadingJoinGroup = leadingJoin
	return true, leftJoinGroup
}

// generateJoinOrderNode used to derive the stats for the joinNodePlans and generate the jrNode groups based on the cost.
func (s *baseSingleGroupJoinOrderSolver) generateJoinOrderNode(joinNodePlans []base.LogicalPlan, tracer *joinReorderTrace) ([]*jrNode, error) {
	joinGroup := make([]*jrNode, 0, len(joinNodePlans))
	for _, node := range joinNodePlans {
		_, err := node.RecursiveDeriveStats(nil)
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
func (s *baseSingleGroupJoinOrderSolver) baseNodeCumCost(groupNode base.LogicalPlan) float64 {
	cost := groupNode.StatsInfo().RowCount
	for _, child := range groupNode.Children() {
		cost += s.baseNodeCumCost(child)
	}
	return cost
}

// checkConnection used to check whether two nodes have equal conditions or not.
func (s *baseSingleGroupJoinOrderSolver) checkConnection(leftPlan, rightPlan base.LogicalPlan) (leftNode, rightNode base.LogicalPlan, usedEdges []*expression.ScalarFunction, joinType *joinTypeWithExtMsg) {
	joinType = &joinTypeWithExtMsg{JoinType: InnerJoin}
	leftNode, rightNode = leftPlan, rightPlan
	for idx, edge := range s.eqEdges {
		lCol := edge.GetArgs()[0].(*expression.Column)
		rCol := edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) && rightPlan.Schema().Contains(rCol) {
			joinType = s.joinTypes[idx]
			usedEdges = append(usedEdges, edge)
		} else if rightPlan.Schema().Contains(lCol) && leftPlan.Schema().Contains(rCol) {
			joinType = s.joinTypes[idx]
			if joinType.JoinType != InnerJoin {
				rightNode, leftNode = leftPlan, rightPlan
				usedEdges = append(usedEdges, edge)
			} else {
				newSf := expression.NewFunctionInternal(s.ctx.GetExprCtx(), ast.EQ, edge.GetStaticType(), rCol, lCol).(*expression.ScalarFunction)

				// after creating the new EQ function, the 2 args might not be column anymore, for example `sf=sf(cast(col))`,
				// which breaks the assumption that join eq keys must be `col=col`, to handle this, inject 2 projections.
				_, isCol0 := newSf.GetArgs()[0].(*expression.Column)
				_, isCol1 := newSf.GetArgs()[1].(*expression.Column)
				if !isCol0 || !isCol1 {
					if !isCol0 {
						leftPlan, rCol = s.injectExpr(leftPlan, newSf.GetArgs()[0])
					}
					if !isCol1 {
						rightPlan, lCol = s.injectExpr(rightPlan, newSf.GetArgs()[1])
					}
					leftNode, rightNode = leftPlan, rightPlan
					newSf = expression.NewFunctionInternal(s.ctx.GetExprCtx(), ast.EQ, edge.GetStaticType(),
						rCol, lCol).(*expression.ScalarFunction)
				}
				usedEdges = append(usedEdges, newSf)
			}
		}
	}
	return
}

func (*baseSingleGroupJoinOrderSolver) injectExpr(p base.LogicalPlan, expr expression.Expression) (base.LogicalPlan, *expression.Column) {
	proj, ok := p.(*LogicalProjection)
	if !ok {
		proj = LogicalProjection{Exprs: cols2Exprs(p.Schema().Columns)}.Init(p.SCtx(), p.QueryBlockOffset())
		proj.SetSchema(p.Schema().Clone())
		proj.SetChildren(p)
	}
	return proj, proj.appendExpr(expr)
}

// makeJoin build join tree for the nodes which have equal conditions to connect them.
func (s *baseSingleGroupJoinOrderSolver) makeJoin(leftPlan, rightPlan base.LogicalPlan, eqEdges []*expression.ScalarFunction, joinType *joinTypeWithExtMsg) (base.LogicalPlan, []expression.Expression) {
	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
	var (
		otherConds []expression.Expression
		leftConds  []expression.Expression
		rightConds []expression.Expression

		// for outer bind conditions
		obOtherConds []expression.Expression
		obLeftConds  []expression.Expression
		obRightConds []expression.Expression
	)
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

	if joinType.JoinType == LeftOuterJoin || joinType.JoinType == RightOuterJoin || joinType.JoinType == LeftOuterSemiJoin || joinType.JoinType == AntiLeftOuterSemiJoin {
		// the original outer join's other conditions has been bound to the outer join Edge,
		// these remained other condition here shouldn't be appended to it because on-mismatch
		// logic will produce more append-null rows which is banned in original semantic.
		remainOtherConds = append(remainOtherConds, otherConds...) // nozero
		remainOtherConds = append(remainOtherConds, leftConds...)  // nozero
		remainOtherConds = append(remainOtherConds, rightConds...) // nozero
		otherConds = otherConds[:0]
		leftConds = leftConds[:0]
		rightConds = rightConds[:0]
	}
	if len(joinType.outerBindCondition) > 0 {
		remainOBOtherConds := make([]expression.Expression, len(joinType.outerBindCondition))
		copy(remainOBOtherConds, joinType.outerBindCondition)
		remainOBOtherConds, obLeftConds = expression.FilterOutInPlace(remainOBOtherConds, func(expr expression.Expression) bool {
			return expression.ExprFromSchema(expr, leftPlan.Schema()) && !expression.ExprFromSchema(expr, rightPlan.Schema())
		})
		remainOBOtherConds, obRightConds = expression.FilterOutInPlace(remainOBOtherConds, func(expr expression.Expression) bool {
			return expression.ExprFromSchema(expr, rightPlan.Schema()) && !expression.ExprFromSchema(expr, leftPlan.Schema())
		})
		// _ here make the linter happy.
		_, obOtherConds = expression.FilterOutInPlace(remainOBOtherConds, func(expr expression.Expression) bool {
			return expression.ExprFromSchema(expr, mergedSchema)
		})
		// case like: (A * B) left outer join C on (A.a = C.a && B.b > 0) will remain B.b > 0 in remainOBOtherConds (while this case
		// has been forbidden by: filters of the outer join is related with multiple leaves of the outer join side in #34603)
		// so noway here we got remainOBOtherConds remained.
	}
	return s.newJoinWithEdges(leftPlan, rightPlan, eqEdges,
		append(otherConds, obOtherConds...), append(leftConds, obLeftConds...), append(rightConds, obRightConds...), joinType.JoinType), remainOtherConds
}

// makeBushyJoin build bushy tree for the nodes which have no equal condition to connect them.
func (s *baseSingleGroupJoinOrderSolver) makeBushyJoin(cartesianJoinGroup []base.LogicalPlan) base.LogicalPlan {
	resultJoinGroup := make([]base.LogicalPlan, 0, (len(cartesianJoinGroup)+1)/2)
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
	// other conditions may be possible to exist across different cartesian join group, resolving cartesianJoin first then adding another selection.
	if len(s.otherConds) > 0 {
		additionSelection := LogicalSelection{
			Conditions: s.otherConds,
		}.Init(cartesianJoinGroup[0].SCtx(), cartesianJoinGroup[0].QueryBlockOffset())
		additionSelection.SetChildren(cartesianJoinGroup[0])
		cartesianJoinGroup[0] = additionSelection
	}
	return cartesianJoinGroup[0]
}

func (s *baseSingleGroupJoinOrderSolver) newCartesianJoin(lChild, rChild base.LogicalPlan) *LogicalJoin {
	offset := lChild.QueryBlockOffset()
	if offset != rChild.QueryBlockOffset() {
		offset = -1
	}
	join := LogicalJoin{
		JoinType:  InnerJoin,
		reordered: true,
	}.Init(s.ctx, offset)
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	s.setNewJoinWithHint(join)
	return join
}

func (s *baseSingleGroupJoinOrderSolver) newJoinWithEdges(lChild, rChild base.LogicalPlan,
	eqEdges []*expression.ScalarFunction, otherConds, leftConds, rightConds []expression.Expression, joinType JoinType) base.LogicalPlan {
	newJoin := s.newCartesianJoin(lChild, rChild)
	newJoin.EqualConditions = eqEdges
	newJoin.OtherConditions = otherConds
	newJoin.LeftConditions = leftConds
	newJoin.RightConditions = rightConds
	newJoin.JoinType = joinType
	return newJoin
}

// setNewJoinWithHint sets the join method hint for the join node.
// Before the join reorder process, we split the join node and collect the join method hint.
// And we record the join method hint and reset the hint after we have finished the join reorder process.
func (s *baseSingleGroupJoinOrderSolver) setNewJoinWithHint(newJoin *LogicalJoin) {
	lChild := newJoin.Children()[0]
	rChild := newJoin.Children()[1]
	if joinMethodHint, ok := s.joinMethodHintInfo[lChild.ID()]; ok {
		newJoin.leftPreferJoinType = joinMethodHint.preferredJoinMethod
		newJoin.hintInfo = joinMethodHint.joinMethodHintInfo
	}
	if joinMethodHint, ok := s.joinMethodHintInfo[rChild.ID()]; ok {
		newJoin.rightPreferJoinType = joinMethodHint.preferredJoinMethod
		newJoin.hintInfo = joinMethodHint.joinMethodHintInfo
	}
	newJoin.setPreferredJoinType()
}

// calcJoinCumCost calculates the cumulative cost of the join node.
func (*baseSingleGroupJoinOrderSolver) calcJoinCumCost(join base.LogicalPlan, lNode, rNode *jrNode) float64 {
	return join.StatsInfo().RowCount + lNode.cumCost + rNode.cumCost
}

func (*joinReOrderSolver) name() string {
	return "join_reorder"
}

func appendJoinReorderTraceStep(tracer *joinReorderTrace, plan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
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
		slices.Sort(joins)
		for i, join := range joins {
			if i > 0 {
				buffer.WriteString(",")
			}
			fmt.Fprintf(buffer, "[%s, cost:%v]", join, tracer.cost[join])
		}
		buffer.WriteString("]")
		return buffer.String()
	}
	opt.AppendStepToCurrent(plan.ID(), plan.TP(), reason, action)
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
	//nolint: prealloc
	var r []*tracing.PlanTrace
	for _, child := range t.Children {
		r = append(r, findRoots(child)...)
	}
	return r
}

type joinReorderTrace struct {
	opt     *optimizetrace.LogicalOptimizeOp
	initial string
	final   string
	cost    map[string]float64
}

func (t *joinReorderTrace) traceJoinReorder(p base.LogicalPlan) {
	if t == nil || t.opt == nil || t.opt.TracerIsNil() {
		return
	}
	if len(t.initial) > 0 {
		t.final = allJoinOrderToString(extractJoinAndDataSource(p.BuildPlanTrace()))
		return
	}
	t.initial = allJoinOrderToString(extractJoinAndDataSource(p.BuildPlanTrace()))
}

func (t *joinReorderTrace) appendLogicalJoinCost(join base.LogicalPlan, cost float64) {
	if t == nil || t.opt == nil || t.opt.TracerIsNil() {
		return
	}
	joinMapKey := allJoinOrderToString(extractJoinAndDataSource(join.BuildPlanTrace()))
	t.cost[joinMapKey] = cost
}
