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
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
<<<<<<< HEAD
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/tracing"
=======
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
)

// extractJoinGroup extracts all the join nodes connected with continuous
// Joins to construct a join group. This join group is further used to
// construct a new join order based on a reorder algorithm.
//
// For example: "InnerJoin(InnerJoin(a, b), LeftJoin(c, d))"
// results in a join group {a, b, c, d}.
func extractJoinGroup(p base.LogicalPlan) *joinGroupResult {
<<<<<<< HEAD
	joinMethodHintInfo := make(map[int]*joinMethodHint)
=======
	return extractJoinGroupImpl(p)
}

// extractJoinGroupImpl is the internal implementation of extractJoinGroup.
//
// It recursively extracts a join group from `p`: a set of leaf plans connected by a chain of
// reorderable joins, together with the join conditions/metadata needed by the join reorder solvers
// (eqEdges, otherConds, joinTypes, and hint info).
//
// When enabled via session variables, it can "look through" a limited set of unary operators while
// extracting the group (currently Selection/Projection; see the NOTE below).
//
// For Projection, we may best-effort inline safe projections on top of a Join and keep a derived
// column mapping (colExprMap) so join conditions that reference derived columns can be substituted
// back to their defining expressions. The colExprMap propagation is bottom-up:
// 1. First recursively process child nodes
// 2. Use child's returned colExprMap to substitute column references in current expressions
// 3. Build current node's colExprMap and return
// This approach is consistent with rule_eliminate_projection.go.
func extractJoinGroupImpl(p base.LogicalPlan) *joinGroupResult {
	// NOTE: We only support extracting join groups through a single Selection/Projection layer for now.
	// TODO: Support stacked unary operators like Projection->Selection->Join or Selection->Projection->Join.
	// Check if the current plan is a Selection. If its child is a join, add the selection conditions
	// to otherConds and continue extracting the join group from the child.
	// Join reorder may distribute/push down conditions during constructing the new join tree.
	// For volatile, side-effect, or otherwise non-deterministic expressions, moving them can
	// change evaluation times/orders or observable results, so we skip reordering through
	// Selection in such cases.
	if selection, isSelection := p.(*logicalop.LogicalSelection); isSelection && p.SCtx().GetSessionVars().TiDBOptJoinReorderThroughSel &&
		!slices.ContainsFunc(selection.Conditions, func(expr expression.Expression) bool {
			return expression.IsMutableEffectsExpr(expr) || expression.CheckNonDeterministic(expr)
		}) {
		child := selection.Children()[0]
		if _, isChildJoin := child.(*logicalop.LogicalJoin); isChildJoin {
			childResult := extractJoinGroup(child)
			selectionConds := selection.Conditions
			if len(childResult.colExprMap) > 0 {
				selectionConds = joinorder.SubstituteColsInExprs(selectionConds, childResult.colExprMap)
			}
			childResult.otherConds = append(childResult.otherConds, selectionConds...)
			return childResult
		}
	} else if proj, isProj := p.(*logicalop.LogicalProjection); isProj && p.SCtx().GetSessionVars().TiDBOptJoinReorderThroughProj {
		// Best-effort: look through a safe Projection on top of a Join when enabled.
		// TODO: This only handles a single projection directly on top of a join. A stacked pattern
		// like Proj -> Proj -> Join still falls back here because the outer projection cannot reuse
		// the inner projection's join-group extraction/inlining result yet. Optimize this in a
		// follow-up if we want better support for double-projection pipelines.
		if result, handled := tryInlineProjectionForJoinGroup(p, proj); handled {
			return result
		}
	}

	joinMethodHintInfo := make(map[int]*joinorder.JoinMethodHint)
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
	var (
		group             []base.LogicalPlan
		joinOrderHintInfo []*h.PlanHints
		eqEdges           []*expression.ScalarFunction
		otherConds        []expression.Expression
		joinTypes         []*joinTypeWithExtMsg
		hasOuterJoin      bool
	)

	join, isJoin := p.(*logicalop.LogicalJoin)
	if isJoin && join.PreferJoinOrder {
		// When there is a leading hint, the hint may not take effect for other reasons.
		// For example, the join type is cross join or straight join, or exists the join algorithm hint, etc.
		// We need to return the hint information to warn
		joinOrderHintInfo = append(joinOrderHintInfo, join.HintInfo)
	}
	// If the variable `tidb_opt_advanced_join_hint` is false and the join node has the join method hint, we will not split the current join node to join reorder process.
	if !isJoin || (join.PreferJoinType > uint(0) && !p.SCtx().GetSessionVars().EnableAdvancedJoinHint) || join.StraightJoin ||
		(join.JoinType != logicalop.InnerJoin && join.JoinType != logicalop.LeftOuterJoin && join.JoinType != logicalop.RightOuterJoin) ||
		((join.JoinType == logicalop.LeftOuterJoin || join.JoinType == logicalop.RightOuterJoin) && join.EqualConditions == nil) ||
		// with NullEQ in the EQCond, the join order needs to consider the transitivity of null and avoid the wrong result.
		// so we skip the join order when to meet the NullEQ in the EQCond
		(slices.ContainsFunc(join.EqualConditions, func(e *expression.ScalarFunction) bool {
			return e.FuncName.L == ast.NullEQ
		})) {
		if joinOrderHintInfo != nil {
			// The leading hint can not work for some reasons. So clear it in the join node.
			join.HintInfo = nil
		}
		return &joinGroupResult{
			group:              []base.LogicalPlan{p},
			joinOrderHintInfo:  joinOrderHintInfo,
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}
	}
	// If the session var is set to off, we will still reject the outer joins.
	if !p.SCtx().GetSessionVars().EnableOuterJoinReorder && (join.JoinType == logicalop.LeftOuterJoin || join.JoinType == logicalop.RightOuterJoin) {
		return &joinGroupResult{
			group:              []base.LogicalPlan{p},
			joinOrderHintInfo:  joinOrderHintInfo,
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}
	}
	// `leftHasHint` and `rightHasHint` are used to record whether the left child and right child are set by the join method hint.
	leftHasHint, rightHasHint := false, false
	if isJoin && p.SCtx().GetSessionVars().EnableAdvancedJoinHint && join.PreferJoinType > uint(0) {
		// If the current join node has the join method hint, we should store the hint information and restore it when we have finished the join reorder process.
		if join.LeftPreferJoinType > uint(0) {
			joinMethodHintInfo[join.Children()[0].ID()] = &joinMethodHint{join.LeftPreferJoinType, join.HintInfo}
			leftHasHint = true
		}
		if join.RightPreferJoinType > uint(0) {
			joinMethodHintInfo[join.Children()[1].ID()] = &joinMethodHint{join.RightPreferJoinType, join.HintInfo}
			rightHasHint = true
		}
	}
<<<<<<< HEAD
	hasOuterJoin = hasOuterJoin || (join.JoinType != logicalop.InnerJoin)
	// If the left child has the hint, it means there are some join method hints want to specify the join method based on the left child.
	// For example: `select .. from t1 join t2 join (select .. from t3 join t4) t5 where ..;` If there are some join method hints related to `t5`, we can't split `t5` into `t3` and `t4`.
	// So we don't need to split the left child part. The right child part is the same.
	if join.JoinType != logicalop.RightOuterJoin && !leftHasHint {
		lhsJoinGroupResult := extractJoinGroup(join.Children()[0])
		lhsGroup, lhsEqualConds, lhsOtherConds, lhsJoinTypes, lhsJoinOrderHintInfo, lhsJoinMethodHintInfo, lhsHasOuterJoin := lhsJoinGroupResult.group, lhsJoinGroupResult.eqEdges, lhsJoinGroupResult.otherConds, lhsJoinGroupResult.joinTypes, lhsJoinGroupResult.joinOrderHintInfo, lhsJoinGroupResult.joinMethodHintInfo, lhsJoinGroupResult.hasOuterJoin
		noExpand := false
		// If the filters of the outer join is related with multiple leaves of the outer join side. We don't reorder it for now.
		if join.JoinType == logicalop.LeftOuterJoin {
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
=======
	var colExprMap map[int64]expression.Expression

	hasOuterJoin = hasOuterJoin || (join.JoinType != base.InnerJoin)
	// If the left child has the hint, it means there are some join method hints want to specify the join method based on the left child.
	// For example: `select .. from t1 join t2 join (select .. from t3 join t4) t5 where ..;` If there are some join method hints related to `t5`, we can't split `t5` into `t3` and `t4`.
	// So we don't need to split the left child part. The right child part is the same.

	// Check if left child should be preserved due to LEADING hint reference
	leftShouldPreserve := currentLeadingHint != nil && joinorder.IsDerivedTableInLeadingHint(join.Children()[0], currentLeadingHint)

	if join.JoinType != base.RightOuterJoin && !leftHasHint && !leftShouldPreserve {
		lhsJoinGroupResult := extractJoinGroupImpl(join.Children()[0])
		lhsGroup, lhsEqualConds, lhsOtherConds, lhsJoinTypes, lhsJoinOrderHintInfo, lhsJoinMethodHintInfo, lhsHasOuterJoin, lhsColExprMap := lhsJoinGroupResult.group, lhsJoinGroupResult.eqEdges, lhsJoinGroupResult.otherConds, lhsJoinGroupResult.joinTypes, lhsJoinGroupResult.joinOrderHintInfo, lhsJoinGroupResult.joinMethodHintInfo, lhsJoinGroupResult.hasOuterJoin, lhsJoinGroupResult.colExprMap
		// If the filters of the outer join is related with multiple leaves of the outer join side. We don't reorder it for now.
		if join.JoinType == base.LeftOuterJoin &&
			joinorder.OuterJoinSideFiltersTouchMultipleLeaves(join, lhsGroup, lhsColExprMap, true) {
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
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
		colExprMap = mergeMap(colExprMap, lhsColExprMap)
	} else {
		group = append(group, join.Children()[0])
	}

	// You can see the comments in the upside part which we try to split the left child part. It's the same here.
<<<<<<< HEAD
	if join.JoinType != logicalop.LeftOuterJoin && !rightHasHint {
		rhsJoinGroupResult := extractJoinGroup(join.Children()[1])
		rhsGroup, rhsEqualConds, rhsOtherConds, rhsJoinTypes, rhsJoinOrderHintInfo, rhsJoinMethodHintInfo, rhsHasOuterJoin := rhsJoinGroupResult.group, rhsJoinGroupResult.eqEdges, rhsJoinGroupResult.otherConds, rhsJoinGroupResult.joinTypes, rhsJoinGroupResult.joinOrderHintInfo, rhsJoinGroupResult.joinMethodHintInfo, rhsJoinGroupResult.hasOuterJoin
		noExpand := false
		// If the filters of the outer join is related with multiple leaves of the outer join side. We don't reorder it for now.
		if join.JoinType == logicalop.RightOuterJoin {
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
=======
	if join.JoinType != base.LeftOuterJoin && !rightHasHint && !rightShouldPreserve {
		rhsJoinGroupResult := extractJoinGroupImpl(join.Children()[1])
		rhsGroup, rhsEqualConds, rhsOtherConds, rhsJoinTypes, rhsJoinOrderHintInfo, rhsJoinMethodHintInfo, rhsHasOuterJoin, rhsColExprMap := rhsJoinGroupResult.group, rhsJoinGroupResult.eqEdges, rhsJoinGroupResult.otherConds, rhsJoinGroupResult.joinTypes, rhsJoinGroupResult.joinOrderHintInfo, rhsJoinGroupResult.joinMethodHintInfo, rhsJoinGroupResult.hasOuterJoin, rhsJoinGroupResult.colExprMap
		// If the filters of the outer join is related with multiple leaves of the outer join side. We don't reorder it for now.
		if join.JoinType == base.RightOuterJoin &&
			joinorder.OuterJoinSideFiltersTouchMultipleLeaves(join, rhsGroup, rhsColExprMap, false) {
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
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
		colExprMap = mergeMap(colExprMap, rhsColExprMap)
	} else {
		group = append(group, join.Children()[1])
	}

	eqEdges = append(eqEdges, join.EqualConditions...)
	tmpOtherConds := make(expression.CNFExprs, 0, len(join.OtherConditions)+len(join.LeftConditions)+len(join.RightConditions))
	tmpOtherConds = append(tmpOtherConds, join.OtherConditions...)
	tmpOtherConds = append(tmpOtherConds, join.LeftConditions...)
	tmpOtherConds = append(tmpOtherConds, join.RightConditions...)
	if join.JoinType == logicalop.LeftOuterJoin || join.JoinType == logicalop.RightOuterJoin || join.JoinType == logicalop.LeftOuterSemiJoin || join.JoinType == logicalop.AntiLeftOuterSemiJoin {
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
<<<<<<< HEAD
=======

	// If we have colExprMap from children (projections were inlined), substitute derived columns in edges
	if len(colExprMap) > 0 {
		eqEdges = joinorder.SubstituteColsInEqEdges(eqEdges, colExprMap)
		// TODO: When a derived column (e.g., t2.b * 2) is substituted in otherConds and also
		// appears in the output projection, the expression may be computed twice. Consider
		// introducing common subexpression elimination or referencing the computed column
		// instead of duplicating the expression. Example:
		//   dt.doubled_b > 100 is substituted to (t2.b * 2) > 100
		//   while Projection also computes: t2.b * 2 -> Column#X
		// Ideally, the filter should reference Column#X instead of recomputing.
		otherConds = joinorder.SubstituteColsInExprs(otherConds, colExprMap)
		// Also substitute in outerBindCondition for outer joins
		for _, jt := range joinTypes {
			if jt.outerBindCondition != nil {
				jt.outerBindCondition = joinorder.SubstituteColsInExprs(jt.outerBindCondition, colExprMap)
			}
		}
	}
	var nullExtendedSchema *expression.Schema
	if len(nullExtendedCols) > 0 {
		nullExtendedSchema = expression.NewSchema(nullExtendedCols...)
	}
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
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
		colExprMap: colExprMap,
	}
}

// JoinReOrderSolver is used to reorder the join nodes in a logical plan.
type JoinReOrderSolver struct {
}

type jrNode struct {
	p       base.LogicalPlan
	cumCost float64
}

type joinTypeWithExtMsg struct {
	logicalop.JoinType
	outerBindCondition []expression.Expression
}

// Optimize implements the base.LogicalOptRule.<0th> interface.
func (s *JoinReOrderSolver) Optimize(_ context.Context, p base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	tracer := &joinReorderTrace{cost: map[string]float64{}, opt: opt}
	tracer.traceJoinReorder(p)
	p, err := s.optimizeRecursive(p.SCtx(), p, tracer)
	tracer.traceJoinReorder(p)
	appendJoinReorderTraceStep(tracer, p, opt)
	return p, planChanged, err
}

// optimizeRecursive recursively collects join groups and applies join reorder algorithm for each group.
func (s *JoinReOrderSolver) optimizeRecursive(ctx base.PlanContext, p base.LogicalPlan, tracer *joinReorderTrace) (base.LogicalPlan, error) {
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return p, nil
	}

	var err error

	var result *joinGroupResult
	if _, ok := p.(*logicalop.LogicalJoin); ok {
		result = extractJoinGroup(p)
		curJoinGroup, joinTypes, joinOrderHintInfo, hasOuterJoin := result.group, result.joinTypes, result.joinOrderHintInfo, result.hasOuterJoin
		if len(curJoinGroup) > 1 {
			for i := range curJoinGroup {
				curJoinGroup[i], err = s.optimizeRecursive(ctx, curJoinGroup[i], tracer)
				if err != nil {
					return nil, err
				}
			}
			origJoinRoot := p
			originalSchema := p.Schema()
			fallbackOnErr := func(err error) (base.LogicalPlan, error) {
				if ctx.GetSessionVars().TiDBOptJoinReorderThroughProj {
					// This optimization is best-effort. If anything goes wrong, rerun join reorder
					// on the current recursively optimized subtree with through-projection extraction
					// disabled, so Projections are treated as atomic leaves again.
					saved := ctx.GetSessionVars().TiDBOptJoinReorderThroughProj
					ctx.GetSessionVars().TiDBOptJoinReorderThroughProj = false
					defer func() { ctx.GetSessionVars().TiDBOptJoinReorderThroughProj = saved }()
					return s.optimizeRecursive(ctx, origJoinRoot)
				}
				return nil, err
			}

			// Not support outer join reorder when using the DP algorithm
			isSupportDP := true
			for _, joinType := range joinTypes {
				if joinType.JoinType != logicalop.InnerJoin {
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
					ok, leftJoinGroup := baseGroupSolver.generateLeadingJoinGroup(curJoinGroup, leadingHintInfo, hasOuterJoin, tracer.opt)
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
				return fallbackOnErr(err)
			}
<<<<<<< HEAD
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
				proj := logicalop.LogicalProjection{
					Exprs: expression.Column2Exprs(originalSchema.Columns),
				}.Init(p.SCtx(), p.QueryBlockOffset())
				// Clone the schema here, because the schema may be changed by column pruning rules.
				proj.SetSchema(originalSchema.Clone())
				proj.SetChildren(p)
				p = proj
=======
			p, err = restoreSchemaIfChanged(p, originalSchema, result.colExprMap)
			if err != nil {
				return fallbackOnErr(err)
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
			}
			return p, nil
		}
		if len(curJoinGroup) == 1 && joinOrderHintInfo != nil {
			ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check the join type or the join algorithm hint")
		}
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

<<<<<<< HEAD
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
=======
// restoreSchemaIfChanged restores the original schema of a join root after join reorder.
//
// Join reorder may inject projections to materialize join keys, which can change the output schema.
// For correctness, we wrap the reordered plan with a Projection to match the original schema.
//
// When projection inlining is enabled, `colExprMap` records derived column definitions. In that
// case, we reconstruct the original output columns by substituting derived columns with their
// defining expressions.
func restoreSchemaIfChanged(
	p base.LogicalPlan,
	originalSchema *expression.Schema,
	colExprMap map[int64]expression.Expression,
) (base.LogicalPlan, error) {
	if p == nil || originalSchema == nil {
		return p, nil
	}

	// Fast path: schemas are identical (same columns in the same order).
	schemaChanged := false
	curCols := p.Schema().Columns
	origCols := originalSchema.Columns
	if len(curCols) != len(origCols) {
		schemaChanged = true
	} else {
		for i, col := range curCols {
			if !col.EqualColumn(origCols[i]) {
				schemaChanged = true
				break
			}
		}
	}
	if !schemaChanged {
		return p, nil
	}

	// Build projection expressions.
	// When projections were inlined (colExprMap exists), we need to substitute derived columns
	// with their original expressions.
	var projExprs []expression.Expression
	if len(colExprMap) > 0 {
		projExprs = make([]expression.Expression, len(origCols))
		for i, col := range origCols {
			if expr, ok := colExprMap[col.UniqueID]; ok {
				// This is a derived column, use its defining expression.
				projExprs[i] = expr
				continue
			}

			// For non-derived columns, we should be able to reference them from the new join schema.
			// If not, the inlining mapping is incomplete, and we conservatively return an error so
			// caller can fallback to the non-inlining behavior.
			if !p.Schema().Contains(col) {
				return nil, plannererrors.ErrInternal.GenWithStack("join reorder: schema restore mapping missing after projection inlining")
			}
			projExprs[i] = col
		}
	} else {
		projExprs = expression.Column2Exprs(origCols)
	}

	proj := logicalop.LogicalProjection{
		Exprs: projExprs,
	}.Init(p.SCtx(), p.QueryBlockOffset())
	// Clone the schema here, because the schema may be changed by column pruning rules.
	proj.SetSchema(originalSchema.Clone())
	proj.SetChildren(p)
	return proj, nil
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
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

	// colExprMap maps derived column UniqueID to its defining expression.
	// When a projection is inlined, this records the mapping so parent joins
	// can substitute references to derived columns back to their original expressions.
	// key: Column.UniqueID from the projection's output schema
	// value: the expression used to compute that column (from projection.Exprs)
	colExprMap map[int64]expression.Expression
}

// nolint:structcheck
type baseSingleGroupJoinOrderSolver struct {
	ctx              base.PlanContext
	curJoinGroup     []*jrNode
	leadingJoinGroup base.LogicalPlan
	*basicJoinGroupInfo
}

<<<<<<< HEAD
func (s *baseSingleGroupJoinOrderSolver) generateLeadingJoinGroup(curJoinGroup []base.LogicalPlan, hintInfo *h.PlanHints, hasOuterJoin bool, opt *optimizetrace.LogicalOptimizeOp) (bool, []base.LogicalPlan) {
	var leadingJoinGroup []base.LogicalPlan
=======
func mergeMap[K comparable, V any](dst map[K]V, src map[K]V) map[K]V {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = make(map[K]V, len(src))
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// generateLeadingJoinGroup processes both flat and nested leading hints through the unified LeadingList structure
func (s *baseSingleGroupJoinOrderSolver) generateLeadingJoinGroup(
	curJoinGroup []base.LogicalPlan, hintInfo *h.PlanHints, hasOuterJoin bool,
) (bool, []base.LogicalPlan, error) {
	if hintInfo == nil || hintInfo.LeadingList == nil {
		return false, nil, nil
	}
	// Leading hint processing can partially build join trees and consume otherConds.
	// If the hint is inapplicable, restore original otherConds to avoid losing filters.
	origOtherConds := slices.Clone(s.otherConds)
	// Use the unified nested processing for both flat and nested structures
	ok, remaining, err := s.generateNestedLeadingJoinGroup(curJoinGroup, hintInfo.LeadingList, hasOuterJoin)
	if err != nil {
		s.otherConds = origOtherConds
		return false, nil, err
	}
	if !ok {
		s.otherConds = origOtherConds
	}
	return ok, remaining, nil
}

// generateNestedLeadingJoinGroup processes both flat and nested LEADING hint structures
func (s *baseSingleGroupJoinOrderSolver) generateNestedLeadingJoinGroup(
	curJoinGroup []base.LogicalPlan, leadingList *ast.LeadingList, hasOuterJoin bool,
) (bool, []base.LogicalPlan, error) {
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
	leftJoinGroup := make([]base.LogicalPlan, len(curJoinGroup))
	copy(leftJoinGroup, curJoinGroup)
	var queryBlockNames []ast.HintTable
	if p := s.ctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
		queryBlockNames = *p
	}
	for _, hintTbl := range hintInfo.LeadingJoinOrder {
		match := false
		for i, joinGroup := range leftJoinGroup {
			tableAlias := util.ExtractTableAlias(joinGroup, joinGroup.QueryBlockOffset())
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
		leadingJoin, s.otherConds = s.makeJoin(leadingJoin, leadingJoinGroup[0], usedEdges, joinType, opt)
		leadingJoinGroup = leadingJoinGroup[1:]
	}
	s.leadingJoinGroup = leadingJoin
<<<<<<< HEAD
	return true, leftJoinGroup
=======
	return true, remainingGroup, nil
}

// connectJoinNodes handles joining two subplans, performing connection checks
// and enforcing the outer join constraint.
// This function is only used by the leading hint processing path.
func (s *baseSingleGroupJoinOrderSolver) connectJoinNodes(
	currentJoin, nextNode base.LogicalPlan,
	hasOuterJoin bool,
) (base.LogicalPlan, bool) {
	lNode, rNode, usedEdges, joinType := s.checkConnection(currentJoin, nextNode)
	if hasOuterJoin && len(usedEdges) == 0 {
		// If the joinGroups contain an outer join, we disable cartesian product.
		// For non-equality conditions, only allow them when they do not reference
		// null-extended columns from any outer join in the current group.
		if !s.hasOtherJoinCondition(lNode, rNode) {
			return nil, false
		}
	}
	var rem []expression.Expression
	currentJoin, rem = s.makeJoin(lNode, rNode, usedEdges, joinType, s.otherConds)
	s.otherConds = rem
	return currentJoin, true
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
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
// After extractJoinGroup phase, all derived columns in eqEdges have been substituted
// with their defining expressions (containing only base table columns), so we can
// directly use expression.ExprFromSchema to check if an expression belongs to a schema.
// Note: join reorder expects eqEdges to be join connectors. With projection inlining
// restrictions (single-leaf + must reference columns), substituted eqEdges should still
// connect two different join-group nodes.
func (s *baseSingleGroupJoinOrderSolver) checkConnection(leftPlan, rightPlan base.LogicalPlan) (leftNode, rightNode base.LogicalPlan, usedEdges []*expression.ScalarFunction, joinType *joinTypeWithExtMsg) {
	joinType = &joinTypeWithExtMsg{JoinType: logicalop.InnerJoin}
	leftNode, rightNode = leftPlan, rightPlan
	for idx, edge := range s.eqEdges {
<<<<<<< HEAD
		lCol := edge.GetArgs()[0].(*expression.Column)
		rCol := edge.GetArgs()[1].(*expression.Column)
		if leftPlan.Schema().Contains(lCol) && rightPlan.Schema().Contains(rCol) {
			joinType = s.joinTypes[idx]
			usedEdges = append(usedEdges, edge)
		} else if rightPlan.Schema().Contains(lCol) && leftPlan.Schema().Contains(rCol) {
			joinType = s.joinTypes[idx]
			if joinType.JoinType != logicalop.InnerJoin {
				rightNode, leftNode = leftPlan, rightPlan
				usedEdges = append(usedEdges, edge)
			} else {
				funcName := edge.FuncName.L
				newSf := expression.NewFunctionInternal(s.ctx.GetExprCtx(), funcName, edge.GetStaticType(), rCol, lCol).(*expression.ScalarFunction)

				// after creating the new EQCondition function, the 2 args might not be column anymore, for example `sf=sf(cast(col))`,
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
					newSf = expression.NewFunctionInternal(s.ctx.GetExprCtx(), funcName, edge.GetStaticType(),
						rCol, lCol).(*expression.ScalarFunction)
				}
				usedEdges = append(usedEdges, newSf)
			}
=======
		lArg, rArg, lCols, rCols, ok := joinorder.GetEqEdgeArgsAndCols(edge)
		if !ok {
			continue
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
		}

		// Join reorder expects eqEdges to be real join connectors.
		// ExprFromSchema returns true for Constant/Correlated-only expressions,
		// so a side with no column reference can be wrongly attributed to any schema.
		intest.Assert(len(lCols) > 0 && len(rCols) > 0)
		if len(lCols) == 0 || len(rCols) == 0 {
			continue
		}

		// Check if this edge connects leftPlan and rightPlan.
		// After substitution in extractJoinGroup, expressions only contain base table columns.
		lExpr, rExpr, swapped, ok := joinorder.AlignJoinEdgeArgs(lArg, rArg, leftPlan.Schema(), rightPlan.Schema())
		if !ok {
			continue
		}

		joinType = s.joinTypes[idx]
		if !swapped {
			// Normal order: lArg from left, rArg from right.
			newEdge := s.buildJoinEdge(edge, lExpr, rExpr, &leftPlan, &rightPlan)
			leftNode, rightNode = leftPlan, rightPlan
			usedEdges = append(usedEdges, newEdge)
			continue
		}

		// Reverse order: original args are (right, left).
		if joinType.JoinType != base.InnerJoin {
			// For outer joins, keep outer/inner side semantics by swapping the node positions.
			// Note: buildJoinEdge may inject projections, so we must set leftNode/rightNode AFTER it.
			newEdge := s.buildJoinEdge(edge, lArg, rArg, &rightPlan, &leftPlan)
			leftNode, rightNode = rightPlan, leftPlan
			usedEdges = append(usedEdges, newEdge)
			continue
		}

		// For inner joins, keep the node positions but swap the arguments (already aligned by alignJoinEdgeArgs).
		newEdge := s.buildJoinEdge(edge, lExpr, rExpr, &leftPlan, &rightPlan)
		leftNode, rightNode = leftPlan, rightPlan
		usedEdges = append(usedEdges, newEdge)
	}
	return
}

func (*baseSingleGroupJoinOrderSolver) injectExpr(p base.LogicalPlan, expr expression.Expression) (base.LogicalPlan, *expression.Column) {
	proj, ok := p.(*logicalop.LogicalProjection)
	if !ok {
		proj = logicalop.LogicalProjection{Exprs: cols2Exprs(p.Schema().Columns)}.Init(p.SCtx(), p.QueryBlockOffset())
		proj.SetSchema(p.Schema().Clone())
		proj.SetChildren(p)
	}
	return proj, proj.AppendExpr(expr)
}

// buildJoinEdge creates a join edge (equality condition) ensuring both sides are columns.
// If an argument is not a column, it injects a projection to materialize it.
// lExpr is expected to come from leftPlan's schema, rExpr from rightPlan's schema.
func (s *baseSingleGroupJoinOrderSolver) buildJoinEdge(
	originalEdge *expression.ScalarFunction,
	lExpr, rExpr expression.Expression,
	leftPlan, rightPlan *base.LogicalPlan,
) *expression.ScalarFunction {
	funcName := originalEdge.FuncName.L

	// Check if arguments are already columns
	lCol, lIsCol := lExpr.(*expression.Column)
	rCol, rIsCol := rExpr.(*expression.Column)

	// If both are columns, create the edge directly
	if lIsCol && rIsCol {
		newSf := expression.NewFunctionInternal(s.ctx.GetExprCtx(), funcName, originalEdge.GetStaticType(), lCol, rCol)
		return newSf.(*expression.ScalarFunction)
	}

	// Need to inject projections for non-column expressions
	if !lIsCol {
		*leftPlan, lCol = s.injectExpr(*leftPlan, lExpr)
	}
	if !rIsCol {
		*rightPlan, rCol = s.injectExpr(*rightPlan, rExpr)
	}

	// Create the final edge with column arguments
	// TODO: Reusing these injected columns inside `otherConds` is intentionally disabled for now.
	// Residual-predicate reuse can change evaluation timing and warning/error behavior
	// (for example IF/CASE/AND/OR short-circuiting, division-by-zero, cast overflow or truncation).
	// Revisit this as a separate optimization after we have a semantics-preserving model for
	// residual predicate reuse.
	newSf := expression.NewFunctionInternal(s.ctx.GetExprCtx(), funcName, originalEdge.GetStaticType(), lCol, rCol)
	return newSf.(*expression.ScalarFunction)
}

func canReuseInjectedJoinExpr(expr expression.Expression) bool {
	return !expression.IsMutableEffectsExpr(expr) && !expression.CheckNonDeterministic(expr)
}

// injectExpr materializes expr as a column on top of p so rewritten join edges can keep using
// column arguments only.
//
// If p is already a projection, we append/reuse the expression there. Otherwise we wrap p in a
// pass-through projection first, so the new derived column stays local to this branch and the
// branch's output schema remains explicit for later join construction.
//
// When possible, we reuse an existing semantically equivalent deterministic expression instead of
// appending a duplicate column. This keeps the rewritten join tree smaller and prevents repeated
// materialization when multiple join edges need the same derived expression.
func (s *baseSingleGroupJoinOrderSolver) injectExpr(p base.LogicalPlan, expr expression.Expression) (base.LogicalPlan, *expression.Column) {
	originalPlanID := p.ID()
	proj, ok := p.(*logicalop.LogicalProjection)
	if !ok {
		// Build a pass-through projection so the injected expression has a stable output column
		// without changing the child's original outputs.
		proj = logicalop.LogicalProjection{Exprs: expression.Column2Exprs(p.Schema().Columns)}.Init(p.SCtx(), p.QueryBlockOffset())
		proj.SetSchema(p.Schema().Clone())
		proj.SetChildren(p)
		s.propagateJoinMethodHint(originalPlanID, proj.ID())
	}
	// Avoid injecting duplicate expressions into the same projection.
	// This keeps plans smaller when multiple join edges need the same deterministic expression.
	// We substitute through the current projection first so reuse works even when proj already
	// contains previously injected expressions or pass-through aliases.
	substituted := expression.ColumnSubstitute(proj.SCtx().GetExprCtx(), expr, proj.Schema(), proj.Exprs)
	if canReuseInjectedJoinExpr(substituted) {
		for i, e := range proj.Exprs {
			if expression.ExpressionsSemanticEqual(e, substituted) {
				return proj, proj.Schema().Columns[i]
			}
		}
	}
	// AppendExpr substitutes against the current projection schema again, so passing
	// the original expr is intentional here. Using the pre-substituted child-space
	// expression would incorrectly rematerialize non-reusable projection outputs.
	return proj, proj.AppendExpr(expr)
}

func (s *baseSingleGroupJoinOrderSolver) propagateJoinMethodHint(oldPlanID, newPlanID int) {
	if s == nil || oldPlanID == newPlanID || len(s.joinMethodHintInfo) == 0 {
		return
	}
	if _, exists := s.joinMethodHintInfo[newPlanID]; exists {
		return
	}
	if hintInfo, ok := s.joinMethodHintInfo[oldPlanID]; ok {
		// Keep the original vertex entry for alternative join trees that still use the
		// unwrapped child, and add an alias for the injected projection wrapper.
		s.joinMethodHintInfo[newPlanID] = hintInfo
	}
}

// makeJoin build join tree for the nodes which have equal conditions to connect them.
<<<<<<< HEAD
func (s *baseSingleGroupJoinOrderSolver) makeJoin(leftPlan, rightPlan base.LogicalPlan, eqEdges []*expression.ScalarFunction, joinType *joinTypeWithExtMsg, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, []expression.Expression) {
	remainOtherConds := make([]expression.Expression, len(s.otherConds))
	copy(remainOtherConds, s.otherConds)
=======
func (s *baseSingleGroupJoinOrderSolver) makeJoin(leftPlan, rightPlan base.LogicalPlan, eqEdges []*expression.ScalarFunction, joinType *joinTypeWithExtMsg, inputOtherConds []expression.Expression) (base.LogicalPlan, []expression.Expression) {
	remainOtherConds := make([]expression.Expression, len(inputOtherConds))
	copy(remainOtherConds, inputOtherConds)
>>>>>>> a64f198a6ee (planner: handle the projection between the join group (#65367))
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

	if joinType.JoinType == logicalop.LeftOuterJoin || joinType.JoinType == logicalop.RightOuterJoin || joinType.JoinType == logicalop.LeftOuterSemiJoin || joinType.JoinType == logicalop.AntiLeftOuterSemiJoin {
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
		append(otherConds, obOtherConds...), append(leftConds, obLeftConds...), append(rightConds, obRightConds...), joinType.JoinType, opt), remainOtherConds
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
				if newJoin.Schema().ColumnsIndices(cols) != nil {
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
		additionSelection := logicalop.LogicalSelection{
			Conditions: s.otherConds,
		}.Init(cartesianJoinGroup[0].SCtx(), cartesianJoinGroup[0].QueryBlockOffset())
		additionSelection.SetChildren(cartesianJoinGroup[0])
		cartesianJoinGroup[0] = additionSelection
	}
	return cartesianJoinGroup[0]
}

func (s *baseSingleGroupJoinOrderSolver) newCartesianJoin(lChild, rChild base.LogicalPlan) *logicalop.LogicalJoin {
	offset := lChild.QueryBlockOffset()
	if offset != rChild.QueryBlockOffset() {
		offset = -1
	}
	join := logicalop.LogicalJoin{
		JoinType:  logicalop.InnerJoin,
		Reordered: true,
	}.Init(s.ctx, offset)
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	s.setNewJoinWithHint(join)
	return join
}

func (s *baseSingleGroupJoinOrderSolver) newJoinWithEdges(lChild, rChild base.LogicalPlan,
	eqEdges []*expression.ScalarFunction, otherConds, leftConds, rightConds []expression.Expression, joinType logicalop.JoinType, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	newJoin := s.newCartesianJoin(lChild, rChild)
	newJoin.EqualConditions = eqEdges
	newJoin.OtherConditions = otherConds
	newJoin.LeftConditions = leftConds
	newJoin.RightConditions = rightConds
	newJoin.JoinType = joinType
	if newJoin.JoinType == logicalop.InnerJoin {
		if newJoin.LeftConditions != nil {
			left := newJoin.Children()[0]
			logicalop.AddSelection(newJoin, left, newJoin.LeftConditions, 0, opt)
			newJoin.LeftConditions = nil
		}
		if newJoin.RightConditions != nil {
			right := newJoin.Children()[1]
			logicalop.AddSelection(newJoin, right, newJoin.RightConditions, 1, opt)
			newJoin.RightConditions = nil
		}
	}
	return newJoin
}

// setNewJoinWithHint sets the join method hint for the join node.
// Before the join reorder process, we split the join node and collect the join method hint.
// And we record the join method hint and reset the hint after we have finished the join reorder process.
func (s *baseSingleGroupJoinOrderSolver) setNewJoinWithHint(newJoin *logicalop.LogicalJoin) {
	lChild := newJoin.Children()[0]
	rChild := newJoin.Children()[1]
	if joinMethodHint, ok := s.joinMethodHintInfo[lChild.ID()]; ok {
		newJoin.LeftPreferJoinType = joinMethodHint.preferredJoinMethod
		newJoin.HintInfo = joinMethodHint.joinMethodHintInfo
	}
	if joinMethodHint, ok := s.joinMethodHintInfo[rChild.ID()]; ok {
		newJoin.RightPreferJoinType = joinMethodHint.preferredJoinMethod
		newJoin.HintInfo = joinMethodHint.joinMethodHintInfo
	}
	newJoin.SetPreferredJoinType()
}

// calcJoinCumCost calculates the cumulative cost of the join node.
func (*baseSingleGroupJoinOrderSolver) calcJoinCumCost(join base.LogicalPlan, lNode, rNode *jrNode) float64 {
	return join.StatsInfo().RowCount + lNode.cumCost + rNode.cumCost
}

// Name implements the base.LogicalOptRule.<1st> interface.
func (*JoinReOrderSolver) Name() string {
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
