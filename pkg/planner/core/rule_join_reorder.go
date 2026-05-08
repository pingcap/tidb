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
	"maps"
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/joinorder"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	h "github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// extractJoinGroup extracts all the join nodes connected with continuous
// Joins to construct a join group. This join group is further used to
// construct a new join order based on a reorder algorithm.
//
// For example: "InnerJoin(InnerJoin(a, b), LeftJoin(c, d))"
// results in a join group {a, b, c, d}.
func extractJoinGroup(p base.LogicalPlan) *joinGroupResult {
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
	var (
		group              []base.LogicalPlan
		joinOrderHintInfo  []*h.PlanHints
		eqEdges            []*expression.ScalarFunction
		otherConds         []expression.Expression
		joinTypes          []*joinTypeWithExtMsg
		nullExtendedCols   []*expression.Column
		nullExtendedColSet = make(map[int64]struct{})
		hasOuterJoin       bool
		currentLeadingHint *h.PlanHints // Track the active LEADING hint
	)
	appendNullExtendedCols := func(schema *expression.Schema) {
		if schema == nil {
			return
		}
		for _, col := range schema.Columns {
			if col == nil {
				continue
			}
			if _, ok := nullExtendedColSet[col.UniqueID]; ok {
				continue
			}
			nullExtendedColSet[col.UniqueID] = struct{}{}
			nullExtendedCols = append(nullExtendedCols, col)
		}
	}

	join, isJoin := p.(*logicalop.LogicalJoin)
	if isJoin && join.PreferJoinOrder {
		// When there is a leading hint, the hint may not take effect for other reasons.
		// For example, the join type is cross join or straight join, or exists the join algorithm hint, etc.
		// We need to return the hint information to warn
		joinOrderHintInfo = append(joinOrderHintInfo, join.HintInfo)
		currentLeadingHint = join.HintInfo
	} else if isJoin && join.InternalPreferJoinOrder {
		joinOrderHintInfo = append(joinOrderHintInfo, join.InternalHintInfo)
		currentLeadingHint = join.InternalHintInfo
	}

	// If the variable `tidb_opt_advanced_join_hint` is false and the join node has the join method hint, we will not split the current join node to join reorder process.
	if !isJoin || (join.PreferJoinType > uint(0) && !p.SCtx().GetSessionVars().EnableAdvancedJoinHint) || join.StraightJoin ||
		(join.JoinType != base.InnerJoin && join.JoinType != base.LeftOuterJoin && join.JoinType != base.RightOuterJoin) ||
		((join.JoinType == base.LeftOuterJoin || join.JoinType == base.RightOuterJoin) && join.EqualConditions == nil) ||
		// with NullEQ in the EQCond, the join order needs to consider the transitivity of null and avoid the wrong result.
		// so we skip the join order when to meet the NullEQ in the EQCond
		(slices.ContainsFunc(join.EqualConditions, func(e *expression.ScalarFunction) bool {
			return e.FuncName.L == ast.NullEQ
		})) {
		if joinOrderHintInfo != nil {
			// The leading hint can not work for some reasons. So clear it in the join node.
			join.HintInfo = nil
			join.InternalHintInfo = nil
		}
		return &joinGroupResult{
			group:              []base.LogicalPlan{p},
			joinOrderHintInfo:  joinOrderHintInfo,
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}
	}
	// If the session var is set to off, we will still reject the outer joins.
	if !p.SCtx().GetSessionVars().EnableOuterJoinReorder && (join.JoinType == base.LeftOuterJoin || join.JoinType == base.RightOuterJoin) {
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
			joinMethodHintInfo[join.Children()[0].ID()] = &joinorder.JoinMethodHint{PreferJoinMethod: join.LeftPreferJoinType, HintInfo: join.HintInfo}
			leftHasHint = true
		}
		if join.RightPreferJoinType > uint(0) {
			joinMethodHintInfo[join.Children()[1].ID()] = &joinorder.JoinMethodHint{PreferJoinMethod: join.RightPreferJoinType, HintInfo: join.HintInfo}
			rightHasHint = true
		}
	}
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
		appendNullExtendedCols(lhsJoinGroupResult.nullExtendedCols)
		maps.Copy(joinMethodHintInfo, lhsJoinMethodHintInfo)
		hasOuterJoin = hasOuterJoin || lhsHasOuterJoin
		colExprMap = mergeMap(colExprMap, lhsColExprMap)
	} else {
		group = append(group, join.Children()[0])
	}

	// Check if right child should be preserved due to LEADING hint reference
	rightShouldPreserve := currentLeadingHint != nil && joinorder.IsDerivedTableInLeadingHint(join.Children()[1], currentLeadingHint)

	// You can see the comments in the upside part which we try to split the left child part. It's the same here.
	if join.JoinType != base.LeftOuterJoin && !rightHasHint && !rightShouldPreserve {
		rhsJoinGroupResult := extractJoinGroupImpl(join.Children()[1])
		rhsGroup, rhsEqualConds, rhsOtherConds, rhsJoinTypes, rhsJoinOrderHintInfo, rhsJoinMethodHintInfo, rhsHasOuterJoin, rhsColExprMap := rhsJoinGroupResult.group, rhsJoinGroupResult.eqEdges, rhsJoinGroupResult.otherConds, rhsJoinGroupResult.joinTypes, rhsJoinGroupResult.joinOrderHintInfo, rhsJoinGroupResult.joinMethodHintInfo, rhsJoinGroupResult.hasOuterJoin, rhsJoinGroupResult.colExprMap
		// If the filters of the outer join is related with multiple leaves of the outer join side. We don't reorder it for now.
		if join.JoinType == base.RightOuterJoin &&
			joinorder.OuterJoinSideFiltersTouchMultipleLeaves(join, rhsGroup, rhsColExprMap, false) {
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
		appendNullExtendedCols(rhsJoinGroupResult.nullExtendedCols)
		maps.Copy(joinMethodHintInfo, rhsJoinMethodHintInfo)
		hasOuterJoin = hasOuterJoin || rhsHasOuterJoin
		colExprMap = mergeMap(colExprMap, rhsColExprMap)
	} else {
		group = append(group, join.Children()[1])
	}

	// Collect full null-producing-side schemas for outer joins.
	switch join.JoinType {
	case base.LeftOuterJoin:
		appendNullExtendedCols(join.Children()[1].Schema())
	case base.RightOuterJoin:
		appendNullExtendedCols(join.Children()[0].Schema())
	}

	eqEdges = append(eqEdges, join.EqualConditions...)
	tmpOtherConds := make(expression.CNFExprs, 0, len(join.OtherConditions)+len(join.LeftConditions)+len(join.RightConditions))
	tmpOtherConds = append(tmpOtherConds, join.OtherConditions...)
	tmpOtherConds = append(tmpOtherConds, join.LeftConditions...)
	tmpOtherConds = append(tmpOtherConds, join.RightConditions...)
	if join.JoinType == base.LeftOuterJoin || join.JoinType == base.RightOuterJoin || join.JoinType == base.LeftOuterSemiJoin || join.JoinType == base.AntiLeftOuterSemiJoin {
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
	return &joinGroupResult{
		group:             group,
		hasOuterJoin:      hasOuterJoin,
		joinOrderHintInfo: joinOrderHintInfo,
		basicJoinGroupInfo: &basicJoinGroupInfo{
			eqEdges:            eqEdges,
			otherConds:         otherConds,
			joinTypes:          joinTypes,
			nullExtendedCols:   nullExtendedSchema,
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
	base.JoinType
	outerBindCondition []expression.Expression
}

// Optimize implements the base.LogicalOptRule.<0th> interface.
func (s *JoinReOrderSolver) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	if p.SCtx().GetSessionVars().TiDBOptEnableAdvancedJoinReorder {
		p, err := joinorder.Optimize(p)
		return p, false, err
	}
	p, err := s.optimizeRecursive(p.SCtx(), p)
	return p, false, err
}

// optimizeRecursive recursively collects join groups and applies join reorder algorithm for each group.
func (s *JoinReOrderSolver) optimizeRecursive(ctx base.PlanContext, p base.LogicalPlan) (base.LogicalPlan, error) {
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
				curJoinGroup[i], err = s.optimizeRecursive(ctx, curJoinGroup[i])
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
			allInnerJoin := true
			for _, joinType := range joinTypes {
				if joinType.JoinType != base.InnerJoin {
					allInnerJoin = false
					break
				}
			}

			baseGroupSolver := &baseSingleGroupJoinOrderSolver{
				ctx:                ctx,
				basicJoinGroupInfo: result.basicJoinGroupInfo,
			}

			joinGroupNum := len(curJoinGroup)
			useGreedy := !allInnerJoin || joinGroupNum > ctx.GetSessionVars().TiDBOptJoinReorderThreshold

			leadingHintInfo, hasDiffLeadingHint := joinorder.CheckAndGenerateLeadingHint(joinOrderHintInfo)
			if hasDiffLeadingHint {
				ctx.GetSessionVars().StmtCtx.SetHintWarning(
					"We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid")
			}

			if leadingHintInfo != nil && leadingHintInfo.LeadingJoinOrder != nil {
				if useGreedy {
					ok, leftJoinGroup, err := baseGroupSolver.generateLeadingJoinGroup(curJoinGroup, leadingHintInfo, hasOuterJoin)
					if err != nil {
						return nil, err
					}
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
					allInnerJoin:                   allInnerJoin,
					baseSingleGroupJoinOrderSolver: baseGroupSolver,
				}
				p, err = groupSolver.solve(curJoinGroup)
			} else {
				dpSolver := &joinReorderDPSolver{
					baseSingleGroupJoinOrderSolver: baseGroupSolver,
				}
				dpSolver.newJoin = dpSolver.newJoinWithEdges
				p, err = dpSolver.solve(curJoinGroup)
			}
			if err != nil {
				return fallbackOnErr(err)
			}
			p, err = restoreSchemaIfChanged(p, originalSchema, result.colExprMap)
			if err != nil {
				return fallbackOnErr(err)
			}
			return p, nil
		}
		if len(curJoinGroup) == 1 && joinOrderHintInfo != nil {
			ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable, check the join type or the join algorithm hint")
		}
	}
	newChildren := make([]base.LogicalPlan, 0, len(p.Children()))
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
}

// basicJoinGroupInfo represents basic information for a join group in the join reorder process.
type basicJoinGroupInfo struct {
	eqEdges    []*expression.ScalarFunction
	otherConds []expression.Expression
	joinTypes  []*joinTypeWithExtMsg
	// nullExtendedCols records columns that may become NULL due to outer joins
	// in this join group. It is used to avoid semantically invalid non-eq reorder.
	nullExtendedCols *expression.Schema
	// `joinMethodHintInfo` is used to map the sub-plan's ID to the join method hint.
	// The sub-plan will join the join reorder process to build the new plan.
	// So after we have finished the join reorder process, we can reset the join method hint based on the sub-plan's ID.
	joinMethodHintInfo map[int]*joinorder.JoinMethodHint
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
	leftJoinGroup := make([]base.LogicalPlan, len(curJoinGroup))
	copy(leftJoinGroup, curJoinGroup)

	find := func(available []base.LogicalPlan, hint *ast.HintTable) (base.LogicalPlan, []base.LogicalPlan, bool) {
		return joinorder.FindAndRemovePlanByAstHint(s.ctx, available, hint, func(plan base.LogicalPlan) base.LogicalPlan {
			return plan
		})
	}
	joiner := func(left, right base.LogicalPlan) (base.LogicalPlan, bool, error) {
		currentJoin, ok := s.connectJoinNodes(left, right, hasOuterJoin)
		if !ok {
			return nil, false, nil
		}
		return currentJoin, true, nil
	}
	warn := func() {
		s.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint contains unexpected element type")
	}

	leadingJoin, remainingGroup, ok, err := joinorder.BuildLeadingTreeFromList(leadingList, leftJoinGroup, find, joiner, warn)
	if err != nil {
		return false, nil, err
	}
	if !ok {
		return false, nil, nil
	}
	s.leadingJoinGroup = leadingJoin
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
}

// generateJoinOrderNode used to derive the stats for the joinNodePlans and generate the jrNode groups based on the cost.
func (s *baseSingleGroupJoinOrderSolver) generateJoinOrderNode(joinNodePlans []base.LogicalPlan) ([]*jrNode, error) {
	joinGroup := make([]*jrNode, 0, len(joinNodePlans))
	for _, node := range joinNodePlans {
		_, _, err := node.RecursiveDeriveStats(nil)
		if err != nil {
			return nil, err
		}
		cost := s.baseNodeCumCost(node)
		joinGroup = append(joinGroup, &jrNode{
			p:       node,
			cumCost: cost,
		})
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
	joinType = &joinTypeWithExtMsg{JoinType: base.InnerJoin}
	leftNode, rightNode = leftPlan, rightPlan
	for idx, edge := range s.eqEdges {
		lArg, rArg, lCols, rCols, ok := joinorder.GetEqEdgeArgsAndCols(edge)
		if !ok {
			continue
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

// hasOtherJoinCondition checks whether there are non-equality join conditions
// connecting the two plans (i.e. conditions referencing columns from both sides).
// Conditions referencing null-extended columns are excluded since reordering
// across them can change outer-join semantics.
func (s *baseSingleGroupJoinOrderSolver) hasOtherJoinCondition(leftPlan, rightPlan base.LogicalPlan) bool {
	if len(s.otherConds) == 0 {
		return false
	}
	mergedSchema := expression.MergeSchema(leftPlan.Schema(), rightPlan.Schema())
	for _, cond := range s.otherConds {
		if !expression.ExprFromSchema(cond, mergedSchema) {
			continue
		}
		if expression.ExprFromSchema(cond, leftPlan.Schema()) || expression.ExprFromSchema(cond, rightPlan.Schema()) {
			continue
		}
		if s.nullExtendedCols != nil && expression.ExprReferenceSchema(cond, s.nullExtendedCols) {
			continue
		}
		return true
	}
	return false
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
func (s *baseSingleGroupJoinOrderSolver) makeJoin(leftPlan, rightPlan base.LogicalPlan, eqEdges []*expression.ScalarFunction, joinType *joinTypeWithExtMsg, inputOtherConds []expression.Expression) (base.LogicalPlan, []expression.Expression) {
	remainOtherConds := make([]expression.Expression, len(inputOtherConds))
	copy(remainOtherConds, inputOtherConds)
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
		if expression.IsMutableEffectsExpr(expr) {
			return false
		}
		return expression.ExprFromSchema(expr, leftPlan.Schema()) && !expression.ExprFromSchema(expr, rightPlan.Schema())
	})
	remainOtherConds, rightConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		if expression.IsMutableEffectsExpr(expr) {
			return false
		}
		return expression.ExprFromSchema(expr, rightPlan.Schema()) && !expression.ExprFromSchema(expr, leftPlan.Schema())
	})
	remainOtherConds, otherConds = expression.FilterOutInPlace(remainOtherConds, func(expr expression.Expression) bool {
		return expression.ExprFromSchema(expr, mergedSchema)
	})

	if joinType.JoinType == base.LeftOuterJoin || joinType.JoinType == base.RightOuterJoin || joinType.JoinType == base.LeftOuterSemiJoin || joinType.JoinType == base.AntiLeftOuterSemiJoin {
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
			if expression.IsMutableEffectsExpr(expr) {
				return false
			}
			return expression.ExprFromSchema(expr, leftPlan.Schema()) && !expression.ExprFromSchema(expr, rightPlan.Schema())
		})
		remainOBOtherConds, obRightConds = expression.FilterOutInPlace(remainOBOtherConds, func(expr expression.Expression) bool {
			if expression.IsMutableEffectsExpr(expr) {
				return false
			}
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
				if newJoin.Schema().ColumnsIndices(cols) != nil {
					newJoin.OtherConditions = append(newJoin.OtherConditions, s.otherConds[i])
					s.otherConds = slices.Delete(s.otherConds, i, i+1)
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
		JoinType:  base.InnerJoin,
		Reordered: true,
	}.Init(s.ctx, offset)
	join.SetSchema(expression.MergeSchema(lChild.Schema(), rChild.Schema()))
	join.SetChildren(lChild, rChild)
	joinorder.SetNewJoinWithHint(join, s.joinMethodHintInfo)
	return join
}

func (s *baseSingleGroupJoinOrderSolver) newJoinWithEdges(lChild, rChild base.LogicalPlan,
	eqEdges []*expression.ScalarFunction, otherConds, leftConds, rightConds []expression.Expression, joinType base.JoinType) base.LogicalPlan {
	newJoin := s.newCartesianJoin(lChild, rChild)
	newJoin.EqualConditions = eqEdges
	newJoin.OtherConditions = otherConds
	newJoin.LeftConditions = leftConds
	newJoin.RightConditions = rightConds
	newJoin.JoinType = joinType
	if newJoin.JoinType == base.InnerJoin {
		if newJoin.LeftConditions != nil {
			left := newJoin.Children()[0]
			logicalop.AddSelection(newJoin, left, newJoin.LeftConditions, 0)
			newJoin.LeftConditions = nil
		}
		if newJoin.RightConditions != nil {
			right := newJoin.Children()[1]
			logicalop.AddSelection(newJoin, right, newJoin.RightConditions, 1)
			newJoin.RightConditions = nil
		}
	}
	return newJoin
}

// calcJoinCumCost calculates the cumulative cost of the join node.
func (*baseSingleGroupJoinOrderSolver) calcJoinCumCost(join base.LogicalPlan, lNode, rNode *jrNode) float64 {
	return join.StatsInfo().RowCount + lNode.cumCost + rNode.cumCost
}

// Name implements the base.LogicalOptRule.<1st> interface.
func (*JoinReOrderSolver) Name() string {
	return "join_reorder"
}
