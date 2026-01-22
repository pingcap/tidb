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
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
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
// It uses a bottom-up approach for colExprMap propagation:
// 1. First recursively process child nodes
// 2. Use child's returned colExprMap to substitute column references in current expressions
// 3. Build current node's colExprMap and return
// This approach is consistent with rule_eliminate_projection.go.
func extractJoinGroupImpl(p base.LogicalPlan) *joinGroupResult {
	// Check if we should try to inline projections
	if proj, ok := p.(*logicalop.LogicalProjection); ok &&
		p.SCtx() != nil && p.SCtx().GetSessionVars().TiDBOptJoinReorderProj {
		// Only inline projections whose child is a join; otherwise the projection is treated as an atomic leaf.
		if _, isJoin := proj.Children()[0].(*logicalop.LogicalJoin); isJoin {
			// Fast safety checks first to avoid unnecessary recursion.
			if canInlineProjectionBasic(proj) {
				childResult := extractJoinGroupImpl(proj.Children()[0])
				// The single-leaf check needs the extracted join group and child's colExprMap.
				if canInlineProjection(proj, childResult) {
					// Build current Projection's colExprMap.
					colExprMap := make(map[int64]expression.Expression, len(proj.Schema().Columns))
					for i, col := range proj.Schema().Columns {
						expr := proj.Exprs[i]

						// Use child's colExprMap to substitute column references in current expression.
						if len(childResult.colExprMap) > 0 {
							expr = substituteColsInExpr(expr, childResult.colExprMap)
						}

						// For pass-through columns (where the projection expression is just a Column reference
						// that has been substituted to the underlying expression), we need to check if the output
						// column's UniqueID matches the input column's UniqueID. This can happen when a projection
						// simply passes through a column without transformation (e.g., SELECT dt1.key_a FROM ...).
						// In such cases, the output column may share the same UniqueID as the input column.
						//
						// If expr is a Column and its UniqueID equals col.UniqueID, this is a pass-through scenario.
						// We should NOT add it to colExprMap because:
						// 1. It would create a self-referential mapping (UniqueID -> Column with same UniqueID)
						// 2. It could conflict with child's colExprMap entry for the same UniqueID
						//
						// The child's colExprMap will be merged later and will provide the proper mapping for this UniqueID.
						if colExpr, isCol := expr.(*expression.Column); isCol && colExpr.UniqueID == col.UniqueID {
							// Skip pass-through columns - they will get their mapping from child's colExprMap.
							continue
						}

						colExprMap[col.UniqueID] = expr
					}

					// Merge child's colExprMap entries.
					// Note: A key conflict here should not happen because each projection layer
					// produces new UniqueIDs. If it occurs, we skip the child's entry and keep
					// the current projection's entry, which is the correct behavior.
					for k, v := range childResult.colExprMap {
						if _, exists := colExprMap[k]; !exists {
							colExprMap[k] = v
						}
					}

					childResult.colExprMap = colExprMap
					return childResult
				}
			}
		}
	}

	joinMethodHintInfo := make(map[int]*joinMethodHint)
	var (
		group             []base.LogicalPlan
		joinOrderHintInfo []*h.PlanHints
		eqEdges           []*expression.ScalarFunction
		otherConds        []expression.Expression
		joinTypes         []*joinTypeWithExtMsg
		hasOuterJoin      bool
	)

	// Check if the current plan is a Selection. If its child is a join, add the selection conditions
	// to otherConds and continue extracting the join group from the child.
	// Join reorder may distribute/push down conditions during constructing the new join tree.
	// For volatile or side-effect expressions, moving them can change evaluation times/orders
	// thus may change query results, so we skip reordering through Selection in such cases.
	if selection, isSelection := p.(*logicalop.LogicalSelection); isSelection && p.SCtx().GetSessionVars().TiDBOptJoinReorderThroughSel &&
		!slices.ContainsFunc(selection.Conditions, expression.IsMutableEffectsExpr) {
		child := selection.Children()[0]
		if _, isChildJoin := child.(*logicalop.LogicalJoin); isChildJoin {
			childResult := extractJoinGroup(child)
			childResult.otherConds = append(childResult.otherConds, selection.Conditions...)
			return childResult
		}
	}

	join, isJoin := p.(*logicalop.LogicalJoin)
	if isJoin && join.PreferJoinOrder {
		// When there is a leading hint, the hint may not take effect for other reasons.
		// For example, the join type is cross join or straight join, or exists the join algorithm hint, etc.
		// We need to return the hint information to warn
		joinOrderHintInfo = append(joinOrderHintInfo, join.HintInfo)
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
			joinMethodHintInfo[join.Children()[0].ID()] = &joinMethodHint{join.LeftPreferJoinType, join.HintInfo}
			leftHasHint = true
		}
		if join.RightPreferJoinType > uint(0) {
			joinMethodHintInfo[join.Children()[1].ID()] = &joinMethodHint{join.RightPreferJoinType, join.HintInfo}
			rightHasHint = true
		}
	}
	var colExprMap map[int64]expression.Expression

	hasOuterJoin = hasOuterJoin || (join.JoinType != base.InnerJoin)
	// If the left child has the hint, it means there are some join method hints want to specify the join method based on the left child.
	// For example: `select .. from t1 join t2 join (select .. from t3 join t4) t5 where ..;` If there are some join method hints related to `t5`, we can't split `t5` into `t3` and `t4`.
	// So we don't need to split the left child part. The right child part is the same.
	if join.JoinType != base.RightOuterJoin && !leftHasHint {
		lhsJoinGroupResult := extractJoinGroupImpl(join.Children()[0])
		lhsGroup, lhsEqualConds, lhsOtherConds, lhsJoinTypes, lhsJoinOrderHintInfo, lhsJoinMethodHintInfo, lhsHasOuterJoin, lhsColExprMap := lhsJoinGroupResult.group, lhsJoinGroupResult.eqEdges, lhsJoinGroupResult.otherConds, lhsJoinGroupResult.joinTypes, lhsJoinGroupResult.joinOrderHintInfo, lhsJoinGroupResult.joinMethodHintInfo, lhsJoinGroupResult.hasOuterJoin, lhsJoinGroupResult.colExprMap
		noExpand := false
		// If the filters of the outer join is related with multiple leaves of the outer join side. We don't reorder it for now.
		if join.JoinType == base.LeftOuterJoin {
			eqConds := expression.ScalarFuncs2Exprs(join.EqualConditions)
			checkOtherConds := join.OtherConditions
			checkLeftConds := join.LeftConditions
			checkEQConds := eqConds
			// If projections were inlined under the outer side, join conditions may reference derived columns
			// that are not contained in any leaf schema. Substitute them to correctly detect how many leaves
			// the outer-join filters depend on.
			if len(lhsColExprMap) > 0 {
				checkOtherConds = substituteColsInExprs(checkOtherConds, lhsColExprMap)
				checkLeftConds = substituteColsInExprs(checkLeftConds, lhsColExprMap)
				checkEQConds = substituteColsInExprs(checkEQConds, lhsColExprMap)
			}
			extractedCols := make(map[int64]*expression.Column, len(checkLeftConds)+len(checkOtherConds)+len(checkEQConds))
			expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkOtherConds...)
			expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkLeftConds...)
			expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkEQConds...)
			affectedGroups := 0
			for _, lhs := range lhsGroup {
				lhsSchema := lhs.Schema()
				for _, col := range extractedCols {
					if lhsSchema.Contains(col) {
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
		maps.Copy(joinMethodHintInfo, lhsJoinMethodHintInfo)
		hasOuterJoin = hasOuterJoin || lhsHasOuterJoin
		if lhsColExprMap != nil {
			colExprMap = make(map[int64]expression.Expression)
			for k, v := range lhsColExprMap {
				colExprMap[k] = v
			}
		}
	} else {
		group = append(group, join.Children()[0])
	}

	// You can see the comments in the upside part which we try to split the left child part. It's the same here.
	if join.JoinType != base.LeftOuterJoin && !rightHasHint {
		rhsJoinGroupResult := extractJoinGroupImpl(join.Children()[1])
		rhsGroup, rhsEqualConds, rhsOtherConds, rhsJoinTypes, rhsJoinOrderHintInfo, rhsJoinMethodHintInfo, rhsHasOuterJoin, rhsColExprMap := rhsJoinGroupResult.group, rhsJoinGroupResult.eqEdges, rhsJoinGroupResult.otherConds, rhsJoinGroupResult.joinTypes, rhsJoinGroupResult.joinOrderHintInfo, rhsJoinGroupResult.joinMethodHintInfo, rhsJoinGroupResult.hasOuterJoin, rhsJoinGroupResult.colExprMap
		noExpand := false
		// If the filters of the outer join is related with multiple leaves of the outer join side. We don't reorder it for now.
		if join.JoinType == base.RightOuterJoin {
			eqConds := expression.ScalarFuncs2Exprs(join.EqualConditions)
			checkOtherConds := join.OtherConditions
			checkRightConds := join.RightConditions
			checkEQConds := eqConds
			// Same as left outer join: substitute derived columns from inlined projections to correctly
			// detect whether the outer side filters touch multiple leaves.
			if len(rhsColExprMap) > 0 {
				checkOtherConds = substituteColsInExprs(checkOtherConds, rhsColExprMap)
				checkRightConds = substituteColsInExprs(checkRightConds, rhsColExprMap)
				checkEQConds = substituteColsInExprs(checkEQConds, rhsColExprMap)
			}
			extractedCols := make(map[int64]*expression.Column, len(checkOtherConds)+len(checkRightConds)+len(checkEQConds))
			expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkOtherConds...)
			expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkRightConds...)
			expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkEQConds...)
			affectedGroups := 0
			for _, rhs := range rhsGroup {
				rhsSchema := rhs.Schema()
				for _, col := range extractedCols {
					if rhsSchema.Contains(col) {
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
		maps.Copy(joinMethodHintInfo, rhsJoinMethodHintInfo)
		hasOuterJoin = hasOuterJoin || rhsHasOuterJoin
		if rhsColExprMap != nil {
			if colExprMap == nil {
				colExprMap = make(map[int64]expression.Expression)
			}
			for k, v := range rhsColExprMap {
				colExprMap[k] = v
			}
		}
	} else {
		group = append(group, join.Children()[1])
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
		eqEdges = substituteColsInEqEdges(eqEdges, colExprMap)
		// TODO: When a derived column (e.g., t2.b * 2) is substituted in otherConds and also
		// appears in the output projection, the expression may be computed twice. Consider
		// introducing common subexpression elimination or referencing the computed column
		// instead of duplicating the expression. Example:
		//   dt.doubled_b > 100 is substituted to (t2.b * 2) > 100
		//   while Projection also computes: t2.b * 2 -> Column#X
		// Ideally, the filter should reference Column#X instead of recomputing.
		otherConds = substituteColsInExprs(otherConds, colExprMap)
		// Also substitute in outerBindCondition for outer joins
		for _, jt := range joinTypes {
			if jt.outerBindCondition != nil {
				jt.outerBindCondition = substituteColsInExprs(jt.outerBindCondition, colExprMap)
			}
		}
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
		colExprMap: colExprMap,
	}
}

// substituteColsInEqEdges substitutes derived columns in equality edges using colExprMap.
func substituteColsInEqEdges(edges []*expression.ScalarFunction, colExprMap map[int64]expression.Expression) []*expression.ScalarFunction {
	result := make([]*expression.ScalarFunction, 0, len(edges))
	for _, edge := range edges {
		substituted := substituteColsInExpr(edge, colExprMap)
		if sf, ok := substituted.(*expression.ScalarFunction); ok {
			result = append(result, sf)
		} else {
			result = append(result, edge)
		}
	}
	return result
}

// substituteColsInExprs substitutes derived columns in a list of expressions using colExprMap.
func substituteColsInExprs(exprs []expression.Expression, colExprMap map[int64]expression.Expression) []expression.Expression {
	result := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		result = append(result, substituteColsInExpr(expr, colExprMap))
	}
	return result
}

// substituteExprsWithColsInExprs replaces deterministic sub-expressions in exprs with a
// pre-materialized column from expr2Col (keyed by expression.CanonicalHashCode()).
func substituteExprsWithColsInExprs(exprs []expression.Expression, expr2Col map[string]*expression.Column) []expression.Expression {
	if len(expr2Col) == 0 {
		return exprs
	}
	result := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		result = append(result, substituteExprsWithColsInExpr(expr, expr2Col))
	}
	return result
}

func substituteExprsWithColsInExpr(expr expression.Expression, expr2Col map[string]*expression.Column) expression.Expression {
	if len(expr2Col) == 0 {
		return expr
	}
	if col, ok := expr2Col[string(expr.CanonicalHashCode())]; ok {
		return col
	}
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return expr
	}
	// Copy-on-write: only clone the function node when any argument changes.
	oldArgs := sf.GetArgs()
	changed := false
	newArgs := make([]expression.Expression, len(oldArgs))
	for i, arg := range oldArgs {
		newArgs[i] = substituteExprsWithColsInExpr(arg, expr2Col)
		changed = changed || newArgs[i] != arg
	}
	if !changed {
		return sf
	}
	newSf := sf.Clone().(*expression.ScalarFunction)
	args := newSf.GetArgs()
	for i := range args {
		args[i] = newArgs[i]
	}
	return newSf
}

// substituteColsInExpr recursively substitutes derived columns in an expression using colExprMap.
// It replaces column references with their defining expressions from colExprMap.
func substituteColsInExpr(expr expression.Expression, colExprMap map[int64]expression.Expression) expression.Expression {
	if len(colExprMap) == 0 {
		return expr
	}

	switch e := expr.(type) {
	case *expression.Column:
		if defExpr, ok := colExprMap[e.UniqueID]; ok {
			// Recursively substitute - the defining expression might contain other derived columns
			return substituteColsInExpr(defExpr, colExprMap)
		}
		return e

	case *expression.ScalarFunction:
		// Copy-on-write: only clone the function node when any argument changes.
		oldArgs := e.GetArgs()
		changed := false
		newArgs := make([]expression.Expression, len(oldArgs))
		for i, arg := range oldArgs {
			newArgs[i] = substituteColsInExpr(arg, colExprMap)
			changed = changed || newArgs[i] != arg
		}
		if !changed {
			return e
		}
		newSf := e.Clone().(*expression.ScalarFunction)
		args := newSf.GetArgs()
		for i := range args {
			args[i] = newArgs[i]
		}
		return newSf
	}
	return expr
}

// JoinReOrderSolver is used to reorder the join nodes in a logical plan.
type JoinReOrderSolver struct {
}

// canInlineProjectionBasic checks if a projection is safe to inline, independent of join-group shape.
func canInlineProjectionBasic(proj *logicalop.LogicalProjection) bool {
	// Proj4Expand projections cannot be inlined
	if proj.Proj4Expand {
		return false
	}

	// No side effects allowed
	if expression.ExprsHasSideEffects(proj.Exprs) {
		return false
	}

	// Check each expression for various constraints
	for _, expr := range proj.Exprs {
		// No non-deterministic functions
		if expression.CheckNonDeterministic(expr) {
			return false
		}

		// No correlated columns
		if len(expression.ExtractCorColumns(expr)) > 0 {
			return false
		}
	}

	return true
}

// canInlineProjection checks whether every projection expression depends on at most one join-group leaf.
// This is required because we will replace derived columns in join conditions with their defining expressions,
// and join reorder must be able to attribute each expression to exactly one side.
//
// Note: It assumes canInlineProjectionBasic has passed.
func canInlineProjection(proj *logicalop.LogicalProjection, childResult *joinGroupResult) bool {
	if childResult == nil {
		return false
	}

	// Build a UniqueID -> leaf index map for the join group.
	leafByColID := make(map[int64]int)
	for leafIdx, leaf := range childResult.group {
		for _, col := range leaf.Schema().Columns {
			// If a column appears in multiple leaf schemas (shouldn't happen), treat it as unsafe.
			if prev, ok := leafByColID[col.UniqueID]; ok && prev != leafIdx {
				return false
			}
			leafByColID[col.UniqueID] = leafIdx
		}
	}

	for _, expr := range proj.Exprs {
		checkExpr := expr
		// Inline any previously inlined derived columns first, so we only reason on columns of join-group leaves.
		if len(childResult.colExprMap) > 0 {
			checkExpr = substituteColsInExpr(checkExpr, childResult.colExprMap)
		}

		cols := expression.ExtractColumns(checkExpr)
		if len(cols) == 0 {
			// Constant expression - doesn't constrain join reorder leaf attribution.
			continue
		}

		leafIdx := -1
		for _, col := range cols {
			idx, ok := leafByColID[col.UniqueID]
			if !ok {
				// Column doesn't belong to any join-group leaf schema; be conservative.
				return false
			}
			if leafIdx == -1 {
				leafIdx = idx
				continue
			}
			if idx != leafIdx {
				// Cross-leaf expression.
				return false
			}
		}
	}
	return true
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
	} else {
		result = &joinGroupResult{
			group:              []base.LogicalPlan{p},
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}
	}
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
			if ctx.GetSessionVars().TiDBOptJoinReorderProj {
				// This optimization is best-effort. If anything goes wrong, fallback to the
				// original behavior (treat projections as atomic leaves) to keep correctness.
				saved := ctx.GetSessionVars().TiDBOptJoinReorderProj
				ctx.GetSessionVars().TiDBOptJoinReorderProj = false
				defer func() { ctx.GetSessionVars().TiDBOptJoinReorderProj = saved }()
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

		leadingHintInfo, hasDiffLeadingHint := checkAndGenerateLeadingHint(joinOrderHintInfo)
		if hasDiffLeadingHint {
			ctx.GetSessionVars().StmtCtx.SetHintWarning(
				"We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid")
		}

		if leadingHintInfo != nil && leadingHintInfo.LeadingJoinOrder != nil {
			if !useGreedy {
				ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint is inapplicable for the DP join reorder algorithm")
			} else {
				ok, leftJoinGroup := baseGroupSolver.generateLeadingJoinGroup(curJoinGroup, leadingHintInfo, hasOuterJoin)
				if !ok {
					ctx.GetSessionVars().StmtCtx.SetHintWarning(
						"leading hint is inapplicable, check if the leading hint table is valid")
				} else {
					curJoinGroup = leftJoinGroup
				}
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
			// Build projection expressions
			// When projections were inlined (colExprMap exists), we need to substitute
			// derived columns with their original expressions
			var projExprs []expression.Expression
			if len(result.colExprMap) > 0 {
				projExprs = make([]expression.Expression, len(originalSchema.Columns))
				missingMapping := false
				for i, col := range originalSchema.Columns {
					if expr, ok := result.colExprMap[col.UniqueID]; ok {
						// This is a derived column, use its defining expression
						projExprs[i] = expr
					} else {
						// For non-derived columns, we should be able to reference them from the new join schema.
						// If not, the inlining mapping is incomplete, and we conservatively fallback.
						if !p.Schema().Contains(col) {
							missingMapping = true
							break
						}
						projExprs[i] = col
					}
				}
				if missingMapping {
					intest.Assert(false)
					return fallbackOnErr(plannererrors.ErrInternal.GenWithStack("join reorder: schema restore mapping missing after projection inlining"))
				}
			} else {
				projExprs = expression.Column2Exprs(originalSchema.Columns)
			}

			proj := logicalop.LogicalProjection{
				Exprs: projExprs,
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
		newChild, err := s.optimizeRecursive(ctx, child)
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

// generateLeadingJoinGroup processes both flat and nested leading hints through the unified LeadingList structure
func (s *baseSingleGroupJoinOrderSolver) generateLeadingJoinGroup(
	curJoinGroup []base.LogicalPlan, hintInfo *h.PlanHints, hasOuterJoin bool,
) (bool, []base.LogicalPlan) {
	if hintInfo == nil || hintInfo.LeadingList == nil {
		return false, nil
	}
	// Use the unified nested processing for both flat and nested structures
	return s.generateNestedLeadingJoinGroup(curJoinGroup, hintInfo.LeadingList, hasOuterJoin)
}

// generateNestedLeadingJoinGroup processes both flat and nested LEADING hint structures
func (s *baseSingleGroupJoinOrderSolver) generateNestedLeadingJoinGroup(
	curJoinGroup []base.LogicalPlan, leadingList *ast.LeadingList, hasOuterJoin bool,
) (bool, []base.LogicalPlan) {
	leftJoinGroup := make([]base.LogicalPlan, len(curJoinGroup))
	copy(leftJoinGroup, curJoinGroup)

	leadingJoin, remainingGroup, ok := s.buildLeadingTreeFromList(leadingList, leftJoinGroup, hasOuterJoin)
	if !ok {
		return false, nil
	}
	s.leadingJoinGroup = leadingJoin
	return true, remainingGroup
}

// buildLeadingTreeFromList recursively constructs a LEADING join order tree.
// the `leadingList` argument is derived from a LEADING hint in SQL, e.g.:
//
//	/*+ LEADING(t1, (t2, t3), (t4, (t5, t6, t7))) */
//
// and it is parsed into a nested structure of *ast.LeadingList and *ast.HintTable:
// leadingList.Items = [
//
//	*ast.HintTable{name: "t1"},
//	*ast.LeadingList{ // corresponds to (t2, t3)
//	    Items: [
//	        *ast.HintTable{name: "t2"},
//	        *ast.HintTable{name: "t3"},
//	    ],
//	},
//	*ast.LeadingList{ // corresponds to (t4, (t5, t6, t7))
//	    Items: [
//	        *ast.HintTable{name: "t4"},
//	        *ast.LeadingList{
//	            Items: [
//	                *ast.HintTable{name: "t5"},
//	                *ast.HintTable{name: "t6"},
//	                *ast.HintTable{name: "t7"},
//	            ],
//	        },
//	    ],
//	},
//
// ]
func (s *baseSingleGroupJoinOrderSolver) buildLeadingTreeFromList(
	leadingList *ast.LeadingList, availableGroups []base.LogicalPlan, hasOuterJoin bool,
) (base.LogicalPlan, []base.LogicalPlan, bool) {
	if leadingList == nil || len(leadingList.Items) == 0 {
		return nil, availableGroups, false
	}

	var (
		currentJoin     base.LogicalPlan
		remainingGroups = availableGroups
		ok              bool
	)

	for i, item := range leadingList.Items {
		switch element := item.(type) {
		case *ast.HintTable:
			// find and remove the plan node that matches ast.HintTable from remainingGroups
			var tablePlan base.LogicalPlan
			tablePlan, remainingGroups, ok = s.findAndRemovePlanByAstHint(remainingGroups, element)
			if !ok {
				return nil, availableGroups, false
			}

			if i == 0 {
				currentJoin = tablePlan
			} else {
				currentJoin, availableGroups, ok = s.connectJoinNodes(currentJoin, tablePlan, hasOuterJoin, availableGroups)
				if !ok {
					return nil, availableGroups, false
				}
			}
		case *ast.LeadingList:
			// recursively handle nested lists
			var nestedJoin base.LogicalPlan
			nestedJoin, remainingGroups, ok = s.buildLeadingTreeFromList(element, remainingGroups, hasOuterJoin)
			if !ok {
				return nil, availableGroups, false
			}

			if i == 0 {
				currentJoin = nestedJoin
			} else {
				currentJoin, availableGroups, ok = s.connectJoinNodes(currentJoin, nestedJoin, hasOuterJoin, availableGroups)
				if !ok {
					return nil, availableGroups, false
				}
			}
		default:
			s.ctx.GetSessionVars().StmtCtx.SetHintWarning("leading hint contains unexpected element type")
			return nil, availableGroups, false
		}
	}

	return currentJoin, remainingGroups, true
}

// connectJoinNodes handles joining two subplans, performing connection checks
// and enforcing the outer join constraint.
func (s *baseSingleGroupJoinOrderSolver) connectJoinNodes(
	currentJoin, nextNode base.LogicalPlan,
	hasOuterJoin bool, availableGroups []base.LogicalPlan,
) (base.LogicalPlan, []base.LogicalPlan, bool) {
	lNode, rNode, usedEdges, joinType := s.checkConnection(currentJoin, nextNode)
	if hasOuterJoin && usedEdges == nil {
		// If the joinGroups contain an outer join, we disable cartesian product.
		return nil, availableGroups, false
	}
	var rem []expression.Expression
	currentJoin, rem = s.makeJoin(lNode, rNode, usedEdges, joinType)
	s.otherConds = rem
	return currentJoin, availableGroups, true
}

// findAndRemovePlanByAstHint: Find the plan in `plans` that matches `ast.HintTable` and remove that plan, returning the new slice.
// Matching rules:
//  1. Match by regular table name (db/table/*)
//  2. Match by query-block alias (subquery name, e.g., tx)
//  3. If multiple join groups belong to the same block alias, mark as ambiguous and skip (consistent with old logic)
func (s *baseSingleGroupJoinOrderSolver) findAndRemovePlanByAstHint(
	plans []base.LogicalPlan, astTbl *ast.HintTable,
) (base.LogicalPlan, []base.LogicalPlan, bool) {
	var queryBlockNames []ast.HintTable
	if p := s.ctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
		queryBlockNames = *p
	}

	// Step 1: Direct match by table name
	for i, joinGroup := range plans {
		tableAlias := util.ExtractTableAlias(joinGroup, joinGroup.QueryBlockOffset())
		if tableAlias != nil {
			// Match db/table (supports astTbl.DBName == "*")
			dbMatch := astTbl.DBName.L == "" || astTbl.DBName.L == tableAlias.DBName.L || astTbl.DBName.L == "*"
			tableMatch := astTbl.TableName.L == tableAlias.TblName.L

			// Match query block names
			// Use SelectOffset to match query blocks
			qbMatch := true
			if astTbl.QBName.L != "" {
				expectedOffset := extractSelectOffset(astTbl.QBName.L)
				if expectedOffset > 0 {
					qbMatch = tableAlias.SelectOffset == expectedOffset
				} else {
					// If QBName cannot be parsed, ignore the QB match.
					qbMatch = true
				}
			}
			if dbMatch && tableMatch && qbMatch {
				newPlans := append(plans[:i], plans[i+1:]...)
				return joinGroup, newPlans, true
			}
		}
	}

	// Step 2: Match by query-block alias (subquery name)
	// Only execute this step if no direct table name match was found
	groupIdx := -1
	for i, joinGroup := range plans {
		blockOffset := joinGroup.QueryBlockOffset()
		if blockOffset > 1 && blockOffset < len(queryBlockNames) {
			blockName := queryBlockNames[blockOffset]
			dbMatch := astTbl.DBName.L == "" || astTbl.DBName.L == blockName.DBName.L
			tableMatch := astTbl.TableName.L == blockName.TableName.L
			if dbMatch && tableMatch {
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
		matched := plans[groupIdx]
		newPlans := append(plans[:groupIdx], plans[groupIdx+1:]...)
		return matched, newPlans, true
	}

	return nil, plans, false
}

// extract the number x from 'sel_x'
func extractSelectOffset(qbName string) int {
	if strings.HasPrefix(qbName, "sel_") {
		if offset, err := strconv.Atoi(qbName[4:]); err == nil {
			return offset
		}
	}
	return -1
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
func (s *baseSingleGroupJoinOrderSolver) checkConnection(leftPlan, rightPlan base.LogicalPlan) (leftNode, rightNode base.LogicalPlan, usedEdges []*expression.ScalarFunction, joinType *joinTypeWithExtMsg) {
	joinType = &joinTypeWithExtMsg{JoinType: base.InnerJoin}
	leftNode, rightNode = leftPlan, rightPlan

	for idx, edge := range s.eqEdges {
		args := edge.GetArgs()
		if len(args) != 2 {
			continue
		}
		lExpr, rExpr := args[0], args[1]

		// Check if this edge connects leftPlan and rightPlan (lExpr from left, rExpr from right)
		// After substitution in extractJoinGroup, expressions only contain base table columns.
		if expression.ExprFromSchema(lExpr, leftPlan.Schema()) && expression.ExprFromSchema(rExpr, rightPlan.Schema()) {
			joinType = s.joinTypes[idx]
			// Build the join edge, materializing expressions as columns if needed
			newEdge := s.buildJoinEdge(edge, lExpr, rExpr, &leftPlan, &rightPlan)
			leftNode, rightNode = leftPlan, rightPlan
			usedEdges = append(usedEdges, newEdge)
		} else if expression.ExprFromSchema(lExpr, rightPlan.Schema()) && expression.ExprFromSchema(rExpr, leftPlan.Schema()) {
			// Edge connects in reverse order (lExpr from right, rExpr from left)
			joinType = s.joinTypes[idx]
			if joinType.JoinType != base.InnerJoin {
				// For outer joins, keep outer/inner side semantics by swapping the node positions.
				// Note: buildJoinEdge may inject projections, so we must set leftNode/rightNode AFTER it.
				newEdge := s.buildJoinEdge(edge, lExpr, rExpr, &rightPlan, &leftPlan)
				leftNode, rightNode = rightPlan, leftPlan
				usedEdges = append(usedEdges, newEdge)
			} else {
				// For inner joins, swap the arguments and potentially inject projections
				// Build new edge with swapped arguments: rExpr = lExpr becomes rExpr on left, lExpr on right
				newEdge := s.buildJoinEdge(edge, rExpr, lExpr, &leftPlan, &rightPlan)
				leftNode, rightNode = leftPlan, rightPlan
				usedEdges = append(usedEdges, newEdge)
			}
		}
	}
	return
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
	expr2Col := make(map[string]*expression.Column, 2)
	if !lIsCol {
		*leftPlan, lCol = s.injectExpr(*leftPlan, lExpr)
		expr2Col[string(lExpr.CanonicalHashCode())] = lCol
	}
	if !rIsCol {
		*rightPlan, rCol = s.injectExpr(*rightPlan, rExpr)
		expr2Col[string(rExpr.CanonicalHashCode())] = rCol
	}
	if len(expr2Col) > 0 && len(s.otherConds) > 0 {
		// Reuse the injected expression columns in non-eq conditions to avoid recomputation.
		s.otherConds = substituteExprsWithColsInExprs(s.otherConds, expr2Col)
	}

	// Create the final edge with column arguments
	newSf := expression.NewFunctionInternal(s.ctx.GetExprCtx(), funcName, originalEdge.GetStaticType(), lCol, rCol)
	return newSf.(*expression.ScalarFunction)
}

func (*baseSingleGroupJoinOrderSolver) injectExpr(p base.LogicalPlan, expr expression.Expression) (base.LogicalPlan, *expression.Column) {
	proj, ok := p.(*logicalop.LogicalProjection)
	if !ok {
		proj = logicalop.LogicalProjection{Exprs: cols2Exprs(p.Schema().Columns)}.Init(p.SCtx(), p.QueryBlockOffset())
		proj.SetSchema(p.Schema().Clone())
		proj.SetChildren(p)
	}
	// Avoid injecting duplicate expressions into the same projection.
	// This keeps plans smaller and allows later predicates to reuse computed columns.
	substituted := expression.ColumnSubstitute(proj.SCtx().GetExprCtx(), expr, proj.Schema(), proj.Exprs)
	for i, e := range proj.Exprs {
		if expression.ExpressionsSemanticEqual(e, substituted) {
			return proj, proj.Schema().Columns[i]
		}
	}
	return proj, proj.AppendExpr(expr)
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
	s.setNewJoinWithHint(join)
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

// setNewJoinWithHint sets the join method hint for the join node.
// Before the join reorder process, we split the join node and collect the join method hint.
// And we record the join method hint and reset the hint after we have finished the join reorder process.
func (s *baseSingleGroupJoinOrderSolver) setNewJoinWithHint(newJoin *logicalop.LogicalJoin) {
	lChild := newJoin.Children()[0]
	rChild := newJoin.Children()[1]
	if joinMethodHint, ok := s.findJoinMethodHintInfo(lChild); ok {
		newJoin.LeftPreferJoinType = joinMethodHint.preferredJoinMethod
		newJoin.HintInfo = joinMethodHint.joinMethodHintInfo
	}
	if joinMethodHint, ok := s.findJoinMethodHintInfo(rChild); ok {
		newJoin.RightPreferJoinType = joinMethodHint.preferredJoinMethod
		newJoin.HintInfo = joinMethodHint.joinMethodHintInfo
	}
	newJoin.SetPreferredJoinType()
}

func (s *baseSingleGroupJoinOrderSolver) findJoinMethodHintInfo(p base.LogicalPlan) (*joinMethodHint, bool) {
	if len(s.joinMethodHintInfo) == 0 {
		return nil, false
	}
	cur := p
	for {
		if hint, ok := s.joinMethodHintInfo[cur.ID()]; ok {
			return hint, true
		}
		children := cur.Children()
		if len(children) != 1 {
			return nil, false
		}
		switch cur.(type) {
		case *logicalop.LogicalProjection, *logicalop.LogicalSelection:
			cur = children[0]
		default:
			return nil, false
		}
	}
}

// calcJoinCumCost calculates the cumulative cost of the join node.
func (*baseSingleGroupJoinOrderSolver) calcJoinCumCost(join base.LogicalPlan, lNode, rNode *jrNode) float64 {
	return join.StatsInfo().RowCount + lNode.cumCost + rNode.cumCost
}

// Name implements the base.LogicalOptRule.<1st> interface.
func (*JoinReOrderSolver) Name() string {
	return "join_reorder"
}
