// Copyright 2026 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

// This file contains helpers for join reorder "look through Projection" optimization:
// - best-effort inlining checks for Projection on top of Join
// - derived column mapping (colExprMap) propagation and substitution
// - shared expression rewrite helpers used by the optimization (and join-key materialization)

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

type exprReplacer func(expr expression.Expression) (newExpr expression.Expression, replaced bool)

// rewriteExprTree rewrites an expression tree in a best-effort, copy-on-write way.
//
// The replacer is applied in pre-order (parent before children). If it replaces a node,
// the returned expression will be rewritten again so callers can implement recursive
// substitutions (e.g. colExprMap chains) without duplicating traversal logic.
func rewriteExprTree(expr expression.Expression, replace exprReplacer) expression.Expression {
	if expr == nil {
		return nil
	}

	if replace != nil {
		if newExpr, replaced := replace(expr); replaced {
			if newExpr == nil {
				return nil
			}
			if newExpr != expr {
				return rewriteExprTree(newExpr, replace)
			}
		}
	}

	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return expr
	}

	// Copy-on-write: only clone the function node when any argument changes.
	oldArgs := sf.GetArgs()
	var newArgs []expression.Expression
	for i, arg := range oldArgs {
		rewrittenArg := rewriteExprTree(arg, replace)
		if newArgs == nil {
			if rewrittenArg == arg {
				continue
			}
			newArgs = make([]expression.Expression, len(oldArgs))
			copy(newArgs, oldArgs[:i])
		}
		newArgs[i] = rewrittenArg
	}
	if newArgs == nil {
		return sf
	}

	newSf := sf.Clone().(*expression.ScalarFunction)
	args := newSf.GetArgs()
	for i := range args {
		args[i] = newArgs[i]
	}
	// Args changed: clear cached hash so CanonicalHashCode reflects rewritten children.
	newSf.CleanHashCode()
	return newSf
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
	return rewriteExprTree(expr, func(e expression.Expression) (expression.Expression, bool) {
		if col, ok := expr2Col[string(e.CanonicalHashCode())]; ok {
			return col, true
		}
		return e, false
	})
}

// substituteColsInExpr recursively substitutes derived columns in an expression using colExprMap.
// It replaces column references with their defining expressions from colExprMap.
func substituteColsInExpr(expr expression.Expression, colExprMap map[int64]expression.Expression) expression.Expression {
	if len(colExprMap) == 0 {
		return expr
	}
	return rewriteExprTree(expr, func(e expression.Expression) (expression.Expression, bool) {
		col, ok := e.(*expression.Column)
		if !ok {
			return e, false
		}
		if defExpr, ok := colExprMap[col.UniqueID]; ok {
			// Expressions in colExprMap are treated as immutable in join-reorder flow.
			// Reuse pointers to avoid extra clones/allocations; if a future pass starts
			// mutating these expression trees in-place, clone defExpr here before return.
			return defExpr, true
		}
		return e, false
	})
}

// tryInlineProjectionForJoinGroup tries to extract the join group through a Projection on top of a Join.
//
// It returns (result, true) when the Projection node is handled:
//   - inlined successfully (join group from child is returned, with updated colExprMap), or
//   - determined unsafe to inline (treat this Projection as an atomic leaf).
//
// It returns (nil, false) when the caller should continue with normal extractJoinGroupImpl logic.
func tryInlineProjectionForJoinGroup(p base.LogicalPlan, proj *logicalop.LogicalProjection) (*joinGroupResult, bool) {
	// Only inline projections whose child is a join; otherwise the projection is treated as an atomic leaf.
	child := proj.Children()[0]
	if _, isJoin := child.(*logicalop.LogicalJoin); !isJoin {
		return nil, false
	}

	// Fast safety checks first to avoid unnecessary recursion.
	if !canInlineProjectionBasic(proj) {
		return nil, false
	}

	childResult := extractJoinGroupImpl(child)
	// The single-leaf check needs the extracted join group and child's colExprMap.
	if !canInlineProjection(proj, childResult) {
		// Cannot safely inline this projection (e.g. expression spans multiple leaves, or
		// a column is shared by multiple leaves). Treat it as an atomic leaf.
		return &joinGroupResult{
			group:              []base.LogicalPlan{p},
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}, true
	}

	// Only record when projection inlining actually happens (best-effort feature).
	proj.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptJoinReorderThroughProj)

	childResult.colExprMap = buildColExprMapForProjection(proj, childResult.colExprMap)
	return childResult, true
}

func buildColExprMapForProjection(
	proj *logicalop.LogicalProjection,
	childColExprMap map[int64]expression.Expression,
) map[int64]expression.Expression {
	colExprMap := make(map[int64]expression.Expression, len(proj.Schema().Columns)+len(childColExprMap))
	for i, outCol := range proj.Schema().Columns {
		expr := proj.Exprs[i]
		// Use child's colExprMap to substitute column references in current expression.
		if len(childColExprMap) > 0 {
			expr = substituteColsInExpr(expr, childColExprMap)
		}

		// For pass-through columns (where the projection expression is just a Column reference
		// that has been substituted to the underlying expression), we need to check if the output
		// column's UniqueID matches the input column's UniqueID. This can happen when a projection
		// simply passes through a column without transformation (e.g., SELECT dt1.key_a FROM ...).
		// In such cases, the output column may share the same UniqueID as the input column.
		//
		// If expr is a Column and its UniqueID equals outCol.UniqueID, this is a pass-through scenario.
		// We should NOT add it to colExprMap because:
		// 1. It would create a self-referential mapping (UniqueID -> Column with same UniqueID)
		// 2. It could conflict with child's colExprMap entry for the same UniqueID
		//
		// The child's colExprMap will be merged later and will provide the proper mapping for this UniqueID.
		if colExpr, isCol := expr.(*expression.Column); isCol && colExpr.UniqueID == outCol.UniqueID {
			continue
		}
		colExprMap[outCol.UniqueID] = expr
	}

	for k, v := range childColExprMap {
		if _, exists := colExprMap[k]; !exists {
			colExprMap[k] = v
		}
	}
	return colExprMap
}

// outerJoinSideFiltersTouchMultipleLeaves checks whether the outer-join filters depend on more than one
// leaf on the outer side. If so, we conservatively disable join reordering for this join node.
//
// When projections are inlined under the outer side, join conditions may reference derived columns that
// are not contained in any leaf schema. We substitute those derived columns via `outerColExprMap` before
// extracting referenced columns.
func outerJoinSideFiltersTouchMultipleLeaves(
	join *logicalop.LogicalJoin,
	outerGroup []base.LogicalPlan,
	outerColExprMap map[int64]expression.Expression,
	outerIsLeft bool,
) bool {
	if join == nil {
		return false
	}

	checkOtherConds := join.OtherConditions
	checkSideConds := join.RightConditions
	if outerIsLeft {
		checkSideConds = join.LeftConditions
	}
	checkEQConds := expression.ScalarFuncs2Exprs(join.EqualConditions)

	if len(outerColExprMap) > 0 {
		checkOtherConds = substituteColsInExprs(checkOtherConds, outerColExprMap)
		checkSideConds = substituteColsInExprs(checkSideConds, outerColExprMap)
		checkEQConds = substituteColsInExprs(checkEQConds, outerColExprMap)
	}

	extractedCols := make(map[int64]*expression.Column, len(checkOtherConds)+len(checkSideConds)+len(checkEQConds))
	expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkOtherConds...)
	expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkSideConds...)
	expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkEQConds...)

	affectedGroups := 0
	for _, outerLeaf := range outerGroup {
		leafSchema := outerLeaf.Schema()
		for _, col := range extractedCols {
			if leafSchema.Contains(col) {
				affectedGroups++
				break
			}
		}
		if affectedGroups > 1 {
			return true
		}
	}
	return false
}

// isInlineableProjectionExpr checks whether `expr` only consists of supported expression nodes,
// recursively. The inlining/substitution helpers (substituteColsInExpr / ExtractColumns) only
// reason about Column/ScalarFunction/Constant trees; other Expression implementations (e.g.
// ScalarSubQueryExpr) may hide mutability/correlation and lead to incorrect inlining.
func isInlineableProjectionExpr(expr expression.Expression) bool {
	switch x := expr.(type) {
	case *expression.Column:
		return true
	case *expression.ScalarFunction:
		for _, arg := range x.GetArgs() {
			if !isInlineableProjectionExpr(arg) {
				return false
			}
		}
		return true
	case *expression.Constant:
		// Constant with DeferredExpr may wrap mutable/non-deterministic functions (e.g. NOW()).
		return x.DeferredExpr == nil
	default:
		return false
	}
}

// canInlineProjectionBasic checks if a projection is safe to inline, independent of join-group shape.
func canInlineProjectionBasic(proj *logicalop.LogicalProjection) bool {
	// Proj4Expand projections cannot be inlined
	if proj.Proj4Expand {
		return false
	}

	for _, expr := range proj.Exprs {
		// We only inline projection expressions that actually reference columns.
		// Constant-only expressions (including deterministic "pure functions") are rejected
		// to keep substitution and edge classification simple.
		//
		// TODO: Support inlining constant-only / pure-function expressions. When such derived
		// columns are substituted into join equalities, an original "col = col" join key can
		// degenerate into "expr-without-cols = col" (or even a constant), which needs extra
		// handling in eq-edge normalization and connector classification.
		if len(expression.ExtractColumns(expr)) == 0 {
			return false
		}
		// Reject projections that contain unsupported expression nodes anywhere in the tree.
		if !isInlineableProjectionExpr(expr) {
			return false
		}
		// No volatile / side-effect expressions: join reorder may move/duplicate evaluation.
		if expression.IsMutableEffectsExpr(expr) || expression.CheckNonDeterministic(expr) || expr.IsCorrelated() {
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
			return false
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
