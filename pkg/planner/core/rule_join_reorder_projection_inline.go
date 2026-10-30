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
	"github.com/pingcap/tidb/pkg/planner/core/joinorder"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

// This file contains helpers for join reorder "look through Projection" optimization:
// - best-effort inlining checks for Projection on top of Join
// - derived column mapping (colExprMap) propagation and inlining safety checks

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
			expr = joinorder.SubstituteColsInExpr(expr, childColExprMap)
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
			checkExpr = joinorder.SubstituteColsInExpr(checkExpr, childResult.colExprMap)
		}
		if childResult.nullExtendedCols != nil && expression.ExprReferenceSchema(checkExpr, childResult.nullExtendedCols) {
			// Expressions that reference the null-extended side of an outer join must stay above
			// that outer join. Their value can depend on the outer join having already produced
			// a NULL-extended row, so they are not ordinary "leaf-local" expressions.
			//
			// Example:
			//   SELECT * FROM (
			//     SELECT t1.id, IFNULL(t2.v, 0) AS k
			//     FROM t1 LEFT JOIN t2 ON t1.id = t2.id
			//   ) dt JOIN t3 ON dt.k = t3.v;
			//
			// Here `IFNULL(t2.v, 0)` only becomes `0` after the LEFT JOIN has manufactured the
			// null-extended t2 row for an unmatched t1 row. If we inline such an expression here,
			// later join reorder may treat it as a normal single-leaf connector and move joins or
			// filters below the outer join, which can change SQL semantics.
			//
			// The current legacy through-proj implementation is intentionally conservative:
			// projection inline is all-or-nothing, and once a projection is accepted we globally
			// substitute derived columns through colExprMap before restoring the output schema.
			// Blocking every reference to nullExtendedCols is therefore a coarse correctness fence.
			//
			// TODO: Relax this for provably-harmless cases. In particular, an injected projection
			// created for join-key materialization may carry identity pass-through columns from the
			// null-extended side even though those columns keep their original UniqueIDs and never
			// participate in colExprMap substitution. More precise options include partial inline or
			// materializing join keys on the minimal subtree instead of on the whole child.
			return false
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
