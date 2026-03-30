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

package rule

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
)

// JoinKeyTypeCastRewriter rewrites join conditions where implicit type
// conversion prevents index usage.
//
// By the time this rule runs (after column pruning and predicate pushdown),
// a condition like t_int.id = t_varchar.id has been transformed into:
//
//	EqualConditions: proj_col_L = proj_col_R  (both type DOUBLE)
//	Left child Projection:  [..., CAST(int_col AS DOUBLE) -> proj_col_L]
//	Right child Projection: [..., CAST(varchar_col AS DOUBLE) -> proj_col_R]
//
// This rule detects the pattern and rewrites:
//
//	EqualConditions: new_col_L = new_col_R  (both type INT)
//	Left child Projection:  [..., int_col -> new_col_L]            (cast removed)
//	Right child Projection: [..., CAST(varchar_col AS SIGNED) -> new_col_R]
//	Guard Selection on VARCHAR side
//
// The guard predicate filters non-integer VARCHAR values:
//
//	CAST(CAST(varchar_col AS SIGNED) AS DOUBLE) = CAST(varchar_col AS DOUBLE)
type JoinKeyTypeCastRewriter struct{}

// Optimize implements base.LogicalOptRule.
func (*JoinKeyTypeCastRewriter) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	newP, changed := rewriteJoinTypeCasts(p)
	return newP, changed, nil
}

// Name implements base.LogicalOptRule.
func (*JoinKeyTypeCastRewriter) Name() string {
	return "join_key_type_cast"
}

// rewriteJoinTypeCasts recursively walks the plan tree.
func rewriteJoinTypeCasts(p base.LogicalPlan) (base.LogicalPlan, bool) {
	anyChanged := false
	for i, child := range p.Children() {
		newChild, changed := rewriteJoinTypeCasts(child)
		if changed {
			p.SetChild(i, newChild)
			anyChanged = true
		}
	}
	join, ok := p.(*logicalop.LogicalJoin)
	if !ok {
		return p, anyChanged
	}
	changed := rewriteJoinEqConds(join)
	return p, anyChanged || changed
}

// projCastInfo describes a CAST-to-DOUBLE expression in a child Projection.
type projCastInfo struct {
	childIdx int // 0=left, 1=right
	proj     *logicalop.LogicalProjection
	exprIdx  int                // index in Projection.Exprs
	origCol  *expression.Column // the underlying column inside CAST
}

// rewriteJoinEqConds scans EqualConditions for DOUBLE-typed column pairs
// that were produced by CAST-to-DOUBLE Projections, and rewrites INT-vs-VARCHAR
// cases to use integer equality.
func rewriteJoinEqConds(join *logicalop.LogicalJoin) bool {
	if len(join.EqualConditions) == 0 {
		return false
	}

	// Both children must be Projections (inserted by column pruning).
	leftProj, leftOk := join.Children()[0].(*logicalop.LogicalProjection)
	rightProj, rightOk := join.Children()[1].(*logicalop.LogicalProjection)
	if !leftOk || !rightOk {
		return false
	}

	ctx := join.SCtx()
	exprCtx := ctx.GetExprCtx()
	evalCtx := exprCtx.GetEvalCtx()
	anyChanged := false

	// Process each EqualCondition.
	for eqIdx := 0; eqIdx < len(join.EqualConditions); eqIdx++ {
		eq := join.EqualConditions[eqIdx]
		args := eq.GetArgs()
		if len(args) != 2 {
			continue
		}
		colL, okL := args[0].(*expression.Column)
		colR, okR := args[1].(*expression.Column)
		if !okL || !okR {
			continue
		}
		// Both must be DOUBLE BINARY (the telltale sign of implicit CAST).
		if colL.GetType(evalCtx).EvalType() != types.ETReal || colR.GetType(evalCtx).EvalType() != types.ETReal {
			continue
		}

		// Trace each column to its Projection expression.
		leftInfo := findCastInProj(leftProj, colL, evalCtx)
		rightInfo := findCastInProj(rightProj, colR, evalCtx)
		if leftInfo == nil || rightInfo == nil {
			continue
		}

		// Classify: one side must be signed INT, the other STRING.
		intInfo, strInfo := classifyCastPair(leftInfo, rightInfo)
		if intInfo == nil || strInfo == nil {
			continue
		}

		// Rewrite the INT-side Projection: CAST(int_col AS DOUBLE) → int_col
		intInfo.proj.Exprs[intInfo.exprIdx] = intInfo.origCol.Clone()
		intSchemaCol := intInfo.proj.Schema().Columns[intInfo.exprIdx]
		intSchemaCol.RetType = intInfo.origCol.RetType.Clone()

		// Rewrite the VARCHAR-side Projection: CAST(varchar_col AS DOUBLE) → CAST(varchar_col AS SIGNED)
		castIntExpr := expression.WrapWithCastAsInt(exprCtx, strInfo.origCol.Clone(), intInfo.origCol.RetType)
		strInfo.proj.Exprs[strInfo.exprIdx] = castIntExpr
		strSchemaCol := strInfo.proj.Schema().Columns[strInfo.exprIdx]
		strSchemaCol.RetType = castIntExpr.GetType(evalCtx).Clone()

		// Add guard Selection below the VARCHAR-side Projection.
		guardLeft := expression.WrapWithCastAsReal(exprCtx, expression.WrapWithCastAsInt(exprCtx, strInfo.origCol.Clone(), intInfo.origCol.RetType))
		guardRight := expression.WrapWithCastAsReal(exprCtx, strInfo.origCol.Clone())
		guard := expression.NewFunctionInternal(
			exprCtx,
			ast.EQ,
			types.NewFieldType(mysql.TypeTiny),
			guardLeft,
			guardRight,
		)
		strProjChild := strInfo.proj.Children()[0]
		sel := logicalop.LogicalSelection{Conditions: []expression.Expression{guard}}.Init(ctx, join.QueryBlockOffset())
		sel.SetChildren(strProjChild)
		strInfo.proj.SetChildren(sel)

		// Replace the EQ condition with a new one comparing INT columns.
		newEq := expression.NewFunctionInternal(
			exprCtx,
			eq.FuncName.L,
			types.NewFieldType(mysql.TypeTiny),
			intSchemaCol.Clone(),
			strSchemaCol.Clone(),
		)
		if sf, ok := newEq.(*expression.ScalarFunction); ok {
			join.EqualConditions[eqIdx] = sf
		}

		anyChanged = true
	}

	if anyChanged {
		join.MergeSchema()
	}
	return anyChanged
}

// findCastInProj looks up a column in a Projection's schema and checks if the
// corresponding expression is CAST(col AS DOUBLE). Returns nil if not matched.
func findCastInProj(proj *logicalop.LogicalProjection, col *expression.Column, evalCtx expression.EvalContext) *projCastInfo {
	schema := proj.Schema()
	for i, schemaCol := range schema.Columns {
		if schemaCol.UniqueID != col.UniqueID {
			continue
		}
		if i >= len(proj.Exprs) {
			return nil
		}
		sf, ok := proj.Exprs[i].(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.Cast {
			return nil
		}
		if sf.GetType(evalCtx).EvalType() != types.ETReal {
			return nil
		}
		args := sf.GetArgs()
		if len(args) != 1 {
			return nil
		}
		origCol, ok := args[0].(*expression.Column)
		if !ok {
			return nil
		}
		return &projCastInfo{
			proj:    proj,
			exprIdx: i,
			origCol: origCol,
		}
	}
	return nil
}

// classifyCastPair determines which side is INT (signed) and which is STRING.
func classifyCastPair(leftInfo, rightInfo *projCastInfo) (intInfo, strInfo *projCastInfo) {
	isSignedInt := func(info *projCastInfo) bool {
		tp := info.origCol.GetType(nil)
		return tp.EvalType() == types.ETInt && !mysql.HasUnsignedFlag(tp.GetFlag())
	}
	isStr := func(info *projCastInfo) bool {
		return info.origCol.GetType(nil).EvalType().IsStringKind()
	}

	switch {
	case isSignedInt(leftInfo) && isStr(rightInfo):
		return leftInfo, rightInfo
	case isStr(leftInfo) && isSignedInt(rightInfo):
		return rightInfo, leftInfo
	default:
		return nil, nil
	}
}
