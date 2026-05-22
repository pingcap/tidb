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
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// JoinKeyTypeCastRewriter rewrites implicit type cast predicates to enable
// index usage.
//
// When a query contains WHERE varchar_col = 123, type coercion wraps the
// column in CAST(varchar_col AS DOUBLE), which prevents the ranger from
// building index ranges. This rule detects the pattern and adds first-
// character prefix predicates (OR of LIKE patterns) that the ranger CAN
// use for index range building. The original CAST equality remains as a
// filter for correctness.
//
// For integer 123, the valid first characters of strings that CAST to
// 123.0 as DOUBLE are: whitespace (trimmed by TrimSpace), '+', '.',
// '0' (leading zero), and '1' (first significant digit). The rule adds:
//
//	(col LIKE ' %' OR col LIKE '\t%' OR ... OR col LIKE '1%')
//
// The ranger converts each LIKE to an index range. The original
// CAST(varchar_col AS DOUBLE) = 123.0 stays as a table filter to remove
// false positives, guaranteeing correctness.
//
// For join conditions like t_int.id = t_varchar.id, type coercion produces:
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
//	Guard Selection on VARCHAR side:
//	  CAST(CAST(varchar_col AS SIGNED) AS DOUBLE) = CAST(varchar_col AS DOUBLE)
//
// Limitation: TiDB's expression framework does not distinguish implicit CASTs
// (inserted by type coercion) from explicit user-written CASTs. If a user
// writes CAST(int_col AS DOUBLE) = CAST(varchar_col AS DOUBLE) in a join
// condition, this rule will rewrite it too. In practice the semantic difference
// is negligible: it only manifests for integers outside DOUBLE's exact range
// (>2^53), where the INT comparison is arguably more correct than DOUBLE.
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

// rewriteImplicitCasts recursively walks the plan tree.
func rewriteImplicitCasts(p base.LogicalPlan) bool {
	changed := false
	for _, child := range p.Children() {
		if rewriteImplicitCasts(child) {
			changed = true
		}
	}

	switch x := p.(type) {
	case *logicalop.DataSource:
		if rewriteDataSourceCastPredicates(x) {
			changed = true
		}
	case *logicalop.LogicalJoin:
		if rewriteJoinEqConds(x) {
			changed = true
		}
	}

	return changed
}

// rewriteDataSourceCastPredicates scans DataSource.PushedDownConds for
// CAST(varchar_col AS DOUBLE) = int_const patterns and adds OR-of-LIKE
// prefix predicates that the ranger can use for index range building.
func rewriteDataSourceCastPredicates(ds *logicalop.DataSource) bool {
	if len(ds.PushedDownConds) == 0 {
		return false
	}

	ctx := ds.SCtx()
	var newConds []expression.Expression

	for _, cond := range ds.PushedDownConds {
		col, intVal, ok := extractCastEqPattern(ctx, cond)
		if !ok {
			continue
		}
		likePred := buildPrefixLikePredicate(ctx, col, intVal)
		if likePred != nil {
			newConds = append(newConds, likePred)
		}
	}

	if len(newConds) == 0 {
		return false
	}

	ds.PushedDownConds = append(ds.PushedDownConds, newConds...)
	ds.AllConds = append(ds.AllConds, newConds...)
	return true
}

// extractCastEqPattern detects the pattern EQ(CAST(varchar_col AS DOUBLE), const)
// where the constant is an exact integer value. Returns the original column
// and integer value if matched.
func extractCastEqPattern(sctx base.PlanContext, cond expression.Expression) (*expression.Column, int64, bool) {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return nil, 0, false
	}
	args := sf.GetArgs()
	if len(args) != 2 {
		return nil, 0, false
	}

	evalCtx := sctx.GetExprCtx().GetEvalCtx()

	// Try both orderings: CAST(col) = const and const = CAST(col).
	for i := range 2 {
		castArg, constArg := args[i], args[1-i]

		castSF, ok1 := castArg.(*expression.ScalarFunction)
		constExpr, ok2 := constArg.(*expression.Constant)
		if !ok1 || !ok2 {
			continue
		}

		// The CAST must target DOUBLE.
		if castSF.FuncName.L != ast.Cast {
			continue
		}
		if castSF.GetType(evalCtx).EvalType() != types.ETReal {
			continue
		}

		// Inner of CAST must be a Column with string type.
		castArgs := castSF.GetArgs()
		if len(castArgs) != 1 {
			continue
		}
		col, ok3 := castArgs[0].(*expression.Column)
		if !ok3 || !col.GetType(evalCtx).EvalType().IsStringKind() {
			continue
		}

		// Constant must be DOUBLE with an exact integer value.
		if constExpr.GetType(evalCtx).EvalType() != types.ETReal {
			continue
		}
		val, isNull, err := constExpr.EvalReal(evalCtx, chunk.Row{})
		if err != nil || isNull {
			continue
		}
		if math.IsInf(val, 0) || math.IsNaN(val) {
			continue
		}
		if math.Floor(val) != val {
			continue
		}
		if val > float64(math.MaxInt64) || val < float64(math.MinInt64) {
			continue
		}
		// Beyond 2^53, DOUBLE cannot exactly represent every integer.
		// Multiple distinct integers map to the same DOUBLE value, and
		// their string representations may start with different digits,
		// so our first-character prefix ranges would be incomplete.
		const maxExactInt = 1 << 53
		if val > maxExactInt || val < -maxExactInt {
			continue
		}
		return col, int64(val), true
	}
	return nil, 0, false
}

// validFirstChars returns the set of byte values that can appear as the
// first character of a string whose CAST to DOUBLE equals intVal.
//
// TiDB's StrToFloat (pkg/types/convert.go) calls strings.TrimSpace first,
// then getValidFloatPrefix accepts: +, -, ., 0-9 as valid first characters.
func validFirstChars(intVal int64) []byte {
	if intVal == 0 {
		// Zero also matches empty and non-numeric strings via
		// CAST(... AS DOUBLE) (e.g. CAST('abc' AS DOUBLE) = 0),
		// so a prefix filter is not a sound implication here.
		return nil
	}

	// All characters stripped by strings.TrimSpace that could precede
	// the numeric content in the stored string value.
	wsChars := []byte{' ', '\t', '\n', '\r', '\x0b', '\x0c'}

	switch {
	case intVal > 0:
		d := firstSignificantDigit(intVal)
		return append(wsChars, '+', '.', '0', d)
	default: // intVal < 0
		return append(wsChars, '-')
	}
}

// firstSignificantDigit returns the leading digit of |n| as a byte.
// For example, 123 → '1', 9 → '9', 50 → '5'.
func firstSignificantDigit(n int64) byte {
	if n < 0 {
		n = -n
	}
	for n >= 10 {
		n /= 10
	}
	return byte('0' + n)
}

// buildPrefixLikePredicate constructs an OR of LIKE 'char%' predicates for
// each valid first character, using col as the match target.
func buildPrefixLikePredicate(sctx base.PlanContext, col *expression.Column, intVal int64) expression.Expression {
	chars := validFirstChars(intVal)
	if len(chars) == 0 {
		return nil
	}

	exprCtx := sctx.GetExprCtx()
	evalCtx := exprCtx.GetEvalCtx()
	retType := types.NewFieldType(mysql.TypeLonglong)
	escapeConst := &expression.Constant{
		Value:   types.NewIntDatum(int64('\\')),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	likes := make([]expression.Expression, 0, len(chars))
	for _, ch := range chars {
		pattern := string([]byte{ch, '%'})
		patternConst := &expression.Constant{
			Value:   types.NewStringDatum(pattern),
			RetType: col.GetType(evalCtx).Clone(),
		}
		like := expression.NewFunctionInternal(exprCtx, ast.Like, retType, col.Clone(), patternConst, escapeConst)
		if like != nil {
			likes = append(likes, like)
		}
	}

	if len(likes) == 0 {
		return nil
	}

	// Chain with OR: likes[0] OR likes[1] OR ... OR likes[n-1].
	result := likes[0]
	for i := 1; i < len(likes); i++ {
		result = expression.NewFunctionInternal(exprCtx, ast.LogicOr, retType, result, likes[i])
	}
	return result
}

// ---------------------------------------------------------------------------
// Join key type cast rewriting
// ---------------------------------------------------------------------------

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
	proj    *logicalop.LogicalProjection
	exprIdx int                // index in Projection.Exprs
	origCol *expression.Column // the underlying column inside CAST
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

	// Track which Projections need guard Selections added, keyed by child index.
	type guardEntry struct {
		proj  *logicalop.LogicalProjection
		conds []expression.Expression
	}
	guards := map[int]*guardEntry{}

	// Process each EqualCondition.
	// Determine which child index is the preserved (outer) side, if any.
	// The preserved side is the one whose rows appear in the output regardless
	// of whether a match exists. Pushing a guard filter there would incorrectly
	// remove rows that should survive with NULL-padded (or false-flagged) columns.
	//
	// LEFT JOIN / LeftOuterSemiJoin / AntiLeftOuterSemiJoin / AntiSemiJoin:
	//   left (child 0) is preserved.
	// RIGHT JOIN: right (child 1) is preserved.
	// INNER JOIN / SemiJoin: no preserved side (unmatched rows are discarded).
	preservedChildIdx := -1 // -1 means no preserved side
	switch join.JoinType {
	case base.LeftOuterJoin,
		base.AntiSemiJoin,
		base.LeftOuterSemiJoin,
		base.AntiLeftOuterSemiJoin:
		preservedChildIdx = 0
	case base.RightOuterJoin:
		preservedChildIdx = 1
	}

	for eqIdx := 0; eqIdx < len(join.EqualConditions); eqIdx++ {
		eq := join.EqualConditions[eqIdx]

		// Skip null-safe equality (<=>): the guard uses = which filters
		// NULLs, breaking NULL<=>NULL semantics.
		if eq.FuncName.L == ast.NullEQ {
			continue
		}

		args := eq.GetArgs()
		if len(args) != 2 {
			continue
		}
		colL, okL := args[0].(*expression.Column)
		colR, okR := args[1].(*expression.Column)
		if !okL || !okR {
			continue
		}
		// Both must be DOUBLE (the telltale sign of implicit CAST).
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
		intInfo, strInfo := classifyCastPair(leftInfo, rightInfo, evalCtx)
		if intInfo == nil || strInfo == nil {
			continue
		}

		// Skip if the VARCHAR side is the preserved (outer) side of an
		// outer join. Pushing a guard filter there would incorrectly
		// remove rows that should be preserved with NULL-padded columns.
		strChildIdx := 1
		if strInfo.proj == leftProj {
			strChildIdx = 0
		}
		if strChildIdx == preservedChildIdx {
			continue
		}

		// INT side: add a pass-through of the bare int column, keeping
		// origCol.UniqueID so the index join builder can match it to the
		// table's PK/index.
		newIntCol := &expression.Column{
			UniqueID: intInfo.origCol.UniqueID,
			RetType:  intInfo.origCol.RetType.Clone(),
		}
		intInfo.proj.Exprs = append(intInfo.proj.Exprs, intInfo.origCol.Clone())
		intInfo.proj.Schema().Append(newIntCol)

		// VARCHAR side: add CAST(varchar_col AS SIGNED). We allocate a new
		// UniqueID because the data type changes (VARCHAR→INT).
		castIntExpr := expression.WrapWithCastAsInt(exprCtx, strInfo.origCol.Clone(), intInfo.origCol.RetType)
		newStrCol := &expression.Column{
			UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  castIntExpr.GetType(evalCtx).Clone(),
		}
		strInfo.proj.Exprs = append(strInfo.proj.Exprs, castIntExpr)
		strInfo.proj.Schema().Append(newStrCol)

		// Accumulate guard condition for the VARCHAR-side Projection.
		if guards[strChildIdx] == nil {
			guards[strChildIdx] = &guardEntry{proj: strInfo.proj}
		}
		guardLeft := expression.WrapWithCastAsReal(exprCtx, expression.WrapWithCastAsInt(exprCtx, strInfo.origCol.Clone(), intInfo.origCol.RetType))
		guardRight := expression.WrapWithCastAsReal(exprCtx, strInfo.origCol.Clone())
		guard := expression.NewFunctionInternal(
			exprCtx,
			ast.EQ,
			types.NewFieldType(mysql.TypeTiny),
			guardLeft,
			guardRight,
		)
		guards[strChildIdx].conds = append(guards[strChildIdx].conds, guard)

		// Replace the EQ condition with a new one using the NEW columns.
		var newLeftCol, newRightCol *expression.Column
		if intInfo.proj == leftProj {
			newLeftCol, newRightCol = newIntCol, newStrCol
		} else {
			newLeftCol, newRightCol = newStrCol, newIntCol
		}
		newEq := expression.NewFunctionInternal(
			exprCtx,
			eq.FuncName.L,
			types.NewFieldType(mysql.TypeTiny),
			newLeftCol.Clone(),
			newRightCol.Clone(),
		)
		if sf, ok := newEq.(*expression.ScalarFunction); ok {
			join.EqualConditions[eqIdx] = sf
		}

		anyChanged = true
	}

	// Insert guard Selections below the affected Projections.
	for _, g := range guards {
		projChild := g.proj.Children()[0]
		sel := logicalop.LogicalSelection{Conditions: g.conds}.Init(ctx, join.QueryBlockOffset())
		sel.SetChildren(projChild)
		g.proj.SetChildren(sel)
	}

	// Note: we do NOT call MergeSchema() here. The new columns are only used
	// internally by EqualConditions as join keys, not by the join's output.
	// Column pruning (FlagPruneColumnsAgain) runs later and will rebuild
	// schemas correctly.
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
func classifyCastPair(leftInfo, rightInfo *projCastInfo, evalCtx expression.EvalContext) (intInfo, strInfo *projCastInfo) {
	isSignedInt := func(info *projCastInfo) bool {
		tp := info.origCol.GetType(evalCtx)
		return tp.EvalType() == types.ETInt && !mysql.HasUnsignedFlag(tp.GetFlag())
	}
	isStr := func(info *projCastInfo) bool {
		return info.origCol.GetType(evalCtx).EvalType().IsStringKind()
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
