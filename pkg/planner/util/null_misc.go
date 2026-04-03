// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// Null-reject proof for outer-join simplification.
//
// When every column in an inner (null-producing) schema is set to SQL NULL, a
// predicate is "null-rejected" if it cannot evaluate to TRUE — proving the
// outer join can be simplified to an inner join.
//
// The proof tracks two related but distinct guarantees per sub-expression:
//
//   - nonTrue: the expression cannot be TRUE (it is FALSE or NULL).
//   - mustNull: the expression must be NULL.
//
// Both are needed because SQL uses three-valued logic. For example, NOT(expr)
// is nonTrue only when expr is mustNull (since NOT(NULL) = NULL, which is
// nonTrue; but NOT(FALSE) = TRUE, which is not). Similarly, OR(a, b) is
// nonTrue only when both sides are nonTrue, but its mustNull requires both
// sides to be mustNull.
//
// Two classification tables drive the proof for builtin scalar functions:
//
//   - nullRejectNullPreservingFunctions: functions that return NULL whenever any
//     argument is NULL. A mustNull argument propagates mustNull through these.
//   - nullRejectRejectNullTests: IS TRUE / IS FALSE style tests that convert
//     NULL to a definite boolean. These produce nonTrue when the child is
//     mustNull.
//
// The classification is intentionally conservative: unclassified builtins are
// treated as opaque (no proof is derived), which reduces optimization
// opportunities but cannot cause incorrect join simplification. When adding a
// builtin to either table, also update TestNullRejectBuiltinRegistrySnapshot
// which guards against silent registry drift.
//
// The proof works in two phases. First it reasons symbolically using the two
// proof bits above. Before giving up on a sub-expression, it also tries a
// "nullify then fold" bridge via tryFoldNullifiedConstant: replace inner-side
// columns with typed SQL NULL, and if the result becomes an immutable constant,
// classify that exact value. This recovers cases such as COALESCE/IF/IFNULL
// that may hide NULL but still collapse after nullification. The bridge stays
// conservative for plan-cache-sensitive expressions by refusing to treat
// ParamMarker/DeferredExpr values as static fold results.

// nullRejectProof holds the two proof results for a sub-expression.
// See the file-level comment above for the full model.
type nullRejectProof struct {
	nonTrue  bool
	mustNull bool
}

// allConstants checks whether the expression tree consists entirely of constants.
func allConstants(ctx expression.BuildContext, expr expression.Expression) bool {
	if expression.MaybeOverOptimized4PlanCache(ctx, expr) {
		return false
	}
	switch v := expr.(type) {
	case *expression.ScalarFunction:
		for _, arg := range v.GetArgs() {
			if !allConstants(ctx, arg) {
				return false
			}
		}
		return true
	case *expression.Constant:
		return true
	}
	return false
}

// IsNullRejected proves whether `predicate` can be TRUE after every column in
// `innerSchema` is replaced with SQL NULL.
func IsNullRejected(ctx base.PlanContext, innerSchema *expression.Schema, predicate expression.Expression,
	skipPlanCacheCheck bool) bool {
	_ = skipPlanCacheCheck // kept for API compatibility; the new proof does not use EvaluateExprWithNull
	predicate = expression.PushDownNot(ctx.GetNullRejectCheckExprCtx(), predicate)
	return proveNullRejected(ctx, innerSchema, predicate).nonTrue
}

// proveNullRejected recursively proves the two proof bits for one expression.
//
// The proof first tries "nullify then fold": replace inner-side columns with
// SQL NULL and fold the expression if that becomes possible. This covers
// null-hiding wrappers such as COALESCE/IF that cannot be proven by looking at
// the top-level builtin alone.
//
// Example:
//
//	COALESCE(t2.a, 2) > 2
//
// becomes
//
//	COALESCE(NULL, 2) > 2
//
// then folds to
//
//	2 > 2
//
// so the predicate is nonTrue.
func proveNullRejected(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	expr expression.Expression,
) nullRejectProof {
	if cons, ok := tryFoldNullifiedConstant(ctx, innerSchema, expr); ok {
		return proofFromConstant(ctx, cons)
	}

	switch x := expr.(type) {
	case *expression.Column:
		if innerSchema.Contains(x) {
			// A bare inner-side column becomes NULL after outer-join null
			// extension, so it can never be TRUE by itself.
			//
			// Example:
			//   SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a WHERE t2.b;
			// Here `t2.b` is the column `x`. For unmatched rows it becomes
			// NULL, and `WHERE NULL` filters the row out.
			return nullRejectProof{nonTrue: true, mustNull: true}
		}
	case *expression.Constant:
		return proofFromConstant(ctx, x)
	case *expression.ScalarFunction:
		return proveNullRejectedScalarFunc(ctx, innerSchema, x)
	}
	return nullRejectProof{}
}

// proveNullRejectedScalarFunc handles builtins whose proof can be derived from
// child proofs plus SQL three-valued logic.
//
// Most builtins fall into one of two conservative buckets:
//  1. NULL-preserving builtins: any mustNull child makes the result NULL.
//  2. NULL-tests such as IS TRUE / IS FALSE: they turn NULL into a definite
//     boolean, so they only contribute nonTrue, not always mustNull.
//
// A few builtins need bespoke rules because their truth tables are more subtle
// than either bucket, notably AND / OR / NOT / IN / IS NULL.
func proveNullRejectedScalarFunc(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	expr *expression.ScalarFunction,
) nullRejectProof {
	switch expr.FuncName.L {
	case ast.LogicAnd:
		lhs := proveNullRejected(ctx, innerSchema, expr.GetArgs()[0])
		rhs := proveNullRejected(ctx, innerSchema, expr.GetArgs()[1])
		return nullRejectProof{
			nonTrue:  lhs.nonTrue || rhs.nonTrue,
			mustNull: lhs.mustNull && rhs.mustNull,
		}
	case ast.LogicOr:
		lhs := proveNullRejected(ctx, innerSchema, expr.GetArgs()[0])
		rhs := proveNullRejected(ctx, innerSchema, expr.GetArgs()[1])
		return nullRejectProof{
			nonTrue:  lhs.nonTrue && rhs.nonTrue,
			mustNull: lhs.mustNull && rhs.mustNull,
		}
	case ast.UnaryNot:
		// NOT(IS NULL(x)): when x is mustNull, IS NULL(NULL) = TRUE and
		// NOT(TRUE) = FALSE, so nonTrue holds. mustNull does not hold
		// because the result is FALSE, not NULL.
		//
		// Example:
		//   SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a
		//   WHERE NOT(IS NULL(t2.b));
		// After null extension, the predicate becomes NOT(IS NULL(NULL)) =
		// NOT(TRUE) = FALSE, so it is null-rejected.
		if child, ok := expr.GetArgs()[0].(*expression.ScalarFunction); ok && child.FuncName.L == ast.IsNull {
			return nullRejectProof{
				nonTrue: proveNullRejected(ctx, innerSchema, child.GetArgs()[0]).mustNull,
			}
		}
		// General NOT: NOT(NULL) = NULL (nonTrue), but NOT(FALSE) = TRUE
		// (not nonTrue). So nonTrue requires child.mustNull, not just
		// child.nonTrue.
		child := proveNullRejected(ctx, innerSchema, expr.GetArgs()[0])
		return nullRejectProof{
			nonTrue:  child.mustNull,
			mustNull: child.mustNull,
		}
	case ast.In:
		return proveNullRejectedIn(ctx, innerSchema, expr)
	case ast.IsNull:
		return nullRejectProof{}
	}

	if mode, ok := nullRejectRejectNullTests[expr.FuncName.L]; ok {
		child := proveNullRejected(ctx, innerSchema, expr.GetArgs()[0])
		return nullRejectProof{
			nonTrue:  child.mustNull,
			mustNull: child.mustNull && mode == nullRejectTestKeepsNull,
		}
	}

	if _, ok := nullRejectNullPreservingFunctions[expr.FuncName.L]; ok {
		for _, arg := range expr.GetArgs() {
			if proveNullRejected(ctx, innerSchema, arg).mustNull {
				return nullRejectProof{nonTrue: true, mustNull: true}
			}
		}
	}
	return nullRejectProof{}
}

// proveNullRejectedIn handles IN(value, list...).
// IN returns NULL (not FALSE) when value is NULL, or when all list-element
// comparisons yield NULL, making the whole predicate nonTrue in either case.
func proveNullRejectedIn(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	expr *expression.ScalarFunction,
) nullRejectProof {
	args := expr.GetArgs()
	if len(args) == 0 {
		return nullRejectProof{}
	}
	valueProof := proveNullRejected(ctx, innerSchema, args[0])
	if valueProof.mustNull {
		return nullRejectProof{nonTrue: true, mustNull: true}
	}
	allListMustNull := true
	for _, arg := range args[1:] {
		if !proveNullRejected(ctx, innerSchema, arg).mustNull {
			allListMustNull = false
			break
		}
	}
	if allListMustNull {
		return nullRejectProof{nonTrue: true, mustNull: true}
	}
	return nullRejectProof{}
}

// tryFoldNullifiedConstant tries to materialize the expression after replacing
// inner-side columns with SQL NULL.
//
// This is the bridge between symbolic proof and exact constant evaluation:
// whenever nullification turns the expression into a foldable constant, we can
// delegate the final truth-value classification to proofFromConstant.
func tryFoldNullifiedConstant(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	expr expression.Expression,
) (*expression.Constant, bool) {
	if cons, ok := tryFoldStaticConstant(ctx, expr); ok {
		return cons, true
	}
	switch x := expr.(type) {
	case *expression.Column:
		if innerSchema.Contains(x) {
			// Keep the original type/flags so constant folding still dispatches
			// through the same builtin signature after nullification.
			retType := x.RetType.Clone()
			retType.DelFlag(mysql.NotNullFlag)
			return expression.NewNullWithFieldType(retType), true
		}
	case *expression.Constant:
		if x.ParamMarker == nil && x.DeferredExpr == nil {
			return x, true
		}
	case *expression.ScalarFunction:
		return tryFoldNullifiedScalarFunc(ctx, innerSchema, x)
	}
	return nil, false
}

func tryFoldStaticConstant(ctx base.PlanContext, expr expression.Expression) (*expression.Constant, bool) {
	nullRejectCtx := nullRejectFoldCtx(ctx)
	if !allConstants(nullRejectCtx, expr) {
		return nil, false
	}
	cons, ok := expression.FoldConstant(nullRejectCtx, expr).(*expression.Constant)
	if !ok || cons.ParamMarker != nil || cons.DeferredExpr != nil {
		return nil, false
	}
	return cons, true
}

// tryFoldNullifiedScalarFunc handles scalar functions after inner-side columns
// are nullified.
//
// Generic NULL-preserving builtins can use the registry plus full constant
// folding. COALESCE/IFNULL/IF must be handled specially because they may hide a
// NULL and still collapse to a constant after nullification.
//
// Example:
//
//	COALESCE(t2.a, 2) > 2
//
// becomes
//
//	COALESCE(NULL, 2) > 2
//
// then
//
//	2 > 2
//
// so the predicate is provably nonTrue even though COALESCE itself is not
// NULL-preserving.
func tryFoldNullifiedScalarFunc(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	expr *expression.ScalarFunction,
) (*expression.Constant, bool) {
	switch expr.FuncName.L {
	case ast.Coalesce, ast.Ifnull:
		return tryFoldNullifiedCoalesceLike(ctx, innerSchema, expr)
	case ast.If:
		return tryFoldNullifiedIf(ctx, innerSchema, expr)
	}

	args := make([]expression.Expression, 0, len(expr.GetArgs()))
	allConstantArgs := true
	hasNullArg := false
	for _, arg := range expr.GetArgs() {
		cons, ok := tryFoldNullifiedConstant(ctx, innerSchema, arg)
		if !ok {
			allConstantArgs = false
			continue
		}
		args = append(args, cons)
		hasNullArg = hasNullArg || cons.Value.IsNull()
	}
	if _, ok := nullRejectNullPreservingFunctions[expr.FuncName.L]; ok && hasNullArg {
		return expression.NewNull(), true // null-preserving: any NULL arg makes the result NULL
	}
	if !allConstantArgs {
		return nil, false
	}
	return foldNullifiedFunction(ctx, expr, args)
}

// tryFoldNullifiedCoalesceLike nullifies every argument and then returns the
// first non-NULL folded argument, exactly matching COALESCE/IFNULL semantics.
//
// We need this special path because COALESCE/IFNULL are explicitly not
// NULL-preserving: a NULL child does not force the final result to be NULL.
func tryFoldNullifiedCoalesceLike(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	expr *expression.ScalarFunction,
) (*expression.Constant, bool) {
	for _, arg := range expr.GetArgs() {
		cons, ok := tryFoldNullifiedConstant(ctx, innerSchema, arg)
		if !ok {
			return nil, false
		}
		if !cons.Value.IsNull() {
			return cons, true
		}
	}
	return expression.NewNull(), true
}

// tryFoldNullifiedIf evaluates the condition after nullification and then only
// folds the taken branch.
//
// IF also needs a special path: after inner columns become NULL, the condition
// may collapse to a constant and reveal that only one branch matters.
func tryFoldNullifiedIf(
	ctx base.PlanContext,
	innerSchema *expression.Schema,
	expr *expression.ScalarFunction,
) (*expression.Constant, bool) {
	args := expr.GetArgs()
	cond, ok := tryFoldNullifiedConstant(ctx, innerSchema, args[0])
	if !ok {
		return nil, false
	}
	condVal, isNull, err := cond.EvalInt(nullRejectFoldCtx(ctx).GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, false
	}
	if !isNull && condVal != 0 {
		return tryFoldNullifiedConstant(ctx, innerSchema, args[1])
	}
	return tryFoldNullifiedConstant(ctx, innerSchema, args[2])
}

func foldNullifiedFunction(
	ctx base.PlanContext,
	expr *expression.ScalarFunction,
	args []expression.Expression,
) (*expression.Constant, bool) {
	nullRejectCtx := nullRejectFoldCtx(ctx)
	folded, err := expression.NewFunction(nullRejectCtx, expr.FuncName.L, expr.RetType.Clone(), args...)
	if err != nil {
		return nil, false
	}
	cons, ok := expression.FoldConstant(nullRejectCtx, folded).(*expression.Constant)
	if !ok || cons.ParamMarker != nil || cons.DeferredExpr != nil {
		return nil, false
	}
	return cons, true
}

// proofFromConstant classifies the exact folded constant result.
//
// NULL means both nonTrue and mustNull. Any exact FALSE-ish constant means
// nonTrue only. TRUE or non-foldable values contribute no proof.
func proofFromConstant(ctx base.PlanContext, cons *expression.Constant) nullRejectProof {
	if cons == nil || cons.ParamMarker != nil || cons.DeferredExpr != nil {
		return nullRejectProof{}
	}
	if cons.Value.IsNull() {
		return nullRejectProof{nonTrue: true, mustNull: true}
	}
	isTrue, err := cons.Value.ToBool(nullRejectFoldCtx(ctx).GetEvalCtx().TypeCtx())
	if err == nil && isTrue == 0 {
		return nullRejectProof{nonTrue: true}
	}
	return nullRejectProof{}
}

func nullRejectFoldCtx(ctx base.PlanContext) expression.BuildContext {
	return exprctx.CtxWithHandleTruncateErrLevel(ctx.GetNullRejectCheckExprCtx(), errctx.LevelIgnore)
}

// ResetNotNullFlag resets the not null flag of [start, end] columns in the schema.
func ResetNotNullFlag(schema *expression.Schema, start, end int) {
	for i := start; i < end; i++ {
		col := *schema.Columns[i]
		newFieldType := *col.RetType
		newFieldType.DelFlag(mysql.NotNullFlag)
		col.RetType = &newFieldType
		schema.Columns[i] = &col
	}
}
