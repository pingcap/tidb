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
	"github.com/pingcap/tidb/pkg/expression"
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

// nullRejectProof holds the two proof results for a sub-expression.
// See the file-level comment above for the full model.
type nullRejectProof struct {
	nonTrue  bool
	mustNull bool
}

type nullRejectTestMode uint8

const (
	nullRejectTestReturnsFalse nullRejectTestMode = iota // f(NULL) = FALSE; nonTrue but not mustNull
	nullRejectTestKeepsNull                              // f(NULL) = NULL; both nonTrue and mustNull
)

// nullRejectNullPreservingFunctions lists builtins that return NULL when any
// argument is NULL. See the file-level comment for how this drives the proof.
var nullRejectNullPreservingFunctions = map[string]struct{}{
	ast.Cast:            {},
	ast.GE:              {},
	ast.LE:              {},
	ast.EQ:              {},
	ast.NE:              {},
	ast.LT:              {},
	ast.GT:              {},
	ast.Plus:            {},
	ast.Minus:           {},
	ast.Mod:             {},
	ast.Div:             {},
	ast.Mul:             {},
	ast.IntDiv:          {},
	ast.BitNeg:          {},
	ast.And:             {},
	ast.LeftShift:       {},
	ast.RightShift:      {},
	ast.Or:              {},
	ast.Xor:             {},
	ast.UnaryMinus:      {},
	ast.LogicXor:        {},
	ast.Like:            {},
	ast.Ilike:           {},
	ast.Regexp:          {},
	ast.RegexpLike:      {},
	ast.RegexpSubstr:    {},
	ast.RegexpInStr:     {},
	ast.RegexpReplace:   {},
	ast.Strcmp:          {},
	ast.Abs:             {},
	ast.Ceil:            {},
	ast.Ceiling:         {},
	ast.CRC32:           {},
	ast.Degrees:         {},
	ast.Exp:             {},
	ast.Floor:           {},
	ast.Ln:              {},
	ast.Log:             {},
	ast.Log2:            {},
	ast.Log10:           {},
	ast.Pow:             {},
	ast.Power:           {},
	ast.Radians:         {},
	ast.Round:           {},
	ast.Sign:            {},
	ast.Sqrt:            {},
	ast.Truncate:        {},
	ast.ASCII:           {},
	ast.Bin:             {},
	ast.BitLength:       {},
	ast.CharLength:      {},
	ast.CharacterLength: {},
	ast.Concat:          {},
	ast.FindInSet:       {},
	ast.Format:          {},
	ast.FromBase64:      {},
	ast.Hex:             {},
	ast.InsertFunc:      {},
	ast.Lcase:           {},
	ast.Left:            {},
	ast.Length:          {},
	ast.Locate:          {},
	ast.Lower:           {},
	ast.LTrim:           {},
	ast.Mid:             {},
	ast.Oct:             {},
	ast.Ord:             {},
	ast.Position:        {},
	ast.Repeat:          {},
	ast.Replace:         {},
	ast.Reverse:         {},
	ast.Right:           {},
	ast.RTrim:           {},
	ast.Space:           {},
	ast.Substring:       {},
	ast.SubstringIndex:  {},
	ast.Trim:            {},
	ast.Ucase:           {},
	ast.Unhex:           {},
	ast.Upper:           {},
	ast.AddDate:         {},
	ast.DateAdd:         {},
	ast.SubDate:         {},
	ast.DateSub:         {},
	ast.AddTime:         {},
	ast.ConvertTz:       {},
	ast.Date:            {},
	ast.DateFormat:      {},
	ast.DateDiff:        {},
	ast.Day:             {},
	ast.DayOfMonth:      {},
	ast.DayOfWeek:       {},
	ast.DayOfYear:       {},
	ast.Extract:         {},
	ast.FromDays:        {},
	ast.FromUnixTime:    {},
	ast.Hour:            {},
	ast.MakeDate:        {},
	ast.MakeTime:        {},
	ast.MicroSecond:     {},
	ast.Minute:          {},
	ast.Month:           {},
	ast.PeriodAdd:       {},
	ast.PeriodDiff:      {},
	ast.Quarter:         {},
	ast.SecToTime:       {},
	ast.Second:          {},
	ast.StrToDate:       {},
	ast.SubTime:         {},
	ast.Time:            {},
	ast.TimeDiff:        {},
	ast.TimestampDiff:   {},
	ast.ToDays:          {},
	ast.ToSeconds:       {},
	ast.UnixTimestamp:   {},
	ast.Year:            {},
}

// nullRejectRejectNullTests lists IS-TRUE / IS-FALSE style builtins that
// convert a NULL input into a definite boolean. The mode records whether the
// output is FALSE (ReturnsFalse) or stays NULL (KeepsNull).
var nullRejectRejectNullTests = map[string]nullRejectTestMode{
	ast.IsTruthWithoutNull: nullRejectTestReturnsFalse,
	ast.IsTruthWithNull:    nullRejectTestKeepsNull,
	ast.IsFalsity:          nullRejectTestReturnsFalse,
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
	predicate = expression.PushDownNot(ctx.GetExprCtx(), predicate)
	return proveNullRejected(ctx, innerSchema, predicate).nonTrue
}

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
			return nullRejectProof{nonTrue: true, mustNull: true}
		}
	case *expression.Constant:
		return proofFromConstant(ctx, x)
	case *expression.ScalarFunction:
		return proveNullRejectedScalarFunc(ctx, innerSchema, x)
	}
	return nullRejectProof{}
}

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
			return expression.NewNull(), true
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
	if !allConstants(ctx.GetExprCtx(), expr) {
		return nil, false
	}
	cons, ok := expression.FoldConstant(ctx.GetExprCtx(), expr).(*expression.Constant)
	if !ok || cons.ParamMarker != nil || cons.DeferredExpr != nil {
		return nil, false
	}
	return cons, true
}

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
	condVal, isNull, err := cond.EvalInt(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
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
	folded, err := expression.NewFunction(ctx.GetExprCtx(), expr.FuncName.L, expr.RetType.Clone(), args...)
	if err != nil {
		return nil, false
	}
	cons, ok := expression.FoldConstant(ctx.GetExprCtx(), folded).(*expression.Constant)
	if !ok || cons.ParamMarker != nil || cons.DeferredExpr != nil {
		return nil, false
	}
	return cons, true
}

func proofFromConstant(ctx base.PlanContext, cons *expression.Constant) nullRejectProof {
	if cons == nil || cons.ParamMarker != nil || cons.DeferredExpr != nil {
		return nullRejectProof{}
	}
	if cons.Value.IsNull() {
		return nullRejectProof{nonTrue: true, mustNull: true}
	}
	isTrue, err := cons.Value.ToBool(ctx.GetSessionVars().StmtCtx.TypeCtxOrDefault())
	if err == nil && isTrue == 0 {
		return nullRejectProof{nonTrue: true}
	}
	return nullRejectProof{}
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
