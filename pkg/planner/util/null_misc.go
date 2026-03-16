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
	"github.com/pingcap/tidb/pkg/planner/planctx"
)

// StableNullRejectStatus describes whether a predicate's null-reject property stays fixed
// after replacing the probed inner-side columns with NULL while leaving parameters symbolic.
type StableNullRejectStatus uint8

const (
	// StableNullRejectUnknown means the structural fast path cannot prove the outcome.
	StableNullRejectUnknown StableNullRejectStatus = iota
	// StableNullRejectYes means the predicate is always null-rejected for the probed input.
	StableNullRejectYes
	// StableNullRejectNo means the predicate is always non-null-rejected for the probed input.
	StableNullRejectNo
)

// stableNullRejectNullPropagatingFuncs is the conservative set of scalar
// operator/function names whose result is guaranteed to be NULL once any argument is
// forced to NULL.
//
// Boolean predicate nodes with their own three-valued-logic folding rules, such as
// AND/OR/IS NULL/NOT IS NULL, are classified separately below.
var stableNullRejectNullPropagatingFuncs = map[string]struct{}{
	ast.Abs:        {},
	ast.Cast:       {},
	ast.Div:        {},
	ast.EQ:         {},
	ast.GE:         {},
	ast.GT:         {},
	ast.IntDiv:     {},
	ast.LE:         {},
	ast.LT:         {},
	ast.Minus:      {},
	ast.Mod:        {},
	ast.Mul:        {},
	ast.NE:         {},
	ast.Plus:       {},
	ast.UnaryMinus: {},
	ast.UnaryPlus:  {},
}

// stableNullRejectProbe describes which columns are conceptually replaced with NULL
// during the structural null-reject classification.
type stableNullRejectProbe struct {
	evalCtx   expression.EvalContext
	schema    *expression.Schema
	targetCol *expression.Column
}

func (p stableNullRejectProbe) matches(col *expression.Column) bool {
	if p.schema != nil {
		return p.schema.Contains(col)
	}
	return p.targetCol != nil && col.Equal(p.evalCtx, p.targetCol)
}

// ClassifyStableNullRejectBySchema classifies whether a predicate's null-reject property
// stays fixed after replacing the referenced inner-side schema columns with NULL.
func ClassifyStableNullRejectBySchema(
	ctx planctx.PlanContext,
	innerSchema *expression.Schema,
	predicate expression.Expression,
) StableNullRejectStatus {
	predicate = expression.PushDownNot(ctx.GetNullRejectCheckExprCtx(), predicate)
	if expression.ContainOuterNot(predicate) {
		return StableNullRejectUnknown
	}
	return classifyStableNullReject(ctx, predicate, stableNullRejectProbe{
		evalCtx: ctx.GetExprCtx().GetEvalCtx(),
		schema:  innerSchema,
	})
}

// ClassifyStableNullRejectByColumn classifies whether a predicate's null-reject property
// stays fixed after replacing the target column with NULL.
func ClassifyStableNullRejectByColumn(
	ctx planctx.PlanContext,
	targetCol *expression.Column,
	predicate expression.Expression,
) StableNullRejectStatus {
	predicate = expression.PushDownNot(ctx.GetNullRejectCheckExprCtx(), predicate)
	if expression.ContainOuterNot(predicate) {
		return StableNullRejectUnknown
	}
	return classifyStableNullReject(ctx, predicate, stableNullRejectProbe{
		evalCtx:   ctx.GetExprCtx().GetEvalCtx(),
		targetCol: targetCol,
	})
}

func classifyStableNullReject(
	ctx planctx.PlanContext,
	predicate expression.Expression,
	probe stableNullRejectProbe,
) StableNullRejectStatus {
	// Constant-only predicates do not depend on the probed columns at all, so we can
	// classify them directly once they are known to be free of mutable constants.
	if allConstants(ctx.GetExprCtx(), predicate) {
		return classifyStableNullRejectImmutablePredicate(ctx, predicate)
	}
	if expr, ok := predicate.(*expression.ScalarFunction); ok {
		switch expr.FuncName.L {
		case ast.LogicAnd:
			allNo := true
			for _, arg := range expr.GetArgs() {
				result := classifyStableNullReject(ctx, arg, probe)
				if result == StableNullRejectYes {
					return StableNullRejectYes
				}
				if result != StableNullRejectNo {
					allNo = false
				}
			}
			if allNo {
				return StableNullRejectNo
			}
			return StableNullRejectUnknown
		case ast.LogicOr:
			allYes := true
			for _, arg := range expr.GetArgs() {
				result := classifyStableNullReject(ctx, arg, probe)
				if result == StableNullRejectNo {
					return StableNullRejectNo
				}
				if result != StableNullRejectYes {
					allYes = false
				}
			}
			if allYes {
				return StableNullRejectYes
			}
			return StableNullRejectUnknown
		case ast.IsNull:
			if exprAlwaysNullWhenProbeColumnsNull(expr.GetArgs()[0], probe) {
				return StableNullRejectNo
			}
			return StableNullRejectUnknown
		case ast.UnaryNot:
			child, ok := expr.GetArgs()[0].(*expression.ScalarFunction)
			if ok && child.FuncName.L == ast.IsNull && exprAlwaysNullWhenProbeColumnsNull(child.GetArgs()[0], probe) {
				return StableNullRejectYes
			}
			return StableNullRejectUnknown
		}
	}

	if exprAlwaysNullWhenProbeColumnsNull(predicate, probe) {
		return StableNullRejectYes
	}
	return StableNullRejectUnknown
}

// classifyStableNullRejectImmutablePredicate handles the degenerate case where the predicate is
// already fully constant and therefore independent from the probed columns.
//
// false/NULL means null-reject, because the outer-join generated row can never make the
// predicate true; true means non-null-reject.
func classifyStableNullRejectImmutablePredicate(ctx planctx.PlanContext, predicate expression.Expression) StableNullRejectStatus {
	result, err := expression.EvaluateExprWithNull(ctx.GetNullRejectCheckExprCtx(), expression.NewSchema(), predicate, false)
	if err != nil {
		return StableNullRejectUnknown
	}
	x, ok := result.(*expression.Constant)
	if !ok || isMutableConst(x) {
		return StableNullRejectUnknown
	}
	if x.Value.IsNull() {
		return StableNullRejectYes
	}
	isTrue, err := x.Value.ToBool(ctx.GetSessionVars().StmtCtx.TypeCtxOrDefault())
	if err != nil {
		return StableNullRejectUnknown
	}
	if isTrue == 0 {
		return StableNullRejectYes
	}
	return StableNullRejectNo
}

// exprAlwaysNullWhenProbeColumnsNull answers a narrower question than full null-reject:
// after replacing the probed columns with NULL, is this expression guaranteed to become
// NULL regardless of the remaining symbolic parameters?
//
// This helper is intentionally conservative. It only recognizes columns/constant NULLs
// directly, then recurses through a small set of operators/functions whose result is NULL
// whenever one argument is NULL. More complex boolean forms are handled by
// classifyStableNullReject.
func exprAlwaysNullWhenProbeColumnsNull(expr expression.Expression, probe stableNullRejectProbe) bool {
	switch x := expr.(type) {
	case *expression.Column:
		return probe.matches(x)
	case *expression.Constant:
		return !isMutableConst(x) && x.Value.IsNull()
	case *expression.ScalarFunction:
		if _, ok := stableNullRejectNullPropagatingFuncs[x.FuncName.L]; !ok {
			return false
		}
		for _, arg := range x.GetArgs() {
			if exprAlwaysNullWhenProbeColumnsNull(arg, probe) {
				return true
			}
		}
	}
	return false
}

func isMutableConst(expr *expression.Constant) bool {
	return expr.ParamMarker != nil || expr.DeferredExpr != nil
}

// allConstants checks if only the expression has only constants.
func allConstants(ctx expression.BuildContext, expr expression.Expression) bool {
	if expression.MaybeOverOptimized4PlanCache(ctx, expr) {
		return false // expression contains non-deterministic parameter
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

// isNullRejectedInList checks null filter for IN list using OR logic.
// Reason is that null filtering through evaluation by isNullRejectedSimpleExpr
// has problems with IN list. For example, constant in (outer-table.col1, inner-table.col2)
// is not null rejecting since constant in (outer-table.col1, NULL) is not false/unknown.
func isNullRejectedInList(ctx base.PlanContext, expr *expression.ScalarFunction,
	innerSchema *expression.Schema, skipPlanCacheCheck bool) bool {
	for i, arg := range expr.GetArgs() {
		if i > 0 {
			newArgs := make([]expression.Expression, 0, 2)
			newArgs = append(newArgs, expr.GetArgs()[0])
			newArgs = append(newArgs, arg)
			eQCondition, err := expression.NewFunction(ctx.GetExprCtx(), ast.EQ,
				expr.GetType(ctx.GetExprCtx().GetEvalCtx()), newArgs...)
			if err != nil {
				return false
			}
			if !(isNullRejectedSimpleExpr(ctx, innerSchema, eQCondition, skipPlanCacheCheck)) {
				return false
			}
		}
	}
	return true
}

// IsNullRejected takes care of complex predicates like this:
// IsNullRejected(A OR B) = IsNullRejected(A) AND IsNullRejected(B)
// IsNullRejected(A AND B) = IsNullRejected(A) OR IsNullRejected(B)
func IsNullRejected(ctx base.PlanContext, innerSchema *expression.Schema, predicate expression.Expression,
	skipPlanCacheCheck bool) bool {
	predicate = expression.PushDownNot(ctx.GetNullRejectCheckExprCtx(), predicate)
	if expression.ContainOuterNot(predicate) {
		return false
	}

	switch expr := predicate.(type) {
	case *expression.ScalarFunction:
		if expr.FuncName.L == ast.LogicAnd {
			if IsNullRejected(ctx, innerSchema, expr.GetArgs()[0], skipPlanCacheCheck) {
				return true
			}
			return IsNullRejected(ctx, innerSchema, expr.GetArgs()[1], skipPlanCacheCheck)
		} else if expr.FuncName.L == ast.LogicOr {
			if !(IsNullRejected(ctx, innerSchema, expr.GetArgs()[0], skipPlanCacheCheck)) {
				return false
			}
			return IsNullRejected(ctx, innerSchema, expr.GetArgs()[1], skipPlanCacheCheck)
		} else if expr.FuncName.L == ast.In {
			return isNullRejectedInList(ctx, expr, innerSchema, skipPlanCacheCheck)
		}
		return isNullRejectedSimpleExpr(ctx, innerSchema, expr, skipPlanCacheCheck)
	default:
		return isNullRejectedSimpleExpr(ctx, innerSchema, predicate, skipPlanCacheCheck)
	}
}

// isNullRejectedSimpleExpr check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner table (null producing side) that evaluates
// to UNKNOWN or FALSE when one of its arguments is NULL.
func isNullRejectedSimpleExpr(ctx planctx.PlanContext, schema *expression.Schema, expr expression.Expression,
	skipPlanCacheCheck bool) bool {
	// The expression should reference at least one field in innerSchema or all constants.
	if !expression.ExprReferenceSchema(expr, schema) && !allConstants(ctx.GetExprCtx(), expr) {
		return false
	}
	exprCtx := ctx.GetNullRejectCheckExprCtx()
	sc := ctx.GetSessionVars().StmtCtx
	result, err := expression.EvaluateExprWithNull(exprCtx, schema, expr, skipPlanCacheCheck)
	if err != nil {
		return false
	}
	x, ok := result.(*expression.Constant)
	if ok {
		if x.Value.IsNull() {
			return true
		} else if isTrue, err := x.Value.ToBool(sc.TypeCtxOrDefault()); err == nil && isTrue == 0 {
			return true
		}
	}
	return false
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
