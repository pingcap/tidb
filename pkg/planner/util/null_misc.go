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
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// allConstants checks if only the expression has only constants.
func allConstants(ctx expression.BuildContext, expr expression.Expression) bool {
	if expression.MaybeOverOptimized4PlanCache(ctx, []expression.Expression{expr}) {
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
func isNullRejectedInList(ctx base.PlanContext, expr *expression.ScalarFunction, innerSchema *expression.Schema) bool {
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
			if !(isNullRejectedSimpleExpr(ctx, innerSchema, eQCondition)) {
				return false
			}
		}
	}
	return true
}

// IsNullRejected takes care of complex predicates like this:
// IsNullRejected(A OR B) = IsNullRejected(A) AND IsNullRejected(B)
// IsNullRejected(A AND B) = IsNullRejected(A) OR IsNullRejected(B)
func IsNullRejected(ctx base.PlanContext, innerSchema *expression.Schema, predicate expression.Expression) bool {
	predicate = expression.PushDownNot(ctx.GetNullRejectCheckExprCtx(), predicate)
	if expression.ContainOuterNot(predicate) {
		return false
	}

	switch expr := predicate.(type) {
	case *expression.ScalarFunction:
		if expr.FuncName.L == ast.LogicAnd {
			if IsNullRejected(ctx, innerSchema, expr.GetArgs()[0]) {
				return true
			}
			return IsNullRejected(ctx, innerSchema, expr.GetArgs()[1])
		} else if expr.FuncName.L == ast.LogicOr {
			if !(IsNullRejected(ctx, innerSchema, expr.GetArgs()[0])) {
				return false
			}
			return IsNullRejected(ctx, innerSchema, expr.GetArgs()[1])
		} else if expr.FuncName.L == ast.In {
			return isNullRejectedInList(ctx, expr, innerSchema)
		} else {
			return isNullRejectedSimpleExpr(ctx, innerSchema, expr)
		}
	default:
		return isNullRejectedSimpleExpr(ctx, innerSchema, predicate)
	}
}

// isNullRejectedSimpleExpr check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner table (null producing side) that evaluates
// to UNKNOWN or FALSE when one of its arguments is NULL.
func isNullRejectedSimpleExpr(ctx context.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	// The expression should reference at least one field in innerSchema or all constants.
	if !expression.ExprReferenceSchema(expr, schema) && !allConstants(ctx.GetExprCtx(), expr) {
		return false
	}
	exprCtx := ctx.GetNullRejectCheckExprCtx()
	sc := ctx.GetSessionVars().StmtCtx
	result := expression.EvaluateExprWithNull(exprCtx, schema, expr)
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
