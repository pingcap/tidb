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

package coreusage

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/context"
)

// IsNullRejected check whether a condition is null-rejected
// A condition would be null-rejected in one of following cases:
// If it is a predicate containing a reference to an inner table that evaluates to UNKNOWN or FALSE when one of its arguments is NULL.
// If it is a conjunction containing a null-rejected condition as a conjunct.
// If it is a disjunction of null-rejected conditions.
func IsNullRejected(ctx context.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	exprCtx := ctx.GetExprCtx()
	expr = expression.PushDownNot(exprCtx, expr)
	if expression.ContainOuterNot(expr) {
		return false
	}
	sc := ctx.GetSessionVars().StmtCtx
	if !exprCtx.IsInNullRejectCheck() {
		exprCtx.SetInNullRejectCheck(true)
		defer exprCtx.SetInNullRejectCheck(false)
	}
	for _, cond := range expression.SplitCNFItems(expr) {
		if isNullRejectedSpecially(ctx, schema, expr) {
			return true
		}

		result := expression.EvaluateExprWithNull(exprCtx, schema, cond)
		x, ok := result.(*expression.Constant)
		if !ok {
			continue
		}
		if x.Value.IsNull() {
			return true
		} else if isTrue, err := x.Value.ToBool(sc.TypeCtxOrDefault()); err == nil && isTrue == 0 {
			return true
		}
	}
	return false
}

// isNullRejectedSpecially handles some null-rejected cases specially, since the current in
// EvaluateExprWithNull is too strict for some cases, e.g. #49616.
func isNullRejectedSpecially(ctx context.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	return specialNullRejectedCase1(ctx, schema, expr) // only 1 case now
}

// specialNullRejectedCase1 is mainly for #49616.
// Case1 specially handles `null-rejected OR (null-rejected AND {others})`, then no matter what the result
// of `{others}` is (True, False or Null), the result of this predicate is null, so this predicate is null-rejected.
func specialNullRejectedCase1(ctx context.PlanContext, schema *expression.Schema, expr expression.Expression) bool {
	isFunc := func(e expression.Expression, lowerFuncName string) *expression.ScalarFunction {
		f, ok := e.(*expression.ScalarFunction)
		if !ok {
			return nil
		}
		if f.FuncName.L == lowerFuncName {
			return f
		}
		return nil
	}
	orFunc := isFunc(expr, ast.LogicOr)
	if orFunc == nil {
		return false
	}
	for i := 0; i < 2; i++ {
		andFunc := isFunc(orFunc.GetArgs()[i], ast.LogicAnd)
		if andFunc == nil {
			continue
		}
		if !IsNullRejected(ctx, schema, orFunc.GetArgs()[1-i]) {
			continue // the other side should be null-rejected: null-rejected OR (... AND ...)
		}
		for _, andItem := range expression.SplitCNFItems(andFunc) {
			if IsNullRejected(ctx, schema, andItem) {
				return true // hit the case in the comment: null-rejected OR (null-rejected AND ...)
			}
		}
	}
	return false
}
