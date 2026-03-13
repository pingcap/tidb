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

// nullRejectPlanCacheStrictFuncs is a small whitelist of expressions whose result is guaranteed
// to stay NULL once an input subtree is known to be NULL.
var nullRejectPlanCacheStrictFuncs = map[string]struct{}{
	ast.Abs:        {},
	ast.Div:        {},
	ast.IntDiv:     {},
	ast.Minus:      {},
	ast.Mod:        {},
	ast.Mul:        {},
	ast.Plus:       {},
	ast.UnaryMinus: {},
	ast.UnaryPlus:  {},
}

type nullRejectColumnProbe struct {
	evalCtx   expression.EvalContext
	schema    *expression.Schema
	targetCol *expression.Column
}

func (p nullRejectColumnProbe) matches(col *expression.Column) bool {
	if p.schema != nil {
		return p.schema.Contains(col)
	}
	return p.targetCol != nil && col.Equal(p.evalCtx, p.targetCol)
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
	if skipPlanCacheCheck && expression.MaybeOverOptimized4PlanCache(ctx.GetExprCtx(), predicate) {
		return isNullRejectedConservative(ctx, predicate, nullRejectColumnProbe{
			evalCtx: ctx.GetExprCtx().GetEvalCtx(),
			schema:  innerSchema,
		})
	}
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

// IsNullRejectedByInnerColumn is a conservative null-reject checker for a single inner column.
// It is intended for plan-cache-sensitive rewrites where parameter values must stay symbolic.
func IsNullRejectedByInnerColumn(ctx planctx.PlanContext, targetCol *expression.Column, predicate expression.Expression) bool {
	predicate = expression.PushDownNot(ctx.GetNullRejectCheckExprCtx(), predicate)
	return isNullRejectedConservative(ctx, predicate, nullRejectColumnProbe{
		evalCtx:   ctx.GetExprCtx().GetEvalCtx(),
		targetCol: targetCol,
	})
}

func isNullRejectedConservative(ctx planctx.PlanContext, predicate expression.Expression, probe nullRejectColumnProbe) bool {
	switch expr := predicate.(type) {
	case *expression.ScalarFunction:
		switch expr.FuncName.L {
		case ast.LogicAnd:
			for _, arg := range expr.GetArgs() {
				if isNullRejectedConservative(ctx, arg, probe) {
					return true
				}
			}
			return false
		case ast.LogicOr:
			for _, arg := range expr.GetArgs() {
				if !isNullRejectedConservative(ctx, arg, probe) {
					return false
				}
			}
			return true
		case ast.In:
			return isNullRejectedConservativeInList(ctx, expr, probe)
		}
	}
	return isNullRejectedConservativeLeaf(ctx, predicate, probe)
}

func isNullRejectedConservativeInList(ctx planctx.PlanContext, expr *expression.ScalarFunction, probe nullRejectColumnProbe) bool {
	for i, arg := range expr.GetArgs() {
		if i == 0 {
			continue
		}
		eqExpr, err := expression.NewFunction(ctx.GetExprCtx(), ast.EQ, expr.GetType(ctx.GetExprCtx().GetEvalCtx()), expr.GetArgs()[0], arg)
		if err != nil || !isNullRejectedConservative(ctx, eqExpr, probe) {
			return false
		}
	}
	return true
}

func isNullRejectedConservativeLeaf(ctx planctx.PlanContext, expr expression.Expression, probe nullRejectColumnProbe) bool {
	if immutableConst, ok := expr.(*expression.Constant); ok {
		return immutableConstRejectsNull(ctx, immutableConst)
	}
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	switch sf.FuncName.L {
	case ast.NullEQ:
		return false
	case ast.IsNull:
		return false
	case ast.UnaryNot:
		child, ok := sf.GetArgs()[0].(*expression.ScalarFunction)
		return ok && child.FuncName.L == ast.IsNull && exprAlwaysNullForNullReject(child.GetArgs()[0], probe)
	case ast.IsTruthWithNull, ast.IsTruthWithoutNull:
		return exprAlwaysNullForNullReject(sf.GetArgs()[0], probe)
	}
	if _, ok := expression.CompareOpMap[sf.FuncName.L]; ok {
		args := sf.GetArgs()
		return len(args) >= 2 &&
			(exprAlwaysNullForNullReject(args[0], probe) || exprAlwaysNullForNullReject(args[1], probe))
	}
	return exprAlwaysNullForNullReject(expr, probe)
}

func exprAlwaysNullForNullReject(expr expression.Expression, probe nullRejectColumnProbe) bool {
	switch x := expr.(type) {
	case *expression.Column:
		return probe.matches(x)
	case *expression.Constant:
		return !isMutableConst(x) && x.Value.IsNull()
	case *expression.ScalarFunction:
		if _, ok := nullRejectPlanCacheStrictFuncs[x.FuncName.L]; !ok {
			return false
		}
		for _, arg := range x.GetArgs() {
			if exprAlwaysNullForNullReject(arg, probe) {
				return true
			}
		}
	}
	return false
}

func immutableConstRejectsNull(ctx planctx.PlanContext, expr *expression.Constant) bool {
	if isMutableConst(expr) {
		return false
	}
	if expr.Value.IsNull() {
		return true
	}
	isTrue, err := expr.Value.ToBool(ctx.GetSessionVars().StmtCtx.TypeCtxOrDefault())
	return err == nil && isTrue == 0
}

func isMutableConst(expr *expression.Constant) bool {
	return expr.ParamMarker != nil || expr.DeferredExpr != nil
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
