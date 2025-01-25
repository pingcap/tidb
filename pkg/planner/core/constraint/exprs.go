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

package constraint

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// DeleteTrueExprs deletes the surely true expressions
func DeleteTrueExprs(p base.LogicalPlan, conds []expression.Expression) []expression.Expression {
	newConds := make([]expression.Expression, 0, len(conds))
	for _, cond := range conds {
		con, ok := cond.(*expression.Constant)
		if !ok {
			newConds = append(newConds, cond)
			continue
		}
		if expression.MaybeOverOptimized4PlanCache(p.SCtx().GetExprCtx(), []expression.Expression{con}) {
			newConds = append(newConds, cond)
			continue
		}
		sc := p.SCtx().GetSessionVars().StmtCtx
		if isTrue, err := con.Value.ToBool(sc.TypeCtx()); err == nil && isTrue == 1 {
			continue
		}
		newConds = append(newConds, cond)
	}
	return newConds
}

// IsConstFalse is used to check whether the expression is a constant false expression.
func IsConstFalse(ctx expression.BuildContext, expr expression.Expression) bool {
	if expression.MaybeOverOptimized4PlanCache(ctx, []expression.Expression{expr}) {
		ctx.SetSkipPlanCache("some parameters may be overwritten when constant propagation")
	}
	if e, ok := expr.(*expression.ScalarFunction); ok {
		switch e.FuncName.L {
		case ast.LT, ast.LE, ast.GT, ast.GE, ast.EQ, ast.NE:
			if constExpr, ok := e.GetArgs()[1].(*expression.Constant); ok && constExpr.Value.IsNull() && constExpr.DeferredExpr == nil {
				return true
			}
		}
	}
	return false
}
