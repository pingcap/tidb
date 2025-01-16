// Copyright 2025 PingCAP, Inc.
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

package logicalop

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// isConstFalse is used to check whether the expression is a constant false expression.
func isConstFalse(expr expression.Expression) bool {
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

// Conds2TableDual builds a LogicalTableDual if cond is constant false or null.
func Conds2TableDual(p base.LogicalPlan, conds []expression.Expression) base.LogicalPlan {
	for _, cond := range conds {
		if isConstFalse(cond) {
			if expression.MaybeOverOptimized4PlanCache(p.SCtx().GetExprCtx(), conds) {
				return nil
			}
			dual := LogicalTableDual{}.Init(p.SCtx(), p.QueryBlockOffset())
			dual.SetSchema(p.Schema())
			return dual
		}
	}
	if len(conds) != 1 {
		return nil
	}

	con, ok := conds[0].(*expression.Constant)
	if !ok {
		return nil
	}
	sc := p.SCtx().GetSessionVars().StmtCtx
	if expression.MaybeOverOptimized4PlanCache(p.SCtx().GetExprCtx(), []expression.Expression{con}) {
		return nil
	}
	if isTrue, err := con.Value.ToBool(sc.TypeCtxOrDefault()); (err == nil && isTrue == 0) || con.Value.IsNull() {
		dual := LogicalTableDual{}.Init(p.SCtx(), p.QueryBlockOffset())
		dual.SetSchema(p.Schema())
		return dual
	}
	return nil
}
