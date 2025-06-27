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
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
)

// DeleteTrueExprs deletes the surely true expressions
func DeleteTrueExprs(buildCtx expression.BuildContext, stmtCtx *stmtctx.StatementContext, conds []expression.Expression) []expression.Expression {
	if len(conds) == 0 {
		return conds
	}
	return slices.DeleteFunc(conds, func(cond expression.Expression) bool {
		switch con := cond.(type) {
		case *expression.ScalarFunction:
			if con.FuncName.L == ast.UnaryNot {
				// isnull(not null) is always false, so we can remove it.
				return IsNullWithNotNullColumn(buildCtx.GetEvalCtx(), con.GetArgs()[0])
			}
		case *expression.Constant:
			if expression.MaybeOverOptimized4PlanCache(buildCtx, con) {
				return false
			}
			isTrue, err := con.Value.ToBool(stmtCtx.TypeCtx())
			return err == nil && isTrue == 1
		}
		return false
	})
}

// IsNullWithNotNullColumn checks if the expression is `isnull()` or `not(isnull())` with a not null column.
func IsNullWithNotNullColumn(ctx expression.EvalContext, expr expression.Expression) bool {
	if e, ok := expr.(*expression.ScalarFunction); ok && e.FuncName.L == ast.IsNull {
		if args := e.GetArgs(); len(args) == 1 {
			if col, ok := args[0].(*expression.Column); ok && mysql.HasNotNullFlag(col.GetType(ctx).GetFlag()) {
				return true
			}
		}
	}
	return false
}
