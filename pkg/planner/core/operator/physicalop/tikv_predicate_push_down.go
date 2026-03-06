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

package physicalop

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// selectTiKVBlockFilterPredicates selects conditions that are useful for
// TiKV block-level min/max filtering. Only simple column-vs-constant
// comparisons are selected, since those are the ones that can leverage
// per-block min/max statistics in TiKV's cloud-storage-engine.
func selectTiKVBlockFilterPredicates(conds []expression.Expression) []expression.Expression {
	var result []expression.Expression
	for _, cond := range conds {
		if isTiKVBlockFilterable(cond) {
			result = append(result, cond)
		}
	}
	return result
}

// isTiKVBlockFilterable checks whether a single condition is useful for
// block-level min/max filtering. Supported patterns:
//   - col {<, <=, =, >=, >} const
//   - col IN (const, ...)
//   - col IS NULL
//   - cond AND cond (recursive)
//   - cond OR cond (recursive)
func isTiKVBlockFilterable(cond expression.Expression) bool {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	switch sf.FuncName.L {
	case ast.LT, ast.LE, ast.EQ, ast.GE, ast.GT:
		return isColumnCompareConst(sf)
	case ast.In:
		return isColumnInConsts(sf)
	case ast.IsNull:
		return isColumnIsNull(sf)
	case ast.LogicAnd, ast.LogicOr:
		args := sf.GetArgs()
		if len(args) != 2 {
			return false
		}
		return isTiKVBlockFilterable(args[0]) && isTiKVBlockFilterable(args[1])
	default:
		return false
	}
}

// isColumnCompareConst checks whether the scalar function is a comparison
// between a column and a constant (in either order).
func isColumnCompareConst(sf *expression.ScalarFunction) bool {
	args := sf.GetArgs()
	if len(args) != 2 {
		return false
	}
	return (isColumnExpr(args[0]) && isConstExpr(args[1])) ||
		(isConstExpr(args[0]) && isColumnExpr(args[1]))
}

// isColumnInConsts checks whether the scalar function is col IN (const, ...).
func isColumnInConsts(sf *expression.ScalarFunction) bool {
	args := sf.GetArgs()
	if len(args) < 2 {
		return false
	}
	if !isColumnExpr(args[0]) {
		return false
	}
	for _, arg := range args[1:] {
		if !isConstExpr(arg) {
			return false
		}
	}
	return true
}

// isColumnIsNull checks whether the scalar function is col IS NULL.
func isColumnIsNull(sf *expression.ScalarFunction) bool {
	args := sf.GetArgs()
	if len(args) != 1 {
		return false
	}
	return isColumnExpr(args[0])
}

// isColumnExpr checks if the expression is a simple column reference.
func isColumnExpr(expr expression.Expression) bool {
	_, ok := expr.(*expression.Column)
	return ok
}

// isConstExpr checks if the expression is a constant (at least within context).
func isConstExpr(expr expression.Expression) bool {
	return expr.ConstLevel() >= expression.ConstOnlyInContext
}
