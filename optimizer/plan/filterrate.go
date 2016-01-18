// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser/opcode"
)

// computeFilterRate computes the filter rate for an expression.
// It only depends on the expression type, not the expression value.
// The expr parameter should contains only one column name.
func computeFilterRate(expr ast.ExprNode) float64 {
	switch x := expr.(type) {
	case *ast.BetweenExpr:
		return computeBetweenFilterRate(x)
	case *ast.BinaryOperationExpr:
		return computeBinopFilterRate(x)
	case *ast.IsNullExpr:
		return computeIsNullFilterRate(x)
	case *ast.IsTruthExpr:
		return computeIsTrueFilterRate(x)
	case *ast.ParenthesesExpr:
		return computeFilterRate(x.Expr)
	case *ast.PatternInExpr:
		return computePatternInFilterRate(x)
	case *ast.PatternLikeExpr:
		return computePatternLikeFilterRate(x)
	case *ast.ColumnNameExpr:
		return 1
	}
	return 1
}

func computeBetweenFilterRate(expr *ast.BetweenExpr) float64 {
	return 0.3
}

func computeBinopFilterRate(expr *ast.BinaryOperationExpr) float64 {
	switch expr.Op {
	case opcode.AndAnd:
		return computeFilterRate(expr.L) * computeFilterRate(expr.R)
	case opcode.OrOr:
		rate := computeFilterRate(expr.L) + computeFilterRate(expr.R)
		if rate > 1 {
			rate = 1
		}
		return rate
	case opcode.EQ:
		return 0.001
	case opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		return 0.4
	}
	return 1
}

func computeIsNullFilterRate(expr *ast.IsNullExpr) float64 {
	return 0.01
}

func computeIsTrueFilterRate(expr *ast.IsTruthExpr) float64 {
	return 0.9
}

func computePatternInFilterRate(expr *ast.PatternInExpr) float64 {
	if len(expr.List) > 0 {
		return 0.01 * float64(len(expr.List))
	}
	return 1
}

func computePatternLikeFilterRate(expr *ast.PatternLikeExpr) float64 {
	return 0.1
}
