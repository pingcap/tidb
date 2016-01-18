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

const (
	rateFull          = 1
	rateEqual         = 0.001
	rateNotEqual      = 0.999
	rateBetween       = 0.3
	rateGreaterOrLess = 0.4
	rateIsFalse       = 0.01
	rateIsNull        = 0.01
	rateLike          = 0.1
)

// computeFilterRate computes the filter rate for an expression.
// It only depends on the expression type, not the expression value.
// The expr parameter should contain only one column name.
func computeFilterRate(expr ast.ExprNode) float64 {
	switch x := expr.(type) {
	case *ast.BetweenExpr:
		return rateBetween
	case *ast.BinaryOperationExpr:
		return computeBinopFilterRate(x)
	case *ast.ColumnNameExpr:
		return rateFull
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
	}
	return rateFull
}

func computeBinopFilterRate(expr *ast.BinaryOperationExpr) float64 {
	switch expr.Op {
	case opcode.AndAnd:
		return computeFilterRate(expr.L) * computeFilterRate(expr.R)
	case opcode.OrOr:
		rate := computeFilterRate(expr.L) + computeFilterRate(expr.R)
		if rate > rateFull {
			rate = rateFull
		}
		return rate
	case opcode.EQ:
		return rateEqual
	case opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		return rateGreaterOrLess
	case opcode.NE:
		return rateNotEqual
	}
	return 1
}

func computeIsNullFilterRate(expr *ast.IsNullExpr) float64 {
	if expr.Not {
		return rateFull - rateIsNull
	}
	return rateIsNull
}

func computeIsTrueFilterRate(expr *ast.IsTruthExpr) float64 {
	if expr.True == 0 {
		if expr.Not {
			return rateFull - rateIsFalse
		}
		return rateIsFalse
	}
	if expr.Not {
		return rateIsFalse + rateIsNull
	}
	return rateFull - rateIsFalse - rateIsNull
}

func computePatternInFilterRate(expr *ast.PatternInExpr) float64 {
	if len(expr.List) > 0 {
		rate := rateEqual * float64(len(expr.List))
		if expr.Not {
			return rateFull - rate
		}
		return rate
	}
	return rateFull
}

func computePatternLikeFilterRate(expr *ast.PatternLikeExpr) float64 {
	if expr.Not {
		return rateFull - rateLike
	}
	return rateLike
}
