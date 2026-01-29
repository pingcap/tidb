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
)

// ResolveExprAndReplace replaces columns fields of expressions by children logical plans.
func ResolveExprAndReplace(origin expression.Expression, replace map[string]*expression.Column) expression.Expression {
	switch expr := origin.(type) {
	case *expression.Column:
		return ResolveColumnAndReplace(expr, replace)
	case *expression.CorrelatedColumn:
		newCol, changed := resolveColumnAndReplace(&expr.Column, replace)
		if !changed {
			return expr
		}
		newExpr := expr.Clone().(*expression.CorrelatedColumn)
		newExpr.Data = expr.Data
		newExpr.Column = *newCol
		return newExpr
	case *expression.ScalarFunction:
		for i, arg := range expr.GetArgs() {
			expr.GetArgs()[i] = ResolveExprAndReplace(arg, replace)
		}
		return expr
	}
	return origin
}

// ResolveColumnAndReplace replaces columns fields of expressions by children logical plans.
func ResolveColumnAndReplace(origin *expression.Column, replace map[string]*expression.Column) *expression.Column {
	newCol, _ := resolveColumnAndReplace(origin, replace)
	return newCol
}

func resolveColumnAndReplace(origin *expression.Column, replace map[string]*expression.Column) (*expression.Column, bool) {
	dst := replace[string(origin.HashCode())]
	if dst != nil {
		// To avoid origin column is shared by multiple operators,
		// need to clone it before modification.
		newCol := dst.Clone().(*expression.Column)
		newCol.RetType, newCol.InOperand = origin.RetType, origin.InOperand
		return newCol, true
	}
	return origin, false
}

// SetPredicatePushDownFlag is a hook for other packages to set rule flag.
var SetPredicatePushDownFlag func(uint64) uint64
