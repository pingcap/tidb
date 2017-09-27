// Copyright 2017 PingCAP, Inc.
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
)

// Cacheable checks whether the input ast is cacheable.
func Cacheable(node ast.Node) bool {
	checker := cacheableChecker{
		cacheable: true,
	}
	node.Accept(&checker)
	return checker.cacheable
}

type cacheableChecker struct {
	cacheable bool
}

// Enter implements Visitor interface.
func (checker *cacheableChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.SelectStmt:
		for _, field := range node.Fields.Fields {
			if field.Expr == nil {
				continue
			}
			if !checker.canExprBeCached(field.Expr) {
				checker.cacheable = false
				return in, true
			}
		}
	}
	return in, false
}

func (checker *cacheableChecker) canExprBeCached(exprNode ast.ExprNode) bool {
	switch exprNode.(type) {
	case *ast.VariableExpr:
		return false
	}
	return true
}

// Leave implements Visitor interface.
func (checker *cacheableChecker) Leave(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}
