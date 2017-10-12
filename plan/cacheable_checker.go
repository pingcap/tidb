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
	if _, isSelect := node.(*ast.SelectStmt); !isSelect {
		return false
	}
	checker := cacheableChecker{
		cacheable: true,
	}
	node.Accept(&checker)
	return checker.cacheable
}

// cacheableChecker checks whether a query's plan can be cached, querys that:
//	 1. have ExistsSubqueryExpr, or
//	 2. have VariableExpr
// will not be cached currently.
// NOTE: we can add more rules in the future.
type cacheableChecker struct {
	cacheable bool
}

// Enter implements Visitor interface.
func (checker *cacheableChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.VariableExpr, *ast.ExistsSubqueryExpr:
		checker.cacheable = false
		return in, true
	case *ast.FuncCallExpr:
		if node.FnName.L == ast.Now || node.FnName.L == ast.CurrentTimestamp ||
			node.FnName.L == ast.UTCTime || node.FnName.L == ast.Curtime ||
			node.FnName.L == ast.CurrentTime || node.FnName.L == ast.UTCTimestamp ||
			node.FnName.L == ast.UnixTimestamp {
			checker.cacheable = false
			return in, true
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (checker *cacheableChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, checker.cacheable
}
