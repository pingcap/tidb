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

package executor

import (
	"github.com/pingcap/tidb/ast"
)

// IsReadOnly checks whether the input ast is readOnly.
func IsReadOnly(node ast.Node) bool {
	switch st := node.(type) {
	case *ast.SelectStmt:
		if st.LockTp == ast.SelectLockForUpdate {
			return false
		}

		checker := readOnlyChecker{
			readOnly: true,
		}

		node.Accept(&checker)
		return checker.readOnly
	case *ast.ExplainStmt:
		return true
	default:
		return false
	}
}

// readOnlyChecker checks whether a query's ast is readonly, if it satisfied
// 1. selectstmt;
// 2. need not to set var;
// it is readonly statement.
type readOnlyChecker struct {
	readOnly bool
}

// Enter implements Visitor interface.
func (checker *readOnlyChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.VariableExpr:
		// like func rewriteVariable(), this stands for SetVar.
		if !node.IsSystem && node.Value != nil {
			checker.readOnly = false
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (checker *readOnlyChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, checker.readOnly
}
