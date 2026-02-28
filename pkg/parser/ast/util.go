// Copyright 2018 PingCAP, Inc.
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

package ast

import "math"

// UnspecifiedSize is unspecified size.
const (
	UnspecifiedSize = math.MaxUint64
)

// IsReadOnly checks that the ast is readonly.  If checkGlobalVars is set to
// true, then updates to global variables are counted as writes. Otherwise, if
// this flag is false, they are ignored.
func IsReadOnly(node Node, checkGlobalVars bool) bool {
	switch st := node.(type) {
	case *SelectStmt:
		if st.LockInfo != nil {
			switch st.LockInfo.LockType {
			case SelectLockForUpdate, SelectLockForUpdateNoWait, SelectLockForUpdateWaitN,
				SelectLockForShare, SelectLockForShareNoWait:
				return false
			}
		}

		if !checkGlobalVars {
			return true
		}

		checker := readOnlyChecker{
			readOnly: true,
		}

		node.Accept(&checker)
		return checker.readOnly
	case *ExplainStmt:
		return !st.Analyze || IsReadOnly(st.Stmt, checkGlobalVars)
	case *DoStmt, *ShowStmt:
		return true
	case *SetOprStmt:
		for _, sel := range node.(*SetOprStmt).SelectList.Selects {
			if !IsReadOnly(sel, checkGlobalVars) {
				return false
			}
		}
		return true
	case *SetOprSelectList:
		for _, sel := range node.(*SetOprSelectList).Selects {
			if !IsReadOnly(sel, checkGlobalVars) {
				return false
			}
		}
		return true
	case *AdminStmt:
		switch node.(*AdminStmt).Tp {
		case AdminShowDDL, AdminShowDDLJobs, AdminShowSlow,
			AdminCaptureBindings, AdminShowNextRowID, AdminShowDDLJobQueries,
			AdminShowDDLJobQueriesWithRange:
			return true
		default:
			return false
		}
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
func (checker *readOnlyChecker) Enter(in Node) (out Node, skipChildren bool) {
	if node, ok := in.(*VariableExpr); ok {
		// like func rewriteVariable(), this stands for SetVar.
		if node.IsSystem && node.Value != nil {
			checker.readOnly = false
			return in, true
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (checker *readOnlyChecker) Leave(in Node) (out Node, ok bool) {
	return in, checker.readOnly
}
