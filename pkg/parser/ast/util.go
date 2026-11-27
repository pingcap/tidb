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

		checker := readOnlyChecker{
			readOnly:        true,
			checkGlobalVars: checkGlobalVars,
		}

		Walk(node, &checker)
		return checker.readOnly
	case *ExplainStmt:
		return !st.Analyze || IsReadOnly(st.Stmt, checkGlobalVars)
	case *DoStmt:
		checker := readOnlyChecker{
			readOnly:        true,
			checkGlobalVars: checkGlobalVars,
		}

		Walk(node, &checker)
		return checker.readOnly
	case *ShowStmt:
		return true
	case *RecommendIndexStmt:
		return st.Action == RecommendIndexActionRun || st.Action == RecommendIndexActionReturn
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
	case *TraceStmt:
		return IsReadOnly(st.Stmt, checkGlobalVars)
	default:
		return false
	}
}

// readOnlyChecker checks whether a query's ast is readonly, if it satisfies:
// 1. does not set a system variable (when checkGlobalVars is true);
// 2. does not call state-mutating sequence functions (nextval/setval).
type readOnlyChecker struct {
	readOnly        bool
	checkGlobalVars bool
}

// Enter implements InPlaceVisitor interface.
func (checker *readOnlyChecker) Enter(in Node) (skipChildren bool) {
	switch node := in.(type) {
	case *VariableExpr:
		// like func rewriteVariable(), this stands for SetVar.
		if checker.checkGlobalVars && node.IsSystem && node.Value != nil {
			checker.readOnly = false
			return true
		}
	case *FuncCallExpr:
		if node.Schema.L == "" && (node.FnName.L == NextVal || node.FnName.L == SetVal) {
			checker.readOnly = false
			return true
		}
	}
	return false
}

// Leave implements InPlaceVisitor interface.
func (checker *readOnlyChecker) Leave(Node) (proceed bool) {
	return checker.readOnly
}
