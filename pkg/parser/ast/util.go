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

// IsReadOnly checks whether the input ast is readOnly.
func IsReadOnly(node Node) bool {
	switch st := node.(type) {
	case *SelectStmt:
		if st.LockInfo != nil {
			switch st.LockInfo.LockType {
			case SelectLockForUpdate, SelectLockForUpdateNoWait, SelectLockForUpdateWaitN,
				SelectLockForShare, SelectLockForShareNoWait:
				return false
			}
		}

		return true
	case *ExplainStmt:
		return !st.Analyze || IsReadOnly(st.Stmt)
	case *DoStmt, *ShowStmt:
		return true
	case *SetOprStmt:
		for _, sel := range node.(*SetOprStmt).SelectList.Selects {
			if !IsReadOnly(sel) {
				return false
			}
		}
		return true
	case *SetOprSelectList:
		for _, sel := range node.(*SetOprSelectList).Selects {
			if !IsReadOnly(sel) {
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
