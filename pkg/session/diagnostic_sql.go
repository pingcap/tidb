// Copyright 2026 PingCAP, Inc.
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

package session

import "github.com/pingcap/tidb/pkg/parser/ast"

// isDiagnosticSQLAllowed returns whether stmt is safe for the user-facing
// diagnostic SQL endpoint. Internal restricted SQL is handled by the caller
// and intentionally does not use this predicate.
//
// The diagnostic endpoint is deliberately an allowlist: it accepts a
// non-ANALYZE EXPLAIN over a SELECT (or a SELECT set operation), USE, and the
// explicitly listed metadata SHOW statements. A generic read-only check is
// insufficient because SELECT can acquire locks, write a file, assign a
// variable, or call functions that change session/sequence or advisory-lock
// state; future SHOW statement types must also be reviewed before they are
// exposed here.
func isDiagnosticSQLAllowed(stmt ast.StmtNode) bool {
	switch node := stmt.(type) {
	case *ast.UseStmt:
		return true
	case *ast.ShowStmt:
		if !diagnosticSQLReadOnlyShowTypes[node.Tp] {
			return false
		}
		checker := diagnosticSQLChecker{}
		ast.Walk(node, &checker)
		return !checker.denied
	case *ast.ExplainStmt:
		if node.Analyze {
			return false
		}
		switch child := node.Stmt.(type) {
		case *ast.SelectStmt:
			if child.Kind != ast.SelectStmtKindSelect {
				return false
			}
		case *ast.SetOprStmt:
			// Set operations contain SELECT query blocks. The AST walk below
			// checks every child block for the same side-effect restrictions.
		default:
			return false
		}
		checker := diagnosticSQLChecker{}
		ast.Walk(node, &checker)
		return !checker.denied
	default:
		return false
	}
}

type diagnosticSQLChecker struct {
	denied bool
}

func (c *diagnosticSQLChecker) Enter(node ast.Node) (skipChildren bool) {
	switch n := node.(type) {
	case *ast.CommonTableExpression:
		// The SubqueryExpr stored on a CTE is the query definition itself,
		// rather than an expression that the optimizer can evaluate as a
		// scalar/EXISTS subquery. Walk its query directly so side effects in
		// the definition are still checked without rejecting read-only CTEs.
		if n.Query != nil && n.Query.Query != nil {
			ast.Walk(n.Query.Query, c)
		}
		return true
	case *ast.SubqueryExpr:
		// Non-CTE subqueries may be evaluated by the optimizer while it is
		// compiling EXPLAIN. Reject them before compilation can start.
		c.denied = true
	case *ast.SelectStmt:
		// Any lock clause can cause TiKV lock-resolution or lock-writing
		// requests even though the statement is syntactically a SELECT.
		if n.Kind != ast.SelectStmtKindSelect || n.LockInfo != nil || n.SelectIntoOpt != nil || hasDiagnosticSQLSetVarHint(n) {
			c.denied = true
		}
	case *ast.VariableExpr:
		// This covers expressions such as @var := value.
		if n.Value != nil {
			c.denied = true
		}
	case *ast.TableOptimizerHint:
		if n.HintName.L == "set_var" {
			c.denied = true
		}
	case *ast.FuncCallExpr:
		if diagnosticSQLSideEffectFunctions[n.FnName.L] {
			c.denied = true
		}
	}
	return c.denied
}

func hasDiagnosticSQLSetVarHint(stmt *ast.SelectStmt) bool {
	for _, hint := range stmt.TableHints {
		if hint != nil && hint.HintName.L == "set_var" {
			return true
		}
	}
	if stmt.SelectStmtOpts == nil {
		return false
	}
	for _, hint := range stmt.SelectStmtOpts.TableHints {
		if hint != nil && hint.HintName.L == "set_var" {
			return true
		}
	}
	return false
}

func (c *diagnosticSQLChecker) Leave(ast.Node) (proceed bool) {
	return !c.denied
}

var diagnosticSQLSideEffectFunctions = map[string]bool{
	ast.GetLock:         true,
	ast.ReleaseLock:     true,
	ast.ReleaseAllLocks: true,
	ast.LastInsertId:    true,
	ast.NextVal:         true,
	ast.LastVal:         true,
	ast.SetVal:          true,
	ast.Sleep:           true,
	ast.SetVar:          true,
}

// These SHOW forms only inspect metadata, session state, or already available
// status information. Keep this list explicit: an unreviewed SHOW form must
// not become available merely because it was added to the parser.
var diagnosticSQLReadOnlyShowTypes = map[ast.ShowStmtType]bool{
	ast.ShowEngines:               true,
	ast.ShowDatabases:             true,
	ast.ShowTables:                true,
	ast.ShowTableStatus:           true,
	ast.ShowColumns:               true,
	ast.ShowWarnings:              true,
	ast.ShowCharset:               true,
	ast.ShowVariables:             true,
	ast.ShowStatus:                true,
	ast.ShowCollation:             true,
	ast.ShowCreateTable:           true,
	ast.ShowCreateView:            true,
	ast.ShowCreateUser:            true,
	ast.ShowCreateSequence:        true,
	ast.ShowCreatePlacementPolicy: true,
	ast.ShowCreateDatabase:        true,
	ast.ShowCreateResourceGroup:   true,
	ast.ShowCreateProcedure:       true,
	ast.ShowGrants:                true,
	ast.ShowMaskingPolicies:       true,
	ast.ShowTriggers:              true,
	ast.ShowProcedureStatus:       true,
	ast.ShowFunctionStatus:        true,
	ast.ShowIndex:                 true,
	ast.ShowProcessList:           true,
	ast.ShowOpenTables:            true,
	ast.ShowEvents:                true,
	ast.ShowPlugins:               true,
	ast.ShowPrivileges:            true,
	ast.ShowErrors:                true,
	ast.ShowBuiltins:              true,
	ast.ShowMasterStatus:          true,
	ast.ShowBinlogStatus:          true,
	ast.ShowReplicaStatus:         true,
}
