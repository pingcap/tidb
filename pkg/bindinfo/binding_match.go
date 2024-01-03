// Copyright 2023 PingCAP, Inc.
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

package bindinfo

import (
	"strings"
	"sync"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
)

var (
	// GetGlobalBindingHandle is a function to get the global binding handle.
	// It is mainly used to resolve cycle import issue.
	GetGlobalBindingHandle func(sctx sessionctx.Context) GlobalBindingHandle
)

// MatchSQLBinding returns the matched binding for this statement.
func MatchSQLBinding(sctx sessionctx.Context, stmtNode ast.StmtNode) (bindRecord *BindRecord, scope string, matched bool) {
	useBinding := sctx.GetSessionVars().UsePlanBaselines
	if !useBinding || stmtNode == nil {
		return nil, "", false
	}
	var err error
	bindRecord, scope, err = getBindRecord(sctx, stmtNode)
	if err != nil || bindRecord == nil || len(bindRecord.Bindings) == 0 {
		return nil, "", false
	}
	return bindRecord, scope, true
}

func getBindRecord(ctx sessionctx.Context, stmt ast.StmtNode) (*BindRecord, string, error) {
	// When the domain is initializing, the bind will be nil.
	if ctx.Value(SessionBindInfoKeyType) == nil {
		return nil, "", nil
	}
	stmtNode, normalizedSQL, sqlDigest, err := NormalizeStmtForBinding(stmt, ctx.GetSessionVars().CurrentDB, false)
	if err != nil || stmtNode == nil {
		return nil, "", err
	}
	var normalizedSQLUni, sqlDigestUni string
	if ctx.GetSessionVars().EnableUniversalBinding {
		_, normalizedSQLUni, sqlDigestUni, err = NormalizeStmtForBinding(stmt, ctx.GetSessionVars().CurrentDB, true)
		if err != nil {
			return nil, "", err
		}
	}

	// the priority: session normal > session universal > global normal > global universal
	sessionHandle := ctx.Value(SessionBindInfoKeyType).(SessionBindingHandle)
	if bindRecord := sessionHandle.GetSessionBinding(sqlDigest, normalizedSQL, ""); bindRecord != nil && bindRecord.HasEnabledBinding() {
		return bindRecord, metrics.ScopeSession, nil
	}
	if ctx.GetSessionVars().EnableUniversalBinding {
		if bindRecord := sessionHandle.GetSessionBinding(sqlDigestUni, normalizedSQLUni, ""); bindRecord != nil && bindRecord.HasEnabledBinding() {
			return bindRecord, metrics.ScopeSession, nil
		}
	}
	globalHandle := GetGlobalBindingHandle(ctx)
	if globalHandle == nil {
		return nil, "", nil
	}
	if bindRecord := globalHandle.GetGlobalBinding(sqlDigest, normalizedSQL, ""); bindRecord != nil && bindRecord.HasEnabledBinding() {
		return bindRecord, metrics.ScopeGlobal, nil
	}
	if ctx.GetSessionVars().EnableUniversalBinding {
		if bindRecord := globalHandle.GetGlobalBinding(sqlDigestUni, normalizedSQLUni, ""); bindRecord != nil && bindRecord.HasEnabledBinding() {
			return bindRecord, metrics.ScopeGlobal, nil
		}
	}
	return nil, "", nil
}

// NormalizeStmtForBinding normalizes a statement for binding.
// This function skips Explain automatically, and literals in in-lists will be normalized as '...'.
// For normal bindings, DB name will be completed automatically:
//
//	e.g. `select * from t where a in (1, 2, 3)` --> `select * from test.t where a in (...)`
//
// For universal bindings, DB name will be ignored:
//
//	e.g. `select * from test.t where a in (1, 2, 3)` --> `select * from t where a in (...)`
func NormalizeStmtForBinding(stmtNode ast.StmtNode, specifiedDB string, isUniversalBinding bool) (stmt ast.StmtNode, normalizedStmt, sqlDigest string, err error) {
	if isUniversalBinding {
		return normalizeStmt(stmtNode, specifiedDB, 2)
	}
	return normalizeStmt(stmtNode, specifiedDB, 1)
}

func eraseLastSemicolon(stmt ast.StmtNode) {
	sql := stmt.Text()
	if len(sql) > 0 && sql[len(sql)-1] == ';' {
		stmt.SetText(nil, sql[:len(sql)-1])
	}
}

// flag 0 is for plan cache, 1 is for normal bindings and 2 is for universal bindings.
// see comments in NormalizeStmtForPlanCache and NormalizeStmtForBinding.
func normalizeStmt(stmtNode ast.StmtNode, specifiedDB string, flag int) (stmt ast.StmtNode, normalizedStmt, sqlDigest string, err error) {
	normalize := func(n ast.StmtNode) (normalizedStmt, sqlDigest string) {
		eraseLastSemicolon(n)
		var digest *parser.Digest
		switch flag {
		case 1:
			normalizedStmt, digest = parser.NormalizeDigestForBinding(utilparser.RestoreWithDefaultDB(n, specifiedDB, n.Text()))
		case 2:
			normalizedStmt, digest = parser.NormalizeDigestForBinding(utilparser.RestoreWithoutDB(n))
		}
		return normalizedStmt, digest.String()
	}

	switch x := stmtNode.(type) {
	case *ast.ExplainStmt:
		// This function is only used to find bind record.
		// For some SQLs, such as `explain select * from t`, they will be entered here many times,
		// but some of them do not want to obtain bind record.
		// The difference between them is whether len(x.Text()) is empty. They cannot be distinguished by stmt.restore.
		// For these cases, we need return "" as normalize SQL and hash.
		if len(x.Text()) == 0 {
			return x.Stmt, "", "", nil
		}
		switch x.Stmt.(type) {
		case *ast.SelectStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
			normalizeSQL, digest := normalize(x.Stmt)
			return x.Stmt, normalizeSQL, digest, nil
		case *ast.SetOprStmt:
			normalizeExplainSQL, _ := normalize(x)

			idx := strings.Index(normalizeExplainSQL, "select")
			parenthesesIdx := strings.Index(normalizeExplainSQL, "(")
			if parenthesesIdx != -1 && parenthesesIdx < idx {
				idx = parenthesesIdx
			}
			// If the SQL is `EXPLAIN ((VALUES ROW ()) ORDER BY 1);`, the idx will be -1.
			if idx == -1 {
				hash := parser.DigestNormalized(normalizeExplainSQL)
				return x.Stmt, normalizeExplainSQL, hash.String(), nil
			}
			normalizeSQL := normalizeExplainSQL[idx:]
			hash := parser.DigestNormalized(normalizeSQL)
			return x.Stmt, normalizeSQL, hash.String(), nil
		}
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
		// This function is only used to find bind record.
		// For some SQLs, such as `explain select * from t`, they will be entered here many times,
		// but some of them do not want to obtain bind record.
		// The difference between them is whether len(x.Text()) is empty. They cannot be distinguished by stmt.restore.
		// For these cases, we need return "" as normalize SQL and hash.
		if len(x.Text()) == 0 {
			return x, "", "", nil
		}
		normalizedSQL, digest := normalize(x)
		return x, normalizedSQL, digest, nil
	}
	return nil, "", "", nil
}

// CollectTableNames gets all table names from ast.Node.
// This function is mainly for binding fuzzy matching.
// ** the return is read-only.
// For example:
//
//	`select * from t1 where a < 1` --> [t1]
//	`select * from db1.t1, t2 where a < 1` --> [db1.t1, t2]
//
// You can see more example at the TestExtractTableName.
func CollectTableNames(in ast.Node) []*ast.TableName {
	collector := tableNameCollectorPool.Get().(*tableNameCollector)
	collector.reset()
	in.Accept(collector)
	return collector.GetResult()
}

var tableNameCollectorPool = sync.Pool{
	New: func() any {
		return newCollectTableName()
	},
}

type tableNameCollector struct {
	tableNames []*ast.TableName
}

func newCollectTableName() *tableNameCollector {
	return &tableNameCollector{
		tableNames: make([]*ast.TableName, 0, 4),
	}
}

// Enter implements Visitor interface.
func (c *tableNameCollector) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	if node, ok := in.(*ast.TableName); ok {
		c.tableNames = append(c.tableNames, node)
		return in, true
	}
	return in, false
}

// Leave implements Visitor interface.
func (*tableNameCollector) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

func (c *tableNameCollector) GetResult() []*ast.TableName {
	return c.tableNames
}

func (c *tableNameCollector) reset() {
	c.tableNames = nil
	tableNameCollectorPool.Put(c)
}
