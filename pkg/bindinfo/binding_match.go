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
	"github.com/pingcap/tidb/pkg/util/hint"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
)

var (
	// GetGlobalBindingHandle is a function to get the global binding handle.
	// It is mainly used to resolve cycle import issue.
	GetGlobalBindingHandle func(sctx sessionctx.Context) GlobalBindingHandle
)

// BindingMatchInfo records necessary information for fuzzy binding matching.
// This is mainly for plan cache to avoid normalizing the same statement repeatedly.
type BindingMatchInfo struct {
	FuzzyDigest string
	TableNames  []*ast.TableName
}

// MatchSQLBindingForPlanCache matches binding for plan cache.
func MatchSQLBindingForPlanCache(sctx sessionctx.Context, stmtNode ast.StmtNode, info *BindingMatchInfo) (bindingSQL string, ignoreBinding bool) {
	bindRecord, _ := getBindRecord(sctx, stmtNode, info)
	if bindRecord != nil {
		if enabledBinding := bindRecord.FindEnabledBinding(); enabledBinding != nil {
			bindingSQL = enabledBinding.BindSQL
			ignoreBinding = enabledBinding.Hint.ContainTableHint(hint.HintIgnorePlanCache)
		}
	}
	return
}

// MatchSQLBinding returns the matched binding for this statement.
func MatchSQLBinding(sctx sessionctx.Context, stmtNode ast.StmtNode) (bindRecord *BindRecord, scope string, matched bool) {
	bindRecord, scope = getBindRecord(sctx, stmtNode, nil)
	if bindRecord == nil || len(bindRecord.Bindings) == 0 {
		return nil, "", false
	}
	return bindRecord, scope, true
}

func getBindRecord(sctx sessionctx.Context, stmtNode ast.StmtNode, info *BindingMatchInfo) (*BindRecord, string) {
	useBinding := sctx.GetSessionVars().UsePlanBaselines
	if !useBinding || stmtNode == nil {
		return nil, ""
	}
	// When the domain is initializing, the bind will be nil.
	if sctx.Value(SessionBindInfoKeyType) == nil {
		return nil, ""
	}

	// record the normalization result into info to avoid repeat normalization next time.
	var fuzzyDigest string
	var tableNames []*ast.TableName
	if info == nil || info.TableNames == nil || info.FuzzyDigest == "" {
		_, fuzzyDigest = NormalizeStmtForFuzzyBinding(stmtNode)
		tableNames = CollectTableNames(stmtNode)
		if info != nil {
			info.FuzzyDigest = fuzzyDigest
			info.TableNames = tableNames
		}
	} else {
		fuzzyDigest = info.FuzzyDigest
		tableNames = info.TableNames
	}

	sessionHandle := sctx.Value(SessionBindInfoKeyType).(SessionBindingHandle)
	if bindRecord, err := sessionHandle.MatchSessionBinding(sctx, fuzzyDigest, tableNames); err == nil && bindRecord != nil && bindRecord.HasEnabledBinding() {
		return bindRecord, metrics.ScopeSession
	}
	globalHandle := GetGlobalBindingHandle(sctx)
	if globalHandle == nil {
		return nil, ""
	}
	if bindRecord, err := globalHandle.MatchGlobalBinding(sctx, fuzzyDigest, tableNames); err == nil && bindRecord != nil && bindRecord.HasEnabledBinding() {
		return bindRecord, metrics.ScopeGlobal
	}
	return nil, ""
}

func eraseLastSemicolon(stmt ast.StmtNode) {
	sql := stmt.Text()
	if len(sql) > 0 && sql[len(sql)-1] == ';' {
		stmt.SetText(nil, sql[:len(sql)-1])
	}
}

// NormalizeStmtForBinding normalizes a statement for binding.
// Schema names will be completed automatically: `select * from t` --> `select * from db . t`.
func NormalizeStmtForBinding(stmtNode ast.StmtNode, specifiedDB string) (normalizedStmt, exactSQLDigest string) {
	return normalizeStmt(stmtNode, specifiedDB, false)
}

// NormalizeStmtForFuzzyBinding normalizes a statement for fuzzy matching.
// Schema names will be eliminated automatically: `select * from db . t` --> `select * from t`.
func NormalizeStmtForFuzzyBinding(stmtNode ast.StmtNode) (normalizedStmt, fuzzySQLDigest string) {
	return normalizeStmt(stmtNode, "", true)
}

// NormalizeStmtForBinding normalizes a statement for binding.
// This function skips Explain automatically, and literals in in-lists will be normalized as '...'.
// For normal bindings, DB name will be completed automatically:
//
//	e.g. `select * from t where a in (1, 2, 3)` --> `select * from test.t where a in (...)`
func normalizeStmt(stmtNode ast.StmtNode, specifiedDB string, fuzzy bool) (normalizedStmt, sqlDigest string) {
	normalize := func(n ast.StmtNode) (normalizedStmt, sqlDigest string) {
		eraseLastSemicolon(n)
		var digest *parser.Digest
		var normalizedSQL string
		if !fuzzy {
			normalizedSQL = utilparser.RestoreWithDefaultDB(n, specifiedDB, n.Text())
		} else {
			normalizedSQL = utilparser.RestoreWithoutDB(n)
		}
		normalizedStmt, digest = parser.NormalizeDigestForBinding(normalizedSQL)
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
			return "", ""
		}
		switch x.Stmt.(type) {
		case *ast.SelectStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
			normalizeSQL, digest := normalize(x.Stmt)
			return normalizeSQL, digest
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
				return normalizeExplainSQL, hash.String()
			}
			normalizeSQL := normalizeExplainSQL[idx:]
			hash := parser.DigestNormalized(normalizeSQL)
			return normalizeSQL, hash.String()
		}
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt:
		// This function is only used to find bind record.
		// For some SQLs, such as `explain select * from t`, they will be entered here many times,
		// but some of them do not want to obtain bind record.
		// The difference between them is whether len(x.Text()) is empty. They cannot be distinguished by stmt.restore.
		// For these cases, we need return "" as normalize SQL and hash.
		if len(x.Text()) == 0 {
			return "", ""
		}
		normalizedSQL, digest := normalize(x)
		return normalizedSQL, digest
	}
	return "", ""
}

func fuzzyMatchBindingTableName(currentDB string, stmtTableNames, bindingTableNames []*ast.TableName) (numWildcards int, matched bool) {
	if len(stmtTableNames) != len(bindingTableNames) {
		return 0, false
	}
	for i := range stmtTableNames {
		if stmtTableNames[i].Name.L != bindingTableNames[i].Name.L {
			return 0, false
		}
		if bindingTableNames[i].Schema.L == "*" {
			numWildcards++
		}
		if bindingTableNames[i].Schema.L == stmtTableNames[i].Schema.L || // exactly same, or
			(stmtTableNames[i].Schema.L == "" && bindingTableNames[i].Schema.L == currentDB) || // equal to the current DB, or
			bindingTableNames[i].Schema.L == "*" { // fuzzy match successfully
			continue
		}
		return 0, false
	}
	return numWildcards, true
}

// isFuzzyBinding checks whether the stmtNode is a fuzzy binding.
func isFuzzyBinding(stmt ast.Node) bool {
	for _, t := range CollectTableNames(stmt) {
		if t.Schema.L == "*" {
			return true
		}
	}
	return false
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
	defer func() {
		collector.tableNames = nil
		tableNameCollectorPool.Put(collector)
	}()
	in.Accept(collector)
	return collector.tableNames
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
