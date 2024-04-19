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

package norm

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
)

type option struct {
	specifiedDB string
	fuzz        bool
}

type optionFunc func(*option)

// WithFuzz specifies whether to eliminate schema names.
func WithFuzz(fuzz bool) optionFunc {
	return func(user *option) {
		user.fuzz = fuzz
	}
}

// WithSpecifiedDB specifies the specified DB name.
func WithSpecifiedDB(specifiedDB string) optionFunc {
	return func(user *option) {
		user.specifiedDB = specifiedDB
	}
}

// NormalizeStmtForBinding normalizes a statement for binding.
// when fuzz is false, schema names will be completed automatically: `select * from t` --> `select * from db . t`.
// when fuzz is true, schema names will be eliminated automatically: `select * from db . t` --> `select * from t`.
func NormalizeStmtForBinding(stmtNode ast.StmtNode, options ...optionFunc) (normalizedStmt, exactSQLDigest string) {
	opt := &option{}
	for _, option := range options {
		option(opt)
	}
	return normalizeStmt(stmtNode, opt.specifiedDB, opt.fuzz)
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

func eraseLastSemicolon(stmt ast.StmtNode) {
	sql := stmt.Text()
	if len(sql) > 0 && sql[len(sql)-1] == ';' {
		stmt.SetText(nil, sql[:len(sql)-1])
	}
}
