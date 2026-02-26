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
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

// view_parser.go handles parsing of CREATE VIEW statements.

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseCreateViewStmt parses:
//
//	CREATE [OR REPLACE] [ALGORITHM = {UNDEFINED|MERGE|TEMPTABLE}]
//	  [DEFINER = user] [SQL SECURITY {DEFINER|INVOKER}]
//	  VIEW name [(col_list)] AS select_stmt
func (p *HandParser) parseCreateViewStmt() ast.StmtNode {
	p.expect(create)
	stmt := Alloc[ast.CreateViewStmt](p.arena)

	// Set defaults (Algorithm=UNDEFINED and Security=DEFINER are zero values).
	// CheckOption=CASCADED suppresses "WITH LOCAL CHECK OPTION" in Restore.
	authDefiner := p.arena.AllocUserIdentity()
	authDefiner.CurrentUser = true
	stmt.Definer = authDefiner
	stmt.CheckOption = ast.CheckOptionCascaded

	// [OR REPLACE]
	if _, ok := p.accept(or); ok {
		p.expect(replace)
		stmt.OrReplace = true
	}

	// [ALGORITHM = {UNDEFINED|MERGE|TEMPTABLE}]
	if _, ok := p.accept(algorithm); ok {
		p.expect(eq)
		switch p.peek().Tp {
		case merge:
			p.next()
			stmt.Algorithm = ast.AlgorithmMerge
		case temptable:
			p.next()
			stmt.Algorithm = ast.AlgorithmTemptable
		default:
			// UNDEFINED or any other value
			p.next()
			stmt.Algorithm = ast.AlgorithmUndefined
		}
	}

	// [DEFINER = user]
	if _, ok := p.accept(definer); ok {
		p.expect(eq)
		if _, ok := p.accept(currentUser); ok {
			// already set to CurrentUser — DEFINER = CURRENT_USER
			if _, ok := p.accept('('); ok {
				p.accept(')')
			}
		} else {
			stmt.Definer = p.parseUserIdentity()
		}
	}

	// [SQL SECURITY {DEFINER|INVOKER}]
	if _, ok := p.accept(sql); ok {
		if _, ok := p.accept(security); ok {
			if _, ok := p.accept(invoker); ok {
				stmt.Security = ast.SecurityInvoker
			} else {
				// DEFINER or any other value → accept the token
				p.accept(definer)
				// default is DEFINER (zero value)
			}
		}
	}

	p.expect(view)

	// View name
	stmt.ViewName = p.parseTableName()

	// Optional column list: (col1, col2, ...)
	if _, ok := p.accept('('); ok {
		for {
			if tok, ok := p.expectAny(identifier); ok {
				stmt.Cols = append(stmt.Cols, ast.NewCIStr(tok.Lit))
			}
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
	}

	p.expect(as)

	// Record the start offset of the select body (right after AS keyword).
	selectStartOff := p.peek().Offset

	// AS select_stmt (SELECT, TABLE, VALUES, WITH, or parenthesized)
	// Must also handle set operations at the top level, e.g.
	//   CREATE VIEW v AS (SELECT a) UNION (SELECT b)
	if sub := p.parseSubquery(); sub != nil {
		sub = p.maybeParseUnion(sub)
		stmt.Select = sub.(ast.StmtNode)
	}

	// Compute end offset BEFORE parsing optional WITH CHECK OPTION.
	// This is the end of the select body text.
	selectEndOff := p.peek().Offset

	// [WITH [LOCAL|CASCADED] CHECK OPTION]
	if _, ok := p.accept(with); ok {
		// LOCAL
		if _, ok := p.accept(local); ok {
			stmt.CheckOption = ast.CheckOptionLocal
		}
		// CASCADED is the default — accept it but don't change.
		p.accept(cascaded)
		// CHECK OPTION
		p.expect(check)
		p.accept(option)
	}

	// Set text on the select body node, mirroring the original parser.
	if stmt.Select != nil {
		stmt.Select.SetText(p.connectionEncoding, strings.TrimSpace(p.src[selectStartOff:selectEndOff]))
	}

	return stmt
}
