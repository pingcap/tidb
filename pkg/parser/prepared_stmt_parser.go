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

// prepared_stmt_parser.go handles PREPARE, EXECUTE, and DEALLOCATE statements.

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parsePrepareStmt parses: PREPARE stmt_name FROM preparable_stmt
func (p *HandParser) parsePrepareStmt() ast.StmtNode {
	stmt := Alloc[ast.PrepareStmt](p.arena)
	p.expect(prepare)
	stmt.Name = p.parseName()
	p.expect(from)

	if tok, ok := p.accept(stringLit); ok {
		stmt.SQLText = tok.Lit
		return stmt
	}

	// Must be user variable @var
	expr := p.parseVariableExpr()
	v, ok := expr.(*ast.VariableExpr)
	if !ok {
		p.syntaxErrorAt(p.peek())
		return nil
	}
	stmt.SQLVar = v
	return stmt
}

// parseExecuteStmt parses: EXECUTE stmt_name [USING @var_name [, @var_name] ...]
func (p *HandParser) parseExecuteStmt() ast.StmtNode {
	stmt := Alloc[ast.ExecuteStmt](p.arena)
	p.expect(execute)
	stmt.Name = p.parseName()

	if _, ok := p.accept(using); ok {
		for {
			expr := p.parseVariableExpr()
			stmt.UsingVars = append(stmt.UsingVars, expr)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}
	return stmt
}

// parseDeallocateStmt parses: {DEALLOCATE | DROP} PREPARE stmt_name
func (p *HandParser) parseDeallocateStmt() ast.StmtNode {
	stmt := Alloc[ast.DeallocateStmt](p.arena)
	// Consumes DEALLOCATE if called from dispatch, or DROP if called from parseDropStmt?
	// If called from dispatch (deallocate), we expect deallocate.
	// If called from parseDropStmt (drop + prepare), we expect prepare (Drop consumed).
	// But here I name it parseDeallocateStmt and use p.expect(deallocate).
	// Only for distinct DEALLOCATE statement.
	// For DROP PREPARE, I'll need a different path or logic.
	p.expect(deallocate)
	p.expect(prepare)
	stmt.Name = p.parseName()
	return stmt
}

// parseName parses a simple identifier name (identifiers and unreserved keywords).
func (p *HandParser) parseName() string {
	tok := p.next()
	if !isIdentLike(tok.Tp) || tok.Tp == stringLit {
		p.syntaxErrorAt(tok)
		return ""
	}
	return tok.Lit
}
