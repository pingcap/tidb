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

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseLoadDataStmt parses LOAD DATA statements.
func (p *HandParser) parseLoadDataStmt() ast.StmtNode {
	stmt := Alloc[ast.LoadDataStmt](p.arena)

	p.expect(load)
	p.expect(data)

	// [LOW_PRIORITY]
	if _, ok := p.acceptKeyword(lowPriority, "LOW_PRIORITY"); ok {
		stmt.LowPriority = true
	}

	if _, ok := p.acceptKeyword(local, "LOCAL"); ok {
		stmt.FileLocRef = ast.FileLocClient
	} else {
		stmt.FileLocRef = ast.FileLocServerOrRemote
	}

	// [REPLACE | IGNORE] (before INFILE)
	stmt.OnDuplicate = p.acceptOnDuplicateOpt()

	p.expect(infile)
	if tok, ok := p.expect(stringLit); ok {
		stmt.Path = tok.Lit
	}

	// FormatOpt: [FORMAT 'string']
	if _, ok := p.acceptKeyword(format, "FORMAT"); ok {
		if tok, ok := p.expect(stringLit); ok {
			stmt.Format = sptr(tok.Lit)
		}
	}

	// [REPLACE | IGNORE] (after INFILE path, only if not already set)
	if stmt.OnDuplicate == ast.OnDuplicateKeyHandlingError {
		stmt.OnDuplicate = p.acceptOnDuplicateOpt()
	}

	// If LOCAL specified and no modifier, default is IGNORE (logic from parser.y)
	if stmt.FileLocRef == ast.FileLocClient && stmt.OnDuplicate == ast.OnDuplicateKeyHandlingError {
		stmt.OnDuplicate = ast.OnDuplicateKeyHandlingIgnore
	}

	p.expect(into)
	p.expect(tableKwd)
	stmt.Table = p.parseTableName()

	// CharsetOpt
	if _, ok := p.acceptKeyword(character, "CHARACTER"); ok {
		p.expect(set)
		if tok, ok := p.expectAny(stringLit, identifier); ok {
			stmt.Charset = sptr(tok.Lit)
		}
	}

	// Fields/Lines options
	for {
		tok := p.peek()
		if tok.Tp == fields || tok.Tp == columns {
			p.next()
			stmt.FieldsInfo = p.parseFieldsClause(true)
			if stmt.FieldsInfo == nil {
				return nil // validation error (e.g. [parser:1083])
			}
		} else if tok.Tp == lines {
			p.next()
			stmt.LinesInfo = p.parseLinesClause()
		} else {
			break
		}
	}

	// IGNORE n LINES
	if p.peekKeyword(ignore, "IGNORE") {
		m := p.mark()
		p.next()
		if p.peek().Tp == intLit {
			stmt.IgnoreLines = uptr(p.parseUint64())
			p.accept(lines)
		} else {
			p.restore(m)
		}
	}

	// ColumnNameOrUserVarListOptWithBrackets
	if _, ok := p.accept('('); ok {
		stmt.ColumnsAndUserVars = make([]*ast.ColumnNameOrUserVar, 0)
		for {
			tok := p.peek()
			if isIdentLike(tok.Tp) && tok.Tp != stringLit {
				p.next()
				node := &ast.ColumnNameOrUserVar{ColumnName: &ast.ColumnName{Name: ast.NewCIStr(tok.Lit)}}
				stmt.ColumnsAndUserVars = append(stmt.ColumnsAndUserVars, node)
			} else if tok.Tp == singleAtIdentifier {
				// @dummy user variables (lexer fuses @ + ident into singleAtIdentifier)
				p.next()
				node := &ast.ColumnNameOrUserVar{UserVar: &ast.VariableExpr{Name: tok.Lit}}
				stmt.ColumnsAndUserVars = append(stmt.ColumnsAndUserVars, node)
			} else if tok.Tp == '@' {
				p.next()
				if tok, ok := p.expect(identifier); ok {
					node := &ast.ColumnNameOrUserVar{UserVar: &ast.VariableExpr{Name: tok.Lit}}
					stmt.ColumnsAndUserVars = append(stmt.ColumnsAndUserVars, node)
				}
			}
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
	}

	// SET assignments
	if _, ok := p.accept(set); ok {
		stmt.ColumnAssignments = make([]*ast.Assignment, 0)
		for {
			m := p.mark()
			col := p.parseColumnName()
			if col == nil {
				p.restore(m)
				break
			}
			p.expectAny(eq, assignmentEq)
			expr := p.parseExprOrDefault()
			stmt.ColumnAssignments = append(stmt.ColumnAssignments, &ast.Assignment{Column: col, Expr: expr})
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}

	// WITH options: WITH name[=value][, name[=value]]...
	if _, ok := p.accept(with); ok {
		stmt.Options = make([]*ast.LoadDataOpt, 0)
		for {
			tok := p.next() // option name (identifier or keyword)
			opt := &ast.LoadDataOpt{Name: strings.ToLower(tok.Lit)}
			if p.acceptEqOrAssign() {
				opt.Value = p.parseExpression(precNone)
			}
			stmt.Options = append(stmt.Options, opt)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}

	return stmt
}

// acceptOnDuplicateOpt accepts optional REPLACE or IGNORE keyword
// and returns the corresponding OnDuplicateKeyHandlingType.
func (p *HandParser) acceptOnDuplicateOpt() ast.OnDuplicateKeyHandlingType {
	if _, ok := p.acceptKeyword(replace, "REPLACE"); ok {
		return ast.OnDuplicateKeyHandlingReplace
	}
	if _, ok := p.acceptKeyword(ignore, "IGNORE"); ok {
		return ast.OnDuplicateKeyHandlingIgnore
	}
	return ast.OnDuplicateKeyHandlingError
}
