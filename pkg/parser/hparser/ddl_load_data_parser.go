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

package hparser

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseLoadDataStmt parses LOAD DATA statements.
func (p *HandParser) parseLoadDataStmt() ast.StmtNode {
	stmt := Alloc[ast.LoadDataStmt](p.arena)

	p.expect(tokLoad)
	p.expect(tokData)

	// [LOW_PRIORITY]
	if _, ok := p.acceptKeyword(tokLowPriority, "LOW_PRIORITY"); ok {
		stmt.LowPriority = true
	}

	if _, ok := p.acceptKeyword(tokLocal, "LOCAL"); ok {
		stmt.FileLocRef = ast.FileLocClient
	} else {
		stmt.FileLocRef = ast.FileLocServerOrRemote
	}

	// [REPLACE | IGNORE] (before INFILE)
	stmt.OnDuplicate = p.acceptOnDuplicateOpt()

	p.expect(tokInfile)
	if tok, ok := p.expect(tokStringLit); ok {
		stmt.Path = tok.Lit
	}

	// FormatOpt: [FORMAT 'string']
	if _, ok := p.acceptKeyword(tokFormat, "FORMAT"); ok {
		if tok, ok := p.expect(tokStringLit); ok {
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

	p.expect(tokInto)
	p.expect(tokTable)
	stmt.Table = p.parseTableName()

	// CharsetOpt
	if _, ok := p.acceptKeyword(tokCharacter, "CHARACTER"); ok {
		p.expect(tokSet)
		if tok, ok := p.expectAny(tokStringLit, tokIdentifier); ok {
			stmt.Charset = sptr(tok.Lit)
		}
	}

	// Fields/Lines options
	for {
		tok := p.peek()
		if tok.Tp == tokFields || tok.Tp == tokColumns {
			p.next()
			stmt.FieldsInfo = p.parseFieldsClause(true)
			if stmt.FieldsInfo == nil {
				return nil // validation error (e.g. [parser:1083])
			}
		} else if tok.Tp == tokLines {
			p.next()
			stmt.LinesInfo = p.parseLinesClause()
		} else {
			break
		}
	}

	// IGNORE n LINES
	if p.peekKeyword(tokIgnore, "IGNORE") {
		m := p.mark()
		p.next()
		if p.peek().Tp == tokIntLit {
			stmt.IgnoreLines = uptr(p.parseUint64())
			p.accept(tokLines)
		} else {
			p.restore(m)
		}
	}

	// ColumnNameOrUserVarListOptWithBrackets
	if _, ok := p.accept('('); ok {
		stmt.ColumnsAndUserVars = make([]*ast.ColumnNameOrUserVar, 0)
		for {
			if tok, ok := p.accept(tokIdentifier); ok {
				node := &ast.ColumnNameOrUserVar{ColumnName: &ast.ColumnName{Name: ast.NewCIStr(tok.Lit)}}
				stmt.ColumnsAndUserVars = append(stmt.ColumnsAndUserVars, node)
			} else if tok.Tp == tokSingleAtIdentifier {
				// @dummy user variables (lexer fuses @ + ident into tokSingleAtIdentifier)
				p.next()
				node := &ast.ColumnNameOrUserVar{UserVar: &ast.VariableExpr{Name: tok.Lit}}
				stmt.ColumnsAndUserVars = append(stmt.ColumnsAndUserVars, node)
			} else if tok.Tp == '@' {
				p.next()
				if tok, ok := p.expect(tokIdentifier); ok {
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
	if _, ok := p.accept(tokSet); ok {
		stmt.ColumnAssignments = make([]*ast.Assignment, 0)
		for {
			m := p.mark()
			col := p.parseColumnName()
			if col == nil {
				p.restore(m)
				break
			}
			p.expectAny(tokEq, tokAssignmentEq)
			expr := p.parseExpression(0)
			stmt.ColumnAssignments = append(stmt.ColumnAssignments, &ast.Assignment{Column: col, Expr: expr})
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}

	// WITH options: WITH name[=value][, name[=value]]...
	if _, ok := p.accept(tokWith); ok {
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
	if _, ok := p.acceptKeyword(tokReplace, "REPLACE"); ok {
		return ast.OnDuplicateKeyHandlingReplace
	}
	if _, ok := p.acceptKeyword(tokIgnore, "IGNORE"); ok {
		return ast.OnDuplicateKeyHandlingIgnore
	}
	return ast.OnDuplicateKeyHandlingError
}
