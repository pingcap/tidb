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

	p.expect(57480)
	p.expect(57679)

	// [LOW_PRIORITY]
	if _, ok := p.acceptKeyword(57487, "LOW_PRIORITY"); ok {
		stmt.LowPriority = true
	}

	if _, ok := p.acceptKeyword(57770, "LOCAL"); ok {
		stmt.FileLocRef = ast.FileLocClient
	} else {
		stmt.FileLocRef = ast.FileLocServerOrRemote
	}

	// [REPLACE | IGNORE] (before INFILE)
	stmt.OnDuplicate = p.acceptOnDuplicateOpt()

	p.expect(57450)
	if tok, ok := p.expect(57353); ok {
		stmt.Path = tok.Lit
	}

	// FormatOpt: [FORMAT 'string']
	if _, ok := p.acceptKeyword(57728, "FORMAT"); ok {
		if tok, ok := p.expect(57353); ok {
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

	p.expect(57463)
	p.expect(57556)
	stmt.Table = p.parseTableName()

	// CharsetOpt
	if _, ok := p.acceptKeyword(57382, "CHARACTER"); ok {
		p.expect(57541)
		if tok, ok := p.expectAny(57353, 57346); ok {
			stmt.Charset = sptr(tok.Lit)
		}
	}

	// Fields/Lines options
	for {
		tok := p.peek()
		if tok.Tp == 57722 || tok.Tp == 57653 {
			p.next()
			stmt.FieldsInfo = p.parseFieldsClause(true)
			if stmt.FieldsInfo == nil {
				return nil // validation error (e.g. [parser:1083])
			}
		} else if tok.Tp == 57479 {
			p.next()
			stmt.LinesInfo = p.parseLinesClause()
		} else {
			break
		}
	}

	// IGNORE n LINES
	if p.peekKeyword(57446, "IGNORE") {
		m := p.mark()
		p.next()
		if p.peek().Tp == 58197 {
			stmt.IgnoreLines = uptr(p.parseUint64())
			p.accept(57479)
		} else {
			p.restore(m)
		}
	}

	// ColumnNameOrUserVarListOptWithBrackets
	if _, ok := p.accept('('); ok {
		stmt.ColumnsAndUserVars = make([]*ast.ColumnNameOrUserVar, 0)
		for {
			if tok, ok := p.accept(57346); ok {
				node := &ast.ColumnNameOrUserVar{ColumnName: &ast.ColumnName{Name: ast.NewCIStr(tok.Lit)}}
				stmt.ColumnsAndUserVars = append(stmt.ColumnsAndUserVars, node)
			} else if tok.Tp == 57354 {
				// @dummy user variables (lexer fuses @ + ident into 57354)
				p.next()
				node := &ast.ColumnNameOrUserVar{UserVar: &ast.VariableExpr{Name: tok.Lit}}
				stmt.ColumnsAndUserVars = append(stmt.ColumnsAndUserVars, node)
			} else if tok.Tp == '@' {
				p.next()
				if tok, ok := p.expect(57346); ok {
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
	if _, ok := p.accept(57541); ok {
		stmt.ColumnAssignments = make([]*ast.Assignment, 0)
		for {
			m := p.mark()
			col := p.parseColumnName()
			if col == nil {
				p.restore(m)
				break
			}
			p.expectAny(58202, 58201)
			expr := p.parseExpression(0)
			stmt.ColumnAssignments = append(stmt.ColumnAssignments, &ast.Assignment{Column: col, Expr: expr})
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}

	// WITH options: WITH name[=value][, name[=value]]...
	if _, ok := p.accept(57590); ok {
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
	if _, ok := p.acceptKeyword(57530, "REPLACE"); ok {
		return ast.OnDuplicateKeyHandlingReplace
	}
	if _, ok := p.acceptKeyword(57446, "IGNORE"); ok {
		return ast.OnDuplicateKeyHandlingIgnore
	}
	return ast.OnDuplicateKeyHandlingError
}
