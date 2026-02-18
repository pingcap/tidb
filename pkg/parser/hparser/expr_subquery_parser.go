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

package hparser

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseParenOrSubquery parses a parenthesized expression or subquery.
func (p *HandParser) parseParenOrSubquery() ast.ExprNode {
	p.next() // consume '('

	// Check if this is a subquery.
	tp := p.peek().Tp
	if tp == tokSelect || tp == tokWith || tp == tokTable || tp == tokValues {
		startOffset := p.peek().Offset
		// Use parseSubquery to handle SELECT, TABLE, VALUES, WITH, and UNIONs.
		query := p.parseSubquery()
		if query == nil {
			return nil
		}
		endOffset := p.peek().Offset // ')' token
		// Set subquery text from the original SQL (matching goyacc's behavior).
		if endOffset > startOffset && endOffset <= len(p.src) {
			query.SetText(nil, p.src[startOffset:endOffset])
		}
		p.expect(')')
		sub := Alloc[ast.SubqueryExpr](p.arena)
		sub.Query = query
		return sub
	}

	// When next token is '(', it's ambiguous: could be ((SELECT ...)) subquery
	// or ((expr)) nested parenthesized expression. Use speculative parsing.
	if tp == '(' {
		saved := p.mark()
		savedErrs := len(p.errs)
		startOffset := p.peek().Offset
		query := p.parseSubquery()
		if query != nil && p.peek().Tp == ')' {
			// Successfully parsed as subquery.
			p.errs = p.errs[:savedErrs]
			endOffset := p.peek().Offset
			if endOffset > startOffset && endOffset <= len(p.src) {
				query.SetText(nil, p.src[startOffset:endOffset])
			}
			p.expect(')')
			sub := Alloc[ast.SubqueryExpr](p.arena)
			sub.Query = query
			return sub
		}
		// Not a subquery — restore and parse as expression.
		p.errs = p.errs[:savedErrs]
		p.restore(saved)
	}

	// Standard parenthesized expression or row expression.
	expr := p.parseExpression(precNone)
	if expr == nil {
		return nil
	}

	// Check for row expression: (expr, expr, ...)
	if p.peek().Tp == ',' {
		values := []ast.ExprNode{expr}
		for {
			if _, ok := p.accept(','); !ok {
				break
			}
			e := p.parseExpression(precNone)
			if e == nil {
				return nil
			}
			values = append(values, e)
		}
		p.expect(')')
		row := Alloc[ast.RowExpr](p.arena)
		row.Values = values
		return row
	}

	p.expect(')')
	paren := Alloc[ast.ParenthesesExpr](p.arena)
	paren.Expr = expr
	return paren
}

// parseExistsSubquery parses EXISTS (SELECT ...).
func (p *HandParser) parseExistsSubquery() ast.ExprNode {
	p.next() // consume EXISTS
	p.expect('(')
	// Use parseSubquery which handles nested parens, WITH, TABLE, VALUES, and UNIONs.
	query := p.parseSubquery()
	if query == nil {
		return nil
	}
	// Clear IsInBraces since EXISTS's own parens provide the wrapping.
	if s, ok := query.(*ast.SelectStmt); ok {
		s.IsInBraces = false
	} else if s, ok := query.(*ast.SetOprStmt); ok {
		s.IsInBraces = false
	}
	p.expect(')')

	sub := Alloc[ast.SubqueryExpr](p.arena)
	sub.Query = query
	sub.Exists = true

	exists := Alloc[ast.ExistsSubqueryExpr](p.arena)
	exists.Sel = sub
	return exists
}

// parseCaseExpr parses CASE [expr] WHEN ... THEN ... [ELSE ...] END.
func (p *HandParser) parseCaseExpr() ast.ExprNode {
	p.next() // consume CASE

	node := Alloc[ast.CaseExpr](p.arena)

	// Optional value expression.
	if p.peek().Tp != tokWhen {
		node.Value = p.parseExpression(precNone)
	}

	// WHEN clauses.
	for {
		if _, ok := p.accept(tokWhen); !ok {
			break
		}
		when := Alloc[ast.WhenClause](p.arena)
		when.Expr = p.parseExpression(precNone)
		p.expect(tokThen)
		when.Result = p.parseExpression(precNone)
		node.WhenClauses = append(node.WhenClauses, when)
	}

	// Optional ELSE.
	if _, ok := p.accept(tokElse); ok {
		node.ElseClause = p.parseExpression(precNone)
	}

	p.expect(tokEnd)
	return node
}

// parseDefaultExpr parses DEFAULT or DEFAULT(column).
func (p *HandParser) parseDefaultExpr() ast.ExprNode {
	p.next() // consume DEFAULT
	node := Alloc[ast.DefaultExpr](p.arena)

	if _, ok := p.accept('('); ok {
		col := Alloc[ast.ColumnName](p.arena)
		// Parse potentially dotted column name: col / tbl.col / schema.tbl.col
		part1 := p.next().Lit
		if _, ok2 := p.accept('.'); ok2 {
			part2 := p.next().Lit
			if _, ok3 := p.accept('.'); ok3 {
				// schema.table.column
				part3 := p.next().Lit
				col.Schema = ast.NewCIStr(part1)
				col.Table = ast.NewCIStr(part2)
				col.Name = ast.NewCIStr(part3)
			} else {
				// table.column
				col.Table = ast.NewCIStr(part1)
				col.Name = ast.NewCIStr(part2)
			}
		} else {
			// simple column
			col.Name = ast.NewCIStr(part1)
		}
		node.Name = col
		p.expect(')')
	}

	return node
}

// parseInExpr parses [NOT] IN (expr_list) or [NOT] IN (subquery).
func (p *HandParser) parseInExpr(left ast.ExprNode, not bool) ast.ExprNode {
	p.next() // consume IN
	p.expect('(')

	node := Alloc[ast.PatternInExpr](p.arena)
	node.Expr = left
	node.Not = not

	// Check for subquery (SELECT, WITH, TABLE, VALUES).
	if p.peek().Tp == tokSelect || p.peek().Tp == tokWith {
		query := p.parseSelectStmt()
		if query == nil {
			return nil
		}
		// Handle UNION/EXCEPT/INTERSECT after the initial SELECT.
		result := p.maybeParseUnion(query)
		sub := Alloc[ast.SubqueryExpr](p.arena)
		sub.Query = result
		node.Sel = sub
	} else if p.peek().Tp == tokTable || p.peek().Tp == tokValues {
		var query ast.StmtNode
		if p.peek().Tp == tokTable {
			query = p.parseTableStmt()
		} else {
			query = p.parseValuesStmt()
		}
		sub := Alloc[ast.SubqueryExpr](p.arena)
		sub.Query = query.(*ast.SelectStmt)
		node.Sel = sub
	} else {
		// Expression list.
		for {
			expr := p.parseExpression(precNone)
			if expr == nil {
				return nil
			}
			node.List = append(node.List, expr)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}

	p.expect(')')
	return node
}
