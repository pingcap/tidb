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

package parser

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// peeksThroughParensToSubquery checks whether nested '(' tokens eventually
// lead to a subquery keyword (SELECT, WITH, TABLE, VALUES). This avoids
// speculative parsing that can overflow the ring buffer (maxLookahead = 8)
// when there are many levels of nested parentheses ((((expr)))).
func (p *HandParser) peeksThroughParensToSubquery() bool {
	for i := 0; i < maxLookahead-1; i++ {
		tok := p.peekN(i)
		switch tok.Tp {
		case '(':
			continue
		case 57540, 57590, 57556, 57580:
			return true
		default:
			return false
		}
	}
	return false
}

// parseParenOrSubquery parses a parenthesized expression or subquery.
func (p *HandParser) parseParenOrSubquery() ast.ExprNode {
	p.next() // consume '('

	// Check if this is a subquery.
	tp := p.peek().Tp
	if tp == 57540 || tp == 57590 || tp == 57556 || tp == 57580 {
		startOffset := p.peek().Offset
		// Use parseSubquery to handle SELECT, TABLE, VALUES, WITH, and UNIONs.
		query := p.parseSubquery()
		if query == nil {
			return nil
		}
		endOffset := p.peek().Offset // ')' token
		// Set subquery text from the original SQL (matching the behavior).
		if endOffset > startOffset && endOffset <= len(p.src) {
			query.SetText(nil, p.src[startOffset:endOffset])
		}
		p.expect(')')
		sub := Alloc[ast.SubqueryExpr](p.arena)
		sub.Query = query
		return sub
	}

	// When next token is '(', it's ambiguous: could be ((SELECT ...)) subquery
	// or ((expr)) nested parenthesized expression. Peek through nested parens
	// to check for a subquery keyword before attempting speculation, avoiding
	// ring buffer overflow in the lexer bridge (maxLookahead = 8).
	if tp == '(' && p.peeksThroughParensToSubquery() {
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
	if p.peek().Tp != 57586 {
		node.Value = p.parseExpression(precNone)
	}

	// WHEN clauses.
	for {
		if _, ok := p.accept(57586); !ok {
			break
		}
		when := Alloc[ast.WhenClause](p.arena)
		when.Expr = p.parseExpression(precNone)
		p.expect(57559)
		when.Result = p.parseExpression(precNone)
		node.WhenClauses = append(node.WhenClauses, when)
	}

	// Optional ELSE.
	if _, ok := p.accept(57417); ok {
		node.ElseClause = p.parseExpression(precNone)
	}

	p.expect(57701)
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
	if p.peek().Tp == 57540 || p.peek().Tp == 57590 {
		query := p.parseSelectStmt()
		if query == nil {
			return nil
		}
		// Handle UNION/EXCEPT/INTERSECT after the initial SELECT.
		result := p.maybeParseUnion(query)
		sub := Alloc[ast.SubqueryExpr](p.arena)
		sub.Query = result
		node.Sel = sub
	} else if p.peek().Tp == 57556 || p.peek().Tp == 57580 {
		var query ast.StmtNode
		if p.peek().Tp == 57556 {
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
