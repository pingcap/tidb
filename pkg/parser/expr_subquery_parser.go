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
	for i := range maxLookahead - 1 {
		tok := p.peekN(i)
		switch tok.Tp {
		case '(':
			continue
		case selectKwd, with, tableKwd, values:
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
	if tp == selectKwd || tp == with || tp == tableKwd || tp == values {
		startOffset := p.peek().Offset
		// Use parseSubquery to handle SELECT, TABLE, VALUES, WITH, and UNIONs.
		query := p.parseSubquery()
		if query == nil {
			return nil
		}
		endOffset := p.peek().Offset // ')' token
		// Set subquery text from the original SQL (matching the behavior).
		if endOffset > startOffset && endOffset <= len(p.src) {
			query.SetText(p.connectionEncoding, p.src[startOffset:endOffset])
		}
		p.expect(')')
		sub := p.arena.AllocSubqueryExpr()
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
				query.SetText(p.connectionEncoding, p.src[startOffset:endOffset])
			}
			p.expect(')')
			sub := p.arena.AllocSubqueryExpr()
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
	// After parseSubquery, there may be set operators (UNION/EXCEPT/INTERSECT)
	// at this level, e.g. EXISTS ( (SELECT ...) UNION (SELECT ...) ).
	query = p.maybeParseUnion(query)
	// Clear IsInBraces since EXISTS's own parens provide the wrapping.
	if s, ok := query.(*ast.SelectStmt); ok {
		s.IsInBraces = false
	} else if s, ok := query.(*ast.SetOprStmt); ok {
		s.IsInBraces = false
	}
	p.expect(')')

	sub := p.arena.AllocSubqueryExpr()
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
	if p.peek().Tp != when {
		node.Value = p.parseExpression(precNone)
	}

	// WHEN clauses.
	for {
		if _, ok := p.accept(when); !ok {
			break
		}
		when := Alloc[ast.WhenClause](p.arena)
		when.Expr = p.parseExpression(precNone)
		p.expect(then)
		when.Result = p.parseExpression(precNone)
		node.WhenClauses = append(node.WhenClauses, when)
	}

	// Optional ELSE.
	if _, ok := p.accept(elseKwd); ok {
		node.ElseClause = p.parseExpression(precNone)
	}

	p.expect(end)
	return node
}

// parseDefaultExpr parses DEFAULT(column).
// Bare DEFAULT (without parentheses) is only valid in INSERT VALUES and
// SET col = DEFAULT contexts, not as a general expression. Those contexts
// handle bare DEFAULT via parseExprOrDefault before calling parseExpression.
func (p *HandParser) parseDefaultExpr() ast.ExprNode {
	p.next() // consume DEFAULT
	node := p.arena.AllocDefaultExpr()

	if p.peek().Tp != '(' {
		// Bare DEFAULT is not a valid general expression (only valid in
		// VALUES and SET assignment contexts). Report error at the token
		// AFTER default (matching yacc, which has consumed DEFAULT before
		// detecting the error).
		nextTok := p.peek()
		p.errorNear(nextTok.EndOffset, nextTok.Offset)
		return nil
	}
	if _, ok := p.accept('('); ok {
		col := p.arena.AllocColumnName()
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
	if tp := p.peek().Tp; tp == selectKwd || tp == with || tp == tableKwd || tp == values {
		query := p.parseSubquery()
		if query == nil {
			return nil
		}
		sub := p.arena.AllocSubqueryExpr()
		sub.Query = query
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
