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
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// parseSelectStmt parses a SELECT statement.
// Grammar: SELECT [ALL|DISTINCT] field_list FROM table_refs
//
//	[WHERE expr] [GROUP BY expr_list] [HAVING expr]
//	[ORDER BY order_list] [LIMIT count [OFFSET offset]]
func (p *HandParser) parseSelectStmt() *ast.SelectStmt {
	stmt := p.arena.AllocSelectStmt()
	stmt.Kind = ast.SelectStmtKindSelect

	p.expect(selectKwd)

	// Parse SELECT options (ALL, DISTINCT, HIGH_PRIORITY, etc.)
	p.parseSelectOpts(stmt)

	// Parse field list (select expressions).
	stmt.Fields = p.parseFieldList()
	if stmt.Fields == nil {
		return nil
	}

	// Optional FROM clause.
	if _, ok := p.accept(from); ok {
		// FROM DUAL is special: parsed as no FROM clause (From stays nil).
		if p.peek().Tp == dual {
			p.next() // consume DUAL
			// Don't set stmt.From — match expected behavior.
		} else {
			stmt.From = p.parseTableRefs()
			if stmt.From == nil {
				return nil
			}
		}
	}

	// Optional WHERE clause.
	if _, ok := p.accept(where); ok {
		stmt.Where = p.parseExpression(precNone)
	}

	// Optional GROUP BY clause.
	if p.peek().Tp == group {
		stmt.GroupBy = p.parseGroupByClause()
	}

	// Optional HAVING clause.
	if _, ok := p.accept(having); ok {
		expr := p.parseExpression(precNone)
		if expr == nil {
			return nil
		}
		having := Alloc[ast.HavingClause](p.arena)
		having.Expr = expr
		stmt.Having = having
	}

	// Optional WINDOW clause (only when window functions are enabled).
	// The scanner may tokenize WINDOW as window or identifier.
	// Disambiguate: WINDOW clause is always `WINDOW name AS (...)`,
	// so check if the next token after WINDOW is an identifier followed by AS.
	if p.supportWindowFunc {
		isWindowClause := p.peek().Tp == window ||
			(p.peek().IsKeyword("WINDOW") && p.peekN(1).Tp == identifier && p.peekN(2).Tp == as)
		if isWindowClause {
			stmt.WindowSpecs = p.parseWindowClause()
		}
	}

	// Optional ORDER BY clause.
	if p.peek().Tp == order {
		stmt.OrderBy = p.parseOrderByClause()
	}

	// Optional LIMIT clause.
	isFetch := p.peekKeyword(fetch, "FETCH")
	if p.peek().Tp == limit || isFetch || p.peek().Tp == offset {
		stmt.Limit = p.parseLimitClause()
	}

	// INTO OUTFILE can appear before or after FOR UPDATE/SHARE.
	if p.peek().Tp == into {
		stmt.SelectIntoOpt = p.parseSelectIntoOption()
	}

	// Optional FOR UPDATE / FOR SHARE / LOCK IN SHARE MODE.
	if tp := p.peek().Tp; tp == forKwd || tp == lock {
		stmt.LockInfo = p.parseSelectLock()
	}

	// INTO OUTFILE can also appear after FOR UPDATE/SHARE.
	if stmt.SelectIntoOpt == nil && p.peek().Tp == into {
		stmt.SelectIntoOpt = p.parseSelectIntoOption()
	}

	return stmt
}

// parseSubquery parses a subquery which can be SELECT, TABLE, VALUES, or parenthesized query.
func (p *HandParser) parseSubquery() ast.ResultSetNode {
	var res ast.ResultSetNode

	switch p.peek().Tp {
	case selectKwd:
		stmt := p.parseSelectStmt()
		if stmt != nil {
			res = p.maybeParseUnion(stmt)
		}
	case tableKwd:
		stmt := p.parseTableStmt()
		if s, ok := stmt.(*ast.SelectStmt); ok {
			res = p.maybeParseUnion(s)
		}
	case values:
		stmt := p.parseValuesStmt()
		if s, ok := stmt.(*ast.SelectStmt); ok {
			res = p.maybeParseUnion(s)
		}
	case '(':
		p.next()
		innerStartOff := p.peek().Offset
		res = p.parseSubquery()
		if res != nil {
			// First set IsInBraces on the inner result (for single SELECT in parens)
			if s, ok := res.(*ast.SelectStmt); ok {
				s.IsInBraces = true
			} else if s, ok := res.(*ast.SetOprStmt); ok {
				s.IsInBraces = true
			}
			res = p.maybeParseUnion(res)
			// If maybeParseUnion created a new wrapper SetOprStmt, set IsInBraces on it too.
			// This covers cases like ((SELECT) EXCEPT SELECT) where the EXCEPT creates a
			// new SetOprStmt that needs IsInBraces=true for the outer parentheses.
			if s, ok := res.(*ast.SetOprStmt); ok {
				s.IsInBraces = true
			}
			// Set text on the inner result to match yacc SubSelect behavior.
			innerEndOff := p.peek().Offset // offset of ')'
			if innerEndOff > innerStartOff {
				res.(ast.Node).SetText(p.connectionEncoding, p.src[innerStartOff:innerEndOff])
			}
		}
		p.expect(')')
	case with:
		// WITH ... SELECT ...
		stmt := p.parseWithStmt()
		if rs, ok := stmt.(ast.ResultSetNode); ok {
			res = rs
		}
	}
	return res
}

// parseWithStmt parses WITH [RECURSIVE] cte_list statement.
func (p *HandParser) parseWithStmt() ast.StmtNode {
	p.expect(with)

	withInfo := Alloc[ast.WithClause](p.arena)
	if _, ok := p.accept(recursive); ok {
		withInfo.IsRecursive = true
	}

	for {
		cte := Alloc[ast.CommonTableExpression](p.arena)
		cte.IsRecursive = withInfo.IsRecursive
		// CTE name: accept any identifier-like token (including unreserved keywords
		// like COLUMNS, STATUS, etc.) to match MySQL behavior.
		nameTok := p.peek()
		if !isIdentLike(nameTok.Tp) || IsReserved(nameTok.Tp) {
			p.syntaxErrorAt(nameTok)
			return nil
		}
		p.next()
		cte.Name = ast.NewCIStr(nameTok.Lit)

		if p.peek().Tp == '(' {
			p.next()
			for {
				colTok := p.next()
				if !isIdentLike(colTok.Tp) || colTok.Tp == stringLit {
					p.syntaxErrorAt(colTok)
					return nil
				}
				cte.ColNameList = append(cte.ColNameList, ast.NewCIStr(colTok.Lit))
				if _, ok := p.accept(','); !ok {
					break
				}
			}
			p.expect(')')
		}

		p.expect(as)
		p.expect('(')
		subStartOff := p.peek().Offset

		// Parse subquery
		// Use parseSubquery to support SELECT/TABLE/VALUES/WITH/UNION/Parens
		subNode := p.parseSubquery()
		if subNode == nil {
			return nil
		}
		subNode = p.maybeParseUnion(subNode)
		// Set text on the inner statement to match yacc SubSelect behavior.
		subEndOff := p.peek().Offset
		if subEndOff > subStartOff {
			subNode.(ast.Node).SetText(p.connectionEncoding, p.src[subStartOff:subEndOff])
		}
		// CTE Definition is always a SubqueryExpr wrapping the ResultSetNode
		subExpr := p.arena.AllocSubqueryExpr()
		subExpr.Query = subNode
		cte.Query = subExpr

		p.expect(')')

		withInfo.CTEs = append(withInfo.CTEs, cte)

		if _, ok := p.accept(','); !ok {
			break
		}
	}

	// Parse main statement
	// Use parseStatement to support UPDATE/DELETE/SELECT, etc.
	// Reject double WITH: the yacc grammar does not allow WITH as the
	// inner statement of a WITH clause (e.g., WITH x AS (...) WITH y AS (...) SELECT ...).
	if p.peek().Tp == with {
		p.syntaxErrorAt(p.peek())
		return nil
	}
	stmt := p.parseStatement()
	if stmt == nil {
		return nil
	}

	// Attach WithClause
	// Originally, WITH is attached at the SimpleSelect level, meaning:
	// - For a plain SELECT: attach to the SelectStmt directly
	// - For SELECT ... UNION SELECT ...: attach to the FIRST child SelectStmt,
	//   NOT to the SetOprStmt wrapper
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		s.With = withInfo
		s.WithBeforeBraces = s.IsInBraces
	case *ast.SetOprStmt:
		s.With = withInfo
		s.WithBeforeBraces = s.IsInBraces
	case *ast.UpdateStmt:
		s.With = withInfo
	case *ast.DeleteStmt:
		s.With = withInfo
	default:
		p.error(p.peek().Offset, "WITH clause not supported for this statement type")
		return nil
	}

	return stmt
}

// parseSelectOpts parses SELECT modifiers: hints, ALL, DISTINCT, HIGH_PRIORITY,
// STRAIGHT_JOIN, SQL_CALC_FOUND_ROWS, etc. in any order.
func (p *HandParser) parseSelectOpts(stmt *ast.SelectStmt) {
	opts := Alloc[ast.SelectStmtOpts](p.arena)
	opts.SQLCache = true // Default is true; false emits SQL_NO_CACHE.
	stmt.SelectStmtOpts = opts

	// Optimizer hints (/*+ ... */) appear among select options.
	opts.TableHints = p.parseOptHints()
	if opts.TableHints != nil {
		stmt.TableHints = opts.TableHints
	}

	// Grammar: SELECT [ALL | DISTINCT | DISTINCTROW ] [HIGH_PRIORITY] ...
	// All modifiers can appear in any order.
	for {
		switch p.peek().Tp {
		case all:
			p.next()
			if opts.Distinct {
				p.errs = append(p.errs, ErrWrongUsage.GenWithStackByArgs("ALL", "DISTINCT"))
				return
			}
			opts.ExplicitAll = true
		case distinct, distinctRow:
			p.next()
			if opts.ExplicitAll {
				p.errs = append(p.errs, ErrWrongUsage.GenWithStackByArgs("ALL", "DISTINCT"))
				return
			}
			stmt.Distinct = true
			opts.Distinct = true
		case highPriority:
			p.next()
			opts.Priority = mysql.HighPriority
		case lowPriority:
			p.next()
			opts.Priority = mysql.LowPriority
		case delayed:
			p.next()
			opts.Priority = mysql.DelayedPriority
		case straightJoin:
			p.next()
			opts.StraightJoin = true
		case sqlCalcFoundRows:
			p.next()
			opts.CalcFoundRows = true
		case sqlCache:
			p.next()
			opts.SQLCache = true
		case sqlNoCache:
			p.next()
			opts.SQLCache = false
		case sqlSmallResult:
			p.next()
			opts.SQLSmallResult = true
		case sqlBigResult:
			p.next()
			opts.SQLBigResult = true
		case sqlBufferResult:
			p.next()
			opts.SQLBufferResult = true
		default:
			return
		}
	}
}

// parseWindowClause parses the WINDOW clause.
// Grammar: WINDOW window_name AS (window_spec) [, window_name AS (window_spec)] ...
func (p *HandParser) parseWindowClause() []ast.WindowSpec {
	p.next() // consume WINDOW
	var specs []ast.WindowSpec
	for {
		// Window name (identifier or unreserved keyword)
		nameTok := p.next()
		if !isIdentLike(nameTok.Tp) || nameTok.Tp == stringLit {
			p.syntaxErrorAt(nameTok)
			return nil
		}

		p.expect(as)

		// Parse the window specification: ( [ref] [PARTITION BY] [ORDER BY] [frame] )
		// We reuse parseWindowSpec from expr_parser.go which handles the parens and content.
		spec := p.parseWindowSpec()
		spec.Name = ast.NewCIStr(nameTok.Lit)

		specs = append(specs, spec)

		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return specs
}

// parseFieldList parses the select field list.
func (p *HandParser) parseFieldList() *ast.FieldList {
	fl := Alloc[ast.FieldList](p.arena)

	for {
		field := p.parseSelectField()
		if field == nil {
			// Found nothing where a field was expected.
			// This usually means we hit an unexpected token or EOF invalidly.
			p.syntaxErrorAt(p.peek())
			return nil
		}
		// Set field text from source SQL (matching the FieldList rule).
		if field.Expr != nil {
			endOffset := p.peek().Offset
			if endOffset > field.Offset && endOffset <= len(p.src) {
				field.SetText(p.connectionEncoding, strings.TrimSpace(p.src[field.Offset:endOffset]))
			}
		}
		fl.Fields = append(fl.Fields, field)
		if _, ok := p.accept(','); !ok {
			break
		}
	}

	return fl
}

// parseSelectField parses a single select field (expression, wildcard, or alias).
func (p *HandParser) parseSelectField() *ast.SelectField {
	sf := Alloc[ast.SelectField](p.arena)
	startOff := p.peek().Offset
	sf.Offset = startOff

	// Check for qualified wildcards: table.* or schema.table.*
	if p.peek().Tp == identifier {
		if p.lexer.PeekN(1).Tp == '.' {
			if p.lexer.PeekN(2).Tp == '*' {
				// table.*
				tableName := p.next()
				p.next() // .
				p.next() // *
				wf := Alloc[ast.WildCardField](p.arena)
				wf.Table = ast.NewCIStr(tableName.Lit)
				sf.WildCard = wf
				return sf
			} else if p.lexer.PeekN(2).Tp == identifier &&
				p.lexer.PeekN(3).Tp == '.' &&
				p.lexer.PeekN(4).Tp == '*' {
				// schema.table.*
				schema := p.next()
				p.next() // .
				table := p.next()
				p.next() // .
				p.next() // *
				wf := Alloc[ast.WildCardField](p.arena)
				wf.Schema = ast.NewCIStr(schema.Lit)
				wf.Table = ast.NewCIStr(table.Lit)
				sf.WildCard = wf
				return sf
			}
		}
	}

	// Check for bare * wildcard.
	if p.peek().Tp == '*' {
		p.next()
		wf := Alloc[ast.WildCardField](p.arena)
		sf.WildCard = wf
		return sf
	}

	// Parse expression.
	sf.Expr = p.parseExpression(precNone)
	if sf.Expr == nil {
		return nil
	}

	// Optional alias: [AS] identifier
	// Originally, only Identifier (non-reserved keywords) is valid after AS.
	if _, ok := p.accept(as); ok {
		aliasTok := p.peek()
		if aliasTok.Tp != identifier && aliasTok.Tp != stringLit && (aliasTok.Tp < identifier || IsReserved(aliasTok.Tp)) {
			p.syntaxErrorAt(aliasTok)
			return nil
		}
		p.next()
		sf.AsName = ast.NewCIStr(aliasTok.Lit)
	} else if p.CanBeImplicitAlias(p.peek()) {
		// Implicit alias (without AS).
		aliasTok := p.next()
		sf.AsName = ast.NewCIStr(aliasTok.Lit)
	}

	// Capture text from source for field text.
	endOff := p.peek().Offset
	if endOff > startOff && endOff <= len(p.src) {
		sf.SetText(p.connectionEncoding, strings.TrimSpace(p.src[startOff:endOff]))
	}

	return sf
}
