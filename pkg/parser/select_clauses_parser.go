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
	"math"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

// parseGroupByClause parses GROUP BY expr_list [WITH ROLLUP].
func (p *HandParser) parseGroupByClause() *ast.GroupByClause {
	p.expect(group)
	p.expect(by)

	gb := Alloc[ast.GroupByClause](p.arena)
	gb.Items = p.parseByItems()

	// Optional WITH ROLLUP
	if p.peek().Tp == with {
		// Speculate: WITH ROLLUP vs WITH CTE
		saved := p.mark()
		p.next() // consume WITH
		if p.peek().IsKeyword("ROLLUP") {
			p.next() // consume ROLLUP
			gb.Rollup = true
		} else {
			// Not ROLLUP — restore and let caller handle WITH
			p.restore(saved)
		}
	}
	return gb
}

// parseOrderByClause parses ORDER BY order_item_list.
func (p *HandParser) parseOrderByClause() *ast.OrderByClause {
	p.expect(order)
	p.expect(by)

	ob := Alloc[ast.OrderByClause](p.arena)
	ob.Items = p.parseByItems()
	return ob
}

// parseByItems parses a comma-separated list of order/group items.
func (p *HandParser) parseByItems() []*ast.ByItem {
	var items []*ast.ByItem
	for {
		item := Alloc[ast.ByItem](p.arena)
		item.Expr = p.parseExpression(precNone)
		if item.Expr == nil {
			return nil
		}

		// Convert integer literal to PositionExpr for positional references
		// in GROUP BY / ORDER BY (e.g., "GROUP BY 1", "ORDER BY 0"). The
		// planner relies on PositionExpr to resolve these to SELECT field
		// positions. Matches expected behavior: any int64 becomes PositionExpr.
		// Only set N (not P — P is reserved for prepared-statement param markers).
		if ve, ok := item.Expr.(ast.ValueExpr); ok {
			if n, ok2 := ve.GetValue().(int64); ok2 {
				pe := Alloc[ast.PositionExpr](p.arena)
				pe.N = int(n)
				item.Expr = pe
			}
		}

		// Optional ASC/DESC. Per grammar, items without explicit
		// ASC/DESC get NullOrder=true (for both GROUP BY and ORDER BY).
		if _, ok := p.accept(desc); ok {
			item.Desc = true
		} else if _, ok := p.accept(asc); !ok {
			item.NullOrder = true
		}

		items = append(items, item)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return items
}

// parseLimitClause parses LIMIT [offset,] count or LIMIT count OFFSET offset
// or FETCH {FIRST|NEXT} [count] {ROW|ROWS} ONLY
// yacc: no standalone OFFSET before FETCH, no WITH TIES
func (p *HandParser) parseLimitClause() *ast.Limit {
	limitNode := Alloc[ast.Limit](p.arena)

	// Handle LIMIT or FETCH
	if _, ok := p.accept(limit); ok {
		// LIMIT count [OFFSET offset] or LIMIT offset, count
		firstTok := p.peek()
		first := p.parseExpression(precNone)
		if _, ok := p.accept(','); ok {
			// LIMIT offset, count
			limitNode.Offset = p.toUint64Value(first, firstTok)
			exprTok := p.peek()
			limitNode.Count = p.toUint64Value(p.parseExpression(precNone), exprTok)
		} else if _, ok := p.accept(offset); ok {
			// LIMIT count OFFSET offset
			limitNode.Count = p.toUint64Value(first, firstTok)
			exprTok := p.peek()
			limitNode.Offset = p.toUint64Value(p.parseExpression(precNone), exprTok)
		} else {
			// LIMIT count
			limitNode.Count = p.toUint64Value(first, firstTok)
		}
	} else if _, ok := p.acceptKeyword(fetch, "FETCH"); ok {
		// FETCH {FIRST|NEXT} [count] {ROW|ROWS} {ONLY|WITH TIES}
		// FIRST / NEXT
		if _, ok := p.acceptKeyword(first, "FIRST"); !ok {
			if _, ok := p.acceptKeyword(next, "NEXT"); !ok {
				p.error(p.peek().Offset, "expected FIRST or NEXT")
				return nil
			}
		}

		// Count is optional — check if next is ROW/ROWS (meaning no count specified)
		isRowOrRows := p.peekKeyword(row, "ROW") || p.peekKeyword(rows, "ROWS")

		if !isRowOrRows {
			exprTok := p.peek()
			limitNode.Count = p.toUint64Value(p.parseExpression(precNone), exprTok)
		} else {
			// Implicit count 1.
			val := ast.NewValueExpr(uint64(1), "", "")
			limitNode.Count = val
		}

		// ROW/ROWS
		if _, ok := p.acceptKeyword(row, "ROW"); !ok {
			if _, ok := p.acceptKeyword(rows, "ROWS"); !ok {
				p.syntaxErrorAt(p.peek())
				return nil
			}
		}

		// yacc: ONLY (no WITH TIES in TiDB grammar)
		if _, ok := p.acceptKeyword(only, "ONLY"); !ok {
			p.error(p.peek().Offset, "expected ONLY")
			return nil
		}
	}

	return limitNode
}

// parseLimitClauseSimple parses LIMIT count only (no offset, no FETCH).
// This matches yacc's LimitClause used by DELETE and UPDATE, which only accepts
// "LIMIT" LimitOption (where LimitOption = LengthNum | paramMarker).
func (p *HandParser) parseLimitClauseSimple() *ast.Limit {
	limitNode := Alloc[ast.Limit](p.arena)
	p.expect(limit)
	exprTok := p.peek()
	limitNode.Count = p.toUint64Value(p.parseExpression(precNone), exprTok)
	return limitNode
}

// parseSelectLock parses FOR UPDATE [NOWAIT|WAIT N|SKIP LOCKED],
// FOR SHARE [NOWAIT|SKIP LOCKED], or LOCK IN SHARE MODE.
func (p *HandParser) parseSelectLock() *ast.SelectLockInfo {
	lockNode := Alloc[ast.SelectLockInfo](p.arena)

	if _, ok := p.accept(lock); ok {
		// LOCK IN SHARE MODE
		p.expect(in)
		p.expect(share)
		p.expect(mode) // MODE is required (matching yacc)
		lockNode.LockType = ast.SelectLockForShare
		return lockNode
	}

	p.expect(forKwd)

	switch p.peek().Tp {
	case update:
		p.next()
		lockNode.LockType = ast.SelectLockForUpdate
		p.parseLockTablesAndModifiers(lockNode, ast.SelectLockForUpdateNoWait,
			ast.SelectLockForUpdateSkipLocked, ast.SelectLockForUpdateWaitN)
	case share:
		p.next()
		lockNode.LockType = ast.SelectLockForShare
		p.parseLockTablesAndModifiers(lockNode, ast.SelectLockForShareNoWait, ast.SelectLockForShareSkipLocked, 0)
	default:
		p.syntaxErrorAt(p.peek())
	}

	return lockNode
}

// parseLockTablesAndModifiers parses: [OF tbl_name [, ...]] [NOWAIT|SKIP LOCKED|WAIT N]
// waitNType of 0 disables WAIT N support (only FOR UPDATE supports it).
func (p *HandParser) parseLockTablesAndModifiers(
	lockNode *ast.SelectLockInfo, nowaitType, skipLockedType ast.SelectLockType, waitNType ast.SelectLockType,
) {
	// Optional: OF tbl_name [, tbl_name ...]
	if _, ok := p.accept(of); ok {
		for {
			tn := p.parseTableName()
			if tn == nil {
				return
			}
			lockNode.Tables = append(lockNode.Tables, tn)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}
	// Optional modifiers
	if _, ok := p.accept(nowait); ok {
		lockNode.LockType = nowaitType
	} else if _, ok := p.accept(skip); ok {
		p.expect(locked)
		lockNode.LockType = skipLockedType
	} else if waitNType != 0 {
		if _, ok := p.accept(wait); ok {
			lockNode.LockType = waitNType
			lockNode.WaitSec = p.parseUint64()
		}
	}
}

// parseSelectIntoOption parses INTO OUTFILE 'file' [FIELDS ...] [LINES ...]
func (p *HandParser) parseSelectIntoOption() *ast.SelectIntoOption {
	p.expect(into)
	p.expect(outfile)

	opt := &ast.SelectIntoOption{Tp: ast.SelectIntoOutfile}
	if tok, ok := p.expect(stringLit); ok {
		opt.FileName = tok.Lit
	}

	// Optional FIELDS/COLUMNS clause
	if tp := p.peek().Tp; tp == fields || tp == columns {
		p.next()
		opt.FieldsInfo = p.parseFieldsClause(false)
	}

	// Optional LINES clause
	if _, ok := p.accept(lines); ok {
		opt.LinesInfo = p.parseLinesClause()
	}

	return opt
}

// CanBeImplicitAlias returns true if a token can serve as an implicit table/column alias.
// In MySQL/TiDB, most keywords can be used as unquoted identifiers. Only truly reserved
// SQL keywords (SELECT, FROM, WHERE, ON, etc.) and structural tokens are excluded.
func (p *HandParser) CanBeImplicitAlias(tok Token) bool {
	// Plain identifiers and string literals always work.
	if tok.Tp == identifier || tok.Tp == stringLit {
		if tok.IsKeyword("FETCH") {
			return false // FETCH is reserved for Limit clause
		}
		// WINDOW is an unreserved keyword and valid as alias (e.g., `select 1 WINDOW`).
		// But when followed by `identifier AS`, it introduces a WINDOW clause.
		if tok.IsKeyword("WINDOW") && p.peekN(1).Tp == identifier && p.peekN(2).Tp == as {
			return false
		}
		return true
	}
	// Exclude ASCII single-char tokens (parens, commas, operators, etc.)
	if tok.Tp < 256 {
		return false
	}
	// Exclude empty-literal tokens (EOF, etc.)
	if tok.Lit == "" {
		return false
	}
	// Exclude reserved SQL keywords and structural tokens that cannot be aliases.
	switch tok.Tp {
	case selectKwd, from, where, group, order, limit,
		having, set, update, deleteKwd, insert, into,
		values, on, using, as, ifKwd, exists,
		join, inner, cross, left, right, natural, straightJoin,
		union, except, intersect,
		use, ignore, force, fetch,
		forKwd, lock, in, not, and, or, is, null,
		trueKwd, falseKwd, like, between, caseKwd, when, then, elseKwd,
		create, alter, drop, tableKwd, index, column,
		primary, key, unique, foreign, check, constraint,
		defaultKwd, all, distinct,
		partition, with, window, over, groups,
		row, of, tableSample,
		// Window function names are reserved and cannot be aliases.
		cumeDist, denseRank, firstValue, lag, lastValue,
		lead, nthValue, ntile, percentRank, rank, rowNumber,
		intLit, floatLit, decLit, hexLit, bitLit:
		return false
	}
	// Any other keyword token with a literal can be used as an alias.
	return true
}

// toUint64Value converts a ValueExpr to uint64 to match the yacc LengthNum behavior.
// - int64 values >= 0 are converted to uint64
// - uint64 values are kept as-is
// - decimal values (overflow from lexer for numbers > MaxUint64) produce a syntax error
//
// errTok is the token at the start of the expression, used for error positioning
// when the literal overflows uint64.
func (p *HandParser) toUint64Value(expr ast.ExprNode, errTok Token) ast.ExprNode {
	if expr == nil {
		return nil
	}
	// ParamMarkerExpr embeds ValueExpr, so check for it first to avoid
	// destroying parameter markers (e.g., LIMIT ? in prepared statements).
	if _, ok := expr.(ast.ParamMarkerExpr); ok {
		return expr
	}
	// Reject negative numbers (e.g., LIMIT -1). The yacc parser's LengthNum
	// rule only accepted unsigned integer literals, so unary minus applied to
	// a number is a syntax error in LIMIT/OFFSET context.
	if ue, ok := expr.(*ast.UnaryOperationExpr); ok && ue.Op == opcode.Minus {
		p.errorNear(errTok.EndOffset, errTok.Offset)
		return ast.NewValueExpr(uint64(0), p.charset, p.collation)
	}
	ve, ok := expr.(ast.ValueExpr)
	if !ok {
		// The yacc parser's LimitOption rule only accepts LengthNum (bare integer)
		// or paramMarker. Reject any other expression (e.g., 1+1, column refs).
		p.errorNear(errTok.EndOffset, errTok.Offset)
		return ast.NewValueExpr(uint64(0), p.charset, p.collation)
	}
	switch val := ve.GetValue().(type) {
	case int64:
		if val >= 0 {
			return ast.NewValueExpr(uint64(val), p.charset, p.collation)
		}
	case uint64:
		return ast.NewValueExpr(val, p.charset, p.collation)
	default:
		// Decimal overflow (number > MaxUint64): report a syntax error.
		// The yacc parser's LengthNum rule only accepted intLit tokens,
		// so numbers that overflow uint64 (tokenized as decLit) caused
		// a parse error. We replicate that behavior here.
		_ = val
		p.errorNear(errTok.EndOffset, errTok.Offset)
		return ast.NewValueExpr(uint64(math.MaxUint64), p.charset, p.collation)
	}
	return expr
}

// maybeParseUnion is the entry point for parsing set operations (UNION/EXCEPT/INTERSECT).
// It starts with an already-parsed left-hand side (first) and parses the rest of the chain
// using precedence climbing to build a correct binary tree (ast.SetOprStmt).
func (p *HandParser) maybeParseUnion(first ast.ResultSetNode) ast.ResultSetNode {
	if first == nil {
		return nil
	}
	res := p.parseSetOprRest(first, 0)

	// Outer ORDER BY / LIMIT should only be parsed when:
	// 1. A set operation was found (res != first), e.g. SELECT ... UNION SELECT ... ORDER BY ...
	// 2. The input was a parenthesized query, e.g. (SELECT ...) ORDER BY ...
	// For a plain SELECT that already parsed its own ORDER BY / LIMIT in parseSelectStmt,
	// we must NOT re-parse here (that would accept invalid SQL like "SELECT 1 ORDER BY a ORDER BY b").
	if res == first {
		isInBraces := false
		if s, ok := first.(*ast.SetOprStmt); ok && s.IsInBraces {
			isInBraces = true
		}
		if s, ok := first.(*ast.SelectStmt); ok && s.IsInBraces {
			isInBraces = true
		}
		if !isInBraces {
			return res
		}
	}

	hasOuterOrderBy := p.peek().Tp == order
	pt := p.peek().Tp
	hasOuterLimit := pt == limit || pt == offset || pt == fetch

	// Check if res already has ORDER BY/LIMIT (from inner parenthesized context).
	// If so, we need to wrap in a new SetOprStmt before attaching outer ORDER BY/LIMIT.
	if hasOuterOrderBy || hasOuterLimit {
		needsWrap := false
		if s, ok := res.(*ast.SetOprStmt); ok && s.IsInBraces {
			if s.OrderBy != nil || s.Limit != nil {
				needsWrap = true
			}
		}
		if s, ok := res.(*ast.SelectStmt); ok && s.IsInBraces {
			if s.OrderBy != nil || s.Limit != nil {
				needsWrap = true
			}
		}
		if needsWrap {
			// Wrap the existing result (which has inner ORDER BY/LIMIT) as a child
			// of a new SetOprStmt. The outer ORDER BY/LIMIT will be set on the wrapper.
			wrapper := Alloc[ast.SetOprStmt](p.arena)
			wrapper.SelectList = &ast.SetOprSelectList{Selects: []ast.Node{res}}
			res = wrapper
		}
	}

	// Try parsing ORDER BY
	if p.peek().Tp == order {
		orderBy := p.parseOrderByClause()
		switch r := res.(type) {
		case *ast.SelectStmt:
			r.OrderBy = orderBy
		case *ast.SetOprStmt:
			r.OrderBy = orderBy
		}
	}

	// Try parsing LIMIT / OFFSET / FETCH
	// parseLimitClause handles LIMIT, OFFSET, and FETCH keywords internally.
	pt = p.peek().Tp
	if pt == limit || pt == offset || pt == fetch {
		limit := p.parseLimitClause()
		switch r := res.(type) {
		case *ast.SelectStmt:
			r.Limit = limit
		case *ast.SetOprStmt:
			r.Limit = limit
		}
	}

	return res
}

// parseSetOprRest continues parsing set operators with precedence >= minPrec.
// Precedence: UNION/EXCEPT = 1, INTERSECT = 2.
func (p *HandParser) parseSetOprRest(lhs ast.ResultSetNode, minPrec int) ast.ResultSetNode {
	for {
		opType := p.peekSetOprType()
		if opType == nil {
			break
		}

		// Determine precedence
		prec := 1
		if *opType == ast.Intersect {
			prec = 2
		}

		if prec < minPrec {
			break
		}

		p.next() // consume operator

		// Handle ALL/DISTINCT
		if _, ok := p.accept(all); ok {
			switch *opType {
			case ast.Union:
				v := ast.UnionAll
				opType = &v
			case ast.Except:
				v := ast.ExceptAll
				opType = &v
			case ast.Intersect:
				v := ast.IntersectAll
				opType = &v
			}
		} else {
			p.accept(distinct)
		}

		// Parse RHS unit
		rhs := p.parseSetOprUnit()
		if rhs == nil {
			// Parse error already recorded by p.error(); return what we have
			// so callers don't crash on nil (e.g. type assertion in maybeParseUnion).
			return lhs
		}

		// If next op has higher precedence, recurse for RHS
		nextOp := p.peekSetOprType()
		if nextOp != nil {
			nextPrec := 1
			if *nextOp == ast.Intersect {
				nextPrec = 2
			}
			if nextPrec > prec {
				rhs = p.parseSetOprRest(rhs, prec+1)
			}
		}

		// Flatten set operations to produce a parser-compatible flat SelectList.
		// The parser always produces SetOprStmt{SelectList: {Selects: [sel1, sel2, sel3, ...]}}
		// — a single flat list. Never nested SetOprStmt inside Selects.
		//
		// When lhs is already a SetOprStmt (from a previous loop iteration),
		// we reuse it and append rhs. Otherwise, create a new SetOprStmt.

		// Prepare rhs nodes for insertion into the flat list.
		// If rhs is a SetOprStmt (from higher-precedence recursion, e.g.,
		// INTERSECT grouping), we flatten its children into the parent's Selects.
		var rhsNodes []ast.Node
		if rhsSet, ok := rhs.(*ast.SetOprStmt); ok && !rhsSet.IsInBraces {
			// Flatten: set AfterSetOperator on the first child of the RHS SetOprStmt
			if len(rhsSet.SelectList.Selects) > 0 {
				if first, ok2 := rhsSet.SelectList.Selects[0].(*ast.SelectStmt); ok2 {
					first.AfterSetOperator = opType
				} else if first, ok2 := rhsSet.SelectList.Selects[0].(*ast.SetOprSelectList); ok2 {
					first.AfterSetOperator = opType
				}
			}
			rhsNodes = rhsSet.SelectList.Selects
			// Inherit ORDER BY / LIMIT from the flattened RHS SetOprStmt
			// These will be transferred to the outermost stmt below.
			rhs = rhsSet // keep reference for ORDER BY / LIMIT extraction
		} else {
			// Set AfterSetOperator on RHS to bind it to the operator.
			// If RHS is a SetOprStmt (parenthesized subquery), wrap it in a
			// SetOprSelectList. The planner only handles *ast.SelectStmt and
			// *ast.SetOprSelectList children in SetOprSelectList.Selects, not
			// bare *ast.SetOprStmt. This matches the SubSelect rule where
			// "(SelectStmtWithClause)" becomes SetOprSelectList{Selects:[...]}.
			//
			// Crucially, we flatten the inner SetOprStmt: instead of nesting the
			// SetOprStmt itself as a child, we take its SelectList.Selects and
			// put them directly into the wrapper. This produces the flat structure
			// that the planner expects.
			if s, ok := rhs.(*ast.SelectStmt); ok {
				s.AfterSetOperator = opType
				rhsNodes = []ast.Node{rhs}
			} else if s, ok := rhs.(*ast.SetOprStmt); ok {
				// Flatten: take the inner SelectList's Selects and wrap them
				// in a new SetOprSelectList with the AfterSetOperator from
				// the outer operator, plus any ORDER BY/LIMIT from the inner
				// SetOprStmt.
				wrapper := &ast.SetOprSelectList{
					Selects:          s.SelectList.Selects,
					AfterSetOperator: opType,
				}
				if s.OrderBy != nil {
					wrapper.OrderBy = s.OrderBy
				}
				if s.Limit != nil {
					wrapper.Limit = s.Limit
				}
				if s.With != nil {
					wrapper.With = s.With
				}
				rhsNodes = []ast.Node{wrapper}
			} else {
				rhsNodes = []ast.Node{rhs}
			}
		}

		// If LHS is a parenthesized CTE subquery (has With + IsInBraces), wrap it in a
		// SetOprSelectList. This matches the SetOprClause: SubSelect rule where
		// "(SelectStmtWithClause)" becomes SetOprSelectList{Selects:[SelectStmt]}.
		// The SetOprSelectList.Restore() method wraps the child in "()", producing
		// "(WITH ... SELECT ...)" instead of "WITH ... (SELECT ...)".
		var lhsNode ast.Node = lhs
		if s, ok := lhs.(*ast.SelectStmt); ok && s.IsInBraces && s.With != nil && s.WithBeforeBraces {
			wrapper := &ast.SetOprSelectList{Selects: []ast.Node{s}}
			// Keep s.IsInBraces=true — it represents the inner body braces (SELECT ...).
			// Only clear WithBeforeBraces and move With to the wrapper.
			s.WithBeforeBraces = false
			wrapper.With = s.With
			s.With = nil
			lhsNode = wrapper
		} else if s, ok := lhs.(*ast.SetOprStmt); ok && s.IsInBraces {
			// Parenthesized set operation as LHS (e.g., "(SELECT UNION ALL SELECT) INTERSECT ...").
			// Wrap in SetOprSelectList so the planner can handle it — it only handles
			// *ast.SelectStmt and *ast.SetOprSelectList children, not bare *ast.SetOprStmt.
			wrapper := &ast.SetOprSelectList{
				Selects: s.SelectList.Selects,
			}
			if s.OrderBy != nil {
				wrapper.OrderBy = s.OrderBy
			}
			if s.Limit != nil {
				wrapper.Limit = s.Limit
			}
			if s.With != nil {
				wrapper.With = s.With
			}
			lhsNode = wrapper
		}

		var stmt *ast.SetOprStmt
		if prevStmt, ok := lhs.(*ast.SetOprStmt); ok && !prevStmt.IsInBraces {
			// Flatten: reuse the existing SetOprStmt from prior iteration.
			// Append rhs to its flat Selects list.
			stmt = prevStmt
			stmt.SelectList.Selects = append(stmt.SelectList.Selects, rhsNodes...)
		} else {
			// First set operation or parenthesized LHS: create new SetOprStmt.
			stmt = Alloc[ast.SetOprStmt](p.arena)
			selects := make([]ast.Node, 0, 2+len(rhsNodes)-1)
			selects = append(selects, lhsNode)
			selects = append(selects, rhsNodes...)
			stmt.SelectList = &ast.SetOprSelectList{Selects: selects}
		}

		// Move ORDER BY / LIMIT from RHS to SetOprStmt if RHS is unparenthesized
		if sel, ok := rhs.(*ast.SelectStmt); ok && !sel.IsInBraces {
			if sel.OrderBy != nil {
				stmt.OrderBy = sel.OrderBy
				sel.OrderBy = nil
			}
			if sel.Limit != nil {
				stmt.Limit = sel.Limit
				sel.Limit = nil
			}
		} else if set, ok := rhs.(*ast.SetOprStmt); ok && !set.IsInBraces {
			// If RHS is a SetOprStmt (parsed by recursion), it might have accumulated ORDER BY
			if set.OrderBy != nil {
				stmt.OrderBy = set.OrderBy
				set.OrderBy = nil
			}
			if set.Limit != nil {
				stmt.Limit = set.Limit
				set.Limit = nil
			}
		}

		lhs = stmt
	}

	return lhs
}

// parseSetOprUnit parses the unit following a set operator: SELECT, TABLE, VALUES, WITH, or parens.
func (p *HandParser) parseSetOprUnit() ast.ResultSetNode {
	var res ast.ResultSetNode
	switch p.peek().Tp {
	case selectKwd:
		res = p.parseSelectStmt()

	case tableKwd, values, '(':
		// parseSubquery handles TABLE, VALUES, and parenthesized subqueries.
		// For parenthesized subqueries, it returns *ast.SubqueryExpr.
		res = p.parseSubquery()

	// NOTE: case with is intentionally NOT handled here.
	// The grammar's SimpleSelect rule does not allow WITH after
	// set operators (UNION/EXCEPT/INTERSECT). WITH (CTE) can only appear
	// at the top level of a statement, not inside a set operator chain.
	// `SELECT 1 UNION WITH cte AS (...) SELECT * FROM cte` is invalid.

	default:
		p.error(p.peek().Offset, "expected query expression after set operator")
		return nil
	}

	// Unwrap SubqueryExpr if it wraps a valid result set node (SelectStmt or SetOprStmt)
	// This is necessary because SetOprStmt logic requires children to have AfterSetOperator,
	// which is available on SelectStmt/SetOprStmt but not SubqueryExpr.
	if sub, ok := res.(*ast.SubqueryExpr); ok {
		if s, ok := sub.Query.(*ast.SelectStmt); ok {
			s.IsInBraces = true
			return s
		} else if s, ok := sub.Query.(*ast.SetOprStmt); ok {
			s.IsInBraces = true
			return s
		}
		// If SubqueryExpr wraps something else (e.g. TableSource?), keep it as is.
	}

	return res
}

// peekSetOprType checks if the peek token is a set operator (UNION/INTERSECT/EXCEPT)
// and returns the corresponding SetOprType. Returns nil if not a set operator.
func (p *HandParser) peekSetOprType() *ast.SetOprType {
	switch p.peek().Tp {
	case union:
		v := ast.Union
		return &v
	case intersect:
		v := ast.Intersect
		return &v
	case except:
		v := ast.Except
		return &v
	}
	return nil
}
