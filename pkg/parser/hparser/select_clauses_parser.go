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
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseGroupByClause parses GROUP BY expr_list [WITH ROLLUP].
func (p *HandParser) parseGroupByClause() *ast.GroupByClause {
	p.expect(tokGroup)
	p.expect(tokBy)

	gb := Alloc[ast.GroupByClause](p.arena)
	gb.Items = p.parseByItems()

	// Optional WITH ROLLUP
	if p.peek().Tp == tokWith {
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
	p.expect(tokOrder)
	p.expect(tokBy)

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
		if _, ok := p.accept(tokDesc); ok {
			item.Desc = true
		} else if _, ok := p.accept(tokAsc); ok {
			// explicit ASC: NullOrder stays false
		} else {
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
// or OFFSET offset {ROW|ROWS} FETCH {FIRST|NEXT} [count] {ROW|ROWS} {ONLY|WITH TIES}
// or FETCH {FIRST|NEXT} [count] {ROW|ROWS} {ONLY|WITH TIES}
func (p *HandParser) parseLimitClause() *ast.Limit {
	limit := Alloc[ast.Limit](p.arena)

	// Handle OFFSET first if present (Standard SQL)
	if _, ok := p.accept(tokOffset); ok {
		limit.Offset = p.toUint64Value(p.parseExpression(precNone))
		// Optional ROW/ROWS
		if p.peek().Tp == tokRow || p.peek().Tp == tokRows {
			p.next()
		}
	}

	// Handle LIMIT or FETCH
	if _, ok := p.accept(tokLimit); ok {
		// LIMIT count [OFFSET offset] or LIMIT offset, count
		first := p.parseExpression(precNone)
		if _, ok := p.accept(','); ok {
			// LIMIT offset, count
			limit.Offset = p.toUint64Value(first)
			limit.Count = p.toUint64Value(p.parseExpression(precNone))
		} else if _, ok := p.accept(tokOffset); ok {
			// LIMIT count OFFSET offset
			limit.Count = p.toUint64Value(first)
			limit.Offset = p.toUint64Value(p.parseExpression(precNone))
		} else {
			// LIMIT count
			limit.Count = p.toUint64Value(first)
		}
	} else if _, ok := p.acceptKeyword(tokFetch, "FETCH"); ok {
		// FETCH {FIRST|NEXT} [count] {ROW|ROWS} {ONLY|WITH TIES}
		// FIRST / NEXT
		if _, ok := p.acceptKeyword(tokFirst, "FIRST"); !ok {
			if _, ok := p.acceptKeyword(tokNext, "NEXT"); !ok {
				p.error(p.peek().Offset, "expected FIRST or NEXT")
				return nil
			}
		}

		// Count is optional — check if next is ROW/ROWS (meaning no count specified)
		isRowOrRows := p.peekKeyword(tokRow, "ROW") || p.peekKeyword(tokRows, "ROWS")

		if !isRowOrRows {
			limit.Count = p.toUint64Value(p.parseExpression(precNone))
		} else {
			// Implicit count 1.
			val := ast.NewValueExpr(uint64(1), "", "")
			limit.Count = val
		}

		// ROW/ROWS
		if _, ok := p.acceptKeyword(tokRow, "ROW"); !ok {
			if _, ok := p.acceptKeyword(tokRows, "ROWS"); !ok {
				p.error(p.peek().Offset, "expected ROW or ROWS")
				return nil
			}
		}

		// ONLY or WITH TIES
		if _, ok := p.acceptKeyword(tokOnly, "ONLY"); ok {
			// done
		} else if _, ok := p.acceptKeyword(tokWith, "WITH"); ok {
			// Expect TIES
			if ident, ok := p.expect(tokIdentifier); !ok || !ident.IsKeyword("TIES") {
				p.error(p.peek().Offset, "expected TIES after WITH")
				return nil
			}
			// ast.Limit doesn't support WithTies. Ignore.
		} else {
			p.error(p.peek().Offset, "expected ONLY or WITH TIES")
			return nil
		}
	} else if limit.Offset != nil {
		// Just OFFSET without LIMIT/FETCH? Valid standard SQL?
		// MySQL doesn't support it without LIMIT/FETCH usually, but maybe TiDB does?
		// Returning limit with just Offset.
	} else {
		// Should have matched one of them if called.
		p.error(p.peek().Offset, "expected LIMIT, OFFSET or FETCH")
		return nil
	}

	return limit
}

// parseSelectLock parses FOR UPDATE [NOWAIT|WAIT N|SKIP LOCKED],
// FOR SHARE [NOWAIT|SKIP LOCKED], or LOCK IN SHARE MODE.
func (p *HandParser) parseSelectLock() *ast.SelectLockInfo {
	lock := Alloc[ast.SelectLockInfo](p.arena)

	if _, ok := p.accept(tokLock); ok {
		// LOCK IN SHARE MODE
		p.expect(tokIn)
		p.expect(tokShare)
		p.accept(tokMode) // MODE is optional in some dialects
		lock.LockType = ast.SelectLockForShare
		return lock
	}

	p.expect(tokFor)

	switch p.peek().Tp {
	case tokUpdate:
		p.next()
		lock.LockType = ast.SelectLockForUpdate
		p.parseLockTablesAndModifiers(lock, ast.SelectLockForUpdateNoWait, ast.SelectLockForUpdateSkipLocked, ast.SelectLockForUpdateWaitN)
	case tokShare:
		p.next()
		lock.LockType = ast.SelectLockForShare
		p.parseLockTablesAndModifiers(lock, ast.SelectLockForShareNoWait, ast.SelectLockForShareSkipLocked, 0)
	default:
		p.error(p.peek().Offset, "expected UPDATE or SHARE after FOR")
	}

	return lock
}

// parseLockTablesAndModifiers parses: [OF tbl_name [, ...]] [NOWAIT|SKIP LOCKED|WAIT N]
// waitNType of 0 disables WAIT N support (only FOR UPDATE supports it).
func (p *HandParser) parseLockTablesAndModifiers(lock *ast.SelectLockInfo, nowaitType, skipLockedType ast.SelectLockType, waitNType ast.SelectLockType) {
	// Optional: OF tbl_name [, tbl_name ...]
	if _, ok := p.accept(tokOf); ok {
		for {
			lock.Tables = append(lock.Tables, p.parseTableName())
			if _, ok := p.accept(','); !ok {
				break
			}
		}
	}
	// Optional modifiers
	if _, ok := p.accept(tokNowait); ok {
		lock.LockType = nowaitType
	} else if _, ok := p.accept(tokSkip); ok {
		p.expect(tokLocked)
		lock.LockType = skipLockedType
	} else if waitNType != 0 {
		if _, ok := p.accept(tokWait); ok {
			lock.LockType = waitNType
			lock.WaitSec = p.parseUint64()
		}
	}
}

// parseSelectIntoOption parses INTO OUTFILE 'file' [FIELDS ...] [LINES ...]
func (p *HandParser) parseSelectIntoOption() *ast.SelectIntoOption {
	p.expect(tokInto)
	p.expect(tokOutfile)

	opt := &ast.SelectIntoOption{Tp: ast.SelectIntoOutfile}
	if tok, ok := p.expect(tokStringLit); ok {
		opt.FileName = tok.Lit
	}

	// Optional FIELDS/COLUMNS clause
	if tp := p.peek().Tp; tp == tokFields || tp == tokColumns {
		p.next()
		opt.FieldsInfo = p.parseFieldsClause(false)
	}

	// Optional LINES clause
	if _, ok := p.accept(tokLines); ok {
		opt.LinesInfo = p.parseLinesClause()
	}

	return opt
}

// isJoinKeyword returns true if the token type is a keyword that starts a JOIN.
func (p *HandParser) isJoinKeyword(tp int) bool {
	switch tp {
	case tokJoin, tokInner, tokCross, tokLeft, tokRight, tokNatural, tokStraightJoin:
		return true
	}
	return false
}

// isSimpleJoinKeyword returns true if the token type starts a join that doesn't
// require an ON clause (inner/cross join). Used by nested join recursion to
// avoid recursing into LEFT/RIGHT/NATURAL joins which need their own ON.
func (p *HandParser) isSimpleJoinKeyword(tp int) bool {
	switch tp {
	case tokJoin, tokInner, tokCross, tokStraightJoin:
		return true
	}
	return false
}

// CanBeImplicitAlias returns true if a token can serve as an implicit table/column alias.
// In MySQL/TiDB, most keywords can be used as unquoted identifiers. Only truly reserved
// SQL keywords (SELECT, FROM, WHERE, ON, etc.) and structural tokens are excluded.
func (p *HandParser) CanBeImplicitAlias(tok Token) bool {
	// Plain identifiers and string literals always work.
	if tok.Tp == tokIdentifier || tok.Tp == tokStringLit {
		if tok.IsKeyword("FETCH") {
			return false // FETCH is reserved for Limit clause
		}
		// WINDOW is an unreserved keyword and valid as alias (e.g., `select 1 WINDOW`).
		// But when followed by `identifier AS`, it introduces a WINDOW clause.
		if tok.IsKeyword("WINDOW") && p.peekN(1).Tp == tokIdentifier && p.peekN(2).Tp == tokAs {
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
	// Exclude reserved SQL keywords that cannot be aliases.
	switch tok.Tp {
	case tokSelect, tokFrom, tokWhere, tokGroup, tokOrder, tokLimit,
		tokHaving, tokSet, tokUpdate, tokDelete, tokInsert, tokInto,
		tokValues, tokOn, tokUsing, tokAs, tokIf, tokExists,
		tokJoin, tokInner, tokCross, tokLeft, tokRight, tokNatural, tokStraightJoin,
		tokUnion, tokExcept, tokIntersect,
		tokUse, tokIgnore, tokForce, tokFetch, tokOffset,
		tokFor, tokLock, tokIn, tokNot, tokAnd, tokOr, tokIs, tokNull,
		tokTrue, tokFalse, tokLike, tokBetween, tokCase, tokWhen, tokThen, tokElse, tokEnd,
		tokCreate, tokAlter, tokDrop, tokTable, tokIndex, tokColumn,
		tokPrimary, tokKey, tokUnique, tokForeign, tokCheck, tokConstraint,
		tokDefault, tokAll, tokDistinct,
		tokPartition, tokWith, tokWindow, tokOver, tokGroups,
		tokRow, tokFunction, tokOf, tokTableSample,
		// Window function names are reserved and cannot be aliases.
		tokCumeDist, tokDenseRank, tokFirstValue, tokLag, tokLastValue,
		tokLead, tokNthValue, tokNtile, tokPercentRank, tokRank, tokRowNumber,
		tokIntLit, tokFloatLit, tokDecLit, tokHexLit, tokBitLit:
		return false
	}
	// Any other keyword token with a literal can be used as an alias.
	return true
}

// toUint64Value converts a ValueExpr containing an int64 to uint64 if possible.
// This is required to match the LengthNum behavior for LIMIT/OFFSET.
func (p *HandParser) toUint64Value(expr ast.ExprNode) ast.ExprNode {
	if expr == nil {
		return nil
	}
	if ve, ok := expr.(ast.ValueExpr); ok {
		if val, ok := ve.GetValue().(int64); ok && val >= 0 {
			return ast.NewValueExpr(uint64(val), p.charset, p.collation)
		}
	}
	return expr
}

// maybeParseUnion is the entry point for parsing set operations (UNION/EXCEPT/INTERSECT).
// It starts with an already-parsed left-hand side (first) and parses the rest of the chain
// using precedence climbing to build a correct binary tree (ast.SetOprStmt).
func (p *HandParser) maybeParseUnion(first ast.ResultSetNode) ast.ResultSetNode {
	res := p.parseSetOprRest(first, 0)

	// Parse optional ORDER BY and LIMIT applying to the result of the set operation (or the single statement).
	// This covers cases like `(SELECT ...) UNION (SELECT ...) ORDER BY ... LIMIT ...`
	// or `(SELECT ...) LIMIT ...` if `first` was a parenthesized subquery.

	hasOuterOrderBy := p.peek().Tp == tokOrder
	pt := p.peek().Tp
	hasOuterLimit := pt == tokLimit || pt == tokOffset || pt == tokFetch

	// Check if res already has ORDER BY/LIMIT (from inner parenthesized context).
	// If so, we need to wrap in a new SetOprStmt before attaching outer ORDER BY/LIMIT.
	if hasOuterOrderBy || hasOuterLimit {
		needsWrap := false
		if s, ok := res.(*ast.SetOprStmt); ok && s.IsInBraces {
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
	if p.peek().Tp == tokOrder {
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
	if pt == tokLimit || pt == tokOffset || pt == tokFetch {
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
		if _, ok := p.accept(tokAll); ok {
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
			p.accept(tokDistinct)
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
	case tokSelect:
		res = p.parseSelectStmt()

	case tokTable, tokValues, '(':
		// parseSubquery handles TABLE, VALUES, and parenthesized subqueries.
		// For parenthesized subqueries, it returns *ast.SubqueryExpr.
		res = p.parseSubquery()

	// NOTE: case tokWith is intentionally NOT handled here.
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
	case tokUnion:
		v := ast.Union
		return &v
	case tokIntersect:
		v := ast.Intersect
		return &v
	case tokExcept:
		v := ast.Except
		return &v
	}
	return nil
}
