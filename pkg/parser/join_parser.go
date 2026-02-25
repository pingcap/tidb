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
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// parseTableRefs parses the FROM clause table references.
func (p *HandParser) parseTableRefs() *ast.TableRefsClause {
	res := p.parseCommaJoin()
	if res == nil {
		return nil
	}
	clause := Alloc[ast.TableRefsClause](p.arena)
	clause.TableRefs = res
	return clause
}

// parseCommaJoin parses table references separated by commas (lowest precedence).
// Expected structure for `t, v`: Join{ Left: Join{Left:t, Right:nil}, Right: v, Tp: CrossJoin }
func (p *HandParser) parseCommaJoin() *ast.Join {
	res := p.parseJoin()
	if res == nil {
		return nil
	}

	// Base case: return as a Join.
	var innerJoin *ast.Join
	if j, ok := res.(*ast.Join); ok {
		innerJoin = j
	} else {
		innerJoin = p.arena.AllocJoin()
		innerJoin.Left = res
	}

	for {
		if _, ok := p.accept(','); !ok {
			break
		}
		right := p.parseJoin()
		if right == nil {
			return nil
		}

		// Comma join ALWAYS nests the previous ref (even if it's already a Join).
		newJoin := p.arena.AllocJoin()
		newJoin.Left = innerJoin
		newJoin.Right = right
		newJoin.Tp = ast.CrossJoin
		innerJoin = newJoin
	}
	return innerJoin
}

// parseJoin parses table references with keyword joins (middle precedence).
//
// MySQL's grammar for joins has two forms:
//  1. table_ref JOIN table_factor           — left-associative cross join (no ON)
//  2. table_ref JOIN table_ref ON expr      — right-recursive (right side absorbs more joins)
//
// For example:
//   - "t1 JOIN t2 LEFT JOIN t3 ON c" → "(t1 JOIN t2) LEFT JOIN t3 ON c" (no ON for first JOIN)
//   - "A LEFT JOIN G INNER JOIN J ON c1 ON c2" → "A LEFT JOIN (G INNER JOIN J ON c1) ON c2"
//
// Implementation: iterate over sequential joins. For each:
//   - Recursively parse the right side (may absorb further joins + their ON clauses)
//   - If ON/USING follows, keep the right-leaning subtree
//   - If no ON/USING, apply makeCrossJoin rotation to produce left-leaning cross join
//   - Result becomes the new left-hand side for the next iteration
func (p *HandParser) parseJoin() ast.ResultSetNode {
	left := p.parseTableSource()
	if left == nil {
		return nil
	}

	lhs := left
	for {
		// Check if next token is a join keyword. If not, done.
		joinType, natural, straight, commaJoin := p.peekJoinType()
		if commaJoin || (joinType == 0 && !natural && !straight) {
			break
		}

		// Consume the join keyword(s).
		joinType, natural, straight, _ = p.parseJoinType()

		// Parse the right side.
		// NATURAL JOIN and STRAIGHT_JOIN never take ON/USING, so they complete
		// immediately at this level (left-associative per the %left join
		// keyword precedence — same precedence means reduce wins). Use
		// parseTableSource() for these to avoid absorbing subsequent joins.
		// For all other joins, use parseJoinRHS() which recursively absorbs
		// subsequent JOINs and their ON/USING clauses, building a right-leaning
		// subtree for stacked ON clauses.
		var rhs ast.ResultSetNode
		if natural || straight {
			rhs = p.parseTableSource()
		} else {
			rhs = p.parseJoinRHS()
		}
		if rhs == nil {
			return nil
		}

		// Check for ON/USING clause for THIS join level.
		if _, ok := p.accept(on); ok {
			if natural {
				p.error(p.peek().Offset, "natural join cannot have ON clause")
				return nil
			}
			join := p.arena.AllocJoin()
			join.Left = lhs
			join.Right = rhs
			join.Tp = joinType
			join.NaturalJoin = natural
			join.StraightJoin = straight
			on := Alloc[ast.OnCondition](p.arena)
			on.Expr = p.parseExpression(precNone)
			join.On = on
			lhs = join
		} else if _, ok := p.accept(using); ok {
			if natural {
				p.error(p.peek().Offset, "natural join cannot have USING clause")
				return nil
			}
			join := p.arena.AllocJoin()
			join.Left = lhs
			join.Right = rhs
			join.Tp = joinType
			join.NaturalJoin = natural
			join.StraightJoin = straight
			p.expect('(')
			join.Using = p.parseColumnNameList()
			p.expect(')')
			lhs = join
		} else if natural || straight {
			// NATURAL JOIN and STRAIGHT_JOIN don't take ON/USING.
			join := p.arena.AllocJoin()
			join.Left = lhs
			join.Right = rhs
			join.Tp = joinType
			join.NaturalJoin = natural
			join.StraightJoin = straight
			lhs = join
		} else {
			// No ON/USING. LEFT/RIGHT JOIN require ON or USING.
			if joinType == ast.LeftJoin || joinType == ast.RightJoin {
				p.error(p.peek().Offset, "Outer join requires ON/USING clause")
				return nil
			}
			// Pure cross join — apply tree rotation to match MySQL's
			// left-associative cross join semantics.
			lhs = p.makeCrossJoin(lhs, rhs)
		}
	}

	return lhs
}

// parseJoinRHS parses the right-hand side of a JOIN — recursively parsing
// subsequent JOINs (and their ON/USING clauses) to build a right-leaning
// subtree for stacked ON clauses. Stops when no more join keywords follow
// or when the calling level should consume its ON clause.
func (p *HandParser) parseJoinRHS() ast.ResultSetNode {
	left := p.parseTableSource()
	if left == nil {
		return nil
	}

	// If no join follows, return just the table source.
	joinType, natural, straight, commaJoin := p.peekJoinType()
	if commaJoin || (joinType == 0 && !natural && !straight) {
		return left
	}

	// Consume the join keyword.
	joinType, natural, straight, _ = p.parseJoinType()

	// Recurse for the right side.
	rhs := p.parseJoinRHS()
	if rhs == nil {
		return nil
	}

	join := p.arena.AllocJoin()
	join.Left = left
	join.Right = rhs
	join.Tp = joinType
	join.NaturalJoin = natural
	join.StraightJoin = straight

	// Consume ON/USING for THIS level.
	if _, ok := p.accept(on); ok {
		if natural {
			p.error(p.peek().Offset, "natural join cannot have ON clause")
			return nil
		}
		on := Alloc[ast.OnCondition](p.arena)
		on.Expr = p.parseExpression(precNone)
		join.On = on
	} else if _, ok := p.accept(using); ok {
		if natural {
			p.error(p.peek().Offset, "natural join cannot have USING clause")
			return nil
		}
		p.expect('(')
		join.Using = p.parseColumnNameList()
		p.expect(')')
	} else if !natural && !straight {
		// No ON/USING. LEFT/RIGHT JOIN require ON or USING.
		if joinType == ast.LeftJoin || joinType == ast.RightJoin {
			p.error(p.peek().Offset, "Outer join requires ON/USING clause")
			return nil
		}
		// Pure cross join — apply rotation.
		return p.makeCrossJoin(left, rhs)
	}

	return join
}

// peekJoinType checks if the next tokens form a join keyword without consuming them.
func (p *HandParser) peekJoinType() (joinType ast.JoinType, isNatural bool, straight bool, commaJoin bool) {
	tok := p.peek()
	switch tok.Tp {
	case join:
		return ast.CrossJoin, false, false, false
	case inner, cross:
		return ast.CrossJoin, false, false, false
	case left:
		return ast.LeftJoin, false, false, false
	case right:
		return ast.RightJoin, false, false, false
	case natural:
		return ast.CrossJoin, true, false, false
	case straightJoin:
		return ast.CrossJoin, false, true, false
	case ',':
		return ast.CrossJoin, false, false, true
	default:
		return 0, false, false, false
	}
}

// makeCrossJoin builds a cross join without `on` or `using` clause, using arena allocation.
// Logic ported from ast.NewCrossJoin to avoid heap allocation.
func (p *HandParser) makeCrossJoin(left, right ast.ResultSetNode) *ast.Join {
	rj, ok := right.(*ast.Join)
	// don't break the explicit parents name scope constraints.
	if !ok || rj.Right == nil || rj.ExplicitParens {
		join := p.arena.AllocJoin()
		join.Left = left
		join.Right = right
		join.Tp = ast.CrossJoin
		return join
	}

	var leftMostLeafFatherOfRight = rj
	// Walk down the right hand side.
	for {
		if leftMostLeafFatherOfRight.Tp == ast.RightJoin && leftMostLeafFatherOfRight.ExplicitParens {
			// Rewrite right join to left join.
			tmpChild := leftMostLeafFatherOfRight.Right
			leftMostLeafFatherOfRight.Right = leftMostLeafFatherOfRight.Left
			leftMostLeafFatherOfRight.Left = tmpChild
			leftMostLeafFatherOfRight.Tp = ast.LeftJoin
		}
		leftChild := leftMostLeafFatherOfRight.Left
		join, ok := leftChild.(*ast.Join)
		if !(ok && join.Right != nil) {
			break
		}
		leftMostLeafFatherOfRight = join
	}

	newCrossJoin := p.arena.AllocJoin()
	newCrossJoin.Left = left
	newCrossJoin.Right = leftMostLeafFatherOfRight.Left
	newCrossJoin.Tp = ast.CrossJoin

	leftMostLeafFatherOfRight.Left = newCrossJoin
	return rj
}

// parseJoinType returns the join type if the next tokens form a join keyword.
// commaJoin is true only when the join operator is a literal comma (',').
func (p *HandParser) parseJoinType() (joinType ast.JoinType, isNatural bool, straight bool, commaJoin bool) {
	tok := p.peek()

	switch tok.Tp {
	case join:
		p.next()
		return ast.CrossJoin, false, false, false

	case inner:
		p.next()
		p.expect(join)
		return ast.CrossJoin, false, false, false

	case cross:
		p.next()
		p.expect(join)
		return ast.CrossJoin, false, false, false

	case left:
		p.next()
		p.accept(outer) // optional OUTER
		p.expect(join)
		return ast.LeftJoin, false, false, false

	case right:
		p.next()
		p.accept(outer) // optional OUTER
		p.expect(join)
		return ast.RightJoin, false, false, false

	case natural:
		p.next()
		switch p.peek().Tp {
		case left:
			p.next()
			p.accept(outer)
			p.expect(join)
			return ast.LeftJoin, true, false, false
		case right:
			p.next()
			p.accept(outer)
			p.expect(join)
			return ast.RightJoin, true, false, false
		case join:
			p.next()
			return ast.CrossJoin, true, false, false
		default:
			p.syntaxErrorAt(p.peek())
			return 0, false, false, false
		}

	case straightJoin:
		p.next()
		return ast.CrossJoin, false, true, false

	case ',':
		// Don't p.next() here. Comma is handled by parseCommaJoin.
		return ast.CrossJoin, false, false, true

	default:
		return 0, false, false, false
	}
}

// parseTableSource parses a single table reference (table name, subquery, or join in parens).
func (p *HandParser) parseTableSource() ast.ResultSetNode {
	var res ast.ResultSetNode

	switch p.peek().Tp {
	case '{':
		// ODBC escaped table reference: { OJ table_ref }
		// The braces are stripped and the identifier after '{' must be "OJ".
		p.next() // consume '{'
		if !p.peek().IsKeyword("OJ") {
			p.syntaxErrorAt(p.peek())
			return nil
		}
		p.next() // consume OJ
		res = p.parseJoin()
		p.expect('}')
		// After { OJ ... }, handle alias if present
		if res != nil {
			return res
		}
		return nil
	case '(':
		p.next()
		if p.peek().Tp == selectKwd || p.peek().Tp == with || p.peek().Tp == tableKwd || p.peek().Tp == values {
			// Unambiguous derived table: subquery with optional UNION.
			inner := p.parseSubquery()
			if inner == nil {
				return nil
			}
			res = inner
			p.expect(')')
		} else if p.peek().Tp == '(' && p.peeksThroughParensToSubquery() {
			// Ambiguous: could be nested subquery ((select ...)) or parenthesized join ((t1 join t2)).
			// peeksThroughParensToSubquery confirmed a subquery keyword exists; try subquery first.
			saved := p.mark()
			savedErrs := len(p.errs)
			inner := p.parseSubquery()
			if inner != nil {
				// Consume UNION/EXCEPT/INTERSECT at this paren level.
				inner = p.maybeParseUnion(inner)
			}
			if inner != nil && p.peek().Tp == ')' {
				// Successfully parsed as subquery, and next is ')'. Use it.
				// Discard any errors from the successful speculative parse.
				p.errs = p.errs[:savedErrs]
				res = inner
				p.expect(')')
			} else {
				// Not a subquery (or leftover tokens before ')'), restore and parse as join.
				p.errs = p.errs[:savedErrs]
				p.restore(saved)
				join := p.parseCommaJoin()
				if join == nil {
					return nil
				}
				p.expect(')')
				join.ExplicitParens = true
				res = join
			}
		} else {
			// Parenthesized join.
			join := p.parseCommaJoin()
			if join == nil {
				return nil
			}
			p.expect(')')
			join.ExplicitParens = true
			res = join
		}

	case dual:
		p.next()
		// DUAL is a virtual table that returns one row. Treat as empty table name.
		tn := Alloc[ast.TableName](p.arena)
		tn.Name = ast.NewCIStr("DUAL")
		res = tn

	default:
		// Table name, possibly qualified: [schema.]table
		tn := p.parseTableName()
		if tn == nil {
			return nil
		}
		// Handle non-standard '.*' suffix (e.g. for DELETE syntax compatibility via Join TableSource).
		if p.peek().Tp == '.' && p.peekN(1).Tp == '*' {
			p.next() // .
			p.next() // *
			tn.HasWildcard = true
		}
		res = tn
	}

	// After the core node, handle optional alias and partitions.
	// Note: Partitions are only for table names, but MySQL grammar permits them in weird places.
	// For simplicity, we only attach them to TableSource if it wraps a TableName.

	// Check for PARTITION clause.
	if _, ok := p.accept(partition); ok {
		p.expect('(')
		var names []ast.CIStr
		for {
			ident, ok := p.expect(identifier)
			if !ok {
				return nil
			}
			names = append(names, ast.NewCIStr(ident.Lit))
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')

		// Attach to TableName if possible.
		if tn, ok := res.(*ast.TableName); ok {
			tn.PartitionNames = names
		}
	}

	// Parse optional index hints: USE/IGNORE/FORCE INDEX/KEY [FOR ...] (names)
	// MySQL grammar: table_name [AS alias] [index_hint_list]
	if tn, ok := res.(*ast.TableName); ok {
		p.parseIndexHintsInto(tn)
	}

	// Optional alias or AS OF TIMESTAMP clause.
	var asName ast.CIStr
	if p.peek().Tp == asof {
		// AS OF TIMESTAMP expr — stale read / time travel
		p.next() // consume AS OF (combined token)
		if p.peek().Tp != timestampType {
			p.syntaxErrorAt(p.peek())
			return nil
		}
		p.next() // consume TIMESTAMP
		asOfClause := Alloc[ast.AsOfClause](p.arena)
		asOfClause.TsExpr = p.parseExpression(precNone)
		if tn, ok := res.(*ast.TableName); ok {
			tn.AsOf = asOfClause
		}
	} else if _, ok := p.accept(as); ok {
		aliasTok := p.next()
		// Table alias cannot be a string literal (unlike column alias).
		if !isIdentLike(aliasTok.Tp) || aliasTok.Tp == stringLit {
			p.syntaxErrorAt(aliasTok)
			return nil
		}
		asName = ast.NewCIStr(aliasTok.Lit)
	} else if p.CanBeImplicitAlias(p.peek()) {
		// Implicit alias — can be an identifier or an unreserved keyword.
		aliasTok := p.next()
		asName = ast.NewCIStr(aliasTok.Lit)
	}

	// Parse index hints AFTER alias (some syntaxes put hints after alias).
	if tn, ok := res.(*ast.TableName); ok {
		p.parseIndexHintsInto(tn)
	}

	// Parse optional TABLESAMPLE clause (can appear after alias):
	// TABLESAMPLE [SYSTEM|BERNOULLI|REGION] (expr [PERCENT|ROWS]) [REPEATABLE(seed)]
	// Only valid for table name references, not derived tables/subqueries.
	_, isResTableName := res.(*ast.TableName)
	if p.peek().Tp == tableSample && isResTableName {
		p.next()
		ts := Alloc[ast.TableSample](p.arena)
		// Optional method: SYSTEM, BERNOULLI, REGION
		if p.peek().IsKeyword("SYSTEM") {
			p.next()
			ts.SampleMethod = ast.SampleMethodTypeSystem
		} else if p.peek().IsKeyword("BERNOULLI") {
			p.next()
			ts.SampleMethod = ast.SampleMethodTypeBernoulli
		} else if p.peek().Tp == region || p.peek().Tp == regions {
			p.next()
			ts.SampleMethod = ast.SampleMethodTypeTiDBRegion
		}
		p.expect('(')
		if p.peek().Tp != ')' {
			ts.Expr = p.parseExpression(precNone)
			// Optional unit: PERCENT or ROWS
			if p.peek().IsKeyword("PERCENT") {
				p.next()
				ts.SampleClauseUnit = ast.SampleClauseUnitTypePercent
			} else if p.peek().Tp == rows {
				p.next()
				ts.SampleClauseUnit = ast.SampleClauseUnitTypeRow
			}
		}
		p.expect(')')
		// Optional REPEATABLE(seed)
		if p.peek().IsKeyword("REPEATABLE") {
			p.next()
			p.expect('(')
			ts.RepeatableSeed = p.parseExpression(precNone)
			p.expect(')')
		}

		// Attach to TableName
		if tn, ok := res.(*ast.TableName); ok {
			tn.TableSample = ts
		}
	}

	// If it's a TableName, initialize its slices to empty (matching the TableFactor).
	if tn, ok := res.(*ast.TableName); ok {
		if tn.IndexHints == nil {
			tn.IndexHints = make([]*ast.IndexHint, 0)
		}
		if tn.PartitionNames == nil {
			tn.PartitionNames = make([]ast.CIStr, 0)
		}
	}

	// If we have alias, or it's a TableName, or it's a subquery (SelectStmt/SetOprStmt),
	// we MUST wrap in TableSource. The parser always wraps these in TableSource.
	_, isTableName := res.(*ast.TableName)
	_, isSelect := res.(*ast.SelectStmt)
	_, isSetOpr := res.(*ast.SetOprStmt)
	if asName.O != "" || isTableName || isSelect || isSetOpr {
		ts := Alloc[ast.TableSource](p.arena)
		ts.Source = res
		ts.AsName = asName
		return ts
	}

	return res
}

// parseTableName parses [schema.]table.
// Accepts both identifiers and keyword tokens as table names, since MySQL
// allows most keywords to be used as unquoted identifiers.
func (p *HandParser) parseTableName() *ast.TableName {
	tok := p.next()
	// Special case: '*' as wildcard schema name (e.g., *.tablename in bindings)
	if tok.Tp == '*' && p.peek().Tp == '.' {
		p.next() // consume '.'
		nextTok := p.next()
		tn := Alloc[ast.TableName](p.arena)
		tn.Schema = ast.NewCIStr("*")
		tn.Name = ast.NewCIStr(nextTok.Lit)
		return tn
	}
	// All keyword/identifier tokens have Tp >= identifier and carry their text in Lit.
	if !isIdentLike(tok.Tp) && tok.Tp != underscoreCS {
		// Use errorNear for yacc-compatible error format (no extra diagnostic text).
		// colOffset uses peek (the next token after the consumed one) for column alignment,
		// nearOffset uses the consumed token's offset for the "near" context text.
		p.errorNear(p.peek().Offset, tok.Offset)
		return nil
	}

	tn := Alloc[ast.TableName](p.arena)
	// Check for schema.table, but not table.* (used in DELETE t1.* FROM ...)
	if p.peek().Tp == '.' && p.peekN(1).Tp != '*' {
		p.next() // consume '.'
		nextTok := p.next()
		if nextTok.Tp < identifier && nextTok.Tp != underscoreCS {
			p.syntaxErrorAt(nextTok)
			return nil
		}
		tn.Schema = ast.NewCIStr(tok.Lit)
		tn.Name = ast.NewCIStr(nextTok.Lit)

		// Validate: empty or whitespace-only database name is invalid (MySQL error 1102)
		if strings.TrimSpace(tok.Lit) == "" {
			p.errs = append(p.errs, fmt.Errorf("[parser:1102]Incorrect database name '%s'", tok.Lit))
			return nil
		}
	} else {
		tn.Name = ast.NewCIStr(tok.Lit)
	}

	return tn
}

// parseColumnNameList parses a comma-separated list of column names.
func (p *HandParser) parseColumnNameList() []*ast.ColumnName {
	var cols []*ast.ColumnName
	for {
		tok := p.next()
		col := p.arena.AllocColumnName()
		col.Name = ast.NewCIStr(tok.Lit)
		cols = append(cols, col)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return cols
}

// parseIndexHint parses: {USE|IGNORE|FORCE} {INDEX|KEY} [FOR {JOIN|ORDER BY|GROUP BY}] (index_name [,...])
func (p *HandParser) parseIndexHint() *ast.IndexHint {
	hint := &ast.IndexHint{
		HintScope: ast.HintForScan, // Default: no FOR clause = scan scope
	}

	// Hint type.
	switch p.peek().Tp {
	case use:
		p.next()
		hint.HintType = ast.HintUse
	case ignore:
		p.next()
		hint.HintType = ast.HintIgnore
	case force:
		p.next()
		hint.HintType = ast.HintForce
	default:
		return nil
	}

	// INDEX or KEY (both valid, treated identically).
	if _, ok := p.accept(index); !ok {
		if _, ok := p.accept(key); !ok {
			p.syntaxErrorAt(p.peek())
			return nil
		}
	}

	// Optional FOR scope.
	if _, ok := p.accept(forKwd); ok {
		switch p.peek().Tp {
		case join:
			p.next()
			hint.HintScope = ast.HintForJoin
		case order:
			p.next()
			p.expect(by)
			hint.HintScope = ast.HintForOrderBy
		case group:
			p.next()
			p.expect(by)
			hint.HintScope = ast.HintForGroupBy
		}
	}

	// (index_name [, index_name ...])
	p.expect('(')
	for {
		tok := p.peek()
		if tok.Tp == ')' {
			break
		}
		// Index names can be identifiers or backtick-quoted.
		nameTok := p.next()
		hint.IndexNames = append(hint.IndexNames, ast.NewCIStr(nameTok.Lit))
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	p.expect(')')

	return hint
}

// parseIndexHintsInto parses index hints (USE/IGNORE/FORCE INDEX/KEY) into a TableName.
func (p *HandParser) parseIndexHintsInto(tn *ast.TableName) {
	for p.peek().Tp == use || p.peek().Tp == ignore || p.peek().Tp == force {
		hint := p.parseIndexHint()
		if hint != nil {
			tn.IndexHints = append(tn.IndexHints, hint)
		}
	}
}
