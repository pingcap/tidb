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
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

// parseExpression parses an expression using Pratt (top-down operator precedence) parsing.
// minPrec is the minimum precedence to bind; operators with lower precedence
// will cause the parser to return and let the caller handle them.
func (p *HandParser) parseExpression(minPrec int) ast.ExprNode {
	left := p.parsePrefixExpr(minPrec)
	if left == nil {
		return nil
	}
	return p.parseInfixExpr(left, minPrec)
}

// parseInfixExpr continues parsing infix/postfix operators after a prefix expression.
func (p *HandParser) parseInfixExpr(left ast.ExprNode, minPrec int) ast.ExprNode {
	for {
		tok := p.peek()
		if tok.Tp == EOF {
			break
		}

		// Handle keyword-based infix operators that don't fit the simple precedence model.
		switch tok.Tp {
		case not:
			// "NOT IN", "NOT LIKE", "NOT BETWEEN", "NOT REGEXP/RLIKE"
			if minPrec > precPredicate {
				return left
			}
			// Only treat NOT as infix if followed by IN/LIKE/ILIKE/BETWEEN/REGEXP/RLIKE
			nextTp := p.peekN(1).Tp
			if nextTp != in && nextTp != like && nextTp != ilike &&
				nextTp != between && nextTp != regexpKwd && nextTp != rlike {
				// yacc: when NOT follows an expression but isn't followed by
				// IN/LIKE/BETWEEN/REGEXP, yacc consumes NOT and errors at the
				// next token (e.g., NULL). Match that behavior.
				notTok := p.next() // consume NOT
				nextTok := p.peek()
				if nextTok.Tp == EOF {
					p.syntaxErrorAt(notTok)
				} else {
					p.syntaxErrorAt(nextTok)
				}
				return nil
			}
			left = p.parseNotInfixExpr(left)
			if left == nil {
				return nil
			}
			continue

		case in:
			if minPrec > precPredicate {
				return left
			}
			left = p.parseInExpr(left, false)
			if left == nil {
				return nil
			}
			continue

		case like, ilike:
			if minPrec > precPredicate {
				return left
			}
			isNotLike := (tok.Tp == like)
			left = p.parseLikeExpr(left, false, isNotLike)
			if left == nil {
				return nil
			}
			continue

		case between:
			if minPrec > precPredicate {
				return left
			}
			left = p.parseBetweenExpr(left, false)
			if left == nil {
				return nil
			}
			continue

		case regexpKwd, rlike:
			if minPrec > precPredicate {
				return left
			}
			left = p.parseRegexpExpr(left, false)
			if left == nil {
				return nil
			}
			continue

		case is:
			// IS is at the same precedence as comparison operators (=, >=, etc.)
			// in MySQL grammar — both are at the bool_primary level.
			// IS [NOT] NULL chains left-to-right at boolean_primary level:
			// "A IS NULL IS NULL IS NULL" parses as ((A IS NULL) IS NULL) IS NULL.
			// IS [NOT] TRUE/FALSE/UNKNOWN is at expr level and does NOT chain.
			if minPrec > precComparison {
				return left
			}
			var chainable bool
			left, chainable = p.parseIsExpr(left)
			if left == nil {
				return nil
			}
			if !chainable {
				// IS [NOT] TRUE/FALSE/UNKNOWN doesn't chain with IS,
				// but AND/OR/XOR should still bind. Continue the loop
				// so expression-level operators can be parsed.
				continue
			}
			continue

		case collate:
			if minPrec > precCollate {
				return left
			}
			left = p.parseCollateExpr(left)
			if left == nil {
				return nil
			}
			continue

		case jss, juss: // -> or ->> (JSON extract / unquote+extract)
			if minPrec > precPredicate {
				return left
			}
			left = p.parseJSONExtract(left, tok.Tp == juss)
			if left == nil {
				return nil
			}
			continue

		case memberof: // MEMBER OF (expr) - lexer merges MEMBER+OF into single token
			if minPrec > precPredicate {
				return left
			}
			p.next() // consume MEMBEROF token
			if _, ok := p.expect('('); !ok {
				return nil
			}
			// Parse the array argument as SimpleExpr: prefix + JSON extract chains.
			// Do NOT use parseExpression — MySQL grammar restricts MEMBER OF arg to SimpleExpr.
			arg := p.parsePrefixExpr(0)
			if arg == nil {
				return nil
			}
			// Allow -> and ->> JSON extract chains on the simple expr.
			for p.peek().Tp == jss || p.peek().Tp == juss {
				arg = p.parseJSONExtract(arg, p.peek().Tp == juss)
				if arg == nil {
					return nil
				}
			}
			if _, ok := p.expect(')'); !ok {
				return nil
			}
			node := &ast.FuncCallExpr{
				FnName: ast.NewCIStr(ast.JSONMemberOf),
				Args:   []ast.ExprNode{left, arg},
			}
			left = node
			continue
		}

		// Standard binary operator precedence.
		prec := tokenPrecedence(tok.Tp, p.sqlMode)
		if prec == precNone || prec < minPrec {
			break
		}

		op := p.next() // consume the operator

		if op.Tp == pipes {
			// In pipes_as_concat mode, '||' produces the 'pipes' token.
			// It compiles to a CONCAT() function call instead of a BinaryExpr.
			right := p.parseExpression(prec + 1)
			if right == nil {
				p.syntaxErrorAt(op)
				return nil
			}
			node := p.arena.AllocFuncCallExpr()
			node.FnName = ast.NewCIStr("concat")
			node.Args = []ast.ExprNode{left, right}
			left = node
			continue
		}

		opCode := tokenToOp(op.Tp)
		if opCode == 0 {
			p.syntaxErrorAt(op)
			return nil
		}

		// Check for ANY/SOME/ALL subquery comparison: expr <op> ANY/SOME/ALL (subquery)
		if prec == precComparison {
			switch p.peek().Tp {
			case any, some:
				if res := p.parseCompareSubquery(left, opCode, false); res != nil {
					left = res
					continue
				}
				return nil
			case all:
				if res := p.parseCompareSubquery(left, opCode, true); res != nil {
					left = res
					continue
				}
				return nil
			}
		}

		// Handle expr + INTERVAL expr unit → DATE_ADD(expr, INTERVAL expr unit)
		// Handle expr - INTERVAL expr unit → DATE_SUB(expr, INTERVAL expr unit)
		if node := p.parseDateArith(left, opCode); node != nil {
			left = node
			continue
		}

		// Right-hand side parsed at higher precedence for left-associativity.
		right := p.parseExpression(prec + 1)
		if right == nil {
			p.syntaxErrorAt(op)
			return nil
		}

		node := Alloc[ast.BinaryOperationExpr](p.arena)
		node.Op = opCode
		node.L = left
		node.R = right
		left = node
	}

	return left
}

// parsePrefixExpr parses a prefix expression (literals, identifiers, unary ops, etc.)
// minPrec prevents low-precedence prefix operators (like NOT at precNot=4) from
// being parsed when the caller requires a higher-precedence expression (e.g. precUnary=12).
func (p *HandParser) parsePrefixExpr(minPrec int) ast.ExprNode {
	tok := p.peek()

	switch tok.Tp {
	case intLit, floatLit, decLit, stringLit, hexLit, bitLit:
		return p.parseLiteral()

	case '{': // ODBC escape sequence
		p.next() // consume '{'
		tok := p.next()
		// The ODBC type identifier can be 'd', 't', 'ts', 'fn', 'date', 'time', 'timestamp', etc.
		// These may be tokenized as keywords (e.g. DATE → dateType), so we match on Lit, not Tp.
		typ := strings.ToLower(tok.Lit)

		// Parse the inner expression (not just string literal).
		// The grammar has: '{' Identifier Expression '}'
		innerExpr := p.parseExpression(precNone)
		if innerExpr == nil {
			return nil
		}
		p.expect('}')

		switch typ {
		case "d", "date":
			// Clear charset on the inner expression to match expected behavior.
			if ve, ok := innerExpr.(ast.ValueExpr); ok {
				ve.GetType().SetCharset("")
				ve.GetType().SetCollate("")
			}
			return &ast.FuncCallExpr{FnName: ast.NewCIStr(ast.DateLiteral), Args: []ast.ExprNode{innerExpr}}
		case "t", "time":
			if ve, ok := innerExpr.(ast.ValueExpr); ok {
				ve.GetType().SetCharset("")
				ve.GetType().SetCollate("")
			}
			return &ast.FuncCallExpr{FnName: ast.NewCIStr(ast.TimeLiteral), Args: []ast.ExprNode{innerExpr}}
		case "ts", "timestamp":
			if ve, ok := innerExpr.(ast.ValueExpr); ok {
				ve.GetType().SetCharset("")
				ve.GetType().SetCollate("")
			}
			return &ast.FuncCallExpr{FnName: ast.NewCIStr(ast.TimestampLiteral), Args: []ast.ExprNode{innerExpr}}
		default:
			// Unknown ODBC identifier (e.g. fn): pass through inner expression.
			return innerExpr
		}

	case null:
		p.next()
		return p.newValueExpr(nil)

	case trueKwd, falseKwd:
		return p.newValueExpr(p.next().Tp == trueKwd)

	case utcTimestamp, utcTime, utcDate:
		return p.parseTimeFunc(p.peek().Lit)

	case underscoreCS:
		return p.parseCharsetIntroducer()

	case identifier:
		return p.parseIdentOrFuncCall()

	case paramMarker:
		return p.parseParamMarker()

	case '(':
		return p.parseParenOrSubquery()

	case '-', '+', '~', '!':
		var op opcode.Op
		switch tok.Tp {
		case '-':
			op = opcode.Minus
		case '+':
			op = opcode.Plus
		case '~':
			op = opcode.BitNeg
		case '!':
			op = opcode.Not2
		}
		return p.parseUnaryOp(op, precUnary)

	case not:
		if minPrec > precNot {
			return nil
		}
		p.next()
		expr := p.parseExpression(precNot)
		if expr == nil {
			return nil
		}
		// Special case: NOT EXISTS → toggle ExistsSubqueryExpr.Not
		// instead of wrapping in UnaryOperationExpr.
		if existsExpr, ok := expr.(*ast.ExistsSubqueryExpr); ok {
			existsExpr.Not = !existsExpr.Not
			return existsExpr
		}
		node := Alloc[ast.UnaryOperationExpr](p.arena)
		node.Op = opcode.Not // NOT keyword restores as "NOT"
		node.V = expr
		return node

	case not2:
		// not2 is emitted by the lexer when HIGH_NOT_PRECEDENCE mode is set.
		// It has the same precedence as ! and restores as "!".
		if minPrec > precUnary {
			return nil
		}
		return p.parseUnaryOp(opcode.Not2, precUnary)

	default:
		return p.parsePrefixKeywordExpr(minPrec)
	}
}

// parseLiteral parses a numeric or string literal.
func (p *HandParser) parseLiteral() ast.ExprNode {
	tok := p.next()

	switch tok.Tp {
	case intLit, floatLit, decLit:
		return p.newValueExpr(tok.Item)
	case stringLit:
		// MySQL concatenates adjacent string literals: 'a' 'b' → 'ab'
		val := tok.Lit
		firstLen := -1
		for p.peek().Tp == stringLit {
			if firstLen < 0 {
				firstLen = len(val)
			}
			val += p.next().Lit
		}
		node := p.newValueExpr(val)
		// When adjacent string literals are concatenated, set projectionOffset
		// to the length of the first literal so that the column name in SELECT
		// uses only the first literal (matching MySQL behavior).
		if firstLen >= 0 {
			if ve, ok := node.(ast.ValueExpr); ok {
				ve.SetProjectionOffset(firstLen)
			}
		}
		return node
	case hexLit, bitLit:
		var ctor func(string) (interface{}, error)
		if tok.Tp == hexLit {
			ctor = ast.NewHexLiteral
		} else {
			ctor = ast.NewBitLiteral
		}
		return p.newLiteralExpr(ctor, tok.Lit)
	default:
		p.syntaxErrorAt(tok)
		return nil
	}
}

// parseSignedLiteral parses the yacc SignedLiteral production:
//
//	SignedLiteral: Literal | '+' NumLiteral | '-' NumLiteral
//
// Literal includes: FALSE, NULL, TRUE, intLit, floatLit, decLit, stringLit,
// UNDERSCORE_CHARSET stringLit, hexLit, bitLit.
// NumLiteral includes: intLit, floatLit, decLit.
func (p *HandParser) parseSignedLiteral() ast.ExprNode {
	tok := p.peek()
	switch tok.Tp {
	case '+', '-':
		p.next()
		numTok := p.peek()
		if numTok.Tp != intLit && numTok.Tp != floatLit && numTok.Tp != decLit {
			p.syntaxErrorAt(numTok)
			return nil
		}
		val := p.newValueExpr(p.next().Item)
		if tok.Tp == '+' {
			return &ast.UnaryOperationExpr{Op: opcode.Plus, V: val}
		}
		return &ast.UnaryOperationExpr{Op: opcode.Minus, V: val}
	case null:
		p.next()
		return p.newValueExpr(nil)
	case trueKwd, falseKwd:
		return p.newValueExpr(p.next().Tp == trueKwd)
	case underscoreCS:
		return p.parseCharsetIntroducer()
	case intLit, floatLit, decLit, stringLit, hexLit, bitLit:
		return p.parseLiteral()
	default:
		p.syntaxErrorAt(tok)
		return nil
	}
}

// parseIdentOrFuncCall parses an identifier, which may be a column name
// or a function call (if followed by '(').
func (p *HandParser) parseIdentOrFuncCall() ast.ExprNode {
	tok := p.next() // consume the identifier

	// Schema-qualified function call: schema.func(...)
	if p.peek().Tp == '.' && p.peekN(1).Tp >= identifier && p.peekN(2).Tp == '(' {
		p.next()                // consume '.'
		funcNameTok := p.next() // consume function name
		// Generic function with schema: s.a()
		p.expect('(')
		var args []ast.ExprNode
		if p.peek().Tp != ')' {
			for {
				arg := p.parseExpression(precNone)
				if arg == nil {
					return nil
				}
				args = append(args, arg)
				if _, ok := p.accept(','); !ok {
					break
				}
			}
		}
		p.expect(')')
		node := &ast.FuncCallExpr{
			Tp:     ast.FuncCallExprTypeGeneric,
			Schema: ast.NewCIStr(tok.Lit),
			FnName: ast.NewCIStr(funcNameTok.Lit),
			Args:   args,
		}
		node.SetOriginTextPosition(tok.Offset)
		return node
	}

	// If followed by '(', it's a function call.
	if p.peek().Tp == '(' {
		result := p.parseFuncCall(tok.Lit)
		if result != nil {
			result.(ast.Node).SetOriginTextPosition(tok.Offset)
		}
		return result
	}

	// Otherwise, it's a column reference.
	return p.parseColumnRef(tok)
}

// parseColumnRef parses a possibly-qualified column name starting with the
// already-consumed first identifier token.
func (p *HandParser) parseColumnRef(first Token) ast.ExprNode {
	col := p.arena.AllocColumnName()
	col.Name = ast.NewCIStr(first.Lit)

	// Check for schema.table.column or table.column
	if _, ok := p.accept('.'); ok {
		nextTok := p.next()
		if _, ok2 := p.accept('.'); ok2 {
			// schema.table.column
			thirdTok := p.next()
			col.Schema = col.Name
			col.Table = ast.NewCIStr(nextTok.Lit)
			col.Name = ast.NewCIStr(thirdTok.Lit)
		} else {
			// table.column
			col.Table = col.Name
			col.Name = ast.NewCIStr(nextTok.Lit)
		}
	}

	expr := Alloc[ast.ColumnNameExpr](p.arena)
	expr.Name = col
	return expr
}

// parseParamMarker parses a ? parameter marker.
func (p *HandParser) parseParamMarker() ast.ExprNode {
	tok := p.next()
	return ast.NewParamMarkerExpr(tok.Offset)
}

// parseNotInfixExpr parses NOT followed by IN, LIKE, BETWEEN, REGEXP.
func (p *HandParser) parseNotInfixExpr(left ast.ExprNode) ast.ExprNode {
	p.next() // consume NOT

	switch p.peek().Tp {
	case in:
		return p.parseInExpr(left, true)
	case like:
		return p.parseLikeExpr(left, true, true)
	case ilike:
		return p.parseLikeExpr(left, true, false)
	case between:
		return p.parseBetweenExpr(left, true)
	case regexpKwd, rlike:
		return p.parseRegexpExpr(left, true)
	default:
		p.syntaxErrorAt(p.peek())
		return nil
	}
}

// parseLikeExpr parses [NOT] LIKE/ILIKE pattern [ESCAPE escape_char].
func (p *HandParser) parseLikeExpr(left ast.ExprNode, isNot bool, isLike bool) ast.ExprNode {
	p.next() // consume LIKE or ILIKE

	pattern := p.parseExpression(precPredicate + 1)
	if pattern == nil {
		return nil
	}
	node := Alloc[ast.PatternLikeOrIlikeExpr](p.arena)
	node.Expr = left
	node.Not = isNot
	node.IsLike = isLike
	node.Pattern = pattern

	// Optional ESCAPE clause.
	// Note: 'escape' can be either an identifier or the escape keyword token.
	if _, ok := p.acceptKeyword(escape, "escape"); ok {
		escTok := p.next()
		switch len(escTok.Lit) {
		case 0:
			// Empty string: default to backslash (Restore omits ESCAPE when '\\').
			node.Escape = '\\'
		case 1:
			node.Escape = escTok.Lit[0]
		default:
			p.errs = append(p.errs, ErrWrongArguments.GenWithStackByArgs("ESCAPE"))
			return nil
		}
	} else {
		node.Escape = '\\'
	}

	return node
}

// parseBetweenExpr parses [NOT] BETWEEN low AND high.
func (p *HandParser) parseBetweenExpr(left ast.ExprNode, isNot bool) ast.ExprNode {
	p.next() // consume BETWEEN

	low := p.parseExpression(precPredicate + 1)
	if low == nil {
		return nil
	}
	if _, ok := p.expect(and); !ok {
		return nil
	}
	high := p.parseExpression(precPredicate)
	if high == nil {
		return nil
	}

	node := Alloc[ast.BetweenExpr](p.arena)
	node.Expr = left
	node.Not = isNot
	node.Left = low
	node.Right = high

	return node
}

// parseRegexpExpr parses [NOT] REGEXP|RLIKE pattern.
func (p *HandParser) parseRegexpExpr(left ast.ExprNode, isNot bool) ast.ExprNode {
	p.next() // consume REGEXP or RLIKE

	pattern := p.parseExpression(precPredicate + 1)
	if pattern == nil {
		return nil
	}
	node := Alloc[ast.PatternRegexpExpr](p.arena)
	node.Expr = left
	node.Not = isNot
	node.Pattern = pattern

	return node
}

// parseJSONExtract parses -> or ->> JSON extract operators.
// If unquote is true, wraps the result in json_unquote (for ->>).
func (p *HandParser) parseJSONExtract(left ast.ExprNode, unquote bool) ast.ExprNode {
	opName := "->"
	if unquote {
		opName = "->>"
	}
	if _, ok := left.(*ast.ColumnNameExpr); !ok {
		p.error(p.peek().Offset, "%s requires a column reference on the left side", opName)
		return nil
	}
	p.next() // consume -> or ->>
	if p.peek().Tp != stringLit {
		p.error(p.peek().Offset, "%s requires a string literal JSON path on the right side", opName)
		return nil
	}
	path := p.parsePrefixExpr(0)
	if path == nil {
		return nil
	}
	extractNode := &ast.FuncCallExpr{
		FnName: ast.NewCIStr("json_extract"),
		Args:   []ast.ExprNode{left, path},
	}
	if !unquote {
		return extractNode
	}
	return &ast.FuncCallExpr{
		FnName: ast.NewCIStr("json_unquote"),
		Args:   []ast.ExprNode{extractNode},
	}
}

// parseIsExpr parses IS [NOT] NULL | IS [NOT] TRUE | IS [NOT] FALSE | IS [NOT] UNKNOWN.
// Returns (node, chainable). In MySQL grammar IS [NOT] NULL is at bool_primary level
// and chains left-to-right, while IS [NOT] TRUE/FALSE/UNKNOWN is at expr level and
// does NOT chain (e.g. "SELECT TRUE IS TRUE IS TRUE" is a syntax error in MySQL).
func (p *HandParser) parseIsExpr(left ast.ExprNode) (ast.ExprNode, bool) {
	p.next() // consume IS

	isNot := false
	if _, ok := p.accept(not); ok {
		isNot = true
	}

	switch p.peek().Tp {
	case null:
		p.next()
		node := Alloc[ast.IsNullExpr](p.arena)
		node.Expr = left
		node.Not = isNot
		return node, true // IS NULL chains at boolean_primary level

	case trueKwd, falseKwd:
		trueVal := int64(0)
		if p.peek().Tp == trueKwd {
			trueVal = 1
		}
		p.next()
		node := Alloc[ast.IsTruthExpr](p.arena)
		node.Expr = left
		node.Not = isNot
		node.True = trueVal
		return node, false // IS TRUE/FALSE does NOT chain

	default:
		// IS [NOT] UNKNOWN — UNKNOWN is not a reserved keyword, so it comes as
		// an identifier. SQL standard: UNKNOWN is equivalent to NULL.
		if p.peek().IsKeyword("UNKNOWN") {
			p.next()
			node := Alloc[ast.IsNullExpr](p.arena)
			node.Expr = left
			node.Not = isNot
			return node, false // IS UNKNOWN does NOT chain (expr level in MySQL grammar)
		}
		p.syntaxErrorAt(p.peek())
		return nil, false
	}
}

// parseCollateExpr parses COLLATE collation_name.
func (p *HandParser) parseCollateExpr(left ast.ExprNode) ast.ExprNode {
	p.next() // consume COLLATE
	collTok := p.next()

	// Validate collation name (matching the CollationName rule).
	info, err := charset.GetCollationByName(collTok.Lit)
	if err != nil {
		p.errs = append(p.errs, err)
		return nil
	}

	node := Alloc[ast.SetCollationExpr](p.arena)
	node.Expr = left
	node.Collate = info.Name
	return node
}

// parseVariableExpr parses @var or @@var (including @@global.var, @@session.var).
func (p *HandParser) parseVariableExpr() ast.ExprNode {
	tok := p.next()
	node := Alloc[ast.VariableExpr](p.arena)

	if tok.Tp == doubleAtIdentifier {
		node.IsSystem = true
		// tok.Lit contains the full literal, e.g. "@@global.max_connections"
		// Strip leading "@@" and lowercase (matching the SystemVariable rule).
		name := strings.ToLower(tok.Lit)
		if len(name) >= 2 && name[0] == '@' && name[1] == '@' {
			name = name[2:]
		}
		// Check for scope prefix: global., session., instance., local.
		if idx := strings.IndexByte(name, '.'); idx >= 0 {
			scope := name[:idx]
			name = name[idx+1:]
			node.ExplicitScope = true
			switch scope {
			case "global":
				node.IsGlobal = true
			case "instance":
				node.IsInstance = true
				// "session" and "local" are the default — IsGlobal=false, IsInstance=false.
			}
		}
		node.Name = name
	} else {
		// @user_var: strip leading "@".
		name := tok.Lit
		if len(name) > 1 && name[0] == '@' {
			name = name[1:]
		}
		node.Name = name
	}

	// Check for assignment: @var := expr
	if p.peek().Tp == assignmentEq {
		p.next() // consume :=
		node.IsGlobal = false
		node.IsSystem = false
		node.Value = p.parseExpression(precNone)
	}
	return node
}

// Helper methods for creating AST value nodes.

// newValueExpr creates a ValueExpr using the registered driver function.
func (p *HandParser) newValueExpr(val interface{}) ast.ExprNode {
	return ast.NewValueExpr(val, p.charset, p.collation)
}

// newLiteralExpr creates a literal node from a parse function (hex or bit).
func (p *HandParser) newLiteralExpr(parse func(string) (interface{}, error), lit string) ast.ExprNode {
	val, err := parse(lit)
	if err != nil {
		p.errs = append(p.errs, err)
		return p.newValueExpr(nil)
	}
	return p.newValueExpr(val)
}

// parseCharsetIntroducer parses _charset 'string' / _charset 0xHex / _charset 0bBit.
// Matches the UNDERSCORE_CHARSET rule.
func (p *HandParser) parseCharsetIntroducer() ast.ExprNode {
	csTok := p.next() // consume _charset token (e.g. _UTF8MB4)
	csName := csTok.Lit

	co, err := charset.GetDefaultCollationLegacy(csName)
	if err != nil {
		p.errs = append(p.errs, ast.ErrUnknownCharacterSet.GenWithStack("Unsupported character introducer: '%-.64s'", csName))
		return nil
	}

	// The next token should be a string literal, hex literal, or bit literal.
	valTok := p.peek()
	var expr ast.ExprNode
	switch valTok.Tp {
	case stringLit:
		p.next()
		expr = ast.NewValueExpr(valTok.Lit, csName, co)
	case hexLit, bitLit:
		p.next()
		var parseFn func(string) (interface{}, error)
		if valTok.Tp == hexLit {
			parseFn = ast.NewHexLiteral
		} else {
			parseFn = ast.NewBitLiteral
		}
		val, litErr := parseFn(valTok.Lit)
		if litErr != nil {
			p.errs = append(p.errs, litErr)
			return nil
		}
		expr = ast.NewValueExpr(val, csName, co)
	default:
		p.syntaxErrorAt(valTok)
		return nil
	}

	// Set charset and collation flags on the expression's type.
	if ve, ok := expr.(ast.ValueExpr); ok {
		tp := ve.GetType()
		tp.SetCharset(csName)
		tp.SetCollate(co)
		tp.AddFlag(mysql.UnderScoreCharsetFlag)
		if co == charset.CollationBin {
			tp.AddFlag(mysql.BinaryFlag)
		}
	}

	return expr
}

// parseOptPrecisionFunc parses a builtin function that takes an optional precision:
// NAME([intlit]) for CURTIME/SYSDATE, or NAME([expr]) for NOW.
//
// In the yacc grammar, NOW goes through FunctionNameConflict '(' ExpressionListOpt ')'
// and accepts any expression. CURTIME/SYSDATE go through FunctionCallNonKeyword with
// FuncDatetimePrecListOpt which only accepts intLit or empty.
func (p *HandParser) parseOptPrecisionFunc() ast.ExprNode {
	tok := p.next() // consume the function token
	p.expect('(')

	node := p.arena.AllocFuncCallExpr()
	node.FnName = ast.NewCIStr(tok.Lit)

	if p.peek().Tp != ')' {
		if tok.Tp == builtinFnNow || tok.Tp == now {
			// NOW() accepts any expression — the yacc parser routes builtinNow
			// through FunctionNameConflict, and runtime validates via getFspByIntArg.
			arg := p.parseExpression(precNone)
			if arg == nil {
				return nil
			}
			node.Args = []ast.ExprNode{arg}
		} else {
			// CURTIME()/SYSDATE() only accept an integer literal precision.
			node.Args = []ast.ExprNode{p.parseIntLitPrecision()}
		}
	}

	p.expect(')')
	return node
}

// parseIntLitPrecision expects an intLit token and returns it as a ValueExpr.
// Used for datetime precision arguments where only integer literals are allowed.
func (p *HandParser) parseIntLitPrecision() ast.ExprNode {
	tok, ok := p.expectAny(intLit)
	if !ok {
		return nil
	}
	return ast.NewValueExpr(tok.Item, p.charset, p.collation)
}

// parseCurDateFunc parses CURDATE() and CURRENT_DATE().
// Uses the original identifier as the function name.
func (p *HandParser) parseCurDateFunc() ast.ExprNode {
	tok := p.next() // consume builtinFnCurDate token
	p.expect('(')
	p.expect(')')

	node := p.arena.AllocFuncCallExpr()
	node.FnName = ast.NewCIStr(tok.Lit)
	return node
}

// parseMatchAgainstExpr parses MATCH(col [, col ...]) AGAINST (expr [search_modifier]).
// search_modifier = IN BOOLEAN MODE | WITH QUERY EXPANSION |
//
//	IN NATURAL LANGUAGE MODE [WITH QUERY EXPANSION]
func (p *HandParser) parseMatchAgainstExpr() ast.ExprNode {
	p.next() // consume MATCH
	p.expect('(')

	node := &ast.MatchAgainst{}

	// Column list — yacc uses ColumnNameList which supports qualified names.
	for {
		col := p.parseColumnName()
		if col == nil {
			return nil
		}
		node.ColumnNames = append(node.ColumnNames, col)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	p.expect(')')

	// AGAINST (expr [modifier])
	p.expect(against)
	p.expect('(')
	againstExpr := p.parseExpression(precPredicate + 1)
	if againstExpr == nil {
		return nil
	}
	node.Against = againstExpr

	// Optional search modifier.
	if _, ok := p.accept(in); ok {
		// IN BOOLEAN MODE or IN NATURAL LANGUAGE MODE [WITH QUERY EXPANSION]
		if _, ok := p.accept(booleanType); ok {
			p.expect(mode)
			node.Modifier = ast.FulltextSearchModifier(ast.FulltextSearchModifierBooleanMode)
			// IN BOOLEAN MODE WITH QUERY EXPANSION is invalid.
			if _, ok := p.accept(with); ok {
				p.error(p.peek().Offset, "IN BOOLEAN MODE WITH QUERY EXPANSION is not supported")
				return nil
			}
		} else if _, ok := p.accept(natural); ok {
			p.expect(language)
			p.expect(mode)
			// "IN NATURAL LANGUAGE MODE" = NaturalLanguageMode (0)
			// Check for optional "WITH QUERY EXPANSION"
			if _, ok := p.accept(with); ok {
				p.expect(query)
				p.expect(expansion)
				node.Modifier = ast.FulltextSearchModifier(
					ast.FulltextSearchModifierNaturalLanguageMode | ast.FulltextSearchModifierWithQueryExpansion,
				)
			} else {
				node.Modifier = ast.FulltextSearchModifier(ast.FulltextSearchModifierNaturalLanguageMode)
			}
		} else {
			p.syntaxErrorAt(p.peek())
			return nil
		}
	} else if _, ok := p.accept(with); ok {
		// WITH QUERY EXPANSION
		p.expect(query)
		p.expect(expansion)
		node.Modifier = ast.FulltextSearchModifier(ast.FulltextSearchModifierWithQueryExpansion)
	}

	p.expect(')')
	return node
}

// parseCompareSubquery parses ANY/SOME/ALL (subquery).
// Returns nil on failure.
func (p *HandParser) parseCompareSubquery(left ast.ExprNode, opCode opcode.Op, all bool) ast.ExprNode {
	p.next() // consume ANY/SOME/ALL

	// The next token MUST be '('. The subquery is parsed directly, not as
	// an arbitrary expression, because (SELECT a) UNION (SELECT b) is valid
	// here but would fail in the expression parser.
	p.expect('(')
	subStartOff := p.peek().Offset
	sub := p.parseSubquery()
	// After parsing the first subquery, check for UNION/EXCEPT/INTERSECT.
	// This handles: > ALL((SELECT a) UNION (SELECT b))
	sub = p.maybeParseUnion(sub)
	// Set text on the inner statement to match yacc SubSelect behavior.
	subEndOff := p.peek().Offset
	if sub != nil && subEndOff > subStartOff {
		sub.(ast.Node).SetText(nil, p.src[subStartOff:subEndOff])
	}
	p.expect(')')

	subExpr := p.arena.AllocSubqueryExpr()
	subExpr.Query = sub

	cmpNode := Alloc[ast.CompareSubqueryExpr](p.arena)
	cmpNode.L = left
	cmpNode.Op = opCode
	cmpNode.R = subExpr
	cmpNode.All = all
	return cmpNode
}

func (p *HandParser) parseDateArith(left ast.ExprNode, opCode opcode.Op) ast.ExprNode {
	if (opCode != opcode.Plus && opCode != opcode.Minus) || p.peek().Tp != interval {
		return nil
	}
	// Use mark/restore to speculatively try date arithmetic.
	// In yacc, BitExpr '+' INTERVAL Expression TimeUnit is committed once INTERVAL
	// is consumed, but we need to handle INTERVAL(...) function calls that start with '('.
	saved := p.mark()
	savedErrs := len(p.errs)
	p.next() // consume INTERVAL
	intervalExpr := p.parseExpression(precNone)
	if intervalExpr == nil {
		p.errs = p.errs[:savedErrs]
		p.restore(saved)
		return nil
	}
	unit := p.parseTimeUnit()
	if unit == nil {
		p.errs = p.errs[:savedErrs]
		p.restore(saved)
		return nil
	}
	fnName := "DATE_ADD"
	if opCode == opcode.Minus {
		fnName = "DATE_SUB"
	}
	node := &ast.FuncCallExpr{
		FnName: ast.NewCIStr(fnName),
		Args: []ast.ExprNode{
			left,
			intervalExpr,
			unit,
		},
	}
	return node
}

func (p *HandParser) parseTimeFunc(fnName string) ast.ExprNode {
	p.next()
	if _, ok := p.accept('('); ok {
		// Parse optional fsp (0-6), must be literal integer
		var args []ast.ExprNode
		if p.peek().Tp != ')' {
			if p.peek().Tp == intLit { //revive:disable-line
				if lit := p.parseLiteral(); lit != nil { //revive:disable-line
					args = append(args, lit)
				} else {
					p.syntaxErrorAt(p.peek())
					return nil
				}
			} else {
				p.syntaxErrorAt(p.peek())
				return nil
			}
		}
		p.expect(')')
		node := p.arena.AllocFuncCallExpr()
		node.FnName = ast.NewCIStr(fnName)
		node.Args = args
		return node
	}
	// Niladic form
	node := p.arena.AllocFuncCallExpr()
	node.FnName = ast.NewCIStr(fnName)
	return node
}

func (p *HandParser) parseUnaryOp(opCode opcode.Op, prec int) ast.ExprNode {
	p.next()
	expr := p.parseExpression(prec)
	if expr == nil {
		return nil
	}
	node := Alloc[ast.UnaryOperationExpr](p.arena)
	node.Op = opCode
	node.V = expr
	return node
}
