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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

// parseKeywordFuncCall parses a function call where the function name is a
// SQL keyword (IF, REPLACE, COALESCE, etc.). These are not identifier,
// so they need special routing.
func (p *HandParser) parseKeywordFuncCall() ast.ExprNode {
	tok := p.next()
	var name string
	switch tok.Tp {
	case ifKwd, replace, coalesce:
		name = tok.Lit
	case insert:
		name = "insert_func"
	default:
		p.syntaxErrorAt(tok)
		return nil
	}
	return p.parseFuncCall(name)
}

// parseFuncCall parses a function call expression.
// Routes to AggregateFuncExpr for aggregate functions and FuncCallExpr for scalars.
// If followed by window modifiers (FROM LAST, IGNORE NULLS) and OVER (...), wraps in WindowFuncExpr.
func (p *HandParser) parseFuncCall(name string) ast.ExprNode {
	var result ast.ExprNode
	if isAggregateFunc(name) {
		result = p.parseAggregateFuncCall(name)
	} else {
		result = p.parseScalarFuncCall(name)
	}

	// Parse optional window function modifiers (only when window functions are enabled):
	// [FROM LAST | FROM FIRST] [IGNORE NULLS | RESPECT NULLS]
	// Use 2-token lookahead for FROM to avoid consuming FROM in SELECT...FROM.
	var fromLast, ignoreNull bool
	if p.supportWindowFunc && p.peek().Tp == from && (p.peekN(1).Tp == last || p.peekN(1).Tp == first) {
		p.next() // consume FROM
		if _, ok := p.accept(last); ok {
			fromLast = true
		} else {
			p.expect(first)
		}
	}
	if p.supportWindowFunc && p.peek().Tp == ignore && p.peekN(1).Tp == nulls {
		p.next() // consume IGNORE
		p.next() // consume NULLS
		ignoreNull = true
	} else if p.supportWindowFunc && p.peek().Tp == respect && p.peekN(1).Tp == nulls {
		p.next() // consume RESPECT
		p.next() // consume NULLS
		// RESPECT NULLS is the default, no flag needed
	}

	// Check for window function: func(...) OVER (...)
	// The scanner may tokenize OVER as either over (reserved keyword) or
	// identifier (non-reserved keyword — MySQL treats OVER as non-reserved).
	// Only parse window functions when supportWindowFunc is enabled.
	if p.supportWindowFunc {
		if _, ok := p.accept(over); ok || p.peek().IsKeyword("OVER") {
			if !ok {
				p.next() // consume the identifier "OVER" token
			}
			wf := p.parseWindowFuncExpr(name, result)
			if wfExpr, ok := wf.(*ast.WindowFuncExpr); ok {
				wfExpr.FromLast = fromLast
				wfExpr.IgnoreNull = ignoreNull
			}
			return wf
		}
	}
	return result
}

// parseWindowFuncExpr wraps a function call in a WindowFuncExpr with an OVER clause.
func (p *HandParser) parseWindowFuncExpr(name string, funcExpr ast.ExprNode) ast.ExprNode {
	// Normalize aggregate function aliases so downstream TypeInfer recognizes them.
	switch strings.ToLower(name) {
	case "std", "stddev":
		name = "STDDEV_POP"
	case "variance":
		name = "VAR_POP"
	}
	wf := &ast.WindowFuncExpr{
		Name: name,
	}

	// Extract args from the underlying function call.
	switch fn := funcExpr.(type) {
	case *ast.AggregateFuncExpr:
		wf.Args = fn.Args
		wf.Distinct = fn.Distinct
	case *ast.FuncCallExpr:
		wf.Args = fn.Args
	}

	// Validate arg count for specific window functions.
	lowerName := strings.ToLower(name)
	switch lowerName {
	case "nth_value":
		if len(wf.Args) != 2 {
			p.error(p.peek().Offset, "NTH_VALUE requires exactly 2 arguments")
			return nil
		}
	case "ntile":
		if len(wf.Args) != 1 {
			p.error(p.peek().Offset, "NTILE requires exactly 1 argument")
			return nil
		}
	case "lead", "lag":
		if len(wf.Args) < 1 || len(wf.Args) > 3 {
			p.error(p.peek().Offset, "%s requires 1 to 3 arguments", strings.ToUpper(name))
			return nil
		}
		// Argument restriction (2nd arg = NumLiteral only) is enforced at parse
		// time in parseLeadLagFuncCall, so no further validation needed here.
	}

	// Parse window specification: ( [PARTITION BY ...] [ORDER BY ...] [frame] )
	wf.Spec = p.parseWindowSpec()
	return wf
}

// parseWindowSpec parses a window specification: ( [PARTITION BY ...] [ORDER BY ...] [frame] )
func (p *HandParser) parseWindowSpec() ast.WindowSpec {
	spec := ast.WindowSpec{}

	// Check for named window reference: OVER w or OVER (w ...)
	if p.peek().Tp != '(' {
		// OVER window_name (alias only)
		spec.Name = ast.NewCIStr(p.next().Lit)
		spec.OnlyAlias = true
		return spec
	}

	p.expect('(') // consume '('

	// Optional window reference name
	if p.peek().Tp == identifier && p.peekN(1).Tp != by {
		spec.Ref = ast.NewCIStr(p.next().Lit)
	}

	// PARTITION BY
	if _, ok := p.accept(partition); ok {
		p.expect(by)
		spec.PartitionBy = p.parsePartitionByClause()
	}

	// ORDER BY
	if p.peek().Tp == order {
		spec.OrderBy = p.parseOrderByClause()
	}

	// Frame clause (ROWS/RANGE/GROUPS ...)
	if p.peek().Tp == rows || p.peek().Tp == groups || (p.peek().Tp == rangeKwd && p.peekN(1).Tp != ')') {
		spec.Frame = p.parseFrameClause()
	}

	p.expect(')') // consume ')'
	return spec
}

// parsePartitionByClause parses: PARTITION BY expr [, expr ...]
func (p *HandParser) parsePartitionByClause() *ast.PartitionByClause {
	clause := &ast.PartitionByClause{}
	for {
		item := &ast.ByItem{Expr: p.parseExpression(precNone)}
		clause.Items = append(clause.Items, item)
		if _, ok := p.accept(','); !ok {
			break
		}
	}
	return clause
}

// parseFrameClause parses: ROWS/RANGE frame_extent
func (p *HandParser) parseFrameClause() *ast.FrameClause {
	frame := &ast.FrameClause{}
	switch p.peek().Tp {
	case rows:
		p.next()
		frame.Type = ast.Rows
	case groups:
		p.next()
		frame.Type = ast.Groups
	default:
		p.next() // RANGE
		frame.Type = ast.Ranges
	}

	// BETWEEN ... AND ... or single bound
	if _, ok := p.accept(between); ok {
		frame.Extent.Start = p.parseFrameBound()
		p.expect(and)
		frame.Extent.End = p.parseFrameBound()
	} else {
		frame.Extent.Start = p.parseFrameBound()
		// Default end
		frame.Extent.End = ast.FrameBound{Type: ast.CurrentRow}
	}
	return frame
}

// parseFrameBound parses a frame bound: CURRENT ROW | UNBOUNDED PRECEDING/FOLLOWING | expr PRECEDING/FOLLOWING
func (p *HandParser) parseFrameBound() ast.FrameBound {
	bound := ast.FrameBound{}

	if _, ok := p.accept(current); ok {
		// CURRENT ROW — consume the ROW token (row, distinct from ROWS rows)
		p.next()
		bound.Type = ast.CurrentRow
		return bound
	}

	if _, ok := p.accept(unbounded); ok {
		if _, ok := p.accept(preceding); ok {
			bound.Type = ast.Preceding
			bound.UnBounded = true
		} else {
			p.expect(following)
			bound.Type = ast.Following
			bound.UnBounded = true
		}
		return bound
	}

	// INTERVAL expr unit PRECEDING/FOLLOWING (for RANGE frames)
	if _, ok := p.accept(interval); ok {
		expr := p.parseExpression(precNone)
		unit := p.parseTimeUnit()
		bound.Expr = expr
		if unit != nil {
			bound.Unit = unit.Unit
		}
	} else {
		// expr PRECEDING/FOLLOWING
		bound.Expr = p.parseExpression(precNone)
	}
	if _, ok := p.accept(preceding); ok {
		bound.Type = ast.Preceding
	} else {
		p.expect(following)
		bound.Type = ast.Following
	}
	return bound
}

// parseAggregateFuncCall parses an aggregate function: COUNT(*), SUM(DISTINCT x), etc.
func (p *HandParser) parseAggregateFuncCall(name string) ast.ExprNode {
	p.expect('(')

	node := Alloc[ast.AggregateFuncExpr](p.arena)
	// Normalize aggregate function aliases:
	// STD, STDDEV → STDDEV_POP; VARIANCE → VAR_POP
	switch strings.ToLower(name) {
	case "std", "stddev":
		name = "STDDEV_POP"
	case "variance":
		name = "VAR_POP"
	}
	node.F = name

	// COUNT(*) special case.
	if p.peek().Tp == '*' {
		p.next()
		p.expect(')')
		node.Args = []ast.ExprNode{p.newValueExpr(int64(1))}
		return node
	}

	// Empty arg list.
	if _, ok := p.accept(')'); ok {
		// Only COUNT() is valid with 0 args.
		switch strings.ToLower(name) {
		case "count":
			return node
		default:
			p.error(p.peek().Offset, "%s requires at least 1 argument", name)
			return nil
		}
	}

	// Optional DISTINCT/DISTINCTROW inside aggregate.
	hasDistinctAll := false
	if _, ok := p.accept(distinct); ok {
		node.Distinct = true
	} else if _, ok := p.accept(distinctRow); ok {
		// DISTINCTROW is a MySQL alias for DISTINCT.
		node.Distinct = true
	}
	if node.Distinct {
		if _, ok := p.accept(all); ok {
			hasDistinctAll = true
		}
	}

	// Reject DISTINCT ALL for COUNT.
	if hasDistinctAll && strings.ToLower(name) == "count" {
		p.error(p.peek().Offset, "COUNT does not support DISTINCT ALL")
		return nil
	}

	// Reject DISTINCT for functions that don't support it.
	// Only check canonical names, not aliases (std/stddev/variance are normalized above).
	if node.Distinct {
		switch strings.ToLower(name) {
		case "bit_and", "bit_or", "bit_xor", "json_arrayagg", "json_objectagg":
			p.error(p.peek().Offset, "%s does not support DISTINCT", name)
			return nil
		}
	}

	// Argument list.
	var firstComma *Token
	for {
		p.accept(all)

		arg := p.parseExpression(precNone)
		if arg == nil {
			return nil
		}
		node.Args = append(node.Args, arg)
		commaTok, ok := p.accept(',')
		if !ok {
			break
		}
		if firstComma == nil {
			ct := commaTok // copy
			firstComma = &ct
		}
	}

	// Multi-arg aggregates: COUNT(DISTINCT a, b), JSON_OBJECTAGG(a, b), GROUP_CONCAT(...)
	// are allowed. Others (MAX, MIN, SUM, AVG) accept at most 1 argument.
	// COUNT requires DISTINCT for multiple arguments.
	lowerName := strings.ToLower(name)
	switch lowerName {
	case "group_concat":
		// GROUP_CONCAT accepts multiple arguments.
	case "json_objectagg":
		// JSON_OBJECTAGG accepts exactly 2 arguments (key, value).
		if len(node.Args) != 2 {
			p.error(p.peek().Offset, "JSON_OBJECTAGG requires exactly 2 arguments")
			return nil
		}
	case "count":
		// COUNT(DISTINCT a, b) is valid but COUNT(a, b) is not.
		if len(node.Args) > 1 && !node.Distinct {
			if firstComma != nil {
				p.errorNear(firstComma.EndOffset, firstComma.Offset)
			} else {
				p.syntaxErrorAt(p.peek())
			}
			return nil
		}
	case "approx_count_distinct", "approx_percentile":
		// These accept multiple arguments.
	default:
		if len(node.Args) > 1 {
			if firstComma != nil {
				p.errorNear(firstComma.EndOffset, firstComma.Offset)
			} else {
				p.syntaxErrorAt(p.peek())
			}
			return nil
		}
	}

	// GROUP_CONCAT requires special handling:
	// - optional ORDER BY clause
	// - optional SEPARATOR clause (defaults to ',')
	// AggregateFuncExpr.Restore() expects the last Args element to be the SEPARATOR value.
	if lowerName == "group_concat" {
		if _, ok := p.accept(order); ok {
			p.expect(by)
			ob := Alloc[ast.OrderByClause](p.arena)
			ob.Items = p.parseByItems()
			node.Order = ob
		}
		if _, ok := p.accept(separator); ok {
			// SEPARATOR followed by a string literal — use empty charset/collation
			// to match the OptGConcatSeparator rule.
			tok := p.next()
			node.Args = append(node.Args, ast.NewValueExpr(tok.Lit, "", ""))
		} else {
			node.Args = append(node.Args, ast.NewValueExpr(",", "", ""))
		}
	}

	p.expect(')')
	return node
}

// parseSequenceTableArg parses a sequence name as a TableNameExpr.
// Shared by nextval/lastval/setval function calls and NEXT VALUE FOR.
func (p *HandParser) parseSequenceTableArg() ast.ExprNode {
	tName := p.parseTableName()
	if tName == nil {
		return nil
	}
	tnExpr := Alloc[ast.TableNameExpr](p.arena)
	tnExpr.Name = tName
	return tnExpr
}

// parseScalarFuncCall parses a non-aggregate function call: CONCAT(a, b), IF(x, y, z), etc.
func (p *HandParser) parseScalarFuncCall(name string) ast.ExprNode {
	p.expect('(')

	// Empty arg list — must allocate FuncCallExpr for this case.
	if _, ok := p.accept(')'); ok {
		node := p.arena.AllocFuncCallExpr()
		node.FnName = ast.NewCIStr(name)
		return node
	}

	lowerName := strings.ToLower(name)

	// MOD returns a BinaryOperationExpr, not a FuncCallExpr — check before allocating.
	if lowerName == "mod" {
		return p.parseModFuncCall()
	}

	node := p.arena.AllocFuncCallExpr()
	node.FnName = ast.NewCIStr(name)

	switch lowerName {
	case "nextval", "lastval":
		return p.parseNextLastValFuncCall(node)
	case "setval":
		return p.parseSetValFuncCall(node)
	case "adddate", "subdate":
		return p.parseDateAddSubFuncCall(node)
	case "char", "char_func":
		return p.parseCharFuncCall(node, lowerName)
	case "get_format":
		return p.parseGetFormatFuncCall(node)
	case "weight_string":
		return p.parseWeightStringFuncCall(node)
	case "timestampadd":
		return p.parseTimestampAddFuncCall(node)
	case "lead", "lag":
		return p.parseLeadLagFuncCall(node)
	}

	// Generic argument list.
	for {
		arg := p.parseExpression(precNone)
		if arg == nil {
			return nil
		}
		node.Args = append(node.Args, arg)
		if _, ok := p.accept(','); !ok {
			break
		}
	}

	p.expect(')')
	return node
}

// parseLeadLagFuncCall parses LEAD/LAG function calls with restricted argument syntax.
// yacc grammar restricts the 2nd argument to NumLiteral (unsigned int/float literal
// or param marker), not a general Expression. This prevents accepting `-1`.
func (p *HandParser) parseLeadLagFuncCall(node *ast.FuncCallExpr) ast.ExprNode {
	// First arg: general expression (required)
	arg1 := p.parseExpression(precNone)
	if arg1 == nil {
		return nil
	}
	node.Args = append(node.Args, arg1)

	// Optional 2nd and 3rd args
	if _, ok := p.accept(','); ok {
		// 2nd arg: NumLiteral only (yacc grammar: unsigned int/float literal or param marker)
		tok := p.peek()
		switch tok.Tp {
		case intLit, floatLit, decLit:
			arg2 := p.parseExpression(precNone)
			if arg2 == nil {
				return nil
			}
			node.Args = append(node.Args, arg2)
		case paramMarker:
			arg2 := p.parseExpression(precNone)
			if arg2 == nil {
				return nil
			}
			node.Args = append(node.Args, arg2)
		default:
			// Not a NumLiteral — error at the unexpected token (e.g., '-')
			p.syntaxErrorAt(tok)
			return nil
		}

		// Optional 3rd arg: general expression (default value)
		if _, ok := p.accept(','); ok {
			arg3 := p.parseExpression(precNone)
			if arg3 == nil {
				return nil
			}
			node.Args = append(node.Args, arg3)
		}
	}

	p.expect(')')
	return node
}

// parseNextLastValFuncCall parses NEXTVAL(seq) or LASTVAL(seq).
// The sequence name is parsed as a TableName wrapped in TableNameExpr.
func (p *HandParser) parseNextLastValFuncCall(node *ast.FuncCallExpr) ast.ExprNode {
	seqArg := p.parseSequenceTableArg()
	if seqArg == nil {
		return nil
	}
	p.expect(')')
	node.Args = []ast.ExprNode{seqArg}
	return node
}

// parseSetValFuncCall parses SETVAL(seq, value).
func (p *HandParser) parseSetValFuncCall(node *ast.FuncCallExpr) ast.ExprNode {
	seqArg := p.parseSequenceTableArg()
	if seqArg == nil {
		return nil
	}
	p.expect(',')
	valExpr := p.parseExpression(precNone)
	if valExpr == nil {
		return nil
	}
	p.expect(')')
	node.Args = []ast.ExprNode{seqArg, valExpr}
	return node
}

// parseModFuncCall parses MOD(a, b) → a % b (BinaryOperationExpr).
func (p *HandParser) parseModFuncCall() ast.ExprNode {
	leftExpr := p.parseExpression(precNone)
	if leftExpr == nil {
		return nil
	}
	p.expect(',')
	rightExpr := p.parseExpression(precNone)
	if rightExpr == nil {
		return nil
	}
	p.expect(')')
	binop := Alloc[ast.BinaryOperationExpr](p.arena)
	binop.Op = opcode.Mod
	binop.L = leftExpr
	binop.R = rightExpr
	return binop
}

// parseDateAddSubFuncCall parses ADDDATE/SUBDATE with optional INTERVAL form.
func (p *HandParser) parseDateAddSubFuncCall(node *ast.FuncCallExpr) ast.ExprNode {
	dateExpr := p.parseExpression(precNone)
	if dateExpr == nil {
		return nil
	}
	p.expect(',')
	if _, ok := p.accept(interval); ok {
		// ADDDATE(expr, INTERVAL expr unit)
		intervalExpr := p.parseExpression(precNone)
		unit := p.parseTimeUnit()
		if unit == nil {
			return nil
		}
		p.expect(')')
		node.Args = []ast.ExprNode{dateExpr, intervalExpr, unit}
	} else {
		// ADDDATE(expr, N) → ADDDATE(expr, INTERVAL N DAY)
		intervalExpr := p.parseExpression(precNone)
		p.expect(')')
		unit := &ast.TimeUnitExpr{Unit: ast.TimeUnitDay}
		node.Args = []ast.ExprNode{dateExpr, intervalExpr, unit}
	}
	return node
}

// parseCharFuncCall parses CHAR(expr [, expr ...] [USING charset_name]).
func (p *HandParser) parseCharFuncCall(node *ast.FuncCallExpr, lowerName string) ast.ExprNode {
	var args []ast.ExprNode
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
	if _, ok := p.accept(using); ok {
		charsetTok := p.next()
		cs, err := charset.GetCharsetInfo(charsetTok.Lit)
		if err != nil {
			p.errs = append(p.errs, ErrUnknownCharacterSet.
				GenWithStackByArgs(charsetTok.Lit))
			return nil
		}
		args = append(args, ast.NewValueExpr(cs.Name, "", ""))
	} else if lowerName == "char" {
		// Only add nil sentinel for the original CHAR() form, not CHAR_FUNC()
		args = append(args, ast.NewValueExpr(nil, "", ""))
	}
	p.expect(')')
	node.FnName = ast.NewCIStr("char_func")
	node.Args = args
	return node
}

// parseGetFormatFuncCall parses GET_FORMAT(DATE|TIME|DATETIME|TIMESTAMP, expr).
func (p *HandParser) parseGetFormatFuncCall(node *ast.FuncCallExpr) ast.ExprNode {
	selector := &ast.GetFormatSelectorExpr{}
	tok := p.next()
	switch tok.Tp {
	case dateType:
		selector.Selector = ast.GetFormatSelectorDate
	case timeType:
		selector.Selector = ast.GetFormatSelectorTime
	case datetimeType:
		selector.Selector = ast.GetFormatSelectorDatetime
	default:
		selector.Selector = ast.GetFormatSelectorDatetime
	}
	p.expect(',')
	arg2 := p.parseExpression(precNone)
	p.expect(')')
	node.Args = []ast.ExprNode{selector, arg2}
	return node
}

// parseWeightStringFuncCall parses WEIGHT_STRING(expr [AS {CHAR|CHARACTER|BINARY}(N)]).
func (p *HandParser) parseWeightStringFuncCall(node *ast.FuncCallExpr) ast.ExprNode {
	expr := p.parseExpression(precNone)
	if expr == nil {
		return nil
	}
	if _, ok := p.accept(as); ok {
		var typName string
		switch p.peek().Tp {
		case charType, character:
			p.next()
			typName = "CHAR"
		case binaryType:
			p.next()
			typName = "BINARY"
		}
		p.expect('(')
		lenExpr := p.parseExpression(precNone)
		p.expect(')')
		p.expect(')')
		node.Args = []ast.ExprNode{expr, ast.NewValueExpr(typName, "", ""), lenExpr}
	} else {
		p.expect(')')
		node.Args = []ast.ExprNode{expr}
	}
	return node
}

// parseTimestampAddFuncCall parses TIMESTAMPADD(unit, interval, expr).
// Note: TIMESTAMPDIFF is routed via the timestampDiff token in parsePrefixKeywordExpr,
// not through this path. Only TIMESTAMPADD reaches here.
func (p *HandParser) parseTimestampAddFuncCall(node *ast.FuncCallExpr) ast.ExprNode {
	unit := p.parseTimeUnit()
	if unit == nil {
		return nil
	}
	p.expect(',')
	arg2 := p.parseExpression(precNone)
	if arg2 == nil {
		return nil
	}
	p.expect(',')
	arg3 := p.parseExpression(precNone)
	if arg3 == nil {
		return nil
	}
	p.expect(')')
	node.Args = []ast.ExprNode{unit, arg2, arg3}
	return node
}
