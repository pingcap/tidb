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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

// parseKeywordFuncCall parses a function call where the function name is a
// SQL keyword (IF, REPLACE, COALESCE, etc.). These are not tokIdentifier,
// so they need special routing.
func (p *HandParser) parseKeywordFuncCall() ast.ExprNode {
	tok := p.next()
	var name string
	switch tok.Tp {
	case tokIf:
		name = "if"
	case tokReplace:
		name = "replace"
	case tokCoalesce:
		name = "coalesce"
	case tokInsert:
		name = "insert_func"
	default:
		p.error(tok.Offset, "unexpected keyword-function token %d", tok.Tp)
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

	// Parse optional window function modifiers:
	// [FROM LAST | FROM FIRST] [IGNORE NULLS | RESPECT NULLS]
	// Use 2-token lookahead for FROM to avoid consuming FROM in SELECT...FROM.
	var fromLast, ignoreNull bool
	if p.peek().Tp == tokFrom && (p.peekN(1).Tp == tokLast || p.peekN(1).Tp == tokFirst) {
		p.next() // consume FROM
		if _, ok := p.accept(tokLast); ok {
			fromLast = true
		} else {
			p.expect(tokFirst)
		}
	}
	if p.peek().Tp == tokIgnore && p.peekN(1).Tp == tokNulls {
		p.next() // consume IGNORE
		p.next() // consume NULLS
		ignoreNull = true
	} else if p.peek().Tp == tokRespect && p.peekN(1).Tp == tokNulls {
		p.next() // consume RESPECT
		p.next() // consume NULLS
		// RESPECT NULLS is the default, no flag needed
	}

	// Check for window function: func(...) OVER (...)
	// The scanner may tokenize OVER as either tokOver (reserved keyword) or
	// tokIdentifier (non-reserved keyword — MySQL treats OVER as non-reserved).
	if _, ok := p.accept(tokOver); ok || p.peek().IsKeyword("OVER") {
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
	return result
}

// parseWindowFuncExpr wraps a function call in a WindowFuncExpr with an OVER clause.
func (p *HandParser) parseWindowFuncExpr(name string, funcExpr ast.ExprNode) ast.ExprNode {
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
	if p.peek().Tp == tokIdentifier && p.peekN(1).Tp != tokBy {
		spec.Ref = ast.NewCIStr(p.next().Lit)
	}

	// PARTITION BY
	if _, ok := p.accept(tokPartition); ok {
		p.expect(tokBy)
		spec.PartitionBy = p.parsePartitionByClause()
	}

	// ORDER BY
	if p.peek().Tp == tokOrder {
		spec.OrderBy = p.parseOrderByClause()
	}

	// Frame clause (ROWS/RANGE/GROUPS ...)
	if p.peek().Tp == tokRows || p.peek().Tp == tokGroups || (p.peek().Tp == tokRange && p.peekN(1).Tp != ')') {
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
	case tokRows:
		p.next()
		frame.Type = ast.Rows
	case tokGroups:
		p.next()
		frame.Type = ast.Groups
	default:
		p.next() // RANGE
		frame.Type = ast.Ranges
	}

	// BETWEEN ... AND ... or single bound
	if _, ok := p.accept(tokBetween); ok {
		frame.Extent.Start = p.parseFrameBound()
		p.expect(tokAnd)
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

	if _, ok := p.accept(tokCurrent); ok {
		// CURRENT ROW — consume the ROW token (57536, distinct from ROWS 57537)
		p.next()
		bound.Type = ast.CurrentRow
		return bound
	}

	if _, ok := p.accept(tokUnbounded); ok {
		if _, ok := p.accept(tokPreceding); ok {
			bound.Type = ast.Preceding
			bound.UnBounded = true
		} else {
			p.expect(tokFollowing)
			bound.Type = ast.Following
			bound.UnBounded = true
		}
		return bound
	}

	// INTERVAL expr unit PRECEDING/FOLLOWING (for RANGE frames)
	if _, ok := p.accept(tokInterval); ok {
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
	if _, ok := p.accept(tokPreceding); ok {
		bound.Type = ast.Preceding
	} else {
		p.expect(tokFollowing)
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
			p.error(0, "%s requires at least 1 argument", name)
			return nil
		}
	}

	// Optional DISTINCT/DISTINCTROW inside aggregate.
	hasDistinctAll := false
	if _, ok := p.accept(tokDistinct); ok {
		node.Distinct = true
	} else if _, ok := p.accept(tokDistinctRow); ok {
		// DISTINCTROW is a MySQL alias for DISTINCT.
		node.Distinct = true
	}
	if node.Distinct {
		if _, ok := p.accept(tokAll); ok {
			hasDistinctAll = true
		}
	}

	// Reject DISTINCT ALL for COUNT.
	if hasDistinctAll && strings.ToLower(name) == "count" {
		p.error(0, "COUNT does not support DISTINCT ALL")
		return nil
	}

	// Reject DISTINCT for functions that don't support it.
	// Only check canonical names, not aliases (std/stddev/variance are normalized above).
	if node.Distinct {
		switch strings.ToLower(name) {
		case "bit_and", "bit_or", "bit_xor", "json_arrayagg", "json_objectagg":
			p.error(0, "%s does not support DISTINCT", name)
			return nil
		}
	}

	// Argument list.
	for {
		p.accept(tokAll)

		arg := p.parseExpression(precNone)
		if arg == nil {
			return nil
		}
		node.Args = append(node.Args, arg)
		if _, ok := p.accept(','); !ok {
			break
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
			p.error(0, "JSON_OBJECTAGG requires exactly 2 arguments")
			return nil
		}
	case "count":
		// COUNT(DISTINCT a, b) is valid but COUNT(a, b) is not.
		if len(node.Args) > 1 && !node.Distinct {
			p.error(0, "COUNT with multiple arguments requires DISTINCT")
			return nil
		}
	case "approx_count_distinct", "approx_percentile":
		// These accept multiple arguments.
	default:
		if len(node.Args) > 1 {
			p.error(0, "%s accepts at most 1 argument", name)
			return nil
		}
	}

	// GROUP_CONCAT requires special handling:
	// - optional ORDER BY clause
	// - optional SEPARATOR clause (defaults to ',')
	// AggregateFuncExpr.Restore() expects the last Args element to be the SEPARATOR value.
	if lowerName == "group_concat" {
		if _, ok := p.accept(tokOrder); ok {
			p.expect(tokBy)
			ob := Alloc[ast.OrderByClause](p.arena)
			ob.Items = p.parseByItems()
			node.Order = ob
		}
		if _, ok := p.accept(tokSeparator); ok {
			// SEPARATOR followed by a string literal — use empty charset/collation
			// to match goyacc's OptGConcatSeparator rule.
			tok := p.next()
			node.Args = append(node.Args, ast.NewValueExpr(tok.Lit, "", ""))
		} else {
			node.Args = append(node.Args, ast.NewValueExpr(",", "", ""))
		}
	}

	p.expect(')')
	return node
}

// parseScalarFuncCall parses a non-aggregate function call: CONCAT(a, b), IF(x, y, z), etc.
func (p *HandParser) parseScalarFuncCall(name string) ast.ExprNode {
	p.expect('(')

	node := Alloc[ast.FuncCallExpr](p.arena)
	node.FnName = ast.NewCIStr(name)

	// Empty arg list.
	if _, ok := p.accept(')'); ok {
		return node
	}

	lowerName := strings.ToLower(name)

	// Special: MOD(a, b) → a % b (BinaryOperationExpr), matching goyacc behavior.
	if lowerName == "mod" {
		left := p.parseExpression(precNone)
		if left == nil {
			return nil
		}
		p.expect(',')
		right := p.parseExpression(precNone)
		if right == nil {
			return nil
		}
		p.expect(')')
		binop := Alloc[ast.BinaryOperationExpr](p.arena)
		binop.Op = opcode.Mod
		binop.L = left
		binop.R = right
		return binop
	}

	// Special: ADDDATE/SUBDATE with INTERVAL form: ADDDATE(expr, INTERVAL expr unit).
	if lowerName == "adddate" || lowerName == "subdate" {
		dateExpr := p.parseExpression(precNone)
		if dateExpr == nil {
			return nil
		}
		p.expect(',')
		// Check for INTERVAL keyword.
		if _, ok := p.accept(tokInterval); ok {
			intervalExpr := p.parseExpression(precNone)
			unit := p.parseTimeUnit()
			if unit == nil {
				return nil
			}
			p.expect(')')
			node.Args = []ast.ExprNode{dateExpr, intervalExpr, unit}
		} else {
			// Non-INTERVAL form: ADDDATE(expr, N) → ADDDATE(expr, INTERVAL N DAY)
			intervalExpr := p.parseExpression(precNone)
			p.expect(')')
			unit := &ast.TimeUnitExpr{Unit: ast.TimeUnitDay}
			node.Args = []ast.ExprNode{dateExpr, intervalExpr, unit}
		}
		return node
	}

	// Special: CHAR(expr [, expr ...] USING charset_name)
	if lowerName == "char" || lowerName == "char_func" {
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
		// Check for USING keyword.
		if _, ok := p.accept(tokUsing); ok {
			charsetTok := p.next()
			charsetName := strings.ToLower(charsetTok.Lit)
			if !charset.ValidCharsetAndCollation(charsetName, "") {
				p.errs = append(p.errs, terror.ClassParser.NewStd(mysql.ErrUnknownCharacterSet).GenWithStack("Unknown character set: '%s'", charsetTok.Lit))
				return nil
			}
			args = append(args, ast.NewValueExpr(charsetName, "", ""))
		} else if lowerName == "char" {
			// Only add nil sentinel for the original CHAR() form, not CHAR_FUNC()
			// which already includes the NULL arg in the restored SQL.
			args = append(args, ast.NewValueExpr(nil, "", ""))
		}
		p.expect(')')
		node.FnName = ast.NewCIStr("char_func")
		node.Args = args
		return node
	}

	// Special: GET_FORMAT(DATE|TIME|DATETIME|TIMESTAMP, expr)
	if lowerName == "get_format" {
		selector := &ast.GetFormatSelectorExpr{}
		tok := p.next()
		switch tok.Tp {
		case tokDateType:
			selector.Selector = ast.GetFormatSelectorDate
		case tokTimeType:
			selector.Selector = ast.GetFormatSelectorTime
		case tokDatetimeType:
			selector.Selector = ast.GetFormatSelectorDatetime
		default:
			// TIMESTAMP is also accepted
			selector.Selector = ast.GetFormatSelectorDatetime
		}
		p.expect(',')
		arg2 := p.parseExpression(precNone)
		p.expect(')')
		node.Args = []ast.ExprNode{selector, arg2}
		return node
	}

	// Special: EXTRACT(unit FROM expr)
	if lowerName == "extract" {
		unit := p.parseTimeUnit()
		if unit == nil {
			return nil
		}
		p.expect(tokFrom)
		expr := p.parseExpression(precNone)
		p.expect(')')
		node.Args = []ast.ExprNode{unit, expr}
		return node
	}

	// Special: POSITION(substr IN str)
	if lowerName == "position" {
		substr := p.parseExpression(precNone)
		p.expect(tokIn)
		str := p.parseExpression(precNone)
		p.expect(')')
		node.Args = []ast.ExprNode{substr, str}
		return node
	}

	// Special: WEIGHT_STRING(expr [AS {CHAR|CHARACTER|BINARY}(N)])
	if lowerName == "weight_string" {
		expr := p.parseExpression(precNone)
		if expr == nil {
			return nil
		}
		if _, ok := p.accept(tokAs); ok {
			// AS CHAR(N) or AS CHARACTER(N) or AS BINARY(N)
			var typName string
			switch p.peek().Tp {
			case tokCharType, tokCharacter:
				p.next()
				typName = "CHAR"
			case tokBinaryType:
				p.next()
				typName = "BINARY"
			}
			// Parse (N) for the type length.
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

	// Special: TIMESTAMPADD(unit, interval, expr) / TIMESTAMPDIFF(unit, expr1, expr2)
	if lowerName == "timestampadd" || lowerName == "timestampdiff" {
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

	// Argument list.
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
