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
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// parsePrefixKeywordExpr handles prefix expressions starting with keywords (e.g. CASE, INTERVAL, Functions).
func (p *HandParser) parsePrefixKeywordExpr(minPrec int) ast.ExprNode { //revive:disable-line
	tok := p.peek()
	switch tok.Tp {
	case exists:
		return p.parseExistsSubquery()

	case caseKwd:
		return p.parseCaseExpr()

	case defaultKwd:
		return p.parseDefaultExpr()

	case row:
		// ROW(expr, expr, ...) — explicit row constructor (requires 2+ elements).
		p.next()
		p.expect('(')
		row := Alloc[ast.RowExpr](p.arena)
		for {
			e := p.parseExpression(precNone)
			if e == nil {
				return nil
			}
			row.Values = append(row.Values, e)
			if _, ok := p.accept(','); !ok {
				break
			}
		}
		p.expect(')')
		if len(row.Values) < 2 {
			p.error(p.peek().Offset, "ROW constructor requires at least 2 elements")
			return nil
		}
		return row

	case maxValue:
		p.next()
		return &ast.MaxValueExpr{}

	case match:
		return p.parseMatchAgainstExpr()

	case values:
		// VALUES(column) or VALUES(table.column) in ON DUPLICATE KEY UPDATE context.
		p.next()
		p.expect('(')
		col := &ast.ColumnNameExpr{Name: &ast.ColumnName{}}
		nameTok := p.next()
		col.Name.Name = ast.NewCIStr(nameTok.Lit)
		// Support qualified form: VALUES(db.t.col) or VALUES(t.col)
		if _, ok := p.accept('.'); ok {
			nextTok := p.next()
			if _, ok2 := p.accept('.'); ok2 {
				// schema.table.column
				thirdTok := p.next()
				col.Name.Schema = col.Name.Name
				col.Name.Table = ast.NewCIStr(nextTok.Lit)
				col.Name.Name = ast.NewCIStr(thirdTok.Lit)
			} else {
				// table.column
				col.Name.Table = col.Name.Name
				col.Name.Name = ast.NewCIStr(nextTok.Lit)
			}
		}
		p.expect(')')
		return &ast.ValuesExpr{Column: col}

	case '*':
		// Wildcard in select list — handled at statement level.
		// This should only be reached in expression context for table.*.
		return nil

	case singleAtIdentifier, doubleAtIdentifier:
		return p.parseVariableExpr()

	case currentDate, currentTime, currentTs, currentUser, currentRole, localTime, localTs, curDate, curTime:
		return p.parseCurrentFunc()

	case builtinFnCast:
		return p.tryBuiltinFunc(p.parseCastFunc)

	case builtinFnExtract:
		return p.tryBuiltinFunc(p.parseExtractFunc)

	case builtinFnTrim:
		return p.tryBuiltinFunc(p.parseTrimFunc)

	case builtinFnPosition:
		return p.tryBuiltinFunc(p.parsePositionFunc)

	case convert:
		return p.tryBuiltinFunc(p.parseConvertFunc)

	case binaryType:
		// BINARY expr → FuncCastExpr with binary charset (per parser.y:8242-8253).
		// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#operator_binary
		p.next()
		expr := p.parseExpression(precUnary)
		if expr != nil {
			tp := types.NewFieldType(mysql.TypeString)
			tp.SetCharset(charset.CharsetBin)
			tp.SetCollate(charset.CharsetBin)
			tp.AddFlag(mysql.BinaryFlag)
			return &ast.FuncCastExpr{
				Expr:         expr,
				Tp:           tp,
				FunctionType: ast.CastBinaryOperator,
			}
		}
		// Fallback to identifier if not a binary expression.
		return &ast.ColumnNameExpr{Name: &ast.ColumnName{Name: ast.NewCIStr("binary")}}

	case timestampDiff:
		return p.tryBuiltinFunc(p.parseTimestampDiffFunc)

	// Keywords that are also valid as function names in expression context.
	case ifKwd, replace, coalesce, insert:
		return p.parseKeywordFuncCall()

	case timeType, timestampType, dateType:
		var tp string
		switch p.peek().Tp {
		case timeType:
			tp = ast.TimeLiteral
		case timestampType:
			tp = ast.TimestampLiteral
		default:
			tp = ast.DateLiteral
		}
		return p.parsePrefixTimeLiteral(tp)

	case EOF:
		return nil

	// NowSymFunc: NOW(), CURRENT_TIMESTAMP(), LOCALTIME(), LOCALTIMESTAMP()
	// Originally, these all produce FnName "CURRENT_TIMESTAMP" (canonical name).
	// The scanner may produce either builtinFnNow or now depending on context.
	case builtinFnNow, now, builtinFnCurTime, builtinSysDate:
		return p.tryBuiltinFunc(p.parseOptPrecisionFunc)

	case builtinFnCurDate:
		return p.tryBuiltinFunc(p.parseCurDateFunc)

	// DATE_ADD / DATE_SUB with INTERVAL syntax.
	case builtinFnDateAdd, builtinFnDateSub:
		return p.tryBuiltinFunc(p.parseDateArithFunc)

	// SUBSTRING/SUBSTR with FROM/FOR and comma forms.
	case builtinFnSubstring:
		return p.tryBuiltinFunc(p.parseSubstringFunc)

	// JSON_SUM_CRC32(expr AS type)
	case jsonSumCrc32:
		return p.tryBuiltinFunc(p.parseJsonSumCrc32Func)

	// CHAR(expr, ...) - must route through parseScalarFuncCall for USING/NULL-sentinel handling.
	case charType, character:
		if p.peekN(1).Tp == '(' {
			p.next() // consume the CHAR/CHARACTER token
			return p.parseScalarFuncCall(tok.Lit)
		}
		return p.parseIdentOrFuncCall()

	// INTERVAL expr unit + expr → DATE_ADD(expr, INTERVAL expr unit)
	// INTERVAL(N, N1, N2, ...) → comparison function (handled by generic path)
	case interval:
		if p.peekN(1).Tp != '(' {
			p.next() // consume INTERVAL
			intervalExpr := p.parseExpression(precNone)
			if intervalExpr == nil {
				return nil
			}
			unit := p.parseTimeUnit()
			if unit == nil {
				return nil
			}
			// Expect '+' then date expression.
			// yacc: INTERVAL Expression TimeUnit '+' BitExpr
			if _, ok := p.expect('+'); !ok {
				return nil
			}
			dateExpr := p.parseExpression(precPredicate + 1) // BitExpr level
			if dateExpr == nil {
				return nil
			}
			return &ast.FuncCallExpr{
				FnName: ast.NewCIStr("DATE_ADD"),
				Args:   []ast.ExprNode{dateExpr, intervalExpr, unit},
			}
		}
		return p.parseIdentOrFuncCall()

	default:
		if tok.Tp == invalid {
			p.syntaxErrorAt(tok)
			return nil
		}
		// Check if this is a builtin function token (COUNT, SUM, MAX, etc.).
		if builtinFuncName(tok.Tp) != "" {
			p.next() // consume the builtin token
			// Use tok.Lit (original identifier) as the function name,
			// matching the grammar which uses $1 (the ident field).
			return p.parseFuncCall(tok.Lit)
		}
		// NEXT VALUE FOR seq_name → NEXTVAL(seq)
		if tok.Tp == next && p.peekN(1).Tp == value && p.peekN(2).Tp == forKwd {
			p.next() // consume NEXT
			p.next() // consume VALUE
			p.next() // consume FOR
			seqArg := p.parseSequenceTableArg()
			if seqArg == nil {
				return nil
			}
			node := p.arena.AllocFuncCallExpr()
			node.FnName = ast.NewCIStr("nextval")
			node.Args = []ast.ExprNode{seqArg}
			return node
		}
		// Fallback: any keyword token (Tp >= identifier) can be used in
		// expression context. Reserved clause-introducing keywords (FROM, WHERE,
		// etc.) must NOT be consumed — they terminate the current expression.
		if tok.Tp >= identifier && !isReservedClauseKeyword(tok.Tp) {
			if p.peekN(1).Tp == '(' {
				// keyword followed by '(' → function call (e.g., LEFT(...))
				p.next() // consume the keyword token
				return p.parseFuncCall(tok.Lit)
			}
			// Bare keyword → treat as column name only for unreserved keywords.
			// Reserved keywords (OF, RANGE, etc.) cannot be bare identifiers.
			if isIdentLike(tok.Tp) {
				return p.parseIdentOrFuncCall()
			}
		}
		p.syntaxErrorAt(tok)
		return nil
	}
}

// parsePrefixTimeLiteral parses DATE '...', TIME '...', TIMESTAMP '...'.
func (p *HandParser) parsePrefixTimeLiteral(fnName string) ast.ExprNode {
	if p.peekN(1).Tp == stringLit {
		p.next()        // consume type token
		lit := p.next() // consume string literal
		node := &ast.FuncCallExpr{
			FnName: ast.NewCIStr(fnName),
			Args:   []ast.ExprNode{ast.NewValueExpr(lit.Lit, "", "")},
		}
		return node
	}
	return p.parseIdentOrFuncCall()
}

// isReservedClauseKeyword returns true for SQL reserved keywords that introduce
// clauses and must NOT be consumed as bare identifiers in expression context.
// These keywords terminate field lists and expression parsing. They can still
// be used as identifiers when backtick-quoted (the lexer emits identifier
// for quoted names).
func isReservedClauseKeyword(tp int) bool {
	switch tp {
	case from, where, group, order, limit,
		having, union, into, forKwd, lock,
		selectKwd, set, on:
		return true
	}
	return false
}
