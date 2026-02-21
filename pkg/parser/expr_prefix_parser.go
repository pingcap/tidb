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
func (p *HandParser) parsePrefixKeywordExpr(minPrec int) ast.ExprNode {
	tok := p.peek()
	switch tok.Tp {
	case 57422:
		return p.parseExistsSubquery()

	case 57379:
		return p.parseCaseExpr()

	case 57405:
		return p.parseDefaultExpr()

	case 57536:
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

	case 57489:
		p.next()
		return &ast.MaxValueExpr{}

	case 57488:
		return p.parseMatchAgainstExpr()

	case 57580:
		// VALUES(column) in ON DUPLICATE KEY UPDATE context.
		p.next()
		p.expect('(')
		col := &ast.ColumnNameExpr{Name: &ast.ColumnName{}}
		nameTok := p.next()
		col.Name.Name = ast.NewCIStr(nameTok.Lit)
		p.expect(')')
		return &ast.ValuesExpr{Column: col}

	case '*':
		// Wildcard in select list — handled at statement level.
		// This should only be reached in expression context for table.*.
		return nil

	case 57354, 57355:
		return p.parseVariableExpr()

	case 57392, 57394, 57395, 57396, 57393:
		return p.parseCurrentFunc()

	case builtinFnCast:
		return p.tryBuiltinFunc(p.parseCastFunc)

	case builtinFnExtract:
		return p.tryBuiltinFunc(p.parseExtractFunc)

	case builtinFnTrim:
		return p.tryBuiltinFunc(p.parseTrimFunc)

	case builtinFnPosition:
		return p.tryBuiltinFunc(p.parsePositionFunc)

	case 57388:
		return p.tryBuiltinFunc(p.parseConvertFunc)

	case 57373:
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

	case 58095:
		return p.tryBuiltinFunc(p.parseTimestampDiffFunc)

	// Keywords that are also valid as function names in expression context.
	case 57445, 57530, 57650, 57453:
		return p.parseKeywordFuncCall()

	case 57952, 57954, 57680:
		var tp string
		switch p.peek().Tp {
		case 57952:
			tp = ast.TimeLiteral
		case 57954:
			tp = ast.TimestampLiteral
		default:
			tp = ast.DateLiteral
		}
		return p.parsePrefixTimeLiteral(tp)

	case EOF:
		return nil

	// NowSymFunc: NOW(), CURRENT_TIMESTAMP(), LOCALTIME(), LOCALTIMESTAMP()
	// Originally, these all produce FnName "CURRENT_TIMESTAMP" (canonical name).
	// The scanner may produce either builtinFnNow or 58052 depending on context.
	case builtinFnNow, 58052, builtinFnCurTime:
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
	case 58038:
		return p.tryBuiltinFunc(p.parseJsonSumCrc32Func)

	// CHAR(expr, ...) - must route through parseScalarFuncCall for USING/NULL-sentinel handling.
	case 57381, 57382:
		if p.peekN(1).Tp == '(' {
			p.next() // consume the CHAR/CHARACTER token
			return p.parseScalarFuncCall(tok.Lit)
		}
		return p.parseIdentOrFuncCall()

	// INTERVAL expr unit + expr → DATE_ADD(expr, INTERVAL expr unit)
	// INTERVAL(N, N1, N2, ...) → comparison function (handled by generic path)
	case 57462:
		if p.peekN(1).Tp != '(' {
			p.next() // consume INTERVAL
			intervalExpr := p.parseExpression(precNone)
			unit := p.parseTimeUnit()
			// Expect '+' then date expression
			p.expect('+')
			dateExpr := p.parseExpression(precNone)
			return &ast.FuncCallExpr{
				FnName: ast.NewCIStr("DATE_ADD"),
				Args:   []ast.ExprNode{dateExpr, intervalExpr, unit},
			}
		}
		return p.parseIdentOrFuncCall()

	default:
		if tok.Tp == 57356 {
			p.error(tok.Offset, "invalid token")
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
		if tok.Tp == 57797 && p.peekN(1).Tp == 57977 && p.peekN(2).Tp == 57431 {
			p.next() // consume NEXT
			p.next() // consume VALUE
			p.next() // consume FOR
			// Parse the sequence name as an expression to handle schema-qualified names (test.seq).
			seqExpr := p.parseExpression(precNone)
			return &ast.FuncCallExpr{
				FnName: ast.NewCIStr("nextval"),
				Args:   []ast.ExprNode{seqExpr},
			}
		}
		// Fallback: any keyword token (Tp >= 57346) can be used as an
		// identifier in expression context. MySQL allows most non-reserved keywords
		// as column/table names. However, reserved clause-introducing keywords
		// (FROM, WHERE, etc.) must NOT be consumed as identifiers — they terminate
		// the current expression/field list.
		if tok.Tp >= 57346 && !isReservedClauseKeyword(tok.Tp) {
			if p.peekN(1).Tp == '(' {
				// keyword followed by '(' → function call (e.g., AVG(...))
				p.next() // consume the keyword token
				return p.parseFuncCall(tok.Lit)
			}
			// Bare keyword → treat as column name reference (e.g., subject, score)
			return p.parseIdentOrFuncCall()
		}
		tokLen := len(tok.Lit)
		if tokLen == 0 {
			tokLen = 1 // single-char tokens have empty Lit
		}
		p.errorNear(tok.Offset+tokLen, tok.Offset)
		return nil
	}
}

// parsePrefixTimeLiteral parses DATE '...', TIME '...', TIMESTAMP '...'.
func (p *HandParser) parsePrefixTimeLiteral(fnName string) ast.ExprNode {
	if p.peekN(1).Tp == 57353 {
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
// be used as identifiers when backtick-quoted (the lexer emits 57346
// for quoted names).
func isReservedClauseKeyword(tp int) bool {
	switch tp {
	case 57434, 57587, 57438, 57510, 57477,
		57440, 57568, 57463, 57431, 57483,
		57540, 57541, 57505:
		return true
	}
	return false
}
