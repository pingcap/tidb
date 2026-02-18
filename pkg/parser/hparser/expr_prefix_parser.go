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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// parsePrefixKeywordExpr handles prefix expressions starting with keywords (e.g. CASE, INTERVAL, Functions).
func (p *HandParser) parsePrefixKeywordExpr(minPrec int) ast.ExprNode {
	tok := p.peek()
	switch tok.Tp {
	case tokExists:
		return p.parseExistsSubquery()

	case tokCase:
		return p.parseCaseExpr()

	case tokDefault:
		return p.parseDefaultExpr()

	case tokRow:
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

	case tokMaxValue:
		p.next()
		return &ast.MaxValueExpr{}

	case tokMatch:
		return p.parseMatchAgainstExpr()

	case tokValues:
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

	case tokSingleAtIdentifier, tokDoubleAtIdentifier:
		return p.parseVariableExpr()

	case tokCurrentDate, tokCurrentTime, tokCurrentTs, tokCurrentUser, tokCurrentRole:
		return p.parseCurrentFunc()

	case builtinCast:
		return p.tryBuiltinFunc(p.parseCastFunc)

	case builtinExtract:
		return p.tryBuiltinFunc(p.parseExtractFunc)

	case builtinTrim:
		return p.tryBuiltinFunc(p.parseTrimFunc)

	case builtinPosition:
		return p.tryBuiltinFunc(p.parsePositionFunc)

	case tokConvert:
		return p.tryBuiltinFunc(p.parseConvertFunc)

	case tokBinaryType:
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

	case tokTimestampDiff:
		return p.tryBuiltinFunc(p.parseTimestampDiffFunc)

	// Keywords that are also valid as function names in expression context.
	case tokIf, tokReplace, tokCoalesce, tokInsert:
		return p.parseKeywordFuncCall()

	case tokTimeType, tokTimestampType, tokDateType:
		var tp string
		switch p.peek().Tp {
		case tokTimeType:
			tp = ast.TimeLiteral
		case tokTimestampType:
			tp = ast.TimestampLiteral
		default:
			tp = ast.DateLiteral
		}
		return p.parsePrefixTimeLiteral(tp)

	case EOF:
		return nil

	// NowSymFunc: NOW(), CURRENT_TIMESTAMP(), LOCALTIME(), LOCALTIMESTAMP()
	// In goyacc, these all produce FnName "CURRENT_TIMESTAMP" (canonical name).
	// The scanner may produce either builtinNow or tokNow depending on context.
	case builtinNow, tokNow, builtinCurTime:
		return p.tryBuiltinFunc(p.parseOptPrecisionFunc)

	case builtinCurDate:
		return p.tryBuiltinFunc(p.parseCurDateFunc)

	// DATE_ADD / DATE_SUB with INTERVAL syntax.
	case builtinDateAdd, builtinDateSub:
		return p.tryBuiltinFunc(p.parseDateArithFunc)

	// SUBSTRING/SUBSTR with FROM/FOR and comma forms.
	case builtinSubstring:
		return p.tryBuiltinFunc(p.parseSubstringFunc)

	// JSON_SUM_CRC32(expr AS type)
	case tokJsonSumCrc32:
		return p.tryBuiltinFunc(p.parseJsonSumCrc32Func)

	// CHAR(expr, ...) - must route through parseScalarFuncCall for USING/NULL-sentinel handling.
	case tokCharType, tokCharacter:
		if p.peekN(1).Tp == '(' {
			p.next() // consume the CHAR/CHARACTER token
			return p.parseScalarFuncCall(tok.Lit)
		}
		return p.parseIdentOrFuncCall()

	// INTERVAL expr unit + expr → DATE_ADD(expr, INTERVAL expr unit)
	// INTERVAL(N, N1, N2, ...) → comparison function (handled by generic path)
	case tokInterval:
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
		if tok.Tp == tokInvalid {
			p.error(tok.Offset, "invalid token")
			return nil
		}
		// Check if this is a builtin function token (COUNT, SUM, MAX, etc.).
		if builtinFuncName(tok.Tp) != "" {
			p.next() // consume the builtin token
			// Use tok.Lit (original identifier) as the function name,
			// matching goyacc which uses $1 (the ident field).
			return p.parseFuncCall(tok.Lit)
		}
		// NEXT VALUE FOR seq_name → NEXTVAL(seq)
		if tok.Tp == tokNext && p.peekN(1).Tp == tokValue && p.peekN(2).Tp == tokFor {
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
		// Fallback: any keyword token (Tp >= tokIdentifier) can be used as an
		// identifier in expression context. MySQL allows most non-reserved keywords
		// as column/table names. However, reserved clause-introducing keywords
		// (FROM, WHERE, etc.) must NOT be consumed as identifiers — they terminate
		// the current expression/field list.
		if tok.Tp >= tokIdentifier && !isReservedClauseKeyword(tok.Tp) {
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
	if p.peekN(1).Tp == tokStringLit {
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
// be used as identifiers when backtick-quoted (the lexer emits tokIdentifier
// for quoted names).
func isReservedClauseKeyword(tp int) bool {
	switch tp {
	case tokFrom, tokWhere, tokGroup, tokOrder, tokLimit,
		tokHaving, tokUnion, tokInto, tokFor, tokLock,
		tokSelect, tokSet, tokOn:
		return true
	}
	return false
}
