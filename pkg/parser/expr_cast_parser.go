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
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// ---------------------------------------------------------------------------
// CAST / CONVERT / type-related function parsers
// ---------------------------------------------------------------------------

func (p *HandParser) parseCurrentFunc() ast.ExprNode {
	tok := p.next()

	node := p.arena.AllocFuncCallExpr()

	// Map token to canonical function name.
	switch tok.Tp {
	case currentTs:
		node.FnName = ast.NewCIStr("CURRENT_TIMESTAMP")
	case currentDate:
		node.FnName = ast.NewCIStr("CURRENT_DATE")
	case currentTime:
		node.FnName = ast.NewCIStr("CURRENT_TIME")
	case currentUser:
		node.FnName = ast.NewCIStr("CURRENT_USER")
	case currentRole:
		node.FnName = ast.NewCIStr("CURRENT_ROLE")
	default:
		p.error(tok.Offset, "unexpected current-func token %d", tok.Tp)
		return nil
	}

	// Optional parentheses with optional precision argument.
	if _, ok := p.accept('('); ok {
		if p.peek().Tp == intLit {
			arg := p.parseLiteral()
			if arg == nil {
				return nil
			}
			node.Args = []ast.ExprNode{arg}
		}
		p.expect(')')
	}

	return node
}

// parseCastFunc parses CAST(expr AS cast_type).
// Produces ast.FuncCastExpr with FunctionType = CastFunction.
func (p *HandParser) parseCastFunc() ast.ExprNode {
	p.next() // consume CAST
	expr, tp, explicitCharset := p.parseCastExprAndType()
	if tp == nil {
		return nil
	}
	return &ast.FuncCastExpr{
		Expr:            expr,
		Tp:              tp,
		FunctionType:    ast.CastFunction,
		ExplicitCharSet: explicitCharset,
	}
}

// parseConvertFunc parses CONVERT(expr, type) or CONVERT(expr USING charset).
// Produces ast.FuncCastExpr with FunctionType = CastConvertFunction.
func (p *HandParser) parseConvertFunc() ast.ExprNode {
	p.next() // consume CONVERT
	p.expect('(')

	expr := p.parseExpression(0)
	if expr == nil {
		return nil
	}

	// CONVERT(expr USING charset_name)
	if _, ok := p.accept(using); ok {
		charsetTok := p.next()
		charsetName := strings.ToLower(charsetTok.Lit)
		if !charset.ValidCharsetAndCollation(charsetName, "") {
			p.errs = append(p.errs, terror.ClassParser.NewStd(mysql.ErrUnknownCharacterSet).GenWithStack("Unknown character set: '%s'", charsetTok.Lit))
			return nil
		}
		p.expect(')')
		// Produce FuncCallExpr : FnName="convert", Args=[expr, charsetValueExpr]
		return &ast.FuncCallExpr{
			FnName: ast.NewCIStr("convert"),
			Args:   []ast.ExprNode{expr, ast.NewValueExpr(charsetName, "", "")},
		}
	}

	p.expect(',')

	tp, explicitCharset := p.parseCastType()
	if tp == nil {
		return nil
	}

	p.expect(')')

	node := &ast.FuncCastExpr{
		Expr:            expr,
		Tp:              tp,
		FunctionType:    ast.CastConvertFunction,
		ExplicitCharSet: explicitCharset,
	}
	return node
}

// parseTimestampDiffFunc parses TIMESTAMPDIFF(unit, dt_expr1, dt_expr2).
func (p *HandParser) parseTimestampDiffFunc() ast.ExprNode {
	p.next() // consume TIMESTAMPDIFF
	p.expect('(')

	unit := p.parseTimeUnit()
	if unit == nil {
		return nil
	}

	// TIMESTAMPDIFF only accepts single time units (not compound like SECOND_MICROSECOND).
	switch unit.Unit {
	case ast.TimeUnitMicrosecond, ast.TimeUnitSecond, ast.TimeUnitMinute,
		ast.TimeUnitHour, ast.TimeUnitDay, ast.TimeUnitWeek,
		ast.TimeUnitMonth, ast.TimeUnitQuarter, ast.TimeUnitYear:
		// valid
	default:
		p.error(p.peek().Offset, "TIMESTAMPDIFF does not support interval unit '%s'", unit.Unit)
		return nil
	}

	p.expect(',')
	dt1 := p.parseExpression(0)
	if dt1 == nil {
		return nil
	}

	p.expect(',')
	dt2 := p.parseExpression(0)
	if dt2 == nil {
		return nil
	}

	p.expect(')')

	node := &ast.FuncCallExpr{
		FnName: ast.NewCIStr("timestampdiff"),
		Args:   []ast.ExprNode{unit, dt1, dt2},
	}
	return node
}

// parseCastType parses the target type in CAST(expr AS <type>).
// Returns the FieldType and whether an explicit charset was specified.
// parseCastType parses the target type in CAST(expr AS <type>).
// Returns the FieldType and whether an explicit charset was specified.
func (p *HandParser) parseCastType() (*types.FieldType, bool) {
	tp, explicit := p.parseCastTypeInternal()
	if tp != nil && p.peek().Tp == array {
		p.next()
		tp.SetArray(true)
	}
	return tp, explicit
}

// setBinaryCastType sets the charset, collation, and binary flag for a CAST type.
// This consolidates the 3-line pattern repeated across most CAST type cases.
func setBinaryCastType(tp *types.FieldType) {
	tp.SetCharset(charset.CharsetBin)
	tp.SetCollate(charset.CollationBin)
	tp.AddFlag(mysql.BinaryFlag)
}

// parseCastTypeInternal parses the target type in CAST(expr AS <type>).
// Returns the FieldType and whether an explicit charset was specified.
func (p *HandParser) parseCastTypeInternal() (*types.FieldType, bool) {
	tok := p.peek()
	switch tok.Tp {
	case binaryType:
		p.next()
		tp := types.NewFieldType(mysql.TypeVarString)
		tp.SetFlen(p.parseOptFieldLen())
		if tp.GetFlen() != types.UnspecifiedLength {
			tp.SetType(mysql.TypeString)
		}
		setBinaryCastType(tp)
		return tp, false

	case charType, character:
		p.next()
		tp := types.NewFieldType(mysql.TypeVarString)
		tp.SetFlen(p.parseOptFieldLen())
		// Check for BINARY suffix: CHAR(N) BINARY
		if _, ok := p.accept(binaryType); ok {
			if tp.GetFlen() != types.UnspecifiedLength {
				tp.SetType(mysql.TypeString)
			}
			setBinaryCastType(tp)
			return tp, false
		}
		// Check for CHARACTER SET / CHARSET / CHAR SET (yacc CharsetKw)
		hasCharset := p.acceptCharsetKw()
		if hasCharset {
			csTok := p.next()
			csName := strings.ToUpper(csTok.Lit)
			if !charset.ValidCharsetAndCollation(csName, "") {
				p.errs = append(p.errs, terror.ClassParser.NewStd(mysql.ErrUnknownCharacterSet).GenWithStack("Unknown character set: '%s'", csName))
				return nil, false
			}
			tp.SetCharset(csName)
			// Look up default collation for this charset.
			defCollation, err := charset.GetDefaultCollation(csName)
			if err == nil {
				tp.SetCollate(defCollation)
			}
			return tp, true
		}
		tp.SetCharset(p.charset)
		tp.SetCollate(p.collation)
		return tp, false

	case dateType:
		p.next()
		tp := types.NewFieldType(mysql.TypeDate)
		flen, dec := mysql.GetDefaultFieldLengthAndDecimalForCast(mysql.TypeDate)
		tp.SetFlen(flen)
		tp.SetDecimal(dec)
		setBinaryCastType(tp)
		return tp, false

	case yearType:
		p.next()
		tp := types.NewFieldType(mysql.TypeYear)
		setBinaryCastType(tp)
		return tp, false

	case datetimeType, timeType:
		p.next()
		var mysqlType byte
		if tok.Tp == datetimeType {
			mysqlType = mysql.TypeDatetime
		} else {
			mysqlType = mysql.TypeDuration
		}
		tp := types.NewFieldType(mysqlType)
		flen, _ := mysql.GetDefaultFieldLengthAndDecimalForCast(mysqlType)
		tp.SetFlen(flen)
		tp.SetDecimal(p.parseOptFieldLen())
		if tp.GetDecimal() > 0 {
			tp.SetFlen(tp.GetFlen() + 1 + tp.GetDecimal())
		}
		setBinaryCastType(tp)
		return tp, false

	case decimalType:
		p.next()
		tp := types.NewFieldType(mysql.TypeNewDecimal)
		flen, dec := p.parseFloatOpt()
		if flen == types.UnspecifiedLength {
			// Apply MySQL defaults: bare DECIMAL defaults to DECIMAL(10, 0).
			flen, dec = mysql.GetDefaultFieldLengthAndDecimalForCast(mysql.TypeNewDecimal)
		}
		tp.SetFlen(flen)
		tp.SetDecimal(dec)
		setBinaryCastType(tp)
		return tp, false

	case signed, unsigned:
		isUnsigned := tok.Tp == unsigned
		p.next()
		// Accept optional INTEGER/INT keyword.
		if _, ok := p.accept(intType); !ok {
			if isUnsigned {
				p.accept(integerType)
			}
		}
		tp := types.NewFieldType(mysql.TypeLonglong)
		if isUnsigned {
			tp.AddFlag(mysql.UnsignedFlag)
		}
		setBinaryCastType(tp)
		return tp, false

	case jsonType:
		p.next()
		tp := types.NewFieldType(mysql.TypeJSON)
		flen, dec := mysql.GetDefaultFieldLengthAndDecimalForCast(mysql.TypeJSON)
		tp.SetFlen(flen)
		tp.SetDecimal(dec)
		tp.AddFlag(mysql.BinaryFlag | mysql.ParseToJSONFlag)
		tp.SetCharset(mysql.DefaultCharset)
		tp.SetCollate(mysql.DefaultCollationName)
		return tp, false

	case doubleType, realType:
		isReal := tok.Tp == realType
		p.next()
		tp := types.NewFieldType(mysql.TypeDouble)
		if isReal && p.sqlMode.HasRealAsFloatMode() {
			tp = types.NewFieldType(mysql.TypeFloat)
		}
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimalForCast(tp.GetType())
		tp.SetFlen(flen)
		tp.SetDecimal(decimal)
		setBinaryCastType(tp)
		return tp, false

	case floatType:
		p.next()
		tp := types.NewFieldType(mysql.TypeFloat)
		flen, _ := p.parseFloatOpt()
		if flen >= 54 {
			p.error(tok.Offset, "precision %d too big for CAST (max 53)", flen)
			return nil, false
		}
		if flen >= 25 {
			tp = types.NewFieldType(mysql.TypeDouble)
		}
		// Always use default flen/decimal for CAST, as expected.
		defFlen, defDec := mysql.GetDefaultFieldLengthAndDecimalForCast(tp.GetType())
		tp.SetFlen(defFlen)
		tp.SetDecimal(defDec)
		setBinaryCastType(tp)
		return tp, false

	case vectorType:
		p.next()
		// OptVectorElementType: <FLOAT> or <DOUBLE>
		if p.peek().Tp == '<' {
			p.next()
			switch p.peek().Tp {
			case floatType, float4Type:
				p.next()
			case doubleType, float8Type:
				p.next()
				p.errs = append(p.errs, fmt.Errorf("Only VECTOR is supported for now"))
				p.expect('>')
				return nil, false
			default:
				p.error(p.peek().Offset, "expected FLOAT or DOUBLE inside VECTOR")
				return nil, false
			}
			p.expect('>')
		}
		tp := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
		tp.SetFlen(p.parseOptFieldLen())
		tp.SetDecimal(0)
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
		return tp, false

	default:
		p.error(tok.Offset, "expected cast type")
		return nil, false
	}
}

// parseJsonSumCrc32Func parses JSON_SUM_CRC32(expr AS type).
func (p *HandParser) parseJsonSumCrc32Func() ast.ExprNode {
	p.next() // consume JSON_SUM_CRC32
	expr, tp, explicitCharset := p.parseCastExprAndType()
	if tp == nil {
		return nil
	}

	if !tp.IsArray() {
		p.error(p.peek().Offset, "JSON_SUM_CRC32 requires ARRAY type")
		return nil
	}

	return &ast.JSONSumCrc32Expr{
		Expr:            expr,
		Tp:              tp,
		ExplicitCharSet: explicitCharset,
	}
}

// parseCastExprAndType parses the shared body: '(' expr AS cast_type ')'.
// Used by CAST, CONVERT, and JSON_SUM_CRC32.
func (p *HandParser) parseCastExprAndType() (ast.ExprNode, *types.FieldType, bool) {
	p.expect('(')
	expr := p.parseExpression(0)
	if expr == nil {
		return nil, nil, false
	}
	p.expect(as)
	tp, explicitCharset := p.parseCastType()
	if tp == nil {
		return nil, nil, false
	}
	p.expect(')')
	return expr, tp, explicitCharset
}

// parseOptFieldLen parses an optional (N) length. Returns UnspecifiedLength if no parens.
func (p *HandParser) parseOptFieldLen() int {
	if _, ok := p.accept('('); !ok {
		return types.UnspecifiedLength
	}
	tok := p.next()
	if tok.Tp != intLit {
		p.error(tok.Offset, "expected integer in field length")
		return types.UnspecifiedLength
	}
	val := tok.Item.(int64)
	p.expect(')')
	return int(val)
}

// parseFloatOpt parses optional (M) or (M,D) for DECIMAL/FLOAT types.
func (p *HandParser) parseFloatOpt() (flen, decimal int) {
	if _, ok := p.accept('('); !ok {
		return types.UnspecifiedLength, types.UnspecifiedLength
	}
	tok := p.next()
	if tok.Tp != intLit {
		p.error(tok.Offset, "expected integer in float opt")
		return types.UnspecifiedLength, types.UnspecifiedLength
	}
	flen = int(tok.Item.(int64))
	decimal = types.UnspecifiedLength
	if _, ok := p.accept(','); ok {
		tok = p.next()
		if tok.Tp != intLit {
			p.error(tok.Offset, "expected integer in float opt decimal")
			return flen, types.UnspecifiedLength
		}
		decimal = int(tok.Item.(int64))
	}
	p.expect(')')
	return flen, decimal
}

// parseExtractFunc parses EXTRACT(unit FROM expr).
// Produces ast.FuncCallExpr with FnName "extract" and Args [TimeUnitExpr, expr].
func (p *HandParser) parseExtractFunc() ast.ExprNode {
	tok := p.next() // consume EXTRACT
	p.expect('(')

	unit := p.parseTimeUnit()
	if unit == nil {
		return nil
	}

	p.expect(from)

	expr := p.parseExpression(0)
	if expr == nil {
		return nil
	}

	p.expect(')')

	return &ast.FuncCallExpr{
		FnName: ast.NewCIStr(tok.Lit),
		Args:   []ast.ExprNode{unit, expr},
	}
}

// parseTrimFunc parses TRIM(...) with 5 syntax variants:
//   - TRIM(expr)
//   - TRIM(expr FROM expr)
//   - TRIM(direction FROM expr)
//   - TRIM(direction expr FROM expr)
//
// Produces ast.FuncCallExpr with FnName "trim".
func (p *HandParser) parseTrimFunc() ast.ExprNode {
	tok := p.next() // consume TRIM
	p.expect('(')

	// Check for TrimDirection keyword.
	var direction ast.TrimDirectionType = -1 // sentinel: not specified
	switch p.peek().Tp {
	case both:
		p.next()
		direction = ast.TrimBoth
	case leading:
		p.next()
		direction = ast.TrimLeading
	case trailing:
		p.next()
		direction = ast.TrimTrailing
	}

	if direction >= 0 {
		// direction FROM expr — TRIM(BOTH FROM expr) or TRIM(LEADING FROM expr) etc.
		if _, ok := p.accept(from); ok {
			expr := p.parseExpression(0)
			if expr == nil {
				return nil
			}
			p.expect(')')
			spaceVal := ast.NewValueExpr(" ", p.charset, p.collation)
			dirExpr := &ast.TrimDirectionExpr{Direction: direction}
			return &ast.FuncCallExpr{
				FnName: ast.NewCIStr(tok.Lit),
				Args:   []ast.ExprNode{expr, spaceVal, dirExpr},
			}
		}
		// direction expr FROM expr — TRIM(BOTH 'x' FROM str)
		remStr := p.parseExpression(0)
		if remStr == nil {
			return nil
		}
		p.expect(from)
		expr := p.parseExpression(0)
		if expr == nil {
			return nil
		}
		p.expect(')')
		dirExpr := &ast.TrimDirectionExpr{Direction: direction}
		return &ast.FuncCallExpr{
			FnName: ast.NewCIStr(tok.Lit),
			Args:   []ast.ExprNode{expr, remStr, dirExpr},
		}
	}

	// No direction keyword: either TRIM(expr) or TRIM(expr FROM str).
	firstExpr := p.parseExpression(0)
	if firstExpr == nil {
		return nil
	}

	if _, ok := p.accept(from); ok {
		// TRIM(remstr FROM str)
		expr := p.parseExpression(0)
		if expr == nil {
			return nil
		}
		p.expect(')')
		return &ast.FuncCallExpr{
			FnName: ast.NewCIStr(tok.Lit),
			Args:   []ast.ExprNode{expr, firstExpr},
		}
	}

	// TRIM(expr) — simple form.
	p.expect(')')
	return &ast.FuncCallExpr{
		FnName: ast.NewCIStr(tok.Lit),
		Args:   []ast.ExprNode{firstExpr},
	}
}

// parsePositionFunc parses POSITION(substr IN str).
// Produces ast.FuncCallExpr with FnName "position" and Args [substr, str].
func (p *HandParser) parsePositionFunc() ast.ExprNode {
	tok := p.next() // consume POSITION
	p.expect('(')

	// Parse substr at precComparison+1 so that IN is not consumed as the
	// SQL IN operator (same pattern used for LIKE/BETWEEN subexpressions).
	substr := p.parseExpression(precPredicate + 1)
	if substr == nil {
		return nil
	}

	p.expect(in)

	str := p.parseExpression(0)
	if str == nil {
		return nil
	}

	p.expect(')')

	return &ast.FuncCallExpr{
		FnName: ast.NewCIStr(tok.Lit),
		Args:   []ast.ExprNode{substr, str},
	}
}

// parseDateArithFunc parses DATE_ADD/DATE_SUB:
//
//	DATE_ADD(expr, INTERVAL expr unit)
//
// The non-INTERVAL form (expr, expr) is only valid for ADDDATE/SUBDATE tokens,
// not for DATE_ADD/DATE_SUB.
func (p *HandParser) parseDateArithFunc() ast.ExprNode {
	tok := p.next() // consume DATE_ADD or DATE_SUB
	p.expect('(')

	dateExpr := p.parseExpression(0)
	if dateExpr == nil {
		return nil
	}

	p.expect(',')
	p.expect(interval)

	intervalExpr := p.parseExpression(0)
	if intervalExpr == nil {
		return nil
	}
	unit := p.parseTimeUnit()
	if unit == nil {
		return nil
	}
	p.expect(')')
	return &ast.FuncCallExpr{
		FnName: ast.NewCIStr(tok.Lit),
		Args:   []ast.ExprNode{dateExpr, intervalExpr, unit},
	}
}

// parseSubstringFunc parses SUBSTRING/SUBSTR with comma and FROM/FOR forms:
//   - SUBSTRING(str, pos)
//   - SUBSTRING(str, pos, len)
//   - SUBSTRING(str FROM pos)
//   - SUBSTRING(str FROM pos FOR len)
//
// Produces ast.FuncCallExpr with FnName "substring".
func (p *HandParser) parseSubstringFunc() ast.ExprNode {
	tok := p.next() // consume the builtin SUBSTRING/SUBSTR token
	p.expect('(')

	str := p.parseExpression(0)
	if str == nil {
		return nil
	}

	// FROM form or comma form — both parse pos and optional length.
	var usesFor bool
	if _, ok := p.accept(from); ok {
		usesFor = true // length separator is FOR
	} else {
		p.expect(',') // length separator is ','
	}

	pos := p.parseExpression(0)
	if pos == nil {
		return nil
	}

	args := []ast.ExprNode{str, pos}

	// Optional length: FOR (FROM form) or , (comma form)
	lenSep := forKwd
	if !usesFor {
		lenSep = ','
	}
	if _, ok := p.accept(lenSep); ok {
		length := p.parseExpression(0)
		if length == nil {
			return nil
		}
		args = append(args, length)
	}

	p.expect(')')
	return &ast.FuncCallExpr{
		FnName: ast.NewCIStr(tok.Lit),
		Args:   args,
	}
}

// parseTimeUnit parses a MySQL time unit keyword.
// Returns a *ast.TimeUnitExpr or nil on error.
func (p *HandParser) parseTimeUnit() *ast.TimeUnitExpr {
	tok := p.next()
	var unit ast.TimeUnitType
	switch tok.Tp {
	case microsecond:
		unit = ast.TimeUnitMicrosecond
	case second, sqlTsiSecond:
		unit = ast.TimeUnitSecond
	case minute, sqlTsiMinute:
		unit = ast.TimeUnitMinute
	case hour, sqlTsiHour:
		unit = ast.TimeUnitHour
	case day, sqlTsiDay:
		unit = ast.TimeUnitDay
	case week, sqlTsiWeek:
		unit = ast.TimeUnitWeek
	case month, sqlTsiMonth:
		unit = ast.TimeUnitMonth
	case quarter, sqlTsiQuarter:
		unit = ast.TimeUnitQuarter
	case yearType, sqlTsiYear:
		unit = ast.TimeUnitYear
	case secondMicrosecond:
		unit = ast.TimeUnitSecondMicrosecond
	case minuteMicrosecond:
		unit = ast.TimeUnitMinuteMicrosecond
	case minuteSecond:
		unit = ast.TimeUnitMinuteSecond
	case hourMicrosecond:
		unit = ast.TimeUnitHourMicrosecond
	case hourSecond:
		unit = ast.TimeUnitHourSecond
	case hourMinute:
		unit = ast.TimeUnitHourMinute
	case dayMicrosecond:
		unit = ast.TimeUnitDayMicrosecond
	case daySecond:
		unit = ast.TimeUnitDaySecond
	case dayMinute:
		unit = ast.TimeUnitDayMinute
	case dayHour:
		unit = ast.TimeUnitDayHour
	case yearMonth:
		unit = ast.TimeUnitYearMonth
	case identifier:
		// Handle any remaining identifier-based time unit aliases
		switch strings.ToUpper(tok.Lit) {
		case "SQL_TSI_SECOND":
			unit = ast.TimeUnitSecond
		case "SQL_TSI_MINUTE":
			unit = ast.TimeUnitMinute
		case "SQL_TSI_HOUR":
			unit = ast.TimeUnitHour
		case "SQL_TSI_DAY":
			unit = ast.TimeUnitDay
		case "SQL_TSI_WEEK":
			unit = ast.TimeUnitWeek
		case "SQL_TSI_MONTH":
			unit = ast.TimeUnitMonth
		case "SQL_TSI_QUARTER":
			unit = ast.TimeUnitQuarter
		case "SQL_TSI_YEAR":
			unit = ast.TimeUnitYear
		default:
			p.error(tok.Offset, "expected time unit")
			return nil
		}
	default:
		p.error(tok.Offset, "expected time unit")
		return nil
	}
	return &ast.TimeUnitExpr{Unit: unit}
}
