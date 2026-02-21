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
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// ---------------------------------------------------------------------------
// Field Type Parsing
// ---------------------------------------------------------------------------

// parseFieldType parses a data type definition: INT(10) UNSIGNED, VARCHAR(255) CHARSET utf8, etc.
func (p *HandParser) parseFieldType() *types.FieldType {
	tp := types.NewFieldType(mysql.TypeUnspecified)
	p.lastFieldTypeExplicitCollate = false // reset for this column

	token := p.peek()
	switch token.Tp {
	case tokBit:
		p.next()
		tp.SetType(mysql.TypeBit)
		if p.peek().Tp == '(' {
			tp.SetFlen(p.parseFieldLen())
		} else {
			tp.SetFlen(1)
		}
	case tokTinyInt, tokInt1:
		p.parseIntegerType(tp, mysql.TypeTiny)
	case tokSmallInt, tokInt2:
		p.parseIntegerType(tp, mysql.TypeShort)
	case tokMediumInt, tokInt3, tokMiddleInt:
		p.parseIntegerType(tp, mysql.TypeInt24)
	case tokInt, tokInteger, tokInt4:
		p.parseIntegerType(tp, mysql.TypeLong)
	case tokBigInt, tokInt8:
		p.parseIntegerType(tp, mysql.TypeLonglong)
	case tokReal:
		p.next()
		if p.sqlMode.HasRealAsFloatMode() {
			tp.SetType(mysql.TypeFloat)
		} else {
			tp.SetType(mysql.TypeDouble)
		}
		p.parseFloatOptions(tp)
	case tokDouble, tokFloat8:
		p.next()
		tp.SetType(mysql.TypeDouble)
		if p.peek().Tp == tokPrecision {
			p.next()
		}
		p.parseFloatOptions(tp)
	case tokFloat, tokFloat4:
		p.next()
		tp.SetType(mysql.TypeFloat)
		p.parseFloatOptions(tp)
		if tp.GetDecimal() == types.UnspecifiedLength && tp.GetFlen() <= mysql.MaxDoublePrecisionLength {
			// Single-arg float(N): N is a precision specifier, not a display width.
			// Per MySQL, float(N<=24) → FLOAT, float(24<N<=53) → DOUBLE.
			// Only reset Flen for valid precision values; preserve out-of-range
			// values so the preprocessor can report "Display width out of range".
			if tp.GetFlen() > mysql.MaxFloatPrecisionLength {
				tp.SetType(mysql.TypeDouble)
			}
			tp.SetFlen(types.UnspecifiedLength)
		}
	case tokDecimal, tokNumeric, tokFixed:
		p.next()
		tp.SetType(mysql.TypeNewDecimal)
		p.parseDecimalOptions(tp)
	case tokDate:
		p.next()
		tp.SetType(mysql.TypeDate)
	case tokDatetime:
		p.parseFspType(tp, mysql.TypeDatetime)
	case tokTimestamp:
		p.parseFspType(tp, mysql.TypeTimestamp)
	case tokTime:
		p.parseFspType(tp, mysql.TypeDuration)

	case tokYear, tokSqlTsiYear:
		p.next()
		tp.SetType(mysql.TypeYear)
		if p.peek().Tp == '(' {
			// consume length
			_ = p.next() // consume '('
			lenTok := p.peek()

			var v int64
			var valid bool
			switch x := lenTok.Item.(type) {
			case int64:
				v = x
				valid = true
			case uint64:
				v = int64(x)
				valid = true
			case int:
				v = int64(x)
				valid = true
			}

			if valid {
				if v != 4 {
					// Use raw error to match test expectation (no location prefix)
					p.errs = append(p.errs, errors.New("[parser:1818]Supports only YEAR or YEAR(4) column"))
				}
				tp.SetFlen(int(v))
				p.next()
			} else {
				// error handling or just consume
				p.next()
			}
			p.expect(')')
		}
		// Discard options for YEAR type to match yacc parser behavior
		p.parseIntegerOptions(types.NewFieldType(mysql.TypeYear))
	case tokNational:
		p.next()
		switch p.peek().Tp {
		case tokChar, tokCharacter:
			// NATIONAL CHAR / NATIONAL CHARACTER [VARYING]
			p.next()
			if p.resolveCharVarchar() {
				tp.SetType(mysql.TypeVarchar)
				tp.SetFlen(p.parseFieldLen()) // VARYING requires length
			} else {
				tp.SetType(mysql.TypeString)
				p.parseOptionalFieldLen(tp)
			}
		case tokVarchar, tokVarCharacter:
			// NATIONAL VARCHAR / NATIONAL VARCHARACTER
			p.next()
			tp.SetType(mysql.TypeVarchar)
			tp.SetFlen(p.parseFieldLen()) // always requires length
		default:
			return nil
		}
		p.parseStringOptions(tp)
	case tokNChar, tokChar, tokCharacter:
		// NCHAR/CHAR/CHARACTER [VARYING|VARCHAR|VARCHARACTER]
		p.next()
		if p.resolveCharVarchar() {
			tp.SetType(mysql.TypeVarchar)
		} else {
			tp.SetType(mysql.TypeString)
		}
		p.parseOptionalFieldLen(tp)
		p.parseStringOptions(tp)
	case tokNVarchar, tokVarchar, tokVarCharacter:
		p.next()
		tp.SetType(mysql.TypeVarchar)
		p.parseOptionalFieldLen(tp)
		p.parseStringOptions(tp)
	case tokBinary:
		p.parseBinaryFieldType(tp, mysql.TypeString, false)
	case tokVarbinary:
		p.parseBinaryFieldType(tp, mysql.TypeVarchar, true)
	case tokTinyBlob:
		p.parseBlobType(tp, mysql.TypeTinyBlob)
	case tokBlob:
		p.parseBlobType(tp, mysql.TypeBlob)
		if p.peek().Tp == '(' {
			tp.SetFlen(p.parseFieldLen())
		}
	case tokMediumBlob:
		p.parseBlobType(tp, mysql.TypeMediumBlob)
	case tokLongBlob:
		p.parseBlobType(tp, mysql.TypeLongBlob)
	case tokTinyText:
		p.parseTextType(tp, mysql.TypeTinyBlob)
	case tokText:
		p.next()
		tp.SetType(mysql.TypeBlob)
		if p.peek().Tp == '(' {
			tp.SetFlen(p.parseFieldLen())
		}
		p.parseStringOptions(tp)
	case tokMediumText:
		p.parseTextType(tp, mysql.TypeMediumBlob)
	case tokLongText:
		p.parseTextType(tp, mysql.TypeLongBlob)
	case tokLong:
		p.next()
		if p.peek().Tp == tokVarbinary || p.peek().Tp == tokByte {
			p.next()
			tp.SetType(mysql.TypeMediumBlob)
			tp.SetCharset(charset.CharsetBin)
			tp.SetCollate(charset.CollationBin)
			tp.SetFlag(mysql.BinaryFlag)
		} else {
			// Consume optional Varchar form: VARCHAR | VARCHARACTER | CHAR VARYING | CHARACTER VARYING
			switch p.peek().Tp {
			case tokVarchar, tokVarCharacter:
				p.next()
			case tokChar, tokCharacter:
				// LONG CHAR VARYING / LONG CHARACTER VARYING
				if p.peekN(1).Tp == tokVarying {
					p.next() // consume CHAR/CHARACTER
					p.next() // consume VARYING
				}
			}
			tp.SetType(mysql.TypeMediumBlob)
			p.parseStringOptions(tp)
		}
	case tokEnum, tokSetType:
		if p.next().Tp == tokEnum {
			tp.SetType(mysql.TypeEnum)
		} else {
			tp.SetType(mysql.TypeSet)
		}
		p.parseEnumSetOptions(tp)
	case tokJson:
		p.next()
		tp.SetType(mysql.TypeJSON)
		tp.SetDecimal(0)
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
	case tokGeometry, tokPoint:
		p.next()
		tp.SetType(mysql.TypeGeometry)
	case tokIdentifier:
		// Handle LINESTRING, POLYGON, etc. which are not keywords
		str := token.Lit
		switch strings.ToUpper(str) {
		case "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
			p.next()
			tp.SetType(mysql.TypeGeometry)
		default:
			return nil // Not a known type
		}
	case tokVector:
		p.next()
		tp.SetType(mysql.TypeTiDBVectorFloat32)
		// Check for <FLOAT> or <DOUBLE>
		if p.peek().Tp == '<' {
			p.next()
			// Consume element type
			switch p.peek().Tp {
			case tokFloat, tokFloat4:
				// VECTOR<FLOAT> — default, already TypeTiDBVectorFloat32
				p.next()
			case tokDouble, tokFloat8:
				// VECTOR<DOUBLE> — parsed but rejected per parser.y (AppendError is fatal)
				p.next()
				p.errs = append(p.errs, fmt.Errorf("Only VECTOR is supported for now"))
				p.expect('>')
				return nil
			default:
				p.error(p.peek().Offset, "expected FLOAT or DOUBLE inside VECTOR")
				return nil
			}
			p.expect('>')
		}
		if p.peek().Tp == '(' {
			tp.SetFlen(p.parseFieldLen())
		}
		tp.SetDecimal(0)
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
	case tokBoolean, tokBool:
		p.next()
		tp.SetType(mysql.TypeTiny)
		tp.SetFlen(1)
		p.parseIntegerOptions(tp)
	default:
		return nil
	}
	return tp
}

// acceptCharsetKw tries to consume a charset keyword matching the yacc CharsetKw production:
//
//	CharsetKw: CHARACTER SET | CHARSET | CHAR SET
//
// Returns true if consumed, false otherwise. Caller should then parse the charset name.
func (p *HandParser) acceptCharsetKw() bool {
	switch p.peek().Tp {
	case tokCharacter, tokChar:
		if p.peekN(1).Tp == tokSet {
			p.next() // consume CHARACTER/CHAR
			p.next() // consume SET
			return true
		}
	case tokCharsetKwd:
		p.next()
		return true
	}
	return false
}

// resolveCharVarchar checks if the next token converts a CHAR-like type to VARCHAR.
// Returns true (and consumes the token) if VARYING, VARCHAR, or VARCHARACTER follows.
// This consolidates the CHAR→VARCHAR promotion logic used across multiple type branches.
func (p *HandParser) resolveCharVarchar() bool {
	switch p.peek().Tp {
	case tokVarying, tokVarchar, tokVarCharacter:
		p.next()
		return true
	}
	return false
}

// parseOptionalFieldLen parses an optional field length: (N).
// If the next token is '(', it consumes the parenthesized length and sets it on tp.
func (p *HandParser) parseOptionalFieldLen(tp *types.FieldType) {
	if p.peek().Tp == '(' {
		tp.SetFlen(p.parseFieldLen())
	}
}

func (p *HandParser) parseFieldLen() int {
	p.expect('(')
	l := p.peek()
	val, ok := l.Item.(int64)
	if !ok {
		// handle error or unexpected token
		p.next() // consume error token (replacing advance)
		return 0
	}
	p.next()
	p.expect(')')
	return int(val)
}

func (p *HandParser) parseFieldLenAndOptions(tp *types.FieldType) {
	if p.peek().Tp == '(' {
		p.expect('(')
		l := p.peek()
		if val, ok := l.Item.(int64); ok {
			tp.SetFlen(int(val))
			p.next()
		}
		p.expect(')')
	}
	p.parseIntegerOptions(tp)
}

func (p *HandParser) parseIntegerOptions(tp *types.FieldType) {
	for {
		switch p.peek().Tp {
		case tokUnsigned:
			p.next()
			tp.AddFlag(mysql.UnsignedFlag)
		case tokSigned:
			p.next()
			tp.DelFlag(mysql.UnsignedFlag)
		case tokZerofill:
			p.next()
			tp.AddFlag(mysql.ZerofillFlag)
			tp.AddFlag(mysql.UnsignedFlag) // Zerofill implies Unsigned
		default:
			return
		}
	}
}

func (p *HandParser) parseFloatOptions(tp *types.FieldType) {
	if p.peek().Tp == '(' {
		p.expect('(')
		lenTok := p.peek()
		if lenVal, ok := lenTok.Item.(int64); ok {
			tp.SetFlen(int(lenVal))
			p.next()
			if p.peek().Tp == ',' {
				p.next()
				decTok := p.peek()
				if decVal, ok := decTok.Item.(int64); ok {
					tp.SetDecimal(int(decVal))
					p.next()
				}
			} else if p.strictDoubleFieldType && tp.GetType() == mysql.TypeDouble {
				p.syntaxError(p.peek().Offset)
				return
			}
		}
		p.expect(')')
	}
	p.parseIntegerOptions(tp)
}

func (p *HandParser) parseDecimalOptions(tp *types.FieldType) {
	p.parseFloatOptions(tp) // Same syntax as float
}

// parseCharsetName reads a charset name after CHARACTER SET has been consumed.
// Matches parser.y CharsetName: StringName { GetCharsetInfo($1) } | binaryType { CharsetBin }
func (p *HandParser) parseCharsetName(tp *types.FieldType) {
	tok := p.peek()
	// binaryType → charset.CharsetBin
	if tok.Tp == tokBinary {
		tp.SetCharset(charset.CharsetBin)
		p.next()
		return
	}
	// StringName → validate via GetCharsetInfo and use canonical cs.Name
	var name string
	if s := tok.Lit; s != "" {
		name = s
	} else if s, ok := tok.Item.(string); ok {
		name = s
	}
	if name != "" {
		cs, err := charset.GetCharsetInfo(name)
		if err != nil {
			p.errs = append(p.errs, fmt.Errorf("[parser:1115]Unknown character set: '%s'", name))
			return
		}
		tp.SetCharset(cs.Name)
		p.next()
	}
}

func (p *HandParser) parseStringOptions(tp *types.FieldType) {
	// Match OptCharsetWithOptBinary / OptBinary:
	//   BYTE           → binary charset, done
	//   ASCII          → latin1 charset, done
	//   UNICODE        → ucs2 charset, done
	//   BINARY [CHARACTER SET charset] → binary flag, optional charset
	//   CHARACTER SET charset [BINARY] → charset, optional binary flag
	switch p.peek().Tp {
	case tokByte:
		// BYTE = binary charset (standalone, no further options)
		p.next()
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
		tp.AddFlag(mysql.BinaryFlag)
		return
	case tokAscii:
		p.next()
		tp.SetCharset("latin1")
		return
	case tokUnicode:
		p.next()
		cs, err := charset.GetCharsetInfo("ucs2")
		if err != nil {
			p.errs = append(p.errs, fmt.Errorf("[parser:1115]Unknown character set: 'ucs2'"))
			return
		}
		tp.SetCharset(cs.Name)
		// Fall through to auto-flag below
	case tokBinary:
		// BINARY [CHARACTER SET charset]
		p.next()
		tp.AddFlag(mysql.BinaryFlag)
		if p.acceptCharsetKw() {
			p.parseCharsetName(tp)
		}
	default:
		if p.acceptCharsetKw() {
			p.parseCharsetName(tp)
		}
	}

	// Auto-set BinaryFlag and CollationBin when charset is binary.
	// parser.y does this in TextType+OptCharsetWithOptBinary and LONG+OptCharsetWithOptBinary.
	if tp.GetCharset() == charset.CharsetBin {
		tp.AddFlag(mysql.BinaryFlag)
		if tp.GetCollate() == "" {
			tp.SetCollate(charset.CollationBin)
		}
	}
	if p.peek().Tp == tokCollate {
		p.lastFieldTypeExplicitCollate = true // track explicit COLLATE for duplicate detection
		p.next()
		tok := p.peek()
		if s := tok.Lit; s != "" {
			info, err := charset.GetCollationByName(s)
			if err != nil {
				p.errs = append(p.errs, err)
				return
			}
			tp.SetCollate(info.Name)
			p.next()
		} else if s, ok := tok.Item.(string); ok && s != "" {
			info, err := charset.GetCollationByName(s)
			if err != nil {
				p.errs = append(p.errs, err)
				return
			}
			tp.SetCollate(info.Name)
			p.next()
		} else if tok.Tp == tokBinary || tok.Tp == tokByte {
			tp.SetCollate(charset.CollationBin)
			p.next()
		} else {
			p.error(tok.Offset, "expected collation name")
		}
	}

	// Parse optional trailing BINARY: [CHARACTER SET ...] [COLLATE ...] [BINARY]
	if p.peek().Tp == tokBinary {
		p.next()
		tp.AddFlag(mysql.BinaryFlag)
	}
}

func (p *HandParser) parseEnumSetOptions(tp *types.FieldType) {
	p.expect('(')

	// Collect elements into local slices first, then batch-set with IsBinaryLit metadata.
	type enumElem struct {
		val         string
		isBinaryLit bool
	}
	var elems []enumElem

	for {
		tok := p.peek()
		var elemVal string
		isBinaryLit := false
		if tok.Tp == tokStringLit {
			elemVal = strings.TrimRight(tok.Lit, " ")
			p.next()
		} else if tok.Tp == tokHexLit {
			s := tok.Lit
			if len(s) > 2 && (s[0] == 'x' || s[0] == 'X') && s[1] == '\'' {
				s = s[2 : len(s)-1]
			} else if len(s) > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
				s = s[2:]
			}
			if len(s)%2 != 0 {
				s = "0" + s
			}
			b, err := hex.DecodeString(s)
			if err == nil {
				elemVal = string(b)
			} else {
				elemVal = tok.Lit
			}
			isBinaryLit = true
			p.next()
		} else if tok.Tp == tokBitLit {
			s := tok.Lit
			if len(s) > 2 && (s[0] == 'b' || s[0] == 'B') && s[1] == '\'' {
				s = s[2 : len(s)-1]
			} else if len(s) > 2 && s[0] == '0' && (s[1] == 'b' || s[1] == 'B') {
				s = s[2:]
			}
			if len(s)%8 != 0 {
				pad := 8 - len(s)%8
				s = strings.Repeat("0", pad) + s
			}
			b := make([]byte, len(s)/8)
			for i := 0; i < len(s); i += 8 {
				val, err := strconv.ParseUint(s[i:i+8], 2, 8)
				if err == nil {
					b[i/8] = byte(val)
				}
			}
			elemVal = string(b)
			isBinaryLit = true
			p.next()
		} else {
			break
		}
		elems = append(elems, enumElem{val: elemVal, isBinaryLit: isBinaryLit})
		if p.peek().Tp != ',' {
			break
		}
		p.next()
	}
	p.expect(')')

	// Pre-allocate elems slice and set each element with IsBinaryLit metadata
	strs := make([]string, len(elems))
	tp.SetElems(strs)
	for i, e := range elems {
		tp.SetElemWithIsBinaryLit(i, e.val, e.isBinaryLit)
	}

	// Compute flen from element list (matching MySQL syntax).
	finalElems := tp.GetElems()
	if tp.GetType() == mysql.TypeEnum {
		// ENUM: flen = max element string length
		maxLen := 0
		for _, e := range finalElems {
			if len(e) > maxLen {
				maxLen = len(e)
			}
		}
		tp.SetFlen(maxLen)
	} else if tp.GetType() == mysql.TypeSet {
		// SET: flen = sum of all element lengths + (n-1) commas
		totalLen := 0
		for _, e := range finalElems {
			totalLen += len(e)
		}
		if len(finalElems) > 1 {
			totalLen += len(finalElems) - 1 // commas between elements
		}
		tp.SetFlen(totalLen)
	}

	p.parseStringOptions(tp)
}

func (p *HandParser) parseIntegerType(tp *types.FieldType, mysqlType byte) {
	p.next()
	tp.SetType(mysqlType)
	p.parseFieldLenAndOptions(tp)
}

func (p *HandParser) parseBlobType(tp *types.FieldType, mysqlType byte) {
	p.next()
	tp.SetType(mysqlType)
	tp.SetCharset(charset.CharsetBin)
	tp.SetCollate(charset.CollationBin)
	tp.SetFlag(mysql.BinaryFlag)
}

func (p *HandParser) parseTextType(tp *types.FieldType, mysqlType byte) {
	p.next()
	tp.SetType(mysqlType)
	p.parseStringOptions(tp)
}

// parseFspType parses a type with optional fractional-second precision: DATETIME/TIMESTAMP/TIME.
func (p *HandParser) parseFspType(tp *types.FieldType, mysqlType byte) {
	p.next()
	tp.SetType(mysqlType)
	// Set base Flen per parser.y: DateAndTimeType
	switch mysqlType {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		tp.SetFlen(mysql.MaxDatetimeWidthNoFsp)
	case mysql.TypeDuration:
		tp.SetFlen(mysql.MaxDurationWidthNoFsp)
	}
	if p.peek().Tp == '(' {
		tp.SetDecimal(p.parseFieldLen())
		if tp.GetDecimal() > 0 {
			tp.SetFlen(tp.GetFlen() + 1 + tp.GetDecimal())
		}
	}
}

// parseBinaryFieldType parses BINARY/VARBINARY: sets binary charset/collation/flag.
// For VARBINARY, FieldLen is mandatory per parser.y; for BINARY, it's optional.
func (p *HandParser) parseBinaryFieldType(tp *types.FieldType, mysqlType byte, requireLen bool) {
	p.next()
	tp.SetType(mysqlType)
	tp.SetCharset(charset.CharsetBin)
	tp.SetCollate(charset.CollationBin)
	tp.SetFlag(mysql.BinaryFlag)
	if p.peek().Tp == '(' {
		tp.SetFlen(p.parseFieldLen())
	} else if requireLen {
		p.error(p.peek().Offset, "missing length for VARBINARY")
	}
}
