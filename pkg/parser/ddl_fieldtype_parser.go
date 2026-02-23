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
	case bitType:
		p.next()
		tp.SetType(mysql.TypeBit)
		if p.peek().Tp == '(' {
			tp.SetFlen(p.parseFieldLen())
		} else {
			tp.SetFlen(1)
		}
	case tinyIntType, int1Type:
		p.parseIntegerType(tp, mysql.TypeTiny)
	case smallIntType, int2Type:
		p.parseIntegerType(tp, mysql.TypeShort)
	case mediumIntType, int3Type, middleIntType:
		p.parseIntegerType(tp, mysql.TypeInt24)
	case intType, integerType, int4Type:
		p.parseIntegerType(tp, mysql.TypeLong)
	case bigIntType, int8Type:
		p.parseIntegerType(tp, mysql.TypeLonglong)
	case realType:
		p.next()
		if p.sqlMode.HasRealAsFloatMode() {
			tp.SetType(mysql.TypeFloat)
		} else {
			tp.SetType(mysql.TypeDouble)
		}
		p.parseFloatOptions(tp)
	case doubleType, float8Type:
		p.next()
		tp.SetType(mysql.TypeDouble)
		if p.peek().Tp == precisionType {
			p.next()
		}
		p.parseFloatOptions(tp)
	case floatType, float4Type:
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
	case decimalType, numericType, fixed:
		p.next()
		tp.SetType(mysql.TypeNewDecimal)
		p.parseDecimalOptions(tp)
	case dateType:
		p.next()
		tp.SetType(mysql.TypeDate)
	case datetimeType:
		p.parseFspType(tp, mysql.TypeDatetime)
	case timestampType:
		p.parseFspType(tp, mysql.TypeTimestamp)
	case timeType:
		p.parseFspType(tp, mysql.TypeDuration)

	case yearType, sqlTsiYear:
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
	case national:
		p.next()
		switch p.peek().Tp {
		case charType, character:
			// NATIONAL CHAR / NATIONAL CHARACTER [VARYING]
			p.next()
			if p.resolveCharVarchar() {
				tp.SetType(mysql.TypeVarchar)
				tp.SetFlen(p.parseFieldLen()) // VARYING requires length
			} else {
				tp.SetType(mysql.TypeString)
				p.parseOptionalFieldLen(tp)
			}
		case varcharType, varcharacter:
			// NATIONAL VARCHAR / NATIONAL VARCHARACTER
			p.next()
			tp.SetType(mysql.TypeVarchar)
			tp.SetFlen(p.parseFieldLen()) // always requires length
		default:
			return nil
		}
		p.parseStringOptions(tp)
	case ncharType, charType, character:
		// NCHAR/CHAR/CHARACTER [VARYING|VARCHAR|VARCHARACTER]
		p.next()
		if p.resolveCharVarchar() {
			tp.SetType(mysql.TypeVarchar)
		} else {
			tp.SetType(mysql.TypeString)
		}
		p.parseOptionalFieldLen(tp)
		p.parseStringOptions(tp)
	case nvarcharType, varcharType, varcharacter:
		p.next()
		tp.SetType(mysql.TypeVarchar)
		p.parseOptionalFieldLen(tp)
		p.parseStringOptions(tp)
	case binaryType:
		p.parseBinaryFieldType(tp, mysql.TypeString, false)
	case varbinaryType:
		p.parseBinaryFieldType(tp, mysql.TypeVarchar, true)
	case tinyblobType:
		p.parseBlobType(tp, mysql.TypeTinyBlob)
	case blobType:
		p.parseBlobType(tp, mysql.TypeBlob)
		if p.peek().Tp == '(' {
			tp.SetFlen(p.parseFieldLen())
		}
	case mediumblobType:
		p.parseBlobType(tp, mysql.TypeMediumBlob)
	case longblobType:
		p.parseBlobType(tp, mysql.TypeLongBlob)
	case tinytextType:
		p.parseTextType(tp, mysql.TypeTinyBlob)
	case textType:
		p.next()
		tp.SetType(mysql.TypeBlob)
		if p.peek().Tp == '(' {
			tp.SetFlen(p.parseFieldLen())
		}
		p.parseStringOptions(tp)
	case mediumtextType:
		p.parseTextType(tp, mysql.TypeMediumBlob)
	case longtextType:
		p.parseTextType(tp, mysql.TypeLongBlob)
	case long:
		p.next()
		if p.peek().Tp == varbinaryType || p.peek().Tp == byteType {
			p.next()
			tp.SetType(mysql.TypeMediumBlob)
			tp.SetCharset(charset.CharsetBin)
			tp.SetCollate(charset.CollationBin)
			tp.SetFlag(mysql.BinaryFlag)
		} else {
			// Consume optional Varchar form: VARCHAR | VARCHARACTER | CHAR VARYING | CHARACTER VARYING
			switch p.peek().Tp {
			case varcharType, varcharacter:
				p.next()
			case charType, character:
				// LONG CHAR VARYING / LONG CHARACTER VARYING
				if p.peekN(1).Tp == varying {
					p.next() // consume CHAR/CHARACTER
					p.next() // consume VARYING
				}
			}
			tp.SetType(mysql.TypeMediumBlob)
			p.parseStringOptions(tp)
		}
	case enum, set:
		if p.next().Tp == enum {
			tp.SetType(mysql.TypeEnum)
		} else {
			tp.SetType(mysql.TypeSet)
		}
		p.parseEnumSetOptions(tp)
	case jsonType:
		p.next()
		tp.SetType(mysql.TypeJSON)
		tp.SetDecimal(0)
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
	case spatial, point:
		p.next()
		tp.SetType(mysql.TypeGeometry)
	case identifier:
		// Handle LINESTRING, POLYGON, etc. which are not keywords
		str := token.Lit
		switch strings.ToUpper(str) {
		case "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
			p.next()
			tp.SetType(mysql.TypeGeometry)
		default:
			return nil // Not a known type
		}
	case vectorType:
		p.next()
		tp.SetType(mysql.TypeTiDBVectorFloat32)
		// Check for <FLOAT> or <DOUBLE>
		if p.peek().Tp == '<' {
			p.next()
			// Consume element type
			switch p.peek().Tp {
			case floatType, float4Type:
				// VECTOR<FLOAT> — default, already TypeTiDBVectorFloat32
				p.next()
			case doubleType, float8Type:
				// VECTOR<DOUBLE> — parsed but rejected per parser.y (AppendError is fatal)
				p.next()
				p.errs = append(p.errs, fmt.Errorf("only VECTOR is supported for now"))
				p.expect('>')
				return nil
			default:
				p.syntaxErrorAt(p.peek().Offset)
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
	case booleanType, boolType:
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
	case character, charType:
		if p.peekN(1).Tp == set {
			p.next() // consume CHARACTER/CHAR
			p.next() // consume SET
			return true
		}
	case charsetKwd:
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
	case varying, varcharType, varcharacter:
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
		case unsigned:
			p.next()
			tp.AddFlag(mysql.UnsignedFlag)
		case signed:
			p.next()
			tp.DelFlag(mysql.UnsignedFlag)
		case zerofill:
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
	// binaryType → charset.CharsetBin
	if _, ok := p.accept(binaryType); ok {
		tp.SetCharset(charset.CharsetBin)
		return
	}
	// StringName: identifier, keyword-as-identifier, or string literal.
	// Uses acceptStringName to correctly handle keyword tokens like "ascii".
	if tok, ok := p.acceptStringName(); ok {
		cs, err := charset.GetCharsetInfo(tok.Lit)
		if err != nil {
			p.errs = append(p.errs, fmt.Errorf("[parser:1115]Unknown character set: '%s'", tok.Lit))
			return
		}
		tp.SetCharset(cs.Name)
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
	case byteType:
		// BYTE = binary charset (standalone, no further options)
		p.next()
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
		tp.AddFlag(mysql.BinaryFlag)
		return
	case ascii:
		p.next()
		tp.SetCharset("latin1")
		return
	case unicodeSym:
		p.next()
		cs, err := charset.GetCharsetInfo("ucs2")
		if err != nil {
			p.errs = append(p.errs, fmt.Errorf("[parser:1115]Unknown character set: 'ucs2'"))
			return
		}
		tp.SetCharset(cs.Name)
		// Fall through to auto-flag below
	case binaryType:
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
	if p.peek().Tp == collate {
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
		} else if tok.Tp == binaryType || tok.Tp == byteType {
			tp.SetCollate(charset.CollationBin)
			p.next()
		} else {
			p.syntaxErrorAt(tok.Offset)
		}
	}

	// Parse optional trailing BINARY: [CHARACTER SET ...] [COLLATE ...] [BINARY]
	if p.peek().Tp == binaryType {
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
		if tok.Tp == stringLit {
			elemVal = strings.TrimRight(tok.Lit, " ")
			p.next()
		} else if tok.Tp == hexLit {
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
		} else if tok.Tp == bitLit {
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
		p.syntaxErrorAt(p.peek().Offset)
	}
}
