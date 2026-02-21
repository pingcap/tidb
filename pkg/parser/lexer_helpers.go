// Copyright 2015 PingCAP, Inc.
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

// lexer_helpers.go contains literal conversion helpers called by Scanner.Lex
// (in lexer.go) to parse integer, decimal, float, hex, and bit literals.

import (
	"math"
	"strconv"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/parser/types"
)

func toInt(l Lexer, lval *Token, str string) int {
	n, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		e := err.(*strconv.NumError)
		if e.Err == strconv.ErrRange {
			return toDecimal(l, lval, str)
		}
		l.AppendError(l.Errorf("integer literal: %v", err))
		return invalid
	}

	switch {
	case n <= math.MaxInt64:
		lval.Item = int64(n)
	default:
		lval.Item = n
	}
	return intLit
}

func toDecimal(l Lexer, lval *Token, str string) int {
	dec, err := ast.NewDecimal(str)
	if err != nil {
		if terror.ErrorEqual(err, types.ErrDataOutOfRange) {
			l.AppendWarn(types.ErrTruncatedWrongValue.FastGenByArgs("DECIMAL", dec))
			dec, _ = ast.NewDecimal(mysql.DefaultDecimal)
		} else {
			l.AppendError(l.Errorf("decimal literal: %v", err))
		}
	}
	lval.Item = dec
	return decLit
}

func toFloat(l Lexer, lval *Token, str string) int {
	n, err := strconv.ParseFloat(str, 64)
	if err != nil {
		e := err.(*strconv.NumError)
		if e.Err == strconv.ErrRange {
			l.AppendError(types.ErrIllegalValueForType.GenWithStackByArgs("double", str))
			return invalid
		}
		l.AppendError(l.Errorf("float literal: %v", err))
		return invalid
	}

	lval.Item = n
	return floatLit
}

// See https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func toHex(l Lexer, lval *Token, str string) int {
	h, err := ast.NewHexLiteral(str)
	if err != nil {
		l.AppendError(l.Errorf("hex literal: %v", err))
		return invalid
	}
	lval.Item = h
	return hexLit
}

// See https://dev.mysql.com/doc/refman/5.7/en/bit-type.html
func toBit(l Lexer, lval *Token, str string) int {
	b, err := ast.NewBitLiteral(str)
	if err != nil {
		l.AppendError(l.Errorf("bit literal: %v", err))
		return invalid
	}
	lval.Item = b
	return bitLit
}
