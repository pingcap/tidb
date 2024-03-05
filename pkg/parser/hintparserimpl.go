// Copyright 2020 PingCAP, Inc.
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
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

//revive:disable:exported
var (
	ErrWarnOptimizerHintUnsupportedHint = terror.ClassParser.NewStd(mysql.ErrWarnOptimizerHintUnsupportedHint)
	ErrWarnOptimizerHintInvalidToken    = terror.ClassParser.NewStd(mysql.ErrWarnOptimizerHintInvalidToken)
	ErrWarnMemoryQuotaOverflow          = terror.ClassParser.NewStd(mysql.ErrWarnMemoryQuotaOverflow)
	ErrWarnOptimizerHintParseError      = terror.ClassParser.NewStd(mysql.ErrWarnOptimizerHintParseError)
	ErrWarnOptimizerHintInvalidInteger  = terror.ClassParser.NewStd(mysql.ErrWarnOptimizerHintInvalidInteger)
	ErrWarnOptimizerHintWrongPos        = terror.ClassParser.NewStd(mysql.ErrWarnOptimizerHintWrongPos)
)

//revive:enable:exported

// hintScanner implements the yyhintLexer interface
type hintScanner struct {
	Scanner
}

func (hs *hintScanner) Errorf(format string, args ...interface{}) error {
	inner := hs.Scanner.Errorf(format, args...)
	return ErrParse.GenWithStackByArgs("Optimizer hint syntax error at", inner)
}

func (hs *hintScanner) Lex(lval *yyhintSymType) int {
	tok, pos, lit := hs.scan()
	hs.lastScanOffset = pos.Offset
	var errorTokenType string

	switch tok {
	case intLit:
		n, e := strconv.ParseUint(lit, 10, 64)
		if e != nil {
			hs.AppendError(ErrWarnOptimizerHintInvalidInteger.GenWithStackByArgs(lit))
			return hintInvalid
		}
		lval.number = n
		return hintIntLit

	case singleAtIdentifier:
		lval.ident = lit
		return hintSingleAtIdentifier

	case identifier:
		lval.ident = lit
		if tok1, ok := hintTokenMap[strings.ToUpper(lit)]; ok {
			return tok1
		}
		return hintIdentifier

	case stringLit:
		lval.ident = lit
		if hs.sqlMode.HasANSIQuotesMode() && hs.r.s[pos.Offset] == '"' {
			return hintIdentifier
		}
		return hintStringLit

	case bitLit:
		if strings.HasPrefix(lit, "0b") {
			lval.ident = lit
			return hintIdentifier
		}
		errorTokenType = "bit-value literal"

	case hexLit:
		if strings.HasPrefix(lit, "0x") {
			lval.ident = lit
			return hintIdentifier
		}
		errorTokenType = "hexadecimal literal"

	case quotedIdentifier:
		lval.ident = lit
		return hintIdentifier

	case eq:
		return '='

	case floatLit:
		errorTokenType = "floating point number"
	case decLit:
		errorTokenType = "decimal number"

	default:
		if tok <= 0x7f {
			return tok
		}
		errorTokenType = "unknown token"
	}

	hs.AppendError(ErrWarnOptimizerHintInvalidToken.GenWithStackByArgs(errorTokenType, lit, tok))
	return hintInvalid
}

type hintParser struct {
	lexer  hintScanner
	result []*ast.TableOptimizerHint

	// the following fields are used by yyParse to reduce allocation.
	cache  []yyhintSymType
	yylval yyhintSymType
	yyVAL  *yyhintSymType
}

func newHintParser() *hintParser {
	return &hintParser{cache: make([]yyhintSymType, 50)}
}

func (hp *hintParser) parse(input string, sqlMode mysql.SQLMode, initPos Pos) ([]*ast.TableOptimizerHint, []error) {
	hp.result = nil
	hp.lexer.reset(input[3:])
	hp.lexer.SetSQLMode(sqlMode)
	hp.lexer.r.updatePos(Pos{
		Line:   initPos.Line,
		Col:    initPos.Col + 3, // skipped the initial '/*+'
		Offset: 0,
	})
	hp.lexer.inBangComment = true // skip the final '*/' (we need the '*/' for reporting warnings)

	yyhintParse(&hp.lexer, hp)

	warns, errs := hp.lexer.Errors()
	if len(errs) == 0 {
		errs = warns
	}
	return hp.result, errs
}

// ParseHint parses an optimizer hint (the interior of `/*+ ... */`).
func ParseHint(input string, sqlMode mysql.SQLMode, initPos Pos) ([]*ast.TableOptimizerHint, []error) {
	hp := newHintParser()
	return hp.parse(input, sqlMode, initPos)
}

func (hp *hintParser) warnUnsupportedHint(name string) {
	warn := ErrWarnOptimizerHintUnsupportedHint.FastGenByArgs(name)
	hp.lexer.warns = append(hp.lexer.warns, warn)
}

func (hp *hintParser) lastErrorAsWarn() {
	hp.lexer.lastErrorAsWarn()
}
