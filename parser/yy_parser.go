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

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
)

var (
	// ErrSyntax returns for sql syntax error.
	ErrSyntax = terror.ClassParser.New(mysql.ErrSyntax, mysql.MySQLErrName[mysql.ErrSyntax])
	// ErrParse returns for sql parse error.
	ErrParse = terror.ClassParser.New(mysql.ErrParse, mysql.MySQLErrName[mysql.ErrParse])
	// ErrUnknownCharacterSet returns for no character set found error.
	ErrUnknownCharacterSet = terror.ClassParser.New(mysql.ErrUnknownCharacterSet, mysql.MySQLErrName[mysql.ErrUnknownCharacterSet])
	// ErrInvalidYearColumnLength returns for illegal column length for year type.
	ErrInvalidYearColumnLength = terror.ClassParser.New(mysql.ErrInvalidYearColumnLength, mysql.MySQLErrName[mysql.ErrInvalidYearColumnLength])
	// ErrWrongArguments returns for illegal argument.
	ErrWrongArguments = terror.ClassParser.New(mysql.ErrWrongArguments, mysql.MySQLErrName[mysql.ErrWrongArguments])
	// ErrWrongFieldTerminators returns for illegal field terminators.
	ErrWrongFieldTerminators = terror.ClassParser.New(mysql.ErrWrongFieldTerminators, mysql.MySQLErrName[mysql.ErrWrongFieldTerminators])
	// ErrTooBigDisplayWidth returns for data display width exceed limit .
	ErrTooBigDisplayWidth = terror.ClassParser.New(mysql.ErrTooBigDisplaywidth, mysql.MySQLErrName[mysql.ErrTooBigDisplaywidth])
	// ErrTooBigPrecision returns for data precision exceed limit.
	ErrTooBigPrecision = terror.ClassParser.New(mysql.ErrTooBigPrecision, mysql.MySQLErrName[mysql.ErrTooBigPrecision])
	// ErrUnknownAlterLock returns for no alter lock type found error.
	ErrUnknownAlterLock = terror.ClassParser.New(mysql.ErrUnknownAlterLock, mysql.MySQLErrName[mysql.ErrUnknownAlterLock])
	// ErrUnknownAlterAlgorithm returns for no alter algorithm found error.
	ErrUnknownAlterAlgorithm = terror.ClassParser.New(mysql.ErrUnknownAlterAlgorithm, mysql.MySQLErrName[mysql.ErrUnknownAlterAlgorithm])
	// ErrWrongValue returns for wrong value
	ErrWrongValue = terror.ClassParser.New(mysql.ErrWrongValue, mysql.MySQLErrName[mysql.ErrWrongValue])
	// ErrWarnDeprecatedSyntaxNoReplacement return when the syntax was deprecated and there is no replacement.
	ErrWarnDeprecatedSyntaxNoReplacement = terror.ClassParser.New(mysql.ErrWarnDeprecatedSyntaxNoReplacement, mysql.MySQLErrName[mysql.ErrWarnDeprecatedSyntaxNoReplacement])
	// ErrWarnDeprecatedIntegerDisplayWidth share the same code 1681, and it will be returned when length is specified in integer.
	ErrWarnDeprecatedIntegerDisplayWidth = terror.ClassParser.New(mysql.ErrWarnDeprecatedSyntaxNoReplacement, "Integer display width is deprecated and will be removed in a future release.")
	// SpecFieldPattern special result field pattern
	SpecFieldPattern = regexp.MustCompile(`(\/\*!(M?[0-9]{5,6})?|\*\/)`)
	specCodeStart    = regexp.MustCompile(`^\/\*!(M?[0-9]{5,6})?[ \t]*`)
	specCodeEnd      = regexp.MustCompile(`[ \t]*\*\/$`)
)

// TrimComment trim comment for special comment code of MySQL.
func TrimComment(txt string) string {
	txt = specCodeStart.ReplaceAllString(txt, "")
	return specCodeEnd.ReplaceAllString(txt, "")
}

// Parser represents a parser instance. Some temporary objects are stored in it to reduce object allocation during Parse function.
type Parser struct {
	charset    string
	collation  string
	result     []ast.StmtNode
	src        string
	lexer      Scanner
	hintParser *hintParser

	explicitCharset bool

	// the following fields are used by yyParse to reduce allocation.
	cache  []yySymType
	yylval yySymType
	yyVAL  *yySymType
}

type stmtTexter interface {
	stmtText() string
}

// New returns a Parser object.
func New() *Parser {
	if ast.NewValueExpr == nil ||
		ast.NewParamMarkerExpr == nil ||
		ast.NewHexLiteral == nil ||
		ast.NewBitLiteral == nil {
		panic("no parser driver (forgotten import?) https://github.com/pingcap/parser/issues/43")
	}

	p := &Parser{
		cache: make([]yySymType, 200),
	}
	p.EnableWindowFunc(true)
	return p
}

// Parse parses a query string to raw ast.StmtNode.
// If charset or collation is "", default charset and collation will be used.
func (parser *Parser) Parse(sql, charset, collation string) (stmt []ast.StmtNode, warns []error, err error) {
	if charset == "" {
		charset = mysql.DefaultCharset
	}
	if collation == "" {
		collation = mysql.DefaultCollationName
	}
	parser.charset = charset
	parser.collation = collation
	parser.src = sql
	parser.result = parser.result[:0]

	var l yyLexer
	parser.lexer.reset(sql)
	l = &parser.lexer
	yyParse(l, parser)

	warns, errs := l.Errors()
	if len(warns) > 0 {
		warns = append([]error(nil), warns...)
	} else {
		warns = nil
	}
	if len(errs) != 0 {
		return nil, warns, errors.Trace(errs[0])
	}
	for _, stmt := range parser.result {
		ast.SetFlag(stmt)
	}
	return parser.result, warns, nil
}

func (parser *Parser) lastErrorAsWarn() {
	parser.lexer.lastErrorAsWarn()
}

// ParseOneStmt parses a query and returns an ast.StmtNode.
// The query must have one statement, otherwise ErrSyntax is returned.
func (parser *Parser) ParseOneStmt(sql, charset, collation string) (ast.StmtNode, error) {
	stmts, _, err := parser.Parse(sql, charset, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(stmts) != 1 {
		return nil, ErrSyntax
	}
	ast.SetFlag(stmts[0])
	return stmts[0], nil
}

// SetSQLMode sets the SQL mode for parser.
func (parser *Parser) SetSQLMode(mode mysql.SQLMode) {
	parser.lexer.SetSQLMode(mode)
}

// EnableWindowFunc controls whether the parser to parse syntax related with window function.
func (parser *Parser) EnableWindowFunc(val bool) {
	parser.lexer.EnableWindowFunc(val)
}

// ParseErrorWith returns "You have a syntax error near..." error message compatible with mysql.
func ParseErrorWith(errstr string, lineno int) error {
	if len(errstr) > mysql.ErrTextLength {
		errstr = errstr[:mysql.ErrTextLength]
	}
	return fmt.Errorf("near '%-.80s' at line %d", errstr, lineno)
}

// The select statement is not at the end of the whole statement, if the last
// field text was set from its offset to the end of the src string, update
// the last field text.
func (parser *Parser) setLastSelectFieldText(st *ast.SelectStmt, lastEnd int) {
	lastField := st.Fields.Fields[len(st.Fields.Fields)-1]
	if lastField.Offset+len(lastField.Text()) >= len(parser.src)-1 {
		lastField.SetText(parser.src[lastField.Offset:lastEnd])
	}
}

func (parser *Parser) startOffset(v *yySymType) int {
	return v.offset
}

func (parser *Parser) endOffset(v *yySymType) int {
	offset := v.offset
	for offset > 0 && unicode.IsSpace(rune(parser.src[offset-1])) {
		offset--
	}
	return offset
}

func (parser *Parser) parseHint(input string) ([]*ast.TableOptimizerHint, []error) {
	if parser.hintParser == nil {
		parser.hintParser = newHintParser()
	}
	return parser.hintParser.parse(input, parser.lexer.GetSQLMode(), parser.lexer.lastHintPos)
}

func toInt(l yyLexer, lval *yySymType, str string) int {
	n, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		e := err.(*strconv.NumError)
		if e.Err == strconv.ErrRange {
			// TODO: toDecimal maybe out of range still.
			// This kind of error should be throw to higher level, because truncated data maybe legal.
			// For example, this SQL returns error:
			// create table test (id decimal(30, 0));
			// insert into test values(123456789012345678901234567890123094839045793405723406801943850);
			// While this SQL:
			// select 1234567890123456789012345678901230948390457934057234068019438509023041874359081325875128590860234789847359871045943057;
			// get value 99999999999999999999999999999999999999999999999999999999999999999
			return toDecimal(l, lval, str)
		}
		l.AppendError(l.Errorf("integer literal: %v", err))
		return int(unicode.ReplacementChar)
	}

	switch {
	case n <= math.MaxInt64:
		lval.item = int64(n)
	default:
		lval.item = n
	}
	return intLit
}

func toDecimal(l yyLexer, lval *yySymType, str string) int {
	dec, err := ast.NewDecimal(str)
	if err != nil {
		l.AppendError(l.Errorf("decimal literal: %v", err))
	}
	lval.item = dec
	return decLit
}

func toFloat(l yyLexer, lval *yySymType, str string) int {
	n, err := strconv.ParseFloat(str, 64)
	if err != nil {
		l.AppendError(l.Errorf("float literal: %v", err))
		return int(unicode.ReplacementChar)
	}

	lval.item = n
	return floatLit
}

// See https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func toHex(l yyLexer, lval *yySymType, str string) int {
	h, err := ast.NewHexLiteral(str)
	if err != nil {
		l.AppendError(l.Errorf("hex literal: %v", err))
		return int(unicode.ReplacementChar)
	}
	lval.item = h
	return hexLit
}

// See https://dev.mysql.com/doc/refman/5.7/en/bit-type.html
func toBit(l yyLexer, lval *yySymType, str string) int {
	b, err := ast.NewBitLiteral(str)
	if err != nil {
		l.AppendError(l.Errorf("bit literal: %v", err))
		return int(unicode.ReplacementChar)
	}
	lval.item = b
	return bitLit
}

func getUint64FromNUM(num interface{}) uint64 {
	switch v := num.(type) {
	case int64:
		return uint64(v)
	case uint64:
		return v
	}
	return 0
}

func getInt64FromNUM(num interface{}) (val int64, errMsg string) {
	switch v := num.(type) {
	case int64:
		return v, ""
	}
	return -1, fmt.Sprintf("%d is out of range [–9223372036854775808,9223372036854775807]", num)
}
