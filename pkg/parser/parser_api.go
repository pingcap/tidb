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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/parser/types"
)

var (
	// ErrSyntax returns for sql syntax error.
	ErrSyntax = terror.ClassParser.NewStd(mysql.ErrSyntax)
	// ErrParse returns for sql parse error.
	ErrParse = terror.ClassParser.NewStd(mysql.ErrParse)
	// ErrUnknownCharacterSet returns for no character set found error.
	ErrUnknownCharacterSet = terror.ClassParser.NewStd(mysql.ErrUnknownCharacterSet)
	// ErrInvalidYearColumnLength returns for illegal column length for year type.
	ErrInvalidYearColumnLength = terror.ClassParser.NewStd(mysql.ErrInvalidYearColumnLength)
	// ErrWrongArguments returns for illegal argument.
	ErrWrongArguments = terror.ClassParser.NewStd(mysql.ErrWrongArguments)
	// ErrWrongFieldTerminators returns for illegal field terminators.
	ErrWrongFieldTerminators = terror.ClassParser.NewStd(mysql.ErrWrongFieldTerminators)
	// ErrTooBigDisplayWidth returns for data display width exceed limit .
	ErrTooBigDisplayWidth = terror.ClassParser.NewStd(mysql.ErrTooBigDisplaywidth)
	// ErrTooBigPrecision returns for data precision exceed limit.
	ErrTooBigPrecision = terror.ClassParser.NewStd(mysql.ErrTooBigPrecision)
	// ErrUnknownAlterLock returns for no alter lock type found error.
	ErrUnknownAlterLock = terror.ClassParser.NewStd(mysql.ErrUnknownAlterLock)
	// ErrUnknownAlterAlgorithm returns for no alter algorithm found error.
	ErrUnknownAlterAlgorithm = terror.ClassParser.NewStd(mysql.ErrUnknownAlterAlgorithm)
	// ErrWrongValue returns for wrong value
	ErrWrongValue = terror.ClassParser.NewStd(mysql.ErrWrongValue)
	// ErrWarnDeprecatedSyntax return when the syntax was deprecated
	ErrWarnDeprecatedSyntax = terror.ClassParser.NewStd(mysql.ErrWarnDeprecatedSyntax)
	// ErrWarnDeprecatedSyntaxNoReplacement return when the syntax was deprecated and there is no replacement.
	ErrWarnDeprecatedSyntaxNoReplacement = terror.ClassParser.NewStd(mysql.ErrWarnDeprecatedSyntaxNoReplacement)
	// ErrWrongUsage returns for incorrect usages.
	ErrWrongUsage = terror.ClassParser.NewStd(mysql.ErrWrongUsage)
	// ErrWrongDBName returns for incorrect DB name.
	ErrWrongDBName = terror.ClassParser.NewStd(mysql.ErrWrongDBName)
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

//revive:disable:exported

// ParserConfig is the parser config.
type ParserConfig struct {
	EnableWindowFunction        bool
	EnableStrictDoubleTypeCheck bool
	SkipPositionRecording       bool
}

//revive:enable:exported

// Parser represents a parser instance. Some temporary objects are stored in it
// to reduce object allocation during Parse function.
type Parser struct {
	charset    string
	collation  string
	result     []ast.StmtNode
	src        string
	lexer      Scanner
	hintParser *hintParser
	handParser *HandParser

	explicitCharset       bool
	strictDoubleFieldType bool

	// hintBridgeFn is stored once in New() to avoid per-ParseSQL heap allocation
	// of the parser.hintBridge method value.
	hintBridgeFn HintParseFn
}

// New returns a Parser object with default SQL mode.
func New() *Parser {
	if ast.NewValueExpr == nil ||
		ast.NewParamMarkerExpr == nil ||
		ast.NewHexLiteral == nil ||
		ast.NewBitLiteral == nil {
		panic("no parser driver (forgotten import?) https://github.com/pingcap/parser/issues/43")
	}

	p := &Parser{
		handParser: NewHandParser(),
	}
	// Bind method value once to avoid per-ParseSQL closure allocation.
	p.hintBridgeFn = p.hintBridge
	p.reset()
	return p
}

// Reset resets the parser.
func (parser *Parser) Reset() {
	parser.reset()
}

func (parser *Parser) reset() {
	parser.explicitCharset = false
	parser.strictDoubleFieldType = false
	parser.EnableWindowFunc(true)
	parser.SetStrictDoubleTypeCheck(true)
	mode, _ := mysql.GetSQLMode(mysql.DefaultSQLMode)
	parser.SetSQLMode(mode)
	parser.handParser.Reset() // Ensure clean state for hand parser
}

// SetStrictDoubleTypeCheck enables/disables strict double type check.
func (parser *Parser) SetStrictDoubleTypeCheck(val bool) {
	parser.strictDoubleFieldType = val
}

// SetParserConfig sets the parser config.
func (parser *Parser) SetParserConfig(config ParserConfig) {
	parser.EnableWindowFunc(config.EnableWindowFunction)
	parser.SetStrictDoubleTypeCheck(config.EnableStrictDoubleTypeCheck)
	parser.lexer.skipPositionRecording = config.SkipPositionRecording

	// Sync config to hand parser
	parser.handParser.EnableWindowFunc(config.EnableWindowFunction)
	parser.handParser.SetStrictDoubleFieldTypeCheck(config.EnableStrictDoubleTypeCheck)
}

// hintBridge is a bound method that parses optimizer hints.
func (parser *Parser) hintBridge(input string) ([]*ast.TableOptimizerHint, []error) {
	return parser.parseHint(input)
}

// ParseSQL parses a query string to raw ast.StmtNode.
func (parser *Parser) ParseSQL(sql string, params ...ParseParam) (stmt []ast.StmtNode, warns []error, err error) {
	parser.charset = mysql.DefaultCharset
	parser.collation = mysql.DefaultCollationName
	parser.lexer.reset(sql)
	for _, p := range params {
		if err := p.ApplyOn(parser); err != nil {
			return nil, nil, err
		}
	}
	parser.src = sql
	parser.result = parser.result[:0]

	// Configure the hand-written parser.
	parser.handParser.SetSQLMode(parser.lexer.GetSQLMode())

	// Set hint parse callback (uses stored method value, no allocation).
	parser.handParser.SetHintParse(parser.hintBridgeFn)

	// Initialize hand parser: Reset + Init + set charset/collation.
	parser.handParser.Reset()
	parser.handParser.Init(&parser.lexer, sql)
	parser.handParser.SetCharsetCollation(parser.charset, parser.collation)

	// Sync parser state to hand parser AFTER reset.
	parser.handParser.SetStrictDoubleFieldTypeCheck(parser.strictDoubleFieldType)

	hStmts, hWarns, hErr := parser.handParser.ParseSQL()

	// Merge scanner-level warnings/errors.
	scannerWarns, scannerErrs := parser.lexer.Errors()
	warns = hWarns
	if len(scannerWarns) > 0 {
		warns = append(warns, scannerWarns...)
	}

	if len(scannerErrs) > 0 {
		return nil, warns, errors.Trace(scannerErrs[0])
	}
	if hErr != nil {
		return nil, warns, errors.Trace(hErr)
	}

	parser.result = hStmts
	return hStmts, warns, nil
}

// Parse parses a query string to raw ast.StmtNode.
// If charset or collation is "", default charset and collation will be used.
func (parser *Parser) Parse(sql, charset, collation string) (stmt []ast.StmtNode, warns []error, err error) {
	return parser.ParseSQL(sql, CharsetConnection(charset), CollationConnection(collation))
}

// ParseOneStmt parses a query and returns an ast.StmtNode.
// The query must have one statement, otherwise ErrSyntax is returned.
func (parser *Parser) ParseOneStmt(sql, charset, collation string) (ast.StmtNode, error) {
	stmts, _, err := parser.ParseSQL(sql, CharsetConnection(charset), CollationConnection(collation))
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
	parser.handParser.SetSQLMode(mode)
}

// EnableWindowFunc controls whether the parser to parse syntax related with window function.
func (parser *Parser) EnableWindowFunc(val bool) {
	parser.lexer.EnableWindowFunc(val)
	parser.handParser.EnableWindowFunc(val)
}

// ParseErrorWith returns "You have a syntax error near..." error message compatible with mysql.
func ParseErrorWith(errstr string, lineno int) error {
	if len(errstr) > mysql.ErrTextLength {
		errstr = errstr[:mysql.ErrTextLength]
	}
	return fmt.Errorf("near '%-.80s' at line %d", errstr, lineno)
}

func (parser *Parser) parseHint(input string) ([]*ast.TableOptimizerHint, []error) {
	if parser.hintParser == nil {
		parser.hintParser = newHintParser()
	}
	return parser.hintParser.parse(input, parser.lexer.GetSQLMode(), parser.lexer.lastHintPos)
}

// ---------- Literal conversion helpers (used by Scanner.Lex in lexer.go) ----------

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

// ---------- ParseParam types ----------

var (
	_ ParseParam = CharsetConnection("")
	_ ParseParam = CollationConnection("")
	_ ParseParam = CharsetClient("")
)

// ParseParam represents the parameter of parsing.
type ParseParam interface {
	ApplyOn(*Parser) error
}

// CharsetConnection is used for literals specified without a character set.
type CharsetConnection string

// ApplyOn implements ParseParam interface.
func (c CharsetConnection) ApplyOn(p *Parser) error {
	if c == "" {
		p.charset = mysql.DefaultCharset
	} else {
		p.charset = string(c)
	}
	p.lexer.connection = charset.FindEncoding(string(c))
	return nil
}

// CollationConnection is used for literals specified without a collation.
type CollationConnection string

// ApplyOn implements ParseParam interface.
func (c CollationConnection) ApplyOn(p *Parser) error {
	if c == "" {
		p.collation = mysql.DefaultCollationName
	} else {
		p.collation = string(c)
	}
	return nil
}

// CharsetClient specifies the charset of a SQL.
// This is used to decode the SQL into a utf-8 string.
type CharsetClient string

// ApplyOn implements ParseParam interface.
func (c CharsetClient) ApplyOn(p *Parser) error {
	p.lexer.client = charset.FindEncoding(string(c))
	return nil
}
