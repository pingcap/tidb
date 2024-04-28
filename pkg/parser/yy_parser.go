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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
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

// Parser represents a parser instance. Some temporary objects are stored in it to reduce object allocation during Parse function.
type Parser struct {
	charset    string
	collation  string
	result     []ast.StmtNode
	src        string
	lexer      Scanner
	hintParser *hintParser

	explicitCharset       bool
	strictDoubleFieldType bool

	// the following fields are used by yyParse to reduce allocation.
	cache  []yySymType
	yylval yySymType
	yyVAL  *yySymType
}

func yySetOffset(yyVAL *yySymType, offset int) {
	if yyVAL.expr != nil {
		yyVAL.expr.SetOriginTextPosition(offset)
	}
}

func yyhintSetOffset(_ *yyhintSymType, _ int) {
}

type stmtTexter interface {
	stmtText() string
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
		cache: make([]yySymType, 200),
	}
	p.EnableWindowFunc(true)
	p.SetStrictDoubleTypeCheck(true)
	mode, _ := mysql.GetSQLMode(mysql.DefaultSQLMode)
	p.SetSQLMode(mode)
	return p
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
}

// ParseSQL parses a query string to raw ast.StmtNode.
func (parser *Parser) ParseSQL(sql string, params ...ParseParam) (stmt []ast.StmtNode, warns []error, err error) {
	resetParams(parser)
	parser.lexer.reset(sql)
	for _, p := range params {
		if err := p.ApplyOn(parser); err != nil {
			return nil, nil, err
		}
	}
	parser.src = sql
	parser.result = parser.result[:0]

	var l yyLexer = &parser.lexer
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

// Parse parses a query string to raw ast.StmtNode.
// If charset or collation is "", default charset and collation will be used.
func (parser *Parser) Parse(sql, charset, collation string) (stmt []ast.StmtNode, warns []error, err error) {
	return parser.ParseSQL(sql, CharsetConnection(charset), CollationConnection(collation))
}

func (parser *Parser) lastErrorAsWarn() {
	parser.lexer.lastErrorAsWarn()
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
	if st.Kind != ast.SelectStmtKindSelect {
		return
	}
	lastField := st.Fields.Fields[len(st.Fields.Fields)-1]
	if lastField.Offset+len(lastField.OriginalText()) >= len(parser.src)-1 {
		lastField.SetText(parser.lexer.client, parser.src[lastField.Offset:lastEnd])
	}
}

func (*Parser) startOffset(v *yySymType) int {
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
		return invalid
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
		if terror.ErrorEqual(err, types.ErrDataOutOfRange) {
			l.AppendWarn(types.ErrTruncatedWrongValue.FastGenByArgs("DECIMAL", dec))
			dec, _ = ast.NewDecimal(mysql.DefaultDecimal)
		} else {
			l.AppendError(l.Errorf("decimal literal: %v", err))
		}
	}
	lval.item = dec
	return decLit
}

func toFloat(l yyLexer, lval *yySymType, str string) int {
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

	lval.item = n
	return floatLit
}

// See https://dev.mysql.com/doc/refman/5.7/en/hexadecimal-literals.html
func toHex(l yyLexer, lval *yySymType, str string) int {
	h, err := ast.NewHexLiteral(str)
	if err != nil {
		l.AppendError(l.Errorf("hex literal: %v", err))
		return invalid
	}
	lval.item = h
	return hexLit
}

// See https://dev.mysql.com/doc/refman/5.7/en/bit-type.html
func toBit(l yyLexer, lval *yySymType, str string) int {
	b, err := ast.NewBitLiteral(str)
	if err != nil {
		l.AppendError(l.Errorf("bit literal: %v", err))
		return invalid
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
	default:
		return -1, fmt.Sprintf("%d is out of range [–9223372036854775808,9223372036854775807]", num)
	}
}

func isRevokeAllGrant(roleOrPrivList []*ast.RoleOrPriv) bool {
	if len(roleOrPrivList) != 2 {
		return false
	}
	priv, err := roleOrPrivList[0].ToPriv()
	if err != nil {
		return false
	}
	if priv.Priv != mysql.AllPriv {
		return false
	}
	priv, err = roleOrPrivList[1].ToPriv()
	if err != nil {
		return false
	}
	if priv.Priv != mysql.GrantPriv {
		return false
	}
	return true
}

// convertToRole tries to convert elements of roleOrPrivList to RoleIdentity
func convertToRole(roleOrPrivList []*ast.RoleOrPriv) ([]*auth.RoleIdentity, error) {
	var roles []*auth.RoleIdentity
	for _, elem := range roleOrPrivList {
		role, err := elem.ToRole()
		if err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}
	return roles, nil
}

// convertToPriv tries to convert elements of roleOrPrivList to PrivElem
func convertToPriv(roleOrPrivList []*ast.RoleOrPriv) ([]*ast.PrivElem, error) {
	var privileges []*ast.PrivElem
	for _, elem := range roleOrPrivList {
		priv, err := elem.ToPriv()
		if err != nil {
			return nil, err
		}
		privileges = append(privileges, priv)
	}
	return privileges, nil
}

var (
	_ ParseParam = CharsetConnection("")
	_ ParseParam = CollationConnection("")
	_ ParseParam = CharsetClient("")
)

func resetParams(p *Parser) {
	p.charset = mysql.DefaultCharset
	p.collation = mysql.DefaultCollationName
}

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
