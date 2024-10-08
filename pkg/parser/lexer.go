// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tidbfeature "github.com/pingcap/tidb/pkg/parser/tidb"
)

var _ = yyLexer(&Scanner{})

// Pos represents the position of a token.
type Pos struct {
	Line   int
	Col    int
	Offset int
}

// Scanner implements the yyLexer interface.
type Scanner struct {
	r   reader
	buf bytes.Buffer

	client     charset.Encoding
	connection charset.Encoding

	errs         []error
	warns        []error
	stmtStartPos int

	// inBangComment is true if we are inside a `/*! ... */` block.
	// It is used to ignore a stray `*/` when scanning.
	inBangComment bool

	sqlMode mysql.SQLMode

	// If the lexer should recognize keywords for window function.
	// It may break the compatibility when support those keywords,
	// because some application may already use them as identifiers.
	supportWindowFunc bool

	// Whether record the original text keyword position to the AST node.
	skipPositionRecording bool

	// lastScanOffset indicates last offset returned by scan().
	// It's used to substring sql in syntax error message.
	lastScanOffset int

	// lastKeyword records the previous keyword returned by scan().
	// determine whether an optimizer hint should be parsed or ignored.
	lastKeyword int
	// lastKeyword2 records the keyword before lastKeyword, it is used
	// to disambiguate hint after for update, which should be ignored.
	lastKeyword2 int
	// lastKeyword3 records the keyword before lastKeyword2, it is used
	// to disambiguate hint after create binding for update, which should
	// be pertained.
	lastKeyword3 int

	// hintPos records the start position of the previous optimizer hint.
	lastHintPos Pos

	// true if a dot follows an identifier
	identifierDot bool

	// keepHint, if true, Scanner will keep hint when normalizing .
	keepHint bool
}

// Errors returns the errors and warns during a scan.
func (s *Scanner) Errors() (warns []error, errs []error) {
	return s.warns, s.errs
}

// reset resets the sql string to be scanned.
func (s *Scanner) reset(sql string) {
	s.client = charset.FindEncoding(mysql.DefaultCharset)
	s.connection = charset.FindEncoding(mysql.DefaultCharset)
	s.r = reader{s: sql, p: Pos{Line: 1}, l: len(sql)}
	s.buf.Reset()
	s.errs = s.errs[:0]
	s.warns = s.warns[:0]
	s.stmtStartPos = 0
	s.inBangComment = false
	s.lastKeyword = 0
	s.identifierDot = false
}

func (s *Scanner) stmtText() string {
	endPos := s.r.pos().Offset
	if s.r.s[endPos-1] == '\n' {
		endPos = endPos - 1 // trim new line
	}
	if s.r.s[s.stmtStartPos] == '\n' {
		s.stmtStartPos++
	}

	text := s.r.s[s.stmtStartPos:endPos]

	s.stmtStartPos = endPos
	return text
}

// Errorf tells scanner something is wrong.
// Scanner satisfies yyLexer interface which need this function.
func (s *Scanner) Errorf(format string, a ...interface{}) (err error) {
	str := fmt.Sprintf(format, a...)
	val := s.r.s[s.lastScanOffset:]
	var lenStr = ""
	if len(val) > 2048 {
		lenStr = "(total length " + strconv.Itoa(len(val)) + ")"
		val = val[:2048]
	}
	err = fmt.Errorf("line %d column %d near \"%s\"%s %s",
		s.r.p.Line, s.r.p.Col, val, str, lenStr)
	return
}

// AppendError sets error into scanner.
// Scanner satisfies yyLexer interface which need this function.
func (s *Scanner) AppendError(err error) {
	if err == nil {
		return
	}
	s.errs = append(s.errs, err)
}

// AppendWarn sets warning into scanner.
func (s *Scanner) AppendWarn(err error) {
	if err == nil {
		return
	}
	s.warns = append(s.warns, err)
}

// convert2System convert lit from client encoding to system encoding which is utf8mb4.
func (s *Scanner) convert2System(tok int, lit string) (int, string) {
	utf8Lit, err := s.client.Transform(nil, charset.HackSlice(lit), charset.OpDecodeReplace)
	if err != nil {
		s.AppendWarn(err)
	}

	return tok, charset.HackString(utf8Lit)
}

// convert2Connection convert lit from client encoding to connection encoding.
func (s *Scanner) convert2Connection(tok int, lit string) (int, string) {
	if mysql.IsUTF8Charset(s.client.Name()) {
		return tok, lit
	}
	utf8Lit, err := s.client.Transform(nil, charset.HackSlice(lit), charset.OpDecodeReplace)
	if err != nil {
		s.AppendError(err)
		if s.sqlMode.HasStrictMode() && s.client.Tp() == s.connection.Tp() {
			return invalid, lit
		}
		s.lastErrorAsWarn()
	}

	// It is definitely valid if `client` is the same with `connection`, so just transform if they are not the same.
	if s.client.Tp() != s.connection.Tp() {
		utf8Lit, _ = s.connection.Transform(nil, utf8Lit, charset.OpReplaceNoErr)
	}
	return tok, charset.HackString(utf8Lit)
}

func (s *Scanner) getNextToken() int {
	r := s.r
	tok, pos, lit := s.scan()
	if tok == identifier {
		tok = s.handleIdent(&yySymType{})
	}
	if tok == identifier {
		if tok1 := s.isTokenIdentifier(lit, pos.Offset); tok1 != 0 {
			tok = tok1
		}
	}
	s.r = r
	return tok
}

func (s *Scanner) getNextTwoTokens() (tok1 int, tok2 int) {
	r := s.r
	tok1, pos, lit := s.scan()
	if tok1 == identifier {
		tok1 = s.handleIdent(&yySymType{})
	}
	if tok1 == identifier {
		if tmpToken := s.isTokenIdentifier(lit, pos.Offset); tmpToken != 0 {
			tok1 = tmpToken
		}
	}
	tok2, pos, lit = s.scan()
	if tok2 == identifier {
		tok2 = s.handleIdent(&yySymType{})
	}
	if tok2 == identifier {
		if tmpToken := s.isTokenIdentifier(lit, pos.Offset); tmpToken != 0 {
			tok2 = tmpToken
		}
	}
	s.r = r
	return tok1, tok2
}

// Lex returns a token and store the token value in v.
// Scanner satisfies yyLexer interface.
// 0 and invalid are special token id this function would return:
// return 0 tells parser that scanner meets EOF,
// return invalid tells parser that scanner meets illegal character.
func (s *Scanner) Lex(v *yySymType) int {
	tok, pos, lit := s.scan()
	s.lastScanOffset = pos.Offset
	s.lastKeyword3 = s.lastKeyword2
	s.lastKeyword2 = s.lastKeyword
	s.lastKeyword = 0
	v.offset = pos.Offset
	v.ident = lit
	if tok == identifier {
		tok = s.handleIdent(v)
	}
	if tok == identifier {
		if tok1 := s.isTokenIdentifier(lit, pos.Offset); tok1 != 0 {
			tok = tok1
			s.lastKeyword = tok1
		}
	}
	if s.sqlMode.HasANSIQuotesMode() &&
		tok == stringLit &&
		s.r.s[v.offset] == '"' {
		tok = identifier
	}

	if tok == pipes && !(s.sqlMode.HasPipesAsConcatMode()) {
		return pipesAsOr
	}

	if tok == not && s.sqlMode.HasHighNotPrecedenceMode() {
		return not2
	}
	if (tok == as || tok == member) && s.getNextToken() == of {
		_, pos, lit = s.scan()
		v.ident = fmt.Sprintf("%s %s", v.ident, lit)
		s.lastScanOffset = pos.Offset
		v.offset = pos.Offset
		if tok == as {
			s.lastKeyword = asof
			return asof
		}
		s.lastKeyword = memberof
		return memberof
	}
	if tok == to {
		tok1, tok2 := s.getNextTwoTokens()
		if tok1 == timestampType && tok2 == stringLit {
			_, pos, lit = s.scan()
			v.ident = fmt.Sprintf("%s %s", v.ident, lit)
			s.lastKeyword = toTimestamp
			s.lastScanOffset = pos.Offset
			v.offset = pos.Offset
			return toTimestamp
		}

		if tok1 == tsoType && tok2 == intLit {
			_, pos, lit = s.scan()
			v.ident = fmt.Sprintf("%s %s", v.ident, lit)
			s.lastKeyword = toTSO
			s.lastScanOffset = pos.Offset
			v.offset = pos.Offset
			return toTSO
		}
	}
	// fix shift/reduce conflict with DEFINED NULL BY xxx OPTIONALLY ENCLOSED
	if tok == optionally {
		tok1, tok2 := s.getNextTwoTokens()
		if tok1 == enclosed && tok2 == by {
			_, _, lit = s.scan()
			_, pos2, lit2 := s.scan()
			v.ident = fmt.Sprintf("%s %s %s", v.ident, lit, lit2)
			s.lastKeyword = optionallyEnclosedBy
			s.lastScanOffset = pos2.Offset
			v.offset = pos2.Offset
			return optionallyEnclosedBy
		}
	}

	switch tok {
	case intLit:
		return toInt(s, v, lit)
	case floatLit:
		return toFloat(s, v, lit)
	case decLit:
		return toDecimal(s, v, lit)
	case hexLit:
		return toHex(s, v, lit)
	case bitLit:
		return toBit(s, v, lit)
	case singleAtIdentifier, doubleAtIdentifier, cast, extract:
		v.item = lit
		return tok
	case null:
		v.item = nil
	case quotedIdentifier, identifier:
		tok = identifier
		s.identifierDot = s.r.peek() == '.'
		tok, v.ident = s.convert2System(tok, lit)
	case stringLit:
		tok, v.ident = s.convert2Connection(tok, lit)
	}

	return tok
}

// LexLiteral returns the value of the converted literal
func (s *Scanner) LexLiteral() interface{} {
	symType := &yySymType{}
	s.Lex(symType)
	if symType.item == nil {
		return symType.ident
	}
	return symType.item
}

// SetSQLMode sets the SQL mode for scanner.
func (s *Scanner) SetSQLMode(mode mysql.SQLMode) {
	s.sqlMode = mode
}

// GetSQLMode return the SQL mode of scanner.
func (s *Scanner) GetSQLMode() mysql.SQLMode {
	return s.sqlMode
}

// EnableWindowFunc controls whether the scanner recognize the keywords of window function.
func (s *Scanner) EnableWindowFunc(val bool) {
	s.supportWindowFunc = val
}

// setKeepHint set the keepHint flag when normalizing.
func (s *Scanner) setKeepHint(val bool) {
	s.keepHint = val
}

// InheritScanner returns a new scanner object which inherits configurations from the parent scanner.
func (s *Scanner) InheritScanner(sql string) *Scanner {
	return &Scanner{
		r:                 reader{s: sql},
		client:            s.client,
		sqlMode:           s.sqlMode,
		supportWindowFunc: s.supportWindowFunc,
	}
}

// NewScanner returns a new scanner object.
func NewScanner(s string) *Scanner {
	lexer := &Scanner{r: reader{s: s}}
	lexer.reset(s)
	return lexer
}

func (*Scanner) handleIdent(lval *yySymType) int {
	str := lval.ident
	// A character string literal may have an optional character set introducer and COLLATE clause:
	// [_charset_name]'string' [COLLATE collation_name]
	// See https://dev.mysql.com/doc/refman/5.7/en/charset-literal.html
	if !strings.HasPrefix(str, "_") {
		return identifier
	}
	cs, _ := charset.GetCharsetInfo(str[1:])
	if cs == nil {
		return identifier
	}
	lval.ident = cs.Name
	return underscoreCS
}

func (s *Scanner) skipWhitespace() byte {
	return s.r.incAsLongAs(func(b byte) bool {
		return unicode.IsSpace(rune(b))
	})
}

func (s *Scanner) scan() (tok int, pos Pos, lit string) {
	ch0 := s.r.peek()
	if unicode.IsSpace(rune(ch0)) {
		ch0 = s.skipWhitespace()
	}
	pos = s.r.pos()
	if s.r.eof() {
		// when scanner meets EOF, the returned token should be 0,
		// because 0 is a special token id to remind the parser that stream is end.
		return 0, pos, ""
	}

	if isIdentExtend(ch0) {
		return scanIdentifier(s)
	}

	// search a trie to get a token.
	node := &ruleTable
	for !(node.childs[ch0] == nil || s.r.eof()) {
		node = node.childs[ch0]
		if node.fn != nil {
			return node.fn(s)
		}
		s.r.inc()
		ch0 = s.r.peek()
	}

	tok, lit = node.token, s.r.data(&pos)
	return
}

func startWithXx(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	if s.r.peek() == '\'' {
		s.r.inc()
		s.scanHex()
		if s.r.peek() == '\'' {
			s.r.inc()
			tok, lit = hexLit, s.r.data(&pos)
		} else {
			tok = invalid
		}
		return
	}
	s.r.updatePos(pos)
	return scanIdentifier(s)
}

func startWithNn(s *Scanner) (tok int, pos Pos, lit string) {
	tok, pos, lit = scanIdentifier(s)
	// The National Character Set, N'some text' or n'some test'.
	// See https://dev.mysql.com/doc/refman/5.7/en/string-literals.html
	// and https://dev.mysql.com/doc/refman/5.7/en/charset-national.html
	if lit == "N" || lit == "n" {
		if s.r.peek() == '\'' {
			tok = underscoreCS
			lit = "utf8"
		}
	}
	return
}

func startWithBb(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	if s.r.peek() == '\'' {
		s.r.inc()
		s.scanBit()
		if s.r.peek() == '\'' {
			s.r.inc()
			tok, lit = bitLit, s.r.data(&pos)
		} else {
			tok = invalid
		}
		return
	}
	s.r.updatePos(pos)
	return scanIdentifier(s)
}

func startWithSharp(s *Scanner) (tok int, pos Pos, lit string) {
	s.r.incAsLongAs(func(ch byte) bool {
		return ch != '\n'
	})
	return s.scan()
}

func startWithDash(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	if strings.HasPrefix(s.r.s[pos.Offset:], "--") {
		remainLen := len(s.r.s[pos.Offset:])
		if remainLen == 2 || (remainLen > 2 && unicode.IsSpace(rune(s.r.s[pos.Offset+2]))) {
			s.r.incAsLongAs(func(ch byte) bool {
				return ch != '\n'
			})
			return s.scan()
		}
	}
	if strings.HasPrefix(s.r.s[pos.Offset:], "->>") {
		tok = juss
		s.r.incN(3)
		return
	}
	if strings.HasPrefix(s.r.s[pos.Offset:], "->") {
		tok = jss
		s.r.incN(2)
		return
	}
	tok = int('-')
	lit = "-"
	s.r.inc()
	return
}

func startWithSlash(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	if s.r.peek() != '*' {
		tok = int('/')
		lit = "/"
		return
	}

	isOptimizerHint := false
	currentCharIsStar := false

	s.r.inc() // we see '/*' so far.
	switch s.r.readByte() {
	case '!': // '/*!' MySQL-specific comments
		// See http://dev.mysql.com/doc/refman/5.7/en/comments.html
		// in '/*!', which we always recognize regardless of version.
		s.scanVersionDigits(5, 5)
		s.inBangComment = true
		return s.scan()

	case 'T': // '/*T' maybe TiDB-specific comments
		if s.r.peek() != '!' {
			// '/*TX' is just normal comment.
			break
		}
		s.r.inc()
		// in '/*T!', try to match the pattern '/*T![feature1,feature2,...]'.
		features := s.scanFeatureIDs()
		if tidbfeature.CanParseFeature(features...) {
			s.inBangComment = true
			return s.scan()
		}
	case 'M': // '/*M' maybe MariaDB-specific comments
		// no special treatment for now.

	case '+': // '/*+' optimizer hints
		// See https://dev.mysql.com/doc/refman/5.7/en/optimizer-hints.html
		if _, ok := hintedTokens[s.lastKeyword]; ok || s.keepHint {
			// only recognize optimizers hints directly followed by certain
			// keywords like SELECT, INSERT, etc., only a special case "FOR UPDATE" needs to be handled
			// we will report a warning in order to match MySQL's behavior, but the hint content will be ignored
			if s.lastKeyword2 == forKwd {
				if s.lastKeyword3 == binding {
					// special case of `create binding for update`
					isOptimizerHint = true
				} else {
					s.warns = append(s.warns, ParseErrorWith(s.r.data(&pos), s.r.p.Line))
				}
			} else {
				isOptimizerHint = true
			}
		} else {
			s.AppendWarn(ErrWarnOptimizerHintWrongPos)
		}

	case '*': // '/**' if the next char is '/' it would close the comment.
		currentCharIsStar = true

	default:
	}

	// standard C-like comment. read until we see '*/' then drop it.
	for {
		if currentCharIsStar || s.r.incAsLongAs(func(ch byte) bool { return ch != '*' }) == '*' {
			switch s.r.readByte() {
			case '/':
				// Meets */, means comment end.
				if isOptimizerHint {
					s.lastHintPos = pos
					return hintComment, pos, s.r.data(&pos)
				}
				return s.scan()
			case '*':
				currentCharIsStar = true
				continue
			default:
				currentCharIsStar = false
				continue
			}
		}
		// unclosed comment or other errors.
		s.errs = append(s.errs, ParseErrorWith(s.r.data(&pos), s.r.p.Line))
		return
	}
}

func startWithStar(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()

	// skip and exit '/*!' if we see '*/'
	if s.inBangComment && s.r.peek() == '/' {
		s.inBangComment = false
		s.r.inc()
		return s.scan()
	}
	// otherwise it is just a normal star.
	s.identifierDot = false
	return '*', pos, "*"
}

func startWithAt(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()

	tok, lit = scanIdentifierOrString(s)
	switch tok {
	case '@':
		s.r.inc()
		stream := s.r.s[pos.Offset+2:]
		var prefix string
		for _, v := range []string{"global.", "session.", "local."} {
			if len(v) > len(stream) {
				continue
			}
			if strings.EqualFold(stream[:len(v)], v) {
				prefix = v
				s.r.incN(len(v))
				break
			}
		}
		tok, lit = scanIdentifierOrString(s)
		switch tok {
		case stringLit, quotedIdentifier:
			var sb strings.Builder
			sb.WriteString("@@")
			sb.WriteString(prefix)
			sb.WriteString(lit)
			tok, lit = doubleAtIdentifier, sb.String()
		case identifier:
			tok, lit = doubleAtIdentifier, s.r.data(&pos)
		}
	case invalid:
		return
	default:
		tok = singleAtIdentifier
	}

	return
}

func scanIdentifier(s *Scanner) (int, Pos, string) {
	pos := s.r.pos()
	s.r.incAsLongAs(isIdentChar)
	return identifier, pos, s.r.data(&pos)
}

func scanIdentifierOrString(s *Scanner) (tok int, lit string) {
	ch1 := s.r.peek()
	switch ch1 {
	case '\'', '"':
		tok, _, lit = startString(s)
	case '`':
		tok, _, lit = scanQuotedIdent(s)
	default:
		if isUserVarChar(ch1) {
			pos := s.r.pos()
			s.r.incAsLongAs(isUserVarChar)
			tok, lit = identifier, s.r.data(&pos)
		} else {
			tok = int(ch1)
		}
	}
	return
}

var (
	quotedIdentifier = -identifier
)

func scanQuotedIdent(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	s.buf.Reset()
	for !s.r.eof() {
		tPos := s.r.pos()
		if s.r.skipRune(s.client) {
			s.buf.WriteString(s.r.data(&tPos))
			continue
		}
		ch := s.r.readByte()
		if ch == '`' {
			if s.r.peek() != '`' {
				// don't return identifier in case that it's interpreted as keyword token later.
				tok, lit = quotedIdentifier, s.buf.String()
				return
			}
			s.r.inc()
		}
		s.buf.WriteByte(ch)
	}
	tok = invalid
	return
}

func startString(s *Scanner) (tok int, pos Pos, lit string) {
	return s.scanString()
}

// lazyBuf is used to avoid allocation if possible.
// it has a useBuf field indicates whether bytes.Buffer is necessary. if
// useBuf is false, we can avoid calling bytes.Buffer.String(), which
// make a copy of data and cause allocation.
type lazyBuf struct {
	useBuf bool
	r      *reader
	b      *bytes.Buffer
	p      *Pos
}

func (mb *lazyBuf) setUseBuf(str string) {
	if !mb.useBuf {
		mb.useBuf = true
		mb.b.Reset()
		mb.b.WriteString(str)
	}
}

func (mb *lazyBuf) writeRune(r rune, w int) {
	if mb.useBuf {
		if w > 1 {
			mb.b.WriteRune(r)
		} else {
			mb.b.WriteByte(byte(r))
		}
	}
}

func (mb *lazyBuf) data() string {
	var lit string
	if mb.useBuf {
		lit = mb.b.String()
	} else {
		lit = mb.r.data(mb.p)
		lit = lit[1 : len(lit)-1]
	}
	return lit
}

func (s *Scanner) scanString() (tok int, pos Pos, lit string) {
	tok, pos = stringLit, s.r.pos()
	ending := s.r.readByte()
	s.buf.Reset()
	for !s.r.eof() {
		tPos := s.r.pos()
		if s.r.skipRune(s.client) {
			s.buf.WriteString(s.r.data(&tPos))
			continue
		}
		ch0 := s.r.readByte()
		if ch0 == ending {
			if s.r.peek() != ending {
				lit = s.buf.String()
				return
			}
			s.r.inc()
			s.buf.WriteByte(ch0)
		} else if ch0 == '\\' && !s.sqlMode.HasNoBackslashEscapesMode() {
			if s.r.eof() {
				break
			}
			s.handleEscape(s.r.peek(), &s.buf)
			s.r.inc()
		} else {
			s.buf.WriteByte(ch0)
		}
	}

	tok = invalid
	return
}

// handleEscape handles the case in scanString when previous char is '\'.
func (*Scanner) handleEscape(b byte, buf *bytes.Buffer) {
	var ch0 byte
	/*
		\" \' \\ \n \0 \b \Z \r \t ==> escape to one char
		\% \_ ==> preserve both char
		other ==> remove \
	*/
	switch b {
	case 'n':
		ch0 = '\n'
	case '0':
		ch0 = 0
	case 'b':
		ch0 = 8
	case 'Z':
		ch0 = 26
	case 'r':
		ch0 = '\r'
	case 't':
		ch0 = '\t'
	case '%', '_':
		buf.WriteByte('\\')
		ch0 = b
	default:
		ch0 = b
	}
	buf.WriteByte(ch0)
}

func startWithNumber(s *Scanner) (tok int, pos Pos, lit string) {
	if s.identifierDot {
		return scanIdentifier(s)
	}
	pos = s.r.pos()
	tok = intLit
	ch0 := s.r.readByte()
	if ch0 == '0' {
		tok = intLit
		ch1 := s.r.peek()
		switch {
		case ch1 >= '0' && ch1 <= '7':
			s.r.inc()
			s.scanOct()
		case ch1 == 'x' || ch1 == 'X':
			s.r.inc()
			p1 := s.r.pos()
			s.scanHex()
			p2 := s.r.pos()
			// 0x, 0x7fz3 are identifier
			if p1 == p2 || isDigit(s.r.peek()) {
				s.r.incAsLongAs(isIdentChar)
				return identifier, pos, s.r.data(&pos)
			}
			tok = hexLit
		case ch1 == 'b':
			s.r.inc()
			p1 := s.r.pos()
			s.scanBit()
			p2 := s.r.pos()
			// 0b, 0b123, 0b1ab are identifier
			if p1 == p2 || isDigit(s.r.peek()) {
				s.r.incAsLongAs(isIdentChar)
				return identifier, pos, s.r.data(&pos)
			}
			tok = bitLit
		case ch1 == '.':
			return s.scanFloat(&pos)
		case ch1 == 'B':
			s.r.incAsLongAs(isIdentChar)
			return identifier, pos, s.r.data(&pos)
		}
	}

	s.scanDigits()
	ch0 = s.r.peek()
	if ch0 == '.' || ch0 == 'e' || ch0 == 'E' {
		return s.scanFloat(&pos)
	}

	// Identifiers may begin with a digit but unless quoted may not consist solely of digits.
	if !s.r.eof() && isIdentChar(ch0) {
		s.r.incAsLongAs(isIdentChar)
		return identifier, pos, s.r.data(&pos)
	}
	lit = s.r.data(&pos)
	return
}

func startWithDot(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	if s.identifierDot {
		return int('.'), pos, "."
	}
	if isDigit(s.r.peek()) {
		tok, p, l := s.scanFloat(&pos)
		if tok == identifier {
			return invalid, p, l
		}
		return tok, p, l
	}
	tok, lit = int('.'), "."
	return
}

func (s *Scanner) scanOct() {
	s.r.incAsLongAs(func(ch byte) bool {
		return ch >= '0' && ch <= '7'
	})
}

func (s *Scanner) scanHex() {
	s.r.incAsLongAs(func(ch byte) bool {
		return ch >= '0' && ch <= '9' ||
			ch >= 'a' && ch <= 'f' ||
			ch >= 'A' && ch <= 'F'
	})
}

func (s *Scanner) scanBit() {
	s.r.incAsLongAs(func(ch byte) bool {
		return ch == '0' || ch == '1'
	})
}

func (s *Scanner) scanFloat(beg *Pos) (tok int, pos Pos, lit string) {
	s.r.updatePos(*beg)
	// float = D1 . D2 e D3
	s.scanDigits()
	ch0 := s.r.peek()
	if ch0 == '.' {
		s.r.inc()
		s.scanDigits()
		ch0 = s.r.peek()
	}
	if ch0 == 'e' || ch0 == 'E' {
		s.r.inc()
		ch0 = s.r.peek()
		if ch0 == '-' || ch0 == '+' {
			s.r.inc()
		}
		if isDigit(s.r.peek()) {
			s.scanDigits()
			tok = floatLit
		} else {
			// D1 . D2 e XX when XX is not D3, parse the result to an identifier.
			// 9e9e = 9e9(float) + e(identifier)
			// 9est = 9est(identifier)
			s.r.updatePos(*beg)
			s.r.incAsLongAs(isIdentChar)
			tok = identifier
		}
	} else {
		tok = decLit
	}
	pos, lit = *beg, s.r.data(beg)
	return
}

func (s *Scanner) scanDigits() string {
	pos := s.r.pos()
	s.r.incAsLongAs(isDigit)
	return s.r.data(&pos)
}

// scanVersionDigits scans for `min` to `max` digits (range inclusive) used in
// `/*!12345 ... */` comments.
func (s *Scanner) scanVersionDigits(minv, maxv int) {
	pos := s.r.pos()
	for i := 0; i < maxv; i++ {
		ch := s.r.peek()
		if isDigit(ch) {
			s.r.inc()
		} else if i < minv {
			s.r.updatePos(pos)
			return
		} else {
			break
		}
	}
}

func (s *Scanner) scanFeatureIDs() (featureIDs []string) {
	pos := s.r.pos()
	const init, expectChar, obtainChar = 0, 1, 2
	state := init
	var b strings.Builder
	for !s.r.eof() {
		ch := s.r.peek()
		s.r.inc()
		switch state {
		case init:
			if ch == '[' {
				state = expectChar
				break
			}
			s.r.updatePos(pos)
			return nil
		case expectChar:
			if isIdentChar(ch) {
				b.WriteByte(ch)
				state = obtainChar
				break
			}
			s.r.updatePos(pos)
			return nil
		case obtainChar:
			if isIdentChar(ch) {
				b.WriteByte(ch)
				state = obtainChar
				break
			} else if ch == ',' {
				featureIDs = append(featureIDs, b.String())
				b.Reset()
				state = expectChar
				break
			} else if ch == ']' {
				featureIDs = append(featureIDs, b.String())
				return featureIDs
			}
			s.r.updatePos(pos)
			return nil
		}
	}
	s.r.updatePos(pos)
	return nil
}

func (s *Scanner) lastErrorAsWarn() {
	if len(s.errs) == 0 {
		return
	}
	s.warns = append(s.warns, s.errs[len(s.errs)-1])
	s.errs = s.errs[:len(s.errs)-1]
}

type reader struct {
	s string
	p Pos
	l int
}

var eof = Pos{-1, -1, -1}

func (r *reader) eof() bool {
	return r.p.Offset >= r.l
}

// peek() peeks a rune from underlying reader.
// if reader meets EOF, it will return 0. to distinguish from
// the real 0, the caller should call r.eof() again to check.
func (r *reader) peek() byte {
	if r.eof() {
		return 0
	}
	return r.s[r.p.Offset]
}

// inc increase the position offset of the reader.
// peek must be called before calling inc!
func (r *reader) inc() {
	if r.s[r.p.Offset] == '\n' {
		r.p.Line++
		r.p.Col = 0
	}
	r.p.Offset++
	r.p.Col++
}

func (r *reader) incN(n int) {
	for i := 0; i < n; i++ {
		r.inc()
	}
}

func (r *reader) readByte() (ch byte) {
	ch = r.peek()
	if r.eof() {
		return
	}
	r.inc()
	return
}

func (r *reader) pos() Pos {
	return r.p
}

func (r *reader) updatePos(pos Pos) {
	r.p = pos
}

func (r *reader) data(from *Pos) string {
	return r.s[from.Offset:r.p.Offset]
}

func (r *reader) incAsLongAs(fn func(b byte) bool) byte {
	for {
		ch := r.peek()
		if !fn(ch) {
			return ch
		}
		if r.eof() {
			return 0
		}
		r.inc()
	}
}

// skipRune skip mb character, return true indicate something has been skipped.
func (r *reader) skipRune(enc charset.Encoding) bool {
	if r.s[r.p.Offset] <= unicode.MaxASCII {
		return false
	}
	c := enc.MbLen(r.s[r.p.Offset:])
	r.incN(c)
	return c > 0
}
