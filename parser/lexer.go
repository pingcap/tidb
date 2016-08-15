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
	"strings"
	"unicode"
	"unicode/utf8"
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

	errs         []error
	stmtStartPos int
}

// Errors returns the errors during a scan.
func (s *Scanner) Errors() []error {
	return s.errs
}

// reset resets the sql string to be scanned.
func (s *Scanner) reset(sql string) {
	s.r = reader{s: sql}
	s.buf.Reset()
	s.errs = s.errs[:0]
	s.stmtStartPos = 0
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
func (s *Scanner) Errorf(format string, a ...interface{}) {
	str := fmt.Sprintf(format, a...)
	val := s.r.s[s.r.pos().Offset:]
	err := fmt.Errorf("line %d column %d near \"%s\"%s", s.r.p.Line, s.r.p.Col, val, str)
	s.errs = append(s.errs, err)
}

// Lex returns a token and store the token value in v.
// Scanner satisfies yyLexer interface.
func (s *Scanner) Lex(v *yySymType) int {
	tok, pos, lit := s.scan()
	v.offset = pos.Offset
	v.ident = lit
	if tok == identifier {
		tok = handleIdent(v)
	}
	if tok == identifier {
		if tok1 := isTokenIdentifier(lit, &s.buf); tok1 != 0 {
			tok = tok1
		}
	}

	switch tok {
	case intLit:
		return toInt(s, v, lit)
	case floatLit:
		return toFloat(s, v, lit)
	case hexLit:
		return toHex(s, v, lit)
	case bitLit:
		return toBit(s, v, lit)
	case userVar, sysVar, database, currentUser, replace, cast, sysDate, currentTs, currentTime, currentDate, curDate, utcDate, extract, repeat, secondMicrosecond, minuteMicrosecond, minuteSecond, hourMicrosecond, hourMinute, hourSecond, dayMicrosecond, dayMinute, daySecond, dayHour, yearMonth, ifKwd, left, convert:
		v.item = lit
		return tok
	case null:
		v.item = nil
	case quotedIdentifier:
		tok = identifier
	}
	if tok == unicode.ReplacementChar && s.r.eof() {
		return 0
	}
	return tok
}

// NewScanner returns a new scanner object.
func NewScanner(s string) *Scanner {
	return &Scanner{r: reader{s: s}}
}

func (s *Scanner) skipWhitespace() rune {
	return s.r.incAsLongAs(unicode.IsSpace)
}

func (s *Scanner) scan() (tok int, pos Pos, lit string) {
	ch0 := s.r.peek()
	if isWhitespace(ch0) {
		ch0 = s.skipWhitespace()
	}
	pos = s.r.pos()

	if ch0 != unicode.ReplacementChar && isIdentExtend(ch0) {
		return scanIdentifier(s)
	}

	// search a trie to get a token.
	node := &ruleTable
	for ch0 >= 0 && ch0 <= 255 {
		if node.childs[ch0] == nil || s.r.eof() {
			break
		}
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
			tok = unicode.ReplacementChar
		}
		return
	}
	s.r.incAsLongAs(isIdentChar)
	tok, lit = identifier, s.r.data(&pos)
	return
}

func startWithb(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	if s.r.peek() == '\'' {
		s.r.inc()
		s.scanBit()
		if s.r.peek() == '\'' {
			s.r.inc()
			tok, lit = bitLit, s.r.data(&pos)
		} else {
			tok = unicode.ReplacementChar
		}
		return
	}
	s.r.incAsLongAs(isIdentChar)
	tok, lit = identifier, s.r.data(&pos)
	return
}

func startWithSharp(s *Scanner) (tok int, pos Pos, lit string) {
	s.r.incAsLongAs(func(ch rune) bool {
		return ch != '\n'
	})
	return s.scan()
}

func startWithDash(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	if !strings.HasPrefix(s.r.s[pos.Offset:], "-- ") {
		tok = int('-')
		s.r.inc()
		return
	}

	s.r.incN(3)
	s.r.incAsLongAs(func(ch rune) bool {
		return ch != '\n'
	})
	return s.scan()
}

func startWithSlash(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	ch0 := s.r.peek()
	if ch0 == '*' {
		s.r.inc()
		for {
			ch0 = s.r.readByte()
			if ch0 == unicode.ReplacementChar && s.r.eof() {
				tok = unicode.ReplacementChar
				return
			}
			if ch0 == '*' && s.r.readByte() == '/' {
				break
			}
		}
		return s.scan()
	}
	tok = int('/')
	return
}

func startWithAt(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	ch1 := s.r.peek()
	if isIdentFirstChar(ch1) {
		s.r.incAsLongAs(isIdentChar)
		tok, lit = userVar, s.r.data(&pos)
	} else if ch1 == '@' {
		s.r.inc()
		stream := s.r.s[pos.Offset+2:]
		for _, v := range []string{"global.", "session.", "local."} {
			if strings.HasPrefix(stream, v) {
				s.r.incN(len(v))
				break
			}
		}
		s.r.incAsLongAs(isIdentChar)
		tok, lit = sysVar, s.r.data(&pos)
	} else {
		tok = at
	}
	return
}

func scanIdentifier(s *Scanner) (int, Pos, string) {
	pos := s.r.pos()
	s.r.inc()
	s.r.incAsLongAs(isIdentChar)
	return identifier, pos, s.r.data(&pos)
}

var (
	quotedIdentifier = -identifier
)

func scanQuotedIdent(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	s.buf.Reset()
	for {
		ch := s.r.readByte()
		if ch == unicode.ReplacementChar && s.r.eof() {
			tok = unicode.ReplacementChar
			return
		}
		if ch == '`' {
			if s.r.peek() != '`' {
				// don't return identifier in case that it's interpreted as keyword token later.
				tok, lit = quotedIdentifier, s.buf.String()
				return
			}
			s.r.inc()
		}
		s.buf.WriteRune(ch)
	}
}

func startString(s *Scanner) (tok int, pos Pos, lit string) {
	tok, pos, lit = s.scanString()

	// Quoted strings placed next to each other are concatenated to a single string.
	// See http://dev.mysql.com/doc/refman/5.7/en/string-literals.html
	ch := s.skipWhitespace()
	for ch == '\'' || ch == '"' {
		_, _, lit1 := s.scanString()
		lit = lit + lit1
		ch = s.skipWhitespace()
	}
	return
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
	mb := lazyBuf{false, &s.r, &s.buf, &pos}
	ending := s.r.readByte()
	ch0 := s.r.peek()
	for !s.r.eof() {
		if ch0 == ending {
			s.r.inc()
			if s.r.peek() != ending {
				lit = mb.data()
				return
			}
			str := mb.r.data(&pos)
			mb.setUseBuf(str[1 : len(str)-1])
		} else if ch0 == '\\' {
			mb.setUseBuf(mb.r.data(&pos)[1:])
			ch0 = handleEscape(s)
		}
		mb.writeRune(ch0, s.r.w)
		s.r.inc()
		ch0 = s.r.peek()
	}

	tok = unicode.ReplacementChar
	return
}

// handleEscape handles the case in scanString when previous char is '\'.
func handleEscape(s *Scanner) rune {
	s.r.inc()
	ch0 := s.r.peek()
	/*
		\" \' \\ \n \0 \b \Z \r \t ==> escape to one char
		\% \_ ==> preserve both char
		other ==> remove \
	*/
	switch ch0 {
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
		s.buf.WriteByte('\\')
	}
	return ch0
}

func startWithNumber(s *Scanner) (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	ch0 := s.r.readByte()
	switch ch0 {
	case '0':
		tok = intLit
		ch1 := s.r.peek()
		switch {
		case ch1 >= '0' && ch1 <= '7':
			s.r.inc()
			s.scanOct()
		case ch1 == 'x' || ch1 == 'X':
			s.r.inc()
			s.scanHex()
			tok = hexLit
		case ch1 == 'b':
			s.r.inc()
			s.scanBit()
			tok = bitLit
		case ch1 == '.':
			return s.scanFloat(&pos)
		case ch1 == 'B':
			tok = unicode.ReplacementChar
			return
		}
		lit = s.r.data(&pos)
		return
	case '.':
		if isDigit(s.r.peek()) {
			return s.scanFloat(&pos)
		}
		tok, lit = int('.'), "."
		return
	}

	s.scanDigits()
	ch0 = s.r.peek()
	if ch0 == '.' || ch0 == 'e' || ch0 == 'E' {
		return s.scanFloat(&pos)
	}
	tok, lit = intLit, s.r.data(&pos)
	return
}

func (s *Scanner) scanOct() {
	s.r.incAsLongAs(func(ch rune) bool {
		return ch >= '0' && ch <= '7'
	})
}

func (s *Scanner) scanHex() {
	s.r.incAsLongAs(func(ch rune) bool {
		return ch >= '0' && ch <= '9' ||
			ch >= 'a' && ch <= 'f' ||
			ch >= 'A' && ch <= 'F'
	})
}

func (s *Scanner) scanBit() {
	s.r.incAsLongAs(func(ch rune) bool {
		return ch == '0' || ch == '1'
	})
}

func (s *Scanner) scanFloat(beg *Pos) (tok int, pos Pos, lit string) {
	s.r.p = *beg
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
		s.scanDigits()
	}
	tok, pos, lit = floatLit, *beg, s.r.data(beg)
	return
}

func (s *Scanner) scanDigits() string {
	pos := s.r.pos()
	s.r.incAsLongAs(isDigit)
	return s.r.data(&pos)
}

type reader struct {
	s string
	p Pos
	w int
}

var eof = Pos{-1, -1, -1}

func (r *reader) eof() bool {
	return r.p.Offset >= len(r.s)
}

func (r *reader) peek() rune {
	if r.eof() {
		return unicode.ReplacementChar
	}
	v, w := rune(r.s[r.p.Offset]), 1
	switch {
	case v == 0:
		return unicode.ReplacementChar
	case v >= 0x80:
		v, w = utf8.DecodeRuneInString(r.s[r.p.Offset:])
		if v == utf8.RuneError && w == 1 {
			v = rune(r.s[r.p.Offset]) // illegal UTF-8 encoding
		}
	}
	r.w = w
	return v
}

// inc increase the position offset of the reader.
// peek must be called before calling inc!
func (r *reader) inc() {
	if r.s[r.p.Offset] == '\n' {
		r.p.Line++
		r.p.Col = 0
	}
	r.p.Offset += r.w
	r.p.Col++
}

func (r *reader) incN(n int) {
	for i := 0; i < n; i++ {
		r.inc()
	}
}

func (r *reader) readByte() (ch rune) {
	ch = r.peek()
	if ch == unicode.ReplacementChar && r.eof() {
		return
	}
	r.inc()
	return
}

func (r *reader) pos() Pos {
	return r.p
}

func (r *reader) data(from *Pos) string {
	return r.s[from.Offset:r.p.Offset]
}

func (r *reader) incAsLongAs(fn func(rune) bool) rune {
	for {
		ch := r.peek()
		if !fn(ch) {
			return ch
		}
		if ch == unicode.ReplacementChar && r.eof() {
			return 0
		}
		r.inc()
	}
}
