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
)

var _ = yyLexer(&Scanner{})

// Pos represents the position of a token.
type Pos struct {
	Line   int
	Col    int
	Offset int
}

// Scanner implements the yyLexer interface
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
// Scanner satisfies yyReset interface.
func (s *Scanner) reset(sql string) yyLexer {
	s.r = reader{s: sql}
	s.buf.Reset()
	s.errs = s.errs[:0]
	s.stmtStartPos = 0
	return s
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
	case userVar, sysVar, database, currentUser, replace, cast, sysDate, currentTs, currentTime, currentDate, curDate, utcDate, extract, repeat, secondMicrosecond, minuteMicrosecond, minuteSecond, hourMicrosecond, hourMinute, hourSecond, dayMicrosecond, dayMinute, daySecond, dayHour, yearMonth, ifKwd, left:
		v.item = lit
		return tok
	case null:
		v.item = nil
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

func (s *Scanner) skipWhitespace() byte {
	return s.r.incAsLongAs(isWhitespace)
}

func (s *Scanner) scan() (tok int, pos Pos, lit string) {
	ch0 := s.r.peek()
	if isWhitespace(ch0) {
		ch0 = s.skipWhitespace()
	}
	pos = s.r.pos()

	if isIdentFirstChar(ch0) {
		switch ch0 {
		case 'X', 'x':
			s.r.inc()
			if s.r.readByte() == '\'' {
				s.scanHex()
				if s.r.peek() == '\'' {
					s.r.inc()
					tok, lit = hexLit, s.r.data(&pos)
				} else {
					tok = unicode.ReplacementChar
				}
				return
			}
			s.r.p = pos
		case 'b':
			s.r.inc()
			if s.r.readByte() == '\'' {
				s.scanBit()
				if s.r.peek() == '\'' {
					s.r.inc()
					tok, lit = bitLit, s.r.data(&pos)
				} else {
					tok = unicode.ReplacementChar
				}
				return
			}
			s.r.p = pos
		}
		return s.scanIdent()
	} else if isDigit(ch0) || ch0 == '.' {
		return s.scanNumber()
	} else if ch0 == '\'' || ch0 == '"' {
		return s.scanString()
	} else if ch0 == '`' {
		return s.scanQuotedIdent()
	}

	switch ch0 {
	case '@':
		return s.startWithAt()
	case '/':
		return s.startWithSlash()
	case '-':
		return s.startWithDash()
	case '#':
		s.r.incAsLongAs(func(ch byte) bool {
			return ch != '\n'
		})
		return s.scan()
	}

	// search a trie to scan a token
	ch := ch0
	node := &ruleTable
	for {
		if node.childs[ch] == nil || s.r.eof() {
			break
		}
		node = node.childs[ch]
		s.r.inc()
		ch = s.r.peek()
	}

	tok, lit = node.token, s.r.data(&pos)
	return
}

func (s *Scanner) startWithDash() (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	if !strings.HasPrefix(s.r.s[pos.Offset:], "-- ") {
		tok = int('-')
		s.r.inc()
		return
	}

	s.r.incN(3)
	s.r.incAsLongAs(func(ch byte) bool {
		return ch != '\n'
	})
	return s.scan()
}

func (s *Scanner) startWithSlash() (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	ch0 := s.r.peek()
	if ch0 == '*' {
		s.r.inc()
		for {
			ch0 = s.r.readByte()
			if ch0 == 0 && s.r.eof() {
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

func (s *Scanner) startWithAt() (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	ch1 := s.r.peek()
	if isIdentFirstChar(ch1) {
		s.scanIdent()
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
		s.scanIdent()
		tok, lit = sysVar, s.r.data(&pos)
	} else {
		tok = at
	}
	return
}

func (s *Scanner) scanIdent() (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.incAsLongAs(isIdentChar)
	return identifier, pos, s.r.data(&pos)
}

func (s *Scanner) scanQuotedIdent() (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	s.r.inc()
	s.buf.Reset()
	for {
		ch := s.r.readByte()
		if ch == 0 && s.r.eof() {
			tok = unicode.ReplacementChar
			return
		}
		if ch == '`' {
			if s.r.peek() != '`' {
				tok, lit = identifier, s.buf.String()
				return
			}
			s.r.inc()
		}
		s.buf.WriteByte(ch)
	}
}

func (s *Scanner) scanString() (tok int, pos Pos, lit string) {
	s.buf.Reset()
	tok, pos = stringLit, s.r.pos()
	ending := s.r.readByte()
	for {
		if s.r.eof() {
			tok, lit = 0, s.buf.String()
			return
		}
		ch0 := s.r.peek()
		if ch0 == ending {
			s.r.inc()
			if s.r.peek() != ending {
				break
			}
		}
		// TODO this would break reader's line and col information
		if ch0 == '\n' {
		}
		if ch0 == '\\' {
			save := s.r.pos()
			s.r.inc()
			ch1 := s.r.peek()
			if ch1 == 'n' {
				s.buf.WriteByte('\n')
				s.r.inc()
				continue
			} else if ch1 == '\\' {
			} else if ch1 == '"' {
				s.buf.WriteByte('"')
				s.r.inc()
				continue
			} else if ch1 == '\'' {
				s.buf.WriteByte('\'')
				s.r.inc()
				continue
			} else {
				s.r.p = save
			}
		} else if !isASCII(ch0) {
			// TODO handle non-ascii
		}
		s.buf.WriteByte(ch0)
		s.r.inc()
	}

	lit = s.buf.String()
	// Quoted strings placed next to each other are concatenated to a single string.
	// See http://dev.mysql.com/doc/refman/5.5/en/string-literals.html
	ch := s.r.peek()
	if ch == '\'' {
		_, _, lit1 := s.scanString()
		lit = lit + lit1
	}
	return
}

func (s *Scanner) scanNumber() (tok int, pos Pos, lit string) {
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
}

var eof = Pos{-1, -1, -1}

func (r *reader) eof() bool {
	return r.p.Offset >= len(r.s)
}

func (r *reader) peek() byte {
	if r.eof() {
		return 0
	}
	return r.s[r.p.Offset]
}

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
	if ch == 0 && r.eof() {
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

func (r *reader) incAsLongAs(fn func(byte) bool) byte {
	for {
		ch := r.peek()
		if !fn(ch) {
			return ch
		}
		if ch == 0 && r.eof() {
			return 0
		}
		r.inc()
	}
}
