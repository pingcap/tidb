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

	errs []error
}

// Errors returns the errors during a scan.
func (s *Scanner) Errors() []error {
	return s.errs
}

// reset resets the sql string to be scanned.
// Scanner satisfies yyReset interface.
func (s *Scanner) reset(sql string) yyLexerXX {
	s.r = reader{s: sql}
	return s
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
	if tok == identifier {
		if tok1 := isTokenIdentifier(lit, &s.buf); tok1 != 0 {
			return tok1
		}
	}
	if tok == unicode.ReplacementChar && s.r.eof() {
		return 0
	}
	v.item = lit
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
		return s.scanIdent()
	} else if isDigit(ch0) || ch0 == '.' {
		return s.scanNumber()
	} else if ch0 == '\'' || ch0 == '"' {
		return s.scanString()
	}

	for _, v := range tokenPrefixTable {
		if strings.HasPrefix(s.r.s[pos.Offset:], v.prefix) {
			s.r.incN(len(v.prefix))
			tok, lit = v.token, v.prefix
			return
		}
	}

	if ch0 == '@' {
		return s.startWithAt()
	} else if ch0 == '/' {
		return s.startWithSlash()
	} else if ch0 == '-' {
		return s.startWithDash()
	} else if ch0 == '#' {
		s.r.incAsLongAs(func(ch byte) bool {
			return ch != '\n'
		})
		return s.scan()
	}

	if ch0 == 0 && s.r.eof() {
		return 0, eof, ""
	}
	if tok = isTokenByte(ch0); tok != 0 {
		s.r.inc()
		return
	}

	tok = unicode.ReplacementChar
	s.r.inc()
	return
}

func (s *Scanner) startWithDash() (tok int, pos Pos, lit string) {
	pos = s.r.pos()
	if !strings.HasPrefix(s.r.s[pos.Offset:], "-- ") {
		tok = int('-')
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

func (s *Scanner) scanString() (tok int, pos Pos, lit string) {
	s.buf.Reset()
	tok, pos = stringLit, s.r.pos()
	ending := s.r.readByte()
	for {
		ch0 := s.r.peek()
		if ch0 == ending {
			s.r.inc()
			break
		}
		// TODO this would break reader's line col information
		if ch0 == '\n' {
			tok = unicode.ReplacementChar
			return
		}
		if ch0 == '\\' {
			s.r.inc()
			ch1 := s.r.peek()
			if ch1 == 'n' {
				s.buf.WriteByte('\n')
			} else if ch1 == '\\' {
				s.buf.WriteByte('\\')
			} else if ch1 == '"' {
				s.buf.WriteByte('"')
			} else if ch1 == '\'' {
				s.buf.WriteByte('\'')
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
		case ch1 == 'b' || ch1 == 'B':
			s.r.inc()
			s.scanBit()
			tok = bitLit
		case ch1 == '.':
			return s.scanFloat(&pos)
		}
		lit = s.r.data(&pos)
		return
	case '.':
		return s.scanFloat(&pos)
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
		r.inc()
	}
}
