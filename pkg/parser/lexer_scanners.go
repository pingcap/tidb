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
	"strings"
	"unicode"

	tidbfeature "github.com/pingcap/tidb/pkg/parser/tidb"
)
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
