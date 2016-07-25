// Copyright (c) 2014 The lexer Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lexer

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

func qs(s string) string {
	q := fmt.Sprintf("%q", s)
	return q[1 : len(q)-1]
}

// ParseRE compiles a regular expression re into Nfa, returns the re component starting
// and accepting states or an Error if any.
func (n *Nfa) ParseRE(name, re string) (in, out *NfaState, err error) {
	s := NewScannerSource(name, strings.NewReader(re))

	defer func() {
		if e := recover(); e != nil {
			in, out = nil, nil
			pos := s.CurrentRune().Position
			err = fmt.Errorf(`%s - "%s^%s" - %s`, pos, qs(re[:pos.Offset]), qs(re[pos.Offset:]), e.(error))
		}
	}()

	in, out = n.parseExpr(s, nil, nil, 0)
	if s.Position().Offset < len(re) {
		panic(fmt.Errorf("syntax error"))
	}

	return
}

func (n *Nfa) parseExpr(s *ScannerSource, in0, out0 *NfaState, nest int) (in, out *NfaState) {
	in, out = in0, out0
	for s.Current() != 0 {
		a, b := n.parseAlt(s, nest)
		if in == nil {
			in, out = a, b
		} else {
			in.AddNonConsuming(&EpsilonEdge{0, a})
			b.AddNonConsuming(&EpsilonEdge{0, out})
		}
		if !s.Accept('|') {
			break
		}

		if s.Current() == 0 {
			out = nil
			break
		}
	}
	if out != nil {
		return
	}

	panic(fmt.Errorf(`expected "alernative"`)) //TODO all parameterless fmt.Errof -> os.NewError
}

func (n *Nfa) parseAlt(s *ScannerSource, nest int) (in, out *NfaState) {
	var a, b *NfaState
	for s.Current() != 0 {
		if a, b = n.parseTerm(s, out, nest); a == nil {
			break
		}

		if out = b; in == nil {
			in = a
		}
	}
	if out != nil {
		return
	}

	panic(fmt.Errorf(`expected "term"`))
}

func (n *Nfa) parseTerm(s *ScannerSource, in0 *NfaState, nest int) (in, out *NfaState) {
	if in = in0; in == nil {
		in = n.NewState()
	}
	switch arune := s.Current(); arune {
	default:
		s.Move()
		out = in.AddConsuming(NewRuneEdge(n.NewState(), arune)).Target()
	case '+', '*', '?':
		panic(fmt.Errorf("unexpected metachar %q", string(arune)))
	case '\\':
		switch arune = s.mustParseChar("ApPz"); arune {
		default:
			out = in.AddConsuming(NewRuneEdge(n.NewState(), arune)).Target()
		case 'A':
			out = in.AddNonConsuming(NewAssertEdge(n.NewState(), TextStart)).Target()
		case 'z':
			out = in.AddNonConsuming(NewAssertEdge(n.NewState(), TextEnd)).Target()
		case 'p', 'P':
			name, ok := "", false
			s.expect('{')
			for !s.Accept('}') {
				name += string(s.mustGetChar())
			}
			var ranges *unicode.RangeTable
			if ranges, ok = unicode.Categories[name]; !ok {
				if ranges, ok = unicode.Scripts[name]; !ok {
					panic(fmt.Errorf("unknown Unicode category name %q", name))
				}
			}
			out = in.AddConsuming(NewRangesEdge(n.NewState(), arune == 'P', ranges)).Target()
		}
	case 0, '|':
		return nil, nil
	case ')':
		if nest == 0 {
			panic(fmt.Errorf(`unexpected ")"`))
		}
		return nil, nil
	case '(':
		s.Move()
		in, out = n.parseExpr(s, in, n.NewState(), nest+1)
		s.expect(')')
	case '.': // All but '\U+0000', '\n'
		s.Move()
		out = in.AddConsuming(NewRangesEdge(n.NewState(), true, &unicode.RangeTable{R16: []unicode.Range16{{'\n', '\n', 1}}})).Target()
	case '^':
		s.Move()
		out = in.AddNonConsuming(NewAssertEdge(n.NewState(), LineStart)).Target()
	case '$':
		s.Move()
		out = in.AddNonConsuming(NewAssertEdge(n.NewState(), LineEnd)).Target()
	case '[':
		s.Move()
		ranges := &unicode.RangeTable{}
		invert := s.Accept('^')
	loop:
		for {
			a := s.mustParseChar("-")
			switch s.Current() {
			case '\\':
				ranges.R32 = append(ranges.R32, unicode.Range32{uint32(a), uint32(a), 1})
				a := s.mustParseChar("-")
				ranges.R32 = append(ranges.R32, unicode.Range32{uint32(a), uint32(a), 1})
			default:
				if s.Accept('-') {
					// Allow `[+-]`
					if s.Current() == ']' {
						s.Move()
						ranges.R32 = append(ranges.R32, unicode.Range32{uint32(a), uint32(a), 1})
						ranges.R32 = append(ranges.R32, unicode.Range32{'-', '-', 1})
						break loop
					}

					b := s.mustParseChar("")
					if b < a {
						panic(fmt.Errorf(`missing or invalid range bounds ordering in bracket expression "%s-%s"`, string(a), string(b)))
					}
					ranges.R32 = append(ranges.R32, unicode.Range32{uint32(a), uint32(b), 1})
				} else {
					ranges.R32 = append(ranges.R32, unicode.Range32{uint32(a), uint32(a), 1})
				}
			}
			if s.Accept(']') {
				break
			}
		}
		(*rangeSlice)(&ranges.R32).normalize()
		out = in.AddConsuming(NewRangesEdge(n.NewState(), invert, ranges)).Target()
	}

	// postfix ops
	switch s.Current() {
	case '+':
		s.Move()
		_, out = n.OneOrMore(in, out)
	case '*':
		s.Move()
		_, out = n.ZeroOrMore(in, out)
	case '?':
		s.Move()
		_, out = n.ZeroOrOne(in, out)
	}

	return
}

func (s *ScannerSource) mustGetChar() rune {
	if c := s.Current(); c != 0 {
		s.Move()
		return c
	}

	panic(fmt.Errorf("unexpected end of regexp"))
}

func (s *ScannerSource) mustParseChar(more string) (arune rune) {
	allowZero := s.Current() == '\\'
	if arune = s.parseChar(more); arune == 0 && !allowZero {
		panic(fmt.Errorf("unexpected regexp end"))
	}

	s.Move()
	return
}

func (s *ScannerSource) parseChar(more string) (arune rune) {
	if arune = s.Current(); arune != '\\' {
		return
	}

	s.Move()
	switch arune = s.Current(); arune {
	default:
		if strings.IndexAny(string(arune), more) < 0 {
			panic(fmt.Errorf(`unknown escape sequence "\%s (more: %q)"`, string(arune), more))
		}

		return
	case '\\', '.', '+', '*', '?', '(', ')', '|', '[', ']', '{', '}', '^', '$':
		return
	case 'a':
		return '\a'
	case 'b':
		return '\b'
	case 'f':
		return '\f'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	case 'v':
		return '\v'
	case 'x':
		s.Move()
		arune = s.hex() << 4
		s.Move()
		return arune | s.hex()
	}
}

func (s *ScannerSource) hex() (v rune) {
	switch v = s.Current(); {
	case v >= '0' && v <= '9':
		return v - '0'
	case v >= 'a' && v <= 'f':
		return v - 'a' + 10
	case 'v' >= 'A' && v <= 'F':
		return v - 'A' + 10
	}

	panic(errors.New("expected hex digit"))
}

func (s *ScannerSource) expect(arune rune) {
	if !s.Accept(arune) {
		panic(fmt.Errorf("expected %q, got %q", string(arune), string(s.Current())))
	}
}
