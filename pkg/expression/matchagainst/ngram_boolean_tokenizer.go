// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package matchagainst

import "unicode"

type ngramTokType uint8

const (
	ngramTokWord ngramTokType = iota
	ngramTokQuoteOpen
	ngramTokQuoteClose
	ngramTokUnsupportedOp
	ngramTokEOF
)

type ngramToken struct {
	typ ngramTokType

	// WORD only.
	text  string
	trunc bool

	// modifier (applies to this token/phrase)
	//
	// yesno corresponds to '+' / '-' prefix operators:
	//   +1: '+' (must include)
	//   -1: '-' (must not include)
	//    0: default
	// In MySQL DEFAULT_FTB_SYNTAX, inside quotes the default behaves like '+'
	// (required), so defaultYesno() returns 1 when inQuote is true.
	yesno int8

	// Unsupported operator only.
	op rune
}

type ngramScanState struct {
	runes []rune
	i     int

	// prevChar aligns with MYSQL_FTPARSER_BOOLEAN_INFO::prev. Prefix operators
	// are only recognized when prevChar == ' ' (space) and not in a quote.
	prevChar rune
	// inQuote indicates we are inside a quoted phrase (started by '"').
	inQuote bool
}

func newNgramScanState(input string) *ngramScanState {
	return &ngramScanState{
		runes:    []rune(input),
		prevChar: ' ',
	}
}

// isNgramWordChar must agree with the analyzer's word characters (see
// fulltext.isTokenChar), which a word of the search string is split by again.
func isNgramWordChar(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsNumber(ch) || unicode.IsMark(ch) || ch == '_'
}

func isNgramUnsupportedOp(ch rune) bool {
	switch ch {
	case '(', ')':
		return true
	default:
		return false
	}
}

func (s *ngramScanState) defaultYesno() int8 {
	if s.inQuote {
		return 1
	}
	return 0
}

func (s *ngramScanState) nextToken() ngramToken {
	yesno := s.defaultYesno()

	for s.i < len(s.runes) {
		ch := s.runes[s.i]
		if isNgramWordChar(ch) {
			break
		}

		// In quote: only '"' closes the phrase.
		if s.inQuote && ch == '"' {
			s.i++
			return ngramToken{
				typ:   ngramTokQuoteClose,
				yesno: yesno,
			}
		}

		if !s.inQuote {
			if isNgramUnsupportedOp(ch) {
				s.i++
				return ngramToken{
					typ: ngramTokUnsupportedOp,
					op:  ch,
				}
			}

			// '"' begins a phrase.
			if ch == '"' {
				s.i++
				s.inQuote = true
				return ngramToken{
					typ:   ngramTokQuoteOpen,
					yesno: yesno,
				}
			}

			// Prefix operators are only active when prevChar == ' ' and not in quote.
			if s.prevChar == ' ' {
				switch ch {
				case '+':
					yesno = +1
					s.i++
					continue
				case '-':
					yesno = -1
					s.i++
					continue
				}
			}
		}

		// Other delimiter: update prevChar, reset modifiers.
		s.prevChar = ch
		yesno = s.defaultYesno()
		s.i++
	}

	// EOF.
	if s.i >= len(s.runes) {
		// An unclosed quote is closed by the end of the input.
		if s.inQuote {
			return ngramToken{
				typ:   ngramTokQuoteClose,
				yesno: 1,
			}
		}
		return ngramToken{typ: ngramTokEOF}
	}

	// WORD.
	start := s.i
	for s.i < len(s.runes) && isNgramWordChar(s.runes[s.i]) {
		s.i++
	}
	word := string(s.runes[start:s.i])
	s.prevChar = 'A' // ensure prev is true_word_char

	trunc := false
	if s.i < len(s.runes) && s.runes[s.i] == '*' {
		trunc = true
		s.i++
	}

	return ngramToken{
		typ:   ngramTokWord,
		text:  word,
		trunc: trunc,
		yesno: yesno,
	}
}
