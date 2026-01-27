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

type tokType uint8

const (
	tokWord tokType = iota
	tokLParen
	tokRParen
	tokEOF
)

type token struct {
	typ tokType

	// WORD only.
	text  string
	trunc bool

	// modifiers (apply to this token/group)
	//
	// NOTE: These three fields are the inputs to InnoDB's unary op selection
	// priority (see pickUnaryOp):
	//   yesno (+ / -) > weightAdjust (> / <) > negateToggle (~).
	//
	// yesno corresponds to '+' / '-' prefix operators:
	//   +1: '+' (must include)
	//   -1: '-' (must not include)
	//    0: default
	// In MySQL DEFAULT_FTB_SYNTAX, inside quotes the default behaves like '+'
	// (required), so defaultYesno() returns 1 when inQuote is true.
	yesno int8
	// weightAdjust counts how many '>' and '<' were seen; only its sign matters
	// (positive => OpIncrRating, negative => OpDecrRating).
	weightAdjust int8
	// negateToggle is toggled by '~'. If it's true (odd number of '~') and no
	// higher-priority modifier exists, it becomes OpNegate.
	negateToggle bool

	fromQuote bool

	// '@' handling: mark the first WORD after '@'. If the word is all digits,
	// it is treated as a distance term (to be ignored by later stages).
	fromAt     bool
	isDistance bool
}

type scanState struct {
	runes []rune
	i     int

	// prevChar aligns with MYSQL_FTPARSER_BOOLEAN_INFO::prev. Prefix operators
	// are only recognized when prevChar == ' ' (space) and not in a quote.
	prevChar rune
	// inQuote indicates we are inside a quoted phrase (started by '"').
	inQuote bool
	// afterAt marks the delimiter '@' to tag the first following word as a
	// potential distance term (e.g. @"phrase"@2).
	afterAt bool
}

func newScanState(input string) *scanState {
	return &scanState{
		runes:    []rune(input),
		prevChar: ' ',
	}
}

func isWordChar(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_'
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func (s *scanState) defaultYesno() int8 {
	if s.inQuote {
		return 1
	}
	return 0
}

func (s *scanState) nextToken() token {
	yesno := s.defaultYesno()
	var weightAdjust int8
	var negateToggle bool

	for s.i < len(s.runes) {
		ch := s.runes[s.i]
		if isWordChar(ch) {
			break
		}

		// In quote: only '"' yields a synthetic ')' (quote).
		if s.inQuote && ch == '"' {
			s.i++
			return token{
				typ:          tokRParen,
				yesno:        yesno,
				weightAdjust: weightAdjust,
				negateToggle: negateToggle,
				fromQuote:    true,
			}
		}

		if !s.inQuote {
			// '(' / ')' directly produce paren token (do not change prevChar).
			if ch == '(' {
				s.i++
				return token{
					typ:          tokLParen,
					yesno:        yesno,
					weightAdjust: weightAdjust,
					negateToggle: negateToggle,
				}
			}
			if ch == ')' {
				s.i++
				return token{
					typ:          tokRParen,
					yesno:        yesno,
					weightAdjust: weightAdjust,
					negateToggle: negateToggle,
				}
			}

			// '"' begins a quote group: produce synthetic '(' (quote) and set inQuote.
			if ch == '"' {
				s.i++
				s.inQuote = true
				return token{
					typ:          tokLParen,
					yesno:        yesno,
					weightAdjust: weightAdjust,
					negateToggle: negateToggle,
					fromQuote:    true,
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
				case '>':
					weightAdjust++
					s.i++
					continue
				case '<':
					weightAdjust--
					s.i++
					continue
				case '~':
					negateToggle = !negateToggle
					s.i++
					continue
				}
			}
		}

		// Other delimiter: update prevChar, reset modifiers.
		if ch == '@' {
			s.afterAt = true
		}
		s.prevChar = ch
		yesno = s.defaultYesno()
		weightAdjust = 0
		negateToggle = false
		s.i++
	}

	// EOF.
	if s.i >= len(s.runes) {
		// Unclosed quote: auto-close with a synthetic ')' (quote).
		if s.inQuote {
			return token{
				typ:       tokRParen,
				yesno:     1,
				fromQuote: true,
			}
		}
		return token{typ: tokEOF}
	}

	// WORD.
	start := s.i
	for s.i < len(s.runes) && isWordChar(s.runes[s.i]) {
		s.i++
	}
	word := string(s.runes[start:s.i])
	s.prevChar = 'A' // ensure prev is true_word_char

	trunc := false
	if s.i < len(s.runes) && s.runes[s.i] == '*' {
		trunc = true
		s.i++
	}

	fromAt := s.afterAt
	s.afterAt = false
	isDistance := fromAt && isAllDigits(word)

	return token{
		typ:          tokWord,
		text:         word,
		trunc:        trunc,
		yesno:        yesno,
		weightAdjust: weightAdjust,
		negateToggle: negateToggle,
		fromAt:       fromAt,
		isDistance:   isDistance,
	}
}
