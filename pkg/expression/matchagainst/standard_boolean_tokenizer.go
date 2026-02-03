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

import (
	"fmt"

	"github.com/pingcap/errors"
)

type standardBooleanTokenKind uint8

const (
	standardBooleanTokenEOF standardBooleanTokenKind = iota
	standardBooleanTokenOp
	standardBooleanTokenTerm
	standardBooleanTokenText
	standardBooleanTokenNum
)

type standardBooleanToken struct {
	kind standardBooleanTokenKind
	pos  int
	op   byte
	raw  string
}

// tokenizeStandardBooleanMode tokenizes the boolean-mode query string for the STANDARD parser.
//
// It follows the InnoDB boolean lexer behavior:
//   - Operators: * ( ) + - < > ~ @
//   - NUM: consecutive ASCII digits
//   - TEXT: double-quoted string on the same line ("..."), returned with quotes included
//   - TERM: a run of bytes up to the next delimiter (whitespace, quote, operator, or '%')
//
// '%' is a hard delimiter: it is not part of TERM, and a standalone '%' causes an error.
func tokenizeStandardBooleanMode(input string) ([]standardBooleanToken, error) {
	in := []byte(input)
	var tokens []standardBooleanToken

	for i := 0; i < len(in); {
		ch := in[i]

		if isStandardBooleanWhitespace(ch) {
			i++
			continue
		}

		if isStandardBooleanOp(ch) {
			tokens = append(tokens, standardBooleanToken{
				kind: standardBooleanTokenOp,
				pos:  i,
				op:   ch,
			})
			i++
			continue
		}

		if ch >= '0' && ch <= '9' {
			j := i + 1
			for j < len(in) && in[j] >= '0' && in[j] <= '9' {
				j++
			}
			tokens = append(tokens, standardBooleanToken{
				kind: standardBooleanTokenNum,
				pos:  i,
				raw:  string(in[i:j]),
			})
			i = j
			continue
		}

		if ch == '"' {
			j := i + 1
			for j < len(in) && in[j] != '"' && in[j] != '\n' {
				j++
			}
			if j >= len(in) || in[j] != '"' {
				return nil, errors.Errorf("unterminated quote at pos %d", i)
			}
			tokens = append(tokens, standardBooleanToken{
				kind: standardBooleanTokenText,
				pos:  i,
				raw:  string(in[i : j+1]),
			})
			i = j + 1
			continue
		}

		j := i
		for j < len(in) {
			c := in[j]
			if isStandardBooleanWhitespace(c) || c == '"' || c == '%' || isStandardBooleanOp(c) {
				break
			}
			j++
		}
		if j == i {
			return nil, errors.Errorf("unexpected char %s at pos %d", standardBooleanQuoteByte(ch), i)
		}
		tokens = append(tokens, standardBooleanToken{
			kind: standardBooleanTokenTerm,
			pos:  i,
			raw:  string(in[i:j]),
		})
		i = j
	}

	tokens = append(tokens, standardBooleanToken{kind: standardBooleanTokenEOF, pos: len(in)})
	return tokens, nil
}

func isStandardBooleanWhitespace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

func isStandardBooleanOp(ch byte) bool {
	switch ch {
	case '*', '(', ')', '+', '-', '<', '>', '~', '@':
		return true
	default:
		return false
	}
}

func standardBooleanQuoteByte(b byte) string {
	if b >= 0x20 && b <= 0x7E {
		return fmt.Sprintf("%q", b)
	}
	return fmt.Sprintf("0x%02X", b)
}
