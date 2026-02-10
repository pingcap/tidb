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
	"strings"

	"github.com/pingcap/errors"
)

// Possible parsing errors.
var (
	ErrUnmatchedLeftParen  = errors.New("unmatched '(' in BOOLEAN MODE query")
	ErrUnmatchedRightParen = errors.New("unmatched ')' in BOOLEAN MODE query")
)

func pickNgramModifier(yesno int8) BooleanModifier {
	// yesno: +1 / 0 / -1 (required / default / prohibited)
	if yesno > 0 {
		return BooleanModifierMust
	}
	if yesno < 0 {
		return BooleanModifierMustNot
	}
	return BooleanModifierNone
}

type ngramPhraseBuilder struct {
	modifier BooleanModifier
	words    []string
}

// ParseNgramBooleanMode parses a MySQL MATCH ... AGAINST (... IN BOOLEAN MODE)
// query using the ngram parser behavior into a simplified, InnoDB-aligned IR.
//
// Notes:
//   - Quoted phrase is normalized into a space-joined string.
//   - Empty quoted phrases are ignored.
//   - Only '+' and '-' are accepted as unary operators.
//   - Operators '(', ')', '<', '>', '~' and '@' are currently unsupported.
func ParseNgramBooleanMode(input string) (*BooleanGroup, error) {
	s := newNgramScanState(input)

	root := &BooleanGroup{}
	curGroup := root
	var groupStack []*BooleanGroup

	var curPhrase *ngramPhraseBuilder

	for {
		tok := s.nextToken()
		switch tok.typ {
		case ngramTokEOF:
			if curPhrase != nil {
				return nil, errors.New("internal error: EOF in quote")
			}
			if len(groupStack) != 0 {
				return nil, ErrUnmatchedLeftParen
			}
			return root, nil
		case ngramTokWord:
			if curPhrase != nil {
				w := tok.text
				if tok.trunc {
					w += "*"
				}
				curPhrase.words = append(curPhrase.words, w)
				continue
			}
			op := pickNgramModifier(tok.yesno)
			word := &BooleanTerm{
				text:     tok.text,
				Wildcard: tok.trunc,
			}
			curGroup.addClause(BooleanClause{Modifier: op, Expr: word})
		case ngramTokLParen:
			op := pickNgramModifier(tok.yesno)
			if tok.fromQuote {
				groupStack = append(groupStack, curGroup)
				curPhrase = &ngramPhraseBuilder{modifier: op}
				continue
			}

			child := &BooleanGroup{Parenthesized: true}
			curGroup.addClause(BooleanClause{Modifier: op, Expr: child})

			groupStack = append(groupStack, curGroup)
			curGroup = child
		case ngramTokRParen:
			if tok.fromQuote {
				if curPhrase == nil {
					return nil, errors.New("internal error: unexpected quote close")
				}
				phraseText := strings.Join(curPhrase.words, " ")
				if phraseText != "" {
					curGroup.addClause(BooleanClause{
						Modifier: curPhrase.modifier,
						Expr:     &BooleanPhrase{text: phraseText},
					})
				}
				curPhrase = nil
				s.inQuote = false
			} else if curPhrase != nil {
				return nil, errors.New("internal error: ')' in quote")
			}

			if len(groupStack) == 0 {
				return nil, ErrUnmatchedRightParen
			}
			curGroup = groupStack[len(groupStack)-1]
			groupStack = groupStack[:len(groupStack)-1]
		case ngramTokUnsupportedOp:
			return nil, errors.Errorf("unsupported operator %q in BOOLEAN MODE query", tok.op)
		default:
			return nil, errors.New("internal error: unknown token")
		}
	}
}
