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

func pickNgramModifier(yesno int8, weightAdjust int8, negateToggle bool) BooleanModifier {
	// InnoDB only keeps one unary operator for each element. If multiple modifier
	// symbols are present, the effective operator is chosen by this priority:
	//
	//   yesno (+ / -) > weightAdjust (> / <) > negateToggle (~)
	//
	// where:
	//   yesno: +1 / 0 / -1 (required / default / prohibited)
	//   weightAdjust: sign-only, >0 => Boost, <0 => DeBoost
	//   negateToggle: true if '~' appears odd times
	if yesno > 0 {
		return BooleanModifierMust
	}
	if yesno < 0 {
		return BooleanModifierMustNot
	}
	if weightAdjust > 0 {
		return BooleanModifierBoost
	}
	if weightAdjust < 0 {
		return BooleanModifierDeBoost
	}
	if negateToggle {
		return BooleanModifierNegate
	}
	return BooleanModifierNone
}

type ngramPhraseBuilder struct {
	phrase *BooleanPhrase
	words  []string
}

// ParseNgramBooleanMode parses a MySQL MATCH ... AGAINST (... IN BOOLEAN MODE)
// query using the ngram parser behavior into a simplified, InnoDB-aligned IR.
//
// Notes:
//   - Quoted phrase is normalized into a space-joined string.
//   - "@N" produces a term with Ignored=true (N must be digits). Later stages
//     should ignore it.
//   - Unary operators follow InnoDB priority: + / - > < ~ (only one is kept).
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
				if tok.isDistance {
					continue
				}
				w := tok.text
				if tok.trunc {
					w += "*"
				}
				curPhrase.words = append(curPhrase.words, w)
				continue
			}
			op := pickNgramModifier(tok.yesno, tok.weightAdjust, tok.negateToggle)
			word := &BooleanTerm{
				text:     tok.text,
				Wildcard: tok.trunc,
				Ignored:  tok.isDistance,
			}
			curGroup.addClause(BooleanClause{Modifier: op, Expr: word})
		case ngramTokLParen:
			op := pickNgramModifier(tok.yesno, tok.weightAdjust, tok.negateToggle)
			if tok.fromQuote {
				phrase := &BooleanPhrase{}
				curGroup.addClause(BooleanClause{Modifier: op, Expr: phrase})

				groupStack = append(groupStack, curGroup)
				curPhrase = &ngramPhraseBuilder{phrase: phrase}
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
				curPhrase.phrase.text = strings.Join(curPhrase.words, " ")
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
		default:
			return nil, errors.New("internal error: unknown token")
		}
	}
}
