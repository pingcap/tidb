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

func pickUnaryOp(yesno int8, weightAdjust int8, negateToggle bool) UnaryOp {
	if yesno > 0 {
		return OpExist
	}
	if yesno < 0 {
		return OpIgnore
	}
	if weightAdjust > 0 {
		return OpIncrRating
	}
	if weightAdjust < 0 {
		return OpDecrRating
	}
	if negateToggle {
		return OpNegate
	}
	return OpNone
}

type phraseBuilder struct {
	phrase *Phrase
	words  []string
}

// ParseBooleanMode parses a MySQL MATCH ... AGAINST (... IN BOOLEAN MODE) query
// string into a simplified, InnoDB-aligned IR.
//
// Notes:
//   - Quoted phrase is normalized into a space-joined string.
//   - "@N" produces a Word with Ignored=true (N must be digits). Later stages
//     should ignore it.
//   - Unary operators follow InnoDB priority: + / - > < ~ (only one is kept).
func ParseBooleanMode(input string) (*Group, error) {
	s := newScanState(input)

	root := &Group{}
	curGroup := root
	var groupStack []*Group

	var curPhrase *phraseBuilder

	for {
		tok := s.nextToken()
		switch tok.typ {
		case tokEOF:
			if curPhrase != nil {
				return nil, errors.New("internal error: EOF in quote")
			}
			if len(groupStack) != 0 {
				return nil, ErrUnmatchedLeftParen
			}
			return root, nil
		case tokWord:
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
			op := pickUnaryOp(tok.yesno, tok.weightAdjust, tok.negateToggle)
			word := &Word{
				Text:    tok.text,
				Trunc:   tok.trunc,
				Ignored: tok.isDistance,
			}
			curGroup.addNode(Node{Op: op, Item: word})
		case tokLParen:
			op := pickUnaryOp(tok.yesno, tok.weightAdjust, tok.negateToggle)
			if tok.fromQuote {
				phrase := &Phrase{}
				curGroup.addNode(Node{Op: op, Item: phrase})

				groupStack = append(groupStack, curGroup)
				curPhrase = &phraseBuilder{phrase: phrase}
				continue
			}

			child := &Group{}
			curGroup.addNode(Node{Op: op, Item: child})

			groupStack = append(groupStack, curGroup)
			curGroup = child
		case tokRParen:
			if tok.fromQuote {
				if curPhrase == nil {
					return nil, errors.New("internal error: unexpected quote close")
				}
				curPhrase.phrase.Text = strings.Join(curPhrase.words, " ")
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
