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
	"strconv"

	"github.com/pingcap/errors"
)

// ParseStandardBooleanMode parses the STANDARD parser's boolean mode query syntax.
//
// It intentionally does not do any additional term tokenization.
//
// The parsing logic follows InnoDB's boolean-mode lexer/parser model.
//
// Informal grammar:
//
//	query  := clause*
//	clause := [prefix] term | [prefix] phrase | ['*'] term
//	prefix := '+' | '-'
//	term   := (TERM | NUM) ['*']
//	phrase := TEXT
//
// Notes:
//   - Leading '*' before a term is accepted but ignored.
//   - Trailing '*' after a term sets the term's wildcard flag.
//   - Empty phrases "\"\"" are ignored.
//   - The STANDARD parser path currently rejects operators: ()<>~@.
func ParseStandardBooleanMode(input string) (BooleanGroup, error) {
	tokens, err := tokenizeStandardBooleanMode(input)
	if err != nil {
		return BooleanGroup{}, err
	}

	p := standardBooleanParser{
		tokens: tokens,
	}
	g, err := p.parseGroup(false)
	if err != nil {
		return BooleanGroup{}, err
	}
	if t := p.peek(); t.kind != standardBooleanTokenEOF {
		return BooleanGroup{}, errors.Errorf("unexpected token %s at pos %d", p.tokenDesc(t), t.pos)
	}
	return g, nil
}

type standardBooleanParser struct {
	tokens []standardBooleanToken
	idx    int
}

// parseGroup reads zero or more clauses until EOF, or until it sees ')' (when stopAtRightParen is true).
func (p *standardBooleanParser) parseGroup(stopAtRightParen bool) (BooleanGroup, error) {
	var g BooleanGroup
	for {
		t := p.peek()
		if t.kind == standardBooleanTokenEOF {
			break
		}
		if t.kind == standardBooleanTokenOp && t.op == ')' {
			if stopAtRightParen {
				break
			}
			return BooleanGroup{}, errors.Errorf("unexpected ')' at pos %d", t.pos)
		}

		clause, err := p.parseClause()
		if err != nil {
			return BooleanGroup{}, err
		}
		if clause == nil {
			continue
		}
		g.addClause(*clause)
	}

	return g, nil
}

// parseClause parses one boolean "clause". A clause can be:
//   - a term token (TERM/NUM) (optionally prefixed by '+' or '-', optionally with trailing '*')
//   - a phrase token ("...") (optionally prefixed by '+' or '-')
//
// Operators (), <, >, ~ and @ are currently rejected in the STANDARD boolean-mode path.
func (p *standardBooleanParser) parseClause() (*BooleanClause, error) {
	mod := BooleanModifierNone
	if p.peekIsPrefixOp() {
		mod = p.consumePrefixOp()
	}

	if p.peekIsOp('*') {
		p.consume()
		term, err := p.parseTermExpr()
		if err != nil {
			return nil, err
		}
		p.applyTrailingWildcardIfPresent(term)
		return &BooleanClause{Modifier: mod, Expr: term}, nil
	}

	switch p.peek().kind {
	case standardBooleanTokenTerm, standardBooleanTokenNum:
		term, err := p.parseTermExpr()
		if err != nil {
			return nil, err
		}
		p.applyTrailingWildcardIfPresent(term)
		return &BooleanClause{Modifier: mod, Expr: term}, nil
	case standardBooleanTokenText:
		phrase, err := p.parsePhraseExpr()
		if err != nil {
			return nil, err
		}
		if phrase == nil {
			return nil, nil
		}
		return &BooleanClause{Modifier: mod, Expr: phrase}, nil
	default:
		t := p.peek()
		return nil, errors.Errorf("unexpected token %s at pos %d", p.tokenDesc(t), t.pos)
	}
}

// parseGroupClause parses a parenthesized boolean sub-expression "(...)". It is currently unused because
// '(' / ')' are rejected in the STANDARD boolean-mode path, but kept to preserve the overall parser structure.
// func (p *standardBooleanParser) parseGroupClause(mod BooleanModifier) (*BooleanClause, error) {
// 	if err := p.expectOp('('); err != nil {
// 		return nil, err
// 	}
// 	inner, err := p.parseGroup(true)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if err := p.expectOp(')'); err != nil {
// 		return nil, err
// 	}
// 	if inner.IsEmpty() {
// 		return nil, nil
// 	}
// 	inner.Parenthesized = true
// 	return &BooleanClause{
// 		Modifier: mod,
// 		Expr:     &inner,
// 	}, nil
// }

func (p *standardBooleanParser) parseTermExpr() (*BooleanTerm, error) {
	t := p.peek()
	if t.kind != standardBooleanTokenTerm && t.kind != standardBooleanTokenNum {
		return nil, errors.Errorf("expected term at pos %d, got %s", t.pos, p.tokenDesc(t))
	}
	p.consume()
	return &BooleanTerm{
		text: t.raw,
	}, nil
}

// parsePhraseExpr parses a double-quoted phrase token.
//
// It returns (nil, nil) for an empty phrase "\"\"" because InnoDB treats it as invalid/ignored.
func (p *standardBooleanParser) parsePhraseExpr() (*BooleanPhrase, error) {
	t := p.peek()
	if t.kind != standardBooleanTokenText {
		return nil, errors.Errorf("expected quoted text at pos %d, got %s", t.pos, p.tokenDesc(t))
	}
	p.consume()

	raw := t.raw
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return nil, errors.Errorf("invalid quoted text at pos %d", t.pos)
	}
	inner := raw[1 : len(raw)-1]
	if len(inner) == 0 {
		return nil, nil
	}
	return &BooleanPhrase{text: inner}, nil
}

// applyTrailingWildcardIfPresent consumes a trailing '*' after a term and marks the term as wildcard.
func (p *standardBooleanParser) applyTrailingWildcardIfPresent(term *BooleanTerm) {
	if p.peekIsOp('*') {
		p.consume()
		term.Wildcard = true
	}
}

func (p *standardBooleanParser) peek() standardBooleanToken {
	if p.idx >= len(p.tokens) {
		return standardBooleanToken{kind: standardBooleanTokenEOF}
	}
	return p.tokens[p.idx]
}

func (p *standardBooleanParser) peekIsOp(op byte) bool {
	t := p.peek()
	return t.kind == standardBooleanTokenOp && t.op == op
}

func (p *standardBooleanParser) consume() standardBooleanToken {
	t := p.peek()
	if p.idx < len(p.tokens) {
		p.idx++
	}
	return t
}

func (p *standardBooleanParser) peekIsPrefixOp() bool {
	t := p.peek()
	if t.kind != standardBooleanTokenOp {
		return false
	}
	switch t.op {
	case '+', '-':
		return true
	default:
		return false
	}
}

func (p *standardBooleanParser) consumePrefixOp() BooleanModifier {
	t := p.consume()
	switch t.op {
	case '+':
		return BooleanModifierMust
	case '-':
		return BooleanModifierMustNot
	default:
		return BooleanModifierNone
	}
}

func (p *standardBooleanParser) tokenDesc(t standardBooleanToken) string {
	switch t.kind {
	case standardBooleanTokenEOF:
		return "EOF"
	case standardBooleanTokenOp:
		return "'" + string([]byte{t.op}) + "'"
	case standardBooleanTokenTerm:
		return "TERM(" + strconv.Quote(t.raw) + ")"
	case standardBooleanTokenText:
		return "TEXT(" + strconv.Quote(t.raw) + ")"
	case standardBooleanTokenNum:
		return "NUM(" + strconv.Quote(t.raw) + ")"
	default:
		return "UNKNOWN"
	}
}
