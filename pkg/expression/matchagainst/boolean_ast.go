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

import "strings"

// BooleanGroup is the parsed form of `AGAINST (... IN BOOLEAN MODE)`.
//
// It is a direct representation of the boolean query syntax (not an execution plan).
//
// Notes:
//   - Term secondary tokenization (splitting one TERM into multiple tokens) is intentionally NOT performed here.
//   - Clauses are grouped by their boolean role (must/must-not/should) to match the evaluation model.
type BooleanGroup struct {
	// Must holds the clauses prefixed by '+'.
	Must []BooleanClause
	// Should holds the clauses without '+'/'-'. The relevance modifiers
	// '~', '>' and '<' are kept in the clause's Modifier.
	Should []BooleanClause
	// MustNot holds the clauses prefixed by '-'.
	MustNot []BooleanClause
}

// DebugString returns a deterministic, human-readable representation of the group.
// It is intended for debugging and unit tests.
func (g BooleanGroup) DebugString() string {
	if g.IsEmpty() {
		return ""
	}
	var b strings.Builder
	writeSection := func(label string, clauses []BooleanClause, keepModifier bool) {
		b.WriteString(label)
		b.WriteByte('[')
		for i, c := range clauses {
			if i > 0 {
				b.WriteString(", ")
			}
			if keepModifier {
				b.WriteString(c.DebugString())
			} else {
				b.WriteString(c.Expr.DebugString())
			}
		}
		b.WriteByte(']')
	}

	// Print in the evaluation order: MUST, SHOULD, MUST_NOT.
	writeSection("M", g.Must, false)
	b.WriteByte(' ')
	writeSection("S", g.Should, true)
	b.WriteByte(' ')
	writeSection("N", g.MustNot, false)
	return b.String()
}

// IsEmpty returns whether the group contains no clauses.
func (g BooleanGroup) IsEmpty() bool {
	return len(g.Must) == 0 && len(g.Should) == 0 && len(g.MustNot) == 0
}

func (g *BooleanGroup) addClause(c BooleanClause) {
	switch c.Modifier {
	case BooleanModifierMust:
		g.Must = append(g.Must, c)
	case BooleanModifierMustNot:
		g.MustNot = append(g.MustNot, c)
	default:
		g.Should = append(g.Should, c)
	}
}

// BooleanClause is a single boolean-mode clause with an optional prefix modifier.
type BooleanClause struct {
	// Modifier is the prefix operator (if any) for this clause.
	Modifier BooleanModifier
	// Expr is the clause payload.
	Expr BooleanExpr
}

// DebugString returns a human-readable representation of the clause.
func (c BooleanClause) DebugString() string {
	prefix := c.Modifier.prefixChar()
	if prefix == 0 {
		return c.Expr.DebugString()
	}
	var b strings.Builder
	b.WriteByte(prefix)
	b.WriteString(c.Expr.DebugString())
	return b.String()
}

// BooleanModifier is the boolean-mode prefix operator applied to a clause.
type BooleanModifier uint8

// BooleanModifier values correspond to boolean-mode prefix operators.
const (
	// BooleanModifierNone indicates no prefix modifier.
	BooleanModifierNone BooleanModifier = iota
	// BooleanModifierMust is '+'.
	BooleanModifierMust
	// BooleanModifierMustNot is '-'.
	BooleanModifierMustNot
	// BooleanModifierNegate is '~' (affects scoring but stays in SHOULD group).
	BooleanModifierNegate
	// BooleanModifierBoost is '>' (affects scoring but stays in SHOULD group).
	BooleanModifierBoost
	// BooleanModifierDeBoost is '<' (affects scoring but stays in SHOULD group).
	BooleanModifierDeBoost
)

func (m BooleanModifier) prefixChar() byte {
	switch m {
	case BooleanModifierMust:
		return '+'
	case BooleanModifierMustNot:
		return '-'
	case BooleanModifierNegate:
		return '~'
	case BooleanModifierBoost:
		return '>'
	case BooleanModifierDeBoost:
		return '<'
	default:
		return 0
	}
}

// BooleanExpr is the payload of a boolean-mode clause.
type BooleanExpr interface {
	DebugString() string
	booleanExpr()
	Text() string
}

// BooleanTerm is a TERM token in boolean mode.
type BooleanTerm struct {
	// text is the raw TERM token text (no secondary tokenization).
	text string
	// Wildcard means a trailing '*' is present, representing a prefix match.
	Wildcard bool
}

func (*BooleanTerm) booleanExpr() {}

// DebugString returns a human-readable representation of the term.
func (e *BooleanTerm) DebugString() string {
	if e == nil {
		return ""
	}
	if e.Wildcard {
		return e.text + "*"
	}
	return e.text
}

// Text returns the raw term text without the wildcard suffix, for use in execution.
func (e *BooleanTerm) Text() string {
	if e == nil {
		return ""
	}
	return e.text
}

// BooleanPhrase is a double-quoted phrase token in boolean mode.
type BooleanPhrase struct {
	// text is the content between the quotes (without the surrounding quotes).
	text string
}

func (*BooleanPhrase) booleanExpr() {}

// DebugString returns a human-readable representation of the phrase.
func (e *BooleanPhrase) DebugString() string {
	if e == nil {
		return ""
	}
	return `"` + e.text + `"`
}

// Text returns the raw phrase text without the quotes, for use in execution.
func (e *BooleanPhrase) Text() string {
	if e == nil {
		return ""
	}
	return e.text
}
