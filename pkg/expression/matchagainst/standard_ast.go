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
	"strings"
)

// StandardBooleanGroup is the parsed form of `AGAINST (... IN BOOLEAN MODE)` for the STANDARD parser.
//
// This structure is used for both:
//   - the root boolean query (with Parenthesized=false)
//   - a parenthesized sub-expression "( ... )" (with Parenthesized=true)
//
// It is a direct representation of the boolean query syntax (not an execution plan).
//
// Notes:
//   - Term secondary tokenization (splitting one TERM into multiple tokens) is intentionally NOT performed here.
//   - Clauses are grouped by their boolean role (must/must-not/should) to match the evaluation model.
type StandardBooleanGroup struct {
	// Must holds the clauses prefixed by '+'.
	Must []StandardBooleanClause
	// Should holds the clauses without '+'/'-'.
	// Other boolean-mode modifiers may be represented in the clause's Modifier (even if not currently accepted).
	Should []StandardBooleanClause
	// MustNot holds the clauses prefixed by '-'.
	MustNot []StandardBooleanClause

	// Parenthesized indicates this group comes from an explicit "( ... )" in the query.
	// It only affects DebugString formatting, and is not part of the boolean semantics.
	Parenthesized bool
}

// DebugString returns a deterministic, human-readable representation of the group.
// It is intended for debugging and unit tests.
func (g StandardBooleanGroup) DebugString() string {
	if g.IsEmpty() {
		return ""
	}
	inner := g.debugStringNoParen()
	if g.Parenthesized {
		return "(" + inner + ")"
	}
	return inner
}

func (g StandardBooleanGroup) debugStringNoParen() string {
	var b strings.Builder
	writeSection := func(label string, clauses []StandardBooleanClause, keepModifier bool) {
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
func (g StandardBooleanGroup) IsEmpty() bool {
	return len(g.Must) == 0 && len(g.Should) == 0 && len(g.MustNot) == 0
}

func (g *StandardBooleanGroup) addClause(c StandardBooleanClause) {
	switch c.Modifier {
	case StandardBooleanModifierMust:
		g.Must = append(g.Must, c)
	case StandardBooleanModifierMustNot:
		g.MustNot = append(g.MustNot, c)
	default:
		g.Should = append(g.Should, c)
	}
}

// StandardBooleanClause is a single boolean-mode clause with an optional prefix modifier.
type StandardBooleanClause struct {
	// Modifier is the prefix operator (if any) for this clause.
	Modifier StandardBooleanModifier
	// Expr is the clause payload.
	Expr StandardBooleanExpr
}

// DebugString returns a human-readable representation of the clause.
func (c StandardBooleanClause) DebugString() string {
	prefix := c.Modifier.prefixChar()
	if prefix == 0 {
		return c.Expr.DebugString()
	}
	var b strings.Builder
	b.WriteByte(prefix)
	b.WriteString(c.Expr.DebugString())
	return b.String()
}

// StandardBooleanModifier is the boolean-mode prefix operator applied to a clause.
type StandardBooleanModifier uint8

// StandardBooleanModifier values correspond to boolean-mode prefix operators.
const (
	// StandardBooleanModifierNone indicates no prefix modifier.
	StandardBooleanModifierNone StandardBooleanModifier = iota
	// StandardBooleanModifierMust is '+'.
	StandardBooleanModifierMust
	// StandardBooleanModifierMustNot is '-'.
	StandardBooleanModifierMustNot
	// StandardBooleanModifierNegate is '~' (affects scoring but stays in SHOULD group).
	StandardBooleanModifierNegate
	// StandardBooleanModifierBoost is '>' (affects scoring but stays in SHOULD group).
	StandardBooleanModifierBoost
	// StandardBooleanModifierDeBoost is '<' (affects scoring but stays in SHOULD group).
	StandardBooleanModifierDeBoost
)

func (m StandardBooleanModifier) prefixChar() byte {
	switch m {
	case StandardBooleanModifierMust:
		return '+'
	case StandardBooleanModifierMustNot:
		return '-'
	case StandardBooleanModifierNegate:
		return '~'
	case StandardBooleanModifierBoost:
		return '>'
	case StandardBooleanModifierDeBoost:
		return '<'
	default:
		return 0
	}
}

// StandardBooleanExpr is the payload of a boolean-mode clause.
type StandardBooleanExpr interface {
	DebugString() string
	standardBooleanExpr()
}

// StandardBooleanTerm is a TERM/NUM token in boolean mode.
type StandardBooleanTerm struct {
	// Text is the raw TERM/NUM token text (no secondary tokenization).
	Text string
	// Wildcard means a trailing '*' is present, representing a prefix match.
	Wildcard bool
}

func (*StandardBooleanTerm) standardBooleanExpr() {}

// DebugString returns a human-readable representation of the term.
func (e *StandardBooleanTerm) DebugString() string {
	if e == nil {
		return ""
	}
	if !e.Wildcard {
		return e.Text
	}
	return e.Text + "*"
}

// StandardBooleanPhrase is a double-quoted phrase token in boolean mode.
type StandardBooleanPhrase struct {
	// Text is the content between the quotes (without the surrounding quotes).
	Text string
	// Distance is from the optional "@N" suffix. It is nil when not specified.
	// Note: the current STANDARD boolean-mode parser rejects '@', so this is reserved for future use.
	Distance *int
}

func (*StandardBooleanPhrase) standardBooleanExpr() {}

// DebugString returns a human-readable representation of the phrase.
func (e *StandardBooleanPhrase) DebugString() string {
	if e == nil {
		return ""
	}
	var b strings.Builder
	b.WriteByte('"')
	b.WriteString(e.Text)
	b.WriteByte('"')
	if e.Distance != nil {
		b.WriteByte('@')
		b.WriteString(strconv.Itoa(*e.Distance))
	}
	return b.String()
}

func (*StandardBooleanGroup) standardBooleanExpr() {}
