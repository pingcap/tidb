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
// See the License for the specific language governing permissions and
// limitations under the License.

// Package hparser implements a hand-written recursive descent SQL parser
// that produces MySQL-compatible ASTs.
//
// It wraps the existing Scanner/lexer and produces ast.StmtNode values
// compatible with the existing TiDB query pipeline.
package hparser

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

// Token represents a lexed token with its metadata.
// It mirrors parser.Token but is a separate type to avoid an import cycle
// (parser imports hparser, so hparser cannot import parser).
type Token struct {
	// Tp is the token type (matches token constants in tokens.go).
	Tp int
	// Offset is the byte offset in the original SQL string.
	Offset int
	// Lit is the literal string value (identifier name, string content, etc.).
	Lit string
	// Item holds converted values (int64, float64, etc.) for numeric literals.
	Item any
}

// IsIdent returns true if the token is an identifier or a string literal (which can be used as a name).
func (t Token) IsIdent() bool {
	return t.Tp == 57346 || t.Tp == 57347 // tokIdentifier or tokStringLit
}

// IsKeyword returns true if the token's literal matches the given keyword (case-insensitive).
// This works regardless of token type — keywords with dedicated token IDs will still match.
func (t Token) IsKeyword(kw string) bool {
	return strings.EqualFold(t.Lit, kw)
}

// EOF is the token type returned at end of input.
const EOF = 0

// Precedence levels for the Pratt expression parser.
// Higher number = tighter binding.
//
// These follow MySQL's grammar hierarchy:
//
//	bool_pri → comp_op predicate   (precComparison for =, >=, etc.)
//	predicate → LIKE/IN/BETWEEN/REGEXP/IS   (precPredicate)
//	bit_expr → |, &, <<, >>, +, -, *, /, etc.
const (
	precNone       = 0
	precOr         = 1  // OR, ||
	precXor        = 2  // XOR
	precAnd        = 3  // AND, &&
	precNot        = 4  // NOT (prefix only)
	precComparison = 5  // =, <=>, >=, >, <=, <, !=, <>
	precPredicate  = 6  // LIKE, IN, BETWEEN, REGEXP (predicate level in MySQL grammar; IS is at precComparison)
	precBitOr      = 7  // |
	precBitAnd     = 8  // &
	precShift      = 9  // <<, >>
	precAddSub     = 10 // +, -
	precMulDiv     = 11 // *, /, DIV, MOD, %
	precBitXor     = 12 // ^
	precUnary      = 13 // - (unary), ~ (bit inversion), ! (not)
	precCollate    = 14 // COLLATE
)

// tokenPrecedence returns the infix precedence for the given token.
// Returns precNone if the token is not a valid infix operator.
func tokenPrecedence(tok int, sqlMode mysql.SQLMode) int {
	switch tok {
	case tokOr, tokPipesAsOr:
		return precOr
	case tokXor:
		return precXor
	case tokAnd, tokAndand:
		return precAnd
	case '=', tokEq, tokNulleq:
		return precComparison
	case tokGe, '>', tokLe, '<', tokNeq, tokNeqSynonym:
		return precComparison
	case '|':
		return precBitOr
	case '&':
		return precBitAnd
	case tokLsh, tokRsh:
		return precShift
	case '+', '-':
		return precAddSub
	case '*', '/', '%', tokDiv, tokMod:
		return precMulDiv
	case '^':
		return precBitXor
	}
	return precNone
}

// tokenToOp converts a token type to an opcode.Op for binary expressions.
func tokenToOp(tok int) opcode.Op {
	switch tok {
	case '+':
		return opcode.Plus
	case '-':
		return opcode.Minus
	case '*':
		return opcode.Mul
	case '/':
		return opcode.Div
	case '%', tokMod:
		return opcode.Mod
	case tokDiv:
		return opcode.IntDiv
	case '|':
		return opcode.Or
	case '&':
		return opcode.And
	case '^':
		return opcode.Xor
	case tokLsh:
		return opcode.LeftShift
	case tokRsh:
		return opcode.RightShift
	case '=', tokEq:
		return opcode.EQ
	case tokNulleq:
		return opcode.NullEQ
	case tokGe:
		return opcode.GE
	case '>':
		return opcode.GT
	case tokLe:
		return opcode.LE
	case '<':
		return opcode.LT
	case tokNeq, tokNeqSynonym:
		return opcode.NE
	case tokOr, tokPipesAsOr:
		return opcode.LogicOr
	case tokAnd, tokAndand:
		return opcode.LogicAnd
	case tokXor:
		return opcode.LogicXor
	}
	return 0
}
