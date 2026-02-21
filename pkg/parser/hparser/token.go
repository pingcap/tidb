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
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

// Token is an alias for parser.Token. All token values flow through this type
// from the Scanner into the hand-written parser's ring buffer (LexerBridge).
type Token = parser.Token

// EOF is the token type returned at end of input.
const EOF = 0

// Precedence levels for the Pratt expression parser.
// Higher number = tighter binding.
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
