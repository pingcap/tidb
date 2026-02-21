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
package parser

import (
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
)

// Token is an alias for parser.Token. All token values flow through this type
// from the Scanner into the hand-written parser's ring buffer (LexerBridge).

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
	case 57509, 57835:
		return precOr
	case 57592:
		return precXor
	case 57367, 57358:
		return precAnd
	case '=', 58202, 58210:
		return precComparison
	case 58203, '>', 58204, '<', 58208, 58209:
		return precComparison
	case '|':
		return precBitOr
	case '&':
		return precBitAnd
	case 58207, 58212:
		return precShift
	case '+', '-':
		return precAddSub
	case '*', '/', '%', 57413, 57496:
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
	case '%', 57496:
		return opcode.Mod
	case 57413:
		return opcode.IntDiv
	case '|':
		return opcode.Or
	case '&':
		return opcode.And
	case '^':
		return opcode.Xor
	case 58207:
		return opcode.LeftShift
	case 58212:
		return opcode.RightShift
	case '=', 58202:
		return opcode.EQ
	case 58210:
		return opcode.NullEQ
	case 58203:
		return opcode.GE
	case '>':
		return opcode.GT
	case 58204:
		return opcode.LE
	case '<':
		return opcode.LT
	case 58208, 58209:
		return opcode.NE
	case 57509, 57835:
		return opcode.LogicOr
	case 57367, 57358:
		return opcode.LogicAnd
	case 57592:
		return opcode.LogicXor
	}
	return 0
}
