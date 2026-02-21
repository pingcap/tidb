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

package hparser

import (
	"github.com/pingcap/tidb/pkg/parser"
)

// LexerBridge wraps a parser.Lexer (the Scanner) to provide token lookahead
// and backtracking for the recursive descent parser.
//
// It maintains a ring buffer of pre-fetched tokens so the parser can
// peek ahead without consuming tokens, and can backtrack to a saved position.
type LexerBridge struct {
	// lexer is the underlying scanner that produces tokens.
	lexer parser.Lexer

	// buf is a ring buffer of pre-read tokens.
	buf [maxLookahead]Token
	// head is the index of the next token to return in buf.
	head int
	// count is the number of valid tokens in buf (from head onward).
	count int

	// src is the original SQL string, used for error messages and text extraction.
	src string
}

const maxLookahead = 8

// NewLexerBridge creates a LexerBridge backed by the given lexer.
func NewLexerBridge(lexer parser.Lexer, src string) *LexerBridge {
	return &LexerBridge{
		lexer: lexer,
		src:   src,
	}
}

// fill ensures at least n tokens are buffered. Panics if n > maxLookahead.
func (lb *LexerBridge) fill(n int) {
	if n > maxLookahead {
		panic("LexerBridge.fill: lookahead exceeds maxLookahead")
	}
	for lb.count < n {
		idx := (lb.head + lb.count) % maxLookahead
		// Lex directly into the ring buffer slot — zero copy.
		lb.buf[idx].Tp = lb.lexer.Lex(&lb.buf[idx])
		lb.count++
	}
}

// Peek returns the next token without consuming it.
func (lb *LexerBridge) Peek() Token {
	lb.fill(1)
	return lb.buf[lb.head]
}

// PeekN returns the n-th token ahead (0-indexed: PeekN(0) == Peek()).
// n must be < maxLookahead.
func (lb *LexerBridge) PeekN(n int) Token {
	lb.fill(n + 1)
	return lb.buf[(lb.head+n)%maxLookahead]
}

// Next consumes and returns the next token.
func (lb *LexerBridge) Next() Token {
	lb.fill(1)
	tok := lb.buf[lb.head]
	lb.head = (lb.head + 1) % maxLookahead
	lb.count--
	return tok
}

// Expect consumes the next token and returns it if it matches the expected type.
// Returns false if the token doesn't match (the token is still consumed).
func (lb *LexerBridge) Expect(expected int) (Token, bool) {
	tok := lb.Next()
	return tok, tok.Tp == expected
}

// Accept consumes the next token if it matches the expected type and returns true.
// If the token doesn't match, it is NOT consumed and false is returned.
func (lb *LexerBridge) Accept(expected int) (Token, bool) {
	tok := lb.Peek()
	if tok.Tp == expected {
		lb.Next()
		return tok, true
	}
	return tok, false
}

// AcceptAny consumes the next token if it matches any of the expected types.
// Returns the token and true if matched, or zero Token and false if not.
func (lb *LexerBridge) AcceptAny(expected ...int) (Token, bool) {
	tok := lb.Peek()
	for _, e := range expected {
		if tok.Tp == e {
			lb.Next()
			return tok, true
		}
	}
	return tok, false
}

// Mark returns a position marker that can be used to restore the lexer state.
func (lb *LexerBridge) Mark() LexerMark {
	return LexerMark{head: lb.head, count: lb.count}
}

// Restore resets the lexer to a previously saved mark.
// Only valid if no more than maxLookahead tokens have been consumed since the mark.
func (lb *LexerBridge) Restore(m LexerMark) {
	// Calculate how many tokens were consumed since the mark.
	// These tokens are still in the ring buffer and become available again.
	consumed := (lb.head - m.head + maxLookahead) % maxLookahead
	lb.head = m.head
	lb.count = consumed + lb.count
}

// LexerMark is an opaque position marker for lexer backtracking.
type LexerMark struct {
	head  int
	count int
}
