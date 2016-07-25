// Copyright (c) 2014 The lexer Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lexer

import (
	"fmt"
	"go/token"
	"io"
	"unicode"
)

type Scanner struct {
	stack      []int // start state sets
	tos        int   // yy_top_state()
	lexer      *Lexer
	src        *ScannerSource
	vm         vm
	tokenStart token.Position
	token      []rune
}

func newScanner(lx *Lexer, src *ScannerSource) *Scanner {
	return &Scanner{lexer: lx, src: src, vm: newVM(lx.nfa)}
}

// Include includes a RuneReader having fname. Recursive including is not checked.
// Include discards the one arune lookahead data if there are any.
// Lookahead data exists iff Next() has been called and Move() has not yet been called afterwards.
func (s *Scanner) Include(fname string, r io.RuneReader) {
	s.src.Include(fname, r)
}

// Begin switches the Scanner's start state (start set).
func (s *Scanner) Begin(state StartSetID) {
	s.tos = int(state)
}

// Position returns the current Scanner position, i.e. after a Scan() it returns the position after the current token.
func (s *Scanner) Position() token.Position {
	return s.src.CurrentRune().Position
}

// PopState pops the top of the stack and switches to it via Begin().
func (s *Scanner) PopState() {
	sp := len(s.stack) - 1
	if sp < 0 {
		panic(fmt.Errorf("state stack underflow")) //TODO all -> os.NewError
	}
	s.Begin(StartSetID(s.stack[sp]))
	s.stack = s.stack[0:sp]
}

// PushState pushes the current start condition onto the top of the start condition stack
// and switches to newState as though you had used Begin(newState).
func (s *Scanner) PushState(newState StartSetID) {
	s.stack = append(s.stack, s.tos)
	s.Begin(newState)
}

/*
Scan scans the Scanner source, consumes arunes as long as there is a chance to recognize a token
(i.e. until the Scanner FSM stops).
	If the scanner is starting a Scan at EOF:
	    Return 0, false.

	If a valid token was recognized:
	    If the token's numeric id is >= 0:
	        Return id, true.
	    If the id is < 0:
	        If the Scan has consumed at least one arune:
	            Scan restarts discarding any consumed arunes.
	        If the Scan has not consumed any arune:
	            Scanner is stalled¹. Move on by one arune, return unicode.ReplacementChar, false.

	If a valid token was not recognized:
	    If the Scanner has not consumed any arune:
	        Return the current arune, false.² Move on by one arune.
	    If the Scanner has moved by exactly one arune:
	        Return that arune, false.²
	    If the Scanner has consumed more than one arune:
	        Return unicode.ReplacementChar, false.

The actual arunes consumed by the last Scan can be retrieved by Token.

If the assigned token ids do not overlap with the otherwise expected arunes, i.e. their ids are e.g. in the Unicode private usage area,
then it is possible, as any other unsuccessful scan will return either zero (EOF) or unicode.ReplacementChar,
to ignore the returned ok value and drive a parser only by the arune/token id value. This is presumably the easier way for e.g. goyacc.

¹The FSM has stopped in an accepting state without consuming any arunes. Caused by using (re)* or (re)? for negative numeric id (i.e. ignored) tokens.
Better avoid that.

²Intended for processing single arune tokens (e.g. a semicolon) without defining the regexp and token id for it.
Examples of such usage can be found in many .y files.
*/
func (s *Scanner) Scan() (arune rune, ok bool) {
	for {
		var moves int
		s.tokenStart = s.src.current.Position
		if arune, moves, ok = s.vm.start(s.src, s.lexer.starts[s.tos], s.lexer.accept); ok {
			if arune >= 0 { // arune == recognized token id
				break
			}

			// arune < 0, arune == recognized ignore token id
			if moves > 0 {
				s.src.Collect() // discard arunes
				continue
			}

			// moves == 0
			s.src.Move() // Scanner stall
			arune, ok = unicode.ReplacementChar, false
			break
		}

		// ok == false
		if arune == 0 { // EOF
			if moves != 0 {
				arune = unicode.ReplacementChar
			}
			break
		}

		if moves == 0 { // make progress
			s.src.Move()
			break
		}

		// moves > 0
		if moves == 1 { // made progress
			break
		}

		// moves > 1, don't confuse an easy parser by returning the last arune of an invalid token
		arune = unicode.ReplacementChar
		break

	}

	s.token = s.src.Collect()
	return
}

// Token returns the arunes consumed by last Scan. Repeated Scans for ignored tokens (id < 0) are discarded.
func (s *Scanner) Token() []rune {
	return s.token
}

// TokenStart returns the starting position of the token returned by last Scan.
func (s *Scanner) TokenStart() token.Position {
	return s.tokenStart
}

// TopState returns the top of the stack without altering the stack's contents.
func (s *Scanner) TopState() StartSetID {
	return StartSetID(s.tos)
}
