// Copyright (c) 2014 The lexer Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lexer

import (
	"go/token"
	"io"

	"github.com/qiuyesuifeng/golex/Godeps/_workspace/src/github.com/cznic/fileutil"
)

// EOFReader implements a RuneReader allways returning 0 (EOF)
type EOFReader int

func (r EOFReader) ReadRune() (arune rune, size int, err error) {
	return 0, 0, io.EOF
}

type source struct {
	reader   io.RuneReader
	position token.Position
}

func newSource(fname string, r io.RuneReader) (s source) {
	s.reader = r
	s.position.Filename = fname
	s.position.Line = 1
	s.position.Column = 1
	return
}

// Source provides a stack of arune streams with position information.
type Source struct {
	stack []source
	tos   source
}

// NewSource returns a new Source from a RuneReader having fname.
// The RuneReader can be nil. Then an EOFReader is supplied and
// the real RuneReader(s) can be Included anytime afterwards.
func NewSource(fname string, r io.RuneReader) *Source {
	s := &Source{}
	if r == nil {
		r = EOFReader(0)
	}
	s.tos = newSource(fname, r)
	return s
}

// Include includes a RuneReader having fname. Recursive including is not checked.
func (s *Source) Include(fname string, r io.RuneReader) {
	s.stack = append(s.stack, s.tos)
	s.tos = newSource(fname, r)
}

// Position return the position of the next Read.
func (s *Source) Position() token.Position {
	return s.tos.position
}

// Read returns the next Source ScannerRune.
func (s *Source) Read() (r ScannerRune) {
	for {
		r.Position = s.Position()
		r.Rune, r.Size, r.Err = s.tos.reader.ReadRune()
		if r.Err == nil || !fileutil.IsEOF(r.Err) {
			p := &s.tos.position
			p.Offset += r.Size
			if r.Rune != '\n' {
				p.Column++
			} else {
				p.Line++
				p.Column = 1
			}
			return
		}

		// err == os.EOF, try parent source
		if sp := len(s.stack) - 1; sp >= 0 {
			s.tos = s.stack[sp]
			s.stack = s.stack[:sp]
		} else {
			r.Rune, r.Size = 0, 0
			return
		}
	}
}

// ScannerRune is a struct holding info about a arune and its origin
type ScannerRune struct {
	Position token.Position // Starting position of Rune
	Rune     rune           // Rune value
	Size     int            // Rune size
	Err      error          // os.EOF or nil. Any other value invalidates all other fields of a ScannerRune.
}

// ScannerSource is a Source with one ScannerRune look behind and an on demand one ScannerRune lookahead.
type ScannerSource struct {
	source  *Source
	prev    ScannerRune
	current ScannerRune
	next    ScannerRune
	arunes  []rune
}

// Accept checks if arune matches Current. If true then does Move.
func (s *ScannerSource) Accept(arune rune) bool {
	if arune == s.Current() {
		s.Move()
		return true
	}

	return false
}

// NewScannerSource returns a new ScannerSource from a RuneReader having fname.
// The RuneReader can be nil. Then an EOFReader is supplied and
// the real RuneReader(s) can be Included anytime afterwards.
func NewScannerSource(fname string, r io.RuneReader) *ScannerSource {
	s := &ScannerSource{}
	s.source = NewSource(fname, r)
	s.Move()
	return s
}

// Collect returns all arunes seen by the ScannerSource since last Collect or CollectString.
// Either Collect or CollectString can be called but only one of them as both clears the collector.
func (s *ScannerSource) Collect() (arunes []rune) {
	arunes, s.arunes = s.arunes, nil
	return
}

// CollectString returns all arunes seen by the ScannerSource since last CollectString or Collect as a string.
// Either Collect or CollectString can be called but only one of them as both clears the collector.
func (s *ScannerSource) CollectString() string {
	return string(s.Collect())
}

// CurrentRune returns the current ScannerSource arune. At EOF it's zero.
func (s *ScannerSource) Current() rune {
	return s.current.Rune
}

// Current returns the current ScannerSource ScannerRune.
func (s *ScannerSource) CurrentRune() ScannerRune {
	return s.current
}

// Include includes a RuneReader having fname. Recursive including is not checked.
// Include discards the one arune lookahead data if there are any.
// Lookahead data exists iff Next() has been called and Move() has not yet been called afterwards.
func (s *ScannerSource) Include(fname string, r io.RuneReader) {
	s.invalidateNext()
	s.arunes = nil
	s.source.Include(fname, r)
	s.Move()
}

func (s *ScannerSource) invalidateNext() {
	s.next.Position.Line = 0
}

func (s *ScannerSource) lookahead() {
	if !s.next.Position.IsValid() {
		s.read(&s.next)
	}
}

// Move moves ScannerSource one arune ahead.
func (s *ScannerSource) Move() {
	if arune := s.Current(); arune != 0 { // collect
		s.arunes = append(s.arunes, arune)
	}
	s.prev = s.current
	if s.next.Position.IsValid() {
		s.current = s.next
		s.invalidateNext()
		return
	}

	s.read(&s.current)
}

// Next returns ScannerSource next (lookahead) ScannerRune. It's Rune is zero if next is EOF.
func (s *ScannerSource) NextRune() ScannerRune {
	s.lookahead()
	return s.next
}

// NextRune returns ScannerSource next (lookahead) arune. It is zero if next is EOF
func (s *ScannerSource) Next() rune {
	s.lookahead()
	return s.next.Rune
}

// Position returns the current ScannerSource position, i.e. after a Move() it returns the position after CurrentRune.
func (s *ScannerSource) Position() token.Position {
	return s.source.Position()
}

// Prev returns then previous (look behind) ScanerRune. Before first Move() its Rune is zero and Position.IsValid == false
func (s *ScannerSource) PrevRune() ScannerRune {
	return s.prev
}

// PrevRune returns the previous (look behind) ScannerRune arune. Before first Move() its zero.
func (s *ScannerSource) Prev() rune {
	return s.prev.Rune
}

func (s *ScannerSource) read(dest *ScannerRune) {
	*dest = s.source.Read()
}
