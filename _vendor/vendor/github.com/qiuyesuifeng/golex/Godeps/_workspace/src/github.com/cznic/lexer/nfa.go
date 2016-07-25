// Copyright (c) 2014 The lexer Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lexer

import (
	"fmt"
	"unicode"
)

// Edger interface defines the method set for all NFA edge types.
type Edger interface {
	Accepts(s *ScannerSource) bool // Accepts() returns wheter an edge accepts the ScannerSource present state.
	Priority() int                 // Priority returns the priority tag of an edge (lower value wins).
	Target() *NfaState             // Target() returns the edge's target NFA state.
	String() string
	SetTarget(s *NfaState) *NfaState // SetTarget() assigns s as a new target and returns the original Target
}

type EdgeAssert int

const (
	TextStart EdgeAssert = iota
	TextEnd
	LineStart
	LineEnd
)

// AssertEdge is a non consuming edge which asserts line/text start/end.
type AssertEdge struct {
	EpsilonEdge
	Asserts EdgeAssert
}

// NewAssertEdge returns a new AssertdEdge pointing to target, asserting asserts.
func NewAssertEdge(target *NfaState, asserts EdgeAssert) *AssertEdge {
	return &AssertEdge{EpsilonEdge{0, target}, asserts}
}

// Accepts is the AssertEdge implementation of the Edger interface.
func (e *AssertEdge) Accepts(s *ScannerSource) bool {
	switch e.Asserts {
	default:
		panic("unexpected Asserts")
	case TextStart:
		return s.Prev() == 0
	case TextEnd:
		return s.Next() == 0
	case LineStart:
		p := s.Prev()
		return p == '\n' || p == 0
	case LineEnd:
		n := s.Next()
		return n == '\n' || n == 0
	}
}

func (e *AssertEdge) String() (s string) {
	switch e.Asserts {
	default:
		panic("unexpected Asserts")
	case TextStart:
		s = "\\A"
	case TextEnd:
		s = "\\z"
	case LineStart:
		s = "^"
	case LineEnd:
		s = "$"
	}
	return s + e.EpsilonEdge.String()
}

// EpsilonEdge is a non consuming, always accepting NFA edge.
type EpsilonEdge struct {
	Prio int
	Targ *NfaState
}

// Accepts is the EpsilonEdge implementation of the Edger interface.
func (e *EpsilonEdge) Accepts(s *ScannerSource) bool {
	return true
}

// Priority is the EpsilonEdge implementation of the Edger interface.
func (e *EpsilonEdge) Priority() int {
	return e.Prio
}

func (e *EpsilonEdge) String() (s string) {
	if e.Prio != 0 {
		s = fmt.Sprintf("(%d) ", e.Prio)
	}
	return fmt.Sprintf("%s--> %d", s, e.Target().Index)
}

func (e *EpsilonEdge) SetTarget(s *NfaState) (old *NfaState) {
	old, e.Targ = e.Targ, s
	return
}

// Target is the EpsilonEdge implementation of the Edger interface.
func (e *EpsilonEdge) Target() *NfaState {
	return e.Targ
}

// NfaState desribes a single NFA state.
type NfaState struct {
	Index        uint    // Index of this state in its owning NFA.
	Consuming    []Edger // The NFA state non consuming edge set.
	NonConsuming []Edger // The NFA state consuming edge set.
}

// AddConsuming adds an Edger to the state's consuming edge set and returns the Edger.
// No checks are made if the edge really is a consuming egde.
func (n *NfaState) AddConsuming(edge Edger) Edger {
	n.Consuming = append(n.Consuming, edge)
	return edge
}

// AddNonConsuming adds an Edger to the state's non consuming edge set and returns the Edger.
// No checks are made if the edge really is a non consuming edge.
func (n *NfaState) AddNonConsuming(edge Edger) Edger {
	n.NonConsuming = append(n.NonConsuming, edge)
	return edge
}

func (n *NfaState) String() (s string) {
	s += fmt.Sprintf("[%d]", n.Index)
	for _, edge := range n.NonConsuming {
		s += "\n\t" + edge.String()
	}
	for _, edge := range n.Consuming {
		s += "\n\t" + edge.String()
	}
	return
}

func (n *NfaState) isRedundant() (retarget *NfaState, ok bool) { //TODO func (rcvr) (**NfaState) bool ?
	if len(n.Consuming) != 0 || len(n.NonConsuming) == 0 || len(n.NonConsuming) != 1 {
		return
	}

	if _, ok = n.NonConsuming[0].(*EpsilonEdge); ok {
		retarget = n.NonConsuming[0].Target()
	}

	return
}

//TODO s/^func (n *NfaState)/func (state *NfaState)/g
//TODO s/^func (n *Nfa)/func (nfa *Nfa)/g
func (n *NfaState) retarget() (target *NfaState, ok bool) {
	if target, ok = n.isRedundant(); !ok {
		return
	}

	for t := target; ok; t, ok = target.isRedundant() {
		target = t
	}
	return target, true
}

// Nfa is a set of NfaStates.
type Nfa []*NfaState

// AddState adds and existing NfaState to Nfa. One NfaState should not appear in more than one Nfa
// because the NfaState Index property should always reflect its position in the owner Nfa.
func (n *Nfa) AddState(s *NfaState) *NfaState {
	s.Index = uint(len(*n))
	*n = append(*n, s)
	return s
}

// NewState returns a newly created NfaState and adds it to the Nfa.
func (n *Nfa) NewState() (s *NfaState) {
	return n.AddState(&NfaState{Index: uint(len(*n))})
}

// Reduce attempts to decrease the number of states in a Nfa.
func (n *Nfa) reduce() {
	nfa := *n
	remove := map[*NfaState]bool{}
	for _, state := range nfa {
		for _, edge := range state.NonConsuming {
			target := edge.Target()
			if retarget, ok := target.retarget(); ok {
				remove[edge.SetTarget(retarget)] = true
			}
		}
		for _, edge := range state.Consuming {
			target := edge.Target()
			if retarget, ok := target.retarget(); ok {
				remove[edge.SetTarget(retarget)] = true
			}
		}
	}
	if len(remove) == 0 {
		return
	}

	w := 0
	for r := 0; r < len(nfa); r++ {
		if !remove[nfa[r]] {
			state := nfa[r]
			state.Index = uint(w)
			nfa[w] = state
			w++
		}
	}
	*n = nfa[0:w]
}

func (n Nfa) String() (s string) {
	for _, st := range n {
		s += fmt.Sprintf("\n%s", st.String())
	}
	return
}

// OneOrMore converts a Nfa component C to C+
func (n *Nfa) OneOrMore(in, out *NfaState) (from, to *NfaState) {
	// >(in)-C->((out)) => >(from)-->(s)-C->(out)-->((to))
	//                                 ↖_____/
	s := n.NewState()
	s.Consuming, s.NonConsuming = in.Consuming, in.NonConsuming
	from = in
	from.Consuming, from.NonConsuming = nil, nil
	from.AddNonConsuming(&EpsilonEdge{0, s})
	out.AddNonConsuming(&EpsilonEdge{0, s}) // loop back
	return from, out.AddNonConsuming(&EpsilonEdge{0, n.NewState()}).Target()
}

// ZeroOrMore converts a Nfa component C to C*
func (n *Nfa) ZeroOrMore(in, out *NfaState) (from, to *NfaState) {
	//                                 /¯¯¯¯¯↘
	// >(in)-C->((out)) => >(from)-->(s)-C->(out)-->((to))
	//                                 ↖_____/
	s := n.NewState()
	s.Consuming, s.NonConsuming = in.Consuming, in.NonConsuming
	from = in
	from.Consuming, from.NonConsuming = nil, nil
	from.AddNonConsuming(&EpsilonEdge{0, s})
	out.AddNonConsuming(&EpsilonEdge{0, s}) // loop back
	s.AddNonConsuming(&EpsilonEdge{0, out}) // loop forward
	return from, out.AddNonConsuming(&EpsilonEdge{0, n.NewState()}).Target()
}

// ZeroOrOne converts a Nfa component C to C?
func (n *Nfa) ZeroOrOne(in, out *NfaState) (from, to *NfaState) {
	//                                 /¯¯¯¯¯↘
	// >(in)-C->((out)) => >(from)-->(s)-C->(out)-->((to))
	s := n.NewState()
	s.Consuming, s.NonConsuming = in.Consuming, in.NonConsuming
	from = in
	from.Consuming, from.NonConsuming = nil, nil
	from.AddNonConsuming(&EpsilonEdge{0, s})
	s.AddNonConsuming(&EpsilonEdge{0, out}) // loop forward
	return from, out.AddNonConsuming(&EpsilonEdge{0, n.NewState()}).Target()
}

// RuneEdge is a consuming egde which accepts a single arune.
type RuneEdge struct {
	EpsilonEdge
	Rune rune
}

// NewRuneEdge returns a new RuneEdge pointing to target which accepts arune.
func NewRuneEdge(target *NfaState, arune rune) *RuneEdge {
	return &RuneEdge{EpsilonEdge{0, target}, arune}
}

// Accepts is the RuneEdge implementation of the Edger interface.
func (e *RuneEdge) Accepts(s *ScannerSource) bool {
	return e.Rune == s.Current()
}

func (e *RuneEdge) String() string {
	return fmt.Sprintf("%q%s", string(e.Rune), e.EpsilonEdge.String())
}

// RangesEdge is a consuming egde which accepts arune ranges except \U+0000.
type RangesEdge struct {
	EpsilonEdge
	Invert bool                // Accepts all but Ranges as in [^exp]
	Ranges *unicode.RangeTable // Accepted arune set
}

// NewRangesEdge returns a new RangesEdge pointing to target which accepts ranges.
func NewRangesEdge(target *NfaState, invert bool, ranges *unicode.RangeTable) *RangesEdge {
	return &RangesEdge{EpsilonEdge{0, target}, invert, ranges}
}

// Accepts is the RangesEdge implementation of the Edger interface.
func (e *RangesEdge) Accepts(s *ScannerSource) bool {
	arune := s.Current()
	if arune != 0 {
		if e.Invert {
			return !unicodeIs(e.Ranges, arune)
		}

		return unicodeIs(e.Ranges, arune)
	}

	return false
}

func (e *RangesEdge) String() (s string) {
	if e.Invert {
		s = "!"
	}
	for _, r := range e.Ranges.R16 {
		switch {
		default:
			s += fmt.Sprintf("%q-%q(%d), ", string(r.Lo), string(r.Hi), r.Stride)
		case r.Lo == r.Hi:
			s += fmt.Sprintf("%q, ", string(r.Lo))
		case r.Stride == 1:
			s += fmt.Sprintf("%q...%q, ", string(r.Lo), string(r.Hi))
		}
	}
	for _, r := range e.Ranges.R32 {
		switch {
		default:
			s += fmt.Sprintf("%q-%q(%d), ", string(r.Lo), string(r.Hi), r.Stride)
		case r.Lo == r.Hi:
			s += fmt.Sprintf("%q, ", string(r.Lo))
		case r.Stride == 1:
			s += fmt.Sprintf("%q...%q, ", string(r.Lo), string(r.Hi))
		}
	}
	return s + e.EpsilonEdge.String()
}
