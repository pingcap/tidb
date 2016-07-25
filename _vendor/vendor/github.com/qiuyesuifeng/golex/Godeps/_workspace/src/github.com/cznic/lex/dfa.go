// Copyright (c) 2014 The lex Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lex

import (
	"fmt"
	"github.com/qiuyesuifeng/golex/Godeps/_workspace/src/github.com/cznic/lexer"
	"sort"
	"unicode"
)

var (
	partialDFAs = []*dfa{nil}
	allNfa      nfa
)

type nfa struct {
	nfa     lexer.Nfa
	in, out *lexer.NfaState
}

func (n *nfa) String() string {
	var i, o string
	if n.in != nil {
		i = fmt.Sprintf("%d", n.in.Index)
	}
	if n.out != nil {
		o = fmt.Sprintf("%d", n.out.Index)
	}
	return fmt.Sprintf("in %s out %s\n%s", i, o, n.nfa.String())
}

type dfa struct {
	nfa
	accept     []*lexer.NfaState
	acceptRule []int
}

func (d *dfa) String() (s string) {
	s = "accept states list:"
	for i, a := range d.accept {
		s += fmt.Sprintf(" %d(%d)", a.Index, d.acceptRule[i])
	}
	return s + "\n" + d.nfa.String()
}

func (n *nfa) reverse() *nfa { // in place
	cEdges, ncEdges := make([][]lexer.Edger, len(n.nfa)), make([][]lexer.Edger, len(n.nfa))
	for _, state := range n.nfa {
		for _, edge := range state.Consuming {
			target := edge.SetTarget(state)
			tid := target.Index
			cEdges[tid] = append(cEdges[tid], edge)
		}
		for _, edge := range state.NonConsuming {
			target := edge.SetTarget(state)
			tid := target.Index
			ncEdges[tid] = append(ncEdges[tid], edge)
		}
		state.Consuming, state.NonConsuming = nil, nil
	}
	for id, state := range n.nfa {
		state.Consuming, state.NonConsuming = cEdges[id], ncEdges[id]
	}
	n.in, n.out = n.out, n.in
	return n
}

func (n *nfa) powerSet() (d *dfa) {
	d = &dfa{}
	m := map[string]*lexer.NfaState{}

	s := func(c map[int]*stateSet, i int) (p *stateSet) {
		if !bits32 && !(i >= 0 && i <= 255) {
			panic(fmt.Errorf("unsupported value %d > 255", i))
		}

		p, ok := c[i]
		if !ok {
			p = newStateSet(len(n.nfa))
			c[i] = p
		}
		return
	}

	var f func(*stateSet) *lexer.NfaState

	f = func(closure *stateSet) (state *lexer.NfaState) {
		var prio int
		id := closure.id()
		state, ok := m[id]
		if !ok {
			state = d.nfa.nfa.NewState()
			m[id] = state
			if closure.has(n.out, &prio) {
				d.accept = append(d.accept, state)
				d.acceptRule = append(d.acceptRule, prio)
			}
			closures := map[int]*stateSet{}
			for i := uint(0); i < closure.count; i++ {
				nfaState := n.nfa[closure.dense[i].id]
				prio = closure.dense[i].priority
				for _, edge := range nfaState.Consuming {
					p := edge.Priority()
					if p == 0 {
						p = prio
					}
					switch x := edge.(type) {
					default:
						panic(fmt.Errorf("unexpeceted type %T", edge))
					case *lexer.RuneEdge:
						s(closures, int(x.Rune)).closure(x.Target(), p)
					case *lexer.RangesEdge:
						if !bits32 {
							var m [256]bool
							for _, r := range x.Ranges.R16 {
								for c := r.Lo; c <= r.Hi; c += r.Stride {
									m[c] = true
								}
							}
							for _, r := range x.Ranges.R32 {
								for c := r.Lo; c <= r.Hi; c += r.Stride {
									m[c] = true
								}
							}
							for i, v := range m {
								if v != x.Invert {
									s(closures, i).closure(x.Target(), p)
								}
							}
						} else { // bits32
							panic("unreachable")
							/* mode32 disabled
							r := rangeSlice(x.Ranges)
							if x.Invert {
								r.invert()
							}
							cnt := 0
							for _, v := range r {
								for c := v.Lo; c <= v.Hi; c += v.Stride {
									cnt++
									if cnt > 256 {
										panic(fmt.Errorf("not yet implemented feature"))
									}
									s(closures, c).closure(x.Target(), p)
								}
							}
							*/
						}
					}
				}
			}
			mm := map[*lexer.NfaState][]int{}

			// Stabilize
			a := []int{}
			for char := range closures {
				a = append(a, char)
			}
			sort.Ints(a)
			for _, char := range a {
				closure = closures[char]
				state := f(closure)
				mm[state] = append(mm[state], char)
			}

			// Stabilize
			ai, as := []int{}, map[int]*lexer.NfaState{}
			for s := range mm {
				i := int(s.Index)
				ai = append(ai, i)
				as[i] = s
			}
			sort.Ints(ai)
			for _, si := range ai {
				s := as[si]
				slice := mm[s]
				switch len(slice) {
				case 1:
					state.AddConsuming(lexer.NewRuneEdge(s, rune(slice[0])))
				default:
					r := []unicode.Range32{}
					for _, char := range slice {
						r = append(r, unicode.Range32{uint32(char), uint32(char), 1})
					}
					(*rangeSlice)(&r).normalize()
					state.AddConsuming(lexer.NewRangesEdge(s, false, &unicode.RangeTable{R32: r}))
				}
			}
		}
		return
	}

	d.nfa.in = f(newStateSet(len(n.nfa)).closure(n.in, 0))
	return
}

func (d *dfa) toNfa() *nfa {
	s := d.nfa.nfa.NewState()
	d.nfa.out = s
	for _, st := range d.accept {
		st.AddNonConsuming(&lexer.EpsilonEdge{0, s})
	}
	return &d.nfa
}

func computePartialDFAs() {
	var err error
	for irule, rule := range rules {
		if irule == 0 {
			continue
		}

		nfa := nfa{}
		nfa.in, nfa.out, err = nfa.nfa.ParseRE("", rule.re)
		if err != nil {
			logErr(fmt.Sprintf("%s - %s", rulePos[irule], err.Error()))
			return // <- this was missing!
		}

		if nodfaopt {
			partialDFAs = append(partialDFAs, nfa.powerSet())
		} else {
			partialDFAs = append(partialDFAs, nfa.reverse().powerSet().toNfa().reverse().powerSet())
		}
	}
}

func computeAllNfa() {
	in, out := allNfa.nfa.NewState(), allNfa.nfa.NewState()
	seenBol := false
	for irule, dfa := range partialDFAs {
		if irule == 0 {
			continue
		}

		if rules[irule].bol {
			seenBol = true
			continue
		}

		ruleIn := allNfa.nfa.NewState()
		rules[irule].in = ruleIn
		// an irule priority e-edge from ruleIn to the partial dfa.in
		ruleIn.AddNonConsuming(&lexer.EpsilonEdge{irule, dfa.in})
		for _, state := range dfa.nfa.nfa {
			allNfa.nfa.AddState(state)
		}
		for _, state := range dfa.accept {
			state.AddNonConsuming(&lexer.EpsilonEdge{0, out})
		}
		conds := rules[irule].conds
		if len(conds) == 0 { // rule is active in all non exclusive start conditions
			for _, sc := range sStarts {
				in.AddConsuming(lexer.NewRuneEdge(ruleIn, rune(iStarts[sc])))
			}
			continue
		}

		// len(conds) != 0
		for _, sc := range conds {
			if sc != "*" { // rule is active in all its explicitly declared start conditions
				in.AddConsuming(lexer.NewRuneEdge(ruleIn, rune(iStarts[sc])))
				continue
			}

			// sc == "*", rule is always active
			for sc := range defStarts {
				in.AddConsuming(lexer.NewRuneEdge(ruleIn, rune(iStarts[sc])))
			}
		}
	}

	if seenBol {
		for irule, dfa := range partialDFAs {
			if irule == 0 {
				continue
			}

			ruleIn := allNfa.nfa.NewState()
			rules[irule].bolIn = ruleIn
			// an irule priority e-edge from ruleIn to the partial dfa.in
			ruleIn.AddNonConsuming(&lexer.EpsilonEdge{irule, dfa.in})
			for _, state := range dfa.nfa.nfa {
				allNfa.nfa.AddState(state)
			}
			for _, state := range dfa.accept {
				state.AddNonConsuming(&lexer.EpsilonEdge{0, out})
			}
			conds := rules[irule].conds
			if len(conds) == 0 { // rule is active in all non exclusive start conditions
				for _, sc := range sStarts {
					in.AddConsuming(lexer.NewRuneEdge(ruleIn, rune(iStarts[sc]+128)))
				}
				continue
			}

			// len(conds) != 0
			for _, sc := range conds {
				if sc != "*" { // rule is active in all its explicitly declared start conditions
					in.AddConsuming(lexer.NewRuneEdge(ruleIn, rune(iStarts[sc]+128)))
					continue
				}

				// sc == "*", rule is always active
				for sc := range defStarts {
					in.AddConsuming(lexer.NewRuneEdge(ruleIn, rune(iStarts[sc]+128)))
				}
			}
		}
	}
	allNfa.in, allNfa.out = in, out
}
