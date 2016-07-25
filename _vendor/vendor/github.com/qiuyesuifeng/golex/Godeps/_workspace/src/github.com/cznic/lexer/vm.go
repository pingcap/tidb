// Copyright (c) 2014 The lexer Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lexer

type stateSet struct {
	count  uint
	dense  []dense
	sparse []uint
}

type dense struct {
	id       uint
	priority int
}

func newStateSet(n int) stateSet {
	return stateSet{0, make([]dense, n), make([]uint, n)}
}

func (s *stateSet) clear() {
	s.count = 0
}

func (s *stateSet) has(state *NfaState, priority *int) bool {
	id := state.Index
	i := s.sparse[id]
	d := s.dense[i]
	if i < s.count && d.id == id {
		*priority = d.priority
		return true
	}

	return false
}

func (s *stateSet) include(state *NfaState, priority int) bool {
	id := state.Index
	i := s.sparse[id]
	d := &s.dense[i]
	if i < s.count && d.id == id { // has
		if d.priority > priority {
			d.priority = priority
			return false
		}

		return true
	}

	s.sparse[id], s.dense[s.count] = s.count, dense{id, priority}
	s.count++
	return false
}

func (s *stateSet) closure(src *ScannerSource, state *NfaState, priority int) {
	if !s.include(state, priority) {
		for _, edge := range state.NonConsuming {
			if edge.Accepts(src) {
				p := edge.Priority()
				if p == 0 { // inherit
					p = priority
				}
				s.closure(src, edge.Target(), p)
			}
		}
	}
}

type vm struct {
	nfa  Nfa
	x, y stateSet
}

func newVM(nfa Nfa) vm {
	n := len(nfa)
	return vm{nfa, newStateSet(n), newStateSet(n)}
}

func (vm *vm) start(src *ScannerSource, start, accept *NfaState) (arune rune, moves int, ok bool) {
	nfa, x, y := vm.nfa, &vm.x, &vm.y
	x.clear()
	for x.closure(src, start, 0); ; x, y = y, x {
		arune = src.Current()
		y.clear()
		for i := uint(0); i < x.count; i++ {
			d := x.dense[i]
			for _, edge := range nfa[d.id].Consuming {
				if edge.Accepts(src) {
					priority := edge.Priority()
					if priority == 0 { // inherit
						priority = d.priority
					}
					y.closure(src, edge.Target(), priority)
				}
			}
		}
		if y.count == 0 { // FSM halted
			i := int(arune)
			ok = x.has(accept, &i)
			arune = rune(i)
			return
		}

		src.Move()
		moves++
	}
}
