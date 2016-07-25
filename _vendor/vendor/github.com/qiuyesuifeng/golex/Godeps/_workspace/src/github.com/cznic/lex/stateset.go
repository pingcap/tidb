// Copyright (c) 2014 The lex Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lex

import (
	"fmt"
	"github.com/qiuyesuifeng/golex/Godeps/_workspace/src/github.com/cznic/lexer"
	"sort"
	"strings"
)

type stateSet struct {
	count  uint
	dense  denses
	sparse []uint
}

type dense struct {
	id       uint
	priority int
}

type denses []dense

func (d denses) Len() int {
	return len(d)
}

func (d denses) Less(i, j int) bool {
	return d[i].id < d[j].id
}

func (d denses) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func newStateSet(n int) *stateSet {
	return &stateSet{0, make([]dense, n), make([]uint, n)}
}

func (s *stateSet) clear() {
	s.count = 0
}

func (s *stateSet) has(state *lexer.NfaState, priority *int) bool {
	id := state.Index
	i := s.sparse[id]
	d := s.dense[i]
	if i < s.count && d.id == id {
		*priority = d.priority
		return true
	}

	return false
}

func (s *stateSet) include(state *lexer.NfaState, priority int) bool {
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

func (s *stateSet) closure(state *lexer.NfaState, priority int) *stateSet {
	if !s.include(state, priority) {
		for _, edge := range state.NonConsuming {
			p := edge.Priority()
			if p == 0 { // inherit
				p = priority
			}
			s.closure(edge.Target(), p)
		}
	}
	return s
}

func (s *stateSet) id() string {
	d := make(denses, s.count)
	copy(d, s.dense)
	sort.Sort(d)
	a := make([]string, len(d))
	for i, x := range d {
		a[i] = fmt.Sprintf("%d %d", x.id, x.priority)
	}
	return strings.Join(a, "|")
}
