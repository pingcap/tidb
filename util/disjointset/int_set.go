// Copyright 2018 PingCAP, Inc.
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

package disjointset

type IntSet struct {
	parent []int
}

// NewIntSet returns a new int disjoint set.
func NewIntSet(size int) *IntSet {
	p := make([]int, size)
	for i := range p {
		p[i] = i
	}
	return &IntSet{parent: p}
}

// Init inits or reset the int disjoint set.
func (m *IntSet) Init(l int) {
	m.parent = make([]int, l)
	for i := range m.parent {
		m.parent[i] = i
	}
}

// AddRelation merges two sets in int disjoint set.
func (m *IntSet) AddRelation(a int, b int) {
	m.parent[m.FindRoot(a)] = m.FindRoot(b)
}

// FindRoot finds the representative element of the set that `a` belongs to.
func (m *IntSet) FindRoot(a int) int {
	if a == m.parent[a] {
		return a
	}
	m.parent[a] = m.FindRoot(m.parent[a])
	return m.parent[a]
}
