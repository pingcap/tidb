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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disjointset

import (
	"slices"
)

// SimpleIntSet is the int disjoint set.
// It's not designed for sparse case. You should use it when the elements are continuous.
// Time complexity: the union operation is inverse ackermann function, which is very close to O(1).
type SimpleIntSet struct {
	parent []int
}

// NewIntSet returns a new int disjoint set.
func NewIntSet(size int) *SimpleIntSet {
	p := make([]int, size)
	for i := range p {
		p[i] = i
	}
	return &SimpleIntSet{parent: p}
}

// Union unions two sets in int disjoint set.
func (m *SimpleIntSet) Union(a int, b int) {
	m.parent[m.FindRoot(a)] = m.FindRoot(b)
}

// FindRoot finds the representative element of the set that `a` belongs to.
func (m *SimpleIntSet) FindRoot(a int) int {
	if a == m.parent[a] {
		return a
	}
	// Path compression, which leads the time complexity to the inverse Ackermann function.
	m.parent[a] = m.FindRoot(m.parent[a])
	return m.parent[a]
}

// Clear clears the int disjoint set.
func (m *SimpleIntSet) Clear() {
	m.parent = m.parent[:0]
}

// GrowNewIntSet grows the int disjoint set to at least `n` elements.
func (m *SimpleIntSet) GrowNewIntSet(n int) {
	m.parent = m.parent[:0]
	m.parent = slices.Grow(m.parent, n)
	for i := range n {
		m.parent = append(m.parent, i)
	}
}
