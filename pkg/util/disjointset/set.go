// Copyright 2024 PingCAP, Inc.
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

// Set is the universal implementation of disjoint set.
// It's designed for sparse case or not integer type.
// We hash the original value to an integer index and then apply the core disjoint set algorithm.
// Time complexity: the union operation is inverse ackermann function, which is very close to O(1).
type Set[T comparable] struct {
	parent  []int
	val2idx map[T]int
	tailIdx int
}

func NewSet[T comparable](size int) *Set[T] {
	return &Set[T]{
		parent:  make([]int, 0, size),
		val2idx: make(map[T]int),
		tailIdx: 0,
	}
}

func (s *Set[T]) findRootOrigialVal(a T) int {
	idx, ok := s.val2idx[a]
	if !ok {
		s.parent = append(s.parent, s.tailIdx)
		s.val2idx[a] = s.tailIdx
		s.tailIdx++
		return s.tailIdx - 1
	}
	s.parent[idx] = s.findRoot(s.parent[idx])
	return s.parent[idx]
}

// findRoot is internal impl. Call it inside the findRootOrig.
func (s *Set[T]) findRoot(a int) int {
	if s.parent[a] == a {
		return a
	}
	s.parent[a] = s.findRoot(s.parent[a])
	return s.parent[a]
}

func (s *Set[T]) InSameGroup(a T, b T) bool {
	return s.findRootOrigialVal(a) == s.findRootOrigialVal(b)
}

func (s *Set[T]) Union(a T, b T) {
	rootA := s.findRootOrigialVal(a)
	rootB := s.findRootOrigialVal(b)
	if rootA != rootB {
		s.parent[rootA] = rootB
	}
}
