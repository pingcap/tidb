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

// Set is the universal implementation of a disjoint set.
// It's designed for sparse cases or non-integer types.
// If you are dealing with continuous integers, you should use SimpleIntSet to avoid the cost of a hash map.
// We hash the original value to an integer index and then apply the core disjoint set algorithm.
// Time complexity: the union operation has an inverse Ackermann function time complexity, which is very close to O(1).
type Set[T comparable] struct {
	parent  []int
	val2Idx map[T]int
	idx2Val map[int]T
	tailIdx int
}

// NewSet creates a disjoint set.
func NewSet[T comparable](size int) *Set[T] {
	return &Set[T]{
		parent:  make([]int, 0, size),
		val2Idx: make(map[T]int, size),
		idx2Val: make(map[int]T, size),
		tailIdx: 0,
	}
}

func (s *Set[T]) findRootOriginalVal(a T) int {
	idx, ok := s.val2Idx[a]
	if !ok {
		s.parent = append(s.parent, s.tailIdx)
		s.val2Idx[a] = s.tailIdx
		s.tailIdx++
		s.idx2Val[s.tailIdx-1] = a
		return s.tailIdx - 1
	}
	return s.findRootInternal(idx)
}

// findRoot is an internal implementation. Call it inside findRootOriginalVal.
func (s *Set[T]) findRootInternal(a int) int {
	if s.parent[a] != a {
		// Path compression, which leads the time complexity to the inverse Ackermann function.
		s.parent[a] = s.findRootInternal(s.parent[a])
	}
	return s.parent[a]
}

// InSameGroup checks whether a and b are in the same group.
func (s *Set[T]) InSameGroup(a, b T) bool {
	return s.findRootOriginalVal(a) == s.findRootOriginalVal(b)
}

// Union joins two sets in the disjoint set.
func (s *Set[T]) Union(a, b T) {
	rootA := s.findRootOriginalVal(a)
	rootB := s.findRootOriginalVal(b)
	// take b as successor, respect the rootA as the root of the new set.
	if rootA != rootB {
		s.parent[rootB] = rootA
	}
}

// FindRoot finds the root of the set that contains a.
func (s *Set[T]) FindRoot(a T) int {
	// if a is not in the set, assign a new index to it.
	return s.findRootOriginalVal(a)
}

// FindVal finds the value of the set corresponding to the index.
func (s *Set[T]) FindVal(idx int) (T, bool) {
	v, ok := s.idx2Val[s.findRootInternal(idx)]
	return v, ok
}
