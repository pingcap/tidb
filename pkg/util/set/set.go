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

package set

import (
	"fmt"
	"sort"
	"strings"
)

// Key is the interface for the key of a set item.
type Key interface {
	Key() string
}

// Set is the interface for a set.
type Set[T Key] interface {
	Add(items ...T)
	Contains(item T) bool
	Remove(item T)
	ToList() []T
	Size() int
	Clone() Set[T]
	String() string
}

type setImpl[T Key] struct {
	s map[string]T
}

// NewSet creates a new set.
func NewSet[T Key]() Set[T] {
	return new(setImpl[T])
}

func (s *setImpl[T]) Add(items ...T) {
	if s.s == nil {
		s.s = make(map[string]T)
	}
	for _, item := range items {
		s.s[item.Key()] = item
	}
}

func (s *setImpl[T]) Contains(item T) bool {
	if s.s == nil {
		return false
	}
	_, ok := s.s[item.Key()]
	return ok
}

func (s *setImpl[T]) ToList() []T {
	if s == nil {
		return nil
	}
	list := make([]T, 0, len(s.s))
	for _, v := range s.s {
		list = append(list, v)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Key() < list[j].Key()
	}) // to make the result stable
	return list
}

func (s *setImpl[T]) Remove(item T) {
	delete(s.s, item.Key())
}

func (s *setImpl[T]) Size() int {
	if s == nil {
		return 0
	}
	return len(s.s)
}

func (s *setImpl[T]) Clone() Set[T] {
	clone := NewSet[T]()
	clone.Add(s.ToList()...)
	return clone
}

func (s *setImpl[T]) String() string {
	items := make([]string, 0, len(s.s))
	for _, item := range s.s {
		items = append(items, item.Key())
	}
	sort.Strings(items)
	return fmt.Sprintf("{%v}", strings.Join(items, ", "))
}

// ListToSet converts a list to a set.
func ListToSet[T Key](items ...T) Set[T] {
	s := NewSet[T]()
	for _, item := range items {
		s.Add(item)
	}
	return s
}

// UnionSet returns the union set of the given sets.
func UnionSet[T Key](ss ...Set[T]) Set[T] {
	if len(ss) == 0 {
		return NewSet[T]()
	}
	if len(ss) == 1 {
		return ss[0].Clone()
	}
	s := NewSet[T]()
	for _, set := range ss {
		s.Add(set.ToList()...)
	}
	return s
}

// AndSet returns the intersection set of the given sets.
func AndSet[T Key](ss ...Set[T]) Set[T] {
	if len(ss) == 0 {
		return NewSet[T]()
	}
	if len(ss) == 1 {
		return ss[0].Clone()
	}
	s := NewSet[T]()
	for _, item := range ss[0].ToList() {
		contained := true
		for _, set := range ss[1:] {
			if !set.Contains(item) {
				contained = false
				break
			}
		}
		if contained {
			s.Add(item)
		}
	}
	return s
}

// DiffSet returns a set of items that are in s1 but not in s2.
// DiffSet({1, 2, 3, 4}, {2, 3}) = {1, 4}
func DiffSet[T Key](s1, s2 Set[T]) Set[T] {
	s := NewSet[T]()
	for _, item := range s1.ToList() {
		if !s2.Contains(item) {
			s.Add(item)
		}
	}
	return s
}

// CombSet returns all combinations of `numberOfItems` items in the given set.
// For example ({a, b, c}, 2) returns {ab, ac, bc}.
func CombSet[T Key](s Set[T], numberOfItems int) []Set[T] {
	return combSetIterate(s.ToList(), NewSet[T](), 0, numberOfItems)
}

func combSetIterate[T Key](itemList []T, currSet Set[T], depth, numberOfItems int) []Set[T] {
	if currSet.Size() == numberOfItems {
		return []Set[T]{currSet.Clone()}
	}
	if depth == len(itemList) || currSet.Size() > numberOfItems {
		return nil
	}
	var res []Set[T]
	currSet.Add(itemList[depth])
	res = append(res, combSetIterate(itemList, currSet, depth+1, numberOfItems)...)
	currSet.Remove(itemList[depth])
	res = append(res, combSetIterate(itemList, currSet, depth+1, numberOfItems)...)
	return res
}
