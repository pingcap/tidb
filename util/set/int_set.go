// Copyright 2019 PingCAP, Inc.
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

package set

// IntSet is a int set.
type IntSet map[int]struct{}

// NewIntSet builds a IntSet.
func NewIntSet() IntSet {
	return make(map[int]struct{})
}

// Exist checks whether `val` exists in `s`.
func (s IntSet) Exist(val int) bool {
	_, ok := s[val]
	return ok
}

// Insert inserts `val` into `s`.
func (s IntSet) Insert(val int) {
	s[val] = struct{}{}
}

// Count returns the number in Set s.
func (s IntSet) Count() int {
	return len(s)
}

// Int64Set is a int64 set.
type Int64Set map[int64]struct{}

// NewInt64Set builds a Int64Set.
func NewInt64Set(xs ...int64) Int64Set {
	set := make(Int64Set, len(xs))
	for _, x := range xs {
		set.Insert(x)
	}
	return set
}

// Exist checks whether `val` exists in `s`.
func (s Int64Set) Exist(val int64) bool {
	_, ok := s[val]
	return ok
}

// Insert inserts `val` into `s`.
func (s Int64Set) Insert(val int64) {
	s[val] = struct{}{}
}

// Count returns the number in Set s.
func (s Int64Set) Count() int {
	return len(s)
}
