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

package set

// Float64Set is a float64 set.
type Float64Set map[float64]struct{}

// NewFloat64Set builds a float64 set.
func NewFloat64Set(fs ...float64) Float64Set {
	x := make(map[float64]struct{}, len(fs))
	for _, f := range fs {
		x[f] = struct{}{}
	}
	return x
}

// Exist checks whether `val` exists in `s`.
func (s Float64Set) Exist(val float64) bool {
	_, ok := s[val]
	return ok
}

// Insert inserts `val` into `s`.
func (s Float64Set) Insert(val float64) {
	s[val] = struct{}{}
}

// Count returns the number in Set s.
func (s Float64Set) Count() int {
	return len(s)
}
