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

// Int64Set is a string set.
type Int64Set map[int64]struct{}

// NewInt64Set builds a Int64Set set.
func NewInt64Set() Int64Set {
	return make(map[int64]struct{})
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
