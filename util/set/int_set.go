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
func NewIntSet(is ...int) IntSet {
	set := make(IntSet, len(is))
	for _, x := range is {
		set.Insert(x)
	}
	return set
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

const (
	// ref https://github.com/golang/go/blob/go1.15.6/src/reflect/type.go#L2162.
	// DefInt64SetBucketMemoryUsage = bucketSize*(1+unsafe.Sizeof(int64) + unsafe.Sizeof(struct{}))+2*ptrSize
	// The bucket size may be changed by golang implement in the future.
	DefInt64SetBucketMemoryUsage = 8*(1+8+0) + 16
)

type Int64SetWithMemoryUsage struct {
	Int64Set
	bInMap int64
}

// NewStringSetWithMemoryUsage builds a string set.
func NewInt64SetWithMemoryUsage(ss ...int64) (setWithMemoryUsage Int64SetWithMemoryUsage, memDelta int64) {
	set := make(Int64Set, len(ss))
	setWithMemoryUsage = Int64SetWithMemoryUsage{
		Int64Set: set,
		bInMap:   0,
	}
	memDelta = DefInt64SetBucketMemoryUsage * (1 << setWithMemoryUsage.bInMap)
	for _, s := range ss {
		memDelta += setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, memDelta
}

func (s *Int64SetWithMemoryUsage) Insert(val int64) (memDelta int64) {
	s.Int64Set.Insert(val)
	if s.Count() < (1<<s.bInMap)*loadFactorNum/loadFactorDen {
		memDelta = DefInt64SetBucketMemoryUsage * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta + 8
}
