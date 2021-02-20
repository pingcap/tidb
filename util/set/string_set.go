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

// StringSet is a string set.
type StringSet map[string]struct{}

// NewStringSet builds a string set.
func NewStringSet(ss ...string) StringSet {
	set := make(StringSet, len(ss))
	for _, s := range ss {
		set.Insert(s)
	}
	return set
}

// Exist checks whether `val` exists in `s`.
func (s StringSet) Exist(val string) bool {
	_, ok := s[val]
	return ok
}

// Insert inserts `val` into `s`.
func (s StringSet) Insert(val string) {
	s[val] = struct{}{}
}

// Intersection returns the intersection of two sets
func (s StringSet) Intersection(rhs StringSet) StringSet {
	newSet := NewStringSet()
	for elt := range s {
		if rhs.Exist(elt) {
			newSet.Insert(elt)
		}
	}
	return newSet
}

// Count returns the number in Set s.
func (s StringSet) Count() int {
	return len(s)
}

const (
	// ref https://github.com/golang/go/blob/go1.15.6/src/reflect/type.go#L2162.
	// DefStringSetBucketMemoryUsage = bucketSize*(1+unsafe.Sizeof(string) + unsafe.Sizeof(struct{}))+2*ptrSize
	// The bucket size may be changed by golang implement in the future.
	DefStringSetBucketMemoryUsage = 8*(1+16+0) + 16
	// Maximum average load of a bucket that triggers growth is 6.5.
	// Represent as loadFactorNum/loadFactorDen, to allow integer math.
	loadFactorNum = 13
	loadFactorDen = 2
)

type StringSetWithMemoryUsage struct {
	StringSet
	bInMap int64
}

// NewStringSetWithMemoryUsage builds a string set.
func NewStringSetWithMemoryUsage(ss ...string) (setWithMemoryUsage StringSetWithMemoryUsage, memDelta int64) {
	set := make(StringSet, len(ss))
	setWithMemoryUsage = StringSetWithMemoryUsage{
		StringSet: set,
		bInMap:    0,
	}
	memDelta = DefStringSetBucketMemoryUsage * (1 << setWithMemoryUsage.bInMap)
	for _, s := range ss {
		memDelta += setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, memDelta
}

func (s StringSetWithMemoryUsage) Insert(val string) (memDelta int64) {
	s.StringSet.Insert(val)
	if s.Count() < (1<<s.bInMap)*loadFactorNum/loadFactorDen {
		memDelta = DefStringSetBucketMemoryUsage * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta + int64(len(val))
}
