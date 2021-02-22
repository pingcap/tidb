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
	x := make(Float64Set, len(fs))
	for _, f := range fs {
		x.Insert(f)
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

const (
	// DefFloat64SetBucketMemoryUsage = bucketSize*(1+unsafe.Sizeof(float64) + unsafe.Sizeof(struct{}))+2*ptrSize
	// ref https://github.com/golang/go/blob/go1.15.6/src/reflect/type.go#L2162.
	// The bucket size may be changed by golang implement in the future.
	DefFloat64SetBucketMemoryUsage = 8*(1+8+0) + 16
)

// Float64SetWithMemoryUsage is a float64 set with memory usage.
type Float64SetWithMemoryUsage struct {
	Float64Set
	bInMap int64
}

// NewFloat64SetWithMemoryUsage builds a float64 set.
func NewFloat64SetWithMemoryUsage(ss ...float64) (setWithMemoryUsage Float64SetWithMemoryUsage, memDelta int64) {
	set := make(Float64Set, len(ss))
	setWithMemoryUsage = Float64SetWithMemoryUsage{
		Float64Set: set,
		bInMap:     0,
	}
	memDelta = DefFloat64SetBucketMemoryUsage * (1 << setWithMemoryUsage.bInMap)
	for _, s := range ss {
		memDelta += setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, memDelta
}

// Insert inserts `val` into `s` and return memDelta.
func (s *Float64SetWithMemoryUsage) Insert(val float64) (memDelta int64) {
	s.Float64Set.Insert(val)
	if s.Count() < (1<<s.bInMap)*loadFactorNum/loadFactorDen {
		memDelta = DefFloat64SetBucketMemoryUsage * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta + 8
}
