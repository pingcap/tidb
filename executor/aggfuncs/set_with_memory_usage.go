// Copyright 2020 PingCAP, Inc.
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

package aggfuncs

import "github.com/pingcap/tidb/util/set"

const (
	// DefStringSetBucketMemoryUsage = bucketSize*(1+unsafe.Sizeof(string) + unsafe.Sizeof(struct{}))+2*ptrSize
	DefStringSetBucketMemoryUsage = 8*(1+16+0) + 16
	// DefFloat64SetBucketMemoryUsage = bucketSize*(1+unsafe.Sizeof(float64) + unsafe.Sizeof(struct{}))+2*ptrSize
	DefFloat64SetBucketMemoryUsage = 8*(1+8+0) + 16
	// DefInt64SetBucketMemoryUsage = bucketSize*(1+unsafe.Sizeof(int64) + unsafe.Sizeof(struct{}))+2*ptrSize
	DefInt64SetBucketMemoryUsage = 8*(1+8+0) + 16
	// Maximum average load of a bucket that triggers growth is 6.5.
	// Represent as loadFactorNum/loadFactDen, to allow integer math.
	loadFactorNum = 13
	loadFactorDen = 2
)

// stringSetWithMemoryUsage is a string set with memory usage.
type stringSetWithMemoryUsage struct {
	set.StringSet
	bInMap int64
}

// newStringSetWithMemoryUsage builds a string set.
func newStringSetWithMemoryUsage(ss ...string) (setWithMemoryUsage stringSetWithMemoryUsage, memDelta int64) {
	set := make(set.StringSet, len(ss))
	setWithMemoryUsage = stringSetWithMemoryUsage{
		StringSet: set,
		bInMap:    0,
	}
	memDelta = DefStringSetBucketMemoryUsage * (1 << setWithMemoryUsage.bInMap)
	for _, s := range ss {
		memDelta += setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, memDelta
}

// Insert inserts `val` into `s` and return memDelta.
func (s *stringSetWithMemoryUsage) Insert(val string) (memDelta int64) {
	s.StringSet.Insert(val)
	if s.Count() < (1<<s.bInMap)*loadFactorNum/loadFactorDen {
		memDelta = DefStringSetBucketMemoryUsage * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta + int64(len(val))
}

// float64SetWithMemoryUsage is a float64 set with memory usage.
type float64SetWithMemoryUsage struct {
	set.Float64Set
	bInMap int64
}

// newFloat64SetWithMemoryUsage builds a float64 set.
func newFloat64SetWithMemoryUsage(ss ...float64) (setWithMemoryUsage float64SetWithMemoryUsage, memDelta int64) {
	set := make(set.Float64Set, len(ss))
	setWithMemoryUsage = float64SetWithMemoryUsage{
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
func (s *float64SetWithMemoryUsage) Insert(val float64) (memDelta int64) {
	s.Float64Set.Insert(val)
	if s.Count() < (1<<s.bInMap)*loadFactorNum/loadFactorDen {
		memDelta = DefFloat64SetBucketMemoryUsage * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta + 8
}

// int64SetWithMemoryUsage is a int set with memory usage.
type int64SetWithMemoryUsage struct {
	set.Int64Set
	bInMap int64
}

// newInt64SetWithMemoryUsage builds a int64 set.
func newInt64SetWithMemoryUsage(ss ...int64) (setWithMemoryUsage int64SetWithMemoryUsage, memDelta int64) {
	set := make(set.Int64Set, len(ss))
	setWithMemoryUsage = int64SetWithMemoryUsage{
		Int64Set: set,
		bInMap:   0,
	}
	memDelta = DefInt64SetBucketMemoryUsage * (1 << setWithMemoryUsage.bInMap)
	for _, s := range ss {
		memDelta += setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, memDelta
}

// Insert inserts `val` into `s` and return memDelta.
func (s *int64SetWithMemoryUsage) Insert(val int64) (memDelta int64) {
	s.Int64Set.Insert(val)
	if s.Count() < (1<<s.bInMap)*loadFactorNum/loadFactorDen {
		memDelta = DefInt64SetBucketMemoryUsage * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta + 8
}
