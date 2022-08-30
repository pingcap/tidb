// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/memory"
)

// StringSetWithMemoryUsage is a string set with memory usage.
type StringSetWithMemoryUsage struct {
	StringSet
	bInMap int64

	// For tracking large memory usage in time.
	// If tracker is non-nil, memDelta will track immediately and reset to 0. Otherwise, memDelta will return and lazy track.
	tracker *memory.Tracker
}

// NewStringSetWithMemoryUsage builds a string set.
func NewStringSetWithMemoryUsage(ss ...string) (setWithMemoryUsage StringSetWithMemoryUsage, memDelta int64) {
	set := make(StringSet, len(ss))
	setWithMemoryUsage = StringSetWithMemoryUsage{
		StringSet: set,
		bInMap:    0,
	}
	memDelta = hack.DefBucketMemoryUsageForSetString * (1 << setWithMemoryUsage.bInMap)
	for _, s := range ss {
		memDelta += setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, memDelta
}

// Insert inserts `val` into `s` and return memDelta.
func (s *StringSetWithMemoryUsage) Insert(val string) (memDelta int64) {
	s.StringSet.Insert(val)
	if s.Count() > (1<<s.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta = hack.DefBucketMemoryUsageForSetString * (1 << s.bInMap)
		s.bInMap++
		if s.tracker != nil {
			s.tracker.Consume(memDelta)
			memDelta = 0
		}
	}
	return memDelta
}

// SetTracker sets memory tracker for StringSetWithMemoryUsage
func (s *StringSetWithMemoryUsage) SetTracker(t *memory.Tracker) {
	s.tracker = t
}

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
	memDelta = hack.DefBucketMemoryUsageForSetFloat64 * (1 << setWithMemoryUsage.bInMap)
	for _, s := range ss {
		memDelta += setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, memDelta
}

// Insert inserts `val` into `s` and return memDelta.
func (s *Float64SetWithMemoryUsage) Insert(val float64) (memDelta int64) {
	s.Float64Set.Insert(val)
	if s.Count() > (1<<s.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta = hack.DefBucketMemoryUsageForSetFloat64 * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta
}

// Int64SetWithMemoryUsage is a int set with memory usage.
type Int64SetWithMemoryUsage struct {
	Int64Set
	bInMap int64
}

// NewInt64SetWithMemoryUsage builds an int64 set.
func NewInt64SetWithMemoryUsage(ss ...int64) (setWithMemoryUsage Int64SetWithMemoryUsage, memDelta int64) {
	set := make(Int64Set, len(ss))
	setWithMemoryUsage = Int64SetWithMemoryUsage{
		Int64Set: set,
		bInMap:   0,
	}
	memDelta = hack.DefBucketMemoryUsageForSetInt64 * (1 << setWithMemoryUsage.bInMap)
	for _, s := range ss {
		memDelta += setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, memDelta
}

// Insert inserts `val` into `s` and return memDelta.
func (s *Int64SetWithMemoryUsage) Insert(val int64) (memDelta int64) {
	s.Int64Set.Insert(val)
	if s.Count() > (1<<s.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta = hack.DefBucketMemoryUsageForSetInt64 * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta
}
