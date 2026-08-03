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
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// StringSetWithMemoryUsage is a string set with memory usage.
type StringSetWithMemoryUsage struct {
	hack.MemAwareMap[string, struct{}]

	// For tracking large memory usage in time.
	// If tracker is non-nil, memDelta will track immediately and reset to 0. Otherwise, memDelta will return and lazy track.
	tracker *memory.Tracker
}

// NewStringSetWithMemoryUsage builds a string set.
func NewStringSetWithMemoryUsage(ss ...string) (setWithMemoryUsage StringSetWithMemoryUsage, memDelta int64) {
	set := make(StringSet, len(ss))
	setWithMemoryUsage.Init(set)
	for _, s := range ss {
		setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, int64(setWithMemoryUsage.Bytes)
}

// Insert inserts `val` into `s` and return memDelta.
func (s *StringSetWithMemoryUsage) Insert(val string) int64 {
	delta := s.Set(val, struct{}{})
	if delta != 0 && s.tracker != nil {
		s.tracker.Consume(delta)
		delta = 0
	}
	return delta
}

// SetTracker sets memory tracker for StringSetWithMemoryUsage
func (s *StringSetWithMemoryUsage) SetTracker(t *memory.Tracker) {
	s.tracker = t
}

// Float64SetWithMemoryUsage is a float64 set with memory usage.
type Float64SetWithMemoryUsage struct {
	hack.MemAwareMap[float64, struct{}]
}

// NewFloat64SetWithMemoryUsage builds a float64 set.
func NewFloat64SetWithMemoryUsage(ss ...float64) (setWithMemoryUsage Float64SetWithMemoryUsage, memDelta int64) {
	set := make(Float64Set, len(ss))
	setWithMemoryUsage.Init(set)
	for _, s := range ss {
		setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, int64(setWithMemoryUsage.Bytes)
}

// Insert inserts `val` into `s` and return memDelta.
func (s *Float64SetWithMemoryUsage) Insert(val float64) int64 {
	return s.Set(val, struct{}{})
}

// Int64SetWithMemoryUsage is a int set with memory usage.
type Int64SetWithMemoryUsage struct {
	hack.MemAwareMap[int64, struct{}]
}

// NewInt64SetWithMemoryUsage builds an int64 set.
func NewInt64SetWithMemoryUsage(ss ...int64) (setWithMemoryUsage Int64SetWithMemoryUsage, memDelta int64) {
	set := make(Int64Set, len(ss))
	setWithMemoryUsage.Init(set)
	for _, s := range ss {
		setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, int64(setWithMemoryUsage.Bytes)
}

// Insert inserts `val` into `s` and return memDelta.
func (s *Int64SetWithMemoryUsage) Insert(val int64) (memDelta int64) {
	return s.Set(val, struct{}{})
}
