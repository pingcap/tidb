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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// StringToStringSetWithMemoryUsage is a string-string set with memory usage.
type StringToStringSetWithMemoryUsage struct {
	hack.MemAwareMap[string, string]
	bInMap int64

	// For tracking large memory usage in time.
	// If tracker is non-nil, memDelta will track immediately and reset to 0. Otherwise, memDelta will return and lazy track.
	tracker *memory.Tracker
}

// NewStringToStringSetWithMemoryUsage builds a string set.
func NewStringToStringSetWithMemoryUsage() (setWithMemoryUsage StringToStringSetWithMemoryUsage, delta int64) {
	set := make(map[string]string)
	delta = setWithMemoryUsage.Init(set)
	setWithMemoryUsage.bInMap = 0
	return setWithMemoryUsage, delta
}

// Insert inserts `val` into `s` and return delta.
func (s *StringToStringSetWithMemoryUsage) Insert(key string, val string) (delta int64) {
	delta = s.Set(key, val)
	if delta != 0 && s.tracker != nil {
		s.tracker.Consume(delta)
		delta = 0
	}
	return delta
}

// SetTracker sets memory tracker for StringToStringMapWithMemoryUsage
func (s *StringToStringSetWithMemoryUsage) SetTracker(t *memory.Tracker) {
	s.tracker = t
}

// StringToDecimalSetWithMemoryUsage is a string-decimal set with memory usage.
type StringToDecimalSetWithMemoryUsage struct {
	hack.MemAwareMap[string, *types.MyDecimal]
	bInMap int64

	// For tracking large memory usage in time.
	// If tracker is non-nil, memDelta will track immediately and reset to 0. Otherwise, memDelta will return and lazy track.
	tracker *memory.Tracker
}

// NewStringToDecimalSetWithMemoryUsage builds a string set.
func NewStringToDecimalSetWithMemoryUsage() (setWithMemoryUsage StringToDecimalSetWithMemoryUsage, delta int64) {
	set := make(map[string]*types.MyDecimal)
	delta = setWithMemoryUsage.Init(set)
	setWithMemoryUsage.bInMap = 0
	return setWithMemoryUsage, delta
}

// Insert inserts `val` into `s` and return delta.
func (s *StringToDecimalSetWithMemoryUsage) Insert(key string, val *types.MyDecimal) (delta int64) {
	delta = s.Set(key, val)
	if delta != 0 && s.tracker != nil {
		s.tracker.Consume(delta)
		delta = 0
	}
	return delta
}

// SetTracker sets memory tracker for StringToDecimalMapWithMemoryUsage
func (s *StringToDecimalSetWithMemoryUsage) SetTracker(t *memory.Tracker) {
	s.tracker = t
}

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
