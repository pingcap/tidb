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
	"unsafe"

	"github.com/pingcap/tidb/util/hack"
)

const (
	// DefStringSetBucketMemoryUsage = bucketSize*(1+unsafe.Sizeof(string) + unsafe.Sizeof(struct{}))+2*ptrSize
	// ref https://github.com/golang/go/blob/go1.15.6/src/reflect/type.go#L2162.
	DefStringSetBucketMemoryUsage = 8*(1+16+0) + 16
	// DefFloat64SetBucketMemoryUsage = bucketSize*(1+unsafe.Sizeof(float64) + unsafe.Sizeof(struct{}))+2*ptrSize
	DefFloat64SetBucketMemoryUsage = 8*(1+8+0) + 16
	// DefInt64SetBucketMemoryUsage = bucketSize*(1+unsafe.Sizeof(int64) + unsafe.Sizeof(struct{}))+2*ptrSize
	DefInt64SetBucketMemoryUsage = 8*(1+8+0) + 16

	// DefFloat64Size is the size of float64
	DefFloat64Size = int64(unsafe.Sizeof(float64(0)))
	// DefInt64Size is the size of int64
	DefInt64Size = int64(unsafe.Sizeof(int64(0)))
)

// StringSetWithMemoryUsage is a string set with memory usage.
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

// Insert inserts `val` into `s` and return memDelta.
func (s *StringSetWithMemoryUsage) Insert(val string) (memDelta int64) {
	s.StringSet.Insert(val)
	if s.Count() > (1<<s.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta = DefStringSetBucketMemoryUsage * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta + int64(len(val))
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
	memDelta = DefFloat64SetBucketMemoryUsage * (1 << setWithMemoryUsage.bInMap)
	for _, s := range ss {
		memDelta += setWithMemoryUsage.Insert(s)
	}
	return setWithMemoryUsage, memDelta
}

// Insert inserts `val` into `s` and return memDelta.
func (s *Float64SetWithMemoryUsage) Insert(val float64) (memDelta int64) {
	s.Float64Set.Insert(val)
	if s.Count() > (1<<s.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta = DefFloat64SetBucketMemoryUsage * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta + DefFloat64Size
}

// Int64SetWithMemoryUsage is a int set with memory usage.
type Int64SetWithMemoryUsage struct {
	Int64Set
	bInMap int64
}

// NewInt64SetWithMemoryUsage builds a int64 set.
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

// Insert inserts `val` into `s` and return memDelta.
func (s *Int64SetWithMemoryUsage) Insert(val int64) (memDelta int64) {
	s.Int64Set.Insert(val)
	if s.Count() > (1<<s.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta = DefInt64SetBucketMemoryUsage * (1 << s.bInMap)
		s.bInMap++
	}
	return memDelta + DefInt64Size
}
