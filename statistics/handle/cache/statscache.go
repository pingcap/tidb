// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a Copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"sync/atomic"
)

// StatsCachePointer is used to cache the stats of a table.
type StatsCachePointer struct {
	atomic.Pointer[StatsCacheWrapper]
}

// NewStatsCachePointer creates a new StatsCache.
func NewStatsCachePointer() *StatsCachePointer {
	newCache := NewStatsCacheWrapper()
	result := StatsCachePointer{}
	result.Store(newCache)
	return &result
}

// Load loads the cached stats from the cache.
func (s *StatsCachePointer) Load() *StatsCacheWrapper {
	return s.Pointer.Load()
}

// Replace replaces the cache with the new cache.
func (s *StatsCachePointer) Replace(newCache *StatsCacheWrapper) {
	s.Store(newCache)
}
