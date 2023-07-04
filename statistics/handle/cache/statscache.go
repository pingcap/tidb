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

	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/syncutil"
)

// StatsCache is used to cache the stats of a table.
type StatsCache struct {
	cache      atomic.Pointer[StatsCacheWrapper]
	memTracker *memory.Tracker
	mu         syncutil.Mutex
}

// NewStatsCache creates a new StatsCache.
func NewStatsCache() *StatsCache {
	newCache := NewStatsCacheWrapper()
	result := StatsCache{
		memTracker: memory.NewTracker(memory.LabelForStatsCache, -1),
	}
	result.cache.Store(newCache)
	return &result
}

// Clear removes all cached stats from the cache.
func (s *StatsCache) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	newCache := NewStatsCacheWrapper()
	s.cache.Store(newCache)
	s.memTracker = memory.NewTracker(memory.LabelForStatsCache, -1)
}

// Load loads the cached stats from the cache.
func (s *StatsCache) Load() *StatsCacheWrapper {
	return s.cache.Load()
}

// Version returns the version of the cached stats.
func (s *StatsCache) Version() uint64 {
	return s.Load().version
}

// GetMemConsumed returns the memory usage of the cache.
func (s *StatsCache) GetMemConsumed() int64 {
	return s.memTracker.BytesConsumed()
}

// UpdateCache updates the cache with the new cache.
func (s *StatsCache) UpdateCache(newCache StatsCacheWrapper) (updated bool, newCost int64) {
	s.mu.Lock()
	oldCache := s.cache.Load()
	newCost = newCache.Cost()
	if oldCache.version < newCache.version || (oldCache.version == newCache.version && oldCache.minorVersion < newCache.minorVersion) {
		s.memTracker.Consume(newCost - oldCache.Cost())
		s.cache.Store(&newCache)
		updated = true
	}
	s.mu.Unlock()
	return updated, newCost
}
