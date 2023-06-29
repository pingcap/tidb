// Copyright 2022 PingCAP, Inc.
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

package cache

import (
	"sync/atomic"

	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/syncutil"
)

type StatsCache struct {
	cache      atomic.Pointer[statsCache]
	memTracker *memory.Tracker
	mu         syncutil.Mutex
}

func NewStatsCache() StatsCache {
	newCache := newStatsCache()
	result := StatsCache{
		memTracker: memory.NewTracker(memory.LabelForStatsCache, -1),
	}
	result.cache.Store(&newCache)
	return result
}

// Clear removes all cached stats from the cache.
func (s *StatsCache) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	newCache := newStatsCache()
	s.cache.Store(&newCache)
	s.memTracker = memory.NewTracker(memory.LabelForStatsCache, -1)
}

func (s *StatsCache) Load() *statsCache {
	return s.cache.Load()
}

func (s *StatsCache) Version() uint64 {
	return s.Load().version
}
