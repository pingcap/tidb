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

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal/metrics"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// StatsCacheImpl implements util.StatsCache.
type StatsCacheImpl struct {
	atomic.Pointer[StatsCache]
}

// NewStatsCacheImpl creates a new StatsCache.
func NewStatsCacheImpl() (util.StatsCache, error) {
	newCache, err := NewStatsCache()
	if err != nil {
		return nil, err
	}
	result := &StatsCacheImpl{}
	result.Store(newCache)
	return result, nil
}

// Replace replaces this cache.
func (s *StatsCacheImpl) Replace(cache util.StatsCache) {
	x := cache.(*StatsCacheImpl)
	s.replace(x.Load())
}

// replace replaces the cache with the new cache.
func (s *StatsCacheImpl) replace(newCache *StatsCache) {
	old := s.Swap(newCache)
	if old != nil {
		old.Close()
	}
	metrics.CostGauge.Set(float64(newCache.Cost()))
}

// UpdateStatsCache updates the cache with the new cache.
func (s *StatsCacheImpl) UpdateStatsCache(tables []*statistics.Table, deletedIDs []int64) {
	if enableQuota := config.GetGlobalConfig().Performance.EnableStatsCacheMemQuota; enableQuota {
		s.Load().Update(tables, deletedIDs)
	} else {
		newCache := s.Load().CopyAndUpdate(tables, deletedIDs)
		s.replace(newCache)
	}
}

// Close closes this cache.
func (s *StatsCacheImpl) Close() {
	s.Load().Close()
}

// Clear clears this cache.
func (s *StatsCacheImpl) Clear() {
	cache, err := NewStatsCache()
	if err != nil {
		logutil.BgLogger().Warn("create stats cache failed", zap.Error(err))
		return
	}
	s.replace(cache)
}

// MemConsumed returns its memory usage.
func (s *StatsCacheImpl) MemConsumed() (size int64) {
	return s.Load().Cost()
}

// Get returns the specified table's stats.
func (s *StatsCacheImpl) Get(tableID int64) (*statistics.Table, bool) {
	return s.Load().Get(tableID)
}

// Put puts this table stats into the cache.
func (s *StatsCacheImpl) Put(id int64, t *statistics.Table) {
	s.Load().put(id, t)
}

// MaxTableStatsVersion returns the version of the current cache, which is defined as
// the max table stats version the cache has in its lifecycle.
func (s *StatsCacheImpl) MaxTableStatsVersion() uint64 {
	return s.Load().Version()
}

// Values returns all values in this cache.
func (s *StatsCacheImpl) Values() []*statistics.Table {
	return s.Load().Values()
}

// Len returns the length of this cache.
func (s *StatsCacheImpl) Len() int {
	return s.Load().Len()
}

// SetStatsCacheCapacity sets the cache's capacity.
func (s *StatsCacheImpl) SetStatsCacheCapacity(c int64) {
	s.Load().SetCapacity(c)
}
