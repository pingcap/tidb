// Copyright 2023 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal/lfu"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal/mapcache"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// NewStatsCache creates a new StatsCacheWrapper.
func NewStatsCache() (*StatsCache, error) {
	enableQuota := config.GetGlobalConfig().Performance.EnableStatsCacheMemQuota
	if enableQuota {
		capacity := variable.StatsCacheMemQuota.Load()
		stats, err := lfu.NewLFU(capacity)
		if err != nil {
			return nil, err
		}
		return &StatsCache{
			c: stats,
		}, nil
	}

	return &StatsCache{
		c: mapcache.NewMapCache(),
	}, nil
}

// StatsCache caches the tables in memory for Handle.
// TODO: hide this structure or merge it into StatsCacheImpl.
type StatsCache struct {
	c internal.StatsCacheInner
	// the max table stats version the cache has in its lifecycle.
	maxTblStatsVer atomic.Uint64
}

// Len returns the number of tables in the cache.
func (sc *StatsCache) Len() int {
	return sc.c.Len()
}

// Get returns the statistics of the specified Table ID.
// The returned value should be read-only, if you update it, don't forget to use Put to put it back again, otherwise the memory trace can be inaccurate.
//
//	e.g. v := sc.Get(id); /* update the value */ v.Version = 123; sc.Put(id, v);
func (sc *StatsCache) Get(id int64) (*statistics.Table, bool) {
	result, ok := sc.c.Get(id)
	if ok {
		metrics.HitCounter.Add(1)
	} else {
		metrics.MissCounter.Add(1)
	}
	return result, ok
}

// Put puts the table statistics to the cache from query.
func (sc *StatsCache) Put(id int64, t *statistics.Table) {
	sc.put(id, t)
}

func (sc *StatsCache) putCache(id int64, t *statistics.Table) bool {
	metrics.UpdateCounter.Inc()
	ok := sc.c.Put(id, t)
	if ok {
		return ok
	}
	// TODO(hawkingrei): If necessary, add asynchronous retries
	logutil.BgLogger().Warn("fail to put the stats cache", zap.Int64("id", id))
	return ok
}

// Put puts the table statistics to the cache.
func (sc *StatsCache) put(id int64, t *statistics.Table) {
	i := 1
	for {
		// retry if the cache is full
		ok := sc.putCache(id, t)
		if ok {
			// update the maxTblStatsVer
			for v := sc.maxTblStatsVer.Load(); v < t.Version; v = sc.maxTblStatsVer.Load() {
				if sc.maxTblStatsVer.CompareAndSwap(v, t.Version) {
					break
				} // other goroutines have updated the sc.maxTblStatsVer, so we need to check again.
			}
			return
		}
		if i%10 == 0 {
			logutil.BgLogger().Warn("fail to put the stats cache", zap.Int64("id", id))
		}
		time.Sleep(5 * time.Millisecond)
		i++
	}
}

// Values returns all the cached statistics tables.
func (sc *StatsCache) Values() []*statistics.Table {
	return sc.c.Values()
}

// Cost returns the memory usage of the cache.
func (sc *StatsCache) Cost() int64 {
	return sc.c.Cost()
}

// SetCapacity sets the memory capacity of the cache.
func (sc *StatsCache) SetCapacity(c int64) {
	// metrics will be updated in the SetCapacity function of the StatsCacheInner.
	sc.c.SetCapacity(c)
}

// Close stops the cache.
func (sc *StatsCache) Close() {
	sc.c.Close()
}

// Version returns the version of the current cache, which is defined as
// the max table stats version the cache has in its lifecycle.
func (sc *StatsCache) Version() uint64 {
	return sc.maxTblStatsVer.Load()
}

// CopyAndUpdate copies a new cache and updates the new statistics table cache. It is only used in the COW mode.
func (sc *StatsCache) CopyAndUpdate(tables []*statistics.Table, deletedIDs []int64) *StatsCache {
	newCache := &StatsCache{c: sc.c.Copy()}
	newCache.maxTblStatsVer.Store(sc.maxTblStatsVer.Load())
	for _, tbl := range tables {
		id := tbl.PhysicalID
		newCache.c.Put(id, tbl)
	}
	for _, id := range deletedIDs {
		newCache.c.Del(id)
	}

	// update the maxTblStatsVer
	for _, t := range tables {
		if t.Version > newCache.maxTblStatsVer.Load() {
			newCache.maxTblStatsVer.Store(t.Version)
		}
	}
	return newCache
}

// Update updates the new statistics table cache.
func (sc *StatsCache) Update(tables []*statistics.Table, deletedIDs []int64) {
	for _, tbl := range tables {
		id := tbl.PhysicalID
		metrics.UpdateCounter.Inc()
		sc.c.Put(id, tbl)
	}
	for _, id := range deletedIDs {
		metrics.DelCounter.Inc()
		sc.c.Del(id)
	}

	// update the maxTblStatsVer
	for _, t := range tables {
		if oldVersion := sc.maxTblStatsVer.Load(); t.Version > oldVersion {
			sc.maxTblStatsVer.CompareAndSwap(oldVersion, t.Version)
		}
	}
}
