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

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/internal/lfu"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache/metrics"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// newStatsCache creates a statsCache backed by an LFU cache sized by tidb_stats_cache_mem_quota.
func newStatsCache() (*statsCache, error) {
	capacity := vardef.StatsCacheMemQuota.Load()
	stats, err := lfu.NewLFU(capacity)
	if err != nil {
		return nil, err
	}
	return &statsCache{
		c: stats,
	}, nil
}

// statsCache caches the tables in memory for Handle.
type statsCache struct {
	c *lfu.LFU
	// the max table stats version the cache has in its lifecycle.
	maxTblStatsVer atomic.Uint64
}

// Len returns the number of tables in the cache.
func (sc *statsCache) Len() int {
	return sc.c.Len()
}

// Get returns the statistics of the specified Table ID.
// The returned value should be read-only, if you update it, don't forget to use put to put it back again, otherwise the memory trace can be inaccurate.
//
//	e.g. v := sc.Get(id); /* update the value */ v.Version = 123; sc.put(id, v);
func (sc *statsCache) Get(id int64) (*statistics.Table, bool) {
	result, ok := sc.c.Get(id)
	if ok {
		metrics.HitCounter.Add(1)
	} else {
		metrics.MissCounter.Add(1)
	}
	return result, ok
}

func (sc *statsCache) putCache(id int64, t *statistics.Table) bool {
	metrics.UpdateCounter.Inc()
	intest.Assert(t.ColAndIdxExistenceMap != nil, "ColAndIdxExistenceMap should not be nil")
	ok := sc.c.Put(id, t)
	if ok {
		return ok
	}
	// TODO(hawkingrei): If necessary, add asynchronous retries
	logutil.BgLogger().Warn("fail to put the stats cache", zap.Int64("id", id))
	return ok
}

// put puts the table statistics to the cache and retries until the cache accepts them.
func (sc *statsCache) put(id int64, t *statistics.Table) {
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
func (sc *statsCache) Values() []*statistics.Table {
	return sc.c.Values()
}

// Cost returns the memory usage of the cache.
func (sc *statsCache) Cost() int64 {
	return sc.c.Cost()
}

// SetCapacity sets the memory capacity of the cache.
func (sc *statsCache) SetCapacity(c int64) {
	// LFU.SetCapacity updates the capacity and cost metrics.
	sc.c.SetCapacity(c)
}

// Close stops the cache.
func (sc *statsCache) Close() {
	sc.c.Close()
}

// Version returns the version of the current cache, which is defined as
// the max table stats version the cache has in its lifecycle.
func (sc *statsCache) Version() uint64 {
	return sc.maxTblStatsVer.Load()
}

// Update updates the new statistics table cache.
func (sc *statsCache) Update(tables []*statistics.Table, deletedIDs []int64, skipMoveForwardStatsCache bool) {
	for _, tbl := range tables {
		id := tbl.PhysicalID
		metrics.UpdateCounter.Inc()
		sc.c.Put(id, tbl)
	}
	for _, id := range deletedIDs {
		metrics.DelCounter.Inc()
		sc.c.Del(id)
	}

	if !skipMoveForwardStatsCache {
		// update the maxTblStatsVer
		for _, t := range tables {
			if oldVersion := sc.maxTblStatsVer.Load(); t.Version > oldVersion {
				sc.maxTblStatsVer.CompareAndSwap(oldVersion, t.Version)
			}
		}
	}
}

// TriggerEvict triggers the cache to evict some items.
func (sc *statsCache) TriggerEvict() {
	sc.c.TriggerEvict()
}

// WaitForAsyncUpdates blocks until buffered asynchronous cache writes are visible to later Get calls.
func (sc *statsCache) WaitForAsyncUpdates() {
	sc.c.WaitForAsyncUpdates()
}
