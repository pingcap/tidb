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

package handle

import (
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"math"
)

// statsCacheInner is the interface to manage the statsCache, it can be implemented by map, lru cache or other structures.
type statsCacheInner interface {
	GetByQuery(int64) (*statistics.Table, bool)
	Get(int64) (*statistics.Table, bool)
	PutByQuery(int64, *statistics.Table)
	Put(int64, *statistics.Table)
	Del(int64)
	Cost() int64
	Keys() []int64
	Values() []*statistics.Table
	Map() map[int64]*statistics.Table
	Len() int
	FreshMemUsage()
	FreshTableCost(int64)
	Copy() statsCacheInner
	SetCapacity(int64)
}

func newStatsCache() statsCache {
	capacity := variable.StatsCacheMemQuota.Load()
	if capacity < 1 {
		capacity = math.MaxInt64
	}
	return statsCache{
		statsCacheInner: newStatsLruCache(capacity),
	}
}

// statsCache caches the tables in memory for Handle.
type statsCache struct {
	// version is the latest version of cache. It is bumped when new records of `mysql.stats_meta` are loaded into cache.
	version uint64
	// minorVersion is to differentiate the cache when the version is unchanged while the cache contents are
	// modified indeed. This can happen when we load extra column histograms into cache, or when we modify the cache with
	// statistics feedbacks, etc. We cannot bump the version then because no new changes of `mysql.stats_meta` are loaded,
	// while the override of statsCache is in a copy-on-write way, to make sure the statsCache is unchanged by others during the
	// the interval of 'copy' and 'write', every 'write' should bump / check this minorVersion if the version keeps
	// unchanged.
	// This bump / check logic is encapsulated in `statsCache.update` and `updateStatsCache`, callers don't need to care
	// about this minorVersion actually.
	minorVersion uint64

	statsCacheInner
}

func (sc statsCache) copy() statsCache {
	newCache := statsCache{
		version:      sc.version,
		minorVersion: sc.minorVersion,
	}
	newCache.statsCacheInner = sc.statsCacheInner.Copy()
	return newCache
}

// update updates the statistics table cache using copy on write.
func (sc statsCache) update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64, opts ...TableStatsOpt) statsCache {
	option := &tableStatsOption{}
	for _, opt := range opts {
		opt(option)
	}
	newCache := sc.copy()
	if newVersion == newCache.version {
		newCache.minorVersion += uint64(1)
	} else {
		newCache.version = newVersion
		newCache.minorVersion = uint64(0)
	}
	for _, tbl := range tables {
		id := tbl.PhysicalID
		if option.byQuery {
			newCache.PutByQuery(id, tbl)
		} else {
			newCache.Put(id, tbl)
		}
	}
	for _, id := range deletedIDs {
		newCache.Del(id)
	}
	return newCache
}
