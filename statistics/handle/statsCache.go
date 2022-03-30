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
	"github.com/pingcap/tidb/statistics"
)

type kvCache interface {
	Get(int64) (*statistics.Table, bool)
	Put(int64, *statistics.Table)
	Del(int64)
	Cost() int64
	Keys() []int64
	Values() []*statistics.Table
	Len() int
	Copy() kvCache
}

func newStatsCache() statsCache {
	return statsCache{
		kvCache: newInternalMapCache(),
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

	kvCache
}

func (sc statsCache) copy() statsCache {
	newCache := statsCache{kvCache: newInternalMapCache(),
		version:      sc.version,
		minorVersion: sc.minorVersion,
	}
	for _, k := range sc.kvCache.Keys() {
		v, _ := sc.kvCache.Get(k)
		newCache.kvCache.Put(k, v)
	}
	return newCache
}

// update updates the statistics table cache using copy on write.
func (sc statsCache) update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) statsCache {
	newCache := sc.copy()
	if newVersion == newCache.version {
		newCache.minorVersion += uint64(1)
	} else {
		newCache.version = newVersion
		newCache.minorVersion = uint64(0)
	}
	for _, tbl := range tables {
		id := tbl.PhysicalID
		newCache.Put(id, tbl)
	}
	for _, id := range deletedIDs {
		newCache.Del(id)
	}
	return newCache
}

type internapMapCache struct {
	cache map[int64]*statistics.Table
	cost  int64
}

func newInternalMapCache() kvCache {
	return &internapMapCache{
		cache: make(map[int64]*statistics.Table),
	}
}

func (m *internapMapCache) Get(key int64) (*statistics.Table, bool) {
	v, ok := m.cache[key]
	return v, ok
}

func (m *internapMapCache) Put(key int64, table *statistics.Table) {
	old, ok := m.Get(key)
	if ok {
		m.cache[key] = table
		m.cost += table.MemoryUsage() - old.MemoryUsage()
		return
	}
	m.cache[key] = table
	m.cost += table.MemoryUsage()
}

func (m *internapMapCache) Del(key int64) {
	old, ok := m.Get(key)
	if !ok {
		return
	}
	delete(m.cache, key)
	m.cost -= old.MemoryUsage()
}

func (m *internapMapCache) Cost() int64 {
	return m.cost
}

func (m *internapMapCache) Keys() []int64 {
	r := make([]int64, 0)
	for k := range m.cache {
		r = append(r, k)
	}
	return r
}

func (m *internapMapCache) Values() []*statistics.Table {
	r := make([]*statistics.Table, 0)
	for _, v := range m.cache {
		r = append(r, v)
	}
	return r
}

func (m *internapMapCache) Len() int {
	return len(m.cache)
}

func (m *internapMapCache) Copy() kvCache {
	newM := &internapMapCache{
		cache: make(map[int64]*statistics.Table),
		cost:  m.cost,
	}
	for k, v := range m.cache {
		newM.cache[k] = v
	}
	return newM
}
