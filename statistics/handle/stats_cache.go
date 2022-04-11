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
	CalculateCost()
	CalculateTableCost(int64)
	Copy() kvCache
}

func newStatsCache(cap int64) statsCache {
	if cap > 0 {
		return statsCache{
			kvCache: newInternalLRUCache(cap),
		}
	}
	return statsCache{
		kvCache: mapCache{
			tables: make(map[int64]cacheItem),
		},
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
	newCache := statsCache{
		version:      sc.version,
		minorVersion: sc.minorVersion,
	}
	newCache.kvCache = sc.kvCache.Copy()
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

type mapCache struct {
	tables   map[int64]cacheItem
	memQuota int64
}

// Get implements kvCache
func (m mapCache) Get(k int64) (*statistics.Table, bool) {
	v, ok := m.tables[k]
	return v.value, ok
}

// Put implements kvCache
func (m mapCache) Put(k int64, v *statistics.Table) {
	item, ok := m.tables[k]
	if ok {
		oldCost := item.cost
		newCost := v.MemoryUsage()
		item.value = v
		item.cost = newCost
		m.tables[k] = item
		m.memQuota += newCost - oldCost
		return
	}
	cost := v.MemoryUsage()
	item = cacheItem{
		key:   k,
		value: v,
		cost:  cost,
	}
	m.tables[k] = item
	m.memQuota += cost
}

// Del implements kvCache
func (m mapCache) Del(k int64) {
	item, ok := m.tables[k]
	if !ok {
		return
	}
	delete(m.tables, k)
	m.memQuota -= item.cost
}

// Cost implements kvCache
func (m mapCache) Cost() int64 {
	return m.memQuota
}

// Keys implements kvCache
func (m mapCache) Keys() []int64 {
	ks := make([]int64, 0)
	for k := range m.tables {
		ks = append(ks, k)
	}
	return ks
}

// Values implements kvCache
func (m mapCache) Values() []*statistics.Table {
	vs := make([]*statistics.Table, 0)
	for _, v := range m.tables {
		vs = append(vs, v.value)
	}
	return vs
}

// Len implements kvCache
func (m mapCache) Len() int {
	return len(m.tables)
}

// CalculateCost implements kvCache
func (m mapCache) CalculateCost() {
	for _, v := range m.tables {
		oldCost := v.cost
		newCost := v.value.MemoryUsage()
		m.memQuota += newCost - oldCost
	}
}

// CalculateTableCost implements kvCache
func (m mapCache) CalculateTableCost(k int64) {
	item, ok := m.tables[k]
	if !ok {
		return
	}
	m.Put(k, item.value)
}

// Copy implements kvCache
func (m mapCache) Copy() kvCache {
	newM := mapCache{
		tables: make(map[int64]cacheItem),
	}
	newM.memQuota = m.memQuota
	for k, v := range m.tables {
		newM.tables[k] = v
	}
	return newM
}
