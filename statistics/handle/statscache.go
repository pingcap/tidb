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
	"sync"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"golang.org/x/exp/maps"
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
	Copy() statsCacheInner
	SetCapacity(int64)
	EnableQuota() bool
}

func newStatsCache() statsCache {
	enableQuota := config.GetGlobalConfig().Performance.EnableStatsCacheMemQuota
	if enableQuota {
		capacity := variable.StatsCacheMemQuota.Load()
		return statsCache{
			statsCacheInner: newStatsLruCache(capacity),
		}
	}
	return statsCache{
		statsCacheInner: &mapCache{
			tables:   make(map[int64]cacheItem),
			memUsage: 0,
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

type cacheItem struct {
	key   int64
	value *statistics.Table
	cost  int64
}

type mapCache struct {
	sync.RWMutex
	tables   map[int64]cacheItem
	memUsage int64
}

// GetByQuery implements statsCacheInner
func (m *mapCache) GetByQuery(k int64) (*statistics.Table, bool) {
	return m.Get(k)
}

// Get implements statsCacheInner
func (m *mapCache) Get(k int64) (*statistics.Table, bool) {
	m.RLock()
	defer m.RUnlock()
	v, ok := m.tables[k]
	return v.value, ok
}

// PutByQuery implements statsCacheInner
func (m *mapCache) PutByQuery(k int64, v *statistics.Table) {
	m.Put(k, v)
}

// Put implements statsCacheInner
func (m *mapCache) Put(k int64, v *statistics.Table) {
	m.Lock()
	defer m.Unlock()
	item, ok := m.tables[k]
	if ok {
		oldCost := item.cost
		newCost := v.MemoryUsage().TotalMemUsage
		item.value = v
		item.cost = newCost
		m.tables[k] = item
		m.memUsage += newCost - oldCost
		return
	}
	cost := v.MemoryUsage().TotalMemUsage
	item = cacheItem{
		key:   k,
		value: v,
		cost:  cost,
	}
	m.tables[k] = item
	m.memUsage += cost
}

// Del implements statsCacheInner
func (m *mapCache) Del(k int64) {
	m.Lock()
	defer m.Unlock()
	item, ok := m.tables[k]
	if !ok {
		return
	}
	delete(m.tables, k)
	m.memUsage -= item.cost
}

// Cost implements statsCacheInner
func (m *mapCache) Cost() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.memUsage
}

// Keys implements statsCacheInner
func (m *mapCache) Keys() []int64 {
	m.RLock()
	defer m.RUnlock()
	ks := make([]int64, 0, m.Len())
	for k := range m.tables {
		ks = append(ks, k)
	}
	return ks
}

// Values implements statsCacheInner
func (m *mapCache) Values() []*statistics.Table {
	m.RLock()
	defer m.RUnlock()
	vs := make([]*statistics.Table, 0, m.Len())
	for _, v := range m.tables {
		vs = append(vs, v.value)
	}
	return vs
}

// Map implements statsCacheInner
func (m *mapCache) Map() map[int64]*statistics.Table {
	m.RLock()
	defer m.RUnlock()
	t := make(map[int64]*statistics.Table, m.Len())
	for k, v := range m.tables {
		t[k] = v.value
	}
	return t
}

// Len implements statsCacheInner
func (m *mapCache) Len() int {
	m.RLock()
	defer m.RUnlock()
	return len(m.tables)
}

// FreshMemUsage implements statsCacheInner
func (m *mapCache) FreshMemUsage() {
	m.Lock()
	defer m.Unlock()
	for _, v := range m.tables {
		oldCost := v.cost
		newCost := v.value.MemoryUsage().TotalMemUsage
		m.memUsage += newCost - oldCost
	}
}

// Copy implements statsCacheInner
func (m *mapCache) Copy() statsCacheInner {
	m.RLock()
	defer m.RUnlock()
	newM := &mapCache{
		tables:   make(map[int64]cacheItem, m.Len()),
		memUsage: m.memUsage,
	}
	maps.Copy(newM.tables, m.tables)
	return newM
}

// SetCapacity implements statsCacheInner
func (m *mapCache) SetCapacity(int64) {}

// EnableQuota implements statsCacheInner
func (m *mapCache) EnableQuota() bool {
	return false
}
