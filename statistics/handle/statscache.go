// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package handle

import (
	"sync"

	"github.com/pingcap/badger/cache"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

// statsCache caches Regions loaded from PD.
type statsCache struct {
	mu          sync.Mutex
	cache       *cache.Cache
	memCapacity int64
	version     uint64
	memTracker  *memory.Tracker // track memory usage.
}

// newstatsCache returns a new statsCahce with capacity maxMemoryLimit(initial 1G)
func newstatsCache(maxMemoryLimit int64) *statsCache {
	// since newstatsCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	c := &statsCache{
		memCapacity: maxMemoryLimit,
		memTracker:  memory.NewTracker(stringutil.StringerStr("statsCache"), -1),
	}
	cache, err := cache.NewCache(&cache.Config{
		NumCounters: 1e7,            // number of keys to track frequency of (10M).
		MaxCost:     maxMemoryLimit, // maximum cost of cache (1GB).
		BufferItems: 64,             // number of keys per Get buffer.
		Cost: func(value interface{}) int64 {
			return (value.(*statistics.Table).MemoryUsage())
		},
		OnEvict: func(key uint64, value interface{}) {
			if t, ok := value.(*statistics.Table); ok {
				c.memTracker.Consume(-t.MemoryUsage())
				if value != nil && t.PhysicalID == int64(key) {
					c.cache.Set(key, t.CopyMeta(), 0)
				}
			}
		},
		OnInsert: func(key uint64, cost int64) {
			c.memTracker.Consume(cost)
		},
	})
	if err != nil {

	}
	c.cache = cache
	return c
}

// lookupUnsafe get table with id without Lock.
func (sc *statsCache) lookupUnsafe(id int64) (*statistics.Table, bool) {
	key := uint64(id)
	value, hit := sc.cache.Get(key)
	if !hit || value.(*statistics.Table).PhysicalID != id {
		return nil, false
	}
	table := value.(*statistics.Table)
	return table, true
}

// Lookup get table with id.
func (sc *statsCache) Lookup(id int64) (*statistics.Table, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.lookupUnsafe(id)
}

// Insert insert a new table to tables and update the cache.
// if bytesconsumed is more than capacity, remove oldest cache and add metadata of it
func (sc *statsCache) Insert(table *statistics.Table) {

	key := table.PhysicalID
	mem := table.MemoryUsage()
	sc.cache.Set(uint64(key), table, mem)
	return
}

// Erase Erase a stateCache with physical id
func (sc *statsCache) Erase(deletedID int64) bool {
	table, hit := sc.lookupUnsafe(deletedID)
	if !hit {
		return false
	}
	sc.memTracker.Consume(-table.MemoryUsage())

	key := deletedID
	sc.cache.Del(uint64(key))
	return true
}

// Update updates the statistics table cache.
func (sc *statsCache) Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) {
	sc.mu.Lock()
	if sc.version <= newVersion {
		sc.version = newVersion
		for _, id := range deletedIDs {
			sc.Erase(id)
		}
		for _, tbl := range tables {
			sc.Insert(tbl)
		}
	}
	sc.mu.Unlock()
}

func (sc *statsCache) GetVersion() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.version
}

// initStatsCache should be called after the tables and their stats are initilazed
// using tables map and version to init statscache
func (sc *statsCache) initStatsCache(tables map[int64]*statistics.Table, version uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, tbl := range tables {
		sc.Insert(tbl)
	}
	sc.version = version
	return
}
