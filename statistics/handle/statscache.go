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
	"encoding/binary"
	"sync"

	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
)

// statsCache caches Regions loaded from PD.
type statsCache struct {
	mu          sync.Mutex
	cache       *kvcache.SimpleLRUCache
	memCapacity int64
	version     uint64
	memTracker  *memory.Tracker // track memory usage.
}
type statsCacheKey int64

func (key statsCacheKey) Hash() []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(key))
	return buf
}

// newstatsCache returns a new statsCahce with capacity maxMemoryLimit
func newstatsCache(memoryLimit int64) *statsCache {
	// since newstatsCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(uint(memoryLimit), 0.1, 0)
	c := statsCache{
		cache:       cache,
		memCapacity: memoryLimit,
		memTracker:  memory.NewTracker(memory.LabelForStatsCache, -1),
	}
	return &c
}

// Clear clears the statsCache.
func (sc *statsCache) Clear() {
	// since newstatsCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	sc.mu.Lock()
	cache := kvcache.NewSimpleLRUCache(uint(sc.memCapacity), 0.1, 0)
	sc.memTracker.ReplaceBytesUsed(0)
	sc.cache = cache
	sc.version = 0
	sc.mu.Unlock()
}

// GetAll get all the tables point.
func (sc *statsCache) GetAll() []*statistics.Table {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	values := sc.cache.GetAll()
	tables := make([]*statistics.Table, 0)
	for _, v := range values {
		if t, ok := v.(*statistics.Table); ok && t != nil {
			tables = append(tables, t)
		}
	}
	return tables
}

// lookupUnsafe get table with id without Lock.
func (sc *statsCache) lookupUnsafe(id int64) (*statistics.Table, bool) {
	var key = statsCacheKey(id)
	value, hit := sc.cache.Get(key)
	if !hit {
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
	if table == nil {
		return
	}
	var key = statsCacheKey(table.PhysicalID)
	mem := table.MemoryUsage()
	if mem > sc.memCapacity { // ignore this kv pair if its size is too large
		return
	}
	for mem+sc.memTracker.BytesConsumed() > sc.memCapacity {
		evictedKey, evictedValue, evicted := sc.cache.RemoveOldest()
		if !evicted {
			return
		}
		sc.memTracker.Consume(-evictedValue.(*statistics.Table).MemoryUsage())
		sc.cache.Put(evictedKey, evictedValue.(*statistics.Table).CopyMeta())
	}
	sc.Erase(table.PhysicalID)
	sc.memTracker.Consume(mem)
	sc.cache.Put(key, table)
	return
}

// Erase Erase a stateCache with physical id
func (sc *statsCache) Erase(deletedID int64) bool {
	table, hit := sc.lookupUnsafe(deletedID)
	if !hit {
		return false
	}
	sc.memTracker.Consume(-table.MemoryUsage())

	key := statsCacheKey(deletedID)
	sc.cache.Delete(key)
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
