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
	"errors"
	"sync"

	"github.com/pingcap/badger/cache"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/uber-go/atomic"
)

type StatsCache interface {
	Lookup(id int64) (*statistics.Table, bool)
	Insert(table *statistics.Table)
	Erase(deletedID int64) bool
	Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64)
	GetVersion() uint64
	initStatsCache(tables map[int64]*statistics.Table, version uint64)
	GetMutex() *sync.Mutex
	BytesConsumed() int64
	GetBytesLimit() int64
}

// statsCache caches Regions loaded from PD.
type ristrettoStatsCache struct {
	mu          sync.Mutex
	cache       *cache.Cache
	memCapacity int64
	version     atomic.Value
	memTracker  *memory.Tracker // track memory usage.
}

type statsCacheType int8

const (
	//RistrettoStatsCacheType type
	RistrettoStatsCacheType statsCacheType = iota
	//SimpleStatsCacheType simple type
	SimpleStatsCacheType
)

// NewStatsCache returns a new statsCahce with capacity maxMemoryLimit(initial 1G)
func NewStatsCache(maxMemoryLimit int64, tp statsCacheType) (StatsCache, error) {
	if tp == RistrettoStatsCacheType {
		return newRistrettoStatsCache(maxMemoryLimit), nil
	}
	if tp == SimpleStatsCacheType {
		return newSimpleStatsCache(maxMemoryLimit), nil
	}
	return nil, errors.New("wrong statsCache type")
}

// newRistrettoStatsCache returns a new statsCahce with capacity maxMemoryLimit(initial 1G)
func newRistrettoStatsCache(maxMemoryLimit int64) *ristrettoStatsCache {
	// since newRistrettoStatsCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	c := &ristrettoStatsCache{
		memCapacity: maxMemoryLimit,
		memTracker:  memory.NewTracker(stringutil.StringerStr("statsCache"), -1),
	}
	c.version.Store(uint64(0))
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

//BytesConsumed returns the consumed memory usage value in bytes.
func (sc *ristrettoStatsCache) BytesConsumed() int64 {
	return sc.memTracker.BytesConsumed()
}

// GetBytesLimit get the limits of memory.
func (sc *ristrettoStatsCache) GetBytesLimit() int64 {
	return sc.memTracker.GetBytesLimit()
}

//GetMutex return the Muetex point
func (sc *ristrettoStatsCache) GetMutex() *sync.Mutex {
	return &sc.mu
}

// lookupUnsafe get table with id without Lock.
func (sc *ristrettoStatsCache) lookupUnsafe(id int64) (*statistics.Table, bool) {
	key := uint64(id)
	value, hit := sc.cache.Get(key)
	if !hit || value == nil || value.(*statistics.Table).PhysicalID != id {
		return nil, false
	}
	table := value.(*statistics.Table)
	return table, true
}

// Lookup get table with id.
func (sc *ristrettoStatsCache) Lookup(id int64) (*statistics.Table, bool) {
	return sc.lookupUnsafe(id)
}

// Insert insert a new table to tables and update the cache.
// if bytesconsumed is more than capacity, remove oldest cache and add metadata of it
func (sc *ristrettoStatsCache) Insert(table *statistics.Table) {

	key := table.PhysicalID
	mem := table.MemoryUsage()
	sc.cache.Set(uint64(key), table, mem)
	return
}

// Erase Erase a stateCache with physical id
func (sc *ristrettoStatsCache) Erase(deletedID int64) bool {
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
func (sc *ristrettoStatsCache) Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) {
	if sc.version.Load().(uint64) <= newVersion {
		sc.version.Store(newVersion)
		for _, id := range deletedIDs {
			sc.Erase(id)
		}
		for _, tbl := range tables {
			sc.Insert(tbl)
		}
	}
}

func (sc *ristrettoStatsCache) GetVersion() uint64 {
	return sc.version.Load().(uint64)
}

// initStatsCache should be called after the tables and their stats are initilazed
// using tables map and version to init statscache
func (sc *ristrettoStatsCache) initStatsCache(tables map[int64]*statistics.Table, version uint64) {
	for _, tbl := range tables {
		sc.Insert(tbl)
	}
	sc.version.Store(version)
	return
}

// simpleStatsCache caches Regions loaded from PD.
type simpleStatsCache struct {
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

// newstatsCache returns a new statsCahce with capacity maxMemoryLimit(initial 1G)
func newSimpleStatsCache(maxMemoryLimit int64) *simpleStatsCache {
	// since newstatsCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(uint(maxMemoryLimit), 0.1, 0)
	c := simpleStatsCache{
		cache:       cache,
		memCapacity: maxMemoryLimit,
		memTracker:  memory.NewTracker(stringutil.StringerStr("statsCache"), -1),
	}
	return &c
}

//GetMutex return the Muetex point
func (sc *simpleStatsCache) GetMutex() *sync.Mutex {
	return &sc.mu
}

//BytesConsumed returns the consumed memory usage value in bytes.
func (sc *simpleStatsCache) BytesConsumed() int64 {
	return sc.memTracker.BytesConsumed()
}

// lookupUnsafe get table with id without Lock.
func (sc *simpleStatsCache) lookupUnsafe(id int64) (*statistics.Table, bool) {
	var key = statsCacheKey(id)
	value, hit := sc.cache.Get(key)
	if !hit {
		return nil, false
	}
	table := value.(*statistics.Table)
	return table, true
}

// Lookup get table with id.
func (sc *simpleStatsCache) Lookup(id int64) (*statistics.Table, bool) {
	return sc.lookupUnsafe(id)
}

// Insert insert a new table to tables and update the cache.
// if bytesconsumed is more than capacity, remove oldest cache and add metadata of it
func (sc *simpleStatsCache) Insert(table *statistics.Table) {
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
func (sc *simpleStatsCache) Erase(deletedID int64) bool {
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
func (sc *simpleStatsCache) Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) {
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

// GetBytesLimit get the limits of memory.
func (sc *simpleStatsCache) GetBytesLimit() int64 {
	return sc.memTracker.GetBytesLimit()
}
func (sc *simpleStatsCache) GetVersion() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.version
}

// initStatsCache should be called after the tables and their stats are initilazed
// using tables map and version to init statscache
func (sc *simpleStatsCache) initStatsCache(tables map[int64]*statistics.Table, version uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, tbl := range tables {
		sc.Insert(tbl)
	}
	sc.version = version
	return
}
