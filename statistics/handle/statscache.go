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
	"time"

	"github.com/pingcap/badger/cache"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
)

// StatsCache a interface can LookUp Update
type StatsCache interface {
	Lookup(id int64) (*statistics.Table, bool)
	Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64)
	GetVersion() uint64
	InitStatsCache(tables map[int64]*statistics.Table, version uint64)
	GetAll() []*statistics.Table

	// interface below are used only for test
	Clear()
	Close()
	GetBytesLimit() int64
	SetBytesLimit(bytesLimit int64)
	BytesConsumed() int64
}

const byteShards = 8
const numShards = 1 << byteShards

// statsCache caches Regions loaded from PD.
type ristrettoStatsCache struct {
	mu         sync.Mutex
	cache      *cache.Cache
	version    uint64
	memTracker *memory.Tracker // track memory usage.
	tablesMap
}

type tablesMap struct {
	tablesShards [numShards]tableShard
}

// tableShard is a shard of table
type tableShard struct {
	data map[int64]*statistics.Table
	sync.RWMutex
}

// Set set key with value
func (ts *tablesMap) Set(key int64, value *statistics.Table) {
	shard := &ts.tablesShards[key&(numShards-1)]
	shard.Lock()
	defer shard.Unlock()
	shard.data[key>>byteShards] = value
}

// Get get key with value table
func (ts *tablesMap) Get(key int64) (*statistics.Table, bool) {
	shard := &ts.tablesShards[key&(numShards-1)]
	shard.RLock()
	defer shard.RUnlock()
	data, ok := shard.data[key>>byteShards]
	return data, ok
}

// Del delete key
func (ts *tablesMap) Del(key int64) {
	shard := &ts.tablesShards[key&(numShards-1)]
	shard.Lock()
	defer shard.Unlock()
	delete(shard.data, key>>byteShards)
}

type statsCacheType int8

const (
	//RistrettoStatsCacheType type
	RistrettoStatsCacheType statsCacheType = iota
	//SimpleStatsCacheType simple type
	SimpleStatsCacheType
)

// DefStatsCacheType defines statistic cache type
var DefStatsCacheType = RistrettoStatsCacheType

// NewStatsCache returns a new statsCahce with capacity memoryLimit(initial 1G)
func NewStatsCache(memoryLimit int64, tp statsCacheType) (StatsCache, error) {
	switch tp {
	case RistrettoStatsCacheType:
		return newRistrettoStatsCache(memoryLimit)
	case SimpleStatsCacheType:
		return newSimpleStatsCache(memoryLimit), nil
	}
	return nil, errors.New("wrong statsCache type")
}

// newRistrettoStatsCache returns a new statsCahce with capacity memoryLimit(initial 1G)
func newRistrettoStatsCache(memoryLimit int64) (*ristrettoStatsCache, error) {
	// since newRistrettoStatsCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	sc := &ristrettoStatsCache{
		memTracker: memory.NewTracker(memory.LabelForStatsCache, -1),
	}
	for i := range sc.tablesShards {
		sc.tablesShards[i].data = make(map[int64]*statistics.Table)
	}
	sc.version = 0
	cache, err := cache.NewCache(&cache.Config{
		NumCounters: 1e7,         // number of keys to track frequency of (10M).
		MaxCost:     memoryLimit, // maximum cost of cache (1GB).
		BufferItems: 64,          // number of keys per Get buffer.
		Cost: func(value interface{}) int64 {
			return (value.(*statistics.Table).MemUsage)
		},
		OnEvict: func(key uint64, value interface{}) {
			if t, ok := sc.Get(int64(key)); ok {
				if value != nil {
					sc.memTracker.Consume(-t.MemUsage)
					sc.Set(int64(key), t.CopyWithoutBucketsAndCMS())
				}
			}
		},
	})
	if err != nil {
		return nil, err
	}

	sc.cache = cache
	return sc, nil
}

// BytesConsumed returns the consumed memory usage value in bytes.
// only used by test.
func (sc *ristrettoStatsCache) BytesConsumed() int64 {
	rndTbl := sc.GetAll()[0]
	sc.Erase(rndTbl.PhysicalID)
	// insert a table memory = 0 to make cache renew the memory
	for i := 0; i < 10; i++ {
		sc.Insert(rndTbl)
		time.Sleep(10 * time.Millisecond)
	}
	return sc.memTracker.BytesConsumed()
}

// GetBytesLimit get the limits of memory.
func (sc *ristrettoStatsCache) GetBytesLimit() int64 {
	return sc.memTracker.GetBytesLimit()
}

// SetBytesLimit set new byteslimit
func (sc *ristrettoStatsCache) SetBytesLimit(bytesLimit int64) {
	sc.cache.SetNewMaxCost(bytesLimit)
}

// Close close the cache.
func (sc *ristrettoStatsCache) Close() {
	sc.cache.Clear()
	sc.cache.Close()
}

// Clear clears the cache data.
func (sc *ristrettoStatsCache) Clear() {
	sc.cache.Clear()
	sc.mu.Lock()
	sc.version = 0
	sc.mu.Unlock()
	for i := range sc.tablesShards {
		sc.tablesShards[i].Lock()
		sc.tablesShards[i].data = make(map[int64]*statistics.Table)
		sc.tablesShards[i].Unlock()
	}
	sc.memTracker = memory.NewTracker(memory.LabelForStatsCache, -1)

}

// GetAll get all the tables.
func (sc *ristrettoStatsCache) GetAll() []*statistics.Table {
	tables := make([]*statistics.Table, 0)
	for i := range sc.tablesShards {
		shard := &sc.tablesShards[i]
		for _, tbl := range shard.data {
			ntbl, _ := sc.Lookup(tbl.PhysicalID)
			tables = append(tables, ntbl)
		}
	}
	return tables
}

// lookupUnsafe get table with id without Lock.
func (sc *ristrettoStatsCache) lookupUnsafe(id int64) (*statistics.Table, bool) {
	key := uint64(id)
	value, hit := sc.cache.Get(key)
	if !hit || value == nil {
		if table, ok := sc.Get(id); ok {
			return table, true
		}
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
func (sc *ristrettoStatsCache) Insert(table *statistics.Table) {
	key := table.PhysicalID
	// Calc memusage only on insert.
	if oldTbl, ok := sc.Get(key); ok {
		sc.memTracker.Consume(-oldTbl.MemUsage)
	}
	mem := table.MemoryUsage()
	table.MemUsage = mem
	sc.cache.Set(uint64(key), table, mem)
	sc.memTracker.Consume(mem)
	sc.Set(key, table)
	return
}

// Erase Erase a stateCache with physical id
func (sc *ristrettoStatsCache) Erase(deletedID int64) bool {
	key := deletedID
	sc.cache.Del(uint64(key))
	if oldTbl, ok := sc.Get(key); ok {
		sc.memTracker.Consume(-oldTbl.MemUsage)
	}
	sc.Del(deletedID)
	return true
}

// Update updates the statistics table cache.
func (sc *ristrettoStatsCache) Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) {
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

func (sc *ristrettoStatsCache) GetVersion() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.version
}

// InitStatsCache should be called after the tables and their stats are initilazed
// using tables map and version to init statscache
func (sc *ristrettoStatsCache) InitStatsCache(tables map[int64]*statistics.Table, version uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, tbl := range tables {
		sc.Insert(tbl)
	}
	sc.version = version
	return
}

// simpleStatsCache caches Regions loaded from PD.
type simpleStatsCache struct {
	mu          sync.Mutex
	cache       *kvcache.SimpleLRUCache
	memCapacity int64
	version     uint64
	memTracker  *memory.Tracker
}

type statsCacheKey int64

func (key statsCacheKey) Hash() []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(key))
	return buf
}

// newstatsCache returns a new statsCahce with capacity memoryLimit(initial 1G)
func newSimpleStatsCache(memoryLimit int64) *simpleStatsCache {
	// since newstatsCache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(uint(memoryLimit), 0.1, 0)
	c := simpleStatsCache{
		cache:       cache,
		memCapacity: memoryLimit,
		memTracker:  memory.NewTracker(memory.LabelForStatsCache, memoryLimit),
	}
	return &c
}

// SetBytesLimit sets the bytes limit for this tracker.
func (sc *simpleStatsCache) SetBytesLimit(BytesLimit int64) {
	sc.memTracker.SetBytesLimit(BytesLimit)
}

// BytesConsumed returns the consumed memory usage value in bytes.
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

func (sc *simpleStatsCache) Close() {
	sc.Clear()
}

// Clear clears the cache
func (sc *simpleStatsCache) Clear() {
	sc.mu.Lock()
	sc.cache.DeleteAll()
	sc.memTracker = memory.NewTracker(memory.LabelForStatsCache, 1024*1024*1024)
	sc.mu.Unlock()
}

// Lookup get table with id.
func (sc *simpleStatsCache) Lookup(id int64) (*statistics.Table, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.lookupUnsafe(id)
}

// Insert inserts a new table to the statsCache.
// If the memory consumption exceeds the capacity, remove the buckets and
// CMSketch of the oldest cache and add metadata of it
func (sc *simpleStatsCache) Insert(table *statistics.Table) {
	if table == nil {
		return
	}
	var key = statsCacheKey(table.PhysicalID)
	mem := table.MemoryUsage()
	// We do not need to check whether mem > sc.memCapacity, because the lower
	// bound of statistics is set, it's almost impossible the stats memory usage
	// of one table exceeds the capacity.
	for mem+sc.memTracker.BytesConsumed() > sc.memCapacity {
		evictedKey, evictedValue, evicted := sc.cache.RemoveOldest()
		if !evicted {
			return
		}
		sc.memTracker.Consume(-evictedValue.(*statistics.Table).MemoryUsage())
		sc.cache.Put(evictedKey, evictedValue.(*statistics.Table).CopyWithoutBucketsAndCMS())
	}
	// erase the old element since the value may be different from the existing one.
	sc.Erase(table.PhysicalID)
	sc.cache.Put(key, table)
	sc.memTracker.Consume(mem)
	return
}

// Erase Erase a stateCache with physical id
func (sc *simpleStatsCache) Erase(deletedID int64) bool {
	table, hit := sc.lookupUnsafe(deletedID)
	if !hit {
		return false
	}

	key := statsCacheKey(deletedID)
	sc.cache.Delete(key)
	sc.memTracker.Consume(-table.MemoryUsage())
	return true
}

// GetAll get all the tables point.
func (sc *simpleStatsCache) GetAll() []*statistics.Table {
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

// Update updates the statistics table cache.
func (sc *simpleStatsCache) Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) {
	sc.mu.Lock()
	if sc.version <= newVersion {
		sc.version = newVersion
		sc.mu.Unlock()
		for _, id := range deletedIDs {
			sc.Erase(id)
		}
		for _, tbl := range tables {
			sc.Insert(tbl)
		}
	} else {
		sc.mu.Unlock()
	}
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

// InitStatsCache should be called after the tables and their stats are initilazed
// using tables map and version to init statscache
func (sc *simpleStatsCache) InitStatsCache(tables map[int64]*statistics.Table, version uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, tbl := range tables {
		sc.Insert(tbl)
	}
	sc.version = version
	return
}
