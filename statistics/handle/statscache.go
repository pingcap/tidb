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

	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
)

// StatsCache is an interface for the collection of statistics.
type StatsCache interface {
	Lookup(id int64) (*statistics.Table, bool)
	Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64)
	GetVersion() uint64
	InitStatsCache(tables map[int64]*statistics.Table, version uint64)
	GetAll() []*statistics.Table

	// Interface below are used only for test.
	Clear()
	GetBytesLimit() int64
	SetBytesLimit(bytesLimit int64)
	BytesConsumed() int64
}

type statsCacheType int8

const (
	simpleLRUCache statsCacheType = iota
)

var defaultStatsCacheType = simpleLRUCache

// newStatsCacheWithMemCap returns a new stats cache with memory capacity.
func newStatsCacheWithMemCap(memoryCapacity int64, tp statsCacheType) (StatsCache, error) {
	switch tp {
	case simpleLRUCache:
		return newSimpleLRUStatsCache(memoryCapacity), nil
	}
	return nil, errors.New("wrong statsCache type")
}

// simpleLRUStatsCache uses the simpleLRUCache to store the cache of statistics.
type simpleLRUStatsCache struct {
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

func newSimpleLRUStatsCache(memoryCapacity int64) *simpleLRUStatsCache {
	// since stats cache controls the memory usage by itself, set the capacity of
	// the underlying LRUCache to max to close its memory control
	cache := kvcache.NewSimpleLRUCache(uint(memoryCapacity), 0.1, 0)
	c := simpleLRUStatsCache{
		cache:       cache,
		memCapacity: memoryCapacity,
		memTracker:  memory.NewTracker(memory.LabelForStatsCache, memoryCapacity),
	}
	return &c
}

// SetBytesLimit sets the bytes limit for this tracker.
func (sc *simpleLRUStatsCache) SetBytesLimit(BytesLimit int64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.memTracker.SetBytesLimit(BytesLimit)
	sc.memCapacity = BytesLimit
}

// BytesConsumed returns the consumed memory usage value in bytes.
func (sc *simpleLRUStatsCache) BytesConsumed() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.memTracker.BytesConsumed()
}

// lookupUnsafe get table with id without Lock.
func (sc *simpleLRUStatsCache) lookupUnsafe(id int64) (*statistics.Table, bool) {
	var key = statsCacheKey(id)
	value, hit := sc.cache.Get(key)
	if !hit {
		return nil, false
	}
	table := value.(*statistics.Table)
	return table, true
}

// Clear clears the cache
func (sc *simpleLRUStatsCache) Clear() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.version = 0
	sc.cache.DeleteAll()
	sc.memTracker = memory.NewTracker(memory.LabelForStatsCache, sc.memCapacity)
}

// Lookup get table with id.
func (sc *simpleLRUStatsCache) Lookup(id int64) (*statistics.Table, bool) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.lookupUnsafe(id)
}

// Insert inserts a new table to the statsCache.
// If the memory consumption exceeds the capacity, remove the buckets and
// CMSketch of the oldest cache and add metadata of it
func (sc *simpleLRUStatsCache) Insert(table *statistics.Table) {
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

// Erase removes a stateCache with physical id.
func (sc *simpleLRUStatsCache) Erase(deletedID int64) bool {
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
func (sc *simpleLRUStatsCache) GetAll() []*statistics.Table {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	values := sc.cache.GetAll()
	tables := make([]*statistics.Table, 0, len(values))
	for _, v := range values {
		if t, ok := v.(*statistics.Table); ok && t != nil {
			tables = append(tables, t)
		}
	}
	return tables
}

// Update updates the statistics table cache.
func (sc *simpleLRUStatsCache) Update(tables []*statistics.Table, deletedIDs []int64, newVersion uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.version <= newVersion {
		sc.version = newVersion
		for _, id := range deletedIDs {
			sc.Erase(id)
		}
		for _, tbl := range tables {
			sc.Insert(tbl)
		}
	}
}

// GetBytesLimit get the limits of memory.
func (sc *simpleLRUStatsCache) GetBytesLimit() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.memTracker.GetBytesLimit()
}

func (sc *simpleLRUStatsCache) GetVersion() uint64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.version
}

// InitStatsCache should be called after the tables and their stats are initilazed
// using tables map and version to init statscache
func (sc *simpleLRUStatsCache) InitStatsCache(tables map[int64]*statistics.Table, version uint64) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, tbl := range tables {
		sc.Insert(tbl)
	}
	sc.version = version
	return
}
