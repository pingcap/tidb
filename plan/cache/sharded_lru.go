// Copyright 2017 PingCAP, Inc.
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

package cache

import (
	"sync"

	"github.com/pingcap/tidb/terror"
	"github.com/spaolacci/murmur3"
)

var (
	// EnablePlanCache stores the global config "enable-plan-cache".
	EnablePlanCache bool
	// PlanCacheShards stores the global config "plan-cache-shards".
	PlanCacheShards int64 = 200
	// PlanCacheCapacity stores the global config "plan-cache-capacity".
	PlanCacheCapacity int64 = 1000
	// GlobalPlanCache stores the global plan cache for every session in a tidb-server
	GlobalPlanCache *ShardedLRUCache
)

// ShardedLRUCache is a sharded LRU Cache, thread safe.
type ShardedLRUCache struct {
	shards []*SimpleLRUCache
	locks  []sync.RWMutex
}

// NewShardedLRUCache creates a ShardedLRUCache.
func NewShardedLRUCache(capacity, shardCount int64) *ShardedLRUCache {
	shardedLRUCache := &ShardedLRUCache{
		shards: make([]*SimpleLRUCache, 0, shardCount),
		locks:  make([]sync.RWMutex, shardCount),
	}
	for i := int64(0); i < shardCount; i++ {
		shardedLRUCache.shards = append(shardedLRUCache.shards, NewSimpleLRUCache(capacity/shardCount))
	}
	return shardedLRUCache
}

func (s *ShardedLRUCache) hash(key Key) int {
	hashFunc := murmur3.New32()

	// hash.Hash32.Write() should never returns an error
	if _, err := hashFunc.Write(key.Hash()); err != nil {
		terror.Log(err)
	}

	return int(hashFunc.Sum32() % uint32(len(s.shards)))
}

// Get gets a value from a ShardedLRUCache.
func (s *ShardedLRUCache) Get(key Key) (Value, bool) {
	id := s.hash(key)

	s.locks[id].Lock()
	value, ok := s.shards[id].Get(key)
	s.locks[id].Unlock()

	return value, ok
}

// Put puts a (key, value) pair to a ShardedLRUCache.
func (s *ShardedLRUCache) Put(key Key, value Value) {
	id := s.hash(key)

	s.locks[id].Lock()
	s.shards[id].Put(key, value)
	s.locks[id].Unlock()
}

// Clear clears a ShardedLRUCache.
func (s *ShardedLRUCache) Clear() {
	for i, length := 0, len(s.shards); i < length; i++ {
		s.locks[i].Lock()
		s.shards[i].Clear()
		s.locks[i].Unlock()
	}
}
