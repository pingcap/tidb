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
	"hash"
	"sync"

	"github.com/pingcap/tidb/terror"
	"github.com/spaolacci/murmur3"
)

type ShardedLRUCache struct {
	shards   []*SimpleLRUCache
	locks    []sync.RWMutex
	hashFunc hash.Hash32
}

func NewShardedLRUCache(shardCount, capacity int64) *ShardedLRUCache {
	shardedLRUCache := &ShardedLRUCache{
		shards:   make([]*SimpleLRUCache, 0, shardCount),
		locks:    make([]sync.RWMutex, shardCount),
		hashFunc: murmur3.New32(),
	}
	for i := int64(0); i < shardCount; i++ {
		shardedLRUCache.shards = append(shardedLRUCache.shards, NewSimpleLRUCache(capacity/shardCount))
	}
	return shardedLRUCache
}

func (s *ShardedLRUCache) Get(key Key) (Value, bool) {
	s.hashFunc.Reset()

	// hash.Hash32.Write() should never returns an error
	if _, err := s.hashFunc.Write(key.Hash()); err != nil {
		terror.Log(err)
	}
	id := s.hashFunc.Sum32() % uint32(len(s.shards))

	s.locks[id].Lock()
	value, ok := s.shards[id].Get(key)
	s.locks[id].Unlock()

	return value, ok
}

func (s *ShardedLRUCache) Put(key Key, value Value) {
	s.hashFunc.Reset()

	// hash.Hash32.Write() should never returns an error
	if _, err := s.hashFunc.Write(key.Hash()); err != nil {
		terror.Log(err)
	}
	id := s.hashFunc.Sum32() % uint32(len(s.shards))

	s.locks[id].Lock()
	s.shards[id].Put(key, value)
	s.locks[id].Unlock()
}

func (s *ShardedLRUCache) Clear() {
	for i, length := 0, len(s.shards); i < length; i++ {
		s.locks[i].Lock()
		s.shards[i].Clear()
		s.locks[i].Unlock()
	}
}
