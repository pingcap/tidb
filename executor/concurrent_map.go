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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"sync"

	"github.com/pingcap/tidb/util/hack"
)

// ShardCount controls the shard maps within the concurrent map
var ShardCount = 320

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (ShardCount) map shards.
type concurrentMap []*concurrentMapShared

// A "thread" safe string to anything map.
type concurrentMapShared struct {
	items        map[uint64]*entry
	sync.RWMutex       // Read Write mutex, guards access to internal map.
	bInMap       int64 // indicate there are 2^bInMap buckets in items
}

// newConcurrentMap creates a new concurrent map.
func newConcurrentMap() concurrentMap {
	m := make(concurrentMap, ShardCount)
	for i := 0; i < ShardCount; i++ {
		m[i] = &concurrentMapShared{items: make(map[uint64]*entry), bInMap: 0}
	}
	return m
}

// getShard returns shard under given key
func (m concurrentMap) getShard(hashKey uint64) *concurrentMapShared {
	return m[hashKey%uint64(ShardCount)]
}

// Insert inserts a value in a shard safely
func (m concurrentMap) Insert(key uint64, value *entry) (memDelta int64) {
	shard := m.getShard(key)
	shard.Lock()
	oldValue := shard.items[key]
	value.next = oldValue
	shard.items[key] = value
	if len(shard.items) > (1<<shard.bInMap)*hack.LoadFactorNum/hack.LoadFactorDen {
		memDelta = hack.DefBucketMemoryUsageForMapIntToPtr * (1 << shard.bInMap)
		shard.bInMap++
	}
	shard.Unlock()
	return memDelta
}

// UpsertCb : Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb func(exist bool, valueInMap, newValue *entry) *entry

// Upsert: Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m concurrentMap) Upsert(key uint64, value *entry, cb UpsertCb) (res *entry) {
	shard := m.getShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// Get retrieves an element from map under given key.
// Note that in hash joins, reading proceeds after all writes, so we ignore RLock() here.
// Otherwise, we should use RLock() for concurrent reads and writes.
func (m concurrentMap) Get(key uint64) (*entry, bool) {
	// Get shard
	shard := m.getShard(key)
	// shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	// shard.RUnlock()
	return val, ok
}

// IterCb :Iterator callback,called for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb func(key uint64, e *entry)

// IterCb iterates the map using a callback, cheapest way to read
// all elements in a map.
func (m concurrentMap) IterCb(fn IterCb) {
	for idx := range m {
		shard := (m)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}
