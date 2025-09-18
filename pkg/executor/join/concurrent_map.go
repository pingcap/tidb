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

package join

import (
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/syncutil"
)

// ShardCount controls the shard maps within the concurrent map
const ShardCount = 320

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (ShardCount) map shards.
type concurrentMap []*concurrentMapShared

// A "thread" safe string to anything map.
type concurrentMapShared struct {
	items            hack.MemAwareMap[uint64, *entry]
	syncutil.RWMutex // Read Write mutex, guards access to internal map.
}

// newConcurrentMap creates a new concurrent map.
func newConcurrentMap() concurrentMap {
	m := make(concurrentMap, ShardCount)
	for i := range ShardCount {
		m[i] = &concurrentMapShared{}
		m[i].items.Init(make(map[uint64]*entry))
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
	oldValue := shard.items.M[key]
	value.Next = oldValue
	memDelta += shard.items.Set(key, value)
	shard.Unlock()
	return memDelta
}

// Get retrieves an element from map under given key.
// Note that in hash joins, reading proceeds after all writes, so we ignore RLock() here.
// Otherwise, we should use RLock() for concurrent reads and writes.
func (m concurrentMap) Get(key uint64) (*entry, bool) {
	// Get shard
	shard := m.getShard(key)
	// shard.RLock()
	// Get item from shard.
	val, ok := shard.items.M[key]
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
		for key, value := range shard.items.M {
			fn(key, value)
		}
		shard.RUnlock()
	}
}
