package executor

import (
	"sync"
)

var SHARD_COUNT = 320

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map shards.
type concurrentMap []*concurrentMapShared

// A "thread" safe string to anything map.
type concurrentMapShared struct {
	items        map[uint64]*entry
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

// Creates a new concurrent map.
func NewConcMap() concurrentMap {
	m := make(concurrentMap, SHARD_COUNT)
	for i := 0; i < SHARD_COUNT; i++ {
		m[i] = &concurrentMapShared{items: make(map[uint64]*entry)}
	}
	return m
}

// GetShard returns shard under given key
func (m concurrentMap) GetShard(hashKey uint64) *concurrentMapShared {
	return m[hashKey%uint64(SHARD_COUNT)]
}

// Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb func(exist bool, valueInMap, newValue *entry) *entry

// Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m concurrentMap) Upsert(key uint64, value *entry, cb UpsertCb) (res *entry) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// Get retrieves an element from map under given key.
func (m concurrentMap) Get(key uint64) (*entry, bool) {
	// Get shard
	shard := m.GetShard(key)
	//	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	//	shard.RUnlock()
	return val, ok
}

// Iterator callback,called for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb func(key uint64, e *entry)

// Callback based iterator, cheapest way to read
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
