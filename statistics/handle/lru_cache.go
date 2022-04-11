// Copyright 2022 PingCAP, Inc.
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

package handle

import (
	"container/list"
	"sync"

	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/statistics"
)

const (
	typHit    = "hit"
	typMiss   = "miss"
	typInsert = "insert"
	typUpdate = "update"
	typDel    = "del"
	typEvict  = "evict"
	typCopy   = "copy"
)

// cacheEntry wraps Key and Value. It's the value of list.Element.
type cacheItem struct {
	key   int64
	value *statistics.Table
	cost  int64
}

// internalLRUCache is a simple least recently used cache
type internalLRUCache struct {
	sync.RWMutex
	capacity int64
	cost     int64
	elements map[int64]*list.Element
	cache    *list.List
}

// newInternalLRUCache returns internalLRUCache
func newInternalLRUCache(capacity int64) *internalLRUCache {
	if capacity < 1 {
		panic("capacity of LRU Cache should be at least 1.")
	}
	return &internalLRUCache{
		capacity: capacity,
		cost:     0,
		elements: make(map[int64]*list.Element),
		cache:    list.New(),
	}
}

// Get tries to find the corresponding value according to the given key.
func (l *internalLRUCache) Get(key int64) (*statistics.Table, bool) {
	var hit bool
	var r *statistics.Table
	l.Lock()
	r, hit = l.get(key)
	l.Unlock()
	if hit {
		metrics.StatsCacheLRUCounter.WithLabelValues(typHit).Inc()
	} else {
		metrics.StatsCacheLRUCounter.WithLabelValues(typMiss).Inc()
	}
	return r, hit
}

func (l *internalLRUCache) get(key int64) (*statistics.Table, bool) {
	element, exists := l.elements[key]
	if !exists {
		return nil, false
	}
	l.cache.MoveToFront(element)
	return element.Value.(*cacheItem).value, true
}

// Put puts the (key, value) pair into the LRU Cache.
func (l *internalLRUCache) Put(key int64, value *statistics.Table) {
	var updated bool
	l.Lock()
	updated = l.put(key, value, value.MemoryUsage(), true)
	l.Unlock()
	if updated {
		metrics.StatsCacheLRUCounter.WithLabelValues(typUpdate).Inc()
		return
	}
	metrics.StatsCacheLRUCounter.WithLabelValues(typInsert).Inc()
}

func (l *internalLRUCache) put(key int64, value *statistics.Table, cost int64, tryEvict bool) bool {
	defer func() {
		if tryEvict {
			l.evictIfNeeded()
		}
	}()
	element, exists := l.elements[key]
	if exists {
		oldCost := element.Value.(*cacheItem).cost
		element.Value.(*cacheItem).value = value
		element.Value.(*cacheItem).cost = cost
		l.cache.MoveToFront(element)
		l.cost += cost - oldCost
		return true
	}
	newCacheEntry := &cacheItem{
		key:   key,
		value: value,
		cost:  cost,
	}
	element = l.cache.PushFront(newCacheEntry)
	l.elements[key] = element
	l.cost += cost
	return false
}

// Del deletes the key-value pair from the LRU Cache.
func (l *internalLRUCache) Del(key int64) {
	var del bool
	l.Lock()
	del = l.del(key)
	l.Unlock()
	if del {
		metrics.StatsCacheLRUCounter.WithLabelValues(typDel).Inc()
	}
}

func (l *internalLRUCache) del(key int64) bool {
	element := l.elements[key]
	if element == nil {
		return false
	}
	l.cache.Remove(element)
	delete(l.elements, key)
	l.cost -= element.Value.(*cacheItem).cost
	return true
}

// Cost returns the current cost
func (l *internalLRUCache) Cost() int64 {
	l.RLock()
	defer l.RUnlock()
	return l.cost
}

// Keys returns the current Keys
func (l *internalLRUCache) Keys() []int64 {
	l.RLock()
	defer l.RUnlock()
	r := make([]int64, 0)
	for _, v := range l.elements {
		r = append(r, v.Value.(*cacheItem).key)
	}
	return r
}

// Values returns the current Values
func (l *internalLRUCache) Values() []*statistics.Table {
	l.RLock()
	defer l.RUnlock()
	r := make([]*statistics.Table, 0)
	for _, v := range l.elements {
		r = append(r, v.Value.(*cacheItem).value)
	}
	return r
}

// Len returns the current length
func (l *internalLRUCache) Len() int {
	l.RLock()
	defer l.RUnlock()
	return len(l.elements)
}

// CalculateCost re-calculate the memory message
func (l *internalLRUCache) CalculateCost() {
	var cost int64
	l.Lock()
	for _, v := range l.elements {
		item := v.Value.(*cacheItem)
		oldCost := item.cost
		newCost := item.value.MemoryUsage()
		item.cost = newCost
		l.cost += newCost - oldCost
	}
	l.evictIfNeeded()
	cost = l.cost
	l.Unlock()
	metrics.StatsCacheMemUsage.WithLabelValues("cache").Set(float64(cost))
}

// CalculateTableCost re-calculate the memory message for the certain key
func (l *internalLRUCache) CalculateTableCost(key int64) {
	var cost int64
	var calculated bool
	l.Lock()
	calculated = l.calculateTableCost(key)
	cost = l.cost
	l.Unlock()
	if calculated {
		metrics.StatsCacheMemUsage.WithLabelValues("cache").Set(float64(cost))
	}
}

func (l *internalLRUCache) calculateTableCost(key int64) bool {
	element, exists := l.elements[key]
	if !exists {
		return false
	}
	item := element.Value.(*cacheItem)
	l.put(item.key, item.value, item.value.MemoryUsage(), true)
	l.evictIfNeeded()
	return true
}

// Copy returns a replication of LRU
func (l *internalLRUCache) Copy() kvCache {
	var newCache *internalLRUCache
	l.RLock()
	newCache = newInternalLRUCache(l.capacity)
	node := l.cache.Back()
	for node != nil {
		key := node.Value.(*cacheItem).key
		value := node.Value.(*cacheItem).value
		cost := node.Value.(*cacheItem).cost
		newCache.put(key, value, cost, false)
		node = node.Prev()
	}
	l.RUnlock()
	metrics.StatsCacheLRUCounter.WithLabelValues(typCopy).Inc()
	return newCache
}

func (l *internalLRUCache) evictIfNeeded() {
	for l.cost > l.capacity {
		lru := l.cache.Back()
		droppedItem := lru.Value.(*cacheItem)
		delete(l.elements, droppedItem.key)
		l.cache.Remove(lru)
		l.cost -= droppedItem.cost
		metrics.StatsCacheLRUCounter.WithLabelValues(typEvict).Inc()
	}
}
