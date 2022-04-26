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
	typUpdate = "update"
	typDel    = "del"
	typEvict  = "evict"
	typCopy   = "copy"
	typTrack  = "track"
	typTotal  = "total"
)

// cacheItem wraps Key and Value. It's the value of list.Element.
type cacheItem struct {
	key         int64
	value       *statistics.Table
	tblMemUsage *statistics.TableMemoryUsage
}

// internalLRUCache is a simple least recently used cache
type internalLRUCache struct {
	sync.RWMutex
	capacity int64
	// trackingCost records the tracking memory usage of the elements stored in the internalLRUCache
	// trackingCost should be kept under capacity by evict policy
	trackingCost int64
	// totalCost records the total memory usage of the elements stored in the internalLRUCache
	totalCost int64
	elements  map[int64]*list.Element
	// cache maintains elements in list.
	// Note that if the element's trackingMemUsage is 0, it will be removed from cache in order to keep cache not too long
	cache *list.List
}

// newInternalLRUCache returns internalLRUCache
func newInternalLRUCache(capacity int64) *internalLRUCache {
	if capacity < 1 {
		panic("capacity of LRU Cache should be at least 1.")
	}
	return &internalLRUCache{
		capacity: capacity,
		elements: make(map[int64]*list.Element),
		cache:    list.New(),
	}
}

// Get tries to find the corresponding value according to the given key.
func (l *internalLRUCache) Get(key int64) (*statistics.Table, bool) {
	l.Lock()
	r, hit := l.get(key)
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
	l.Lock()
	l.put(key, value, value.MemoryUsage(), true)
	trackingCost := l.trackingCost
	totalCost := l.totalCost
	l.Unlock()
	metrics.StatsCacheLRUCounter.WithLabelValues(typUpdate).Inc()
	metrics.StatsCacheLRUMemUsage.WithLabelValues(typTrack).Set(float64(trackingCost))
	metrics.StatsCacheLRUMemUsage.WithLabelValues(typTotal).Set(float64(totalCost))
}

func (l *internalLRUCache) put(key int64, value *statistics.Table, tblMemUsage *statistics.TableMemoryUsage, tryEvict bool) {
	// If the item TotalColTrackingMemUsage is larger than capacity, we will drop some structures in order to put it in cache
	for l.capacity < tblMemUsage.TotalColTrackingMemUsage() {
		for _, col := range value.Columns {
			col.DropEvicted()
			tblMemUsage = value.MemoryUsage()
			if l.capacity >= tblMemUsage.TotalColTrackingMemUsage() {
				break
			}
		}
	}
	defer func() {
		if tryEvict {
			l.evictIfNeeded()
		}
	}()
	element, exists := l.elements[key]
	if exists {
		oldMemUsage := element.Value.(*cacheItem).tblMemUsage
		element.Value.(*cacheItem).value = value
		element.Value.(*cacheItem).tblMemUsage = tblMemUsage
		l.calculateCost(tblMemUsage, oldMemUsage)
		l.maintainList(element, nil, tblMemUsage.TotalColTrackingMemUsage(), oldMemUsage.TotalColTrackingMemUsage())
		return
	}
	newCacheEntry := &cacheItem{
		key:         key,
		value:       value,
		tblMemUsage: tblMemUsage,
	}
	l.calculateCost(tblMemUsage, &statistics.TableMemoryUsage{})
	// We first push it into cache front here, if the element TotalColTrackingMemUsage is 0, it will be
	// removed form cache list in l.maintainList
	element = l.cache.PushFront(newCacheEntry)
	l.maintainList(element, newCacheEntry, tblMemUsage.TotalColTrackingMemUsage(), 1)
	l.elements[key] = element
}

// Del deletes the key-value pair from the LRU Cache.
func (l *internalLRUCache) Del(key int64) {
	l.Lock()
	del := l.del(key)
	trackingCost := l.trackingCost
	totalCost := l.totalCost
	l.Unlock()
	if del {
		metrics.StatsCacheLRUCounter.WithLabelValues(typDel).Inc()
		metrics.StatsCacheLRUMemUsage.WithLabelValues(typTrack).Set(float64(trackingCost))
		metrics.StatsCacheLRUMemUsage.WithLabelValues(typTotal).Set(float64(totalCost))
	}
}

func (l *internalLRUCache) del(key int64) bool {
	element, exists := l.elements[key]
	if !exists {
		return false
	}
	delete(l.elements, key)
	memUsage := element.Value.(*cacheItem).tblMemUsage
	l.calculateCost(&statistics.TableMemoryUsage{}, memUsage)
	l.maintainList(element, nil, 0, 1)
	return true
}

// Cost returns the current cost
func (l *internalLRUCache) Cost() int64 {
	l.RLock()
	defer l.RUnlock()
	return l.totalCost
}

// Keys returns the current Keys
func (l *internalLRUCache) Keys() []int64 {
	l.RLock()
	defer l.RUnlock()
	r := make([]int64, 0, len(l.elements))
	for _, v := range l.elements {
		r = append(r, v.Value.(*cacheItem).key)
	}
	return r
}

// Values returns the current Values
func (l *internalLRUCache) Values() []*statistics.Table {
	l.RLock()
	defer l.RUnlock()
	r := make([]*statistics.Table, 0, len(l.elements))
	for _, v := range l.elements {
		r = append(r, v.Value.(*cacheItem).value)
	}
	return r
}

// Map returns the map of table statistics
func (l *internalLRUCache) Map() map[int64]*statistics.Table {
	l.RLock()
	defer l.RUnlock()
	r := make(map[int64]*statistics.Table, len(l.elements))
	for k, v := range l.elements {
		r[k] = v.Value.(*cacheItem).value
	}
	return r
}

// Len returns the current length
func (l *internalLRUCache) Len() int {
	l.RLock()
	defer l.RUnlock()
	return len(l.elements)
}

// FreshMemUsage re-calculate the memory message
func (l *internalLRUCache) FreshMemUsage() {
	l.Lock()
	for _, v := range l.elements {
		item := v.Value.(*cacheItem)
		oldMemUsage := item.tblMemUsage
		newMemUsage := item.value.MemoryUsage()
		item.tblMemUsage = newMemUsage
		l.calculateCost(newMemUsage, oldMemUsage)
		l.maintainList(v, nil, newMemUsage.TotalColTrackingMemUsage(), oldMemUsage.TotalColTrackingMemUsage())
	}
	l.evictIfNeeded()
	totalCost := l.totalCost
	trackingCost := l.trackingCost
	l.Unlock()
	metrics.StatsCacheLRUMemUsage.WithLabelValues(typTrack).Set(float64(trackingCost))
	metrics.StatsCacheLRUMemUsage.WithLabelValues(typTotal).Set(float64(totalCost))
}

// FreshTableCost re-calculate the memory message for the certain key
func (l *internalLRUCache) FreshTableCost(key int64) {
	l.Lock()
	calculated := l.calculateTableCost(key)
	totalCost := l.totalCost
	trackingCost := l.trackingCost
	l.Unlock()
	if calculated {
		metrics.StatsCacheLRUMemUsage.WithLabelValues(typTrack).Set(float64(trackingCost))
		metrics.StatsCacheLRUMemUsage.WithLabelValues(typTotal).Set(float64(totalCost))
	}
}

func (l *internalLRUCache) calculateTableCost(key int64) bool {
	element, exists := l.elements[key]
	if !exists {
		return false
	}
	item := element.Value.(*cacheItem)
	l.put(item.key, item.value, item.value.MemoryUsage(), true)
	return true
}

// Copy returns a replication of LRU
func (l *internalLRUCache) Copy() statsCacheInner {
	var newCache *internalLRUCache
	l.RLock()
	newCache = newInternalLRUCache(l.capacity)
	node := l.cache.Back()
	for node != nil {
		key := node.Value.(*cacheItem).key
		value := node.Value.(*cacheItem).value
		tblMemUsage := node.Value.(*cacheItem).tblMemUsage
		newCache.put(key, value, tblMemUsage, false)
		node = node.Prev()
	}
	l.RUnlock()
	metrics.StatsCacheLRUCounter.WithLabelValues(typCopy).Inc()
	return newCache
}

// internalLRUCache will evict a table's column' structure in order to keep tracking cost under capacity
// If the elements has no structure can be evicted, it will be removed from list.
func (l *internalLRUCache) evictIfNeeded() {
	curr := l.cache.Back()
	evicted := false
	for !l.underCapacity() {
		evicted = true
		item := curr.Value.(*cacheItem)
		tbl := item.value
		oldMemUsage := item.tblMemUsage
		prev := curr.Prev()
		for _, col := range tbl.Columns {
			if col.IsEvicted() {
				continue
			}
			col.DropEvicted()
			newMemUsage := tbl.MemoryUsage()
			item.tblMemUsage = newMemUsage
			l.calculateCost(newMemUsage, oldMemUsage)
			if l.underCapacity() {
				break
			}
		}
		newMemUsage := tbl.MemoryUsage()
		if newMemUsage.TotalColTrackingMemUsage() < 1 {
			l.maintainList(curr, nil, newMemUsage.TotalColTrackingMemUsage(), oldMemUsage.TotalColTrackingMemUsage())
		}
		curr = prev
	}
	if evicted {
		metrics.StatsCacheLRUCounter.WithLabelValues(typEvict).Inc()
	}
}

func (l *internalLRUCache) calculateCost(newUsage, oldUsage *statistics.TableMemoryUsage) {
	l.totalCost += newUsage.TotalMemUsage - oldUsage.TotalMemUsage
	l.trackingCost += newUsage.TotalColTrackingMemUsage() - oldUsage.TotalColTrackingMemUsage()
}

// maintainList maintains elements in list cache
// For oldTotalColTrackingMemUsage>0 && newTotalColTrackingMemUsage>0, it means the element is updated.
// For oldTotalColTrackingMemUsage>0 && newTotalColTrackingMemUsage=0, it means the element is removed.
// For oldTotalColTrackingMemUsage=0 && newTotalColTrackingMemUsage>0, it means the new element is inserted
// For oldTotalColTrackingMemUsage=0 && newTotalColTrackingMemUsage=0, we do nothing.
func (l *internalLRUCache) maintainList(element *list.Element, item *cacheItem, newTotalColTrackingMemUsage, oldTotalColTrackingMemUsage int64) *list.Element {
	if oldTotalColTrackingMemUsage > 0 {
		if newTotalColTrackingMemUsage > 0 {
			l.cache.MoveToFront(element)
			return element
		}
		l.cache.Remove(element)
		return nil
	}
	if newTotalColTrackingMemUsage > 0 {
		return l.cache.PushFront(item)
	}
	return nil
}

func (l *internalLRUCache) underCapacity() bool {
	return l.trackingCost <= l.capacity
}
