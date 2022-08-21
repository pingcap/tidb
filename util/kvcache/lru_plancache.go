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

package kvcache

import (
	"container/list"

	"github.com/pingcap/errors"
)

// CacheEntry wraps Key and Value. It's the value of list.Element.
type CacheEntry struct {
	PlanKey   Key
	PlanValue Value
}

// LRUPlanCache is a simple least recently used cache for plan cache, not thread-safe, use for plan cache.
type LRUPlanCache struct {
	capacity uint
	size     uint
	// buckets replace the map in general LRU
	buckets map[string][]*list.Element

	// choose set rules to get one element from bucket.
	choose func([]*list.Element, interface{}) (*list.Element, int, bool)

	// onEvict function will be called if any eviction happened
	onEvict func(Key, Value)
	cache   *list.List
}

// NewLRUPlanCacheOld creates a PCLRUCache object, whose capacity is "capacity".
// NOTE: "capacity" should be a positive value.
func NewLRUPlanCacheOld(capacity uint, choose func([]*list.Element, interface{}) (*list.Element, int, bool)) *LRUPlanCache {
	if capacity < 1 {
		panic("capacity of LRU Cache should be at least 1.")
	}
	return &LRUPlanCache{
		capacity: capacity,
		size:     0,
		buckets:  make(map[string][]*list.Element),
		onEvict:  nil,
		cache:    list.New(),
		choose:   choose,
	}
}

// NewLRUPlanCache creates a PCLRUCache object, whose capacity is "capacity".
// NOTE: "capacity" should be a positive value.
func NewLRUPlanCache(capacity uint) *LRUPlanCache {
	if capacity < 1 {
		panic("capacity of LRU Cache should be at least 1.")
	}
	return &LRUPlanCache{
		capacity: capacity,
		size:     0,
		buckets:  make(map[string][]*list.Element),
		onEvict:  nil,
		cache:    list.New(),
	}
}

// SetOnEvict set the function called on each eviction.
func (l *LRUPlanCache) SetOnEvict(onEvict func(Key, Value)) {
	l.onEvict = onEvict
}

// SetChoose set the function called on each eviction.
func (l *LRUPlanCache) SetChoose(choose func([]*list.Element, interface{}) (*list.Element, int, bool)) {
	l.choose = choose
}

// Get tries to find the corresponding value according to the given key.
func (l *LRUPlanCache) Get(key Key, paramTypes interface{}) (value Value, ok bool) {
	bucket, bucketExist := l.buckets[string(key.Hash())]
	if bucketExist {
		if element, _, exist := l.choose(bucket, paramTypes); exist {
			l.cache.MoveToFront(element)
			return element.Value.(*CacheEntry).PlanValue, true
		}
	}
	return nil, false
}

// Put puts the (key, value) pair into the LRU Cache.
func (l *LRUPlanCache) Put(key Key, value Value, paramTypes interface{}) {
	hash := string(key.Hash())
	bucket, bucketExist := l.buckets[hash]
	// in the cache
	if bucketExist {
		if element, _, exist := l.choose(bucket, paramTypes); exist {
			element.Value.(*CacheEntry).PlanValue = value
			l.cache.MoveToFront(element)
			return
		}
	}
	// not in the cache
	newCacheEntry := &CacheEntry{
		PlanKey:   key,
		PlanValue: value,
	}
	element := l.cache.PushFront(newCacheEntry)
	bucket = append(bucket, element)
	l.buckets[hash] = bucket
	l.size++
	if l.size > l.capacity {
		l.RemoveOldest()
	}
}

// Delete deletes the multi-values from the LRU Cache.
func (l *LRUPlanCache) Delete(key Key) {
	hash := string(key.Hash())
	bucket, bucketExist := l.buckets[hash]
	if bucketExist {
		for _, element := range bucket {
			l.cache.Remove(element)
			l.size--
		}
		l.buckets[hash] = nil
	}
}

// DeleteAll deletes all elements from the LRU Cache.
func (l *LRUPlanCache) DeleteAll() {
	for lru := l.cache.Back(); lru != nil; lru = l.cache.Back() {
		l.cache.Remove(lru)
		l.size--
	}
	l.buckets = make(map[string][]*list.Element)
}

// Size gets the current cache size.
func (l *LRUPlanCache) Size() int {
	return int(l.size)
}

// Values return all values in cache.
func (l *LRUPlanCache) Values() []Value {
	values := make([]Value, 0, l.cache.Len())
	for ele := l.cache.Front(); ele != nil; ele = ele.Next() {
		value := ele.Value.(*CacheEntry).PlanValue
		values = append(values, value)
	}
	return values
}

// Keys return all keys in cache.
func (l *LRUPlanCache) Keys() []Key {
	keys := make([]Key, 0, len(l.buckets))
	for _, bucket := range l.buckets {
		keys = append(keys, bucket[0].Value.(*CacheEntry).PlanKey)
	}
	return keys
}

// SetCapacity sets capacity of the cache.
func (l *LRUPlanCache) SetCapacity(capacity uint) error {
	if capacity < 1 {
		return errors.New("capacity of lru cache should be at least 1")
	}
	l.capacity = capacity
	for l.size > l.capacity {
		l.RemoveOldest()
	}
	return nil
}

// RemoveOldest removes the oldest element from the cache.
func (l *LRUPlanCache) RemoveOldest() (key Key, value Value, ok bool) {
	if l.size > 0 {
		lru := l.cache.Back()
		if l.onEvict != nil {
			l.onEvict(lru.Value.(*CacheEntry).PlanKey, lru.Value.(*CacheEntry).PlanValue)
		}

		l.cache.Remove(lru)
		l.removeFromBucket(lru)
		l.size--
		return lru.Value.(*CacheEntry).PlanKey, lru.Value.(*CacheEntry).PlanValue, true
	}
	return nil, nil, false
}

// removeFromBucket remove element from bucket
func (l *LRUPlanCache) removeFromBucket(element *list.Element) {
	hash := string(element.Value.(*CacheEntry).PlanKey.Hash())
	bucket := l.buckets[hash]
	for i, ele := range bucket {
		if ele == element {
			bucket = append(bucket[:i], bucket[i+1:]...)
			l.buckets[hash] = bucket
			return
		}
	}
}
