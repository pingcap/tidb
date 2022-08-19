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

// PCLRUCache is a simple least recently used cache, not thread-safe, use for plan cache.
type PCLRUCache struct {
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

// NewPCLRUCache creates a PCLRUCache object, whose capacity is "capacity".
// NOTE: "capacity" should be a positive value.
func NewPCLRUCache(capacity uint, choose func([]*list.Element, interface{}) (*list.Element, int, bool)) *PCLRUCache {
	if capacity < 1 {
		panic("capacity of LRU Cache should be at least 1.")
	}
	return &PCLRUCache{
		capacity: capacity,
		size:     0,
		buckets:  make(map[string][]*list.Element),
		onEvict:  nil,
		cache:    list.New(),
		choose:   choose,
	}
}

// SetOnEvict set the function called on each eviction.
func (l *PCLRUCache) SetOnEvict(onEvict func(Key, Value)) {
	l.onEvict = onEvict
}

// Get tries to find the corresponding value according to the given key.
func (l *PCLRUCache) Get(key Key, paramTypes interface{}) (value Value, ok bool) {
	if bucket, exist := l.buckets[string(key.Hash())]; exist {
		if element, _, exist1 := l.choose(bucket, paramTypes); exist1 {
			l.cache.MoveToFront(element)
			return element.Value.(*cacheEntry).value, true
		}
	}
	return nil, false
}

// Put puts the (key, value) pair into the LRU Cache.
func (l *PCLRUCache) Put(key Key, value Value, paramTypes interface{}) {
	hash := string(key.Hash())
	bucket, bucketExist := l.buckets[hash]
	if bucketExist {
		if candidate, _, exist := l.choose(bucket, paramTypes); exist {
			candidate.Value.(*cacheEntry).value = value
			l.cache.MoveToFront(candidate)
			return
		}
	}

	newCacheEntry := &cacheEntry{
		key:   key,
		value: value,
	}

	element := l.cache.PushFront(newCacheEntry)
	bucket = append(bucket, element)
	l.buckets[hash] = bucket
	l.size++
	if l.size > l.capacity {
		l.RemoveOldest()
	}
}

// Delete deletes the key-value pair from the LRU Cache.
func (l *PCLRUCache) Delete(key Key, paramTypes interface{}) {
	k := string(key.Hash())
	bucket, ok := l.buckets[k]
	if !ok {
		return
	}
	// remove from bucket
	if element, idx, exist1 := l.choose(bucket, paramTypes); exist1 {
		bucket = append(bucket[:idx], bucket[idx+1:]...)
		l.buckets[k] = bucket
		l.cache.Remove(element)
		l.size--
	}
}

// DeleteAll deletes all elements from the LRU Cache.
func (l *PCLRUCache) DeleteAll() {
	for lru := l.cache.Back(); lru != nil; lru = l.cache.Back() {
		l.cache.Remove(lru)
		l.size--
	}
	l.buckets = nil
}

// Size gets the current cache size.
func (l *PCLRUCache) Size() int {
	return int(l.size)
}

// Values return all values in cache.
func (l *PCLRUCache) Values() []Value {
	values := make([]Value, 0, l.cache.Len())
	for ele := l.cache.Front(); ele != nil; ele = ele.Next() {
		value := ele.Value.(*cacheEntry).value
		values = append(values, value)
	}
	return values
}

// Keys return all keys in cache.
func (l *PCLRUCache) Keys() []Key {
	keys := make([]Key, 0, l.cache.Len())
	for ele := l.cache.Front(); ele != nil; ele = ele.Next() {
		key := ele.Value.(*cacheEntry).key
		keys = append(keys, key)
	}
	return keys
}

// SetCapacity sets capacity of the cache.
func (l *PCLRUCache) SetCapacity(capacity uint) error {
	if capacity < 1 {
		return errors.New("capacity of lru cache should be at least 1")
	}
	l.capacity = capacity
	for l.size > l.capacity {
		_, _, _ = l.RemoveOldest()
	}
	return nil
}

// RemoveOldest removes the oldest element from the cache.
func (l *PCLRUCache) RemoveOldest() (key Key, value Value, ok bool) {
	if l.size > 0 {
		lru := l.cache.Back()
		if l.onEvict != nil {
			l.onEvict(lru.Value.(*cacheEntry).key, lru.Value.(*cacheEntry).value)
		}

		l.cache.Remove(lru)
		l.removeFromBucket(lru)
		l.size--
		return lru.Value.(*cacheEntry).key, lru.Value.(*cacheEntry).value, true
	}
	return nil, nil, false
}

// removeFromBucket remove element from bucket
func (l *PCLRUCache) removeFromBucket(element *list.Element) {
	k := string(element.Value.(*cacheEntry).key.Hash())
	bucket := l.buckets[k]
	for i, ele := range bucket {
		if ele == element {
			bucket = append(bucket[:i], bucket[i+1:]...)
			l.buckets[k] = bucket
			break
		}
	}
}
