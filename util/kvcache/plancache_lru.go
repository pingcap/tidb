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
	"github.com/pingcap/tidb/util/memory"
)

// PCLRUCache is a simple least recently used cache, not thread-safe, use for plan cache.
type PCLRUCache struct {
	capacity uint
	size     uint
	// 0 indicates no quota
	quota uint64
	guard float64

	elements map[string]*list.Element
	bucket   map[string][]*list.Element

	// onGet set rules to get one element from multi element.It can not be nil!
	choose func([]*list.Element) (*list.Element, bool)

	// onEvict function will be called if any eviction happened
	onEvict func(Key, Value)
	cache   *list.List
}

// NewPCLRUCache creates a PCLRUCache object, whose capacity is "capacity".
// NOTE: "capacity" should be a positive value.
func NewPCLRUCache(capacity uint, guard float64, quota uint64, choose func([]*list.Element) (*list.Element, bool)) *PCLRUCache {
	if capacity < 1 {
		panic("capacity of LRU Cache should be at least 1.")
	}
	return &PCLRUCache{
		capacity: capacity,
		size:     0,
		quota:    quota,
		guard:    guard,
		elements: make(map[string]*list.Element),
		bucket:   make(map[string][]*list.Element),
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
func (l *PCLRUCache) Get(key Key) (value Value, ok bool) {
	//element, exists := l.elements[string(key.Hash())]
	if elements, exist := l.bucket[string(key.Hash())]; exist {
		if element, exist1 := l.choose(elements); exist1 {
			l.cache.MoveToFront(element)
			return element.Value.(*cacheEntry).value, true
		}
	}
	return nil, false
}

// Put puts the (key, value) pair into the LRU Cache.
func (l *PCLRUCache) Put(key Key, value Value) {
	hash := string(key.Hash())
	//element, exists := l.elements[hash]
	if elements, exist := l.bucket[hash]; exist {
		if element, exist1 := l.choose(elements); exist1 {
			element.Value.(*cacheEntry).value = value
			l.cache.MoveToFront(element)
			return
		}
	}
	//if exists {
	//	element.Value.(*cacheEntry).value = value
	//	l.cache.MoveToFront(element)
	//	return
	//}

	newCacheEntry := &cacheEntry{
		key:   key,
		value: value,
	}

	element := l.cache.PushFront(newCacheEntry)
	l.elements[hash] = element
	l.size++
	// Getting used memory is expensive and can be avoided by setting quota to 0.
	if l.quota == 0 {
		if l.size > l.capacity {
			lru := l.cache.Back()
			l.cache.Remove(lru)
			if l.onEvict != nil {
				l.onEvict(lru.Value.(*cacheEntry).key, lru.Value.(*cacheEntry).value)
			}
			delete(l.elements, string(lru.Value.(*cacheEntry).key.Hash()))
			l.size--
		}
		return
	}

	for memUsed, _ := memory.InstanceMemUsed(); memUsed > uint64(float64(l.quota)*(1.0-l.guard)); memUsed, _ = memory.InstanceMemUsed() {
		lru := l.cache.Back()
		if lru == nil {
			break
		}

		if l.onEvict != nil {
			l.onEvict(lru.Value.(*cacheEntry).key, lru.Value.(*cacheEntry).value)
		}

		l.cache.Remove(lru)
		delete(l.elements, string(lru.Value.(*cacheEntry).key.Hash()))
		l.size--
	}
}

// Delete deletes the key-value pair from the LRU Cache.
func (l *PCLRUCache) Delete(key Key) {
	k := string(key.Hash())
	element := l.elements[k]
	if element == nil {
		return
	}
	l.cache.Remove(element)
	delete(l.elements, k)
	l.size--
}

// DeleteAll deletes all elements from the LRU Cache.
func (l *PCLRUCache) DeleteAll() {
	for lru := l.cache.Back(); lru != nil; lru = l.cache.Back() {
		l.cache.Remove(lru)
		delete(l.elements, string(lru.Value.(*cacheEntry).key.Hash()))
		l.size--
	}
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
		lru := l.cache.Back()
		l.cache.Remove(lru)
		delete(l.elements, string(lru.Value.(*cacheEntry).key.Hash()))
		l.size--
	}
	return nil
}

// RemoveOldest removes the oldest element from the cache.
func (l *PCLRUCache) RemoveOldest() (key Key, value Value, ok bool) {
	if l.size > 0 {
		ele := l.cache.Back()
		l.cache.Remove(ele)
		delete(l.elements, string(ele.Value.(*cacheEntry).key.Hash()))
		l.size--
		return ele.Value.(*cacheEntry).key, ele.Value.(*cacheEntry).value, true
	}
	return nil, nil, false
}
