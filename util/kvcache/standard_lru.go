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
)

// cacheEntry wraps Key and Value. It's the value of list.Element.
type cacheItem struct {
	key   Key
	value Value
	cost  uint64
}

// StandardLRUCache is a simple least recently used cache, not thread-safe.
type StandardLRUCache struct {
	capacity uint64
	cost     uint64
	elements map[string]*list.Element
	cache    *list.List
}

// NewStandardLRUCache returns StandardLRUCache
func NewStandardLRUCache(capacity uint64) *StandardLRUCache {
	if capacity < 1 {
		panic("capacity of LRU Cache should be at least 1.")
	}
	return &StandardLRUCache{
		capacity: capacity,
		cost:     0,
		elements: make(map[string]*list.Element),
		cache:    list.New(),
	}
}

// Get tries to find the corresponding value according to the given key.
func (l *StandardLRUCache) Get(key Key) (value Value, ok bool) {
	element, exists := l.elements[string(key.Hash())]
	if !exists {
		return nil, false
	}
	l.cache.MoveToFront(element)
	return element.Value.(*cacheItem).value, true
}

// Put puts the (key, value) pair into the LRU Cache.
func (l *StandardLRUCache) Put(key Key, value Value, cost uint64) {
	hash := string(key.Hash())
	element, exists := l.elements[hash]
	if exists {
		oldCost := element.Value.(*cacheItem).cost
		element.Value.(*cacheItem).value = value
		element.Value.(*cacheItem).cost = cost
		l.cache.MoveToFront(element)
		l.cost += cost - oldCost
		return
	}
	newCacheEntry := &cacheItem{
		key:   key,
		value: value,
		cost:  cost,
	}
	element = l.cache.PushFront(newCacheEntry)
	l.elements[hash] = element
	l.cost += cost
	for l.cost > l.capacity {
		lru := l.cache.Back()
		l.cache.Remove(lru)
		droppedItem := lru.Value.(*cacheItem)
		delete(l.elements, string(droppedItem.key.Hash()))
		l.cost -= droppedItem.cost
	}
}

// Delete deletes the key-value pair from the LRU Cache.
func (l *StandardLRUCache) Delete(key Key) {
	k := string(key.Hash())
	element := l.elements[k]
	if element == nil {
		return
	}
	l.cache.Remove(element)
	delete(l.elements, k)
	l.cost -= element.Value.(*cacheItem).cost
}

// Cost returns the current cost
func (l *StandardLRUCache) Cost() uint64 {
	return l.cost
}

// Copy returns a replication of LRU
func (l *StandardLRUCache) Copy() *StandardLRUCache {
	newCache := NewStandardLRUCache(l.capacity)
	node := l.cache.Back()
	for node != nil {
		key := node.Value.(*cacheItem).key
		value := node.Value.(*cacheItem).value
		cost := node.Value.(*cacheItem).cost
		newCache.Put(key, value, cost)
		node = node.Prev()
	}
	return newCache
}

// Keys returns the current Keys
func (l *StandardLRUCache) Keys() []Key {
	r := make([]Key, 0, len(l.elements))
	for _, v := range l.elements {
		r = append(r, v.Value.(*cacheItem).key)
	}
	return r
}

// Values returns the current Values
func (l *StandardLRUCache) Values() []Value {
	r := make([]Value, 0, len(l.elements))
	for _, v := range l.elements {
		r = append(r, v.Value.(*cacheItem).value)
	}
	return r
}

// Len returns the current length
func (l *StandardLRUCache) Len() int {
	return len(l.elements)
}
