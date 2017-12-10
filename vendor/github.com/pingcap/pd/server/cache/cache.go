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
	"sync"
)

// Cache is an interface for cache system
type Cache interface {
	// Put puts an item into cache.
	Put(key uint64, value interface{})
	// Get retrives an item from cache.
	Get(key uint64) (interface{}, bool)
	// Peek reads an item from cache. The action is no considered 'Use'.
	Peek(key uint64) (interface{}, bool)
	// Remove eliminates an item from cache.
	Remove(key uint64)
	// Elems return all items in cache.
	Elems() []*Item
	// Len returns current cache size
	Len() int
}

// Type is cache's type such as LRUCache and etc.
type Type int

const (
	// LRUCache is for LRU cache
	LRUCache Type = 1
	// TwoQueueCache is for 2Q cache
	TwoQueueCache Type = 2
)

var (
	// DefaultCacheType set default cache type for NewDefaultCache function
	DefaultCacheType = LRUCache
)

type threadSafeCache struct {
	cache Cache
	lock  sync.RWMutex
}

func newThreadSafeCache(cache Cache) Cache {
	return &threadSafeCache{
		cache: cache,
	}
}

// Put puts an item into cache.
func (c *threadSafeCache) Put(key uint64, value interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache.Put(key, value)
}

// Get retrives an item from cache.
func (c *threadSafeCache) Get(key uint64) (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.cache.Get(key)
}

// Peek reads an item from cache. The action is no considered 'Use'.
func (c *threadSafeCache) Peek(key uint64) (interface{}, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.cache.Peek(key)
}

// Remove eliminates an item from cache.
func (c *threadSafeCache) Remove(key uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.cache.Remove(key)
}

// Elems return all items in cache.
func (c *threadSafeCache) Elems() []*Item {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.cache.Elems()
}

// Len returns current cache size
func (c *threadSafeCache) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.cache.Len()
}

// NewCache create Cache instance by CacheType
func NewCache(size int, cacheType Type) Cache {
	switch cacheType {
	case LRUCache:
		return newThreadSafeCache(newLRU(size))
	case TwoQueueCache:
		return newThreadSafeCache(newTwoQueue(size))
	default:
		panic("Unknown cache type")
	}
}

// NewDefaultCache create Cache instance by default cache type
func NewDefaultCache(size int) Cache {
	return NewCache(size, DefaultCacheType)
}
