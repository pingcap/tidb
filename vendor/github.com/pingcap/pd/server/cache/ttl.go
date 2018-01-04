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
	"time"

	log "github.com/sirupsen/logrus"
)

type ttlCacheItem struct {
	value  interface{}
	expire time.Time
}

// TTL is a cache that assigns TTL(Time-To-Live) for each items.
type TTL struct {
	sync.RWMutex

	items      map[uint64]ttlCacheItem
	ttl        time.Duration
	gcInterval time.Duration
}

// NewTTL returns a new TTL cache.
func NewTTL(gcInterval time.Duration, ttl time.Duration) *TTL {
	c := &TTL{
		items:      make(map[uint64]ttlCacheItem),
		ttl:        ttl,
		gcInterval: gcInterval,
	}

	go c.doGC()
	return c
}

// Put puts an item into cache.
func (c *TTL) Put(key uint64, value interface{}) {
	c.PutWithTTL(key, value, c.ttl)
}

// PutWithTTL puts an item into cache with specified TTL.
func (c *TTL) PutWithTTL(key uint64, value interface{}, ttl time.Duration) {
	c.Lock()
	defer c.Unlock()

	c.items[key] = ttlCacheItem{
		value:  value,
		expire: time.Now().Add(ttl),
	}
}

// Get retrives an item from cache.
func (c *TTL) Get(key uint64) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	if item.expire.Before(time.Now()) {
		return nil, false
	}

	return item.value, true
}

// Remove eliminates an item from cache.
func (c *TTL) Remove(key uint64) {
	c.Lock()
	defer c.Unlock()

	delete(c.items, key)
}

// Len returns current cache size.
func (c *TTL) Len() int {
	c.RLock()
	defer c.RUnlock()

	return len(c.items)
}

func (c *TTL) doGC() {
	ticker := time.NewTicker(c.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			count := 0
			now := time.Now()
			c.Lock()
			for key := range c.items {
				if value, ok := c.items[key]; ok {
					if value.expire.Before(now) {
						count++
						delete(c.items, key)
					}
				}
			}
			c.Unlock()

			log.Debugf("GC %d items", count)
		}
	}
}

// TTLUint64 is simple TTL saves only uint64s.
type TTLUint64 struct {
	*TTL
}

// NewIDTTL creates a new TTLUint64 cache.
func NewIDTTL(gcInterval, ttl time.Duration) *TTLUint64 {
	return &TTLUint64{
		TTL: NewTTL(gcInterval, ttl),
	}
}

// Put saves an ID in cache.
func (c *TTLUint64) Put(id uint64) {
	c.TTL.Put(id, nil)
}

// Exists checks if an ID exists in cache.
func (c *TTLUint64) Exists(id uint64) bool {
	_, ok := c.TTL.Get(id)
	return ok
}
