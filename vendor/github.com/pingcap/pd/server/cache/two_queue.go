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

const (

	// defaultRecentRatio is the ratio of the 2Q cache dedicated
	// to recently added entries that have only been accessed once.
	defaultRecentRatio float64 = 0.25

	// defaultGhostRatio is the default ratio of ghost
	// entries kept to track entries recently evicted
	defaultGhostRatio float64 = 0.50
)

// TwoQueue is a fixed size 2Q cache.
// 2Q is an enhancement over the standard LRU cache
// in that it tracks both frequently and recently used
// entries separately. This avoids a burst in access to new
// entries from evicting frequently used entries. It adds some
// additional tracking overhead to the standard LRU cache, and is
// computationally about 2x the cost, and adds some metadata over
// head. The ARCCache is similar, but does not require setting any
// parameters.
// TwoQueue implementation is based on https://github.com/hashicorp/golang-lru/blob/master/2q.go
type TwoQueue struct {
	size       int
	recentSize int

	recent   *LRU
	frequent *LRU
	ghost    *LRU
}

func newTwoQueue(size int) *TwoQueue {
	return newTwoQueueParams(size, defaultRecentRatio, defaultGhostRatio)
}

func newTwoQueueParams(size int, recentRatio, ghostRatio float64) *TwoQueue {
	recentSize := int(float64(size) * recentRatio)
	ghostSize := int(float64(size) * ghostRatio)

	return &TwoQueue{
		size:       size,
		recentSize: recentSize,
		recent:     newLRU(size),
		frequent:   newLRU(size),
		ghost:      newLRU(ghostSize),
	}
}

// Put puts an item into cache.
func (c *TwoQueue) Put(key uint64, value interface{}) {
	// Check if value is in frequent list,
	// then just update it
	if c.frequent.contains(key) {
		c.frequent.Put(key, value)
		return
	}

	// Check if value is in recent list,
	// then move it to frequent list
	if c.recent.contains(key) {
		c.recent.Remove(key)
		c.frequent.Put(key, value)
		return
	}

	// Check if value is in ghost list,
	// then put it to frequent list
	if c.ghost.contains(key) {
		c.ensureSpace(true)
		c.ghost.Remove(key)
		c.frequent.Put(key, value)
		return
	}

	// Put it to recent list
	c.ensureSpace(false)
	c.recent.Put(key, value)
	return
}

func (c *TwoQueue) ensureSpace(ghost bool) {
	recentLen := c.recent.Len()
	frequentLen := c.frequent.Len()
	if recentLen+frequentLen < c.size {
		return
	}

	// If recent list is larger than target, evict from there
	if recentLen > 0 && (recentLen > c.recentSize || (recentLen == c.recentSize && !ghost)) {
		k, _, _ := c.recent.getAndRemoveOldest()
		c.ghost.Put(k, nil)
		return
	}

	// Remove from frequent list
	c.frequent.removeOldest()
}

// Get retrives an item from cache.
func (c *TwoQueue) Get(key uint64) (interface{}, bool) {
	// Check in frequent list
	if val, ok := c.frequent.Get(key); ok {
		return val, ok
	}

	// If in recent list, move it to frequent list
	if val, ok := c.recent.Peek(key); ok {
		c.recent.Remove(key)
		c.frequent.Put(key, val)
		return val, ok
	}

	return nil, false
}

// Peek reads an item from cache. The action is no considerd 'Use'.
func (c *TwoQueue) Peek(key uint64) (interface{}, bool) {
	if val, ok := c.frequent.Peek(key); ok {
		return val, ok
	}
	return c.recent.Peek(key)
}

// Remove eliminates an item from cache.
func (c *TwoQueue) Remove(key uint64) {
	if c.frequent.remove(key) {
		return
	}
	if c.recent.remove(key) {
		return
	}
	if c.ghost.remove(key) {
		return
	}
}

// Elems return all items in cache.
func (c *TwoQueue) Elems() []*Item {
	size := c.Len()
	elems := make([]*Item, 0, size)
	elems = append(elems, c.recent.Elems()...)
	elems = append(elems, c.frequent.Elems()...)
	return elems
}

// Len returns current cache size.
func (c *TwoQueue) Len() int {
	return c.recent.Len() + c.frequent.Len()
}
