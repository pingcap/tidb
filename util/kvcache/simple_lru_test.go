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

package kvcache

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testLRUCacheSuite{})

type testLRUCacheSuite struct {
}

type mockCacheKey struct {
	hash []byte
	key  int64
}

func (mk *mockCacheKey) Hash() []byte {
	if mk.hash != nil {
		return mk.hash
	}
	mk.hash = make([]byte, 8)
	for i := uint(0); i < 8; i++ {
		mk.hash[i] = byte((mk.key >> ((i - 1) * 8)) & 0xff)
	}
	return mk.hash
}

func newMockHashKey(key int64) *mockCacheKey {
	return &mockCacheKey{
		key: key,
	}
}

func (s *testLRUCacheSuite) TestPut(c *C) {
	lru := NewSimpleLRUCache(3)
	c.Assert(lru.capacity, Equals, uint(3))

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)

	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	c.Assert(lru.size, Equals, lru.capacity)
	c.Assert(lru.size, Equals, uint(3))

	// test for non-existent elements
	for i := 0; i < 2; i++ {
		hash := string(keys[i].Hash())
		element, exists := lru.elements[hash]
		c.Assert(exists, IsFalse)
		c.Assert(element, IsNil)
	}

	// test for existent elements
	root := lru.cache.Front()
	c.Assert(root, NotNil)
	for i := 4; i >= 2; i-- {
		entry, ok := root.Value.(*cacheEntry)
		c.Assert(ok, IsTrue)
		c.Assert(entry, NotNil)

		// test key
		key := entry.key
		c.Assert(key, NotNil)
		c.Assert(key, Equals, keys[i])

		hash := string(keys[i].Hash())
		element, exists := lru.elements[hash]
		c.Assert(exists, IsTrue)
		c.Assert(element, NotNil)
		c.Assert(element, Equals, root)

		// test value
		value, ok := entry.value.(int64)
		c.Assert(ok, IsTrue)
		c.Assert(value, Equals, vals[i])

		root = root.Next()
	}
	// test for end of double-linked list
	c.Assert(root, IsNil)
}

func (s *testLRUCacheSuite) TestGet(c *C) {
	lru := NewSimpleLRUCache(3)

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)

	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}

	// test for non-existent elements
	for i := 0; i < 2; i++ {
		value, exists := lru.Get(keys[i])
		c.Assert(exists, IsFalse)
		c.Assert(value, IsNil)
	}

	for i := 2; i < 5; i++ {
		value, exists := lru.Get(keys[i])
		c.Assert(exists, IsTrue)
		c.Assert(value, NotNil)
		c.Assert(value, Equals, vals[i])
		c.Assert(lru.size, Equals, uint(3))
		c.Assert(lru.capacity, Equals, uint(3))

		root := lru.cache.Front()
		c.Assert(root, NotNil)

		entry, ok := root.Value.(*cacheEntry)
		c.Assert(ok, IsTrue)
		c.Assert(entry.key, Equals, keys[i])

		value, ok = entry.value.(int64)
		c.Assert(ok, IsTrue)
		c.Assert(value, Equals, vals[i])
	}
}
