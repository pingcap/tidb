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
	"fmt"
	"reflect"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/memory"
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
	maxMem, err := memory.MemTotal()
	c.Assert(err, IsNil)

	lru := NewSimpleLRUCache(3, 0, maxMem)
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
		element, exists := lru.elements[string(keys[i].Hash())]
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

		element, exists := lru.elements[string(keys[i].Hash())]
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

func (s *testLRUCacheSuite) TestZeroQuota(c *C) {
	lru := NewSimpleLRUCache(100, 0, 0)
	c.Assert(lru.capacity, Equals, uint(100))

	keys := make([]*mockCacheKey, 100)
	vals := make([]int64, 100)

	for i := 0; i < 100; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	c.Assert(lru.size, Equals, lru.capacity)
	c.Assert(lru.size, Equals, uint(100))
}

func (s *testLRUCacheSuite) TestOOMGuard(c *C) {
	maxMem, err := memory.MemTotal()
	c.Assert(err, IsNil)

	lru := NewSimpleLRUCache(3, 1.0, maxMem)
	c.Assert(lru.capacity, Equals, uint(3))

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)

	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	c.Assert(lru.size, Equals, uint(0))

	// test for non-existent elements
	for i := 0; i < 5; i++ {
		element, exists := lru.elements[string(keys[i].Hash())]
		c.Assert(exists, IsFalse)
		c.Assert(element, IsNil)
	}
}

func (s *testLRUCacheSuite) TestGet(c *C) {
	maxMem, err := memory.MemTotal()
	c.Assert(err, IsNil)

	lru := NewSimpleLRUCache(3, 0, maxMem)

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

func (s *testLRUCacheSuite) TestDelete(c *C) {
	maxMem, err := memory.MemTotal()
	c.Assert(err, IsNil)

	lru := NewSimpleLRUCache(3, 0, maxMem)

	keys := make([]*mockCacheKey, 3)
	vals := make([]int64, 3)

	for i := 0; i < 3; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	c.Assert(int(lru.size), Equals, 3)

	lru.Delete(keys[1])
	value, exists := lru.Get(keys[1])
	c.Assert(exists, IsFalse)
	c.Assert(value, IsNil)
	c.Assert(int(lru.size), Equals, 2)

	_, exists = lru.Get(keys[0])
	c.Assert(exists, IsTrue)

	_, exists = lru.Get(keys[2])
	c.Assert(exists, IsTrue)
}

func (s *testLRUCacheSuite) TestDeleteAll(c *C) {
	maxMem, err := memory.MemTotal()
	c.Assert(err, IsNil)

	lru := NewSimpleLRUCache(3, 0, maxMem)

	keys := make([]*mockCacheKey, 3)
	vals := make([]int64, 3)

	for i := 0; i < 3; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}
	c.Assert(int(lru.size), Equals, 3)

	lru.DeleteAll()

	for i := 0; i < 3; i++ {
		value, exists := lru.Get(keys[i])
		c.Assert(exists, IsFalse)
		c.Assert(value, IsNil)
		c.Assert(int(lru.size), Equals, 0)
	}
}

func (s *testLRUCacheSuite) TestValues(c *C) {
	maxMem, err := memory.MemTotal()
	c.Assert(err, IsNil)

	lru := NewSimpleLRUCache(5, 0, maxMem)

	keys := make([]*mockCacheKey, 5)
	vals := make([]int64, 5)

	for i := 0; i < 5; i++ {
		keys[i] = newMockHashKey(int64(i))
		vals[i] = int64(i)
		lru.Put(keys[i], vals[i])
	}

	values := lru.Values()
	c.Assert(len(values), Equals, 5)
	for i := 0; i < 5; i++ {
		c.Assert(values[i], Equals, int64(4-i))
	}
}

func (s *testLRUCacheSuite) TestPutProfileName(c *C) {
	lru := NewSimpleLRUCache(3, 0, 10)
	c.Assert(lru.capacity, Equals, uint(3))
	t := reflect.TypeOf(*lru)
	pt := reflect.TypeOf(lru)
	functionName := ""
	for i := 0; i < pt.NumMethod(); i++ {
		if pt.Method(i).Name == "Put" {
			functionName = "Put"
		}
	}
	pName := fmt.Sprintf("%s.(*%s).%s", t.PkgPath(), t.Name(), functionName)
	c.Assert(pName, Equals, ProfileName)
}
