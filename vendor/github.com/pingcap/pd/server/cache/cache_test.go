// Copyright 2016 PingCAP, Inc.
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
	"testing"
	"time"

	. "github.com/pingcap/check"
)

func TestCore(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRegionCacheSuite{})

type testRegionCacheSuite struct {
}

func (s *testRegionCacheSuite) TestExpireRegionCache(c *C) {
	cache := NewTTL(time.Second, 2*time.Second)
	cache.PutWithTTL(1, 1, 1*time.Second)
	cache.PutWithTTL(2, "v2", 5*time.Second)
	cache.PutWithTTL(3, 3.0, 5*time.Second)

	value, ok := cache.Get(1)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 1)

	value, ok = cache.Get(2)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, "v2")

	value, ok = cache.Get(3)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 3.0)

	c.Assert(cache.Len(), Equals, 3)

	time.Sleep(2 * time.Second)

	value, ok = cache.Get(1)
	c.Assert(ok, IsFalse)
	c.Assert(value, IsNil)

	value, ok = cache.Get(2)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, "v2")

	value, ok = cache.Get(3)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 3.0)

	c.Assert(cache.Len(), Equals, 2)

	cache.Remove(2)

	value, ok = cache.Get(2)
	c.Assert(ok, IsFalse)
	c.Assert(value, IsNil)

	value, ok = cache.Get(3)
	c.Assert(ok, IsTrue)
	c.Assert(value, Equals, 3.0)

	c.Assert(cache.Len(), Equals, 1)
}

func (s *testRegionCacheSuite) TestLRUCache(c *C) {
	cache := newLRU(3)
	cache.Put(1, "1")
	cache.Put(2, "2")
	cache.Put(3, "3")

	val, ok := cache.Get(3)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "3")

	val, ok = cache.Get(2)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "2")

	val, ok = cache.Get(1)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "1")

	c.Assert(cache.Len(), Equals, 3)

	cache.Put(4, "4")

	c.Assert(cache.Len(), Equals, 3)

	val, ok = cache.Get(3)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.Get(1)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "1")

	val, ok = cache.Get(2)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "2")

	val, ok = cache.Get(4)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "4")

	c.Assert(cache.Len(), Equals, 3)

	val, ok = cache.Peek(1)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "1")

	elems := cache.Elems()
	c.Assert(elems, HasLen, 3)
	c.Assert(elems[0].Value, DeepEquals, "4")
	c.Assert(elems[1].Value, DeepEquals, "2")
	c.Assert(elems[2].Value, DeepEquals, "1")

	cache.Remove(1)
	cache.Remove(2)
	cache.Remove(4)

	c.Assert(cache.Len(), Equals, 0)

	val, ok = cache.Get(1)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.Get(2)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.Get(3)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.Get(4)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)
}

func (s *testRegionCacheSuite) TestFifoCache(c *C) {
	cache := NewFIFO(3)
	cache.Put(1, "1")
	cache.Put(2, "2")
	cache.Put(3, "3")
	c.Assert(cache.Len(), Equals, 3)

	cache.Put(4, "4")
	c.Assert(cache.Len(), Equals, 3)

	elems := cache.Elems()
	c.Assert(elems, HasLen, 3)
	c.Assert(elems[0].Value, DeepEquals, "2")
	c.Assert(elems[1].Value, DeepEquals, "3")
	c.Assert(elems[2].Value, DeepEquals, "4")

	elems = cache.FromElems(3)
	c.Assert(elems, HasLen, 1)
	c.Assert(elems[0].Value, DeepEquals, "4")

	cache.Remove()
	cache.Remove()
	cache.Remove()
	c.Assert(cache.Len(), Equals, 0)
}

func (s *testRegionCacheSuite) TestTwoQueueCache(c *C) {
	cache := newTwoQueue(3)
	cache.Put(1, "1")
	cache.Put(2, "2")
	cache.Put(3, "3")

	val, ok := cache.Get(3)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "3")

	val, ok = cache.Get(2)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "2")

	val, ok = cache.Get(1)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "1")

	c.Assert(cache.Len(), Equals, 3)

	cache.Put(4, "4")

	c.Assert(cache.Len(), Equals, 3)

	val, ok = cache.Get(3)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.Get(1)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "1")

	val, ok = cache.Get(2)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "2")

	val, ok = cache.Get(4)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "4")

	c.Assert(cache.Len(), Equals, 3)

	val, ok = cache.Peek(1)
	c.Assert(ok, IsTrue)
	c.Assert(val, DeepEquals, "1")

	elems := cache.Elems()
	c.Assert(elems, HasLen, 3)
	c.Assert(elems[0].Value, DeepEquals, "4")
	c.Assert(elems[1].Value, DeepEquals, "2")
	c.Assert(elems[2].Value, DeepEquals, "1")

	cache.Remove(1)
	cache.Remove(2)
	cache.Remove(4)

	c.Assert(cache.Len(), Equals, 0)

	val, ok = cache.Get(1)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.Get(2)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.Get(3)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)

	val, ok = cache.Get(4)
	c.Assert(ok, IsFalse)
	c.Assert(val, IsNil)
}
