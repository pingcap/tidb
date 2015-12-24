// Copyright 2015 PingCAP, Inc.
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

package kv

import (
	"github.com/juju/errors"
	. "github.com/pingcap/check"
)

var _ = Suite(&testCacheSnapshotSuite{})

type testCacheSnapshotSuite struct {
	store              MemBuffer
	lazyConditionPairs MemBuffer
	cache              Snapshot
}

func (s *testCacheSnapshotSuite) SetUpTest(c *C) {
	s.store = NewMemDbBuffer()
	s.lazyConditionPairs = NewMemDbBuffer()
	s.cache = NewCacheSnapshot(&mockSnapshot{s.store}, s.lazyConditionPairs, &mockOptions{})
}

func (s *testCacheSnapshotSuite) TearDownTest(c *C) {
	s.cache.Release()
	s.lazyConditionPairs.Release()
}

func (s *testCacheSnapshotSuite) TestGet(c *C) {
	s.store.Set([]byte("1"), []byte("1"))
	s.store.Set([]byte("2"), []byte("2"))

	v, err := s.cache.Get([]byte("1"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("1"))

	s.store.Set([]byte("1"), []byte("3"))
	v, err = s.cache.Get([]byte("1"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("1"))

	s.store.Set([]byte("2"), []byte("4"))
	v, err = s.cache.Get([]byte("2"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("4"))
}

func (s *testCacheSnapshotSuite) TestBatchGet(c *C) {
	s.store.Set([]byte("1"), []byte("1"))
	s.store.Set([]byte("2"), []byte("2"))

	m, err := s.cache.BatchGet([]Key{[]byte("1"), []byte("2"), []byte("3")})
	c.Assert(err, IsNil)
	c.Assert(m["1"], BytesEquals, []byte("1"))
	c.Assert(m["2"], BytesEquals, []byte("2"))
	_, exist := m["3"]
	c.Assert(exist, IsFalse)

	// result should be saved in cache
	s.store.Set([]byte("1"), []byte("4"))
	v, err := s.cache.Get([]byte("1"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("1"))

	// nil result should also be saved in cache
	s.store.Set([]byte("3"), []byte("3"))
	m, err = s.cache.BatchGet([]Key{[]byte("3")})
	c.Assert(err, IsNil)
	_, exist = m["3"]
	c.Assert(exist, IsFalse)
}

type mockSnapshot struct {
	store MemBuffer
}

func (s *mockSnapshot) Get(k Key) ([]byte, error) {
	return s.store.Get(k)
}

func (s *mockSnapshot) BatchGet(keys []Key) (map[string][]byte, error) {
	m := make(map[string][]byte)
	for _, k := range keys {
		v, err := s.store.Get(k)
		if IsErrNotFound(err) {
			continue
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		m[string(k)] = v
	}
	return m, nil
}

func (s *mockSnapshot) Seek(k Key) (Iterator, error) {
	return s.store.Seek(k)
}

func (s *mockSnapshot) Release() {
	s.store.Release()
}

type mockOptions map[Option]interface{}

func (opts mockOptions) Get(opt Option) (interface{}, bool) {
	v, ok := opts[opt]
	return v, ok
}
