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
	k1, k2, k3, k4 []byte
	v1, v2, v3, v4 []byte
	store          MemBuffer
	cache          Snapshot
}

func (s *testCacheSnapshotSuite) SetUpTest(c *C) {
	s.k1 = []byte("k1")
	s.k2 = []byte("k2")
	s.k3 = []byte("k3")
	s.k4 = []byte("k4")
	s.v1 = []byte("1")
	s.v2 = []byte("2")
	s.v3 = []byte("3")
	s.v4 = []byte("4")

	s.store = NewMemDbBuffer()
	s.cache = NewCacheSnapshot(&mockSnapshot{s.store})
}

func (s *testCacheSnapshotSuite) TearDownTest(c *C) {
	s.cache.Release()
}

func (s *testCacheSnapshotSuite) TestGet(c *C) {
	s.store.Set(s.k1, s.v1)
	s.store.Set(s.k2, s.v2)

	v, err := s.cache.Get(s.k1)
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, s.v1)

	s.store.Set(s.k1, s.v3)
	v, err = s.cache.Get(s.k1)
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, s.v1)

	s.store.Set(s.k2, s.v4)
	v, err = s.cache.Get(s.k2)
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, s.v4)
}

func (s *testCacheSnapshotSuite) TestBatchGet(c *C) {
	s.store.Set(s.k1, s.v1)
	s.store.Set(s.k2, s.v2)

	m, err := s.cache.BatchGet([]Key{s.k1, s.k2, s.k3})
	c.Assert(err, IsNil)
	c.Assert(m[string(s.k1)], BytesEquals, s.v1)
	c.Assert(m[string(s.k2)], BytesEquals, s.v2)
	_, exist := m[string(s.k3)]
	c.Assert(exist, IsFalse)

	// result should be saved in cache
	s.store.Set(s.k1, s.v4)
	v, err := s.cache.Get(s.k1)
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, s.v1)
}

func (s *testCacheSnapshotSuite) TestRangeGet(c *C) {
	s.store.Set(s.k1, s.v1)
	s.store.Set(s.k2, s.v2)
	s.store.Set(s.k3, s.v3)

	m, err := s.cache.RangeGet(s.k1, s.k2, 100)
	c.Assert(err, IsNil)
	c.Assert(m, HasLen, 2)
	c.Assert(m[string(s.k1)], BytesEquals, s.v1)
	c.Assert(m[string(s.k2)], BytesEquals, s.v2)

	// result should be saved in cache
	s.store.Set(s.k1, s.v4)
	v, err := s.cache.Get(s.k1)
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, s.v1)
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

func (s *mockSnapshot) RangeGet(start, end Key, limit int) (map[string][]byte, error) {
	m := make(map[string][]byte)
	it := s.NewIterator([]byte(start))
	defer it.Close()
	endKey := string(end)
	for i := 0; i < limit; i++ {
		if !it.Valid() {
			break
		}
		if it.Key() > endKey {
			break
		}
		m[string(it.Key())] = it.Value()
		err := it.Next()
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (s *mockSnapshot) NewIterator(param interface{}) Iterator {
	return s.store.NewIterator(param)
}

func (s *mockSnapshot) Release() {
	s.store.Release()
}
