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
	"bytes"

	. "github.com/pingcap/check"
)

var _ = Suite(&testUnionStoreSuite{})

type testUnionStoreSuite struct {
	k1, k2, k3, k4 []byte
	v1, v2, v3, v4 []byte

	store MemBuffer
	us    UnionStore
}

func (s *testUnionStoreSuite) SetUpTest(c *C) {
	s.k1 = []byte("k1")
	s.k2 = []byte("k2")
	s.k3 = []byte("k3")
	s.k4 = []byte("k4")
	s.v1 = []byte("1")
	s.v2 = []byte("2")
	s.v3 = []byte("3")
	s.v4 = []byte("4")

	s.store = NewMemDbBuffer()
	s.us = NewUnionStore(&mockSnapshot{s.store})
}

func (s *testUnionStoreSuite) TearDownTest(c *C) {
	s.us.Close()
}

func (s *testUnionStoreSuite) TestGetSet(c *C) {
	s.store.Set(s.k1, s.v1)
	v, err := s.us.Get(s.k1)
	c.Assert(err, IsNil)
	c.Assert(bytes.Compare(v, s.v1), Equals, 0)
	s.us.Set(s.k1, s.v2)
	v, err = s.us.Get(s.k1)
	c.Assert(err, IsNil)
	c.Assert(bytes.Compare(v, s.v2), Equals, 0)
}

func (s *testUnionStoreSuite) TestDelete(c *C) {
	s.store.Set(s.k1, s.v1)
	err := s.us.Delete(s.k1)
	c.Assert(err, IsNil)
	_, err = s.us.Get(s.k1)
	c.Assert(IsErrNotFound(err), IsTrue)

	s.us.Set(s.k1, s.v2)
	v, err := s.us.Get(s.k1)
	c.Assert(err, IsNil)
	c.Assert(bytes.Compare(v, s.v2), Equals, 0)
}

func (s *testUnionStoreSuite) TestSeek(c *C) {
	s.store.Set(s.k1, s.v1)
	s.store.Set(s.k2, s.v2)
	s.store.Set(s.k3, s.v3)

	iter, err := s.us.Seek(nil, nil)
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{s.k1, s.k2, s.k3}, [][]byte{s.v1, s.v2, s.v3})

	iter, err = s.us.Seek(s.k2, nil)
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{s.k2, s.k3}, [][]byte{s.v2, s.v3})

	s.us.Set(s.k4, s.v4)
	iter, err = s.us.Seek(s.k2, nil)
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{s.k2, s.k3, s.k4}, [][]byte{s.v2, s.v3, s.v4})

	s.us.Delete(s.k3)
	iter, err = s.us.Seek(s.k2, nil)
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{s.k2, s.k4}, [][]byte{s.v2, s.v4})
}

func checkIterator(c *C, iter Iterator, keys [][]byte, values [][]byte) {
	defer iter.Close()
	c.Assert(len(keys), Equals, len(values))
	for i, k := range keys {
		v := values[i]
		c.Assert(iter.Valid(), IsTrue)
		c.Assert(iter.Key(), Equals, string(k))
		c.Assert(bytes.Compare(iter.Value(), v), Equals, 0)
		c.Assert(iter.Next(), IsNil)
	}
	c.Assert(iter.Valid(), IsFalse)
}
