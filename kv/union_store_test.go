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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testUnionStoreSuite{})

type testUnionStoreSuite struct {
	store MemBuffer
	us    UnionStore
}

func (s *testUnionStoreSuite) SetUpTest(c *C) {
	s.store = NewMemDbBuffer(DefaultTxnMembufCap)
	s.us = NewUnionStore(&mockSnapshot{s.store})
}

func (s *testUnionStoreSuite) TestGetSet(c *C) {
	defer testleak.AfterTest(c)()
	s.store.Set([]byte("1"), []byte("1"))
	v, err := s.us.Get([]byte("1"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("1"))
	s.us.Set([]byte("1"), []byte("2"))
	v, err = s.us.Get([]byte("1"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("2"))
}

func (s *testUnionStoreSuite) TestDelete(c *C) {
	defer testleak.AfterTest(c)()
	s.store.Set([]byte("1"), []byte("1"))
	err := s.us.Delete([]byte("1"))
	c.Assert(err, IsNil)
	_, err = s.us.Get([]byte("1"))
	c.Assert(IsErrNotFound(err), IsTrue)

	s.us.Set([]byte("1"), []byte("2"))
	v, err := s.us.Get([]byte("1"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("2"))
}

func (s *testUnionStoreSuite) TestSeek(c *C) {
	defer testleak.AfterTest(c)()
	s.store.Set([]byte("1"), []byte("1"))
	s.store.Set([]byte("2"), []byte("2"))
	s.store.Set([]byte("3"), []byte("3"))

	iter, err := s.us.Seek(nil)
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{[]byte("1"), []byte("2"), []byte("3")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")})

	iter, err = s.us.Seek([]byte("2"))
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{[]byte("2"), []byte("3")}, [][]byte{[]byte("2"), []byte("3")})

	s.us.Set([]byte("4"), []byte("4"))
	iter, err = s.us.Seek([]byte("2"))
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{[]byte("2"), []byte("3"), []byte("4")}, [][]byte{[]byte("2"), []byte("3"), []byte("4")})

	s.us.Delete([]byte("3"))
	iter, err = s.us.Seek([]byte("2"))
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{[]byte("2"), []byte("4")}, [][]byte{[]byte("2"), []byte("4")})
}

func (s *testUnionStoreSuite) TestSeekReverse(c *C) {
	defer testleak.AfterTest(c)()
	s.store.Set([]byte("1"), []byte("1"))
	s.store.Set([]byte("2"), []byte("2"))
	s.store.Set([]byte("3"), []byte("3"))

	iter, err := s.us.SeekReverse(nil)
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{[]byte("3"), []byte("2"), []byte("1")}, [][]byte{[]byte("3"), []byte("2"), []byte("1")})

	iter, err = s.us.SeekReverse([]byte("3"))
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{[]byte("2"), []byte("1")}, [][]byte{[]byte("2"), []byte("1")})

	s.us.Set([]byte("0"), []byte("0"))
	iter, err = s.us.SeekReverse([]byte("3"))
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{[]byte("2"), []byte("1"), []byte("0")}, [][]byte{[]byte("2"), []byte("1"), []byte("0")})

	s.us.Delete([]byte("1"))
	iter, err = s.us.SeekReverse([]byte("3"))
	c.Assert(err, IsNil)
	checkIterator(c, iter, [][]byte{[]byte("2"), []byte("0")}, [][]byte{[]byte("2"), []byte("0")})
}

func (s *testUnionStoreSuite) TestLazyConditionCheck(c *C) {
	defer testleak.AfterTest(c)()
	s.store.Set([]byte("1"), []byte("1"))
	s.store.Set([]byte("2"), []byte("2"))

	v, err := s.us.Get([]byte("1"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("1"))

	s.us.SetOption(PresumeKeyNotExists, nil)
	s.us.SetOption(PresumeKeyNotExistsError, ErrNotExist)
	_, err = s.us.Get([]byte("2"))
	c.Assert(terror.ErrorEqual(err, ErrNotExist), IsTrue)

	err = s.us.CheckLazyConditionPairs()
	c.Assert(err, NotNil)
}

func checkIterator(c *C, iter Iterator, keys [][]byte, values [][]byte) {
	defer iter.Close()
	c.Assert(len(keys), Equals, len(values))
	for i, k := range keys {
		v := values[i]
		c.Assert(iter.Valid(), IsTrue)
		c.Assert([]byte(iter.Key()), BytesEquals, k)
		c.Assert(iter.Value(), BytesEquals, v)
		c.Assert(iter.Next(), IsNil)
	}
	c.Assert(iter.Valid(), IsFalse)
}
