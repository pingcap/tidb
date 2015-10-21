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

package structure

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

func TestStructure(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStructureSuite{})

type testStructureSuite struct {
	s *TStore
}

func (s *testStructureSuite) SetUpSuite(c *C) {
	path := "memory:"
	d := localstore.Driver{
		Driver: goleveldb.MemoryDriver{},
	}
	store, err := d.Open(path)
	c.Assert(err, IsNil)
	s.s = NewStore(store, []byte{0x00})
}

func (s *testStructureSuite) TearDownSuite(c *C) {
	err := s.s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testStructureSuite) TestString(c *C) {
	tx, err := s.s.Begin()
	c.Assert(err, IsNil)

	defer tx.Rollback()

	key := []byte("a")
	value := []byte("1")
	err = tx.Set(key, value)
	c.Assert(err, IsNil)

	v, err := tx.Get(key)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, value)

	n, err := tx.Inc(key, 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	v, err = tx.Get(key)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("2"))

	n, err = tx.GetInt64(key)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	err = tx.Clear(key)
	c.Assert(err, IsNil)

	v, err = tx.Get(key)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	err = tx.Commit()
	c.Assert(err, IsNil)
}

func (s *testStructureSuite) TestList(c *C) {
	tx, err := s.s.Begin()
	c.Assert(err, IsNil)

	defer tx.Rollback()

	key := []byte("a")
	err = tx.LPush(key, []byte("3"), []byte("2"), []byte("1"))
	c.Assert(err, IsNil)

	l, err := tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(3))

	value, err := tx.LIndex(key, 1)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("2"))

	value, err = tx.LIndex(key, -1)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("3"))

	value, err = tx.LPop(key)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("1"))

	l, err = tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	err = tx.RPush(key, []byte("4"))
	c.Assert(err, IsNil)

	l, err = tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(3))

	value, err = tx.LIndex(key, -1)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("4"))

	value, err = tx.RPop(key)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("4"))

	value, err = tx.RPop(key)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("3"))

	value, err = tx.RPop(key)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("2"))

	l, err = tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(0))

	err = tx.LPush(key, []byte("1"))
	c.Assert(err, IsNil)

	err = tx.LClear(key)
	c.Assert(err, IsNil)

	l, err = tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(0))

	err = tx.Commit()
	c.Assert(err, IsNil)
}

func (s *testStructureSuite) TestHash(c *C) {
	tx, err := s.s.Begin()
	c.Assert(err, IsNil)

	defer tx.Rollback()

	key := []byte("a")

	err = tx.HSet(key, []byte("1"), []byte("1"))
	c.Assert(err, IsNil)

	err = tx.HSet(key, []byte("2"), []byte("2"))
	c.Assert(err, IsNil)

	l, err := tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	value, err := tx.HGet(key, []byte("1"))
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("1"))

	value, err = tx.HGet(key, []byte("fake"))
	c.Assert(err, IsNil)
	c.Assert(err, IsNil)

	keys, err := tx.HKeys(key)
	c.Assert(err, IsNil)
	c.Assert(keys, DeepEquals, [][]byte{[]byte("1"), []byte("2")})

	err = tx.HDel(key, []byte("1"))
	c.Assert(err, IsNil)

	value, err = tx.HGet(key, []byte("1"))
	c.Assert(err, IsNil)
	c.Assert(value, IsNil)

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(1))

	n, err := tx.HInc(key, []byte("1"), 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(1))

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	n, err = tx.HInc(key, []byte("1"), 1)
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	n, err = tx.HGetInt64(key, []byte("1"))
	c.Assert(err, IsNil)
	c.Assert(n, Equals, int64(2))

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(2))

	err = tx.HClear(key)
	c.Assert(err, IsNil)

	l, err = tx.HLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(0))

	err = tx.Commit()
	c.Assert(err, IsNil)

	fn := func(t *TStructure) error {
		err = t.Set(key, []byte("abc"))
		c.Assert(err, IsNil)

		value, err = t.Get(key)
		c.Assert(err, IsNil)
		c.Assert(value, DeepEquals, []byte("abc"))
		return nil
	}
	err = s.s.RunInNewTxn(false, fn)
	c.Assert(err, IsNil)
}
