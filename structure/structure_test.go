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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
)

func TestTxStructure(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&tesTxStructureSuite{})

type tesTxStructureSuite struct {
	store kv.Storage
}

func (s *tesTxStructureSuite) SetUpSuite(c *C) {
	path := "memory:"
	d := localstore.Driver{
		Driver: goleveldb.MemoryDriver{},
	}
	store, err := d.Open(path)
	c.Assert(err, IsNil)
	s.store = store
}

func (s *tesTxStructureSuite) TearDownSuite(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *tesTxStructureSuite) TestString(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	tx := NewStructure(txn, []byte{0x00})

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

	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *tesTxStructureSuite) TestList(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	tx := NewStructure(txn, []byte{0x00})

	key := []byte("a")
	err = tx.LPush(key, []byte("3"), []byte("2"), []byte("1"))
	c.Assert(err, IsNil)

	l, err := tx.LLen(key)
	c.Assert(err, IsNil)
	c.Assert(l, Equals, int64(3))

	value, err := tx.LIndex(key, 1)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("2"))

	err = tx.LSet(key, 1, []byte("4"))
	c.Assert(err, IsNil)

	value, err = tx.LIndex(key, 1)
	c.Assert(err, IsNil)
	c.Assert(value, DeepEquals, []byte("4"))

	err = tx.LSet(key, 1, []byte("2"))
	c.Assert(err, IsNil)

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

	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *tesTxStructureSuite) TestHash(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	defer txn.Rollback()

	tx := NewStructure(txn, []byte{0x00})

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

	res, err := tx.HGetAll(key)
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []HashPair{
		{[]byte("1"), []byte("1")},
		{[]byte("2"), []byte("2")}})

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

	err = tx.HDel(key, []byte("fake_key"))
	c.Assert(err, IsNil)

	err = txn.Commit()
	c.Assert(err, IsNil)

	err = kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		t := NewStructure(txn, []byte{0x00})
		err = t.Set(key, []byte("abc"))
		c.Assert(err, IsNil)

		value, err = t.Get(key)
		c.Assert(err, IsNil)
		c.Assert(value, DeepEquals, []byte("abc"))
		return nil
	})
	c.Assert(err, IsNil)
}
