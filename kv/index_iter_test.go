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

package kv_test

import (
	"io"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testIndexSuite{})

type testIndexSuite struct {
	s kv.Storage
}

func (s *testIndexSuite) SetUpSuite(c *C) {
	path := "memory:"
	d := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := d.Open(path)
	c.Assert(err, IsNil)
	s.s = store
}

func (s *testIndexSuite) TearDownSuite(c *C) {
	err := s.s.Close()
	c.Assert(err, IsNil)
}

func (s *testIndexSuite) TestIndex(c *C) {
	index := kv.NewKVIndex([]byte("i"), "test", 0, false)

	// Test ununiq index.
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	values := types.MakeDatums(1, 2)
	err = index.Create(txn, values, 1)
	c.Assert(err, IsNil)

	it, err := index.SeekFirst(txn)
	c.Assert(err, IsNil)

	getValues, h, err := it.Next()
	c.Assert(err, IsNil)
	c.Assert(getValues, HasLen, 2)
	c.Assert(getValues[0].GetInt64(), Equals, int64(1))
	c.Assert(getValues[1].GetInt64(), Equals, int64(2))
	c.Assert(h, Equals, int64(1))
	it.Close()

	exist, _, err := index.Exist(txn, values, 100)
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	exist, _, err = index.Exist(txn, values, 1)
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	err = index.Delete(txn, values, 1)
	c.Assert(err, IsNil)

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue)
	it.Close()

	err = index.Create(txn, values, 0)
	c.Assert(err, IsNil)

	_, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, hit, err := index.Seek(txn, values)
	c.Assert(err, IsNil)
	c.Assert(hit, IsTrue)

	err = index.Drop(txn)
	c.Assert(err, IsNil)

	it, hit, err = index.Seek(txn, values)
	c.Assert(err, IsNil)
	c.Assert(hit, IsFalse)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue)
	it.Close()

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue)
	it.Close()

	err = txn.Commit()
	c.Assert(err, IsNil)

	index = kv.NewKVIndex([]byte("j"), "test", 1, true)

	// Test uniq index.
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	err = index.Create(txn, values, 1)
	c.Assert(err, IsNil)

	err = index.Create(txn, values, 2)
	c.Assert(err, NotNil)

	exist, h, err = index.Exist(txn, values, 1)
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(1))
	c.Assert(exist, IsTrue)

	exist, h, err = index.Exist(txn, values, 2)
	c.Assert(err, NotNil)
	c.Assert(h, Equals, int64(1))
	c.Assert(exist, IsTrue)

	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testIndexSuite) TestCombineIndexSeek(c *C) {
	index := kv.NewKVIndex([]byte("i"), "test", 1, false)

	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	values := types.MakeDatums("abc", "def")
	err = index.Create(txn, values, 1)
	c.Assert(err, IsNil)

	index2 := kv.NewKVIndex([]byte("i"), "test", 1, false)
	iter, hit, err := index2.Seek(txn, types.MakeDatums("abc", nil))
	c.Assert(err, IsNil)
	defer iter.Close()
	c.Assert(hit, IsFalse)
	_, h, err := iter.Next()
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(1))
}
