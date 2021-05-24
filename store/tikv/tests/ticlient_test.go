// Copyright 2021 PingCAP, Inc.
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

package tikv_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	tikvstore "github.com/pingcap/tidb/store/tikv/kv"
)

type testTiclientSuite struct {
	OneByOneSuite
	store *tikv.KVStore
	// prefix is prefix of each key in this test. It is used for table isolation,
	// or it may pollute other data.
	prefix string
}

var _ = Suite(&testTiclientSuite{})

func (s *testTiclientSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.store = NewTestStore(c)
	s.prefix = fmt.Sprintf("ticlient_%d", time.Now().Unix())
}

func (s *testTiclientSuite) TearDownSuite(c *C) {
	// Clean all data, or it may pollute other data.
	txn := s.beginTxn(c)
	scanner, err := txn.Iter(encodeKey(s.prefix, ""), nil)
	c.Assert(err, IsNil)
	c.Assert(scanner, NotNil)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		c.Assert(err, IsNil)
		scanner.Next()
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = s.store.Close()
	c.Assert(err, IsNil)
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testTiclientSuite) beginTxn(c *C) *tikv.KVTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn
}

func (s *testTiclientSuite) TestSingleKey(c *C) {
	txn := s.beginTxn(c)
	err := txn.Set(encodeKey(s.prefix, "key"), []byte("value"))
	c.Assert(err, IsNil)
	err = txn.LockKeys(context.Background(), new(tikvstore.LockCtx), encodeKey(s.prefix, "key"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	txn = s.beginTxn(c)
	val, err := txn.Get(context.TODO(), encodeKey(s.prefix, "key"))
	c.Assert(err, IsNil)
	c.Assert(val, BytesEquals, []byte("value"))

	txn = s.beginTxn(c)
	err = txn.Delete(encodeKey(s.prefix, "key"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testTiclientSuite) TestMultiKeys(c *C) {
	const keyNum = 100

	txn := s.beginTxn(c)
	for i := 0; i < keyNum; i++ {
		err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
		c.Assert(err, IsNil)
	}
	err := txn.Commit(context.Background())
	c.Assert(err, IsNil)

	txn = s.beginTxn(c)
	for i := 0; i < keyNum; i++ {
		val, err1 := txn.Get(context.TODO(), encodeKey(s.prefix, s08d("key", i)))
		c.Assert(err1, IsNil)
		c.Assert(val, BytesEquals, valueBytes(i))
	}

	txn = s.beginTxn(c)
	for i := 0; i < keyNum; i++ {
		err = txn.Delete(encodeKey(s.prefix, s08d("key", i)))
		c.Assert(err, IsNil)
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testTiclientSuite) TestNotExist(c *C) {
	txn := s.beginTxn(c)
	_, err := txn.Get(context.TODO(), encodeKey(s.prefix, "noSuchKey"))
	c.Assert(err, NotNil)
}

func (s *testTiclientSuite) TestLargeRequest(c *C) {
	largeValue := make([]byte, 9*1024*1024) // 9M value.
	txn := s.beginTxn(c)
	txn.GetUnionStore().SetEntrySizeLimit(1024*1024, 100*1024*1024)
	err := txn.Set([]byte("key"), largeValue)
	c.Assert(err, NotNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	c.Assert(kv.IsTxnRetryableError(err), IsFalse)
}
