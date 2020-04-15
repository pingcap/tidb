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

package tikv

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/v4/util/codec"
)

var (
	withTiKVGlobalLock sync.RWMutex
	WithTiKV           = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
	pdAddrs            = flag.String("pd-addrs", "127.0.0.1:2379", "pd addrs")
)

// NewTestStore creates a kv.Storage for testing purpose.
func NewTestStore(c *C) kv.Storage {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *WithTiKV {
		var d Driver
		store, err := d.Open(fmt.Sprintf("tikv://%s", *pdAddrs))
		c.Assert(err, IsNil)
		err = clearStorage(store)
		c.Assert(err, IsNil)
		return store
	}

	client, pdClient, err := mocktikv.NewTiKVAndPDClient(nil, nil, "")
	c.Assert(err, IsNil)

	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
	return store
}

func clearStorage(store kv.Storage) error {
	txn, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	iter, err := txn.Iter(nil, nil)
	if err != nil {
		return errors.Trace(err)
	}
	for iter.Valid() {
		txn.Delete(iter.Key())
		if err := iter.Next(); err != nil {
			return errors.Trace(err)
		}
	}
	return txn.Commit(context.Background())
}

type testTiclientSuite struct {
	OneByOneSuite
	store *tikvStore
	// prefix is prefix of each key in this test. It is used for table isolation,
	// or it may pollute other data.
	prefix string
}

var _ = Suite(&testTiclientSuite{})

func (s *testTiclientSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.store = NewTestStore(c).(*tikvStore)
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

func (s *testTiclientSuite) beginTxn(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testTiclientSuite) TestSingleKey(c *C) {
	txn := s.beginTxn(c)
	err := txn.Set(encodeKey(s.prefix, "key"), []byte("value"))
	c.Assert(err, IsNil)
	err = txn.LockKeys(context.Background(), new(kv.LockCtx), encodeKey(s.prefix, "key"))
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
	err := txn.Set([]byte("key"), largeValue)
	c.Assert(err, NotNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	c.Assert(kv.IsTxnRetryableError(err), IsFalse)
}

func encodeKey(prefix, s string) []byte {
	return codec.EncodeBytes(nil, []byte(fmt.Sprintf("%s_%s", prefix, s)))
}

func valueBytes(n int) []byte {
	return []byte(fmt.Sprintf("value%d", n))
}

// s08d is for returning format string "%s%08d" to keep string sorted.
// e.g.: "0002" < "0011", otherwise "2" > "11"
func s08d(prefix string, n int) string {
	return fmt.Sprintf("%s%08d", prefix, n)
}
