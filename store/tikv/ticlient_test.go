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
	"flag"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/codec"
)

var (
	withTiKV     = flag.Bool("with-tikv", false, "run tests with TiKV cluster started. (not use the mock server)")
	etcdAddrs    = flag.String("etcd-addrs", "127.0.0.1:2379", "etcd addrs")
	pdLeaderPath = flag.String("pd-path", "/pd", "PD leader path on etcd")
	clusterID    = flag.Int("cluster", 1, "cluster ID")
)

func newTestStore(c *C) *tikvStore {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *withTiKV {
		var d Driver
		store, err := d.Open(fmt.Sprintf("tikv://%s%s?cluster=%d", *etcdAddrs, *pdLeaderPath, *clusterID))
		c.Assert(err, IsNil)
		return store.(*tikvStore)
	}
	return NewMockTikvStore().(*tikvStore)
}

type testTiclientSuite struct {
	store *tikvStore
	// prefix is prefix of each key in this test. It is used for table isolation,
	// or it may pollute other data.
	prefix string
}

var _ = Suite(&testTiclientSuite{})

func (s *testTiclientSuite) SetUpSuite(c *C) {
	s.store = newTestStore(c)
	s.prefix = fmt.Sprintf("ticlient_%d", time.Now().Unix())
}

func (s *testTiclientSuite) TearDownSuite(c *C) {
	// Clean all data, or it may pollute other data.
	txn := s.beginTxn(c)
	scanner, err := txn.Seek(encodeKey(s.prefix, ""))
	c.Assert(err, IsNil)
	c.Assert(scanner, NotNil)
	for scanner.Valid() {
		k := scanner.Key()
		err = txn.Delete(k)
		c.Assert(err, IsNil)
		scanner.Next()
	}
	err = txn.Commit()
	c.Assert(err, IsNil)
	err = s.store.Close()
	c.Assert(err, IsNil)
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
	err = txn.LockKeys(encodeKey(s.prefix, "key"))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)

	txn = s.beginTxn(c)
	val, err := txn.Get(encodeKey(s.prefix, "key"))
	c.Assert(err, IsNil)
	c.Assert(val, BytesEquals, []byte("value"))

	txn = s.beginTxn(c)
	err = txn.Delete(encodeKey(s.prefix, "key"))
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testTiclientSuite) TestMultiKeys(c *C) {
	const keyNum = 100

	txn := s.beginTxn(c)
	for i := 0; i < keyNum; i++ {
		err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
		c.Assert(err, IsNil)
	}
	err := txn.Commit()
	c.Assert(err, IsNil)

	txn = s.beginTxn(c)
	for i := 0; i < keyNum; i++ {
		val, err1 := txn.Get(encodeKey(s.prefix, s08d("key", i)))
		c.Assert(err1, IsNil)
		c.Assert(val, BytesEquals, valueBytes(i))
	}

	txn = s.beginTxn(c)
	for i := 0; i < keyNum; i++ {
		err = txn.Delete(encodeKey(s.prefix, s08d("key", i)))
		c.Assert(err, IsNil)
	}
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testTiclientSuite) TestCleanLock(c *C) {
	const keyNum = 10

	txn := s.beginTxn(c)
	for i := 0; i < keyNum; i++ {
		err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
		c.Assert(err, IsNil)
	}
	txn.DONOTCOMMIT = true
	err := txn.Commit()
	c.Assert(err, IsNil)

	txn2 := s.beginTxn(c)
	for i := 0; i < keyNum; i++ {
		err2 := txn2.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i+1))
		c.Assert(err2, IsNil)
	}
	err2 := txn2.Commit()
	c.Assert(err2, IsNil)
}

func (s *testTiclientSuite) TestNotExist(c *C) {
	txn := s.beginTxn(c)
	_, err := txn.Get(encodeKey(s.prefix, "noSuchKey"))
	c.Assert(err, NotNil)
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
