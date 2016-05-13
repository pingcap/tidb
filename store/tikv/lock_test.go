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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
)

type testLockSuite struct {
	store   *tikvStore
	cluster *mocktikv.Cluster
}

var _ = Suite(&testLockSuite{})

func (s *testLockSuite) SetUpTest(c *C) {
	s.store, s.cluster = createMockStoreCluster()
}

func (s *testLockSuite) lockKey(c *C, key, value, primaryKey, primaryValue []byte, commitPrimary bool) {
	txn, err := newTiKVTxn(s.store)
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
	c.Assert(err, IsNil)
	err = txn.Set(primaryKey, primaryValue)
	c.Assert(err, IsNil)
	committer, err := newTxnCommitter(txn)
	c.Assert(err, IsNil)
	committer.keys = [][]byte{primaryKey, key}

	err = committer.prewriteKeys(committer.keys)
	c.Assert(err, IsNil)

	if commitPrimary {
		committer.commitTS, err = s.store.oracle.GetTimestamp()
		c.Assert(err, IsNil)
		err = committer.commitKeys([][]byte{primaryKey})
		c.Assert(err, IsNil)
	}
}

func (s *testLockSuite) putAlphabets(c *C) {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.putKV(c, []byte{ch}, []byte{ch})
	}
}

func (s *testLockSuite) putKV(c *C, key, value []byte) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func (s *testLockSuite) TestScanLockResolve(c *C) {
	mocktikv.BootstrapWithSingleStore(s.cluster)
	s.putAlphabets(c)
	s.putKV(c, []byte("c"), []byte("cc"))
	s.lockKey(c, []byte("c"), []byte("c"), []byte("zz"), []byte("zz"), true)
	s.lockKey(c, []byte("d"), []byte("dd"), []byte("zz"), []byte("zz"), false)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	iter, err := txn.Seek([]byte("a"))
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		c.Assert(iter.Valid(), IsTrue)
		c.Assert([]byte(iter.Key()), BytesEquals, []byte{ch})
		c.Assert([]byte(iter.Value()), BytesEquals, []byte{ch})
		c.Assert(iter.Next(), IsNil)
	}
}
