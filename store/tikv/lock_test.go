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
	"github.com/pingcap/tidb/kv"
)

type testLockSuite struct {
	store *tikvStore
}

var _ = Suite(&testLockSuite{})

func (s *testLockSuite) SetUpTest(c *C) {
	s.store = newTestStore(c)
}

func (s *testLockSuite) TearDownTest(c *C) {
	s.store.Close()
}

func (s *testLockSuite) lockKey(c *C, key, value, primaryKey, primaryValue []byte, commitPrimary bool) {
	txn, err := newTiKVTxn(s.store)
	c.Assert(err, IsNil)
	if len(value) > 0 {
		err = txn.Set(key, value)
	} else {
		err = txn.Delete(key)
	}
	c.Assert(err, IsNil)
	if len(primaryValue) > 0 {
		err = txn.Set(primaryKey, primaryValue)
	} else {
		err = txn.Delete(primaryKey)
	}
	c.Assert(err, IsNil)
	committer, err := newTxnCommitter(txn)
	c.Assert(err, IsNil)
	committer.keys = [][]byte{primaryKey, key}

	err = committer.prewriteKeys(NewBackoffer(prewriteMaxBackoff), committer.keys)
	c.Assert(err, IsNil)

	if commitPrimary {
		committer.commitTS, err = s.store.oracle.GetTimestamp()
		c.Assert(err, IsNil)
		err = committer.commitKeys(NewBackoffer(commitMaxBackoff), [][]byte{primaryKey})
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

func (s *testLockSuite) prepareAlphabetLocks(c *C) {
	s.putKV(c, []byte("c"), []byte("cc"))
	s.lockKey(c, []byte("c"), []byte("c"), []byte("z1"), []byte("z1"), true)
	s.lockKey(c, []byte("d"), []byte("dd"), []byte("z2"), []byte("z2"), false)
	s.lockKey(c, []byte("foo"), []byte("foo"), []byte("z3"), []byte("z3"), false)
	s.putKV(c, []byte("bar"), []byte("bar"))
	s.lockKey(c, []byte("bar"), nil, []byte("z4"), []byte("z4"), true)
}

func (s *testLockSuite) TestScanLockResolveWithGet(c *C) {
	s.putAlphabets(c)
	s.prepareAlphabetLocks(c)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		v, err := txn.Get([]byte{ch})
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, []byte{ch})
	}
}

func (s *testLockSuite) TestScanLockResolveWithSeek(c *C) {
	s.putAlphabets(c)
	s.prepareAlphabetLocks(c)

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

func (s *testLockSuite) TestScanLockResolveWithBatchGet(c *C) {
	s.putAlphabets(c)
	s.prepareAlphabetLocks(c)

	var keys []kv.Key
	for ch := byte('a'); ch <= byte('z'); ch++ {
		keys = append(keys, kv.Key{ch})
	}

	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	snapshot := newTiKVSnapshot(s.store, ver)
	m, err := snapshot.BatchGet(keys)
	c.Assert(len(m), Equals, int('z'-'a'+1))
	for ch := byte('a'); ch <= byte('z'); ch++ {
		k := []byte{ch}
		c.Assert(m[string(k)], BytesEquals, k)
	}
}

func (s *testLockSuite) TestCleanLock(c *C) {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		k := []byte{ch}
		s.lockKey(c, k, k, k, k, false)
	}
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		err = txn.Set([]byte{ch}, []byte{ch + 1})
		c.Assert(err, IsNil)
	}
	err = txn.Commit()
	c.Assert(err, IsNil)
}

func init() {
	// Speed up tests.
	lockTTL = 3
	oracleUpdateInterval = 2
}
