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
	"bytes"
	"context"
	"math"
	"runtime"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type testLockSuite struct {
	OneByOneSuite
	store *tikvStore
}

var _ = Suite(&testLockSuite{})

func (s *testLockSuite) SetUpTest(c *C) {
	s.store = NewTestStore(c).(*tikvStore)
}

func (s *testLockSuite) TearDownTest(c *C) {
	s.store.Close()
}

func (s *testLockSuite) lockKey(c *C, key, value, primaryKey, primaryValue []byte, commitPrimary bool) (uint64, uint64) {
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
	tpc, err := newTwoPhaseCommitterWithInit(txn, 0)
	tpc.primaryKey = primaryKey
	c.Assert(err, IsNil)
	if bytes.Equal(key, primaryKey) {
		tpc.keys = [][]byte{primaryKey}
	} else {
		tpc.keys = [][]byte{primaryKey, key}
	}

	ctx := context.Background()
	err = tpc.prewriteKeys(NewBackoffer(ctx, prewriteMaxBackoff), tpc.keys)
	c.Assert(err, IsNil)

	if commitPrimary {
		tpc.commitTS, err = s.store.oracle.GetTimestamp(ctx)
		c.Assert(err, IsNil)
		err = tpc.commitKeys(NewBackoffer(ctx, CommitMaxBackoff), [][]byte{primaryKey})
		c.Assert(err, IsNil)
	}
	return txn.startTS, tpc.commitTS
}

func (s *testLockSuite) putAlphabets(c *C) {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.putKV(c, []byte{ch}, []byte{ch})
	}
}

func (s *testLockSuite) putKV(c *C, key, value []byte) (uint64, uint64) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	return txn.StartTS(), txn.(*tikvTxn).commitTS
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
	iter, err := txn.Iter([]byte("a"), nil)
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		c.Assert(iter.Valid(), IsTrue)
		c.Assert([]byte(iter.Key()), BytesEquals, []byte{ch})
		c.Assert([]byte(iter.Value()), BytesEquals, []byte{ch})
		c.Assert(iter.Next(), IsNil)
	}
}

func (s *testLockSuite) TestScanLockResolveWithSeekKeyOnly(c *C) {
	s.putAlphabets(c)
	s.prepareAlphabetLocks(c)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.SetOption(kv.KeyOnly, true)
	iter, err := txn.Iter([]byte("a"), nil)
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		c.Assert(iter.Valid(), IsTrue)
		c.Assert([]byte(iter.Key()), BytesEquals, []byte{ch})
		c.Assert(iter.Next(), IsNil)
	}
}

func (s *testLockSuite) TestScanLockResolveWithBatchGet(c *C) {
	s.putAlphabets(c)
	s.prepareAlphabetLocks(c)

	var keys []kv.Key
	for ch := byte('a'); ch <= byte('z'); ch++ {
		keys = append(keys, []byte{ch})
	}

	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	snapshot := newTiKVSnapshot(s.store, ver)
	m, err := snapshot.BatchGet(keys)
	c.Assert(err, IsNil)
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
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testLockSuite) TestGetTxnStatus(c *C) {
	startTS, commitTS := s.putKV(c, []byte("a"), []byte("a"))
	status, err := s.store.lockResolver.GetTxnStatus(startTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Equals, commitTS)

	startTS, commitTS = s.lockKey(c, []byte("a"), []byte("a"), []byte("a"), []byte("a"), true)
	status, err = s.store.lockResolver.GetTxnStatus(startTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Equals, commitTS)

	startTS, _ = s.lockKey(c, []byte("a"), []byte("a"), []byte("a"), []byte("a"), false)
	status, err = s.store.lockResolver.GetTxnStatus(startTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsFalse)
	c.Assert(status.ttl, Greater, uint64(0))
}

func (s *testLockSuite) TestCheckTxnStatusTTL(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("key"), []byte("value"))
	s.prewriteTxn(c, txn.(*tikvTxn))

	// Check the lock TTL of a transaction.
	bo := NewBackoffer(context.Background(), prewriteMaxBackoff)
	lr := newLockResolver(s.store)
	status, err := lr.GetTxnStatus(txn.StartTS(), []byte("key"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsFalse)
	c.Assert(status.ttl, Greater, uint64(0))
	c.Assert(status.CommitTS(), Equals, uint64(0))

	// Rollback the txn.
	lock := s.mustGetLock(c, []byte("key"))
	status = TxnStatus{}
	cleanRegions := make(map[RegionVerID]struct{})
	err = newLockResolver(s.store).resolveLock(bo, lock, status, cleanRegions)
	c.Assert(err, IsNil)

	// Check its status is rollbacked.
	status, err = lr.GetTxnStatus(txn.StartTS(), []byte("key"))
	c.Assert(err, IsNil)
	c.Assert(status.ttl, Equals, uint64(0))
	c.Assert(status.commitTS, Equals, uint64(0))

	// Check a committed txn.
	startTS, commitTS := s.putKV(c, []byte("a"), []byte("a"))
	status, err = lr.GetTxnStatus(startTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.ttl, Equals, uint64(0))
	c.Assert(status.commitTS, Equals, commitTS)
}

func (s *testLockSuite) TestTxnHeartBeat(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("key"), []byte("value"))
	s.prewriteTxn(c, txn.(*tikvTxn))

	bo := NewBackoffer(context.Background(), prewriteMaxBackoff)
	newTTL, err := sendTxnHeartBeat(bo, s.store, []byte("key"), txn.StartTS(), 666)
	c.Assert(err, IsNil)
	c.Assert(newTTL, Equals, uint64(666))

	newTTL, err = sendTxnHeartBeat(bo, s.store, []byte("key"), txn.StartTS(), 555)
	c.Assert(err, IsNil)
	c.Assert(newTTL, Equals, uint64(666))

	// The getTxnStatus API is confusing, it really means rollback!
	status, err := newLockResolver(s.store).getTxnStatus(bo, txn.StartTS(), []byte("key"), 0)
	c.Assert(err, IsNil)
	c.Assert(status.ttl, Equals, uint64(0))
	c.Assert(status.commitTS, Equals, uint64(0))

	newTTL, err = sendTxnHeartBeat(bo, s.store, []byte("key"), txn.StartTS(), 666)
	c.Assert(err, NotNil)
	c.Assert(newTTL, Equals, uint64(0))
}

func (s *testLockSuite) prewriteTxn(c *C, txn *tikvTxn) {
	committer, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	err = committer.prewriteKeys(NewBackoffer(context.Background(), prewriteMaxBackoff), committer.keys)
	c.Assert(err, IsNil)
}

func (s *testLockSuite) mustGetLock(c *C, key []byte) *Lock {
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	bo := NewBackoffer(context.Background(), getMaxBackoff)
	req := &tikvrpc.Request{
		Type: tikvrpc.CmdGet,
		Get: &kvrpcpb.GetRequest{
			Key:     key,
			Version: ver.Ver,
		},
	}
	loc, err := s.store.regionCache.LocateKey(bo, key)
	c.Assert(err, IsNil)
	resp, err := s.store.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	cmdGetResp := resp.Get
	c.Assert(cmdGetResp, NotNil)
	keyErr := cmdGetResp.GetError()
	c.Assert(keyErr, NotNil)
	lock, err := extractLockFromKeyErr(keyErr)
	c.Assert(err, IsNil)
	return lock
}

func (s *testLockSuite) ttlEquals(c *C, x, y uint64) {
	// NOTE: On ppc64le, all integers are by default unsigned integers,
	// hence we have to separately cast the value returned by "math.Abs()" function for ppc64le.
	if runtime.GOARCH == "ppc64le" {
		c.Assert(int(-math.Abs(float64(x-y))), LessEqual, 2)
	} else {
		c.Assert(int(math.Abs(float64(x-y))), LessEqual, 2)
	}

}

func (s *testLockSuite) TestLockTTL(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("key"), []byte("value"))
	time.Sleep(time.Millisecond)
	s.prewriteTxn(c, txn.(*tikvTxn))
	l := s.mustGetLock(c, []byte("key"))
	c.Assert(l.TTL >= defaultLockTTL, IsTrue)

	// Huge txn has a greater TTL.
	txn, err = s.store.Begin()
	start := time.Now()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("key"), []byte("value"))
	for i := 0; i < 2048; i++ {
		k, v := randKV(1024, 1024)
		txn.Set(kv.Key(k), []byte(v))
	}
	s.prewriteTxn(c, txn.(*tikvTxn))
	l = s.mustGetLock(c, []byte("key"))
	s.ttlEquals(c, l.TTL, uint64(ttlFactor*2)+uint64(time.Since(start)/time.Millisecond))

	// Txn with long read time.
	start = time.Now()
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond * 50)
	txn.Set(kv.Key("key"), []byte("value"))
	s.prewriteTxn(c, txn.(*tikvTxn))
	l = s.mustGetLock(c, []byte("key"))
	s.ttlEquals(c, l.TTL, defaultLockTTL+uint64(time.Since(start)/time.Millisecond))
}

func (s *testLockSuite) TestNewLockZeroTTL(c *C) {
	l := NewLock(&kvrpcpb.LockInfo{})
	c.Assert(l.TTL, Equals, uint64(0))
}

func init() {
	// Speed up tests.
	defaultLockTTL = 3
	maxLockTTL = 120
	ttlFactor = 6
	oracleUpdateInterval = 2
}
