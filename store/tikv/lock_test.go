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
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
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
	c.Assert(err, IsNil)
	tpc.primaryKey = primaryKey

	ctx := context.Background()
	err = tpc.prewriteMutations(NewBackofferWithVars(ctx, PrewriteMaxBackoff, nil), tpc.mutations)
	c.Assert(err, IsNil)

	if commitPrimary {
		tpc.commitTS, err = s.store.oracle.GetTimestamp(ctx)
		c.Assert(err, IsNil)
		err = tpc.commitMutations(NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), nil), tpc.mutationsOfKeys([][]byte{primaryKey}))
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
		v, err := txn.Get(context.TODO(), []byte{ch})
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
		c.Assert(iter.Value(), BytesEquals, []byte{ch})
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
	snapshot := newTiKVSnapshot(s.store, ver, 0)
	m, err := snapshot.BatchGet(context.Background(), keys)
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
	status, err := s.store.lockResolver.GetTxnStatus(startTS, startTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Equals, commitTS)

	startTS, commitTS = s.lockKey(c, []byte("a"), []byte("a"), []byte("a"), []byte("a"), true)
	status, err = s.store.lockResolver.GetTxnStatus(startTS, startTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Equals, commitTS)

	startTS, _ = s.lockKey(c, []byte("a"), []byte("a"), []byte("a"), []byte("a"), false)
	status, err = s.store.lockResolver.GetTxnStatus(startTS, startTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsFalse)
	c.Assert(status.ttl, Greater, uint64(0), Commentf("action:%s", status.action))
}

func (s *testLockSuite) TestCheckTxnStatusTTL(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("key"), []byte("value"))
	s.prewriteTxnWithTTL(c, txn.(*tikvTxn), 1000)

	bo := NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil)
	lr := newLockResolver(s.store)
	callerStartTS, err := lr.store.GetOracle().GetTimestamp(bo.ctx)
	c.Assert(err, IsNil)

	// Check the lock TTL of a transaction.
	status, err := lr.GetTxnStatus(txn.StartTS(), callerStartTS, []byte("key"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsFalse)
	c.Assert(status.ttl, Greater, uint64(0))
	c.Assert(status.CommitTS(), Equals, uint64(0))

	// Rollback the txn.
	lock := s.mustGetLock(c, []byte("key"))
	status = TxnStatus{}
	cleanRegions := make(map[RegionVerID]struct{})
	err = newLockResolver(s.store).resolveLock(bo, lock, status, false, cleanRegions)
	c.Assert(err, IsNil)

	// Check its status is rollbacked.
	status, err = lr.GetTxnStatus(txn.StartTS(), callerStartTS, []byte("key"))
	c.Assert(err, IsNil)
	c.Assert(status.ttl, Equals, uint64(0))
	c.Assert(status.commitTS, Equals, uint64(0))
	c.Assert(status.action, Equals, kvrpcpb.Action_NoAction)

	// Check a committed txn.
	startTS, commitTS := s.putKV(c, []byte("a"), []byte("a"))
	status, err = lr.GetTxnStatus(startTS, callerStartTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.ttl, Equals, uint64(0))
	c.Assert(status.commitTS, Equals, commitTS)
}

func (s *testLockSuite) TestTxnHeartBeat(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("key"), []byte("value"))
	s.prewriteTxn(c, txn.(*tikvTxn))

	bo := NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil)
	newTTL, err := sendTxnHeartBeat(bo, s.store, []byte("key"), txn.StartTS(), 6666)
	c.Assert(err, IsNil)
	c.Assert(newTTL, Equals, uint64(6666))

	newTTL, err = sendTxnHeartBeat(bo, s.store, []byte("key"), txn.StartTS(), 5555)
	c.Assert(err, IsNil)
	c.Assert(newTTL, Equals, uint64(6666))

	lock := s.mustGetLock(c, []byte("key"))
	status := TxnStatus{ttl: newTTL}
	cleanRegions := make(map[RegionVerID]struct{})
	err = newLockResolver(s.store).resolveLock(bo, lock, status, false, cleanRegions)
	c.Assert(err, IsNil)

	newTTL, err = sendTxnHeartBeat(bo, s.store, []byte("key"), txn.StartTS(), 6666)
	c.Assert(err, NotNil)
	c.Assert(newTTL, Equals, uint64(0))
}

func (s *testLockSuite) TestCheckTxnStatus(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("key"), []byte("value"))
	txn.Set(kv.Key("second"), []byte("xxx"))
	s.prewriteTxnWithTTL(c, txn.(*tikvTxn), 1000)

	oracle := s.store.GetOracle()
	currentTS, err := oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	c.Assert(currentTS, Greater, txn.StartTS())

	bo := NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil)
	resolver := newLockResolver(s.store)
	// Call getTxnStatus to check the lock status.
	status, err := resolver.getTxnStatus(bo, txn.StartTS(), []byte("key"), currentTS, currentTS, true)
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsFalse)
	c.Assert(status.ttl, Greater, uint64(0))
	c.Assert(status.CommitTS(), Equals, uint64(0))
	c.Assert(status.action, Equals, kvrpcpb.Action_MinCommitTSPushed)

	// Test the ResolveLocks API
	lock := s.mustGetLock(c, []byte("second"))
	timeBeforeExpire, _, err := resolver.ResolveLocks(bo, currentTS, []*Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(timeBeforeExpire > int64(0), IsTrue)

	// Force rollback the lock using lock.TTL = 0.
	lock.TTL = uint64(0)
	timeBeforeExpire, _, err = resolver.ResolveLocks(bo, currentTS, []*Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(timeBeforeExpire, Equals, int64(0))

	// Then call getTxnStatus again and check the lock status.
	currentTS, err = oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	status, err = newLockResolver(s.store).getTxnStatus(bo, txn.StartTS(), []byte("key"), currentTS, 0, true)
	c.Assert(err, IsNil)
	c.Assert(status.ttl, Equals, uint64(0))
	c.Assert(status.commitTS, Equals, uint64(0))
	c.Assert(status.action, Equals, kvrpcpb.Action_NoAction)

	// Call getTxnStatus on a committed transaction.
	startTS, commitTS := s.putKV(c, []byte("a"), []byte("a"))
	status, err = newLockResolver(s.store).getTxnStatus(bo, startTS, []byte("a"), currentTS, currentTS, true)
	c.Assert(err, IsNil)
	c.Assert(status.ttl, Equals, uint64(0))
	c.Assert(status.commitTS, Equals, commitTS)
}

func (s *testLockSuite) TestCheckTxnStatusNoWait(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("key"), []byte("value"))
	txn.Set(kv.Key("second"), []byte("xxx"))
	committer, err := newTwoPhaseCommitterWithInit(txn.(*tikvTxn), 0)
	c.Assert(err, IsNil)
	// Increase lock TTL to make CI more stable.
	committer.lockTTL = txnLockTTL(txn.(*tikvTxn).startTime, 200*1024*1024)

	// Only prewrite the secondary key to simulate a concurrent prewrite case:
	// prewrite secondary regions success and prewrite the primary region is pending.
	err = committer.prewriteMutations(NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil), committer.mutationsOfKeys([][]byte{[]byte("second")}))
	c.Assert(err, IsNil)

	oracle := s.store.GetOracle()
	currentTS, err := oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	bo := NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil)
	resolver := newLockResolver(s.store)

	// Call getTxnStatus for the TxnNotFound case.
	_, err = resolver.getTxnStatus(bo, txn.StartTS(), []byte("key"), currentTS, currentTS, false)
	c.Assert(err, NotNil)
	_, ok := errors.Cause(err).(txnNotFoundErr)
	c.Assert(ok, IsTrue)

	errCh := make(chan error)
	go func() {
		errCh <- committer.prewriteMutations(NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil), committer.mutationsOfKeys([][]byte{[]byte("key")}))
	}()

	lock := &Lock{
		Key:     []byte("second"),
		Primary: []byte("key"),
		TxnID:   txn.StartTS(),
		TTL:     100000,
	}
	// Call getTxnStatusFromLock to cover the retry logic.
	status, err := resolver.getTxnStatusFromLock(bo, lock, currentTS)
	c.Assert(err, IsNil)
	c.Assert(status.ttl, Greater, uint64(0))
	c.Assert(<-errCh, IsNil)
	c.Assert(committer.cleanupMutations(bo, committer.mutations), IsNil)

	// Call getTxnStatusFromLock to cover TxnNotFound and retry timeout.
	startTS, err := oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	lock = &Lock{
		Key:     []byte("second"),
		Primary: []byte("key_not_exist"),
		TxnID:   startTS,
		TTL:     1000,
	}
	status, err = resolver.getTxnStatusFromLock(bo, lock, currentTS)
	c.Assert(err, IsNil)
	c.Assert(status.ttl, Equals, uint64(0))
	c.Assert(status.commitTS, Equals, uint64(0))
	c.Assert(status.action, Equals, kvrpcpb.Action_LockNotExistRollback)
}

func (s *testLockSuite) prewriteTxn(c *C, txn *tikvTxn) {
	s.prewriteTxnWithTTL(c, txn, 0)
}

func (s *testLockSuite) prewriteTxnWithTTL(c *C, txn *tikvTxn, ttl uint64) {
	committer, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	if ttl > 0 {
		elapsed := time.Since(txn.startTime) / time.Millisecond
		committer.lockTTL = uint64(elapsed) + ttl
	}
	err = committer.prewriteMutations(NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil), committer.mutations)
	c.Assert(err, IsNil)
}

func (s *testLockSuite) mustGetLock(c *C, key []byte) *Lock {
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	bo := NewBackofferWithVars(context.Background(), getMaxBackoff, nil)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver.Ver,
	})
	loc, err := s.store.regionCache.LocateKey(bo, key)
	c.Assert(err, IsNil)
	resp, err := s.store.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErr := resp.Resp.(*kvrpcpb.GetResponse).GetError()
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
	s.prewriteTxnWithTTL(c, txn.(*tikvTxn), 3100)
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

func (s *testLockSuite) TestBatchResolveLocks(c *C) {
	// The first transaction is a normal transaction with a long TTL
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("k1"), []byte("v1"))
	txn.Set(kv.Key("k2"), []byte("v2"))
	s.prewriteTxnWithTTL(c, txn.(*tikvTxn), 20000)

	// The second transaction is an async commit transaction
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("k3"), []byte("v3"))
	txn.Set(kv.Key("k4"), []byte("v4"))
	tikvTxn := txn.(*tikvTxn)
	committer, err := newTwoPhaseCommitterWithInit(tikvTxn, 0)
	c.Assert(err, IsNil)
	committer.setAsyncCommit(true)
	committer.lockTTL = 20000
	err = committer.prewriteMutations(NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil), committer.mutations)
	c.Assert(err, IsNil)

	var locks []*Lock
	for _, key := range []string{"k1", "k2", "k3", "k4"} {
		l := s.mustGetLock(c, []byte(key))
		locks = append(locks, l)
	}

	// Locks may not expired
	msBeforeLockExpired := s.store.GetOracle().UntilExpired(locks[0].TxnID, locks[1].TTL)
	c.Assert(msBeforeLockExpired, Greater, int64(0))
	msBeforeLockExpired = s.store.GetOracle().UntilExpired(locks[3].TxnID, locks[3].TTL)
	c.Assert(msBeforeLockExpired, Greater, int64(0))

	lr := newLockResolver(s.store)
	bo := NewBackofferWithVars(context.Background(), GcResolveLockMaxBackoff, nil)
	loc, err := lr.store.GetRegionCache().LocateKey(bo, locks[0].Primary)
	c.Assert(err, IsNil)
	// Check BatchResolveLocks resolve the lock even the ttl is not expired.
	success, err := lr.BatchResolveLocks(bo, locks, loc.Region)
	c.Assert(success, IsTrue)
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	// transaction 1 is rolled back
	_, err = txn.Get(context.Background(), kv.Key("k1"))
	c.Assert(err, Equals, kv.ErrNotExist)
	_, err = txn.Get(context.Background(), kv.Key("k2"))
	c.Assert(err, Equals, kv.ErrNotExist)
	// transaction 2 is committed
	v, err := txn.Get(context.Background(), kv.Key("k3"))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(v, []byte("v3")), IsTrue)
	v, err = txn.Get(context.Background(), kv.Key("k4"))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(v, []byte("v4")), IsTrue)
}

func (s *testLockSuite) TestNewLockZeroTTL(c *C) {
	l := NewLock(&kvrpcpb.LockInfo{})
	c.Assert(l.TTL, Equals, uint64(0))
}

func init() {
	// Speed up tests.
	oracleUpdateInterval = 2
}

func (s *testLockSuite) TestZeroMinCommitTS(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set(kv.Key("key"), []byte("value"))
	bo := NewBackofferWithVars(context.Background(), PrewriteMaxBackoff, nil)

	mockValue := fmt.Sprintf(`return(%d)`, txn.StartTS())
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockZeroCommitTS", mockValue), IsNil)
	s.prewriteTxnWithTTL(c, txn.(*tikvTxn), 1000)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockZeroCommitTS"), IsNil)

	lock := s.mustGetLock(c, []byte("key"))
	expire, pushed, err := newLockResolver(s.store).ResolveLocks(bo, 0, []*Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(pushed, HasLen, 0)
	c.Assert(expire, Greater, int64(0))

	expire, pushed, err = newLockResolver(s.store).ResolveLocks(bo, math.MaxUint64, []*Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(pushed, HasLen, 1)
	c.Assert(expire, Greater, int64(0))

	// Clean up this test.
	lock.TTL = uint64(0)
	expire, _, err = newLockResolver(s.store).ResolveLocks(bo, 0, []*Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(expire, Equals, int64(0))
}

func (s *testLockSuite) TestDeduplicateKeys(c *C) {
	inputs := []string{
		"a b c",
		"a a b c",
		"a a a b c",
		"a a a b b b b c",
		"a b b b b c c c",
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, in := range inputs {
		strs := strings.Split(in, " ")
		keys := make([][]byte, len(strs))
		for _, i := range r.Perm(len(strs)) {
			keys[i] = []byte(strs[i])
		}
		keys = deduplicateKeys(keys)
		strs = strs[:len(keys)]
		for i := range keys {
			strs[i] = string(keys[i])
		}
		out := strings.Join(strs, " ")
		c.Assert(out, Equals, "a b c")
	}
}
