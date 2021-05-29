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

package tikv_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	deadlockpb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/tikv"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

var getMaxBackoff = tikv.ConfigProbe{}.GetGetMaxBackoff()

type testLockSuite struct {
	OneByOneSuite
	store tikv.StoreProbe
}

var _ = Suite(&testLockSuite{})

func (s *testLockSuite) SetUpTest(c *C) {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(c)}
}

func (s *testLockSuite) TearDownTest(c *C) {
	s.store.Close()
}

func (s *testLockSuite) lockKey(c *C, key, value, primaryKey, primaryValue []byte, commitPrimary bool) (uint64, uint64) {
	txn, err := s.store.Begin()
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
	tpc, err := txn.NewCommitter(0)
	c.Assert(err, IsNil)
	tpc.SetPrimaryKey(primaryKey)

	ctx := context.Background()
	err = tpc.PrewriteAllMutations(ctx)
	c.Assert(err, IsNil)

	if commitPrimary {
		commitTS, err := s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		c.Assert(err, IsNil)
		tpc.SetCommitTS(commitTS)
		err = tpc.CommitMutations(ctx)
		c.Assert(err, IsNil)
	}
	return txn.StartTS(), tpc.GetCommitTS()
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
	return txn.StartTS(), txn.GetCommitTS()
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
		c.Assert(iter.Key(), BytesEquals, []byte{ch})
		c.Assert(iter.Value(), BytesEquals, []byte{ch})
		c.Assert(iter.Next(), IsNil)
	}
}

func (s *testLockSuite) TestScanLockResolveWithSeekKeyOnly(c *C) {
	s.putAlphabets(c)
	s.prepareAlphabetLocks(c)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.GetSnapshot().SetKeyOnly(true)
	iter, err := txn.Iter([]byte("a"), nil)
	c.Assert(err, IsNil)
	for ch := byte('a'); ch <= byte('z'); ch++ {
		c.Assert(iter.Valid(), IsTrue)
		c.Assert(iter.Key(), BytesEquals, []byte{ch})
		c.Assert(iter.Next(), IsNil)
	}
}

func (s *testLockSuite) TestScanLockResolveWithBatchGet(c *C) {
	s.putAlphabets(c)
	s.prepareAlphabetLocks(c)

	var keys [][]byte
	for ch := byte('a'); ch <= byte('z'); ch++ {
		keys = append(keys, []byte{ch})
	}

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	m, err := toTiDBTxn(&txn).BatchGet(context.Background(), toTiDBKeys(keys))
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
	status, err := s.store.GetLockResolver().GetTxnStatus(startTS, startTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Equals, commitTS)

	startTS, commitTS = s.lockKey(c, []byte("a"), []byte("a"), []byte("a"), []byte("a"), true)
	status, err = s.store.GetLockResolver().GetTxnStatus(startTS, startTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Equals, commitTS)

	startTS, _ = s.lockKey(c, []byte("a"), []byte("a"), []byte("a"), []byte("a"), false)
	status, err = s.store.GetLockResolver().GetTxnStatus(startTS, startTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsFalse)
	c.Assert(status.TTL(), Greater, uint64(0), Commentf("action:%s", status.Action()))
}

func (s *testLockSuite) TestCheckTxnStatusTTL(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set([]byte("key"), []byte("value"))
	s.prewriteTxnWithTTL(c, txn, 1000)

	bo := tikv.NewBackofferWithVars(context.Background(), tikv.PrewriteMaxBackoff, nil)
	lr := s.store.NewLockResolver()
	callerStartTS, err := s.store.GetOracle().GetTimestamp(bo.GetCtx(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)

	// Check the lock TTL of a transaction.
	status, err := lr.LockResolver.GetTxnStatus(txn.StartTS(), callerStartTS, []byte("key"))
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsFalse)
	c.Assert(status.TTL(), Greater, uint64(0))
	c.Assert(status.CommitTS(), Equals, uint64(0))

	// Rollback the txn.
	lock := s.mustGetLock(c, []byte("key"))
	err = s.store.NewLockResolver().ResolveLock(context.Background(), lock)
	c.Assert(err, IsNil)

	// Check its status is rollbacked.
	status, err = lr.LockResolver.GetTxnStatus(txn.StartTS(), callerStartTS, []byte("key"))
	c.Assert(err, IsNil)
	c.Assert(status.TTL(), Equals, uint64(0))
	c.Assert(status.CommitTS(), Equals, uint64(0))
	c.Assert(status.Action(), Equals, kvrpcpb.Action_NoAction)

	// Check a committed txn.
	startTS, commitTS := s.putKV(c, []byte("a"), []byte("a"))
	status, err = lr.LockResolver.GetTxnStatus(startTS, callerStartTS, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(status.TTL(), Equals, uint64(0))
	c.Assert(status.CommitTS(), Equals, commitTS)
}

func (s *testLockSuite) TestTxnHeartBeat(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set([]byte("key"), []byte("value"))
	s.prewriteTxn(c, txn)

	newTTL, err := s.store.SendTxnHeartbeat(context.Background(), []byte("key"), txn.StartTS(), 6666)
	c.Assert(err, IsNil)
	c.Assert(newTTL, Equals, uint64(6666))

	newTTL, err = s.store.SendTxnHeartbeat(context.Background(), []byte("key"), txn.StartTS(), 5555)
	c.Assert(err, IsNil)
	c.Assert(newTTL, Equals, uint64(6666))

	lock := s.mustGetLock(c, []byte("key"))
	err = s.store.NewLockResolver().ResolveLock(context.Background(), lock)
	c.Assert(err, IsNil)

	newTTL, err = s.store.SendTxnHeartbeat(context.Background(), []byte("key"), txn.StartTS(), 6666)
	c.Assert(err, NotNil)
	c.Assert(newTTL, Equals, uint64(0))
}

func (s *testLockSuite) TestCheckTxnStatus(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set([]byte("key"), []byte("value"))
	txn.Set([]byte("second"), []byte("xxx"))
	s.prewriteTxnWithTTL(c, txn, 1000)

	o := s.store.GetOracle()
	currentTS, err := o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	c.Assert(currentTS, Greater, txn.StartTS())

	bo := tikv.NewBackofferWithVars(context.Background(), tikv.PrewriteMaxBackoff, nil)
	resolver := s.store.NewLockResolver()
	// Call getTxnStatus to check the lock status.
	status, err := resolver.GetTxnStatus(bo, txn.StartTS(), []byte("key"), currentTS, currentTS, true, false, nil)
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsFalse)
	c.Assert(status.TTL(), Greater, uint64(0))
	c.Assert(status.CommitTS(), Equals, uint64(0))
	c.Assert(status.Action(), Equals, kvrpcpb.Action_MinCommitTSPushed)

	// Test the ResolveLocks API
	lock := s.mustGetLock(c, []byte("second"))
	timeBeforeExpire, _, err := resolver.ResolveLocks(bo, currentTS, []*tikv.Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(timeBeforeExpire > int64(0), IsTrue)

	// Force rollback the lock using lock.TTL = 0.
	lock.TTL = uint64(0)
	timeBeforeExpire, _, err = resolver.ResolveLocks(bo, currentTS, []*tikv.Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(timeBeforeExpire, Equals, int64(0))

	// Then call getTxnStatus again and check the lock status.
	currentTS, err = o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	status, err = s.store.NewLockResolver().GetTxnStatus(bo, txn.StartTS(), []byte("key"), currentTS, 0, true, false, nil)
	c.Assert(err, IsNil)
	c.Assert(status.TTL(), Equals, uint64(0))
	c.Assert(status.CommitTS(), Equals, uint64(0))
	c.Assert(status.Action(), Equals, kvrpcpb.Action_NoAction)

	// Call getTxnStatus on a committed transaction.
	startTS, commitTS := s.putKV(c, []byte("a"), []byte("a"))
	status, err = s.store.NewLockResolver().GetTxnStatus(bo, startTS, []byte("a"), currentTS, currentTS, true, false, nil)
	c.Assert(err, IsNil)
	c.Assert(status.TTL(), Equals, uint64(0))
	c.Assert(status.CommitTS(), Equals, commitTS)
}

func (s *testLockSuite) TestCheckTxnStatusNoWait(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set([]byte("key"), []byte("value"))
	txn.Set([]byte("second"), []byte("xxx"))
	committer, err := txn.NewCommitter(0)
	c.Assert(err, IsNil)
	// Increase lock TTL to make CI more stable.
	committer.SetLockTTLByTimeAndSize(txn.GetStartTime(), 200*1024*1024)

	// Only prewrite the secondary key to simulate a concurrent prewrite case:
	// prewrite secondary regions success and prewrite the primary region is pending.
	err = committer.PrewriteMutations(context.Background(), committer.MutationsOfKeys([][]byte{[]byte("second")}))
	c.Assert(err, IsNil)

	o := s.store.GetOracle()
	currentTS, err := o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	bo := tikv.NewBackofferWithVars(context.Background(), tikv.PrewriteMaxBackoff, nil)
	resolver := s.store.NewLockResolver()

	// Call getTxnStatus for the TxnNotFound case.
	_, err = resolver.GetTxnStatus(bo, txn.StartTS(), []byte("key"), currentTS, currentTS, false, false, nil)
	c.Assert(err, NotNil)
	c.Assert(resolver.IsErrorNotFound(err), IsTrue)

	errCh := make(chan error)
	go func() {
		errCh <- committer.PrewriteMutations(context.Background(), committer.MutationsOfKeys([][]byte{[]byte("key")}))
	}()

	lock := &tikv.Lock{
		Key:     []byte("second"),
		Primary: []byte("key"),
		TxnID:   txn.StartTS(),
		TTL:     100000,
	}
	// Call getTxnStatusFromLock to cover the retry logic.
	status, err := resolver.GetTxnStatusFromLock(bo, lock, currentTS, false)
	c.Assert(err, IsNil)
	c.Assert(status.TTL(), Greater, uint64(0))
	c.Assert(<-errCh, IsNil)
	c.Assert(committer.CleanupMutations(context.Background()), IsNil)

	// Call getTxnStatusFromLock to cover TxnNotFound and retry timeout.
	startTS, err := o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	lock = &tikv.Lock{
		Key:     []byte("second"),
		Primary: []byte("key_not_exist"),
		TxnID:   startTS,
		TTL:     1000,
	}
	status, err = resolver.GetTxnStatusFromLock(bo, lock, currentTS, false)
	c.Assert(err, IsNil)
	c.Assert(status.TTL(), Equals, uint64(0))
	c.Assert(status.CommitTS(), Equals, uint64(0))
	c.Assert(status.Action(), Equals, kvrpcpb.Action_LockNotExistRollback)
}

func (s *testLockSuite) prewriteTxn(c *C, txn tikv.TxnProbe) {
	s.prewriteTxnWithTTL(c, txn, 0)
}

func (s *testLockSuite) prewriteTxnWithTTL(c *C, txn tikv.TxnProbe, ttl uint64) {
	committer, err := txn.NewCommitter(0)
	c.Assert(err, IsNil)
	if ttl > 0 {
		elapsed := time.Since(txn.GetStartTime()) / time.Millisecond
		committer.SetLockTTL(uint64(elapsed) + ttl)
	}
	err = committer.PrewriteAllMutations(context.Background())
	c.Assert(err, IsNil)
}

func (s *testLockSuite) mustGetLock(c *C, key []byte) *tikv.Lock {
	ver, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	bo := tikv.NewBackofferWithVars(context.Background(), getMaxBackoff, nil)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver,
	})
	loc, err := s.store.GetRegionCache().LocateKey(bo, key)
	c.Assert(err, IsNil)
	resp, err := s.store.SendReq(bo, req, loc.Region, tikv.ReadTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErr := resp.Resp.(*kvrpcpb.GetResponse).GetError()
	c.Assert(keyErr, NotNil)
	lock, err := tikv.LockProbe{}.ExtractLockFromKeyErr(keyErr)
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
	defaultLockTTL := tikv.ConfigProbe{}.GetDefaultLockTTL()
	ttlFactor := tikv.ConfigProbe{}.GetTTLFactor()

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set([]byte("key"), []byte("value"))
	time.Sleep(time.Millisecond)
	s.prewriteTxnWithTTL(c, txn, 3100)
	l := s.mustGetLock(c, []byte("key"))
	c.Assert(l.TTL >= defaultLockTTL, IsTrue)

	// Huge txn has a greater TTL.
	txn, err = s.store.Begin()
	start := time.Now()
	c.Assert(err, IsNil)
	txn.Set([]byte("key"), []byte("value"))
	for i := 0; i < 2048; i++ {
		k, v := randKV(1024, 1024)
		txn.Set([]byte(k), []byte(v))
	}
	s.prewriteTxn(c, txn)
	l = s.mustGetLock(c, []byte("key"))
	s.ttlEquals(c, l.TTL, uint64(ttlFactor*2)+uint64(time.Since(start)/time.Millisecond))

	// Txn with long read time.
	start = time.Now()
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond * 50)
	txn.Set([]byte("key"), []byte("value"))
	s.prewriteTxn(c, txn)
	l = s.mustGetLock(c, []byte("key"))
	s.ttlEquals(c, l.TTL, defaultLockTTL+uint64(time.Since(start)/time.Millisecond))
}

func (s *testLockSuite) TestBatchResolveLocks(c *C) {
	// The first transaction is a normal transaction with a long TTL
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set([]byte("k1"), []byte("v1"))
	txn.Set([]byte("k2"), []byte("v2"))
	s.prewriteTxnWithTTL(c, txn, 20000)

	// The second transaction is an async commit transaction
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set([]byte("k3"), []byte("v3"))
	txn.Set([]byte("k4"), []byte("v4"))
	committer, err := txn.NewCommitter(0)
	c.Assert(err, IsNil)
	committer.SetUseAsyncCommit()
	committer.SetLockTTL(20000)
	committer.PrewriteAllMutations(context.Background())
	c.Assert(err, IsNil)

	var locks []*tikv.Lock
	for _, key := range []string{"k1", "k2", "k3", "k4"} {
		l := s.mustGetLock(c, []byte(key))
		locks = append(locks, l)
	}

	// Locks may not expired
	msBeforeLockExpired := s.store.GetOracle().UntilExpired(locks[0].TxnID, locks[1].TTL, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(msBeforeLockExpired, Greater, int64(0))
	msBeforeLockExpired = s.store.GetOracle().UntilExpired(locks[3].TxnID, locks[3].TTL, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(msBeforeLockExpired, Greater, int64(0))

	lr := s.store.NewLockResolver()
	bo := tikv.NewGcResolveLockMaxBackoffer(context.Background())
	loc, err := s.store.GetRegionCache().LocateKey(bo, locks[0].Primary)
	c.Assert(err, IsNil)
	// Check BatchResolveLocks resolve the lock even the ttl is not expired.
	success, err := lr.BatchResolveLocks(bo, locks, loc.Region)
	c.Assert(success, IsTrue)
	c.Assert(err, IsNil)

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	// transaction 1 is rolled back
	_, err = txn.Get(context.Background(), []byte("k1"))
	c.Assert(err, Equals, tikverr.ErrNotExist)
	_, err = txn.Get(context.Background(), []byte("k2"))
	c.Assert(err, Equals, tikverr.ErrNotExist)
	// transaction 2 is committed
	v, err := txn.Get(context.Background(), []byte("k3"))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(v, []byte("v3")), IsTrue)
	v, err = txn.Get(context.Background(), []byte("k4"))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(v, []byte("v4")), IsTrue)
}

func (s *testLockSuite) TestNewLockZeroTTL(c *C) {
	l := tikv.NewLock(&kvrpcpb.LockInfo{})
	c.Assert(l.TTL, Equals, uint64(0))
}

func init() {
	// Speed up tests.
	tikv.ConfigProbe{}.SetOracleUpdateInterval(2)
}

func (s *testLockSuite) TestZeroMinCommitTS(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.Set([]byte("key"), []byte("value"))
	bo := tikv.NewBackofferWithVars(context.Background(), tikv.PrewriteMaxBackoff, nil)

	mockValue := fmt.Sprintf(`return(%d)`, txn.StartTS())
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/mockZeroCommitTS", mockValue), IsNil)
	s.prewriteTxnWithTTL(c, txn, 1000)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/mockZeroCommitTS"), IsNil)

	lock := s.mustGetLock(c, []byte("key"))
	expire, pushed, err := s.store.NewLockResolver().ResolveLocks(bo, 0, []*tikv.Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(pushed, HasLen, 0)
	c.Assert(expire, Greater, int64(0))

	expire, pushed, err = s.store.NewLockResolver().ResolveLocks(bo, math.MaxUint64, []*tikv.Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(pushed, HasLen, 1)
	c.Assert(expire, Greater, int64(0))

	// Clean up this test.
	lock.TTL = uint64(0)
	expire, _, err = s.store.NewLockResolver().ResolveLocks(bo, 0, []*tikv.Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(expire, Equals, int64(0))
}

func (s *testLockSuite) prepareTxnFallenBackFromAsyncCommit(c *C) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set([]byte("fb1"), []byte("1"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("fb2"), []byte("2"))
	c.Assert(err, IsNil)

	committer, err := txn.NewCommitter(1)
	c.Assert(err, IsNil)
	c.Assert(committer.GetMutations().Len(), Equals, 2)
	committer.SetLockTTL(0)
	committer.SetUseAsyncCommit()
	committer.SetCommitTS(committer.GetStartTS() + (100 << 18)) // 100ms

	err = committer.PrewriteMutations(context.Background(), committer.GetMutations().Slice(0, 1))
	c.Assert(err, IsNil)
	c.Assert(committer.IsAsyncCommit(), IsTrue)

	// Set an invalid maxCommitTS to produce MaxCommitTsTooLarge
	committer.SetMaxCommitTS(committer.GetStartTS() - 1)
	err = committer.PrewriteMutations(context.Background(), committer.GetMutations().Slice(1, 2))
	c.Assert(err, IsNil)
	c.Assert(committer.IsAsyncCommit(), IsFalse) // Fallback due to MaxCommitTsTooLarge
}

func (s *testLockSuite) TestCheckLocksFallenBackFromAsyncCommit(c *C) {
	s.prepareTxnFallenBackFromAsyncCommit(c)

	lock := s.mustGetLock(c, []byte("fb1"))
	c.Assert(lock.UseAsyncCommit, IsTrue)
	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)
	lr := s.store.NewLockResolver()
	status, err := lr.GetTxnStatusFromLock(bo, lock, 0, false)
	c.Assert(err, IsNil)
	c.Assert(tikv.LockProbe{}.GetPrimaryKeyFromTxnStatus(status), DeepEquals, []byte("fb1"))

	err = lr.CheckAllSecondaries(bo, lock, &status)
	c.Assert(lr.IsNonAsyncCommitLock(err), IsTrue)

	status, err = lr.GetTxnStatusFromLock(bo, lock, 0, true)
	c.Assert(err, IsNil)
	c.Assert(status.Action(), Equals, kvrpcpb.Action_TTLExpireRollback)
	c.Assert(status.TTL(), Equals, uint64(0))
}

func (s *testLockSuite) TestResolveTxnFallenBackFromAsyncCommit(c *C) {
	s.prepareTxnFallenBackFromAsyncCommit(c)

	lock := s.mustGetLock(c, []byte("fb1"))
	c.Assert(lock.UseAsyncCommit, IsTrue)
	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)
	expire, pushed, err := s.store.NewLockResolver().ResolveLocks(bo, 0, []*tikv.Lock{lock})
	c.Assert(err, IsNil)
	c.Assert(expire, Equals, int64(0))
	c.Assert(len(pushed), Equals, 0)

	t3, err := s.store.Begin()
	c.Assert(err, IsNil)
	_, err = t3.Get(context.Background(), []byte("fb1"))
	c.Assert(tikverr.IsErrNotFound(err), IsTrue)
	_, err = t3.Get(context.Background(), []byte("fb2"))
	c.Assert(tikverr.IsErrNotFound(err), IsTrue)
}

func (s *testLockSuite) TestBatchResolveTxnFallenBackFromAsyncCommit(c *C) {
	s.prepareTxnFallenBackFromAsyncCommit(c)

	lock := s.mustGetLock(c, []byte("fb1"))
	c.Assert(lock.UseAsyncCommit, IsTrue)
	bo := tikv.NewBackoffer(context.Background(), getMaxBackoff)
	loc, err := s.store.GetRegionCache().LocateKey(bo, []byte("fb1"))
	c.Assert(err, IsNil)
	ok, err := s.store.NewLockResolver().BatchResolveLocks(bo, []*tikv.Lock{lock}, loc.Region)
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)

	t3, err := s.store.Begin()
	c.Assert(err, IsNil)
	_, err = t3.Get(context.Background(), []byte("fb1"))
	c.Assert(tikverr.IsErrNotFound(err), IsTrue)
	_, err = t3.Get(context.Background(), []byte("fb2"))
	c.Assert(tikverr.IsErrNotFound(err), IsTrue)
}

func (s *testLockSuite) TestDeadlockReportWaitChain(c *C) {
	// Utilities to make the test logic clear and simple.
	type txnWrapper struct {
		tikv.TxnProbe
		wg sync.WaitGroup
	}

	makeLockCtx := func(txn *txnWrapper, resourceGroupTag string) *kv.LockCtx {
		return &kv.LockCtx{
			ForUpdateTS:      txn.StartTS(),
			WaitStartTime:    time.Now(),
			LockWaitTime:     1000,
			ResourceGroupTag: []byte(resourceGroupTag),
		}
	}

	// Prepares several transactions and each locks a key.
	prepareTxns := func(num int) []*txnWrapper {
		res := make([]*txnWrapper, 0, num)
		for i := 0; i < num; i++ {
			txnProbe, err := s.store.Begin()
			c.Assert(err, IsNil)
			txn := &txnWrapper{TxnProbe: txnProbe}
			txn.SetPessimistic(true)
			tag := fmt.Sprintf("tag-init%v", i)
			key := []byte{'k', byte(i)}
			err = txn.LockKeys(context.Background(), makeLockCtx(txn, tag), key)
			c.Assert(err, IsNil)

			res = append(res, txn)
		}
		return res
	}

	// Let the i-th trnasaction lock the key that has been locked by j-th transaction
	tryLock := func(txns []*txnWrapper, i int, j int) error {
		c.Logf("txn %v try locking %v", i, j)
		txn := txns[i]
		tag := fmt.Sprintf("tag-%v-%v", i, j)
		key := []byte{'k', byte(j)}
		return txn.LockKeys(context.Background(), makeLockCtx(txn, tag), key)
	}

	// Asserts the i-th transaction waits for the j-th transaction.
	makeWaitFor := func(txns []*txnWrapper, i int, j int) {
		txns[i].wg.Add(1)
		go func() {
			defer txns[i].wg.Done()
			err := tryLock(txns, i, j)
			// After the lock being waited for is released, the transaction returns a WriteConflict error
			// unconditionally, which is by design.
			c.Assert(err, NotNil)
			c.Logf("txn %v wait for %v finished, err: %s", i, j, err.Error())
			_, ok := errors.Cause(err).(*tikverr.ErrWriteConflict)
			c.Assert(ok, IsTrue)
		}()
	}

	waitAndRollback := func(txns []*txnWrapper, i int) {
		// It's expected that each transaction should be rolled back after its blocker, so that `Rollback` will not
		// run when there's concurrent `LockKeys` running.
		// If it's blocked on the `Wait` forever, it means the transaction's blocker is not rolled back.
		c.Logf("rollback txn %v", i)
		txns[i].wg.Wait()
		err := txns[i].Rollback()
		c.Assert(err, IsNil)
	}

	// Check the given WaitForEntry is caused by txn[i] waiting for txn[j].
	checkWaitChainEntry := func(txns []*txnWrapper, entry *deadlockpb.WaitForEntry, i, j int) {
		c.Assert(entry.Txn, Equals, txns[i].StartTS())
		c.Assert(entry.WaitForTxn, Equals, txns[j].StartTS())
		c.Assert(entry.Key, BytesEquals, []byte{'k', byte(j)})
		c.Assert(string(entry.ResourceGroupTag), Equals, fmt.Sprintf("tag-%v-%v", i, j))
	}

	c.Log("test case 1: 1->0->1")

	txns := prepareTxns(2)

	makeWaitFor(txns, 0, 1)
	// Sleep for a while to make sure it has been blocked.
	time.Sleep(time.Millisecond * 100)

	// txn2 tries locking k1 and encounters deadlock error.
	err := tryLock(txns, 1, 0)
	c.Assert(err, NotNil)
	dl, ok := errors.Cause(err).(*tikverr.ErrDeadlock)
	c.Assert(ok, IsTrue)

	waitChain := dl.GetWaitChain()
	c.Assert(len(waitChain), Equals, 2)
	checkWaitChainEntry(txns, waitChain[0], 0, 1)
	checkWaitChainEntry(txns, waitChain[1], 1, 0)

	// Each transaction should be rolled back after its blocker being rolled back
	waitAndRollback(txns, 1)
	waitAndRollback(txns, 0)

	c.Log("test case 2: 3->2->0->1->3")
	txns = prepareTxns(4)

	makeWaitFor(txns, 0, 1)
	makeWaitFor(txns, 2, 0)
	makeWaitFor(txns, 1, 3)
	// Sleep for a while to make sure it has been blocked.
	time.Sleep(time.Millisecond * 100)

	err = tryLock(txns, 3, 2)
	c.Assert(err, NotNil)
	dl, ok = errors.Cause(err).(*tikverr.ErrDeadlock)
	c.Assert(ok, IsTrue)

	waitChain = dl.GetWaitChain()
	c.Assert(len(waitChain), Equals, 4)
	c.Logf("wait chain: \n** %v\n**%v\n**%v\n**%v\n", waitChain[0], waitChain[1], waitChain[2], waitChain[3])
	checkWaitChainEntry(txns, waitChain[0], 2, 0)
	checkWaitChainEntry(txns, waitChain[1], 0, 1)
	checkWaitChainEntry(txns, waitChain[2], 1, 3)
	checkWaitChainEntry(txns, waitChain[3], 3, 2)

	// Each transaction should be rolled back after its blocker being rolled back
	waitAndRollback(txns, 3)
	waitAndRollback(txns, 1)
	waitAndRollback(txns, 0)
	waitAndRollback(txns, 2)
}
