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
	"math"
	"math/rand"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type testCommitterSuite struct {
	OneByOneSuite
	cluster *mocktikv.Cluster
	store   *tikvStore
}

var _ = Suite(&testCommitterSuite{})

func (s *testCommitterSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithMultiRegions(s.cluster, []byte("a"), []byte("b"), []byte("c"))
	mvccStore, err := mocktikv.NewMVCCLevelDB("")
	c.Assert(err, IsNil)
	client := mocktikv.NewRPCClient(s.cluster, mvccStore)
	pdCli := &codecPDClient{mocktikv.NewPDClient(s.cluster)}
	spkv := NewMockSafePointKV()
	store, err := newTikvStore("mocktikv-store", pdCli, spkv, client, false)
	c.Assert(err, IsNil)
	store.EnableTxnLocalLatches(1024000)
	s.store = store
	CommitMaxBackoff = 2000
}

func (s *testCommitterSuite) TearDownSuite(c *C) {
	CommitMaxBackoff = 20000
	s.store.Close()
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testCommitterSuite) begin(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testCommitterSuite) checkValues(c *C, m map[string]string) {
	txn := s.begin(c)
	for k, v := range m {
		val, err := txn.Get(context.TODO(), []byte(k))
		c.Assert(err, IsNil)
		c.Assert(string(val), Equals, v)
	}
}

func (s *testCommitterSuite) mustCommit(c *C, m map[string]string) {
	txn := s.begin(c)
	for k, v := range m {
		err := txn.Set([]byte(k), []byte(v))
		c.Assert(err, IsNil)
	}
	err := txn.Commit(context.Background())
	c.Assert(err, IsNil)

	s.checkValues(c, m)
}

func randKV(keyLen, valLen int) (string, string) {
	const letters = "abc"
	k, v := make([]byte, keyLen), make([]byte, valLen)
	for i := range k {
		k[i] = letters[rand.Intn(len(letters))]
	}
	for i := range v {
		v[i] = letters[rand.Intn(len(letters))]
	}
	return string(k), string(v)
}

func (s *testCommitterSuite) TestCommitRollback(c *C) {
	s.mustCommit(c, map[string]string{
		"a": "a",
		"b": "b",
		"c": "c",
	})

	txn := s.begin(c)
	txn.Set([]byte("a"), []byte("a1"))
	txn.Set([]byte("b"), []byte("b1"))
	txn.Set([]byte("c"), []byte("c1"))

	s.mustCommit(c, map[string]string{
		"c": "c2",
	})

	err := txn.Commit(context.Background())
	c.Assert(err, NotNil)

	s.checkValues(c, map[string]string{
		"a": "a",
		"b": "b",
		"c": "c2",
	})
}

func (s *testCommitterSuite) TestPrewriteRollback(c *C) {
	s.mustCommit(c, map[string]string{
		"a": "a0",
		"b": "b0",
	})

	ctx := context.Background()
	txn1 := s.begin(c)
	err := txn1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn1.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn1, 0)
	c.Assert(err, IsNil)
	err = committer.prewriteKeys(NewBackoffer(ctx, prewriteMaxBackoff), committer.keys)
	c.Assert(err, IsNil)

	txn2 := s.begin(c)
	v, err := txn2.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a0"))

	err = committer.prewriteKeys(NewBackoffer(ctx, prewriteMaxBackoff), committer.keys)
	if err != nil {
		// Retry.
		txn1 = s.begin(c)
		err = txn1.Set([]byte("a"), []byte("a1"))
		c.Assert(err, IsNil)
		err = txn1.Set([]byte("b"), []byte("b1"))
		c.Assert(err, IsNil)
		committer, err = newTwoPhaseCommitterWithInit(txn1, 0)
		c.Assert(err, IsNil)
		err = committer.prewriteKeys(NewBackoffer(ctx, prewriteMaxBackoff), committer.keys)
		c.Assert(err, IsNil)
	}
	committer.commitTS, err = s.store.oracle.GetTimestamp(ctx)
	c.Assert(err, IsNil)
	err = committer.commitKeys(NewBackoffer(ctx, CommitMaxBackoff), [][]byte{[]byte("a")})
	c.Assert(err, IsNil)

	txn3 := s.begin(c)
	v, err = txn3.Get(context.TODO(), []byte("b"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("b1"))
}

func (s *testCommitterSuite) TestContextCancel(c *C) {
	txn1 := s.begin(c)
	err := txn1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn1.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn1, 0)
	c.Assert(err, IsNil)

	bo := NewBackoffer(context.Background(), prewriteMaxBackoff)
	backoffer, cancel := bo.Fork()
	cancel() // cancel the context
	err = committer.prewriteKeys(backoffer, committer.keys)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
}

func (s *testCommitterSuite) TestContextCancel2(c *C) {
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("b"), []byte("b"))
	c.Assert(err, IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	cancel()
	// Secondary keys should not be canceled.
	time.Sleep(time.Millisecond * 20)
	c.Assert(s.isKeyLocked(c, []byte("b")), IsFalse)
}

func (s *testCommitterSuite) TestContextCancelRetryable(c *C) {
	txn1, txn2, txn3 := s.begin(c), s.begin(c), s.begin(c)
	// txn1 locks "b"
	err := txn1.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn1, 0)
	c.Assert(err, IsNil)
	err = committer.prewriteKeys(NewBackoffer(context.Background(), prewriteMaxBackoff), committer.keys)
	c.Assert(err, IsNil)
	// txn3 writes "c"
	err = txn3.Set([]byte("c"), []byte("c3"))
	c.Assert(err, IsNil)
	err = txn3.Commit(context.Background())
	c.Assert(err, IsNil)
	// txn2 writes "a"(PK), "b", "c" on different regions.
	// "c" will return a retryable error.
	// "b" will get a Locked error first, then the context must be canceled after backoff for lock.
	err = txn2.Set([]byte("a"), []byte("a2"))
	c.Assert(err, IsNil)
	err = txn2.Set([]byte("b"), []byte("b2"))
	c.Assert(err, IsNil)
	err = txn2.Set([]byte("c"), []byte("c2"))
	c.Assert(err, IsNil)
	err = txn2.Commit(context.Background())
	c.Assert(err, NotNil)
	c.Assert(kv.ErrWriteConflictInTiDB.Equal(err), IsTrue, Commentf("err: %s", err))
}

func (s *testCommitterSuite) mustGetRegionID(c *C, key []byte) uint64 {
	loc, err := s.store.regionCache.LocateKey(NewBackoffer(context.Background(), getMaxBackoff), key)
	c.Assert(err, IsNil)
	return loc.Region.id
}

func (s *testCommitterSuite) isKeyLocked(c *C, key []byte) bool {
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	bo := NewBackoffer(context.Background(), getMaxBackoff)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver.Ver,
	})
	loc, err := s.store.regionCache.LocateKey(bo, key)
	c.Assert(err, IsNil)
	resp, err := s.store.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErr := (resp.Resp.(*kvrpcpb.GetResponse)).GetError()
	return keyErr.GetLocked() != nil
}

func (s *testCommitterSuite) TestPrewriteCancel(c *C) {
	// Setup region delays for key "b" and "c".
	delays := map[uint64]time.Duration{
		s.mustGetRegionID(c, []byte("b")): time.Millisecond * 10,
		s.mustGetRegionID(c, []byte("c")): time.Millisecond * 20,
	}
	s.store.client = &slowClient{
		Client:       s.store.client,
		regionDelays: delays,
	}

	txn1, txn2 := s.begin(c), s.begin(c)
	// txn2 writes "b"
	err := txn2.Set([]byte("b"), []byte("b2"))
	c.Assert(err, IsNil)
	err = txn2.Commit(context.Background())
	c.Assert(err, IsNil)
	// txn1 writes "a"(PK), "b", "c" on different regions.
	// "b" will return an error and cancel commit.
	err = txn1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn1.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	err = txn1.Set([]byte("c"), []byte("c1"))
	c.Assert(err, IsNil)
	err = txn1.Commit(context.Background())
	c.Assert(err, NotNil)
	// "c" should be cleaned up in reasonable time.
	for i := 0; i < 50; i++ {
		if !s.isKeyLocked(c, []byte("c")) {
			return
		}
		time.Sleep(time.Millisecond * 10)
	}
	c.Fail()
}

// slowClient wraps rpcClient and makes some regions respond with delay.
type slowClient struct {
	Client
	regionDelays map[uint64]time.Duration
}

func (c *slowClient) SendReq(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	for id, delay := range c.regionDelays {
		reqCtx := &req.Context
		if reqCtx.GetRegionId() == id {
			time.Sleep(delay)
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func (s *testCommitterSuite) TestIllegalTso(c *C) {
	txn := s.begin(c)
	data := map[string]string{
		"name": "aa",
		"age":  "12",
	}
	for k, v := range data {
		err := txn.Set([]byte(k), []byte(v))
		c.Assert(err, IsNil)
	}
	// make start ts bigger.
	txn.startTS = uint64(math.MaxUint64)
	err := txn.Commit(context.Background())
	c.Assert(err, NotNil)
	errMsgMustContain(c, err, "invalid txnStartTS")
}

func errMsgMustContain(c *C, err error, msg string) {
	c.Assert(strings.Contains(err.Error(), msg), IsTrue)
}

func newTwoPhaseCommitterWithInit(txn *tikvTxn, connID uint64) (*twoPhaseCommitter, error) {
	c, err := newTwoPhaseCommitter(txn, connID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err = c.initKeysAndMutations(); err != nil {
		return nil, errors.Trace(err)
	}
	return c, nil
}

func (s *testCommitterSuite) TestCommitBeforePrewrite(c *C) {
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	commiter, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	ctx := context.Background()
	err = commiter.cleanupKeys(NewBackoffer(ctx, cleanupMaxBackoff), commiter.keys)
	c.Assert(err, IsNil)
	err = commiter.prewriteKeys(NewBackoffer(ctx, prewriteMaxBackoff), commiter.keys)
	c.Assert(err, NotNil)
	errMsgMustContain(c, err, "conflictCommitTS")
}

func (s *testCommitterSuite) TestPrewritePrimaryKeyFailed(c *C) {
	// commit (a,a1)
	txn1 := s.begin(c)
	err := txn1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = txn1.Commit(context.Background())
	c.Assert(err, IsNil)

	// check a
	txn := s.begin(c)
	v, err := txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a1"))

	// set txn2's startTs before txn1's
	txn2 := s.begin(c)
	txn2.startTS = txn1.startTS - 1
	err = txn2.Set([]byte("a"), []byte("a2"))
	c.Assert(err, IsNil)
	err = txn2.Set([]byte("b"), []byte("b2"))
	c.Assert(err, IsNil)
	// prewrite:primary a failed, b success
	err = txn2.Commit(context.Background())
	c.Assert(err, NotNil)

	// txn2 failed with a rollback for record a.
	txn = s.begin(c)
	v, err = txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a1"))
	_, err = txn.Get(context.TODO(), []byte("b"))
	errMsgMustContain(c, err, "key not exist")

	// clean again, shouldn't be failed when a rollback already exist.
	ctx := context.Background()
	commiter, err := newTwoPhaseCommitterWithInit(txn2, 0)
	c.Assert(err, IsNil)
	err = commiter.cleanupKeys(NewBackoffer(ctx, cleanupMaxBackoff), commiter.keys)
	c.Assert(err, IsNil)

	// check the data after rollback twice.
	txn = s.begin(c)
	v, err = txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a1"))

	// update data in a new txn, should be success.
	err = txn.Set([]byte("a"), []byte("a3"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	// check value
	txn = s.begin(c)
	v, err = txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a3"))
}

func (s *testCommitterSuite) TestWrittenKeysOnConflict(c *C) {
	// This test checks that when there is a write conflict, written keys is collected,
	// so we can use it to clean up keys.
	region, _ := s.cluster.GetRegionByKey([]byte("x"))
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(region.Id, newRegionID, []byte("y"), []uint64{newPeerID}, newPeerID)
	var totalTime time.Duration
	for i := 0; i < 10; i++ {
		txn1 := s.begin(c)
		txn2 := s.begin(c)
		txn2.Set([]byte("x1"), []byte("1"))
		commiter2, err := newTwoPhaseCommitterWithInit(txn2, 2)
		c.Assert(err, IsNil)
		err = commiter2.execute(context.Background())
		c.Assert(err, IsNil)
		txn1.Set([]byte("x1"), []byte("1"))
		txn1.Set([]byte("y1"), []byte("2"))
		commiter1, err := newTwoPhaseCommitterWithInit(txn1, 2)
		c.Assert(err, IsNil)
		err = commiter1.execute(context.Background())
		c.Assert(err, NotNil)
		commiter1.cleanWg.Wait()
		txn3 := s.begin(c)
		start := time.Now()
		txn3.Get(context.TODO(), []byte("y1"))
		totalTime += time.Since(start)
		txn3.Commit(context.Background())
	}
	c.Assert(totalTime, Less, time.Millisecond*200)
}

func (s *testCommitterSuite) TestPrewriteTxnSize(c *C) {
	// Prepare two regions first: (, 100) and [100, )
	region, _ := s.cluster.GetRegionByKey([]byte{50})
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(region.Id, newRegionID, []byte{100}, []uint64{newPeerID}, newPeerID)

	txn := s.begin(c)
	var val [1024]byte
	for i := byte(50); i < 120; i++ {
		err := txn.Set([]byte{i}, val[:])
		c.Assert(err, IsNil)
	}

	commiter, err := newTwoPhaseCommitterWithInit(txn, 1)
	c.Assert(err, IsNil)

	ctx := context.Background()
	err = commiter.prewriteKeys(NewBackoffer(ctx, prewriteMaxBackoff), commiter.keys)
	c.Assert(err, IsNil)

	// Check the written locks in the first region (50 keys)
	for i := byte(50); i < 100; i++ {
		lock := s.getLockInfo(c, []byte{i})
		c.Assert(int(lock.TxnSize), Equals, 50)
	}

	// Check the written locks in the second region (20 keys)
	for i := byte(100); i < 120; i++ {
		lock := s.getLockInfo(c, []byte{i})
		c.Assert(int(lock.TxnSize), Equals, 20)
	}
}

func (s *testCommitterSuite) TestPessimisticPrewriteRequest(c *C) {
	// This test checks that the isPessimisticLock field is set in the request even when no keys are pessimistic lock.
	txn := s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	err := txn.Set([]byte("t1"), []byte("v1"))
	c.Assert(err, IsNil)
	commiter, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	commiter.forUpdateTS = 100
	var batch batchKeys
	batch.keys = append(batch.keys, []byte("t1"))
	batch.region = RegionVerID{1, 1, 1}
	req := commiter.buildPrewriteRequest(batch, 1)
	c.Assert(len(req.Prewrite().IsPessimisticLock), Greater, 0)
	c.Assert(req.Prewrite().ForUpdateTs, Equals, uint64(100))
}

func (s *testCommitterSuite) TestUnsetPrimaryKey(c *C) {
	// This test checks that the isPessimisticLock field is set in the request even when no keys are pessimistic lock.
	key := kv.Key("key")
	txn := s.begin(c)
	c.Assert(txn.Set(key, key), IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)

	txn = s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	txn.SetOption(kv.PresumeKeyNotExists, nil)
	txn.SetOption(kv.PresumeKeyNotExistsError, kv.NewExistErrInfo("name", "value"))
	_, _ = txn.us.Get(context.TODO(), key)
	c.Assert(txn.Set(key, key), IsNil)
	txn.DelOption(kv.PresumeKeyNotExistsError)
	txn.DelOption(kv.PresumeKeyNotExists)
	err := txn.LockKeys(context.Background(), txn.startTS, key)
	c.Assert(err, NotNil)
	c.Assert(txn.Delete(key), IsNil)
	key2 := kv.Key("key2")
	c.Assert(txn.Set(key2, key2), IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testCommitterSuite) TestPessimisticLockedKeysDedup(c *C) {
	txn := s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	err := txn.LockKeys(context.Background(), 100, kv.Key("abc"), kv.Key("def"))
	c.Assert(err, IsNil)
	err = txn.LockKeys(context.Background(), 100, kv.Key("abc"), kv.Key("def"))
	c.Assert(err, IsNil)
	c.Assert(txn.lockKeys, HasLen, 2)
}

func (s *testCommitterSuite) TestPessimisticTTL(c *C) {
	key := kv.Key("key")
	txn := s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	time.Sleep(time.Millisecond * 100)
	err := txn.LockKeys(context.Background(), txn.startTS, key)
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond * 100)
	key2 := kv.Key("key2")
	err = txn.LockKeys(context.Background(), txn.startTS, key2)
	c.Assert(err, IsNil)
	lockInfo := s.getLockInfo(c, key)
	elapsedTTL := lockInfo.LockTtl - PessimisticLockTTL
	c.Assert(elapsedTTL, GreaterEqual, uint64(100))

	lr := newLockResolver(s.store)
	bo := NewBackoffer(context.Background(), getMaxBackoff)
	status, err := lr.getTxnStatus(bo, txn.startTS, key2, 0, txn.startTS)
	c.Assert(err, IsNil)
	c.Assert(status.ttl, Equals, lockInfo.LockTtl)

	// Check primary lock TTL is auto increasing while the pessimistic txn is ongoing.
	for i := 0; i < 50; i++ {
		lockInfoNew := s.getLockInfo(c, key)
		if lockInfoNew.LockTtl > lockInfo.LockTtl {
			currentTS, err := lr.store.GetOracle().GetTimestamp(bo.ctx)
			c.Assert(err, IsNil)
			// Check that the TTL is update to a reasonable range.
			expire := oracle.ExtractPhysical(txn.startTS) + int64(lockInfoNew.LockTtl)
			now := oracle.ExtractPhysical(currentTS)
			c.Assert(expire > now, IsTrue)
			c.Assert(uint64(expire-now) <= PessimisticLockTTL, IsTrue)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(false, IsTrue, Commentf("update pessimistic ttl fail"))
}

func (s *testCommitterSuite) getLockInfo(c *C, key []byte) *kvrpcpb.LockInfo {
	txn := s.begin(c)
	err := txn.Set(key, key)
	c.Assert(err, IsNil)
	commiter, err := newTwoPhaseCommitterWithInit(txn, 1)
	c.Assert(err, IsNil)
	bo := NewBackoffer(context.Background(), getMaxBackoff)
	loc, err := s.store.regionCache.LocateKey(bo, key)
	c.Assert(err, IsNil)
	batch := batchKeys{region: loc.Region, keys: [][]byte{key}}
	req := commiter.buildPrewriteRequest(batch, 1)
	resp, err := s.store.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErrs := (resp.Resp.(*kvrpcpb.PrewriteResponse)).Errors
	c.Assert(keyErrs, HasLen, 1)
	locked := keyErrs[0].Locked
	c.Assert(locked, NotNil)
	return locked
}
