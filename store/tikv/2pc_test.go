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
	"github.com/pingcap/tidb/sessionctx/variable"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type testCommitterSuite struct {
	OneByOneSuite
	cluster   *mocktikv.Cluster
	store     *tikvStore
	mvccStore mocktikv.MVCCStore
}

var _ = SerialSuites(&testCommitterSuite{})

func (s *testCommitterSuite) SetUpSuite(c *C) {
	atomic.StoreUint64(&ManagedLockTTL, 3000) // 3s
	s.OneByOneSuite.SetUpSuite(c)
}

func (s *testCommitterSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithMultiRegions(s.cluster, []byte("a"), []byte("b"), []byte("c"))
	mvccStore, err := mocktikv.NewMVCCLevelDB("")
	c.Assert(err, IsNil)
	s.mvccStore = mvccStore
	client := mocktikv.NewRPCClient(s.cluster, mvccStore)
	pdCli := &codecPDClient{mocktikv.NewPDClient(s.cluster)}
	spkv := NewMockSafePointKV()
	store, err := newTikvStore("mocktikv-store", pdCli, spkv, client, false, nil)
	c.Assert(err, IsNil)
	store.EnableTxnLocalLatches(1024000)
	s.store = store
	CommitMaxBackoff = 1000
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
	err = committer.prewriteMutations(NewBackoffer(ctx, PrewriteMaxBackoff), committer.mutations)
	c.Assert(err, IsNil)

	txn2 := s.begin(c)
	v, err := txn2.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a0"))

	err = committer.prewriteMutations(NewBackoffer(ctx, PrewriteMaxBackoff), committer.mutations)
	if err != nil {
		// Retry.
		txn1 = s.begin(c)
		err = txn1.Set([]byte("a"), []byte("a1"))
		c.Assert(err, IsNil)
		err = txn1.Set([]byte("b"), []byte("b1"))
		c.Assert(err, IsNil)
		committer, err = newTwoPhaseCommitterWithInit(txn1, 0)
		c.Assert(err, IsNil)
		err = committer.prewriteMutations(NewBackoffer(ctx, PrewriteMaxBackoff), committer.mutations)
		c.Assert(err, IsNil)
	}
	committer.commitTS, err = s.store.oracle.GetTimestamp(ctx)
	c.Assert(err, IsNil)
	err = committer.commitMutations(NewBackoffer(ctx, CommitMaxBackoff), committerMutations{keys: [][]byte{[]byte("a")}})
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

	bo := NewBackoffer(context.Background(), PrewriteMaxBackoff)
	backoffer, cancel := bo.Fork()
	cancel() // cancel the context
	err = committer.prewriteMutations(backoffer, committer.mutations)
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
	err = committer.prewriteMutations(NewBackoffer(context.Background(), PrewriteMaxBackoff), committer.mutations)
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
	c, err := newTwoPhaseCommitter(context.Background(), txn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.connID = connID
	c.concurrency = variable.DefTwoPhaseCommitterConcurrency
	if err = c.initKeysAndMutations(); err != nil {
		return nil, errors.Trace(err)
	}
	return c, nil
}

func (s *testCommitterSuite) TestCommitBeforePrewrite(c *C) {
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	ctx := context.Background()
	err = committer.cleanupMutations(NewBackoffer(ctx, cleanupMaxBackoff), committer.mutations)
	c.Assert(err, IsNil)
	err = committer.prewriteMutations(NewBackoffer(ctx, PrewriteMaxBackoff), committer.mutations)
	c.Assert(err, NotNil)
	errMsgMustContain(c, err, "already rolled back")
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
	committer, err := newTwoPhaseCommitterWithInit(txn2, 0)
	c.Assert(err, IsNil)
	err = committer.cleanupMutations(NewBackoffer(ctx, cleanupMaxBackoff), committer.mutations)
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
		committer2, err := newTwoPhaseCommitterWithInit(txn2, 2)
		c.Assert(err, IsNil)
		err = committer2.execute(context.Background())
		c.Assert(err, IsNil)
		txn1.Set([]byte("x1"), []byte("1"))
		txn1.Set([]byte("y1"), []byte("2"))
		committer1, err := newTwoPhaseCommitterWithInit(txn1, 2)
		c.Assert(err, IsNil)
		err = committer1.execute(context.Background())
		c.Assert(err, NotNil)
		committer1.cleanWg.Wait()
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

	committer, err := newTwoPhaseCommitterWithInit(txn, 1)
	c.Assert(err, IsNil)

	ctx := context.Background()
	err = committer.prewriteMutations(NewBackoffer(ctx, PrewriteMaxBackoff), committer.mutations)
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

func (s *testCommitterSuite) TestRejectCommitTS(c *C) {
	txn := s.begin(c)
	c.Assert(txn.Set([]byte("x"), []byte("v")), IsNil)

	committer, err := newTwoPhaseCommitterWithInit(txn, 1)
	c.Assert(err, IsNil)
	bo := NewBackoffer(context.Background(), getMaxBackoff)
	loc, err := s.store.regionCache.LocateKey(bo, []byte("x"))
	c.Assert(err, IsNil)
	mutations := []*kvrpcpb.Mutation{
		{
			Op:    committer.mutations.ops[0],
			Key:   committer.mutations.keys[0],
			Value: committer.mutations.values[0],
		},
	}
	prewrite := &kvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  committer.primary(),
		StartVersion: committer.startTS,
		LockTtl:      committer.lockTTL,
		MinCommitTs:  committer.startTS + 100, // Set minCommitTS
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, prewrite)
	_, err = s.store.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)

	// Make commitTS less than minCommitTS.
	committer.commitTS = committer.startTS + 1
	// Ensure that the new commit ts is greater than minCommitTS when retry
	time.Sleep(3 * time.Millisecond)
	err = committer.commitMutations(bo, committer.mutations)
	c.Assert(err, IsNil)

	// Use startTS+2 to read the data and get nothing.
	// Use max.Uint64 to read the data and success.
	// That means the final commitTS > startTS+2, it's not the one we provide.
	// So we cover the rety commitTS logic.
	txn1, err := s.store.BeginWithStartTS(committer.startTS + 2)
	c.Assert(err, IsNil)
	_, err = txn1.Get(bo.ctx, []byte("x"))
	c.Assert(kv.IsErrNotFound(err), IsTrue)

	txn2, err := s.store.BeginWithStartTS(math.MaxUint64)
	c.Assert(err, IsNil)
	val, err := txn2.Get(bo.ctx, []byte("x"))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(val, []byte("v")), IsTrue)
}

func (s *testCommitterSuite) TestPessimisticPrewriteRequest(c *C) {
	// This test checks that the isPessimisticLock field is set in the request even when no keys are pessimistic lock.
	txn := s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	err := txn.Set([]byte("t1"), []byte("v1"))
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn, 0)
	c.Assert(err, IsNil)
	committer.forUpdateTS = 100
	var batch batchMutations
	batch.mutations = committer.mutations.subRange(0, 1)
	batch.region = RegionVerID{1, 1, 1}
	req := committer.buildPrewriteRequest(batch, 1)
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
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.startTS, WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, key)
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
	lockCtx := &kv.LockCtx{ForUpdateTS: 100, WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, kv.Key("abc"), kv.Key("def"))
	c.Assert(err, IsNil)
	lockCtx = &kv.LockCtx{ForUpdateTS: 100, WaitStartTime: time.Now()}
	err = txn.LockKeys(context.Background(), lockCtx, kv.Key("abc"), kv.Key("def"))
	c.Assert(err, IsNil)
	c.Assert(txn.lockKeys, HasLen, 2)
}

func (s *testCommitterSuite) TestPessimisticTTL(c *C) {
	key := kv.Key("key")
	txn := s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	time.Sleep(time.Millisecond * 100)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.startTS, WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond * 100)
	key2 := kv.Key("key2")
	lockCtx = &kv.LockCtx{ForUpdateTS: txn.startTS, WaitStartTime: time.Now()}
	err = txn.LockKeys(context.Background(), lockCtx, key2)
	c.Assert(err, IsNil)
	lockInfo := s.getLockInfo(c, key)
	msBeforeLockExpired := s.store.GetOracle().UntilExpired(txn.StartTS(), lockInfo.LockTtl)
	c.Assert(msBeforeLockExpired, GreaterEqual, int64(100))

	lr := newLockResolver(s.store)
	bo := NewBackoffer(context.Background(), getMaxBackoff)
	status, err := lr.getTxnStatus(bo, txn.startTS, key2, 0, txn.startTS, true)
	c.Assert(err, IsNil)
	c.Assert(status.ttl, GreaterEqual, lockInfo.LockTtl)

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
			c.Assert(uint64(expire-now) <= atomic.LoadUint64(&ManagedLockTTL), IsTrue)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(false, IsTrue, Commentf("update pessimistic ttl fail"))
}

func (s *testCommitterSuite) TestPessimisticLockReturnValues(c *C) {
	key := kv.Key("key")
	key2 := kv.Key("key2")
	txn := s.begin(c)
	c.Assert(txn.Set(key, key), IsNil)
	c.Assert(txn.Set(key2, key2), IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	txn = s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.startTS, WaitStartTime: time.Now()}
	lockCtx.ReturnValues = true
	lockCtx.Values = map[string]kv.ReturnedValue{}
	c.Assert(txn.LockKeys(context.Background(), lockCtx, key, key2), IsNil)
	c.Assert(lockCtx.Values, HasLen, 2)
	c.Assert(lockCtx.Values[string(key)].Value, BytesEquals, []byte(key))
	c.Assert(lockCtx.Values[string(key2)].Value, BytesEquals, []byte(key2))
}

// TestElapsedTTL tests that elapsed time is correct even if ts physical time is greater than local time.
func (s *testCommitterSuite) TestElapsedTTL(c *C) {
	key := kv.Key("key")
	txn := s.begin(c)
	txn.startTS = oracle.ComposeTS(oracle.GetPhysical(time.Now().Add(time.Second*10)), 1)
	txn.SetOption(kv.Pessimistic, true)
	time.Sleep(time.Millisecond * 100)
	lockCtx := &kv.LockCtx{
		ForUpdateTS:   oracle.ComposeTS(oracle.ExtractPhysical(txn.startTS)+100, 1),
		WaitStartTime: time.Now(),
	}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	c.Assert(err, IsNil)
	lockInfo := s.getLockInfo(c, key)
	c.Assert(lockInfo.LockTtl-atomic.LoadUint64(&ManagedLockTTL), GreaterEqual, uint64(100))
	c.Assert(lockInfo.LockTtl-atomic.LoadUint64(&ManagedLockTTL), Less, uint64(150))
}

// TestAcquireFalseTimeoutLock tests acquiring a key which is a secondary key of another transaction.
// The lock's own TTL is expired but the primary key is still alive due to heartbeats.
func (s *testCommitterSuite) TestAcquireFalseTimeoutLock(c *C) {
	atomic.StoreUint64(&ManagedLockTTL, 1000)       // 1s
	defer atomic.StoreUint64(&ManagedLockTTL, 3000) // restore default test value

	// k1 is the primary lock of txn1
	k1 := kv.Key("k1")
	// k2 is a secondary lock of txn1 and a key txn2 wants to lock
	k2 := kv.Key("k2")

	txn1 := s.begin(c)
	txn1.SetOption(kv.Pessimistic, true)
	// lock the primary key
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.startTS, WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1)
	c.Assert(err, IsNil)
	// lock the secondary key
	lockCtx = &kv.LockCtx{ForUpdateTS: txn1.startTS, WaitStartTime: time.Now()}
	err = txn1.LockKeys(context.Background(), lockCtx, k2)
	c.Assert(err, IsNil)

	// Heartbeats will increase the TTL of the primary key

	// wait until secondary key exceeds its own TTL
	time.Sleep(time.Duration(atomic.LoadUint64(&ManagedLockTTL)) * time.Millisecond)
	txn2 := s.begin(c)
	txn2.SetOption(kv.Pessimistic, true)

	// test no wait
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.startTS, LockWaitTime: kv.LockNoWait, WaitStartTime: time.Now()}
	startTime := time.Now()
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	elapsed := time.Since(startTime)
	// cannot acquire lock immediately thus error
	c.Assert(err.Error(), Equals, ErrLockAcquireFailAndNoWaitSet.Error())
	// it should return immediately
	c.Assert(elapsed, Less, 50*time.Millisecond)

	// test for wait limited time (200ms)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.startTS, LockWaitTime: 200, WaitStartTime: time.Now()}
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	// cannot acquire lock in time thus error
	c.Assert(err.Error(), Equals, ErrLockWaitTimeout.Error())
}

func (s *testCommitterSuite) getLockInfo(c *C, key []byte) *kvrpcpb.LockInfo {
	txn := s.begin(c)
	err := txn.Set(key, key)
	c.Assert(err, IsNil)
	committer, err := newTwoPhaseCommitterWithInit(txn, 1)
	c.Assert(err, IsNil)
	bo := NewBackoffer(context.Background(), getMaxBackoff)
	loc, err := s.store.regionCache.LocateKey(bo, key)
	c.Assert(err, IsNil)
	batch := batchMutations{region: loc.Region, mutations: committer.mutations.subRange(0, 1)}
	req := committer.buildPrewriteRequest(batch, 1)
	resp, err := s.store.SendReq(bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErrs := (resp.Resp.(*kvrpcpb.PrewriteResponse)).Errors
	c.Assert(keyErrs, HasLen, 1)
	locked := keyErrs[0].Locked
	c.Assert(locked, NotNil)
	return locked
}

func (s *testCommitterSuite) TestPkNotFound(c *C) {
	atomic.StoreUint64(&ManagedLockTTL, 100)        // 100ms
	defer atomic.StoreUint64(&ManagedLockTTL, 3000) // restore default value
	// k1 is the primary lock of txn1
	k1 := kv.Key("k1")
	// k2 is a secondary lock of txn1 and a key txn2 wants to lock
	k2 := kv.Key("k2")
	k3 := kv.Key("k3")

	txn1 := s.begin(c)
	txn1.SetOption(kv.Pessimistic, true)
	// lock the primary key
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.startTS, WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1)
	c.Assert(err, IsNil)
	// lock the secondary key
	lockCtx = &kv.LockCtx{ForUpdateTS: txn1.startTS, WaitStartTime: time.Now()}
	err = txn1.LockKeys(context.Background(), lockCtx, k2)
	c.Assert(err, IsNil)

	// Stop txn ttl manager and remove primary key, like tidb server crashes and the priamry key lock does not exists actually,
	// while the secondary lock operation succeeded
	bo := NewBackoffer(context.Background(), pessimisticLockMaxBackoff)
	txn1.committer.ttlManager.close()
	err = txn1.committer.pessimisticRollbackMutations(bo, committerMutations{keys: [][]byte{k1}})
	c.Assert(err, IsNil)

	// Txn2 tries to lock the secondary key k2, dead loop if the left secondary lock by txn1 not resolved
	txn2 := s.begin(c)
	txn2.SetOption(kv.Pessimistic, true)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.startTS, WaitStartTime: time.Now()}
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	c.Assert(err, IsNil)

	// Using smaller forUpdateTS cannot rollback this lock, other lock will fail
	lockKey3 := &Lock{
		Key:             k3,
		Primary:         k1,
		TxnID:           txn1.startTS,
		TTL:             ManagedLockTTL,
		TxnSize:         txnCommitBatchSize,
		LockType:        kvrpcpb.Op_PessimisticLock,
		LockForUpdateTS: txn1.startTS - 1,
	}
	cleanTxns := make(map[RegionVerID]struct{})
	err = s.store.lockResolver.resolvePessimisticLock(bo, lockKey3, cleanTxns)
	c.Assert(err, IsNil)

	lockCtx = &kv.LockCtx{ForUpdateTS: txn1.startTS, WaitStartTime: time.Now()}
	err = txn1.LockKeys(context.Background(), lockCtx, k3)
	c.Assert(err, IsNil)
	txn3 := s.begin(c)
	txn3.SetOption(kv.Pessimistic, true)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn1.startTS - 1, WaitStartTime: time.Now(), LockWaitTime: kv.LockNoWait}
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/txnNotFoundRetTTL", "return"), IsNil)
	err = txn3.LockKeys(context.Background(), lockCtx, k3)
	c.Assert(err.Error(), Equals, ErrLockAcquireFailAndNoWaitSet.Error())
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/txnNotFoundRetTTL"), IsNil)
}

func (s *testCommitterSuite) TestPessimisticLockPrimary(c *C) {
	// a is the primary lock of txn1
	k1 := kv.Key("a")
	// b is a secondary lock of txn1 and a key txn2 wants to lock, b is on another region
	k2 := kv.Key("b")

	txn1 := s.begin(c)
	txn1.SetOption(kv.Pessimistic, true)
	// txn1 lock k1
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.startTS, WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1)
	c.Assert(err, IsNil)

	// txn2 wants to lock k1, k2, k1(pk) is blocked by txn1, pessimisticLockKeys has been changed to
	// lock primary key first and then secondary keys concurrently, k2 should not be locked by txn2
	doneCh := make(chan error)
	go func() {
		txn2 := s.begin(c)
		txn2.SetOption(kv.Pessimistic, true)
		lockCtx2 := &kv.LockCtx{ForUpdateTS: txn2.startTS, WaitStartTime: time.Now(), LockWaitTime: 200}
		waitErr := txn2.LockKeys(context.Background(), lockCtx2, k1, k2)
		doneCh <- waitErr
	}()
	time.Sleep(50 * time.Millisecond)

	// txn3 should locks k2 successfully using no wait
	txn3 := s.begin(c)
	txn3.SetOption(kv.Pessimistic, true)
	lockCtx3 := &kv.LockCtx{ForUpdateTS: txn3.startTS, WaitStartTime: time.Now(), LockWaitTime: kv.LockNoWait}
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/txnNotFoundRetTTL", "return"), IsNil)
	err = txn3.LockKeys(context.Background(), lockCtx3, k2)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/txnNotFoundRetTTL"), IsNil)
	c.Assert(err, IsNil)
	waitErr := <-doneCh
	c.Assert(ErrLockWaitTimeout.Equal(waitErr), IsTrue)
}

func (c *twoPhaseCommitter) mutationsOfKeys(keys [][]byte) committerMutations {
	var res committerMutations
	for i := range c.mutations.keys {
		for _, key := range keys {
			if bytes.Equal(c.mutations.keys[i], key) {
				res.push(c.mutations.ops[i], c.mutations.keys[i], c.mutations.values[i], c.mutations.isPessimisticLock[i])
				break
			}
		}
	}
	return res
}

func (s *testCommitterSuite) TestCommitDeadLock(c *C) {
	// Split into two region and let k1 k2 in different regions.
	s.cluster.SplitKeys(s.mvccStore, kv.Key("z"), kv.Key("a"), 2)
	k1 := kv.Key("a_deadlock_k1")
	k2 := kv.Key("y_deadlock_k2")

	region1, _ := s.cluster.GetRegionByKey(k1)
	region2, _ := s.cluster.GetRegionByKey(k2)
	c.Assert(region1.Id != region2.Id, IsTrue)

	txn1 := s.begin(c)
	txn1.Set(k1, []byte("t1"))
	txn1.Set(k2, []byte("t1"))
	commit1, err := newTwoPhaseCommitterWithInit(txn1, 1)
	c.Assert(err, IsNil)
	commit1.primaryKey = k1
	commit1.txnSize = 1000 * 1024 * 1024
	commit1.lockTTL = txnLockTTL(txn1.startTime, commit1.txnSize)

	txn2 := s.begin(c)
	txn2.Set(k1, []byte("t2"))
	txn2.Set(k2, []byte("t2"))
	commit2, err := newTwoPhaseCommitterWithInit(txn2, 2)
	c.Assert(err, IsNil)
	commit2.primaryKey = k2
	commit2.txnSize = 1000 * 1024 * 1024
	commit2.lockTTL = txnLockTTL(txn1.startTime, commit2.txnSize)

	s.cluster.ScheduleDelay(txn2.startTS, region1.Id, 5*time.Millisecond)
	s.cluster.ScheduleDelay(txn1.startTS, region2.Id, 5*time.Millisecond)

	// Txn1 prewrites k1, k2 and txn2 prewrites k2, k1, the large txn
	// protocol run ttlManager and update their TTL, cause dead lock.
	ch := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		ch <- commit2.execute(context.Background())
		wg.Done()
	}()
	ch <- commit1.execute(context.Background())
	wg.Wait()
	close(ch)

	res := 0
	for e := range ch {
		if e != nil {
			res++
		}
	}
	c.Assert(res, Equals, 1)
}

// TestPushPessimisticLock tests that push forward the minCommiTS of pessimistic locks.
func (s *testCommitterSuite) TestPushPessimisticLock(c *C) {
	// k1 is the primary key.
	k1, k2 := kv.Key("a"), kv.Key("b")
	ctx := context.Background()

	txn1 := s.begin(c)
	txn1.SetOption(kv.Pessimistic, true)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.startTS, WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1, k2)
	c.Assert(err, IsNil)

	txn1.Set(k2, []byte("v2"))
	err = txn1.committer.initKeysAndMutations()
	c.Assert(err, IsNil)
	// Strip the prewrite of the primary key.
	txn1.committer.mutations = txn1.committer.mutations.subRange(1, 2)
	c.Assert(err, IsNil)
	err = txn1.committer.prewriteMutations(NewBackoffer(ctx, PrewriteMaxBackoff), txn1.committer.mutations)
	c.Assert(err, IsNil)
	// The primary lock is a pessimistic lock and the secondary lock is a optimistic lock.
	lock1 := s.getLockInfo(c, k1)
	c.Assert(lock1.LockType, Equals, kvrpcpb.Op_PessimisticLock)
	c.Assert(lock1.PrimaryLock, BytesEquals, []byte(k1))
	lock2 := s.getLockInfo(c, k2)
	c.Assert(lock2.LockType, Equals, kvrpcpb.Op_Put)
	c.Assert(lock2.PrimaryLock, BytesEquals, []byte(k1))

	txn2 := s.begin(c)
	start := time.Now()
	_, err = txn2.Get(ctx, k2)
	elapsed := time.Since(start)
	// The optimistic lock shouldn't block reads.
	c.Assert(elapsed, Less, 500*time.Millisecond)
	c.Assert(kv.IsErrNotFound(err), IsTrue)

	txn1.Rollback()
	txn2.Rollback()
}

// TestResolveMixed tests mixed resolve with left behind optimistic locks and pessimistic locks,
// using clean whole region resolve path
func (s *testCommitterSuite) TestResolveMixed(c *C) {
	atomic.StoreUint64(&ManagedLockTTL, 100)        // 100ms
	defer atomic.StoreUint64(&ManagedLockTTL, 3000) // restore default value
	ctx := context.Background()

	// pk is the primary lock of txn1
	pk := kv.Key("pk")
	secondaryLockkeys := make([]kv.Key, 0, bigTxnThreshold)
	for i := 0; i < bigTxnThreshold; i++ {
		optimisticLock := kv.Key(fmt.Sprintf("optimisticLockKey%d", i))
		secondaryLockkeys = append(secondaryLockkeys, optimisticLock)
	}
	pessimisticLockKey := kv.Key("pessimisticLockKey")

	// make the optimistic and pessimistic lock left with primary lock not found
	txn1 := s.begin(c)
	txn1.SetOption(kv.Pessimistic, true)
	// lock the primary key
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.startTS, WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, pk)
	c.Assert(err, IsNil)
	// lock the optimistic keys
	for i := 0; i < bigTxnThreshold; i++ {
		txn1.Set(secondaryLockkeys[i], []byte(fmt.Sprintf("v%d", i)))
	}
	err = txn1.committer.initKeysAndMutations()
	c.Assert(err, IsNil)
	err = txn1.committer.prewriteMutations(NewBackoffer(ctx, PrewriteMaxBackoff), txn1.committer.mutations)
	c.Assert(err, IsNil)
	// lock the pessimistic keys
	err = txn1.LockKeys(context.Background(), lockCtx, pessimisticLockKey)
	c.Assert(err, IsNil)
	lock1 := s.getLockInfo(c, pessimisticLockKey)
	c.Assert(lock1.LockType, Equals, kvrpcpb.Op_PessimisticLock)
	c.Assert(lock1.PrimaryLock, BytesEquals, []byte(pk))
	optimisticLockKey := secondaryLockkeys[0]
	lock2 := s.getLockInfo(c, optimisticLockKey)
	c.Assert(lock2.LockType, Equals, kvrpcpb.Op_Put)
	c.Assert(lock2.PrimaryLock, BytesEquals, []byte(pk))

	// stop txn ttl manager and remove primary key, make the other keys left behind
	bo := NewBackoffer(context.Background(), pessimisticLockMaxBackoff)
	txn1.committer.ttlManager.close()
	err = txn1.committer.pessimisticRollbackMutations(bo, committerMutations{keys: [][]byte{pk}})
	c.Assert(err, IsNil)

	// try to resolve the left optimistic locks, use clean whole region
	cleanTxns := make(map[RegionVerID]struct{})
	time.Sleep(time.Duration(atomic.LoadUint64(&ManagedLockTTL)) * time.Millisecond)
	optimisticLockInfo := s.getLockInfo(c, optimisticLockKey)
	lock := NewLock(optimisticLockInfo)
	err = s.store.lockResolver.resolveLock(NewBackoffer(ctx, pessimisticLockMaxBackoff), lock, TxnStatus{}, cleanTxns)
	c.Assert(err, IsNil)

	// txn2 tries to lock the pessimisticLockKey, the lock should has been resolved in clean whole region resolve
	txn2 := s.begin(c)
	txn2.SetOption(kv.Pessimistic, true)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.startTS, WaitStartTime: time.Now(), LockWaitTime: kv.LockNoWait}
	err = txn2.LockKeys(context.Background(), lockCtx, pessimisticLockKey)
	c.Assert(err, IsNil)

	err = txn1.Rollback()
	c.Assert(err, IsNil)
	err = txn2.Rollback()
	c.Assert(err, IsNil)
}
