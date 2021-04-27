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
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	tidbkv "github.com/pingcap/tidb/kv"
	drivertxn "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/config"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/mockstore/cluster"
	"github.com/pingcap/tidb/store/tikv/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

var (
	txnCommitBatchSize = tikv.ConfigProbe{}.GetTxnCommitBatchSize()
	bigTxnThreshold    = tikv.ConfigProbe{}.GetBigTxnThreshold()
)

type testCommitterSuite struct {
	OneByOneSuite
	cluster cluster.Cluster
	store   tikv.StoreProbe
}

var _ = SerialSuites(&testCommitterSuite{})

func (s *testCommitterSuite) SetUpSuite(c *C) {
	atomic.StoreUint64(&tikv.ManagedLockTTL, 3000) // 3s
	s.OneByOneSuite.SetUpSuite(c)
	atomic.StoreUint64(&tikv.CommitMaxBackoff, 1000)
}

func (s *testCommitterSuite) SetUpTest(c *C) {
	mvccStore, err := mocktikv.NewMVCCLevelDB("")
	c.Assert(err, IsNil)
	cluster := mocktikv.NewCluster(mvccStore)
	mocktikv.BootstrapWithMultiRegions(cluster, []byte("a"), []byte("b"), []byte("c"))
	s.cluster = cluster
	client := mocktikv.NewRPCClient(cluster, mvccStore, nil)
	pdCli := &tikv.CodecPDClient{Client: mocktikv.NewPDClient(cluster)}
	spkv := tikv.NewMockSafePointKV()
	store, err := tikv.NewKVStore("mocktikv-store", pdCli, spkv, client)
	store.EnableTxnLocalLatches(1024000)
	c.Assert(err, IsNil)

	// TODO: make it possible
	// store, err := mockstore.NewMockStore(
	// 	mockstore.WithStoreType(mockstore.MockTiKV),
	// 	mockstore.WithClusterInspector(func(c cluster.Cluster) {
	// 		mockstore.BootstrapWithMultiRegions(c, []byte("a"), []byte("b"), []byte("c"))
	// 		s.cluster = c
	// 	}),
	// 	mockstore.WithPDClientHijacker(func(c pd.Client) pd.Client {
	// 		return &codecPDClient{c}
	// 	}),
	// 	mockstore.WithTxnLocalLatches(1024000),
	// )
	// c.Assert(err, IsNil)

	s.store = tikv.StoreProbe{KVStore: store}
}

func (s *testCommitterSuite) TearDownSuite(c *C) {
	atomic.StoreUint64(&tikv.CommitMaxBackoff, 20000)
	s.store.Close()
	s.OneByOneSuite.TearDownSuite(c)
}

func (s *testCommitterSuite) begin(c *C) tikv.TxnProbe {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn
}

func (s *testCommitterSuite) beginAsyncCommit(c *C) tikv.TxnProbe {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.SetOption(kv.EnableAsyncCommit, true)
	return txn
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

func (s *testCommitterSuite) TestDeleteYourWritesTTL(c *C) {
	conf := *config.GetGlobalConfig()
	oldConf := conf
	defer config.StoreGlobalConfig(&oldConf)
	conf.TiKVClient.TTLRefreshedTxnSize = 0
	config.StoreGlobalConfig(&conf)

	{
		txn := s.begin(c)
		err := txn.GetMemBuffer().SetWithFlags([]byte("bb"), []byte{0}, kv.SetPresumeKeyNotExists)
		c.Assert(err, IsNil)
		err = txn.Set([]byte("ba"), []byte{1})
		c.Assert(err, IsNil)
		err = txn.Delete([]byte("bb"))
		c.Assert(err, IsNil)
		committer, err := txn.NewCommitter(0)
		c.Assert(err, IsNil)
		err = committer.PrewriteAllMutations(context.Background())
		c.Assert(err, IsNil)
		c.Check(committer.IsTTLRunning(), IsTrue)
	}

	{
		txn := s.begin(c)
		err := txn.GetMemBuffer().SetWithFlags([]byte("dd"), []byte{0}, kv.SetPresumeKeyNotExists)
		c.Assert(err, IsNil)
		err = txn.Set([]byte("de"), []byte{1})
		c.Assert(err, IsNil)
		err = txn.Delete([]byte("dd"))
		c.Assert(err, IsNil)
		committer, err := txn.NewCommitter(0)
		c.Assert(err, IsNil)
		err = committer.PrewriteAllMutations(context.Background())
		c.Assert(err, IsNil)
		c.Check(committer.IsTTLRunning(), IsTrue)
	}
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
	committer, err := txn1.NewCommitter(0)
	c.Assert(err, IsNil)
	err = committer.PrewriteAllMutations(ctx)
	c.Assert(err, IsNil)

	txn2 := s.begin(c)
	v, err := txn2.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, []byte("a0"))

	err = committer.PrewriteAllMutations(ctx)
	if err != nil {
		// Retry.
		txn1 = s.begin(c)
		err = txn1.Set([]byte("a"), []byte("a1"))
		c.Assert(err, IsNil)
		err = txn1.Set([]byte("b"), []byte("b1"))
		c.Assert(err, IsNil)
		committer, err = txn1.NewCommitter(0)
		c.Assert(err, IsNil)
		err = committer.PrewriteAllMutations(ctx)
		c.Assert(err, IsNil)
	}
	commitTS, err := s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	committer.SetCommitTS(commitTS)
	err = committer.CommitMutations(ctx)
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
	committer, err := txn1.NewCommitter(0)
	c.Assert(err, IsNil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel the context
	err = committer.PrewriteAllMutations(ctx)
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
	committer, err := txn1.NewCommitter(0)
	c.Assert(err, IsNil)
	err = committer.PrewriteAllMutations(context.Background())
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
	c.Assert(tidbkv.ErrWriteConflictInTiDB.Equal(err), IsTrue, Commentf("err: %s", err))
}

func (s *testCommitterSuite) TestContextCancelCausingUndetermined(c *C) {
	// For a normal transaction, if RPC returns context.Canceled error while sending commit
	// requests, the transaction should go to the undetermined state.
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("va"))
	c.Assert(err, IsNil)
	committer, err := txn.NewCommitter(0)
	c.Assert(err, IsNil)
	committer.PrewriteAllMutations(context.Background())
	c.Assert(err, IsNil)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/rpcContextCancelErr", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/rpcContextCancelErr"), IsNil)
	}()

	err = committer.CommitMutations(context.Background())
	c.Assert(committer.GetUndeterminedErr(), NotNil)
	c.Assert(errors.Cause(err), Equals, context.Canceled)
}

func (s *testCommitterSuite) mustGetRegionID(c *C, key []byte) uint64 {
	loc, err := s.store.GetRegionCache().LocateKey(tikv.NewBackofferWithVars(context.Background(), 500, nil), key)
	c.Assert(err, IsNil)
	return loc.Region.GetID()
}

func (s *testCommitterSuite) isKeyLocked(c *C, key []byte) bool {
	ver, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	bo := tikv.NewBackofferWithVars(context.Background(), 500, nil)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver,
	})
	loc, err := s.store.GetRegionCache().LocateKey(bo, key)
	c.Assert(err, IsNil)
	resp, err := s.store.SendReq(bo, req, loc.Region, 5000)
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
	s.store.SetTiKVClient(&slowClient{
		Client:       s.store.GetTiKVClient(),
		regionDelays: delays,
	})

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
	tikv.Client
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
	txn.SetStartTS(math.MaxUint64)
	err := txn.Commit(context.Background())
	c.Assert(err, NotNil)
	errMsgMustContain(c, err, "invalid txnStartTS")
}

func errMsgMustContain(c *C, err error, msg string) {
	c.Assert(strings.Contains(err.Error(), msg), IsTrue)
}

func (s *testCommitterSuite) TestCommitBeforePrewrite(c *C) {
	txn := s.begin(c)
	err := txn.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	committer, err := txn.NewCommitter(0)
	c.Assert(err, IsNil)
	ctx := context.Background()
	committer.Cleanup(ctx)
	err = committer.PrewriteAllMutations(ctx)
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
	txn2.SetStartTS(txn1.StartTS() - 1)
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
	committer, err := txn2.NewCommitter(0)
	c.Assert(err, IsNil)
	committer.Cleanup(ctx)

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
		committer2, err := txn2.NewCommitter(2)
		c.Assert(err, IsNil)
		err = committer2.Execute(context.Background())
		c.Assert(err, IsNil)
		txn1.Set([]byte("x1"), []byte("1"))
		txn1.Set([]byte("y1"), []byte("2"))
		committer1, err := txn1.NewCommitter(2)
		c.Assert(err, IsNil)
		err = committer1.Execute(context.Background())
		c.Assert(err, NotNil)
		committer1.WaitCleanup()
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

	committer, err := txn.NewCommitter(1)
	c.Assert(err, IsNil)

	ctx := context.Background()
	err = committer.PrewriteAllMutations(ctx)
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

	committer, err := txn.NewCommitter(1)
	c.Assert(err, IsNil)
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := s.store.GetRegionCache().LocateKey(bo, []byte("x"))
	c.Assert(err, IsNil)
	mutations := []*kvrpcpb.Mutation{
		{
			Op:    committer.GetMutations().GetOp(0),
			Key:   committer.GetMutations().GetKey(0),
			Value: committer.GetMutations().GetValue(0),
		},
	}
	prewrite := &kvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  committer.GetPrimaryKey(),
		StartVersion: committer.GetStartTS(),
		LockTtl:      committer.GetLockTTL(),
		MinCommitTs:  committer.GetStartTS() + 100, // Set minCommitTS
	}
	req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, prewrite)
	_, err = s.store.SendReq(bo, req, loc.Region, 5000)
	c.Assert(err, IsNil)

	// Make commitTS less than minCommitTS.
	committer.SetCommitTS(committer.GetStartTS() + 1)
	// Ensure that the new commit ts is greater than minCommitTS when retry
	time.Sleep(3 * time.Millisecond)
	err = committer.CommitMutations(context.Background())
	c.Assert(err, IsNil)

	// Use startTS+2 to read the data and get nothing.
	// Use max.Uint64 to read the data and success.
	// That means the final commitTS > startTS+2, it's not the one we provide.
	// So we cover the rety commitTS logic.
	txn1, err := s.store.BeginWithStartTS(oracle.GlobalTxnScope, committer.GetStartTS()+2)
	c.Assert(err, IsNil)
	_, err = txn1.Get(bo.GetCtx(), []byte("x"))
	c.Assert(tidbkv.IsErrNotFound(err), IsTrue)

	txn2, err := s.store.BeginWithStartTS(oracle.GlobalTxnScope, math.MaxUint64)
	c.Assert(err, IsNil)
	val, err := txn2.Get(bo.GetCtx(), []byte("x"))
	c.Assert(err, IsNil)
	c.Assert(bytes.Equal(val, []byte("v")), IsTrue)
}

func (s *testCommitterSuite) TestPessimisticPrewriteRequest(c *C) {
	// This test checks that the isPessimisticLock field is set in the request even when no keys are pessimistic lock.
	txn := s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	err := txn.Set([]byte("t1"), []byte("v1"))
	c.Assert(err, IsNil)
	committer, err := txn.NewCommitter(0)
	c.Assert(err, IsNil)
	committer.SetForUpdateTS(100)
	req := committer.BuildPrewriteRequest(1, 1, 1, committer.GetMutations().Slice(0, 1), 1)
	c.Assert(len(req.Prewrite().IsPessimisticLock), Greater, 0)
	c.Assert(req.Prewrite().ForUpdateTs, Equals, uint64(100))
}

func (s *testCommitterSuite) TestUnsetPrimaryKey(c *C) {
	// This test checks that the isPessimisticLock field is set in the request even when no keys are pessimistic lock.
	key := []byte("key")
	txn := s.begin(c)
	c.Assert(txn.Set(key, key), IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)

	txn = s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	_, _ = txn.GetUnionStore().Get(context.TODO(), key)
	c.Assert(txn.GetMemBuffer().SetWithFlags(key, key, kv.SetPresumeKeyNotExists), IsNil)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	c.Assert(err, NotNil)
	c.Assert(txn.Delete(key), IsNil)
	key2 := []byte("key2")
	c.Assert(txn.Set(key2, key2), IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
}

func (s *testCommitterSuite) TestPessimisticLockedKeysDedup(c *C) {
	txn := s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	lockCtx := &kv.LockCtx{ForUpdateTS: 100, WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, []byte("abc"), []byte("def"))
	c.Assert(err, IsNil)
	lockCtx = &kv.LockCtx{ForUpdateTS: 100, WaitStartTime: time.Now()}
	err = txn.LockKeys(context.Background(), lockCtx, []byte("abc"), []byte("def"))
	c.Assert(err, IsNil)
	c.Assert(txn.CollectLockedKeys(), HasLen, 2)
}

func (s *testCommitterSuite) TestPessimisticTTL(c *C) {
	key := []byte("key")
	txn := s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	time.Sleep(time.Millisecond * 100)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond * 100)
	key2 := []byte("key2")
	lockCtx = &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
	err = txn.LockKeys(context.Background(), lockCtx, key2)
	c.Assert(err, IsNil)
	lockInfo := s.getLockInfo(c, key)
	msBeforeLockExpired := s.store.GetOracle().UntilExpired(txn.StartTS(), lockInfo.LockTtl, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(msBeforeLockExpired, GreaterEqual, int64(100))

	lr := s.store.NewLockResolver()
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	status, err := lr.GetTxnStatus(bo, txn.StartTS(), key2, 0, txn.StartTS(), true, false, nil)
	c.Assert(err, IsNil)
	c.Assert(status.TTL(), GreaterEqual, lockInfo.LockTtl)

	// Check primary lock TTL is auto increasing while the pessimistic txn is ongoing.
	for i := 0; i < 50; i++ {
		lockInfoNew := s.getLockInfo(c, key)
		if lockInfoNew.LockTtl > lockInfo.LockTtl {
			currentTS, err := s.store.GetOracle().GetTimestamp(bo.GetCtx(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
			c.Assert(err, IsNil)
			// Check that the TTL is update to a reasonable range.
			expire := oracle.ExtractPhysical(txn.StartTS()) + int64(lockInfoNew.LockTtl)
			now := oracle.ExtractPhysical(currentTS)
			c.Assert(expire > now, IsTrue)
			c.Assert(uint64(expire-now) <= atomic.LoadUint64(&tikv.ManagedLockTTL), IsTrue)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Assert(false, IsTrue, Commentf("update pessimistic ttl fail"))
}

func (s *testCommitterSuite) TestPessimisticLockReturnValues(c *C) {
	key := []byte("key")
	key2 := []byte("key2")
	txn := s.begin(c)
	c.Assert(txn.Set(key, key), IsNil)
	c.Assert(txn.Set(key2, key2), IsNil)
	c.Assert(txn.Commit(context.Background()), IsNil)
	txn = s.begin(c)
	txn.SetOption(kv.Pessimistic, true)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now()}
	lockCtx.ReturnValues = true
	lockCtx.Values = map[string]kv.ReturnedValue{}
	c.Assert(txn.LockKeys(context.Background(), lockCtx, key, key2), IsNil)
	c.Assert(lockCtx.Values, HasLen, 2)
	c.Assert(lockCtx.Values[string(key)].Value, BytesEquals, key)
	c.Assert(lockCtx.Values[string(key2)].Value, BytesEquals, key2)
}

// TestElapsedTTL tests that elapsed time is correct even if ts physical time is greater than local time.
func (s *testCommitterSuite) TestElapsedTTL(c *C) {
	key := []byte("key")
	txn := s.begin(c)
	txn.SetStartTS(oracle.ComposeTS(oracle.GetPhysical(time.Now().Add(time.Second*10)), 1))
	txn.SetOption(kv.Pessimistic, true)
	time.Sleep(time.Millisecond * 100)
	lockCtx := &kv.LockCtx{
		ForUpdateTS:   oracle.ComposeTS(oracle.ExtractPhysical(txn.StartTS())+100, 1),
		WaitStartTime: time.Now(),
	}
	err := txn.LockKeys(context.Background(), lockCtx, key)
	c.Assert(err, IsNil)
	lockInfo := s.getLockInfo(c, key)
	c.Assert(lockInfo.LockTtl-atomic.LoadUint64(&tikv.ManagedLockTTL), GreaterEqual, uint64(100))
	c.Assert(lockInfo.LockTtl-atomic.LoadUint64(&tikv.ManagedLockTTL), Less, uint64(150))
}

func (s *testCommitterSuite) TestDeleteYourWriteCauseGhostPrimary(c *C) {
	s.cluster.SplitKeys([]byte("d"), []byte("a"), 4)
	k1 := []byte("a") // insert but deleted key at first pos in txn1
	k2 := []byte("b") // insert key at second pos in txn1
	k3 := []byte("c") // insert key in txn1 and will be conflict read by txn2

	// insert k1, k2, k3 and delete k1
	txn1 := s.begin(c)
	txn1.DelOption(kv.Pessimistic)
	s.store.ClearTxnLatches()
	txn1.Get(context.Background(), k1)
	txn1.GetMemBuffer().SetWithFlags(k1, []byte{0}, kv.SetPresumeKeyNotExists)
	txn1.Set(k2, []byte{1})
	txn1.Set(k3, []byte{2})
	txn1.Delete(k1)
	committer1, err := txn1.NewCommitter(0)
	c.Assert(err, IsNil)
	// setup test knob in txn's committer
	ac, bk := make(chan struct{}), make(chan struct{})
	committer1.SetPrimaryKeyBlocker(ac, bk)
	txn1.SetCommitter(committer1)
	var txn1Done sync.WaitGroup
	txn1Done.Add(1)
	go func() {
		err1 := txn1.Commit(context.Background())
		c.Assert(err1, IsNil)
		txn1Done.Done()
	}()
	// resume after after primary key be committed
	<-ac

	// start txn2 to read k3(prewrite success and primary should be committed)
	txn2 := s.begin(c)
	txn2.DelOption(kv.Pessimistic)
	s.store.ClearTxnLatches()
	v, err := txn2.Get(context.Background(), k3)
	c.Assert(err, IsNil) // should resolve lock and read txn1 k3 result instead of rollback it.
	c.Assert(v[0], Equals, byte(2))
	bk <- struct{}{}
	txn1Done.Wait()
}

func (s *testCommitterSuite) TestDeleteAllYourWrites(c *C) {
	s.cluster.SplitKeys([]byte("d"), []byte("a"), 4)
	k1 := []byte("a")
	k2 := []byte("b")
	k3 := []byte("c")

	// insert k1, k2, k3 and delete k1, k2, k3
	txn1 := s.begin(c)
	txn1.DelOption(kv.Pessimistic)
	s.store.ClearTxnLatches()
	txn1.GetMemBuffer().SetWithFlags(k1, []byte{0}, kv.SetPresumeKeyNotExists)
	txn1.Delete(k1)
	txn1.GetMemBuffer().SetWithFlags(k2, []byte{1}, kv.SetPresumeKeyNotExists)
	txn1.Delete(k2)
	txn1.GetMemBuffer().SetWithFlags(k3, []byte{2}, kv.SetPresumeKeyNotExists)
	txn1.Delete(k3)
	err1 := txn1.Commit(context.Background())
	c.Assert(err1, IsNil)
}

func (s *testCommitterSuite) TestDeleteAllYourWritesWithSFU(c *C) {
	s.cluster.SplitKeys([]byte("d"), []byte("a"), 4)
	k1 := []byte("a")
	k2 := []byte("b")
	k3 := []byte("c")

	// insert k1, k2, k2 and delete k1
	txn1 := s.begin(c)
	txn1.DelOption(kv.Pessimistic)
	s.store.ClearTxnLatches()
	txn1.GetMemBuffer().SetWithFlags(k1, []byte{0}, kv.SetPresumeKeyNotExists)
	txn1.Delete(k1)
	err := txn1.LockKeys(context.Background(), &kv.LockCtx{}, k2, k3) // select * from t where x in (k2, k3) for update
	c.Assert(err, IsNil)

	committer1, err := txn1.NewCommitter(0)
	c.Assert(err, IsNil)
	// setup test knob in txn's committer
	ac, bk := make(chan struct{}), make(chan struct{})
	committer1.SetPrimaryKeyBlocker(ac, bk)
	txn1.SetCommitter(committer1)
	var txn1Done sync.WaitGroup
	txn1Done.Add(1)
	go func() {
		err1 := txn1.Commit(context.Background())
		c.Assert(err1, IsNil)
		txn1Done.Done()
	}()
	// resume after after primary key be committed
	<-ac
	// start txn2 to read k3
	txn2 := s.begin(c)
	txn2.DelOption(kv.Pessimistic)
	s.store.ClearTxnLatches()
	err = txn2.Set(k3, []byte{33})
	c.Assert(err, IsNil)
	var meetLocks []*tikv.Lock
	resolver := tikv.LockResolverProbe{LockResolver: s.store.GetLockResolver()}
	resolver.SetMeetLockCallback(func(locks []*tikv.Lock) {
		meetLocks = append(meetLocks, locks...)
	})
	err = txn2.Commit(context.Background())
	c.Assert(err, IsNil)
	bk <- struct{}{}
	txn1Done.Wait()
	c.Assert(meetLocks[0].Primary[0], Equals, k2[0])
}

// TestAcquireFalseTimeoutLock tests acquiring a key which is a secondary key of another transaction.
// The lock's own TTL is expired but the primary key is still alive due to heartbeats.
func (s *testCommitterSuite) TestAcquireFalseTimeoutLock(c *C) {
	atomic.StoreUint64(&tikv.ManagedLockTTL, 1000)       // 1s
	defer atomic.StoreUint64(&tikv.ManagedLockTTL, 3000) // restore default test value

	// k1 is the primary lock of txn1
	k1 := []byte("k1")
	// k2 is a secondary lock of txn1 and a key txn2 wants to lock
	k2 := []byte("k2")

	txn1 := s.begin(c)
	txn1.SetOption(kv.Pessimistic, true)
	// lock the primary key
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1)
	c.Assert(err, IsNil)
	// lock the secondary key
	lockCtx = &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err = txn1.LockKeys(context.Background(), lockCtx, k2)
	c.Assert(err, IsNil)

	// Heartbeats will increase the TTL of the primary key

	// wait until secondary key exceeds its own TTL
	time.Sleep(time.Duration(atomic.LoadUint64(&tikv.ManagedLockTTL)) * time.Millisecond)
	txn2 := s.begin(c)
	txn2.SetOption(kv.Pessimistic, true)

	// test no wait
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.StartTS(), LockWaitTime: tikv.LockNoWait, WaitStartTime: time.Now()}
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	// cannot acquire lock immediately thus error
	c.Assert(err.Error(), Equals, kv.ErrLockAcquireFailAndNoWaitSet.Error())

	// test for wait limited time (200ms)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.StartTS(), LockWaitTime: 200, WaitStartTime: time.Now()}
	err = txn2.LockKeys(context.Background(), lockCtx, k2)
	// cannot acquire lock in time thus error
	c.Assert(err.Error(), Equals, kv.ErrLockWaitTimeout.Error())
}

func (s *testCommitterSuite) getLockInfo(c *C, key []byte) *kvrpcpb.LockInfo {
	txn := s.begin(c)
	err := txn.Set(key, key)
	c.Assert(err, IsNil)
	committer, err := txn.NewCommitter(1)
	c.Assert(err, IsNil)
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := s.store.GetRegionCache().LocateKey(bo, key)
	c.Assert(err, IsNil)
	req := committer.BuildPrewriteRequest(loc.Region.GetID(), loc.Region.GetConfVer(), loc.Region.GetVer(), committer.GetMutations().Slice(0, 1), 1)
	resp, err := s.store.SendReq(bo, req, loc.Region, 5000)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErrs := (resp.Resp.(*kvrpcpb.PrewriteResponse)).Errors
	c.Assert(keyErrs, HasLen, 1)
	locked := keyErrs[0].Locked
	c.Assert(locked, NotNil)
	return locked
}

func (s *testCommitterSuite) TestPkNotFound(c *C) {
	atomic.StoreUint64(&tikv.ManagedLockTTL, 100)        // 100ms
	defer atomic.StoreUint64(&tikv.ManagedLockTTL, 3000) // restore default value
	ctx := context.Background()
	// k1 is the primary lock of txn1.
	k1 := []byte("k1")
	// k2 is a secondary lock of txn1 and a key txn2 wants to lock.
	k2 := []byte("k2")
	k3 := []byte("k3")

	txn1 := s.begin(c)
	txn1.SetOption(kv.Pessimistic, true)
	// lock the primary key.
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err := txn1.LockKeys(ctx, lockCtx, k1)
	c.Assert(err, IsNil)
	// lock the secondary key.
	lockCtx = &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err = txn1.LockKeys(ctx, lockCtx, k2, k3)
	c.Assert(err, IsNil)
	// Stop txn ttl manager and remove primary key, like tidb server crashes and the priamry key lock does not exists actually,
	// while the secondary lock operation succeeded.
	txn1.GetCommitter().CloseTTLManager()

	var status tikv.TxnStatus
	bo := tikv.NewBackofferWithVars(ctx, 5000, nil)
	lockKey2 := &tikv.Lock{
		Key:             k2,
		Primary:         k1,
		TxnID:           txn1.StartTS(),
		TTL:             0, // let the primary lock k1 expire doing check.
		TxnSize:         txnCommitBatchSize,
		LockType:        kvrpcpb.Op_PessimisticLock,
		LockForUpdateTS: txn1.StartTS(),
	}
	resolver := tikv.LockResolverProbe{LockResolver: s.store.GetLockResolver()}
	status, err = resolver.GetTxnStatusFromLock(bo, lockKey2, oracle.GoTimeToTS(time.Now().Add(200*time.Millisecond)), false)
	c.Assert(err, IsNil)
	c.Assert(status.Action(), Equals, kvrpcpb.Action_TTLExpirePessimisticRollback)

	// Txn2 tries to lock the secondary key k2, there should be no dead loop.
	// Since the resolving key k2 is a pessimistic lock, no rollback record should be written, and later lock
	// and the other secondary key k3 should succeed if there is no fail point enabled.
	status, err = resolver.GetTxnStatusFromLock(bo, lockKey2, oracle.GoTimeToTS(time.Now().Add(200*time.Millisecond)), false)
	c.Assert(err, IsNil)
	c.Assert(status.Action(), Equals, kvrpcpb.Action_LockNotExistDoNothing)
	txn2 := s.begin(c)
	txn2.SetOption(kv.Pessimistic, true)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.StartTS(), WaitStartTime: time.Now()}
	err = txn2.LockKeys(ctx, lockCtx, k2)
	c.Assert(err, IsNil)

	// Pessimistic rollback using smaller forUpdateTS does not take effect.
	lockKey3 := &tikv.Lock{
		Key:             k3,
		Primary:         k1,
		TxnID:           txn1.StartTS(),
		TTL:             tikv.ManagedLockTTL,
		TxnSize:         txnCommitBatchSize,
		LockType:        kvrpcpb.Op_PessimisticLock,
		LockForUpdateTS: txn1.StartTS() - 1,
	}
	err = resolver.ResolvePessimisticLock(ctx, lockKey3)
	c.Assert(err, IsNil)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err = txn1.LockKeys(ctx, lockCtx, k3)
	c.Assert(err, IsNil)

	// After disable fail point, the rollbackIfNotExist flag will be set, and the resolve should succeed. In this
	// case, the returned action of TxnStatus should be LockNotExistDoNothing, and lock on k3 could be resolved.
	txn3 := s.begin(c)
	txn3.SetOption(kv.Pessimistic, true)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn3.StartTS(), WaitStartTime: time.Now(), LockWaitTime: tikv.LockNoWait}
	err = txn3.LockKeys(ctx, lockCtx, k3)
	c.Assert(err, IsNil)
	status, err = resolver.GetTxnStatusFromLock(bo, lockKey3, oracle.GoTimeToTS(time.Now().Add(200*time.Millisecond)), false)
	c.Assert(err, IsNil)
	c.Assert(status.Action(), Equals, kvrpcpb.Action_LockNotExistDoNothing)
}

func (s *testCommitterSuite) TestPessimisticLockPrimary(c *C) {
	// a is the primary lock of txn1
	k1 := []byte("a")
	// b is a secondary lock of txn1 and a key txn2 wants to lock, b is on another region
	k2 := []byte("b")

	txn1 := s.begin(c)
	txn1.SetOption(kv.Pessimistic, true)
	// txn1 lock k1
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1)
	c.Assert(err, IsNil)

	// txn2 wants to lock k1, k2, k1(pk) is blocked by txn1, pessimisticLockKeys has been changed to
	// lock primary key first and then secondary keys concurrently, k2 should not be locked by txn2
	doneCh := make(chan error)
	go func() {
		txn2 := s.begin(c)
		txn2.SetOption(kv.Pessimistic, true)
		lockCtx2 := &kv.LockCtx{ForUpdateTS: txn2.StartTS(), WaitStartTime: time.Now(), LockWaitTime: 200}
		waitErr := txn2.LockKeys(context.Background(), lockCtx2, k1, k2)
		doneCh <- waitErr
	}()
	time.Sleep(50 * time.Millisecond)

	// txn3 should locks k2 successfully using no wait
	txn3 := s.begin(c)
	txn3.SetOption(kv.Pessimistic, true)
	lockCtx3 := &kv.LockCtx{ForUpdateTS: txn3.StartTS(), WaitStartTime: time.Now(), LockWaitTime: tikv.LockNoWait}
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/txnNotFoundRetTTL", "return"), IsNil)
	err = txn3.LockKeys(context.Background(), lockCtx3, k2)
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/txnNotFoundRetTTL"), IsNil)
	c.Assert(err, IsNil)
	waitErr := <-doneCh
	c.Assert(kv.ErrLockWaitTimeout.Equal(waitErr), IsTrue)
}

func (s *testCommitterSuite) TestResolvePessimisticLock(c *C) {
	untouchedIndexKey := []byte("t00000001_i000000001")
	untouchedIndexValue := []byte{0, 0, 0, 0, 0, 0, 0, 1, 49}
	noValueIndexKey := []byte("t00000001_i000000002")
	txn := s.begin(c)
	txn.SetOption(kv.KVFilter, drivertxn.TiDBKVFilter{})
	err := txn.Set(untouchedIndexKey, untouchedIndexValue)
	c.Assert(err, IsNil)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn.StartTS(), WaitStartTime: time.Now(), LockWaitTime: tikv.LockNoWait}
	err = txn.LockKeys(context.Background(), lockCtx, untouchedIndexKey, noValueIndexKey)
	c.Assert(err, IsNil)
	commit, err := txn.NewCommitter(1)
	c.Assert(err, IsNil)
	mutation := commit.MutationsOfKeys([][]byte{untouchedIndexKey, noValueIndexKey})
	c.Assert(mutation.Len(), Equals, 2)
	c.Assert(mutation.GetOp(0), Equals, pb.Op_Lock)
	c.Assert(mutation.GetKey(0), BytesEquals, untouchedIndexKey)
	c.Assert(mutation.GetValue(0), BytesEquals, untouchedIndexValue)
	c.Assert(mutation.GetOp(1), Equals, pb.Op_Lock)
	c.Assert(mutation.GetKey(1), BytesEquals, noValueIndexKey)
	c.Assert(mutation.GetValue(1), BytesEquals, []byte{})
}

func (s *testCommitterSuite) TestCommitDeadLock(c *C) {
	// Split into two region and let k1 k2 in different regions.
	s.cluster.SplitKeys([]byte("z"), []byte("a"), 2)
	k1 := []byte("a_deadlock_k1")
	k2 := []byte("y_deadlock_k2")

	region1, _ := s.cluster.GetRegionByKey(k1)
	region2, _ := s.cluster.GetRegionByKey(k2)
	c.Assert(region1.Id != region2.Id, IsTrue)

	txn1 := s.begin(c)
	txn1.Set(k1, []byte("t1"))
	txn1.Set(k2, []byte("t1"))
	commit1, err := txn1.NewCommitter(1)
	c.Assert(err, IsNil)
	commit1.SetPrimaryKey(k1)
	commit1.SetTxnSize(1000 * 1024 * 1024)

	txn2 := s.begin(c)
	txn2.Set(k1, []byte("t2"))
	txn2.Set(k2, []byte("t2"))
	commit2, err := txn2.NewCommitter(2)
	c.Assert(err, IsNil)
	commit2.SetPrimaryKey(k2)
	commit2.SetTxnSize(1000 * 1024 * 1024)

	s.cluster.ScheduleDelay(txn2.StartTS(), region1.Id, 5*time.Millisecond)
	s.cluster.ScheduleDelay(txn1.StartTS(), region2.Id, 5*time.Millisecond)

	// Txn1 prewrites k1, k2 and txn2 prewrites k2, k1, the large txn
	// protocol run ttlManager and update their TTL, cause dead lock.
	ch := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		ch <- commit2.Execute(context.Background())
		wg.Done()
	}()
	ch <- commit1.Execute(context.Background())
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
	k1, k2 := []byte("a"), []byte("b")
	ctx := context.Background()

	txn1 := s.begin(c)
	txn1.SetOption(kv.Pessimistic, true)
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, k1, k2)
	c.Assert(err, IsNil)

	txn1.Set(k2, []byte("v2"))
	committer := txn1.GetCommitter()
	err = committer.InitKeysAndMutations()
	c.Assert(err, IsNil)
	// Strip the prewrite of the primary key.
	committer.SetMutations(committer.GetMutations().Slice(1, 2))
	c.Assert(err, IsNil)
	err = committer.PrewriteAllMutations(ctx)
	c.Assert(err, IsNil)
	// The primary lock is a pessimistic lock and the secondary lock is a optimistic lock.
	lock1 := s.getLockInfo(c, k1)
	c.Assert(lock1.LockType, Equals, kvrpcpb.Op_PessimisticLock)
	c.Assert(lock1.PrimaryLock, BytesEquals, k1)
	lock2 := s.getLockInfo(c, k2)
	c.Assert(lock2.LockType, Equals, kvrpcpb.Op_Put)
	c.Assert(lock2.PrimaryLock, BytesEquals, k1)

	txn2 := s.begin(c)
	start := time.Now()
	_, err = txn2.Get(ctx, k2)
	elapsed := time.Since(start)
	// The optimistic lock shouldn't block reads.
	c.Assert(elapsed, Less, 500*time.Millisecond)
	c.Assert(tidbkv.IsErrNotFound(err), IsTrue)

	txn1.Rollback()
	txn2.Rollback()
}

// TestResolveMixed tests mixed resolve with left behind optimistic locks and pessimistic locks,
// using clean whole region resolve path
func (s *testCommitterSuite) TestResolveMixed(c *C) {
	atomic.StoreUint64(&tikv.ManagedLockTTL, 100)        // 100ms
	defer atomic.StoreUint64(&tikv.ManagedLockTTL, 3000) // restore default value
	ctx := context.Background()

	// pk is the primary lock of txn1
	pk := []byte("pk")
	secondaryLockkeys := make([][]byte, 0, bigTxnThreshold)
	for i := 0; i < bigTxnThreshold; i++ {
		optimisticLock := []byte(fmt.Sprintf("optimisticLockKey%d", i))
		secondaryLockkeys = append(secondaryLockkeys, optimisticLock)
	}
	pessimisticLockKey := []byte("pessimisticLockKey")

	// make the optimistic and pessimistic lock left with primary lock not found
	txn1 := s.begin(c)
	txn1.SetOption(kv.Pessimistic, true)
	// lock the primary key
	lockCtx := &kv.LockCtx{ForUpdateTS: txn1.StartTS(), WaitStartTime: time.Now()}
	err := txn1.LockKeys(context.Background(), lockCtx, pk)
	c.Assert(err, IsNil)
	// lock the optimistic keys
	for i := 0; i < bigTxnThreshold; i++ {
		txn1.Set(secondaryLockkeys[i], []byte(fmt.Sprintf("v%d", i)))
	}
	committer := txn1.GetCommitter()
	err = committer.InitKeysAndMutations()
	c.Assert(err, IsNil)
	err = committer.PrewriteAllMutations(ctx)
	c.Assert(err, IsNil)
	// lock the pessimistic keys
	err = txn1.LockKeys(context.Background(), lockCtx, pessimisticLockKey)
	c.Assert(err, IsNil)
	lock1 := s.getLockInfo(c, pessimisticLockKey)
	c.Assert(lock1.LockType, Equals, kvrpcpb.Op_PessimisticLock)
	c.Assert(lock1.PrimaryLock, BytesEquals, pk)
	optimisticLockKey := secondaryLockkeys[0]
	lock2 := s.getLockInfo(c, optimisticLockKey)
	c.Assert(lock2.LockType, Equals, kvrpcpb.Op_Put)
	c.Assert(lock2.PrimaryLock, BytesEquals, pk)

	// stop txn ttl manager and remove primary key, make the other keys left behind
	committer.CloseTTLManager()
	muts := tikv.NewPlainMutations(1)
	muts.Push(kvrpcpb.Op_Lock, pk, nil, true)
	err = committer.PessimisticRollbackMutations(context.Background(), &muts)
	c.Assert(err, IsNil)

	// try to resolve the left optimistic locks, use clean whole region
	time.Sleep(time.Duration(atomic.LoadUint64(&tikv.ManagedLockTTL)) * time.Millisecond)
	optimisticLockInfo := s.getLockInfo(c, optimisticLockKey)
	lock := tikv.NewLock(optimisticLockInfo)
	resolver := tikv.LockResolverProbe{LockResolver: s.store.GetLockResolver()}
	err = resolver.ResolveLock(ctx, lock)
	c.Assert(err, IsNil)

	// txn2 tries to lock the pessimisticLockKey, the lock should has been resolved in clean whole region resolve
	txn2 := s.begin(c)
	txn2.SetOption(kv.Pessimistic, true)
	lockCtx = &kv.LockCtx{ForUpdateTS: txn2.StartTS(), WaitStartTime: time.Now(), LockWaitTime: tikv.LockNoWait}
	err = txn2.LockKeys(context.Background(), lockCtx, pessimisticLockKey)
	c.Assert(err, IsNil)

	err = txn1.Rollback()
	c.Assert(err, IsNil)
	err = txn2.Rollback()
	c.Assert(err, IsNil)
}

// TestSecondaryKeys tests that when async commit is enabled, each prewrite message includes an
// accurate list of secondary keys.
func (s *testCommitterSuite) TestPrewriteSecondaryKeys(c *C) {
	// Prepare two regions first: (, 100) and [100, )
	region, _ := s.cluster.GetRegionByKey([]byte{50})
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(region.Id, newRegionID, []byte{100}, []uint64{newPeerID}, newPeerID)

	txn := s.beginAsyncCommit(c)
	var val [1024]byte
	for i := byte(50); i < 120; i++ {
		err := txn.Set([]byte{i}, val[:])
		c.Assert(err, IsNil)
	}
	// Some duplicates.
	for i := byte(50); i < 120; i += 10 {
		err := txn.Set([]byte{i}, val[512:700])
		c.Assert(err, IsNil)
	}

	committer, err := txn.NewCommitter(1)
	c.Assert(err, IsNil)

	mock := mockClient{inner: s.store.GetTiKVClient()}
	s.store.SetTiKVClient(&mock)
	ctx := context.Background()
	// TODO remove this when minCommitTS is returned from mockStore prewrite response.
	committer.SetMinCommitTS(committer.GetStartTS() + 10)
	committer.SetNoFallBack()
	err = committer.Execute(ctx)
	c.Assert(err, IsNil)
	c.Assert(mock.seenPrimaryReq > 0, IsTrue)
	c.Assert(mock.seenSecondaryReq > 0, IsTrue)
}

func (s *testCommitterSuite) TestAsyncCommit(c *C) {
	ctx := context.Background()
	pk := []byte("tpk")
	pkVal := []byte("pkVal")
	k1 := []byte("tk1")
	k1Val := []byte("k1Val")
	txn1 := s.beginAsyncCommit(c)
	err := txn1.Set(pk, pkVal)
	c.Assert(err, IsNil)
	err = txn1.Set(k1, k1Val)
	c.Assert(err, IsNil)

	committer, err := txn1.NewCommitter(0)
	c.Assert(err, IsNil)
	committer.SetSessionID(1)
	committer.SetMinCommitTS(txn1.StartTS() + 10)
	err = committer.Execute(ctx)
	c.Assert(err, IsNil)

	s.checkValues(c, map[string]string{
		string(pk): string(pkVal),
		string(k1): string(k1Val),
	})
}

func updateGlobalConfig(f func(conf *config.Config)) {
	g := config.GetGlobalConfig()
	newConf := *g
	f(&newConf)
	config.StoreGlobalConfig(&newConf)
}

// restoreFunc gets a function that restore the config to the current value.
func restoreGlobalConfFunc() (restore func()) {
	g := config.GetGlobalConfig()
	return func() {
		config.StoreGlobalConfig(g)
	}
}

func (s *testCommitterSuite) TestAsyncCommitCheck(c *C) {
	defer restoreGlobalConfFunc()()
	updateGlobalConfig(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.KeysLimit = 16
		conf.TiKVClient.AsyncCommit.TotalKeySizeLimit = 64
	})

	txn := s.beginAsyncCommit(c)
	buf := []byte{0, 0, 0, 0}
	// Set 16 keys, each key is 4 bytes long. So the total size of keys is 64 bytes.
	for i := 0; i < 16; i++ {
		buf[0] = byte(i)
		err := txn.Set(buf, []byte("v"))
		c.Assert(err, IsNil)
	}

	committer, err := txn.NewCommitter(1)
	c.Assert(err, IsNil)
	c.Assert(committer.CheckAsyncCommit(), IsTrue)

	updateGlobalConfig(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.KeysLimit = 15
	})
	c.Assert(committer.CheckAsyncCommit(), IsFalse)

	updateGlobalConfig(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.KeysLimit = 20
		conf.TiKVClient.AsyncCommit.TotalKeySizeLimit = 63
	})
	c.Assert(committer.CheckAsyncCommit(), IsFalse)
}

type mockClient struct {
	inner            tikv.Client
	seenPrimaryReq   uint32
	seenSecondaryReq uint32
}

func (m *mockClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	// If we find a prewrite request, check if it satisfies our constraints.
	if pr, ok := req.Req.(*kvrpcpb.PrewriteRequest); ok {
		if pr.UseAsyncCommit {
			if isPrimary(pr) {
				// The primary key should not be included, nor should there be any duplicates. All keys should be present.
				if !includesPrimary(pr) && allKeysNoDups(pr) {
					atomic.StoreUint32(&m.seenPrimaryReq, 1)
				}
			} else {
				// Secondaries should only be sent with the primary key
				if len(pr.Secondaries) == 0 {
					atomic.StoreUint32(&m.seenSecondaryReq, 1)
				}
			}
		}
	}
	return m.inner.SendRequest(ctx, addr, req, timeout)
}

func (m *mockClient) Close() error {
	return m.inner.Close()
}

func isPrimary(req *kvrpcpb.PrewriteRequest) bool {
	for _, m := range req.Mutations {
		if bytes.Equal(req.PrimaryLock, m.Key) {
			return true
		}
	}

	return false
}

func includesPrimary(req *kvrpcpb.PrewriteRequest) bool {
	for _, k := range req.Secondaries {
		if bytes.Equal(req.PrimaryLock, k) {
			return true
		}
	}

	return false
}

func allKeysNoDups(req *kvrpcpb.PrewriteRequest) bool {
	check := make(map[string]bool)

	// Create the check map and check for duplicates.
	for _, k := range req.Secondaries {
		s := string(k)
		if check[s] {
			return false
		}
		check[s] = true
	}

	// Check every key is present.
	for i := byte(50); i < 120; i++ {
		k := []byte{i}
		if !bytes.Equal(req.PrimaryLock, k) && !check[string(k)] {
			return false
		}
	}
	return true
}
