// Copyright 2020 PingCAP, Inc.
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
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/store/tikv/util"
)

// testAsyncCommitCommon is used to put common parts that will be both used by
// testAsyncCommitSuite and testAsyncCommitFailSuite.
type testAsyncCommitCommon struct {
	cluster cluster.Cluster
	store   *tikvStore
}

func (s *testAsyncCommitCommon) setUpTest(c *C) {
	if *WithTiKV {
		s.store = NewTestStore(c).(*tikvStore)
		return
	}

	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)

	s.store = store.(*tikvStore)
}

func (s *testAsyncCommitCommon) putAlphabets(c *C, enableAsyncCommit bool) {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.putKV(c, []byte{ch}, []byte{ch}, enableAsyncCommit)
	}
}

func (s *testAsyncCommitCommon) putKV(c *C, key, value []byte, enableAsyncCommit bool) (uint64, uint64) {
	txn := s.beginAsyncCommit(c)
	err := txn.Set(key, value)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	return txn.StartTS(), txn.commitTS
}

func (s *testAsyncCommitCommon) mustGetFromTxn(c *C, txn kv.Transaction, key, expectedValue []byte) {
	v, err := txn.Get(context.Background(), key)
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetLock(c *C, key []byte) *Lock {
	ver, err := s.store.CurrentVersion(oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver.Ver,
	})
	bo := NewBackofferWithVars(context.Background(), 5000, nil)
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

func (s *testAsyncCommitCommon) mustPointGet(c *C, key, expectedValue []byte) {
	snap := s.store.GetSnapshot(kv.MaxVersion)
	value, err := snap.Get(context.Background(), key)
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetFromSnapshot(c *C, version uint64, key, expectedValue []byte) {
	snap := s.store.GetSnapshot(kv.Version{Ver: version})
	value, err := snap.Get(context.Background(), key)
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetNoneFromSnapshot(c *C, version uint64, key []byte) {
	snap := s.store.GetSnapshot(kv.Version{Ver: version})
	_, err := snap.Get(context.Background(), key)
	c.Assert(errors.Cause(err), Equals, kv.ErrNotExist)
}

func (s *testAsyncCommitCommon) beginAsyncCommitWithExternalConsistency(c *C) *tikvTxn {
	txn := s.beginAsyncCommit(c)
	txn.SetOption(kv.GuaranteeExternalConsistency, true)
	return txn
}

func (s *testAsyncCommitCommon) beginAsyncCommit(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.SetOption(kv.EnableAsyncCommit, true)
	return txn.(*tikvTxn)
}

func (s *testAsyncCommitCommon) begin(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

type testAsyncCommitSuite struct {
	OneByOneSuite
	testAsyncCommitCommon
	bo *Backoffer
}

var _ = SerialSuites(&testAsyncCommitSuite{})

func (s *testAsyncCommitSuite) SetUpTest(c *C) {
	s.testAsyncCommitCommon.setUpTest(c)
	s.bo = NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testAsyncCommitSuite) lockKeysWithAsyncCommit(c *C, keys, values [][]byte, primaryKey, primaryValue []byte, commitPrimary bool) (uint64, uint64) {
	txn, err := newTiKVTxn(s.store, oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	txn.SetOption(kv.EnableAsyncCommit, true)
	for i, k := range keys {
		if len(values[i]) > 0 {
			err = txn.Set(k, values[i])
		} else {
			err = txn.Delete(k)
		}
		c.Assert(err, IsNil)
	}
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
		tpc.commitTS, err = s.store.oracle.GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		c.Assert(err, IsNil)
		err = tpc.commitMutations(NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), nil), tpc.mutationsOfKeys([][]byte{primaryKey}))
		c.Assert(err, IsNil)
	}
	return txn.startTS, tpc.commitTS
}

func (s *testAsyncCommitSuite) TestCheckSecondaries(c *C) {
	// This test doesn't support tikv mode.
	if *WithTiKV {
		return
	}

	s.putAlphabets(c, true)

	loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.Split(loc.Region.id, newRegionID, []byte("e"), []uint64{peerID}, peerID)
	s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)

	// No locks to check, only primary key is locked, should be successful.
	s.lockKeysWithAsyncCommit(c, [][]byte{}, [][]byte{}, []byte("z"), []byte("z"), false)
	lock := s.mustGetLock(c, []byte("z"))
	lock.UseAsyncCommit = true
	ts, err := s.store.oracle.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	status := TxnStatus{primaryLock: &kvrpcpb.LockInfo{Secondaries: [][]byte{}, UseAsyncCommit: true, MinCommitTs: ts}}

	err = s.store.lockResolver.resolveLockAsync(s.bo, lock, status)
	c.Assert(err, IsNil)
	currentTS, err := s.store.oracle.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	status, err = s.store.lockResolver.getTxnStatus(s.bo, lock.TxnID, []byte("z"), currentTS, currentTS, true, false, nil)
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Equals, ts)

	// One key is committed (i), one key is locked (a). Should get committed.
	ts, err = s.store.oracle.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	commitTs := ts + 10

	gotCheckA := int64(0)
	gotCheckB := int64(0)
	gotResolve := int64(0)
	gotOther := int64(0)
	mock := mockResolveClient{
		inner: s.store.client,
		onCheckSecondaries: func(req *kvrpcpb.CheckSecondaryLocksRequest) (*tikvrpc.Response, error) {
			if req.StartVersion != ts {
				return nil, errors.Errorf("Bad start version: %d, expected: %d", req.StartVersion, ts)
			}
			var resp kvrpcpb.CheckSecondaryLocksResponse
			for _, k := range req.Keys {
				if bytes.Equal(k, []byte("a")) {
					atomic.StoreInt64(&gotCheckA, 1)

					resp = kvrpcpb.CheckSecondaryLocksResponse{
						Locks:    []*kvrpcpb.LockInfo{{Key: []byte("a"), PrimaryLock: []byte("z"), LockVersion: ts, UseAsyncCommit: true}},
						CommitTs: commitTs,
					}
				} else if bytes.Equal(k, []byte("i")) {
					atomic.StoreInt64(&gotCheckB, 1)

					resp = kvrpcpb.CheckSecondaryLocksResponse{
						Locks:    []*kvrpcpb.LockInfo{},
						CommitTs: commitTs,
					}
				} else {
					fmt.Printf("Got other key: %s\n", k)
					atomic.StoreInt64(&gotOther, 1)
				}
			}
			return &tikvrpc.Response{Resp: &resp}, nil
		},
		onResolveLock: func(req *kvrpcpb.ResolveLockRequest) (*tikvrpc.Response, error) {
			if req.StartVersion != ts {
				return nil, errors.Errorf("Bad start version: %d, expected: %d", req.StartVersion, ts)
			}
			if req.CommitVersion != commitTs {
				return nil, errors.Errorf("Bad commit version: %d, expected: %d", req.CommitVersion, commitTs)
			}
			for _, k := range req.Keys {
				if bytes.Equal(k, []byte("a")) || bytes.Equal(k, []byte("z")) {
					atomic.StoreInt64(&gotResolve, 1)
				} else {
					atomic.StoreInt64(&gotOther, 1)
				}
			}
			resp := kvrpcpb.ResolveLockResponse{}
			return &tikvrpc.Response{Resp: &resp}, nil
		},
	}
	s.store.client = &mock

	status = TxnStatus{primaryLock: &kvrpcpb.LockInfo{Secondaries: [][]byte{[]byte("a"), []byte("i")}, UseAsyncCommit: true}}
	lock = &Lock{
		Key:            []byte("a"),
		Primary:        []byte("z"),
		TxnID:          ts,
		LockType:       kvrpcpb.Op_Put,
		UseAsyncCommit: true,
		MinCommitTS:    ts + 5,
	}

	_ = s.beginAsyncCommit(c)

	err = s.store.lockResolver.resolveLockAsync(s.bo, lock, status)
	c.Assert(err, IsNil)
	c.Assert(gotCheckA, Equals, int64(1))
	c.Assert(gotCheckB, Equals, int64(1))
	c.Assert(gotOther, Equals, int64(0))
	c.Assert(gotResolve, Equals, int64(1))

	// One key has been rolled back (b), one is locked (a). Should be rolled back.
	ts, err = s.store.oracle.GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	commitTs = ts + 10

	gotCheckA = int64(0)
	gotCheckB = int64(0)
	gotResolve = int64(0)
	gotOther = int64(0)
	mock.onResolveLock = func(req *kvrpcpb.ResolveLockRequest) (*tikvrpc.Response, error) {
		if req.StartVersion != ts {
			return nil, errors.Errorf("Bad start version: %d, expected: %d", req.StartVersion, ts)
		}
		if req.CommitVersion != commitTs {
			return nil, errors.Errorf("Bad commit version: %d, expected: 0", req.CommitVersion)
		}
		for _, k := range req.Keys {
			if bytes.Equal(k, []byte("a")) || bytes.Equal(k, []byte("z")) {
				atomic.StoreInt64(&gotResolve, 1)
			} else {
				atomic.StoreInt64(&gotOther, 1)
			}
		}
		resp := kvrpcpb.ResolveLockResponse{}
		return &tikvrpc.Response{Resp: &resp}, nil
	}

	lock.TxnID = ts
	lock.MinCommitTS = ts + 5

	err = s.store.lockResolver.resolveLockAsync(s.bo, lock, status)
	c.Assert(err, IsNil)
	c.Assert(gotCheckA, Equals, int64(1))
	c.Assert(gotCheckB, Equals, int64(1))
	c.Assert(gotResolve, Equals, int64(1))
	c.Assert(gotOther, Equals, int64(0))
}

func (s *testAsyncCommitSuite) TestRepeatableRead(c *C) {
	var sessionID uint64 = 0
	test := func(isPessimistic bool) {
		s.putKV(c, []byte("k1"), []byte("v1"), true)

		sessionID++
		ctx := context.WithValue(context.Background(), util.SessionIDCtxKey, sessionID)
		txn1 := s.beginAsyncCommit(c)
		txn1.SetOption(kv.Pessimistic, isPessimistic)
		s.mustGetFromTxn(c, txn1, []byte("k1"), []byte("v1"))
		txn1.Set([]byte("k1"), []byte("v2"))

		for i := 0; i < 20; i++ {
			_, err := s.store.GetOracle().GetTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
			c.Assert(err, IsNil)
		}

		txn2 := s.beginAsyncCommit(c)
		s.mustGetFromTxn(c, txn2, []byte("k1"), []byte("v1"))

		err := txn1.Commit(ctx)
		c.Assert(err, IsNil)
		// Check txn1 is committed in async commit.
		c.Assert(txn1.committer.isAsyncCommit(), IsTrue)
		s.mustGetFromTxn(c, txn2, []byte("k1"), []byte("v1"))
		err = txn2.Rollback()
		c.Assert(err, IsNil)

		txn3 := s.beginAsyncCommit(c)
		s.mustGetFromTxn(c, txn3, []byte("k1"), []byte("v2"))
		err = txn3.Rollback()
		c.Assert(err, IsNil)
	}

	test(false)
	test(true)
}

// It's just a simple validation of external consistency.
// Extra tests are needed to test this feature with the control of the TiKV cluster.
func (s *testAsyncCommitSuite) TestAsyncCommitExternalConsistency(c *C) {
	t1 := s.beginAsyncCommitWithExternalConsistency(c)
	t2 := s.beginAsyncCommitWithExternalConsistency(c)
	err := t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = t2.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	ctx := context.WithValue(context.Background(), util.SessionIDCtxKey, uint64(1))
	// t2 commits earlier than t1
	err = t2.Commit(ctx)
	c.Assert(err, IsNil)
	err = t1.Commit(ctx)
	c.Assert(err, IsNil)
	commitTS1 := t1.committer.commitTS
	commitTS2 := t2.committer.commitTS
	c.Assert(commitTS2, Less, commitTS1)
}

// TestAsyncCommitWithMultiDC tests that async commit can only be enabled in global transactions
func (s *testAsyncCommitSuite) TestAsyncCommitWithMultiDC(c *C) {
	// It requires setting placement rules to run with TiKV
	if *WithTiKV {
		return
	}

	localTxn := s.beginAsyncCommit(c)
	err := localTxn.Set([]byte("a"), []byte("a1"))
	localTxn.SetOption(kv.TxnScope, "bj")
	c.Assert(err, IsNil)
	ctx := context.WithValue(context.Background(), util.SessionIDCtxKey, uint64(1))
	err = localTxn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(localTxn.committer.isAsyncCommit(), IsFalse)

	globalTxn := s.beginAsyncCommit(c)
	err = globalTxn.Set([]byte("b"), []byte("b1"))
	globalTxn.SetOption(kv.TxnScope, oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	err = globalTxn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(globalTxn.committer.isAsyncCommit(), IsTrue)
}

type mockResolveClient struct {
	inner              Client
	onResolveLock      func(*kvrpcpb.ResolveLockRequest) (*tikvrpc.Response, error)
	onCheckSecondaries func(*kvrpcpb.CheckSecondaryLocksRequest) (*tikvrpc.Response, error)
}

func (m *mockResolveClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	// Intercept check secondary locks and resolve lock messages if the callback is non-nil.
	// If the callback returns (nil, nil), forward to the inner client.
	if cr, ok := req.Req.(*kvrpcpb.CheckSecondaryLocksRequest); ok && m.onCheckSecondaries != nil {
		result, err := m.onCheckSecondaries(cr)
		if result != nil || err != nil {
			return result, err
		}
	} else if rr, ok := req.Req.(*kvrpcpb.ResolveLockRequest); ok && m.onResolveLock != nil {
		result, err := m.onResolveLock(rr)
		if result != nil || err != nil {
			return result, err
		}
	}
	return m.inner.SendRequest(ctx, addr, req, timeout)
}

func (m *mockResolveClient) Close() error {
	return m.inner.Close()
}
