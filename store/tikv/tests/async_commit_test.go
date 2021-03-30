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

package tikv_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/mockstore/cluster"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/store/tikv/util"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

// testAsyncCommitCommon is used to put common parts that will be both used by
// testAsyncCommitSuite and testAsyncCommitFailSuite.
type testAsyncCommitCommon struct {
	cluster cluster.Cluster
	store   *tikv.KVStore
}

func (s *testAsyncCommitCommon) setUpTest(c *C) {
	if *WithTiKV {
		s.store = NewTestStore(c)
		return
	}

	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)

	s.store = store
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
	return txn.StartTS(), txn.GetCommitTS()
}

func (s *testAsyncCommitCommon) mustGetFromTxn(c *C, txn tikv.TxnProbe, key, expectedValue []byte) {
	v, err := txn.Get(context.Background(), key)
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetLock(c *C, key []byte) *tikv.Lock {
	ver, err := s.store.CurrentTimestamp(oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver,
	})
	bo := tikv.NewBackofferWithVars(context.Background(), 5000, nil)
	loc, err := s.store.GetRegionCache().LocateKey(bo, key)
	c.Assert(err, IsNil)
	resp, err := s.store.SendReq(bo, req, loc.Region, time.Second*10)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErr := resp.Resp.(*kvrpcpb.GetResponse).GetError()
	c.Assert(keyErr, NotNil)
	var lockutil tikv.LockProbe
	lock, err := lockutil.ExtractLockFromKeyErr(keyErr)
	c.Assert(err, IsNil)
	return lock
}

func (s *testAsyncCommitCommon) mustPointGet(c *C, key, expectedValue []byte) {
	snap := s.store.GetSnapshot(math.MaxUint64)
	value, err := snap.Get(context.Background(), key)
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetFromSnapshot(c *C, version uint64, key, expectedValue []byte) {
	snap := s.store.GetSnapshot(version)
	value, err := snap.Get(context.Background(), key)
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetNoneFromSnapshot(c *C, version uint64, key []byte) {
	snap := s.store.GetSnapshot(version)
	_, err := snap.Get(context.Background(), key)
	c.Assert(errors.Cause(err), Equals, tidbkv.ErrNotExist)
}

func (s *testAsyncCommitCommon) beginAsyncCommitWithLinearizability(c *C) tikv.TxnProbe {
	txn := s.beginAsyncCommit(c)
	txn.SetOption(kv.GuaranteeLinearizability, true)
	return txn
}

func (s *testAsyncCommitCommon) beginAsyncCommit(c *C) tikv.TxnProbe {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	txn.SetOption(kv.EnableAsyncCommit, true)
	return tikv.TxnProbe{KVTxn: txn}
}

func (s *testAsyncCommitCommon) begin(c *C) tikv.TxnProbe {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return tikv.TxnProbe{KVTxn: txn}
}

type testAsyncCommitSuite struct {
	OneByOneSuite
	testAsyncCommitCommon
	bo *tikv.Backoffer
}

var _ = SerialSuites(&testAsyncCommitSuite{})

func (s *testAsyncCommitSuite) SetUpTest(c *C) {
	s.testAsyncCommitCommon.setUpTest(c)
	s.bo = tikv.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testAsyncCommitSuite) lockKeysWithAsyncCommit(c *C, keys, values [][]byte, primaryKey, primaryValue []byte, commitPrimary bool) (uint64, uint64) {
	txn, err := s.store.Begin()
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
	txnProbe := tikv.TxnProbe{KVTxn: txn}
	tpc, err := txnProbe.NewCommitter(0)
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

func (s *testAsyncCommitSuite) TestCheckSecondaries(c *C) {
	// This test doesn't support tikv mode.
	if *WithTiKV {
		return
	}

	s.putAlphabets(c, true)

	loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.Split(loc.Region.GetID(), newRegionID, []byte("e"), []uint64{peerID}, peerID)
	s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)

	// No locks to check, only primary key is locked, should be successful.
	s.lockKeysWithAsyncCommit(c, [][]byte{}, [][]byte{}, []byte("z"), []byte("z"), false)
	lock := s.mustGetLock(c, []byte("z"))
	lock.UseAsyncCommit = true
	ts, err := s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	var lockutil tikv.LockProbe
	status := lockutil.NewLockStatus(nil, true, ts)

	resolver := tikv.LockResolverProbe{LockResolver: s.store.GetLockResolver()}
	err = resolver.ResolveLockAsync(s.bo, lock, status)
	c.Assert(err, IsNil)
	currentTS, err := s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	status, err = resolver.GetTxnStatus(s.bo, lock.TxnID, []byte("z"), currentTS, currentTS, true, false, nil)
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Equals, ts)

	// One key is committed (i), one key is locked (a). Should get committed.
	ts, err = s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	c.Assert(err, IsNil)
	commitTs := ts + 10

	gotCheckA := int64(0)
	gotCheckB := int64(0)
	gotResolve := int64(0)
	gotOther := int64(0)
	mock := mockResolveClient{
		inner: s.store.GetTiKVClient(),
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
	s.store.SetTiKVClient(&mock)

	status = lockutil.NewLockStatus([][]byte{[]byte("a"), []byte("i")}, true, 0)
	lock = &tikv.Lock{
		Key:            []byte("a"),
		Primary:        []byte("z"),
		TxnID:          ts,
		LockType:       kvrpcpb.Op_Put,
		UseAsyncCommit: true,
		MinCommitTS:    ts + 5,
	}

	_ = s.beginAsyncCommit(c)

	err = resolver.ResolveLockAsync(s.bo, lock, status)
	c.Assert(err, IsNil)
	c.Assert(gotCheckA, Equals, int64(1))
	c.Assert(gotCheckB, Equals, int64(1))
	c.Assert(gotOther, Equals, int64(0))
	c.Assert(gotResolve, Equals, int64(1))

	// One key has been rolled back (b), one is locked (a). Should be rolled back.
	ts, err = s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
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

	err = resolver.ResolveLockAsync(s.bo, lock, status)
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
		ctx := context.WithValue(context.Background(), util.SessionID, sessionID)
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
		c.Assert(txn1.IsAsyncCommit(), IsTrue)
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

// It's just a simple validation of linearizability.
// Extra tests are needed to test this feature with the control of the TiKV cluster.
func (s *testAsyncCommitSuite) TestAsyncCommitLinearizability(c *C) {
	t1 := s.beginAsyncCommitWithLinearizability(c)
	t2 := s.beginAsyncCommitWithLinearizability(c)
	err := t1.Set([]byte("a"), []byte("a1"))
	c.Assert(err, IsNil)
	err = t2.Set([]byte("b"), []byte("b1"))
	c.Assert(err, IsNil)
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	// t2 commits earlier than t1
	err = t2.Commit(ctx)
	c.Assert(err, IsNil)
	err = t1.Commit(ctx)
	c.Assert(err, IsNil)
	commitTS1 := t1.GetCommitTS()
	commitTS2 := t2.GetCommitTS()
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
	ctx := context.WithValue(context.Background(), util.SessionID, uint64(1))
	err = localTxn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(localTxn.IsAsyncCommit(), IsFalse)

	globalTxn := s.beginAsyncCommit(c)
	err = globalTxn.Set([]byte("b"), []byte("b1"))
	globalTxn.SetOption(kv.TxnScope, oracle.GlobalTxnScope)
	c.Assert(err, IsNil)
	err = globalTxn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(globalTxn.IsAsyncCommit(), IsTrue)
}

type mockResolveClient struct {
	inner              tikv.Client
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
