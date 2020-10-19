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
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/store/mockstore/unistore"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

// testAsyncCommitCommon is used to put common parts that will be both used by
// testAsyncCommitSuite and testAsyncCommitFailSuite.
type testAsyncCommitCommon struct {
	cluster cluster.Cluster
	store   *tikvStore
}

func (s *testAsyncCommitCommon) setUpTest(c *C) {
	client, pdClient, cluster, err := unistore.New("")
	c.Assert(err, IsNil)
	unistore.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)

	s.store = store.(*tikvStore)
}

func (s *testAsyncCommitCommon) putAlphabets(c *C) {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.putKV(c, []byte{ch}, []byte{ch})
	}
}

func (s *testAsyncCommitCommon) putKV(c *C, key, value []byte) (uint64, uint64) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	return txn.StartTS(), txn.(*tikvTxn).commitTS
}

func (s *testAsyncCommitCommon) mustGetFromTxn(c *C, txn kv.Transaction, key, expectedValue []byte) {
	v, err := txn.Get(context.Background(), key)
	c.Assert(err, IsNil)
	c.Assert(v, BytesEquals, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetLock(c *C, key []byte) *Lock {
	ver, err := s.store.CurrentVersion()
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
	snap, err := s.store.GetSnapshot(kv.MaxVersion)
	c.Assert(err, IsNil)
	value, err := snap.Get(context.Background(), key)
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetFromSnapshot(c *C, version uint64, key, expectedValue []byte) {
	snap, err := s.store.GetSnapshot(kv.Version{Ver: version})
	c.Assert(err, IsNil)
	value, err := snap.Get(context.Background(), key)
	c.Assert(err, IsNil)
	c.Assert(value, BytesEquals, expectedValue)
}

func (s *testAsyncCommitCommon) mustGetNoneFromSnapshot(c *C, version uint64, key []byte) {
	snap, err := s.store.GetSnapshot(kv.Version{Ver: version})
	c.Assert(err, IsNil)
	_, err = snap.Get(context.Background(), key)
	c.Assert(errors.Cause(err), Equals, kv.ErrNotExist)
}

type testAsyncCommitSuite struct {
	OneByOneSuite
	testAsyncCommitCommon
	bo *Backoffer
}

var _ = Suite(&testAsyncCommitSuite{})

func (s *testAsyncCommitSuite) SetUpTest(c *C) {
	s.testAsyncCommitCommon.setUpTest(c)
	s.bo = NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testAsyncCommitSuite) lockKeys(c *C, keys, values [][]byte, primaryKey, primaryValue []byte, commitPrimary bool) (uint64, uint64) {
	txn, err := newTiKVTxn(s.store)
	c.Assert(err, IsNil)
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
		tpc.commitTS, err = s.store.oracle.GetTimestamp(ctx)
		c.Assert(err, IsNil)
		err = tpc.commitMutations(NewBackofferWithVars(ctx, int(atomic.LoadUint64(&CommitMaxBackoff)), nil), tpc.mutationsOfKeys([][]byte{primaryKey}))
		c.Assert(err, IsNil)
	}
	return txn.startTS, tpc.commitTS
}

func (s *testAsyncCommitSuite) TestCheckSecondaries(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.Enable = true
	})

	s.putAlphabets(c)

	loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.Split(loc.Region.id, newRegionID, []byte("e"), []uint64{peerID}, peerID)
	s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)

	// No locks to check, only primary key is locked, should be successful.
	s.lockKeys(c, [][]byte{}, [][]byte{}, []byte("z"), []byte("z"), false)
	lock := s.mustGetLock(c, []byte("z"))
	lock.UseAsyncCommit = true
	ts, err := s.store.oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	status := TxnStatus{primaryLock: &kvrpcpb.LockInfo{Secondaries: [][]byte{}, UseAsyncCommit: true, MinCommitTs: ts}}

	err = s.store.lockResolver.resolveLockAsync(s.bo, lock, status)
	c.Assert(err, IsNil)
	currentTS, err := s.store.oracle.GetTimestamp(context.Background())
	c.Assert(err, IsNil)
	status, err = s.store.lockResolver.getTxnStatus(s.bo, lock.TxnID, []byte("z"), currentTS, currentTS, true)
	c.Assert(err, IsNil)
	c.Assert(status.IsCommitted(), IsTrue)
	c.Assert(status.CommitTS(), Equals, ts)

	// One key is committed (i), one key is locked (a). Should get committed.
	ts, err = s.store.oracle.GetTimestamp(context.Background())
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
						Locks:    []*kvrpcpb.LockInfo{{Key: []byte("a"), PrimaryLock: []byte("z"), LockVersion: ts}},
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

	_, err = s.store.Begin()
	c.Assert(err, IsNil)

	err = s.store.lockResolver.resolveLockAsync(s.bo, lock, status)
	c.Assert(err, IsNil)
	c.Assert(gotCheckA, Equals, int64(1))
	c.Assert(gotCheckB, Equals, int64(1))
	c.Assert(gotOther, Equals, int64(0))
	c.Assert(gotResolve, Equals, int64(1))

	// One key has been rolled back (b), one is locked (a). Should be rolled back.
	ts, err = s.store.oracle.GetTimestamp(context.Background())
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
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.Enable = true
	})

	var connID uint64 = 0
	test := func(isPessimistic bool) {
		s.putKV(c, []byte("k1"), []byte("v1"))

		connID++
		ctx := context.WithValue(context.Background(), sessionctx.ConnID, connID)
		txn1, err := s.store.Begin()
		txn1.SetOption(kv.Pessimistic, isPessimistic)
		c.Assert(err, IsNil)
		s.mustGetFromTxn(c, txn1, []byte("k1"), []byte("v1"))
		txn1.Set([]byte("k1"), []byte("v2"))

		for i := 0; i < 20; i++ {
			_, err := s.store.GetOracle().GetTimestamp(ctx)
			c.Assert(err, IsNil)
		}

		txn2, err := s.store.Begin()
		c.Assert(err, IsNil)
		s.mustGetFromTxn(c, txn2, []byte("k1"), []byte("v1"))

		err = txn1.Commit(ctx)
		c.Assert(err, IsNil)
		// Check txn1 is committed in async commit.
		c.Assert(txn1.(*tikvTxn).committer.isAsyncCommit(), IsTrue)
		s.mustGetFromTxn(c, txn2, []byte("k1"), []byte("v1"))
		err = txn2.Rollback()
		c.Assert(err, IsNil)

		txn3, err := s.store.Begin()
		c.Assert(err, IsNil)
		s.mustGetFromTxn(c, txn3, []byte("k1"), []byte("v2"))
		err = txn3.Rollback()
		c.Assert(err, IsNil)
	}

	test(false)
	test(true)
}

func (s *testAsyncCommitSuite) Test1PC(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableOnePC = true
	})

	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))

	k1 := []byte("k1")
	v1 := []byte("v1")

	begin := func() *tikvTxn {
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		return txn.(*tikvTxn)
	}

	txn := begin()
	err := txn.Set(k1, v1)
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsTrue)
	c.Assert(txn.committer.onePCCommitTS, Equals, txn.committer.commitTS)
	c.Assert(txn.committer.onePCCommitTS, Greater, txn.startTS)

	// 1PC doesn't work if connID == 0
	k2 := []byte("k2")
	v2 := []byte("v2")

	txn = begin()
	err = txn.Set(k2, v2)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsFalse)
	c.Assert(txn.committer.onePCCommitTS, Equals, uint64(0))
	c.Assert(txn.committer.commitTS, Greater, txn.startTS)

	// 1PC doesn't work if config not set
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableOnePC = false
	})
	k3 := []byte("k3")
	v3 := []byte("v3")

	txn = begin()
	err = txn.Set(k3, v3)
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsFalse)
	c.Assert(txn.committer.onePCCommitTS, Equals, uint64(0))
	c.Assert(txn.committer.commitTS, Greater, txn.startTS)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableOnePC = true
	})

	// Test multiple keys
	k4 := []byte("k4")
	v4 := []byte("v4")
	k5 := []byte("k5")
	v5 := []byte("v5")
	k6 := []byte("k6")
	v6 := []byte("v6")

	txn = begin()
	err = txn.Set(k4, v4)
	c.Assert(err, IsNil)
	err = txn.Set(k5, v5)
	c.Assert(err, IsNil)
	err = txn.Set(k6, v6)
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsTrue)
	c.Assert(txn.committer.onePCCommitTS, Equals, txn.committer.commitTS)
	c.Assert(txn.committer.onePCCommitTS, Greater, txn.startTS)
	// Check keys are committed with the same version
	s.mustGetFromSnapshot(c, txn.commitTS, k4, v4)
	s.mustGetFromSnapshot(c, txn.commitTS, k5, v5)
	s.mustGetFromSnapshot(c, txn.commitTS, k6, v6)
	s.mustGetNoneFromSnapshot(c, txn.commitTS-1, k4)
	s.mustGetNoneFromSnapshot(c, txn.commitTS-1, k5)
	s.mustGetNoneFromSnapshot(c, txn.commitTS-1, k6)

	// Overwriting in MVCC
	v6New := []byte("v6new")
	txn = begin()
	err = txn.Set(k6, v6New)
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsTrue)
	c.Assert(txn.committer.onePCCommitTS, Equals, txn.committer.commitTS)
	c.Assert(txn.committer.onePCCommitTS, Greater, txn.startTS)
	s.mustGetFromSnapshot(c, txn.commitTS, k6, v6New)
	s.mustGetFromSnapshot(c, txn.commitTS-1, k6, v6)

	// 1PC doesn't work if it affects multiple regions.
	loc, err := s.store.regionCache.LocateKey(s.bo, k4)
	c.Assert(err, IsNil)
	newRegionID := s.cluster.AllocID()
	newPeerID := s.cluster.AllocID()
	s.cluster.Split(loc.Region.id, newRegionID, k4, []uint64{newPeerID}, newPeerID)

	k35 := []byte("k35")
	v35 := []byte("v35")
	k45 := []byte("k45")
	v45 := []byte("v45")
	txn = begin()
	err = txn.Set(k35, v35)
	c.Assert(err, IsNil)
	err = txn.Set(k45, v45)
	c.Assert(err, IsNil)
	err = txn.Commit(ctx)
	c.Assert(err, IsNil)
	c.Assert(txn.committer.isOnePC(), IsFalse)
	c.Assert(txn.committer.onePCCommitTS, Equals, uint64(0))
	c.Assert(txn.committer.commitTS, Greater, txn.startTS)

	// Check all keys
	keys := [][]byte{k1, k2, k3, k35, k4, k45, k5, k6}
	values := [][]byte{v1, v2, v3, v35, v4, v45, v5, v6New}
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	snap, err := s.store.GetSnapshot(ver)
	c.Assert(err, IsNil)
	for i, k := range keys {
		v, err := snap.Get(ctx, k)
		c.Assert(err, IsNil)
		c.Assert(v, BytesEquals, values[i])
	}
}

func (s *testAsyncCommitSuite) Test1PCIsolation(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableOnePC = true
	})

	ctx := context.WithValue(context.Background(), sessionctx.ConnID, uint64(1))

	k := []byte("k")
	v1 := []byte("v1")

	begin := func() *tikvTxn {
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		return txn.(*tikvTxn)
	}

	txn := begin()
	txn.Set(k, v1)
	err := txn.Commit(ctx)
	c.Assert(err, IsNil)

	v2 := []byte("v2")
	txn = begin()
	txn.Set(k, v2)

	time.Sleep(time.Millisecond * 300)

	txn2 := begin()
	s.mustGetFromTxn(c, txn2, k, v1)

	err = txn.Commit(ctx)
	c.Assert(txn.committer.isOnePC(), IsTrue)
	c.Assert(err, IsNil)

	s.mustGetFromTxn(c, txn2, k, v1)
	c.Assert(txn2.Rollback(), IsNil)

	s.mustGetFromSnapshot(c, txn.commitTS, k, v2)
	s.mustGetFromSnapshot(c, txn.commitTS-1, k, v1)
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
