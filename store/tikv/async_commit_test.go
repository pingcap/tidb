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
	"sort"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type testAsyncCommitSuite struct {
	OneByOneSuite
	cluster cluster.Cluster
	store   *tikvStore
	bo      *Backoffer
}

var _ = Suite(&testAsyncCommitSuite{})

func (s *testAsyncCommitSuite) SetUpTest(c *C) {
	client, clstr, pdClient, err := mocktikv.NewTiKVAndPDClient("")
	c.Assert(err, IsNil)
	mocktikv.BootstrapWithSingleStore(clstr)
	s.cluster = clstr
	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)

	s.store = store.(*tikvStore)
	s.bo = NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testAsyncCommitSuite) putAlphabets(c *C) {
	for ch := byte('a'); ch <= byte('z'); ch++ {
		s.putKV(c, []byte{ch}, []byte{ch})
	}
}

func (s *testAsyncCommitSuite) putKV(c *C, key, value []byte) (uint64, uint64) {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = txn.Set(key, value)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	return txn.StartTS(), txn.(*tikvTxn).commitTS
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

func (s *testAsyncCommitSuite) mustGetLock(c *C, key []byte) *Lock {
	ver, err := s.store.CurrentVersion()
	c.Assert(err, IsNil)
	req := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Key:     key,
		Version: ver.Ver,
	})
	loc, err := s.store.regionCache.LocateKey(s.bo, key)
	c.Assert(err, IsNil)
	resp, err := s.store.SendReq(s.bo, req, loc.Region, readTimeoutShort)
	c.Assert(err, IsNil)
	c.Assert(resp.Resp, NotNil)
	keyErr := resp.Resp.(*kvrpcpb.GetResponse).GetError()
	c.Assert(keyErr, NotNil)
	lock, err := extractLockFromKeyErr(keyErr)
	c.Assert(err, IsNil)
	return lock
}

func (s *testAsyncCommitSuite) TestCheckSecondaries(c *C) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableAsyncCommit = true
	})
	defer config.RestoreFunc()()

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

func (s *testAsyncCommitSuite) TestSecondaryListInPrimaryLock(c *C) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.EnableAsyncCommit = true
	})
	defer config.RestoreFunc()()

	s.putAlphabets(c)

	// Split into several regions.
	for _, splitKey := range []string{"h", "o", "u"} {
		loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte(splitKey))
		c.Assert(err, IsNil)
		newRegionID := s.cluster.AllocID()
		newPeerID := s.cluster.AllocID()
		s.cluster.Split(loc.Region.GetID(), newRegionID, []byte(splitKey), []uint64{newPeerID}, newPeerID)
		s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)
	}

	// Ensure the region has been split
	loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte("i"))
	c.Assert(err, IsNil)
	c.Assert([]byte(loc.StartKey), BytesEquals, []byte("h"))
	c.Assert([]byte(loc.EndKey), BytesEquals, []byte("o"))

	loc, err = s.store.GetRegionCache().LocateKey(s.bo, []byte("p"))
	c.Assert(err, IsNil)
	c.Assert(loc.StartKey, BytesEquals, []byte("o"))
	c.Assert(loc.EndKey, BytesEquals, []byte("u"))

	test := func(keys []string, values []string) {
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		for i := range keys {
			txn.Set([]byte(keys[i]), []byte(values[i]))
		}

		c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/tikv/asyncCommitDoNothing", "return"), IsNil)

		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)

		primary := txn.(*tikvTxn).committer.primary()
		txnStatus, err := s.store.lockResolver.getTxnStatus(s.bo, txn.StartTS(), primary, 0, 0, false)
		c.Assert(err, IsNil)
		c.Assert(txnStatus.IsCommitted, IsFalse)
		c.Assert(txnStatus.action, Equals, kvrpcpb.Action_NoAction)
		expectedSecondaries := make([][]byte, 0, len(keys)-1)
		for _, k := range keys {
			if !bytes.Equal([]byte(k), primary) {
				expectedSecondaries = append(expectedSecondaries, []byte(k))
			}
		}
		sort.Slice(expectedSecondaries, func(i, j int) bool {
			return bytes.Compare(expectedSecondaries[i], expectedSecondaries[j]) < 0
		})

		gotSecondaries := txnStatus.primaryLock.GetSecondaries()
		sort.Slice(gotSecondaries, func(i, j int) bool {
			return bytes.Compare(gotSecondaries[i], gotSecondaries[j]) < 0
		})

		c.Assert(gotSecondaries, DeepEquals, expectedSecondaries)

		c.Assert(failpoint.Disable("github.com/pingcap/tidb/store/tikv/asyncCommitDoNothing"), IsNil)
	}

	test([]string{"a"}, []string{"a1"})
	test([]string{"a", "b"}, []string{"a2", "b2"})
	test([]string{"a", "b", "d"}, []string{"a3", "b3", "d3"})
	test([]string{"a", "b", "h", "i", "u"}, []string{"a4", "b4", "h4", "i4", "u4"})
	test([]string{"i", "a", "z", "u", "b"}, []string{"i5", "a5", "z5", "u5", "b5"})
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
