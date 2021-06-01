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
	"context"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/mockstore/cluster"
	"github.com/pingcap/tidb/store/tikv/mockstore/mocktikv"
	pd "github.com/tikv/pd/client"
)

type testSplitSuite struct {
	OneByOneSuite
	cluster cluster.Cluster
	store   tikv.StoreProbe
	bo      *tikv.Backoffer
}

var _ = Suite(&testSplitSuite{})

func (s *testSplitSuite) SetUpTest(c *C) {
	client, cluster, pdClient, err := mocktikv.NewTiKVAndPDClient("", mockcopr.NewCoprRPCHandler())
	c.Assert(err, IsNil)
	mocktikv.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	store, err := tikv.NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)

	// TODO: make this possible
	// store, err := mockstore.NewMockStore(
	// 	mockstore.WithClusterInspector(func(c cluster.Cluster) {
	// 		mockstore.BootstrapWithSingleStore(c)
	// 		s.cluster = c
	// 	}),
	// )
	// c.Assert(err, IsNil)
	s.store = tikv.StoreProbe{KVStore: store}
	s.bo = tikv.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testSplitSuite) begin(c *C) tikv.TxnProbe {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn
}

func (s *testSplitSuite) split(c *C, regionID uint64, key []byte) {
	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.Split(regionID, newRegionID, key, []uint64{peerID}, peerID)
}

func (s *testSplitSuite) TestSplitBatchGet(c *C) {
	loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)

	txn := s.begin(c)

	keys := [][]byte{{'a'}, {'b'}, {'c'}}
	_, region, err := s.store.GetRegionCache().GroupKeysByRegion(s.bo, keys, nil)
	c.Assert(err, IsNil)

	s.split(c, loc.Region.GetID(), []byte("b"))
	s.store.GetRegionCache().InvalidateCachedRegion(loc.Region)

	// mocktikv will panic if it meets a not-in-region key.
	err = txn.BatchGetSingleRegion(s.bo, region, keys, func([]byte, []byte) {})
	c.Assert(err, IsNil)
}

func (s *testSplitSuite) TestStaleEpoch(c *C) {
	mockPDClient := &mockPDClient{client: s.store.GetRegionCache().PDClient()}
	s.store.SetRegionCachePDClient(mockPDClient)

	loc, err := s.store.GetRegionCache().LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)

	txn := s.begin(c)
	err = txn.Set([]byte("a"), []byte("a"))
	c.Assert(err, IsNil)
	err = txn.Set([]byte("c"), []byte("c"))
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Initiate a split and disable the PD client. If it still works, the
	// new region is updated from kvrpc.
	s.split(c, loc.Region.GetID(), []byte("b"))
	mockPDClient.disable()

	txn = s.begin(c)
	_, err = txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	_, err = txn.Get(context.TODO(), []byte("c"))
	c.Assert(err, IsNil)
}

var errStopped = errors.New("stopped")

type mockPDClient struct {
	sync.RWMutex
	client pd.Client
	stop   bool
}

func (c *mockPDClient) disable() {
	c.Lock()
	defer c.Unlock()
	c.stop = true
}

func (c *mockPDClient) GetAllMembers(ctx context.Context) ([]*pdpb.Member, error) {
	return nil, nil
}

func (c *mockPDClient) GetClusterID(context.Context) uint64 {
	return 1
}

func (c *mockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return 0, 0, errors.Trace(errStopped)
	}
	return c.client.GetTS(ctx)
}

func (c *mockPDClient) GetLocalTS(ctx context.Context, dcLocation string) (int64, int64, error) {
	return c.GetTS(ctx)
}

func (c *mockPDClient) GetTSAsync(ctx context.Context) pd.TSFuture {
	return nil
}

func (c *mockPDClient) GetLocalTSAsync(ctx context.Context, dcLocation string) pd.TSFuture {
	return nil
}

func (c *mockPDClient) GetRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetRegion(ctx, key)
}

func (c *mockPDClient) GetRegionFromMember(ctx context.Context, key []byte, memberURLs []string) (*pd.Region, error) {
	return nil, nil
}

func (c *mockPDClient) GetPrevRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetPrevRegion(ctx, key)
}

func (c *mockPDClient) GetRegionByID(ctx context.Context, regionID uint64) (*pd.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetRegionByID(ctx, regionID)
}

func (c *mockPDClient) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*pd.Region, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.ScanRegions(ctx, startKey, endKey, limit)
}

func (c *mockPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.RLock()
	defer c.RUnlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetStore(ctx, storeID)
}

func (c *mockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	c.RLock()
	defer c.Unlock()

	if c.stop {
		return nil, errors.Trace(errStopped)
	}
	return c.client.GetAllStores(ctx)
}

func (c *mockPDClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *mockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	panic("unimplemented")
}

func (c *mockPDClient) Close() {}

func (c *mockPDClient) ScatterRegion(ctx context.Context, regionID uint64) error {
	return nil
}

func (c *mockPDClient) ScatterRegions(ctx context.Context, regionsID []uint64, opts ...pd.RegionsOption) (*pdpb.ScatterRegionResponse, error) {
	return nil, nil
}

func (c *mockPDClient) SplitRegions(ctx context.Context, splitKeys [][]byte, opts ...pd.RegionsOption) (*pdpb.SplitRegionsResponse, error) {
	return nil, nil
}

func (c *mockPDClient) GetOperator(ctx context.Context, regionID uint64) (*pdpb.GetOperatorResponse, error) {
	return &pdpb.GetOperatorResponse{Status: pdpb.OperatorStatus_SUCCESS}, nil
}

func (c *mockPDClient) GetLeaderAddr() string { return "mockpd" }
