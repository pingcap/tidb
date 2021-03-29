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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/mockstore/cluster"
)

type testSplitSuite struct {
	OneByOneSuite
	cluster cluster.Cluster
	store   tikv.StoreProbe
	bo      *tikv.Backoffer
}

var _ = Suite(&testSplitSuite{})

func (s *testSplitSuite) SetUpTest(c *C) {
	client, cluster, pdClient, err := mocktikv.NewTiKVAndPDClient("")
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
