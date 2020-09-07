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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

type testSplitSuite struct {
	OneByOneSuite
	cluster *mocktikv.Cluster
	store   *tikvStore
	bo      *Backoffer
}

var _ = Suite(&testSplitSuite{})

func (s *testSplitSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(s.cluster)
	client, pdClient, err := mocktikv.NewTiKVAndPDClient(s.cluster, nil, "")
	c.Assert(err, IsNil)

	store, err := NewTestTiKVStore(client, pdClient, nil, nil, 0)
	c.Check(err, IsNil)
	s.store = store.(*tikvStore)
	s.bo = NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testSplitSuite) begin(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func (s *testSplitSuite) split(c *C, regionID uint64, key []byte) {
	newRegionID, peerID := s.cluster.AllocID(), s.cluster.AllocID()
	s.cluster.Split(regionID, newRegionID, key, []uint64{peerID}, peerID)
}

func (s *testSplitSuite) TestSplitBatchGet(c *C) {
	loc, err := s.store.regionCache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)

	txn := s.begin(c)
	snapshot := newTiKVSnapshot(s.store, kv.Version{Ver: txn.StartTS()}, 0)

	keys := [][]byte{{'a'}, {'b'}, {'c'}}
	_, region, err := s.store.regionCache.GroupKeysByRegion(s.bo, keys, nil)
	c.Assert(err, IsNil)
	batch := batchKeys{
		region: region,
		keys:   keys,
	}

	s.split(c, loc.Region.id, []byte("b"))
	s.store.regionCache.InvalidateCachedRegion(loc.Region)

	// mocktikv will panic if it meets a not-in-region key.
	err = snapshot.batchGetSingleRegion(s.bo, batch, func([]byte, []byte) {})
	c.Assert(err, IsNil)
}

func (s *testSplitSuite) TestStaleEpoch(c *C) {
	mockPDClient := &mockPDClient{client: s.store.regionCache.pdClient}
	s.store.regionCache.pdClient = mockPDClient

	loc, err := s.store.regionCache.LocateKey(s.bo, []byte("a"))
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
	s.split(c, loc.Region.id, []byte("b"))
	mockPDClient.disable()

	txn = s.begin(c)
	_, err = txn.Get(context.TODO(), []byte("a"))
	c.Assert(err, IsNil)
	_, err = txn.Get(context.TODO(), []byte("c"))
	c.Assert(err, IsNil)
}
