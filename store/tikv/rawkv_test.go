// Copyright 2021 PingCAP, Inc.
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
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/retry"
)

type testRawkvSuite struct {
	OneByOneSuite
	cluster *mocktikv.Cluster
	store1  uint64 // store1 is leader
	store2  uint64 // store2 is follower
	peer1   uint64 // peer1 is leader
	peer2   uint64 // peer2 is follower
	region1 uint64
	bo      *retry.Backoffer
}

var _ = Suite(&testRawkvSuite{})

func (s *testRawkvSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster(mocktikv.MustNewMVCCStore())
	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.region1 = regionID
	s.store1 = storeIDs[0]
	s.store2 = storeIDs[1]
	s.peer1 = peerIDs[0]
	s.peer2 = peerIDs[1]
	s.bo = retry.NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testRawkvSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

func (s *testRawkvSuite) TestReplaceAddrWithNewStore(c *C) {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &RawKVClient{
		clusterID:   0,
		regionCache: NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	err := client.Put(testKey, testValue)
	c.Assert(err, IsNil)

	// make store2 using store1's addr and store1 offline
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)
	s.cluster.RemoveStore(s.store1)
	s.cluster.ChangeLeader(s.region1, s.peer2)
	s.cluster.RemovePeer(s.region1, s.peer1)

	getVal, err := client.Get(testKey)

	c.Assert(err, IsNil)
	c.Assert(getVal, BytesEquals, testValue)
}

func (s *testRawkvSuite) TestUpdateStoreAddr(c *C) {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &RawKVClient{
		clusterID:   0,
		regionCache: NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	err := client.Put(testKey, testValue)
	c.Assert(err, IsNil)
	// tikv-server reports `StoreNotMatch` And retry
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)

	getVal, err := client.Get(testKey)

	c.Assert(err, IsNil)
	c.Assert(getVal, BytesEquals, testValue)
}

func (s *testRawkvSuite) TestReplaceNewAddrAndOldOfflineImmediately(c *C) {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &RawKVClient{
		clusterID:   0,
		regionCache: NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	err := client.Put(testKey, testValue)
	c.Assert(err, IsNil)

	// pre-load store2's address into cache via follower-read.
	loc, err := client.regionCache.LocateKey(s.bo, testKey)
	c.Assert(err, IsNil)
	fctx, err := client.regionCache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 0)
	c.Assert(err, IsNil)
	c.Assert(fctx.Store.storeID, Equals, s.store2)
	c.Assert(fctx.Addr, Equals, "store2")

	// make store2 using store1's addr and store1 offline
	store1Addr := s.storeAddr(s.store1)
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store2))
	s.cluster.UpdateStoreAddr(s.store2, store1Addr)
	s.cluster.RemoveStore(s.store1)
	s.cluster.ChangeLeader(s.region1, s.peer2)
	s.cluster.RemovePeer(s.region1, s.peer1)

	getVal, err := client.Get(testKey)
	c.Assert(err, IsNil)
	c.Assert(getVal, BytesEquals, testValue)
}

func (s *testRawkvSuite) TestReplaceStore(c *C) {
	mvccStore := mocktikv.MustNewMVCCStore()
	defer mvccStore.Close()

	client := &RawKVClient{
		clusterID:   0,
		regionCache: NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mvccStore, nil),
	}
	defer client.Close()
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	err := client.Put(testKey, testValue)
	c.Assert(err, IsNil)

	s.cluster.MarkTombstone(s.store1)
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(s.store1))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.RemovePeer(s.region1, s.peer1)
	s.cluster.ChangeLeader(s.region1, peer3)

	err = client.Put(testKey, testValue)
	c.Assert(err, IsNil)
}
