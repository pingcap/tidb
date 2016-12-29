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
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
	"golang.org/x/net/context"
)

type testRegionCacheSuite struct {
	cluster *mocktikv.Cluster
	store1  uint64
	store2  uint64
	peer1   uint64
	peer2   uint64
	region1 uint64
	cache   *RegionCache
	bo      *Backoffer
}

var _ = Suite(&testRegionCacheSuite{})

func (s *testRegionCacheSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.region1 = regionID
	s.store1 = storeIDs[0]
	s.store2 = storeIDs[1]
	s.peer1 = peerIDs[0]
	s.peer2 = peerIDs[1]
	pdCli := &codecPDClient{mocktikv.NewPDClient(s.cluster)}
	s.cache = NewRegionCache(pdCli)
	s.bo = NewBackoffer(5000, context.Background())
}

func (s *testRegionCacheSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

func (s *testRegionCacheSuite) checkCache(c *C, len int) {
	c.Assert(s.cache.mu.regions, HasLen, len)
	c.Assert(s.cache.mu.sorted.Len(), Equals, len)
	for _, r := range s.cache.mu.regions {
		c.Assert(r, DeepEquals, s.cache.getRegionFromCache(r.StartKey()))
	}
}

func (s *testRegionCacheSuite) getRegion(c *C, key []byte) *Region {
	_, err := s.cache.LocateKey(s.bo, key)
	c.Assert(err, IsNil)
	return s.cache.getRegionFromCache(key)
}

func (s *testRegionCacheSuite) getAddr(c *C, key []byte) string {
	loc, err := s.cache.LocateKey(s.bo, key)
	c.Assert(err, IsNil)
	ctx, err := s.cache.GetRPCContext(s.bo, loc.Region)
	c.Assert(err, IsNil)
	if ctx == nil {
		return ""
	}
	return ctx.Addr
}

func (s *testRegionCacheSuite) TestSimple(c *C) {
	r := s.getRegion(c, []byte("a"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.getAddr(c, []byte("a")), Equals, s.storeAddr(s.store1))
	s.checkCache(c, 1)
}

func (s *testRegionCacheSuite) TestDropStore(c *C) {
	bo := NewBackoffer(100, context.Background())
	s.cluster.RemoveStore(s.store1)
	loc, err := s.cache.LocateKey(bo, []byte("a"))
	c.Assert(err, IsNil)
	ctx, err := s.cache.GetRPCContext(bo, loc.Region)
	c.Assert(err, IsNil)
	c.Assert(ctx, IsNil)
	s.checkCache(c, 0)
}

func (s *testRegionCacheSuite) TestDropStoreRetry(c *C) {
	s.cluster.RemoveStore(s.store1)
	done := make(chan struct{})
	go func() {
		time.Sleep(time.Millisecond * 10)
		s.cluster.AddStore(s.store1, s.storeAddr(s.store1))
		close(done)
	}()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(loc.Region.id, Equals, s.region1)
	<-done
}

func (s *testRegionCacheSuite) TestUpdateLeader(c *C) {
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(loc.Region, s.store2)

	r := s.getRegion(c, []byte("a"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.unreachableStores, HasLen, 0)
	c.Assert(s.getAddr(c, []byte("a")), Equals, s.storeAddr(s.store2))
}

func (s *testRegionCacheSuite) TestUpdateLeader2(c *C) {
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	// new store3 becomes leader
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(loc.Region, store3)

	// Store3 does not exist in cache, causes a reload from PD.
	r := s.getRegion(c, []byte("a"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.unreachableStores, HasLen, 0)
	c.Assert(s.getAddr(c, []byte("a")), Equals, s.storeAddr(s.store1))

	// tikv-server notifies new leader to pd-server.
	s.cluster.ChangeLeader(s.region1, peer3)
	// tikv-server reports `NotLeader` again.
	s.cache.UpdateLeader(r.VerID(), store3)
	r = s.getRegion(c, []byte("a"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(r.unreachableStores, HasLen, 0)
	c.Assert(s.getAddr(c, []byte("a")), Equals, s.storeAddr(store3))
}

func (s *testRegionCacheSuite) TestUpdateLeader3(c *C) {
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	// store2 becomes leader
	s.cluster.ChangeLeader(s.region1, s.peer2)
	// store2 gone, store3 becomes leader
	s.cluster.RemoveStore(s.store2)
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	// tikv-server notifies new leader to pd-server.
	s.cluster.ChangeLeader(s.region1, peer3)
	// tikv-server reports `NotLeader`(store2 is the leader)
	s.cache.UpdateLeader(loc.Region, s.store2)

	// Store2 does not exist any more, causes a reload from PD.
	r := s.getRegion(c, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	addr := s.getAddr(c, []byte("a"))
	c.Assert(addr, Equals, "")
	r = s.getRegion(c, []byte("a"))
	// pd-server should return the new leader.
	c.Assert(r.unreachableStores, HasLen, 0)
	c.Assert(s.getAddr(c, []byte("a")), Equals, s.storeAddr(store3))
}

func (s *testRegionCacheSuite) TestSplit(c *C) {
	r := s.getRegion(c, []byte("x"))
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.getAddr(c, []byte("x")), Equals, s.storeAddr(s.store1))

	// split to ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	// tikv-server reports `NotInRegion`
	s.cache.DropRegion(r.VerID())
	s.checkCache(c, 0)

	r = s.getRegion(c, []byte("x"))
	c.Assert(r.GetID(), Equals, region2)
	c.Assert(s.getAddr(c, []byte("x")), Equals, s.storeAddr(s.store1))
	s.checkCache(c, 1)
}

func (s *testRegionCacheSuite) TestMerge(c *C) {
	// ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	loc, err := s.cache.LocateKey(s.bo, []byte("x"))
	c.Assert(err, IsNil)
	c.Assert(loc.Region.id, Equals, region2)

	// merge to single region
	s.cluster.Merge(s.region1, region2)

	// tikv-server reports `NotInRegion`
	s.cache.DropRegion(loc.Region)
	s.checkCache(c, 0)

	loc, err = s.cache.LocateKey(s.bo, []byte("x"))
	c.Assert(err, IsNil)
	c.Assert(loc.Region.id, Equals, s.region1)
	s.checkCache(c, 1)
}

func (s *testRegionCacheSuite) TestReconnect(c *C) {
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)

	// connect tikv-server failed, cause drop cache
	s.cache.DropRegion(loc.Region)

	r := s.getRegion(c, []byte("a"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.getAddr(c, []byte("a")), Equals, s.storeAddr(s.store1))
	s.checkCache(c, 1)
}

func (s *testRegionCacheSuite) TestRequestFail(c *C) {
	region := s.getRegion(c, []byte("a"))
	c.Assert(region.unreachableStores, HasLen, 0)

	ctx, _ := s.cache.GetRPCContext(s.bo, region.VerID())
	s.cache.OnRequestFail(ctx)
	region = s.getRegion(c, []byte("a"))
	c.Assert(region.unreachableStores, DeepEquals, []uint64{s.store1})

	ctx, _ = s.cache.GetRPCContext(s.bo, region.VerID())
	s.cache.OnRequestFail(ctx)
	region = s.getRegion(c, []byte("a"))
	// Out of range of Peers, so get Region again and pick Stores[0] as leader.
	c.Assert(region.unreachableStores, HasLen, 0)
}

func (s *testRegionCacheSuite) TestUpdateStoreAddr(c *C) {
	client := &RawKVClient{
		clusterID:   0,
		regionCache: NewRegionCache(mocktikv.NewPDClient(s.cluster)),
		rpcClient:   mocktikv.NewRPCClient(s.cluster, mocktikv.NewMvccStore()),
	}
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
