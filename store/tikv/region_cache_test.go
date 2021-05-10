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
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/btree"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/mockstore/mocktikv"
	pd "github.com/tikv/pd/client"
)

type testRegionCacheSuite struct {
	OneByOneSuite
	cluster *mocktikv.Cluster
	store1  uint64 // store1 is leader
	store2  uint64 // store2 is follower
	peer1   uint64 // peer1 is leader
	peer2   uint64 // peer2 is follower
	region1 uint64
	cache   *RegionCache
	bo      *Backoffer
}

var _ = Suite(&testRegionCacheSuite{})

func (s *testRegionCacheSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster(mocktikv.MustNewMVCCStore())
	storeIDs, peerIDs, regionID, _ := mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.region1 = regionID
	s.store1 = storeIDs[0]
	s.store2 = storeIDs[1]
	s.peer1 = peerIDs[0]
	s.peer2 = peerIDs[1]
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster)}
	s.cache = NewRegionCache(pdCli)
	s.bo = NewBackofferWithVars(context.Background(), 5000, nil)
}

func (s *testRegionCacheSuite) TearDownTest(c *C) {
	s.cache.Close()
}

func (s *testRegionCacheSuite) storeAddr(id uint64) string {
	return fmt.Sprintf("store%d", id)
}

func (s *testRegionCacheSuite) checkCache(c *C, len int) {
	ts := time.Now().Unix()
	c.Assert(validRegions(s.cache.mu.regions, ts), Equals, len)
	c.Assert(validRegionsSearchedByVersions(s.cache.mu.latestVersions, s.cache.mu.regions, ts), Equals, len)
	c.Assert(validRegionsInBtree(s.cache.mu.sorted, ts), Equals, len)
}

func validRegionsSearchedByVersions(
	versions map[uint64]RegionVerID,
	regions map[RegionVerID]*Region,
	ts int64,
) (count int) {
	for _, ver := range versions {
		region, ok := regions[ver]
		if !ok || !region.checkRegionCacheTTL(ts) {
			continue
		}
		count++
	}
	return
}

func validRegions(regions map[RegionVerID]*Region, ts int64) (len int) {
	for _, region := range regions {
		if !region.checkRegionCacheTTL(ts) {
			continue
		}
		len++
	}
	return
}

func validRegionsInBtree(t *btree.BTree, ts int64) (len int) {
	t.Descend(func(item btree.Item) bool {
		r := item.(*btreeItem).cachedRegion
		if !r.checkRegionCacheTTL(ts) {
			return true
		}
		len++
		return true
	})
	return
}

func (s *testRegionCacheSuite) getRegion(c *C, key []byte) *Region {
	_, err := s.cache.LocateKey(s.bo, key)
	c.Assert(err, IsNil)
	r := s.cache.searchCachedRegion(key, false)
	c.Assert(r, NotNil)
	return r
}

func (s *testRegionCacheSuite) getRegionWithEndKey(c *C, key []byte) *Region {
	_, err := s.cache.LocateEndKey(s.bo, key)
	c.Assert(err, IsNil)
	r := s.cache.searchCachedRegion(key, true)
	c.Assert(r, NotNil)
	return r
}

func (s *testRegionCacheSuite) getAddr(c *C, key []byte, replicaRead kv.ReplicaReadType, seed uint32) string {
	loc, err := s.cache.LocateKey(s.bo, key)
	c.Assert(err, IsNil)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, replicaRead, seed)
	c.Assert(err, IsNil)
	if ctx == nil {
		return ""
	}
	return ctx.Addr
}

func (s *testRegionCacheSuite) TestStoreLabels(c *C) {
	testcases := []struct {
		storeID uint64
	}{
		{
			storeID: s.store1,
		},
		{
			storeID: s.store2,
		},
	}
	for _, testcase := range testcases {
		c.Log(testcase.storeID)
		store := s.cache.getStoreByStoreID(testcase.storeID)
		_, err := store.initResolve(s.bo, s.cache)
		c.Assert(err, IsNil)
		labels := []*metapb.StoreLabel{
			{
				Key:   "id",
				Value: fmt.Sprintf("%v", testcase.storeID),
			},
		}
		stores := s.cache.getStoresByLabels(labels)
		c.Assert(len(stores), Equals, 1)
		c.Assert(stores[0].labels, DeepEquals, labels)
	}
}

func (s *testRegionCacheSuite) TestSimple(c *C) {
	seed := rand.Uint32()
	r := s.getRegion(c, []byte("a"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.getAddr(c, []byte("a"), kv.ReplicaReadLeader, 0), Equals, s.storeAddr(s.store1))
	c.Assert(s.getAddr(c, []byte("a"), kv.ReplicaReadFollower, seed), Equals, s.storeAddr(s.store2))
	s.checkCache(c, 1)
	c.Assert(r.GetMeta(), DeepEquals, r.meta)
	c.Assert(r.GetLeaderPeerID(), Equals, r.meta.Peers[r.getStore().workTiKVIdx].Id)
	s.cache.mu.regions[r.VerID()].lastAccess = 0
	r = s.cache.searchCachedRegion([]byte("a"), true)
	c.Assert(r, IsNil)
}

func (s *testRegionCacheSuite) TestDropStore(c *C) {
	bo := NewBackofferWithVars(context.Background(), 100, nil)
	s.cluster.RemoveStore(s.store1)
	loc, err := s.cache.LocateKey(bo, []byte("a"))
	c.Assert(err, IsNil)
	ctx, err := s.cache.GetTiKVRPCContext(bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx, IsNil)
	ctx, err = s.cache.GetTiKVRPCContext(bo, loc.Region, kv.ReplicaReadFollower, rand.Uint32())
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
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(loc.Region, s.store2, 0)

	r := s.getRegion(c, []byte("a"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.getAddr(c, []byte("a"), kv.ReplicaReadLeader, 0), Equals, s.storeAddr(s.store2))
	c.Assert(s.getAddr(c, []byte("a"), kv.ReplicaReadFollower, seed), Equals, s.storeAddr(s.store1))

	r = s.getRegionWithEndKey(c, []byte("z"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.getAddr(c, []byte("z"), kv.ReplicaReadLeader, 0), Equals, s.storeAddr(s.store2))
	c.Assert(s.getAddr(c, []byte("a"), kv.ReplicaReadFollower, seed), Equals, s.storeAddr(s.store1))
}

func (s *testRegionCacheSuite) TestUpdateLeader2(c *C) {
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	// new store3 becomes leader
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	// tikv-server reports `NotLeader`
	s.cache.UpdateLeader(loc.Region, store3, 0)

	// Store3 does not exist in cache, causes a reload from PD.
	r := s.getRegion(c, []byte("a"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.getAddr(c, []byte("a"), kv.ReplicaReadLeader, 0), Equals, s.storeAddr(s.store1))
	follower := s.getAddr(c, []byte("a"), kv.ReplicaReadFollower, seed)
	if seed%2 == 0 {
		c.Assert(follower, Equals, s.storeAddr(s.store2))
	} else {
		c.Assert(follower, Equals, s.storeAddr(store3))
	}
	follower2 := s.getAddr(c, []byte("a"), kv.ReplicaReadFollower, seed+1)
	if (seed+1)%2 == 0 {
		c.Assert(follower2, Equals, s.storeAddr(s.store2))
	} else {
		c.Assert(follower2, Equals, s.storeAddr(store3))
	}
	c.Assert(follower, Not(Equals), follower2)

	// tikv-server notifies new leader to pd-server.
	s.cluster.ChangeLeader(s.region1, peer3)
	// tikv-server reports `NotLeader` again.
	s.cache.UpdateLeader(r.VerID(), store3, 0)
	r = s.getRegion(c, []byte("a"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.getAddr(c, []byte("a"), kv.ReplicaReadLeader, 0), Equals, s.storeAddr(store3))
	follower = s.getAddr(c, []byte("a"), kv.ReplicaReadFollower, seed)
	if seed%2 == 0 {
		c.Assert(follower, Equals, s.storeAddr(s.store1))
	} else {
		c.Assert(follower, Equals, s.storeAddr(s.store2))
	}
	follower2 = s.getAddr(c, []byte("a"), kv.ReplicaReadFollower, seed+1)
	if (seed+1)%2 == 0 {
		c.Assert(follower2, Equals, s.storeAddr(s.store1))
	} else {
		c.Assert(follower2, Equals, s.storeAddr(s.store2))
	}
	c.Assert(follower, Not(Equals), follower2)
}

func (s *testRegionCacheSuite) TestUpdateLeader3(c *C) {
	seed := rand.Uint32()
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
	s.cache.UpdateLeader(loc.Region, s.store2, 0)

	// Store2 does not exist any more, causes a reload from PD.
	r := s.getRegion(c, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	loc, err = s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	// return resolved store2 address and send fail
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, seed)
	c.Assert(err, IsNil)
	c.Assert(ctx.Addr, Equals, "store2")
	s.cache.OnSendFail(NewNoopBackoff(context.Background()), ctx, false, errors.New("send fail"))
	s.cache.checkAndResolve(nil)
	s.cache.UpdateLeader(loc.Region, s.store2, 0)
	addr := s.getAddr(c, []byte("a"), kv.ReplicaReadLeader, 0)
	c.Assert(addr, Equals, "")
	addr = s.getAddr(c, []byte("a"), kv.ReplicaReadLeader, 0)
	c.Assert(addr, Equals, s.storeAddr(store3))

	addr = s.getAddr(c, []byte("a"), kv.ReplicaReadFollower, seed)
	addr2 := s.getAddr(c, []byte("a"), kv.ReplicaReadFollower, seed+1)
	c.Assert(addr, Not(Equals), s.storeAddr(store3))
	c.Assert(addr2, Not(Equals), s.storeAddr(store3))
}

func (s *testRegionCacheSuite) TestSendFailedButLeaderNotChange(c *C) {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer1)
	c.Assert(len(ctx.Meta.Peers), Equals, 3)

	// verify follower to be one of store2 and store3
	seed := rand.Uint32()
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer2)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, peer3)
	}
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer2)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, peer3)
	}
	c.Assert(ctxFollower1.Peer.Id, Equals, ctxFollower2.Peer.Id)

	// send fail leader switch to 2
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	c.Assert(err, IsNil)
	if (seed+1)%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, peer3)
	}
	c.Assert(ctxFollower1.Peer.Id, Not(Equals), ctxFollower2.Peer.Id)

	// access 1 it will return NotLeader, leader back to 2 again
	s.cache.UpdateLeader(loc.Region, s.store2, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	c.Assert(err, IsNil)
	if (seed+1)%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, peer3)
	}
	c.Assert(ctxFollower1.Peer.Id, Not(Equals), ctxFollower2.Peer.Id)
}

func (s *testRegionCacheSuite) TestSendFailedInHibernateRegion(c *C) {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer1)
	c.Assert(len(ctx.Meta.Peers), Equals, 3)

	// verify follower to be one of store2 and store3
	seed := rand.Uint32()
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer2)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, peer3)
	}
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer2)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, peer3)
	}
	c.Assert(ctxFollower1.Peer.Id, Equals, ctxFollower2.Peer.Id)

	// send fail leader switch to 2
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, peer3)
	}
	c.Assert(ctxFollower1.Peer.Id == s.peer1 || ctxFollower1.Peer.Id == peer3, IsTrue)
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	c.Assert(err, IsNil)
	if (seed+1)%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, peer3)
	}
	c.Assert(ctxFollower1.Peer.Id, Not(Equals), ctxFollower2.Peer.Id)

	// access 2, it's in hibernate and return 0 leader, so switch to 3
	s.cache.UpdateLeader(loc.Region, 0, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, peer3)

	// verify follower to be one of store1 and store2
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer2)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer2)
	}
	c.Assert(ctxFollower1.Peer.Id, Equals, ctxFollower2.Peer.Id)

	// again peer back to 1
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	s.cache.UpdateLeader(loc.Region, 0, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer1)

	// verify follower to be one of store2 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer2)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	c.Assert(err, IsNil)
	if (seed+1)%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer2)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, peer3)
	}
	c.Assert(ctxFollower1.Peer.Id, Not(Equals), ctxFollower2.Peer.Id)
}

func (s *testRegionCacheSuite) TestSendFailInvalidateRegionsInSameStore(c *C) {
	// key range: ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	// Check the two regions.
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(loc1.Region.id, Equals, s.region1)
	loc2, err := s.cache.LocateKey(s.bo, []byte("x"))
	c.Assert(err, IsNil)
	c.Assert(loc2.Region.id, Equals, region2)

	// Send fail on region1
	ctx, _ := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	s.checkCache(c, 2)
	s.cache.OnSendFail(s.bo, ctx, false, errors.New("test error"))

	// Get region2 cache will get nil then reload.
	ctx2, err := s.cache.GetTiKVRPCContext(s.bo, loc2.Region, kv.ReplicaReadLeader, 0)
	c.Assert(ctx2, IsNil)
	c.Assert(err, IsNil)
}

func (s *testRegionCacheSuite) TestSendFailEnableForwarding(c *C) {
	s.cache.enableForwarding = true

	// key range: ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	var storeState uint32 = uint32(unreachable)
	s.cache.testingKnobs.mockRequestLiveness = func(s *Store, bo *Backoffer) livenessState {
		return livenessState(atomic.LoadUint32(&storeState))
	}

	// Check the two regions.
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(loc1.Region.id, Equals, s.region1)

	// Invoke OnSendFail so that the store will be marked as needForwarding
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx, NotNil)
	s.cache.OnSendFail(s.bo, ctx, false, errors.New("test error"))

	// ...then on next retry, proxy will be used
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx, NotNil)
	c.Assert(ctx.ProxyStore, NotNil)
	c.Assert(ctx.ProxyStore.storeID, Equals, s.store2)

	// Proxy will be also applied to other regions whose leader is on the store
	loc2, err := s.cache.LocateKey(s.bo, []byte("x"))
	c.Assert(err, IsNil)
	c.Assert(loc2.Region.id, Equals, region2)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc2.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx, NotNil)
	c.Assert(ctx.ProxyStore, NotNil)
	c.Assert(ctx.ProxyStore.storeID, Equals, s.store2)

	// Recover the store
	atomic.StoreUint32(&storeState, uint32(reachable))
	// The proxy should be unset after several retries
	for retry := 0; retry < 15; retry++ {
		ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
		c.Assert(err, IsNil)
		if ctx.ProxyStore == nil {
			break
		}
		time.Sleep(time.Millisecond * 200)
	}
	c.Assert(ctx.ProxyStore, IsNil)
}

func (s *testRegionCacheSuite) TestSendFailedInMultipleNode(c *C) {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer1)
	c.Assert(len(ctx.Meta.Peers), Equals, 3)

	// verify follower to be one of store2 and store3
	seed := rand.Uint32()
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer2)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, peer3)
	}
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer2)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, peer3)
	}
	c.Assert(ctxFollower1.Peer.Id, Equals, ctxFollower2.Peer.Id)

	// send fail leader switch to 2
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer2)

	// verify follower to be one of store1 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	c.Assert(err, IsNil)
	if (seed+1)%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, peer3)
	}
	c.Assert(ctxFollower1.Peer.Id, Not(Equals), ctxFollower2.Peer.Id)

	// send 2 fail leader switch to 3
	s.cache.OnSendFail(s.bo, ctx, false, nil)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, peer3)

	// verify follower to be one of store1 and store2
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer2)
	}
	c.Assert(ctxFollower1.Peer.Id == s.peer1 || ctxFollower1.Peer.Id == s.peer2, IsTrue)
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer1)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer2)
	}
	c.Assert(ctxFollower1.Peer.Id, Equals, ctxFollower2.Peer.Id)

	// 3 can be access, so switch to 1
	s.cache.UpdateLeader(loc.Region, s.store1, ctx.AccessIdx)
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer1)

	// verify follower to be one of store2 and store3
	ctxFollower1, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed)
	c.Assert(err, IsNil)
	if seed%2 == 0 {
		c.Assert(ctxFollower1.Peer.Id, Equals, s.peer2)
	} else {
		c.Assert(ctxFollower1.Peer.Id, Equals, peer3)
	}
	ctxFollower2, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, seed+1)
	c.Assert(err, IsNil)
	if (seed+1)%2 == 0 {
		c.Assert(ctxFollower2.Peer.Id, Equals, s.peer2)
	} else {
		c.Assert(ctxFollower2.Peer.Id, Equals, peer3)
	}
	c.Assert(ctxFollower1.Peer.Id, Not(Equals), ctxFollower2.Peer.Id)
}

func (s *testRegionCacheSuite) TestLabelSelectorTiKVPeer(c *C) {
	dc1Label := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "dc-1",
		},
	}
	dc2Label := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "dc-2",
		},
	}
	dc3Label := []*metapb.StoreLabel{
		{
			Key:   "zone",
			Value: "dc-3",
		},
	}
	s.cluster.UpdateStoreLabels(s.store1, dc1Label)
	s.cluster.UpdateStoreLabels(s.store2, dc2Label)

	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.UpdateStoreLabels(store3, dc1Label)
	// Region have 3 peer, leader located in dc-1, followers located in dc-1, dc-2
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	seed := rand.Uint32()

	testcases := []struct {
		name               string
		t                  kv.ReplicaReadType
		labels             []*metapb.StoreLabel
		expectStoreIDRange map[uint64]struct{}
	}{
		{
			name:   "any Peer,located in dc-1",
			t:      kv.ReplicaReadMixed,
			labels: dc1Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store1: {},
				store3:   {},
			},
		},
		{
			name:   "any Peer,located in dc-2",
			t:      kv.ReplicaReadMixed,
			labels: dc2Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store2: {},
			},
		},
		{
			name:   "only follower,located in dc-1",
			t:      kv.ReplicaReadFollower,
			labels: dc1Label,
			expectStoreIDRange: map[uint64]struct{}{
				store3: {},
			},
		},
		{
			name:   "only leader, shouldn't consider labels",
			t:      kv.ReplicaReadLeader,
			labels: dc2Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store1: {},
			},
		},
		{
			name:   "no label matching, fallback to leader",
			t:      kv.ReplicaReadMixed,
			labels: dc3Label,
			expectStoreIDRange: map[uint64]struct{}{
				s.store1: {},
			},
		},
	}

	for _, testcase := range testcases {
		c.Log(testcase.name)
		ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, testcase.t, seed, WithMatchLabels(testcase.labels))
		c.Assert(err, IsNil)
		_, exist := testcase.expectStoreIDRange[ctx.Store.storeID]
		c.Assert(exist, Equals, true)
	}
}

func (s *testRegionCacheSuite) TestSplit(c *C) {
	seed := rand.Uint32()
	r := s.getRegion(c, []byte("x"))
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.getAddr(c, []byte("x"), kv.ReplicaReadLeader, 0), Equals, s.storeAddr(s.store1))
	c.Assert(s.getAddr(c, []byte("x"), kv.ReplicaReadFollower, seed), Equals, s.storeAddr(s.store2))

	// split to ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	// tikv-server reports `NotInRegion`
	s.cache.InvalidateCachedRegion(r.VerID())
	s.checkCache(c, 0)

	r = s.getRegion(c, []byte("x"))
	c.Assert(r.GetID(), Equals, region2)
	c.Assert(s.getAddr(c, []byte("x"), kv.ReplicaReadLeader, 0), Equals, s.storeAddr(s.store1))
	c.Assert(s.getAddr(c, []byte("x"), kv.ReplicaReadFollower, seed), Equals, s.storeAddr(s.store2))
	s.checkCache(c, 1)

	r = s.getRegionWithEndKey(c, []byte("m"))
	c.Assert(r.GetID(), Equals, s.region1)
	s.checkCache(c, 2)
}

func (s *testRegionCacheSuite) TestMerge(c *C) {
	// key range: ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	loc, err := s.cache.LocateKey(s.bo, []byte("x"))
	c.Assert(err, IsNil)
	c.Assert(loc.Region.id, Equals, region2)

	// merge to single region
	s.cluster.Merge(s.region1, region2)

	// tikv-server reports `NotInRegion`
	s.cache.InvalidateCachedRegion(loc.Region)
	s.checkCache(c, 0)

	loc, err = s.cache.LocateKey(s.bo, []byte("x"))
	c.Assert(err, IsNil)
	c.Assert(loc.Region.id, Equals, s.region1)
	s.checkCache(c, 1)
}

func (s *testRegionCacheSuite) TestReconnect(c *C) {
	seed := rand.Uint32()
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)

	// connect tikv-server failed, cause drop cache
	s.cache.InvalidateCachedRegion(loc.Region)

	r := s.getRegion(c, []byte("a"))
	c.Assert(r, NotNil)
	c.Assert(r.GetID(), Equals, s.region1)
	c.Assert(s.getAddr(c, []byte("a"), kv.ReplicaReadLeader, 0), Equals, s.storeAddr(s.store1))
	c.Assert(s.getAddr(c, []byte("x"), kv.ReplicaReadFollower, seed), Equals, s.storeAddr(s.store2))
	s.checkCache(c, 1)
}

func (s *testRegionCacheSuite) TestRegionEpochAheadOfTiKV(c *C) {
	// Create a separated region cache to do this test.
	pdCli := &CodecPDClient{mocktikv.NewPDClient(s.cluster)}
	cache := NewRegionCache(pdCli)
	defer cache.Close()

	region := createSampleRegion([]byte("k1"), []byte("k2"))
	region.meta.Id = 1
	region.meta.RegionEpoch = &metapb.RegionEpoch{Version: 10, ConfVer: 10}
	cache.insertRegionToCache(region)

	r1 := metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 9, ConfVer: 10}}
	r2 := metapb.Region{Id: 1, RegionEpoch: &metapb.RegionEpoch{Version: 10, ConfVer: 9}}

	bo := NewBackofferWithVars(context.Background(), 2000000, nil)

	err := cache.OnRegionEpochNotMatch(bo, &RPCContext{Region: region.VerID()}, []*metapb.Region{&r1})
	c.Assert(err, IsNil)
	err = cache.OnRegionEpochNotMatch(bo, &RPCContext{Region: region.VerID()}, []*metapb.Region{&r2})
	c.Assert(err, IsNil)
	c.Assert(len(bo.errors), Equals, 2)
}

func (s *testRegionCacheSuite) TestRegionEpochOnTiFlash(c *C) {
	// add store3 as tiflash
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.UpdateStoreAddr(s.store1, s.storeAddr(s.store1), &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, peer3)

	// pre-load region cache
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(loc1.Region.id, Equals, s.region1)
	lctx, err := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(lctx.Peer.Id, Equals, peer3)

	// epoch-not-match on tiflash
	ctxTiFlash, err := s.cache.GetTiFlashRPCContext(s.bo, loc1.Region, true)
	c.Assert(err, IsNil)
	c.Assert(ctxTiFlash.Peer.Id, Equals, s.peer1)
	ctxTiFlash.Peer.Role = metapb.PeerRole_Learner
	r := ctxTiFlash.Meta
	reqSend := NewRegionRequestSender(s.cache, nil)
	regionErr := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{r}}}
	reqSend.onRegionError(s.bo, ctxTiFlash, nil, regionErr)

	// check leader read should not go to tiflash
	lctx, err = s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(lctx.Peer.Id, Not(Equals), s.peer1)
}

const regionSplitKeyFormat = "t%08d"

func createClusterWithStoresAndRegions(regionCnt, storeCount int) *mocktikv.Cluster {
	cluster := mocktikv.NewCluster(mocktikv.MustNewMVCCStore())
	_, _, regionID, _ := mocktikv.BootstrapWithMultiStores(cluster, storeCount)
	for i := 0; i < regionCnt; i++ {
		rawKey := []byte(fmt.Sprintf(regionSplitKeyFormat, i))
		ids := cluster.AllocIDs(4)
		// Make leaders equally distributed on the 3 stores.
		storeID := ids[0]
		peerIDs := ids[1:]
		leaderPeerID := peerIDs[i%3]
		cluster.SplitRaw(regionID, storeID, rawKey, peerIDs, leaderPeerID)
		regionID = ids[0]
	}
	return cluster
}

func loadRegionsToCache(cache *RegionCache, regionCnt int) {
	for i := 0; i < regionCnt; i++ {
		rawKey := []byte(fmt.Sprintf(regionSplitKeyFormat, i))
		cache.LocateKey(NewBackofferWithVars(context.Background(), 1, nil), rawKey)
	}
}

func (s *testRegionCacheSuite) TestUpdateStoreAddr(c *C) {
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

func (s *testRegionCacheSuite) TestReplaceAddrWithNewStore(c *C) {
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

func (s *testRegionCacheSuite) TestReplaceNewAddrAndOldOfflineImmediately(c *C) {
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

func (s *testRegionCacheSuite) TestReplaceStore(c *C) {
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

func (s *testRegionCacheSuite) TestListRegionIDsInCache(c *C) {
	// ['' - 'm' - 'z']
	region2 := s.cluster.AllocID()
	newPeers := s.cluster.AllocIDs(2)
	s.cluster.Split(s.region1, region2, []byte("m"), newPeers, newPeers[0])

	regionIDs, err := s.cache.ListRegionIDsInKeyRange(s.bo, []byte("a"), []byte("z"))
	c.Assert(err, IsNil)
	c.Assert(regionIDs, DeepEquals, []uint64{s.region1, region2})
	regionIDs, err = s.cache.ListRegionIDsInKeyRange(s.bo, []byte("m"), []byte("z"))
	c.Assert(err, IsNil)
	c.Assert(regionIDs, DeepEquals, []uint64{region2})

	regionIDs, err = s.cache.ListRegionIDsInKeyRange(s.bo, []byte("a"), []byte("m"))
	c.Assert(err, IsNil)
	c.Assert(regionIDs, DeepEquals, []uint64{s.region1, region2})
}

func (s *testRegionCacheSuite) TestScanRegions(c *C) {
	// Split at "a", "b", "c", "d"
	regions := s.cluster.AllocIDs(4)
	regions = append([]uint64{s.region1}, regions...)

	peers := [][]uint64{{s.peer1, s.peer2}}
	for i := 0; i < 4; i++ {
		peers = append(peers, s.cluster.AllocIDs(2))
	}

	for i := 0; i < 4; i++ {
		s.cluster.Split(regions[i], regions[i+1], []byte{'a' + byte(i)}, peers[i+1], peers[i+1][0])
	}

	scannedRegions, err := s.cache.scanRegions(s.bo, []byte(""), nil, 100)
	c.Assert(err, IsNil)
	c.Assert(len(scannedRegions), Equals, 5)
	for i := 0; i < 5; i++ {
		r := scannedRegions[i]
		_, p, _, _ := r.WorkStorePeer(r.getStore())

		c.Assert(r.meta.Id, Equals, regions[i])
		c.Assert(p.Id, Equals, peers[i][0])
	}

	scannedRegions, err = s.cache.scanRegions(s.bo, []byte("a"), nil, 3)
	c.Assert(err, IsNil)
	c.Assert(len(scannedRegions), Equals, 3)
	for i := 1; i < 4; i++ {
		r := scannedRegions[i-1]
		_, p, _, _ := r.WorkStorePeer(r.getStore())

		c.Assert(r.meta.Id, Equals, regions[i])
		c.Assert(p.Id, Equals, peers[i][0])
	}

	scannedRegions, err = s.cache.scanRegions(s.bo, []byte("a1"), nil, 1)
	c.Assert(err, IsNil)
	c.Assert(len(scannedRegions), Equals, 1)

	r0 := scannedRegions[0]
	_, p0, _, _ := r0.WorkStorePeer(r0.getStore())
	c.Assert(r0.meta.Id, Equals, regions[1])
	c.Assert(p0.Id, Equals, peers[1][0])

	// Test region with no leader
	s.cluster.GiveUpLeader(regions[1])
	s.cluster.GiveUpLeader(regions[3])
	scannedRegions, err = s.cache.scanRegions(s.bo, []byte(""), nil, 5)
	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		r := scannedRegions[i]
		_, p, _, _ := r.WorkStorePeer(r.getStore())

		c.Assert(r.meta.Id, Equals, regions[i*2])
		c.Assert(p.Id, Equals, peers[i*2][0])
	}
}

func (s *testRegionCacheSuite) TestBatchLoadRegions(c *C) {
	// Split at "a", "b", "c", "d"
	regions := s.cluster.AllocIDs(4)
	regions = append([]uint64{s.region1}, regions...)

	peers := [][]uint64{{s.peer1, s.peer2}}
	for i := 0; i < 4; i++ {
		peers = append(peers, s.cluster.AllocIDs(2))
	}

	for i := 0; i < 4; i++ {
		s.cluster.Split(regions[i], regions[i+1], []byte{'a' + byte(i)}, peers[i+1], peers[i+1][0])
	}

	testCases := []struct {
		startKey      []byte
		endKey        []byte
		limit         int
		expectKey     []byte
		expectRegions []uint64
	}{
		{[]byte(""), []byte("a"), 1, []byte("a"), []uint64{regions[0]}},
		{[]byte("a"), []byte("b1"), 2, []byte("c"), []uint64{regions[1], regions[2]}},
		{[]byte("a1"), []byte("d"), 2, []byte("c"), []uint64{regions[1], regions[2]}},
		{[]byte("c"), []byte("c1"), 2, nil, []uint64{regions[3]}},
		{[]byte("d"), nil, 2, nil, []uint64{regions[4]}},
	}

	for _, tc := range testCases {
		key, err := s.cache.BatchLoadRegionsFromKey(s.bo, tc.startKey, tc.limit)
		c.Assert(err, IsNil)
		if tc.expectKey != nil {
			c.Assert(key, DeepEquals, tc.expectKey)
		} else {
			c.Assert(key, HasLen, 0)
		}
		loadRegions, err := s.cache.BatchLoadRegionsWithKeyRange(s.bo, tc.startKey, tc.endKey, tc.limit)
		c.Assert(err, IsNil)
		c.Assert(loadRegions, HasLen, len(tc.expectRegions))
		for i := range loadRegions {
			c.Assert(loadRegions[i].GetID(), Equals, tc.expectRegions[i])
		}
	}

	s.checkCache(c, len(regions))
}

func (s *testRegionCacheSuite) TestFollowerReadFallback(c *C) {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer1)
	c.Assert(len(ctx.Meta.Peers), Equals, 3)

	// verify follower to be store2 and store3
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 0)
	c.Assert(err, IsNil)
	c.Assert(ctxFollower1.Peer.Id, Equals, s.peer2)
	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 1)
	c.Assert(err, IsNil)
	c.Assert(ctxFollower2.Peer.Id, Equals, peer3)
	c.Assert(ctxFollower1.Peer.Id, Not(Equals), ctxFollower2.Peer.Id)

	// send fail on store2, next follower read is going to fallback to store3
	s.cache.OnSendFail(s.bo, ctxFollower1, false, errors.New("test error"))
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadFollower, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, peer3)
}

func (s *testRegionCacheSuite) TestMixedReadFallback(c *C) {
	// 3 nodes and no.1 is leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer1)
	c.Assert(len(ctx.Meta.Peers), Equals, 3)

	// verify follower to be store1, store2 and store3
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 0)
	c.Assert(err, IsNil)
	c.Assert(ctxFollower1.Peer.Id, Equals, s.peer1)

	ctxFollower2, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 1)
	c.Assert(err, IsNil)
	c.Assert(ctxFollower2.Peer.Id, Equals, s.peer2)

	ctxFollower3, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 2)
	c.Assert(err, IsNil)
	c.Assert(ctxFollower3.Peer.Id, Equals, peer3)

	// send fail on store2, next follower read is going to fallback to store3
	s.cache.OnSendFail(s.bo, ctxFollower1, false, errors.New("test error"))
	ctx, err = s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadMixed, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.Id, Equals, s.peer2)
}

func (s *testRegionCacheSuite) TestFollowerMeetEpochNotMatch(c *C) {
	// 3 nodes and no.1 is region1 leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	// Check the two regions.
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(loc1.Region.id, Equals, s.region1)

	reqSend := NewRegionRequestSender(s.cache, nil)

	// follower read failed on store2
	followReqSeed := uint32(0)
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadFollower, followReqSeed)
	c.Assert(err, IsNil)
	c.Assert(ctxFollower1.Peer.Id, Equals, s.peer2)
	c.Assert(ctxFollower1.Store.storeID, Equals, s.store2)

	regionErr := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	reqSend.onRegionError(s.bo, ctxFollower1, &followReqSeed, regionErr)
	c.Assert(followReqSeed, Equals, uint32(1))

	regionErr = &errorpb.Error{RegionNotFound: &errorpb.RegionNotFound{}}
	reqSend.onRegionError(s.bo, ctxFollower1, &followReqSeed, regionErr)
	c.Assert(followReqSeed, Equals, uint32(2))
}

func (s *testRegionCacheSuite) TestMixedMeetEpochNotMatch(c *C) {
	// 3 nodes and no.1 is region1 leader.
	store3 := s.cluster.AllocID()
	peer3 := s.cluster.AllocID()
	s.cluster.AddStore(store3, s.storeAddr(store3))
	s.cluster.AddPeer(s.region1, store3, peer3)
	s.cluster.ChangeLeader(s.region1, s.peer1)

	// Check the two regions.
	loc1, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(loc1.Region.id, Equals, s.region1)

	reqSend := NewRegionRequestSender(s.cache, nil)

	// follower read failed on store1
	followReqSeed := uint32(0)
	ctxFollower1, err := s.cache.GetTiKVRPCContext(s.bo, loc1.Region, kv.ReplicaReadMixed, followReqSeed)
	c.Assert(err, IsNil)
	c.Assert(ctxFollower1.Peer.Id, Equals, s.peer1)
	c.Assert(ctxFollower1.Store.storeID, Equals, s.store1)

	regionErr := &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}
	reqSend.onRegionError(s.bo, ctxFollower1, &followReqSeed, regionErr)
	c.Assert(followReqSeed, Equals, uint32(1))
}

func (s *testRegionCacheSuite) TestPeersLenChange(c *C) {
	// 2 peers [peer1, peer2] and let peer2 become leader
	loc, err := s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	s.cache.UpdateLeader(loc.Region, s.store2, 0)

	// current leader is peer2 in [peer1, peer2]
	loc, err = s.cache.LocateKey(s.bo, []byte("a"))
	c.Assert(err, IsNil)
	ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
	c.Assert(err, IsNil)
	c.Assert(ctx.Peer.StoreId, Equals, s.store2)

	// simulate peer1 became down in kv heartbeat and loaded before response back.
	cpMeta := &metapb.Region{
		Id:          ctx.Meta.Id,
		StartKey:    ctx.Meta.StartKey,
		EndKey:      ctx.Meta.EndKey,
		RegionEpoch: ctx.Meta.RegionEpoch,
		Peers:       make([]*metapb.Peer, len(ctx.Meta.Peers)),
	}
	copy(cpMeta.Peers, ctx.Meta.Peers)
	cpRegion := &pd.Region{
		Meta:      cpMeta,
		DownPeers: []*metapb.Peer{{Id: s.peer1, StoreId: s.store1}},
	}
	filterUnavailablePeers(cpRegion)
	region := &Region{meta: cpRegion.Meta}
	err = region.init(s.cache)
	c.Assert(err, IsNil)
	s.cache.insertRegionToCache(region)

	// OnSendFail should not panic
	s.cache.OnSendFail(NewNoopBackoff(context.Background()), ctx, false, errors.New("send fail"))
}

func createSampleRegion(startKey, endKey []byte) *Region {
	return &Region{
		meta: &metapb.Region{
			StartKey: startKey,
			EndKey:   endKey,
		},
	}
}

func (s *testRegionCacheSuite) TestContains(c *C) {
	c.Assert(createSampleRegion(nil, nil).Contains([]byte{}), IsTrue)
	c.Assert(createSampleRegion(nil, nil).Contains([]byte{10}), IsTrue)
	c.Assert(createSampleRegion([]byte{10}, nil).Contains([]byte{}), IsFalse)
	c.Assert(createSampleRegion([]byte{10}, nil).Contains([]byte{9}), IsFalse)
	c.Assert(createSampleRegion([]byte{10}, nil).Contains([]byte{10}), IsTrue)
	c.Assert(createSampleRegion(nil, []byte{10}).Contains([]byte{}), IsTrue)
	c.Assert(createSampleRegion(nil, []byte{10}).Contains([]byte{9}), IsTrue)
	c.Assert(createSampleRegion(nil, []byte{10}).Contains([]byte{10}), IsFalse)
	c.Assert(createSampleRegion([]byte{10}, []byte{20}).Contains([]byte{}), IsFalse)
	c.Assert(createSampleRegion([]byte{10}, []byte{20}).Contains([]byte{15}), IsTrue)
	c.Assert(createSampleRegion([]byte{10}, []byte{20}).Contains([]byte{30}), IsFalse)
}

func (s *testRegionCacheSuite) TestContainsByEnd(c *C) {
	c.Assert(createSampleRegion(nil, nil).ContainsByEnd([]byte{}), IsFalse)
	c.Assert(createSampleRegion(nil, nil).ContainsByEnd([]byte{10}), IsTrue)
	c.Assert(createSampleRegion([]byte{10}, nil).ContainsByEnd([]byte{}), IsFalse)
	c.Assert(createSampleRegion([]byte{10}, nil).ContainsByEnd([]byte{10}), IsFalse)
	c.Assert(createSampleRegion([]byte{10}, nil).ContainsByEnd([]byte{11}), IsTrue)
	c.Assert(createSampleRegion(nil, []byte{10}).ContainsByEnd([]byte{}), IsFalse)
	c.Assert(createSampleRegion(nil, []byte{10}).ContainsByEnd([]byte{10}), IsTrue)
	c.Assert(createSampleRegion(nil, []byte{10}).ContainsByEnd([]byte{11}), IsFalse)
	c.Assert(createSampleRegion([]byte{10}, []byte{20}).ContainsByEnd([]byte{}), IsFalse)
	c.Assert(createSampleRegion([]byte{10}, []byte{20}).ContainsByEnd([]byte{15}), IsTrue)
	c.Assert(createSampleRegion([]byte{10}, []byte{20}).ContainsByEnd([]byte{30}), IsFalse)
}

func (s *testRegionCacheSuite) TestSwitchPeerWhenNoLeader(c *C) {
	var prevCtx *RPCContext
	for i := 0; i <= len(s.cluster.GetAllStores()); i++ {
		loc, err := s.cache.LocateKey(s.bo, []byte("a"))
		c.Assert(err, IsNil)
		ctx, err := s.cache.GetTiKVRPCContext(s.bo, loc.Region, kv.ReplicaReadLeader, 0)
		c.Assert(err, IsNil)
		if prevCtx == nil {
			c.Assert(i, Equals, 0)
		} else {
			c.Assert(ctx.AccessIdx, Not(Equals), prevCtx.AccessIdx)
			c.Assert(ctx.Peer, Not(DeepEquals), prevCtx.Peer)
		}
		s.cache.InvalidateCachedRegionWithReason(loc.Region, NoLeader)
		c.Assert(s.cache.getCachedRegionWithRLock(loc.Region).invalidReason, Equals, NoLeader)
		prevCtx = ctx
	}
}

func BenchmarkOnRequestFail(b *testing.B) {
	/*
			This benchmark simulate many concurrent requests call OnSendRequestFail method
			after failed on a store, validate that on this scene, requests don't get blocked on the
		    RegionCache lock.
	*/
	regionCnt, storeCount := 998, 3
	cluster := createClusterWithStoresAndRegions(regionCnt, storeCount)
	cache := NewRegionCache(mocktikv.NewPDClient(cluster))
	defer cache.Close()
	loadRegionsToCache(cache, regionCnt)
	bo := NewBackofferWithVars(context.Background(), 1, nil)
	loc, err := cache.LocateKey(bo, []byte{})
	if err != nil {
		b.Fatal(err)
	}
	region := cache.getRegionByIDFromCache(loc.Region.id)
	b.ResetTimer()
	regionStore := region.getStore()
	store, peer, accessIdx, _ := region.WorkStorePeer(regionStore)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rpcCtx := &RPCContext{
				Region:     loc.Region,
				Meta:       region.meta,
				AccessIdx:  accessIdx,
				Peer:       peer,
				Store:      store,
				AccessMode: TiKVOnly,
			}
			r := cache.getCachedRegionWithRLock(rpcCtx.Region)
			if r != nil {
				r.getStore().switchNextTiKVPeer(r, rpcCtx.AccessIdx)
			}
		}
	})
	if len(cache.mu.regions) != regionCnt*2/3 {
		b.Fatal(len(cache.mu.regions))
	}
}
