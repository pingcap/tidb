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

package server

import (
	"math/rand"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testStoresInfoSuite{})

type testStoresInfoSuite struct{}

func checkStaleRegion(origin *metapb.Region, region *metapb.Region) error {
	o := origin.GetRegionEpoch()
	e := region.GetRegionEpoch()

	if e.GetVersion() < o.GetVersion() || e.GetConfVer() < o.GetConfVer() {
		return errors.Trace(errRegionIsStale(region, origin))
	}

	return nil
}

// Create n stores (0..n).
func newTestStores(n uint64) []*core.StoreInfo {
	stores := make([]*core.StoreInfo, 0, n)
	for i := uint64(1); i <= n; i++ {
		store := &metapb.Store{
			Id: i,
		}
		stores = append(stores, core.NewStoreInfo(store))
	}
	return stores
}

func (s *testStoresInfoSuite) TestStores(c *C) {
	n := uint64(10)
	cache := core.NewStoresInfo()
	stores := newTestStores(n)

	for i, store := range stores {
		id := store.GetId()
		c.Assert(cache.GetStore(id), IsNil)
		c.Assert(cache.BlockStore(id), NotNil)
		cache.SetStore(store)
		c.Assert(cache.GetStore(id), DeepEquals, store)
		c.Assert(cache.GetStoreCount(), Equals, int(i+1))
		c.Assert(cache.BlockStore(id), IsNil)
		c.Assert(cache.GetStore(id).IsBlocked(), IsTrue)
		c.Assert(cache.BlockStore(id), NotNil)
		cache.UnblockStore(id)
		c.Assert(cache.GetStore(id).IsBlocked(), IsFalse)
	}
	c.Assert(cache.GetStoreCount(), Equals, int(n))

	for _, store := range cache.GetStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()-1])
	}
	for _, store := range cache.GetMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()-1].Store)
	}

	c.Assert(cache.GetStoreCount(), Equals, int(n))
}

var _ = Suite(&testRegionsInfoSuite{})

type testRegionsInfoSuite struct{}

// Create n regions (0..n) of n stores (0..n).
// Each region contains np peers, the first peer is the leader.
func newTestRegions(n, np uint64) []*core.RegionInfo {
	regions := make([]*core.RegionInfo, 0, n)
	for i := uint64(0); i < n; i++ {
		peers := make([]*metapb.Peer, 0, np)
		for j := uint64(0); j < np; j++ {
			peer := &metapb.Peer{
				Id: i*np + j,
			}
			peer.StoreId = (i + j) % n
			peers = append(peers, peer)
		}
		region := &metapb.Region{
			Id:       i,
			Peers:    peers,
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
		}
		regions = append(regions, core.NewRegionInfo(region, peers[0]))
	}
	return regions
}

func (s *testRegionsInfoSuite) Test(c *C) {
	n, np := uint64(10), uint64(3)
	cache := core.NewRegionsInfo()
	regions := newTestRegions(n, np)

	for i := uint64(0); i < n; i++ {
		region := regions[i]
		regionKey := []byte{byte(i)}

		c.Assert(cache.GetRegion(i), IsNil)
		c.Assert(cache.SearchRegion(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		cache.AddRegion(region)
		checkRegion(c, cache.GetRegion(i), region)
		checkRegion(c, cache.SearchRegion(regionKey), region)
		checkRegions(c, cache, regions[0:(i+1)])

		// Update leader to peer np-1.
		region.Leader = region.Peers[np-1]
		cache.SetRegion(region)
		checkRegion(c, cache.GetRegion(i), region)
		checkRegion(c, cache.SearchRegion(regionKey), region)
		checkRegions(c, cache, regions[0:(i+1)])

		cache.RemoveRegion(region)
		c.Assert(cache.GetRegion(i), IsNil)
		c.Assert(cache.SearchRegion(regionKey), IsNil)
		checkRegions(c, cache, regions[0:i])

		// Reset leader to peer 0.
		region.Leader = region.Peers[0]
		cache.AddRegion(region)
		checkRegion(c, cache.GetRegion(i), region)
		checkRegions(c, cache, regions[0:(i+1)])
		checkRegion(c, cache.SearchRegion(regionKey), region)
	}

	for i := uint64(0); i < n; i++ {
		region := cache.RandLeaderRegion(i)
		c.Assert(region.Leader.GetStoreId(), Equals, i)

		region = cache.RandFollowerRegion(i)
		c.Assert(region.Leader.GetStoreId(), Not(Equals), i)

		c.Assert(region.GetStorePeer(i), NotNil)
	}

	// All regions will be filtered out if they have pending peers.
	for i := uint64(0); i < n; i++ {
		for j := 0; j < cache.GetStoreLeaderCount(i); j++ {
			region := cache.RandLeaderRegion(i)
			region.PendingPeers = region.Peers
			cache.SetRegion(region)
		}
		c.Assert(cache.RandLeaderRegion(i), IsNil)
	}
	for i := uint64(0); i < n; i++ {
		c.Assert(cache.RandFollowerRegion(i), IsNil)
	}
}

func checkRegion(c *C, a *core.RegionInfo, b *core.RegionInfo) {
	c.Assert(a.Region, DeepEquals, b.Region)
	c.Assert(a.Leader, DeepEquals, b.Leader)
	c.Assert(a.Peers, DeepEquals, b.Peers)
	if len(a.DownPeers) > 0 || len(b.DownPeers) > 0 {
		c.Assert(a.DownPeers, DeepEquals, b.DownPeers)
	}
	if len(a.PendingPeers) > 0 || len(b.PendingPeers) > 0 {
		c.Assert(a.PendingPeers, DeepEquals, b.PendingPeers)
	}
}

func checkRegionsKV(c *C, kv *core.KV, regions []*core.RegionInfo) {
	if kv != nil {
		for _, region := range regions {
			var meta metapb.Region
			ok, err := kv.LoadRegion(region.GetId(), &meta)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(&meta, DeepEquals, region.Region)
		}
	}
}

func checkRegions(c *C, cache *core.RegionsInfo, regions []*core.RegionInfo) {
	regionCount := make(map[uint64]int)
	leaderCount := make(map[uint64]int)
	followerCount := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.Peers {
			regionCount[peer.StoreId]++
			if peer.Id == region.Leader.Id {
				leaderCount[peer.StoreId]++
				checkRegion(c, cache.GetLeader(peer.StoreId, region.Id), region)
			} else {
				followerCount[peer.StoreId]++
				checkRegion(c, cache.GetFollower(peer.StoreId, region.Id), region)
			}
		}
	}

	c.Assert(cache.GetRegionCount(), Equals, len(regions))
	for id, count := range regionCount {
		c.Assert(cache.GetStoreRegionCount(id), Equals, count)
	}
	for id, count := range leaderCount {
		c.Assert(cache.GetStoreLeaderCount(id), Equals, count)
	}
	for id, count := range followerCount {
		c.Assert(cache.GetStoreFollowerCount(id), Equals, count)
	}

	for _, region := range cache.GetRegions() {
		checkRegion(c, region, regions[region.GetId()])
	}
	for _, region := range cache.GetMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].Region)
	}
}

var _ = Suite(&testClusterInfoSuite{})

type testClusterInfoSuite struct{}

func (s *testClusterInfoSuite) Test(c *C) {
	var tests []func(*C, *clusterInfo)
	tests = append(tests, s.testStoreHeartbeat)
	tests = append(tests, s.testRegionHeartbeat)
	tests = append(tests, s.testRegionSplitAndMerge)

	_, opt := newTestScheduleConfig()

	// Test without kv.
	{
		for _, test := range tests {
			cluster := newClusterInfo(core.NewMockIDAllocator(), opt, nil)
			test(c, cluster)
		}
	}

	// Test with kv.
	{
		for _, test := range tests {
			server, cleanup := mustRunTestServer(c)
			defer cleanup()
			cluster := newClusterInfo(server.idAlloc, opt, server.kv)
			test(c, cluster)
		}
	}
}

func (s *testClusterInfoSuite) TestLoadClusterInfo(c *C) {
	server, cleanup := mustRunTestServer(c)
	defer cleanup()

	kv := server.kv
	_, opt := newTestScheduleConfig()

	// Cluster is not bootstrapped.
	cluster, err := loadClusterInfo(server.idAlloc, kv, opt)
	c.Assert(err, IsNil)
	c.Assert(cluster, IsNil)

	// Save meta, stores and regions.
	n := 10
	meta := &metapb.Cluster{Id: 123}
	c.Assert(kv.SaveMeta(meta), IsNil)
	stores := mustSaveStores(c, kv, n)
	regions := mustSaveRegions(c, kv, n)

	cluster, err = loadClusterInfo(server.idAlloc, kv, opt)
	c.Assert(err, IsNil)
	c.Assert(cluster, NotNil)

	// Check meta, stores, and regions.
	c.Assert(cluster.getMeta(), DeepEquals, meta)
	c.Assert(cluster.getStoreCount(), Equals, n)
	for _, store := range cluster.getMetaStores() {
		c.Assert(store, DeepEquals, stores[store.GetId()])
	}
	c.Assert(cluster.getRegionCount(), Equals, n)
	for _, region := range cluster.getMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()])
	}
}

func (s *testClusterInfoSuite) testStoreHeartbeat(c *C, cache *clusterInfo) {
	n, np := uint64(3), uint64(3)
	stores := newTestStores(n)
	regions := newTestRegions(n, np)

	for _, region := range regions {
		c.Assert(cache.putRegion(region), IsNil)
	}
	c.Assert(cache.getRegionCount(), Equals, int(n))

	for i, store := range stores {
		storeStats := &pdpb.StoreStats{StoreId: store.GetId()}
		c.Assert(cache.handleStoreHeartbeat(storeStats), NotNil)

		c.Assert(cache.putStore(store), IsNil)
		c.Assert(cache.getStoreCount(), Equals, int(i+1))

		c.Assert(store.LastHeartbeatTS.IsZero(), IsTrue)

		c.Assert(cache.handleStoreHeartbeat(storeStats), IsNil)

		s := cache.GetStore(store.GetId())
		c.Assert(s.LastHeartbeatTS.IsZero(), IsFalse)
	}

	c.Assert(cache.getStoreCount(), Equals, int(n))

	// Test with kv.
	if kv := cache.kv; kv != nil {
		for _, store := range stores {
			tmp := &metapb.Store{}
			ok, err := kv.LoadStore(store.GetId(), tmp)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(tmp, DeepEquals, store.Store)
		}
	}
}

func (s *testClusterInfoSuite) testRegionHeartbeat(c *C, cache *clusterInfo) {
	n, np := uint64(3), uint64(3)

	stores := newTestStores(3)
	regions := newTestRegions(n, np)

	for _, store := range stores {
		cache.putStore(store)
	}

	for i, region := range regions {
		// region does not exist.
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.Regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		// region is the same, not updated.
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.Regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		epoch := region.Clone().GetRegionEpoch()

		// region is updated.
		region.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
		}
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.Regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		// region is stale (Version).
		stale := region.Clone()
		stale.RegionEpoch = &metapb.RegionEpoch{
			ConfVer: epoch.GetConfVer() + 1,
		}
		c.Assert(cache.handleRegionHeartbeat(stale), NotNil)
		checkRegions(c, cache.Regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		// region is updated.
		region.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
			ConfVer: epoch.GetConfVer() + 1,
		}
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.Regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		// region is stale (ConfVer).
		stale = region.Clone()
		stale.RegionEpoch = &metapb.RegionEpoch{
			Version: epoch.GetVersion() + 1,
		}
		c.Assert(cache.handleRegionHeartbeat(stale), NotNil)
		checkRegions(c, cache.Regions, regions[:i+1])
		checkRegionsKV(c, cache.kv, regions[:i+1])

		// Add a down peer.
		region.DownPeers = []*pdpb.PeerStats{
			{
				Peer:        region.Peers[rand.Intn(len(region.Peers))],
				DownSeconds: 42,
			},
		}
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.Regions, regions[:i+1])

		// Add a pending peer.
		region.PendingPeers = []*metapb.Peer{region.Peers[rand.Intn(len(region.Peers))]}
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.Regions, regions[:i+1])

		// Clear down peers.
		region.DownPeers = nil
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.Regions, regions[:i+1])

		// Clear pending peers.
		region.PendingPeers = nil
		c.Assert(cache.handleRegionHeartbeat(region), IsNil)
		checkRegions(c, cache.Regions, regions[:i+1])
	}

	regionCounts := make(map[uint64]int)
	for _, region := range regions {
		for _, peer := range region.GetPeers() {
			regionCounts[peer.GetStoreId()]++
		}
	}
	for id, count := range regionCounts {
		c.Assert(cache.getStoreRegionCount(id), Equals, count)
	}

	for _, region := range cache.getRegions() {
		checkRegion(c, region, regions[region.GetId()])
	}
	for _, region := range cache.getMetaRegions() {
		c.Assert(region, DeepEquals, regions[region.GetId()].Region)
	}

	for _, region := range regions {
		for _, store := range cache.GetRegionStores(region) {
			c.Assert(region.GetStorePeer(store.GetId()), NotNil)
		}
		for _, store := range cache.GetFollowerStores(region) {
			peer := region.GetStorePeer(store.GetId())
			c.Assert(peer.GetId(), Not(Equals), region.Leader.GetId())
		}
	}

	for _, store := range cache.Stores.GetStores() {
		c.Assert(store.LeaderCount, Equals, cache.Regions.GetStoreLeaderCount(store.GetId()))
		c.Assert(store.RegionCount, Equals, cache.Regions.GetStoreRegionCount(store.GetId()))
		c.Assert(store.LeaderSize, Equals, cache.Regions.GetStoreLeaderRegionSize(store.GetId()))
		c.Assert(store.RegionSize, Equals, cache.Regions.GetStoreRegionSize(store.GetId()))
	}

	// Test with kv.
	if kv := cache.kv; kv != nil {
		for _, region := range regions {
			tmp := &metapb.Region{}
			ok, err := kv.LoadRegion(region.GetId(), tmp)
			c.Assert(ok, IsTrue)
			c.Assert(err, IsNil)
			c.Assert(tmp, DeepEquals, region.Region)
		}
	}
}

func heartbeatRegions(c *C, cache *clusterInfo, regions []*metapb.Region) {
	// Heartbeat and check region one by one.
	for _, region := range regions {
		r := core.NewRegionInfo(region, nil)

		c.Assert(cache.handleRegionHeartbeat(r), IsNil)

		checkRegion(c, cache.GetRegion(r.GetId()), r)
		checkRegion(c, cache.searchRegion(r.StartKey), r)

		if len(r.EndKey) > 0 {
			end := r.EndKey[0]
			checkRegion(c, cache.searchRegion([]byte{end - 1}), r)
		}
	}

	// Check all regions after handling all heartbeats.
	for _, region := range regions {
		r := core.NewRegionInfo(region, nil)

		checkRegion(c, cache.GetRegion(r.GetId()), r)
		checkRegion(c, cache.searchRegion(r.StartKey), r)

		if len(r.EndKey) > 0 {
			end := r.EndKey[0]
			checkRegion(c, cache.searchRegion([]byte{end - 1}), r)
			result := cache.searchRegion([]byte{end + 1})
			c.Assert(result.GetId(), Not(Equals), r.GetId())
		}
	}
}

func (s *testClusterInfoSuite) testRegionSplitAndMerge(c *C, cache *clusterInfo) {
	regions := []*metapb.Region{
		{
			Id:          1,
			StartKey:    []byte{},
			EndKey:      []byte{},
			RegionEpoch: &metapb.RegionEpoch{},
		},
	}

	// Byte will underflow/overflow if n > 7.
	n := 7

	// Split.
	for i := 0; i < n; i++ {
		regions = core.SplitRegions(regions)
		heartbeatRegions(c, cache, regions)
	}

	// Merge.
	for i := 0; i < n; i++ {
		regions = core.MergeRegions(regions)
		heartbeatRegions(c, cache, regions)
	}

	// Split twice and merge once.
	for i := 0; i < n*2; i++ {
		if (i+1)%3 == 0 {
			regions = core.MergeRegions(regions)
		} else {
			regions = core.SplitRegions(regions)
		}
		heartbeatRegions(c, cache, regions)
	}
}

func (s *testClusterInfoSuite) TestUpdateStorePendingPeerCount(c *C) {
	_, opt := newTestScheduleConfig()
	tc := newTestClusterInfo(opt)
	stores := newTestStores(5)
	for _, s := range stores {
		tc.putStore(s)
	}
	peers := []*metapb.Peer{
		{
			Id:      2,
			StoreId: 1,
		},
		{
			Id:      3,
			StoreId: 2,
		},
		{
			Id:      3,
			StoreId: 3,
		},
		{
			Id:      4,
			StoreId: 4,
		},
	}
	origin := &core.RegionInfo{
		Region:       &metapb.Region{Id: 1, Peers: peers[:3]},
		Leader:       peers[0],
		PendingPeers: peers[1:3],
	}
	tc.handleRegionHeartbeat(origin)
	checkPendingPeerCount([]int{0, 1, 1, 0}, tc.clusterInfo, c)
	newRegion := &core.RegionInfo{
		Region:       &metapb.Region{Id: 1, Peers: peers[1:]},
		Leader:       peers[1],
		PendingPeers: peers[3:4],
	}
	tc.handleRegionHeartbeat(newRegion)
	checkPendingPeerCount([]int{0, 0, 0, 1}, tc.clusterInfo, c)
}

func checkPendingPeerCount(expect []int, cache *clusterInfo, c *C) {
	for i, e := range expect {
		s := cache.Stores.GetStore(uint64(i + 1))
		c.Assert(s.PendingPeerCount, Equals, e)
	}
}

var _ = Suite(&testClusterUtilSuite{})

type testClusterUtilSuite struct{}

func (s *testClusterUtilSuite) TestCheckStaleRegion(c *C) {
	// (0, 0) v.s. (0, 0)
	region := core.NewRegion([]byte{}, []byte{})
	origin := core.NewRegion([]byte{}, []byte{})
	c.Assert(checkStaleRegion(region, origin), IsNil)
	c.Assert(checkStaleRegion(origin, region), IsNil)

	// (1, 0) v.s. (0, 0)
	region.RegionEpoch.Version++
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)

	// (1, 1) v.s. (0, 0)
	region.RegionEpoch.ConfVer++
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)

	// (0, 1) v.s. (0, 0)
	region.RegionEpoch.Version--
	c.Assert(checkStaleRegion(origin, region), IsNil)
	c.Assert(checkStaleRegion(region, origin), NotNil)
}

func mustSaveStores(c *C, kv *core.KV, n int) []*metapb.Store {
	stores := make([]*metapb.Store, 0, n)
	for i := 0; i < n; i++ {
		store := &metapb.Store{Id: uint64(i)}
		stores = append(stores, store)
	}

	for _, store := range stores {
		c.Assert(kv.SaveStore(store), IsNil)
	}

	return stores
}

func mustSaveRegions(c *C, kv *core.KV, n int) []*metapb.Region {
	regions := make([]*metapb.Region, 0, n)
	for i := 0; i < n; i++ {
		region := &metapb.Region{Id: uint64(i)}
		regions = append(regions, region)
	}

	for _, region := range regions {
		c.Assert(kv.SaveRegion(region), IsNil)
	}

	return regions
}
