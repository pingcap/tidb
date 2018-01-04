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
	"bytes"
	"context"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/testutil"
)

var _ = Suite(&testClusterWorkerSuite{})

type mockRaftStore struct {
	sync.Mutex

	s *testClusterWorkerSuite

	listener net.Listener

	store *metapb.Store

	// peerID -> Peer
	peers map[uint64]*metapb.Peer
}

func (s *mockRaftStore) addPeer(c *C, peer *metapb.Peer) {
	s.Lock()
	defer s.Unlock()

	c.Assert(s.peers, Not(HasKey), peer.GetId())
	s.peers[peer.GetId()] = peer
}

func (s *mockRaftStore) removePeer(c *C, peer *metapb.Peer) {
	s.Lock()
	defer s.Unlock()

	c.Assert(s.peers, HasKey, peer.GetId())
	delete(s.peers, peer.GetId())
}

func addRegionPeer(c *C, region *metapb.Region, peer *metapb.Peer) {
	for _, p := range region.Peers {
		if p.GetId() == peer.GetId() || p.GetStoreId() == peer.GetStoreId() {
			c.Fatalf("new peer %v conflict with existed peer %v", peer, p)
		}
	}
	region.Peers = append(region.Peers, peer)
}

func removeRegionPeer(c *C, region *metapb.Region, peer *metapb.Peer) {
	peers := make([]*metapb.Peer, 0, len(region.Peers))
	for _, p := range region.Peers {
		if p.GetId() == peer.GetId() {
			c.Assert(p.GetStoreId(), Equals, peer.GetStoreId())
			continue
		}
		peers = append(peers, p)
	}
	c.Assert(len(region.Peers), Not(Equals), len(peers))
	region.Peers = peers
}

type testClusterWorkerSuite struct {
	testClusterBaseSuite

	clusterID uint64

	storeLock sync.Mutex
	// storeID -> mockRaftStore
	stores map[uint64]*mockRaftStore

	regionLeaderLock sync.Mutex
	// regionID -> Peer
	regionLeaders    map[uint64]metapb.Peer
	heartbeatClients map[uint64]*regionHeartbeatClient
}

type regionHeartbeatClient struct {
	stream pdpb.PD_RegionHeartbeatClient
	respCh chan *pdpb.RegionHeartbeatResponse
}

func newRegionheartbeatClient(c *C, grpcClient pdpb.PDClient) *regionHeartbeatClient {
	stream, err := grpcClient.RegionHeartbeat(context.Background())
	c.Assert(err, IsNil)
	ch := make(chan *pdpb.RegionHeartbeatResponse)
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				return
			}
			ch <- res
		}
	}()
	return &regionHeartbeatClient{
		stream: stream,
		respCh: ch,
	}
}

func (c *regionHeartbeatClient) close() {
	c.stream.CloseSend()
}

func (c *regionHeartbeatClient) SendRecv(msg *pdpb.RegionHeartbeatRequest, timeout time.Duration) *pdpb.RegionHeartbeatResponse {
	c.stream.Send(msg)
	select {
	case <-time.After(timeout):
		return nil
	case res := <-c.respCh:
		return res
	}
}

func (s *testClusterWorkerSuite) clearRegionLeader(c *C, regionID uint64) {
	s.regionLeaderLock.Lock()
	defer s.regionLeaderLock.Unlock()

	delete(s.regionLeaders, regionID)
}

func (s *testClusterWorkerSuite) chooseRegionLeader(c *C, region *metapb.Region) *metapb.Peer {
	// Randomly select a peer in the region as the leader.
	peer := region.Peers[rand.Intn(len(region.Peers))]

	s.regionLeaderLock.Lock()
	defer s.regionLeaderLock.Unlock()

	s.regionLeaders[region.GetId()] = *peer
	return peer
}

func (s *testClusterWorkerSuite) bootstrap(c *C) *mockRaftStore {
	req := s.newBootstrapRequest(c, s.clusterID, "127.0.0.1:0")
	store := req.Store
	region := req.Region

	_, err := s.svr.bootstrapCluster(req)
	c.Assert(err, IsNil)

	raftStore := s.newMockRaftStore(c, store)
	c.Assert(region.Peers, HasLen, 1)
	raftStore.addPeer(c, region.Peers[0])
	return raftStore
}

func (s *testClusterWorkerSuite) newMockRaftStore(c *C, metaStore *metapb.Store) *mockRaftStore {
	if metaStore == nil {
		metaStore = s.newStore(c, 0, "127.0.0.1:0")
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, IsNil)

	addr := l.Addr().String()
	metaStore.Address = addr
	store := &mockRaftStore{
		s:        s,
		listener: l,
		store:    metaStore,
		peers:    make(map[uint64]*metapb.Peer),
	}

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	err = cluster.putStore(metaStore)
	c.Assert(err, IsNil)

	stats := &pdpb.StoreStats{
		StoreId:            metaStore.GetId(),
		Capacity:           100,
		Available:          50,
		SendingSnapCount:   1,
		ReceivingSnapCount: 1,
	}

	c.Assert(cluster.cachedCluster.handleStoreHeartbeat(stats), IsNil)

	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	s.stores[metaStore.GetId()] = store
	return store
}

func (s *testClusterWorkerSuite) SetUpTest(c *C) {
	s.stores = make(map[uint64]*mockRaftStore)

	s.svr, s.cleanup = newTestServer(c)
	s.svr.cfg.nextRetryDelay = 50 * time.Millisecond
	s.svr.scheduleOpt.SetMaxReplicas(1)

	err := s.svr.Run()
	c.Assert(err, IsNil)

	s.client = s.svr.client
	s.clusterID = s.svr.clusterID

	s.regionLeaders = make(map[uint64]metapb.Peer)

	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())

	// Build raft cluster with 5 stores.
	s.bootstrap(c)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)
	s.newMockRaftStore(c, nil)

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	err = cluster.putConfig(&metapb.Cluster{
		Id:           s.clusterID,
		MaxPeerCount: 5,
	})
	c.Assert(err, IsNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)

	s.heartbeatClients = make(map[uint64]*regionHeartbeatClient)
	for _, store := range stores {
		s.heartbeatClients[store.GetId()] = newRegionheartbeatClient(c, s.grpcPDClient)
	}
}

func (s *testClusterWorkerSuite) runHeartbeatReceiver(c *C) (pdpb.PD_RegionHeartbeatClient, chan *pdpb.RegionHeartbeatResponse) {
	client, err := s.grpcPDClient.RegionHeartbeat(context.Background())
	c.Assert(err, IsNil)
	ch := make(chan *pdpb.RegionHeartbeatResponse)
	go func() {
		for {
			res, err := client.Recv()
			if err != nil {
				return
			}
			ch <- res
		}
	}()
	return client, ch
}

func (s *testClusterWorkerSuite) TearDownTest(c *C) {
	s.cleanup()
	for _, client := range s.heartbeatClients {
		client.close()
	}
}

func (s *testClusterWorkerSuite) checkRegionPeerCount(c *C, regionKey []byte, expectCount int) bool {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	region, _ := cluster.GetRegionByKey(regionKey)
	return len(region.Peers) == expectCount
}

func (s *testClusterWorkerSuite) onChangePeerRes(c *C, res *pdpb.ChangePeer, region *metapb.Region) {
	if res == nil {
		return
	}
	peer := res.GetPeer()
	c.Assert(peer, NotNil)
	store, ok := s.stores[peer.GetStoreId()]
	c.Assert(ok, IsTrue)
	switch res.GetChangeType() {
	case eraftpb.ConfChangeType_AddNode:
		if _, ok := store.peers[peer.GetId()]; ok {
			return
		}
		store.addPeer(c, peer)
		addRegionPeer(c, region, peer)
	case eraftpb.ConfChangeType_RemoveNode:
		if _, ok := store.peers[peer.GetId()]; !ok {
			return
		}
		store.removePeer(c, peer)
		removeRegionPeer(c, region, peer)
	default:
		c.Fatalf("invalid conf change type, %v", res.GetChangeType())
	}
	// Increase conVer.
	region.RegionEpoch.ConfVer = region.GetRegionEpoch().GetConfVer() + 1
}

func (s *testClusterWorkerSuite) askSplit(c *C, r *metapb.Region) (uint64, []uint64) {
	req := &pdpb.AskSplitRequest{
		Header: newRequestHeader(s.clusterID),
		Region: r,
	}
	askResp, err := s.grpcPDClient.AskSplit(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(askResp.GetNewRegionId(), Not(Equals), 0)
	c.Assert(askResp.GetNewPeerIds(), HasLen, len(r.Peers))
	return askResp.GetNewRegionId(), askResp.GetNewPeerIds()
}

func updateRegionRange(r *metapb.Region, start, end []byte) {
	r.StartKey = start
	r.EndKey = end
	r.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: r.GetRegionEpoch().GetConfVer(),
		Version: r.GetRegionEpoch().GetVersion() + 1,
	}
}

func splitRegion(c *C, old *metapb.Region, splitKey []byte, newRegionID uint64, newPeerIDs []uint64) *metapb.Region {
	c.Assert(len(old.Peers), Equals, len(newPeerIDs))
	peers := make([]*metapb.Peer, 0, len(old.Peers))
	for i, peer := range old.Peers {
		peers = append(peers, &metapb.Peer{
			Id:      newPeerIDs[i],
			StoreId: peer.GetStoreId(),
		})
	}
	newRegion := &metapb.Region{
		Id:          newRegionID,
		RegionEpoch: proto.Clone(old.RegionEpoch).(*metapb.RegionEpoch),
		Peers:       peers,
	}
	updateRegionRange(newRegion, old.StartKey, splitKey)
	updateRegionRange(old, splitKey, old.EndKey)
	return newRegion
}

func (s *testClusterWorkerSuite) heartbeatRegion(c *C, clusterID uint64, region *metapb.Region, leader *metapb.Peer) *pdpb.RegionHeartbeatResponse {
	req := &pdpb.RegionHeartbeatRequest{
		Header: newRequestHeader(clusterID),
		Leader: leader,
		Region: region,
	}
	heartbeatClient := s.heartbeatClients[leader.GetStoreId()]
	return heartbeatClient.SendRecv(req, time.Millisecond*10)
}

func (s *testClusterWorkerSuite) heartbeatStore(c *C, stats *pdpb.StoreStats) *pdpb.StoreHeartbeatResponse {
	req := &pdpb.StoreHeartbeatRequest{
		Header: newRequestHeader(s.clusterID),
		Stats:  stats,
	}
	resp, err := s.grpcPDClient.StoreHeartbeat(context.Background(), req)
	c.Assert(err, IsNil)
	return resp
}

func (s *testClusterWorkerSuite) reportSplit(c *C, left *metapb.Region, right *metapb.Region) *pdpb.ReportSplitResponse {
	req := &pdpb.ReportSplitRequest{
		Header: newRequestHeader(s.clusterID),
		Left:   left,
		Right:  right,
	}
	resp, err := s.grpcPDClient.ReportSplit(context.Background(), req)
	c.Assert(err, IsNil)
	return resp
}

func mustGetRegion(c *C, cluster *RaftCluster, key []byte, expect *metapb.Region) {
	r, _ := cluster.GetRegionByKey(key)
	c.Assert(r, DeepEquals, expect)
}

func (s *testClusterWorkerSuite) checkSearchRegions(cluster *RaftCluster, keys ...string) func(c *C) bool {
	return func(c *C) bool {
		cluster.cachedCluster.RLock()
		defer cluster.cachedCluster.RUnlock()

		cacheRegions := cluster.cachedCluster.Regions
		if cacheRegions.TreeLength() != len(keys)/2 {
			c.Logf("region length not match, expect %v, got %v", len(keys)/2, cacheRegions.TreeLength())
			return false
		}

		for i := 0; i < len(keys); i += 2 {
			start, end := []byte(keys[i]), []byte(keys[i+1])
			region := cacheRegions.SearchRegion(start)
			if region == nil {
				c.Logf("region not found for key: %q", start)
				return false
			}
			if bytes.Compare(region.StartKey, start) != 0 || bytes.Compare(region.EndKey, end) != 0 {
				c.Logf("keyrange not match, expect: [%q, %q], got [%q, %q]", start, end, region.StartKey, region.EndKey)
				return false
			}
		}
		return true
	}
}

func (s *testClusterWorkerSuite) TestHeartbeatSplit(c *C) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	r1, _ := cluster.GetRegionByKey([]byte("a"))
	// 1: [nil, nil)
	testutil.WaitUntil(c, s.checkSearchRegions(cluster, "", ""))

	// split 1 to 2: [nil, m) 1: [m, nil), sync 2 first
	r2ID, r2PeerIDs := s.askSplit(c, r1)
	r2 := splitRegion(c, r1, []byte("m"), r2ID, r2PeerIDs)
	leaderPeer2 := s.chooseRegionLeader(c, r2)
	s.heartbeatRegion(c, s.clusterID, r2, leaderPeer2)
	testutil.WaitUntil(c, s.checkSearchRegions(cluster, "", "m"))
	mustGetRegion(c, cluster, []byte("a"), r2)
	// [m, nil) is missing before r1's heartbeat.
	mustGetRegion(c, cluster, []byte("z"), nil)

	leaderPeer1 := s.chooseRegionLeader(c, r1)
	s.heartbeatRegion(c, s.clusterID, r1, leaderPeer1)
	testutil.WaitUntil(c, s.checkSearchRegions(cluster, "", "m", "m", ""))
	mustGetRegion(c, cluster, []byte("z"), r1)

	// split 1 to 3: [m, q) 1: [q, nil), sync 1 first
	r3ID, r3PeerIDs := s.askSplit(c, r1)
	r3 := splitRegion(c, r1, []byte("q"), r3ID, r3PeerIDs)
	s.heartbeatRegion(c, s.clusterID, r1, leaderPeer1)
	testutil.WaitUntil(c, s.checkSearchRegions(cluster, "", "m", "q", ""))
	mustGetRegion(c, cluster, []byte("z"), r1)
	mustGetRegion(c, cluster, []byte("a"), r2)
	// [m, q) is missing before r3's heartbeat.
	mustGetRegion(c, cluster, []byte("n"), nil)
	leaderPeer3 := s.chooseRegionLeader(c, r3)
	s.heartbeatRegion(c, s.clusterID, r3, leaderPeer3)
	testutil.WaitUntil(c, s.checkSearchRegions(cluster, "", "m", "m", "q", "q", ""))
	mustGetRegion(c, cluster, []byte("n"), r3)
}

func (s *testClusterWorkerSuite) TestHeartbeatSplit2(c *C) {
	s.svr.scheduleOpt.SetMaxReplicas(5)

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	r1, _ := cluster.GetRegionByKey([]byte("a"))
	leaderPeer := s.chooseRegionLeader(c, r1)

	// Set MaxPeerCount to 10.
	meta := cluster.GetConfig()
	meta.MaxPeerCount = 10
	err := cluster.putConfig(meta)
	c.Assert(err, IsNil)

	// Add Peers util all stores are used up.
	testutil.WaitUntil(c, func(c *C) bool {
		s.waitAddNode(c, r1, leaderPeer)
		return len(r1.Peers) == len(s.stores)
	})

	// Split.
	r2ID, r2PeerIDs := s.askSplit(c, r1)
	r2 := splitRegion(c, r1, []byte("m"), r2ID, r2PeerIDs)
	leaderPeer2 := s.chooseRegionLeader(c, r2)
	s.heartbeatRegion(c, s.clusterID, r2, leaderPeer2)
	testutil.WaitUntil(c, s.checkSearchRegions(cluster, "", "m"))
}

func (s *testClusterWorkerSuite) waitAddNode(c *C, r *metapb.Region, leader *metapb.Peer) {
	testutil.WaitUntil(c, func(c *C) bool {
		res := s.heartbeatRegion(c, s.clusterID, r, leader)
		if res == nil {
			c.Log("no response")
			return false
		}
		if res.GetChangePeer() == nil || res.GetChangePeer().GetChangeType() != eraftpb.ConfChangeType_AddNode {
			c.Log("response is not AddNode")
			return false
		}
		s.onChangePeerRes(c, res.GetChangePeer(), r)
		return true
	})
}

func (s *testClusterWorkerSuite) TestHeartbeatChangePeer(c *C) {
	s.svr.scheduleOpt.SetMaxReplicas(5)

	opt := s.svr.scheduleOpt

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	meta := cluster.GetConfig()
	c.Assert(meta.GetMaxPeerCount(), Equals, uint32(5))

	// There is only one region now, directly use it for test.
	regionKey := []byte("a")
	region, _ := cluster.GetRegionByKey(regionKey)
	c.Assert(region.Peers, HasLen, 1)

	//	leaderPd := mustGetLeader(c, s.client, s.svr.getLeaderPath())

	leaderPeer := s.chooseRegionLeader(c, region)
	c.Logf("[leaderPeer]:%v, [region]:%v", leaderPeer, region)

	// Add 4 peers.
	for i := 1; i <= 4; i++ {
		s.waitAddNode(c, region, leaderPeer)
		testutil.WaitUntil(c, func(c *C) bool {
			// update to server
			s.heartbeatRegion(c, s.clusterID, region, leaderPeer)
			return s.checkRegionPeerCount(c, regionKey, i+1)
		})
	}

	// Wait util no more commands.
	testutil.WaitUntil(c, func(c *C) bool {
		res := s.heartbeatRegion(c, s.clusterID, region, leaderPeer)
		if res == nil {
			return true
		}
		if transferLeader := res.GetTransferLeader(); transferLeader != nil {
			c.Log("transfer leader")
			leaderPeer = transferLeader.GetPeer()
			return false
		}
		if res.GetChangePeer() != nil {
			c.Fatal("should be no more ChangePeer commands.")
		}
		return false
	})

	opt.SetMaxReplicas(3)

	// Remove 2 peers
	for peerCount := 5; peerCount > 3; peerCount-- {
		testutil.WaitUntil(c, func(c *C) bool {
			res := s.heartbeatRegion(c, s.clusterID, region, leaderPeer)
			if res == nil {
				c.Log("no response")
				return false
			}
			if transferLeader := res.GetTransferLeader(); transferLeader != nil {
				c.Log("transfer leader")
				leaderPeer = transferLeader.GetPeer()
				return false
			}
			if res.GetChangePeer() == nil || res.GetChangePeer().GetChangeType() != eraftpb.ConfChangeType_RemoveNode {
				c.Log("response is not RemoveNode")
				return false
			}
			s.onChangePeerRes(c, res.GetChangePeer(), region)
			return true
		})
		testutil.WaitUntil(c, func(c *C) bool {
			// update to server
			s.heartbeatRegion(c, s.clusterID, region, leaderPeer)
			return s.checkRegionPeerCount(c, regionKey, peerCount-1)
		})
	}
}

func (s *testClusterWorkerSuite) TestHeartbeatSplitAddPeer(c *C) {
	s.svr.scheduleOpt.SetMaxReplicas(2)
	// Stop schedulers.
	scheduleCfg := s.svr.GetScheduleConfig()
	scheduleCfg.LeaderScheduleLimit = 0
	scheduleCfg.RegionScheduleLimit = 0
	s.svr.SetScheduleConfig(*scheduleCfg)

	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	r1, _ := cluster.GetRegionByKey([]byte("a"))
	leaderPeer1 := s.chooseRegionLeader(c, r1)

	// Wait for AddPeer command.
	s.waitAddNode(c, r1, leaderPeer1)
	// Split 1 to 2: [nil, m) 1: [m, nil).
	r2ID, r2PeerIDs := s.askSplit(c, r1)
	r2 := splitRegion(c, r1, []byte("m"), r2ID, r2PeerIDs)

	// Sync r1 with both ConfVer and Version updated.
	s.heartbeatRegion(c, s.clusterID, r1, leaderPeer1)
	testutil.WaitUntil(c, s.checkSearchRegions(cluster, "m", ""))
	mustGetRegion(c, cluster, []byte("z"), r1)
	mustGetRegion(c, cluster, []byte("a"), nil)
	// Sync r2.
	leaderPeer2 := s.chooseRegionLeader(c, r2)
	s.heartbeatRegion(c, s.clusterID, r2, leaderPeer2)
	testutil.WaitUntil(c, s.checkSearchRegions(cluster, "", "m", "m", ""))
}

func (s *testClusterWorkerSuite) TestStoreHeartbeat(c *C) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)

	// Mock a store stats.
	storeID := stores[0].GetId()
	stats := &pdpb.StoreStats{
		StoreId:     storeID,
		Capacity:    100,
		Available:   50,
		RegionCount: 1,
	}

	resp := s.heartbeatStore(c, stats)
	c.Assert(resp, NotNil)

	store := cluster.cachedCluster.GetStore(storeID)
	c.Assert(stats, DeepEquals, store.Stats)
}

func (s *testClusterWorkerSuite) TestReportSplit(c *C) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	stores := cluster.GetStores()
	c.Assert(stores, HasLen, 5)

	// Mock a report split request.
	peer := s.newPeer(c, 999, 0)
	left := s.newRegion(c, 2, []byte("aaa"), []byte("bbb"), []*metapb.Peer{peer}, nil)
	right := s.newRegion(c, 1, []byte("bbb"), []byte("ccc"), []*metapb.Peer{peer}, nil)

	resp := s.reportSplit(c, left, right)
	c.Assert(resp, NotNil)
}
