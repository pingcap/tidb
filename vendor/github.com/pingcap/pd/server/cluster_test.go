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
	"context"
	"strings"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/core"
	"google.golang.org/grpc"
)

const (
	initEpochVersion uint64 = 1
	initEpochConfVer uint64 = 1
)

var _ = Suite(&testClusterSuite{})

type testClusterBaseSuite struct {
	client       *clientv3.Client
	svr          *Server
	cleanup      cleanUpFunc
	grpcPDClient pdpb.PDClient
}

type testClusterSuite struct {
	testClusterBaseSuite
}

func (s *testClusterSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = newTestServer(c)
	s.client = s.svr.client
	err := s.svr.Run()
	c.Assert(err, IsNil)
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())
}

func (s *testClusterSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func mustNewGrpcClient(c *C, addr string) pdpb.PDClient {
	conn, err := grpc.Dial(strings.TrimLeft(addr, "http://"), grpc.WithInsecure())

	c.Assert(err, IsNil)
	return pdpb.NewPDClient(conn)
}

func (s *testClusterBaseSuite) allocID(c *C) uint64 {
	id, err := s.svr.idAlloc.Alloc()
	c.Assert(err, IsNil)
	return id
}

func newRequestHeader(clusterID uint64) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

func (s *testClusterBaseSuite) newPeer(c *C, storeID uint64, peerID uint64) *metapb.Peer {
	c.Assert(storeID, Greater, uint64(0))

	if peerID == 0 {
		peerID = s.allocID(c)
	}

	return &metapb.Peer{
		StoreId: storeID,
		Id:      peerID,
	}
}

func (s *testClusterBaseSuite) newStore(c *C, storeID uint64, addr string) *metapb.Store {
	if storeID == 0 {
		storeID = s.allocID(c)
	}

	return &metapb.Store{
		Id:      storeID,
		Address: addr,
	}
}

func (s *testClusterBaseSuite) newRegion(c *C, regionID uint64, startKey []byte,
	endKey []byte, peers []*metapb.Peer, epoch *metapb.RegionEpoch) *metapb.Region {
	if regionID == 0 {
		regionID = s.allocID(c)
	}

	if epoch == nil {
		epoch = &metapb.RegionEpoch{
			ConfVer: initEpochConfVer,
			Version: initEpochVersion,
		}
	}

	for _, peer := range peers {
		peerID := peer.GetId()
		c.Assert(peerID, Greater, uint64(0))
	}

	return &metapb.Region{
		Id:          regionID,
		StartKey:    startKey,
		EndKey:      endKey,
		RegionEpoch: epoch,
		Peers:       peers,
	}
}

func (s *testClusterSuite) TestBootstrap(c *C) {

	clusterID := s.svr.clusterID

	// IsBootstrapped returns false.
	req := s.newIsBootstrapRequest(clusterID)
	resp, err := s.grpcPDClient.IsBootstrapped(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	c.Assert(resp.GetBootstrapped(), IsFalse)

	// Bootstrap the cluster.
	storeAddr := "127.0.0.1:0"
	s.bootstrapCluster(c, clusterID, storeAddr)

	// IsBootstrapped returns true.
	req = s.newIsBootstrapRequest(clusterID)
	resp, err = s.grpcPDClient.IsBootstrapped(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetBootstrapped(), IsTrue)

	// check bootstrapped error.
	reqBoot := s.newBootstrapRequest(c, clusterID, storeAddr)
	respBoot, err := s.grpcPDClient.Bootstrap(context.Background(), reqBoot)
	c.Assert(err, IsNil)
	c.Assert(respBoot.GetHeader().GetError(), NotNil)
	c.Assert(respBoot.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_ALREADY_BOOTSTRAPPED)
}

func (s *testClusterBaseSuite) newIsBootstrapRequest(clusterID uint64) *pdpb.IsBootstrappedRequest {
	req := &pdpb.IsBootstrappedRequest{
		Header: newRequestHeader(clusterID),
	}

	return req
}

func (s *testClusterBaseSuite) newBootstrapRequest(c *C, clusterID uint64, storeAddr string) *pdpb.BootstrapRequest {
	store := s.newStore(c, 0, storeAddr)
	peer := s.newPeer(c, store.GetId(), 0)
	region := s.newRegion(c, 0, []byte{}, []byte{}, []*metapb.Peer{peer}, nil)

	req := &pdpb.BootstrapRequest{
		Header: newRequestHeader(clusterID),
		Store:  store,
		Region: region,
	}

	return req
}

// helper function to check and bootstrap.
func (s *testClusterBaseSuite) bootstrapCluster(c *C, clusterID uint64, storeAddr string) {
	req := s.newBootstrapRequest(c, clusterID, storeAddr)
	_, err := s.grpcPDClient.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
}

func (s *testClusterBaseSuite) tryBootstrapCluster(c *C, grpcPDClient pdpb.PDClient, clusterID uint64, storeAddr string) {
	req := s.newBootstrapRequest(c, clusterID, storeAddr)
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	if err != nil {
		c.Assert(resp, NotNil)
	}
}

func (s *testClusterBaseSuite) getStore(c *C, clusterID uint64, storeID uint64) *metapb.Store {
	req := &pdpb.GetStoreRequest{
		Header:  newRequestHeader(clusterID),
		StoreId: storeID,
	}
	resp, err := s.grpcPDClient.GetStore(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetStore().GetId(), Equals, uint64(storeID))

	return resp.GetStore()
}

func (s *testClusterBaseSuite) getRegion(c *C, clusterID uint64, regionKey []byte) *metapb.Region {
	req := &pdpb.GetRegionRequest{
		Header:    newRequestHeader(clusterID),
		RegionKey: regionKey,
	}

	resp, err := s.grpcPDClient.GetRegion(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetRegion(), NotNil)

	return resp.GetRegion()
}

func (s *testClusterBaseSuite) getRegionByID(c *C, clusterID uint64, regionID uint64) *metapb.Region {
	req := &pdpb.GetRegionByIDRequest{
		Header:   newRequestHeader(clusterID),
		RegionId: regionID,
	}

	resp, err := s.grpcPDClient.GetRegionByID(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetRegion(), NotNil)

	return resp.GetRegion()
}

func (s *testClusterBaseSuite) getRaftCluster(c *C) *RaftCluster {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	return cluster
}

func (s *testClusterBaseSuite) getClusterConfig(c *C, clusterID uint64) *metapb.Cluster {
	req := &pdpb.GetClusterConfigRequest{
		Header: newRequestHeader(clusterID),
	}

	resp, err := s.grpcPDClient.GetClusterConfig(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetCluster(), NotNil)

	return resp.GetCluster()
}

func (s *testClusterSuite) TestGetPutConfig(c *C) {
	clusterID := s.svr.clusterID

	storeAddr := "127.0.0.1:0"
	s.tryBootstrapCluster(c, s.grpcPDClient, clusterID, storeAddr)

	// Get region.
	region := s.getRegion(c, clusterID, []byte("abc"))
	c.Assert(region.GetPeers(), HasLen, 1)
	peer := region.GetPeers()[0]

	// Get region by id.
	regionByID := s.getRegionByID(c, clusterID, region.GetId())
	c.Assert(region, DeepEquals, regionByID)

	// Get store.
	storeID := peer.GetStoreId()
	store := s.getStore(c, clusterID, storeID)
	c.Assert(store.GetAddress(), Equals, storeAddr)

	// Update store.
	store.Address = "127.0.0.1:1"
	s.testPutStore(c, clusterID, store)

	// Remove store.
	s.testRemoveStore(c, clusterID, store)

	// Update cluster config.
	req := &pdpb.PutClusterConfigRequest{
		Header: newRequestHeader(clusterID),
		Cluster: &metapb.Cluster{
			Id:           clusterID,
			MaxPeerCount: 5,
		},
	}
	resp, err := s.grpcPDClient.PutClusterConfig(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp, NotNil)
	meta := s.getClusterConfig(c, clusterID)
	c.Assert(meta.GetMaxPeerCount(), Equals, uint32(5))
}

func putStore(c *C, grpcPDClient pdpb.PDClient, clusterID uint64, store *metapb.Store) (*pdpb.PutStoreResponse, error) {
	req := &pdpb.PutStoreRequest{
		Header: newRequestHeader(clusterID),
		Store:  store,
	}
	resp, err := grpcPDClient.PutStore(context.Background(), req)
	return resp, err
}

func (s *testClusterSuite) testPutStore(c *C, clusterID uint64, store *metapb.Store) {
	// Update store.
	_, err := putStore(c, s.grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)
	updatedStore := s.getStore(c, clusterID, store.GetId())
	c.Assert(updatedStore, DeepEquals, store)

	// Update store again.
	_, err = putStore(c, s.grpcPDClient, clusterID, store)
	c.Assert(err, IsNil)

	// Put new store with a duplicated address when old store is up will fail.
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, store.GetAddress()))
	c.Assert(err, NotNil)

	// Put new store with a duplicated address when old store is offline will fail.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, store.GetAddress()))
	c.Assert(err, NotNil)

	// Put new store with a duplicated address when old store is tombstone is OK.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Tombstone)
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, store.GetAddress()))
	c.Assert(err, IsNil)

	// Put a new store.
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, 0, "127.0.0.1:12345"))
	c.Assert(err, IsNil)

	// Put an existed store with duplicated address with other old stores.
	s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
	_, err = putStore(c, s.grpcPDClient, clusterID, s.newStore(c, store.GetId(), "127.0.0.1:12345"))
	c.Assert(err, NotNil)
}

func (s *testClusterSuite) resetStoreState(c *C, storeID uint64, state metapb.StoreState) {
	cluster := s.svr.GetRaftCluster().cachedCluster
	c.Assert(cluster, NotNil)
	store := cluster.GetStore(storeID)
	c.Assert(store, NotNil)
	store.State = state
	cluster.putStore(store)
}

func (s *testClusterSuite) testRemoveStore(c *C, clusterID uint64, store *metapb.Store) {
	cluster := s.getRaftCluster(c)

	// When store is up:
	{
		// Case 1: RemoveStore should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err := cluster.RemoveStore(store.GetId())
		c.Assert(err, IsNil)
		removedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(removedStore.GetState(), Equals, metapb.StoreState_Offline)
		// Case 2: BuryStore w/ force should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err = cluster.BuryStore(store.GetId(), true)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(buriedStore.GetState(), Equals, metapb.StoreState_Tombstone)
		// Case 3: BuryStore w/o force should fail.
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Up)
		err = cluster.BuryStore(store.GetId(), false)
		c.Assert(err, NotNil)
	}

	// When store is offline:
	{
		// Case 1: RemoveStore should be OK;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
		err := cluster.RemoveStore(store.GetId())
		c.Assert(err, IsNil)
		removedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(removedStore.GetState(), Equals, metapb.StoreState_Offline)
		// Case 2: BuryStore w/ or w/o force should be OK.
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Offline)
		err = cluster.BuryStore(store.GetId(), false)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(buriedStore.GetState(), Equals, metapb.StoreState_Tombstone)
	}

	// When store is tombstone:
	{
		// Case 1: RemoveStore should should fail;
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Tombstone)
		err := cluster.RemoveStore(store.GetId())
		c.Assert(err, NotNil)
		// Case 2: BuryStore w/ or w/o force should be OK.
		s.resetStoreState(c, store.GetId(), metapb.StoreState_Tombstone)
		err = cluster.BuryStore(store.GetId(), false)
		c.Assert(err, IsNil)
		buriedStore := s.getStore(c, clusterID, store.GetId())
		c.Assert(buriedStore.GetState(), Equals, metapb.StoreState_Tombstone)
	}

	{
		// Put after removed should return tombstone error.
		resp, err := putStore(c, s.grpcPDClient, clusterID, store)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_STORE_TOMBSTONE)
	}
	{
		// Update after removed should return tombstone error.
		req := &pdpb.StoreHeartbeatRequest{
			Header: newRequestHeader(clusterID),
			Stats:  &pdpb.StoreStats{StoreId: store.GetId()},
		}
		resp, err := s.grpcPDClient.StoreHeartbeat(context.Background(), req)
		c.Assert(err, IsNil)
		c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_STORE_TOMBSTONE)
	}
}

func (s *testClusterSuite) testCheckStores(c *C, clusterID uint64) {
	cluster := s.svr.GetRaftCluster()
	c.Assert(cluster, NotNil)

	store := s.newStore(c, 0, "127.0.0.1:11111")
	putStore(c, s.grpcPDClient, clusterID, store)

	// store is up w/o region peers will not be buried.
	cluster.checkStores()
	tmpStore := s.getStore(c, clusterID, store.GetId())
	c.Assert(tmpStore.GetState(), Equals, metapb.StoreState_Up)

	// Add a region peer to store.
	leader := &metapb.Peer{StoreId: store.GetId()}
	region := s.newRegion(c, 0, []byte{'a'}, []byte{'b'}, []*metapb.Peer{leader}, nil)
	regionInfo := core.NewRegionInfo(region, leader)
	err := cluster.HandleRegionHeartbeat(regionInfo)
	c.Assert(err, IsNil)
	c.Assert(cluster.cachedCluster.getStoreRegionCount(store.GetId()), Equals, 1)

	// store is up w/ region peers will not be buried.
	cluster.checkStores()
	tmpStore = s.getStore(c, clusterID, store.GetId())
	c.Assert(tmpStore.GetState(), Equals, metapb.StoreState_Up)

	err = cluster.RemoveStore(store.GetId())
	c.Assert(err, IsNil)
	removedStore := s.getStore(c, clusterID, store.GetId())
	c.Assert(removedStore.GetState(), Equals, metapb.StoreState_Offline)

	// store is offline w/ region peers will not be buried.
	cluster.checkStores()
	tmpStore = s.getStore(c, clusterID, store.GetId())
	c.Assert(tmpStore.GetState(), Equals, metapb.StoreState_Up)

	// Clear store's region peers.
	leader.StoreId = 0
	region.Peers = []*metapb.Peer{leader}
	err = cluster.HandleRegionHeartbeat(regionInfo)
	c.Assert(err, IsNil)
	c.Assert(cluster.cachedCluster.getStoreRegionCount(store.GetId()), Equals, 0)

	// store is offline w/o region peers will be buried.
	cluster.checkStores()
	tmpStore = s.getStore(c, clusterID, store.GetId())
	c.Assert(tmpStore.GetState(), Equals, metapb.StoreState_Tombstone)
}

// Make sure PD will not panic if it start and stop again and again.
func (s *testClusterSuite) TestClosedChannel(c *C) {
	svr, cleanup := newTestServer(c)
	defer cleanup()
	err := svr.Run()
	c.Assert(err, IsNil)

	clusterID := svr.clusterID
	storeAddr := "127.0.0.1:0"
	leader := mustGetLeader(c, svr.client, svr.getLeaderPath())
	grpcPDClient := mustNewGrpcClient(c, getLeaderAddr(leader))
	s.tryBootstrapCluster(c, grpcPDClient, clusterID, storeAddr)

	cluster := svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	cluster.stop()

	err = svr.createRaftCluster()
	c.Assert(err, IsNil)

	cluster = svr.GetRaftCluster()
	c.Assert(cluster, NotNil)
	cluster.stop()
}

func (s *testClusterSuite) TestGetPDMembers(c *C) {

	req := &pdpb.GetMembersRequest{
		Header: newRequestHeader(s.svr.ClusterID()),
	}

	resp, err := s.grpcPDClient.GetMembers(context.Background(), req)
	c.Assert(err, IsNil)
	// A more strict test can be found at api/member_test.go
	c.Assert(len(resp.GetMembers()), Not(Equals), 0)
}
