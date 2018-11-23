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

package pd

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
	"google.golang.org/grpc"
)

func TestClient(t *testing.T) {
	server.EnableZap = true
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

var (
	regionIDAllocator = &core.MockIDAllocator{}
	// Note: IDs below are entirely arbitrary. They are only for checking
	// whether GetRegion/GetStore works.
	// If we alloc ID in client in the future, these IDs must be updated.
	store = &metapb.Store{
		Id:      1,
		Address: "localhost",
	}
	peer = &metapb.Peer{
		Id:      2,
		StoreId: store.GetId(),
	}
)

type testClientSuite struct {
	cleanup server.CleanupFunc

	srv             *server.Server
	client          Client
	grpcPDClient    pdpb.PDClient
	regionHeartbeat pdpb.PD_RegionHeartbeatClient
}

func (s *testClientSuite) SetUpSuite(c *C) {
	var err error
	_, s.srv, s.cleanup, err = server.NewTestServer()
	c.Assert(err, IsNil)
	s.grpcPDClient = mustNewGrpcClient(c, s.srv.GetAddr())

	mustWaitLeader(c, map[string]*server.Server{s.srv.GetAddr(): s.srv})
	bootstrapServer(c, newHeader(s.srv), s.grpcPDClient)

	s.client, err = NewClient(s.srv.GetEndpoints(), SecurityOption{})
	c.Assert(err, IsNil)
	s.regionHeartbeat, err = s.grpcPDClient.RegionHeartbeat(context.Background())
	c.Assert(err, IsNil)
}

func (s *testClientSuite) TearDownSuite(c *C) {
	s.client.Close()

	s.cleanup()
}

func mustNewGrpcClient(c *C, addr string) pdpb.PDClient {
	conn, err := grpc.Dial(strings.TrimPrefix(addr, "http://"), grpc.WithInsecure())

	c.Assert(err, IsNil)
	return pdpb.NewPDClient(conn)
}

func mustWaitLeader(c *C, svrs map[string]*server.Server) *server.Server {
	for i := 0; i < 500; i++ {
		for _, s := range svrs {
			if s.IsLeader() {
				return s
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Fatal("no leader")
	return nil
}

func newHeader(srv *server.Server) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: srv.ClusterID(),
	}
}

func bootstrapServer(c *C, header *pdpb.RequestHeader, client pdpb.PDClient) {
	regionID, _ := regionIDAllocator.Alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: []*metapb.Peer{peer},
	}
	req := &pdpb.BootstrapRequest{
		Header: header,
		Store:  store,
		Region: region,
	}
	_, err := client.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
}

func (s *testClientSuite) TestTSO(c *C) {
	var tss []int64
	for i := 0; i < 100; i++ {
		p, l, err := s.client.GetTS(context.Background())
		c.Assert(err, IsNil)
		tss = append(tss, p<<18+l)
	}

	var last int64
	for _, ts := range tss {
		c.Assert(ts, Greater, last)
		last = ts
	}
}

func (s *testClientSuite) TestTSORace(c *C) {
	var wg sync.WaitGroup
	begin := make(chan struct{})
	count := 10
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			<-begin
			for i := 0; i < 100; i++ {
				_, _, err := s.client.GetTS(context.Background())
				c.Assert(err, IsNil)
			}
			wg.Done()
		}()
	}
	close(begin)
	wg.Wait()
}

func (s *testClientSuite) TestGetRegion(c *C) {
	regionID, _ := regionIDAllocator.Alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: []*metapb.Peer{peer},
	}
	req := &pdpb.RegionHeartbeatRequest{
		Header: newHeader(s.srv),
		Region: region,
		Leader: peer,
	}
	err := s.regionHeartbeat.Send(req)
	c.Assert(err, IsNil)

	testutil.WaitUntil(c, func(c *C) bool {
		r, leader, err := s.client.GetRegion(context.Background(), []byte("a"))
		c.Assert(err, IsNil)
		return c.Check(r, DeepEquals, region) &&
			c.Check(leader, DeepEquals, peer)
	})
	c.Succeed()
}

func (s *testClientSuite) TestGetPrevRegion(c *C) {
	regionLen := 10
	regions := make([]*metapb.Region, 0, regionLen)
	for i := 0; i < regionLen; i++ {
		regionID, _ := regionIDAllocator.Alloc()
		r := &metapb.Region{
			Id: regionID,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			StartKey: []byte{byte(i)},
			EndKey:   []byte{byte(i + 1)},
			Peers:    []*metapb.Peer{peer},
		}
		regions = append(regions, r)
		req := &pdpb.RegionHeartbeatRequest{
			Header: newHeader(s.srv),
			Region: r,
			Leader: peer,
		}
		err := s.regionHeartbeat.Send(req)
		c.Assert(err, IsNil)
	}
	for i := 0; i < 20; i++ {
		testutil.WaitUntil(c, func(c *C) bool {
			r, leader, err := s.client.GetPrevRegion(context.Background(), []byte{byte(i)})
			c.Assert(err, IsNil)
			if i > 0 && i < regionLen {
				return c.Check(leader, DeepEquals, peer) &&
					c.Check(r, DeepEquals, regions[i-1])
			}
			return c.Check(leader, IsNil) &&
				c.Check(r, IsNil)
		})
	}
	c.Succeed()
}

func (s *testClientSuite) TestGetRegionByID(c *C) {
	regionID, _ := regionIDAllocator.Alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: []*metapb.Peer{peer},
	}
	req := &pdpb.RegionHeartbeatRequest{
		Header: newHeader(s.srv),
		Region: region,
		Leader: peer,
	}
	err := s.regionHeartbeat.Send(req)
	c.Assert(err, IsNil)

	testutil.WaitUntil(c, func(c *C) bool {
		r, leader, err := s.client.GetRegionByID(context.Background(), regionID)
		c.Assert(err, IsNil)
		return c.Check(r, DeepEquals, region) &&
			c.Check(leader, DeepEquals, peer)
	})
	c.Succeed()
}

func (s *testClientSuite) TestGetStore(c *C) {
	cluster := s.srv.GetRaftCluster()
	c.Assert(cluster, NotNil)

	// Get an up store should be OK.
	n, err := s.client.GetStore(context.Background(), store.GetId())
	c.Assert(err, IsNil)
	c.Assert(n, DeepEquals, store)

	// Get a removed store should return error.
	err = cluster.RemoveStore(store.GetId())
	c.Assert(err, IsNil)

	// Get an offline store should be OK.
	n, err = s.client.GetStore(context.Background(), store.GetId())
	c.Assert(err, IsNil)
	c.Assert(n.GetState(), Equals, metapb.StoreState_Offline)

	err = cluster.BuryStore(store.GetId(), true)
	c.Assert(err, IsNil)

	// Get a tombstone store should fail.
	n, err = s.client.GetStore(context.Background(), store.GetId())
	c.Assert(err, IsNil)
	c.Assert(n, IsNil)
}

func (s *testClientSuite) TestGetAllStores(c *C) {
	cluster := s.srv.GetRaftCluster()
	c.Assert(cluster, NotNil)

	stores, err := s.client.GetAllStores(context.Background())
	c.Assert(err, IsNil)
	c.Assert(stores, DeepEquals, []*metapb.Store{store})
}

func (s *testClientSuite) checkGCSafePoint(c *C, expectedSafePoint uint64) {
	req := &pdpb.GetGCSafePointRequest{
		Header: newHeader(s.srv),
	}
	resp, err := s.srv.GetGCSafePoint(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.SafePoint, Equals, expectedSafePoint)
}

func (s *testClientSuite) TestUpdateGCSafePoint(c *C) {
	s.checkGCSafePoint(c, 0)
	for _, safePoint := range []uint64{0, 1, 2, 3, 233, 23333, 233333333333, math.MaxUint64} {
		newSafePoint, err := s.client.UpdateGCSafePoint(context.Background(), safePoint)
		c.Assert(err, IsNil)
		c.Assert(newSafePoint, Equals, safePoint)
		s.checkGCSafePoint(c, safePoint)
	}
	// If the new safe point is less than the old one, it should not be updated.
	newSafePoint, err := s.client.UpdateGCSafePoint(context.Background(), 1)
	c.Assert(newSafePoint, Equals, uint64(math.MaxUint64))
	c.Assert(err, IsNil)
	s.checkGCSafePoint(c, math.MaxUint64)
}
