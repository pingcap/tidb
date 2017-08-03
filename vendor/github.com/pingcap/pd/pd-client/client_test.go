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
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

var (
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
	region = &metapb.Region{
		Id: 3,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: []*metapb.Peer{peer},
	}
)

var stripUnix = strings.NewReplacer("unix://", "")

func cleanServer(cfg *server.Config) {
	// Clean data directory
	os.RemoveAll(cfg.DataDir)

	// Clean unix sockets
	os.Remove(stripUnix.Replace(cfg.PeerUrls))
	os.Remove(stripUnix.Replace(cfg.ClientUrls))
	os.Remove(stripUnix.Replace(cfg.AdvertisePeerUrls))
	os.Remove(stripUnix.Replace(cfg.AdvertiseClientUrls))
}

type cleanupFunc func()

type testClientSuite struct {
	srv          *server.Server
	client       Client
	grpcPDClient pdpb.PDClient
	cleanup      cleanupFunc
}

func (s *testClientSuite) SetUpSuite(c *C) {
	s.srv, s.cleanup = newServer(c)
	s.grpcPDClient = mustNewGrpcClient(c, s.srv.GetAddr())

	mustWaitLeader(c, map[string]*server.Server{s.srv.GetAddr(): s.srv})
	bootstrapServer(c, newHeader(s.srv), s.grpcPDClient)

	var err error
	s.client, err = NewClient(s.srv.GetEndpoints())
	c.Assert(err, IsNil)
}

func (s *testClientSuite) TearDownSuite(c *C) {
	s.client.Close()

	s.cleanup()
}

func newServer(c *C) (*server.Server, cleanupFunc) {
	cfg := server.NewTestSingleConfig()

	s := server.CreateServer(cfg)
	err := s.StartEtcd(api.NewHandler(s))
	c.Assert(err, IsNil)

	go s.Run()

	cleanup := func() {
		s.Close()

		cleanServer(cfg)
	}

	return s, cleanup
}

var unixStripper = strings.NewReplacer("unix://", "", "unixs://", "")

func unixGrpcDialer(addr string, timeout time.Duration) (net.Conn, error) {
	sock, err := net.DialTimeout("unix", unixStripper.Replace(addr), timeout)
	return sock, err
}

func mustNewGrpcClient(c *C, addr string) pdpb.PDClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithDialer(unixGrpcDialer))

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
	req := &pdpb.RegionHeartbeatRequest{
		Header: newHeader(s.srv),
		Region: region,
		Leader: peer,
	}
	s.grpcPDClient.RegionHeartbeat(context.Background(), req)

	r, leader, err := s.client.GetRegion(context.Background(), []byte("a"))
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, region)
	c.Assert(leader, DeepEquals, peer)
}

func (s *testClientSuite) TestGetRegionByID(c *C) {
	req := &pdpb.RegionHeartbeatRequest{
		Header: newHeader(s.srv),
		Region: region,
		Leader: peer,
	}
	s.grpcPDClient.RegionHeartbeat(context.Background(), req)

	r, leader, err := s.client.GetRegionByID(context.Background(), 3)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, region)
	c.Assert(leader, DeepEquals, peer)
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
