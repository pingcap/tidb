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

package api

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"google.golang.org/grpc"
)

var (
	clusterID = uint64(time.Now().Unix())
	store     = &metapb.Store{
		Id:      1,
		Address: "localhost",
	}
	peers = []*metapb.Peer{
		{
			Id:      2,
			StoreId: store.GetId(),
		},
	}
	region = &metapb.Region{
		Id: 8,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers,
	}
)

func TestAPIServer(t *testing.T) {
	TestingT(t)
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
	}
}

func cleanServer(cfg *server.Config) {
	// Clean data directory
	os.RemoveAll(cfg.DataDir)
}

type cleanUpFunc func()

func mustNewServer(c *C) (*server.Server, cleanUpFunc) {
	_, svrs, cleanup := mustNewCluster(c, 1)
	return svrs[0], cleanup
}

func mustNewCluster(c *C, num int) ([]*server.Config, []*server.Server, cleanUpFunc) {
	svrs := make([]*server.Server, 0, num)
	cfgs := server.NewTestMultiConfig(num)

	ch := make(chan *server.Server, num)
	for _, cfg := range cfgs {
		go func(cfg *server.Config) {
			s, err := server.CreateServer(cfg, NewHandler)
			c.Assert(err, IsNil)
			err = s.Run()
			c.Assert(err, IsNil)
			ch <- s
		}(cfg)
	}

	for i := 0; i < num; i++ {
		svr := <-ch
		svrs = append(svrs, svr)
	}
	close(ch)

	// wait etcds and http servers
	mustWaitLeader(c, svrs)

	// clean up
	clean := func() {
		for _, s := range svrs {
			s.Close()
		}
		for _, cfg := range cfgs {
			cleanServer(cfg)
		}
	}

	return cfgs, svrs, clean
}

func mustWaitLeader(c *C, svrs []*server.Server) *server.Server {
	for i := 0; i < 100; i++ {
		for _, svr := range svrs {
			if svr.IsLeader() {
				return svr
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.Fatal("no leader")
	return nil
}

func newRequestHeader(clusterID uint64) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: clusterID,
	}
}

func mustNewGrpcClient(c *C, addr string) pdpb.PDClient {
	conn, err := grpc.Dial(strings.TrimLeft(addr, "http://"), grpc.WithInsecure())

	c.Assert(err, IsNil)
	return pdpb.NewPDClient(conn)
}
func mustBootstrapCluster(c *C, s *server.Server) {
	grpcPDClient := mustNewGrpcClient(c, s.GetAddr())
	req := &pdpb.BootstrapRequest{
		Header: newRequestHeader(s.ClusterID()),
		Store:  store,
		Region: region,
	}
	resp, err := grpcPDClient.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.GetHeader().GetError().GetType(), Equals, pdpb.ErrorType_OK)
}

func readJSONWithURL(url string, data interface{}) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return readJSON(resp.Body, data)
}
