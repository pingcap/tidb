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
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testTsoSuite{})

type testTsoSuite struct {
	client       *clientv3.Client
	svr          *Server
	cleanup      cleanUpFunc
	grpcPDClient pdpb.PDClient
}

func (s *testTsoSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustRunTestServer(c)
	s.client = s.svr.client
	mustWaitLeader(c, []*Server{s.svr})
	s.grpcPDClient = mustNewGrpcClient(c, s.svr.GetAddr())
}

func (s *testTsoSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testTsoSuite) testGetTimestamp(c *C, n int) *pdpb.Timestamp {
	req := &pdpb.TsoRequest{
		Header: newRequestHeader(s.svr.clusterID),
		Count:  uint32(n),
	}

	tsoClient, err := s.grpcPDClient.Tso(context.Background())
	c.Assert(err, IsNil)
	defer tsoClient.CloseSend()
	err = tsoClient.Send(req)
	c.Assert(err, IsNil)
	resp, err := tsoClient.Recv()
	c.Assert(resp.GetCount(), Equals, uint32(n))

	res := resp.GetTimestamp()
	c.Assert(res.GetLogical(), Greater, int64(0))

	return res
}

func mustGetLeader(c *C, client *clientv3.Client, leaderPath string) *pdpb.Member {
	for i := 0; i < 20; i++ {
		leader, err := getLeader(client, leaderPath)
		c.Assert(err, IsNil)
		if leader != nil {
			return leader
		}
		time.Sleep(500 * time.Millisecond)
	}

	c.Fatal("get leader error")
	return nil
}

func (s *testTsoSuite) TestTso(c *C) {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			last := &pdpb.Timestamp{
				Physical: 0,
				Logical:  0,
			}

			for j := 0; j < 50; j++ {
				ts := s.testGetTimestamp(c, 10)
				c.Assert(ts.GetPhysical(), Not(Less), last.GetPhysical())
				if ts.GetPhysical() == last.GetPhysical() {
					c.Assert(ts.GetLogical(), Greater, last.GetLogical())
				}
				last = ts
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}
