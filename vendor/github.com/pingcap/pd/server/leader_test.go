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

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

var _ = Suite(&testGetLeaderSuite{})

type testGetLeaderSuite struct {
	svr  *Server
	wg   sync.WaitGroup
	done chan bool
}

func (s *testGetLeaderSuite) SetUpSuite(c *C) {
	cfg := NewTestSingleConfig()

	// Send requests before server has started.
	s.wg.Add(1)
	s.done = make(chan bool)
	go s.sendRequest(c, cfg.ClientUrls)
	time.Sleep(100 * time.Millisecond)

	svr, err := CreateServer(cfg, nil)
	c.Assert(err, IsNil)

	err = svr.Run()
	c.Assert(err, IsNil)

	s.svr = svr
}

func (s *testGetLeaderSuite) TearDownSuite(c *C) {
	s.svr.Close()
	cleanServer(s.svr.cfg)
}

func (s *testGetLeaderSuite) TestGetLeader(c *C) {
	mustWaitLeader(c, []*Server{s.svr})

	leader, err := s.svr.GetLeader()
	c.Assert(err, IsNil)
	c.Assert(leader, NotNil)

	s.done <- true
	s.wg.Wait()
}

func (s *testGetLeaderSuite) sendRequest(c *C, addr string) {
	defer s.wg.Done()

	req := &pdpb.AllocIDRequest{
		Header: newRequestHeader(0),
	}

	for {
		select {
		case <-s.done:
			return
		default:
			// We don't need to check the response and error,
			// just make sure the server will not panic.
			grpcPDClient := mustNewGrpcClient(c, addr)
			if grpcPDClient != nil {
				grpcPDClient.AllocID(context.Background(), req)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}
