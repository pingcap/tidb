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
	"net/http"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testRedirectorSuite{})

type testRedirectorSuite struct {
}

func (s *testRedirectorSuite) SetUpSuite(c *C) {
}

func (s *testRedirectorSuite) TearDownSuite(c *C) {
}

func (s *testRedirectorSuite) TestRedirect(c *C) {
	_, svrs, cleanup := mustNewCluster(c, 3)
	defer cleanup()

	for _, svr := range svrs {
		mustRequestSuccess(c, svr)
	}
}

func (s *testRedirectorSuite) TestReconnect(c *C) {
	_, svrs, cleanup := mustNewCluster(c, 3)
	defer cleanup()

	// Collect two followers.
	var followers []*server.Server
	leader := mustWaitLeader(c, svrs)
	for _, svr := range svrs {
		if svr != leader {
			followers = append(followers, svr)
		}
	}

	// Make connections to followers.
	// Make sure they proxy requests to the leader.
	for i := 0; i < 2; i++ {
		svr := followers[i]
		mustRequestSuccess(c, svr)
	}

	// Close the leader and wait for a new one.
	leader.Close()
	newLeader := mustWaitLeader(c, followers)

	// Make sure they proxy requests to the new leader.
	for i := 0; i < 2; i++ {
		svr := followers[i]
		mustRequestSuccess(c, svr)
	}

	// Close the new leader and then we have only one node.
	newLeader.Close()
	time.Sleep(time.Second)

	// Request will fail with no leader.
	for i := 0; i < 2; i++ {
		svr := followers[i]
		if svr != newLeader {
			resp := mustRequest(c, svr)
			c.Assert(resp.StatusCode, Equals, http.StatusInternalServerError)
		}
	}
}

func (s *testRedirectorSuite) TestNotLeader(c *C) {
	_, svrs, cleanup := mustNewCluster(c, 3)
	defer cleanup()

	// Find a follower.
	var follower *server.Server
	leader := mustWaitLeader(c, svrs)
	for _, svr := range svrs {
		if svr != leader {
			follower = svr
			break
		}
	}

	client := newHTTPClient()

	addr := follower.GetAddr() + apiPrefix + "/api/v1/version"
	// Request to follower without redirectorHeader is OK.
	request, err := http.NewRequest("GET", addr, nil)
	c.Assert(err, IsNil)
	resp, err := client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)

	// Request to follower with redirectorHeader will fail.
	request.RequestURI = ""
	request.Header.Set(redirectorHeader, "pd")
	resp, err = client.Do(request)
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Not(Equals), http.StatusOK)
}

func mustRequest(c *C, s *server.Server) *http.Response {
	resp, err := http.Get(s.GetAddr() + apiPrefix + "/api/v1/version")
	c.Assert(err, IsNil)
	return resp
}

func mustRequestSuccess(c *C, s *server.Server) {
	resp := mustRequest(c, s)
	c.Assert(resp.StatusCode, Equals, http.StatusOK)
}
