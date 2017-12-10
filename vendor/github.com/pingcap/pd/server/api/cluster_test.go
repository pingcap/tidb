// Copyright 2017 PingCAP, Inc.
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
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testClusterInfo{})

type testClusterInfo struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testClusterInfo) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)
}

func (s *testClusterInfo) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testClusterInfo) TestCluster(c *C) {
	url := fmt.Sprintf("%s/cluster", s.urlPrefix)
	c1 := &metapb.Cluster{}
	err := readJSONWithURL(url, c1)
	c.Assert(err, IsNil)

	c2 := &metapb.Cluster{}
	r := server.ReplicationConfig{MaxReplicas: 6}
	s.svr.SetReplicationConfig(r)
	err = readJSONWithURL(url, c2)
	c.Assert(err, IsNil)

	c1.MaxPeerCount = 6
	c.Assert(c1, DeepEquals, c2)
}

func (s *testClusterInfo) TestGetClusterStatus(c *C) {
	url := fmt.Sprintf("%s/cluster/status", s.urlPrefix)
	status := server.ClusterStatus{}
	err := readJSONWithURL(url, &status)
	c.Assert(status.RaftBootstrapTime.IsZero(), IsTrue)
	now := time.Now()
	mustBootstrapCluster(c, s.svr)
	err = readJSONWithURL(url, &status)
	c.Assert(err, IsNil)
	c.Assert(status.RaftBootstrapTime.After(now), IsTrue)
}
