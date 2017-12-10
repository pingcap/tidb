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

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testRegionSuite{})

type testRegionSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testRegionSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testRegionSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func newTestRegionInfo(regionID, storeID uint64, start, end []byte) *core.RegionInfo {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: storeID,
	}

	return &core.RegionInfo{
		Region: &metapb.Region{
			Id:       regionID,
			StartKey: start,
			EndKey:   end,
			Peers:    []*metapb.Peer{leader},
		},
		Leader: leader,
	}
}

func (s *testRegionSuite) TestRegion(c *C) {
	r := newTestRegionInfo(2, 1, []byte("a"), []byte("b"))
	mustRegionHeartbeat(c, s.svr, r)
	url := fmt.Sprintf("%s/region/id/%d", s.urlPrefix, r.GetId())
	r1 := &regionInfo{}
	err := readJSONWithURL(url, r1)
	c.Assert(err, IsNil)
	c.Assert(r1, DeepEquals, newRegionInfo(r))

	url = fmt.Sprintf("%s/region/key/%s", s.urlPrefix, "a")
	r2 := &regionInfo{}
	err = readJSONWithURL(url, r2)
	c.Assert(err, IsNil)
	c.Assert(r2, DeepEquals, newRegionInfo(r))
}
