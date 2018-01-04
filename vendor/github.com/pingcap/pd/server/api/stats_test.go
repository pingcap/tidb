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
	"net/http"
	"net/url"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/apiutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testStatsSuite{})

type testStatsSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testStatsSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testStatsSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testStatsSuite) TestRegionStats(c *C) {
	statsURL := s.urlPrefix + "/stats/region"

	regions := []*core.RegionInfo{
		{
			Region: &metapb.Region{
				Id:       1,
				StartKey: []byte(""),
				EndKey:   []byte("a"),
				Peers: []*metapb.Peer{
					{Id: 101, StoreId: 1},
					{Id: 102, StoreId: 2},
					{Id: 103, StoreId: 3},
				},
			},
			Leader:          &metapb.Peer{Id: 101, StoreId: 1},
			ApproximateSize: 100,
		},
		{
			Region: &metapb.Region{
				Id:       2,
				StartKey: []byte("a"),
				EndKey:   []byte("t"),
				Peers: []*metapb.Peer{
					{Id: 104, StoreId: 1},
					{Id: 105, StoreId: 4},
					{Id: 106, StoreId: 5},
				},
			},
			Leader:          &metapb.Peer{Id: 105, StoreId: 4},
			ApproximateSize: 200,
		},
		{
			Region: &metapb.Region{
				Id:       3,
				StartKey: []byte("t"),
				EndKey:   []byte("x"),
				Peers: []*metapb.Peer{
					{Id: 106, StoreId: 1},
					{Id: 107, StoreId: 5},
				},
			},
			Leader:          &metapb.Peer{Id: 107, StoreId: 5},
			ApproximateSize: 1,
		},
		{
			Region: &metapb.Region{
				Id:       4,
				StartKey: []byte("x"),
				EndKey:   []byte(""),
				Peers: []*metapb.Peer{
					{Id: 108, StoreId: 4},
				},
			},
			Leader:          &metapb.Peer{Id: 108, StoreId: 4},
			ApproximateSize: 50,
		},
	}

	for _, r := range regions {
		mustRegionHeartbeat(c, s.svr, r)
	}

	// Distribution (L for leader, F for follower):
	// region range       size  store1 store2 store3 store4 store5
	// 1      ["", "a")   100   L      F      F
	// 2      ["a", "t")  200   F                    L      F
	// 3      ["t", "x")  1     F                           L
	// 4      ["x", "")   50                         L

	statsAll := &core.RegionStats{
		Count:            4,
		EmptyCount:       1,
		StorageSize:      351,
		StoreLeaderCount: map[uint64]int{1: 1, 4: 2, 5: 1},
		StorePeerCount:   map[uint64]int{1: 3, 2: 1, 3: 1, 4: 2, 5: 2},
		StoreLeaderSize:  map[uint64]int64{1: 100, 4: 250, 5: 1},
		StorePeerSize:    map[uint64]int64{1: 301, 2: 100, 3: 100, 4: 250, 5: 201},
	}
	res, err := http.Get(statsURL)
	c.Assert(err, IsNil)
	stats := &core.RegionStats{}
	err = apiutil.ReadJSON(res.Body, stats)
	c.Assert(err, IsNil)
	c.Assert(stats, DeepEquals, statsAll)

	stats23 := &core.RegionStats{
		Count:            2,
		EmptyCount:       1,
		StorageSize:      201,
		StoreLeaderCount: map[uint64]int{4: 1, 5: 1},
		StorePeerCount:   map[uint64]int{1: 2, 4: 1, 5: 2},
		StoreLeaderSize:  map[uint64]int64{4: 200, 5: 1},
		StorePeerSize:    map[uint64]int64{1: 201, 4: 200, 5: 201},
	}
	args := fmt.Sprintf("?start_key=%s&end_key=%s", url.QueryEscape("\x01\x02"), url.QueryEscape("xyz\x00\x00"))
	res, err = http.Get(statsURL + args)
	c.Assert(err, IsNil)
	stats = &core.RegionStats{}
	err = apiutil.ReadJSON(res.Body, stats)
	c.Assert(err, IsNil)
	c.Assert(stats, DeepEquals, stats23)
}
