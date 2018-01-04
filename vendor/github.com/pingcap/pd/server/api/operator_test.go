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
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/core"
)

var _ = Suite(&testOperatorSuite{})

type testOperatorSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testOperatorSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testOperatorSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testOperatorSuite) TestAddRemovePeer(c *C) {
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, nil)
	mustPutStore(c, s.svr, 2, metapb.StoreState_Up, nil)

	peer1 := &metapb.Peer{Id: 1, StoreId: 1}
	peer2 := &metapb.Peer{Id: 2, StoreId: 2}
	region := &metapb.Region{Id: 1, Peers: []*metapb.Peer{peer1, peer2}}
	regionInfo := core.NewRegionInfo(region, peer1)
	mustRegionHeartbeat(c, s.svr, regionInfo)

	regionURL := fmt.Sprintf("%s/operators/%d", s.urlPrefix, region.Id)
	operator := mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "operator not found"), IsTrue)

	mustPutStore(c, s.svr, 3, metapb.StoreState_Up, nil)
	err := postJSON(&http.Client{}, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"add-peer", "region_id": 1, "store_id": 3}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Assert(strings.Contains(operator, "add peer 1 on store 3"), IsTrue)

	err = doDelete(regionURL)
	c.Assert(err, IsNil)

	err = postJSON(&http.Client{}, fmt.Sprintf("%s/operators", s.urlPrefix), []byte(`{"name":"remove-peer", "region_id": 1, "store_id": 2}`))
	c.Assert(err, IsNil)
	operator = mustReadURL(c, regionURL)
	c.Log(operator)
	c.Assert(strings.Contains(operator, "remove peer on store 2"), IsTrue)
}

func mustPutStore(c *C, svr *server.Server, id uint64, state metapb.StoreState, labels []*metapb.StoreLabel) {
	_, err := svr.PutStore(context.Background(), &pdpb.PutStoreRequest{
		Header: &pdpb.RequestHeader{ClusterId: svr.ClusterID()},
		Store: &metapb.Store{
			Id:      id,
			Address: fmt.Sprintf("tikv%d", id),
			State:   state,
			Labels:  labels,
		},
	})
	c.Assert(err, IsNil)
}

func mustRegionHeartbeat(c *C, svr *server.Server, region *core.RegionInfo) {
	cluster := svr.GetRaftCluster()
	err := cluster.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)
}

func mustReadURL(c *C, url string) string {
	res, err := http.Get(url)
	c.Assert(err, IsNil)
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	c.Assert(err, IsNil)
	return string(data)
}
