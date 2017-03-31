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

package tikv

import (
	"strings"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/tikv/mock-tikv"
	goctx "golang.org/x/net/context"
)

type testRegionRequestSuite struct {
	cluster             *mocktikv.Cluster
	store               uint64
	peer                uint64
	region              uint64
	cache               *RegionCache
	bo                  *Backoffer
	regionRequestSender *RegionRequestSender
	mvccStore           *mocktikv.MvccStore
}

var _ = Suite(&testRegionRequestSuite{})

func (s *testRegionRequestSuite) SetUpTest(c *C) {
	s.cluster = mocktikv.NewCluster()
	s.store, s.peer, s.region = mocktikv.BootstrapWithSingleStore(s.cluster)
	pdCli := &codecPDClient{mocktikv.NewPDClient(s.cluster)}
	s.cache = NewRegionCache(pdCli)
	s.bo = NewBackoffer(1, goctx.Background())
	s.mvccStore = mocktikv.NewMvccStore()
	client := mocktikv.NewRPCClient(s.cluster, s.mvccStore)
	s.regionRequestSender = NewRegionRequestSender(s.bo, s.cache, client)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithStoreRestart(c *C) {
	req := &kvrpcpb.Request{
		Type: kvrpcpb.MessageType_CmdRawPut,
		CmdRawPutReq: &kvrpcpb.CmdRawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendKVReq(req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.GetCmdRawPutResp(), NotNil)

	// stop store.
	s.cluster.StopStore(s.store)
	_, err = s.regionRequestSender.SendKVReq(req, region.Region, time.Second)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "try again later"), IsTrue)

	// start store.
	s.cluster.StartStore(s.store)

	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err = s.regionRequestSender.SendKVReq(req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.GetCmdRawPutResp(), NotNil)
}

func (s *testRegionRequestSuite) TestOnSendFailedWithCancelled(c *C) {
	req := &kvrpcpb.Request{
		Type: kvrpcpb.MessageType_CmdRawPut,
		CmdRawPutReq: &kvrpcpb.CmdRawPutRequest{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
	}
	region, err := s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err := s.regionRequestSender.SendKVReq(req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.GetCmdRawPutResp(), NotNil)

	// set store to cancel state.
	s.cluster.CancelStore(s.store)
	// locate region again is needed
	// since last request on the region failed and region's info had been cleared.
	_, err = s.regionRequestSender.SendKVReq(req, region.Region, time.Second)
	c.Assert(err, NotNil)
	c.Assert(errors.Cause(err), Equals, goctx.Canceled)

	// set store to normal state.
	s.cluster.UnCancelStore(s.store)
	region, err = s.cache.LocateRegionByID(s.bo, s.region)
	c.Assert(err, IsNil)
	c.Assert(region, NotNil)
	resp, err = s.regionRequestSender.SendKVReq(req, region.Region, time.Second)
	c.Assert(err, IsNil)
	c.Assert(resp.GetCmdRawPutResp(), NotNil)
}
