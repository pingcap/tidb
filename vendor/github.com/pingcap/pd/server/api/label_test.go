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
)

var _ = Suite(&testLabelsStoreSuite{})

type testLabelsStoreSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
	stores    []*metapb.Store
}

func (s *testLabelsStoreSuite) SetUpSuite(c *C) {
	s.stores = []*metapb.Store{
		{
			Id:      1,
			Address: "tikv1",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west-1",
				},
				{
					Key:   "disk",
					Value: "ssd",
				},
			},
		},
		{
			Id:      4,
			Address: "tikv4",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "us-west-2",
				},
				{
					Key:   "disk",
					Value: "hdd",
				},
			},
		},
		{
			Id:      6,
			Address: "tikv6",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "beijing",
				},
				{
					Key:   "disk",
					Value: "ssd",
				},
			},
		},
		{
			Id:      7,
			Address: "tikv7",
			State:   metapb.StoreState_Up,
			Labels: []*metapb.StoreLabel{
				{
					Key:   "zone",
					Value: "hongkong",
				},
				{
					Key:   "disk",
					Value: "ssd",
				},
				{
					Key:   "other",
					Value: "test",
				},
			},
		},
	}

	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	for _, store := range s.stores {
		mustPutStore(c, s.svr, store.Id, store.State, store.Labels)
	}
}

func (s *testLabelsStoreSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testLabelsStoreSuite) TestLabelsGet(c *C) {
	url := fmt.Sprintf("%s/labels", s.urlPrefix)
	labels := make([]*metapb.StoreLabel, 0, len(s.stores))
	err := readJSONWithURL(url, &labels)
	c.Assert(err, IsNil)
}

func (s *testLabelsStoreSuite) TestStoresLabelFilter(c *C) {

	var table = []struct {
		name, value string
		want        []*metapb.Store
	}{
		{
			name: "Zone",
			want: s.stores[:],
		},
		{
			name: "other",
			want: s.stores[3:],
		},
		{
			name:  "zone",
			value: "Us-west-1",
			want:  s.stores[:1],
		},
		{
			name:  "Zone",
			value: "west",
			want:  s.stores[:2],
		},
		{
			name:  "Zo",
			value: "Beijing",
			want:  s.stores[2:3],
		},
		{
			name:  "ZONE",
			value: "SSD",
			want:  []*metapb.Store{},
		},
	}
	for _, t := range table {
		url := fmt.Sprintf("%s/labels/stores?name=%s&value=%s", s.urlPrefix, t.name, t.value)
		info := new(storesInfo)
		err := readJSONWithURL(url, info)
		c.Assert(err, IsNil)
		checkStoresInfo(c, info.Stores, t.want)
	}
	_, err := newStoresLabelFilter("test", ".[test")
	c.Assert(err, NotNil)
}
