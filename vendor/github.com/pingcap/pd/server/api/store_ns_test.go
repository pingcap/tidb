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
	"encoding/json"
	"fmt"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/server"
	_ "github.com/pingcap/pd/table"
)

var _ = Suite(&testStoreNsSuite{})

type testStoreNsSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
	stores    []*metapb.Store
}

func (s *testStoreNsSuite) SetUpSuite(c *C) {
	s.stores = []*metapb.Store{
		{
			// metapb.StoreState_Up == 0
			Id:      1,
			Address: "tikv:1",
			State:   metapb.StoreState_Up,
		},
		{
			Id:      4,
			Address: "tikv:4",
			State:   metapb.StoreState_Up,
		},
		{
			// metapb.StoreState_Offline == 1
			Id:      6,
			Address: "tikv:6",
			State:   metapb.StoreState_Offline,
		},
		{
			// metapb.StoreState_Tombstone == 2
			Id:      7,
			Address: "tikv:7",
			State:   metapb.StoreState_Tombstone,
		},
	}

	cfg := server.NewTestSingleConfig()
	cfg.NamespaceClassifier = "table"
	srv, err := server.CreateServer(cfg, NewHandler)
	c.Assert(err, IsNil)
	c.Assert(srv.Run(), IsNil)
	s.svr = srv
	s.cleanup = func() {
		srv.Close()
		cleanServer(cfg)
	}

	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	for _, store := range s.stores {
		mustPutStore(c, s.svr, store.Id, store.State, nil)
	}
}

func (s *testStoreNsSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testStoreNsSuite) TestCreateNamespace(c *C) {
	body := map[string]string{"namespace": "test"}
	b, err := json.Marshal(body)
	c.Assert(err, IsNil)
	err = postJSON(&http.Client{}, fmt.Sprintf("%s/classifier/table/namespaces", s.urlPrefix), b)
	c.Assert(err, IsNil)
}
