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

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
	_ "github.com/pingcap/pd/server/schedulers"
)

var _ = Suite(&testHotStatusSuite{})

type testHotStatusSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testHotStatusSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/hotspot", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testHotStatusSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s testHotStatusSuite) TestGetHotStore(c *C) {
	stat := hotStoreStats{}
	resp, err := http.Get(s.urlPrefix + "/stores")
	c.Assert(err, IsNil)
	err = readJSON(resp.Body, &stat)
	c.Assert(err, IsNil)
}
