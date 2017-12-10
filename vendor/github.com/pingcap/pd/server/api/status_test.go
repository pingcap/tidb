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
	"io/ioutil"
	"math/rand"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testStatusAPISuite{})

type testStatusAPISuite struct {
	hc *http.Client
}

func (s *testStatusAPISuite) SetUpSuite(c *C) {
	s.hc = newHTTPClient()
}

func checkStatusResponse(c *C, body []byte, cfgs []*server.Config) {
	got := status{}
	json.Unmarshal(body, &got)

	c.Assert(got.BuildTS, Equals, server.PDBuildTS)
	c.Assert(got.GitHash, Equals, server.PDGitHash)
}

func (s *testStatusAPISuite) testStatusInternal(c *C, num int) {
	cfgs, _, clean := mustNewCluster(c, num)
	defer clean()

	addr := cfgs[rand.Intn(len(cfgs))].ClientUrls + apiPrefix + "/api/v1/status"
	resp, err := s.hc.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	checkStatusResponse(c, buf, cfgs)
}

func (s *testStatusAPISuite) TestStatus(c *C) {
	numbers := []int{1, 3}

	for _, num := range numbers {
		s.testStatusInternal(c, num)
	}
}
