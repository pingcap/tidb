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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testMemberAPISuite{})

type testMemberAPISuite struct {
	hc *http.Client
}

func (s *testMemberAPISuite) SetUpSuite(c *C) {
	s.hc = newHTTPClient()
}

func relaxEqualStings(c *C, a, b []string) {
	sort.Strings(a)
	sortedStringA := strings.Join(a, "")

	sort.Strings(b)
	sortedStringB := strings.Join(b, "")

	c.Assert(sortedStringA, Equals, sortedStringB)
}

func checkListResponse(c *C, body []byte, cfgs []*server.Config) {
	got := make(map[string][]*pdpb.Member)
	json.Unmarshal(body, &got)

	c.Assert(len(got["members"]), Equals, len(cfgs))

	for _, memb := range got["members"] {
		for _, cfg := range cfgs {
			if memb.GetName() != cfg.Name {
				continue
			}

			relaxEqualStings(c, memb.ClientUrls, strings.Split(cfg.ClientUrls, ","))
			relaxEqualStings(c, memb.PeerUrls, strings.Split(cfg.PeerUrls, ","))
		}
	}
}

func (s *testMemberAPISuite) TestMemberList(c *C) {
	numbers := []int{1, 3}

	for _, num := range numbers {
		cfgs, _, clean := mustNewCluster(c, num)
		defer clean()

		addr := cfgs[rand.Intn(len(cfgs))].ClientUrls + apiPrefix + "/api/v1/members"
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		buf, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		checkListResponse(c, buf, cfgs)
	}
}

func (s *testMemberAPISuite) TestMemberDelete(c *C) {
	s.testMemberDelete(c, true)
	s.testMemberDelete(c, false)
}

func (s *testMemberAPISuite) testMemberDelete(c *C, byName bool) {
	cfgs, svrs, clean := mustNewCluster(c, 3)
	defer clean()

	target := rand.Intn(len(svrs))
	server := svrs[target]

	for i, cfg := range cfgs {
		if cfg.Name == server.Name() {
			cfgs = append(cfgs[:i], cfgs[i+1:]...)
			break
		}
	}
	for i, svr := range svrs {
		if svr.Name() == server.Name() {
			svrs = append(svrs[:i], svrs[i+1:]...)
			break
		}
	}
	clientURL := cfgs[rand.Intn(len(cfgs))].ClientUrls

	server.Close()
	time.Sleep(5 * time.Second)
	mustWaitLeader(c, svrs)

	var table = []struct {
		name    string
		id      uint64
		checker Checker
		status  int
	}{
		{
			// delete a nonexistent pd
			name:    fmt.Sprintf("test-%d", rand.Int63()),
			checker: Equals,
			status:  http.StatusNotFound,
		},
		{
			// delete a pd randomly
			name:    server.Name(),
			id:      server.ID(),
			checker: Equals,
			status:  http.StatusOK,
		},
		{
			// delete it again
			name:    server.Name(),
			id:      server.ID(),
			checker: Equals,
			status:  http.StatusNotFound,
		},
	}

	for _, t := range table {
		var addr string
		if byName {
			addr = clientURL + apiPrefix + "/api/v1/members/name/" + t.name
		} else {
			addr = clientURL + apiPrefix + "/api/v1/members/id/" + strconv.FormatUint(t.id, 10)
		}
		req, err := http.NewRequest("DELETE", addr, nil)
		c.Assert(err, IsNil)
		resp, err := s.hc.Do(req)
		c.Assert(err, IsNil)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			c.Assert(resp.StatusCode, t.checker, t.status)
		}
	}

	for i := 0; i < 10; i++ {
		addr := clientURL + apiPrefix + "/api/v1/members"
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusInternalServerError {
			time.Sleep(1)
			continue
		}
		buf, err := ioutil.ReadAll(resp.Body)
		c.Assert(err, IsNil)
		checkListResponse(c, buf, cfgs)
	}
}

func (s *testMemberAPISuite) TestMemberLeader(c *C) {
	cfgs, svrs, clean := mustNewCluster(c, 3)
	defer clean()

	leader, err := svrs[0].GetLeader()
	c.Assert(err, IsNil)

	addr := cfgs[rand.Intn(len(cfgs))].ClientUrls + apiPrefix + "/api/v1/leader"
	c.Assert(err, IsNil)
	resp, err := s.hc.Get(addr)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	buf, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)

	var got pdpb.Member
	json.Unmarshal(buf, &got)
	c.Assert(got.GetClientUrls(), DeepEquals, leader.GetClientUrls())
	c.Assert(got.GetMemberId(), Equals, leader.GetMemberId())
}

func (s *testMemberAPISuite) TestLeaderResign(c *C) {
	cfgs, svrs, clean := mustNewCluster(c, 3)
	defer clean()

	addrs := make(map[uint64]string)
	for i := range cfgs {
		addrs[svrs[i].ID()] = cfgs[i].ClientUrls
	}

	leader1, err := svrs[0].GetLeader()
	c.Assert(err, IsNil)

	s.post(c, addrs[leader1.GetMemberId()]+apiPrefix+"/api/v1/leader/resign")
	leader2 := s.waitLeaderChange(c, svrs[0], leader1)
	s.post(c, addrs[leader2.GetMemberId()]+apiPrefix+"/api/v1/leader/transfer/"+leader1.GetName())
	leader3 := s.waitLeaderChange(c, svrs[0], leader2)
	c.Assert(leader3.GetMemberId(), Equals, leader1.GetMemberId())
}

func (s *testMemberAPISuite) post(c *C, url string) {
	for i := 0; i < 5; i++ {
		res, err := http.Post(url, "", nil)
		c.Assert(err, IsNil)
		if res.StatusCode == http.StatusOK {
			return
		}
		time.Sleep(time.Millisecond * 500)
	}
	c.Fatal("failed to send query after retry 5 times")
}

func (s *testMemberAPISuite) waitLeaderChange(c *C, svr *server.Server, old *pdpb.Member) *pdpb.Member {
	var leader *pdpb.Member
	testutil.WaitUntil(c, func(c *C) bool {
		var err error
		leader, err = svr.GetLeader()
		if err != nil {
			c.Log(err)
			return false
		}
		if leader.GetMemberId() == old.GetMemberId() {
			c.Log("leader not change")
			return false
		}
		return true
	})
	return leader
}
