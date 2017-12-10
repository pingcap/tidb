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
	"math/rand"
	"net/http"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/pd/server"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct {
	hc *http.Client
}

func (s *testConfigSuite) SetUpSuite(c *C) {
	s.hc = newHTTPClient()
}

func checkConfigResponse(c *C, body []byte, cfgs []*server.Config) {
	got := &server.Config{}
	err := json.Unmarshal(body, &got)
	c.Assert(err, IsNil)
}

func (s *testConfigSuite) TestConfigAll(c *C) {
	numbers := []int{1, 3}
	for _, num := range numbers {
		cfgs, _, clean := mustNewCluster(c, num)
		defer clean()

		addr := cfgs[rand.Intn(len(cfgs))].ClientUrls + apiPrefix + "/api/v1/config"
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		cfg := &server.Config{}
		err = readJSON(resp.Body, cfg)
		c.Assert(err, IsNil)

		r := map[string]int{"max-replicas": 5}
		postData, err := json.Marshal(r)
		c.Assert(err, IsNil)
		err = postJSON(s.hc, addr, postData)
		c.Assert(err, IsNil)
		l := map[string]interface{}{
			"location-labels":       "zone,rack",
			"region-schedule-limit": 10,
		}
		postData, err = json.Marshal(l)
		c.Assert(err, IsNil)
		err = postJSON(s.hc, addr, postData)
		c.Assert(err, IsNil)

		resp, err = s.hc.Get(addr)
		newCfg := &server.Config{}
		err = readJSON(resp.Body, newCfg)
		c.Assert(err, IsNil)
		cfg.Replication.MaxReplicas = 5
		cfg.Replication.LocationLabels = []string{"zone", "rack"}
		cfg.Schedule.RegionScheduleLimit = 10
		c.Assert(cfg, DeepEquals, newCfg)
	}
}

func (s *testConfigSuite) TestConfigSchedule(c *C) {
	numbers := []int{1, 3}
	for _, num := range numbers {
		cfgs, _, clean := mustNewCluster(c, num)
		defer clean()

		addr := cfgs[rand.Intn(len(cfgs))].ClientUrls + apiPrefix + "/api/v1/config/schedule"
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)
		sc := &server.ScheduleConfig{}
		readJSON(resp.Body, sc)

		sc.MaxStoreDownTime.Duration = time.Second
		postData, err := json.Marshal(sc)
		postAddr := cfgs[rand.Intn(len(cfgs))].ClientUrls + apiPrefix + "/api/v1/config/schedule"
		err = postJSON(s.hc, postAddr, postData)
		c.Assert(err, IsNil)

		resp, err = s.hc.Get(addr)
		sc1 := &server.ScheduleConfig{}
		readJSON(resp.Body, sc1)

		c.Assert(*sc, DeepEquals, *sc1)
	}
}

func (s *testConfigSuite) TestConfigReplication(c *C) {
	numbers := []int{1, 3}
	for _, num := range numbers {
		cfgs, _, clean := mustNewCluster(c, num)
		defer clean()

		addr := cfgs[rand.Intn(len(cfgs))].ClientUrls + apiPrefix + "/api/v1/config/replicate"
		resp, err := s.hc.Get(addr)
		c.Assert(err, IsNil)

		rc := &server.ReplicationConfig{}
		err = readJSON(resp.Body, rc)
		c.Assert(err, IsNil)

		rc.MaxReplicas = 5

		rc1 := map[string]int{"max-replicas": 5}
		postData, err := json.Marshal(rc1)
		postAddr := cfgs[rand.Intn(len(cfgs))].ClientUrls + apiPrefix + "/api/v1/config/replicate"
		err = postJSON(s.hc, postAddr, postData)
		c.Assert(err, IsNil)
		rc.LocationLabels = []string{"zone", "rack"}

		rc2 := map[string]string{"location-labels": "zone,rack"}
		postData, err = json.Marshal(rc2)
		err = postJSON(s.hc, postAddr, postData)

		resp, err = s.hc.Get(addr)
		rc3 := &server.ReplicationConfig{}
		err = readJSON(resp.Body, rc3)

		c.Assert(*rc, DeepEquals, *rc3)
	}
}
