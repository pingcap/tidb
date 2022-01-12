// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package unistore

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	pd "github.com/tikv/pd/client"
)

var _ = Suite(&GlobalConfigTestSuite{})

type GlobalConfigTestSuite struct {
	rpc     *RPCClient
	cluster *Cluster
	client  pd.Client
}

func Test(t *testing.T) {
	TestingT(t)
}

func (s *GlobalConfigTestSuite) SetUpSuite(c *C) {
	var err error
	s.rpc, s.client, s.cluster, err = New("")
	c.Assert(err, IsNil)
}

func (s *GlobalConfigTestSuite) TestLoad(c *C) {
	s.client.StoreGlobalConfig(context.Background(), []pd.GlobalConfigItem{{Name: "LoadOkGlobalConfig", Value: "ok"}})

	res, err := s.client.LoadGlobalConfig(context.Background(), []string{"LoadOkGlobalConfig", "LoadErrGlobalConfig"})
	c.Assert(err, IsNil)
	for _, j := range res {
		switch j.Name {
		case "/global/config/LoadOkGlobalConfig":
			c.Assert(j.Value, Equals, "ok")
		case "/global/config/LoadErrGlobalConfig":
			c.Assert(j.Value, Equals, "")
			c.Assert(j.Error.Error(), Equals, "not found")
		default:
			c.Assert(err, NotNil)
		}
	}
}

func (s *GlobalConfigTestSuite) TestStore(c *C) {
	res, err := s.client.LoadGlobalConfig(context.Background(), []string{"NewObject"})
	c.Assert(err, IsNil)
	c.Assert(res[0].Error.Error(), Equals, "not found")
	err = s.client.StoreGlobalConfig(context.Background(), []pd.GlobalConfigItem{{Name: "NewObject", Value: "ok"}})
	c.Assert(err, IsNil)
	res, err = s.client.LoadGlobalConfig(context.Background(), []string{"NewObject"})
	c.Assert(err, IsNil)
	c.Assert(res[0].Error, IsNil)
}

func (s *GlobalConfigTestSuite) TestWatch(c *C) {
	ch, err := s.client.WatchGlobalConfig(context.Background())
	c.Assert(err, IsNil)
	for i := 0; i < 10; i++ {
		res := <-ch
		c.Assert(res[0].Value, Not(Equals), "")
	}
}

func (s *GlobalConfigTestSuite) TearDownSuite(c *C) {
	s.client.Close()
	s.rpc.Close()
	s.cluster.Close()
}
