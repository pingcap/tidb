// Copyright 2019 PingCAP, Inc.
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

package client_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/server/client"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
)

type ClientTestSuite struct {
	store kv.Storage
	dom   *domain.Domain
	ctx   sessionctx.Context
}

var _ = Suite(new(ClientTestSuite))

func (s ClientTestSuite) TestClient(c *C) {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)

	globalConfig := config.GetGlobalConfig()
	cfg := config.NewConfig()
	cfg.Port = globalConfig.Port
	cfg.Store = "tikv"
	cfg.Status.StatusPort = globalConfig.Status.StatusPort
	cfg.Status.ReportStatus = true
	srv, err := server.NewServer(cfg, server.NewTiDBDriver(store))
	c.Assert(err, IsNil)
	go srv.Run()

	svrClient := client.NewFromGlobalConfig()
	err = svrClient.PollServerOnline()
	c.Assert(err, IsNil)

	resp, err := svrClient.Get("/status")
	c.Assert(err, IsNil)
	c.Assert(resp.StatusCode, Equals, 200)
}
