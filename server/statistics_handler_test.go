// Copyright 2018 PingCAP, Inc.
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

package server

import (
	"database/sql"
	"encoding/json"
	"net/http"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/mocktikv"
)

type testDumpStatsSuite struct {
	server *Server
}

var _ = Suite(new(testDumpStatsSuite))

func (ds *testDumpStatsSuite) TestDumpStatsAPI(c *C) {
	ds.startServer(c)
	ds.prepareData(c)
	defer ds.stopServer(c)

	resp, err := http.Get("http://127.0.0.1:10090/stats/dump/tidb/test")
	c.Assert(err, IsNil)

	decoder := json.NewDecoder(resp.Body)
	var jsonTbl statistics.JSONTable
	err = decoder.Decode(&jsonTbl)
	c.Assert(err, IsNil)
	c.Assert(jsonTbl.DatabaseName, Equals, "tidb")
	c.Assert(jsonTbl.TableName, Equals, "test")
}

func (ds *testDumpStatsSuite) startServer(c *C) {
	mvccStore := mocktikv.NewMvccStore()
	store, err := tikv.NewMockTikvStore(tikv.WithMVCCStore(mvccStore))
	c.Assert(err, IsNil)

	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)

	tidbdrv := NewTiDBDriver(store)

	cfg := config.NewConfig()
	cfg.Port = 4001
	cfg.Status.StatusPort = 10090
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, tidbdrv)
	c.Assert(err, IsNil)

	ds.server = server
	go server.Run()
	waitUntilServerOnline(cfg.Status.StatusPort)
}

func (ds *testDumpStatsSuite) stopServer(c *C) {
	if ds.server != nil {
		ds.server.Close()
	}
}

func (ds *testDumpStatsSuite) prepareData(c *C) {
	db, err := sql.Open("mysql", getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()
	dbt := &DBTest{c, db}

	dbt.mustExec("create database tidb")
	dbt.mustExec("use tidb")
	dbt.mustExec("create table test (a int, b varchar(20))")
	dbt.mustExec("create index c on test (a, b)")
	dbt.mustExec("insert test values (1, 2)")
	dbt.mustExec("analyze table test")
}
