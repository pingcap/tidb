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
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

type testDumpStatsSuite struct {
	server *Server
	sh     *StatsHandler
}

var _ = Suite(new(testDumpStatsSuite))

func (ds *testDumpStatsSuite) startServer(c *C) {
	mvccStore := mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(mockstore.WithMVCCStore(mvccStore))
	c.Assert(err, IsNil)
	session.SetStatsLease(0)
	_, err = session.BootstrapSession(store)
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

	do, err := session.GetDomain(store)
	c.Assert(err, IsNil)
	ds.sh = &StatsHandler{do}
}

func (ds *testDumpStatsSuite) TestDumpStatsAPI(c *C) {
	ds.startServer(c)
	ds.prepareData(c)
	defer ds.server.Close()

	router := mux.NewRouter()
	router.Handle("/stats/dump/{db}/{table}", ds.sh)

	resp, err := http.Get("http://127.0.0.1:10090/stats/dump/tidb/test")
	c.Assert(err, IsNil)
	defer resp.Body.Close()

	path := "/tmp/stats.json"
	fp, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)
	defer func() {
		c.Assert(fp.Close(), IsNil)
		c.Assert(os.Remove(path), IsNil)
	}()

	js, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	fp.Write(js)
	ds.checkData(c, path)
}

func (ds *testDumpStatsSuite) prepareData(c *C) {
	db, err := sql.Open("mysql", getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()
	dbt := &DBTest{c, db}

	h := ds.sh.do.StatsHandle()
	dbt.mustExec("create database tidb")
	dbt.mustExec("use tidb")
	dbt.mustExec("create table test (a int, b varchar(20))")
	h.HandleDDLEvent(<-h.DDLEventCh())
	dbt.mustExec("create index c on test (a, b)")
	dbt.mustExec("insert test values (1, 's')")
	c.Assert(h.DumpStatsDeltaToKV(), IsNil)
	dbt.mustExec("analyze table test")
	dbt.mustExec("insert into test(a,b) values (1, 'v'),(3, 'vvv'),(5, 'vv')")
	is := ds.sh.do.InfoSchema()
	c.Assert(h.DumpStatsDeltaToKV(), IsNil)
	c.Assert(h.Update(is), IsNil)
}

func (ds *testDumpStatsSuite) checkData(c *C, path string) {
	db, err := sql.Open("mysql", getDSN(func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Strict = false
	}))
	c.Assert(err, IsNil, Commentf("Error connecting"))
	dbt := &DBTest{c, db}
	defer func() {
		dbt.mustExec("drop database tidb")
		dbt.mustExec("truncate table mysql.stats_meta")
		dbt.mustExec("truncate table mysql.stats_histograms")
		dbt.mustExec("truncate table mysql.stats_buckets")
		db.Close()
	}()

	dbt.mustExec("use tidb")
	dbt.mustExec("drop stats test")
	_, err = dbt.db.Exec(fmt.Sprintf("load stats '%s'", path))
	c.Assert(err, IsNil)

	rows := dbt.mustQuery("show stats_meta")
	dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
	var dbName, tableName string
	var modifyCount, count int64
	var other interface{}
	err = rows.Scan(&dbName, &tableName, &other, &modifyCount, &count)
	dbt.Check(err, IsNil)
	dbt.Check(dbName, Equals, "tidb")
	dbt.Check(tableName, Equals, "test")
	dbt.Check(modifyCount, Equals, int64(3))
	dbt.Check(count, Equals, int64(4))
}
