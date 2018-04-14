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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

type testDumpStatsSuite struct {
	server *Server
	store  kv.Storage
	do     *domain.Domain
	db     *sql.DB
	tk     *testkit.TestKit
}

var _ = Suite(new(testDumpStatsSuite))

func (ds *testDumpStatsSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	var err error
	ds.store, ds.do, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	ds.db, err = sql.Open("mysql", getDSN(func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Strict = false
	}))
	c.Assert(err, IsNil, Commentf("Error connecting"))
	ds.tk = testkit.NewTestKit(c, ds.store)
}

func (ds *testDumpStatsSuite) TearDownSuite(c *C) {
	dbt := ds.tk
	dbt.MustExec("use tidb")
	r := ds.tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		ds.tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
	ds.do.StatsHandle().Clear()
	dbt.MustExec("truncate table mysql.stats_meta")
	dbt.MustExec("truncate table mysql.stats_histograms")
	dbt.MustExec("truncate table mysql.stats_buckets")
	ds.db.Close()
	ds.store.Close()
	testleak.AfterTest(c)()
}

func (ds *testDumpStatsSuite) TestDumpStatsAPI(c *C) {
	ds.startServer(c)
	ds.prepareData(c)
	defer ds.stopServer(c)

	resp, err := http.Get("http://127.0.0.1:10090/stats/dump/tidb/test")
	c.Assert(err, IsNil)

	path := "/tmp/stats.json"
	fp, err := os.Create(path)
	c.Assert(err, IsNil)
	c.Assert(fp, NotNil)

	defer func() {
		err = fp.Close()
		c.Assert(err, IsNil)
		err = os.Remove(path)
		c.Assert(err, IsNil)
	}()

	js, err := ioutil.ReadAll(resp.Body)
	c.Assert(err, IsNil)
	fp.Write(js)
	ds.checkData(c, path)
}

func (ds *testDumpStatsSuite) startServer(c *C) {
	mvccStore := mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(mockstore.WithMVCCStore(mvccStore))
	c.Assert(err, IsNil)

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
}

func (ds *testDumpStatsSuite) stopServer(c *C) {
	if ds.server != nil {
		ds.server.Close()
	}
}

func (ds *testDumpStatsSuite) prepareData(c *C) {
	dbt := ds.tk
	dbt.MustExec("create database tidb")
	dbt.MustExec("use tidb")
	dbt.MustExec("create table test (a int, b varchar(20))")
	dbt.MustExec("create index c on test (a, b)")
	dbt.MustExec("insert test values (1, 's')")
	dbt.MustExec("analyze table test")
	dbt.MustExec("insert into test(a,b) values (1, 'v'),(3, 'vvv'),(5, 'vv')")

	is := ds.do.InfoSchema()
	h := ds.do.StatsHandle()
	h.DumpStatsDeltaToKV()
	h.Update(is)
}

func (ds *testDumpStatsSuite) checkData(c *C, path string) {
	is := ds.do.InfoSchema()
	h := ds.do.StatsHandle()
	tableInfo, err := is.TableByName(model.NewCIStr("tidb"), model.NewCIStr("test"))
	c.Assert(err, IsNil)
	tbl := h.GetTableStats(tableInfo.Meta())

	dbt := ds.tk
	dbt.MustExec("use tidb")
	dbt.MustExec("drop stats test")
	_, err = dbt.Exec(fmt.Sprintf("load stats '%s'", path))
	c.Assert(err, IsNil)
	loadTbl := h.GetTableStats(tableInfo.Meta())
	assertTableEqual(c, loadTbl, tbl)
}

func assertTableEqual(c *C, a *statistics.Table, b *statistics.Table) {
	c.Assert(a.Version, Equals, b.Version)
	c.Assert(a.Count, Equals, b.Count)
	c.Assert(a.ModifyCount, Equals, b.ModifyCount)
	c.Assert(len(a.Columns), Equals, len(b.Columns))
	for i := range a.Columns {
		c.Assert(a.Columns[i].Count, Equals, b.Columns[i].Count)
		c.Assert(statistics.HistogramEqual(&a.Columns[i].Histogram, &b.Columns[i].Histogram, false), IsTrue)
		if a.Columns[i].CMSketch == nil {
			c.Assert(b.Columns[i].CMSketch, IsNil)
		} else {
			c.Assert(a.Columns[i].CMSketch.Equal(b.Columns[i].CMSketch), IsTrue)
		}
	}
	c.Assert(len(a.Indices), Equals, len(b.Indices))
	for i := range a.Indices {
		c.Assert(statistics.HistogramEqual(&a.Indices[i].Histogram, &b.Indices[i].Histogram, false), IsTrue)
		if a.Columns[i].CMSketch == nil {
			c.Assert(b.Columns[i].CMSketch, IsNil)
		} else {
			c.Assert(a.Columns[i].CMSketch.Equal(b.Columns[i].CMSketch), IsTrue)
		}
	}
}
