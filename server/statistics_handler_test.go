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
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

type testDumpStatsSuite struct {
	server *Server
	sh     *StatsHandler
	store  kv.Storage
	domain *domain.Domain
}

var _ = Suite(new(testDumpStatsSuite))

func (ds *testDumpStatsSuite) startServer(c *C) {
	mvccStore := mocktikv.MustNewMVCCStore()
	var err error
	ds.store, err = mockstore.NewMockTikvStore(mockstore.WithMVCCStore(mvccStore))
	c.Assert(err, IsNil)
	session.DisableStats4Test()
	ds.domain, err = session.BootstrapSession(ds.store)
	c.Assert(err, IsNil)
	ds.domain.SetStatsUpdating(true)
	tidbdrv := NewTiDBDriver(ds.store)

	cfg := config.NewConfig()
	cfg.Port = 4001
	cfg.Status.StatusPort = 10090
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, tidbdrv)
	c.Assert(err, IsNil)
	ds.server = server
	go server.Run()
	waitUntilServerOnline(cfg.Status.StatusPort)

	do, err := session.GetDomain(ds.store)
	c.Assert(err, IsNil)
	ds.sh = &StatsHandler{do}
}

func (ds *testDumpStatsSuite) stopServer(c *C) {
	if ds.domain != nil {
		ds.domain.Close()
	}
	if ds.store != nil {
		ds.store.Close()
	}
	if ds.server != nil {
		ds.server.Close()
	}
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
	ds.checkCorrelation(c)

	// sleep for 1 seconds to ensure the existence of tidb.test
	time.Sleep(time.Second)
	timeBeforeDropStats := time.Now()
	snapshot := timeBeforeDropStats.Format("20060102150405")
	ds.prepare4DumpHistoryStats(c)

	// test dump history stats
	resp1, err := http.Get("http://127.0.0.1:10090/stats/dump/tidb/test")
	c.Assert(err, IsNil)
	defer resp1.Body.Close()
	js, err = ioutil.ReadAll(resp1.Body)
	c.Assert(err, IsNil)
	c.Assert(string(js), Equals, "null")

	path1 := "/tmp/stats_history.json"
	fp1, err := os.Create(path1)
	c.Assert(err, IsNil)
	c.Assert(fp1, NotNil)
	defer func() {
		c.Assert(fp1.Close(), IsNil)
		c.Assert(os.Remove(path1), IsNil)
	}()

	resp1, err = http.Get("http://127.0.0.1:10090/stats/dump/tidb/test/" + snapshot)
	c.Assert(err, IsNil)

	js, err = ioutil.ReadAll(resp1.Body)
	c.Assert(err, IsNil)
	fp1.Write(js)
	ds.checkData(c, path1)
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
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	dbt.mustExec("analyze table test")
	dbt.mustExec("insert into test(a,b) values (1, 'v'),(3, 'vvv'),(5, 'vv')")
	is := ds.sh.do.InfoSchema()
	c.Assert(h.DumpStatsDeltaToKV(handle.DumpAll), IsNil)
	c.Assert(h.Update(is), IsNil)
}

func (ds *testDumpStatsSuite) prepare4DumpHistoryStats(c *C) {
	db, err := sql.Open("mysql", getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()

	dbt := &DBTest{c, db}

	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	dbt.mustExec(updateSafePoint)

	dbt.mustExec("drop table tidb.test")
	dbt.mustExec("create table tidb.test (a int, b varchar(20))")
}

func (ds *testDumpStatsSuite) checkCorrelation(c *C) {
	db, err := sql.Open("mysql", getDSN(nil))
	c.Assert(err, IsNil, Commentf("Error connecting"))
	dbt := &DBTest{c, db}
	defer db.Close()

	dbt.mustExec("use tidb")
	rows := dbt.mustQuery("SELECT tidb_table_id FROM information_schema.tables WHERE table_name = 'test' AND table_schema = 'tidb'")
	var tableID int64
	if rows.Next() {
		rows.Scan(&tableID)
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
	} else {
		dbt.Error("no data")
	}
	rows.Close()
	rows = dbt.mustQuery("select correlation from mysql.stats_histograms where table_id = ? and hist_id = 1 and is_index = 0", tableID)
	if rows.Next() {
		var corr float64
		rows.Scan(&corr)
		dbt.Check(corr, Equals, float64(1))
		dbt.Check(rows.Next(), IsFalse, Commentf("unexpected data"))
	} else {
		dbt.Error("no data")
	}
	rows.Close()
}

func (ds *testDumpStatsSuite) checkData(c *C, path string) {
	db, err := sql.Open("mysql", getDSN(func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params = map[string]string{"sql_mode": "''"}
	}))
	c.Assert(err, IsNil, Commentf("Error connecting"))
	dbt := &DBTest{c, db}
	defer db.Close()

	dbt.mustExec("use tidb")
	dbt.mustExec("drop stats test")
	_, err = dbt.db.Exec(fmt.Sprintf("load stats '%s'", path))
	c.Assert(err, IsNil)

	rows := dbt.mustQuery("show stats_meta")
	dbt.Check(rows.Next(), IsTrue, Commentf("unexpected data"))
	var dbName, tableName string
	var modifyCount, count int64
	var other interface{}
	err = rows.Scan(&dbName, &tableName, &other, &other, &modifyCount, &count)
	dbt.Check(err, IsNil)
	dbt.Check(dbName, Equals, "tidb")
	dbt.Check(tableName, Equals, "test")
	dbt.Check(modifyCount, Equals, int64(3))
	dbt.Check(count, Equals, int64(4))
}

func (ds *testDumpStatsSuite) clearData(c *C, path string) {
	db, err := sql.Open("mysql", getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()

	dbt := &DBTest{c, db}
	dbt.mustExec("drop database tidb")
	dbt.mustExec("truncate table mysql.stats_meta")
	dbt.mustExec("truncate table mysql.stats_histograms")
	dbt.mustExec("truncate table mysql.stats_buckets")
}
