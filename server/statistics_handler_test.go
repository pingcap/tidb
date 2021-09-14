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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

type testDumpStatsHelper struct {
	*testServerClient
	server *Server
	sh     *StatsHandler
	store  kv.Storage
	domain *domain.Domain
}

func (ds *testDumpStatsHelper) startServer(t *testing.T) {
	var err error
	ds.store, ds.domain, _ = testkit.CreateMockStoreAndDomain(t)
	tidbdrv := NewTiDBDriver(ds.store)

	cfg := newTestConfig()
	cfg.Port = ds.port
	cfg.Status.StatusPort = ds.statusPort
	cfg.Status.ReportStatus = true

	server, err := NewServer(cfg, tidbdrv)
	require.NoError(t, err)
	ds.port = getPortFromTCPAddr(server.listener.Addr())
	ds.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	ds.server = server
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	ds.waitUntilServerOnline()

	do, err := session.GetDomain(ds.store)
	require.NoError(t, err)
	ds.sh = &StatsHandler{do}
}

func (ds *testDumpStatsHelper) stopServer(t *testing.T) {
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

func TestDumpStatsAPI(t *testing.T) {
	ds := &testDumpStatsHelper{testServerClient: newTestServerClient()}
	ds.startServer(t)
	defer ds.stopServer(t)
	ds.prepareData(t)

	router := mux.NewRouter()
	router.Handle("/stats/dump/{db}/{table}", ds.sh)

	resp, err := ds.fetchStatus("/stats/dump/tidb/test")
	require.NoError(t, err)
	defer resp.Body.Close()

	path := "/tmp/stats.json"
	fp, err := os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)
	defer func() {
		require.NoError(t, fp.Close())
		require.NoError(t, os.Remove(path))
	}()

	js, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_, err = fp.Write(js)
	require.NoError(t, err)
	ds.checkData(t, path)
	ds.checkCorrelation(t)

	// sleep for 1 seconds to ensure the existence of tidb.test
	time.Sleep(time.Second)
	timeBeforeDropStats := time.Now()
	snapshot := timeBeforeDropStats.Format("20060102150405")
	ds.prepare4DumpHistoryStats(t)

	// test dump history stats
	resp1, err := ds.fetchStatus("/stats/dump/tidb/test")
	require.NoError(t, err)
	defer resp1.Body.Close()
	js, err = io.ReadAll(resp1.Body)
	require.NoError(t, err)
	require.Equal(t, "null", string(js))

	path1 := "/tmp/stats_history.json"
	fp1, err := os.Create(path1)
	require.NoError(t, err)
	require.NotNil(t, fp1)
	defer func() {
		require.NoError(t, fp1.Close())
		require.NoError(t, os.Remove(path1))
	}()

	resp1, err = ds.fetchStatus("/stats/dump/tidb/test/" + snapshot)
	require.NoError(t, err)

	js, err = io.ReadAll(resp1.Body)
	require.NoError(t, err)
	_, err = fp1.Write(js)
	require.NoError(t, err)
	ds.checkData(t, path1)
}

func (ds *testDumpStatsHelper) prepareData(t *testing.T) {
	db, err := sql.Open("mysql", ds.getDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	dbt := &DBTestWithT{t, db}

	h := ds.sh.do.StatsHandle()
	dbt.mustExec("create database tidb")
	dbt.mustExec("use tidb")
	dbt.mustExec("create table test (a int, b varchar(20))")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	dbt.mustExec("create index c on test (a, b)")
	dbt.mustExec("insert test values (1, 's')")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	dbt.mustExec("analyze table test")
	dbt.mustExec("insert into test(a,b) values (1, 'v'),(3, 'vvv'),(5, 'vv')")
	is := ds.sh.do.InfoSchema()
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
}

func (ds *testDumpStatsHelper) prepare4DumpHistoryStats(t *testing.T) {
	db, err := sql.Open("mysql", ds.getDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	dbt := &DBTestWithT{t, db}

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

func (ds *testDumpStatsHelper) checkCorrelation(t *testing.T) {
	db, err := sql.Open("mysql", ds.getDSN())
	require.NoError(t, err, "Error connecting")
	dbt := &DBTestWithT{t, db}
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	dbt.mustExec("use tidb")
	rows := dbt.mustQuery("SELECT tidb_table_id FROM information_schema.tables WHERE table_name = 'test' AND table_schema = 'tidb'")
	var tableID int64
	if rows.Next() {
		err = rows.Scan(&tableID)
		require.NoError(t, err)
		require.False(t, rows.Next(), "unexpected data")
	} else {
		dbt.Error("no data")
	}
	rows.Close()
	rows = dbt.mustQuery("select correlation from mysql.stats_histograms where table_id = ? and hist_id = 1 and is_index = 0", tableID)
	if rows.Next() {
		var corr float64
		err = rows.Scan(&corr)
		require.NoError(t, err)
		require.Equal(t, float64(1), corr)
		require.False(t, rows.Next(), "unexpected data")
	} else {
		dbt.Error("no data")
	}
	rows.Close()
}

func (ds *testDumpStatsHelper) checkData(t *testing.T, path string) {
	db, err := sql.Open("mysql", ds.getDSN(func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}))
	require.NoError(t, err, "Error connecting")
	dbt := &DBTestWithT{t, db}
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	dbt.mustExec("use tidb")
	dbt.mustExec("drop stats test")
	_, err = dbt.db.Exec(fmt.Sprintf("load stats '%s'", path))
	require.NoError(t, err)

	rows := dbt.mustQuery("show stats_meta")
	require.True(t, rows.Next(), "unexpected data")
	var dbName, tableName string
	var modifyCount, count int64
	var other interface{}
	err = rows.Scan(&dbName, &tableName, &other, &other, &modifyCount, &count)
	require.NoError(t, err)
	require.Equal(t, "tidb", dbName)
	require.Equal(t, "test", tableName)
	require.Equal(t, int64(3), modifyCount)
	require.Equal(t, int64(4), count)
}
