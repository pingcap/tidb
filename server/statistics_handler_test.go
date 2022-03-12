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
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestDumpStatsAPI(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	driver := NewTiDBDriver(store)
	client := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = client.port
	cfg.Status.StatusPort = client.statusPort
	cfg.Status.ReportStatus = true
	cfg.Socket = fmt.Sprintf("/tmp/tidb-mock-%d.sock", time.Now().UnixNano())

	server, err := NewServer(cfg, driver)
	require.NoError(t, err)
	defer server.Close()

	client.port = getPortFromTCPAddr(server.listener.Addr())
	client.statusPort = getPortFromTCPAddr(server.statusListener.Addr())
	go func() {
		err := server.Run()
		require.NoError(t, err)
	}()
	client.waitUntilServerOnline()

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	statsHandler := &StatsHandler{dom}

	prepareData(t, client, statsHandler)

	router := mux.NewRouter()
	router.Handle("/stats/dump/{db}/{table}", statsHandler)

	resp0, err := client.fetchStatus("/stats/dump/tidb/test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()

	path := "/tmp/stats.json"
	fp, err := os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)
	defer func() {
		require.NoError(t, fp.Close())
		require.NoError(t, os.Remove(path))
	}()

	js, err := io.ReadAll(resp0.Body)
	require.NoError(t, err)
	_, err = fp.Write(js)
	require.NoError(t, err)
	checkData(t, path, client)
	checkCorrelation(t, client)

	// sleep for 1 seconds to ensure the existence of tidb.test
	time.Sleep(time.Second)
	timeBeforeDropStats := time.Now()
	snapshot := timeBeforeDropStats.Format("20060102150405")
	prepare4DumpHistoryStats(t, client)

	// test dump history stats
	resp1, err := client.fetchStatus("/stats/dump/tidb/test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp1.Body.Close())
	}()
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

	resp2, err := client.fetchStatus("/stats/dump/tidb/test/" + snapshot)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp2.Body.Close())
	}()
	js, err = io.ReadAll(resp2.Body)
	require.NoError(t, err)
	_, err = fp1.Write(js)
	require.NoError(t, err)
	checkData(t, path1, client)
}

func prepareData(t *testing.T, client *testServerClient, statHandle *StatsHandler) {
	db, err := sql.Open("mysql", client.getDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)

	h := statHandle.do.StatsHandle()
	tk.MustExec("create database tidb")
	tk.MustExec("use tidb")
	tk.MustExec("create table test (a int, b varchar(20))")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	tk.MustExec("create index c on test (a, b)")
	tk.MustExec("insert test values (1, 's')")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table test")
	tk.MustExec("insert into test(a,b) values (1, 'v'),(3, 'vvv'),(5, 'vv')")
	is := statHandle.do.InfoSchema()
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
}

func prepare4DumpHistoryStats(t *testing.T, client *testServerClient) {
	db, err := sql.Open("mysql", client.getDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	tk := testkit.NewDBTestKit(t, db)

	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	tk.MustExec("drop table tidb.test")
	tk.MustExec("create table tidb.test (a int, b varchar(20))")
}

func checkCorrelation(t *testing.T, client *testServerClient) {
	db, err := sql.Open("mysql", client.getDSN())
	require.NoError(t, err, "Error connecting")
	tk := testkit.NewDBTestKit(t, db)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	tk.MustExec("use tidb")
	rows := tk.MustQuery("SELECT tidb_table_id FROM information_schema.tables WHERE table_name = 'test' AND table_schema = 'tidb'")
	var tableID int64
	if rows.Next() {
		err = rows.Scan(&tableID)
		require.NoError(t, err)
		require.False(t, rows.Next(), "unexpected data")
	} else {
		require.FailNow(t, "no data")
	}
	require.NoError(t, rows.Close())
	rows = tk.MustQuery("select correlation from mysql.stats_histograms where table_id = ? and hist_id = 1 and is_index = 0", tableID)
	if rows.Next() {
		var corr float64
		err = rows.Scan(&corr)
		require.NoError(t, err)
		require.Equal(t, float64(1), corr)
		require.False(t, rows.Next(), "unexpected data")
	} else {
		require.FailNow(t, "no data")
	}
	require.NoError(t, rows.Close())
}

func checkData(t *testing.T, path string, client *testServerClient) {
	db, err := sql.Open("mysql", client.getDSN(func(config *mysql.Config) {
		config.AllowAllFiles = true
		config.Params["sql_mode"] = "''"
	}))
	require.NoError(t, err, "Error connecting")
	tk := testkit.NewDBTestKit(t, db)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	tk.MustExec("use tidb")
	tk.MustExec("drop stats test")
	tk.MustExec(fmt.Sprintf("load stats '%s'", path))

	rows := tk.MustQuery("show stats_meta")
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
	require.NoError(t, rows.Close())
}
