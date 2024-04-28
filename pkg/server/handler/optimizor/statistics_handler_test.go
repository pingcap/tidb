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

package optimizor_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/pkg/parser/model"
	server2 "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/handler/optimizor"
	"github.com/pingcap/tidb/pkg/server/internal/testserverclient"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session"
	util2 "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDumpStatsAPI(t *testing.T) {
	store := testkit.CreateMockStore(t)

	driver := server2.NewTiDBDriver(store)
	client := testserverclient.NewTestServerClient()
	cfg := util.NewTestConfig()
	cfg.Port = client.Port
	cfg.Status.StatusPort = client.StatusPort
	cfg.Status.ReportStatus = true
	cfg.Socket = fmt.Sprintf("/tmp/tidb-mock-%d.sock", time.Now().UnixNano())

	server, err := server2.NewServer(cfg, driver)
	require.NoError(t, err)
	defer server.Close()

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	server.SetDomain(dom)
	go func() {
		err := server.Run(nil)
		require.NoError(t, err)
	}()
	<-server2.RunInGoTestChan
	client.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	client.StatusPort = testutil.GetPortFromTCPAddr(server.StatusListenerAddr())
	client.WaitUntilServerOnline()

	statsHandler := optimizor.NewStatsHandler(dom)

	prepareData(t, client, statsHandler)
	tableInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("tidb"), model.NewCIStr("test"))
	require.NoError(t, err)
	err = dom.GetHistoricalStatsWorker().DumpHistoricalStats(tableInfo.Meta().ID, dom.StatsHandle())
	require.NoError(t, err)

	router := mux.NewRouter()
	router.Handle("/stats/dump/{db}/{table}", statsHandler)

	resp0, err := client.FetchStatus("/stats/dump/tidb/test")
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
	resp1, err := client.FetchStatus("/stats/dump/tidb/test")
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

	resp2, err := client.FetchStatus("/stats/dump/tidb/test/" + snapshot)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp2.Body.Close())
	}()
	js, err = io.ReadAll(resp2.Body)
	require.NoError(t, err)
	_, err = fp1.Write(js)
	require.NoError(t, err)
	checkData(t, path1, client)

	testDumpPartitionTableStats(t, client, statsHandler)
}

func prepareData(t *testing.T, client *testserverclient.TestServerClient, statHandle *optimizor.StatsHandler) {
	db, err := sql.Open("mysql", client.GetDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)

	h := statHandle.Domain().StatsHandle()
	tk.MustExec("create database tidb")
	tk.MustExec("use tidb")
	tk.MustExec("create table test (a int, b varchar(20))")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	tk.MustExec("create index c on test (a, b)")
	tk.MustExec("insert test values (1, 's')")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	tk.MustExec("analyze table test")
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	tk.MustExec("insert into test(a,b) values (1, 'v'),(3, 'vvv'),(5, 'vv')")
	is := statHandle.Domain().InfoSchema()
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
}

func testDumpPartitionTableStats(t *testing.T, client *testserverclient.TestServerClient, handler *optimizor.StatsHandler) {
	preparePartitionData(t, client, handler)
	check := func(dumpStats bool) {
		expectedLen := 1
		if dumpStats {
			expectedLen = 2
		}
		url := fmt.Sprintf("/stats/dump/test/test2?dumpPartitionStats=%v", dumpStats)
		resp0, err := client.FetchStatus(url)
		require.NoError(t, err)
		defer func() {
			resp0.Body.Close()
		}()
		b, err := io.ReadAll(resp0.Body)
		require.NoError(t, err)
		jsonTable := &util2.JSONTable{}
		err = json.Unmarshal(b, jsonTable)
		require.NoError(t, err)
		require.NotNil(t, jsonTable.Partitions[util2.TiDBGlobalStats])
		require.Len(t, jsonTable.Partitions, expectedLen)
	}
	check(false)
	check(true)
}

func preparePartitionData(t *testing.T, client *testserverclient.TestServerClient, statHandle *optimizor.StatsHandler) {
	db, err := sql.Open("mysql", client.GetDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	h := statHandle.Domain().StatsHandle()
	tk := testkit.NewDBTestKit(t, db)
	tk.MustExec("create table test2(a int) PARTITION BY RANGE ( a ) (PARTITION p0 VALUES LESS THAN (6))")
	tk.MustExec("insert into test2 (a) values (1)")
	tk.MustExec("analyze table test2")
	is := statHandle.Domain().InfoSchema()
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(is))
}

func prepare4DumpHistoryStats(t *testing.T, client *testserverclient.TestServerClient) {
	db, err := sql.Open("mysql", client.GetDSN())
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

func checkCorrelation(t *testing.T, client *testserverclient.TestServerClient) {
	db, err := sql.Open("mysql", client.GetDSN())
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

func checkData(t *testing.T, path string, client *testserverclient.TestServerClient) {
	db, err := sql.Open("mysql", client.GetDSN(func(config *mysql.Config) {
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
	var other any
	err = rows.Scan(&dbName, &tableName, &other, &other, &modifyCount, &count, &other)
	require.NoError(t, err)
	require.Equal(t, "tidb", dbName)
	require.Equal(t, "test", tableName)
	require.Equal(t, int64(3), modifyCount)
	require.Equal(t, int64(4), count)
	require.NoError(t, rows.Close())
}
