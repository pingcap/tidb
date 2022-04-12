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

package server

import (
	"bytes"
	"database/sql"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestDumpPlanReplayerAPI(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	driver := NewTiDBDriver(store)
	client := newTestServerClient()
	cfg := newTestConfig()
	cfg.Port = client.port
	cfg.Socket = ""
	cfg.Status.StatusPort = client.statusPort
	cfg.Status.ReportStatus = true

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

	planReplayerHandler := &PlanReplayerHandler{}
	filename := prepareData4PlanReplayer(t, client, statsHandler)

	router := mux.NewRouter()
	router.Handle("/plan_replayer/dump/{filename}", planReplayerHandler)

	resp0, err := client.fetchStatus(filepath.Join("/plan_replayer/dump/", filename))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, resp0.Body.Close())
	}()

	body, err := ioutil.ReadAll(resp0.Body)
	require.NoError(t, err)

	path := "/tmp/plan_replayer.zip"
	fp, err := os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)
	defer func() {
		require.NoError(t, fp.Close())
		require.NoError(t, os.Remove(path))
	}()

	_, err = io.Copy(fp, bytes.NewReader(body))
	require.NoError(t, err)
	require.NoError(t, fp.Sync())

	db, err := sql.Open("mysql", client.getDSN(func(config *mysql.Config) {
		config.AllowAllFiles = true
	}))
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)

	tk.MustExec("use planReplayer")
	tk.MustExec("drop table planReplayer.t")
	tk.MustExec(`plan replayer load "/tmp/plan_replayer.zip"`)
	rows := tk.MustQuery("show stats_meta")
	require.True(t, rows.Next(), "unexpected data")
	var dbName, tableName string
	var modifyCount, count int64
	var other interface{}
	err = rows.Scan(&dbName, &tableName, &other, &other, &modifyCount, &count)
	require.NoError(t, err)
	require.Equal(t, "planReplayer", dbName)
	require.Equal(t, "t", tableName)
	require.Equal(t, int64(4), modifyCount)
	require.Equal(t, int64(8), count)
}

func prepareData4PlanReplayer(t *testing.T, client *testServerClient, statHandle *StatsHandler) string {
	db, err := sql.Open("mysql", client.getDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	tk := testkit.NewDBTestKit(t, db)

	h := statHandle.do.StatsHandle()
	tk.MustExec("create database planReplayer")
	tk.MustExec("use planReplayer")
	tk.MustExec("create table t(a int)")
	err = h.HandleDDLEvent(<-h.DDLEventCh())
	require.NoError(t, err)
	tk.MustExec("insert into t values(1), (2), (3), (4)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t values(5), (6), (7), (8)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	rows := tk.MustQuery("plan replayer dump explain select * from t")
	require.True(t, rows.Next(), "unexpected data")
	var filename string
	err = rows.Scan(&filename)
	require.NoError(t, err)
	return filename
}
